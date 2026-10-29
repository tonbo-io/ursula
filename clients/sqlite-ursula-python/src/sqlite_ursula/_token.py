"""The token source (design: Tokens): ``open()``'s and ``aopen()``'s ``token`` is a string or a
token provider, a synchronous callable that hands out a :class:`BearerToken`. Both kinds are
applied with ``ursula_set_token``, so the token stays one per process and the last open that
names one wins.

The package calls a provider before an open's first request when it holds no usable token, again
ahead of each token's expiry, and again at once when a request was refused with 401: the
replication condition ``unauthorized`` of a bound database (an ``unauthorized`` event, or the
status of the bound paths after a gap or a replaced events thread), or an open that failed
``unauthorized`` while it was binding. An open with a string, or :func:`set_token`, stops the
provider: the waiting opens go on under the string. Each call is handed the token the provider last
handed out as ``previous`` (the refused one after a 401), and an answer equal to it counts as a
failed call: the extension takes an equal token as no change, so a refused request would never go
out again. A retry is due by the expiry of ``previous`` at the latest, when a provider's cache turns
over, and the failures before that expiry no longer count. A token a 401 condition dropped is taken
back once none of the bindings that reported the condition is still bound and blocked.

One source per process, and its provider never runs on an event loop: ``aopen`` waits for its
token on the loop. An open with another provider only names the function the next call runs, and
shares the token held and the call in flight. A daemon thread, ``sqlite-ursula-token-source``,
keeps the schedule, and each call of the provider runs on a daemon thread of its own,
``sqlite-ursula-token``, so a call that does not return within :data:`PROVIDER_S` is abandoned
rather than holding up the next one. Once no binding the process opened is bound and no open with a
provider is under way, the source stops at its next due call or at a ``retired`` event. A child
forked from the process starts with no provider and no thread.

Tokens are never logged. No frame of the package's that a failure of the provider or of the source
passes through keeps one in a plain local: they travel in a :class:`_Fresh`, whose ``repr`` leaves
them out, so an error reporter that records frame locals records none.
"""

from __future__ import annotations

import inspect
import math
import os
import random
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from typing import Any
from typing import Protocol

from . import _control
from ._args import invalid
from .errors import UrsulaError
from .errors import UrsulaUnavailableError
from .errors import again

#: How long the package waits for one call of the provider: the call is then abandoned, counts as
#: failed, and its answer is discarded when it comes.
PROVIDER_S = 30.0
#: The first retry after a failed call, doubled after each failure in a row up to
#: :data:`MAX_RETRY_S`, then jittered down to half.
FIRST_RETRY_S = 1.0
MAX_RETRY_S = 30.0
#: The shortest token lifetime the package takes, and the earliest refresh after a token arrived.
MIN_REFRESH_S = 1.0
#: ``expires_at`` as a number is seconds since the epoch: one below this (2001-09-09) reads as a
#: lifetime, and one at or above :data:`MAX_EPOCH_S` (year 5138) as milliseconds.
MIN_EPOCH_S = 1e9
MAX_EPOCH_S = 1e11
#: A token as ``Authorization: Bearer`` carries it (clients/sqlite-vfs/src/auth.rs ``usable``).
USABLE = re.compile(r"[\x21-\x7e]+")


@dataclass(frozen=True)
class BearerToken:
    """What a token provider hands out: a bearer token and when it expires."""

    #: The token, printable ASCII without spaces. Never logged, and left out of ``repr``.
    token: str = field(repr=False)
    #: When the token expires: seconds since the epoch, as ``time.time()`` and a JWT's ``exp``
    #: count them, or a timezone-aware ``datetime``. A naive ``datetime``, a number below 10^9
    #: (a lifetime rather than a time) and one at or above 10^11 (milliseconds) are refused.
    expires_at: float | datetime


class TokenProvider(Protocol):
    """``open()``'s and ``aopen()``'s ``token`` when it changes: a synchronous callable, called
    with keywords only on a thread of the package, that returns a fresh :class:`BearerToken`.

    ``previous`` is the token the provider last handed out, exactly as it handed it out, when the
    package will not take it again: the current one at a scheduled refresh, or the one the server
    refused after a 401. It is ``None`` before the first call and after the provider stopped,
    except that a token an open's bind was refused under, or one a database still bound and
    blocked was refused under, stays ``previous`` across a stop, until the provider hands out
    another one or a string token or :func:`set_token` stops the provider. When ``previous`` is
    set, hand out a different token, and refill a cache that holds it: the package compares tokens
    without surrounding whitespace, and an equal one counts as a failed call, retried with backoff
    and by the expiry of ``previous`` at the latest, from where the backoff starts again. A token
    that only replication was refused under is taken back once each database whose replication
    reported the refusal is closed and retired (every commit acknowledged) or no longer blocked. A
    token that never changes belongs in a string ``token`` or in ``URSULA_VFS_TOKEN_FILE``.

    ``timeout`` is how many seconds the package waits for the answer. A call that has not returned
    by then counts as failed, and a call cut off by a stop has its answer discarded: either is left
    to finish on its thread.

    To fail for good, raise an exception whose ``action`` is ``"fix"``: the waiting opens raise
    it when it is an :class:`UrsulaError`, and an :class:`UrsulaUnavailableError`
    ``unauthorized``, ``fix``, whose ``__cause__`` it is, otherwise. The package calls the provider
    again only for the next open. Every other exception is retried with backoff.
    """

    def __call__(self, *, previous: str | None, timeout: float) -> BearerToken: ...


@dataclass(frozen=True)
class _Dropped:
    """The token a condition ``unauthorized`` of ``paths`` dropped. The event may predate the
    token, since the extension clears a condition only once the blocked request went out again
    under a new one: once none of ``paths`` is still bound and blocked, the provider's answer of
    that token is taken. A stop keeps it as long as it keeps the token held."""

    token: str = field(repr=False)
    paths: tuple[str, ...]


@dataclass(frozen=True)
class _Fresh:
    """A token as the source holds it."""

    #: The token as requests carry it: what the provider handed out, without surrounding
    #: whitespace.
    token: str = field(repr=False)
    #: What the provider handed out, which a later call gets back as ``previous``.
    handed: str = field(repr=False)
    #: When it expires, in ``time.time()`` seconds.
    expires_at: float
    #: When to ask for the next one, in ``time.monotonic()`` seconds.
    refresh_at: float


def _refresh_after(lifetime: float) -> float:
    """After 70 to 80% of the token's lifetime, at least :data:`MIN_REFRESH_S`."""
    return max(MIN_REFRESH_S, lifetime * (0.7 + 0.1 * random.random()))


def _retry_after(failures: int) -> float:
    capped = min(MAX_RETRY_S, FIRST_RETRY_S * 2 ** min(failures - 1, 16))
    return capped * (0.5 + 0.5 * random.random())


def permanent(error: BaseException) -> bool:
    """A failure the package does not retry: one whose ``action`` is ``fix``, the provider's own
    or an answer the package cannot use."""
    return getattr(error, "action", None) == "fix"


def failed_for_good(stream: str, error: BaseException) -> UrsulaError:
    """The error an open of ``stream`` raises when the provider failed for good with ``error``:
    one of the package's own classes as it is, any other error as the cause of one that says
    ``fix``. Only the package's own classes are copied, since another constructor may take other
    arguments: a subclass of :class:`UrsulaError` defined elsewhere is raised as the package's
    class it derives from, with its message, kind, action and fix, and ``error`` as its
    ``__cause__``."""
    if isinstance(error, UrsulaError):
        if type(error).__module__ == UrsulaError.__module__:
            return again(error)
        own = next(c for c in type(error).__mro__ if c.__module__ == UrsulaError.__module__)
        return own(str(error), error.kind, error.action, fix=error.fix, cause=error)
    reason = str(error) or type(error).__name__
    return UrsulaUnavailableError(
        f"unauthorized: the token provider failed for good ({reason}): the open of {stream} has "
        "no token",
        "unauthorized",
        "fix",
        cause=error,
    )


def _unanswered(message: str) -> UrsulaUnavailableError:
    return UrsulaUnavailableError(f"unavailable: {message}", "unavailable", "retry")


def _expiry(raw: object) -> float:
    if isinstance(raw, datetime):
        if raw.utcoffset() is None:
            raise invalid(
                "the token provider returned a naive datetime as expires_at: pass a "
                "timezone-aware one, or seconds since the epoch"
            )
        return raw.timestamp()
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or not math.isfinite(raw):
        raise invalid(
            f"the token provider returned expires_at {type(raw).__name__}: pass seconds since "
            "the epoch or a timezone-aware datetime"
        )
    if raw < MIN_EPOCH_S:
        raise invalid(
            f"the token provider returned expires_at {raw!r}, which reads as a lifetime rather "
            "than a time: pass seconds since the epoch (time.time() + lifetime)"
        )
    if raw >= MAX_EPOCH_S:
        raise invalid(
            f"the token provider returned expires_at {raw!r}, which reads as milliseconds rather "
            "than seconds since the epoch: pass seconds (a JWT's exp as it is)"
        )
    return float(raw)


def _checked(got: object) -> _Fresh:
    """What the provider returned, checked: an answer the package cannot use fails for good, and
    a token that expires within :data:`MIN_REFRESH_S` is retried."""
    if inspect.isawaitable(got):
        close = getattr(got, "close", None)
        if callable(close):
            # Never awaited: closed, so it warns of nothing.
            close()
        raise invalid(
            f"the token provider returned {type(got).__name__}, an awaitable: it must be "
            "synchronous and return a sqlite_ursula.BearerToken, since it runs on a thread of "
            "the package, never on an event loop"
        )
    if not isinstance(got, BearerToken):
        raise invalid(
            f"the token provider returned {type(got).__name__}, not a sqlite_ursula.BearerToken"
        )
    # The token is read inline, never into a local of this frame, which a failure passes through.
    if not _usable(got.token):
        raise invalid(
            "the token provider returned no usable token: a non-empty string of printable ASCII "
            "without spaces"
        )
    expires_at = _expiry(got.expires_at)
    lifetime = expires_at - time.time()
    if lifetime < MIN_REFRESH_S:
        raise _unanswered(
            f"the token provider returned a token that expires in {lifetime:.3f} s, under the "
            f"{MIN_REFRESH_S:g} s the package needs to refresh it in time"
        )
    return _Fresh(
        got.token.strip(), got.token, expires_at, time.monotonic() + _refresh_after(lifetime)
    )


def _usable(token: object) -> bool:
    """Whether ``token``, without surrounding whitespace, can go out as a bearer token."""
    return isinstance(token, str) and USABLE.fullmatch(token.strip()) is not None


class _Wanted:
    """An open waiting for a token: its outcome is set once, then its waiter."""

    __slots__ = ("outcome", "waiter")

    def __init__(self, waiter: Any) -> None:
        self.waiter = waiter
        #: ``("token", str | None)``, ``None`` when a string replaced the provider, or
        #: ``("error", error)`` when the provider failed for good.
        self.outcome: tuple[str, Any] | None = None

    def settle(self, outcome: tuple[str, Any]) -> None:
        if self.outcome is None:
            self.outcome = outcome
            self.waiter.set()


class Opening:
    """One open with a provider, from :meth:`Source.begin` to :meth:`end`: its token, at once or
    once the provider answers. The token stays an attribute, never a local of the open's frame,
    which the open's error passes through."""

    def __init__(self, source: Source, token: str | None, wanted: _Wanted | None) -> None:
        self._source = source
        self.token = token
        self._wanted = wanted

    def wait(self, stream: str, deadline: float, timeout: float) -> None:
        """Until the token is in :attr:`token`, waiting on the calling thread until ``deadline``
        (``time.monotonic()``)."""
        wanted = self._wanted
        while wanted is not None and wanted.outcome is None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise self._source.gave_up(wanted, stream, timeout)
            wanted.waiter.wait(left)
        self._outcome(stream)

    async def await_(self, stream: str, deadline: float, timeout: float) -> None:
        """:meth:`wait` on the event loop."""
        wanted = self._wanted
        while wanted is not None and wanted.outcome is None:
            left = deadline - time.monotonic()
            if left <= 0:
                raise self._source.gave_up(wanted, stream, timeout)
            await wanted.waiter.wait(left)
        self._outcome(stream)

    def _outcome(self, stream: str) -> None:
        wanted = self._wanted
        if wanted is None:
            return
        assert wanted.outcome is not None
        if wanted.outcome[0] == "error":
            raise failed_for_good(stream, wanted.outcome[1])
        self.token = wanted.outcome[1]

    def refused_with(self, error: UrsulaError) -> UrsulaError:
        """:meth:`Source.refused_with` for this open's token."""
        return self._source.refused_with(error, self.token)

    def end(self) -> None:
        self._source.end(self._wanted)


class Source:
    """The process's provider and its schedule. At most one call of it runs at a time, on the
    source's thread, and concurrent opens share it.

    Lock order: the control lock (``_control.lock()``), then the source's condition. The thread
    never takes the control lock while it holds the condition.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._thread: threading.Thread | None = None
        self._provider: Callable[..., Any] | None = None
        #: Increases at each stop: a call of an earlier epoch is discarded.
        self._epoch = 0
        self._current: _Fresh | None = None
        #: The token the provider last handed out, also once a 401 dropped it, which its next call
        #: gets as ``previous``. A stop forgets it, unless the server refused it and no string
        #: replaced it: a bind was refused under it, or a reporter of the condition that dropped it
        #: is still bound and blocked.
        self._held: _Fresh | None = None
        #: The token a condition ``unauthorized`` dropped, and the paths that reported it.
        self._dropped: _Dropped | None = None
        #: The epoch of the call in flight, if any.
        self._calling: int | None = None
        #: A call is asked for now.
        self._kick = False
        #: When the next call is due, in ``time.monotonic()`` seconds: a refresh or a retry.
        self._due: float | None = None
        #: Failed calls in a row.
        self._failures = 0
        #: Of ``_failures``, those the backoff no longer counts once ``previous`` expired: the
        #: ones before its expiry.
        self._forgiven: int | None = None
        #: The last call failed by handing back the token it was handed as ``previous``.
        self._handed_back = False
        #: The last call's failure, one for good included, until a call succeeds.
        self._last_failure: BaseException | None = None
        #: It failed for good: no call until the next open.
        self._failed = False
        self._wanted: list[_Wanted] = []
        #: Opens with the provider that have not ended.
        self._opening = 0
        #: The paths the process bound, whatever token their opens named: the process's token
        #: serves them all. Under the control lock.
        self._paths: set[str] = set()

    # The opens'.

    def begin(self, provider: Callable[..., Any], waiter: Any) -> Opening:
        """An open with ``provider``: its token at once when one is held and usable, else an
        :class:`Opening` that waits for it with ``waiter``. The next call runs ``provider``: the
        token held and the call in flight serve on, since the process carries one token, whichever
        function minted it."""
        with _control.lock(), self._cond:
            self._provider = provider
            self._start_locked()
            self._opening += 1
            # A new open asks a provider that failed for good again.
            failing = self._failed or self._failures > 0
            self._failed = False
            current = self._current
            if current is not None and (failing or current.expires_at > time.time()):
                if self._calling != self._epoch and self._due is None:
                    self._kick_locked()
                # The process carries it already: the call that took it set it in the same
                # step, under the control lock, and every other writer of the process's token
                # stops the source.
                return Opening(self, current.token, None)
            wanted = _Wanted(waiter)
            self._wanted.append(wanted)
            # A retry already scheduled runs on time, or at once when it would take back a
            # token whose reporters cleared.
            if self._calling != self._epoch:
                if not self._backing_off():
                    self._kick_locked()
                else:
                    self._reconsider_locked()
            return Opening(self, None, wanted)

    def end(self, wanted: _Wanted | None) -> None:
        with self._cond:
            self._opening -= 1
            if wanted is not None and wanted in self._wanted:
                self._wanted.remove(wanted)

    def gave_up(self, wanted: _Wanted, stream: str, timeout: float) -> UrsulaUnavailableError:
        """The open of ``stream`` gave up waiting for its token."""
        with self._cond:
            if wanted in self._wanted:
                self._wanted.remove(wanted)
            last = self._last_failure if self._failures > 0 else None
        why = (
            f"its last call failed: {last}"
            if last is not None
            else f"it did not answer within {timeout:g} s"
        )
        return UrsulaUnavailableError(
            f"unavailable: the token provider handed out no token ({why}): the open of {stream} "
            f"gave up after {timeout:g} s",
            "unavailable",
            "retry",
            cause=last,
        )

    def refused_with(self, error: UrsulaError, token: str | None) -> UrsulaError:
        """The error an open under ``token`` raises: a 401 drops the token, and says ``retry``
        when the provider's last call failed but may succeed."""
        if (
            not isinstance(error, UrsulaUnavailableError)
            or error.kind != "unauthorized"
            or token is None
        ):
            return error
        with self._cond:
            last = self._last_failure
        self.refused(token)
        # No failure recorded: the server refused a fresh token, which a new call will not fix.
        if last is None:
            return error
        return UrsulaUnavailableError(
            f"{error} (the token provider's last call failed: {last})",
            "unauthorized",
            "fix" if permanent(last) else "retry",
            cause=last,
        )

    def refused(self, token: str) -> None:
        """A bind under ``token`` was refused with 401: the token is dropped unless a newer one is
        held, and a call starts unless one is in flight or a retry is scheduled."""
        with self._cond:
            if self._provider is None or self._failed:
                return
            current = self._current
            if current is not None and current.token != token:
                return
            if self._dropped is not None and self._dropped.token == token:
                self._dropped = None
            self._current = None
            self._ask_locked()

    def bound(self, path: str) -> None:
        """A binding at ``path`` is open: the source serves it until it retires."""
        with _control.lock():
            self._paths.add(path)

    def use_string(self, token: str) -> None:
        """An open with a string token, or :func:`set_token`: requests carry ``token`` (``""``:
        the token file's) from now on, and the provider stops. Stopped already, it still forgets a
        token the server refused."""
        _control.check_fork()
        with _control.lock():
            with self._cond:
                self._stop_locked(replaced=True)
            _control.call("SELECT ursula_set_token('', ?)", token)

    # The events thread's, with the control lock held.

    def blocked(self, paths: list[str]) -> None:
        """The replication of ``paths`` blocked ``unauthorized``: the token held is dropped, and a
        call starts unless one is in flight or a retry is scheduled."""
        with self._cond:
            if self._provider is None or self._failed:
                return
            current = self._current
            if current is not None:
                self._dropped = _Dropped(current.token, tuple(dict.fromkeys(paths)))
            elif self._dropped is not None:
                merged = dict.fromkeys((*self._dropped.paths, *paths))
                self._dropped = _Dropped(self._dropped.token, tuple(merged))
            self._current = None
            self._ask_locked()

    def retired(self) -> None:
        """A binding retired: the source stops once nothing uses it, and otherwise reconsiders its
        scheduled retry. Pruned with or without a provider, so the paths of string opens never
        pile up."""
        used = self._in_use()
        with self._cond:
            if not used and self._provider is not None and not self._wanted:
                self._stop_locked(replaced=False)
            else:
                self._reconsider_locked()

    def missed(self) -> None:
        """Events may have been missed (a gap, a replaced events thread): they may have held an
        ``unauthorized`` condition, which the bound paths' status still shows, or a clear."""
        if self._provider is None:
            return
        blocked = [path for path in list(self._paths) if _standing(path) == "blocked"]
        if blocked:
            self.blocked(blocked)
        else:
            self.reconsider()

    def reconsider(self) -> None:
        """A binding retired, a condition other than ``unauthorized`` arrived, or an open waits:
        when the provider handed back the token a condition dropped and its reporters are now
        clear, the retry scheduled runs at once rather than after its backoff, so the opens
        waiting take that token back."""
        with self._cond:
            self._reconsider_locked()

    def serving(self) -> bool:
        """Whether a provider serves the process: the events thread is replaced for it."""
        return self._provider is not None

    # The source's own.

    def _in_use(self) -> bool:
        """Drops the paths that retired, then whether a path is bound or an open with a provider
        is under way. With the control lock held, not the condition."""
        for path in list(self._paths):
            if _standing(path) == "retired":
                self._paths.discard(path)
        with self._cond:
            opening = self._opening
        return bool(self._paths) or opening > 0

    def _backing_off(self) -> bool:
        """A retry after a failed call is scheduled."""
        return self._failures > 0 and self._due is not None

    def _reconsider_locked(self) -> None:
        """:meth:`reconsider` with the condition held, and the control lock, which the status
        reads need."""
        dropped = self._dropped
        if (
            self._provider is None
            or self._failed
            or not self._handed_back
            or not self._backing_off()
            or dropped is None
        ):
            return
        held = self._held
        if held is not None and held.token == dropped.token and _cleared(dropped.paths):
            self._kick_locked()

    def _ask_locked(self) -> None:
        """A call at once, unless one is in flight or a retry is scheduled."""
        if self._calling != self._epoch and not self._backing_off():
            self._kick_locked()

    def _kick_locked(self) -> None:
        self._kick = True
        self._cond.notify_all()

    def _schedule_locked(self, seconds: float) -> None:
        self._due = time.monotonic() + max(0.0, seconds)
        self._cond.notify_all()

    def _start_locked(self) -> None:
        """Starts the source's thread, unless it runs. It never calls the provider itself, so only
        an interpreter shutting down ends it."""
        if self._thread is None or not self._thread.is_alive():
            self._thread = threading.Thread(
                target=self._run, name="sqlite-ursula-token-source", daemon=True
            )
            self._thread.start()

    def _stop_locked(self, *, replaced: bool) -> None:
        """No provider from now on: the call in flight is cut off, its answer discarded, and the
        opens waiting go on with the process's token. ``replaced``: a string replaced the
        provider's token. Otherwise a token the server refused is still the process's, so the next
        call gets it as ``previous``, unless only a condition dropped it and its reporters are
        clear. A token kept stays dropped, so it is taken back once its reporters clear. With the
        control lock held, which that check needs."""
        self._epoch += 1
        self._provider = None
        held = self._held
        if (
            replaced
            or self._current is not None
            or (held is not None and self._restored_locked(held.token))
        ):
            self._held = None
            self._dropped = None
        self._current = None
        self._kick = False
        self._due = None
        self._failures = 0
        self._forgiven = None
        self._handed_back = False
        self._last_failure = None
        self._failed = False
        for wanted in self._wanted:
            wanted.settle(("token", None))
        self._wanted.clear()
        # The thread waiting for the call in flight stops waiting.
        self._cond.notify_all()

    def _run(self) -> None:
        while True:
            with self._cond:
                while not self._kick and (self._due is None or time.monotonic() < self._due):
                    self._cond.wait(None if self._due is None else self._due - time.monotonic())
                kicked = self._kick
                self._kick = False
                # The call runs now: no retry is scheduled any more, so nothing asks for another
                # one meanwhile.
                self._due = None
                epoch = self._epoch
                waited = bool(self._wanted)
                if kicked or waited:
                    self._calling = epoch
            try:
                if not kicked and not waited and self._idle(epoch):
                    continue
                self._call(epoch)
            except Exception as error:
                # A failure of the source's own, not of the provider, which ``_call`` keeps: it
                # counts as a failed call, so the thread and the schedule go on and the next open
                # still asks for a call.
                self._broke(epoch, _frameless(error))

    def _broke(self, epoch: int, error: Exception) -> None:
        with self._cond:
            if self._calling == epoch:
                self._calling = None
            if epoch != self._epoch or self._provider is None or self._failed:
                return
            held = self._held
            self._failed_locked(error, None if held is None else held.expires_at)

    def _idle(self, epoch: int) -> bool:
        """A refresh or a retry is due: the source stops, and says so, if nothing uses it."""
        with _control.lock():
            used = self._in_use()
            with self._cond:
                if used or epoch != self._epoch or self._wanted or self._kick:
                    if epoch == self._epoch:
                        # The call runs now, and serves a call asked for meanwhile.
                        self._kick = False
                        self._calling = epoch
                    return False
                self._stop_locked(replaced=False)
                return True

    def _call(self, epoch: int) -> None:
        with self._cond:
            provider = self._provider
            if provider is None or epoch != self._epoch:
                if self._calling == epoch:
                    self._calling = None
                return
            previous = self._held
            self._calling = epoch
            self._due = None
        fresh: _Fresh | None = None
        failure: BaseException | None = None
        try:
            fresh = _checked(self._answer(provider, previous, epoch))
        except BaseException as error:
            failure = error
        # The token goes to the process and to the source in one step: an open that reads the
        # token held finds the one the process carries.
        with _control.lock(), self._cond:
            if self._calling == epoch:
                self._calling = None
            if epoch != self._epoch:
                return
            if (
                fresh is not None
                and previous is not None
                and fresh.token == previous.token
                and not self._restored_locked(fresh.token)
            ):
                # The extension takes an equal token as no change, so it would not send a refused
                # request again.
                fresh = None
                failure = _HandedBack(
                    "unavailable: the token provider returned the token it was handed as "
                    "previous, which the package holds already or the server refused: hand out a "
                    "new one",
                    "unavailable",
                    "retry",
                )
            if fresh is not None:
                try:
                    _control.call("SELECT ursula_set_token('', ?)", fresh.token)
                except Exception as error:
                    # Its frames hold the token as an argument of the call.
                    failure = _frameless(error)
                else:
                    self._succeeded_locked(fresh)
                    return
            assert failure is not None
            self._failed_locked(failure, None if previous is None else previous.expires_at)

    def _restored_locked(self, token: str) -> bool:
        """Whether the provider's answer ``token``, equal to ``previous``, is taken: only when a
        condition that may predate it dropped it, and none of the paths that reported the
        condition is bound and blocked any more."""
        dropped = self._dropped
        return dropped is not None and dropped.token == token and _cleared(dropped.paths)

    def _answer(self, provider: Callable[..., Any], previous: _Fresh | None, epoch: int) -> object:
        """The provider's answer to one call, made on a thread of its own: a call that has not
        returned after :data:`PROVIDER_S`, or once the source stopped, is abandoned. A call the
        source stopped before it started never reaches the provider."""
        call = _Call(self._cond, provider, previous)
        deadline = time.monotonic() + PROVIDER_S
        with self._cond:
            if epoch != self._epoch:
                raise _unanswered("the token provider stopped before its call started")
            call.start()
            while not call.done and epoch == self._epoch:
                left = deadline - time.monotonic()
                if left <= 0:
                    break
                self._cond.wait(left)
            if not call.done:
                raise _unanswered(
                    f"the token provider did not answer within {PROVIDER_S:g} s, and the call "
                    "was abandoned"
                )
            if call.failure is not None:
                raise call.failure
            return call.got

    def _succeeded_locked(self, fresh: _Fresh) -> None:
        self._current = fresh
        self._held = fresh
        self._dropped = None
        self._failures = 0
        self._forgiven = None
        self._handed_back = False
        self._last_failure = None
        for wanted in self._wanted:
            wanted.settle(("token", fresh.token))
        self._wanted.clear()
        self._schedule_locked(fresh.refresh_at - time.monotonic())

    def _failed_locked(self, error: BaseException, expiry: float | None) -> None:
        """A failed call: retried with backoff, and by ``expiry`` (``time.time()``), that of
        ``previous``, at the latest, unless it failed for good. From that expiry on, only the
        failures since it count, whichever retry they came from: the first failure at or after it
        restarts the backoff."""
        self._last_failure = error
        self._handed_back = isinstance(error, _HandedBack)
        if permanent(error):
            self._failed = True
            for wanted in self._wanted:
                wanted.settle(("error", error))
            self._wanted.clear()
            return
        self._failures += 1
        now = time.time()
        if expiry is not None and now >= expiry and self._forgiven is None:
            self._forgiven = self._failures - 1
        backoff = _retry_after(self._failures - (self._forgiven or 0))
        self._schedule_locked(
            min(backoff, expiry - now) if expiry is not None and now < expiry else backoff
        )


class _HandedBack(UrsulaUnavailableError):
    """A call that handed back the token it was handed as ``previous``: a failed call, retried."""


def _standing(path: str) -> str:
    """The binding of ``path`` now: ``blocked`` ``unauthorized``, ``clear``, ``retired`` (also
    once the process no longer has it), or ``unknown`` when its status cannot be read for another
    reason. With the control lock held."""
    try:
        status = _control.call("SELECT ursula_status(?)", path)
        if status.get("state") == "retired":
            return "retired"
        replication = status.get("replication") or {}
        return "blocked" if replication.get("condition") == "unauthorized" else "clear"
    except UrsulaError as error:
        return "retired" if error.kind == "not_bound" else "unknown"
    except Exception:
        # A status the package cannot read, malformed included.
        return "unknown"


def _frameless(error: BaseException) -> BaseException:
    """``error`` with no traceback along its causes, so it keeps no frame's locals."""
    seen: set[int] = set()
    at: BaseException | None = error
    while at is not None and id(at) not in seen:
        seen.add(id(at))
        at.__traceback__ = None
        at = at.__cause__ or at.__context__
    return error


def _cleared(paths: tuple[str, ...]) -> bool:
    """Whether none of ``paths`` is still bound and blocked ``unauthorized``: a path that retired
    is clear, and so is a condition with no path, which a binding the process no longer has
    reported. A path whose status cannot be read may still be blocked."""
    return all(_standing(path) in ("clear", "retired") for path in paths)


class _Call:
    """One call of the provider, on a daemon thread of its own, ``sqlite-ursula-token``: the
    source waits for it up to :data:`PROVIDER_S`, and a call it abandoned keeps its answer to
    itself when it comes. Any exception the call raises is its failure, ``SystemExit`` included."""

    def __init__(
        self, cond: threading.Condition, provider: Callable[..., Any], previous: _Fresh | None
    ):
        self._cond = cond
        self._provider = provider
        self._previous = previous
        #: Set with ``got`` or ``failure`` under the condition, which is then notified.
        self.done = False
        self.got: object = None
        self.failure: BaseException | None = None

    def start(self) -> None:
        threading.Thread(target=self._run, name="sqlite-ursula-token", daemon=True).start()

    def _run(self) -> None:
        got: object = None
        failure: BaseException | None = None
        try:
            # ``previous`` is read inline, never into a local of this frame, which the provider's
            # failure passes through.
            got = self._provider(
                previous=None if self._previous is None else self._previous.handed,
                timeout=PROVIDER_S,
            )
        except BaseException as error:
            failure = error
        with self._cond:
            self.got = got
            self.failure = failure
            self.done = True
            self._cond.notify_all()


#: The process's token source.
source = Source()


def provider_of(token: object) -> Callable[..., Any] | None:
    """``token`` when it is a provider: callable and not a string."""
    return token if callable(token) and not isinstance(token, (str, BearerToken)) else None


def asynchronous(provider: Callable[..., Any]) -> bool:
    """Whether ``provider`` is an async function or an async generator function, or an object
    whose ``__call__`` is one: its answer would be a coroutine, never a token."""
    # An object's ``__call__`` is looked up on its class, as a call looks it up.
    for function in (provider, inspect.getattr_static(type(provider), "__call__", None)):
        if function is not None and (
            inspect.iscoroutinefunction(function) or inspect.isasyncgenfunction(function)
        ):
            return True
    return False


def takes_keywords(provider: Callable[..., Any]) -> bool:
    """Whether ``provider`` takes the source's call, ``provider(previous=..., timeout=...)``, so a
    provider written for another signature fails its open at once rather than each call. A callable
    whose signature Python cannot read passes: its calls say."""
    try:
        signature = inspect.signature(provider)
    except (TypeError, ValueError):
        return True
    try:
        signature.bind(previous=None, timeout=PROVIDER_S)
    except TypeError:
        return False
    return True


def _after_fork_in_child() -> None:
    global source
    # The parent's thread is not the child's, and neither are its provider, its token and the
    # bindings it served: the child starts with none.
    source = Source()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)
