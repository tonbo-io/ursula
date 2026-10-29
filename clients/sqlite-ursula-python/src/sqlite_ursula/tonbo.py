"""``sqlite_ursula.tonbo`` (design: Tonbo): open a Tonbo database by name with a Tonbo API key. The
Python peer of the npm entry ``@tonbo/sqlite-ursula/tonbo``::

    from sqlite_ursula import tonbo

    db = tonbo.open("notes")  # or the database's https:// URL, which skips the lookup
    db.execute("INSERT INTO notes(body) VALUES (?)", ("hello",))
    db.commit()
    db.synced()

The extension's ``tonbo_resolve`` does the work, in one implementation for every host: it finds the
key, looks the name up in the Management API and, unless the open brings a token of its own, makes
the key the process's source of Streams tokens, which it exchanges and refreshes itself, holds the
first one before it answers, and clears the token set. This module calls it without blocking
(timeout 0, then its ``resolved`` event on the events thread, as ``open`` waits for
``ursula_open``), joins the process's concurrent resolves of the same arguments, from any thread or
task, and turns the failure the extension reports as fields into the package's classes. Then it
opens the stream as the root module's ``open`` does.

- The key: the ``api_key`` option, else ``TONBO_API_KEY``, else the file ``tonbo login`` writes,
  as the extension reads them. ``api_origin`` and ``auth_origin`` go to the extension as they are,
  over ``TONBO_API_ORIGIN`` and ``TONBO_AUTH_ORIGIN``.
- An explicit ``token`` serves the Streams requests instead of the key's tokens, and so does
  ``URSULA_VFS_TOKEN_FILE``, which the extension reads.
- ``timeout`` bounds the whole open, the lookup included. At the timeout one last pass takes an
  outcome that arrived. Otherwise the call leaves the resolve to the other calls that share it and
  reports ``cancelled``, and the last one cancels it (``ursula_cancel``) and reports the last
  condition it met. On Ctrl-C, a cancellation or another exception, the call leaves the resolve
  the same way and raises that exception. A Ctrl-C that lands inside the first ``tonbo_resolve``
  call of a resolve, before its reply is read, can miss the cancel: that resolve then runs to the
  extension's own deadline.

Nothing here logs, no error message carries a key or a token, and the options a resolve sends,
which may hold a key, never show in a ``repr``.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from . import _control
from . import _events
from . import _open
from ._args import invalid
from ._args import seconds
from ._connection import UrsulaConnection
from ._connection import UrsulaCursor
from ._gate import agate
from ._gate import gate
from ._open import DEFAULT_BUSY_S
from ._open import DEFAULT_OPEN_S
from .errors import Action
from .errors import UrsulaError
from .errors import UrsulaResetError
from .errors import UrsulaSetupError
from .errors import UrsulaUnavailableError
from .errors import UrsulaWriteRefusedError
from .errors import again
from .errors import error_of

__all__ = [
    "Action",
    "ResolvedDatabase",
    "TonboError",
    "UrsulaConnection",
    "UrsulaCursor",
    "UrsulaError",
    "UrsulaResetError",
    "UrsulaSetupError",
    "UrsulaUnavailableError",
    "UrsulaWriteRefusedError",
    "agate",
    "aopen",
    "aresolve_database",
    "gate",
    "open",
    "resolve_database",
]

#: The extension's own deadline for a resolve: far past any ``timeout``, which governs it.
EXTENSION_RESOLVE_MS = 24 * 60 * 60 * 1000
#: The kinds a :class:`TonboError` carries.
TONBO_KINDS = frozenset(
    {"missing_credentials", "credentials_refused", "invalid_api_key", "database_not_found"}
)


class TonboError(UrsulaError):
    """A Tonbo API key or login the open cannot use, or a database the account does not have.

    ``kind`` names what failed, ``fix`` what to do (the variable to set or the command to run),
    which ends the message, and ``request_id`` the Tonbo API's id of the answer that failed, when
    one did. The action is always ``fix``.

    - ``missing_credentials``: no ``api_key``, no ``TONBO_API_KEY`` and no ``tonbo login``.
    - ``credentials_refused``: the login file is a symlink, not a regular file, or open to group
      or others.
    - ``invalid_api_key``: the Tonbo API refused the key, the key cannot read the database (403),
      ``TONBO_API_KEY`` holds no usable key, or the login file expired or is not one the extension
      reads.
    - ``database_not_found``: the account has no database of that name, or it is being deleted.
    """

    fix: str
    request_id: str | None

    def __init__(
        self,
        message: str,
        kind: str,
        action: Action = "fix",
        *,
        fix: str | None = None,
        request_id: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message, kind, action, fix=fix or "", cause=cause)
        self.request_id = request_id


@dataclass(frozen=True)
class ResolvedDatabase:
    """What :func:`resolve_database` hands out: the stream to open, and the ``token`` to open it
    with."""

    #: The database's stream URL.
    url: str
    #: The database's name when it was looked up, ``None`` for a URL.
    name: str | None
    #: The ``token`` the resolve was given, or ``None`` when the key's Streams tokens, which the
    #: extension holds and refreshes, or ``URSULA_VFS_TOKEN_FILE`` serve the Streams requests.
    #: Never in the repr.
    token: str | None = field(repr=False)


class _Options:
    """``tonbo_resolve``'s options. They may hold a key, so their repr never shows them, and calls
    are joined by a digest of them, not by them."""

    __slots__ = ("digest", "text")

    def __init__(self, target: str, text: str) -> None:
        self.text = text
        self.digest = hashlib.sha256(f"{target}\n{text}".encode()).digest()

    def __repr__(self) -> str:
        return "<tonbo_resolve options>"


def _options(
    name_or_url: object, api_key: object, api_origin: object, auth_origin: object, token: object
) -> tuple[str, _Options]:
    """Checks a resolve's arguments, before any request. No local but ``api_key`` and ``token``
    holds a key or a token while a check raises, so that error reporters that scrub those names
    find no other."""
    if not isinstance(name_or_url, str):
        raise invalid(f"name_or_url must be a string, not {type(name_or_url).__name__}")
    wrong = [
        # Never the value itself: it may be a key.
        f"{option} must be a string, not {type(value).__name__}"
        for option, value in (
            ("api_key", api_key),
            ("api_origin", api_origin),
            ("auth_origin", auth_origin),
        )
        if value is not None and not isinstance(value, str)
    ]
    if wrong:
        raise invalid(wrong[0])
    _open._token(token)
    # Options left out take the extension's defaults.
    settings: dict[str, object] = {
        option: value
        for option, value in (
            ("api_key", api_key),
            ("api_origin", api_origin),
            ("auth_origin", auth_origin),
        )
        if value is not None
    }
    settings["own_token"] = token is not None
    settings["timeout_ms"] = EXTENSION_RESOLVE_MS
    return name_or_url, _Options(name_or_url, json.dumps(settings))


class _Shared:
    """One resolve the process's calls with the same arguments share: the request the extension
    named, and its outcome once a call took it. Changed under the control lock."""

    __slots__ = ("calls", "outcome", "request")

    def __init__(self) -> None:
        #: The calls waiting for it.
        self.calls = 0
        self.request: int | None = None
        self.outcome: dict[str, Any] | UrsulaError | None = None


#: The resolves in flight, by the digest of their arguments. Kept under the control lock.
_inflight: dict[bytes, _Shared] = {}


class _Resolve:
    """One call's resolve, an :class:`_events.Step`: ``tonbo_resolve`` with timeout 0, asked again
    each time its waiter wakes, until a call of the shared resolve took its outcome.

    The extension owes a host one outcome for its calls that returned at once, so the calls of the
    process share one :class:`_Shared` by their arguments: the first back takes the outcome for
    all. A call joins it in its first pass, under the lock, so a call counted is one that leaves.
    A call that gives up, at its timeout or at an exception, leaves the resolve to the others, and
    the last one cancels it. At the timeout it reports ``cancelled`` or what the cancel reports,
    and at an exception the exception is raised."""

    def __init__(self, target: str, options: _Options, timeout: float, deadline: float) -> None:
        self._target = target
        self._options = options
        self._timeout = timeout
        self._deadline = deadline
        self._ev: _events.Events | None = None
        self._waiter: _events.Waiter | None = None
        self._request: int | None = None
        #: The call took the outcome or left the resolve.
        self._done = False
        #: The resolve the call shares, from its first pass on.
        self._joined: _Shared | None = None

    @property
    def _shared(self) -> _Shared:
        shared = self._joined
        assert shared is not None, "a pass joins the resolve first"
        return shared

    def ask(self, waiter: _events.Waiter) -> dict[str, Any] | None:
        """One pass: the answer when the resolve is done, else ``None`` with ``waiter`` registered
        for the request's ``resolved`` event. Raises the resolve's failure. The first pass joins
        the resolve the process's calls with the same arguments share."""
        with _control.lock():
            if self._joined is None:
                shared = _inflight.get(self._options.digest)
                if shared is None:
                    shared = _inflight[self._options.digest] = _Shared()
                shared.calls += 1
                self._joined = shared
            self._forget()
            if self._shared.outcome is None:
                # The thread first, so its cursor precedes the request, and the waiter registered
                # in the step that got the request, before any event about it applies.
                ev = _events.events()
                request = self._poll()
                if request is not None:
                    self._request = request
                    ev.wait_request(request, waiter)
                    self._ev, self._waiter = ev, waiter
                    return None
            return self._take()

    def left(self) -> float:
        return self._deadline - time.monotonic()

    def again(self, error: BaseException) -> bool:
        """A pass raised ``error``, which the caller raises: a call that did not take the outcome
        leaves the resolve, and the last one cancels it."""
        with _control.lock():
            self._forget()
            if self._joined is not None and not self._done:
                self._leave()
        return False

    def timed_out(self) -> dict[str, Any]:
        """The timeout passed: one last pass takes an outcome that arrived, else the call leaves
        the resolve and raises ``cancelled`` or, as the last, what the cancel reports."""
        with _control.lock():
            self._forget()
            try:
                pending = self._shared.outcome is None and self._poll() is not None
            except BaseException:
                self._leave()
                raise
            if not pending:
                return self._take()
            error = self._leave()
        raise error

    def _poll(self) -> int | None:
        """``tonbo_resolve`` once, under the lock: its request while the resolve goes on, else
        ``None`` with the outcome in for every call that shares it."""
        shared = self._shared
        answer = _control.call("SELECT tonbo_resolve(?, ?, 0)", self._target, self._options.text)
        if not answer["done"]:
            shared.request = int(answer["request"])
            return shared.request
        shared.outcome = _error(answer["error"]) if "error" in answer else answer
        if _inflight.get(self._options.digest) is shared:
            del _inflight[self._options.digest]
        return None

    def _take(self) -> dict[str, Any]:
        """Takes the outcome, under the lock: the answer, or raises the failure."""
        outcome = self._shared.outcome
        assert outcome is not None
        self._done = True
        self._shared.calls -= 1
        if isinstance(outcome, UrsulaError):
            raise again(outcome)
        return outcome

    def _leave(self) -> UrsulaError:
        """The call leaves the resolve, under the lock. It goes on for the other calls that share
        it, and the error is ``cancelled``. The last call cancels it, and the error is the last
        condition it met, or ``cancelled`` when it met none or ended as it was cancelled. The
        cancel of a resolve never waits."""
        shared = self._shared
        self._done = True
        shared.calls -= 1
        if shared.outcome is not None:
            return self._cancelled("")
        if shared.calls > 0:
            return self._cancelled(", and it goes on for the other calls that share it")
        if _inflight.get(self._options.digest) is shared:
            del _inflight[self._options.digest]
        if shared.request is None:
            return self._cancelled(" before it met any condition")
        try:
            ended = _control.call("SELECT ursula_cancel(?)", shared.request)
        except UrsulaError:
            # It ended and its outcome was taken meanwhile.
            ended = None
        if _open.met_nothing(ended):
            return self._cancelled(" before it met any condition")
        return _error(ended)

    def _forget(self) -> None:
        if self._ev is not None and self._waiter is not None and self._request is not None:
            self._ev.forget_request(self._request, self._waiter)
        self._ev = self._waiter = None

    def _cancelled(self, why: str) -> UrsulaUnavailableError:
        return UrsulaUnavailableError(
            f'cancelled: the resolve of "{self._target}" gave up after {self._timeout:g} s{why}',
            "cancelled",
            "retry",
        )


def _error(report: dict[str, Any]) -> UrsulaError:
    """The package's error for what a resolve failed with, as ``tonbo_resolve`` and
    ``ursula_cancel`` report it: a :class:`TonboError` for its kinds, the class the report's
    ``class`` field names otherwise. The message is the extension's,
    ``<kind>: <what>[ (request <id>)][. <fix>]``."""
    kind = str(report["kind"])
    message = str(report["message"])
    if kind not in TONBO_KINDS:
        return error_of(report, message, None)
    fix = report.get("fix")
    request_id = report.get("request_id")
    return TonboError(
        message,
        kind,
        fix=fix if isinstance(fix, str) else None,
        request_id=request_id if isinstance(request_id, str) else None,
    )


def _resolve(
    name_or_url: object,
    api_key: object,
    api_origin: object,
    auth_origin: object,
    token: object,
    timeout: float,
    deadline: float,
) -> _Resolve:
    target, options = _options(name_or_url, api_key, api_origin, auth_origin, token)
    _control.start()
    return _Resolve(target, options, timeout, deadline)


def resolve_database(
    name_or_url: str,
    *,
    api_key: str | None = None,
    api_origin: str | None = None,
    auth_origin: str | None = None,
    token: str | None = None,
    timeout: float = DEFAULT_OPEN_S,
) -> ResolvedDatabase:
    """Resolves the Tonbo database ``name_or_url`` names into what opens it, as :func:`open` does
    before it opens the stream: its stream URL, its name, and the ``token`` to open it with. Hand
    them to another opener, such as the root module's ``sqlite_ursula.open(url, token=token)``.

    A name is looked up in the Management API with the account's API key, and a URL
    (``https://...``) is taken as it is. Unless ``token`` or ``URSULA_VFS_TOKEN_FILE`` serves the
    Streams requests, the key becomes the process's source of Streams tokens, which the extension
    exchanges and keeps fresh, and the call returns once it holds the first one: a key the token
    endpoint refuses fails here, and the ``token`` handed out is ``None``, since the key's tokens
    serve the open. A ``token`` is handed out as it was given, and then a URL needs no key and the
    resolve sends nothing. Concurrent calls with the same arguments, from any thread or task, share
    one resolve. It blocks the calling thread for at most ``timeout`` seconds (30 by default).

    Raises as :func:`open` does before the open of the stream.
    """
    limit = seconds("timeout", timeout, positive=True)
    resolve = _resolve(
        name_or_url, api_key, api_origin, auth_origin, token, limit, time.monotonic() + limit
    )
    answer = _events.wait_for(resolve)
    return _handed_out(answer, token)


async def aresolve_database(
    name_or_url: str,
    *,
    api_key: str | None = None,
    api_origin: str | None = None,
    auth_origin: str | None = None,
    token: str | None = None,
    timeout: float = DEFAULT_OPEN_S,
) -> ResolvedDatabase:
    """:func:`resolve_database` for asyncio: the same arguments, answer and errors, and it waits on
    the event loop instead of blocking it. Cancelling it, as ``asyncio.timeout`` around it does,
    leaves the resolve to the other calls that share it, and the last one cancels it."""
    limit = seconds("timeout", timeout, positive=True)
    resolve = _resolve(
        name_or_url, api_key, api_origin, auth_origin, token, limit, time.monotonic() + limit
    )
    answer = await _events.await_for(resolve)
    return _handed_out(answer, token)


def _handed_out(answer: dict[str, Any], token: object) -> ResolvedDatabase:
    name = answer.get("name")
    return ResolvedDatabase(
        url=str(answer["url"]),
        name=name if isinstance(name, str) else None,
        token=token if isinstance(token, str) else None,
    )


def open(  # noqa: A001 - the module's entry point, as Node's `open`
    name_or_url: str,
    *,
    api_key: str | None = None,
    api_origin: str | None = None,
    auth_origin: str | None = None,
    token: str | None = None,
    dir: str | os.PathLike[str] | None = None,  # noqa: A002 - the option's name, as Node's
    read_only: bool = False,
    recreate: bool = False,
    max_pending_bytes: int | None = None,
    timeout: float = DEFAULT_OPEN_S,
    busy_timeout: float = DEFAULT_BUSY_S,
    **connect_kwargs: Any,
) -> UrsulaConnection:
    """Opens the Tonbo database ``name_or_url`` names, as ``sqlite_ursula.open`` does, and returns
    its :class:`UrsulaConnection`. A name is looked up in the Management API with the account's API
    key. A URL (``https://...``) skips the lookup and opens that stream.

    - ``api_key``: the Tonbo API key. By default ``TONBO_API_KEY``, else the key ``tonbo login``
      saved.
    - ``api_origin``: the Management API's origin. By default ``TONBO_API_ORIGIN``, else
      ``https://api.tonbo.dev``.
    - ``auth_origin``: the token endpoint's origin. By default ``TONBO_AUTH_ORIGIN``, else
      ``https://tonbo.dev``.
    - ``token``: serves the Streams requests instead of tokens exchanged for the key, and so does
      ``URSULA_VFS_TOKEN_FILE``: then no key is needed for a URL.
    - ``timeout`` bounds the whole open, the lookup included, 30 seconds by default. The other
      arguments are the root module's ``open``'s.

    Streams requests carry tokens exchanged for the key, which the extension keeps fresh in the
    background, unless ``token`` or ``URSULA_VFS_TOKEN_FILE`` gives one. The process holds one key
    for Streams tokens: once it held one, another key that would fetch them fails
    ``invalid_argument`` while any database of the process may send requests, and for 60 s after a
    resolve was handed its token. A database still being created is waited for.

    Raises :class:`TonboError` for a key or database to fix, :class:`UrsulaUnavailableError` when
    the timeout passed while the Tonbo API did not answer (``unreachable``, ``unavailable`` or
    ``rate_limited``, action ``retry``, or ``cancelled`` for a call that left the resolve to other
    calls that share it), :class:`UrsulaSetupError` ``invalid_argument`` for an
    argument it cannot use, an answer of the Tonbo API it cannot read or a second key,
    ``invalid_url`` for a stream URL the open cannot take or that would get the key's Streams
    tokens over plain http to another host, and the root module's errors from the open of the
    stream.
    """
    target, options = _options(name_or_url, api_key, api_origin, auth_origin, token)
    checked = _open.plan(
        token,
        dir,
        read_only,
        recreate,
        max_pending_bytes,
        timeout,
        busy_timeout,
        connect_kwargs,
    )
    deadline = time.monotonic() + checked.timeout
    _control.start()
    answer = _events.wait_for(_Resolve(target, options, checked.timeout, deadline))
    return _open.opened(str(answer["url"]), checked, deadline)


async def aopen(
    name_or_url: str,
    *,
    api_key: str | None = None,
    api_origin: str | None = None,
    auth_origin: str | None = None,
    token: str | None = None,
    dir: str | os.PathLike[str] | None = None,  # noqa: A002 - the option's name, as Node's
    read_only: bool = False,
    recreate: bool = False,
    max_pending_bytes: int | None = None,
    timeout: float = DEFAULT_OPEN_S,
    busy_timeout: float = DEFAULT_BUSY_S,
    **connect_kwargs: Any,
) -> UrsulaConnection:
    """:func:`open` for asyncio: the same arguments, the same :class:`UrsulaConnection` and the same
    errors, and it waits for the resolve and the bind on the event loop instead of blocking it.

    Cancelling it, as ``asyncio.timeout`` around it does, cancels the resolve when no other call
    shares it, or the bind, and raises ``CancelledError``.
    """
    target, options = _options(name_or_url, api_key, api_origin, auth_origin, token)
    checked = _open.plan(
        token,
        dir,
        read_only,
        recreate,
        max_pending_bytes,
        timeout,
        busy_timeout,
        connect_kwargs,
    )
    deadline = time.monotonic() + checked.timeout
    _control.start()
    answer = await _events.await_for(_Resolve(target, options, checked.timeout, deadline))
    return await _open.aopened(str(answer["url"]), checked, deadline)
