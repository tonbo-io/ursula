"""The events thread (design: Pump): this process's view of the extension's events.

Calls into the extension never wait on the network: ``ursula_open`` and ``ursula_synced`` are
made with timeout 0 and either answer at once or hand back a request or a ticket, which an event
later resolves. One daemon thread, ``sqlite-ursula-events``, started at the first open or
``synced()``, waits for those events on a connection of its own and applies them under the
control lock:

- ``synced {path, through}`` resolves every ticket of the path up to ``through``. The extension
  keeps only the latest watermark of a binding in its queue, so any number of waiters costs one
  event.
- ``sync_failed {ticket}`` fails that ticket alone.
- ``reset {path}`` fails the path's tickets, then runs the ``on_reset`` callbacks of its
  connections on a thread of their own, so a callback may call into the package.
- ``gap`` (events fell out of the queue) asks for every pending ticket again under a new ticket,
  wakes every open, and reads resets from status.
- ``opened {request}`` wakes the opens waiting for it, which call ``ursula_open`` again to
  collect the result.
- ``condition {path, condition: "unauthorized"}`` tells the token source (``_token``) that the
  replication of ``path`` was refused with 401: it drops the token held and calls its provider.
  ``retired`` lets it stop once nothing uses it, and a gap has it read the bound paths' status for
  a missed ``unauthorized``. Otherwise a ``retired``, another condition or a gap has it reconsider
  a retry it scheduled after the provider handed back a token those reporters have cleared.

A waiter is a ``threading.Event`` for ``synced()``, ``gate`` and ``open()``, and a
:class:`LoopEvent` for ``asynced()``, ``agate`` and ``aopen()``, which the thread wakes through its
event loop's ``call_soon_threadsafe``. Nothing runs on the loop's default executor. One wait
covers the tickets of every database it was given, with one waiter that each ticket's outcome
wakes: it ends once every ticket resolved, or at the first that failed, and drops the rest.

The thread takes its cursor before any request or ticket is handed out, and requests and tickets
are registered under the lock in the same step that asks for them, so no event about one applies
before it is registered. A thread that fails is replaced while tickets wait, connections are open
or a token provider serves the process, and the new one starts with a gap, since events may have
passed between the two. After three failures in a row, waits fail ``internal``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import threading
import time
import weakref
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any

from . import _args
from . import _control
from . import _token
from .errors import UrsulaResetError
from .errors import UrsulaSetupError
from .errors import again
from .errors import error_of

if TYPE_CHECKING:
    from ._connection import UrsulaConnection

log = logging.getLogger("sqlite_ursula")

#: After this many failures of the thread in a row, it stays down and its waits fail.
MAX_FAILURES = 3
#: How long one ``ursula_events`` call waits for an event.
POLL_MS = 1000


class LoopEvent:
    """What a coroutine waits on where a thread waits on a ``threading.Event``: :meth:`set` from any
    thread wakes it on its event loop. Made on the loop, by the coroutine that waits."""

    __slots__ = ("_future", "_loop")

    def __init__(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._future: asyncio.Future[None] = self._loop.create_future()

    def set(self) -> None:
        # A loop that closed raises RuntimeError: nothing waits on it any more.
        with contextlib.suppress(RuntimeError):
            self._loop.call_soon_threadsafe(self._settle)

    def clear(self) -> None:
        """On the loop: the next :meth:`set` wakes the waiter again. A wake already on its way may
        still come, which only makes the waiter look again."""
        if self._future.done():
            self._future = self._loop.create_future()

    def _settle(self) -> None:
        # A waiter that gave up (its timeout, a cancellation) cancelled the future.
        if not self._future.done():
            self._future.set_result(None)

    async def wait(self, timeout: float | None) -> bool:
        """Whether it was set within ``timeout`` seconds (``None``: no limit). Cancelling the
        coroutine ends the wait."""
        try:
            async with asyncio.timeout(timeout):
                await self._future
        except TimeoutError:
            return False
        return True


Waiter = threading.Event | LoopEvent


class Wait:
    """One ``synced()``, ``asynced()`` or gate call waiting for the tickets of its databases. Its
    state changes under the lock, and each change wakes ``waiter``."""

    __slots__ = ("failed", "left", "waiter")

    def __init__(self, waiter: Waiter) -> None:
        self.waiter = waiter
        #: The tickets not resolved yet.
        self.left = 0
        #: The path and the error of the first ticket that failed.
        self.failed: tuple[str, BaseException] | None = None

    def done(self) -> bool:
        return self.failed is not None or self.left == 0


class Ticket:
    """The ticket of one database of a :class:`Wait`."""

    __slots__ = ("path", "wait")

    def __init__(self, path: str, wait: Wait) -> None:
        self.path = path
        self.wait = wait

    def resolve(self) -> None:
        self.wait.left -= 1
        self.wait.waiter.set()

    def reject(self, error: BaseException) -> None:
        if self.wait.failed is None:
            self.wait.failed = (self.path, error)
        self.wait.waiter.set()


#: The pending tickets of the process, by ticket: kept across threads.
_tickets: dict[int, Ticket] = {}
#: The open bound connections by bound path, whose callbacks a reset runs. Held weakly: a
#: connection the application dropped is collected.
_targets: dict[str, weakref.WeakSet[UrsulaConnection]] = {}
_current: Events | None = None
#: Threads that failed in a row, before any delivered events.
_failures = 0


def reset_error(named: dict[str, Any]) -> UrsulaResetError:
    """The error of the reset the extension named (a ``reset`` event, ``status()["reset"]``)."""
    return UrsulaResetError(str(named["message"]), str(named["kind"]), named["action"])


def track(target: UrsulaConnection) -> None:
    """Runs ``target``'s callbacks on the resets of its path."""
    with _control.lock():
        _targets.setdefault(target.path, weakref.WeakSet()).add(target)


def untrack(target: UrsulaConnection) -> None:
    with _control.lock():
        found = _targets.get(target.path)
        if found is not None:
            found.discard(target)
            if not found:
                del _targets[target.path]


def _live(path: str | None = None) -> list[UrsulaConnection]:
    """The tracked connections of ``path``, or of every path."""
    if path is None:
        return [t for found in _targets.values() for t in found]
    return list(_targets.get(path, ()))


def _fire(hit: list[tuple[UrsulaConnection, UrsulaResetError]]) -> None:
    """Runs the callbacks of the connections ``hit`` on a thread of their own."""
    if not hit:
        return

    def run() -> None:
        for target, error in hit:
            target._reset_by(error)

    threading.Thread(target=run, name="sqlite-ursula-reset", daemon=True).start()


def _reissue() -> None:
    """Asks for every pending ticket again (``ursula_synced``, timeout 0), under a new ticket.
    Called with the lock held."""
    pending = list(_tickets.values())
    _tickets.clear()
    for t in pending:
        try:
            again = _control.call("SELECT ursula_synced(?, 0)", t.path)
        except Exception as error:
            t.reject(error)
            continue
        if again["synced"]:
            t.resolve()
        else:
            _tickets[int(again["ticket"])] = t


class Events:
    """One events thread, from the cursor at its start."""

    def __init__(self) -> None:
        # From the cursor at this moment: every request and ticket handed out after it is
        # answered by an event this thread sees, and no event of an earlier binding is taken for a
        # later one's.
        self._cursor = int(_control.call("SELECT ursula_events(0, 0)")["cursor"])
        self._opens: dict[int, set[Waiter]] = {}
        self._failed = False
        self.conn: Any = None
        self._thread = threading.Thread(target=self._run, name="sqlite-ursula-events", daemon=True)
        self._thread.start()

    def wait_open(self, request: int, waiter: Waiter) -> None:
        """Wakes ``waiter`` for open ``request`` at its ``opened`` event, a gap, or the thread's
        end. Called with the lock held, in the step that got the request."""
        self._opens.setdefault(request, set()).add(waiter)

    def forget_open(self, request: int, waiter: Waiter) -> None:
        with _control.lock():
            found = self._opens.get(request)
            if found is not None:
                found.discard(waiter)
                if not found:
                    del self._opens[request]

    def _wake(self, request: int) -> None:
        for waiter in self._opens.pop(request, ()):
            waiter.set()

    def _run(self) -> None:
        global _failures
        try:
            self.conn = _control.connection()
            cursor = self._cursor
            while True:
                row = self.conn.execute("SELECT ursula_events(?, ?)", (cursor, POLL_MS)).fetchone()
                batch = _control.parsed(row[0])
                cursor = int(batch["cursor"])
                if batch["events"]:
                    with _control.lock():
                        if self._failed:
                            return
                        _failures = 0
                        self.handle(batch["events"])
        except Exception:
            self._fail()

    def handle(self, events: list[dict[str, Any]]) -> None:
        """A batch of events, in order. Called with the lock held."""
        for event in events:
            kind = event.get("event")
            if kind == "opened":
                self._wake(int(event["request"]))
            elif kind == "synced":
                through = int(event["through"])
                for ticket, t in list(_tickets.items()):
                    if t.path == event["path"] and ticket <= through:
                        del _tickets[ticket]
                        t.resolve()
            elif kind == "sync_failed":
                t = _tickets.pop(int(event["ticket"]), None)
                if t is not None:
                    t.reject(error_of(event, str(event["message"]), None))
            elif kind == "reset":
                self._reset(event.get("path"), event)
            elif kind == "gap":
                self._gap()
            elif kind == "condition":
                if event.get("condition") == "unauthorized":
                    path = event.get("path")
                    # No path: the binding that reported it is gone, so it blocks nothing any more.
                    _token.source.blocked([] if path is None else [str(path)])
                else:
                    _token.source.reconsider()
            elif kind == "retired":
                _token.source.retired()

    def _reset(self, path: str | None, named: dict[str, Any]) -> None:
        """A reset of ``path`` (``None``: a binding the process no longer has): its pending
        ``synced()`` calls fail, then its callbacks run."""
        hit: list[tuple[UrsulaConnection, UrsulaResetError]] = []
        if path is None:
            for target in _live():
                error = target._current_reset()
                if error is not None:
                    hit.append((target, error))
            paths = {target.path for target, _ in hit}
        else:
            hit = [(target, reset_error(named)) for target in _live(path)]
            paths = {path}
        for ticket, t in list(_tickets.items()):
            if t.path in paths:
                del _tickets[ticket]
                t.reject(reset_error(named))
        _fire(hit)

    def _gap(self) -> None:
        """Events fell out of the queue before the thread read them. Every pending ticket is asked
        for again under a new ticket, which no watermark of the batch covers, so a ticket whose
        failure the gap dropped never passes for resolved. Opens ask again, and resets are read
        from status."""
        for request in list(self._opens):
            self._wake(request)
        _reissue()
        hit = []
        for target in _live():
            error = target._current_reset()
            if error is not None:
                hit.append((target, error))
        _fire(hit)
        _token.source.missed()

    def _fail(self) -> None:
        """The thread is gone: pending opens ask again, and while tickets wait, connections are
        open or a token provider serves the process a new thread starts with a gap, since events
        may have passed between the two. After :data:`MAX_FAILURES` failures in a row the tickets
        fail instead, and so does every later call."""
        global _current, _failures
        with _control.lock():
            if self._failed:
                return
            self._failed = True
            _failures += 1
            if _current is self:
                _current = None
            for request in list(self._opens):
                self._wake(request)
            if not _tickets and not _live() and not _token.source.serving():
                return
            try:
                replacement = events()
            except Exception as error:
                pending = list(_tickets.values())
                _tickets.clear()
                for t in pending:
                    t.reject(error)
                return
            replacement._gap()


def events() -> Events:
    """The process's events thread, started at the first call that needs it."""
    global _current
    with _control.lock():
        if _current is None:
            if _failures >= MAX_FAILURES:
                raise UrsulaSetupError(
                    f"internal: the sqlite-ursula events thread failed {_failures} times in a "
                    "row: check the extension, then restart the process",
                    "internal",
                    "reopen",
                )
            _current = Events()
        return _current


def _ask(dbs: Sequence[UrsulaConnection], waiter: type[Waiter]) -> Wait | None:
    """Asks for a ticket of every database in ``dbs`` (``ursula_synced``, timeout 0), in one step
    under the lock: ``None`` when every one is synced already, else the :class:`Wait` of the
    tickets, registered in the same step, with a new ``waiter``."""
    for db in dbs:
        db._usable()
    with _control.lock():
        # Started before the calls, so its events cursor precedes the tickets.
        events()
        wait = Wait(waiter())
        try:
            for db in dbs:
                try:
                    answer = _control.call("SELECT ursula_synced(?, 0)", db.path)
                except UrsulaResetError as error:
                    db._remember(error)
                    raise
                if not answer["synced"]:
                    _tickets[int(answer["ticket"])] = Ticket(db.path, wait)
                    wait.left += 1
        except BaseException:
            _drop(wait)
            raise
    return wait if wait.left else None


def _drop(wait: Wait) -> None:
    """The call of ``wait`` ended: no event resolves its tickets any more."""
    with _control.lock():
        for ticket, pending in list(_tickets.items()):
            if pending.wait is wait:
                del _tickets[ticket]


def _settled(wait: Wait) -> bool:
    """Whether ``wait`` is over, else its waiter cleared for the next change: under the lock, so no
    change falls between the two."""
    with _control.lock():
        if wait.done():
            return True
        wait.waiter.clear()
        return False


def _left(deadline: float | None) -> float | None:
    return None if deadline is None else max(0.0, deadline - time.monotonic())


def _outcome(wait: Wait, dbs: Sequence[UrsulaConnection]) -> None:
    """Raises the first failure of ``wait``, if any, and the connections of its path remember a
    reset."""
    if wait.failed is None:
        return
    path, error = wait.failed
    if isinstance(error, UrsulaResetError):
        for db in dbs:
            if db.path == path:
                db._remember(error)
    raise again(error)


def _timed_out(call: str, timeout: float | None) -> TimeoutError:
    return TimeoutError(
        f"{call} gave up after {timeout} s: the commits keep replicating, so call it again to wait "
        "for them"
    )


def synced(dbs: Sequence[UrsulaConnection], timeout: float | None) -> None:
    """Waits until the stream of each database in ``dbs`` holds every commit the process logged on
    it before the call, for at most ``timeout`` seconds (``None``: no limit). Raises
    ``TimeoutError`` on the timeout, and the first failure, such as a reset, at once."""
    limit = _args.limit(timeout)
    wait = _ask(dbs, threading.Event)
    if wait is None:
        return
    assert isinstance(wait.waiter, threading.Event)
    deadline = None if limit is None else time.monotonic() + limit
    try:
        while not _settled(wait):
            left = _left(deadline)
            if left == 0.0:
                raise _timed_out("synced()", timeout)
            wait.waiter.wait(left)
    finally:
        _drop(wait)
    _outcome(wait, dbs)


async def asynced(dbs: Sequence[UrsulaConnection], timeout: float | None) -> None:
    """:func:`synced` for a coroutine: it waits on the event loop, and cancelling it ends the wait
    while the commits keep replicating."""
    limit = _args.limit(timeout)
    wait = _ask(dbs, LoopEvent)
    if wait is None:
        return
    assert isinstance(wait.waiter, LoopEvent)
    deadline = None if limit is None else time.monotonic() + limit
    try:
        while not _settled(wait):
            left = _left(deadline)
            if left == 0.0:
                raise _timed_out("asynced()", timeout)
            await wait.waiter.wait(left)
    finally:
        _drop(wait)
    _outcome(wait, dbs)


def _after_fork_in_child() -> None:
    global _current, _failures
    if _current is not None:
        _control._inherited.append(_current)
    # The parent's connections stay referenced, so collecting one never closes a bound file in
    # the child.
    _control._inherited.extend(_live())
    _current = None
    _failures = 0
    _tickets.clear()
    _targets.clear()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)
