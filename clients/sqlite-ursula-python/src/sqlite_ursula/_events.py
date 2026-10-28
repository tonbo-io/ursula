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

The thread takes its cursor before any request or ticket is handed out, and requests and tickets
are registered under the lock in the same step that asks for them, so no event about one applies
before it is registered. A thread that fails is replaced, handing its pending tickets to the new
one as a gap would. After three failures in a row, waits fail.
"""

from __future__ import annotations

import logging
import os
import threading
import weakref
from collections.abc import Callable
from typing import TYPE_CHECKING
from typing import Any

from . import _control
from .errors import UrsulaResetError
from .errors import UrsulaSetupError
from .errors import error_of

if TYPE_CHECKING:
    from ._connection import UrsulaConnection

log = logging.getLogger("sqlite_ursula")

#: After this many failures of the thread in a row, it stays down and its waits fail.
MAX_FAILURES = 3
#: How long one ``ursula_events`` call waits for an event.
POLL_MS = 1000


class Ticket:
    """A ``synced()`` call waiting for its ticket."""

    __slots__ = ("done", "error", "path")

    def __init__(self, path: str) -> None:
        self.path = path
        self.done = threading.Event()
        self.error: BaseException | None = None

    def resolve(self) -> None:
        self.done.set()

    def reject(self, error: BaseException) -> None:
        self.error = error
        self.done.set()


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
        self._opens: dict[int, set[threading.Event]] = {}
        self._failed = False
        self.conn: Any = None
        self._thread = threading.Thread(target=self._run, name="sqlite-ursula-events", daemon=True)
        self._thread.start()

    def wait_open(self, request: int) -> threading.Event:
        """A waiter for open ``request``, woken by its ``opened`` event, a gap, or the thread's end.
        Called with the lock held, in the step that got the request."""
        waiter = threading.Event()
        self._opens.setdefault(request, set()).add(waiter)
        return waiter

    def forget_open(self, request: int, waiter: threading.Event) -> None:
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

    def _fail(self) -> None:
        """The thread is gone: pending opens ask again, and pending tickets move to a new thread.
        After :data:`MAX_FAILURES` failures in a row the tickets fail instead, and so does every
        later call."""
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
            if not _tickets:
                return
            try:
                events()
            except Exception as error:
                pending = list(_tickets.values())
                _tickets.clear()
                for t in pending:
                    t.reject(error)
                return
            _reissue()


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


def synced(path: str, timeout: float | None) -> None:
    """Waits until the stream holds every commit of the process logged before the call, for at
    most ``timeout`` seconds (``None``: no limit). Raises ``TimeoutError`` on the timeout, and the
    reset error on a reset."""
    with _control.lock():
        # Started before the call, so its events cursor precedes the ticket.
        events()
        answer = _control.call("SELECT ursula_synced(?, 0)", path)
        if answer["synced"]:
            return
        t = Ticket(path)
        _tickets[int(answer["ticket"])] = t
    try:
        if not t.done.wait(timeout):
            raise TimeoutError(
                f"synced() gave up after {timeout} s: the commits keep replicating, so call it "
                "again to wait for them"
            )
    except BaseException:
        with _control.lock():
            for ticket, pending in list(_tickets.items()):
                if pending is t:
                    del _tickets[ticket]
        raise
    if t.error is not None:
        raise t.error


def wait_open(call: Callable[[], Any]) -> tuple[Any, Events | None, threading.Event | None]:
    """Runs ``call`` (an ``ursula_open`` with timeout 0) with the events thread started and the lock
    held: its answer, and while it goes on, the thread and a waiter for its request."""
    with _control.lock():
        ev = events()
        opened = call()
        if opened["done"]:
            return opened, None, None
        return opened, ev, ev.wait_open(int(opened["request"]))


def _after_fork_in_child() -> None:
    global _current, _failures
    if _current is not None:
        _control._inherited.append(_current)
    _current = None
    _failures = 0
    _tickets.clear()
    _targets.clear()


os.register_at_fork(after_in_child=_after_fork_in_child)
