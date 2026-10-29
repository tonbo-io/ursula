"""``gate`` and ``agate``: the output gate as a context manager (design: Gates).

The block runs, and leaving it waits for a ``synced()`` call made after the block ended, on every
database given, before the result or the block's error goes on. ``synced()`` is the gate: this
module only decides when to call it and which errors not to hold, and adds no call of the
extension but ``classify`` for an error a framework's own connection raised.

- An :class:`UrsulaError` the block raises passes at once, also when a library wrapped it (as
  ``__cause__``, as SQLAlchemy's ``.orig``, or in an ``ExceptionGroup``): it carries no
  application state. A refused write committed nothing, and a reset is what the wait would report.
  So does a SQLite error the extension classifies (``ursula_classify``) on one of the databases,
  which a framework's own ``sqlite3`` connection raised unclassified: it raises as its
  :class:`UrsulaError`.
- Every other ``Exception`` waits as a result does, then passes. A ``BaseException`` that is not an
  ``Exception`` (Ctrl-C, a cancellation, ``SystemExit``) passes at once.
- A reset during the wait raises the :class:`UrsulaResetError`, which replaces the outcome.
- ``timeout`` bounds the wait, not the block: past it, ``TimeoutError``, while the commits keep
  replicating.
"""

from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import time
from collections.abc import AsyncIterator
from collections.abc import Iterator

from ._args import invalid
from ._args import seconds
from ._connection import UrsulaConnection
from .errors import UrsulaError


def _databases(dbs: tuple[object, ...]) -> tuple[UrsulaConnection, ...]:
    if not dbs:
        raise invalid("gate needs at least one database")
    checked: list[UrsulaConnection] = []
    for db in dbs:
        if not isinstance(db, UrsulaConnection) or not db._bound:
            raise invalid(f"gate takes connections open() made, not {db!r}")
        checked.append(db)
    return tuple(checked)


def _limit(timeout: object) -> float | None:
    return None if timeout is None else seconds("timeout", timeout, positive=False)


def _held(error: BaseException, dbs: tuple[UrsulaConnection, ...]) -> BaseException | None:
    """What passes at once instead of waiting: ``error`` itself when it is or wraps an
    :class:`UrsulaError`, the :class:`UrsulaError` of a SQLite error it is or wraps that the
    extension classifies on one of ``dbs``, or ``None`` when the error waits."""
    found: list[BaseException] = []
    pending = [error]
    seen: set[int] = set()
    while pending:
        next_ = pending.pop()
        if id(next_) in seen:
            continue
        seen.add(id(next_))
        if isinstance(next_, UrsulaError):
            return error
        found.append(next_)
        if next_.__cause__ is not None:
            pending.append(next_.__cause__)
        orig = getattr(next_, "orig", None)
        if isinstance(orig, BaseException):
            pending.append(orig)
        if isinstance(next_, BaseExceptionGroup):
            pending.extend(next_.exceptions)
    for wrapped in found:
        if isinstance(wrapped, sqlite3.Error):
            for db in dbs:
                classified = db._classify(wrapped)
                if isinstance(classified, UrsulaError):
                    return classified
    return None


def _gave_up(timeout: float, dbs: tuple[UrsulaConnection, ...]) -> TimeoutError:
    names = ", ".join(db.name for db in dbs)
    return TimeoutError(
        f"the gate gave up after {timeout:g} s waiting for {names}: the commits keep replicating"
    )


@contextlib.contextmanager
def gate(*dbs: UrsulaConnection, timeout: float | None = None) -> Iterator[None]:
    """Runs the block, then lets its outcome go on only once a ``synced()`` call made after the
    block ended returned on every database in ``dbs``, so nothing outside the process acts on a
    result, or an error, that rests on an unacknowledged commit or on ownership this process is
    about to lose. A block that only read waits too: it may have read another connection's pending
    commit::

        with sqlite_ursula.gate(db):
            reply = handle(request)
        send(reply)

    An :class:`UrsulaError` the block raises, also wrapped, passes at once, and so does the error
    of a framework's own connection that the extension classifies. Every other error waits, then
    passes. A reset during the wait raises the :class:`UrsulaResetError`. ``timeout`` (seconds,
    ``None``: no limit) bounds the wait and raises ``TimeoutError``. Effects inside the block, such
    as an outbound call, are not gated: call ``db.synced()`` before them.
    """
    targets = _databases(dbs)
    limit = _limit(timeout)
    try:
        yield
    except Exception as error:
        held = _held(error, targets)
        if held is error:
            raise
        if held is not None:
            raise held from held.__cause__
        _wait(targets, limit)
        raise
    _wait(targets, limit)


def _wait(dbs: tuple[UrsulaConnection, ...], timeout: float | None) -> None:
    """``synced()`` on every database, within one ``timeout``."""
    deadline = None if timeout is None else time.monotonic() + timeout
    for db in dbs:
        left = None if deadline is None else max(0.0, deadline - time.monotonic())
        try:
            db.synced(left)
        except TimeoutError as error:
            assert timeout is not None
            raise _gave_up(timeout, dbs) from error


@contextlib.asynccontextmanager
async def agate(*dbs: UrsulaConnection, timeout: float | None = None) -> AsyncIterator[None]:
    """:func:`gate` for asyncio: ``async with agate(db):``. It waits with ``asynced()`` on every
    database at once, and a failure on one ends the waits on the others. Cancelling the task ends
    the wait."""
    targets = _databases(dbs)
    limit = _limit(timeout)
    try:
        yield
    except Exception as error:
        held = _held(error, targets)
        if held is error:
            raise
        if held is not None:
            raise held from held.__cause__
        await _await(targets, limit)
        raise
    await _await(targets, limit)


async def _await(dbs: tuple[UrsulaConnection, ...], timeout: float | None) -> None:
    """``asynced()`` on every database at once, within one ``timeout``: the first failure raises
    and ends the other waits, which have dropped their tickets by the time it returns."""
    waits = [asyncio.ensure_future(db.asynced()) for db in dbs]
    try:
        async with asyncio.timeout(timeout):
            for done in asyncio.as_completed(waits):
                await done
    except TimeoutError as error:
        assert timeout is not None
        raise _gave_up(timeout, dbs) from error
    finally:
        for wait in waits:
            wait.cancel()
        # Their own outcomes are not wanted once one failed.
        await asyncio.gather(*waits, return_exceptions=True)
