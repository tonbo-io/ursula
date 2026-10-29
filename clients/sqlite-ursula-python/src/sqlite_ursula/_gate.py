"""``gate`` and ``agate``: the output gate as a context manager (design: Gates).

The block runs, and leaving it waits for a ``synced()`` call made after the block ended, on every
database given, before the result or the block's error goes on. ``synced()`` is the gate: this
module only decides when to wait and which errors not to hold, and adds no call of the extension
but ``classify`` for an error a framework's own connection raised.

- An :class:`UrsulaError` the block raises passes at once, as the block raised it, also when a
  library wrapped it (as ``__cause__``, as SQLAlchemy's ``.orig``, or in an ``ExceptionGroup``):
  it carries no application state. A refused write committed nothing, and a reset is what the wait
  would report. So does a SQLite error, also wrapped, that the extension classifies
  (``ursula_classify``) on one of the databases, which a framework's own ``sqlite3`` connection
  raised unclassified. A bare SQLite error that one database's state names a refused write or a
  reset raises as that :class:`UrsulaError`, which is a ``sqlite3.OperationalError`` too. With
  several databases a statement's I/O error does not say whose it is, so it passes unchanged.
- Every other ``Exception`` waits as a result does, then passes. A ``BaseException`` that is not an
  ``Exception`` (Ctrl-C, a cancellation, ``SystemExit``) passes at once.
- The wait covers every database at once, and the first failure, such as a reset, ends it and
  replaces the outcome.
- ``timeout`` bounds the wait, not the block: past it, ``TimeoutError``, while the commits keep
  replicating.
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import AsyncIterator
from collections.abc import Iterator

from . import _control
from . import _events
from ._args import invalid
from ._args import limit
from ._connection import UrsulaConnection
from ._connection import bound
from .errors import UrsulaError
from .errors import UrsulaResetError
from .errors import UrsulaWriteRefusedError


def _databases(dbs: tuple[object, ...]) -> tuple[UrsulaConnection, ...]:
    if not dbs:
        raise invalid("gate needs at least one database")
    return tuple(bound(db, "gate") for db in dbs)


def _passing(error: Exception, dbs: tuple[UrsulaConnection, ...]) -> BaseException | None:
    """What raises at once instead of waiting, or ``None`` when ``error`` waits: ``error`` itself
    when its chain holds an :class:`UrsulaError` or a SQLite error the extension classifies, or
    the refused write or reset that a bare SQLite error is on the one database given."""
    classified = _control.classify_wrapped(error, [db.path for db in dbs])
    if not isinstance(classified, UrsulaError):
        return None
    if (
        len(dbs) == 1
        and classified.__cause__ is error
        and isinstance(error, sqlite3.Error)
        and isinstance(classified, (UrsulaWriteRefusedError, UrsulaResetError))
    ):
        return classified
    return error


def _gave_up(timeout: float | None, dbs: tuple[UrsulaConnection, ...]) -> TimeoutError:
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

    An :class:`UrsulaError` the block raises, also wrapped, passes at once as it was raised, and so
    does the error of a framework's own connection that the extension classifies. Every other error
    waits, then passes. The wait covers every database at once, and the first failure on one, such
    as a reset, raises at once. ``timeout`` (seconds, ``None``: no limit) bounds the wait and raises
    ``TimeoutError``. Effects inside the block, such as an outbound call, are not gated: call
    ``db.synced()`` before them.
    """
    targets = _databases(dbs)
    bound = limit(timeout)
    try:
        yield
    except Exception as error:
        passing = _passing(error, targets)
        if passing is error:
            raise
        if passing is not None:
            raise passing from error
        _wait(targets, bound)
        raise
    _wait(targets, bound)


def _wait(dbs: tuple[UrsulaConnection, ...], timeout: float | None) -> None:
    try:
        _events.synced(dbs, timeout)
    except TimeoutError as error:
        raise _gave_up(timeout, dbs) from error


@contextlib.asynccontextmanager
async def agate(*dbs: UrsulaConnection, timeout: float | None = None) -> AsyncIterator[None]:
    """:func:`gate` for asyncio: ``async with agate(db):``. It waits on the event loop instead of
    blocking it, and cancelling the task ends the wait."""
    targets = _databases(dbs)
    bound = limit(timeout)
    try:
        yield
    except Exception as error:
        passing = _passing(error, targets)
        if passing is error:
            raise
        if passing is not None:
            raise passing from error
        await _await(targets, bound)
        raise
    await _await(targets, bound)


async def _await(dbs: tuple[UrsulaConnection, ...], timeout: float | None) -> None:
    try:
        await _events.asynced(dbs, timeout)
    except TimeoutError as error:
        raise _gave_up(timeout, dbs) from error
