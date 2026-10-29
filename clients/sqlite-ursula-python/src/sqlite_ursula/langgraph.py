"""LangGraph's SQLite checkpointers and stores on a replicated database (design: LangGraph).

The only module of the package that imports LangGraph, through the ``langgraph`` extra
(``pip install 'sqlite-ursula[langgraph]'``), which pins ``langgraph-checkpoint-sqlite`` to the
releases CI tests. Each class is a subclass of LangGraph's own, unmodified but for its writes::

    from sqlite_ursula.langgraph import UrsulaSaver

    db = sqlite_ursula.open(url)
    graph = builder.compile(checkpointer=UrsulaSaver(db))
    reply = graph.invoke(inputs, {"configurable": {"thread_id": "1"}})
    send(reply)  # every checkpoint of the run is in the stream

- :class:`UrsulaSaver` is ``SqliteSaver`` on ``db`` itself, and :class:`UrsulaStore` is
  ``SqliteStore`` on ``db``, which needs a connection without implicit transactions, as
  ``SqliteStore``'s own are: ``open(url, isolation_level=None)``.
- :class:`AsyncUrsulaSaver` and :class:`AsyncUrsulaStore` are ``AsyncSqliteSaver`` and
  ``AsyncSqliteStore`` on an aiosqlite connection to ``db.path`` that ``open(db)`` makes. That
  connection is on ``db``'s binding as ``db`` is, so its errors are the package's too.
  ``aclose()``, or ``async with``, closes it.

Every write returns only once ``db.synced()`` (``asynced()``) returned after it: the setup that
creates the tables, which LangGraph runs before the first read too, a checkpoint, a task's writes,
a deleted thread, a store batch with a put or a delete, a TTL sweep. So a graph's
``invoke`` returns once the checkpoints of its run are in the stream, since LangGraph waits for its
background checkpoint writes before it returns. Reads do not wait. A write refused with
``backlog_full`` committed nothing: it waits for ``synced()`` and runs again. A reset raises the
:class:`~sqlite_ursula.UrsulaResetError` itself, from a write or a read. Keep ``db`` open while the
graph runs.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Self
from typing import TypeVar

import aiosqlite
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.store.base import Op
from langgraph.store.base import PutOp
from langgraph.store.base import Result
from langgraph.store.sqlite import AsyncSqliteStore
from langgraph.store.sqlite import SqliteStore

from ._args import invalid
from ._connection import UrsulaConnection
from ._connection import bound
from ._connection import on_binding
from .errors import UrsulaSetupError
from .errors import UrsulaWriteRefusedError

__all__ = ["AsyncUrsulaSaver", "AsyncUrsulaStore", "UrsulaSaver", "UrsulaStore"]

T = TypeVar("T")


def _backlog_full(error: UrsulaWriteRefusedError) -> bool:
    return error.kind == "backlog_full"


def _gated(db: UrsulaConnection, write: Callable[..., T], *args: Any) -> T:
    """``write(*args)``'s result once ``db.synced()`` returned after it. A ``backlog_full`` refusal
    committed nothing, and the extension admits any commit while nothing is pending: it runs again
    once ``synced()`` returned."""
    while True:
        try:
            result = write(*args)
        except UrsulaWriteRefusedError as error:
            if not _backlog_full(error):
                raise
        else:
            db.synced()
            return result
        db.synced()


async def _agated(db: UrsulaConnection, write: Callable[..., Awaitable[T]], *args: Any) -> T:
    """:func:`_gated` for a coroutine function, on ``db.asynced()``."""
    while True:
        try:
            result = await write(*args)
        except UrsulaWriteRefusedError as error:
            if not _backlog_full(error):
                raise
        else:
            await db.asynced()
            return result
        await db.asynced()


def _no_conn_string(cls: type, use: str) -> UrsulaSetupError:
    return invalid(f"{cls.__name__} replicates a database open() made, not a file: use {use}")


def _autocommits(db: UrsulaConnection) -> bool:
    """Whether ``db`` opens no transaction of its own: ``isolation_level=None``, or
    ``autocommit=True`` (Python 3.12 or newer)."""
    legacy = getattr(sqlite3, "LEGACY_TRANSACTION_CONTROL", None)
    mode = getattr(db, "autocommit", legacy)
    return mode is True or (mode == legacy and db.isolation_level is None)


async def _connect(db: UrsulaConnection, **kwargs: Any) -> aiosqlite.Connection:
    """An aiosqlite connection on ``db``'s binding."""
    database, binding = on_binding(db)
    return await aiosqlite.connect(database, **binding, **kwargs)


def _writes(ops: Sequence[Op]) -> bool:
    """Whether a store batch writes: a put or a delete."""
    return any(isinstance(op, PutOp) for op in ops)


class UrsulaSaver(SqliteSaver):
    """LangGraph's ``SqliteSaver`` on ``db``, a connection ``open()`` made: ``put``,
    ``put_writes`` and ``delete_thread`` return once ``db.synced()`` returned after them, and a
    ``backlog_full`` refusal runs again after ``synced()``.

    LangGraph runs a graph's checkpoint writes in the background and waits for them before
    ``invoke`` returns, so ``invoke``'s return is gated in every durability mode.
    ``durability="sync"`` also gates what a node does after the step before it, at one round trip
    per step.
    """

    def __init__(self, db: UrsulaConnection, *, serde: SerializerProtocol | None = None) -> None:
        self.db = bound(db, type(self).__name__)
        super().__init__(self.db, serde=serde)

    @classmethod
    def from_conn_string(cls, conn_string: str) -> Any:
        raise _no_conn_string(cls, f"{cls.__name__}(sqlite_ursula.open(url))")

    def setup(self) -> None:
        # LangGraph runs it lazily, before a read too: its tables are a write like any other.
        if not self.is_setup:
            _gated(self.db, super().setup)

    def put(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        return _gated(self.db, super().put, config, checkpoint, metadata, new_versions)

    def put_writes(
        self, config: Any, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        _gated(self.db, super().put_writes, config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        _gated(self.db, super().delete_thread, thread_id)


class AsyncUrsulaSaver(AsyncSqliteSaver):
    """LangGraph's ``AsyncSqliteSaver`` on ``db``'s database, through an aiosqlite connection on
    its binding: ``aput``, ``aput_writes`` and ``adelete_thread`` return once ``db.asynced()``
    returned after them, and a ``backlog_full`` refusal runs again after ``asynced()``. Its
    synchronous calls, which LangGraph runs from other threads, are those.

    Make it with :meth:`open`, and close it with :meth:`aclose` or ``async with``.
    """

    def __init__(
        self,
        db: UrsulaConnection,
        conn: aiosqlite.Connection,
        *,
        serde: SerializerProtocol | None = None,
    ) -> None:
        """``conn`` is the aiosqlite connection :meth:`open` made on ``db``'s binding."""
        self.db = bound(db, type(self).__name__)
        super().__init__(conn, serde=serde)

    @classmethod
    async def open(cls, db: UrsulaConnection, *, serde: SerializerProtocol | None = None) -> Self:
        """The saver of ``db``, a connection ``open()`` or ``aopen()`` made."""
        checked = bound(db, cls.__name__)
        conn = await _connect(checked)
        try:
            return cls(checked, conn, serde=serde)
        except BaseException:
            await conn.close()
            raise

    @classmethod
    def from_conn_string(cls, conn_string: str) -> Any:
        raise _no_conn_string(cls, f"await {cls.__name__}.open(sqlite_ursula.open(url))")

    async def setup(self) -> None:
        # LangGraph runs it lazily, before a read too: its tables are a write like any other.
        if not self.is_setup:
            await _agated(self.db, super().setup)

    async def aput(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        return await _agated(self.db, super().aput, config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self, config: Any, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        await _agated(self.db, super().aput_writes, config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        await _agated(self.db, super().adelete_thread, thread_id)

    async def aclose(self) -> None:
        """Closes the aiosqlite connection. ``db`` stays open."""
        await self.conn.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


class UrsulaStore(SqliteStore):
    """LangGraph's ``SqliteStore`` on ``db``, a connection ``open()`` made without implicit
    transactions (``isolation_level=None``), as ``SqliteStore``'s own connections are: a batch with
    a put or a delete, and a TTL sweep, return once ``db.synced()`` returned after them, and a
    ``backlog_full`` refusal runs again after ``synced()``."""

    def __init__(
        self,
        db: UrsulaConnection,
        *,
        deserializer: Callable[[Any], dict[str, Any]] | None = None,
        index: Any = None,
        ttl: Any = None,
    ) -> None:
        checked = bound(db, type(self).__name__)
        if not _autocommits(checked):
            raise invalid(
                f"{type(self).__name__} runs its own BEGIN and COMMIT, so it needs a connection "
                "without implicit transactions: open(url, isolation_level=None)"
            )
        self.db = checked
        super().__init__(checked, deserializer=deserializer, index=index, ttl=ttl)

    @classmethod
    def from_conn_string(cls, conn_string: str, **kwargs: Any) -> Any:
        raise _no_conn_string(cls, f"{cls.__name__}(sqlite_ursula.open(url, isolation_level=None))")

    def setup(self) -> None:
        # LangGraph runs it lazily, before a read too: its tables are a write like any other.
        if not self.is_setup:
            _gated(self.db, super().setup)

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        listed = list(ops)
        if not _writes(listed):
            return super().batch(listed)
        return _gated(self.db, super().batch, listed)

    def sweep_ttl(self) -> int:
        return _gated(self.db, super().sweep_ttl)


class AsyncUrsulaStore(AsyncSqliteStore):
    """LangGraph's ``AsyncSqliteStore`` on ``db``'s database, through an aiosqlite connection on
    its binding without implicit transactions: a batch with a put or a delete, and a TTL sweep,
    return once ``db.asynced()`` returned after them, and a ``backlog_full`` refusal runs again
    after ``asynced()``.

    Make it with :meth:`open`, and close it with :meth:`aclose` or ``async with``, which also stop
    its TTL sweeper.
    """

    def __init__(
        self,
        db: UrsulaConnection,
        conn: aiosqlite.Connection,
        *,
        deserializer: Callable[[Any], dict[str, Any]] | None = None,
        index: Any = None,
        ttl: Any = None,
    ) -> None:
        """``conn`` is the aiosqlite connection :meth:`open` made on ``db``'s binding."""
        self.db = bound(db, type(self).__name__)
        super().__init__(conn, deserializer=deserializer, index=index, ttl=ttl)

    @classmethod
    async def open(
        cls,
        db: UrsulaConnection,
        *,
        deserializer: Callable[[Any], dict[str, Any]] | None = None,
        index: Any = None,
        ttl: Any = None,
    ) -> Self:
        """The store of ``db``, a connection ``open()`` or ``aopen()`` made, whatever its own
        transaction control."""
        checked = bound(db, cls.__name__)
        conn = await _connect(checked, isolation_level=None)
        try:
            return cls(checked, conn, deserializer=deserializer, index=index, ttl=ttl)
        except BaseException:
            await conn.close()
            raise

    @classmethod
    def from_conn_string(cls, conn_string: str, **kwargs: Any) -> Any:
        raise _no_conn_string(cls, f"await {cls.__name__}.open(sqlite_ursula.open(url))")

    async def setup(self) -> None:
        # LangGraph runs it lazily, before a read too: its tables are a write like any other.
        if not self.is_setup:
            await _agated(self.db, super().setup)

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        listed = list(ops)
        if not _writes(listed):
            return await super().abatch(listed)
        return await _agated(self.db, super().abatch, listed)

    async def sweep_ttl(self) -> int:
        return await _agated(self.db, super().sweep_ttl)

    async def aclose(self) -> None:
        """Stops the TTL sweeper and closes the aiosqlite connection. ``db`` stays open."""
        await self.stop_ttl_sweeper()
        await self.conn.close()

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
