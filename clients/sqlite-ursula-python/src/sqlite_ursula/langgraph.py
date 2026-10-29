"""LangGraph's SQLite checkpointers and stores on a replicated database (design: LangGraph).

The only module of the package that imports LangGraph, through the ``langgraph`` extra
(``pip install 'sqlite-ursula[langgraph]'``), which pins ``langgraph-checkpoint-sqlite`` to the
releases CI tests. Each class is a subclass of LangGraph's own, unmodified but for its connection
and its writes::

    from sqlite_ursula.langgraph import UrsulaSaver

    db = sqlite_ursula.open(url)
    with UrsulaSaver(db) as saver:
        graph = builder.compile(checkpointer=saver)
        reply = graph.invoke(inputs, {"configurable": {"thread_id": "1"}})
    send(reply)  # every checkpoint of the run is in the stream

Each class runs on a connection of its own on ``db``'s binding, which it opens and closes:
``close()`` or ``with`` for :class:`UrsulaSaver` and :class:`UrsulaStore`, ``aclose()`` or
``async with`` for :class:`AsyncUrsulaSaver` and :class:`AsyncUrsulaStore`, which ``open(db)``
makes on an aiosqlite connection. LangGraph writes checkpoints from threads of its own while the
graph's nodes run, so a connection the application also writes on would share its transactions.
The savers' connections keep SQLite's default transaction control, which makes each checkpoint,
each task's writes and each deleted thread one transaction, and the stores' open no transactions
of their own, since ``SqliteStore`` runs its own ``BEGIN`` and ``COMMIT``. Each is bound as ``db``
is, so its errors are the package's too.

Every write returns only once ``db.synced()`` (``asynced()``) returned after it: a checkpoint, a
task's writes, a deleted thread, a store put or delete, a store get or search that refreshes TTLs,
a TTL sweep. So a graph's ``invoke`` returns once the checkpoints of its run are in the stream,
since LangGraph waits for its background checkpoint writes before it returns. Reads do not wait,
and neither does the setup that creates the tables: the next write's wait covers it. A write
refused with ``backlog_full`` committed nothing: it waits for ``synced()`` and runs again, which
is safe because each of these writes is one transaction. LangGraph's setup commits its statements
one by one, and a store's migrations are not safe to run twice, so there each refused statement
waits and runs again by itself. A reset raises the :class:`~sqlite_ursula.UrsulaResetError`
itself, from a write or a read. Keep ``db`` open while the graph runs.
"""

from __future__ import annotations

import contextlib
import sqlite3
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from typing import Any
from typing import Self
from typing import TypeVar
from typing import cast

import aiosqlite
from langgraph.checkpoint.serde.base import SerializerProtocol
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.store.base import GetOp
from langgraph.store.base import Op
from langgraph.store.base import PutOp
from langgraph.store.base import Result
from langgraph.store.base import SearchOp
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

#: How long a store's ``close()`` or ``aclose()`` waits for its TTL sweeper to stop, since a sweep
#: that waits for the stream during an outage would not stop by itself. ``aclose()`` then cancels
#: it.
SWEEPER_STOP_S = 1.0


def _backlog_full(error: UrsulaWriteRefusedError) -> bool:
    return error.kind == "backlog_full"


def _gated(db: UrsulaConnection, writes: bool, call: Callable[..., T], *args: Any) -> T:
    """``call(*args)``'s result, once ``db.synced()`` returned after it when it ``writes``. A
    ``backlog_full`` refusal committed nothing, since ``call`` runs one transaction, and the
    extension admits any commit while nothing is pending: it runs again once ``synced()``
    returned."""
    while True:
        try:
            result = call(*args)
        except UrsulaWriteRefusedError as error:
            if not _backlog_full(error):
                raise
        else:
            if writes:
                db.synced()
            return result
        db.synced()


async def _agated(
    db: UrsulaConnection, writes: bool, call: Callable[..., Awaitable[T]], *args: Any, **kw: Any
) -> T:
    """:func:`_gated` for a coroutine function, on ``db.asynced()``. ``AsyncSqliteStore`` runs the
    calls of one tick as one batch, so a call that does not write is refused only with a write of
    another call: it runs again at once, and that call waits."""
    while True:
        try:
            result = await call(*args, **kw)
        except UrsulaWriteRefusedError as error:
            if not _backlog_full(error):
                raise
        else:
            if writes:
                await db.asynced()
            return result
        if writes:
            await db.asynced()


class _Connection(UrsulaConnection):
    """An adapter's own connection on its database's binding.

    LangGraph's ``setup`` commits its statements one by one, and a store's migrations are not safe
    to run twice: run again after its ``INSERT INTO store_migrations`` was refused, an
    ``ALTER TABLE ... ADD COLUMN`` fails with "duplicate column name". So while ``setting_up``, a
    statement run outside a transaction that is refused with ``backlog_full``, which committed
    nothing, waits for ``synced()`` and runs again by itself. Each of setup's scripts is one
    statement, or statements that are safe to run twice (``CREATE TABLE IF NOT EXISTS``).

    For the async adapters that wait blocks aiosqlite's thread, which cancelling the setup's caller
    does not end: the connection's other statements and ``aclose()`` wait behind it until the
    stream acknowledges the commits pending before it."""

    setting_up = False

    def execute(self, sql: str, parameters: Any = (), /) -> Any:
        return self._admitted(super().execute, sql, parameters)

    def executescript(self, script: str, /) -> Any:
        return self._admitted(super().executescript, script)

    def _admitted(self, run: Callable[..., T], *args: Any) -> T:
        if not self.setting_up or self._in_transaction():
            return run(*args)
        while True:
            try:
                return run(*args)
            except UrsulaWriteRefusedError as error:
                if not _backlog_full(error):
                    raise
            self.synced()


@contextlib.contextmanager
def _setting_up(conn: _Connection) -> Iterator[None]:
    conn.setting_up = True
    try:
        yield
    finally:
        conn.setting_up = False


def _connect(db: UrsulaConnection, **kwargs: Any) -> _Connection:
    """A connection of an adapter's own on ``db``'s binding, which any thread may use."""
    database, binding = on_binding(db, _Connection)
    conn = sqlite3.connect(database, check_same_thread=False, **binding, **kwargs)
    return cast(_Connection, conn)


async def _aconnect(
    db: UrsulaConnection, **kwargs: Any
) -> tuple[aiosqlite.Connection, _Connection]:
    """An aiosqlite connection of an adapter's own on ``db``'s binding, and the sqlite3
    connection it made on its thread."""
    database, binding = on_binding(db, _Connection)
    bind = binding.pop("factory")
    made: list[_Connection] = []

    def factory(*args: Any, **kw: Any) -> UrsulaConnection:
        conn = cast(_Connection, bind(*args, **kw))
        made.append(conn)
        return conn

    conn = await aiosqlite.connect(database, factory=factory, **binding, **kwargs)
    return conn, made[0]


def _refreshes(ttl: Any, refresh_ttl: bool | None) -> bool:
    """Whether a get or a search with ``refresh_ttl`` (``None``: the store's default) writes:
    ``SqliteStore`` refreshes the TTLs of what it reads when its ``ttl`` sets
    ``refresh_on_read``."""
    return bool(ttl and ttl.get("refresh_on_read", False)) and refresh_ttl is not False


def _writes(ops: Sequence[Op], ttl: Any) -> bool:
    """Whether a store batch writes: a put, a delete, or a get or a search that refreshes TTLs."""
    return any(
        isinstance(op, PutOp)
        or (isinstance(op, (GetOp, SearchOp)) and _refreshes(ttl, op.refresh_ttl))
        for op in ops
    )


def _no_conn_string(cls: type, use: str) -> UrsulaSetupError:
    return invalid(f"{cls.__name__} replicates a database open() made, not a file: use {use}")


class UrsulaSaver(SqliteSaver):
    """LangGraph's ``SqliteSaver`` on a connection of its own on ``db``'s binding, ``db`` a
    connection ``open()`` made: ``put``, ``put_writes`` and ``delete_thread`` return once
    ``db.synced()`` returned after them, and a ``backlog_full`` refusal runs again after
    ``synced()``.

    LangGraph runs a graph's checkpoint writes in the background and waits for them before
    ``invoke`` returns, so ``invoke``'s return is gated in every durability mode.
    ``durability="sync"`` also gates what a node does after the step before it, at one round trip
    per step. ``close()``, or ``with``, closes its connection. ``db`` stays open.
    """

    conn: _Connection

    def __init__(self, db: UrsulaConnection, *, serde: SerializerProtocol | None = None) -> None:
        self.db = bound(db, type(self).__name__)
        conn = _connect(self.db)
        try:
            super().__init__(conn, serde=serde)
        except BaseException:
            conn.close()
            raise

    @classmethod
    def from_conn_string(cls, conn_string: str) -> Any:
        raise _no_conn_string(cls, f"{cls.__name__}(sqlite_ursula.open(url))")

    def setup(self) -> None:
        if not self.is_setup:
            with _setting_up(self.conn):
                super().setup()

    def put(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        return _gated(self.db, True, super().put, config, checkpoint, metadata, new_versions)

    def put_writes(
        self, config: Any, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        _gated(self.db, True, super().put_writes, config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        _gated(self.db, True, super().delete_thread, thread_id)

    def close(self) -> None:
        """Closes the saver's connection, after the call that holds it. ``db`` stays open."""
        with self.lock:
            self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncUrsulaSaver(AsyncSqliteSaver):
    """LangGraph's ``AsyncSqliteSaver`` on an aiosqlite connection of its own on ``db``'s
    binding: ``aput``, ``aput_writes`` and ``adelete_thread`` return once ``db.asynced()``
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
        _own: _Connection,
    ) -> None:
        """Made by :meth:`open`: ``conn`` is the aiosqlite connection it made on ``db``'s
        binding, and ``_own`` the sqlite3 connection under it."""
        self.db = bound(db, type(self).__name__)
        self._own = _own
        super().__init__(conn, serde=serde)

    @classmethod
    async def open(cls, db: UrsulaConnection, *, serde: SerializerProtocol | None = None) -> Self:
        """The saver of ``db``, a connection ``open()`` or ``aopen()`` made."""
        checked = bound(db, cls.__name__)
        conn, own = await _aconnect(checked)
        try:
            return cls(checked, conn, serde=serde, _own=own)
        except BaseException:
            await conn.close()
            raise

    @classmethod
    def from_conn_string(cls, conn_string: str) -> Any:
        raise _no_conn_string(cls, f"await {cls.__name__}.open(sqlite_ursula.open(url))")

    async def setup(self) -> None:
        if not self.is_setup:
            with _setting_up(self._own):
                await super().setup()

    async def aput(self, config: Any, checkpoint: Any, metadata: Any, new_versions: Any) -> Any:
        return await _agated(
            self.db, True, super().aput, config, checkpoint, metadata, new_versions
        )

    async def aput_writes(
        self, config: Any, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        await _agated(self.db, True, super().aput_writes, config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        await _agated(self.db, True, super().adelete_thread, thread_id)

    async def aclose(self) -> None:
        """Closes the saver's connection. ``db`` stays open."""
        await self.conn.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()


class UrsulaStore(SqliteStore):
    """LangGraph's ``SqliteStore`` on a connection of its own on ``db``'s binding, ``db`` a
    connection ``open()`` made, without implicit transactions as ``SqliteStore``'s own
    connections are: a batch that writes (a put, a delete, a get or a search that refreshes TTLs)
    and a TTL sweep return once ``db.synced()`` returned after them, and a ``backlog_full`` refusal
    runs again after ``synced()``. ``close()``, or ``with``, stops its TTL sweeper and closes its
    connection. ``db`` stays open."""

    conn: _Connection

    def __init__(
        self,
        db: UrsulaConnection,
        *,
        deserializer: Callable[[Any], dict[str, Any]] | None = None,
        index: Any = None,
        ttl: Any = None,
    ) -> None:
        self.db = bound(db, type(self).__name__)
        conn = _connect(self.db, isolation_level=None)
        try:
            super().__init__(conn, deserializer=deserializer, index=index, ttl=ttl)
        except BaseException:
            conn.close()
            raise

    @classmethod
    def from_conn_string(cls, conn_string: str, **kwargs: Any) -> Any:
        raise _no_conn_string(cls, f"{cls.__name__}(sqlite_ursula.open(url))")

    def setup(self) -> None:
        if not self.is_setup:
            with _setting_up(self.conn):
                super().setup()

    def batch(self, ops: Iterable[Op]) -> list[Result]:
        listed = list(ops)
        return _gated(self.db, _writes(listed, self.ttl_config), super().batch, listed)

    def sweep_ttl(self) -> int:
        return _gated(self.db, True, super().sweep_ttl)

    def close(self) -> None:
        """Stops the TTL sweeper, waiting at most a second for it, and closes the store's
        connection after the call that holds it. ``db`` stays open."""
        self.stop_ttl_sweeper(timeout=SWEEPER_STOP_S)
        with self.lock:
            self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class AsyncUrsulaStore(AsyncSqliteStore):
    """LangGraph's ``AsyncSqliteStore`` on an aiosqlite connection of its own on ``db``'s binding,
    without implicit transactions: ``aput`` and ``adelete``, an ``aget`` or ``asearch`` that
    refreshes TTLs, and a TTL sweep return once ``db.asynced()`` returned after them, and each
    write refused with ``backlog_full`` runs again after ``asynced()``. A read batched with a
    refused write runs again at once. The waits are the callers', so the task that runs the store's
    batches never waits for the stream, and a read never waits behind another call's write. A
    statement of the setup refused with ``backlog_full`` is the exception: it waits on aiosqlite's
    thread (``_Connection``). ``abatch`` and ``batch``, which take operations, are LangGraph's own:
    call ``db.asynced()`` after them.

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
        _own: _Connection,
    ) -> None:
        """Made by :meth:`open`: ``conn`` is the aiosqlite connection it made on ``db``'s
        binding, and ``_own`` the sqlite3 connection under it."""
        self.db = bound(db, type(self).__name__)
        self._own = _own
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
        """The store of ``db``, a connection ``open()`` or ``aopen()`` made."""
        checked = bound(db, cls.__name__)
        conn, own = await _aconnect(checked, isolation_level=None)
        try:
            return cls(checked, conn, deserializer=deserializer, index=index, ttl=ttl, _own=own)
        except BaseException:
            await conn.close()
            raise

    @classmethod
    def from_conn_string(cls, conn_string: str, **kwargs: Any) -> Any:
        raise _no_conn_string(cls, f"await {cls.__name__}.open(sqlite_ursula.open(url))")

    async def setup(self) -> None:
        if not self.is_setup:
            with _setting_up(self._own):
                await super().setup()

    async def aget(self, *args: Any, **kwargs: Any) -> Any:
        writes = _refreshes(self.ttl_config, kwargs.get("refresh_ttl"))
        return await _agated(self.db, writes, super().aget, *args, **kwargs)

    async def asearch(self, *args: Any, **kwargs: Any) -> Any:
        writes = _refreshes(self.ttl_config, kwargs.get("refresh_ttl"))
        return await _agated(self.db, writes, super().asearch, *args, **kwargs)

    async def alist_namespaces(self, *args: Any, **kwargs: Any) -> Any:
        return await _agated(self.db, False, super().alist_namespaces, *args, **kwargs)

    async def aput(self, *args: Any, **kwargs: Any) -> None:
        await _agated(self.db, True, super().aput, *args, **kwargs)

    async def adelete(self, *args: Any, **kwargs: Any) -> None:
        await _agated(self.db, True, super().adelete, *args, **kwargs)

    async def sweep_ttl(self) -> int:
        return await _agated(self.db, True, super().sweep_ttl)

    async def aclose(self) -> None:
        """Stops the TTL sweeper, cancelling it after a second, and closes the store's
        connection. ``db`` stays open."""
        await self.stop_ttl_sweeper(timeout=SWEEPER_STOP_S)
        await self.conn.close()

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()
