"""The LangGraph adapters (``sqlite_ursula.langgraph``) against the e2e server, on the LangGraph
releases CI installs: a graph's ``invoke`` and ``ainvoke`` return once the checkpoints of the run
are in the stream, and another process reads them from there, and the stores' puts return once
they are in the stream, while a read never waits behind a write. A write refused with
``backlog_full`` runs again once ``synced()`` returned, the setup's statements one by one, a reset
raises the ``UrsulaResetError`` itself from runs, reads and store calls, the adapters' own
connections are on the binding (their errors classify, they are read-only on a read-only
connection, they keep ``db``'s busy timeout, and the savers' keep SQLite's default transaction
control), and the adapters take only connections ``open()`` made.

The tests that script a late acknowledgement take the model server, and the rest run on one real
``ursula`` node too. The module skips where LangGraph is not installed. CI names the
``langgraph-checkpoint-sqlite`` release it installs in ``SQLITE_URSULA_LANGGRAPH``, and then it
never skips."""

from __future__ import annotations

import asyncio
import importlib.metadata
import os
import sqlite3
import threading
import time
from typing import Any
from typing import TypedDict

import pytest
from conftest import Child

import sqlite_ursula
from sqlite_ursula import UrsulaConnection
from sqlite_ursula import UrsulaResetError
from sqlite_ursula import UrsulaSetupError

EXPECTED = os.environ.get("SQLITE_URSULA_LANGGRAPH")
if EXPECTED is None:
    pytest.importorskip(
        "langgraph.checkpoint.sqlite", reason="the langgraph extra is not installed"
    )

import aiosqlite  # noqa: E402
from langgraph.graph import END  # noqa: E402
from langgraph.graph import START  # noqa: E402
from langgraph.graph import StateGraph  # noqa: E402

from sqlite_ursula.langgraph import AsyncUrsulaSaver  # noqa: E402
from sqlite_ursula.langgraph import AsyncUrsulaStore  # noqa: E402
from sqlite_ursula.langgraph import UrsulaSaver  # noqa: E402
from sqlite_ursula.langgraph import UrsulaStore  # noqa: E402


class Count(TypedDict):
    n: int


def counter(checkpointer: Any) -> Any:
    """A graph of one node that adds one."""
    builder = StateGraph(Count)
    builder.add_node("add", lambda state: {"n": state["n"] + 1})
    builder.add_edge(START, "add")
    builder.add_edge("add", END)
    return builder.compile(checkpointer=checkpointer)


def thread(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id}}


def in_the_stream(url: str, root: str, query: str) -> list[list[Any]]:
    """The rows of ``query`` that another process reads from the stream, in a cache root of its
    own."""
    reader = Child(url, root)
    try:
        return reader.send(query=query)["rows"]
    finally:
        reader.stop()


def checkpoints_in_the_stream(url: str, root: str, thread_id: str) -> int:
    """How many checkpoints of ``thread_id`` another process reads from the stream."""
    query = f"SELECT count(*) FROM checkpoints WHERE thread_id = '{thread_id}'"
    return int(in_the_stream(url, root, query)[0][0])


def busy_ms(conn: sqlite3.Connection) -> int:
    return int(conn.execute("PRAGMA busy_timeout").fetchone()[0])


class Refusals:
    """The kinds ``status()["refused"]`` names while it runs, polled from a thread."""

    def __init__(self, db: UrsulaConnection) -> None:
        self.seen: set[str] = set()
        self._db = db
        self._done = threading.Event()
        self._thread = threading.Thread(target=self._watch)
        self._thread.start()

    def _watch(self) -> None:
        while not self._done.wait(0.05):
            refused = self._db.status()["refused"]
            if refused is not None:
                self.seen.add(refused["kind"])

    def stop(self) -> set[str]:
        self._done.set()
        self._thread.join()
        return self.seen


@pytest.mark.skipif(EXPECTED is None, reason="CI names the release it installs")
def test_runs_on_the_release_ci_installs():
    assert importlib.metadata.version("langgraph-checkpoint-sqlite") == EXPECTED


def test_invoke_returns_once_the_checkpoints_of_the_run_are_in_the_stream(stream, fresh):
    # db commits each statement, and the saver's own connection keeps the default transactions.
    db = sqlite_ursula.open(stream.url, dir=fresh(), isolation_level=None, busy_timeout=2.5)
    with UrsulaSaver(db) as saver:
        assert saver.db is db
        assert saver.conn is not db
        assert saver.conn.isolation_level == ""
        assert busy_ms(saver.conn) == 2500
        graph = counter(saver)
        config = thread("t")
        assert graph.invoke({"n": 1}, config) == {"n": 2}
        assert db.status()["commits"]["pending"] == 0
        assert graph.get_state(config).values == {"n": 2}
        assert len(list(saver.list(config))) >= 2

        saver.delete_thread("t")
        assert db.status()["commits"]["pending"] == 0
        assert list(saver.list(config)) == []
        assert graph.invoke({"n": 20}, thread("u")) == {"n": 21}
        assert db.status()["commits"]["pending"] == 0
        saved = len(list(saver.list(thread("u"))))
    db.close()
    assert checkpoints_in_the_stream(stream.url, fresh(), "t") == 0
    assert checkpoints_in_the_stream(stream.url, fresh(), "u") == saved


def test_invoke_waits_for_a_late_acknowledgement(model, stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh())
    with UrsulaSaver(db) as saver:
        graph = counter(saver)
        config = thread("t")
        assert graph.invoke({"n": 1}, config) == {"n": 2}

        # The run's first append is answered a second late: invoke waits for it and for the rest.
        model.fault("delay 1000", 1, "POST")
        started = time.monotonic()
        assert graph.invoke({"n": 10}, config) == {"n": 11}
        assert time.monotonic() - started >= 0.9
        assert db.status()["commits"]["pending"] == 0
    db.close()


def test_a_write_refused_with_backlog_full_runs_again_once_synced(model, stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh(), max_pending_bytes=1)
    with UrsulaSaver(db) as saver:
        graph = counter(saver)
        config = thread("t")
        # This commit waits a second for the stream and holds the backlog full meanwhile: each
        # statement of the saver's setup, before the run's first read, is refused and runs again.
        model.fault("delay 1000", 1, "POST")
        db.execute("CREATE TABLE t(x)")
        refusals = Refusals(db)
        try:
            assert graph.invoke({"n": 1}, config) == {"n": 2}
        finally:
            seen = refusals.stop()
        assert "backlog_full" in seen

        # While this commit is pending, every commit of the run is refused and runs again.
        model.fault("delay 2000", 1, "POST")
        db.execute("INSERT INTO t VALUES (1)")
        db.commit()
        started = time.monotonic()
        assert graph.invoke({"n": 5}, config) == {"n": 6}
        assert time.monotonic() - started >= 1.5
        assert graph.get_state(config).values == {"n": 6}
        assert db.status()["commits"]["pending"] == 0
    db.close()


def test_a_reset_raises_from_the_run_and_from_reads(stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh())
    with UrsulaSaver(db) as saver:
        graph = counter(saver)
        config = thread("t")
        assert graph.invoke({"n": 1}, config) == {"n": 2}
        taker = Child(stream.url, fresh())
        with pytest.raises(UrsulaResetError) as reset:
            graph.invoke({"n": 1}, config)
        assert (reset.value.kind, reset.value.action) == ("fenced", "stop")
        with pytest.raises(UrsulaResetError):
            saver.get_tuple(config)
        taker.stop()
    db.close()


def test_ainvoke_returns_once_the_checkpoints_of_the_run_are_in_the_stream(stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaSaver.open(db) as saver:
            assert saver.db is db
            assert isinstance(saver.conn, aiosqlite.Connection)
            graph = counter(saver)
            config = thread("t")
            assert await graph.ainvoke({"n": 1}, config) == {"n": 2}
            assert db.status()["commits"]["pending"] == 0
            assert (await graph.aget_state(config)).values == {"n": 2}
            saved = len([c async for c in saver.alist(config)])
            assert saved >= 2
        db.close()
        assert checkpoints_in_the_stream(stream.url, fresh(), "t") == saved

    asyncio.run(main())


def test_ainvoke_waits_for_a_late_acknowledgement(model, stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaSaver.open(db) as saver:
            graph = counter(saver)
            config = thread("t")
            assert await graph.ainvoke({"n": 1}, config) == {"n": 2}

            model.fault("delay 1000", 1, "POST")
            started = time.monotonic()
            assert await graph.ainvoke({"n": 10}, config) == {"n": 11}
            assert time.monotonic() - started >= 0.9
            assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_an_async_write_refused_with_backlog_full_runs_again_once_synced(model, stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh(), max_pending_bytes=1)
        async with await AsyncUrsulaSaver.open(db) as saver:
            graph = counter(saver)
            config = thread("t")
            # Each statement of the setup is refused while that commit is pending, and runs again.
            model.fault("delay 1000", 1, "POST")
            db.execute("CREATE TABLE t(x)")
            refusals = Refusals(db)
            try:
                assert await graph.ainvoke({"n": 1}, config) == {"n": 2}
            finally:
                seen = refusals.stop()
            assert "backlog_full" in seen

            # While this commit is pending, every commit of the run is refused and runs again: the
            # aiosqlite connection raises the refusal classified, which a plain one would raise as
            # a bare sqlite3.OperationalError.
            model.fault("delay 2000", 1, "POST")
            db.execute("INSERT INTO t VALUES (1)")
            db.commit()
            started = time.monotonic()
            assert await graph.ainvoke({"n": 5}, config) == {"n": 6}
            assert time.monotonic() - started >= 1.5
            assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_the_async_connections_are_on_the_binding(stream, fresh):
    async def main() -> None:
        root = fresh()
        db = await sqlite_ursula.aopen(stream.url, dir=root, busy_timeout=2.5)
        saver = await AsyncUrsulaSaver.open(db)
        async with saver.conn.execute("PRAGMA busy_timeout") as cursor:
            assert await cursor.fetchone() == (2500,)
        graph = counter(saver)
        config = thread("t")
        assert await graph.ainvoke({"n": 1}, config) == {"n": 2}

        # On a read-only connection, the saver's connection is read-only too.
        reader = sqlite_ursula.open(stream.url, dir=root, read_only=True)
        async with await AsyncUrsulaSaver.open(reader) as read_only:
            assert (await read_only.aget_tuple(config)) is not None
            latest = await saver.aget_tuple(config)
            assert latest is not None
            with pytest.raises(sqlite3.OperationalError) as refused:
                await read_only.aput(latest.config, latest.checkpoint, {}, {})
            assert refused.value.sqlite_errorcode == sqlite3.SQLITE_READONLY
        reader.close()

        # After a takeover, the statements of the saver's own connection raise the reset.
        taker = Child(stream.url, fresh())
        with pytest.raises(UrsulaResetError) as reset:
            await graph.ainvoke({"n": 1}, config)
        assert reset.value.kind == "fenced"
        with pytest.raises(UrsulaResetError):
            await saver.aget_tuple(config)
        taker.stop()
        await saver.aclose()
        db.close()

    asyncio.run(main())


def test_the_store_returns_from_a_put_once_it_is_in_the_stream(stream, fresh):
    # Its own connection opens no transactions of its own, whatever db's transaction control.
    db = sqlite_ursula.open(stream.url, dir=fresh(), busy_timeout=2.5)
    with UrsulaStore(db) as store:
        assert store.db is db
        assert store.conn.isolation_level is None
        assert busy_ms(store.conn) == 2500
        store.put(("users", "1"), "prefs", {"theme": "dark"})
        assert db.status()["commits"]["pending"] == 0
        item = store.get(("users", "1"), "prefs")
        assert item is not None
        assert item.value == {"theme": "dark"}
        store.delete(("users", "1"), "prefs")
        assert db.status()["commits"]["pending"] == 0
        assert store.get(("users", "1"), "prefs") is None
        store.put(("users", "2"), "prefs", {"theme": "light"})
    db.close()
    assert in_the_stream(stream.url, fresh(), "SELECT prefix, key FROM store") == [
        ["users.2", "prefs"]
    ]


def test_the_store_waits_for_a_late_acknowledgement(model, stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh())
    with UrsulaStore(db) as store:
        assert store.get(("users", "1"), "prefs") is None
        model.fault("delay 1000", 1, "POST")
        started = time.monotonic()
        store.put(("users", "1"), "prefs", {"theme": "dark"})
        assert time.monotonic() - started >= 0.9
        assert db.status()["commits"]["pending"] == 0
    db.close()


def test_a_store_write_refused_with_backlog_full_runs_again_once_synced(model, stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh(), max_pending_bytes=1)
    store = UrsulaStore(db)
    # The setup commits each migration and its row in store_migrations apart, and its ALTER TABLEs
    # would fail when run twice: each statement refused while this commit is pending runs again.
    model.fault("delay 1000", 1, "POST")
    db.execute("CREATE TABLE t(x)")
    refusals = Refusals(db)
    try:
        store.setup()
    finally:
        seen = refusals.stop()
    assert "backlog_full" in seen

    # While this commit is pending, the put's batch is refused and runs again.
    model.fault("delay 2000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    db.commit()
    started = time.monotonic()
    store.put(("users", "1"), "prefs", {"theme": "dark"})
    assert time.monotonic() - started >= 1.5
    assert db.status()["commits"]["pending"] == 0
    store.close()

    # Another store on the database finds every migration applied.
    with UrsulaStore(db) as reopened:
        item = reopened.get(("users", "1"), "prefs")
        assert item is not None
        assert item.value == {"theme": "dark"}
    db.close()


def test_a_reset_raises_from_the_store(stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh())
    with UrsulaStore(db) as store:
        store.put(("users", "1"), "prefs", {"theme": "dark"})
        taker = Child(stream.url, fresh())
        with pytest.raises(UrsulaResetError) as reset:
            store.put(("users", "1"), "prefs", {"theme": "light"})
        assert (reset.value.kind, reset.value.action) == ("fenced", "stop")
        # A statement's reset rolls the batch's transaction back, and the COMMIT that LangGraph
        # runs after it finds none: the reset passes, not that COMMIT's error.
        with pytest.raises(UrsulaResetError) as read:
            store.get(("users", "1"), "prefs")
        assert (read.value.kind, read.value.action) == ("fenced", "stop")
        with pytest.raises(UrsulaResetError) as write:
            store.put(("users", "1"), "prefs", {"theme": "dark"})
        assert (write.value.kind, write.value.action) == ("fenced", "stop")
        taker.stop()
    db.close()


def test_the_async_store_returns_from_a_put_once_it_is_in_the_stream(stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaStore.open(db) as store:
            assert store.db is db
            await store.aput(("users", "1"), "prefs", {"theme": "dark"})
            assert db.status()["commits"]["pending"] == 0
            item = await store.aget(("users", "1"), "prefs")
            assert item is not None
            assert item.value == {"theme": "dark"}
            # Concurrent puts run as one batch, and each returns once asynced() returned after it.
            await asyncio.gather(*(store.aput(("docs",), str(i), {"i": i}) for i in range(10)))
            assert db.status()["commits"]["pending"] == 0
            assert len(await store.asearch(("docs",), limit=20)) == 10
            await store.adelete(("users", "1"), "prefs")
            assert db.status()["commits"]["pending"] == 0
        db.close()
        rows = in_the_stream(stream.url, fresh(), "SELECT count(*) FROM store")
        assert rows == [[10]]

    asyncio.run(main())


def test_the_async_store_reads_without_waiting_behind_a_write(model, stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaStore.open(db) as store:
            await store.aput(("users", "1"), "prefs", {"theme": "dark"})
            model.fault("delay 1000", 1, "POST")
            started = time.monotonic()
            put = asyncio.create_task(store.aput(("users", "2"), "prefs", {"theme": "light"}))
            # The put's batch ran, and the put waits for the stream: the read runs meanwhile.
            await asyncio.sleep(0.2)
            assert not put.done()
            item = await store.aget(("users", "1"), "prefs")
            assert item is not None
            assert time.monotonic() - started < 0.8
            await put
            assert time.monotonic() - started >= 0.9
            assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_an_async_store_read_batched_with_a_refused_write_does_not_wait(model, stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh(), max_pending_bytes=1)
        async with await AsyncUrsulaStore.open(db) as store:
            await store.aput(("users", "1"), "prefs", {"theme": "dark"})
            # While this commit is pending, the batch of the put and the get is refused: the get
            # runs again at once, and the put once asynced() returned.
            model.fault("delay 2000", 1, "POST")
            db.execute("CREATE TABLE t(x)")
            started = time.monotonic()

            async def get() -> tuple[Any, float]:
                item = await store.aget(("users", "1"), "prefs")
                return item, time.monotonic() - started

            refusals = Refusals(db)
            try:
                _, (item, read_s) = await asyncio.gather(
                    store.aput(("users", "2"), "prefs", {"theme": "light"}), get()
                )
            finally:
                seen = refusals.stop()
            assert "backlog_full" in seen
            assert item is not None
            assert item.value == {"theme": "dark"}
            assert read_s < 1.0
            assert time.monotonic() - started >= 1.5
            assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_a_reset_raises_from_the_async_store(stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaStore.open(db) as store:
            await store.aput(("users", "1"), "prefs", {"theme": "dark"})
            taker = Child(stream.url, fresh())
            with pytest.raises(UrsulaResetError) as reset:
                await store.aput(("users", "1"), "prefs", {"theme": "light"})
            assert (reset.value.kind, reset.value.action) == ("fenced", "stop")
            with pytest.raises(UrsulaResetError) as read:
                await store.aget(("users", "1"), "prefs")
            assert (read.value.kind, read.value.action) == ("fenced", "stop")
            with pytest.raises(UrsulaResetError) as write:
                await store.aput(("users", "1"), "prefs", {"theme": "dark"})
            assert (write.value.kind, write.value.action) == ("fenced", "stop")
            taker.stop()
        db.close()

    asyncio.run(main())


def test_the_adapters_take_connections_open_made():
    plain = sqlite3.connect(":memory:", check_same_thread=False)
    for make in (UrsulaSaver, UrsulaStore):
        with pytest.raises(UrsulaSetupError) as invalid:
            make(plain)
        assert (invalid.value.kind, invalid.value.action) == ("invalid_argument", "fix")
    for cls in (UrsulaSaver, UrsulaStore, AsyncUrsulaSaver, AsyncUrsulaStore):
        with pytest.raises(UrsulaSetupError) as by_file:
            cls.from_conn_string("checkpoints.sqlite")
        assert by_file.value.kind == "invalid_argument"
        assert "open(url" in str(by_file.value)

    async def main() -> None:
        for cls in (AsyncUrsulaSaver, AsyncUrsulaStore):
            with pytest.raises(UrsulaSetupError) as invalid:
                await cls.open(plain)
            assert invalid.value.kind == "invalid_argument"

    asyncio.run(main())
    plain.close()
