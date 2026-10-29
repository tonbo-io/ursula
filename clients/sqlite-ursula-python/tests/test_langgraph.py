"""The LangGraph adapters (``sqlite_ursula.langgraph``) against the e2e server, on the LangGraph
releases CI installs: a graph's ``invoke`` and ``ainvoke`` return once the checkpoints of the run
are in the stream, and another process reads them from there. A write refused with
``backlog_full`` runs again once ``synced()`` returned, a reset raises the ``UrsulaResetError``
itself from runs and reads, the async adapters' aiosqlite connections are on the binding (their
errors classify, and they are read-only on a read-only connection), the stores gate their puts,
and the adapters take only connections ``open()`` made.

The module skips where LangGraph is not installed. CI names the ``langgraph-checkpoint-sqlite``
release it installs in ``SQLITE_URSULA_LANGGRAPH``, and then it never skips."""

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


def checkpoints_in_the_stream(url: str, root: str, thread_id: str) -> int:
    """How many checkpoints of ``thread_id`` another process reads from the stream, in a cache
    root of its own."""
    reader = Child(url, root)
    try:
        query = f"SELECT count(*) FROM checkpoints WHERE thread_id = '{thread_id}'"
        return int(reader.send(query=query)["rows"][0][0])
    finally:
        reader.stop()


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


def test_invoke_returns_once_the_checkpoints_of_the_run_are_in_the_stream(model, stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh())
    saver = UrsulaSaver(db)
    assert saver.db is db
    graph = counter(saver)
    config = thread("t")
    assert graph.invoke({"n": 1}, config) == {"n": 2}

    # The run's first append is answered a second late: invoke waits for it and for the rest.
    model.fault("delay 1000", 1, "POST")
    started = time.monotonic()
    assert graph.invoke({"n": 10}, config) == {"n": 11}
    assert time.monotonic() - started >= 0.9
    assert db.status()["commits"]["pending"] == 0
    assert graph.get_state(config).values == {"n": 11}
    saved = len(list(saver.list(config)))
    assert saved >= 4

    saver.delete_thread("t")
    assert db.status()["commits"]["pending"] == 0
    assert list(saver.list(config)) == []
    assert graph.invoke({"n": 20}, thread("u")) == {"n": 21}
    db.close()
    assert checkpoints_in_the_stream(stream.url, fresh(), "t") == 0
    assert checkpoints_in_the_stream(stream.url, fresh(), "u") >= 2


def test_a_write_refused_with_backlog_full_runs_again_once_synced(model, stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh(), max_pending_bytes=1, isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    graph = counter(UrsulaSaver(db))
    config = thread("t")
    # The saver's setup, before the run's first read, is refused while that commit is pending, and
    # runs again.
    assert graph.invoke({"n": 1}, config) == {"n": 2}

    # This commit waits two seconds for the stream and holds the backlog full meanwhile.
    model.fault("delay 2000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    refusals = Refusals(db)
    try:
        assert graph.invoke({"n": 5}, config) == {"n": 6}
    finally:
        seen = refusals.stop()
    assert "backlog_full" in seen
    assert graph.get_state(config).values == {"n": 6}
    assert db.status()["commits"]["pending"] == 0
    db.close()


def test_a_reset_raises_from_the_run_and_from_reads(stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh())
    saver = UrsulaSaver(db)
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


def test_ainvoke_returns_once_the_checkpoints_of_the_run_are_in_the_stream(model, stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaSaver.open(db) as saver:
            assert saver.db is db
            assert isinstance(saver.conn, aiosqlite.Connection)
            graph = counter(saver)
            config = thread("t")
            assert await graph.ainvoke({"n": 1}, config) == {"n": 2}

            model.fault("delay 1000", 1, "POST")
            started = time.monotonic()
            assert await graph.ainvoke({"n": 10}, config) == {"n": 11}
            assert time.monotonic() - started >= 0.9
            assert db.status()["commits"]["pending"] == 0
            assert (await graph.aget_state(config)).values == {"n": 11}
            saved = len([c async for c in saver.alist(config)])
        db.close()
        assert checkpoints_in_the_stream(stream.url, fresh(), "t") == saved

    asyncio.run(main())


def test_an_async_write_refused_with_backlog_full_runs_again_once_synced(model, stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(
            stream.url, dir=fresh(), max_pending_bytes=1, isolation_level=None
        )
        db.execute("CREATE TABLE t(x)")
        async with await AsyncUrsulaSaver.open(db) as saver:
            graph = counter(saver)
            config = thread("t")
            # The setup is refused while that commit is pending, and runs again.
            assert await graph.ainvoke({"n": 1}, config) == {"n": 2}

            model.fault("delay 2000", 1, "POST")
            db.execute("INSERT INTO t VALUES (1)")
            refusals = Refusals(db)
            try:
                # The aiosqlite connection raises the refusal classified, which a plain one would
                # raise as a bare sqlite3.OperationalError.
                assert await graph.ainvoke({"n": 5}, config) == {"n": 6}
            finally:
                seen = refusals.stop()
            assert "backlog_full" in seen
            assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_the_async_connections_are_on_the_binding(stream, fresh):
    async def main() -> None:
        root = fresh()
        db = await sqlite_ursula.aopen(stream.url, dir=root)
        saver = await AsyncUrsulaSaver.open(db)
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


def test_the_store_returns_from_a_put_once_it_is_in_the_stream(model, stream, fresh):
    root = fresh()
    keeper = sqlite_ursula.open(stream.url, dir=root)
    with pytest.raises(UrsulaSetupError) as implicit:
        UrsulaStore(keeper)
    assert (implicit.value.kind, implicit.value.action) == ("invalid_argument", "fix")

    db = sqlite_ursula.open(stream.url, dir=root, isolation_level=None)
    store = UrsulaStore(db)
    store.setup()
    db.synced()
    model.fault("delay 1000", 1, "POST")
    started = time.monotonic()
    store.put(("users", "1"), "prefs", {"theme": "dark"})
    assert time.monotonic() - started >= 0.9
    assert db.status()["commits"]["pending"] == 0
    item = store.get(("users", "1"), "prefs")
    assert item is not None
    assert item.value == {"theme": "dark"}
    store.delete(("users", "1"), "prefs")
    assert store.get(("users", "1"), "prefs") is None
    assert db.status()["commits"]["pending"] == 0
    db.close()
    keeper.close()


def test_the_async_store_returns_from_a_put_once_it_is_in_the_stream(model, stream, fresh):
    async def main() -> None:
        # Its own connection takes no implicit transactions, whatever db's are.
        db = await sqlite_ursula.aopen(stream.url, dir=fresh())
        async with await AsyncUrsulaStore.open(db) as store:
            assert store.db is db
            await store.setup()
            await db.asynced()
            model.fault("delay 1000", 1, "POST")
            started = time.monotonic()
            await store.aput(("users", "1"), "prefs", {"theme": "dark"})
            assert time.monotonic() - started >= 0.9
            assert db.status()["commits"]["pending"] == 0
            item = await store.aget(("users", "1"), "prefs")
            assert item is not None
            assert item.value == {"theme": "dark"}
            # Concurrent puts run as one batch, gated once.
            await asyncio.gather(*(store.aput(("docs",), str(i), {"i": i}) for i in range(10)))
            assert db.status()["commits"]["pending"] == 0
            assert len(await store.asearch(("docs",), limit=20)) == 10
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
