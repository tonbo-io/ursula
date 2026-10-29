"""``aopen()`` and ``asynced()`` against the e2e server: they return what ``open()`` and
``synced()`` do and wait on the event loop instead of blocking it, any number of tasks resolve
through one watermark, a timeout or a cancellation of ``asynced()`` ends the wait and leaves no
ticket while the commits keep replicating, a reset raises, and a cancelled or timed-out
``aopen()`` cancels its bind, so it never goes on to claim the stream."""

from __future__ import annotations

import asyncio
import signal
import threading
import time

import pytest
from conftest import Child
from conftest import new_stream
from conftest import request

import sqlite_ursula
from sqlite_ursula import UrsulaConnection
from sqlite_ursula import UrsulaResetError
from sqlite_ursula import UrsulaSetupError
from sqlite_ursula import UrsulaUnavailableError
from sqlite_ursula import _events


def waiting(db: UrsulaConnection) -> int:
    """The tickets the process holds for ``db``'s database."""
    return sum(1 for t in list(_events._tickets.values()) if t.path == db.path)


async def opened(server, fresh, label: str) -> UrsulaConnection:
    s = new_stream(server, label)
    db = await sqlite_ursula.aopen(s.url, dir=fresh(), isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    await db.asynced()
    return db


def test_aopen_and_asynced_work_as_open_and_synced(stream, fresh):
    async def main() -> None:
        root = fresh()
        db = await sqlite_ursula.aopen(stream.url, dir=root)
        assert isinstance(db, UrsulaConnection)
        assert db.name == stream.path
        db.execute("CREATE TABLE t(x)")
        db.execute("INSERT INTO t VALUES (1)")
        db.commit()
        await db.asynced(timeout=30)
        status = db.status()
        assert (status["state"], status["role"]) == ("live", "owner")
        assert status["commits"]["pending"] == 0
        # Another open in the process, from either call: the same binding.
        again = await sqlite_ursula.aopen(stream.url, dir=root)
        plain = sqlite_ursula.open(stream.url, dir=root)
        assert again.path == plain.path == db.path
        assert again.execute("SELECT x FROM t").fetchall() == [(1,)]
        for timeout in (-1, "5", float("nan"), True):
            with pytest.raises(UrsulaSetupError) as invalid:
                await db.asynced(timeout=timeout)
            assert (invalid.value.kind, invalid.value.action) == ("invalid_argument", "fix")
        with pytest.raises(UrsulaSetupError) as bad:
            await sqlite_ursula.aopen(stream.url, dir=root, timeout=0)
        assert bad.value.kind == "invalid_argument"
        plain.close()
        again.close()
        db.close()

    asyncio.run(main())


def test_asynced_waits_on_the_loop_while_the_stream_holds_its_answer_back(model, fresh):
    async def main() -> None:
        db = await opened(model, fresh, "async-wait")
        model.fault("delay 1000", 1, "POST")
        db.execute("INSERT INTO t VALUES (1)")
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.05)
                ticks += 1

        ticking = asyncio.create_task(ticker())
        started = time.monotonic()
        await db.asynced(timeout=30)
        assert time.monotonic() - started >= 0.9
        # The loop ran other tasks meanwhile.
        assert ticks >= 10
        ticking.cancel()
        assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_resolves_hundreds_of_waiting_tasks_through_one_watermark(model, fresh):
    async def main() -> None:
        db = await opened(model, fresh, "async-many")
        model.fault("delay 1000", 1, "POST")
        db.execute("INSERT INTO t VALUES (1)")
        waits = [asyncio.create_task(db.asynced(timeout=30)) for _ in range(300)]
        db.execute("INSERT INTO t VALUES (2)")
        waits += [asyncio.create_task(db.asynced(timeout=30)) for _ in range(300)]
        await asyncio.gather(*waits)
        assert db.status()["commits"]["pending"] == 0
        assert waiting(db) == 0
        db.close()

    asyncio.run(main())


def test_a_timeout_or_a_cancellation_ends_the_wait_and_the_commits_keep_replicating(model, fresh):
    async def main() -> None:
        db = await opened(model, fresh, "async-cancel")
        model.fault("delay 1500", 1, "POST")
        db.execute("INSERT INTO t VALUES (1)")
        with pytest.raises(TimeoutError):
            await db.asynced(timeout=0.2)
        assert waiting(db) == 0

        wait = asyncio.create_task(db.asynced())
        await asyncio.sleep(0.2)
        assert waiting(db) == 1
        wait.cancel()
        with pytest.raises(asyncio.CancelledError):
            await wait
        assert waiting(db) == 0

        # The caller's own deadline is a cancellation too.
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(0.2):
                await db.asynced()
        assert waiting(db) == 0

        await db.asynced()
        assert db.status()["commits"]["pending"] == 0
        db.close()

    asyncio.run(main())


def test_ctrl_c_ends_asynced_under_asyncio_run(model, fresh):
    db = asyncio.run(opened(model, fresh, "async-interrupt"))
    model.fault("delay 2000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    main = threading.main_thread().ident
    assert main == threading.get_ident()
    timer = threading.Timer(0.3, signal.pthread_kill, (main, signal.SIGINT))
    started = time.monotonic()
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        asyncio.run(db.asynced())
    assert time.monotonic() - started < 1.5
    timer.join()
    assert waiting(db) == 0
    db.synced()
    assert db.status()["commits"]["pending"] == 0
    db.close()


def test_a_takeover_raises_the_reset_in_asynced(stream, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh(), isolation_level=None)
        db.execute("CREATE TABLE t(x)")
        await db.asynced()
        taker = Child(stream.url, fresh())
        with pytest.raises(UrsulaResetError) as reset:
            await db.asynced(timeout=30)
        assert (reset.value.kind, reset.value.action) == ("fenced", "stop")
        # Remembered: it raises again at once.
        with pytest.raises(UrsulaResetError):
            await db.asynced()
        taker.stop()
        db.close()

    asyncio.run(main())


def test_concurrent_aopens_share_one_bind(model, fresh):
    async def main() -> None:
        s = new_stream(model, "async-shared")
        root = fresh()
        got = await asyncio.gather(*(sqlite_ursula.aopen(s.url, dir=root) for _ in range(8)))
        assert len({db.path for db in got}) == 1
        got[0].execute("CREATE TABLE t(x)")
        got[0].commit()
        await got[-1].asynced()
        assert model.frames(s.path) == {"claims": 1, "commits": 1}
        for db in got:
            db.close()

    asyncio.run(main())


@pytest.mark.parametrize("how", ["cancel", "deadline"])
def test_a_cancelled_aopen_cancels_its_bind_and_the_stream_is_not_held(model, fresh, how):
    s = new_stream(model, f"async-open-{how}")
    root = fresh()

    async def main() -> None:
        # The bind's first request, its HEAD, is answered later than the open gives up.
        model.fault("delay 3000", 1)
        started = time.monotonic()
        if how == "cancel":
            opening = asyncio.create_task(sqlite_ursula.aopen(s.url, dir=root))
            await asyncio.sleep(0.3)
            opening.cancel()
            with pytest.raises(asyncio.CancelledError):
                await opening
        else:
            with pytest.raises(TimeoutError):
                async with asyncio.timeout(0.3):
                    await sqlite_ursula.aopen(s.url, dir=root)
        assert time.monotonic() - started < 2.5
        await asyncio.sleep(3.5)
        # The cancelled bind never went on to create the stream, let alone claim it.
        assert request("HEAD", s.url)[0] == 404
        db = await sqlite_ursula.aopen(s.url, dir=root)
        db.execute("CREATE TABLE t(x)")
        db.commit()
        await db.asynced()
        db.close()

    asyncio.run(main())


def test_aopen_reports_the_last_condition_it_met_when_its_timeout_passes(fresh):
    from conftest import free_port

    url = f"http://127.0.0.1:{free_port()}/sqlite-e2e/nobody"

    async def main() -> None:
        started = time.monotonic()
        with pytest.raises(UrsulaUnavailableError) as gave_up:
            await sqlite_ursula.aopen(url, dir=fresh(), timeout=2)
        assert time.monotonic() - started < 15
        assert gave_up.value.kind in ("unreachable", "cancelled")
        assert gave_up.value.action == "retry"

    asyncio.run(main())
