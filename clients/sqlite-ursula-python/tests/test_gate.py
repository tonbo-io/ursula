"""``gate`` and ``agate`` against the e2e server: leaving the block waits for a ``synced()`` call
made after it ended, on every database given, a block that only read included. The block's own
error waits as a result does, while a refused write passes at once, also wrapped, and so does the
raw refusal of a framework's own connection, which the extension classifies. A reset during the
wait replaces the outcome, the timeout bounds the wait alone, and a cancelled ``agate`` leaves no
ticket."""

from __future__ import annotations

import asyncio
import sqlite3
import time

import pytest
from conftest import Child
from conftest import new_stream

import sqlite_ursula
from sqlite_ursula import UrsulaConnection
from sqlite_ursula import UrsulaResetError
from sqlite_ursula import UrsulaSetupError
from sqlite_ursula import UrsulaWriteRefusedError
from sqlite_ursula import _events
from sqlite_ursula import agate
from sqlite_ursula import gate

SQLITE_IOERR_AUTH = 7178


def opened(server, fresh, label: str, **kwargs: object) -> UrsulaConnection:
    s = new_stream(server, label)
    db = sqlite_ursula.open(s.url, dir=fresh(), isolation_level=None, **kwargs)
    db.execute("CREATE TABLE t(x)")
    db.synced()
    return db


def waiting(db: UrsulaConnection) -> int:
    return sum(1 for t in list(_events._tickets.values()) if t.path == db.path)


class Wrapper(Exception):
    """A library's error that keeps the driver's as ``.orig``, as SQLAlchemy's DBAPIError does."""

    def __init__(self, orig: BaseException) -> None:
        super().__init__(f"wrapped: {orig}")
        self.orig = orig


def test_holds_the_block_until_the_stream_acknowledged_what_it_wrote_or_read(model, fresh):
    db = opened(model, fresh, "gate-hold")
    model.fault("delay 1000", 1, "POST")
    started = time.monotonic()
    with gate(db):
        db.execute("INSERT INTO t VALUES (1)")
        # The block itself does not wait.
        assert time.monotonic() - started < 0.5
    assert time.monotonic() - started >= 0.9
    assert db.status()["commits"]["pending"] == 0

    # A block that only read a framework connection's pending commit waits for it too.
    plain = sqlite3.connect(db.path, isolation_level=None)
    model.fault("delay 1000", 1, "POST")
    plain.execute("INSERT INTO t VALUES (2)")
    started = time.monotonic()
    with gate(db):
        rows = db.execute("SELECT x FROM t ORDER BY x").fetchall()
    assert rows == [(1,), (2,)]
    assert time.monotonic() - started >= 0.9
    plain.close()
    db.close()


def test_holds_the_blocks_own_error_and_passes_a_refused_write_at_once(model, fresh):
    db = opened(model, fresh, "gate-errors", max_pending_bytes=1)
    # The next append's answer comes late: its commit stays pending, and holds the backlog full.
    model.fault("delay 3000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    started = time.monotonic()
    with pytest.raises(UrsulaWriteRefusedError) as refused, gate(db):
        db.execute("INSERT INTO t VALUES (2)")
    assert refused.value.kind == "backlog_full"

    # Wrapped by a library: as its cause, as SQLAlchemy's .orig, in an ExceptionGroup.
    def wrapped(how: str) -> None:
        try:
            db.execute("INSERT INTO t VALUES (3)")
        except UrsulaWriteRefusedError as error:
            if how == "cause":
                raise RuntimeError("the library's own error") from error
            if how == "orig":
                raise Wrapper(error) from None
            raise ExceptionGroup("tasks failed", [ValueError("other"), error]) from None

    for how, raised in (("cause", RuntimeError), ("orig", Wrapper), ("group", ExceptionGroup)):
        with pytest.raises(raised), gate(db):
            wrapped(how)

    # A framework's own connection raises the refusal unclassified: the gate asks the extension.
    plain = sqlite3.connect(db.path)
    plain.execute("INSERT INTO t VALUES (4)")
    with pytest.raises(UrsulaWriteRefusedError) as classified, gate(db):
        plain.commit()
    assert classified.value.kind == "backlog_full"
    assert isinstance(classified.value.__cause__, sqlite3.OperationalError)
    assert not isinstance(classified.value.__cause__, sqlite_ursula.UrsulaError)
    assert classified.value.__cause__.sqlite_errorcode == SQLITE_IOERR_AUTH
    plain.rollback()
    # None of them waited for the delayed append.
    assert time.monotonic() - started < 2

    # SQLite's own error, and any other, waits as a result does.
    with pytest.raises(sqlite3.OperationalError) as own, gate(db):
        db.execute("SELECT nope FROM t")
    assert not isinstance(own.value, sqlite_ursula.UrsulaError)
    assert time.monotonic() - started >= 2.5
    assert db.status()["commits"]["pending"] == 0
    plain.close()
    db.close()


def test_a_takeover_replaces_the_outcome_with_the_reset(stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh(), isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced()
    taker = Child(stream.url, fresh())
    released = []
    with pytest.raises(UrsulaResetError) as reset:
        with gate(db, timeout=30):
            released.append(db.execute("SELECT count(*) FROM t").fetchall())
        released.append("after the gate")
    assert (reset.value.kind, reset.value.action) == ("fenced", "stop")
    assert "after the gate" not in released
    taker.stop()
    db.close()


def test_the_timeout_bounds_the_wait_while_the_commits_keep_replicating(model, fresh):
    db = opened(model, fresh, "gate-timeout")
    model.fault("delay 1500", 1, "POST")
    with pytest.raises(TimeoutError), gate(db, timeout=0.2):
        db.execute("INSERT INTO t VALUES (1)")
    assert waiting(db) == 0
    db.synced()
    assert db.status()["commits"]["pending"] == 0
    db.close()


def test_with_two_databases_waits_for_both(model, fresh):
    fast = opened(model, fresh, "gate-fast")
    slow = opened(model, fresh, "gate-slow")
    model.fault("delay 1000", 1, "POST")
    slow.execute("INSERT INTO t VALUES (1)")
    time.sleep(0.1)
    fast.execute("INSERT INTO t VALUES (1)")
    started = time.monotonic()
    with gate(fast, slow):
        pass
    assert time.monotonic() - started >= 0.7
    assert slow.status()["commits"]["pending"] == 0
    assert fast.status()["commits"]["pending"] == 0
    fast.close()
    slow.close()


def test_refuses_what_it_cannot_take_before_the_block_runs(model, fresh):
    db = opened(model, fresh, "gate-invalid")
    ran = []
    for args, kwargs in (
        ((), {}),
        ((sqlite3.connect(":memory:"),), {}),
        ((db,), {"timeout": -1}),
        ((db,), {"timeout": "5"}),
    ):
        with pytest.raises(UrsulaSetupError) as invalid, gate(*args, **kwargs):
            ran.append(args)
        assert (invalid.value.kind, invalid.value.action) == ("invalid_argument", "fix")
    assert ran == []
    db.close()


def test_agate_holds_the_block_and_waits_on_the_loop(model, fresh):
    async def main() -> None:
        one = opened(model, fresh, "agate-one")
        two = opened(model, fresh, "agate-two")
        model.fault("delay 1000", 2, "POST")
        one.execute("INSERT INTO t VALUES (1)")
        two.execute("INSERT INTO t VALUES (1)")
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.05)
                ticks += 1

        ticking = asyncio.create_task(ticker())
        started = time.monotonic()
        async with agate(one, two):
            pass
        assert time.monotonic() - started >= 0.9
        assert ticks >= 10
        ticking.cancel()
        assert one.status()["commits"]["pending"] == 0
        assert two.status()["commits"]["pending"] == 0

        # Its own error waits, then passes.
        model.fault("delay 1000", 1, "POST")
        one.execute("INSERT INTO t VALUES (2)")
        started = time.monotonic()
        with pytest.raises(KeyError):
            async with agate(one):
                raise KeyError("the block's")
        assert time.monotonic() - started >= 0.9

        # The timeout, then a cancellation: neither leaves a ticket.
        model.fault("delay 1500", 1, "POST")
        one.execute("INSERT INTO t VALUES (3)")
        with pytest.raises(TimeoutError):
            async with agate(one, two, timeout=0.2):
                pass
        assert waiting(one) == 0 and waiting(two) == 0

        async def gated() -> None:
            async with agate(one):
                pass

        task = asyncio.create_task(gated())
        await asyncio.sleep(0.2)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert waiting(one) == 0
        await one.asynced()
        one.close()
        two.close()

    asyncio.run(main())


def test_agate_passes_a_refused_write_at_once_and_raises_a_reset(stream, server, fresh):
    async def main() -> None:
        db = await sqlite_ursula.aopen(stream.url, dir=fresh(), isolation_level=None)
        db.execute("CREATE TABLE t(x)")
        await db.asynced()
        taker = Child(stream.url, fresh())
        with pytest.raises(UrsulaResetError) as reset:
            async with agate(db, timeout=30):
                db.execute("SELECT 1").fetchall()
        assert reset.value.kind == "fenced"
        taker.stop()
        db.close()

        if not server.on_model:
            return
        refusing = opened(server, fresh, "agate-refused", max_pending_bytes=1)
        server.fault("delay 3000", 1, "POST")
        refusing.execute("INSERT INTO t VALUES (1)")
        started = time.monotonic()
        with pytest.raises(UrsulaWriteRefusedError):
            async with agate(refusing):
                refusing.execute("INSERT INTO t VALUES (2)")
        assert time.monotonic() - started < 2
        await refusing.asynced()
        refusing.close()

    asyncio.run(main())
