"""Errors through ``ursula_classify``: a takeover resets the old owner (its ``synced()`` raises,
then its ``on_reset`` callbacks run once, and every statement raises the reset), a refused write
fails alone, and SQLite's own errors stay SQLite's. Refused writes and resets are
``sqlite3.OperationalError`` too, and a transaction helper's ROLLBACK after one does not hide it.
"""

from __future__ import annotations

import pickle
import sqlite3
import threading

import pytest
from conftest import Child

import sqlite_ursula
from sqlite_ursula import UrsulaError
from sqlite_ursula import UrsulaResetError
from sqlite_ursula import UrsulaWriteRefusedError

SQLITE_IOERR_READ = 266
SQLITE_IOERR_AUTH = 7178


def test_a_takeover_raises_in_synced_then_runs_the_callbacks_once_and_fails_every_statement(
    stream, fresh
):
    root = fresh()
    owner = sqlite_ursula.open(stream.url, dir=root, isolation_level=None)
    second = sqlite_ursula.open(stream.url, dir=root, isolation_level=None)
    owner.execute("CREATE TABLE t(x)")
    owner.execute("INSERT INTO t VALUES (1)")
    owner.synced()
    heard: list[tuple[str, UrsulaResetError]] = []
    fired = threading.Event()

    @owner.on_reset
    def listener(error: UrsulaResetError) -> None:
        heard.append((threading.current_thread().name, error))
        fired.set()

    def removed(error: UrsulaResetError) -> None:
        raise AssertionError("a removed callback ran")

    owner.on_reset(removed)
    owner.off_reset(removed)
    second_heard = threading.Event()
    second.on_reset(lambda error: second_heard.set())

    taker = Child(stream.url, fresh())
    with pytest.raises(UrsulaResetError) as reset:
        owner.synced(timeout=30)
    assert (reset.value.kind, reset.value.action) == ("fenced", "stop")
    assert isinstance(reset.value, sqlite3.OperationalError)
    assert fired.wait(10)
    assert second_heard.wait(10)
    assert len(heard) == 1
    assert heard[0][0] != threading.current_thread().name
    assert heard[0][1].kind == "fenced"

    with pytest.raises(sqlite3.OperationalError) as read:
        owner.execute("SELECT x FROM t").fetchall()
    assert isinstance(read.value, UrsulaResetError)
    assert (read.value.kind, read.value.sqlite_errorcode) == ("fenced", SQLITE_IOERR_READ)
    assert isinstance(read.value.__cause__, sqlite3.OperationalError)
    with pytest.raises(UrsulaResetError):
        second.execute("INSERT INTO t VALUES (2)")
    with pytest.raises(UrsulaResetError):
        owner.synced()
    assert owner.status()["reset"]["kind"] == "fenced"

    taker.send(exec="INSERT INTO t VALUES (3)")
    taker.send(synced=30)
    taker.stop()
    owner.close()
    second.close()
    # A new open takes the stream back, and reads what the taker wrote.
    back = sqlite_ursula.open(stream.url, dir=root)
    assert back.execute("SELECT x FROM t ORDER BY x").fetchall() == [(1,), (3,)]
    back.execute("INSERT INTO t VALUES (4)")
    back.commit()
    back.synced()
    assert len(heard) == 1
    back.close()


def test_refuses_an_unsupported_write_alone_and_leaves_sqlites_own_errors(stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh(), isolation_level=None)
    db.executescript("CREATE TABLE t(x UNIQUE); INSERT INTO t VALUES (1)")
    with pytest.raises(sqlite3.IntegrityError) as constraint:
        db.execute("INSERT INTO t VALUES (1)")
    assert not isinstance(constraint.value, UrsulaError)
    db.execute("PRAGMA locking_mode=EXCLUSIVE")
    with pytest.raises(UrsulaWriteRefusedError) as refused:
        db.execute("INSERT INTO t VALUES (2)")
    assert (refused.value.kind, refused.value.action) == ("unsupported", "fix")
    assert refused.value.sqlite_errorcode == SQLITE_IOERR_AUTH
    assert isinstance(refused.value, sqlite3.OperationalError)
    # It is picklable, for multiprocessing.
    again = pickle.loads(pickle.dumps(refused.value))
    assert (type(again), again.kind, again.action) == (
        UrsulaWriteRefusedError,
        "unsupported",
        "fix",
    )
    db.close()


def test_refuses_a_write_past_max_pending_bytes_and_keeps_it_across_a_helpers_rollback(
    model, fresh
):
    from conftest import new_stream

    s = new_stream(model, "backlog")
    root = fresh()
    db = sqlite_ursula.open(s.url, dir=root, max_pending_bytes=1, isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced()
    # The next append's answer comes late: its commit stays pending, and holds the backlog full.
    model.fault("delay 3000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(UrsulaWriteRefusedError) as refused:
        db.execute("INSERT INTO t VALUES (2)")
    assert (refused.value.kind, refused.value.action) == ("backlog_full", "retry")
    assert refused.value.sqlite_errorcode == SQLITE_IOERR_AUTH

    # A transaction helper's ROLLBACK after the refused COMMIT does not hide it.
    with pytest.raises(UrsulaWriteRefusedError) as manual:
        try:
            db.execute("BEGIN")
            db.execute("INSERT INTO t VALUES (3)")
            db.execute("COMMIT")
        except UrsulaError:
            db.execute("ROLLBACK")
            raise
    assert manual.value.kind == "backlog_full"
    # Through executescript, which may hold more after it, the ROLLBACK raises the refusal.
    with pytest.raises(UrsulaWriteRefusedError):
        db.execute("INSERT INTO t VALUES (4)")
    with pytest.raises(UrsulaWriteRefusedError):
        db.executescript("ROLLBACK")
    # The legacy transaction of the stdlib: the context manager's COMMIT is refused.
    legacy = sqlite_ursula.open(s.url, dir=root)
    with pytest.raises(UrsulaWriteRefusedError), legacy:
        legacy.execute("INSERT INTO t VALUES (5)")
    assert not legacy.in_transaction

    db.synced()
    db.execute("INSERT INTO t VALUES (6)")
    db.synced()
    # Once a statement succeeded, a ROLLBACK without a transaction is SQLite's error again.
    with pytest.raises(sqlite3.OperationalError) as plain:
        db.execute("ROLLBACK")
    assert not isinstance(plain.value, UrsulaError)
    assert db.execute("SELECT x FROM t ORDER BY x").fetchall() == [(1,), (6,)]
    legacy.close()
    db.close()
