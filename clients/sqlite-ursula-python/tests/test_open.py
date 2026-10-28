"""``open()`` against the e2e server: it binds and claims the stream and returns a
``sqlite3.Connection`` subclass, later opens share the binding, a framework's own
``sqlite3.connect(db.path)`` replicates through the same binding, and what cannot succeed fails
as a setup error or, past the timeout, as an unavailable one. Ctrl-C cancels an open."""

from __future__ import annotations

import signal
import sqlite3
import threading
import time

import pytest
from conftest import Child
from conftest import free_port

import sqlite_ursula
from sqlite_ursula import UrsulaConnection
from sqlite_ursula import UrsulaSetupError
from sqlite_ursula import UrsulaUnavailableError


def test_binds_and_claims_the_stream_and_returns_a_sqlite3_connection(stream, fresh):
    root = fresh()
    db = sqlite_ursula.open(stream.url, dir=root)
    assert isinstance(db, sqlite3.Connection)
    assert isinstance(db, UrsulaConnection)
    assert db.name == stream.path
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (?)", (1,))
    db.commit()
    db.synced()
    status = db.status()
    assert status["name"] == stream.path
    assert status["path"] == db.path
    assert status["state"] == "live"
    assert status["role"] == "owner"
    assert status["commits"]["pending"] == 0
    assert status["commits"]["acknowledged"] >= status["commits"]["logged"]

    # Another open in the process: another connection on the same binding.
    again = sqlite_ursula.open(stream.url, dir=root)
    assert again is not db
    assert again.path == db.path
    assert again.execute("SELECT x FROM t").fetchall() == [(1,)]

    # A framework opens the path with the stdlib alone, and its commits replicate too.
    plain = sqlite3.connect(db.path)
    plain.execute("INSERT INTO t VALUES (2)")
    plain.commit()
    db.synced()
    plain.close()
    again.close()
    db.close()

    # Another process, another cache: it builds from the stream and takes it over.
    child = Child(stream.url, fresh())
    assert child.send(query="SELECT x FROM t ORDER BY x")["rows"] == [[1], [2]]
    child.stop()


def test_concurrent_opens_from_threads_share_one_bind(model, fresh):
    from conftest import new_stream

    s = new_stream(model, "threads")
    root = fresh()
    got: list[UrsulaConnection] = []
    failed: list[BaseException] = []

    def opener() -> None:
        try:
            got.append(sqlite_ursula.open(s.url, dir=root))
        except BaseException as e:
            failed.append(e)

    threads = [threading.Thread(target=opener) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert failed == []
    assert len({db.path for db in got}) == 1
    got[0].execute("CREATE TABLE t(x)")
    got[0].commit()
    got[-1].synced()
    assert model.frames(s.path) == {"claims": 1, "commits": 1}
    for db in got:
        db.close()


def test_a_read_only_open_in_the_owner_reads_its_commits_and_cannot_write(stream, fresh):
    root = fresh()
    db = sqlite_ursula.open(stream.url, dir=root)
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (1)")
    db.commit()
    ro = sqlite_ursula.open(stream.url, dir=root, read_only=True)
    assert ro.path == db.path
    assert ro.execute("SELECT x FROM t").fetchall() == [(1,)]
    with pytest.raises(sqlite3.OperationalError) as refused:
        ro.execute("INSERT INTO t VALUES (2)")
    assert not isinstance(refused.value, sqlite_ursula.UrsulaError)
    assert refused.value.sqlite_errorcode & 0xFF == sqlite3.SQLITE_READONLY
    ro.synced()
    ro.close()
    db.close()


def test_passes_sqlite3_settings_through(stream, fresh):
    class Mine(UrsulaConnection):
        pass

    db = sqlite_ursula.open(stream.url, dir=fresh(), isolation_level=None, factory=Mine)
    assert isinstance(db, Mine)
    assert db.isolation_level is None
    db.execute("CREATE TABLE t(x)")
    assert not db.in_transaction
    db.close()


def test_refuses_what_it_cannot_take_as_a_setup_error_to_fix(stream, fresh):
    with pytest.raises(UrsulaSetupError) as bad_url:
        sqlite_ursula.open("ftp://example.com/a/b", dir=fresh())
    assert (bad_url.value.kind, bad_url.value.action) == ("invalid_url", "fix")
    assert not isinstance(bad_url.value, sqlite3.Error)

    for kwargs in (
        {"token": lambda: "t"},
        {"max_pending_bytes": -1},
        {"timeout": 0},
        {"read_only": "yes"},
        {"factory": sqlite3.Connection},
        {"uri": True},
        # Always in a transaction, where CPython's ROLLBACK after a refused COMMIT hides it.
        {"autocommit": False},
    ):
        with pytest.raises(UrsulaSetupError) as invalid:
            sqlite_ursula.open(stream.url, dir=fresh(), **kwargs)
        assert (invalid.value.kind, invalid.value.action) == ("invalid_argument", "fix"), kwargs

    # The extension's own refusal, as the extension classifies it.
    with pytest.raises(UrsulaSetupError) as both:
        sqlite_ursula.open(stream.url, dir=fresh(), read_only=True, recreate=True)
    assert (both.value.kind, both.value.action) == ("invalid_argument", "fix")


def test_reports_the_last_condition_the_open_met_when_its_timeout_passes(fresh):
    url = f"http://127.0.0.1:{free_port()}/sqlite-e2e/nobody"
    started = time.monotonic()
    with pytest.raises(UrsulaUnavailableError) as gave_up:
        sqlite_ursula.open(url, dir=fresh(), timeout=2)
    assert time.monotonic() - started < 15
    assert gave_up.value.kind in ("unreachable", "cancelled")
    assert gave_up.value.action == "retry"


def test_ctrl_c_cancels_an_open_and_the_stream_is_not_held(model, fresh):
    from conftest import new_stream

    s = new_stream(model, "interrupted")
    root = fresh()
    # The bind's answers are held back longer than the interrupt waits.
    model.fault("delay 3000", 4)
    main = threading.main_thread().ident
    assert main == threading.get_ident()
    timer = threading.Timer(0.3, signal.pthread_kill, (main, signal.SIGINT))
    started = time.monotonic()
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        sqlite_ursula.open(s.url, dir=root)
    assert time.monotonic() - started < 2.5
    timer.join()
    time.sleep(3.5)
    db = sqlite_ursula.open(s.url, dir=root)
    db.execute("CREATE TABLE t(x)")
    db.commit()
    db.synced()
    db.close()
