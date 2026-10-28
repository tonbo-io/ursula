"""``synced()`` and the events thread against the model server, whose faults hold the stream's
answers back: it returns once the stream holds every commit made before the call, resolves any
number of waiting threads through one watermark, gives up at its timeout and at Ctrl-C while the
commits keep replicating, asks again for what a gap in the events may have dropped, and survives
the loss of its events thread."""

from __future__ import annotations

import signal
import sqlite3
import threading
import time

import pytest

import sqlite_ursula
from sqlite_ursula import _control
from sqlite_ursula import _events


def opened(model, fresh, label: str):
    from conftest import new_stream

    s = new_stream(model, label)
    db = sqlite_ursula.open(s.url, dir=fresh(), isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced()
    return s, db


def test_returns_once_the_stream_holds_every_commit_made_before_the_call(model, fresh):
    s, db = opened(model, fresh, "synced")
    for i in range(5):
        db.execute("INSERT INTO t VALUES (?)", (i,))
    db.synced()
    commits = db.status()["commits"]
    assert commits["pending"] == 0 and commits["pending_bytes"] == 0
    assert model.frames(s.path) == {"claims": 1, "commits": 6}
    # With nothing new to send it still makes a round trip, and returns.
    db.synced()
    db.close()


def test_waits_while_the_stream_holds_its_answer_back(model, fresh):
    _, db = opened(model, fresh, "wait")
    model.fault("delay 1000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    started = time.monotonic()
    db.synced()
    assert time.monotonic() - started >= 0.9
    db.close()


def test_resolves_hundreds_of_waiting_threads_through_one_watermark(model, fresh):
    _, db = opened(model, fresh, "many")
    model.fault("delay 1000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    failed: list[BaseException] = []

    def wait() -> None:
        try:
            db.synced(timeout=30)
        except BaseException as e:
            failed.append(e)

    threads = [threading.Thread(target=wait) for _ in range(200)]
    for i, t in enumerate(threads):
        t.start()
        if i % 50 == 0:
            db.execute("INSERT INTO t VALUES (?)", (i,))
    for t in threads:
        t.join()
    assert failed == []
    assert db.status()["commits"]["pending"] == 0
    db.close()


def test_gives_up_at_its_timeout_while_the_commits_keep_replicating(model, fresh):
    s, db = opened(model, fresh, "timeout")
    model.fault("delay 1500", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    with pytest.raises(TimeoutError):
        db.synced(timeout=0.2)
    db.synced()
    assert model.frames(s.path) == {"claims": 1, "commits": 2}
    db.close()


def test_ctrl_c_interrupts_it(model, fresh):
    _, db = opened(model, fresh, "interrupt")
    model.fault("delay 2000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    main = threading.main_thread().ident
    assert main == threading.get_ident()
    timer = threading.Timer(0.3, signal.pthread_kill, (main, signal.SIGINT))
    started = time.monotonic()
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        db.synced()
    assert time.monotonic() - started < 1.5
    timer.join()
    db.synced()
    assert db.status()["commits"]["pending"] == 0
    db.close()


def test_asks_again_for_what_a_gap_in_the_events_may_have_dropped(model, fresh):
    _, db = opened(model, fresh, "gap")
    model.fault("delay 500", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    failed: list[BaseException] = []

    def wait() -> None:
        try:
            db.synced(timeout=30)
        except BaseException as e:
            failed.append(e)

    threads = [threading.Thread(target=wait) for _ in range(10)]
    for t in threads:
        t.start()
    time.sleep(0.1)
    # As the extension reports one when events fell out of the queue before the thread read them.
    with _control.lock():
        _events.events().handle([{"event": "gap", "dropped": 1}])
    for t in threads:
        t.join()
    assert failed == []
    assert db.status()["commits"]["pending"] == 0
    db.close()


def test_a_lost_events_thread_is_replaced_and_its_waits_go_on(model, fresh):
    _, db = opened(model, fresh, "replaced")
    model.fault("delay 1000", 1, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    first = _events.events()
    done = threading.Event()
    failed: list[BaseException] = []

    def wait() -> None:
        try:
            db.synced(timeout=30)
        except BaseException as e:
            failed.append(e)
        done.set()

    waiter = threading.Thread(target=wait)
    waiter.start()
    time.sleep(0.1)
    # Its connection is gone: its next call fails, and the thread with it.
    dead = sqlite3.connect(":memory:")
    dead.close()
    first.conn = dead
    assert done.wait(30)
    waiter.join()
    assert failed == []
    assert _events.events() is not first
    db.synced()
    db.close()
