"""Forks: a child forked after the parent opened a database raises ``forked`` from the package's
calls and from the inherited connection's, whose ``close()`` closes nothing, while the parent goes
on. A child forked before the parent started the extension opens databases as any process does:
import at module level, open in the worker."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import warnings

import pytest
from conftest import env

import sqlite_ursula
from sqlite_ursula import UrsulaSetupError

pytestmark = pytest.mark.skipif(not hasattr(os, "fork"), reason="no fork on this platform")


def in_child(run) -> object:
    """Forks, runs ``run()`` in the child, and returns what it returned, through a pipe."""
    r, w = os.pipe()
    with warnings.catch_warnings():
        # CPython warns on a fork of a process with threads: the package's threads are the point.
        warnings.simplefilter("ignore", DeprecationWarning)
        pid = os.fork()
    if pid == 0:
        os.close(r)
        try:
            out = {"ok": True, "result": run()}
        except BaseException as e:
            out = {"ok": False, "error": repr(e)}
        os.write(w, json.dumps(out).encode())
        os._exit(0)
    os.close(w)
    with os.fdopen(r, "rb") as f:
        data = f.read()
    _, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0, status
    out = json.loads(data)
    assert out["ok"], out["error"]
    return out["result"]


def test_a_child_forked_after_an_open_raises_forked_and_the_parent_goes_on(stream, fresh):
    db = sqlite_ursula.open(stream.url, dir=fresh(), isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced()

    def calls() -> dict[str, list[str]]:
        seen: dict[str, list[str]] = {}
        for name, call in {
            "open": lambda: sqlite_ursula.open(stream.url + "-other", dir=fresh()),
            "statement": lambda: db.execute("INSERT INTO t VALUES (2)"),
            "synced": lambda: db.synced(),
            "status": lambda: db.status(),
            "on_reset": lambda: db.on_reset(print),
            "commit": lambda: db.commit(),
            "set_token": lambda: sqlite_ursula.set_token("t"),
        }.items():
            try:
                call()
                seen[name] = ["returned"]
            except UrsulaSetupError as e:
                seen[name] = [e.kind, e.action]
        # Closing the inherited connection touches nothing of the parent's.
        db.close()
        seen["close"] = ["returned"]
        return seen

    seen = in_child(calls)
    assert seen.pop("close") == ["returned"]
    assert seen == {name: ["forked", "fix"] for name in seen}
    db.execute("INSERT INTO t VALUES (1)")
    db.synced()
    assert db.execute("SELECT x FROM t").fetchall() == [(1,)]
    db.close()


def test_a_child_forked_before_the_extension_started_opens_as_any_process(stream, fresh):
    script = textwrap.dedent(
        """
        import json, os, sys, warnings
        import sqlite_ursula

        url, root = sys.argv[1], sys.argv[2]
        pid = os.fork()
        if pid == 0:
            db = sqlite_ursula.open(url, dir=root, isolation_level=None)
            db.execute("CREATE TABLE t(x)")
            db.execute("INSERT INTO t VALUES (1)")
            db.synced()
            rows = db.execute("SELECT x FROM t").fetchall()
            db.close()
            print(json.dumps(rows), flush=True)
            os._exit(0)
        _, status = os.waitpid(pid, 0)
        sys.exit(os.waitstatus_to_exitcode(status))
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", script, stream.url, fresh()],
        env=env(),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert out.returncode == 0, out.stderr
    assert json.loads(out.stdout) == [[1]]
