"""The host check: ``python -m sqlite_ursula`` says whether this Python can host the extension,
and a library SQLite cannot load fails the first call as ``local_setup``. The refusal of a SQLite
older than the extension's floor (``host_unsupported``) runs in CI on ``python:3.13-slim``, whose
SQLite is older."""

from __future__ import annotations

import subprocess
import sys

from conftest import env


def run(*args: str, **extra: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        env={**env(), **extra},
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_the_doctor_says_this_python_can_host_the_extension():
    out = run("-m", "sqlite_ursula")
    assert out.returncode == 0, out.stdout + out.stderr
    assert "result: ok" in out.stdout
    assert "extension loading: yes" in out.stdout


def test_a_library_sqlite_cannot_load_is_local_setup():
    out = run("-m", "sqlite_ursula", SQLITE_URSULA_VFS="/nonexistent/libsqlite_ursula_vfs.so")
    assert out.returncode == 1
    assert "result: local_setup" in out.stdout
    script = (
        "import sqlite_ursula\n"
        "try:\n"
        "    sqlite_ursula.set_token(None)\n"
        "except sqlite_ursula.UrsulaSetupError as e:\n"
        "    print(e.kind, e.action)\n"
    )
    out = run("-c", script, SQLITE_URSULA_VFS="/nonexistent/libsqlite_ursula_vfs.so")
    assert out.stdout.split() == ["local_setup", "fix"], out.stdout + out.stderr
