"""Smoke test of an installed wheel (.github/workflows/sqlite-ursula-python-wheels.yml), run by the
Python under test outside the checkout, where the wheel is installed:

    python smoke.py <version> ok
    python smoke.py <version> <kind> <fix> <why>

With ``SQLITE_URSULA_VFS`` unset, the package loads the library its wheel carries. ``ok``: this
Python hosts it, ``python -m sqlite_ursula`` says so, and a plain database works through its VFS,
which the extension made SQLite's default. Otherwise ``python -m sqlite_ursula`` and ``open()`` fail
with ``<kind>``, the reason contains ``<why>``, and the fix starts with ``<fix>``. No server is
needed.
"""

from __future__ import annotations

import os
import platform
import sqlite3
import subprocess
import sys
import sysconfig
import tempfile
from pathlib import Path

import sqlite_ursula

# The package reads it at the first call that loads the extension.
os.environ.pop("SQLITE_URSULA_VFS", None)

version, expect, *rest = sys.argv[1:]
assert sqlite_ursula.__version__ == version, (sqlite_ursula.__version__, version)
assert "site-packages" in Path(sqlite_ursula.__file__).parts, sqlite_ursula.__file__

doctor = subprocess.run(
    [sys.executable, "-m", "sqlite_ursula"], capture_output=True, text=True, timeout=60
)
print(doctor.stdout, doctor.stderr, sep="", end="")

if expect == "ok":
    assert doctor.returncode == 0
    assert "(bundled)\n" in doctor.stdout
    assert "result: ok" in doctor.stdout
    # Loads the extension into this process's sqlite3, as open() does, without a server.
    sqlite_ursula.set_token(None)
    db = sqlite3.connect(str(Path(tempfile.mkdtemp(prefix="smoke-")) / "plain.db"))
    db.executescript("PRAGMA journal_mode=WAL; CREATE TABLE t(x); INSERT INTO t VALUES (42)")
    assert db.execute("SELECT x FROM t").fetchone() == (42,)
    db.close()
else:
    fix, why = rest
    assert doctor.returncode == 1
    assert f"result: {expect}:" in doctor.stdout
    (reason,) = [line for line in doctor.stdout.splitlines() if line.startswith("why: ")]
    assert why in reason, reason
    assert f"\nfix: {fix}" in doctor.stdout
    try:
        sqlite_ursula.open("http://127.0.0.1:9/smoke/db", timeout=5)
    except sqlite_ursula.UrsulaSetupError as error:
        assert (error.kind, error.action) == (expect, "fix"), (error.kind, error.action)
        assert why in str(error), str(error)
        assert error.fix is not None and error.fix.startswith(fix), error.fix
    else:
        raise AssertionError("open() did not fail")

build = "free-threaded" if sysconfig.get_config_var("Py_GIL_DISABLED") else "GIL"
print(
    f"{expect}: sqlite-ursula {version} on {sys.platform}-{platform.machine()}, "
    f"CPython {platform.python_version()} ({build}), SQLite {sqlite3.sqlite_version}"
)
