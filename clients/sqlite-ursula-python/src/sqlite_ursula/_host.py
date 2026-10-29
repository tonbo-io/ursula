"""The host: where the extension is, whether this Python can load it, and loading it.

The extension loads into the stdlib ``sqlite3`` module's SQLite, never into a SQLite of its own:
it registers the "ursula" VFS as that SQLite's default, so every ``sqlite3.connect(path)`` of a
bound database in the process goes through it, a framework's included. A Python can host it when
its ``sqlite3`` loads extensions and its SQLite is new enough. The extension says which SQLite is
new enough: its refusal to load is the one floor, and this module turns it into
``host_unsupported`` with the fix.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .errors import UrsulaSetupError

#: Where the README explains the hosts, with the Docker recipes.
HOSTS_URL = "https://github.com/tonbo-io/ursula/tree/main/clients/sqlite-ursula-python#hosts"

#: ``host_unsupported``'s fix for a Python whose ``sqlite3`` cannot host the extension. It names no
#: SQLite version: the extension's refusal, in the error's message, names the one it needs.
OTHER_PYTHON = (
    "use a Python whose sqlite3 module loads extensions and runs a SQLite the extension "
    "supports: a uv-managed CPython (uv python install 3.13, then uv venv --managed-python), "
    "Homebrew's or conda-forge's, or the python:3.13-alpine image in Docker instead of "
    f"python:3.13-slim ({HOSTS_URL}). python -m sqlite_ursula checks a Python."
)
#: ``host_unsupported``'s fix on a platform the extension has no VFS for.
UNDER_WSL = (
    "run the application under WSL, with a Python there that can host the extension "
    f"({HOSTS_URL}). python -m sqlite_ursula checks a Python."
)
#: ``host_unsupported``'s fix for an install that carries no library.
OWN_LIBRARY = (
    "set SQLITE_URSULA_VFS to a library built from clients/sqlite-vfs, or install on a platform "
    "with a prebuilt one: Linux x64 and arm64 with glibc, Linux x64 with musl, macOS arm64. In "
    "Docker on arm64, use a uv-managed CPython on a glibc base, as recipes/Dockerfile.uv does "
    f"({HOSTS_URL})."
)

#: The library's file name on each platform the extension supports.
FILES = {"linux": "libsqlite_ursula_vfs.so", "darwin": "libsqlite_ursula_vfs.dylib"}

#: The start of every refusal of the extension to load into a host's SQLite (clients/sqlite-vfs
#: `GateRefusal`, design: SQL functions), which its unit tests pin.
REFUSAL = "sqlite-ursula-vfs needs "


@dataclass(frozen=True)
class Library:
    """The extension this process loads."""

    path: str
    #: ``SQLITE_URSULA_VFS`` when that names it, else ``bundled``: the one in the installed wheel.
    source: Literal["SQLITE_URSULA_VFS", "bundled"]


def unsupported(why: str, fix: str) -> UrsulaSetupError:
    """``host_unsupported``: this Python cannot host the extension, for the reason ``why``, and
    ``fix`` says what to do about that reason."""
    return UrsulaSetupError(f"host_unsupported: {why}: {fix}", "host_unsupported", "fix", fix=fix)


def library() -> Library:
    """The extension to load: ``SQLITE_URSULA_VFS`` when set, else the one the wheel bundles.

    Raises ``host_unsupported`` on a platform the extension does not support, and on an install
    without a bundled library (the wheel of a platform with none).
    """
    chosen = os.environ.get("SQLITE_URSULA_VFS", "")
    if chosen:
        return Library(chosen, "SQLITE_URSULA_VFS")
    name = FILES.get(sys.platform)
    if name is None:
        raise unsupported(f"the extension has no VFS for {sys.platform}", UNDER_WSL)
    bundled = Path(__file__).parent / "_lib" / name
    if not bundled.is_file():
        raise unsupported(
            "this install of sqlite-ursula carries no extension for this platform", OWN_LIBRARY
        )
    return Library(str(bundled), "bundled")


def loads_extensions() -> bool:
    """Whether this Python's ``sqlite3`` was built to load extensions at all."""
    return hasattr(sqlite3.Connection, "enable_load_extension")


def load(conn: sqlite3.Connection, lib: Library) -> None:
    """Loads ``lib`` into ``conn``: the VFS once per process, the SQL functions on ``conn``.

    Extension loading is on only for the load: the same switch enables SQL's ``load_extension()``.
    The extension's refusal of this SQLite is ``host_unsupported``. A library SQLite cannot load,
    or one without ``ursula_classify`` (older than this package), is ``local_setup``.
    """
    if not loads_extensions():
        raise unsupported(
            "this Python's sqlite3 module was built without loadable extensions, as the "
            "python.org macOS installer's, Apple's and pyenv's default builds are",
            OTHER_PYTHON,
        )
    conn.enable_load_extension(True)
    try:
        conn.load_extension(lib.path)
    except sqlite3.OperationalError as error:
        if REFUSAL in str(error):
            raise unsupported(str(error), OTHER_PYTHON) from error
        raise UrsulaSetupError(
            f"local_setup: SQLite could not load the sqlite-ursula extension at {lib.path} "
            f"({error})",
            "local_setup",
            "fix",
            cause=error,
        ) from error
    finally:
        conn.enable_load_extension(False)
    try:
        conn.execute("SELECT ursula_classify('', 0, NULL)").fetchone()
    except sqlite3.OperationalError as error:
        raise UrsulaSetupError(
            f"local_setup: the sqlite-ursula extension at {lib.path} has no ursula_classify: it "
            "is older than this package, so load the library of the package's own version",
            "local_setup",
            "fix",
            cause=error,
        ) from error


def connection(lib: Library) -> sqlite3.Connection:
    """A new in-memory connection with ``lib`` loaded, for the extension's SQL functions."""
    conn = sqlite3.connect(":memory:", check_same_thread=False, isolation_level=None)
    try:
        load(conn, lib)
    except BaseException:
        conn.close()
        raise
    return conn
