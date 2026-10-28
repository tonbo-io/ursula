"""The process's control connection, the one way the package calls the extension's SQL functions
on it, ``classify``, and what a fork leaves of the process's state.

The registry of bound databases and the replication driver are the process's: one control
connection serves every thread. Its calls are short (every call that could wait is made with
timeout 0, or on a connection of its own), so one lock serializes them. The events thread
(``_events``) takes the same lock while it applies events, so a request or a ticket is registered
before any event about it applies.

A fork copies the process without its threads. A child forked after the parent started the
extension (an open or an import) is marked forked, and the package's calls in it (``open``,
``import_sqlite``, ``set_token``) raise ``forked`` at once, before they could touch a lock that
another thread of the parent held at the fork. So do the calls of a connection the parent made
(``_connection``), whose ``close()`` touches nothing. A child forked before that starts the
extension as any process does. A platform without ``fork`` (Windows) registers no hook.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from . import _host
from .errors import UrsulaError
from .errors import UrsulaSetupError
from .errors import error_of

_lock = threading.RLock()
_control: sqlite3.Connection | None = None
_library: _host.Library | None = None
#: Whether this process started the extension: an open or an import ran.
_started = False
#: Whether this process is a child forked after its parent started the extension.
_forked = False
#: Increases at each fork in the child: a connection made before it is the parent's.
_generation = 0
#: What a fork left in the child: kept, never closed, since a thread of the parent may have held
#: their locks at the fork.
_inherited: list[object] = []


def lock() -> threading.RLock:
    """The lock of the control connection and of the events' state."""
    return _lock


def generation() -> int:
    """This process's generation: a connection made under another one belongs to the parent."""
    return _generation


def forked_error() -> UrsulaSetupError:
    """``forked``: what the guarded calls (``open``, ``import_sqlite``, ``set_token`` and the
    overridden calls of an inherited connection) raise in a child forked after the parent started
    the extension."""
    return UrsulaSetupError(
        "forked: this process forked after its parent opened an Ursula database, and the "
        "databases and the replication of the parent are not this process's: open databases "
        "after the fork (gunicorn's post_fork hook, Celery's worker_process_init signal, or "
        "multiprocessing's spawn or forkserver start method)",
        "forked",
        "fix",
    )


def check_fork() -> None:
    """Raises ``forked`` in a child forked after the parent started the extension."""
    if _forked:
        raise forked_error()


def start() -> None:
    """Marks the extension started: a child forked from now on is forked."""
    global _started
    check_fork()
    _started = True


def library() -> _host.Library:
    """The library the process loaded, loading it first."""
    control()
    assert _library is not None
    return _library


def control() -> sqlite3.Connection:
    """The control connection, loading the extension at the first call."""
    global _control, _library
    check_fork()
    with _lock:
        if _control is None:
            lib = _host.library()
            _control = _host.connection(lib)
            _library = lib
        return _control


def connection() -> sqlite3.Connection:
    """A new connection with the process's extension loaded, for a call that waits."""
    return _host.connection(library())


def parsed(text: object) -> Any:
    """A SQL function's JSON result."""
    return json.loads(text) if isinstance(text, str) and text else None


def call(sql: str, *args: object) -> Any:
    """One of the extension's SQL functions on the control connection: its JSON result, or its
    error as what :func:`classify` makes of it."""
    db = control()
    try:
        with _lock:
            row = db.execute(sql, args).fetchone()
    except sqlite3.Error as error:
        classified = classify(error, None)
        if classified is error:
            raise
        raise classified from error
    return parsed(row[0] if row else None)


def classify(error: BaseException, path: str | None) -> BaseException:
    """What to raise for ``error``, which a statement or an Ursula SQL function raised.

    The :class:`UrsulaError` of what the extension's ``ursula_classify`` makes of it, or ``error``
    itself when the extension says it is not its failure (a constraint, a syntax error, an I/O
    error of a database used through ``ATTACH DATABASE``). ``path`` is the bound database the
    statement ran on, whose state names a statement's I/O error, which says nothing.
    """
    if isinstance(error, UrsulaError) or not isinstance(error, sqlite3.Error):
        return error
    code = getattr(error, "sqlite_errorcode", None)
    db = _control
    if not isinstance(code, int) or db is None or _forked:
        return error
    try:
        with _lock:
            row = db.execute("SELECT ursula_classify(?, ?, ?)", (str(error), code, path)).fetchone()
        named = parsed(row[0] if row else None)
    except (sqlite3.Error, ValueError):
        return error
    if not isinstance(named, dict):
        return error
    return error_of(named, str(named.get("message", error)), error)


def _after_fork_in_child() -> None:
    global _lock, _control, _forked, _generation
    _lock = threading.RLock()
    _generation += 1
    if _control is not None:
        _inherited.append(_control)
    _control = None
    if _started:
        _forked = True


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork_in_child)
