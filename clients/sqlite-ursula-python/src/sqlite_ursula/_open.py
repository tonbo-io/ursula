"""``open``, ``import_sqlite`` and ``set_token``: the root module's calls of the extension.

An open binds with ``ursula_open`` at timeout 0 and waits on the events thread while the bind
goes on, as Node's does. The extension shares one bind between concurrent opens of a stream,
from any thread. Its own deadline for the bind is far past ``timeout``, which governs the open: on
the timeout, or on Ctrl-C or any other exception once the request is known, the open cancels the
bind (``ursula_cancel``).

``autocommit=False`` (Python 3.12 or newer) is refused. In that mode the connection is always in
a transaction: CPython runs its own ROLLBACK after a refused COMMIT, which hides the refusal, and a
connection kept open holds a read snapshot after any SELECT, which stops checkpoints of the WAL.
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

from . import _control
from . import _events
from ._args import flag
from ._args import invalid
from ._args import seconds
from ._connection import UrsulaConnection
from .errors import UrsulaError
from .errors import UrsulaSetupError
from .errors import UrsulaUnavailableError
from .errors import error_of

#: The extension's own deadline for a bind: far past any ``timeout``, which governs the open.
EXTENSION_OPEN_MS = 24 * 60 * 60 * 1000
#: How long an open may take without ``timeout``.
DEFAULT_OPEN_S = 30.0
#: How long an import may take without ``timeout``.
DEFAULT_IMPORT_S = 600.0
#: ``sqlite3.connect``'s busy timeout unless the caller passes one: the connections of a binding
#: share its files.
DEFAULT_BUSY_S = 5.0
#: ``sqlite3.connect`` arguments ``open`` decides itself.
OWN = frozenset({"database", "uri"})


def _url(url: object) -> str:
    if not isinstance(url, str):
        raise invalid(f"url must be a string, not {type(url).__name__}")
    return url


def _token(token: object) -> str | None:
    if token is not None and not isinstance(token, str):
        raise invalid(
            f"token must be a string, not {type(token).__name__}: for a token that changes, call "
            "set_token() with each new one"
        )
    return token


def set_token(token: str | None) -> None:
    """Sets the bearer token every request of the process carries from now on. ``None`` or ``""``
    goes back to ``URSULA_VFS_TOKEN_FILE``, the file the extension reads the token from, when set.
    A change resends the requests a 401 refused.

    One token per process: the last call's, or the last ``open()``'s with a ``token``, applies.
    """
    chosen = _token(token)
    _control.call("SELECT ursula_set_token('', ?)", chosen or "")


def open(  # noqa: A001 - the package's entry point, as Node's `open`
    url: str,
    *,
    token: str | None = None,
    dir: str | os.PathLike[str] | None = None,  # noqa: A002 - the option's name, as Node's
    read_only: bool = False,
    recreate: bool = False,
    max_pending_bytes: int | None = None,
    timeout: float = DEFAULT_OPEN_S,
    busy_timeout: float = DEFAULT_BUSY_S,
    **connect_kwargs: Any,
) -> UrsulaConnection:
    """Opens the database the stream at ``url`` holds, and makes this process its owner.

    The first open of a stream in the process binds it, catches the local cache up and claims the
    stream, fencing every earlier owner. Later opens of it in the process return another
    connection on the same binding, with no network. Concurrent opens, from any thread, share one
    bind and its outcome.

    - ``token``: the bearer token requests carry, applied with :func:`set_token` before the open.
    - ``dir``: the cache root, created when missing. By default ``<user cache>/sqlite-ursula/0.8``.
    - ``read_only``: claims nothing, so it never fences the owner, and writes fail with
      ``SQLITE_READONLY``. In a process that owns the stream it is a read-only connection on the
      owner's binding. Otherwise the extension builds or catches up a reader cache of its own.
    - ``recreate``: when the stream is missing and the local cache holds data, create the stream
      again and move the cache aside, instead of failing with ``not_found``.
    - ``max_pending_bytes``: how many bytes of compressed commits may wait for the stream before
      writes fail with ``backlog_full``. By default 64 MiB.
    - ``timeout``: how many seconds the open may take, 30 by default.
    - ``busy_timeout``: ``sqlite3.connect``'s ``timeout``, 5 seconds by default, since the
      connections of a binding share its files.
    - Every other keyword goes to ``sqlite3.connect``. ``check_same_thread`` is ``False`` unless
      given, since frameworks run statements on threads of their own. A ``factory`` must be a
      subclass of :class:`UrsulaConnection`. ``autocommit=False`` is refused: in that mode the
      connection is always in a transaction, so a refused COMMIT would be hidden by CPython's own
      ROLLBACK, and a connection kept open would hold a read snapshot that stops checkpoints.

    Returns an :class:`UrsulaConnection`, a ``sqlite3.Connection`` subclass. Statements are local:
    commit, then call ``db.synced()`` before anything outside the process acts on what you wrote or
    read.

    Raises :class:`UrsulaSetupError` when it cannot succeed as asked (``host_unsupported`` when
    this Python cannot load the extension), and :class:`UrsulaUnavailableError` when the timeout
    passed first: the last condition the open met, or ``cancelled`` when it met none.
    """
    stream = _url(url)
    chosen = _token(token)
    settings: dict[str, object] = {"timeout_ms": EXTENSION_OPEN_MS}
    if max_pending_bytes is not None:
        if (
            isinstance(max_pending_bytes, bool)
            or not isinstance(max_pending_bytes, int)
            or max_pending_bytes < 0
        ):
            raise invalid(
                f"max_pending_bytes must be a whole number of bytes, not {max_pending_bytes!r}"
            )
        settings["max_pending_bytes"] = max_pending_bytes
    if flag("recreate", recreate):
        settings["recreate"] = True
    if flag("read_only", read_only):
        # With `recreate` too, the extension refuses it: `invalid_argument`.
        settings["read_only"] = True
    limit = seconds("timeout", timeout, positive=True)
    busy = seconds("busy_timeout", busy_timeout, positive=False)
    for key in OWN.intersection(connect_kwargs):
        raise invalid(f"open() decides sqlite3.connect's {key}")
    if connect_kwargs.get("autocommit") is False:
        raise invalid(
            "autocommit=False keeps the connection in a transaction, where a refused COMMIT is "
            "hidden by the ROLLBACK that follows it: use the default transaction control, "
            "isolation_level=None, or autocommit=True"
        )
    factory = connect_kwargs.pop("factory", UrsulaConnection)
    if not (isinstance(factory, type) and issubclass(factory, UrsulaConnection)):
        raise invalid(f"factory must be a subclass of UrsulaConnection, not {factory!r}")
    root = None if dir is None else os.fspath(dir)
    _control.start()
    # The token is applied once, here, never in `ursula_open`'s options, which a bind sends
    # again on each of its retries, over a token applied meanwhile.
    if chosen is not None:
        set_token(chosen)
    binding = _bind(stream, root, json.dumps(settings), limit)
    return _connect(binding, read_only, busy, factory, connect_kwargs)


def _bind(stream: str, root: str | None, options: str, timeout: float) -> dict[str, Any]:
    """Binds ``stream``: ``ursula_open`` with timeout 0, waiting on the events thread while it goes
    on.

    Any exception once the request is known (Ctrl-C included, which Python delivers right after
    the call returns) cancels it, so no bind goes on to claim the stream after the open gave up.
    """
    deadline = time.monotonic() + timeout
    while True:
        opened: dict[str, Any] | None = None
        ev: _events.Events | None = None
        waiter = None
        try:
            with _control.lock():
                # The thread first, so its cursor precedes the request, and the waiter registered
                # in the step that got the request, before any event about it applies.
                ev = _events.events()
                opened = _control.call("SELECT ursula_open(?, ?, ?, 0)", stream, root, options)
                if opened["done"]:
                    return opened
                waiter = ev.wait_open(int(opened["request"]))
            left = deadline - time.monotonic()
            woke = left > 0 and waiter.wait(left)
        except BaseException as error:
            if opened is not None and not opened["done"]:
                request = int(opened["request"])
                if ev is not None and waiter is not None:
                    ev.forget_open(request, waiter)
                _give_up(stream, request, timeout)
                raise
            # Another thread's open of the stream, which this one shared, was cancelled.
            if (
                isinstance(error, UrsulaUnavailableError)
                and error.kind == "cancelled"
                and time.monotonic() < deadline
            ):
                continue
            raise
        if not woke:
            request = int(opened["request"])
            ev.forget_open(request, waiter)
            raise _give_up(stream, request, timeout)


def _give_up(stream: str, request: int, timeout: float) -> UrsulaError:
    """The open gave up: cancels ``request``, and the error is the last condition it met, or
    ``cancelled``."""
    try:
        ended = _control.call("SELECT ursula_cancel(?)", request)
    except UrsulaError:
        # It resolved meanwhile and its result was taken.
        ended = None
    if ended is None or ended.get("kind") == "cancelled":
        return UrsulaUnavailableError(
            f"cancelled: the open of {stream} gave up after {timeout:g} s before it met any "
            "condition",
            "cancelled",
            "retry",
        )
    return error_of(
        ended, f"{ended['message']}: the open of {stream} gave up after {timeout:g} s", None
    )


def _connect(
    binding: dict[str, Any],
    read_only: bool,
    busy: float,
    factory: type[UrsulaConnection],
    connect_kwargs: dict[str, Any],
) -> UrsulaConnection:
    """A connection to what ``ursula_open`` bound."""
    path = str(binding["path"])
    connect_kwargs.setdefault("check_same_thread", False)
    database = f"file:{urllib.parse.quote(path)}?mode=ro" if read_only else path
    try:
        conn = sqlite3.connect(
            database, timeout=busy, factory=factory, uri=read_only, **connect_kwargs
        )
    except (sqlite3.Error, TypeError, ValueError) as error:
        raise UrsulaSetupError(
            f"invalid_argument: bound {binding['name']}, then sqlite3 refused to open {path} "
            f"({error})",
            "invalid_argument",
            "fix",
            cause=error,
        ) from error
    conn._bind(str(binding["name"]), path)
    return conn


@dataclass(frozen=True)
class Imported:
    """What :func:`import_sqlite` published: the stream's first snapshot."""

    #: The snapshot's stream offset, the end of the import's claim.
    offset: int
    digest: str
    pages: int
    #: The compressed snapshot's size.
    bytes: int


def import_sqlite(
    url: str,
    file: str | os.PathLike[str],
    *,
    token: str | None = None,
    timeout: float = DEFAULT_IMPORT_S,
) -> Imported:
    """Imports the plain SQLite database ``file`` into a new stream at ``url``, as the stream's
    first snapshot, page for page. Then :func:`open` the stream as any other.

    The file must have 4096-byte pages (``PRAGMA page_size = 4096; VACUUM INTO ...`` makes such a
    copy), no ``-wal`` beside it and no hot journal. It is only read. The stream must be missing or
    empty: anything else fails with ``stream_state`` and nothing is published. ``token`` is the
    bearer token of the import's requests alone.

    It blocks the calling thread until it is done, for at most ``timeout`` seconds (10 minutes by
    default). Errors are :class:`UrsulaSetupError` (such as ``invalid_argument`` for the file) and
    :class:`UrsulaUnavailableError`, whose action is ``fix``: the stream may hold the import's
    claim.
    """
    stream = _url(url)
    chosen = _token(token)
    limit = seconds("timeout", timeout, positive=True)
    source = os.fspath(file)
    settings = {} if chosen is None else {"token": chosen}
    _control.start()
    # A connection of its own: the import blocks for as long as it takes.
    conn = _control.connection()
    try:
        row = conn.execute(
            "SELECT ursula_import(?, ?, ?, ?)",
            (stream, source, json.dumps(settings), max(1, round(limit * 1000))),
        ).fetchone()
    except sqlite3.Error as error:
        classified = _control.classify(error, None)
        if classified is error:
            raise
        raise classified from error
    finally:
        conn.close()
    done = _control.parsed(row[0])
    return Imported(
        offset=int(done["offset"]),
        digest=str(done["digest"]),
        pages=int(done["pages"]),
        bytes=int(done["bytes"]),
    )
