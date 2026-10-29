"""``open``, ``aopen``, ``import_sqlite`` and ``set_token``: the root module's calls of the
extension. ``plan`` checks an open's arguments and ``opened`` / ``aopened`` bind a stream with
them, which ``tonbo`` calls too once it resolved a database into its stream.

An open binds with ``ursula_open`` at timeout 0 and waits on the events thread while the bind
goes on, as Node's does: ``open`` on a ``threading.Event``, ``aopen`` on the event loop. The
extension shares one bind between concurrent opens of a stream, from any thread or task. Its own
deadline for the bind is far past ``timeout``, which governs the open: on the timeout, or on
Ctrl-C, a cancellation or any other exception once a reply named the request, the open cancels
the bind (``ursula_cancel``). A Ctrl-C inside the first ``ursula_open`` call, before its reply is
read, can miss the cancel.

``autocommit=False`` (Python 3.12 or newer) is refused, and so is setting it later. In that mode
the connection is always in a transaction: CPython runs its own ROLLBACK after a refused COMMIT,
which hides the refusal, and a connection kept open holds a read snapshot after any SELECT, which
stops checkpoints of the WAL.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import urllib.parse
from dataclasses import dataclass
from dataclasses import field
from typing import Any

from . import _control
from . import _events
from ._args import flag
from ._args import invalid
from ._args import seconds
from ._connection import UrsulaConnection
from ._connection import autocommit_false
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
    return opened(
        stream,
        plan(
            token,
            dir,
            read_only,
            recreate,
            max_pending_bytes,
            timeout,
            busy_timeout,
            connect_kwargs,
        ),
    )


async def aopen(
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
    """:func:`open` for asyncio: the same arguments, the same :class:`UrsulaConnection` and the same
    errors, and it waits for the bind on the event loop instead of blocking it.

    Cancelling it, as ``asyncio.timeout`` around it does, cancels the bind (``ursula_cancel``) and
    raises ``CancelledError``, so no bind goes on to claim the stream after the caller gave up.
    """
    stream = _url(url)
    return await aopened(
        stream,
        plan(
            token,
            dir,
            read_only,
            recreate,
            max_pending_bytes,
            timeout,
            busy_timeout,
            connect_kwargs,
        ),
    )


def opened(stream: str, checked: Plan, deadline: float | None = None) -> UrsulaConnection:
    """The open of ``stream`` with the checked arguments ``checked``: it starts the extension,
    applies the token and binds, waiting on a ``threading.Event``. ``deadline`` (a
    ``time.monotonic()`` instant) ends it instead of ``checked.timeout`` from now, for an open whose
    time began earlier (``tonbo``'s, which resolved the stream first)."""
    _apply(checked)
    bind = _Bind(stream, checked, deadline)
    while True:
        waiter = threading.Event()
        try:
            binding = bind.ask(waiter)
            if binding is not None:
                return _connect(checked, binding)
            left = bind.left()
            woke = left > 0 and waiter.wait(left)
        except BaseException as error:
            if bind.again(error):
                continue
            raise
        if not woke:
            raise bind.timed_out()


async def aopened(stream: str, checked: Plan, deadline: float | None = None) -> UrsulaConnection:
    """:func:`opened` for a coroutine: it waits on the event loop."""
    _apply(checked)
    bind = _Bind(stream, checked, deadline)
    while True:
        waiter = _events.LoopEvent()
        try:
            binding = bind.ask(waiter)
            if binding is not None:
                return _connect(checked, binding)
            left = bind.left()
            woke = left > 0 and await waiter.wait(left)
        except BaseException as error:
            if bind.again(error):
                continue
            raise
        if not woke:
            raise bind.timed_out()


@dataclass(frozen=True)
class Plan:
    """An open's checked arguments, all but the stream."""

    #: The token to apply before the bind, if any. Never in the repr.
    token: str | None = field(repr=False)
    root: str | None
    #: ``ursula_open``'s options.
    options: str
    timeout: float
    read_only: bool
    busy: float
    factory: type[UrsulaConnection]
    connect_kwargs: dict[str, Any]


def plan(
    token: object,
    dir: str | os.PathLike[str] | None,  # noqa: A002 - the option's name
    read_only: object,
    recreate: object,
    max_pending_bytes: object,
    timeout: object,
    busy_timeout: object,
    connect_kwargs: dict[str, Any],
) -> Plan:
    """Checks an open's arguments but the stream, before anything starts."""
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
    ro = flag("read_only", read_only)
    if ro:
        # With `recreate` too, the extension refuses it: `invalid_argument`.
        settings["read_only"] = True
    limit = seconds("timeout", timeout, positive=True)
    busy = seconds("busy_timeout", busy_timeout, positive=False)
    for key in OWN.intersection(connect_kwargs):
        raise invalid(f"open() decides sqlite3.connect's {key}")
    if connect_kwargs.get("autocommit") is False:
        raise autocommit_false()
    factory = connect_kwargs.pop("factory", UrsulaConnection)
    if not (isinstance(factory, type) and issubclass(factory, UrsulaConnection)):
        raise invalid(f"factory must be a subclass of UrsulaConnection, not {factory!r}")
    root = None if dir is None else os.fspath(dir)
    return Plan(chosen, root, json.dumps(settings), limit, ro, busy, factory, connect_kwargs)


def _apply(checked: Plan) -> None:
    """Starts the extension and applies the open's token."""
    _control.start()
    # The token is applied once, here, never in `ursula_open`'s options, which a bind sends
    # again on each of its retries, over a token applied meanwhile.
    if checked.token is not None:
        set_token(checked.token)


class _Bind:
    """One open's bind of its stream: ``ursula_open`` with timeout 0, asked again each time its
    waiter wakes, until it is done. ``open`` waits on a ``threading.Event`` and ``aopen`` on a
    :class:`_events.LoopEvent`, through the same steps.

    Any exception once a reply named the request cancels it, in a later pass too (one a gap or a
    lost events thread woke), so no bind goes on to claim the stream after the open gave up. A
    Ctrl-C that lands inside the first ``ursula_open`` call, before its reply is read, can miss the
    cancel: that bind then runs to the extension's own deadline.
    """

    def __init__(self, stream: str, plan: Plan, deadline: float | None) -> None:
        self._stream = stream
        self._plan = plan
        self._deadline = time.monotonic() + plan.timeout if deadline is None else deadline
        # The request of the last reply that was not done: concurrent opens of a stream, and each
        # pass of this one, share it until it resolves.
        self._known: int | None = None
        self._ev: _events.Events | None = None
        self._waiter: _events.Waiter | None = None

    def ask(self, waiter: _events.Waiter) -> dict[str, Any] | None:
        """One pass: the binding when the bind is done, else ``None`` with ``waiter`` registered
        for the request's completion."""
        plan = self._plan
        self._ev = None
        self._waiter = None
        with _control.lock():
            # The thread first, so its cursor precedes the request, and the waiter registered in
            # the step that got the request, before any event about it applies.
            self._ev = _events.events()
            answer = _control.call(
                "SELECT ursula_open(?, ?, ?, 0)", self._stream, plan.root, plan.options
            )
            if answer["done"]:
                return answer
            self._known = int(answer["request"])
            self._ev.wait_request(self._known, waiter)
            self._waiter = waiter
        return None

    def left(self) -> float:
        return self._deadline - time.monotonic()

    def again(self, error: BaseException) -> bool:
        """A pass raised ``error``: whether to ask anew, when another open's bind of the stream,
        which this one shared, was cancelled. Otherwise the request is cancelled and the caller
        raises ``error``."""
        if (
            isinstance(error, UrsulaUnavailableError)
            and error.kind == "cancelled"
            and time.monotonic() < self._deadline
        ):
            self._known = None
            return True
        if self._known is not None:
            self._forget()
            # One that resolved meanwhile is no longer a request: the cancel finds nothing.
            _give_up(self._stream, self._known, self._plan.timeout)
        return False

    def timed_out(self) -> UrsulaError:
        """The timeout passed: the request is cancelled, and the error is the last condition it
        met, or ``cancelled``."""
        assert self._known is not None
        self._forget()
        return _give_up(self._stream, self._known, self._plan.timeout)

    def _forget(self) -> None:
        if self._ev is not None and self._waiter is not None and self._known is not None:
            self._ev.forget_request(self._known, self._waiter)


def _give_up(stream: str, request: int, timeout: float) -> UrsulaError:
    """The open gave up: cancels ``request``, and the error is the last condition it met, or
    ``cancelled``. The cancel waits up to a second for a disk job of the open, so it runs on a
    connection of its own."""
    try:
        ended = _control.call_waiting("SELECT ursula_cancel(?)", request)
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


def _connect(plan: Plan, binding: dict[str, Any]) -> UrsulaConnection:
    """A connection to what ``ursula_open`` bound."""
    path = str(binding["path"])
    kwargs = dict(plan.connect_kwargs)
    kwargs.setdefault("check_same_thread", False)
    database = f"file:{urllib.parse.quote(path)}?mode=ro" if plan.read_only else path
    try:
        conn = sqlite3.connect(
            database, timeout=plan.busy, factory=plan.factory, uri=plan.read_only, **kwargs
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
    # On a connection of its own: the import blocks for as long as it takes.
    done = _control.call_waiting(
        "SELECT ursula_import(?, ?, ?, ?)",
        stream,
        source,
        json.dumps(settings),
        max(1, round(limit * 1000)),
    )
    return Imported(
        offset=int(done["offset"]),
        digest=str(done["digest"]),
        pages=int(done["pages"]),
        bytes=int(done["bytes"]),
    )
