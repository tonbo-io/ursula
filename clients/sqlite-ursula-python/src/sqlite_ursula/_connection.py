"""The connection ``open`` returns: a ``sqlite3.Connection`` subclass, so every library that takes
one (LangGraph's ``SqliteSaver`` among them) takes it, and ``isinstance`` holds.

``Connection.execute``, ``commit`` and ``__exit__`` call SQLite directly, not through
``cursor()``, so the subclass overrides them along with ``cursor``, and its cursor overrides
``execute*`` and ``fetch*``. Each raises what ``classify`` (``_control``) makes of its error: an
:class:`UrsulaError` for a failure of the extension, every other error unchanged. On top of
``sqlite3.Connection`` it has ``name``, ``path``, ``synced()``, ``status()`` and the reset
callbacks.

A refused write or a reset is one of SQLite's I/O errors, on which SQLite rolls back the whole
transaction. A transaction helper's ``ROLLBACK`` then fails with "cannot rollback - no transaction
is active", which would replace the error the caller needs. So the connection remembers an
:class:`UrsulaError` after which no transaction is open, until a statement succeeds. A ``ROLLBACK``
run through ``execute`` that finds no transaction then succeeds, since nothing is left to roll
back. Through ``executescript``, which may hold more statements after it, it raises the remembered
error instead, and so does a ``ROLLBACK TO`` or ``RELEASE`` that finds no savepoint. The legacy
``rollback()`` method runs no ROLLBACK when no transaction is open.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable
from typing import Any
from typing import TypeVar

from . import _control
from . import _events
from .errors import UrsulaError
from .errors import UrsulaResetError

log = logging.getLogger("sqlite_ursula")

#: SQLITE_ERROR: a SQL error, and every error of an Ursula SQL function.
SQLITE_ERROR = 1
#: SQLite's message for a ROLLBACK, and the start of its message for a ROLLBACK TO or RELEASE,
#: that finds nothing to undo.
NO_TRANSACTION = "cannot rollback - no transaction is active"
NO_SAVEPOINT = "no such savepoint: "

T = TypeVar("T")

ResetCallback = Callable[[UrsulaResetError], object]


class UrsulaCursor(sqlite3.Cursor):
    """A ``sqlite3.Cursor`` whose statements raise what their errors classify as."""

    def _run(
        self, f: Callable[..., T], *args: Any, rolled_back: Callable[[], T] | None = None
    ) -> T:
        run = getattr(self.connection, "_run", None)
        if run is None:
            return f(*args)
        return run(f, *args, rolled_back=rolled_back)

    def execute(self, sql: str, parameters: Any = (), /) -> UrsulaCursor:
        return self._run(super().execute, sql, parameters, rolled_back=lambda: self)

    def executemany(self, sql: str, parameters: Any, /) -> UrsulaCursor:
        return self._run(super().executemany, sql, parameters, rolled_back=lambda: self)

    def executescript(self, script: str, /) -> UrsulaCursor:
        return self._run(super().executescript, script)

    def fetchone(self) -> Any:
        return self._run(super().fetchone)

    def fetchmany(self, size: int | None = None) -> list[Any]:
        if size is None:
            return self._run(super().fetchmany)
        return self._run(super().fetchmany, size)

    def fetchall(self) -> list[Any]:
        return self._run(super().fetchall)

    def __next__(self) -> Any:
        return self._run(super().__next__)


class UrsulaConnection(sqlite3.Connection):
    """A connection to a database a stream holds.

    Statements are local: commit, then call :meth:`synced` before anything outside the process
    acts on what you wrote or read. Keep it open while a framework uses :attr:`path`: the binding
    retires once its last connection closes.

    Made with ``sqlite3.connect(..., factory=UrsulaConnection)`` outside ``open()`` it binds
    nothing, classifies nothing, and works as a ``sqlite3.Connection``.
    """

    #: The stream's path, such as ``/acme/notes``, or ``""`` for a connection ``open()`` did not
    #: make.
    name: str
    #: The local database file the extension bound, as ``status()["path"]`` reports it: the path
    #: a framework opens with ``sqlite3.connect`` to use the same database.
    path: str

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        database = args[0] if args else kwargs.get("database", "")
        self.name = ""
        self.path = str(database)
        self._bound = False
        self._generation = _control.generation()
        self._closed = False
        self._listeners: list[ResetCallback] = []
        #: The reset this connection knows of, from an event, a statement or ``synced()``.
        self._reset: UrsulaResetError | None = None
        self._fired = False
        #: An UrsulaError after which no transaction was open, until a statement succeeds.
        self._rolled_back: UrsulaError | None = None

    def _bind(self, name: str, path: str) -> None:
        """``open()``: the connection is on the binding of stream ``name``, whose file is
        ``path``."""
        self.name = name
        self.path = path
        self._bound = True
        _events.track(self)

    # The statements.

    def cursor(self, factory: Any = None) -> Any:
        self._check_fork()
        return super().cursor(UrsulaCursor if factory is None else factory)

    def execute(self, sql: str, parameters: Any = (), /) -> Any:
        return self.cursor().execute(sql, parameters)

    def executemany(self, sql: str, parameters: Any, /) -> Any:
        return self.cursor().executemany(sql, parameters)

    def executescript(self, script: str, /) -> Any:
        return self.cursor().executescript(script)

    def commit(self) -> None:
        self._run(super().commit)

    def rollback(self) -> None:
        self._run(super().rollback)

    def __exit__(self, *exc: Any) -> Any:
        return self._run(super().__exit__, *exc)

    def close(self) -> None:
        """Closes the connection. Once a binding's last connection closes, replication drains in the
        background and the binding retires. Use ``open()`` to use the database again."""
        super().close()
        if not self._closed:
            self._closed = True
            if self._bound:
                _events.untrack(self)

    # What the extension adds.

    def synced(self, timeout: float | None = None) -> None:
        """Returns once the stream holds every commit this process logged before the call, and a
        round trip that started after the call found this process still the owner (design:
        Synced). Call it before anything outside the process acts on what you wrote or read.

        It covers every connection of the process, a framework's on :attr:`path` included. It
        raises ``TimeoutError`` after ``timeout`` seconds (``None``: no limit), while the commits
        keep replicating, and an :class:`UrsulaResetError` on a reset. Network failures never
        raise: it waits. Ctrl-C interrupts it. On a reader cache it returns at once.
        """
        self._check_fork()
        if self._reset is not None:
            raise self._reset
        try:
            _events.synced(self.path, timeout)
        except UrsulaResetError as error:
            if self._reset is None:
                self._reset = error
            raise

    def status(self) -> dict[str, Any]:
        """``ursula_status`` of the database (design: Status)."""
        self._check_fork()
        return _control.call("SELECT ursula_status(?)", self.path)

    def on_reset(self, callback: ResetCallback) -> ResetCallback:
        """Runs ``callback`` once when the database is reset, with the reset error, after its
        pending ``synced()`` calls raised it. It runs on a thread of the package's. Returns
        ``callback``, so it serves as a decorator."""
        self._listeners.append(callback)
        return callback

    def off_reset(self, callback: ResetCallback) -> None:
        """Removes a callback :meth:`on_reset` added."""
        if callback in self._listeners:
            self._listeners.remove(callback)

    # The events thread's.

    def _reset_by(self, error: UrsulaResetError) -> None:
        """The database was reset: its callbacks run once."""
        if self._reset is None:
            self._reset = error
        if self._fired or self._closed:
            return
        self._fired = True
        for callback in list(self._listeners):
            try:
                callback(self._reset)
            except Exception:
                log.exception("an on_reset callback of %s raised", self.name)

    def _current_reset(self) -> UrsulaResetError | None:
        """The reset ``ursula_status`` reports, if any."""
        try:
            reset = self.status().get("reset")
        except Exception:
            return None
        return None if reset is None else _events.reset_error(reset)

    # Classification.

    def _check_fork(self) -> None:
        if self._bound and self._generation != _control.generation():
            raise _control.forked_error()

    def _in_transaction(self) -> bool:
        try:
            return self.in_transaction
        except sqlite3.ProgrammingError:
            return True

    def _classify(self, error: BaseException) -> BaseException:
        if not self._bound:
            return error
        classified = _control.classify(error, self.path)
        if isinstance(classified, UrsulaResetError) and self._reset is None:
            self._reset = classified
        return classified

    def _run(
        self, f: Callable[..., T], *args: Any, rolled_back: Callable[[], T] | None = None
    ) -> T:
        """Runs ``f``, a method that runs a statement, and raises what its error classifies as.
        ``rolled_back`` is what it returns for a ROLLBACK that found no transaction after an
        UrsulaError, when it can report one."""
        self._check_fork()
        try:
            result = f(*args)
        except sqlite3.Error as error:
            remembered = self._rolled_back
            # SQLite tells these errors from others by their message alone: they are SQLITE_ERROR.
            if remembered is not None and getattr(error, "sqlite_errorcode", None) == SQLITE_ERROR:
                message = str(error)
                if message == NO_TRANSACTION and rolled_back is not None:
                    self._rolled_back = None
                    return rolled_back()
                if message == NO_TRANSACTION or message.startswith(NO_SAVEPOINT):
                    raise remembered from remembered.__cause__
            classified = self._classify(error)
            if isinstance(classified, UrsulaError) and not self._in_transaction():
                self._rolled_back = classified
            if classified is error:
                raise
            raise classified from error
        self._rolled_back = None
        return result
