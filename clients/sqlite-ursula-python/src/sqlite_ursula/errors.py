"""The errors the package raises (design: Errors).

Which failure is which error is the extension's to say (``ursula_classify``): its kind, its action
and its class. :func:`error_of` turns that answer into one of the four classes below. Only the
errors the package makes itself (an invalid argument, a host that cannot load the extension, a
forked process, a cancelled open) name their action at the place that raises them.

A refused write and a reset replace a ``sqlite3.OperationalError`` that a statement raised, so
their classes derive from it too: an ``except sqlite3.OperationalError`` clause, and the DB-API
wrapping of SQLAlchemy, keep working. The SQLite error is ``__cause__``, and its
``sqlite_errorcode`` and ``sqlite_errorname`` are copied.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Mapping
from typing import Any
from typing import Literal

__all__ = [
    "Action",
    "UrsulaError",
    "UrsulaResetError",
    "UrsulaSetupError",
    "UrsulaUnavailableError",
    "UrsulaWriteRefusedError",
]

Action = Literal["retry", "reopen", "stop", "fix"]
"""What to do about a failure. Branch on it.

- ``retry``: nothing was lost and the database stays usable. For ``backlog_full``, call
  ``synced()`` and run the transaction again. For a failed ``open()``, call it again later.
- ``reopen``: close every connection of the database and ``open()`` it again.
- ``stop``: another owner took the stream over. Stop using the database, and never reopen it in a
  loop: opening it takes the stream back.
- ``fix``: it cannot succeed as asked. Fix the URL, the host, the stream or the transaction.
"""


class UrsulaError(Exception):
    """A failure of the extension or of an open: the base class of every error the package raises.

    ``kind`` names what failed and ``action`` what to do (:data:`Action`). ``fix`` says how to fix
    a host the package cannot run on, and is ``None`` otherwise.
    """

    kind: str
    action: Action
    fix: str | None
    sqlite_errorcode: int | None
    sqlite_errorname: str | None

    def __init__(
        self,
        message: str,
        kind: str,
        action: Action,
        *,
        fix: str | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.action = action
        self.fix = fix
        self.sqlite_errorcode = getattr(cause, "sqlite_errorcode", None)
        self.sqlite_errorname = getattr(cause, "sqlite_errorname", None)
        if cause is not None:
            self.__cause__ = cause

    def __reduce__(self) -> tuple[Any, ...]:
        # Picklable across processes (multiprocessing), whatever the constructor's keywords.
        return (type(self), (str(self), self.kind, self.action), self.__dict__)


class UrsulaWriteRefusedError(UrsulaError, sqlite3.OperationalError):
    """A write or COMMIT refused with nothing committed (``SQLITE_IOERR_AUTH``, 7178).

    SQLite rolled the whole transaction back, and the database stays usable. ``backlog_full``
    (``retry``): more than ``max_pending_bytes`` of commits wait for the stream, so call
    ``synced()`` and run the transaction again. ``unsupported`` (``fix``): a write the extension
    does not capture, such as one in EXCLUSIVE locking mode. ``too_large`` (``fix``): the
    transaction's commit holds more bytes of pages than the commit limit.
    """


class UrsulaResetError(UrsulaError, sqlite3.OperationalError):
    """The database was reset (design: Resets).

    Every statement of it raises this, reads included, ``synced()`` raises it, and the
    ``on_reset`` callbacks get it. ``fenced`` (``stop``): another owner took the stream over. The
    other kinds say to ``reopen`` (a new ``open()`` of the URL) or to ``fix`` the stream first.
    """


class UrsulaUnavailableError(UrsulaError):
    """``open()`` found no trusted state in time.

    ``kind`` is the last condition the open met (``unreachable``, ``unauthorized``, ...), or
    ``cancelled`` when it met none yet.
    """


class UrsulaSetupError(UrsulaError):
    """It cannot succeed as asked: the message says what to change.

    ``host_unsupported`` says this Python cannot load the extension, and ``fix`` names a Python
    that can. ``forked`` says the process forked after its parent opened a database.
    """


def error_of(named: Mapping[str, Any], message: str, cause: BaseException | None) -> UrsulaError:
    """The error of a failure the extension named (``ursula_classify``'s answer, an event, a status
    field), with ``message`` and ``cause``: its class picks the error, and its action is the
    extension's."""
    kind = str(named["kind"])
    action: Action = named["action"]
    fix = named.get("fix")
    cls: type[UrsulaError] = {
        "write_refused": UrsulaWriteRefusedError,
        "reset": UrsulaResetError,
        "unavailable": UrsulaUnavailableError,
    }.get(str(named.get("class")), UrsulaSetupError)
    return cls(message, kind, action, fix=fix if isinstance(fix, str) else None, cause=cause)
