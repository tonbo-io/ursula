"""SQLite replicated through an Ursula stream by the sqlite-ursula-vfs loadable extension
(clients/sqlite-vfs), for the stdlib ``sqlite3`` module. The Python peer of the npm package
``@tonbo/sqlite-ursula``.

A commit is local: the extension appends it to a log it owns and replicates the log to the stream
in the background. ``db.synced()`` is the output gate::

    import sqlite_ursula

    db = sqlite_ursula.open("https://streams.example.com/acme/notes")
    db.execute("CREATE TABLE IF NOT EXISTS notes(body TEXT)")
    db.execute("INSERT INTO notes VALUES (?)", ("hello",))
    db.commit()
    db.synced()  # this database's commits are in the stream, and this process still owns it

``aopen`` and ``db.asynced()`` are the same calls for asyncio, and ``gate`` / ``agate`` wrap a
block of work in the gate.

Module map:

- ``_host``: where the extension is, whether this Python can load it (``host_unsupported``), and
  loading it.
- ``_control``: the process's control connection, calls of the extension's SQL functions,
  ``classify``, which asks the extension (``ursula_classify``) what a failure is, and the fork
  state.
- ``_events``: the events thread, which waits for the extension's events, resolves ``synced()``
  and ``asynced()``, wakes the opens and the Tonbo resolves, and runs the reset callbacks, and the
  one wait loop of a call that waits for a request of the extension (``Step``, ``wait_for`` and
  ``await_for``).
- ``_connection``: :class:`UrsulaConnection`, the ``sqlite3.Connection`` subclass ``open``
  returns, and its cursor, whose statements classify their errors.
- ``_open``: :func:`open`, :func:`aopen`, :func:`import_sqlite` and :func:`set_token`.
- ``tonbo``: Tonbo databases opened by name with a Tonbo API key, through the extension's
  ``tonbo_resolve``: ``tonbo.open``, ``tonbo.aopen``, ``tonbo.resolve_database`` and
  ``tonbo.aresolve_database``, and ``TonboError``. Imported on its own
  (``from sqlite_ursula import tonbo``).
- ``_gate``: :func:`gate` and :func:`agate`, which hold a block's outcome until ``synced()``.
- ``_args``: the checks of their arguments, and of ``synced()``'s, as ``invalid_argument``.
- ``_doctor``: ``python -m sqlite_ursula``, which says whether this Python can host the extension.
- ``errors``: the four error classes.
"""

from __future__ import annotations

from ._connection import UrsulaConnection
from ._connection import UrsulaCursor
from ._gate import agate
from ._gate import gate
from ._open import Imported
from ._open import aopen
from ._open import import_sqlite
from ._open import open  # noqa: A004 - the package's entry point, as Node's `open`
from ._open import set_token
from .errors import Action
from .errors import UrsulaError
from .errors import UrsulaResetError
from .errors import UrsulaSetupError
from .errors import UrsulaUnavailableError
from .errors import UrsulaWriteRefusedError

__version__ = "0.11.2"

__all__ = [
    "Action",
    "Imported",
    "UrsulaConnection",
    "UrsulaCursor",
    "UrsulaError",
    "UrsulaResetError",
    "UrsulaSetupError",
    "UrsulaUnavailableError",
    "UrsulaWriteRefusedError",
    "__version__",
    "agate",
    "aopen",
    "gate",
    "import_sqlite",
    "open",
    "set_token",
]
