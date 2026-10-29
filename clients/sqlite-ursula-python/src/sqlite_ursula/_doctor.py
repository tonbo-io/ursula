"""``python -m sqlite_ursula``: whether this Python can host the extension, and if not, what to use
instead. It loads the extension as ``open()`` would, and prints what it found. The exit code is 0
when this Python can host it and 1 when it cannot.
"""

from __future__ import annotations

import platform
import sqlite3
import sys
import sysconfig
from typing import TextIO

from . import __version__
from . import _host
from .errors import UrsulaSetupError


def main(out: TextIO = sys.stdout) -> int:
    free_threaded = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
    build = f"{platform.python_implementation()}{', free-threaded' if free_threaded else ''}"
    print(f"sqlite-ursula {__version__}", file=out)
    print(f"python: {platform.python_version()} ({build}) at {sys.executable}", file=out)
    print(f"sqlite: {sqlite3.sqlite_version}", file=out)
    print(f"extension loading: {'yes' if _host.loads_extensions() else 'no'}", file=out)
    try:
        lib = _host.library()
        print(f"extension: {lib.path} ({lib.source})", file=out)
        _host.connection(lib).close()
    except UrsulaSetupError as error:
        print(f"result: {error.kind}: this Python cannot host sqlite-ursula", file=out)
        print(f"why: {error}", file=out)
        if error.fix is not None:
            print(f"fix: {error.fix}", file=out)
        return 1
    print("result: ok: this Python can host sqlite-ursula", file=out)
    return 0
