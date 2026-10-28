"""A process of a test (``conftest.Child``): opens the stream ``argv[1]`` in the cache root
``argv[2]``, prints ``{"ok": true}``, then runs one JSON command per line of stdin and answers each
with one line: ``{"exec": sql}`` runs a script, ``{"synced": seconds}`` waits for ``synced()``,
``{"query": sql}`` returns the rows, ``{"exit": true}`` closes the database and exits.
"""

import json
import sys

import sqlite_ursula


def say(reply: dict) -> None:
    sys.stdout.write(json.dumps(reply) + "\n")
    sys.stdout.flush()


def main() -> None:
    url, root = sys.argv[1], sys.argv[2]
    db = sqlite_ursula.open(url, dir=root, isolation_level=None)
    say({"ok": True})
    for line in sys.stdin:
        command = json.loads(line)
        try:
            if "exec" in command:
                db.executescript(command["exec"])
                say({"ok": True})
            elif "synced" in command:
                db.synced(timeout=command["synced"])
                say({"ok": True})
            elif "query" in command:
                say(
                    {"ok": True, "rows": [list(r) for r in db.execute(command["query"]).fetchall()]}
                )
            elif command.get("exit"):
                db.close()
                say({"ok": True})
                return
        except Exception as error:
            say({"ok": False, "error": repr(error), "kind": getattr(error, "kind", None)})


if __name__ == "__main__":
    main()
