"""The fork guard (docs/architecture/sqlite-vfs.md, Forks) in a real host: CPython's sqlite3 with
the extension loaded, against the model server.

A child forked before the extension started its driver uses the extension as usual: it opens after
the fork. A child forked after it fails every Ursula SQL function and every statement of the
inherited bound database, opens none of its files, closes the inherited connection without
touching what the parent holds, and uses another SQLite database as usual. The parent goes on
committing and syncing, and its database reopens whole.

Run with a CPython 3.11 or newer whose SQLite is 3.50.4 or newer and loads extensions (a uv-managed
one does):

    SQLITE_URSULA_VFS=<libsqlite_ursula_vfs> URSULA_VFS_MODEL=<ursula-vfs-model> python3 fork.py
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import warnings

EXT = os.environ["SQLITE_URSULA_VFS"]
MODEL = os.environ["URSULA_VFS_MODEL"]
# CPython warns on a fork of a process with threads: the extension's threads are the point here.
warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*fork.*")


def control():
    """A connection that loads the extension, for its SQL functions."""
    c = sqlite3.connect(":memory:", check_same_thread=False)
    c.enable_load_extension(True)
    c.load_extension(EXT)
    c.enable_load_extension(False)
    return c


def call(c, sql, *args):
    """The JSON an Ursula SQL function returned."""
    return json.loads(c.execute(sql, args).fetchone()[0])


def failure(run):
    """What `run()` failed with: SQLite's primary result code and message, or None if it returned."""
    try:
        run()
    except sqlite3.Error as e:
        return [e.sqlite_errorcode & 0xFF, str(e)]
    return None


def in_child(run):
    """Forks, runs `run()` in the child and returns what it returned, as JSON. A child that fails or
    hangs (killed after 30 s) fails the caller."""
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(r)
        signal.alarm(30)
        try:
            out = {"ok": True, "result": run()}
        except BaseException as e:
            out = {"ok": False, "error": repr(e)}
        os.write(w, json.dumps(out).encode())
        os._exit(0)
    os.close(w)
    with os.fdopen(r, "rb") as f:
        data = f.read()
    _, status = os.waitpid(pid, 0)
    if not os.WIFEXITED(status):
        raise AssertionError(f"the child died, wait status {status}")
    out = json.loads(data)
    if not out["ok"]:
        raise AssertionError(f"the child failed: {out['error']}")
    return out["result"]


def expect(what, got, want):
    if got != want:
        raise AssertionError(f"{what}: got {got!r}, want {want!r}")
    print(f"ok: {what}")


def main():
    print(f"CPython {sys.version.split()[0]}, SQLite {sqlite3.sqlite_version}")
    server = subprocess.Popen([MODEL, "--exit-with-stdin"], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    port = json.loads(server.stdout.readline())["port"]
    base = f"http://127.0.0.1:{port}/sqlite-e2e/fork-{os.getpid()}"
    cache = tempfile.mkdtemp(prefix="sqlite-ursula-fork-")
    c = control()

    # Before the driver started, a child opens and syncs a database of its own.
    def opens_after_the_fork():
        own = control()
        opened = call(own, "SELECT ursula_open(?, ?, '{}', 20000)", base + "-child", cache)
        db = sqlite3.connect(opened["path"], isolation_level=None)
        db.execute("CREATE TABLE t(n)")
        db.execute("INSERT INTO t VALUES (1)")
        synced = call(own, "SELECT ursula_synced(?, 20000)", opened["path"])["synced"]
        db.close()
        return synced

    expect("a child forked before the driver started opens and syncs", in_child(opens_after_the_fork), True)

    opened = call(c, "SELECT ursula_open(?, ?, '{}', 20000)", base, cache)
    path = opened["path"]
    db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    db.execute("CREATE TABLE t(n)")
    db.execute("INSERT INTO t VALUES (1)")
    expect("the parent syncs", call(c, "SELECT ursula_synced(?, 20000)", path)["synced"], True)
    plain = os.path.join(cache, "plain.db")

    def forked_after_the_driver_started():
        seen = {}
        for name, sql, args in [
            ("ursula_synced", "SELECT ursula_synced(?, 1000)", (path,)),
            ("ursula_status", "SELECT ursula_status(?)", (path,)),
            ("ursula_events", "SELECT ursula_events(0, 0)", ()),
            ("ursula_open", "SELECT ursula_open(?, ?, '{}', 1000)", (base + "-other", cache)),
        ]:
            seen[name] = failure(lambda: c.execute(sql, args).fetchone())
        seen["read"] = failure(lambda: db.execute("SELECT count(*) FROM t").fetchall())
        seen["write"] = failure(lambda: db.execute("INSERT INTO t VALUES (2)"))
        seen["connect"] = failure(lambda: sqlite3.connect(path).execute("SELECT count(*) FROM t").fetchall())
        other = sqlite3.connect(plain, isolation_level=None)
        other.execute("PRAGMA journal_mode=WAL")
        other.execute("CREATE TABLE p(n)")
        other.execute("INSERT INTO p VALUES (7)")
        seen["plain"] = other.execute("SELECT n FROM p").fetchall()
        other.close()
        db.close()
        return seen

    seen = in_child(forked_after_the_driver_started)
    for name in ["ursula_synced", "ursula_status", "ursula_events", "ursula_open"]:
        code, message = seen[name]
        expect(f"{name} fails forked in the child", (code, message.startswith(f"{name}: forked: ")), (1, True))
    expect("a read of the inherited connection fails with an I/O error", seen["read"][0], 10)
    expect("a write of the inherited connection fails with an I/O error", seen["write"][0], 10)
    expect("the child cannot open the bound database", seen["connect"][0], 14)
    expect("another database works in the child", seen["plain"], [[7]])

    db.execute("INSERT INTO t VALUES (3)")
    expect("the parent syncs after the child closed its copy", call(c, "SELECT ursula_synced(?, 20000)", path)["synced"], True)
    expect("the parent's rows", db.execute("SELECT n FROM t ORDER BY n").fetchall(), [(1,), (3,)])
    expect("no reset", call(c, "SELECT ursula_status(?)", path)["reset"], None)
    db.close()
    again = call(c, "SELECT ursula_open(?, ?, '{}', 20000)", base, cache)
    reopened = sqlite3.connect(again["path"], isolation_level=None)
    expect("the reopened rows", reopened.execute("SELECT n FROM t ORDER BY n").fetchall(), [(1,), (3,)])
    reopened.close()
    server.stdin.close()
    server.wait()


if __name__ == "__main__":
    main()
