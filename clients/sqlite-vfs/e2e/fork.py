"""The fork guard (docs/architecture/sqlite-vfs.md, Forks) in a real host: CPython's sqlite3 with
the extension loaded, against the model server.

A child forked before the process started the extension uses the extension as usual: it opens
after the fork. An import starts the process as an open does. A child forked after the start fails
every Ursula SQL function and every statement of the inherited bound database that reaches a file,
says `forked` once on stderr, opens none of the database's files, rolls back and closes the
inherited connections without touching what the parent holds (a write transaction open at the
fork, with frames in the WAL, included), and uses another SQLite database as usual. The parent goes
on committing and syncing, and its database reopens whole.

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


def functions(path, cache, base, source):
    """Every Ursula SQL function, with arguments it would run with."""
    return [
        ("ursula_open", "SELECT ursula_open(?, ?, '{}', 1000)", (base + "-other", cache)),
        ("ursula_synced", "SELECT ursula_synced(?, 1000)", (path,)),
        ("ursula_events", "SELECT ursula_events(0, 0)", ()),
        ("ursula_cancel", "SELECT ursula_cancel(1)", ()),
        ("ursula_status", "SELECT ursula_status(?)", (path,)),
        ("ursula_stats", "SELECT ursula_stats(?)", (path,)),
        ("ursula_set_token", "SELECT ursula_set_token(?, 'token')", (path,)),
        ("ursula_import", "SELECT ursula_import(?, ?, '{}', 1000)", (base + "-again", source)),
    ]


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

    # Before the process started the extension, a child opens and syncs a database of its own.
    def opens_after_the_fork():
        own = control()
        opened = call(own, "SELECT ursula_open(?, ?, '{}', 20000)", base + "-child", cache)
        db = sqlite3.connect(opened["path"], isolation_level=None)
        db.execute("CREATE TABLE t(n)")
        db.execute("INSERT INTO t VALUES (1)")
        synced = call(own, "SELECT ursula_synced(?, 20000)", opened["path"])["synced"]
        db.close()
        return synced

    expect("a child forked before the process started opens and syncs", in_child(opens_after_the_fork), True)

    # An import starts the process too: a child forked after it fails `forked`.
    source = os.path.join(cache, "source.db")
    plain_source = sqlite3.connect(source)
    plain_source.execute("CREATE TABLE s(n)")
    plain_source.execute("INSERT INTO s VALUES (5)")
    plain_source.commit()
    plain_source.close()
    expect("the import", call(c, "SELECT ursula_import(?, ?, '{}', 20000)", base + "-imported", source)["done"], True)
    code, message = in_child(lambda: failure(lambda: c.execute("SELECT ursula_open(?, ?, '{}', 1000)", (base + "-imported", cache)).fetchone()))
    expect("a child forked after an import fails forked", (code, message.startswith("ursula_open: forked: ")), (1, True))

    opened = call(c, "SELECT ursula_open(?, ?, '{}', 20000)", base, cache)
    path = opened["path"]
    db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    db.execute("CREATE TABLE t(n)")
    db.execute("INSERT INTO t VALUES (1)")
    expect("the parent syncs", call(c, "SELECT ursula_synced(?, 20000)", path)["synced"], True)
    plain = os.path.join(cache, "plain.db")

    # A write transaction open at the fork, whose frames a small page cache already spilled into the
    # WAL: SQLite listed them in the wal-index, which the child's rollback would clear in the
    # parent's if the child's copy were shared.
    w = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
    w.execute("PRAGMA cache_size = 4")
    wal = path + "-uwal"
    before = os.path.getsize(wal)
    w.execute("BEGIN IMMEDIATE")
    w.execute("CREATE TABLE big(i INTEGER PRIMARY KEY, b BLOB)")
    for i in range(200):
        w.execute("INSERT INTO big VALUES (?, randomblob(3000))", (i,))
    expect("the open write transaction spilled into the WAL", os.path.getsize(wal) > before, True)

    def forked_after_the_process_started():
        said = tempfile.TemporaryFile()
        os.dup2(said.fileno(), 2)
        seen = {}
        for name, sql, args in functions(path, cache, base, source):
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
        failure(lambda: w.execute("ROLLBACK"))
        w.close()
        db.close()
        said.seek(0)
        seen["said"] = said.read().decode().count("event=forked")
        return seen

    seen = in_child(forked_after_the_process_started)
    for name, _, _ in functions(path, cache, base, source):
        code, message = seen[name]
        expect(f"{name} fails forked in the child", (code, message.startswith(f"{name}: forked: ")), (1, True))
    expect("a read of the inherited connection fails with an I/O error", seen["read"][0], 10)
    expect("a write of the inherited connection fails with an I/O error", seen["write"][0], 10)
    expect("the child cannot open the bound database", seen["connect"][0], 14)
    expect("another database works in the child", seen["plain"], [[7]])
    expect("the child says forked once", seen["said"], 1)

    w.execute("INSERT INTO big VALUES (1000, 'last')")
    w.execute("COMMIT")
    w.close()
    expect("the parent's wal-index is whole after the child rolled back", db.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
    expect("the parent's open transaction commits whole", db.execute("SELECT count(*), sum(length(b)) FROM big").fetchone(), (201, 200 * 3000 + 4))

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
