"""``sqlite_ursula.tonbo`` against the e2e server: a name resolves through the extension's
``tonbo_resolve`` and opens through the root module's open, sync and async, the options reach the
APIs, the extension's failures become ``TonboError`` with their fix and request id, a timeout
reports the last condition and cancels the resolve, and the process's concurrent resolves with the
same arguments, from threads and tasks, share one, which a call that gives up leaves to the others
and the last cancels.

The Node suite's stand-in for the Tonbo APIs (clients/sqlite-ursula/test/tonbo-api.ts) serves the
lookups and the token exchanges, in a process of its own (tests/tonbo-api-process.mts), and its
databases are streams of the e2e server. It runs on ``E2E_NODE``, a Node 22.18 or newer: without
it the tests skip. The extension's own rules (the key's sources, the login file's checks, the
origins it refuses, backoff, retries, redirects) run in its unit tests and the Node suite, so these
assert kinds and fields, not its wording.

The process holds one API key for Streams tokens, so the tests of this process that fetch them
share ``KEY``, and the others give the Streams requests a token of their own. The test of a key
that gets no Streams token runs in a child process.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from conftest import HERE
from conftest import env
from conftest import new_stream

from sqlite_ursula import UrsulaConnection
from sqlite_ursula import UrsulaSetupError
from sqlite_ursula import UrsulaUnavailableError
from sqlite_ursula import tonbo
from sqlite_ursula.tonbo import ResolvedDatabase
from sqlite_ursula.tonbo import TonboError

#: The one API key whose Streams tokens the tests of this process fetch.
KEY = "tbo_process"
STREAMS_AUDIENCE = "https://streams.tonbo.dev"


class TonboApi:
    """The stand-in's process, driven over its stdin."""

    def __init__(self, proc: subprocess.Popen[str], origin: str) -> None:
        self.proc = proc
        self.origin = origin

    def send(self, **command: object) -> dict[str, Any]:
        assert self.proc.stdin is not None and self.proc.stdout is not None
        self.proc.stdin.write(json.dumps(command) + "\n")
        self.proc.stdin.flush()
        line = self.proc.stdout.readline()
        assert line, f"the stand-in exited ({self.proc.wait()})"
        return json.loads(line)

    def reset(self) -> None:
        self.send(reset=True)

    def seen(self) -> list[dict[str, Any]]:
        return self.send(seen=True)["seen"]

    def of(self, api: str) -> list[dict[str, Any]]:
        return [s for s in self.seen() if s["api"] == api]

    def lookups(self, name: str) -> int:
        return sum(1 for s in self.of("management") if s.get("name") == name)


@pytest.fixture(scope="module")
def tonbo_api() -> Iterator[TonboApi]:
    node = os.environ.get("E2E_NODE")
    if node is None:
        pytest.skip("the Tonbo API stand-in runs on Node 22.18 or newer: set E2E_NODE")
    # A CI leg that sets it and lost its Node fails here rather than skips.
    if not node:
        raise RuntimeError("E2E_NODE is set but empty")
    proc = subprocess.Popen(
        [node, "--no-warnings", str(HERE / "tonbo-api-process.mts")],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None and proc.stdin is not None
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"the Tonbo API stand-in exited ({proc.wait()}) before it listened")
    try:
        yield TonboApi(proc, json.loads(line)["origin"])
    finally:
        proc.stdin.close()
        proc.wait(timeout=10)


@pytest.fixture(autouse=True)
def tonbo_env(tonbo_api: TonboApi, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TonboApi:
    """Each test starts with the stand-in's origins, no key in the environment, a config home of
    its own (so no ``tonbo login``), and a stand-in that forgot what it saw."""
    for name in ("TONBO_API_KEY", "URSULA_VFS_TOKEN_FILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("TONBO_API_ORIGIN", tonbo_api.origin)
    monkeypatch.setenv("TONBO_AUTH_ORIGIN", tonbo_api.origin)
    tonbo_api.reset()
    return tonbo_api


@dataclass(frozen=True)
class Database:
    name: str
    url: str
    path: str


def database(api: TonboApi, server, label: str, state: str = "active", **extra: object) -> Database:
    """A database of the stand-in on a stream of its own."""
    s = new_stream(server, f"tonbo-{label}")
    name = s.path.rsplit("/", 1)[-1]
    api.send(database=name, url=s.url, state=state, **extra)
    return Database(name, s.url, s.path)


def stays_cancelled(api: TonboApi, name: str) -> None:
    """The resolve of ``name`` was cancelled: no lookup of it goes out any more, past the longest
    backoff of a database being created (2 s)."""
    time.sleep(0.5)
    before = api.lookups(name)
    time.sleep(2.5)
    assert api.lookups(name) == before
    assert tonbo._inflight == {}


def test_a_key_refused_a_streams_token_fails_invalid_api_key_with_its_fix_and_request_id(
    tonbo_api, server, fresh
):
    d = database(tonbo_api, server, "nostreams")
    # In a process of its own, which holds no key yet.
    script = textwrap.dedent(
        """
        import json, pickle, sys
        from sqlite_ursula import UrsulaError, UrsulaSetupError, tonbo

        name, root = sys.argv[1], sys.argv[2]
        out = []
        for call in (
            lambda: tonbo.open(name, dir=root, api_key="tbo_nostreams"),
            lambda: tonbo.resolve_database(name, api_key="tbo_nostreams"),
        ):
            try:
                call()
                out.append(None)
            except UrsulaError as e:
                copied = pickle.loads(pickle.dumps(e))
                out.append({
                    "type": type(e).__name__,
                    "setup": isinstance(e, UrsulaSetupError),
                    "fields": [e.kind, e.action, e.fix, e.request_id],
                    "message": str(e),
                    "copied": [type(copied).__name__, copied.kind, copied.fix, copied.request_id],
                })
        out.append(len(tonbo._inflight))
        print(json.dumps(out))
        """
    )
    run = subprocess.run(
        [sys.executable, "-c", script, d.name, fresh()],
        env=env(),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert run.returncode == 0, run.stderr
    *failures, inflight = json.loads(run.stdout)
    assert inflight == 0
    assert len(failures) == 2
    for failure in failures:
        assert failure is not None
        assert (failure["type"], failure["setup"]) == ("TonboError", False)
        kind, action, fix, request_id = failure["fields"]
        assert (kind, action) == ("invalid_api_key", "fix")
        assert fix and request_id
        assert failure["message"].endswith(fix)
        assert f"(request {request_id})" in failure["message"]
        # It crosses processes with its fields.
        assert failure["copied"] == ["TonboError", kind, fix, request_id]
    assert len(tonbo_api.of("management")) == 2
    assert [(s["authorization"], s["audience"]) for s in tonbo_api.of("token")] == [
        ("Bearer tbo_nostreams", STREAMS_AUDIENCE)
    ] * 2


def test_opens_by_name_with_the_key_whose_streams_tokens_the_extension_holds(
    tonbo_api, server, fresh
):
    d = database(tonbo_api, server, "name")
    root = fresh()
    db = tonbo.open(d.name, dir=root, api_key=KEY)
    assert isinstance(db, UrsulaConnection)
    assert db.name == d.path
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (1)")
    db.commit()
    db.synced(timeout=30)
    assert [(s["name"], s["authorization"]) for s in tonbo_api.of("management")] == [
        (d.name, f"Bearer {KEY}")
    ]
    # The key's first Streams token, unless an earlier test of the process fetched it.
    exchanged = tonbo_api.of("token")
    assert [(s["authorization"], s["audience"]) for s in exchanged] in (
        [],
        [(f"Bearer {KEY}", STREAMS_AUDIENCE)],
    )
    # A later open looks the name up again and reuses the Streams token.
    again = tonbo.open(d.name, dir=root, api_key=KEY)
    assert again.path == db.path
    assert again.execute("SELECT x FROM t").fetchall() == [(1,)]
    assert len(tonbo_api.of("management")) == 2
    assert tonbo_api.of("token") == exchanged
    # Resolving alone hands out the URL and the name, and no token: the key's serve.
    assert tonbo.resolve_database(d.name, api_key=KEY) == ResolvedDatabase(d.url, d.name, None)
    again.close()
    db.close()


def test_aopen_and_aresolve_database_wait_on_the_loop_and_a_cancelled_aopen_cancels_the_resolve(
    tonbo_api, server, fresh
):
    created = database(tonbo_api, server, "acreating", "creating", activeAfter=2)
    plain = database(tonbo_api, server, "aplain")
    stuck = database(tonbo_api, server, "astuck", "creating")

    async def main() -> int:
        ticks = 0
        done = asyncio.Event()

        async def ticker() -> None:
            nonlocal ticks
            while not done.is_set():
                ticks += 1
                await asyncio.sleep(0.01)

        task = asyncio.create_task(ticker())
        # Lookups after 0 s, 0.25 s and 0.75 s: the loop runs meanwhile.
        db = await tonbo.aopen(created.name, dir=fresh(), api_key=KEY)
        done.set()
        await task
        assert db.name == created.path
        db.execute("CREATE TABLE t(x)")
        db.commit()
        await db.asynced(timeout=30)
        db.close()
        resolved = await tonbo.aresolve_database(plain.name, api_key=KEY)
        assert resolved == ResolvedDatabase(plain.url, plain.name, None)
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(1):
                await tonbo.aopen(stuck.name, dir=fresh(), api_key=KEY)
        return ticks

    ticks = asyncio.run(main())
    assert ticks >= 20
    assert tonbo_api.lookups(created.name) == 3
    stays_cancelled(tonbo_api, stuck.name)


def test_a_timeout_reports_the_last_condition_and_cancels_the_resolve(tonbo_api, server, fresh):
    stuck = database(tonbo_api, server, "stuck", "creating")
    started = time.monotonic()
    with pytest.raises(UrsulaUnavailableError) as gave_up:
        tonbo.open(stuck.name, dir=fresh(), api_key=KEY, timeout=1.5)
    assert time.monotonic() - started < 5
    assert (gave_up.value.kind, gave_up.value.action) == ("unavailable", "retry")
    stays_cancelled(tonbo_api, stuck.name)


# Keys other than KEY look the name up only: the Streams requests carry a token of their own.
def test_the_api_key_option_reaches_the_api_and_a_token_of_its_own_serves_the_streams(
    tonbo_api, server, fresh
):
    d = database(tonbo_api, server, "option")
    token = "explicit-option"
    resolved = tonbo.resolve_database(d.name, api_key="tbo_option", token=token)
    assert resolved == ResolvedDatabase(d.url, d.name, token)
    assert token not in repr(resolved)
    db = tonbo.open(d.name, dir=fresh(), api_key="tbo_option", token=token)
    db.execute("CREATE TABLE t(x)")
    db.commit()
    db.synced(timeout=30)
    db.close()
    assert [s["authorization"] for s in tonbo_api.of("management")] == ["Bearer tbo_option"] * 2
    assert tonbo_api.of("token") == []


def test_fails_with_the_extensions_kind_fix_and_request_id(tonbo_api, server, fresh):
    d = database(tonbo_api, server, "missing")
    for target in (d.name, d.url):
        # A URL needs the key only for Streams tokens.
        with pytest.raises(TonboError) as missing:
            tonbo.open(target, dir=fresh())
        assert (missing.value.kind, missing.value.action) == ("missing_credentials", "fix")
        assert missing.value.fix
        assert str(missing.value).endswith(missing.value.fix)
        assert missing.value.request_id is None
    db = tonbo.open(d.url, dir=fresh(), token="explicit-url")
    db.execute("CREATE TABLE t(x)")
    db.commit()
    db.synced(timeout=30)
    db.close()
    assert tonbo_api.seen() == []

    with pytest.raises(TonboError) as not_found:
        tonbo.resolve_database("py-nope", api_key="tbo_answers")
    assert (not_found.value.kind, not_found.value.action) == ("database_not_found", "fix")
    assert not_found.value.fix
    assert str(not_found.value).endswith(not_found.value.fix)
    assert not_found.value.request_id
    assert f"(request {not_found.value.request_id})" in str(not_found.value)

    forbidden = database(tonbo_api, server, "forbidden", forbidden=True)
    with pytest.raises(TonboError) as refused:
        tonbo.open(forbidden.name, dir=fresh(), api_key="tbo_answers")
    assert (refused.value.kind, refused.value.action) == ("invalid_api_key", "fix")
    assert refused.value.fix
    assert refused.value.request_id
    assert "tbo_" not in str(refused.value)

    with pytest.raises(UrsulaSetupError) as empty:
        tonbo.resolve_database("", api_key="tbo_answers")
    assert (empty.value.kind, empty.value.action) == ("invalid_name", "fix")
    assert not isinstance(empty.value, TonboError)


def test_passes_the_origins_through_and_refuses_arguments_it_cannot_send(
    tonbo_api, server, fresh, monkeypatch
):
    d = database(tonbo_api, server, "origins")
    # The options name the origins over the environment's, for any key.
    monkeypatch.setenv("TONBO_API_ORIGIN", "http://127.0.0.1:1")
    monkeypatch.setenv("TONBO_AUTH_ORIGIN", "http://127.0.0.1:1")
    tonbo.open(
        d.name,
        dir=fresh(),
        api_key=KEY,
        api_origin=tonbo_api.origin,
        auth_origin=tonbo_api.origin,
    ).close()
    assert len(tonbo_api.of("management")) == 1

    tonbo_api.reset()
    for option in ("api_key", "api_origin", "auth_origin"):
        for value in (1, b"tbo_bytes", ["tbo_list"]):
            with pytest.raises(UrsulaSetupError) as bad:
                tonbo.open(d.name, dir=fresh(), **{option: value})
            assert bad.value.kind == "invalid_argument"
            assert option in str(bad.value)
            assert "tbo_" not in str(bad.value)
    for call in (
        lambda: tonbo.open(d.name, api_key=KEY, timeout=0),
        lambda: tonbo.open(d.name, api_key=KEY, token=5),
        lambda: tonbo.open(5, api_key=KEY),
        lambda: tonbo.open(d.name, api_key=KEY, autocommit=False),
        lambda: tonbo.resolve_database(d.name, api_key=KEY, timeout=-1),
    ):
        with pytest.raises(UrsulaSetupError) as refused:
            call()
        assert refused.value.kind == "invalid_argument"
    assert tonbo_api.seen() == []


def test_shares_one_resolve_among_the_threads_and_tasks_with_the_same_arguments(tonbo_api, server):
    d = database(tonbo_api, server, "shared", "creating", activeAfter=2)
    callers = 7
    barrier = threading.Barrier(callers)
    got: list[ResolvedDatabase] = []
    failed: list[BaseException] = []
    arguments: dict[str, Any] = {"api_key": "tbo_shared", "token": "explicit-shared"}

    def resolver() -> None:
        barrier.wait()
        try:
            got.append(tonbo.resolve_database(d.name, **arguments))
        except BaseException as e:
            failed.append(e)

    def task() -> None:
        async def main() -> ResolvedDatabase:
            barrier.wait()
            return await tonbo.aresolve_database(d.name, **arguments)

        try:
            got.append(asyncio.run(main()))
        except BaseException as e:
            failed.append(e)

    threads = [threading.Thread(target=resolver) for _ in range(callers - 1)]
    threads.append(threading.Thread(target=task))
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    assert failed == []
    assert got == [ResolvedDatabase(d.url, d.name, "explicit-shared")] * callers
    # One resolve: its lookups while the database was created, and the one that found it active.
    assert tonbo_api.lookups(d.name) == 3
    assert tonbo._inflight == {}


def test_a_call_that_gives_up_leaves_the_shared_resolve_to_the_others(tonbo_api, server):
    # Lookups after 0 s, 0.25 s, 0.75 s, 1.75 s and 3.75 s, the last of which finds it active.
    d = database(tonbo_api, server, "leave", "creating", activeAfter=4)
    arguments: dict[str, Any] = {"api_key": "tbo_leave", "token": "explicit-leave"}
    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def resolver(label: str, timeout: float) -> None:
        barrier.wait()
        try:
            outcomes[label] = tonbo.resolve_database(d.name, timeout=timeout, **arguments)
        except BaseException as e:
            outcomes[label] = e

    threads = [
        threading.Thread(target=resolver, args=("short", 1)),
        threading.Thread(target=resolver, args=("long", 8)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    short = outcomes["short"]
    assert isinstance(short, UrsulaUnavailableError)
    assert (short.kind, short.action) == ("cancelled", "retry")
    assert outcomes["long"] == ResolvedDatabase(d.url, d.name, "explicit-leave")
    assert tonbo_api.lookups(d.name) == 5
    assert tonbo._inflight == {}
    # A call after the outcome was taken resolves anew.
    assert tonbo.resolve_database(d.name, **arguments) == outcomes["long"]
    assert tonbo_api.lookups(d.name) == 6


def test_the_last_call_that_gives_up_cancels_the_shared_resolve(tonbo_api, server):
    stuck = database(tonbo_api, server, "abandon", "creating")
    arguments: dict[str, Any] = {"api_key": "tbo_abandon", "token": "explicit-abandon"}
    barrier = threading.Barrier(2)
    outcomes: dict[str, object] = {}

    def resolver() -> None:
        barrier.wait()
        try:
            outcomes["first"] = tonbo.resolve_database(stuck.name, timeout=1, **arguments)
        except BaseException as e:
            outcomes["first"] = e

    def task() -> None:
        async def main() -> ResolvedDatabase:
            barrier.wait()
            return await tonbo.aresolve_database(stuck.name, timeout=2, **arguments)

        try:
            outcomes["last"] = asyncio.run(main())
        except BaseException as e:
            outcomes["last"] = e

    threads = [threading.Thread(target=resolver), threading.Thread(target=task)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)
    first, last = outcomes["first"], outcomes["last"]
    assert isinstance(first, UrsulaUnavailableError)
    assert (first.kind, first.action) == ("cancelled", "retry")
    # The last reports the last condition the resolve met: the database is being created.
    assert isinstance(last, UrsulaUnavailableError)
    assert (last.kind, last.action) == ("unavailable", "retry")
    stays_cancelled(tonbo_api, stuck.name)
