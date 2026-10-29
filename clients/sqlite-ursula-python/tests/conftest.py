"""The server the suite runs against, and what the tests share.

- ``E2E_SERVER=model`` (the default): the model of the server (``ursula-vfs-model``,
  clients/sqlite-vfs with the ``model`` feature) from ``URSULA_VFS_MODEL``, the one the Node suite
  runs against. Its ``/__model/faults`` scripts the delays the waiting tests need.
- ``E2E_SERVER=ursula``: one ``ursula server`` node from ``URSULA_BIN``, with its Raft WAL in a
  temporary directory and an in-memory cold store, behind clients/sqlite-ursula/test/gate.mjs (run
  with ``node``), the transparent proxy the Node suite puts in front of Ursula. The gate scripts
  ``delay <ms>`` and ``status <code>`` faults as the model does, records each request's
  ``Authorization`` header (:meth:`Server.sent`), and stands in for an authorizer
  (:meth:`Server.require`). The tests that read the model's frames or script its other faults
  skip there.

``SQLITE_URSULA_VFS`` names the extension the package loads.
"""

from __future__ import annotations

import itertools
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

HERE = Path(__file__).parent
SRC = HERE.parent / "src"
GATE = HERE.parent.parent / "sqlite-ursula" / "test" / "gate.mjs"
CLUSTER_ID = "sqlite-ursula-python-test"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def request(method: str, url: str, body: bytes | None = None, **headers: str) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()


@dataclass
class Server:
    url: str
    on_model: bool
    #: The control URL of the gate in front of Ursula, ``None`` on the model.
    gate: str | None = None

    @property
    def faults(self) -> str:
        """Where :meth:`fault` posts: the model's faults, or the gate's in front of Ursula."""
        return f"{self.url}/__model/faults" if self.gate is None else f"{self.gate}/faults"

    def fault(self, line: str, times: int = 1, method: str | None = None) -> None:
        """Scripts a fault for the next ``times`` requests of ``method``: on the model, or on the
        gate in front of Ursula, which takes ``delay <ms>`` and ``status <code>`` only."""
        query = f"times={times}" + ("" if method is None else f"&method={method}")
        status, body = request("POST", f"{self.faults}?{query}", line.encode())
        assert status == 204, body

    def require(self, tokens: dict[str, float | None] | None) -> None:
        """Has the gate in front of Ursula answer ``401`` to every request that does not carry one
        of ``tokens`` before its expiry (``time.time()`` seconds, ``None``: never), as an
        authorizer refuses a token from its expiry on. ``None``: none required."""
        assert self.gate is not None
        auth = (
            {"token": None}
            if tokens is None
            else {
                "tokens": [
                    {"token": token, "expiresAt": None if at is None else round(at * 1000)}
                    for token, at in tokens.items()
                ]
            }
        )
        status, body = request("PUT", f"{self.gate}/auth", json.dumps(auth).encode())
        assert status == 204, body

    def sent(self, path: str) -> list[dict[str, str | None]] | None:
        """The method and the ``Authorization`` header of each request the gate in front of Ursula
        received for the stream at ``path``, oldest first: ``None`` on the model, which records
        none."""
        if self.gate is None:
            return None
        status, body = request("GET", f"{self.gate}/requests?path={urllib.parse.quote(path)}")
        assert status == 200, body
        return json.loads(body)

    def frames(self, path: str) -> dict[str, int]:
        """The ``USQ2`` frames the stream at ``path`` holds, by kind (the model only)."""
        assert self.on_model
        status, data = request("GET", f"{self.url}/__model/frames?path={path}")
        assert status == 200
        count = {"claims": 0, "commits": 0}
        at = 0
        while at < len(data):
            assert data[at : at + 4] == b"USQ2"
            kind = data[at + 4]
            length = int.from_bytes(data[at + 5 : at + 9], "little")
            if kind == 1:
                count["claims"] += 1
            if kind == 2:
                count["commits"] += 1
            at += 13 + length
        return count


def _model() -> Iterator[Server]:
    binary = os.environ.get("URSULA_VFS_MODEL")
    if not binary:
        raise RuntimeError("set URSULA_VFS_MODEL to an ursula-vfs-model binary")
    proc = subprocess.Popen(
        [binary, "--exit-with-stdin"], stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"{binary} exited before it printed its port")
    port = json.loads(line)["port"]
    try:
        yield Server(f"http://127.0.0.1:{port}", on_model=True)
    finally:
        assert proc.stdin is not None
        proc.stdin.close()
        proc.wait(timeout=10)


def _ursula() -> Iterator[Server]:
    binary = os.environ.get("URSULA_BIN")
    if not binary:
        raise RuntimeError("set URSULA_BIN to an ursula binary")
    root = Path(tempfile.mkdtemp(prefix="sqlite-ursula-python-node-"))
    port, admin = free_port(), free_port()
    config = root / "ursula.toml"
    config.write_text(
        "\n".join(
            [
                "[server]",
                f'listen = "127.0.0.1:{port}"',
                f'admin_listen = "127.0.0.1:{admin}"',
                f'cluster_id = "{CLUSTER_ID}"',
                "",
                "[runtime]",
                "core_count = 2",
                "",
                "[raft]",
                "node_id = 1",
                "group_count = 4",
                "",
                "[raft.wal]",
                f'path = "{root / "wal"}"',
                "",
                "[[raft.peers]]",
                "node_id = 1",
                f'url = "http://127.0.0.1:{port}"',
                "",
                "[storage.cold]",
                'backend = "memory"',
                "",
            ]
        )
    )
    log_dir = Path(os.environ.get("E2E_LOG_DIR") or root)
    log_dir.mkdir(parents=True, exist_ok=True)
    with (log_dir / "ursula.log").open("wb") as log:
        proc = subprocess.Popen(
            [binary, "server", "--config", str(config)], cwd=root, stdout=log, stderr=log
        )
    url = f"http://127.0.0.1:{port}"
    try:
        _wait_writable(url, proc)
        yield from _gate(url)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _gate(target: str) -> Iterator[Server]:
    """clients/sqlite-ursula/test/gate.mjs in front of ``target``. It exits when its stdin
    closes."""
    node = shutil.which("node")
    if node is None:
        raise RuntimeError("E2E_SERVER=ursula runs Ursula behind test/gate.mjs: install node")
    proc = subprocess.Popen(
        [node, "--no-warnings", str(GATE), target], stdin=subprocess.PIPE, stdout=subprocess.PIPE
    )
    assert proc.stdout is not None
    line = proc.stdout.readline()
    if not line:
        raise RuntimeError(f"{GATE} exited before it printed its ports")
    ports = json.loads(line)
    try:
        yield Server(
            f"http://127.0.0.1:{ports['port']}",
            on_model=False,
            gate=f"http://127.0.0.1:{ports['control']}",
        )
    finally:
        assert proc.stdin is not None
        proc.stdin.close()
        proc.wait(timeout=10)


def _wait_writable(url: str, proc: subprocess.Popen[bytes]) -> None:
    """Until the bucket and a stream in every group can be created."""
    deadline = time.monotonic() + 120
    last = ""
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"ursula exited ({proc.returncode}) before it was writable")
        try:
            status, _ = request("PUT", f"{url}/sqlite-e2e")
            last = str(status)
            if status in (200, 201, 204, 409):
                ok = True
                for i in range(16):
                    s, _ = request(
                        "PUT",
                        f"{url}/sqlite-e2e/probe-{i}",
                        b"",
                        **{"content-type": "application/octet-stream"},
                    )
                    ok = ok and s in (200, 201, 204, 409)
                    last = f"stream {s}"
                if ok:
                    return
        except OSError as e:
            last = str(e)
        time.sleep(0.2)
    raise RuntimeError(f"ursula not writable: {last}")


@pytest.fixture(scope="session")
def server() -> Iterator[Server]:
    mode = os.environ.get("E2E_SERVER", "model")
    if mode == "model":
        yield from _model()
    elif mode == "ursula":
        yield from _ursula()
    else:
        raise RuntimeError(f"E2E_SERVER is model or ursula, not {mode}")


@pytest.fixture
def model(server: Server) -> Server:
    """The server, for a test that reads the model's frames or was written for its faults: the
    model only."""
    if not server.on_model:
        pytest.skip("runs on the model server only")
    return server


_n = itertools.count(1)


@dataclass(frozen=True)
class Stream:
    url: str
    path: str


@pytest.fixture
def stream(server: Server) -> Stream:
    """A stream of the test's own, in the bucket ``sqlite-e2e``."""
    return new_stream(server, "s")


def new_stream(server: Server, label: str) -> Stream:
    path = f"/sqlite-e2e/py-{label}-{os.getpid()}-{int(time.time() * 1000):x}-{next(_n)}"
    return Stream(server.url + path, path)


@pytest.fixture
def fresh(tmp_path_factory: pytest.TempPathFactory):
    """A new cache root for each call."""
    return lambda: str(tmp_path_factory.mktemp("cache"))


def env() -> dict[str, str]:
    """The environment of a Python child of a test: the package from this checkout."""
    path = os.environ.get("PYTHONPATH")
    return {**os.environ, "PYTHONPATH": str(SRC) if not path else f"{SRC}{os.pathsep}{path}"}


class Child:
    """Another process that opens ``url`` in the cache root ``root`` (tests/child.py), and runs what
    it is told."""

    def __init__(self, url: str, root: str) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, str(HERE / "child.py"), url, root],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            env=env(),
            text=True,
        )
        assert self.next()["ok"], "the child did not open"

    def next(self) -> dict:
        assert self.proc.stdout is not None
        line = self.proc.stdout.readline()
        assert line, f"the child exited ({self.proc.wait()})"
        return json.loads(line)

    def send(self, **command: object) -> dict:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(command) + "\n")
        self.proc.stdin.flush()
        reply = self.next()
        assert reply["ok"], reply
        return reply

    def stop(self) -> None:
        self.send(exit=True)
        self.proc.wait(timeout=30)
