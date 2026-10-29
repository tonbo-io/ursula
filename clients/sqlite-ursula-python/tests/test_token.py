"""``open()``'s and ``aopen()``'s token provider against the e2e server: the package calls it on
its own thread before the open's first request, again ahead of each token's expiry and at once
after a 401, shares one call between concurrent opens, whatever providers they name, passes the
token it holds as ``previous`` and counts that token handed out again as a failed call, and stops
once no binding is bound or a string token replaced it. Its failures are typed: an error whose
action is ``fix`` is permanent until the next open, any other is retried with backoff, and a call
that does not return is abandoned.

The 401s are scripted faults (the model's, or the gate's in front of Ursula), since neither e2e
server checks tokens, or, in front of Ursula, come from the gate standing in for an authorizer that
refuses each token from its expiry on (``Server.require``). In front of Ursula the gate records
each request's ``Authorization`` header (``Server.sent``), so there the tests check which token
each request of their streams carried. The token is one per process, so each test ends with
``set_token(None)``, which stops the provider, and the checks that need no other binding in the
process (the provider stops once none is bound) run in a process of their own.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest
from conftest import env
from conftest import new_stream

import sqlite_ursula
from sqlite_ursula import BearerToken
from sqlite_ursula import UrsulaConnection
from sqlite_ursula import UrsulaSetupError
from sqlite_ursula import UrsulaUnavailableError
from sqlite_ursula import _control
from sqlite_ursula import _token


class Provider:
    """A provider of tokens ``<label>-<n>`` that last ``lifetime`` seconds, which records each
    call. ``before(n, previous)`` runs first in call ``n`` (from 1): it may wait, raise to fail the
    call, or return the answer itself."""

    def __init__(
        self,
        label: str,
        lifetime: float,
        before: Callable[[int, str | None], BearerToken | None] | None = None,
    ) -> None:
        self.label = label
        self.lifetime = lifetime
        self.before = before
        #: When each call started, in ``time.monotonic()`` seconds.
        self.calls: list[float] = []
        self.previous: list[str | None] = []
        self.timeouts: list[float] = []
        self.threads: list[str] = []

    def __call__(self, *, previous: str | None, timeout: float) -> BearerToken:
        self.calls.append(time.monotonic())
        self.previous.append(previous)
        self.timeouts.append(timeout)
        self.threads.append(threading.current_thread().name)
        n = len(self.calls)
        if self.before is not None:
            answer = self.before(n, previous)
            if answer is not None:
                return answer
        return BearerToken(f"{self.label}-{n}", time.time() + self.lifetime)


class KeyRevoked(Exception):
    """A provider's own error that says it cannot succeed: its action is ``fix``. Its constructor
    takes a keyword only, as many libraries' errors do, so a copy of it cannot be made from its
    ``args``."""

    action = "fix"

    def __init__(self, *, key_id: str) -> None:
        super().__init__(f"the key {key_id} was revoked")
        self.key_id = key_id


class Revoked(UrsulaUnavailableError):
    """An :class:`UrsulaUnavailableError` of the provider's own, whose constructor takes a keyword
    only, so a copy of it cannot be made from its message, kind and action."""

    def __init__(self, *, key_id: str) -> None:
        super().__init__(f"unauthorized: the key {key_id} was revoked", "unauthorized", "fix")
        self.key_id = key_id


class Expired(UrsulaUnavailableError):
    """An :class:`UrsulaUnavailableError` of the provider's own whose first positional argument
    means something else, so a copy made from its message would read the message as a key."""

    def __init__(self, key_id: str, kind: str = "unauthorized", action: str = "fix") -> None:
        super().__init__(f"unauthorized: the key {key_id} expired", kind, action)
        self.key_id = key_id


def nth(label: str, authorization: str | None) -> float:
    """The number ``n`` of the token ``<label>-<n>`` an ``Authorization`` header carries, or NaN."""
    prefix = f"Bearer {label}-"
    if authorization is None or not authorization.startswith(prefix):
        return float("nan")
    return float(authorization[len(prefix) :])


def posts(sent: list[dict[str, str | None]]) -> list[str | None]:
    """The ``Authorization`` header of each append among ``sent``."""
    return [r["authorization"] for r in sent if r["method"] == "POST"]


def until(what: str, done: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not done():
        assert time.monotonic() < deadline, what
        time.sleep(0.02)


@pytest.fixture(autouse=True)
def no_provider() -> Iterator[None]:
    """Each test starts and ends with no provider: ``set_token(None)`` stops it."""
    sqlite_ursula.set_token(None)
    yield
    sqlite_ursula.set_token(None)


def test_is_called_on_its_own_thread_before_the_open_and_again_ahead_of_each_expiry(
    server, stream, fresh
):
    p = Provider("refresh", 3.0)
    db = sqlite_ursula.open(stream.url, dir=fresh(), token=p, isolation_level=None)
    assert len(p.calls) == 1
    assert p.previous == [None]
    assert p.timeouts == [30.0]
    assert p.threads == ["sqlite-ursula-token"]
    db.execute("CREATE TABLE t(x)")
    db.execute("INSERT INTO t VALUES (1)")
    db.synced(timeout=30)
    # After 70 to 80% of each 3 s token: 2.1 to 2.4 s after it arrived.
    until("the first refresh", lambda: len(p.calls) >= 2)
    until("the second refresh", lambda: len(p.calls) >= 3)
    first, second, third = p.calls[:3]
    assert second - first >= 2.0
    assert third - second >= 2.0
    # No token before the first call, then each scheduled refresh is handed the current token.
    assert p.previous[:3] == [None, "refresh-1", "refresh-2"]
    assert set(p.threads) == {"sqlite-ursula-token"}
    db.execute("INSERT INTO t VALUES (2)")
    db.synced(timeout=30)
    sent = server.sent(stream.path)
    if sent is not None:
        # The open's first request carries the first token, and the append after the refreshes a
        # later one: the third call started only once the second token was set.
        assert sent[0]["authorization"] == "Bearer refresh-1"
        assert all(nth("refresh", r["authorization"]) >= 1 for r in sent)
        assert nth("refresh", posts(sent)[-1]) >= 2
    db.close()


def test_is_called_once_for_concurrent_opens_and_a_later_open_reuses_its_token(server, fresh):
    root = fresh()

    def slow(n: int, previous: str | None) -> None:
        time.sleep(0.3)

    p = Provider("shared", 3600.0, slow)
    streams = [new_stream(server, f"token-shared-{n}") for n in range(4)]
    got: list[UrsulaConnection] = []
    failed: list[BaseException] = []

    def opener(url: str) -> None:
        try:
            got.append(sqlite_ursula.open(url, dir=root, token=p))
        except BaseException as e:
            failed.append(e)

    threads = [threading.Thread(target=opener, args=(s.url,)) for s in streams[:3]]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert failed == []
    assert len(got) == 3
    assert len(p.calls) == 1
    got.append(sqlite_ursula.open(streams[3].url, dir=root, token=p))
    assert len(p.calls) == 1
    for s in streams:
        sent = server.sent(s.path)
        if sent is not None:
            assert sent[0]["authorization"] == "Bearer shared-1"
    for db in got:
        db.close()


def test_hands_the_bindings_it_served_to_the_provider_of_a_later_open_which_shares_the_token_held(
    server, fresh
):
    root = fresh()
    first = Provider("first", 2.0)
    second = Provider("second", 2.0)
    s = new_stream(server, "token-first")
    a = sqlite_ursula.open(s.url, dir=root, token=first, isolation_level=None)
    b = sqlite_ursula.open(new_stream(server, "token-second").url, dir=root, token=second)
    # The token held served the second open: no call of the second provider yet.
    assert second.calls == []
    b.close()
    until("the second binding retired", lambda: b.status()["state"] == "retired")
    # `a` still needs a fresh token: the second provider keeps refreshing, the first never runs
    # again.
    until("a refresh for the first binding", lambda: len(second.calls) >= 3, 8.0)
    assert len(first.calls) == 1
    a.execute("CREATE TABLE t(x)")
    a.synced(timeout=30)
    sent = server.sent(s.path)
    if sent is not None:
        assert nth("second", posts(sent)[-1]) >= 3
    a.close()


def test_stops_once_a_string_token_replaced_it(server, fresh):
    root = fresh()
    # Its refresh is due 4.2 to 4.8 s after the first call.
    p = Provider("one", 6.0)
    a = sqlite_ursula.open(new_stream(server, "token-one").url, dir=root, token=p)
    assert len(p.calls) == 1
    # A string token stops the provider: no refresh past the token's due time, and the requests
    # carry the string.
    s = new_stream(server, "token-string")
    c = sqlite_ursula.open(s.url, dir=root, token="a-string", isolation_level=None)
    c.execute("CREATE TABLE t(x)")
    c.synced(timeout=30)
    time.sleep(5.0)
    assert len(p.calls) == 1
    sent = server.sent(s.path)
    if sent is not None:
        assert {r["authorization"] for r in sent} == {"Bearer a-string"}
    for db in (a, c):
        db.close()


def test_serves_the_token_it_holds_at_once_expired_too_while_its_refresh_fails(stream, fresh):
    root = fresh()

    def down(n: int, previous: str | None) -> None:
        if n > 1:
            raise RuntimeError("the exchange is down")

    p = Provider("held", 1.5, down)
    db = sqlite_ursula.open(stream.url, dir=root, token=p)
    until("a failed refresh", lambda: len(p.calls) >= 2, 5.0)
    # Past the token's expiry: a connection on the bound binding still needs no network.
    time.sleep(1.0)
    started = time.monotonic()
    again = sqlite_ursula.open(stream.url, dir=root, token=p, timeout=5)
    assert time.monotonic() - started < 1.0
    again.close()
    db.close()


def test_waits_for_the_refresh_in_flight_rather_than_serve_an_expired_token_while_healthy(
    server, fresh
):
    root = fresh()

    def slow(n: int, previous: str | None) -> None:
        # Every refresh takes 2 s, longer than what is left of the 1.5 s token when it starts.
        if n > 1:
            time.sleep(2.0)

    p = Provider("slow", 1.5, slow)
    a = sqlite_ursula.open(new_stream(server, "token-slow-a").url, dir=root, token=p)
    until("the refresh in flight", lambda: len(p.calls) >= 2, 5.0)
    time.sleep(0.6)
    # The token held expired and the refresh has not answered: the open waits for it.
    b = new_stream(server, "token-slow-b")
    db = sqlite_ursula.open(b.url, dir=root, token=p, timeout=5)
    assert time.monotonic() >= p.calls[1] + 2.0
    sent = server.sent(b.path)
    if sent is not None:
        assert len(sent) > 0
        assert sent[0]["authorization"] == "Bearer slow-2"
    db.close()
    a.close()


def test_keeps_refreshing_while_a_binding_opened_with_a_string_token_is_bound(server, fresh):
    root = fresh()
    p = Provider("any", 2.0)
    s = new_stream(server, "token-string-bound")
    a = sqlite_ursula.open(s.url, dir=root, token="a-string-token", isolation_level=None)
    b = sqlite_ursula.open(new_stream(server, "token-provider-bound").url, dir=root, token=p)
    b.close()
    until("the provider's binding retired", lambda: b.status()["state"] == "retired")
    # The provider's tokens are the process's, so `a` carries them now: it keeps them fresh.
    until("a refresh for the binding opened with a string", lambda: len(p.calls) >= 3, 8.0)
    a.execute("CREATE TABLE t(x)")
    a.synced(timeout=30)
    sent = server.sent(s.path)
    if sent is not None:
        assert nth("any", posts(sent)[-1]) >= 3
    a.close()


def test_fails_the_open_with_its_own_error_when_it_fails_for_good(stream, fresh):
    def fail_with(revoked: Exception) -> Provider:
        def fail(n: int, previous: str | None) -> None:
            raise revoked

        return Provider("bad", 60.0, fail)

    # Its own UrsulaError is the open's error.
    sqlite_ursula.set_token(None)
    own = UrsulaUnavailableError("unauthorized: the key was revoked", "unauthorized", "fix")
    bad = fail_with(own)
    with pytest.raises(UrsulaUnavailableError) as raised:
        sqlite_ursula.open(stream.url, dir=fresh(), token=bad)
    assert str(raised.value) == str(own)
    assert (raised.value.kind, raised.value.action) == ("unauthorized", "fix")
    # Permanent: no retry until the next open.
    time.sleep(2.0)
    assert len(bad.calls) == 1
    with pytest.raises(UrsulaUnavailableError):
        sqlite_ursula.open(stream.url, dir=fresh(), token=bad)
    assert len(bad.calls) == 2

    # A subclass defined outside the package is never copied, whatever its constructor takes:
    # the package's class it derives from carries its message, kind and action, with it as the
    # cause.
    for subclass in (Revoked(key_id="k0"), Expired("k3")):
        sqlite_ursula.set_token(None)
        bad = fail_with(subclass)
        with pytest.raises(UrsulaUnavailableError) as raised:
            sqlite_ursula.open(stream.url, dir=fresh(), token=bad)
        assert type(raised.value) is UrsulaUnavailableError
        assert str(raised.value) == str(subclass)
        assert (raised.value.kind, raised.value.action) == ("unauthorized", "fix")
        assert raised.value.__cause__ is subclass

    # Any other error whose action is fix is the cause of one: never copied, since its
    # constructor takes a keyword only.
    sqlite_ursula.set_token(None)
    revoked = KeyRevoked(key_id="k1")
    bad = fail_with(revoked)
    with pytest.raises(UrsulaUnavailableError) as raised:
        sqlite_ursula.open(stream.url, dir=fresh(), token=bad)
    assert (raised.value.kind, raised.value.action) == ("unauthorized", "fix")
    assert raised.value.__cause__ is revoked
    assert "the key k1 was revoked" in str(raised.value)
    time.sleep(2.0)
    assert len(bad.calls) == 1


def test_retries_any_other_failure_with_backoff(stream, fresh):
    def flaky(n: int, previous: str | None) -> None:
        if n == 1:
            raise RuntimeError("the exchange is down")
        if n == 2:
            # Any exception, one that is not an Exception too.
            raise SystemExit(1)

    p = Provider("flaky", 3600.0, flaky)
    db = sqlite_ursula.open(stream.url, dir=fresh(), token=p)
    assert len(p.calls) == 3
    first, second, third = p.calls
    # min(30 s, 1 s * 2^(n-1)), jittered down to half: 0.5 to 1 s, then 1 to 2 s.
    assert 0.5 <= second - first < 1.5
    assert 1.0 <= third - second < 2.5
    db.close()


def test_fails_the_open_at_its_timeout_with_unavailable_retry_when_it_handed_out_no_token(
    stream, fresh
):
    down = RuntimeError("the exchange is down")

    def fail(n: int, previous: str | None) -> None:
        raise down

    p = Provider("down", 60.0, fail)
    with pytest.raises(UrsulaUnavailableError) as raised:
        sqlite_ursula.open(stream.url, dir=fresh(), token=p, timeout=2.5)
    assert (raised.value.kind, raised.value.action) == ("unavailable", "retry")
    assert raised.value.__cause__ is down
    assert len(p.calls) > 1


def test_refuses_an_answer_it_cannot_use_with_invalid_argument(stream, fresh):
    later = time.time() + 60
    answers: list[object] = [
        BearerToken("two words", later),
        BearerToken("", later),
        BearerToken("café", later),
        BearerToken("token", "soon"),  # type: ignore[arg-type]
        BearerToken("token", float("nan")),
        # A naive datetime: which timezone is ambiguous.
        BearerToken("token", datetime.now() + timedelta(hours=1)),
        # Milliseconds since the epoch, and a lifetime rather than a time.
        BearerToken("token", later * 1000),
        BearerToken("token", 3600),
        {"token": "token", "expires_at": later},
    ]
    for answer in answers:
        sqlite_ursula.set_token(None)
        with pytest.raises(UrsulaSetupError) as raised:
            sqlite_ursula.open(
                stream.url,
                dir=fresh(),
                token=lambda answer=answer, **_: answer,
            )
        assert (raised.value.kind, raised.value.action) == ("invalid_argument", "fix"), answer
    with pytest.raises(UrsulaSetupError) as raised:
        sqlite_ursula.open(stream.url, dir=fresh(), token=42)  # type: ignore[arg-type]
    assert raised.value.kind == "invalid_argument"
    # A provider that does not take the keywords previous and timeout is refused at the open,
    # before any call, rather than failing each call.
    calls: list[str] = []

    def without_timeout(*, previous: str | None) -> BearerToken:
        calls.append("called")
        return BearerToken("token", later)

    # An async provider, which would hand out a coroutine rather than a token, is refused at the
    # open too: the provider runs on a thread of the package, never on an event loop.
    async def asynchronous(*, previous: str | None, timeout: float) -> BearerToken:
        calls.append("called")
        return BearerToken("token", later)

    class AsyncCallable:
        async def __call__(self, *, previous: str | None, timeout: float) -> BearerToken:
            calls.append("called")
            return BearerToken("token", later)

    for wrong in (
        lambda: BearerToken("token", later),
        without_timeout,
        asynchronous,
        AsyncCallable(),
    ):
        with pytest.raises(UrsulaSetupError) as raised:
            sqlite_ursula.open(stream.url, dir=fresh(), token=wrong)  # type: ignore[arg-type]
        assert (raised.value.kind, raised.value.action) == ("invalid_argument", "fix"), wrong
    assert calls == []
    # Its repr leaves the token out.
    assert "secret" not in repr(BearerToken("secret", later))


def test_is_called_again_at_once_when_a_request_is_refused_with_401(server, fresh):
    s = new_stream(server, "token-401")
    p = Provider("refused", 3600.0)
    db = sqlite_ursula.open(s.url, dir=fresh(), token=p, isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced(timeout=30)
    assert len(p.calls) == 1
    # The append and its resend with the token read again are both refused: replication blocks
    # `unauthorized`, long before the token's refresh is due.
    server.fault("status 401", 2, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    until("a call after the 401", lambda: len(p.calls) == 2)
    # The new token sends the blocked append again at once.
    db.synced(timeout=30)
    assert len(p.calls) == 2
    # The call after the 401 is handed the refused token.
    assert p.previous == [None, "refused-1"]
    sent = server.sent(s.path)
    if sent is not None:
        # The append refused twice went out under the first token, and its resend under the
        # second.
        assert posts(sent)[-1] == "Bearer refused-2"
        assert posts(sent)[-3:-1] == ["Bearer refused-1", "Bearer refused-1"]
    db.close()


def test_handing_out_the_previous_token_again_counts_as_a_failed_call(server, fresh):
    s = new_stream(server, "token-previous")

    def cached(n: int, previous: str | None) -> BearerToken | None:
        # A cache that still holds the refused token on the call after the 401.
        return BearerToken("cached-1", time.time() + 3600) if n == 2 else None

    p = Provider("cached", 3600.0, cached)
    db = sqlite_ursula.open(s.url, dir=fresh(), token=p, isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced(timeout=30)
    server.fault("status 401", 2, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    until("a call after the 401", lambda: len(p.calls) == 2)
    # The previous token again failed the call: the next one comes after backoff.
    until("a retry", lambda: len(p.calls) == 3)
    assert p.calls[2] - p.calls[1] >= 0.5
    db.synced(timeout=30)
    assert len(p.calls) == 3
    # The retry's previous is still the refused token, which the failed call did not replace.
    assert p.previous == [None, "cached-1", "cached-1"]
    sent = server.sent(s.path)
    if sent is not None:
        # The refused token never went out again: the blocked append's resend carried the next
        # one.
        assert posts(sent)[-1] == "Bearer cached-3"
        assert posts(sent)[-3:-1] == ["Bearer cached-1", "Bearer cached-1"]
    db.close()


def test_is_asked_again_at_the_expiry_of_a_token_its_cache_hands_back(server, fresh):
    # A cache that hands its token out until it expires, whatever `previous` says, unless the test
    # evicted it. The first token lasts an hour, so the setup takes any time it needs. The one a
    # 401 makes the cache mint after the eviction lasts 1.2 s: its refresh is due 1 s after it
    # arrived, gets it back, and the backoff alone would retry 0.5 to 1 s later, at least 0.3 s
    # past the expiry. In front of Ursula the gate, standing in for an authorizer, refuses it from
    # its expiry on and takes the next one, armed before the call hands it out. It still takes the
    # first, which a request in flight may carry.
    minted: list[BearerToken] = []
    #: When each token was minted, in ``time.time()`` seconds.
    minted_at: list[float] = []
    asked: list[str | None] = []
    evicted = threading.Event()

    def cached(*, previous: str | None, timeout: float) -> BearerToken:
        asked.append(previous)
        if minted and float(minted[-1].expires_at) > time.time() and not evicted.is_set():
            return minted[-1]
        evicted.clear()
        lifetime = 1.2 if len(minted) == 1 else 3600.0
        token = BearerToken(f"expiring-{len(minted) + 1}", time.time() + lifetime)
        if len(minted) == 1 and server.gate is not None:
            server.require(
                {"expiring-1": None, "expiring-2": float(token.expires_at), "expiring-3": None}
            )
        minted_at.append(time.time())
        minted.append(token)
        return token

    s = new_stream(server, "token-expiring")
    try:
        db = sqlite_ursula.open(s.url, dir=fresh(), token=cached, isolation_level=None)
        db.execute("CREATE TABLE t(x)")
        db.synced(timeout=30)
        # A commit refused twice blocks replication: the call it makes mints the 1.2 s token, and
        # the blocked commit goes out again under it.
        evicted.set()
        server.fault("status 401", 2, "POST")
        db.execute("INSERT INTO t VALUES (1)")
        db.synced(timeout=30)
        assert [m.token for m in minted] == ["expiring-1", "expiring-2"]
        expiry = float(minted[1].expires_at)
        # The cache mints the next token only from the expiry on: the retry due at the expiry
        # takes it, rather than one after the backoff alone, 0.3 s or more past it.
        until("the call at the expiry", lambda: len(minted) == 3)
        assert [m.token for m in minted] == ["expiring-1", "expiring-2", "expiring-3"]
        assert expiry <= minted_at[2] < expiry + 0.3
        # Replication goes on under the next token.
        db.execute("INSERT INTO t VALUES (2)")
        db.synced(timeout=30)
        assert db.status()["replication"]["condition"] is None
        # The call after the 401 is handed the refused token, and each call after it the 1.2 s
        # one, which the cache hands back until it expires.
        assert asked[:2] == [None, "expiring-1"]
        assert set(asked[2:]) == {"expiring-2"}
        sent = server.sent(s.path)
        if sent is not None:
            assert posts(sent)[-1] == "Bearer expiring-3"
        db.close()
    finally:
        if server.gate is not None:
            server.require(None)


def test_starts_its_backoff_again_at_the_expiry_of_the_token_it_hands_back(server, fresh):
    # A cache whose clock runs 300 ms behind the package's: it hands its token out until 300 ms
    # past its expiry, whatever `previous` says.
    minted: list[tuple[BearerToken, float]] = []

    def cached(*, previous: str | None, timeout: float) -> BearerToken:
        if minted and float(minted[-1][0].expires_at) + 0.3 > time.time():
            return minted[-1][0]
        token = BearerToken(f"behind-{len(minted) + 1}", time.time() + 5.0)
        minted.append((token, time.time()))
        return token

    s = new_stream(server, "token-behind")
    db = sqlite_ursula.open(s.url, dir=fresh(), token=cached, isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced(timeout=30)
    # Replication blocks `unauthorized`, and each call gets the refused token back: retried after
    # 0.5 to 1 s, then after 1 to 2 s, and so on, and at the expiry, which gets it back once more.
    server.fault("status 401", 2, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    until("the next token", lambda: len(minted) == 2, 15.0)
    # The failures before the expiry no longer count: the call after it came 0.5 to 1 s later, not
    # 2 s or more.
    first = float(minted[0][0].expires_at)
    second = minted[1][1]
    assert 0.3 <= second - first < 1.5
    db.synced(timeout=30)
    assert db.status()["replication"]["condition"] is None
    db.close()


def _synced_in_background(db: UrsulaConnection) -> None:
    """``db.synced()`` on a thread of its own, its outcome ignored: an idle owner's read."""

    def sync() -> None:
        with contextlib.suppress(Exception):
            db.synced(timeout=5)

    threading.Thread(target=sync, daemon=True).start()


def test_takes_back_a_dropped_token_once_the_database_that_reported_it_closed(
    server, stream, fresh
):
    # In a process of its own, where the provider stops once the database closed.
    script = textwrap.dedent(
        """
        import json, sys, threading, time, urllib.request
        import sqlite_ursula
        from sqlite_ursula import BearerToken, _token

        url, root, faults = sys.argv[1], sys.argv[2], sys.argv[3]
        asked = []
        # A cache that hands out one token for an hour, whatever `previous` says.
        kept = BearerToken("kept-1", time.time() + 3600)

        def cached(*, previous, timeout):
            asked.append(previous)
            return kept

        db = sqlite_ursula.open(url, dir=root, token=cached, isolation_level=None)
        db.execute("CREATE TABLE t(x)")
        db.synced(timeout=30)
        # The read of an idle owner's synced() and its resend are refused: replication blocks
        # `unauthorized` with every commit acknowledged, so a close retires the binding at once.
        fault = urllib.request.Request(
            faults + "?times=2&method=GET", data=b"status 401", method="POST"
        )
        urllib.request.urlopen(fault, timeout=10).close()

        def sync():
            try:
                db.synced(timeout=5)
            except Exception:
                pass

        threading.Thread(target=sync, daemon=True).start()
        # The call after the 401 gets the refused token back: a failed call.
        deadline = time.monotonic() + 20
        while len(asked) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        condition = db.status()["replication"]["condition"]
        # The database that reported the 401 retired, and the provider stopped with it: the token
        # is not refused any more, so the next open takes it from the cache at once.
        db.close()
        while db.status()["state"] != "retired" and time.monotonic() < deadline:
            time.sleep(0.02)
        while _token.source.serving() and time.monotonic() < deadline:
            time.sleep(0.02)
        started = time.monotonic()
        following = sqlite_ursula.open(url + "-next", dir=root, token=cached, timeout=5)
        took = time.monotonic() - started
        following.close()
        print(json.dumps({"asked": asked, "condition": condition, "took": took}), flush=True)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", script, stream.url, fresh(), server.faults],
        env=env(),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert out.returncode == 0, out.stderr
    result = json.loads(out.stdout)
    assert result["asked"][:2] == [None, "kept-1"]
    assert result["condition"] == "unauthorized"
    # Well within the open's timeout, which a token still refused would run into.
    assert result["took"] < 3.0
    assert result["asked"][-1] is None


def test_takes_back_a_dropped_token_once_its_database_closed_while_another_stays_bound(
    server, fresh
):
    root = fresh()
    #: Each call's ``previous``, and when it started in ``time.monotonic()`` seconds.
    calls: list[tuple[str | None, float]] = []
    kept = BearerToken("kept-2", time.time() + 3600)

    def cached(*, previous: str | None, timeout: float) -> BearerToken:
        calls.append((previous, time.monotonic()))
        return kept

    a = sqlite_ursula.open(
        new_stream(server, "token-kept-a").url, dir=root, token=cached, isolation_level=None
    )
    b = sqlite_ursula.open(
        new_stream(server, "token-kept-b").url, dir=root, token=cached, isolation_level=None
    )
    b.execute("CREATE TABLE t(x)")
    b.synced(timeout=30)
    server.fault("status 401", 2, "GET")
    _synced_in_background(b)
    # Each call after the 401 gets the refused token back: retried after 0.5 to 1 s, 1 to 2 s and
    # 2 to 4 s, so the retry after the fourth is due 4 to 8 s later.
    until("four failed calls", lambda: len(calls) >= 5, 15.0)
    assert all(previous == "kept-2" for previous, _ in calls[1:])
    assert b.status()["replication"]["condition"] == "unauthorized"
    b.close()
    until("the binding retired", lambda: b.status()["state"] == "retired")
    retired_at = time.monotonic()
    # `a` keeps the provider running. The retirement runs the retry at once rather than after its
    # backoff: it takes the token back, and an open waiting for it goes on under it.
    c = sqlite_ursula.open(
        new_stream(server, "token-kept-c").url, dir=root, token=cached, timeout=5
    )
    assert len(calls) >= 6
    taken, at = calls[5]
    assert taken == "kept-2"
    assert at - retired_at < 1.0
    a.execute("CREATE TABLE t(x)")
    a.synced(timeout=30)
    assert a.status()["replication"]["condition"] is None
    a.close()
    c.close()


def test_counts_a_binding_it_cannot_find_as_retired_and_a_condition_with_no_path_as_clear():
    # A path the process never bound reads as `not_bound`, the status of a binding that retired:
    # no reporter there blocks a dropped token any more. Neither does a condition that carries
    # no path, which a binding the process no longer has reported.
    with _control.lock():
        assert _token._standing("/nowhere/db") == "retired"
        assert _token._cleared(("/nowhere/db",))
        assert _token._cleared(())


def test_takes_the_token_a_condition_dropped_again_once_its_binding_is_unblocked(server, fresh):
    s = new_stream(server, "token-dropped")
    other = new_stream(server, "token-dropped-other")
    root = fresh()

    def same(n: int, previous: str | None) -> BearerToken:
        # A cache that holds one token, which the server refused only for the scripted 401s.
        return BearerToken("same-1", time.time() + 3600)

    p = Provider("same", 3600.0, same)
    db = sqlite_ursula.open(s.url, dir=root, token=p, isolation_level=None)
    db.execute("CREATE TABLE t(x)")
    db.synced(timeout=30)
    assert len(p.calls) == 1
    # The append and its resend are refused: replication blocks `unauthorized`, which drops the
    # token held. While the binding is blocked, the same token back counts as a failed call.
    server.fault("status 401", 2, "POST")
    db.execute("INSERT INTO t VALUES (1)")
    until("a call after the condition", lambda: len(p.calls) >= 2)
    # The extension sends the blocked append again on its own 30 s retry, under the same token,
    # which goes through: the condition clears.
    until(
        "the condition cleared",
        lambda: db.status()["replication"]["condition"] is None,
        60.0,
    )
    db.synced(timeout=30)
    # The next retry hands back the token the condition dropped, and with the binding no longer
    # blocked it is taken: an open waiting for a token gets it, and the calls stop.
    following = sqlite_ursula.open(other.url, dir=root, token=p, timeout=45)
    calls = len(p.calls)
    time.sleep(3.0)
    assert len(p.calls) == calls
    assert p.previous[0] is None
    assert set(p.previous[1:]) == {"same-1"}
    for path in (s.path, other.path):
        sent = server.sent(path)
        if sent is not None:
            assert {r["authorization"] for r in sent} == {"Bearer same-1"}
    following.close()
    db.close()


def test_an_open_refused_with_401_drops_its_token_and_the_next_open_uses_the_next(server, fresh):
    p = Provider("bind", 3600.0)
    refused = new_stream(server, "token-bind-401")
    # The open's HEAD and its resend with the token read again are both refused: the bind blocks
    # `unauthorized`, and the extension reports no condition for a binding not bound.
    server.fault("status 401", 2, "HEAD")
    with pytest.raises(UrsulaUnavailableError) as raised:
        sqlite_ursula.open(refused.url, dir=fresh(), token=p, timeout=3)
    assert (raised.value.kind, raised.value.action) == ("unauthorized", "fix")
    # The refused open dropped the token and asked for the next one, which the next open uses.
    until("a call after the refused open", lambda: len(p.calls) == 2)
    # The first call held no token, and the call after the 401 is handed the refused one.
    assert p.previous == [None, "bind-1"]
    following = new_stream(server, "token-bind-next")
    db = sqlite_ursula.open(following.url, dir=fresh(), token=p)
    assert len(p.calls) == 2
    before, after = server.sent(refused.path), server.sent(following.path)
    if before is not None and after is not None:
        assert len(before) >= 2
        assert all(r["authorization"] == "Bearer bind-1" for r in before)
        assert len(after) > 0
        assert all(r["authorization"] == "Bearer bind-2" for r in after)
    db.close()


def test_abandons_a_call_that_does_not_return_once_the_provider_is_replaced(stream, fresh):
    release = threading.Event()

    def hang(n: int, previous: str | None) -> None:
        # A provider that ignores its timeout, as one blocked in a DNS lookup does.
        release.wait(60)

    stuck = Provider("stuck", 3600.0, hang)
    try:
        with pytest.raises(UrsulaUnavailableError) as raised:
            sqlite_ursula.open(stream.url, dir=fresh(), token=stuck, timeout=1)
        assert (raised.value.kind, raised.value.action) == ("unavailable", "retry")
        # A string token stops the provider, and the next one is called at once, while the
        # stuck call has not returned yet.
        sqlite_ursula.set_token(None)
        nxt = Provider("next", 3600.0)
        started = time.monotonic()
        db = sqlite_ursula.open(stream.url, dir=fresh(), token=nxt)
        assert time.monotonic() - started < 5
        assert len(nxt.calls) == 1
        assert len(stuck.calls) == 1
        db.close()
    finally:
        release.set()


def test_aopen_runs_the_provider_on_the_package_thread_and_waits_on_the_loop(stream, fresh):
    def slow(n: int, previous: str | None) -> BearerToken:
        time.sleep(0.5)
        # A timezone-aware datetime serves as expires_at too.
        return BearerToken(f"async-{n}", datetime.now(UTC) + timedelta(hours=1))

    p = Provider("async", 3600.0, slow)

    async def main() -> None:
        loop_thread = threading.current_thread().name
        ticks = 0

        async def ticker() -> None:
            nonlocal ticks
            while True:
                await asyncio.sleep(0.05)
                ticks += 1

        ticking = asyncio.create_task(ticker())
        root = fresh()
        db = await sqlite_ursula.aopen(stream.url, dir=root, token=p, isolation_level=None)
        # The loop ran other tasks while the provider slept on its own thread.
        assert ticks >= 5
        ticking.cancel()
        assert p.threads == ["sqlite-ursula-token"]
        assert loop_thread != "sqlite-ursula-token"
        db.execute("CREATE TABLE t(x)")
        await db.asynced(timeout=30)
        again = await sqlite_ursula.aopen(stream.url, dir=root, token=p)
        assert len(p.calls) == 1
        again.close()
        db.close()

    asyncio.run(main())


def test_aopen_fails_with_the_providers_permanent_error(stream, fresh):
    revoked = KeyRevoked(key_id="k2")

    def fail(n: int, previous: str | None) -> None:
        raise revoked

    p = Provider("async-bad", 60.0, fail)

    async def main() -> None:
        with pytest.raises(UrsulaUnavailableError) as raised:
            await sqlite_ursula.aopen(stream.url, dir=fresh(), token=p)
        assert (raised.value.kind, raised.value.action) == ("unauthorized", "fix")
        assert raised.value.__cause__ is revoked

    asyncio.run(main())
    assert len(p.calls) == 1


def test_stops_once_no_binding_uses_it_and_the_next_open_calls_it_again(stream, fresh):
    # In a process of its own, where no other binding is bound.
    script = textwrap.dedent(
        """
        import json, sys, time
        import sqlite_ursula
        from sqlite_ursula import BearerToken

        url, root = sys.argv[1], sys.argv[2]
        calls = []

        def p(*, previous, timeout):
            calls.append(time.monotonic())
            return BearerToken(f"p-{len(calls)}", time.time() + 2)

        db = sqlite_ursula.open(url, dir=root, token=p, isolation_level=None)
        db.execute("CREATE TABLE t(x)")
        db.synced(timeout=30)
        deadline = time.monotonic() + 20
        while len(calls) < 2 and time.monotonic() < deadline:
            time.sleep(0.02)
        refreshed = len(calls)
        db.close()
        while db.status()["state"] != "retired" and time.monotonic() < deadline:
            time.sleep(0.02)
        time.sleep(0.5)
        stopped = len(calls)
        time.sleep(3)
        after = len(calls)
        # The source stopped and forgot its token: the next open calls the provider again.
        other = sqlite_ursula.open(url + "-next", dir=root, token=p)
        other.close()
        print(json.dumps({"refreshed": refreshed, "stopped": stopped, "after": after,
                          "reopened": len(calls)}), flush=True)
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", script, stream.url, fresh()],
        env=env(),
        capture_output=True,
        text=True,
        timeout=90,
    )
    assert out.returncode == 0, out.stderr
    result = json.loads(out.stdout)
    assert result["refreshed"] >= 2
    assert result["after"] == result["stopped"]
    assert result["reopened"] == result["after"] + 1
