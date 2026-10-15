# Ursula Gateway

The `ursula gateway` role is a small HTTP/SSE gateway for Ursula clusters. It gives public
clients one stable HTTP endpoint while keeping internal Ursula node addresses
out of public responses.

The gateway is intentionally thin: it does not own stream routing state or
terminate SSE streams. It keeps two things in memory: the Raft leader it last
learned for each stream (or each Raft group, with `--raft-group-count`), and
which upstreams it is skipping after transport failures. It buffers request
bodies up to a configured limit only so it can replay requests when Ursula
identifies the Raft leader.

## Behavior

- Sends each incoming request to the leader it learned for the request's
  stream, or else to a configured upstream Ursula node picked at random,
  skipping upstreams that recently failed at the transport. See
  [Upstream Selection](#upstream-selection).
- Buffers request bodies up to `--max-request-body-bytes`; larger requests
  receive `413 Payload Too Large` before reaching an upstream.
- Reads the body of a request it refuses from the head alone (access control,
  tenant admission) before it answers, so that a client that sends its whole
  body first gets the refusal. See
  [Refusals Before the Body](#refusals-before-the-body).
- Follows Ursula Raft leader redirects internally, so clients never get one.
  See [Redirects](#redirects).
- Returns other redirects to clients and rewrites redirect targets so clients
  continue through the gateway.
- Keeps long-lived SSE reads open after response headers arrive.
- Optionally classifies Durable Streams requests as an explicit
  `bucket/stream/action` resource before forwarding them to provider-neutral
  authentication and authorization hooks.

## Upstream Selection

- **Leader cache.** When an upstream answers with Ursula's internal `307` and
  `x-ursula-raft-leader-id`, the gateway follows it (see
  [Redirects](#redirects)). The upstream that then serves the request with a
  success (`2xx`) becomes the route for the stream, or for the stream's Raft
  group with `--raft-group-count`. Any other answer teaches no route, since a
  node gives some before its leadership check. Later requests for the stream
  or group go to the cached leader directly. A route is dropped when its
  upstream redirects, when it answers `503` with `Retry-After` (while it does
  not know the leader, or under backpressure), or when a request to it fails.
  The cache holds up to 16384 routes and starts over when full. Requests
  without a route go to a configured upstream picked at random.
- **Ejection.** An upstream that fails at the transport (it refuses or times
  out the connection, or the exchange breaks) is ejected: requests skip it
  for 10 s. Each failure after a window ejects it again for twice as long,
  up to 60 s. Failures of requests that were already in flight do not
  lengthen a window. An upstream that sends no response headers within
  `--response-header-timeout` is not ejected, because one slow Raft group on
  a live node does the same. A node that vanished fails within
  `--upstream-tcp-user-timeout` instead.
- **Probe.** When a window expires, the next request that selects the
  upstream, along a cached route, at random, or as a search hop after a
  leader redirect, probes it. The other requests
  keep skipping the upstream until the probe answers or fails, or for at most
  `--connect-timeout` plus `--upstream-tcp-user-timeout`, after which the
  next request that selects it probes it again. Any response from the
  upstream, whatever its status, ends its ejection. That includes the
  response to a request that follows a leader redirect to it.
- **Fallback.** If every upstream is ejected or being probed, requests go
  along their cached route, or else to any upstream, rather than to none.
- **Metrics.** `/__ursula/gateway/metrics` reports `upstream_ejections`
  (times an upstream was ejected) and `upstream_ejected` (upstreams skipped
  now, inside a window or with a probe outstanding), next to the leader-cache
  counters `leader_cache_hits`, `leader_cache_misses`, `leader_cache_updates`,
  `leader_cache_evictions` and `leader_cache_entries`, and the redirect
  counters `leader_redirects` (hops that followed a redirect, search hops
  included), `leader_redirect_ns` (time spent by the requests that followed
  one), `leader_searches` (search hops) and `leader_not_found` (requests
  answered `503` because no upstream answered without redirecting). A
  gateway with access control answers the path `404` unless
  `--auth-public-metrics` is set (see
  [Access Control and Logical Tenancy](#access-control-and-logical-tenancy)).

## Access Control and Logical Tenancy

Ursula keeps the public resource model at two levels: `/{bucket}/{stream}`.
Shared hosted deployments can treat the bucket as the logical tenant or owner
namespace, while a standalone deployment can keep using bucket names exactly as
before.

`Gateway::new` does not enable access control. The stock `ursula gateway` role
therefore remains a transparent, tenant-unaware gateway and requires no OAuth
configuration.

A hosted gateway can instead construct `Gateway::with_access_control` with an
`auth::AccessControl` containing:

- a `PrincipalResolver` that verifies a presented OAuth bearer token and
  normalizes it into a provider-neutral principal;
- an `Authorizer` that evaluates the optional principal, explicit bucket and
  stream, and requested action.

Anonymous requests still reach the authorizer, allowing public buckets to be
read without a token. An authorizer can return `ConcealAsNotFound` for a private
bucket the caller cannot see. Validated bearer credentials terminate at the
gateway and are not forwarded to Ursula nodes. Once access control is enabled,
unrecognized and internal Ursula paths fail closed with `404`; a hosted binary
can mount its own health or operator routes outside the gateway fallback.

The gateway's own metrics path, `/__ursula/gateway/metrics`, is one of those
internal paths. `--auth-public-metrics` on `ursula gateway`, or
`AccessControl::with_public_metrics(true)` in a hosted binary, serves the
counters there to every caller, with or without a token, as a gateway without
access control does. The gateway then does not check a presented token on that
path, so an invalid or expired one gets the counters too. The counters hold no
tenant data, but they show traffic volume. Make them public only where
untrusted clients cannot reach the gateway, or where a proxy in front of it
denies `/__ursula/*`. Gateways before 0.7.4 served the path to every caller,
with or without access control.

## Redirects

The gateway follows Ursula's routing redirects itself, so a client never gets
one: a `307 Temporary Redirect` to the leader, with `x-ursula-raft-leader-id`,
`x-ursula-raft-group` and `x-ursula-raft-term`, and a `307` from a node that
hosts no replica of the stream's group to one that does, which names the group
alone in `x-ursula-raft-group`:

- It sends the request to the upstream that the redirect's `Location` names,
  unless the request already went to that upstream.
- Otherwise, and when the `Location` names no upstream because `--upstream`
  names the nodes otherwise than their peer URLs, it searches: it sends the
  request to the next upstream it has not tried yet, starting after the one
  that redirected, in the configured order and around. It skips ejected
  upstreams, and one whose ejection window has expired becomes the request's
  probe. A search hop that cannot connect sent nothing, so the search goes on
  to the next upstream.
- At most three hops follow redirects, search hops included, and none starts
  later than 5 s after the request's first redirect. A hop under way runs to
  its answer, so a long-poll or a large upload that reached the leader is not
  cut short.
- When no upstream answers without redirecting within these limits, the
  client gets `503` with `Retry-After: 1`, as from a node whose group has no
  known leader.

Gateways before 0.7.3 followed one redirect, to a configured upstream only,
and passed any other on to the client with a path-only `Location`.

Each hop sends the request again. Only stream requests are redirected. A
node routes a create, an append or close, and a snapshot publish before it
reads the body, so it never redirects one of them after it proposed it: a
node that loses the leadership while it reads the body answers `503` with
`Retry-After`, which the gateway passes on. A bodiless write, a delete or a
retention move, can still be redirected after OpenRaft took it, so the next
hop's copy may meet a write that already applied:

- A delete answers `404`: the stream is gone. An unconditional delete also
  deletes a stream that another client recreated between the two hops. With
  `Stream-Incarnation`, it answers `412` and deletes nothing instead, so send
  it.
- A retention move makes the same change again.

Other `307` responses remain visible to the client. Their `Location` header is
reduced to the path and query so redirect-following clients such as `curl -L`
continue through the gateway instead of connecting to internal Ursula nodes
directly.

## Proxies Around the Gateway

No response says that a write applied nothing. A client resolves an unknown
outcome by repeating the write with its producer key. See
[write outcomes](https://ursula.tonbo.io/docs/api/overview#write-outcomes).

A proxy in front of the gateway must not resend writes, and a client must not
let its HTTP library follow redirects or retry writes on its own: a resent
write without producer headers applies twice. Point `--upstream` at the nodes
themselves. A proxy, service-mesh sidecar or another gateway between the
gateway and the nodes must not resend requests either.

## Refusals Before the Body

The gateway decides some refusals from the request's head alone, before it
reads the body: every access-control refusal (`401`, `403`, `404`, and the
`503` while the token issuer or the authorizer is unavailable) and the tenant
admission refusals (`429`). From 0.7.4 it reads the body before it sends such
a refusal, and drops each chunk as it arrives. A client that sends its whole
body before it reads the answer, as many HTTP/1.1 clients do, the SQLite
extension among them, then gets the refusal with its headers.

Before 0.7.4 the gateway sent such a refusal at once and closed the connection
with the body unread. When the body was larger than one read of the gateway,
such a client often saw a failed write or a connection reset instead of the
refusal, which says nothing about whether the request was applied. The SQLite
extension then retried a refused commit as an unknown outcome until its retry
budget ran out, and poisoned the database. A proxy in front of the gateway
could also turn the closed connection into an error of its own.

The gateway reads at most `--max-request-body-bytes` of such a body, for 30 s
at most, and stops early when no data arrives for 5 s. A body that goes on
past the limit, stops arriving for 5 s, or is still arriving after 30 s gets
the refusal at once, and the connection closes with the rest of the body
unread. A body whose `Content-Length` is over the limit is not read at all,
since the reading would stop at the limit before the body's end. A client that
cannot send its body within 30 s, such as 32 MiB at less than about 9 Mbit/s,
may still see the connection close instead of the refusal. The `413` for a
body over `--max-request-body-bytes` is sent the same way: the gateway stops
reading at the limit, so a client still sending the body may see the
connection close instead of the `413`. Keep bodies below the limit on the
client, as the SQLite extension does for commits with
`URSULA_VFS_MAX_FRAME_BYTES`.

The gateway drops what it reads of a refused body, so a refused upload costs
it the body's ingress but no memory. Once the body was read to its end, the
connection stays open for the client's next request. Programs that embed the
gateway get the same from `Gateway::handle`: it returns such a refusal once
the body has been read.

Over HTTP/2 the gateway reads a refused request's body the same way, so the
client uploads its body before it gets the refusal, and a reading cut at one
of the bounds resets only that request's stream. A refusal sent at once over
HTTP/2 reached the client already, but an HTTP/1.1 client behind a proxy that
speaks HTTP/2 to the gateway gets the refusal intact only when the gateway
reads the body. So the gateway reads it over every protocol version.

A request with `Expect: 100-continue`, which curl sends for uploads over 1
MiB, gets `100 Continue` and sends its body before it gets such a refusal,
unless its `Content-Length` is over the limit. Answering at once would spare
the client the upload only while it still waits for `100 Continue`. Common
clients wait about a second (ureq 3 and curl) and then send the body anyway,
and a decision that waits on the token issuer or the authorizer can take
longer.

`/__ursula/gateway/metrics` reports `refusal_drains`, the refused requests
with a body that the gateway handled this way, and `refusal_drains_cut`, those
whose body it did not read to the end: one over the limit, one that stopped
arriving for 5 s or was still arriving after 30 s, one that failed, or one
whose request went away while the gateway read it. Only a gateway with access
control refuses requests from their head, so only its counters move. Such a
gateway answers the metrics path `404` unless `--auth-public-metrics` is set,
so read the counters with that option (see [Access Control and Logical
Tenancy](#access-control-and-logical-tenancy)).

## SSE Behavior

`live=sse` responses stream through the gateway. `--response-header-timeout`
only covers the time needed to receive response headers; streamed response
bodies remain open.

Example:

```bash
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Run

Start a gateway in front of one or more Ursula HTTP or HTTPS nodes:

```bash
cargo run -p ursula --bin ursula -- gateway \
  --listen 127.0.0.1:8080 \
  --upstream http://127.0.0.1:4437 \
  --upstream http://127.0.0.1:4438 \
  --upstream http://127.0.0.1:4439
```

Then send normal Durable Streams requests to the gateway:

```bash
curl -X PUT http://127.0.0.1:8080/demo
curl -X PUT http://127.0.0.1:8080/demo/hello

curl -X POST http://127.0.0.1:8080/demo/hello \
  -H 'Content-Type: application/octet-stream' \
  --data-binary 'hello world'

curl 'http://127.0.0.1:8080/demo/hello?offset=-1'
curl -N 'http://127.0.0.1:8080/demo/hello?offset=-1&live=sse'
```

## Options

```text
--listen <ADDR>
    Address to bind. Defaults to 0.0.0.0:4437.

--upstream <URL>
    Base URL for an Ursula HTTP or HTTPS node. Repeat once per node. Required.
    Point it at the node itself, by the node's peer URL where you can. A
    leader redirect names the leader by its peer URL, and the gateway follows
    one that names another address only by trying the upstreams it has not
    tried yet. A proxy, service-mesh sidecar or another gateway in between
    must not resend requests: a resent write without producer headers
    applies twice.

--response-header-timeout <SECONDS>
    Timeout for sending the upstream request and receiving response headers.
    Streamed response bodies such as SSE live reads are not covered.
    Defaults to 30.

--connect-timeout <SECONDS>
    TCP connect timeout per upstream request attempt. Defaults to 5.

--upstream-tcp-user-timeout <SECONDS>
    How long data sent to an upstream may stay unacknowledged before the
    connection closes and its request fails with 502 (Linux TCP_USER_TIMEOUT).
    Idle connections are probed three times within it: first after 2/5 of it,
    then every 1/5, each at least 1 s. 0.7.0 effectively used reqwest's 30 s
    default. Defaults to 5. At least 1.

--max-request-body-bytes <BYTES>
    Maximum request body bytes buffered for leader-redirect replay.
    Larger requests return 413 before upstream forwarding. Also the most the
    gateway reads of a body before it sends a refusal it decided from the
    request's head alone. Defaults to 33554432.

--graceful-shutdown-timeout <SECONDS>
    Maximum graceful shutdown drain time after SIGTERM/CTRL-C. Defaults to 3600.
```

## Operational Notes

- Upstream URLs support `http` and `https`.
- A node that vanishes without closing its connections, such as a
  force-deleted pod or a powered-off host, fails the requests waiting on them
  within `--upstream-tcp-user-timeout` with `502`. The request may have
  reached the node, so the client decides whether to retry it.
- Upstreams that fail at the transport are skipped for a while. See
  [Upstream Selection](#upstream-selection).
- Request bodies are buffered up to `--max-request-body-bytes` because internal
  leader redirects require replaying the request to a different upstream.
- The gateway keeps no durable state and does not maintain cluster membership.
  Its leader cache and upstream ejections live in memory and start empty.
- `RUST_LOG=ursula_gateway=debug` enables request forwarding and redirect logs.

## Verify

```bash
cargo fmt --all -- --check
cargo test -p ursula-gateway
cargo clippy -p ursula-gateway --all-targets -- -D warnings
```
