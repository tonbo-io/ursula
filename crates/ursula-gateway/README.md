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
- Buffers request bodies up to `--max-request-body-bytes`. Larger requests
  receive `413 Payload Too Large` before reaching an upstream.
- Bounds the bodies it buffers across requests with a node-wide budget and
  headroom for small bodies, bounds each bucket's large uploads under a quota
  policy, and cuts bodies whose client abandoned them. See [Request Body
  Budget](#request-body-budget).
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

- After a leader redirect, it sends the request to the upstream that the
  redirect's `Location` names, unless the request already went to that
  upstream. When that upstream cannot be connected to, the request ends with
  `503` and `Retry-After`.
- The host that a non-hosting node's redirect names is a hint, often the
  node its group moves away from. The gateway sends the request there only
  when the request has not tried that upstream and it is not ejected. A hop
  to it that cannot connect sent nothing, so the gateway searches on as it
  does after a search hop that cannot connect. A blackholed host costs one
  connect, after which it is ejected and later hints to it are passed over.
  The search after that connect starts only when the connect timeout is
  shorter than the 5 s redirect deadline. With the default `--connect-timeout 5`,
  the request that pays the blackholed connect ends with `503` and
  `Retry-After`, and only later requests pass the ejected host.
- Otherwise, and when the `Location` names no upstream because `--upstream`
  names the nodes otherwise than their client URLs, it searches: it sends the
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
node redirects a write only when it refused the write before proposing it,
so no hop's copy meets a write that an earlier hop applied. A node routes a
create, an append or close, and a snapshot publish before it reads the body.
A node that loses the leadership before it proposes a write, while it reads
the body or after, answers `503 NO_LEADER` with `Retry-After`: nothing
applied. A node that loses it after it proposed the write and before it saw
the apply answers `503 OUTCOME_UNKNOWN` with `Retry-After`: the write may
have applied. The gateway passes both on. The same holds for a write
without a body, a delete or a retention move.

Other `307` responses remain visible to the client. Their `Location` header is
reduced to the path and query so redirect-following clients such as `curl -L`
continue through the gateway instead of connecting to internal Ursula nodes
directly.

## Proxies Around the Gateway

A refusal says that its request applied nothing by its status and error
code, by one rule for every client, which `ursula_headers::classify`
implements: every refusal the gateway sends itself is one, and so is a
node's `NO_LEADER`. Any other outcome, such as `OUTCOME_UNKNOWN`, a `502` or
a `504`, is unknown, and a client resolves it by repeating the write with
its producer key. See
[write outcomes](https://ursula.tonbo.io/docs/specs/extensions#111-write-outcomes).

A proxy in front of the gateway must not resend writes, and a client must not
let its HTTP library follow redirects or retry writes on its own: a resent
write without producer headers applies twice. Point `--upstream` at the nodes
themselves. A proxy, service-mesh sidecar or another gateway between the
gateway and the nodes must not resend requests either.

A client reads any `4xx` other than `408` as a refusal that applied nothing,
whoever sent it. So a proxy in front of the gateway must never answer a write
with such a `4xx` after it forwarded any of the request, for example from a
firewall that inspects the response, as
[write outcomes](https://ursula.tonbo.io/docs/specs/extensions#111-write-outcomes)
requires. The gateway itself uses no system proxy (`HTTP_PROXY`) for its
upstreams.

## Refusals of an Embedder

A program that embeds the gateway can refuse a request before the gateway
forwards it, such as one of an unpaid account, with `Gateway::refuse` and an
`EmbedderRefusal`:

| Refusal | Status | Code |
| --- | --- | --- |
| `PaymentRequired` | `402` | `PAYMENT_REQUIRED` |
| `AccountRevoked` | `403` | `ACCOUNT_REVOKED` |
| `Forbidden` | `403` | `FORBIDDEN` |
| `RateLimited` | `429`, with `Retry-After` | `RATE_LIMITED` |

`Gateway::refuse` reads and drops the request's body first, as the gateway
does for its own refusals from the head (see
[Refusals Before the Body](#refusals-before-the-body)), then answers through
the gateway's one error renderer: the JSON error body with the code,
`Cache-Control: no-store`, and the gateway's CORS headers. The answer carries
no producer header and no server identity. A client reads it as a refusal:
nothing applied. A node's producer `403` always carries `Producer-Epoch` and
`Producer-Outcome`, so a client tells the two apart. The codes are fixed, so
every client knows them. An embedder that needs another refusal asks for a
code to be added.

## Refusals Before the Body

The gateway decides some refusals from the request's head alone, before it
reads the body: every access-control refusal (`401`, `403`, `404`, and the
`503` while the token issuer or the authorizer is unavailable) and the
tenant admission refusals (`429`). From 0.7.4 it reads the body before it
sends such a refusal, and drops each chunk as it arrives. It does the same
for the refusals of a body it would otherwise forward, described in [Request
Body Budget](#request-body-budget). A client that sends its whole
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
whose request went away while the gateway read it, and `refusal_drain_bytes`,
the bytes of those bodies it read and dropped. They count the refusals
of [Request Body Budget](#request-body-budget) too, which a gateway without
access control also sends. A gateway with access
control answers the metrics path `404` unless `--auth-public-metrics` is set,
so read the counters with that option (see [Access Control and Logical
Tenancy](#access-control-and-logical-tenancy)).

## Request Body Budget

The gateway buffers the body of each request it forwards whole, so that it
can send it again after a leader redirect. `--max-request-body-bytes` bounds
one body. The body budget bounds the bodies under way across requests, so
that concurrent large or slow uploads cannot exhaust the gateway's memory.

`--max-request-body-bytes` is at most 32 MiB, the protocol's request limit
(`ursula_headers::MAX_REQUEST_BYTES`), and a gateway configured above it
refuses to start. An Ursula node refuses a body over that limit from its
`Content-Length`, before reading it, and closes the connection. The gateway,
still sending the body, would then see a reset and answer `502`, which a
client reads as an unknown outcome. At or below the limit, the gateway's own
`413` refuses every body the node would, typed and before forwarding.

Before it reads any byte of a body, a request reserves the body's
`Content-Length` from `--body-budget-bytes`. A body without a declared length
reserves `--small-body-max-bytes` at first, and its reservation grows as the
body arrives. The gateway copies each piece of a body into one buffer as it
arrives and drops the piece, however small the pieces: a declared body gets a
buffer of its `Content-Length`, and one without a declared length grows its
buffer within its reservation, briefly holding up to twice its share while the
buffer reallocates. The reservation shrinks to the buffer's size once the
gateway has the body whole, and ends when the gateway is done forwarding it,
when it refuses the request, or when the client goes away. A request without a
body reserves nothing.

- `--small-body-headroom-bytes` of the budget only bodies of at most
  `--small-body-max-bytes` may take, so that small writes, such as SQLite
  commits, never queue behind large uploads. A small body takes the rest of
  the budget first and the headroom only when the rest is spent. A body
  without a declared length counts as small until it grows past the
  threshold, and then leaves the headroom.
- A request that finds the budget spent waits for it for
  `--body-budget-wait-ms`, and then gets `503` with `Retry-After: 1`.
- With `--body-budget-bytes` unset, the budget is 256 MiB or
  `--max-request-body-bytes` plus `--small-body-headroom-bytes`, whichever is
  larger, so raising the limit alone grows the budget, with a warning. A
  budget set explicitly must hold a body at the limit beside the headroom, and
  `--small-body-max-bytes` must fit the headroom. The gateway exits at startup
  otherwise.
- Give the gateway at least twice its budget in memory. The gateway warns at
  startup when the budget exceeds half of its cgroup memory limit, which it
  looks up along its own cgroup path. Since the budget cannot go below the
  limit plus the headroom, the remedy is a lower `--max-request-body-bytes`,
  with the budget lowered to match, or more memory.
- Once grown, the budget holds exactly one body at the limit beside the
  headroom. Requests wait for the budget in order, so a request near the limit
  that waits for the whole budget takes every share released meanwhile, for up
  to `--body-budget-wait-ms`, and large bodies behind it can get `503` too.

A body declared over its bucket's own limit (`max_request_body_bytes` in the
quota policy) gets its `413` before it reserves anything. A body declared
over `--max-request-body-bytes` gets it at once, with the body unread, and
the connection closes.

### Bucket Upload Limits

With access control and a quota policy (`--quota-policy`), a bucket may also
bound its large bodies, those over `--small-body-max-bytes`, while the
gateway reads them:

```toml
[default]
max_concurrent_uploads = 8
max_upload_bytes = 67108864

[[bucket]]
id = "tenant-a"
max_concurrent_uploads = 2
```

- `max_concurrent_uploads`: the bucket's large bodies the gateway reads at
  once.
- `max_upload_bytes`: the bytes those bodies reserve at once. The bucket's
  only large body under way always passes, so a cap below a body's size never
  refuses it for good.

A `[[bucket]]` entry replaces `[default]` for its bucket rather than
inheriting from it. In the example, `tenant-a` has two concurrent uploads and
no `max_upload_bytes`. Repeat every limit the bucket should keep.

A body counts from its reservation until the gateway has buffered it, not
while a node commits it. A request past either limit gets `429` with
`Retry-After: 1` at once. Small bodies never count and are never refused by
these limits. Without a quota policy, or for a bucket the policy gives
neither limit, only the budget bounds a bucket's uploads. Like the other
quota limits, these are per gateway process.

### Abandoned Uploads

The gateway cuts a body only once its client has abandoned it: when no byte
of it arrives for `--body-stall-timeout` (30 s by default), or when less than
`--body-min-bytes-per-sec` (1 KiB/s by default) arrives over one window of
the stall timeout, counted from the start of the reading. It then answers
`408`, with nothing of the body forwarded. Nothing bounds a body's reading in
all, and neither rule looks at its size or at its average rate since the
start, so a slow upload that keeps arriving above the floor is never cut,
however long it takes. Over HTTP/1.1 the connection closes after the `408`,
so a client that is still sending may see the closed connection instead.

A chunk counts toward the window it arrives in, so a pause of just under the
stall timeout that spans the end of a window, on a link slower than the floor
over a whole window, can leave that window short. The SQLite extension gives a
commit 10 s and a snapshot transfer 120 s. With the defaults the gateway never
cuts a body that keeps arriving. Keep `--body-stall-timeout` above 10 s. The
gateway warns at startup otherwise.

### Refusals and Metrics

The gateway reads the body of each refusal here other than the `408` to its
end before it answers, at the pace a forwarded body may keep, and drops each
chunk. A client that sends its whole body first then gets the refusal on an
intact connection, as in
[Refusals Before the Body](#refusals-before-the-body). That holds for the
`413` of a body over its bucket's own limit too. Only a body past
`--max-request-body-bytes` gets its `413` with the rest unread.

Each refusal here carries the JSON error body with its own code:

| Status | Code | Refusal |
| ------ | ---- | ------- |
| `413` | `REQUEST_TOO_LARGE` | The body is over `--max-request-body-bytes` or the bucket's limit. `Ursula-Max-Request-Bytes` names the smaller of the two |
| `429` | `UPLOAD_BYTES_EXHAUSTED` | The large body would take the bucket's large bodies under way past its `max_upload_bytes`. With `Retry-After: 1` |
| `429` | `UPLOADS_EXHAUSTED` | The bucket has `max_concurrent_uploads` large bodies under way. With `Retry-After: 1` |
| `503` | `UPLOAD_BUDGET_EXHAUSTED` | The body budget stayed spent for `--body-budget-wait-ms`. With `Retry-After: 1` |
| `408` | `BODY_TOO_SLOW` | No byte of the body arrived for `--body-stall-timeout`, or less than `--body-min-bytes-per-sec` over one such window |

`/__ursula/gateway/metrics` reports `upload_refusals_tenant_bytes`,
`upload_refusals_tenant_uploads`, `upload_refusals_budget` and
`upload_refusals_too_slow`, the refusals by reason, `upload_budget_bytes`, the
budget in effect, `upload_reserved_bytes`, the bytes reserved now with each
body's share rounded up to whole KiB, `upload_tenants`, the buckets with a
large body under way under an upload limit now, and `body_bytes_buffered`, the
bytes of bodies the gateway read to forward them, a body cut or refused part
way included. The rest of a body refused part way counts in
`refusal_drain_bytes`.

The defaults suit a 512 MiB pod with the default 32 MiB per-request limit.
The 256 MiB budget is half of the pod, leaving the rest to response streams,
connection buffers and the allocator. The 32 MiB headroom holds at least 32
small bodies of up to 1 MiB, and the rest of the budget seven bodies at the
limit.

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
    Point it at the node itself, by the node's client URL where you can:
    its [[raft.peers]] client_url, or its peer url when the node serves
    both planes on one port. Never use a cluster port (server.cluster_listen),
    which serves no client route. A leader redirect names the leader by its
    client URL, and the gateway follows one that names another address only
    by trying the upstreams it has not tried yet. A proxy, service-mesh
    sidecar or another gateway in between must not resend requests: a resent
    write without producer headers applies twice.

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
    gateway reads of a body before it sends a refusal. Must fit
    --body-budget-bytes less --small-body-headroom-bytes. At most 33554432,
    the protocol's request limit, which every node applies: a larger value
    refuses to start (see Request Body Budget). Defaults to 33554432.

--body-budget-bytes <BYTES>
    Bytes of request bodies the gateway buffers at once, across requests,
    the small-body headroom included. See Request Body Budget. Defaults to
    268435456.

--small-body-headroom-bytes <BYTES>
    Bytes of the body budget only bodies of at most --small-body-max-bytes
    may take. Must hold one such body. Defaults to 33554432.

--small-body-max-bytes <BYTES>
    The largest declared body length that counts as small: it may take the
    small-body headroom and never counts toward a bucket's upload limits.
    Defaults to 1048576.

--body-budget-wait-ms <MILLISECONDS>
    How long a request waits for the body budget before the gateway answers
    503. 0 refuses at once. Defaults to 1000.

--body-stall-timeout <SECONDS>
    How long a request body may go without a byte arriving before the
    gateway answers 408, and the window --body-min-bytes-per-sec is measured
    over. At least 1. Defaults to 30.

--body-min-bytes-per-sec <BYTES>
    The least rate at which a request body must arrive over each window of
    --body-stall-timeout. A slower body is answered 408 before anything of it
    is forwarded. 0 disables the floor. Defaults to 1024.

--graceful-shutdown-timeout <SECONDS>
    Maximum graceful shutdown drain time after SIGTERM/CTRL-C. Defaults to 3600.
```

## Operational Notes

- Upstream URLs support `http` and `https`.
- A node that vanishes without closing its connections, such as a
  force-deleted pod or a powered-off host, fails the requests waiting on them
  within `--upstream-tcp-user-timeout` with `502`. The request may have
  reached the node, so the client decides whether to retry it.
- The gateway reuses an idle upstream connection for at most 0.5 s. A node
  that stops gracefully stops accepting connections, marks every answer it
  starts from then on `Connection: close`, and keeps a connection that was
  idle when it began to stop open for up to 1 s. A request the gateway sends
  on such a connection is answered if it reaches the node within that second.
  A new connection to the stopping node is refused, and the gateway answers as
  for an unreachable upstream. Two cases still fail with `502`, because the
  node may have read the request. One is a connection the node's kernel had
  completed but the node had not accepted when it stopped accepting, which is
  reset after the gateway wrote its request. The other is a connection whose
  last answer started before the stop. That answer carries no
  `Connection: close`, so the gateway may send its next request on the
  connection as it closes.
- Upstreams that fail at the transport are skipped for a while. See
  [Upstream Selection](#upstream-selection).
- Request bodies are buffered up to `--max-request-body-bytes` because internal
  leader redirects require replaying the request to a different upstream.
  The bodies under way across requests stay within `--body-budget-bytes`.
- The gateway keeps no durable state and does not maintain cluster membership.
  Its leader cache and upstream ejections live in memory and start empty.
- `RUST_LOG=ursula_gateway=debug` enables request forwarding and redirect logs.

## Verify

```bash
cargo fmt --all -- --check
cargo test -p ursula-gateway
cargo clippy -p ursula-gateway --all-targets -- -D warnings
```
