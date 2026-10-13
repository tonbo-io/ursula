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
- Marks its answers to requests that no node can have applied with the
  `x-ursula-not-applied` response header, and sets
  `x-ursula-not-applied-checked: 1` on every response. See
  [Refusals That Applied Nothing](#refusals-that-applied-nothing).

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

The gateway follows Ursula `307 Temporary Redirect` responses marked as Raft
leader redirects with `x-ursula-raft-leader-id` itself, so a client never gets
one:

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
  cut short. Nor does a slow first hop use the 5 s up: a follower can store a
  large body in object storage before it redirects.
- When no upstream answers without redirecting within these limits, the
  client gets `503` with `Retry-After: 1`, as from a node whose group has no
  known leader.

Gateways before 0.7.3 followed one redirect, to a configured upstream only,
and passed any other on to the client with a path-only `Location`.

Each hop sends the request again. Only stream requests are redirected. Two
hops of one request are at most 5 s and the earlier hop's own time apart.
When a node proposed a write before it redirected, so that the write may
already apply, the next hop's copy fares as follows:

- A producer append or close is deduplicated by its `Producer-Id`,
  `Producer-Epoch` and `Producer-Seq`.
- A create answers `200` for a stream that already exists as it asks. If
  another client deleted the stream between the two hops, it creates the
  stream again.
- A close without a body answers as the first close did.
- A snapshot publish or a retention move makes the same change again.
- A delete answers `404`: the stream is gone. An unconditional delete also
  deletes a stream that another client recreated between the two hops. With
  `Stream-Incarnation`, it answers `412` and deletes nothing instead, so send
  it.
- An append with `Stream-Seq`, or one that also closes the stream, is refused
  with `409`, which its client cannot tell apart from a conflict with another
  writer.
- A plain append, without a producer or `Stream-Seq`, applies again: once for
  each node that proposed it and then redirected. Its client gets
  at-least-once delivery, as on any retry of it, and as through gateways
  before 0.7.3, which followed one redirect. Use a producer for exactly-once
  writes.

Other `307` responses remain visible to the client. Their `Location` header is
reduced to the path and query so redirect-following clients such as `curl -L`
continue through the gateway instead of connecting to internal Ursula nodes
directly.

## Refusals That Applied Nothing

The gateway sets the `x-ursula-not-applied` response header, the marker,
when no node can have applied the request: on every request it refuses
before forwarding it, on a request it could not send because the connection
to its first upstream failed, and after redirects that were all marked
(below). The value names the refusal:

| Status | Token | Refusal |
| ------ | ----- | ------- |
| `404` | `NotFound` | Access control does not serve the route, or the authorizer concealed the resource |
| `401` | `InvalidCredential` | The bearer credential is malformed or failed verification |
| `503` | `AuthenticationUnavailable` | The credential could not be verified, for example because the token issuer is down |
| `403` | `Forbidden` | The authorizer denied the request |
| `503` | `AuthorizationUnavailable` | The authorizer could not decide |
| `429` | `TenantRateLimited` | The bucket is over its request rate |
| `429` | `TenantLiveReadsExhausted` | The bucket uses all of its live-read connections |
| `413` | `RequestBodyTooLarge` | The body is over `--max-request-body-bytes` or the bucket's limit |
| `429` | `TenantUploadBytes` | The large body would take the bucket's large bodies under way past its `max_upload_bytes` |
| `429` | `TenantUploads` | The bucket has `max_concurrent_uploads` large bodies under way |
| `503` | `UploadBudgetExhausted` | The body budget stayed spent for `--body-budget-wait-ms` |
| `408` | `RequestBodyTooSlow` | No byte of the body arrived for `--body-stall-timeout`, or less than `--body-min-bytes-per-sec` over one such window |
| `400` | `RequestBodyUnreadable` | The request body could not be read |
| `503` | `NoUpstream` | No upstream is configured |
| `503` | `UpstreamUnreachable` | The connection to the first upstream failed, so the request was never sent. Or the connection to an upstream after redirects failed, and every redirect was marked |
| `503` | `LeaderNotFound` | No upstream answered without redirecting within the [redirect limits](#redirects), and every redirect was marked |

A node's marker passes through unchanged when the gateway sent the request
to one node. After the gateway follows Raft leader redirects, it keeps the
final answer's marker only if every redirect carried one too, because a node
can redirect a write after it proposed it. A node marks only the redirects it
sends before it proposes a write, so a leader's admission refusal reached
through such redirects keeps its marker. The gateway's `503` for an upstream
it could not connect to after redirects, and its `503` when it stops
following redirects, are marked only when every redirect was marked. A hop
that could not connect sent nothing, so it does not count against the
marker. The other
answers the gateway gives when an upstream fails carry no marker: the `502`,
and the `504` for an upstream that sent no response headers in time.

From 0.7.3 the gateway sets `x-ursula-not-applied-checked: 1`, the vouch, on
every response it returns: forwarded responses, its own refusals and its own
errors. It adds the vouch after the redirect rule above decided whether a
node's marker stays, and drops any vouch that came from an upstream. Nodes
never set it. A client counts an attempt as never applied only when the
response carries both headers, or has the status `401`, `402` or `429`.
Gateways before 0.7.3 never send the vouch, so clients ignore their markers,
whatever order gateways and nodes are upgraded or rolled back in. Upgrading
gateways first is still recommended, so that clients keep the benefit of the
marker during a rollout. See
[refusals that applied nothing](https://ursula.tonbo.io/docs/api/overview#refusals-that-applied-nothing).

The vouch covers only the one request the gateway received. A proxy in front
of the gateway must not resend writes, and a client must not let its HTTP
library follow redirects or retry writes on its own.

The vouch also covers only the exchanges the gateway makes itself, so point
`--upstream` at the nodes themselves. A proxy, service-mesh sidecar or
another gateway between the gateway and the nodes must not resend requests,
or the gateway vouches for a marker from a later delivery. For example, a
node can propose a write and then answer `503` without a marker. If a
sidecar resends the request on that `503`, as Istio's default retry policy
did before 1.24, and a node refuses the copy at admission with a marked
`503`, the gateway sees only that refusal and vouches for it. For the same
reason, a 0.7.3 gateway must not sit in front of an older gateway, which
follows redirects itself and passes a leader's marker on after any of them.

Nothing between the gateway and the nodes may answer `401`, `402` or `429`
either, as a sidecar's local rate limit does. A client counts those
statuses as never applied without the headers, and after a redirect without
a marker the gateway returns the leader's status unchanged, although the
node that redirected may have proposed the write.

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
whose request went away while the gateway read it. They count the refusals
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

Before it reads any byte of a body, a request reserves the body's
`Content-Length` from `--body-budget-bytes`. A body without a declared length
reserves `--small-body-max-bytes` at first, and its reservation grows as the
body arrives. The reservation shrinks to the body's size once the gateway has
it whole, and ends when the gateway is done forwarding it, when it refuses
the request, or when the client goes away. A request without a body reserves
nothing.

- `--small-body-headroom-bytes` of the budget only bodies of at most
  `--small-body-max-bytes` may take, so that small writes, such as SQLite
  commits, never queue behind large uploads. A small body takes the rest of
  the budget first and the headroom only when the rest is spent. A body
  without a declared length counts as small until it grows past the
  threshold, and then leaves the headroom.
- A request that finds the budget spent waits for it for
  `--body-budget-wait-ms`, and then gets `503` with `Retry-After: 1`.
- `--max-request-body-bytes` must fit the part of the budget large bodies may
  take, `--body-budget-bytes` less `--small-body-headroom-bytes`, and
  `--small-body-max-bytes` must fit the headroom. The gateway exits at startup
  otherwise. Raise `--body-budget-bytes` together with
  `--max-request-body-bytes`: a 1 GiB limit needs a budget of at least 1 GiB
  and 32 MiB, and memory to match.

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

The SQLite extension gives a commit 10 s and a snapshot transfer 120 s. With
the defaults the gateway never cuts a body the extension still waits for.
Keep `--body-stall-timeout` above 10 s. The gateway warns at startup
otherwise.

### Refusals and Metrics

The gateway reads the body of each refusal here to its end before it answers,
at the pace a forwarded body may keep, and drops each chunk. A client that
sends its whole body first then gets the refusal on an intact connection, as
in [Refusals Before the Body](#refusals-before-the-body). That holds for the
`413` of a body over its bucket's own limit too. Only a body past
`--max-request-body-bytes` gets its `413` with the rest unread.

Every refusal here carries `x-ursula-not-applied` (see [Refusals That Applied
Nothing](#refusals-that-applied-nothing)). `/__ursula/gateway/metrics`
reports `upload_refusals_tenant_bytes`, `upload_refusals_tenant_uploads`,
`upload_refusals_budget` and `upload_refusals_too_slow`, the refusals by
reason, `upload_reserved_bytes`, the budget reserved now in whole KiB, and
`upload_tenants`, the buckets with a large body under way under an upload
limit now.

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
    Point it at the node itself, by the node's peer URL where you can. A
    leader redirect names the leader by its peer URL, and the gateway follows
    one that names another address only by trying the upstreams it has not
    tried yet. A proxy, service-mesh sidecar or another gateway in between
    must not resend requests, or the gateway vouches for an
    x-ursula-not-applied marker from a later delivery. Nor may it answer
    401, 402 or 429, for example from a local rate limit: a client counts
    those statuses as never applied.

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
    --body-budget-bytes less --small-body-headroom-bytes. Defaults to
    33554432.

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
