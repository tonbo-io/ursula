# Immutable reads

The protocol is
[extensions §4](../web/src/content/docs/pages/specs/extensions.mdx#4-immutable-reads). This
document covers how a node implements it.

## 1. Why

A reader that starts behind the tail downloads bytes that can no longer change: a subscriber that
replays a stream, or a SQLite database that attaches on a fresh host and fetches a snapshot and the
log after it. Every such reader asks the leader of the stream's group for the same bytes. A shared
cache can serve them near the reader and off the Raft nodes, if four things hold:

- The URL names the incarnation and the history. A stream recreated at the same URL, restored from
  a backup or rewound by an accepted loss reuses its offsets, so a URL without the token could name
  different bytes over time.
- Something says which answers are final. A tail answer that a cache kept would tell a later
  reader that nothing follows when something does.
- Readers share URLs. Each reader continues from where its previous answer ended, so two readers
  rarely ask for the same offset unless the server fixes the boundaries.
- A cache keyed on the path alone cannot merge two resources. A query string is ignored by many
  CDN configurations.

## 2. Shape

| Part | Set by | Meaning |
| --- | --- | --- |
| `{stream}/incarnation/{token}/chunk/{offset}` | client | The bytes from `offset` to the end of its 4 MiB chunk, or to the tail, of the stream under `token`. |
| `{stream}/incarnation/{token}/snapshot/{offset}` | client | The latest snapshot at `offset` under `token`. |
| `Stream-Offset`, `Stream-Next-Offset`, `Stream-Incarnation` | node | What the answer holds, which a client checks against the URL it asked for. |
| `Stream-Immutable: true` | node | No later answer for this URL contradicts this one. |
| `Stream-Read-Origin` on `HEAD` | deployment | Where to send these requests. |

Each layer decides what it knows:

| Layer | Knows | Decides |
| --- | --- | --- |
| Ursula node | the stream's state | whether an answer is immutable |
| Gateway or proxy in front | who may read a bucket, which CDN exists | `Cache-Control` on marked answers, the hint |
| Cache | nothing about streams | stores what `Cache-Control` allows |
| Client | its own trust settings | whether to follow the hint, and checks every answer |

The node sends `Cache-Control: no-store` on every answer and never sends the hint. A deployment
with no layer in front gets correct answers that no cache stores.

## 3. Node

`crates/ursula/src/immutable_reads.rs` holds the routes and the grid. The header crate
(`crates/ursula-headers/src/immutable.rs`) parses and renders the path (`ImmutableResource`), the
chunk answer (`ChunkRead`, with `ChunkMark`), the snapshot answer (`ImmutableSnapshotRead`) and the
hint (`ReadOrigin`), so the node and its clients share one implementation.

**Routes.** Both resources are `GET`, and so `HEAD`, on the client router. A query component
answers `400`. `ImmutableResource::parse` accepts a token only as canonical decimal and an offset
only as 20 zero-padded digits, the forms the server sends. A chunk read from inside a chunk is a
second, overlapping URL by design, and percent-encoded digits reach the same resource, which only
splits a cache key. Anything
else answers `400` `INVALID_RESOURCE`, and a query component `400` `QUERY_NOT_ACCEPTED`. A path
segment that is not valid UTF-8 once percent-decoded never reaches the route: the router's path
extractor refuses it, and the router's net renders `400` `INVALID_REQUEST`. The path's token is the `Stream-Incarnation` precondition, and the request's
own header is not read. The read itself evaluates `404`, `410` and `416` first, and `412` comes
after them, as for every read.

**The grid.** `chunk_len` turns the offset into the length to the next multiple of
`CHUNK_GRID_BYTES` (4 MiB). A const assertion keeps a chunk within the 8 MiB response cap, so an
answer below the tail always reaches the end of its chunk.

**Marking.** `ursula_runtime::immutable_read` is the one rule. A read is immutable when it filled
the length it asked for and ended below the tail of the state that served it, or when it ended at
the tail of a closed stream. `ReadEnd` reads `up_to_date` and `closed` once: below the tail, at an
open tail, or at a closed tail. A read that ends at an open tail is never immutable, even when it
filled its length, because the stream may still be closed at that offset. An empty answer at a
closed tail is marked, and it carries `Stream-Closed: true`.

**Followers and the leader.** A `ReadStreamRequest` names what a follower may serve from its own applied state
(`FollowerRead`). A base-protocol read serves any applied prefix (`Prefix`). A chunk read
(`Immutable`) is served by a follower only when the answer is immutable by the rule above.
Otherwise the follower forwards the read to the leader, whose answer stands. So a chunk that
reaches a lagging follower's own tail, which the follower cannot tell from the stream's tail, is
answered by the leader, and an unmarked answer always means that the open tail lies within the
chunk. The policy travels with a forwarded read (`ReadStreamReadV1.follower`), so a node that lost
its leadership while the read was in flight applies it too. A follower whose token differs from
the path's forwards as for any read with a precondition, and a follower forwards a missing stream,
an offset beyond its tail and an offset below its retained offset, which may hold only for an
incarnation it has not seen replaced (`may_hold_only_for_a_lagging_state`). With no leader
known, a chunk the follower cannot mark answers `503`, and a marked one is still served.

The leader never forwards. It answers a chunk it cannot mark, and a read that would answer `404`,
`410` or `416`, only after a ReadIndex round (`require_linearizable_leader_read`) and a second
plan. The same predicate decides these errors for the follower's forward and the leader's round.
A leader that was just elected, or was deposed without knowing it, may not have applied everything
committed: without the round its chunk could be short of one another replica marked, and its `404`
or `410` could be for the stream before a recreate. A markable answer holds committed bytes, so it
needs no round, and neither does a base-protocol read that succeeds. A deposed leader whose last quorum
acknowledgement is older than its lease no longer leads (`LeaderRole::Lapsed`) and takes the
follower's path. Inside its lease, or before any quorum acknowledged it, it still leads, as after a
leadership-transfer election, which skips the voters' leases, and then only the round stops it.

**Refusals.** A `410` or `416` of any read, and the `404` of a snapshot that is not the latest, carry the token of the stream that decided them: the state machine puts it in the error's context (`StreamErrorContext::StreamIncarnation`), the context survives a forward from a follower, and `render::insert_stream_error_headers` renders it. These answers come before the precondition, so a client compares the token instead of sending a `HEAD`.

**Answers.** `render::chunk_response` renders the chunk with `ChunkRead`: `Content-Type` (NDJSON
for a JSON stream), `Stream-Incarnation`, `Stream-Offset`, `Stream-Next-Offset`, the marker, and
`Stream-Closed` at a closed tail. It sends no `Stream-Up-To-Date` and no `Stream-Cursor`. The
snapshot resource shares `snapshot_answer` with the stream's own snapshot route, which renders
`ImmutableSnapshotRead` for it: the marker on `200`, `206` and `304`, and never
`Stream-Up-To-Date`. A range past the end answers `416` as a JSON error answer,
`RANGE_NOT_SATISFIABLE` with `Content-Type: application/json`, that keeps the snapshot headers,
the `ETag`, `Accept-Ranges` and `Content-Range: bytes */total`, without the marker.

**Validators.** `read_etag` is BLAKE3 over the token, the range, where the read ended, the content
type and the body, cut to 128 bits. Under one token the range names the bytes (G1), but the body is
hashed too: a deployment that breaks G1 without a history event, such as one restored from volume
snapshots (unsupported), then gets the bytes it now serves at revalidation instead of a `304`.
Where the read ended counts closure only at the tail, so closing a stream keeps the tag of every
chunk below it. BLAKE3 makes the tag the same on every node and release. A snapshot's tag is its
digest. An ordinary read that the compression layer may gzip (`render::compressible_media` and
`COMPRESS_MIN_BYTES`, the predicate the layer uses) gets the weak form of the tag, since one strong
tag must not cover two representations. Chunks are never compressed and keep the strong form.
`If-None-Match` is compared weakly, as RFC 9110 requires. A request whose `If-None-Match` names
the tag answers `304` with every header of the `200`, the marker included, and for a snapshot
before any range. A chunk's `304` still reads the chunk, since the tag covers the body. A
snapshot's does not, and neither does a snapshot `HEAD`, which answers from the metadata.

**Error answers.** Every refusal goes through the client plane's one error renderer
(`render::error_answer`, extensions Section 11), which sends the default headers,
`Cache-Control: no-store` among them, and the JSON body: `QUERY_NOT_ACCEPTED` for a query
component, `INVALID_RESOURCE` for a malformed token or offset, `UNSUPPORTED` for the refused query
form, and, through the router's net for answers no handler rendered, `METHOD_NOT_ALLOWED` and
`NOT_FOUND`.

**No compression.** `render::uncompressed` puts a response extension on chunk and snapshot answers,
and the compression layer's predicate skips any answer that has it. A marked answer then has one
representation per tag and carries no `Vary`.

## 4. Why a cache never needs purging

A token names one history of one incarnation. A delete and recreate, a restore and an accepted
loss each retire the token before the node serves anything of the new history, so every URL a
cache keyed under the old token answers `412` at the node from then on. What the cache keeps is
correct for its URL forever, and clients learn the new token from `HEAD` at the stream's own
origin. A cache may serve a marked answer after the stream is deleted, until the entry expires.
The deployment bounds that lifetime (extensions §4.5).

## 5. Tests

- `crates/ursula/src/removed_surface.rs`: the query form of the earlier design answers `400`.
- `crates/ursula/src/immutable_reads_tests.rs`: marking below the tail, at an open tail and at a
  closed tail, a chunk read from inside a chunk, `304` with the marker, `HEAD`, no compression on
  a JSON chunk, a recreate that retires the URLs and changes the tag, the order of `404`, `416`,
  `410` and `412`, every `400`, and the snapshot resource with ranges, `416`, `304`, `412` and a
  superseded snapshot.
- `crates/ursula-raft/src/rejoin_grpc_tests.rs`: a lagging follower forwards the chunks it cannot
  mark and answers the ones below its tail, a lagging follower forwards a `410` of a recreated
  stream, and a leader that cannot reach a quorum refuses the chunks it cannot mark and the `416`s
  while it answers markable ones.
- `crates/ursula-sim/src/madsim_harness/immutable_reads.rs` and `immutable_reads_loss.rs`: the
  `marked_chunks_never_contradict` DST. A cache-like observer keeps every marked answer, through
  every replica and through the HTTP chunk route, across leader kills with reads in flight,
  lagging followers, closes, recreates, retention advances, a restore and an accepted loss with
  its recovery plan, and every later answer for its URL agrees or fails as allowed. Immutable
  snapshot reads through the route keep their digest and body. Its negative controls turn off the
  open-tail rule, the follower's check and the leader's round.
- `crates/ursula/src/tests.rs`: through a follower's HTTP surface, a marked chunk without a leader
  and the tail chunk refused with `503`. The restore drill records a marked chunk and a closed
  tail, restores, regrows, and finds both URLs answering `412` and `HEAD` naming the recorded
  token.
- `crates/ursula-headers`: round trips and parse failures of every type, with property tests.
