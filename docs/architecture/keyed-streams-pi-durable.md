# Keyed Streams: Pi Durable on Ursula

Status: Accepted 2026-10-02 (Q0–Q5 confirmed; Q6–Q8 take the recommendations; `/bootstrap` fix uses the honest-partial form). Implementation in progress on `agent/keyed-streams`.

Scope: make Ursula the single storage backend for Pi Durable. This document defines a writer-declared keyed record format, a keyed-state read resource served from a rebuildable projection, JSON message text fidelity, byte-bounded record reads, and the TypeScript owner that implements Pi's 17-method `Storage` contract on top of them. The Ursula-core work that lets a log stay untrimmed with bounded replicated memory is general and lives in `docs/architecture/bounded-stream-state.md`; §6.2 summarizes what Pi needs from it.

Related: `docs/web/src/content/docs/pages/specs/extensions.mdx` §1.4 (bucket stream listing), §1.7 (path affinity), §2 (snapshots), §6 (JSON record coordinates); `docs/architecture/bounded-stream-state.md` (the never-trim core, C0 to C7); `docs/architecture/json-record-coordinates-validation.md` (external index boundary); `docs/architecture/raft-wal-production.md`; `docs/architecture/deterministic-simulation-testing.md`; Ursula #84/#85 (record coordinates), #86 (event time), #87 (append sessions); Durable Streams #404 (server-side compaction of State Protocol streams), #405 (write fencing), #281 (JSON validation envelope), #110 (RFC 9457 errors); Pi Durable `packages/durable/docs/spec.md` and `src/testing/storage-conformance.ts`.

Conventions: Ursula paths are relative to the repository root. Pi paths are prefixed `pi:` and are relative to the Pi monorepo. Figures marked *measured* come from the probes in §11.9; all other figures are priors that M0 replaces with measurements.

## 1. Summary

1. One Pi harness is one Ursula JSON stream at a unique, never-reused path. One Pi commit is one record, and Pi's `Seq` is the record ordinal. The first record is the creator's claim, so Pi Seqs start at 1.
2. Records use a writer-declared format, `keyed-batch-v1`: `{"ops":[["p",k,v],["d",k],["x",start,end]]}`, with opaque base64url keys and verbatim JSON values. Nodes validate the grammar at append time and never run user logic. All Pi denormalization (covering rows, status indexes, `document.copy` materialization, current-only pruning, the global ID registry) is computed by the owner.
3. The log is the permanent source of truth and is never trimmed. The keyed projection is a versioned, disposable cache: every published namespace equals `fold(log[0, D))`. Format changes and corruption are handled by rebuilding from record 0.
4. The keyed engine is ursula-index generalized in place: covering rows, LWW versions, point and range tombstones, and size-tiered compaction. Ingestion runs on demand, triggered by reads that carry `min_through_record`. Idle harnesses cost zero S3 requests.
5. `GET {stream}/keyed-state` returns exactly one `state(D)` per response. The stream's node serves it as a thin proxy to an internal indexer endpoint, passing the stream's incarnation and tail. There is no gateway pool, no HEAD, and no time travel.
6. JSON writes keep the writer's text: Ursula validates it and strips insignificant whitespace, and never reorders members or rewrites literals. Record-aware reads accept `max_bytes`.
7. Never-trim is paid for in Ursula core rather than worked around, by the general bounded-state workstream (`docs/architecture/bounded-stream-state.md`): per-stream replicated state stays at about 32 KiB plus 8 B per unflushed record plus 16 B per MiB of cold log, with no retention. Its main parts are sparse cold record marks, pack-reference compaction through the existing `CompactCold`, and bounded producer receipts. It also fixes cold-path defects a never-trim stream would hit, among them data made unreadable by a ≥1 MiB append and stale external page entries. Replicated group feature levels gate every replicated-core change, Pi's included.
8. Fencing uses only `Stream-Record-Match`: claim on open (epoch = claim ordinal, plus a random nonce), fixed bytes per attempt, and read-back on ambiguity. Open runs its preflight first and claims last, in a host-chosen mode: `fence` or `fail-if-active`.
9. The owner keeps an overlay of unflushed records `[E, tail)` and a cache of key ranges holding materialized `state(tail)`. Remote pages merge by replaying the overlay over them. Rows keyed by IDs minted in the current session are complete by construction. After warm-up, every read on Pi's Session line is local.
10. Failure policy: transient failures on the Session line retry until a deadline, then poison the storage; the background flush loop retries indefinitely and never poisons on a transient failure. `StorageRejected` is used only for deterministic outcomes where nothing became durable. Once any attempt of a commit is ambiguous, every later outcome for that commit is resolved by reading back record N.
11. Four protocol items need alignment (P1, P2, P3, P7), written as one new `extensions.md` section plus two general amendments. Everything else is internal.
12. Milestones: M0 covers alignment plus four risk spikes. M1 is a full-resident owner passing Pi conformance against a real node. M2 adds the engine and keyed-state, M3 the bounded owner, M4 hardening. The bounded-state workstream (B0–B7) starts now, runs in parallel, and must reach B4 before Pi production.

The decisions needed from you are in §13: eight items, each with a recommendation.

## 2. Goals, non-goals, principles

Goals:

- **G1.** Ursula is Pi Durable's only store: log, query state, and session catalog.
- **G2.** Pi `Storage` semantics are reproduced exactly: 23 conformance cases, run both directly and with close+reopen after every commit, with MemoryStorage error classes and messages.
- **G3.** Session-line latency is one append round trip per commit; steady-state reads are local.
- **G4.** Memory is bounded independently of history length: owner, Raft replicas, and indexer.
- **G5.** An idle harness costs only its storage bytes.
- **G6.** Everything added to Ursula is general: keyed batches, keyed-state, text fidelity, byte-bounded reads, never-trim core.

Non-goals for v1: server-side extraction specs, WASM, or any user code in Ursula; multi-key get; KV-level historical reads; a read-only non-owner Pi reader (deferred); pipelined commits (#87); value separation; authorization finer than bucket; State Protocol wire compatibility; trimming keyed logs.

Principles (binding): no force-fit, and adjust an existing Ursula abstraction before adding a layer. Simplicity and runtime efficiency come first. The design is declarative: records declare their mutations, and the server only folds them.

## 3. Architecture

### 3.1 Components

| Component | Runs in | Responsibility | State |
|---|---|---|---|
| Owner (`UrsulaStorage`, TypeScript) | Pi harness process | Pi `Storage`; plans commits into keyed batches; fencing; LocalStore | overlay + range cache, memory only |
| ursulagw | gateway | JWT, bucket authorization, leader-affine routing; classifies keyed-state waits as `Tail` | none |
| Ursula node | voters | P1 fidelity, P2 validation, P7 reads, keyed-state proxy, Raft log | replicated log and per-stream metadata |
| ursula indexer (keyed mode) | separate pods | on-demand ingest, publish, compaction, GC, verify/rebuild; internal `/v1/keyed` API | S3 `.keyed/` prefix; local disk is cache only |
| S3 | — | log chunks, packs, cold-index pages and external payloads; projection namespaces | |

### 3.2 Diagram

```
                    Pi harness process (owner, TypeScript)
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Session line ─► UrsulaStorage: Pi layer (17 methods + commit planner) │
 │   LocalStore: overlay [E,tail) (pinned) + range cache = state(tail)   │
 └─────────┬──────────────────────────────────────────┬─────────────────┘
  POST {log}  Stream-Record-Match: N       GET {log}/keyed-state?…&min_through_record=E
  GET {log}?record=…&max_bytes=…                      │
           ▼                                          ▼
 ┌───────────── ursulagw: JWT, bucket authz, leader-affine routing ──────────────┐
 └─────────┬─────────────────────────────────────────────────────────────────────┘
           ▼
 ┌───────────── Ursula node (stream leader) ─────────────────────────────────────┐
 │ P1 text fidelity · P2 keyed-batch validation · P7 byte-bounded record reads   │
 │ keyed-state proxy: resolves stream, incarnation c, tail N ──────────┐          │
 │ Raft core: log, sparse record marks, pack compaction, feature level │          │
 └─────────────────────────────────────▲──────────────────────────────┼──────────┘
     GET {log}?record=D−1&max_bytes=…  │ (ingest, rebuild)            │ GET /v1/keyed/{stream}?incarnation=c&source_next=N&…
 ┌─────────────────────────────────────┴──────────────────────────────▼──────────┐
 │ ursula indexer (keyed engine): ingest · publish (CAS) · size-tiered compaction  │
 │ delta GC · continuity check · verify/rebuild (blue/green) · drain(bucket)       │
 └─────────┬─────────────────────────────────────────────────────────────────────┘
           ▼
  S3: {bucket}/{stream}/chunks|cold-index|external/…, {bucket}/_packs/…   (log; nodes)
      .keyed/{bucket}/{key}/{incarnation}/v{fmt}/…                        (projection; indexer)
```

### 3.3 Write path

1. The Session line calls `commit(writes)`. A closed or poisoned storage rejects at once.
2. Plan the commit. Validation reads go through LocalStore (§7.2). Validation and error messages are those of MemoryStorage (pi: `src/storage/memory.ts:250-311, 679-761`). `document.copy` is materialized here. Pre-checks: the encoded record is ≤ 32 MiB, its JSON depth is ≤ 127 (§5.1), and every key is ≤ 4096 octets. Failures raise the MemoryStorage-class `Error`, or `StorageRejected` for copy, size, depth and key-length failures. No write I/O happens before this step completes.
3. Encode one record at `N`, the applied tail. Every Seq-valued field (`createdAt`, `retiredAt`, revision keys, entry `seq`) is `N`, and `"o"` is the owner's epoch.
4. `POST {log}` with `Content-Type: application/json; profile=keyed-batch-v1` and `Stream-Record-Match: N`.
5. Classify the outcome. The classification is stateful across the attempts of one commit (tables below).
6. On success: append the record's ops to the overlay, apply them to covered cache ranges, set `tail := N+1`, advance `nextId` and the persisted `m/next_id` tracker (§7.7), and resolve with `Seq = N`.

Outcome policy:

| Attempt outcome | No earlier attempt of this commit was ambiguous | Some earlier attempt was ambiguous |
|---|---|---|
| 2xx with `Stream-Record-Start = N` | success | success |
| 2xx with another start | poison (invariant violation) | poison |
| 412 | read back N | read back N |
| 400, 413, 422 | `StorageRejected`, plus an alert: a pre-check missed a server limit | read back N |
| 429 without `Retry-After` (node quota; the entry was applied and rejected) | `StorageRejected`, no retry | read back N |
| 404, 409, 410 (stream deleted, closed or mistyped) | poison | read back N, then poison |
| 401, 403 (gateway credentials expired or revoked) | poison with a plain `Error`; the host refreshes credentials and reopens | poison |
| 5xx, 429 with `Retry-After` (gateway rate limit), timeout, connection error | mark the commit ambiguous; back off, honouring `Retry-After`; read back N | same |

Read-back is `GET {log}?record=N&max_records=1&consistency=leader`, plus `max_bytes` when the node advertises `keyed-state-v1` (P7.5):

| Read-back result | Action |
|---|---|
| record N exists and its bytes equal the attempt's bytes B | success |
| record N exists with other bytes | poison with `FencedError` |
| record N absent and tail = N | resend the identical attempt (B, match N) after backoff |
| read failed, or answered by a lagging ex-leader (record N absent, tail < N) | retry the read-back |
| 30 s commit deadline passed | poison |

Why each rule holds:

- Every attempt of a commit sends the same bytes under the same match, so at most one attempt can land at N, and any record at N with those bytes is this commit. `"o"` differs between owners, so another owner's record can never compare equal.
- A 412 on the very first attempt can be self-inflicted. A node returns 307 for a post-proposal `ForwardToLeader` (`crates/ursula/src/lib.rs:4145-4167`), and the gateway follows 307 internally and replays the write (`crates/ursula-gateway/src/lib.rs:454-484`). So 412 always reads back.
- A node 429 is an apply-time quota rejection with no `Retry-After` (`crates/ursula/src/render.rs:116-119`). Retrying would only add more rejected Raft entries.
- Transient failures never become `StorageRejected`. Pi turns `StorageRejected` inside a phase into a durable `faulted` task (pi: `src/harness/scheduler.ts:853-871, 889, 950`), whereas a poisoned storage only forces a reopen (pi: `docs/spec.md:76-78`).
- Reading back before resending means a payload is shipped again only when the previous attempt was lost or is still uncommitted.

Large commits: until the external-staging fix (C6, §6.2) lands, Pi clusters set `external_payload_min_size` above the 32 MiB body cap (`crates/ursula-config/src/config.rs:94`). Every commit then rides Raft inline, and the staging defects are unreachable. An inline append must also fit under the per-group admission caps, `raft.max_uncommitted_size_per_group` and `storage.cold.max_hot_size_per_group` (`crates/ursula-runtime/src/core_worker.rs:290-307`, `crates/ursula-runtime/src/engine/in_memory.rs:657-679`), or it gets 503 on every attempt and poisons at the deadline. Pi clusters therefore run with both caps at least 64 MiB or unset, as in the standard and large presets; the tiny (8 MiB) and small (16 MiB) presets are excluded (`crates/ursula-config/src/preset.rs:61-82`).

### 3.4 Index build path

1. **Trigger.** Any keyed-state `GET` whose `min_through_record = r` exceeds the published `D`: owner flush-waits, open, or any other reader.
2. **Node.** The node validates parameters and resolves the stream from local leader state. It returns 404 if the stream is absent, not keyed, or has no indexer configured, and 400 if `r > N`. It then forwards `GET /v1/keyed/{stream}?incarnation=c&source_next=N&…` to the indexer.
3. **Namespace.** The indexer's namespace is `.keyed/{bucket}/{key}/{c:016x}/v{fmt}/`, where `key` is the stream's local name (`{stream}`, or `{affinity}/{stream}` for the affinity form) percent-encoded as one path component (`%` → `%25`, `/` → `%2F`) and `fmt` is the projection format version. Cold-object paths render the affinity form with a `/` (`crates/ursula-shard/src/lib.rs:91-99`), so a two-segment stream's prefix would contain every affinity stream under the same name; the single component keeps each stream's namespaces disjoint. A missing namespace is `state(0)`. Ingestion is single-flight per namespace and starts no earlier than `CURRENT.published_at_ms + min_publish_interval` (5 s). Waiters coalesce onto the next publish.
4. **Ingest.** Read `[D−1, N)` (from record 0 when `D = 0`) from the node in P7 pages (`record=D−1&max_bytes=16777216`). Check record `D−1` against the manifest's `through_digest` (the continuity check, §5.5). Parse the batches with the shared validator (U3) and fold them in order into one sorted run, applying in-batch range tombstones. Rows deleted within the batch, such as pi.live deltas pruned by a later base, never reach S3.
5. **Publish.** Write the parts, write the manifest (put-if-absent), then CAS `CURRENT`. There is no refresh before publishing. On a CAS conflict, reload and continue from the new `D`. A writer deletes the objects of its own attempts that end unpublished, after the GC grace period, so only a crash leaves orphans. opendal 0.51 `write` returns no ETag (`crates/ursula-index/src/object_store.rs:384-402`), and a bare stat after the CAS could pair this writer's manifest with the ETag of a CAS that another pod or this pod's compaction made in between; the next CAS would then overwrite that update. So the writer reads `CURRENT` back with the existing conditional get (a stat and an `If-Match` read, `object_store.rs:333-357`) and adopts the ETag only if the bytes equal what it wrote; otherwise it treats the publish as a conflict and reloads. Footer, page-index and part caches are filled on write. Each waiter receives exactly one `state(D)`.
6. **After publish.** Immediately after each CAS, the indexer confirms through the node that incarnation `c` still exists (one HEAD per publish). If it does not, the indexer deletes the namespace. This catches a publish that completes after the stream-delete sweep (§3.8), which no later read would ever trigger. Compaction (§6.1 U16) and delta GC are then scheduled.

### 3.5 Owner read path

1. The Pi layer turns the call into key ranges from the family schema (§4.3). Fork-chain walks proceed one segment at a time.
2. A range that is covered (an explicit cached range, or a fresh region under §7.3) is served locally. Any other range is fetched with `GET {log}/keyed-state?start|after=…&end=…&limit=…&min_through_record=E`.
3. Each page is handled synchronously on arrival:
   - If `D_resp < E`, discard it and refetch.
   - If `D_resp > tail`, wait for the in-flight commit. With no commit in flight, poison: another writer exists.
   - Otherwise compute `merged = fold(page rows, overlay records [D_resp, tail))` over the page's covered range. That range is `[lo, Stream-Keyed-After]` when the page was truncated and `[lo, hi)` otherwise. The merged rows replace that range in the cache, which then marks it covered.
4. The result is computed in one synchronous pass over covered ranges, with the ranges in use pinned against eviction. If a needed range turns out to be missing (a chain walk, or residual filters left the page short of `limit+1` matches), go back to step 2.
5. Remote failures follow §7.6: retry, then poison.

### 3.6 Open and takeover

The host chooses the mode: `fence` takes over unconditionally; `fail-if-active` refuses while the current owner shows activity. The activity window is `W`, default 3 s. Every step before the claim is a read, apart from the idempotent create of a new stream, so an open that fails or refuses before the claim never fences anyone.

1. `HEAD {log}`. On 404, create the stream with `PUT {log}` using `Content-Type: application/json; profile=keyed-batch-v1`, no body (idempotent create). The response must advertise `keyed-batch-v1`, and `keyed-state-v1` if keyed-state is required; otherwise open refuses, because an old or ungated node would silently re-normalize records (§6.3). This step yields `N0`.
2. If `N0 > 0`, read `m/` with `GET keyed-state?start=m/&end=strinc(m/)&min_through_record=max(0, N0 − 50000)&timeout_ms=60000`, which yields `D`, `m/format`, `m/owner` and `m/next_id`. A 204 means keyed-state is lagging: retry until the 120 s open deadline, then fail with a retryable `keyed-state lag` error. Open refuses a newer `m/format`.
3. Replay `[D, N0)` from the log in P7 pages into the overlay (`E := D`, `tail := max(N0, D)`), then merge the step 2 page into the cache (§3.5). If replay exceeds 64 MiB, discard it and go back to step 2 with a higher `min_through_record`.
4. Preload in parallel, every request with `min_through_record = E`, merged per §3.5:
   - `t.s/` and `s.s/` (the live set), with derived point rows `t/{id}`, `s/{id}`, `x/{id}` (§7.4);
   - session-scope `d.s/` and `d.a/`;
   - for each live task T: `d.s/{task T}`, `d.a/{task T}` and `c.ot/{T}/`;
   - `c/` for every conversation referenced by live tasks or unsettled submissions, iterated along owner chains until closed;
   - for the root conversation: `c/1`, the newest `e.h/1` marker, and the newest `e/1` page (257 rows).

   Before the claim, any sign of records beyond `N0` (`D > N0` in step 2, or a page with `D_resp` beyond the replayed tail) means the previous owner is still writing. In `fence` mode, replay `[tail, D_resp)` and merge. In `fail-if-active` mode, refuse with `OwnershipActive`. The §3.5 poison rule applies only after the claim.
5. Check the mode. In `fence` mode, continue. In `fail-if-active` mode, continue if `m/owner` is absent or carries `closed_at_ms`. Otherwise long-poll `GET {log}?record=tail&live=long-poll&timeout_ms=W`: if any record arrives, refuse with `OwnershipActive`. Nothing has been written at this point.
6. Claim. Append `["p", m/owner, {epoch, nonce, host, pid, opened_at_ms, mode}]` with `Stream-Record-Match: N`, where N is the tail after replay. At `N0 = 0` the same record also writes `m/format`, making it the genesis.
   - On 412, first compare record N with this attempt's bytes: the gateway's internal replay of a 307 can make the claim's own landed attempt answer 412. If they are equal, the claim succeeded. Otherwise replay the new records `[N, Stream-Record-Next)` into the overlay. A foreign claim among them raises `OwnershipContention`. Any foreign record in `fail-if-active` mode raises `OwnershipActive`. Otherwise retry immediately with `match = Stream-Record-Next`.
   - The claim loop is bounded by a 5 s deadline, not an attempt count. Because each retry uses the 412's continuation, the conflict window is about one `L`. At the deadline, open fails with a retryable `ClaimTimeout`. If an attempt was still ambiguous, its claim may land afterwards and fence the previous owner; the host's retry then sees it as a foreign, unclosed claim and proceeds per its mode.
   - On an ambiguous claim, read back N: our own claim bytes mean success; a foreign claim means contention; a foreign non-claim record in `fence` mode means retry at N+1.
7. On success, `epoch := N_c` (the claim ordinal), `tail := N_c + 1`, `nextId := m/next_id` (2 if absent), and the fresh floor `F_fresh := nextId` (§7.3). Start the flush loop.

A fenced owner learns of the takeover on its next commit (412, then read-back shows a foreign record, then `FencedError`) or on its next flush-wait, whose `m/owner` row carries a foreign nonce. `FencedError` is terminal for the process: a host in `fence` mode must not auto-reopen, which prevents ping-pong. A host in `fail-if-active` mode may reopen, and that reopen refuses while the new owner is active.

### 3.7 Close

`close()` stops accepting new operations and waits for the in-flight commit. It then appends a close marker: `m/owner` with `closed_at_ms`, a 200 B record that lets a later `fail-if-active` open skip the wait. Next it issues a keyed-state request with `min_through_record = tail` and `timeout_ms=1` (finalize-on-close: it triggers ingestion without waiting for it), cancels the flush loop, and rejects in-flight multi-request reads. Every later call rejects with a message containing `closed`, as conformance case 23 requires. If the close marker cannot be appended, `close()` still succeeds; the next `fail-if-active` open then pays `W` once.

### 3.8 Lifecycle and catalog

- **Naming.** `/{bucket}/{harness_id}` (two-segment form). This replaces the brief's affinity key per harness: an affinity key only co-locates several streams in one Raft group (`extensions.md` §1.7), and a harness is one stream, so the key would add a path segment and buy nothing. `harness_id` is chosen by the host, unique, and never reused. The recommended shape is `{scope}-{rev_ms13}-{ulid}`: `scope` is a short hash of the host's grouping key (for example the cwd), and `rev_ms13 = 9999999999999 − created_ms`, so a prefix listing returns one scope newest-first. The ID fits the 122-byte limit (`extensions.md` §1.1).
- **Catalog.** Implement the specified but unimplemented bucket listing, `GET /{bucket}/streams?prefix=…&after=…&limit=…` (`extensions.md` §1.4; there is no route today, `crates/ursula/src/lib.rs:1323-1400`). Session metadata lives in stream attrs (`title`, `metadata`, at most 16 KiB): `{"title":…,"metadata":{"pi_durable":1,"cwd":…,"created_at_ms":…}}`. The owner writes attrs at create time and when the title changes.
- **Abandonment.** Hosts may set a sliding `Stream-TTL` at create (`crates/ursula-stream/src/state_machine.rs:734-750`). Appends and log reads renew it (`crates/ursula-runtime/src/engine/in_memory.rs:952`), including the one ingestion that catches the projection up after the last append. Keyed-state requests themselves do not, so reads of an abandoned, caught-up harness never renew it. The default is no TTL, because sessions are user data.
- **Delete.** `DELETE {stream}` enqueues the stream's cold GC sweep (`crates/ursula-runtime/src/runtime.rs:1016-1019`), which the bounded-state workstream scopes to the deleted incarnation and extends to its external payloads (C7, F14a). For a keyed stream, delete apply also enqueues the deleted incarnation's namespace prefix `.keyed/{bucket}/{key}/{c:016x}/` as a GC path (U22). A recreated stream's new incarnation is never swept.
- **Purge.** `DELETE /__ursula/purge/{bucket}` keeps its existing tombstone step. It then sends `drain(bucket)` to every indexer pod. Each pod blocks new work for the bucket, waits for its in-flight operations and acknowledges; the purge proceeds only when every pod has acknowledged. After the tombstone, nodes answer 404 for the bucket's streams, so no new work arrives. Finally the purge erases both `{bucket}/` and `.keyed/{bucket}/` and proves both empty (U23, extending `crates/ursula-runtime/src/runtime.rs:602-633`). `.keyed/` is a top-level prefix: a bucket ID cannot contain `.`, so no bucket's erasure domain can collide with it, and lifecycle and IAM rules can target it by prefix.
- **Disaster recovery.** The projection is a cache. Restore the log with the standard Ursula DR procedure (RPO = export time). Namespaces that are now ahead of, or divergent from, their restored source fail the continuity check and rebuild (§5.5); keyed-state answers 503 for them until the rebuild catches up. Owners poison at the commit deadline and reopen. `.keyed/` does not need to be in backups.
- **Usage.** No system process advances retention, so log bytes stay in the bucket's retained-bytes gauge and quota. Projection live bytes are exported per bucket as a separate usage metric.

## 4. Record format and Pi key schema

### 4.1 `keyed-batch-v1` grammar and fold

```
message = JSON object with exactly one member whose (unescaped) name is "ops";
          other members: any JSON value, ignored by the fold, stored verbatim;
          the message's own member names MUST NOT contain unpaired-surrogate
          escapes (names inside values are unrestricted)
ops     = JSON array of zero or more op
op      = ["p", key, value]      ; put
        / ["d", key]             ; point delete
        / ["x", key, key]        ; range delete [start, end), start < end in unsigned octet order
key     = JSON string whose characters are exactly the canonical unpadded base64url encoding
          (RFC 4648 §5; no "=", zero pad bits, no backslash escapes) of 1..4096 octets
value   = any JSON value; a put of the literal null makes the key visible with value null
```

`state(D)` applies records `0 … D−1` in order, and the ops within a record in array order. A put sets `k → (r, value)`. A point delete removes `k`. A range delete removes every `k` with `start ≤ k < end`. A key is visible in `state(D)` when its last applicable op is a put. Its `record` is the ordinal of that put. The order of ops is `(r, j)`. Because a record is never split across runs, the engine only needs `r` across runs: a range tombstone of record `r` deletes rows of records `< r`. The server never derives a key that no op declares.

### 4.2 Tuple layer (owner side; informative appendix of the spec)

| Component | Encoding | Order |
|---|---|---|
| tag | 1 octet family tag (§4.3) | — |
| `u64(x)` | 8 octets, big-endian | ascending |
| `u64desc(x)` | 8 octets, big-endian, of `2^64−1−x` | descending |
| `u8(e)` | 1 octet enum | — |
| `str(s)`, short | WTF-8 of the JS string: valid surrogate pairs become 4-octet UTF-8, lone surrogates become `ED A0..BF xx`. `TextEncoder` must not be used because it replaces lone surrogates with U+FFFD. Each `0x00` is escaped as `00 FF`, and a `0x00` terminator follows. Used when the result is ≤ 1024 octets. | groups by string |
| `str(s)`, long | `FE ‖ SHA-256(WTF-8(s))`: 33 octets | exact match only |
| `strinc(p)` | strip trailing `FF` octets, then increment the last octet | prefix successor |

Short and long forms never collide: a short form never starts with `FE`, because its first octet is `00` or a WTF-8 lead octet ≤ `F4`. The code is not prefix-free, though: a short form is a prefix of its NUL-extensions (`str("k")` = `6B 00` starts `str("k\u0000z")` = `6B 00 FF 7A 00`), so a range `[p, strinc(p))` that ends in a `str()` component also returns rows of every string that extends the target with U+0000, and in `d.a` those rows sort before the target's. Long forms occur only in exact-match positions: task kind, document kind and key, and requestId. Each family that can contain either form also carries the full string, either in its value or in the primary row it points to. Every scan over a `t.k` or `d.a` prefix therefore compares the full kind (and, in `d.a`, the key) of each row and skips mismatches, and an exact `s.r` lookup whose stored `requestId` differs is a miss. SHA-256 collisions are treated as impossible. The longest Pi key is a `d.a` key, `27 + 2·1024 = 2075` octets, well under the 4096-octet server cap. Encoded by the executable model:

- `t.k/"\ud800"/5` = `M-2ggAAAAAAAAAAABQ` and `t.k/"\ud801"/5` = `M-2ggQAAAAAAAAAABQ`, so lone surrogates stay distinct (case 21).
- `s.r/1/<2000×"R">` = `QgAAAAAAAAAB_qlzUi6syczOrf_AEPEPP8d5ztBpK79Sfr0W73LnIc4y`, the long form.

### 4.3 Key families

In the table, † marks ID-typed components, which complete-at-mint uses (§7.3). Scope codes are session 1 (owner 0), conversation 2 (owner = conversation ID) and task 3 (owner = task ID). `fam` is 0 for a singleton and 1 for a family member, so a singleton stays distinct from family key `""`.

| Family (tag) | Key | Value | Mutability | Serves |
|---|---|---|---|---|
| `m` 0x01 | `str(name)`, name ∈ {`next_id`, `owner`, `format`} | number / claim / format object | LWW | mintId recovery, fencing, format check |
| `x` 0x02 | `u64(id†)` | `{"t":"c"\|"e"\|"t"\|"s"\|"d"}`; entries add `"c":conv` | write-once | global ID ownership; `entry(id)` |
| `c` 0x10 | `u64(id†)` | ConversationRecord | write-once | `conversation`, `scanConversations({})`, fork chains |
| `c.oc` 0x11 | `u64(ownerConv†) u64(id†)` | ConversationRecord (covering) | write-once | `scanConversations({ownerConversationId})` |
| `c.ot` 0x12 | `u64(ownerTask†) u64(id†)` | ConversationRecord (covering) | write-once | `scanConversations({ownerTaskId…})`, task graph |
| `e` 0x20 | `u64(conv†) u64desc(id†)` | `{"seq":commitSeq,"entry":EntryRecord}` | write-once | `scanEntries`, both `entry` overloads |
| `e.h` 0x21 | same as `e` | same bytes as the `e/` row; only entries with `head` | write-once | `findLatestHeadMarker` |
| `t` 0x30 | `u64(id†)` | TaskRecord | replace (LWW) | `task`, terminal and unfiltered scans |
| `t.s` 0x31 | `u8(status) u64(id†)`: pending 1, running 2, waiting 3, completing 4 | TaskRecord (covering) | delete + put on status change; no row when terminal | live scans, open preload |
| `t.c` 0x32 | `u64(conv†) u64(id†)` | `null` | put at create; moved on conversation change | `scanTasks({conversationId})` |
| `t.k` 0x33 | `str(kind) u64(id†)` | `null` | put at create; moved on kind change | `scanTasks({kind})` |
| `s` 0x40 | `u64(id†)` | SubmissionRecord | replace | `submission`, settled and unfiltered scans |
| `s.s` 0x41 | `u8(status) u64(id†)`: queued 1, placed 2 | SubmissionRecord (covering) | as `t.s`; no row when settled | unsettled scans, open preload |
| `s.r` 0x42 | `u64(conv†) str(requestId)` | `{"id":n}`, plus `"requestId"` when the key is digested | LWW; deleted only while it still points at this ID (pi: `memory.ts:377-392`) | `submissionByRequest` |
| `s.c` 0x43 | `u64(conv†) u64(id†)` | `null` | as `t.c` | `scanSubmissions({conversationId})` |
| `d` 0x50 | `u64(id†)` | `{"record":DocumentRecord,"version":n}` | rewritten on version change and retire | `document`, commit validation |
| `d.a` 0x51 | `u8(scope) u64(owner†) str(kind) u8(fam) str(key) u64desc(createdAt) u64(id†)` | DocumentRecord (covering) | create; rewritten on retire | `findDocument`, address occupancy |
| `d.s` 0x52 | `u8(scope) u64(owner†) u64(id†)` | DocumentRecord (covering) | create; rewritten on retire | `scanDocuments` |
| `d.b` 0x53 | `u64(id†) u64desc(seq)` | `{"version":n,"value":V}` | write-once; range-deleted for current-only documents | newest base ≤ `at` in one seek |
| `d.r` 0x54 | `u64(id†) u64(seq)` | `{"version":n,"ops":[…]}` | write-once; range-deleted for current-only documents | deltas after the base |

Design notes:

- **Explicit commitSeq.** `commitSeq` is a field of the `e/` value, not the row's physical version. A future writer-side migration may then rewrite `e/` rows without changing the `asOf` reads that use commitSeq (pi: `src/session/forks.ts:26-97`). `record` in keyed-state rows (§5.3) remains informational.
- **Live statuses only.** Status families hold only live statuses. Terminal tasks and settled submissions are scanned through `t/` and `s/`, so the bulk of records never gets a second copy.
- **Key-only families stay.** `t.c`, `t.k` and `s.c` hold keys only. Pi's spec makes these filters part of the public task query surface for extension code (pi: `docs/spec.md:4344-4346`, `src/types.ts:625-631`). Without them, such scans cost O(all tasks) remote reads in a long-lived harness. They cost about 40–60 B of log per created task or submission, and nothing else.
- **No multi-put.** Covering rows are separate puts. This adds about 2–5% to byte-weighted log volume and leaves projection bytes unchanged, and it keeps the grammar to three op shapes.

### 4.4 `StorageWrite` → ops (`Seq = N`)

Ops are emitted in MemoryStorage's application order (pi: `memory.ts:313-406` applies table writes, then calls `applyDocumentActions`, `memory.ts:763-805`). Table writes come first, in batch order, so a second write of the same ID wins. Document actions follow, per incarnation: content first, then retire. `m/next_id` comes last.

- **conversation:** `p c/id R`; `p c.oc/oc/id R` and `p c.ot/ot/id R` when owned; `p x/id {"t":"c"}`.
- **entry:** `p e/conv/~id {"seq":N,"entry":R}`; the same value at `e.h/conv/~id` when `head` is set; `p x/id {"t":"e","c":conv}`.
- **task:** `prev` is the earlier write of the same ID in this batch, or else the committed record (local by §7.3–§7.4, otherwise remote).
  - Always `p t/id R`, and `p t.s/st/id R` if `st` is live.
  - `d t.s/prev.st/id` when `prev` was live and its status changed or became terminal.
  - For a new ID: `p x/id {"t":"t"}`, `p t.c/conv/id null`, `p t.k/kind/id null`.
  - On a conversation or kind change: `d` the old key, then `p` the new one.
- **submission:** symmetric to task, where the live statuses are queued and placed. For requestId: if `prev` had one and `s.r/prevConv/prevReq` still holds this ID, `d` it. If `R` has one, `p s.r/conv/req {"id":id}`, adding `"requestId"` when the key is digested.
- **document.create:** copies are already materialized at this point.
  - `p d/id {"record":R',"version":v}`, where `R'` has `createdAt: N`, plus `retiredAt: N` when retired in the same batch.
  - `p d.a/… R'`, `p d.s/… R'`, `p d.b/id/~N {"version":v,"value":V}`, `p x/id {"t":"d"}`.
  - For a current-only document retired in the same batch, also `x [d.b/id/, strinc)` and `x [d.r/id/, strinc)`.
- **document.change, base:** `p d.b/id/~N …`, plus `p d/id …` when the version changes. For a current-only document, also `x [d.b/id/~(N−1), strinc(d.b/id/))` and `x [d.r/id/0, d.r/id/N)`, which together equal MemoryStorage's `revisions = [new base]`.
- **document.change, delta:** `p d.r/id/N {"version":v,"ops":[…]}`.
- **document.retire:** `p d/id {record + retiredAt:N, …}`, `p d.a/… R''`, `p d.s/… R''`. A current-only document also gets `x [d.b/id/, strinc)` and `x [d.r/id/, strinc)`.
- **`m/next_id`:** a put when `max(nextId, written IDs + 1)` exceeds the persisted tracker (§7.7).

This mapping was checked with an executable model that implements §4.2–§4.5 together with MemoryStorage-ported validation:

- Pi's own conformance suite (`storage-conformance.ts`) passes 23/23 directly and 23/23 with close+reopen after every commit, failed commits included. Every reopen appends a claim, so Seqs have gaps.
- Differential fuzzing against MemoryStorage found 0 divergences: 450 table trials and 240 document trials, with random reopen. The trials cover in-batch duplicate IDs, terminal rewrites, conversation and kind moves, requestId remaps with lone-surrogate, NUL and >1 KiB strings, singleton vs key `""` vs `"k"`, kinds and keys next to their NUL-extensions (`"k"` and `"k\u0000z"`, `""` and `"\u0000"`), current-only vs rewindable documents, historical copies, and >1 KiB document kinds and keys on the digest path.
- Mutation tests confirm that the fuzzers catch dropped status deletes, collapsed `fam` bytes, unconditional `s.r` deletes, and scans that trust a `t.k` or `d.a` prefix instead of comparing the full kind and key.

The model becomes the seed of the owner model tests (§11.2).

### 4.5 Read plans and steady-state locality

| Method | Keys and ranges | Steady state |
|---|---|---|
| `commit` validation | `x/{id}`, `c/{conv}`, `t/{id}`, `s/{id}`, `d/{id}`, `d.a` address prefix; for copies, `d/`, `d.b`, `d.r` at the source point | local: fresh IDs (§7.3), live-set preload (§7.4), previously touched prefixes; the first touch of an old scope's address, and copy sources, may be remote |
| `mintId` | none | local |
| `conversation` | `c/{id}` | fresh, preloaded, or cached after first read |
| `scanConversations` | `c/`, `c.oc/{oc}/` or `c.ot/{ot}/`; scan in key order until `limit+1` matches or the range ends | `c.ot` of live tasks preloaded; others first-touch |
| `entry(id)` | `x/{id}`, then `e/{c}/{~id}` | fresh or cached |
| `entry(conv, id)` | walk the `c/` chain with a cumulative cap, then `x/{id}` and `e/` | cached |
| `findLatestHeadMarker` | per ancestor, the first row of `e.h/{C}/` at or after `~min(cutoff, cap)` | root marker preloaded; fresh; cached |
| `scanEntries` | per chain segment, `e/{C}/[~hi, ~lo]` until `limit+1` | fresh entries local; active range cached after first scan |
| `task`, `scanTasks` | live status: `t.s/{st}/`; otherwise by priority `t.c/{conv}/` (keys, then `t/` points), `t.k/{kind}/`, `t/`; residual filters; scan until `limit+1` matches | live set preloaded; terminal tasks remote (rare) |
| `submission`, `scanSubmissions`, `submissionByRequest` | `s/{id}`; `s.s/{st}/`; `s.c/{conv}/` or `s/`; `s.r/{conv}/{req}` then `s/{id}`. The first requestId lookup in a conversation fetches the whole `s.r/{conv}/` prefix (about 70 B per requestId) | unsettled preloaded; fresh; requestId lookups local after the first per conversation |
| `findDocument` | `d.a/{scope}/{kind}/{fam}/{key}/` from `~at`: skip rows whose full kind or key differs (§4.2) and empty lifetimes; return the first incarnation alive at `at`; stop at the first non-empty dead one | fresh scope, preloaded session and task scopes, or cached |
| `document` | `d/{id}`; the first `d.b/{id}/` row at or after `~at`; `d.r/{id}/(base, at]`; Chord applied in TypeScript | Session caches trackers after the first load |
| `scanDocuments` | `d.s/{scope}/` with alive and kind filters | fresh or preloaded |

A scan with residual filters continues past filtered rows until `limit+1` matches, as MemoryStorage does (pi: `memory.ts:438-445`). Merged output is final only up to the smallest remote frontier among open cursors. M3 gates on zero remote reads on the line for a steady-state text turn and a steady-state tool turn (§10).

### 4.6 Worked examples

An admission commit at `N = 103`, epoch 17. It writes entry 127, submission 128 (placed, requestId `req-1`), pending generation task 129, a new base for current-only document 4 (pi.live), and `m/next_id = 130`. The record is 1,222 B, against about 590 B for the same commit in JSONL (*measured*).

```json
{"o":17,"ops":[["p","IAAAAAAAAAAB_________4A",{"seq":103,"entry":{"id":127,"conversationId":1,"kind":"pi.user","model":[{"role":"user","content":"hi"}]}}],
 ["p","AgAAAAAAAAB_",{"t":"e","c":1}],
 ["p","QAAAAAAAAACA",{"id":128,"conversationId":1,"requestId":"req-1","type":"input","status":"placed","entry":127}],
 ["p","QQIAAAAAAAAAgA",{"id":128,"conversationId":1,"requestId":"req-1","type":"input","status":"placed","entry":127}],
 ["p","AgAAAAAAAACA",{"t":"s"}],
 ["p","QwAAAAAAAAABAAAAAAAAAIA",null],
 ["p","QgAAAAAAAAABcmVxLTEA",{"id":128}],
 ["p","MAAAAAAAAACB",{"id":129,"conversationId":1,"kind":"pi.generation","version":1,"input":{},"background":false,"abortRequested":false,"state":{"status":"pending","checkpoint":{}}}],
 ["p","MQEAAAAAAAAAgQ",{"id":129,"conversationId":1,"kind":"pi.generation","version":1,"input":{},"background":false,"abortRequested":false,"state":{"status":"pending","checkpoint":{}}}],
 ["p","AgAAAAAAAACB",{"t":"t"}],
 ["p","MgAAAAAAAAABAAAAAAAAAIE",null],
 ["p","M3BpLmdlbmVyYXRpb24AAAAAAAAAAIE",null],
 ["p","UwAAAAAAAAAE_________5g",{"version":1,"value":{"generation":null}}],
 ["x","UwAAAAAAAAAE_________5k","UwAAAAAAAAAF"],
 ["x","VAAAAAAAAAAEAAAAAAAAAAA","VAAAAAAAAAAEAAAAAAAAAGc"],
 ["p","AW5leHRfaWQA",130]]}
```

| Key (base64url) | Meaning |
|---|---|
| `IAAAAAAAAAAB_________4A` | `e/1/~127` |
| `AgAAAAAAAAB_`, `AgAAAAAAAACA`, `AgAAAAAAAACB` | `x/127`, `x/128`, `x/129` |
| `QAAAAAAAAACA`, `QQIAAAAAAAAAgA` | `s/128`, `s.s/placed/128` |
| `QwAAAAAAAAABAAAAAAAAAIA`, `QgAAAAAAAAABcmVxLTEA` | `s.c/1/128`, `s.r/1/"req-1"` |
| `MAAAAAAAAACB`, `MQEAAAAAAAAAgQ` | `t/129`, `t.s/pending/129` |
| `MgAAAAAAAAABAAAAAAAAAIE`, `M3BpLmdlbmVyYXRpb24AAAAAAAAAAIE` | `t.c/1/129`, `t.k/"pi.generation"/129` |
| `UwAAAAAAAAAE_________5g` | `d.b/4/~103`, the new base |
| `["x","…_________5k","UwAAAAAAAAAF"]` | `[d.b/4/~102, strinc(d.b/4/))`: all older bases |
| `["x","VAAA…AAAA","VAAA…AGc"]` | `[d.r/4/0, d.r/4/103)`: all older deltas |
| `AW5leHRfaWQA` | `m/"next_id"` |

A streaming partial commit is one op, 127 B, about 50 B more than the raw delta. One is written every 100 ms per streaming conversation:

```json
{"o":17,"ops":[["p","VAAAAAAAAAAEAAAAAAAAAGg",{"version":1,"ops":[["a",["live","generation","message","text"],"hello wor"]]}]]}
```

Genesis (record 0), a takeover claim (record 42), and a close marker:

```json
{"o":0,"ops":[["p","AWZvcm1hdAA",{"pi_durable_keyed":1,"tuple":1}],["p","AW93bmVyAA",{"epoch":0,"nonce":"9f2c4e1a7b3d5f60a1b2c3d4e5f60718","host":"worker-7","pid":4711,"opened_at_ms":1790000000000,"mode":"fence"}]]}
{"o":42,"ops":[["p","AW93bmVyAA",{"epoch":42,"nonce":"c0ffee00112233445566778899aabbcc","host":"worker-9","pid":812,"opened_at_ms":1790000360000,"mode":"fail-if-active"}]]}
{"o":42,"ops":[["p","AW93bmVyAA",{"epoch":42,"nonce":"c0ffee00112233445566778899aabbcc","host":"worker-9","pid":812,"opened_at_ms":1790000360000,"mode":"fail-if-active","closed_at_ms":1790000720000}]]}
```

There are no other control records. A 33 KB tool settle is almost the same size as in JSONL. Byte-weighted over a session, the log is about 1.2–1.3× JSONL.

### 4.7 Seq, epochs and IDs

- The first record is always genesis at ordinal 0, so Pi's first Seq is 1 (`extensions.md` §6.1).
- Claims and close markers leave gaps in the Seq sequence. Pi allows gaps, and conformance compares Seqs only relatively (pi: `storage-conformance.ts:161, 1449`).
- Rejected appends consume no ordinal. Ordinals are never renumbered, and nothing is trimmed.
- IDs share one global namespace across tables. `x/{id}` enforces cross-table uniqueness with MemoryStorage's exact messages.

## 5. Protocol changes to align

Placement:

- P2 and P3 go into a new `extensions.md` section, "9. Keyed Streams": 9.1 record format, 9.2 the keyed-state resource, 9.3 correctness requirements, and 9.4 an informative tuple-layer appendix.
- P1 becomes a new general section, "JSON Message Text", which also amends the wording of `extensions.md` §6.8.
- P7 amends `extensions.md` §6.6.
- `durable-stream.md` stays a verbatim upstream mirror.

Error bodies stay plain text, with machine state in `Stream-*` headers, until upstream #110 lands.

### 5.1 P1 — JSON Message Text

- **Name and token.** No token: this is a storage rule that every server applies to every `application/json` stream. It defines the base protocol's "normalized message", which is undefined today.
- **Surface.** All JSON writes: the `PUT` create body, `POST`, each `append-batch` frame, and each `$transaction` JSON op. All read representations: default NDJSON, envelope, SSE, and bootstrap.
- **Normative text:**
  1. The server MUST validate each request body as RFC 8259 JSON text. `\uXXXX` escapes that encode unpaired surrogates are valid.
  2. After top-level array flattening, the server MUST store each message as the original text with insignificant whitespace removed (U+0020, U+0009, U+000A and U+000D outside strings), followed by one LF. It MUST NOT reorder object members, drop duplicate members, or rewrite number or string literal text.
  3. The nesting depth of a scalar is 0. The depth of an array or object is 1 plus the maximum depth of its elements, and an empty one has depth 1. A message deeper than 127 MUST be rejected with 400. Depth is measured per message, after flattening.
  4. A body with invalid JSON or invalid UTF-8 MUST be rejected with 400, and no record is committed.
  5. Every read representation MUST carry the stored message text unchanged. The envelope `value` member is the stored text.
  6. Offsets, ETags and record coordinates refer to the stored text.
- **Compatibility.** Only new writes are affected, and stored data is not rewritten. Observable changes:
  - members keep the writer's order (they were sorted);
  - number text is kept (`1.50e3` stays; `1e400` is accepted verbatim, where it was a 400);
  - duplicate members are kept;
  - lone-surrogate escapes are accepted (they were a 400);
  - bare messages keep today's depth limit of 127 (`serde_json` accepts 127 and rejects 128, *measured*); messages in an array body gain one level.

  In-tree consumers that parse records into `serde_json::Value` must tolerate lone surrogates; the only one is the event-time indexer, fixed in M1 (U11). The M0b gate is the official Durable Streams conformance suite at 300/300.
- **Alternatives.** A per-stream opt-in means two code paths, and no caller relies on sorting. `preserve_order` still rewrites numbers and rejects lone surrogates. Double-encoding Pi payloads is a force-fit. Self-framing as octet streams loses record coordinates and `Stream-Record-Match`.
- **Recommendation.** Default for every JSON stream (§13 Q2). Propose it upstream as "servers MUST NOT reorder members or rewrite literal text, and MAY remove insignificant whitespace".

### 5.2 P2 — `keyed-batch-v1` record format

- **Name and token.** `keyed-batch-v1`, activated by the stream's immutable content type. The activation form is a §13 Q3 decision:
  - (a) `application/json; profile=keyed-batch-v1` (recommended). Generic clients keep JSON mode, and `json-record-coordinates-v1` stays active with no code change, because its check ignores parameters (`crates/ursula-stream/src/record_index.rs:39-44`). The cost is a deviation from RFC 6838 §4.3 and RFC 6906, which this text records: `application/json` defines no parameters, and profile values are URIs.
  - (b) `application/vnd.ursula.keyed-batch+json`, added to the JSON-mode and record-coordinate content-type checks. It is standards-clean, but generic clients that key JSON mode on `application/json` lose it.
- **Surface.** Every write path (as in P1); advertisement on `HEAD`, create, append and record reads; status 422.
- **Normative text:**
  1. A stream is a keyed stream if and only if its normalized content type equals the activation type exactly. Normalization lowercases the type and its parameters and joins parameters with `; ` (`crates/ursula/src/lib.rs:3979-3987`). A quoted `profile="keyed-batch-v1"` does not activate the extension. Keyed streams also implement `json-record-coordinates-v1`.
  2. Every create body, append, append-batch frame and `$transaction` op that targets a keyed stream MUST carry a content type that normalizes to exactly the stream's content type, or the server MUST respond 409. This existing rule (`crates/ursula-stream/src/state_machine/append.rs:329, 513, 668`) becomes normative for keyed streams.
  3. A server that advertises `keyed-batch-v1` MUST implement JSON Message Text (P1).
  4. After P1 flattening, each message MUST match the grammar in §4.1. Op codes and member names are compared after JSON unescaping. Keys are checked on raw characters, so escapes are invalid.
  5. The server MUST validate every message of a request before committing any of them. On any failure it MUST respond 422 and commit nothing. The plain-text body SHOULD read `invalid keyed batch at message <i>: <reason>`, where `<i>` is the zero-based message index after P1 flattening (`frame <f> message <i>` for `append-batch`). For `append-batch` this fails the whole request rather than one frame (`extensions.md` §4.3), as JSON normalization errors already do (`crates/ursula/src/lib.rs:2638-2646`). JSON syntax errors remain 400 under P1. Validation runs in the HTTP layer before apply, so precedence is 400 (JSON), then 422 (grammar), then the apply-time 404, 409 and 412.
  6. Fold semantics are as in §4.1. The server MUST NOT derive keys that no op declares. Members other than `ops` MUST be stored verbatim and ignored by the fold.
  7. Responses to `HEAD`, create, append and record reads of a keyed stream MUST advertise `keyed-batch-v1`. Responses for other streams MUST NOT.
  8. Limits: keys ≤ 4096 octets; message depth per P1; request body per the server limit (32 MiB, 413). There is no limit on the number of ops.
  9. Snapshots, retention and `/bootstrap` keep their generic semantics on keyed streams. Retention, however, removes what keyed state is built from. Ingestion re-reads record `max(D−1, 0)` for the continuity check, so a namespace stops advancing once the first retained ordinal `F` (`extensions.md` §6.1) passes that record, and a namespace that needs a rebuild from record 0 (continuity failure, new format) can no longer be rebuilt. Keyed-state then responds 500 for any `r > D` (P3.9). Pi never advances retention. `/bootstrap` returns the batch log, not keyed state, so clients SHOULD use keyed-state for state.
- **Compatibility.** Purely additive. Existing streams are unaffected, and clients unaware of the extension can still read keyed logs as JSON.
- **Format evolution.** Grammar changes ship as a new profile token (`keyed-batch-v2`) for new streams. Servers keep validating and folding v1 indefinitely, because logs are permanent and the v1 fold is about 200 lines. Pi-level schema changes go through `m/format`, which §7.1 describes.
- **Alternatives.**
  - A server-side extraction spec (§12).
  - A `Stream-Record-Format` create header, which changes `StreamMetadata`, the codec and the state machine.
  - Declaring the format in attrs, a force-fit.
  - Typed tuple keys decoded in Rust.
  - State Protocol messages, one change per record, which multiply record count and break "one commit = one Seq".
  - Multi-put: a third op shape in every applier for 2–5% of log bytes.
  - Status 400 for batch errors, which would not distinguish invalid JSON from a JSON value that is not a batch.
- **Recommendation.** Adopt with (a) and 422. 422 matches upstream #281 and the indexer's existing use of it.

### 5.3 P3 — `keyed-state-v1` read resource

- **Name and token.** `keyed-state-v1`. The resource is `{stream_url}/keyed-state` for both the two-segment and the affinity form. `keyed-state` joins the reserved affinity stream IDs (`extensions.md` §1.7, `crates/ursula-shard/src/lib.rs:17-22`).
- **Surface:**

```
GET {stream_url}/keyed-state?[key=k | [start=k | after=k][&end=k][&limit=n]][&min_through_record=r][&timeout_ms=t]
```

- **Normative text:**
  1. Only `GET` is defined. Every other method, `HEAD` included, MUST be answered 405 with `Allow: GET`.
  2. `k` is the canonical unpadded base64url encoding of 1..4096 octets. The range is `[lo, hi)`. `lo` is `start` (inclusive) or `after` (exclusive); the two are mutually exclusive, and the default is the first key. `hi` is `end` (exclusive), defaulting to past the last key. If `lo ≥ hi`, nothing is selected. `key=k` is a point read and MUST NOT be combined with `start`, `after`, `end` or `limit`. `limit` is 1..1000, default 100. `timeout_ms` follows the house long-poll rule: default 1000, an unparseable value means the default, and values are clamped to 1..60000 (`crates/ursula/src/lib.rs:3906-3911`). It is ignored without `min_through_record`. A repeated parameter MUST be rejected with 400.
  3. On 200, the body is `application/vnd.durable-stream-keyed-rows+ndjson`: one line per visible key in range, in ascending unsigned octet order, each `{"key":"<k>","record":<r>,"value":<v>}` with members in exactly that order. `record` is the ordinal of the record containing the last put applied to the key in `state(D)`; it is a function of `state(D)`, so compaction and rebuild never change it. `value` is the value's text as stored under P1.
  4. 200 and 204 responses MUST carry `Stream-Keyed-Through: D` and `Cache-Control: no-store`. `D` is exclusive: the response reflects every record below `D` and none at or above it.
  5. `Stream-Keyed-After: k` names the last returned key. It MUST be present when the server stopped before the end of the range because of `limit` or the response budget, and MAY be present otherwise. Its absence means the range is exhausted. The response budget is 4 MiB of uncompressed body. The server MUST NOT add a row that would exceed it, except that the first row is always returned whole.
  6. Every 200 and 204 MUST reflect exactly one `state(D)`, with `D` at most the source tail `N`. For one stream incarnation, once a response has carried `Stream-Keyed-Through: D`, no later 200 or 204 carries a smaller `D`, unless the source log itself was rewound by a restore; `D` then restarts from the rebuilt state.
  7. With `min_through_record=r`, the server MUST respond with `D ≥ r`, waiting up to `timeout_ms`. If `D < r` when the timeout elapses, it MUST respond 204 with `Stream-Keyed-Through: D`. Without `r`, any published `D` may be returned.
  8. A keyed stream with no keyed state yet is served as `state(0)`.
  9. Errors:
     - 400 for invalid parameters;
     - 400 with `Stream-Record-Next: N` when `r` exceeds the source tail `N`;
     - 404 when the stream is absent, not keyed, or not served;
     - 500 with a plain-text reason naming the record ordinal when a permanent condition prevents `state(D)` for any `D ≥ r`: a source record that cannot be applied, an irreparable continuity break, or source records below the first retained ordinal (P2.9);
     - 503 with `Retry-After` when the resource is temporarily unavailable, including transient source read failures and a namespace ahead of its source (`D > N`) while it is re-validated or rebuilt.

     Precedence is parameter 400, then 404, then the beyond-tail 400, then 5xx, then 204. A 503 MAY precede 404 when the server cannot determine whether the stream exists.
  10. Every response of this resource for a keyed stream MUST advertise `keyed-state-v1`, except 404. Responses to the create `PUT` and to `HEAD {stream_url}` of a keyed stream MUST advertise `keyed-state-v1` when the resource is served.
  11. A server MAY coalesce concurrent ingestion of one stream and space publications by a minimum interval. Correctness MUST NOT depend on either.
- **Compatibility.** This is a new resource. An existing affinity stream literally named `keyed-state` becomes unaddressable; none is expected, and the apply-time reservation is gated (§6.3). For keyed streams, this resource supersedes Option D of the record-coordinates design (`docs/architecture/json-record-coordinates-validation.md:41-58`). Pi's longest keys yield URLs under 6 KB, but a generic client using two 4096-octet keys produces request targets of about 11 KB; deployments SHOULD allow request lines of at least 12 KiB on this resource.
- **Alternatives.**
  - Serving keyed-state from the indexer behind a gateway HRW pool. Its incarnation checks would cost a source HEAD per read, and failover on 503 breaks single-flight.
  - `HEAD`, `prefix=`, `op_index`, and status headers.
  - 409 for blocked or mismatched incarnations, and 410.
  - Generation pinning, horizons and as-of reads.
  - A server-side tail overlay.
  - A JSON body `{rows, next}`, from which a TypeScript client cannot slice raw values; `JSON.parse` reorders integer-like keys.
  - Multi-get (deferred).
  - Naming the resource `$projection` (reserved for affinity operations) or `__ds` (upstream's control namespace).
- **Recommendation.** Adopt as written, GET-only, served through the stream's node.

### 5.4 P7 — Byte-bounded record-aware reads

- **Name.** No new token. This amends `json-record-coordinates-v1` §6.6 by deleting both sentences of its rule (`extensions.md:667`; `crates/ursula/src/lib.rs:3101-3107`): "`max_bytes` and `max_records` MUST NOT appear in the same request" and "A record-aware read that includes `max_bytes` MUST return 400".
- **Normative text:**
  1. A record-aware read (`record=` or `tail_records=`) MAY include `max_bytes=<positive integer>`. The response then contains the longest run of complete records from the resolved start whose total size is at most `max_bytes`, and always at least one record when one exists at the start. Size counts the stored message bytes, including each record's terminating LF, whatever representation the response uses.
  2. `max_bytes` MAY be combined with `max_records`. Both limits apply.
  3. `Stream-Record-Next` gives the continuation. All other headers are unchanged.
  4. Offset reads keep base-protocol `max_bytes` semantics.
  5. A server that advertises `keyed-state-v1` for a stream MUST support this section on that stream's record-aware reads, as P2.3 ties P1 to `keyed-batch-v1`. Keyed clients verify support through that token.
- **Compatibility.** Requests that were rejected with 400 now succeed; this is additive. During a rolling upgrade, the owner sends `max_bytes` only after seeing `keyed-state-v1`, and the indexer treats a `max_bytes` 400 from a node that does not advertise it as transient. Implementation is a byte cut over the planned window (`crates/ursula-runtime/src/engine/in_memory.rs:1003-1022`), and it works with sparse marks (§6.2).
- **Alternatives.** Choosing `max_records` adaptively from past record sizes cannot bound the window, because Pi records reach 32 MiB. A new parameter name would add surface for the same meaning.
- **Recommendation.** Adopt. Owner replay, read-back and indexer ingest all depend on it to bound voter memory.

### 5.5 Internal items (no protocol surface)

- **Incarnation binding.**
  - The node passes `incarnation = created_at_ms` and the source tail `N` to the indexer. The namespace path contains the incarnation, so a recreated path can never be served from an old namespace.
  - Uniqueness is made a guarantee: `created_at_ms := max(now_ms, group.last_created_at_ms + 1)` at create apply (C7). Today's test and DST clocks are manual, so delete-and-recreate tests collide, and leader clock skew can do the same in production. `HeadStreamResponse` gains `#[serde(default)] created_at_ms: Option<u64>`, because followers forward HEAD over MessagePack (`crates/ursula-runtime/src/request.rs:117-131`).
- **Source continuity check.**
  - Each manifest records `through_record = D` and `through_digest = blake3(stored bytes of record D−1)`.
  - Every ingest starts at `D−1` and compares the digest. While `D > N`, keyed-state answers 503 (P3.6 forbids serving `D` above the tail). On a mismatch, or when `D > N` persists across a leader read, the namespace is invalid for its source: it is retired and rebuilt from record 0, and keyed-state answers 503 until the rebuild catches up.
  - This covers restores from backup and any incarnation or ingestion bug. A restore rewinds the log itself; it is an operator action after which owners reopen, and the keyed state of a restored stream restarts from its rebuilt namespace.
- **Projection format versions.**
  - Namespaces are versioned, `v{fmt}`. A new format means a new namespace rebuilt from record 0 (§6.1 U20).
  - The indexer keeps serving the old version until the new version's `D` reaches the old version's last published `D`, so the served `D` never decreases. The old version is then deleted after grace.
  - Readers ship before writers, so a namespace is never read by a binary older than its format.
- **`$transaction` content-type normalization.** This is a bug fix, not a protocol item: op content types are compared raw (`crates/ursula/src/lib.rs:2721-2735`). It adds a one-line clarification in `extensions.md` §1.8.
- **Gateway classification.**
  - A keyed-state GET is `Action::Read`, or `Action::Tail` when it carries `min_through_record`, so it passes live-read admission and the live-read usage class (`crates/ursula-gateway/src/lib.rs:904-912`). The gateway's live-read limit answers 429 without `Retry-After` (`crates/ursula-gateway/src/lib.rs:735-739`); owners retry it (§7.6).
  - The long-poll response-header timeout rule extends to keyed-state waits (`crates/ursula-gateway/src/lib.rs:525-547`).
  - Routing is unchanged: stream affinity already sends `…/keyed-state` to the stream's leader.
- **Node routes.** Explicit `/{b}/{s}/keyed-state` and `/{b}/{a}/{s}/keyed-state` routes. Today such a request matches the affinity route and returns 400 once the name is reserved.
- **Bucket listing (`extensions.md` §1.4).** Implemented as specified. `last_write_at_ms` is omitted until it is tracked; this is a one-line clarification that the field is optional.
- **Rolling-upgrade gate.** See §6.3.

## 6. Ursula implementation changes

### 6.1 By crate

Production LoC; tests are listed separately below.

| ID | Crate / file | Change | LoC | Risk | Milestone |
|---|---|---|---|---|---|
| U1 | `ursula/src/render.rs:725-753` | P1: `RawValue` validation, flattening, lexical minify with per-message depth counting; a minify loop that skips string contents quickly | +110 / −25 | medium (all JSON writes) | M0b → M1 |
| U2 | `ursula/src/render.rs:498-527` | envelope view splices stored bytes (once P1 stores lone surrogates, re-parsing into `serde_json::Value` would turn the envelope into a 500) | +20 / −15 | low | M1 |
| U3 | `ursula-index/src/keyed/batch.rs` (new) | `keyed-batch-v1` validator and parser (serde + `&RawValue`, raw-key check, canonical base64url), shared by node and indexer; `ursula` already depends on `ursula-index` (`crates/ursula/Cargo.toml:56`) | +220 | low (fuzzed) | M1 |
| U4 | `ursula/src/lib.rs:2421, 2552, 2642, 2721` | keyed streams validate at all four write paths; token advertisement; keyed creates refused below `Lk` (§6.2) | +120 | low | M1 |
| U5 | `ursula-shard/src/lib.rs` | split `is_reserved_affinity_stream_id`, which today serves the apply-time validator, node routing and gateway routing alike (`ursula-stream/src/validate.rs:28`, `ursula/src/lib.rs:1108`, `ursula-gateway/src/lib.rs:677, 880`): the routing and HTTP predicate reserves `keyed-state` from M1, and the apply-time one only from C8's level; move `normalize_content_type` and `profile_of` here so node, gateway and indexer share them | +50 | low | M1 |
| U6 | `ursula/src/lib.rs:3101-3107`, `ursula-runtime/src/engine/in_memory.rs:1003-1022` | P7: `max_bytes` on record reads | +60 | low | M2 |
| U7 | `ursula/src/lib.rs` (new handler), `ursula-runtime/src/request.rs:117-131` | keyed-state proxy: routes, parameter validation, stream metadata (incarnation, tail), forwarding to `/v1/keyed`, status mapping, tokens, gzip; an explicit 405 for `HEAD` and other methods (axum's `get()` would answer `HEAD` by running the GET handler, wait included); `HeadStreamResponse.created_at_ms` | +350 | medium | M2 |
| U8 | `ursula-gateway/src/lib.rs:525-547, 839-941` | `classify_request` entries (keyed-state Read/Tail; bucket listing); keyed-state waits use the long-poll header timeout | +50 | low | M1/M2 |
| U9 | `ursula`, `ursula-runtime` | bucket stream listing (`extensions.md` §1.4): scatter-gather across groups, merge by stream ID, cursor | +300 | low-medium | M1 |
| U10 | `ursula/src/lib.rs:2721` | normalize `$transaction` op content types | +10 | low | M1 |
| U11 | `ursula-index/src/source.rs`, `index.rs` | event-time ingest extracts the timestamp lexically, tolerating lone surrogates | +80 | low | M1 |
| U12 | workspace `Cargo.toml:73` | declare `serde_json` `features = ["raw_value"]` (today it is enabled only through feature unification) | +1 | — | M0b |
| U13 | `ursula-index/src/part.rs` | part v2: `key`, `record`, `del`, `value`; ZSTD; page index; reader with `after`/`limit` pushdown; range tombstones and the layout in footer metadata, so there is no separate layout object (today one is written per part, `index.rs:783`); a part's key range covers its tombstones' endpoints | +450 | medium | M2 |
| U14 | `ursula-index/src/keyed/merge.rs` | streaming k-way merge: LWW by record, point and range tombstones, `after`/`limit`/byte budget | +300 | medium (cache; rebuildable) | M2 |
| U15 | `ursula-index/src/manifest.rs` | manifest v6: `through_record`, `through_digest`, `source{stream, incarnation}`, format, runs (record range + parts), `published_at_ms`, list of obsoleted objects | +200 / −60 | low-medium | M2 |
| U16 | `ursula-index/src/keyed/engine.rs` | ingest through P7; continuity check; publish without pre-refresh; post-CAS read-back of `CURRENT`; post-CAS incarnation check; deletion of the writer's own unpublished objects; size-tiered compaction (ratio 2, width 4); part-granular merges into the oldest run when newer runs reach 200% of it; tombstones dropped only by merges into the oldest run; at most 8 runs; compaction committed by rebasing its manifest edit when all inputs remain; per-namespace compaction byte budget; ranged-GET streaming inputs; delta-driven GC with 10 min grace | +850 | medium | M2 |
| U17 | `ursula-index/src/keyed/http.rs`, `service.rs` | internal `/v1/keyed` API; per-namespace single-flight; `min_publish_interval` enforced against `CURRENT.published_at_ms`; waiter budget; `ArcSwap` manifests in place of `query(&mut self)`; caches filled on write; `drain(bucket)`, acknowledged per pod | +450 | medium | M2 |
| U18 | `ursula-index/src/source.rs` | default view; P7 pages; follower reads for rebuild; client reuse | +60 / −10 | low | M2 |
| U19 | `ursula-index/src/cache.rs:394-465` | parameterize the cache validator | +20 / −10 | low | M2 |
| U20 | `ursula-index` CLI | `keyed-state verify` (sampled rebuild, row-by-row compare at equal `D`), `rebuild` (followers, rate-limited, parallel by record range, blue/green), `sweep` (one namespace LIST; deletes unreferenced objects older than the GC grace period), `dump` | +320 | low | M3 |
| U21 | `ursula-sim`, `ursula-index` | indexer DST: `SourceClient` trait, injectable GC clock (always the epoch under madsim today) | test infra +900 | medium | M4 |
| U22 | `ursula-stream/src/state_machine/lifecycle.rs:596-603` | delete apply of a keyed stream enqueues its incarnation's prefix `.keyed/{bucket}/{key}/{c:016x}/` as a GC path (gated), which the worker removes as a prefix. The general parts, removing the stream's external payloads and sweeping only the deleted incarnation's names so that a two-segment stream's sweep never reaches an affinity stream under the same name, are bounded-state F14a and F14g (C7) | +30 | low | M2 |
| U23 | `ursula-runtime/src/runtime.rs:602-633`, `ursulactl` | purge: `drain(bucket)` to every indexer pod, acknowledged by each, then erase and prove `.keyed/{bucket}/` | +70 | low | M2 |
| U24 | all | metrics: S3 requests by class for packs and C2 per group; per-namespace S3 requests by class, live bytes per bucket, `N − D` lag, CAS conflicts, run count, GC backlog; owner counters (§7.6). The per-group state gauges (record marks, dense entries, pack and external refs, feature level) come from bounded-state B0 | +150 | low | M2–M3 |

Pi-specific Rust (U1–U24 and C8; C9 is optional, +60) totals about +4,300 / −120 production LoC and about +2,800 test LoC, plus about 900 LoC of DST infrastructure. The never-trim core (§6.2) is general Ursula work, costed in the bounded-state document at about +5,800 / −400 production LoC and +6,500 test LoC.

TypeScript package:

| Part | LoC |
|---|---|
| tuple layer and families | 300 |
| LocalStore | 650 |
| log and keyed-state clients | 450 |
| Pi layer (17 methods + planner, ported from `sqlite/storage.ts` and `memory.ts`) | 1,150 |
| open, claim, fencing, flush, close | 400 |
| verify and inspect CLI | 200 |
| tests | about 2,000 |

Converging the event-time index onto the keyed engine (about −1,200 / +350) is a deliberate Ursula follow-up, not on Pi's path. Two engines coexist until then. Because keyed logs are never trimmed, such later server-side extractors (that index, or a #404 export view) can backfill from full history.

### 6.2 Never-trim core

Never-trim needs bounded replicated state, and that is general Ursula work: `docs/architecture/bounded-stream-state.md` (the bounded-state document) specifies it, and its workstream B0–B7 delivers it. Every stream stays within about 32 KiB + 8 B per unflushed record + 16 B per MiB of cold log, with no retention, and the workstream fixes the cold-path defects a never-trim stream would hit. The IDs this document uses map onto it:

| ID | Bounded-state item | Level | Milestone |
|---|---|---|---|
| C0 | F0: group feature levels, `SetFeatureLevel`, a level frame in snapshots that older binaries refuse, `TidyStream` | plumbing; `TidyStream` at Lb1 | B1; B3 |
| C1 | F1: sparse cold record marks; retention into cold history lands on the mark at or below its target | Lb2 | B1 (ungated parts), B4 |
| C2 | F2: pack-reference compaction through `CompactCold`: the Raft engine's all-shared branch, then a driver | none | B1, B2 |
| C3 | F3: a receipt window of 1,024 items per stream plus each producer's newest acknowledgement; idle expiry; caps | Lb1 | B3 |
| C4 | F4a: message-record collapse at every cold transition | Lb1 | B3 |
| C5 | F6a: `hot_payload_len` counter | none | B1 |
| C6 | F5 with F19: external locators committed in state and offloaded to cold-index pages; a clip rule and a repair for stale page entries | Lb3; F19 none | B1, B5 |
| C7 | F14g: unique incarnations, `created_at_ms := max(now_ms, group.last_created_at_ms + 1)`, and incarnation-scoped cold objects and GC | Lb1 | B3 |

What this means for Pi:

- **Prerequisite.** Pi production requires bounded-state B4 (sparse marks, Lb2), which comes after B1's defect fixes and B2's pack driver. B5 (C6) is required before Pi lowers `external_payload_min_size`; B6 (byte-based snapshot cadence, compact hot window) is recommended.
- **Interim.** Until C6 ships, Pi clusters keep `external_payload_min_size` above the 32 MiB body cap and the per-group admission caps at 64 MiB or more (§3.3). Every commit rides Raft inline, so the staging defects are unreachable, including the cold-frontier regression that a ≥1 MiB append after hot bytes triggers today (bounded-state D1).
- **Behavior Pi relies on.** Pi sends no producer headers, so it holds no receipts; it never advances retention, so retention landing on a mark does not affect it; and it does not use `/bootstrap`.
- **Reads.** *Measured*: 200k sealed records over 124 MiB need 2,000 B of marks instead of 1,600,000 B of dense offsets. A cold read by record over-reads less than 1 MiB before its start record, adding one block GET about half the time plus about 50 µs of CPU. Past its end, the window runs to the next anchor, which a large record can push far out, so the owner and the indexer bound every record read with `max_bytes` (P7).
- **Levels.** Levels follow release order across both documents. The keyed level `Lk` is the first level that carries C8; keyed creates are refused below it. C7 and U22's GC-path enqueue ride the first level released after they are ready, so numbers are assigned at release.

Pi-only core items:

- **C8, apply-time reservation of `keyed-state`.** `crates/ursula-stream/src/validate.rs:28` runs on apply for every stream command, so the reservation is gated: `validate_stream_id` receives the group feature level, and only its apply-time predicate (split out in U5) adds `keyed-state` at `Lk`. The HTTP layer rejects the name at once (U5). +20 LoC, M1.
- **C9, optional: `record_match` fast-fail.** A check before `stage_external_payload` and before any index write, with apply still authoritative. +60 LoC, M4, latency lever 1.

### 6.3 Rolling-upgrade gate

- **Raising the level.** P1 and P2 run in the HTTP layer of whichever node receives the write. Followers forward commands they have already built, and the leader does not re-validate (`crates/ursula-raft/src/engine/mod.rs:1230-1236`). An old binary would therefore silently re-normalize keyed records (*measured*: sorted members, `1.50e3` becomes `1500.0`, `\ud800` is rejected), producing false `FencedError`s and LocalStore/projection divergence. So keyed streams may be created only at level `Lk` or above (§6.2). The level is raised after every node runs the new binary, and never lowered.
- **Owner check.** The owner requires `keyed-batch-v1` in the create response and in HEAD before its first commit. An old node never advertises it.
- **Indexer check.** The indexer rejects sources whose metadata lacks the token.
- **No downgrade.** Once a level is raised, binaries below it must not run. Snapshots at a raised level carry a level frame that binaries without C0 refuse, and newer binaries refuse levels above their maximum (bounded-state F0). Release notes state this.
- **Projection format.** Format changes ship readers before writers. Namespaces are versioned, so no binary reads a format newer than it understands.
- **Exit drill (M4).** A rolling restart under live keyed traffic, with zero poison and byte-identical records.

### 6.4 Replicated-core changes (complete list)

The state machine, snapshot codec or command set changes for:

- C0, C1, C3, C4, C6 and C7: the bounded-state levels Lb1 to Lb3, each change listed with its gate in the bounded-state document (§5.20);
- C8: apply-time reservation, at `Lk`;
- U22: delete apply of a keyed stream enqueues its projection prefix as a GC path (an existing `ColdGcTarget::Paths` entry, so no codec change).

All of them are gated by C0. C1 also changes `StreamResponse::Appended`, which is apply output, not replicated state. C2 and C5 change no replicated state. P1, P2 and P7 are HTTP-layer and read-path changes. Fencing needs no core change.

### 6.5 Not changed

- `record_match` semantics.
- Producer semantics, apart from C3's window.
- `append-batch`, attrs, TTL.
- Generic snapshot and retention semantics: keyed streams get no special rules.
- The authorization model: still per bucket.
- No ReadIndex: writes linearize through `record_match`, and every read names an explicit ordinal.
- `/bootstrap`: keyed streams get no special rules, and Pi does not use it. Its generic defects (updates dropped after a checkpoint, one part for the whole cold suffix) are fixed for every stream by bounded-state F11.

## 7. Owner (TypeScript) design

### 7.1 Structure

`UrsulaStorage` is made of four parts:

- **the Pi layer:** the 17 methods' query structure is ported from `SqliteStorage`, since the key families are nearly SQLite's tables and indexes; the commit planner is ported from `MemoryStorage.prepareCommit`, with identical error classes and messages;
- **LocalStore;**
- **a log client:** append, read-back, replay, long-poll;
- **a keyed-state client.**

`m/format` (`{"pi_durable_keyed":1,"tuple":1}`) versions the Pi schema. A migration bumps it through resumable batches within the 32 MiB record limit, and owners refuse to open formats newer than their own. Because commitSeq is an explicit field, migrations may rewrite any family.

### 7.2 LocalStore: overlay and range cache

- **Overlay.** The parsed op lists of committed records `[E, tail)`. This is the pinned set. A record is appended to the overlay when its commit is confirmed, and the prefix below `E` is dropped when `E` rises. `E` is monotone: after a flush-wait returns `D_pub`, `E := max(E, min(D_pub, tail))`.
- **Cache.** A set of disjoint key ranges. Each range holds the visible rows of `state(tail)` within it, as `key → {record, value text}`, with no tombstones and no per-entry versions. Write-through applies each confirmed record to covered ranges only. Keys outside every covered range live only in the overlay, so nothing leaks.
- **Merging a remote page.** This is the synchronous step in §3.5. Because fold is defined per record, `state(tail)|R = fold(state(D_resp)|R, log[D_resp, tail))` for any range `R`. The merge is exact by definition, and `E` only governs which pages are acceptable. Two failure modes are impossible by construction: a stale page backfilled after its tombstone was dropped, and rows masked by a dropped range tombstone reappearing.
- **Single-state reads.** The final computation of every Storage read runs in one synchronous pass over covered ranges, with refcount pins on the ranges in use, so eviction cannot livelock a multi-step read.
- **Data structure.** An O(log n) ordered map (B+tree). Keys are binary strings: in JavaScript, comparing strings of code units 0–255 equals octet order. Values are stored as JSON text and parsed fresh on every read, which keeps returned objects detached (cases 3 and 4).

### 7.3 Complete-at-mint (fresh floor)

`F_fresh` starts at `next_id_open = m/next_id` and only increases. A key is fresh-covered when any of its ID-typed components (marked † in §4.3) is at least `F_fresh`. Fresh-covered keys are answered from the cache, where write-through has put every write made since open.

Why this is sound: at open, no committed key has an ID component of `next_id_open` or more.

- Every committed ID is below `m/next_id` (the high-water rule, §7.7).
- Every referenced ID was minted, and the planner persists `m/next_id ≥ max(minted) + 1` in the same record as any reference (pi: `sqlite/storage.ts:162-185, 579-586`). The first half is Pi's contract, not a storage check: Storage trusts the Session to supply semantically valid references (pi: `src/types.ts:985-992`), and MemoryStorage does not validate them (pi: `memory.ts:679-700`). A reference to a never-minted `X ≥ m/next_id`, such as `task.conversationId = X`, would make a later `scanTasks({conversationId: X})` answer locally and miss the task; it is outside the contract. Extending the high-water rule to every ID component would close the gap, but `mintId` would then skip IDs that MemoryStorage returns (G2).
- Every later write is local.

This makes local, with no remote reads: the primary rows of everything created in this session (`t/`, `s/`, `d/`, `x/`, `c/`); new scopes (`e/{new conv}/`, `c.ot/{new task}/`, `d.s/d.a` of new scopes); and new IDs inside old scopes (the front of `e/{conv}/`, the tail of `t.s/{st}/`). In particular, the reservation's `storage.task(T)` for a just-created task (pi: `src/session/transaction.ts:762-770`) and new-ID ownership checks are local.

Coverage marks are applied by write-through after a commit is confirmed, never at planning time, so a rejected or poisoned commit leaves no mark. Under memory pressure, fresh rows are evicted oldest-ID first by raising `F_fresh` to `F'`. Rows whose freshness came only from IDs in `[F_fresh, F')` and that lie in no explicit range are dropped.

### 7.4 Preload

Open preloads the live set and its context (§3.6 step 4).

From the `t.s` and `s.s` scans, it also installs derived point ranges for each live task or submission: `t/{id}` and `s/{id}` hold the same bytes as the covering row, and `x/{id}` holds the table tag. Invariants I24 and I25 make this sound, and the verify tool checks them. The scheduler's running→pending rewrite and its `#committedTask` reads then stay local.

Owner-chain conversations and `c.ot/{T}/` cover `Harness.open`'s reconcile (pi: `scheduler.ts:519-555`) and the task graph's per-task scans (pi: `task-graph.ts:131-148`). The root conversation's record, its newest head marker and its newest entry page cover the first turn after open.

### 7.5 Eviction and memory budgets

| Pool | Default | Bound | Eviction |
|---|---|---|---|
| Cache (explicit ranges + fresh rows) | 64 MiB, configurable; size it to the sum of active ranges | budget | LRU over ranges; a range may shrink from its cold end, because any sub-range of materialized state is still exact; fresh rows go via `F_fresh`; values over 1 MiB are evicted first |
| Overlay (pinned) | ≤ 4–8 MiB in steady state (flush policy) | hard cap 256 MiB; alert at 64 MiB | prefix dropped when `E` rises |
| Session document trackers | Pi-side; unbounded today (pi: `session.ts:61`) | — | optional upstream fix |

At the hard cap, new commits wait for the flush loop until the 30 s commit deadline, then the storage poisons. Reopen does not append a claim while keyed-state lags (§3.6 step 2), so a lagging indexer never causes repeated fencing.

### 7.6 Error policy

| Situation | Result |
|---|---|
| Remote reads on the Session line, at open, and read-back (keyed-state, log replay) | idempotent: retry 5xx, every 429 (the gateway's live-read limit sends none, so back off even without `Retry-After`), timeouts, connection resets, and a keyed-state 400 whose `Stream-Record-Next` is below an `r` that is at most the acknowledged tail (a lagging node, as in the read-back table); honour `Retry-After` when present; up to 30 s. Then poison the storage and throw a plain `Error` (never `StorageRejected`); at open, the open fails instead. Pi's follow-up `#step` commit then also fails, the Session poisons, the host reopens, and no task is faulted. A 400 for an `r` above the acknowledged tail is an invariant violation and poisons at once |
| Background flush-waits (§7.9) | the same transient outcomes, plus 204 and 500, retry indefinitely with backoff capped at 30 s and never poison: a failed flush-wait only delays `E`, and the overlay cap (§7.5) governs liveness. A 404 (stream gone or no longer served) poisons |
| 401, 403 on any request (gateway credentials expired or revoked) | poison with a plain `Error`; the host refreshes credentials and reopens |
| Validation equal to MemoryStorage's | the same plain `Error` and message |
| `document.copy` source invalid; record > 32 MiB; depth > 127; key > 4096 | `StorageRejected` (pre-checks mirror every server limit, so 413 and 422 do not occur in normal operation) |
| Append outcomes | the stateful policy in §3.3 |
| Fenced; foreign `m/owner` seen by a flush-wait; `D_resp > tail` with no commit in flight | poison with `FencedError` |
| Poisoned storage | rejects every operation, including `mintId` |

Owner metrics: remote reads on the Session line and their latency; pinned bytes; poison, fence and contention counts; remote-read retries.

### 7.7 mintId

`nextId` is a local counter. The persisted tracker follows SQLite's high-water rule: when `max(nextId, written IDs + 1)` exceeds the persisted value, the record includes `p m/next_id`. Both `nextId` and the tracker advance only after the append is confirmed, by a 2xx or a successful read-back. A deterministically rejected commit therefore cannot leave the tracker ahead of `m/next_id`. Open sets `nextId := m/next_id`, or 2 if absent. Verified behaviour: case 22's 101 after an explicit entry 100, and exhaustion past `MAX_SAFE_INTEGER` (`m/next_id = 9007199254740992` round-trips as a JSON number).

### 7.8 Fencing and claims

Claims carry a 128-bit random nonce, so two openers can never write identical claim bytes. The epoch is the claim ordinal, carried as `"o"` in every record. §3.6 describes the claim loop, which uses the 412 continuation and a 5 s deadline; that combination prevents starvation against a busy zombie.

Takeover semantics, stated exactly:

- Claims racing for the same tail are detected (`OwnershipContention`).
- Otherwise, in `fence` mode the last opener wins.
- `fail-if-active` refuses when the current claim is not closed and the log tail advances within `W`.

Every flush-wait reads `m/owner`. A foreign nonce poisons promptly, which shortens a zombie's window to one flush interval even if it never commits again.

### 7.9 Flush policy

A background loop, never on the Session line, issues one flush-wait at a time: `GET keyed-state?key=m/owner&min_through_record=tail&timeout_ms=60000`. It fires when:

- the overlay reaches 4 MiB, or 5,000 records; or
- its oldest record is 10 min old; or
- the storage is closing (`timeout_ms=1`, §3.7).

Its outcomes follow the flush-wait row of §7.6: every transient outcome retries indefinitely with capped backoff, so an indexer outage only delays `E`; only a foreign `m/owner`, a 404, or a 401/403 poisons. The server still caps publishes per namespace at one per 5 s.

## 8. Consistency model and invariants

Model:

- One writer per harness, linearized by `record_match`.
- Owner reads are read-your-writes, and each read observes `state(tail)` at a single point.
- A generic keyed-state response is exactly one `state(D)`. Reads are monotone across requests only when the client passes the highest `D` it has seen as `min_through_record`. Read-your-writes for a writer's Seq `s` is `min_through_record = s+1`.
- Pagination is keyset-monotone: no duplicates, and rows changed between pages may be skipped. Memory and SQLite behave the same way.

| # | Invariant | Test |
|---|---|---|
| I1 | A Pi commit is durable iff its record exists; `Seq` = its ordinal | fault proxy + reopen comparison |
| I2 | After the claim, every commit attempt carries `Stream-Record-Match` = the applied tail; only the claim loop derives a match from a read (the 412 continuation) | review, dual-owner and zombie tests |
| I3 | All attempts of one commit send identical bytes under one match; at most one lands; once any attempt is ambiguous, every later outcome is resolved by read-back | fault proxy: drop, duplicate, delay, a 429 storm after a timeout, ≥1 MiB payloads |
| I4 | The epoch is the claim ordinal and claims carry a 128-bit nonce, so distinct owners' records differ in bytes | property test |
| I5 | Claims racing for one tail raise `OwnershipContention`; `fail-if-active` never claims while the tail moved within `W`; an open that fails or refuses before the claim writes nothing but an idempotent create; an open whose claim is unresolved at the deadline (`ClaimTimeout`) may have fenced the previous owner | concurrent and sequential open tests, both modes; claim timeouts under the fault proxy |
| I6 | `StorageRejected` only for deterministic outcomes with nothing durable; transient failures on the Session line end in poison at the deadline; flush-waits never poison on a transient failure; a poisoned storage rejects everything | outcome-table cases; flaky-indexer conformance; indexer-outage drill |
| I7 | Stored bytes of every `application/json` write = `minify(input)`; every reader (LocalStore, keyed-state rows, log reads, envelope) sees those bytes | 10^6-case proptest; Pi case 21 |
| I8 | Every record of a keyed stream satisfies the grammar | HTTP negative vectors on all four write paths; fuzz |
| I9 | Every published namespace equals `fold(log[0, D))` at its `D` | engine model test vs a `BTreeMap` oracle; `keyed-state verify` sampled daily |
| I10 | For one incarnation, served `D` never decreases except after a source restore, and never exceeds the source tail; a response to `min_through_record=r` has `D ≥ r` or is 204; each 200/204 reflects one `state(D)` | concurrent publish and compaction linearizability check; restore drill |
| I11 | A row's `record` is the ordinal of the record holding the key's last applied put in `state(D)`, unchanged by compaction and rebuild | engine tests |
| I12 | Tombstones are dropped only by merges into the oldest run for the merged key range; a part's key range covers its range-tombstone endpoints | engine tests with tombstone-only parts |
| I13 | Publication is a CAS on `CURRENT`; after a CAS, a writer adopts an ETag only from a read-back of its own bytes; a compaction commits by rebasing iff all its inputs are still present | two-pod race test, with a compaction CAS between a publish's CAS and its read-back |
| I14 | GC deletes an object only when it is obsolete and no manifest published within the grace period (≥ the longest request) references it | GC/read concurrency test; DST |
| I15 | A namespace is served only for the incarnation it was built from, and only while record `D−1`'s digest matches the source | recreate-same-path test with a frozen clock; restore drill |
| I16 | After purge, nothing remains under `{bucket}/` or `.keyed/{bucket}/`; after stream deletion GC, nothing remains of the deleted incarnation's `.keyed/{bucket}/{key}/{c:016x}/` or of the stream's external payloads, and no other stream's objects are touched | purge and delete drills, including a two-segment stream next to affinity streams under the same name |
| I17 | For every covered range `R`, `cache|R = state(tail)|R` between synchronous steps | LocalStore property test with a deterministic scheduler |
| I18 | The overlay holds exactly records `[E, tail)`; `E` is monotone; pages with `D_resp < E` are rejected at merge time | unit tests; interleaving tests |
| I19 | `D_resp > tail` with no commit in flight poisons | third-party append injection |
| I20 | Every Storage read result is computed in one synchronous pass over covered ranges | deterministic scheduler completing flush-waits and evictions between any two awaits |
| I21 | Keys with an ID component `≥ F_fresh` are covered; `F_fresh` only increases | model test with dangling references below `m/next_id` |
| I22 | LocalStore stores JSON text and returns freshly parsed objects | cases 3 and 4 |
| I23 | `mintId` returns IDs above every committed ID; `nextId` and the tracker advance only after confirmation | cases 1 and 22 (both variants); rejection-then-commit test |
| I24 | Each committed ID has exactly one `x/{id}` row, whose table never changes | cases 1, 2, 22; model test |
| I25 | Derived keys equal `derive(primary)`: `t.s`, `t.c`, `t.k`, `s.s`, `s.c`, `s.r`, `c.oc`, `c.ot`, `e.h`, `d.a`, `d.s` | TypeScript verify at the end of every model case; sampled in production |
| I26 | `e/` value `seq` = commitSeq; the `e/` and `e.h/` values of an entry are byte-identical | model test |
| I27 | The Rust indexer fold, the TypeScript overlay fold and the reference model produce identical `key → (record, value)` maps on any log prefix | applier conformance property test |
| I28 | Sparse marks resolve every retained record to the boundary the dense index would | differential vs `crates/ursula-stream/tests/record_coordinates_reference.rs`; DST cold paths |
| I29 | Per-stream replicated state stays within bounded-state I1: about 32 KiB + 8 B per unflushed record + 16 B/MiB of cold log, with at most 64 pack refs and 16 staged external refs | bounded-state soak gauges and DST invariant 9 |

## 9. Performance and cost model

### 9.1 Workload profiles

The profiles below drive the rest of this section (*measured* with an instrumented Pi harness, §11.9):

- **(a) Streaming generation:** about 9 commits/s per streaming conversation, 3–10 KB/s of log. About 40% of those bytes are permanent projection rows (entries); partials are transient.
- **(b) Tool-output rounds:** about 10 commits/s per running tool. pi.live deltas run at up to 100 KiB/s per tool (300 KiB/s with three tools) and are range-deleted at the next base.
- **(c) Slow trickle:** deferred polls at about 0.2 commits/s.
- **(d) Resumed long sessions:** 0.5–2 GB of history, then activity as in (a).

### 9.2 Commit latency: mean-value line model

Pi has one Session line per harness, and exactly one commit is in flight at a time. The model:

- Service time `S = L̄ + 1.1 ms`, where 1.1 ms is Pi's per-commit line overhead, fitted to probe2: the model gives 89 commits/s against 90 measured at 10 ms, and 38 against 38 at 25 ms.
- N streaming conversations are partial sources, each re-armed 100 ms after its commit settles.
- A blocking commit waits `S·(1+Q)`.
- submit→provider ≈ 4 blocking commits + 3 ms.
- Line capacity ≈ `1000/(L̄ + 1.1)` commits/s.

| Mean L | N=1 | N=4 | N=8 | N=16 (line throughput) |
|---|---|---|---|---|
| 4 ms | 24 ms | 28 ms | 35 ms | 62 ms (141/s) |
| 5 ms | 29 ms | 34 ms | 44 ms | 93 ms (133/s) |
| 6 ms | 33 ms | 40 ms | 56 ms | 133 ms (124/s) |
| 8 ms | 42 ms | 55 ms | 84 ms | 237 ms (106/s) |

Reading the table:

- At N=16 the offered load is about 142 commits/s (*measured* at zero latency), so the line saturates above L̄ ≈ 5.5 ms. Partial commits then coalesce, because the throttle re-arms after each settle, and degradation is gradual.
- No pipelining is needed for N ≤ 8.
- Each extra 1 ms of owner CPU per commit adds about 40–50 ms at N=16. This is why the LocalStore must be O(log n) and why there are no per-commit scans.
- No disk-WAL latency data exists. The published Ursula numbers (p50/p99/p999 of 3.0/7.4/51.5 ms at 100 streams) used `--raft-memory`, octet bodies and producer headers. Production adds a leader and a follower fsync plus a 200 µs group-commit linger (`crates/ursula-raft/src/types.rs:21`).
- Prior for production `L`: p50 4–6 ms, p99 10–25 ms. M0c measures it (§10).
- P1+P2 CPU (*measured*): 0.37 µs at 127 B, 2.6 µs at 938 B and 83 µs at 56 KB, all ≤ 2% of `L`. 1 MiB bodies take 1 ms, against a 30–100 ms staging PUT.

### 9.3 Reads, open, first turn

- Local reads take < 0.1 ms. A 256-row page takes ≤ 1 ms (*measured*: V8 parses a 727 KiB page in 0.71 ms).
- keyed-state reads take 2–6 ms with warm caches, including the proxy hop, and 30–100 ms cold (1–3 serial S3 GETs).
- Open: HEAD, the `m/` read, a replay of ≤ 4 MiB, and 10–20 parallel preload ranges. That is p50 100–250 ms with a warm indexer, flat in history length and in conversation count.
- The first submit→provider after open targets ≤ 300 ms p50, because the root context is preloaded.

### 9.4 S3 requests by class, per harness-hour

Projection figures come from a simulation of the U16 policy. Log figures assume Ursula's existing packing plus C2.

| Profile | Publishes/h | Projection PUT-class/h | Projection GET/h | Log PUT/h | Log GET/h | $/h |
|---|---|---|---|---|---|---|
| (a) streaming | ≈ 7 (5,000-record trigger) | ≈ 30 | ≈ 26 | ≈ 5–10 (packs, C2 output) | ≈ 130 (C2 inputs) | ≈ 0.0003 |
| (b) tool rounds, sustained | 88 (100 KiB/s) to 260 (300 KiB/s) | ≈ 340–1,000 | ≈ 290–870 | ≈ 30 | ≈ 900 (hot group) | ≈ 0.002–0.006 while sustained |
| (c) slow trickle | ≤ 6 | ≈ 25 | ≈ 20 | ≈ 1 | ≈ 0 | < 0.0002 |
| (d) resumed 0.5–2 GB | ≈ 7 | ≈ 30, flat in history | ≈ 27 | as (a) | as (a) | ≈ 0.0003 |
| idle | 0 | 0 | 0 | 0 | 0 | 0 |

Each publish costs one part PUT, one manifest PUT and one `CURRENT` CAS, plus a read-back of `CURRENT` (a stat and a conditional GET, §3.4 step 5). A GET through `ObjectStore::get` is a stat plus a read (`crates/ursula-index/src/object_store.rs:333-357`), so the indexer caches ETags.

GC is delta-driven: manifests list the objects they made obsolete, so GC issues no LISTs and no reads of old manifests. Its DELETEs are free. There is no maintenance LIST either. A writer deletes the objects of its own unpublished attempts, so only an indexer crash leaves orphans, at most one publish or one compaction budget per crash; they stay until the stream is deleted, the bucket is erased, or an operator runs `keyed-state sweep` (U20).

Worst-case read amplification: an anonymous `public_read` reader that sends a new `min_through_record` every 5 s forces at most 720 publishes/h per namespace, about 3,300 PUT/h and 2,600 GET/h or $0.018/h, with about 160 MB/h of compaction. The gateway's `Tail` classification bounds such readers' concurrency per tenant.

### 9.5 Storage and replicated memory under never-trim

- **Log on S3.** The full history is kept. Profile (a) writes 11–36 MB per streaming hour, which costs $0.00025–0.0008 per month per streaming hour. Large images are stored once in the log and once as a live projection row.
- **Projection on S3.** About 1.2× the live permanent rows, i.e. about 0.5× of log bytes. With bucket versioning enabled (the reference stack enables it), a lifecycle rule `NoncurrentVersionExpiration = 1 day` on the `.keyed/` prefix is required. Without it, compaction leaves noncurrent copies of about write amplification × ingest per retained day.
- **Replicated memory per replica, active harness.** About 1 KB of metadata and attrs, dense offsets for unflushed records (a few KB), 16 B per MiB of cold log (C1), at most 64 × 250 B = 16 KB of pack refs (C2), 0 receipts (C3), and 0 external refs under the interim threshold (C6). That is about 5–30 KB plus 16 B/MiB.
- **Replicated memory per replica, idle harness.** About 1 KB plus 16 B/MiB, because the idle pack trigger compacts the refs away.
- **At scale.** 10,000 harnesses with 1 GB of history each need about 160 MB of marks per replica. Today's dense index would need about 28 MB per such harness (8 B per record at about 300 B per record).
- **Independence from the indexer.** None of this depends on the indexer being alive.
- **Group snapshots.** Snapshots every 5,000 entries (`crates/ursula-config/src/config.rs:205`) are dominated by hot payload, which the group hot cap bounds, not by per-stream state. Bounded-state F12e makes the cadence byte-based, so snapshot I/O stays near half the appended bytes.
- **Rebuild.** Reading the log through followers at about 100 MiB/s per pod, a 2 GB history reads in about 20 s, and about 20 min per pod per heavy harness-year. Rebuilds parallelize by record range.

### 9.6 Compaction write amplification

Simulation of U16's policy (`tiered.py`, §11.9): size ratio 2, width 4, a part-granular merge into the oldest run when newer runs reach 200% of it (`AMP` = 2.0) that rewrites 15% of its parts (`PHI` = 0.15, inside the 10–25% overlap expected for Pi's keys), and at most 8 runs:

| Profile | Flushed | Compaction | × flushed | Merges into the oldest run (part-granular) | Runs |
|---|---|---|---|---|---|
| (a) 10 MB/h for 1 week (1.7 GB) | 10.7 MB/h | ≈ 52 MB/h | ≈ 4.9× | ≈ 0.9/day, part-granular | ≤ 6 |
| (b) sustained tool rounds, 100 KiB/s | 4.8 MB/h | ≈ 21 MB/h | ≈ 4.4× | ≈ 7/day while sustained, on a small namespace | ≤ 4 |
| (d) resumed 2 GB + (a) | 10.7 MB/h | ≈ 59 MB/h | ≈ 5.5× | 0/day | ≤ 5 |
| storm, a publish every 5 s on 0.5 GB | 10.8 MB/h | ≈ 160 MB/h | ≈ 15× | 0/day | 8 |

Compaction cost is flat in history size. The previous policy (a full merge at ≥ 8 runs) rewrote 1–3.8 GB/h for resumed sessions, and grew linearly with history. Profile (a)'s 0.9 oldest-run merges per day leave little margin under the §11.10 gate of 1; at `AMP` = 1.0 the same profile needs 1.4/day and 5.3×. The storm has no gate: `min_publish_interval` and `Tail` admission bound it (§9.4).

### 9.7 Owner memory and outage tolerance

The overlay is ≤ 4–8 MiB in steady state. With the indexer down, the 256 MiB cap is reached (counting record bytes; JavaScript object overhead shortens these times):

- in (a), after 7–24 h;
- in (b), after 15–45 min;
- in (c), after days.

After that, commits wait and the storage poisons at the commit deadline, and reopen waits for keyed-state. Until then the owner keeps committing, because flush-waits never poison on a transient failure (§7.6). This assumes the Session-line reads stay local, which is the steady state (M3 gate); a cache miss during the outage poisons at the 30 s read deadline.

An S3 outage is different, and the error policy accepts it: once a group's hot buffer fills, its commits get 503 past the 30 s deadline, every active owner in the group poisons, and reopens fail until S3 recovers. No task is faulted, because poison is never `StorageRejected` (§7.6).

The cache defaults to 64 MiB; image-heavy active ranges need it sized to their sum. A deferred lever: fold the overlay to the latest op per key, which collapses (b)'s delta chains.

## 10. Milestones

Every exit criterion uses metrics that exist today or are added in the same milestone (U24). The order retires the largest unknowns first: protocol shape, commit latency, byte fidelity, the staging defect, and the subtle owner merge logic all run in M0.

### M0 — Alignment and risk spikes (1–2 weeks, in parallel)

- **M0a, alignment.**
  - Scope: `extensions.md` drafts of P1, P2, P3 and P7; a Rust reference model (`BTreeMap` fold); JSON vectors (valid and invalid grammar, fold results, HTTP statuses and headers, every edge case in §11.6); tuple-layer vectors.
  - Exit: the vectors pass the reference model, and you sign off §5 and §13 after M0b–M0e report.
- **M0b, P1 fidelity.**
  - Scope: U1, U2 and U12 on a branch; the official DS suite moved into CI (3-process memory-WAL shape).
  - Exit: DS conformance 300/300 in CI; 10^6 random cases with `stored == minify(input)`; depth 127/128 vectors, bare and wrapped; criterion ns/byte ≤ today's path for 127 B–56 KB; an end-to-end check over ≥ 3 runs that reports the coefficient of variation.
- **M0c, commit latency.**
  - Scope: a minimal owner (`MemoryStorage.prepareCommit` + the §4.4 encoder + match append), run through `probe2`/`probe3`. Setup: 3 nodes with disk WAL on the target volume, through ursulagw; the owner in a non-leader AZ; Pi payload mix (127 B–50 KiB); 50% background load with cold flush on; both 3-AZ and 1-AZ; load points N = 1/4/8/16; 3 runs.
  - `ursula-bench` gains `--content-type json --record-match`.
  - Exit: mean L ≤ 5 ms, p50 ≤ 4, p99 ≤ 20, p999 ≤ 60; N=1 submit→provider ≤ 4·L̄ + 5 ms; N=16 ≥ 130 commits/s and submit→provider ≤ 100 ms. A miss triggers §13 Q6. Results are recorded in the repo.
- **M0d, staging defects.**
  - Scope: failing tests for (i) a post-proposal error deleting the staged object and (ii) a 412-rejected ≥1 MiB append whose stale page entry corrupts later cold reads. Bounded-state B0 already commits (ii) among its reproductions; (i) joins them.
  - Exit: the tests are committed and the bounded-state design for them (C6: F5 and F19) is agreed with the maintainers.
- **M0e, owner merge.**
  - Scope: the TypeScript overlay/cache (§7.2–§7.3) against a ~200 LoC fake projection that folds the log to a random lagging `D` and truncates pages at random, under a deterministic scheduler that can complete a flush-wait or eviction between any two awaits.
  - Exit: 10^5 cases with `cache|R = state(tail)|R` and zero divergence from a MemoryStorage oracle.

### M1 — Log and Pi layer, full-resident owner (about 3 weeks)

- **Scope.**
  - Ursula: U1–U5, U8 (listing), U9–U11 and C8 on main, on bounded-state B1's level plumbing (C0).
  - TypeScript: tuple layer, families, Pi layer, log client, claims and fencing in both modes, the §3.3 outcome policy, close.
  - LocalStore without eviction: `E = 0`, with a full replay from record 0 in `max_records` pages until P7.
- **Exit.**
  - Pi conformance 23 × {direct, reopen after every commit} passes in CI against a pinned single-node `ursula` image.
  - Backend tests cover what the 23 cases miss: in-batch duplicate IDs; singleton vs key `""` at one address; requestId remaps within and across batches; terminal→pending rewrites; conversation and kind moves; >1 KiB strings.
  - Fencing, ambiguity, takeover, dual-owner and busy-zombie tests pass, including ≥1 MiB commits (inline under the interim threshold).
  - Bucket listing tests pass.
  - Keyed creates are refused below `Lk`.
- **Validation.** CI; the fault proxy (§11.7); P2 negative vectors on all four write paths.

### M2 — Keyed engine, keyed-state, P7 (about 4 weeks)

- **Scope.** U6, U7, U8 (keyed-state), U13–U19, U22–U24 (node and indexer metrics). In CI the indexer runs on S3-compatible storage (MinIO). The index bench gains an S3 `ObjectStore` mode for cold-read calibration on real S3.
- **Exit.**
  - Engine model test: ≥ 10^5 random workloads match the oracle at every `D`, with flush, compaction, GC and reads interleaved, including tombstone-only parts.
  - Crash injection between part, manifest and CAS, mid-compaction and mid-GC leaves state unchanged. A lost CAS leaves no orphans after the grace period; a crash leaves at most one publish's or one compaction budget's worth, which stream deletion reclaims.
  - A two-pod CAS race stays consistent.
  - Continuity checks: same-path recreate with a frozen clock (needs C7, from bounded-state Lb1 or carried on `Lk`); a restored source.
  - Logs written in M1 ingest from record 0.
  - P3 and P7 vectors pass at the real HTTP boundary.
  - Compaction is ≤ 6× flushed bytes for profiles (a) and (d) at 2 GB.
  - Purge proves both prefixes empty. Deleting a two-segment stream leaves affinity streams under the same name intact.
- **Validation.** §11.4–§11.6, and §11.7's indexer crash injection.

### M3 — Bounded owner (about 3 weeks)

- **Scope.** Overlay/cache, complete-at-mint, preload, eviction, flush policy, the full error policy; U20 verify and rebuild.
- **Exit.**
  - Conformance 23 × 2 × {indexer normal, paused, aggressive (publish and compact after every record), flaky (random 503s and resets)} × a 4 KiB cache, with zero faulted tasks.
  - TypeScript model vs MemoryStorage across all 17 methods, with random flush, reopen, takeover and eviction.
  - Zero remote reads on the line for steady-state text and tool turns.
  - Harness-level open (open + resume + first taskGraph + first viewState) at p50 ≤ 250 ms and p99 ≤ 1 s; p50 at 100k records ≤ 1.2× p50 at 1k, and likewise for 1,000 vs 10 conversations.
  - First submit after open ≤ 300 ms p50.
  - `verify` equals `CURRENT`.
- **Validation.** §11.1–§11.2 and the §11.10 benchmarks.

### Bounded-state workstream (B0–B7, starts now, parallel with M0–M4; required before production)

- **Scope.** The bounded-state document's milestones (§8 there): B0 harness; B1 correctness defects and ungated fixes, with the level plumbing; B2 pack-reference driver and orphan sweep; B3 level Lb1 (C3, C4, C7); B4 level Lb2 (C1); B5 level Lb3 (C6); B6 hot window and byte-based snapshot cadence; B7 hardening. About 11 weeks to B4, which Pi production requires.
- **Exit criteria Pi depends on.**
  - B1: the cold-path defects (bounded-state D1 to D4) are fixed or contained, with their reproductions in CI, M0d's tests among them; the level plumbing is available for `Lk`.
  - B2: W2-shaped workloads keep at most 64 pack refs per stream, and packs are GC'd after compaction.
  - B3: incarnations are unique (C7), and delete and recreate never loses the new incarnation's objects.
  - B4: sparse marks pass RC-1 to RC-21 and DST invariants 9 to 12; W1 at 3M records holds at most ⌈cold MiB⌉ + 2 marks and exactly its unflushed records as dense entries; an EKS rolling upgrade and raise under live traffic shows zero acknowledged-data divergence.

### M4 — Production hardening (about 3 weeks)

- **Scope.** U21 indexer DST; drills; metrics, alerts and runbooks (indexer outage, S3 outage, restore with continuity rebuild, purge, rolling upgrade, namespace verify and corruption); spec finalization; C9 optional.
- **Exit.**
  - DST: 10^4 seeds clean, 10^5 nightly.
  - A 7-day soak with a mixed fast/slow harness population in a hot group, with drills:
    - a 10 min indexer outage: zero poison among owners whose overlays stay under the cap and whose Session-line reads stay local, and `E` catching up after recovery;
    - a 10 min S3 outage producing only retries and poison, and no faulted task;
    - a rolling upgrade with zero poison and byte-identical records;
    - purge;
    - restore plus continuity rebuild, with keyed-state answering 503 until the rebuild catches up and owners reopening after poison;
    - blue/green format rebuild.
  - Replicated state per active harness ≤ 32 KB + 16 B/MiB.
  - Per streaming harness: projection ≤ 50 PUT-class/h and ≤ 50 GET/h (U24 per-namespace counters), and log ≤ 50 PUT-class/h and ≤ 250 GET/h (U24 per-group counters divided by active harnesses); idle harnesses 0.

## 11. Validation plan

Items are cited elsewhere as §11.n.

1. **Pi conformance.** 23 cases × {direct, close+reopen after every commit, failed commits included}, against a real `ursula` node and indexer. In CI from M1, multiplied by indexer modes and a 4 KiB cache from M3. The reopen wrapper follows pi: `test/jsonl-storage.test.ts:59-77`.
2. **Owner model tests** (fast-check). The same operation sequences run against MemoryStorage (the oracle) and `UrsulaStorage`. The generators include contract-allowed writes Session never makes: terminal rewrites, in-batch duplicates, explicit low IDs, cross-table collisions, dangling references to IDs below `m/next_id` (a reference to a never-minted ID is outside the contract, §7.3), and >1 KiB strings. Flush, reopen, takeover and eviction are interleaved. A deterministic scheduler completes flush-waits (raising `E`, eviction, `F_fresh` raises) between any two awaits. The LocalStore property `cache|R = state(tail)|R` holds on every covered range. Verify (I25) runs at the end of each case.
3. **Differential fuzzers.** The table and document fuzzers built for the model (MemoryStorage oracle, random reopen, NUL-extension kind and key pairs, 0 divergences today) move into the TypeScript test suite.
4. **Engine model tests** (proptest). A `BTreeMap` oracle with a range-tombstone table; random batches, flush boundaries, compaction choices, GC and reads over `key | start/after/end × limit × budget`.
5. **Applier conformance (I27).** Rust ingest, the TypeScript overlay fold and the reference model must produce identical maps.
6. **Byte fidelity and batch parsing.** proptest plus cargo-fuzz: no panics, and accepted ⇔ grammatical. HTTP vectors run at the real boundary from M1. Edge cases:
   - escaped and duplicate `ops`;
   - a lone surrogate in a top-level member name (rejected) and inside a value's member name (accepted);
   - non-canonical base64url (`AB`, `AA==`, `+/`, empty);
   - escaped keys;
   - depth 127/128/129, bare and array-wrapped;
   - `1e400`;
   - `lo ≥ hi`;
   - a repeated parameter;
   - `HEAD` and other methods on keyed-state → 405;
   - out-of-range and unparseable `timeout_ms` (clamped or defaulted);
   - wait timeout → 204;
   - `D > N` → 503;
   - an invalid batch on a non-keyed or absent stream → 422 (precedence);
   - `max_bytes` with `max_records`, and with `record_view=envelope`;
   - namespace absent → `state(0)`;
   - recreating the same path within one millisecond.

   The DST `http-protocol-surface` seed families extend to P1/P2.
7. **Fault and takeover.**
   - A fault-injecting proxy between owner and gateway drops requests and responses, delays them and duplicates them, with small and ≥1 MiB payloads, including a timeout → 429 storm → delayed commit sequence.
   - Two owners plus a zombie: every commit lands exactly once, zombie appends fail, and reopen after poison matches the oracle.
   - Both open modes run against idle, busy and crashed owners; a claim still ambiguous at the 5 s deadline yields `ClaimTimeout`.
   - Gateway responses the owner must survive: a live-read 429 without `Retry-After` during parallel preloads, 401 after credential expiry, and a lagging node's 400 on a flush-wait.
   - On the indexer, crashes are injected between part, manifest and CAS, mid-compaction and mid-GC.
8. **Ursula-core tests.** The bounded-state document's RC-1 to RC-21 suite, DST invariants 9 to 12 and measurement gates (its §6 and §7) cover C0 to C7, including the M0d regressions and the reproductions of its defects D1 to D4. Pi adds keyed cases on top: deletion of a keyed stream next to affinity streams named `chunks`, `cold-index` and `external`, and under the same name (U22 with C7); the keyed level `Lk` in the mixed-version gate test.
9. **Probes behind *measured* figures.** These are reproduced in the repository during M0:
   - the Pi harness instrumented through a Storage proxy (commit cadence, sizes, critical path);
   - the `serde_json` probes (`RawValue` fidelity, the depth limit, base64 canonicality);
   - the line model;
   - the compaction simulation;
   - the normalization microbenchmark;
   - the mixed-version re-normalization probe;
   - the sparse-mark probe;
   - the executable key-schema model.
10. **Benchmarks and targets:**

| Benchmark | Target |
|---|---|
| `L_append` through ursulagw, disk WAL, Pi mix | mean ≤ 5 ms, p50 ≤ 4, p99 ≤ 20, p999 ≤ 60 |
| submit→provider | N=1 ≤ 4·L̄ + 5 ms; N=16 ≤ 100 ms at ≥ 130 commits/s |
| P1+P2 normalization | ns/byte ≤ today's path, 127 B–56 KB |
| Local read on the Session line | p99 ≤ 100 µs; 256-row page ≤ 1 ms |
| keyed-state point read, warm / cold | p50 ≤ 6 ms / ≤ 80 ms |
| Harness-level open | p50 ≤ 250 ms, p99 ≤ 1 s; ≤ 1.2× from 1k to 100k records and from 10 to 1,000 conversations |
| First submit after open | p50 ≤ 300 ms |
| Remote reads on the line, steady-state turn | 0 |
| Compaction | ≤ 6× flushed bytes, flat for 0.5–2 GB; ≤ 1 part-granular oldest-run merge/day in (a) |
| S3 per streaming harness | projection ≤ 50 PUT-class/h and ≤ 50 GET/h per namespace; log ≤ 50 PUT-class/h and ≤ 250 GET/h per active harness (per-group counters); flat in history; idle 0 |
| Replicated state per active harness per replica | ≤ 32 KB + 16 B/MiB of history |
| Indexer ingest per pod | ≥ 50 MiB/s and ≥ 20k records/s |

## 12. Rejected alternatives

**Force-fits named by the principles:**

1. Keeping the pointer index and doing N random record reads to fetch values: each cold point read costs about a 1 MiB ranged GET, and the pattern is N+1.
2. Declaring the projection in the 16 KiB attrs `metadata`: attrs have no CAS, no ordering against records, and are uninterpreted by contract.
3. Double-encoding Pi payloads as JSON strings to dodge canonicalization. P1 fixes ingestion instead.
4. Putting a growing live set into the replicated snapshot.
5. Trimming the log so the projection becomes authoritative, with pointer "checkpoint" snapshots guarding retention, a two-slot pinned window, and idle finalize:
   - it makes a new, medium-high-risk engine the sole copy of history;
   - its rebuild window is seconds to minutes;
   - S3-versioning PITR is unusable under its own invariants;
   - the checkpoint pointer contradicts `extensions.md` §2's definition of a snapshot as a fold;
   - replicated memory stays bounded only while the indexer is alive;
   - it pulls the projection outside Ursula's erasure, quota and DR envelope.

   Never-trim plus C1/C2 is the root fix.
6. Reusing staged external objects as projection values. Under never-trim this becomes viable as value-by-reference for values ≥ 1 MiB, deferred with the trigger "large-value bytes > 50% of a namespace and compaction > 5×".

**Other rejected or deferred options:**

- A server-side extraction spec (JSON pointers, key templates) or a hybrid. The writer must denormalize anyway: it needs old values, document scope and history, `copy` materialization with UTF-16 Chord semantics, and a global ID registry. Format errors would surface only at asynchronous ingest, and a second evaluator would be needed in TypeScript.
- Multi-put; a gateway HRW keyed-state pool with 503 failover; `HEAD`/`prefix=`/`op_index`/status headers; 409 "blocked" and "incarnation-mismatch"; 410; a public `Stream-Created-At` header.
- Generation pinning, horizons, exact as-of reads; a server-side tail overlay.
- A read-only non-owner Pi reader (deferred: Pi hosts read through the worker).
- `Producer-*` for exactly-once: receipts grow, epochs are client-chosen, and the duplicate check runs before `record_match`. Also rejected: a new fencing primitive, and logical fencing inside the log.
- A blob stream plus `$transaction`: inline Raft replication, base64, ≤ 64 ops, undocumented acks. Out-of-band S3 writes: owner-held credentials and lost atomicity.
- Owner-built Parquet parts (two format implementations); a projection inside the Raft state machine.
- Per-registration tailers, or keeping catalog polling, whose pool mode costs ≥ 5 S3 requests per idle registration per 250 ms pass today.
- Paging the dense record index into S3 cold-index pages: page writes are not rolled back and lookups become asynchronous. Sparse in-state marks achieve the bound with no page change.
- Lease records in the log for `fail-if-active` (permanent log growth). A per-bucket catalog keyed stream: the specified `extensions.md` §1.4 listing plus attrs cover it.
- JSON fidelity as a per-stream opt-in; `preserve_order`; a `Stream-Record-Format` header; State Protocol one-change-per-record.
- Full merges at ≥ 8 runs (write amplification linear in history); restarting long merges after a CAS conflict.
- A pre-proposal "not applied" write marker header. Deferred: under the §7.6 error policy it would only lengthen retries, never change an outcome.
- Occupying slot N with an empty batch to resolve ambiguity. It adds a second mechanism and saves nothing over read-back-then-resend once staging is inline.

## 13. Decisions needed

**Q1. Never trim keyed logs.**
- (a) Never trim: the log is the source of truth and the projection a rebuildable cache. Cost: S3 for the full log (§9.5, ≈ $0.0008 per month per streaming hour at most) and the bounded-state workstream through B4 (§6.2), which is general Ursula work that every long-lived stream needs. Every engine bug is then repairable by rebuild.
- (b) Trim with a first-class retention-hold primitive (≈ +180 LoC, gated), a time-based window at least as long as the verify cadence, a restore procedure, and no trimming before indexer DST. Most of the bounded-state work is still needed, because retention bounds neither the TTL heap, producer state, capacity slack nor the cold-path defects. Choose (b) only if O(1) footprint per idle harness is required.
- **Recommendation: (a).**

**Q2. P1 as the default for all `application/json` streams.**
- (a) Default: one code path. It fixes key sorting, f64 number rewriting and lone-surrogate rejection for everyone. It is an observable change and needs a release note; the gate is DS conformance 300/300.
- (b) Per-stream opt-in: two code paths.
- **Recommendation: (a).**

**Q3. Activation and names.**
- Activation: (a) `application/json; profile=keyed-batch-v1`, keeping generic JSON mode but deviating from RFC 6838/6906; or (b) `application/vnd.ursula.keyed-batch+json`, standards-clean, but generic clients lose JSON mode.
- Names: `keyed-batch-v1`, `keyed-state-v1`, the resource `keyed-state`, headers `Stream-Keyed-Through` / `Stream-Keyed-After`, rows `application/vnd.durable-stream-keyed-rows+ndjson`, parameter `min_through_record`.
- **Recommendation: (a) with these names.**

**Q4. Default takeover semantics.**
- (a) `fail-if-active`: refuses when the current claim is not closed and the log tail advances within `W` = 3 s. It matches today's Pi hosts, whose lockfile refuses a second open. A crashed owner costs `W` once. An idle-but-alive owner is taken over; its next commit gets `FencedError`.
- (b) `fence`: immediate; meant for supervised workers.
- **Recommendation: (a) as the default; hosts with their own lease use (b).**

**Q5. New contract limits for Pi.**
- 32 MiB per commit (`StorageRejected`). Lifting it needs a cross-request atomic commit primitive.
- Record depth ≤ 127, i.e. value depth ≤ 121–124 depending on wrapper; SQLite allows about 1000.
- Server key cap 4096 octets. Pi strings over 1 KiB are hashed, so Pi sees no length limit.
- No op-count limit.
- Liveness: an overlay cap of 256 MiB couples owner liveness to the indexer (§9.7).
- Deployment, until C6: per-group admission caps of at least 64 MiB (standard or large preset), so a 32 MiB inline commit can be admitted (§3.3).
- **Recommendation: accept all of these for v1.**

**Q6. Latency lever order if M0c misses.**
1. Ursula internal knobs: adaptive group-commit linger, NVMe/io2 WAL, AZ-aware owner and gateway placement, and the `record_match` fast-fail before staging (C9), which answers most doomed appends with 412 before they are proposed.
2. Pi-side cross-conversation partial coalescing (an optional upstream change; about N-fold fewer commits at N concurrent conversations).
3. #87 append sessions plus pipelined commits.

Single-AZ voters are excluded: they give up AZ fault tolerance and turn an AZ loss into a restore. **Recommendation: this order.**

**Q7. Upstream strategy.**
- (a) Propose P1 now; keep `keyed-batch-v1` and `keyed-state-v1` as Ursula extensions until v1 is validated (after M4), then present them as related to #404. They are not wire-compatible with the State Protocol.
- (b) Propose everything now.
- **Recommendation: (a).**

**Q8. Where the TypeScript package lives.**
- (a) The Ursula repo, as one package: protocol, server and client evolve together through M0–M3, and CI has a real node; Pi conformance runs against a pinned Pi version.
- (b) A separate repo.
- (c) A contribution to the Pi monorepo, where Pi's types and conformance are native, but Ursula CI must pin it.
- **Recommendation: (a) through M4; offer it upstream to Pi afterwards.**
