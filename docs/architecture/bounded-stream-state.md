# Bounded Per-Stream State for Long-Lived Streams

Related documents: [`raft-wal.md`](raft-wal.md), [`deterministic-simulation-testing.md`](deterministic-simulation-testing.md), [`sqlite-vfs.md`](sqlite-vfs.md). Specs: `extensions.mdx` §2 and §5, `durable-stream.mdx`, `concepts/exactly-once-writes.mdx`, `operations.mdx`.

## 1. Scope and principle

A stream may live forever, receive many tiny records and never advance retention. Its footprint must still stay bounded. This covers replicated state on every replica, the group snapshots built from it, node-local state outside the state machine, per-request memory, and the S3 objects that state points to.

The principle: nothing a replica holds grows with the number of records, appends, flushes or producers a stream has ever seen. Per stream, replicated state is a small constant plus the unflushed hot window plus bounded producer state. Per node, nothing is keyed by what was ever seen. Retention is never required. It remains a storage-cost tool for applications that want it.

Each mechanism below has a stable label (F0 to F19, D1 to D6, W1 to W6, I1 to I5). Code comments cite these labels, so they do not change when the text around them does.

Rationale in one paragraph: long-lived streams of tiny records (agent trajectories, chat and session logs, workflow event logs, telemetry) are the product's history, and their owners do not want to trim. The state machine is in memory on every replica by design and local storage is disposable, so nothing can be paged out. Any per-record or per-flush term in replicated state turns into replica memory, snapshot size, snapshot install time and Raft log amplification that grow with the stream's age.

## 2. Invariants

For a stream `s`, `H(s)` is its unflushed payload bytes and `P(s)` is the number of distinct producer ids it holds (at most 4,096).

**I1. Per stream, replicated, on every replica.**

```text
state(s) ≤ C0 + H(s) + Prod(s)

C0      ≤ 32 KiB + visible snapshot (inline body or a ~150 B reference, F16)
          metadata, counters, ≤ 64 shared pack refs (F2),
          ≤ 16 staged external refs (F5), one node-local TTL heap entry (F8)
H(s)    = unflushed payload bytes (also capped per group by admission)
Prod(s) ≤ 0.4 KiB · P(s) + 56 KiB     (0 for streams without Producer-* headers)
```

`Prod(s)` covers each producer's id and its fixed-size record (scalars, the newest write's receipt, up to two digests, the closed epoch and the floor marker), plus the stream's epoch floor (F3). The constant term is slack. No term grows with record count or with cold history.

**I2. Per group.** The sum of I1 over the group's streams, plus three terms. The hot window is capped by the 64 MiB per-group admission cap (F6c). Shared-pack maps hold at most one entry per live pack. The cold-GC queue stays bounded while GC is healthy (F14b). Tenant tombstones are the one documented exception (F15).

**I3. Snapshots.** A group snapshot encodes I2 in a binary envelope that does not amplify it (F12a). Cadence follows bytes (F12e): a group snapshots after max(F, 2 × its last snapshot) bytes of Raft log, so snapshot bytes written per appended log byte stay near one half and never grow with history.

**I4. Per node.** No structure keyed by something ever seen (streams, pages, lookups) grows without a cap. Caches are bounded by bytes or entries (F13). Per-request memory is at most the 8 MiB response cap or one record (at most 32 MiB), and no write copies group state (F9, F11). The TTL heap holds at most two entries per live TTL stream (F8). Hot payload across the node stays near `flush_pressure_hot_size` (128 MiB) while S3 is healthy (F10), and the per-group admission cap is the hard bound. Unpurged Raft log stays near the node log-byte budget and never passes twice the budget, where admission refuses client writes (F12e).

**I5. Independence.** No bound depends on `AdvanceRetention`, a visible snapshot, TTL, a client, or an external consumer. The constants 64 (F2) and 16 (F5) for state-held references depend only on Ursula's own leader-side drivers. A stalled driver shows in the gauges (§7.4, Gauges), and nothing else grows while it is stalled.

## 3. Definitions

- **Seal point** `p(s)`: the offset of the first hot byte, or the tail when nothing is hot (`StreamSlot::seal_point`). Every byte of `[retained, tail)` that the hot buffer does not hold is cold and immutable: everything below `p(s)`, and external appends that sit above hot bytes (F18).
- **Hot window**: the unflushed suffix of a stream, held in the hot buffer as 64 KiB blocks (F6b). It may have gaps where external appends sit above hot bytes.
- **LF message boundary**: on a JSON stream, a message boundary is an offset whose preceding byte is LF. Replicated state keeps nothing per message. Snapshot and retention offsets on a JSON stream must be boundaries: apply checks the preceding byte when it is hot. Otherwise the HTTP layer reads it from the leader and proposes again pinned to the stream incarnation, and a read that fails answers 503. Other content types have no message boundaries.
- **Incarnation**: every create assigns `created_at_ms := max(now_ms, last_created_at_ms + 1)` within the group, so a stream recreated under the same name gets a new, unique incarnation. Cold-index pages live under generation = incarnation, and exclusive chunk names carry it (F14g).

## 4. Mechanisms

Each subsection names the structure, its bound, the driver that enforces it, the constants, and the module.

### F0: `TidyStream` and the expiry sweep

`TidyStream { stream_id, now_ms }` normalizes one idle stream's producer state: it expires idle producers and evicts producers over the cap (F3), then shrinks the producer map's capacity. Each command does bounded work (at most 4,096 idle producers and 1,024 cap evictions), so a large backlog converges over several commands without stalling apply. It is idempotent and answers `StreamTidied { debt_remaining }`.

The leader-side driver (`ursula-runtime/src/tidy_worker.rs`) runs every 60 s on every node, with or without a cold store. In each group the node leads it proposes `TidyStream` for at most 64 streams with debt (`StreamStateMachine::tidy_candidates`), in stream-id order, and repeats on later passes until no debt remains. Debt is derived from persisted fields and `now_ms` only (`stream_has_tidy_debt` in `ursula-stream/src/state_machine/producers.rs`).

The same driver sweeps expired streams (`ursula-runtime/src/expiry_sweep.rs`, #669) with the dedicated `ExpireStreams { now_ms, limit }` command. A command that names an expired stream removes it: a create over it, an append, external append or close to it, a snapshot publish or retention advance on it, a delete of it and a read's `TouchStreamAccess`. No other command removes a stream it does not name, so `ExpireStreams` is how every other expired stream leaves. Every quarter of `raft.deleted_data_max_age`, between 1 s and 60 s, each group the node leads proposes `ExpireStreams { now_ms, limit: 256 }` while the front of its TTL index is due (`StreamStateMachine::expiry_due`, F8), at most 16 entries per group per pass, and starts none after 250 ms in that group. A pass runs the groups one after another, so its worst case is the groups the node leads times 250 ms and one commit. `now_ms` is the leader's clock less a second, so a renewal sent at the deadline by a node whose clock is that much behind still wins. The apply removes up to `limit` streams expired at `now_ms`, earliest expiry first, then by bucket and stream id. The removed set is a function of replicated state, `now_ms` and `limit` only, so every replica, restored or replayed, agrees, and a stream renewed after the leader looked stays. A stale front costs at most one entry that removes nothing, since the apply discards or re-arms it. A pass that stops at its 16 entries or its 250 ms while streams are still due leaves the group capped, and the driver runs the next sweep a second later instead of after the interval, with 50 ms per group so a draining group does not hold client writes past the latency goal. A pass that stops on an error keeps the interval. A group whose `TidyStream` step fails still reports its sweep, so one failing group does not stop the re-runs of the others. The sweep writes through the engine's write path, with its apply-health, history and leadership checks, and keeps no state between passes. A pass on a follower proposes nothing. Reads, `HEAD` and writes check expiry at their own `now_ms`, and `StreamStateMachine::head_at` never removes, so an expired stream leaves state only through a replicated command and no answer depends on when the sweep ran.

### F2: pack-reference compaction

**Structure.** Each packed flush adds one shared `ColdChunkRef` per participating stream, plus group refcount entries per live pack.

**Bound.** At most T = 64 shared refs per stream, plus the refs one driver interval adds. Group maps hold at most the live packs.

**Driver.** The shared-ref compaction driver (`ursula-runtime/src/runtime/shared_ref_compaction.rs`) runs on the leader with the cold compaction worker, which is on by default (`compaction_enabled`). Discovery is a state query (`ursula-stream/src/state_machine/cold_refs.rs`): streams with at least 64 shared refs, or with one shared ref and a tail unchanged for an hour (`SHARED_REF_IDLE_MS`, tracked leader-locally), fewest live slices per pack first. The driver repairs the stream's pages (F19), takes the oldest contiguous run of shared refs up to `compaction_max_size`, range-reads the slices bypassing the read cache, writes one exclusive chunk, and proposes `CompactCold` with `gc_not_before = now + compaction_gc_grace` (300 s). Both engines apply a shared-to-exclusive `CompactCold`. On a definite rejection the engine rolls back the page entries and the driver deletes the replacement. An ambiguous outcome keeps both, and F14h reclaims the replacement if nothing references it. Releasing a pack's last ref queues the pack for GC with the grace delay.

### F3: producer state bounds

**Structure.** Per stream, a producer map of `ProducerState`: the current epoch, its highest applied sequence, the newest write's receipt (start, end, whether it closed the stream, and its `Producer-Digest`), the claim's digest, the epoch the claim closed with that epoch's last sequence, and `last_seen_ms`. Per stream, one more field: `retired_epoch_watermark`, the epoch floor.

**Bound.** At most 4,096 producers, one fixed-size record each. One receipt per producer and no receipt window. `Prod(s)` in I1.

**Mechanism** (`ursula-stream/src/state_machine/producers.rs`):

- **One receipt per producer.** A repeat of the producer's newest write gets that write's receipt. A repeat of an older write of the epoch answers `204` with `Producer-Outcome: receipt-evicted` and no range headers: the record's highest applied sequence says it applied, so it never appends again.
- **Identity.** A record holds at most two 32-byte digests, the newest write's and the current epoch's claim's, and only when those writes carried `Producer-Digest`.
- **Closed epoch.** A record remembers the last sequence of the one epoch its current epoch's claim closed. A write of an older epoch answers `stale-epoch`.
- **Epoch floor.** Retiring a record, by idle expiry, the producer cap or F0, raises `retired_epoch_watermark` to at least its epoch. A write with `Stream-Incarnation` from a producer without a record, at or below the floor, answers `403 producer-expired` with the floor. Without the token a forgotten producer is new, and each record carries one marker, opened above or below the floor: a tokened write on a record opened below answers `producer-expired` until it claims above the floor. A record opened above the floor holds epochs above every retired epoch of its producer, so its claims are never checked against the floor again.
- **Idle expiry.** A producer whose `last_seen_ms` is at least 7 days older than the command's `now_ms` is treated as absent at its next write and removed then. F0 removes idle producers in bulk.
- **Producer cap.** A new producer beyond 4,096 evicts the least recently seen producers idle for at least an hour, at most 1,024 per command. When none is idle that long the write fails with `ProducerLimit` (`429`).
- **Length caps.** `Producer-Id` and `Stream-Seq` are capped at 256 B on every HTTP write path and at apply.

The spec pages state the answers: `extensions.mdx` §5, write keys and typed answers.

### F4b: no per-message state

Replicated state keeps nothing per message. There are no message records and no record index, and the snapshot fields that held them are reserved. Log entries carry nothing per message either. An external create or append carries the number of messages in its staged payload, which `committed_records` counts, so its entry does not grow with its body. JSON boundaries are LF boundaries (§3). `/bootstrap` follows the honest-partial rule: it answers from the hot range `[S, tail)` when `S` is at or above max(seal point, retained) and every byte of the range is hot, and with the snapshot alone otherwise. A non-JSON stream gets the range as one part when it is at most 8 MiB. A JSON stream gets one part per message, split on LF, up to the last LF within 8 MiB.

### F5: external payload locators

**Structure.** `StreamColdState.external_segments`: the `ObjectPayloadRef` of each committed `AppendExternal` (bodies of `external_payload_min_size`, 1 MiB by default, or more).

**Bound.** At most T_ext = 16 staged refs per stream plus the ones in flight.

**Mechanism** (`ursula-stream/src/state_machine/external_locators.rs`, `ursula-runtime/src/cold_refs.rs`). Apply pushes the ref into state, so the locator exists exactly when the append committed. The engine writes no page before proposing. The offload worker (`spawn_cold_ref_offload_worker`) runs every 2 s on every node with a cold store, not behind a config switch. For each led group it takes up to 64 streams with more than 16 staged refs or one older than 10 s (`STAGED_EXTERNAL_REF_MAX_AGE_MS`, from the write time in the object name). It writes their page entries with a read-modify-write that clips every overlapping entry (F19), then proposes `OffloadColdRefs`, whose apply removes exactly the listed refs still present and queues no GC.

**Cleanup.** The HTTP layer deletes a staged object only on a definite rejection, a deduplicated response, or a create that answers already-exists. Ambiguous outcomes keep it for F14h. The orphan sweep reads state refs before pages and the offload writes pages before it removes refs, so a ref in transit is always seen in one of them.

### F6a, F6b, F6c: hot window

- **F6a.** `HotBuffer` keeps a running payload counter, so no path rescans the buffer per append. Write responses carry the stream's hot backlog.
- **F6b.** Appends coalesce into blocks of up to `HOT_BLOCK_BYTES` = 64 KiB, each with its own start offset (`ursula-stream/src/state_machine/hot_buffer.rs`). A block ends at a gap or when full. Reads binary-search blocks, and a flush drops whole blocks and trims at most one. Snapshots emit one hot segment per block, so every replica holds the same layout after the same history. Overhead is one block header per 64 KiB, not one per append.
- **F6c.** Admission (64 MiB per group) and flush thresholds count hot payload. With F4b and F6b nothing else is kept per message, so payload is the real cost.

### F7: capacity hygiene

After removing elements, a vector or deque whose capacity exceeds 2 × len + 64 shrinks to 2 × len (`shrink_vec_if_slack`, `shrink_deque_if_slack`). This applies to the hot buffer and to cold chunk and external ref lists. The registry boxes each `StreamSlot`, so a vacant `SlotMap` slot costs a pointer, and shrinks its keys map after deletes.

### F8: TTL index

**Bound.** At most two heap entries per live TTL stream. Node-local, not replicated.

**Mechanism** (`ursula-stream/src/state_machine/registry.rs`). The registry keeps one armed entry per stream and records its expiry in `armed`. A refresh pushes only when the stream has no armed entry or its expiry moved earlier. An armed entry that pops before the stream's current expiry is re-pushed at that expiry. Every entry `pop_expired` returns is at its stream's true expiry with every smaller key already processed, so `ExpireStreams` expires the same streams in the same order as one entry per refresh would. Stale entries are discarded lazily, and the heap is rebuilt from `armed` once it holds more than twice as many entries.

**Front check** (`StreamRegistry::ttl_front_due`). The expiry sweep (F0) reads only the heap's front, in O(1). A front not due means no stream has expired, exactly: every live stream with an expiry has one armed entry at or before its true expiry, and the front is the smallest entry. A due front may be stale, of a stream removed or renewed since, and the next `ExpireStreams` discards or re-arms it.

### F9: engine bookkeeping

The per-stream append count lives in `StreamSlot`, so it dies with the stream on delete, TTL expiry or bucket purge. Single-node admission uses the O(1) `check_cold_write_admission` instead of cloning the engine to preview a write.

### F10: flush planner

**Mechanism** (`ursula-stream/src/state_machine/flush_planner.rs`, `ursula-runtime/src/cold_worker.rs`). The planner works from a derived index of streams with hot bytes, reads each stream's size from the F6a counter before copying payload, and sorts once per pass.

- **Fair batches.** A candidate gets at most the remaining batch budget, and a flush may end mid-chunk, so a stream that does not fit never stops the pass. Among equally large streams, a leader-local cursor rotates the starting point.
- **Group drain.** A group at or above its flush threshold flushes streams largest first until it falls below half the threshold.
- **Node pressure.** Once hot bytes across the node's led groups reach `flush_pressure_hot_size` (128 MiB), each group drains, largest first, its proportional share of the excess over three quarters of the watermark.
- **Maximum hot age.** A stream whose hot tail has been hot for `flush_max_hot_age` (5 min by default, observed leader-locally) is flushed whole, even below the threshold. A tail stays hot at most about twice the age plus one pass interval.

The index and cursor are not replicated. Restore rebuilds the index from the restored hot buffers.

### F11: reads and bootstrap

Every read is capped at `READ_MAX_RESPONSE_BYTES` = 8 MiB (`crates/ursula/src/lib.rs`). A capped read ends at the cap with `up_to_date = false`, inside a message if the cap falls there. `/bootstrap` plans one read window instead of one plan per message, materializes it outside the state machine, and stops at the cap with a continuation in `Stream-Next-Offset` (F4b gives the part rules).

### F12a to F12e: snapshot pipeline

- **F12a, binary envelope.** `SnapshotPointer` and `PersistedSnapshot` are MessagePack with `serde_bytes`. Decoders refuse the old JSON envelope.
- **F12b, S3 by default.** The snapshot backend `auto` (the default) picks S3 snapshots whenever the cold store is S3 and inline otherwise (`resolved_snapshot_backend`). Under madsim `auto` stays inline.
- **F12c, no extra copies.** The frame iterator borrows the group snapshot, and install decodes once.
- **F12d, non-blocking permit.** `try_create_snapshot_builder` try-acquires the node-wide build permit and returns `None` when it is busy, so no apply worker waits on another group's build.
- **F12e, byte-based cadence** (`ursula-raft/src/snapshot_cadence.rs`). A group snapshots once the log it applied since its last snapshot reaches max(F, 2 × that snapshot's size). F is the node log-byte budget (1 GiB by default) divided by twice the group count, between 64 KiB and 16 MiB (4 MiB at 128 groups). 100,000 entries (`raft.snapshot_backstop_logs`) remain only as a far backstop, on a node without `raft.peers` as on a cluster node. The driver takes every snapshot: OpenRaft's own snapshot policy is off on every group, and the driver always runs (a zero `storage.snapshot.drive_interval` is refused at startup). Once the node's groups hold three quarters of the budget, a pressure pass snapshots the groups with the most log first until the node would fall to half the budget. At or past the budget it escalates and keeps more builds in flight, in even steps up to the node's build concurrency (2 by default) before the hard limit of twice the budget, where a cluster node's admission refuses client writes. Groups whose live log pins old WAL segments go first. Removals (#669) count too: the bytes a deletion, bucket purge or TTL expiry removed since a group's last snapshot count toward its threshold, and a removal no snapshot covers makes its group due a quarter of `raft.deleted_data_max_age` after it applied. Below the pressure watermark such groups lead a pass, replayed removals first and then the oldest. Under pressure they plan as due groups after the groups that free the most log, so removal debt never holds back log relief. Removed bytes never count toward the node's log, its pressure or admission. An expired stream is removed by its leader's expiry sweep (F0) within a minute, or sooner by a request that names it, and its time starts then. Once a snapshot covers a removal, or after a restore, the driver purges the group's log through that snapshot, except while the group's Raft is not running. Every purge, the admin route's included, holds the group's builds until it lands: until the log is purged through the snapshot, or the group applied `max_in_snapshot_log_to_keep` entries past it, after which any build's own purge reaches it (`SnapshotBuildCoordinator::hold_for_purge`). A removal's debt lasts until the purge lands. A replica times a removal from when it first applied it, also across a restart: a removal it applies again while it replays its log is due at once, and a restored snapshot is purged through.

### F13: node caches

The cold-index page cache is bounded by pages and by approximate bytes (`DEFAULT_COLD_INDEX_CACHE_BYTES` = 32 MiB). Its LRU deque is rebuilt from the live pages once it holds more than twice as many entries, which is amortized O(1) per lookup. One page cache per group serves reads, bootstrap and forwarded reads, so apply-time invalidation covers all of them. The cold read cache prunes idle `readers` entries by last-read generation once the map reaches max(4 × cache blocks, a fixed minimum).

### F14a to F14i: cold-object hygiene

- **F14a, external prefix in stream GC.** Stream delete reclaims the stream incarnation's `external/` objects with its chunks and pages.
- **F14b, GC worker.** One group's GC failure does not stop the others. `DeferColdGc` moves a failing head entry to the tail with exponential backoff, so one bad entry cannot block the FIFO.
- **F14c, no tiny exclusive objects.** A lone flush candidate below 1 MiB is packed alone as a pack of one, and F2 bounds the resulting refs.
- **F14d, compaction debt** (`ursula-runtime/src/runtime/compaction_debt.rs`). Paths that create small exclusive chunks record the pages they touched, at most 65,536 pages per node. The same-stream compactor drains that set and lists nothing. The F19 repair cursor refills it for idle streams after a failover.
- **F14e, stale flushes.** On a definite rejection the Raft flush path rolls back its page entry and deletes the chunk.
- **F14f, retention GC** (`ursula-runtime/src/retention_gc.rs`). Only deployments that trim need it. The F19 repair cursor notes each stream's retained offset. Once that observation is older than the retention grace (300 s), it deletes the pages wholly below the offset that hold only entries below it, then the objects only those pages name. It never writes the boundary page, so up to about one page span (64 MiB) of objects below the offset stays per stream. Direct external refs that retention drops from state are queued for GC at apply with the same grace.
- **F14g, incarnation-scoped objects.** Pages live under generation = incarnation and exclusive chunk names carry the incarnation. Stream GC entries carry the incarnation and delete only that incarnation's names. The worker refuses a stream entry without one and deletes nothing. The name of every new chunk, pack, external payload and snapshot part ends in its write time in nanoseconds and 64 random bits (`cold_store.rs`). Whatever the clocks and restarts of the nodes, two names collide only when they share the nanosecond and the 64 random bits.
- **F14h, orphan sweep** (`ursula-runtime/src/runtime/orphan_sweep.rs`). Every 600 s each led group sweeps up to 16 streams after its cursor, plus its pack directories at the start of a cycle, and deletes objects older than one day that no state ref, page or GC entry references. It feeds the `cold_orphan_*` counters.
- **F14i, retention grace.** Retention releases dropped pack slices with `RETENTION_COLD_GC_GRACE_MS` (300 s), replicated as the GC entry's not-before time, so a read planned before the retention still finds its bytes.

### F15: tenant tombstones (known exception)

`bucket_usage` keeps one row per bucket ever written in each group, and `erased_buckets` keeps one fence per bucket ever purged in every group. Both grow with tenant churn, not with records, and both are deliberate. They are the one exception to I2. The `bucket_usage_rows` and `erased_buckets` gauges expose them.

### F16: visible snapshots in the cold tier

A snapshot body above the staging threshold (the smaller of `external_payload_min_size` and the 32 MiB request limit) is streamed into an object under the stream's `external/` prefix while being hashed, and published with `PublishSnapshotExternal` (`crates/ursula/src/cold_snapshot.rs`). State holds about 150 B per cold snapshot instead of its body. One request carries at most 32 MiB. A larger snapshot, up to `MAX_COLD_SNAPSHOT_BYTES` = 1 GiB, goes up in parts (`crates/ursula/src/snapshot_parts.rs`): each part is staged as a `.part` object in the group's part directory of the bucket, `{bucket}/_parts/{group:08x}/`, named by its id and a hash of the stream and its token, which no state ref ever names. The commit copies the parts in order into one new `.bin` object under the stream's `external/` prefix while hashing it, four pieces read ahead, then proposes the same `PublishSnapshotExternal`. The copy keeps replicated state, reads, cold GC and import unchanged. The commit runs in a task the node owns, so a client that goes away cancels nothing, and it deletes its parts after its answer once that answer is final for its key, or after a digest mismatch. F14h lists each part directory with the pack directories at the start of a cycle, at most 4,096 names per directory, one more LIST request per bucket and group, so it reclaims a part that no commit used once it is older than the grace, whether its stream still exists or not. A node runs two copies at once. Each is charged against the in-flight body budget once it holds its slot, until the copy ends, and starts only while one 32 MiB request still fits beside it: 56 MiB with four pieces read ahead at the default 256 MiB budget, 32 MiB with one at 64 MiB, the least budget a cold-backed node accepts, which therefore copies one commit at a time. Each part directory is walked from a cursor, 4,096 names a cycle, so every part is visited within a bounded number of cycles. Reads and `/bootstrap` stream the object in 8 MiB pieces outside the state machine, and a ranged read only the pieces its range covers. Apply queues a superseded or removed body for GC with the F14i grace, and F14h treats the visible body as a state ref. The parts a stream holds staged and unused are bounded too, at 8,192 parts and 2 GiB, twice what one commit can use (`crates/ursula/src/staged_parts.rs`). The parts of its earlier incarnations count until they are deleted. A part past either bound answers `429` before its body is read. The count is node-local state of the leader and is never replicated: one tally per stream, set by one prefix LIST of the stream's run of names in the part directory when the node has no fresh tally for the stream's incarnation in the current Raft term, after every commit and discard, and at a refused part when the last listing is over a minute old. A part's or a discard's listing, which a fresh `HEAD` anchors, also deletes the parts of other incarnations staged more than a minute before it. A commit's listing only counts. A delete of the stream deletes, after it answers, the parts of the incarnations the leader knew before the delete at any age and its other parts staged more than a minute before it. The margins assume the nodes' clocks agree within a minute. Every listing holds the stream's lock, and the parts that finish uploading during it are added on top of it. A node runs one commit or discard per stream incarnation at a time, so two commits never spend the same parts. The orphan sweep tells the tracker what it reclaimed, and the tally gives it back and lists again at the stream's next part. After a leader change the new leader misses the parts still uploading to the old one, at most what the old leader's in-flight body budget held. Those parts stay over the bound until a commit, a discard, a delete or the sweep removes them. A node tracks at most 16,384 streams that no request holds, about 400 B each and about 6.5 MiB in all, plus an index of their part-name prefixes, and evicts the least recently used, whose next part lists again. A slot a request holds is never evicted, so the map can pass the cap by the slots in use, which request concurrency bounds. A listing holds the names and sizes of at most 16,384 parts while it runs, and at most four stream deletes clean up at once. For usage the leader also keeps per-group totals (`crates/ursula/src/staged_parts/groups.rs`): a background listing of each led group's part directories seeds them when the node starts to lead, woken by the group's Raft metrics, and the same paths keep them up to date afterwards. They hold one entry of about 100 B per stream with staged parts in a led group, removed at zero, and are dropped when the node stops leading the group. A purge drops the bucket's tallies and totals. Without a cold store every body stays inline under the 32 MiB cap. `ImportSnapshot` copies the reference, not the object, and the gateway still buffers request bodies up to `--max-request-body-bytes`.

### F17: hardening

- **WAL reclaim.** The segmented journal deletes sealed segments no group needs, and once it holds more than twice its live bytes it frees the oldest segment by rewriting small remainders forward and reporting groups with large ones as lagging (`ursula-raft/src/log_store/reclaim.rs`, [`raft-wal.md`](raft-wal.md)). F12e snapshots lagging groups first, and their purge frees the segment. A segment that held dead entries for a quarter of `raft.deleted_data_max_age` is freed the same way though the journal is within its budget (#669), driven by the snapshot driver's maintenance passes, at most an eighth segment per core per drive interval. A lagging group that applied nothing since its snapshot is purged through it.
- **Admin copies.** Group snapshot export takes one transient copy of the group per call, bounded by call rate.

### F18: cold coverage derived from the hot buffer

Coverage is the complement of the hot buffer. In read planning and in the restore check, every byte of `[retained, tail)` that no hot block holds is cold, served by state refs where they exist and by cold-index pages otherwise. There is no scalar cold frontier, and its snapshot field is reserved. F5's `OffloadColdRefs` can always drop a ref, because its range is never hot.

### F19: page-entry hygiene

- **Clip on proven writes.** A page write for a range whose bytes state proves (a flush of hot bytes, an F2 replacement, an F5 offload) removes or clips every other entry overlapping the range in the same read-modify-write.
- **Repair cursor** (`spawn_cold_index_repair_worker`). Every 60 s each led group repairs the pages of up to 16 streams after its cursor. Per page it keeps only the last-written external entry at each start offset, and drops external entries that overlap a chunk entry or another object's state ref, start at or beyond the tail, or whose objects predate the stream's incarnation. The cursor also drives F14d and F14f. Two overlapping page-only external entries with different starts written before F5 cannot be told apart and remain a documented hazard.

## 5. Structure, bound and mechanism

| Structure | Scope | Bound | Mechanism |
|---|---|---|---|
| Stream metadata, counters, visible snapshot | replicated | C0 (inline body ≤ 32 MiB, or a reference) | F16 |
| Hot window | replicated | payload plus one header per 64 KiB block, ≤ 64 MiB per group | F6a, F6b, F6c, F10 |
| Per-message boundaries | replicated | none kept, in state or in log entries | F4b |
| Shared pack refs and group pack maps | replicated | ≤ 64 + one interval per stream, ≤ live packs per group | F2 |
| Staged external refs | replicated | ≤ 16 + in flight per stream | F5 |
| Producer map and epoch floor | replicated | ≤ 4,096 producers, one receipt each | F0, F3 |
| Cold coverage | replicated | derived, no field | F18 |
| Cold-GC queue | replicated | bounded while GC is healthy | F14b |
| `bucket_usage`, `erased_buckets` | replicated | O(buckets ever written or purged) | F15 (exception) |
| Group snapshot | replicated | encodes the above, cadence by bytes | F12a to F12e |
| Unpurged Raft log | node | near the log budget, hard limit 2 × budget on a cluster node (a single node has no write gate) | F12e, F17 |
| TTL heap | node | ≤ 2 entries per live TTL stream | F8 |
| Staged-part tallies | node | ≤ 16,384 unheld streams, LRU, plus held ones | F16 |
| Staged-part group totals | node | one entry per stream with staged parts in a led group | F16 |
| Staged snapshot parts in S3 | S3 | ≤ 8,192 parts and 2 GiB per stream id, plus a leader change's in-flight parts, for at most the sweep grace | F14h, F16 |
| Registry slots and keys map | node | O(live streams) | F7 |
| Per-stream append count | node | dies with the stream | F9 |
| Flush planner index and cursor | node | O(hot streams) | F10 |
| Page cache and readers map | node | pages and 32 MiB, max(4 × blocks, minimum) | F13 |
| Compaction debt | node | ≤ 65,536 pages | F14d |
| Read and bootstrap responses | request | ≤ 8 MiB, or one record ≤ 32 MiB | F11 |
| Exclusive chunks, packs, externals, pages in S3 | S3 | O(live history) | F2, F14a to F14i, F19 |

## 6. Regression catalog

Each defect class is pinned by tests that drive the production entry points.

| Label | Defect class | Fix | Pinning tests |
|---|---|---|---|
| D1 | A scalar cold frontier moved backwards after hot bytes, an external append above them and a flush of the hot prefix, so the external became unreadable and snapshots could not restore | F18 | `d1_read_plan_serves_external_bytes_above_a_flushed_hot_prefix` (ursula-stream), `d1_runtime_reads_and_installs_external_above_a_flushed_hot_prefix` (ursula-runtime), `openraft_snapshot_with_regressed_frontier_builds_and_installs` (ursula-raft) |
| D2 | `/bootstrap` skipped bytes between a checkpoint and a later flush point and still answered up to date | F4b, F11 | `bootstrap_after_cold_flush_past_snapshot_does_not_skip_messages` (ursula-stream) |
| D3 | Page entries of rejected external appends served another object's bytes, or surfaced after compaction | F5, F19 | `d3_flush_clips_the_page_entry_of_a_rejected_external_append`, `external_create_of_a_live_stream_leaves_its_pages_alone`, `f2_rejected_external_append_under_a_packed_trickle_reads_correctly_after_compaction` (ursula-runtime) |
| D4 | Stream GC after a delete and recreate deleted the new incarnation's objects | F14g | `d4_recreate_with_gc_pending_keeps_new_incarnation_objects` (ursula-runtime) |
| D5 | Objects of ambiguous publishes leaked with no reclaim path | F14h | `f14h_orphan_sweep_reclaims_only_unreferenced_objects_after_the_grace` (ursula-runtime) |
| D6 | Retention released dropped pack slices with no grace, failing reads planned before it | F14i | `f14i_retention_keeps_dropped_pack_slices_for_the_grace` (ursula-runtime) |

## 7. Verification

### 7.1 State-probe harness

`crates/ursula-state-probe` (`publish = false`) measures bounded state with a counting global allocator (requested bytes, live blocks, and every live allocation of 64 KiB or more) and sizes group snapshots with the production codec (`ursula_raft::group_snapshot_frames`).

- **L1** drivers run the real `StreamStateMachine` with the commands the runtime issues.
- **L2** drivers run the real `ShardRuntime` on the in-memory and the single-node OpenRaft engines, plus a shared `CompactCold` probe (`l2_compact`).
- **W1**: one JSON stream of 200 B inline appends with cold flushes (`w1` with a forced flush before each checkpoint, `w1_hot` without).
- **W2**: slow streams sharing packed flushes, including the reduced starvation reproduction (`w2_starve`) and the F2 driver (`w2_driver`).
- **W3**: external-only streams with 1 or 5,000 records per append.
- **W4**: producer headers with one producer, epoch bumps, or many producer ids.
- **W5**: churn through deletes, TTL expiry, the TTL heap and bucket purge.
- **W6**: the other workloads with periodic retention.
- **planner**: deterministic counters for one flush-planning pass over many hot streams, and the W2 starved-stream shape.
- **cadence**: real state machines with round-robin appends under the production F12e policy, reporting snapshot bytes per appended log byte (target at most 0.6) and the largest node log against the budget. `cadence_w1` is one heavy stream on a 128-group node, `cadence_g128` is a uniform 128-group workload.
- **wal**: real Raft log stores on one segmented journal, checking that WAL heap stays near the per-group entry caches plus the index, not the retained log.

Every workload uses distinct stream names, so counts and snapshot sizes are deterministic. Output is JSONL per checkpoint, and `summarize.py` renders it.

### 7.2 CI ratchet

`.github/workflows/bounded-state.yml` runs two jobs.

- **state-growth** runs on every pull request and push to `main`. It runs the reduced `pr` suite (`gate::pr_suite`: W1, W3 to W6, the W2 starvation and driver runs, planner, L2, cadence and wal) and compares it with `crates/ursula-state-probe/ratchet.json`. The ratchet holds 123 metrics and 73 formula targets with a tolerance of 0.1. A metric above its baseline plus 10% fails, and one below its baseline minus 10% is reported for tightening. A target recorded as met that is lost fails, and a newly met target is reported. Metrics or targets missing on either side fail, so coverage cannot shrink silently. A `workflow_dispatch` with `write_ratchet` regenerates the file.
- **nightly** runs the full-scale `nightly` suite (W1 at 3M records, W2 over 24 simulated hours, W4 and W5 at 1M appends, L2 120-minute runs on both engines, cadence at full scale, and W6 variants) and publishes JSONL as artifacts. It gates nothing.

The formula targets (`crates/ursula-state-probe/src/formula.rs`) check per stream: shared refs at most 64 plus one interval (F2), producers at most 4,096 and producer bytes within `Prod(s)` (F3), staged external refs at most 16 (F5), TTL heap entries at most twice the TTL streams (F8), heap slack after flush or retention within the tight size plus 64 KiB (F7), residual snapshot state growing at most 1 KiB between N and 4N records on W1, F12e snapshot bytes per log byte and node log within budget, F2 shared `CompactCold` on both engines, F10 maximum stream hot on W2, and WAL heap within index and caches. One target is recorded as unmet: `planner.f10_starved_stream_planned`.

### 7.3 DST invariants

The bounded-state seed families in `crates/ursula-sim/src/madsim_harness/tests.rs` check two invariants on top of the correctness invariants in [`deterministic-simulation-testing.md`](deterministic-simulation-testing.md).

- **Invariant 11, no stale locators.** After any ambiguous external append, offload or compaction, readable bytes equal acknowledged bytes on every replica, and inputs of a compaction that never committed are not deleted. Tests: `external_locators_survive_ambiguous_commits`, `external_locators_survive_snapshot_install_with_staged_refs`, `external_locators_survive_ambiguous_compaction`.
- **Invariant 12, identical producer state.** After the same log prefix every replica holds the same producer records and epoch floor, whether it replayed the log or installed a snapshot. Test: `producer_state_matches_after_snapshot_install_mid_stream`.

`ci.yml` runs a few seeds of each on every pull request. `dst-nightly.yml` sweeps larger seed ranges of the `external_locators_survive` family through the `*_SEEDS` variables.

### 7.4 Gauges

`StreamStateMachine::state_gauges` (`ursula-stream/src/state_machine/gauges.rs`) walks a group's streams on demand, so scrapes pay for it and apply does not. `/__ursula/metrics` exports it per group as `group_state_gauges`, from local state on leaders and followers: streams, shared refs (total and per-stream maximum), live packs, staged external refs, producers (total and per-stream maximum), producer bytes (total and per-stream maximum), streams with an epoch floor, TTL streams and TTL heap entries, hot payload bytes, hot blocks and their overhead, pending cold-GC entries, bucket usage rows and erased buckets. Per group the same endpoint exports log bytes since the last snapshot and the last snapshot's size, and per node `raft_log_bytes`, the pressure-flush counters, the `cold_orphan_*` counters and `producer_expired_answers`. No alerts ship with them.

## 8. Design rationale

- **Retention is not required.** Requiring it would discard the history these streams exist for (#41) and make Raft memory depend on an application or consumer staying healthy. It would also leave producer state and the TTL heap unbounded.
- **No local KV store.** Persisting the state machine in RocksDB or similar is a non-goal. Nodes stay close to stateless, and S3 cold-index pages are the sanctioned offload (F5, F14).
- **Coverage from the hot buffer.** Keeping a scalar cold frontier and raising it with `max` would fix D1's reads but accept intra-message offsets below an external append. The complement of the hot buffer is exact and needs no field (F18).
- **Incarnation names over a recreate ban.** Rejecting a recreate while stream GC is pending would couple create availability to GC health, and the new stream would still share the old one's page keys during a sweep (F14g).
- **One receipt per producer, no window.** The base protocol answers an older duplicate `204` with the epoch's highest accepted sequence and requires no range for it, and a pipelining client needs only `Producer-Expected-Seq` and `Producer-Seq`. Keeping each producer's newest receipt covers the common retry, so a window of older receipts would add a derived structure, its rebuild on restore and a trim budget for ranges no client reads (F3).
