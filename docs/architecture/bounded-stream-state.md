# Bounded Per-Stream State for Long-Lived Streams

Status: Accepted 2026-10-02.

Scope: make the memory and snapshot footprint of every Ursula stream at most a small constant, plus its unflushed hot window, plus 16 bytes per MiB of cold history, without requiring retention. The footprint then no longer grows with record count, but it is not independent of history: it still grows by about 16 MiB per TiB of cold history, per stream and per replica. A history-independent bound is a follow-up (§3, I1). This covers replicated state on every replica, the group snapshots built from it, node-local state outside the state machine, per-request memory, and the S3 objects that state points to. It also fixes the cold-path correctness defects that the audit and its adversarial review found, because several fixes build on them.

Related: issue #17 (stable memory under cold storage) and PR #15 (cold metadata moved into cold-index pages); issue #84 and PR #91 (record coordinates; #84 anticipated a sparse index); #146 (producer receipts); #164, #182, #184 (pack references in replicated state); #278 (legacy pack migration); #57, #58, #167 (TTL sweeps and renewal); #190 (incremental group hot gauge); #194, #198, #212, #252 (snapshot cost and cadence); #111, #274 (WAL reclaim); #210 (the eviction rule); #41 (agent trajectories keep full history); #170 (framed binary records); `docs/architecture/json-record-coordinates-validation.md`; `docs/architecture/raft-wal-production.md`; `docs/architecture/deterministic-simulation-testing.md`; specs `extensions.mdx` §2 and §6, `durable-stream.mdx`, `concepts/exactly-once-writes.mdx`, `operations.mdx`.

Conventions: paths are relative to the repository root, and line numbers refer to commit `e6d8d70` (0.5.1). Figures marked *measured* come from the bounded-state probe that milestone B0 commits (§7.1). It drives the real `StreamStateMachine` with the exact commands the runtime issues (L1) and the real `ShardRuntime` with both the in-memory and the single-node OpenRaft engine (L2), and it counts requested heap bytes with a counting allocator, so real RSS is somewhat higher. Defects marked *reproduced* come from the adversarial probes, which also drive the real `ShardRuntime`; B0 commits them as regression tests. Unmarked figures are estimates.

## 1. Summary

1. A stream that lives forever and receives many tiny records grows per-stream state without bound unless the application advances retention. Five replicated structures grow with history: the dense record index (8 B per JSON record), shared pack references (about 250 B per packed flush), producer receipts (about 120 B per append within an epoch), message records on streams that only receive external appends (16 B per record), and `Vec` capacity that is never returned. One node-local structure on every replica, the TTL index, grows by about 104 B per append, and not even retention bounds it.
2. *Measured*: one stream of 3M records of 200 B holds a 33.5 MB index buffer on every replica and puts 13.6 MB into each 18.9 MB group snapshot. A default-config group of 200 slow streams reaches 1.72 GB of heap and a 584 MB group snapshot after 24 hours, because the flush planner starves one stream and then packs every other stream every second.
3. The cold path also has correctness defects on the default configuration (§4.2), all *reproduced*. The worst: when a stream holds small hot bytes, receives an append of 1 MiB or more, and then flushes, its cold frontier moves backwards; the large append becomes unreadable, and no group snapshot holding the stream can be restored or installed. Others: `/bootstrap` silently drops bytes after a checkpoint that retention did not follow; cold-index pages keep entries of rejected large appends, which later serve wrong bytes; stream GC after a delete and recreate deletes the new incarnation's objects. They are fixed first, in B1, and ungated where possible.
4. Target: per stream, replicated state is at most C0 + 8 B per unflushed record + 16 B per MiB of cold history, plus bounded producer state, with C0 about 32 KiB. That stops growth with record count; growth with history remains at about 16 MiB per TiB of cold log per stream per replica, and removing it is a follow-up (offload marks into cold-index pages, about 128 B per GB, or thin old marks to 8 MiB). Per node, nothing is keyed by what was ever seen. Retention is never required.
5. The record index keeps exact offsets only for records that are not yet flushed, plus one `(record, offset)` mark per 1 MiB block of cold log. A JSON record boundary is exactly an LF in the stored bytes, so a cold lookup scans at most one block. Apply never stores a record number it cannot recompute: retention into cold history lands on the mark at or below its target.
6. Pack references compact through the existing `CompactCold`. The Raft engine lacks the all-shared branch that the in-memory engine has; adding it also fixes the legacy-pack migration that bucket purge runs. Every page write for a range whose bytes state proves clears the other entries overlapping it, so compaction never exposes a stale entry.
7. Producer receipts become a per-stream window of 1,024 items plus each producer's newest acknowledgement, and idle producers expire; duplicates beyond the window answer `204` without ranges, as the base protocol says. Message records collapse at every cold transition. External payload locators are committed first and moved into cold-index pages afterwards, so state holds only the in-flight ones.
8. Two other defects get their own fixes: the flush planner's head-of-line starvation with its O(S²) planning, and the per-append TTL heap. Node-local leaks (page-cache LRU deque, cold-read `readers` map, engine append counts), unbounded per-request materialization, quadratic bootstrap planning and byte-blind snapshot cadence are fixed alongside.
9. Replicated changes are gated by a group feature level. Four levels keep the risky changes apart, so sparse marks ship alone, and their behavior-preserving parts ship ungated first.
10. Milestones: B0 harness; B1 correctness defects and ungated fixes; B2 pack-reference driver, orphan sweep and decode support; B3 level Lb1 (state hygiene); B4 level Lb2 (sparse marks); B5 level Lb3 (external locators); B6 hot window and snapshot cadence; B7 hardening with a 72-hour soak gate. About +5,800 / −400 production LoC and +6,500 test LoC, twice the first estimate; #91, which built the dense index, took about 1,600 production lines alone.
11. This is general Ursula work and can start now.

## 2. Problem

### 2.1 The workload

Some streams are the product's history. Agent trajectories read from the latest checkpoint but keep every earlier event for reflection, debugging and fork-and-rewrite (#41). Chat and session logs, collaborative documents, workflow event logs (the consumer #146 was built for) and browser telemetry (the event-time index example) look the same: records of tens to hundreds of bytes, streams that live for months, many streams per Raft group, and owners who do not want to trim.

Today they have to trim anyway. `operations.mdx:135` says "There is no automatic retention policy. Application checkpoints and explicit stream-retention advances control which cold objects become eligible for GC." Retention needs a published snapshot first (`crates/ursula-stream/src/state_machine/cold.rs:333-352`), discards the history these applications exist to keep, and makes Raft memory depend on an application or a sidecar staying healthy. It also does not bound everything: receipts, producer ids, the TTL heap and `Vec` capacity survive it.

### 2.2 What grows

Section 4 lists every source. The ones that dominate long-lived streams of tiny records, all *measured*:

- **Record index.** `StreamRecordIndex.record_offsets` holds one `u64` per retained JSON record, hot or cold (`crates/ursula-stream/src/record_index.rs:6-10`). Heap is 8.4 to 11.2 B per record because the buffer doubles: 33.5 MB of capacity at 3M records. Snapshots spend 2.9 to 4.6 B per record as absolute-offset varints: 13.6 MB of an 18.9 MB snapshot at 3M records. `FlushCold` never touches it. Retention clones it first (`cold.rs:368-385`) and drains without shrinking (`record_index.rs:227`): after trimming 990k of 1M records, 8.04 MB stays allocated for 10k records.
- **Pack references.** Each packed flush adds one shared `ColdChunkRef` per participating stream (`state_machine/cold_state.rs:26-31`): 227 B tight and 421 B with slack in memory, 166 B in snapshots, plus about 260 B of heap and 84 B of snapshot per live pack in the group maps. At 30 KB/s per group that is 308 refs per stream per day, and 1,625 per day under node pressure flushing.
- **Flush-planner starvation.** With the default `flush_size = flush_max_size = 8 MiB`, the drain pass stops at the first candidate that does not fit and always walks streams in the same order (`cold.rs:71-72, 167`). One stream's hot bytes then grow until the group's admission cap starts rejecting writes, and the group stays in drain mode, packing every other active stream every second. For 200 streams at 30 KB/s per group, onset came at about 15 h. At 24 h: 16.3k refs per stream, a 584 MB group snapshot (234 MB with zstd), 1.72 GB of heap, and 44 s of flush CPU per hour. It reproduces through the real runtime on both engines (onset at about 65 min with 50 streams at 2 KB/s).
- **Producer receipts.** One receipt per append within an epoch (`state_machine/append.rs:1005`): 1M appends cost 133.5 MB of heap (against 18.7 MB without producers) and 35.9 MB of a 48.1 MB snapshot. A duplicate of the newest sequence scans linearly from the oldest: 0.95 ms at 1M receipts. 100k producer ids with 10 appends each cost 188 MB of heap.
- **Message records.** 16 B per JSON record (or per binary append) until the next `FlushCold`. External appends never collapse them (`append.rs:589-594`): 1,000 appends of 5,000-record bodies leave 123 MB of heap and a 91 MB snapshot, 67.4 MB of it message records.
- **TTL index.** Every append to a stream with `Stream-TTL` or `Stream-Expires-At` pushes a new heap entry with cloned ids, even when the expiry did not change (`state_machine/registry.rs:126-138`). That is 104 B and 3 allocations per append. 1M appends add 104 MB, which survives `DeleteStream`. At 10 appends/s under a 7-day TTL this is about 1 GB per stream on every replica.
- **Hot window overhead.** One `HotChunk` per append plus a message record plus a dense offset is about 64 B of tight overhead per unflushed record (more with allocator slack), several times the payload for tiny records. Admission and flush thresholds count payload only.

### 2.3 Why it matters

**Replica memory.** Every voter and learner holds every stream of its groups in memory. The state machine is in memory by design and local memory is meant to be disposable (#61, #268), so nothing can be paged out. A W2-shaped group needs 1.72 GB after one day on every replica.

**Group snapshots.** Snapshots are full rebuilds triggered by entry counts, never by bytes. Without a snapshot store, openraft snapshots every 5,000 to 10,000 entries (`crates/ursula-config/src/config.rs:205`; `crates/ursula-raft/src/engine/factory.rs:99-110, 686-697`). With one, the manual driver triggers at 5,000 entries, and a node pressure pass snapshots up to 16 groups whenever unpurged entries across the node reach 65,536, about 512 per group at the default 128 groups (`crates/ursula/src/bootstrap/snapshot.rs:34-53, 181-208`; `config.rs:187, 206-207`). So snapshot I/O per append grows with history: 1.9 to 3.8 KB of snapshot per Raft entry for one stream at 3M records, 10 to 19 times the 200 B payload. #194 measured p99 of 458 to 467 ms with 5,000-entry snapshots at about 524K streams against 59 to 79 ms with snapshots off. The default inline backend amplifies everything (*measured*): the pointer kept in memory per group is 3.7 times the raw snapshot, the disk-WAL metadata file is 11 times raw on every build, and decoding peaks at about 35 B per raw byte. Followers receive the pointer as one gRPC message capped at 256 MiB, so a group whose raw snapshot passes about 69 MiB (about 17M records with the dense index) can no longer be shipped to a lagging follower at all.

**Restart and install.** Memory-WAL voters rejoin only through snapshot install, against a 120 s install timeout (`config.rs:200`). Install time and peak memory scale with snapshot size.

**S3 objects.** Slow streams produce one exclusive object or pack slice per flush pass. A pack stays whole while any slice is referenced. External payloads are never deleted when their stream is deleted, and objects kept after ambiguous publishes are never reclaimed. Compaction discovery lists the whole cold root every 30 s on every node.

### 2.4 How we got here

The goal is not new. #17 (2026-06-05) set it: "Fixed number of live streams should have stable memory usage over time", "Cold metadata should not grow forever just because a stream keeps flushing to cold storage", and retention or TTL "can be lazy, but once triggered it must fully reclaim the related in-memory state". PR #15 delivered it by moving per-chunk and external references into S3 cold-index pages and collapsing message records after flush. Three July features then put per-record or per-flush growth back into replicated state, each as a local decision: the dense record index (#91, whose cost `json-record-coordinates-validation.md:64-66` records as accepted), exact producer receipts (#146) and pack-slice references (#182). This document restores #17's goal and extends it to the structures #17 did not name.

## 3. Target invariants

Definitions for a stream `s`:

- **Seal point** `p(s)`: the offset of the first hot byte, or the tail when nothing is hot. Every byte of `[retained, tail)` that the hot buffer does not hold is cold and immutable: everything below `p(s)`, and external appends that sit above hot bytes (F18).
- **U(s)**: records (JSON) or appends (other content types) that start at or above `p(s)`.
- **K(s)**: MiB of retained log below `p(s)`.
- **P(s)**: distinct producer ids seen within the producer idle period.

**I1. Per stream, replicated, on every replica:**

```text
state(s) ≤ C0 + H(s) + 8 B · U(s) + 16 B · ⌈K(s)⌉ + Prod(s)

C0      ≤ 32 KiB + inline visible snapshot
          metadata, counters, ≤ 64 shared pack refs,
          ≤ 16 staged external refs, plus one node-local TTL heap entry
H(s)    = unflushed payload bytes (also capped per group by admission)
Prod(s) ≤ 0.4 KiB · P(s) + 56 KiB     (P(s) ≤ 4,096; window of 1,024 receipt items)
          and 0 for streams without Producer-* headers
```

In the shorthand of "C0 + C1 · unflushed records + C2 · cold MiB": C0 is about 32 KiB, C1 is 8 B plus the record's payload, and C2 is 16 B.

I1 does not grow with record count, but its last term grows with history: 16 B per MiB is about 16 MiB per TiB of cold log, per stream, per replica. This document commits to that bound, not to one independent of the stream's age. **Follow-up for history-independent replicated state:** offload marks into cold-index pages, leaving state about 128 B per GB of cold log, or thin marks older than a cutoff to one per 8 MiB (Q10). Neither is part of B0–B7.

**I2. Per group.** The sum of I1 over the group's streams, plus: hot-window real memory (payload plus boundaries) at most the 64 MiB admission cap once admission counts real memory (F6c); shared-pack maps at most one entry per live pack, which is at most 64 per stream; a cold-GC queue bounded while GC is healthy and alerted otherwise. The one documented exception is tenant tombstones, which are O(buckets ever written or purged) by design (F15).

**I3. Snapshots.** A group snapshot encodes I2: about 100 B per stream, about 9 B per cold MiB, and the hot window. No representation amplifies it (F12). Cadence follows bytes (F12e): a group snapshots after max(F, 2 × its last snapshot) bytes of Raft log, so snapshot bytes written per appended log byte stay near one half, averaged over a flush cycle, and never grow with history. A node pressure pass may snapshot earlier to keep unpurged log within the node's log-byte budget; it spends that budget on the groups that free the most log per snapshot byte.

**I4. Per node.** No structure keyed by something "ever seen" (streams, pages, lookups) grows without a cap. Caches are bounded by bytes or entries. Per-request memory is at most the response cap (8 MiB) or one record (at most 32 MiB), and no write copies group state (F9). The TTL heap holds at most two entries per live TTL stream. Hot payload across the node stays near `flush_pressure_hot_size` (128 MiB) while S3 is healthy (F10); the per-group admission cap is the hard bound. Unpurged Raft log stays within the node log-byte budget (F12e). Marks cost 16 MiB per TiB of cold history across the node's groups; a gauge and an alert track them, and Q10 fixes the threshold at which older history is thinned.

**I5. Independence.** No bound depends on `AdvanceRetention`, a visible snapshot, TTL, a client, or a sidecar such as an indexer. The constants 64 and 16 for state-held references depend only on Ursula's own leader-side drivers; a stalled driver is an alert, and nothing else grows while it is stalled.

**I6. Equivalence.** Every client-visible response stays byte-identical, except where this document says otherwise: producer-state bounds, including duplicates beyond the receipt window (F3); retention into cold history taking effect at the mark at or below its target, and `400` for JSON snapshot or retention targets at intra-record cold offsets, which the spec already requires (F1); bootstrap parts split per JSON record, and bootstrap no longer skipping bytes after a checkpoint (F11); one bootstrap part for the cold history of binary streams that only receive external appends (F4a); and server-side response caps (F11).

Retention is never required for boundedness. It remains a storage-cost tool for applications that want it. `operations.mdx:135` must say so.

Worked example: a JSON stream of 100M records of 200 B (20 GB) with a 1 MiB hot tail. Today it holds 800 MB of index (up to 1.07 GB with doubling slack) on every replica and about 460 MB of index in every group snapshot. Under I1 it holds 32 KiB + 1 MiB of payload + 42 KB of dense offsets + 305 KB of marks, about 1.4 MB.

## 4. Inventory

### 4.1 Growth sources

Every growth source the audits found, replicated or not, with the fix that bounds it. "Repl." means the structure is part of replicated state or group snapshots; node-local structures are still paid on every replica that runs the code. Costs are *measured* unless marked "est.".

| # | Structure (location) | Repl. | Growth | Cost | Bound today | Fix |
|---|---|---|---|---|---|---|
| 1 | Dense record index `record_offsets` (`record_index.rs:6-10`) | yes | O(records), hot and cold | 8 B/record tight, 8.4-11.2 B heap, 2.9-4.6 B in snapshots; 3M records: 33.5 MB buffer, 13.6 of 18.9 MB snapshot | retention only; capacity kept; retention clones it | F1, F7 |
| 2 | Shared pack refs `cold_chunks` and group `shared_cold_object_refs/owners` (`cold_state.rs:26-31`; `state_machine.rs:115-122`) | yes | O(packed flushes) | 227 B tight, 421 B with slack, 166 B snapshot per ref; 260 B heap and 84 B snapshot per live pack | retention or delete; Raft engine rejects shared `CompactCold` | F2, F10 |
| 3 | Producer receipts (`append.rs:1005`) and `last_items` (`model.rs:120`) | yes | O(appends per epoch) | 115-131 B heap, 24-36 B snapshot per append; a batch receipt up to about 28 KB, duplicated in `last_items` | epoch bump | F3 |
| 4 | Producer map (`state_machine.rs:143`) | yes | O(distinct ids) | about 380 B per id; id length unbounded | stream delete | F3 |
| 5 | `message_records` (`state_machine.rs:138`) | yes | O(records since `FlushCold`); forever on external-only streams | 16 B/record heap, 12-15 B snapshot; 5,000-record external body: 120 KB heap, 91 KB snapshot per append | `FlushCold` or retention; collapse keeps capacity | F4, F7 |
| 6 | Hot window chunk overhead (`hot_buffer.rs:7-17, 88-97`) | yes | O(unflushed appends) | about 64 B tight per record beyond payload; 2.6 MB deque kept after one flush window | flush thresholds and group cap, both payload-only | F6, F7 |
| 7 | External locators in state (planned by an earlier design) `external_segments` (`cold_state.rs:9`) | yes | O(external appends) if shipped as written | est. 120 B snapshot, 170 B heap per append of 1 MiB or more | retention only | F5 |
| 8 | Visible snapshot payload (`model.rs:264-273`) | yes | O(1), up to the 32 MiB body cap; at Lb5 a reference above the staging threshold | inline on every replica and every group snapshot | replacement | F16 |
| 9 | `last_stream_seq` and producer id length (`append.rs:347-349`; `state_machine.rs:752-761`) | yes | O(1), length unbounded through `$transaction` JSON (removed in 0.6.0) | up to 32 MiB | replacement | F3 |
| 10 | Engine `stream_append_counts` (`ursula-runtime/src/engine/in_memory.rs:128`) | frames | one leaked entry per TTL-expired or purged stream | est. 150 B per removed stream | restart or snapshot install | F9 |
| 11 | TTL heap (`registry.rs:24-29, 126-138`; `ttl.rs:11-21`) | no | O(appends) on TTL streams | 104 B and 3 allocations per append; 1M appends: 104 MB that survives delete | each entry's own expiry | F8 |
| 12 | Registry capacity (`SlotMap`, keys map) (`registry.rs:24-29`) | no | O(peak streams) | 592-882 B per peak slot | restart or install | F7 |
| 13 | `bucket_usage` rows (`state_machine.rs:123-126`) | yes | O(buckets ever written), per group | about 200 B per bucket | none, by design (#258) | F15 |
| 14 | `erased_buckets` fences (`state_machine.rs:107-110`) | yes | O(buckets ever purged), in every group | 95-148 B per bucket per group | none, by design (#280) | F15 |
| 15 | Cold-GC queue (`state_machine/cold_gc.rs:13-64`) | yes | O(pending deletes) | 35-200 B per entry | GC worker; a stuck FIFO head blocks it | F14 |
| 16 | Group snapshot (full rebuild) | yes | O(group state) per 5,000-10,000 entries | 1.9-3.8 KB per Raft entry at 3M records; 584 MB at W2 24 h | state size | F1-F5, F12 |
| 17 | Inline snapshot envelope (`ursula-runtime/src/snapshot_store.rs:124-157`) | yes | O(state) times amplification | 3.7x in memory, 11x on disk, about 35 B per raw byte at decode; above about 69 MiB raw it cannot ship | none | F12 |
| 18 | Snapshot build clones and node-wide permit (`ursula-raft/src/state_machine.rs:591-612`) | n/a | O(state) per build; other groups' apply waits | one to two extra copies of group state per build | build concurrency 1 | F12 |
| 19 | Cold-index page LRU deque (`ursula-runtime/src/cold_index.rs:678-729`) | no | O(lookups) | 156-219 B per lookup; 1.1M lookups of one page: +223 MiB | only when pages exceed capacity | F13 |
| 20 | Cold-read `readers` map (`ursula-runtime/src/cold_store.rs:1182-1201`) | no | O(streams ever read) | 149 B per stream | restart | F13 |
| 21 | Read materialization (`crates/ursula/src/lib.rs:3156-3159`; `in_memory.rs:1004-1018, 1106-1185`) | no | O(history) per request | whole range without `max_bytes` or `max_records`, also first SSE and long-poll read | retention only | F11 |
| 22 | `/bootstrap` (`in_memory.rs:1203-1233`; `ursula-raft/src/engine/mod.rs:1003-1018`) | no | O(suffix after snapshot) per request; one plan per message record, O(hot records²) CPU, inside the Raft apply worker | whole suffix; est. ≥ 0.6 s of apply at a full W1 hot window (42k chunks × 14 µs) | snapshot offset | F11 |
| 23 | In-memory engine admission preview (`in_memory.rs:682-802`) | node memory, CPU | a full copy of the group engine per create and append (`:690, 729`) | 30k appends: 17.7 s with admission, 0.44 s without | none | F9 |
| 24 | `hot_payload_len` scan (`state_machine/query.rs:153-160`; `hot_buffer.rs:47-49`) | CPU | O(hot chunks) per append | 14 µs at 42k chunks against 0.8 µs for the apply | flush thresholds | F6 |
| 25 | Flush planner (`cold.rs:61-178`) | CPU and state | O(C x S log S) per pass; starvation lets one stream's hot bytes grow to the group cap | 182 ms and 1.83 s per drain pass at 1k and 3k streams; starvation as in §2.2 | none | F10 |
| 26 | Duplicate receipt lookup (`append.rs:931-948`) | CPU | O(receipts) per duplicate | 0.95 ms at 1M receipts | epoch bump | F3 |
| 27 | Ref sorting per read (`cold_state.rs:83-108`; `query.rs:264-278`) | CPU | O(state refs) per read and per frontier query | proportional to refs | refs | F2, F5, F18 |
| 28 | External objects on stream delete (`ursula-runtime/src/runtime.rs:1016-1021`) | S3 | O(external appends) | one object of 1 MiB or more per append | bucket purge only | F14 |
| 29 | Tiny exclusive chunks and whole-page rewrite per flush (`cold_index.rs:360-399`) | S3 | O(flush passes) objects; page I/O quadratic within a page | one object per pass from 1 B; one page GET and PUT per flush | compaction, off by default | F10, F14 |
| 30 | Packs pinned whole by any live slice (`runtime.rs:417-560`) | S3 | O(packs) | up to 8 MiB per pack, also when one idle slice remains | refcount reaches 0 | F2 |
| 31 | Objects kept after stale or ambiguous publishes (`runtime.rs:361-415, 537-546`; `ursula-raft/src/engine/mod.rs:1454-1475`) | S3 | O(stale or ambiguous flushes) | one chunk or pack per ambiguous publish; est. 170 B of page per stale flush | stream delete for chunks; nothing for packs | F14 |
| 32 | Compaction discovery (`runtime.rs:770-900`) | S3 requests | O(all objects) every 30 s per node | est. 86M LIST per day at 10M objects on 3 nodes | none | F14 |
| 33 | Objects below retention (`cold_state.rs:56-68`) | S3 | O(trimmed history) | chunks, externals, pages | none | F14 |
| 34 | WAL online reclaim (`ursula-raft/src/log_store/file.rs:548-575`) | disk I/O | full live-journal rewrite per purge once at least 64 MiB | full rewrite and fsync | live log size | F17 |
| 35 | Admin deep clones (`runtime.rs:905-924`; `crates/ursula/src/lib.rs:1986-2000`) | no | O(state) per call | one transient copy per group | call rate | F17 |
| 36 | Meta-group migrations (`crates/ursula-control/src/state.rs:334`) | meta | O(migrations ever) | one record each | none | F17 |
| 37 | Raft log held until purge (`config.rs:205-208`) | yes | O(entries since snapshot) | full payload per entry; triggers count entries, not bytes | snapshot cadence | F12 |

Already constant per stream: metadata (apart from row 9), `retained_offset`, the group hot gauge and the commit index. Bounded per node: read watchers (65,536 per core), the cold read block cache (256 MiB LRU with deque compaction), gateway leader and rate caches, metrics arrays, gRPC channel maps.

### 4.2 Correctness defects

Found on the way, independent of growth, and present at `e6d8d70` with default configuration. Several fixes in §5 depend on these, so they come first (B1).

| # | Defect (location) | Effect | Evidence | Fix |
|---|---|---|---|---|
| D1 | The cold frontier moves backwards: `push_cold_chunk` assigns it while `push_external_segment` takes the maximum (`cold_state.rs:26-36`) | after small hot bytes, an append of 1 MiB or more (externalized by default, `config.rs:94`) and a flush of the hot prefix, reads of the large append fail (they use pages only below the frontier, `query.rs:266`), and the group snapshot can be neither restored nor installed (restore requires the frontier to cover the suffix, `persist.rs:432-470`) | *reproduced* on both engines, restore and install included | F18 |
| D2 | `/bootstrap` keeps only message records that start at or after the snapshot offset (`query.rs:405`), but collapse puts the prefix into one record that starts at the retained offset | after a checkpoint without retention and a flush or collapse past it, bootstrap skips every byte between the checkpoint and the collapse point and still answers 200 with up-to-date | *reproduced* on both engines | F11 |
| D3 | Pages keep entries of rejected external appends: the pre-proposal write is never rolled back (`ursula-raft/src/engine/mod.rs:1258`), same-start entries with different ends are both kept (`cold_index.rs:479`), and reads merge entries by start offset (`:880-893`) | cold reads return another object's bytes; compacting shared refs into page entries exposes stale entries that the refs hid (`query.rs:264-334`) | *reproduced* on both engines; the compaction case on the in-memory engine | F19, F5 |
| D4 | Stream GC deletes by name prefix, and page keys carry no incarnation (`lifecycle.rs:598-604`; `runtime.rs:1016-1019`; `cold_store.rs:1316-1351`) | after a delete and a recreate under the same name, GC deletes the new incarnation's chunks and pages; until it runs, the new incarnation reads and extends the old one's pages; the recursive sweep of a two-segment stream also reaches an affinity stream under the same name | *reproduced* on the in-memory engine | F14g |
| D5 | Publishes with ambiguous outcomes keep their objects, and no orphan sweep exists (`runtime.rs:404-414, 537-546`; the `cold_orphan_cleanup_*` counters, `metrics.rs:413-415`, are never incremented) | chunks leak until stream delete, packs forever; packs live outside every stream prefix | code reading | F14h |
| D6 | Retention releases dropped pack slices with no grace (`cold.rs:706`) | a read planned before a concurrent retention fails with `NotFound` | code reading | F14i |

## 5. Fix designs

Each subsection gives the data-structure change, read and write path changes, snapshot codec changes, migration of existing state, gating, cost and tests. Section 5.20 summarizes gating, cost and milestones.

### 5.1 F0: group feature levels and `TidyStream`

**Why a level.** Old binaries would apply receipt eviction, message-record collapse, external staging and sealing differently, and cannot restore a sealed record index: `StreamRecordIndex::validate` requires the dense vector to start at the retained offset (`record_index.rs:231-256`). Commands travel as MessagePack maps with named fields (`crates/ursula-raft/src/codec.rs:26-38`): an old binary silently ignores a new optional field, which diverges quietly, and fails to decode a new variant, which fails loudly. `RAFT_GRPC_PROTOCOL_VERSION` is compared for strict equality (`crates/ursula-raft/src/grpc.rs:291, 757`), so bumping it breaks the graceful rollouts that #178 and #200 established. A per-group level makes the cut explicit: a new binary behaves exactly like its predecessor until every member supports the new level and an operator raises it.

**State, command and snapshot frame.**

- `StreamStateMachine` gains `feature_level: u32` (0 is today's behavior) and `feature_level_raised_at_ms: u64`.
- At any level above 0, group snapshots carry a new frame variant, `FeatureLevelV1 feature_level = 6` in `SnapshotFrameV1`, holding both. A binary without F0 decodes the unknown variant as an empty frame and refuses the snapshot (`required(frame.frame, …)`, `crates/ursula-raft/src/snapshot_codec.rs:55`). A header field would not do: prost skips unknown header fields, so a rolled-back binary would install the group and apply later commands with level-0 semantics. A binary with F0 refuses a level above its `MAX_SUPPORTED`. Backup exports carry the level, and `ImportSnapshot` refuses a level above the target group's.
- New command `SetFeatureLevel { level, now_ms }`. If `level` is at or below the current level, apply is a no-op success. If it is at or below `MAX_SUPPORTED`, apply sets it. Above `MAX_SUPPORTED`, apply is fatal and the node stops applying, which is also what an old binary does when it cannot decode the variant.
- Each gated call site checks one predicate, such as `self.feature_level >= LB1`. The check and the old path are deleted once the oldest supported release always runs at that level.
- A gated command never depends on an optional field that a proposer below the level would omit. At a raised level apply decides from state alone, so a command proposed before the raise and applied after it is decided identically on every replica.

**Raising.** `ursulactl cluster raise-feature-level --to N` reads each node's maximum supported level from the node admin info endpoint (a new field), requires every voter and learner of every group to support `N`, then proposes `SetFeatureLevel` to each group. The raise to Lb2 also requires every group to report a completed page-repair cycle (F19). Levels are never lowered. Membership changes refuse to add a node whose maximum is below the group's level. Once raised, binaries below the level cannot restore the group's snapshots, so downgrades are unsupported.

**Levels.** Levels follow release order, and each costs one predicate per call site. This document defines five:

- **Lb1, state hygiene:** F18's derived cold coverage, F3, F4a, `TidyStream`, F14a with F14g, F14b's `DeferColdGc`, F14i, and F12a emission.
- **Lb2, sparse marks:** F1.
- **Lb3, external locators:** F5.
- **Lb4, hot representation:** F4b.
- **Lb5, cold snapshots:** F16 (§5.16), feature level 5 (`FEATURE_LEVEL_COLD_SNAPSHOTS`).

Numbers are assigned at release, and a level may carry several items when they release together.

**`TidyStream { stream_id, now_ms }`** (Lb1) applies the normalizations that `FlushCold` and `AppendExternal` run inline: collapse message records (F4a), trim receipts and expire idle producers (F3), shrink capacities (F7), and from Lb2 seal the record index (F1). It is idempotent. Each command does bounded work, at most 1M records sealed and 64k receipts trimmed, so a legacy stream converges over several commands without stalling the group's apply. A leader-side maintenance driver issues it for streams whose derived debt exceeds a threshold (dense records below the seal point, receipts beyond the window, idle producers, capacity slack), at most 64 streams per group per minute, and repeats until no debt remains. This is how idle legacy streams converge after a raise without any O(group) apply.

**Cost and risk.** +600 production LoC including `ursulactl`, the frame, the join and import checks and the debt driver; +400 test LoC. Medium risk, mostly operational.

**Tests.** Raise refused when a member reports a lower maximum; raise applied and a fresh node installs a level-Lb1 snapshot; a decoder built from `e6d8d70`'s proto refuses a snapshot that carries the frame; restore above `MAX_SUPPORTED` fails closed; `SetFeatureLevel` replay is idempotent; a command proposed before a raise and applied after it decides identically on every replica; a madsim seed family with a test-only cap on a node's maximum level (§7.4).

### 5.2 F1: sparse cold record marks

**Observation.** For `application/json` streams every stored record is one compact JSON value followed by LF. `normalize_http_write_payload` serializes each message with `serde_json::to_writer` and appends `b'\n'` (`crates/ursula/src/render.rs:725-753`), compact JSON never contains a raw LF, and every write path derives record ends from LF positions with `canonical_json_record_ends` (`record_index.rs:46-65`; inline appends at `append.rs:258-268`; create and external paths at `crates/ursula-runtime/src/request.rs:80, 612`). The dense vector is therefore a cache of LF positions. Once bytes are cold they are immutable, and a mark per MiB plus a bounded scan reproduces the cache exactly, provided the cold bytes are right (F19).

**Data structure.**

```rust
pub struct StreamRecordIndex {
    first_record: u64,             // first retained record; meaning unchanged
    marks: Vec<RecordMark>,        // sealed records: one mark per 1 MiB block that contains a record start
    dense_first_record: u64,       // records at or above this have exact offsets
    dense_offsets: VecDeque<u64>,  // start offsets of [dense_first_record, next_record); prefix drains cost O(drained)
}
pub struct RecordMark { pub record: u64, pub offset: u64 }
const MARK_BLOCK_SHIFT: u32 = 20; // fixed by level Lb2
```

Invariants, maintained by apply and checked at restore where bytes are not needed:

- **M1.** `first_record ≤ dense_first_record ≤ next_record = dense_first_record + dense_offsets.len()`.
- **M2.** Sealed records exist exactly when `marks` is non-empty, and then `marks[0] = (first_record, retained_offset)`.
- **M3.** Marks strictly increase in both record and offset, and the last mark's record is below `dense_first_record`.
- **M4 (locality).** Every sealed record `r` starts in the same 1 MiB block as `mark_le(r)`. Equivalently, sealing emits a mark for each sealed record that starts in a later block than the previous mark.
- **M5.** Dense offsets strictly increase and are below the tail. With marks present, `dense_offsets[0]` exceeds the last mark's offset; without marks, `dense_offsets[0] = retained_offset`, as today.

**Sealing.** One function, `seal_below(p, budget)` with `p` the seal point (`hot_buffer.first_start_offset()` or the tail). It moves dense records whose end is at or below `p` into the sealed set, oldest first and at most `budget` = 1M records per call, emitting marks per M4, then drains the moved prefix and shrinks (F7). A record that straddles `p` stays dense; flushes can split an append mid-record (`hot_buffer.rs:131-136`). Records of an external append that sits above hot bytes also stay dense until the hot bytes below them flush. Apply calls it at the end of `FlushCold` (`cold.rs:402-518`), `AppendExternal` (`append.rs:419-597`), create with an external body, `AdvanceRetention` (retention can drop the hot bytes below an external append's dense records, so sealing there leaves no debt for the tidy driver), and `TidyStream`. The cost is amortized O(1) per record, and no apply spends more than about 5 ms sealing: a legacy stream's first seals after the raise proceed in 1M-record steps.

**Lookups.**

```rust
enum Locate { Exact(u64), Scan { from_record: u64, from_offset: u64, limit: u64 } }
fn locate_record(&self, r: u64, tail: u64) -> Result<Locate, RecordIndexError>;
fn locate_offset(&self, o: u64, tail: u64) -> Result<OffsetLocate, RecordIndexError>; // Exact(r) | Scan{..} | NotBoundary
```

For a sealed record `r`, let `m = mark_le(r)`. The result is `Exact(m.offset)` when `m.record == r`, and otherwise `Scan` with `limit = min(block_end(m.offset), next anchor offset)`, where the next anchor is the next mark, else `dense_offsets[0]`, else the tail. By M4, `start(r)` lies strictly between `m.offset` and `limit`, so no scan leaves one block. For an offset `o`: `o == m.offset` is exact; `o` at or beyond `block_end(m.offset)` but below the next anchor is `NotBoundary`, because it lies inside the last record that starts in `m`'s block; otherwise the scan counts LFs in `[m.offset, o)` and `o` is a boundary exactly when the byte before it is LF.

**Read path.** `read_stream_plan_after_access` (`in_memory.rs:956-1031`), which both engines use (`crates/ursula-raft/src/engine/mod.rs:814`), turns the two locations of `[r, r_end)` into a byte window plus `RecordTrim { skip, take }`. The window starts at the exact offset or at `m.offset` with `skip = r − m.record`. It ends at the exact end offset or at the end location's `limit`, which always contains the LF that ends record `r_end − 1`. Materialization (`request.rs:268-314`, `in_memory.rs:1106-1185`) becomes a record cursor. It pulls bytes segment by segment, counts `skip` LFs without copying, copies the next `take` records, and stops at the `take`-th LF, at the window end, or at the response cap (F11) with at least one complete record. It then rewrites `offset`, `next_offset`, `record_range`, and `up_to_date = (next_offset == tail)`. A bracketed plan never claims `up_to_date` before trimming, so the follower forwarding check (`ursula-raft/src/engine/mod.rs:846-856`) must use the trimmed result. Over-read is under 1 MiB before the first record and under one cache block after the last; both fall on 1 MiB cold read-cache blocks (`config.rs:472`). The CPU cost is a `memchr` over at most 1 MiB, about 50 µs.

Scans verify the anchors they cross (F19): the byte before each must be LF, and the LF count between two consecutive anchors must equal their record difference. A mismatch fails the read with a corruption error and a metric instead of returning shifted records.

Server-side continuations carry an anchor taken from their own previous response, in a new `ReadStreamRequest` field `record_anchor: Option<RecordAnchor { incarnation, record, offset }>` with `#[serde(default)]`. The `offset` field cannot carry it, because record reads send `offset = 0` (`lib.rs:3141-3142`). That covers the SSE loop (`lib.rs:3800-3850`), including the envelope view, which reads one record per iteration. The engine uses an anchor only if its incarnation matches (unique from Lb1, F14g) and it validates: a dense anchor must equal the dense offset, and a sealed anchor must lie in its mark's bracket with an LF in the byte before it, which the plan reads. Otherwise it resolves from the mark. The anchor is never parsed from client input, and nodes that do not know the field ignore it. Client loops of `?record=r&max_records=k` pay at most one front scan per request; a node-local anchor cache could remove that later (Q11).

Offset reads never consult the index and do not change. Record headers appear only on record reads (`render.rs:539-545`).

**Writes and acknowledgements.** `StreamResponse::Appended` gains `record_range: Option<StreamRecordRange>`, which apply fills from the prepared range it already computes (`append.rs:270-286, 476-492`). `StreamResponse` is not serialized, so this is a local change. Deduplicated responses carry the stored receipt's range (`ProducerDecision::Duplicate` already holds the items, `append.rs:1019-1025`), or the producer's newest acknowledgement when the duplicate is of the newest sequence and its receipt was evicted (F3). `record_range_for_append` (`query.rs:57-91`) stops consulting the index. Today it searches the dense vector for appends without a producer and as a fallback when a batch duplicate is not the latest receipt. After F1 that search would fail for sealed records, including an external append that sealed its own records in the same apply. After retention the fallback already fails today with a 500. Close acknowledgements keep using `next_record`. `record_match`, HEAD, `tail_records` resolution and every first/next record header need only `(first_record, next_record)`, which stays O(1).

**Retention and snapshot publish.** Apply never stores a record number it cannot recompute.

- **Dense target:** the exact check, as today.
- **Sealed target:** apply retains to `m = mark_le(o)`, the last mark at or below the target offset; that is exact when `o == m.offset`. Ordinals never change: `F` becomes `m.record`, and the response's `Stream-Retained-Offset` and `Stream-Record-First` report the effective boundary, as the retention route already returns them (`lib.rs:3456-3485`). At most one 1 MiB block below the requested boundary stays readable, a storage cost only. Retention mutates in place with no clone: it drops marks below `m`, or drains the dense prefix when the target is dense.
- **Leader-side checks.** The `?record=` routes resolve `O(r)` with a one-record read, as today (`resolve_record_offset`, `lib.rs:3388-3410`). On JSON streams the raw-offset routes check a sealed target with a bounded scan and return 400 for an intra-record offset before proposing.
- **Publish.** Apply checks dense targets exactly and accepts sealed targets at or below `p(s)`, as today's frontier rule does. The leader-side scan rejects intra-record cold offsets with 400, which closes today's gap in practice: `snapshot_offset_aligned` accepts any offset at or below the cold frontier (`cold.rs:670-686`), so a JSON snapshot at an intra-record cold offset is accepted, contrary to `extensions.mdx:751`.

Neither command gains a field. A wrong scan, from wrong cold bytes or from a delete and recreate between resolution and commit, can make the leader accept or reject the wrong request, as a stale offset can today, but apply still lands on a real boundary, so record coordinates stay exact on every replica. Retention landing below its target needs a one-sentence amendment to `extensions.mdx` §2.2 (Q4).

**Snapshot codec.** `StreamSnapshotEntryV1` gains `repeated uint64 record_mark_records = 17`, `repeated uint64 record_mark_offsets = 18` and `optional uint64 dense_first_record = 19`, and field 15 holds dense offsets only. Absent fields mean all-dense, so old snapshots restore unchanged. They are written only at level Lb2 or above; below it the index is all-dense anyway. The serde `StreamSnapshot` used by backup export (#154) gets the same fields with `#[serde(default)]`. A binary below Lb2 refuses such a snapshot through the level frame (F0) before it reaches `validate()`.

**Migration.** Nothing is rewritten. After the raise each stream seals at its next `FlushCold` or `AppendExternal`, at most 1M records per command, and `TidyStream` seals idle legacy streams.

**Gating.** Lb2, for the snapshot format and the apply representation. The behavior-preserving parts ship ungated in B1: acknowledgements carry the range apply computed; the record cursor and trim, which with exact locates only reproduce today's reads; and the RC-2 differential suite against the dense index. The level then only switches on sealing and the codec. Prerequisites: F18's coverage (Lb1) and a completed F19 repair cycle in every group.

**Cost.** +1,300 production LoC and +1,800 test LoC; #91 took about 1,600 and 2,000 for the dense form. Medium-high risk, because it touches every record-coordinate path; §6 is the mitigation.

**Effect.** 16 B per MiB of sealed log in memory and about 9 B per mark in snapshots, plus 8 B per unflushed record. W1 at 3M records (600 MB, 4.4 MB of it hot): about 570 marks (9.1 KB) plus 22k dense offsets (176 KB) instead of a 33.5 MB buffer, and about 5 KB of marks in the snapshot instead of 13.6 MB of offsets. W2's 200 streams over 24 h (about 2.6 GB): about 2,600 marks (42 KB) plus, under F10's maximum hot age, 150 to 225 dense records per stream (240 to 360 KB) instead of 33.1 MB of offsets. An earlier probe measured 2,000 B instead of 1,600,000 B for 200k fully sealed records over 124 MiB.

**Tests.** Unit tests for sealing at block boundaries, records larger than 1 MiB, mid-record seal points, the per-call budget, retention onto marks and an empty dense part. A property test against a dense oracle (§6). The `record_coordinates_reference.rs` oracle extended to persistence cases, as `json-record-coordinates-validation.md:15` requires. HTTP tests for each RC invariant. The `append_apply` benchmark reports hot and cold seek and aligned-read latency (`json-record-coordinates-validation.md:62`).

### 5.3 F2: pack-reference compaction

**Today.** Every packed flush pass adds one shared `ColdChunkRef` per participating stream (`cold_state.rs:26-31`) and group refcount and owner entries (`state_machine.rs:115-122, 163-211`). Only retention or delete removes them. The state machine's `compact_cold` already rewrites a contiguous all-shared run into one exclusive chunk and releases pack refcounts with a GC grace (`cold.rs:520-627`), and the in-memory engine drives it (`in_memory.rs:1889-1921`). The Raft engine instead always calls `replace_cold_chunk_index_pages_with_rollback` (`crates/ursula-raft/src/engine/mod.rs:1477-1503`). That function returns `None` for shared inputs because they are never in pages (`cold_index.rs:814-866`), so the Raft engine reports "cold compaction input no longer matches the cold index". *Measured* on both engines: in-memory OK, Raft ERR. The same bug breaks `migrate_legacy_shared_cold_once` (`runtime.rs:905-990`, which passes one shared chunk at `:966`) on every Raft cluster, so a bucket purge that finds legacy pack slices fails. The in-memory branch also writes the replacement's entry without clearing overlapping entries, which exposes stale ones (D3).

**Design.**

1. **Raft branch (B1).** Mirror `in_memory.rs:1896-1905`: for all-shared inputs, write the replacement's page entries with `write_cold_chunk_index_pages_with_rollback`, under F19's clip rule, and keep the existing `rollback_safe` rule. About 40 LoC, and a standalone bug fix.
2. **Driver (B2).** `compact_shared_refs_once` runs in the leader's cold worker for each led group every `compaction_interval`. It compacts a stream only right after repairing that stream's pages (F19), because turning refs into page entries uncovers whatever the refs hid.
   - **Discovery** is a state query, `shared_ref_candidates(limit)`. It returns streams with at least T = 64 shared refs, or with at least one shared ref and a tail that has not moved for an hour (tracked leader-locally), ordered by the occupancy of the packs they pin, fewest live slices first, so nearly empty packs are released first. One slice is enough: `compact_cold` accepts a single shared input (`cold.rs:536-543`). No `snapshot_group` and no S3 LIST.
   - **Plan.** Take the oldest contiguous run of shared refs, where each start equals the previous end, up to `compaction_max_size` (16 MiB).
   - **Execute.** Range-read each slice from its pack, bypassing the read cache so compaction does not evict hot read blocks. Write one exclusive chunk at `new_cold_chunk_path` and call `compact_cold` with `gc_not_before = now + compaction_gc_grace` (300 s).
   - **Failures.** On a definite rejection (a typed stream error, or a redirect before proposal), the engine rolls back the page entries and the driver deletes the replacement. On an ambiguous outcome it keeps both; F14h reclaims the replacement if it was never referenced. A page entry for a range still covered by state refs is shadowed, because reads exclude state-ref ranges from cold-index lookups (`query.rs:264-275`). A retried compaction's page write clips the earlier attempt's entry.
   - **Packs.** Pack GC is unchanged: releasing a pack's last ref queues it with the grace delay (`state_machine.rs:175-211`).
3. `migrate_legacy_shared_cold_once` becomes a call into the same function with a different candidate filter, so #278 can delete it on its own schedule. It stops cloning every group's state on each purge retry (`runtime.rs:913-924`).

**Bound.** At most T shared refs per stream between driver passes, plus the passes in one interval while a run is pending. That is about 16 KB per stream. Group maps hold at most the live packs. A slow stream adds about 300 slices per day in the healthy regime (F10); the driver keeps at most T of them in state and releases idle ones within an hour.

**Codec and gating.** None. Every binary's apply already accepts `CompactCold` with shared inputs.

**S3 cost.** Compacting T slices costs T range GETs, one chunk PUT, and one page GET and PUT: about 1.05 requests per slice. #182 put refs into state to avoid one page PUT per slice; this keeps that saving and amortizes the page write over 64 slices.

**Cost.** +40 production LoC for the branch and +300 for the driver, query and idle tracker, +350 test LoC. Low risk.

**Tests.** A Raft-engine test for shared-to-exclusive `CompactCold`; legacy migration and bucket purge on Raft; driver tests with typed rejection, ambiguous outcome and concurrent retention; a rejected external append, a packed trickle over the same offsets, compaction, then a read (D3); a single idle slice released within an hour; the W2 harness asserts at most T refs per stream and pack GC after compaction.

### 5.4 F3: producer state bounds

**Today.** `ProducerState.receipts` grows by one receipt per append within an epoch (`append.rs:1005`), and a batch receipt carries up to 512 items, which `last_items` duplicates (`model.rs:120`, set at `append.rs:1004`). The duplicate lookup scans linearly from the oldest receipt (`append.rs:931-948`). The producers map is never pruned, ids have no length limit (`state_machine.rs:752-761`), and transaction undo clones whole `ProducerState`s (`append.rs:108-111`). `model.rs:86-88` calls the history "Bounded exact response history", but nothing bounds it.

**Design.**

- **Receipt window.** Each stream keeps at most R = 1,024 receipt items, one item per frame, where a single append is one item. Receipts stay in per-producer `VecDeque`s with contiguous sequences, so a duplicate's receipt sits at index `seq − front.seq`, which is O(1). Receipts are created in commit order, so a derived per-stream FIFO of receipt handles gives the eviction order; restore rebuilds it by sorting receipts by start offset, which is unique. When the stream exceeds R, apply drops the oldest receipt. Eviction never touches epoch, sequence or a producer's newest acknowledgement.
- **Newest acknowledgement.** `ProducerState` keeps the newest append's byte range, closed flag and record range as scalars. A duplicate of the newest sequence whose receipt was evicted is answered from them, with its ranges. This is the most common retry, a client's last request after a timeout, so it keeps exact answers whatever other producers do. `last_items` is dropped; it duplicated the newest receipt, and its only reader (`query.rs:67-72`) goes away with F1's acknowledgements.
- **Beyond the window.** A duplicate whose receipt was evicted, other than of the newest sequence, is answered `204` deduplicated, with `Producer-Seq` and without byte or record range headers. Today it gets `409` "older than the retained receipt window" (`append.rs:931-948`). A `409` tells a client its sequence was wrong, which can provoke a re-append under a new sequence, which is a duplicate. The `204` follows the base protocol (upstream `PROTOCOL.md:417-418`) and #210's rule that eviction must never let a sequence be accepted twice.
- **One enforcement point.** The window, idle expiry and the producer cap are enforced once per command, after the whole command has applied. (0.6.0 removed group transactions, the only multi-append command.)
- **Idle expiry.** `ProducerState.last_seen_ms` is the command's `now_ms`. A producer idle for 7 days is treated as absent at its own next append and removed then; `TidyStream` removes idle producers in bulk. Every trigger depends only on persisted fields and the command's `now_ms`, so a replica that installed a snapshot expires exactly what a replica that replayed the log expires. Producers that existed before the raise count from the level's raise time. The base protocol suggests 7 days for in-memory stores and recommends that persistent stores keep producer state while data exists, so this is a deliberate trade that Q2 asks the maintainers to confirm. Expiry can duplicate only a producer's first append (sequence 0) retried after the idle period; any later sequence from an expired producer gets the existing `409` (expected 0) and cannot double-write.
- **Producer cap.** At most 4,096 producers per stream. A new producer beyond the cap evicts the least recently seen producer, found through a derived index on `(last_seen_ms, producer_id)`, if that one has been idle for at least an hour; otherwise the append fails with `429 producer_limit` (Q2).
- **Length caps.** `Producer-Id` and `Stream-Seq` are capped at 256 B on every HTTP write path, immediately and ungated, and at apply under Lb1.
- **Bounded catch-up.** A legacy producer can hold a million receipts. Enforcement trims at most 64k receipts per command, so the backlog drains over the producer's next appends and `TidyStream` commands.

**Codec.** `ProducerSnapshotV1` gains `optional uint64 last_seen_ms = 9`, `optional uint64 last_record_start = 10` and `optional uint64 last_record_next = 11`; `last_items` (field 7) is no longer written. At Lb1 an empty receipt list restores as empty. Only level-0 snapshots keep today's synthesis of a receipt from `last_*` (`persist.rs:359-365`); applied to a Lb1 snapshot, it would give a restored replica one receipt more than a live one, and their windows would diverge. Restore knows the level from the snapshot's level frame (F0).

**Gating.** Lb1, since apply behavior and response semantics change. Documentation changes ship in the same release: `exactly-once-writes.mdx:16, 31`, `durable-stream.mdx:329` and `extensions.mdx:786` change from "every accepted seq" to "within the stream's receipt window, and always for the newest sequence".

**Bound.** At most 0.4 KiB per producer plus R items, about 56 KiB at today's 56 B per item: about 1.7 MiB at the 4,096-producer cap, and under 60 KiB for a stream with a few producers. Streams without producers pay nothing.

**Cost.** +350 production LoC, +450 test LoC. Medium risk, because responses beyond the window are client-visible.

**Tests.** Window edges at R − 1, R and R + 1; interleaved producers; a producer retrying its newest sequence after others have filled R (answered with its ranges); batch receipts; expiry with a deterministic clock; cap behavior; snapshot round trip; a restore-versus-live differential with fully evicted producers; a madsim seed that installs a snapshot mid-stream and compares producer maps across replicas; a duplicate after eviction returns `204` and never appends; duplicate lookup is O(1) at 1M appends; a legacy producer with 1M receipts converges in bounded commands.

### 5.5 F4: message records

**Today.** `message_records` holds one 16-byte entry per JSON record or per binary append. It collapses to one `[retained, frontier)` entry only at `FlushCold` (`cold.rs:510-514`) and retention (`cold.rs:699`). `AppendExternal` extends it (`append.rs:589-594`) and never collapses it, so external-only streams grow 16 B per record forever. The collapse allocates with the old length (`cold.rs:728`) and returns no memory. Consumers are bootstrap (`query.rs:386-416`), `snapshot_offset_aligned` (`cold.rs:670-686`), restore coverage (`persist.rs:223-229, 484-497`) and transaction rollback.

**F4a (Lb1).** One rule at every cold transition. After `FlushCold`, `AppendExternal`, create with an external body, and `TidyStream`, every record ending at or below the seal point collapses into a single `[retained, p)` entry, built with exact capacity. F11 already reads bootstrap from the snapshot offset and splits JSON at LF, so JSON responses do not change. For binary streams that only receive external appends, cold history becomes one bootstrap part instead of one per append, as flushed history already is (I6). Alignment checks are unaffected: F18 accepts every offset at or below `p(s)`, which this collapse covers. +40 production LoC and +80 test LoC, low risk.

**F4b (Lb4, with F6b).** Delete the field. In the hot window, message boundaries are the record index's dense offsets for JSON or the hot buffer's append starts for other types, so one vector per stream serves both. Bootstrap uses those for binary hot messages plus one cold part from the snapshot offset; JSON keeps F11's LF split. Binary streams keep one part for cold history, which the spec should state. `snapshot_offset_aligned` and restore coverage use the derived boundaries, and the codec stops writing field 10. +100 / −140 production LoC and +150 test LoC, medium risk.

*Implementation notes (level 4).* Streams without a record index, including legacy JSON streams created before the index existed, keep their append starts in the hot buffer; snapshots carry them in stream entry field 20 (`hot_append_starts`), which a level-3 binary never sees because the level frame refuses the snapshot first. Bootstrap follows the honest-partial rule: there is no cold part, and a snapshot offset below the first derived start at or above the seal point answers the snapshot alone. Because a recorded start at the seal point is a real message start, a snapshot published exactly at a flushed message boundary now gets update parts where levels 1 to 3 conservatively answered a partial. Legacy message records left after the raise are converted per stream, on its next append, external append, flush, retention or `TidyStream` (whose debt includes them), never in the raise itself; a legacy record that starts at the seal point is treated as a fragment, as before. F6c charges 8 B per hot record at level 4 (`HOT_RECORD_OVERHEAD_BYTES_LB4`).

### 5.6 F5: external payload locators, commit first and index after

**Today.** For bodies of 1 MiB or more, the HTTP layer stages an object (`lib.rs:1419-1446`). The Raft engine then writes a cold-index page entry at the tail it read before proposing (`ursula-raft/src/engine/mod.rs:1223-1270`, and the create path at `:667-690`). It then proposes `AppendExternal`, whose apply only advances the frontier (`cold_state.rs:33-36`). The page entry is the only locator, and several things go wrong:

- **Rejected proposals.** A rejected proposal (412 on `Stream-Record-Match`, a closed stream, a producer conflict) leaves the entry behind, and it later overlaps different bytes at the same offsets (D3).
- **Duplicates.** A deduplicated retry writes an entry at the current tail for an object nobody references.
- **Ambiguous errors.** The HTTP layer deletes the staged object on any runtime error, including ambiguous ones where the append may still commit (`lib.rs:2482-2485, 2596-2601`).

An earlier design fixed the locator by keeping the `ObjectPayloadRef` in state at apply, at about 120 B of snapshot per append "until offloaded (a follow-up)". This design makes the offload part of the fix: state is the staging area, pages are the durable index.

**Design.**

1. **Apply (Lb3)** pushes the `ObjectPayloadRef` with its apply-assigned offsets into `StreamColdState.external_segments`. `read_plan_at` already serves state refs as direct object segments and excludes them from cold-index lookups (`query.rs:264-334`), so reads need no change.
2. **No pre-proposal write.** At Lb3 the engine stops writing pages before proposing, on both the append and the create path. Below Lb3 the old path stays, and F19's clip rule and repair contain what it leaves behind.
3. **Offload.** A leader-side offload step joins F2's driver loop, which becomes one "drain state-held cold refs" loop. It writes page entries for staged refs with an idempotent read-modify-write that clears every other entry overlapping them (F19); the refs are committed, so the entries are correct whatever happens next. It then proposes `OffloadColdRefs { stream_id, refs }`, a new Lb3 command. Apply removes exactly those refs if they are still present and queues no GC, because pages now reference the objects. A ref's range is never hot, so the coverage rule of F18 still covers it after removal. The trigger is any staged ref older than 10 s, or more than T_ext = 16 refs.
4. **Cleanup (B1, ungated).** The staged object is deleted only on a definite rejection (a typed stream error from apply, or a redirect before proposal) or a deduplicated response. Ambiguous outcomes keep the object; F14h reclaims it if nothing references it, and stream GC reclaims it at delete (F14a).

**Bound.** At most T_ext plus in-flight refs per stream.

**Implementation note.** Lb3 is feature level 3 (`FEATURE_LEVEL_EXTERNAL_LOCATORS`). The offload runs as its own leader-side worker every 2 s whenever a cold store is configured, rather than inside F2's driver loop, because that loop only runs when `compaction_enabled` is set and the offload bounds replicated state. A ref's age comes from the write time in its object name; a name without one counts as due. The cleanup rule also deletes the staged object of a create that answers already-exists. The orphan sweep (F14h) reads state refs before pages, and the offload writes pages before it removes refs, so a ref moving from state to pages is always seen in one of them.

**Codec and gating.** No new codec field; `external_segments` is field 9 already. The apply change and the new `OffloadColdRefs` command are gated at Lb3. Lb3 may trail the other levels: until it ships, deployments that cannot afford the staging path keep `external_payload_min_size` above the 32 MiB body cap.

**Cost.** +500 production LoC, +600 test LoC. Medium-high risk, since this is the data path for large appends; madsim ambiguous-commit seeds mitigate it (§7.4).

### 5.7 F6: hot window

**Today.** Each append becomes its own `HotChunk` (a 40-byte header plus an allocation) and also adds a 16-byte message record and an 8-byte dense offset. Admission (64 MiB per group, `config.rs:376`) and flush thresholds count payload only. `hot_payload_len` sums every chunk on every append response (`hot_buffer.rs:47-49`; `query.rs:153-160`; `in_memory.rs:325, 454, 649, 902`) and in `record_cold_hot_backlog` after every mutation (`ursula-runtime/src/metrics.rs:825-838`).

**F6a (B1, ungated).** A running byte counter in `HotBuffer`, updated in `push`, `flush_prefix`, `discard_before`, the same pattern #190 used for the group gauge. Write responses return the stream backlog, so the Raft engine drops its second state-machine round trip for metrics (`ursula-raft/src/engine/mod.rs:1101-1110`). +20 LoC.

**F6b (B6, ungated).** Coalesce appends into 64 KiB blocks: a `VecDeque` of blocks, each with its own start offset. A block ends at a gap, and an external append above hot bytes leaves one (F18). Keep append starts for binary streams and rely on the dense record offsets for JSON as the only per-message vector. Reads binary-search blocks; flush drops whole blocks and splits at most one. Snapshots keep emitting `hot_segments`, now one per block, which every binary restores (`persist.rs:196-217`). +350 / −120 LoC, medium risk.

**F6c (B6, ungated).** Admission and flush thresholds count payload plus the per-record overhead of the live representation: about 64 B today, about 24 B with F6b, 8 to 12 B with F4b. These are leader-side checks made before proposal (`ursula-raft/src/state_machine.rs:420-507`), so apply does not change. +40 LoC.

**Bound.** Payload plus about 24 B per unflushed record with F6b (a dense offset and a message record), and 8 to 12 B once F4b removes message records at Lb4, against about 64 B today. Tests: hot read, flush and rollback equivalence against the current buffer under random workloads, including gaps from external appends; the harness asserts the overhead.

### 5.8 F7: capacity hygiene

**Today.**

- `advance_retention` clones the whole record index to validate it (`cold.rs:368-385`), then drains without shrinking (`record_index.rs:227`).
- `compact_message_records_before` allocates with the old length (`cold.rs:728`).
- `flush_prefix` pops chunks without shrinking (`hot_buffer.rs:205-221`).
- The registry keeps `SlotMap` slots of `sizeof(StreamSlot)` and `HashMap` capacity after deletes.

*Measured*: 8.04 MB held for 10k records after trimming 990k, 16 MB held for a single message record, 2.6 MB of deque after one flush window, and 882 B per deleted stream.

**Design.** One rule: after removing elements, if capacity exceeds 2 × len + 64, shrink to 2 × len. Retention validates against the borrowed index and then mutates in place, the prepare/commit shape appends have used since #91. The registry stores `SlotMap<StreamKey, Box<StreamSlot>>`, so a vacant slot costs 8 B, and shrinks the keys map when its length falls below a quarter of capacity.

**Gating.** None; this changes representation only. +80 production LoC, +100 test LoC, low risk.

### 5.9 F8: TTL index with one armed entry per stream

**Today.** Every append and external append calls `refresh_ttl_entry` (`append.rs:356, 545`), which pushes a new entry with a cloned `BucketStreamId` even when the expiry did not change (`registry.rs:126-138`). Stale entries leave only when popped after their own expiry (`registry.rs:103-124`). Writes, `FlushCold` and `DeleteStream` never sweep them.

**Design.** Keep at most one armed entry per stream. The registry records `armed_at` per key in a side map that is not replicated.

- `refresh` pushes only when the stream has no armed entry or its new expiry is earlier than `armed_at`.
- `pop_expired` discards an entry whose expiry does not match `armed_at`. If it matches and the stream's current expiry is later, it re-pushes at the current expiry, updates `armed_at` and continues. Otherwise it returns the entry, as today.

Every returned entry is at its stream's true expiry, and every smaller key has already been processed, so the pop order is identical to today's. The apply-time sweep (#57) therefore expires the same streams in the same order: no gate and no codec change. The heap is rebuilt if it ever exceeds twice the live TTL streams, which only expiry decreases can cause.

**Cost.** +60 production LoC, +150 test LoC, low risk.

**Tests.** An equivalence test runs random appends, renewals, deletes and sweeps through the old and new registry and requires identical expiry sequences. The harness asserts at most two entries per TTL stream.

### 5.10 F9: engine bookkeeping

**Append counts.** `stream_append_counts` (`in_memory.rs:128`) loses entries only on `Deleted` (`in_memory.rs:594-599`). TTL expiry and `PurgeBucket` leave them. Snapshots filter them out (`in_memory.rs:1247-1273`), so after a recreate a replica that installed a snapshot and a long-lived one disagree. The fix moves the count into `StreamSlot`, so it dies with the slot. `StreamAppendCountV1` frames stay as they are, and the `restore_stream_append_counts` cross-check (`in_memory.rs:2123-2146`) goes away. The count is not exposed over HTTP. Ungated, +40 / −80 LoC.

**Admission.** The default single-node mode clones the whole engine to preview admission on every create and append (`in_memory.rs:690, 729`): a transient full copy of the group's memory per write, and 17.7 s for 30k appends with the default admission against 0.44 s without. It should use the O(1) `check_cold_write_admission_bytes` that the Raft path uses, plus the read-only `evaluate_producer` when deduplicated retries must bypass backpressure. Ungated, +40 / −90 LoC.

### 5.11 F10: flush planner

**Today.** `plan_next_cold_flush_batch` breaks at the first candidate that would exceed the batch (`cold.rs:167`). It walks streams in one fixed sorted order (`cold.rs:71-72`), and `compare_stream_ids` ignores the affinity key, so streams that differ only by affinity fall back to `HashMap` order (`state_machine.rs:866-870`). The worker passes `max_batch_bytes = max_flush_bytes` (`ursula-runtime/src/cold_worker.rs:83`). Planning re-sums every stream's hot bytes and clones and sorts every stream id once per candidate (`cold.rs:61-160`), on the group's apply worker in Raft mode (`ursula-raft/src/engine/mod.rs:1083-1099`). Under node pressure (hot bytes across the node at `flush_pressure_hot_size`, 128 MiB), every group's minimum drops to one byte, so every stream that holds a byte is flushed (`cold_worker.rs:8-22, 66-80`).

**Design.**

- **Fair, non-blocking batches.** A candidate gets at most the remaining batch budget; `plan_cold_flush_from` already cuts mid-chunk. A misfit never stops the pass, and the starting stream rotates per pass through a leader-local cursor.
- **Largest first.** In group drain mode (group hot at `flush_size`), flush streams in descending hot bytes until the group is below half of `flush_size`, instead of flushing every stream that holds a byte.
- **Node pressure.** Under node pressure, flush the largest streams across all led groups until node hot bytes fall to three quarters of the watermark. A per-group low watermark would make pressure a no-op for every group below 4 MiB, which is the common case: 128 MiB over 85 groups is about 1.5 MiB each.
- **Maximum hot age.** A stream's hot tail is flushed once it is older than `flush_max_hot_age` (5 min by default), even if small. This bounds how long a quiet stream's records stay hot, and with them U(s) and H(s) in groups that never reach `flush_size`. It does not reduce slices in the healthy regime: about 288 per day against the 308 *measured* without it. The large reductions come from ending starvation and per-second pressure packing, and F2 compacts what remains (Q6).
- **Hot index.** Each group keeps a derived index of streams with hot bytes, keyed by `compare_stream_ids` (bucket, stream). The index is not replicated, is maintained at apply and rebuilt at restore. A pass sorts once and checks `min_hot` against the counter before copying any payload; today `plan_cold_flush_from` copies first (`hot_buffer.rs:107-143`).

**Gating.** None. Planning is leader-side, and `FlushCold` validation is unchanged.

**Cost.** +250 production LoC, +300 test LoC. Low-medium risk, because flush cadence changes the S3 request mix, which the harness measures.

**Tests.** The starvation reproduction under the default configuration shows no starvation (at most twice `flush_size` hot per stream); node pressure brings node hot bytes below the watermark with groups of 1 to 2 MiB; planner work is asserted with deterministic counters (streams visited, bytes copied and hashed, sorts per pass) rather than wall-clock time; candidates are a deterministic function of the index and the cursor.

### 5.12 F11: read and bootstrap

**Today.** Offset reads default `max_bytes` to `usize::MAX` (`lib.rs:3156-3159`), for SSE and long-poll too. Record reads without `max_records` plan up to the tail (`in_memory.rs:1004-1018`), and the payload is fully assembled before the response is built (`in_memory.rs:1106-1185`). Without retention, one request can materialize a stream's whole history. Bootstrap has three further problems:

- It keeps only message records that start at or after the snapshot offset (`query.rs:405`), so after a collapse it drops the bytes between the checkpoint and the collapse point (D2).
- It plans every message record separately, and each `read_plan_at` scans every hot chunk and sorts every ref (`in_memory.rs:1203-1233`): O(hot records²), about 0.6 s at a full W1 hot window.
- Raft runs the whole call, S3 reads included, inside `with_state_machine` (`ursula-raft/src/engine/mod.rs:1003-1018`), so it stalls the group's apply worker.

**Design.**

- **Read cap.** A server default `read_max_response_bytes = 8 MiB`. Responses end at a message or record boundary with `up_to_date = false`; upstream `PROTOCOL.md:610` allows server-defined chunk limits. A single record larger than the cap is returned whole, and the 32 MiB body cap bounds it.
- **Bootstrap, one window.** The state machine plans one byte window from the snapshot offset (or the retained offset without a snapshot) to the smaller of the tail and the cap, covering every message record that ends after that offset, the first clipped to start at it. The engine materializes it outside the state machine, as `read_stream_parts` already does (`ursula-raft/src/engine/mod.rs:770-858`). Parts are cut after materialization: JSON at every LF, so each part is one record again (`extensions.mdx:751`), which the #15 collapse broke for flushed history; other content types at message-record boundaries, with collapsed cold history as one part. The response stops at the cap with a continuation in `Stream-Next-Offset`.

**Gating.** None: read path only. A client audit is needed first for the caps (Q5); the gap fix and the single plan need none. +250 production LoC, +300 test LoC, low-medium risk. CI asserts exactly one read plan per bootstrap.

### 5.13 F12: snapshot pipeline

**Today.** Snapshots are full rebuilds on entry counts (§2.3). The inline backend serializes bytes as a JSON number array inside a tagged enum (`snapshot_store.rs:124-157`). `CurrentSnapshot` keeps that pointer for the group's lifetime (`ursula-raft/src/state_machine.rs:195-202`). The disk-WAL factories JSON-encode it again into `group-N.snapshot.json` on every build (`state_machine.rs:219-223, 830-861`). `get_snapshot_builder` waits for a node-wide permit on the openraft state-machine worker and deep-copies the group (`state_machine.rs:591-612`); openraft awaits it there, so one group's apply can stall behind another group's build. `build_snapshot` clones again (`:701, 719, 736, 774`), and install decodes twice (`ursula-raft/src/registry.rs:1159-1168`; `state_machine.rs:619-660`).

**Design.**

- **F12a, binary envelope.** `SnapshotPointer` and `PersistedSnapshot` move to MessagePack with `serde_bytes`. Decoders accept the old JSON forever. Decode support ships ungated in B2; emission starts at Lb1, because every follower must be able to decode it.
- **F12b, S3 by default.** Use S3 snapshots by default whenever a cold store is configured; inline stays the fallback. This also starts the manual snapshot driver on every such cluster, and with it leadership shedding on S3 probe or flush failures (`ursula/src/bootstrap/snapshot.rs:139-179`): an operational change that is rated and rolled out as one.
- **F12c, no extra copies.** The frame iterator borrows the `GroupSnapshot`, cloning only on fallback, and install decodes once.
- **F12d, non-blocking permit.** `try_create_snapshot_builder` try-acquires the permit and returns `None` when it is busy, so no apply worker waits on another group's build.
- **F12e, byte-based cadence.** Snapshot cadence follows log bytes end to end. Per group, the driver snapshots when unpurged log bytes reach max(F, 2 × the last snapshot's raw size); an entry count of 100k remains only as a far backstop. The floor F is the node log-byte budget (1 GiB by default) divided by twice the group count, at most 16 MiB: 4 MiB at the default 128 groups. The node pressure pass replaces its 65,536-entry threshold (`snapshot.rs:181-208`) with that budget and snapshots the groups with the most log bytes per snapshot byte first. The inline backend moves to `SnapshotPolicy::Never` with the same driver minus S3-health gating, unless the maintainers retire inline for production (Q7). After F1 a snapshot is dominated by the hot window, so entry triggers would keep the ratio at 2.6 to 5.2 snapshot bytes per appended byte on W1 (about 5.2 MB of snapshot per 1 to 2 MB of appends); byte triggers bring it near one half.

**Cost.** +350 production LoC, +300 test LoC. Medium risk for F12a (mixed versions) and F12b (operational), low for the rest. Only F12a's emission is gated.

### 5.14 F13: node caches

**Today.** `ColdIndexPageCache::touch` pushes a key per lookup and drains only when the page count exceeds capacity (`cold_index.rs:678-729`). `ColdReadCacheInner.readers` keeps an entry for every stream ever read (`cold_store.rs:1182-1201`). Each forwarded gRPC `GroupRead` builds a throwaway page cache (`ursula-raft/src/grpc.rs:650-655`). Inferred from code but not reproduced: the Raft read-path page cache is not invalidated when `CompactCold` applies (`ursula-raft/src/engine/mod.rs:376` against `in_memory.rs:283-288`), so after the 300 s grace a cached page could point at deleted inputs.

**Design.**

- **Page cache.** Port `compact_lru_if_needed` (`cold_store.rs:1251-1272`) into the page cache. Keep one `Arc`'d page cache per group, shared by reads, bootstrap and forwarded reads, so apply-time invalidation covers all of them. Bound it by approximate bytes rather than 1,024 pages.
- **Readers map.** Give each reader a last-touch generation and prune idle entries, amortized, whenever the map exceeds max(4 × blocks, 4,096). `invalidate_prefix` drops the stream's entry.

**Gating.** None. +100 production LoC, +120 test LoC, low risk, plus a reproduction test for the stale-cache hazard.

### 5.15 F14: cold-object hygiene

- **(a) External prefix in stream GC (Lb1, with g).** Stream delete reclaims `{stream}/chunks/` and `{stream}/cold-index/` only (`runtime.rs:1016-1021`), so payloads under `{stream}/external/` (`cold_store.rs:1347-1351`) survive until bucket purge. Add that prefix. It ships with (g): while GC is not scoped to an incarnation, the wider sweep would also delete a recreated stream's staged initial payload, which a create body of 1 MiB or more stages at once (`lib.rs:2436-2465`).
- **(b) GC worker.** `run_cold_gc_all_groups_once` aborts all later groups on one group's error (`runtime.rs:1065-1069`); it should log and continue, ungated. The FIFO stops at the first entry that is not yet due or that fails (`runtime.rs:1011-1014`; `cold_gc.rs:54-64`). `DeferColdGc { seq }` (Lb1) moves a failing head entry to the tail with backoff.
- **(c) No tiny exclusive objects.** A lone candidate below 1 MiB goes down the pack path as a pack of one. Per-flush page rewrites (`cold_index.rs:360-399`) then happen only for large flushes and compaction outputs, and F2 bounds the resulting refs. Ungated.
- **(d) Compaction discovery.** The flush path records `(stream, page)` debt when it publishes an exclusive chunk below `compaction_target_size`, and the compactor drains that debt instead of recursively listing the whole cold root every 30 s (`runtime.rs:770-900`). After failover the debt set restarts empty and refills on the next small flush; the slow cursor over the group's own stream ids that F19 introduces catches idle streams. Compaction then turns on by default (`config.rs:370`). Ungated.
- **(e) Stale flushes.** On a definite rejection, roll back the page entry and delete the chunk, porting `write_cold_chunk_index_pages_with_rollback` to the Raft flush path (`ursula-raft/src/engine/mod.rs:1454-1475`). Ungated.
- **(f) Retention GC.** Only deployments that trim need this: a leader-side pass deletes pages wholly below the retained offset together with their exclusive chunks and externals, through a gated `EnqueueColdGc` command. It is not needed for boundedness. *Status (2026-10-03):* shipped without a replicated command, after an AWS run showed nothing below the retained offset was ever deleted. The F19 repair cursor runs it on the leader: once a retained offset has been observed for the F14i grace (leader-local clock, restarted on failover), it deletes the pages that lie wholly below it and hold only entries below it, then the objects only those pages name (`crates/ursula-runtime/src/retention_gc.rs`). It never writes the boundary page and never deletes an object a kept page names: page writes are unconditional PUTs and leadership is checked only when a step starts, so a deposed leader rewriting the boundary page could drop an entry a new leader just flushed, which at Lb1 is the chunk's only reference. This leaves at most about one page span of objects below the retained offset per stream. Follow-up: conditional PUTs on page writes for repair, flush and compaction, which would remove that hazard everywhere. Direct external refs that retention drops from state are still left to the orphan sweep.
- **(g) Incarnation-scoped objects (D4).**
  1. *B1, ungated.* The GC worker acknowledges a stream entry without deleting anything when a stream with that name exists again, and checks again before deleting pages; (h) reclaims the old incarnation's unreferenced objects later. Deleting a live stream's data becomes a bounded leak, and the window left is a recreate during an in-progress sweep. F19's repair drops page entries whose objects predate the stream's creation.
  2. *Lb1.* A create assigns a unique incarnation, `created_at_ms := max(now_ms, group.last_created_at_ms + 1)`. `last_created_at_ms` (header field 10) starts at the raise as the maximum of its `now_ms` and every live stream's `created_at_ms`. A stream created at Lb1 or later keeps its cold-index pages under generation = incarnation; the page key's generation component and snapshot field 7 already exist and are always 0 today (`cold_index.rs:37`; `cold_state.rs:22-24`). Its chunk and external names gain an `{incarnation:016x}/` component. Stream GC entries carry the incarnation (`ColdGcEntryV1` field 6) and delete only that incarnation's names and generation. Legacy entries delete only legacy-format names directly under `chunks/` and `external/`, and generation-0 pages, which also stops today's recursive sweep from reaching an affinity stream under the same name.
- **(h) Orphan sweep (D5, B2, ungated).** Publishes with ambiguous outcomes keep their objects on purpose (`runtime.rs:404-414, 537-546`), and nothing reclaims them; packs live under `{bucket}/_packs/{group}/` (`cold_store.rs:1328-1331`), outside every stream prefix. A per-group leader job lists the group's pack prefix and walks stream prefixes with F19's cursor, deleting objects older than a day that no state ref, cold-index page or GC entry references; packs need only the group's ref maps. It wires up the existing `cold_orphan_cleanup_*` counters (`metrics.rs:413-415`).
- **(i) Retention grace (D6, Lb1).** Retention releases dropped pack slices with no grace (`cold.rs:706`). It uses `compaction_gc_grace` instead, so a read planned before the retention still finds its bytes. The not-before time is replicated, hence the gate.

**Cost.** +600 production LoC, +500 test LoC, medium risk: (g) changes object naming.

### 5.16 F15 and F16: deferred tenant and snapshot items

**F15, tenant tombstones.** `bucket_usage` keeps a row of about 200 B per bucket ever written in every group. `erased_buckets` keeps about 95 B per purged bucket in every group, and `PurgeBucket` runs on all groups (`runtime.rs:579-600`). Both are deliberate (#258, #280), and both grow with tenant churn rather than records. Options: a gated `PruneBucketUsage { bucket_id, observed }` sent after the meter durably records the counters; fences held in the meta group, with only a 16-byte fingerprint set in data groups. Q9 asks the maintainers to decide.

**F16, visible snapshot.** O(1) per stream, but inline up to the 32 MiB body cap on every replica and in every group snapshot. Accepted for the SQLite VFS, which publishes the database file itself as the snapshot, and implemented at Lb5 as follows.

*Design (Lb5, feature level 5).* The smallest change that reuses F5:

1. **Staging.** `PUT {stream}/snapshot/{offset}` (and `?record=`) reads the body as a stream. Below the staging threshold, the smaller of `runtime.external_payload_min_size` (1 MiB by default) and the 32 MiB inline cap, the body stays inline and is proposed as today's `PublishSnapshot`. Above it, when a cold store is configured and the stream's group is at level 5 as the local replica sees it, the HTTP layer streams the body into a new object under `{stream}/external/` (F5's `new_external_payload_path`, uploaded in 8 MiB multipart parts) while hashing it with the same BLAKE3 digest apply uses for inline bodies. Memory per request stays at most one inline cap; admission charges at most 32 MiB of the in-flight budget for a snapshot PUT and admits bodies up to `MAX_COLD_SNAPSHOT_BYTES` = 1 GiB, enforced again while streaming (413).
2. **Command.** A new command, `PublishSnapshotExternal { stream_id, snapshot_offset, content_type, object: ExternalPayloadRef, digest, now_ms }`, gated at Lb5 (`FeatureNotEnabled` below it). It is a new variant rather than an optional field on `PublishSnapshot`, so a binary without it fails loudly instead of applying an empty inline body (§5.1). Apply runs the existing publish rules unchanged (scope, tail, retained offset, alignment, idempotency by digest) and stores `StreamVisibleSnapshot { offset, content_type, digest, object, payload: [] }`. Group snapshots carry the reference as `StreamVisibleSnapshotV1.object` (field 5); the level frame keeps older binaries from installing them.
3. **Reads.** `GET {stream}/snapshot/{offset}` and `/bootstrap` plan as before; the plan carries the reference instead of bytes, and the HTTP layer streams the object from the cold store in 8 MiB pieces outside the state machine (no S3 inside `with_state_machine`, as F11 requires). The first piece is read before the status line, so a missing object answers 502 rather than a truncated 200. Both set `Content-Length`. Snapshot reads already require the local leader, so no body crosses the Raft gRPC path.
4. **Object lifecycle, all through existing machinery.** The F5 cleanup rule deletes a staged body after a definite rejection and keeps it after an ambiguous failure. Apply queues a `ColdGcTarget::Paths` entry for a superseded cold body with the F14i grace (300 s), so a read planned before the publish still finds it; for the staged copy of an idempotent repeat (same digest, nothing references it), with the same grace; and for the visible body when the stream is removed (stream GC only reaches externals that pages reference). The orphan sweep (F14h) treats the visible body as a state ref (`stream_referenced_cold_paths`), so it reclaims only staged bodies whose publish never committed, after a day. Bucket purge erases the prefix as before.

*Not covered.* `ImportSnapshot` copies the reference, not the object, so an import into a cluster with a different cold store loses cold bodies. The gateway (`ursulagw`) still buffers request bodies up to `--max-request-body-bytes` (32 MiB by default); larger snapshots go straight to a node or need that flag raised. A node without a cold store never stages, so every body there keeps the 32 MiB cap.

*Bound.* Replicated state holds about 150 B per cold snapshot instead of its body; the inline path is unchanged.

### 5.17 F17: hardening

- **WAL reclaim.** Online reclaim rewrites the whole live journal on every purge once the file reaches 64 MiB (`ursula-raft/src/log_store/file.rs:548-575`). Track live and dead bytes and reclaim only when dead bytes reach live bytes.
- **Admin clones.** Legacy migration and backup export deep-clone group state (`runtime.rs:905-924`; `lib.rs:1986-2000`). F2 removes the first; export should stream.
- **Meta-group migrations.** The control-plane `migrations` map is only ever inserted into (`ursula-control/src/state.rs:334`). Prune terminal migrations older than N.

+150 production LoC, low risk.

### 5.18 F18: cold coverage derived from the hot buffer

**Today.** The cold frontier is a scalar that `push_cold_chunk` assigns and `push_external_segment` raises (`cold_state.rs:26-36`). An external append above hot bytes leaves a gap in the hot buffer, and the flush of the hot prefix then sets the frontier below the external (D1). Reads plan cold-index segments only up to the frontier (`query.rs:266-267`), and restore requires the frontier, refs and hot segments to cover `[retained, tail)` (`persist.rs:432-470`), so the external's bytes have no source and the snapshot cannot be restored or installed.

**Design.**

1. **B1, ungated, node-local.** Coverage is the complement of the hot buffer. In `read_plan_at` and `payload_sources_cover_retained_suffix`, every byte of `[retained, tail)` that no hot segment holds is cold, served by state refs where they exist and by cold-index pages otherwise. Reads of affected streams work again, and nodes restore and install snapshots that already carry a regressed frontier. Snapshots keep writing the frontier as before, so replicated state does not change.
2. **Lb1.** Apply drops the scalar frontier. `snapshot_offset_aligned` accepts the retained offset, any offset at or below `p(s)`, or a message-record end. Today's frontier clause also accepts intra-message offsets in hot bytes that lie below an external append, and a frontier raised with `max` would widen that, which is why the clause is replaced rather than repaired. Retention's message-record collapse uses `p(s)` too. Field 6 is written as `p(s)` for tooling and ignored at restore. The per-call sort of external segments in `cold_frontier_offset` (`cold_state.rs:83-108`) goes away with it.
3. **Consequences elsewhere.** F5's `OffloadColdRefs` can always drop a ref, because its range is never hot. F6b's blocks keep their own start offsets, because the hot buffer has gaps.

**Cost.** +80 production LoC, +150 test LoC (the D1 sequence on both engines, plus restore and install of snapshots with a regressed frontier). Low risk.

### 5.19 F19: page-entry hygiene

**Today.** Cold-index pages can serve another object's bytes (D3). Rejected external appends leave entries that later overlap different bytes, and pages keep same-start entries with different ends. While state refs cover a range, reads ignore its page entries (`query.rs:264-334`), so turning refs into page entries, as F2 does, exposes whatever was hidden. Under F1, wrong bytes would also shift record boundaries inside sealed blocks.

**Design.**

1. **Clip on proven writes (B1, ungated).** A page write for a range whose bytes state proves removes or clips, in the same read-modify-write, every other entry overlapping the range. State proves three kinds of range: a flush of hot bytes (the hot buffer holds only committed appends), an F2 replacement of state-held refs, and an F5 offload of a committed external ref. Rollback restores the previous page, as today. Page writes are leader-side, so mixed versions need no gate.
2. **Repair (B1, ungated).** A slow leader-side cursor over each group's stream ids repairs pages: per page, it keeps only the last-written external entry at each start offset, and drops external entries that overlap a chunk entry or another object's state ref, that start at or beyond the stream's tail, or whose objects predate the stream's creation by more than a minute (D4). Within a leader's term, a group's writes run one at a time through its group actor, so a later entry at the same start offset follows a proposal that did not commit there. F2 repairs a stream's pages right before compacting it. Each leader reports when it last completed a full cycle; the Lb2 raise requires one in every group. Two overlapping page-only external entries with different starts, both written before F5, cannot be told apart; they remain a documented hazard, which step 3 turns into an error on JSON streams.
3. **Anchor verification (with F1).** A scan that crosses an anchor checks that the byte before it is LF, and when it spans two consecutive anchors, that their LF count equals their record difference. A mismatch fails the read with a corruption error and a metric.

New stale entries stop at Lb3, when F5 removes the pre-proposal write.

**Cost.** +200 production LoC, +250 test LoC (the three D3 reproductions as regression tests). Medium risk: the repair rewrites pages.

### 5.20 Gating, cost and milestone summary

| Fix | Gate | Prod LoC | Test LoC | Risk | Milestone |
|---|---|---|---|---|---|
| F0 levels, frame, `TidyStream` | defines levels | +600 | +400 | medium | B1 plumbing, B3 `TidyStream` |
| F1 sparse marks | Lb2 | +1,300 | +1,800 | medium-high | B1 preparation, B4 |
| F2 pack refs: Raft branch / driver | none | +40 / +300 | +350 | low | B1 / B2 |
| F3 producer bounds (HTTP caps ungated) | Lb1 | +350 | +450 | medium | B1 caps, B3 |
| F4a collapse / F4b removal | Lb1 / Lb4 | +40 / +100 −140 | +80 / +150 | low / medium | B3 / B6 |
| F5 external locators (cleanup rule ungated) | Lb3 | +500 | +600 | medium-high | B1 cleanup, B5 |
| F6a counter / F6b blocks / F6c accounting | none | +20 / +350 −120 / +40 | +300 | low / medium / low | B1 / B6 / B6 |
| F7 capacity | none | +80 | +100 | low | B1 |
| F8 TTL index | none | +60 | +150 | low | B1 |
| F9 engine bookkeeping | none | +80 −170 | +80 | low | B1 |
| F10 planner | none | +250 | +300 | low-medium | B1 |
| F11 read and bootstrap | none | +250 | +300 | low-medium | B1 |
| F12 snapshot pipeline | F12a emission at Lb1 | +350 | +300 | medium | B1 (c, d), B2 (a decode), B3 (a emit), B6 (b, e) |
| F13 node caches | none | +100 | +120 | low | B1 |
| F14 cold-object hygiene | (a), (b) defer, (g) 2, (i) at Lb1; (f) gated | +600 | +500 | medium | B1 (b, e, g 1), B2 (h), B3 (a, b defer, g 2, i), B6 (c, d) |
| F15 | next level, if accepted | +100 | +100 | low | decision in B7 |
| F16 cold snapshots | Lb5 | +450 | +200 | medium | after B7 (SQLite VFS) |
| F17 hardening | none | +150 | +100 | low | B7 |
| F18 cold coverage | B1 rule none; representation Lb1 | +80 | +150 | low | B1, B3 |
| F19 page-entry hygiene | none | +200 | +250 | medium | B1 |

Totals, excluding F15 and F16: about +5,800 / −400 production LoC and +6,500 test LoC, plus the harness (§7.1). The first estimate was half of this; the history of #91, `PurgeBucket` (#157, #282) and bucket quotas (#158) supports the larger figures.

## 6. Record-coordinate correctness under sparse marks

F1 changes how every record coordinate is resolved, so it carries its own invariants. The dense implementation at `e6d8d70` is the oracle; marks must be invisible, apart from retention landing on a mark. Each invariant names its test.

**RC-1, boundaries are LFs.** On a JSON stream each record is one compact JSON value plus one LF and contains no other LF, on every write path: inline append, create with a body, and external create and append. *Test*: a fuzz test (arbitrary JSON, arrays, escapes, lone surrogates) asserting that the stored bytes' LF positions equal the committed record ends on each path.

**RC-2, oracle equivalence.** In every reachable state, `offset_for(r)` after scanning equals the dense oracle for every retained `r`, and `record_for(o)` agrees for every boundary `o` and rejects every non-boundary. *Test*: a proptest differential suite on `StreamStateMachine` with an in-memory byte store standing in for S3. Random JSON appends of 1 to 2,000 records with sizes from 2 B to 3 MiB, external appends, flushes at random points including mid-record splits, retention at random boundaries, and snapshot round trips at random points. Explicit cases: the D1 sequence (hot prefix, external append, flush, offload, snapshot round trip), and stale page entries from rejected external appends at the same and at overlapping starts.

**RC-3, scan locality.** Resolving a sealed record reads at most one 1 MiB block before the record's start. Resolving a range reads at most one cache block past its end, or exactly one record when that record is larger. *Test*: an instrumented cold store in the RC-2 suite counts bytes read per lookup.

**RC-4, constant-time ranges.** `first_record`, `next_record`, `Stream-Record-Match`, HEAD and `tail_records` resolution never scan. *Test*: a counting store asserts zero cold reads for these operations.

**RC-5, offset reads unchanged.** Offset reads, live or not, return byte-identical bodies and headers. *Test*: an HTTP suite run against both implementations with response diffing.

**RC-6, record reads.** `?record=r&max_records=k` returns exactly the records `[r, min(r + k, next_record))` as complete NDJSON, with `offset = O(r)`, `next_offset = O(r + k')`, the matching record headers, and `up_to_date` exactly when `next_offset` is the tail. Under F11 or P7 caps it may return fewer records, but at least one. *Test*: HTTP vectors from `record_coordinates_reference.rs`, extended to the cold region and to records straddling blocks, chunks and packs.

**RC-7, `tail_records`.** `?tail_records=n` returns the same records as the oracle, whether they are hot, cold or straddle the seal point. *Test*: HTTP.

**RC-8, live reads by record.** SSE and long-poll by record, including the envelope view with one record per event, produce the same event sequence (payloads, `streamNextOffset`, `streamNextRecord`, `streamFirstRecord`) as the oracle, including when a `FlushCold` seals records in the middle of a session. *Test*: HTTP live tests with a flush injected between iterations, plus the madsim cold-path family.

**RC-9, `Stream-Record-Match`.** The decision depends only on `next_record` and is identical to the oracle. *Test*: the existing HTTP and precondition tests run with sealing enabled.

**RC-10, fresh acknowledgements.** Append, close and create responses carry the range computed in apply and never derive it from the index afterwards. *Test*: append immediately followed by a seal in the same apply batch; external appends that seal their own records; an inline append followed by an external append.

**RC-11, duplicate acknowledgements.** A duplicate returns its stored receipt's original byte and record ranges, independent of sealing and retention. A duplicate of a producer's newest sequence always returns its ranges, from the newest acknowledgement if its receipt was evicted. Any other evicted duplicate returns no range, never a recomputed one. *Test*: batch duplicates for non-latest receipts (a 500 today after retention); duplicates after sealing; duplicates after eviction, newest and older.

**RC-12, retention by offset.** On a JSON stream, retention to offset `o` succeeds exactly when `o` is a record boundary at or above the current retained offset and at or below the latest snapshot. The effective retained offset is `o` when `o` is dense or a mark, and otherwise the mark at or below `o`. Afterwards `first_record` equals the oracle's ordinal at the effective offset, and no record is renumbered. *Test*: RC-2 suite plus HTTP, with targets in dense, sealed, mark, block-edge and intra-record positions.

**RC-13, retention by record.** `?record=r` resolves `O(r)` and retains as RC-12; the response reports the effective offset and first record. *Test*: HTTP.

**RC-14, snapshot publish.** Publishing by record or offset accepts exactly the record boundaries; an intra-record offset returns 400, which fixes today's acceptance below the cold frontier. Snapshot and bootstrap responses carry oracle-equal `Stream-Record-First` and `Stream-Record-Next`. *Test*: HTTP snapshot and bootstrap suite.

**RC-15, bootstrap.** The parts after the snapshot offset concatenate to the oracle's bytes from that offset, and JSON parts are exactly one record each. This holds after a checkpoint that retention did not follow, after a flush past it, and after an external append that collapses message records. *Test*: HTTP bootstrap tests over hot, cold and mixed suffixes, including those three sequences on both engines.

**RC-16, persistence.** Marks survive snapshot build, restore, WAL replay, snapshot install, backup export and import byte-for-byte. Old snapshots restore as all-dense, including those that carry a regressed frontier (D1). A snapshot at a raised level is refused by a binary below that level, and by a binary without F0. *Test*: codec round-trip property test; restore fixtures from `e6d8d70`, among them a D1 snapshot; a madsim snapshot-install family.

**RC-17, determinism.** All replicas hold identical marks after the same log prefix. *Test*: madsim compares per-group introspection digests across replicas at quiescent points.

**RC-18, transactions.** Removed in 0.6.0 with group transactions: no command rolls back appends, so the record index has no rollback path.

**RC-19, plan and state races.** A plan computed before a concurrent `FlushCold`, seal or retention still returns correct bytes. Trimming depends only on immutable bytes and the plan's skip and take, and retention into sealed history lands on a mark, so a bracketed plan never needs bytes below the new retained offset. Reads below a new retained offset behave as today within the GC grace, which retention now applies to packs (F14i). *Test*: an injected seal or retention between plan and materialize in both engines.

**RC-20, anchors.** A continuation anchor is used only when its incarnation matches and it validates; otherwise the read resolves from the mark. *Test*: SSE across a delete and recreate under the same name; an anchor whose offset is not preceded by LF.

**RC-21, corruption is an error.** A scan that crosses an anchor not preceded by LF, or that finds a different LF count between two anchors than their record difference, fails with a corruption error and never returns shifted records. *Test*: a stale page entry injected under a sealed block.

Acceptance: RC-1 to RC-21 pass in CI. The `append_apply` benchmark reports index bytes per record and per cold MiB, plus hot and cold seek and aligned-read latency, with at least three runs attached to the PR, as `json-record-coordinates-validation.md:60-68` requires.

## 7. Measurement and gates

### 7.1 Harness (B0)

Commit the bounded-state probe as a workspace member `crates/ursula-state-probe` (`publish = false`). It contains:

- **L1 drivers** over `StreamStateMachine`:
  - W1: one JSON stream of 200 B inline appends with cold flushes.
  - W2: 200 slow streams sharing packed flushes, under the default, 16 MiB `flush_max_size` and node-pressure configurations.
  - W3: external-only streams, with 1 or 5,000 records per append.
  - W4: producer headers, with one producer, epoch bumps, or 100k ids.
  - W5: churn through deletes, TTL expiry, the TTL heap and bucket purge.
  - W6: the same workloads with periodic retention.
- **L2 drivers** through `ShardRuntime` on both engines, plus `CompactCold` and legacy-migration probes.
- **The adversarial reproductions** of D1 to D4 on both engines, which start as expected failures and flip as B1 lands.
- **A planner cost probe** that reports deterministic counters.
- **A counting global allocator** that reports requested bytes, live blocks, and every live allocation of 64 KiB or more, which exposes `Vec` capacities directly.
- **Exact per-structure snapshot sizes** from the real snapshot codec. It is exposed for tests instead of compiled in through `#[path]` as in the scratchpad.

Output is JSONL per checkpoint with a `summarize.py`. Workloads use distinct stream names: streams that differ only by affinity fall back to `HashMap` order today, so W2-shaped runs differ between processes (296 against 293 passes in two identical 12 h runs) until F10's planner order lands.

B0 also adds per-group gauges that the soak needs (§7.5) and the harness reads where it can.

### 7.2 Per-PR CI gate

A `state-growth` job runs reduced-scale versions of W1 and W3 to W5, plus the reduced starvation reproduction (50 streams at 2 KB/s for 2 simulated hours, onset at about 65 minutes, about 23 s of wall time), in under 3 minutes on the CI runner. Assertions start as ratchets, failing at today's measured value plus 10%. Each one tightens to its target in the PR that lands the fix:

| Assertion | Target | Fix |
|---|---|---|
| marks; dense entries | ≤ ⌈cold MiB⌉ + 2; = unflushed records | F1 |
| snapshot bytes of mark fields 17 and 18 per cold MiB | ≤ 32 | F1 |
| shared refs per stream | ≤ 64 + refs added in one driver interval | F2 |
| receipt items per stream; producers per stream; producer bytes per stream and per group | ≤ 1,024; ≤ 4,096; within Prod(s) | F3 |
| message records per stream | ≤ unflushed + 2 | F4a |
| staged external refs per stream | ≤ 16 + in flight | F5 |
| hot overhead per unflushed record | ≤ 24 B (F6b), ≤ 12 B (F6b with F4b); `hot_payload_len` O(1) (F6a) | F6 |
| capacity after flush or retention | ≤ 2 × len + 64 | F7 |
| TTL heap entries | ≤ 2 × live TTL streams | F8 |
| starvation reproduction | max stream hot ≤ 2 × `flush_size` | F10 |
| planner pass at 3k hot streams | streams visited ≤ hot streams; bytes copied ≤ bytes flushed; one sort | F10 |
| read plans per bootstrap | 1 | F11 |
| D1 to D4 reproductions | pass on both engines | F11, F14g, F18, F19 |
| per-structure formulas | every structure within its own I1 or I2 term, evaluated with the live U, K and P and length-based bytes, at every checkpoint | all |
| residual growth | state − H − 8·U − 16 B·K − Prod − cache caps grows by ≤ 1 KiB between N and 4N records, with W1 checkpoints taken after a forced flush | all |

Slopes on raw state would be confounded: W1's hot bytes swing between 1.3 and 7.1 MB across checkpoints, against an allowance of a few KB. Snapshot sizes and structure counts are deterministic for workloads with distinct stream names. Heap figures are deterministic for a fixed toolchain and are compared with a 5% tolerance.

### 7.3 Nightly

Full-scale runs nightly: W1 at 3M records, W2 over 24 simulated hours at the default configuration (about 7.5 minutes of wall time today), W4 at 1M appends, W5 at 1M appends, the L2 120-minute runs on both engines, and from B6 the snapshot-cadence ratio on W1 and on a uniform 128-group workload. JSONL results are published as artifacts and trends are plotted. The job sits next to the existing `dst-nightly.yml` sweep.

### 7.4 DST additions

The invariants in `deterministic-simulation-testing.md:245-258` are all about correctness, and none bounds state. Add:

- **Invariant 9, bounded replicated metadata.** At quiescent points, each stream's replicated metadata, read through introspection (`crates/ursula-sim/src/madsim_harness/introspect.rs`), satisfies I1 with the stream's own U, K and P.
- **Invariant 10, record-coordinate equivalence.** Every record read, acknowledgement and retention result equals the client-side oracle (RC-2, RC-6 to RC-14).
- **Invariant 11, no stale locators.** After any ambiguous external append or compaction, readable bytes equal acknowledged bytes, and no page entry overlaps differing bytes.
- **Invariant 12, identical producer state.** Every replica holds the same producers, receipt windows and newest acknowledgements after the same log prefix, whether it replayed the log or installed a snapshot.

Seed families:

- **Cold-path record reads** across flush, seal and compaction boundaries.
- **Retention by record** into sealed regions.
- **Snapshot install** with marks, and mid-stream with producers.
- **Mixed hot and external appends** (D1) with leader churn and snapshot installs.
- **Delete and recreate** under the same name with GC pending (D4).
- **Level raise** under live traffic.
- **Old-binary emulation.** Nodes with a test-only cap on their maximum level emulate old binaries, so raises are refused and joins rejected. madsim runs one binary (`rolling_restart.rs`), so true N-1/N interop stays on the EKS commit-candidate rollout gate.
- **Producer churn** against the receipt and producer bounds.
- **TTL churn** against the heap bound.
- **Ambiguous outcomes** for `CompactCold`, `OffloadColdRefs` and `AppendExternal` under network faults.

### 7.5 Soak metrics and gate

Per-group gauges:

- record marks and dense entries
- shared refs and live packs
- staged external refs
- receipt items, producers and producer bytes
- message records
- TTL heap entries
- hot payload and hot real bytes
- pending cold-GC entries and the age of the head entry
- unpurged log bytes, snapshot raw bytes and build time
- page-repair cycle age, entries clipped and dropped, corruption errors
- feature level

Per-node gauges:

- mark bytes across the node's groups
- page-cache LRU length and pages
- `readers` entries
- orphan objects found and deleted
- RSS

Alerts:

- shared refs above 2 × T on any stream
- staged external refs above 4 × T_ext
- TTL heap above twice the TTL streams
- page-cache deque above twice its capacity
- the GC head older than 1 h
- any corruption error from anchor verification
- mark bytes per node above the thinning threshold (Q10)
- snapshot bytes growing by more than 1% per day beyond 16 B × the day's added cold MiB, at constant live streams

**Soak gate (B7).** A 72-hour EKS soak on a commit candidate with W2-shaped trickle streams, one W1-shaped heavy stream, producer and TTL streams, mixed inline and external appends, delete-and-recreate churn, and leader churn. Passing means every structure stays within its own formula at every scrape, and after a 6-hour warm-up, the residual (state − H − 8·U − 16 B·K − Prod − cache caps) and RSS minus the same terms grow by no more than 1% per day.

## 8. Milestones

The workstream starts now. It touches no protocol surface except the receipt window (F3), retention granularity in cold history (F1), bootstrap parts and response caps (F11), and binary bootstrap parts (F4a, F4b).

**B0, harness (about 1.5 weeks).** Port the probe and the adversarial reproductions (§7.1), add the per-group gauges, and land the CI ratchet job and the nightly job. *Exit*: CI reproduces the audit's figures within 10% at reduced scale and runs D1 to D4 as expected failures; the nightly job publishes W1 to W6; dashboards exist.

**B1, correctness and ungated fixes (about 4 weeks, parallel work).** Correctness first: F18 step 1 (D1), F11 (D2 and the quadratic bootstrap), F19 steps 1 and 2 (D3), F14g step 1 (D4), and F2's Raft branch. Then F0's plumbing with no behavior assigned to any level, F1's behavior-preserving parts, F3's HTTP length caps, F5's cleanup rule, F6a, F7, F8, F9, F10, F12c and F12d, F13, and F14b's continue-on-error and F14e. Old and new binaries mix freely. *Exit*:

- The D1 to D4 reproductions pass on both engines, including restore and install of snapshots that carry a regressed frontier.
- A bootstrap issues one read plan.
- A page-repair cycle completes on a staging cluster, with clip and drop counts reported.
- The TTL heap stays at most twice the TTL streams; the starvation reproduction shows no starvation; planner counters stay within bounds; capacity is at most 2 × len + 64 after flush and retention; the page-cache deque is bounded; in-memory admission is O(1).
- Shared-input `CompactCold`, legacy-pack migration and bucket purge pass on the Raft engine.
- No staged object is deleted after an ambiguous error.
- On a 3-node cluster a raise to a test level is refused when one node reports a lower maximum and applied otherwise; its snapshots carry the level frame, and a decoder from `e6d8d70` refuses them.

**B2, pack-reference driver, orphan sweep and decode support (about 2 weeks).** F2's driver, F14h, and F12a decode support. *Exit*: W2 with F10 keeps at most 64 refs per stream, and packs are GC'd after compaction; a rejected external append under a packed trickle reads correctly after compaction; the orphan sweep reclaims objects from injected ambiguous publishes after the grace and nothing that is referenced.

**B3, level Lb1, state hygiene (about 3 weeks).** F18 step 2, F3, F4a, `TidyStream`, F14a with F14g step 2, F14b's `DeferColdGc`, F14i and F12a emission, in one release that defines Lb1, together with the documentation they need. *Exit*:

- W4 keeps at most 1,024 receipt items per stream and producer bytes within Prod(s); a restore-versus-live differential and a madsim install mid-stream show identical producer state on every replica.
- W3 keeps at most 2 message records per stream.
- A delete and recreate with GC pending keeps every object of the new incarnation, and stream GC removes every object of the old one, external payloads included.
- A legacy producer with 1M receipts converges through bounded commands of under 10 ms of apply each.
- An EKS rolling upgrade at level 0, followed by a raise to Lb1 under live traffic, completes with zero acknowledged-data divergence.

**B4, level Lb2, sparse marks (about 4 weeks).** F1. Development starts in B2, in parallel with B3, and the release follows B3's, about 11 weeks after the start. *Exit*:

- W1 at 3M records holds marks ≤ ⌈cold MiB⌉ + 2 (about 9.2 KB) and dense entries equal to its unflushed records (about 22k); its snapshot spends at most 32 B per cold MiB on marks.
- RC-1 to RC-21 and DST invariants 9 to 12 pass.
- A legacy stream of 10M records seals in commands of at most 1M records, each under 10 ms of apply.
- The Lb2 raise is refused until every group reports a completed page-repair cycle.
- An EKS rolling upgrade followed by a raise to Lb2 under live traffic completes with zero acknowledged-data divergence.

**B5, level Lb3, external locators (about 3 weeks; may trail, in parallel with B6).** F5. *Exit*: W3 keeps at most 16 staged refs per stream; the ambiguous-commit seeds and Invariant 11 pass; no page entry is written before a proposal.

**B6, hot window and snapshot cadence (about 3 weeks).** F6b, F6c, F4b (defines Lb4), F12b, F12e, F14c and F14d, and compaction on by default. *Exit*: hot overhead in W1 is at most payload plus 24 B per record with F6b and 12 B with F4b; snapshot bytes written per appended log byte average at most 0.6 on W1 and on a uniform 128-group workload, with unpurged log within the node budget; compaction issues no LIST.

**B7, hardening (about 2 weeks, then ongoing).** The 72-hour soak gate (§7.5), F17, decisions on F15 and F16, and removal of the legacy paths (pre-level code, legacy pack migration) after the deprecation window. *Exit*: the soak gate passes and `operations.mdx` no longer names retention as the way to bound memory. *Status (2026-10-02):* the soak is deferred to a run on AWS (ECS or EKS) against real S3; it has not run, so the gate is open. (Its script drove the Pi Durable adapter and was removed with it.)

## 9. Rejected alternatives

**Require retention.** Applications publish snapshots and trim to stay bounded. This contradicts the principle and discards the history these streams exist for (#41). It makes Raft memory depend on applications or indexers staying alive; Pi's review measured unbounded growth during an indexer outage. It still leaves receipts, producer ids, the TTL heap and `Vec` capacity unbounded.

**Page the dense index into cold-index pages.** Record lookups would need asynchronous, two-phase resolution, extra S3 GETs per seek, and page writes that are never rolled back (the D3 defect class). Marks cost 16 B per MiB already. Moving marks into a page format v3 (about 128 B per GB of state) remains possible later if 16 B per MiB ever matters.

**Per-chunk marks.** Mark count would follow flush count rather than bytes, which for slow streams means one per pass or pack slice. Compacted chunks of up to 16 MiB would make scans up to 16 MiB, and records straddle chunk and pack boundaries anyway.

**Compress the dense index in memory** (varints or deltas, 1 to 2 B per record). Still O(records); only the constant changes, and lookups get slower.

**Trust a leader-resolved record number at apply within a bracket.** Apply would store a value it cannot recompute. Wrong cold bytes (D3), or a delete and recreate between resolution and commit, would then corrupt numbering permanently on every replica. Landing on the mark costs at most 1 MiB of untrimmed history and removes the command field.

**Keep the scalar cold frontier and raise it with `max`.** That fixes D1's reads but widens `snapshot_offset_aligned` to intra-message offsets in hot bytes below an external append. Coverage derived from the hot buffer is exact and needs no field.

**Reject recreating a name while its stream GC is pending.** Smaller than incarnation-scoped names, but it couples create availability to GC health, and the new incarnation would still share the old one's page keys while a sweep runs.

**Per-producer receipt rings without a stream budget.** They bound each producer's retries but not the stream: 4,096 producers with 64 receipts each hold 262k receipts, and far more bytes when those are batch receipts. Each producer's newest acknowledgement already covers the common retry.

**Gate everything through maintenance commands instead of levels.** Every new behavior would leave its natural command. `FlushCold` would need a second "seal" entry per flush, receipt trimming a separate command, and so on, which adds permanent machinery and Raft entries. A level costs one predicate per call site, and those predicates can be deleted later. `TidyStream` exists, but only for idle streams and catch-up.

**Bump `RAFT_GRPC_PROTOCOL_VERSION` with a full restart.** This breaks the graceful mixed-version rollouts shipped since 0.4 (#178, #200, #233).

**Keep external locators in state permanently** (as an earlier design proposed). That is 120 B per external append, 7.5 times the marks' constant, for no benefit, since the offload needs no byte copy.

**Make the legacy migration the permanent pack compactor.** It snapshots every group per pass, compacts one chunk per command, deletes replacements on ambiguous errors, and #278 plans to remove it.

**Exact receipts forever, or receipts in S3.** The first is unbounded. The second needs asynchronous lookups inside apply.

**Persist the state machine in RocksDB or another local KV store.** A non-goal per #268 and the maintainers' position in the #61 discussion: nodes stay close to stateless, and S3 cold-index pages are the sanctioned offload.

**Sweep stale TTL entries on every append.** This is O(stale) work and still allocates per append; arming one entry per stream removes the growth instead.

## 10. Open questions for maintainers

1. **Receipt window.** Is R = 1,024 receipt items per stream, plus each producer's newest acknowledgement, enough for the retry horizon of workflow consumers (#146)? Beyond the window, do you accept `204` without ranges (recommended, base-protocol conformant) over today's `409`? Should the response carry an explicit marker header?
2. **Producers.** Is 7-day idle expiry acceptable for Ursula's exactly-once promise? For the 4,096-producer cap, should a new producer that cannot evict an idle one get `429` (recommended) or evict the least recently seen producer anyway?
3. **Levels and upgrades.** Which versions must interoperate (N-1 to N only?), and is no-downgrade after a raise acceptable? Are four levels, each raised by an operator, acceptable? `AGENTS.md` and `loc-reduction-plan.md:10-12` still describe atomic upgrades, while practice since 0.4 is graceful mixed-version rollout.
4. **Spec and docs.** Will you amend these? Recommended: yes, each with the level that needs it.
   - `extensions.mdx` §2.2 (`:235`) and §6.10: a retention boundary in cold history of a JSON stream may take effect at the nearest preceding indexed record boundary, at most 1 MiB earlier, reported in `Stream-Retained-Offset` and `Stream-Record-First` (Lb2).
   - `extensions.mdx:753`: allow a sparse representation, with §6.1 (`:591`) keeping exact resolvability (Lb2).
   - `extensions.mdx:751`: state bootstrap parts for cold binary history (Lb1).
   - `extensions.mdx:786`, `durable-stream.mdx:329` and `exactly-once-writes.mdx:16, 31`: limit exact ranges to the receipt window and the newest sequence (Lb1).
   - `operations.mdx:135`: retention is not needed for memory.
5. **Response caps.** Can reads and `/bootstrap` cap responses at 8 MiB by default? Do the ursula-index source or the SDKs assume that a record read without `max_records`, or a bootstrap, returns everything up to the tail?
6. **Maximum hot age.** Is keeping slow streams' small tails hot for up to 5 minutes acceptable? It bounds how long records stay hot in quiet groups; in the healthy regime it changes slices little (288 against 308 per day), and the large slice reductions come from F10's batching and F2.
7. **Defaults.** Should compaction become on by default once discovery is debt-driven (F14d), S3 snapshots the default whenever a cold store exists (F12b, which also turns on S3-health leadership shedding), and snapshot cadence byte-based with a 1 GiB node log budget (F12e)? Is the inline backend meant for production clusters at all?
8. **Visible snapshots.** Decided 2026-10-03: externalize above the staging threshold at Lb5 (F16, §5.16), up to 1 GiB.
9. **Tenant tombstones.** Who acknowledges metered usage so rows can be pruned, and should erasure fences move to the meta group (F15)? Until then, O(buckets ever) rows per group are the documented exception to I2.
10. **Mark granularity.** Is a 1 MiB block right, or should cold reads by record trade 16 times more marks (256 B per MiB) for a 64 KiB scan bound? Per node, marks cost 16 MiB per TiB of cold history, about 65 MiB per day at a sustained 50 MB/s of ingest. Recommended: commit now to thinning history older than 30 days to 8 MiB marks, at a later level, once mark bytes on any node exceed 1 GiB.
11. **Anchor cache.** Should a node-local anchor cache keyed by stream incarnation, exact once F14g makes incarnations unique, remove the front scan for client loops that read by record?
12. **Scope of #17.** Does the target apply to disk-WAL clusters and to the default single-node in-memory mode as well? This document assumes yes.
13. **Binary streams.** Should #170 framed binary records reuse marks over a length-prefixed framing, rather than add a second dense index later?
