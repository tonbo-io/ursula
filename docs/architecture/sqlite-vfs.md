# SQLite on Ursula: the replicating VFS

The VFS runs an unmodified SQLite application with an Ursula stream as the source of truth. A
commit is local: at SQLite's commit point the VFS appends the transaction's final pages to a log it
owns, and a process-wide driver replicates the log to the stream in the background. A commit is
durable once the stream acknowledges it, and `await db.synced()` is the output gate.

Code: `clients/sqlite-vfs` (the loadable extension, Rust, a standalone workspace) and
`clients/sqlite-ursula` (TypeScript: the loader, `open`, `importSqlite`, the error classes, the
event pump, `gate`, the Pi Durable adapter in the `/pi` entry, Tonbo's open by name in the `/tonbo`
entry, Pi storage on a Tonbo database by name in the `/tonbo/pi` entry, the e2e suites and the
benchmark). The server
contract is the [extensions specification](../web/src/content/docs/pages/specs/extensions.mdx). User
docs: [SQLite on Ursula](../web/src/content/docs/pages/examples/sqlite-vfs.mdx).

Each heading below is a label. Code comments cite `design: <Heading>` for a heading here and
`extensions: <Heading>` for a heading of the extensions specification. Tests are named by file and
test name: Rust paths are relative to `clients/sqlite-vfs/src/`, TypeScript paths to
`clients/sqlite-ursula/`.

## Model

```ts
import { open } from "@tonbo/sqlite-ursula";

const db = await open("https://streams.example.com/acme/notes");
db.prepare("INSERT INTO notes(body) VALUES (?)").run("hello"); // committed on this host
await db.synced();                                             // in the stream, still the owner
res.end("saved");
```

**The one rule:** before anything outside the process acts on what you wrote or read (a response,
a queue acknowledgement, a call to another service), `await db.synced()`.

| the application | what happens |
| --- | --- |
| runs a statement or COMMIT | local work: SQLite, plus capture, compression and one log write |
| awaits `synced()` | one append round trip while writing, one leader read when idle |
| loses the stream | commits continue, `synced()` waits, status says `retrying`, past `maxPendingBytes` writes fail with `backlog_full` |
| is taken over | statements and `synced()` throw `UrsulaResetError` (`fenced`), `reset` listeners run |
| crashes | unacknowledged commits come back from the log at the next open and are resent |

Every commit ends acknowledged or discarded, and only an unacknowledged commit is ever discarded,
together with a reset. Only an `open()` that reaches the stream takes it over. Every claim the
replicator makes on its own is conditional and loses to any newer owner. Unacknowledged commits do
not survive a power loss before the next log fsync, and there is no local durability mode.

## Shape

```text
 application connections (node:sqlite, Pi's driver, any host of the SQL functions)
          SQLite pager and WAL
                |  xWrite xRead xShmLock xFileControl xOpen xAccess xDelete
          ursula VFS (vfs.rs): capture, PHASETWO confirmation, page-1 marker,
                |               WAL name, reset fences, fsync hooks
   generation directory: db, db-uwal, db-shm, log segments (replog.rs)
                |
   process-wide driver (driver/): a deterministic core with a replicator per database
   (replicator/, a pure state machine), blocking HTTP workers, disk-job threads,
   and a snapshot thread per binding (snapshotter.rs)
                |
   gateway or single node --> the stream's leader
```

- The extension registers a VFS named `ursula` over SQLite's `unix` VFS and makes it the default.
  The registry (`db.rs`) keeps one binding per stream per process, shared by its connections.
- The replicator is `Replicator::on(event, now_ms) -> Vec<Action>`: no I/O, no randomness, time
  passed in. The driver's core (`driver/core.rs`) executes its actions and is itself
  deterministic, so tests drive it with a virtual clock.
- The runtime (`driver/runtime.rs`) runs the core on one driver thread. Each stream has two
  request queues with a blocking ureq worker each: reads (the reconcile's, a build's, the
  `synced()` probe) and everything else, so a probe never waits behind an append and no stream
  waits behind another. Each disk job (materialize, catch-up, build, publish) runs on a thread of
  its own. Calls into the driver return at once, and callers wait on a table of outcomes.
- Every bound database has a snapshot thread with a private connection through the `ursula` VFS.

## Commit path

### Capture

The VFS tracks the WAL write lock in `xShmLock` and records, for each frame a write transaction
writes, its page number and offset. The commit point is the write of the page data of the frame
whose header carries a non-zero database size. There:

1. Admission ([Backpressure](#backpressure)) already ran at the transaction's first WAL write.
2. The commit limit: a commit whose pages take more than `URSULA_VFS_MAX_COMMIT_BYTES` (128 MiB
   by default, clamped to 4096 bytes through 1 GiB) fails alone as `too_large`
   (`SQLITE_IOERR_AUTH`, action `fix`). The page count comes from the frames the VFS recorded, so
   nothing is waited for, read back or encoded first: a later frame of a page counts once, frames
   a savepoint rollback undid are gone, and pages above the commit's size are dropped.
3. The commit waits while another connection's logged commit is undecided
   ([Publication](#publication)) or the snapshot window is open, at most 1 s for each.
4. The VFS reads the final image of every page the transaction wrote back from the WAL file and
   checks each against the crc32c of what this process wrote. It takes the commit frame's own page
   from the buffer and drops pages above the commit's size. A commit that leaves WAL mode or
   4096-byte pages fails alone as `unsupported`.
5. It encodes one `USQ2` COMMIT frame ([Frames](#frames)), zstd-compressed, into one buffer, and
   drops the pages.
6. It appends a COMMIT record to the log ([Log](#log)): one positioned write, no fsync. The
   position becomes `logged`.
7. It forwards the commit frame's page data and returns. SQLite publishes and releases the lock.

The log write precedes the commit frame's page data, so every commit SQLite could recover is in
the log (L2). A failed log write fails the write before that page exists, so the frame's checksum
cannot match, and the partial record is cut. Failures before step 6 roll back with nothing logged.
A commit refused by the limit never reaches the log, so the replicator, `synced()` and
`pending_bytes` never see it, and SQLite rolls the whole transaction back with the database
usable. A commit's pages are in memory at once. At its peak the writer holds about the pages plus
twice the compressed record, and the replicator keeps the frame until the stream acknowledges it.
WAL writes go straight to the WAL file. A WAL write without the WRITE shm lock (EXCLUSIVE locking
mode) is refused as `unsupported`.

Tests:

- Pages and shapes: `capture/tests.rs` `one_commit_captures_every_page`,
  `in_place_rewrites_inside_a_transaction_capture_the_newest_image`,
  `a_commit_frame_after_spilled_frames_of_its_page_takes_the_buffer`,
  `pages_above_the_commit_size_are_dropped`,
  `a_savepoint_rollback_that_rewrites_a_frame_drops_what_it_undid`,
  `a_commit_leaving_wal_mode_or_4096_byte_pages_is_refused`, and `test/core.e2e.ts` "captures a
  spilled transaction rolled back to a savepoint: only what it committed reaches the stream".
- The commit limit: `capture/tests.rs`
  `a_commit_above_the_limit_is_refused_before_any_page_is_read_back` (every shape test also checks
  the count against the record), `vfs/tests.rs`
  `a_commit_above_the_limit_fails_alone_with_nothing_logged`, `config.rs`
  `the_commit_limit_is_clamped_to_one_page_through_one_gib`, `driver/tests.rs`
  `a_logged_commit_above_the_limit_still_replicates`, and `test/limits.e2e.ts` "refuses a
  transaction above the commit limit alone, as too_large, and commits the next", "rolls a refused
  transaction that spilled back whole, and the next commit writes over its frames".
- Read-backs: `capture/tests.rs` `a_read_back_that_differs_is_a_lost_write`, `vfs/tests.rs`
  `pages_read_back_are_checked_against_what_was_written`.
- A failed log write: `replog/tests.rs` `an_injected_write_failure_cuts_the_segment_back`.
- EXCLUSIVE mode: `vfs/tests.rs` `a_wal_write_without_the_write_lock_is_unsupported`,
  `publication/tests.rs` `exclusive_locking_mode_is_refused`, `test/core.e2e.ts` "refuses a write
  in EXCLUSIVE locking mode alone, as unsupported".
- The steps under SIGKILL: `test/crash.e2e.ts`, steps `admission`, `capture`, `encode` (the commit
  never happened) and `logged`, `forward`, `phasetwo` (the reopen replays it).

### Publication

A logged commit is final: shipped, and replayed whatever happens next. What remains is whether
SQLite published it locally, since nothing built without it may be logged after it.

- SQLite sends `SQLITE_FCNTL_COMMIT_PHASETWO` on the committing connection's main db handle right
  after it releases the WAL write lock of a committed transaction, and only then. When it is the
  very next call on that handle after the unlock, the commit is **confirmed**.
- Any other call there first (a read-mark unlock, a file lock, a new write lock, a close) leaves it
  **unconfirmed**: the binding resets `local_damage`, and the reopen replays the log, which commits
  it.
- While a logged commit is undecided, another connection reaching its commit point waits at most
  1 s, then the binding resets. Nothing is logged on top of an undecided commit.
- The PHASETWO handler never returns an error, since SQLite reports a published commit as failed
  when it gets one.

The rule is a pure state machine over a handle's calls (`publication.rs` `HandleState`). It reads
no wal-index state and holds no SQLite lock past its release.

Tests: `publication/tests.rs`
`a_commit_is_confirmed_only_by_phase_two_right_after_its_write_unlock`,
`a_published_commit_without_phase_two_is_unconfirmed`, `an_unpublished_commit_is_unconfirmed`,
`a_new_write_lock_before_phase_two_leaves_the_commit_unconfirmed`,
`a_close_after_a_logged_commit_leaves_it_unconfirmed`, `the_phase_two_handler_never_fails`, and
against node:sqlite in `test/core.e2e.ts` "basic: one connection", "spill: a transaction that
outgrows the page cache before its commit", "multi: two connections committing in turn" and
"attachfail: a commit published locally whose attached database fails phase one is unconfirmed, and
the reopen commits it". The reopen after the reset: `test/reopen.e2e.ts` "replays the unconfirmed
commit after a local_damage reset".

### Fsyncs

Nothing is fsynced per commit. `xSync` of a bound file is a no-op whatever `PRAGMA synchronous`
says.

- **F1, log before db:** at `SQLITE_FCNTL_CKPT_START`, sent before a checkpoint copies its first
  page, the VFS fdatasyncs the log if it holds unsynced records. Every page that reaches the db
  file comes from a durable record. The snapshot thread's private connection takes the same hook.
- **F2, db before a base:** before a new WAL generation and a WAL truncate to nothing, the VFS
  fsyncs the db file, then a BASE record names the last confirmed commit the db holds.
- **F3, before trimming:** segments are deleted only after the log is fsynced with the BASE and ACK
  records that make them unnecessary.
- **Claims:** a CLAIM record is fsynced before its claim is sent ([Claims](#claims)).

A failed fsync resets `local_damage`. The cost is one log fdatasync per checkpoint and one db
fsync per WAL wrap.

Tests: `test/crash.e2e.ts`, steps `checkpoint_start`, `checkpoint_synced`, `checkpoint_page` (F1)
and `base_start`, `base_synced`, `based` (F2), each under autocheckpoint and TRUNCATE checkpoints,
and `trim_unlinked` (F3). Claims: `driver/tests.rs` `a_failed_claim_write_sends_no_claim`. Trims:
`replog/tests.rs` `a_roll_carries_the_state_and_trim_deletes_what_base_and_ack_cover`,
`a_failed_unlink_in_a_trim_keeps_the_log_contiguous`.

### Backpressure

`pending_bytes` counts compressed frame bytes logged and not acknowledged. At `maxPendingBytes`
(64 MiB by default) a write transaction fails at its first WAL write with `backlog_full`
(`SQLITE_IOERR_AUTH`, action `retry` after `synced()`), nothing written, the database usable. A
transaction is admitted while nothing is pending, and its commit is at most the commit limit
([Capture](#capture)). This is the only admission rule: a `402`, an authorizer's `403` or a
refused token is a replication condition in status, and commits continue until the backlog fills.
The bound also caps what a reset discards.

Tests: `driver/tests.rs` `admission_counts_commits_the_driver_thread_has_not_seen`,
`test/open.e2e.ts` "refuses a write past maxPendingBytes with nothing committed, and keeps the
error across a helper", `test/outage.e2e.ts` (commits go on, nothing resets, `backlog_full` past a
small bound, then a full catch-up).

## Replication

### Frames

Stream frames are `USQ2`, little-endian, under the stream content type
`application/vnd.ursula.sqlite-vfs.v2`.

```text
frame  = "USQ2" | u8 kind | u32 body_len | u32 crc32c(kind | body) | body
kind 1 CLAIM   body = u64 epoch | [16] activation
kind 2 COMMIT  body = [16] lineage | u64 position | zstd(record)
kind 3 PART    body = [16] lineage | u64 position | u32 index | u32 count | chunk
record = u32 db_pages_after | u32 n | n x (u32 pgno | [4096] image)      (pgno ascending)
```

- The commit id `(lineage, position)` is in the clear, so a reconcile matches without
  decompressing. `lineage` is 16 random bytes per local lineage, `position` the record's place in
  its log. Positions continue across rebuilds.
- Frames carry no epoch, seq or stream identity. Bytes logged at commit time go out under any
  epoch, so a resend needs no re-encoding and nothing relies on zstd being deterministic.
- zstd frames declare their content size, carry a checksum and use a window of at most 2^22. No
  frame body exceeds 4 MiB.
- A commit above the batch limit ([Appends](#appends)) goes out as a run of PART frames,
  consecutive slices of the COMMIT body's compressed record (`count >= 2`), one append each.
- Readers apply a COMMIT, and a complete PART run `0..count-1` as one COMMIT. Any other frame
  inside a run voids it: its owner resends the commit whole after a claim.
- Readers join a run in memory. A run longer than any COMMIT frame holds (every part but the last
  as long as part 0, against a body of at most 2^32 - 1 bytes) is undecodable at its first part,
  before anything is joined. Readers apply every commit a frame holds, whatever the commit limit.

Tests: `codec/frame/tests.rs` `frames_round_trip`, `every_strict_prefix_is_partial`,
`a_flipped_bit_is_never_another_frame`, `a_complete_run_is_the_commit`,
`a_foreign_frame_voids_the_run`, `a_part_of_the_same_commit_out_of_turn_voids_the_run`,
`a_run_restarted_whole_completes`, `part_frames_cut_a_commit_and_join_back_to_it`,
`a_run_no_commit_frame_holds_is_refused_at_its_first_part`,
`a_huge_declared_size_fails_fast_with_bounded_memory`, `a_frame_with_a_larger_window_is_refused`.
Frozen bytes: `codec/vectors/mod.rs` `the_claim_frame_is_frozen`, `the_commit_frame_is_frozen`,
`the_part_frames_are_frozen`.

### Appends

```text
POST {stream}
Content-Type: application/vnd.ursula.sqlite-vfs.v2
Content-Length: <n>
Stream-Incarnation: <token>
Producer-Id: sqlite-ursula-vfs/2
Producer-Epoch: <E>
Producer-Seq: <s>
Producer-Digest: <64 hex digits>     BLAKE3-256 of the body, on every write, claims included
User-Agent: sqlite-ursula-vfs/<version>
body = one CLAIM frame, or one sealed batch
```

- **Offsets** are the `u64` the server renders, compared numerically everywhere: receipts, reads,
  HEAD, the log and snapshot headers.
- **Limits.** The request limit `L` is the smallest of `Ursula-Max-Request-Bytes` from the latest
  HEAD, any limit a typed `413` named after it, and `URSULA_VFS_MAX_REQUEST_BYTES`. A limit below
  4 KiB blocks `request_too_large`. The batch limit is `min(L, 4 MiB)`.
- **One append in flight per database.** With none in flight, the replicator seals whole frames of
  consecutive pending records from the first unacknowledged one, at most the batch limit, ending
  exactly at a position the snapshot thread asked for ([Snapshots](#snapshots)). A seal fixes
  `seq = last + 1`, the body and its digest until a definite answer. Commits made meanwhile ride in
  the next append.
- **Receipt contiguity.** A receipt's `Stream-Offset` must equal the end of the previous
  acknowledged write or of this owner's claim.
- **Run tail.** `acked_offset` stays a commit boundary. Progress inside a PART run is the run tail
  (`Ctx::tail`), which the next part's receipt and a `synced()` probe check against, so a probe
  never reads the owner's own parts as foreign.
- **Retries** resend the identical request with no budget, on a fresh connection after a request
  that got no answer. Backoff doubles from 25 ms to 5 s with full jitter and never comes before
  `Retry-After`, capped at 60 s. A blocked condition retries every 30 s, and `unauthorized` at once
  when the token changes ([Transport](#transport)).

Tests:

- Batches and the ACK: `replicator/tests/machine.rs`
  `a_batch_takes_every_pending_record_and_the_ack_advances`,
  `a_batch_ends_exactly_at_a_boundary_and_within_the_limit`.
- Identical retries: `replicator/tests/machine.rs`
  `a_lost_answer_resends_the_identical_request_on_a_fresh_connection_after_backoff`,
  `retry_after_is_a_floor_capped_at_a_minute`, `late_and_duplicate_answers_are_ignored`, and the
  world's property 3 (`replicator/tests/world.rs`
  `the_replicator_keeps_its_properties_against_the_model`, with
  `negative_control_a_renumbering_replicator_sends_two_bodies_under_one_key`).
- Across a crash: `driver/tests.rs`
  `an_append_applied_before_a_crash_is_acknowledged_by_the_reopen_and_not_resent`,
  `an_append_never_applied_before_a_crash_is_resent_identically_under_the_new_epoch`.
- Outages and failover: `driver/tests.rs`
  `a_ten_minute_outage_ends_in_full_catch_up_without_a_reset`, `test/outage.e2e.ts`,
  `test/failover.e2e.ts` "kills the stream's leader with commits unacknowledged: commits go on, no
  reset, a full catch-up on the new leader".
- PART runs and the run tail: `replicator/tests/parts.rs`
  `seal_cuts_a_frame_above_the_batch_limit_into_a_run`,
  `a_frame_above_the_batch_limit_goes_out_as_parts_acknowledged_with_the_last`,
  `a_probe_inside_a_run_reads_from_the_run_tail_and_never_fences`, and `test/parts.e2e.ts` "a 1 MiB
  transaction above a 64 KiB request limit commits in parts", "a 9 MiB transaction at the default
  limit commits in 4 MiB parts", "a crash between parts leaves no part of the transaction applied,
  and the reopen sends it whole".
- Content-Length: `transport/http/tests.rs` `every_body_carries_content_length`.

### Answers

The replicator reads `Producer-Outcome` before `Producer-Epoch` and decides by what was in flight.
Rows match in order (`replicator/answer.rs`).

| answer to the in-flight append `(E, s)` | meaning | next |
| --- | --- | --- |
| `200` at the expected tail | applied | acknowledge |
| `204` at the expected tail | an earlier attempt applied this body (the server compared digests) | acknowledge |
| `200` or `204` at another offset | bytes landed between our writes | reset `stream_state` |
| `403 key-conflict`, or `204 receipt-evicted` | another body holds our key, or a write past `s` in our epoch | reset `fenced` |
| `403 producer-expired` | the server forgot our epoch | continuation claim at own + 1 ([Claims](#claims)) |
| `403 epoch-closed`, or another `403` with `Producer-Epoch` | a newer claim or a history event closed our epoch | reset `fenced` |
| `409` or `400` with `Producer-Expected-Seq` | the server lost acknowledged state under one token | reset `stream_state` |
| `409` with `Stream-Closed` | someone closed the stream | reset `stream_state` |
| `412` or `404` | another history, a deleted stream, or misrouting | HEAD without a precondition ([History](#history), [Identity](#identity)) |
| `413` with `Ursula-Max-Request-Bytes` | a named limit | lower `L`, and below the body a continuation claim and a re-cut, below 4 KiB blocked `request_too_large` |
| `413` without a limit header | a layer outside the specification | HEAD, then the same as a named limit |
| `401` | token refused | reread it and resend, the same token refused twice blocks `unauthorized` |
| `402`, or `403` without `Producer-Epoch` | quota, or an authorizer | blocked `payment_required` or `forbidden` |
| a 3xx | a redirect, never followed | resent 3 times, then blocked `redirected` |
| no answer, a reset, a timeout, a 5xx, `429`, `408` | not acknowledged | resend identically after backoff |
| any other status | a request the server cannot take | blocked `rejected` |

A claim is answered by the same rules, except that `200`, or `204` with `Stream-Offset`, means our
claim holds `E` and its reconcile follows ([Reconcile](#reconcile)), and any `403` with
`Producer-Epoch` or `204 receipt-evicted` means the claim lost. `receipt-evicted` on a claim
implies our digest, since a key conflict is decided first.

- No refusal is read as "not applied". Each leads to an identical retry, a claim, or a reset. A
  reset after a typed fence keeps pending records for the next open's reconcile.
- `epoch-closed` never acknowledges: the server checks the epoch before identity, so
  `Producer-Closed-Seq` at or above `s` proves that a write under our key applied, not that our
  body did.
- An idle owner's `producer-expired`, when the pre-check could not find the owner's own claim
  frame, resets `stale` (action `reopen`) instead of `fenced`, and the reopen takes over with the
  pending commits.
- A HEAD follows three lost answers in a row for a body above 64 KiB, since a layer that refuses a
  large body without reading it looks like a lost answer.
- Every `410` and `416` of a read names the token of the stream that refused it, and comes before
  the `412`. A `synced()` probe or a pre-check whose `410` names our token resets with no HEAD: a
  probe `fenced`, a pre-check `stale`. A `410` that names another token, or none, sends a HEAD,
  which takes the history path, or with our token still there resets the same way. A `416` of a
  replicator's read that names our token is resent after backoff. Another token, or none, sends
  the HEAD of a `412`.

Tests: `replicator/tests/answer_rows.rs`, one test per row
(`append_row_1_a_200_at_the_expected_tail_acknowledges` through
`append_row_16_any_other_status_blocks_rejected`) and per claim row
(`claim_row_1_a_receipt_wins_and_names_what_it_closed`,
`claim_row_2_any_epoch_403_or_receipt_evicted_loses`,
`claim_rows_otherwise_follow_the_append_rules`). Also `replicator/tests/machine.rs`
`a_401_rereads_the_token_once_then_blocks`, `redirects_are_resent_a_few_times_then_block`,
`a_typed_413_below_the_smallest_limit_lowers_it_and_blocks`,
`a_410_on_a_fresh_probe_fences_once_head_shows_our_token`,
`a_410_naming_our_token_on_a_probe_fences_with_no_head`,
`a_416_naming_our_token_on_a_probe_is_resent_with_no_head`, `replicator/tests/parts.rs`
`a_413_inside_a_run_claims_the_next_epoch_and_seals_again_smaller`,
`three_lost_answers_for_a_large_body_send_a_head_whose_limit_recuts`, and
`replicator/tests/rekey.rs`
`a_provisional_claim_meeting_an_evicted_receipt_claims_the_next_epoch_and_expiry_goes_stale`,
`a_410_on_a_probe_after_a_history_event_takes_the_history_path`,
`a_416_on_a_read_sends_head_unless_it_names_our_token`.

### Claims

| | takeover | continuation |
| --- | --- | --- |
| made by | `open()` with the stream answering | the replicator: `producer-expired`, a re-cut, a re-key while bound, a provisional open after its pre-check |
| epoch | above every epoch seen (own CLAIM records, the snapshot's epoch, a `403`'s `Producer-Epoch`), higher after any answer but its receipt, at most 16 rounds | exactly own + 1 |
| loses | after 16 lost claims (`claim_races`) | at the first answer but its receipt, or a foreign frame in its reconcile: reset `fenced` |

- Before sending a claim the VFS appends a CLAIM record `(epoch, activation)` and fsyncs it, so a
  claim frame whose activation is in the log is ours whether or not its answer arrived.
- A claim's identity is its `Producer-Digest`. The server keeps a claim's digest for its epoch's
  lifetime, so another claimant of the same epoch gets `403 key-conflict` and a repeat of ours gets
  its receipt.
- A claim's answer reports the epoch it closed in `Producer-Closed-Epoch` and
  `Producer-Closed-Seq`. When that is the epoch the binding streamed in, the reconcile cross-checks
  it: a batch the binding sent above `Producer-Closed-Seq` found in the stream resets
  `stream_state`.
- The activation id is 16 random bytes drawn by each open that starts a binding, or the
  `activation` option of `ursula_open`, and the CLAIM frame carries it. An open that joins a
  binding, or takes back one still draining, draws nothing. Claims of one epoch from different
  activations have different digests. Two opens with one `activation` that compute one epoch send
  identical claims, and the second binds on the repeat ([Clones](#clones)).
- The epoch floor refuses every tokened claim at or below the highest retired epoch. A
  continuation at own + 1 after idle expiry applies when own is the floor. A takeover learns the
  floor from `Producer-Epoch` and goes above it.

A claim nobody timed never takes a stream over: a process back from a partition cannot fence the
owner an explicit open installed, unless it opens a copy of that owner's own cache or shares its
activation, as a memory clone or a constant `activation` does ([Limits](#limits)).

#### Pre-check

A provisional open ([Open](#open)) and a re-key of a continuation whose latest claim applied past
its acknowledged offset do not claim at once. They first:

1. Send a HEAD, which gives the token, the cluster id and the request limit.
2. Read from the log's acknowledged offset to the tail that HEAD named, with `consistency=leader`
   and the incarnation precondition. Any foreign frame resets with nothing sent. A `410` that
   names our token resets `stale`. One that names another token, or none, sends a HEAD: our
   token still there resets `stale`, another token takes the history path.
3. Only then send the log's latest claim again, same epoch and body. Nothing is logged before. A
   repeat receipt, or `receipt-evicted`, means that claim applied before, and one continuation
   claim of the next epoch follows. That holds for a repeat whose receipt ends at or below the
   acknowledged offset too, as when the owner bound and never wrote under the claim. A receipt
   that starts below the acknowledged offset and ends past it resets `stream_state`.

A provisional open thus claims at most one above the latest claim of its own the stream applied,
and never above an owner an explicit open installed before the pre-check read the tail, unless
the open is of a copy of that owner's own cache, whose claims are its own ([Limits](#limits)). A
generation whose log holds no CLAIM record has nothing to continue and does not open
provisionally.

Tests:

- Claims: `replicator/tests/machine.rs`
  `producer_expired_claims_exactly_own_plus_one_after_persisting_it`,
  `a_takeover_retries_above_every_epoch_seen_then_gives_up`,
  `a_batch_found_above_the_closed_seq_resets_stream_state`, `driver/tests.rs`
  `idle_expiry_claims_own_plus_one_and_carries_on`,
  `a_continuation_after_another_owners_claim_loses_and_fences`,
  `a_failed_claim_write_sends_no_claim`,
  `each_open_draws_an_activation_and_a_reopen_continues_the_claims_before_it`, `db.rs`
  `each_open_draws_its_own_activation`, model
  `model/tests.rs` `a_second_claimant_of_an_epoch_gets_key_conflict_and_a_clone_too`, `idle_expiry_raises_the_floor`.
- Background claims never fence an explicit owner: the world's properties 4 and 20, and
  `replicator/tests/world.rs` `offline_restarts_never_fence_an_owner_an_explicit_open_installed`,
  `an_unapplied_claim_in_the_log_never_fences_an_explicit_owner`,
  `a_re_key_while_reconciling_never_fences_an_explicit_owner`, `test/provisional.e2e.ts` "never
  fences an owner an explicit open installed, after two offline opens in a row".
- Pre-check: `replicator/tests/rekey.rs`
  `a_provisional_open_sends_its_logs_claim_again_after_the_pre_check`,
  `a_provisional_pre_check_resets_on_a_foreign_frame_with_nothing_sent`,
  `a_provisional_claim_found_in_the_pre_check_claims_on_after_idle_expiry`,
  `a_provisional_claim_repeated_at_the_acknowledged_offset_claims_the_next_epoch`,
  `a_provisional_claim_repeated_across_the_acknowledged_offset_resets_stream_state`,
  `a_receipt_below_the_acknowledged_offset_resets_stream_state_unless_a_resent_claims_repeat`,
  `driver/tests.rs` `a_provisional_open_after_another_owners_takeover_is_fenced`,
  `a_provisional_open_of_an_owner_that_never_wrote_goes_live`, `replicator/tests/world.rs`
  `negative_control_guarding_the_repeat_start_resets_an_idle_owners_provisional_open`,
  `test/provisional.e2e.ts` "resets `fenced` when another owner took the stream over during the
  outage, and its commits never reach the stream" and "goes live over an owner that never wrote".

### Reconcile

One routine runs after every claim. It reads from the durable acknowledged offset to the claim's
start, with `consistency=leader` and the incarnation precondition, and sorts every frame by
activation and by commit id plus bytes, never by `(epoch, seq)` (`replicator/reconcile.rs`).

| frame | takeover | continuation |
| --- | --- | --- |
| a CLAIM with our activation or one in the log | skip | skip |
| a COMMIT or complete PART run with the next pending record's commit id and bytes | acknowledged | acknowledged |
| a foreign CLAIM | skip | reset `fenced`, unless a CLAIM of our own follows it in the range, which supersedes it |
| a foreign COMMIT, or our commit id with other bytes | divergence: the pending tail is discarded, the generation caught up or rebuilt ([Materialize](#materialize)) | reset `stale` |
| undecodable bytes | refuse `stream_state` | reset `stream_state` |
| `410` at the start | the latest snapshot's header: our lineage at or below our durable acknowledged position continues from its offset, else a divergence | the same, except it never skips unread bytes: the snapshot's offset must equal the reconcile's start, else reset `stale` |

The remaining pending records then go out under the new epoch from seq 1, in order, same bytes.
Once a claim at `E'` applies, no older-epoch frame of the producer can apply, and the read sees
every frame applied before the claim, so each inherited record has landed or never will. Clones
share a lineage, so a snapshot above the durable acknowledged position is never trusted.

Tests: `replicator/tests/reconcile_rows.rs`, one test per row:
`row_1_a_claim_with_our_activation_is_skipped`, `row_2_the_next_pending_commit_is_acknowledged`,
`row_3_a_foreign_claim_is_skipped_by_a_takeover_and_fences_a_continuation`,
`row_4_a_foreign_commit_diverges_a_takeover_and_resets_a_continuation_stale`,
`row_5_undecodable_bytes_end_a_takeover_and_a_continuation_with_stream_state`,
`row_6_a_410_at_the_start_rebuilds_a_takeover_and_resets_a_continuation_stale`,
`row_6_a_410_continues_from_our_lineages_snapshot_at_or_below_acked`,
`row_6_a_410_with_a_snapshot_that_does_not_cover_us_diverges`. Superseded claims:
`replicator/tests/world.rs`
`a_crash_before_the_bind_supersedes_a_foreign_claim_and_goes_stale_on_a_foreign_commit`,
`replicator/tests/rekey.rs`
`a_later_claim_of_an_activation_in_the_log_supersedes_a_foreign_claim_but_not_its_commits`. Clones:
`replicator/tests/world.rs` `a_clones_snapshot_never_covers_the_originals_commit`. Takeovers:
`driver/tests.rs` `a_takeover_fences_the_owner_which_keeps_its_tail_then_discards_it_and_rebuilds`,
`a_410_at_a_takeover_with_a_pending_commit_rebuilds_from_the_snapshot`.
End to end across hosts, with the superseded lineage absent above its acknowledged position and after the takeover's claim: `test/stale.e2e.ts` "S2: a cache killed with an unsent tail rebuilds without it after another host wrote" and "S2b: a cache killed with an unsent tail rebuilds without it after another host wrote, from the latest snapshot once retention passed it".

### Replicator

```rust
enum Replicator {
    Activating { kind: ClaimKind, step: Step, ctx },   // claim, reconcile, apply, rebuild, pre-check, 410 rule
    Streaming { epoch, seq, work: Work, probe: Option<Probe>, ctx },
    Reset(ResetKind),
    Refused(Refusal),                                  // claim_races at open
    Stopped,
}
enum ClaimKind { Takeover { round, rekeyed }, Continuation { own, resent } }
enum Condition { Retrying, Blocked(BlockedReason) }
enum BlockedReason { RequestTooLarge, Unauthorized, PaymentRequired, Forbidden, Redirected, WrongCluster,
                     NoServerIdentity, ProtocolMismatch { found }, Rejected }
enum ResetKind { Fenced, Stale, Rewound, Deleted, LocalDamage, StreamState, Internal }
// events:  Logged, Answer, Tick, Boundary, Snapshotted, Sync, CaughtUp, Stop
// actions: Send, PersistClaim, ReKey, PersistAck, Wake, Synced, Abandoned, Timer, Condition,
//          Apply, Rebuild, Bound, Reset, Refuse, Boundary
```

A blocked condition is a `Condition` of a streaming replicator, not a state of its own. The driver
makes `PersistClaim` and `ReKey` durable before it carries out the actions that follow them.

A proptest world (`replicator/tests/world.rs`) runs owners against the model server over a network
that delays, reorders, loses and duplicates requests and answers, with scripted faults, idle
expiry, history events, misrouting, foreign writers, retention, clones, copies of a cache opened
on another host, restarts, reopens that keep the activation, provisional restarts and power
losses, then heals. One seed in eight runs every open under one activation. Its monitors check
twenty-two properties, among them:
acknowledged never regresses (1), acknowledged records are a log prefix in stream order (2), one
lineage never sends two bodies under one key (3), a continuation lands exactly one above the
latest applied claim (4), no older key applies after a claim (5), a typed fence keeps pending
records (6), every run ends acknowledged, retrying, blocked or reset (7), of two divergent clone
commits at most one is acknowledged (8), `Synced` never fires past a foreign frame (9), a re-key
happens only when the history inherits the generation (15), nothing applies on another deployment
(16), no acknowledged commit is lost, across clones and copies too (19), a background claim never
applies above an explicit owner's (20), an owner alone on the stream never resets `stream_state`
(21), and every producer key the server acknowledged carries one body within a history, whoever
sent it (22). Negative controls must fail them.

Tests: `replicator/tests/world.rs` `the_replicator_keeps_its_properties_against_the_model`,
`negative_controls_fail_the_proptest_too`, `the_proptests_shrunk_counterexamples_pass`, and each
`negative_control_*` test there.

### Snapshots

A snapshot is due when the frame bytes acknowledged since the latest one exceed `max(database
size, 8 MiB)`. The snapshot thread:

1. Opens its private connection through the `ursula` VFS and checkpoints passively.
2. Opens the window once no logged commit is undecided and no other connection checkpoints
   (commit points wait while it is open, at most a second).
3. Takes `k`, the last logged commit, and asks the replicator for a batch ending exactly at `k`.
4. Checkpoints every frame, begins a read that sees exactly `k` and closes the window.
5. Copies the pages into a temp file 256 pages at a time, fsyncs the db and appends BASE(`k`).
6. Waits for `k`'s acknowledgement, whose receipt names `W`, fsyncs ACK(`k`, `W`) before the
   publish, and writes the `USS3` body to a temp file while its digest is computed.
7. Publishes, keyed by `(W, digest)`: a body of at most `L` in one `PUT {stream}/snapshot/{W}`, a
   larger one in parts when HEAD's `Ursula-Max-Snapshot-Bytes` holds it in at most 4096 parts of
   size `L`.
8. Moves retention to the snapshot the stream held before, one behind, monotonic, after the `204`.

`USS3` is a 56-byte header (`"USS3" | u64 W | u64 highest epoch claimed before W | [16] lineage |
u64 last position | u32 pages | u32 crc32c(image) | u32 crc32c(header)`) that verifies on its own,
then the zstd image. ACK(`k`, `W`) is durable first, so the owner's own snapshot never runs ahead
of the durable acknowledged position the `410` rule compares with.

- **Parts.** Each part is staged with `POST {stream}/snapshot/{W}/parts` and answered `201` with a
  part id the server mints. A part whose answer was lost is staged again under a new id. The
  commit `PUT {stream}/snapshot/{W}` carries `Stream-Snapshot-Parts` and the digest and lists its
  ids. A `400` stages every part again once. A `429` to a part is the stream's staged-parts limit:
  the attempt discards the stream's staged parts (`DELETE {stream}/snapshot/parts`) and fails as
  transient. An attempt that fails before its commit is decided discards them too, once, best
  effort. Nothing of a staging survives the attempt.
- **Answers.** Only a `204` naming `W` and the digest, or a `409` naming them as the latest,
  confirms. A commit whose answer is lost or retryable (a timeout, `502`, `503`) is repeated
  unchanged for up to 30 minutes, and its answer head may take its parts' copy longer (see
  Transport). A `409` with a later offset means superseded. One at `W` with another digest stops
  snapshots (`stream_state`).
- **Not published.** A body above `Ursula-Max-Snapshot-Bytes`, or above `L` on a server without a
  cold store (parts answered `501`), is not published, and status reports `snapshot.error`.
- **Failures** wait before the next attempt from 100 ms, doubling, up to five minutes, or an hour
  when the body is too large. An attempt a `401` failed starts again at once when the bearer token
  changes ([Transport](#transport)). Requests carry the binding's current token, so snapshots go on
  after a re-key.
- **Builds** read a snapshot in byte ranges sized from HEAD's limit, and a chunk that does not
  continue the download restarts the build.

Tests:

- Boundaries and the ACK: `driver/tests.rs`
  `a_boundary_seals_a_batch_ending_at_k_and_hands_back_its_receipt_end`,
  `a_410_with_our_lineages_snapshot_past_the_durable_ack_rebuilds`.
- Publishing: `driver/tests.rs`
  `publishes_keep_retention_one_snapshot_behind_and_refuse_bodies_above_the_limit`,
  `a_body_above_the_request_limit_publishes_in_parts`,
  `a_part_whose_answer_is_lost_is_staged_again_under_a_new_id`,
  `a_commit_is_repeated_until_a_definite_answer`,
  `a_commit_answered_400_stages_its_parts_again_once`,
  `a_part_past_the_staged_limit_discards_the_parts_and_a_later_attempt_publishes`,
  `a_publish_behind_a_later_snapshot_is_superseded`,
  `snapshots_publish_under_the_new_token_and_a_stale_token_is_transient`, and `test/core.e2e.ts`
  "publishes snapshots, keeps retention one behind, and a fresh directory builds from the latest",
  "publishes a snapshot body above the request limit in parts with a cold store, and reports it in
  snapshot.error without one".
- Backoff: `snapshotter.rs` `failures_double_the_wait_up_to_their_kinds_limit`,
  `a_failure_holds_the_next_attempt_back_until_its_wait_ends_or_the_database_shrinks`,
  `a_token_change_brings_an_attempt_a_401_failed_forward`.
- Ranged builds: `driver/tests.rs` `a_build_reads_the_snapshot_in_chunks_of_the_request_limit`,
  `a_chunk_that_does_not_continue_the_download_restarts_the_build`,
  `a_whole_body_answer_to_a_ranged_read_installs_it`,
  `a_416_on_every_chunk_ends_the_build_after_its_rechecks`.
- The body: `codec/snapshot/tests.rs` `every_header_bit_flip_is_refused`,
  `a_streamed_body_checks_its_digest_header_and_offset`, and `codec/vectors/mod.rs`
  `the_snapshot_header_is_frozen`.

### Transport

- One attempt of a request is a blocking HTTP/1.1 request through ureq with rustls
  (`transport/http.rs`). Appends, claims, reads and HEADs share one agent that keeps a connection
  90 s after its last request (`URSULA_VFS_IDLE_MS`), snapshot transfers use another. Both keep up
  to 256 idle connections per host and 1,024 in all.
- Per-attempt limits scale with size: connect 5 s, request head 5 s, request body
  `5 s + n / 256 KiB/s` (`URSULA_VFS_MIN_UPLOAD_KBPS`), response head `15 s + n / 8 MiB/s`,
  response body `10 s + m / 256 KiB/s`. For a parts commit `n` counts each part at the 32 MiB
  request limit, at most 1 GiB in all (`ursula_headers::answer_allowance`). A gateway in front of
  the node adds the same allowance to its own lower base, so the VFS waits longer than the
  gateway. A timeout is another lost attempt.
- A failure is "not sent" only when the resolver or the connector failed, decided by where it
  failed. Every other failure is lost, and its retry goes out on a fresh connection.
- Every request with a body carries `Content-Length`, so a node can answer `413` or an early answer
  before reading it. Over HTTP/1.1 the body is still sent, and the server drains it.
- No redirect is followed, so the stream URL names a gateway or a single node.
- The DNS cache serves a host's addresses for 30 s after a lookup, and after a failed lookup keeps
  the last addresses for up to 10 minutes.
- One bearer token per process: the last open's `token` (a string, or the latest a provider handed
  out, see [Tokens](#tokens)), `ursula_set_token`, or the file `URSULA_VFS_TOKEN_FILE` names,
  re-read when it changes and after a `401`. A token change sends every request a `401` blocked
  again at once, with no wait for the 30 s retry: the replicators', the opens' and the rebuilds'
  alike, through the retry policy they share (`Core::token_changed`), and a snapshot attempt a
  `401` failed. Other waits keep their timers and jitter. A `401` to an attempt sent before the
  change sends the request again under the new token instead of blocking it. A token set changes
  at once. A token in the file changes when a read finds it: any request's, or the driver thread's
  check of the file every second while requests carry the file's token (no token set) and an open
  or a writer may send requests. A private CA bundle goes in `URSULA_VFS_CA_FILE`. A token sent
  over `http://` to a host that is not a loopback address logs `token_in_clear` once.

Tests: `transport/http/tests.rs` `a_redirect_is_never_followed`,
`every_body_carries_content_length`, `time_limits_scale_with_the_body_and_the_answer`,
`only_a_proven_unsent_failure_is_not_sent`, `a_refused_connection_sent_nothing`,
`a_connection_closed_after_the_body_is_lost`, `model_answers_round_trip_through_http`. DNS:
`net.rs` `requests_within_30_s_of_a_lookup_make_none`,
`a_failed_lookup_uses_the_last_addresses_until_the_retry`,
`a_failed_lookup_past_the_stale_limit_fails_as_never_sent`. Tokens: `auth.rs`
`tokens_are_trimmed_printable_ascii`, `a_token_file_without_a_token_keeps_the_last_one`,
`the_read_that_finds_a_new_token_signals_the_change`,
`a_lent_token_goes_back_unless_another_was_set_since`, `replicator/tests/machine.rs`
`a_token_change_resends_a_request_blocked_unauthorized_at_once`,
`a_token_changed_while_the_refused_attempt_was_out_goes_at_once`,
`a_token_change_leaves_a_resend_in_backoff_to_its_timer`, `driver/tests.rs`
`a_token_change_resends_what_a_401_blocked_at_once`,
`a_token_changed_while_an_opens_reread_is_out_goes_at_once`,
`a_token_change_resends_a_rebuilds_read_a_401_blocked`, the replicator proptest's token changes,
and `test/token-change.e2e.ts`.

## Local state

### Layout

The extension owns its files. `open(url, { dir })` and `ursula_open(url, dir, ...)` derive the
directory and return the db path.

```text
<dir>/                                    default <user cache>/sqlite-ursula/0.8
  <label>-<hash32>/                       BLAKE3 of the canonical stream path, first 32 hex digits
    lock                                  flock, held with the binding
    gen-<id32>/                           the current generation
      db                                  the SQLite file, page 1 marked
      db-uwal                             the WAL, under a name SQLite never probes
      db-shm                              the wal-index
      log/<20-digit first position>.usl   log segments
    gen-<id32>.tmp/                       a generation being built
    gen-<id32>.trash/                     a superseded generation being deleted
    reader-<n>/                           a reader process's lock and generations
    salvage/<UTC time>-<reason>/          moved-aside generations, with manifest.json
```

- A generation directory is named by a random id, not by its lineage. The lineage is in the log's
  segment header, so a copy of a generation keeps its lineage in a directory of its own.
- A generation is built in `gen-<id32>.tmp`, its db initialized, marked and fsynced there, and
  published by one rename after the old one's fate is written. No published db file is ever empty
  or unmarked. Every segment names the directory its generation supersedes, and a roll carries
  that forward, so a crash that leaves two generations is finished at the next open.
- The current generation is the one no other generation's first segment supersedes. One pure
  function chooses it and changes nothing: it reads only `gen-<id32>` directories, and their first
  segment headers only when there are two or more. Two that nothing supersedes are ambiguous, and
  generations that all supersede one another (only damage or a copy by hand makes them) are
  circular. Either is an error, told apart from a root with no generation. The open finishes the
  superseded ones by it, deleting or salvaging them, and then reads the current generation by it.
  Every circular generation is superseded, so the open finishes them all and then sees none.
- A build supersedes the root's one generation, and refuses a root that holds two or more: it
  runs after the open finished them, and building over a superseded one would leave it named by
  nobody.
- A superseded generation is renamed to `.trash` before it is deleted.
- The `lock` of a root or a reader directory is unlocked when the last binding or open of it in
  the process is gone, before the file is closed. A flock belongs to the open file, which a child
  the host spawns shares until its exec, and a child forked without one keeps. Closing alone would
  leave the lock held while such a child lives, and a reopen right after a close would be
  `in_use`. Only the process that took the lock unlocks it, so a forked child that drops its copy
  of a root never unlocks its parent's.

Tests: `generation/tests.rs` `the_root_name_is_a_label_and_the_path_hash`,
`a_second_lock_on_the_root_is_in_use`,
`a_dropped_root_is_unlocked_while_another_descriptor_of_its_lock_lives`,
`a_built_generation_becomes_current_once_published`,
`a_crash_after_each_publish_step_converges_to_one_generation`,
`a_rolled_and_trimmed_build_converges_after_a_crash_at_each_publish_step`,
`two_directories_of_one_lineage_resolve_by_supersedes`,
`a_build_refuses_a_root_with_two_generations`,
`generations_that_supersede_one_another_are_circular`,
`a_generation_is_published_with_page_one_written`, `ids.rs`
`generation_ids_spell_as_32_hex_digits`, `db.rs` `the_default_dir_is_versioned`, `replog/tests.rs`
`every_segment_names_what_its_generation_supersedes`.

### Log

```text
segment = header | record*                                         rolled at 16 MiB
header  = "USL1" | u32 version=1 | [16] lineage | u64 first_position | opt16 supersedes
        | str cluster_id | str stream_path | u64 incarnation
        | u64 base | u64 acked | u64 acked_offset | u64 log_bytes   carried over
        | claim                                                    the latest CLAIM, if any
        | u32 crc32c(header)
record  = u32 len | u8 kind | u64 position | payload | u32 chain
chain   = crc32c(previous chain | kind | position | payload)        seeded by the header crc
str     = u16 len | utf-8
opt16   = u8 0 | u8 1 [16]
claim   = u8 0 | u8 1 u64 epoch [16] activation
```

| kind | payload | written |
| --- | --- | --- |
| 1 COMMIT | the frame: our commit, or a stream frame caught up at open | at the commit point, or before catch-up pages are applied |
| 2 CLAIM | `u64 epoch`, `[16] activation` | before the claim is sent, fsynced |
| 3 ACK | acked position, acked offset (a commit boundary), log bytes since the latest snapshot | at most once a second, at once during an open, before a snapshot publishes, and when a binding resets or retires |
| 4 BASE | position | after F2, materialization, or a snapshot copy |

- The log is read by one rule, which changes nothing: each segment streams through a 64 KiB
  buffer, its chains are verified, and each record of the valid log goes to a fold that keeps what
  its reader needs, and then the log's end: the state there and where the log is cut. The open's
  fold keeps every record's body, and the open then cuts. Whatever the size of a record or a
  segment, a read takes the list of segments, the buffer, what the fold keeps, a mark per frame of
  the longest run of caught-up stream frames, and the headers of the segments that run reaches. The commit id of a COMMIT record's frame is read as the frame streams by, which tells
  caught-up stream frames from the generation's own commits.
- Positions start at 1. The valid log is the longest prefix whose chain verifies and whose
  positions grow by one. Open cuts the log at its first broken record, whether or not a
  chain-valid record follows, and reports the cut. A power loss can keep later unsynced records and
  lose an earlier one, and a user can store bytes that verify as a later record, so neither may
  refuse the open. The cut keeps L3, since pages reach the db only after F1 fsyncs the log.
- A gap, a position that overflows, or a malformed record under valid chains fails as `internal`,
  in the first segment as in any other. Caught-up stream frames with no ACK after them are cut,
  since a catch-up applies its frames only once that ACK is fsynced.
- A binding writes an ACK when it resets or retires. A clean retire and a `rewound` reset fsync
  it, so a cleanly closed log holds no commit after its latest ACK but ones never acknowledged.
- A segment goes once all its records are at or below `min(base, acked)` (F3). Disk use is the
  pending records, the records since the base and one segment.
- A re-key ([History](#history)) rolls the log to a segment that names the new token, fsynced with
  the CLAIM record that follows.

Tests: `codec/segment/tests.rs` `segments_round_trip`, `a_chain_break_keeps_the_prefix_before_it`,
`a_position_gap_under_valid_chains_is_a_gap`, `every_header_bit_flip_is_refused`,
`the_reader_reads_what_a_whole_segment_scan_reads`,
`a_record_larger_than_the_buffer_streams_through_it`, `codec/frame/tests.rs`
`the_commit_id_reader_reads_what_decode_reads`, `replog/tests.rs`
`the_read_opens_logs_as_the_whole_segment_open_did`,
`a_hosts_fold_keeps_the_pending_commits_without_their_frames`,
`a_later_segment_that_does_not_follow_fails_after_its_records`, `a_torn_tail_is_cut`,
`crash_cuts_at_every_byte_keep_the_whole_records`,
`a_record_lost_before_a_kept_one_is_cut_there`,
`a_planted_look_alike_record_does_not_refuse_the_open`, `a_gap_under_valid_chains_is_an_error`,
`caught_up_frames_no_ack_follows_are_cut`, `a_torn_roll_deletes_the_new_segment`,
`a_rekey_rolls_to_a_segment_naming_the_new_token_with_its_claim`, `driver/tests.rs`
`acks_are_written_at_most_once_a_second`,
`a_clean_close_writes_an_exact_ack_so_an_inheriting_history_re_keys`, `test/crash.e2e.ts` step
`cut_removed`. Frozen bytes: `codec/vectors/mod.rs` `the_log_segment_is_frozen`,
`a_fresh_builds_segment_header_is_frozen`.

### Marker

On disk, bytes 0 to 16 of the db file hold `Ursula08` and the first 8 bytes of the lineage. On the
main db file only, the VFS stores the marker where SQLite writes its magic (`xWrite`), returns
SQLite's magic for a read that finds the marker of this lineage (`xRead`), and resets
`local_damage` on anything else: the file was replaced or written outside. `xFetch` maps nothing
over page 1, so SQLite reads it through `xRead`. Frames and snapshots carry plain pages, and the
Applier stamps the marker.

Tests: `codec/marker/tests.rs` `the_marker_names_the_lineage`,
`magic_written_reads_back_as_magic`, `another_lineage_is_not_marked`, `vfs/tests.rs`
`page_one_lands_on_disk_marked_and_reads_back_as_magic`, `a_foreign_marker_resets_local_damage`,
`fetch_maps_nothing_over_page_one`, `applier.rs` `page_one_written_outside_the_applier_is_stamped`,
`codec/vectors/mod.rs` `the_page_one_marker_is_frozen`.

### Invariants

- **L1, log:** the log means its longest valid prefix.
- **L2, record first:** every commit SQLite could recover has a COMMIT record, appended before its
  commit frame's page data.
- **L3, base:** with `B` the latest BASE, the db file holds every commit up to `B` durably, and
  otherwise only pages from records after `B`.
- **L4, order:** the log is fsynced before a checkpoint copies a page (F1) and the db before a
  BASE (F2), so any crash-consistent image satisfies L3 with its durable log prefix.
- **L5, acknowledged prefix:** acknowledged records are a log prefix, and past the acknowledged
  offset the stream holds this lineage's frames only in log order, among its claims, until another
  owner's frame.

Re-applying the records after `B` gives exactly the state at the end of the valid log. The WAL
plays no part.

Tests: L1 to L4 by `materialize/tests.rs`
`materialization_yields_the_state_at_the_last_valid_record` (a proptest over random commits,
checkpoints and crashes) and the `test/crash.e2e.ts` matrix. L5 by the world's property 2.

### Open

`open()` checks in-process state, then the first disk job takes the root's `lock` (`in_use` when
another process holds it), records the cache directory in the host directory when the open is an
owner's and there is one ([Hosts](#hosts)), finishes leftovers (`.tmp`, `.trash`, a superseded
generation, a salvage marked `moving`), checks page 1 and the segment header, and materializes the
current generation.
Then a HEAD without a precondition decides, before any claim (`open/table.rs`):

| local generation | HEAD | the open |
| --- | --- | --- |
| any | no `Ursula-Protocol` (a gateway's concealing `404` included) | refuse `no_server_identity` |
| any | `Ursula-Protocol` of another release line | refuse `protocol_mismatch` |
| trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
| none, replaced | `404` | create, take over |
| trusted, holding no data | `404` | create, take over |
| trusted, holding data | `404` | refuse `not_found`, or with `recreate` salvage (`recreated`), create, take over |
| any | another content type (a 0.7 stream) | refuse `stream_format` |
| any | closed | refuse `stream_state` |
| none | `200` | build, take over, reconcile |
| replaced | `200` | build, the generation salvaged (`replaced`) |
| trusted, same token, holding no data, a snapshot past its ACK | `200` | build, the generation trashed |
| trusted, same token | `200` | resume: take over, reconcile, catch up or rebuild |
| trusted, other token, inheriting, nothing pending | `200` | re-key, take over ([History](#history)) |
| trusted, other token, otherwise, holding data | `200` | build, the generation salvaged (`history_changed`) |
| trusted, other token, otherwise, holding no data | `200` | build, the generation trashed |

- **Trusted:** the log validates up to its first broken record, the segment header reads, and page
  1 carries the lineage. A generation whose db file is not its own or whose first segment header is
  unreadable is **replaced**. No boot id, inode, size or WAL shape enters, and a power loss does
  not end trust.
- **Holding data:** the db file is longer than one page, or the valid log holds a COMMIT record.
- **Behind a snapshot:** a generation holding no data whose latest ACK is below the stream's
  latest snapshot is built over, not resumed. It has nothing to keep, and the stream below that
  snapshot may be an import's claim ([Import](#import)), which no read takes, while a build
  starts at the snapshot.
- **Nothing moved on a refusal:** no generation is renamed or removed and nothing is sent. Deleting
  the WAL and `-shm` during materialization is allowed, and so is the copy a reopen beside reset
  handles publishes ([Materialize](#materialize)), which is equivalent to the original.
- **Unreachable:** the first request got no answer with nothing sent: the name lookup or the
  connect failed, or took longer than its 5 s limit. A stream that answers, even with a 5xx, is
  reachable, and `open()` waits for the takeover, bounded by its signal.
- **Provisional:** a trusted generation whose log holds a CLAIM record opens at once on an
  unreachable stream. Reads come from local files, commits are pending, `synced()` waits, and the
  claim waits for the stream behind the [Pre-check](#pre-check). Without a generation or a claim,
  the open retries until its signal fires.
- All db writes at open precede binding. After binding, anything that would rewrite pages under
  live connections is a reset, never an in-place apply.
- A stream binds under one `dir` at a time in a process. An open under another `dir` waits for a
  binding nothing of the host holds to retire, and is refused `bound_elsewhere` otherwise.

Tests: `open/table.rs` `another_line_or_no_identity_is_refused_before_anything_else`,
`a_404_without_identity_is_refused_not_missing`,
`another_cluster_refuses_a_trusted_generation_before_anything_else`,
`a_missing_stream_is_created_unless_a_trusted_generation_holds_data`,
`another_content_type_and_a_closed_stream_are_refused`,
`no_generation_builds_and_the_same_token_resumes`,
`an_empty_generation_behind_the_snapshot_is_built_over_not_resumed`,
`a_replaced_generation_is_salvaged_and_built_over`,
`another_token_whose_history_inherits_the_acknowledged_offset_re_keys`,
`an_inheriting_history_salvages_a_generation_with_pending_commits`,
`another_token_that_does_not_inherit_salvages_or_trashes`,
`an_unreachable_stream_opens_a_trusted_generation_with_a_claim_provisionally`,
`an_unreachable_stream_waits_without_a_generation_a_claim_to_continue_or_with_a_replaced_one`.
Driver: `driver/tests.rs` `a_server_without_identity_is_refused`,
`a_server_of_another_release_line_is_refused`,
`a_head_404_without_the_protocol_is_no_missing_stream`,
`a_stream_of_another_content_type_is_refused`, `a_stream_of_usq1_frames_is_refused`,
`a_missing_stream_with_a_local_generation_holding_data_is_not_found`,
`recreate_salvages_the_generation_and_creates_the_stream`,
`a_missing_stream_over_a_generation_holding_no_data_is_created`,
`a_root_locked_elsewhere_is_in_use`, `an_unreachable_stream_fails_the_open_at_its_deadline`,
`a_provisional_open_resolves_when_its_first_connect_times_out`,
`a_stream_that_answered_first_is_reachable_and_the_open_waits`,
`a_provisional_open_waits_for_its_claim_then_goes_live`. End to end: `test/provisional.e2e.ts`
"opens a trusted cache provisionally: reads and commits work, synced() waits, then it goes live",
"goes live after two offline opens in a row, with the commits of both", `test/open.e2e.ts` "cancels
at its signal: an unavailable error, and the stream is not held", `test/handout.e2e.ts` "is refused
in another cache while a connection of the host is open, naming the cache it is open in".
A provisional open of a cache another host took over serves its stale rows until `synced()` rejects `fenced`, which only code that waits for `synced()` is protected from: `test/provisional.e2e.ts` "S6: serves the stale rows of a cache another host took over, keeps its commits local, rejects synced() `fenced` once the stream answers, and rebuilds at the next open".

### Materialize

- **Materialize**, at every open of a trusted generation: cut the log to its valid prefix, fsync
  it, delete the WAL and `-shm` unread (and a `db-wal` or `db-journal` left by SQLite outside the
  extension), re-apply the COMMIT records after the latest BASE in batches of final images, fsync
  the db, append BASE.
- **Catch up in place**, when a takeover's reconcile found foreign frames and nothing is pending:
  log each frame as a COMMIT record plus an ACK, fsync the log, apply, fsync the db, append BASE,
  so a crash midway replays. A catch-up larger than the database rebuilds instead.
- **Rebuild**, for a fresh host, a token change that does not inherit, a discarded pending tail or
  a replaced generation: build `gen-<id32>.tmp` from the latest snapshot and the tail, fsync,
  write its first segment, publish. A discarded tail always rebuilds, since checkpoints may have
  moved its pages into the db with no earlier images kept. A history change during a fresh build
  starts the build over under the new token, at most 8 times.
- **Reopen from a copy:** a reopen after a reset need not wait for old handles. If they still hold
  the generation, the open copies its db and then its log into a `.tmp` with the same lineage,
  publishes the copy, and the old connections keep their files. A commit after the reset is
  refused under the log lock, so none lands in the old log after the copy.

Tests: `materialize/tests.rs` `materialization_yields_the_state_at_the_last_valid_record`,
`catching_up_logs_applies_and_survives_a_reopen`,
`catching_up_is_refused_while_our_commits_are_pending`, `stray_sqlite_files_are_deleted`,
`driver/tests.rs` `a_reopen_without_pending_commits_catches_up_in_place`,
`a_catch_up_larger_than_the_database_rebuilds`,
`a_history_change_during_a_fresh_build_starts_it_over`, `a_fresh_build_gives_up_after_its_restarts`,
`a_reopen_while_old_handles_live_builds_from_a_copy`, `generation/tests.rs`
`a_copy_keeps_the_lineage_the_marker_and_the_log`,
`a_crash_during_a_copy_leaves_one_generation_current`, `vfs/tests.rs`
`a_commit_after_a_reset_is_refused_under_the_log_lock`, `test/reopen.e2e.ts` "succeeds after a
fenced reset: the old read keeps its snapshot, its new statements fail, the new database commits",
and its `copy_db` and `copy_log` kills.
A stale cache reopened on its host after another host wrote, with real pages: `test/stale.e2e.ts` "S1: a cache that came back with nothing pending catches up in place" and "S1: a cache that came back with nothing pending rebuilds when the catch-up is larger than the database".

### Readers

A `readOnly` open claims nothing and never fences the owner.

- In a process that owns the stream it returns a read-only connection on the owner's binding,
  which sees the owner's latest commits, acknowledged or not.
- Otherwise it locks the first free `reader-<n>/` of the root (up to 64) and builds or catches up a
  reader generation to the stream's tail at the open. Its connections open read-only, its
  `synced()` resolves at once, and it retires with its last connection.
- A reader generation holds no commit of its own, so it is never salvaged and never re-keyed:
  anything the stream no longer extends is built anew. A closed stream opens. A trusted reader
  generation of another cluster is refused `wrong_cluster`.
- The reader rows, in order (`open/table.rs` `decide_reader`):

  | reader generation | HEAD | the open |
  | --- | --- | --- |
  | any | no `Ursula-Protocol` | refuse `no_server_identity` |
  | any | `Ursula-Protocol` of another release line | refuse `protocol_mismatch` |
  | trusted | another `Ursula-Cluster-Id` | refuse `wrong_cluster` |
  | any | `404` | refuse `not_found` |
  | any | another content type | refuse `stream_format` |
  | none, replaced | `200`, closed or not | build, the replaced one trashed |
  | trusted, same token, a snapshot past its ACK | `200`, closed or not | build, the generation trashed |
  | trusted, same token | `200`, closed or not | catch up in place |
  | trusted, other token | `200`, closed or not | build, the generation trashed |

- On an unreachable stream a reader opens a trusted reader generation as it is, and otherwise waits
  until its signal fires. A missing stream is `not_found`. `readOnly` with `recreate` is
  `invalid_argument`.
- The extension does not make a connection on the owner's binding read-only. A host of the SQL
  functions opens a reader's path with `SQLITE_OPEN_READONLY`, as the TypeScript package does.

Tests: `open/table.rs` `a_reader_refuses_an_old_server_another_cluster_and_a_missing_stream`,
`a_reader_catches_up_the_same_token_and_builds_over_anything_else`,
`a_reader_reads_a_closed_stream`,
`an_unreachable_reader_opens_a_trusted_generation_and_waits_without_one`,
`a_reader_behind_the_snapshot_builds_from_it`, `driver/tests.rs`
`a_reader_built_before_an_import_reopens_from_its_snapshot`,
`a_reader_builds_in_a_reader_directory_and_claims_nothing`,
`a_reader_reopened_catches_its_generation_up_in_place`,
`a_replaced_reader_generation_is_rebuilt_without_salvage`, `generation/tests.rs`
`readers_lock_the_first_free_reader_directory_beside_the_owner`,
`concurrent_readers_and_cleanups_never_share_or_delete_a_held_directory`, and `test/readers.e2e.ts`.

### Crashes

Every row resumes or rebuilds with no manual step.

| failure or crash | next open | relies on | `URSULA_VFS_ABORT_AT` steps |
| --- | --- | --- | --- |
| before the COMMIT record | never committed | L1 | `admission`, `capture`, `encode` |
| after the record | replayed and shipped | L2 | `logged`, `forward`, `phasetwo` |
| an unconfirmed commit | reset, replayed | [Publication](#publication) | |
| during a checkpoint | re-applying from the BASE overwrites pages written since | L3, F1 | `checkpoint_start`, `checkpoint_synced`, `checkpoint_page` |
| around the F2 fsync and its BASE | replays from the older or newer BASE | L3 | `base_start`, `base_synced`, `based` |
| between the parts of a run | the next claim voids the run, the commit goes out whole | [Frames](#frames) | `part_sent` |
| during a re-key | the latest segment names the old token or the new one | [History](#history) | `rekey_rolled`, `rekey_claimed` |
| during a salvaging publish | the publish or the salvage finished from the manifest | [Layout](#layout) | `salvage_marked`, `publish_renamed`, `salvage_moved` |
| during a copy | one generation current | [Materialize](#materialize) | `copy_db`, `copy_log` |
| during a trim or a cut | the log reads the same | F3, L1 | `trim_unlinked`, `cut_removed` |
| while an owner open records its cache directory | the record is complete or absent, at most a temp file beside it, and the open claimed, built and logged nothing: the root is as the open found it (a fresh one holds only its `lock`) | [Hosts](#hosts) | `host_tmp_synced`, `host_linked`, `host_dir_synced` |
| during a claim | its CLAIM record marks it ours, the next claim goes higher | [Claims](#claims) | |
| power loss | the durable log prefix with L3: unacknowledged commits after the last log fsync lost, acknowledged ones return from the stream | L4 | |
| a failed fsync, a WAL page that reads back other bytes | reset `local_damage`, materialization rewrites pages since the BASE | L3 | |

Tests: `test/crash.e2e.ts` (both matrices, each asserting no manual step, one generation, no lost
commit and equality with a fresh build), `test/reopen.e2e.ts` "leaves one generation when the
process dies during the copy", `test/parts.e2e.ts` "a crash between parts leaves no part of the
transaction applied, and the reopen sends it whole", `test/core.e2e.ts` "keeps every commit a
SIGKILL interrupts: the reopen sends what never reached the stream", `generation/tests.rs`
`a_crash_after_each_salvaging_publish_step_converges`, `replog/tests.rs`
`a_crash_after_the_roll_before_the_claim_leaves_the_new_token`,
`a_crash_before_the_rename_keeps_the_old_header`, `test/host.e2e.ts` "crashes while an owner open
records its cache directory". Power loss: the world's restarts after a power loss
(`replicator/tests/world.rs`
`a_provisional_restart_after_a_power_loss_of_acknowledged_commits_goes_stale`) and
`driver/tests.rs` `a_provisional_open_whose_log_lost_acknowledged_commits_goes_stale`.

## Trust

### Outsiders

1. **The page-1 marker** ([Marker](#marker)): SQLite refuses a file whose first 16 bytes are not
   its magic with `SQLITE_NOTADB` before reading anything else.
2. **The WAL name:** SQLite opens, probes and deletes the WAL through the VFS, and the `ursula` VFS
   maps `db-wal` to `db-uwal` in `xOpen`, `xAccess` and `xDelete`. An outside SQLite finds no WAL,
   stays in rollback mode and fails on page 1.

| outsider | result |
| --- | --- |
| a connection opened before the extension loaded, or `?vfs=unix` | `SQLITE_NOTADB`, no file changed |
| a process without the extension (the `sqlite3` shell, a backup tool) | `SQLITE_NOTADB`, no file changed |
| byte-level writes that keep the lineage (`dd`, an older copy in place) | unseen, unsupported |

Inspection goes through a bound connection, or `VACUUM INTO` a plain file.

Tests: `test/outsiders.e2e.ts` "finds no database in a cache to read or write, and changes none of
its files", "finds no database in a cache that holds no commit yet, which the owner then writes",
"refuses a connection opened before the extension loaded, reads and writes, while the bound one
works", and `vfs/tests.rs` `the_wal_of_a_bound_db_is_db_uwal`,
`access_and_delete_map_the_wal_of_a_registered_db_only`.

### History

`Stream-Incarnation` changes whenever bytes at a served offset may differ: a delete and recreate,
a restore, an accepted loss. Every write and read the VFS bases a decision on carries it. HEAD
carries none, so it reports the current token and, after a history event,
`Stream-Previous-Incarnation` and `Stream-Inherited-Offset`.

- **Inherits.** A generation inherits a history when the previous token is its own and its
  acknowledged offset is at or below the inherited offset. Two events with no write between them
  report the token from before both.
- **Re-key.** An inheriting generation re-keys: the log rolls to a segment naming the new token,
  fsynced with the CLAIM record of the claim that follows. At open the claim is a takeover. While
  bound it is a continuation at own + 1 with no reset, and `synced()` waits meanwhile. An
  unconfirmed continuation claim is sent again under the new token, same epoch and body, and a
  repeat answer is followed by one more claim at the next epoch. A continuation whose latest claim
  applied past its acknowledged offset pre-checks first ([Pre-check](#pre-check)).
- **Re-key at open only with nothing at risk.** A takeover re-keys only when no commit after the
  latest ACK was logged before this open, since only those may have been sent and acknowledged
  behind a lagging ACK. Otherwise it salvages. Commits logged while provisional were never sent,
  so a history that loses nothing re-keys them and they replicate.
- **Otherwise, at open**, a generation holding data is renamed into
  `salvage/<time>-history_changed/` and the open builds a new generation and continues as owner.
  A rebuild that discards pending records after a re-key at open salvages too.
- **Otherwise, while bound**, a `412` confirmed by HEAD resets `rewound` (`reopen`). The retiring
  binding fsyncs an exact ACK, so the reopen's test fails the same way and salvages.
- **Salvage.** One rename of the whole generation directory, so it needs no free space. The db
  gets SQLite's magic back (a replaced one stays as found) and holds every commit of its valid log.
  `manifest.json` names the path, the cluster id, both tokens, the acknowledged position and
  offset, and the unacknowledged count. The extension never reads or deletes a salvage, and
  `status().salvage` lists it. Unacknowledged tails a takeover or a `stale` reset discards within
  one history are not salvaged, since no gated output showed them.
- **No history change discards a local commit without a salvage.**

Tests:

- Inheritance and re-keys: `replicator/tests/rekey.rs`
  `inherits_needs_our_token_and_every_acknowledged_byte`,
  `a_rekey_from_streaming_acknowledges_a_batch_that_survived`,
  `a_rekey_from_streaming_resends_a_batch_that_was_lost_from_seq_1`,
  `a_rekey_during_a_continuation_claim_sends_the_same_claim_again`,
  `a_repeat_after_a_rekey_claims_once_more`,
  `a_takeover_with_commits_pending_resets_before_it_re_keys`,
  `a_rebuild_that_discards_after_a_rekey_at_open_salvages`, the world's properties 15, 18 and 19
  with `negative_control_re_keying_on_any_token_drops_acknowledged_records`,
  `negative_control_re_keying_with_commits_pending_discards_them_with_no_salvage` and
  `negative_control_re_keying_with_commits_pending_loses_a_commit_after_a_clone`.
- The driver: `driver/tests.rs` `an_inheriting_history_re_keys_without_a_reset`,
  `a_lossy_restore_resets_rewound_and_the_reopen_salvages`,
  `a_reopen_after_an_inheriting_history_salvages_while_commits_are_pending`,
  `an_acknowledged_commit_behind_a_lagging_ack_is_salvaged`,
  `an_offline_commit_survives_a_history_that_keeps_everything`,
  `a_provisional_claim_meeting_an_inheriting_history_re_keys`,
  `a_delete_then_recreate_or_import_under_a_bound_owner_salvages_every_commit`.
- Salvage: `salvage/tests.rs` `a_salvage_moves_the_whole_generation_and_restores_the_magic`,
  `a_replaced_db_is_left_as_it_was_found`, `the_manifest_renders_as_its_golden_json`, and
  `codec/vectors/mod.rs` `the_salvage_manifest_is_frozen`.
- End to end, model: `test/history.e2e.ts`, `test/salvage.e2e.ts`. Real server:
  `test/history-real.e2e.ts` "re-keys a bound owner across a restore that keeps everything: no
  reset, the new token, the next epoch", "resets `rewound` when a restore lost an acknowledged
  commit, and the reopen salvages it", "reopens a closed owner's cache as `rekeyed` after two
  restores with no write between".

### Identity

The database is its canonical stream path: segments percent-decoded and re-encoded one way, dot
segments resolved, no trailing `/`, a query or fragment refused. Scheme, host, port and userinfo
are transport, so every host alias of a deployment shares one root. The deployment is checked
instead of hashed: the segment header keeps the `Ursula-Cluster-Id` HEAD reported when the
generation was built.

- An open whose HEAD names another cluster id refuses `wrong_cluster` and moves nothing.
- A bound database whose HEAD names another id, while confirming a `412` or `404`, blocks
  `wrong_cluster` and retries every 30 s, so a routing fix resumes it with no reopen. A write
  misrouted to another deployment fails its incarnation precondition and never applies there.
- A misrouted host alias is refused, never salvaged and adopted. A fresh host has nothing to
  compare, so a misrouted alias there opens the other deployment's stream ([Limits](#limits)).
- `Ursula-Protocol` carries the server's release line, major × 1000 + minor of its version
  (`11` for 0.11.x), which `ursula-headers` derives from its package version at compile time, so
  the server and the extension built from one tree speak one line. Every HEAD goes through one
  check (`open/table.rs` `check_identity`): no identity refuses `no_server_identity`, another line
  `protocol_mismatch`, then another cluster `wrong_cluster`. The open table, a build's HEADs, the
  import, the snapshotter and the replicator's HEAD while bound share it. A bound database that
  meets another line or no identity while confirming a `412` or `404` blocks with that condition
  and retries every 30 s, as for another cluster. A bound database sends a `HEAD` only to confirm
  such an answer, and its writes carry no identity, so a cluster moved to another line under it
  is noticed at the next such `HEAD`. Every SQLite session therefore stops before the move. A gateway's concealing
  `404` carries no identity, so it is never read as a deleted stream.

Tests: `generation/tests.rs` `every_spelling_of_a_path_is_one_canonical_path`,
`urls_that_name_no_stream_are_refused`, `open/table.rs`
`another_cluster_refuses_a_trusted_generation_before_anything_else`,
`another_line_or_no_identity_is_refused_before_anything_else`, `replicator/tests/rekey.rs`
`decide_head_rows`, `replicator/tests/machine.rs`
`head_blocks_another_release_line_and_an_answer_without_identity`, `driver/tests.rs`
`a_server_of_another_release_line_is_refused`, `a_misroute_blocks_and_then_resumes`, `a_build_whose_route_flips_to_another_deployment_is_refused`,
the world's properties 16 and 17 with
`negative_control_a_misroute_taken_for_a_deleted_stream_resets` and
`negative_control_a_misroute_taken_for_a_history_writes_to_the_other_deployment`, and
`test/cluster.e2e.ts` both tests.

### Foreign writers

The server refuses appends of another content type, which stops accidental writers. Receipt
contiguity detects any append between two of ours at the next acknowledgement, and the reset
(`stream_state`) stops the owner before it builds on a stream it does not match. Undecodable bytes
fail every replay closed. Detection rather than prevention is accepted because a writer with
credentials and the VFS content type could equally claim and take over, which the VFS sees as a
fence. Prevention belongs to authorization.

Tests: `driver/tests.rs` `a_foreign_append_breaks_receipt_contiguity_and_resets_stream_state`,
`model/tests.rs` `a_foreign_append_breaks_receipt_contiguity`, `replicator/tests/machine.rs`
`a_probe_that_finds_a_frame_fences_and_undecodable_bytes_reset_stream_state`.

### Clones

A hosting platform may restore a process's memory snapshot once, several times, or on another
host. Copies share the epoch, next seq, in-flight batch, lineage and activation.

- Memory is a cache over durable state: the epoch comes from a claim, acknowledgements from the
  stream, local state from the log on disk.
- One append is in flight and its bytes and digest never change, so identical retries from two
  copies are deduplicated. At the first commit where they differ, the second copy's append carries
  another digest under the same key and gets `403 key-conflict`, and that copy resets `fenced`
  before anything built on its commit is acknowledged. At most one of two divergent commits is
  ever acknowledged, and the loser's gate never opened.
- Host contract: each restored copy gets its own copy-on-write disk, since `flock` in a guest does
  not exclude another VM on a shared directory.

A copy of a cache directory carries the log, so the original's CLAIM records and commits are
its own to it. Opened while the stream answers, it takes the stream over like any open, and the
original resets `fenced` at its next commit or `synced()`. Opened offline, its pre-check decides:
commits the original made since the copy reset it `stale` with nothing sent, and an idle
original's claim is continued at own + 1, which fences the original. Opened while the stream
answers, a copy behind retention does not take the original's snapshot as covering its own
commits ([Reconcile](#reconcile)), and rebuilds from it into a new generation. Offline, its
pre-check's `410` resets it `stale`.

Tests:

- The world's properties 3, 8, 19 and 22 (`replicator/tests/world.rs`
  `the_replicator_keeps_its_properties_against_the_model`) with
  `negative_control_the_model_without_digests_breaks_clone_safety`,
  `negative_control_a_probe_skipping_claims_of_its_activation_syncs_past_a_copys_claim`,
  `negative_control_matching_commits_by_id_acknowledges_a_copys_commit`, the
  `trust_clone_snapshot` control in `a_clones_snapshot_never_covers_the_originals_commit`, and
  `a_provisional_clone_never_fences_the_owner_that_bound`,
  `a_shared_epoch_takeover_fences_exactly_one_copy`,
  `a_copy_taken_over_while_the_original_writes_fences_the_original`,
  `an_offline_copy_of_an_idle_cache_fences_the_original`,
  `an_offline_copy_and_the_original_both_reset`.
- The machine: `replicator/tests/machine.rs`
  `a_takeover_won_by_a_repeat_binds_and_its_first_refused_append_fences`.
- The driver, over copied directories: `driver/tests.rs`
  `a_copy_of_a_live_cache_takes_over_and_the_original_is_fenced_losing_nothing`,
  `a_copy_carrying_a_commit_the_original_acknowledged_since_matches_it_and_resends_nothing`,
  `two_copies_on_one_activation_share_an_epoch_until_a_divergent_commit_fences_one`,
  `a_stale_copy_below_retention_rebuilds_and_never_trusts_the_originals_snapshot`,
  `an_offline_copy_behind_the_original_goes_stale_with_nothing_sent`,
  `a_delete_then_recreate_or_import_under_a_bound_owner_salvages_every_commit`.
- End to end, `test/clone.e2e.ts` "takes
over from the original when opened by another process, and the original is fenced at its next
commit", "shares one epoch between two copies opened with one activation, and their first
divergent commit fences exactly one", "resets `stale` with nothing sent when its original kept
writing", "goes live over an idle original and fences it", "goes live over an idle original that
never wrote and fences it", and "rebuilds a copy that fell behind retention, never trusting the
original's snapshot". An old disk snapshot whose catch-up and next commit take the commit id of the original's unsent tail: `test/stale.e2e.ts` "S5: an old disk snapshot catches up and commits under the id of the original's unsent tail, and the original rebuilds without sending it".

### Hosts

A host that snapshots or moves the process, or the VM it runs in, needs to know whether the
snapshot holds a commit the stream lacks. The extension tells it where to look.

The host creates the host directory, `/var/lib/sqlite-ursula/host`, before any process that may
load the extension starts. Every such process must be able to create files in it, for example a
root-owned directory of mode 1777. Every ancestor of it must be a directory the process can search:
a missing, unsearchable or non-directory ancestor makes the host directory absent to that process,
which then records nothing. Links among the ancestors are followed, only the host directory itself
must not be a link.

```text
/var/lib/sqlite-ursula/host/
  dir-<key>               a record of a cache directory, mode 0444, never changed by the extension
  .tmp-<pid>-<16 hex>     a record being published, ignored by readers
  hold, host<anything>    reserved for the host: the extension never writes these names
```

An owner open that locks a stream's root records the root's cache directory there before it claims
anything, as `dir-<key>`: one JSON line with the directory's real path, such as
`{"v":1,"dir":"/home/app/.cache/sqlite-ursula/0.8"}`, where `<key>` is the first 32 hex digits of
the BLAKE3 of that path. Every owner open that locks a root records, a fresh lock as much as one
beside a reset binding or after an abandoned build, so every root an owner open locks while the
host directory exists lives in a recorded directory, wherever the application put it. A record is
written once (a temp file, fsynced, linked into place, then the directory fsynced) and never
changed. `link` never replaces a file, so a record is complete or absent. An open that finds the
record, or loses the race to link it, checks that it names the same directory and fsyncs the
directory too, so the record is durable before the open logs anything.

When the host directory exists and the open cannot record its directory, it fails with
`local_setup`, action `fix`, before it claims anything, and nothing reaches the stream: a symlink or
a file that is not a directory in place of the host directory, something under the record's name
that is not a record of this directory, a real path that is not UTF-8, or a missing record that
cannot be written because the filesystem is read-only or full or the process cannot write the
directory. An open that finds its record only reads it and fsyncs the directory, so a read-only,
full or unwritable host directory refuses no open whose directory is already recorded. After the
directory's open (`O_DIRECTORY` and `O_NOFOLLOW`), every step goes through its descriptor (`openat`,
`linkat`, `unlinkat`), so a path swapped meanwhile cannot redirect one. The open claimed, built and
logged nothing, so the root is as the open found it (a fresh one holds only its `lock` file), and
the next open tries again. A reader or an import records nothing, since neither logs a commit of its
own, and loads, commits, `synced()` and status never consult the host directory.

- The host directory is the compiled-in path when `lstat` finds anything there, whatever its type.
  A path the process cannot `lstat`, such as one a confinement policy hides, counts as absent
  (logged once, `host_dir_unreadable`). `URSULA_VFS_HOST_DIR` is a test hook, read only when that
  path is absent, and not a setting: an application can scrub its environment, never the
  compiled-in path.
- A host may delete the record of a directory that no longer exists, while nothing that may load
  the extension runs: an open that locks a root in a directory at that path again records it
  before it can log.
- A `.tmp-<pid>-*` carries the writer's PID as its own PID namespace numbers it. A host may delete
  one only when it shares that namespace and no process with that PID runs. A PID from another
  namespace says nothing about the processes the host sees.
- A mode-1777 directory is no boundary between the processes that share it. Any of them can link a
  record under a name another's directory hashes to, failing that one's opens, or record a
  directory that is not its own. Processes that must not affect each other need host directories
  of their own, which means separate mount namespaces, since the path is compiled in.
- A record is a flat JSON object: every value is a string, a number, a boolean or null, never an
  object or an array, so a later line may add only such keys. A reader takes its `dir` whatever
  its `v` and ignores keys it does not know. The format is frozen within a release line.

Moving or deleting a cache directory, or a directory above it, while a root in it holds
unacknowledged commits and no process has it open hides them from the host, as from the extension,
until the root is opened where it now is.

`ursula-vfs-host inspect` (`inspection::inspect`) is the host's read of the disk: a binary of the
extension's crate that never loads SQLite, never writes a file and never takes a lock. A host runs
it as root, so that it can read every root and every process's descriptors. One pass reads:

1. The host directory: the one `--dir` names, or the compiled-in path. The inspector never reads
   `URSULA_VFS_HOST_DIR`, so a test hook left in the host's environment cannot point it at another
   directory than the extension's. It must be a directory and not a link. When it is the
   compiled-in directory (the same device and inode, however `--dir` spells it), it must be owned
   by root with mode 1777, in a directory owned by root with mode 0755, with every ancestor of the
   compiled-in path a directory and none a link, so an ancestor that disabled recording shows
   here. The listing and the records come from one directory: the one the inspector opened, which
   the path must still name once it is listed. Every name in it must be a record, a
   `.tmp-<pid>-<16 hex>`, `hold` or a name starting with `host`, and every record must have `v` 1
   and an absolute `dir` whose key is its name. The open's own check takes any `v`, so that
   release lines can share a directory. The inspection refuses what it does not know.
2. The directories: every recorded one and every `--scan` one, once per real path. A path with
   nothing at it, or with something other than a directory, holds no roots. In a directory, every
   subdirectory named `<label>-<32 hex>` is a root, and other entries belong to the application.
   An entry with a root's name that cannot be looked at (a `stat` that fails other than for a
   missing file or a dangling or looping link) makes the directory unreadable. A root with no
   `gen-<id32>` directory holds no commit and is not listed.
3. Each root: its current generation by the open's rule (`Root::classify`), and that generation's
   log by the open's reading rule (`Log::read`), with a fold that keeps a position and a frame
   length per pending commit. A pass takes about 64 KiB plus 16 bytes per pending commit, whatever
   the size of a record or a segment. A root's unacknowledged bytes are the frame lengths of its
   generation's own COMMIT records above the latest ACK: the commits the next open resends, and
   its `commits.pending_bytes` once it binds. The ACK is lazy, so the bytes can include commits
   acknowledged within the last second, and never miss one. A log the open would refuse, or
   generations of which none is current, make the root `unreadable`.
4. The lock check: every open descriptor of a live process under `/proc/<pid>/fd`, or under
   `/proc/<pid>/task/<tid>/fd` once its first thread exited, that may be a root's lock must be the
   lock (device and inode) of a root the pass read, which then shows `held`. A file named `lock`,
   deleted or not, may be a root's when its directory is named `<label>-<32 hex>`, or when the
   process holds an exclusive `flock` on it (the `lock:` lines of `/proc/<pid>/fdinfo/<n>`),
   since a root's directory may have been renamed to any name. Every release since 0.8 takes a
   root's lock with `flock`, so a record lock (`fcntl`, as dpkg holds `/var/lib/dpkg/lock`) is
   not one, while any other process holding an exclusive `flock` on a file named `lock` makes the
   pass incomplete. Any other is an unread lock: a root moved, renamed, deleted or hidden under a
   live process, a root in another mount namespace, or a running copy of any release since 0.8,
   whatever it recorded. A lock in a reader directory is skipped, since readers hold no commit
   and record nothing. A reader directory is a reader's name (`reader-<n>`, or
   `reader-trash-<32 hex>` while a cleanup deletes it) inside a directory named
   `<label>-<32 hex>`, as it sits in its root. `reader-trash-<32 hex>` is also the root of a
   stream whose label is `reader-trash`, which is checked as a root like any other. A root
   renamed to a reader's name inside a directory with a root's name, or one in a cache directory
   whose own name has a root's grammar, is not seen. The descriptors of a process are those its
   first thread lists, or each thread's once the first exited, so a thread that unshared its
   descriptor table (`unshare(CLONE_FILES)`) while the first runs is not read. `--pid` limits the
   check to the processes it names, and a named process that is alive while `/proc` does not
   show it (`hidepid`, told apart from an exited one by `kill(pid, 0)`) makes the pass
   incomplete. Without `--pid` the inspector must run as root, since another user sees some
   processes as absent or cannot read their descriptors, and with either it needs a `/proc` in
   which it can list its own descriptors: otherwise the pass is incomplete (`unread_lock`).

```json
{"v":1,"dir":"/var/lib/sqlite-ursula/host","complete":true,"clean":false,"incomplete":null,
 "unacknowledged_bytes":14336,
 "dirs":[{"dir":"/home/app/.cache/sqlite-ursula/0.8","from":"record","present":true,"error":null}],
 "roots":[{"id":"notes-1f3a…","root":"/home/app/.cache/sqlite-ursula/0.8/notes-1f3a…",
           "stream":"/acme/notes","cluster":"c1","state":"pending","held":true,
           "unacknowledged_bytes":14336,"commits":3,"salvaged":0,"error":null}],
 "unread_locks":[],"unrecognized":[],"error":null}
```

- `complete` is true when the pass read the host directory and every record in it, found no name
  it does not know, read every root and the open files of every process, and found no unread
  lock, all within `--budget-ms`. `clean` means complete with no unacknowledged byte.
- `incomplete` is null for a complete inspection, and otherwise names one reason, the first that
  applies of `host_dir`, `unrecognized`, `unread_lock`, `unreadable` and `budget`.
- `error` is a message saying what failed when the pass as a whole did (the host directory, the
  process table or the budget), and null otherwise. A host acts on `incomplete`, never on the
  message.
- `dirs[]` lists each directory read: where it came from (`record` or `scan`), whether something
  is there, and `error`, a message when it may hold roots and could not be read.
- A root's `state` is `clean`, `pending` or `unreadable`, and its bytes, `commits` and `salvaged`
  are null when it is unreadable, with `error` its message. `held` says whether a live process
  holds its lock open, and is null when the lock check did not run (the process table could not
  be read, or the budget ran out first). `salvaged` counts the commits its salvages list as
  unacknowledged, and those of a superseded generation the next open moves into `salvage/`: the
  extension gave them up and never sends them, so they are outside the bytes, and a host reports
  them on their own.
- `unread_locks[]` holds the `pid` and the `path` of each unread lock, as `/proc` names it.
  `unrecognized[]` holds each name the host directory should not hold, with `error` saying why a
  record's name holds no record of this line, null for a name of no record.
- `--until-clean MS` reads again every 50 ms, rereading only the roots whose files changed, until
  the result is clean or incomplete or MS have passed. A root whose read failed while its files
  changed, as a drain trims and rolls its log, is read again rather than ending the wait. Any
  other incomplete result, a root that stays unreadable or a spent budget, ends it at once.
- `--budget-ms` bounds a pass. It stops it at the next entry of a directory it lists for roots or
  for a root's fingerprint, at the next descriptor of a process, or in the middle of a segment at
  its next read of at most 64 KiB. It does not bound an open or a read the kernel blocks, such as a FIFO planted in
  place of a segment or a hung mount, so a host also bounds the inspector's run time and treats a
  timeout as incomplete.
- The binary exits 0 when it printed a result and 2 on a usage error. Paths may be any bytes. The
  JSON may only grow within a release line.

A read proves nothing about commits made after it. A host freezes everything that may write a root,
syncs the filesystems, then inspects once, and takes the snapshot only from a complete inspection
with no unacknowledged byte. That holds when the host directory existed before any process that may
load the extension ran on the disk, and every filesystem the workload wrote to is mounted at the
path it was written through. A read while processes run can see a record being written as a torn
tail, and call a root clean too early. What no read sees is a root no process holds whose cache
directory is not at a recorded or scanned path in the inspector's view, as the extension does not
see it either until the root is opened where it now is. A copy of the extension from before the host
directory records nothing, so its roots are read only while it runs (the lock check) or in a
directory the host scans.

Tests: `hostdir/tests.rs` `the_default_wins_whenever_lstat_finds_it`,
`without_the_default_the_hook_decides`, `a_record_names_the_real_path_under_its_hash`,
`a_real_path_that_is_not_utf8_fails`, `an_open_records_its_cache_directory_once`,
`eight_threads_recording_eight_roots_leave_one_record`,
`a_record_that_is_not_the_directorys_fails_the_open`,
`a_record_that_is_not_a_regular_file_fails_the_open`, `a_deleted_record_is_written_again`,
`a_host_directory_that_is_not_a_directory_fails_the_open`,
`a_host_directory_the_process_cannot_write_fails_the_open`, `codec/vectors/mod.rs`
`the_host_directory_record_is_frozen`, and `test/host.e2e.ts` H1 to H5 (the record before the first
commit, through a symlink and under concurrent opens, nothing from readers and imports, the
`local_setup` refusal with nothing created on the stream, nothing without a host directory, and a
crash at each step of the record). The inspection: `inspection/tests.rs`
`root_names_and_lock_paths_have_one_grammar`, `the_host_directory_knows_each_name`,
`a_pass_counts_the_bytes_the_open_would_resend`,
`absent_directories_and_roots_without_a_generation_are_clean`,
`what_the_pass_cannot_read_makes_it_incomplete`,
`a_missing_host_directory_is_reported_and_the_rest_read`, `a_spent_budget_stops_the_pass`,
`an_open_lock_is_held_or_unread`, `a_held_lock_under_a_renamed_root_is_unread`,
`the_compiled_in_host_directory_is_checked_strictly`,
`a_root_that_cannot_be_looked_at_is_unreadable`, `the_check_of_every_process_needs_root`,
`replog/tests.rs` `a_fold_past_its_deadline_stops_the_read` and the read's equivalence proptest,
`test/host.e2e.ts` H6 to H8 and H13 to H15, and CI's `sqlite-vfs-host-musl`, which runs the static
binary as root against the compiled-in directory. H6: after a crash at each step of the commit path
and of a checkpoint (both checkpoint modes), of a re-key while bound and at open, of a publish that
salvages, of a fresh open's first publish, of a trim and of an open's cut of a torn tail, the bytes
read equal the `commits.pending_bytes` of a reopen with the stream refused, the salvaged commits
are the same before and after the reopen, and the root drains to clean. The others: unreadable and
unrecognized states, an inspection that changes no byte or name, an orphan drained by a reopen, an
unread lock under a cache directory moved or deleted and under a root renamed out of its grammar,
and a 0.11.0 copy's root read in a scanned directory.

## API

```ts
export function open(url: string | URL, options?: OpenOptions): Promise<UrsulaDatabase>;
export function importSqlite(url: string | URL, file: string, options?: ImportOptions): Imported;

export interface OpenOptions extends Omit<DatabaseSyncOptions, "open" | "readOnly"> {
  dir?: string;             // cache root, default <user cache>/sqlite-ursula/0.8
  token?: string | TokenProvider; // one per process: the last open's applies (Tokens)
  signal?: AbortSignal;     // bounds the open, 30 s without one
  maxPendingBytes?: number; // default 64 MiB of compressed frames
  recreate?: boolean;       // stream missing, local data present: salvage, create, take over
  readOnly?: boolean;       // claims nothing (Readers)
}

export class UrsulaDatabase extends DatabaseSync {
  readonly name: string;    // "/acme/notes"
  synced(options?: { signal?: AbortSignal }): Promise<void>;
  status(): UrsulaStatus;
  on(event: "reset", listener: (error: UrsulaResetError) => void): this;
  off(event: "reset", listener: (error: UrsulaResetError) => void): this;
}

export interface BearerToken {
  token: string;
  expiresAt: Date | number; // epoch ms
}
export type TokenProvider = (options: { signal: AbortSignal }) => Promise<BearerToken>;

export interface GateOptions {
  signal?: AbortSignal;     // bounds the wait for the outcome (Gates)
}
export function gate<T>(db: UrsulaDatabase | readonly UrsulaDatabase[], work: () => T | Promise<T>, options?: GateOptions): Promise<Awaited<T>>;
```

- The first open of a stream in a process binds, catches up and claims ([Open](#open)). Later
  opens return another connection on the same binding with no claim and no network, concurrent
  opens share one bind and its outcome, and worker threads call `open` too.
- `UrsulaDatabase` is a `DatabaseSync` subclass, so drizzle, kysely and Pi's driver take it. It
  remembers an error after which no transaction is open, so a helper's ROLLBACK does not replace it.
  node:sqlite's busy `timeout` defaults to 5000 ms, since a binding's connections share its files.
- `db.close()` is synchronous. After a binding's last connection closes, replication drains in the
  background, then the lock is released. There is no drain at exit: the log keeps pending commits
  for the next open.

Tests: `test/open.e2e.ts` "binds and claims the stream, and returns a DatabaseSync named after
it", "returns another connection on the same binding for a later open, with no claim", "shares
one open between concurrent calls", "runs under Kysely, whose errors are the database",
`test/workers.e2e.ts` "open the stream the main thread holds: one binding, one claim, synced() in
each thread", `driver/tests.rs` `a_binding_no_connection_opens_retires`.

### Synced

At the call `synced()` takes the target `T`, the last logged commit. Positions are assigned at the
commit point, before SQLite can publish, so every commit a reader can see is at or below `T`. It
resolves when every record up to `T` is acknowledged and a stream round trip that started after
the call found no foreign frame past this owner's acknowledged offset: a receipt contiguous with
the expected tail, or a `consistency=leader` read at the tail, with the incarnation precondition,
that returns no bytes.

Concurrent callers share one probe, and under write load appends are the probe, so only an idle
owner pays one leader read per gated output. There is no freshness window. `synced()` rejects with
the reset error on a reset and with `signal.reason` on its signal, and the commits keep
replicating. A provisional database's waits for its claim. A reader's resolves at once. Tickets
wait while the replicator activates, so none resolves below its target during a re-key.

Tests: `replicator/tests/machine.rs` `synced_waits_for_a_round_trip_that_started_after_the_call`,
`a_probe_that_finds_our_own_batch_waits_for_its_answer`,
`a_stale_probe_never_reads_our_own_acknowledged_batch_as_foreign`,
`a_ticket_waits_for_the_continuation_reconcile_to_finish`, `driver/tests.rs`
`an_idle_owner_pays_one_probe_per_synced`, `tickets_resolve_in_order_through_one_watermark`,
`a_sync_ahead_of_its_logged_waits_for_it`, the world's property 9 with
`negative_control_settling_tickets_while_activating_resolves_below_the_target`, and
`test/synced.e2e.ts` "resolves once the stream holds every commit made before the call", "rejects
with its signal's reason, while the commits keep replicating", "resolves thousands of concurrent
calls through one watermark while the stream holds its answer back".

### Gates

A gate holds back what leaves the process until `synced()` says it may. It is the one definition of output gating: `gate` (`src/gate.ts`) implements it, and any later gate (a `node:http` helper, an in-guest HTTP gate) implements the same table. The gate is `synced()` itself: a gate only decides when to call it and what to hold, and adds no wire field, extension call or dependency. Gating twice is safe and costs at most one more round trip.

| what is held | released when | if a reset comes first | if `signal` fires first |
| --- | --- | --- | --- |
| `gate`: the result of `work`, or its error unless that is or wraps an `UrsulaError` | a `synced()` call made after `work` settled resolves on every database | rejects with the `UrsulaResetError`; the outcome is dropped | rejects with `signal.reason`; the outcome is dropped and a returned body is cancelled |
| `gate`: each chunk of a returned global `Response` body, and its end or error | a `synced()` call made after the chunk was read resolves on every database | the body errors and its source is cancelled | not applied; cancelling the body ends the waits |

1. **Release.** An output is released only by a `synced()` call made after the output was produced, on every database given. The queue takes its cutoff before it calls `synced()`.
2. **Batching.** A gate has at most one `synced()` call per database in flight. Outputs produced meanwhile wait for the next call, so a stream costs one round trip per batch, not per chunk. A body is read ahead at most 64 KiB plus one chunk, counting held chunks and released chunks its reader has not taken: the bound is checked before each read, so one chunk of any size is read once the body is under it.
3. **Order.** Outputs leave in the order they were produced.
4. **Package errors.** An `UrsulaError` thrown by `work` is rethrown at once, also when a library wrapped it (as a `cause`, or in an `AggregateError`: the chain `classifyWrapped` walks, through the same `findWrapped`), because it carries no application state: a refusal committed nothing, and a reset is what `synced()` would report. Every other outcome waits, errors included.
5. **Resets.** A reset replaces the outcome. A gate never releases what it held after its wait failed, and a failure on one database ends the waits on the others.
6. **Signal scope.** `signal` bounds the wait only until the head (the outcome, or a `Response`'s status and headers) has left, so a timeout never cuts a long stream short. A signal that fired before the call keeps `work` from running.
7. **Cancellation.** Cancelling a gated body cancels its source and aborts the wait in flight, so no ticket outlives the body ([Pump](#pump)).

A returned `Response` comes back as a new `Response` with the same status, status text and headers. Its `url`, `type` and `redirected` are not carried, since a handler's response has none of them.

What a gate does not cover: effects inside `work` before it returns, such as an outbound call or a manual acknowledgement in the middle of a handler, which need `await db.synced()` first; WebSocket frames, raw sockets, `node:http` responses written outside the gate, timers, background jobs and child processes; exactly-once effects, since a crash after the release but before an acknowledgement repeats the work, so effects need idempotency keys and message ids recorded in the same transaction; and anything beyond what `synced()` means.

Tests: `test/gated.e2e.ts` "G1: settles only once the stream acknowledged the commit work made, where awaiting work alone does not", "G2: holds a read-only result that read another call's pending commit until that commit is acknowledged", "G3: holds work's own error as it holds a result, and rethrows a backlog_full refusal at once, also when a library wrapped it", "G4: rejects with the reset when another process took the stream over before the release, never with the result", "G5: rejects with its signal's reason, cancelling a returned body, while the commits keep replicating", "G6: passes a returned Response's head, and each chunk, on only after the commit before it is acknowledged, in order", "G7: gates 10,000 chunks with one synced() call per batch, not per chunk", "G8: errors a body with the reset when another process takes the stream over in the middle of it", "G9: with two databases, settles only once both acknowledged", "G10: holds a body's end, and its error, until the commit before them is acknowledged", "G11: a body cancelled while its synced() call is in flight cancels the source with the reason and leaves no call pending", "G12: errors a body with a TypeError when its source enqueues a chunk that is not bytes".

### Resets

| kind | trigger | action | unacknowledged commits | next open |
| --- | --- | --- | --- | --- |
| `fenced` | `key-conflict`, `epoch-closed` or another producer `403` to an append, `receipt-evicted` for the in-flight seq, a lost continuation claim, a foreign claim in a pre-check | `stop` | kept for the next reconcile | takeover |
| `stale` | a continuation found the stream moved on, a foreign commit in a pre-check (our commit id with other bytes included), an idle `producer-expired` without the owner's claim frame, a provisional open whose log lost acknowledged commits to a power loss | `reopen` | discarded | takeover, catch-up or rebuild |
| `rewound` | a `412` confirmed by HEAD when the generation does not inherit | `reopen` | salvaged at the reopen | salvage, rebuild |
| `deleted` | a `404` confirmed by HEAD with the generation's cluster id | `fix` | kept | `not_found` unless `recreate` |
| `local_damage` | a failed fsync, a lost WAL page, an uncuttable log write, an unconfirmed commit, a foreign page 1 | `reopen` | kept | materialize |
| `stream_state` | a foreign append, a closed stream, `Producer-Expected-Seq`, a batch above `Producer-Closed-Seq` | `fix` | kept | refused until fixed |
| `internal` | a panic in a file method or in the replicator | `reopen` | kept | materialize |

After a reset, pending `synced()` calls reject, then `reset` listeners run once. Every statement of
the binding throws, reads included (a new read transaction fails at its read-mark lock), and
db-file writes are refused, so no close-time checkpoint moves pages of a commit that may be
discarded. An open read transaction keeps its snapshot. The binding retires, and the next `open()`
need not wait for old handles ([Materialize](#materialize)).

Tests: `error.rs` `resets_say_what_to_do`, `vfs/tests.rs`
`a_reset_refuses_new_reads_and_every_write`, `driver/tests.rs`
`a_404_confirmed_by_head_resets_deleted`, `a_closed_stream_resets_stream_state`,
`test/reset.e2e.ts` "a takeover rejects pending synced() calls, then runs the listeners once, and
fails every statement", "runs the listeners when a commit's append finds the takeover, with no
synced() pending", `test/core.e2e.ts` "resets a database another process took over: 266 for reads,
778 for writes, a fenced event", `test/workers.e2e.ts` "hear a reset in every thread: synced()
rejects, the listeners run, statements throw".

### Errors

Four classes with `kind`, `action` (`retry`, `reopen`, `stop`, `fix`), `message` and `cause`,
mapped by one function (`errors.ts` `classify`). Callers branch on `action`.

| class | thrown by | kinds |
| --- | --- | --- |
| `UrsulaWriteRefusedError` | a write or COMMIT, nothing committed | `backlog_full` (`retry` after `synced()`), `unsupported` (`fix`), `too_large` (`fix`: split the transaction or raise the commit limit) |
| `UrsulaResetError` | statements after a reset, `synced()`, `reset` listeners | the kinds of [Resets](#resets) |
| `UrsulaUnavailableError` | `open()` when its signal fires (also while it waits for a token provider: `unavailable`, action `retry`), and `importSqlite()` when it cannot finish (action `fix`, since a failed import leaves the stream refusing opens) | the last condition: `unreachable`, `unavailable`, `rate_limited`, `unauthorized`, `payment_required`, `forbidden`, or `cancelled` when none was met |
| `UrsulaSetupError` | `open()`, `importSqlite()`, and `synced()` or `status()` of a retired database | `invalid_url`, `invalid_argument`, `not_found`, `in_use`, `bound_elsewhere`, `local_setup`, `stream_format`, `no_server_identity`, `protocol_mismatch`, `wrong_cluster`, `stream_state`, `claim_races`, `not_bound`, `internal` |

Network failures never throw from statements or `synced()`. A refused write fails with
`SQLITE_IOERR_AUTH` (7178), a read after a reset with `SQLITE_IOERR_READ` (266), a write after one
with `SQLITE_IOERR_WRITE` (778). Other errors stay SQLite's own. Every SQL function's error reads
`<function>: <kind>: <message>`.

Tests: `error.rs` `write_refusals_say_what_to_do`, `resets_say_what_to_do`, `test/open.e2e.ts`
"refuses a URL it cannot take, as a setup error to fix", "reports the last condition the open met
when its signal fires", "refuses an unsupported write alone, and leaves SQLite", `test/limits.e2e.ts`
"refuses a transaction above the commit limit alone, as too_large, and commits the next".

### Pi

`openUrsulaPiStorage(url, options)` (the `/pi` entry) opens with `open`, runs Pi Durable's official
node driver unmodified on `db.location()`, and wraps its `transaction`, `run`, `exec`, `get` and
`all` to resolve only after `await db.synced()`. A Pi commit resolves once the stream holds it,
which is Pi's `Storage.commit` contract, and a read once what it read is confirmed.

- `backlog_full` commits nothing. From Pi 1.2, whose storages retry their own transient failures,
  the adapter waits for `synced()` and runs the work again. On Pi 1.0 and 1.1 it throws Pi's
  `StorageRejected`, and the session continues.
- A reset throws the `UrsulaResetError` itself, unwrapped, so Pi's Session stops and the
  host reopens or stops.
- `close` waits up to `closeTimeoutMs` (10 s) for `synced()`, then closes. Pi 1.3's
  `Session.close` waits for storage calls in flight, and those wait for `synced()`, so closing a
  Pi session during an outage waits for the stream, and `closeTimeoutMs` applies only once the
  session reaches the storage's `close`.
- `@earendil-works/pi-durable` is an optional peer dependency, `>=1.0.0 <1.4.0`.

Tests: `test/pi.e2e.ts` "runs a commit refused with backlog_full again once synced() drained the
backlog (Pi 1.2 on)", "throws backlog_full as Pi's StorageRejected, nothing committed, and the
storage goes on (Pi up to 1.1)", "throws a reset as the UrsulaResetError itself, and closes",
"waits for synced() on close, at most closeTimeoutMs", and Pi's storage conformance suite in
`test/conformance.e2e.ts`, run in CI on Pi 1.0.0 and the newest supported Pi.

### Tonbo

`open(nameOrUrl, options)` of the `/tonbo` entry (`tonbo.ts`) opens a database of a Tonbo account
by name with a Tonbo API key, then calls `open` with the stream URL and a token provider. It only
resolves the URL and the Streams token: everything after is the root entry's open. That resolution
is `resolveDatabase(nameOrUrl, options)`, exported on its own: it returns `{ url, token }`, and
`open` is it followed by the root entry's open, so the key sources, the lookup and the provider have
one implementation. A caller of another opener, such as `openUrsulaPiStorage`, passes both to it,
and that open calls the provider.

The `/tonbo/pi` entry (`tonbo-pi.ts`) is that composition for Pi: its `openUrsulaPiStorage(nameOrUrl,
options)` is `resolveDatabase` followed by the `/pi` entry's `openUrsulaPiStorage`, under one signal
(30 s by default) for both, so `Harness.open(await openUrsulaPiStorage("harness-1"), ...)` persists a
Pi harness in one line. It is an entry of its own because `/tonbo` must not import
`@earendil-works/pi-durable`, an optional peer only Pi users install, and `/pi` knows nothing of
Tonbo. It adds no logic: name resolution, the key sources and the provider stay in `tonbo.ts`, and
the adapter in `pi.ts`. Pi's `Harness.close` closes the storage it was opened with (Pi 1.0 to 1.3:
`Session.close` calls `Storage.close`, and `SqliteStorage.close` the driver's `close`, which the
adapter wraps), so the caller keeps no reference for the close.

- The key is the `apiKey` option, else `TONBO_API_KEY`, else the login `tonbo login` saves (the
  Tonbo CLI's credentials contract, version 1): `${XDG_CONFIG_HOME:-$HOME/.config}/tonbo/credentials.json`,
  `{version, account_id, key_id, key, expires_at}`. A symlink, a non-regular file or a file with a
  group or other permission bit is refused (`credentials_refused`) and never read, with no other
  source tried. The file is opened with `O_NOFOLLOW` and checked again on the descriptor. A login of
  another version or schema, or one whose `expires_at` has passed, fails `invalid_api_key`.
- A name is looked up with `GET {api origin}/v1/databases/{name}`, the key as the bearer token. A
  `401` with `exchange_required` exchanges the key at the token endpoint with no audience and sends
  the lookup again once with that token, which is kept per key until 30 s before it expires. A
  `401` to a held token exchanges again and retries once. The key goes only to the two configured
  origins, never to an endpoint an answer names, and no redirect is followed (`redirect: "manual"`,
  a 3xx fails `invalid_argument`). A Streams token never goes to the Management API.
- An `active` database opens its `url`. `creating`, or a state the package does not know, is looked
  up again with backoff. `deleting` and `deleted` fail `database_not_found`, as `404` does. `401`
  fails `invalid_api_key`, as `403` does with a message that the key cannot read the database. `429`
  waits for `Retry-After` but never less than the backoff, and `408`, `5xx` and network failures
  back off from 250 ms up to 2 s, until the open's signal fires: then `UrsulaUnavailableError` with
  the last condition. An `active` database without a `url`, and any other status, fail
  `invalid_argument` naming the option and variable of that origin.
- The Streams token comes from the provider each open passes, a closure over that open's key, its
  source and the auth origin. The process holds one token whichever function minted it, so later
  opens reuse it and the next refresh runs the last open's provider ([Tokens](#tokens)). It is
  attached only to a stream URL that is `https://`, or `http://` to a loopback host: any other
  fails `invalid_url`. One token per process means one key per process: an open with a second key
  reuses the first key's token until the next refresh exchanges the second. It posts
  `{"audience":"https://streams.tonbo.dev"}` to `{auth origin}/api/iam/token`. A `401` or `403`
  there throws `invalid_api_key`, which fails for good. An explicit `token`, or
  `URSULA_VFS_TOKEN_FILE`, serves Streams requests instead, and then a URL needs no key. The
  extension reads the file only while no open of the process has set a token.
- Origins are `https://`, or `http://` to a loopback host. `TONBO_API_ORIGIN` and
  `TONBO_AUTH_ORIGIN` set them when the options do not.
- Errors are `TonboError`, kinds `missing_credentials`, `credentials_refused`, `invalid_api_key`
  and `database_not_found`, action `fix`, with `fix` (the variable or command, which ends the
  message) and the API's `requestId`. No message carries a key or a token.

Tests: `test/tonbo.e2e.ts`, through the real `open` against the suite's servers. test/tonbo-api.ts
stands in for the Management API and the token endpoint as their contracts document them, and
serves databases whose URLs are streams of the suite's server. The credentials cases are the
contract's own usable and unusable examples. Each test waits until its bindings retired, so the
next starts with no token held. `URSULA_VFS_TOKEN_FILE` runs in a process of its own
(test/tonbo-child.ts), on a stream and cache root of its own with a token no other test uses, where
no open set a token first, and the gate shows every request carried the file's token.
`resolveDatabase` runs with `openUrsulaPiStorage`: the gate shows the Pi storage's stream requests
carried the token the provider exchanged for the key. "opens Pi storage by name in one call for
Harness.open, whose close closes it, and its Streams requests carry the token exchanged for the key"
runs the `/tonbo/pi` entry under Pi's `Harness`, and its binding retires once `harness.close`
returned.

### SQL functions

Other hosts call the extension directly. Long calls take `timeout_ms`, where 0 starts the work and
returns at once.

- `ursula_open(url, dir, options_json, timeout_ms)`: open, returning `{done, path, name, state}` or
  `{done: false, request}`. `options_json` holds `read_only`, `recreate`, `activation` (32 hex
  digits), `max_pending_bytes`, `token` and `timeout_ms`. A path `ursula_open` handed out stays
  openable until a connection opens it, or for a minute.
- `ursula_synced(path, timeout_ms)`: `{synced, ...}`, or the reset error.
- `ursula_events(cursor, timeout_ms)`: completions, acknowledgements, conditions and resets from a
  bounded queue of 4,096, acknowledgements coalesced, what was dropped reported.
- `ursula_import(url, file, options_json, timeout_ms)`: [Import](#import). `timeout_ms` must be
  positive.
- `ursula_status(path)`, `ursula_stats(path)`, `ursula_cancel(request)`,
  `ursula_set_token(path, token)`.

The extension refuses to load into SQLite older than 3.50.4.

Tests: `test/core.e2e.ts` "loads into the running SQLite and registers the 0.8 functions only",
`options.rs` `every_option`, `refusals_name_the_option`, `extension.rs` `the_floor_is_3_50_4`,
`driver/tests.rs` `the_event_log_coalesces_acks_and_reports_what_it_dropped`, `test/handout.e2e.ts`.

### Import

`ursula_import` (TypeScript `importSqlite(url, file, { token, timeoutMs })`) makes a plain SQLite
file the first snapshot of a new stream, page for page:

1. The file is checked and copied into a temp directory before anything is sent: SQLite's magic,
   4096-byte pages, no `-wal` and no hot journal beside it (before and after the copy), a length of
   exactly the pages its header counts. Page 1 of the copy is marked WAL.
2. A HEAD: the stream must be missing, or an empty stream of the VFS content type with no
   snapshot. Anything else refuses (`no_server_identity`, `protocol_mismatch`, `stream_format`,
   `stream_state`). A `404` without `Ursula-Protocol` is `no_server_identity`, never a missing
   stream.
3. The create, then a claim of epoch 1 with the reserved import activation (`ursula-import-v1`),
   resent unchanged until a definite answer. Its receipt must start at offset 0. Its end is `W`,
   above 0, since builds skip a snapshot at offset 0, and the first open then claims above epoch 1.
4. The `USS3` body at `W`, epoch 1, a fresh lineage, position 0.
5. A HEAD naming the tail `W`, the publish of [Snapshots](#snapshots) (one `PUT`, or parts with a
   cold store, otherwise refused naming the limit), then a HEAD again. The import is done only on
   a `204` naming `(W, digest)` and a HEAD naming that snapshot with the tail still at `W`. Bytes
   after `W` fail with `stream_state`.

The source file is only read. Nothing is published on a stream that is not new. A snapshot at `W`
need not equal a replay of the bytes before it, so a build or a reader catch-up that reads from
offset 0 with no snapshot and meets the import's claim refuses with `stream_state` and never
builds an empty database. While an import runs, opens of the stream fail that way. A failure
after the claim leaves a stream that refuses opens and imports with `stream_state` until it is
deleted or recreated.

Tests: `import/tests.rs` `a_header_must_name_whole_4096_byte_pages`,
`a_wal_or_a_hot_journal_beside_the_file_refuses_it`,
`a_file_becomes_the_first_snapshot_of_a_new_stream`,
`a_file_above_the_request_limit_goes_up_in_parts`,
`a_file_above_the_limit_without_parts_is_refused_naming_it`,
`only_a_missing_or_empty_stream_is_imported_into`, `a_claim_that_is_not_first_publishes_nothing`,
`a_claim_whose_answer_is_lost_is_resent_unchanged`, `bytes_after_the_claim_fail_the_import`,
`a_head_404_without_the_protocol_creates_nothing`,
`a_server_of_another_release_line_creates_nothing`, `the_claim_carries_the_import_activation`,
`codec/stream.rs` `the_claim_of_an_import_stops_the_read`, `driver/tests.rs`
`an_empty_owner_behind_an_import_builds_from_its_snapshot_and_claims_after`,
`a_reader_built_before_an_import_reopens_from_its_snapshot`, and `test/import.e2e.ts` "imports a
plain database as the first snapshot of a new stream, in parts under a 64 KiB limit", "refuses a
file with 8192-byte pages before anything is sent", "refuses a file with a -wal beside it before
anything is sent".

### Pump

Main-thread calls into the extension return at once (timeout 0). The package starts one unref'd
worker per JavaScript thread that uses it. The worker loads the extension the process already
holds, shares its registry, and loops on `SELECT ursula_events(cursor, 1000)`, posting events that
resolve `synced()`, complete `open()` and fire `reset` listeners. No native addon is needed. What
still blocks is what blocks plain SQLite: local I/O and the busy handler. `importSqlite` blocks its
thread for at most its timeout.

Tests: `test/synced.e2e.ts` "waits without blocking the event loop while the stream holds its
answer back", "asks again for what a gap in the events may have dropped", "keeps the process alive
while it waits: `await db.synced()` never ends the process unresolved", "lets an idle process exit
on its own, its database left open", `test/workers.e2e.ts`.

### Tokens

`open()`'s `token` is a string or a provider, an async function that returns `{ token, expiresAt }`
(`token.ts`). The extension knows only the process's token, which the package sets with
`ursula_set_token` for both kinds, once per open. No token goes into `ursula_open`'s options, which
a bind sends again on each of its retries and would put back a token older than one applied
meanwhile. So the last open that names a token sets it.

- One source per JavaScript thread, since a function lives on the thread that made it. The
  process carries one token, so which function minted the token held does not matter: an open with
  another function only names the function the next call runs, and shares the token held and the
  call in flight. An open with a string stops the source on its thread: the call in flight is
  abandoned (its `signal` fires), and the opens waiting go on under the string. An open without
  `token` changes nothing.
- An open asks the source for a token before its first request. A valid token held serves at
  once, so a connection on a bound binding needs no network, and a refresh that is due runs in the
  background. An expired one serves only while the provider fails (a call failed since the last
  success, or it failed for good): a bind under it then fails with the extension's own condition.
  Otherwise the open waits for the call in flight or a new one: after a suspension longer than the
  token's lifetime, the first open binds under the next token rather than the expired one. At most
  one call of the provider runs at a time, and concurrent opens share it. A call abandoned before it
  started never reaches the provider.
- A refresh is due after 70 to 80% of the token's lifetime, and at least 1 s after the token
  arrived. A token that expires within 1 s counts as a failed call, so backoff bounds how often a
  provider with a skewed clock is called. A failed call is retried after
  `min(30 s, 1 s * 2^(n-1))`, jittered down to half. The provider's own `UrsulaError` with action
  `fix`, a token that is not printable ASCII without spaces, or an `expiresAt` that is not a time
  in milliseconds (one below 10^12 is in seconds) fails for good: the waiting opens reject with it,
  and the source calls the provider again only for the next open. An open whose signal fires while
  it waits rejects with `UrsulaUnavailableError` `unavailable`, action `retry`, whose `cause` is the
  last failure, or the signal's reason when no call failed.
- The pump's `condition` event `unauthorized` (a request of a bound binding refused twice with
  `401`) drops the token held and calls the provider at once, unless a retry is scheduled. Setting
  the new token sends the blocked request again at once ([Transport](#transport)). After a `gap`,
  and when a failed pump worker is replaced, the bound paths' status says whether one was missed.
  The pump replaces a failed worker while the source serves a bound path, so no `401` goes unheard.
  The driver reports conditions of bound bindings only, so a bind refused with `401` reaches the
  source when the open fails `unauthorized` at its signal: the refused token is dropped and a call
  starts, unless a newer token is held already.
- The last failure is kept until a call succeeds. An open that fails `unauthorized` carries it as
  its `cause`, with action `retry` unless it failed for good, since a new call may then succeed.
  With no failure recorded the server refused a fresh token, and the action stays `fix`. Before
  that, a failure after the open shows only once the server refuses the token held, as the
  replication condition `unauthorized`.
- Timers are unref'd, except a retry while an open waits for a token. The source follows every path
  its thread bound, whatever token the open named, since they all carry the process's token. Each
  `retired` event drops the paths whose status says retired. When a refresh is due, or a binding
  retires, the source stops if no path is bound and no open waits for it or is binding.
- `importSqlite` takes a string only: it blocks its thread, so nothing could refresh it meanwhile.

Tests: `test/token.e2e.ts` "is called again for the next open after an open was refused with 401",
"is called before the open's first request, again ahead of each expiry, and no more once no binding
uses it", "is called once for concurrent opens, and a later open with the same function reuses its
token", "serves concurrent opens with different functions from one call, and runs the last one
next", "serves the token it holds at once, expired too, while its refresh fails", "waits for the
refresh in flight rather than serve an expired token while it is healthy", "is called again at once
when a request is refused with 401", "fails the open with its own error when it fails for good, and
is retried after any other failure", "fails the open at its signal with `unavailable`, `retry`,
when it handed out no token", "fails the open with `invalid_argument` when it hands out a token that
cannot be sent", "stops once an open with a string token replaced it, and the opens waiting for it
go on under the string", "hands the bindings it served to the provider of a later open, which
shares the token held", "keeps refreshing while a binding opened with a string token is bound",
"never keeps the process alive: an idle process exits on its own". In front of Ursula,
test/gate.mjs records each request's `Authorization` header, and the tests check which token each
request of their streams carried. The `401`s are scripted faults (the model's, or the gate's),
since neither e2e server checks tokens.

## Status

`db.status()` and `ursula_status(path)` return one snake_case JSON:

```jsonc
{
  "name": "/acme/notes", "path": ".../gen-<id32>/db",
  "state": "live",                  // provisional | live | retiring | retired | reset
  "role": "owner",                  // owner | reader
  "closed": false, "epoch": 7, "cluster_id": "...", "incarnation": "...", "lineage": "...",
  "commits": { "logged": 1234, "confirmed": 1234, "acknowledged": 1231, "pending": 3, "pending_bytes": 14336 },
  "replication": { "state": "streaming", "condition": null },   // activating | streaming | reset | refused | stopped | none
  "attach": { "local": "resumed", "snapshot_offset": null },    // resumed | rekeyed | caught_up | built | salvaged
  "snapshot": { "offset": 1048576, "age_ms": 1000, "failures": 0, "error": null, "log_bytes": 123, "due_bytes": 8388608 },
  "refused": null,                  // the last write refused with nothing committed
  "salvage": [],                    // { dir, reason, time, incarnation, stream_incarnation, acknowledged_position, unacknowledged, plain }
  "reset": null                     // { kind, action }
}
```

- `replication.condition` names the retrying or blocked condition of [Answers](#answers):
  `retrying`, `request_too_large`, `unauthorized`, `payment_required`, `forbidden`, `redirected`,
  `wrong_cluster`, `no_server_identity`, `protocol_mismatch`, `rejected`.
- `ursula_stats(path)` drains per-commit measurements (bytes, pages, time in the log and the VFS),
  at most 8,192 of each kind between drains, and reports what it dropped.
- Logs are one logfmt line per event on stderr: `sqlite-ursula-vfs level=warn event=<event> ...`.
  Events include `open`, `provisional`, `live`, `rekey`, `salvaged`, `reset`, `recut`, `log_cut`,
  `write_refused`, `snapshot_published`, `snapshot_failed`, `snapshots_stopped`,
  `retention_failed`, `dns_stale`, `token_in_clear`, `token_file_unreadable`, `setting_adjusted`,
  `host_dir_unreadable` (the host directory's path cannot be read, so it counts as absent) and
  `panic`.
- Alert on a growing `commits.pending_bytes`, any `replication.condition` that stays, refused
  writes, any reset, and a `snapshot.log_bytes` that stays above a few times `due_bytes`.

Tests: `status.rs` `a_handed_out_binding_reports_live_or_provisional`, `synced_names_its_target`,
`events_render_as_one_object`, `stats_report_what_was_dropped`, `stats.rs`
`a_database_nobody_drains_keeps_a_bounded_number_of_each_kind`, `log.rs`
`values_are_quoted_only_when_needed`.

## Contract

The VFS relies on these server behaviors, each specified in the
[extensions specification](../web/src/content/docs/pages/specs/extensions.mdx). Every row is
implemented by the model server (`model/`), which the proptest world and the e2e suites run
against, and the e2e suites run against a real server in CI.

| # | contract | extensions heading | used in |
| --- | --- | --- | --- |
| 1 | `Stream-Incarnation` changes whenever bytes at a served offset may differ, compared at apply for writes and against the serving state for reads (`412` with the current token) | Snapshot Metadata on HEAD and Stream Incarnation | [Appends](#appends), [History](#history) |
| 2 | A history event changes every token of the group and closes every producer epoch (`epoch-closed`, `by=history`) | Producer Write Answers | [Answers](#answers), [History](#history) |
| 3 | The epoch floor: a tokened write from a producer without a record at or below the highest retired epoch gets `403 producer-expired` naming the floor | Epoch Floor | [Answers](#answers), [Claims](#claims) |
| 4 | Definite repeats: a repeat with the same key and digest gets the newest write's receipt, `receipt-evicted`, `epoch-closed` with `Producer-Closed-*`, `key-conflict`, `producer-expired`, or `Producer-Expected-Seq`. The epoch is checked before deduplication, and every producer `403` carries `Producer-Epoch` | Producer Write Answers | [Answers](#answers) |
| 5 | `Producer-Digest` is the BLAKE3-256 of the body, verified, and another identity under a held key gets `key-conflict` | Write Identity | [Appends](#appends), [Claims](#claims), [Clones](#clones) |
| 6 | Receipts carry `Stream-Offset` and `Stream-Next-Offset` of the request's own write, and a claim's answer and its repeats carry `Producer-Closed-Epoch` and `Producer-Closed-Seq` | Producer Write Answers | [Appends](#appends), [Claims](#claims) |
| 7 | Size, routing, repeats and key conflicts are answered before the body, and a node never redirects after reading a body. Over HTTP/1.1 an early answer drains the body | Answers Before the Body, Routing Answers | [Answers](#answers), [Transport](#transport) |
| 8 | Snapshots are keyed by `(offset, digest)`, `204` or `409` naming the latest. Parts are staged and committed by id, a commit checks its key before its parts, and retention is monotonic | Publish Snapshot | [Snapshots](#snapshots), [Import](#import) |
| 9 | Snapshot reads honor one byte range | Read Snapshot | [Snapshots](#snapshots) |
| 10 | HEAD carries the token, tail, snapshot, `Ursula-Protocol` (the server's release line), `Ursula-Cluster-Id` (on every answer, `404` included), `Ursula-Max-Request-Bytes`, `Ursula-Max-Snapshot-Bytes` with a cold store, and after a history event `Stream-Previous-Incarnation` and `Stream-Inherited-Offset`, all in one integer offset space | Server Metadata, Snapshot Metadata on HEAD and Stream Incarnation | [Appends](#appends), [Open](#open), [History](#history), [Identity](#identity) |
| 11 | Every `413` carries the limit it applied, decided from `Content-Length` | Server Metadata | [Answers](#answers) |
| 12 | The cluster id is stable across restarts, upgrades, leader changes and history events, and unique among the deployments a client reaches | Server Metadata | [Log](#log), [Identity](#identity) |
| 13 | Leader-consistent reads with the incarnation precondition | Leader Consistency | [Reconcile](#reconcile), [Synced](#synced) |
| 14 | Every `410` and `416` of a read, and the `404` of a snapshot that is not the latest, carry the token of the stream that refused them and come before the `412`. A missing stream's `404` carries none | Snapshot Metadata on HEAD and Stream Incarnation | [Answers](#answers), [Pre-check](#pre-check) |

Tests: `model/tests.rs` and `model/producer.rs` (one test per producer row, the floor, digests,
history events and inheritance, HEAD identity on `200` and `404`, typed `413`s, snapshots, parts
and ranges), and `transport/http/tests.rs`
`addendum_4_the_order_is_the_same_over_http_and_in_process`. The CI job `sqlite-vfs-e2e-ursula`
runs every e2e suite against one real node with an in-memory cold store and against three nodes behind the gateway, and `sqlite-vfs-e2e-ursula-parts` runs the parts suites against three nodes behind a gateway with a 4 MiB request limit and an S3 cold store ([Verification](#verification)).

## Formats

| | format |
| --- | --- |
| stream frames | `USQ2` ([Frames](#frames)), and the reserved import activation `ursula-import-v1` ([Import](#import)) |
| snapshot bodies | `USS3` ([Snapshots](#snapshots)), digest BLAKE3 of the length-prefixed content type and the body |
| local state | generation directories ([Layout](#layout)), `USL1` log segments ([Log](#log)), the page-1 marker ([Marker](#marker)), `db-uwal`, `manifest.json` |
| host directory | `dir-<key>` records, and the JSON of `ursula-vfs-host inspect` ([Hosts](#hosts)) |
| producer | `Producer-Id: sqlite-ursula-vfs/2`, `Producer-Digest`, receipt contiguity |
| content type | `application/vnd.ursula.sqlite-vfs.v2` |

Within a release line these formats do not change, so owners upgrade one at a time. Golden
vectors freeze their bytes. The APIs that may only grow within a release line are the
`options_json` keys, the `ursula_open` result, status keys and values, event shapes, error kinds
and actions, the result codes 7178, 266 and 778, the SQL function names and arities, and the JSON
`ursula-vfs-host inspect` prints.

0.8 refuses 0.7 streams (`stream_format`) and 0.7 servers and never reads 0.7 caches, since the
default directory and the names changed. 0.7 fails closed on 0.8 files.

The extension opens only on its own release line: `Ursula-Protocol` carries the server's line,
major × 1000 + minor (`11` for 0.11.x), and the extension refuses another line
(`protocol_mismatch`) and an answer without it and `Ursula-Cluster-Id` (`no_server_identity`) at
open, at import, at a snapshot's `HEAD` and at a bound database's `HEAD` ([Identity](#identity)).
A bound database writes without a `HEAD` between, so every SQLite session stops before a cluster
moves to another line.
Patches of one line interoperate. The 0.8 to 0.10 extensions accept only `2`, so they refuse a
0.11 server with their `server_too_old`.

Tests: `codec/vectors/mod.rs` `the_claim_frame_is_frozen`, `the_commit_frame_is_frozen`,
`the_part_frames_are_frozen`, `the_log_segment_is_frozen`,
`a_fresh_builds_segment_header_is_frozen`, `the_snapshot_header_is_frozen`,
`the_page_one_marker_is_frozen`, `the_salvage_manifest_is_frozen`,
`the_import_activation_is_frozen`, `the_host_directory_record_is_frozen`, `codec/digest.rs`
`snapshot_digests_equal_the_servers`, `producer_digests_are_blake3`.

## Verification

What CI runs (`.github/workflows/integration.yml`):

- `sqlite-vfs`, `sqlite-vfs-macos`: clippy and the extension's unit tests, the proptests among them
  (the replicator world, the log codec, materialization, the frame and snapshot codecs), on Linux
  and macOS.
- `sqlite-vfs-host-musl`: `ursula-vfs-host` built for x64 Linux with musl and linked statically,
  as a host ships it, and run once.
- `sqlite-vfs-e2e`: every e2e suite against the model server (`E2E_SERVER=model`) on Node 22.19.0
  and Node 22, and Pi conformance on the oldest supported Pi.
- `sqlite-vfs-e2e-ursula`: every suite against a real server, one node with an in-memory cold store (with `test/history-real.e2e.ts`) and three nodes behind the gateway with no cold tier (with `test/failover.e2e.ts`). Both report, without asserting, the open time of a fresh build, a stale catch-up and a stale rebuild from `test/stale.e2e.ts` in the job summary, at 1 MiB and, on one node, 16 MiB. That suite prints its seed, and `STALE_SEED` replays it.
- `sqlite-vfs-e2e-ursula-parts`: `test/parts.e2e.ts`, `test/import.e2e.ts` and the snapshot-parts cases of `test/core.e2e.ts` (`-t "in parts"`) against three nodes behind a gateway started with `--max-request-body-bytes 4194304` (`E2E_GATEWAY_MAX_REQUEST_BYTES`), with the CI S3 as the cold store (`.github/actions/ci-s3`: AWS S3 on same-repo pull requests and main, MinIO otherwise). The gateway lowers HEAD's `Ursula-Max-Request-Bytes` to 4 MiB and passes the node's `Ursula-Max-Snapshot-Bytes` through, and the test setup refuses to start unless HEAD shows both, so every snapshot above 4 MiB goes up in parts through the gateway and is read back in 4 MiB ranges. 4 MiB is also the client's batch cap, so PART runs keep their sizes. Of the sqlite-vfs e2e jobs, only it has `id-token: write`, and it installs the package with `npm ci --ignore-scripts`. A suite that needs three nodes, the gateway and a cold store joins this job through its `SUITES` list.
- `sqlite-vfs-perf`: the commit benchmark ([Performance](#performance)). It fails when the VFS's
  commit p50 exceeds B1's by more than 0.5 ms. The `synced()` timings are reported only.
- `memory-soak.yml` job `vfs-outage`: `test/outage.e2e.ts` through a 600 s outage, daily, on pushes
  to main that change `clients/sqlite-vfs` or `clients/sqlite-ursula`, and by hand. It does not run
  on pull requests. The 60 s outage in `sqlite-vfs-e2e` does.

Crash points use `URSULA_VFS_ABORT_AT=<step>[:<n>]`, which SIGKILLs the process at the named step
([Crashes](#crashes)).

## Limits

- **Commit size is bounded by the commit limit.** A commit holds at most
  `URSULA_VFS_MAX_COMMIT_BYTES` of pages (128 MiB by default, at most 1 GiB), or it fails as
  `too_large`. The writer holds about the pages plus twice the compressed record, so about three
  times the limit for pages that do not compress. A reader joining a PART run holds about four to
  five times the compressed record. Readers apply every commit a frame holds, whatever their own
  limit, so a host needs memory for the largest limit any owner of the database runs with.
- **Two history events and a crash.** Two history events with no write between them, plus a crash,
  a clean close or a reset between a re-key's log roll and its claim, salvage a generation that
  lost nothing.
- **Offline commits after a history event.** Commits logged before an open and after its latest
  ACK are salvaged after any history event, since they may have been sent and acknowledged. A
  clean close writes an exact ACK, and commits made while provisional still replicate.
- **Idle expiry.** An owner idle past producer expiry whose own claim frame the pre-check cannot
  see resets `stale`.
- **No resume after a memory restore.** A mismatch between memory and disk goes unseen, and each
  restored copy needs its own disk ([Clones](#clones)). A host that snapshots the process finds
  the directories whose roots may hold commits in the host directory ([Hosts](#hosts)).
- **Copies of a live cache.** A copy of a cache directory opened offline while its original is
  bound and idle continues the original's claim once the stream answers, goes live and fences the
  original, though the copy's open was provisional. Every commit the original acknowledged stays
  in the stream, and its next commit or `synced()` resets it `fenced`. A host that copies a live
  cache stops the original first, or opens the copy while the stream answers, so its takeover is
  explicit.
- **A shared activation.** Two opens under one activation, a constant `activation` or a memory
  clone and its original, take each other's claims for their own. Two that compute one epoch both
  bind on one claim. Until their first divergent commit both are live, and only the server's
  digest per producer key tells them apart: the second append under that key gets
  `key-conflict` and that open resets `fenced`. After a restore starts the epochs again, a
  background claim of one can also apply above the claim an explicit open of the other installed,
  and the explicit owner resets `fenced` at its next commit.
- **Transport.** HTTP/1.1 only, so a retry resends the whole body. ureq tries addresses in turn, so
  an IPv6 path that connects and then stalls fails every attempt (an IPv4-only hostname works
  around it). Each database uses two HTTP worker threads and a snapshot thread.
- **One token per process,** and no separate read origin.
- **Snapshots** are not published above 1 GiB compressed, or above 32 MiB without a cold store.
- **Local durability.** A power loss drops unacknowledged commits since the last log fsync.
  Network filesystems, filesystems that ignore fsync, and a fork after open are unsupported.
- **Host alias on a fresh host.** With no local cluster id to compare, a misrouted alias opens the
  other deployment's stream.
- **Import.** Opens of the stream fail with `stream_state` while an import runs, and a failed
  import leaves a stream that refuses opens until it is deleted or recreated.
- **Platforms.** Linux x64 and arm64 with glibc 2.28 or later, Linux x64 with musl, and macOS on
  Apple silicon. Node.js 22.19 or later.

## Performance

The commit benchmark (`bench/commit.ts`) runs Pi-shaped transactions (an entry row of about 4.5 KB
and a head row) in one process, alternating rounds of two arms: B1, plain node:sqlite on a local
file in WAL mode with `synchronous=NORMAL`, and the VFS through `open()`, replicating in the
background. A last phase times `await db.synced()` after one commit at a time. CI fails the run
when the VFS's commit p50 exceeds B1's by more than 0.5 ms.

5,000 commits per arm against the model server on the same host (darwin-arm64, Node 22.22.1):

| arm | p50 ms | p90 ms | p99 ms |
| --- | --- | --- | --- |
| B1, plain node:sqlite | 0.016 | 0.020 | 0.048 |
| VFS commit | 0.053 | 0.062 | 0.303 |
| VFS `synced()` after a commit | 0.124 | 0.137 | 0.186 |

The `synced()` row measures the VFS's own path to a local model server. Against a real deployment
it adds one append round trip. B3, the raw append floor of a stream, is measured by
`bench/raw.ts`.
