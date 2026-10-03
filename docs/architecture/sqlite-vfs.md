# SQLite on Ursula: the replicating VFS

Status: implemented. M1 (#324): the VFS core. M3: snapshots, retention, attach from a snapshot.

Scope: run an unmodified SQLite application (Pi Durable's official node `SqliteStorage` is the
reference user) with an Ursula stream as the source of truth. Code: `clients/sqlite-vfs` (the
loadable extension, Rust, a standalone workspace) and `clients/sqlite-ursula` (TypeScript: loader,
`attach`, the Pi helper `openUrsulaPiStorage`, and the e2e suites). User docs:
`docs/web/src/content/docs/pages/examples/sqlite-vfs.mdx`.

## 1. Shape

The extension registers a shim VFS named `ursula` over SQLite's `unix` VFS and makes it the
default. `SELECT ursula_attach(path, stream_url)` binds one database file to one stream. From then
on every connection that opens the file, in this process, is replicated without knowing it: each
WAL commit is appended to the stream *before* any of the transaction's frames reach the local
`-wal`. The local db file and WAL are a cache of the stream.

- One stream per database, `application/octet-stream`. Each append is one self-delimiting frame:
  `"USQ1" | u32 len | u32 crc32c | zstd(record)`. A record is a **commit** (the database size after
  it and the transaction's final page images) or a **claim** (a producer epoch and a 128-bit
  random nonce).
- The local file's position in the stream is the sidecar `<db>-ursula`: the byte offset after the
  last frame the file reflects, and the owner's epoch, replaced atomically (temp file, fsync,
  rename, directory fsync).
- One owner per file per host: attach takes `flock` on `<db>-ursula.lock` for the process
  lifetime, and refuses while any connection to the file is open.

## 2. Writes

WAL writes of an attached database go to a per-transaction overlay (reads and the file size see
it). The write of the commit frame's page data (the frame whose header has a non-zero "db size
after commit") is the commit point: the transaction's final page images become one commit frame,
appended with the idempotent producer (`Producer-Id` per database, `Producer-Epoch` per owner,
`Producer-Seq` per append).

- **Acknowledged**: the overlay goes to the local WAL, later writes of the transaction (checksum
  rewrites of spilled frames, padding) go straight to it, and when the write transaction ends (the
  WAL write lock is released) the WAL is synced and only then the sidecar advances.
- **Outcome unknown** (timeout, connection loss, 5xx): retried with the same sequence until the
  server answers, for up to `URSULA_VFS_RETRY_MS` (30 s). The server deduplicates.
- **403** (a newer epoch claimed the stream), a definite rejection, or an exhausted budget: the
  write fails with `SQLITE_IOERR_WRITE`, SQLite rolls the transaction back, nothing of it reaches
  the local WAL, and the database is poisoned until it is re-attached.
- The overlay belongs to the write transaction: it is cleared whenever the WAL write lock is taken
  or released, so a rolled-back transaction's spilled frames never shadow a later one's.
- Any write to the main db file outside a checkpoint (a rollback journal, `journal_mode=MEMORY`) is
  refused: it would bypass replication.

## 3. Attach and fencing

`ursula_attach`:

1. Takes the host lock; refuses if a connection to the file is open in this process.
2. Reads the sidecar. A file with content and no sidecar was never attached and is refused.
3. Recovery (only when it rewrites pages): a private `unix` connection runs
   `wal_checkpoint(TRUNCATE)` and must see every frame checkpointed and be the last connection
   (the WAL is deleted on its close); otherwise attach fails rather than rewriting pages under
   another connection's cache.
4. `HEAD` the stream. If its latest snapshot is ahead of the file (a fresh host, or a file left
   below the retention), installs it (§4.3). Then replays frames to the tail.
5. Claims: appends a claim at (epoch = highest seen + 1, seq 0) and reads the frame back. A 2xx
   alone proves nothing (two owners claiming the same epoch both get one, the second as a
   duplicate), so the claim counts only if the bytes at the answered offset are ours (the nonce
   makes them unique). Lost: epoch + 1. 403: the server's epoch + 1.
6. Replays up to the claim, writes the sidecar, and attaches.

The new epoch fences every earlier owner at the server: their next append gets 403. Replay applies
page images per read batch (last image per page, truncate to the batch's smallest size first), so
replaying from an older offset than the file reflects is idempotent.

Producer expiry: the server forgets a producer idle for 7 days. The owner's next append gets 409
expecting seq 0; it takes the stream back only if the stream still ends at its own offset and its
new claim (epoch + 1) lands exactly there, else it is fenced. Reads in recovery use
`consistency=leader` (a follower may lag an acknowledged append).

## 4. Snapshots and retention (M3)

Without them the log, and so attach time and stored bytes, grows forever; without a cold tier
the default per-group hot limit (64 MiB) trips after about 68 MB of frames.

### 4.1 When

After a commit is published, if the log since the latest known snapshot exceeds
`max(database size, URSULA_VFS_SNAPSHOT_MIN_BYTES)` (8 MiB by default), the database's snapshot
thread is woken. Snapshotting never runs on the commit path. Stored log is therefore bounded by
about twice the threshold plus what accumulates while a snapshot is in flight (§4.4).

### 4.2 Taking one

The body must be the stream's state at a frame boundary `W`, page-identical to a replay of
`[0, W)`, so later page-image frames apply on top of it (`VACUUM INTO` and the backup API both
rewrite pages, and are not). The thread:

1. Opens a private `unix` connection on the file and runs `wal_checkpoint(PASSIVE)` once, so
   the checkpoint inside the window only covers the frames committed since.
2. Opens the *window* (under the database's mutex): waits until no acknowledged commit is
   unpublished and no other connection holds the checkpoint lock (typically the writer's own
   auto-checkpoint right after a commit), records `W` (the offset), the epoch and the page count.
   While it waits, the next commit to reach its commit point waits there too (for at most 1 s)
   until the window has opened: a writer committing back to back would otherwise leave the
   window only the gaps between its transactions and can starve the snapshot indefinitely
   (observed: no snapshot for 120 s under 1.4 MB transactions while ~1 GB of log piled up). So a
   due snapshot pins a state at most one transaction past the threshold. While the window is open
   a commit that reaches its commit point waits there (holding SQLite's write lock, which neither
   step below needs).
3. `wal_checkpoint(PASSIVE)` on the private connection, retried for up to ~100 ms while another
   connection's checkpoint keeps it busy (a passive checkpoint gets no busy handler). Unless every
   WAL frame was checkpointed (a reader pins older frames), gives up for now and retries shortly
   (backoff from 10 ms up to 1 s; server errors back off from 100 ms up to 30 s).
4. `BEGIN` and a read: the read transaction starts at `W`. Closes the window.
5. Copies pages `1..n` of the db file through the private connection's own file handle (a second
   descriptor's close would drop the process's POSIX locks). While the read transaction lasts no
   checkpoint can write newer frames into the db file (a reader at mark 0 blocks backfill; at a
   later mark it caps it) and no closing connection can checkpoint (that needs an EXCLUSIVE lock),
   so the pages are those of `W`. Ends the read transaction.

Commits wait only for steps 3 and 4 (a passive checkpoint of the few frames committed since step
1, and the start of a read transaction) and, once a snapshot is due, for the window to open
(step 2, at most one commit, bounded). A re-attach stops the thread: it checks the stop flag
between steps and retries, and each snapshot request is bounded to 120 s.

The body is `"USS1" | u64 W | u64 epoch | u32 pages | u32 crc32c(image) | zstd(image)`. The epoch
is the highest one claimed before `W`: retention may trim every claim frame, and the next owner
must still claim above it (after a producer expiry the server would accept a lower epoch, and a
zombie with a higher one could then fence the new owner).

### 4.3 Publishing, retention, and attach

1. `PUT {stream}/snapshot/{W}` (retried while the outcome is unknown; publishing is idempotent).
   409/410: a newer snapshot exists; nothing to do.
2. `GET {stream}/snapshot/{W}` until it returns exactly the published bytes.
3. Only then `PUT {stream}/retention/{P}`, where `P` is the *previous* snapshot's offset (the latest
   one known at attach, or the one before `W`).

Why one snapshot behind: a host that read the previous snapshot's offset from `HEAD` and is
fetching it, or whose file sits between `P` and `W`, still finds every frame after `P`; retention at
`W` would turn its tail read into `410` and a full re-download. The cost is up to one more
threshold of retained log. The newer snapshot has read back before any history is dropped, so the
retained stream always holds a readable snapshot at or above its start.

Attach installs a snapshot when the file is behind the latest one: it verifies the body (offset,
size, checksum), recovers the old file as in §3, writes the image to a temp file, fsyncs it,
renames it over the db file and fsyncs the directory, then replays the tail. A crash after the
rename leaves the sidecar at the old offset (a fresh file gets a `0 0` sidecar before anything is
written), so the next attach installs again or replays idempotently. A tail read that hits `410`
(retention moved under a stale `HEAD`), or a snapshot superseded between `HEAD` and `GET` (a 404,
or a body cut short when its cold object is deleted after the grace), restarts
attach from `HEAD`, up to ten times.

Any owner may publish a snapshot of its own offset, a fenced one included: its state at its offset
is a true prefix of the stream. Retention never passes the latest snapshot (the server refuses).

### 4.4 Snapshot bodies on the server

Bodies of at least `runtime.external_payload_min_size` (1 MiB) are stored in the cold tier at
feature level 5, up to 1 GiB; inline otherwise (up to 32 MiB). Through `ursulagw` a body above its
`--max-request-body-bytes` (32 MiB) is refused: the snapshot thread logs and retries with backoff,
and the log keeps growing. A superseded cold body stays readable for 5 minutes.

## 5. Guarantees

- An acknowledged commit (the SQL `COMMIT` returned) is in the stream, and every host that
  attaches after it sees it, through replay or a snapshot that includes it.
- Nothing unacknowledged becomes visible: no frame of a transaction reaches the local WAL before
  its append is acknowledged, and a fenced or failed commit leaves no trace locally or remotely
  (the server deduplicates retries).
- Fencing: after a claim at epoch `e` is verified, appends below `e` fail. Snapshots carry the
  highest epoch, so this survives retention trimming the claims.
- A snapshot reflects exactly the stream at its offset; retention advances only past a snapshot
  that has been read back.

## 6. Failure model

- Crash of the owner at any point: the sidecar never runs ahead of a synced local WAL; re-attach
  replays from it (or installs a snapshot). Covered: SIGKILL before and after the ack, the cache
  spill with in-place checksum rewrites, a failed local write after the ack.
- Network partition or slow server: commits block up to the retry budget, then poison.
- Two owners: the later claim wins; the earlier one's next commit fails cleanly
  (`UrsulaReplicationError` with `fenced: true` through the Pi helper).
- A snapshot that cannot be taken (a long reader pins WAL frames) or published (body too large):
  the log grows, nothing is lost; retention simply does not advance.

## 7. Limits

- 4 KiB pages; WAL mode only; `locking_mode=EXCLUSIVE` unsupported.
- One owner process per stream at a time; connections in other processes are not replicated (and
  block recovery).
- Commit frames are at most the server's request limit (32 MiB), about 8000 changed pages per
  transaction.
- Snapshots hold the database image in memory (twice, briefly: raw and compressed) and are capped
  by the server at 1 GiB compressed (32 MiB inline or through the gateway).
- Producer expiry after 7 idle days; the owner reclaims only if nobody wrote meanwhile.
- Plain HTTP only.

## 8. Tests

- Units (`cargo test`): frame and snapshot encodings (whole frames only, damage refused).
- e2e against a real node (`clients/sqlite-ursula`): transparency (any schema, byte-identical
  rebuild), the crash matrix, fencing, recovery exclusion, snapshots + retention (a ~160 MB run;
  CI also runs it without a cold tier under the default hot limit; fresh and lagging hosts rebuild
  byte-identical from snapshot + tail; the takeover after the trim fences the old owner), Pi
  conformance in three modes, and a benchmark (sanity numbers only).
- The same Pi conformance, snapshot and benchmark suites on 3 nodes + gateway + MinIO at feature
  level 5 (the snapshot run's ~3.5 MB bodies go to the cold tier), with a 64 KiB snapshot minimum
  so the benchmark's Pi workload snapshots and trims at its database size.

## 9. Performance

One run, 2026-10-03, main `740d910`. EKS 1.33 in us-east-1: three `m6i.xlarge` Ursula nodes, one
per AZ (chart defaults of `charts/ursula/examples/production-eks.yaml`: 256 groups, 4 cores, 8 GiB
limit), three gateways, real S3 cold tier and snapshot store, feature level 5. The client is one
`m6i.2xlarge` pod (node 22, the extension built in CI) in us-east-1a, one process per database,
writing through the gateway Service. Workload: Pi Durable's `SqliteStorage` via
`openUrsulaPiStorage`, a real `Harness` with a faux model, turns of text, text, tool (5.0 Pi
commits per turn). Latency is `Storage.commit` wall time after a 30 s warm-up; each cell ran 10.5
min. Disk: `raft.wal.backend = "disk"` on 50 GiB gp3. Deviations from the chart: gateway
`maxRequestBodyBytes` 1 GiB (for the 1 GB snapshot) and `server_side_encryption = "none"` (see
the S3 note below; the bucket's default SSE-S3 still applies).

Commit latency, ms (p50 / p99 / p99.9 / max), and throughput:

| cell | memory WAL | disk WAL |
| --- | --- | --- |
| 1 owner, 1 turn / 2 s | 7.0 / 10.9 / 23.7 / 93 | 14.6 / 18.7 / 24.0 / 26 |
| 1 owner, flat out | 7.4 / 11.1 / 47 / 117; 68 commits/s | 16.0 / 19.8 / 62 / 1619; 44 commits/s |
| 16 owners, flat out | 14.6 / 48.5 / 79 / 298; 542 commits/s | 22.7 / 49.2 / 116 / 658; 478 commits/s |
| 128 owners, flat out | 92 / 190 / 236 / 5232; 1202 commits/s | failed: Ursula OOM, see below |

The append request alone (p50): memory 1.8 ms (1 owner), 4.9 ms (16), 48 ms (128); disk 9.0 /
10.3 ms (1), 14.8 ms (16). The rest of a single owner's commit (about 5.5 ms) is local: Pi's own
work and SQLite's WAL sync. At 128 owners the client node (8 vCPU, 128 node processes) was
saturated (load average 86), so that cell measures the client as much as Ursula.

Stream and snapshots (all cells alike): 10.8 pages per commit, 6.3x zstd at speed (9.8x at
agent pace, smaller databases), 4.5 to 7.4 KB per commit, 22 KB per turn at agent pace and 35
KB per turn flat out. Snapshot bodies 0.1 to 0.55 MB for 1 to 6 MB databases, 40 to 100 ms each
(350 ms p50 at 128 owners). Retained log per database (tail minus retention) stayed between 10
and 17 MB in every cell that ran (threshold 8 MiB), while streams grew to 315 MB.

Cold start (fresh host: snapshot install + tail replay; best of three, first in parentheses):

| database | snapshot body | tail after it | memory WAL | disk WAL |
| --- | --- | --- | --- | --- |
| 10 MB | 2.8 MB | 3 to 5 MB | 104 ms (207) | 134 ms (361) |
| 100 MB | 27.8 MB | 20.5 MB | 845 ms (1580) | 864 ms (1671) |
| 1 GB | 278 MB | 120 MB | 14.8 s (20.8) | 15.6 s (22.9) |

Taking the 1 GB snapshot took 13.7 to 14.1 s; the 120 MB tail is what the writer committed
meanwhile.

Failover: 16 owners flat out, `kubectl delete --force` of the node leading the most groups at
120 s (86 of 256). The owners whose groups it led stalled 19.1 to 22.2 s (memory, 6 of 16) and
18.3 to 18.4 s (disk, 4 of 16), with 8 to 14 retried appends and no failed commit; the others
stayed under 0.9 s. After the run every owner's file and a fresh rebuild from its stream were
identical row for row, `integrity_check` ok.

S3 (CloudWatch request metrics, whole bucket): idle about 75 PUT and 130 GET per minute; 1 owner
flat out about 75 PUT / 145 GET; 16 owners about 180 PUT / 500 GET; 128 owners 350 to 450 PUT
and 600 to 1,500 GET per minute.

Found in this run:

- Cold objects written in parts (`ColdObjectWriter`, 8 MiB parts: snapshot bodies of 8 MiB and
  more) fail on AWS with the default `server_side_encryption = "aes256"`: S3 rejects the
  encryption header on `UploadPart` ("x-amz-server-side-encryption header is not supported for
  this operation"). The snapshot `PUT` returns 502, the VFS retries with backoff, and the log
  grows unbounded. MinIO accepts the header, so CI does not catch it.
- 128 owners flat out (about 1,200 appends/s, 7.6 MB/s): node RSS grew about 0.7 GB/min to 5.2
  GB with the memory WAL, and past the 8 GiB limit with the disk WAL; all three nodes were
  OOM-killed and OOM-killed again during recovery until the load stopped. Every database was
  poisoned (appends 502/503 past the 30 s budget).
- Retention did not reclaim cold chunks: 17 to 25 minutes after retention passed them, a
  stream's chunk objects from offset 0 were all still in S3 (201 MB for a 193 MB retention).
- A writer that commits back to back in large transactions (5 MB database, 1.4 MB frames, no
  await between them) published no snapshot in 120 s (970 MB of log). Yielding briefly every 20
  commits, the first snapshot came at 59 MB of log; with a 50 ms pause between transactions, at
  1.1x to 3x the threshold.
