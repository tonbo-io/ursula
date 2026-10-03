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

One run, 2026-10-03. EKS 1.33 in us-east-1: three `m6i.xlarge` Ursula nodes, one per AZ (the chart's
`examples/production-eks.yaml` shape: 256 groups, 4 cores, 8 GiB limit), three gateways, real S3
for the cold tier and snapshots with Ursula's default S3 settings (`server_side_encryption =
"aes256"`), feature level 5. Server image: main `05132e0`. Extension: `05132e0` for the memory-WAL
cells, `6ba4e60` for the disk-WAL cells (the difference is 429/503 retry and recovery, not the
commit path). Clients: `m6i.2xlarge` pods (node 22) in us-east-1a, one process per database,
through the gateway Service. The chart deploys the gateway without a quota policy, so there was no
rate limit and no client saw a 429. Gateway `maxRequestBodyBytes` was raised to 1 GiB for the 1 GB
snapshot; everything else is the chart default.

Workload: Pi Durable's `SqliteStorage` on the VFS via `openUrsulaPiStorage`, a real `Harness` with
a faux model, turns of text, text, tool (5.0 Pi commits per turn). Latency is `Storage.commit`
wall time after a 30 s warm-up; each cell runs 10.5 min. Agent pace: one turn per 2 s per database.
Baselines on the same client node type:

- B1: the same Pi workload on Pi's own `openNodeSqliteStorage` on the pod's disk, no extension
  (WAL, `synchronous=NORMAL`).
- B2: the same with `PRAGMA synchronous=FULL` (the WAL synced on every commit).
- B3: raw appends, no SQLite: closed-loop writers, one stream each, 5 to 7 KiB random bodies (the
  VFS's compressed frames are 4.5 to 7.4 KB) with producer headers, through the gateway or straight
  to the stream's leader (node Service, `307` followed and the leader kept). 120 s cells (memory
  WAL) and 75 s cells (disk WAL), 15 s warm-up.

Pi commit latency, ms, p50 / p99 (p99.9 / max where they matter), and Pi commits/s flat out:

| cell | B1 local, NORMAL | B2 local, FULL | VFS, memory WAL | VFS, disk WAL |
| --- | --- | --- | --- | --- |
| 1 db, agent pace | 0.11 / 7.6 | 1.8 / 4.6 | 9.7 / 12.8 | 16.5 / 20.0 |
| 1 db, flat out | 0.16 / 8.9; 115/s | 2.2 / 4.9; 100/s | 9.4 / 12.8 (42 / 147); 60/s | 15.2 / 18.7 (65 / 124); 46/s |
| 16 dbs, agent pace | 0.11 / 6.5 | 1.6 / 4.8 | not run | 16.0 / 182 (3,043 / 8,960) [1] |
| 16 dbs, flat out | 0.20 / 13.1; 651/s | 2.8 / 19.1; 652/s | 13.0 / 31.8 (50 / 132); 565/s | 21.7 / 37.1 (110 / 363); 499/s |
| 128 dbs, flat out [2] | | | 49.9 / 120 (5,041 / 10,097); 1,900/s | 46.3 / 2,123 (4,826 / 26,220); 841/s |

[1] Run on the disk cluster after the 128-database and failover cells; see the issues below.
[2] Two client nodes, 64 databases each, both saturated (load average 31 to 38 on 8 vCPU), so this
row measures the clients as much as Ursula.

Raw append floor (B3), ms p50 / p99 (p99.9), and appends/s:

| writers | memory WAL, gateway | memory WAL, leader | disk WAL, gateway | disk WAL, leader |
| --- | --- | --- | --- | --- |
| 1 | 1.15 / 1.75 (7.2); 796/s | 1.12 / 1.55 (8.8); 833/s | 8.2 / 8.8 (16); 121/s | 8.7 / 9.2 (20); 114/s |
| 16 | 2.4 / 7.4 (18); 6,274/s | 1.9 / 7.6 (20); 6,957/s | 16.9 / 27.3 (93); 916/s | 15.1 / 28.2 (247); 1,000/s |

The disk-WAL row is from a freshly deployed cluster. On the disk cluster that had just run the
128-database cell, the same 16-writer cells had p99 330 ms (p99.9 2.0 and 2.4 s), and the
leader-direct writers got 763 `503`s.

Where one database's commit goes (agent pace, p50, ms):

| component | memory WAL | disk WAL |
| --- | --- | --- |
| Pi + SQLite, no fsync (B1) | 0.1 | 0.1 |
| local WAL fsync (B2 minus B1) | 1.7 | 1.7 |
| VFS commit hook: frame build and local WAL write | 0.2 | 0.1 |
| VFS commit hook: the append request | 4.0 | 10.9 |
| sidecar replace: temp file, fsync, rename, directory fsync (measured alone) | 3.0 | 3.0 |
| sum | 9.0 | 15.8 |
| measured Pi commit | 9.7 | 16.5 |
| for reference: raw append, same frame sizes (B3, gateway, 1 writer) | 1.15 | 8.2 |

The VFS's append request was 1.4 to 2.8 ms above the raw floor: 4.0 and 10.9 ms at agent pace, and
3.8 and 9.6 ms for one database flat out. It reuses its connection (one `TIME_WAIT` socket in 15 s of commits), so connection
setup is not the cause; each of these is a single stream, so the leader's placement differs between
them, and a same-stream comparison is still to do.

**128 databases.** Memory WAL: 1,900 commits/s, no database poisoned, 1 retried append in 1.2
million. Node RSS rose from 0.9 to 2.6 GB and fell back to 1.6 to 1.8 GB after the load (8 GiB
limit). No AppendStream backpressure rejections. Disk WAL: 841 commits/s, RSS peak 2.8 GB, 367
AppendStream backpressure rejections on two of the nodes, 55 appends retried (up to 13 attempts)
and acknowledged, and one of the 128 databases poisoned (below).

**Stream and snapshots.** 10.9 pages per commit (10.6 at agent pace). zstd ratio 6.1 to 7.3 flat
out, 9.8 at agent pace. 4.5 KB per commit at agent pace and 6.0 to 7.4 KB flat out, which is 22
KB and 30 to 37 KB per turn. Snapshot bodies were 0.09 to 0.5 MB for 0.9 to 5.2 MB databases,
taking 40 to 78 ms p50 (180 to 235 ms at 128 databases). The retained log (tail minus retention)
stayed at or below 16.8 MB per database in every cell.

**Back-to-back large transactions** (5 MB table, 2,000-row updates, 1.4 MB frames, no pause, 300
s). Memory WAL: 352 snapshots. Disk WAL: 336. The first came at 9.7 MB of log, while 3.4 and 3.2 GB
were written. The retained log never exceeded 20.7 MB (sampled every 10 s) and ended at 15.2 and
11.0 MB. Commit p50 was 118 and 124 ms.

**Cold start** (fresh host: snapshot install plus tail replay; best of three, first attach in
parentheses). The 27.8 MB and 278 MB snapshots were published to S3 with the default encryption
setting:

| database | snapshot (publish time, memory / disk) | memory WAL | disk WAL |
| --- | --- | --- | --- |
| 10 MB | 2.8 MB (0.2 / 0.2 s) | 107 ms (205) | 110 ms (222) |
| 100 MB | 27.8 MB (2.7 / 1.8 s) | 0.91 s (2.17) | 0.79 s (1.80) |
| 1 GB | 278 MB (18.7 / 15.7 s) | 16.9 s (27.1) | 16.1 s (25.0) |

The tail after the snapshot was 2.8 MB, 21 to 34 MB and 133 to 169 MB: what the writer committed
while the snapshot was being taken.

**Failover** (16 databases flat out; `kubectl delete --force` at 120 s of the node leading the
most groups, 86 of 256). Memory WAL: 5 databases stalled 19.3 to 22.3 s, two for 1.3 and 2.3 s, and
the rest under 0.15 s. Disk WAL: 7 stalled 8.3 to 10.1 s and the rest 1.7 to 4.0 s. No commit
failed. Afterwards every owner's file and a fresh rebuild from its stream were identical row for
row, with `integrity_check` ok (16 of 16 on both WALs).

**S3.** Retention frees cold chunks. One minute after the memory-WAL failover cell, its 16 streams
still held 2 or 3 chunks wholly below retention minus 64 MiB. Ten minutes later, and in a later
check of the disk cluster, no chunk object of any of 167 and 218 streams lay below that line. A
279 MB stream retained from 262 MB held 85 MB in S3 (its first chunk starts at 193 MB); a 211 MB
stream retained from 202 MB held 17 MB.

Requests per minute (CloudWatch, whole bucket):

| load | PUT | GET |
| --- | --- | --- |
| 1 database at agent pace | 0 to 11 | under 30 |
| 1 database flat out | 6 to 37 | 31 to 81 |
| 16 databases flat out | 110 to 350 | 340 to 870 |
| 128 databases, memory WAL (1,900 commits/s) | 1,060 to 1,540 | 3,100 to 4,150 |
| 128 databases, disk WAL (841 commits/s) | 500 to 600 | 1,550 to 1,920 |

A freshly deployed cluster made 1,400 to 1,550 PUTs in its first 2 to 3 minutes.

**Open issues seen in this run.**

- Disk WAL, 128 databases: one database was poisoned. Its append hit the VFS's 10 s per-request
  timeout three times, using up the 30 s budget (`append: timeout: global (outcome unknown after
  3 attempts); database poisoned`). Acknowledged commits reached 26.2 s. Each node logged 3,700 to
  5,300 `rebuilding channel ... after 8 consecutive AppendStream failures` warnings during the
  11-minute cell. Outside it there were 64 to 160 at deploy, about 180 at the failover kill, and
  bursts of 24 to 119 later.
- Disk WAL after that load: 16 databases at agent pace (40 commits/s in total) had p99 182 ms,
  p99.9 3.0 s and max 9.0 s, and raw 16-writer appends p99 330 ms with 763 `503`s. On a fresh disk
  cluster the raw p99 was 27 to 28 ms. The nodes were reclaiming 100 to 137 MB WAL journals online
  in this period.
- Memory WAL, 128 databases: the p99.9 sits at 5.0 s in both client halves, with a max of 10.1 s.
- Retention does not reclaim external payload objects (appends of 1 MiB or more). The two
  back-to-back streams still held 3.41 and 3.25 GB in S3 16 and 25 minutes after the writes ended,
  although retention had passed all but their last 15 and 11 MB.
- A snapshot read-back `GET` through the gateway was answered `503` ("read_snapshot has to forward
  request to leader") instead of being forwarded; the VFS retried it.
