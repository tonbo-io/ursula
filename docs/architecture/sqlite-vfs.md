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

1. Opens a private `unix` connection on the file.
2. Opens the *window* (under the database's mutex): waits until no acknowledged commit is
   unpublished, records `W` (the offset), the epoch and the page count. While the window is open a
   commit that reaches its commit point waits there (holding SQLite's write lock, which neither
   step below needs).
3. `wal_checkpoint(PASSIVE)` on the private connection. Unless every WAL frame was checkpointed (a
   reader pins older frames), gives up for now (backoff, from 100 ms up to 30 s).
4. `BEGIN` and a read: the read transaction starts at `W`. Closes the window.
5. Copies pages `1..n` of the db file through the private connection's own file handle (a second
   descriptor's close would drop the process's POSIX locks). While the read transaction lasts no
   checkpoint can write newer frames into the db file (a reader at mark 0 blocks backfill; at a
   later mark it caps it) and no closing connection can checkpoint (that needs an EXCLUSIVE lock),
   so the pages are those of `W`. Ends the read transaction.

Commits wait only for steps 3 and 4: one passive checkpoint of the WAL since the previous one
(SQLite's auto-checkpoint keeps it under about 1000 pages) and the start of a read transaction.

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
(retention moved under a stale `HEAD`), or a snapshot superseded between `HEAD` and `GET`, restarts
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
- The same Pi conformance and snapshot suites on 3 nodes + gateway + MinIO at feature level 5,
  with a 64 KiB snapshot minimum, so the reopen modes rebuild from snapshot + tail throughout.
