# SQLite on Ursula: the replicating VFS

Status: implemented. M1 (#324): the VFS core. M3: snapshots, retention, attach from a snapshot.
The local files are a non-durable cache (§6): durability is the stream's acknowledgement alone.

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
  last frame the file reflects, the owner's epoch, the kernel boot id it was written in, the
  stream's path, the db file's inode and its claim on the local WAL (generation and last commit
  frame, §6), replaced atomically against a process crash (temp file, rename; no fsync).
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
  WAL write lock is released) the sidecar advances. Nothing is fsynced, except the db file before
  a commit that starts a new WAL generation, once per WAL wrap (§6).
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
2. Reads the sidecar, before anything opens the file through SQLite. A file with content and no
   sidecar was never attached and is refused (it may be a database whose pages were never in the
   stream). A sidecar for another stream path is refused. The local files are trusted when the
   sidecar was written in this boot for this db file (with or without the recovery marker, step 3)
   and the local WAL holds what the sidecar claims of it (§6); anything else (another or unknown
   boot id, an older version's sidecar, a torn one, a replaced db file, a WAL behind its sidecar)
   means the local files are discarded (§6) and the attach proceeds as on a fresh host, unless a
   read at the sidecar's offset shows the stream lost acknowledged data (§6, wrong stream).
3. Recovery (only when it rewrites pages): a private `unix` connection runs
   `wal_checkpoint(TRUNCATE)` and must see every frame checkpointed and be the last connection
   (the WAL is deleted on its close); otherwise attach fails rather than rewriting pages under
   another connection's cache. Recovery then fsyncs the db file and, before its first page write,
   replaces the sidecar with the recovery marker (`<restart offset> <epoch> boot=… file=…
   recovering`). An attach in the same boot that finds it does not open the file through SQLite,
   because the file may be inconsistent: it checks that no other process holds the file, deletes
   `-wal`/`-shm` (empty, since recovery starts only after a complete checkpoint) and replays from
   the recorded offset.
4. `HEAD` the stream. If its latest snapshot is ahead of the file (a fresh host, or a file left
   below the retention), installs it (§4.3). Then replays frames to the tail.
5. Claims: appends a claim at (epoch = highest seen + 1, seq 0) and reads the frame back. A 2xx
   alone proves nothing (two owners claiming the same epoch both get one, the second as a
   duplicate), so the claim counts only if the bytes at the answered offset are ours (the nonce
   makes them unique). Lost: epoch + 1. 403: the server's epoch + 1.
6. Replays up to the claim, fsyncs the db file if attach wrote it, writes the sidecar, and
   attaches.

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
size, checksum), recovers the old file as in §3 (writing the recovery marker into the sidecar
first), writes the image to a temp file and renames it over the db file, then replays the tail. A
crash after the rename leaves the marked sidecar at the old offset naming the replaced file, so
the next attach discards the files and installs the snapshot again. A tail read that hits `410`
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
  attaches after it sees it, through replay or a snapshot that includes it. Caveat: the owner's
  later commits are page images read from its local files, so local files that lose a write
  underneath a running process (the write-back I/O error of §6) can overwrite acknowledged changes
  in the stream. Local files rolled back while no process had them attached (a restored disk
  image) are caught by attach (§6).
- Nothing the stream did not acknowledge becomes visible: no frame of a transaction reaches the
  local WAL before its append is acknowledged, and a fenced or failed commit leaves no trace
  locally or remotely (the server deduplicates retries). An append the server applied but whose
  answer the client never saw is in the stream, and appears after the next attach.
- Fencing: after a claim at epoch `e` is verified, appends below `e` fail. Snapshots carry the
  highest epoch, so this survives retention trimming the claims.
- A snapshot reflects exactly the stream at its offset; retention advances only past a snapshot
  that has been read back.

## 6. Failure model

Durability of a commit is Ursula's acknowledgement, nothing else. The local db file, `-wal`, `-shm`
and sidecar are a cache of the stream. Nothing on the per-commit path is fsynced: `xSync` of an
attached database is a no-op, the extension's private connections run `synchronous=OFF`, and the
sidecar is replaced by temp file + rename without fsync. So the application's `PRAGMA synchronous`
level affects neither correctness nor commit latency (leave it as the application sets it).

But the files must never be *trusted* when they do not hold the state the sidecar names: with page
images, the owner's next commits are built on whatever pages the files hold, so a stale page would
be written into the stream. Trust is therefore verified against the files, not inferred:

- **The claim.** At every commit the sidecar records `wal=<salts>:<frame>`: the local WAL's
  generation (the salts of its header, which SQLite changes at every restart) and the frame number
  of the acknowledged commit frame. `:0` says the WAL holds no commit and the db file alone holds
  the state.
- **The check.** Attach trusts the files only when the boot id and the db file's inode match and
  the WAL, read the way SQLite's recovery reads it (header checksum, then frames in order while
  their salts match and the cumulative checksum holds), is the claimed generation with valid
  frames up to at least the claimed one; for `:0`, it must hold no commit frame. The recovery
  marker makes no claim on the WAL (that attach deletes it).
- **Three fsyncs of the db file**, none per commit, all through SQLite's own handle except at
  attach (when no connection is open): (1) before a commit that starts a new WAL generation is
  appended (once per WAL wrap; SQLite starts one only when every frame of the previous one is in
  the db file); (2) before an attached connection truncates the WAL, and when it truncates it to
  nothing (only after a complete checkpoint: `wal_checkpoint(TRUNCATE)`, a close with
  `journal_size_limit`) the sidecar switches to `:0` before the truncate; (3) at attach, after the
  local checkpoint and before the recovery marker, and before the final sidecar whenever attach
  wrote the db file (that sidecar claims `:0`). A failed fsync poisons the database (or fails the
  attach).
- The attached connections keep the WAL past the last close (`SQLITE_FCNTL_PERSIST_WAL`:
  checkpointed, not deleted), and so does the snapshot thread's, so a clean shutdown keeps the
  claim checkable.

Why this holds for any crash-consistent image of the files (every write that reached the disk
before an fsync returned is in it; any later one may be missing or torn): take claim `(G, f)` at
offset `X` and an image whose WAL recovers generation `G` with at least `f` valid frames. The
image shows `G`'s header, so it holds the db file as of fsync (1), i.e. every page as of `G`'s
start. A page in the recovered frames has its state at `X` or later (every commit up to `X` since
`G` started is a frame up to `f`). Any other page is unchanged since `G` started up to `X`, so the
db file has it, unless a checkpoint wrote it from a frame past `f` (a commit after `X`). For `:0`
the db file's state at `X` is on disk (fsync 2 or 3) and every later write to it comes from a
commit after `X`; a WAL with commit frames is rejected (it may be one whose deletion before the
fsync never reached the disk). Either way every page that differs from the state at `X` belongs to
a commit after `X`, and attach replays from `X` (final page images, so replay is idempotent).
Every other image is rejected, discarded and rebuilt.

What attach does in each case:

- **Process crash, same boot** (SIGKILL, OOM kill, abort, a pod rescheduled to the same node with a
  local volume, a container restart with runtimes that show the host's boot id: Docker,
  containerd, CRI-O): every completed `write()` is in the page cache, so the files are exactly
  what this host wrote and the claim holds (the sidecar is written only after the WAL writes
  return). A killed write leaves a prefix; SQLite's salted, cumulative WAL checksums stop recovery
  at the last whole commit, `-shm` is rebuilt, checkpoints are redone from the WAL. Attach trusts
  the files and replays from the sidecar's offset (fast). A crash in the middle of a WAL truncate,
  or of attach's local checkpoint before the recovery marker, can leave a claim the files do not
  meet; that only costs a rebuild. Covered: SIGKILL before and after the ack, the cache spill with
  in-place checksum rewrites, a failed local write after the ack, a crash mid-recovery (the
  recovery marker).
- **Same boot, files restored from a crash-consistent image**: a block-level snapshot of the volume
  (EBS, PD or Azure disk snapshots, a CSI VolumeSnapshot or PVC clone) restored or cloned onto a
  host that has not rebooted since, or a block volume force-detached and reattached. Boot id and
  inode survive a block copy, so the WAL claim decides: an image whose WAL holds the claimed
  frames (or more) is used; one whose sidecar ran ahead of its WAL (typical: the sidecar's rename
  reaches the disk before the WAL's in-place writes) or names another generation is discarded and
  rebuilt.
- **Reboot, power loss, kexec, a volume moved to another host**: the kernel boot id differs (Linux
  `/proc/sys/kernel/random/boot_id`, macOS `kern.bootsessionuuid`; unknown never matches), and a
  power loss may have left any prefix of any unsynced write in any file. Attach discards the local
  files and rebuilds from the latest snapshot and the tail. A new random boot id cannot be on disk
  from before it was generated, so no torn sidecar passes the check. Discarding runs before
  anything opens the file through SQLite (a torn file could fail any checkpoint), refuses while
  another process has the file open, and removes the db file and a leftover snapshot temp file
  (never the held lock file); every attach then removes `-journal`, and the fresh path `-wal` and
  `-shm`, and rewrites the sidecar last, so a crash midway discards again. The first attach after
  upgrading from a version without the WAL claim rebuilds once; older versions refuse this one's
  sidecar, so after a downgrade delete `<db>` (it is rebuilt from the stream).
- **Container runtimes with their own boot id**: LXC/LXD/Incus and systemd-nspawn bind-mount a new
  random boot id at every container start, and gVisor generates one per procfs instance, so a
  container restart there rebuilds; sandboxes with their own kernel (Kata, Firecracker, WSL2,
  Docker Desktop) rebuild on every restart of the sandbox or VM. Safe, only slower.
- **Cost of a rebuild**: one snapshot GET (the database, held in memory twice while it is decoded;
  up to 1 GiB compressed) plus the tail since it, at most about twice `max(database size,
  URSULA_VFS_SNAPSHOT_MIN_BYTES)` while snapshots keep up. Every reboot pays it, clean ones
  included, so a fleet-wide rolling reboot is a burst of snapshot reads. If snapshots cannot be
  published (a body over the gateway's 32 MiB, a reader pinning WAL frames), a rebuild replays
  everything since the last published snapshot (the whole log if none was ever published):
  snapshot health is an availability dependency (watch the log since the latest snapshot).
- **Wrong stream**: the sidecar names the stream's path; attaching the file to another stream is
  refused, and so is attaching it to its stream after that was deleted and recreated shorter: a
  read at the sidecar's offset (the replay start for trusted files, a check before discarding
  otherwise) is beyond the stream's end. Both hold after a reboot too, and the local files are
  kept. A stream recreated at the same path and already grown past the file's offset is not
  detected.
- A rollback journal next to an attached file can only be left by a crash while attach switched
  an empty file to WAL; attach deletes it in every case (rolling it back would truncate the file
  under the pages attach writes next).
- Network partition or slow server: commits block up to the retry budget, then poison.
- Two owners: the later claim wins; the earlier one's next commit fails cleanly
  (`UrsulaReplicationError` with `fenced: true` through the Pi helper).
- A snapshot that cannot be taken (a long reader pins WAL frames) or published (body too large):
  the log grows, nothing is lost; retention simply does not advance.

Residual risk, a disk write-back I/O error underneath a running process (EIO from the device, a
thin-provisioned volume out of space). The WAL is never fsynced and the db file only at WAL
restarts, truncates and attach, so nothing reports such an error when it happens; once the dirty
page is evicted, SQLite reads the old block back and the owner's next commits append page images
built on it, losing acknowledged changes from the stream itself, not only from the cache. An error
writing back the db file is reported (Linux reports it to an fsync on any descriptor opened
before it) by the next fsync of the db file, at the latest when the WAL wraps, which poisons the
database; one writing back a WAL frame is never reported. Between the error and that fsync the
stream can already hold the damage.

Unsupported, because they break the model above: a runtime that fakes a fixed boot id; a volume
swapped or rolled back underneath a running process; edits to the files outside the extension (a
process that never loaded it, copying a backup over the db in place; a db file replaced by rename
is detected and discarded); a file-level copy of the files taken while the process runs (`cp`,
`rsync`, a file-level backup: each file is copied at another moment, so the copy is not one
crash-consistent image, whereas a block-level snapshot of the volume is); and network or FUSE
filesystems for the local files (NFS/EFS, SMB, 9p, virtiofs: no page-cache coherence or reliable
POSIX locks, and a second host's attach would discard files the first is using). To force a
rebuild, delete `<db>`.

## 7. Limits

- 4 KiB pages; WAL mode only; `locking_mode=EXCLUSIVE` unsupported.
- One owner process per stream at a time; connections in other processes are not replicated (and
  block recovery).
- The local files must be on a local filesystem (§6).
- Commit frames are at most the server's request limit (32 MiB), about 8000 changed pages per
  transaction.
- Snapshots hold the database image in memory (twice, briefly: raw and compressed) and are capped
  by the server at 1 GiB compressed (32 MiB inline or through the gateway).
- Producer expiry after 7 idle days; the owner reclaims only if nobody wrote meanwhile.
- Plain HTTP only.

## 8. Tests

- Units (`cargo test`): frame and snapshot encodings (whole frames only, damage refused).
- Units also cover the sidecar: trusted only in this boot for this db file (the recovery marker
  included) and with its WAL claim met (a claim on frames the WAL does not hold, or no claim, is
  not); legacy, other-boot and torn sidecars are not, nothing is when the current boot id is
  unknown, and a missing sidecar is an error. CI runs them on macOS too (its boot id).
- e2e against a real node (`clients/sqlite-ursula`): transparency (any schema, byte-identical
  rebuild), the crash matrix (same-boot re-attaches resume from the sidecar or the recovery
  marker, without a snapshot), fencing, recovery exclusion, the local cache (a simulated reboot
  with a rolled-back db file and sidecar, below retention, a torn first sector and a cut WAL
  rebuilds byte-identical from snapshot + tail; the same boot reuses the files with no snapshot
  and no replay from scratch, also for a file rebuilt from a snapshot; a same-boot disk image
  whose WAL is behind its sidecar is rebuilt and the stream stays intact, one whose WAL is ahead is
  trusted; discarding is refused while another process has the file open; a replaced db file is
  rebuilt; a file without a sidecar, another stream and a recreated stream are refused, the last
  also after a reboot), snapshots and retention (a ~160 MB run; CI also runs it without a cold
  tier under the default hot limit; fresh and lagging hosts rebuild byte-identical from snapshot +
  tail; the takeover after the trim fences the old owner), Pi conformance in three modes, and a
  benchmark (sanity numbers only).
- The same Pi conformance, snapshot and benchmark suites on 3 nodes + gateway + MinIO at feature
  level 5 (the snapshot run's ~3.5 MB bodies go to the cold tier), with a 64 KiB snapshot minimum
  so the benchmark's Pi workload snapshots and trims at its database size.
