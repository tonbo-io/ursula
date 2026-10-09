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
- The local file's position in the stream is the sidecar `<db>-ursula`: the stream offset after the
  last frame the file reflects, the owner's epoch, the sidecar's format (`v=2`), the log since the
  latest snapshot in bytes (§4.1), the kernel boot id it was written in, the stream's path and its
  `Stream-Incarnation`, the db file's inode and its claim on the local WAL (generation and last
  commit frame, §6), replaced atomically against a process crash (temp file, rename; no fsync).
- Offsets are opaque: the VFS keeps every offset as the string the server wrote
  (`Stream-Next-Offset`, `Stream-Snapshot-Offset`, `Stream-Retained-Offset`), compares offsets only
  as strings (the protocol orders them lexicographically), and never computes one. `-1`, the
  protocol's "beginning of the stream", also stands for "none" (no snapshot, no local state). A
  read without `Stream-Next-Offset` is an error. `ursula_attach`, `ursula_status`, `ursula_stats`
  and the TypeScript API return offsets as these strings.
- One owner per file per host: attach takes `flock` on `<db>-ursula.lock` for the process
  lifetime, and refuses while any connection to the file is open.
- A db file with a sidecar is the cache of a stream: it opens through the VFS only while an attach
  of it has succeeded in this process. Before that (a process that never attached it, including a
  restarted one), and after a failed attach, opening it fails with `SQLITE_CANTOPEN` (attach it
  first); passed through to `unix`, its commits would never reach the stream. A file without a
  sidecar passes through.

## 2. Writes

WAL writes of an attached database go to a per-transaction overlay (reads and the file size see
it). The write of the commit frame's page data (the frame whose header has a non-zero "db size
after commit") is the commit point: the transaction's final page images become one commit frame,
appended with the idempotent producer (`Producer-Id` `sqlite-ursula-vfs/<Stream-Incarnation>`,
`Producer-Epoch` per owner, `Producer-Seq` per append). The id names the stream incarnation, an
opaque token compared for equality only: the owners of one incarnation fence each other by epoch.
Every request after attach's first `HEAD` (appends, reads, snapshot and retention `PUT`s, snapshot
`GET`s) also carries that incarnation as the `Stream-Incarnation` request precondition, which the
server checks atomically with the request (an append at Raft apply): a stream recreated at the
same path refuses everything of an owner of the deleted one with `412` (§6, wrong stream).

- **Acknowledged**: the overlay goes to the local WAL, later writes of the transaction (checksum
  rewrites of spilled frames, padding) go straight to it, and when the write transaction ends (the
  WAL write lock is released) the sidecar advances. Nothing is fsynced, except the db file before
  a commit that starts a new WAL generation, once per WAL wrap (§6).
- **Outcome unknown** (timeout, connection loss, 5xx): retried with the same sequence until the
  server answers, for up to `URSULA_VFS_RETRY_MS` (30 s). The server deduplicates.
- **401** (an authorizer in front of the stream, such as `ursula gateway --auth-*`, refused the
  token): retried like an unknown outcome, with the token read again (§7). On a synchronous host
  (node:sqlite) a set token cannot change meanwhile, because the committing thread is the one that
  would call `ursula_set_token`: one that expires fails the commit once the budget runs out, so
  applications refresh it before it expires. A **403 without `Producer-Epoch`** is an authorizer's
  denial and final. Both fail the write as unauthorized, never as fenced: only the server's 403
  carries `Producer-Epoch` (one classifier, `refused`, tells them apart for every request).
- **403 with `Producer-Epoch`** (a newer epoch claimed the stream), a definite rejection (any other
  4xx but 402), or an exhausted budget: the write fails with `SQLITE_IOERR_WRITE`, SQLite rolls the
  transaction back, nothing of it reaches the local WAL, and the database is poisoned until it is
  re-attached.
- **402 Payment Required** (a layer in front of the stream, such as an authorizer or a quota or
  billing service, refuses writes until something is paid or settled): nothing of the transaction
  reached the stream, so it fails alone. The write fails with `SQLITE_IOERR_AUTH` (an extended code
  SQLite reserves for extensions), SQLite rolls the transaction back and nothing of it reaches the
  local WAL, as above, but the database is not poisoned. Reads go on from the local files. The owner
  keeps its epoch, producer sequence and offset, so the next commit is sent as usual, with the
  sequence the refused append never used, and is acknowledged once the layer accepts writes again,
  without a new attach. `ursula_status` reports `payment_required`, with the layer's explanation (its
  response body, cut to 256 bytes) as `payment_reason`, until a commit is acknowledged. This holds
  only while no attempt of the append can have reached the stream. After an attempt whose outcome
  is unknown (no answer, or a 5xx) the append may be in the stream, and the next commit, sent with
  the same sequence, would be acknowledged as its duplicate, its pages never sent. So a 402 there is
  retried like an unknown outcome, until an answer settles it (the duplicate's receipt, once the
  layer forwards the append again) or the budget runs out, which poisons. Refusals in front of the
  stream (401, 402) and rate limiting (429) apply nothing, so they leave the outcome known.
- **A 2xx is accepted only with a `Stream-Next-Offset` past the owner's offset**, which becomes the
  owner's offset; another poisons. The VFS does not check where its frame landed (that would need
  offset arithmetic). A duplicate answered without one (its receipt is beyond the server's
  receipt window) fences and poisons: it is never this owner's own retry, because the owner is the
  only writer of its producer at its epoch, retries only its one newest append, and the server
  never evicts a producer's newest receipt. So another writer holds the producer at this epoch
  past this sequence.
- **Foreign writers.** Every commit carries `Stream-Seq` = the owner's epoch and producer sequence,
  each zero-padded to 20 digits. Owners claim ever higher epochs and number commits upwards within
  one, so each commit's is above every earlier commit's, retries included (a retry is answered as a
  duplicate before `Stream-Seq` is checked). The server refuses an append whose `Stream-Seq` is not
  above the stream's last one (409), so a writer outside this protocol that appended with a higher
  one since the owner's last commit fences and poisons the owner at its next commit; one with a
  lower or equal `Stream-Seq` is refused itself and never lands. One that appends without
  `Stream-Seq` goes unnoticed: its bytes then fail every replay that crosses them (the frames no
  longer decode; attach refuses) until the owner publishes a snapshot past them.
- The overlay belongs to the write transaction: it is cleared whenever the WAL write lock is taken
  or released, so a rolled-back transaction's spilled frames never shadow a later one's.
- Any write to the main db file outside a checkpoint (a rollback journal, `journal_mode=MEMORY`) is
  refused: it would bypass replication. So is a commit whose page 1 leaves WAL format: with the
  WAL kept (§6), SQLite reopens it and would commit `journal_mode=MEMORY`/`DELETE` through it,
  leaving a stream whose rebuilt copies need a rollback journal to be written.

## 3. Attach and fencing

`ursula_attach`:

1. Takes the host lock; refuses if a connection to the file is open in this process.
2. Reads the sidecar of a file with content, then `HEAD`s the stream for its `Stream-Incarnation`
   (an opaque token that changes when the stream is deleted and recreated at the same path; a
   stream without one is refused), before anything opens the file through SQLite. A file with
   content and no sidecar was never attached and is refused (it may be a database whose pages were
   never in the stream). A sidecar for another stream path is refused. A missing stream is created
   for a file without content, and refused for one with content (§6, wrong stream): the local
   files may be the only copy left. The local files are trusted when the sidecar was
   written in this boot, from this incarnation of the stream, for this db file, and the local WAL
   holds what the sidecar claims of it (§6); anything else (another or unknown boot id, another
   incarnation, an older version's sidecar, a torn one, a replaced db file, a WAL behind its
   sidecar) means the local files are discarded (§6) and the attach proceeds as on a fresh host.
   A sidecar of another incarnation is always discarded, whatever the recreated stream's length
   (logged as a recreate). For one of the same incarnation, or an older version's (which records
   no incarnation), a read at its offset first checks that the stream did not lose acknowledged
   data (§6, wrong stream).
3. Recovery (only when it rewrites pages) never opens the local files through SQLite before
   replaying onto them: trust (§6) says every page holds the state at the sidecar's offset or a
   later commit's, not that SQLite can read the file (a disk image may hold a torn page 1 that
   replay rewrites). It locks other processes out of the file: a POSIX write lock on the db file's
   lock bytes, where every SQLite connection on a WAL file holds a read lock from its first read
   until it closes, so attach fails while one is open rather than rewriting pages under its cache.
   The lock is held, not just probed, from before attach first writes the file until it is done
   writing it (a connection that read before the WAL is deleted would recover the stale WAL and
   checkpoint it over the replayed pages at its close), and the file keeps its inode throughout
   (a discard truncates it, §6; a snapshot is written over it, §4.3), so a connection that opened
   the path meanwhile gets SQLITE_BUSY until the lock drops (a busy timeout retries) and then reads
   the rewritten file. Within the process, opens of the main db through the VFS fail with
   `SQLITE_BUSY` while attach runs, and the path keeps its previous attachment until attach ends
   (see the end of this section for a failure). Then attach folds the local WAL into the db file
   itself (the frames SQLite's recovery would read, up to the last commit, a later frame winning,
   the file cut to that commit's size), fsyncs it, rewrites the sidecar at the same offset with a
   `:0` claim, deletes `-wal`/`-shm`, and only then replays onto it. No stale WAL sits next to pages
   replay has moved past it (once a crash midway has dropped the lock, a plain SQLite connection
   opening the file would checkpoint it over them when it closes), and an attach in the same boot
   after a crash midway trusts the files again and replays from the same offset (folded pages hold
   the state at the WAL's last commit, replayed ones later commits; replay is idempotent). A crash
   between that sidecar and the delete leaves commits in a WAL the `:0` claim rejects: a rebuild.
4. `HEAD`s the stream and compares its `Stream-Incarnation` with step 2's (a change fails the
   attach round, as in step 6). From here on every request carries step 2's incarnation as the
   `Stream-Incarnation` precondition. If its latest snapshot is ahead of the file (a fresh host, or a file left
   below the retention), installs it (§4.3). Then replays frames to the tail.
5. Claims with the incarnation's `Producer-Id` (§2): appends a claim at (epoch = highest seen + 1,
   seq 0) and reads it back. A 2xx alone proves nothing (two owners claiming the same epoch both
   get one, the second as a duplicate), so the claim counts only if the frame ending at the
   answered offset is ours (the nonce makes it unique). Offsets are opaque, so its start is not
   computed from that offset: the reads run from the tail step 4 reached (a frame boundary at or
   before the claim) to the answered offset, and the frames read are decoded. When the reads run
   past the answered offset (another owner appended meanwhile, and the offset's place in the bytes
   is unknown), our claim being among the frames at all suffices: the server applies one append
   per (producer, epoch, seq 0) and answers every other with its receipt, so our claim is in the
   stream only if the answer was its own end. Lost: epoch + 1. 403: the server's epoch + 1.
6. Replays up to the claim. Every read and the claim carried step 2's incarnation, which the
   server checked atomically with each, so all of them hit that incarnation. A stream deleted and
   recreated meanwhile answers `412` to the first of them that reaches it (a read at an offset the
   recreated stream does not have answers `410` or `416` instead, RFC 9110 §13.2.1; attach then
   retries from `HEAD`, or, on a `416`, `HEAD`s to tell a recreate from lost data): the round fails, and
   attach starts again from step 2 (up to three more rounds), which, the sidecar still being
   stamped with the old incarnation, discards whatever this round wrote and rebuilds from the
   recreated stream. If the replay up to the claim reaches a newer owner's claim (a higher epoch,
   which the server accepts from this producer only after ours), the attach fails as fenced and
   the application retries it: this owner is already fenced, and a snapshot it took would record an
   epoch below the highest one claimed before it (§4.2). Then, if attach wrote the db file, fsyncs
   it; writes the sidecar (with the incarnation), and swaps the path's binding to the new
   attachment.

An attach that fails after step 1 leaves the path unbound: its files may hold anything between the
old attachment's state and the stream's, and the stream may hold a newer claim. Opening its main
db then fails with `SQLITE_CANTOPEN` while it has a sidecar (§1; the reason is logged, and
`ursula_status` names it) until an attach succeeds; passing it through to the plain `unix` VFS
would let its commits bypass replication. An attach refused at step 1 (connections open, or
another thread attaching it) changes nothing: a bound path stays bound. An attach whose claim or
stream create a layer in front of the stream refuses with 402 fails the same way, and
`ursula_attach` reports it with `SQLITE_AUTH` rather than `SQLITE_ERROR`, so callers can attach
read-only instead.

**Read-only attach.** `ursula_attach(path, stream_url, 'read_only')` runs steps 1 to 4 and binds the
file without a claim (no steps 5 and 6). It never writes the stream: no claim, no commit, no
snapshot and no retention move (it has no snapshot thread). So it fences no owner and works while a
layer in front of the stream refuses writes. A missing stream is an error, never created. The file
holds the stream as of the attach (attach again to catch up), and the sidecar records the highest
epoch claimed up to its offset, which a later owner's attach claims above. Every WAL write of the
database fails with `SQLITE_READONLY` before anything is appended or written to the WAL, and SQLite
reports "attempt to write a readonly database" and rolls the transaction back. A write transaction
that changes nothing (`BEGIN IMMEDIATE` then `COMMIT`) writes no WAL frame and still succeeds, so a
library that opens one at startup (Pi's migrations on an existing database) works. The local files
are kept as for an owner: checkpoints, the WAL claim and the sidecar. The uses: exporting a database
without fencing its owner, and reading a database whose owner cannot claim it because a layer
refuses its writes.

The new epoch fences every earlier owner at the server: their next append gets 403. Replay applies
page images per read batch (last image per page, truncate to the batch's smallest size first), so
replaying from an older offset than the file reflects is idempotent.

Producer expiry: the server forgets a producer idle for 7 days. The owner's next append gets 409
expecting seq 0; it takes the stream back only if the stream still ends at its own offset and its
new claim (epoch + 1) is the first frame after it (read and decoded from that offset, as in step
5), else it is fenced. Both requests carry the incarnation, so a recreated stream (which answers
`412` before any producer check) fences the owner instead. A new claim refused with 402 fails the
commit alone, as a refused commit does (§2). Reads in
recovery use `consistency=leader` (a follower may lag an acknowledged append). Catch-up reads,
`HEAD` and snapshot `GET`s retry `429` and `503` (a leader that could not confirm its leadership in
time) like appends: no sooner than `Retry-After`, with backoff, within `URSULA_VFS_RETRY_MS`; the
snapshot thread's retries also end at its stop flag.

## 4. Snapshots and retention (M3)

Without them the log, and so attach time and stored bytes, grows forever; without a cold tier
the default per-group hot limit (64 MiB) trips after about 68 MB of frames.

### 4.1 When

After a commit is published, if the log since the latest known snapshot exceeds
`max(database size, URSULA_VFS_SNAPSHOT_MIN_BYTES)` (8 MiB by default), the database's snapshot
thread is woken. Offsets are opaque, so the log is counted in frame bytes, not taken from offsets:
attach counts what it replays after the snapshot it installed (or, from trusted local files, adds
it to the count the sidecar carries), every acknowledged commit adds its frame, and a snapshot
taken (or found newer at publish) subtracts what was counted when its window opened.
Snapshotting never runs on the commit path. Stored log is therefore bounded by about twice the
threshold plus what accumulates while a snapshot is in flight (§4.4).

### 4.2 Taking one

The body must be the stream's state at a frame boundary `W`, page-identical to a replay from the
stream's start to `W`, so later page-image frames apply on top of it (`VACUUM INTO` and the backup
API both rewrite pages, and are not). The thread:

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
6. Fsyncs the WAL and the db file through the private connection's own handles, which reports a
   write-back error of a page the checkpoints or the copy read (§6). A failed fsync poisons the
   database, and so does a lost write the owner found meanwhile: either way nothing is published.

Commits wait only for steps 3 and 4 (a passive checkpoint of the few frames committed since step
1, and the start of a read transaction) and, once a snapshot is due, for the window to open
(step 2, at most one commit, bounded). A re-attach stops the thread: it checks the stop flag
between steps and retries, and each snapshot request is bounded to 120 s.

The body is `"USS2" | u8 n | W (n bytes, the offset string) | u64 epoch | u32 pages |
u32 crc32c(image) | zstd(image)` (`"USS1"` held `W` as a u64 and is no longer read). The epoch
is the highest one claimed before `W`: retention may trim every claim frame, and the next owner
must still claim above it (after a producer expiry the server would accept a lower epoch, and a
zombie with a higher one could then fence the new owner).

### 4.3 Publishing, retention, and attach

1. `PUT {stream}/snapshot/{W}` (retried while the outcome is unknown; publishing is idempotent),
   with the `Stream-Incarnation` precondition (a recreated stream answers `412`; below). 409/410: a newer snapshot
   exists; nothing to do.
2. `GET {stream}/snapshot/{W}` until it returns exactly the published bytes.
3. Only then `PUT {stream}/retention/{P}`, where `P` is the *previous* snapshot's offset (the latest
   one known at attach, or the one before `W`).

Why one snapshot behind: a host that read the previous snapshot's offset from `HEAD` and is
fetching it, or whose file sits between `P` and `W`, still finds every frame after `P`; retention at
`W` would turn its tail read into `410` and a full re-download. The cost is up to one more
threshold of retained log. The newer snapshot has read back before any history is dropped, so the
retained stream always holds a readable snapshot at or above its start.

Attach installs a snapshot when the file is behind the latest one: it verifies the body (offset,
size, checksum), locks other processes out of the file (§3), deletes its WAL, writes the image over
the db file in place (same inode, cut to the image's size), then replays the tail. A crash midway
leaves a mix of old pages and the image's. Either way the next attach installs the snapshot again: if
the sidecar claims no WAL frame, the old pages hold the state at its offset or a later one (the db
file alone held it) and the files can be trusted; if it claims frames, those went with the deleted
WAL (the old pages may predate the offset) and the files are rebuilt. A tail read that hits
`410` (retention moved under a stale `HEAD`), or a snapshot superseded between `HEAD` and `GET` (a
404, or a body cut short when its cold object is deleted after the grace), restarts attach from
`HEAD`, up to ten times.

A publish or retention move that a layer in front of the stream refuses with 402 publishes or moves
nothing and is not counted as a snapshot failure. It records the refusal (`payment_required`), and
the thread takes no snapshot until a commit is acknowledged again, which requests one if it is
still due. While writes are refused no commit adds to the log, and each attempt would copy and
compress the whole database for nothing.

Any owner may publish a snapshot of its own offset, a fenced one included: its state at its offset
is a true prefix of the incarnation it attached to. The snapshot and retention endpoints know no
producer, so the snapshot `PUT`, its read-back `GET` and the retention `PUT` carry the
incarnation as the `Stream-Incarnation` precondition, checked by the server atomically with each:
a stream deleted and recreated meanwhile answers `412`, and the thread publishes nothing there (nor
moves its retention) and fences the owner (§6, wrong stream). Retention never passes the latest
snapshot (the server refuses).

### 4.4 Snapshot bodies on the server

Bodies of at least `runtime.external_payload_min_size` (1 MiB) are stored in the cold tier when
a cold backend is configured, up to 1 GiB; inline otherwise (up to 32 MiB). Through `ursulagw` a body above its
`--max-request-body-bytes` (32 MiB) is refused: the snapshot thread logs and retries with backoff,
and the log keeps growing. A superseded cold body stays readable for 5 minutes.

## 5. Guarantees

- An acknowledged commit (the SQL `COMMIT` returned) is in the stream, and every host that
  attaches after it sees it, through replay or a snapshot that includes it. Caveat: the owner's
  later commits are page images read from its local files, so local files that lose a write
  underneath a running process (the write-back I/O error of §6) could overwrite acknowledged
  changes in the stream. The VFS checks WAL pages read back against what it wrote, which closes
  most of that window; §6 lists what remains. Local files rolled back while no process had them attached (a restored disk
  image) are caught by attach (§6).
- Nothing the stream did not acknowledge becomes visible: no frame of a transaction reaches the
  local WAL before its append is acknowledged, and a fenced, refused or failed commit leaves no trace
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
- **The check.** Attach trusts the files only when the boot id, the stream's incarnation and the
  db file's inode match and the WAL, read the way SQLite's recovery reads it (header checksum,
  then frames in order while their salts match and the cumulative checksum holds), is the claimed
  generation with valid frames up to at least the claimed one; for `:0`, it must hold no commit
  frame.
- **Three fsyncs of the db file**, none per commit, all through SQLite's own handle except at
  attach (when no connection is open): (1) before a commit that starts a new WAL generation is
  appended (once per WAL wrap; SQLite starts one only when every frame of the previous one is in
  the db file); (2) before an attached connection truncates the WAL to nothing (only after a
  complete checkpoint: `wal_checkpoint(TRUNCATE)`, a close with `journal_size_limit`), the
  sidecar switching to `:0` first (a truncate to a non-zero size, `journal_size_limit` in the
  commit that starts a generation, cuts only the previous generation's tail, already synced by
  (1)); (3) at attach, after folding the WAL and before the sidecar that drops its claim (`:0`; the
  WAL is deleted after it), and again before the final sidecar whenever attach wrote the db file.
  A failed fsync poisons the database (or fails the attach). The snapshot thread also fsyncs the
  WAL and the db file after reading them, off the commit path (§4.2, step 6): that reports lost
  write-backs, and the claim does not rely on it.
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
  what this host wrote and, outside the windows below, the claim holds (the sidecar is
  written only after the WAL writes return). A killed write leaves a prefix; SQLite's salted,
  cumulative WAL checksums stop recovery at the last whole commit, `-shm` is rebuilt, checkpoints
  are redone from the WAL. Attach trusts the files and replays from the sidecar's offset (fast).
  A crash in the middle of a WAL truncate (between the `:0` sidecar and the truncate), or between
  the first WAL write of a new generation (the commit after a wrap or a truncate) and the sidecar
  update, or in a recovery between the `:0` sidecar and deleting the folded WAL, leaves a claim
  the files do not meet; that only costs a rebuild. Covered: SIGKILL before and after the ack,
  the cache spill with in-place checksum rewrites, a failed local write after the ack, a crash
  mid-recovery (the WAL is folded and gone, the sidecar claims `:0` at the old offset).
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
  another process has the file open, and truncates the db file under the lock, keeping it locked
  for the rebuild (§3), and removes a snapshot temp file an older version may have left (never the
  held lock file); every attach then removes `-journal`, and the fresh path `-wal` and `-shm`, and
  rewrites the sidecar last, so a crash midway discards again. The first attach after upgrading
  from a version with numeric offsets (sidecar format 1, no `v=`), or without the WAL claim or the
  incarnation, rebuilds once (such a sidecar is not trusted; it is still parsed, for the stream
  path, the incarnation and the read check at its offset). Older versions take this one's sidecar
  for a torn one and rebuild, or refuse it, and none of them reads a `"USS2"` snapshot body: after a
  downgrade, a stream this version has snapshotted cannot be attached (attach under a new stream
  URL, as for `"USS1"` below); one it has not snapshotted is rebuilt from the stream after deleting
  `<db>`. Snapshot bodies of the numeric-offset versions (`"USS1"`) are not read: a stream
  whose latest snapshot is one cannot be attached (attach fails on its snapshot; deleting the local
  files does not help): attach under a new stream URL. Versions before the incarnation-scoped
  `Producer-Id` (§2) append as `sqlite-ursula-vfs`, a producer this version's claims do not fence:
  stop every owner of a stream that runs an older version before attaching it with this one (and the
  reverse on a downgrade), or two owners can both commit and corrupt the database (replay then mixes
  page images of two diverged states).
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
- **Wrong stream**: the sidecar names the stream's path and its incarnation; attaching the file
  to another stream is refused. A stream deleted and recreated at the same path is another
  incarnation, so the files are never trusted for it: whatever its length, they are discarded
  (logged) and rebuilt from it, also after a reboot. A sidecar of the same incarnation, or an older
  version's (which records no incarnation, so may be of the same one), whose offset lies beyond the
  stream's end means the stream lost acknowledged data: attach refuses and keeps the files (delete
  `<db>` to rebuild). After an operator restore from backup (same incarnation), delete `<db>`:
  attach refuses while the stream ends before the sidecar's offset, but trusts the files once it
  has grown past it. A stream deleted (by mistake, by TTL expiry, or by a fresh install that did not
  carry it over) leaves the local files as possibly the only copy: attach creates a missing stream
  only for a file without content, refuses one with content and keeps its files. To start over
  from an empty stream, delete `<db>` (attach then creates the stream), or attach a new file under
  another stream URL: the files attach to no other URL (the sidecar names this one), and a copy of
  them has no sidecar, so attach refuses it as never attached. To salvage the data, copy `<db>` and
  `<db>-wal` to a new name before deleting anything (an attached file's WAL persists and may hold
  commits not yet checkpointed), open the copy, which has no sidecar and so opens as a plain SQLite
  file, and export its rows into a newly attached database. An empty file attached at the same URL
  after the delete (another host, a fresh install) creates a new incarnation, and the original
  owner's next attach then discards its files as the cache of a recreated stream: moving them aside
  instead is a follow-up. A recreate during attach fails that round, and attach rebuilds from the
  recreated stream (§3).
  While attached, every request of the owner carries its incarnation as the `Stream-Incarnation`
  precondition, which the server checks atomically with the request (an append when Raft applies
  it, so a commit proposed before a delete and recreate and applied after is refused too): the
  recreated stream answers `412` to the owner's next commit, claim, re-claim, read, snapshot
  publish or retention move, nothing of it lands, and the owner is fenced and poisoned. The
  windows earlier versions had (a stray claim of an attach racing the recreate, accepted as a new
  producer's seq 0; commits of an old owner accepted under that claim's `Producer-Id`; a snapshot
  or retention move between the thread's `HEAD` and its `PUT`) are closed for owners of this
  version. An owner of an earlier version still has them: stop such owners before deleting a
  stream you will recreate. The precondition needs a server of Ursula 0.6.0 or later; an older
  server ignores the header (attach does not detect this), so the windows stay open there.
- A rollback journal next to an attached file can only be left by a crash while attach switched
  an empty file to WAL; attach deletes it in every case (rolling it back would truncate the file
  under the pages attach writes next).
- Network partition or slow server: commits block up to the retry budget, then poison. An attach
  that fails after step 1 leaves a file with a sidecar refusing opens until an attach succeeds
  (§1, §3); a file whose first attach failed before writing its sidecar (the server unreachable,
  for example) is still a plain file and passes through.
- A layer in front of the stream that refuses writes with 402 (an exhausted quota, a suspended
  account): commits fail alone with `SQLITE_IOERR_AUTH` and nothing of them reaches the stream,
  reads go on, and the first commit after the layer accepts writes again succeeds, with no attach
  (§2). A process that restarts meanwhile cannot attach as the owner, because its claim is refused.
  It attaches read-only to read (§3).
- Two owners: the later claim wins; the earlier one's next commit fails cleanly
  (`UrsulaReplicationError` with `fenced: true` through the Pi helper).
- A snapshot that cannot be taken (a long reader pins WAL frames) or published (body too large):
  the log grows, nothing is lost; retention simply does not advance.

A disk write-back I/O error underneath a running process (EIO from the device, a
thin-provisioned volume out of space). The commit path never fsyncs the WAL, and the db file only
at WAL restarts, truncates and attach, so nothing reports such an error when it happens. Once the
dirty page is evicted, a read gets the old block back, and SQLite does not check WAL frames on a
read: the owner's next commits would append page images built on it, losing acknowledged changes
from the stream itself, not only from the cache. So the VFS remembers the crc32c of every WAL page
this process writes in the WAL's current generation, by offset (4 bytes per page), and checks
every page its connections read back. A page that reads back other bytes fails the read and poisons
the database before any commit can build on it. An error writing back the db file is reported
(Linux reports it to an fsync on any descriptor opened before it) by the next fsync of the db file.
That fsync comes before the commit that restarts the WAL, which is normally the first commit after
a complete checkpoint, the point from which readers read checkpointed pages from the db file
instead of the WAL.

The snapshot thread's private connection reads the WAL (its checkpoints) and the db file (its
page copy) outside the VFS, so it does not see that check. A lost page it published would become
permanent once the next snapshot moves retention past the frames that hold the good one. So after
reading, it fsyncs both files through its own handles (§4.2, step 6) and publishes nothing when an
fsync fails (which poisons the database) or the owner is poisoned for a lost write. On Linux an
fsync reports a write-back error that no fsync has reported yet, from any descriptor, and in the
process only the owner's fsync of the db file could have reported it first, which poisons too.

Residual risk:
- The snapshot's fsyncs rely on the kernel reporting an earlier write-back error to a later fsync
  on another descriptor, which Linux does. Elsewhere (macOS) a lost page the checkpoints or the
  copy read, and no connection of the owner read back, can still be published.
- A page changed by anything but a failed write-back (unsupported, below) is reported by no fsync:
  unless a connection of the owner read it back first, a snapshot can publish it.
- A commit that cannot restart the WAL, because another connection still reads from it, can read
  a db page that lost its write-back since the last fsync and append the damage.

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
- One owner process per stream at a time; connections in other processes that load the extension
  cannot open the file (§1); processes without it are not replicated (and block recovery).
- The local files must be on a local filesystem (§6).
- Commit frames are at most the server's request limit (32 MiB), about 8000 changed pages per
  transaction.
- Snapshots hold the database image in memory (twice, briefly: raw and compressed) and are capped
  by the server at 1 GiB compressed (32 MiB inline or through the gateway).
- Producer expiry after 7 idle days; the owner reclaims only if nobody wrote meanwhile.
- HTTP, or TLS for `https://` URLs with the bundled Mozilla roots, or only the certificate
  authorities in `URSULA_VFS_CA_FILE`. One bearer token per process: `ursula_set_token`, else the
  content of `URSULA_VFS_TOKEN_FILE`, read again when the file changes and after a 401. A read of
  the file that finds no token (empty, half-written, missing) keeps the last one it held: sent
  without one, a request is concealed as a missing stream (404), which poisons at once. A token
  sent over `http://` to a host that is not loopback is logged once (`token_in_clear`). The SQL
  functions are direct-only: a trigger or view of an untrusted database cannot call them.

### Versions

Extensions of different versions meet in a stream and in the local files. They must agree on:

| what | where | now |
| --- | --- | --- |
| commit and claim frames | the stream | `USQ1` |
| snapshot bodies | the snapshot store | `USS2` |
| the sidecar | `<db>-ursula`, on each host | format 2 (`v=2`) |
| the producer | every append | `Producer-Id: sqlite-ursula-vfs/<Stream-Incarnation>`, and `Stream-Seq` from the epoch and sequence |

None of these changes within a minor (0.7.x). Any 0.7.x extension attaches a stream another 0.7.x
wrote, installs its snapshots, resumes from its local files on the same host, and fences its owner,
and is fenced by it in turn. So upgrading the extension one owner at a time is safe: the new
owner's attach fences the old one. CI checks this on every change: `test/compat.e2e.ts` runs the
previous published patch of the minor against the build, both ways (a new host taking over, the old
owner's next commit failing, the old version reading the new one's frames, and the same host
resuming from the other version's files). Until a minor has a second patch it runs the build
against itself.

A change to any of them waits for a minor and changes the identifier with it: a new frame or
snapshot magic, a new sidecar format, a new `Producer-Id` prefix. An extension fails closed on what
it cannot read. A frame or snapshot of another format stops attach with an error that names the
magic it found (`frame magic "USQ2": this extension reads "USQ1" frames, ...`) instead of
misreading it. A sidecar it cannot parse is not trusted, so attach rebuilds the files from the
stream. Owners with different `Producer-Id`s do not fence each other, so a minor that changes it
must say in its release notes to stop every owner of a stream before upgrading.

## 8. Tests

- Units (`cargo test`): frame and snapshot encodings (whole frames only, damage refused, another
  format refused by its magic, named in the error), and the claim lookup (a claim wins only as the
  frame ending at the answered offset, or anywhere in a read-back that ran past it; a re-claim also
  only as the first frame after the owner's offset). A 402 fails a request alone with the layer's
  explanation, and ends an append only while no attempt of it can have reached the stream: after a
  401 or a 429 it does, after no answer or a 5xx it is retried until an answer settles the outcome,
  and an outcome still unknown when the budget runs out fails as unknown. A refused commit leaves
  the owner unpoisoned with its epoch, sequence and offset, and the attach mode parses.
- Units also cover the sidecar: trusted only in this boot, for this stream incarnation and db file,
  and with its WAL claim met (a claim on frames the WAL does not hold or on another WAL generation,
  or no claim, is not); legacy, other-boot, other-incarnation and torn sidecars are not, nothing is
  when the current boot id is unknown, and a missing sidecar is an error; folding a WAL into the db
  file keeps the last image and cuts the file to the commit's size. CI runs them on macOS too (its
  boot id).
- e2e against a real node (`clients/sqlite-ursula`): transparency (any schema, byte-identical
  rebuild), the crash matrix (same-boot re-attaches resume from the sidecar, a recovery killed
  mid-rewrite included, without a snapshot), fencing (an owner of a deleted stream included: its
  next commit fails and nothing of it reaches the recreated stream; a foreign append with a higher
  `Stream-Seq` fences the owner), recovery exclusion, the local cache (a simulated reboot with a
  rolled-back db file and sidecar, below retention, a torn first sector and a cut WAL rebuilds
  byte-identical from snapshot + tail; the same boot reuses the files with no snapshot and no replay
  from scratch, also for a file rebuilt from a snapshot; a same-boot disk image whose WAL is behind
  its sidecar is rebuilt and the stream stays intact, one whose WAL is ahead is trusted; discarding
  is refused while another process has the file open; a replaced db file is rebuilt; a file without
  a sidecar, the cache of another stream, and files whose sidecar offset lies beyond their stream's
  end are refused (also with an older version's sidecar), and so is a file with content whose stream
  was deleted (its files kept, no empty stream created); a format-1 sidecar (numeric offset) behind
  a stream another owner extended is rebuilt; the cache of a deleted stream is rebuilt from the
  stream recreated at its path, both shorter than the file's offset and grown past it), a file with
  a sidecar opening only while attached in the process (refused before an attach, and after a failed
  re-attach of an attached file, until an attach succeeds; then commits replicate again), a WAL
  page changed underneath the owner (standing in for a lost write-back) failing the read that gets
  it back and poisoning the owner, not fenced, with the stream intact, snapshots
  and retention (a ~160 MB run; CI also runs it without a cold tier under the default hot limit;
  fresh and lagging hosts rebuild byte-identical from snapshot + tail; the takeover after the trim
  fences the old owner), Pi conformance in three modes, and a benchmark (sanity numbers only).
- Writes refused with 402 (`test/payment.e2e.ts`), through `test/gate.mjs`, an HTTP layer in its
  own process that answers 402 on demand (the server never does). Commits refused in autocommit
  and in an explicit transaction fail with `SQLITE_IOERR_AUTH` while reads go on, the owner keeps
  its epoch and offset, the next commit after the layer accepts writes succeeds without an attach,
  and the stream holds the accepted commits alone. An append whose answer was lost and whose
  retries meet a 402 is applied once. An owner refused with 402 is still fenced by a newer claim. A
  refused attach throws `UrsulaPaymentRequiredError`, and a read-only attach of the same file
  reads it, refuses writes, appends nothing, fences no owner and catches up when attached again. A
  read-only attach never creates a missing stream. A snapshot refused with 402 is no snapshot
  failure and is published after the next commit. Through Pi, a refused commit rejects with
  `UrsulaPaymentRequiredError` and the storage commits again once the layer accepts, and a
  read-only storage reads what an owner wrote and refuses its commits.
- Same-minor compatibility (`test/compat.e2e.ts`, §7): the previous published patch of the minor
  and the build take over each other's streams on a new host and on the same one, both ways.
- The same Pi conformance, snapshot and benchmark suites on 3 nodes + gateway + S3 (AWS S3 in CI,
  MinIO locally and for fork PRs; the snapshot run's ~3.5 MB bodies go to the cold tier), plus a
  12 MiB snapshot body that exercises the multipart path, with a 64 KiB snapshot minimum so the
  benchmark's Pi workload snapshots and trims at its database size.

## 9. Performance

One run, 2026-10-09 UTC. EKS 1.37 in us-east-1: three `m6i.xlarge` Ursula nodes, one per AZ (the
chart's `examples/production-eks.yaml` shape: 256 groups, 4 cores, 8 GiB limit), three gateways on
`m6i.large` nodes, one per AZ, and real S3 for the cold tier and snapshots with Ursula's default S3
settings. Server image: main `bb7a61b` (`0.0.0-main.bb7a61b1c968`) with the chart's defaults: the
Raft WAL on a gp3 volume with `raft.wal.fsync = never`, and no gateway quota policy, so no client
saw a 429. Gateway `maxRequestBodyBytes` was raised to 1 GiB for the cold-start cells. Extension:
#488 at `398369d`, and `ee5e278` for two of the three failover runs (it changes nothing on the
commit path except a log line when an append is retried, `append_retry`). Clients: `m6i.2xlarge`
pods (Node 22.20) in us-east-1a, one process per database, through the gateway Service. The tools
are in `clients/sqlite-ursula/bench/`.

Workload: Pi Durable's `SqliteStorage` on the VFS via `openUrsulaPiStorage`, a real `Harness` with
a faux model, turns of text, text, tool (5.0 Pi commits per turn). Latency is `Storage.commit`
wall time after a 30 s warm-up. Each cell runs 10.5 min. Agent pace: one turn per 2 s per database.
Baselines on the same client node type:

- B1: the same Pi workload on Pi's own `openNodeSqliteStorage` on the pod's disk, no extension
  (WAL, `synchronous=NORMAL`). Measured on 2026-10-03. It involves no server.
- B2: the same with `PRAGMA synchronous=FULL` (the WAL synced on every commit). Also 2026-10-03.
- B3: raw appends, no SQLite: closed-loop writers, one stream each, 5 to 7 KiB random bodies with
  producer headers, through the gateway or straight to the stream's leader (node Service, `307`
  followed and the leader kept). 75 s cells after a 15 s warm-up, on the fresh cluster and again
  right after the 128-database cells.

Pi commit latency, ms, p50 / p99 (p99.9 / max), and Pi commits/s:

| cell | B1 local, NORMAL | B2 local, FULL | VFS |
| --- | --- | --- | --- |
| 1 db, agent pace | 0.11 / 7.6 | 1.8 / 4.6 | 4.7 / 7.9 (11.6 / 11.9) |
| 1 db, flat out | 0.16 / 8.9; 115/s | 2.2 / 4.9; 100/s | 4.4 / 7.0 (51 / 144); 81/s |
| 16 dbs, agent pace | 0.11 / 6.5 | 1.6 / 4.8 | 4.1 / 7.4 (13 / 162) |
| 16 dbs, flat out [1] | 0.20 / 13.1; 651/s | 2.8 / 19.1; 652/s | 8.1 / 28.4 (58 / 199); 541/s |
| 128 dbs, agent pace [2] | | | 3.9 / 6.0 (13 / 171); 320/s |
| 128 dbs, flat out [2] | | | 26.7 / 105 (191 / 883); 2,051/s |

[1] 16 processes on one 8-vCPU client node, which ran at about 70% CPU while each Ursula node used
0.3 to 0.6 cores, so this row measures the client too.
[2] Two client nodes, 64 databases each, pooled. Each Ursula node used up to 2.5 of its 4 cores in
the first minutes of the flat-out cell and 0.8 to 1.1 later, with RSS at 1.4 to 1.8 GB. Client CPU
was not recorded.

No database was poisoned, no commit failed, and none of the 1.85 million commits in these cells
retried its append. The 2026-10-03 run measured a disk WAL that fsynced every
batch and an extension that still fsynced its local WAL and sidecar on each commit (before #332).
It read 16.5 / 20.0 ms for one database at agent pace, 46 commits/s for one database flat out,
499/s at 16 and 841/s at 128 with a p99 of 2,123 ms, and one of the 128 databases was poisoned.

Raw append floor (B3), ms p50 / p99 (p99.9), and appends/s. No raw cell saw a `503` or a `429`:

| writers | gateway | leader | gateway, after 128 dbs | leader, after 128 dbs |
| --- | --- | --- | --- | --- |
| 1 | 1.54 / 2.53 (19.8); 593/s | 1.20 / 2.38 (12.7); 747/s | | |
| 16 | 2.94 / 7.53 (75); 4,662/s | 2.87 / 7.25 (75); 4,752/s | 2.92 / 7.65 (81); 4,696/s | 2.82 / 7.92 (80); 4,677/s |

Where one database's commit goes (1 db at agent pace, p50, ms):

| component | ms |
| --- | --- |
| Pi and SQLite outside the VFS's commit | 0.18 |
| VFS commit hook without the append | 0.12 |
| the append request | 4.31 |
| measured Pi commit | 4.66 |

The append is the commit, and what it costs depends on where the stream's leader and the
connection's gateway are. Through the Service, each connection lands on one of the three gateways,
one per AZ, and the stream's group is led from one of the three nodes. To separate the VFS from
placement, an ad hoc run outside the committed tools (`raw.ts` creates its own streams, sends 5 to
7 KiB bodies and goes through the Service) used six fresh streams. On each, one database ran flat
out for 30 s, and then raw appends of the VFS's frame size (4.8 KB) went to the same stream, both
sent to the gateway pod in the client's AZ rather than through the Service. The VFS appends with
ureq from inside the extension and the raw appends use Node's `fetch`, so the difference includes
the two HTTP clients:

| leader's AZ | VFS append p50, ms | raw append p50 on the same stream, ms |
| --- | --- | --- |
| us-east-1a (the client's) | 1.31, 1.29 | 1.22, 1.21 |
| us-east-1b | 1.66, 1.39 | 1.53, 1.28 |
| us-east-1c | 2.33, 2.39 | 2.26, 2.35 |

The VFS, with its HTTP client, adds 0.04 to 0.13 ms to an append over `fetch`. The one-database
cells' 4.0 to 4.3 ms went through the Service, where the connection's gateway can also be in another
AZ, and that placement was not recorded. Over 16 and 128 databases at agent pace the VFS's append
p50 was 3.5 to 3.7 ms.

**The 2026-10-03 problems.** Neither reproduced. At 128 databases flat out no database was
poisoned, no append was retried, no node logged `rebuilding channel`, and there were no
AppendStream backpressure rejections. Right after the 128-database cells, the 16-writer raw cells
and 16 databases at agent pace (4.2 / 7.2 (17 / 59)) read as on the fresh cluster. What remains is
#496. During the 16-writer raw cells and the 128-database flat-out cell, the leaders' AppendStream
calls to followers exceeded OpenRaft's hard TTL, with failed heartbeats (400 to 900 warnings per
node in the 128-database cell, most in its first minute). The client tails above (p99.9 75 ms on
raw appends, maxima of 0.5 to 1.4 s) line up with them only in part.

**Stream and snapshots.** 7 pages per commit. zstd ratio 9.4 at agent pace and 6.1 to 6.2 flat
out, so 3.6 KB per commit at agent pace and 4.6 to 4.8 KB flat out. A snapshot is due once the log
outgrows the database, at least 8 MiB (§4.1). The agent-pace databases had about 7 MB of log after
10.5 min and took none. The retained log (tail minus retention) ended at no more than 7.0 MB at
agent pace, 16.8 MB in the 16- and 128-database flat-out cells, and 25.4 MB for the single
flat-out database. Retention trails one snapshot behind (§4.3), so the retained log reaches about
twice the snapshot threshold, plus what is committed while a snapshot is taken: 16.8 MB is twice
the 8 MiB minimum, and the single flat-out database had grown to 13.8 MB, which set its threshold
above the minimum.

**Back-to-back large transactions** (5 MB table, 2,000-row updates, frames of about 1 MB, no pause,
300 s): 3,429 commits at p50 91 / p99 182 / max 1,138 ms, 3.35 GB written, 381 snapshots. The
retained log never exceeded 19.5 MB (sampled every 10 s) and ended at 15.7 MB. S3 still held
2.94 GB for the stream 29 minutes after the writes: the external payloads that #475 leaves behind,
fixed by #487, which this image predates.

**Cold start**: a fresh host attaching new files, three times, best with the first in parentheses
(`bench/coldstart.ts`; the log replayed, and the tail after a snapshot, are the attached file's
`log_bytes`). A newly built database has no snapshot yet, because one is due only once its log
outgrows the database (§4.1), so the first column replays the whole log. For the second, rows were
rewritten after the build until a snapshot was published, as a database that has lived a while would
have one:

| database | log only: log replayed, attach | with a snapshot: body (publish time), tail after it, attach |
| --- | --- | --- |
| 10 MB | 2.5 MB, 48 ms (77) | |
| 100 MB | 24.6 MB, 0.28 s (1.11) | 23.8 MB (6.7 s), 11 MB, 0.85 s (1.10) |
| 1 GB | 246 MB, 8.0 s (12.8) | 237 MB (10.9 s), 74 MB, 15.5 s (18.7) |

Installing a snapshot is slower than replaying as many bytes of log (#497): 237 MB of snapshot and
74 MB of tail took 15.5 s, 246 MB of log 8.0 s. On 2026-10-03 a 1 GB database took 16.1 to 16.9 s (first
attach 25 to 27 s) from a 278 MB snapshot and 133 to 169 MB of tail.

**Failover**: 16 databases flat out, and at 120 s `kubectl delete --force` of the node leading the
most groups (86 of 256), three runs. No commit failed and no database was poisoned. Afterwards every
owner's file and a fresh rebuild from its stream were identical row for row, with
`integrity_check` ok (48 of 48). The longest commit around the kill, per database:

| run | over 10 s | 2 to 10 s | 0.5 to 2 s | under 0.5 s |
| --- | --- | --- | --- | --- |
| 1 | 21.9 | 11 (2.1 to 4.4) | 0 | 4 |
| A | 21.4, 21.4, 13.2, 13.2, 12.2, 11.5 | 7 (2.2 to 6.1) | 0 | 3 |
| B | 21.8, 10.4 | 9 (2.0 to 2.4) | 1 | 4 |

Elections took up to about 5 s: the VFS got `503` from the groups' nodes once a second for that
long. The stalls over 10 s are whole 10 s VFS request timeouts, one or two in a row (the
`append_retry` lines of runs A and B). They are requests the gateway sent over pooled connections
to the deleted pod, which never answered (#495). Two in a row use 21 s of the VFS's 30 s retry
budget. The 2026-10-03 run's disk-WAL stalls of 8.3 to 10.1 s fit one such timeout each. The
replacement pod exited twice with `the Raft WAL is already in use` while the force-deleted
container still held the WAL lock, then started. Two snapshot uploads during the kills got the
gateway's `504` after its own 30 s upstream timeout, the same cause, and the next attempt published
them. The snapshot read-back `503` of the 2026-10-03 run did not recur.

**S3.** Requests per minute (CloudWatch S3 request metrics), whole bucket, median (range) over the
minutes wholly inside each cell:

| cell | PUT | GET |
| --- | --- | --- |
| 1 db, agent pace | 11 (2 to 98) | 189 (145 to 529) |
| 1 db, flat out | 42 (29 to 52) | 275 (219 to 329) |
| 16 dbs, agent pace | 3 (2 to 115) | 181 (159 to 560) |
| 16 dbs, flat out | 253 (180 to 498) | 978 (816 to 1,800) |
| 128 dbs, agent pace | 67 (9 to 767) | 673 (377 to 2,613) |
| 128 dbs, flat out | 1,317 (916 to 1,721) | 4,921 (2,957 to 5,647) |
| 16 dbs, agent pace, after the 128-db cells | 61 (26 to 181) | 1,220 (1,093 to 1,593) |
| back-to-back large transactions | 701 (595 to 723) | 1,537 (1,327 to 1,653) |

Two background costs grow over a run, which is why later cells read higher than earlier ones at
the same load:

- The cold-index repair cursor (bounded-stream-state F19). Every 60 s each group's leader reads
  the index pages of up to 16 of its streams. A stream with cold data therefore costs one or two
  GETs a minute whether or not it is written, until a group has more than 16 such streams. With no
  client running (05:05 to 05:21 UTC) and about 310 such streams, the stream prefixes took 550 to
  615 GETs and at most 6 PUTs a minute. Near the end, with over 600 streams, about 1,000 GETs a
  minute.
- Raft snapshots in the S3 snapshot store. Every voter uploads its own object per snapshot,
  several MB for a busy group, and prunes after its own builds, past a 1 h grace. In the
  128-database flat-out cell the snapshot prefix took 450 to 950 PUTs and 1,900 to 3,900 GETs a
  minute. At 05:49 UTC the store held 5,448 objects (15.8 GB), 3.9 GB of them past the grace. A
  group that stops building snapshots never prunes (#494), and a pin released between listing and
  reading aborts a prune (#490).

**Open issues seen in this run.** #495 (failover stalls of 10 to 22 s through the gateway), #496
(AppendStream calls past the OpenRaft hard TTL under load), #494 and #490 (Raft snapshot objects in
S3), #475 (external payloads kept after retention, fixed by #487 after this image), and #497
(installing a snapshot is slower than replaying as many bytes of log).

### Baseline: Pi Durable in a Durable Object (Cloudflare PiHarness)

Cloudflare runs Pi Durable inside a Durable Object through the Agents SDK: `PiHarness` from
`agents/harness/pi` (`agents` 0.26.0, cloudflare/agents#2423). Pi's own `SqliteStorage`
(`@earendil-works/pi-durable` 1.0.0) runs over the object's SQLite, with Pi's tables prefixed `pi_`.
We ran the VFS workload inside such objects: a real `Harness` with the faux model, turns of text,
text, tool (5.0 commits per turn), a 30 s warm-up and 10.5 min cells. Each database is its own fresh
object of a SQLite-backed class (122,880 bytes at the start of a cell). Every object was in IAD
(Ashburn, Northern Virginia, the same metro as the VFS's us-east-1 cluster), created with
`locationHint: "enam"` and its colo checked through `cdn-cgi/trace`. The objects of a cell ran at the
same time. The account is on Workers Paid (standard usage model). The Worker uses compatibility date
2026-06-11 with `nodejs_compat` and `limits.cpu_ms` 300,000. It was deployed and driven with
`wrangler` 4.147.0 from a laptop, not from GitHub Actions. Runs: re-run 2 on 2026-10-03 (harness v2,
`wrangler tail` attached) and re-run 3 on 2026-10-04 UTC (harness v3, no tail). Run 1 used a timer
that turned out to include Pi's CPU and is withdrawn, except for the cold-start numbers below.

**Paths and modes.** The *pi path* calls Pi's root conversation `submit` and `wait` inside the
object, the same calls as the Node bench, through PiHarness's storage adapter. This is the baseline
in the tables. The *shipped path* calls `PiHarness.submit` and `wait`, which add a Lifecycle wake
job and an alarm to each turn. In *durable* mode every commit awaits `ctx.storage.sync()` before Pi
continues, as every VFS commit waits for its append. *As shipped*, commits return at once and the
output gate holds the object's outgoing messages until its writes are confirmed. There we await
`sync()` before each faux model call, where a real model fetch would be held, and at the end of each
turn.

**What a confirmed write means.** A DO write is confirmed once at least 3 of the object's 5
followers, each in a different physical data center, report that they received it; a follower keeps
what it receives in a buffer on local disk. Changes then go to object storage in batches of up to
10 s or 16 MB, whichever comes first (Cloudflare, "Zero-latency SQLite storage in every Durable
Object", https://blog.cloudflare.com/sqlite-in-durable-objects/). In the §9 run a VFS append was acknowledged
when 2 of the group's 3 replicas, one per AZ, had written it to the Raft log on local disk, in the
page cache without `fsync` (`raft.wal.fsync = never`, the default). A process crash loses nothing. A
host crash can cost that replica the unsynced tail, which it then recovers from the group behind
its recovery gate, and a write acknowledged after the last `fsync` is lost only if a majority of
the group's voters crash at once. Spreading followers over data centers rather than AZs is likely
the stronger geographic guarantee, which favours the DO.

**Timing.** On deployed Workers an object's clock does not advance while JavaScript runs. It catches
up at a later, unpredictable I/O (`results/clockprobe*.json`). A span read on the object's own clock
therefore includes part of Pi's CPU: in the flat-out cells it reads 10 to 11 ms above the observer
on average. So each owner sends a mark to a second object, the observer, right before
`Storage.commit` and right after `await commit; await ctx.storage.sync()`, and yields
(`setTimeout(0)`) after each send. The observer is in the same colo and was checked not to share an
isolate with any owner: at the start of each re-run-2 cell, and at every 2 s drain in re-run 3, which
moves the owner to a fresh observer when the check fails. It stamps arrivals with `Date.now()`, so DO
samples have 1 ms resolution. The VFS numbers are `Storage.commit` wall time on Node's clock. A probe
ran the exact measured sequence (`timedSpan`) 100 times per combination on two objects
(`results/floorprobe3.json`). Around no work at all it read 0 ms p50 and at most 1 ms p90. Around a
one-row insert plus `sync()` it read 26 ms p50 on one object and 8 ms on the other, the same with 0,
11 or 22 ms of CPU before the sequence. The objects' own clocks read 27, 29, 33 ms and 9, 10, 12 ms.
How long `commit` takes to return without `sync()` is not measured: it is CPU only, below the
observer's resolution, and the object's clock does not move during it. On Node, B1 (0.11 ms p50) is
the non-durable reference.

**Durable commit latency**, ms, p50 / p99 (p99.9 / max), pi path, IAD. "Per object" gives the
median and range across objects of each object's own p50 and p99. The interval is a 95% bootstrap
over objects (2,000 resamples of the objects, all samples of a drawn object pooled). The VFS column is
from §9 (2026-10-09), measured on the same Pi workload but not at the same time.

| cell | DO durable commit | n | per object p50 | per object p99 | 95% interval, pooled p99 | VFS |
| --- | --- | --- | --- | --- | --- | --- |
| 1 db, agent pace, 64 objects, gaps 1.5 to 2.5 s (re-run 3) | 18 / 52 (190 / 6,494) | 95,058 | 18.5 (9 to 29) | 41 (17 to 122) | 37 to 67 | 4.7 / 7.9 |
| 1 db, agent pace, 64 objects, fixed 2 s (re-run 3) [3] | 19 / 43 (213 / 5,506) | 78,871 | 19 (9 to 29) | 30 (21 to 257) | 34 to 64 | 4.7 / 7.9 |
| 1 db, agent pace, 8 objects, fixed 2 s (re-run 2) | 20 / 68 (210 / 641) | 12,000 | 19 (11 to 28) | 33.5 (25 to 146) | 32 to 113 | 4.7 / 7.9 |
| 1 db, flat out, 8 objects (re-run 2) | 20 / 41 (167 / 2,955) | 148,130 | 19.5 (18 to 28) | 41 (30 to 75) | 35 to 53 | 4.4 / 7.0 (51 / 144) |
| 16 dbs, flat out (re-run 2) [1] | 19 / 46 (180 / 723) | 315,291 | 20 (10 to 20) | 46 (28 to 56) | 41 to 49 | 8.1 / 28.4 (58 / 199) |
| 128 dbs, flat out (re-run 2) [2] | 19 / 63 (205 / 5,335) | 2,561,957 | 19 (10 to 29) | 51 (19 to 143) | 60 to 67 | not comparable |

[1] The VFS ran its 16 databases as 16 processes on one client node, at about 70% of its 8 vCPU.
On a 6-CPU client pod on 2026-10-03, B1 fell from 115 to 41 commits/s per database and its p99 rose
from 8.9 to 13.1 ms at 16 databases, so the VFS row includes client scheduling delay, mostly in
p99. The DO's 16 owners ran in 16 isolates.
[2] The VFS ran 64 databases on each of two client nodes. On 2026-10-03 both were saturated. The
§9 run did not record their CPU, but each database managed 16 commits/s against 34 at 16 databases. Run 1 of the DO's 128-owner cell read
18 / 67 (216 / 3,035), but two of its owners lost 51% and 35% of their marks to a broken stub (below).
[3] Recorded from 30 to 522 s. The driver's laptop went to sleep 522 s into the cell, and the owners
stopped 60 s later, as designed when no drain arrives. The records up to then are complete: at least
99.6% of each owner's commits are on the observer.

The observer column is reported as measured. Three variants move p99 by at most 1 ms in every
cell: dropping samples under 5 ms (a durable commit cannot take less; 0 ms readings are 0.03% to 1%
of samples, and in the flat-out cells 74% to 94% of them come in runs of two or more consecutive
commits, so they are bursts of marks delivered together rather than single late marks); replacing a
high sample by the owner's own-clock span only when the next commit shows the paired artifact of a
late end mark (next start within 2 ms, next sample under 5 ms); and excluding windows in which the
observer shared an isolate with an owner. The paired correction lowers two maxima: 2,955 to 1,194 ms
(1 db flat out) and 5,335 to 4,839 ms (128 dbs).

Per object, the largest commit at the median object is 116 ms at agent pace with fixed gaps (range
28 to 5,506 ms over the 64 objects), 191 ms with jittered gaps (84 to 6,494), 513 ms for 1 db flat
out (217 to 2,955), 419 ms at 16 dbs (243 to 723) and 466 ms at 128 dbs (174 to 5,335). An object in
the 1-db flat-out cell has about 18,500 samples. The VFS's single database had about 48,000, with a
maximum of 144 ms.

**Where the DO's tail comes from.**

- *The first commit of a turn.* By position in the turn, agent pace on 64 objects reads 21 / 113,
  18 / 38, 18 / 33, 17 / 33 and 21 / 42 ms with jittered gaps, and 21 / 131, 18 / 34, 18 / 34,
  18 / 32 and 21 / 38 ms with fixed gaps. That first commit follows 1.5 to 2.5 s of idle. Flat out,
  the five positions read 19 to 22 ms p50 and 32 to 59 ms p99 (1 db).
- *A 10 s cycle per object.* We picked, for each object, the 750 ms slot of a 10 s cycle that held
  most of its commits of 80 ms or more in the first half of the cell, and counted the second half.
  With fixed 2 s pacing, 42 of the 46 slow commits fell in the slot on 8 objects and 99 of 123 on 64
  objects, while the slot holds 6.5% and 9.6% of all commits. With jittered gaps (64 objects) it was
  64 of 251 against 6.9%. Flat out it was 40% against 6.9% (1 db) and 34% against 6.7% (128 dbs).
  Fixed pacing hits the same phase every fifth turn, so an object either keeps landing in its slot or
  never does: per-object p99 runs from 21 to 257 ms with fixed gaps on 64 objects, against 17 to 122
  with jittered gaps. With 8 objects the pooled p99 depended on which objects landed there (per-object
  p99 25 to 146 ms). The cycle matches the 10 s batch period Cloudflare describes, but we did not
  verify the link.
- *The object.* Per-object p50 ranges from 9 to 29 ms in the same colo, and the floor probe's two
  objects read 8 and 26 ms for a one-row commit. Objects differ, not just samples.
- *Shared isolates.* At 128 owners, 37 owners spent time in 19 isolates that held 2 owners each.
  Their median per-object p99 was 70 ms against 42 ms for owners alone, at the same p50 (18 against
  19). The yield after the start mark lies inside the measured span, so a co-resident owner's CPU can
  land in it; part of that gap may be the instrument. At agent pace on 64 objects, 4 owners shared 2
  isolates (jittered gaps) and 10 shared 5 (fixed gaps), with no p99 difference (40 against 41, and
  31.5 against 29.5).

**Throughput**, Pi commits/s from the owners' own counters over 30 to 630 s, pi path, durable.

| cell | DO per owner, median (range) | DO total | VFS |
| --- | --- | --- | --- |
| 1 db, flat out (8 objects) | 31.0 (26.9 to 33.8) | 247 | 81 |
| 16 dbs, flat out | 32.2 (28.7 to 45.2) | 525 | 541 |
| 128 dbs, flat out | 32.5 (24.2 to 48.4) | 4,267 | 2,051 [2] |

DO throughput falls within a cell. With 1 db flat out it goes from 42.1 commits/s in the first
minute to 24.2 in the last half minute, while commit p50 stays at 20 ms. The owner's time between
commits on the observer (Pi's CPU, two mark sends, two yields and any mark delay) grows from 2.3 to
19.8 ms on average. Pi rereads its growing transcript from the object's SQLite (re-run 2 read 11.25
billion rows). B1 sustains 115 commits/s on Node over 14.4k turns. So DO commits/s measures Pi's CPU
on Workers more than storage, depends on cell length, and is not a storage comparison.

**As shipped**, turn level, 8 objects flat out, re-run 3: each turn's writes are confirmed before its
model calls and before its reply (output-gate semantics). Pi does not wait for each commit. The VFS
has no equivalent mode: every VFS commit waits for its append. "Turn" is turn start to the end of the
final `sync()`, on the observer.

| path | commits/s per owner, median (range) | total | turn p50 / p99 (p99.9 / max), ms | turns |
| --- | --- | --- | --- | --- |
| Pi on PiHarness's DO SQLite adapter | 57.6 (38.4 to 64.8) | 428 | 87 / 210 (326 / 2,616) | 51,440 |
| Shipped `PiHarness.submit` / `wait` | 22.4 (21.0 to 32.6) | 188 | 215 / 397 (1,003 / 3,582) | 22,540 |

The shipped path, which adds a Lifecycle wake job and an alarm to each turn, runs at about 39% of
the pi path's commit rate per owner. On the shipped path the owners' own clocks agree with the
observer within 20 ms on 94% of turns (own clock 214 / 430 (1,125 / 3,697)). On the pi path they
cannot check it: the observer reads more than 20 ms above the own clock on 56% of turns and more
than 20 ms below it on 5% (own-clock p99 1,245 ms), so the pi path's p99.9 and max are unverified. A
turn under 10 ms is impossible here (at least two confirmed syncs), so its start mark arrived late:
48 turns on the pi path and 4 on the shipped path. Dropping them and the turn before each gives
87 / 210 (323 / 826) on the pi path and leaves the shipped path unchanged. Re-run 2's pi-path cell,
on other objects, read 81 / 234 (478 / 888) at 60.7 commits/s per owner.

**Cold start** (run 1, not re-run). Time to the first answer of a call that opens Pi and reads the
root transcript after `ctx.abort()`, from a client object in IAD, minus that client's warm no-op
call (4 to 8 ms). Where the object came back was not recorded, and we could not make it restore on
another host, so this is not comparable to the VFS cold start (a fresh host installing a snapshot
and replaying the tail). Each abort was confirmed by its error. That the next call ran on a new
instance is proven only for the first repetition.

| database | first restart after the bulk load | next two restarts |
| --- | --- | --- |
| 10.5 MB | 348 ms | 81 / 81 ms |
| 103 MB | 1,916 ms | 86 / 112 ms |
| 1.03 GB | 2,084 ms | 74 / 73 ms |

A full scan of the 1 GB table took 1,799 ms wall and 1,519 ms CPU in one invocation (from `wrangler
tail`; the object's own clock read 0 ms).

**Incidents.** No commit or `sync()` threw and no turn failed in any cell.

- *Owner resets* (the object restarted and lost its loop): 0 in the 8- and 16-object re-run-2 cells;
  2 and 4 in the two 128-owner runs; 1 in each re-run-2 native cell. In re-run 3: 6 at agent pace
  with jittered gaps (50 to 442 s into the cell), 2 with fixed gaps, 0 in the pi-path native cell
  and 1 in the shipped-path native cell. A stall is the first acknowledged commit (in the native
  cells, turn) after a reset minus the last one before it. It includes our driver noticing (drains
  every 2 s) and restarting the owner, so it bounds the platform's share from above: 0.9 to 2.5 s at
  128 owners; 1.6 and 8.9 s (re-run 2) and 14.0 s (re-run 3) in the native cells; 2.6 to 24 s at
  agent pace with jittered gaps and 2.8 and 5.1 s with fixed gaps. One more agent-pace stall of
  397 s was a drain request that hung after its object reset; every request now has a 60 s timeout.
  At most 2 owners reset at the same moment (128 owners, run 2, 608 s). In run 1 (withdrawn), all 16
  objects of one 16-owner run reset together; that run started 4 minutes after a deploy, which may
  have caused it.
- *Long commits without a reset.* The longest single commits in re-run 3 were 6,494 ms (jittered
  gaps) and 5,506 ms (fixed gaps). Neither object was reset, and their own clocks agree (6,504 and
  5,508 ms).
- *Commits that never finished on the observer:* 7 and 19 in the two 128-owner runs. 2 and 4 were
  in flight at an owner reset, 1 and 8 were near an observer reset, and the rest are end marks that
  never arrived while the owner carried on (harness v2 did not record failed sends). In re-run 3: 1
  at agent pace with jittered gaps, at an observer move; 7 with fixed gaps, 6 of them in flight when
  the driver stopped draining at 522 s and 1 at an observer reset.
- *Observer resets* (an idle object with no storage): 1 (1 db flat out), 5 and 4 (128 owners), 1
  (re-run-2 shipped path); in re-run 3, 5 at agent pace with jittered gaps and 3 with fixed gaps (all
  three at the same moment). Harness v3 also moved 2 owners off an observer that shared an isolate
  with an owner.
- *Lost marks.* Harness v2 swallowed a failed mark and kept the broken stub. Owner 5 of the re-run-2
  shipped-path cell lost 80% of its turns that way, and owners 30 and 73 of the first 128-owner run
  lost 51% and 35%. Those cells are withdrawn or superseded. Harness v3 renews the stub and counts
  failures: 1 failed send in re-run 3 (a turn-end mark 596 s into the pi-path native cell, "Network
  connection lost"); the renewed stub delivered every later mark. Every re-run-2 and re-run-3 cell in
  the tables has at least 99.5% of each owner's commits (or turns) on the observer.
- *Platform errors* (GraphQL, bench namespace). Re-run 2: 384 `clientDisconnected` (270 during
  probes and smoke tests at 20:50Z, 84 in the first 128-owner run, 9 between the native cells, 9
  alarms in the shipped-path cell, 12 in the second 128-owner run) and 2 `scriptThrewException` (one
  RPC during deploys at 20:40Z, one alarm at 21:55Z in the shipped-path cell, the same 5 minutes as
  its owner reset). These were read by the teardown audit and are no longer queryable: analytics for
  a deleted namespace disappear. Re-run 3, read before its teardown: 19 of 880,521 invocations,
  none from the durable cells. 15 alarms ended `clientDisconnected`: 2 during the shipped-path smoke
  test (00:16Z), 4 during the shipped-path cell and 9 around its end (00:50 to 00:51Z); pi-path
  objects run no alarms. 3 RPCs ended `clientDisconnected` in the minute of the failed mark send
  above (00:39Z), and 1 RPC ended `scriptThrewException` in the minute of the shipped-path owner
  reset (00:45Z).

**Cost.** Re-run 2: 16.2 million DO requests, 79.6 million rows written, 11.25 billion rows read and
291,315 s of active time, read from GraphQL after the data settled. Run 1 had used 76.3 million rows
written of the monthly 50 million included, so re-run 2's rows written cost about $80 and its
requests about $2. Re-run 3: 0.88 million DO requests, 7.57 million rows written, 0.79 billion rows
read, 97,031 s of active time and 159 MB of peak storage, so about $7.6 for rows written and $0.13
for requests. Rows read over all runs (at least 21 billion) and duration stay within the included
amounts. Run 1 and re-run 2 together cost about $108, and all three runs about $116. These are
estimates from the published rates; the OAuth token cannot read billing.

**Reproduce.** Code in `do-bench/` (to be moved into the repo).

- Worker: `worker/src/index.ts` (harness v3; v2 in `worker-v2/`, v1 in `worker-v1/`) and
  `worker/wrangler.jsonc`. Packages pinned in `worker/package.json`: `agents` 0.26.0,
  `@earendil-works/pi-durable` 1.0.0, `@earendil-works/pi-ai` 1.0.0, `@earendil-works/chord` 1.0.0,
  `typebox` 1.3.27, `wrangler` 4.147.0. Node 22.22.1 on the driver.
- Deploy: `cd worker && npx wrangler deploy`, then `npx wrangler secret put BENCH_TOKEN` with the
  value in `.token`. Re-run 3 ran on version 1b08d4cf-5d1a-4e5a-a7cd-081517f6dfff, re-run 2 on
  b62c11f1-f26e-4089-9a47-27af16de08a7.
- Cells: `node cell3.mjs <label> <owners> <turnMs> <durable|native> <pi|harness> 630 30 <gateModel>
  <jitterMs> <staggerMs>`. Re-run 3 is `seq6.sh` and `seq7.sh`; re-run 2 is `seq4.sh` and `seq5.sh`
  with `cell2.mjs`. Floor probe: `node probe6.mjs`. Keep the driver machine awake for the whole cell
  (one re-run-3 cell lost its last 108 s to a laptop sleep).
- Analysis: `python3 analyze3.py <cell...>` writes `results/<cell>/summary3.json` (all cells:
  `run3/analyze3-final.txt`); `python3 zerobursts.py <cell...>`. Raw data:
  `results/<cell>/owner-<i>.json.gz` (every mark with the observer's arrival time, every own-clock
  commit and turn record, every drain) and `results/<cell>/config.json` (worker version, isolates,
  observer moves). Usage and errors: `run3/gql3.py` (`results/gql3-final.json`, `gql3-minute.json`);
  it must run before the teardown.
- Teardown: `teardown/run3.sh` deploys a stub whose migration v2 deletes the class and its data,
  deletes the Worker, and compares read-only account snapshots taken before and after
  (`teardown/snapshot.py`). Everything created is listed in `INVENTORY.md`.

**In short.**

- Waiting for a confirmed write costs about 18 to 20 ms at p50 in a DO in IAD, against 4.7 ms on
  the VFS at agent pace, so about 4x.
- The DO's tail is wider. At agent pace on 64 objects its p99 is 52 ms with jittered gaps (95%
  interval 37 to 67) and 43 ms with fixed 2 s gaps (34 to 64), against 7.9 ms on the VFS. The
  slow commits are mostly the first commit of a turn and a 10 s cycle per object, and single commits
  reached 5 to 6.5 s.
- The DO's latency depends on the object: per-object p50 runs from 9 to 29 ms in one colo.
- Flat out, a DO owner manages 31 durable commits/s against 81 on the VFS, and its rate falls as
  Pi's transcript grows. That is Pi's CPU on Workers, not storage.
- As shipped, Pi on a DO does not wait for each commit, so it runs at 58 commits/s per owner with
  each turn confirmed before its model calls and its reply; through `PiHarness.submit` it runs at
  22. The VFS has no such mode.
- The DO confirms writes on 3 of 5 followers in different data centers. That is likely a stronger
  geographic guarantee than the VFS's 2 of 3 AZs.
