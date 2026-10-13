# SQLite on Ursula: the replicating VFS

The VFS runs an unmodified SQLite application (Pi Durable's official node `SqliteStorage` is the
reference user) with an Ursula stream as the source of truth. A commit is durable once the stream
acknowledges it. The local files are a non-durable cache of the stream (§6).

Code: `clients/sqlite-vfs` (the loadable extension, Rust, a standalone workspace) and
`clients/sqlite-ursula` (TypeScript: the loader and `attach` in the root entry, which has no runtime
dependencies, the Pi helper `openUrsulaPiStorage` in the `/pi` entry, whose
`@earendil-works/pi-durable` is an optional peer dependency, the e2e suites and the benchmark tools). User docs: `docs/web/src/content/docs/pages/examples/sqlite-vfs.mdx`.

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
- The local file's position in the stream is the sidecar `<db>-ursula`. It holds the stream offset
  after the last frame the file reflects, the owner's epoch, the sidecar's format (`v=2`), the log
  since the latest snapshot in bytes (§4.1), the kernel boot id it was written in, the stream's
  path and its `Stream-Incarnation`, the db file's inode, and its claim on the local WAL
  (generation and last commit frame, §6). It is replaced atomically against a process crash (temp
  file, then rename, no fsync).
- Offsets are opaque. The VFS keeps every offset as the string the server wrote
  (`Stream-Next-Offset`, `Stream-Snapshot-Offset`, `Stream-Retained-Offset`), compares offsets only
  as strings (the protocol orders them lexicographically), and never computes one. `-1`, the
  protocol's "beginning of the stream", also stands for "none" (no snapshot, no local state). A
  read without `Stream-Next-Offset` is an error. `ursula_attach`, `ursula_status`, `ursula_stats`
  and the TypeScript API return offsets as these strings.
- One owner per file per host: attach takes `flock` on `<db>-ursula.lock` for the process
  lifetime, and refuses while any connection to the file is open, a writable descriptor of this
  process outside the extension included (§3, step 1).
- A db file with a sidecar is the cache of a stream. It opens through the VFS only while an attach
  of it has succeeded in this process. Before that (a process that never attached it, including a
  restarted one) opening it fails with `SQLITE_CANTOPEN`. Passed through to `unix`, its commits
  would never reach the stream. A file without a sidecar passes through, unless its last attach in
  this process failed: after a failed attach the path opens through the VFS only once an attach
  succeeds, sidecar or not. An attach refused up front (connections of the VFS open, another thread
  attaching, another process holding the file, a writable descriptor outside the extension)
  changes nothing, and is no failed attach.

### Errors

Every failure has a kind, a stable token that callers branch on instead of the message's text, and
each kind one action. `Error::kind` maps every error, `Error::poison_kind` the reason of a poison,
and `WriteRefusal::error_kind` a write refused before it reached the stream, all in `error.rs`.

- A SQL function's error message is `<function>: <kind>: <message>`, such as
  `ursula_attach: invalid_name: head http://h/s2/kv1: 400 ... bucket_id must be 4 to 64 bytes, got
  2 bytes`. The result codes are unchanged: `SQLITE_ERROR`, `SQLITE_AUTH` for an attach refused
  with `402`, `SQLITE_IOERR_AUTH` for one refused before it reached the stream in another way.
- `ursula_status` of an attached file has `error`: `null` while writes go through, otherwise
  `{"kind", "action", "message"}`. A poisoned file reports `fenced` and `stop` once a fence was
  seen, and otherwise its first reason's kind and that kind's action, with the first reason as the
  message (the `reason` field's text). A write refused before it reached the stream reports the
  refusal's kind and action (`refused_kind` says what refused it).
- `ursula_status` of a path that is not attached fails. Its kind is `not_attached` for a path
  never attached in this process, and `attach_failed` for one whose last attach in this process
  failed, followed by that failure's kind in a fixed place:
  `ursula_status: attach_failed: <kind>: <path> is not attached: its last attach failed
  (<message>)`. `attach_failed` has no action of its own: the wrapped kind's action applies.
- A file method cannot choose SQLite's message, so a commit or read that fails names nothing by
  itself: the caller reads `ursula_status` right after it. A commit refused before it reached the
  stream fails with `SQLITE_IOERR_AUTH` (7178), and the file stays usable. A poisoned commit fails
  with `SQLITE_IOERR_WRITE` (778), and `error.kind` tells an unknown outcome from a fence. A read
  transaction of a fenced file fails with `SQLITE_IOERR_READ` (266, §3, fencing). An open refused
  because the path is not attached fails with `SQLITE_CANTOPEN` (14).

| kind | what failed | `action` |
| --- | --- | --- |
| `invalid_argument` | an argument of a SQL function (an attach mode, a NUL byte in a path) | `fix` |
| `invalid_url` | the URL does not parse, or answers with a redirect the extension does not follow | `fix` |
| `invalid_name` | the server answered the stream's `HEAD` or create with `400`, refusing its bucket or stream id | `fix` |
| `not_found` | the stream is missing (or hidden from the credentials) | `fix` |
| `in_use` | connections of the VFS open, another thread or process attaching or holding the file, or a writable descriptor of this process outside the extension (§3, step 1) | `fix` |
| `unreplicated_file` | the db file holds data but has no readable sidecar: it was never replicated (§3, step 2) | `fix` |
| `other_stream` | the local files are the cache of another stream | `fix` |
| `too_large` | a commit's frame is over the frame limit, or a vouched `413` refused it: the same transaction never goes through (§7) | `fix` |
| `unsupported` | the application left WAL mode, changed the page size, or used `locking_mode=EXCLUSIVE` | `fix` |
| `stream_state` | the stream holds or answers something the extension cannot follow (an undecodable frame, lost acknowledged data, no incarnation) | `fix` |
| `not_attached` | the path was never attached in this process | `fix` |
| `unreachable` | no answer: no lookup, no connection, a timeout, or a write whose every attempt failed before it went out | `retry` |
| `unavailable` | answered `429`, `408`, `307` or 5xx until the retry budget ran out | `retry` |
| `unauthorized` | `401`, or a `403` that is no fence, on a request that applies nothing | `retry` |
| `refused` | a write refused before it reached the stream (§2) | `retry` |
| `outcome_unknown` | an append whose outcome is unknown: no answer within the retry budget, or an answer that does not prove that nothing was applied (§2) | `reattach` |
| `foreign_write` | a connection outside the extension committed to the local files (§3, step 1) | `reattach` |
| `local_damage` | a page read back other bytes than written, or an fsync failed (§6) | `reattach` |
| `local_state` | an I/O error on the local files | `reattach` |
| `internal` | a panic in the extension, or a missing routine of the host's SQLite | `reattach` |
| `fenced` | another owner claimed the stream, it was recreated, or a writer outside the protocol appended | `stop` |
| `attach_failed` | the path's last attach in this process failed | the wrapped kind's |

The actions:

- `retry`: the same handle works again later, since nothing reached the stream. A refused commit
  leaves the file usable, and the next commit is sent as usual.
- `reattach`: close the file's connections and attach it again. The attach replays what the stream
  holds, and rebuilds local files that a poison found damaged or written outside the extension
  (§6, distrust).
- `stop`: another owner holds the stream. Stop writing. Attaching again takes the stream back from
  that owner, which only a deliberate takeover does.
- `fix`: the request cannot succeed as made. Fix the URL, the name, the setup, the files or the
  transaction, then try again. Nothing tells a caller to remove a file that may hold data: an
  `unreplicated_file`'s message says how to copy its rows into a newly attached file.

The three ways a commit fails: refused before it reached the stream (7178, `refused` or
`unreachable` or `too_large`, file usable), an unknown outcome (778, `outcome_unknown`, poisoned,
`fenced: false`), and fenced (778, `fenced`, poisoned, `fenced: true`). A poison is never retried on
the same handle, so a poison whose reason is of a `retry` kind (a re-claim's read that the endpoint
put off) reports `outcome_unknown`. An append's `403` that is neither the fence nor vouched, and a
`401` after an attempt that may have reached the stream, are `outcome_unknown` themselves, for a
commit and for an attach's claim alike.

## 2. Writes

WAL writes of an attached database go to a per-transaction overlay, which reads and the file size
see. The write of the commit frame's page data (the frame whose header has a non-zero "db size
after commit") is the commit point. The transaction's final page images become one commit frame,
appended with the idempotent producer: `Producer-Id` `sqlite-ursula-vfs/<Stream-Incarnation>`,
`Producer-Epoch` per owner, `Producer-Seq` per append. The incarnation is an opaque token compared
for equality only, and the owners of one incarnation fence each other by epoch. Every request
after attach's first `HEAD` (appends, reads, snapshot and retention `PUT`s, snapshot `GET`s) also
carries the incarnation as the `Stream-Incarnation` request precondition. The server checks it
atomically with the request (an append at Raft apply), so a stream recreated at the same path
refuses everything of an owner of the deleted one with `412` (§6, wrong stream).

### Outcomes

A commit ends in one of three ways. It is **acknowledged**. It **fails alone**: SQLite gets
`SQLITE_IOERR_AUTH` (an extended code SQLite reserves for extensions), rolls the transaction back,
nothing of it reaches the local WAL, and the database stays usable. Or it **poisons**: SQLite gets
`SQLITE_IOERR_WRITE`, rolls the transaction back, nothing of it reaches the local WAL, and the
database refuses every write until it is attached again.

| answer to an append attempt | outcome | `refused_kind` |
| --- | --- | --- |
| 2xx with a `Stream-Next-Offset` past the owner's offset | acknowledged | |
| 2xx without one, or with one not past the owner's offset | poison | |
| a duplicate (`204`) answered without its receipt | fenced, poison | |
| no answer after the request went out (timeout, reset), a `307`, a 5xx without a vouched marker | unknown: retried, poison when the budget runs out | |
| an attempt that never went out (no lookup, no connection, failed TLS handshake) | retried, fail alone when the budget runs out | `unreachable` |
| `401` | retried with the token read again, fail alone when the budget runs out | `unauthorized` |
| `429` | retried, fail alone when the budget runs out | `rate_limited` |
| a vouched 5xx | retried, fail alone when the budget runs out | `not_applied` |
| `402` | fail alone at once | `payment_required` |
| a vouched `403` | fail alone at once | `forbidden` |
| a vouched `413` | fail alone at once | `too_large` |
| any other vouched 4xx | fail alone at once | `not_applied` |
| `403` with `Producer-Epoch` | fenced, poison | |
| any other 4xx, a redirect other than `307` | poison | |
| (a frame over `URSULA_VFS_MAX_FRAME_BYTES`, §7, never sent) | fail alone | `too_large` |

"Vouched" means the answer carries both `x-ursula-not-applied` and `x-ursula-not-applied-checked`
(below). A commit fails alone only while none of its attempts can have reached the stream. After
an attempt whose outcome is unknown, refusals say nothing about it: a `402` is retried like an
unknown outcome until an answer settles it (the duplicate's receipt once the layer forwards the
append again), a vouched `403` poisons, and a budget that runs out on refusals poisons as unknown.

- **Acknowledged.** The overlay goes to the local WAL, later writes of the transaction (checksum
  rewrites of spilled frames, padding) go straight to it, and when the write transaction ends the
  sidecar advances. Nothing is fsynced, except the db file before a commit that starts a new WAL
  generation (§6). The new `Stream-Next-Offset` becomes the owner's offset. The VFS does not check
  where its frame landed, which would need offset arithmetic.
- **A duplicate without a receipt** is never this owner's own retry: the owner is the only writer
  of its producer at its epoch, retries only its newest append, and the server never evicts a
  producer's newest receipt. So another writer holds the producer past this sequence.
- **Outcome unknown.** The attempt is retried with the same sequence for up to
  `URSULA_VFS_RETRY_MS` (30 s), no sooner than any `Retry-After`. The server deduplicates.
- **Redirects** are never followed, by any request. Followed, a request would go out without the
  token, and a `301`, `302` or `303` would turn an append into a `GET`. Ursula's gateway follows a
  server's leader redirect itself and answers `503` with `Retry-After: 1` when it finds no leader
  within three redirects and 5 s, so a `307` comes from a node the URL names directly or from
  another layer. The VFS sends it again to the same URL like a `503`. For an append it is an
  unknown outcome, because a leader that loses its leadership answers so after it may have
  proposed the write. Any other redirect fails the write and the error names its `Location`, a
  `308` from a proxy that redirects `http://` to `https://` among them (§7).
- **401.** The token is read again before each retry (§7). A token that expires refuses the commit
  once the budget runs out, and the application commits again after it sets a fresh one.
- **403.** Only the server's fence carries `Producer-Epoch`. A `403` without it is an authorizer's
  denial: it fails the commit alone when vouched, and poisons otherwise, because a filtering proxy
  (a web application firewall that inspects responses) can answer `403` in place of the stream's
  own answer after the stream applied the write. One classifier, `refused`, tells these apart.

### Refused before it reached the stream

One function, `client::applied_nothing`, decides for every answer whether it proves that the
attempt applied nothing. A `401`, `402` or `429` does by its status alone. Any refusal (a 4xx or
5xx) does when it carries both the marker `x-ursula-not-applied: <reason>`, which says the request
was refused before it reached the stream and names why, and the vouch
`x-ursula-not-applied-checked`, which says a gateway checked the marker. Any other answer proves
nothing: a `403`, `413` or `503` with only one of the two headers, and a redirect or a success
whatever headers it carries.

An attempt that failed before any byte of it went out applied nothing too: its host did not
resolve, or no connection to it was made, the TLS handshake included. The client's resolver and
connector mark the failures they return (`net::NotSent`), so this is decided by where the failure
happened, not by its kind. A failure on a connection that was made, a pooled one included, stays an
unknown outcome.

A commit that fails alone leaves the owner its epoch, producer sequence and offset. Reads go on
from the local files. The next commit is sent as usual, with the sequence the refused append never
used, and is acknowledged once the refusals stop, without a new attach. `ursula_status` reports
`writes_refused`, `refused_kind` (the table above) and `refused_reason` until a commit is
acknowledged or the database is poisoned. `refused_reason` is the refusing layer's response body
(its first 256 bytes followed by `...` when it is longer), or the marker's value when the body is
empty. It is always the marker's value for `not_applied`. For `too_large` it is the frame's size
and limit, or the marker's value of the `413`. For `unreachable` it is the last attempt's failure.
A `402` also shows in `payment_required` and `payment_reason`. The extension logs
`writes_refused` (`payment_required` for a `402`) when refusals of a kind start, and
`writes_resumed` at the first acknowledged commit after them.

A layer's refusal is recorded as `layer_refused`, and while it stands no snapshot is taken (§4.3).
A transaction over the frame limit is refused unsent (`Append::Unsent`, `Db::refuse_unsent`). It
leaves `layer_refused` as it was and neither sets nor clears the next-commit check below, since the
stream cannot hold a frame that was never sent. A vouched `413` also leaves `layer_refused` as it
was, but sets the check like any refusal a layer answered.

### The requirement on layers in front of the stream

The VFS takes these answers to mean that the attempt applied nothing, and cannot check it. Every
layer in front of the stream must answer `401`, `402` or `429`, or send a vouched marker, only on
requests the stream did not apply: it refuses them before it passes them on, or relays such an
answer from the layer behind it. A layer that passes an append on and then answers so (a `402` from
metering after the response, say) breaks the rule. So does a layer that sends one request upstream
more than once, because it retries or follows a redirect, and relays such an answer to a later
exchange, since an earlier exchange may have applied the request.

The rule for deployments: **turn off retries and redirect following for writes** in every proxy,
load balancer and service mesh in front of Ursula, on the routes to the gateway and on those from
the gateway to the servers. Some service meshes retry a `503`, POSTs included, unless a route says
otherwise (in Istio, `retries: attempts: 0`).

When the rule breaks, the stream holds the refused transaction, which the local file lacks, and
the next commit goes out with the producer sequence the refused one used.

### The next-commit check

The server answers a fresh producer append `200` and a duplicate `204` with the receipt of the
append it duplicates. The next commit after a refused one goes out with the same epoch and
sequence, so when the refused commit was applied after all, the server answers the next commit as
its duplicate. A `204` while no earlier attempt of the commit can have reached the stream is a
duplicate none of its attempts explains, and at the sequence a refused commit left free the VFS
poisons the database (`reason`: a commit refused as never applied may have been applied after all)
instead of acknowledging pages that may never reach the stream. The reason names both commits, and
the attach that follows replays whichever the stream holds. When it is the refused transaction, the
database then holds it although its commit failed with `SQLITE_IOERR_AUTH`. So a false refusal
usually poisons at the next commit instead of diverging.

The check has known gaps:

- **An earlier attempt of the next commit proved nothing.** When an attempt of the next commit got
  no answer, or an answer that proves nothing (a timeout, a `307`, a 5xx without a vouched marker),
  before the `204`, the `204` is taken for that attempt's own duplicate, as every retry's is. The
  commit is acknowledged without its pages and the database diverges from the stream with no
  error. The VFS logs `refused_commit_unchecked`, with the commit and its receipt, when it
  acknowledges a commit that way at the sequence a refused commit left free.
- **The owner attaches again first.** After a restart, or when the refused commit was the process's
  last, the attach replays the refused commit into the database with no error, and no later commit
  meets its sequence. An application that runs the refused transaction again applies it twice.
- **The producer expired** before the next commit (7 idle days): the re-claim finds the refused
  commit past the owner's offset (`producer expired and the stream moved past ...`). When the
  refused request was a re-claim's claim, the owner keeps its epoch and its next commit meets the
  epoch the refused claim took (`superseded`). Both poison, and `ursula_status` reports `fenced`
  with no second owner behind it.
- **The commit's own `204`.** A gateway follows a leader redirect that a server can send after it
  proposed the write, and the new leader answers a later exchange as the duplicate of the first.
  At a sequence no refused commit used that is the only explanation, so the commit is acknowledged.
  At the sequence a refused commit left free it poisons too, which costs an attach but loses
  nothing.

### What Ursula's gateway and server guarantee

Ursula's own components meet the requirement.

- **The gateway** answers `401`, `403` and `429` before it forwards a request, marks every refusal
  it makes before forwarding, and vouches for every answer it returns, after removing any vouch
  from upstream. It follows leader redirects, which a server can send after it proposed the write,
  so it keeps the final answer's marker only if every redirect carried the marker too. On the same
  condition it marks its `503` for a node it could not connect to, and its `503` when it finds no
  leader.
- **The server** never vouches, so its markers count only behind the gateway, and without a
  gateway only a `401`, `402` or `429` proves anything. It marks writes its ingress admission
  refuses (`503` with `RaftLogPressure`, `WalDiskPressure` or `IngressBodyBytesLimitReached`, and
  `413` with `RequestBodyTooLarge`), and a create, append, close or delete it refuses before it
  proposes the write (`503` with `NotLeader`, `GroupNotHosted`, `RaftUncommittedBackpressure` or
  `ColdBackpressure`, and a redirect with `NotLeader` or `GroupNotHosted`). It never answers `401`
  or `402`, answers `403` only as the fence, and `429` only to a new producer over the stream's
  producer limit. Neither applies anything.

A `401`, `402` or `429` counts by its status alone, so the gateway's redirect rule does not guard
it. It is safe because a server's only `429` refuses the first append (sequence 0) of a producer
the stream does not hold, and a replay of an append whose first exchange was committed is answered
as its duplicate (`204`), never `429`. So no layer between the gateway and the servers may answer
`401`, `402` or `429`. A third-party rate limiter or authorizer there is unsupported.

### Foreign writers

Every commit carries `Stream-Seq`: the owner's epoch and producer sequence, each zero-padded to 20
digits. Owners claim ever higher epochs and number commits upwards within one, so each commit's
`Stream-Seq` is above every earlier commit's, retries included (a retry is answered as a duplicate
before `Stream-Seq` is checked). The server refuses an append whose `Stream-Seq` is not above the
stream's last one (`409`). A writer outside this protocol that appended with a higher one since the
owner's last commit therefore fences and poisons the owner at its next commit, and one with a lower
or equal `Stream-Seq` is refused itself and never lands. One that appends without `Stream-Seq`
goes unnoticed: its bytes fail every replay that crosses them (the frames no longer decode, so
attach refuses) until the owner publishes a snapshot past them.

### Overlay lifetime and non-WAL writes

The overlay belongs to the write transaction. It is cleared whenever the WAL write lock is taken or
released, so a rolled-back transaction's spilled frames never shadow a later one's.

Any write to the main db file outside a checkpoint (a rollback journal, `journal_mode=MEMORY`) is
refused, because it would bypass replication. So is a commit whose page 1 leaves WAL format. With
the WAL kept (§6), SQLite reopens it and would commit `journal_mode=MEMORY` or `DELETE` through it,
leaving a stream whose rebuilt copies need a rollback journal to be written.

## 3. Attach and fencing

`ursula_attach`:

1. Takes the host lock. Refuses if a connection to the file is open in this process. Once the
   previous attachment's snapshot thread has closed its connection, it also refuses (`in_use`) while
   the process holds the db file open on a writable descriptor (`local::open_in_process`). It lists
   `/proc/self/fd` (or `/dev/fd` on macOS), keeps the descriptors whose access mode can write
   (`F_GETFL`), and of those the ones whose file name, as the kernel records it (`/proc/self/fd`
   links, `F_GETPATH` on macOS, the case ignored there), is the db file's. Only those are compared
   by device and inode, so no descriptor of an unrelated file is stat'ed, which on a hung network
   mount could block. No connection of the VFS is open then, so such a descriptor is outside the
   extension: a connection opened before the extension was loaded, when `unix` was the default VFS,
   one opened through another VFS, or any other writer. SQLite opens a connection's file read-write
   unless the connection is read-only, and a read-only one can neither commit nor checkpoint, so a
   read-only descriptor (a file watcher, a backup reading the file, a `mode=ro` connection) does not
   refuse the attach. The `unix` VFS shares its locks and wal-index with the VFS's own connections
   in the process, so no lock tells such a connection apart, and its commits would never reach the
   stream. The refusal changes nothing, like the other refusals of this step: a bound path keeps its
   binding, whose snapshot thread runs again, and the path is no failed attach.

   A connection outside the extension that opens later (`?vfs=unix`), or a process without the
   extension, writes the same WAL, so a commit it makes is caught later (`WalSeen`). The local WAL
   file changes only at a commit of the VFS (its writes before the commit point stay in the overlay)
   or when a connection of the VFS truncates it. So the database records the wal-index header a
   connection of the VFS leaves whenever it releases the WAL write lock, and the header the WAL file
   holds as of the VFS's last commit (what a recovery rebuilds from it, and after an attach that
   kept the WAL its claim, which the WAL's last commit frame meets exactly, §6). A header with
   frames that is neither is a commit no connection of the VFS made. It is checked whenever a
   connection of the VFS takes the WAL write lock, whenever it releases one it committed nothing
   under (a recovery rebuilt the header from the WAL file meanwhile), and before the snapshot thread
   copies pages (§4.2, after step 4). The header is read the way SQLite reads it without a lock:
   both copies, equal and initialized. After an attach that left no WAL, until the VFS's first
   commit, no checkpoint of the VFS writes the db file either, so the db file changing (its size,
   inode or modification time, `DbFileStamp`) is a checkpoint of a foreign commit whose WAL its
   writer deleted. Either poisons the database before any other commit is acknowledged, as local
   damage (`foreign_write`): no snapshot publishes it, and the next attach rebuilds the local files
   (§6, distrust).
2. Reads the sidecar of a file with content, then `HEAD`s the stream for its `Stream-Incarnation`
   (an opaque token that changes when the stream is deleted and recreated at the same path, and a
   stream without one is refused), before anything opens the file through SQLite. A file with
   content and no sidecar was never attached and is refused (`unreplicated_file`), since its pages
   may never have been in the stream: the message says to copy its rows into a newly attached file,
   or to move it aside to attach the path. A sidecar for another stream path is refused
   (`other_stream`). A refused `HEAD` carries the reason a `GET` of the stream gives (§3, reads). A
   missing stream is created for a file without content, and refused for one with content (§6, wrong
   stream), because the local files may be the only copy left. The local files are trusted when the
   sidecar was written in this boot, from this incarnation of the stream, for this db file, and the
   local WAL holds what the sidecar claims of it (§6). Anything else (another or unknown boot id,
   another incarnation, a sidecar of another format, a torn one, a replaced db file, a WAL behind or
   past its sidecar, a sidecar a poison distrusted) means the local files are discarded (§6) and the
   attach proceeds as on a fresh host. A sidecar of another incarnation is always discarded,
   whatever the recreated stream's length (logged as a recreate). For one of the same incarnation, a
   read at its offset first checks that the stream did not lose acknowledged data (§6, wrong
   stream).
3. Recovery, when it rewrites pages, never opens the local files through SQLite before replaying
   onto them. Trust (§6) says every page holds the state at the sidecar's offset or a later
   commit's, not that SQLite can read the file (a disk image may hold a torn page 1 that replay
   rewrites). Recovery locks other processes out with a POSIX write lock on the db file's lock
   bytes. Every SQLite connection on a WAL file holds a read lock there from its first read until
   it closes, so attach fails while one is open rather than rewriting pages under its cache. The
   lock is held from before attach first writes the file until it is done writing it, and the file
   keeps its inode throughout (a discard truncates it, a snapshot is written over it, §4.3). A
   connection that opened the path meanwhile gets `SQLITE_BUSY` until the lock drops and then reads
   the rewritten file. Within the process, opens of the main db through the VFS fail with
   `SQLITE_BUSY` while attach runs, and the path keeps its previous attachment until attach ends.
   Attach then folds the local WAL into the db file itself (the frames SQLite's recovery would read,
   up to the last commit, a later frame winning, the file cut to that commit's size), fsyncs it,
   rewrites the sidecar at the same offset with a `:0` claim, deletes `-wal` and `-shm`, and only
   then replays onto it. No stale WAL sits next to pages replay has moved past it, which a plain
   SQLite connection would otherwise checkpoint over them when it closes. An attach in the same
   boot after a crash midway trusts the files again and replays from the same offset (folded pages
   hold the state at the WAL's last commit, replayed ones later commits, and replay is idempotent).
   A crash between that sidecar and the delete leaves commits in a WAL the `:0` claim rejects,
   which costs a rebuild.
4. `HEAD`s the stream and compares its `Stream-Incarnation` with step 2's (a change fails the
   attach round, as in step 6). From here on every request carries step 2's incarnation as the
   `Stream-Incarnation` precondition. If the stream's latest snapshot is ahead of the file (a fresh
   host, or a file left below the retention), installs it (§4.3). Then replays frames to the tail.
5. Claims with the incarnation's `Producer-Id` (§2): appends a claim at (epoch = highest seen + 1,
   seq 0) and reads it back. A 2xx alone proves nothing, because two owners claiming the same epoch
   both get one, the second as a duplicate. So the claim counts only if the frame ending at the
   answered offset is ours (the nonce makes it unique). Offsets are opaque, so its start is not
   computed from that offset: the reads run from the tail step 4 reached (a frame boundary at or
   before the claim) to the answered offset, and the frames read are decoded. When the reads run
   past the answered offset (another owner appended meanwhile, and the offset's place in the bytes
   is unknown), our claim being among the frames at all suffices: the server applies one append
   per (producer, epoch, seq 0) and answers every other with its receipt, so our claim is in the
   stream only if the answer was its own end. Lost: epoch + 1. `403`: the server's epoch + 1.
6. Replays up to the claim. Every read and the claim carried step 2's incarnation, which the server
   checked atomically with each. A stream deleted and recreated meanwhile answers `412` to the first
   of them that reaches it (a read at an offset the recreated stream does not have answers `410` or
   `416` instead, RFC 9110 §13.2.1, and attach then retries from `HEAD`, or, on a `416`, `HEAD`s to
   tell a recreate from lost data). The round fails, and attach starts again from step 2 (up to
   three more rounds). The sidecar still names the old incarnation, so the next round discards
   whatever this round wrote and rebuilds from the recreated stream. If the replay up to the claim
   reaches a newer owner's claim (a higher epoch, which the server accepts from this producer only
   after ours), the attach fails as fenced and the application retries it: this owner is already
   fenced, and a snapshot it took would record an epoch below the highest one claimed before it
   (§4.2). Finally, if attach wrote the db file, it fsyncs it, writes the sidecar (with the
   incarnation), and swaps the path's binding to the new attachment.

**A failed attach.** An attach that fails after step 1 leaves the path unbound, since its files may
hold anything between the old attachment's state and the stream's, and the stream may hold a newer
claim. Opening its main db then fails with `SQLITE_CANTOPEN` (§1, the reason is logged, and
`ursula_status` names it as `attach_failed` with the failure's kind) until an attach succeeds,
whether or not the path has a sidecar. The refusal is keyed by the path as SQLite names it
(`xFullPathname`, which resolves symbolic links): another spelling of it, such as another case on a
case-insensitive volume, opens unless a sidecar refuses it, and a hard link of the db file has a
sidecar path of its own.

A failed attach undoes only what it wrote into a path whose db file held no data when it began
(`local::empty_again`): when the attach opened the db file to replay into it (`Applier::fresh`,
`Applier::wrote`) or gave it its WAL format, the db file is emptied again in place, keeping its
inode, under the lock that keeps other processes off it, and only while no writable descriptor of
this process outside the extension is open on it. Either may have written it since. Nothing else is
removed: the sidecar stays (it names the stream, a db file without data attaches to any stream
later, and it keeps opens through the VFS refused in other processes and under another case of the
path), and so do `-wal` and `-shm`. The undo runs after a panic midway too. Data found in such a db
file before the attach writes it came from a writer outside the extension: the attach fails
(`in_use`), removes the sidecar it created, and leaves the data, which a later attach then refuses
as an `unreplicated_file` rather than discarding it as a cache. When the undo cannot run (another
process holds the file, an outside writable descriptor is open), the extension logs
`attach_restore_failed` and the files stay as they are: an attach to the same stream discards and
rebuilds them, and one to another stream is refused (`other_stream`) until they are moved aside. A
db file that held data keeps its files.

An attach refused at step 1 changes nothing, and a bound path stays bound. An attach whose claim or
stream create a layer in front refuses with `402` reports `SQLITE_AUTH` rather than `SQLITE_ERROR`,
so callers can attach read-only instead. An attach whose claim is refused before it reached the
stream in another way (§2) reports `SQLITE_IOERR_AUTH`, the code of a refused commit. Nothing was
claimed either way, so the attach can be tried again later. SQLite rolls back the calling
connection's open transaction when a statement that reads a table fails with an I/O error code, so
call `ursula_attach` without a `FROM` clause, as in `SELECT ursula_attach(?, ?)`.

**Read-only attach.** `ursula_attach(path, stream_url, 'read_only')` runs steps 1 to 4, skips the
claim and the replay up to it, and ends as step 6 does: it fsyncs the db file if attach wrote it,
writes the sidecar and binds the file. It never writes the stream (no claim, no commit, no snapshot
thread, no retention move), so it fences no owner and works while a layer in front refuses writes.
A missing stream is an error, never created. The file holds the stream as of the attach (attach
again to catch up), and the sidecar records the highest epoch claimed up to its offset, which a
later owner's attach claims above. Every WAL write fails with `SQLITE_READONLY` before anything is
appended or written to the WAL. SQLite does not roll a transaction back on its own for that code.
A failed `COMMIT` or autocommit statement leaves no transaction open, but in an explicit
transaction a statement whose changes outgrow the page cache fails before `COMMIT`, when SQLite
spills them to the WAL. The transaction then stays open, and even reads on that connection fail
with `SQLITE_READONLY` until the application rolls it back (Pi's transaction helper does). A write
transaction that changes nothing (`BEGIN IMMEDIATE` then `COMMIT`) writes no WAL frame and
succeeds, so a library that opens one at startup works. The uses: exporting a database without
fencing its owner, and reading a database whose owner cannot claim it because a layer refuses its
writes.

**Fencing.** The new epoch fences every earlier owner at the server: their next append gets `403`.
Once an owner knows it is fenced (its own append, or its snapshot thread's request, met the fence),
new read transactions on the file fail too: every read transaction takes a read-mark lock of the
wal-index shared when it starts, even one whose pages its connection holds in its cache, and the
VFS refuses it with `SQLITE_IOERR_READ` while the database is fenced. So from the next read
transaction on, no connection serves the state the new owner moved past, until the file is
attached again. A read transaction open already when the fence lands (an explicit `BEGIN`, a
statement not stepped to its end) keeps its snapshot until it ends. The check reads a flag set with
the fence (`Db::fence`), not the database's mutex, which a commit holds across its append: a read
transaction never waits for a commit. An owner that does not write never learns of the fence: its
reads go on from the state as of its last commit.

Replay applies page images per read batch (last image per page, truncate to the batch's smallest
size first), so replaying from an older offset than the file reflects is idempotent.

**Producer expiry.** The server forgets a producer idle for 7 days. The owner's next append gets
`409` expecting seq 0. It takes the stream back only if the stream still ends at its own offset and
its new claim (epoch + 1) is the first frame after it (read and decoded from that offset, as in
step 5), and is fenced otherwise. Both requests carry the incarnation, so a recreated stream, which
answers `412` before any producer check, fences the owner instead. A new claim refused before it
reached the stream (§2), or a read of it answered `402`, fails the commit alone.

**Reads.** Reads in recovery use `consistency=leader`, because a follower may lag an acknowledged
append. Catch-up reads, `HEAD` and snapshot `GET`s retry `429`, `503` (a leader that could not
confirm its leadership in time) and `307` like appends: no sooner than `Retry-After`, with backoff,
within `URSULA_VFS_RETRY_MS`. A read that got no answer (its connection reset or closed under it,
or a timeout) is sent again on a new connection (§7). A refused connection fails it at once. The
snapshot thread's retries also end at its stop flag. A redirect that lasts the whole budget fails
the read with its status, so attach never takes it for a missing stream or a superseded snapshot.
Attach's `HEAD` refused with a 4xx other than `404` carries no explanation (a `HEAD` has no body),
so the client sends one `GET` of the stream from its beginning and takes that answer's body as the
reason when it is refused with the same status (`client::head_explained`): the server's `400` names
the bucket or stream id it refuses. A `HEAD` that only probes the stream (a re-claim's, the snapshot
thread's) sends no `GET`, and none is sent under the database's mutex.

## 4. Snapshots and retention

Without snapshots the log, and so attach time and stored bytes, grows forever. Without a cold tier
the default per-group hot limit (64 MiB) trips after about 68 MB of frames.

### 4.1 When

After a commit is published, if the log since the latest known snapshot exceeds
`max(database size, URSULA_VFS_SNAPSHOT_MIN_BYTES)` (8 MiB by default), the database's snapshot
thread is woken. Offsets are opaque, so the log is counted in frame bytes, not taken from offsets.
Attach counts what it replays after the snapshot it installed (or, from trusted local files, adds
it to the count the sidecar carries), every acknowledged commit adds its frame, and a snapshot
taken (or found newer at publish) subtracts what was counted when its window opened. Snapshotting
never runs on the commit path. Stored log is bounded by about twice the threshold plus what
accumulates while a snapshot is in flight (§4.3, §4.4).

### 4.2 Taking one

The body must be the stream's state at a frame boundary `W`, page-identical to a replay from the
stream's start to `W`, so that later page-image frames apply on top of it. `VACUUM INTO` and the
backup API both rewrite pages, so neither qualifies. The thread:

1. Opens a private `unix` connection on the file and runs `wal_checkpoint(PASSIVE)` once, so the
   checkpoint inside the window only covers the frames committed since.
2. Opens the *window* (under the database's mutex). It waits until no acknowledged commit is
   unpublished and no other connection holds the checkpoint lock (typically the writer's own
   auto-checkpoint right after a commit), and records `W` (the offset), the epoch and the page
   count. While it waits, the next commit to reach its commit point waits there too (for at most
   1 s) until the window has opened. Otherwise a writer committing back to back would leave the
   window only the gaps between its transactions and could starve the snapshot indefinitely. So a
   due snapshot pins a state at most one transaction past the threshold. While the window is open
   a commit that reaches its commit point waits there, holding SQLite's write lock, which neither
   step below needs.
3. Runs `wal_checkpoint(PASSIVE)` on the private connection, retried for up to about 100 ms while
   another connection's checkpoint keeps it busy (a passive checkpoint gets no busy handler).
   Unless every WAL frame was checkpointed (a reader pins older frames), it gives up for now and
   retries shortly (backoff from 10 ms up to 1 s, and a failed attempt waits longer, §4.4).
4. `BEGIN` and a read: the read transaction starts at `W`. Checks the wal-index header, and the db
   file while nothing of the VFS can have written it, for a commit no connection of the VFS made
   (§3, step 1): one poisons the database, and nothing is published. Closes the window.
5. Copies pages `1..n` of the db file through the private connection's own file handle (a second
   descriptor's close would drop the process's POSIX locks). While the read transaction lasts no
   checkpoint can write newer frames into the db file (a reader at mark 0 blocks backfill, and at a
   later mark it caps it) and no closing connection can checkpoint (that needs an EXCLUSIVE lock),
   so the pages are those of `W`. Ends the read transaction.
6. Fsyncs the WAL and the db file through the private connection's own handles, which reports a
   write-back error of a page the checkpoints or the copy read (§6). A failed fsync poisons the
   database, and so does a lost write the owner found meanwhile. Either way nothing is published.

Commits wait only for steps 3 and 4 (a passive checkpoint of the few frames committed since step 1,
and the start of a read transaction) and, once a snapshot is due, for the window to open (step 2,
at most one commit). A re-attach stops the thread: it checks the stop flag between steps and
retries, and each snapshot request is bounded to 120 s.

The body is `"USS2" | u8 n | W (n bytes, the offset string) | u64 epoch | u32 pages |
u32 crc32c(image) | zstd(image)`. The epoch is the highest one claimed before `W`. Retention may
trim every claim frame, and the next owner must still claim above it: after a producer expiry the
server would accept a lower epoch, and a zombie with a higher one could then fence the new owner.

### 4.3 Publishing, retention, and attach

1. `PUT {stream}/snapshot/{W}`, retried while the outcome is unknown since publishing is
   idempotent, with the `Stream-Incarnation` precondition. A body over 1 MiB is sent twice at most,
   and the wait after a failed attempt covers the rest (§4.4). `409` or `410`: a newer snapshot
   exists, so there is nothing to do.
2. `GET {stream}/snapshot/{W}` until it returns exactly the published bytes.
3. Only then `PUT {stream}/retention/{P}`, where `P` is the *previous* snapshot's offset (the latest
   one known at attach, or the one before `W`).

Retention stays one snapshot behind because a host that read the previous snapshot's offset from
`HEAD` and is fetching it, or whose file sits between `P` and `W`, still finds every frame after
`P`. Retention at `W` would turn its tail read into `410` and a full re-download. The cost is up to
one more threshold of retained log. The newer snapshot has read back before any history is
dropped, so the retained stream always holds a readable snapshot at or above its start. Retention
never passes the latest snapshot (the server refuses).

Attach installs a snapshot when the file is behind the latest one. It verifies the body (offset,
size, checksum), locks other processes out of the file (§3), deletes its WAL, writes the image over
the db file in place (same inode, cut to the image's size), then replays the tail. A crash midway
leaves a mix of old pages and the image's, and the next attach installs the snapshot again. If the
sidecar claims no WAL frame, the old pages hold the state at its offset or a later one (the db file
alone held it) and the files can be trusted. If it claims frames, those went with the deleted WAL
(the old pages may predate the offset) and the files are rebuilt. A tail read that hits `410`
(retention moved under a stale `HEAD`), or a snapshot superseded between `HEAD` and `GET` (a `404`,
or a body cut short when its cold object is deleted after the grace), restarts attach from `HEAD`,
up to ten times.

A publish or retention move that a layer in front refuses with `402` publishes or moves nothing and
is not counted as a snapshot failure. It records the refusal (`payment_required`), and the thread
takes no snapshot until a commit is acknowledged again, which requests one if it is still due.
While writes are refused no commit adds to the log, and each attempt would copy and compress the
whole database for nothing.

Any owner may publish a snapshot of its own offset, a fenced one included, since its state at its
offset is a true prefix of the incarnation it attached to. The snapshot and retention endpoints
know no producer, so the snapshot `PUT`, its read-back `GET` and the retention `PUT` carry the
incarnation as the `Stream-Incarnation` precondition. A stream deleted and recreated meanwhile
answers `412`, and the thread publishes nothing there, moves no retention, and fences the owner
(§6, wrong stream).

### 4.4 Snapshot bodies on the server, and failed attempts

Bodies of at least `runtime.external_payload_min_size` (1 MiB) are stored in the cold tier when a
cold backend is configured, up to 1 GiB, and inline otherwise (up to 32 MiB). A superseded cold
body stays readable for 5 minutes.

Through `ursulagw` a body above its `--max-request-body-bytes` (32 MiB by default) is refused, and so
is one above a proxy's limit, but the client does not always see the `413`. The gateway stops
reading the body at its limit, answers `413` and closes the connection with the rest unread. A body
a little over the limit usually gets the `413`, and one far over it usually meets a failed write,
whose outcome is unknown. A proxy may also reset the connection or time the upload out.

Each attempt reads, compresses and sends the whole database, and a body over a size limit fails the
same way every time until the database shrinks. So failed attempts back off:

- Each failure doubles the wait before the next attempt, from 100 ms, up to 1 h when the body is too
  large and up to 5 min for anything else (an outage, a snapshot that does not read back, a local
  error). Each wait is spread by a fifth, so owners that failed together do not retry together.
- **Size.** A body answered `413` is too large. A body over 1 MiB and larger than any this owner
  published or installed may be too large when its publish got no answer after a connection
  carried it, a 5xx or a `408`. The thread then sends one `HEAD` (`client::answers`). An answer
  below 500 that is no redirect means the stream is reachable, so the body is what failed.
  Anything else is an outage.
- **Outage.** A 5xx with `Retry-After` or `x-ursula-not-applied` (the server's ingress admission
  refuses a large body first under pressure) is an outage with no probe, and the next attempt waits
  at least that `Retry-After`. A connection that never opened says nothing about the body, and
  neither does a failure of a body of at most 1 MiB, retried within `URSULA_VFS_RETRY_MS`.
- Within an attempt a body over 1 MiB is sent twice at most while its outcome stays unknown (a
  refused token does not count, a `307` does).
- A database that shrank below three quarters of the image that last failed (after a `VACUUM`)
  starts the attempt at the next commit. After an outage, the first acknowledged commit brings the
  next attempt forward, once per run, to a random point within the next 30 s.
- A snapshot published, or found at its offset, ends the run. The run lives in memory, so a new
  attach starts without a wait.
- A retention move that fails after the snapshot read back is logged (`retention_failed`) and tried
  again after the next snapshot. It does not count as a snapshot failure.

`ursula_status` reports the run (`snapshot_failures`, and `snapshot_error`: a `413` with the body's
size, or the failed write) and the compressed size of the latest body built (`snapshot_bytes`).
Each failure is logged (`snapshot_failed`) with the wait before the next attempt as `retry_ms` and
what it failed on as `kind`: `size`, `outage` or `other`. The log keeps growing meanwhile, and a
rebuild replays all of it.

## 5. Guarantees

- An acknowledged commit (the SQL `COMMIT` returned) is in the stream, and every host that attaches
  after it sees it, through replay or a snapshot that includes it. Caveat: the owner's later
  commits are page images read from its local files, so local files that lose a write underneath a
  running process (the write-back I/O error of §6) could overwrite acknowledged changes in the
  stream. The VFS checks WAL pages read back against what it wrote, which closes most of that
  window, and §6 lists what remains. Local files rolled back while no process had them attached (a
  restored disk image) are caught by attach (§6).
- Nothing the stream did not acknowledge becomes visible. No frame of a transaction reaches the
  local WAL before its append is acknowledged, and a fenced, refused or failed commit leaves no
  trace locally or remotely. The server deduplicates retries, and a refused commit relies on §2's
  requirement on layers in front of the stream (when a layer breaks it, the next commit poisons,
  except in the cases §2 names). An append the server applied but whose answer the client never
  saw is in the stream, and appears after the next attach.
- Fencing: after a claim at epoch `e` is verified, appends below `e` fail. Snapshots carry the
  highest epoch, so this survives retention trimming the claims.
- A snapshot reflects exactly the stream at its offset. Retention advances only past a snapshot that
  has been read back.

## 6. Failure model

Durability of a commit is Ursula's acknowledgement, nothing else. The local db file, `-wal`, `-shm`
and sidecar are a cache of the stream. Nothing on the per-commit path is fsynced: `xSync` of an
attached database is a no-op, the extension's private connections run `synchronous=OFF`, and the
sidecar is replaced by temp file and rename without fsync. So the application's
`PRAGMA synchronous` level affects neither correctness nor commit latency.

But the files must never be *trusted* when they do not hold the state the sidecar names. With page
images, the owner's next commits are built on whatever pages the files hold, so a stale page would
be written into the stream. Trust is therefore verified against the files, not inferred:

- **The claim.** At every commit the sidecar records `wal=<salts>:<frame>`: the local WAL's
  generation (the salts of its header, which SQLite changes at every restart) and the frame number
  of the acknowledged commit frame. `:0` says the WAL holds no commit and the db file alone holds
  the state.
- **The check.** Attach trusts the files only when the boot id, the stream's incarnation and the db
  file's inode match and the WAL, read the way SQLite's recovery reads it (header checksum, then
  frames in order while their salts match and the cumulative checksum holds), is the claimed
  generation with its last commit at the claimed frame exactly. For `:0`, it must hold no commit
  frame. A commit frame past the claim is a commit the sidecar never saw: one whose sidecar update a
  crash cut off (in the stream, so the rebuild loses nothing), or a commit of a connection outside
  the extension (in no stream, and a checkpoint may have moved its pages into the db file). The
  files cannot tell which, so they are rebuilt.
- **Distrust.** A poison that finds the local files damaged (a lost write-back, a failed fsync) or
  written by a connection outside the extension (`foreign_write`) rewrites the sidecar with a boot
  id no kernel has (`local::DAMAGED_BOOT`), and every later sidecar of that attachment carries it
  (`Db::distrust_local`). So the next attach, in this process or after a restart, discards the
  files and rebuilds them from the stream, pages a checkpoint already moved into the db file
  included. The format stays version 2: every extension of the minor reads such a sidecar as one
  of another boot.
- **Three fsyncs of the db file**, none per commit, all through SQLite's own handle except at attach
  (when no connection is open):
  1. before a commit that starts a new WAL generation is appended (once per WAL wrap, since SQLite
     starts one only when every frame of the previous one is in the db file),
  2. before an attached connection truncates the WAL to nothing (only after a complete checkpoint:
     `wal_checkpoint(TRUNCATE)`, a close with `journal_size_limit`), the sidecar switching to `:0`
     first. A truncate to a non-zero size (`journal_size_limit` in the commit that starts a
     generation) cuts only the previous generation's tail, already synced by (1),
  3. at attach, after folding the WAL and before the sidecar that drops its claim (`:0`, the WAL is
     deleted after it), and again before the final sidecar whenever attach wrote the db file.

  A failed fsync poisons the database (or fails the attach). The snapshot thread also fsyncs the
  WAL and the db file after reading them, off the commit path (§4.2, step 6). That reports lost
  write-backs, and the claim does not rely on it.
- The attached connections keep the WAL past the last close (`SQLITE_FCNTL_PERSIST_WAL`:
  checkpointed, not deleted), and so does the snapshot thread's, so a clean shutdown keeps the
  claim checkable.

**Why this holds** for any crash-consistent image of the files (every write that reached the disk
before an fsync returned is in it, and any later one may be missing or torn). Take claim `(G, f)` at
offset `X` and an image whose WAL recovers generation `G` with its last commit at frame `f`. The
image shows `G`'s header, so it holds the db file as of fsync (1), that is every page as of `G`'s
start.
A page in the recovered frames has its state at `X` or later, since every commit up to `X` since
`G` started is a frame up to `f`. Any other page is unchanged since `G` started up to `X`, so the db
file has it, unless a checkpoint wrote it from a frame past `f` (a commit after `X`). For `:0` the
db file's state at `X` is on disk (fsync 2 or 3) and every later write to it comes from a commit
after `X`. A WAL with commit frames is rejected, since it may be one whose deletion before the
fsync never reached the disk. Either way every page that differs from the state at `X` belongs to a
commit after `X`, and attach replays from `X` (final page images, so replay is idempotent). Every
other image is rejected, discarded and rebuilt.

What attach does in each case:

- **Process crash, same boot** (SIGKILL, OOM kill, abort, a pod rescheduled to the same node with a
  local volume, a container restart with runtimes that show the host's boot id: Docker,
  containerd, CRI-O). Every completed `write()` is in the page cache, so the files are exactly what
  this host wrote and, outside the windows below, the claim holds (the sidecar is written only
  after the WAL writes return). A killed write leaves a prefix. SQLite's salted, cumulative WAL
  checksums stop recovery at the last whole commit, `-shm` is rebuilt, and checkpoints are redone
  from the WAL. Attach trusts the files and replays from the sidecar's offset. A crash in the
  middle of a WAL truncate (between the `:0` sidecar and the truncate), between a commit's WAL
  writes and the sidecar update (a new generation's first, or any other), or in a recovery between
  the `:0` sidecar and deleting the folded WAL, leaves a claim the files do not meet, which only
  costs a rebuild.
- **Same boot, files restored from a crash-consistent image**: a block-level snapshot of the volume
  (EBS, PD or Azure disk snapshots, a CSI VolumeSnapshot or PVC clone) restored or cloned onto a
  host that has not rebooted since, or a block volume force-detached and reattached. Boot id and
  inode survive a block copy, so the WAL claim decides. An image whose WAL ends at the claimed
  commit is used. One whose sidecar ran ahead of its WAL (typical, since the sidecar's rename
  reaches the disk before the WAL's in-place writes), or behind it, or names another generation is
  discarded and rebuilt.
- **Reboot, power loss, kexec, a volume moved to another host**: the kernel boot id differs (Linux
  `/proc/sys/kernel/random/boot_id`, macOS `kern.bootsessionuuid`, and unknown never matches), and
  a power loss may have left any prefix of any unsynced write in any file. Attach discards the local
  files and rebuilds from the latest snapshot and the tail. A new random boot id cannot be on disk
  from before it was generated, so no torn sidecar passes the check. Discarding runs before
  anything opens the file through SQLite (a torn file could fail any checkpoint), refuses while
  another process has the file open, truncates the db file under the lock and keeps it locked for
  the rebuild (§3). Every attach then removes `-journal`, and the fresh path `-wal` and `-shm`, and
  rewrites the sidecar last, so a crash midway discards again.
- **Container runtimes with their own boot id**: LXC, LXD, Incus and systemd-nspawn bind-mount a new
  random boot id at every container start, and gVisor generates one per procfs instance, so a
  container restart there rebuilds. Sandboxes with their own kernel (Kata, Firecracker, WSL2,
  Docker Desktop) rebuild on every restart of the sandbox or VM. This is safe, only slower.
- **Cost of a rebuild**: one snapshot `GET` (the database, held in memory twice while it is decoded,
  up to 1 GiB compressed) plus the tail since it, at most about twice `max(database size,
  URSULA_VFS_SNAPSHOT_MIN_BYTES)` while snapshots keep up. Every reboot pays it, clean ones
  included, so a fleet-wide rolling reboot is a burst of snapshot reads. If snapshots cannot be
  published (a body over the gateway's 32 MiB, a reader pinning WAL frames), a rebuild replays
  everything since the last published snapshot, or the whole log if none was ever published.
  Snapshot health is an availability dependency: watch the log since the latest snapshot.
- **Wrong stream**: the sidecar names the stream's path and its incarnation, and attaching the file
  to another stream is refused. A stream deleted and recreated at the same path is another
  incarnation, so the files are never trusted for it. Whatever its length, they are discarded
  (logged) and rebuilt from it. A sidecar of the same incarnation whose offset lies beyond the
  stream's end means the stream lost acknowledged data: attach refuses and keeps the files (delete
  `<db>` to rebuild, as after an operator restore from backup). A deleted stream (by mistake, by
  TTL expiry, or by a fresh install that did not carry it over) leaves the local files as possibly
  the only copy, so attach creates a missing stream only for a file without content. To start over,
  delete `<db>` or attach a new file under another stream URL. To salvage the data, copy `<db>` and
  `<db>-wal` to a new name before deleting anything, open the copy, which has no sidecar and so opens
  as a plain SQLite file, and export its rows into a newly attached database. An empty file attached
  at the same URL after the delete creates a new incarnation, and the original owner's next attach
  then discards its files. While attached, the recreated stream answers `412` to the owner's next
  request of any kind (§2, the precondition is checked when Raft applies an append, so a commit
  proposed before a delete and recreate and applied after is refused too), nothing of it lands, and
  the owner is fenced and poisoned. A recreate during attach fails that round (§3).
- A rollback journal next to an attached file can only be left by a crash while attach switched an
  empty file to WAL. Attach deletes it in every case, since rolling it back would truncate the file
  under the pages attach writes next.
- **An unreachable endpoint**, for every attempt (its host does not resolve, its connections are
  refused or time out, the TLS handshake fails): commits block up to the retry budget, then fail
  alone as `unreachable`. Nothing of them went out, so the database is not poisoned, and the first
  commit after the endpoint is back succeeds with no attach (§2).
- **A network partition or slow server**, where an attempt went out on a connection and got no
  answer (a reset, a timeout waiting for the answer): its outcome is unknown, so commits block up to
  the retry budget, then poison (`outcome_unknown`), unless a later attempt's answer settles it. An
  attach that fails after step 1 leaves the path refusing opens in the process until an attach
  succeeds (§1, §3).
- **DNS.** The client looks a host up at most once every 30 s (`net::DNS_FRESH`), one request at a
  time, while the host's other requests go on with its last addresses. The request that looks it
  up waits for the lookup, up to the resolve timeout of 5 s. A failed connection makes the next
  lookup due. While lookups fail, the last addresses serve for up to 10 minutes after the last
  successful lookup (`net::DNS_STALE_LIMIT`), with a new lookup 5 s after each failed one
  (`net::DNS_RETRY`, logged as `dns_stale`). So a DNS outage shorter than 10 minutes delays one
  request per host by up to 5 s, at most once every 5 s, and fails nothing. Past it, and for a host
  that has not resolved yet, the host's requests fail before they go out until a lookup succeeds,
  and commits fail alone as `unreachable`. A pooled connection keeps its old address until it
  closes.
- **A server or gateway that refuses writes for a while** (the server's Raft log over its hard limit
  under sustained write load, its WAL disk full, a gateway whose token issuer is down) marks its
  refusals `x-ursula-not-applied`, and Ursula's gateway vouches for them. Commits block up to the
  retry budget. When every attempt was refused that way, the commit fails alone and the first
  commit after the refusals stop succeeds with no attach. An attempt answered without a vouched
  marker leaves the outcome unknown, and the commit poisons. That is every such refusal of a server
  with no gateway in front of it. A gateway drops the marker when it reached the refusing node by
  following a leader redirect that carried none (§2).
- **A layer that refuses writes** with `402` (an exhausted quota, a suspended account), or an
  authorizer that denies them with a vouched `403`: commits fail alone and nothing of them reaches
  the stream, reads go on, and the first commit after the layer accepts writes again succeeds with
  no attach. A `403` without both headers poisons. A process that restarts meanwhile cannot attach
  as the owner, because its claim is refused. It attaches read-only to read (§3).
- **A layer that breaks §2's requirement** (it retries writes, follows redirects for them, or
  refuses after it forwarded): the next commit usually poisons with a reason that names both
  commits the stream may hold, and after the next attach the database holds the refused
  transaction. §2 lists the cases the check misses.
- **Two owners**: the later claim wins, and the earlier one's next commit fails cleanly
  (`UrsulaReplicationError` with `fenced: true` through the Pi helper). From then on its reads fail
  too (§3, fencing).
- **A snapshot that cannot be taken** (a long reader pins WAL frames) or published (body too large):
  the log grows, nothing is lost, and retention does not advance. A body too large is sent again
  every 48 to 72 minutes once its failures run long (an hour, give or take a fifth), and at once
  after the database shrank by more than a quarter (§4.4).

**Write-back errors.** A disk write-back I/O error underneath a running process (EIO from the
device, a thin-provisioned volume out of space) is reported by nothing when it happens, since the
commit path never fsyncs the WAL, and the db file only at WAL restarts, truncates and attach. Once
the dirty page is evicted, a read gets the old block back, and SQLite does not check WAL frames on a
read. The owner's next commits would append page images built on it, losing acknowledged changes
from the stream itself, not only from the cache. So the VFS remembers the crc32c of every WAL page
this process writes in the WAL's current generation, by offset (4 bytes per page), and checks every
page its connections read back. A page that reads back other bytes fails the read and poisons the
database before any commit can build on it. An error writing back the db file is reported by the
next fsync of the db file (Linux reports it to an fsync on any descriptor opened before it). That
fsync comes before the commit that restarts the WAL, which is normally the first commit after a
complete checkpoint, the point from which readers read checkpointed pages from the db file instead
of the WAL.

The snapshot thread's private connection reads the WAL (its checkpoints) and the db file (its page
copy) outside the VFS, so it does not see that check. A lost page it published would become
permanent once the next snapshot moves retention past the frames that hold the good one. So after
reading, it fsyncs both files through its own handles (§4.2, step 6) and publishes nothing when an
fsync fails (which poisons the database) or the owner is poisoned for a lost write. On Linux an
fsync reports a write-back error that no fsync has reported yet, from any descriptor, and in the
process only the owner's fsync of the db file could have reported it first, which poisons too.

Residual risk:

- The snapshot's fsyncs rely on the kernel reporting an earlier write-back error to a later fsync on
  another descriptor, which Linux does. Elsewhere (macOS) a lost page the checkpoints or the copy
  read, and no connection of the owner read back, can still be published.
- A page changed by anything but a failed write-back (unsupported, below) is reported by no fsync.
  Unless a connection of the owner read it back first, a snapshot can publish it.
- A commit that cannot restart the WAL, because another connection still reads from it, can read a
  db page that lost its write-back since the last fsync and append the damage.

**Unsupported**, because they break the model above: a runtime that fakes a fixed boot id, a volume
swapped or rolled back underneath a running process, edits to the files outside the extension (a
process that never loaded it, copying a backup over the db in place, while a db file replaced by
rename is detected and discarded), a file-level copy of the files taken while the process runs
(`cp`, `rsync`, a file-level backup: each file is copied at another moment, so the copy is not one
crash-consistent image, whereas a block-level snapshot of the volume is), and network or FUSE
filesystems for the local files (NFS, EFS, SMB, 9p, virtiofs: no page-cache coherence or reliable
POSIX locks, and a second host's attach would discard files the first is using). To force a
rebuild, delete `<db>`.

**Panics.** A panic inside the extension stops at its C entry point (`guard::contain`), because
unwinding out of an `extern "C"` function would abort the host process. The process goes on, and
the log names the entry point (`event=panic`, after the Rust runtime's own panic message). What
fails:

- A file method answers its I/O error, and its database is poisoned (attach it again), since the
  panic may have left its state half changed. The poison keeps the write transaction's overlay,
  which may belong to another connection, and that connection's next write fails instead. `xOpen`
  answers `SQLITE_CANTOPEN`, leaves the file without methods and closes the `unix` file if its own
  open succeeded. `xClose` takes the file's state and does its bookkeeping (the WAL handles open,
  the handle with the EXCLUSIVE lock) before the contained body, so a panic poisons nothing, and it
  closes the `unix` file and still releases the file's open count, so the file can be attached
  again in the process.
- A SQL function fails with an error. A panic while `ursula_attach` binds the files is a failed
  attach: it drops the binding and the attach in progress, and the file opens again only after an
  attach succeeds.
- A panic in the extension's init fails the load, with the message "sqlite3_extension_init
  panicked inside the extension".
- The snapshot thread is not contained. A panic there ends that thread alone, which the next attach
  of the file reports (`snapshot_thread_panicked`), and no snapshot is taken until then.

The crate refuses to build with `panic = "abort"`.

## 7. Limits

- 4 KiB pages, WAL mode only, `locking_mode=EXCLUSIVE` unsupported.
- One owner process per stream at a time. Connections in other processes that load the extension
  cannot open the file (§1), and processes without it are not replicated (and block recovery).
- **Connections outside the extension.** Attach refuses while this process holds the db file open
  for writing outside the VFS, and a commit such a connection makes later poisons the owner at its
  next write transaction or snapshot, and rebuilds the files at the next attach (§3, step 1). What
  stays unseen:
  - A foreign commit that a complete checkpoint moved into the db file, followed by a WAL restart or
    truncate (the writer committed again after the checkpoint, ran `wal_checkpoint(RESTART)` or
    `TRUNCATE`, or deleted the WAL as the last connection to close), before the owner's next write
    transaction, once the owner has committed since its attach or its attach kept the WAL. No frame
    count is left to compare, and the owner's own checkpoints write the db file too. Its pages stay
    in the db file and never reach the stream. Before the owner's first commit after an attach that
    left no WAL, the db file's size, inode and modification time give such a commit away, unless it
    lands within the filesystem's timestamp granularity of the attach's last write and keeps the
    size.
  - A connection outside the extension in this process that opens while attach runs, after its
    check: it is caught once it commits. A read-only one never refuses the attach, and attach may
    rewrite pages under it and drop its POSIX locks (its descriptor closes do).
  - A descriptor opened through another hard link of the db file has another name, and is not
    compared. Where the process's descriptors cannot be listed (no `/proc` or `/dev/fd`), or their
    names cannot be read (other than Linux and macOS), attach does not check them.
- The local files must be on a local filesystem (§6).
- **A transaction must fit one request.** Its commit frame as sent, a 12-byte header and the
  transaction's changed pages compressed with zstd, is at most `URSULA_VFS_MAX_FRAME_BYTES`: 32 MiB
  (33,554,432 bytes) by default, the server's request body limit, which a gateway's
  `--max-request-body-bytes` matches by default. The setting is in bytes, from 64 KiB to 32 MiB. A
  value outside is taken to the nearer bound, and one that is not a number of bytes is ignored, both
  logged once as `setting_adjusted`. 32 MiB is about 8,000 changed pages of incompressible data
  (random, encrypted or already compressed bytes, such as embeddings or media), and about three
  times as many of typical text. A larger transaction is never sent. It fails alone as `too_large`
  with the frame's size and the limit in `refused_reason` (§2), the database stays usable, and
  snapshots go on. Before it refuses, the extension holds the transaction several times in memory:
  the changed pages, the record built from them, and its compressed frame. Split bulk imports into
  smaller transactions. Statements that rewrite a whole table or index run as one transaction too:
  `CREATE INDEX` on a large table, `REINDEX`, an `ALTER TABLE` that rebuilds the table, and
  `VACUUM`, which rewrites every page. On a database whose compressed size is above the limit,
  `VACUUM` fails the same way: delete rows before it, so that what it writes compresses below the
  limit, or copy the rows into a new database in transactions below the limit.
- Behind a layer with a lower request limit, set `URSULA_VFS_MAX_FRAME_BYTES` at or below it. A
  vouched `413` fails the commit alone as `too_large`, and any other `413` poisons at once. The
  gateway's per-bucket body limit reads the whole body before it answers, so its `413` reaches the
  client. So do the refusals the gateway decides from a write's head alone (a refused token, an
  authorizer's denial, a bucket's rate limit): it reads the body before it answers them, up to its
  `--max-request-body-bytes`, for 30 s at most, which outlasts a commit's 10 s request timeout, and
  until no data arrived for 5 s. At its `--max-request-body-bytes` the gateway stops reading,
  answers a vouched `413` and closes the connection with the rest of the body unread. A commit a
  little over that limit may get the `413` and fail alone. One far over it usually meets a failed
  write, an unknown outcome, and poisons once the retry budget runs out. The server's limit equals
  the default frame limit, which the setting cannot raise, so no frame the extension sends is over
  it.
- Snapshots hold the database image in memory (twice, briefly: raw and compressed) and are capped
  by the server at 1 GiB compressed (32 MiB inline or through the gateway). `snapshot_bytes` in
  `ursula_status` is the latest body's compressed size.
- Producer expiry after 7 idle days. The owner reclaims only if nobody wrote meanwhile.
- Redirects are never followed (§2), so the stream URL must reach the stream's leader by itself:
  through a gateway, or on a single-node server. A URL that names one node of a multi-node cluster
  works only while that node leads the stream's group. A URL that a proxy redirects (from `http://`
  to `https://`, say, usually with a `308`) fails at once with the redirect's `Location`, so name
  the address the redirect points to.
- HTTP, or TLS for `https://` URLs with the bundled Mozilla roots, or only the certificate
  authorities in `URSULA_VFS_CA_FILE`. One bearer token per process: `ursula_set_token`, else the
  content of `URSULA_VFS_TOKEN_FILE`, read again when the file changes and after a `401`. A read of
  the file that finds no token (empty, half-written, missing) keeps the last one it held, since a
  request sent without one is concealed as a missing stream (`404`). That fails an attach, and
  fails a commit at once, alone when the `404` is vouched (§2) and with a poison otherwise. A token
  sent over `http://` to a host that is not loopback is logged once (`token_in_clear`). The SQL
  functions are direct-only: a trigger or view of an untrusted database cannot call them.
- Every request carries `User-Agent: sqlite-ursula-vfs/<version>`, the extension's version.
- A host's addresses are looked up through the system's resolver at most once every 30 s per
  process, so a stream whose endpoint moves is reached at its new addresses within 30 s, or 5 s
  after a connection to an old address failed (§6, DNS).
- **Connections.** Appends, reads and `HEAD`s keep a connection for 90 s after its last request
  (`URSULA_VFS_IDLE_MS`), so a writer that commits every 15 to 90 s does not open a new TCP and TLS
  connection per commit, which behind a reverse proxy or a CDN can cost about a second. Snapshot
  transfers keep 15 s, since a dead connection would hold a transfer for its whole 120 s timeout.
  Before reusing a connection, the client reads its socket once without blocking and drops one
  closed while idle. One that a NAT, a load balancer or a firewall dropped silently still looks
  open, and a request on it fails at once on a reset or after the 10 s request timeout. A request
  that got no answer is sent again on a new connection, which also drops the other pooled
  connections idle for more than 1 s. Many such devices drop connections idle for about 4 minutes.
  Behind one that drops them sooner, set `URSULA_VFS_IDLE_MS` below its limit. On macOS a reset
  can surface as `EINVAL` (`Invalid argument`) because ureq sets a socket option before each read
  and write, and macOS refuses options on a reset socket. That `EINVAL` counts as no answer on
  macOS only (`client::reset_on_macos`).

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
previous published patch of the minor against the build, both ways, and runs the build against
itself until a minor has a second patch.

A change to any of them waits for a minor and changes the identifier with it: a new frame or
snapshot magic, a new sidecar format, a new `Producer-Id` prefix. An extension fails closed on what
it cannot read. A frame or snapshot of another format stops attach with an error that names the
magic it found (`frame magic "USQ2": this extension reads "USQ1" frames, ...`) instead of misreading
it. A sidecar it cannot parse or does not trust is discarded, so attach rebuilds the files from the
stream. Owners with different `Producer-Id`s do not fence each other, so a minor that changes it
must say in its release notes to stop every owner of a stream before upgrading.

## 8. Tests

Rust unit tests run with `cargo test` in `clients/sqlite-vfs` (CI also runs them on macOS, for its
boot id and its `EINVAL` resets). The e2e suites in `clients/sqlite-ursula/test` run against a real
single node, and the cluster suites against 3 nodes, a gateway and S3 (AWS S3 in CI, MinIO for fork
PRs).

| layer | files | what it proves |
| --- | --- | --- |
| unit: encodings | `frame.rs`, `snapshot.rs` | whole frames only, damage refused, another format refused by its magic and named in the error |
| unit: claims | `claim.rs` | a claim wins only as the frame ending at the answered offset, or anywhere in a read-back that ran past it, and a re-claim only as the first frame after the owner's offset |
| unit: answers | `client.rs`, `error.rs` | every error's kind and action, attach's refused `HEAD` carries the reason of a `GET` refused the same way, the last `401` keeps its body, `applied_nothing` (status for 401, 402, 429, marker with vouch for other refusals, nothing for a redirect or a success), each answer's outcome and `refused_kind`, mixed attempts, `refused_reason` cut to 256 bytes, no request follows a redirect, a `307` resent with the same sequence, a large snapshot body sent twice at most, the snapshot size probe, idle times, resent requests on new connections, `EINVAL` a reset only on macOS, failures before sending fail alone as `unreachable` |
| unit: owner state | `db.rs`, `vfs.rs`, `status.rs`, `extension.rs` | a refused commit fails alone with epoch, sequence and offset kept while an unknown one poisons, the next-commit check, an unsent `too_large` leaves `layer_refused` and the check, no snapshot while a layer refuses, WAL pages checked when read back, `ursula_status` fields and the error's kind and action for poisons and refusals, attach error codes and modes, foreign commits by the wal-index header (a first commit into an empty WAL, one after the owner's, one a recovery found, a rolled-back restart), opens refused after a failed attach, local damage distrusts the files, a fence raises the read flag |
| unit: snapshots | `snapshotter.rs` | backoff by kind (size, outage, other), doubling up to each kind's limit, spread by a fifth, `Retry-After` honoured, a shrink of a quarter releases the wait, one brought-forward attempt per run, only a snapshot at the offset ends a run |
| unit: transport | `net.rs` | DNS freshness, one lookup at a time, the stale limit and retry, `NotSent` for failed lookups, connections and TLS handshakes |
| unit: local files | `local.rs` | sidecar trust only in this boot, for this incarnation and db file, with its WAL claim met exactly, folding a WAL keeps the last image and cuts the file, a failed attach empties only the db file it wrote, a writable descriptor on the db file is found and a read-only one or another file of the name is not |
| unit: panics | `guard.rs`, `vfs.rs` | every file method's C entry point contains a panic and poisons, `xClose` and `xOpen` release the open count, the writer's overlay is kept |
| unit: settings | `config.rs`, `auth.rs`, `log.rs`, `stats.rs` | frame limit bounds, a token file without a token keeps the last one, log format, bounded per-commit measurements |
| e2e: transparency | `transparency.e2e.ts`, `conformance.e2e.ts` | any schema rebuilds byte-identical, the overlay's write-transaction scope, Pi conformance in three modes (direct, reopen, rebuild every commit) |
| e2e: crash and recovery | `crash.e2e.ts`, `recovery.e2e.ts` | SIGKILL and abort around the commit point, the cache spill, an unknown append, one owner per host, no connection open while pages are rewritten |
| e2e: local cache | `reboot.e2e.ts`, `writeback.e2e.ts` | trust only in the same boot with the WAL claim met, rebuild from snapshot and tail otherwise, a changed WAL page fails its read and poisons with the stream intact |
| e2e: fencing | `fencing.e2e.ts` | a new claim fences every earlier owner, an owner of a deleted stream, a foreign higher `Stream-Seq`, a fenced owner's reads fail, through the Pi storage as `UrsulaReplicationError` |
| e2e: refusals | `payment.e2e.ts`, `refused.e2e.ts`, `refusal-errors.e2e.ts`, `early-refusal.e2e.ts`, `unreachable.e2e.ts` | every fail-alone row of §2 through `gate.mjs`, read-only attach, the next-commit check and its first gap, `too_large`, a real gateway's head-decided `401` on an 8 MiB commit, refused connections, Pi error mapping |
| e2e: snapshots | `snapshot.e2e.ts`, `snapshot-backoff.e2e.ts`, `cold-snapshot.e2e.ts` | a run far past the hot limit stays bounded, fresh and lagging hosts rebuild byte-identical, takeover after a trim fences, §4.4's backoff through `gate.mjs` and a real gateway, a 12 MiB body through S3 multipart (cluster only) |
| e2e: deployment | `auth.e2e.ts`, `redirects.e2e.ts`, `health.e2e.ts` | TLS and gateway-checked tokens, leaderships moved under commits and snapshot read-backs with no `307` reaching the client, `ursula_status` and logs for operators |
| e2e: panics | `panics.e2e.ts` | every SQL function and init fail with an error and the process goes on, a panic poisons one file alone |
| e2e: package entries | `entries.e2e.ts` | the root entry, source and built, imports nothing outside Node.js, the `/pi` entry imports Pi Durable |
| e2e: misuse | `misuse.e2e.ts` | a connection opened before the extension was loaded refuses the attach and a read-only descriptor does not, a refused attach keeps the binding, a commit through `?vfs=unix` poisons the owner in either order and after a reopen, and a reattach rebuilds the file, a failed attach names the server's reason, refuses opens and leaves no data that refuses a later attach, an unreplicated file, a fenced owner's reads fail, a reader is not held up by another thread's commit, and refused, unknown and fenced commits report kinds of their own |
| e2e: versions | `compat.e2e.ts` | same-minor compatibility (§7, versions) |
| e2e: load | `bench.e2e.ts`, `soak.e2e.ts` | sanity numbers (bytes per turn, pages per commit), server memory under VFS-shaped load (soak, opt-in) |

The suites run the Pi Durable version in the lockfile. The single-node job also installs the oldest
version in `package.json`'s peer range and runs the typecheck, conformance, `fencing.e2e.ts`,
`payment.e2e.ts` and `refused.e2e.ts` again, whose Pi cases reach `openUrsulaPiStorage`'s mapping of
the errors the node driver throws for a failed commit.

## 9. Performance

Measured on 2026-10-09 on EKS in us-east-1: three `m6i.xlarge` Ursula nodes, one per AZ (256 groups,
4 cores, the Raft WAL on gp3 with `raft.wal.fsync = never`), three gateways, one per AZ, S3 for the
cold tier and snapshots, and `m6i.2xlarge` client pods in one AZ, one process per database, through
the gateway Service. The extension measured predates the snapshot thread's fsync of the WAL and the
db file (§4.2, step 6), which can change snapshot publish times. The tools are in
`clients/sqlite-ursula/bench/`.

**Workload** (`bench/owner.ts`): Pi Durable's `SqliteStorage` on the VFS via `openUrsulaPiStorage`,
a real `Harness` with a faux model, turns of text, text, tool (5 Pi commits per turn). Latency is
`Storage.commit` wall time after a 30 s warm-up, in 10.5 min cells. Agent pace is one turn per 2 s
per database. Baselines, B1 and B2 from an earlier run on the same client node type:

- B1: the same workload on Pi's own `openNodeSqliteStorage` on the pod's disk, no extension (WAL,
  `synchronous=NORMAL`). It involves no server.
- B2: the same with `synchronous=FULL` (the WAL synced on every commit).
- B3 (`bench/raw.ts`): raw appends, no SQLite. Closed-loop writers, one stream each, 5 to 7 KiB
  random bodies with producer headers, through the gateway.

Pi commit latency, ms, p50 / p99 (p99.9 / max), and Pi commits/s:

| cell | B1 local, NORMAL | B2 local, FULL | VFS |
| --- | --- | --- | --- |
| 1 db, agent pace | 0.11 / 7.6 | 1.8 / 4.6 | 4.7 / 7.9 (11.6 / 11.9) |
| 1 db, flat out | 0.16 / 8.9, 115/s | 2.2 / 4.9, 100/s | 4.4 / 7.0 (51 / 144), 81/s |
| 16 dbs, agent pace | 0.11 / 6.5 | 1.6 / 4.8 | 4.1 / 7.4 (13 / 162) |
| 16 dbs, flat out [1] | 0.20 / 13.1, 651/s | 2.8 / 19.1, 652/s | 8.1 / 28.4 (58 / 199), 541/s |
| 128 dbs, agent pace [2] | | | 3.9 / 6.0 (13 / 171), 320/s |
| 128 dbs, flat out [2] | | | 26.7 / 105 (191 / 883), 2,051/s |

[1] 16 processes on one 8-vCPU client node at about 70% CPU, so this row measures the client too.
[2] Two client nodes, 64 databases each.

B3, the raw append floor through the gateway: one writer 1.54 / 2.53 ms (p99.9 19.8), 593
appends/s, and 16 writers 2.94 / 7.53 ms (p99.9 75), 4,662 appends/s. No database was poisoned, no
commit failed, and none of the 1.85 million commits retried its append.

**The append is the commit.** Where one database's commit goes (1 db at agent pace, p50):

| component | ms |
| --- | --- |
| Pi and SQLite outside the VFS's commit | 0.18 |
| VFS commit hook without the append | 0.12 |
| the append request | 4.31 |
| measured Pi commit | 4.66 |

What the append costs depends on where the stream's leader and the connection's gateway are. Sent
to the gateway in the client's AZ, the VFS's append p50 was 1.3 ms with the leader in the same AZ
and 1.4 to 2.4 ms with it in another, within 0.04 to 0.13 ms of Node's `fetch` sending the same
bytes to the same stream.

**Stream sizing.** 7 pages per commit. zstd ratio 9.4 at agent pace and about 6.2 flat out, so
3.6 KB per commit at agent pace and 4.6 to 4.8 KB flat out. Retention trails one snapshot behind
(§4.3), so the retained log reaches about twice the snapshot threshold plus what is committed while
a snapshot is taken: it ended at 16.8 MB in the 16- and 128-database flat-out cells, twice the
8 MiB minimum. Back-to-back large transactions (`bench/bigtx.ts`: a 5 MB table, 2,000-row updates,
frames of about 1 MB, no pause, 300 s) committed 3,429 times at p50 91 / p99 182 ms, wrote 3.35 GB
and took 381 snapshots, and the retained log never exceeded 19.5 MB.

**Cold start** (`bench/coldstart.ts`): a fresh host attaching new files, best of three, first
attach in parentheses. A newly built database has no snapshot yet (§4.1), so the first column
replays the whole log. For the second, rows were rewritten until a snapshot was published:

| database | log only: log replayed, attach | with a snapshot: body (publish time), tail after it, attach |
| --- | --- | --- |
| 10 MB | 2.5 MB, 48 ms (77) | |
| 100 MB | 24.6 MB, 0.28 s (1.11) | 23.8 MB (6.7 s), 11 MB, 0.85 s (1.10) |
| 1 GB | 246 MB, 8.0 s (12.8) | 237 MB (10.9 s), 74 MB, 15.5 s (18.7) |

Installing a snapshot is slower than replaying as many bytes of log: 237 MB of snapshot and 74 MB of
tail took 15.5 s, 246 MB of log 8.0 s.

**Failover**: 16 databases flat out, and at 120 s a forced delete of the node leading the most groups
(86 of 256), three runs. No commit failed and no database was poisoned, and afterwards every owner's
file and a fresh rebuild from its stream were identical, with `integrity_check` ok. Elections took
up to about 5 s, during which the VFS got `503` and retried. The longest stalls, 10 to 22 s, were
one or two whole 10 s request timeouts on connections to the killed node, inside the 30 s retry
budget.
