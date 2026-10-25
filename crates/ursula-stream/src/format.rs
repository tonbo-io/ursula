//! Format epoch: the one version number for everything Ursula persists or
//! sends between nodes, and the backup format, which moves only when what a
//! backup holds changes.
//!
//! Epoch 1 is the format of Ursula 0.5.x and earlier, and of main builds
//! before format epoch 2 landed. Epoch 2 is the format of Ursula 0.6. Epoch 3
//! checksums each Raft WAL frame's length together with the sequence number of
//! the journal segment that holds it (`docs/architecture/raft-wal.md`); it is
//! the format of Ursula 0.7. Epoch 4 is the format of Ursula 0.8: write keys
//! with a producer record of one receipt, closed-epoch reports, write digests
//! and the per-stream epoch floor, and history identity: the group's history
//! event, the history a producer record was claimed in, and what a stream
//! inherited from the history before. Epoch 5 is the format of Ursula 0.9:
//! the `ExpireStreams` command, a clock on `DeleteStream`, writes that
//! remove only the stream they name, and compact MessagePack
//! ([`crate::msgpack`]) for everything Ursula writes as MessagePack.
//!
//! Epoch 6 is the format of Ursula 0.10 (#768):
//!
//! - a run state with one record per journal class, and a log generation on
//!   every Raft WAL journal record, core metadata entry and group snapshot
//!   record;
//! - two rules for how a Raft WAL journal opens: a journal read below its
//!   class's recovery epoch opens its groups recovering, and the meta
//!   journal's group opens recovering when the meta journal is gone (neither
//!   its metadata file nor a record) or holds nothing (no record, no
//!   snapshot record, and no group's vote, log state, recovery plan or
//!   generation in its metadata file) while a data journal holds records,
//!   and when it is gone while the meta group's snapshot record remains;
//! - a Raft entry payload with the meta group's commands beside the data
//!   groups' (`RaftCommand`), whose data entries keep their epoch-5 bytes;
//! - the meta state machine's dispatch of that payload: it stops its apply
//!   on a stream entry, applies nothing for a history event whose
//!   `previous` is not its current history or whose `leader` is not the
//!   leader of the entry that carries it, makes an event that names both
//!   the history and begins the placement kernel's history
//!   (`begin_history`) at the event's entry's log id with the event's
//!   cause, and applies control commands through the placement kernel at
//!   their entry's log id;
//! - the placement kernel's state, commands and answers, and the meta
//!   group's state, history event and inline snapshot record. A meta
//!   snapshot's state is bounded by
//!   `ursula_raft::meta::snapshot::META_SNAPSHOT_MAX_STATE_BYTES` (16 MiB),
//!   and the bound is fixed for the epoch, since a node refuses a meta
//!   record or install above it;
//! - placement parameters with no maintenance authority: `ClusterParams`
//!   hold the group count, the replication factor, the meta voters and the
//!   move limit, and the kernel refuses no `RequestMove`, no `Decommission`
//!   and no `Bootstrap` topology on behalf of a startup reservation;
//! - a plane and a target node on every Raft request, the plane of an
//!   `AppendStream` in its `ursula-raft-plane` metadata, the refusal of a
//!   request for another node, and the refusing node of a refused
//!   `AppendStream` item (`RaftAppendStreamError.refusing_node`);
//! - barrier answers that echo the plane and the group they answer for and
//!   carry the leader's memberships;
//! - the genesis probe RPC, which a node that runs no meta replica refuses
//!   (`NOT_FOUND`) and a node that runs one answers with its own genesis
//!   claim when the prober's differs, else with its meta replica's answer to
//!   the probe vote;
//! - the meta plane's forwarded write: a `GroupWrite` of the meta plane with
//!   core and shard 0, answered per command with a control answer or a group
//!   engine error, and refused (`INVALID_ARGUMENT`, before anything is
//!   proposed) when it names no admitted sender or registers another node
//!   than the sender the cluster check admitted;
//! - the group engine error without its static-membership variant, with a
//!   not-held WAL open, a control answer kind and a command of the other
//!   plane, and with the closed `ApplyStopKind` in `ApplyStopped`, which no
//!   longer decodes the unknown kind epoch 5 accepted. Each of its infra
//!   errors that names a group names it as a `RaftTarget` in `group` (in
//!   `expected` and `actual` for `RecoveryGroupMismatch`), the field's
//!   place unchanged: `Data` with the group id, or `Meta`, so a refusal of
//!   the meta group never reads as one of data group 0;
//! - the identity of a WAL root and of each process on it: `topology.bin`
//!   records the node id and a root id the root draws, and a root opens only
//!   under its node id; `identity.bin` counts the runs of the root,
//!   records whether a run read a predecessor that may still run, which an
//!   unbound root, a remount, a host crash or a lost run state reads, and
//!   records whether the meta group ever bound the root (`unbound` until
//!   the first answer to its registration, `bound` from then on), which
//!   alone decides whether a start holds its data replicas until a binding
//!   and what `RegisterNode` says as `empty`; the run
//!   state records the device the root is mounted from, and a run that did
//!   not end cleanly reads as `Remounted` on the same boot from another
//!   device, replayed and gated as a host crash; a process incarnation is
//!   the root id, the run and a nonce in 32 hex digits, and a root id is 16
//!   lowercase hex digits in every encoding, never a number;
//! - root slots in the placement kernel: every node record and every
//!   registration before Bootstrap binds a root (`RootSlot`), `RegisterNode`
//!   names the root and whether it is `empty` (its identity file records
//!   that meta never bound it), `ReleaseRoot` releases a bound
//!   root by compare-and-set, a slot changes only as its table says, a root
//!   registered as holding state never takes a released slot, the root a
//!   released slot names never binds again (the release rule, a disk
//!   revoked, keeps a root released earlier out), and `OtherRoot` is
//!   retried while `StaleRoot` confirms a view;
//! - a node id between lives: the lifecycle `Vacant`, which only
//!   `ReportRemovalComplete` writes, a compare-and-set on a `Removed` record
//!   and its slot's root that releases that root (`RemovalCompleted`,
//!   `NotRemoved`, `StaleRoot`), and a `RegisterNode` of an `empty` root
//!   other than the released one that begins a fresh `Active` record on a
//!   vacant id; a vacant id refuses every other command naming it with
//!   `NodeVacant`;
//! - a recovery gate rule: a replica whose log is empty and whose persisted
//!   vote is committed above term 0, a live leader's vote (a barrier's
//!   floor, or the vote saved before a leader's entries), opens refusing
//!   every candidate;
//! - the Raft metadata of the process fence: `ursula-incarnation`,
//!   `ursula-target-incarnation` and `ursula-fence` with its six refusals,
//!   which a sender reads only after the mismatch text, a cluster refusal
//!   and a misdelivery, and a misdelivery only from a refusal that names
//!   another node than the request's target. Every Raft request but the
//!   format-epoch probe names its sender's process, and the target's when
//!   the sender holds one, an `AppendStream` at its open; every answer and
//!   refusal a handler gives names the responder's node id and process in
//!   `ursula-node-id` and `ursula-incarnation`; a receiver checks them after
//!   its group lookup, and a sender decodes only an answer whose process its
//!   table accepts;
//! - two variant casings, each frozen with the epoch. The Raft-layer
//!   envelopes (`StreamCommand`, `RaftCommand`, `MetaCommand`,
//!   `MetaResponse` and `GenesisAnswer`) keep serde's default `PascalCase`
//!   variant names, as 0.9's `StreamCommand` does. The types of
//!   `ursula_proto::placement` and `ursula_control` write `snake_case`
//!   names, because one serde form serves both their admin JSON and their
//!   MessagePack. Neither is converted to the other.
//!
//! Epoch 7 is the format of Ursula 0.11:
//!
//! - every version number below reads 7, so 0.11 refuses the data and peers
//!   of 0.10 as it refuses those of any other epoch.
//! - a bucket purge removes its streams in stream-id order
//!   (`compare_stream_ids`), so every replica numbers the purge's cold-GC
//!   entries alike.
//! - per-version usage (bounded-state F20): each live stream version's
//!   committed counters, applied appends (F9), newest write time, usage
//!   sequence number, counters when its current history began and the
//!   history its counters began in, counted at the bucket counter's call
//!   sites by its rule. Bucket totals derive from each bucket's retired
//!   usage, its live versions and its end records. The group keeps its
//!   genesis history id.
//! - every removal leaves an end record with the version's final usage,
//!   its end (`Deleted`, `Expired`, `Purged`), the removing entry's
//!   `now_ms` and the history it ended in, under a new usage seq. A group
//!   holds at most `MAX_END_RECORDS`: a removal past that first folds the
//!   lowest-seq end record into its bucket's retired usage.
//! - `PurgeBucket` carries the purge request's `now_ms`.
//! - `Sweep { now_ms, limit, retire_through }` in place of `ExpireStreams`:
//!   it removes up to `limit` expired streams, then folds up to `limit` end
//!   records at or below `retire_through` that the group held before the
//!   entry. It answers `Swept { expired, retired }`, and the group write
//!   answer and `ResponseKind` follow.
//! - an import gives every restored version zero counters, counted since
//!   the import's history, and retires the backup's bucket totals. A
//!   restore refuses a backup with two usage rows of one bucket
//!   (`StreamSnapshotError::DuplicateBucketUsage`, in the group engine
//!   error).
//! - `AppendResponse` without `stream_append_count`.
//! - group snapshots carry the ledger: each stream frame its version's
//!   usage (field 24), each end record a frame (`end_record`, frame 8,
//!   after the cold-GC frames), and the header the group's `usage_seq`,
//!   `retired_through`, retired rows and genesis history id (fields 13 to
//!   16), the scalars required. Frame field 3, the per-stream append
//!   counts, is reserved. An install restores through `restore_group`,
//!   which requires each bucket's totals (header field 5) to equal its
//!   retired usage plus its live versions plus its end records, and
//!   refuses what does not fit with `StreamSnapshotError::Usage`
//!   (`UsageSnapshotError`, `UsageCounter`), in the group engine error.
//!
//! A binary reads exactly one epoch: it refuses data and peers from any
//! other epoch loudly, before it writes anything, and it does not convert
//! them.
//!
//! Every version number that an earlier binary checks is set equal to
//! [`FORMAT_EPOCH`]: the Raft WAL journal header and state files, the Raft
//! gRPC protocol and group snapshots. The data
//! directory and the object-storage namespace carry a marker with the epoch,
//! and every group snapshot starts with a format-epoch frame. Cold-index
//! pages carry a page version of their own.
//!
//! A backup carries [`BACKUP_FORMAT_VERSION`] instead. It covers the
//! manifest, each group's `StreamSnapshot` in compact MessagePack with every
//! type it contains, and the cold objects a backup references (cold-index
//! pages and the cold key layout), in encoding and in meaning. It is the
//! format epoch that last changed one of them, so it never exceeds
//! [`FORMAT_EPOCH`]. Backup format 4 is written by 0.8, backup format 5 by
//! 0.9, 0.10 and 0.11. From backup format 5 on, a group object's first field
//! is its backup format.
//!
//! Policy:
//!
//! - Within an epoch, a release must not change apply-time semantics,
//!   persisted formats or the Raft RPC protocol.
//! - Any change that does so bumps [`FORMAT_EPOCH`] and requires a fresh
//!   install: new object-storage prefixes and empty data directories. If a
//!   future release needs an apply-time change under a rolling upgrade, it adds
//!   a narrowly scoped gate at that point.
//! - A change to what a backup holds also sets [`BACKUP_FORMAT_VERSION`] to
//!   the new epoch, and the release notes say that backups of the earlier
//!   format no longer restore.
//! - There is no migration between epochs. Data of an older epoch moves by
//!   `ursulactl` backup and restore when its backup format is this binary's,
//!   and otherwise by export and re-import through the applications that own
//!   it. Neither the server nor `ursulactl` converts it. 0.11 refuses every
//!   epoch-6 data directory, object-storage prefix, peer and group snapshot.
//!   It restores backups of backup format 5, which epochs 6 and 7 did not
//!   change.
//! - Epoch 2 was frozen when 0.6.0 was tagged, epoch 3 when 0.7.0 was tagged,
//!   epoch 4 when 0.8.0 was tagged, epoch 5 when 0.9.0 was tagged, epoch 6
//!   when 0.10.0 was tagged. Epoch 7 is frozen when 0.11.0 is tagged.
//!   Main builds before that tag are not supported on each other's data;
//!   every deployment of such a build starts from fresh prefixes and empty
//!   data directories.

/// The format epoch this binary reads and writes.
pub const FORMAT_EPOCH: u32 = 7;

/// The backup format this binary reads and writes: the format epoch that last
/// changed what a backup holds.
pub const BACKUP_FORMAT_VERSION: u32 = 5;

const _: () = assert!(BACKUP_FORMAT_VERSION <= FORMAT_EPOCH);

/// The text of every refusal of data of another format epoch:
/// `{subject} {found}`, the epoch this binary reads, that nothing converts
/// between epochs, and how data moves instead: by backup and restore when
/// its backup format is this binary's, as epoch-6 data's is, and otherwise
/// by export and re-import. Messages name epochs, never the binary's own
/// crate version.
pub fn format_epoch_refusal(subject: &str, found: &str) -> String {
    format!(
        "{subject} {found}; this binary reads format epoch {FORMAT_EPOCH} only and does not \
         convert other epochs (move data by `ursulactl` backup and restore when its backup \
         format is {BACKUP_FORMAT_VERSION}, otherwise by export and re-import)"
    )
}

/// The text of every refusal of a backup of another backup format:
/// `{subject} {found}` and the backup format this binary reads and writes.
pub fn backup_format_refusal(subject: &str, found: &str) -> String {
    format!(
        "{subject} {found}; this binary reads and writes backup format \
         {BACKUP_FORMAT_VERSION} only and does not convert other formats"
    )
}
