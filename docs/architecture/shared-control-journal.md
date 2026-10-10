# Shared data and control journal

This draft proposes the root-format prerequisite for a durable maintenance authority.
The explicit constructor is not connected to production server configuration.
Managed conversion and its later adapters require design review before activation.
This does not enable a meta listener or create a second authority during standalone
startup. The planned adapters must share the existing per-core writer, journal
framing, rotation, reclaim, poison handling and `JournalDisk` I/O seam.

## WAL root boundary

Managed roots use **local WAL topology version 4**, not global format epoch 4.
The existing `topology.bin` header has version 3 for data-only roots and version
4 for explicitly managed roots. The serialized routing counts, existing data
journal frames, S3 formats and RPC protocol epoch remain unchanged.

`RaftWal::start` and `start_with` retain data-only behavior. Only the explicit
`start_managed` constructor can create or upgrade a managed root. It takes the
existing root lock, validates the immutable routing counts, atomically writes
the version-4 topology file, syncs the file and parent directory, and only then
continues recovery and returns a handle capable of opening cores. A write/sync
failure returns no handle. A crash after the durable upgrade requires an
explicit managed restart; it does not revert the root to data-only mode.

An existing data-only root is read without rewriting its records. Selecting a
WAL path does not upgrade it. Explicitly selecting managed mode upgrades only
the root marker. A managed root cannot subsequently start through the data-only
constructor. Deleting the marker is not a downgrade procedure: missing topology
with prior run-state/core files already fails closed.

Ursula 0.7's root-opening path reads `topology.bin` before journal recovery,
run-state writes or runtime construction. Its existing version-3 decoder
rejects the version-4 header before decoding. This applies to standalone/lazy
cores as well as static clusters. The same magic is retained deliberately so
the old version check is reached. The data-only constructor reads the root
exactly as 0.7 does and refuses a managed root with the same version error.
Only the managed constructor reads both root modes, and it never silently
downgrades a managed marker. No rollback conversion is
provided; retain a separate pre-upgrade root if binary rollback is required.

A guard in only the meta core's metadata is insufficient: standalone startup
can bind HTTP before opening that core, then serve another core. The root
boundary must be installed before any meta bytes are introduced. Future meta
adapters must use the explicit managed constructor; the codec alone does not
authorize managed startup. These PRs never delete roots or clean object storage.

## Envelope follow-up requirements

The planned internal journal group identifies either a data `RaftGroupId` or the meta
Raft group. The meta identifier cannot alias any valid data group, including
`u32::MAX`. The persisted entry payload distinguishes a stream command from a
control command. Data and meta adapters validate that namespace and payload
agree before handing an entry to OpenRaft.

The common journal index tracks log IDs, membership, committed and purged
positions independently for each internal group. Both adapters use this index
and the existing segmented writer; there is no second rotation, reclaim or
verified-prefix implementation. Votes and recovery metadata retain the existing
always-synced metadata path through the same I/O seam.

## Follow-up durability requirements

Meta entries and commit bookkeeping must be synced before their completion
callback, under both `Always` and `Never`. `Never` may relax stream payload
persistence; it cannot acknowledge a maintenance authority transition that a
power loss can erase. Membership entries preserve the durability rule added by
#439. A mixed writer batch satisfies the strongest durability requirement of
its records.

Recovery must reject malformed namespaces and payload mismatches before
mutation. Reclaim must preserve live data and meta records independently and
must not interpret a meta command as a stream command. Fault tests use
`SimDisk`, including power loss before/after append, sync, rotation, metadata
replacement and reclaim. Native tests cover actual file/parent-directory
failures. Callback ordering and durable ACK assertions run for both policies.

## Follow-up adapters

The durable meta OpenRaft adapter and authenticated transport follow the
storage envelope in separate PRs. They reuse the pure maintenance kernel and
canonical process identity. Authentication, action execution and admission
certificates are not provided by the codec, and no new public management
endpoint is enabled by this prerequisite.
