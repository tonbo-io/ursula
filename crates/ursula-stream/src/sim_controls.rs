//! Defences a simulation's negative control turns off, to show that a DST
//! invariant catches their loss (`crates/ursula-sim`). The switches live in
//! this crate, which every crate with a defence depends on.
//!
//! Only `cfg(madsim)` builds can turn one off. madsim runs a simulation on
//! one OS thread, so the switches are thread-local and a simulation sees only
//! its own. Every other build has no switch: each query is a constant
//! `false`.

/// One defence of the state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Defence {
    /// Row 11: a producer record from an earlier history closes its epoch.
    HistoryFence,
    /// G1: a command that names a `Stream-Incarnation` applies only to that
    /// incarnation.
    IncarnationCheck,
    /// An import of a backup the group was already restored from answers
    /// `already` instead of rewinding again.
    ImportKey,
    /// A recovery plan that cuts the logs cuts them inside its accept job,
    /// through an in-process restart of the group, which also demotes a
    /// replica that leads or campaigns. Off, there is no such restart and
    /// the cut waits for the member's next start, as before the #581 review
    /// (B1).
    RecoveryCut,
    /// A member of a recovery plan refuses the plan's leader while its log
    /// holds an entry above the point at or below the plan term (M1 of the
    /// second #581 review).
    PlanLogGuard,
    /// A replica holding a recovery plan takes replication only from a
    /// leader above the plan term (M1 of the second #581 review).
    PlanReplicationFence,
    /// A write whose body was read never redirects: a leadership refusal
    /// after the body answers `503` with `Retry-After` (#581 section 6.1).
    /// Off, the HTTP front door answers it as it would before the body, with
    /// a `307` to the leader its hint names.
    NoRedirectAfterBody,
    /// An immutable-read chunk that ends at the tail of an open stream is
    /// never marked, even when it fills its chunk (#581 section 5.3).
    ImmutableOpenTail,
    /// A follower answers an immutable-read chunk only when it can mark it,
    /// and forwards the others to the leader. Off, it answers every chunk
    /// from its own state, as a base-protocol read, which hides its own open
    /// tail (#567 review finding 4).
    FollowerMarkCheck,
    /// A leader linearizes an immutable-read chunk it cannot mark, and a
    /// `416`, before it answers (#581 section 5.3, m1 of the step 5 review).
    ImmutableLeaderBarrier,
    /// The plan's leader is checked against the membership it keeps after a
    /// cut, and drops a plan whose cut leaves it no vote (#613, R2). Off, it
    /// is checked against its effective membership even when the plan cuts,
    /// and keeps such a plan, as before #613.
    PlanKeptMembership,
    /// The plan's leader restarts with the plan term as its uncommitted vote
    /// (#613), so its first election is above the plan term. OpenRaft then
    /// also refuses appends from a leader whose vote is below that one.
    PlanTermVote,
    /// A replica refuses an append whose entries differ from its log at or
    /// below its commit marker (#613). Off, a plan led from a replica that
    /// knew less overwrites a replica's committed entries.
    CommitScreen,
    /// A member of a recovery plan takes it whatever its own membership says
    /// of the plan's leader (#613, R3). Off, it refuses a leader outside its
    /// own membership, as before #613.
    PlanMemberCheck,
    /// A replica refuses a snapshot install that would replace entries at or
    /// below its commit marker (#613). Off for installs only, the append
    /// screen still runs.
    CommitScreenInstall,
    /// The install screen admits a snapshot of any other history that kept
    /// entries up to the marker, not only of one that directly replaced the
    /// replica's own (#613, as at `b9de4186`).
    CommitScreenChain,
    /// The commit screen reads the store's marker alone and skips every
    /// index at or below the last purged entry (#613, as at `20bb1b88`).
    CommitScreenPurged,
    /// A node answers a write with a refusal, such as `NO_LEADER`, only when
    /// nothing of it applied (extensions Section 11.1). Off, the HTTP front
    /// door answers each producer write that applied now with `503
    /// NO_LEADER`, as a node that took OpenRaft's forward after a commit for
    /// a refusal before the proposal would: the write's repeat then gets its
    /// receipt.
    RefusalMeansUnapplied,
    /// A removal no snapshot covers makes its group due once it is a quarter
    /// of `raft.deleted_data_max_age` old (#669). Off, only the removed
    /// bytes count toward the group's snapshot, so a small removal on a
    /// quiet group stays in its snapshot and log.
    RemovalFloor,
    /// The snapshot driver purges a group's log through the snapshot that
    /// covers its removals (#669). Off, OpenRaft keeps
    /// `raft.max_in_snapshot_log_to_keep` entries below it, the removed
    /// stream's records among them.
    ScrubPurge,
    /// A purge through a covering snapshot starts only while the group's
    /// snapshot work is idle, and holds the group's builds until it lands
    /// (#669). Off, a build that ends while a leader defers that purge
    /// schedules a lower one, which OpenRaft's debug assertion reports.
    ScrubPurgeHold,
    /// A replica that restores its snapshot at startup purges its log
    /// through it (#669). Off, a restart between the snapshot that dropped a
    /// stream and the purge through it leaves the stream's records in the
    /// log.
    RestorePurge,
    /// A Raft log journal segment that has held dead frames (of entries
    /// purged, truncated or copied away) for a quarter of
    /// `raft.deleted_data_max_age` is sealed and freed (#669). Off, only
    /// the journal's size frees segments, so a small journal keeps a
    /// removed stream's records in its files.
    WalDirtyAge,
    /// Each group's leader proposes `ExpireStreams` for the streams that
    /// expired in its group, in its background tidy pass (#669). Off, an
    /// expired stream that no request names stays in its group: no other
    /// command removes a stream it does not name.
    ExpirySweep,
    /// A group engine's shutdown returns only once the work OpenRaft runs
    /// for it on tasks of its own ended: its state-machine worker, the
    /// snapshot builds it spawned and its log readers (#709). Off, the
    /// shutdown returns once RaftCore stopped, and that work may still hold
    /// the group's log store and change its state.
    EngineWorkWait,
    /// A group with a recovery gate runs OpenRaft with log reversion
    /// allowed, so a leader rewinds a follower that answers a conflict below
    /// the index it had matched and replicates its log to it again. Off,
    /// OpenRaft treats that conflict as a bug and its debug assertion
    /// reports it.
    FollowerRewind,
    /// A node holds each snapshot object it writes, a build's or an install
    /// copy's, from before its PUT (R2 of #687). Off, the lease is taken once
    /// the PUT answered, before the HEAD, so a GC pass whose LIST shows the
    /// PUT and whose read of the holds falls in the PUT's answer may delete
    /// the object the build then commits.
    LeaseBeforePut,
    /// A GC pass reads the holds after its LIST returned (R4 of #687). Off,
    /// it reads them before the LIST, so an object leased and written while
    /// the LIST runs is listed but not held, and the pass deletes it.
    HoldsAfterList,
    /// A follower that installs a peer's snapshot writes its own copy and
    /// commits the copy (R5 of #687). Off, the install commits the sender's
    /// pointer, uncopied, so the follower names an object that only its
    /// sender holds and deletes.
    InstallCopy,
    /// A send holds the object it names until its RPC returns (R6 of #687).
    /// Off, the snapshot a send reads carries no lease, so the sender's next
    /// build and the pass after it may delete the object while the
    /// receiver still reads it.
    SendHold,
    /// A node's GC passes delete every object of its own directory that
    /// nothing holds (#687). Off, a pass lists and deletes nothing, so every
    /// replaced object stays.
    SnapshotGc,
    /// No GC pass runs on a group's holds before its restore ended, so the
    /// holds name the restored object when the first pass reads them (R4 of
    /// #687). Off, the restore marks the holds restored first and runs the
    /// pass that owes before it reads the pointer, so the pass finds the
    /// persisted object unheld and deletes it.
    NoPassBeforeRestore,
    /// A proposal that names a cold object its leader uploaded is appended
    /// only under the committed leader the upload was planned under, and
    /// one without a fence is refused (F14j). Off, it is proposed under any
    /// leadership, so an upload that waited across a re-election commits
    /// under the next one.
    UploadFence,
    /// Before the meta group forms, the genesis node's drivers ask every
    /// `raft.peers` node with the genesis probe, so a node that holds meta
    /// history but is no meta voter holds the genesis back (design 3.5). Off,
    /// they ask only the claimed meta voters, so meta voters that lost
    /// everything form a second meta group beside the learners' history.
    GenesisEveryPeer,
    /// A node whose data journals hold records opens a lost meta journal
    /// `Recovering` (D9's S1 rule), so its closed gate rejoins the meta group
    /// it belonged to. Off, it opens the lost journal empty, so meta voters
    /// whose meta directories are gone form a second meta group.
    MetaOverDataHistory,
    /// The meta journal syncs every append, whatever the data journals run.
    /// Off, it runs the data journals' policy, so a power loss under `never`
    /// loses acknowledged meta writes.
    MetaAlwaysSync,
    /// A data replica's seed role comes from its placement view: the
    /// bootstrap driver probes and initializes only while the view shows
    /// the replica as its `Seeded` group's initializer, and it initializes
    /// only on a confirmed view (N3 of design 15.2). Off, the
    /// seed role answers `Initializer` for any view, so every seed voter
    /// races to initialize its group.
    InitializeOnlySeeded,
}

#[cfg(madsim)]
mod switches {
    use std::cell::Cell;

    use super::Defence;

    std::thread_local! {
        static OFF: Cell<[bool; 37]> = const { Cell::new([false; 37]) };
        static FIRED: Cell<[u64; 37]> = const { Cell::new([0; 37]) };
    }

    fn slot(defence: Defence) -> usize {
        match defence {
            Defence::HistoryFence => 0,
            Defence::IncarnationCheck => 1,
            Defence::ImportKey => 2,
            Defence::RecoveryCut => 3,
            Defence::PlanLogGuard => 4,
            Defence::PlanReplicationFence => 5,
            Defence::NoRedirectAfterBody => 6,
            Defence::ImmutableOpenTail => 7,
            Defence::FollowerMarkCheck => 8,
            Defence::ImmutableLeaderBarrier => 9,
            Defence::PlanKeptMembership => 10,
            Defence::PlanTermVote => 11,
            Defence::CommitScreen => 12,
            Defence::PlanMemberCheck => 13,
            Defence::CommitScreenInstall => 14,
            Defence::CommitScreenChain => 15,
            Defence::CommitScreenPurged => 16,
            Defence::RefusalMeansUnapplied => 17,
            Defence::RemovalFloor => 18,
            Defence::ScrubPurge => 19,
            Defence::ScrubPurgeHold => 20,
            Defence::RestorePurge => 21,
            Defence::WalDirtyAge => 22,
            Defence::ExpirySweep => 23,
            Defence::EngineWorkWait => 24,
            Defence::FollowerRewind => 25,
            Defence::LeaseBeforePut => 26,
            Defence::HoldsAfterList => 27,
            Defence::InstallCopy => 28,
            Defence::SendHold => 29,
            Defence::SnapshotGc => 30,
            Defence::NoPassBeforeRestore => 31,
            Defence::UploadFence => 32,
            Defence::GenesisEveryPeer => 33,
            Defence::MetaOverDataHistory => 34,
            Defence::MetaAlwaysSync => 35,
            Defence::InitializeOnlySeeded => 36,
        }
    }

    pub(super) fn is_off(defence: Defence) -> bool {
        OFF.with(|off| off.get().get(slot(defence)).copied().unwrap_or(false))
    }

    pub(super) fn note_fired(defence: Defence) {
        FIRED.with(|fired| {
            let mut all = fired.get();
            if let Some(entry) = all.get_mut(slot(defence)) {
                *entry = entry.saturating_add(1);
            }
            fired.set(all);
        });
    }

    pub(super) fn fired(defence: Defence) -> u64 {
        FIRED.with(|fired| fired.get().get(slot(defence)).copied().unwrap_or(0))
    }

    pub(super) fn reset_fired() {
        FIRED.with(|fired| fired.set([0; 37]));
    }

    pub(super) fn set_off(defence: Defence, value: bool) {
        OFF.with(|off| {
            let mut all = off.get();
            if let Some(entry) = all.get_mut(slot(defence)) {
                *entry = value;
            }
            off.set(all);
        });
    }
}

/// Whether a negative control turned `defence` off on this thread.
#[inline]
pub fn off(defence: Defence) -> bool {
    #[cfg(madsim)]
    {
        switches::is_off(defence)
    }
    #[cfg(not(madsim))]
    {
        let _ = defence;
        false
    }
}

/// Turns `defence` off (or back on) for this thread's simulation.
#[cfg(madsim)]
pub fn set_off(defence: Defence, value: bool) {
    switches::set_off(defence, value);
}

/// Records that `defence` decided an outcome on this thread: a negative
/// control's sibling test reads it to show which defence fired.
#[inline]
pub fn note_fired(defence: Defence) {
    #[cfg(madsim)]
    {
        switches::note_fired(defence);
    }
    #[cfg(not(madsim))]
    {
        let _ = defence;
    }
}

/// How often `defence` decided an outcome on this thread since
/// [`reset_fired`].
#[cfg(madsim)]
pub fn fired(defence: Defence) -> u64 {
    switches::fired(defence)
}

/// Forgets what fired on this thread, before a simulation.
#[cfg(madsim)]
pub fn reset_fired() {
    switches::reset_fired();
}
