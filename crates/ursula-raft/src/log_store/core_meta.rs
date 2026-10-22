//! The metadata file in each core journal's directory.
//!
//! It holds, for every raft group on the core, the group's vote and the state
//! of the group's log on this replica ([`GroupLogState`]). Neither may be lost
//! in any crash, so the file is always replaced with an `fsync`
//! ([`state_file::write`]), whatever the journal's fsync policy. Votes change
//! only during elections and the log state only when a group is initialized
//! or enters or leaves recovery, which keeps this cost off the append path,
//! and a vote no longer forces an `fsync` of the shared journal.
//!
//! A group's log state leaves [`GroupLogState::Empty`] before its first entry
//! or purge is acknowledged and never returns to it. A replica whose log is
//! empty but whose state is not knows it once held the group's log.
//!
//! Each group's entry also names the group's [`LogGeneration`] on this
//! replica: the journal and the snapshot record keep only what belongs to
//! it.
//!
//! It also holds a group's recovery plan ([`RecoveryRecord`]) while the
//! replica waits for the history event that satisfies it (#581 section
//! 4.5).
//!
//! The file also records the recovery epoch in which the journal was last
//! read back in full (see `run_state`), so a journal that a crash interrupted
//! before it was read is still read as a verified prefix.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use openraft::alias::LogIdOf;
use openraft::alias::VoteOf;
use serde::Deserialize;
use serde::Serialize;

use super::journal::JournalError;
use super::state_file;
use super::state_file::StateFileError;
use super::state_file::StateFileKind;
use crate::types::UrsulaRaftTypeConfig;

/// The generation of one raft group's log on this replica: 0 until the
/// replica's first retirement of the group, one more after each.
///
/// A core journal interleaves the frames of its groups, so the frames of a
/// retired log cannot be erased in place, and a deleted segment may come
/// back. Every journal record, the group's metadata entry and its snapshot
/// record therefore name the generation they belong to, and only what
/// belongs to the generation the metadata names is read back: older frames
/// are dead bytes the age rule frees, and an older snapshot record restores
/// nothing. Until replica teardown lands, every group stays at
/// [`LogGeneration::FIRST`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct LogGeneration(u64);

impl LogGeneration {
    /// A group's generation before its first retirement on this replica.
    pub const FIRST: Self = Self(0);

    pub const fn new(generation: u64) -> Self {
        Self(generation)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// The state of one raft group's log on this replica.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupLogState {
    /// The replica never persisted an entry or a purge of the group.
    #[default]
    Empty,
    /// The replica's log holds every entry it acknowledged.
    Initialized,
    /// The replica's log may be missing entries it acknowledged: a host
    /// crash or an I/O failure may have cost it its unsynced tail, or its
    /// history before this log is unknown. The replica stays out of
    /// elections until its recovery gate opens.
    Recovering,
    /// The replica's log of the group was torn down: the metadata names the
    /// generation after the retired one and holds no vote and no recovery
    /// plan for it. It counts as initialized, so the replica never joins a
    /// new group's first election or answers as empty. Only a retirement
    /// records it, and only the replica's reopen leaves it. Nothing records
    /// it before replica teardown lands.
    Retired,
}

impl GroupLogState {
    /// Whether the replica ever persisted an entry or a purge of the group.
    pub fn is_initialized(self) -> bool {
        self != Self::Empty
    }

    /// The state of a log a store may open: every state but
    /// [`Self::Retired`], which no store opens.
    pub(crate) fn held(self) -> Option<HeldLogState> {
        match self {
            Self::Empty => Some(HeldLogState::Empty),
            Self::Initialized => Some(HeldLogState::Initialized),
            Self::Recovering => Some(HeldLogState::Recovering),
            Self::Retired => None,
        }
    }
}

/// The state of a log a store holds open: a [`GroupLogState`] other than
/// `Retired`. A retired log opens only through its reopen, which records it
/// recovering first, so no store runs on one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HeldLogState {
    Empty,
    Initialized,
    Recovering,
}

impl HeldLogState {
    /// The state as the metadata file records it.
    pub(crate) fn durable(self) -> GroupLogState {
        match self {
            Self::Empty => GroupLogState::Empty,
            Self::Initialized => GroupLogState::Initialized,
            Self::Recovering => GroupLogState::Recovering,
        }
    }
}

/// A recovery plan this replica holds (#581 section 4.5): it votes only for
/// `leader`, at a term above `leader_term`, and the plan is satisfied once
/// the replica applied a history event at a log index above `point`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryRecord {
    /// The node the plan elects.
    pub leader: u64,
    /// The plan's leader's last log index after the plan.
    pub point: u64,
    /// The plan term: the leader's vote term when it took the plan. Every
    /// campaign the leader starts after taking it is above.
    pub leader_term: u64,
    /// Whether the plan cuts this replica's log, and how far it got.
    pub cut: PlanCut,
}

/// The cut of a recovery plan on one replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanCut {
    /// The plan keeps every entry.
    NoCut,
    /// The log keeps entries up to `keep`, the committed entry the replica
    /// recorded. The cut runs while the group restarts, before its new Raft
    /// core.
    Pending { keep: LogIdOf<UrsulaRaftTypeConfig> },
    /// The cut ran.
    Done,
}

/// What a core's metadata file holds for one raft group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GroupMetadata {
    pub(crate) vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    pub(crate) log: GroupLogState,
    /// The recovery plan the replica holds, until it is satisfied.
    pub(crate) recovery: Option<RecoveryRecord>,
    /// The generation of the group's log: replay keeps only its records.
    pub(crate) generation: LogGeneration,
}

/// The metadata of one core journal and of every raft group on the core.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CoreMetadata {
    /// The recovery epoch in which the journal was last read in full.
    verified_epoch: u64,
    groups: BTreeMap<u32, GroupMetadata>,
}

/// The metadata file of the core journal in the directory `core_dir`.
pub(crate) fn core_metadata_path(core_dir: &Path) -> PathBuf {
    core_dir.join(CORE_METADATA_FILE)
}

/// The metadata file's name in a core journal directory.
const CORE_METADATA_FILE: &str = "journal.meta";

impl CoreMetadata {
    /// Reads the metadata file at `path`; empty when there is none.
    pub(crate) fn load(path: &Path) -> Result<Self, StateFileError> {
        Self::load_with_presence(path).map(|(metadata, _missing)| metadata)
    }

    pub(crate) fn load_with_presence(path: &Path) -> Result<(Self, bool), StateFileError> {
        let metadata = state_file::read(StateFileKind::CoreMetadata, path)?;
        let missing = metadata.is_none();
        Ok((metadata.unwrap_or_default(), missing))
    }

    /// Replaces the metadata file at `path` with `self`. Returns the number of
    /// `fsync`s.
    pub(crate) fn store(&self, path: &Path) -> Result<u64, JournalError> {
        let mut temp = path.as_os_str().to_owned();
        temp.push(".tmp");
        state_file::write(StateFileKind::CoreMetadata, path, Path::new(&temp), self)
    }

    pub(crate) fn verified_epoch(&self) -> u64 {
        self.verified_epoch
    }

    /// Records that the journal was read in full in `epoch`; whether
    /// anything changed.
    pub(crate) fn set_verified_epoch(&mut self, epoch: u64) -> bool {
        let changed = self.verified_epoch != epoch;
        self.verified_epoch = epoch;
        changed
    }

    pub(crate) fn group(&self, group_id: u32) -> GroupMetadata {
        self.groups.get(&group_id).copied().unwrap_or_default()
    }

    /// Whether some group holds state here: a vote, a log state other than
    /// [`GroupLogState::Empty`], a recovery plan or a later log generation.
    pub(crate) fn holds_group_state(&self) -> bool {
        self.groups
            .values()
            .any(|group| *group != GroupMetadata::default())
    }

    pub(crate) fn groups(&self) -> impl Iterator<Item = (u32, GroupMetadata)> + '_ {
        self.groups
            .iter()
            .map(|(group_id, group)| (*group_id, *group))
    }

    /// Replaces `group_id`'s entry with `group`, for tests that set up a
    /// state no write records yet, such as a retired log.
    #[cfg(all(test, not(madsim)))]
    pub(crate) fn set_group(&mut self, group_id: u32, group: GroupMetadata) {
        self.groups.insert(group_id, group);
    }

    /// Records `vote` for `group_id`; whether anything changed.
    pub(crate) fn set_vote(&mut self, group_id: u32, vote: VoteOf<UrsulaRaftTypeConfig>) -> bool {
        let group = self.groups.entry(group_id).or_default();
        let changed = group.vote != Some(vote);
        group.vote = Some(vote);
        changed
    }

    /// Records `recovery` as `group_id`'s recovery plan, or removes it;
    /// whether anything changed.
    pub(crate) fn set_recovery(&mut self, group_id: u32, recovery: Option<RecoveryRecord>) -> bool {
        let group = self.groups.entry(group_id).or_default();
        let changed = group.recovery != recovery;
        group.recovery = recovery;
        changed
    }

    /// Records the first entry or purge of an empty `group_id` as `first`,
    /// initialized or recovering; whether anything changed. A group that is
    /// not empty keeps its state.
    pub(crate) fn initialize(&mut self, group_id: u32, first: GroupLogState) -> bool {
        let group = self.groups.entry(group_id).or_default();
        let first_entry = matches!(
            first,
            GroupLogState::Initialized | GroupLogState::Recovering
        );
        if group.log != GroupLogState::Empty || !first_entry {
            return false;
        }
        group.log = first;
        true
    }

    /// Moves `group_id` into or out of recovery, between
    /// [`GroupLogState::Initialized`] and [`GroupLogState::Recovering`];
    /// whether anything changed. An empty group stays empty and a retired
    /// one stays retired.
    pub(crate) fn set_log_state(&mut self, group_id: u32, state: GroupLogState) -> bool {
        let Some(group) = self.groups.get_mut(&group_id) else {
            return false;
        };
        let moves = matches!(
            (group.log, state),
            (GroupLogState::Initialized, GroupLogState::Recovering)
                | (GroupLogState::Recovering, GroupLogState::Initialized)
        );
        if !moves {
            return false;
        }
        group.log = state;
        true
    }

    /// Names `generation` as `group_id`'s generation, for a replay that
    /// found no metadata file and kept the group's highest generation among
    /// its frames; whether anything changed.
    pub(crate) fn adopt_generation(&mut self, group_id: u32, generation: LogGeneration) -> bool {
        if self.group(group_id).generation == generation {
            return false;
        }
        self.groups.entry(group_id).or_default().generation = generation;
        true
    }

    /// Moves every initialized group into recovery; whether anything changed.
    pub(crate) fn mark_recovering(&mut self) -> bool {
        let mut changed = false;
        for group in self.groups.values_mut() {
            if group.log == GroupLogState::Initialized {
                group.log = GroupLogState::Recovering;
                changed = true;
            }
        }
        changed
    }
}

/// Which generation of each group's log a replay of a core journal keeps:
/// the one the core's metadata names, or, without a metadata file, the
/// highest among the group's records, since nothing then says which life
/// of the group is current.
#[derive(Debug)]
pub(crate) enum ReplayGenerations {
    /// The metadata file names each group's generation; a group it does not
    /// list is at [`LogGeneration::FIRST`].
    Named(BTreeMap<u32, LogGeneration>),
    /// No metadata file: the highest generation found so far per group.
    Highest(BTreeMap<u32, LogGeneration>),
}

/// What replay does with one record, by its generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplayRecord {
    /// The record belongs to the generation replay keeps.
    Keep,
    /// The record belongs to another generation: a dead frame.
    Skip,
    /// The record is the first of a higher generation than the log replay
    /// built so far, which it replaces: that log's frames are dead.
    Replace,
}

impl ReplayGenerations {
    /// The rule for a replay of a journal whose metadata is `metadata`,
    /// which was `missing` from disk.
    pub(crate) fn of(metadata: &CoreMetadata, missing: bool) -> Self {
        if missing {
            Self::Highest(BTreeMap::new())
        } else {
            Self::Named(
                metadata
                    .groups()
                    .map(|(group_id, group)| (group_id, group.generation))
                    .collect(),
            )
        }
    }

    /// What replay does with a record of `group_id` of `generation`.
    pub(crate) fn admit(&mut self, group_id: u32, generation: LogGeneration) -> ReplayRecord {
        match self {
            Self::Named(named) => {
                if named.get(&group_id).copied().unwrap_or_default() == generation {
                    ReplayRecord::Keep
                } else {
                    ReplayRecord::Skip
                }
            }
            Self::Highest(highest) => match highest.get(&group_id).copied() {
                None => {
                    highest.insert(group_id, generation);
                    ReplayRecord::Keep
                }
                Some(kept) if kept.get() == generation.get() => ReplayRecord::Keep,
                Some(kept) if kept.get() > generation.get() => ReplayRecord::Skip,
                Some(_) => {
                    highest.insert(group_id, generation);
                    ReplayRecord::Replace
                }
            },
        }
    }

    /// The generation replay kept of each group it found, when no metadata
    /// named them: the metadata adopts them ([`CoreMetadata::adopt_generation`]).
    pub(crate) fn adopted(&self) -> impl Iterator<Item = (u32, LogGeneration)> + '_ {
        let highest = match self {
            Self::Named(_) => None,
            Self::Highest(highest) => Some(highest),
        };
        highest
            .into_iter()
            .flatten()
            .map(|(group_id, generation)| (*group_id, *generation))
    }
}

/// Moves every initialized group of the core whose metadata file is at
/// `path` into recovery, if the file exists; whether anything changed.
pub(crate) fn mark_core_recovering(path: &Path) -> Result<bool, MarkRecoveringError> {
    let mut metadata = CoreMetadata::load(path).map_err(MarkRecoveringError::Read)?;
    if !metadata.mark_recovering() {
        return Ok(false);
    }
    metadata.store(path).map_err(MarkRecoveringError::Write)?;
    Ok(true)
}

/// Failure to move a core's groups into recovery.
#[derive(Debug, thiserror::Error)]
pub enum MarkRecoveringError {
    #[error(transparent)]
    Read(StateFileError),
    #[error(transparent)]
    Write(JournalError),
}

/// The core metadata of [`sample_file`]: five groups, with every
/// [`GroupLogState`] and every [`PlanCut`], and distinct values in the fields
/// of one type.
#[cfg(test)]
fn sample_metadata() -> CoreMetadata {
    use openraft::vote::RaftLeaderId;

    type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

    let plan = |leader, point, leader_term, cut| RecoveryRecord {
        leader,
        point,
        leader_term,
        cut,
    };
    let groups = [
        (3, GroupMetadata {
            vote: Some(openraft::Vote::new_committed(7, 2)),
            log: GroupLogState::Initialized,
            recovery: Some(plan(2, 19, 6, PlanCut::Pending {
                keep: openraft::LogId::new(LeaderId::new(5, 1), 17),
            })),
            generation: LogGeneration(1),
        }),
        (4, GroupMetadata {
            vote: Some(openraft::Vote::new(8, 3)),
            log: GroupLogState::Recovering,
            recovery: Some(plan(3, 23, 9, PlanCut::NoCut)),
            generation: LogGeneration(4),
        }),
        (5, GroupMetadata {
            vote: Some(openraft::Vote::new(10, 4)),
            log: GroupLogState::Recovering,
            recovery: Some(plan(4, 29, 11, PlanCut::Done)),
            generation: LogGeneration(3),
        }),
        (6, GroupMetadata {
            vote: None,
            log: GroupLogState::Retired,
            recovery: None,
            generation: LogGeneration(2),
        }),
        (8, GroupMetadata {
            vote: Some(openraft::Vote::new(12, 6)),
            log: GroupLogState::Empty,
            recovery: None,
            generation: LogGeneration::FIRST,
        }),
    ];
    // Every log state and every cut, behind matches with no wildcard.
    for (_, group) in &groups {
        match group.log {
            GroupLogState::Empty
            | GroupLogState::Initialized
            | GroupLogState::Recovering
            | GroupLogState::Retired => {}
        }
        if let Some(record) = group.recovery {
            match record.cut {
                PlanCut::NoCut | PlanCut::Pending { .. } | PlanCut::Done => {}
            }
        }
    }
    CoreMetadata {
        verified_epoch: 5,
        groups: groups.into_iter().collect(),
    }
}

/// A core metadata file of five groups ([`sample_metadata`]) for the
/// persisted-envelopes fixture. It decodes to the metadata it was encoded
/// from and re-encodes to the same bytes.
#[cfg(test)]
pub(crate) fn sample_file() -> Vec<u8> {
    let metadata = sample_metadata();
    let bytes = state_file::encode(StateFileKind::CoreMetadata, &metadata);
    let decoded: CoreMetadata = state_file::decode(
        StateFileKind::CoreMetadata,
        Path::new(CORE_METADATA_FILE),
        &bytes,
    )
    .expect("the sample decodes");
    assert_eq!(decoded, metadata);
    assert_eq!(
        state_file::encode(StateFileKind::CoreMetadata, &decoded),
        bytes
    );
    bytes
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::CoreMetadata;
    use super::GroupLogState;
    use super::GroupMetadata;
    use super::LogGeneration;
    use super::ReplayGenerations;
    use super::ReplayRecord;
    use super::core_metadata_path;
    use super::mark_core_recovering;
    use super::sample_file;
    use super::sample_metadata;

    /// Storing core metadata writes the file as the persisted-envelopes
    /// fixture's sample holds it ([`sample_file`]).
    #[test]
    fn stored_core_metadata_is_the_fixture_sample() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = core_metadata_path(dir.path());
        let metadata = sample_metadata();
        metadata.store(&path).expect("store");
        assert_eq!(std::fs::read(&path).expect("read"), sample_file());
        assert_eq!(CoreMetadata::load(&path).expect("load"), metadata);
    }

    #[test]
    fn core_metadata_round_trips_votes_and_log_states() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = core_metadata_path(dir.path());
        assert_eq!(path, dir.path().join("journal.meta"));
        assert_eq!(
            CoreMetadata::load(&path).expect("no file yet"),
            CoreMetadata::default()
        );

        let mut metadata = CoreMetadata::default();
        let vote = openraft::Vote::new_committed(4, 2);
        assert!(metadata.set_vote(3, vote));
        assert!(!metadata.set_vote(3, vote), "the same vote changes nothing");
        assert!(metadata.initialize(7, GroupLogState::Initialized));
        assert!(
            !metadata.initialize(7, GroupLogState::Recovering),
            "only an empty group is initialized"
        );
        assert!(metadata.initialize(8, GroupLogState::Recovering));
        assert!(metadata.set_verified_epoch(2));
        assert!(!metadata.set_verified_epoch(2));
        metadata.store(&path).expect("store");

        let loaded = CoreMetadata::load(&path).expect("load");
        assert_eq!(loaded, metadata);
        assert_eq!(loaded.verified_epoch(), 2);
        assert_eq!(loaded.group(3), GroupMetadata {
            vote: Some(vote),
            log: GroupLogState::Empty,
            recovery: None,
            generation: LogGeneration::FIRST,
        });
        assert_eq!(loaded.group(7).log, GroupLogState::Initialized);
        assert_eq!(loaded.group(8).log, GroupLogState::Recovering);
        assert_eq!(loaded.group(9), GroupMetadata::default());
    }

    /// Recovery is entered and left only by initialized groups, and an
    /// initialized group never becomes empty again.
    #[test]
    fn log_states_move_between_initialized_and_recovering_only() {
        let mut metadata = CoreMetadata::default();
        assert!(!metadata.set_log_state(1, GroupLogState::Recovering));
        metadata.set_vote(1, openraft::Vote::new(1, 1));
        assert!(
            !metadata.set_log_state(1, GroupLogState::Recovering),
            "an empty group has nothing to recover"
        );
        assert!(!metadata.initialize(1, GroupLogState::Empty));
        assert!(metadata.initialize(1, GroupLogState::Initialized));
        assert!(metadata.initialize(2, GroupLogState::Initialized));
        assert!(metadata.mark_recovering());
        assert!(!metadata.mark_recovering());
        assert_eq!(metadata.group(1).log, GroupLogState::Recovering);
        assert!(metadata.set_log_state(1, GroupLogState::Initialized));
        assert!(!metadata.set_log_state(1, GroupLogState::Empty));
        assert_eq!(metadata.group(1).log, GroupLogState::Initialized);
        assert_eq!(metadata.group(2).log, GroupLogState::Recovering);
    }

    /// A retired log leaves `Retired` only by the reopen: neither recovery
    /// nor a first entry moves it, and nothing but a retirement enters it.
    #[test]
    fn a_retired_log_stays_retired() {
        let mut metadata = CoreMetadata::default();
        metadata.groups.insert(1, GroupMetadata {
            vote: None,
            log: GroupLogState::Retired,
            recovery: None,
            generation: LogGeneration::new(1),
        });
        assert!(metadata.group(1).log.is_initialized());
        assert!(!metadata.initialize(1, GroupLogState::Recovering));
        assert!(!metadata.set_log_state(1, GroupLogState::Recovering));
        assert!(!metadata.set_log_state(1, GroupLogState::Initialized));
        assert!(!metadata.mark_recovering());
        assert_eq!(metadata.group(1).log, GroupLogState::Retired);

        assert!(metadata.initialize(2, GroupLogState::Initialized));
        assert!(!metadata.set_log_state(2, GroupLogState::Retired));
        assert!(!metadata.initialize(3, GroupLogState::Retired));
        assert_eq!(metadata.group(2).log, GroupLogState::Initialized);
        assert_eq!(metadata.group(3).log, GroupLogState::Empty);
    }

    /// A replay without metadata names the generation it kept; adopting a
    /// group's own generation changes nothing and adds no entry.
    #[test]
    fn a_replay_without_metadata_adopts_the_generation_it_kept() {
        let mut metadata = CoreMetadata::default();
        assert!(!metadata.adopt_generation(4, LogGeneration::FIRST));
        assert_eq!(metadata, CoreMetadata::default());
        assert!(metadata.adopt_generation(4, LogGeneration::new(3)));
        assert!(!metadata.adopt_generation(4, LogGeneration::new(3)));
        assert_eq!(metadata.group(4).generation, LogGeneration::new(3));
        assert_eq!(metadata.group(4).log, GroupLogState::Empty);
    }

    /// With metadata, replay keeps the generation it names, and a group it
    /// does not list is at the first: records of any other generation, a
    /// resurrected segment of an older one included, are dead frames.
    #[test]
    fn replay_keeps_the_generation_the_metadata_names() {
        let mut metadata = CoreMetadata::default();
        metadata.groups.insert(1, GroupMetadata {
            vote: None,
            log: GroupLogState::Recovering,
            recovery: None,
            generation: LogGeneration::new(2),
        });
        let mut generations = ReplayGenerations::of(&metadata, false);
        let admitted = [
            (1, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (1, 2),
            (1, 1),
            (5, 0),
            (5, 1),
        ]
        .map(|(group, generation)| generations.admit(group, LogGeneration::new(generation)));
        assert_eq!(admitted, [
            ReplayRecord::Skip,
            ReplayRecord::Skip,
            ReplayRecord::Keep,
            ReplayRecord::Skip,
            ReplayRecord::Keep,
            ReplayRecord::Skip,
            ReplayRecord::Keep,
            ReplayRecord::Skip,
        ]);
        assert_eq!(generations.adopted().count(), 0);
    }

    /// Without metadata, replay keeps each group's highest generation, in
    /// whatever order its records come: a higher one replaces the log built
    /// so far, and a lower one after it is dead.
    #[test]
    fn replay_without_metadata_keeps_each_groups_highest_generation() {
        let mut generations = ReplayGenerations::of(&CoreMetadata::default(), true);
        let admitted = [(1, 0), (1, 0), (2, 3), (1, 2), (1, 0), (1, 2), (2, 1)]
            .map(|(group, generation)| generations.admit(group, LogGeneration::new(generation)));
        assert_eq!(admitted, [
            ReplayRecord::Keep,
            ReplayRecord::Keep,
            ReplayRecord::Keep,
            ReplayRecord::Replace,
            ReplayRecord::Skip,
            ReplayRecord::Keep,
            ReplayRecord::Skip,
        ]);
        assert_eq!(generations.adopted().collect::<Vec<_>>(), [
            (1, LogGeneration::new(2)),
            (2, LogGeneration::new(3)),
        ]);
    }

    #[test]
    fn a_recovering_node_marks_every_initialized_group_of_a_core() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = core_metadata_path(dir.path());
        assert!(!mark_core_recovering(&path).expect("no file: nothing to mark"));
        let mut metadata = CoreMetadata::default();
        metadata.initialize(1, GroupLogState::Initialized);
        metadata.set_vote(2, openraft::Vote::new(3, 2));
        metadata.store(&path).expect("store");
        assert!(mark_core_recovering(&path).expect("mark"));
        assert!(!mark_core_recovering(&path).expect("already marked"));
        let loaded = CoreMetadata::load(&path).expect("load");
        assert_eq!(loaded.group(1).log, GroupLogState::Recovering);
        assert_eq!(loaded.group(2).log, GroupLogState::Empty);
    }

    /// A core metadata file decodes to the metadata it was encoded from and
    /// re-encodes to the same bytes, whatever its groups hold.
    mod round_trip {
        use openraft::vote::RaftLeaderId;
        use proptest::prelude::*;

        use super::CoreMetadata;
        use super::GroupLogState;
        use super::GroupMetadata;
        use super::LogGeneration;
        use crate::log_store::PlanCut;
        use crate::log_store::RecoveryRecord;
        use crate::log_store::state_file;
        use crate::log_store::state_file::StateFileKind;

        type LeaderId = <crate::UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

        fn group() -> impl Strategy<Value = GroupMetadata> {
            let vote = proptest::option::of((any::<u64>(), any::<u64>(), any::<bool>()).prop_map(
                |(term, node_id, committed)| {
                    if committed {
                        openraft::Vote::new_committed(term, node_id)
                    } else {
                        openraft::Vote::new(term, node_id)
                    }
                },
            ));
            // Every log state and every cut, behind matches with no wildcard.
            let states = [
                GroupLogState::Empty,
                GroupLogState::Initialized,
                GroupLogState::Recovering,
                GroupLogState::Retired,
            ];
            for state in states {
                match state {
                    GroupLogState::Empty
                    | GroupLogState::Initialized
                    | GroupLogState::Recovering
                    | GroupLogState::Retired => {}
                }
            }
            let log = proptest::sample::select(states.to_vec());
            let cut = (0..3_u8, any::<u64>(), any::<u64>(), any::<u64>()).prop_map(
                |(kind, term, node_id, index)| {
                    let cut = match kind {
                        0 => PlanCut::NoCut,
                        1 => PlanCut::Pending {
                            keep: openraft::LogId::new(LeaderId::new(term, node_id), index),
                        },
                        _ => PlanCut::Done,
                    };
                    match cut {
                        PlanCut::NoCut | PlanCut::Pending { .. } | PlanCut::Done => cut,
                    }
                },
            );
            let recovery =
                proptest::option::of((any::<u64>(), any::<u64>(), any::<u64>(), cut).prop_map(
                    |(leader, point, leader_term, cut)| RecoveryRecord {
                        leader,
                        point,
                        leader_term,
                        cut,
                    },
                ));
            (vote, log, recovery, any::<u64>()).prop_map(|(vote, log, recovery, generation)| {
                GroupMetadata {
                    vote,
                    log,
                    recovery,
                    generation: LogGeneration::new(generation),
                }
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            #[test]
            fn core_metadata_round_trips(
                verified_epoch in any::<u64>(),
                groups in proptest::collection::btree_map(any::<u32>(), group(), 0..4),
            ) {
                let metadata = CoreMetadata { verified_epoch, groups };
                let bytes = state_file::encode(StateFileKind::CoreMetadata, &metadata);
                let decoded: CoreMetadata = state_file::decode(
                    StateFileKind::CoreMetadata,
                    std::path::Path::new("journal.meta"),
                    &bytes,
                )
                .expect("decodes");
                prop_assert_eq!(&decoded, &metadata);
                prop_assert_eq!(
                    state_file::encode(StateFileKind::CoreMetadata, &decoded),
                    bytes
                );
            }
        }
    }
}
