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
//! The file also records the recovery epoch in which the journal was last
//! read back in full (see `run_state`), so a journal that a crash interrupted
//! before it was read is still read as a verified prefix.

use std::collections::BTreeMap;
use std::path::Path;
use std::path::PathBuf;

use openraft::alias::VoteOf;
use serde::Deserialize;
use serde::Serialize;

use super::journal::JournalError;
use super::state_file;
use super::state_file::StateFileError;
use super::state_file::StateFileKind;
use crate::types::UrsulaRaftTypeConfig;

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
}

impl GroupLogState {
    /// Whether the replica ever persisted an entry or a purge of the group.
    pub fn is_initialized(self) -> bool {
        self != Self::Empty
    }
}

/// What a core's metadata file holds for one raft group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GroupMetadata {
    pub(crate) vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    pub(crate) log: GroupLogState,
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

    #[cfg(all(test, not(madsim)))]
    pub(crate) fn group(&self, group_id: u32) -> GroupMetadata {
        self.groups.get(&group_id).copied().unwrap_or_default()
    }

    pub(crate) fn groups(&self) -> impl Iterator<Item = (u32, GroupMetadata)> + '_ {
        self.groups
            .iter()
            .map(|(group_id, group)| (*group_id, *group))
    }

    /// Records `vote` for `group_id`; whether anything changed.
    pub(crate) fn set_vote(&mut self, group_id: u32, vote: VoteOf<UrsulaRaftTypeConfig>) -> bool {
        let group = self.groups.entry(group_id).or_default();
        let changed = group.vote != Some(vote);
        group.vote = Some(vote);
        changed
    }

    /// Records the first entry or purge of an empty `group_id` as `first`;
    /// whether anything changed. A group that is not empty keeps its state.
    pub(crate) fn initialize(&mut self, group_id: u32, first: GroupLogState) -> bool {
        let group = self.groups.entry(group_id).or_default();
        if group.log.is_initialized() || !first.is_initialized() {
            return false;
        }
        group.log = first;
        true
    }

    /// Moves an initialized `group_id` to `state`, into or out of recovery;
    /// whether anything changed. An empty group stays empty.
    pub(crate) fn set_log_state(&mut self, group_id: u32, state: GroupLogState) -> bool {
        let Some(group) = self.groups.get_mut(&group_id) else {
            return false;
        };
        if !group.log.is_initialized() || !state.is_initialized() || group.log == state {
            return false;
        }
        group.log = state;
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

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::CoreMetadata;
    use super::GroupLogState;
    use super::GroupMetadata;
    use super::core_metadata_path;
    use super::mark_core_recovering;

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
}
