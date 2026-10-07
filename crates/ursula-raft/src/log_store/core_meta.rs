//! The metadata file next to each core journal.
//!
//! It holds, for every raft group on the core, the group's vote and whether
//! the group was ever initialized on this replica. Neither may be lost in any
//! crash, so the file is always replaced with an `fsync`
//! ([`state_file::write`]), whatever the journal's fsync policy. Votes change
//! only during elections, which keeps this cost off the append path, and a
//! vote no longer forces an `fsync` of the shared journal.
//!
//! `initialized` is set before a group's first entry or purge is acknowledged
//! and is never cleared. A replica whose log is empty but whose flag is set
//! knows it once held the group's log.
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

/// What a core's metadata file holds for one raft group.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct GroupMetadata {
    pub(crate) vote: Option<VoteOf<UrsulaRaftTypeConfig>>,
    pub(crate) initialized: bool,
}

/// The metadata of one core journal and of every raft group on the core.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CoreMetadata {
    /// The recovery epoch in which the journal was last read in full.
    verified_epoch: u64,
    groups: BTreeMap<u32, GroupMetadata>,
}

/// The metadata file of the core journal at `journal_path`.
pub(crate) fn core_metadata_path(journal_path: &Path) -> PathBuf {
    journal_path.with_extension("meta")
}

impl CoreMetadata {
    /// Reads the metadata file at `path`; empty when there is none.
    pub(crate) fn load(path: &Path) -> Result<Self, StateFileError> {
        Ok(state_file::read(StateFileKind::CoreMetadata, path)?.unwrap_or_default())
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

    #[cfg(test)]
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

    /// Marks `group_id` initialized; whether it was not before.
    pub(crate) fn mark_initialized(&mut self, group_id: u32) -> bool {
        let group = self.groups.entry(group_id).or_default();
        let changed = !group.initialized;
        group.initialized = true;
        changed
    }
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use super::CoreMetadata;
    use super::GroupMetadata;
    use super::core_metadata_path;

    #[test]
    fn core_metadata_round_trips_votes_and_initialized_flags() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = core_metadata_path(&dir.path().join("journal.bin"));
        assert_eq!(path, dir.path().join("journal.meta"));
        assert_eq!(
            CoreMetadata::load(&path).expect("no file yet"),
            CoreMetadata::default()
        );

        let mut metadata = CoreMetadata::default();
        let vote = openraft::Vote::new_committed(4, 2);
        assert!(metadata.set_vote(3, vote));
        assert!(!metadata.set_vote(3, vote), "the same vote changes nothing");
        assert!(metadata.mark_initialized(7));
        assert!(
            !metadata.mark_initialized(7),
            "initialized is never cleared"
        );
        assert!(metadata.set_verified_epoch(2));
        assert!(!metadata.set_verified_epoch(2));
        metadata.store(&path).expect("store");

        let loaded = CoreMetadata::load(&path).expect("load");
        assert_eq!(loaded, metadata);
        assert_eq!(loaded.verified_epoch(), 2);
        assert_eq!(loaded.group(3), GroupMetadata {
            vote: Some(vote),
            initialized: false,
        });
        assert_eq!(loaded.group(7), GroupMetadata {
            vote: None,
            initialized: true,
        });
        assert_eq!(loaded.group(9), GroupMetadata::default());
    }
}
