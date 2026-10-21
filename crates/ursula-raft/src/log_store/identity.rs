//! The run counter of a node's WAL root (`identity.bin`).
//!
//! Every run that opens the root counts itself: [`advance`] writes the run's
//! number, one more than the run before, durably and under the root's lock,
//! before the node binds its peer identity. A process is then named by its
//! root, its run and a nonce it draws ([`ProcessIncarnation`]), so a later
//! run of a root always names itself above every earlier one.
//!
//! The file also records whether a predecessor of this root may still run
//! ([`Predecessor`]) and has not been waited out (`predecessor_wait`). A run
//! that reads a predecessor that may live sets it, and only a finished wait
//! clears it, so no restart, a clean one included, forgets it.
//!
//! A root that holds prior state without the file refuses to start
//! ([`RaftWalError::MissingIdentity`]): a counter that started again would
//! make a later run read as an earlier one. A counter at `u32::MAX` refuses
//! too ([`RaftWalError::RunCounterExhausted`]).

use std::path::Path;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::placement::ProcessIncarnation;
use ursula_proto::placement::RootId;

use super::RaftWalError;
use super::run_state::Predecessor;
use super::state_file;
use super::state_file::StateFileKind;

const IDENTITY_FILE: &str = "identity.bin";

/// The identity file's contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IdentityFile {
    /// The runs that opened the root, this one included.
    pub(crate) run: u32,
    /// Whether a predecessor that may still run has not been waited out.
    pub(crate) predecessor_wait: bool,
}

/// What a run read of the identity file before it counts itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoredIdentity {
    /// A new root: no file and no prior state. The first run is 1, and its
    /// predecessor is not waited out.
    New,
    /// The file of the run before.
    Recorded(IdentityFile),
}

impl StoredIdentity {
    /// Whether the run before left a predecessor not waited out. A new root
    /// owes the wait, since nothing proves that no process runs on it.
    pub(super) fn predecessor_wait(self) -> bool {
        match self {
            Self::New => true,
            Self::Recorded(file) => file.predecessor_wait,
        }
    }
}

/// Reads the identity file of `root`, refusing a root that holds prior
/// state (`has_prior_state`) without one.
pub(super) fn read(root: &Path, has_prior_state: bool) -> Result<StoredIdentity, RaftWalError> {
    let path = root.join(IDENTITY_FILE);
    match state_file::read::<IdentityFile>(StateFileKind::Identity, &path)
        .map_err(RaftWalError::ReadIdentity)?
    {
        Some(file) => Ok(StoredIdentity::Recorded(file)),
        None if has_prior_state => Err(RaftWalError::MissingIdentity {
            root: root.to_owned(),
        }),
        None => Ok(StoredIdentity::New),
    }
}

/// Counts this run of `root` after `stored`, records it with whether the
/// predecessor must still be waited out, `fsync`ed, and returns this
/// process's incarnation with the nonce it draws. Called under the root's
/// lock, before the run records itself running and before the node binds
/// its peer identity, so no process names this run before it is durable.
pub(super) fn advance(
    root: &Path,
    root_id: RootId,
    stored: StoredIdentity,
    predecessor: Predecessor,
) -> Result<(IdentityFile, ProcessIncarnation), RaftWalError> {
    let run = match stored {
        StoredIdentity::New => 1,
        StoredIdentity::Recorded(file) => {
            file.run
                .checked_add(1)
                .ok_or_else(|| RaftWalError::RunCounterExhausted {
                    root: root.to_owned(),
                    run: file.run,
                })?
        }
    };
    let file = IdentityFile {
        run,
        predecessor_wait: predecessor == Predecessor::MayLive,
    };
    let path = root.join(IDENTITY_FILE);
    state_file::write(
        StateFileKind::Identity,
        &path,
        &path.with_extension("tmp"),
        &file,
    )
    .map_err(RaftWalError::RecordIdentity)?;
    Ok((file, ProcessIncarnation {
        root: root_id,
        run,
        nonce: rand::random(),
    }))
}

/// An identity file with distinct values in its fields, for the
/// persisted-envelopes fixture. It decodes to the file it was encoded from
/// and re-encodes to the same bytes.
#[cfg(test)]
pub(crate) fn sample_file() -> Vec<u8> {
    let file = IdentityFile {
        run: 7,
        predecessor_wait: true,
    };
    let bytes = state_file::encode(StateFileKind::Identity, &file);
    let decoded: IdentityFile =
        state_file::decode(StateFileKind::Identity, Path::new(IDENTITY_FILE), &bytes)
            .expect("the sample decodes");
    assert_eq!(decoded, file);
    assert_eq!(state_file::encode(StateFileKind::Identity, &decoded), bytes);
    bytes
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use ursula_config::WalFsync;
    use ursula_shard::StaticShardMap;

    use super::IDENTITY_FILE;
    use super::IdentityFile;
    use super::sample_file;
    use super::state_file;
    use super::state_file::StateFileKind;
    use crate::RaftWal;
    use crate::RaftWalError;
    use crate::log_store::RUN_STATE_FILE;
    use crate::log_store::run_state::Predecessor;

    fn identity(root: &std::path::Path) -> IdentityFile {
        state_file::read(StateFileKind::Identity, &root.join(IDENTITY_FILE))
            .expect("the identity file reads")
            .expect("an identity file")
    }

    /// Every start counts itself in `identity.bin` before `start` returns,
    /// which is before the node can bind its peer identity: the file holds
    /// the run the incarnation names, whatever the run before recorded, and
    /// a later run names itself above it on the same root. The record is
    /// `state_file::write`'s, which syncs it. This test reads the file back
    /// and does not lose power, so it shows the record, not its sync.
    #[tokio::test]
    async fn the_run_counter_is_recorded_before_the_start_returns() {
        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(1, 2).unwrap();
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        let first = wal.incarnation();
        assert_eq!(first.run, 1);
        // A new root owes its wait: nothing proves no process runs on it.
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 1,
            predecessor_wait: true,
        });
        assert_eq!(wal.predecessor(), Predecessor::MayLive);
        assert!(wal.empty_at_open());
        wal.shutdown().await.unwrap();
        drop(wal);
        // The root is still empty, so its predecessor may live.
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        let second = wal.incarnation();
        assert_eq!((second.root, second.run), (first.root, 2));
        assert_eq!(
            first.relation(&second),
            ursula_proto::placement::Relation::Newer
        );
        assert_eq!(identity(dir.path()).run, 2);
        assert_eq!(wal.predecessor(), Predecessor::MayLive);
        drop(wal);
        // A crash after the count leaves the counter where the crashed run
        // put it, and the next run counts on from there.
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        assert_eq!(wal.incarnation().run, 3);
        assert_eq!(identity(dir.path()).run, 3);
        wal.shutdown().await.unwrap();
    }

    /// A root whose metadata holds a vote is not empty, so after a clean
    /// end its predecessor is gone and, once no wait is owed, the run
    /// records none. An empty root's predecessor may live whatever ended,
    /// and a wait the identity file still owes outlasts a clean end on a
    /// root that is not empty.
    #[tokio::test]
    async fn a_root_with_a_vote_after_a_clean_end_owes_no_wait() {
        use openraft::storage::RaftLogStorage;

        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(1, 2).unwrap();
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        wal.shutdown().await.unwrap();
        drop(wal);
        // As a finished wait leaves it.
        let path = dir.path().join(IDENTITY_FILE);
        let waited = |run| {
            state_file::write(
                StateFileKind::Identity,
                &path,
                &path.with_extension("tmp"),
                &IdentityFile {
                    run,
                    predecessor_wait: false,
                },
            )
            .unwrap();
        };
        waited(4);
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        assert!(wal.empty_at_open());
        assert_eq!(wal.predecessor(), Predecessor::MayLive);
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 5,
            predecessor_wait: true,
        });
        let placement = topology.placement(ursula_shard::RaftGroupId(1)).unwrap();
        let metrics = ursula_runtime::RuntimeMetrics::new(1, 2).group_engine_metrics();
        let mut store = wal.open(placement, metrics).unwrap();
        store
            .save_vote(&crate::types::UrsulaVote::new(3, 1))
            .await
            .unwrap();
        drop(store);
        wal.shutdown().await.unwrap();
        drop(wal);
        // The root is no longer empty and the run before ended cleanly, so
        // only the wait the identity file still owes decides: a clean end
        // never shortens it.
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        assert!(!wal.empty_at_open());
        assert_eq!(wal.predecessor(), Predecessor::MayLive);
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 6,
            predecessor_wait: true,
        });
        wal.shutdown().await.unwrap();
        drop(wal);
        waited(6);
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        assert!(!wal.empty_at_open());
        assert_eq!(wal.predecessor(), Predecessor::Gone);
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 7,
            predecessor_wait: false,
        });
        wal.shutdown().await.unwrap();
    }

    /// A root with prior state and no identity file, or with a counter at
    /// its end, refuses to start and changes nothing.
    #[tokio::test]
    async fn the_identity_file_refuses_a_lost_or_exhausted_counter() {
        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(1, 2).unwrap();
        let wal = RaftWal::start(dir.path(), WalFsync::Never, &topology).unwrap();
        wal.shutdown().await.unwrap();
        drop(wal);
        let path = dir.path().join(IDENTITY_FILE);
        let run_state = std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap();

        std::fs::remove_file(&path).unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::MissingIdentity { .. })
        ));
        assert!(!path.exists());
        assert_eq!(
            std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap(),
            run_state
        );

        let exhausted = IdentityFile {
            run: u32::MAX,
            predecessor_wait: false,
        };
        state_file::write(
            StateFileKind::Identity,
            &path,
            &path.with_extension("tmp"),
            &exhausted,
        )
        .unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::RunCounterExhausted { run: u32::MAX, .. })
        ));
        assert_eq!(identity(dir.path()), exhausted);
        assert_eq!(
            std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap(),
            run_state
        );

        // A damaged file is reported as itself and kept.
        std::fs::write(&path, b"URSWIDNT").unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::ReadIdentity(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), b"URSWIDNT");
    }

    /// A WAL root records its identity file as the persisted-envelopes
    /// fixture's sample holds it ([`sample_file`]).
    #[test]
    fn a_recorded_identity_file_is_the_fixture_sample() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(IDENTITY_FILE);
        state_file::write(
            StateFileKind::Identity,
            &path,
            &path.with_extension("tmp"),
            &IdentityFile {
                run: 7,
                predecessor_wait: true,
            },
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), sample_file());
    }
}
