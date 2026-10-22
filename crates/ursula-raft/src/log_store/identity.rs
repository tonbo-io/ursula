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
//! And it records whether the meta group ever bound the root
//! ([`RootStanding`]). A root is created `Unbound`, and the first answer of
//! the meta group to its registration records it `Bound`
//! (`NodeWal::record_bound`) before that answer releases its data replicas. Nothing else decides the
//! standing: a refused root keeps what its meta replica wrote, a vote floor
//! say, and still stands `Unbound` after a restart, so it registers as a
//! root that holds nothing a bound process left and takes the slot once an
//! operator releases the old root.
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

/// How a root stands in the meta group, as its identity file records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RootStanding {
    /// The meta group answered no registration of the root yet. Its data
    /// replicas took part in nothing, so it holds nothing a bound process
    /// left, whatever its meta replica holds.
    Unbound,
    /// The meta group answered a registration of the root: the slot of its
    /// node id was bound to it.
    Bound,
}

/// The identity file's contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct IdentityFile {
    /// The runs that opened the root, this one included.
    pub(crate) run: u32,
    /// Whether a predecessor that may still run has not been waited out.
    pub(crate) predecessor_wait: bool,
    /// Whether the meta group ever bound the root.
    pub(crate) standing: RootStanding,
}

/// What a run read of the identity file before it counts itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StoredIdentity {
    /// A new root: no file and no prior state. The first run is 1, its
    /// predecessor is not waited out, and meta never bound it.
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

    /// How the root stands in meta: a new root is unbound.
    pub(super) fn standing(self) -> RootStanding {
        match self {
            Self::New => RootStanding::Unbound,
            Self::Recorded(file) => file.standing,
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
/// predecessor must still be waited out and the standing `stored` holds,
/// `fsync`ed, and returns this
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
        predecessor_wait: predecessor == Predecessor::MayLive && records_owed_wait(),
        standing: stored.standing(),
    };
    write(root, &file)?;
    Ok((file, ProcessIncarnation {
        root: root_id,
        run,
        nonce: rand::random(),
    }))
}

/// Whether the start records an owed wait. A negative control turns it off
/// (`Defence::OwedWait`), and a restart during the wait then owes nothing.
fn records_owed_wait() -> bool {
    use ursula_stream::sim_controls::Defence;
    if ursula_stream::sim_controls::off(Defence::OwedWait) {
        ursula_stream::sim_controls::note_fired(Defence::OwedWait);
        return false;
    }
    true
}

/// Records `file`, `fsync`ed: the whole file, replacing the one before.
/// The run's writes after its start go through
/// [`super::run_state::NodeWal`], one at a time, so none of them loses
/// another's field.
pub(super) fn write(root: &Path, file: &IdentityFile) -> Result<(), RaftWalError> {
    let path = root.join(IDENTITY_FILE);
    state_file::write(
        StateFileKind::Identity,
        &path,
        &path.with_extension("tmp"),
        file,
    )
    .map(|_bytes| ())
    .map_err(RaftWalError::RecordIdentity)
}

/// One identity file of each [`RootStanding`], by its persisted-envelopes
/// fixture name, each with its own run and wait. A new standing fails to
/// compile here until it has a sample.
#[cfg(test)]
fn sample_identities() -> [(&'static str, IdentityFile); 2] {
    let samples = [
        ("identity_file", IdentityFile {
            run: 7,
            predecessor_wait: true,
            standing: RootStanding::Bound,
        }),
        ("identity_file_unbound", IdentityFile {
            run: 8,
            predecessor_wait: false,
            standing: RootStanding::Unbound,
        }),
    ];
    for (_, file) in &samples {
        match file.standing {
            RootStanding::Unbound | RootStanding::Bound => {}
        }
    }
    samples
}

/// The identity files of [`sample_identities`], encoded, for the
/// persisted-envelopes fixture. Each decodes to the file it was encoded from
/// and re-encodes to the same bytes.
#[cfg(test)]
pub(crate) fn sample_files() -> Vec<(&'static str, Vec<u8>)> {
    sample_identities()
        .into_iter()
        .map(|(name, file)| {
            let bytes = state_file::encode(StateFileKind::Identity, &file);
            let decoded: IdentityFile =
                state_file::decode(StateFileKind::Identity, Path::new(IDENTITY_FILE), &bytes)
                    .expect("the sample decodes");
            assert_eq!(decoded, file);
            assert_eq!(state_file::encode(StateFileKind::Identity, &decoded), bytes);
            (name, bytes)
        })
        .collect()
}

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::time::Duration;

    use ursula_config::WalFsync;
    use ursula_proto::placement::NodeId;
    use ursula_runtime::NodeFence;
    use ursula_shard::StaticShardMap;

    use super::IDENTITY_FILE;
    use super::IdentityFile;
    use super::RootStanding;
    use super::sample_files;
    use super::sample_identities;
    use super::state_file;
    use super::state_file::StateFileKind;
    use crate::JournalTuning;
    use crate::RaftWal;
    use crate::RaftWalError;
    use crate::WaitFrom;
    use crate::log_store::RUN_STATE_FILE;
    use crate::log_store::run_state::Predecessor;

    fn identity(root: &std::path::Path) -> IdentityFile {
        state_file::read(StateFileKind::Identity, &root.join(IDENTITY_FILE))
            .expect("the identity file reads")
            .expect("an identity file")
    }

    fn record(root: &std::path::Path, file: &IdentityFile) {
        super::write(root, file).expect("the identity file records");
    }

    /// A start with a leased fence, so a run that owes its wait reports
    /// where the wait is measured from.
    fn leased_start(root: &std::path::Path, topology: &StaticShardMap) -> RaftWal {
        RaftWal::start_in_cluster(
            root,
            JournalTuning::new(WalFsync::Never),
            topology,
            NodeId::new(1),
            None,
            NodeFence::leased(Duration::from_secs(5)),
        )
        .expect("the WAL starts")
    }

    /// Saves a vote of group 1 on `wal`, so the root holds state.
    async fn save_a_vote(wal: &RaftWal, topology: &StaticShardMap) {
        use openraft::storage::RaftLogStorage;

        let placement = topology
            .placement(ursula_shard::RaftGroupId(1))
            .expect("group 1 is placed");
        let metrics = ursula_runtime::RuntimeMetrics::new(1, 2).group_engine_metrics();
        let mut store = wal
            .open(placement, ursula_control::Admission::Create, metrics)
            .expect("group 1 opens");
        store
            .save_vote(&crate::types::UrsulaVote::new(3, 1))
            .await
            .expect("the vote saves");
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
        // A new root owes its wait, since nothing proves no process runs on
        // it, and meta never bound it.
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 1,
            predecessor_wait: true,
            standing: RootStanding::Unbound,
        });
        assert_eq!(wal.predecessor(), Predecessor::MayLive);
        assert_eq!(wal.binding().standing_at_open(), RootStanding::Unbound);
        wal.shutdown().await.unwrap();
        drop(wal);
        // The root is still unbound, so its predecessor may live.
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

    /// What a root holds never decides how it stands: a root that took a
    /// vote while meta refused it is still unbound after a clean end, owes
    /// its wait from its binding, and keeps its data replicas held until
    /// then. Its binding is recorded, `fsync`ed, with the run and the owed
    /// wait as they were, and the finished wait keeps it, so the next start
    /// stands bound and waits from its open. Once no wait is owed, a bound
    /// root after a clean end owes none. Catches a start that reads the
    /// standing from the journals (the vote would make the refused root
    /// stand bound, so its registration would supersede it and a released
    /// slot would refuse it), and a binding or wait record that drops the
    /// other's field.
    #[tokio::test]
    async fn a_root_stands_as_recorded_and_its_binding_outlives_restarts() {
        let dir = tempfile::tempdir().unwrap();
        let topology = StaticShardMap::new(1, 2).unwrap();
        let wal = leased_start(dir.path(), &topology);
        save_a_vote(&wal, &topology).await;
        wal.shutdown().await.unwrap();
        drop(wal);

        let wal = leased_start(dir.path(), &topology);
        assert_eq!(wal.binding().standing_at_open(), RootStanding::Unbound);
        assert_eq!(wal.predecessor(), Predecessor::MayLive);
        let wait = wal.predecessor_wait().expect("an unbound root owes a wait");
        assert_eq!(wait.from(), WaitFrom::Binding);
        assert!(wal.fence().awaits_binding() && wal.fence().holds_data());
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 2,
            predecessor_wait: true,
            standing: RootStanding::Unbound,
        });

        wal.binding().record_bound().await.unwrap();
        let bound = IdentityFile {
            run: 2,
            predecessor_wait: true,
            standing: RootStanding::Bound,
        };
        assert_eq!(identity(dir.path()), bound);
        // A second record changes nothing.
        wal.binding().record_bound().await.unwrap();
        assert_eq!(identity(dir.path()), bound);
        wait.finish().await.unwrap();
        assert_eq!(identity(dir.path()), IdentityFile {
            predecessor_wait: false,
            ..bound
        });
        wal.shutdown().await.unwrap();
        drop(wal);

        // Bound, no wait owed and a clean end: the predecessor is gone.
        let wal = leased_start(dir.path(), &topology);
        assert_eq!(wal.binding().standing_at_open(), RootStanding::Bound);
        assert_eq!(wal.predecessor(), Predecessor::Gone);
        assert!(wal.predecessor_wait().is_none());
        assert!(!wal.fence().holds_data());
        drop(wal);

        // Bound after a crash it cannot tell from a moved volume, or with a
        // wait still owed: the wait is measured from the open.
        record(dir.path(), &IdentityFile {
            run: 4,
            predecessor_wait: true,
            standing: RootStanding::Bound,
        });
        let wal = leased_start(dir.path(), &topology);
        assert_eq!(wal.binding().standing_at_open(), RootStanding::Bound);
        assert!(matches!(
            wal.predecessor_wait().map(|wait| wait.from()),
            Some(WaitFrom::Open(_))
        ));
        assert!(!wal.fence().awaits_binding() && wal.fence().owes_wait());
        assert_eq!(identity(dir.path()), IdentityFile {
            run: 5,
            predecessor_wait: true,
            standing: RootStanding::Bound,
        });
        wal.shutdown().await.unwrap();
    }

    /// A root with prior state and no identity file, with a counter at its
    /// end, or with the file of a build before the standing was recorded,
    /// refuses to start and changes nothing.
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
            standing: RootStanding::Bound,
        };
        record(dir.path(), &exhausted);
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::RunCounterExhausted { run: u32::MAX, .. })
        ));
        assert_eq!(identity(dir.path()), exhausted);
        assert_eq!(
            std::fs::read(dir.path().join(RUN_STATE_FILE)).unwrap(),
            run_state
        );

        // The two fields of a build before the standing was recorded.
        let unstanding = state_file::encode(StateFileKind::Identity, &(7_u32, true));
        std::fs::write(&path, &unstanding).unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::ReadIdentity(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), unstanding);

        // A damaged file is reported as itself and kept.
        std::fs::write(&path, b"URSWIDNT").unwrap();
        assert!(matches!(
            RaftWal::start(dir.path(), WalFsync::Never, &topology),
            Err(RaftWalError::ReadIdentity(_))
        ));
        assert_eq!(std::fs::read(&path).unwrap(), b"URSWIDNT");
    }

    /// A WAL root records its identity file of each standing as the
    /// persisted-envelopes fixture's sample holds it ([`sample_files`]).
    #[test]
    fn a_recorded_identity_file_is_the_fixture_sample() {
        let encoded = sample_files();
        for ((name, file), (encoded_name, bytes)) in sample_identities().iter().zip(&encoded) {
            assert_eq!(name, encoded_name);
            let dir = tempfile::tempdir().unwrap();
            record(dir.path(), file);
            assert_eq!(
                &std::fs::read(dir.path().join(IDENTITY_FILE)).unwrap(),
                bytes,
                "{name}"
            );
        }
    }
}
