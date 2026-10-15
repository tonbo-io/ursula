//! The driver's disk jobs: the local work of opens and takeovers that may take long (materialize,
//! a build's reads applied, a publish, a catch-up in place). The core hands each out as an
//! [`super::Io::Disk`] and gets its [`DiskDone`] back, so the runtime runs them off the driver
//! thread and one database's catch-up never stalls the others. The state a job works on (a
//! generation, a build) moves into the job and comes back in its outcome, and every job holds
//! the stream's root (its lock) while it runs.

use std::sync::Arc;

use ursula_headers::ClusterId;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;

use crate::generation::CanonicalPath;
use crate::generation::Fate;
use crate::generation::Root;
use crate::ids::Activation;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::materialize;
use crate::materialize::ForeignFrame;
use crate::materialize::LocalError;
use crate::materialize::Materialized;
use crate::open::Build;
use crate::open::OpenError;
use crate::open::OpenSpec;
use crate::open::Prepared;
use crate::open::Published;
use crate::open::new_build;
use crate::open::prepare;
use crate::open::prepare_locked;

/// A build to start: a fresh generation in `gen-<id32>.tmp`, read up to `end`.
#[derive(Debug)]
pub(crate) struct NewBuild {
    pub(crate) root: Arc<Root>,
    pub(crate) name: CanonicalPath,
    pub(crate) lineage: Lineage,
    pub(crate) cluster_id: ClusterId,
    pub(crate) incarnation: Incarnation,
    /// The first position of its log: above every position the generation it replaces logged.
    pub(crate) first: Position,
    /// A rebuild's claim, logged in the new generation.
    pub(crate) claim: Option<(Epoch, Activation)>,
    pub(crate) end: Offset,
    /// What becomes of the generation the build replaces when it publishes.
    pub(crate) fate: Fate,
}

#[derive(Debug)]
pub(crate) enum DiskJob {
    /// An open's local steps ([`prepare`]), in `root` when a reset binding of the process still
    /// holds the stream's root.
    Prepare {
        spec: OpenSpec,
        root: Option<Arc<Root>>,
    },
    NewBuild(NewBuild),
    /// The latest snapshot's body into a build; done as a [`DiskDone::Chunk`].
    Install {
        build: Box<Build>,
        body: Vec<u8>,
    },
    /// A build started over in a fresh `.tmp`; done as a [`DiskDone::Built`].
    Restart(Box<Build>),
    /// One read's frames into a build.
    Chunk {
        build: Box<Build>,
        frames: Vec<ForeignFrame>,
    },
    /// A complete build, published.
    Publish(Box<Build>),
    /// A fresh build that found the stream missing, abandoned, then the open's local steps
    /// again under the lock it held; done as a [`DiskDone::Prepared`].
    Abandon {
        build: Box<Build>,
        spec: OpenSpec,
    },
    /// One read of a takeover's catch-up in place, nothing pending.
    CatchUp {
        m: Box<Materialized>,
        frames: Vec<ForeignFrame>,
        /// Holds the root's lock while the job runs.
        root: Arc<Root>,
    },
}

#[derive(Debug)]
pub(crate) enum DiskDone {
    Prepared(Result<Prepared, OpenError>),
    Built(Result<Box<Build>, OpenError>),
    Chunk {
        build: Box<Build>,
        result: Result<(), OpenError>,
    },
    Published(Result<Published, OpenError>),
    /// The last position the frames took and the log's bytes.
    CaughtUp {
        m: Box<Materialized>,
        outcome: Result<(Position, u64), LocalError>,
    },
    /// The job panicked: what it worked on is gone.
    Panicked,
}

/// The last position a generation's log took, and its frame bytes.
pub(crate) fn caught_up(m: &Materialized) -> (Position, u64) {
    (
        Position(m.log.next_position().0.saturating_sub(1)),
        m.log.log_bytes(),
    )
}

impl DiskJob {
    pub(crate) fn run(self) -> DiskDone {
        match self {
            Self::Prepare { spec, root } => DiskDone::Prepared(prepare(&spec, root)),
            Self::NewBuild(job) => DiskDone::Built(new_build(job).map(Box::new)),
            Self::Install { mut build, body } => {
                let result = build.install(&body);
                DiskDone::Chunk { build, result }
            }
            Self::Restart(build) => DiskDone::Built(build.restart().map(Box::new)),
            Self::Chunk { mut build, frames } => {
                let result = build.apply(&frames);
                DiskDone::Chunk { build, result }
            }
            Self::Publish(build) => DiskDone::Published(build.publish()),
            Self::Abandon { build, spec } => {
                DiskDone::Prepared(build.abandon().and_then(|root| prepare_locked(root, &spec)))
            }
            Self::CatchUp {
                mut m,
                frames,
                root,
            } => {
                let outcome = catch_up(&mut m, &frames);
                drop(root);
                DiskDone::CaughtUp { m, outcome }
            }
        }
    }
}

/// Logs the frames as COMMIT records with an ACK covering them, fsyncs, then applies them. The
/// replicator asks for a catch-up in place only with nothing pending, so the frames go straight
/// after the log (its reconcile may have acknowledged commits no ACK names yet, which the
/// pending check of [`materialize::catch_up`] cannot know).
fn catch_up(m: &mut Materialized, frames: &[ForeignFrame]) -> Result<(Position, u64), LocalError> {
    materialize::catch_up_in_place(m, frames)?;
    Ok(caught_up(m))
}
