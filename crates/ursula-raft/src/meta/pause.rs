//! A node's one pause registry, for tests and simulations that stop the
//! node at a step and act while it waits there, as a data group's
//! `pause_plans_after_the_fence` does for plans: the meta runtime's steps,
//! the step of every replica's membership reconcile shell and the steps of
//! the hosting driver's teardowns. The node's registry holds it
//! ([`crate::RaftGroupHandleRegistry::pauses`]), so its meta runtime, its
//! hosting driver and every replica it opens share it, and a new process
//! starts with none closed.
//!
//! [`MetaPauses::pause`] (tests, simulations and the `fault-injection`
//! feature only) closes a point: every later pass waits there until the
//! returned semaphore has a permit, and [`MetaPauses::waiting`] tells
//! whether a pass waits there now. Elsewhere a pass never waits.

#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::Arc;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::Mutex;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::PoisonError;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::atomic::AtomicUsize;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::atomic::Ordering;

use ursula_control::TeardownStep;
use ursula_proto::placement::StepKind;
use ursula_shard::RaftGroupId;
use ursula_shard::RaftTarget;

#[cfg(any(test, madsim, feature = "fault-injection"))]
use crate::rt::sync::Semaphore;

/// A step of the node a test can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetaPausePoint {
    /// Before the node submits its registration.
    Registration,
    /// Once the meta group answered the node's registration, before the
    /// node records its root bound and publishes the answer.
    RegistrationAnswered,
    /// Before the meta leader submits `Bootstrap`.
    Bootstrap,
    /// In a recovery plan's restart, once the old replica stopped and
    /// before the new one opens.
    RestartStopped,
    /// In a recovery plan's restart, once the new replica opened.
    RestartReopened,
    /// In the reconcile shell of `target`'s replica, once it planned a step
    /// of kind `step` and published it, before the step runs: a held step
    /// runs as it was planned, whatever changed while it waited.
    ShellStep { target: RaftTarget, step: StepKind },
    /// In the hosting driver's teardown of this node's replica of `group`,
    /// once the teardown reached `step`, before the step's action runs: a
    /// held step runs once released, whatever changed while it waited.
    Teardown {
        group: RaftGroupId,
        step: TeardownStep,
    },
}

/// A closed point: the semaphore its passes wait on, and how many wait.
#[cfg(any(test, madsim, feature = "fault-injection"))]
#[derive(Debug)]
struct Closed {
    point: MetaPausePoint,
    pause: Arc<Semaphore>,
    waiting: Arc<AtomicUsize>,
}

/// Every closed point.
#[cfg(any(test, madsim, feature = "fault-injection"))]
type Points = Vec<Closed>;

/// One pass counted as waiting at a closed point until it goes on or is
/// dropped, an aborted pass included.
#[cfg(any(test, madsim, feature = "fault-injection"))]
struct Waiting(Arc<AtomicUsize>);

#[cfg(any(test, madsim, feature = "fault-injection"))]
impl Waiting {
    fn enter(count: Arc<AtomicUsize>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self(count)
    }
}

#[cfg(any(test, madsim, feature = "fault-injection"))]
impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The node's pause points (see the module docs).
#[derive(Debug, Clone, Default)]
pub struct MetaPauses {
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    points: Arc<Mutex<Points>>,
}

impl MetaPauses {
    /// Closes `point`: every later pass waits there until the returned
    /// semaphore has a permit.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub fn pause(&self, point: MetaPausePoint) -> Arc<Semaphore> {
        let pause = Arc::new(Semaphore::new(0));
        let mut points = self.points.lock().unwrap_or_else(PoisonError::into_inner);
        points.retain(|closed| closed.point != point);
        points.push(Closed {
            point,
            pause: Arc::clone(&pause),
            waiting: Arc::new(AtomicUsize::new(0)),
        });
        pause
    }

    /// Opens `point` for every pass, those waiting there included.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub fn release(&self, point: MetaPausePoint) {
        let points = self.points.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(closed) = points.iter().find(|closed| closed.point == point) {
            closed.pause.add_permits(1);
        }
    }

    /// Whether a pass waits at the closed `point` now.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub fn waiting(&self, point: MetaPausePoint) -> bool {
        self.points
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|closed| closed.point == point && closed.waiting.load(Ordering::Acquire) > 0)
    }

    /// Waits at `point` while it is closed.
    pub(crate) async fn at(&self, point: MetaPausePoint) {
        #[cfg(any(test, madsim, feature = "fault-injection"))]
        {
            let closed = self
                .points
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .iter()
                .find(|closed| closed.point == point)
                .map(|closed| (Arc::clone(&closed.pause), Arc::clone(&closed.waiting)));
            if let Some((pause, waiting)) = closed {
                let _waiting = Waiting::enter(waiting);
                drop(pause.acquire().await);
            }
        }
        #[cfg(not(any(test, madsim, feature = "fault-injection")))]
        let _ = point;
    }
}
