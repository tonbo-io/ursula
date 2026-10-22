//! Pause points of the meta runtime, for tests and simulations that stop
//! it at a step and act while it waits there, as a data group's
//! `pause_plans_after_the_fence` does for plans.
//!
//! [`MetaPauses::pause`] (tests, simulations and the `fault-injection`
//! feature only) closes a point: every later pass waits there until the
//! returned semaphore has a permit. Elsewhere a pass never waits.

#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::collections::BTreeMap;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::Arc;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::Mutex;
#[cfg(any(test, madsim, feature = "fault-injection"))]
use std::sync::PoisonError;

#[cfg(any(test, madsim, feature = "fault-injection"))]
use crate::rt::sync::Semaphore;

/// A step of the meta runtime a test can hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
}

/// The meta runtime's pause points (see the module docs).
#[derive(Debug, Clone, Default)]
pub struct MetaPauses {
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    points: Arc<Mutex<BTreeMap<MetaPausePoint, Arc<Semaphore>>>>,
}

impl MetaPauses {
    /// Closes `point`: every later pass waits there until the returned
    /// semaphore has a permit.
    #[cfg(any(test, madsim, feature = "fault-injection"))]
    pub fn pause(&self, point: MetaPausePoint) -> Arc<Semaphore> {
        let pause = Arc::new(Semaphore::new(0));
        self.points
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(point, Arc::clone(&pause));
        pause
    }

    /// Waits at `point` while it is closed.
    pub(crate) async fn at(&self, point: MetaPausePoint) {
        #[cfg(any(test, madsim, feature = "fault-injection"))]
        {
            let pause = self
                .points
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get(&point)
                .cloned();
            if let Some(pause) = pause {
                drop(pause.acquire().await);
            }
        }
        #[cfg(not(any(test, madsim, feature = "fault-injection")))]
        let _ = point;
    }
}
