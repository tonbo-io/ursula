//! Process-local ordering for admitted maintenance executors.
//!
//! Mutation handlers retain a read guard through completion. Activation and
//! retirement take the write guard and reconcile the local command queue.
//! This orders request submission, not asynchronous replication completion.
//! The external cell reservation supplies generations; this module neither
//! acquires it nor permits expiry takeover.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::RwLock;
use ursula_proto::admin::MaintenanceFence;
use ursula_proto::admin::MaintenanceFenceState;

#[derive(Clone, Default)]
pub(crate) struct AdminMutationFence {
    current: Arc<RwLock<MaintenanceFenceState>>,
    uncertain: Arc<AtomicBool>,
}

#[derive(Debug)]
pub(crate) enum FenceRejection {
    Missing,
    Changed,
    Retired,
    Uncertain,
    Barrier(String),
}

impl FenceRejection {
    pub(crate) fn response(self) -> Response {
        match self {
            Self::Missing => (
                StatusCode::PRECONDITION_REQUIRED,
                "admin mutation requires its admitted maintenance executor",
            ),
            Self::Changed => (
                StatusCode::PRECONDITION_FAILED,
                "maintenance executor changed or is not activated; stop the current operation",
            ),
            Self::Barrier(error) => {
                tracing::error!(%error, "maintenance command barrier failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "maintenance command barrier failed; authority remains closed",
                )
                    .into_response();
            }
            Self::Uncertain => (
                StatusCode::SERVICE_UNAVAILABLE,
                "maintenance work is unresolved; authority remains closed",
            ),
            Self::Retired => (
                StatusCode::PRECONDITION_FAILED,
                "maintenance executor was retired; its authority cannot be reopened",
            ),
        }
        .into_response()
    }
}

impl AdminMutationFence {
    pub(crate) fn from_startup(current: MaintenanceFenceState) -> Self {
        Self {
            current: Arc::new(RwLock::new(current)),
            uncertain: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) async fn snapshot(&self) -> MaintenanceFenceState {
        self.current.read().await.clone()
    }

    pub(crate) fn is_uncertain(&self) -> bool {
        self.uncertain.load(Ordering::Acquire)
    }

    pub(crate) async fn activate<F>(
        &self,
        fence: MaintenanceFence,
        barrier: F,
    ) -> Result<MaintenanceFenceState, FenceRejection>
    where
        F: Future<Output = Result<(), String>>,
    {
        let mut current = self.current.write().await;
        if self.is_uncertain() {
            return Err(FenceRejection::Uncertain);
        }
        if let MaintenanceFenceState::Active { fence: active } = &*current
            && active == &fence
        {
            return Ok(current.clone());
        }
        let retry = matches!(&*current, MaintenanceFenceState::Activating { fence: pending } if pending == &fence);
        if !retry
            && let Some(previous) = current.fence()
            && fence.generation() <= previous.generation()
        {
            return Err(if previous == &fence {
                FenceRejection::Retired
            } else {
                FenceRejection::Changed
            });
        }
        // Record the new high-water mark before awaiting reconciliation. A
        // cancelled/failed activation cannot restore the previous authority.
        *current = MaintenanceFenceState::Activating {
            fence: fence.clone(),
        };
        barrier.await.map_err(FenceRejection::Barrier)?;
        *current = MaintenanceFenceState::Active { fence };
        Ok(current.clone())
    }

    pub(crate) async fn retire<F>(
        &self,
        fence: MaintenanceFence,
        barrier: F,
    ) -> Result<MaintenanceFenceState, FenceRejection>
    where
        F: Future<Output = Result<(), String>>,
    {
        let mut current = self.current.write().await;
        if self.is_uncertain() {
            return Err(FenceRejection::Uncertain);
        }
        if current.fence() != Some(&fence) {
            return Err(FenceRejection::Changed);
        }
        *current = MaintenanceFenceState::Retiring {
            fence: fence.clone(),
        };
        barrier.await.map_err(FenceRejection::Barrier)?;
        *current = MaintenanceFenceState::Retired { fence };
        Ok(current.clone())
    }

    pub(crate) async fn admit_mutation(
        &self,
        header: Option<&str>,
    ) -> Result<AdmittedMutation, FenceRejection> {
        let guard = self.current.clone().read_owned().await;
        if self.is_uncertain() {
            return Err(FenceRejection::Uncertain);
        }
        match &*guard {
            // Explicitly uncertified until a reservation consumer activates
            // this process. Installing one makes the precondition mandatory
            // and retirement never returns to this permissive state.
            MaintenanceFenceState::Unclaimed if header.is_none() => {}
            MaintenanceFenceState::Unclaimed => return Err(FenceRejection::Changed),
            MaintenanceFenceState::AwaitingReservation
            | MaintenanceFenceState::Activating { .. }
            | MaintenanceFenceState::Retiring { .. } => {
                return Err(FenceRejection::Uncertain);
            }
            MaintenanceFenceState::Retired { .. } => return Err(FenceRejection::Retired),
            MaintenanceFenceState::Active { fence } => {
                let header = header.ok_or(FenceRejection::Missing)?;
                let observed = header
                    .parse::<MaintenanceFence>()
                    .map_err(|_malformed| FenceRejection::Changed)?;
                if &observed != fence {
                    return Err(FenceRejection::Changed);
                }
            }
        }
        Ok(AdmittedMutation {
            _guard: guard,
            uncertain: self.uncertain.clone(),
            completed: false,
        })
    }
}

/// Abnormal handler termination must not let takeover certify unknown work.
/// HTTP cancellation is handled by the detached mutation task; panic/task
/// abortion additionally latches this process closed until it is replaced.
pub(crate) struct AdmittedMutation {
    _guard: OwnedRwLockReadGuard<MaintenanceFenceState>,
    uncertain: Arc<AtomicBool>,
    completed: bool,
}

impl AdmittedMutation {
    pub(crate) fn complete(mut self) {
        self.completed = true;
    }
}

impl Drop for AdmittedMutation {
    fn drop(&mut self) {
        if !self.completed {
            self.uncertain.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::AdminMutationFence;
    use super::FenceRejection;
    use super::MaintenanceFence;
    use super::MaintenanceFenceState;

    fn token(generation: u64) -> MaintenanceFence {
        MaintenanceFence::new(
            format!("{:032x}", 1),
            format!("{generation:032x}"),
            generation,
        )
        .unwrap()
    }

    /// Asserts that `result` is a rejection of the given variant. The success
    /// type needs no `Debug`, so the value is not printed on a wrong success.
    macro_rules! assert_rejected {
        ($result:expr, $variant:pat $(,)?) => {{
            let result = $result;
            match result {
                Err(rejection) => {
                    assert!(matches!(rejection, $variant), "{rejection:?}");
                }
                Ok(_) => panic!("expected a rejection, got success"),
            }
        }};
    }

    #[tokio::test]
    async fn startup_retains_closed_authority_and_generation_before_first_request() {
        let pending =
            AdminMutationFence::from_startup(MaintenanceFenceState::Activating { fence: token(7) });
        assert_rejected!(
            pending.admit_mutation(None).await,
            FenceRejection::Uncertain
        );
        assert_rejected!(
            pending.admit_mutation(Some(&token(7).header_value())).await,
            FenceRejection::Uncertain
        );
        assert_rejected!(
            pending.activate(token(6), async { Ok(()) }).await,
            FenceRejection::Changed
        );
        let changed_executor =
            MaintenanceFence::new(format!("{:032x}", 1), format!("{:032x}", 8), 7).unwrap();
        assert_rejected!(
            pending.activate(changed_executor, async { Ok(()) }).await,
            FenceRejection::Changed
        );
        pending
            .activate(token(7), async { Ok(()) })
            .await
            .expect("the pending executor must be able to retry its own activation");
        pending
            .admit_mutation(Some(&token(7).header_value()))
            .await
            .unwrap()
            .complete();

        let idle =
            AdminMutationFence::from_startup(MaintenanceFenceState::Retired { fence: token(7) });
        assert_rejected!(idle.admit_mutation(None).await, FenceRejection::Retired);
        assert_rejected!(
            idle.activate(token(6), async { Ok(()) }).await,
            FenceRejection::Changed
        );
        assert_rejected!(
            idle.activate(token(7), async { Ok(()) }).await,
            FenceRejection::Retired
        );
        idle.activate(token(8), async { Ok(()) })
            .await
            .expect("a newer generation must supersede a retired executor");

        let initial = AdminMutationFence::from_startup(MaintenanceFenceState::AwaitingReservation);
        assert_rejected!(
            initial.admit_mutation(None).await,
            FenceRejection::Uncertain
        );
        initial
            .activate(token(1), async { Ok(()) })
            .await
            .expect("the first reservation must activate an awaiting process");
    }

    #[tokio::test]
    async fn takeover_orders_inflight_work_and_rejects_old_executor() {
        let gate = AdminMutationFence::default();
        let old = token(1);
        let new = token(2);
        gate.activate(old.clone(), async { Ok(()) })
            .await
            .expect("the first executor must activate");
        let guard = gate
            .admit_mutation(Some(&old.header_value()))
            .await
            .unwrap_or_else(|_| panic!("admitted"));
        let target = gate.clone();
        let next = new.clone();
        let (submitted, observed) = tokio::sync::oneshot::channel();
        let mut takeover = tokio::spawn(async move {
            target
                .activate(next, async {
                    submitted.send(()).unwrap();
                    Ok(())
                })
                .await
        });
        tokio::time::timeout(Duration::from_millis(30), &mut takeover)
            .await
            .expect_err("takeover must wait for the in-flight mutation");
        guard.complete();
        observed.await.unwrap();
        takeover
            .await
            .unwrap()
            .expect("takeover must activate once in-flight work completes");
        assert!(matches!(
            gate.admit_mutation(Some(&old.header_value())).await,
            Err(FenceRejection::Changed)
        ));
        gate.admit_mutation(Some(&new.header_value()))
            .await
            .unwrap_or_else(|_| panic!("new admitted"))
            .complete();
        assert_rejected!(
            gate.retire(old, async { Ok(()) }).await,
            FenceRejection::Changed
        );
    }

    #[tokio::test]
    async fn retirement_cannot_be_reactivated_or_downgraded() {
        let gate = AdminMutationFence::default();
        let old = token(1);
        gate.activate(old.clone(), async { Ok(()) })
            .await
            .expect("the executor must activate");
        gate.retire(old.clone(), async { Ok(()) })
            .await
            .expect("the active executor must retire");
        gate.retire(old.clone(), async { Ok(()) })
            .await
            .expect("retiring a retired executor again must be idempotent");
        assert!(matches!(
            gate.activate(old.clone(), async { Ok(()) }).await,
            Err(FenceRejection::Retired)
        ));
        assert_rejected!(gate.admit_mutation(None).await, FenceRejection::Retired);
        gate.activate(token(2), async { Ok(()) })
            .await
            .expect("a newer generation must supersede a retired executor");
        assert_rejected!(
            gate.retire(old, async { Ok(()) }).await,
            FenceRejection::Changed
        );
        assert_eq!(gate.snapshot().await, MaintenanceFenceState::Active {
            fence: token(2)
        });
    }

    #[tokio::test]
    async fn failed_activation_closes_prior_authority_and_can_retry_only_pending_identity() {
        let gate = AdminMutationFence::default();
        gate.activate(token(1), async { Ok(()) })
            .await
            .expect("the first executor must activate");
        assert_rejected!(
            gate.activate(token(2), async { Err("queue failed".to_owned()) })
                .await,
            FenceRejection::Barrier(_)
        );
        assert_eq!(gate.snapshot().await, MaintenanceFenceState::Activating {
            fence: token(2)
        });
        assert_rejected!(
            gate.admit_mutation(Some(&token(1).header_value())).await,
            FenceRejection::Uncertain
        );
        assert_rejected!(
            gate.activate(token(1), async { Ok(()) }).await,
            FenceRejection::Changed
        );
        gate.activate(token(2), async { Ok(()) })
            .await
            .expect("the pending identity must be able to retry");
    }

    #[tokio::test]
    async fn cancelled_retirement_cannot_reopen_authority() {
        let gate = AdminMutationFence::default();
        gate.activate(token(1), async { Ok(()) })
            .await
            .expect("the first executor must activate");
        let target = gate.clone();
        let (entered, observed) = tokio::sync::oneshot::channel();
        let retirement = tokio::spawn(async move {
            target
                .retire(token(1), async {
                    entered.send(()).unwrap();
                    std::future::pending::<Result<(), String>>().await
                })
                .await
        });
        observed.await.unwrap();
        retirement.abort();
        assert!(retirement.await.unwrap_err().is_cancelled());
        assert_eq!(gate.snapshot().await, MaintenanceFenceState::Retiring {
            fence: token(1)
        });
        assert_rejected!(
            gate.activate(token(1), async { Ok(()) }).await,
            FenceRejection::Retired
        );
        assert_rejected!(
            gate.admit_mutation(Some(&token(1).header_value())).await,
            FenceRejection::Uncertain
        );
        gate.retire(token(1), async { Ok(()) })
            .await
            .expect("a cancelled retirement must be retryable");
    }

    #[tokio::test]
    async fn cancelled_activation_never_restores_previous_executor() {
        let gate = AdminMutationFence::default();
        gate.activate(token(1), async { Ok(()) })
            .await
            .expect("the first executor must activate");
        let target = gate.clone();
        let (entered, observed) = tokio::sync::oneshot::channel();
        let activation = tokio::spawn(async move {
            target
                .activate(token(2), async {
                    entered.send(()).unwrap();
                    std::future::pending::<Result<(), String>>().await
                })
                .await
        });
        observed.await.unwrap();
        activation.abort();
        assert!(activation.await.unwrap_err().is_cancelled());
        assert_eq!(gate.snapshot().await, MaintenanceFenceState::Activating {
            fence: token(2)
        });
        assert_rejected!(
            gate.activate(token(1), async { Ok(()) }).await,
            FenceRejection::Changed
        );
        assert_rejected!(
            gate.admit_mutation(Some(&token(1).header_value())).await,
            FenceRejection::Uncertain
        );
        gate.activate(token(2), async { Ok(()) })
            .await
            .expect("a cancelled activation must be retryable by its own identity");
    }

    #[tokio::test]
    async fn abandoned_handler_latches_process_closed() {
        let gate = AdminMutationFence::default();
        let guard = gate
            .admit_mutation(None)
            .await
            .unwrap_or_else(|_| panic!("legacy admitted"));
        drop(guard);
        assert!(gate.is_uncertain());
        assert_rejected!(gate.admit_mutation(None).await, FenceRejection::Uncertain);
        assert_rejected!(
            gate.activate(token(1), async { Ok(()) }).await,
            FenceRejection::Uncertain
        );
    }
}
