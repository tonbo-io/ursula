//! External snapshot pins and current-reference publication outside RaftCore.
//!
//! Prepared operations retain a pin until their synchronous pointer transition
//! completes. Serialize reference I/O without holding a lock across a Raft call,
//! so installation cannot deadlock a concurrent snapshot builder.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::time::Duration;

use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotStoreError;

use crate::rt::sync::Mutex as AsyncMutex;
use crate::rt::time::Instant;

#[derive(Debug, Default)]
pub(crate) struct SnapshotReferences {
    serial: AsyncMutex<PublicationRetry>,
    state: Mutex<ReferenceState>,
}

#[derive(Debug, Default)]
struct PublicationRetry {
    failures: u32,
    not_before: Option<Instant>,
}

#[derive(Debug, Default)]
struct ReferenceState {
    current_known: bool,
    current: Option<SnapshotLocation>,
    active: BTreeMap<String, (SnapshotLocation, usize)>,
}

impl ReferenceState {
    fn retained(&self) -> Vec<SnapshotLocation> {
        self.current
            .iter()
            .cloned()
            .chain(self.active.values().map(|(location, _)| location.clone()))
            .collect()
    }
}

#[derive(Debug)]
pub(crate) struct SnapshotReferenceLease {
    references: Arc<SnapshotReferences>,
    key: String,
}

impl Drop for SnapshotReferenceLease {
    fn drop(&mut self) {
        let mut state = self
            .references
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some((_, count)) = state.active.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active.remove(&self.key);
            }
        }
    }
}

impl SnapshotReferences {
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        store: &SharedSnapshotStore,
        group: u32,
        location: &SnapshotLocation,
    ) -> Result<Option<SnapshotReferenceLease>, SnapshotStoreError> {
        let SnapshotLocation::S3 { key, .. } = location else {
            return Ok(None);
        };
        let _serial = self.serial.lock().await;
        let mut retained = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained();
        retained.push(location.clone());
        // Reconcile abandoned pins before creating more. On durable startup,
        // the incoming location is the restored pointer and must be retained.
        store.reconcile_reference_pins(group, &retained).await?;
        store.pin_reference(group, location).await?;
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (_, count) = state
            .active
            .entry(key.clone())
            .or_insert((location.clone(), 0));
        *count = count.saturating_add(1);
        Ok(Some(SnapshotReferenceLease {
            references: self.clone(),
            key: key.clone(),
        }))
    }

    /// Call after durable metadata and the current pointer change. A prepared
    /// lease protects the new external pointer during this transition.
    pub(crate) fn commit_current(&self, location: &SnapshotLocation) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.current_known = true;
        state.current = match location {
            SnapshotLocation::S3 { .. } => Some(location.clone()),
            SnapshotLocation::Inline { .. } | SnapshotLocation::Local { .. } => None,
        };
    }

    pub(crate) async fn publish_current(
        &self,
        store: &SharedSnapshotStore,
        group: u32,
    ) -> Result<(), SnapshotStoreError> {
        let mut retry = self.serial.lock().await;
        if let Some(not_before) = retry.not_before {
            crate::rt::time::sleep_until(not_before).await;
        }
        let (known, current) = {
            let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            (state.current_known, state.current.clone())
        };
        // Keep the current pin even after a successful PUT. A prepared pointer
        // may become current during that PUT; its pin protects a lagging primary.
        let publication = if known {
            store
                .publish_reference(
                    group,
                    &current.unwrap_or(SnapshotLocation::Inline { bytes: Vec::new() }),
                )
                .await
        } else {
            Ok(())
        };
        let retained = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retained();
        let cleanup = store.reconcile_reference_pins(group, &retained).await;
        let result = publication.and(cleanup);
        if result.is_err() {
            retry.failures = retry.failures.saturating_add(1);
            let delay = Duration::from_millis(
                100_u64
                    .saturating_mul(2_u64.saturating_pow(retry.failures.saturating_sub(1).min(6))),
            )
            .min(Duration::from_secs(5));
            retry.not_before = Instant::now().checked_add(delay);
        } else {
            *retry = PublicationRetry::default();
        }
        result
    }
}
