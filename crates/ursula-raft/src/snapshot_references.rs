//! External snapshot pins and current-reference publication outside RaftCore.
//!
//! Prepared operations retain a pin until their synchronous pointer transition
//! completes. Serialize reference I/O without holding a lock across a Raft call,
//! so installation cannot deadlock a concurrent snapshot builder.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;

use ursula_runtime::GroupActivity;
use ursula_runtime::GroupActivityGuard;
use ursula_runtime::SharedSnapshotStore;
use ursula_runtime::SnapshotLocation;
use ursula_runtime::SnapshotStoreError;

use crate::rt::sync::Mutex as AsyncMutex;

#[derive(Debug, Default)]
pub(crate) struct SnapshotReferences {
    pub(crate) activity: Arc<GroupActivity>,
    serial: AsyncMutex<()>,
    state: Mutex<ReferenceState>,
}

#[derive(Debug, Default)]
struct ReferenceState {
    retirement_complete: bool,
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
    _activity: GroupActivityGuard,
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
        let activity = self.activity.enter().map_err(SnapshotStoreError::Backend)?;
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
            _activity: activity,
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
        let _activity = self.activity.enter().map_err(SnapshotStoreError::Backend)?;
        let _serial = self.serial.lock().await;
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
        publication.and(cleanup)
    }

    pub(crate) fn retirement_complete(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retirement_complete
    }

    /// Call after closing entry and draining cached/active snapshot work.
    /// Failure leaves the lifecycle closed and permits an explicit retry.
    pub(crate) async fn retire(
        &self,
        store: &SharedSnapshotStore,
        group: u32,
    ) -> Result<(), SnapshotStoreError> {
        self.activity.close();
        self.activity.drain().await;
        let _serial = self.serial.lock().await;
        {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if !state.active.is_empty() {
                return Err(SnapshotStoreError::Backend(
                    "snapshot pins did not drain".to_owned(),
                ));
            }
            state.current_known = true;
            state.current = None;
        }
        store
            .publish_reference(group, &SnapshotLocation::Inline { bytes: Vec::new() })
            .await?;
        store.reconcile_reference_pins(group, &[]).await?;
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retirement_complete = true;
        Ok(())
    }
}
