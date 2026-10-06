//! Explicit, restartable receiver storage for simulation. The harness owns the
//! disk independently of node tasks; dropping a node does not erase authority.
//! Publication is atomic and can fail on either side of its durability point.

use std::io;
use std::sync::Arc;
use std::sync::Mutex;

use ursula_control::MetaLocalIdentity;
use ursula_control::ReceiverLedger;

use super::Checkpoint;
use super::Inner;
use super::ManagedReceiverStore;
use super::invalid;
use super::validate_receipt_node;

/// A one-shot failure at the simulated atomic checkpoint boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatedReceiverWriteFault {
    BeforeCommit,
    AfterCommit,
}

#[derive(Default)]
struct DiskState {
    checkpoint: Option<Checkpoint>,
    opened: bool,
    fault: Option<SimulatedReceiverWriteFault>,
}

/// A simulation-owned disk, never a process-global registry or host file.
#[derive(Clone, Default)]
pub struct SimulatedReceiverDisk {
    state: Arc<Mutex<DiskState>>,
}

impl SimulatedReceiverDisk {
    /// Inject failure into the next changed, valid checkpoint publication.
    /// Rejected CAS/authority mutations and idempotent retries do not consume it.
    pub fn fail_next_write(&self, fault: SimulatedReceiverWriteFault) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("simulated receiver disk poisoned"))?;
        if state.fault.is_some() {
            return Err(invalid("simulated receiver write fault is already armed"));
        }
        state.fault = Some(fault);
        Ok(())
    }

    pub(super) fn publish(&self, checkpoint: Checkpoint) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("simulated receiver disk poisoned"))?;
        if !state.opened {
            return Err(invalid("simulated receiver disk is not open"));
        }
        let fault = state.fault.take();
        if fault == Some(SimulatedReceiverWriteFault::BeforeCommit) {
            return Err(io::Error::other("simulated failure before receiver commit"));
        }
        state.checkpoint = Some(checkpoint);
        if fault == Some(SimulatedReceiverWriteFault::AfterCommit) {
            return Err(io::Error::other("simulated receiver commit reply lost"));
        }
        Ok(())
    }
}

impl ManagedReceiverStore {
    /// Reopen the same harness-owned disk after dropping the old node/store.
    /// The normal file constructor remains unavailable under `cfg(madsim)`.
    pub fn open_simulated(
        disk: SimulatedReceiverDisk,
        identity: MetaLocalIdentity,
    ) -> io::Result<Arc<Self>> {
        let identity = identity.normalize().map_err(invalid)?;
        let mut state = disk
            .state
            .lock()
            .map_err(|_| invalid("simulated receiver disk poisoned"))?;
        if state.opened {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "simulated receiver disk is already open",
            ));
        }
        let ledger = match &state.checkpoint {
            Some(checkpoint) => {
                if checkpoint.identity != identity {
                    return Err(invalid("receiver checkpoint identity differs"));
                }
                checkpoint
                    .ledger
                    .validate(identity.cluster.group_count)
                    .map_err(invalid)?;
                validate_receipt_node(&checkpoint.ledger, identity.node.node_id)?;
                checkpoint.ledger.clone()
            }
            None => {
                let ledger = ReceiverLedger::default();
                state.checkpoint = Some(Checkpoint {
                    identity: identity.clone(),
                    ledger: ledger.clone(),
                });
                ledger
            }
        };
        state.opened = true;
        drop(state);
        Ok(Arc::new(Self {
            identity,
            inner: Mutex::new(Inner {
                ledger,
                failed: false,
            }),
            disk,
        }))
    }
}

impl Drop for ManagedReceiverStore {
    fn drop(&mut self) {
        // Only the last store reference owns the lease, matching the native WAL
        // lock. Failed publication still requires dropping/reopening the store.
        self.disk
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .opened = false;
    }
}

#[cfg(test)]
mod tests;
