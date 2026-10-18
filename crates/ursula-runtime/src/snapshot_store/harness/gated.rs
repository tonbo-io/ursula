//! A snapshot store wrapper that pauses operations at named gates, so a
//! test can force one order of a build against a GC pass.
//!
//! Each owned-key operation ([`StoreOp`]) has two gates: one before it
//! reaches the wrapped store and one after the store answered, before the
//! caller sees the answer ([`GatePoint`]). A gate passes everything until a
//! test holds it. While held, the operations that arrive wait there, and
//! [`SnapshotStoreGates::release`] lets them through one by one in the
//! order they arrived. Every arrival and pass is logged, so a test asserts
//! the order the operations took.
//!
//! The gates of a GC pass are its seams: `After(ListOwn)` sits between the
//! LIST and the read of the holds, and `Before(DeleteOwn)` between that
//! read and each DELETE. For example, to run a whole build between a pass's
//! LIST and its DELETEs, hold `After(ListOwn)`, start the pass, wait for its
//! arrival, run the build's lease, `write_staged` and commit, then release.
//!
//! The gates wait on a [`Notify`], which wakes waiters in the order they
//! queued, and draw nothing at random, so they keep a madsim run
//! deterministic.

use std::collections::BTreeMap;
use std::fmt;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use bytes::Bytes;
use ursula_shard::RaftGroupId;

use crate::rt::sync::Notify;
use crate::snapshot_store::OwnListing;
use crate::snapshot_store::OwnedNamespace;
use crate::snapshot_store::OwnedObject;
use crate::snapshot_store::SharedSnapshotStore;
use crate::snapshot_store::SnapshotBytesIterator;
use crate::snapshot_store::SnapshotKey;
use crate::snapshot_store::SnapshotLocation;
use crate::snapshot_store::SnapshotPrune;
use crate::snapshot_store::SnapshotStore;
use crate::snapshot_store::SnapshotStoreFuture;
use crate::snapshot_store::StagedSnapshot;

/// An operation of a [`SnapshotStore`] that has gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreOp {
    /// [`SnapshotStore::stage`].
    Stage,
    /// [`SnapshotStore::write_staged`], the PUT.
    WriteStaged,
    /// [`SnapshotStore::list_own`], the LIST.
    ListOwn,
    /// [`SnapshotStore::delete_own`], the DELETE.
    DeleteOwn,
    /// [`SnapshotStore::download`], the GET.
    Get,
    /// [`SnapshotStore::verify_uploaded`], the HEAD.
    Head,
}

/// Where an operation waits: before the wrapped store acts, or after it
/// answered and before the caller sees the answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum GatePoint {
    Before,
    After,
}

/// One gate: an operation and a point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gate {
    pub op: StoreOp,
    pub point: GatePoint,
}

impl Gate {
    pub const fn before(op: StoreOp) -> Self {
        Self {
            op,
            point: GatePoint::Before,
        }
    }

    pub const fn after(op: StoreOp) -> Self {
        Self {
            op,
            point: GatePoint::After,
        }
    }
}

impl fmt::Display for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}({:?})", self.point, self.op)
    }
}

/// Whether an operation reached a gate or went through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatePass {
    Arrived,
    Passed,
}

/// One entry of the gate log. `ticket` numbers the arrivals at the gate
/// from 0, and `subject` names what the operation acts on: the object key,
/// or the group and index of a stage or a LIST.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateEvent {
    pub gate: Gate,
    pub ticket: usize,
    pub subject: String,
    pub pass: GatePass,
}

#[derive(Debug, Default)]
struct GateCounts {
    arrived: usize,
    passed: usize,
    /// While held, the arrivals with a lower ticket may pass.
    allowed: Option<usize>,
}

#[derive(Debug, Default)]
struct GatesState {
    gates: BTreeMap<Gate, GateCounts>,
    log: Vec<GateEvent>,
}

#[derive(Debug, Default)]
struct GatesShared {
    state: Mutex<GatesState>,
    changed: Notify,
}

/// The gates of a [`GatedSnapshotStore`]. Clones share them, so one set
/// can gate the stores of several nodes.
#[derive(Debug, Clone, Default)]
pub struct SnapshotStoreGates {
    shared: Arc<GatesShared>,
}

impl SnapshotStoreGates {
    pub fn new() -> Self {
        Self::default()
    }

    /// Holds `gate`: every operation that arrives from now on waits there
    /// until released. The ones that already arrived go through. Holding a
    /// held gate changes nothing.
    pub fn hold(&self, gate: Gate) {
        self.update(|state| {
            let counts = state.gates.entry(gate).or_default();
            if counts.allowed.is_none() {
                counts.allowed = Some(counts.arrived);
            }
        });
    }

    /// Lets the next `count` operations held at `gate` through, in the
    /// order they arrived, those yet to arrive included.
    pub fn release(&self, gate: Gate, count: usize) {
        self.update(|state| {
            let counts = state.gates.entry(gate).or_default();
            if let Some(allowed) = counts.allowed.as_mut() {
                *allowed = allowed.saturating_add(count);
            }
        });
    }

    /// Stops holding `gate` and lets every operation waiting there through.
    pub fn open(&self, gate: Gate) {
        self.update(|state| {
            state.gates.entry(gate).or_default().allowed = None;
        });
    }

    /// How many operations arrived at `gate`.
    pub fn arrived(&self, gate: Gate) -> usize {
        self.state()
            .gates
            .get(&gate)
            .map_or(0, |counts| counts.arrived)
    }

    /// How many operations went through `gate`.
    pub fn passed(&self, gate: Gate) -> usize {
        self.state()
            .gates
            .get(&gate)
            .map_or(0, |counts| counts.passed)
    }

    /// Waits until `count` operations arrived at `gate`. A test bounds the
    /// wait with a timeout of its runtime.
    pub async fn wait_arrived(&self, gate: Gate, count: usize) {
        self.wait_until(|state| {
            state
                .gates
                .get(&gate)
                .is_some_and(|counts| counts.arrived >= count)
        })
        .await;
    }

    /// Waits until `count` operations went through `gate`.
    pub async fn wait_passed(&self, gate: Gate, count: usize) {
        self.wait_until(|state| {
            state
                .gates
                .get(&gate)
                .is_some_and(|counts| counts.passed >= count)
        })
        .await;
    }

    /// Every arrival and pass, in order.
    pub fn log(&self) -> Vec<GateEvent> {
        self.state().log.clone()
    }

    /// The subjects that went through `gate`, in order.
    pub fn passed_subjects(&self, gate: Gate) -> Vec<String> {
        self.state()
            .log
            .iter()
            .filter(|event| event.gate == gate && event.pass == GatePass::Passed)
            .map(|event| event.subject.clone())
            .collect()
    }

    fn state(&self) -> MutexGuard<'_, GatesState> {
        self.shared
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn update(&self, change: impl FnOnce(&mut GatesState)) {
        change(&mut self.state());
        self.shared.changed.notify_waiters();
    }

    async fn wait_until(&self, mut ready: impl FnMut(&GatesState) -> bool) {
        loop {
            let mut changed = pin!(self.shared.changed.notified());
            changed.as_mut().enable();
            let is_ready = ready(&self.state());
            if is_ready {
                return;
            }
            changed.await;
        }
    }

    /// Takes an operation on `subject` through `gate`, waiting while the
    /// gate holds it.
    async fn pass(&self, gate: Gate, subject: String) {
        let mut ticket = 0;
        self.update(|state| {
            let counts = state.gates.entry(gate).or_default();
            ticket = counts.arrived;
            counts.arrived = counts.arrived.saturating_add(1);
            state.log.push(GateEvent {
                gate,
                ticket,
                subject: subject.clone(),
                pass: GatePass::Arrived,
            });
        });
        self.wait_until(|state| {
            state
                .gates
                .get(&gate)
                .is_none_or(|counts| match counts.allowed {
                    None => true,
                    // A held gate lets its arrivals through one at a time,
                    // in ticket order, whichever waiter wakes first.
                    Some(allowed) => ticket < allowed && ticket == counts.passed,
                })
        })
        .await;
        self.update(|state| {
            let counts = state.gates.entry(gate).or_default();
            counts.passed = counts.passed.saturating_add(1);
            state.log.push(GateEvent {
                gate,
                ticket,
                subject,
                pass: GatePass::Passed,
            });
        });
    }
}

/// A [`SnapshotStore`] that takes each [`StoreOp`] of the wrapped store
/// through its two gates. Every other method goes straight through.
#[derive(Debug, Clone)]
pub struct GatedSnapshotStore {
    inner: SharedSnapshotStore,
    gates: SnapshotStoreGates,
}

impl GatedSnapshotStore {
    pub fn new(inner: SharedSnapshotStore, gates: SnapshotStoreGates) -> Self {
        Self { inner, gates }
    }

    pub fn gates(&self) -> &SnapshotStoreGates {
        &self.gates
    }

    pub fn inner(&self) -> &SharedSnapshotStore {
        &self.inner
    }

    async fn gated<T>(&self, op: StoreOp, subject: String, act: impl Future<Output = T>) -> T {
        self.gates.pass(Gate::before(op), subject.clone()).await;
        let answer = act.await;
        self.gates.pass(Gate::after(op), subject).await;
        answer
    }
}

/// The subject of a location in the gate log: its object key, or its kind.
fn location_subject(location: &SnapshotLocation) -> String {
    match location {
        SnapshotLocation::S3 { key, .. } => key.clone(),
        other => other.kind().to_string(),
    }
}

/// The subject of a staged snapshot: its object key, or `inline`.
fn staged_subject(staged: &StagedSnapshot) -> String {
    match staged {
        StagedSnapshot::Object(object) => object.path(),
        StagedSnapshot::Inline { .. } => "inline".to_owned(),
    }
}

impl SnapshotStore for GatedSnapshotStore {
    fn upload<'a>(
        &'a self,
        key: SnapshotKey,
        bytes: Bytes,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        self.inner.upload(key, bytes)
    }

    fn upload_iter<'a>(
        &'a self,
        key: SnapshotKey,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        self.inner.upload_iter(key, chunks)
    }

    fn namespace(&self, raft_group_id: RaftGroupId) -> OwnedNamespace {
        self.inner.namespace(raft_group_id)
    }

    fn stage<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        let subject = format!("group-{}/{index}", raft_group_id.0);
        Box::pin(self.gated(
            StoreOp::Stage,
            subject,
            self.inner.stage(raft_group_id, index, chunks),
        ))
    }

    fn write_staged<'a>(
        &'a self,
        staged: StagedSnapshot,
    ) -> SnapshotStoreFuture<'a, SnapshotLocation> {
        let subject = staged_subject(&staged);
        Box::pin(self.gated(
            StoreOp::WriteStaged,
            subject,
            self.inner.write_staged(staged),
        ))
    }

    fn list_own<'a>(&'a self, raft_group_id: RaftGroupId) -> SnapshotStoreFuture<'a, OwnListing> {
        let subject = format!("group-{}", raft_group_id.0);
        Box::pin(self.gated(
            StoreOp::ListOwn,
            subject,
            self.inner.list_own(raft_group_id),
        ))
    }

    fn delete_own<'a>(&'a self, object: OwnedObject) -> SnapshotStoreFuture<'a, ()> {
        let subject = object.path().to_owned();
        Box::pin(self.gated(StoreOp::DeleteOwn, subject, self.inner.delete_own(object)))
    }

    fn download<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, Vec<u8>> {
        Box::pin(self.gated(
            StoreOp::Get,
            location_subject(location),
            self.inner.download(location),
        ))
    }

    fn delete<'a>(&'a self, location: &'a SnapshotLocation) -> SnapshotStoreFuture<'a, ()> {
        self.inner.delete(location)
    }

    fn prune_retired<'a>(
        &'a self,
        raft_group_id: u32,
        current: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, SnapshotPrune> {
        self.inner.prune_retired(raft_group_id, current)
    }

    fn publish_reference<'a>(
        &'a self,
        raft_group_id: u32,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        self.inner.publish_reference(raft_group_id, location)
    }

    fn pin_reference<'a>(
        &'a self,
        raft_group_id: u32,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        self.inner.pin_reference(raft_group_id, location)
    }

    fn reconcile_reference_pins<'a>(
        &'a self,
        raft_group_id: u32,
        retained: &'a [SnapshotLocation],
    ) -> SnapshotStoreFuture<'a, ()> {
        self.inner.reconcile_reference_pins(raft_group_id, retained)
    }

    fn health_check(&self) -> SnapshotStoreFuture<'_, ()> {
        self.inner.health_check()
    }

    fn verify_uploaded<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, ()> {
        Box::pin(self.gated(
            StoreOp::Head,
            location_subject(location),
            self.inner.verify_uploaded(location),
        ))
    }
}
