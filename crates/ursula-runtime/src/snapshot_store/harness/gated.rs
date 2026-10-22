//! A snapshot store wrapper that pauses operations at named gates, so a
//! test can force one order of a build against a GC pass.
//!
//! Each owned-key operation ([`StoreOp`]) of a node has two gates: one
//! before it reaches the wrapped store and one after the store answered,
//! before the caller sees the answer ([`GatePoint`]). A gate passes
//! everything until a test holds it. While held, the operations that
//! arrive wait there, and [`SnapshotStoreGates::release`] lets them through
//! one at a time in the order they arrived. A waiter whose future is
//! dropped, by a timeout, a shutdown or a killed node, gives up its turn,
//! so it never holds back the ones behind it. Every arrival, pass and
//! abandoned wait is logged, so a test asserts the order the operations
//! took.
//!
//! The gates of a GC pass are its seams: `After(ListOwn)` sits between the
//! LIST and the read of the holds, and `Before(DeleteOwn)` between that
//! read and each DELETE. For example, to run a whole build between a pass's
//! LIST and its DELETEs, hold `After(ListOwn)`, start the pass, wait for its
//! arrival, run the build's lease, `write_staged` and commit, then release.
//!
//! The wrapper builds the wrapped store's future before the operation
//! reaches its first gate, and awaits it only after that gate. A store
//! whose method does work outside its future, before the future is first
//! polled, does that work before the gate. Every method of the stores in
//! this crate does its work inside the future.
//!
//! The order of passes comes from the tickets, never from the order in
//! which [`Notify`] wakes the waiters, and the gates draw nothing at
//! random. Under madsim the wake-ups are scheduled from the seed, so a
//! gated run replays.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::pin::pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

use ursula_shard::RaftGroupId;

use crate::rt::sync::Notify;
use crate::snapshot_store::NodeListing;
use crate::snapshot_store::NodeObject;
use crate::snapshot_store::OwnListing;
use crate::snapshot_store::OwnedLocation;
use crate::snapshot_store::OwnedNamespace;
use crate::snapshot_store::OwnedObject;
use crate::snapshot_store::SharedSnapshotStore;
use crate::snapshot_store::SnapshotBytesIterator;
use crate::snapshot_store::SnapshotLocation;
use crate::snapshot_store::SnapshotStore;
use crate::snapshot_store::SnapshotStoreFuture;
use crate::snapshot_store::StagedSnapshot;
use crate::snapshot_store::StoredSnapshot;

/// An operation of a [`SnapshotStore`] that has gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StoreOp {
    /// [`SnapshotStore::stage`] and [`SnapshotStore::stage_stored`].
    Stage,
    /// [`SnapshotStore::write_staged`], the PUT.
    WriteStaged,
    /// [`SnapshotStore::list_own`], the LIST.
    ListOwn,
    /// [`SnapshotStore::delete_own`], the DELETE.
    DeleteOwn,
    /// [`SnapshotStore::list_node_groups`], the LIST of a node's directory.
    ListNodeGroups,
    /// [`SnapshotStore::list_node_group`], the sweep's LIST of another
    /// node's directory for a group.
    ListNodeGroup,
    /// [`SnapshotStore::delete_node_object`], the sweep's DELETE.
    DeleteNodeObject,
    /// [`SnapshotStore::read_stored`], the GET.
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

/// One gate: a node's operation at a point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Gate {
    pub node_id: u64,
    pub op: StoreOp,
    pub point: GatePoint,
}

impl Gate {
    pub const fn before(node_id: u64, op: StoreOp) -> Self {
        Self {
            node_id,
            op,
            point: GatePoint::Before,
        }
    }

    pub const fn after(node_id: u64, op: StoreOp) -> Self {
        Self {
            node_id,
            op,
            point: GatePoint::After,
        }
    }
}

impl fmt::Display for Gate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node {} {:?}({:?})", self.node_id, self.point, self.op)
    }
}

/// A misuse of the gates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum GateError {
    /// Only a held gate can release operations.
    #[error("{gate} is not held")]
    NotHeld { gate: Gate },
}

/// What happened to an operation at a gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatePass {
    Arrived,
    Passed,
    /// The operation's future was dropped while it waited.
    Abandoned,
}

/// One entry of the gate log. `ticket` numbers the arrivals at the gate
/// from 0, and `subject` names what the operation acts on: the object key,
/// or the node, group and index of a stage or a LIST.
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
    abandoned: usize,
    /// While held, how many passes the gate allows in all.
    allowed: Option<usize>,
    /// The lowest ticket that neither passed nor was abandoned.
    next: usize,
    /// Tickets above `next` that passed or were abandoned.
    resolved_ahead: BTreeSet<usize>,
}

impl GateCounts {
    /// Whether the arrival with `ticket` may go through now.
    fn may_pass(&self, ticket: usize) -> bool {
        match self.allowed {
            None => true,
            // A held gate lets its arrivals through one at a time, in
            // ticket order, whichever waiter wakes first.
            Some(allowed) => self.passed < allowed && ticket == self.next,
        }
    }

    fn resolve(&mut self, ticket: usize) {
        if ticket != self.next {
            self.resolved_ahead.insert(ticket);
            return;
        }
        self.next = self.next.saturating_add(1);
        while self.resolved_ahead.remove(&self.next) {
            self.next = self.next.saturating_add(1);
        }
    }
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

/// The gates of [`GatedSnapshotStore`]s. Clones share them, so one set
/// gates the stores of several nodes, each under its own node id.
#[derive(Debug, Clone, Default)]
pub struct SnapshotStoreGates {
    shared: Arc<GatesShared>,
}

/// An operation waiting at a gate. Dropped before it passes, it gives up
/// its ticket.
struct Waiting<'g> {
    gates: &'g SnapshotStoreGates,
    gate: Gate,
    ticket: usize,
    /// Taken when the operation passes.
    subject: Option<String>,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        if let Some(subject) = self.subject.take() {
            self.gates
                .finish(self.gate, self.ticket, subject, GatePass::Abandoned);
        }
    }
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
                let waiting = counts
                    .arrived
                    .saturating_sub(counts.passed)
                    .saturating_sub(counts.abandoned);
                counts.allowed = Some(counts.passed.saturating_add(waiting));
            }
        });
    }

    /// Lets the next `count` operations held at `gate` through, in the
    /// order they arrived, those yet to arrive included. Refused unless
    /// the gate is held.
    pub fn release(&self, gate: Gate, count: usize) -> Result<(), GateError> {
        let mut released = Err(GateError::NotHeld { gate });
        self.update(|state| {
            if let Some(allowed) = state
                .gates
                .get_mut(&gate)
                .and_then(|counts| counts.allowed.as_mut())
            {
                *allowed = allowed.saturating_add(count);
                released = Ok(());
            }
        });
        released
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

    /// Every arrival, pass and abandoned wait, in order.
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

    /// Records that the arrival with `ticket` passed or was abandoned.
    fn finish(&self, gate: Gate, ticket: usize, subject: String, pass: GatePass) {
        self.update(|state| {
            let counts = state.gates.entry(gate).or_default();
            if pass == GatePass::Passed {
                counts.passed = counts.passed.saturating_add(1);
            } else {
                counts.abandoned = counts.abandoned.saturating_add(1);
            }
            counts.resolve(ticket);
            state.log.push(GateEvent {
                gate,
                ticket,
                subject,
                pass,
            });
        });
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
        let mut waiting = Waiting {
            gates: self,
            gate,
            ticket,
            subject: Some(subject),
        };
        self.wait_until(|state| {
            state
                .gates
                .get(&gate)
                .is_none_or(|counts| counts.may_pass(ticket))
        })
        .await;
        if let Some(subject) = waiting.subject.take() {
            self.finish(gate, ticket, subject, GatePass::Passed);
        }
    }
}

/// A [`SnapshotStore`] of node `node_id` that takes each [`StoreOp`] of the
/// wrapped store through its two gates. Every other method goes straight
/// through.
#[derive(Debug, Clone)]
pub struct GatedSnapshotStore {
    node_id: u64,
    inner: SharedSnapshotStore,
    gates: SnapshotStoreGates,
}

impl GatedSnapshotStore {
    pub fn new(node_id: u64, inner: SharedSnapshotStore, gates: SnapshotStoreGates) -> Self {
        Self {
            node_id,
            inner,
            gates,
        }
    }

    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    pub fn gates(&self) -> &SnapshotStoreGates {
        &self.gates
    }

    pub fn inner(&self) -> &SharedSnapshotStore {
        &self.inner
    }

    async fn gated<T>(&self, op: StoreOp, subject: String, act: impl Future<Output = T>) -> T {
        self.gates
            .pass(Gate::before(self.node_id, op), subject.clone())
            .await;
        let answer = act.await;
        self.gates
            .pass(Gate::after(self.node_id, op), subject)
            .await;
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
    fn namespace(&self, raft_group_id: RaftGroupId) -> OwnedNamespace {
        self.inner.namespace(raft_group_id)
    }

    fn stage<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        chunks: SnapshotBytesIterator,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        let subject = format!("node-{}/group-{}/{index}", self.node_id, raft_group_id.0);
        Box::pin(self.gated(
            StoreOp::Stage,
            subject,
            self.inner.stage(raft_group_id, index, chunks),
        ))
    }

    fn write_staged<'a>(
        &'a self,
        staged: StagedSnapshot,
    ) -> SnapshotStoreFuture<'a, OwnedLocation> {
        let subject = staged_subject(&staged);
        Box::pin(self.gated(
            StoreOp::WriteStaged,
            subject,
            self.inner.write_staged(staged),
        ))
    }

    fn list_own<'a>(&'a self, raft_group_id: RaftGroupId) -> SnapshotStoreFuture<'a, OwnListing> {
        let subject = format!("node-{}/group-{}", self.node_id, raft_group_id.0);
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

    fn list_node_groups<'a>(&'a self, node: u64) -> SnapshotStoreFuture<'a, BTreeSet<RaftGroupId>> {
        Box::pin(self.gated(
            StoreOp::ListNodeGroups,
            format!("node-{node}"),
            self.inner.list_node_groups(node),
        ))
    }

    fn list_node_group<'a>(
        &'a self,
        node: u64,
        raft_group_id: RaftGroupId,
    ) -> SnapshotStoreFuture<'a, NodeListing> {
        Box::pin(self.gated(
            StoreOp::ListNodeGroup,
            format!("node-{node}/group-{}", raft_group_id.0),
            self.inner.list_node_group(node, raft_group_id),
        ))
    }

    fn delete_node_object<'a>(&'a self, object: NodeObject) -> SnapshotStoreFuture<'a, ()> {
        let subject = object.path().to_owned();
        Box::pin(self.gated(
            StoreOp::DeleteNodeObject,
            subject,
            self.inner.delete_node_object(object),
        ))
    }

    fn read_stored<'a>(
        &'a self,
        location: &'a SnapshotLocation,
    ) -> SnapshotStoreFuture<'a, StoredSnapshot> {
        Box::pin(self.gated(
            StoreOp::Get,
            location_subject(location),
            self.inner.read_stored(location),
        ))
    }

    fn stage_stored<'a>(
        &'a self,
        raft_group_id: RaftGroupId,
        index: u64,
        stored: StoredSnapshot,
    ) -> SnapshotStoreFuture<'a, StagedSnapshot> {
        let subject = format!("node-{}/group-{}/{index}", self.node_id, raft_group_id.0);
        Box::pin(self.gated(
            StoreOp::Stage,
            subject,
            self.inner.stage_stored(raft_group_id, index, stored),
        ))
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
