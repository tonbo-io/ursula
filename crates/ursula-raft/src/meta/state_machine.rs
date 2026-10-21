//! The meta group's state machine: its dispatch of [`RaftCommand`], its
//! inline snapshot record, and the views it publishes.
//!
//! Its dispatch is frozen with format epoch 6, like the kernel it applies:
//!
//! - a stream entry stops the apply, as a meta entry stops a data group's;
//! - a history event whose `previous` is not the current history applies
//!   nothing and answers [`MetaResponse::HistoryMismatch`], and one that
//!   names it becomes the history and begins it in the kernel
//!   (`ControlPlaneState::begin_history`);
//! - a control command is applied by the kernel
//!   (`ControlPlaneState::apply`), at the entry's log id.
//!
//! A snapshot is the data path's snapshot record (`group-0.snapshot.json`
//! under `raft-log/meta/`) with an inline pointer ([`MetaSnapshotPointer`]).
//! The state machine holds no snapshot store, so no configured backend can
//! reach it. A build syncs the journal before its record (R1) and writes
//! nothing when it is not above the current snapshot, under the record's
//! publication lock, as the data path's do. Every pointer it builds,
//! installs or restores is parsed into a [`MetaSnapshotPointer`] first,
//! which refuses one that is not inline or above the bound.

use std::io;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::EntryPayload;
use openraft::alias::EntryPayloadOf;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use ursula_control::ControlPlaneState;
use ursula_control::PlacementView;
use ursula_proto::placement::MetaLogId;
use ursula_proto::placement::NodeId;
use ursula_proto::placement::RaftLogId;
use ursula_proto::placement::ViewStage;
use ursula_runtime::HistoryPublisher;
use ursula_runtime::SnapshotPointer;
use ursula_shard::RaftGroupId;
use ursula_stream::HistoryEvent;

use super::MetaCommand;
use super::MetaResponse;
use super::MetaState;
use super::replica::MetaReplicaError;
use super::snapshot::MetaSnapshotPointer;
use super::watch::PlacementPublisher;
use crate::apply_failure::ApplyError;
use crate::apply_failure::ApplyHealth;
use crate::engine::invalid_data;
use crate::openraft_work::WorkToken;
use crate::replica::HistoryProposal;
use crate::replica::ReplicaStateMachine;
use crate::state_machine::CurrentSnapshot;
use crate::state_machine::MetadataPublicationState;
use crate::state_machine::RecordSite;
use crate::state_machine::persist_snapshot_metadata;
use crate::state_machine::read_snapshot_record;
use crate::state_machine::snapshot_metadata_work;
use crate::types::RaftCommand;
use crate::types::RaftGroupResponse;
use crate::types::SnapshotData;
use crate::types::UrsulaRaftTypeConfig as C;
use crate::types::UrsulaSnapshot;

/// The meta group's id, in its own namespace: the wire, the journal and
/// the snapshot record name it group 0.
pub(crate) const META_GROUP: RaftGroupId = RaftGroupId(0);

/// The meta log id of the entry at `log_id`: its term, its leader and its
/// index.
pub fn meta_log_id(log_id: LogIdOf<C>) -> MetaLogId {
    let leader = log_id.committed_leader_id();
    MetaLogId::new(RaftLogId {
        term: leader.term,
        leader: NodeId::new(leader.node_id),
        index: log_id.index(),
    })
}

/// The meta log id of the entry that applied `event`: its proposer is the
/// entry's leader, and its index the entry's.
fn history_log_id(event: &HistoryEvent) -> MetaLogId {
    MetaLogId::new(RaftLogId {
        term: event.leader.term,
        leader: NodeId::new(event.leader.node_id),
        index: event.event_index,
    })
}

/// The id of the meta snapshot that covers the log through `last_log_id`.
fn meta_snapshot_id(last_log_id: Option<LogIdOf<C>>) -> String {
    match last_log_id {
        Some(log_id) => format!("meta-{}-{}", log_id.committed_leader_id(), log_id.index()),
        None => "meta-empty".to_owned(),
    }
}

/// The meta group's state machine on this node (see the module docs).
pub struct MetaStateMachine {
    control: Arc<ControlPlaneState>,
    history: Option<HistoryEvent>,
    last_applied: Option<LogIdOf<C>>,
    last_membership: StoredMembershipOf<C>,
    history_publisher: HistoryPublisher,
    placement: PlacementPublisher,
    record: crate::SnapshotRecord,
    current: Arc<Mutex<Option<CurrentSnapshot>>>,
    pub(crate) metadata_serial: Arc<crate::rt::sync::Mutex<MetadataPublicationState>>,
    pub(crate) apply_health: ApplyHealth,
    apply_stop_signal: Arc<AtomicBool>,
    /// The engine's token: OpenRaft's state-machine worker owns this state
    /// machine, and the replica's shutdown waits until it is dropped.
    pub(crate) work: WorkToken,
}

impl MetaStateMachine {
    /// An empty state machine over the meta group's snapshot `record`,
    /// publishing its history to `history_publisher` and its views to
    /// `placement`. `apply_stop_signal` is the log store's.
    pub(crate) fn new(
        record: crate::SnapshotRecord,
        history_publisher: HistoryPublisher,
        placement: PlacementPublisher,
        apply_stop_signal: Arc<AtomicBool>,
    ) -> Self {
        Self {
            control: Arc::new(ControlPlaneState::default()),
            history: None,
            last_applied: None,
            last_membership: StoredMembershipOf::<C>::default(),
            history_publisher,
            placement,
            record,
            current: Arc::default(),
            metadata_serial: Arc::default(),
            apply_health: ApplyHealth::default(),
            apply_stop_signal,
            work: WorkToken::detached(),
        }
    }

    /// The view of what this state machine holds now.
    fn view(&self) -> PlacementView {
        let stage = match (&self.history, self.last_applied) {
            (Some(event), Some(applied)) => ViewStage::Applied {
                applied: meta_log_id(applied),
                history: history_log_id(event),
            },
            _ => ViewStage::Empty,
        };
        PlacementView {
            stage,
            state: Arc::clone(&self.control),
        }
    }

    /// Publishes the history and the view this state machine holds.
    fn publish(&self) {
        self.history_publisher.publish(self.history);
        self.placement.publish(self.view());
    }

    /// Restores the snapshot record this node persisted, if any, before the
    /// meta Raft reads its log, and publishes what it holds: an empty view
    /// when there is no record. The pointer is parsed before the record is
    /// checked against the log, so a record that is not inline is refused
    /// as such, whatever it covers.
    pub(crate) fn restore(&mut self) -> Result<(), MetaReplicaError> {
        let site = RecordSite::Store(self.record.clone());
        let Some(persisted) =
            read_snapshot_record(&site, META_GROUP).map_err(MetaReplicaError::Record)?
        else {
            self.publish();
            return Ok(());
        };
        let pointer = MetaSnapshotPointer::try_from(persisted.pointer.clone())
            .map_err(MetaReplicaError::Pointer)?;
        if let Some(snapshot) = persisted.meta.last_log_id {
            self.record
                .check_restored(snapshot)
                .map_err(MetaReplicaError::LogBelowSnapshot)?;
        }
        persisted.sync_dir().map_err(MetaReplicaError::Record)?;
        let state = pointer.state().map_err(MetaReplicaError::State)?;
        self.replace(state, &persisted.meta);
        *self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(CurrentSnapshot::inline(
            persisted.meta,
            persisted.pointer_bytes,
        ));
        self.publish();
        Ok(())
    }

    /// Replaces the state with a snapshot's `state` and `meta`.
    fn replace(&mut self, state: MetaState, meta: &SnapshotMetaOf<C>) {
        self.control = Arc::new(state.control);
        self.history = state.history;
        self.last_applied = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
    }

    /// Applies the meta command of the entry at `log_id`: the dispatch the
    /// module docs describe.
    pub(crate) fn apply_command(
        &mut self,
        log_id: LogIdOf<C>,
        command: MetaCommand,
    ) -> MetaResponse {
        let at = meta_log_id(log_id);
        match command {
            MetaCommand::NewHistory {
                history,
                previous,
                kept,
                leader,
                cause,
            } => {
                let current = self.history.map_or(0, |event| event.id.get());
                if previous != current {
                    return MetaResponse::HistoryMismatch { current };
                }
                self.history = Some(HistoryEvent {
                    id: history,
                    event_index: log_id.index(),
                    previous,
                    kept,
                    leader,
                    imported: None,
                });
                let begun = Arc::make_mut(&mut self.control).begin_history(at, cause);
                tracing::info!(history = %at, ?cause, ?begun, "the meta group began a new history");
                MetaResponse::History
            }
            MetaCommand::Control(command) => {
                MetaResponse::Control(Arc::make_mut(&mut self.control).apply(at, command))
            }
        }
    }

    /// Applies one entry. An error stops the apply.
    fn apply_entry(
        &mut self,
        log_id: LogIdOf<C>,
        payload: EntryPayloadOf<C>,
    ) -> Result<RaftGroupResponse, ApplyError> {
        match payload {
            EntryPayload::Blank => Ok(RaftGroupResponse::Blank),
            EntryPayload::Membership(membership) => {
                self.last_membership = StoredMembershipOf::<C>::new(Some(log_id), membership);
                Ok(RaftGroupResponse::Membership)
            }
            // The meta group never commits a stream command: the plane of
            // every request admits only its own (`RaftTarget::admits`).
            EntryPayload::Normal(RaftCommand::Stream(_)) => Err(ApplyError::InvariantViolation(
                crate::forward::invalid_raft_command(
                    ursula_runtime::UnproposableCommand::OtherPlane,
                ),
            )),
            EntryPayload::Normal(RaftCommand::Meta(command)) => {
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    self.apply_command(log_id, command)
                }))
                .map(RaftGroupResponse::Meta)
                .map_err(|payload| ApplyError::Panic {
                    message: payload
                        .downcast_ref::<&str>()
                        .map(|value| (*value).to_owned())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "non-string panic payload".to_owned()),
                })
            }
        }
    }

    /// Stops the apply at `log_id` for `error`, as a data group's stops.
    fn stop_apply(&self, log_id: LogIdOf<C>, error: ApplyError) -> io::Error {
        let stopped = crate::apply_failure::StoppedApply {
            term: log_id.committed_leader_id().term,
            index: log_id.index(),
            kind: error.kind(),
            message: error.to_string(),
            last_applied: self.last_applied,
        };
        tracing::error!(log_index = stopped.index, error = %error, "stopping the meta group's apply; preserve its WAL for corrected-code replay");
        self.apply_health.stop(stopped);
        self.apply_stop_signal.store(true, Ordering::Release);
        // OpenRaft's state-machine interface requires io::Error here.
        io::Error::other(error)
    }

    /// The history event this state machine applied last.
    #[cfg(all(test, not(madsim)))]
    pub(crate) fn history(&self) -> Option<HistoryEvent> {
        self.history
    }

    /// The kernel state this state machine holds.
    #[cfg(all(test, not(madsim)))]
    pub(crate) fn control(&self) -> &ControlPlaneState {
        &self.control
    }

    /// The view this state machine would publish now.
    #[cfg(all(test, not(madsim)))]
    pub(crate) fn view_for_test(&self) -> PlacementView {
        self.view()
    }

    /// The membership this state machine applied last.
    pub(crate) fn applied_membership(&self) -> StoredMembershipOf<C> {
        self.last_membership.clone()
    }

    fn snapshot_meta(&self) -> SnapshotMetaOf<C> {
        SnapshotMetaOf::<C> {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
        }
    }
}

/// The refusal of snapshot work of a replica that is shutting down.
fn stopped() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "the meta replica is shutting down",
    )
}

impl RaftStateMachine<C> for MetaStateMachine {
    type SnapshotData = SnapshotData;
    type SnapshotBuilder = MetaSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogIdOf<C>>, StoredMembershipOf<C>), io::Error> {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<C>, io::Error>> + Unpin + openraft::OptionalSend
    {
        let mut applied = false;
        while let Some((entry, responder)) = entries.try_next().await? {
            let response = match self.apply_entry(entry.log_id, entry.payload) {
                Ok(response) => response,
                Err(error) => {
                    // What applied before the stop is published.
                    if applied {
                        self.publish();
                    }
                    return Err(self.stop_apply(entry.log_id, error));
                }
            };
            self.last_applied = Some(entry.log_id);
            applied = true;
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        if applied {
            self.publish();
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaSnapshotBuilder {
            control: Arc::clone(&self.control),
            history: self.history,
            meta: self.snapshot_meta(),
            record: self.record.clone(),
            current: Arc::clone(&self.current),
            metadata_serial: Arc::clone(&self.metadata_serial),
            apply_health: self.apply_health.clone(),
            work: self.work.as_kind(crate::openraft_work::WorkKind::Builder),
        }
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<C>,
        snapshot: SnapshotData,
    ) -> Result<(), io::Error> {
        let (pointer_bytes, _lease) = snapshot.into_parts();
        if self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|current| current.meta == *meta)
        {
            return Ok(());
        }
        if self.work.is_stopping() {
            return Err(stopped());
        }
        let pointer = SnapshotPointer::decode(&pointer_bytes).map_err(invalid_data)?;
        let state = MetaSnapshotPointer::try_from(pointer)
            .map_err(invalid_data)?
            .state()
            .map_err(invalid_data)?;
        self.replace(state, meta);
        // The truncate of a conflicting tail this install replaced is
        // durable before the record is (R1).
        self.record.sync().await.map_err(io::Error::from)?;
        let record = self.record.clone();
        let current = Arc::clone(&self.current);
        let meta = meta.clone();
        snapshot_metadata_work(Arc::clone(&self.metadata_serial), move || {
            persist_snapshot_metadata(record.path(), record.generation(), &meta, &pointer_bytes)?;
            *current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some(CurrentSnapshot::inline(meta, pointer_bytes));
            Ok(())
        })
        .await?;
        self.publish();
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<UrsulaSnapshot>, io::Error> {
        Ok(self
            .current
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map(|current| UrsulaSnapshot {
                meta: current.meta.clone(),
                snapshot: SnapshotData::new(current.pointer_bytes().to_vec()),
            }))
    }
}

/// A meta snapshot build: the state as the state machine held it when
/// OpenRaft asked, encoded and recorded in a task of OpenRaft's own.
pub struct MetaSnapshotBuilder {
    control: Arc<ControlPlaneState>,
    history: Option<HistoryEvent>,
    meta: SnapshotMetaOf<C>,
    record: crate::SnapshotRecord,
    current: Arc<Mutex<Option<CurrentSnapshot>>>,
    metadata_serial: Arc<crate::rt::sync::Mutex<MetadataPublicationState>>,
    apply_health: ApplyHealth,
    work: WorkToken,
}

impl RaftSnapshotBuilder<C> for MetaSnapshotBuilder {
    type SnapshotData = SnapshotData;

    async fn build_snapshot(&mut self) -> Result<UrsulaSnapshot, io::Error> {
        self.apply_health
            .check(META_GROUP)
            .map_err(crate::engine::group_engine_io_error)?;
        if self.work.is_stopping() {
            return Err(stopped());
        }
        let state = MetaState {
            history: self.history,
            control: (*self.control).clone(),
        };
        // A state above the bound is refused here, never written.
        let pointer = MetaSnapshotPointer::new(meta_snapshot_id(self.meta.last_log_id), &state)
            .map_err(invalid_data)?;
        let pointer_bytes = pointer
            .into_pointer()
            .encode_binary()
            .map_err(invalid_data)?;
        // The journal before the record (R1): no truncate syncs by itself.
        self.record.sync().await.map_err(io::Error::from)?;
        let record = self.record.clone();
        let current = Arc::clone(&self.current);
        let meta = self.meta.clone();
        let chosen = snapshot_metadata_work(Arc::clone(&self.metadata_serial), move || {
            // A build not above the current snapshot, which an install or a
            // later build made meanwhile, writes nothing. Every publication
            // runs under the serial lock, so the slot holds still between
            // this read and the write below.
            let held = current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
                .filter(|held| held.meta.last_log_id >= meta.last_log_id);
            if let Some(held) = held {
                return Ok(held);
            }
            persist_snapshot_metadata(record.path(), record.generation(), &meta, &pointer_bytes)?;
            let built = CurrentSnapshot::inline(meta, pointer_bytes);
            *current
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(built.clone());
            Ok(built)
        })
        .await?;
        Ok(UrsulaSnapshot {
            meta: chosen.meta.clone(),
            snapshot: SnapshotData::new(chosen.pointer_bytes().to_vec()),
        })
    }
}

impl ReplicaStateMachine for MetaStateMachine {
    type Answer = ursula_control::ControlResponse;

    fn target(_group: RaftGroupId) -> crate::RaftTarget {
        crate::RaftTarget::Meta
    }

    fn answer(
        response: RaftGroupResponse,
        expected: ursula_runtime::ResponseKind,
    ) -> Result<
        Result<Self::Answer, ursula_runtime::GroupEngineError>,
        ursula_runtime::GroupEngineError,
    > {
        crate::forward::control_answer_from_raft_response(response, expected)
    }

    fn history_command(proposal: HistoryProposal) -> RaftCommand {
        let HistoryProposal {
            history,
            now_ms: _,
            previous,
            kept,
            leader,
            cause,
        } = proposal;
        RaftCommand::Meta(MetaCommand::NewHistory {
            history,
            previous,
            kept,
            leader,
            cause: match cause {
                crate::replica::HistoryCause::Genesis => ursula_control::HistoryCause::Genesis,
                crate::replica::HistoryCause::Plan { .. } => ursula_control::HistoryCause::Plan,
            },
        })
    }

    fn history_refused(response: &RaftGroupResponse) -> bool {
        matches!(
            response,
            RaftGroupResponse::Meta(MetaResponse::HistoryMismatch { .. })
        )
    }
}
