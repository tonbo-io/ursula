use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::io;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::Stream;
use futures_util::TryStreamExt;
use openraft::BasicNode;
use openraft::Config;
use openraft::EntryPayload;
use openraft::OptionalSend;
use openraft::Raft;
use openraft::RaftNetworkFactory;
use openraft::alias::LogIdOf;
use openraft::alias::SnapshotDataOf;
use openraft::alias::SnapshotMetaOf;
use openraft::alias::SnapshotOf;
use openraft::alias::StoredMembershipOf;
use openraft::storage::EntryResponder;
use openraft::storage::RaftLogStorage;
use openraft::storage::RaftSnapshotBuilder;
use openraft::storage::RaftStateMachine;
use ursula_control::ControlCommand;
use ursula_control::ControlPlaneState;
use ursula_control::ControlResponse;

use crate::registry::SingleNodeRaftNetworkFactory;

#[cfg(madsim)]
type MetaOpenRaftRuntime = crate::sim_runtime::MadsimOpenRaftRuntime;
#[cfg(not(madsim))]
type MetaOpenRaftRuntime = openraft::impls::TokioRuntime;

openraft::declare_raft_types!(
    pub MetaRaftTypeConfig:
        D = ControlCommand,
        R = ControlResponse,
        Node = openraft::BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = MetaOpenRaftRuntime,
);

pub type MetaRaft = Raft<MetaRaftTypeConfig, MetaRaftStateMachine>;

#[derive(Debug)]
pub struct MetaRaftError {
    operation: &'static str,
    message: String,
    source: Option<Box<dyn Error + Send + Sync + 'static>>,
}

impl MetaRaftError {
    pub fn new(operation: &'static str, message: impl Into<String>) -> Self {
        Self {
            operation,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        operation: &'static str,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            operation,
            message: source.to_string(),
            source: Some(Box::new(source)),
        }
    }

    pub fn operation(&self) -> &'static str {
        self.operation
    }
}

impl fmt::Display for MetaRaftError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            f.write_str(self.operation)
        } else {
            write!(f, "{}: {}", self.operation, self.message)
        }
    }
}

impl Error for MetaRaftError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| &**source as &(dyn Error + 'static))
    }
}

#[derive(Clone)]
pub struct MetaRaftHandle {
    raft: MetaRaft,
}

impl MetaRaftHandle {
    pub async fn new_node_with_log_store_and_network<NF, LS>(
        node_id: u64,
        config: Arc<Config>,
        network_factory: NF,
        log_store: LS,
    ) -> Result<Self, MetaRaftError>
    where
        NF: RaftNetworkFactory<MetaRaftTypeConfig>,
        LS: RaftLogStorage<MetaRaftTypeConfig>,
    {
        let raft = MetaRaft::new(
            node_id,
            config,
            network_factory,
            log_store,
            MetaRaftStateMachine::default(),
        )
        .await
        .map_err(|err| MetaRaftError::with_source("create meta OpenRaft group", err))?;

        Ok(Self { raft })
    }

    pub async fn new_single_node_with_log_store<LS>(
        node_id: u64,
        node: BasicNode,
        config: Arc<Config>,
        log_store: LS,
    ) -> Result<Self, MetaRaftError>
    where
        LS: RaftLogStorage<MetaRaftTypeConfig>,
    {
        let handle = Self::new_node_with_log_store_and_network(
            node_id,
            config,
            SingleNodeRaftNetworkFactory,
            log_store,
        )
        .await?;

        let mut nodes = BTreeMap::new();
        nodes.insert(node_id, node);
        handle.initialize_membership(nodes).await?;
        handle
            .wait_for_current_leader(node_id, Duration::from_secs(2))
            .await?;

        Ok(handle)
    }

    pub async fn initialize_membership(
        &self,
        nodes: BTreeMap<u64, BasicNode>,
    ) -> Result<(), MetaRaftError> {
        let initialized =
            self.raft.is_initialized().await.map_err(|err| {
                MetaRaftError::with_source("check meta OpenRaft initialization", err)
            })?;
        if initialized {
            return Ok(());
        }

        self.raft
            .initialize(nodes)
            .await
            .map_err(|err| MetaRaftError::with_source("initialize meta OpenRaft group", err))
    }

    pub async fn wait_for_current_leader(
        &self,
        node_id: u64,
        timeout: Duration,
    ) -> Result<(), MetaRaftError> {
        self.raft
            .wait(Some(timeout))
            .current_leader(
                node_id,
                "meta OpenRaft group should observe expected leader",
            )
            .await
            .map(|_| ())
            .map_err(|err| MetaRaftError::with_source("wait for meta OpenRaft leadership", err))
    }

    pub fn raft_handle(&self) -> MetaRaft {
        self.raft.clone()
    }

    pub async fn write(&self, command: ControlCommand) -> Result<ControlResponse, MetaRaftError> {
        self.raft
            .client_write(command)
            .await
            .map(|response| response.data)
            .map_err(|err| MetaRaftError::with_source("write meta OpenRaft command", err))
    }

    pub async fn with_state_machine<V>(
        &self,
        f: impl FnOnce(&mut MetaRaftStateMachine) -> openraft::base::BoxFuture<V>
        + OptionalSend
        + 'static,
    ) -> Result<V, MetaRaftError>
    where
        V: OptionalSend + 'static,
    {
        self.raft
            .with_state_machine(f)
            .await
            .map_err(|err| MetaRaftError::with_source("access meta OpenRaft state machine", err))
    }

    pub async fn read_state<V>(
        &self,
        f: impl FnOnce(&ControlPlaneState) -> V + OptionalSend + 'static,
    ) -> Result<V, MetaRaftError>
    where
        V: OptionalSend + 'static,
    {
        self.with_state_machine(move |state_machine| {
            let value = f(state_machine.state());
            Box::pin(async move { value })
        })
        .await
    }

    pub async fn shutdown(&self) -> Result<(), MetaRaftError> {
        self.raft
            .shutdown()
            .await
            .map_err(|err| MetaRaftError::with_source("shutdown meta OpenRaft group", err))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MetaRaftStateMachine {
    state: ControlPlaneState,
    last_response: Option<ControlResponse>,
    last_applied_log_id: Option<LogIdOf<MetaRaftTypeConfig>>,
    last_membership: StoredMembershipOf<MetaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<MetaCurrentSnapshot>>>,
}

#[derive(Debug, Clone)]
struct MetaCurrentSnapshot {
    meta: SnapshotMetaOf<MetaRaftTypeConfig>,
    bytes: Vec<u8>,
}

impl MetaRaftStateMachine {
    pub fn state(&self) -> &ControlPlaneState {
        &self.state
    }

    pub fn applied_log_id(&self) -> Option<LogIdOf<MetaRaftTypeConfig>> {
        self.last_applied_log_id
    }

    pub fn last_response(&self) -> Option<&ControlResponse> {
        self.last_response.as_ref()
    }

    fn snapshot_meta(&self) -> SnapshotMetaOf<MetaRaftTypeConfig> {
        SnapshotMetaOf::<MetaRaftTypeConfig> {
            last_log_id: self.last_applied_log_id,
            last_membership: self.last_membership.clone(),
            snapshot_id: self
                .last_applied_log_id
                .map(|log_id| format!("meta-{}-{}", log_id.committed_leader_id(), log_id.index()))
                .unwrap_or_else(|| "meta-empty".to_owned()),
        }
    }
}

impl RaftStateMachine<MetaRaftTypeConfig> for MetaRaftStateMachine {
    type SnapshotBuilder = MetaRaftSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogIdOf<MetaRaftTypeConfig>>,
            StoredMembershipOf<MetaRaftTypeConfig>,
        ),
        io::Error,
    > {
        Ok((self.last_applied_log_id, self.last_membership.clone()))
    }

    async fn apply<Strm>(&mut self, mut entries: Strm) -> Result<(), io::Error>
    where Strm: Stream<Item = Result<EntryResponder<MetaRaftTypeConfig>, io::Error>>
            + Unpin
            + openraft::OptionalSend {
        while let Some((entry, responder)) = entries.try_next().await? {
            self.last_applied_log_id = Some(entry.log_id);
            let response = match entry.payload {
                EntryPayload::Blank => ControlResponse::Ok,
                EntryPayload::Normal(command) => self.state.apply(command),
                EntryPayload::Membership(membership) => {
                    self.last_membership = StoredMembershipOf::<MetaRaftTypeConfig>::new(
                        Some(entry.log_id),
                        membership,
                    );
                    ControlResponse::Ok
                }
            };
            self.last_response = Some(response.clone());
            if let Some(responder) = responder {
                responder.send(response);
            }
        }
        Ok(())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetaRaftSnapshotBuilder {
            state: self.state.clone(),
            meta: self.snapshot_meta(),
            current_snapshot: self.current_snapshot.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<SnapshotDataOf<MetaRaftTypeConfig>, io::Error> {
        Ok(Cursor::new(Vec::new()))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMetaOf<MetaRaftTypeConfig>,
        snapshot: SnapshotDataOf<MetaRaftTypeConfig>,
    ) -> Result<(), io::Error> {
        let bytes = snapshot.into_inner();
        self.state = serde_json::from_slice(&bytes).map_err(invalid_snapshot)?;
        self.last_applied_log_id = meta.last_log_id;
        self.last_membership = meta.last_membership.clone();
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(MetaCurrentSnapshot {
            meta: meta.clone(),
            bytes,
        });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<SnapshotOf<MetaRaftTypeConfig>>, io::Error> {
        Ok(self
            .current_snapshot
            .lock()
            .expect("snapshot mutex")
            .as_ref()
            .map(|snapshot| SnapshotOf::<MetaRaftTypeConfig> {
                meta: snapshot.meta.clone(),
                snapshot: Cursor::new(snapshot.bytes.clone()),
            }))
    }
}

#[derive(Debug, Clone)]
pub struct MetaRaftSnapshotBuilder {
    state: ControlPlaneState,
    meta: SnapshotMetaOf<MetaRaftTypeConfig>,
    current_snapshot: Arc<Mutex<Option<MetaCurrentSnapshot>>>,
}

impl RaftSnapshotBuilder<MetaRaftTypeConfig> for MetaRaftSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<SnapshotOf<MetaRaftTypeConfig>, io::Error> {
        let bytes = serde_json::to_vec(&self.state).map_err(invalid_snapshot)?;
        *self.current_snapshot.lock().expect("snapshot mutex") = Some(MetaCurrentSnapshot {
            meta: self.meta.clone(),
            bytes: bytes.clone(),
        });
        Ok(SnapshotOf::<MetaRaftTypeConfig> {
            meta: self.meta.clone(),
            snapshot: Cursor::new(bytes),
        })
    }
}

fn invalid_snapshot(err: serde_json::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use futures_util::stream;
    use openraft::EntryPayload;
    use openraft::LogId;
    use openraft::RaftTypeConfig;
    use openraft::entry::RaftEntry;
    use openraft::storage::RaftSnapshotBuilder;
    use openraft::storage::RaftStateMachine;
    use openraft::vote::RaftLeaderId;
    use ursula_control::ControlCommand;
    use ursula_control::ControlResponse;

    use super::*;

    type LeaderId = <MetaRaftTypeConfig as RaftTypeConfig>::LeaderId;

    fn log_id(index: u64) -> LogId<LeaderId> {
        LogId {
            leader_id: LeaderId::new(1, 1),
            index,
        }
    }

    fn entry(index: u64, command: ControlCommand) -> <MetaRaftTypeConfig as RaftTypeConfig>::Entry {
        <MetaRaftTypeConfig as RaftTypeConfig>::Entry::new(
            log_id(index),
            EntryPayload::Normal(command),
        )
    }

    fn set(values: impl IntoIterator<Item = u64>) -> BTreeSet<u64> {
        values.into_iter().collect()
    }

    fn membership_entry(
        index: u64,
        voters: impl IntoIterator<Item = u64>,
    ) -> <MetaRaftTypeConfig as RaftTypeConfig>::Entry {
        let voters = set(voters);
        let membership =
            openraft::Membership::new_with_defaults(vec![voters.clone()], voters.clone());
        <MetaRaftTypeConfig as RaftTypeConfig>::Entry::new(
            log_id(index),
            EntryPayload::Membership(membership),
        )
    }

    #[tokio::test]
    async fn meta_state_machine_applies_control_commands() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 5,
                    client_url: "http://node5:4491/".to_owned(),
                    cluster_url: "http://node5:4492/".to_owned(),
                    labels: BTreeMap::from([("az".to_owned(), "a".to_owned())]),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let node = machine.state().nodes.get(&5).expect("node registered");
        assert_eq!(node.client_url, "http://node5:4491");
        assert_eq!(node.cluster_url, "http://node5:4492");
        assert_eq!(node.labels.get("az").map(String::as_str), Some("a"));
        assert_eq!(machine.applied_log_id(), Some(log_id(1)));
    }

    #[tokio::test]
    async fn meta_state_machine_records_rejected_command_responses() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 5,
                    client_url: "   ".to_owned(),
                    cluster_url: "http://node5:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply rejected register node");

        assert!(!machine.state().nodes.contains_key(&5));
        assert_eq!(
            machine.last_response(),
            Some(&ControlResponse::Rejected {
                reason: "client_url must not be empty".to_owned(),
            })
        );
    }

    #[tokio::test]
    async fn meta_state_machine_records_membership_logs() {
        let mut machine = MetaRaftStateMachine::default();

        machine
            .apply(stream::iter([Ok((membership_entry(1, [1, 2, 3]), None))]))
            .await
            .expect("apply membership");

        let (applied, membership) = machine.applied_state().await.expect("applied state");
        assert_eq!(applied, Some(log_id(1)));
        assert_eq!(membership.log_id(), &Some(log_id(1)));
        assert_eq!(
            membership.voter_ids().collect::<BTreeSet<_>>(),
            set([1, 2, 3])
        );
        assert_eq!(machine.last_response(), Some(&ControlResponse::Ok));
    }

    #[tokio::test]
    async fn meta_snapshot_builder_round_trips_control_state() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut restored = MetaRaftStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");

        assert!(restored.state().nodes.contains_key(&7));
        assert_eq!(restored.applied_log_id(), Some(log_id(1)));
    }

    #[tokio::test]
    async fn meta_snapshot_builder_updates_current_snapshot() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        builder.build_snapshot().await.expect("build snapshot");
        let current = machine
            .get_current_snapshot()
            .await
            .expect("current snapshot")
            .expect("snapshot present");

        assert_eq!(current.meta.last_log_id, Some(log_id(1)));
        let decoded: ursula_control::ControlPlaneState =
            serde_json::from_slice(current.snapshot.into_inner().as_slice())
                .expect("decode current snapshot");
        assert!(decoded.nodes.contains_key(&7));
    }

    #[tokio::test]
    async fn meta_state_machine_reports_current_snapshot_after_install() {
        let mut machine = MetaRaftStateMachine::default();
        machine
            .apply(stream::iter([Ok((
                entry(1, ControlCommand::RegisterNode {
                    node_id: 7,
                    client_url: "http://node7:4491".to_owned(),
                    cluster_url: "http://node7:4492".to_owned(),
                    labels: BTreeMap::new(),
                    now_ms: 10,
                }),
                None,
            ))]))
            .await
            .expect("apply register node");

        let mut builder = machine.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.expect("build snapshot");

        let mut restored = MetaRaftStateMachine::default();
        restored
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install snapshot");
        let current = restored
            .get_current_snapshot()
            .await
            .expect("current snapshot")
            .expect("snapshot present");

        assert_eq!(current.meta.last_log_id, Some(log_id(1)));
        let decoded: ursula_control::ControlPlaneState =
            serde_json::from_slice(current.snapshot.into_inner().as_slice())
                .expect("decode current snapshot");
        assert!(decoded.nodes.contains_key(&7));
    }

    #[test]
    fn public_meta_types_are_exported_from_crate_root() {
        fn assert_types(
            _machine: crate::MetaRaftStateMachine,
            _builder: Option<crate::MetaRaftSnapshotBuilder>,
        ) {
        }

        assert_types(crate::MetaRaftStateMachine::default(), None);
        let _type_name = std::any::type_name::<crate::MetaRaftTypeConfig>();
    }
}
