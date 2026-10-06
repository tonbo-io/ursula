//! Native-process joint-consensus crash acceptance. The proxy withholds final
//! uniform membership appends; it never constructs a successful Raft response.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use futures_util::StreamExt;
use openraft::EntryPayload;
use openraft::vote::RaftLeaderId;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;
use tonic::transport::Endpoint;
use ursula_raft::UrsulaAppendEntriesRequest;
use ursula_raft::raft_internal_proto as proto;
use ursula_shard::BucketStreamId;
use ursula_shard::RaftGroupId;
use ursula_shard::StaticShardMap;

use super::Cluster;

#[derive(Debug, Clone)]
struct Blocked {
    leader: u64,
    uniform_index: u64,
    committed_index: u64,
}

struct Armed {
    group: u32,
    target: BTreeSet<u64>,
    joint: Option<(u64, Vec<BTreeSet<u64>>)>,
    blocked: BTreeMap<u64, Blocked>,
}

#[derive(Default)]
pub(super) struct Gate {
    state: Mutex<Option<Armed>>,
    entered: Notify,
}

impl Gate {
    pub(super) fn arm(&self, group: u32, target: BTreeSet<u64>) {
        *self.state.lock().unwrap() = Some(Armed {
            group,
            target,
            joint: None,
            blocked: BTreeMap::new(),
        });
    }

    pub(super) fn resume(&self) {
        *self.state.lock().unwrap() = None;
    }

    fn reject(&self, envelope: &proto::RaftRpcEnvelopeV1) -> bool {
        let mut state = self.state.lock().unwrap();
        let Some(armed) = state
            .as_mut()
            .filter(|armed| armed.group == envelope.raft_group_id)
        else {
            return false;
        };
        let append: UrsulaAppendEntriesRequest = rmp_serde::from_slice(&envelope.payload).unwrap();
        for entry in &append.entries {
            let EntryPayload::Membership(membership) = &entry.payload else {
                continue;
            };
            let sets = membership.get_joint_config();
            if sets.len() == 2 && sets.last() == Some(&armed.target) {
                armed.joint = Some((entry.log_id.index(), sets.clone()));
            }
            if sets.as_slice() == [armed.target.clone()] {
                armed.blocked.insert(envelope.node_id, Blocked {
                    leader: *append.vote.leader_id().node_id(),
                    uniform_index: entry.log_id.index(),
                    committed_index: append
                        .leader_commit
                        .map(|log| log.index())
                        .unwrap_or_default(),
                });
                self.entered.notify_one();
                return true;
            }
        }
        false
    }

    async fn boundary(&self) -> (u64, Vec<BTreeSet<u64>>, Blocked) {
        tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let entered = self.entered.notified();
                {
                    let state = self.state.lock().unwrap();
                    let armed = state.as_ref().unwrap();
                    if let (Some((index, sets)), Some(blocked)) =
                        (&armed.joint, armed.blocked.values().next())
                    {
                        return (*index, sets.clone(), blocked.clone());
                    }
                }
                entered.await;
            }
        })
        .await
        .expect("final uniform membership was never withheld")
    }
}

#[derive(Clone)]
struct DataProxy {
    channel: Channel,
    gate: Arc<Gate>,
    snapshot_reply: Option<Arc<super::snapshot_reply::Gate>>,
}

#[derive(Clone)]
struct MetaProxy {
    channel: Channel,
}

pub(super) struct Proxy {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Proxy {
    pub(super) async fn start(address: &str, backend: &str, gate: Arc<Gate>) -> Self {
        Self::start_inner(address, backend, gate, None).await
    }

    pub(super) async fn start_snapshot(
        address: &str,
        backend: &str,
        snapshot_reply: Arc<super::snapshot_reply::Gate>,
    ) -> Self {
        Self::start_inner(
            address,
            backend,
            Arc::new(Gate::default()),
            Some(snapshot_reply),
        )
        .await
    }

    async fn start_inner(
        address: &str,
        backend: &str,
        gate: Arc<Gate>,
        snapshot_reply: Option<Arc<super::snapshot_reply::Gate>>,
    ) -> Self {
        let channel = Endpoint::from_shared(backend.to_owned())
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect_lazy();
        let data = proto::raft_internal_server::RaftInternalServer::new(DataProxy {
            channel: channel.clone(),
            gate,
            snapshot_reply,
        })
        .accept_compressed(CompressionEncoding::Zstd);
        let meta =
            proto::meta_raft_internal_server::MetaRaftInternalServer::new(MetaProxy { channel })
                .accept_compressed(CompressionEncoding::Zstd);
        let router = axum::Router::new()
            .route_service("/ursula.raft.v1.RaftInternal/{method}", data)
            .route_service("/ursula.raft.v1.MetaRaftInternal/{method}", meta)
            .fallback(http_forward)
            .with_state((reqwest::Client::new(), backend.to_owned()));
        let listener = tokio::net::TcpListener::bind(address.trim_start_matches("http://"))
            .await
            .unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { task }
    }
}

async fn http_forward(
    axum::extract::State((client, backend)): axum::extract::State<(reqwest::Client, String)>,
    request: axum::extract::Request,
) -> axum::response::Response {
    let (parts, body) = request.into_parts();
    let body = axum::body::to_bytes(body, 1024 * 1024).await.unwrap();
    match client
        .request(parts.method, format!("{backend}{}", parts.uri))
        .headers(parts.headers)
        .body(body)
        .timeout(Duration::from_secs(2))
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status();
            let headers = response.headers().clone();
            let mut output = axum::response::Response::new(axum::body::Body::from(
                response.bytes().await.unwrap(),
            ));
            *output.status_mut() = status;
            *output.headers_mut() = headers;
            output
        }
        Err(error) => axum::response::Response::builder()
            .status(502)
            .body(axum::body::Body::from(error.to_string()))
            .unwrap(),
    }
}

#[tonic::async_trait]
impl proto::raft_internal_server::RaftInternal for DataProxy {
    type AppendStreamStream = ReceiverStream<Result<proto::RaftAppendStreamResponse, Status>>;

    async fn append(
        &self,
        request: Request<proto::RaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        if self.gate.reject(request.get_ref()) {
            return Err(Status::unavailable("fault: final uniform append withheld"));
        }
        proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .append(request)
            .await
    }

    async fn append_stream(
        &self,
        request: Request<tonic::Streaming<proto::RaftAppendStreamRequest>>,
    ) -> Result<Response<Self::AppendStreamStream>, Status> {
        let mut input = request.into_inner();
        let (sender, receiver) = tokio::sync::mpsc::channel(16);
        let proxy = self.clone();
        tokio::spawn(async move {
            while let Some(frame) = input.next().await {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        break;
                    }
                };
                let mut items = Vec::new();
                for item in frame.items {
                    let result = match item.envelope {
                        Some(envelope) => proxy
                            .append(Request::new(envelope))
                            .await
                            .map(Response::into_inner),
                        None => Err(Status::invalid_argument("missing append envelope")),
                    };
                    let result = match result {
                        Ok(ack) => proto::raft_append_stream_response_item::Result::Ack(ack),
                        Err(error) => proto::raft_append_stream_response_item::Result::Error(
                            proto::RaftAppendStreamError {
                                code: error.code() as i32,
                                message: error.message().to_owned(),
                            },
                        ),
                    };
                    items.push(proto::RaftAppendStreamResponseItem {
                        request_id: item.request_id,
                        result: Some(result),
                    });
                }
                if sender
                    .send(Ok(proto::RaftAppendStreamResponse { items }))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
    async fn vote(
        &self,
        request: Request<proto::RaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .vote(request)
            .await
    }
    async fn full_snapshot(
        &self,
        request: Request<proto::RaftFullSnapshotRequestV1>,
    ) -> Result<Response<proto::RaftFullSnapshotAckV1>, Status> {
        let group = request.get_ref().raft_group_id;
        let metadata = request.get_ref().snapshot_meta.clone();
        let response = proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .full_snapshot(request)
            .await?;
        if let Some(gate) = &self.snapshot_reply {
            gate.hold_response(group, &metadata).await;
        }
        Ok(response)
    }
    async fn group_write(
        &self,
        request: Request<proto::GroupWriteRequestV1>,
    ) -> Result<Response<proto::GroupWriteResponseV1>, Status> {
        proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .group_write(request)
            .await
    }
    async fn group_read(
        &self,
        request: Request<proto::GroupReadRequestV1>,
    ) -> Result<Response<proto::GroupReadResponseV1>, Status> {
        proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .group_read(request)
            .await
    }
    async fn rejoin_barrier(
        &self,
        request: Request<proto::RejoinBarrierRequestV1>,
    ) -> Result<Response<proto::RejoinBarrierResponseV1>, Status> {
        proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .rejoin_barrier(request)
            .await
    }
    async fn transfer_leader(
        &self,
        request: Request<proto::RaftTransferLeaderRequestV1>,
    ) -> Result<Response<proto::RaftTransferLeaderAckV1>, Status> {
        proto::raft_internal_client::RaftInternalClient::new(self.channel.clone())
            .transfer_leader(request)
            .await
    }
}

#[tonic::async_trait]
impl proto::meta_raft_internal_server::MetaRaftInternal for MetaProxy {
    async fn append(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .append(request)
            .await
    }
    async fn vote(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .vote(request)
            .await
    }
    async fn full_snapshot(
        &self,
        request: Request<proto::MetaRaftSnapshotRequestV1>,
    ) -> Result<Response<proto::RaftFullSnapshotAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .full_snapshot(request)
            .await
    }
    async fn transfer_leader(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftTransferLeaderAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .transfer_leader(request)
            .await
    }
    async fn read_projection(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .read_projection(request)
            .await
    }
    async fn read_bootstrap_state(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .read_bootstrap_state(request)
            .await
    }
    async fn status(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .status(request)
            .await
    }
    async fn write_control(
        &self,
        request: Request<proto::MetaRaftRpcEnvelopeV1>,
    ) -> Result<Response<proto::RaftRpcAckV1>, Status> {
        proto::meta_raft_internal_client::MetaRaftInternalClient::new(self.channel.clone())
            .write_control(request)
            .await
    }
}

async fn applied_joint(cluster: &mut Cluster, group: u32, index: u64, blocked: &Blocked) {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        cluster.alive();
        let response: serde_json::Value = cluster
            .client
            .get(format!(
                "{}/__ursula/metrics",
                cluster.nodes[&blocked.leader].admin_url
            ))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let metrics = response["raft_groups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|metrics| metrics["raft_group_id"] == group)
            .unwrap();
        if metrics["committed_index"]
            .as_u64()
            .is_some_and(|value| value >= index)
            && metrics["last_applied_index"]
                .as_u64()
                .is_some_and(|value| value >= index)
        {
            assert!(
                metrics["committed_index"].as_u64().unwrap() < blocked.uniform_index,
                "withheld uniform configuration unexpectedly committed: {metrics}"
            );
            let view = cluster.view().await;
            assert_eq!(view.state.placements[&RaftGroupId(group)].epoch, 0);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "joint not applied before crash: {metrics}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn resume_from_outage(cluster: &mut Cluster, id: u64) {
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_ursulactl"))
        .args([
            "operation",
            "resume",
            "--operation",
            &id.to_string(),
            "--timeout-secs",
            "40",
            "--config",
        ])
        .arg(&cluster.manifest)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stderr = tokio::io::BufReader::new(child.stderr.take().unwrap());
    let mut warning = String::new();
    tokio::time::timeout(Duration::from_secs(10), stderr.read_line(&mut warning))
        .await
        .expect("CLI did not report the complete outage on stderr")
        .unwrap();
    assert!(
        warning.contains("operation status temporarily unavailable"),
        "{warning}"
    );
    let diagnostics = tokio::spawn(async move {
        let mut remaining = String::new();
        stderr.read_to_string(&mut remaining).await.unwrap();
        remaining
    });
    for node in 1..=6 {
        cluster.start(node, "joint-crash-restart");
    }
    let output = tokio::time::timeout(Duration::from_secs(50), child.wait_with_output())
        .await
        .expect("resume CLI deadline")
        .unwrap();
    let diagnostics = diagnostics.await.unwrap();
    assert!(
        output.status.success(),
        "{} {warning}{diagnostics}",
        String::from_utf8_lossy(&output.stdout),
    );
    let operation: serde_json::Value = serde_json::from_slice(&output.stdout)
        .expect("successful resume stdout must contain only JSON despite outage warnings");
    assert_eq!(operation["phase"], "Succeeded");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binaries_recover_rf3_rf5_committed_joint_after_full_process_crash() {
    let mut cluster = Cluster::new_with_transport(None, true).await;
    let gate = cluster.fault.clone().unwrap();
    let map = StaticShardMap::new(1, 2).unwrap();
    let mut payloads = Vec::new();
    for group in 0..2 {
        let name = (0..100)
            .map(|salt| format!("joint-fault-{group}-{salt}"))
            .find(|name| {
                map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(group)
            })
            .unwrap();
        let body = format!("acknowledged-before-joint-crash-{group}");
        cluster.write(&name, &body).await;
        payloads.push((name, body));
    }
    for (group, source, target) in [
        (0, BTreeSet::from([1, 2, 3]), BTreeSet::from([1, 2, 6])),
        (
            1,
            BTreeSet::from([1, 2, 3, 4, 5]),
            BTreeSet::from([1, 2, 4, 5, 6]),
        ),
    ] {
        gate.arm(group, target.clone());
        let voters = target
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let id = cluster
            .submit_group(group, &format!("joint-fault-{group}"), 0, &voters, None)
            .await;
        let (joint_index, sets, blocked) = gate.boundary().await;
        assert_eq!(sets, vec![source, target.clone()]);
        assert!(joint_index < blocked.uniform_index);
        assert!(blocked.committed_index >= joint_index);
        applied_joint(&mut cluster, group, joint_index, &blocked).await;
        let neighbor = 1 - group;
        let name = (0..100)
            .map(|salt| format!("joint-neighbor-{group}-{salt}"))
            .find(|name| {
                map.locate(&BucketStreamId::new("benchcmp", name.clone()))
                    .raft_group_id
                    == RaftGroupId(neighbor)
            })
            .unwrap();
        let body = format!("acknowledged-while-neighbor-joint-{group}");
        cluster.write(&name, &body).await;
        payloads.push((name, body));
        applied_joint(&mut cluster, group, joint_index, &blocked).await;
        let before = cluster.view().await;
        let managed = before.state.migrations[&id].managed.as_ref().unwrap();
        let generation = managed.executor.as_ref().unwrap().token.generation;
        assert!(managed.published_epoch.is_none());
        let old_process: serde_json::Value = cluster
            .client
            .get(format!(
                "{}/__ursula/control/receiver/process",
                cluster.nodes[&blocked.leader].admin_url
            ))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        // SIGKILL + wait via Process::drop; every data/meta process reopens the
        // same journals. No destructor or graceful shutdown can finish the step.
        cluster.processes.clear();
        gate.resume();
        resume_from_outage(&mut cluster, id).await;
        cluster.ready().await;
        cluster.configuration(group, target).await;
        let after = cluster.view().await;
        let managed = after.state.migrations[&id].managed.as_ref().unwrap();
        assert_eq!(managed.published_epoch, Some(1));
        assert!(managed.executor.as_ref().unwrap().token.generation > generation);
        let new_process: serde_json::Value = cluster
            .client
            .get(format!(
                "{}/__ursula/control/receiver/process",
                cluster.nodes[&blocked.leader].admin_url
            ))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_ne!(old_process["process"], new_process["process"]);
        for (name, body) in &payloads {
            cluster.payloads(name, body).await;
        }
    }
    assert_eq!(
        cluster.cli(&["verify-quorum"]).await["maintenance_eligible"],
        true
    );
    let before = cluster.view().await;
    cluster.processes.clear();
    for node in 1..=6 {
        cluster.start(node, "joint-settled-restart");
    }
    cluster.ready().await;
    assert_eq!(cluster.view().await.state, before.state);
    for (name, body) in &payloads {
        cluster.payloads(name, body).await;
    }
}
