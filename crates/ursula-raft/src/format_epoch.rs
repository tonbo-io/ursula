//! Format-epoch checks on the Raft gRPC plane.
//!
//! The Raft gRPC protocol version is the format epoch
//! (`ursula_stream::FORMAT_EPOCH`). Every inbound RPC checks it before the
//! group id and the payload, and Ursula 0.5.x and 0.6 do the same, so a
//! mismatch is answered `FAILED_PRECONDITION` with [`PROTOCOL_MISMATCH_TEXT`]
//! in the message. Two mechanisms keep a node of one epoch from serving next
//! to a node of another:
//!
//! - **Startup peer probe** ([`probe_peer_format_epoch`]): before a node
//!   writes anything, it sends each configured peer one `Vote` for a group no
//!   node registers (`u32::MAX`). `NOT_FOUND` means the peer speaks this
//!   protocol; `FAILED_PRECONDITION` means it does not.
//! - **Readiness** ([`FormatEpochMismatch`]): any later mismatch, inbound or
//!   outbound, is logged at error level and recorded; `/__ursula/ready` then
//!   answers 503 `format_epoch_mismatch` until the process restarts.
//!
//! The refusal between epochs is a contract with every earlier release:
//! nodes of 0.5.x through 0.8 classify a peer's answer only by its shape, so
//! a later epoch keeps these parts unchanged:
//!
//! - the `protocol_version` preamble fields (`RaftRpcEnvelopeV1` field 3,
//!   `GroupWriteRequestV1` field 5, `GroupReadRequestV1` field 11,
//!   `RejoinBarrierRequestV1` field 2, and field 3 of the snapshot and
//!   leader-transfer requests);
//! - the startup probe: a `Vote` for group `u32::MAX`, answered `NOT_FOUND`
//!   by a peer of the same epoch;
//! - the refusal: `FAILED_PRECONDITION` with a message that contains
//!   [`PROTOCOL_MISMATCH_TEXT`], for unary RPCs and as the code and message
//!   of an AppendStream item;
//! - the `ursula-cluster-id` and `ursula-node-id` request metadata.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tonic::transport::Endpoint;

use crate::peer_cluster::PeerIdentity;
use crate::peer_cluster::refusing_cluster;
use crate::raft_internal_proto;

/// Text every protocol-version refusal carries, in 0.5.x, 0.6 and this binary.
pub(crate) const PROTOCOL_MISMATCH_TEXT: &str = "raft grpc protocol mismatch";

/// The group id the startup probe names. No node registers it, so a peer on
/// the same protocol answers `NOT_FOUND` without touching any group.
pub(crate) const FORMAT_EPOCH_PROBE_GROUP: u32 = u32::MAX;

/// A recorded Raft protocol (format-epoch) mismatch. The process-wide
/// instance ([`FormatEpochMismatch::global`]) is what the gRPC plane records
/// into; a fresh instance lets a test drive readiness on its own.
#[derive(Debug, Clone, Default)]
pub struct FormatEpochMismatch(Arc<AtomicBool>);

impl FormatEpochMismatch {
    /// The instance the gRPC plane records into.
    pub fn global() -> Self {
        static GLOBAL: OnceLock<FormatEpochMismatch> = OnceLock::new();
        GLOBAL.get_or_init(Self::default).clone()
    }

    /// Whether a mismatch was seen since the process started.
    pub fn recorded(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// Record a mismatch: readiness answers 503 from now until restart.
    pub fn record(&self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Log a mismatch at error level and record it for readiness.
pub(crate) fn record_format_epoch_mismatch(direction: &'static str, message: &str) {
    tracing::error!(
        direction,
        message,
        "raft grpc protocol mismatch: format epochs differ (nodes of different format epochs \
         cannot run in one cluster); /__ursula/ready answers 503 format_epoch_mismatch until \
         restart"
    );
    FormatEpochMismatch::global().record();
}

/// Whether a peer's answer is a protocol-version refusal.
pub(crate) fn is_protocol_mismatch(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::FailedPrecondition
        && status.message().contains(PROTOCOL_MISMATCH_TEXT)
}

/// Record `status` when it is a protocol-version refusal from a peer.
pub(crate) fn observe_outbound_status(route: &str, status: &tonic::Status) {
    if is_protocol_mismatch(status) {
        record_format_epoch_mismatch("outbound", &format!("{route}: {}", status.message()));
    }
}

/// What the startup probe learned about one peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerFormatEpoch {
    /// The peer answered `NOT_FOUND` for the probe group: same protocol.
    Compatible,
    /// The peer refused this binary's protocol version (E8). Carries the
    /// peer's message.
    Refused(String),
    /// The peer refused this node for another cluster id, which it named
    /// (#581 section 6.5). Not a format-epoch answer: the node starts, and
    /// readiness reports the peer once Raft traffic reaches it.
    OtherCluster(ursula_headers::ClusterId),
    /// The peer could not be reached or answered something else. A fresh
    /// cluster starts its pods in parallel, so this is skipped; readiness
    /// covers a peer that turns out to differ later.
    Unreachable(String),
}

/// Probe one peer's Raft protocol version with a `Vote` for
/// `FORMAT_EPOCH_PROBE_GROUP`, sent as `identity`. Both 0.5.x and this binary
/// check the version before the group and the payload, so the probe has no
/// side effect.
pub async fn probe_peer_format_epoch(
    url: &str,
    identity: &PeerIdentity,
    timeout: Duration,
) -> PeerFormatEpoch {
    probe_peer_with_protocol(
        url,
        identity,
        crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
        timeout,
    )
    .await
}

pub(crate) async fn probe_peer_with_protocol(
    url: &str,
    identity: &PeerIdentity,
    protocol_version: u32,
    timeout: Duration,
) -> PeerFormatEpoch {
    let endpoint = match Endpoint::from_shared(url.to_owned()) {
        Ok(endpoint) => endpoint.connect_timeout(timeout).timeout(timeout),
        Err(err) => return PeerFormatEpoch::Unreachable(format!("invalid endpoint: {err}")),
    };
    let channel = match endpoint.connect().await {
        Ok(channel) => channel,
        Err(err) => return PeerFormatEpoch::Unreachable(format!("connect: {err}")),
    };
    let mut request = tonic::Request::new(raft_internal_proto::RaftRpcEnvelopeV1 {
        raft_group_id: FORMAT_EPOCH_PROBE_GROUP,
        node_id: 0,
        protocol_version,
        payload: Vec::new().into(),
    });
    request.set_timeout(timeout);
    let result = raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
        channel,
        identity.clone(),
    )
    .vote(request)
    .await;
    classify_probe_answer(result.map(|_| ()))
}

/// NOT_FOUND is a peer on this protocol; FAILED_PRECONDITION naming a
/// cluster id is a peer of another cluster; FAILED_PRECONDITION with the
/// mismatch text is E8; anything else is treated as unreachable.
pub(crate) fn classify_probe_answer(result: Result<(), tonic::Status>) -> PeerFormatEpoch {
    let status = match result {
        Err(status) => status,
        // No node registers the probe group; a success is not a known answer.
        Ok(()) => {
            return PeerFormatEpoch::Unreachable("unexpected success for the probe group".into());
        }
    };
    if status.code() == tonic::Code::NotFound {
        return PeerFormatEpoch::Compatible;
    }
    if let Some(cluster_id) = refusing_cluster(&status) {
        return PeerFormatEpoch::OtherCluster(cluster_id);
    }
    if is_protocol_mismatch(&status) {
        return PeerFormatEpoch::Refused(status.message().to_owned());
    }
    PeerFormatEpoch::Unreachable(status.to_string())
}

#[cfg(test)]
mod tests {
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::grpc::RAFT_GRPC_PROTOCOL_VERSION;
    use crate::peer_cluster::test_identity;
    use crate::raft_internal_proto::raft_internal_client::RaftInternalClient;
    use crate::registry::RaftGroupHandleRegistry;

    /// A node of the test cluster.
    async fn spawn_server() -> String {
        spawn_server_in(test_identity(1)).await
    }

    async fn spawn_server_in(identity: PeerIdentity) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind test raft grpc listener");
        let address = listener.local_addr().expect("listener address");
        let registry = RaftGroupHandleRegistry::default();
        registry
            .peer_cluster()
            .bind(identity, crate::PeerDirectory::default());
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(crate::grpc::raft_grpc_service(registry))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .expect("serve test raft grpc");
        });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn probe_treats_not_found_as_compatible_and_a_version_refusal_as_e8() {
        let url = spawn_server().await;
        let timeout = Duration::from_secs(2);
        let identity = test_identity(2);
        assert_eq!(
            probe_peer_format_epoch(&url, &identity, timeout).await,
            PeerFormatEpoch::Compatible
        );
        // A peer on another epoch answers the probe the way this server
        // answers a sender on another epoch.
        match probe_peer_with_protocol(&url, &identity, RAFT_GRPC_PROTOCOL_VERSION - 1, timeout)
            .await
        {
            PeerFormatEpoch::Refused(message) => {
                assert!(message.contains(PROTOCOL_MISMATCH_TEXT), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert!(FormatEpochMismatch::global().recorded());
        assert!(matches!(
            probe_peer_format_epoch("http://127.0.0.1:1", &identity, timeout).await,
            PeerFormatEpoch::Unreachable(_)
        ));
    }

    /// The cross-epoch refusal contract, pinned with literal text because
    /// released nodes match it: a peer of another epoch is refused with
    /// `FAILED_PRECONDITION` and "raft grpc protocol mismatch", and the
    /// answer a 0.8.2 node gives a node of this epoch is read as a refusal.
    #[test]
    fn the_refusal_between_epochs_keeps_its_released_shape() {
        let status = tonic::Status::from(
            crate::grpc::validate_grpc_metadata(4).expect_err("protocol 4 is refused"),
        );
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert!(
            status.message().starts_with("raft grpc protocol mismatch"),
            "{status:?}"
        );
        let from_0_8_2 = tonic::Status::failed_precondition(
            "raft grpc protocol mismatch: local=4, remote=5 (format epochs differ; a node joins \
             only a cluster of its own format epoch)",
        );
        assert!(matches!(
            classify_probe_answer(Err(from_0_8_2)),
            PeerFormatEpoch::Refused(_)
        ));
    }

    #[tokio::test]
    async fn probe_reports_a_peer_of_another_cluster_by_its_id() {
        let other = ursula_headers::ClusterId::new("other-cluster").unwrap();
        let url = spawn_server_in(PeerIdentity::new(1, other.clone())).await;
        assert_eq!(
            probe_peer_format_epoch(&url, &test_identity(2), Duration::from_secs(2)).await,
            PeerFormatEpoch::OtherCluster(other)
        );
    }

    #[tokio::test]
    async fn group_write_and_group_read_without_a_protocol_version_fail_precondition() {
        let url = spawn_server().await;
        let channel = tonic::transport::Endpoint::from_shared(url)
            .expect("endpoint")
            .connect()
            .await
            .expect("connect");
        let mut client = RaftInternalClient::with_interceptor(channel, test_identity(2));
        let write = client
            .group_write(raft_internal_proto::GroupWriteRequestV1 {
                raft_group_id: 0,
                core_id: 0,
                shard_id: 0,
                command_payloads: Vec::new(),
                protocol_version: 0,
            })
            .await
            .expect_err("a sender without protocol_version is refused");
        assert_eq!(write.code(), tonic::Code::FailedPrecondition);
        assert!(write.message().contains(PROTOCOL_MISMATCH_TEXT));
        let read = client
            .group_read(raft_internal_proto::GroupReadRequestV1 {
                raft_group_id: 0,
                core_id: 0,
                shard_id: 0,
                bucket_id: "fmt-epoch".to_owned(),
                stream_id: "probe".to_owned(),
                now_ms: 0,
                read: None,
                protocol_version: 0,
            })
            .await
            .expect_err("a sender without protocol_version is refused");
        assert_eq!(read.code(), tonic::Code::FailedPrecondition);
        assert!(read.message().contains(PROTOCOL_MISMATCH_TEXT));
    }
}
