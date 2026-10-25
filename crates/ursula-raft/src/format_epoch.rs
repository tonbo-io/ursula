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
//!   writes anything, it sends each configured peer one `Vote` for a data
//!   group no node registers (`u32::MAX`), stamped for the peer's configured
//!   id. `NOT_FOUND` means the peer speaks this protocol;
//!   `FAILED_PRECONDITION` with the mismatch text means it does not, and
//!   with an `ursula-node-id` status metadata that the address serves
//!   another node.
//! - **Readiness** ([`FormatEpochMismatch`]): any later mismatch, inbound or
//!   outbound, is logged at error level and recorded; `/__ursula/ready` then
//!   answers 503 `format_epoch_mismatch` until the process restarts.
//!
//! The refusal between epochs is a contract with every earlier release:
//! nodes of 0.5.x through 0.10 classify a peer's answer only by its shape, so
//! a later epoch keeps these parts unchanged:
//!
//! - the `protocol_version` preamble fields (`RaftRpcEnvelopeV1` field 3,
//!   `GroupWriteRequestV1` field 5, `GroupReadRequestV1` field 11,
//!   `RejoinBarrierRequestV1` field 2, field 3 of the snapshot and
//!   leader-transfer requests, and from format epoch 6
//!   `GenesisProbeRequestV1` field 1);
//! - the startup probe: a `Vote` for group `u32::MAX`, answered `NOT_FOUND`
//!   by a peer of the same epoch;
//! - the refusal: `FAILED_PRECONDITION` with a message that contains
//!   [`PROTOCOL_MISMATCH_TEXT`], for unary RPCs and as the code and message
//!   of an AppendStream item. The text decides: a `FAILED_PRECONDITION`
//!   whose message contains it is a version refusal whatever metadata it
//!   carries, and every reader checks the text before any metadata, so no
//!   later refusal that also names a node or a cluster hides a mismatch;
//! - the `ursula-cluster-id` and `ursula-node-id` request metadata.
//!
//! Releases v0.1.0 through v0.2.0 are outside this contract. Later
//! releases reuse some of their field numbers with other types: fields 4
//! and 5 of `RaftRpcEnvelopeV1`, fields 4 and 5 of the snapshot and
//! leader-transfer requests, and field 7 of the snapshot request. Format
//! epoch 6 puts the plane in field 5 of the envelope and of the
//! leader-transfer request and in field 7 of the snapshot request. A peer
//! that old can fail to decode a request before it checks the version, and
//! answers a protobuf decode error instead of the refusal, which the
//! startup probe reads as unreachable.
//!
//! A refusal of a request meant for another node is `FAILED_PRECONDITION`
//! too, told apart by the refusing node's id in its `ursula-node-id` status
//! metadata ([`crate::target::refusing_node`]) and a message without the
//! mismatch text. An AppendStream item carries the id in
//! `RaftAppendStreamError.refusing_node`, and the sender turns it back into
//! that metadata. It is never a format-epoch answer: the probe reads it as
//! unreachable, and no reader records it as a mismatch.

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tonic::transport::Endpoint;

use crate::peer_cluster::PeerIdentity;
use crate::peer_cluster::refusing_cluster;
use crate::raft_internal_proto;
use crate::target::WireTarget;

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

/// Whether a peer's answer is a protocol-version refusal:
/// `FAILED_PRECONDITION` with [`PROTOCOL_MISMATCH_TEXT`] in its message,
/// whatever metadata it carries. The text is the frozen part of the
/// contract, so it is read before any metadata. A refusal of a request meant
/// for another node ([`crate::target::refusing_node`]) never is one: its
/// message does not contain the text.
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

/// Probe the Raft protocol version of peer `peer_id` at `url` with a
/// `Vote` for `FORMAT_EPOCH_PROBE_GROUP`, sent as `identity`. Both 0.5.x and
/// this binary check the version before the group and the payload, so the
/// probe has no side effect.
pub async fn probe_peer_format_epoch(
    url: &str,
    peer_id: u64,
    identity: &PeerIdentity,
    timeout: Duration,
) -> PeerFormatEpoch {
    probe_peer_with_protocol(
        url,
        peer_id,
        identity,
        crate::grpc::RAFT_GRPC_PROTOCOL_VERSION,
        timeout,
    )
    .await
}

pub(crate) async fn probe_peer_with_protocol(
    url: &str,
    peer_id: u64,
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
    let mut envelope = raft_internal_proto::RaftRpcEnvelopeV1 {
        protocol_version,
        ..Default::default()
    };
    ursula_shard::RaftTarget::Data(ursula_shard::RaftGroupId(FORMAT_EPOCH_PROBE_GROUP))
        .stamp(peer_id, &mut envelope);
    let mut request = tonic::Request::new(envelope);
    request.set_timeout(timeout);
    let result = raft_internal_proto::raft_internal_client::RaftInternalClient::with_interceptor(
        channel,
        identity.clone(),
    )
    .vote(request)
    .await;
    classify_probe_answer(peer_id, result.map(|_| ()))
}

/// NOT_FOUND is a peer on this protocol; FAILED_PRECONDITION with the
/// mismatch text is E8, whatever metadata it carries; otherwise
/// FAILED_PRECONDITION naming a cluster id is a peer of another cluster,
/// and naming a node an address that serves another node, unreachable for
/// the peer probed; anything else is treated as unreachable.
pub(crate) fn classify_probe_answer(
    peer_id: u64,
    result: Result<(), tonic::Status>,
) -> PeerFormatEpoch {
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
    if is_protocol_mismatch(&status) {
        return PeerFormatEpoch::Refused(status.message().to_owned());
    }
    if let Some(cluster_id) = refusing_cluster(&status) {
        return PeerFormatEpoch::OtherCluster(cluster_id);
    }
    if let Some(node) = crate::target::refusing_node(&status, peer_id) {
        return PeerFormatEpoch::Unreachable(format!("the address serves node {node}"));
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
            probe_peer_format_epoch(&url, 1, &identity, timeout).await,
            PeerFormatEpoch::Compatible
        );
        // A peer on another epoch answers the probe the way this server
        // answers a sender on another epoch.
        match probe_peer_with_protocol(&url, 1, &identity, RAFT_GRPC_PROTOCOL_VERSION - 1, timeout)
            .await
        {
            PeerFormatEpoch::Refused(message) => {
                assert!(message.contains(PROTOCOL_MISMATCH_TEXT), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert!(FormatEpochMismatch::global().recorded());
        assert!(matches!(
            probe_peer_format_epoch("http://127.0.0.1:1", 1, &identity, timeout).await,
            PeerFormatEpoch::Unreachable(_)
        ));
    }

    /// The cross-epoch refusal contract, pinned with literal text because
    /// released nodes match it: a peer of another epoch, 0.9's (protocol 5)
    /// or 0.10's (protocol 6), is refused with `FAILED_PRECONDITION` and
    /// "raft grpc protocol mismatch", and the answer a 0.9 or a 0.10 node
    /// gives a node of this epoch is read as a refusal.
    #[test]
    fn the_refusal_between_epochs_keeps_its_released_shape() {
        for protocol in [5, 6] {
            let status = tonic::Status::from(
                crate::grpc::validate_grpc_metadata(protocol)
                    .expect_err("a protocol of an earlier epoch is refused"),
            );
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(
                status.message().starts_with("raft grpc protocol mismatch"),
                "{status:?}"
            );
        }
        let from_0_9 = tonic::Status::failed_precondition(
            "raft grpc protocol mismatch: local=5, remote=7 (format epochs differ; a node joins \
             only a cluster of its own format epoch)",
        );
        let from_0_10 = tonic::Status::failed_precondition(
            "raft grpc protocol mismatch: local=6, remote=7 (format epochs differ; a node joins \
             only a cluster of its own format epoch)",
        );
        for answer in [from_0_9, from_0_10] {
            assert!(matches!(
                classify_probe_answer(2, Err(answer)),
                PeerFormatEpoch::Refused(_)
            ));
        }
    }

    /// The text decides, before any metadata: a version refusal that also
    /// names a node or a cluster in its status metadata, as a later epoch's
    /// refusal might, is still E8 to the probe, and no reader takes it as a
    /// misdelivery or a refusal of another cluster.
    #[test]
    fn a_version_refusal_that_names_a_node_or_a_cluster_is_still_refused() {
        let message = "raft grpc protocol mismatch: local=7, remote=6 (format epochs differ)";
        let mut naming_a_node = tonic::Status::failed_precondition(message);
        naming_a_node.metadata_mut().insert(
            crate::peer_cluster::NODE_ID_METADATA,
            tonic::metadata::AsciiMetadataValue::from(3_u64),
        );
        let mut naming_a_cluster = tonic::Status::failed_precondition(message);
        naming_a_cluster.metadata_mut().insert(
            crate::peer_cluster::CLUSTER_ID_METADATA,
            tonic::metadata::AsciiMetadataValue::from_static("other-cluster"),
        );
        for status in [naming_a_node, naming_a_cluster] {
            assert!(is_protocol_mismatch(&status), "{status:?}");
            assert_eq!(crate::target::refusing_node(&status, 2), None);
            assert_eq!(refusing_cluster(&status), None);
            assert_eq!(
                crate::grpc::stream_item_error(&status).refusing_node,
                0,
                "{status:?}"
            );
            match classify_probe_answer(2, Err(status)) {
                PeerFormatEpoch::Refused(refused) => assert_eq!(refused, message),
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
    }

    /// An address that serves another node than the one configured answers
    /// the probe with the misdelivery refusal: the probed peer is
    /// unreachable there, and the answer is no format-epoch answer.
    #[tokio::test]
    async fn probe_of_an_address_that_serves_another_node_is_unreachable() {
        let url = spawn_server().await;
        match probe_peer_format_epoch(&url, 3, &test_identity(2), Duration::from_secs(2)).await {
            PeerFormatEpoch::Unreachable(reason) => {
                assert!(reason.contains("serves node 1"), "{reason}");
            }
            other => panic!("expected an unreachable peer, got {other:?}"),
        }
    }

    /// The order of the checks: the protocol version before the target
    /// node, so a peer of another epoch learns of the mismatch even when it
    /// dialed an address that serves another node.
    #[tokio::test]
    async fn the_protocol_version_is_checked_before_the_target_node() {
        let url = spawn_server().await;
        match probe_peer_with_protocol(
            &url,
            3,
            &test_identity(2),
            RAFT_GRPC_PROTOCOL_VERSION - 1,
            Duration::from_secs(2),
        )
        .await
        {
            PeerFormatEpoch::Refused(message) => {
                assert!(message.contains(PROTOCOL_MISMATCH_TEXT), "{message}");
            }
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn probe_reports_a_peer_of_another_cluster_by_its_id() {
        let other = ursula_headers::ClusterId::new("other-cluster").unwrap();
        let url = spawn_server_in(PeerIdentity::new(1, other.clone())).await;
        assert_eq!(
            probe_peer_format_epoch(&url, 1, &test_identity(2), Duration::from_secs(2)).await,
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
                protocol_version: 0,
                ..Default::default()
            })
            .await
            .expect_err("a sender without protocol_version is refused");
        assert_eq!(write.code(), tonic::Code::FailedPrecondition);
        assert!(write.message().contains(PROTOCOL_MISMATCH_TEXT));
        let read = client
            .group_read(raft_internal_proto::GroupReadRequestV1 {
                bucket_id: "fmt-epoch".to_owned(),
                stream_id: "probe".to_owned(),
                protocol_version: 0,
                ..Default::default()
            })
            .await
            .expect_err("a sender without protocol_version is refused");
        assert_eq!(read.code(), tonic::Code::FailedPrecondition);
        assert!(read.message().contains(PROTOCOL_MISMATCH_TEXT));
    }
}
