//! Explicit rolling-upgrade compatibility for servers without the admin protocol.
//! No current-server operation uses this transport.
#[cfg(feature = "legacy-raft")]
pub(crate) use ursula_raft::confirm_quorum_prefix;
#[cfg(feature = "legacy-raft")]
pub(crate) use ursula_raft::request_self_election_via_transfer;

#[cfg(not(feature = "legacy-raft"))]
pub(crate) async fn confirm_quorum_prefix(
    _placement: ursula_shard::ShardPlacement,
    _leader: u64,
    _address: &str,
    _timeout: std::time::Duration,
) -> Result<ursula_proto::admin::QuorumPrefix, &'static str> {
    Err("legacy server requires a CLI built with the legacy-raft compatibility feature")
}
#[cfg(not(feature = "legacy-raft"))]
pub(crate) async fn request_self_election_via_transfer(
    _address: &str,
    _group: u32,
    _node: u64,
    _term: u64,
    _timeout: std::time::Duration,
) -> Result<(), &'static str> {
    Err("legacy server requires a CLI built with the legacy-raft compatibility feature")
}
