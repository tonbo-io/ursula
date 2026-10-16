//! The startup check that this node's configured Raft `url` is the address
//! its groups' memberships record for it.
//!
//! A group records every member's address when it is first initialized, and
//! peers dial the recorded address from then on. A node whose `url` changed
//! afterwards, for example because `server.cluster_listen` moved the cluster
//! plane to another port, would serve Raft where no peer dials it.

use ursula_raft::RaftGroupHandleRegistry;
use ursula_shard::RaftGroupId;

/// This node's configured Raft `url` differs from the address a group's
/// membership records for it.
#[derive(Debug, thiserror::Error)]
#[error(
    "raft group {} records node {node_id} at {recorded}, but raft.peers configures {configured}: \
     a node's Raft url (and server.cluster_listen) cannot change after its groups are initialized",
    raft_group_id.0
)]
pub struct PeerAddressChanged {
    pub raft_group_id: RaftGroupId,
    pub node_id: u64,
    pub recorded: String,
    pub configured: String,
}

/// Refuses `configured` when a recorded address of `node_id` differs from it.
/// A trailing `/` does not count.
pub(crate) fn check_recorded_address(
    node_id: u64,
    configured: &str,
    recorded: impl IntoIterator<Item = (RaftGroupId, String)>,
) -> Result<(), PeerAddressChanged> {
    let wanted = configured.trim_end_matches('/');
    match recorded
        .into_iter()
        .find(|(_, address)| address.trim_end_matches('/') != wanted)
    {
        None => Ok(()),
        Some((raft_group_id, recorded)) => Err(PeerAddressChanged {
            raft_group_id,
            node_id,
            recorded,
            configured: configured.to_owned(),
        }),
    }
}

/// Reads every hosted group's recorded address of `node_id` from its Raft
/// core and checks it against `configured`.
pub(crate) async fn check_own_recorded_address(
    registry: &RaftGroupHandleRegistry,
    node_id: u64,
    configured: &str,
) -> Result<(), PeerAddressChanged> {
    check_recorded_address(
        node_id,
        configured,
        registry.recorded_node_addresses(node_id).await,
    )
}

#[cfg(test)]
mod tests {
    use ursula_shard::RaftGroupId;

    use super::PeerAddressChanged;
    use super::check_recorded_address;

    #[test]
    fn a_recorded_address_on_another_port_is_refused() {
        check_recorded_address(1, "http://node-1:4439", []).expect("an uninitialized node");
        check_recorded_address(1, "http://node-1:4439/", [
            (RaftGroupId(0), "http://node-1:4439".to_owned()),
            (RaftGroupId(1), "http://node-1:4439/".to_owned()),
        ])
        .expect("the same address");
        let err = check_recorded_address(1, "http://node-1:4439", [
            (RaftGroupId(0), "http://node-1:4439".to_owned()),
            (RaftGroupId(3), "http://node-1:4437".to_owned()),
        ])
        .expect_err("a moved cluster port");
        assert!(
            matches!(
                &err,
                PeerAddressChanged {
                    raft_group_id: RaftGroupId(3),
                    node_id: 1,
                    recorded,
                    configured,
                } if recorded == "http://node-1:4437" && configured == "http://node-1:4439"
            ),
            "{err}"
        );
    }
}
