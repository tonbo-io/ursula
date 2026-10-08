//! Serde wire codec for replicated commands, responses, forwarded reads, and
//! the openraft envelope types.
//!
//! The canonical types ([`GroupWriteCommand`], [`ursula_runtime::GroupWriteResponse`],
//! [`GroupEngineError`], the forwarded read responses) and openraft's own
//! serde-capable RPC/log types travel as self-describing MessagePack produced
//! directly by their serde derives — there are no hand-written per-field proto
//! mirrors. MessagePack (with named struct fields) is used instead of a
//! positional format because the wire types rely on `serde(default)` and
//! `skip_serializing_if`, which require a self-describing wire.

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

/// Encodes a canonical value into its MessagePack wire form.
///
/// Infallible in practice: every wire type is a plain serde derive over owned
/// data, so serialization cannot fail.
pub(crate) fn encode_wire<T: Serialize>(value: &T) -> Bytes {
    rmp_serde::to_vec_named(value)
        .expect("wire value serializes to MessagePack")
        .into()
}

pub(crate) fn decode_wire<T: DeserializeOwned>(
    bytes: &[u8],
    what: &str,
) -> Result<T, GroupEngineError> {
    rmp_serde::from_slice(bytes)
        .map_err(|err| GroupEngineError::new(format!("decode wire {what}: {err}")))
}

pub(crate) fn placement_from_parts(
    core_id: u32,
    shard_id: u32,
    raft_group_id: u32,
    field: &str,
) -> Result<ShardPlacement, GroupEngineError> {
    let core_id = u16::try_from(core_id)
        .map_err(|_overflow| GroupEngineError::new(format!("{field}.core_id does not fit u16")))?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(shard_id),
        raft_group_id: RaftGroupId(raft_group_id),
    })
}

pub(crate) fn required<T>(value: Option<T>, field: &str) -> Result<T, GroupEngineError> {
    value.ok_or_else(|| GroupEngineError::Infra(GroupInfraError::proto_decode(field)))
}

#[cfg(test)]
mod tests {
    use openraft::type_config::alias::EntryOf;
    use openraft::type_config::alias::LogIdOf;
    use openraft::type_config::alias::SnapshotMetaOf;
    use openraft::type_config::alias::StoredMembershipOf;
    use openraft::type_config::alias::VoteOf;

    use super::decode_wire;
    use super::encode_wire;
    use crate::UrsulaRaftTypeConfig;

    #[test]
    fn epoch3_persisted_envelopes_are_stable() {
        // Named MessagePack (rmp_serde::to_vec_named) of the OpenRaft types
        // that format epoch 3 writes to disk and sends to peers: Vote, LogId,
        // a joint StoredMembership with a learner and node addresses, a
        // membership Entry, and SnapshotMeta (snapshot record and transfer).
        // The SnapshotMeta bytes are the map {last_log_id, last_membership,
        // snapshot_id} over the LogId and StoredMembership bytes, with a
        // snapshot id in the state machine's format. Decoding and re-encoding
        // must reproduce each value byte for byte.
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/persisted-envelopes-epoch3.json")).unwrap();
        fn check<T: serde::de::DeserializeOwned + serde::Serialize>(
            fixtures: &serde_json::Value,
            name: &str,
        ) -> T {
            let bytes: Vec<u8> = serde_json::from_value(fixtures[name].clone()).unwrap();
            let value: T = decode_wire(&bytes, name).unwrap();
            assert_eq!(encode_wire(&value).as_ref(), bytes.as_slice());
            value
        }
        let vote: VoteOf<UrsulaRaftTypeConfig> = check(&fixtures, "vote");
        assert_eq!(vote, openraft::Vote::new_committed(7, 2));
        let log: LogIdOf<UrsulaRaftTypeConfig> = check(&fixtures, "log_id");
        assert_eq!(log.index, 19);
        let membership: StoredMembershipOf<UrsulaRaftTypeConfig> = check(&fixtures, "membership");
        assert_eq!(membership.log_id(), &Some(log));
        assert_eq!(membership.membership().get_joint_config(), &vec![
            [1, 2].into_iter().collect(),
            [2, 3].into_iter().collect(),
        ]);
        assert_eq!(
            membership.membership().get_node(&4).unwrap().addr,
            "learner4"
        );
        let entry: EntryOf<UrsulaRaftTypeConfig> = check(&fixtures, "entry");
        assert_eq!(entry.log_id, log);
        assert!(
            matches!(entry.payload, openraft::EntryPayload::Membership(m) if &m == membership.membership())
        );
        let meta: SnapshotMetaOf<UrsulaRaftTypeConfig> = check(&fixtures, "snapshot_meta");
        assert_eq!(meta.last_log_id, Some(log));
        assert_eq!(meta.last_membership, membership);
        assert_eq!(meta.snapshot_id, "group-7-T7-N2-19");
    }
}
