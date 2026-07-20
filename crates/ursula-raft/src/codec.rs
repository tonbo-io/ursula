//! Serde wire codec for replicated commands, responses, forwarded reads, and
//! the openraft envelope types.
//!
//! The canonical types ([`GroupWriteCommand`], [`ursula_runtime::GroupWriteResponse`],
//! [`GroupEngineError`], the forwarded read responses) and openraft's own
//! serde-capable RPC/log types travel as self-describing MessagePack produced
//! directly by their serde derives — there are no hand-written per-field proto
//! mirrors. MessagePack (with named struct fields) is used instead of a
//! positional format because stream attrs embed `serde_json::Value`, which
//! requires a self-describing wire.

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
/// data (JSON attrs keep string keys), so serialization cannot fail.
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
        .map_err(|_| GroupEngineError::new(format!("{field}.core_id does not fit u16")))?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(shard_id),
        raft_group_id: RaftGroupId(raft_group_id),
    })
}

pub(crate) fn required<T>(value: Option<T>, field: &str) -> Result<T, GroupEngineError> {
    value.ok_or_else(|| GroupEngineError::Infra(GroupInfraError::proto_decode(field)))
}
