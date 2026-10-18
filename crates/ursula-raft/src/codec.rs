//! Serde wire codec for replicated commands, responses, forwarded reads, and
//! the openraft envelope types.
//!
//! The canonical types ([`GroupWriteCommand`], [`ursula_runtime::GroupWriteResponse`],
//! [`GroupEngineError`], the forwarded read responses) and openraft's own
//! serde-capable RPC/log types travel as self-describing MessagePack produced
//! directly by their serde derives — there are no hand-written per-field proto
//! mirrors. MessagePack with named struct fields is self-describing, so a
//! decoder names the field or variant it cannot read. Within a minor the wire
//! form of `GroupEngineError` and of every type it carries is frozen, with
//! every field required: the `group-engine-errors-epoch<N>.json` fixture
//! shows any change.

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

    /// A forwarded write's refusal keeps its context, so a `Stream-Seq`
    /// conflict answered on the leader names the last seq after a forward
    /// as it does on the leader.
    #[test]
    fn a_forwarded_refusal_keeps_its_context() {
        let refusal = ursula_runtime::GroupEngineError::stream_with_context(
            ursula_runtime::StreamErrorCode::StreamSeqConflict,
            "stream sequence 'a' is not greater than last sequence 'b'",
            Some(3),
            vec![ursula_runtime::StreamErrorContext::LastStreamSeq {
                seq: "b".to_owned(),
            }],
        );
        let decoded: ursula_runtime::GroupEngineError =
            decode_wire(&encode_wire(&refusal), "group engine error").expect("decodes");
        assert_eq!(decoded, refusal);
    }

    #[test]
    fn epoch5_persisted_envelopes_are_stable() {
        // Named MessagePack (rmp_serde::to_vec_named) of the OpenRaft types
        // that format epoch 5 writes to disk and sends to peers: Vote, LogId,
        // a joint StoredMembership with a learner and node addresses, a
        // membership Entry, and SnapshotMeta (snapshot record and transfer).
        // The SnapshotMeta bytes are the map {last_log_id, last_membership,
        // snapshot_id} over the LogId and StoredMembership bytes, with a
        // snapshot id in the state machine's format. Decoding and re-encoding
        // must reproduce each value byte for byte.
        let fixtures: serde_json::Value =
            serde_json::from_str(include_str!("fixtures/persisted-envelopes-epoch5.json")).unwrap();
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
        // A history event (#581 section 4.3), the entry every group starts
        // with.
        let history: EntryOf<UrsulaRaftTypeConfig> = check(&fixtures, "new_history_entry");
        assert_eq!(history.log_id, log);
        assert!(matches!(
            history.payload,
            openraft::EntryPayload::Normal(ursula_runtime::GroupWriteCommand::Stream(
                ursula_stream::StreamCommand::NewHistory {
                    history,
                    now_ms: 1_700_000_000_000,
                    previous: 0,
                    kept: 18,
                    leader: ursula_stream::HistoryLeader {
                        term: 7,
                        node_id: 2,
                    },
                }
            )) if history.get() == 0x0123_4567_89ab_cdef
        ));
        // A recovery plan's history event (#613): it names the history it
        // replaced and the plan's point, up to which it kept that history.
        let recovery: EntryOf<UrsulaRaftTypeConfig> = check(&fixtures, "recovery_history_entry");
        assert_eq!(recovery.log_id, log);
        assert!(matches!(
            recovery.payload,
            openraft::EntryPayload::Normal(ursula_runtime::GroupWriteCommand::Stream(
                ursula_stream::StreamCommand::NewHistory {
                    history,
                    now_ms: 1_700_000_000_000,
                    previous: 0x0123_4567_89ab_cdef,
                    kept: 12,
                    leader: ursula_stream::HistoryLeader {
                        term: 7,
                        node_id: 2,
                    },
                }
            )) if history.get() == 0x0fed_cba9_8765_4321
        ));
        // An import is a history event keyed by its backup (#581 section
        // 4.4): the history id and the backup's digest are in the entry.
        let import: EntryOf<UrsulaRaftTypeConfig> = check(&fixtures, "import_snapshot_entry");
        assert_eq!(import.log_id, log);
        assert!(matches!(
            import.payload,
            openraft::EntryPayload::Normal(ursula_runtime::GroupWriteCommand::Stream(
                ursula_stream::StreamCommand::ImportSnapshot {
                    ref snapshot,
                    history,
                    backup_digest,
                    now_ms: 1_700_000_000_000,
                }
            )) if history.get() == 0x0123_4567_89ab_cdef
                && backup_digest == [0xab; 32]
                && snapshot.buckets == ["tenant"]
        ));
        // #669: the leader's background expiry sweep writes the
        // `TouchStreamAccess { renew_ttl: false }` a read of an expired
        // stream writes.
        let touch: EntryOf<UrsulaRaftTypeConfig> = check(&fixtures, "touch_stream_access_entry");
        assert_eq!(touch.log_id, log);
        assert!(matches!(
            touch.payload,
            openraft::EntryPayload::Normal(ursula_runtime::GroupWriteCommand::Stream(
                ursula_stream::StreamCommand::TouchStreamAccess {
                    ref stream_id,
                    now_ms: 1_700_000_000_000,
                    renew_ttl: false,
                }
            )) if stream_id == &ursula_shard::BucketStreamId::new("tenant", "expired")
        ));
        // S4 of the fifth #613 review: a history event inside an imported
        // snapshot, as a backup taken after a recovery carries it.
        let import: EntryOf<UrsulaRaftTypeConfig> =
            check(&fixtures, "import_snapshot_history_entry");
        assert!(matches!(
            import.payload,
            openraft::EntryPayload::Normal(ursula_runtime::GroupWriteCommand::Stream(
                ursula_stream::StreamCommand::ImportSnapshot { ref snapshot, .. }
            )) if snapshot.history == Some(ursula_stream::HistoryEvent {
                id: std::num::NonZeroU64::new(0x0fed_cba9_8765_4321).expect("non-zero"),
                event_index: 14,
                previous: 0x0123_4567_89ab_cdef,
                kept: 12,
                leader: ursula_stream::HistoryLeader { term: 7, node_id: 2 },
                imported: Some([0xcd; 32]),
            })
        ));
    }

    /// Within a minor, the wire form of `GroupEngineError` and of every type
    /// it carries is frozen: a follower decodes the error a leader of
    /// another patch release answers a forwarded `GroupRead` or `GroupWrite`
    /// with. One sample of every variant (`group_engine_error_samples`,
    /// built behind exhaustive matches) encodes to the fixture's bytes, and
    /// decoding and re-encoding them reproduces each byte for byte. A change
    /// to the fixture is a change to the protocol.
    #[test]
    fn group_engine_errors_epoch5_wire_form_is_stable() {
        let actual: std::collections::BTreeMap<String, Vec<u8>> =
            ursula_runtime::group_engine_error_samples()
                .iter()
                .map(|error| (format!("{error:?}"), encode_wire(error).to_vec()))
                .collect();
        for (name, bytes) in &actual {
            let decoded: ursula_runtime::GroupEngineError =
                decode_wire(bytes, name).expect("a sample decodes");
            assert_eq!(&encode_wire(&decoded)[..], bytes.as_slice(), "{name}");
            assert_eq!(&format!("{decoded:?}"), name);
        }
        let fixture: std::collections::BTreeMap<String, Vec<u8>> =
            serde_json::from_str(include_str!("fixtures/group-engine-errors-epoch5.json"))
                .expect("the fixture parses");
        assert!(
            fixture == actual,
            "the wire form of GroupEngineError differs from the fixture; the samples encode \
             as:\n{}",
            one_sample_per_line(&actual)
        );
    }

    /// The fixture's form: a JSON object with one sample per line.
    fn one_sample_per_line(samples: &std::collections::BTreeMap<String, Vec<u8>>) -> String {
        let lines: Vec<String> = samples
            .iter()
            .map(|(name, bytes)| {
                format!(
                    "{}: {}",
                    serde_json::to_string(name).expect("a name renders"),
                    serde_json::to_string(bytes).expect("bytes render")
                )
            })
            .collect();
        format!("{{\n{}\n}}\n", lines.join(",\n"))
    }

    /// An error decodes from its own bytes and re-encodes to them, whatever
    /// its free fields hold.
    mod group_engine_error_round_trip {
        use proptest::prelude::*;
        use ursula_runtime::GroupEngineError;
        use ursula_runtime::GroupInfraError;
        use ursula_shard::BucketStreamId;

        use super::decode_wire;
        use super::encode_wire;

        /// `error` with its free fields replaced by `text`, `number` and
        /// `maybe`.
        fn vary(
            error: GroupEngineError,
            text: String,
            number: u64,
            maybe: Option<u64>,
        ) -> GroupEngineError {
            match error {
                GroupEngineError::Stream(mut refusal) => {
                    refusal.text = text;
                    refusal.next_offset = maybe;
                    GroupEngineError::Stream(refusal)
                }
                GroupEngineError::NotLeader(mut refusal) => {
                    if let Some(leader) = refusal.leader.as_mut() {
                        leader.node_id = number;
                        leader.term = maybe.unwrap_or(number);
                        leader.address = Some(text);
                    }
                    GroupEngineError::NotLeader(refusal)
                }
                GroupEngineError::Infra(GroupInfraError::ColdBackpressure { .. }) => {
                    GroupEngineError::Infra(GroupInfraError::ColdBackpressure {
                        stream_id: BucketStreamId::new(text.clone(), text),
                        before_group_hot_bytes: number,
                        after_group_hot_bytes: maybe.unwrap_or(number),
                        limit: number,
                    })
                }
                GroupEngineError::Infra(GroupInfraError::LogBehindCommit {
                    raft_group_id, ..
                }) => GroupEngineError::Infra(GroupInfraError::LogBehindCommit {
                    raft_group_id,
                    committed: number,
                    held: maybe,
                }),
                GroupEngineError::Infra(GroupInfraError::Internal { .. }) => {
                    GroupEngineError::Infra(GroupInfraError::Internal { message: text })
                }
                other => other,
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            #[test]
            fn an_error_round_trips_byte_for_byte(
                index in any::<prop::sample::Index>(),
                text in ".{0,24}",
                number in any::<u64>(),
                maybe in any::<Option<u64>>(),
            ) {
                let samples = ursula_runtime::group_engine_error_samples();
                let error = vary(index.get(&samples).clone(), text, number, maybe);
                let bytes = encode_wire(&error);
                let decoded: GroupEngineError =
                    decode_wire(&bytes, "group engine error").expect("decodes");
                prop_assert_eq!(&decoded, &error);
                prop_assert_eq!(encode_wire(&decoded), bytes);
            }
        }
    }
}
