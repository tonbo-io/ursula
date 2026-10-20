//! Serde wire codec for replicated commands, responses, forwarded reads, and
//! the openraft envelope types.
//!
//! The canonical types ([`crate::RaftCommand`],
//! [`ursula_runtime::GroupWriteResponse`], [`ursula_runtime::GroupEngineError`],
//! the forwarded read responses) and openraft's own serde-capable RPC and log
//! types travel as compact MessagePack ([`ursula_stream::msgpack`]) produced
//! directly by their serde derives. There are no hand-written per-field proto
//! mirrors. A struct is an array of its fields in declaration order and a
//! variant keeps its name, so field order is part of the wire form and every
//! field is required. Within a minor the wire form of `GroupEngineError` and
//! of every type it carries is frozen: the `group-engine-errors-epoch<N>.json`
//! fixture shows any change, and the persisted-envelopes and
//! commands-and-responses fixtures show a change to every other root.

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use ursula_shard::CoreId;
use ursula_shard::RaftGroupId;
use ursula_shard::ShardId;
use ursula_shard::ShardPlacement;

use crate::grpc::GrpcRpcError;

/// A wire value that does not decode. Each caller converts it once: the
/// Raft gRPC plane refuses the request as `INVALID_ARGUMENT`, a follower
/// answers a forwarded RPC's undecodable answer as
/// `ForwardedAnswerInvalid`, and an outbound RPC fails as a network error.
#[derive(Debug, thiserror::Error)]
#[error("decode wire {what}")]
pub(crate) struct WireDecodeError {
    pub(crate) what: &'static str,
    #[source]
    pub(crate) source: ursula_stream::msgpack::DecodeError,
}

/// Encodes a canonical value into its MessagePack wire form.
///
/// Infallible in practice: every wire type is a plain serde derive over owned
/// data, so serialization cannot fail.
pub(crate) fn encode_wire<T: Serialize>(value: &T) -> Bytes {
    ursula_stream::msgpack::encode(value)
        .expect("wire value serializes to MessagePack")
        .into()
}

pub(crate) fn decode_wire<T: DeserializeOwned>(
    bytes: &[u8],
    what: &'static str,
) -> Result<T, WireDecodeError> {
    ursula_stream::msgpack::decode(bytes).map_err(|source| WireDecodeError { what, source })
}

/// The placement a Raft gRPC request names, refused as `INVALID_ARGUMENT`
/// when its core id does not fit.
pub(crate) fn placement_from_parts(
    core_id: u32,
    shard_id: u32,
    raft_group_id: u32,
    field: &str,
) -> Result<ShardPlacement, GrpcRpcError> {
    let core_id = u16::try_from(core_id).map_err(|_overflow| {
        GrpcRpcError::invalid_argument(format!("{field}.core_id does not fit u16"))
    })?;
    Ok(ShardPlacement {
        core_id: CoreId(core_id),
        shard_id: ShardId(shard_id),
        raft_group_id: RaftGroupId(raft_group_id),
    })
}

/// A field a Raft gRPC request must carry, refused as `INVALID_ARGUMENT`
/// when it is missing.
pub(crate) fn required<T>(value: Option<T>, field: &str) -> Result<T, GrpcRpcError> {
    value.ok_or_else(|| GrpcRpcError::invalid_argument(format!("missing {field}")))
}

#[cfg(test)]
mod tests {
    use openraft::type_config::alias::EntryOf;
    use openraft::type_config::alias::LogIdOf;
    use openraft::type_config::alias::SnapshotMetaOf;
    use openraft::type_config::alias::StoredMembershipOf;
    use openraft::type_config::alias::VoteOf;
    use openraft::vote::RaftLeaderId;

    use super::decode_wire;
    use super::encode_wire;
    use crate::RaftCommand;
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

    type LeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;
    type Entry = EntryOf<UrsulaRaftTypeConfig>;
    type TransferLeaderError = openraft::raft::TransferLeaderError<UrsulaRaftTypeConfig>;

    /// `[(name, value), ...]` for one named value per listed pattern, which
    /// must cover every variant of `$ty`: the patterns form a match with no
    /// wildcard, and each value must match its own pattern. A new variant
    /// does not compile until it has a sample.
    macro_rules! every {
        ($ty:ty { $($pattern:pat => $name:literal: $value:expr),+ $(,)? }) => {{
            let samples: Vec<(&'static str, $ty)> = vec![$({
                let value: $ty = $value;
                assert!(matches!(&value, $pattern), "{} is a sample of another variant", $name);
                ($name, value)
            }),+];
            for (_, value) in &samples {
                match value {
                    $($pattern => {})+
                }
            }
            samples
        }};
    }

    /// What the persisted-envelopes fixture says produced it.
    const ENVELOPES_PRODUCER: &str =
        "ursula_stream::msgpack, format epoch 6, openraft 0.10.0-alpha.37";

    fn log_id(term: u64, node_id: u64, index: u64) -> LogIdOf<UrsulaRaftTypeConfig> {
        openraft::LogId {
            leader_id: LeaderId::new(term, node_id),
            index,
        }
    }

    /// The log id the envelope samples name.
    fn log() -> LogIdOf<UrsulaRaftTypeConfig> {
        log_id(7, 2, 19)
    }

    /// The vote the envelope samples name.
    fn vote() -> VoteOf<UrsulaRaftTypeConfig> {
        openraft::Vote::new_committed(7, 2)
    }

    /// A joint membership with a learner, each node an empty map.
    fn membership() -> StoredMembershipOf<UrsulaRaftTypeConfig> {
        StoredMembershipOf::<UrsulaRaftTypeConfig>::new(
            Some(log()),
            openraft::Membership::new_with_defaults(
                vec![[1, 2].into_iter().collect(), [2, 3].into_iter().collect()],
                [4],
            ),
        )
    }

    fn normal(index: u64, command: ursula_stream::StreamCommand) -> Entry {
        Entry {
            log_id: log_id(7, 2, index),
            payload: openraft::EntryPayload::Normal(RaftCommand::Stream(command)),
        }
    }

    fn import_entry(history: Option<ursula_stream::HistoryEvent>) -> Entry {
        normal(19, ursula_stream::StreamCommand::ImportSnapshot {
            snapshot: Box::new(ursula_stream::StreamSnapshot {
                format_version: ursula_stream::BACKUP_FORMAT_VERSION,
                buckets: vec!["tenant".to_owned()],
                erased_buckets: Vec::new(),
                streams: Vec::new(),
                pending_cold_gc: Vec::new(),
                next_cold_gc_seq: 1,
                bucket_usage: Vec::new(),
                last_created_at_ms: 2,
                history,
            }),
            history: std::num::NonZeroU64::new(0x0123_4567_89ab_cdef).expect("non-zero"),
            backup_digest: [0xab; 32],
            now_ms: 1_700_000_000_000,
        })
    }

    /// The pointer of an S3 snapshot, as `group-N.snapshot.json` and a
    /// FullSnapshot carry it (#687): it names an object of one node's own
    /// directory under the snapshot's own id.
    fn s3_pointer() -> ursula_runtime::SnapshotPointer {
        ursula_runtime::SnapshotPointer {
            snapshot_id: crate::state_machine::group_snapshot_id(
                ursula_shard::RaftGroupId(0),
                Some(log()),
            ),
            location: ursula_runtime::SnapshotLocation::S3 {
                key: "snapshots/node-2/group-0/00000000000000000019-0123456789abcdef.snap"
                    .to_owned(),
                size_bytes: 4096,
                stored_size_bytes: 1234,
                compression: ursula_runtime::SnapshotCompression::Zstd,
            },
        }
    }

    /// The MessagePack of `value`, checked to decode to an equal value and
    /// to re-encode to the same bytes.
    fn wire_bytes<T>(name: &str, value: &T) -> Vec<u8>
    where T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug {
        let bytes = encode_wire(value).to_vec();
        let decoded: T = decode_wire(&bytes, "sample")
            .unwrap_or_else(|error| panic!("{name} does not decode: {error}"));
        assert_eq!(format!("{decoded:?}"), format!("{value:?}"), "{name}");
        assert_eq!(encode_wire(&decoded).as_ref(), bytes.as_slice(), "{name}");
        bytes
    }

    fn add<T>(samples: &mut std::collections::BTreeMap<String, Vec<u8>>, name: &str, value: &T)
    where T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug {
        let bytes = wire_bytes(name, value);
        assert!(samples.insert(name.to_owned(), bytes).is_none(), "{name}");
    }

    /// [`add`] under the sample's `Debug` form.
    fn add_by_debug<T>(samples: &mut std::collections::BTreeMap<String, Vec<u8>>, value: &T)
    where T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug {
        add(samples, &format!("{value:?}"), value);
    }

    /// Every root that format epoch 6 writes to disk or sends to a peer
    /// apart from the replicated commands and their answers: the OpenRaft
    /// envelopes (votes, log ids, memberships, entries, snapshot metadata
    /// and every RPC message), each record of the WAL journal, the WAL's
    /// state files (core metadata, run state and topology, each as a whole
    /// file), the snapshot record, the snapshot pointers and a full backup
    /// snapshot.
    fn persisted_envelopes() -> std::collections::BTreeMap<String, Vec<u8>> {
        use ursula_stream::StreamCommand;

        use crate::log_store::CoreJournalRecord;
        use crate::log_store::RaftGroupLogRecord;

        let mut envelopes = std::collections::BTreeMap::new();
        let samples = &mut envelopes;
        add(samples, "vote", &vote());
        add(samples, "log_id", &log());
        add(samples, "membership", &membership());
        add(samples, "entry", &Entry {
            log_id: log(),
            payload: openraft::EntryPayload::Membership(membership().membership().clone()),
        });
        add(samples, "blank_entry", &Entry {
            log_id: log_id(7, 2, 20),
            payload: openraft::EntryPayload::Blank,
        });
        // The map {last_log_id, last_membership, snapshot_id}: OpenRaft
        // writes snapshot_id empty and ignores it on read. Ursula names a
        // snapshot by its group and last log id instead.
        add(samples, "snapshot_meta", &SnapshotMetaOf::<
            UrsulaRaftTypeConfig,
        > {
            last_log_id: Some(log()),
            last_membership: membership(),
        });
        // A history event (#581 section 4.3), the entry every group starts
        // with.
        add(
            samples,
            "new_history_entry",
            &normal(19, StreamCommand::NewHistory {
                history: std::num::NonZeroU64::new(0x0123_4567_89ab_cdef).expect("non-zero"),
                now_ms: 1_700_000_000_000,
                previous: 0,
                kept: 18,
                leader: ursula_stream::HistoryLeader {
                    term: 7,
                    node_id: 2,
                },
            }),
        );
        // A recovery plan's history event (#613): it names the history it
        // replaced and the plan's point, up to which it kept that history.
        add(
            samples,
            "recovery_history_entry",
            &normal(19, StreamCommand::NewHistory {
                history: std::num::NonZeroU64::new(0x0fed_cba9_8765_4321).expect("non-zero"),
                now_ms: 1_700_000_000_000,
                previous: 0x0123_4567_89ab_cdef,
                kept: 12,
                leader: ursula_stream::HistoryLeader {
                    term: 7,
                    node_id: 2,
                },
            }),
        );
        // An import is a history event keyed by its backup (#581 section
        // 4.4): the history id and the backup's digest are in the entry.
        add(samples, "import_snapshot_entry", &import_entry(None));
        // S4 of the fifth #613 review: a history event inside an imported
        // snapshot, as a backup taken after a recovery carries it.
        add(
            samples,
            "import_snapshot_history_entry",
            &import_entry(Some(ursula_stream::HistoryEvent {
                id: std::num::NonZeroU64::new(0x0fed_cba9_8765_4321).expect("non-zero"),
                event_index: 14,
                previous: 0x0123_4567_89ab_cdef,
                kept: 12,
                leader: ursula_stream::HistoryLeader {
                    term: 7,
                    node_id: 2,
                },
                imported: Some([0xcd; 32]),
            })),
        );
        // A read of an expired stream writes `TouchStreamAccess`, whose
        // apply removes that stream on every replica.
        add(
            samples,
            "touch_stream_access_entry",
            &normal(19, StreamCommand::TouchStreamAccess {
                stream_id: ursula_shard::BucketStreamId::new("tenant", "expired"),
                now_ms: 1_700_000_000_000,
                renew_ttl: false,
            }),
        );

        // One pointer of every location.
        let pointers = every!(ursula_runtime::SnapshotPointer {
            ursula_runtime::SnapshotPointer {
                location: ursula_runtime::SnapshotLocation::Inline { .. },
                ..
            } => "snapshot_pointer_inline": ursula_runtime::SnapshotPointer {
                snapshot_id: crate::state_machine::group_snapshot_id(
                    ursula_shard::RaftGroupId(3),
                    Some(log_id(6, 1, 17)),
                ),
                location: ursula_runtime::SnapshotLocation::Inline {
                    bytes: vec![0x92, 0x05, 0x09],
                },
            },
            ursula_runtime::SnapshotPointer {
                location: ursula_runtime::SnapshotLocation::Local { .. },
                ..
            } => "snapshot_pointer_local": ursula_runtime::SnapshotPointer {
                snapshot_id: crate::state_machine::group_snapshot_id(
                    ursula_shard::RaftGroupId(4),
                    None,
                ),
                location: ursula_runtime::SnapshotLocation::Local {
                    path: "/var/lib/ursula/snapshots/group-4.snap".into(),
                    size_bytes: 512,
                },
            },
            ursula_runtime::SnapshotPointer {
                location: ursula_runtime::SnapshotLocation::S3 { .. },
                ..
            } => "snapshot_pointer": s3_pointer(),
        });
        for (name, pointer) in &pointers {
            add(samples, name, pointer);
        }

        // One record of every kind in the journal of core 1.
        let records = every!(RaftGroupLogRecord {
            RaftGroupLogRecord::SaveCommitted(_) => "journal_save_committed":
                RaftGroupLogRecord::SaveCommitted(Some(log_id(7, 2, 18))),
            RaftGroupLogRecord::Append(_) => "journal_append": RaftGroupLogRecord::Append(vec![
                normal(19, StreamCommand::Append {
                    stream_id: ursula_shard::BucketStreamId::new("tenant", "orders"),
                    content_type: Some("text/plain".to_owned()),
                    payload: bytes::Bytes::from_static(b"hello"),
                    close_after: false,
                    stream_seq: Some("seq-1".to_owned()),
                    producer: None,
                    now_ms: 1_700_000_000_000,
                }),
                Entry {
                    log_id: log_id(7, 2, 20),
                    payload: openraft::EntryPayload::Blank,
                },
            ]),
            RaftGroupLogRecord::FrozenAppend(_) => "journal_frozen_append":
                RaftGroupLogRecord::FrozenAppend(Box::new(crate::log_store::FrozenAppend {
                    archive: crate::log_store::ArchiveRef {
                        id: crate::log_store::ArchiveId {
                            group_id: 7,
                            segment: 3,
                            offset: 4096,
                            first_index: 17,
                            last_index: 19,
                            content_hash: [0xe1; 32],
                        },
                        bytes: 8192,
                        checksum: 0xdead_beef,
                    },
                    log_ids: vec![log_id(7, 2, 17), log_id(7, 2, 19)],
                })),
            RaftGroupLogRecord::TruncateAfter(_) => "journal_truncate_after":
                RaftGroupLogRecord::TruncateAfter(Some(log_id(6, 3, 16))),
            RaftGroupLogRecord::Purge(_) => "journal_purge":
                RaftGroupLogRecord::Purge(log_id(5, 4, 15)),
        });
        for (name, record) in records {
            add(samples, name, &CoreJournalRecord {
                group_id: 7,
                generation: crate::log_store::LogGeneration::new(3),
                record,
            });
        }

        // The WAL's state files, whole: header, payload and checksum.
        for (name, bytes) in crate::log_store::state_file_samples() {
            assert!(samples.insert(name.to_owned(), bytes).is_none(), "{name}");
        }
        // The snapshot record (`group-N.snapshot.json`).
        assert!(
            samples
                .insert(
                    "snapshot_record".to_owned(),
                    crate::state_machine::snapshot_record_sample(),
                )
                .is_none()
        );

        // The Raft RPC messages.
        add(
            samples,
            "append_entries_request",
            &crate::types::UrsulaAppendEntriesRequest {
                vote: vote(),
                prev_log_id: Some(log_id(6, 3, 18)),
                entries: vec![normal(19, StreamCommand::DeleteStream {
                    stream_id: ursula_shard::BucketStreamId::new("tenant", "orders"),
                    now_ms: 1_700_000_000_000,
                })],
                leader_commit: Some(log_id(7, 2, 17)),
            },
        );
        let responses = every!(crate::types::UrsulaAppendEntriesResponse {
            openraft::raft::AppendEntriesResponse::Success => "append_entries_success":
                openraft::raft::AppendEntriesResponse::Success,
            openraft::raft::AppendEntriesResponse::PartialSuccess(_) =>
                "append_entries_partial_success":
                openraft::raft::AppendEntriesResponse::PartialSuccess(Some(log())),
            openraft::raft::AppendEntriesResponse::Conflict => "append_entries_conflict":
                openraft::raft::AppendEntriesResponse::Conflict,
            openraft::raft::AppendEntriesResponse::HigherVote(_) => "append_entries_higher_vote":
                openraft::raft::AppendEntriesResponse::HigherVote(openraft::Vote::new_committed(
                    9, 4,
                )),
        });
        for (name, response) in &responses {
            add(samples, name, response);
        }
        add(samples, "vote_request", &crate::types::UrsulaVoteRequest {
            vote: openraft::Vote::new(8, 3),
            last_log_id: Some(log()),
            leadership_transfer: true,
        });
        add(
            samples,
            "vote_response",
            &crate::types::UrsulaVoteResponse {
                vote: openraft::Vote::new_committed(8, 3),
                vote_granted: true,
                last_log_id: Some(log()),
            },
        );
        add(
            samples,
            "snapshot_response",
            &openraft::raft::SnapshotResponse::<UrsulaRaftTypeConfig>::new(vote()),
        );
        add(
            samples,
            "transfer_leader_request",
            &openraft::raft::TransferLeaderRequest::<UrsulaRaftTypeConfig>::new(
                vote(),
                3,
                Some(log()),
            ),
        );
        let responses = every!(openraft::raft::TransferLeaderResponse<UrsulaRaftTypeConfig> {
            Ok(()) => "transfer_leader_ok": Ok(()),
            Err(TransferLeaderError::VoteChanged { .. }) => "transfer_leader_vote_changed":
                Err(TransferLeaderError::VoteChanged {
                    expected: vote(),
                    actual: openraft::Vote::new_committed(8, 3),
                }),
            Err(TransferLeaderError::LogNotFlushed { .. }) => "transfer_leader_log_not_flushed":
                Err(TransferLeaderError::LogNotFlushed {
                    expected: Some(log()),
                    actual: Some(log_id(6, 3, 16)),
                }),
        });
        for (name, response) in &responses {
            add(samples, name, response);
        }

        // A backup snapshot with a value in every field.
        add(
            samples,
            "stream_snapshot",
            &ursula_runtime::wire_samples::stream_snapshot(),
        );
        envelopes
    }

    /// Every root that format epoch 6 writes to disk or sends to a peer
    /// apart from the commands and their answers ([`persisted_envelopes`])
    /// encodes to the fixture's bytes, and decoding and re-encoding them
    /// reproduces each value and each byte. Each sample sets every field,
    /// and fields of one type hold distinct values, so a change of a field's
    /// name or place changes the fixture. A change to the fixture is a
    /// change to the format.
    #[test]
    fn epoch6_persisted_envelopes_are_stable() {
        let mut actual: std::collections::BTreeMap<String, serde_json::Value> =
            persisted_envelopes()
                .into_iter()
                .map(|(name, bytes)| (name, serde_json::Value::from(bytes)))
                .collect();
        actual.insert("producer".to_owned(), ENVELOPES_PRODUCER.into());
        let fixture: std::collections::BTreeMap<String, serde_json::Value> =
            serde_json::from_str(include_str!("fixtures/persisted-envelopes-epoch6.json"))
                .expect("the fixture parses");
        assert!(
            fixture == actual,
            "the persisted envelopes differ from the fixture; the samples encode as:\n{}",
            one_entry_per_line(&actual)
        );

        // The S3 pointer names an object of node 2's own directory (#687).
        let pointer = s3_pointer();
        let key = crate::state_machine::object_key(&pointer.location).expect("an object");
        let owner = ursula_runtime::OwnedKey::parse(
            &ursula_runtime::SnapshotPrefix::try_from("snapshots").expect("a prefix"),
            &key,
        )
        .expect("an owned key");
        assert_eq!(
            (
                owner.owner().node_id(),
                owner.owner().raft_group_id(),
                owner.index()
            ),
            (2, ursula_shard::RaftGroupId(0), 19)
        );
    }

    /// Every replicated command, in the [`RaftCommand`]
    /// a Raft entry and a forwarded `GroupWrite` carry, every write answer
    /// and the forwarded read answers, keyed by their `Debug` form.
    fn commands_and_responses() -> std::collections::BTreeMap<String, Vec<u8>> {
        use ursula_runtime::wire_samples;

        let mut samples = std::collections::BTreeMap::new();
        for command in wire_samples::group_write_commands() {
            add_by_debug(&mut samples, &command);
        }
        for response in wire_samples::group_write_responses() {
            add_by_debug(&mut samples, &response);
        }
        for head in wire_samples::head_stream_responses() {
            add_by_debug(&mut samples, &head);
        }
        for read in wire_samples::read_stream_responses() {
            add_by_debug(&mut samples, &read);
        }
        samples
    }

    /// The commands every replica applies from its log and the answers a
    /// leader sends a follower encode to the fixture's bytes. One sample of
    /// every variant (`ursula_runtime::wire_samples`, built behind
    /// exhaustive matches) sets every field, and fields of one type hold
    /// distinct values, so a change of a field's name or place changes the
    /// fixture. A change to the fixture is a change to the format.
    #[test]
    fn commands_and_responses_epoch6_wire_form_is_stable() {
        let actual = commands_and_responses();
        let fixture: std::collections::BTreeMap<String, Vec<u8>> =
            serde_json::from_str(include_str!("fixtures/commands-and-responses-epoch6.json"))
                .expect("the fixture parses");
        assert!(
            fixture == actual,
            "the wire form of the commands and responses differs from the fixture; the \
             samples encode as:\n{}",
            one_entry_per_line(&actual)
        );
    }

    /// Within a minor, the wire form of `GroupEngineError` and of every type
    /// it carries is frozen: a follower decodes the error a leader of
    /// another patch release answers a forwarded `GroupRead` or `GroupWrite`
    /// with. One sample of every variant (`group_engine_error_samples`,
    /// built behind exhaustive matches) encodes to the fixture's bytes, and
    /// decoding and re-encoding them reproduces each byte for byte. A change
    /// to the fixture is a change to the protocol.
    #[test]
    fn group_engine_errors_epoch6_wire_form_is_stable() {
        let actual: std::collections::BTreeMap<String, Vec<u8>> =
            ursula_runtime::group_engine_error_samples()
                .iter()
                .map(|error| (format!("{error:?}"), encode_wire(error).to_vec()))
                .collect();
        for (name, bytes) in &actual {
            let decoded: ursula_runtime::GroupEngineError =
                decode_wire(bytes, "sample").expect("a sample decodes");
            assert_eq!(&encode_wire(&decoded)[..], bytes.as_slice(), "{name}");
            assert_eq!(&format!("{decoded:?}"), name);
        }
        let fixture: std::collections::BTreeMap<String, Vec<u8>> =
            serde_json::from_str(include_str!("fixtures/group-engine-errors-epoch6.json"))
                .expect("the fixture parses");
        assert!(
            fixture == actual,
            "the wire form of GroupEngineError differs from the fixture; the samples encode \
             as:\n{}",
            one_entry_per_line(&actual)
        );
    }

    /// The form of a fixture: a JSON object with one entry per line.
    fn one_entry_per_line<T: serde::Serialize>(
        entries: &std::collections::BTreeMap<String, T>,
    ) -> String {
        let lines: Vec<String> = entries
            .iter()
            .map(|(name, value)| {
                format!(
                    "{}: {}",
                    serde_json::to_string(name).expect("a name renders"),
                    serde_json::to_string(value).expect("a value renders")
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

    /// Every sample decodes from its own bytes and re-encodes to them with
    /// any of its `Option`s absent, inside the envelopes that carry it.
    mod sample_round_trips {
        use proptest::prelude::*;
        use ursula_runtime::wire_samples;

        use super::Entry;
        use super::log_id;
        use super::wire_bytes;
        use crate::RaftCommand;
        use crate::log_store::CoreJournalRecord;
        use crate::log_store::RaftGroupLogRecord;

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            #[test]
            fn a_command_round_trips_in_an_entry_and_a_journal_record(
                index in any::<prop::sample::Index>(),
                mask in any::<u64>(),
                (term, node_id, log_index) in (any::<u64>(), any::<u64>(), any::<u64>()),
                (group_id, generation) in (any::<u32>(), any::<u64>()),
            ) {
                let commands = wire_samples::stream_commands();
                let command = wire_samples::vary_command(index.get(&commands).clone(), mask);
                let entry = Entry {
                    log_id: log_id(term, node_id, log_index),
                    payload: openraft::EntryPayload::Normal(RaftCommand::Stream(command)),
                };
                wire_bytes("entry", &entry);
                wire_bytes("journal record", &CoreJournalRecord {
                    group_id,
                    generation: crate::log_store::LogGeneration::new(generation),
                    record: RaftGroupLogRecord::Append(vec![entry]),
                });
            }

            #[test]
            fn an_answer_round_trips(
                index in any::<prop::sample::Index>(),
                mask in any::<u64>(),
            ) {
                let responses = wire_samples::group_write_responses();
                wire_bytes(
                    "write answer",
                    &wire_samples::vary_response(index.get(&responses).clone(), mask),
                );
                let heads = wire_samples::head_stream_responses();
                wire_bytes(
                    "head answer",
                    &wire_samples::vary_head(index.get(&heads).clone(), mask),
                );
            }

            #[test]
            fn a_backup_snapshot_round_trips(mask in any::<u64>()) {
                wire_bytes(
                    "backup snapshot",
                    &wire_samples::vary_snapshot(wire_samples::stream_snapshot(), mask),
                );
            }
        }
    }

    mod external_entries {
        use openraft::type_config::alias::EntryOf;
        use openraft::vote::RaftLeaderId;
        use proptest::prelude::*;
        use ursula_runtime::AppendExternalRequest;
        use ursula_runtime::CreateStreamExternalRequest;
        use ursula_shard::BucketStreamId;
        use ursula_stream::ExternalPayloadRef;
        use ursula_stream::ProducerRequest;
        use ursula_stream::StreamCommand;

        use crate::RaftCommand;
        use crate::UrsulaRaftTypeConfig;
        use crate::codec::decode_wire;
        use crate::codec::encode_wire;

        /// Upper bound of an external write's entry apart from its
        /// Content-Type, which no limit of Ursula's own bounds yet.
        const EXTERNAL_ENTRY_BOUND_BYTES: usize = 2048;

        type CommittedLeaderId = <UrsulaRaftTypeConfig as openraft::RaftTypeConfig>::LeaderId;

        #[derive(Debug, Clone)]
        struct External {
            stream_id: BucketStreamId,
            content_type: String,
            payload_len: u64,
            object_slack: u64,
            message_count: u64,
            stream_seq: Option<String>,
            producer: Option<ProducerRequest>,
            close_after: bool,
            now_ms: u64,
            stream_expires_at_ms: Option<u64>,
            if_incarnation: Option<u64>,
        }

        fn producer() -> impl Strategy<Value = ProducerRequest> {
            (
                "[ -~]{1,256}",
                any::<u64>(),
                any::<u64>(),
                proptest::option::of(proptest::collection::vec(0x80_u8..=0xff, 32)),
            )
                .prop_map(|(producer_id, producer_epoch, producer_seq, digest)| {
                    ProducerRequest {
                        producer_id,
                        producer_epoch,
                        producer_seq,
                        digest: digest.map(bytes::Bytes::from),
                        ..Default::default()
                    }
                })
        }

        fn external() -> impl Strategy<Value = External> {
            (
                ("[a-z0-9_-]{4,64}", "[a-z0-9._-]{1,57}"),
                "[ -~]{1,256}",
                (1_u64..=u64::MAX, any::<u64>(), any::<u64>()),
                proptest::option::of("[ -~]{1,256}"),
                proptest::option::of(producer()),
                (
                    any::<bool>(),
                    any::<u64>(),
                    proptest::option::of(any::<u64>()),
                    proptest::option::of(any::<u64>()),
                ),
            )
                .prop_map(
                    |(
                        (bucket, stream),
                        content_type,
                        (payload_len, object_slack, message_count),
                        stream_seq,
                        producer,
                        (close_after, now_ms, stream_expires_at_ms, if_incarnation),
                    )| External {
                        stream_id: BucketStreamId::new(bucket, stream),
                        content_type,
                        payload_len,
                        object_slack,
                        message_count,
                        stream_seq,
                        producer,
                        close_after,
                        now_ms,
                        stream_expires_at_ms,
                        if_incarnation,
                    },
                )
        }

        fn staged(input: &External) -> ExternalPayloadRef {
            ExternalPayloadRef {
                s3_path: ursula_runtime::new_external_payload_path(&input.stream_id),
                payload_len: input.payload_len,
                object_size: input.payload_len.saturating_add(input.object_slack),
            }
        }

        fn append(input: &External) -> StreamCommand {
            AppendExternalRequest {
                stream_id: input.stream_id.clone(),
                content_type: input.content_type.clone(),
                payload: staged(input),
                message_count: input.message_count,
                close_after: input.close_after,
                stream_seq: input.stream_seq.clone(),
                producer: input.producer.clone(),
                now_ms: input.now_ms,
                if_incarnation: input.if_incarnation,
                upload: ursula_runtime::ProposedUpload::new(
                    ursula_runtime::UploadFence::Unreplicated,
                ),
            }
            .into()
        }

        fn create(input: &External) -> StreamCommand {
            CreateStreamExternalRequest {
                stream_id: input.stream_id.clone(),
                content_type: input.content_type.clone(),
                initial_payload: staged(input),
                message_count: input.message_count,
                close_after: input.close_after,
                stream_seq: input.stream_seq.clone(),
                stream_ttl_seconds: input.now_ms.checked_rem(86_400),
                stream_expires_at_ms: input.stream_expires_at_ms,
                now_ms: input.now_ms,
                if_incarnation: input.if_incarnation,
                upload: ursula_runtime::ProposedUpload::new(
                    ursula_runtime::UploadFence::Unreplicated,
                ),
            }
            .into()
        }

        fn entry(
            term: u64,
            node: u64,
            index: u64,
            command: StreamCommand,
        ) -> EntryOf<UrsulaRaftTypeConfig> {
            EntryOf::<UrsulaRaftTypeConfig> {
                log_id: openraft::LogId {
                    leader_id: CommittedLeaderId::new(term, node),
                    index,
                },
                payload: openraft::EntryPayload::Normal(RaftCommand::Stream(command)),
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            /// An external create or append makes an entry of at most 2 KiB
            /// plus its Content-Type whatever its body held, and round trips
            /// through the wire codec.
            #[test]
            fn external_write_entries_are_bounded_and_round_trip(
                input in external(),
                create_stream in any::<bool>(),
                (term, node, index) in (any::<u64>(), any::<u64>(), any::<u64>()),
            ) {
                let command = if create_stream { create(&input) } else { append(&input) };
                let entry = entry(term, node, index, command);
                let bytes = encode_wire(&entry);
                prop_assert!(
                    bytes.len() <= EXTERNAL_ENTRY_BOUND_BYTES.saturating_add(input.content_type.len()),
                    "{} bytes for {:?}",
                    bytes.len(),
                    input
                );
                let decoded: EntryOf<UrsulaRaftTypeConfig> = decode_wire(&bytes, "entry").unwrap();
                prop_assert_eq!(decoded, entry);
            }
        }
    }
}
