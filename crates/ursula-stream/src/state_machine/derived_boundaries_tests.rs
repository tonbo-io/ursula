//! Bounded-state F4b: no message records in replicated state. Binary streams keep append starts in the hot buffer, JSON streams
//! use the dense record offsets. Every test checks bootstrap against an
//! oracle of the messages actually appended: one part per message from the
//! snapshot offset when that offset is at or above the first exact boundary,
//! an honest partial otherwise.

use super::*;

const BUCKET: &str = "derivedbounds";
const OCTET: &str = "application/octet-stream";
const JSON: &str = "application/json";

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, id)
}

fn machine() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: BUCKET.to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));
    machine
}

fn create(machine: &mut StreamStateMachine, id: &str, content_type: &str, initial: &[u8]) {
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: stream(id),
        content_type: content_type.to_owned(),
        initial_payload: bytes::Bytes::copy_from_slice(initial),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 0,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
}

fn append_command(id: &str, content_type: &str, payload: &[u8]) -> StreamCommand {
    StreamCommand::Append {
        stream_id: stream(id),
        content_type: Some(content_type.to_owned()),
        payload: bytes::Bytes::copy_from_slice(payload),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    }
}

fn append(machine: &mut StreamStateMachine, id: &str, content_type: &str, payload: &[u8]) {
    let response = machine.apply(append_command(id, content_type, payload));
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn append_external(machine: &mut StreamStateMachine, id: &str, path: &str, len: u64) {
    let response = machine.apply(StreamCommand::AppendExternal {
        stream_id: stream(id),
        content_type: Some(OCTET.to_owned()),
        payload: ExternalPayloadRef {
            s3_path: path.to_owned(),
            payload_len: len,
            object_size: len,
        },
        record_ends: Vec::new(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
        record_match: None,
    });
    assert!(
        matches!(response, StreamResponse::Appended { .. }),
        "{response:?}"
    );
}

fn flush(machine: &mut StreamStateMachine, id: &str, start: u64, end: u64, path: &str) {
    let response = machine.apply(StreamCommand::FlushCold {
        stream_id: stream(id),
        chunk: ColdChunkRef {
            start_offset: start,
            end_offset: end,
            s3_path: path.to_owned(),
            object_size: end - start,
            object_offset: 0,
            shared_object: false,
            payload_digest: String::new(),
        },
        cold_generation: machine
            .cold_index_generation(&stream(id))
            .unwrap_or_default(),
    });
    assert!(
        matches!(response, StreamResponse::ColdFlushed { .. }),
        "{response:?}"
    );
}

fn publish_snapshot(machine: &mut StreamStateMachine, id: &str, offset: u64) -> StreamResponse {
    machine.apply(StreamCommand::PublishSnapshot {
        stream_id: stream(id),
        snapshot_offset: offset,
        content_type: OCTET.to_owned(),
        payload: bytes::Bytes::from_static(b"state"),
        now_ms: 0,
    })
}

fn retain(machine: &mut StreamStateMachine, id: &str, offset: u64) -> StreamResponse {
    machine.apply(StreamCommand::AdvanceRetention {
        stream_id: stream(id),
        retained_offset: offset,
        now_ms: 0,
    })
}

fn entry(machine: &StreamStateMachine, id: &str) -> StreamSnapshotEntry {
    machine
        .snapshot()
        .streams
        .into_iter()
        .find(|entry| entry.metadata.stream_id == stream(id))
        .expect("snapshot entry")
}

fn seal_point(entry: &StreamSnapshotEntry) -> u64 {
    entry
        .hot_segments
        .first()
        .map_or(entry.metadata.tail_offset, |segment| segment.start_offset)
}

fn json_records(count: usize, seed: u64) -> Vec<u8> {
    let mut payload = Vec::new();
    for index in 0..count {
        payload.extend_from_slice(format!("{{\"r\":{}}}\n", seed * 10 + index as u64).as_bytes());
    }
    payload
}

/// Every message ever appended, `[start, end)`, in offset order.
#[derive(Default)]
struct Oracle {
    messages: Vec<(u64, u64)>,
}

impl Oracle {
    fn push(&mut self, start: u64, end: u64) {
        self.messages.push((start, end));
    }

    fn push_json(&mut self, start: u64, payload: &[u8]) {
        let mut record_start = start;
        for (index, byte) in payload.iter().enumerate() {
            if *byte == b'\n' {
                let end = start + index as u64 + 1;
                self.push(record_start, end);
                record_start = end;
            }
        }
    }

    fn is_boundary(&self, offset: u64) -> bool {
        self.messages
            .iter()
            .any(|(start, end)| *start == offset || *end == offset)
    }

    /// Asserts the bootstrap plan: exact per message from the snapshot
    /// offset, or an honest partial when that offset is below the first
    /// message start at or above the seal point.
    #[track_caller]
    fn check_bootstrap(&self, machine: &StreamStateMachine, id: &str) {
        let entry = entry(machine, id);
        let tail = entry.metadata.tail_offset;
        let retained = entry.retained_offset.unwrap_or(0);
        let plan = machine.bootstrap_plan(&stream(id)).expect("bootstrap plan");
        let snapshot_offset = plan
            .snapshot
            .as_ref()
            .map_or(retained, |snapshot| snapshot.offset);
        let seal = seal_point(&entry);
        let frontier = self
            .messages
            .iter()
            .map(|(start, _)| *start)
            .find(|start| *start >= seal)
            .unwrap_or(tail)
            .max(retained);
        if snapshot_offset < frontier {
            assert!(plan.updates.is_empty(), "{plan:?}");
            assert_eq!(plan.next_offset, snapshot_offset);
            assert!(!plan.up_to_date);
            return;
        }
        let expected = self
            .messages
            .iter()
            .filter(|(start, _)| *start >= snapshot_offset)
            .map(|(start, end)| StreamMessageRecord {
                start_offset: *start,
                end_offset: *end,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            plan.updates, expected,
            "seal {seal} snapshot {snapshot_offset}"
        );
        assert_eq!(plan.next_offset, tail);
        assert!(plan.up_to_date);
    }
}

/// Snapshot, restore and compare: the restored machine must hold the same
/// state and answer bootstrap and hot accounting identically.
#[track_caller]
fn assert_restore_matches_live(machine: &StreamStateMachine, id: &str) -> StreamStateMachine {
    let snapshot = machine.snapshot();
    let restored = StreamStateMachine::restore(snapshot.clone()).expect("restore");
    assert_eq!(restored.snapshot(), snapshot);
    assert_eq!(
        restored.bootstrap_plan(&stream(id)),
        machine.bootstrap_plan(&stream(id))
    );
    assert_eq!(restored.total_hot_records(), machine.total_hot_records());
    assert_eq!(
        restored.total_hot_real_bytes(),
        machine.total_hot_real_bytes()
    );
    assert_eq!(
        restored.hot_real_len(&stream(id)),
        machine.hot_real_len(&stream(id))
    );
    restored
}

#[test]
fn binary_stream_keeps_append_starts() {
    let mut machine = machine();
    create(&mut machine, "bin", OCTET, b"ab");
    append(&mut machine, "bin", OCTET, b"cde");
    append(&mut machine, "bin", OCTET, b"f");
    let entry = entry(&machine, "bin");
    assert_eq!(entry.hot_append_starts, vec![0, 2, 5]);
    let mut oracle = Oracle::default();
    oracle.push(0, 2);
    oracle.push(2, 5);
    oracle.push(5, 6);
    oracle.check_bootstrap(&machine, "bin");
    // Each hot message costs 8 bytes of boundary (F6c).
    assert_eq!(machine.total_hot_records(), 3);
    assert_eq!(
        machine.total_hot_real_bytes(),
        6 + 3 * crate::HOT_RECORD_OVERHEAD_BYTES
    );
    assert_restore_matches_live(&machine, "bin");
}

#[test]
fn json_stream_uses_dense_offsets_and_keeps_no_starts() {
    let mut machine = machine();
    let first = json_records(2, 1);
    create(&mut machine, "json", JSON, &first);
    let second = json_records(3, 2);
    append(&mut machine, "json", JSON, &second);
    let entry = entry(&machine, "json");
    assert!(entry.hot_append_starts.is_empty());
    let mut oracle = Oracle::default();
    oracle.push_json(0, &first);
    oracle.push_json(first.len() as u64, &second);
    oracle.check_bootstrap(&machine, "json");
    assert_eq!(machine.total_hot_records(), 5);
    assert_restore_matches_live(&machine, "json");
}

#[test]
fn flush_that_splits_a_message_moves_the_exact_frontier_past_it() {
    for content_type in [OCTET, JSON] {
        let mut machine = machine();
        let mut oracle = Oracle::default();
        create(&mut machine, "s", content_type, b"");
        let payloads = if content_type == JSON {
            vec![json_records(1, 1), json_records(1, 2), json_records(1, 3)]
        } else {
            vec![b"0123456789".to_vec(); 3]
        };
        let mut offset = 0;
        for payload in &payloads {
            append(&mut machine, "s", content_type, payload);
            if content_type == JSON {
                oracle.push_json(offset, payload);
            } else {
                oracle.push(offset, offset + payload.len() as u64);
            }
            offset += payload.len() as u64;
        }
        let first_len = payloads[0].len() as u64;
        // Flush into the middle of the second message.
        flush(&mut machine, "s", 0, first_len + 3, "chunk-1");
        assert_eq!(seal_point(&entry(&machine, "s")), first_len + 3);
        // No snapshot: S = 0 lies below the frontier (the third message).
        oracle.check_bootstrap(&machine, "s");
        let plan = machine.bootstrap_plan(&stream("s")).expect("plan");
        assert!(!plan.up_to_date);
        // A snapshot at the third message's start is exact again.
        let third = 2 * first_len;
        assert!(matches!(
            publish_snapshot(&mut machine, "s", third),
            StreamResponse::SnapshotPublished { .. }
        ));
        oracle.check_bootstrap(&machine, "s");
        // An intra-message offset above the seal point is not a boundary.
        assert!(matches!(
            publish_snapshot(&mut machine, "s", third + 1),
            StreamResponse::Error { .. }
        ));
        assert_restore_matches_live(&machine, "s");
    }
}

#[test]
fn flush_at_a_message_boundary_keeps_the_next_message_exact() {
    let mut machine = machine();
    create(&mut machine, "bin", OCTET, b"");
    append(&mut machine, "bin", OCTET, b"aaaa");
    append(&mut machine, "bin", OCTET, b"bbbb");
    flush(&mut machine, "bin", 0, 4, "chunk-1");
    assert_eq!(entry(&machine, "bin").hot_append_starts, vec![4]);
    assert!(matches!(
        publish_snapshot(&mut machine, "bin", 4),
        StreamResponse::SnapshotPublished { .. }
    ));
    let plan = machine.bootstrap_plan(&stream("bin")).expect("plan");
    assert_eq!(plan.updates, vec![StreamMessageRecord {
        start_offset: 4,
        end_offset: 8,
    }]);
    assert!(plan.up_to_date);
}

#[test]
fn external_append_above_hot_bytes_is_a_message_until_the_hot_prefix_flushes() {
    let mut machine = machine();
    let mut oracle = Oracle::default();
    create(&mut machine, "bin", OCTET, b"");
    append(&mut machine, "bin", OCTET, b"hot!");
    oracle.push(0, 4);
    append_external(&mut machine, "bin", "external-1", 6);
    oracle.push(4, 10);
    append(&mut machine, "bin", OCTET, b"tail");
    oracle.push(10, 14);
    assert_eq!(entry(&machine, "bin").hot_append_starts, vec![0, 4, 10]);
    oracle.check_bootstrap(&machine, "bin");
    assert_restore_matches_live(&machine, "bin");
    flush(&mut machine, "bin", 0, 4, "chunk-1");
    // The external append is cold now; only the hot tail keeps a start.
    assert_eq!(entry(&machine, "bin").hot_append_starts, vec![10]);
    oracle.check_bootstrap(&machine, "bin");
    assert_restore_matches_live(&machine, "bin");
    // An external append onto an empty hot buffer is cold at once.
    flush(&mut machine, "bin", 10, 14, "chunk-2");
    append_external(&mut machine, "bin", "external-2", 5);
    assert!(entry(&machine, "bin").hot_append_starts.is_empty());
    assert_restore_matches_live(&machine, "bin");
}

#[test]
fn retention_prunes_append_starts_below_the_new_seal_point() {
    let mut machine = machine();
    let mut oracle = Oracle::default();
    create(&mut machine, "bin", OCTET, b"");
    for payload in [b"aa".as_slice(), b"bbb", b"c", b"dddd"] {
        let start = entry(&machine, "bin").metadata.tail_offset;
        append(&mut machine, "bin", OCTET, payload);
        oracle.push(start, start + payload.len() as u64);
    }
    assert!(matches!(
        publish_snapshot(&mut machine, "bin", 5),
        StreamResponse::SnapshotPublished { .. }
    ));
    let response = retain(&mut machine, "bin", 5);
    assert!(
        matches!(response, StreamResponse::RetentionAdvanced { .. }),
        "{response:?}"
    );
    assert_eq!(entry(&machine, "bin").hot_append_starts, vec![5, 6]);
    oracle.check_bootstrap(&machine, "bin");
    assert_restore_matches_live(&machine, "bin");
}

#[test]
fn restore_rejects_inconsistent_append_starts() {
    let mut machine = machine();
    create(&mut machine, "bin", OCTET, b"");
    append(&mut machine, "bin", OCTET, b"aaaa");
    append(&mut machine, "bin", OCTET, b"bbbb");
    flush(&mut machine, "bin", 0, 4, "chunk-1");
    create(&mut machine, "json", JSON, &json_records(2, 1));
    let good = machine.snapshot();
    StreamStateMachine::restore(good.clone()).expect("consistent snapshot restores");

    let mutate = |f: &dyn Fn(&mut StreamSnapshot)| {
        let mut snapshot = good.clone();
        f(&mut snapshot);
        StreamStateMachine::restore(snapshot)
    };
    let bin = |snapshot: &mut StreamSnapshot| -> usize {
        snapshot
            .streams
            .iter()
            .position(|entry| entry.metadata.stream_id == stream("bin"))
            .expect("bin")
    };
    // A start below the seal point.
    assert!(matches!(
        mutate(&|s| {
            let i = bin(s);
            s.streams[i].hot_append_starts = vec![0, 4];
        }),
        Err(StreamSnapshotError::MessageBoundaryMismatch { .. })
    ));
    // Not strictly increasing, or at the tail.
    assert!(matches!(
        mutate(&|s| {
            let i = bin(s);
            s.streams[i].hot_append_starts = vec![6, 5];
        }),
        Err(StreamSnapshotError::MessageBoundaryMismatch { .. })
    ));
    assert!(matches!(
        mutate(&|s| {
            let i = bin(s);
            s.streams[i].hot_append_starts = vec![8];
        }),
        Err(StreamSnapshotError::MessageBoundaryMismatch { .. })
    ));
    // Starts on a stream with a record index.
    assert!(matches!(
        mutate(&|s| {
            let i = s
                .streams
                .iter()
                .position(|entry| entry.metadata.stream_id == stream("json"))
                .expect("json");
            s.streams[i].hot_append_starts = vec![0];
        }),
        Err(StreamSnapshotError::MessageBoundaryMismatch { .. })
    ));
}

/// Restore-versus-live differential over a seeded random
/// workload of binary and JSON appends, external appends, flushes that cut
/// anywhere in the hot prefix, snapshot publishes and retention. After every
/// step the restored machine matches the live one, bootstrap matches the
/// oracle.
#[test]
fn restore_matches_live_and_bootstrap_matches_oracle_under_random_workload() {
    for seed in 1..=24u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move |bound: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % bound
        };
        let json = seed % 2 == 0;
        let content_type = if json { JSON } else { OCTET };
        let mut machine = machine();
        let mut oracle = Oracle::default();
        create(&mut machine, "s", content_type, b"");
        let mut objects = 0u64;
        for step in 0..120u64 {
            let current = entry(&machine, "s");
            let tail = current.metadata.tail_offset;
            match next(10) {
                0..=4 => {
                    let payload = if json {
                        json_records(1 + next(3) as usize, step)
                    } else {
                        vec![b'x'; 1 + next(12) as usize]
                    };
                    append(&mut machine, "s", content_type, &payload);
                    if json {
                        oracle.push_json(tail, &payload);
                    } else {
                        oracle.push(tail, tail + payload.len() as u64);
                    }
                }
                5 if !json => {
                    let len = 1 + next(20);
                    objects += 1;
                    append_external(&mut machine, "s", &format!("ext-{seed}-{objects}"), len);
                    oracle.push(tail, tail + len);
                }
                5..=7 => {
                    // Flush part of the first contiguous hot run.
                    let mut segments = current.hot_segments.iter();
                    if let Some(first) = segments.next() {
                        let mut run_end = first.end_offset;
                        for segment in segments {
                            if segment.start_offset != run_end {
                                break;
                            }
                            run_end = segment.end_offset;
                        }
                        let len = 1 + next(run_end - first.start_offset);
                        objects += 1;
                        flush(
                            &mut machine,
                            "s",
                            first.start_offset,
                            first.start_offset + len,
                            &format!("chunk-{seed}-{objects}"),
                        );
                    }
                }
                8 => {
                    // Publish at a message boundary (always accepted), or
                    // try an intra-message offset above the seal point
                    // (always refused).
                    let retained = current
                        .visible_snapshot
                        .as_ref()
                        .map_or(0, |snapshot| snapshot.offset)
                        .max(current.retained_offset.unwrap_or(0));
                    let candidates = oracle
                        .messages
                        .iter()
                        .map(|(start, _)| *start)
                        .chain(std::iter::once(tail))
                        .filter(|offset| *offset >= retained)
                        .collect::<Vec<_>>();
                    let offset = candidates[next(candidates.len() as u64) as usize];
                    let response = publish_snapshot(&mut machine, "s", offset);
                    assert!(
                        matches!(response, StreamResponse::SnapshotPublished { .. }),
                        "seed {seed} step {step}: {response:?}"
                    );
                    let seal = seal_point(&current);
                    if let Some(inside) = (seal + 1..tail).find(|o| !oracle.is_boundary(*o)) {
                        let response = publish_snapshot(&mut machine, "s", inside);
                        assert!(
                            matches!(response, StreamResponse::Error { .. }),
                            "seed {seed} step {step}: {inside} accepted"
                        );
                    }
                }
                _ => {
                    if let Some(snapshot) = current.visible_snapshot.as_ref() {
                        let response = retain(&mut machine, "s", snapshot.offset);
                        assert!(
                            matches!(response, StreamResponse::RetentionAdvanced { .. }),
                            "seed {seed} step {step}: {response:?}"
                        );
                    }
                }
            }
            let current = entry(&machine, "s");
            if json {
                assert!(current.hot_append_starts.is_empty());
            }
            oracle.check_bootstrap(&machine, "s");
            assert_restore_matches_live(&machine, "s");
        }
    }
}
