//! Bounded-state F4b: no message records in replicated state. JSON streams
//! use the dense record offsets; every other stream keeps no message
//! boundaries. Every test checks bootstrap against an oracle of the messages
//! actually appended: for JSON, one part per record from the snapshot offset
//! when that offset is at or above the first exact boundary; for any other
//! stream, `[S, tail)` as one part when `S` is at or above the seal point; an
//! honest partial otherwise.

use super::*;

const BUCKET: &str = "derivedbounds";
const OCTET: &str = "application/octet-stream";
const JSON: &str = "application/json";

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new(BUCKET, id)
}

fn fresh_machine() -> StreamStateMachine {
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

/// Whether the entry's hot segments hold every byte of `[start, end)`.
fn hot_covers(entry: &StreamSnapshotEntry, start: u64, end: u64) -> bool {
    let mut covered = start;
    for segment in &entry.hot_segments {
        if covered >= end {
            break;
        }
        if segment.end_offset <= covered {
            continue;
        }
        if segment.start_offset > covered {
            return false;
        }
        covered = segment.end_offset;
    }
    covered >= end
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
    /// A stream without a record index: no message boundaries.
    binary: bool,
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

    /// Asserts the bootstrap plan: exact from the snapshot offset (one part
    /// per JSON record, one part in all for any other stream), or an honest
    /// partial when that offset is below the exact frontier or, for a
    /// binary stream, `[S, tail)` is not all hot.
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
        let frontier = if self.binary {
            seal
        } else {
            self.messages
                .iter()
                .map(|(start, _)| *start)
                .find(|start| *start >= seal)
                .unwrap_or(tail)
        }
        .max(retained);
        if snapshot_offset < frontier || (self.binary && !hot_covers(&entry, snapshot_offset, tail))
        {
            assert!(plan.updates.is_empty(), "{plan:?}");
            assert_eq!(plan.next_offset, snapshot_offset);
            assert!(!plan.up_to_date);
            return;
        }
        let expected = if self.binary {
            (snapshot_offset < tail)
                .then_some(StreamMessageRecord {
                    start_offset: snapshot_offset,
                    end_offset: tail,
                })
                .into_iter()
                .collect::<Vec<_>>()
        } else {
            self.messages
                .iter()
                .filter(|(start, _)| *start >= snapshot_offset)
                .map(|(start, end)| StreamMessageRecord {
                    start_offset: *start,
                    end_offset: *end,
                })
                .collect::<Vec<_>>()
        };
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
fn binary_bootstrap_of_several_messages_is_one_part_with_no_per_message_charge() {
    let mut machine = fresh_machine();
    create(&mut machine, "bin", OCTET, b"ab");
    append(&mut machine, "bin", OCTET, b"cde");
    append(&mut machine, "bin", OCTET, b"f");
    let plan = machine.bootstrap_plan(&stream("bin")).expect("plan");
    assert_eq!(plan.updates, vec![StreamMessageRecord {
        start_offset: 0,
        end_offset: 6,
    }]);
    assert_eq!(plan.next_offset, 6);
    assert!(plan.up_to_date);
    // F6c: hot payload only; nothing is kept per binary message.
    assert_eq!(machine.total_hot_records(), 0);
    assert_eq!(machine.total_hot_real_bytes(), 6);
    assert_restore_matches_live(&machine, "bin");

    // An external append above hot bytes leaves a cold gap in `[S, tail)`:
    // bootstrap does not read cold storage, so it is an honest partial.
    append_external(&mut machine, "bin", "external-1", 4);
    append(&mut machine, "bin", OCTET, b"g");
    let plan = machine.bootstrap_plan(&stream("bin")).expect("plan");
    assert!(plan.updates.is_empty(), "{plan:?}");
    assert_eq!(plan.next_offset, 0);
    assert!(!plan.up_to_date);
    assert_restore_matches_live(&machine, "bin");
}

#[test]
fn binary_bootstrap_above_the_cap_is_snapshot_only() {
    let mut machine = fresh_machine();
    create(&mut machine, "bin", OCTET, b"");
    append(&mut machine, "bin", OCTET, b"aaaa");
    append(&mut machine, "bin", OCTET, b"bbbb");
    let capped = machine
        .bootstrap_plan_with_cap(&stream("bin"), 7)
        .expect("plan");
    assert!(capped.updates.is_empty());
    assert_eq!(capped.next_offset, 0);
    assert!(!capped.up_to_date);
    let whole = machine
        .bootstrap_plan_with_cap(&stream("bin"), 8)
        .expect("plan");
    assert_eq!(whole.updates.len(), 1);
    assert!(whole.up_to_date);
}

#[test]
fn json_stream_uses_dense_offsets() {
    let mut machine = fresh_machine();
    let first = json_records(2, 1);
    create(&mut machine, "json", JSON, &first);
    let second = json_records(3, 2);
    append(&mut machine, "json", JSON, &second);
    let mut oracle = Oracle::default();
    oracle.push_json(0, &first);
    oracle.push_json(first.len() as u64, &second);
    oracle.check_bootstrap(&machine, "json");
    assert_eq!(machine.total_hot_records(), 5);
    assert_restore_matches_live(&machine, "json");
}

#[test]
fn flush_that_splits_a_json_record_moves_the_exact_frontier_past_it() {
    let mut machine = fresh_machine();
    let mut oracle = Oracle::default();
    create(&mut machine, "s", JSON, b"");
    let payloads = vec![json_records(1, 1), json_records(1, 2), json_records(1, 3)];
    let mut offset = 0;
    for payload in &payloads {
        append(&mut machine, "s", JSON, payload);
        oracle.push_json(offset, payload);
        offset += payload.len() as u64;
    }
    let first_len = payloads[0].len() as u64;
    // Flush into the middle of the second record.
    flush(&mut machine, "s", 0, first_len + 3, "chunk-1");
    assert_eq!(seal_point(&entry(&machine, "s")), first_len + 3);
    // No snapshot: S = 0 lies below the frontier (the third record).
    oracle.check_bootstrap(&machine, "s");
    let plan = machine.bootstrap_plan(&stream("s")).expect("plan");
    assert!(!plan.up_to_date);
    // A snapshot at the third record's start is exact again.
    let third = 2 * first_len;
    assert!(matches!(
        publish_snapshot(&mut machine, "s", third),
        StreamResponse::SnapshotPublished { .. }
    ));
    oracle.check_bootstrap(&machine, "s");
    // An intra-record offset above the seal point is not a boundary.
    assert!(matches!(
        publish_snapshot(&mut machine, "s", third + 1),
        StreamResponse::Error { .. }
    ));
    assert_restore_matches_live(&machine, "s");
}

#[test]
fn binary_snapshot_and_retention_accept_any_offset_in_range() {
    let mut machine = fresh_machine();
    let mut oracle = Oracle {
        binary: true,
        ..Oracle::default()
    };
    create(&mut machine, "bin", OCTET, b"");
    for payload in [b"aaaa".as_slice(), b"bbbb", b"cccc"] {
        let start = entry(&machine, "bin").metadata.tail_offset;
        append(&mut machine, "bin", OCTET, payload);
        oracle.push(start, start + payload.len() as u64);
    }
    // Mid-message, above the seal point: accepted, and bootstrap answers
    // `[6, tail)` as one part.
    assert!(matches!(
        publish_snapshot(&mut machine, "bin", 6),
        StreamResponse::SnapshotPublished { .. }
    ));
    oracle.check_bootstrap(&machine, "bin");
    assert!(matches!(
        retain(&mut machine, "bin", 6),
        StreamResponse::RetentionAdvanced { .. }
    ));
    oracle.check_bootstrap(&machine, "bin");
    // Past the tail is still refused.
    assert!(matches!(
        publish_snapshot(&mut machine, "bin", 13),
        StreamResponse::Error { .. }
    ));
    // Mid-message after a flush into the third message: the snapshot sits
    // below the seal point, so bootstrap is an honest partial.
    flush(&mut machine, "bin", 6, 10, "chunk-1");
    assert!(matches!(
        publish_snapshot(&mut machine, "bin", 9),
        StreamResponse::SnapshotPublished { .. }
    ));
    oracle.check_bootstrap(&machine, "bin");
    assert_restore_matches_live(&machine, "bin");
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
        let mut machine = fresh_machine();
        let mut oracle = Oracle {
            binary: !json,
            ..Oracle::default()
        };
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
                    // JSON: publish at a record boundary (always accepted),
                    // or try an intra-record offset above the seal point
                    // (always refused). Binary: any offset in range is
                    // accepted.
                    let retained = current
                        .visible_snapshot
                        .as_ref()
                        .map_or(0, |snapshot| snapshot.offset)
                        .max(current.retained_offset.unwrap_or(0));
                    let candidates = if json {
                        oracle
                            .messages
                            .iter()
                            .map(|(start, _)| *start)
                            .chain(std::iter::once(tail))
                            .filter(|offset| *offset >= retained)
                            .collect::<Vec<_>>()
                    } else {
                        (retained..=tail).collect::<Vec<_>>()
                    };
                    let offset = candidates[next(candidates.len() as u64) as usize];
                    let response = publish_snapshot(&mut machine, "s", offset);
                    assert!(
                        matches!(response, StreamResponse::SnapshotPublished { .. }),
                        "seed {seed} step {step}: {response:?}"
                    );
                    let seal = seal_point(&current);
                    if json && let Some(inside) = (seal + 1..tail).find(|o| !oracle.is_boundary(*o))
                    {
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
            oracle.check_bootstrap(&machine, "s");
            assert_restore_matches_live(&machine, "s");
        }
    }
}
