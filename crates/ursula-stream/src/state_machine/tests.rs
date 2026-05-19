use super::*;

fn stream(id: &str) -> BucketStreamId {
    BucketStreamId::new("benchcmp", id)
}

fn create_bucket(machine: &mut StreamStateMachine) {
    assert_eq!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketCreated {
            bucket_id: "benchcmp".to_owned(),
        }
    );
}

fn create_stream(machine: &mut StreamStateMachine, id: &str) {
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream(id),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream(id),
            next_offset: 0,
            closed: false,
        }
    );
}

fn producer(id: &str, epoch: u64, seq: u64) -> ProducerRequest {
    ProducerRequest {
        producer_id: id.to_owned(),
        producer_epoch: epoch,
        producer_seq: seq,
    }
}

#[test]
fn stream_create_requires_existing_bucket_and_valid_ids() {
    let mut machine = StreamStateMachine::new();

    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "Bad".to_owned(),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidBucketId,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("s-1"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::BucketNotFound,
            ..
        }
    ));

    create_bucket(&mut machine);
    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("streams"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidStreamId,
            ..
        }
    ));
}

#[test]
fn create_stream_is_idempotent_only_when_metadata_matches() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("s-1"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: vec![0; 99],
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::AlreadyExists {
            next_offset: 0,
            closed: false,
            content_type: "application/octet-stream".to_owned(),
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
        }
    );

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("s-1"),
            content_type: "text/plain".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamAlreadyExistsConflict,
            ..
        }
    ));
}

#[test]
fn append_advances_offsets_and_checks_content_type() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefg".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 7,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("text/plain".to_owned()),
            payload: b"x".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ContentTypeMismatch,
            next_offset: Some(7),
            ..
        }
    ));
    assert_eq!(machine.head(&stream("s-1")).expect("stream").tail_offset, 7);
}

#[test]
fn catch_up_read_returns_payload_slice_and_bounds_errors() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefg".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));

    assert_eq!(
        machine.read(&stream("s-1"), 2, 3).expect("read"),
        StreamRead {
            offset: 2,
            next_offset: 5,
            content_type: "application/octet-stream".to_owned(),
            payload: b"cde".to_vec(),
            up_to_date: false,
            closed: false,
        }
    );
    assert_eq!(
        machine.read(&stream("s-1"), 7, 16).expect("tail read"),
        StreamRead {
            offset: 7,
            next_offset: 7,
            content_type: "application/octet-stream".to_owned(),
            payload: Vec::new(),
            up_to_date: true,
            closed: false,
        }
    );
    assert!(matches!(
        machine.read(&stream("s-1"), 8, 1),
        Err(StreamResponse::Error {
            code: StreamErrorCode::OffsetOutOfRange,
            next_offset: Some(7),
            ..
        })
    ));
}

#[test]
fn flush_cold_moves_hot_prefix_to_manifest_and_read_plan_splits() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdef".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 6,
            ..
        }
    ));

    let candidate = machine
        .plan_cold_flush(&stream("cold"), 4, 4)
        .expect("plan cold flush")
        .expect("cold flush candidate");
    assert_eq!(candidate.start_offset, 0);
    assert_eq!(candidate.end_offset, 4);
    assert_eq!(candidate.payload, b"abcd");
    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("cold"),
            chunk: ColdChunkRef {
                start_offset: candidate.start_offset,
                end_offset: candidate.end_offset,
                s3_path: "s3://bucket/cold/000000".to_owned(),
                object_size: u64::try_from(candidate.payload.len()).unwrap(),
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 4,
        }
    );
    assert_eq!(machine.hot_start_offset(&stream("cold")), 4);
    assert_eq!(machine.cold_chunks(&stream("cold")).len(), 1);

    let plan = machine.read_plan(&stream("cold"), 2, 4).expect("read plan");
    assert_eq!(plan.next_offset, 6);
    assert_eq!(plan.segments.len(), 2);
    match &plan.segments[0] {
        StreamReadSegment::Object(segment) => {
            assert_eq!(segment.read_start_offset, 2);
            assert_eq!(segment.len, 2);
        }
        other => panic!("expected cold object segment, got {other:?}"),
    }
    match &plan.segments[1] {
        StreamReadSegment::Hot(payload) => assert_eq!(payload, b"ef"),
        other => panic!("expected hot segment, got {other:?}"),
    }
    assert_eq!(
        machine.read(&stream("cold"), 0, 6),
        Err(StreamResponse::Error {
            code: StreamErrorCode::InvalidColdFlush,
            message: "stream 'benchcmp/cold' read requires object payload store".to_owned(),
            next_offset: Some(6),
        })
    );
    assert_eq!(
        machine.read(&stream("cold"), 4, 8).expect("hot read"),
        StreamRead {
            offset: 4,
            next_offset: 6,
            content_type: "application/octet-stream".to_owned(),
            payload: b"ef".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );

    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore snapshot");
    assert_eq!(restored.hot_start_offset(&stream("cold")), 4);
    assert_eq!(restored.cold_chunks(&stream("cold")).len(), 1);
    assert_eq!(
        restored.read(&stream("cold"), 4, 8).expect("hot read"),
        StreamRead {
            offset: 4,
            next_offset: 6,
            content_type: "application/octet-stream".to_owned(),
            payload: b"ef".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
}

#[test]
fn flush_cold_can_coalesce_contiguous_hot_segments() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold-coalesced");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("cold-coalesced"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("cold-coalesced"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"de".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 3,
            next_offset: 5,
            ..
        }
    ));

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("cold-coalesced"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 5,
                s3_path: "s3://bucket/cold-coalesced/000000".to_owned(),
                object_size: 5,
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 0,
        }
    );
    assert!(machine.hot_segments(&stream("cold-coalesced")).is_empty());
    assert_eq!(machine.hot_payload_len(&stream("cold-coalesced")), Ok(0));
    assert_eq!(machine.cold_chunks(&stream("cold-coalesced")).len(), 1);

    let plan = machine
        .read_plan(&stream("cold-coalesced"), 0, 5)
        .expect("read plan");
    assert_eq!(plan.next_offset, 5);
    assert_eq!(plan.segments.len(), 1);
    match &plan.segments[0] {
        StreamReadSegment::Object(segment) => {
            assert_eq!(segment.read_start_offset, 0);
            assert_eq!(segment.len, 5);
        }
        other => panic!("expected cold object segment, got {other:?}"),
    }
}

#[test]
fn plan_cold_flush_coalesces_contiguous_hot_segments() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "cold-planned-coalesced");
    for payload in [b"ab".as_slice(), b"cd".as_slice(), b"ef".as_slice()] {
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream("cold-planned-coalesced"),
                content_type: Some("application/octet-stream".to_owned()),
                payload: payload.to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));
    }

    assert!(
        machine
            .plan_cold_flush(&stream("cold-planned-coalesced"), 4, 4)
            .expect("plan cold flush")
            .is_some(),
        "planner should consider contiguous small hot segments together"
    );
    let candidate = machine
        .plan_cold_flush(&stream("cold-planned-coalesced"), 5, 5)
        .expect("plan cold flush")
        .expect("candidate");
    assert_eq!(candidate.start_offset, 0);
    assert_eq!(candidate.end_offset, 5);
    assert_eq!(candidate.payload, b"abcde");
}

#[test]
fn plan_next_cold_flush_selects_deterministic_eligible_stream() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "z-cold");
    create_stream(&mut machine, "a-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("z-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"zzzz".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("a-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"aaaa".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));

    let candidate = machine
        .plan_next_cold_flush(4, 4)
        .expect("plan next cold flush")
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("a-cold"));
    assert_eq!(candidate.payload, b"aaaa");
}

#[test]
fn plan_next_cold_flush_batch_advances_on_preview_state() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "batched-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("batched-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcd".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));

    let candidates = machine
        .plan_next_cold_flush_batch(1, 1, 4)
        .expect("plan cold flush batch");
    assert_eq!(candidates.len(), 4);
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| (candidate.start_offset, candidate.end_offset))
            .collect::<Vec<_>>(),
        vec![(0, 1), (1, 2), (2, 3), (3, 4)]
    );
    assert_eq!(
        candidates
            .iter()
            .map(|candidate| candidate.payload.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"c".as_slice(),
            b"d".as_slice()
        ]
    );
    assert_eq!(machine.hot_payload_len(&stream("batched-cold")), Ok(4));
    assert!(machine.cold_chunks(&stream("batched-cold")).is_empty());
}

#[test]
fn stale_cold_flush_candidate_after_delete_recreate_is_invalid_without_mutation() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "stale-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("stale-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefghijklmnopqr".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            next_offset: 18,
            ..
        }
    ));
    let candidate = machine
        .plan_cold_flush(&stream("stale-cold"), 18, 18)
        .expect("plan cold flush")
        .expect("candidate");

    assert!(matches!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("stale-cold")
        }),
        StreamResponse::Deleted {
            hard_deleted: true,
            ..
        }
    ));
    create_stream(&mut machine, "stale-cold");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("stale-cold"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abcdefghijklmnopq".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            next_offset: 17,
            ..
        }
    ));

    match machine.apply(StreamCommand::FlushCold {
        stream_id: stream("stale-cold"),
        chunk: ColdChunkRef {
            start_offset: candidate.start_offset,
            end_offset: candidate.end_offset,
            s3_path: "s3://bucket/stale-cold/old-candidate".to_owned(),
            object_size: u64::try_from(candidate.payload.len()).unwrap(),
        },
    }) {
        StreamResponse::Error {
            code: StreamErrorCode::InvalidColdFlush,
            message,
            next_offset: Some(17),
        } => assert!(message.contains("beyond stream")),
        other => panic!("expected stale invalid cold flush, got {other:?}"),
    }

    assert_eq!(
        machine.read(&stream("stale-cold"), 0, 32).expect("read"),
        StreamRead {
            offset: 0,
            next_offset: 17,
            content_type: "application/octet-stream".to_owned(),
            payload: b"abcdefghijklmnopq".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
}

#[test]
fn plan_next_cold_flush_skips_soft_deleted_streams() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "a-gone");
    create_stream(&mut machine, "b-live");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("a-gone"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"gone".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::AddForkRef {
            stream_id: stream("a-gone"),
            now_ms: 0,
        }),
        StreamResponse::ForkRefAdded { .. }
    ));
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("a-gone"),
        }),
        StreamResponse::Deleted {
            hard_deleted: false,
            parent_to_release: None,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("b-live"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"live".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));

    let candidate = machine
        .plan_next_cold_flush(4, 4)
        .expect("plan next cold flush")
        .expect("candidate");
    assert_eq!(candidate.stream_id, stream("b-live"));
    assert_eq!(candidate.payload, b"live");
}

#[test]
fn hot_payload_byte_metrics_follow_cold_flush() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "hot-a");
    create_stream(&mut machine, "hot-b");
    for (stream_name, payload) in [("hot-a", b"abcd".as_slice()), ("hot-b", b"xy".as_slice())] {
        assert!(matches!(
            machine.apply(StreamCommand::Append {
                stream_id: stream(stream_name),
                content_type: Some("application/octet-stream".to_owned()),
                payload: payload.to_vec(),
                close_after: false,
                stream_seq: None,
                producer: None,
                now_ms: 0,
            }),
            StreamResponse::Appended { .. }
        ));
    }

    assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(4));
    assert_eq!(machine.hot_payload_len(&stream("hot-b")), Ok(2));
    assert_eq!(machine.total_hot_payload_bytes(), 6);

    assert_eq!(
        machine.apply(StreamCommand::FlushCold {
            stream_id: stream("hot-a"),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: 3,
                s3_path: "s3://bucket/hot-a/000000".to_owned(),
                object_size: 3,
            },
        }),
        StreamResponse::ColdFlushed {
            hot_start_offset: 3,
        }
    );
    assert_eq!(machine.hot_payload_len(&stream("hot-a")), Ok(1));
    assert_eq!(machine.total_hot_payload_bytes(), 3);
}

#[test]
fn snapshot_restore_round_trips_payload_metadata_and_stream_seq() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("snap-open"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"hi".to_vec(),
            close_after: false,
            stream_seq: Some("0001".to_owned()),
            producer: None,
            stream_ttl_seconds: Some(60),
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream("snap-open"),
            next_offset: 2,
            closed: false,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("snap-open"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 2,
            next_offset: 5,
            ..
        }
    ));
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream("snap-closed"),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"x".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 0,
        }),
        StreamResponse::Created {
            stream_id: stream("snap-closed"),
            next_offset: 1,
            closed: true,
        }
    );

    let encoded = serde_json::to_vec(&machine.snapshot()).expect("serialize snapshot");
    let decoded = serde_json::from_slice::<StreamSnapshot>(&encoded).expect("deserialize snapshot");
    let mut restored = StreamStateMachine::restore(decoded).expect("restore snapshot");

    assert_eq!(
        restored.read(&stream("snap-open"), 0, 16).expect("read"),
        StreamRead {
            offset: 0,
            next_offset: 5,
            content_type: "application/octet-stream".to_owned(),
            payload: b"hiabc".to_vec(),
            up_to_date: true,
            closed: false,
        }
    );
    let metadata = restored.head(&stream("snap-open")).expect("metadata");
    assert_eq!(metadata.last_stream_seq.as_deref(), Some("0002"));
    assert_eq!(metadata.stream_ttl_seconds, Some(60));
    assert_eq!(metadata.stream_expires_at_ms, None);

    assert!(matches!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("snap-open"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"bad".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamSeqConflict,
            next_offset: Some(5),
            ..
        }
    ));
    assert_eq!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("snap-open"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"!".to_vec(),
            close_after: false,
            stream_seq: Some("0003".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 5,
            next_offset: 6,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(matches!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("snap-closed"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"!".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(1),
            ..
        }
    ));
}

#[test]
fn snapshot_order_is_deterministic() {
    let mut machine = StreamStateMachine::new();
    for bucket_id in ["zzzz", "benchcmp", "aaaa"] {
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: bucket_id.to_owned(),
        });
    }
    for stream_id in [
        BucketStreamId::new("zzzz", "stream-b"),
        BucketStreamId::new("benchcmp", "stream-b"),
        BucketStreamId::new("benchcmp", "stream-a"),
        BucketStreamId::new("aaaa", "stream-z"),
    ] {
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id,
                content_type: "application/octet-stream".to_owned(),
                initial_payload: Vec::new(),
                close_after: false,
                stream_seq: None,
                producer: None,
                stream_ttl_seconds: None,
                stream_expires_at_ms: None,
                forked_from: None,
                fork_offset: None,
                now_ms: 0,
            }),
            StreamResponse::Created { .. }
        ));
    }

    let snapshot = machine.snapshot();
    assert_eq!(snapshot.buckets, ["aaaa", "benchcmp", "zzzz"]);
    assert_eq!(
        snapshot
            .streams
            .iter()
            .map(|entry| entry.metadata.stream_id.to_string())
            .collect::<Vec<_>>(),
        [
            "aaaa/stream-z",
            "benchcmp/stream-a",
            "benchcmp/stream-b",
            "zzzz/stream-b",
        ]
    );
}

#[test]
fn snapshot_restore_rejects_invalid_entries() {
    assert_eq!(
        StreamStateMachine::restore(StreamSnapshot {
            buckets: vec!["benchcmp".to_owned(), "benchcmp".to_owned()],
            streams: Vec::new(),
        })
        .expect_err("duplicate bucket"),
        StreamSnapshotError::DuplicateBucket("benchcmp".to_owned())
    );

    assert!(matches!(
        StreamStateMachine::restore(StreamSnapshot {
            buckets: vec!["benchcmp".to_owned()],
            streams: vec![StreamSnapshotEntry {
                metadata: StreamMetadata {
                    stream_id: BucketStreamId::new("missing", "stream"),
                    content_type: "application/octet-stream".to_owned(),
                    status: StreamStatus::Open,
                    tail_offset: 0,
                    last_stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    created_at_ms: 0,
                    last_ttl_touch_at_ms: 0,
                    forked_from: None,
                    fork_offset: None,
                    fork_ref_count: 0,
                },
                hot_start_offset: 0,
                payload: Vec::new(),
                hot_segments: Vec::new(),
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
                message_records: Vec::new(),
                visible_snapshot: None,
                producer_states: Vec::new(),
            }],
        }),
        Err(StreamSnapshotError::MissingBucket(_))
    ));

    assert!(matches!(
        StreamStateMachine::restore(StreamSnapshot {
            buckets: vec!["benchcmp".to_owned()],
            streams: vec![StreamSnapshotEntry {
                metadata: StreamMetadata {
                    stream_id: stream("bad-len"),
                    content_type: "application/octet-stream".to_owned(),
                    status: StreamStatus::Open,
                    tail_offset: 2,
                    last_stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    created_at_ms: 0,
                    last_ttl_touch_at_ms: 0,
                    forked_from: None,
                    fork_offset: None,
                    fork_ref_count: 0,
                },
                hot_start_offset: 0,
                payload: b"x".to_vec(),
                hot_segments: Vec::new(),
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
                message_records: Vec::new(),
                visible_snapshot: None,
                producer_states: Vec::new(),
            }],
        }),
        Err(StreamSnapshotError::PayloadLengthMismatch { .. })
    ));

    assert!(matches!(
        StreamStateMachine::restore(StreamSnapshot {
            buckets: vec!["benchcmp".to_owned()],
            streams: vec![StreamSnapshotEntry {
                metadata: StreamMetadata {
                    stream_id: stream("duplicate-producer"),
                    content_type: "application/octet-stream".to_owned(),
                    status: StreamStatus::Open,
                    tail_offset: 0,
                    last_stream_seq: None,
                    stream_ttl_seconds: None,
                    stream_expires_at_ms: None,
                    created_at_ms: 0,
                    last_ttl_touch_at_ms: 0,
                    forked_from: None,
                    fork_offset: None,
                    fork_ref_count: 0,
                },
                hot_start_offset: 0,
                payload: Vec::new(),
                hot_segments: Vec::new(),
                cold_chunks: Vec::new(),
                external_segments: Vec::new(),
                message_records: Vec::new(),
                visible_snapshot: None,
                producer_states: vec![
                    ProducerSnapshot {
                        producer_id: "writer-1".to_owned(),
                        producer_epoch: 0,
                        producer_seq: 0,
                        last_start_offset: 0,
                        last_next_offset: 0,
                        last_closed: false,
                        last_items: Vec::new(),
                    },
                    ProducerSnapshot {
                        producer_id: "writer-1".to_owned(),
                        producer_epoch: 1,
                        producer_seq: 0,
                        last_start_offset: 0,
                        last_next_offset: 0,
                        last_closed: false,
                        last_items: Vec::new(),
                    },
                ],
            }],
        }),
        Err(StreamSnapshotError::DuplicateProducer { .. })
    ));
}

#[test]
fn close_is_monotonic_and_close_only_is_idempotent() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            closed: true,
            deduplicated: false,
            producer: None,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Close {
            stream_id: stream("s-1"),
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Closed {
            next_offset: 3,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"x".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(3),
            ..
        }
    ));
}

#[test]
fn stream_seq_must_strictly_increase() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"a".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"b".to_vec(),
            close_after: false,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamSeqConflict,
            next_offset: Some(1),
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("s-1"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"c".to_vec(),
            close_after: false,
            stream_seq: Some("0003".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            ..
        }
    ));
}

#[test]
fn producer_headers_deduplicate_retries_and_fence_stale_epochs() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-stream");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"a".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            closed: false,
            deduplicated: false,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"ignored-retry-body".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            closed: false,
            deduplicated: true,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert_eq!(
        machine
            .read(&stream("producer-stream"), 0, 16)
            .expect("read")
            .payload,
        b"a"
    );

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"gap".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 2)),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ProducerSeqConflict,
            ..
        }
    ));

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"b".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 1, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            closed: false,
            deduplicated: false,
            producer: Some(producer("writer-1", 1, 0)),
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-stream"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"stale".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 1)),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::ProducerEpochStale,
            ..
        }
    ));
}

#[test]
fn producer_append_batch_deduplicates_retries_without_partial_mutation() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-batch");

    let first_payloads = [b"ab".as_slice(), b"c".as_slice()];
    let first = machine
        .append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &first_payloads,
            Some(producer("writer-1", 0, 0)),
            0,
        )
        .expect("first batch");
    assert_eq!(
        first.items,
        vec![
            StreamBatchAppendItem {
                offset: 0,
                next_offset: 2,
                closed: false,
                deduplicated: false,
            },
            StreamBatchAppendItem {
                offset: 2,
                next_offset: 3,
                closed: false,
                deduplicated: false,
            },
        ]
    );
    assert!(!first.deduplicated);

    let duplicate = machine
        .append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &first_payloads,
            Some(producer("writer-1", 0, 0)),
            0,
        )
        .expect("duplicate batch");
    assert!(duplicate.deduplicated);
    assert!(duplicate.items.iter().all(|item| item.deduplicated));
    assert_eq!(duplicate.items[0].offset, 0);
    assert_eq!(duplicate.items[1].next_offset, 3);
    assert_eq!(
        machine
            .read(&stream("producer-batch"), 0, 16)
            .expect("read")
            .payload,
        b"abc"
    );

    let invalid_payloads = [b"".as_slice()];
    assert!(matches!(
        machine.append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &invalid_payloads,
            Some(producer("writer-1", 0, 1)),
            0,
        ),
        Err(StreamResponse::Error {
            code: StreamErrorCode::EmptyAppend,
            ..
        })
    ));

    let next_payloads = [b"d".as_slice()];
    let next = machine
        .append_batch_borrowed(
            stream("producer-batch"),
            Some("application/octet-stream"),
            &next_payloads,
            Some(producer("writer-1", 0, 1)),
            0,
        )
        .expect("next batch");
    assert_eq!(next.items[0].offset, 3);
    assert_eq!(
        machine
            .read(&stream("producer-batch"), 0, 16)
            .expect("read")
            .payload,
        b"abcd"
    );
}

#[test]
fn producer_state_survives_snapshot_restore() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-snapshot");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-snapshot"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"a".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            deduplicated: false,
            ..
        }
    ));

    let snapshot = machine.snapshot();
    assert_eq!(snapshot.streams[0].producer_states.len(), 1);
    assert_eq!(
        snapshot.streams[0].producer_states[0].last_items,
        vec![ProducerAppendRecord {
            start_offset: 0,
            next_offset: 1,
            closed: false,
        }]
    );
    let mut restored = StreamStateMachine::restore(snapshot).expect("restore snapshot");

    assert!(matches!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("producer-snapshot"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"retry".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 1,
            deduplicated: true,
            ..
        }
    ));
    assert_eq!(
        restored.apply(StreamCommand::Append {
            stream_id: stream("producer-snapshot"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"b".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 1)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 1,
            next_offset: 2,
            closed: false,
            deduplicated: false,
            producer: Some(producer("writer-1", 0, 1)),
        }
    );
}

#[test]
fn stream_ttl_uses_sliding_access_window() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("ttl-window");

    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: b"hi".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: Some(1),
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 1_000,
        }),
        StreamResponse::Created {
            stream_id: stream_id.clone(),
            next_offset: 2,
            closed: false,
        }
    );

    assert_eq!(
        machine.access_requires_write(&stream_id, 1_500, false),
        Ok(false)
    );
    assert_eq!(
        machine
            .head_at(&stream_id, 1_500)
            .expect("head before ttl expiry")
            .last_ttl_touch_at_ms,
        1_000
    );
    assert_eq!(
        machine.access_requires_write(&stream_id, 1_500, true),
        Ok(true)
    );
    assert_eq!(
        machine.apply(StreamCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms: 1_500,
            renew_ttl: true,
        }),
        StreamResponse::Accessed {
            changed: true,
            expired: false,
        }
    );

    assert!(machine.read_plan_at(&stream_id, 2, 16, 2_400).is_ok());
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"!".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 2_400,
        }),
        StreamResponse::Appended {
            offset: 2,
            next_offset: 3,
            closed: false,
            deduplicated: false,
            producer: None,
        }
    );
    assert!(machine.head_at(&stream_id, 3_399).is_some());
    assert!(machine.head_at(&stream_id, 3_400).is_none());
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"late".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 3_401,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        }
    ));
}

#[test]
fn stream_expires_at_is_absolute_and_recreate_after_expiry() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    let stream_id = stream("absolute-expiry");

    assert!(matches!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "application/octet-stream".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: Some(2_000),
            forked_from: None,
            fork_offset: None,
            now_ms: 1_000,
        }),
        StreamResponse::Created { .. }
    ));
    assert_eq!(
        machine.apply(StreamCommand::TouchStreamAccess {
            stream_id: stream_id.clone(),
            now_ms: 1_500,
            renew_ttl: true,
        }),
        StreamResponse::Accessed {
            changed: false,
            expired: false,
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream_id.clone(),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"body".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 1_600,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(machine.read_plan_at(&stream_id, 0, 16, 1_999).is_ok());
    assert!(matches!(
        machine.read_plan_at(&stream_id, 0, 16, 2_000),
        Err(StreamResponse::Error {
            code: StreamErrorCode::StreamNotFound,
            ..
        })
    ));
    assert_eq!(
        machine.apply(StreamCommand::CreateStream {
            stream_id: stream_id.clone(),
            content_type: "text/plain".to_owned(),
            initial_payload: Vec::new(),
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            forked_from: None,
            fork_offset: None,
            now_ms: 2_001,
        }),
        StreamResponse::Created {
            stream_id,
            next_offset: 0,
            closed: false,
        }
    );
}

#[test]
fn producer_duplicate_final_append_remains_idempotent_after_close() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "producer-close");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-close"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"final".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: false,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-close"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"final".to_vec(),
            close_after: true,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 0)),
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: true,
            producer: Some(producer("writer-1", 0, 0)),
        }
    );
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("producer-close"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"too-late".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: Some(producer("writer-1", 0, 1)),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(5),
            ..
        }
    ));
}

#[test]
fn append_conflict_precedence_reports_closed_before_mismatch_or_seq() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "closed-precedence");

    assert_eq!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("closed-precedence"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"final".to_vec(),
            close_after: true,
            stream_seq: Some("0002".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 5,
            closed: true,
            deduplicated: false,
            producer: None,
        }
    );

    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("closed-precedence"),
            content_type: Some("text/plain".to_owned()),
            payload: b"too-late".to_vec(),
            close_after: false,
            stream_seq: Some("0001".to_owned()),
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::StreamClosed,
            next_offset: Some(5),
            ..
        }
    ));
}

#[test]
fn bucket_delete_requires_empty_bucket() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "s-1");

    assert!(matches!(
        machine.apply(StreamCommand::DeleteBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::Error {
            code: StreamErrorCode::BucketNotEmpty,
            ..
        }
    ));
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("s-1"),
        }),
        StreamResponse::Deleted {
            hard_deleted: true,
            parent_to_release: None,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::DeleteBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketDeleted {
            bucket_id: "benchcmp".to_owned(),
        }
    );
}

#[test]
fn fork_refs_soft_delete_and_release_parent_on_last_child() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "source");
    create_stream(&mut machine, "fork");

    assert_eq!(
        machine.apply(StreamCommand::AddForkRef {
            stream_id: stream("source"),
            now_ms: 0,
        }),
        StreamResponse::ForkRefAdded { fork_ref_count: 1 }
    );
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("source"),
        }),
        StreamResponse::Deleted {
            hard_deleted: false,
            parent_to_release: None,
        }
    );
    assert!(matches!(
        machine.read_plan(&stream("source"), 0, 1),
        Err(StreamResponse::Error {
            code: StreamErrorCode::StreamGone,
            ..
        })
    ));
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: stream("fork"),
        }),
        StreamResponse::Deleted {
            hard_deleted: true,
            parent_to_release: None,
        }
    );
    assert_eq!(
        machine.apply(StreamCommand::ReleaseForkRef {
            stream_id: stream("source"),
        }),
        StreamResponse::ForkRefReleased {
            hard_deleted: true,
            fork_ref_count: 0,
            parent_to_release: None,
        }
    );
    assert!(machine.head(&stream("source")).is_none());
}

#[test]
fn publish_snapshot_advances_retention_on_message_boundary() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "snap");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("snap"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 0,
            next_offset: 3,
            ..
        }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("snap"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"de".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended {
            offset: 3,
            next_offset: 5,
            ..
        }
    ));

    assert_eq!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream("snap"),
            snapshot_offset: 3,
            content_type: "application/json".to_owned(),
            payload: br#"{"state":"abc"}"#.to_vec(),
            now_ms: 0,
        }),
        StreamResponse::SnapshotPublished { snapshot_offset: 3 }
    );
    assert!(matches!(
        machine.read_plan(&stream("snap"), 0, 1),
        Err(StreamResponse::Error {
            code: StreamErrorCode::StreamGone,
            next_offset: Some(3),
            ..
        })
    ));
    let read = machine.read(&stream("snap"), 3, 2).expect("retained read");
    assert_eq!(read.payload, b"de");
    let snapshot = machine
        .read_snapshot(&stream("snap"), 3)
        .expect("visible snapshot");
    assert_eq!(snapshot.content_type, "application/json");
    assert_eq!(snapshot.payload, br#"{"state":"abc"}"#);
    let bootstrap = machine.bootstrap_plan(&stream("snap")).expect("bootstrap");
    assert_eq!(
        bootstrap.snapshot.as_ref().map(|snapshot| snapshot.offset),
        Some(3)
    );
    assert_eq!(
        bootstrap.updates,
        vec![StreamMessageRecord {
            start_offset: 3,
            end_offset: 5,
        }]
    );
}

#[test]
fn publish_snapshot_rejects_unaligned_offset() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "unaligned");
    assert!(matches!(
        machine.apply(StreamCommand::Append {
            stream_id: stream("unaligned"),
            content_type: Some("application/octet-stream".to_owned()),
            payload: b"abc".to_vec(),
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms: 0,
        }),
        StreamResponse::Appended { .. }
    ));
    assert!(matches!(
        machine.apply(StreamCommand::PublishSnapshot {
            stream_id: stream("unaligned"),
            snapshot_offset: 2,
            content_type: "application/octet-stream".to_owned(),
            payload: b"ab".to_vec(),
            now_ms: 0,
        }),
        StreamResponse::Error {
            code: StreamErrorCode::InvalidSnapshot,
            next_offset: Some(3),
            ..
        }
    ));
}

#[test]
fn snapshot_restore_preserves_visible_snapshot_and_message_records() {
    let mut machine = StreamStateMachine::new();
    create_bucket(&mut machine);
    create_stream(&mut machine, "restore-snap");
    let _ = machine.apply(StreamCommand::Append {
        stream_id: stream("restore-snap"),
        content_type: Some("application/octet-stream".to_owned()),
        payload: b"abc".to_vec(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
    });
    let _ = machine.apply(StreamCommand::Append {
        stream_id: stream("restore-snap"),
        content_type: Some("application/octet-stream".to_owned()),
        payload: b"de".to_vec(),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 0,
    });
    let _ = machine.apply(StreamCommand::PublishSnapshot {
        stream_id: stream("restore-snap"),
        snapshot_offset: 3,
        content_type: "application/octet-stream".to_owned(),
        payload: b"abc-state".to_vec(),
        now_ms: 0,
    });

    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(
        restored
            .read_snapshot(&stream("restore-snap"), 3)
            .expect("snapshot")
            .payload,
        b"abc-state"
    );
    assert_eq!(
        restored
            .bootstrap_plan(&stream("restore-snap"))
            .expect("bootstrap")
            .updates,
        vec![StreamMessageRecord {
            start_offset: 3,
            end_offset: 5,
        }]
    );
}
