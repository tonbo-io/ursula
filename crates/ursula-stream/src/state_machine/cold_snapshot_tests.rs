//! Bounded-state F16 (feature level 5): visible snapshot bodies held as
//! cold-tier objects. State keeps the reference; superseded, unreferenced
//! and deleted bodies are queued for cold GC.

use super::*;
use crate::feature::FEATURE_LEVEL_COLD_SNAPSHOTS;
use crate::feature::FEATURE_LEVEL_HOT_REPRESENTATION;

const BUCKET: &str = "f16snapshots";
const OCTET: &str = "application/octet-stream";

fn stream() -> BucketStreamId {
    BucketStreamId::new(BUCKET, "s")
}

fn machine_at(level: u32) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    machine.apply(StreamCommand::CreateBucket {
        bucket_id: BUCKET.to_owned(),
    });
    machine.apply(StreamCommand::SetFeatureLevel { level });
    let response = machine.apply(StreamCommand::CreateStream {
        stream_id: stream(),
        content_type: OCTET.to_owned(),
        initial_payload: bytes::Bytes::from_static(b"ab"),
        close_after: false,
        stream_seq: None,
        producer: None,
        stream_ttl_seconds: None,
        stream_expires_at_ms: None,
        now_ms: 1,
    });
    assert!(
        matches!(response, StreamResponse::Created { .. }),
        "{response:?}"
    );
    machine
}

fn publish_cold(
    machine: &mut StreamStateMachine,
    offset: u64,
    path: &str,
    digest: &str,
) -> StreamResponse {
    machine.apply(StreamCommand::PublishSnapshotExternal {
        stream_id: stream(),
        snapshot_offset: offset,
        content_type: OCTET.to_owned(),
        object: ExternalPayloadRef {
            s3_path: path.to_owned(),
            payload_len: 64 << 20,
            object_size: 64 << 20,
        },
        digest: digest.to_owned(),
        now_ms: 10,
    })
}

fn gc_paths(machine: &StreamStateMachine) -> Vec<(String, u64)> {
    machine
        .pending_cold_gc_batch(64)
        .into_iter()
        .flat_map(|entry| match entry.target {
            ColdGcTarget::Paths(paths) => paths
                .into_iter()
                .map(|path| (path, entry.not_before_ms))
                .collect(),
            ColdGcTarget::Stream(_) => Vec::new(),
        })
        .collect()
}

#[test]
fn cold_snapshot_publish_requires_level_five() {
    let mut machine = machine_at(FEATURE_LEVEL_HOT_REPRESENTATION);
    let response = publish_cold(&mut machine, 2, "s/external/a.bin", "da");
    assert!(
        matches!(response, StreamResponse::Error {
            code: StreamErrorCode::FeatureNotEnabled,
            ..
        }),
        "{response:?}"
    );
    assert_eq!(machine.latest_snapshot(&stream()), Ok(None));
}

#[test]
fn cold_snapshot_keeps_a_reference_and_queues_unreferenced_bodies() {
    let mut machine = machine_at(FEATURE_LEVEL_COLD_SNAPSHOTS);
    let response = publish_cold(&mut machine, 2, "s/external/a.bin", "da");
    assert!(
        matches!(response, StreamResponse::SnapshotPublished { ref snapshot_digest, .. } if snapshot_digest == "da"),
        "{response:?}"
    );
    let visible = machine
        .latest_snapshot(&stream())
        .expect("stream")
        .expect("snapshot");
    assert!(visible.payload.is_empty());
    assert_eq!(
        visible
            .object
            .as_ref()
            .map(|object| object.s3_path.as_str()),
        Some("s/external/a.bin")
    );
    assert!(
        machine
            .stream_referenced_cold_paths(&stream())
            .contains(&"s/external/a.bin".to_owned())
    );

    // A group snapshot carries the reference.
    let restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
    assert_eq!(restored.latest_snapshot(&stream()), Ok(Some(visible)));

    // An idempotent retry staged its own copy, which nothing references.
    publish_cold(&mut machine, 2, "s/external/b.bin", "da");
    let grace = 10 + super::cold::RETENTION_COLD_GC_GRACE_MS;
    assert_eq!(gc_paths(&machine), vec![(
        "s/external/b.bin".to_owned(),
        grace
    )]);

    // A newer snapshot supersedes the body after the grace.
    machine.apply(StreamCommand::Append {
        stream_id: stream(),
        content_type: Some(OCTET.to_owned()),
        payload: bytes::Bytes::from_static(b"cd"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms: 11,
        record_match: None,
    });
    publish_cold(&mut machine, 4, "s/external/c.bin", "dc");
    assert_eq!(gc_paths(&machine), vec![
        ("s/external/b.bin".to_owned(), grace),
        ("s/external/a.bin".to_owned(), grace),
    ]);

    // Deleting the stream releases the visible body.
    machine.apply(StreamCommand::DeleteStream {
        stream_id: stream(),
    });
    assert!(
        gc_paths(&machine)
            .iter()
            .any(|(path, _)| path == "s/external/c.bin")
    );
}
