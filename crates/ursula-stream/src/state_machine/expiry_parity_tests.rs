//! #669: the background expiry sweep proposes only the
//! `TouchStreamAccess { renew_ttl: false }` a read of an expired stream
//! already proposed in 0.8.0, and a 0.8.1 replica applies it as a 0.8.0
//! replica does. The fixed state and the command below were applied by the
//! state machine of tag `v0.8.0`, and [`EXPECTED_DIGEST`] is the BLAKE3 of
//! the MessagePack of its snapshot after the command: this file ran at
//! `v0.8.0` on CI, with the constant unset, and printed it. The same command
//! applied to the restored state
//! gives the same snapshot, so a replica that restored and one that replayed
//! agree too.

use ursula_shard::BucketStreamId;

use crate::command::StreamCommand;
use crate::model::ColdChunkRef;
use crate::response::StreamResponse;
use crate::state_machine::StreamStateMachine;

/// The digest the state machine of `v0.8.0` gives.
const EXPECTED_DIGEST: &str = "7e61560273a1e7a1dd0f24aa0c6228235d3e4636e7219579d27fa730618534ff";

/// The sweep's clock: past two of the three sliding TTLs and part of the
/// absolute expiries, so more streams are due than one command removes.
const SWEEP_NOW_MS: u64 = 10_500;

fn apply(machine: &mut StreamStateMachine, command: StreamCommand) -> StreamResponse {
    machine.apply(command)
}

fn create(
    stream_id: BucketStreamId,
    ttl: Option<u64>,
    at: Option<u64>,
    now_ms: u64,
) -> StreamCommand {
    StreamCommand::CreateStream {
        stream_id,
        content_type: "application/octet-stream".to_owned(),
        initial_payload: bytes::Bytes::new(),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: ttl,
        stream_expires_at_ms: at,
        now_ms,
    }
}

fn append(stream_id: BucketStreamId, payload: &[u8], now_ms: u64) -> StreamCommand {
    StreamCommand::Append {
        stream_id,
        content_type: Some("application/octet-stream".to_owned()),
        payload: bytes::Bytes::copy_from_slice(payload),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
    }
}

fn id(index: usize) -> BucketStreamId {
    let bucket = if index.is_multiple_of(7) {
        "other"
    } else {
        "tenant"
    };
    BucketStreamId::new(bucket, format!("s-{index:03}"))
}

/// A few hundred streams with sliding and absolute TTLs, ties in expiry, a
/// renewed stream, a stream deleted and recreated, hot bytes and cold refs.
fn parity_state() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    for bucket in ["other", "tenant"] {
        apply(&mut machine, StreamCommand::CreateBucket {
            bucket_id: bucket.to_owned(),
        });
    }
    for index in 0..600_usize {
        let offset = u64::try_from(index % 4).expect("small").saturating_mul(500);
        let (ttl, at) = match index % 5 {
            0 => (
                Some(u64::try_from(index % 3).expect("small").saturating_add(8)),
                None,
            ),
            1 => (None, Some(offset.saturating_add(10_000))),
            2 => (None, Some(10_000)),
            3 => (Some(60), None),
            _ => (None, None),
        };
        apply(&mut machine, create(id(index), ttl, at, 1_000));
        apply(
            &mut machine,
            append(id(index), format!("record-{index};").as_bytes(), 1_500),
        );
    }
    // A sliding stream renewed: its TTL entry goes stale.
    apply(&mut machine, StreamCommand::TouchStreamAccess {
        stream_id: id(5),
        now_ms: 4_000,
        renew_ttl: true,
    });
    // A stream deleted and recreated with a later expiry.
    apply(&mut machine, StreamCommand::DeleteStream {
        stream_id: id(10),
    });
    apply(&mut machine, create(id(10), None, Some(11_000), 2_000));
    // Cold refs on a live stream and on an expiring one.
    for index in [4_usize, 1] {
        let generation = machine
            .cold_index_generation(&id(index))
            .unwrap_or_default();
        let end = u64::try_from(format!("record-{index};").len()).expect("small");
        apply(&mut machine, StreamCommand::FlushCold {
            stream_id: id(index),
            chunk: ColdChunkRef {
                start_offset: 0,
                end_offset: end,
                s3_path: format!("s3://cold/{index:03}/000000"),
                object_size: end,
                object_offset: 0,
                shared_object: false,
                payload_digest: String::new(),
            },
            cold_generation: generation,
        });
    }
    machine
}

/// The sweep's command for the fixed stream, an expired one in the TTL
/// index's first expiry.
fn sweep() -> StreamCommand {
    StreamCommand::TouchStreamAccess {
        stream_id: id(1),
        now_ms: SWEEP_NOW_MS,
        renew_ttl: false,
    }
}

fn digest(machine: &StreamStateMachine) -> String {
    let encoded = rmp_serde::to_vec_named(&machine.snapshot()).expect("encode the snapshot");
    blake3::hash(&encoded).to_hex().to_string()
}

#[test]
fn expiry_sweep_applies_as_0_8_0_did() {
    let mut replayed = parity_state();
    let mut restored =
        StreamStateMachine::restore(replayed.snapshot()).expect("restore the snapshot");
    let before = replayed.snapshot().streams.len();
    let response = apply(&mut replayed, sweep());
    assert_eq!(response, StreamResponse::Accessed {
        changed: true,
        expired: true,
    });
    assert_eq!(apply(&mut restored, sweep()), response);
    // The named stream and the sweep's cap of others went.
    assert_eq!(
        before.saturating_sub(replayed.snapshot().streams.len()),
        257
    );
    assert_eq!(digest(&restored), digest(&replayed));
    assert_eq!(digest(&replayed), EXPECTED_DIGEST, "the 0.8.0 digest");
}
