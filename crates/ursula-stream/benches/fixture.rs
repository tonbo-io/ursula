use ursula_shard::BucketStreamId;
use ursula_stream::ColdChunkRef;
use ursula_stream::StreamCommand;
use ursula_stream::StreamResponse;
use ursula_stream::StreamStateMachine;

pub const CONTENT_TYPE: &str = "application/octet-stream";
pub const STREAMS: usize = 128;
pub const APPENDS_PER_STREAM: usize = 64;
pub const PAYLOAD_BYTES: usize = 256;
pub const MAX_FLUSH_BYTES: usize = 1024;
pub const MAX_CANDIDATES: usize = 16;
pub const MIN_HOT_BYTES: usize = 1;

#[derive(Debug, Clone, Copy)]
pub enum FlushScenario {
    HotOnly,
    HalfCold,
    ManyStreams,
}

impl FlushScenario {
    pub fn name(self) -> &'static str {
        match self {
            Self::HotOnly => "one_stream_hot",
            Self::HalfCold => "one_stream_half_cold",
            Self::ManyStreams => "many_streams_hot",
        }
    }

    pub fn stream_count(self) -> usize {
        match self {
            Self::ManyStreams => STREAMS,
            Self::HotOnly | Self::HalfCold => 1,
        }
    }
}

pub fn build_state(scenario: FlushScenario) -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    assert!(matches!(
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: "benchcmp".to_owned(),
        }),
        StreamResponse::BucketCreated { .. }
    ));

    for stream_index in 0..scenario.stream_count() {
        let stream_id = BucketStreamId::new("benchcmp", format!("flush-{stream_index}"));
        assert!(matches!(
            machine.apply(StreamCommand::CreateStream {
                stream_id: stream_id.clone(),
                content_type: CONTENT_TYPE.to_owned(),
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

        let payload =
            vec![u8::try_from(stream_index % 251).expect("payload byte fits u8"); PAYLOAD_BYTES];
        for _ in 0..APPENDS_PER_STREAM {
            assert!(matches!(
                machine.apply(StreamCommand::Append {
                    stream_id: stream_id.clone(),
                    content_type: Some(CONTENT_TYPE.to_owned()),
                    payload: payload.clone(),
                    close_after: false,
                    stream_seq: None,
                    producer: None,
                    now_ms: 0,
                }),
                StreamResponse::Appended { .. }
            ));
        }

        if matches!(scenario, FlushScenario::HalfCold) {
            let cold_bytes = u64::try_from((APPENDS_PER_STREAM / 2) * PAYLOAD_BYTES)
                .expect("cold bytes fit u64");
            assert!(matches!(
                machine.apply(StreamCommand::FlushCold {
                    stream_id: stream_id.clone(),
                    chunk: ColdChunkRef {
                        start_offset: 0,
                        end_offset: cold_bytes,
                        s3_path: format!("{stream_id}/chunks/000000.bin"),
                        object_size: cold_bytes,
                    },
                }),
                StreamResponse::ColdFlushed { .. }
            ));
        }
    }

    machine
}
