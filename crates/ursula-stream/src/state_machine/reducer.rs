//! Atomic materialized reducer transitions.

use bytes::Bytes;

use super::AppendStreamInput;
use super::BucketStreamId;
use super::CreateStreamInput;
use super::ReducerState;
use super::StreamErrorCode;
use super::StreamResponse;
use super::StreamStateMachine;

impl StreamStateMachine {
    pub(super) fn apply_reduction(
        &mut self,
        stream_id: BucketStreamId,
        module_id: String,
        expected_version: u64,
        create_if_missing: bool,
        state: Vec<u8>,
        payload: Bytes,
        now_ms: u64,
    ) -> StreamResponse {
        if let Err(response) = self.validate_stream_scope(&stream_id) {
            return response;
        }
        let Some(slot) = self.stream_slot(&stream_id) else {
            if create_if_missing && expected_version == 0 {
                return self.create_reduced_stream(stream_id, module_id, state, payload, now_ms);
            }
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        let content_type = slot.metadata.content_type.clone();
        let current_version = match slot.reducer_state.as_ref() {
            Some(current) if current.module_id != module_id => {
                return StreamResponse::error(
                    StreamErrorCode::ReducerModuleMismatch,
                    format!(
                        "stream '{stream_id}' reducer module is '{}', not '{module_id}'",
                        current.module_id
                    ),
                );
            }
            Some(current) => current.version,
            None => 0,
        };
        if current_version != expected_version {
            return StreamResponse::error(
                StreamErrorCode::ReducerVersionMismatch,
                format!(
                    "stream '{stream_id}' reducer version is {current_version}, expected {expected_version}"
                ),
            );
        }

        let response = self.append_borrowed(AppendStreamInput {
            stream_id: stream_id.clone(),
            content_type: Some(&content_type),
            payload: &payload,
            close_after: false,
            stream_seq: None,
            producer: None,
            now_ms,
            record_match: None,
        });
        let StreamResponse::Appended {
            offset,
            next_offset,
            deduplicated: false,
            ..
        } = response
        else {
            return response;
        };

        let Some(reducer_version) = current_version.checked_add(1) else {
            return StreamResponse::error(
                StreamErrorCode::ReducerVersionMismatch,
                format!("stream '{stream_id}' reducer version is exhausted"),
            );
        };
        let Some(slot) = self.stream_slot_mut(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        slot.reducer_state = Some(ReducerState {
            module_id,
            version: reducer_version,
            value: state,
        });
        StreamResponse::Reduced {
            offset,
            next_offset,
            reducer_version,
        }
    }

    fn create_reduced_stream(
        &mut self,
        stream_id: BucketStreamId,
        module_id: String,
        state: Vec<u8>,
        payload: Bytes,
        now_ms: u64,
    ) -> StreamResponse {
        let Ok(record_ends) =
            crate::record_index::canonical_json_record_ends("application/json", &payload)
        else {
            return StreamResponse::error(
                StreamErrorCode::InvalidRecordBoundaries,
                "reducer output must use canonical JSON newline boundaries",
            );
        };
        let next_offset = u64::try_from(payload.len()).unwrap_or(u64::MAX);
        let response = self.create_stream(CreateStreamInput {
            stream_id: stream_id.clone(),
            content_type: "application/json".to_owned(),
            initial_payload: payload.into(),
            record_ends,
            close_after: false,
            stream_seq: None,
            producer: None,
            stream_ttl_seconds: None,
            stream_expires_at_ms: None,
            attrs: None,
            now_ms,
        });
        if !matches!(response, StreamResponse::Created { .. }) {
            return response;
        }
        let Some(slot) = self.stream_slot_mut(&stream_id) else {
            return StreamResponse::error(
                StreamErrorCode::StreamNotFound,
                format!("stream '{stream_id}' does not exist"),
            );
        };
        slot.reducer_state = Some(ReducerState {
            module_id,
            version: 1,
            value: state,
        });
        StreamResponse::Reduced {
            offset: 0,
            next_offset,
            reducer_version: 1,
        }
    }
}
