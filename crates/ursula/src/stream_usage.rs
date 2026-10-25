//! The per-stream usage export on the admin port (U-C): the changes of
//! each hosted group's usage ledger after a caller's cursor, and the
//! acknowledgement that folds ended streams into their buckets' retired
//! usage (U-B). Both serve only the admin listener; the acknowledgement, a
//! mutation, takes the observed process incarnation as every admin
//! mutation does.

use std::collections::BTreeMap;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_proto::admin::GroupStreamUsageAck;
use ursula_proto::admin::GroupStreamUsageReport;
use ursula_proto::admin::MAX_STREAM_USAGE_CHANGES;
use ursula_proto::admin::SchemaVersion;
use ursula_proto::admin::StreamUsageAckOutcome;
use ursula_proto::admin::StreamUsageAckReport;
use ursula_proto::admin::StreamUsageAckRequest;
use ursula_proto::admin::StreamUsageEnd;
use ursula_proto::admin::StreamUsageEntryReport;
use ursula_proto::admin::StreamUsageLedgerState;
use ursula_proto::admin::StreamUsageReport;
use ursula_proto::admin::StreamUsageRequest;
use ursula_shard::RaftGroupId;
use ursula_stream::StreamEnd;
use ursula_stream::StreamUsageChange;
use ursula_stream::UsageAck;
use ursula_stream::UsageLedgerProgress;

use crate::HttpState;

/// `POST /__ursula/usage/streams`.
pub(crate) async fn stream_usage(
    State(state): State<HttpState>,
    Json(request): Json<StreamUsageRequest>,
) -> Response {
    if request.limit == 0 || request.limit > MAX_STREAM_USAGE_CHANGES {
        return (
            StatusCode::BAD_REQUEST,
            format!("limit must be 1 to {MAX_STREAM_USAGE_CHANGES}"),
        )
            .into_response();
    }
    let cursors = request
        .cursors
        .iter()
        .map(|cursor| (cursor.raft_group_id, cursor.after_seq))
        .collect::<BTreeMap<_, _>>();
    let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
    match state.runtime.stream_usage_all_groups(&cursors, limit).await {
        Ok(groups) => Json(StreamUsageReport {
            version: SchemaVersion,
            node_id: state.node_id(),
            write_unit_bytes: ursula_runtime::COMMITTED_WRITE_UNIT_BYTES,
            raft_group_count: state.runtime.raft_group_count(),
            groups: groups
                .into_iter()
                .map(|(group, usage)| {
                    let after_seq = cursors.get(&group.0).copied().unwrap_or(0);
                    group_report(group, after_seq, usage)
                })
                .collect(),
        })
        .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("stream usage read failed: {err}"),
        )
            .into_response(),
    }
}

fn group_report(
    group: RaftGroupId,
    after_seq: u64,
    usage: ursula_runtime::GroupStreamUsage,
) -> GroupStreamUsageReport {
    let changes = usage.changes;
    let (ledger, complete) = match changes.progress {
        UsageLedgerProgress::NotStarted => (StreamUsageLedgerState::NotStarted, true),
        UsageLedgerProgress::Started {
            usage_seq,
            acked_through,
        } => (
            StreamUsageLedgerState::Started {
                usage_seq,
                acked_through,
            },
            changes.through == usage_seq,
        ),
    };
    GroupStreamUsageReport {
        raft_group_id: group.0,
        applied_index: usage.applied_index,
        history: changes.history,
        ledger,
        after_seq,
        through: changes.through,
        complete,
        streams: changes.changes.into_iter().map(entry_report).collect(),
    }
}

fn entry_report(change: StreamUsageChange) -> StreamUsageEntryReport {
    StreamUsageEntryReport {
        seq: change.usage.usage_seq,
        bucket_id: change.stream_id.bucket_id,
        stream_id: change.stream_id.stream_id,
        incarnation: change.incarnation,
        created_at_ms: change.created_at_ms,
        content_type: change.content_type,
        last_write_at_ms: change.usage.last_write_at_ms,
        committed_write_units: change.usage.committed_write_units,
        committed_append_bytes: change.usage.committed_append_bytes,
        committed_records: change.usage.committed_records,
        retained_bytes: change.retained_bytes,
        snapshot_bytes: change.snapshot_bytes,
        ended: change.end.map(|end| match end {
            StreamEnd::Deleted => StreamUsageEnd::Deleted,
            StreamEnd::Expired => StreamUsageEnd::Expired,
            StreamEnd::Purged => StreamUsageEnd::Purged,
        }),
    }
}

/// `POST /__ursula/usage/streams/ack`: each group's acknowledgement goes
/// through the group's leader, from a replica this node hosts. One
/// outcome per group, in request order.
pub(crate) async fn ack_stream_usage(
    State(state): State<HttpState>,
    Json(request): Json<StreamUsageAckRequest>,
) -> Response {
    let mut groups = Vec::with_capacity(request.groups.len());
    for ack in request.groups {
        let outcome = match state
            .runtime
            .ack_stream_usage_if_hosted(
                RaftGroupId(ack.raft_group_id),
                ack.history,
                ack.through_seq,
            )
            .await
        {
            Ok(None) => StreamUsageAckOutcome::NotHosted,
            Ok(Some(response)) => match response.ack {
                UsageAck::Started { usage_seq } => StreamUsageAckOutcome::Started { usage_seq },
                UsageAck::Acknowledged {
                    acked_through,
                    folded,
                } => StreamUsageAckOutcome::Acknowledged {
                    acked_through,
                    folded,
                },
                UsageAck::OtherHistory { history } => {
                    StreamUsageAckOutcome::OtherHistory { history }
                }
            },
            Err(err) => StreamUsageAckOutcome::Failed {
                error: err.to_string(),
            },
        };
        groups.push(GroupStreamUsageAck {
            raft_group_id: ack.raft_group_id,
            outcome,
        });
    }
    Json(StreamUsageAckReport {
        version: SchemaVersion,
        groups,
    })
    .into_response()
}
