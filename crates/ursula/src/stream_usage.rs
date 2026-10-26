//! `POST /__ursula/usage/streams`, the per-stream usage export on the admin
//! listener (bounded-state F20): one page of each requested group this node
//! hosts, read from local applied state, leader or follower.
//!
//! A read, so it takes no process-incarnation precondition. It names every
//! tenant's streams, so no other listener serves it. A group that cannot be
//! read answers `unavailable` with why, and never fails the answer: only a
//! runtime that does not serve at all answers `503`.

use std::num::NonZeroUsize;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header::RETRY_AFTER;
use axum::response::IntoResponse;
use axum::response::Response;
use ursula_proto::admin::BucketUsageRow;
use ursula_proto::admin::CommittedUsageReport;
use ursula_proto::admin::GroupStreamUsage;
use ursula_proto::admin::GroupUsagePage;
use ursula_proto::admin::HistoryId;
use ursula_proto::admin::MAX_STREAM_USAGE_ANSWER_ITEMS;
use ursula_proto::admin::RelistReason;
use ursula_proto::admin::SchemaVersion;
use ursula_proto::admin::StreamUsageEnd;
use ursula_proto::admin::StreamUsageEntry;
use ursula_proto::admin::StreamUsageReport;
use ursula_proto::admin::StreamUsageRequest;
use ursula_proto::admin::UsageCursor;
use ursula_proto::admin::UsageHistory;
use ursula_proto::admin::UsageLogId;
use ursula_proto::admin::UsagePage;
use ursula_proto::admin::UsagePosition;
use ursula_proto::admin::UsageUnavailable;
use ursula_proto::admin::VersionCountersReport;
use ursula_proto::admin::VersionLifecycle;
use ursula_runtime::GroupEngineError;
use ursula_runtime::GroupInfraError;
use ursula_shard::RaftGroupId;
use ursula_stream::GroupUsageView;
use ursula_stream::LastWrite;
use ursula_stream::Lifecycle;
use ursula_stream::StreamEnd;
use ursula_stream::StreamUsageRead;
use ursula_stream::UsageListing;
use ursula_stream::VersionCounters;

use crate::HttpState;

/// `POST /__ursula/usage/streams`.
pub(crate) async fn stream_usage(
    State(state): State<HttpState>,
    Json(request): Json<StreamUsageRequest>,
) -> Response {
    let raft_group_count = state.runtime.raft_group_count();
    if let Some(group) = request
        .groups
        .as_slice()
        .iter()
        .find(|group| group.raft_group_id >= raft_group_count)
    {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "group {} is at or above raft_group_count {raft_group_count}",
                group.raft_group_id
            ),
        )
            .into_response();
    }
    // At most `MAX_STREAM_USAGE_GROUP_ENTRIES`, so it fits every `usize`.
    let limit = NonZeroUsize::try_from(request.limit.get()).unwrap_or(NonZeroUsize::MAX);
    let reads = request
        .groups
        .into_inner()
        .into_iter()
        .map(|group| {
            (RaftGroupId(group.raft_group_id), StreamUsageRead {
                cursor: cursor(group.cursor),
                limit,
            })
        })
        .collect();
    let max_items = usize::try_from(MAX_STREAM_USAGE_ANSWER_ITEMS).unwrap_or(usize::MAX);
    match state.runtime.stream_usage_groups(reads, max_items).await {
        Ok(answer) => Json(StreamUsageReport {
            version: SchemaVersion,
            node_id: state.node_id(),
            write_unit_bytes: ursula_runtime::COMMITTED_WRITE_UNIT_BYTES,
            end_record_retention_ms: state.end_record_retention_ms,
            raft_group_count,
            groups: answer
                .groups
                .into_iter()
                .map(|(group, view)| group_answer(group, view))
                .collect(),
            omitted: answer.omitted.into_iter().map(|group| group.0).collect(),
        })
        .into_response(),
        Err(err) => (
            StatusCode::SERVICE_UNAVAILABLE,
            [(RETRY_AFTER, "1")],
            format!("the node's runtime does not serve usage reads: {err}"),
        )
            .into_response(),
    }
}

fn cursor(cursor: UsageCursor) -> ursula_stream::UsageCursor {
    match cursor {
        UsageCursor::Start => ursula_stream::UsageCursor::Start,
        UsageCursor::After {
            genesis,
            history,
            applied,
            retired_through,
            position,
        } => ursula_stream::UsageCursor::After {
            genesis: genesis.get(),
            history: history.get(),
            applied: ursula_stream::UsageLogId {
                term: applied.term,
                index: applied.index,
            },
            retired_through,
            position: match position {
                UsagePosition::Rows { after_bucket } => {
                    ursula_stream::UsagePosition::Rows { after_bucket }
                }
                UsagePosition::Entries { after_seq } => {
                    ursula_stream::UsagePosition::Entries { after_seq }
                }
            },
        },
    }
}

/// One group's answer: its page, `behind`, or why it is unavailable.
fn group_answer(
    group: RaftGroupId,
    view: Result<GroupUsageView, GroupEngineError>,
) -> GroupStreamUsage {
    match view {
        Ok(GroupUsageView::Listed(listing)) => GroupStreamUsage::Listed(page(group, listing)),
        Ok(GroupUsageView::Behind {
            genesis,
            history,
            applied,
        }) => GroupStreamUsage::Behind {
            raft_group_id: group.0,
            genesis: HistoryId::new(genesis),
            history: HistoryId::new(history),
            applied: log_id(applied),
        },
        Ok(GroupUsageView::NoHistory) => GroupStreamUsage::Unavailable {
            raft_group_id: group.0,
            reason: UsageUnavailable::NoHistory,
        },
        Err(error) => GroupStreamUsage::Unavailable {
            raft_group_id: group.0,
            reason: usage_unavailable(group, &error),
        },
    }
}

/// Why a group's engine refused the read, as the export reports it. A
/// refusal the caller cannot act on but by asking another node is `other`,
/// logged here so the operator sees what it was.
fn usage_unavailable(group: RaftGroupId, error: &GroupEngineError) -> UsageUnavailable {
    match error {
        GroupEngineError::Infra(GroupInfraError::ApplyStopped { .. }) => {
            UsageUnavailable::ApplyStopped
        }
        GroupEngineError::Infra(GroupInfraError::RecoveryPending { .. }) => {
            UsageUnavailable::RecoveryPending
        }
        GroupEngineError::Infra(GroupInfraError::OwnerStopped) => UsageUnavailable::RaftStopped,
        other => {
            tracing::warn!(
                raft_group_id = group.0,
                error = %other,
                "per-stream usage read refused"
            );
            UsageUnavailable::Other
        }
    }
}

fn page(group: RaftGroupId, listing: UsageListing) -> GroupUsagePage {
    GroupUsagePage {
        raft_group_id: group.0,
        genesis: HistoryId::new(listing.genesis),
        history: UsageHistory {
            id: HistoryId::new(listing.history.id),
            event_index: listing.history.event_index,
            previous: std::num::NonZeroU64::new(listing.history.previous).map(HistoryId::new),
        },
        applied: log_id(listing.applied),
        usage_seq: listing.usage_seq,
        retired_through: listing.retired_through,
        live_versions: listing.live_versions,
        end_records: listing.end_records,
        relist: listing.relist.map(|reason| match reason {
            ursula_stream::RelistReason::Start => RelistReason::Start,
            ursula_stream::RelistReason::Requested => RelistReason::Requested,
            ursula_stream::RelistReason::Retired => RelistReason::Retired,
            ursula_stream::RelistReason::History => RelistReason::History,
        }),
        rows: listing
            .rows
            .into_iter()
            .map(|row| BucketUsageRow {
                totals: crate::bucket_usage_report(&row.totals, row.snapshot_bytes),
                retired: CommittedUsageReport {
                    committed_append_bytes: row.retired.committed_append_bytes,
                    committed_records: row.retired.committed_records,
                    committed_write_units: row.retired.committed_write_units,
                },
                bucket_id: row.bucket_id,
            })
            .collect(),
        entries: listing.entries.into_iter().map(entry).collect(),
        page: match listing.page {
            ursula_stream::UsagePage::Complete => UsagePage::Complete,
            ursula_stream::UsagePage::More { next } => UsagePage::More {
                next: match next {
                    ursula_stream::UsagePosition::Rows { after_bucket } => {
                        UsagePosition::Rows { after_bucket }
                    }
                    ursula_stream::UsagePosition::Entries { after_seq } => {
                        UsagePosition::Entries { after_seq }
                    }
                },
            },
        },
    }
}

fn entry(entry: ursula_stream::UsageEntry) -> StreamUsageEntry {
    StreamUsageEntry {
        seq: entry.seq,
        bucket_id: entry.stream_id.bucket_id,
        stream_id: entry.stream_id.stream_id,
        created_at_ms: entry.created_at_ms,
        counted_since: HistoryId::new(entry.counted_since),
        incarnation: entry.incarnation,
        counters: counters(entry.counters),
        history_baseline: counters(entry.history_baseline),
        last_write_at_ms: match entry.last_write {
            LastWrite::At(at_ms) => Some(at_ms),
            LastWrite::Imported => None,
        },
        lifecycle: match entry.lifecycle {
            Lifecycle::Live {
                retained_bytes,
                snapshot_bytes,
            } => VersionLifecycle::Live {
                retained_bytes,
                snapshot_bytes,
            },
            Lifecycle::Ended { end, ended_at_ms } => VersionLifecycle::Ended {
                end: match end {
                    StreamEnd::Deleted => StreamUsageEnd::Deleted,
                    StreamEnd::Expired => StreamUsageEnd::Expired,
                    StreamEnd::Purged => StreamUsageEnd::Purged,
                },
                ended_at_ms,
            },
        },
    }
}

fn counters(counters: VersionCounters) -> VersionCountersReport {
    VersionCountersReport {
        committed_append_bytes: counters.committed.committed_append_bytes,
        committed_records: counters.committed.committed_records,
        committed_write_units: counters.committed.committed_write_units,
        appends: counters.appends,
    }
}

fn log_id(applied: ursula_stream::UsageLogId) -> UsageLogId {
    UsageLogId {
        term: applied.term,
        index: applied.index,
    }
}
