//! The usage ledger (F20) against an oracle. A generated run of creates,
//! inline and external appends, producer repeats, closes, publishes,
//! retentions, deletes, touches, expiry, a bucket purge, sweeps that fold
//! at a moving watermark, history events, an import and group snapshot
//! round trips drives one group. The oracle models the ledger on its own:
//! the counting rule applied to each command and its answer, its own TTL
//! clock (a stream expires when the run's clock jumps past its TTL), how
//! each removal ends a version (`Deleted` unless it had expired, `Expired`,
//! `Purged` at the purge's `now_ms`), the history each version counts and
//! ends in, the usage seq each stamp takes, the sweep's fold and the
//! end-record cap. After every step the whole ledger must equal the
//! oracle's: each bucket's totals and retired usage, each live version's
//! usage, every end record, the seq, the fold watermark and the genesis.
//! The ledger invariants L1 to L8 hold, and a round trip through
//! `usage_snapshot` and `restore_group` reproduces the ledger whatever the
//! order of the streams. A second property runs at a cap of three end
//! records, so removals fold at the cap.
//!
//! The export's read is checked against the same oracle after every step:
//! the node-local index of live versions holds exactly each live version at
//! its seq (L1), a relisting lists every bucket's row and every live version
//! and end record at its newest seq, the pages of a listing at any limit
//! join to the same listing, and a consumer that keeps its cursor across
//! the steps is continued with the entries above its listing, or relisted
//! for exactly the reason the cursor rule gives.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::num::NonZeroU64;
use std::num::NonZeroUsize;

use bytes::Bytes;
use proptest::prelude::*;

use super::BucketStreamId;
use super::MAX_END_RECORDS;
use super::ProducerRequest;
use super::StreamCommand;
use super::StreamResponse;
use super::StreamStateMachine;
use super::compare_stream_ids;
use super::usage_ledger::test_cap;
use crate::history::incarnation_token;
use crate::model::BucketUsage;
use crate::model::ExternalPayloadRef;
use crate::usage::BucketRetired;
use crate::usage::BucketUsageRow;
use crate::usage::CommittedUsage;
use crate::usage::EndRecord;
use crate::usage::GroupUsageView;
use crate::usage::LastWrite;
use crate::usage::Lifecycle;
use crate::usage::RelistReason;
use crate::usage::StreamEnd;
use crate::usage::StreamUsageRead;
use crate::usage::UsageCursor;
use crate::usage::UsageEntry;
use crate::usage::UsageListing;
use crate::usage::UsageLogId;
use crate::usage::UsagePage;
use crate::usage::UsagePosition;
use crate::usage::VersionCounters;

const OCTET: &str = "application/octet-stream";
const BUCKETS: [&str; 2] = ["usage-a", "usage-b"];
/// The bucket the purge step purges.
const PURGED: &str = "usage-b";
const STREAMS: usize = 6;
const GENESIS: u64 = 0x0007;
/// The TTL of a stream created with one, in ms.
const TTL_MS: u64 = 1_000;
/// How far an `Expire` step moves the clock: past every TTL. Every other
/// step moves it 1 ms, so a run never reaches a TTL without it.
const EXPIRE_JUMP_MS: u64 = 5_000;
/// The streams a `Sweep` removes at most, and the end records it folds.
const SWEEP_LIMIT: u32 = 4;
/// [`SWEEP_LIMIT`] as a count.
const SWEEP_LIMIT_COUNT: usize = 4;
/// The end-record cap of the second property.
const SMALL_CAP: usize = 3;

#[derive(Debug, Clone)]
enum Op {
    Create {
        stream: usize,
        len: u64,
        external: bool,
        ttl: bool,
    },
    Append {
        stream: usize,
        len: u64,
        external: bool,
    },
    /// A producer write: the next seq, or the last applied one again.
    ProducerAppend {
        stream: usize,
        len: u64,
        repeat: bool,
    },
    Close {
        stream: usize,
    },
    Publish {
        stream: usize,
    },
    Retain {
        stream: usize,
    },
    Delete {
        stream: usize,
    },
    Touch {
        stream: usize,
    },
    /// The clock passes every TTL.
    Expire,
    /// A sweep whose watermark is `quarters` fourths of the usage seq.
    Sweep {
        quarters: u64,
    },
    Purge,
    NewHistory,
    /// The group's snapshot imported as a backup into a fresh group.
    Import,
    /// The group's state replaced by its own group snapshot.
    RoundTrip,
}

fn op_strategy() -> impl Strategy<Value = Op> {
    let stream = 0..STREAMS;
    prop_oneof![
        3 => (stream.clone(), 0u64..20_000, any::<bool>(), any::<bool>()).prop_map(
            |(stream, len, external, ttl)| Op::Create {
                stream,
                len,
                external,
                ttl,
            }
        ),
        4 => (stream.clone(), 0u64..20_000, any::<bool>()).prop_map(|(stream, len, external)| {
            Op::Append {
                stream,
                len,
                external,
            }
        }),
        2 => (stream.clone(), 1u64..64, any::<bool>()).prop_map(|(stream, len, repeat)| {
            Op::ProducerAppend {
                stream,
                len,
                repeat,
            }
        }),
        1 => stream.clone().prop_map(|stream| Op::Close { stream }),
        1 => stream.clone().prop_map(|stream| Op::Publish { stream }),
        1 => stream.clone().prop_map(|stream| Op::Retain { stream }),
        2 => stream.clone().prop_map(|stream| Op::Delete { stream }),
        1 => stream.prop_map(|stream| Op::Touch { stream }),
        1 => Just(Op::Expire),
        2 => (0u64..=4).prop_map(|quarters| Op::Sweep { quarters }),
        1 => Just(Op::Purge),
        1 => Just(Op::NewHistory),
        1 => Just(Op::Import),
        1 => Just(Op::RoundTrip),
    ]
}

fn stream_id(stream: usize) -> BucketStreamId {
    BucketStreamId::new(
        BUCKETS[stream.rem_euclid(BUCKETS.len())],
        format!("s{stream}"),
    )
}

/// `len` bytes of `byte`.
fn filled(byte: u8, len: u64) -> Bytes {
    Bytes::from(vec![
        byte;
        usize::try_from(len).expect("a generated length fits")
    ])
}

fn non_zero(id: u64) -> NonZeroU64 {
    NonZeroU64::new(id).expect("a history id is not zero")
}

/// One live version as the oracle models it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Version {
    /// The incarnation apply chose (C7, not the ledger's).
    created_at_ms: u64,
    seq: u64,
    counters: VersionCounters,
    last_write: LastWrite,
    counted_since: u64,
    /// The last producer seq that applied, not as a repeat.
    producer_seq: Option<u64>,
    tail: u64,
    retained: u64,
    snapshot_offset: Option<u64>,
    /// The body length of its visible snapshot, 0 without one.
    snapshot_bytes: u64,
    closed: bool,
    /// Created with a TTL, renewed by its writes and touches.
    ttl: bool,
    last_touch_ms: u64,
    /// The clock jumped past its TTL since its last renewal: the next
    /// command that names it, or the sweep, removes it as expired.
    expired: bool,
}

/// What the ledger must hold, modelled apart from it.
#[derive(Debug)]
struct Oracle {
    cap: usize,
    genesis: u64,
    history: u64,
    usage_seq: u64,
    retired_through: u64,
    /// Every bucket's committed totals, by the counting rule.
    totals: BTreeMap<String, CommittedUsage>,
    retired: BTreeMap<String, CommittedUsage>,
    live: HashMap<BucketStreamId, Version>,
    ends: BTreeMap<u64, EndRecord>,
    /// The counters, at the group's latest history event, of each version
    /// live then. A version created or restored since has a zero baseline.
    at_event: HashMap<(BucketStreamId, u64), VersionCounters>,
}

impl Oracle {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            genesis: GENESIS,
            history: GENESIS,
            usage_seq: 0,
            retired_through: 0,
            totals: BTreeMap::new(),
            retired: BTreeMap::new(),
            live: HashMap::new(),
            ends: BTreeMap::new(),
            at_event: HashMap::new(),
        }
    }

    fn stamp(&mut self) -> u64 {
        self.usage_seq = self.usage_seq.saturating_add(1);
        self.usage_seq
    }

    fn version(&self, id: &BucketStreamId) -> Option<&Version> {
        self.live.get(id)
    }

    fn expired(&self, id: &BucketStreamId) -> bool {
        self.version(id).is_some_and(|version| version.expired)
    }

    /// A create answered `Created`.
    fn create(&mut self, id: &BucketStreamId, bytes: u64, ttl: bool, now_ms: u64, at: u64) {
        let write = CommittedUsage::of_write(bytes, u64::from(bytes > 0));
        self.totals
            .entry(id.bucket_id.clone())
            .or_default()
            .add(write);
        self.retired.entry(id.bucket_id.clone()).or_default();
        let seq = self.stamp();
        self.live.insert(id.clone(), Version {
            created_at_ms: at,
            seq,
            counters: VersionCounters {
                committed: write,
                appends: 0,
            },
            last_write: LastWrite::At(now_ms),
            counted_since: self.history,
            producer_seq: None,
            tail: bytes,
            retained: 0,
            snapshot_offset: None,
            snapshot_bytes: 0,
            closed: false,
            ttl,
            last_touch_ms: now_ms,
            expired: false,
        });
    }

    /// An append of `bytes` that applied, not as a repeat.
    fn append(&mut self, id: &BucketStreamId, bytes: u64, now_ms: u64) {
        let write = CommittedUsage::of_write(bytes, u64::from(bytes > 0));
        self.totals
            .entry(id.bucket_id.clone())
            .or_default()
            .add(write);
        let seq = self.stamp();
        let version = self.live.get_mut(id).expect("a live version");
        version.counters.committed.add(write);
        version.counters.appends = version.counters.appends.saturating_add(1);
        version.last_write = LastWrite::At(now_ms);
        version.seq = seq;
        version.tail = version.tail.saturating_add(bytes);
        version.last_touch_ms = version.last_touch_ms.max(now_ms);
    }

    /// A change of a version's retained or snapshot bytes: a new seq.
    fn restamp(&mut self, id: &BucketStreamId) {
        let seq = self.stamp();
        self.live.get_mut(id).expect("a live version").seq = seq;
    }

    /// Folds the lowest-seq end record into its bucket's retired usage.
    fn fold_first(&mut self) {
        let (seq, record) = self.ends.pop_first().expect("an end record to fold");
        self.retired
            .entry(record.stream_id.bucket_id.clone())
            .or_default()
            .add(record.counters.committed);
        self.retired_through = seq;
    }

    /// The version of `id` ends as `end` at `now_ms`, leaving its end
    /// record, after the cap folds the oldest one.
    fn end(&mut self, id: &BucketStreamId, end: StreamEnd, now_ms: u64) {
        let version = self.live.remove(id).expect("a live version ends");
        if self.ends.len() >= self.cap {
            self.fold_first();
        }
        let history_baseline = self
            .at_event
            .get(&(id.clone(), version.created_at_ms))
            .copied()
            .unwrap_or_default();
        let seq = self.stamp();
        self.ends.insert(seq, EndRecord {
            seq,
            stream_id: id.clone(),
            created_at_ms: version.created_at_ms,
            counted_since: non_zero(version.counted_since),
            ended_in: non_zero(self.history),
            end,
            ended_at_ms: now_ms,
            counters: version.counters,
            history_baseline,
            last_write: version.last_write,
        });
    }

    /// The live versions of `ids` that expired, in the TTL index's order:
    /// expiry, then bucket and stream.
    fn expired_in_ttl_order(&self) -> Vec<BucketStreamId> {
        let mut expired = self
            .live
            .iter()
            .filter(|(_, version)| version.expired)
            .map(|(id, version)| (version.last_touch_ms.saturating_add(TTL_MS), id.clone()))
            .collect::<Vec<_>>();
        expired.sort_by(|(left_at, left), (right_at, right)| {
            left_at
                .cmp(right_at)
                .then_with(|| compare_stream_ids(left, right))
        });
        expired.into_iter().map(|(_, id)| id).collect()
    }

    /// The baseline of the group's current history of the live version
    /// `id`: its counters at the latest event, zero when created or
    /// restored since.
    fn baseline_of(&self, id: &BucketStreamId, version: &Version) -> VersionCounters {
        self.at_event
            .get(&(id.clone(), version.created_at_ms))
            .copied()
            .unwrap_or_default()
    }

    /// Every bucket's row of a relisting, by bucket id: its retired usage,
    /// its totals by the counting rule and its live versions' gauges.
    fn rows(&self) -> Vec<BucketUsageRow> {
        self.retired
            .iter()
            .map(|(bucket_id, retired)| {
                let mut totals = BucketUsage::default();
                self.totals
                    .get(bucket_id)
                    .copied()
                    .unwrap_or_default()
                    .add_to(&mut totals);
                let mut snapshot_bytes = 0_u64;
                for (_, version) in self
                    .live
                    .iter()
                    .filter(|(id, _)| id.bucket_id == *bucket_id)
                {
                    totals.stream_count = totals.stream_count.saturating_add(1);
                    totals.retained_bytes = totals
                        .retained_bytes
                        .saturating_add(version.tail.saturating_sub(version.retained));
                    snapshot_bytes = snapshot_bytes.saturating_add(version.snapshot_bytes);
                }
                BucketUsageRow {
                    bucket_id: bucket_id.clone(),
                    retired: *retired,
                    totals,
                    snapshot_bytes,
                }
            })
            .collect()
    }

    /// Every live version and end record as the export lists it, by seq.
    fn entries(&self) -> Vec<UsageEntry> {
        let mut entries = self
            .live
            .iter()
            .map(|(id, version)| UsageEntry {
                seq: version.seq,
                stream_id: id.clone(),
                created_at_ms: version.created_at_ms,
                counted_since: non_zero(version.counted_since),
                incarnation: incarnation_token(version.created_at_ms, self.history),
                counters: version.counters,
                history_baseline: self.baseline_of(id, version),
                last_write: version.last_write,
                lifecycle: Lifecycle::Live {
                    retained_bytes: version.tail.saturating_sub(version.retained),
                    snapshot_bytes: version.snapshot_bytes,
                },
            })
            .chain(self.ends.values().map(|record| UsageEntry {
                seq: record.seq,
                stream_id: record.stream_id.clone(),
                created_at_ms: record.created_at_ms,
                counted_since: record.counted_since,
                incarnation: incarnation_token(record.created_at_ms, record.ended_in.get()),
                counters: record.counters,
                // Ended before the current history began: nothing changed
                // since, so its baseline there is its final usage.
                history_baseline: if record.ended_in.get() == self.history {
                    record.history_baseline
                } else {
                    record.counters
                },
                last_write: record.last_write,
                lifecycle: Lifecycle::Ended {
                    end: record.end,
                    ended_at_ms: record.ended_at_ms,
                },
            }))
            .collect::<Vec<_>>();
        entries.sort_by_key(|entry| entry.seq);
        entries
    }

    /// The bucket's live versions in stream-id order.
    fn in_bucket(&self, bucket_id: &str) -> Vec<BucketStreamId> {
        let mut ids = self
            .live
            .keys()
            .filter(|id| id.bucket_id == bucket_id)
            .cloned()
            .collect::<Vec<_>>();
        ids.sort_by(compare_stream_ids);
        ids
    }
}

/// The group under test, its oracle and its clocks.
struct Run {
    machine: StreamStateMachine,
    oracle: Oracle,
    now_ms: u64,
    index: u64,
    next_history: u64,
    external_objects: u64,
    /// The last page a consumer of the export kept: it is complete.
    kept: Option<UsageListing>,
}

/// A page of `listing`'s group that continues it at `position`.
fn continuing(listing: &UsageListing, position: UsagePosition) -> UsageCursor {
    UsageCursor::After {
        genesis: listing.genesis,
        history: listing.history.id,
        applied: listing.applied,
        retired_through: listing.retired_through,
        position,
    }
}

fn read_page(
    machine: &StreamStateMachine,
    cursor: UsageCursor,
    limit: usize,
    applied: UsageLogId,
) -> Result<UsageListing, TestCaseError> {
    let read = StreamUsageRead {
        cursor,
        limit: NonZeroUsize::new(limit).expect("a positive limit"),
    };
    match machine.stream_usage(&read, applied) {
        GroupUsageView::Listed(listing) => {
            prop_assert!(
                listing.rows.len().saturating_add(listing.entries.len()) <= limit,
                "{} items above the limit {}",
                listing.rows.len().saturating_add(listing.entries.len()),
                limit
            );
            Ok(listing)
        }
        other => Err(TestCaseError::fail(format!("not listed: {other:?}"))),
    }
}

fn appended_non_repeat(response: &StreamResponse) -> bool {
    matches!(
        response,
        StreamResponse::Appended { producer, .. }
            if !producer.as_ref().is_some_and(|ack| ack.is_repeat())
    )
}

impl Run {
    fn new(cap: usize) -> Self {
        let mut run = Self {
            machine: StreamStateMachine::new(),
            oracle: Oracle::new(cap),
            now_ms: 1_000,
            index: 0,
            next_history: GENESIS,
            external_objects: 0,
            kept: None,
        };
        let genesis = run.history_event(0);
        assert!(matches!(genesis, StreamResponse::HistoryStarted { .. }));
        for bucket in BUCKETS {
            let created = run.apply(StreamCommand::CreateBucket {
                bucket_id: bucket.to_owned(),
            });
            assert!(matches!(created, StreamResponse::BucketCreated { .. }));
        }
        run
    }

    fn apply(&mut self, command: StreamCommand) -> StreamResponse {
        self.index = self.index.saturating_add(1);
        self.machine.apply_entry(self.index, command)
    }

    fn history_event(&mut self, previous: u64) -> StreamResponse {
        let history = non_zero(self.next_history);
        self.next_history = self.next_history.saturating_add(1);
        let kept = self.index;
        self.apply(StreamCommand::NewHistory {
            history,
            now_ms: self.now_ms,
            previous,
            kept,
            leader: crate::HistoryLeader {
                term: self.index,
                node_id: 1,
            },
        })
    }

    fn external(&mut self, stream_id: &BucketStreamId, len: u64) -> ExternalPayloadRef {
        self.external_objects = self.external_objects.saturating_add(1);
        ExternalPayloadRef {
            s3_path: format!(
                "{}/{}/external/{}.bin",
                stream_id.bucket_id, stream_id.stream_id, self.external_objects
            ),
            payload_len: len,
            object_size: len,
        }
    }

    /// A command naming `id` found it expired: it removed the version as
    /// `Expired` and answered as if it did not exist.
    fn expired_on_access(
        &mut self,
        op: &Op,
        id: &BucketStreamId,
        response: &StreamResponse,
    ) -> Result<(), TestCaseError> {
        prop_assert!(
            matches!(response, StreamResponse::Error {
                code: crate::StreamErrorCode::StreamNotFound,
                ..
            }),
            "{:?}: {:?}",
            op,
            response
        );
        self.oracle.end(id, StreamEnd::Expired, self.now_ms);
        Ok(())
    }

    /// Applies `op`, checks the answer against what the oracle expects and
    /// updates the oracle.
    #[expect(
        clippy::unwrap_in_result,
        reason = "a version the oracle holds is live by the step's own checks; a miss is a bug \
                  of the test, not a case"
    )]
    fn step(&mut self, op: &Op) -> Result<(), TestCaseError> {
        self.now_ms = self.now_ms.saturating_add(1);
        let now_ms = self.now_ms;
        match op {
            Op::Create {
                stream,
                len,
                external,
                ttl,
            } => {
                let id = stream_id(*stream);
                let stream_ttl_seconds = ttl.then_some(TTL_MS / 1_000);
                let bytes = if *external {
                    len.saturating_add(1)
                } else {
                    *len
                };
                let command = if *external {
                    let payload = self.external(&id, bytes);
                    StreamCommand::CreateExternal {
                        stream_id: id.clone(),
                        content_type: OCTET.to_owned(),
                        initial_payload: payload,
                        message_count: 0,
                        close_after: false,
                        stream_seq: None,
                        stream_ttl_seconds,
                        stream_expires_at_ms: None,
                        now_ms,
                    }
                } else {
                    StreamCommand::CreateStream {
                        stream_id: id.clone(),
                        content_type: OCTET.to_owned(),
                        initial_payload: filled(b'c', *len),
                        close_after: false,
                        stream_seq: None,
                        stream_ttl_seconds,
                        stream_expires_at_ms: None,
                        now_ms,
                    }
                };
                let expired = self.oracle.expired(&id);
                let live = self.oracle.version(&id).is_some();
                let response = self.apply(command);
                if expired {
                    // A create over an expired stream ends it, then creates.
                    prop_assert!(
                        matches!(response, StreamResponse::Created { .. }),
                        "{:?}",
                        response
                    );
                    self.oracle.end(&id, StreamEnd::Expired, now_ms);
                } else if live {
                    prop_assert!(
                        !matches!(response, StreamResponse::Created { .. }),
                        "{:?}",
                        response
                    );
                }
                if matches!(response, StreamResponse::Created { .. }) {
                    let at = self
                        .machine
                        .stream_slot(&id)
                        .expect("a created version")
                        .metadata
                        .created_at_ms;
                    self.oracle.create(&id, bytes, *ttl, now_ms, at);
                }
            }
            Op::Append {
                stream,
                len,
                external,
            } => {
                let id = stream_id(*stream);
                let (command, bytes) = if *external {
                    let payload = self.external(&id, len.saturating_add(1));
                    (
                        StreamCommand::AppendExternal {
                            stream_id: id.clone(),
                            content_type: Some(OCTET.to_owned()),
                            payload,
                            message_count: 0,
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms,
                        },
                        len.saturating_add(1),
                    )
                } else {
                    (
                        StreamCommand::Append {
                            stream_id: id.clone(),
                            content_type: Some(OCTET.to_owned()),
                            payload: filled(b'a', *len),
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms,
                        },
                        *len,
                    )
                };
                let expired = self.oracle.expired(&id);
                let open = self
                    .oracle
                    .version(&id)
                    .is_some_and(|version| !version.closed);
                let response = self.apply(command);
                if expired {
                    self.expired_on_access(op, &id, &response)?;
                } else if appended_non_repeat(&response) {
                    prop_assert!(open, "{:?}: {:?}", op, response);
                    self.oracle.append(&id, bytes, now_ms);
                } else {
                    // A refusal: a missing or closed stream, or an empty
                    // inline append.
                    prop_assert!(!open || bytes == 0, "{:?}: refused {:?}", op, response);
                }
            }
            Op::ProducerAppend {
                stream,
                len,
                repeat,
            } => {
                let id = stream_id(*stream);
                let last = self
                    .oracle
                    .version(&id)
                    .and_then(|version| version.producer_seq);
                let producer_seq = match (last, repeat) {
                    (Some(last), true) => last,
                    (Some(last), false) => last.saturating_add(1),
                    (None, _) => 0,
                };
                let expired = self.oracle.expired(&id);
                let response = self.apply(StreamCommand::Append {
                    stream_id: id.clone(),
                    content_type: Some(OCTET.to_owned()),
                    payload: filled(b'p', *len),
                    close_after: false,
                    stream_seq: None,
                    producer: Some(ProducerRequest {
                        producer_id: "p".to_owned(),
                        producer_epoch: 0,
                        producer_seq,
                        ..Default::default()
                    }),
                    now_ms,
                });
                if expired {
                    self.expired_on_access(op, &id, &response)?;
                } else if appended_non_repeat(&response) {
                    // Only a new seq to an open stream applies.
                    prop_assert!(
                        last.is_none_or(|last| producer_seq > last),
                        "{:?}: {:?}",
                        op,
                        response
                    );
                    self.oracle.append(&id, *len, now_ms);
                    self.oracle
                        .live
                        .get_mut(&id)
                        .expect("a live version")
                        .producer_seq = Some(producer_seq);
                }
            }
            Op::Close { stream } => {
                let id = stream_id(*stream);
                let expired = self.oracle.expired(&id);
                let response = self.apply(StreamCommand::Close {
                    stream_id: id.clone(),
                    stream_seq: None,
                    producer: None,
                    now_ms,
                });
                if expired {
                    self.expired_on_access(op, &id, &response)?;
                } else if let Some(version) = self.oracle.live.get_mut(&id) {
                    // A close-only write counts nothing and takes no seq. A
                    // close of an open stream renews its TTL.
                    prop_assert!(
                        matches!(response, StreamResponse::Closed { .. }),
                        "{:?}",
                        response
                    );
                    if !version.closed {
                        version.closed = true;
                        version.last_touch_ms = version.last_touch_ms.max(now_ms);
                    }
                }
            }
            Op::Publish { stream } => {
                let id = stream_id(*stream);
                let expired = self.oracle.expired(&id);
                let version = self.oracle.version(&id).cloned();
                let snapshot_offset = version.as_ref().map_or(0, |version| version.tail);
                let body = format!("snapshot at {now_ms}");
                let body_len = u64::try_from(body.len()).expect("a short body");
                let response = self.apply(StreamCommand::PublishSnapshot {
                    stream_id: id.clone(),
                    snapshot_offset,
                    content_type: OCTET.to_owned(),
                    payload: Bytes::from(body),
                    now_ms,
                    expected_incarnation: None,
                });
                if expired {
                    self.expired_on_access(op, &id, &response)?;
                } else if let Some(version) = version {
                    // Every body differs, so a publish at the visible
                    // snapshot's offset conflicts, and any other replaces
                    // it: a new seq for the snapshot bytes.
                    let replaces = version.snapshot_offset != Some(snapshot_offset);
                    prop_assert_eq!(
                        matches!(response, StreamResponse::SnapshotPublished { .. }),
                        replaces,
                        "{:?}",
                        response
                    );
                    if replaces {
                        self.oracle.restamp(&id);
                        let version = self.oracle.live.get_mut(&id).expect("a live version");
                        version.snapshot_offset = Some(snapshot_offset);
                        version.snapshot_bytes = body_len;
                    }
                }
            }
            Op::Retain { stream } => {
                let id = stream_id(*stream);
                let expired = self.oracle.expired(&id);
                let version = self.oracle.version(&id).cloned();
                let retained_offset = version.as_ref().map_or(0, |version| version.tail);
                let response = self.apply(StreamCommand::AdvanceRetention {
                    stream_id: id.clone(),
                    retained_offset,
                    now_ms,
                    expected_incarnation: None,
                });
                if expired {
                    self.expired_on_access(op, &id, &response)?;
                } else if let Some(version) = version {
                    // Retention moves only forward, and never past the
                    // visible snapshot. A move takes a new seq for the
                    // retained bytes.
                    if retained_offset <= version.retained {
                        prop_assert_eq!(&response, &StreamResponse::RetentionAdvanced {
                            retained_offset: version.retained,
                        });
                    } else if version
                        .snapshot_offset
                        .is_some_and(|snapshot| retained_offset <= snapshot)
                    {
                        prop_assert_eq!(&response, &StreamResponse::RetentionAdvanced {
                            retained_offset,
                        });
                        self.oracle.restamp(&id);
                        self.oracle
                            .live
                            .get_mut(&id)
                            .expect("a live version")
                            .retained = retained_offset;
                    } else {
                        prop_assert!(
                            matches!(response, StreamResponse::Error { .. }),
                            "{:?}",
                            response
                        );
                    }
                }
            }
            Op::Delete { stream } => {
                let id = stream_id(*stream);
                let expired = self.oracle.expired(&id);
                let live = self.oracle.version(&id).is_some();
                let response = self.apply(StreamCommand::DeleteStream {
                    stream_id: id.clone(),
                    now_ms,
                });
                if live {
                    prop_assert_eq!(&response, &StreamResponse::Deleted { expired });
                    let end = if expired {
                        StreamEnd::Expired
                    } else {
                        StreamEnd::Deleted
                    };
                    self.oracle.end(&id, end, now_ms);
                } else {
                    prop_assert!(
                        matches!(response, StreamResponse::Error { .. }),
                        "{:?}",
                        response
                    );
                }
            }
            Op::Touch { stream } => {
                let id = stream_id(*stream);
                let expired = self.oracle.expired(&id);
                let response = self.apply(StreamCommand::TouchStreamAccess {
                    stream_id: id.clone(),
                    now_ms,
                    renew_ttl: true,
                });
                if expired {
                    prop_assert!(
                        matches!(response, StreamResponse::Accessed { expired: true, .. }),
                        "{:?}",
                        response
                    );
                    self.oracle.end(&id, StreamEnd::Expired, now_ms);
                } else if let Some(version) = self.oracle.live.get_mut(&id) {
                    prop_assert!(
                        matches!(response, StreamResponse::Accessed { expired: false, .. }),
                        "{:?}",
                        response
                    );
                    if version.ttl {
                        version.last_touch_ms = version.last_touch_ms.max(now_ms);
                    }
                }
            }
            Op::Expire => {
                self.now_ms = self.now_ms.saturating_add(EXPIRE_JUMP_MS);
                for version in self.oracle.live.values_mut() {
                    version.expired |= version.ttl;
                }
            }
            Op::Sweep { quarters } => {
                let retire_through = self.oracle.usage_seq.saturating_mul(*quarters) / 4;
                let limit = SWEEP_LIMIT_COUNT;
                // The entry folds only what the group held before it.
                let through = retire_through.min(self.oracle.usage_seq);
                let expired = self.oracle.expired_in_ttl_order();
                let removed = expired.len().min(limit);
                for id in expired.iter().take(limit) {
                    self.oracle.end(id, StreamEnd::Expired, now_ms);
                }
                let mut retired = 0_usize;
                while retired < limit
                    && self
                        .oracle
                        .ends
                        .first_key_value()
                        .is_some_and(|(seq, _)| *seq <= through)
                {
                    self.oracle.fold_first();
                    retired = retired.saturating_add(1);
                }
                let response = self.apply(StreamCommand::Sweep {
                    now_ms,
                    limit: NonZeroU32::new(SWEEP_LIMIT).expect("non-zero"),
                    retire_through,
                });
                prop_assert_eq!(&response, &StreamResponse::Swept {
                    expired: u64::try_from(removed).unwrap_or(u64::MAX),
                    retired: u64::try_from(retired).unwrap_or(u64::MAX),
                });
            }
            Op::Purge => {
                let response = self.apply(StreamCommand::PurgeBucket {
                    bucket_id: PURGED.to_owned(),
                    now_ms,
                });
                prop_assert!(
                    matches!(response, StreamResponse::BucketPurged { .. }),
                    "{:?}",
                    response
                );
                // Expired or not, each stream ends purged, in stream-id
                // order.
                for id in self.oracle.in_bucket(PURGED) {
                    self.oracle.end(&id, StreamEnd::Purged, now_ms);
                }
            }
            Op::NewHistory => {
                let previous = self.oracle.history;
                let history = self.next_history;
                let started = self.history_event(previous);
                prop_assert!(
                    matches!(started, StreamResponse::HistoryStarted { .. }),
                    "{:?}",
                    started
                );
                self.oracle.history = history;
                self.oracle.at_event = self
                    .oracle
                    .live
                    .iter()
                    .map(|(id, version)| ((id.clone(), version.created_at_ms), version.counters))
                    .collect();
            }
            Op::Import => self.import()?,
            Op::RoundTrip => {
                self.machine = StreamStateMachine::restore_group(
                    self.machine.snapshot(),
                    self.machine.usage_snapshot(),
                )
                .expect("a group snapshot restores");
            }
        }
        Ok(())
    }

    /// The group's snapshot, imported as a backup into a fresh group: every
    /// version restarts at zero counted since the import, stamped in
    /// stream-id order, and the totals carry on as retired usage.
    fn import(&mut self) -> Result<(), TestCaseError> {
        let snapshot = self.machine.snapshot();
        let genesis = self.next_history;
        let history = self.next_history.saturating_add(1);
        self.next_history = self.next_history.saturating_add(2);
        self.index = self.index.saturating_add(1);
        let mut target = StreamStateMachine::new();
        target.apply_entry(self.index, StreamCommand::NewHistory {
            history: non_zero(genesis),
            now_ms: self.now_ms,
            previous: 0,
            kept: 0,
            leader: crate::HistoryLeader {
                term: self.index,
                node_id: 1,
            },
        });
        self.index = self.index.saturating_add(1);
        let imported = target.apply_entry(self.index, StreamCommand::ImportSnapshot {
            snapshot: Box::new(snapshot),
            history: non_zero(history),
            backup_digest: [self.index.to_le_bytes()[0]; 32],
            now_ms: self.now_ms,
        });
        prop_assert!(
            matches!(imported, StreamResponse::SnapshotImported { .. }),
            "{:?}",
            imported
        );
        self.machine = target;

        let oracle = &mut self.oracle;
        oracle.genesis = genesis;
        oracle.history = history;
        oracle.retired = oracle.totals.clone();
        oracle.ends.clear();
        oracle.retired_through = 0;
        oracle.usage_seq = 0;
        oracle.at_event.clear();
        let mut ids = oracle.live.keys().cloned().collect::<Vec<_>>();
        ids.sort_by(compare_stream_ids);
        for id in ids {
            let seq = oracle.stamp();
            let version = oracle.live.get_mut(&id).expect("a live version");
            version.seq = seq;
            version.counters = VersionCounters::default();
            version.last_write = LastWrite::Imported;
            version.counted_since = history;
        }
        Ok(())
    }

    /// The ledger equals the oracle's, and L1 to L8 hold.
    fn check(&self) -> Result<(), TestCaseError> {
        let machine = &self.machine;
        let oracle = &self.oracle;
        let report = machine.bucket_usage_report();
        let totals = report
            .iter()
            .map(|row| (row.bucket_id.clone(), CommittedUsage::of_bucket(&row.usage)))
            .collect::<BTreeMap<_, _>>();
        prop_assert_eq!(&totals, &oracle.totals);
        // L4: every bucket with a row has its retired row.
        prop_assert_eq!(
            machine.state_gauges().bucket_usage_rows,
            u64::try_from(report.len()).unwrap_or(u64::MAX)
        );

        let usage = machine.usage_snapshot();
        prop_assert_eq!(usage.genesis, oracle.genesis);
        prop_assert_eq!(machine.history_id(), oracle.history);
        prop_assert_eq!(usage.usage_seq, oracle.usage_seq);
        prop_assert_eq!(usage.retired_through, oracle.retired_through);
        let retired = oracle
            .retired
            .iter()
            .map(|(bucket_id, retired)| BucketRetired {
                bucket_id: bucket_id.clone(),
                retired: *retired,
            })
            .collect::<Vec<_>>();
        prop_assert_eq!(&usage.retired, &retired);
        // Every end record, whole: how and when it ended, in which history,
        // its final usage and its seq.
        let ends = oracle.ends.values().cloned().collect::<Vec<_>>();
        prop_assert_eq!(&usage.ends, &ends);

        let usage_seq = machine.usage_seq();
        let retired_through = machine.usage_retired_through();
        // L3.
        prop_assert!(retired_through <= usage_seq);
        let mut seqs = BTreeSet::new();
        prop_assert_eq!(machine.registry.slots().count(), oracle.live.len());
        for (id, version) in &oracle.live {
            let Some(slot) = machine.stream_slot(id) else {
                return Err(TestCaseError::fail(format!("{id} is not live")));
            };
            prop_assert_eq!(slot.metadata.created_at_ms, version.created_at_ms, "{}", id);
            prop_assert_eq!(slot.metadata.tail_offset, version.tail, "{}", id);
            let usage = slot.usage;
            prop_assert_eq!(usage.seq, version.seq, "{}", id);
            prop_assert_eq!(usage.counters, version.counters, "{}", id);
            prop_assert_eq!(usage.last_write, version.last_write, "{}", id);
            prop_assert_eq!(usage.counted_since.get(), version.counted_since, "{}", id);
            // L1.
            prop_assert!((1..=usage_seq).contains(&usage.seq), "{}", id);
            prop_assert!(seqs.insert(usage.seq), "{}", id);
            // The baseline of the group's history, which an export derives,
            // is the counters at the event, or zero for a version created
            // or restored since. L6: it never exceeds the counters.
            let baseline = slot.history_baseline_in(oracle.history);
            prop_assert_eq!(baseline, oracle.baseline_of(id, version), "{}", id);
            prop_assert!(baseline.appends <= usage.counters.appends);
            prop_assert!(
                baseline.committed.committed_write_units
                    <= usage.counters.committed.committed_write_units
            );
        }
        // L1: the node-local index holds exactly each live version at its
        // seq.
        let index = machine
            .usage
            .live_index()
            .iter()
            .map(|(seq, key)| {
                let slot = machine.registry.slot_by_key(*key);
                (*seq, slot.map(|slot| slot.metadata.stream_id.clone()))
            })
            .collect::<BTreeMap<_, _>>();
        let live_seqs = oracle
            .live
            .iter()
            .map(|(id, version)| (version.seq, Some(id.clone())))
            .collect::<BTreeMap<_, _>>();
        prop_assert_eq!(index, live_seqs);
        // L2 and L8.
        prop_assert!(machine.end_records().len() <= MAX_END_RECORDS.min(oracle.cap));
        for record in machine.end_records() {
            prop_assert!(record.seq > retired_through && record.seq <= usage_seq);
            prop_assert!(seqs.insert(record.seq));
        }

        // A round trip through the group snapshot reproduces the ledger,
        // and so does a twin whose snapshot lists its streams the other way.
        let restored = StreamStateMachine::restore_group(machine.snapshot(), usage.clone())
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&restored.usage_snapshot(), &usage);
        prop_assert_eq!(&restored.bucket_usage_report(), &report);
        let mut snapshot = machine.snapshot();
        let mut reversed = usage.clone();
        snapshot.streams.reverse();
        reversed.streams.reverse();
        let twin = StreamStateMachine::restore_group(snapshot, reversed)
            .map_err(|error| TestCaseError::fail(error.to_string()))?;
        prop_assert_eq!(&twin.usage_snapshot(), &usage);
        Ok(())
    }
}

impl Run {
    /// The log id the run's group applied last.
    fn applied(&self) -> UsageLogId {
        UsageLogId {
            term: 0,
            index: self.index,
        }
    }

    /// The export's read against the oracle. A relisting lists every row
    /// and entry, and its pages at limits 1 to 3 join to the same listing.
    /// Then the consumer's kept cursor is continued with exactly the
    /// entries above its listing, or relisted for the reason the cursor
    /// rule gives, and keeps the new page.
    fn check_export(&mut self) -> Result<(), TestCaseError> {
        let applied = self.applied();
        let oracle = &self.oracle;
        let rows = oracle.rows();
        let entries = oracle.entries();
        let whole = read_page(&self.machine, UsageCursor::Start, usize::MAX, applied)?;
        prop_assert_eq!(whole.relist, Some(RelistReason::Start));
        prop_assert_eq!(&whole.page, &UsagePage::Complete);
        prop_assert_eq!(whole.genesis.get(), oracle.genesis);
        prop_assert_eq!(whole.history.id.get(), oracle.history);
        prop_assert_eq!(whole.applied, applied);
        prop_assert_eq!(whole.usage_seq, oracle.usage_seq);
        prop_assert_eq!(whole.retired_through, oracle.retired_through);
        prop_assert_eq!(
            whole.live_versions,
            u64::try_from(oracle.live.len()).unwrap_or(u64::MAX)
        );
        prop_assert_eq!(
            whole.end_records,
            u64::try_from(oracle.ends.len()).unwrap_or(u64::MAX)
        );
        prop_assert_eq!(&whole.rows, &rows);
        prop_assert_eq!(&whole.entries, &entries);

        for limit in 1..=3 {
            let mut paged_rows = Vec::new();
            let mut paged_entries = Vec::new();
            let mut cursor = UsageCursor::Start;
            let mut first = true;
            loop {
                let page = read_page(&self.machine, cursor, limit, applied)?;
                // Only the first page relists, and every page counts the
                // whole group.
                prop_assert_eq!(page.relist, first.then_some(RelistReason::Start));
                prop_assert_eq!(page.live_versions, whole.live_versions);
                prop_assert_eq!(page.end_records, whole.end_records);
                first = false;
                paged_rows.extend(page.rows.iter().cloned());
                paged_entries.extend(page.entries.iter().cloned());
                match &page.page {
                    UsagePage::Complete => break,
                    UsagePage::More { next } => {
                        prop_assert_eq!(
                            page.rows.len().saturating_add(page.entries.len()),
                            limit,
                            "a page cut below its limit"
                        );
                        cursor = continuing(&page, next.clone());
                    }
                }
            }
            prop_assert_eq!(&paged_rows, &rows, "rows at limit {}", limit);
            prop_assert_eq!(&paged_entries, &entries, "entries at limit {}", limit);
        }

        let (cursor, expected_relist, after_seq) = match &self.kept {
            None => (UsageCursor::Start, Some(RelistReason::Start), 0),
            Some(kept) => {
                let relist = if kept.genesis.get() != oracle.genesis
                    || kept.history.id.get() != oracle.history
                {
                    Some(RelistReason::History)
                } else if oracle.retired_through > kept.usage_seq.max(kept.retired_through) {
                    Some(RelistReason::Retired)
                } else {
                    None
                };
                let position = UsagePosition::Entries {
                    after_seq: kept.usage_seq,
                };
                (continuing(kept, position), relist, kept.usage_seq)
            }
        };
        let page = read_page(&self.machine, cursor, usize::MAX, applied)?;
        prop_assert_eq!(page.relist, expected_relist);
        prop_assert_eq!(&page.page, &UsagePage::Complete);
        if expected_relist.is_some() {
            prop_assert_eq!(&page.rows, &rows);
            prop_assert_eq!(&page.entries, &entries);
        } else {
            let above = entries
                .iter()
                .filter(|entry| entry.seq > after_seq)
                .cloned()
                .collect::<Vec<_>>();
            prop_assert!(page.rows.is_empty());
            prop_assert_eq!(&page.entries, &above);
        }
        self.kept = Some(page);
        Ok(())
    }
}

/// Runs `ops` on a fresh group at an end-record cap of `cap`, checking the
/// ledger after every step.
fn run_against_the_oracle(cap: usize, ops: &[Op]) -> Result<(), TestCaseError> {
    test_cap::with(cap, || {
        let mut run = Run::new(cap);
        run.check()?;
        run.check_export()?;
        for op in ops {
            run.step(op)?;
            run.check()?;
            run.check_export()?;
        }
        Ok(())
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn prop_the_ledger_counts_what_the_oracle_counts(
        ops in proptest::collection::vec(op_strategy(), 1..48),
    ) {
        run_against_the_oracle(MAX_END_RECORDS, &ops)?;
    }

    /// At a cap of three end records most runs fold at the cap.
    #[test]
    fn prop_the_ledger_folds_at_its_cap_as_the_oracle_does(
        ops in proptest::collection::vec(op_strategy(), 1..64),
    ) {
        run_against_the_oracle(SMALL_CAP, &ops)?;
    }
}
