//! The usage ledger (F20) against an oracle. A generated run of creates,
//! inline and external appends, producer repeats, closes, publishes,
//! retentions, deletes, touches, expiry, a bucket purge, sweeps that fold
//! at a moving watermark, history events, an import and group snapshot
//! round trips drives one group. An oracle applies the counting rule to
//! each command and its answer, and after every step the ledger must agree
//! with it: each bucket's totals, each live version's counters, last write,
//! `counted_since` and history baseline, and each new end record's final
//! usage. The ledger invariants L1 to L8 hold, and a round trip through
//! `usage_snapshot` and `restore_group` reproduces the ledger whatever the
//! order of the streams.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::num::NonZeroU64;

use bytes::Bytes;
use proptest::prelude::*;

use super::BucketStreamId;
use super::MAX_END_RECORDS;
use super::ProducerRequest;
use super::StreamCommand;
use super::StreamResponse;
use super::StreamStateMachine;
use crate::model::ExternalPayloadRef;
use crate::usage::CommittedUsage;
use crate::usage::LastWrite;
use crate::usage::VersionCounters;

const OCTET: &str = "application/octet-stream";
const BUCKETS: [&str; 2] = ["usage-a", "usage-b"];
/// The bucket the purge step purges.
const PURGED: &str = "usage-b";
const STREAMS: usize = 6;
const GENESIS: u64 = 0x0007;

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
        1 => stream.clone().prop_map(|stream| Op::Delete { stream }),
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

/// One live version as the oracle counts it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Version {
    created_at_ms: u64,
    counters: VersionCounters,
    last_write: LastWrite,
    counted_since: u64,
    /// The last producer seq that applied, not as a repeat.
    producer_seq: Option<u64>,
}

/// What the counting rule says the group committed.
#[derive(Debug, Default)]
struct Oracle {
    totals: BTreeMap<String, CommittedUsage>,
    live: HashMap<BucketStreamId, Version>,
    /// The counters, at the group's latest history event, of each version
    /// live then. A version created or restored since has a zero baseline.
    at_event: HashMap<(BucketStreamId, u64), VersionCounters>,
}

impl Oracle {
    fn count(&mut self, stream_id: &BucketStreamId, bytes: u64, now_ms: u64, append: bool) {
        let records = u64::from(bytes > 0);
        let write = CommittedUsage::of_write(bytes, records);
        self.totals
            .entry(stream_id.bucket_id.clone())
            .or_default()
            .add(write);
        if append {
            let version = self.live.get_mut(stream_id).expect("a live version");
            version.counters.committed.add(write);
            version.counters.appends = version.counters.appends.saturating_add(1);
            version.last_write = LastWrite::At(now_ms);
        }
    }
}

/// The group under test and its clocks.
struct Run {
    machine: StreamStateMachine,
    oracle: Oracle,
    now_ms: u64,
    index: u64,
    next_history: u64,
    external_objects: u64,
}

fn appended_non_repeat(response: &StreamResponse) -> bool {
    matches!(
        response,
        StreamResponse::Appended { producer, .. }
            if !producer.as_ref().is_some_and(|ack| ack.is_repeat())
    )
}

impl Run {
    fn new() -> Self {
        let mut run = Self {
            machine: StreamStateMachine::new(),
            oracle: Oracle::default(),
            now_ms: 1_000,
            index: 0,
            next_history: GENESIS,
            external_objects: 0,
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
        let history = NonZeroU64::new(self.next_history).expect("non-zero");
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

    fn tail(&self, stream_id: &BucketStreamId) -> u64 {
        self.machine
            .head(stream_id)
            .map_or(0, |metadata| metadata.tail_offset)
    }

    /// Applies `op` and updates the oracle from each command and its
    /// answer. Returns the versions the step ended.
    fn step(&mut self, op: &Op) -> Vec<(BucketStreamId, Version)> {
        self.now_ms = self.now_ms.saturating_add(1);
        let now_ms = self.now_ms;
        let before = self.machine.usage_seq();
        let mut created = None;
        let mut appended = None;
        match op {
            Op::Create {
                stream,
                len,
                external,
                ttl,
            } => {
                let id = stream_id(*stream);
                let stream_ttl_seconds = ttl.then_some(1);
                let command = if *external {
                    let payload = self.external(&id, len.saturating_add(1));
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
                        initial_payload: Bytes::from(vec![b'c'; usize::try_from(*len).unwrap()]),
                        close_after: false,
                        stream_seq: None,
                        stream_ttl_seconds,
                        stream_expires_at_ms: None,
                        now_ms,
                    }
                };
                let bytes = if *external {
                    len.saturating_add(1)
                } else {
                    *len
                };
                if matches!(self.apply(command), StreamResponse::Created { .. }) {
                    created = Some((id, bytes));
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
                            payload: Bytes::from(vec![b'a'; usize::try_from(*len).unwrap()]),
                            close_after: false,
                            stream_seq: None,
                            producer: None,
                            now_ms,
                        },
                        *len,
                    )
                };
                if appended_non_repeat(&self.apply(command)) {
                    appended = Some((id, bytes, None));
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
                    .live
                    .get(&id)
                    .and_then(|version| version.producer_seq);
                let producer_seq = match (last, repeat) {
                    (Some(last), true) => last,
                    (Some(last), false) => last.saturating_add(1),
                    (None, _) => 0,
                };
                let response = self.apply(StreamCommand::Append {
                    stream_id: id.clone(),
                    content_type: Some(OCTET.to_owned()),
                    payload: Bytes::from(vec![b'p'; usize::try_from(*len).unwrap()]),
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
                if appended_non_repeat(&response) {
                    appended = Some((id, *len, Some(producer_seq)));
                }
            }
            Op::Close { stream } => {
                self.apply(StreamCommand::Close {
                    stream_id: stream_id(*stream),
                    stream_seq: None,
                    producer: None,
                    now_ms,
                });
            }
            Op::Publish { stream } => {
                let id = stream_id(*stream);
                let snapshot_offset = self.tail(&id);
                self.apply(StreamCommand::PublishSnapshot {
                    stream_id: id,
                    snapshot_offset,
                    content_type: OCTET.to_owned(),
                    payload: Bytes::from(format!("snapshot at {now_ms}")),
                    now_ms,
                    expected_incarnation: None,
                });
            }
            Op::Retain { stream } => {
                let id = stream_id(*stream);
                let retained_offset = self.tail(&id);
                self.apply(StreamCommand::AdvanceRetention {
                    stream_id: id,
                    retained_offset,
                    now_ms,
                    expected_incarnation: None,
                });
            }
            Op::Delete { stream } => {
                self.apply(StreamCommand::DeleteStream {
                    stream_id: stream_id(*stream),
                    now_ms,
                });
            }
            Op::Touch { stream } => {
                self.apply(StreamCommand::TouchStreamAccess {
                    stream_id: stream_id(*stream),
                    now_ms,
                    renew_ttl: true,
                });
            }
            Op::Expire => self.now_ms = self.now_ms.saturating_add(5_000),
            Op::Sweep { quarters } => {
                let retire_through = self.machine.usage_seq().saturating_mul(*quarters) / 4;
                let response = self.apply(StreamCommand::Sweep {
                    now_ms,
                    limit: NonZeroU32::new(4).expect("non-zero"),
                    retire_through,
                });
                assert!(
                    matches!(response, StreamResponse::Swept { .. }),
                    "{response:?}"
                );
            }
            Op::Purge => {
                self.apply(StreamCommand::PurgeBucket {
                    bucket_id: PURGED.to_owned(),
                    now_ms,
                });
            }
            Op::NewHistory => {
                let previous = self.machine.history_id();
                assert!(matches!(
                    self.history_event(previous),
                    StreamResponse::HistoryStarted { .. }
                ));
                self.oracle.at_event = self
                    .oracle
                    .live
                    .iter()
                    .map(|(id, version)| ((id.clone(), version.created_at_ms), version.counters))
                    .collect();
            }
            Op::Import => {
                self.import();
                return Vec::new();
            }
            Op::RoundTrip => {
                self.machine = StreamStateMachine::restore_group(
                    self.machine.snapshot(),
                    self.machine.usage_snapshot(),
                )
                .expect("a group snapshot restores");
            }
        }

        // A version whose slot left, or holds another version, ended.
        let mut ended = Vec::new();
        let live = std::mem::take(&mut self.oracle.live);
        for (id, version) in live {
            let same = self
                .machine
                .stream_slot(&id)
                .is_some_and(|slot| slot.metadata.created_at_ms == version.created_at_ms);
            if same {
                self.oracle.live.insert(id, version);
            } else {
                ended.push((id, version));
            }
        }
        if let Some((id, bytes)) = created {
            self.oracle.count(&id, bytes, now_ms, false);
            let slot = self.machine.stream_slot(&id).expect("a created version");
            self.oracle.live.insert(id, Version {
                created_at_ms: slot.metadata.created_at_ms,
                counters: VersionCounters {
                    committed: CommittedUsage::of_write(bytes, u64::from(bytes > 0)),
                    appends: 0,
                },
                last_write: LastWrite::At(now_ms),
                counted_since: self.machine.history_id(),
                producer_seq: None,
            });
        }
        if let Some((id, bytes, producer_seq)) = appended {
            self.oracle.count(&id, bytes, now_ms, true);
            if producer_seq.is_some() {
                self.oracle
                    .live
                    .get_mut(&id)
                    .expect("a live version")
                    .producer_seq = producer_seq;
            }
        }
        // Every removal of the step stamped one end record above the seq
        // before it.
        let new_ends = self
            .machine
            .end_records()
            .filter(|record| record.seq > before)
            .count();
        assert_eq!(new_ends, ended.len(), "{op:?}");
        ended
    }

    /// The group's snapshot, imported as a backup into a fresh group: every
    /// version restarts at zero counted since the import, and the totals
    /// carry on as retired usage.
    fn import(&mut self) {
        let snapshot = self.machine.snapshot();
        let mut target = StreamStateMachine::new();
        let genesis = NonZeroU64::new(self.next_history).expect("non-zero");
        let history = NonZeroU64::new(self.next_history.saturating_add(1)).expect("non-zero");
        self.next_history = self.next_history.saturating_add(2);
        self.index = self.index.saturating_add(1);
        target.apply_entry(self.index, StreamCommand::NewHistory {
            history: genesis,
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
            history,
            backup_digest: [u8::try_from(self.index % 256).unwrap(); 32],
            now_ms: self.now_ms,
        });
        assert!(
            matches!(imported, StreamResponse::SnapshotImported { .. }),
            "{imported:?}"
        );
        assert_eq!(target.usage_genesis(), genesis.get());
        self.machine = target;
        for version in self.oracle.live.values_mut() {
            version.counters = VersionCounters::default();
            version.last_write = LastWrite::Imported;
            version.counted_since = history.get();
        }
        self.oracle.at_event.clear();
    }

    /// The ledger agrees with the oracle, and L1 to L8 hold.
    fn check(&self, ended: &[(BucketStreamId, Version)]) -> Result<(), TestCaseError> {
        let machine = &self.machine;
        let report = machine.bucket_usage_report();
        let totals = report
            .iter()
            .map(|row| (row.bucket_id.clone(), CommittedUsage::of_bucket(&row.usage)))
            .collect::<BTreeMap<_, _>>();
        prop_assert_eq!(&totals, &self.oracle.totals);
        // L4: every bucket with a row has its retired row.
        prop_assert_eq!(
            machine.state_gauges().bucket_usage_rows,
            u64::try_from(report.len()).unwrap_or(u64::MAX)
        );

        let usage_seq = machine.usage_seq();
        let retired_through = machine.usage_retired_through();
        let group = machine.history_id();
        // L3.
        prop_assert!(retired_through <= usage_seq);
        let mut seqs = BTreeSet::new();
        prop_assert_eq!(machine.registry.slots().count(), self.oracle.live.len());
        for (id, version) in &self.oracle.live {
            let Some(slot) = machine.stream_slot(id) else {
                return Err(TestCaseError::fail(format!("{id} is not live")));
            };
            let usage = slot.usage;
            prop_assert_eq!(usage.counters, version.counters, "{}", id);
            prop_assert_eq!(usage.last_write, version.last_write, "{}", id);
            prop_assert_eq!(usage.counted_since.get(), version.counted_since, "{}", id);
            // L1.
            prop_assert!((1..=usage_seq).contains(&usage.seq), "{}", id);
            prop_assert!(seqs.insert(usage.seq), "{}", id);
            // The baseline of the group's history, which an export derives,
            // is the counters at the event, or zero for a version created
            // or restored since. L6: it never exceeds the counters.
            let baseline = slot.history_baseline_in(group);
            let at_event = self
                .oracle
                .at_event
                .get(&(id.clone(), version.created_at_ms))
                .copied()
                .unwrap_or_default();
            prop_assert_eq!(baseline, at_event, "{}", id);
            prop_assert!(baseline.appends <= usage.counters.appends);
            prop_assert!(
                baseline.committed.committed_write_units
                    <= usage.counters.committed.committed_write_units
            );
        }
        // L2 and L8.
        prop_assert!(machine.end_records().len() <= MAX_END_RECORDS);
        for record in machine.end_records() {
            prop_assert!(record.seq > retired_through && record.seq <= usage_seq);
            prop_assert!(seqs.insert(record.seq));
        }
        // L6: an end record carries the version's usage at its removal.
        for (id, version) in ended {
            let record = machine
                .end_records()
                .filter(|record| {
                    record.stream_id == *id && record.created_at_ms == version.created_at_ms
                })
                .last();
            // A sweep of the same step never folds its own removals.
            let Some(record) = record else {
                return Err(TestCaseError::fail(format!("{id} left no end record")));
            };
            prop_assert_eq!(record.counters, version.counters);
            prop_assert_eq!(record.last_write, version.last_write);
            prop_assert_eq!(record.counted_since.get(), version.counted_since);
            prop_assert_eq!(record.ended_in.get(), group);
        }

        // A round trip through the group snapshot reproduces the ledger,
        // and so does a twin whose snapshot lists its streams the other way.
        let usage = machine.usage_snapshot();
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

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn prop_the_ledger_counts_what_the_oracle_counts(
        ops in proptest::collection::vec(op_strategy(), 1..48),
    ) {
        let mut run = Run::new();
        run.check(&[])?;
        for op in &ops {
            let ended = run.step(op);
            run.check(&ended)?;
        }
    }
}
