//! Bounded-state F0: expired streams leave a group through
//! [`StreamCommand::Sweep`] alone. Each entry removes up to its
//! `limit` streams expired at its `now_ms`, earliest expiry first, then by
//! bucket and stream id. Every other command removes at most the stream it
//! names, and only when that stream was deleted or had expired at the
//! command's `now_ms`. What a client reads never depends on when the sweep
//! ran.

use std::collections::HashMap;
use std::collections::HashSet;
use std::num::NonZeroU32;

use proptest::collection::vec;
use proptest::prelude::*;
use ursula_shard::BucketStreamId;

use super::StreamStateMachine;
use super::compare_stream_ids;
use super::stream_expiry_at_ms;
use super::stream_is_expired;
use crate::command::StreamCommand;
use crate::model::ExternalPayloadRef;
use crate::model::ProducerRequest;
use crate::model::StreamMetadata;
use crate::response::StreamErrorCode;
use crate::response::StreamResponse;

const BUCKETS: [&str; 2] = ["alpha", "beta"];
const NAMES: usize = 24;
const OCTET: &str = "application/octet-stream";

fn stream(bucket: usize, name: usize) -> BucketStreamId {
    BucketStreamId::new(BUCKETS[bucket], format!("s-{name}"))
}

fn every_stream() -> impl Iterator<Item = BucketStreamId> {
    (0..BUCKETS.len()).flat_map(|bucket| (0..NAMES).map(move |name| stream(bucket, name)))
}

fn limit(limit: u32) -> NonZeroU32 {
    NonZeroU32::new(limit).expect("a limit is positive")
}

fn machine() -> StreamStateMachine {
    let mut machine = StreamStateMachine::new();
    for bucket in BUCKETS {
        machine.apply(StreamCommand::CreateBucket {
            bucket_id: bucket.to_owned(),
        });
    }
    machine
}

fn create(
    stream_id: BucketStreamId,
    ttl_seconds: Option<u64>,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> StreamCommand {
    StreamCommand::CreateStream {
        stream_id,
        content_type: OCTET.to_owned(),
        initial_payload: bytes::Bytes::from_static(b"first;"),
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: ttl_seconds,
        stream_expires_at_ms: expires_at_ms,
        now_ms,
    }
}

fn external_payload(name: &str) -> ExternalPayloadRef {
    ExternalPayloadRef {
        s3_path: format!("staged/{name}"),
        payload_len: 7,
        object_size: 7,
    }
}

fn create_external(
    stream_id: BucketStreamId,
    ttl_seconds: Option<u64>,
    expires_at_ms: Option<u64>,
    now_ms: u64,
) -> StreamCommand {
    StreamCommand::CreateExternal {
        initial_payload: external_payload(&stream_id.stream_id),
        stream_id,
        content_type: OCTET.to_owned(),
        message_count: 0,
        close_after: false,
        stream_seq: None,
        stream_ttl_seconds: ttl_seconds,
        stream_expires_at_ms: expires_at_ms,
        now_ms,
    }
}

fn append(stream_id: BucketStreamId, now_ms: u64) -> StreamCommand {
    StreamCommand::Append {
        stream_id,
        content_type: Some(OCTET.to_owned()),
        payload: bytes::Bytes::from_static(b"record;"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
    }
}

fn append_external(stream_id: BucketStreamId, now_ms: u64) -> StreamCommand {
    StreamCommand::AppendExternal {
        payload: external_payload(&stream_id.stream_id),
        stream_id,
        content_type: Some(OCTET.to_owned()),
        message_count: 0,
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
    }
}

fn expire(now_ms: u64, max: u32) -> StreamCommand {
    StreamCommand::Sweep {
        now_ms,
        limit: limit(max),
        retire_through: 0,
    }
}

/// Every live stream's metadata, by id.
fn live(machine: &StreamStateMachine) -> HashMap<BucketStreamId, StreamMetadata> {
    machine
        .snapshot()
        .streams
        .into_iter()
        .map(|entry| (entry.metadata.stream_id.clone(), entry.metadata))
        .collect()
}

/// The streams expired at `now_ms`, in (expiry, bucket, stream) order: the
/// reference the TTL index must agree with.
fn due(machine: &StreamStateMachine, now_ms: u64) -> Vec<BucketStreamId> {
    let mut due = live(machine)
        .into_values()
        .filter_map(|metadata| {
            stream_expiry_at_ms(&metadata)
                .filter(|expires_at_ms| *expires_at_ms <= now_ms)
                .map(|expires_at_ms| (expires_at_ms, metadata.stream_id))
        })
        .collect::<Vec<_>>();
    due.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| compare_stream_ids(&left.1, &right.1))
    });
    due.into_iter().map(|(_, stream_id)| stream_id).collect()
}

fn removed_by(machine: &mut StreamStateMachine, command: StreamCommand) -> Vec<BucketStreamId> {
    let before = live(machine);
    machine.apply(command);
    let after = live(machine);
    let mut removed = before
        .into_keys()
        .filter(|id| !after.contains_key(id))
        .collect::<Vec<_>>();
    removed.sort_by(compare_stream_ids);
    removed
}

#[derive(Debug, Clone)]
enum Ttl {
    Sliding(u64),
    Absolute(u64),
    None,
}

/// One step of a workload. Every write carries the workload's clock, which
/// is past many expiries.
#[derive(Debug, Clone)]
enum Op {
    Create {
        bucket: usize,
        name: usize,
        ttl: Ttl,
        external: bool,
    },
    Append {
        bucket: usize,
        name: usize,
        external: bool,
    },
    Publish {
        bucket: usize,
        name: usize,
    },
    Retain {
        bucket: usize,
        name: usize,
    },
    Touch {
        bucket: usize,
        name: usize,
        renew: bool,
    },
    Close {
        bucket: usize,
        name: usize,
    },
    Tidy {
        bucket: usize,
        name: usize,
    },
    Delete {
        bucket: usize,
        name: usize,
    },
    Purge {
        bucket: usize,
    },
    /// A producer write: an append with producer headers.
    Produce {
        bucket: usize,
        name: usize,
        epoch: u64,
        seq: u64,
    },
    /// `op` under a `Stream-Incarnation` precondition: the token of its
    /// stream when `matching`, else a token no stream has.
    Guarded {
        op: Box<Op>,
        matching: bool,
    },
    Restore,
    Advance(u64),
    Expire(u32),
}

impl Op {
    /// The stream the command names, if it names one.
    fn named(&self) -> Option<BucketStreamId> {
        match self {
            Self::Create { bucket, name, .. }
            | Self::Append { bucket, name, .. }
            | Self::Publish { bucket, name }
            | Self::Retain { bucket, name }
            | Self::Touch { bucket, name, .. }
            | Self::Close { bucket, name }
            | Self::Tidy { bucket, name }
            | Self::Delete { bucket, name }
            | Self::Produce { bucket, name, .. } => Some(stream(*bucket, *name)),
            Self::Guarded { op, .. } => op.named(),
            Self::Purge { .. } | Self::Restore | Self::Advance(_) | Self::Expire(_) => None,
        }
    }

    /// The command this step applies at `now`, if it applies one. A
    /// guarded step names `incarnation`.
    fn command(&self, now: u64, incarnation: u64) -> Option<StreamCommand> {
        let command = match self {
            Self::Guarded { op, .. } => StreamCommand::IfIncarnation {
                incarnation,
                command: Box::new(op.command(now, incarnation)?),
            },
            Self::Produce {
                bucket,
                name,
                epoch,
                seq,
            } => StreamCommand::Append {
                stream_id: stream(*bucket, *name),
                content_type: Some(OCTET.to_owned()),
                payload: bytes::Bytes::from_static(b"produced;"),
                close_after: false,
                stream_seq: None,
                producer: Some(producer(*epoch, *seq)),
                now_ms: now,
            },
            Self::Create {
                bucket,
                name,
                ttl,
                external,
            } => {
                let (ttl_seconds, expires_at_ms) = match ttl {
                    Ttl::Sliding(seconds) => (Some(*seconds), None),
                    Ttl::Absolute(after) => (None, Some(now.saturating_add(*after))),
                    Ttl::None => (None, None),
                };
                if *external {
                    create_external(stream(*bucket, *name), ttl_seconds, expires_at_ms, now)
                } else {
                    create(stream(*bucket, *name), ttl_seconds, expires_at_ms, now)
                }
            }
            Self::Append {
                bucket,
                name,
                external,
            } => {
                if *external {
                    append_external(stream(*bucket, *name), now)
                } else {
                    append(stream(*bucket, *name), now)
                }
            }
            Self::Publish { bucket, name } => StreamCommand::PublishSnapshot {
                stream_id: stream(*bucket, *name),
                snapshot_offset: 6,
                content_type: OCTET.to_owned(),
                payload: bytes::Bytes::from_static(b"snapshot"),
                now_ms: now,
                expected_incarnation: None,
            },
            Self::Retain { bucket, name } => StreamCommand::AdvanceRetention {
                stream_id: stream(*bucket, *name),
                retained_offset: 6,
                now_ms: now,
                expected_incarnation: None,
            },
            Self::Touch {
                bucket,
                name,
                renew,
            } => StreamCommand::TouchStreamAccess {
                stream_id: stream(*bucket, *name),
                now_ms: now,
                renew_ttl: *renew,
            },
            Self::Close { bucket, name } => StreamCommand::Close {
                stream_id: stream(*bucket, *name),
                stream_seq: None,
                producer: None,
                now_ms: now,
            },
            Self::Tidy { bucket, name } => StreamCommand::TidyStream {
                stream_id: stream(*bucket, *name),
                now_ms: now,
            },
            Self::Delete { bucket, name } => StreamCommand::DeleteStream {
                stream_id: stream(*bucket, *name),
                now_ms: now,
            },
            Self::Purge { bucket } => StreamCommand::PurgeBucket {
                bucket_id: BUCKETS[*bucket].to_owned(),
                now_ms: now,
            },
            Self::Expire(max) => expire(now, *max),
            Self::Restore | Self::Advance(_) => return None,
        };
        Some(command)
    }
}

fn ttl_strategy() -> impl Strategy<Value = Ttl> {
    prop_oneof![
        (1_u64..=5).prop_map(Ttl::Sliding),
        (0_u64..=6_000).prop_map(Ttl::Absolute),
        Just(Ttl::None),
    ]
}

fn named_strategy() -> impl Strategy<Value = (usize, usize)> {
    (0_usize..BUCKETS.len(), 0_usize..NAMES)
}

fn producer(epoch: u64, seq: u64) -> ProducerRequest {
    ProducerRequest {
        producer_id: "writer".to_owned(),
        producer_epoch: epoch,
        producer_seq: seq,
        digest: None,
        ..Default::default()
    }
}

fn produce_strategy() -> impl Strategy<Value = Op> {
    (named_strategy(), 1_u64..=2, 0_u64..=3).prop_map(|((bucket, name), epoch, seq)| Op::Produce {
        bucket,
        name,
        epoch,
        seq,
    })
}

/// A client's steps, producer writes and steps under a `Stream-Incarnation`
/// precondition among them.
fn guarded_client_op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => client_op_strategy(),
        2 => produce_strategy(),
        2 => (
            prop_oneof![client_op_strategy(), produce_strategy()],
            any::<bool>(),
        )
            .prop_filter("a precondition names a stream", |(op, _)| op.named().is_some())
            .prop_map(|(op, matching)| Op::Guarded {
                op: Box::new(op),
                matching,
            }),
    ]
}

/// Every step but the expiry sweep: what a client and the other leader
/// drivers do.
fn client_op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (named_strategy(), ttl_strategy(), any::<bool>()).prop_map(
            |((bucket, name), ttl, external)| Op::Create { bucket, name, ttl, external }
        ),
        3 => (named_strategy(), any::<bool>())
            .prop_map(|((bucket, name), external)| Op::Append { bucket, name, external }),
        1 => named_strategy().prop_map(|(bucket, name)| Op::Publish { bucket, name }),
        1 => named_strategy().prop_map(|(bucket, name)| Op::Retain { bucket, name }),
        2 => (named_strategy(), any::<bool>())
            .prop_map(|((bucket, name), renew)| Op::Touch { bucket, name, renew }),
        1 => named_strategy().prop_map(|(bucket, name)| Op::Close { bucket, name }),
        1 => named_strategy().prop_map(|(bucket, name)| Op::Delete { bucket, name }),
        1 => (0_usize..BUCKETS.len()).prop_map(|bucket| Op::Purge { bucket }),
        1 => Just(Op::Restore),
        3 => (0_u64..=1_500).prop_map(Op::Advance),
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        12 => client_op_strategy(),
        1 => named_strategy().prop_map(|(bucket, name)| Op::Tidy { bucket, name }),
        2 => (1_u32..=8).prop_map(Op::Expire),
    ]
}

/// Applies one step at `now` and returns the command's answer. A guarded
/// step names a token no stream has.
fn step(machine: &mut StreamStateMachine, now: &mut u64, op: &Op) -> Option<StreamResponse> {
    step_guarded(machine, now, op, NO_TOKEN)
}

/// A token no stream has: a stream's token is never 0.
const NO_TOKEN: u64 = 0;

/// [`step`] with a guarded step naming `incarnation`.
fn step_guarded(
    machine: &mut StreamStateMachine,
    now: &mut u64,
    op: &Op,
    incarnation: u64,
) -> Option<StreamResponse> {
    match op {
        Op::Restore => {
            *machine = StreamStateMachine::restore(machine.snapshot()).expect("restore");
            None
        }
        Op::Advance(by) => {
            *now = now.saturating_add(*by);
            None
        }
        _ => op
            .command(*now, incarnation)
            .map(|command| machine.apply(command)),
    }
}

/// What a client sees of an answer: its success kind or its error code. A
/// read's `TouchStreamAccess` and a `DELETE` of an expired stream answer a
/// client 404, as a missing stream does. A purge's count of removed streams counts
/// expired streams the sweep has not removed yet, which is documented.
fn answer_class(response: &StreamResponse) -> String {
    match response {
        StreamResponse::Error { code, .. } => format!("{code:?}"),
        StreamResponse::Accessed { expired: true, .. }
        | StreamResponse::Deleted { expired: true } => {
            format!("{:?}", StreamErrorCode::StreamNotFound)
        }
        StreamResponse::BucketPurged { .. } => "BucketPurged".to_owned(),
        other => format!("{other:?}"),
    }
}

/// What a client reads of a stream at `now_ms`.
fn read_class(machine: &StreamStateMachine, stream_id: &BucketStreamId, now_ms: u64) -> String {
    match machine.read_plan_at(stream_id, 0, 64, now_ms) {
        Ok(plan) => format!("ok:{}", plan.next_offset),
        Err(response) => answer_class(&response),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// Each `Sweep` removes exactly the first `min(limit, due)`
    /// streams expired at its clock in (expiry, bucket, stream) order and
    /// changes nothing of a live stream. A leader that proposes it until an
    /// entry removes fewer than `limit` leaves nothing due, in at most one
    /// entry per `limit` due streams and one more. With the front not due,
    /// no stream has expired.
    #[test]
    fn expire_streams_removes_the_earliest_due_streams(
        ops in vec(op_strategy(), 1..=80),
    ) {
        let mut machine = machine();
        let mut now = 1_000_u64;
        for op in &ops {
            let Op::Expire(max) = op else {
                step(&mut machine, &mut now, op);
                if !machine.expiry_due(now) {
                    prop_assert!(due(&machine, now).is_empty(), "{op:?} at {now}");
                }
                continue;
            };
            let expected = due(&machine, now);
            let before = live(&machine);
            let mut order = expected.clone();
            let mut entries = 0_usize;
            loop {
                let removed = removed_by(&mut machine, expire(now, *max));
                entries = entries.saturating_add(1);
                let take = order.len().min(usize::try_from(*max).expect("u32 fits usize"));
                let mut taken = order.drain(..take).collect::<Vec<_>>();
                taken.sort_by(compare_stream_ids);
                prop_assert_eq!(&removed, &taken);
                if removed.len() < usize::try_from(*max).expect("u32 fits usize") {
                    break;
                }
            }
            prop_assert!(order.is_empty(), "left {order:?}");
            prop_assert!(due(&machine, now).is_empty());
            prop_assert!(!machine.expiry_due(now));
            let bound = expected
                .len()
                .div_ceil(usize::try_from(*max).expect("u32 fits usize"))
                .saturating_add(1);
            prop_assert!(entries <= bound, "{entries} entries for {} due", expected.len());
            for (id, metadata) in &live(&machine) {
                prop_assert_eq!(Some(metadata), before.get(id), "{} changed", id);
            }
        }
    }

    /// A `Sweep` applied to a state and to that state restored
    /// from its snapshot gives the same answer and the same snapshot: a
    /// replica that restored and one that replayed agree.
    #[test]
    fn expire_streams_agrees_across_restore(
        ops in vec(op_strategy(), 1..=60),
        max in 1_u32..=8,
    ) {
        let mut machine = machine();
        let mut now = 1_000_u64;
        for op in &ops {
            step(&mut machine, &mut now, op);
        }
        let mut restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
        let response = machine.apply(expire(now, max));
        prop_assert_eq!(restored.apply(expire(now, max)), response);
        prop_assert_eq!(restored.snapshot(), machine.snapshot());
    }

    /// The key regression of bounded-state F0: a command that names a
    /// stream removes at most that stream, and only when it was deleted or
    /// had expired at the command's `now_ms`. A purge removes streams of
    /// its bucket only. Only `Sweep` removes streams it does not
    /// name.
    #[test]
    fn no_command_removes_a_stream_it_does_not_name(
        ops in vec(op_strategy(), 1..=80),
    ) {
        let mut machine = machine();
        let mut now = 1_000_u64;
        for op in &ops {
            let before = live(&machine);
            let at = now;
            step(&mut machine, &mut now, op);
            let after = live(&machine);
            let removed = before
                .iter()
                .filter(|(id, _)| !after.contains_key(*id))
                .collect::<Vec<_>>();
            match op {
                Op::Expire(_) => {}
                Op::Purge { bucket } => {
                    for (id, _) in &removed {
                        prop_assert_eq!(id.bucket_id.as_str(), BUCKETS[*bucket], "{:?}", op);
                    }
                }
                Op::Delete { .. } => {
                    let named = op.named();
                    for (id, _) in &removed {
                        prop_assert_eq!(Some(*id), named.as_ref(), "{:?}", op);
                    }
                }
                _ => {
                    let named = op.named();
                    for (id, metadata) in &removed {
                        prop_assert_eq!(Some(*id), named.as_ref(), "{:?} removed {}", op, id);
                        prop_assert!(
                            stream_is_expired(metadata, at),
                            "{op:?} removed {id}, live at {at}"
                        );
                    }
                }
            }
        }
    }

    /// Two replicas of one workload, one that never sweeps and one with
    /// `Sweep` entries in between, give a client the same answers:
    /// `HEAD`, reads, liveness, producer early answers and every write,
    /// producer writes, writes under `Stream-Incarnation` and `DELETE`
    /// included. Only a purge's count of removed streams and usage depend
    /// on when the sweep ran.
    #[test]
    fn reads_do_not_depend_on_removal_timing(
        ops in vec(
            prop_oneof![
                8 => guarded_client_op_strategy().prop_map(Some),
                2 => Just(None::<Op>),
            ],
            1..=80,
        ),
        max in 1_u32..=8,
    ) {
        let mut unswept = machine();
        let mut swept = machine();
        let mut now = 1_000_u64;
        for op in &ops {
            let Some(op) = op else {
                swept.apply(expire(now, max));
                continue;
            };
            // A matching precondition names the token of the stream the
            // replica that never sweeps holds, expired or not.
            let incarnation = match op {
                Op::Guarded { op, matching: true } => op
                    .named()
                    .and_then(|stream_id| unswept.stream_incarnation(&stream_id))
                    .unwrap_or(NO_TOKEN),
                _ => NO_TOKEN,
            };
            let mut swept_now = now;
            let unswept_answer = step_guarded(&mut unswept, &mut now, op, incarnation);
            let swept_answer = step_guarded(&mut swept, &mut swept_now, op, incarnation);
            prop_assert_eq!(
                unswept_answer.as_ref().map(answer_class),
                swept_answer.as_ref().map(answer_class),
                "{:?} at {}",
                op,
                now
            );
            for stream_id in every_stream() {
                prop_assert_eq!(
                    unswept.head_at(&stream_id, now),
                    swept.head_at(&stream_id, now),
                    "head of {} at {}",
                    stream_id,
                    now
                );
                prop_assert_eq!(
                    unswept.stream_is_live(&stream_id, now),
                    swept.stream_is_live(&stream_id, now)
                );
                prop_assert_eq!(
                    read_class(&unswept, &stream_id, now),
                    read_class(&swept, &stream_id, now),
                    "read of {} at {}",
                    stream_id,
                    now
                );
                let token = unswept.stream_incarnation(&stream_id).unwrap_or(NO_TOKEN);
                for (epoch, seq) in [(1, 0), (1, 1), (2, 0)] {
                    let producer = producer(epoch, seq);
                    prop_assert_eq!(
                        unswept.producer_early_answer(&stream_id, &producer, token, now),
                        swept.producer_early_answer(&stream_id, &producer, token, now),
                        "early answer of {} at {}",
                        stream_id,
                        now
                    );
                }
            }
        }
    }
}

/// A front that went stale, of a stream deleted, recreated, renewed or
/// erased since its entry was armed, removes nothing and is cleared, so the
/// next sweep finds nothing due.
#[test]
fn a_stale_due_front_removes_nothing_and_clears() {
    for stale in ["deleted", "recreated", "renewed", "erased"] {
        let mut machine = machine();
        let named = stream(0, 0);
        machine.apply(create(named.clone(), Some(1), None, 1_000));
        // Streams that expire much later keep the stale entry in the heap
        // (a rebuild drops stale entries once they outnumber armed ones).
        for name in 1..=2 {
            machine.apply(create(stream(1, name), None, Some(1_000_000), 1_000));
        }
        assert!(machine.expiry_due(2_500), "{stale}");
        match stale {
            "deleted" => {
                machine.apply(StreamCommand::DeleteStream {
                    stream_id: named.clone(),
                    now_ms: 1_500,
                });
            }
            "recreated" => {
                machine.apply(StreamCommand::DeleteStream {
                    stream_id: named.clone(),
                    now_ms: 1_500,
                });
                machine.apply(create(named.clone(), None, Some(60_000), 1_500));
            }
            "renewed" => {
                machine.apply(StreamCommand::TouchStreamAccess {
                    stream_id: named.clone(),
                    now_ms: 1_900,
                    renew_ttl: true,
                });
            }
            _ => {
                machine.apply(StreamCommand::PurgeBucket {
                    bucket_id: BUCKETS[0].to_owned(),
                    now_ms: 1_500,
                });
            }
        }
        let before = machine.snapshot();
        assert!(machine.expiry_due(2_500), "{stale}: the front is stale");
        assert_eq!(
            machine.apply(expire(2_500, 256)),
            StreamResponse::Swept {
                expired: 0,
                retired: 0,
            },
            "{stale}"
        );
        assert_eq!(machine.snapshot(), before, "{stale}");
        assert!(!machine.expiry_due(2_500), "{stale}");
    }
}

/// `Sweep` never renews a live stream's sliding TTL.
#[test]
fn expire_streams_never_renews_a_ttl() {
    let mut machine = machine();
    let named = stream(0, 0);
    machine.apply(create(named.clone(), Some(10), None, 1_000));
    let before = live(&machine)[&named].clone();
    for now_ms in [4_000, 8_000, 10_999] {
        assert_eq!(machine.apply(expire(now_ms, 256)), StreamResponse::Swept {
            expired: 0,
            retired: 0,
        });
        assert_eq!(live(&machine)[&named], before);
    }
    assert_eq!(machine.apply(expire(11_000, 256)), StreamResponse::Swept {
        expired: 1,
        retired: 0,
    });
    assert!(!live(&machine).contains_key(&named));
}

/// With nothing due, `Sweep` changes nothing.
#[test]
fn nothing_due_changes_nothing() {
    let mut machine = machine();
    machine.apply(create(stream(0, 0), Some(10), None, 1_000));
    machine.apply(create(stream(1, 0), None, Some(50_000), 1_000));
    machine.apply(create(stream(1, 1), None, None, 1_000));
    let before = machine.snapshot();
    assert!(!machine.expiry_due(5_000));
    assert_eq!(machine.apply(expire(5_000, 256)), StreamResponse::Swept {
        expired: 0,
        retired: 0,
    });
    assert_eq!(machine.snapshot(), before);
    assert_eq!(machine.removed_state().streams, 0);
}

/// Streams with one expiry go by bucket, then by stream id.
#[test]
fn equal_expiries_go_by_bucket_then_stream() {
    let mut machine = machine();
    for bucket in [1, 0] {
        for name in [2, 0, 1] {
            machine.apply(create(stream(bucket, name), None, Some(2_000), 1_000));
        }
    }
    let mut order = Vec::new();
    loop {
        let removed = removed_by(&mut machine, expire(3_000, 1));
        if removed.is_empty() {
            break;
        }
        assert_eq!(removed.len(), 1);
        order.extend(removed);
    }
    let expected = [0, 1]
        .into_iter()
        .flat_map(|bucket| (0..3).map(move |name| stream(bucket, name)))
        .collect::<Vec<_>>();
    assert_eq!(order, expected);
}

/// `head_at` filters an expired stream and leaves it in state: only a
/// replicated command removes it.
#[test]
fn head_at_never_removes() {
    let mut machine = machine();
    let named = stream(0, 0);
    machine.apply(create(named.clone(), None, Some(2_000), 1_000));
    let before = machine.snapshot();
    assert!(machine.head_at(&named, 1_999).is_some());
    assert!(machine.head_at(&named, 2_000).is_none());
    assert_eq!(machine.snapshot(), before);
    assert_eq!(machine.removed_state().streams, 0);
    assert!(machine.expiry_due(2_000));
}

/// A `DELETE` of a stream expired at its clock removes it and answers
/// `Deleted { expired: true }`, which a client sees as 404, as for a
/// missing stream, whether or not the sweep ran. Under a
/// `Stream-Incarnation` precondition it applies the same way.
#[test]
fn a_delete_of_an_expired_stream_answers_not_found() {
    let named = stream(0, 0);
    for precondition in [false, true] {
        let mut machine = machine();
        machine.apply(create(named.clone(), None, Some(2_000), 1_000));
        let incarnation = machine.stream_incarnation(&named).expect("created");
        let delete = |now_ms| {
            let command = StreamCommand::DeleteStream {
                stream_id: named.clone(),
                now_ms,
            };
            if precondition {
                StreamCommand::IfIncarnation {
                    incarnation,
                    command: Box::new(command),
                }
            } else {
                command
            }
        };
        assert_eq!(
            machine.apply(delete(2_000)),
            StreamResponse::Deleted { expired: true },
            "{precondition}"
        );
        assert!(live(&machine).is_empty(), "{precondition}");
        assert_eq!(machine.removed_state().streams, 1, "{precondition}");
        // Once removed, it answers the same.
        let response = machine.apply(delete(2_000));
        assert!(
            matches!(&response, StreamResponse::Error {
                code: StreamErrorCode::StreamNotFound,
                ..
            }),
            "{precondition}: {response:?}"
        );
    }
    // A live stream is deleted as before.
    let mut machine = machine();
    machine.apply(create(named.clone(), None, Some(2_000), 1_000));
    assert_eq!(
        machine.apply(StreamCommand::DeleteStream {
            stream_id: named.clone(),
            now_ms: 1_999,
        }),
        StreamResponse::Deleted { expired: false }
    );
}

/// A create, an append and a close at a clock past every expiry remove
/// none of the expired streams they do not name. `Sweep` does, in
/// batches of its limit.
#[test]
fn writes_never_remove_expired_streams_they_do_not_name() {
    let mut machine = machine();
    for name in 0..300 {
        machine.apply(create(stream(1, name), None, Some(2_000), 1_000));
    }
    let writer = stream(0, 0);
    let commands = [
        create(writer.clone(), None, None, 5_000),
        append(writer.clone(), 5_000),
        StreamCommand::Close {
            stream_id: writer.clone(),
            stream_seq: None,
            producer: None,
            now_ms: 5_000,
        },
    ];
    for command in commands {
        assert!(removed_by(&mut machine, command).is_empty());
    }
    assert_eq!(machine.removed_state().streams, 0);
    assert_eq!(machine.apply(expire(5_000, 256)), StreamResponse::Swept {
        expired: 256,
        retired: 0,
    });
    assert_eq!(machine.apply(expire(5_000, 256)), StreamResponse::Swept {
        expired: 44,
        retired: 0,
    });
    let remaining = live(&machine).into_keys().collect::<HashSet<_>>();
    assert_eq!(remaining, HashSet::from([writer]));
}
