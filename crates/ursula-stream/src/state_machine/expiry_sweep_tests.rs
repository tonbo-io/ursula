//! #669: the leader's background expiry sweep on the state machine. The
//! leader names the front of the TTL index
//! ([`StreamStateMachine::expiry_sweep_target`]) in a
//! `TouchStreamAccess { renew_ttl: false }` until the front is no longer
//! due, and every replica's apply decides what goes: the named stream if it
//! expired, then up to 256 more in (expiry, stream id) order.

use std::collections::HashMap;
use std::collections::HashSet;

use proptest::collection::vec;
use proptest::prelude::*;
use ursula_shard::BucketStreamId;

use super::StreamStateMachine;
use super::TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE;
use super::compare_stream_ids;
use super::stream_expiry_at_ms;
use crate::command::StreamCommand;
use crate::model::StreamMetadata;
use crate::response::StreamErrorCode;
use crate::response::StreamResponse;

const BUCKETS: [&str; 2] = ["alpha", "beta"];

fn stream(bucket: usize, name: usize) -> BucketStreamId {
    BucketStreamId::new(BUCKETS[bucket], format!("s-{name}"))
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
        content_type: "application/octet-stream".to_owned(),
        initial_payload: bytes::Bytes::new(),
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
        content_type: Some("application/octet-stream".to_owned()),
        payload: bytes::Bytes::from_static(b"record;"),
        close_after: false,
        stream_seq: None,
        producer: None,
        now_ms,
    }
}

fn sweep_command(stream_id: BucketStreamId, now_ms: u64) -> StreamCommand {
    StreamCommand::TouchStreamAccess {
        stream_id,
        now_ms,
        renew_ttl: false,
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

/// The live streams expired at `now_ms`, in (expiry, stream id) order: the
/// reference the TTL index must agree with.
fn due(machine: &StreamStateMachine, now_ms: u64) -> Vec<(u64, BucketStreamId)> {
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
    due
}

/// The leader's loop of one pass (`ursula_runtime::expiry_sweep`): while
/// the TTL front is due at `now_ms`, apply the command that names it, at
/// most `max` times. Returns the streams each command removed.
fn sweep_like_a_leader(
    machine: &mut StreamStateMachine,
    now_ms: u64,
    max: usize,
) -> Vec<Vec<BucketStreamId>> {
    let mut removed = Vec::new();
    while removed.len() < max {
        let Some(target) = machine.expiry_sweep_target(now_ms) else {
            break;
        };
        let before = live(machine);
        machine.apply(sweep_command(target, now_ms));
        let after = live(machine);
        removed.push(
            before
                .keys()
                .filter(|id| !after.contains_key(*id))
                .cloned()
                .collect(),
        );
    }
    removed
}

#[derive(Debug, Clone)]
enum Ttl {
    Sliding(u64),
    Absolute(u64),
    None,
}

#[derive(Debug, Clone)]
enum Op {
    Create {
        bucket: usize,
        name: usize,
        ttl: Ttl,
    },
    Renew {
        bucket: usize,
        name: usize,
    },
    Append {
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
    Restore,
    Advance(u64),
    Sweep,
}

fn ttl_strategy() -> impl Strategy<Value = Ttl> {
    prop_oneof![
        (1_u64..=5).prop_map(Ttl::Sliding),
        (0_u64..=6_000).prop_map(Ttl::Absolute),
        Just(Ttl::None),
    ]
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0_usize..2, 0_usize..24, ttl_strategy())
            .prop_map(|(bucket, name, ttl)| Op::Create { bucket, name, ttl }),
        2 => (0_usize..2, 0_usize..24).prop_map(|(bucket, name)| Op::Renew { bucket, name }),
        1 => (0_usize..2, 0_usize..24).prop_map(|(bucket, name)| Op::Append { bucket, name }),
        1 => (0_usize..2, 0_usize..24).prop_map(|(bucket, name)| Op::Delete { bucket, name }),
        1 => (0_usize..2).prop_map(|bucket| Op::Purge { bucket }),
        1 => Just(Op::Restore),
        3 => (0_u64..=1_500).prop_map(Op::Advance),
        2 => Just(Op::Sweep),
    ]
}

/// Applies one workload step at `now`. Appends and renewals renew sliding
/// TTLs, and, like every write but an engine's plain append, sweep at their
/// clock too.
fn step(machine: &mut StreamStateMachine, now: &mut u64, op: &Op) {
    match op {
        Op::Create { bucket, name, ttl } => {
            let (ttl_seconds, expires_at_ms) = match ttl {
                Ttl::Sliding(seconds) => (Some(*seconds), None),
                Ttl::Absolute(after) => (None, Some(now.saturating_add(*after))),
                Ttl::None => (None, None),
            };
            machine.apply(create(
                stream(*bucket, *name),
                ttl_seconds,
                expires_at_ms,
                *now,
            ));
        }
        Op::Renew { bucket, name } => {
            machine.apply(StreamCommand::TouchStreamAccess {
                stream_id: stream(*bucket, *name),
                now_ms: *now,
                renew_ttl: true,
            });
        }
        Op::Append { bucket, name } => {
            machine.apply(append(stream(*bucket, *name), *now));
        }
        Op::Delete { bucket, name } => {
            machine.apply(StreamCommand::DeleteStream {
                stream_id: stream(*bucket, *name),
            });
        }
        Op::Purge { bucket } => {
            machine.apply(StreamCommand::PurgeBucket {
                bucket_id: BUCKETS[*bucket].to_owned(),
            });
        }
        Op::Restore => {
            *machine = StreamStateMachine::restore(machine.snapshot()).expect("restore");
        }
        Op::Advance(by) => *now = now.saturating_add(*by),
        Op::Sweep => {}
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    /// #669: the leader's sweep removes exactly the streams expired at its
    /// clock, each command at most 257 of them in (expiry, stream id)
    /// order, in at most one command per 256 due streams and one more, and
    /// it changes nothing of a live stream. With the front not due no
    /// stream has expired.
    #[test]
    fn expiry_sweep_removes_exactly_the_due_streams(
        ops in vec(op_strategy(), 1..=80),
    ) {
        let mut machine = machine();
        let mut now = 1_000_u64;
        for op in &ops {
            step(&mut machine, &mut now, op);
            if machine.expiry_sweep_target(now).is_none() {
                prop_assert!(due(&machine, now).is_empty(), "{op:?} at {now}");
            }
            if !matches!(op, Op::Sweep) {
                continue;
            }
            let expected = due(&machine, now);
            let before = live(&machine);
            let removed = sweep_like_a_leader(&mut machine, now, usize::MAX);
            let after = live(&machine);
            prop_assert!(due(&machine, now).is_empty());
            prop_assert!(machine.expiry_sweep_target(now).is_none());
            let mut order = expected.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();
            for entry in &removed {
                prop_assert!(entry.len() <= TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE + 1);
                // Each command takes the earliest due streams.
                let taken = order.drain(..entry.len()).collect::<HashSet<_>>();
                prop_assert_eq!(&taken, &entry.iter().cloned().collect::<HashSet<_>>());
            }
            prop_assert!(order.is_empty(), "left {order:?}");
            let commands = removed.len();
            prop_assert!(
                commands
                    <= expected
                        .len()
                        .div_ceil(TTL_EXPIRY_SWEEP_MAX_STREAMS_PER_WRITE)
                        .saturating_add(1),
                "{commands} commands for {} due streams",
                expected.len()
            );
            for (id, metadata) in &after {
                prop_assert_eq!(Some(metadata), before.get(id), "{} changed", id);
            }
        }
    }

    /// #669: the sweep's command applied to a state and to that state
    /// restored from its snapshot gives the same answer and the same
    /// snapshot: a replica that restored and one that replayed agree.
    #[test]
    fn expiry_sweep_agrees_across_restore(
        ops in vec(op_strategy(), 1..=60),
        name in 0_usize..24,
        bucket in 0_usize..2,
    ) {
        let mut machine = machine();
        let mut now = 1_000_u64;
        for op in &ops {
            step(&mut machine, &mut now, op);
        }
        let mut restored = StreamStateMachine::restore(machine.snapshot()).expect("restore");
        let target = machine
            .expiry_sweep_target(now)
            .unwrap_or_else(|| stream(bucket, name));
        let response = machine.apply(sweep_command(target.clone(), now));
        prop_assert_eq!(restored.apply(sweep_command(target, now)), response);
        prop_assert_eq!(restored.snapshot(), machine.snapshot());
    }
}

/// A name that went stale between the leader's pick and the apply is
/// refused, or kept live, but the command still sweeps at its clock.
#[test]
fn a_stale_sweep_target_still_sweeps() {
    let now = 5_000;
    for stale in ["deleted", "recreated", "renewed", "erased"] {
        let mut machine = machine();
        let named = stream(0, 0);
        machine.apply(create(named.clone(), Some(1), None, 1_000));
        for name in 1..=3 {
            machine.apply(create(stream(1, name), None, Some(2_000), 1_000));
        }
        assert_eq!(machine.expiry_sweep_target(now), Some(named.clone()));
        let refusal = match stale {
            "deleted" => {
                machine.apply(StreamCommand::DeleteStream {
                    stream_id: named.clone(),
                });
                Some(StreamErrorCode::StreamNotFound)
            }
            "recreated" => {
                machine.apply(StreamCommand::DeleteStream {
                    stream_id: named.clone(),
                });
                machine.apply(create(named.clone(), None, Some(60_000), 1_500));
                None
            }
            "renewed" => {
                // The leader's clock is behind the renewal's.
                machine.apply(StreamCommand::TouchStreamAccess {
                    stream_id: named.clone(),
                    now_ms: 1_900,
                    renew_ttl: true,
                });
                None
            }
            _ => {
                machine.apply(StreamCommand::PurgeBucket {
                    bucket_id: BUCKETS[0].to_owned(),
                });
                Some(StreamErrorCode::BucketErased)
            }
        };
        let renewed_until = live(&machine).get(&named).and_then(stream_expiry_at_ms);
        let response = machine.apply(sweep_command(named.clone(), 2_500));
        match refusal {
            Some(code) => assert!(
                matches!(&response, StreamResponse::Error { code: answered, .. } if *answered == code),
                "{stale}: {response:?}"
            ),
            None => assert_eq!(
                response,
                StreamResponse::Accessed {
                    changed: false,
                    expired: false,
                },
                "{stale}"
            ),
        }
        // The other bucket's expired streams went all the same.
        let after = live(&machine);
        assert!(
            (1..=3).all(|name| !after.contains_key(&stream(1, name))),
            "{stale}: {after:?}"
        );
        if stale == "renewed" || stale == "recreated" {
            assert_eq!(
                after.get(&named).and_then(stream_expiry_at_ms),
                renewed_until
            );
        }
    }
}

/// `renew_ttl: false` never renews a live stream's sliding TTL.
#[test]
fn a_sweep_never_renews_a_ttl() {
    let mut machine = machine();
    let named = stream(0, 0);
    machine.apply(create(named.clone(), Some(10), None, 1_000));
    let before = live(&machine)[&named].clone();
    for now_ms in [4_000, 8_000, 10_999] {
        assert_eq!(
            machine.apply(sweep_command(named.clone(), now_ms)),
            StreamResponse::Accessed {
                changed: false,
                expired: false,
            }
        );
        assert_eq!(live(&machine)[&named], before);
    }
    assert_eq!(
        machine.apply(sweep_command(named.clone(), 11_000)),
        StreamResponse::Accessed {
            changed: true,
            expired: true,
        }
    );
    assert!(!live(&machine).contains_key(&named));
}
