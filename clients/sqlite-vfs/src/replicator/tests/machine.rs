//! The machine's own rules: continuation claims, retries and backoff, the HEAD check, blocked
//! conditions, batching, the closed-seq cross-check and `synced()`.

use ursula_headers::ClusterId;
use ursula_headers::Delivery;
use ursula_headers::IncarnationMismatch;
use ursula_headers::ProducerAnswer;
use ursula_headers::ServerIdentity;
use ursula_headers::SizeLimit;
use ursula_headers::StreamHead;

use super::ACTIVATION;
use super::Harness;
use super::LINEAGE;
use super::cluster;
use super::commit;
use super::epoch;
use super::local;
use super::off;
use super::producer;
use super::read;
use super::receipt;
use super::reset_of;
use super::sends;
use super::timer_of;
use super::token;
use crate::codec::frame;
use crate::ids::Activation;
use crate::ids::Position;
use crate::replicator::Action;
use crate::replicator::BlockedReason;
use crate::replicator::ClaimKind;
use crate::replicator::Condition;
use crate::replicator::Event;
use crate::replicator::Refusal;
use crate::replicator::Replicator;
use crate::replicator::RequestId;
use crate::replicator::ResetKind;
use crate::replicator::TAKEOVER_ROUNDS;
use crate::replicator::backoff::BACKOFF_CAP_MS;
use crate::replicator::backoff::BLOCKED_RETRY_MS;
use crate::replicator::backoff::RETRY_AFTER_CAP_MS;
use crate::replicator::backoff::backoff_ms;
use crate::wire::Exchange;
use crate::wire::Method;
use crate::wire::NoAnswer;
use crate::wire::RETRY_AFTER;
use crate::wire::RawAnswer;

/// Streaming at epoch 1 from offset 0, with the claim at 0..37 and records `1..=n` pending: the
/// first batch is in flight.
fn streaming(n: u64) -> Harness {
    let pending = (1..=n).map(|p| commit(LINEAGE, p, 1)).collect();
    let (mut h, actions) = Harness::activate(ClaimKind::Takeover { round: 1 }, local(pending, 0));
    assert!(matches!(actions.first(), Some(Action::PersistClaim { .. })));
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, 37, None)));
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
    h
}

fn head(token_: ursula_headers::Incarnation, cluster_id: ClusterId, max: u64) -> RawAnswer {
    let mut raw = RawAnswer::new(200);
    ServerIdentity::current(cluster_id).render(raw.emit());
    StreamHead::new(token_, off(0), off(0), max)
        .unwrap()
        .render(raw.emit());
    raw
}

fn condition(actions: &[Action]) -> Option<Option<Condition>> {
    actions.iter().rev().find_map(|a| match a {
        Action::Condition(c) => Some(c.clone()),
        _ => None,
    })
}

#[test]
fn a_batch_takes_every_pending_record_and_the_ack_advances() {
    let mut h = streaming(3);
    let body = h.last_request().body.clone();
    let expected: Vec<u8> = (1..=3)
        .flat_map(|p| commit(LINEAGE, p, 1).frame.to_vec())
        .collect();
    assert_eq!(&*body, expected.as_slice());
    let keys = h.last_request().keys.producer.clone().unwrap();
    assert_eq!((keys.epoch, keys.seq), (epoch(1), super::seq(1)));
    let len = u64::try_from(body.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    assert!(actions.contains(&Action::Wake {
        acked: Position(3),
        offset: off(37 + len),
    }));
}

#[test]
fn a_batch_ends_exactly_at_a_boundary_and_within_the_limit() {
    let pending: Vec<_> = (1..=4).map(|p| commit(LINEAGE, p, 1)).collect();
    let mut state = local(pending.clone(), 0);
    state.configured_max_request_bytes = Some(
        u64::try_from(pending[0].frame.len() + pending[1].frame.len() + pending[2].frame.len())
            .unwrap(),
    );
    let (mut h, _) = Harness::activate(ClaimKind::Takeover { round: 1 }, state);
    h.on(Event::Boundary(Position(2)));
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, 37, None)));
    let body = h.last_request().body.clone();
    assert_eq!(body.len(), pending[0].frame.len() + pending[1].frame.len());
    let len = u64::try_from(body.len()).unwrap();
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    // Past the boundary, the limit cuts: records 3 and 4 do not fit with the limit at 3 frames
    // only when their sum exceeds it.
    let body = h.last_request().body.clone();
    assert_eq!(body.len(), pending[2].frame.len() + pending[3].frame.len());
}

#[test]
fn a_lost_answer_resends_the_identical_request_on_a_fresh_connection_after_backoff() {
    let mut h = streaming(2);
    let first = h.last_request().clone();
    let actions = h.lose();
    assert_eq!(condition(&actions), Some(Some(Condition::Retrying)));
    let (at, jitter) = timer_of(&actions).unwrap();
    assert_eq!((at, jitter), (h.now_ms, backoff_ms(1)));
    // Commits logged meanwhile do not change the sealed batch.
    h.on(Event::Logged(commit(LINEAGE, 3, 1)));
    let actions = h.tick_at(at);
    let Some(Action::Send {
        request, attempt, ..
    }) = actions.first()
    else {
        panic!("{actions:?}")
    };
    assert_eq!(request, &first);
    assert!(attempt.fresh_connection);
    // Backoff doubles up to its cap.
    assert_eq!(backoff_ms(2), 50);
    assert_eq!(backoff_ms(30), BACKOFF_CAP_MS);
}

#[test]
fn retry_after_is_a_floor_capped_at_a_minute() {
    let mut h = streaming(1);
    let mut raw = RawAnswer::new(503);
    raw.header(RETRY_AFTER, "7");
    let actions = h.answer(&raw);
    assert_eq!(timer_of(&actions).unwrap().0, h.now_ms + 7_000);
    // Not before it.
    let early = h.tick_at(h.now_ms + 6_999);
    assert!(sends(&early).is_empty());
    h.tick_at(h.now_ms + 1);
    let mut raw = RawAnswer::new(429);
    raw.header(RETRY_AFTER, "3600");
    let actions = h.answer(&raw);
    assert_eq!(timer_of(&actions).unwrap().0, h.now_ms + RETRY_AFTER_CAP_MS);
}

#[test]
fn a_401_rereads_the_token_once_then_blocks() {
    let mut h = streaming(1);
    let actions = h.answer(&RawAnswer::new(401));
    let Some(Action::Send { attempt, .. }) = actions.first() else {
        panic!("{actions:?}")
    };
    assert!(attempt.refresh_token);
    let actions = h.answer(&RawAnswer::new(401));
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::Unauthorized)))
    );
    assert_eq!(timer_of(&actions).unwrap().0, h.now_ms + BLOCKED_RETRY_MS);
    // Blocked requests go out again every 30 s, and a definite answer clears the condition.
    let actions = h.tick_at(h.now_ms + BLOCKED_RETRY_MS);
    assert_eq!(sends(&actions).len(), 1);
    let len = u64::try_from(h.last_request().body.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    assert_eq!(condition(&actions), Some(None));
}

#[test]
fn redirects_are_resent_a_few_times_then_block() {
    let mut h = streaming(1);
    for _ in 0..3 {
        let actions = h.answer(&RawAnswer::new(307));
        let (at, jitter) = timer_of(&actions).unwrap();
        h.tick_at(at + jitter);
    }
    let actions = h.answer(&RawAnswer::new(307));
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::Redirected {
            location: None
        })))
    );
}

#[test]
fn a_typed_413_lowers_the_limit_and_blocks() {
    let mut h = streaming(1);
    let mut raw = RawAnswer::new(SizeLimit::STATUS);
    SizeLimit::Request(16).render(raw.emit());
    let actions = h.answer(&raw);
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::RequestTooLarge)))
    );
    assert_eq!(h.replicator.ctx().unwrap().limits.request(), 16);
}

#[test]
fn a_412_checks_head_and_our_token_unchanged_retries() {
    let mut h = streaming(1);
    let append = h.last_request().clone();
    let mut raw = RawAnswer::new(IncarnationMismatch::STATUS);
    IncarnationMismatch {
        current: Some(token()),
    }
    .render(raw.emit());
    h.answer(&raw);
    assert_eq!(h.last_request().method, Method::Head);
    assert_eq!(h.last_request().keys.precondition, None);
    let actions = h.answer(&head(token(), cluster(), 1 << 20));
    let (at, jitter) = timer_of(&actions).unwrap();
    let actions = h.tick_at(at + jitter);
    assert_eq!(sends(&actions), vec![&append]);
    assert_eq!(h.replicator.ctx().unwrap().limits.request(), 1 << 20);
}

#[test]
fn head_resets_rewound_or_deleted_and_blocks_another_cluster() {
    let other = ursula_headers::Incarnation::try_from((1 << 52) | 99).unwrap();
    let mut h = streaming(1);
    h.answer(&RawAnswer::new(404));
    let actions = h.answer(&head(other, cluster(), 1 << 20));
    assert_eq!(reset_of(&actions), Some(ResetKind::Rewound));

    let mut h = streaming(1);
    h.answer(&RawAnswer::new(404));
    let mut missing = RawAnswer::new(404);
    ServerIdentity::current(cluster()).render(missing.emit());
    let actions = h.answer(&missing);
    assert_eq!(reset_of(&actions), Some(ResetKind::Deleted));

    let mut h = streaming(1);
    h.answer(&RawAnswer::new(404));
    let actions = h.answer(&head(
        token(),
        ClusterId::new("cluster-b").unwrap(),
        1 << 20,
    ));
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::WrongCluster)))
    );
}

#[test]
fn producer_expired_claims_exactly_own_plus_one_after_persisting_it() {
    let mut h = streaming(2);
    let actions = h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(1),
    }));
    assert_eq!(
        actions.first(),
        Some(&Action::PersistClaim {
            epoch: epoch(2),
            activation: ACTIVATION,
        })
    );
    let claim = h.last_request().keys.producer.clone().unwrap();
    assert_eq!((claim.epoch, claim.seq), (epoch(2), super::seq(0)));
    assert_eq!(
        &*h.last_request().body,
        frame::encode_claim(epoch(2), ACTIVATION).as_slice()
    );
    // The continuation loses at the first answer but its receipt.
    let actions = h.answer(&producer(&ProducerAnswer::StaleEpoch { epoch: epoch(5) }));
    assert_eq!(reset_of(&actions), Some(ResetKind::Fenced));
}

#[test]
fn a_takeover_retries_above_every_epoch_seen_then_gives_up() {
    let (mut h, _) = Harness::activate(ClaimKind::Takeover { round: 1 }, local(Vec::new(), 0));
    let actions = h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(40),
    }));
    assert!(actions.contains(&Action::PersistClaim {
        epoch: epoch(41),
        activation: ACTIVATION,
    }));
    for round in 2..TAKEOVER_ROUNDS {
        let e = 41 + u64::from(round) - 2;
        h.answer(&producer(&ProducerAnswer::KeyConflict { epoch: epoch(e) }));
    }
    let actions = h.answer(&producer(&ProducerAnswer::KeyConflict { epoch: epoch(99) }));
    assert!(
        actions.contains(&Action::Refuse(Refusal::ClaimRaces)),
        "{actions:?}"
    );
}

#[test]
fn a_batch_found_above_the_closed_seq_resets_stream_state() {
    // Epoch 1 sends records 1 and 2 at seq 1, and the answer says the epoch expired.
    let mut h = streaming(2);
    let batch = h.last_request().body.clone();
    h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(1),
    }));
    // The continuation claim says it closed epoch 1 at seq 0, yet the batch of seq 1 is in the
    // stream before it.
    let len = u64::try_from(batch.len()).unwrap();
    let closed = ursula_headers::ClosedEpoch {
        epoch: epoch(1),
        seq: super::seq(0),
    };
    h.answer(&producer(&receipt(
        Delivery::Applied,
        2,
        0,
        37 + len,
        74 + len,
        Some(closed),
    )));
    let actions = h.answer(&read(&batch, 37 + len));
    assert_eq!(reset_of(&actions), Some(ResetKind::StreamState));
}

#[test]
fn late_and_duplicate_answers_are_ignored() {
    let mut h = streaming(1);
    let (id, request) = h.last.clone().unwrap();
    h.lose();
    let late = Exchange::Answered(Box::new(
        producer(&ProducerAnswer::KeyConflict { epoch: epoch(1) }).parse(&request),
    ));
    let actions = h.on(Event::Answer { id, exchange: late });
    assert!(actions.is_empty());
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
    let other = Exchange::NoAnswer(NoAnswer::NotSent {
        reason: "x".to_owned(),
    });
    assert!(
        h.on(Event::Answer {
            id: RequestId(999),
            exchange: other
        })
        .is_empty()
    );
}

#[test]
fn synced_waits_for_a_round_trip_that_started_after_the_call() {
    let mut h = streaming(1);
    // The call comes while the batch is in flight: its answer started before the call.
    h.on(Event::Sync {
        ticket: 7,
        target: Position(1),
    });
    let len = u64::try_from(h.last_request().body.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    assert!(!actions.contains(&Action::Synced { ticket: 7 }));
    // So the idle owner probes, a leader read at the tail under the precondition.
    let probe = h.last_request().clone();
    assert_eq!(probe.method, Method::Get);
    assert_eq!(
        probe.keys.precondition.map(|p| p.incarnation),
        Some(token())
    );
    // A second caller shares the probe.
    h.on(Event::Sync {
        ticket: 8,
        target: Position(1),
    });
    let actions = h.answer(&read(&[], 37 + len));
    assert!(actions.contains(&Action::Synced { ticket: 7 }));
    // Ticket 8 came after the probe went out: it needs its own.
    assert!(!actions.contains(&Action::Synced { ticket: 8 }));
    let actions = h.answer(&read(&[], 37 + len));
    assert!(actions.contains(&Action::Synced { ticket: 8 }));
}

#[test]
fn a_probe_that_finds_a_frame_fences_and_undecodable_bytes_reset_stream_state() {
    for (bytes, kind) in [
        (
            frame::encode_claim(epoch(2), Activation([9; 16])),
            ResetKind::Fenced,
        ),
        // A copy's claim or commit under our activation and lineage fences too (design §7).
        (frame::encode_claim(epoch(2), ACTIVATION), ResetKind::Fenced),
        (commit(LINEAGE, 50, 3).frame.to_vec(), ResetKind::Fenced),
        (b"not a frame".to_vec(), ResetKind::StreamState),
    ] {
        let mut h = streaming(0);
        h.on(Event::Sync {
            ticket: 1,
            target: Position(0),
        });
        let len = u64::try_from(bytes.len()).unwrap();
        let actions = h.answer(&read(&bytes, 37 + len));
        assert_eq!(reset_of(&actions), Some(kind));
        assert!(!actions.contains(&Action::Synced { ticket: 1 }));
    }
}

#[test]
fn a_ticket_covered_by_acks_probes_while_the_next_append_is_blocked() {
    let mut h = streaming(1);
    let len = u64::try_from(h.last_request().body.len()).unwrap();
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    // Record 2 is logged and its append blocked by a 402.
    h.on(Event::Logged(commit(LINEAGE, 2, 1)));
    h.on(Event::Sync {
        ticket: 9,
        target: Position(1),
    });
    let actions = h.answer(&RawAnswer::new(402));
    // The ticket for record 1 does not wait on record 2's append: a probe goes out.
    let probes: Vec<_> = sends(&actions)
        .into_iter()
        .filter(|r| r.method == Method::Get)
        .collect();
    assert_eq!(probes.len(), 1, "{actions:?}");
    let actions = h.answer(&read(&[], 37 + len));
    assert!(
        actions.contains(&Action::Synced { ticket: 9 }),
        "{actions:?}"
    );
    // Only the in-flight batch is kept for the closed-seq cross-check.
    assert_eq!(h.replicator.ctx().unwrap().sent.len(), 1);
}

#[test]
fn a_probe_that_finds_our_own_batch_waits_for_its_answer() {
    let mut h = streaming(1);
    let batch = h.last_request().body.clone();
    let len = u64::try_from(batch.len()).unwrap();
    h.on(Event::Sync {
        ticket: 4,
        target: Position(0),
    });
    // The batch's answer is lost, so the probe goes out while its resend waits.
    let actions = h.lose();
    assert_eq!(sends(&actions).len(), 1, "{actions:?}");
    let actions = h.answer(&read(&batch, 37 + len));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(!actions.contains(&Action::Synced { ticket: 4 }));
}

#[test]
fn an_oversized_first_frame_blocks_without_sending() {
    let pending = vec![commit(LINEAGE, 1, 1)];
    let mut state = local(pending, 0);
    state.configured_max_request_bytes = Some(4);
    let (mut h, _) = Harness::activate(ClaimKind::Takeover { round: 1 }, state);
    let actions = h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, 37, None)));
    assert!(sends(&actions).is_empty());
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::RequestTooLarge)))
    );
}

#[test]
fn stop_retires_the_binding() {
    let mut h = streaming(1);
    assert!(h.on(Event::Stop).is_empty());
    assert!(matches!(h.replicator, Replicator::Stopped));
    assert!(h.on(Event::Logged(commit(LINEAGE, 2, 1))).is_empty());
}

#[test]
fn a_stale_probe_never_reads_our_own_acknowledged_batch_as_foreign() {
    let mut h = streaming(0);
    h.on(Event::Sync {
        ticket: 5,
        target: Position(1),
    });
    // An idle probe P from offset 37 gets a 503.
    let mut raw = RawAnswer::new(503);
    raw.header(RETRY_AFTER, "1");
    let actions = h.answer(&raw);
    let (at, jitter) = timer_of(&actions).unwrap();
    // Record 1's append A goes out from 37.
    h.on(Event::Logged(commit(LINEAGE, 1, 1)));
    let (append_id, append) = h.last.clone().unwrap();
    assert_eq!(append.method, Method::Post);
    // P is resent, then A lands and is acknowledged.
    h.tick_at(at + jitter);
    let (probe_id, probe) = h.last.clone().unwrap();
    assert_eq!(probe.method, Method::Get);
    let len = u64::try_from(append.body.len()).unwrap();
    let ack = producer(&receipt(Delivery::Applied, 1, 1, 37, 37 + len, None)).parse(&append);
    let actions = h.on(Event::Answer {
        id: append_id,
        exchange: Exchange::Answered(Box::new(ack)),
    });
    assert!(
        actions.contains(&Action::Synced { ticket: 5 }),
        "{actions:?}"
    );
    // P's answer reads A's bytes: it is stale, and fences nothing.
    let late = read(&append.body, 37 + len).parse(&probe);
    let actions = h.on(Event::Answer {
        id: probe_id,
        exchange: Exchange::Answered(Box::new(late)),
    });
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
}

#[test]
fn a_ticket_on_a_claim_or_ack_position_waits_only_for_the_commits_below_it() {
    // Records 1 and 3 are commits; position 4 is, say, the ACK after them.
    let pending = vec![commit(LINEAGE, 1, 1), commit(LINEAGE, 3, 1)];
    let (mut h, _) = Harness::activate(ClaimKind::Takeover { round: 1 }, local(pending, 0));
    h.on(Event::Sync {
        ticket: 2,
        target: Position(2),
    });
    h.on(Event::Sync {
        ticket: 4,
        target: Position(4),
    });
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, 37, None)));
    let len = u64::try_from(h.last_request().body.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    // Every commit at or below 2 and at or below 4 is acknowledged: both resolve.
    assert!(
        actions.contains(&Action::Synced { ticket: 2 }),
        "{actions:?}"
    );
    assert!(
        actions.contains(&Action::Synced { ticket: 4 }),
        "{actions:?}"
    );
    assert!(actions.contains(&Action::Wake {
        acked: Position(3),
        offset: off(37 + len),
    }));
}
