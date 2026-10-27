//! The machine's own rules: continuation claims, retries and backoff, the HEAD check, blocked
//! conditions, batching, the closed-seq cross-check and `synced()`.

use ursula_headers::ClusterId;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::Incarnation;
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
use super::refused;
use super::reset_of;
use super::sends;
use super::timer_of;
use super::token;
use crate::codec::frame;
use crate::ids::Activation;
use crate::ids::Position;
use crate::replicator::Action;
use crate::replicator::BlockedReason;
use crate::replicator::BoundaryReceipt;
use crate::replicator::ClaimKind;
use crate::replicator::Condition;
use crate::replicator::Event;
use crate::replicator::MIN_REQUEST_BYTES;
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
    let (mut h, actions) = Harness::activate(
        ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        },
        local(pending, 0),
    );
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
    let (mut h, _) = Harness::activate(
        ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        },
        state,
    );
    h.on(Event::Boundary(Position(2)));
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, 37, None)));
    let body = h.last_request().body.clone();
    assert_eq!(body.len(), pending[0].frame.len() + pending[1].frame.len());
    let len = u64::try_from(body.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    // W is the end offset of the receipt of the batch ending exactly at the boundary.
    let receipts: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            Action::Boundary(r) => Some(*r),
            _ => None,
        })
        .collect();
    let frame_bytes = u64::try_from(body.len()).unwrap();
    assert_eq!(receipts, vec![BoundaryReceipt {
        position: Position(2),
        offset: Some(off(37 + len)),
        epoch: Some(epoch(1)),
        log_bytes: frame_bytes,
    }]);
    // Past the boundary, the limit cuts: records 3 and 4 do not fit with the limit at 3 frames
    // only when their sum exceeds it.
    let body = h.last_request().body.clone();
    assert_eq!(body.len(), pending[2].frame.len() + pending[3].frame.len());
}

// A boundary at the acknowledged position answers at once with the acknowledged offset; one
// below it, which no receipt ends at any more, answers without an offset.
#[test]
fn a_boundary_already_acknowledged_answers_at_once() {
    let mut h = streaming(2);
    let len = u64::try_from(h.last_request().body.len()).unwrap();
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + len,
        None,
    )));
    let actions = h.on(Event::Boundary(Position(2)));
    assert_eq!(actions, vec![Action::Boundary(BoundaryReceipt {
        position: Position(2),
        offset: Some(off(37 + len)),
        epoch: Some(epoch(1)),
        log_bytes: len,
    })]);
    let actions = h.on(Event::Boundary(Position(1)));
    assert!(matches!(actions.as_slice(), [Action::Boundary(
        BoundaryReceipt {
            position: Position(1),
            offset: None,
            ..
        }
    )]));
    // A published snapshot's bytes stop counting toward the next.
    let actions = h.on(Event::Snapshotted { log_bytes: len });
    assert_eq!(actions, vec![Action::PersistAck {
        acked: Position(2),
        acked_offset: off(37 + len),
        log_bytes: 0,
    }]);
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

/// The attempt of the first send among `actions`.
fn attempt_of(actions: &[Action]) -> crate::transport::Attempt {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Send { attempt, .. } => Some(*attempt),
            _ => None,
        })
        .unwrap_or_else(|| panic!("no send: {actions:?}"))
}

// A token change sends a request a `401` blocked again at once, with no wait for the 30 s retry.
// The new token refused too blocks again at once, until the next change. A change that finds
// nothing blocked sends nothing.
#[test]
fn a_token_change_resends_a_request_blocked_unauthorized_at_once() {
    let mut h = streaming(1);
    assert!(sends(&h.on(Event::TokenChanged)).is_empty());
    let first = h.last_request().clone();
    h.answer(&RawAnswer::new(401));
    let actions = h.answer(&RawAnswer::new(401));
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::Unauthorized)))
    );
    assert_eq!(sends(&h.on(Event::TokenChanged)), vec![&first]);
    let actions = h.answer(&RawAnswer::new(401));
    assert!(sends(&actions).is_empty());
    assert_eq!(timer_of(&actions).unwrap().0, h.now_ms + BLOCKED_RETRY_MS);
    assert_eq!(sends(&h.on(Event::TokenChanged)), vec![&first]);
    let len = u64::try_from(first.body.len()).unwrap();
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

// A token that changes while the token reread after a `401` is out: that attempt carried the old
// token, so its `401` sends the request again at once, the token read again, instead of blocking
// it.
#[test]
fn a_token_changed_while_the_refused_attempt_was_out_goes_at_once() {
    let mut h = streaming(1);
    let first = h.last_request().clone();
    h.answer(&RawAnswer::new(401));
    assert!(sends(&h.on(Event::TokenChanged)).is_empty(), "in flight");
    let actions = h.answer(&RawAnswer::new(401));
    assert_eq!(sends(&actions), vec![&first]);
    assert!(attempt_of(&actions).refresh_token);
    assert_eq!(condition(&actions), None);
    // Refused under the new token too: blocked.
    let actions = h.answer(&RawAnswer::new(401));
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::Unauthorized)))
    );
}

// A token change leaves a request that waits on anything but a `401` to its timer: a resend in
// backoff goes out at its jittered time, and the timer is not asked for again.
#[test]
fn a_token_change_leaves_a_resend_in_backoff_to_its_timer() {
    let mut h = streaming(1);
    let first = h.last_request().clone();
    let actions = h.answer(&RawAnswer::new(503));
    assert_eq!(condition(&actions), Some(Some(Condition::Retrying)));
    let (at, jitter) = timer_of(&actions).unwrap();
    assert_eq!(at, h.now_ms, "no floor: due at once, with jitter");
    let actions = h.on(Event::TokenChanged);
    assert!(sends(&actions).is_empty(), "{actions:?}");
    assert_eq!(timer_of(&actions), None);
    assert_eq!(sends(&h.tick_at(at + jitter)), vec![&first]);
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
fn a_typed_413_below_the_smallest_limit_lowers_it_and_blocks() {
    let mut h = streaming(1);
    let mut raw = RawAnswer::new(SizeLimit::STATUS);
    SizeLimit::Request(16).render(raw.emit());
    let actions = h.answer(&raw);
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::RequestTooLarge)))
    );
    let limits = h.replicator.ctx().unwrap().limits;
    assert_eq!(limits.named, Some(16));
    assert_eq!(limits.request(), MIN_REQUEST_BYTES);
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
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

// A bound session's HEAD (here the one a `404` asks for) refuses what an open refuses: another
// release line blocks `protocol_mismatch`, and an answer without identity `no_server_identity`.
// Neither resets: the session keeps its commits and resends every 30 s.
#[test]
fn head_blocks_another_release_line_and_an_answer_without_identity() {
    for found in [2, crate::open::table::next_line()] {
        let mut h = streaming(1);
        h.answer(&RawAnswer::new(404));
        let mut raw = RawAnswer::new(200);
        ServerIdentity {
            protocol: found,
            cluster_id: cluster(),
        }
        .render(raw.emit());
        StreamHead::new(token(), off(0), off(0), 1 << 20)
            .unwrap()
            .render(raw.emit());
        let actions = h.answer(&raw);
        assert_eq!(reset_of(&actions), None);
        assert_eq!(
            condition(&actions),
            Some(Some(Condition::Blocked(BlockedReason::ProtocolMismatch {
                found
            })))
        );
        let (at, _) = timer_of(&actions).unwrap();
        assert_eq!(at, h.now_ms + BLOCKED_RETRY_MS);
    }
    let mut h = streaming(1);
    h.answer(&RawAnswer::new(404));
    let actions = h.answer(&RawAnswer::new(404));
    assert_eq!(reset_of(&actions), None);
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::NoServerIdentity)))
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
    let (mut h, _) = Harness::activate(
        ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        },
        local(Vec::new(), 0),
    );
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
        // A copy's claim or commit under our activation and lineage fences too (design: Clones).
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
    let (mut h, _) = Harness::activate(
        ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        },
        local(pending, 0),
    );
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

/// Streaming with record 1 acknowledged and record 2's append blocked by a 402, and a ticket for
/// record 1 whose probe is out.
fn blocked_with_a_probe() -> (Harness, u64) {
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
    h.on(Event::Logged(commit(LINEAGE, 2, 1)));
    h.on(Event::Sync {
        ticket: 6,
        target: Position(1),
    });
    let actions = h.answer(&RawAnswer::new(402));
    assert_eq!(
        condition(&actions),
        Some(Some(Condition::Blocked(BlockedReason::PaymentRequired)))
    );
    assert_eq!(h.last_request().method, Method::Get, "{actions:?}");
    (h, 37 + len)
}

#[test]
fn a_probe_that_succeeds_beside_a_blocked_append_keeps_its_condition() {
    let (mut h, tail) = blocked_with_a_probe();
    let actions = h.answer(&read(&[], tail));
    assert!(actions.contains(&Action::Synced { ticket: 6 }));
    assert_eq!(condition(&actions), None, "{actions:?}");
    assert_eq!(
        h.replicator.ctx().unwrap().condition,
        Some(Condition::Blocked(BlockedReason::PaymentRequired))
    );
}

/// The probe meets a history that inherits everything acknowledged: its HEAD re-keys the
/// generation, the blocked append and the probe go, and a continuation claims `own + 1` under the
/// new token (design: History).
#[test]
fn a_probe_whose_head_names_an_inheriting_history_re_keys() {
    let (mut h, tail) = blocked_with_a_probe();
    let new = ursula_headers::Incarnation::try_from((1 << 52) | 8).unwrap();
    let mut mismatch = RawAnswer::new(IncarnationMismatch::STATUS);
    IncarnationMismatch { current: Some(new) }.render(mismatch.emit());
    h.answer(&mismatch);
    assert_eq!(h.last_request().method, Method::Head);
    let mut inheriting = RawAnswer::new(200);
    ServerIdentity::current(cluster()).render(inheriting.emit());
    StreamHead::new(new, off(tail), off(0), 1 << 20)
        .unwrap()
        .with_inherited(Some(ursula_headers::Inherited {
            previous_incarnation: token(),
            offset: off(tail),
        }))
        .unwrap()
        .render(inheriting.emit());
    let actions = h.answer(&inheriting);
    // The blocked condition clears, and the CLAIM record is logged before the claim goes.
    assert_eq!(
        actions.first(),
        Some(&Action::Condition(None)),
        "{actions:?}"
    );
    assert_eq!(
        actions.get(1),
        Some(&Action::ReKey {
            incarnation: new,
            epoch: epoch(2),
            activation: ACTIVATION,
        }),
        "{actions:?}"
    );
    let claim = h.last_request().keys.producer.clone().unwrap();
    assert_eq!((claim.epoch, claim.seq), (epoch(2), super::seq(0)));
    assert!(matches!(h.replicator, Replicator::Activating {
        kind: ClaimKind::Continuation { .. },
        ..
    }));
}

#[test]
fn a_probe_that_fails_beside_a_blocked_append_keeps_its_condition() {
    let (mut h, _) = blocked_with_a_probe();
    let actions = h.answer(&RawAnswer::new(503));
    assert_eq!(condition(&actions), None, "{actions:?}");
    assert_eq!(
        h.replicator.ctx().unwrap().condition,
        Some(Condition::Blocked(BlockedReason::PaymentRequired))
    );
}

#[test]
fn a_probe_that_succeeds_while_idle_clears_the_condition() {
    let mut h = streaming(0);
    h.on(Event::Sync {
        ticket: 1,
        target: Position(0),
    });
    let actions = h.answer(&RawAnswer::new(503));
    assert_eq!(condition(&actions), Some(Some(Condition::Retrying)));
    let (at, jitter) = timer_of(&actions).unwrap();
    h.tick_at(at + jitter);
    let actions = h.answer(&read(&[], 37));
    assert!(actions.contains(&Action::Synced { ticket: 1 }));
    assert_eq!(condition(&actions), Some(None));
}

/// A `410` on a fresh probe sends a HEAD first: a read of another history gets its `410` before
/// its `412`. With our token still there, retention passed our tail: `fenced`.
#[test]
fn a_410_on_a_fresh_probe_fences_once_head_shows_our_token() {
    let mut h = streaming(0);
    h.on(Event::Sync {
        ticket: 1,
        target: Position(0),
    });
    let actions = h.answer(&RawAnswer::new(410));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert_eq!(h.last_request().method, Method::Head);
    let actions = h.answer(&super::head_naming(None, 37));
    assert_eq!(reset_of(&actions), Some(ResetKind::Fenced));
}

/// A `410` names the token of the stream that refused it. Naming ours, retention passed our tail
/// under our stream: the probe resets `fenced` with no HEAD.
#[test]
fn a_410_naming_our_token_on_a_probe_fences_with_no_head() {
    let mut h = streaming(0);
    h.on(Event::Sync {
        ticket: 1,
        target: Position(0),
    });
    let actions = h.answer(&refused(410, Some(token())));
    assert_eq!(reset_of(&actions), Some(ResetKind::Fenced), "{actions:?}");
    assert!(sends(&actions).is_empty(), "{actions:?}");
}

/// A `416` naming our token cannot happen (H1): it is resent after backoff with no HEAD, as a HEAD
/// that found our token would have it resent. Another token, or none, sends the HEAD of a `412`.
#[test]
fn a_416_naming_our_token_on_a_probe_is_resent_with_no_head() {
    let mut h = streaming(0);
    h.on(Event::Sync {
        ticket: 1,
        target: Position(0),
    });
    let actions = h.answer(&refused(416, Some(token())));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(sends(&actions).is_empty(), "{actions:?}");
    let (at, jitter) = timer_of(&actions).unwrap();
    let actions = h.tick_at(at + jitter);
    let methods: Vec<Method> = sends(&actions).iter().map(|r| r.method).collect();
    assert_eq!(methods, vec![Method::Get], "{actions:?}");
    let other = Incarnation::try_from((1 << 52) | 8).unwrap();
    for named in [None, Some(other)] {
        let mut h = streaming(0);
        h.on(Event::Sync {
            ticket: 1,
            target: Position(0),
        });
        let actions = h.answer(&refused(416, named));
        let methods: Vec<Method> = sends(&actions).iter().map(|r| r.method).collect();
        assert_eq!(methods, vec![Method::Head], "{named:?}: {actions:?}");
    }
}

#[test]
fn a_commit_logged_while_catching_up_resets_internal() {
    let (mut h, _) = Harness::activate(
        ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        },
        local(Vec::new(), 0),
    );
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 500, 537, None)));
    h.answer(&RawAnswer::new(410));
    let actions = h.answer(&super::head_naming(None, 537));
    assert!(matches!(actions.first(), Some(Action::Rebuild { .. })));
    let actions = h.on(Event::Logged(commit(LINEAGE, 1, 1)));
    assert_eq!(reset_of(&actions), Some(ResetKind::Internal));
}

/// The world's counterexample: a continuation's reconcile acknowledges a record read by read, and
/// a `synced()` call between two reads must not resolve a ticket on that record before the
/// replicator announces it. Outputs wait for the claim and its reconcile (design: Open, Pre-check),
/// so the ticket resolves at the bind, after the `Wake` that covers it.
#[test]
fn a_ticket_waits_for_the_continuation_reconcile_to_finish() {
    let mut h = streaming(2);
    let first = u64::try_from(commit(LINEAGE, 1, 1).frame.len()).unwrap();
    let len = u64::try_from(h.last_request().body.len()).unwrap();
    // Ticket 1 waits for record 1; the batch of records 1 and 2 applies, but a layer answers
    // 503.
    h.on(Event::Sync {
        ticket: 1,
        target: Position(1),
    });
    let actions = h.answer(&RawAnswer::new(503));
    let (at, jitter) = timer_of(&actions).unwrap();
    // Ticket 2 waits only for a round trip: its probe, which started after ticket 1's call,
    // proves the tail.
    h.on(Event::Sync {
        ticket: 2,
        target: Position(0),
    });
    assert_eq!(h.last_request().method, Method::Get);
    let actions = h.answer(&read(&[], 37));
    assert!(
        actions.contains(&Action::Synced { ticket: 2 }),
        "{actions:?}"
    );
    // The resend meets idle expiry, and the continuation claim lands after the batch.
    h.tick_at(at + jitter);
    h.answer(&producer(&ProducerAnswer::ProducerExpired {
        epoch: epoch(1),
    }));
    let claim_end = 37 + len + 37;
    h.answer(&producer(&receipt(
        Delivery::Applied,
        2,
        0,
        37 + len,
        claim_end,
        None,
    )));
    // The first read holds record 1 only.
    let actions = h.answer(&read(&commit(LINEAGE, 1, 1).frame, 37 + first));
    assert!(
        !actions.contains(&Action::Synced { ticket: 1 }),
        "{actions:?}"
    );
    let actions = h.on(Event::Sync {
        ticket: 3,
        target: Position(2),
    });
    assert!(
        !actions.contains(&Action::Synced { ticket: 1 }),
        "{actions:?}"
    );
    let actions = h.answer(&read(&commit(LINEAGE, 2, 1).frame, 37 + len));
    let wake = actions.iter().position(|a| {
        *a == Action::Wake {
            acked: Position(2),
            offset: off(claim_end),
        }
    });
    let synced = actions
        .iter()
        .position(|a| *a == Action::Synced { ticket: 1 });
    assert!(
        matches!((wake, synced), (Some(w), Some(s)) if w < s),
        "{actions:?}"
    );
}

/// Two copies on one activation take over at one epoch (design: Clones): the second copy's claim is
/// byte-identical to the first's, so it wins by a `204` repeat and binds. Its first append, which
/// the other copy's writes under the same key refuse (`key-conflict` for another body,
/// `receipt-evicted` once later writes evicted the key's receipt), resets `fenced` and keeps the
/// pending commits for the next open's reconcile.
#[test]
fn a_takeover_won_by_a_repeat_binds_and_its_first_refused_append_fences() {
    let refusals = [
        ProducerAnswer::KeyConflict { epoch: epoch(1) },
        ProducerAnswer::ReceiptEvicted {
            epoch: CurrentEpoch::new(epoch(1)),
            seq: super::seq(3),
            incarnation: token(),
        },
    ];
    for refusal in refusals {
        let pending = vec![commit(LINEAGE, 1, 1), commit(LINEAGE, 2, 1)];
        let (mut h, _) = Harness::activate(
            ClaimKind::Takeover {
                round: 1,
                rekeyed: false,
            },
            local(pending, 0),
        );
        let actions = h.answer(&producer(&receipt(Delivery::Repeat, 1, 0, 0, 37, None)));
        assert!(
            actions.iter().any(|a| matches!(a, Action::Bound { .. })),
            "{actions:?}"
        );
        assert!(matches!(h.replicator, Replicator::Streaming { .. }));
        let keys = h.last_request().keys.producer.clone().unwrap();
        assert_eq!((keys.epoch, keys.seq), (epoch(1), super::seq(1)));
        let actions = h.answer(&producer(&refusal));
        assert_eq!(
            reset_of(&actions),
            Some(ResetKind::Fenced),
            "{refusal:?}: {actions:?}"
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::Rebuild { .. } | Action::Wake { .. })),
            "{refusal:?}: {actions:?}"
        );
    }
}
