//! PART runs: a commit above the batch limit goes out one part per append, acknowledged with its
//! last part; a probe inside a run reads from the run's tail; a limit that falls below the body
//! in flight claims the next epoch and seals the commit again, smaller.

use std::sync::Arc;

use ursula_headers::ClosedEpoch;
use ursula_headers::Delivery;
use ursula_headers::ServerIdentity;
use ursula_headers::SizeLimit;
use ursula_headers::StreamHead;

use super::Harness;
use super::LINEAGE;
use super::cluster;
use super::epoch;
use super::local;
use super::off;
use super::producer;
use super::read;
use super::receipt;
use super::reset_of;
use super::sends;
use super::seq;
use super::timer_of;
use super::token;
use crate::codec::frame;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::PartRun;
use crate::ids::CommitId;
use crate::ids::Position;
use crate::replicator::Action;
use crate::replicator::ClaimKind;
use crate::replicator::Ctx;
use crate::replicator::Event;
use crate::replicator::Pending;
use crate::replicator::Replicator;
use crate::replicator::batch;
use crate::replicator::batch::PartOf;
use crate::replicator::batch::Run;
use crate::replicator::batch::SealError;
use crate::wire::Method;
use crate::wire::RawAnswer;
use crate::wire::Request;
use crate::wire::Target;

/// The COMMIT frame of position `position` whose (opaque) compressed record is `len` bytes.
fn big(position: u64, len: usize) -> Pending {
    let id = CommitId {
        lineage: LINEAGE,
        position: Position(position),
    };
    let compressed = (0..len)
        .map(|i| u8::try_from((i * 31 + 7) % 251).unwrap())
        .collect();
    let bytes = frame::encode(&Frame::Commit { id, compressed }).unwrap();
    Pending {
        id,
        frame: Arc::from(bytes),
    }
}

/// Streaming at epoch 1 with the claim at 0..37 and `pending` waiting, under the configured
/// limit `limit`: the first batch is in flight.
fn streaming(pending: Vec<Pending>, limit: Option<u64>) -> Harness {
    let mut state = local(pending, 0);
    state.configured_max_request_bytes = limit;
    let (mut h, _) = Harness::activate(
        ClaimKind::Takeover {
            round: 1,
            rekeyed: false,
        },
        state,
    );
    h.answer(&producer(&receipt(Delivery::Applied, 1, 0, 0, 37, None)));
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
    h
}

/// The PART frame `request` sends: its index, its count, and its length.
fn part_of(request: &Request) -> (u32, u32, u64) {
    let Ok(Decoded::Frame {
        frame: Frame::Part { index, count, .. },
        len,
    }) = frame::decode(&request.body, u64::MAX)
    else {
        panic!("not a PART frame");
    };
    assert_eq!(len, request.body.len(), "one PART frame per append");
    (index, count, u64::try_from(len).unwrap())
}

fn keys(request: &Request) -> (u64, u64) {
    let producer = request.keys.producer.as_ref().unwrap();
    (producer.epoch.get(), producer.seq.get())
}

fn acked(actions: &[Action]) -> bool {
    actions
        .iter()
        .any(|a| matches!(a, Action::Wake { .. } | Action::PersistAck { .. }))
}

/// Answers every part of the run in flight, from part `index` of epoch `e` at seq `s`, landing at
/// `start`: returns the run joined (when it started at part 0) and the stream offset after it.
/// Only the last part's receipt acknowledges.
fn finish_run(
    h: &mut Harness,
    e: u64,
    mut s: u64,
    mut start: u64,
    limit: u64,
) -> (Option<Frame>, u64) {
    let mut run = PartRun::default();
    loop {
        let request = h.last_request().clone();
        assert_eq!(keys(&request), (e, s));
        let (index, count, len) = part_of(&request);
        assert!(
            len <= limit,
            "a part of {len} bytes above the limit of {limit}"
        );
        let Ok(Decoded::Frame { frame: part, .. }) = frame::decode(&request.body, u64::MAX) else {
            unreachable!()
        };
        let joined = run.push(part).unwrap();
        let actions = h.answer(&producer(&receipt(
            Delivery::Applied,
            e,
            s,
            start,
            start + len,
            None,
        )));
        start += len;
        s += 1;
        if index + 1 < count {
            assert!(
                !acked(&actions),
                "part {index} of {count} acknowledged: {actions:?}"
            );
            assert!(joined.is_none());
            continue;
        }
        assert!(
            actions.contains(&Action::Wake {
                acked: Position(1),
                offset: off(start),
            }),
            "{actions:?}"
        );
        return (joined, start);
    }
}

#[test]
fn seal_cuts_a_frame_above_the_batch_limit_into_a_run() {
    let pending = big(1, 10_000);
    let mut state = local(vec![pending.clone()], 37);
    state.configured_max_request_bytes = Some(4096);
    let mut ctx = Ctx::new(state).unwrap();
    let first = batch::seal(epoch(1), seq(1), &ctx).unwrap();
    let chunk = 4096 - frame::PART_OVERHEAD;
    assert_eq!(
        first.part,
        Some(PartOf {
            index: 0,
            count: 3,
            chunk
        })
    );
    assert_eq!(first.body.len(), 4096);
    assert_eq!((first.first, first.last), (Position(1), Position(1)));
    assert_eq!(first.expected_start, off(37));
    // The run's next part lands after the run's tail, and the last one is shorter.
    ctx.run = Some(Run {
        position: Position(1),
        count: 3,
        chunk,
        next: 2,
        tail: off(500),
    });
    let last = batch::seal(epoch(1), seq(3), &ctx).unwrap();
    assert_eq!(last.part.map(|p| p.index), Some(2));
    assert_eq!(last.expected_start, off(500));
    assert_eq!(last.body.len(), frame::PART_OVERHEAD + 10_000 - 2 * chunk);
    // A run of another record is an internal error.
    ctx.run = Some(Run {
        position: Position(9),
        count: 3,
        chunk,
        next: 1,
        tail: off(500),
    });
    assert!(matches!(
        batch::seal(epoch(1), seq(2), &ctx),
        Err(SealError::RunNotFirst { .. })
    ));
}

#[test]
fn a_frame_above_the_batch_limit_goes_out_as_parts_acknowledged_with_the_last() {
    let pending = big(1, 10_000);
    let mut h = streaming(vec![pending.clone()], Some(4096));
    let (joined, end) = finish_run(&mut h, 1, 1, 37, 4096);
    assert_eq!(
        frame::encode(&joined.unwrap()).unwrap(),
        pending.frame.to_vec()
    );
    let ctx = h.replicator.ctx().unwrap();
    assert!(ctx.run.is_none());
    assert_eq!(ctx.acked_offset, off(end));
}

#[test]
fn a_413_inside_a_run_claims_the_next_epoch_and_seals_again_smaller() {
    let pending = big(1, 20_000);
    let mut h = streaming(vec![pending.clone()], Some(8192));
    let part0 = h.last_request().body.clone();
    let l0 = u64::try_from(part0.len()).unwrap();
    let actions = h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + l0,
        None,
    )));
    assert!(!acked(&actions));
    assert_eq!(part_of(h.last_request()).0, 1);
    // Part 1 meets a limit below it.
    let mut raw = RawAnswer::new(SizeLimit::STATUS);
    SizeLimit::Request(4096).render(raw.emit());
    let actions = h.answer(&raw);
    assert!(
        actions.contains(&Action::PersistClaim {
            epoch: epoch(2),
            activation: super::ACTIVATION,
        }),
        "{actions:?}"
    );
    assert_eq!(keys(h.last_request()), (2, 0));
    assert!(h.replicator.ctx().unwrap().run.is_none());
    assert_eq!(h.replicator.ctx().unwrap().acked_offset, off(37));
    // The claim closes epoch 1 at part 0's seq, after part 0.
    h.answer(&producer(&receipt(
        Delivery::Applied,
        2,
        0,
        37 + l0,
        74 + l0,
        Some(ClosedEpoch {
            epoch: epoch(1),
            seq: seq(1),
        }),
    )));
    // The reconcile reads from the acknowledged offset: the voided part 0 acknowledges nothing.
    let actions = h.answer(&read(&part0, 37 + l0));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(actions.contains(&Action::Bound { epoch: epoch(2) }));
    // The commit goes out again from part 0, cut for the lower limit, after the claim.
    let (index, count, _) = part_of(h.last_request());
    assert_eq!((index, count), (0, 5));
    let (joined, _) = finish_run(&mut h, 2, 1, 74 + l0, 4096);
    assert_eq!(
        frame::encode(&joined.unwrap()).unwrap(),
        pending.frame.to_vec()
    );
}

#[test]
fn a_probe_inside_a_run_reads_from_the_run_tail_and_never_fences() {
    let mut h = streaming(vec![big(1, 10_000)], Some(4096));
    let l0 = part_of(h.last_request()).2;
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + l0,
        None,
    )));
    let (append_id, part1) = h.last.clone().unwrap();
    h.on(Event::Sync {
        ticket: 3,
        target: Position(0),
    });
    // Part 1's answer is lost: the probe goes out while its resend waits, from the run's tail.
    let lost = h.lose();
    let probe = h.last_request().clone();
    assert_eq!(probe.method, Method::Get, "{lost:?}");
    assert!(matches!(
        probe.target,
        Target::Read { from: Some(from), .. } if from == off(37 + l0)
    ));
    let actions = h.answer(&read(&[], 37 + l0));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(actions.contains(&Action::Synced { ticket: 3 }));
    // Part 1 goes out again with the same key and body.
    let (at, jitter) = timer_of(&lost).unwrap();
    let actions = h.tick_at(at + jitter);
    let resent = sends(&actions);
    assert_eq!(resent.len(), 1, "{actions:?}");
    assert_eq!(resent[0].body, part1.body);
    assert_eq!(keys(resent[0]), keys(&part1));
    assert_ne!(h.last.as_ref().unwrap().0, append_id);
    finish_run(&mut h, 1, 2, 37 + l0, 4096);
}

#[test]
fn a_probe_that_finds_our_own_part_inside_a_run_waits_for_its_answer() {
    let mut h = streaming(vec![big(1, 10_000)], Some(4096));
    let l0 = part_of(h.last_request()).2;
    h.answer(&producer(&receipt(
        Delivery::Applied,
        1,
        1,
        37,
        37 + l0,
        None,
    )));
    let part1 = h.last_request().body.clone();
    let l1 = u64::try_from(part1.len()).unwrap();
    h.on(Event::Sync {
        ticket: 3,
        target: Position(0),
    });
    h.lose();
    let actions = h.answer(&read(&part1, 37 + l0 + l1));
    assert_eq!(reset_of(&actions), None, "{actions:?}");
    assert!(!actions.contains(&Action::Synced { ticket: 3 }));
    assert!(matches!(h.replicator, Replicator::Streaming { .. }));
}

#[test]
fn three_lost_answers_for_a_large_body_send_a_head_whose_limit_recuts() {
    let mut h = streaming(vec![big(1, 100_000)], None);
    // One whole frame under the default limit.
    assert!(matches!(
        frame::decode(&h.last_request().body, u64::MAX),
        Ok(Decoded::Frame {
            frame: Frame::Commit { .. },
            ..
        })
    ));
    for _ in 0..2 {
        let actions = h.lose();
        let (at, jitter) = timer_of(&actions).unwrap();
        let actions = h.tick_at(at + jitter);
        assert_eq!(sends(&actions)[0].method, Method::Post);
    }
    let actions = h.lose();
    let head_sent = sends(&actions);
    assert_eq!(head_sent.len(), 1, "{actions:?}");
    assert_eq!(head_sent[0].method, Method::Head);
    // A layer in front advertises 64 KiB: the body is sealed again under epoch 2.
    let mut raw = RawAnswer::new(200);
    ServerIdentity::current(cluster()).render(raw.emit());
    StreamHead::new(token(), off(0), off(0), 64 << 10)
        .unwrap()
        .render(raw.emit());
    let actions = h.answer(&raw);
    assert!(
        actions.contains(&Action::PersistClaim {
            epoch: epoch(2),
            activation: super::ACTIVATION,
        }),
        "{actions:?}"
    );
    assert!(matches!(h.replicator, Replicator::Activating {
        kind: ClaimKind::Continuation { .. },
        ..
    }));
}

#[test]
fn a_lost_answer_for_a_part_whose_limit_still_fits_is_sent_again() {
    // Three lost answers, a HEAD that advertises the limit the run was cut for: resent as is.
    let mut h = streaming(vec![big(1, 300_000)], Some(128 << 10));
    let part0 = h.last_request().clone();
    for _ in 0..2 {
        let actions = h.lose();
        let (at, jitter) = timer_of(&actions).unwrap();
        h.tick_at(at + jitter);
    }
    h.lose();
    assert_eq!(h.last_request().method, Method::Head);
    let mut raw = RawAnswer::new(200);
    ServerIdentity::current(cluster()).render(raw.emit());
    StreamHead::new(token(), off(0), off(0), 32 << 20)
        .unwrap()
        .render(raw.emit());
    let actions = h.answer(&raw);
    let (at, jitter) = timer_of(&actions).unwrap();
    let actions = h.tick_at(at + jitter);
    let resent = sends(&actions);
    assert_eq!(resent.len(), 1, "{actions:?}");
    assert_eq!(resent[0].body, part0.body);
    assert_eq!(keys(resent[0]), keys(&part0));
}
