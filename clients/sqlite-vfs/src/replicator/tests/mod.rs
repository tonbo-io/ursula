#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute offsets, positions and counters from small generated sizes"
)]
//! Tests of the replicator: one per row of both answer tables of design §4.7
//! ([`answer_rows`]), one per row of the reconcile table of §4.9 ([`reconcile_rows`]), the
//! machine's own rules ([`machine`]), and the proptest world against the model server
//! ([`world`]).

mod answer_rows;
mod machine;
mod reconcile_rows;
mod world;

use std::sync::Arc;

use ursula_headers::ClosedEpoch;
use ursula_headers::ClusterId;
use ursula_headers::CurrentEpoch;
use ursula_headers::Delivery;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::Offset;
use ursula_headers::ProducerAnswer;
use ursula_headers::Receipt;
use ursula_headers::Seq;
use ursula_headers::names;

use super::Action;
use super::ClaimKind;
use super::Event;
use super::LocalState;
use super::Pending;
use super::Replicator;
use super::RequestId;
use crate::codec::frame;
use crate::codec::frame::Frame;
use crate::ids::Activation;
use crate::ids::CommitId;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::wire::Exchange;
use crate::wire::NoAnswer;
use crate::wire::RawAnswer;
use crate::wire::Request;

pub(crate) const ACTIVATION: Activation = Activation([0xa1; 16]);
pub(crate) const LINEAGE: Lineage = Lineage([0x11; 16]);

pub(crate) fn epoch(n: u64) -> Epoch {
    Epoch::try_from(n).unwrap()
}

pub(crate) fn seq(n: u64) -> Seq {
    Seq::try_from(n).unwrap()
}

pub(crate) fn off(n: u64) -> Offset {
    Offset::from_u64(n)
}

pub(crate) fn token() -> Incarnation {
    Incarnation::try_from((1 << 52) | 7).unwrap()
}

pub(crate) fn cluster() -> ClusterId {
    ClusterId::new("cluster-a").unwrap()
}

/// The COMMIT frame of `(lineage, position)` with `salt` as its (opaque) compressed record.
pub(crate) fn commit(lineage: Lineage, position: u64, salt: u8) -> Pending {
    let id = CommitId {
        lineage,
        position: Position(position),
    };
    let len = 4 + usize::try_from(position % 5).unwrap();
    let bytes = frame::encode(&Frame::Commit {
        id,
        compressed: vec![salt; len],
    })
    .unwrap();
    Pending {
        id,
        frame: Arc::from(bytes),
    }
}

pub(crate) fn local(pending: Vec<Pending>, acked_offset: u64) -> LocalState {
    let logged = pending.last().map_or(Position(0), |p| p.id.position);
    let acked = pending
        .first()
        .map_or(logged, |p| Position(p.id.position.0 - 1));
    LocalState {
        activation: ACTIVATION,
        cluster_id: cluster(),
        incarnation: token(),
        acked,
        acked_offset: off(acked_offset),
        log_bytes: 0,
        logged,
        pending,
        claims: Vec::new(),
        highest_epoch: None,
        max_request_bytes: 32 << 20,
        configured_max_request_bytes: None,
        db_bytes: u64::MAX,
    }
}

/// A raw producer answer.
pub(crate) fn producer(answer: &ProducerAnswer) -> RawAnswer {
    let mut raw = RawAnswer::new(answer.status());
    answer.render(raw.emit());
    raw
}

/// A receipt for a write of `(epoch, seq)` at `start .. next`.
pub(crate) fn receipt(
    delivery: Delivery,
    e: u64,
    s: u64,
    start: u64,
    next: u64,
    closed: Option<ClosedEpoch>,
) -> ProducerAnswer {
    ProducerAnswer::Receipt {
        delivery,
        epoch: match closed {
            None => CurrentEpoch::new(epoch(e)),
            Some(closed) => CurrentEpoch::closing(epoch(e), closed).unwrap(),
        },
        seq: seq(s),
        receipt: Receipt::new(off(start), off(next), false).unwrap(),
        incarnation: token(),
    }
}

/// A read's answer: `body` from the requested offset up to `next`.
pub(crate) fn read(body: &[u8], next: u64) -> RawAnswer {
    let mut raw = RawAnswer::new(200);
    raw.header(names::STREAM_NEXT_OFFSET, off(next).to_string());
    raw.body = body.to_vec();
    raw
}

/// Drives one replicator by hand and remembers its last request.
pub(crate) struct Harness {
    pub(crate) replicator: Replicator,
    pub(crate) now_ms: u64,
    pub(crate) last: Option<(RequestId, Request)>,
}

impl Harness {
    pub(crate) fn activate(kind: ClaimKind, local: LocalState) -> (Self, Vec<Action>) {
        let (replicator, actions) = Replicator::activate(kind, local);
        let mut harness = Self {
            replicator,
            now_ms: 1_000,
            last: None,
        };
        harness.note(&actions);
        (harness, actions)
    }

    fn note(&mut self, actions: &[Action]) {
        for action in actions {
            if let Action::Send { id, request, .. } = action {
                self.last = Some((*id, request.clone()));
            }
        }
    }

    pub(crate) fn on(&mut self, event: Event) -> Vec<Action> {
        let actions = self.replicator.on(event, self.now_ms);
        self.note(&actions);
        actions
    }

    /// Answers the last request with `raw`.
    pub(crate) fn answer(&mut self, raw: &RawAnswer) -> Vec<Action> {
        let (id, request) = self.last.clone().expect("a request went out");
        let exchange = Exchange::Answered(Box::new(raw.parse(&request)));
        self.on(Event::Answer { id, exchange })
    }

    /// The last request got no answer.
    pub(crate) fn lose(&mut self) -> Vec<Action> {
        let (id, _) = self.last.clone().expect("a request went out");
        self.on(Event::Answer {
            id,
            exchange: Exchange::NoAnswer(NoAnswer::Lost {
                reason: "test".to_owned(),
            }),
        })
    }

    /// Moves the clock to `at_ms` and ticks.
    pub(crate) fn tick_at(&mut self, at_ms: u64) -> Vec<Action> {
        self.now_ms = self.now_ms.max(at_ms);
        self.on(Event::Tick)
    }

    pub(crate) fn last_request(&self) -> &Request {
        &self.last.as_ref().expect("a request went out").1
    }
}

/// The sends among `actions`.
pub(crate) fn sends(actions: &[Action]) -> Vec<&Request> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send { request, .. } => Some(request),
            _ => None,
        })
        .collect()
}

/// The reset among `actions`, if any.
pub(crate) fn reset_of(actions: &[Action]) -> Option<super::ResetKind> {
    actions.iter().find_map(|a| match a {
        Action::Reset(kind) => Some(*kind),
        _ => None,
    })
}

/// The timer among `actions`, if any.
pub(crate) fn timer_of(actions: &[Action]) -> Option<(u64, u64)> {
    actions.iter().find_map(|a| match a {
        Action::Timer { at_ms, jitter_ms } => Some((*at_ms, *jitter_ms)),
        _ => None,
    })
}
