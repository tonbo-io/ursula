#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute offsets, positions and counters from small generated sizes"
)]
//! Tests of the replicator: one per row of both answer tables (design: Answers)
//! ([`answer_rows`]), one per row of the reconcile table (design: Reconcile) ([`reconcile_rows`]),
//! the machine's own rules ([`machine`]), re-keys across a history the stream inherits ([`rekey`]),
//! and the proptest world against the model server ([`world`]).

mod answer_rows;
mod machine;
mod parts;
mod reconcile_rows;
mod rekey;
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
use ursula_headers::ServerIdentity;
use ursula_headers::SnapshotRead;
use ursula_headers::SnapshotRef;
use ursula_headers::StreamHead;
use ursula_headers::names;

use super::Action;
use super::ClaimKind;
use super::Event;
use super::LocalState;
use super::Pending;
use super::Replicator;
use super::RequestId;
use crate::codec::digest::SnapshotDigester;
use crate::codec::frame;
use crate::codec::frame::Frame;
use crate::codec::snapshot::SnapshotFields;
use crate::codec::snapshot::write_body;
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
        lineage: LINEAGE,
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
        rekey_from: None,
        unacked: if logged > acked {
            crate::replicator::answer::Unacked::Commits
        } else {
            crate::replicator::answer::Unacked::Nothing
        },
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

/// A HEAD's `200` of our stream with its tail at `tail`, naming `snapshot` as the latest.
pub(crate) fn head_naming(snapshot: Option<SnapshotRef>, tail: u64) -> RawAnswer {
    let mut raw = RawAnswer::new(200);
    ServerIdentity::current(cluster()).render(raw.emit());
    StreamHead::new(token(), off(tail), off(0), 32 << 20)
        .unwrap()
        .with_snapshot(snapshot)
        .unwrap()
        .render(raw.emit());
    raw
}

/// A `USS3` body at `offset` whose last commit is `last`, over a one-page image, and its
/// reference as HEAD names it.
pub(crate) fn snapshot_body(offset: u64, last: CommitId) -> (Vec<u8>, SnapshotRef) {
    let fields = SnapshotFields {
        offset: off(offset),
        epoch: epoch(1),
        last,
    };
    let mut body = Vec::new();
    write_body(
        &fields,
        &mut std::io::Cursor::new(vec![7u8; 4096]),
        &mut body,
    )
    .unwrap();
    let mut digester = SnapshotDigester::new(frame::CONTENT_TYPE);
    digester.update(&body);
    let snapshot = SnapshotRef {
        offset: off(offset),
        digest: digester.finalize(),
    };
    (body, snapshot)
}

/// A snapshot read's `200` with `body`.
pub(crate) fn snapshot_read(snapshot: SnapshotRef, body: Vec<u8>) -> RawAnswer {
    let mut raw = RawAnswer::new(200);
    SnapshotRead {
        snapshot,
        incarnation: token(),
        up_to_date: false,
    }
    .render(raw.emit());
    raw.header("content-type", frame::CONTENT_TYPE.to_owned());
    raw.body = body;
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

    /// A provisional open's replicator (design: Open, Pre-check) whose log's latest claim is
    /// `claim`.
    pub(crate) fn provisional(
        local: LocalState,
        claim: (Epoch, Activation),
    ) -> (Self, Vec<Action>) {
        let (replicator, actions) = Replicator::provisional(local, claim);
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
