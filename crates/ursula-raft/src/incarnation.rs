//! The process fence on Raft RPCs (format epoch 6): every request names the
//! process that sends it and the process it is for, and every answer the
//! process that gives it.
//!
//! A process is a [`ProcessIncarnation`]: its WAL root, the run of that
//! root, and a nonce. A request carries the sender's in
//! [`INCARNATION_METADATA`] and the one the sender holds for the target in
//! [`TARGET_INCARNATION_METADATA`]. An answer or refusal carries the
//! responder's node id and incarnation ([`Stamp`]). An `AppendStream`
//! carries both at its open, and its response headers the stamp.
//!
//! **One table.** [`IncarnationBook`] holds, per peer, the newest
//! incarnation seen on the root this node's placement view binds for it, or
//! on the first root seen while the view has no slot for it. Within a root
//! it rises with `run`, from what a process says of itself: a request's
//! sender, an answer's stamp. The root changes only when the view binds
//! another, and a released root never matches again. [`judge`] is the one
//! rule, which every transport reads.
//!
//! **Receiver** ([`receive`]): after the protocol version, the target node
//! and the group lookup, a request is refused (`FAILED_PRECONDITION` with
//! [`FENCE_METADATA`]) when this process is fenced, names no sender, names
//! another process of this node as its target, or comes from a sender the
//! table judges stale. A request whose target is a newer run of this root
//! supersedes this process for good. Forwards (`GroupWrite`, `GroupRead`)
//! are never refused for their sender.
//!
//! **Sender** ([`crate::PeerCluster::accept_responder`]): an answer is used
//! only when its stamp names the target and the table accepts it. A
//! discarded answer is never decoded, so a stale process deposes nobody.
//! A fence refusal is acted on only when its stamp is accepted
//! ([`effect`]).
//!
//! A process without an incarnation (a test, or a simulation before its
//! harness gives its nodes one) takes no part in this fence: it stamps
//! nothing and checks nothing.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use tonic::metadata::AsciiMetadataValue;
use tonic::metadata::MetadataMap;
use ursula_proto::placement::ProcessIncarnation;
use ursula_proto::placement::Relation;
use ursula_proto::placement::RootSlot;

use crate::peer_cluster::NODE_ID_METADATA;
use crate::target::FenceRefusal;
use crate::target::INCARNATION_METADATA;

/// The most peers a book remembers. Keys come from request metadata of
/// senders of this cluster, so the book is bounded: past the cap a new
/// peer is judged but not recorded.
pub const INCARNATION_BOOK_CAP: usize = 4096;

/// How the table judges an incarnation a peer's process says it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Judgement {
    /// The entry, a newer run of its root, or a first entry.
    Accept,
    /// An older run of the root the entry is on.
    Older,
    /// Another process of the entry's run.
    Duplicate,
    /// A root other than the one the view binds, a released root, or, while
    /// the view has no slot, another root than the first seen.
    Unbound,
}

/// The one rule of the table: how `seen` stands to `entry`, the newest
/// incarnation recorded for its node, under `slot`, the root the view
/// binds for it.
pub fn judge(
    entry: Option<ProcessIncarnation>,
    slot: Option<RootSlot>,
    seen: ProcessIncarnation,
) -> Judgement {
    let root = match (slot, entry) {
        (Some(RootSlot::Bound(root)), _) => root,
        (Some(RootSlot::Released(_)), _) => return Judgement::Unbound,
        (None, Some(entry)) => entry.root,
        (None, None) => return Judgement::Accept,
    };
    if seen.root != root {
        return Judgement::Unbound;
    }
    match entry.filter(|entry| entry.root == root) {
        None => Judgement::Accept,
        Some(entry) => match entry.relation(&seen) {
            Relation::Same | Relation::Newer => Judgement::Accept,
            Relation::Older => Judgement::Older,
            Relation::Duplicate => Judgement::Duplicate,
            // The filter keeps only an entry on `seen`'s root.
            Relation::OtherRoot => Judgement::Unbound,
        },
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct Peer {
    entry: Option<ProcessIncarnation>,
    slot: Option<RootSlot>,
}

/// This node's table of its peers' processes, in memory only (see the
/// module docs).
#[derive(Debug, Default)]
pub(crate) struct IncarnationBook {
    peers: Mutex<BTreeMap<u64, Peer>>,
    counts: Counters,
}

impl IncarnationBook {
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, Peer>> {
        self.peers.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Judges `seen` for `node`, and raises the entry to it when accepted.
    pub(crate) fn observe(&self, node: u64, seen: ProcessIncarnation) -> Judgement {
        let mut peers = self.lock();
        let peer = peers.get(&node).copied().unwrap_or_default();
        let judgement = judge(peer.entry, peer.slot, seen);
        if judgement == Judgement::Accept
            && (peers.contains_key(&node) || peers.len() < INCARNATION_BOOK_CAP)
        {
            peers.insert(node, Peer {
                entry: Some(seen),
                ..peer
            });
        }
        judgement
    }

    /// The incarnation this node holds for `node`: its entry, when the
    /// view's slot does not rule it out.
    pub(crate) fn entry(&self, node: u64) -> Option<ProcessIncarnation> {
        let peer = self.lock().get(&node).copied()?;
        let entry = peer.entry?;
        (judge(Some(entry), peer.slot, entry) == Judgement::Accept).then_some(entry)
    }

    /// Takes the roots a placement view binds, by node id, as the slots of
    /// the table. A node the view does not name keeps no slot.
    pub(crate) fn set_slots(&self, slots: &BTreeMap<u64, RootSlot>) {
        let mut peers = self.lock();
        for peer in peers.values_mut() {
            peer.slot = None;
        }
        for (node, slot) in slots {
            if peers.contains_key(node) || peers.len() < INCARNATION_BOOK_CAP {
                peers.entry(*node).or_default().slot = Some(*slot);
            }
        }
    }

    pub(crate) fn counts(&self) -> &Counters {
        &self.counts
    }
}

/// The responder's identity on an answer or refusal: its node id and its
/// incarnation, `ursula-node-id` and `ursula-incarnation` in gRPC metadata.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stamp {
    pub node: Option<u64>,
    pub incarnation: Option<ProcessIncarnation>,
}

impl Stamp {
    /// The stamp `metadata` carries. A malformed value is no value.
    pub(crate) fn of(metadata: &MetadataMap) -> Self {
        let text = |key| metadata.get(key).and_then(|value| value.to_str().ok());
        Self {
            node: text(NODE_ID_METADATA).and_then(|text| text.parse().ok()),
            incarnation: text(INCARNATION_METADATA).and_then(|text| text.parse().ok()),
        }
    }

    /// Writes the stamp into `metadata`.
    pub(crate) fn write(self, metadata: &mut MetadataMap) {
        if let Some(node) = self.node {
            metadata.insert(NODE_ID_METADATA, AsciiMetadataValue::from(node));
        }
        if let Some(incarnation) = self.incarnation {
            metadata.insert(INCARNATION_METADATA, incarnation_value(incarnation));
        }
    }
}

/// An incarnation as a metadata value: 32 lowercase hex digits.
pub(crate) fn incarnation_value(incarnation: ProcessIncarnation) -> AsciiMetadataValue {
    AsciiMetadataValue::try_from(incarnation.to_string())
        .expect("an incarnation is 32 hex digits, a valid metadata value")
}

/// The incarnation `metadata` names under `key`. `Err` for a malformed
/// value.
pub(crate) fn incarnation_of(
    metadata: &MetadataMap,
    key: &'static str,
) -> Result<Option<ProcessIncarnation>, &'static str> {
    metadata
        .get(key)
        .map(|value| {
            value
                .to_str()
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or(key)
        })
        .transpose()
}

/// What a request is to the receiver's sender check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    /// A Raft RPC, a recovery barrier or a genesis probe.
    Raft,
    /// `GroupWrite` or `GroupRead`: never refused for its sender, since a
    /// forward makes no Raft promise and a fresh root's registration must
    /// reach meta before any view binds it.
    Forward,
}

/// The receiver's verdict on a request ([`receive`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Received {
    Pass,
    Refused(FenceRefusal),
    /// The request names a newer run of this root as its target: this
    /// process is superseded, and the request is refused `fenced`.
    Superseded,
}

/// The receiver table: how a request whose sender says it is `sender` and
/// names `target` as the process it is for stands at the process `me`.
/// `book` judges the sender of a Raft RPC and is raised by an accepted one.
pub(crate) fn receive(
    me: ProcessIncarnation,
    sender: Option<(u64, ProcessIncarnation)>,
    target: Option<ProcessIncarnation>,
    rpc: Rpc,
    book: &IncarnationBook,
) -> Received {
    let Some((node, sender)) = sender else {
        return Received::Refused(FenceRefusal::Unidentified);
    };
    match target.map(|target| me.relation(&target)) {
        Some(Relation::Newer) => return Received::Superseded,
        Some(Relation::Older | Relation::OtherRoot) => {
            return Received::Refused(FenceRefusal::StaleTarget);
        }
        Some(Relation::Duplicate) => return Received::Refused(FenceRefusal::Duplicate),
        Some(Relation::Same) | None => {}
    }
    if rpc == Rpc::Forward {
        return Received::Pass;
    }
    match book.observe(node, sender) {
        Judgement::Accept => Received::Pass,
        Judgement::Older => Received::Refused(FenceRefusal::StaleSender),
        Judgement::Unbound => Received::Refused(FenceRefusal::UnboundRoot),
        Judgement::Duplicate => Received::Refused(FenceRefusal::Duplicate),
    }
}

/// Why a sender discarded an answer before decoding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Discarded {
    /// This process is fenced: an answer reaching it is lost.
    #[error("this node is fenced: the answer is lost")]
    Fenced,
    /// The stamp names another node than the target.
    #[error("the answer is stamped by node {node}, not the target")]
    OtherNode { node: u64 },
    /// The answer names no responder incarnation: no evidence.
    #[error("the answer names no responder process")]
    Unstamped,
    /// The table judges the responder stale.
    #[error("the answer comes from a process this node judges {judgement:?}")]
    Stale { judgement: Judgement },
}

impl Discarded {
    /// Whether the channel and append session to the target are of no more
    /// use: they reach a process the table refuses.
    pub(crate) fn retires(self) -> bool {
        matches!(self, Self::Stale { .. })
    }
}

/// What a fence refusal, from a responder the table accepts, asks of the
/// sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Effect {
    /// The responder is fenced: retire the channel and session and dial
    /// again.
    Retire,
    /// The request named an older process of the target: the stamp raised
    /// the entry, so a retry names the one running.
    Retry,
    /// A newer run of this process's root exists: this process is
    /// superseded.
    Supersede,
    /// Nothing to do: the peer's view lags or this root awaits its binding
    /// (`unbound_root`), or a bug or a copied disk, counted and logged.
    None,
}

/// The sender table's answer to `refusal`.
pub fn effect(refusal: FenceRefusal) -> Effect {
    match refusal {
        FenceRefusal::Fenced => Effect::Retire,
        FenceRefusal::StaleTarget => Effect::Retry,
        FenceRefusal::StaleSender => Effect::Supersede,
        FenceRefusal::UnboundRoot | FenceRefusal::Duplicate | FenceRefusal::Unidentified => {
            Effect::None
        }
    }
}

/// Counters of the fence, since start.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    accepted: AtomicU64,
    discarded: AtomicU64,
    refused: [AtomicU64; 6],
    refusals_received: [AtomicU64; 6],
}

/// What [`crate::PeerCluster::incarnation_counts`] reports.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IncarnationCounts {
    /// Answers whose responder the table accepted.
    pub accepted: u64,
    /// Answers discarded before decoding.
    pub discarded: u64,
    /// Requests this node refused, by refusal in [`FenceRefusal::ALL`]
    /// order.
    pub refused: [u64; 6],
    /// Fence refusals of this node's requests, by refusal, from responders
    /// the table accepted.
    pub refusals_received: [u64; 6],
}

fn slot(refusal: FenceRefusal) -> usize {
    FenceRefusal::ALL
        .iter()
        .position(|known| *known == refusal)
        .unwrap_or(0)
}

impl Counters {
    pub(crate) fn accepted(&self) {
        self.accepted.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn discarded(&self) {
        self.discarded.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn refused(&self, refusal: FenceRefusal) {
        if let Some(count) = self.refused.get(slot(refusal)) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn refusal_received(&self, refusal: FenceRefusal) {
        if let Some(count) = self.refusals_received.get(slot(refusal)) {
            count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn snapshot(&self) -> IncarnationCounts {
        let load = |counts: &[AtomicU64; 6]| counts.each_ref().map(|c| c.load(Ordering::Relaxed));
        IncarnationCounts {
            accepted: self.accepted.load(Ordering::Relaxed),
            discarded: self.discarded.load(Ordering::Relaxed),
            refused: load(&self.refused),
            refusals_received: load(&self.refusals_received),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU64;

    use ursula_proto::placement::ProcessIncarnation;
    use ursula_proto::placement::RootId;
    use ursula_proto::placement::RootSlot;

    use super::Discarded;
    use super::Effect;
    use super::IncarnationBook;
    use super::Judgement;
    use super::Received;
    use super::Rpc;
    use super::Stamp;
    use super::effect;
    use super::judge;
    use super::receive;
    use crate::PeerCluster;
    use crate::PeerClusterError;
    use crate::PeerIdentity;
    use crate::peer_cluster::AdmittedPeer;
    use crate::target::FenceRefusal;
    use crate::target::fence_refusal;
    use crate::target::refusing_node;

    fn superseded(fence: &ursula_runtime::NodeFence) -> bool {
        fence.status(ursula_runtime::node_fence::fence_now())
            == ursula_runtime::node_fence::FenceStatus::Superseded
    }

    fn root(id: u64) -> RootId {
        RootId::new(NonZeroU64::new(id).unwrap())
    }

    /// Run `run` of root `id` with `nonce`: non-default runs and nonces, so
    /// a rule that compared the wrong field would fail.
    fn at(id: u64, run: u32, nonce: u32) -> ProcessIncarnation {
        ProcessIncarnation {
            root: root(id),
            run,
            nonce,
        }
    }

    /// Every row of the table: catches a rule that orders roots, ignores
    /// the slot, lets a released root back, or reads the nonce as an order.
    #[test]
    fn judge_follows_the_view_s_root_then_the_run_then_the_nonce() {
        let entry = at(7, 41, 0xa1);
        let bound = Some(RootSlot::Bound(root(7)));
        for (entry, slot, seen, expected) in [
            // A first entry, with or without a slot naming its root.
            (None, None, at(7, 41, 0xa1), Judgement::Accept),
            (None, bound, at(7, 41, 0xa1), Judgement::Accept),
            (
                None,
                Some(RootSlot::Bound(root(8))),
                at(7, 41, 0xa1),
                Judgement::Unbound,
            ),
            // On the entry's root, the run decides, an equal run's nonce.
            (Some(entry), bound, entry, Judgement::Accept),
            (Some(entry), bound, at(7, 42, 0x01), Judgement::Accept),
            (Some(entry), bound, at(7, 40, 0xff), Judgement::Older),
            (Some(entry), bound, at(7, 41, 0xa2), Judgement::Duplicate),
            (Some(entry), None, at(7, 40, 0xff), Judgement::Older),
            // Without a slot, the first root seen holds.
            (Some(entry), None, at(9, 900, 1), Judgement::Unbound),
            // The view moved the id to another root: the old entry is no
            // evidence, and the old root is refused whatever its run.
            (
                Some(entry),
                Some(RootSlot::Bound(root(9))),
                at(9, 1, 2),
                Judgement::Accept,
            ),
            (
                Some(entry),
                Some(RootSlot::Bound(root(9))),
                at(7, 99, 2),
                Judgement::Unbound,
            ),
            // A released slot binds no root, the released one least of all.
            (
                Some(entry),
                Some(RootSlot::Released(root(7))),
                at(7, 99, 2),
                Judgement::Unbound,
            ),
            (
                None,
                Some(RootSlot::Released(root(7))),
                at(9, 1, 2),
                Judgement::Unbound,
            ),
        ] {
            assert_eq!(
                judge(entry, slot, seen),
                expected,
                "{entry:?} {slot:?} {seen:?}"
            );
        }
    }

    /// The book raises only on acceptance, keeps the first root without a
    /// slot, and drops back to no root's evidence when the view moves.
    #[test]
    fn the_book_rises_within_a_root_and_moves_only_with_the_view() {
        let book = IncarnationBook::default();
        assert_eq!(book.observe(2, at(5, 3, 1)), Judgement::Accept);
        assert_eq!(book.observe(2, at(5, 4, 9)), Judgement::Accept);
        assert_eq!(book.entry(2), Some(at(5, 4, 9)));
        assert_eq!(book.observe(2, at(5, 3, 1)), Judgement::Older);
        assert_eq!(book.observe(2, at(6, 1, 1)), Judgement::Unbound);
        assert_eq!(book.entry(2), Some(at(5, 4, 9)), "a refusal raises nothing");
        // The view binds node 2 to root 6: root 5's entry is no target.
        book.set_slots(&BTreeMap::from([(2, RootSlot::Bound(root(6)))]));
        assert_eq!(book.entry(2), None);
        assert_eq!(book.observe(2, at(5, 5, 1)), Judgement::Unbound);
        assert_eq!(book.observe(2, at(6, 1, 1)), Judgement::Accept);
        assert_eq!(book.entry(2), Some(at(6, 1, 1)));
        // Root 6 released: nothing of node 2 is accepted until a rebind.
        book.set_slots(&BTreeMap::from([(2, RootSlot::Released(root(6)))]));
        assert_eq!(book.entry(2), None);
        assert_eq!(book.observe(2, at(6, 2, 1)), Judgement::Unbound);
    }

    /// Every row of the receiver table, in its order: the target before
    /// the sender, a newer target superseding, forwards exempt from the
    /// sender check only.
    #[test]
    fn the_receiver_checks_the_target_then_the_sender() {
        let me = at(3, 10, 0x33);
        let book = IncarnationBook::default();
        assert_eq!(book.observe(2, at(5, 7, 1)), Judgement::Accept);
        let sender = |run, nonce| Some((2, at(5, run, nonce)));
        let refused = Received::Refused;
        for (sender, target, rpc, expected) in [
            (
                None,
                Some(me),
                Rpc::Raft,
                refused(FenceRefusal::Unidentified),
            ),
            (
                None,
                Some(me),
                Rpc::Forward,
                refused(FenceRefusal::Unidentified),
            ),
            (
                sender(7, 1),
                Some(at(3, 11, 1)),
                Rpc::Raft,
                Received::Superseded,
            ),
            (
                sender(7, 1),
                Some(at(3, 9, 0x33)),
                Rpc::Raft,
                refused(FenceRefusal::StaleTarget),
            ),
            (
                sender(7, 1),
                Some(at(4, 10, 0x33)),
                Rpc::Raft,
                refused(FenceRefusal::StaleTarget),
            ),
            (
                sender(7, 1),
                Some(at(3, 10, 0x34)),
                Rpc::Raft,
                refused(FenceRefusal::Duplicate),
            ),
            // The target decides first, whatever the sender.
            (
                sender(6, 1),
                Some(at(3, 9, 0x33)),
                Rpc::Raft,
                refused(FenceRefusal::StaleTarget),
            ),
            (
                sender(6, 1),
                Some(me),
                Rpc::Raft,
                refused(FenceRefusal::StaleSender),
            ),
            (
                sender(7, 2),
                None,
                Rpc::Raft,
                refused(FenceRefusal::Duplicate),
            ),
            (
                Some((2, at(8, 1, 1))),
                Some(me),
                Rpc::Raft,
                refused(FenceRefusal::UnboundRoot),
            ),
            (sender(6, 1), Some(me), Rpc::Forward, Received::Pass),
            (Some((2, at(8, 1, 1))), None, Rpc::Forward, Received::Pass),
            (sender(7, 1), Some(me), Rpc::Raft, Received::Pass),
            (sender(8, 1), None, Rpc::Raft, Received::Pass),
        ] {
            assert_eq!(
                receive(me, sender, target, rpc, &book),
                expected,
                "{sender:?} {target:?} {rpc:?}"
            );
        }
        assert_eq!(
            book.entry(2),
            Some(at(5, 8, 1)),
            "an accepted sender raised the entry"
        );
    }

    /// Node `node` of the test cluster as the process `me`.
    fn process(node: u64, me: ProcessIncarnation) -> PeerCluster {
        let cluster = PeerCluster::default();
        cluster.bind(
            PeerIdentity::of_process(
                node,
                ursula_headers::ClusterId::new(crate::peer_cluster::TEST_CLUSTER_ID).unwrap(),
                ursula_runtime::NodeFence::unleased(),
                me,
            ),
            crate::PeerDirectory::default(),
        );
        cluster
    }

    fn claims(
        sender: Option<ProcessIncarnation>,
        target: Option<ProcessIncarnation>,
    ) -> AdmittedPeer {
        AdmittedPeer {
            node: 1,
            sender,
            target,
        }
    }

    fn stamp(node: u64, incarnation: Option<ProcessIncarnation>) -> Stamp {
        Stamp {
            node: Some(node),
            incarnation,
        }
    }

    /// The refusal `refusal` of node `node`'s process `incarnation`, as
    /// the stamp layer sends it.
    fn refused(node: u64, incarnation: ProcessIncarnation, refusal: FenceRefusal) -> tonic::Status {
        let mut status = tonic::Status::from(PeerClusterError::Fence {
            node_id: node,
            refusal,
        });
        stamp(node, Some(incarnation)).write(status.metadata_mut());
        status
    }

    /// The receiver seam: a refusal renders as the status the sender reads,
    /// never a misdelivery or a format-epoch mismatch; a newer target
    /// supersedes the process, which refuses everything after; a node
    /// without an incarnation checks only its fence.
    #[test]
    fn a_receiver_refusal_reads_as_one_and_a_newer_target_supersedes() {
        let receiver = process(2, at(3, 10, 1));
        let sender = Some(at(5, 1, 1));
        let status = tonic::Status::from(
            receiver
                .receive(Some(&claims(sender, Some(at(3, 9, 1)))), Rpc::Raft)
                .unwrap_err(),
        );
        assert_eq!(fence_refusal(&status, 2), Some(FenceRefusal::StaleTarget));
        assert_eq!(refusing_node(&status, 2), None);
        assert!(!crate::format_epoch::is_protocol_mismatch(&status));
        assert!(matches!(
            receiver.receive(None, Rpc::Forward),
            Err(PeerClusterError::Fence {
                refusal: FenceRefusal::Unidentified,
                ..
            })
        ));
        receiver
            .receive(Some(&claims(sender, Some(at(3, 10, 1)))), Rpc::Raft)
            .unwrap();
        assert!(matches!(
            receiver.receive(Some(&claims(sender, Some(at(3, 11, 1)))), Rpc::Raft),
            Err(PeerClusterError::Fence {
                refusal: FenceRefusal::Fenced,
                ..
            })
        ));
        assert!(superseded(&receiver.fence()));
        assert!(matches!(
            receiver.receive(Some(&claims(sender, None)), Rpc::Forward),
            Err(PeerClusterError::Fence {
                refusal: FenceRefusal::Fenced,
                ..
            })
        ));
        let counts = receiver.incarnation_counts();
        assert_eq!(counts.refused, [2, 1, 0, 0, 0, 1]);
        crate::peer_cluster::test_peer_cluster(2)
            .receive(None, Rpc::Raft)
            .unwrap();
    }

    /// The sender seam on answers: only a stamp of the target that the
    /// table accepts is used, and it raises the entry.
    #[test]
    fn the_sender_uses_only_answers_the_table_accepts() {
        let sender = process(1, at(5, 1, 1));
        assert_eq!(
            sender.accept_responder(2, stamp(3, Some(at(3, 10, 1)))),
            Err(Discarded::OtherNode { node: 3 })
        );
        assert_eq!(
            sender.accept_responder(2, stamp(2, None)),
            Err(Discarded::Unstamped)
        );
        sender
            .accept_responder(2, stamp(2, Some(at(3, 10, 1))))
            .unwrap();
        assert_eq!(sender.target_incarnation(2), Some(at(3, 10, 1)));
        assert_eq!(
            sender.accept_responder(2, stamp(2, Some(at(3, 9, 1)))),
            Err(Discarded::Stale {
                judgement: Judgement::Older
            })
        );
        sender.fence().close();
        assert_eq!(
            sender.accept_responder(2, stamp(2, Some(at(3, 10, 1)))),
            Err(Discarded::Fenced)
        );
        let counts = sender.incarnation_counts();
        assert_eq!((counts.accepted, counts.discarded), (1, 4));
    }

    /// The precedence of a refusal's readings: a misdelivery and the
    /// mismatch text decide before the fence refusal, and a refusal acts
    /// only from the process the table accepts, so a stale process of the
    /// target cannot supersede this one.
    #[test]
    fn a_fence_refusal_is_read_last_and_only_from_the_accepted_process() {
        let sender = process(1, at(5, 4, 1));
        sender
            .accept_responder(2, stamp(2, Some(at(3, 10, 1))))
            .unwrap();
        // Node 3 answers at node 2's address: a misdelivery, nothing read.
        let misdelivered = refused(3, at(9, 1, 1), FenceRefusal::StaleSender);
        assert_eq!(fence_refusal(&misdelivered, 2), None);
        assert_eq!(refusing_node(&misdelivered, 2), Some(3));
        assert_eq!(sender.observe_failure(2, &misdelivered), None);
        let mut mismatch = tonic::Status::failed_precondition(format!(
            "{}: local=6, remote=5",
            crate::format_epoch::PROTOCOL_MISMATCH_TEXT
        ));
        mismatch.metadata_mut().insert(
            crate::target::FENCE_METADATA,
            tonic::metadata::AsciiMetadataValue::from_static("stale_sender"),
        );
        assert_eq!(fence_refusal(&mismatch, 2), None);
        assert_eq!(sender.observe_failure(2, &mismatch), None);
        // An older process of node 2 is discarded, and supersedes nobody.
        let zombie = refused(2, at(3, 9, 1), FenceRefusal::StaleSender);
        assert_eq!(sender.observe_failure(2, &zombie), Some(Effect::Retire));
        assert!(!superseded(&sender.fence()));
        // The process that runs: a newer one raises the entry for a retry,
        // and its `stale_sender` supersedes this process.
        let newer = refused(2, at(3, 11, 1), FenceRefusal::StaleTarget);
        assert_eq!(sender.observe_failure(2, &newer), Some(Effect::Retry));
        assert_eq!(sender.target_incarnation(2), Some(at(3, 11, 1)));
        let superseding = refused(2, at(3, 11, 1), FenceRefusal::StaleSender);
        assert_eq!(
            sender.observe_failure(2, &superseding),
            Some(Effect::Supersede)
        );
        assert!(superseded(&sender.fence()));
        assert_eq!(sender.incarnation_counts().refusals_received, [
            0, 1, 1, 0, 0, 0
        ]);
    }

    /// The view's roots: a root the view does not bind is refused.
    #[test]
    fn a_view_binds_the_table_s_roots() {
        use ursula_control::ControlPlaneState;
        use ursula_control::PlacementView;
        use ursula_control::Registration;
        use ursula_proto::placement::NodeId;
        let cluster = process(1, at(5, 4, 1));
        let view = |mine: RootSlot| PlacementView {
            stage: ursula_control::ViewStage::Empty,
            state: std::sync::Arc::new(ControlPlaneState::Forming {
                registered: BTreeMap::from([
                    (NodeId::new(1), Registration {
                        since_ms: 1,
                        root: mine,
                    }),
                    (NodeId::new(2), Registration {
                        since_ms: 2,
                        root: RootSlot::Bound(root(3)),
                    }),
                ]),
            }),
        };
        cluster.observe_view(&view(RootSlot::Bound(root(5))));
        assert_eq!(
            cluster.accept_responder(2, stamp(2, Some(at(4, 1, 1)))),
            Err(Discarded::Stale {
                judgement: Judgement::Unbound
            })
        );
        cluster
            .accept_responder(2, stamp(2, Some(at(3, 1, 1))))
            .unwrap();
    }

    /// The sender table: each refusal's effect.
    #[test]
    fn each_fence_refusal_has_one_effect_on_the_sender() {
        for (refusal, expected) in [
            (FenceRefusal::Fenced, Effect::Retire),
            (FenceRefusal::StaleTarget, Effect::Retry),
            (FenceRefusal::StaleSender, Effect::Supersede),
            (FenceRefusal::UnboundRoot, Effect::None),
            (FenceRefusal::Duplicate, Effect::None),
            (FenceRefusal::Unidentified, Effect::None),
        ] {
            assert_eq!(effect(refusal), expected, "{refusal:?}");
        }
    }
}
