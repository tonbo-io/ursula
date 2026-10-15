//! Property tests: every valid value renders and parses back equal, every
//! value's canonical text parses back to it, and no parse function panics
//! on arbitrary statuses, header lists or bodies.

use proptest::collection::vec;
use proptest::option;
use proptest::prelude::Just;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prelude::prop;
use proptest::prop_assert_eq;
use proptest::prop_oneof;
use proptest::proptest;

use crate::answer::ClosedEpoch;
use crate::answer::CurrentEpoch;
use crate::answer::Delivery;
use crate::answer::EpochClosure;
use crate::answer::Outcome;
use crate::answer::ProducerAnswer;
use crate::answer::Receipt;
use crate::head::ClusterId;
use crate::head::Inherited;
use crate::head::ServerIdentity;
use crate::head::StreamHead;
use crate::limit::SizeLimit;
use crate::names;
use crate::refusal::IncarnationMismatch;
use crate::refusal::StreamClosedConflict;
use crate::request::IncarnationPrecondition;
use crate::request::ProducerRequestHeaders;
use crate::request::ProducerWrite;
use crate::routing::NodeId;
use crate::routing::RaftGroup;
use crate::routing::RaftTerm;
use crate::routing::RoutingAnswer;
use crate::snapshot::MAX_SNAPSHOT_PARTS;
use crate::snapshot::PartCount;
use crate::snapshot::PartId;
use crate::snapshot::PartStaged;
use crate::snapshot::PartsCommit;
use crate::snapshot::PartsManifest;
use crate::snapshot::SnapshotPublishAnswer;
use crate::snapshot::SnapshotRead;
use crate::snapshot::SnapshotRef;
use crate::value::BodyDigest;
use crate::value::Epoch;
use crate::value::Incarnation;
use crate::value::MAX_INCARNATION;
use crate::value::MAX_PRODUCER_INTEGER;
use crate::value::Offset;
use crate::value::ProducerId;
use crate::value::Seq;
use crate::value::SnapshotDigest;

type Pairs = Vec<(&'static str, String)>;

/// The headers `render` emits.
fn rendered(render: impl FnOnce(&mut dyn FnMut(&'static str, &str))) -> Pairs {
    let mut pairs = Pairs::new();
    render(&mut |name, value| pairs.push((name, value.to_owned())));
    pairs
}

fn iter(pairs: &Pairs) -> impl Iterator<Item = (&str, &[u8])> {
    pairs.iter().map(|(name, value)| (*name, value.as_bytes()))
}

// Value strategies.

fn offset() -> impl Strategy<Value = Offset> {
    prop_oneof![any::<u64>(), Just(0), Just(u64::MAX),].prop_map(Offset::from_u64)
}

fn incarnation() -> impl Strategy<Value = Incarnation> {
    prop_oneof![1..=MAX_INCARNATION, Just(1), Just(MAX_INCARNATION)]
        .prop_map(|token| Incarnation::try_from(token).unwrap())
}

fn epoch() -> impl Strategy<Value = Epoch> {
    prop_oneof![
        0..=MAX_PRODUCER_INTEGER,
        Just(0),
        Just(MAX_PRODUCER_INTEGER)
    ]
    .prop_map(|value| Epoch::try_from(value).unwrap())
}

fn seq() -> impl Strategy<Value = Seq> {
    prop_oneof![
        0..=MAX_PRODUCER_INTEGER,
        Just(0),
        Just(MAX_PRODUCER_INTEGER)
    ]
    .prop_map(|value| Seq::try_from(value).unwrap())
}

fn producer_id() -> impl Strategy<Value = ProducerId> {
    "[!-~]([ \t!-~]{0,254}[!-~])?".prop_map(|id| ProducerId::new(id).unwrap())
}

fn body_digest() -> impl Strategy<Value = BodyDigest> {
    any::<[u8; 32]>().prop_map(BodyDigest::from_bytes)
}

fn snapshot_digest() -> impl Strategy<Value = SnapshotDigest> {
    any::<[u8; 32]>().prop_map(SnapshotDigest::from_bytes)
}

fn snapshot_ref() -> impl Strategy<Value = SnapshotRef> {
    (offset(), snapshot_digest()).prop_map(|(offset, digest)| SnapshotRef { offset, digest })
}

fn part_id() -> impl Strategy<Value = PartId> {
    "[!-~]{1,64}".prop_map(|id| PartId::new(id).unwrap())
}

fn part_count() -> impl Strategy<Value = PartCount> {
    (1..=MAX_SNAPSHOT_PARTS).prop_map(|count| PartCount::try_from(count).unwrap())
}

fn cluster_id() -> impl Strategy<Value = ClusterId> {
    "[A-Za-z0-9._-]{1,64}".prop_map(|id| ClusterId::new(id).unwrap())
}

fn closed_epoch_below(epoch: Epoch) -> impl Strategy<Value = ClosedEpoch> {
    (0..epoch.get(), seq()).prop_map(|(closed, seq)| ClosedEpoch {
        epoch: Epoch::try_from(closed).unwrap(),
        seq,
    })
}

fn current_epoch() -> impl Strategy<Value = CurrentEpoch> {
    prop_oneof![
        epoch().prop_map(CurrentEpoch::new),
        epoch()
            .prop_filter("an epoch above 0 can close another", |epoch| {
                *epoch > Epoch::ZERO
            })
            .prop_flat_map(|epoch| (Just(epoch), closed_epoch_below(epoch)))
            .prop_map(|(epoch, closed)| CurrentEpoch::closing(epoch, closed).unwrap()),
    ]
}

fn epoch_closure() -> impl Strategy<Value = EpochClosure> {
    prop_oneof![
        epoch()
            .prop_filter("an epoch above 0 can close another", |epoch| {
                *epoch > Epoch::ZERO
            })
            .prop_flat_map(|epoch| (Just(epoch), closed_epoch_below(epoch)))
            .prop_map(|(epoch, closed)| EpochClosure::by_claim(epoch, closed).unwrap()),
        (epoch(), seq())
            .prop_map(|(epoch, seq)| EpochClosure::by_history(ClosedEpoch { epoch, seq })),
    ]
}

fn receipt() -> impl Strategy<Value = Receipt> {
    (offset(), offset(), any::<bool>())
        .prop_map(|(a, b, closes)| Receipt::new(a.min(b), a.max(b), closes).unwrap())
}

fn producer_answer() -> impl Strategy<Value = ProducerAnswer> {
    let delivery = prop_oneof![Just(Delivery::Applied), Just(Delivery::Repeat)];
    prop_oneof![
        (delivery, current_epoch(), seq(), receipt(), incarnation()).prop_map(
            |(delivery, epoch, seq, receipt, incarnation)| ProducerAnswer::Receipt {
                delivery,
                epoch,
                seq,
                receipt,
                incarnation,
            }
        ),
        (current_epoch(), seq(), incarnation()).prop_map(|(epoch, seq, incarnation)| {
            ProducerAnswer::ReceiptEvicted {
                epoch,
                seq,
                incarnation,
            }
        }),
        epoch_closure().prop_map(ProducerAnswer::EpochClosed),
        epoch().prop_map(|epoch| ProducerAnswer::KeyConflict { epoch }),
        epoch().prop_map(|epoch| ProducerAnswer::ProducerExpired { epoch }),
        epoch().prop_map(|epoch| ProducerAnswer::StaleEpoch { epoch }),
        (seq(), seq())
            .prop_map(|(expected, received)| ProducerAnswer::SeqGap { expected, received }),
        Just(ProducerAnswer::EpochNotClaimed),
    ]
}

fn request_headers() -> impl Strategy<Value = ProducerRequestHeaders> {
    let write = (producer_id(), epoch(), seq(), option::of(body_digest())).prop_map(
        |(id, epoch, seq, digest)| ProducerWrite {
            id,
            epoch,
            seq,
            digest,
        },
    );
    let precondition =
        incarnation().prop_map(|incarnation| IncarnationPrecondition { incarnation });
    (option::of(write), option::of(precondition)).prop_map(|(producer, precondition)| {
        ProducerRequestHeaders {
            producer,
            precondition,
        }
    })
}

fn stream_head() -> impl Strategy<Value = StreamHead> {
    (incarnation(), offset())
        .prop_flat_map(|(current, next)| {
            let inherited = (
                incarnation().prop_filter("the previous token differs", move |previous| {
                    *previous != current
                }),
                0..=next.get(),
            )
                .prop_map(|(previous_incarnation, offset)| Inherited {
                    previous_incarnation,
                    offset: Offset::from_u64(offset),
                });
            (
                Just(current),
                Just(next),
                0..=next.get(),
                any::<bool>(),
                option::of(snapshot_ref()),
                option::of(inherited),
                any::<u64>(),
                option::of(any::<u64>()),
            )
        })
        .prop_map(
            |(
                incarnation,
                next,
                retained,
                closed,
                snapshot,
                inherited,
                max_request,
                max_snapshot,
            )| {
                StreamHead::new(incarnation, next, Offset::from_u64(retained), max_request)
                    .unwrap()
                    .with_closed(closed)
                    .with_snapshot(snapshot)
                    .with_max_snapshot_bytes(max_snapshot)
                    .with_inherited(inherited)
                    .unwrap()
            },
        )
}

fn routing_answer() -> impl Strategy<Value = RoutingAnswer> {
    let group = any::<u32>().prop_map(RaftGroup::from_u32);
    prop_oneof![
        (any::<u64>(), group.clone(), any::<u64>()).prop_map(|(leader, group, term)| {
            RoutingAnswer::NotLeader {
                leader: NodeId::from_u64(leader),
                group,
                term: RaftTerm::from_u64(term),
            }
        }),
        group.prop_map(|group| RoutingAnswer::NoLeader { group }),
    ]
}

// Fuzz strategies.

const NAMES: [&str; 31] = [
    names::PRODUCER_ID,
    names::PRODUCER_EPOCH,
    names::PRODUCER_SEQ,
    names::PRODUCER_DIGEST,
    names::PRODUCER_OUTCOME,
    names::PRODUCER_CLOSED_EPOCH,
    names::PRODUCER_CLOSED_SEQ,
    names::PRODUCER_EXPECTED_SEQ,
    names::PRODUCER_RECEIVED_SEQ,
    names::STREAM_INCARNATION,
    names::STREAM_PREVIOUS_INCARNATION,
    names::STREAM_INHERITED_OFFSET,
    names::STREAM_OFFSET,
    names::STREAM_NEXT_OFFSET,
    names::STREAM_CLOSED,
    names::STREAM_UP_TO_DATE,
    names::STREAM_RETAINED_OFFSET,
    names::STREAM_SNAPSHOT_OFFSET,
    names::STREAM_SNAPSHOT_DIGEST,
    names::STREAM_SNAPSHOT_PART,
    names::STREAM_SNAPSHOT_PARTS,
    names::URSULA_PROTOCOL,
    names::URSULA_CLUSTER_ID,
    names::URSULA_MAX_REQUEST_BYTES,
    names::URSULA_MAX_SNAPSHOT_BYTES,
    names::X_URSULA_RAFT_LEADER_ID,
    names::X_URSULA_RAFT_GROUP,
    names::X_URSULA_RAFT_TERM,
    "content-type",
    "location",
    "retry-after",
];

/// A header name: one this crate reads, in any case, or arbitrary text.
fn fuzz_name() -> impl Strategy<Value = String> {
    prop_oneof![
        (prop::sample::select(&NAMES[..]), any::<bool>()).prop_map(|(name, upper)| {
            if upper {
                name.to_ascii_uppercase()
            } else {
                name.to_owned()
            }
        }),
        ".{0,24}",
    ]
}

/// A header value: arbitrary bytes, or text close to a valid value.
fn fuzz_value() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        vec(any::<u8>(), 0..80),
        "[0-9]{0,24}".prop_map(String::into_bytes),
        "[ \t]?[0-9a-f+-]{0,66}[ \t]?".prop_map(String::into_bytes),
        prop::sample::select(
            &[
                "true",
                "TRUE",
                "receipt-evicted",
                "epoch-closed;by=claim",
                "epoch-closed;by=history",
                "key-conflict",
                "producer-expired",
                "stale-epoch",
                "0",
                "9007199254740991",
                "9007199254740992",
                "18446744073709551616",
            ][..]
        )
        .prop_map(|value| value.as_bytes().to_vec()),
    ]
}

fn fuzz_headers() -> impl Strategy<Value = Vec<(String, Vec<u8>)>> {
    vec((fuzz_name(), fuzz_value()), 0..16)
}

fn fuzz_status() -> impl Strategy<Value = u16> {
    prop_oneof![
        any::<u16>(),
        prop::sample::select(&[200_u16, 201, 204, 307, 400, 403, 404, 409, 412, 413, 503][..]),
    ]
}

fn fuzz_iter(headers: &[(String, Vec<u8>)]) -> impl Iterator<Item = (&str, &[u8])> {
    headers
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_slice()))
}

proptest! {
    #[test]
    fn values_round_trip_through_their_canonical_text(
        offset in offset(),
        incarnation in incarnation(),
        epoch in epoch(),
        seq in seq(),
        id in producer_id(),
        body in body_digest(),
        snapshot in snapshot_digest(),
        part in part_id(),
        cluster in cluster_id(),
    ) {
        prop_assert_eq!(Offset::parse("x", &offset.to_string()), Ok(offset));
        prop_assert_eq!(Offset::from_u64(offset.get()), offset);
        prop_assert_eq!(Incarnation::parse("x", &incarnation.to_string()), Ok(incarnation));
        prop_assert_eq!(Incarnation::try_from(incarnation.get()), Ok(incarnation));
        prop_assert_eq!(Epoch::parse("x", &epoch.to_string()), Ok(epoch));
        prop_assert_eq!(Epoch::try_from(epoch.get()), Ok(epoch));
        prop_assert_eq!(Seq::parse("x", &seq.to_string()), Ok(seq));
        prop_assert_eq!(Seq::try_from(seq.get()), Ok(seq));
        prop_assert_eq!(ProducerId::new(id.to_string()), Ok(id));
        prop_assert_eq!(BodyDigest::parse("x", &body.to_string()), Ok(body));
        prop_assert_eq!(SnapshotDigest::parse("x", &snapshot.to_string()), Ok(snapshot));
        prop_assert_eq!(PartId::new(part.to_string()), Ok(part));
        prop_assert_eq!(ClusterId::new(cluster.to_string()), Ok(cluster));
    }

    #[test]
    fn requests_round_trip(headers in request_headers()) {
        let pairs = rendered(|emit| headers.render(emit));
        prop_assert_eq!(ProducerRequestHeaders::parse(iter(&pairs)), Ok(headers.clone()));
        if let Some(precondition) = headers.precondition {
            prop_assert_eq!(IncarnationPrecondition::parse(iter(&pairs)), Ok(Some(precondition)));
        }
    }

    #[test]
    fn producer_answers_round_trip(answer in producer_answer()) {
        let pairs = rendered(|emit| answer.render(emit));
        prop_assert_eq!(ProducerAnswer::parse(answer.status(), iter(&pairs)), Ok(Some(answer)));
    }

    #[test]
    fn outcomes_round_trip(by_claim in any::<bool>(), index in 0_usize..5) {
        let by = if by_claim { crate::answer::ClosedBy::Claim } else { crate::answer::ClosedBy::History };
        let outcomes = [
            Outcome::ReceiptEvicted,
            Outcome::EpochClosed(by),
            Outcome::KeyConflict,
            Outcome::ProducerExpired,
            Outcome::StaleEpoch,
        ];
        let outcome = outcomes[index];
        prop_assert_eq!(Outcome::parse(outcome.as_str()), Ok(outcome));
    }

    #[test]
    fn heads_round_trip(
        identity in (any::<u32>(), cluster_id())
            .prop_map(|(protocol, cluster_id)| ServerIdentity { protocol, cluster_id }),
        head in stream_head(),
    ) {
        let pairs = rendered(|emit| identity.render(emit));
        prop_assert_eq!(ServerIdentity::parse(iter(&pairs)), Ok(Some(identity)));
        let pairs = rendered(|emit| head.render(emit));
        prop_assert_eq!(StreamHead::parse(iter(&pairs)), Ok(head));
    }

    #[test]
    fn snapshot_headers_round_trip(
        read in (snapshot_ref(), incarnation(), any::<bool>()).prop_map(
            |(snapshot, incarnation, up_to_date)| SnapshotRead { snapshot, incarnation, up_to_date }
        ),
        published in (snapshot_ref(), incarnation()).prop_map(
            |(snapshot, incarnation)| SnapshotPublishAnswer::Published { snapshot, incarnation }
        ),
        latest in snapshot_ref(),
        part in part_id(),
        commit in (part_count(), snapshot_digest())
            .prop_map(|(parts, digest)| PartsCommit { parts, digest }),
    ) {
        let pairs = rendered(|emit| read.render(emit));
        prop_assert_eq!(SnapshotRead::parse(iter(&pairs)), Ok(read));
        let conflict = SnapshotPublishAnswer::Conflict { latest };
        for answer in [published, conflict] {
            let pairs = rendered(|emit| answer.render(emit));
            prop_assert_eq!(SnapshotPublishAnswer::parse(answer.status(), iter(&pairs)), Ok(Some(answer)));
        }
        let staged = PartStaged { part };
        let pairs = rendered(|emit| staged.render(emit));
        prop_assert_eq!(PartStaged::parse(iter(&pairs)), Ok(staged));
        let pairs = rendered(|emit| commit.render(emit));
        prop_assert_eq!(PartsCommit::parse(iter(&pairs)), Ok(Some(commit)));
    }

    #[test]
    fn manifests_round_trip(parts in vec(part_id(), 1..64)) {
        let manifest = PartsManifest::new(parts).unwrap();
        let body = manifest.render();
        prop_assert_eq!(PartsManifest::parse(manifest.count(), body.as_bytes()), Ok(manifest));
    }

    #[test]
    fn limits_routing_and_refusals_round_trip(
        limit in prop_oneof![
            any::<u64>().prop_map(SizeLimit::Request),
            any::<u64>().prop_map(SizeLimit::Snapshot),
        ],
        routing in routing_answer(),
        current in option::of(incarnation()),
        final_offset in offset(),
    ) {
        let pairs = rendered(|emit| limit.render(emit));
        prop_assert_eq!(SizeLimit::parse(iter(&pairs)), Ok(Some(limit)));
        let pairs = rendered(|emit| routing.render(emit));
        prop_assert_eq!(RoutingAnswer::parse(routing.status(), iter(&pairs)), Ok(Some(routing)));
        let mismatch = IncarnationMismatch { current };
        let pairs = rendered(|emit| mismatch.render(emit));
        prop_assert_eq!(IncarnationMismatch::parse(412, iter(&pairs)), Ok(Some(mismatch)));
        let conflict = StreamClosedConflict { final_offset };
        let pairs = rendered(|emit| conflict.render(emit));
        prop_assert_eq!(StreamClosedConflict::parse(409, iter(&pairs)), Ok(Some(conflict)));
    }

    /// Every parse function returns, never panics, on arbitrary input; and
    /// whatever one accepts renders back to the same value.
    #[test]
    fn header_parsers_never_panic(status in fuzz_status(), headers in fuzz_headers()) {
        if let Ok(parsed) = ProducerRequestHeaders::parse(fuzz_iter(&headers)) {
            let pairs = rendered(|emit| parsed.render(emit));
            prop_assert_eq!(ProducerRequestHeaders::parse(iter(&pairs)), Ok(parsed));
        }
        let _precondition = IncarnationPrecondition::parse(fuzz_iter(&headers));
        if let Ok(Some(parsed)) = ProducerAnswer::parse(status, fuzz_iter(&headers)) {
            let pairs = rendered(|emit| parsed.render(emit));
            prop_assert_eq!(ProducerAnswer::parse(parsed.status(), iter(&pairs)), Ok(Some(parsed)));
        }
        let _identity = ServerIdentity::parse(fuzz_iter(&headers));
        if let Ok(parsed) = StreamHead::parse(fuzz_iter(&headers)) {
            let pairs = rendered(|emit| parsed.render(emit));
            prop_assert_eq!(StreamHead::parse(iter(&pairs)), Ok(parsed));
        }
        let _read = SnapshotRead::parse(fuzz_iter(&headers));
        let _publish = SnapshotPublishAnswer::parse(status, fuzz_iter(&headers));
        let _staged = PartStaged::parse(fuzz_iter(&headers));
        let _commit = PartsCommit::parse(fuzz_iter(&headers));
        let _limit = SizeLimit::parse(fuzz_iter(&headers));
        let _routing = RoutingAnswer::parse(status, fuzz_iter(&headers));
        let _mismatch = IncarnationMismatch::parse(status, fuzz_iter(&headers));
        let _closed = StreamClosedConflict::parse(status, fuzz_iter(&headers));
    }

    #[test]
    fn value_parsers_never_panic(text in ".{0,80}", bytes in vec(any::<u8>(), 0..80)) {
        let _outcome = Outcome::parse(&text);
        let _offset = Offset::parse("x", &text);
        let _incarnation = Incarnation::parse("x", &text);
        let _epoch = Epoch::parse("x", &text);
        let _seq = Seq::parse("x", &text);
        let _body = BodyDigest::parse("x", &text);
        let _snapshot = SnapshotDigest::parse("x", &text);
        let _id = ProducerId::new(text.clone());
        let _part = PartId::new(text.clone());
        let _cluster = ClusterId::new(text);
        let _from_bytes = String::from_utf8(bytes).map(ProducerId::new);
    }

    #[test]
    fn manifest_parser_never_panics(
        count in part_count(),
        body in prop_oneof![
            vec(any::<u8>(), 0..256),
            vec(prop_oneof![Just(b'\n'), Just(b'\r'), Just(b' '), Just(b'a')], 0..64),
        ],
    ) {
        if let Ok(manifest) = PartsManifest::parse(count, &body) {
            prop_assert_eq!(manifest.count(), count);
            prop_assert_eq!(manifest.render().into_bytes(), body);
        }
    }
}
