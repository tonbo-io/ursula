#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts, bits and positions from small generated sizes"
)]

use proptest::collection::vec;
use proptest::option;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::prop_oneof;
use proptest::proptest;

use super::Record;
use super::RecordBody;
use super::SegmentError;
use super::SegmentHeader;
use super::Tail;
use super::encode_header;
use super::encode_record;
use super::scan;
use crate::ids::Activation;
use crate::ids::Epoch;
use crate::ids::Lineage;
use crate::ids::Position;

fn header() -> impl Strategy<Value = SegmentHeader> {
    (
        (
            any::<[u8; 16]>(),
            0u64..1 << 40,
            option::of(any::<[u8; 16]>()),
            "[a-z0-9-]{0,12}",
            "/[a-z/]{0,20}",
        ),
        (
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<[u8; 16]>(),
        ),
    )
        .prop_map(
            |(
                (lineage, first, supersedes, cluster_id, stream_path),
                (incarnation, base, acked, acked_offset, log_bytes, epoch, activation),
            )| SegmentHeader {
                lineage: Lineage(lineage),
                first_position: Position(first),
                supersedes: supersedes.map(Lineage),
                cluster_id,
                stream_path,
                incarnation,
                base: Position(base),
                acked: Position(acked),
                acked_offset,
                log_bytes,
                claim_epoch: Epoch(epoch),
                claim_activation: Activation(activation),
            },
        )
}

fn body() -> impl Strategy<Value = RecordBody> {
    prop_oneof![
        vec(any::<u8>(), 1..48).prop_map(|frame| RecordBody::Commit { frame }),
        (any::<u64>(), any::<[u8; 16]>()).prop_map(|(e, a)| RecordBody::Claim {
            epoch: Epoch(e),
            activation: Activation(a)
        }),
        (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(a, o, b)| RecordBody::Ack {
            acked: Position(a),
            acked_offset: o,
            log_bytes: b
        }),
        any::<u64>().prop_map(|p| RecordBody::Base {
            position: Position(p)
        }),
    ]
}

/// A segment of `bodies` at consecutive positions, and the records a scan should return.
fn segment(h: &SegmentHeader, bodies: &[RecordBody]) -> (Vec<u8>, usize, Vec<Record>) {
    let (mut bytes, mut chain) = encode_header(h).unwrap();
    let header_len = bytes.len();
    let mut records = Vec::new();
    for (i, body) in bodies.iter().enumerate() {
        let position = Position(h.first_position.0 + i as u64);
        let (record, next) = encode_record(chain, position, body).unwrap();
        bytes.extend(record);
        chain = next;
        records.push(Record {
            position,
            body: body.clone(),
            chain,
            end: bytes.len(),
        });
    }
    (bytes, header_len, records)
}

proptest! {
    #[test]
    fn segments_round_trip(h in header(), bodies in vec(body(), 0..8)) {
        let (bytes, header_len, records) = segment(&h, &bodies);
        let s = scan(&bytes).unwrap();
        prop_assert_eq!(s.header, h);
        prop_assert_eq!(s.header_len, header_len);
        prop_assert_eq!(s.records, records);
        prop_assert_eq!(s.tail, Tail::Clean);
    }

    // A crash may cut the segment anywhere: the scan returns exactly the records that end at or
    // before the cut, and a torn tail after them.
    #[test]
    fn a_cut_keeps_the_records_before_it(h in header(), bodies in vec(body(), 1..6)) {
        let (bytes, header_len, records) = segment(&h, &bodies);
        for cut in header_len..=bytes.len() {
            let s = scan(&bytes[..cut]).unwrap();
            let kept: Vec<Record> = records.iter().filter(|r| r.end <= cut).cloned().collect();
            let at = kept.last().map_or(header_len, |r| r.end);
            prop_assert_eq!(&s.records, &kept, "cut at {}", cut);
            if at == cut {
                prop_assert_eq!(s.tail, Tail::Clean);
            } else {
                prop_assert_eq!(s.tail, Tail::Torn { at });
            }
        }
        for cut in 0..header_len {
            prop_assert!(matches!(
                scan(&bytes[..cut]),
                Err(SegmentError::HeaderTruncated { len }) if len == cut
            ), "cut at {}", cut);
        }
    }

    // A damaged bit anywhere in a record breaks the chain there: the prefix before it is the log.
    #[test]
    fn a_chain_break_keeps_the_prefix_before_it(
        h in header(),
        bodies in vec(body(), 1..6),
        pick in any::<usize>(),
        bit in any::<usize>(),
    ) {
        let (mut bytes, header_len, records) = segment(&h, &bodies);
        let i = pick % records.len();
        let start = if i == 0 { header_len } else { records[i - 1].end };
        let bit = start * 8 + bit % ((records[i].end - start) * 8);
        bytes[bit / 8] ^= 1 << (bit % 8);
        let s = scan(&bytes).unwrap();
        prop_assert_eq!(&s.records[..], &records[..i]);
        prop_assert_eq!(s.tail, Tail::Torn { at: start });
    }

    // A record whose chain verifies but whose position skips is a gap, never a torn tail.
    #[test]
    fn a_position_gap_under_valid_chains_is_a_gap(
        h in header(),
        bodies in vec(body(), 2..6),
        pick in any::<usize>(),
        skip in 1u64..5,
    ) {
        let (_, header_len, records) = segment(&h, &bodies);
        let i = 1 + pick % (records.len() - 1);
        let mut bytes = encode_header(&h).unwrap().0;
        let mut chain = encode_header(&h).unwrap().1;
        for (j, body) in bodies.iter().enumerate() {
            let mut position = h.first_position.0 + j as u64;
            if j >= i {
                position += skip;
            }
            let (record, next) = encode_record(chain, Position(position), body).unwrap();
            bytes.extend(record);
            chain = next;
        }
        let s = scan(&bytes).unwrap();
        prop_assert_eq!(&s.records[..], &records[..i]);
        let expected = Position(h.first_position.0 + i as u64);
        prop_assert_eq!(s.tail, Tail::Gap {
            at: records[i - 1].end,
            expected,
            found: Position(expected.0 + skip),
        });
        prop_assert!(header_len < records[i - 1].end);
    }
}

fn sample() -> SegmentHeader {
    SegmentHeader {
        lineage: Lineage([1; 16]),
        first_position: Position(10),
        supersedes: None,
        cluster_id: "c".into(),
        stream_path: "/b/s".into(),
        incarnation: 1 << 52,
        base: Position(9),
        acked: Position(9),
        acked_offset: 42,
        log_bytes: 0,
        claim_epoch: Epoch(3),
        claim_activation: Activation([2; 16]),
    }
}

// A header of another format or a damaged one fails and names what it found.
#[test]
fn header_errors_name_what_they_found() {
    let (bytes, crc) = encode_header(&sample()).unwrap();
    let mut magic = bytes.clone();
    magic[3] = b'0';
    let err = scan(&magic).unwrap_err();
    assert!(matches!(&err, SegmentError::Magic { found } if found == b"USL0"));
    assert!(err.to_string().starts_with("segment magic \"USL0\""));
    let mut version = bytes.clone();
    version[4] = 2;
    assert!(matches!(
        scan(&version),
        Err(SegmentError::Version { found: 2 })
    ));
    let mut damaged = bytes.clone();
    damaged[20] ^= 1;
    assert!(matches!(
        scan(&damaged),
        Err(SegmentError::HeaderChecksum { found, .. }) if found == crc
    ));
    let mut tag = bytes.clone();
    tag[32] = 7;
    assert!(matches!(
        scan(&tag),
        Err(SegmentError::SupersedesTag { tag: 7 })
    ));
}

// A record whose chain verifies but whose payload no record of its kind has is malformed.
#[test]
fn a_chained_record_of_no_known_shape_is_malformed() {
    let (mut bytes, seed) = encode_header(&sample()).unwrap();
    let at = bytes.len();
    let payload = [0u8; 3];
    for kind in [2u8, 9] {
        let mut b = bytes.clone();
        b.extend_from_slice(&3u32.to_le_bytes());
        b.push(kind);
        b.extend_from_slice(&10u64.to_le_bytes());
        b.extend_from_slice(&payload);
        b.extend_from_slice(&super::chain(seed, kind, Position(10), &payload).to_le_bytes());
        let s = scan(&b).unwrap();
        assert_eq!(s.tail, Tail::Malformed { at, kind });
        assert!(s.records.is_empty());
    }
    bytes.extend(
        encode_record(seed, Position(10), &RecordBody::Base {
            position: Position(10),
        })
        .unwrap()
        .0,
    );
    assert_eq!(scan(&bytes).unwrap().tail, Tail::Clean);
}

#[test]
fn long_strings_are_refused() {
    let mut h = sample();
    h.stream_path = "x".repeat(70_000);
    assert!(matches!(
        encode_header(&h),
        Err(SegmentError::StringTooLong {
            field: "stream_path",
            len: 70_000
        })
    ));
}
