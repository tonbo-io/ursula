#![expect(
    clippy::arithmetic_side_effects,
    reason = "tests compute cuts, bits and positions from small generated sizes"
)]

mod oracle;

use std::convert::Infallible;

use proptest::collection::vec;
use proptest::option;
use proptest::prelude::Just;
use proptest::prelude::ProptestConfig;
use proptest::prelude::Strategy;
use proptest::prelude::any;
use proptest::prop_assert;
use proptest::prop_assert_eq;
use proptest::prop_oneof;
use proptest::proptest;
use ursula_headers::Epoch;
use ursula_headers::Incarnation;
use ursula_headers::MAX_INCARNATION;
use ursula_headers::MAX_PRODUCER_INTEGER;
use ursula_headers::Offset;

use super::BUFFER;
use super::Reader;
use super::Record;
use super::RecordBody;
use super::Scan;
use super::SegmentError;
use super::SegmentHeader;
use super::Skip;
use super::Source;
use super::Step;
use super::Tail;
use super::Whole;
use super::encode_header;
use super::encode_record;
use super::infallible;
use super::scan;
use crate::ids::Activation;
use crate::ids::GenId;
use crate::ids::Lineage;
use crate::ids::Position;

fn epoch(e: u64) -> Epoch {
    Epoch::try_from(e).unwrap()
}

fn header() -> impl Strategy<Value = SegmentHeader> {
    (
        (
            any::<[u8; 16]>(),
            1u64..1 << 40,
            option::of(any::<[u8; 16]>()),
            "[a-z0-9-]{0,12}",
            "/[a-z/]{0,20}",
        ),
        (
            1..=MAX_INCARNATION,
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            any::<u64>(),
            option::of((0..=MAX_PRODUCER_INTEGER, any::<[u8; 16]>())),
        ),
    )
        .prop_map(
            |(
                (lineage, first, supersedes, cluster_id, stream_path),
                (incarnation, base, acked, acked_offset, log_bytes, claim),
            )| SegmentHeader {
                lineage: Lineage(lineage),
                first_position: Position(first),
                supersedes: supersedes.map(GenId),
                cluster_id,
                stream_path,
                incarnation: Incarnation::try_from(incarnation).unwrap(),
                base: Position(base),
                acked: Position(acked),
                acked_offset: Offset::from_u64(acked_offset),
                log_bytes,
                claim: claim.map(|(e, a)| (epoch(e), Activation(a))),
            },
        )
}

fn body() -> impl Strategy<Value = RecordBody> {
    prop_oneof![
        vec(any::<u8>(), 1..48).prop_map(|frame| RecordBody::Commit { frame }),
        (0..=MAX_PRODUCER_INTEGER, any::<[u8; 16]>()).prop_map(|(e, a)| RecordBody::Claim {
            epoch: epoch(e),
            activation: Activation(a)
        }),
        (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(a, o, b)| RecordBody::Ack {
            acked: Position(a),
            acked_offset: Offset::from_u64(o),
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

    // A damaged bit anywhere in a record breaks the chain there: the prefix before it is the log,
    // and the rest is a torn tail whatever follows.
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
        incarnation: Incarnation::try_from(1 << 52).unwrap(),
        base: Position(9),
        acked: Position(9),
        acked_offset: Offset::from_u64(42),
        log_bytes: 0,
        claim: Some((epoch(3), Activation([2; 16]))),
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

// Every damaged bit of a header is refused, whatever field it lands in.
#[test]
fn every_header_bit_flip_is_refused() {
    let mut h = sample();
    h.supersedes = Some(GenId([6; 16]));
    for h in [sample(), h] {
        let (bytes, _) = encode_header(&h).unwrap();
        for bit in 0..bytes.len() * 8 {
            let mut bad = bytes.clone();
            bad[bit / 8] ^= 1 << (bit % 8);
            assert!(scan(&bad).is_err(), "bit {bit}");
        }
    }
}

#[test]
fn positions_start_at_one() {
    let mut h = sample();
    h.first_position = Position(0);
    assert!(matches!(
        encode_header(&h),
        Err(SegmentError::FirstPositionZero)
    ));
    let (mut bytes, _) = encode_header(&sample()).unwrap();
    bytes[24..32].copy_from_slice(&0u64.to_le_bytes());
    assert!(matches!(scan(&bytes), Err(SegmentError::FirstPositionZero)));
}

#[test]
fn a_record_after_the_last_position_overflows() {
    let mut h = sample();
    h.first_position = Position(u64::MAX);
    let (mut bytes, seed) = encode_header(&h).unwrap();
    let base = RecordBody::Base {
        position: Position(1),
    };
    let (first, chain) = encode_record(seed, Position(u64::MAX), &base).unwrap();
    bytes.extend(first);
    let one = scan(&bytes).unwrap();
    assert_eq!(one.records.len(), 1);
    assert_eq!(one.tail, Tail::Clean);
    bytes.extend(encode_record(chain, Position(0), &base).unwrap().0);
    assert!(matches!(
        scan(&bytes),
        Err(SegmentError::PositionOverflow {
            after: Position(u64::MAX)
        })
    ));
}

// A preallocated or zero-filled tail is a torn tail: zeros never verify as a record.
#[test]
fn a_zero_filled_tail_is_torn() {
    let (mut bytes, seed) = encode_header(&sample()).unwrap();
    bytes.extend(
        encode_record(seed, Position(10), &RecordBody::Base {
            position: Position(9),
        })
        .unwrap()
        .0,
    );
    let at = bytes.len();
    bytes.extend([0u8; 4096]);
    let s = scan(&bytes).unwrap();
    assert_eq!(s.records.len(), 1);
    assert_eq!(s.tail, Tail::Torn { at });
}

// A segment header always names its token: 0 is no token, refused once the checksum verifies.
#[test]
fn a_zero_incarnation_is_refused() {
    let mut h = sample();
    h.cluster_id = String::new();
    h.stream_path = String::new();
    let (mut bytes, _) = encode_header(&h).unwrap();
    // After the magic, version, lineage, first position, supersedes tag and two empty strings.
    let at = 4 + 4 + 16 + 8 + 1 + 2 + 2;
    bytes[at..at + 8].copy_from_slice(&0u64.to_le_bytes());
    let end = bytes.len() - 4;
    let crc = crc32c::crc32c(&bytes[..end]);
    bytes[end..].copy_from_slice(&crc.to_le_bytes());
    assert!(matches!(scan(&bytes), Err(SegmentError::Incarnation(_))));
}

/// A segment held whole, handed out at most `step` bytes a read: the reader meets it as it would
/// meet a file through a buffer of `step` bytes, records larger than that included.
struct Trickle<'a> {
    bytes: &'a [u8],
    step: usize,
}

impl Source for Trickle<'_> {
    type Error = Infallible;

    fn read_some(&mut self, out: &mut [u8]) -> Result<usize, Infallible> {
        let n = out.len().min(self.step).min(self.bytes.len());
        out[..n].copy_from_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(n)
    }
}

/// The segment as a [`Reader`] reads it from `bytes` handed out `step` bytes a read.
fn read_trickled(bytes: &[u8], step: usize) -> Result<Scan, SegmentError> {
    let mut reader = Reader::new(Trickle { bytes, step }).map_err(infallible)?;
    let mut whole = Whole::default();
    let mut records = Vec::new();
    let tail = loop {
        match reader.next(&mut whole).map_err(infallible)? {
            Step::Record(r) => records.push(r),
            Step::End(tail) => break tail,
        }
    };
    let (seed, header_len) = (reader.seed(), reader.header_len());
    Ok(Scan {
        header: reader.into_header(),
        seed,
        header_len,
        records,
        tail,
    })
}

/// Whether two reads of a segment agree: the same scan, or the same error.
fn agree(a: &Result<Scan, SegmentError>, b: &Result<Scan, SegmentError>) -> bool {
    match (a, b) {
        (Ok(a), Ok(b)) => a == b,
        (Err(a), Err(b)) => format!("{a:?}") == format!("{b:?}"),
        _ => false,
    }
}

/// What a segment holds, record after record: a valid record, a skipped position (a gap under
/// valid chains), or a chained record of any kind and payload (malformed, unless it happens to
/// have a valid shape).
#[derive(Debug, Clone)]
enum Entry {
    Body(RecordBody),
    Gap(u64),
    Raw(u8, Vec<u8>),
}

fn entry() -> impl Strategy<Value = Entry> {
    prop_oneof![
        12 => body().prop_map(Entry::Body),
        // Larger than the reader's buffer.
        1 => (any::<u8>(), BUFFER..BUFFER * 2 + 7)
            .prop_map(|(b, n)| Entry::Body(RecordBody::Commit { frame: vec![b; n] })),
        1 => (1u64..4).prop_map(Entry::Gap),
        1 => (0u8..6, vec(any::<u8>(), 0..30)).prop_map(|(kind, payload)| Entry::Raw(kind, payload)),
    ]
}

/// The segment of `h` and `entries`.
fn build(h: &SegmentHeader, entries: &[Entry]) -> Vec<u8> {
    let (mut bytes, mut chain) = encode_header(h).unwrap();
    let mut position = h.first_position.0;
    for e in entries {
        match e {
            Entry::Body(body) => {
                let (record, next) = encode_record(chain, Position(position), body).unwrap();
                bytes.extend(record);
                chain = next;
            }
            Entry::Gap(skip) => {
                position += skip;
                continue;
            }
            Entry::Raw(kind, payload) => {
                let next = super::chain(chain, *kind, Position(position), payload);
                bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                bytes.push(*kind);
                bytes.extend_from_slice(&position.to_le_bytes());
                bytes.extend_from_slice(payload);
                bytes.extend_from_slice(&next.to_le_bytes());
                chain = next;
            }
        }
        position += 1;
    }
    bytes
}

/// `bytes` as a crash or a damage may leave them: whole, cut short, with a damaged bit, or with
/// other bytes after them.
fn damage(mut bytes: Vec<u8>, how: u8, at: usize, other: &[u8]) -> Vec<u8> {
    match how {
        0 => {}
        1 => bytes.truncate(at % (bytes.len() + 1)),
        2 => {
            let bit = at % (bytes.len() * 8);
            bytes[bit / 8] ^= 1 << (bit % 8);
        }
        _ => bytes.extend_from_slice(other),
    }
    bytes
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // The streaming reader reads what a scan of the segment held whole read before it, through a
    // buffer smaller than a record or not: the same header, records and tail, or the same error,
    // torn, gapped and malformed tails included. Reading only the header reads the scan's header,
    // and fails where the scan fails but for a record after the last position, which only the
    // records show.
    #[test]
    fn the_reader_reads_what_a_whole_segment_scan_reads(
        h in header(),
        entries in vec(entry(), 0..10),
        how in 0u8..4,
        at in any::<usize>(),
        other in vec(any::<u8>(), 0..40),
        step in prop_oneof![1usize..40, 40usize..5000, Just(BUFFER)],
    ) {
        let bytes = damage(build(&h, &entries), how, at, &other);
        let before = oracle::scan(&bytes);
        let streamed = read_trickled(&bytes, step);
        prop_assert!(
            agree(&streamed, &before),
            "{:?}\n{:?}",
            streamed.as_ref().map(|s| s.tail),
            before.as_ref().map(|s| s.tail)
        );
        prop_assert!(agree(&scan(&bytes), &before));
        let header = Reader::new(Trickle { bytes: &bytes, step })
            .map(Reader::into_header)
            .map_err(infallible);
        match before {
            Err(SegmentError::PositionOverflow { .. }) => prop_assert!(matches!(header, Ok(_))),
            before => prop_assert_eq!(
                format!("{:?}", header),
                format!("{:?}", before.map(|s| s.header))
            ),
        }
    }
}

// A record larger than the reader's buffer passes through it whole, and a reader that keeps
// nothing of it reads the same records.
#[test]
fn a_record_larger_than_the_buffer_streams_through_it() {
    let frame: Vec<u8> = (0..BUFFER * 3 + 5).map(|i| (i % 251) as u8).collect();
    let bodies = [
        RecordBody::Commit {
            frame: frame.clone(),
        },
        RecordBody::Base {
            position: Position(10),
        },
    ];
    let (bytes, _, records) = segment(&sample(), &bodies);
    let mut reader = Reader::new(bytes.as_slice()).map_err(infallible).unwrap();
    let mut whole = Whole::default();
    let read = reader.next(&mut whole).map_err(infallible).unwrap();
    assert_eq!(read, Step::Record(records[0].clone()));
    // The kept frame grew to its own length, and no further.
    let Step::Record(Record {
        body: RecordBody::Commit { frame: kept },
        ..
    }) = read
    else {
        panic!("a COMMIT record");
    };
    assert_eq!(kept.capacity(), frame.len());
    let mut skip = Reader::new(bytes.as_slice()).map_err(infallible).unwrap();
    let Step::Record(first) = skip.next(&mut Skip).map_err(infallible).unwrap() else {
        panic!("a record");
    };
    assert_eq!(
        (first.position, first.chain, first.end),
        (records[0].position, records[0].chain, records[0].end)
    );
    assert_eq!(
        reader.next(&mut whole).map_err(infallible).unwrap(),
        Step::Record(records[1].clone())
    );
    // The end stays the end.
    for _ in 0..2 {
        assert_eq!(
            reader.next(&mut whole).map_err(infallible).unwrap(),
            Step::End(Tail::Clean)
        );
    }
}
