use proptest::prelude::*;

use super::MARK_BLOCK_BYTES;
use super::OffsetLocation;
use super::RecordBracket;
use super::RecordIndexError;
use super::RecordMark;
use super::RecordOffset;
use super::RecordTrim;
use super::StreamRecordIndex;
use super::StreamRecordRange;
use super::canonical_json_record_ends;
use super::mark_block_end;
use super::trim_record_window;

const MIB: u64 = MARK_BLOCK_BYTES;

fn dense(index: &StreamRecordIndex) -> Vec<u64> {
    index.dense_offsets().iter().copied().collect()
}

/// Builds an index over records of the given sizes (each ends with LF) and
/// returns it with the stored bytes.
fn build(sizes: &[u64]) -> (StreamRecordIndex, Vec<u8>) {
    let mut index = StreamRecordIndex::new();
    let mut bytes = Vec::new();
    let mut ends = Vec::new();
    let mut total = 0;
    for size in sizes {
        let size = (*size).max(1);
        total += size;
        ends.push(total);
        bytes.extend(std::iter::repeat_n(
            b'x',
            usize::try_from(size - 1).unwrap(),
        ));
        bytes.push(b'\n');
    }
    if !ends.is_empty() {
        index.append_relative_ends(0, total, &ends).unwrap();
    }
    (index, bytes)
}

/// Resolves `record` the way a reader does: exact, or by counting LFs from
/// the bracket's mark without leaving the bracket.
fn resolve(index: &StreamRecordIndex, bytes: &[u8], record: u64, tail: u64) -> u64 {
    match index.offset_for(record, tail).unwrap() {
        RecordOffset::Exact(offset) => offset,
        RecordOffset::Bracket(bracket) => scan(bytes, bracket, record - bracket.from_record),
    }
}

fn scan(bytes: &[u8], bracket: RecordBracket, skip: u64) -> u64 {
    let window = &bytes
        [usize::try_from(bracket.from_offset).unwrap()..usize::try_from(bracket.limit).unwrap()];
    let mut seen = 0;
    for (index, byte) in window.iter().enumerate() {
        if *byte == b'\n' {
            seen += 1;
            if seen == skip {
                let offset = bracket.from_offset + index as u64 + 1;
                assert!(
                    offset < bracket.limit,
                    "a record start lies inside its bracket"
                );
                return offset;
            }
        }
    }
    panic!("bracket {bracket:?} does not contain record +{skip}");
}

#[test]
fn append_maps_contiguous_ordinals_to_exact_offsets() {
    let mut index = StreamRecordIndex::new();
    assert_eq!(
        index.append_relative_ends(0, 18, &[9, 18]),
        Ok(StreamRecordRange {
            first_record: 0,
            next_record: 2,
        })
    );
    assert_eq!(dense(&index), vec![0, 9]);
    assert_eq!(index.exact_offset_for(0, 18), Ok(0));
    assert_eq!(index.exact_offset_for(1, 18), Ok(9));
    assert_eq!(index.exact_offset_for(2, 18), Ok(18));
    assert_eq!(
        index.append_relative_ends(18, 9, &[9]),
        Ok(StreamRecordRange {
            first_record: 2,
            next_record: 3,
        })
    );
    assert_eq!(dense(&index), vec![0, 9, 18]);
    assert_eq!(index.exact_offset_for(3, 27), Ok(27));
}

#[test]
fn retention_drops_offsets_without_renumbering() {
    let mut index = StreamRecordIndex::new();
    index.append_relative_ends(0, 27, &[9, 18, 27]).unwrap();
    assert_eq!(index.retain_from_offset(18, 27), Ok(2));
    assert_eq!(dense(&index), vec![18]);
    assert_eq!(
        index.offset_for(1, 27),
        Err(RecordIndexError::RecordGone {
            first_record: 2,
            next_record: 3,
        })
    );
    assert_eq!(index.exact_offset_for(2, 27), Ok(18));
}

#[test]
fn restore_rejects_misaligned_or_non_monotonic_offsets() {
    assert_eq!(
        StreamRecordIndex::restore(0, vec![1, 9], 0, 18),
        Err(RecordIndexError::InvalidBoundaries)
    );
    assert_eq!(
        StreamRecordIndex::restore(0, vec![0, 0], 0, 18),
        Err(RecordIndexError::InvalidBoundaries)
    );
    assert!(StreamRecordIndex::restore(2, vec![18], 18, 27).is_ok());
}

#[test]
fn relative_ends_must_cover_the_payload_exactly() {
    let mut index = StreamRecordIndex::new();
    assert_eq!(
        index.append_relative_ends(0, 18, &[9]),
        Err(RecordIndexError::InvalidBoundaries)
    );
    assert_eq!(
        index.append_relative_ends(0, 18, &[9, 9, 18]),
        Err(RecordIndexError::InvalidBoundaries)
    );
    assert_eq!(
        index.append_relative_ends(0, 0, &[0]),
        Err(RecordIndexError::InvalidBoundaries)
    );
}

#[test]
fn canonical_json_payload_exposes_each_ndjson_boundary() {
    assert_eq!(
        canonical_json_record_ends("application/json; charset=utf-8", b"{\"a\":1}\n{\"b\":2}\n"),
        Ok(vec![8, 16])
    );
    assert_eq!(
        canonical_json_record_ends("application/octet-stream", b"x"),
        Ok(vec![])
    );
    assert_eq!(
        canonical_json_record_ends("application/json", b"{}"),
        Err(RecordIndexError::InvalidBoundaries)
    );
}

#[test]
fn sealing_emits_one_mark_per_block_with_a_record_start() {
    // 3 MiB of 256 KiB records: four starts per block.
    let sizes = vec![MIB / 4; 12];
    let (mut index, _) = build(&sizes);
    let tail = 3 * MIB;
    assert_eq!(index.seal_below(tail, tail, u64::MAX), 12);
    assert_eq!(index.marks(), &[
        RecordMark {
            record: 0,
            offset: 0
        },
        RecordMark {
            record: 4,
            offset: MIB
        },
        RecordMark {
            record: 8,
            offset: 2 * MIB
        },
    ]);
    assert_eq!(index.dense_len(), 0);
    assert_eq!(index.dense_first_record(), 12);
    index.validate(0, tail).unwrap();
    assert_eq!(index.offset_for(4, tail), Ok(RecordOffset::Exact(MIB)));
    assert_eq!(
        index.offset_for(5, tail),
        Ok(RecordOffset::Bracket(RecordBracket {
            from_record: 4,
            from_offset: MIB,
            limit: 2 * MIB,
        }))
    );
    // The last bracket ends at the tail.
    assert_eq!(
        index.offset_for(11, tail),
        Ok(RecordOffset::Bracket(RecordBracket {
            from_record: 8,
            from_offset: 2 * MIB,
            limit: tail,
        }))
    );
    assert_eq!(index.offset_for(12, tail), Ok(RecordOffset::Exact(tail)));
}

#[test]
fn records_larger_than_a_block_get_their_own_mark() {
    let sizes = [100, 3 * MIB, 100, 100];
    let (mut index, bytes) = build(&sizes);
    let tail = bytes.len() as u64;
    assert_eq!(index.seal_below(tail, tail, u64::MAX), 4);
    // Record 1 starts in block 0 with record 0; record 2 starts in block 3.
    assert_eq!(index.marks().len(), 2);
    assert_eq!(index.marks()[1].record, 2);
    for record in 0..4 {
        let oracle = sizes[..record].iter().sum::<u64>();
        assert_eq!(resolve(&index, &bytes, record as u64, tail), oracle);
    }
    // An offset inside the big record past its block is provably not a
    // boundary.
    assert_eq!(
        index.locate_offset(2 * MIB, tail),
        Ok(OffsetLocation::NotBoundary)
    );
}

#[test]
fn a_record_straddling_the_seal_point_stays_dense() {
    let (mut index, _) = build(&[10, 10, 10]);
    // Seal point in the middle of record 1: only record 0 seals.
    assert_eq!(index.seal_below(15, 30, u64::MAX), 1);
    assert_eq!(index.dense_first_record(), 1);
    assert_eq!(dense(&index), vec![10, 20]);
    // At record 1's end it seals too; the last record needs the tail.
    assert_eq!(index.seal_below(20, 30, u64::MAX), 1);
    assert_eq!(index.seal_below(29, 30, u64::MAX), 0);
    assert_eq!(index.sealable_records(29, 30), 0);
    assert_eq!(index.sealable_records(30, 30), 1);
    index.validate(0, 30).unwrap();
}

#[test]
fn sealing_respects_the_per_call_budget() {
    let (mut index, _) = build(&[2; 10]);
    assert_eq!(index.seal_below(20, 20, 3), 3);
    assert_eq!(index.seal_below(20, 20, 3), 3);
    assert_eq!(index.seal_below(20, 20, 100), 4);
    assert_eq!(index.marks().len(), 1);
    assert_eq!(index.dense_len(), 0);
    assert_eq!(index.range().unwrap().next_record, 10);
    index.validate(0, 20).unwrap();
}

#[test]
fn appends_after_sealing_keep_exact_offsets_and_rollback_only_dense() {
    let (mut index, _) = build(&[10, 10]);
    index.seal_below(20, 20, u64::MAX);
    let checkpoint = index.append_checkpoint();
    assert_eq!(checkpoint, 2);
    index.append_relative_ends(20, 10, &[5, 10]).unwrap();
    assert_eq!(index.offset_for(3, 30), Ok(RecordOffset::Exact(25)));
    index.rollback_appends(checkpoint);
    assert_eq!(index.range().unwrap().next_record, 2);
    assert_eq!(index.marks().len(), 1);
    // An append must start beyond the last anchor.
    assert_eq!(
        index.prepare_append(0, 10, &[10]).map(|p| p.range()),
        Err(RecordIndexError::InvalidBoundaries)
    );
}

#[test]
fn retention_into_sealed_history_lands_on_the_mark_at_or_below() {
    let sizes = vec![MIB / 4; 12];
    let (mut index, _) = build(&sizes);
    let tail = 3 * MIB;
    index.seal_below(tail, tail, u64::MAX);
    // Record 6 (offset 1.5 MiB) is sealed: retention lands on mark 4.
    let prepared = index.prepare_retain(6 * MIB / 4, tail).unwrap();
    assert_eq!(prepared.effective_offset(), MIB);
    assert_eq!(index.commit_retain(prepared), 4);
    assert_eq!(index.marks()[0], RecordMark {
        record: 4,
        offset: MIB
    });
    index.validate(MIB, tail).unwrap();
    assert!(index.marks_capacity() <= 2 * index.marks().len() + 64);
    // A mark retains exactly; ordinals never change.
    assert_eq!(index.retain_from_offset(2 * MIB, tail), Ok(8));
    assert_eq!(index.offset_for(9, tail).unwrap().upper_bound(), tail);
    index.validate(2 * MIB, tail).unwrap();
    // Retention to the tail empties both parts.
    assert_eq!(index.retain_from_offset(tail, tail), Ok(12));
    assert!(index.marks().is_empty());
    index.validate(tail, tail).unwrap();
}

#[test]
fn retention_rejects_provable_non_boundaries() {
    let (mut index, _) = build(&[100, 3 * MIB, 100]);
    let tail = 100 + 3 * MIB + 100;
    index.seal_below(tail, tail, u64::MAX);
    assert_eq!(
        index
            .prepare_retain(2 * MIB, tail)
            .map(|p| p.effective_offset()),
        Err(RecordIndexError::OffsetNotRecordBoundary)
    );
    // Dense targets keep the exact check.
    let (index, _) = build(&[10, 10]);
    assert_eq!(
        index.prepare_retain(5, 20).map(|p| p.effective_offset()),
        Err(RecordIndexError::OffsetNotRecordBoundary)
    );
}

#[test]
fn dense_retention_drops_every_mark() {
    let (mut index, _) = build(&[10, 10, 10, 10]);
    index.seal_below(20, 40, u64::MAX);
    assert_eq!(index.retain_from_offset(30, 40), Ok(3));
    assert!(index.marks().is_empty());
    assert_eq!(index.dense_first_record(), 3);
    index.validate(30, 40).unwrap();
}

#[test]
fn validate_checks_the_mark_invariants() {
    let marks = |list: &[(u64, u64)]| {
        list.iter()
            .map(|(record, offset)| RecordMark {
                record: *record,
                offset: *offset,
            })
            .collect::<Vec<_>>()
    };
    let tail = 3 * MIB;
    assert!(
        StreamRecordIndex::restore_sparse(0, marks(&[(0, 0), (4, MIB)]), 8, vec![2 * MIB], 0, tail)
            .is_ok()
    );
    // M2: the first mark is the first retained record at the retained offset.
    assert!(
        StreamRecordIndex::restore_sparse(1, marks(&[(0, 0)]), 8, vec![2 * MIB], 0, tail).is_err()
    );
    // M3: two marks in one block.
    assert!(
        StreamRecordIndex::restore_sparse(0, marks(&[(0, 0), (4, 10)]), 8, vec![2 * MIB], 0, tail)
            .is_err()
    );
    // M3: the last mark must be below the dense part.
    assert!(
        StreamRecordIndex::restore_sparse(0, marks(&[(0, 0), (8, MIB)]), 8, vec![2 * MIB], 0, tail)
            .is_err()
    );
    // M5: the dense part starts above the last mark.
    assert!(
        StreamRecordIndex::restore_sparse(0, marks(&[(0, 0), (4, MIB)]), 8, vec![MIB], 0, tail)
            .is_err()
    );
}

#[test]
fn serde_keeps_legacy_payloads_dense_and_round_trips_marks() {
    let legacy: StreamRecordIndex =
        serde_json::from_str(r#"{"first_record":2,"record_offsets":[18,20]}"#).unwrap();
    assert_eq!(legacy.dense_first_record(), 2);
    assert!(legacy.marks().is_empty());
    legacy.validate(18, 27).unwrap();

    let (mut index, _) = build(&[MIB / 2; 6]);
    index.seal_below(2 * MIB, 3 * MIB, u64::MAX);
    let json = serde_json::to_string(&index).unwrap();
    let decoded: StreamRecordIndex = serde_json::from_str(&json).unwrap();
    assert_eq!(decoded, index);
    decoded.validate(0, 3 * MIB).unwrap();
}

#[test]
fn anchors_between_lists_marks_and_the_first_dense_offset() {
    let (mut index, _) = build(&[MIB / 2; 6]);
    index.seal_below(2 * MIB, 3 * MIB, u64::MAX);
    let anchors = index.anchors_between(0, 3 * MIB);
    assert_eq!(anchors, vec![
        RecordMark {
            record: 2,
            offset: MIB
        },
        RecordMark {
            record: 4,
            offset: 2 * MIB
        },
    ]);
}

fn trim(window_record: u64, skip: u64, take: u64, max_bytes: u64) -> RecordTrim {
    RecordTrim {
        window_record,
        leading_lf: false,
        skip,
        take,
        max_bytes,
        anchors: Vec::new(),
        tail_offset: u64::MAX,
        claim_up_to_date: true,
    }
}

#[test]
fn trim_skips_takes_and_cuts_at_the_byte_budget() {
    let window = b"a\nbb\nccc\ndddd\npartial";
    let out = trim_record_window(window, 100, &trim(7, 1, 2, u64::MAX)).unwrap();
    assert_eq!(&window[out.start..out.end], b"bb\nccc\n");
    assert_eq!((out.offset, out.next_offset), (102, 109));
    assert_eq!(out.record_range, StreamRecordRange {
        first_record: 8,
        next_record: 10,
    });
    // A budget keeps at least one record and cuts at a record boundary.
    let out = trim_record_window(window, 100, &trim(7, 1, 3, 4)).unwrap();
    assert_eq!(&window[out.start..out.end], b"bb\n");
    let out = trim_record_window(window, 100, &trim(7, 2, 3, 1)).unwrap();
    assert_eq!(&window[out.start..out.end], b"ccc\n");
    // The unterminated remainder is never returned.
    let out = trim_record_window(window, 100, &trim(7, 3, 9, u64::MAX)).unwrap();
    assert_eq!(&window[out.start..out.end], b"dddd\n");
}

#[test]
fn trim_reports_up_to_date_only_when_allowed_at_the_tail() {
    let window = b"a\nb\n";
    let mut spec = trim(0, 0, 9, u64::MAX);
    spec.tail_offset = 4;
    assert!(trim_record_window(window, 0, &spec).unwrap().up_to_date);
    spec.claim_up_to_date = false;
    assert!(!trim_record_window(window, 0, &spec).unwrap().up_to_date);
}

#[test]
fn trim_fails_on_anchors_that_disagree_with_the_bytes() {
    // RC-21: the leading byte must be LF.
    let mut spec = trim(5, 0, 1, u64::MAX);
    spec.leading_lf = true;
    assert!(trim_record_window(b"x{}\n", 10, &spec).is_err());
    assert_eq!(trim_record_window(b"\n{}\n", 10, &spec).unwrap().offset, 11);
    // An anchor whose LF count disagrees with its record.
    let mut spec = trim(0, 0, 9, u64::MAX);
    spec.anchors = vec![RecordMark {
        record: 3,
        offset: 4,
    }];
    assert!(trim_record_window(b"a\nb\nc\n", 0, &spec).is_err());
    spec.anchors = vec![RecordMark {
        record: 2,
        offset: 4,
    }];
    assert!(trim_record_window(b"a\nb\nc\n", 0, &spec).is_ok());
    // An anchor not preceded by LF.
    spec.anchors = vec![RecordMark {
        record: 2,
        offset: 5,
    }];
    assert!(trim_record_window(b"a\nb\nc\n", 0, &spec).is_err());
    // A window too short for its skip.
    assert!(trim_record_window(b"a\n", 0, &trim(0, 3, 1, u64::MAX)).is_err());
}

#[test]
fn mark_block_end_is_the_next_block_start() {
    assert_eq!(mark_block_end(0), MIB);
    assert_eq!(mark_block_end(MIB - 1), MIB);
    assert_eq!(mark_block_end(MIB), 2 * MIB);
}

#[derive(Debug, Clone)]
enum Op {
    Append(Vec<u64>),
    Seal { at: u64, budget: u64 },
    Retain { at: u64 },
}

fn record_size() -> impl Strategy<Value = u64> {
    prop_oneof![
        8 => 2_u64..300,
        2 => 300_u64..200_000,
        1 => (MIB / 2)..(3 * MIB),
    ]
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => prop::collection::vec(record_size(), 1..40).prop_map(Op::Append),
        3 => (0_u64..1_000, prop_oneof![Just(u64::MAX), 1_u64..20])
            .prop_map(|(at, budget)| Op::Seal { at, budget }),
        1 => (0_u64..1_000).prop_map(|at| Op::Retain { at }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// RC-2 at the index level: after any sequence of appends, seals at
    /// arbitrary (including mid-record) points and retentions, every
    /// retained record resolves to the dense oracle's offset within one
    /// block, every boundary maps back to its ordinal, every other offset is
    /// rejected, and retention lands on the oracle's boundary at or below
    /// its target.
    #[test]
    fn sparse_index_matches_the_dense_oracle(ops in prop::collection::vec(op(), 1..25)) {
        let mut index = StreamRecordIndex::new();
        let mut bytes = Vec::<u8>::new();
        // Oracle: start offset of every record ever appended.
        let mut starts = Vec::<u64>::new();
        let mut first_record = 0_u64;
        let mut retained = 0_u64;
        for op in ops {
            let tail = bytes.len() as u64;
            match op {
                Op::Append(sizes) => {
                    let mut ends = Vec::new();
                    let mut total = 0;
                    for size in sizes {
                        starts.push(tail + total);
                        total += size;
                        ends.push(total);
                        bytes.extend(std::iter::repeat_n(b'x', usize::try_from(size - 1).unwrap()));
                        bytes.push(b'\n');
                    }
                    index.append_relative_ends(tail, total, &ends).unwrap();
                }
                Op::Seal { at, budget } => {
                    let point = retained + (tail - retained) * at / 999;
                    let before = index.range().unwrap();
                    index.seal_below(point, tail, budget);
                    prop_assert_eq!(index.range().unwrap(), before);
                }
                Op::Retain { at } => {
                    if starts.len() as u64 == first_record {
                        continue;
                    }
                    let target_record = first_record
                        + (starts.len() as u64 - first_record) * at / 1_000;
                    let target = starts[usize::try_from(target_record).unwrap()];
                    let prepared = index.prepare_retain(target, tail).unwrap();
                    let effective = prepared.effective_offset();
                    prop_assert!(effective <= target);
                    let new_first = index.commit_retain(prepared);
                    prop_assert!(new_first <= target_record);
                    prop_assert_eq!(starts[usize::try_from(new_first).unwrap()], effective);
                    first_record = new_first;
                    retained = effective;
                }
            }
            let tail = bytes.len() as u64;
            index.validate(retained, tail).unwrap();
            let range = index.range().unwrap();
            prop_assert_eq!(range.first_record, first_record);
            prop_assert_eq!(range.next_record, starts.len() as u64);
            for record in first_record..range.next_record {
                let offset = resolve(&index, &bytes, record, tail);
                prop_assert_eq!(offset, starts[usize::try_from(record).unwrap()]);
                if let Ok(RecordOffset::Bracket(bracket)) = index.offset_for(record, tail) {
                    // RC-3: the scan stays inside one block.
                    prop_assert!(bracket.limit <= mark_block_end(bracket.from_offset));
                }
            }
            for (ordinal, start) in starts.iter().enumerate().skip(usize::try_from(first_record).unwrap()) {
                let located = index.locate_offset(*start, tail).unwrap();
                let ordinal = ordinal as u64;
                match located {
                    OffsetLocation::Exact(record) => prop_assert_eq!(record, ordinal),
                    OffsetLocation::Bracket(bracket) => {
                        let window = &bytes[usize::try_from(bracket.from_offset).unwrap()
                            ..usize::try_from(*start).unwrap()];
                        let lfs = window.iter().filter(|b| **b == b'\n').count() as u64;
                        prop_assert_eq!(bytes[usize::try_from(*start - 1).unwrap()], b'\n');
                        prop_assert_eq!(bracket.from_record + lfs, ordinal);
                    }
                    OffsetLocation::NotBoundary => prop_assert!(false, "boundary rejected"),
                }
                // The byte after a start is never a boundary unless the
                // record is one byte long, which records here never are.
                let inside = *start + 1;
                if inside < tail {
                    match index.locate_offset(inside, tail).unwrap() {
                        OffsetLocation::Exact(_) => prop_assert!(false, "non-boundary accepted"),
                        OffsetLocation::Bracket(_) => {
                            prop_assert_ne!(bytes[usize::try_from(inside - 1).unwrap()], b'\n');
                        }
                        OffsetLocation::NotBoundary => {}
                    }
                }
            }
            prop_assert_eq!(
                index.locate_offset(tail, tail).unwrap(),
                OffsetLocation::Exact(starts.len() as u64)
            );
        }
    }
}
