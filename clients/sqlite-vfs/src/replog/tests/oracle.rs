//! The log's open as it was before [`super::super::Log::read`]: every segment read whole and
//! scanned, the records kept, then the catch-up cut found from them. The equivalence proptest
//! holds the read and the open built on it to it.
//!
//! It scans each segment with today's [`crate::codec::segment::scan`], which streams the segment
//! through a [`crate::codec::segment::Reader`], not with the whole-segment scan the open used
//! before, so it is not independent of the reader. The segment proptest
//! (`the_reader_reads_what_a_whole_segment_scan_reads`) holds that scan to the whole-segment one,
//! and the two equivalences together hold the read to the open before it.

use std::fs::File;
use std::fs::{self};
use std::path::Path;

use super::super::Cut;
use super::super::Log;
use super::super::LogError;
use super::super::LogRecord;
use super::super::NextPosition;
use super::super::Recovered;
use super::super::SEGMENT_BYTES;
use super::super::Segment;
use super::super::carry;
use super::super::io;
use super::super::list;
use super::super::sync_dir;
use super::super::torn_header;
use crate::codec::frame::Decoded;
use crate::codec::frame::Frame;
use crate::codec::frame::decode;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentHeader;
use crate::codec::segment::Tail;
use crate::codec::segment::scan;
use crate::config::Step;
use crate::config::abort_at;
use crate::ids::Lineage;
use crate::ids::Position;

/// Whether a COMMIT record's frame names a commit of another lineage than `ours`, as `decode`
/// reads it.
fn foreign(frame: &[u8], ours: Lineage) -> bool {
    let max_body = u64::try_from(frame.len()).unwrap_or(u64::MAX);
    matches!(
        decode(frame, max_body),
        Ok(Decoded::Frame { frame: Frame::Commit { id, .. }, .. }) if id.lineage != ours
    )
}

/// The open of the log in `dir` as it was: the longest valid prefix, the rest cut (fdatasynced)
/// and the segments after the cut deleted.
pub(super) fn open(dir: &Path) -> Result<(Log, Recovered), LogError> {
    let listed = list(dir)?;
    if listed.is_empty() {
        return Err(LogError::Empty {
            dir: dir.to_owned(),
        });
    }
    let mut kept: Vec<Segment> = Vec::new();
    let mut records: Vec<LogRecord> = Vec::new();
    let mut claims = Vec::new();
    // The current segment's header, length, header length and last chain.
    let mut current: Option<(SegmentHeader, u64, u64, u32)> = None;
    let mut state: Option<SegmentHeader> = None;
    let mut next = Position(0);
    let mut cut: Option<Cut> = None;
    let mut removed = Vec::new();
    // Per record: the index of its segment in `kept`, where it starts, and the chain before it.
    let mut marks: Vec<(usize, u64, u32)> = Vec::new();
    // Per kept segment: its header and the header's length.
    let mut heads: Vec<(SegmentHeader, u64)> = Vec::new();
    let to_u64 = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    for segment in listed {
        if cut.is_some() {
            removed.push(segment);
            continue;
        }
        let bytes = fs::read(&segment.path).map_err(io("read", &segment.path))?;
        let scanned = match scan(&bytes) {
            Ok(s) => s,
            // The first segment's header is the log's identity, and any header of another
            // format or version is a refusal, never a cut.
            Err(source) if kept.is_empty() || !torn_header(&source) => {
                return Err(LogError::Segment {
                    path: segment.path,
                    source,
                });
            }
            // A later segment's header cut short or not verifying: a crash during a roll.
            Err(_torn_header) => {
                cut = Some(Cut {
                    segment: segment.path.clone(),
                    at: 0,
                });
                removed.push(segment);
                continue;
            }
        };
        if let Some(first) = &state {
            if scanned.header.lineage != first.lineage {
                return Err(LogError::Lineage {
                    path: segment.path,
                    expected: first.lineage,
                    found: scanned.header.lineage,
                });
            }
            if scanned.header.first_position != next {
                return Err(LogError::Gap {
                    path: segment.path,
                    at: 0,
                    expected: next,
                    found: scanned.header.first_position,
                });
            }
        }
        let mut carried = scanned.header.clone();
        let mut chain = scanned.seed;
        let mut end = scanned.header_len;
        next = scanned.header.first_position;
        for r in scanned.records {
            marks.push((kept.len(), to_u64(end), chain));
            carry(&mut carried, &r.body);
            if let RecordBody::Claim { epoch, activation } = r.body {
                claims.push((epoch, activation));
            }
            chain = r.chain;
            end = r.end;
            next = r.position.checked_add_one(dir)?;
            records.push(LogRecord {
                position: r.position,
                body: r.body,
            });
        }
        match scanned.tail {
            Tail::Clean => {}
            Tail::Torn { at } => {
                cut = Some(Cut {
                    segment: segment.path.clone(),
                    at: to_u64(at),
                });
            }
            Tail::Gap {
                at,
                expected,
                found,
            } => {
                return Err(LogError::Gap {
                    path: segment.path,
                    at: to_u64(at),
                    expected,
                    found,
                });
            }
            Tail::Malformed { at, kind } => {
                return Err(LogError::Malformed {
                    path: segment.path,
                    at: to_u64(at),
                    kind,
                });
            }
        }
        heads.push((scanned.header.clone(), to_u64(scanned.header_len)));
        current = Some((
            scanned.header,
            to_u64(end),
            to_u64(scanned.header_len),
            chain,
        ));
        state = Some(carried);
        kept.push(segment);
    }
    // Stream frames a catch-up logged with no ACK after them were never applied: a catch-up
    // applies its frames only once the ACK after them is fsynced, and nothing logs a record
    // between them. They are the log's last records, and go like a torn tail, so the log
    // holds no COMMIT record after its latest ACK but the generation's own pending ones. The
    // state carried to the cut point comes back with them: a later segment (an empty one a
    // re-key rolled to, say) may name another token.
    if let Some((acked, lineage)) = state.as_ref().map(|st| (st.acked, st.lineage)) {
        let unapplied = |r: &LogRecord| {
            r.position > acked
                && matches!(&r.body, RecordBody::Commit { frame } if foreign(frame, lineage))
        };
        let keep = records
            .iter()
            .rposition(|r| !unapplied(r))
            .map_or(0, |i| i.saturating_add(1));
        if let (Some(&(seg, start, before)), Some(first)) =
            (marks.get(keep), records.get(keep).map(|r| r.position))
            && let (Some((h, header_len)), Some(path)) = (
                heads.get(seg).cloned(),
                kept.get(seg).map(|k| k.path.clone()),
            )
        {
            let later = kept.split_off(seg.saturating_add(1));
            removed.splice(0..0, later);
            let mut carried = h.clone();
            for (r, _) in records
                .iter()
                .zip(&marks)
                .take(keep)
                .filter(|(_, m)| m.0 == seg)
            {
                carry(&mut carried, &r.body);
            }
            state = Some(carried);
            current = Some((h, start, header_len, before));
            next = first;
            records.truncate(keep);
            cut = Some(Cut {
                segment: path,
                at: start,
            });
        }
    }
    let (Some((header, len, header_len, chain)), Some(mut state), Some(last)) =
        (current, state, kept.last())
    else {
        return Err(LogError::Empty {
            dir: dir.to_owned(),
        });
    };
    // Checked before the cut changes anything.
    let min = state.base.min(state.acked);
    // Trim deletes only segments whose records all lie at or below `min`: a log starting later
    // lost records it still needs.
    if let Some(first) = kept.first()
        && first.first.0 > min.0.saturating_add(1)
    {
        return Err(LogError::MissingPrefix {
            path: first.path.clone(),
            first: first.first,
            needed: Position(min.0.saturating_add(1)),
        });
    }
    let file = File::options()
        .read(true)
        .write(true)
        .open(&last.path)
        .map_err(io("open", &last.path))?;
    if let Some(c) = &cut {
        crate::log::emit(crate::log::Level::Warn, "log_cut", &[
            ("segment", &c.segment.display()),
            ("at", &c.at),
            ("removed_segments", &removed.len()),
        ]);
        // From the back: the later segments go first, then the current one is truncated.
        for segment in removed.iter().rev() {
            fs::remove_file(&segment.path).map_err(io("remove", &segment.path))?;
        }
        if !removed.is_empty() {
            sync_dir(dir).map_err(io("fsync", dir))?;
        }
        abort_at(Step::CutRemoved);
        if file.metadata().map_err(io("stat", &last.path))?.len() > len {
            file.set_len(len).map_err(io("cut", &last.path))?;
            file.sync_data().map_err(io("fdatasync", &last.path))?;
        }
    }
    // The current segment's identity with the state at the end of the log.
    state.first_position = header.first_position;
    state.supersedes = header.supersedes;
    let any_commit = records
        .iter()
        .any(|r| matches!(r.body, RecordBody::Commit { .. }));
    records.retain(|r| r.position > min);
    let recovered = Recovered {
        base: state.base,
        acked: state.acked,
        acked_offset: state.acked_offset,
        log_bytes: state.log_bytes,
        claim: state.claim,
        claims,
        records,
        any_commit,
        cut,
        header,
    };
    let log = Log {
        dir: dir.to_owned(),
        segments: kept,
        file,
        len,
        header_len,
        chain,
        next,
        state,
        // Whatever was written before open may not be on disk yet.
        dirty: true,
        broken: false,
        roll_bytes: SEGMENT_BYTES,
        #[cfg(test)]
        fail_next_append: None,
        #[cfg(test)]
        fail_next_unlink: false,
        #[cfg(test)]
        fail_next_trim_fsync: false,
        #[cfg(test)]
        fail_next_roll: false,
    };
    Ok((log, recovered))
}
