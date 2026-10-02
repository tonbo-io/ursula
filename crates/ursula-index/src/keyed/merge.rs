//! Streaming k-way merge across runs (design §4.1, §6.1 U14).
//!
//! Every key's winner is its entry with the highest record (last writer
//! wins: a record lives in exactly one run). A range tombstone of record `t`
//! deletes entries of records `< t`. A key is visible when its winner is a
//! put that no covering tombstone deletes.
//!
//! Each run is read part by part in key order, opening a part only when the
//! scan reaches it, and registering its tombstones then. A part's key range
//! covers its tombstones, so by the time the merge decides key `k`, every
//! tombstone that can cover `k` is registered: every run's next row is at or
//! above `k`, and every part not yet opened starts above it.

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::batch::encode_key;
use super::fold::Lower;
use super::fold::RangeQuery;
use super::manifest::KeyedPartMeta;
use super::manifest::KeyedRunMeta;
use super::part::KeyedEntry;
use super::part::PartOpener;
use super::part::PartOptions;
use super::part::PartScan;
use super::part::RangeTombstone;
use super::part::open_part;
use crate::IndexError;

/// One visible row of a range read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyedRow {
    /// Key octets.
    pub key: Vec<u8>,
    /// Ordinal of the record holding the key's last applied put.
    pub record: u64,
    /// The value's stored text.
    pub value: String,
}

impl KeyedRow {
    /// Renders the row as `{"key":"<k>","record":<r>,"value":<v>}` plus LF.
    pub fn line(&self) -> String {
        format!(
            "{{\"key\":\"{}\",\"record\":{},\"value\":{}}}\n",
            encode_key(&self.key),
            self.record,
            self.value
        )
    }
}

/// One page of a merged range read.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyedPage {
    /// Rows in ascending unsigned octet order of key.
    pub rows: Vec<KeyedRow>,
    /// The last returned key, present exactly when the read stopped because
    /// of `limit` or the budget while visible rows remained in the range.
    pub after: Option<Vec<u8>>,
}

impl KeyedPage {
    /// The `application/vnd.durable-stream-keyed-rows+ndjson` body.
    pub fn body(&self) -> String {
        self.rows.iter().map(KeyedRow::line).collect()
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ByStart(RangeTombstone);

impl Ord for ByStart {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.start.cmp(&other.0.start)
    }
}

impl PartialOrd for ByStart {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ByRecord(RangeTombstone);

impl Ord for ByRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.record.cmp(&other.0.record)
    }
}

impl PartialOrd for ByRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Sweep over range tombstones for ascending query keys.
#[derive(Debug, Default)]
struct TombstoneSweep {
    pending: BinaryHeap<Reverse<ByStart>>,
    active: BinaryHeap<ByRecord>,
}

impl TombstoneSweep {
    fn add(&mut self, tombstone: RangeTombstone) {
        self.pending.push(Reverse(ByStart(tombstone)));
    }

    /// Highest record of a tombstone covering `key`. Keys must not decrease
    /// between calls.
    fn covering(&mut self, key: &[u8]) -> Option<u64> {
        while self
            .pending
            .peek()
            .is_some_and(|Reverse(ByStart(t))| t.start.as_slice() <= key)
        {
            if let Some(Reverse(ByStart(tombstone))) = self.pending.pop() {
                self.active.push(ByRecord(tombstone));
            }
        }
        while self
            .active
            .peek()
            .is_some_and(|ByRecord(t)| t.end.as_slice() <= key)
        {
            self.active.pop();
        }
        self.active.peek().map(|ByRecord(t)| t.record)
    }
}

/// A run read part by part within `[from, end)`.
struct RunCursor<'a> {
    parts: std::vec::IntoIter<&'a KeyedPartMeta>,
    scan: Option<PartScan>,
    head: Option<KeyedEntry>,
}

/// The raw k-way merge: per key, the winning entry and the highest covering
/// tombstone record.
pub(crate) struct MergeScan<'a> {
    opener: &'a dyn PartOpener,
    options: PartOptions,
    from: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    runs: Vec<RunCursor<'a>>,
    sweep: TombstoneSweep,
    /// Tombstones of every opened part, as registered (for compaction).
    opened_tombstones: Vec<RangeTombstone>,
    keep_tombstones: bool,
}

/// The decision for one key.
pub(crate) struct Merged {
    pub(crate) winner: KeyedEntry,
    /// The highest record of a tombstone covering the key.
    pub(crate) covering: Option<u64>,
}

impl Merged {
    /// Whether the winner is shadowed by a range tombstone.
    pub(crate) fn shadowed(&self) -> bool {
        self.covering.is_some_and(|t| self.winner.record < t)
    }
}

impl<'a> MergeScan<'a> {
    pub(crate) async fn open(
        opener: &'a dyn PartOpener,
        runs: impl IntoIterator<Item = Vec<&'a KeyedPartMeta>>,
        from: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        options: PartOptions,
        keep_tombstones: bool,
    ) -> Result<Self, IndexError> {
        let mut cursors = Vec::new();
        for parts in runs {
            let selected: Vec<&KeyedPartMeta> = parts
                .into_iter()
                .filter(|part| part.may_affect(from.as_deref(), end.as_deref()))
                .collect();
            cursors.push(RunCursor {
                parts: selected.into_iter(),
                scan: None,
                head: None,
            });
        }
        let mut merge = Self {
            opener,
            options,
            from,
            end,
            runs: cursors,
            sweep: TombstoneSweep::default(),
            opened_tombstones: Vec::new(),
            keep_tombstones,
        };
        for index in 0..merge.runs.len() {
            merge.advance(index).await?;
        }
        Ok(merge)
    }

    /// Opens a merge over manifest runs.
    pub(crate) async fn over_runs(
        opener: &'a dyn PartOpener,
        runs: &'a [KeyedRunMeta],
        from: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        options: PartOptions,
    ) -> Result<Self, IndexError> {
        Self::open(
            opener,
            runs.iter().map(|run| run.parts.iter().collect()),
            from,
            end,
            options,
            false,
        )
        .await
    }

    /// Moves run `index` to its next row, opening parts as needed.
    async fn advance(&mut self, index: usize) -> Result<(), IndexError> {
        let Some(cursor) = self.runs.get_mut(index) else {
            return Ok(());
        };
        cursor.head = None;
        loop {
            if let Some(scan) = cursor.scan.as_mut() {
                if let Some(entry) = scan.next().await? {
                    cursor.head = Some(entry);
                    return Ok(());
                }
                cursor.scan = None;
            }
            let Some(part) = cursor.parts.next() else {
                return Ok(());
            };
            let reader = self.opener.open(part).await?;
            let (scan, tombstones) = open_part(
                reader,
                self.from.as_deref(),
                self.end.as_deref(),
                &self.options,
            )
            .await?;
            for tombstone in tombstones {
                let relevant = self
                    .from
                    .as_deref()
                    .is_none_or(|from| tombstone.end.as_slice() > from)
                    && self
                        .end
                        .as_deref()
                        .is_none_or(|end| tombstone.start.as_slice() < end);
                if !relevant {
                    continue;
                }
                if self.keep_tombstones {
                    self.opened_tombstones.push(tombstone.clone());
                }
                self.sweep.add(tombstone);
            }
            cursor.scan = Some(scan);
        }
    }

    /// Takes the tombstones registered so far (compaction output).
    pub(crate) fn take_tombstones(&mut self) -> Vec<RangeTombstone> {
        std::mem::take(&mut self.opened_tombstones)
    }

    /// The next key's winner, or `None` when every run is exhausted.
    pub(crate) async fn next(&mut self) -> Result<Option<Merged>, IndexError> {
        let Some(key) = self
            .runs
            .iter()
            .filter_map(|run| run.head.as_ref().map(|head| head.key.as_slice()))
            .min()
            .map(<[u8]>::to_vec)
        else {
            return Ok(None);
        };
        let mut winner: Option<KeyedEntry> = None;
        for index in 0..self.runs.len() {
            let matches = self
                .runs
                .get(index)
                .and_then(|run| run.head.as_ref())
                .is_some_and(|head| head.key == key);
            if !matches {
                continue;
            }
            let entry = self.runs.get_mut(index).and_then(|run| run.head.take());
            if let Some(entry) = entry
                && winner.as_ref().is_none_or(|w| entry.record > w.record)
            {
                winner = Some(entry);
            }
            self.advance(index).await?;
        }
        let Some(winner) = winner else {
            return Ok(None);
        };
        let covering = self.sweep.covering(&key);
        Ok(Some(Merged { winner, covering }))
    }

    /// The next visible row.
    pub(crate) async fn next_visible(&mut self) -> Result<Option<KeyedRow>, IndexError> {
        while let Some(merged) = self.next().await? {
            if merged.shadowed() {
                continue;
            }
            let KeyedEntry { key, record, value } = merged.winner;
            if let Some(value) = value {
                return Ok(Some(KeyedRow { key, record, value }));
            }
        }
        Ok(None)
    }
}

/// Length of [`KeyedRow::line`] without rendering it.
fn line_len(row: &KeyedRow) -> usize {
    // {"key":"  ","record":  ,"value":  }\n
    const FIXED: usize = 8 + 11 + 9 + 2;
    let key_chars = row.key.len().saturating_mul(4).div_ceil(3);
    let digits = row.record.checked_ilog10().unwrap_or(0).saturating_add(1);
    FIXED
        .saturating_add(key_chars)
        .saturating_add(usize::try_from(digits).unwrap_or(usize::MAX))
        .saturating_add(row.value.len())
}

/// A merged range read over a manifest's runs: the same rows, `after` and
/// budget behaviour as [`super::KeyedState::range`] at the manifest's `D`.
pub async fn read_range(
    opener: &dyn PartOpener,
    runs: &[KeyedRunMeta],
    query: &RangeQuery,
    options: &PartOptions,
) -> Result<KeyedPage, IndexError> {
    let (from, skip) = match &query.lower {
        Lower::First => (None, None),
        Lower::Start(key) => (Some(key.clone()), None),
        Lower::After(key) => (Some(key.clone()), Some(key.clone())),
    };
    if let (Some(from), Some(end)) = (&from, &query.end)
        && from >= end
    {
        return Ok(KeyedPage::default());
    }
    let mut merge = MergeScan::over_runs(opener, runs, from, query.end.clone(), *options).await?;
    let mut rows: Vec<KeyedRow> = Vec::new();
    let mut used = 0_usize;
    loop {
        let Some(row) = merge.next_visible().await? else {
            return Ok(KeyedPage { rows, after: None });
        };
        if skip.as_ref().is_some_and(|skip| &row.key == skip) {
            continue;
        }
        let mut stop = rows.len() >= query.limit;
        if !stop && let Some(budget) = query.budget {
            let next = used.saturating_add(line_len(&row));
            if !rows.is_empty() && next > budget {
                stop = true;
            } else {
                used = next;
            }
        }
        if stop {
            let after = rows.last().map(|row| row.key.clone());
            return Ok(KeyedPage { rows, after });
        }
        rows.push(row);
    }
}

/// Point read: the visible row of `key`, if any.
pub async fn get(
    opener: &dyn PartOpener,
    runs: &[KeyedRunMeta],
    key: &[u8],
    options: &PartOptions,
) -> Result<Option<KeyedRow>, IndexError> {
    let mut end = key.to_vec();
    end.push(0);
    let page = read_range(
        opener,
        runs,
        &RangeQuery {
            lower: Lower::Start(key.to_vec()),
            end: Some(end),
            limit: 1,
            budget: None,
        },
        options,
    )
    .await?;
    Ok(page.rows.into_iter().next())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keyed::part::MemoryParts;
    use crate::keyed::part::encode_part;

    fn entry(key: u8, record: u64, value: Option<&str>) -> KeyedEntry {
        KeyedEntry {
            key: vec![key],
            record,
            value: value.map(str::to_owned),
        }
    }

    fn tomb(start: u8, end: u8, record: u64) -> RangeTombstone {
        RangeTombstone {
            start: vec![start],
            end: vec![end],
            record,
        }
    }

    fn run(
        parts: &mut MemoryParts,
        start: u64,
        end: u64,
        entries: &[KeyedEntry],
        tombstones: &[RangeTombstone],
    ) -> KeyedRunMeta {
        let part = encode_part(entries, tombstones, &PartOptions::default()).unwrap();
        parts.insert(&part);
        KeyedRunMeta {
            start_record: start,
            end_record: end,
            parts: vec![part.meta],
        }
    }

    #[tokio::test]
    async fn lww_tombstones_and_same_record_survivors() {
        let mut parts = MemoryParts::new();
        let old = run(
            &mut parts,
            0,
            5,
            &[
                entry(1, 0, Some("1")),
                entry(2, 1, Some("2")),
                entry(3, 2, Some("3")),
                entry(4, 3, Some("4")),
            ],
            &[],
        );
        let new = run(
            &mut parts,
            5,
            9,
            &[
                entry(1, 6, Some("\"new\"")),
                entry(2, 7, None),
                // Put in the same record as the range delete, after it.
                entry(3, 8, Some("\"kept\"")),
            ],
            &[tomb(3, 5, 8)],
        );
        let runs = [old, new];
        let page = read_range(
            &parts,
            &runs,
            &RangeQuery::default(),
            &PartOptions::default(),
        )
        .await
        .unwrap();
        let keys: Vec<_> = page
            .rows
            .iter()
            .map(|row| (row.key.clone(), row.record))
            .collect();
        assert_eq!(keys, vec![(vec![1], 6), (vec![3], 8)]);
        assert_eq!(page.after, None);
        let row = get(&parts, &runs, &[3], &PartOptions::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.value, "\"kept\"");
        assert!(
            get(&parts, &runs, &[4], &PartOptions::default())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn line_len_matches_rendering() {
        for (key, record, value) in [
            (vec![1_u8], 0_u64, "1"),
            (vec![1, 2], 9, "null"),
            (vec![1, 2, 3], 10, "{\"a\":[]}"),
            (vec![7; 100], u64::MAX, "\"x\""),
        ] {
            let row = KeyedRow {
                key,
                record,
                value: value.to_owned(),
            };
            assert_eq!(line_len(&row), row.line().len());
        }
    }
}
