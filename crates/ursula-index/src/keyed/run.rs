//! Building runs: ingest folds and compaction (design §3.4, §6.1 U16's data
//! plane, §9.6).
//!
//! [`RunBuilder`] folds a contiguous sequence of records into one sorted
//! run, applying range deletes in the batch, so rows deleted within the run
//! never reach storage. [`compact`] merges contiguous runs into one;
//! merging into the oldest run drops tombstones and rewrites only the
//! oldest run's parts that newer runs touch (part-granular).
//! [`plan_compaction`] is the size-tiered policy.

use std::collections::BTreeMap;
use std::ops::Range;

use super::batch::KeyedBatch;
use super::batch::KeyedOp;
use super::batch::parse_batch;
use super::manifest::KeyedPartMeta;
use super::manifest::KeyedRunMeta;
use super::merge::MergeScan;
use super::part::EncodedPart;
use super::part::KeyedEntry;
use super::part::PartOpener;
use super::part::PartOptions;
use super::part::RangeTombstone;
use super::part::coalesce_tombstones;
use super::part::encode_part;
use super::part::entry_weight;
use crate::IndexError;

fn invalid(message: impl Into<String>) -> IndexError {
    IndexError::InvalidKeyedState(message.into())
}

/// A run ready to store: its manifest entry and its new part files.
#[derive(Clone, Debug)]
pub struct BuiltRun {
    /// The run's manifest entry.
    pub meta: KeyedRunMeta,
    /// Parts to store before publishing (a compaction's kept parts are
    /// referenced by `meta` but not repeated here).
    pub parts: Vec<EncodedPart>,
}

/// Folds records `[start, …)` into one run.
#[derive(Debug)]
pub struct RunBuilder {
    start_record: u64,
    next_record: u64,
    drop_tombstones: bool,
    entries: BTreeMap<Vec<u8>, (u64, Option<String>)>,
    tombstones: Vec<RangeTombstone>,
}

impl RunBuilder {
    /// A builder whose first record is `start_record`. With
    /// `drop_tombstones` (no older run exists, e.g. `D = 0`), deletes leave
    /// no tombstones.
    pub fn new(start_record: u64, drop_tombstones: bool) -> Self {
        Self {
            start_record,
            next_record: start_record,
            drop_tombstones,
            entries: BTreeMap::new(),
            tombstones: Vec::new(),
        }
    }

    /// The record the builder expects next.
    pub fn next_record(&self) -> u64 {
        self.next_record
    }

    /// Whether no record was applied.
    pub fn is_empty(&self) -> bool {
        self.next_record == self.start_record
    }

    /// Estimated raw size of the folded content.
    pub fn weight(&self) -> usize {
        self.entries
            .iter()
            .map(|(key, (_, value))| {
                key.len()
                    .saturating_add(value.as_ref().map_or(0, String::len))
                    .saturating_add(16)
            })
            .fold(0_usize, usize::saturating_add)
    }

    /// Validates and applies the stored text of record `record`.
    pub fn apply_message(&mut self, record: u64, text: &str) -> Result<(), IndexError> {
        let batch = parse_batch(text).map_err(|error| IndexError::InvalidKeyedRecord {
            record,
            reason: error.to_string(),
        })?;
        self.apply(record, &batch)
    }

    /// Applies an already parsed record; records must be contiguous.
    pub fn apply(&mut self, record: u64, batch: &KeyedBatch<'_>) -> Result<(), IndexError> {
        if record != self.next_record {
            return Err(IndexError::UnexpectedRecord {
                expected: self.next_record,
                actual: record,
            });
        }
        for op in &batch.ops {
            match op {
                KeyedOp::Put { key, value } => {
                    self.entries
                        .insert(key.clone(), (record, Some(value.get().to_owned())));
                }
                KeyedOp::Delete { key } => {
                    if self.drop_tombstones {
                        self.entries.remove(key.as_slice());
                    } else {
                        self.entries.insert(key.clone(), (record, None));
                    }
                }
                KeyedOp::DeleteRange { start, end } => {
                    let mut middle = self.entries.split_off(start.as_slice());
                    let mut tail = middle.split_off(end.as_slice());
                    self.entries.append(&mut tail);
                    if !self.drop_tombstones {
                        self.tombstones.push(RangeTombstone {
                            start: start.clone(),
                            end: end.clone(),
                            record,
                        });
                    }
                }
            }
        }
        self.next_record = record
            .checked_add(1)
            .ok_or_else(|| invalid("record ordinal overflowed"))?;
        Ok(())
    }

    /// Encodes the run as parts of about `options.target_part_bytes`.
    pub fn finish(self, options: &PartOptions) -> Result<BuiltRun, IndexError> {
        if self.is_empty() {
            return Err(invalid("a run needs at least one record"));
        }
        let mut sink = PartSink::new(*options, Vec::new());
        sink.add_tombstones(self.tombstones);
        for (key, (record, value)) in self.entries {
            sink.push(KeyedEntry { key, record, value })?;
        }
        let parts = sink.finish()?;
        Ok(BuiltRun {
            meta: KeyedRunMeta {
                start_record: self.start_record,
                end_record: self.next_record,
                parts: parts.iter().map(|part| part.meta.clone()).collect(),
            },
            parts,
        })
    }
}

/// Cuts a key-ordered stream of entries plus tombstones into parts.
struct PartSink {
    options: PartOptions,
    /// Key ranges of kept parts the output must not span, ascending.
    fences: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    next_fence: Option<(Vec<u8>, Vec<u8>)>,
    entries: Vec<KeyedEntry>,
    weight: usize,
    /// Tombstones not yet assigned to a part.
    pending: Vec<RangeTombstone>,
    out: Vec<EncodedPart>,
}

impl PartSink {
    fn new(options: PartOptions, fences: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        let mut fences = fences.into_iter();
        let next_fence = fences.next();
        Self {
            options,
            fences,
            next_fence,
            entries: Vec::new(),
            weight: 0,
            pending: Vec::new(),
            out: Vec::new(),
        }
    }

    fn add_tombstones(&mut self, tombstones: Vec<RangeTombstone>) {
        self.pending.extend(tombstones);
    }

    fn push(&mut self, entry: KeyedEntry) -> Result<(), IndexError> {
        while let Some((low, high)) = self.next_fence.as_ref() {
            if entry.key.as_slice() < low.as_slice() {
                break;
            }
            if entry.key.as_slice() <= high.as_slice() {
                return Err(invalid("compaction output falls inside a kept part"));
            }
            let low = low.clone();
            self.cut(Some(&low))?;
            self.next_fence = self.fences.next();
        }
        if self.weight >= self.options.target_part_bytes && !self.entries.is_empty() {
            let boundary = entry.key.clone();
            self.cut(Some(&boundary))?;
        }
        self.weight = self.weight.saturating_add(entry_weight(&entry));
        self.entries.push(entry);
        Ok(())
    }

    /// Emits the current part with the pending tombstones below `boundary`
    /// (all of them when `None`), splitting a tombstone that spans it.
    fn cut(&mut self, boundary: Option<&[u8]>) -> Result<(), IndexError> {
        let mut assigned = Vec::new();
        let mut rest = Vec::new();
        for tombstone in std::mem::take(&mut self.pending) {
            match boundary {
                Some(boundary) if tombstone.start.as_slice() >= boundary => rest.push(tombstone),
                Some(boundary) if tombstone.end.as_slice() > boundary => {
                    rest.push(RangeTombstone {
                        start: boundary.to_vec(),
                        end: tombstone.end.clone(),
                        record: tombstone.record,
                    });
                    assigned.push(RangeTombstone {
                        end: boundary.to_vec(),
                        ..tombstone
                    });
                }
                _ => assigned.push(tombstone),
            }
        }
        self.pending = rest;
        let assigned = coalesce_tombstones(assigned);
        let entries = std::mem::take(&mut self.entries);
        self.weight = 0;
        if entries.is_empty() && assigned.is_empty() {
            return Ok(());
        }
        self.out
            .push(encode_part(&entries, &assigned, &self.options)?);
        Ok(())
    }

    fn finish(mut self) -> Result<Vec<EncodedPart>, IndexError> {
        self.cut(None)?;
        Ok(self.out)
    }
}

/// A compaction's result: the run replacing the inputs.
#[derive(Clone, Debug)]
pub struct CompactionOutput {
    /// The input runs, exactly as in the manifest the plan was made on.
    pub inputs: Vec<KeyedRunMeta>,
    /// The replacement run and the parts to store.
    pub output: BuiltRun,
    /// Whether the inputs began with the oldest run (tombstones dropped).
    pub into_oldest: bool,
}

/// Merges the contiguous runs `runs[range]` of a manifest into one run.
///
/// When the range starts at the oldest run, tombstones and shadowed rows are
/// dropped, and only the oldest run's parts whose key range meets a newer
/// input part are rewritten; the others are kept as they are.
pub async fn compact(
    opener: &dyn PartOpener,
    runs: &[KeyedRunMeta],
    range: Range<usize>,
    options: &PartOptions,
) -> Result<CompactionOutput, IndexError> {
    let inputs = runs
        .get(range.clone())
        .filter(|inputs| !inputs.is_empty())
        .ok_or_else(|| invalid("compaction range is empty or out of bounds"))?
        .to_vec();
    let into_oldest = range.start == 0;
    let (Some(first), Some(last)) = (inputs.first(), inputs.last()) else {
        return Err(invalid("compaction range is empty"));
    };
    let (start_record, end_record) = (first.start_record, last.end_record);

    let mut kept: Vec<KeyedPartMeta> = Vec::new();
    let mut sources: Vec<Vec<&KeyedPartMeta>> = Vec::new();
    if into_oldest {
        let newer: Vec<&KeyedPartMeta> = inputs
            .iter()
            .skip(1)
            .flat_map(|run| run.parts.iter())
            .collect();
        let mut rewritten = Vec::new();
        for part in &first.parts {
            if newer.iter().any(|other| part.overlaps(other)) {
                rewritten.push(part);
            } else {
                kept.push(part.clone());
            }
        }
        sources.push(rewritten);
        sources.extend(inputs.iter().skip(1).map(|run| run.parts.iter().collect()));
    } else {
        sources.extend(inputs.iter().map(|run| run.parts.iter().collect()));
    }
    let fences = kept
        .iter()
        .map(|part| (part.min_key.clone(), part.max_key.clone()))
        .collect();
    let mut sink = PartSink::new(*options, fences);
    let mut merge = MergeScan::open(opener, sources, None, None, *options, !into_oldest).await?;
    while let Some(merged) = merge.next().await? {
        sink.add_tombstones(merge.take_tombstones());
        if merged.shadowed() {
            continue;
        }
        if into_oldest && merged.winner.value.is_none() {
            continue;
        }
        sink.push(merged.winner)?;
    }
    sink.add_tombstones(merge.take_tombstones());
    let parts = sink.finish()?;
    let mut all: Vec<KeyedPartMeta> = kept;
    all.extend(parts.iter().map(|part| part.meta.clone()));
    all.sort_by(|a, b| a.min_key.cmp(&b.min_key));
    if all.windows(2).any(|pair| match pair {
        [a, b] => a.max_key > b.min_key,
        _ => false,
    }) {
        return Err(invalid("compaction produced overlapping parts"));
    }
    Ok(CompactionOutput {
        inputs,
        output: BuiltRun {
            meta: KeyedRunMeta {
                start_record,
                end_record,
                parts: all,
            },
            parts,
        },
        into_oldest,
    })
}

/// Size-tiered compaction policy (design §6.1 U16, §9.6).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionPolicy {
    /// A run joins a tier merge when it is at most `ratio` × the newer runs
    /// of the merge combined.
    pub ratio: u64,
    /// Minimum runs in a tier merge.
    pub width: usize,
    /// Merge into the oldest run once the newer runs reach this percentage
    /// of it.
    pub amp_percent: u64,
    /// Maximum runs in a manifest.
    pub max_runs: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            ratio: 2,
            width: 4,
            amp_percent: 200,
            max_runs: 8,
        }
    }
}

/// Chooses the next compaction, as a range of manifest runs (starting at 0
/// means a merge into the oldest run), or `None`.
pub fn plan_compaction(runs: &[KeyedRunMeta], policy: &CompactionPolicy) -> Option<Range<usize>> {
    let count = runs.len();
    if count < 2 {
        return None;
    }
    let oldest = runs.first().map_or(0, KeyedRunMeta::bytes);
    let newer: u64 = runs
        .iter()
        .skip(1)
        .map(KeyedRunMeta::bytes)
        .fold(0, u64::saturating_add);
    if newer.saturating_mul(100) >= oldest.saturating_mul(policy.amp_percent) {
        return Some(0..count);
    }
    // Longest suffix of newer runs where each run is at most `ratio` times
    // the runs newer than it combined.
    let mut start = count;
    let mut newer_sum = 0_u64;
    for index in (1..count).rev() {
        let bytes = runs.get(index).map_or(0, KeyedRunMeta::bytes);
        if index.saturating_add(1) < count && bytes > newer_sum.saturating_mul(policy.ratio) {
            break;
        }
        newer_sum = newer_sum.saturating_add(bytes);
        start = index;
    }
    if count.saturating_sub(start) >= policy.width.max(2) {
        return Some(start..count);
    }
    if count > policy.max_runs {
        let merged = count.saturating_sub(policy.max_runs).saturating_add(1);
        return Some(count.saturating_sub(merged).max(1)..count);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sized(bytes: &[u64]) -> Vec<KeyedRunMeta> {
        bytes
            .iter()
            .enumerate()
            .map(|(i, &size)| KeyedRunMeta {
                start_record: i as u64,
                end_record: (i as u64).saturating_add(1),
                parts: vec![KeyedPartMeta {
                    key: format!("p{i}"),
                    bytes: size,
                    data_bytes: 0,
                    tail_hash: String::new(),
                    min_key: vec![1],
                    max_key: vec![1],
                    rows: 1,
                    tombstones: 0,
                }],
            })
            .collect()
    }

    #[test]
    fn policy_merges_into_oldest_at_200_percent() {
        let policy = CompactionPolicy::default();
        assert_eq!(
            plan_compaction(&sized(&[100, 150, 50]), &policy),
            Some(0..3)
        );
        assert_eq!(plan_compaction(&sized(&[100, 150, 49]), &policy), None);
    }

    #[test]
    fn policy_tiers_similar_newer_runs() {
        let policy = CompactionPolicy::default();
        assert_eq!(
            plan_compaction(&sized(&[1000, 40, 10, 10, 10, 10]), &policy),
            Some(1..6)
        );
        assert_eq!(
            plan_compaction(&sized(&[1000, 300, 10, 10, 10]), &policy),
            None
        );
    }

    #[test]
    fn policy_caps_runs() {
        let policy = CompactionPolicy {
            width: 100,
            ..CompactionPolicy::default()
        };
        let runs = sized(&[10_000, 1000, 300, 90, 30, 9, 3, 1, 1]);
        assert_eq!(plan_compaction(&runs, &policy), Some(7..9));
    }
}
