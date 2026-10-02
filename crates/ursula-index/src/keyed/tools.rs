//! Keyed namespace maintenance (design §6.1 U20, §5.5, §9.4): the library
//! half of `ursula indexer keyed {verify,rebuild,sweep,dump}`.
//!
//! - [`verify`]: rebuilds `state(D)` from the source log at the published
//!   `D` and compares it with the namespace row by row (optionally on a
//!   deterministic sample of keys), plus the continuity digest of record
//!   `D − 1`.
//! - [`rebuild`]: refolds the namespace from record 0 through followers,
//!   rate-limited and parallel by record range, and swaps the result in
//!   blue/green: the old manifest keeps being served until the rebuilt one,
//!   caught up to at least the old `D`, replaces it with one CAS of
//!   `CURRENT`. Served `D` never decreases.
//! - [`sweep`]: one LIST of the namespace; deletes part and manifest objects
//!   older than the GC grace that no manifest a reader may still hold
//!   references (crash orphans, §9.4).
//! - [`dump`]: the published pointer and manifest, optionally every row.

use std::collections::HashSet;
use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use anyhow::bail;
use serde::Serialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use super::engine::stored_digest;
use super::fold::Lower;
use super::fold::RangeQuery;
use super::manifest::KEYED_CURRENT_KEY;
use super::manifest::KEYED_MANIFEST_VERSION;
use super::manifest::KEYED_PROJECTION_FORMAT;
use super::manifest::KeyedManifest;
use super::manifest::KeyedNamespace;
use super::manifest::KeyedRunMeta;
use super::manifest::KeyedSource;
use super::manifest::PublishOutcome;
use super::manifest::PublishedKeyedManifest;
use super::merge::KeyedRow;
use super::merge::read_range;
use super::part::MemoryParts;
use super::part::PartOpener;
use super::part::PartOptions;
use super::run::BuiltRun;
use super::run::RunBuilder;
use super::source::KeyedSourceClient;
use crate::clock::Clock;
use crate::clock::SystemClock;
use crate::object_store::ObjectStore;

/// Rows fetched per page when scanning a state.
const SCAN_PAGE_ROWS: usize = 1000;
/// Mismatches described in a [`VerifyReport`].
const REPORTED_MISMATCHES: usize = 16;
/// Upper bound of [`RebuildOptions::parallelism`]: each range becomes a run,
/// and a manifest holds at most this many before compaction.
pub const MAX_REBUILD_PARALLELISM: usize = 8;

/// Source reads shared by verify and rebuild.
#[derive(Clone, Debug)]
pub struct SourceReadOptions {
    /// `max_bytes` of one source page (P7).
    pub page_bytes: u64,
    /// Source bytes per second across all ranges; `None` is unlimited.
    pub max_bytes_per_second: Option<u64>,
    /// Part encoding and read knobs.
    pub part_options: PartOptions,
}

impl Default for SourceReadOptions {
    fn default() -> Self {
        Self {
            page_bytes: 16 * 1024 * 1024,
            max_bytes_per_second: None,
            part_options: PartOptions::default(),
        }
    }
}

/// Spaces source reads to a byte rate shared by every range.
struct RateLimiter {
    rate: Option<u64>,
    state: Mutex<(Instant, u64)>,
}

impl RateLimiter {
    fn new(rate: Option<u64>) -> Self {
        Self {
            rate: rate.filter(|rate| *rate > 0),
            state: Mutex::new((Instant::now(), 0)),
        }
    }

    async fn consume(&self, bytes: u64) {
        let Some(rate) = self.rate else {
            return;
        };
        let due = {
            let mut state = self.state.lock().await;
            state.1 = state.1.saturating_add(bytes);
            let seconds = state.1 as f64 / rate as f64;
            state
                .0
                .checked_add(Duration::from_secs_f64(seconds))
                .unwrap_or_else(Instant::now)
        };
        tokio::time::sleep_until(due).await;
    }
}

/// One folded record range.
struct FoldedRange {
    builder: RunBuilder,
    /// Continuity digest of the range's last record.
    last_digest: Option<String>,
    bytes: u64,
}

/// Folds source records `[start, end)` through the default (follower) view.
async fn fold_range(
    client: KeyedSourceClient,
    source: KeyedSource,
    range: (u64, u64),
    drop_tombstones: bool,
    options: SourceReadOptions,
    limiter: Arc<RateLimiter>,
) -> anyhow::Result<FoldedRange> {
    let (start, end) = range;
    let mut builder = RunBuilder::new(start, drop_tombstones);
    let mut cursor = start;
    let mut last_digest = None;
    let mut bytes = 0_u64;
    while cursor < end {
        let page = client
            .read(
                &source.bucket,
                &source.key,
                cursor,
                options.page_bytes,
                Some(end.saturating_sub(cursor)),
                false,
            )
            .await
            .with_context(|| format!("read source record {cursor}"))?;
        if page.records.is_empty() {
            bail!("the source ends at record {cursor}, before {end}");
        }
        let mut page_bytes = 0_u64;
        for (record, text) in (page.start_record..).zip(&page.records) {
            if record >= end {
                break;
            }
            builder
                .apply_message(record, text)
                .with_context(|| format!("apply source record {record}"))?;
            last_digest = Some(stored_digest(text));
            page_bytes = page_bytes
                .saturating_add(u64::try_from(text.len()).unwrap_or(u64::MAX))
                .saturating_add(1);
        }
        bytes = bytes.saturating_add(page_bytes);
        limiter.consume(page_bytes).await;
        cursor = page.next_record.min(end);
    }
    Ok(FoldedRange {
        builder,
        last_digest,
        bytes,
    })
}

async fn finish(builder: RunBuilder, options: PartOptions) -> anyhow::Result<Option<BuiltRun>> {
    if builder.is_empty() {
        return Ok(None);
    }
    let built = tokio::task::spawn_blocking(move || builder.finish(&options))
        .await
        .context("join part encoder")??;
    Ok(Some(built))
}

/// Pages through every visible row of `runs`.
struct RowCursor<'a> {
    opener: &'a dyn PartOpener,
    runs: &'a [KeyedRunMeta],
    options: &'a PartOptions,
    after: Option<Vec<u8>>,
    buffer: VecDeque<KeyedRow>,
    done: bool,
}

impl<'a> RowCursor<'a> {
    fn new(opener: &'a dyn PartOpener, runs: &'a [KeyedRunMeta], options: &'a PartOptions) -> Self {
        Self {
            opener,
            runs,
            options,
            after: None,
            buffer: VecDeque::new(),
            done: false,
        }
    }

    async fn next(&mut self) -> anyhow::Result<Option<KeyedRow>> {
        loop {
            if let Some(row) = self.buffer.pop_front() {
                return Ok(Some(row));
            }
            if self.done {
                return Ok(None);
            }
            let query = RangeQuery {
                lower: self.after.take().map_or(Lower::First, Lower::After),
                end: None,
                limit: SCAN_PAGE_ROWS,
                budget: None,
            };
            let page = read_range(self.opener, self.runs, &query, self.options).await?;
            self.done = page.after.is_none();
            self.after = page.after;
            self.buffer.extend(page.rows);
        }
    }

    /// The next row whose key is in the sample.
    async fn next_sampled(&mut self, modulus: u64) -> anyhow::Result<Option<KeyedRow>> {
        while let Some(row) = self.next().await? {
            if sampled(&row.key, modulus) {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }
}

/// Whether `key` is in the deterministic sample `1/modulus`.
fn sampled(key: &[u8], modulus: u64) -> bool {
    if modulus <= 1 {
        return true;
    }
    let hash = blake3::hash(key);
    let mut prefix = [0_u8; 8];
    prefix.copy_from_slice(hash.as_bytes().get(..8).unwrap_or(&[0; 8]));
    u64::from_le_bytes(prefix).checked_rem(modulus) == Some(0)
}

/// Options of [`verify`].
#[derive(Clone, Debug)]
pub struct VerifyOptions {
    /// Compare only keys whose hash is divisible by this (1 compares all).
    pub sample_modulus: u64,
    /// Source reads.
    pub read: SourceReadOptions,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            sample_modulus: 1,
            read: SourceReadOptions::default(),
        }
    }
}

/// Result of [`verify`].
#[derive(Clone, Debug, Default, Serialize)]
pub struct VerifyReport {
    /// The `D` compared at (the published `D` when verification started).
    pub through_record: u64,
    /// Generation of the manifest compared.
    pub generation: u64,
    /// Rows compared equal.
    pub matched_rows: u64,
    /// Differences found (continuity included).
    pub mismatch_count: u64,
    /// The first differences, described.
    pub mismatches: Vec<String>,
    /// Source bytes read.
    pub source_bytes: u64,
}

impl VerifyReport {
    /// Whether the namespace matches the log.
    pub fn is_ok(&self) -> bool {
        self.mismatch_count == 0
    }

    fn mismatch(&mut self, description: String) {
        self.mismatch_count = self.mismatch_count.saturating_add(1);
        if self.mismatches.len() < REPORTED_MISMATCHES {
            self.mismatches.push(description);
        }
    }
}

fn describe(row: &KeyedRow) -> String {
    row.line().trim_end().to_owned()
}

/// Rebuilds `state(D)` from the source at the namespace's published `D` and
/// compares it with the namespace row by row.
pub async fn verify(
    store: ObjectStore,
    client: &KeyedSourceClient,
    source: &KeyedSource,
    options: &VerifyOptions,
) -> anyhow::Result<VerifyReport> {
    let namespace = KeyedNamespace::new(store, source.clone());
    let Some(published) = namespace.load().await.context("load CURRENT")? else {
        // A missing namespace is state(0), which every log agrees with.
        return Ok(VerifyReport::default());
    };
    let manifest = &published.manifest;
    let mut report = VerifyReport {
        through_record: manifest.through_record,
        generation: manifest.generation,
        ..VerifyReport::default()
    };
    let limiter = Arc::new(RateLimiter::new(options.read.max_bytes_per_second));
    let folded = fold_range(
        client.clone(),
        source.clone(),
        (0, manifest.through_record),
        true,
        options.read.clone(),
        limiter,
    )
    .await?;
    report.source_bytes = folded.bytes;
    if folded.last_digest != manifest.through_digest {
        report.mismatch(format!(
            "continuity: source record {} differs from the record the namespace was built from",
            manifest.through_record.saturating_sub(1)
        ));
    }
    let mut parts = MemoryParts::new();
    let mut runs = Vec::new();
    if let Some(built) = finish(folded.builder, options.read.part_options).await? {
        for part in &built.parts {
            parts.insert(part);
        }
        runs.push(built.meta);
    }
    let opener = namespace.opener();
    let part_options = &options.read.part_options;
    let mut actual = RowCursor::new(&opener, &manifest.runs, part_options);
    let mut expected = RowCursor::new(&parts, &runs, part_options);
    let modulus = options.sample_modulus;
    let mut left = actual.next_sampled(modulus).await?;
    let mut right = expected.next_sampled(modulus).await?;
    loop {
        match (left.take(), right.take()) {
            (None, None) => break,
            (Some(row), None) => {
                report.mismatch(format!("extra row {}", describe(&row)));
                left = actual.next_sampled(modulus).await?;
            }
            (None, Some(row)) => {
                report.mismatch(format!("missing row {}", describe(&row)));
                right = expected.next_sampled(modulus).await?;
            }
            (Some(found), Some(wanted)) => match found.key.cmp(&wanted.key) {
                std::cmp::Ordering::Less => {
                    report.mismatch(format!("extra row {}", describe(&found)));
                    right = Some(wanted);
                    left = actual.next_sampled(modulus).await?;
                }
                std::cmp::Ordering::Greater => {
                    report.mismatch(format!("missing row {}", describe(&wanted)));
                    left = Some(found);
                    right = expected.next_sampled(modulus).await?;
                }
                std::cmp::Ordering::Equal => {
                    if found == wanted {
                        report.matched_rows = report.matched_rows.saturating_add(1);
                    } else {
                        report.mismatch(format!(
                            "row {} should be {}",
                            describe(&found),
                            describe(&wanted)
                        ));
                    }
                    left = actual.next_sampled(modulus).await?;
                    right = expected.next_sampled(modulus).await?;
                }
            },
        }
    }
    Ok(report)
}

/// Options of [`rebuild`].
#[derive(Clone, Debug)]
pub struct RebuildOptions {
    /// Record ranges folded concurrently (1..=[`MAX_REBUILD_PARALLELISM`]).
    pub parallelism: usize,
    /// Publication attempts before giving up on a namespace that keeps
    /// advancing.
    pub max_attempts: usize,
    /// Source reads.
    pub read: SourceReadOptions,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            parallelism: 4,
            max_attempts: 8,
            read: SourceReadOptions::default(),
        }
    }
}

/// Result of [`rebuild`].
#[derive(Clone, Debug, Default, Serialize)]
pub struct RebuildReport {
    /// `D` served before the swap.
    pub previous_through: u64,
    /// Generation replaced.
    pub previous_generation: u64,
    /// `D` of the rebuilt manifest (at least `previous_through`).
    pub through_record: u64,
    /// Generation of the rebuilt manifest.
    pub generation: u64,
    /// Runs of the rebuilt manifest.
    pub runs: usize,
    /// Parts written.
    pub parts: usize,
    /// Source bytes read.
    pub source_bytes: u64,
    /// Objects of lost publication attempts; `sweep` reclaims them after
    /// the GC grace period.
    pub orphans: Vec<String>,
}

/// Splits `[0, end)` into at most `count` contiguous ranges.
fn split(end: u64, count: usize) -> Vec<(u64, u64)> {
    let count = u64::try_from(count.max(1)).unwrap_or(1).min(end.max(1));
    let width = end.div_ceil(count).max(1);
    let mut ranges = Vec::new();
    let mut start = 0;
    while start < end {
        let stop = start.saturating_add(width).min(end);
        ranges.push((start, stop));
        start = stop;
    }
    ranges
}

/// Refolds the namespace from record 0 and swaps it in blue/green.
pub async fn rebuild(
    store: ObjectStore,
    client: &KeyedSourceClient,
    source: &KeyedSource,
    options: &RebuildOptions,
) -> anyhow::Result<RebuildReport> {
    if !(1..=MAX_REBUILD_PARALLELISM).contains(&options.parallelism) {
        bail!("parallelism must be in 1..={MAX_REBUILD_PARALLELISM}");
    }
    let namespace = KeyedNamespace::new(store, source.clone());
    let mut base = namespace
        .load()
        .await
        .context("load CURRENT")?
        .context("the namespace has no published state; a keyed-state read builds it")?;
    let target = base.manifest.through_record;
    if target == 0 {
        bail!("the namespace is at D = 0; there is nothing to rebuild");
    }
    let mut report = RebuildReport {
        previous_through: target,
        previous_generation: base.manifest.generation,
        ..RebuildReport::default()
    };
    let limiter = Arc::new(RateLimiter::new(options.read.max_bytes_per_second));
    let ranges = split(target, options.parallelism);
    let tasks: Vec<_> = ranges
        .iter()
        .map(|(start, end)| {
            tokio::spawn(fold_range(
                client.clone(),
                source.clone(),
                (*start, *end),
                *start == 0,
                options.read.clone(),
                Arc::clone(&limiter),
            ))
        })
        .collect();
    let mut runs = Vec::new();
    let mut digest = None;
    for task in tasks {
        let folded = task.await.context("join range fold")??;
        report.source_bytes = report.source_bytes.saturating_add(folded.bytes);
        digest = folded.last_digest.or(digest);
        if let Some(run) = store_run(&namespace, folded.builder, options, &mut report).await? {
            runs.push(run);
        }
    }
    let mut through = target;
    for _ in 0..options.max_attempts {
        if digest != base.manifest.through_digest {
            bail!(
                "continuity check failed: source record {} differs from the record the \
                 namespace was built from (the engine rebuilds such a namespace itself)",
                through.saturating_sub(1)
            );
        }
        let manifest = replacement(&base, &runs, through, digest.clone())?;
        match namespace.publish(Some(&base), &manifest).await? {
            PublishOutcome::Published(published) => {
                report.through_record = published.manifest.through_record;
                report.generation = published.manifest.generation;
                report.runs = published.manifest.runs.len();
                return Ok(report);
            }
            PublishOutcome::Conflict { manifest_key } => {
                report.orphans.push(manifest_key);
            }
        }
        // The old namespace moved on meanwhile: catch up to its new D.
        base = namespace
            .load()
            .await
            .context("reload CURRENT")?
            .context("the namespace disappeared during the rebuild")?;
        let next = base.manifest.through_record;
        if next < through {
            bail!("the namespace went back from D = {through} to D = {next} during the rebuild");
        }
        if next > through {
            let folded = fold_range(
                client.clone(),
                source.clone(),
                (through, next),
                false,
                options.read.clone(),
                Arc::clone(&limiter),
            )
            .await?;
            report.source_bytes = report.source_bytes.saturating_add(folded.bytes);
            digest = folded.last_digest;
            if let Some(run) = store_run(&namespace, folded.builder, options, &mut report).await? {
                runs.push(run);
            }
            through = next;
        }
    }
    bail!(
        "the namespace kept advancing; gave up after {} attempts",
        options.max_attempts
    )
}

async fn store_run(
    namespace: &KeyedNamespace,
    builder: RunBuilder,
    options: &RebuildOptions,
    report: &mut RebuildReport,
) -> anyhow::Result<Option<KeyedRunMeta>> {
    let Some(built) = finish(builder, options.read.part_options).await? else {
        return Ok(None);
    };
    for part in &built.parts {
        namespace.put_part(part).await?;
    }
    report.parts = report.parts.saturating_add(built.parts.len());
    Ok((!built.meta.parts.is_empty()).then_some(built.meta))
}

/// The rebuilt manifest replacing `base`: every old part it does not reuse
/// and the old manifest become obsolete.
fn replacement(
    base: &PublishedKeyedManifest,
    runs: &[KeyedRunMeta],
    through: u64,
    digest: Option<String>,
) -> anyhow::Result<KeyedManifest> {
    let mut manifest = KeyedManifest {
        version: KEYED_MANIFEST_VERSION,
        format: KEYED_PROJECTION_FORMAT,
        generation: 0,
        source: base.manifest.source.clone(),
        through_record: through,
        through_digest: digest,
        runs: runs.to_vec(),
        published_at_ms: SystemClock.now_ms(),
        obsoleted: vec![base.manifest_key.clone()],
    };
    let kept: HashSet<String> = manifest.part_keys().map(str::to_owned).collect();
    manifest.obsoleted.extend(
        base.manifest
            .part_keys()
            .filter(|key| !kept.contains(*key))
            .map(str::to_owned),
    );
    manifest.validate()?;
    Ok(manifest)
}

/// Result of [`sweep`].
#[derive(Clone, Debug, Default, Serialize)]
pub struct SweepReport {
    /// Objects listed.
    pub listed: usize,
    /// Objects a manifest that readers may still hold references.
    pub referenced: usize,
    /// Unreferenced objects younger than the grace period (kept).
    pub young: usize,
    /// Objects deleted (or, in a dry run, that would be).
    pub deleted: Vec<String>,
}

/// Generation of a manifest object key, `manifests/{generation:020}-{hash}.json`.
fn manifest_generation(key: &str) -> Option<u64> {
    key.strip_prefix("manifests/")?
        .strip_suffix(".json")?
        .split_once('-')?
        .0
        .parse()
        .ok()
}

fn sweepable(key: &str) -> bool {
    (key.starts_with("parts/") && key.ends_with(".parquet")) || manifest_generation(key).is_some()
}

/// Deletes the namespace's unreferenced part and manifest objects older
/// than `grace` at wall-clock time `now` (one LIST, plus a GET of each
/// manifest readers may hold).
///
/// A manifest is protected when a reader may still use it: the published
/// one; every manifest up to the published generation written within the
/// grace period; and the newest manifest written before it, which may have
/// been the published one when the period began. Everything they reference
/// is kept. `CURRENT`, unknown objects and objects of unknown age are never
/// deleted.
pub async fn sweep(
    store: ObjectStore,
    source: &KeyedSource,
    grace: Duration,
    now: SystemTime,
    dry_run: bool,
) -> anyhow::Result<SweepReport> {
    let namespace = KeyedNamespace::new(store, source.clone());
    let objects = namespace.objects().await.context("list the namespace")?;
    let published = namespace.load().await.context("load CURRENT")?;
    let cutoff = now.checked_sub(grace);
    let young = |modified: Option<SystemTime>| match (modified, cutoff) {
        (Some(modified), Some(cutoff)) => modified >= cutoff,
        _ => true,
    };
    let current_generation = published
        .as_ref()
        .map_or(0, |published| published.manifest.generation);
    let mut protected: HashSet<String> = HashSet::new();
    let mut referenced: HashSet<String> = HashSet::new();
    if let Some(published) = &published {
        protected.insert(published.manifest_key.clone());
        referenced.extend(published.manifest.part_keys().map(str::to_owned));
        let manifests: Vec<(u64, &str, bool)> = objects
            .iter()
            .filter_map(|object| {
                manifest_generation(&object.key)
                    .filter(|generation| *generation <= current_generation)
                    .map(|generation| (generation, object.key.as_str(), young(object.modified)))
            })
            .collect();
        let window_start = manifests
            .iter()
            .filter(|(_, _, young)| !young)
            .map(|(generation, _, _)| *generation)
            .max();
        for (generation, key, young) in &manifests {
            let at_window_start =
                Some(*generation) == window_start && *generation < current_generation;
            if *young || at_window_start {
                protected.insert((*key).to_owned());
            }
        }
        for key in &protected {
            if *key == published.manifest_key {
                continue;
            }
            if let Some(manifest) = namespace.manifest(key).await? {
                referenced.extend(manifest.part_keys().map(str::to_owned));
            }
        }
    }
    referenced.extend(protected);
    let mut report = SweepReport {
        listed: objects.len(),
        ..SweepReport::default()
    };
    for object in objects {
        if object.key == KEYED_CURRENT_KEY || !sweepable(&object.key) {
            continue;
        }
        if referenced.contains(&object.key) {
            report.referenced = report.referenced.saturating_add(1);
            continue;
        }
        if young(object.modified) {
            report.young = report.young.saturating_add(1);
            continue;
        }
        if !dry_run {
            namespace.delete(&object.key).await?;
        }
        report.deleted.push(object.key);
    }
    Ok(report)
}

#[derive(Serialize)]
struct DumpHeader<'a> {
    namespace: &'a str,
    pointer_etag: Option<&'a str>,
    manifest_key: Option<&'a str>,
    manifest: Option<&'a KeyedManifest>,
}

/// Writes the published pointer and manifest as one JSON line, then, with
/// `rows`, every visible row as keyed-state NDJSON.
pub async fn dump(
    store: ObjectStore,
    source: &KeyedSource,
    rows: bool,
    part_options: &PartOptions,
    out: &mut (dyn Write + Send),
) -> anyhow::Result<()> {
    let namespace = KeyedNamespace::new(store, source.clone());
    let published = namespace.load().await.context("load CURRENT")?;
    let header = DumpHeader {
        namespace: namespace.prefix(),
        pointer_etag: published
            .as_ref()
            .map(|published| published.pointer_etag.as_str()),
        manifest_key: published
            .as_ref()
            .map(|published| published.manifest_key.as_str()),
        manifest: published.as_ref().map(|published| &published.manifest),
    };
    serde_json::to_writer(&mut *out, &header)?;
    out.write_all(b"\n")?;
    if let (true, Some(published)) = (rows, &published) {
        let opener = namespace.opener();
        let mut cursor = RowCursor::new(&opener, &published.manifest.runs, part_options);
        while let Some(row) = cursor.next().await? {
            out.write_all(row.line().as_bytes())?;
        }
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_covers_the_range_in_order() {
        assert_eq!(split(10, 3), vec![(0, 4), (4, 8), (8, 10)]);
        assert_eq!(split(2, 4), vec![(0, 1), (1, 2)]);
        assert_eq!(split(5, 1), vec![(0, 5)]);
        assert!(split(0, 4).is_empty());
    }

    #[test]
    fn manifest_keys_parse_their_generation() {
        assert_eq!(
            manifest_generation("manifests/00000000000000000007-abc.json"),
            Some(7)
        );
        assert_eq!(manifest_generation("manifests/x.json.create-lock"), None);
        assert!(sweepable("parts/abc.parquet"));
        assert!(!sweepable("parts/abc.create-lock"));
        assert!(!sweepable("CURRENT"));
    }

    #[test]
    fn sampling_is_deterministic() {
        assert!(sampled(b"k", 1));
        let picked = (0_u8..=255).filter(|byte| sampled(&[*byte], 4)).count();
        assert!(picked > 30 && picked < 100, "{picked}");
        assert_eq!(sampled(b"abc", 7), sampled(b"abc", 7));
    }
}
