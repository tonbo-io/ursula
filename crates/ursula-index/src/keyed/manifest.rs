//! Keyed manifest v6 and its namespace (design §3.4, §5.5, §6.1 U15).
//!
//! A namespace `.keyed/{bucket}/{key}/{c:016x}/v{fmt}/` holds parts
//! (`parts/{blake3}-{nonce}.parquet`), manifests
//! (`manifests/{generation:020}-{blake3}-{nonce}.json`) and the `CURRENT`
//! pointer. A missing `CURRENT` is `state(0)`.
//!
//! Publication follows the event-time engine's protocol: write the manifest
//! put-if-absent, then compare-and-swap `CURRENT` against the base's entity
//! tag (put-if-absent for the first publish). opendal returns no entity tag
//! from a write, so the writer reads `CURRENT` back with the conditional get
//! and adopts the tag only when the bytes are its own; anything else is a
//! conflict, after which the caller reloads.
//!
//! Object identity and deletion across processes. S3 has no conditional
//! delete, and a DELETE may land arbitrarily long after it was issued, so
//! safety cannot rest on a writer waiting out deleters. Instead every object
//! a writer stores gets a physical key no other write ever uses: the content
//! hash (which readers verify) plus a random writer nonce
//! ([`unique_object_nonce`]). A key is never reused, so a deleter can only
//! remove an object that some writer stored and later obsoleted or
//! abandoned: a delayed DELETE can never hit a later publication's object.
//! Two writers that encode the same content store two objects; the loser of
//! the `CURRENT` CAS queues its own for deletion.
//!
//! A deleter still removes an object only on an observation, made at most
//! [`delete_decision_ttl`] before the DELETE is issued, that the object is at
//! least the GC grace old and that no manifest a reader may hold references
//! it (readers of a superseded manifest keep their objects for the grace).

use std::collections::HashSet;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;

use super::part::EncodedPart;
use super::part::StorePartOpener;
use crate::IndexError;
use crate::clock::Clock;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectInfo;
use crate::object_store::ObjectStore;
use crate::object_store::digest;

/// Version of the manifest document (the event-time engine is at 5).
pub const KEYED_MANIFEST_VERSION: u32 = 6;
/// Projection format version: the `v{fmt}` namespace component.
pub const KEYED_PROJECTION_FORMAT: u32 = 1;
/// Name of the pointer object inside a namespace.
pub const KEYED_CURRENT_KEY: &str = "CURRENT";
/// Upper bound of [`delete_decision_ttl`].
pub const MAX_DELETE_DECISION_TTL: Duration = Duration::from_secs(10);
/// Lower bound of [`delete_decision_ttl`] (tests run with a zero grace).
pub const MIN_DELETE_DECISION_TTL: Duration = Duration::from_millis(100);

/// How long a deleter may act on one observation of an object's age and of
/// the manifests referencing it: a quarter of the grace, within
/// [`MIN_DELETE_DECISION_TTL`]..=[`MAX_DELETE_DECISION_TTL`].
pub fn delete_decision_ttl(grace: Duration) -> Duration {
    grace
        .checked_div(4)
        .unwrap_or_default()
        .clamp(MIN_DELETE_DECISION_TTL, MAX_DELETE_DECISION_TTL)
}

/// A random 128-bit nonce (32 hex digits) that makes a stored object's key
/// unique to one write (see the module docs). Should the OS have no
/// randomness to give, it falls back to a hash of the process id and a
/// process counter.
pub fn unique_object_nonce() -> String {
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    static FALLBACK: AtomicU64 = AtomicU64::new(0);
    let nonce = crate::rt::random_u128().unwrap_or_else(|| {
        let counter = FALLBACK.fetch_add(1, Ordering::Relaxed);
        let seed = format!("{}-{counter}", std::process::id());
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(
            blake3::hash(seed.as_bytes())
                .as_bytes()
                .get(..16)
                .unwrap_or(&[0; 16]),
        );
        u128::from_le_bytes(bytes)
    });
    format!("{nonce:032x}")
}

/// Result of [`KeyedNamespace::sweep`].
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

/// Generation of a manifest object key,
/// `manifests/{generation:020}-{hash}-{nonce}.json`.
pub(crate) fn manifest_generation(key: &str) -> Option<u64> {
    key.strip_prefix("manifests/")?
        .strip_suffix(".json")?
        .split_once('-')?
        .0
        .parse()
        .ok()
}

/// Whether the sweep may delete `key` (relative to the namespace): parts and
/// manifests only, never `CURRENT`, locks or temporary files.
pub(crate) fn sweepable(key: &str) -> bool {
    (key.starts_with("parts/") && key.ends_with(".parquet")) || manifest_generation(key).is_some()
}

fn millis(time: SystemTime) -> Option<u64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|age| u64::try_from(age.as_millis()).ok())
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// The source a namespace was built from.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KeyedSource {
    /// Bucket of the stream.
    pub bucket: String,
    /// The stream's local name (`{stream}` or `{affinity}/{stream}`).
    pub key: String,
    /// The stream incarnation (`created_at_ms`).
    pub incarnation: u64,
}

/// One part of a run.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KeyedPartMeta {
    /// Object key relative to the namespace.
    pub key: String,
    /// File size.
    pub bytes: u64,
    /// Start of the tail (page index and footer); the data region before it
    /// is described by the footer layout.
    pub data_bytes: u64,
    /// blake3 of the tail `[data_bytes, bytes)`.
    pub tail_hash: String,
    /// Smallest row key or tombstone start (inclusive).
    #[serde(with = "key_text")]
    pub min_key: Vec<u8>,
    /// Largest row key or tombstone end (inclusive bound of the range the
    /// part may affect).
    #[serde(with = "key_text")]
    pub max_key: Vec<u8>,
    /// Stored rows (puts and point tombstones).
    pub rows: u64,
    /// Range tombstones in the footer.
    pub tombstones: u64,
}

impl KeyedPartMeta {
    /// Whether the part may hold rows or tombstones affecting keys in
    /// `[from, end)`.
    pub fn may_affect(&self, from: Option<&[u8]>, end: Option<&[u8]>) -> bool {
        from.is_none_or(|from| self.max_key.as_slice() >= from)
            && end.is_none_or(|end| self.min_key.as_slice() < end)
    }

    /// Whether the key ranges of two parts intersect.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.min_key <= other.max_key && other.min_key <= self.max_key
    }
}

/// A run: the folded effect of records `[start_record, end_record)`, as
/// key-disjoint parts in ascending key order.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KeyedRunMeta {
    /// First record of the run.
    pub start_record: u64,
    /// One past the last record of the run.
    pub end_record: u64,
    /// Parts in ascending key order.
    pub parts: Vec<KeyedPartMeta>,
}

impl KeyedRunMeta {
    /// Total stored bytes.
    pub fn bytes(&self) -> u64 {
        self.parts
            .iter()
            .map(|part| part.bytes)
            .fold(0, u64::saturating_add)
    }
}

/// Manifest v6.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct KeyedManifest {
    /// [`KEYED_MANIFEST_VERSION`].
    pub version: u32,
    /// [`KEYED_PROJECTION_FORMAT`].
    pub format: u32,
    /// Publication counter of the namespace, starting at 1.
    pub generation: u64,
    /// The source stream.
    pub source: KeyedSource,
    /// `D`: the manifest reflects records `0 .. D`.
    pub through_record: u64,
    /// blake3 of the stored bytes of record `D − 1`; absent when `D = 0`.
    pub through_digest: Option<String>,
    /// Runs in ascending, disjoint record order (oldest first).
    pub runs: Vec<KeyedRunMeta>,
    /// Wall-clock time of publication.
    pub published_at_ms: u64,
    /// Objects this manifest's edit made unreachable (delta-driven GC).
    pub obsoleted: Vec<String>,
}

/// The continuity digest of one stored record.
pub fn record_digest(stored_bytes: &[u8]) -> String {
    digest(stored_bytes)
}

fn invalid(message: impl Into<String>) -> IndexError {
    IndexError::InvalidKeyedState(message.into())
}

impl KeyedManifest {
    /// `state(0)` of a source: what a missing namespace means.
    pub fn empty(source: KeyedSource) -> Self {
        Self::empty_at(source, KEYED_PROJECTION_FORMAT)
    }

    /// `state(0)` of a source in a namespace of projection format `format`.
    pub fn empty_at(source: KeyedSource, format: u32) -> Self {
        Self {
            version: KEYED_MANIFEST_VERSION,
            format,
            generation: 0,
            source,
            through_record: 0,
            through_digest: None,
            runs: Vec::new(),
            published_at_ms: 0,
            obsoleted: Vec::new(),
        }
    }

    /// Checks the structural invariants.
    pub fn validate(&self) -> Result<(), IndexError> {
        if self.version != KEYED_MANIFEST_VERSION {
            return Err(IndexError::ManifestVersion(self.version));
        }
        if self.format == 0 {
            return Err(invalid("projection format 0 does not exist"));
        }
        if (self.through_record == 0) != self.through_digest.is_none() {
            return Err(invalid("through_digest must be present exactly when D > 0"));
        }
        let mut next = 0_u64;
        for run in &self.runs {
            if run.start_record < next
                || run.start_record >= run.end_record
                || run.end_record > self.through_record
            {
                return Err(invalid("runs are not ascending, disjoint and below D"));
            }
            next = run.end_record;
            let mut previous: Option<&KeyedPartMeta> = None;
            for part in &run.parts {
                if part.min_key > part.max_key
                    || part.data_bytes >= part.bytes
                    || part.rows.saturating_add(part.tombstones) == 0
                {
                    return Err(invalid(format!("part `{}` is malformed", part.key)));
                }
                if let Some(previous) = previous
                    && previous.max_key > part.min_key
                {
                    return Err(invalid("a run's parts overlap or are out of order"));
                }
                previous = Some(part);
            }
        }
        Ok(())
    }

    /// Every part key referenced.
    pub fn part_keys(&self) -> impl Iterator<Item = &str> {
        self.runs
            .iter()
            .flat_map(|run| run.parts.iter().map(|part| part.key.as_str()))
    }

    /// The manifest after ingesting records `[D, through_record)` into
    /// `run` (or into nothing, when they left no rows or tombstones).
    pub fn after_ingest(
        &self,
        run: Option<KeyedRunMeta>,
        through_record: u64,
        through_digest: String,
        published_at_ms: u64,
    ) -> Result<Self, IndexError> {
        if through_record <= self.through_record {
            return Err(invalid("an ingest must advance D"));
        }
        let mut next = self.clone();
        if let Some(run) = run {
            if run.start_record != self.through_record || run.end_record != through_record {
                return Err(invalid("an ingested run must cover exactly [D, D')"));
            }
            if !run.parts.is_empty() {
                next.runs.push(run);
            }
        }
        next.through_record = through_record;
        next.through_digest = Some(through_digest);
        next.published_at_ms = published_at_ms;
        next.obsoleted = Vec::new();
        next.validate()?;
        Ok(next)
    }

    /// Rebases a compaction onto this manifest: when every input run is
    /// still present, unchanged and contiguous (and at the front when
    /// `into_oldest`), they are replaced by `output`. Returns `None` when
    /// the compaction no longer applies.
    pub fn after_compaction(
        &self,
        inputs: &[KeyedRunMeta],
        output: &KeyedRunMeta,
        into_oldest: bool,
        published_at_ms: u64,
    ) -> Result<Option<Self>, IndexError> {
        let (Some(first), Some(last)) = (inputs.first(), inputs.last()) else {
            return Ok(None);
        };
        let Some(position) = self.runs.iter().position(|run| run == first) else {
            return Ok(None);
        };
        if into_oldest && position != 0 {
            return Ok(None);
        }
        let end = position.saturating_add(inputs.len());
        if self.runs.get(position..end) != Some(inputs) {
            return Ok(None);
        }
        if output.start_record != first.start_record || output.end_record != last.end_record {
            return Err(invalid(
                "a compaction output must cover its inputs' records",
            ));
        }
        let kept: std::collections::HashSet<&str> =
            output.parts.iter().map(|part| part.key.as_str()).collect();
        let obsoleted = inputs
            .iter()
            .flat_map(|run| run.parts.iter())
            .filter(|part| !kept.contains(part.key.as_str()))
            .map(|part| part.key.clone())
            .collect();
        let mut next = self.clone();
        let replacement: Vec<KeyedRunMeta> = if output.parts.is_empty() {
            Vec::new()
        } else {
            vec![output.clone()]
        };
        next.runs.splice(position..end, replacement);
        next.published_at_ms = published_at_ms;
        next.obsoleted = obsoleted;
        next.validate()?;
        Ok(Some(next))
    }

    /// The manifest's bytes, its object key (unique to this write through
    /// `nonce`) and the `CURRENT` pointer to it.
    fn encode(&self, nonce: &str) -> Result<(String, Vec<u8>, Vec<u8>), IndexError> {
        let bytes = serde_json::to_vec(self)?;
        let key = format!(
            "manifests/{:020}-{}-{nonce}.json",
            self.generation,
            digest(&bytes)
        );
        let pointer = serde_json::to_vec(&KeyedCurrent {
            version: KEYED_MANIFEST_VERSION,
            generation: self.generation,
            manifest: key.clone(),
        })?;
        Ok((key, bytes, pointer))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct KeyedCurrent {
    version: u32,
    generation: u64,
    manifest: String,
}

/// A manifest as published, with the `CURRENT` entity tag to CAS against.
#[derive(Clone, Debug)]
pub struct PublishedKeyedManifest {
    /// Entity tag of the `CURRENT` object holding this publication.
    pub pointer_etag: String,
    /// Object key of the manifest, relative to the namespace.
    pub manifest_key: String,
    /// The manifest.
    pub manifest: KeyedManifest,
}

/// Outcome of [`KeyedNamespace::publish`].
#[derive(Clone, Debug)]
pub enum PublishOutcome {
    /// `CURRENT` now names the manifest.
    Published(Box<PublishedKeyedManifest>),
    /// Another writer published first; reload and retry from its state.
    /// `manifest_key` names the manifest object this attempt wrote, which
    /// stays unpublished (its writer deletes it after the GC grace period).
    Conflict {
        /// The unpublished manifest object, relative to the namespace.
        manifest_key: String,
    },
}

/// `.keyed/{bucket}/{key}/{c:016x}/v{fmt}/`.
pub fn namespace_prefix(source: &KeyedSource, format: u32) -> String {
    format!(
        ".keyed/{}/{}/{:016x}/v{format}/",
        source.bucket,
        ursula_shard::keyed_namespace::encode_key_component(&source.key),
        source.incarnation
    )
}

/// The content hash in a manifest key,
/// `manifests/{generation}-{hash}-{nonce}.json` (or, written before nonces,
/// `manifests/{generation}-{hash}.json`).
fn manifest_hash(key: &str) -> Result<&str, IndexError> {
    key.strip_prefix("manifests/")
        .and_then(|name| name.strip_suffix(".json"))
        .and_then(|name| name.split_once('-'))
        .map(|(_, rest)| rest.split_once('-').map_or(rest, |(hash, _)| hash))
        .ok_or_else(|| IndexError::InvalidObjectKey(key.to_owned()))
}

/// One keyed namespace in an object store.
#[derive(Clone)]
pub struct KeyedNamespace {
    store: ObjectStore,
    source: KeyedSource,
    format: u32,
    prefix: String,
}

/// Default GC grace of a namespace (the engine's default `gc_grace`).
pub const DEFAULT_GC_GRACE: Duration = Duration::from_secs(600);

impl KeyedNamespace {
    /// The namespace of `source` at the current projection format.
    pub fn new(store: ObjectStore, source: KeyedSource) -> Self {
        Self::with_format(store, source, KEYED_PROJECTION_FORMAT)
    }

    /// The namespace of `source` at projection format `format`: a separate
    /// `v{format}/` prefix, so a blue/green rebuild at another format never
    /// touches the namespace being served.
    pub fn with_format(store: ObjectStore, source: KeyedSource, format: u32) -> Self {
        let prefix = namespace_prefix(&source, format);
        Self {
            store,
            source,
            format,
            prefix,
        }
    }

    /// The projection format of this namespace.
    pub fn format(&self) -> u32 {
        self.format
    }

    /// The namespace's key prefix, ending in `/`.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The source it serves.
    pub fn source(&self) -> &KeyedSource {
        &self.source
    }

    fn object(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    /// A verified part reader over this namespace.
    pub fn opener(&self) -> StorePartOpener {
        StorePartOpener::new(self.store.clone(), self.prefix.clone())
    }

    /// Stores a part under its unique key (`meta.key`, see
    /// [`unique_object_nonce`]).
    pub async fn put_part(&self, part: &EncodedPart) -> Result<(), IndexError> {
        self.put_new(&part.meta.key, &part.bytes).await
    }

    /// Writes an object under a key unique to this write. An object already
    /// there means the key was reused, which must never happen (a deleter
    /// may have decided to remove it), so it is an error.
    async fn put_new(&self, key: &str, bytes: &[u8]) -> Result<(), IndexError> {
        let object = self.object(key);
        match self.store.put_if_absent(&object, bytes).await? {
            ConditionalWrite::Written => Ok(()),
            ConditionalWrite::Conflict => Err(invalid(format!(
                "object key {key} already exists; keys are never reused"
            ))),
        }
    }

    /// The object's modification time, or `None` when it is absent or its
    /// age is unknown (one HEAD).
    pub(crate) async fn modified_ms(&self, key: &str) -> Result<Option<u64>, IndexError> {
        Ok(self
            .store
            .stat(&self.object(key))
            .await?
            .and_then(|info| info.modified)
            .and_then(millis))
    }

    /// Deletes an object of the namespace (GC, or a writer's own unpublished
    /// objects).
    pub async fn delete(&self, key: &str) -> Result<(), IndexError> {
        self.store.delete(&self.object(key)).await
    }

    /// Every object of the namespace (one LIST), with keys relative to it.
    pub(crate) async fn objects(&self) -> Result<Vec<ObjectInfo>, IndexError> {
        let mut objects = self.store.list(&self.prefix).await?;
        for object in &mut objects {
            if let Some(relative) = object.key.strip_prefix(&self.prefix) {
                object.key = relative.to_owned();
            }
        }
        Ok(objects)
    }

    /// Deletes every object of the namespace (the stream incarnation is
    /// gone). Returns the number of objects deleted.
    pub async fn delete_all(&self) -> Result<usize, IndexError> {
        let objects = self.store.list(&self.prefix).await?;
        let count = objects.len();
        for object in objects {
            self.store.delete(&object.key).await?;
        }
        Ok(count)
    }

    /// The entity tag of `CURRENT` (one HEAD); `None` when the namespace is
    /// missing.
    pub async fn current_etag(&self) -> Result<Option<String>, IndexError> {
        self.store.head(&self.object(KEYED_CURRENT_KEY)).await
    }

    /// Reads the manifest object `key` (relative to the namespace), checking
    /// its content hash; `None` when it is gone.
    pub async fn manifest(&self, key: &str) -> Result<Option<KeyedManifest>, IndexError> {
        let hash = manifest_hash(key)?;
        let Some(object) = self.store.get(&self.object(key)).await? else {
            return Ok(None);
        };
        if digest(&object.bytes) != hash {
            return Err(IndexError::ObjectHashMismatch(key.to_owned()));
        }
        let manifest: KeyedManifest = serde_json::from_slice(&object.bytes)?;
        manifest.validate()?;
        Ok(Some(manifest))
    }

    /// Every object a reader may still use, at `now_ms`, with the listed
    /// objects' ages (one GET of `CURRENT` and one per protected manifest).
    ///
    /// A manifest is protected when a reader may still use it: the
    /// published one; every manifest up to the published generation written
    /// within the grace period; and the newest manifest written before it,
    /// which may have been the published one when the period began.
    /// Everything they reference is protected too.
    async fn protected(
        &self,
        objects: &[ObjectInfo],
        now_ms: u64,
        grace: Duration,
    ) -> Result<HashSet<String>, IndexError> {
        let Some(published) = self.load().await? else {
            return Ok(HashSet::new());
        };
        let young = |object: &ObjectInfo| {
            object
                .modified
                .and_then(millis)
                .is_none_or(|modified| now_ms.saturating_sub(modified) < duration_ms(grace))
        };
        let current_generation = published.manifest.generation;
        let manifests: Vec<(u64, &str, bool)> = objects
            .iter()
            .filter_map(|object| {
                manifest_generation(&object.key)
                    .filter(|generation| *generation <= current_generation)
                    .map(|generation| (generation, object.key.as_str(), young(object)))
            })
            .collect();
        let window_start = manifests
            .iter()
            .filter(|(_, _, young)| !young)
            .map(|(generation, _, _)| *generation)
            .max();
        let mut protected: HashSet<String> = HashSet::new();
        protected.insert(published.manifest_key.clone());
        protected.extend(published.manifest.part_keys().map(str::to_owned));
        for (generation, key, young) in &manifests {
            let at_window_start =
                Some(*generation) == window_start && *generation < current_generation;
            if !(*young || at_window_start) || *key == published.manifest_key {
                continue;
            }
            protected.insert((*key).to_owned());
            if let Some(manifest) = self.manifest(key).await? {
                protected.extend(manifest.part_keys().map(str::to_owned));
            }
        }
        Ok(protected)
    }

    /// [`Self::protected`] over a fresh LIST of the namespace: every object
    /// a reader may still use at `now_ms` (IX2: engine GC checks queued
    /// orphans against it, not only against `CURRENT`, because
    /// content-addressed parts may be referenced by a recent manifest).
    pub(crate) async fn protected_now(
        &self,
        now_ms: u64,
        grace: Duration,
    ) -> Result<HashSet<String>, IndexError> {
        let objects = self.objects().await?;
        self.protected(&objects, now_ms, grace).await
    }

    /// The orphan sweep (U20 `sweep`, also run by the engine): one LIST of
    /// the namespace, then deletes every part and manifest that is older
    /// than `grace` at `clock`'s time and that no manifest a reader may
    /// still hold references (see `protected`). `CURRENT`, unknown objects
    /// and objects of unknown age are never deleted.
    ///
    /// Each deletion acts on observations at most [`delete_decision_ttl`]
    /// old: past that, the protected set is reloaded (it only grows) and the
    /// object's age is observed again (one HEAD) before its DELETE.
    pub async fn sweep(
        &self,
        clock: &dyn Clock,
        grace: Duration,
        dry_run: bool,
    ) -> Result<SweepReport, IndexError> {
        let ttl_ms = duration_ms(delete_decision_ttl(grace));
        let grace_ms = duration_ms(grace);
        let listed_at = clock.now_ms();
        let objects = self.objects().await?;
        let mut protected = self.protected(&objects, listed_at, grace).await?;
        let mut protected_at = listed_at;
        let mut report = SweepReport {
            listed: objects.len(),
            ..SweepReport::default()
        };
        for object in objects {
            if object.key == KEYED_CURRENT_KEY || !sweepable(&object.key) {
                continue;
            }
            if protected.contains(&object.key) {
                report.referenced = report.referenced.saturating_add(1);
                continue;
            }
            let mut observed_at = listed_at;
            let mut modified = object.modified.and_then(millis);
            let old = |modified: Option<u64>, at: u64| {
                modified.is_some_and(|modified| at.saturating_sub(modified) >= grace_ms)
            };
            if !old(modified, observed_at) {
                report.young = report.young.saturating_add(1);
                continue;
            }
            if !dry_run && clock.now_ms().saturating_sub(observed_at) > ttl_ms {
                // A stale observation: observe the age again, then, when
                // stale too, the manifests that may reference the object.
                observed_at = clock.now_ms();
                modified = self.modified_ms(&object.key).await?;
                if modified.is_none() {
                    continue;
                }
                if !old(modified, observed_at) {
                    report.young = report.young.saturating_add(1);
                    continue;
                }
                if observed_at.saturating_sub(protected_at) > ttl_ms {
                    let fresh = self.protected(&[], observed_at, grace).await?;
                    protected.extend(fresh);
                    protected_at = observed_at;
                    if protected.contains(&object.key) {
                        report.referenced = report.referenced.saturating_add(1);
                        continue;
                    }
                }
            }
            if !dry_run {
                self.delete(&object.key).await?;
            }
            report.deleted.push(object.key);
        }
        Ok(report)
    }

    /// Loads the published manifest; `None` is a missing namespace
    /// (`state(0)`).
    pub async fn load(&self) -> Result<Option<PublishedKeyedManifest>, IndexError> {
        let Some(current) = self.store.get(&self.object(KEYED_CURRENT_KEY)).await? else {
            return Ok(None);
        };
        let pointer: KeyedCurrent = serde_json::from_slice(&current.bytes)?;
        if pointer.version != KEYED_MANIFEST_VERSION {
            return Err(IndexError::ManifestVersion(pointer.version));
        }
        let manifest = self
            .manifest(&pointer.manifest)
            .await?
            .ok_or_else(|| IndexError::MissingObject(pointer.manifest.clone()))?;
        if manifest.generation != pointer.generation {
            return Err(invalid("CURRENT generation does not match its manifest"));
        }
        if manifest.source != self.source {
            return Err(invalid("namespace manifest names another source"));
        }
        if manifest.format != self.format {
            return Err(invalid(format!(
                "namespace at projection format {} holds a format {} manifest",
                self.format, manifest.format
            )));
        }
        Ok(Some(PublishedKeyedManifest {
            pointer_etag: current.etag,
            manifest_key: pointer.manifest,
            manifest,
        }))
    }

    /// Publishes `manifest` on top of `base` (`None`: the namespace is
    /// missing). The manifest's generation is set to the base's plus one.
    pub async fn publish(
        &self,
        base: Option<&PublishedKeyedManifest>,
        manifest: &KeyedManifest,
    ) -> Result<PublishOutcome, IndexError> {
        let mut manifest = manifest.clone();
        manifest.generation = base
            .map_or(0, |base| base.manifest.generation)
            .checked_add(1)
            .ok_or_else(|| invalid("generation overflowed"))?;
        if manifest.source != self.source {
            return Err(invalid("manifest names another source"));
        }
        if manifest.format != self.format {
            return Err(invalid(format!(
                "a format {} manifest cannot be published at projection format {}",
                manifest.format, self.format
            )));
        }
        manifest.validate()?;
        let (key, bytes, pointer) = manifest.encode(&unique_object_nonce())?;
        self.put_new(&key, &bytes).await?;
        let current = self.object(KEYED_CURRENT_KEY);
        let written = match base {
            Some(base) => {
                self.store
                    .compare_and_swap(&current, &base.pointer_etag, &pointer)
                    .await?
            }
            None => self.store.put_if_absent(&current, &pointer).await?,
        };
        if written == ConditionalWrite::Conflict {
            return Ok(PublishOutcome::Conflict { manifest_key: key });
        }
        // Read back: adopt the entity tag only from our own bytes.
        match self.store.get(&current).await? {
            Some(object) if object.bytes == pointer => Ok(PublishOutcome::Published(Box::new(
                PublishedKeyedManifest {
                    pointer_etag: object.etag,
                    manifest_key: key,
                    manifest,
                },
            ))),
            _ => Ok(PublishOutcome::Conflict { manifest_key: key }),
        }
    }
}

mod key_text {
    use serde::Deserialize;
    use serde::Deserializer;
    use serde::Serializer;
    use serde::de::Error;

    use crate::keyed::batch::decode_key;
    use crate::keyed::batch::encode_key;

    pub(super) fn serialize<S: Serializer>(key: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&encode_key(key))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<u8>, D::Error> {
        let text = <&str>::deserialize(deserializer)?;
        decode_key(text).map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FsObjectStore;
    use crate::keyed::part::KeyedEntry;
    use crate::keyed::part::PartOptions;
    use crate::keyed::part::encode_part;

    fn source() -> KeyedSource {
        KeyedSource {
            bucket: "b".to_owned(),
            key: "a/s%1".to_owned(),
            incarnation: 0x1234,
        }
    }

    fn run(start: u64, end: u64, key: u8) -> (KeyedRunMeta, EncodedPart) {
        let part = encode_part(
            &[KeyedEntry {
                key: vec![key],
                record: start,
                value: Some("1".to_owned()),
            }],
            &[],
            &PartOptions::default(),
        )
        .unwrap();
        (
            KeyedRunMeta {
                start_record: start,
                end_record: end,
                parts: vec![part.meta.clone()],
            },
            part,
        )
    }

    #[test]
    fn namespace_prefix_keeps_affinity_streams_apart() {
        assert_eq!(
            namespace_prefix(&source(), 1),
            ".keyed/b/a%2Fs%251/0000000000001234/v1/"
        );
    }

    /// The node's stream-delete GC removes `keyed_incarnation_prefix` as a
    /// prefix; every namespace this engine writes must sit under it.
    #[test]
    fn namespace_prefix_sits_under_the_node_gc_prefix() {
        let source = source();
        let stream = ursula_shard::BucketStreamId::new(source.bucket.clone(), source.key.clone());
        let gc =
            ursula_shard::keyed_namespace::keyed_incarnation_prefix(&stream, source.incarnation);
        assert!(ursula_shard::keyed_namespace::is_keyed_incarnation_prefix(
            &gc
        ));
        assert!(namespace_prefix(&source, 1).starts_with(&gc), "{gc}");
    }

    #[tokio::test]
    async fn publish_load_and_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::from(FsObjectStore::new(dir.path()).unwrap());
        let namespace = KeyedNamespace::new(store, source());
        assert!(namespace.load().await.unwrap().is_none());

        let (first_run, part) = run(0, 2, 1);
        namespace.put_part(&part).await.unwrap();
        let empty = KeyedManifest::empty(source());
        let first = empty
            .after_ingest(Some(first_run), 2, record_digest(b"r1"), 10)
            .unwrap();
        let PublishOutcome::Published(published) = namespace.publish(None, &first).await.unwrap()
        else {
            panic!("first publish conflicted");
        };
        assert_eq!(published.manifest.generation, 1);
        let loaded = namespace.load().await.unwrap().unwrap();
        assert_eq!(loaded.manifest, published.manifest);
        assert_eq!(loaded.pointer_etag, published.pointer_etag);

        // A second writer with no base, or a stale base, conflicts.
        assert!(matches!(
            namespace.publish(None, &first).await.unwrap(),
            PublishOutcome::Conflict { .. }
        ));
        let (second_run, part) = run(2, 3, 2);
        namespace.put_part(&part).await.unwrap();
        let second = loaded
            .manifest
            .after_ingest(Some(second_run), 3, record_digest(b"r2"), 20)
            .unwrap();
        let PublishOutcome::Published(next) =
            namespace.publish(Some(&loaded), &second).await.unwrap()
        else {
            panic!("second publish conflicted");
        };
        assert_eq!(next.manifest.generation, 2);
        assert!(matches!(
            namespace.publish(Some(&loaded), &second).await.unwrap(),
            PublishOutcome::Conflict { .. }
        ));
        assert_eq!(
            namespace
                .load()
                .await
                .unwrap()
                .unwrap()
                .manifest
                .through_record,
            3
        );
    }

    #[test]
    fn compaction_rebases_only_when_inputs_remain() {
        let (a, _) = run(0, 2, 1);
        let (b, _) = run(2, 4, 2);
        let (c, _) = run(4, 5, 3);
        let mut manifest = KeyedManifest::empty(source());
        manifest.runs = vec![a.clone(), b.clone(), c.clone()];
        manifest.through_record = 5;
        manifest.through_digest = Some(record_digest(b"x"));
        let (merged, _) = run(2, 5, 9);
        let next = manifest
            .after_compaction(&[b.clone(), c.clone()], &merged, false, 1)
            .unwrap()
            .unwrap();
        assert_eq!(next.runs, vec![a.clone(), merged.clone()]);
        assert_eq!(next.obsoleted.len(), 2);
        // Inputs no longer present: the compaction is abandoned.
        assert!(
            next.after_compaction(&[b.clone(), c], &merged, false, 2)
                .unwrap()
                .is_none()
        );
        // Into-oldest only at the front.
        assert!(
            manifest
                .after_compaction(&[b], &merged, true, 3)
                .unwrap()
                .is_none()
        );
    }

    /// A blue/green rebuild at another projection format writes a separate
    /// `v{fmt}/` namespace and never reads or overwrites the served one.
    #[tokio::test]
    async fn projection_formats_are_separate_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::from(FsObjectStore::new(dir.path()).unwrap());
        let blue = KeyedNamespace::new(store.clone(), source());
        let green = KeyedNamespace::with_format(store.clone(), source(), 2);
        assert_eq!(green.format(), 2);
        assert!(green.prefix().ends_with("/v2/"), "{}", green.prefix());
        assert_ne!(blue.prefix(), green.prefix());

        let (first_run, part) = run(0, 2, 1);
        blue.put_part(&part).await.unwrap();
        let first = KeyedManifest::empty(source())
            .after_ingest(Some(first_run), 2, record_digest(b"r1"), 10)
            .unwrap();
        assert!(matches!(
            blue.publish(None, &first).await.unwrap(),
            PublishOutcome::Published(_)
        ));
        // The green namespace is still `state(0)`, and refuses a blue manifest.
        assert!(green.load().await.unwrap().is_none());
        green.publish(None, &first).await.unwrap_err();
        let green_first = KeyedManifest::empty_at(source(), 2);
        assert_eq!(green_first.format, 2);
        green_first.validate().unwrap();
        KeyedManifest::empty_at(source(), 0).validate().unwrap_err();
        assert_eq!(
            blue.load().await.unwrap().unwrap().manifest.through_record,
            2
        );
    }

    #[test]
    fn validation_rejects_bad_runs() {
        let (a, _) = run(0, 2, 1);
        let mut manifest = KeyedManifest::empty(source());
        manifest.runs = vec![a];
        manifest.through_record = 1;
        manifest.through_digest = Some(record_digest(b"x"));
        manifest.validate().unwrap_err();
        manifest.through_record = 2;
        manifest.validate().unwrap();
        manifest.through_digest = None;
        manifest.validate().unwrap_err();
    }
}
