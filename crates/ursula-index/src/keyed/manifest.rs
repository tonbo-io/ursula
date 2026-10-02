//! Keyed manifest v6 and its namespace (design §3.4, §5.5, §6.1 U15).
//!
//! A namespace `.keyed/{bucket}/{key}/{c:016x}/v{fmt}/` holds content-
//! addressed parts (`parts/{blake3}.parquet`), content-addressed manifests
//! (`manifests/{generation:020}-{blake3}.json`) and the `CURRENT` pointer.
//! A missing `CURRENT` is `state(0)`.
//!
//! Publication follows the event-time engine's protocol: write the manifest
//! put-if-absent, then compare-and-swap `CURRENT` against the base's entity
//! tag (put-if-absent for the first publish). opendal returns no entity tag
//! from a write, so the writer reads `CURRENT` back with the conditional get
//! and adopts the tag only when the bytes are its own; anything else is a
//! conflict, after which the caller reloads.

use serde::Deserialize;
use serde::Serialize;

use super::part::EncodedPart;
use super::part::StorePartOpener;
use crate::IndexError;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectStore;
use crate::object_store::digest;

/// Version of the manifest document (the event-time engine is at 5).
pub const KEYED_MANIFEST_VERSION: u32 = 6;
/// Projection format version: the `v{fmt}` namespace component.
pub const KEYED_PROJECTION_FORMAT: u32 = 1;
/// Name of the pointer object inside a namespace.
pub const KEYED_CURRENT_KEY: &str = "CURRENT";

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
        Self {
            version: KEYED_MANIFEST_VERSION,
            format: KEYED_PROJECTION_FORMAT,
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
        if self.format != KEYED_PROJECTION_FORMAT {
            return Err(invalid(format!(
                "unknown projection format {}",
                self.format
            )));
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

    fn encode(&self) -> Result<(String, Vec<u8>, Vec<u8>), IndexError> {
        let bytes = serde_json::to_vec(self)?;
        let key = format!("manifests/{:020}-{}.json", self.generation, digest(&bytes));
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
    Conflict,
}

/// Percent-encodes a stream's local name as one path component
/// (`%` → `%25`, `/` → `%2F`).
pub fn encode_stream_component(key: &str) -> String {
    key.replace('%', "%25").replace('/', "%2F")
}

/// `.keyed/{bucket}/{key}/{c:016x}/v{fmt}/`.
pub fn namespace_prefix(source: &KeyedSource, format: u32) -> String {
    format!(
        ".keyed/{}/{}/{:016x}/v{format}/",
        source.bucket,
        encode_stream_component(&source.key),
        source.incarnation
    )
}

/// One keyed namespace in an object store.
#[derive(Clone)]
pub struct KeyedNamespace {
    store: ObjectStore,
    source: KeyedSource,
    prefix: String,
}

impl KeyedNamespace {
    /// The namespace of `source` at the current projection format.
    pub fn new(store: ObjectStore, source: KeyedSource) -> Self {
        let prefix = namespace_prefix(&source, KEYED_PROJECTION_FORMAT);
        Self {
            store,
            source,
            prefix,
        }
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

    /// Stores a part (put-if-absent; parts are content-addressed, so an
    /// existing object already holds the same bytes).
    pub async fn put_part(&self, part: &EncodedPart) -> Result<(), IndexError> {
        let _written = self
            .store
            .put_if_absent(&self.object(&part.meta.key), &part.bytes)
            .await?;
        Ok(())
    }

    /// Deletes an object of the namespace (GC, or a writer's own unpublished
    /// objects).
    pub async fn delete(&self, key: &str) -> Result<(), IndexError> {
        self.store.delete(&self.object(key)).await
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
        let hash = pointer
            .manifest
            .strip_prefix("manifests/")
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|name| name.split_once('-'))
            .map(|(_, hash)| hash)
            .ok_or_else(|| IndexError::InvalidObjectKey(pointer.manifest.clone()))?;
        let object = self
            .store
            .get(&self.object(&pointer.manifest))
            .await?
            .ok_or_else(|| IndexError::MissingObject(pointer.manifest.clone()))?;
        if digest(&object.bytes) != hash {
            return Err(IndexError::ObjectHashMismatch(pointer.manifest));
        }
        let manifest: KeyedManifest = serde_json::from_slice(&object.bytes)?;
        manifest.validate()?;
        if manifest.generation != pointer.generation {
            return Err(invalid("CURRENT generation does not match its manifest"));
        }
        if manifest.source != self.source {
            return Err(invalid("namespace manifest names another source"));
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
        manifest.validate()?;
        let (key, bytes, pointer) = manifest.encode()?;
        let _manifest_write = self.store.put_if_absent(&self.object(&key), &bytes).await?;
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
            return Ok(PublishOutcome::Conflict);
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
            _ => Ok(PublishOutcome::Conflict),
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
            PublishOutcome::Conflict
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
            PublishOutcome::Conflict
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
