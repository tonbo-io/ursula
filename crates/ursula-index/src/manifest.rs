use serde::Deserialize;
use serde::Serialize;

use crate::IndexError;
use crate::IndexStatus;
use crate::object_store::ObjectStore;
use crate::object_store::digest;
use crate::part::PartFilter;
use crate::store::IndexBase;
use crate::store::SkipCounts;
use crate::store::SourceBinding;

pub(crate) const FORMAT_VERSION: u32 = 6;
pub(crate) const CURRENT_KEY: &str = "CURRENT";
/// The one claim object of a stream: a single open-ended claim from the
/// first uncovered offset.
pub(crate) const CLAIM_KEY: &str = "claims/current.json";

/// The live claim of one stream. Claims coordinate work but are not a
/// correctness boundary: commits are verified and manifest-CAS guarded.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct SegmentLease {
    pub start_offset: u64,
    pub worker_id: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct PartMeta {
    pub(crate) key: String,
    pub(crate) layout_key: String,
    pub(crate) level: u8,
    pub(crate) partition_start_ms: i64,
    pub(crate) entries: u64,
    pub(crate) min_t_ms: i64,
    pub(crate) max_t_ms: i64,
    pub(crate) max_t_end_ms: i64,
    pub(crate) min_offset: u64,
    pub(crate) max_offset: u64,
    pub(crate) bytes: u64,
}

impl PartMeta {
    pub(crate) fn may_match(&self, filter: &PartFilter) -> bool {
        let time = if filter.overlap {
            self.max_t_end_ms >= filter.from_ms && self.min_t_ms < filter.until_ms
        } else {
            self.max_t_ms >= filter.from_ms && self.min_t_ms < filter.until_ms
        };
        time && self.max_offset >= filter.floor && self.min_offset < filter.through
    }

    pub(crate) fn overlaps_offsets(&self, start: u64, end: u64) -> bool {
        self.max_offset >= start && self.min_offset < end
    }
}

/// Manifest v6. Coverage is the byte range `[floor_offset, durable_offset)`:
/// one claim per stream commits contiguous ranges from the first uncovered
/// offset, so covered bytes never have gaps.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Manifest {
    pub(crate) version: u32,
    pub(crate) source: SourceBinding,
    pub(crate) extractor_digest: String,
    pub(crate) generation: u64,
    /// Where the index started reading.
    pub(crate) indexed_from_offset: u64,
    /// The source's retained offset as last observed.
    pub(crate) floor_offset: u64,
    /// Every complete message before this offset is indexed or counted.
    pub(crate) durable_offset: u64,
    /// A restart point that may not be a message boundary (a retained offset
    /// the index had not reached, or the registration base).
    pub(crate) resync_offset: Option<u64>,
    /// Source bytes trimmed by retention before they were indexed, plus
    /// bytes discarded to resynchronize.
    pub(crate) trimmed_bytes: u64,
    pub(crate) skipped: SkipCounts,
    pub(crate) status: IndexStatus,
    pub(crate) parts: Vec<PartMeta>,
}

/// The fields GC needs to tell a compatible manifest from another format or
/// source, readable from any manifest version.
#[derive(Debug, Deserialize)]
pub(crate) struct ManifestIdentity {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) source: Option<IdentitySource>,
    #[serde(default)]
    pub(crate) extractor_digest: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct IdentitySource {
    pub(crate) stream_url: String,
}

/// What a manifest must match to be adopted.
#[derive(Clone, Debug)]
pub(crate) struct ManifestBinding {
    pub(crate) stream_url: String,
    pub(crate) extractor_digest: String,
}

impl ManifestIdentity {
    pub(crate) fn matches(&self, binding: &ManifestBinding) -> bool {
        self.version == FORMAT_VERSION
            && self
                .source
                .as_ref()
                .is_some_and(|source| source.stream_url == binding.stream_url)
            && self.extractor_digest.as_deref() == Some(binding.extractor_digest.as_str())
    }
}

impl Manifest {
    pub(crate) fn new(binding: &ManifestBinding, base: &IndexBase, generation: u64) -> Self {
        Self {
            version: FORMAT_VERSION,
            source: SourceBinding {
                stream_url: binding.stream_url.clone(),
                incarnation: base.incarnation.clone(),
            },
            extractor_digest: binding.extractor_digest.clone(),
            generation,
            indexed_from_offset: base.offset,
            floor_offset: base.offset,
            durable_offset: base.offset,
            resync_offset: (base.offset > 0).then_some(base.offset),
            trimmed_bytes: 0,
            skipped: SkipCounts::default(),
            status: IndexStatus::Ready,
            parts: Vec::new(),
        }
    }

    /// Serialize this manifest into its content-addressed object plus the
    /// matching `CURRENT` pointer bytes.
    pub(crate) fn encode(&self) -> Result<(String, Vec<u8>, Vec<u8>), IndexError> {
        let bytes = serde_json::to_vec(self)?;
        let key = format!("manifests/{:020}-{}.json", self.generation, digest(&bytes));
        let pointer_bytes = serde_json::to_vec(&CurrentPointer {
            version: FORMAT_VERSION,
            generation: self.generation,
            manifest: key.clone(),
        })?;
        Ok((key, bytes, pointer_bytes))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CurrentPointer {
    version: u32,
    generation: u64,
    manifest: String,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishedManifest {
    pub(crate) pointer_etag: String,
    pub(crate) manifest_key: String,
    pub(crate) manifest: Manifest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GarbageCollectionReport {
    pub deleted_parts: usize,
    pub deleted_layouts: usize,
    pub deleted_manifests: usize,
    pub deleted_claims: usize,
}

pub(crate) async fn initialize(
    store: &ObjectStore,
    binding: &ManifestBinding,
    base: &IndexBase,
) -> Result<(), IndexError> {
    if store.get(CURRENT_KEY).await?.is_some() {
        return Ok(());
    }
    let (key, bytes, pointer_bytes) = Manifest::new(binding, base, 0).encode()?;
    let _manifest_write = store.put_if_absent(&key, &bytes).await?;
    let _pointer_write = store.put_if_absent(CURRENT_KEY, &pointer_bytes).await?;
    Ok(())
}

pub(crate) async fn load_published(
    store: &ObjectStore,
    binding: &ManifestBinding,
) -> Result<PublishedManifest, IndexError> {
    let current = store
        .get(CURRENT_KEY)
        .await?
        .ok_or_else(|| IndexError::MissingObject(CURRENT_KEY.to_owned()))?;
    let pointer: CurrentPointer = serde_json::from_slice(&current.bytes)?;
    if pointer.version != FORMAT_VERSION {
        return Err(IndexError::ManifestVersion(pointer.version));
    }
    let manifest_object = store
        .get(&pointer.manifest)
        .await?
        .ok_or_else(|| IndexError::MissingObject(pointer.manifest.clone()))?;
    let manifest_hash = pointer
        .manifest
        .strip_prefix("manifests/")
        .and_then(|value| value.strip_suffix(".json"))
        .and_then(|value| value.split_once('-'))
        .map(|(_, hash)| hash)
        .ok_or_else(|| IndexError::InvalidObjectKey(pointer.manifest.clone()))?;
    if digest(&manifest_object.bytes) != manifest_hash {
        return Err(IndexError::ObjectHashMismatch(pointer.manifest));
    }
    let manifest: Manifest = serde_json::from_slice(&manifest_object.bytes)?;
    if manifest.version != FORMAT_VERSION {
        return Err(IndexError::ManifestVersion(manifest.version));
    }
    if manifest.generation != pointer.generation {
        return Err(IndexError::InvalidSourceResponse(
            "CURRENT generation does not match manifest",
        ));
    }
    if manifest.source.stream_url != binding.stream_url {
        return Err(IndexError::SourceMismatch {
            stored: manifest.source.stream_url,
            configured: binding.stream_url.clone(),
        });
    }
    if manifest.extractor_digest != binding.extractor_digest {
        return Err(IndexError::SourceMismatch {
            stored: format!("extractor {}", manifest.extractor_digest),
            configured: format!("extractor {}", binding.extractor_digest),
        });
    }
    if manifest.floor_offset < manifest.indexed_from_offset
        || manifest.durable_offset < manifest.floor_offset
    {
        return Err(IndexError::InvalidSourceResponse(
            "manifest offsets precede the index base",
        ));
    }
    Ok(PublishedManifest {
        pointer_etag: current.etag,
        manifest_key: pointer.manifest,
        manifest,
    })
}

pub(crate) fn manifest_generation(key: &str) -> Option<u64> {
    key.strip_prefix("manifests/")?
        .split_once('-')?
        .0
        .parse()
        .ok()
}
