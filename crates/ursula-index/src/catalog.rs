use serde::Deserialize;
use serde::Serialize;

use crate::IndexError;
use crate::extract::Extractor;
use crate::extract::ExtractorConfig;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectStore;
use crate::store::offset_string;

const CATALOG_KEY: &str = "CATALOG";
const MAINTENANCE_LEASE_KEY: &str = "maintenance/lease.json";
const CATALOG_VERSION: u32 = 2;
const MAX_CATALOG_ATTEMPTS: usize = 32;

/// Where a new registration starts reading the source. A restart after the
/// source is recreated always covers the new stream from its first byte.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum StartPosition {
    /// The source's retained offset: index all history still readable.
    #[default]
    Retained,
    /// The source's tail: index only what is appended from now on.
    Tail,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexRegistration {
    pub id: String,
    pub stream_url: String,
    pub extract: ExtractorConfig,
    #[serde(default)]
    pub start: StartPosition,
    #[serde(with = "offset_string")]
    pub indexed_from_offset: u64,
    /// The source's `Stream-Incarnation` when the registration (or its last
    /// restart) was made.
    pub incarnation: Option<String>,
    /// Set when a recreated source restarted this registration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restarted_from_incarnation: Option<String>,
}

impl IndexRegistration {
    /// The object namespace of this registration:
    /// `{id}-{url hash}-{incarnation}-{extractor digest}`. A restart under a
    /// new incarnation therefore never reuses the old namespace.
    pub fn namespace(&self) -> Result<String, IndexError> {
        let extractor = Extractor::new(self.extract.clone())?;
        let url_hash = blake3::hash(self.stream_url.as_bytes()).to_hex();
        let incarnation = match self.incarnation.as_deref() {
            None => "none".to_owned(),
            Some(value)
                if !value.is_empty()
                    && value.len() <= 40
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') =>
            {
                value.to_owned()
            }
            Some(value) => blake3::hash(value.as_bytes())
                .to_hex()
                .chars()
                .take(16)
                .collect(),
        };
        Ok(format!(
            "{}-{}-{incarnation}-{}",
            self.id,
            url_hash.chars().take(16).collect::<String>(),
            extractor.digest().chars().take(16).collect::<String>()
        ))
    }
}

/// A namespace that stopped receiving writes and is deleted after the GC
/// grace period. Tombstones are keyed by namespace, which leaves out `start`:
/// a registration may be re-created under a new incarnation or extractor
/// immediately, but not with only a different `start`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RetiredNamespace {
    pub id: String,
    pub namespace: String,
    pub retired_at_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CatalogManifest {
    version: u32,
    registrations: Vec<IndexRegistration>,
    #[serde(default)]
    retired: Vec<RetiredNamespace>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MaintenanceLease {
    worker_id: String,
    expires_at_ms: u64,
}

impl Default for CatalogManifest {
    fn default() -> Self {
        Self {
            version: CATALOG_VERSION,
            registrations: Vec::new(),
            retired: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct IndexCatalog {
    store: ObjectStore,
}

impl IndexCatalog {
    pub fn new(store: impl Into<ObjectStore>) -> Self {
        Self {
            store: store.into(),
        }
    }

    /// Register idempotently: a retry with the same identity (id, URL,
    /// extractor, start and incarnation) succeeds; anything else that
    /// reuses the id, the stream, or a retired namespace conflicts.
    pub async fn register(&self, registration: &IndexRegistration) -> Result<(), IndexError> {
        let registration = canonical_registration(registration)?;
        let namespace = registration.namespace()?;
        for _attempt in 0..MAX_CATALOG_ATTEMPTS {
            let current = self.store.get(CATALOG_KEY).await?;
            let mut catalog = match &current {
                Some(current) => decode_catalog(&current.bytes)?,
                None => CatalogManifest::default(),
            };
            if let Some(existing) = catalog
                .registrations
                .iter()
                .find(|existing| existing.id == registration.id)
            {
                return if same_registration_identity(existing, &registration) {
                    Ok(())
                } else {
                    Err(IndexError::RegistrationConflict(registration.id.clone()))
                };
            }
            if let Some(existing) = catalog
                .registrations
                .iter()
                .find(|existing| existing.stream_url == registration.stream_url)
            {
                return Err(IndexError::RegistrationConflict(existing.id.clone()));
            }
            if let Some(existing) = catalog
                .retired
                .iter()
                .find(|existing| existing.namespace == namespace)
            {
                return Err(IndexError::NamespaceRetired(existing.id.clone()));
            }
            catalog.registrations.push(registration.clone());
            catalog
                .registrations
                .sort_unstable_by(|left, right| left.id.cmp(&right.id));
            if self.write(current.as_ref(), &catalog).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub async fn get(&self, id: &str) -> Result<IndexRegistration, IndexError> {
        validate_id(id)?;
        self.load()
            .await?
            .registrations
            .into_iter()
            .find(|registration| registration.id == id)
            .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))
    }

    pub async fn list(&self) -> Result<Vec<IndexRegistration>, IndexError> {
        Ok(self.load().await?.registrations)
    }

    /// Restart a registration whose source was deleted and recreated:
    /// retire its namespace and rebind it to `incarnation` from
    /// `indexed_from_offset`. A no-op returning the current registration if
    /// it no longer has `expected_incarnation` (another pod restarted it) or
    /// if `incarnation` names a namespace this catalog already retired: a
    /// stale HEAD can report an earlier incarnation again, and restarting
    /// into a retired namespace would let cleanup delete a live index.
    pub async fn restart(
        &self,
        id: &str,
        expected_incarnation: Option<&str>,
        incarnation: Option<String>,
        indexed_from_offset: u64,
        retired_at_ms: u64,
    ) -> Result<IndexRegistration, IndexError> {
        validate_id(id)?;
        for _attempt in 0..MAX_CATALOG_ATTEMPTS {
            let current = self
                .store
                .get(CATALOG_KEY)
                .await?
                .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
            let mut catalog = decode_catalog(&current.bytes)?;
            let registration = catalog
                .registrations
                .iter_mut()
                .find(|registration| registration.id == id)
                .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
            if registration.incarnation.as_deref() != expected_incarnation {
                return Ok(registration.clone());
            }
            let mut restarted = registration.clone();
            restarted.restarted_from_incarnation = restarted.incarnation.take();
            restarted.incarnation = incarnation.clone();
            restarted.indexed_from_offset = indexed_from_offset;
            let namespace = restarted.namespace()?;
            if catalog
                .retired
                .iter()
                .any(|retired| retired.namespace == namespace)
            {
                return Ok(registration.clone());
            }
            let retired = RetiredNamespace {
                id: id.to_owned(),
                namespace: registration.namespace()?,
                retired_at_ms,
            };
            *registration = restarted.clone();
            catalog.retired.push(retired);
            if self.write(Some(&current), &catalog).await? {
                return Ok(restarted);
            }
        }
        Err(IndexError::PublishConflict)
    }

    /// Elect one pool replica to compact and garbage-collect all indexes.
    /// The owner renews only after half the lease has elapsed so S3 versioning
    /// does not turn the coordination object itself into high-frequency churn.
    pub async fn acquire_maintenance_lease(
        &self,
        worker_id: &str,
        now_ms: u64,
        lease_ms: u64,
    ) -> Result<bool, IndexError> {
        if worker_id.is_empty() || lease_ms == 0 {
            return Err(IndexError::InvalidConfig(
                "maintenance worker id and lease duration must be non-empty",
            ));
        }
        let current = self.store.get(MAINTENANCE_LEASE_KEY).await?;
        if let Some(current) = &current {
            let lease: MaintenanceLease = serde_json::from_slice(&current.bytes)?;
            if lease.worker_id != worker_id && lease.expires_at_ms > now_ms {
                return Ok(false);
            }
            let renewal_threshold = now_ms.saturating_add(lease_ms / 2);
            if lease.worker_id == worker_id && lease.expires_at_ms > renewal_threshold {
                return Ok(true);
            }
        }
        let bytes = serde_json::to_vec(&MaintenanceLease {
            worker_id: worker_id.to_owned(),
            expires_at_ms: now_ms.saturating_add(lease_ms),
        })?;
        let write = match current {
            Some(current) => {
                self.store
                    .compare_and_swap(MAINTENANCE_LEASE_KEY, &current.etag, &bytes)
                    .await?
            }
            None => {
                self.store
                    .put_if_absent(MAINTENANCE_LEASE_KEY, &bytes)
                    .await?
            }
        };
        Ok(matches!(write, ConditionalWrite::Written))
    }

    pub async fn unregister(&self, id: &str, retired_at_ms: u64) -> Result<(), IndexError> {
        validate_id(id)?;
        for _attempt in 0..MAX_CATALOG_ATTEMPTS {
            let current = self
                .store
                .get(CATALOG_KEY)
                .await?
                .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
            let mut catalog = decode_catalog(&current.bytes)?;
            let position = catalog
                .registrations
                .iter()
                .position(|registration| registration.id == id)
                .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
            let registration = catalog.registrations.remove(position);
            catalog.retired.push(RetiredNamespace {
                id: registration.id.clone(),
                namespace: registration.namespace()?,
                retired_at_ms,
            });
            if self.write(Some(&current), &catalog).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub async fn retired_before(
        &self,
        cutoff_ms: u64,
    ) -> Result<Vec<RetiredNamespace>, IndexError> {
        Ok(self
            .load()
            .await?
            .retired
            .into_iter()
            .filter(|retired| retired.retired_at_ms <= cutoff_ms)
            .collect())
    }

    pub async fn forget_retired(&self, namespace: &str) -> Result<(), IndexError> {
        for _attempt in 0..MAX_CATALOG_ATTEMPTS {
            let Some(current) = self.store.get(CATALOG_KEY).await? else {
                return Ok(());
            };
            let mut catalog = decode_catalog(&current.bytes)?;
            let original_len = catalog.retired.len();
            catalog
                .retired
                .retain(|retired| retired.namespace != namespace);
            if catalog.retired.len() == original_len {
                return Ok(());
            }
            if self.write(Some(&current), &catalog).await? {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    async fn load(&self) -> Result<CatalogManifest, IndexError> {
        match self.store.get(CATALOG_KEY).await? {
            Some(stored) => decode_catalog(&stored.bytes),
            None => Ok(CatalogManifest::default()),
        }
    }

    /// Conditionally replace `current` (or create the catalog).
    async fn write(
        &self,
        current: Option<&crate::object_store::StoredObject>,
        catalog: &CatalogManifest,
    ) -> Result<bool, IndexError> {
        let bytes = serde_json::to_vec(catalog)?;
        let result = match current {
            Some(current) => {
                self.store
                    .compare_and_swap(CATALOG_KEY, &current.etag, &bytes)
                    .await?
            }
            None => self.store.put_if_absent(CATALOG_KEY, &bytes).await?,
        };
        Ok(matches!(result, ConditionalWrite::Written))
    }
}

fn decode_catalog(bytes: &[u8]) -> Result<CatalogManifest, IndexError> {
    #[derive(Deserialize)]
    struct Version {
        version: u32,
    }
    let version: Version = serde_json::from_slice(bytes)?;
    if version.version != CATALOG_VERSION {
        return Err(IndexError::ManifestVersion(version.version));
    }
    Ok(serde_json::from_slice(bytes)?)
}

fn same_registration_identity(left: &IndexRegistration, right: &IndexRegistration) -> bool {
    left.id == right.id
        && left.stream_url == right.stream_url
        && left.extract == right.extract
        && left.start == right.start
        && left.incarnation == right.incarnation
}

/// Parse a registered source stream URL, rejecting anything that is not
/// credential-free HTTP(S) without a fragment.
pub fn validate_stream_url(value: &str) -> Result<reqwest::Url, IndexError> {
    let url = reqwest::Url::parse(value)
        .map_err(|_error| IndexError::InvalidConfig("stream URL is invalid"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(IndexError::InvalidConfig(
            "stream URL must be credential-free HTTP(S) without a fragment",
        ));
    }
    Ok(url)
}

fn canonical_registration(
    registration: &IndexRegistration,
) -> Result<IndexRegistration, IndexError> {
    validate_id(&registration.id)?;
    let url = validate_stream_url(&registration.stream_url)?;
    let _validated = Extractor::new(registration.extract.clone())?;
    Ok(IndexRegistration {
        stream_url: url.to_string(),
        ..registration.clone()
    })
}

fn validate_id(id: &str) -> Result<(), IndexError> {
    if id.is_empty()
        || id.len() > 122
        || !id.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_')
        })
    {
        return Err(IndexError::InvalidConfig(
            "index id must be 1-122 lowercase letters, digits, '-' or '_'",
        ));
    }
    Ok(())
}
