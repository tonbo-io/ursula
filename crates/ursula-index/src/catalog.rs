use serde::Deserialize;
use serde::Serialize;

use crate::IndexError;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectStore;

const CATALOG_KEY: &str = "CATALOG";
const MAINTENANCE_LEASE_KEY: &str = "maintenance/lease.json";
const CATALOG_VERSION: u32 = 1;
const MAX_CATALOG_ATTEMPTS: usize = 32;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexRegistration {
    pub id: String,
    pub stream_url: String,
    pub timestamp_field: String,
    pub indexed_from_record: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CatalogManifest {
    version: u32,
    registrations: Vec<IndexRegistration>,
    #[serde(default)]
    retired: Vec<RetiredIndexRegistration>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RetiredIndexRegistration {
    registration: IndexRegistration,
    retired_at_ms: u64,
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

    pub async fn register(&self, registration: &IndexRegistration) -> Result<(), IndexError> {
        let registration = canonical_registration(registration)?;
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
            if let Some(existing) = catalog.retired.iter().find(|existing| {
                existing.registration.id == registration.id
                    || existing.registration.stream_url == registration.stream_url
            }) {
                return Err(IndexError::RegistrationConflict(
                    existing.registration.id.clone(),
                ));
            }
            catalog.registrations.push(registration.clone());
            catalog
                .registrations
                .sort_unstable_by(|left, right| left.id.cmp(&right.id));
            let bytes = serde_json::to_vec(&catalog)?;
            let result = match current {
                Some(current) => {
                    self.store
                        .compare_and_swap(CATALOG_KEY, &current.etag, &bytes)
                        .await?
                }
                None => self.store.put_if_absent(CATALOG_KEY, &bytes).await?,
            };
            if matches!(result, ConditionalWrite::Written) {
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
            catalog.retired.push(RetiredIndexRegistration {
                registration,
                retired_at_ms,
            });
            let bytes = serde_json::to_vec(&catalog)?;
            if matches!(
                self.store
                    .compare_and_swap(CATALOG_KEY, &current.etag, &bytes)
                    .await?,
                ConditionalWrite::Written
            ) {
                return Ok(());
            }
        }
        Err(IndexError::PublishConflict)
    }

    pub async fn retired_before(
        &self,
        cutoff_ms: u64,
    ) -> Result<Vec<IndexRegistration>, IndexError> {
        Ok(self
            .load()
            .await?
            .retired
            .into_iter()
            .filter(|retired| retired.retired_at_ms <= cutoff_ms)
            .map(|retired| retired.registration)
            .collect())
    }

    pub async fn forget_retired(&self, id: &str) -> Result<(), IndexError> {
        validate_id(id)?;
        for _attempt in 0..MAX_CATALOG_ATTEMPTS {
            let current = self
                .store
                .get(CATALOG_KEY)
                .await?
                .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
            let mut catalog = decode_catalog(&current.bytes)?;
            let original_len = catalog.retired.len();
            catalog
                .retired
                .retain(|retired| retired.registration.id != id);
            if catalog.retired.len() == original_len {
                return Ok(());
            }
            let bytes = serde_json::to_vec(&catalog)?;
            if matches!(
                self.store
                    .compare_and_swap(CATALOG_KEY, &current.etag, &bytes)
                    .await?,
                ConditionalWrite::Written
            ) {
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
}

fn decode_catalog(bytes: &[u8]) -> Result<CatalogManifest, IndexError> {
    let catalog: CatalogManifest = serde_json::from_slice(bytes)?;
    if catalog.version != CATALOG_VERSION {
        return Err(IndexError::ManifestVersion(catalog.version));
    }
    Ok(catalog)
}

fn same_registration_identity(left: &IndexRegistration, right: &IndexRegistration) -> bool {
    left.id == right.id
        && left.stream_url == right.stream_url
        && left.timestamp_field == right.timestamp_field
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
    if registration.stream_url.is_empty() || registration.timestamp_field.is_empty() {
        return Err(IndexError::InvalidConfig(
            "stream URL and timestamp field must not be empty",
        ));
    }
    let url = validate_stream_url(&registration.stream_url)?;
    Ok(IndexRegistration {
        id: registration.id.clone(),
        stream_url: url.to_string(),
        timestamp_field: registration.timestamp_field.clone(),
        indexed_from_record: registration.indexed_from_record,
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
