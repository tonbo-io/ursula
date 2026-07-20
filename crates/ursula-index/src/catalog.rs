use serde::Deserialize;
use serde::Serialize;

use crate::FsObjectStore;
use crate::IndexError;
use crate::S3ObjectStore;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectStore;

const CATALOG_KEY: &str = "CATALOG";
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
}

impl Default for CatalogManifest {
    fn default() -> Self {
        Self {
            version: CATALOG_VERSION,
            registrations: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct IndexCatalog {
    store: ObjectStore,
}

impl IndexCatalog {
    pub fn open_fs(store: FsObjectStore) -> Self {
        Self {
            store: store.into(),
        }
    }

    pub fn open_s3(store: S3ObjectStore) -> Self {
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

    pub async fn unregister(&self, id: &str) -> Result<(), IndexError> {
        validate_id(id)?;
        for _attempt in 0..MAX_CATALOG_ATTEMPTS {
            let current = self
                .store
                .get(CATALOG_KEY)
                .await?
                .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
            let mut catalog = decode_catalog(&current.bytes)?;
            let original_len = catalog.registrations.len();
            catalog
                .registrations
                .retain(|registration| registration.id != id);
            if catalog.registrations.len() == original_len {
                return Err(IndexError::UnknownIndex(id.to_owned()));
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

fn canonical_registration(
    registration: &IndexRegistration,
) -> Result<IndexRegistration, IndexError> {
    validate_id(&registration.id)?;
    if registration.stream_url.is_empty() || registration.timestamp_field.is_empty() {
        return Err(IndexError::InvalidConfig(
            "stream URL and timestamp field must not be empty",
        ));
    }
    let url = reqwest::Url::parse(&registration.stream_url)
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
