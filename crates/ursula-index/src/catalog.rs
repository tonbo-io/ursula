use serde::Deserialize;
use serde::Serialize;

use crate::FsObjectStore;
use crate::IndexError;
use crate::S3ObjectStore;
use crate::object_store::ConditionalWrite;
use crate::object_store::ObjectStore;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IndexRegistration {
    pub id: String,
    pub stream_url: String,
    pub timestamp_field: String,
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
        validate_registration(registration)?;
        let source_key = source_key(&registration.stream_url);
        let source_bytes = serde_json::to_vec(&registration.id)?;
        let source_written = match self.store.put_if_absent(&source_key, &source_bytes).await? {
            ConditionalWrite::Written => true,
            ConditionalWrite::Conflict => {
                let existing = self
                    .store
                    .get(&source_key)
                    .await?
                    .ok_or_else(|| IndexError::MissingObject(source_key.clone()))?;
                let existing_id: String = serde_json::from_slice(&existing.bytes)?;
                if existing_id != registration.id {
                    return Err(IndexError::RegistrationConflict(existing_id));
                }
                false
            }
        };
        let key = registration_key(&registration.id);
        let bytes = serde_json::to_vec(registration)?;
        match self.store.put_if_absent(&key, &bytes).await? {
            ConditionalWrite::Written => Ok(()),
            ConditionalWrite::Conflict => {
                let existing = self
                    .store
                    .get(&key)
                    .await?
                    .ok_or_else(|| IndexError::MissingObject(key.clone()))?;
                let existing: IndexRegistration = serde_json::from_slice(&existing.bytes)?;
                if existing == *registration {
                    Ok(())
                } else {
                    if source_written {
                        self.store.delete(&source_key).await?;
                    }
                    Err(IndexError::RegistrationConflict(registration.id.clone()))
                }
            }
        }
    }

    pub async fn get(&self, id: &str) -> Result<IndexRegistration, IndexError> {
        validate_id(id)?;
        let key = registration_key(id);
        let object = self
            .store
            .get(&key)
            .await?
            .ok_or_else(|| IndexError::UnknownIndex(id.to_owned()))?;
        Ok(serde_json::from_slice(&object.bytes)?)
    }

    pub async fn list(&self) -> Result<Vec<IndexRegistration>, IndexError> {
        let mut registrations = Vec::new();
        for object in self.store.list("catalog/").await? {
            if !object.key.ends_with(".json") {
                continue;
            }
            let Some(stored) = self.store.get(&object.key).await? else {
                continue;
            };
            registrations.push(serde_json::from_slice(&stored.bytes)?);
        }
        registrations.sort_unstable_by(|left: &IndexRegistration, right: &IndexRegistration| {
            left.id.cmp(&right.id)
        });
        Ok(registrations)
    }

    pub async fn unregister(&self, id: &str) -> Result<(), IndexError> {
        validate_id(id)?;
        let registration = self.get(id).await?;
        self.store
            .delete(&source_key(&registration.stream_url))
            .await?;
        self.store.delete(&registration_key(id)).await
    }
}

fn registration_key(id: &str) -> String {
    format!("catalog/{id}.json")
}

fn source_key(stream_url: &str) -> String {
    format!(
        "sources/{}.json",
        blake3::hash(stream_url.as_bytes()).to_hex()
    )
}

fn validate_registration(registration: &IndexRegistration) -> Result<(), IndexError> {
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
    Ok(())
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
