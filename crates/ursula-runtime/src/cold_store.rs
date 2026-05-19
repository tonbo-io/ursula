use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use opendal::{Operator, Scheme};
use ursula_shard::BucketStreamId;
use ursula_stream::{ColdChunkRef, ObjectPayloadRef};

pub(crate) const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
static COLD_CHUNK_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct ColdStore {
    operator: Operator,
}

pub type ColdStoreHandle = Arc<ColdStore>;

impl ColdStore {
    pub fn memory() -> io::Result<Self> {
        let operator = Operator::via_iter(Scheme::Memory, [])
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self { operator })
    }

    pub fn fs(root: impl AsRef<Path>) -> io::Result<Self> {
        let root = root.as_ref();
        fs::create_dir_all(root)?;
        let operator = Operator::via_iter(
            Scheme::Fs,
            [("root".to_owned(), root.to_string_lossy().to_string())],
        )
        .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(Self { operator })
    }

    pub fn s3_from_env() -> io::Result<Self> {
        Self::s3_from_env_with_root(None)
    }

    pub fn s3_from_env_with_root(root_override: Option<&str>) -> io::Result<Self> {
        let bucket = std::env::var("URSULA_COLD_S3_BUCKET").map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "URSULA_COLD_S3_BUCKET is required when URSULA_COLD_BACKEND=s3",
            )
        })?;
        if bucket.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "URSULA_COLD_S3_BUCKET must not be empty",
            ));
        }

        let mut builder = opendal::services::S3::default().bucket(&bucket);
        if let Some(root) = root_override {
            if !root.trim().is_empty() {
                builder = builder.root(root);
            }
        } else if let Ok(root) = std::env::var("URSULA_COLD_ROOT")
            && !root.trim().is_empty()
        {
            builder = builder.root(&root);
        }
        if let Ok(region) = std::env::var("URSULA_COLD_S3_REGION")
            && !region.trim().is_empty()
        {
            builder = builder.region(&region);
        }
        if let Ok(endpoint) = std::env::var("URSULA_COLD_S3_ENDPOINT")
            && !endpoint.trim().is_empty()
        {
            builder = builder.endpoint(&endpoint);
        }
        if let Ok(access_key_id) = std::env::var("URSULA_COLD_S3_ACCESS_KEY_ID")
            && !access_key_id.trim().is_empty()
        {
            builder = builder.access_key_id(&access_key_id);
        }
        if let Ok(secret_access_key) = std::env::var("URSULA_COLD_S3_SECRET_ACCESS_KEY")
            && !secret_access_key.trim().is_empty()
        {
            builder = builder.secret_access_key(&secret_access_key);
        }
        if let Ok(session_token) = std::env::var("URSULA_COLD_S3_SESSION_TOKEN")
            && !session_token.trim().is_empty()
        {
            builder = builder.session_token(&session_token);
        }

        Ok(Self {
            operator: Operator::new(builder)
                .map_err(|err| io::Error::other(err.to_string()))?
                .finish(),
        })
    }

    pub fn from_env() -> io::Result<Option<ColdStoreHandle>> {
        let backend = std::env::var("URSULA_COLD_BACKEND")
            .unwrap_or_else(|_| "none".to_owned())
            .to_ascii_lowercase();
        let store = match backend.as_str() {
            "none" | "disabled" | "off" => return Ok(None),
            "memory" | "mem" | "inmem" => Self::memory()?,
            "fs" => {
                let root =
                    std::env::var("URSULA_COLD_ROOT").unwrap_or_else(|_| "data/cold".to_owned());
                Self::fs(root)?
            }
            "s3" => Self::s3_from_env()?,
            other => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unsupported URSULA_COLD_BACKEND '{other}'"),
                ));
            }
        };
        Ok(Some(Arc::new(store)))
    }

    pub async fn write_chunk(&self, path: &str, payload: &[u8]) -> io::Result<u64> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold chunk path must not be empty",
            ));
        }
        self.operator
            .write(path, payload.to_vec())
            .await
            .map_err(|err| cold_store_io_error(path, err))?;
        Ok(u64::try_from(payload.len()).expect("payload len fits u64"))
    }

    pub async fn delete_chunk(&self, path: &str) -> io::Result<()> {
        if path.trim().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cold chunk path must not be empty",
            ));
        }
        self.operator
            .delete(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))
    }

    pub async fn remove_all(&self, path: &str) -> io::Result<()> {
        self.operator
            .remove_all(path)
            .await
            .map_err(|err| cold_store_io_error(path, err))
    }

    pub async fn read_chunk_range(
        &self,
        chunk: &ColdChunkRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        let object = ObjectPayloadRef {
            start_offset: chunk.start_offset,
            end_offset: chunk.end_offset,
            s3_path: chunk.s3_path.clone(),
            object_size: chunk.object_size,
        };
        self.read_object_range(&object, read_start_offset, len)
            .await
    }

    pub async fn read_object_range(
        &self,
        object: &ObjectPayloadRef,
        read_start_offset: u64,
        len: usize,
    ) -> io::Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let len_u64 = u64::try_from(len).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "cold read length exceeds u64")
        })?;
        let read_end = read_start_offset.checked_add(len_u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "cold read range overflow")
        })?;
        if read_start_offset < object.start_offset || read_end > object.end_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cold read range [{read_start_offset}..{read_end}) is outside object segment [{}..{})",
                    object.start_offset, object.end_offset
                ),
            ));
        }
        let object_start = read_start_offset - object.start_offset;
        let object_end = object_start.checked_add(len_u64).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "cold read range overflow")
        })?;
        if object_end > object.object_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold read range [{object_start}..{object_end}) is outside object '{}' size {}",
                    object.s3_path, object.object_size
                ),
            ));
        }
        let bytes = self
            .operator
            .read_with(&object.s3_path)
            .range(object_start..object_end)
            .await
            .map_err(|err| cold_store_io_error(&object.s3_path, err))?
            .to_bytes();
        if bytes.len() != len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold object '{}' returned {} bytes for requested range [{}..{})",
                    object.s3_path,
                    bytes.len(),
                    object_start,
                    object_end
                ),
            ));
        }
        Ok(bytes.to_vec())
    }
}

fn cold_store_io_error(path: &str, err: opendal::Error) -> io::Error {
    io::Error::other(format!("cold object '{path}': {err}"))
}

pub fn new_cold_chunk_path(
    stream_id: &BucketStreamId,
    start_offset: u64,
    end_offset: u64,
) -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "{stream_id}/chunks/{start_offset:016x}-{end_offset:016x}-{unix_nanos:032x}-{sequence:016x}.bin"
    )
}

pub fn new_external_payload_path(stream_id: &BucketStreamId) -> String {
    let unix_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let sequence = COLD_CHUNK_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{stream_id}/external/{unix_nanos:032x}-{sequence:016x}.bin")
}
