//! Cold-tier configuration types.
//!
//! All cold-subsystem configuration lives here so it is co-located with the
//! subsystem it describes.  Configuration can be built explicitly or parsed
//! from the process environment via [`ColdConfig::from_env`].

use std::time::Duration;

use crate::env::env_optional_usize;
use crate::env::env_usize;

/// Default cold-read cache size (256 MiB).
pub const DEFAULT_COLD_CACHE_BYTES: usize = 256 * 1024 * 1024;

/// Default cold-read cache block size (1 MiB).
pub const DEFAULT_COLD_CACHE_BLOCK_BYTES: usize = 1024 * 1024;

/// Default number of read-ahead blocks (4).
pub const DEFAULT_COLD_CACHE_READAHEAD_BLOCKS: usize = 4;

/// Default per-S3-op timeout (10 s).
pub const DEFAULT_S3_OP_TIMEOUT_MS: u64 = 10_000;

/// Default S3 retry count (3).
pub const DEFAULT_S3_MAX_RETRIES: usize = 3;

/// Cold-store backend selector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdStorageBackend {
    None,
    Memory,
    S3,
}

/// Top-level cold-tier configuration.
#[derive(Debug, Clone)]
pub struct ColdConfig {
    pub storage: ColdStorageConfig,
    pub cache: Option<ColdCacheConfig>,
    pub worker: ColdWorkerConfig,
}

impl ColdConfig {
    /// Parse cold-tier configuration from the process environment.
    pub fn from_env() -> Self {
        Self {
            storage: ColdStorageConfig::from_env(),
            cache: ColdCacheConfig::from_env(),
            worker: ColdWorkerConfig::from_env(),
        }
    }

    /// Returns `true` when a real cold-storage backend is configured.
    pub fn is_enabled(&self) -> bool {
        self.storage.backend != ColdStorageBackend::None
    }
}

/// Configuration for the cold object store (backend + connection).
#[derive(Debug, Clone)]
pub struct ColdStorageConfig {
    pub backend: ColdStorageBackend,
    pub s3_bucket: Option<String>,
    pub s3_root: Option<String>,
    pub s3_region: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_access_key_id: Option<String>,
    pub s3_secret_access_key: Option<String>,
    pub s3_session_token: Option<String>,
    pub s3_timeout: Duration,
    pub s3_max_retries: usize,
}

impl Default for ColdStorageConfig {
    fn default() -> Self {
        Self {
            backend: ColdStorageBackend::None,
            s3_bucket: None,
            s3_root: None,
            s3_region: None,
            s3_endpoint: None,
            s3_access_key_id: None,
            s3_secret_access_key: None,
            s3_session_token: None,
            s3_timeout: Duration::from_millis(DEFAULT_S3_OP_TIMEOUT_MS),
            s3_max_retries: DEFAULT_S3_MAX_RETRIES,
        }
    }
}

impl ColdStorageConfig {
    /// Parse from the process environment.
    pub fn from_env() -> Self {
        let backend = std::env::var("URSULA_COLD_BACKEND")
            .unwrap_or_else(|_| "none".to_owned())
            .to_ascii_lowercase();

        Self {
            backend: match backend.as_str() {
                "none" | "disabled" | "off" => ColdStorageBackend::None,
                "memory" | "mem" | "inmem" => ColdStorageBackend::Memory,
                "s3" => ColdStorageBackend::S3,
                other => {
                    tracing::warn!("unsupported URSULA_COLD_BACKEND '{other}', defaulting to none");
                    ColdStorageBackend::None
                }
            },
            s3_bucket: std::env::var("URSULA_COLD_S3_BUCKET").ok(),
            s3_root: std::env::var("URSULA_COLD_ROOT").ok(),
            s3_region: std::env::var("URSULA_COLD_S3_REGION").ok(),
            s3_endpoint: std::env::var("URSULA_COLD_S3_ENDPOINT").ok(),
            s3_access_key_id: std::env::var("URSULA_COLD_S3_ACCESS_KEY_ID").ok(),
            s3_secret_access_key: std::env::var("URSULA_COLD_S3_SECRET_ACCESS_KEY").ok(),
            s3_session_token: std::env::var("URSULA_COLD_S3_SESSION_TOKEN").ok(),
            s3_timeout: Duration::from_millis(
                env_optional_usize("URSULA_S3_TIMEOUT_MS")
                    .map(|v| v as u64)
                    .unwrap_or(10_000),
            ),
            s3_max_retries: env_optional_usize("URSULA_S3_MAX_RETRIES").unwrap_or(3),
        }
    }
}

/// Configuration for the optional cold-read cache.
#[derive(Debug, Clone, Copy)]
pub struct ColdCacheConfig {
    pub max_bytes: usize,
    pub block_bytes: usize,
    pub max_readahead_blocks: usize,
}

impl ColdCacheConfig {
    /// Parse from the process environment.  Returns `None` when
    /// `URSULA_COLD_CACHE_BYTES` is `0` or unset.
    pub fn from_env() -> Option<Self> {
        let max_bytes = env_usize("URSULA_COLD_CACHE_BYTES", 256 * 1024 * 1024);
        if max_bytes == 0 {
            return None;
        }
        Some(Self {
            max_bytes,
            block_bytes: env_usize("URSULA_COLD_CACHE_BLOCK_BYTES", 1024 * 1024),
            max_readahead_blocks: env_usize("URSULA_COLD_CACHE_READAHEAD_BLOCKS", 4),
        })
    }
}

/// Configuration for the optional cold-flush and cold-gc background workers.
#[derive(Debug, Clone)]
pub struct ColdWorkerConfig {
    pub flush_interval_ms: usize,
    pub flush_min_hot_bytes: usize,
    pub flush_max_bytes: usize,
    pub flush_max_concurrency: usize,
    pub gc_interval_ms: usize,
    pub gc_max_entries: usize,
}

impl Default for ColdWorkerConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: 1_000,
            flush_min_hot_bytes: 8 * 1024 * 1024,
            flush_max_bytes: 8 * 1024 * 1024,
            flush_max_concurrency: 4,
            gc_interval_ms: 5_000,
            gc_max_entries: 256,
        }
    }
}

impl ColdWorkerConfig {
    /// Parse from the process environment.
    pub fn from_env() -> Self {
        Self {
            flush_interval_ms: env_usize("URSULA_COLD_FLUSH_INTERVAL_MS", 1_000),
            flush_min_hot_bytes: env_usize("URSULA_COLD_FLUSH_MIN_HOT_BYTES", 8 * 1024 * 1024),
            flush_max_bytes: env_usize("URSULA_COLD_FLUSH_MAX_BYTES", 8 * 1024 * 1024),
            flush_max_concurrency: env_usize("URSULA_COLD_FLUSH_MAX_CONCURRENCY", 4),
            gc_interval_ms: env_usize("URSULA_COLD_GC_INTERVAL_MS", 5_000),
            gc_max_entries: env_usize("URSULA_COLD_GC_MAX_ENTRIES_PER_GROUP", 256),
        }
    }
}
