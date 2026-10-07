//! Verifiable cluster backup, verification, and restore.
//!
//! A backup is a directory (local filesystem or `s3://bucket/prefix`) holding
//! one MessagePack `group-NNNN.snapshot` object per raft group plus a JSON
//! `manifest.json`. The manifest carries the backup format version
//! ([`BACKUP_FORMAT_VERSION`]), per-object byte sizes and BLAKE3 checksums,
//! and the group commit index each export observed.
//!
//! The backup format is independent of the server's format epoch: a backup
//! is how data moves from one format epoch to the next, so this tool reads
//! the backups of Ursula 0.6 and restores them into a 0.7 cluster
//! (`ursula_stream::format`).
//!
//! Recovery contract (also documented on the docs site):
//!
//! - Each group export is the same deterministic `StreamSnapshot` the raft
//!   snapshot path persists: internally consistent per group while writes
//!   continue. Cross-group consistency is not promised; the recovery boundary
//!   is per stream. Acknowledged writes present in the exporting replica's
//!   applied state are included, whether or not they were cold-flushed.
//! - Restore targets a **fresh, empty** cluster with the same
//!   `raft_group_count`. It replays each snapshot as one replicated write, so
//!   the restored cluster keeps its own raft identity and membership; nothing
//!   from the source cluster's raft metadata is reused. Non-empty targets
//!   fail closed.
//! - Cold-store objects referenced by snapshots are part of the backup set:
//!   the restored cluster must be pointed at the same (or a copied) cold
//!   store namespace. `verify` decodes and validates every snapshot but does
//!   not dereference cold objects. `restore` asks the target cluster to check
//!   every group's references against its cold store before the first
//!   import, and stops without importing anything if an object is missing.

use std::collections::HashSet;

use serde::Deserialize;
use serde::Serialize;
use ursula_proto::admin::BackupColdCheck;
use ursula_proto::admin::BackupInfo;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamSnapshotError;
use ursula_stream::StreamStateMachine;

use crate::MetricsClient;
use crate::NodeInfo;

/// The backup format this tool reads and writes, and the one it requires of
/// a target cluster (E9). Ursula 0.6 and later write format 2. A 0.5.x
/// backup or cluster reports format 1 and is refused.
pub const BACKUP_FORMAT_VERSION: u32 = ursula_stream::BACKUP_FORMAT_VERSION;
const MANIFEST_OBJECT: &str = "manifest.json";
/// The procedure that copies a source cluster's cold objects into the target.
pub const COLD_COPY_GUIDE_URL: &str =
    "https://ursula.tonbo.io/docs/operations#copying-cold-objects";
/// How many missing cold object keys a restore refusal names.
const MISSING_COLD_OBJECT_SAMPLE: usize = 5;

/// Why a backup, verification or restore stopped.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    #[error("s3 backup location '{location}' must name a bucket")]
    MissingBucket { location: String },
    #[error("configure backup store '{location}'")]
    Store {
        location: String,
        #[source]
        source: Box<opendal::Error>,
    },
    #[error("write backup object {object}")]
    WriteObject {
        object: String,
        #[source]
        source: Box<opendal::Error>,
    },
    #[error("read backup object {object}")]
    ReadObject {
        object: String,
        #[source]
        source: Box<opendal::Error>,
    },
    #[error("decode backup manifest")]
    DecodeManifest(#[source] serde_json::Error),
    #[error("encode backup manifest")]
    EncodeManifest(#[source] serde_json::Error),
    /// E9: a manifest of another backup format (Ursula 0.5.x wrote 1).
    #[error(
        "backup manifest format_version {found}; this ursulactl reads and writes backup format \
         {BACKUP_FORMAT_VERSION} only (Ursula 0.6 and later)"
    )]
    UnsupportedManifest { found: u32 },
    /// E9: a target cluster of another backup format, checked before the
    /// first export or import.
    #[error(
        "target cluster speaks backup format {found}; this ursulactl reads and writes backup \
         format {BACKUP_FORMAT_VERSION} only (Ursula 0.6 and later)"
    )]
    UnsupportedCluster { found: u32 },
    #[error("manifest lists {listed} group objects but declares {declared} raft groups")]
    ManifestGroupCount { listed: usize, declared: u32 },
    #[error("manifest lists group {raft_group_id} twice")]
    DuplicateGroup { raft_group_id: u32 },
    #[error("group {raft_group_id}: object is {actual} bytes, manifest says {expected}")]
    SizeMismatch {
        raft_group_id: u32,
        actual: u64,
        expected: u64,
    },
    #[error("group {raft_group_id}: checksum mismatch ({actual} != {expected})")]
    ChecksumMismatch {
        raft_group_id: u32,
        actual: String,
        expected: String,
    },
    #[error(
        "group {raft_group_id}: snapshot shape {buckets}b/{streams}s does not match manifest \
         {manifest_buckets}b/{manifest_streams}s"
    )]
    ShapeMismatch {
        raft_group_id: u32,
        buckets: u64,
        streams: u64,
        manifest_buckets: u64,
        manifest_streams: u64,
    },
    #[error("group {raft_group_id} snapshot does not decode")]
    DecodeSnapshot {
        raft_group_id: u32,
        #[source]
        source: rmp_serde::decode::Error,
    },
    #[error("group {raft_group_id} snapshot failed state-machine validation")]
    InvalidSnapshot {
        raft_group_id: u32,
        #[source]
        source: StreamSnapshotError,
    },
    #[error(
        "backup has {backup} raft groups but the target cluster has {target}; streams hash by \
         group count, so restore requires an identical target"
    )]
    TargetGroupCount { backup: u32, target: u32 },
    #[error(
        "group {raft_group_id}: target process identity is not the pinned restore target: \
         {detail}"
    )]
    TargetIdentity { raft_group_id: u32, detail: String },
    #[error("group {raft_group_id}: target not empty: {detail}")]
    TargetNotEmpty { raft_group_id: u32, detail: String },
    /// The target's cold store lacks objects the backup references. Nothing
    /// was imported.
    #[error(
        "{missing} of the {referenced} cold objects this backup references are missing from the \
         target cluster's cold store (first: {}). Nothing was imported. Copy the source \
         cluster's cold objects into the target's storage.cold.root and run restore again: \
         {COLD_COPY_GUIDE_URL}",
        .sample.join(", ")
    )]
    ColdObjectsMissing {
        missing: u64,
        referenced: u64,
        sample: Vec<String>,
    },
    /// The target answers no cold-object check: it runs a release before
    /// 0.7, which this ursulactl does not restore into.
    #[error(
        "{url}: the target cluster cannot check cold objects (HTTP {status}); restore with the \
         ursulactl of the target's release"
    )]
    ColdCheckUnsupported {
        url: url::Url,
        status: reqwest::StatusCode,
    },
    #[error("admin URL")]
    Url(#[from] url::ParseError),
    #[error("{url}: request failed")]
    Request {
        url: url::Url,
        #[source]
        source: reqwest::Error,
    },
    #[error("{url}: HTTP {status}: {detail}")]
    Status {
        url: url::Url,
        status: reqwest::StatusCode,
        detail: String,
    },
    #[error(
        "group {raft_group_id} from node {node_id}: transfer checksum mismatch ({declared} != \
         {actual})"
    )]
    TransferChecksum {
        raft_group_id: u32,
        node_id: u64,
        declared: String,
        actual: String,
    },
    #[error("prepare an admin request for node {node_id}")]
    AdminRequest {
        node_id: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("at least one node URL is required")]
    NoNodes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Backup format ([`BACKUP_FORMAT_VERSION`]).
    pub format_version: u32,
    /// Caller-supplied wall-clock creation time (unix milliseconds).
    pub created_unix_ms: u64,
    /// Group count of the source cluster; restore requires an identical
    /// target because streams hash to groups by this count.
    pub raft_group_count: u32,
    pub groups: Vec<GroupObject>,
    /// Human-readable reminder that cold-store objects referenced by the
    /// snapshots must remain reachable (same or copied namespace).
    pub cold_store_note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupObject {
    pub raft_group_id: u32,
    pub object: String,
    pub bytes: u64,
    pub blake3: String,
    /// Group commit index observed at export time (freshness indicator).
    pub group_commit_index: u64,
    pub buckets: u64,
    pub streams: u64,
}

/// One backup location: local directory or `s3://bucket/prefix`.
pub struct BackupStore {
    operator: opendal::Operator,
}

impl BackupStore {
    pub fn open(location: &str) -> Result<Self, BackupError> {
        let store_error = |source| BackupError::Store {
            location: location.to_owned(),
            source: Box::new(source),
        };
        let operator = if let Some(rest) = location.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                return Err(BackupError::MissingBucket {
                    location: location.to_owned(),
                });
            }
            let mut builder = opendal::services::S3::default().bucket(bucket);
            if !prefix.is_empty() {
                builder = builder.root(prefix);
            }
            opendal::Operator::new(builder)
                .map_err(store_error)?
                .finish()
        } else {
            let builder = opendal::services::Fs::default().root(location);
            opendal::Operator::new(builder)
                .map_err(store_error)?
                .finish()
        };
        Ok(Self { operator })
    }

    pub async fn write(&self, object: &str, bytes: Vec<u8>) -> Result<(), BackupError> {
        self.operator
            .write(object, bytes)
            .await
            .map_err(|source| BackupError::WriteObject {
                object: object.to_owned(),
                source: Box::new(source),
            })?;
        Ok(())
    }

    pub async fn read(&self, object: &str) -> Result<Vec<u8>, BackupError> {
        Ok(self
            .operator
            .read(object)
            .await
            .map_err(|source| BackupError::ReadObject {
                object: object.to_owned(),
                source: Box::new(source),
            })?
            .to_vec())
    }

    async fn read_manifest(&self) -> Result<BackupManifest, BackupError> {
        let bytes = self.read(MANIFEST_OBJECT).await?;
        serde_json::from_slice(&bytes).map_err(BackupError::DecodeManifest)
    }
}

fn group_object_name(raft_group_id: u32) -> String {
    format!("group-{raft_group_id:04}.snapshot")
}

/// E9: the target cluster must speak this tool's backup format, checked
/// before the first export or import.
fn check_cluster_format(info: &BackupInfo) -> Result<(), BackupError> {
    if info.format_version != BACKUP_FORMAT_VERSION {
        return Err(BackupError::UnsupportedCluster {
            found: info.format_version,
        });
    }
    Ok(())
}

pub struct BackupClient {
    http: reqwest::Client,
    metrics: MetricsClient,
    nodes: Vec<NodeInfo>,
}

impl BackupClient {
    pub fn new(metrics: MetricsClient, nodes: Vec<NodeInfo>) -> Result<Self, BackupError> {
        if nodes.is_empty() {
            return Err(BackupError::NoNodes);
        }
        Ok(Self {
            http: metrics.http_client().clone(),
            metrics,
            nodes,
        })
    }

    async fn info(&self) -> Result<BackupInfo, BackupError> {
        let mut last_error = None;
        for node in &self.nodes {
            let url = node.admin_url.join("/__ursula/backup/info")?;
            match self.http.get(url.clone()).send().await {
                Ok(response) if response.status().is_success() => {
                    return response
                        .json::<BackupInfo>()
                        .await
                        .map_err(|source| BackupError::Request { url, source });
                }
                Ok(response) => {
                    last_error = Some(status_error(url, response).await);
                }
                Err(source) => last_error = Some(BackupError::Request { url, source }),
            }
        }
        Err(last_error.unwrap_or(BackupError::NoNodes))
    }

    /// Exports one group, preferring the freshest replica: every node is
    /// asked and the response with the highest commit index wins.
    async fn export_group(&self, raft_group_id: u32) -> Result<(Vec<u8>, u64), BackupError> {
        let mut best: Option<(Vec<u8>, u64)> = None;
        let mut last_error = None;
        for node in &self.nodes {
            let url = node
                .admin_url
                .join(&format!("/__ursula/backup/group/{raft_group_id}"))?;
            let response = match self.http.get(url.clone()).send().await {
                Ok(response) if response.status().is_success() => response,
                Ok(response) => {
                    last_error = Some(status_error(url, response).await);
                    continue;
                }
                Err(source) => {
                    last_error = Some(BackupError::Request { url, source });
                    continue;
                }
            };
            let commit_index = response
                .headers()
                .get("x-ursula-backup-commit-index")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0);
            let declared = response
                .headers()
                .get("x-ursula-backup-blake3")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned);
            let body = match response.bytes().await {
                Ok(body) => body.to_vec(),
                Err(source) => {
                    last_error = Some(BackupError::Request { url, source });
                    continue;
                }
            };
            if let Some(declared) = declared {
                let actual = blake3::hash(&body).to_hex().to_string();
                if actual != declared {
                    last_error = Some(BackupError::TransferChecksum {
                        raft_group_id,
                        node_id: node.id,
                        declared,
                        actual,
                    });
                    continue;
                }
            }
            if best
                .as_ref()
                .is_none_or(|(_, best_index)| commit_index > *best_index)
            {
                best = Some((body, commit_index));
            }
        }
        best.ok_or_else(|| last_error.unwrap_or(BackupError::NoNodes))
    }

    async fn import_group(&self, raft_group_id: u32, body: Vec<u8>) -> Result<(), BackupError> {
        let mut last_error = None;
        for node in &self.nodes {
            let url = node
                .admin_url
                .join(&format!("/__ursula/backup/group/{raft_group_id}/import"))?;
            let request = self
                .metrics
                .admin_request(node, reqwest::Method::POST, url.clone())
                .await
                .map_err(|source| BackupError::AdminRequest {
                    node_id: node.id,
                    source: source.into(),
                })?;
            match request
                .header("content-type", "application/x-msgpack")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let status = response.status();
                    let detail = response.text().await.unwrap_or_default();
                    // The identity and emptiness refusals are final: retrying
                    // another node cannot help, and the operator must not be
                    // told a half-restore is retryable.
                    if status == reqwest::StatusCode::PRECONDITION_FAILED
                        || status == reqwest::StatusCode::PRECONDITION_REQUIRED
                    {
                        return Err(BackupError::TargetIdentity {
                            raft_group_id,
                            detail,
                        });
                    }
                    if status == reqwest::StatusCode::CONFLICT {
                        return Err(BackupError::TargetNotEmpty {
                            raft_group_id,
                            detail,
                        });
                    }
                    last_error = Some(BackupError::Status {
                        url,
                        status,
                        detail,
                    });
                }
                Err(source) => last_error = Some(BackupError::Request { url, source }),
            }
        }
        Err(last_error.unwrap_or(BackupError::NoNodes))
    }

    /// Asks the target whether its cold store holds every cold object one
    /// group references. Any node answers; they share one cold store.
    async fn check_cold_objects(
        &self,
        raft_group_id: u32,
        body: Vec<u8>,
    ) -> Result<BackupColdCheck, BackupError> {
        let mut last_error = None;
        for node in &self.nodes {
            let url = node.admin_url.join(&format!(
                "/__ursula/backup/group/{raft_group_id}/cold-check"
            ))?;
            let request = self
                .metrics
                .admin_request(node, reqwest::Method::POST, url.clone())
                .await
                .map_err(|source| BackupError::AdminRequest {
                    node_id: node.id,
                    source: source.into(),
                })?;
            match request
                .header("content-type", "application/x-msgpack")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    return response
                        .json::<BackupColdCheck>()
                        .await
                        .map_err(|source| BackupError::Request { url, source });
                }
                Ok(response)
                    if matches!(
                        response.status(),
                        reqwest::StatusCode::NOT_FOUND | reqwest::StatusCode::METHOD_NOT_ALLOWED
                    ) =>
                {
                    return Err(BackupError::ColdCheckUnsupported {
                        url,
                        status: response.status(),
                    });
                }
                Ok(response) => last_error = Some(status_error(url, response).await),
                Err(source) => last_error = Some(BackupError::Request { url, source }),
            }
        }
        Err(last_error.unwrap_or(BackupError::NoNodes))
    }
}

async fn status_error(url: url::Url, response: reqwest::Response) -> BackupError {
    let status = response.status();
    let detail = response.text().await.unwrap_or_default();
    BackupError::Status {
        url,
        status,
        detail,
    }
}

/// Decodes and deeply validates one exported snapshot, returning its shape.
fn validate_snapshot(raft_group_id: u32, bytes: &[u8]) -> Result<(u64, u64), BackupError> {
    let snapshot: StreamSnapshot =
        rmp_serde::from_slice(bytes).map_err(|source| BackupError::DecodeSnapshot {
            raft_group_id,
            source,
        })?;
    let buckets = u64::try_from(snapshot.buckets.len()).unwrap_or(u64::MAX);
    let streams = u64::try_from(snapshot.streams.len()).unwrap_or(u64::MAX);
    StreamStateMachine::restore(snapshot).map_err(|source| BackupError::InvalidSnapshot {
        raft_group_id,
        source,
    })?;
    Ok((buckets, streams))
}

pub async fn create(
    client: &BackupClient,
    store: &BackupStore,
    created_unix_ms: u64,
) -> Result<BackupManifest, BackupError> {
    let info = client.info().await?;
    check_cluster_format(&info)?;
    let mut groups = Vec::with_capacity(info.raft_group_count as usize);
    for raft_group_id in 0..info.raft_group_count {
        let (body, group_commit_index) = client.export_group(raft_group_id).await?;
        let (buckets, streams) = validate_snapshot(raft_group_id, &body)?;
        let object = group_object_name(raft_group_id);
        let checksum = blake3::hash(&body).to_hex().to_string();
        let bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
        store.write(&object, body).await?;
        groups.push(GroupObject {
            raft_group_id,
            object,
            bytes,
            blake3: checksum,
            group_commit_index,
            buckets,
            streams,
        });
    }
    let manifest = BackupManifest {
        format_version: BACKUP_FORMAT_VERSION,
        created_unix_ms,
        raft_group_count: info.raft_group_count,
        groups,
        cold_store_note: "cold-store objects referenced by these snapshots must remain \
                          reachable under the same namespace (or be copied alongside)"
            .to_owned(),
    };
    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).map_err(BackupError::EncodeManifest)?;
    store.write(MANIFEST_OBJECT, manifest_bytes).await?;
    Ok(manifest)
}

#[derive(Debug)]
pub struct VerifyReport {
    pub groups: u32,
    pub buckets: u64,
    pub streams: u64,
}

#[derive(Debug)]
pub struct RestoreReport {
    pub groups: u32,
    pub buckets: u64,
    pub streams: u64,
    /// Cold objects the target confirmed it holds, index pages included.
    pub cold_objects: u64,
}

pub async fn verify(store: &BackupStore) -> Result<VerifyReport, BackupError> {
    let manifest = store.read_manifest().await?;
    if manifest.format_version != BACKUP_FORMAT_VERSION {
        return Err(BackupError::UnsupportedManifest {
            found: manifest.format_version,
        });
    }
    if u32::try_from(manifest.groups.len()).ok() != Some(manifest.raft_group_count) {
        return Err(BackupError::ManifestGroupCount {
            listed: manifest.groups.len(),
            declared: manifest.raft_group_count,
        });
    }
    let mut seen = HashSet::new();
    let mut buckets = 0u64;
    let mut streams = 0u64;
    for group in &manifest.groups {
        if !seen.insert(group.raft_group_id) {
            return Err(BackupError::DuplicateGroup {
                raft_group_id: group.raft_group_id,
            });
        }
        let body = store.read(&group.object).await?;
        let bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
        if bytes != group.bytes {
            return Err(BackupError::SizeMismatch {
                raft_group_id: group.raft_group_id,
                actual: bytes,
                expected: group.bytes,
            });
        }
        let actual = blake3::hash(&body).to_hex().to_string();
        if actual != group.blake3 {
            return Err(BackupError::ChecksumMismatch {
                raft_group_id: group.raft_group_id,
                actual,
                expected: group.blake3.clone(),
            });
        }
        let (snapshot_buckets, snapshot_streams) = validate_snapshot(group.raft_group_id, &body)?;
        if snapshot_buckets != group.buckets || snapshot_streams != group.streams {
            return Err(BackupError::ShapeMismatch {
                raft_group_id: group.raft_group_id,
                buckets: snapshot_buckets,
                streams: snapshot_streams,
                manifest_buckets: group.buckets,
                manifest_streams: group.streams,
            });
        }
        buckets = buckets.saturating_add(snapshot_buckets);
        streams = streams.saturating_add(snapshot_streams);
    }
    Ok(VerifyReport {
        groups: manifest.raft_group_count,
        buckets,
        streams,
    })
}

pub async fn restore(
    client: &BackupClient,
    store: &BackupStore,
) -> Result<RestoreReport, BackupError> {
    // Never push unverified bytes at a cluster: restore always verifies the
    // whole backup first and fails closed before the first import.
    let report = verify(store).await?;
    let info = client.info().await?;
    // E9: never push snapshots into a cluster of another backup format.
    check_cluster_format(&info)?;
    let manifest = store.read_manifest().await?;
    if info.raft_group_count != manifest.raft_group_count {
        return Err(BackupError::TargetGroupCount {
            backup: manifest.raft_group_count,
            target: info.raft_group_count,
        });
    }
    // Every group's cold references must resolve in the target before the
    // first import: an import cannot be taken back, and a restore that
    // imported some groups refuses to run again on the now non-empty target.
    let mut referenced = 0u64;
    let mut missing = 0u64;
    let mut sample = Vec::new();
    for group in &manifest.groups {
        let body = store.read(&group.object).await?;
        let check = client.check_cold_objects(group.raft_group_id, body).await?;
        referenced = referenced.saturating_add(check.referenced_objects);
        missing = missing.saturating_add(check.missing_objects);
        let room = MISSING_COLD_OBJECT_SAMPLE.saturating_sub(sample.len());
        sample.extend(check.missing_sample.into_iter().take(room));
    }
    if missing > 0 {
        return Err(BackupError::ColdObjectsMissing {
            missing,
            referenced,
            sample,
        });
    }
    for group in &manifest.groups {
        let body = store.read(&group.object).await?;
        client.import_group(group.raft_group_id, body).await?;
    }
    Ok(RestoreReport {
        groups: report.groups,
        buckets: report.buckets,
        streams: report.streams,
        cold_objects: referenced,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot_bytes(buckets: Vec<String>) -> Vec<u8> {
        let snapshot = StreamSnapshot {
            buckets,
            ..StreamSnapshot::default()
        };
        rmp_serde::to_vec_named(&snapshot).expect("encode snapshot")
    }

    async fn store_in(dir: &std::path::Path) -> BackupStore {
        BackupStore::open(dir.to_str().expect("utf8 tempdir")).expect("open store")
    }

    fn manifest_for(objects: &[(u32, &[u8])]) -> BackupManifest {
        BackupManifest {
            format_version: BACKUP_FORMAT_VERSION,
            created_unix_ms: 1,
            raft_group_count: u32::try_from(objects.len()).expect("group count"),
            groups: objects
                .iter()
                .map(|(id, body)| GroupObject {
                    raft_group_id: *id,
                    object: group_object_name(*id),
                    bytes: u64::try_from(body.len()).expect("len"),
                    blake3: blake3::hash(body).to_hex().to_string(),
                    group_commit_index: 0,
                    buckets: 1,
                    streams: 0,
                })
                .collect(),
            cold_store_note: String::new(),
        }
    }

    fn fixture_0_6_2() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/backup-0.6.2")
    }

    #[tokio::test]
    async fn verify_accepts_a_well_formed_backup() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path()).await;
        let body = snapshot_bytes(vec!["tenant-a".to_owned()]);
        store
            .write(&group_object_name(0), body.clone())
            .await
            .expect("write group");
        let manifest = manifest_for(&[(0, &body)]);
        store
            .write(
                MANIFEST_OBJECT,
                serde_json::to_vec(&manifest).expect("encode"),
            )
            .await
            .expect("write manifest");

        let report = verify(&store).await.expect("verify");
        assert_eq!(report.groups, 1);
        assert_eq!(report.buckets, 1);
    }

    /// A backup written by Ursula 0.6.2's ursulactl verifies unchanged: the
    /// backup format and the stream snapshot encoding did not change in 0.7.
    #[tokio::test]
    async fn verify_accepts_a_backup_written_by_ursula_0_6_2() {
        let store = store_in(&fixture_0_6_2()).await;
        let report = verify(&store).await.expect("a 0.6.2 backup verifies");
        assert_eq!(report.groups, 4);
        assert_eq!(report.buckets, 4);
        assert_eq!(report.streams, 8);
    }

    #[tokio::test]
    async fn verify_fails_closed_on_corruption_and_other_formats() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path()).await;
        let body = snapshot_bytes(vec!["tenant-a".to_owned()]);
        store
            .write(&group_object_name(0), body.clone())
            .await
            .expect("write group");

        // Corrupt checksum.
        let mut manifest = manifest_for(&[(0, &body)]);
        manifest.groups[0].blake3 = "not-a-checksum".to_owned();
        store
            .write(
                MANIFEST_OBJECT,
                serde_json::to_vec(&manifest).expect("encode"),
            )
            .await
            .expect("write manifest");
        let err = verify(&store).await.expect_err("corrupt checksum rejected");
        assert!(
            matches!(err, BackupError::ChecksumMismatch {
                raft_group_id: 0,
                ..
            }),
            "{err}"
        );

        // E9: a manifest of another backup format (Ursula 0.5.x wrote 1).
        for found in [BACKUP_FORMAT_VERSION - 1, BACKUP_FORMAT_VERSION + 1] {
            let mut manifest = manifest_for(&[(0, &body)]);
            manifest.format_version = found;
            store
                .write(
                    MANIFEST_OBJECT,
                    serde_json::to_vec(&manifest).expect("encode"),
                )
                .await
                .expect("write manifest");
            let err = verify(&store).await.expect_err("other format rejected");
            assert!(
                matches!(err, BackupError::UnsupportedManifest { found: got } if got == found),
                "{err}"
            );
        }

        // A group snapshot of another stream snapshot version.
        let body = rmp_serde::to_vec_named(&StreamSnapshot {
            version: ursula_stream::STREAM_SNAPSHOT_VERSION + 1,
            buckets: vec!["tenant-a".to_owned()],
            ..StreamSnapshot::default()
        })
        .expect("encode snapshot");
        store
            .write(&group_object_name(0), body.clone())
            .await
            .expect("write group");
        store
            .write(
                MANIFEST_OBJECT,
                serde_json::to_vec(&manifest_for(&[(0, &body)])).expect("encode"),
            )
            .await
            .expect("write manifest");
        let err = verify(&store).await.expect_err("other version rejected");
        assert!(
            matches!(err, BackupError::InvalidSnapshot {
                raft_group_id: 0,
                source: StreamSnapshotError::UnsupportedVersion { .. },
            }),
            "{err}"
        );
    }

    /// E9: restore checks the target's format before the first import, so
    /// this ursulactl never pushes snapshots into a 0.5.x cluster.
    #[tokio::test]
    async fn restore_refuses_a_cluster_of_another_format_before_any_import() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use axum::Json;
        use axum::Router;
        use axum::routing::get;
        use axum::routing::post;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path()).await;
        let body = snapshot_bytes(vec!["tenant-a".to_owned()]);
        store
            .write(&group_object_name(0), body.clone())
            .await
            .expect("write group");
        let manifest = manifest_for(&[(0, &body)]);
        store
            .write(
                MANIFEST_OBJECT,
                serde_json::to_vec(&manifest).expect("encode"),
            )
            .await
            .expect("write manifest");

        let imports = Arc::new(AtomicUsize::new(0));
        let counted = imports.clone();
        let app = Router::new()
            .route(
                "/__ursula/backup/info",
                get(|| async {
                    Json(serde_json::json!({"format_version": 1, "raft_group_count": 1}))
                }),
            )
            .route(
                "/__ursula/backup/group/{group}/import",
                post(move || {
                    let counted = counted.clone();
                    async move {
                        counted.fetch_add(1, Ordering::SeqCst);
                        "ok"
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = BackupClient::new(
            MetricsClient::new(std::time::Duration::from_secs(1)).unwrap(),
            vec![NodeInfo {
                id: 1,
                admin_url: format!("http://{address}").parse().unwrap(),
                host: address.to_string(),
                http_url: None,
                metrics_url: None,
                expected_process_incarnation: None,
                expected_maintenance_fence: None,
            }],
        )
        .unwrap();

        let err = restore(&client, &store)
            .await
            .expect_err("a format-1 cluster is refused");
        assert!(
            matches!(err, BackupError::UnsupportedCluster { found: 1 }),
            "{err}"
        );
        assert_eq!(imports.load(Ordering::SeqCst), 0);
    }

    /// Restore asks the target to check every group's cold references
    /// before the first import. Missing objects, or a target that cannot
    /// check, stop it with nothing imported.
    #[tokio::test]
    async fn restore_imports_nothing_when_the_target_lacks_cold_objects() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use axum::Json;
        use axum::Router;
        use axum::http::StatusCode;
        use axum::routing::get;
        use axum::routing::post;

        let dir = tempfile::tempdir().expect("tempdir");
        let store = store_in(dir.path()).await;
        let first = snapshot_bytes(vec!["tenant-a".to_owned()]);
        let second = snapshot_bytes(vec!["tenant-b".to_owned()]);
        for (id, body) in [(0, &first), (1, &second)] {
            store
                .write(&group_object_name(id), body.clone())
                .await
                .expect("write group");
        }
        let manifest = manifest_for(&[(0, &first), (1, &second)]);
        store
            .write(
                MANIFEST_OBJECT,
                serde_json::to_vec(&manifest).expect("encode"),
            )
            .await
            .expect("write manifest");

        for cold_check_supported in [true, false] {
            let imports = Arc::new(AtomicUsize::new(0));
            let counted = imports.clone();
            let mut app = Router::new()
                .route(
                    "/__ursula/backup/info",
                    get(|| async {
                        Json(BackupInfo {
                            format_version: BACKUP_FORMAT_VERSION,
                            raft_group_count: 2,
                        })
                    }),
                )
                .route(
                    "/__ursula/metrics",
                    get(|| async {
                        Json(serde_json::json!({
                            "process_node_id": 1,
                            "process_incarnation": "00000000000000000000000000000001"
                        }))
                    }),
                )
                .route(
                    "/__ursula/backup/group/{group}/import",
                    post(move || {
                        let counted = counted.clone();
                        async move {
                            counted.fetch_add(1, Ordering::SeqCst);
                            StatusCode::OK
                        }
                    }),
                );
            if cold_check_supported {
                app = app.route(
                    "/__ursula/backup/group/{group}/cold-check",
                    post(
                        |axum::extract::Path(group): axum::extract::Path<u32>| async move {
                            Json(BackupColdCheck {
                                raft_group_id: group,
                                referenced_objects: 4,
                                missing_objects: u64::from(group),
                                missing_sample: (0..group)
                                    .map(|index| format!("tenant-b/cold/chunk-{index}"))
                                    .collect(),
                            })
                        },
                    ),
                );
            }
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let client = BackupClient::new(
                MetricsClient::new(std::time::Duration::from_secs(1)).unwrap(),
                vec![NodeInfo {
                    id: 1,
                    admin_url: format!("http://{address}").parse().unwrap(),
                    host: address.to_string(),
                    http_url: None,
                    metrics_url: None,
                    expected_process_incarnation: None,
                    expected_maintenance_fence: None,
                }],
            )
            .unwrap();

            let err = restore(&client, &store)
                .await
                .expect_err("an incomplete cold store is refused");
            if cold_check_supported {
                assert!(
                    matches!(
                        &err,
                        BackupError::ColdObjectsMissing {
                            missing: 1,
                            referenced: 8,
                            sample,
                        } if sample == &["tenant-b/cold/chunk-0".to_owned()]
                    ),
                    "{err}"
                );
            } else {
                assert!(
                    matches!(err, BackupError::ColdCheckUnsupported {
                        status: reqwest::StatusCode::NOT_FOUND,
                        ..
                    }),
                    "{err}"
                );
            }
            assert_eq!(imports.load(Ordering::SeqCst), 0, "nothing imported");
            server.abort();
        }
    }

    #[tokio::test]
    async fn import_identity_failure_never_hops_to_another_restore_target() {
        use std::sync::Arc;
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use axum::Router;
        use axum::http::StatusCode;
        use axum::routing::get;
        use axum::routing::post;
        let attempts = Arc::new(AtomicUsize::new(0));
        let received = attempts.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route(
                        "/__ursula/metrics",
                        get(|| async {
                            axum::Json(serde_json::json!({
                    "process_node_id":1, "process_incarnation":"00000000000000000000000000000001"}))
                        }),
                    )
                    .route(
                        "/__ursula/backup/group/0/import",
                        post(move |headers: axum::http::HeaderMap| {
                            let received = received.clone();
                            async move {
                                assert_eq!(
                                    headers[ursula_proto::admin::PROCESS_INCARNATION_HEADER],
                                    "00000000000000000000000000000001"
                                );
                                received.fetch_add(1, Ordering::SeqCst);
                                StatusCode::PRECONDITION_FAILED
                            }
                        }),
                    ),
            )
            .await
            .unwrap();
        });
        let node = NodeInfo {
            id: 1,
            admin_url: format!("http://{address}").parse().unwrap(),
            host: address.to_string(),
            http_url: None,
            metrics_url: None,
            expected_process_incarnation: None,
            expected_maintenance_fence: None,
        };
        let metrics = MetricsClient::new(std::time::Duration::from_secs(1)).unwrap();
        let client = BackupClient::new(metrics, vec![node.clone(), node]).unwrap();
        let error = client.import_group(0, vec![]).await.unwrap_err();
        assert!(
            matches!(error, BackupError::TargetIdentity {
                raft_group_id: 0,
                ..
            }),
            "{error}"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        task.abort();
    }
}
