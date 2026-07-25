//! Verifiable cluster backup, verification, and restore.
//!
//! A backup is a directory (local filesystem or `s3://bucket/prefix`) holding
//! one MessagePack `group-NNNN.snapshot` object per raft group plus a JSON
//! `manifest.json`. The manifest carries a format version independent of the
//! server binary, per-object byte sizes and BLAKE3 checksums, and the group
//! commit index each export observed.
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
//!   not dereference cold objects.

use std::collections::HashMap;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use serde::Deserialize;
use serde::Serialize;
use ursula_stream::StreamSnapshot;
use ursula_stream::StreamStateMachine;

pub const BACKUP_FORMAT_VERSION: u32 = 1;
const MANIFEST_OBJECT: &str = "manifest.json";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Backup format, versioned independently from the server binary.
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
    pub fn open(location: &str) -> Result<Self> {
        let operator = if let Some(rest) = location.strip_prefix("s3://") {
            let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
            if bucket.is_empty() {
                bail!("s3 backup location must name a bucket");
            }
            let mut builder = opendal::services::S3::default().bucket(bucket);
            if !prefix.is_empty() {
                builder = builder.root(prefix);
            }
            opendal::Operator::new(builder)
                .context("configure s3 backup store")?
                .finish()
        } else {
            let builder = opendal::services::Fs::default().root(location);
            opendal::Operator::new(builder)
                .context("configure filesystem backup store")?
                .finish()
        };
        Ok(Self { operator })
    }

    pub async fn write(&self, object: &str, bytes: Vec<u8>) -> Result<()> {
        self.operator
            .write(object, bytes)
            .await
            .with_context(|| format!("write backup object {object}"))?;
        Ok(())
    }

    pub async fn read(&self, object: &str) -> Result<Vec<u8>> {
        Ok(self
            .operator
            .read(object)
            .await
            .with_context(|| format!("read backup object {object}"))?
            .to_vec())
    }
}

fn group_object_name(raft_group_id: u32) -> String {
    format!("group-{raft_group_id:04}.snapshot")
}

#[derive(Debug, Deserialize)]
struct BackupInfo {
    format_version: u32,
    raft_group_count: u32,
}

pub struct BackupClient {
    http: reqwest::Client,
    nodes: Vec<String>,
}

impl BackupClient {
    pub fn new(http: reqwest::Client, nodes: Vec<String>) -> Result<Self> {
        if nodes.is_empty() {
            bail!("at least one node URL is required");
        }
        Ok(Self { http, nodes })
    }

    async fn info(&self) -> Result<BackupInfo> {
        let mut last_error = None;
        for node in &self.nodes {
            let url = format!("{}/__ursula/backup/info", node.trim_end_matches('/'));
            match self.http.get(&url).send().await {
                Ok(response) if response.status().is_success() => {
                    return response.json::<BackupInfo>().await.context("decode info");
                }
                Ok(response) => {
                    last_error = Some(anyhow::anyhow!("{url}: HTTP {}", response.status()));
                }
                Err(err) => last_error = Some(anyhow::anyhow!("{url}: {err}")),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no nodes configured")))
    }

    /// Exports one group, preferring the freshest replica: every node is
    /// asked and the response with the highest commit index wins.
    async fn export_group(&self, raft_group_id: u32) -> Result<(Vec<u8>, u64)> {
        let mut best: Option<(Vec<u8>, u64)> = None;
        let mut last_error = None;
        for node in &self.nodes {
            let url = format!(
                "{}/__ursula/backup/group/{raft_group_id}",
                node.trim_end_matches('/')
            );
            let response = match self.http.get(&url).send().await {
                Ok(response) if response.status().is_success() => response,
                Ok(response) => {
                    last_error = Some(anyhow::anyhow!("{url}: HTTP {}", response.status()));
                    continue;
                }
                Err(err) => {
                    last_error = Some(anyhow::anyhow!("{url}: {err}"));
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
            let body = response.bytes().await.context("read export body")?.to_vec();
            if let Some(declared) = declared {
                let actual = blake3::hash(&body).to_hex().to_string();
                if actual != declared {
                    last_error = Some(anyhow::anyhow!(
                        "{url}: transfer checksum mismatch ({declared} != {actual})"
                    ));
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
        best.ok_or_else(|| {
            last_error.unwrap_or_else(|| anyhow::anyhow!("no node served group {raft_group_id}"))
        })
    }

    async fn import_group(&self, raft_group_id: u32, body: Vec<u8>) -> Result<()> {
        let mut last_error = None;
        for node in &self.nodes {
            let url = format!(
                "{}/__ursula/backup/group/{raft_group_id}/import",
                node.trim_end_matches('/')
            );
            match self
                .http
                .post(&url)
                .header("content-type", "application/x-msgpack")
                .body(body.clone())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(()),
                Ok(response) => {
                    let status = response.status();
                    let detail = response.text().await.unwrap_or_default();
                    // 409 means the target is not empty: retrying another
                    // node cannot help, and the operator must not be told a
                    // half-restore is retryable.
                    if status == reqwest::StatusCode::CONFLICT {
                        bail!("group {raft_group_id}: target not empty: {detail}");
                    }
                    last_error = Some(anyhow::anyhow!("{url}: HTTP {status}: {detail}"));
                }
                Err(err) => last_error = Some(anyhow::anyhow!("{url}: {err}")),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no node accepted group import")))
    }
}

/// Decodes and deeply validates one exported snapshot, returning its shape.
fn validate_snapshot(bytes: &[u8]) -> Result<(u64, u64)> {
    let snapshot: StreamSnapshot =
        rmp_serde::from_slice(bytes).context("decode snapshot MessagePack")?;
    let buckets = u64::try_from(snapshot.buckets.len()).unwrap_or(u64::MAX);
    let streams = u64::try_from(snapshot.streams.len()).unwrap_or(u64::MAX);
    StreamStateMachine::restore(snapshot).context("snapshot failed state-machine validation")?;
    Ok((buckets, streams))
}

pub async fn create(
    client: &BackupClient,
    store: &BackupStore,
    created_unix_ms: u64,
) -> Result<BackupManifest> {
    let info = client.info().await?;
    if info.format_version != BACKUP_FORMAT_VERSION {
        bail!(
            "cluster speaks backup format {}, this tool supports {}",
            info.format_version,
            BACKUP_FORMAT_VERSION
        );
    }
    let mut groups = Vec::with_capacity(info.raft_group_count as usize);
    for raft_group_id in 0..info.raft_group_count {
        let (body, group_commit_index) = client.export_group(raft_group_id).await?;
        let (buckets, streams) = validate_snapshot(&body)
            .with_context(|| format!("group {raft_group_id} export is invalid"))?;
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
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).context("encode manifest")?;
    store.write(MANIFEST_OBJECT, manifest_bytes).await?;
    Ok(manifest)
}

#[derive(Debug)]
pub struct VerifyReport {
    pub groups: u32,
    pub buckets: u64,
    pub streams: u64,
}

pub async fn verify(store: &BackupStore) -> Result<VerifyReport> {
    let manifest_bytes = store.read(MANIFEST_OBJECT).await?;
    let manifest: BackupManifest =
        serde_json::from_slice(&manifest_bytes).context("decode manifest")?;
    if manifest.format_version > BACKUP_FORMAT_VERSION {
        bail!(
            "backup format {} is newer than this tool supports ({})",
            manifest.format_version,
            BACKUP_FORMAT_VERSION
        );
    }
    let expected = u32::try_from(manifest.groups.len()).unwrap_or(u32::MAX);
    if expected != manifest.raft_group_count {
        bail!(
            "manifest lists {} group objects but declares {} raft groups",
            manifest.groups.len(),
            manifest.raft_group_count
        );
    }
    let mut seen = HashMap::new();
    let mut buckets = 0u64;
    let mut streams = 0u64;
    for group in &manifest.groups {
        if seen.insert(group.raft_group_id, ()).is_some() {
            bail!("manifest lists group {} twice", group.raft_group_id);
        }
        let body = store.read(&group.object).await?;
        let bytes = u64::try_from(body.len()).unwrap_or(u64::MAX);
        if bytes != group.bytes {
            bail!(
                "group {}: object is {} bytes, manifest says {}",
                group.raft_group_id,
                bytes,
                group.bytes
            );
        }
        let actual = blake3::hash(&body).to_hex().to_string();
        if actual != group.blake3 {
            bail!(
                "group {}: checksum mismatch ({} != {})",
                group.raft_group_id,
                actual,
                group.blake3
            );
        }
        let (snapshot_buckets, snapshot_streams) = validate_snapshot(&body)
            .with_context(|| format!("group {} snapshot is invalid", group.raft_group_id))?;
        if snapshot_buckets != group.buckets || snapshot_streams != group.streams {
            bail!(
                "group {}: snapshot shape {}b/{}s does not match manifest {}b/{}s",
                group.raft_group_id,
                snapshot_buckets,
                snapshot_streams,
                group.buckets,
                group.streams
            );
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

pub async fn restore(client: &BackupClient, store: &BackupStore) -> Result<VerifyReport> {
    // Never push unverified bytes at a cluster: restore always verifies the
    // whole backup first and fails closed before the first import.
    let report = verify(store).await?;
    let info = client.info().await?;
    let manifest_bytes = store.read(MANIFEST_OBJECT).await?;
    let manifest: BackupManifest =
        serde_json::from_slice(&manifest_bytes).context("decode manifest")?;
    if info.raft_group_count != manifest.raft_group_count {
        bail!(
            "backup has {} raft groups but the target cluster has {}; streams hash by group \
             count, so restore requires an identical target",
            manifest.raft_group_count,
            info.raft_group_count
        );
    }
    for group in &manifest.groups {
        let body = store.read(&group.object).await?;
        client.import_group(group.raft_group_id, body).await?;
    }
    Ok(report)
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

    #[tokio::test]
    async fn verify_fails_closed_on_corruption_and_future_formats() {
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
        assert!(err.to_string().contains("checksum"), "{err}");

        // Future format version.
        let mut manifest = manifest_for(&[(0, &body)]);
        manifest.format_version = BACKUP_FORMAT_VERSION + 1;
        store
            .write(
                MANIFEST_OBJECT,
                serde_json::to_vec(&manifest).expect("encode"),
            )
            .await
            .expect("write manifest");
        let err = verify(&store).await.expect_err("future format rejected");
        assert!(err.to_string().contains("newer"), "{err}");
    }
}
