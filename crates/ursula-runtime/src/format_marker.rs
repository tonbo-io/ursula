//! Format-epoch markers on the Raft WAL directory and the object-storage
//! namespace.
//!
//! A node classifies both read-only first, then probes its peers, and only
//! then writes the missing markers (object storage first, then the
//! directory). A refused node therefore never stamps a directory or a
//! namespace that a live cluster of another epoch still uses.
//!
//! - Directory: `{raft.wal.path}/raft-log/FORMAT_EPOCH`. Absent, or empty but
//!   for a leftover `FORMAT_EPOCH.tmp`, the directory is fresh. Non-empty
//!   without a marker is E1; another epoch is E2.
//! - Object storage: `URSULA_FORMAT_EPOCH` at the cold root when the cold
//!   backend is S3, otherwise at the S3 snapshot namespace when snapshots
//!   live on S3. A namespace with objects and no marker is E3; another epoch
//!   is E4.

use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use futures_util::TryStreamExt;
use opendal::ErrorKind;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;
use ursula_config::RaftSnapshotBackend;
use ursula_config::config::ColdBackend;
use ursula_stream::FORMAT_EPOCH;
use ursula_stream::UPGRADE_GUIDE_URL;

use crate::ColdConfig;

/// Marker file name inside the Raft WAL directory.
pub const DATA_DIR_MARKER: &str = "FORMAT_EPOCH";
const DATA_DIR_MARKER_TMP: &str = "FORMAT_EPOCH.tmp";
/// Marker key at the root of the object-storage namespace. Bucket ids match
/// `^[a-z0-9_-]{4,64}$`, so no tenant prefix can collide with it.
pub const OBJECT_MARKER: &str = "URSULA_FORMAT_EPOCH";
/// Marker content. `written_by` is informational and never compared.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FormatEpochMarker {
    pub format_epoch: u32,
    pub written_by: String,
}

impl FormatEpochMarker {
    pub fn current() -> Self {
        Self {
            format_epoch: FORMAT_EPOCH,
            written_by: format!("ursula {}", env!("CARGO_PKG_VERSION")),
        }
    }

    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("format-epoch marker serializes")
    }

    fn decode(bytes: &[u8], location: &str) -> io::Result<Self> {
        serde_json::from_slice(bytes).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("format-epoch marker {location} is not readable: {err}"),
            )
        })
    }
}

/// What a read-only classification found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarkerState {
    /// Nothing there yet: the marker is written once every check passed.
    Fresh,
    /// A marker of this binary's epoch.
    Current,
}

fn epoch_mismatch(location: String, marker: &FormatEpochMarker) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "{location} is format epoch {} (written by {}); this binary reads format epoch \
             {FORMAT_EPOCH} only. There is no in-place upgrade: move the data to a new cluster \
             with ursulactl backup-create and restore ({UPGRADE_GUIDE_URL}), or run the \
             release that wrote it",
            marker.format_epoch, marker.written_by
        ),
    )
}

/// Classify the Raft WAL directory without writing anything (E1, E2).
pub fn classify_data_dir(dir: &Path) -> io::Result<MarkerState> {
    let marker_path = dir.join(DATA_DIR_MARKER);
    match fs::read(&marker_path) {
        Ok(bytes) => {
            let marker = FormatEpochMarker::decode(&bytes, &marker_path.display().to_string())?;
            if marker.format_epoch != FORMAT_EPOCH {
                return Err(epoch_mismatch(
                    format!("raft WAL directory '{}'", dir.display()),
                    &marker,
                ));
            }
            return Ok(MarkerState::Current);
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(MarkerState::Fresh),
        Err(err) => return Err(err),
    };
    for entry in entries {
        let entry = entry?;
        if entry.file_name() == DATA_DIR_MARKER_TMP {
            continue;
        }
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "raft WAL directory '{}' holds data without a format marker (written by Ursula \
                 0.5.x or earlier, format epoch 1). This binary reads format epoch \
                 {FORMAT_EPOCH} only; there is no in-place upgrade. Start on an empty \
                 raft.wal.path and a new object-storage prefix, or run the release that wrote \
                 it",
                dir.display()
            ),
        ));
    }
    Ok(MarkerState::Fresh)
}

/// Write the directory marker: temp file, fsync, rename, fsync the
/// directory. A crash before the rename leaves only the temp file, which the
/// next start ignores and replaces.
pub fn write_data_dir_marker(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let tmp: PathBuf = dir.join(DATA_DIR_MARKER_TMP);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(&FormatEpochMarker::current().encode())?;
        file.sync_all()?;
    }
    fs::rename(&tmp, dir.join(DATA_DIR_MARKER))?;
    File::open(dir)?.sync_all()
}

/// The object-storage namespace a node's marker lives in.
#[derive(Clone)]
pub struct FormatEpochNamespace {
    operator: Operator,
    /// Namespace path relative to the operator root: `""` for the cold root,
    /// `"{snapshot prefix}/"` for a snapshot-only namespace.
    namespace: String,
    display: String,
}

impl std::fmt::Debug for FormatEpochNamespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FormatEpochNamespace")
            .field("namespace", &self.namespace)
            .field("display", &self.display)
            .finish_non_exhaustive()
    }
}

impl FormatEpochNamespace {
    /// The namespace this configuration writes to: the cold root when the
    /// cold backend is S3, else the S3 snapshot namespace when snapshots live
    /// on S3, else none (memory cold store with inline snapshots).
    pub fn from_config(
        cold: &ColdConfig,
        snapshot: &ursula_config::RaftSnapshotConfig,
    ) -> io::Result<Option<Self>> {
        let snapshot_backend = crate::resolved_snapshot_backend(snapshot.backend, cold.backend);
        let namespace = if cold.backend == ColdBackend::S3 {
            String::new()
        } else if snapshot_backend == RaftSnapshotBackend::S3 {
            let prefix = snapshot
                .s3_prefix
                .as_deref()
                .unwrap_or("snapshots")
                .trim_matches('/');
            format!("{prefix}/")
        } else {
            return Ok(None);
        };
        let operator = crate::cold_store::s3_operator_for_config(cold)?;
        let bucket = cold
            .s3
            .as_ref()
            .and_then(|s3| s3.bucket.as_deref())
            .unwrap_or_default();
        let root = cold.root.as_deref().unwrap_or_default().trim_matches('/');
        let display = [bucket, root, namespace.trim_end_matches('/')]
            .into_iter()
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("/");
        Ok(Some(Self::new(
            operator,
            namespace,
            format!("s3://{display}"),
        )))
    }

    /// A namespace on an explicit operator (tests use an in-memory one).
    pub fn new(operator: Operator, namespace: impl Into<String>, display: String) -> Self {
        Self {
            operator,
            namespace: namespace.into(),
            display,
        }
    }

    fn marker_key(&self) -> String {
        format!("{}{OBJECT_MARKER}", self.namespace)
    }

    async fn read_marker(&self) -> io::Result<Option<FormatEpochMarker>> {
        let key = self.marker_key();
        match self.operator.read(&key).await {
            Ok(buffer) => Ok(Some(FormatEpochMarker::decode(
                &buffer.to_vec(),
                &format!("{}/{OBJECT_MARKER}", self.display),
            )?)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(io::Error::other(format!(
                "read format-epoch marker {}/{OBJECT_MARKER}: {err}",
                self.display
            ))),
        }
    }

    fn check(&self, marker: &FormatEpochMarker) -> io::Result<MarkerState> {
        if marker.format_epoch == FORMAT_EPOCH {
            Ok(MarkerState::Current)
        } else {
            Err(epoch_mismatch(
                format!("object storage {}", self.display),
                marker,
            ))
        }
    }

    /// Whether the namespace holds anything besides its own entry and the
    /// marker. Stops at the first such entry.
    async fn has_foreign_entry(&self) -> io::Result<bool> {
        let marker_key = self.marker_key();
        let mut lister = self
            .operator
            .lister_with(&self.namespace)
            .recursive(false)
            .await
            .map_err(|err| io::Error::other(format!("list {}: {err}", self.display)))?;
        while let Some(entry) = lister
            .try_next()
            .await
            .map_err(|err| io::Error::other(format!("list {}: {err}", self.display)))?
        {
            let path = entry.path();
            // opendal lists a zero-byte `prefix/` object (and the root) as the
            // namespace's own entry.
            if path.is_empty()
                || path == "/"
                || path == self.namespace
                || path.trim_start_matches('/') == marker_key
            {
                continue;
            }
            return Ok(true);
        }
        Ok(false)
    }

    /// Classify the namespace without writing anything (E3, E4).
    pub async fn classify(&self) -> io::Result<MarkerState> {
        let first = self.read_marker().await?;
        self.classify_after_read(first).await
    }

    async fn classify_after_read(
        &self,
        first: Option<FormatEpochMarker>,
    ) -> io::Result<MarkerState> {
        if let Some(marker) = first {
            return self.check(&marker);
        }
        if !self.has_foreign_entry().await? {
            return Ok(MarkerState::Fresh);
        }
        // A peer may have just written the marker together with its first
        // objects; read it once more before refusing.
        if let Some(marker) = self.read_marker().await? {
            return self.check(&marker);
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "object storage {} is not empty and has no {OBJECT_MARKER} marker (data from \
                 Ursula 0.5.x or earlier, or another application). This binary reads format \
                 epoch {FORMAT_EPOCH} only. Point storage.cold.root (Helm: s3.prefix / \
                 coldStorage.prefix), or storage.snapshot.s3_prefix when only snapshots use \
                 S3, at an empty prefix used only by this cluster",
                self.display
            ),
        ))
    }

    /// Write the marker once every check passed. A lost create race re-reads
    /// the winner's marker and compares it.
    pub async fn write_marker(&self) -> io::Result<()> {
        let key = self.marker_key();
        let bytes = FormatEpochMarker::current().encode();
        let result = if self
            .operator
            .info()
            .full_capability()
            .write_with_if_not_exists
        {
            self.operator
                .write_with(&key, bytes)
                .if_not_exists(true)
                .await
                .map(|_| ())
        } else {
            self.operator.write(&key, bytes).await.map(|_| ())
        };
        match result {
            Ok(()) => Ok(()),
            Err(err)
                if matches!(
                    err.kind(),
                    ErrorKind::AlreadyExists | ErrorKind::ConditionNotMatch
                ) =>
            {
                match self.read_marker().await? {
                    Some(marker) => self.check(&marker).map(|_| ()),
                    None => Err(io::Error::other(format!(
                        "format-epoch marker {}/{OBJECT_MARKER} lost a create race and is gone",
                        self.display
                    ))),
                }
            }
            Err(err) => Err(io::Error::other(format!(
                "write format-epoch marker {}/{OBJECT_MARKER}: {err}",
                self.display
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use opendal::Scheme;

    use super::*;

    fn write_marker_file(dir: &Path, epoch: u32) {
        fs::write(
            dir.join(DATA_DIR_MARKER),
            serde_json::to_vec(&FormatEpochMarker {
                format_epoch: epoch,
                written_by: "ursula test".to_owned(),
            })
            .unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn data_dir_marker_fresh_ok_unmarked_wrong_epoch_and_leftover_tmp() {
        let root = tempfile::tempdir().unwrap();
        // Fresh: the directory does not exist yet, then exists empty.
        let dir = root.path().join("raft-log");
        assert_eq!(classify_data_dir(&dir).unwrap(), MarkerState::Fresh);
        fs::create_dir_all(&dir).unwrap();
        assert_eq!(classify_data_dir(&dir).unwrap(), MarkerState::Fresh);

        // A leftover temp file from a crash before the rename is ignored and
        // replaced.
        fs::write(dir.join(DATA_DIR_MARKER_TMP), b"partial").unwrap();
        assert_eq!(classify_data_dir(&dir).unwrap(), MarkerState::Fresh);
        write_data_dir_marker(&dir).unwrap();
        assert!(!dir.join(DATA_DIR_MARKER_TMP).exists());
        assert_eq!(classify_data_dir(&dir).unwrap(), MarkerState::Current);
        fs::write(dir.join("core-0.wal"), b"wal").unwrap();
        assert_eq!(classify_data_dir(&dir).unwrap(), MarkerState::Current);

        // E1: data without a marker.
        let old = root.path().join("old");
        fs::create_dir_all(&old).unwrap();
        fs::write(old.join("core-0.wal"), b"wal").unwrap();
        let err = classify_data_dir(&old).unwrap_err();
        assert!(err.to_string().contains("without a format marker"), "{err}");

        // E2: another epoch.
        let other = root.path().join("other");
        fs::create_dir_all(&other).unwrap();
        write_marker_file(&other, FORMAT_EPOCH + 1);
        let err = classify_data_dir(&other).unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("is format epoch {}", FORMAT_EPOCH + 1)),
            "{err}"
        );
    }

    fn memory_namespace(operator: &Operator, namespace: &str) -> FormatEpochNamespace {
        FormatEpochNamespace::new(operator.clone(), namespace, "memory://test".to_owned())
    }

    fn memory_operator() -> Operator {
        Operator::via_iter(Scheme::Memory, []).unwrap()
    }

    #[tokio::test]
    async fn object_marker_fresh_ok_unmarked_and_wrong_epoch() {
        let operator = memory_operator();
        let ns = memory_namespace(&operator, "");
        assert_eq!(ns.classify().await.unwrap(), MarkerState::Fresh);
        ns.write_marker().await.unwrap();
        assert_eq!(ns.classify().await.unwrap(), MarkerState::Current);
        operator
            .write("tenant/cold/chunk", b"x".to_vec())
            .await
            .unwrap();
        assert_eq!(ns.classify().await.unwrap(), MarkerState::Current);
        // A second writer that lost the race compares the winner's marker.
        ns.write_marker().await.unwrap();

        // E3: objects without a marker.
        let old = memory_operator();
        old.write("tenant/cold/chunk", b"x".to_vec()).await.unwrap();
        let err = memory_namespace(&old, "").classify().await.unwrap_err();
        assert!(err.to_string().contains("is not empty"), "{err}");

        // E4: another epoch.
        let other = memory_operator();
        other
            .write(
                OBJECT_MARKER,
                serde_json::to_vec(&FormatEpochMarker {
                    format_epoch: FORMAT_EPOCH - 1,
                    written_by: "ursula 0.5.1".to_owned(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let err = memory_namespace(&other, "").classify().await.unwrap_err();
        assert!(
            err.to_string()
                .contains(&format!("is format epoch {}", FORMAT_EPOCH - 1)),
            "{err}"
        );
    }

    #[tokio::test]
    async fn object_marker_ignores_the_namespace_entry_and_covers_a_snapshot_only_namespace() {
        let operator = memory_operator();
        // A zero-byte `snapshots/` object is the namespace's own entry.
        operator.create_dir("snapshots/").await.unwrap();
        // Another application's objects outside the snapshot namespace do not
        // count: a snapshot-only node never requires an empty bucket root.
        operator
            .write("other-app/object", b"x".to_vec())
            .await
            .unwrap();
        let ns = memory_namespace(&operator, "snapshots/");
        assert_eq!(ns.classify().await.unwrap(), MarkerState::Fresh);
        ns.write_marker().await.unwrap();
        operator
            .stat(&format!("snapshots/{OBJECT_MARKER}"))
            .await
            .expect("write_marker stores the object marker");
        assert_eq!(ns.classify().await.unwrap(), MarkerState::Current);

        let unmarked = memory_operator();
        unmarked
            .write("snapshots/group-0/objects/a.snap", b"x".to_vec())
            .await
            .unwrap();
        let err = memory_namespace(&unmarked, "snapshots/")
            .classify()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("is not empty"), "{err}");
    }

    #[tokio::test]
    async fn object_marker_written_between_the_read_and_the_list_is_honoured() {
        // A peer wrote its first objects and the marker after this node's
        // first read: the listing sees objects, the second read the marker.
        let operator = memory_operator();
        let ns = memory_namespace(&operator, "");
        let first = ns.read_marker().await.unwrap();
        assert!(first.is_none());
        operator
            .write("tenant/cold/chunk", b"x".to_vec())
            .await
            .unwrap();
        memory_namespace(&operator, "")
            .write_marker()
            .await
            .unwrap();
        assert_eq!(
            ns.classify_after_read(first).await.unwrap(),
            MarkerState::Current
        );
    }
}
