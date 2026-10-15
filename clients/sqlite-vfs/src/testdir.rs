//! Temporary directories for tests that write real files, removed on drop.

use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// A fresh directory under the system's temp dir, unique to the process and the call.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(label: &str) -> TempDir {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("ursula-vfs-{label}-{}-{n}", std::process::id()));
        if dir.exists() {
            std::fs::remove_dir_all(&dir).unwrap();
        }
        std::fs::create_dir_all(&dir).unwrap();
        TempDir(dir)
    }

    pub(crate) fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // A directory left behind in the temp dir is harmless: the test log says so.
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            println!("could not remove {}: {e}", self.0.display());
        }
    }
}
