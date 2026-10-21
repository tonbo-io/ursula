//! The disk lease's probe.
//!
//! Every [`PROBE_INTERVAL`] the probe rewrites the 8 bytes of
//! [`PROBE_FILE`] at the WAL root in place and syncs them, through the one
//! file the run's start opened under the root's lock ([`open`]), and
//! records the outcome in the node's fence with the time it started
//! ([`ursula_runtime::NodeFence::record_probe`]). It never opens the file
//! by path again: after the disk went away the path may name a later
//! attachment of the volume, and a success there would lift a fence that
//! must stay closed. It runs on a thread of its own in production and as a
//! simulated task under madsim, as the journal writer does, so it never
//! waits behind the hot path and the hot path never waits for it. It ends
//! once the fence closed for good, at the shutdown.

use std::path::Path;
use std::time::Duration;

use ursula_runtime::NodeFence;
use ursula_runtime::node_fence::LeaseChange;
use ursula_runtime::node_fence::ProbeOutcome;
use ursula_runtime::node_fence::fence_now;

use super::disk::Disk;
use super::disk::DiskFile;
use super::disk::JournalDisk;
use super::disk::JournalFile;

/// The file the probe rewrites, at the WAL root.
pub(crate) const PROBE_FILE: &str = "lease.probe";
/// How often the probe writes.
pub const PROBE_INTERVAL: Duration = Duration::from_secs(1);

/// The running probe of one run. Dropping it stops the probe; the fence
/// then stops admitting within its lease.
#[derive(Debug)]
pub(crate) struct LeaseProbe {
    #[cfg(not(madsim))]
    _stop: std::sync::mpsc::Sender<()>,
    #[cfg(not(madsim))]
    _thread: std::thread::JoinHandle<()>,
    #[cfg(madsim)]
    task: crate::rt::task::JoinHandle<()>,
}

#[cfg(madsim)]
impl Drop for LeaseProbe {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Opens the probe file of `root`, creating it when missing. A run's start
/// calls it once, under the root's lock, and fails if it fails.
pub(crate) fn open(root: &Path) -> std::io::Result<DiskFile> {
    Disk::open_overwrite(&root.join(PROBE_FILE))
}

/// One probe: rewrite, sync, record. An error keeps the file: the probe
/// never opens another.
fn probe(file: &mut DiskFile, path: &Path, fence: &NodeFence, beat: &mut u64) {
    let started = fence_now();
    *beat = beat.wrapping_add(1);
    let outcome = match file.overwrite_start(&beat.to_le_bytes()) {
        Ok(()) => ProbeOutcome::Synced,
        Err(error) => {
            tracing::error!(
                path = %path.display(),
                %error,
                "disk lease: the probe did not sync; this node stops all Raft traffic and object \
                 writes until a probe succeeds"
            );
            ProbeOutcome::Failed
        }
    };
    if fence.record_probe(started, outcome, fence_now()) == Some(LeaseChange::Lifted) {
        tracing::warn!(
            path = %path.display(),
            "disk lease: a probe synced again; the node takes part in Raft again"
        );
    }
}

/// Starts the probe of `root` for `fence`, over `file` ([`open`]).
#[cfg(not(madsim))]
pub(crate) fn spawn(root: &Path, file: DiskFile, fence: NodeFence) -> std::io::Result<LeaseProbe> {
    let path = root.join(PROBE_FILE);
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::Builder::new()
        .name("ursula-disk-lease".to_owned())
        .spawn(move || {
            let mut file = file;
            let mut beat = 0_u64;
            while !fence.is_closed() {
                probe(&mut file, &path, &fence, &mut beat);
                match stopped.recv_timeout(PROBE_INTERVAL) {
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                    Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
                }
            }
        })?;
    Ok(LeaseProbe {
        _stop: stop,
        _thread: thread,
    })
}

/// Starts the probe of `root` for `fence`, over `file` ([`open`]), as a
/// simulated task.
#[cfg(madsim)]
pub(crate) fn spawn(root: &Path, file: DiskFile, fence: NodeFence) -> std::io::Result<LeaseProbe> {
    let path = root.join(PROBE_FILE);
    Ok(LeaseProbe {
        task: crate::rt::spawn(async move {
            let mut file = file;
            let mut beat = 0_u64;
            loop {
                crate::rt::time::sleep(PROBE_INTERVAL).await;
                if fence.is_closed() {
                    return;
                }
                probe(&mut file, &path, &fence, &mut beat);
            }
        }),
    })
}
