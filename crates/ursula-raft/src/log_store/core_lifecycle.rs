//! Serialized group reclamation and process-local log-store owner leases.
//! The managed caller must persist its retirement assignment and stop its
//! engine before reclamation. This is not a membership or snapshot certificate.

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

use super::CoreFileLogWrite;
use super::CoreFileLogWriter;
use super::RaftGroupFileLogHandle;
use super::RaftGroupFileLogStore;
use super::RaftGroupLogStoreInner;
use super::load_log_store_inners_from_core_journal;
#[cfg(not(madsim))]
use super::rewrite_core_journal;

#[cfg_attr(madsim, allow(dead_code))]
#[derive(Debug)]
pub(super) enum CoreFileLogCommand {
    Append(CoreFileLogWrite),
    Recover {
        group_id: u32,
        response: mpsc::Sender<Result<RaftGroupLogStoreInner, String>>,
    },
    Reclaim {
        group_id: u32,
        response: mpsc::Sender<Result<(u64, u64), String>>,
    },
}

impl CoreFileLogCommand {
    pub(super) fn reject(self, reason: &str) {
        match self {
            Self::Append(request) => {
                let _ = request.response_tx.send(Err(reason.to_owned()));
            }
            Self::Recover { response, .. } => {
                let _ = response.send(Err(reason.to_owned()));
            }
            Self::Reclaim { response, .. } => {
                let _ = response.send(Err(reason.to_owned()));
            }
        }
    }
}

impl CoreFileLogWriter {
    pub(super) fn open_group(
        &self,
        group_id: u32,
    ) -> io::Result<(RaftGroupLogStoreInner, Arc<AtomicBool>)> {
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| io::Error::other("core WAL leases poisoned"))?;
        let inner = if let Some(previous) = leases.get(&group_id) {
            if previous.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "core WAL group already has a live owner",
                ));
            }
            // An in-process reopen must read writes made since initial startup.
            // Consuming the initial recovery cache twice would lose them.
            let (response, receive) = mpsc::channel();
            self.send_control(CoreFileLogCommand::Recover { group_id, response })?;
            receive
                .recv()
                .map_err(|_| io::Error::other("core WAL recovery reply lost"))?
                .map_err(io::Error::other)?
        } else {
            self.take_recovered(group_id)?
        };
        let lease = Arc::new(AtomicBool::new(true));
        leases.insert(group_id, lease.clone());
        Ok((inner, lease))
    }

    /// Serializes with other groups' writes, invalidates the old store's lease
    /// permanently, and fsyncs a replacement containing only retained groups.
    /// Repeated reclamation is safe. Reopening creates a different owner lease.
    pub(crate) fn reclaim_stopped_group(&self, group_id: u32) -> io::Result<(u64, u64)> {
        let mut leases = self
            .leases
            .lock()
            .map_err(|_| io::Error::other("core WAL leases poisoned"))?;
        leases
            .entry(group_id)
            .or_insert_with(|| Arc::new(AtomicBool::new(false)))
            .store(false, Ordering::Release);
        let (response, receive) = mpsc::channel();
        self.send_control(CoreFileLogCommand::Reclaim { group_id, response })?;
        let sizes = receive
            .recv()
            .map_err(|_| io::Error::other("core WAL reclamation reply lost"))?
            .map_err(io::Error::other)?;
        self.recovered
            .lock()
            .map_err(|_| io::Error::other("core WAL recovery poisoned"))?
            .remove(&group_id);
        Ok(sizes)
    }

    fn send_control(&self, command: CoreFileLogCommand) -> io::Result<()> {
        self.tx
            .as_ref()
            .ok_or_else(|| io::Error::other("core WAL is closing"))?
            .send(command)
            .map_err(|_| io::Error::other("core WAL writer closed"))
    }
}

impl Drop for RaftGroupFileLogStore {
    fn drop(&mut self) {
        if let Some(lease) = &self.core_lease {
            lease.store(false, Ordering::Release);
        }
    }
}

pub(super) fn execute_control(
    command: CoreFileLogCommand,
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
) -> Result<(), String> {
    match command {
        CoreFileLogCommand::Recover { group_id, response } => {
            let result = load_log_store_inners_from_core_journal(path)
                .map(|mut groups| groups.remove(&group_id).unwrap_or_default())
                .map_err(|e| e.to_string());
            let failure = result.as_ref().err().cloned();
            let _ = response.send(result);
            failure.map_or(Ok(()), Err)
        }
        CoreFileLogCommand::Reclaim { group_id, response } => {
            let result = reclaim(path, handle, group_id).map_err(|e| e.to_string());
            let failure = result.as_ref().err().cloned();
            let _ = response.send(result);
            failure.map_or(Ok(()), Err)
        }
        CoreFileLogCommand::Append(_) => Err("append reached control dispatch".to_owned()),
    }
}

#[cfg(not(madsim))]
fn reclaim(
    path: &Path,
    handle: &mut RaftGroupFileLogHandle,
    group_id: u32,
) -> io::Result<(u64, u64)> {
    drop(std::mem::replace(
        handle,
        RaftGroupFileLogHandle::new(!path.exists()),
    ));
    let mut retained = load_log_store_inners_from_core_journal(path)?;
    retained.remove(&group_id);
    let sizes = rewrite_core_journal(path, &retained, true)?.unwrap_or((0, 0));
    // A never-written group can be reclaimed before the shared journal exists.
    // Its first subsequent append must still durably publish the new file.
    *handle = RaftGroupFileLogHandle::new(!path.exists());
    Ok(sizes)
}

#[cfg(madsim)]
fn reclaim(
    _path: &Path,
    _handle: &mut RaftGroupFileLogHandle,
    _group_id: u32,
) -> io::Result<(u64, u64)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "core WAL reclamation is unavailable in simulation",
    ))
}

#[cfg(all(test, not(madsim)))]
#[path = "core_lifecycle_tests.rs"]
mod tests;
