//! A host's cleanup of the host directory (design: Hosts): the records of cache directories that
//! no longer exist, and the temp files of writers that no longer run. [`prune`] is the one entry
//! point, and `ursula-vfs-host prune` prints its [`Pruned`] as one line of JSON.
//!
//! 1. One inspection pass ([`inspection::inspect`]) with the host's directory, scans and budget,
//!    its lock check over every process. A pass that is not complete removes nothing.
//! 2. What may go, in name order: the record of each recorded directory the pass found absent
//!    (nothing at the path, or something that is not a directory), and, when the host asks, each
//!    `.tmp-<pid>-<16 hex>` whose PID no process has in this PID namespace (`kill(pid, 0)` answers
//!    `ESRCH`). A name whose number is not a PID `kill` can probe (0, or above `pid_t`'s maximum)
//!    stays.
//! 3. The host directory opened again (`O_DIRECTORY` and `O_NOFOLLOW`), which must be the directory
//!    the pass checked (the same device and inode).
//! 4. The removals, through that descriptor. Each record's removal checks that its directory is
//!    still absent by the pass's test, reads the record again through the descriptor, which must
//!    still be a record of that directory, and unlinks it. A removal that fails is skipped and the
//!    others go on, since each checks for itself. The host directory is fsynced once after the
//!    last.
//!
//! A removal is safe only while nothing that may load the extension runs, and only for a directory
//! that is gone for good at its path (design: Hosts). Neither can be checked here: a host prunes
//! when both hold.

use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use crate::hostdir;
use crate::inspection;
use crate::inspection::FileId;
use crate::inspection::HostListing;
use crate::inspection::Inspect;
use crate::inspection::Inspection;
use crate::inspection::RecordError;
use crate::json;

/// The version of the prune's JSON.
const VERSION: u64 = 1;

/// What [`prune`] reads and removes.
#[derive(Debug, Clone, Default)]
pub struct Prune {
    /// The host directory: the compiled-in one when `None`, as for [`Inspect::dir`].
    pub dir: Option<PathBuf>,
    /// Directories the pass reads besides the recorded ones, as for [`Inspect::scans`].
    pub scans: Vec<PathBuf>,
    /// The pass's budget, as for [`Inspect::budget`].
    pub budget: Option<Duration>,
    /// Also remove the temp files of writers that no longer run in this PID namespace.
    pub tmp: bool,
}

/// A removal that did not happen, or the host directory's fsync after the removals.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error("the host directory {}: {op}: {source}", .dir.display())]
    HostDir {
        dir: PathBuf,
        op: &'static str,
        #[source]
        source: std::io::Error,
    },
    /// The path names another directory than the one the pass checked.
    #[error("the host directory {}: it is not the directory the pass read", .dir.display())]
    Moved { dir: PathBuf },
    #[error("{name}: {} is there again", .cache.display())]
    Present { name: String, cache: PathBuf },
    #[error("{name}: read: {source}")]
    Read {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{name}: {source}")]
    Changed {
        name: String,
        #[source]
        source: RecordError,
    },
    #[error("{name}: unlink: {source}")]
    Unlink {
        name: String,
        #[source]
        source: std::io::Error,
    },
}

/// A name a complete pass lets the prune remove.
#[derive(Debug)]
enum Removal {
    /// The record `name` of the absent directory `cache`.
    Record { name: String, cache: PathBuf },
    /// The temp file `name` of a writer that no longer runs.
    Tmp { name: String },
}

impl Removal {
    fn name(&self) -> &str {
        match self {
            Removal::Record { name, .. } | Removal::Tmp { name } => name,
        }
    }
}

/// The result of [`prune`]: the pass it ran, what it removed, and what failed, as
/// [`Pruned::to_json`] prints it.
#[derive(Debug)]
pub struct Pruned {
    inspection: Inspection,
    /// The records removed, by name, with the directory each named.
    removed: Vec<(String, PathBuf)>,
    /// The temp files removed.
    tmp_removed: Vec<String>,
    /// What failed, in order.
    failures: Vec<Failure>,
}

impl Pruned {
    /// Whether the pass was complete and every removal it tried succeeded, the fsync included.
    pub fn pruned(&self) -> bool {
        self.inspection.complete() && self.failures.is_empty()
    }

    /// The prune as one line of JSON (design: Hosts). Within `v` 1 the keys and their order are
    /// fixed: any change is a new `v`, and a host may refuse any other shape.
    pub fn to_json(&self) -> String {
        let path = |p: &Path| json::str(&p.to_string_lossy());
        let removed: Vec<String> = self
            .removed
            .iter()
            .map(|(name, cache)| json::object(&[("name", json::str(name)), ("dir", path(cache))]))
            .collect();
        let tmp_removed: Vec<String> = self.tmp_removed.iter().map(|n| json::str(n)).collect();
        let error = self
            .failures
            .first()
            .map(ToString::to_string)
            .or_else(|| self.inspection.error());
        json::object(&[
            ("v", VERSION.to_string()),
            ("dir", path(self.inspection.dir())),
            ("pruned", self.pruned().to_string()),
            (
                "incomplete",
                self.inspection
                    .incomplete()
                    .map_or_else(json::null, json::str),
            ),
            ("removed", json::array(&removed)),
            ("tmp_removed", json::array(&tmp_removed)),
            ("error", error.map_or_else(json::null, |e| json::str(&e))),
        ])
    }
}

/// Runs one inspection pass, then, when it is complete, removes the records of the directories it
/// found absent and, with [`Prune::tmp`], the temp files of writers that no longer run (design:
/// Hosts).
pub fn prune(request: &Prune) -> Pruned {
    let inspection = inspection::inspect(&Inspect {
        dir: request.dir.clone(),
        scans: request.scans.clone(),
        pids: None,
        until_clean: None,
        budget: request.budget,
    });
    let mut pruned = Pruned {
        inspection,
        removed: Vec::new(),
        tmp_removed: Vec::new(),
        failures: Vec::new(),
    };
    // An incomplete pass removes nothing. A pass without the host directory's listing is always
    // incomplete, so a complete one has the identity and the temp files it listed.
    let (true, Some(listing)) = (pruned.inspection.complete(), pruned.inspection.listing()) else {
        return pruned;
    };
    let removals = removals(&pruned.inspection, listing, request.tmp);
    if removals.is_empty() {
        return pruned;
    }
    let host = pruned.inspection.dir().to_owned();
    let handle = match open_checked(&host, listing.id) {
        Ok(handle) => handle,
        Err(failure) => {
            pruned.failures.push(failure);
            return pruned;
        }
    };
    for removal in removals {
        match removal {
            Removal::Record { name, cache } => match remove_record(&handle, &name, &cache) {
                Ok(()) => pruned.removed.push((name, cache)),
                Err(failure) => pruned.failures.push(failure),
            },
            Removal::Tmp { name } => match hostdir::unlink(&handle, &name) {
                Ok(()) => pruned.tmp_removed.push(name),
                Err(source) => pruned.failures.push(Failure::Unlink { name, source }),
            },
        }
    }
    if let Err(source) = handle.sync_all() {
        pruned.failures.push(Failure::HostDir {
            dir: host,
            op: "fsync",
            source,
        });
    }
    pruned
}

/// What the complete pass `inspection`, which listed `listing`, lets a prune remove, in name order:
/// the records of the directories it found absent, and with `tmp` the temp files whose writer's PID
/// no process has.
fn removals(inspection: &Inspection, listing: &HostListing, tmp: bool) -> Vec<Removal> {
    let mut removals: Vec<Removal> = inspection
        .absent_records()
        .map(|(name, cache)| Removal::Record {
            name: name.to_owned(),
            cache: cache.to_owned(),
        })
        .collect();
    if tmp {
        removals.extend(
            listing
                .tmps
                .iter()
                .filter(|name| hostdir::tmp_pid(name).is_some_and(|pid| !inspection::alive(pid)))
                .map(|name| Removal::Tmp { name: name.clone() }),
        );
    }
    removals.sort_by(|a, b| a.name().cmp(b.name()));
    removals
}

/// Opens the host directory `host` again, through no link, and refuses it unless it is the
/// directory `checked` names (its device and inode at the pass).
fn open_checked(host: &Path, checked: FileId) -> Result<File, Failure> {
    let failed = |op| {
        move |source| Failure::HostDir {
            dir: host.to_owned(),
            op,
            source,
        }
    };
    let handle = hostdir::open_dir(host).map_err(failed("open"))?;
    let meta = handle.metadata().map_err(failed("stat"))?;
    if (meta.dev(), meta.ino()) != checked {
        return Err(Failure::Moved {
            dir: host.to_owned(),
        });
    }
    Ok(handle)
}

/// Removes the record `name` of the directory `cache` from the host directory open as `handle`:
/// once `cache` is still absent by the pass's test, and the record read again through `handle` is
/// still a record of this line under its name. A record's name is the hash of the path it holds
/// (`hostdir::record_name`), which the read checks, so such a record still names `cache`.
fn remove_record(handle: &File, name: &str, cache: &Path) -> Result<(), Failure> {
    if !inspection::absent(cache) {
        return Err(Failure::Present {
            name: name.to_owned(),
            cache: cache.to_owned(),
        });
    }
    match inspection::read_record(handle, name) {
        Ok(Ok(_cache)) => {}
        Ok(Err(source)) => {
            return Err(Failure::Changed {
                name: name.to_owned(),
                source,
            });
        }
        Err(source) => {
            return Err(Failure::Read {
                name: name.to_owned(),
                source,
            });
        }
    }
    hostdir::unlink(handle, name).map_err(|source| Failure::Unlink {
        name: name.to_owned(),
        source,
    })
}

#[cfg(test)]
mod tests;
