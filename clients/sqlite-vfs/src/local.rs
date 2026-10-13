//! The local files of an attached database besides the WAL: the sidecar (`<db>-ursula`), the
//! boot it was written in, the locks that keep other processes off the db file, the scan for
//! descriptors of this process outside the extension (`open_in_process`), and what a failed
//! attach undoes (`empty_again`).

use std::ffi::OsStr;
use std::fs::OpenOptions;
use std::fs::{self};
use std::path::Path;
use std::path::PathBuf;

use crate::client::offset_token;
use crate::error::Error;
use crate::wal::WalClaim;
use crate::wal::WalMatch;

/// The directory listing this process's open descriptors, each by number.
#[cfg(target_os = "linux")]
const OPEN_FDS: &str = "/proc/self/fd";
#[cfg(not(target_os = "linux"))]
const OPEN_FDS: &str = "/dev/fd";

/// Which file of a database a descriptor is open on ([`open_in_process`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DbFile {
    Db,
    Wal,
    Shm,
    Journal,
}

impl std::fmt::Display for DbFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DbFile::Db => "the db file",
            DbFile::Wal => "its -wal",
            DbFile::Shm => "its -shm",
            DbFile::Journal => "its -journal",
        })
    }
}

/// A descriptor of this process open on the db file `path`, its `-wal`, `-shm` or `-journal`, in
/// any access mode, if any. Attach calls it while no connection of this VFS has the file open, and
/// once the previous attachment's snapshot thread has closed its own connection, so such a
/// descriptor shows a SQLite connection outside the extension (one opened before the extension was
/// loaded, when "unix" was the default VFS, or through another VFS, a read-only one included) or
/// another open of the file. A connection outside the extension shares the "unix" VFS's locks and
/// wal-index node of the file with this VFS's connections in the process: its commits never reach
/// the stream, and even a read-only one keeps the wal-index of a WAL that attach deletes. The "unix"
/// VFS also keeps the descriptors of connections closed meanwhile (this VFS's included) open while
/// such a connection holds a lock on the file, so those show it too.
///
/// A descriptor counts when the path the kernel records for it (the `/proc/self/fd` link, or
/// `F_GETPATH` on macOS, the case ignored there) is one of those four files' (`db_files`), and its
/// device and inode are that file's. A descriptor of a file with another path is never stat'ed (a
/// stat on a hung network mount can block). So one opened through another hard link of the file,
/// or of a file unlinked since, does not count. `None` as well when the descriptors cannot be
/// listed (no `/proc` or `/dev/fd`) or their paths cannot be read (other than Linux and macOS).
pub(crate) fn open_in_process(path: &str) -> Option<(i32, DbFile)> {
    let files = db_files(path)?;
    fs::read_dir(OPEN_FDS)
        .ok()?
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
        .find_map(|fd| {
            // A descriptor closed since the listing (or the listing's own) fails one of these.
            let found = fd_path(fd)?;
            let (which, file) = files.iter().find(|(_, file)| same_path(&found, file))?;
            (fd_identity(fd)? == path_identity(file)?).then_some((fd, *which))
        })
}

/// The paths the kernel can record for a descriptor on the db file `path`, its `-wal`, `-shm` or
/// `-journal`: each name in the db file's directory with every symbolic link resolved (the kernel
/// records the resolved path: `/var` is `/private/var` on macOS), and, when the db file is a link
/// itself, the same four names next to its target (where SQLite puts them when its full pathname
/// resolves links). `None` when the directory cannot be resolved.
fn db_files(path: &str) -> Option<Vec<(DbFile, PathBuf)>> {
    let path = Path::new(path);
    let dir = fs::canonicalize(path.parent()?).ok()?;
    let mut bases = vec![dir.join(path.file_name()?)];
    if let Ok(target) = fs::canonicalize(path)
        && !bases.contains(&target)
    {
        bases.push(target);
    }
    let suffixes = [
        (DbFile::Db, ""),
        (DbFile::Wal, "-wal"),
        (DbFile::Shm, "-shm"),
        (DbFile::Journal, "-journal"),
    ];
    Some(
        bases
            .iter()
            .flat_map(|base| {
                suffixes.iter().map(move |&(which, suffix)| {
                    let mut name = base.clone().into_os_string();
                    name.push(suffix);
                    (which, PathBuf::from(name))
                })
            })
            .collect(),
    )
}

/// Whether another process holds the db file open: tries the lock attach takes before it writes
/// the file (`lock_unused`), up front, and releases it at once. Only that refusal counts
/// ([`Error::OpenElsewhere`]): a file that is missing or cannot be opened for writing goes on to
/// the attach, which reports what fails. Called while nothing of this process has the file open
/// (`open_in_process`), so closing the probe's descriptor drops no lock of this process.
pub(crate) fn probe_unused(path: &str) -> Result<(), Error> {
    let Ok(f) = OpenOptions::new().read(true).write(true).open(path) else {
        return Ok(());
    };
    match lock_unused(path, &f) {
        Err(e @ Error::OpenElsewhere { .. }) => Err(e),
        _ => Ok(()),
    }
}

/// A file's device and inode, as `stat` and `fstat` report them (one type on both sides).
type Identity = (libc::dev_t, libc::ino_t);

/// The device and inode of the file at `path`, `None` when it cannot be stat'ed.
#[expect(unsafe_code, reason = "stat into a libc struct has no safe std API")]
fn path_identity(path: &Path) -> Option<Identity> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `stat` is a C struct of integers, for which all zeros is a valid value.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `c` is NUL-terminated and `stat` writes the struct at the pointer.
    if unsafe { libc::stat(c.as_ptr(), &raw mut st) } != 0 {
        return None;
    }
    Some((st.st_dev, st.st_ino))
}

/// The device and inode of the file descriptor `fd` is open on, `None` when it is not open.
#[expect(
    unsafe_code,
    reason = "fstat of a descriptor number this process lists has no safe std API"
)]
fn fd_identity(fd: i32) -> Option<Identity> {
    // SAFETY: `stat` is a C struct of integers, for which all zeros is a valid value.
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `fstat` only reads the descriptor number (failing with EBADF when it is closed) and
    // writes the struct at the pointer.
    if unsafe { libc::fstat(fd, &raw mut st) } != 0 {
        return None;
    }
    Some((st.st_dev, st.st_ino))
}

/// The path of the file descriptor `fd` is open on, from the kernel's record of it (no filesystem
/// call): the `/proc/self/fd/<fd>` link on Linux, which reads ` (deleted)` after the path of a file
/// since unlinked.
#[cfg(target_os = "linux")]
fn fd_path(fd: i32) -> Option<std::ffi::OsString> {
    Some(
        fs::read_link(format!("{OPEN_FDS}/{fd}"))
            .ok()?
            .into_os_string(),
    )
}

/// As above, through `fcntl(F_GETPATH)` on macOS.
#[cfg(target_os = "macos")]
#[expect(
    unsafe_code,
    reason = "fcntl(F_GETPATH) into a buffer has no safe std API"
)]
fn fd_path(fd: i32) -> Option<std::ffi::OsString> {
    use std::os::unix::ffi::OsStrExt;
    let mut buf = [0u8; libc::PATH_MAX as usize];
    // SAFETY: F_GETPATH writes a NUL-terminated path of at most PATH_MAX bytes into `buf`.
    if unsafe { libc::fcntl(fd, libc::F_GETPATH, buf.as_mut_ptr()) } != 0 {
        return None;
    }
    let path = std::ffi::CStr::from_bytes_until_nul(&buf).ok()?;
    Some(std::ffi::OsStr::from_bytes(path.to_bytes()).to_owned())
}

/// Elsewhere no descriptor's path is known, and none is found.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn fd_path(_fd: i32) -> Option<std::ffi::OsString> {
    None
}

/// Whether a descriptor's path is `file`'s: equal, and on macOS equal but for ASCII case (its
/// volumes are usually case-insensitive, and the kernel reports a name as stored). A path that only
/// differs in case is compared by device and inode next, so it never matches another file.
fn same_path(found: &OsStr, file: &Path) -> bool {
    let (found, file) = (
        found.as_encoded_bytes(),
        file.as_os_str().as_encoded_bytes(),
    );
    if cfg!(target_os = "macos") {
        found.eq_ignore_ascii_case(file)
    } else {
        found == file
    }
}

/// The db file's fingerprint: its inode, size and modification time (to the nanosecond its
/// filesystem keeps), hashed (FNV-1a), never 0. Any write to the file sets its modification time,
/// so a fingerprint taken when the extension last wrote the file tells whether anything wrote it
/// since, unless that write landed within the filesystem's timestamp granularity and kept the
/// size. `None` when the file cannot be stat'ed.
pub(crate) fn db_fingerprint(path: &str) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let m = fs::metadata(path).ok()?;
    let fields = [
        m.ino().to_le_bytes(),
        m.len().to_le_bytes(),
        m.mtime().to_le_bytes(),
        m.mtime_nsec().to_le_bytes(),
    ];
    let hash = fields
        .iter()
        .flatten()
        .fold(0xcbf2_9ce4_8422_2325_u64, |h, &b| {
            (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
        });
    Some(hash | 0x8000_0000_0000_0000)
}

/// The sidecar format (`sidecar_line`): 2 records offsets as the server's strings.
pub(crate) const SIDECAR_VERSION: u32 = 2;

/// The boot id of a sidecar a poison distrusted (`Db::distrust_local`): the files may hold pages
/// the extension did not write (a commit of a connection outside the extension, a lost write-back),
/// so every later attach refuses them (`Untrusted::Distrusted`). No kernel has this boot id, so an
/// extension that does not know it takes the sidecar for one of another boot.
pub(crate) const DISTRUSTED_BOOT: &str = "distrusted";

/// The boot id of a sidecar a poisoned attachment left when it emptied its WAL
/// (`Db::mark_stale`): the files hold only the extension's own pages, but may lag the sidecar's
/// offset, so the next attach discards them (`Discarded::Stale`). An extension that does not know
/// this boot id discards them too, as files of another boot.
pub(crate) const STALE_BOOT: &str = "stale";

/// This kernel's boot id; `None` when unknown, which never matches a recorded one. Within one boot
/// every completed write stays visible (the page cache survives any process crash), so local files
/// written since this boot are exactly what this host wrote; across a reboot or power loss they may
/// be anything. Read at every attach, never cached: a process restored after a reboot (CRIU) must
/// see the new boot. No override, not even for tests (they rewrite the sidecar instead): a fixed id
/// would make every reboot look like the same boot.
pub(crate) fn boot_id() -> Option<String> {
    read_boot_id()
        .map(|id| id.trim().to_owned())
        .filter(|id| !id.is_empty() && !id.contains(char::is_whitespace))
}

#[cfg(target_os = "linux")]
fn read_boot_id() -> Option<String> {
    fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()
}

/// `kern.bootsessionuuid` (not `kern.uuid`, the kernel binary's, which never changes).
#[cfg(target_os = "macos")]
#[expect(unsafe_code, reason = "sysctlbyname has no safe std API")]
fn read_boot_id() -> Option<String> {
    use std::ffi::CStr;
    use std::ptr::null_mut;
    let mut buf = [0u8; 64];
    let mut len = buf.len();
    let name = c"kern.bootsessionuuid";
    // SAFETY: the name is NUL-terminated, `buf` holds `len` writable bytes and `len` is updated in
    // place; no new value is set.
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            null_mut(),
            0,
        )
    };
    if rc != 0 {
        return None;
    }
    let id = CStr::from_bytes_until_nul(&buf).ok()?;
    id.to_str().ok().map(str::to_owned)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_boot_id() -> Option<String> {
    None
}

/// The stream's identity in a sidecar: its URL path (the host may differ: a gateway, another node).
pub(crate) fn stream_key(url: &str) -> &str {
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    rest.find('/').and_then(|i| rest.get(i..)).unwrap_or("")
}

/// The db file's identity, when it exists: its inode. Not the device: an overlay root filesystem
/// (a container's writable layer) gets a new device number at every mount, and a db on another
/// volume has its own sidecar next to it anyway.
pub(crate) fn file_id(path: &str) -> Option<String> {
    use std::os::unix::fs::MetadataExt;
    fs::metadata(path).ok().map(|m| m.ino().to_string())
}

/// What a sidecar records besides offset, epoch, format (`v=`), log count (`log=`) and the WAL
/// claim: the boot it was written in (`boot`, from `boot_id`), the stream and its incarnation
/// (`Head::incarnation`), and the db file it describes (see `Sidecar::verdict`).
pub(crate) fn stamp(path: &str, url: &str, boot: Option<&str>, incarnation: &str) -> String {
    let mut s = format!(
        " boot={} stream={}",
        boot.unwrap_or("unknown"),
        stream_key(url)
    );
    if let Some(id) = file_id(path) {
        s.push_str(&format!(" file={id}"));
    }
    s.push_str(&format!(" incarnation={incarnation}"));
    s
}

/// A sidecar's content: the offset the local files reflect (the server's string), the epoch, the
/// format version, the log since the latest snapshot (`Db::log`), the `stamp`, and the WAL claim
/// (last, `claim_token`).
pub(crate) fn sidecar_line(
    offset: &str,
    epoch: u64,
    log: u64,
    stamp: &str,
    wal: WalClaim,
) -> String {
    format!(
        "{offset} {epoch} v={SIDECAR_VERSION} log={log}{stamp}{}\n",
        claim_token(wal)
    )
}

/// The WAL claim's token, which ends a sidecar's line.
fn claim_token(wal: WalClaim) -> String {
    format!(" wal={:016x}:{}", wal.salts, wal.frame)
}

/// After a failed attach that wrote a db file holding data: the sidecar it left says an attach is
/// in progress (a frame 0 claim without a fingerprint, `attach::Applier::file`), which would vouch
/// for whatever the file holds later. It gets the file's fingerprint now, so a write outside the
/// extension before the next attach refuses the files (`Untrusted::DbFileChanged`), and the next
/// attach otherwise resumes from its offset. Any other sidecar is left as it is, and so is a path
/// without one.
pub(crate) fn seal_sidecar(path: &str) -> Result<(), Error> {
    let sidecar = format!("{path}-ursula");
    let line = match fs::read_to_string(&sidecar) {
        Ok(line) => line,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(Error::Io {
                op: "read sidecar",
                path: sidecar,
                source,
            });
        }
    };
    let in_progress = format!("{}\n", claim_token(WalClaim::NONE));
    let (Some(rest), Some(fingerprint)) = (line.strip_suffix(&in_progress), db_fingerprint(path))
    else {
        return Ok(());
    };
    let claim = claim_token(WalClaim::none_with(fingerprint));
    write_sidecar(&sidecar, &format!("{rest}{claim}\n"))
}

/// Replaces the sidecar (`sidecar_line`) atomically against a process crash: temp file, rename. No
/// fsync: across a reboot attach rebuilds the files whatever the sidecar says, and within one boot
/// every completed rename stays visible (`Sidecar::verdict`).
pub(crate) fn write_sidecar(path: &str, line: &str) -> Result<(), Error> {
    let err = |source| Error::Io {
        op: "write sidecar",
        path: path.to_owned(),
        source,
    };
    let tmp = format!("{path}.tmp");
    fs::write(&tmp, line).map_err(err)?;
    fs::rename(&tmp, path).map_err(err)
}

pub(crate) struct Sidecar {
    /// The offset the local file reflects (as the server wrote it; a version 1 sidecar's is a
    /// decimal number, which the server still reads).
    pub(crate) offset: String,
    pub(crate) epoch: u64,
    /// The format (`SIDECAR_VERSION`; 1 without a `v=` token).
    pub(crate) version: u32,
    /// `Db::log` (0 when absent).
    pub(crate) log: u64,
    pub(crate) boot: Option<String>,
    pub(crate) stream: Option<String>,
    pub(crate) incarnation: Option<String>,
    pub(crate) file: Option<String>,
    pub(crate) wal: Option<WalClaim>,
}

/// What attach does with the local files of a db file that holds data ([`Sidecar::verdict`]).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// Resume from them at the sidecar's offset. `wal_end` is the WAL's last commit frame (0 for a
    /// frame 0 claim): the claimed one, or past it by padding copies (`WalMatch::Covered`).
    Trust { wal_end: u32 },
    /// The stream holds what they hold (a WAL behind its claim holds only this VFS's own frames),
    /// or they cannot be checked as this stream's cache (written in another boot, which a power
    /// loss may have torn, for another incarnation of the stream, in an older format): discard
    /// them and rebuild from the stream.
    Discard(Discarded),
    /// They may hold data that is in no stream: written by a connection outside the extension, or
    /// left by a crash or a failure the files cannot tell from one. Attach refuses and keeps them.
    Refuse(Untrusted),
}

/// Why attach discards local files and rebuilds them (`local_files_discarded`).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Discarded {
    #[error("a sidecar of format {0}, not {SIDECAR_VERSION}")]
    Format(u32),
    #[error("an older version's sidecar, without the stream incarnation or the WAL claim")]
    OlderVersion,
    #[error("a torn sidecar")]
    Torn,
    #[error(
        "a cache of stream incarnation {old}, but the stream is now incarnation {new}: deleted and \
         recreated"
    )]
    OtherIncarnation { old: String, new: String },
    #[error("written in another boot (or the boot is unknown)")]
    OtherBoot,
    #[error("a poisoned attachment emptied its WAL, so they may lag the sidecar's offset")]
    Stale,
    #[error(
        "a WAL that ends before the sidecar's claim: commits the sidecar counted on never reached \
         it"
    )]
    WalBehind,
}

impl Discarded {
    /// The files are a cache of the stream being attached (its incarnation, or an older format's
    /// that does not say), so a read at the sidecar's offset first checks that the stream did not
    /// lose acknowledged data, which the files may be the only copy of.
    pub(crate) fn same_stream(&self) -> bool {
        !matches!(self, Discarded::OtherIncarnation { .. } | Discarded::Torn)
    }
}

/// Why attach refuses local files that may hold data in no stream (`Error::Untrusted`).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Untrusted {
    #[error(
        "a poison found them written by a connection outside the extension, or damaged (a lost \
         write-back, a failed fsync)"
    )]
    Distrusted,
    #[error("the db file is not the one the sidecar describes (replaced, or created anew)")]
    ReplacedFile,
    #[error("its WAL holds commits past the sidecar's claim (frame {claim}, last commit {last})")]
    PastClaim { claim: u32, last: u32 },
    #[error("its WAL is another generation than the sidecar claims, or is missing")]
    OtherGeneration,
    #[error("its WAL holds commits (the last at frame {last}) after the sidecar said it held none")]
    CommitsUnderNone { last: u32 },
    #[error("the db file changed since the extension last wrote it")]
    DbFileChanged,
}

impl Sidecar {
    /// What attach does with the files of `path`, which hold data, attaching stream incarnation
    /// `incarnation` in boot `boot` (`boot_id`). Exactly one kind of file state is trusted: written
    /// in this boot, for this incarnation and this db file, by this format, with the local WAL
    /// holding what the sidecar claims of it, or for a frame 0 claim no commit and the db file as
    /// the extension last left it (`WalClaim::check`, `db_fingerprint`). Any evidence of data that
    /// may be in no stream refuses, whatever else the sidecar says, and so does a sidecar a poison
    /// distrusted (`DISTRUSTED_BOOT`).
    pub(crate) fn verdict(&self, path: &str, boot: Option<&str>, incarnation: &str) -> Verdict {
        if self.version != SIDECAR_VERSION {
            return Verdict::Discard(Discarded::Format(self.version));
        }
        let (Some(own), Some(claim)) = (self.incarnation.as_deref(), self.wal) else {
            return Verdict::Discard(Discarded::OlderVersion);
        };
        if self.boot.as_deref() == Some(DISTRUSTED_BOOT) {
            return Verdict::Refuse(Untrusted::Distrusted);
        }
        if own != incarnation {
            return Verdict::Discard(Discarded::OtherIncarnation {
                old: own.to_owned(),
                new: incarnation.to_owned(),
            });
        }
        if self.boot.as_deref() == Some(STALE_BOOT) {
            return Verdict::Discard(Discarded::Stale);
        }
        if boot.is_none() || self.boot.as_deref() != boot {
            return Verdict::Discard(Discarded::OtherBoot);
        }
        if self.file.is_none() || self.file != file_id(path) {
            return Verdict::Refuse(Untrusted::ReplacedFile);
        }
        match claim.check(path) {
            WalMatch::Covered { end } => {
                if claim.frame == 0 && claim.salts != 0 && db_fingerprint(path) != Some(claim.salts)
                {
                    Verdict::Refuse(Untrusted::DbFileChanged)
                } else {
                    Verdict::Trust { wal_end: end }
                }
            }
            WalMatch::Behind => Verdict::Discard(Discarded::WalBehind),
            WalMatch::PastClaim { last } => Verdict::Refuse(Untrusted::PastClaim {
                claim: claim.frame,
                last,
            }),
            WalMatch::OtherGeneration => Verdict::Refuse(Untrusted::OtherGeneration),
            WalMatch::CommitsUnderNone { last } => {
                Verdict::Refuse(Untrusted::CommitsUnderNone { last })
            }
        }
    }
}

/// `Ok(None)`: the sidecar exists but does not parse (torn by a power loss, or garbage). `Err`: it
/// cannot be read (missing included).
pub(crate) fn read_sidecar(path: &str) -> Result<Option<Sidecar>, Error> {
    let text = match fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => return Ok(None),
        Err(source) => {
            return Err(Error::Io {
                op: "read sidecar",
                path: path.to_owned(),
                source,
            });
        }
    };
    let mut it = text.split_whitespace();
    let offset = it.next().and_then(offset_token);
    let epoch = it.next().and_then(|v| v.parse::<u64>().ok());
    let (Some(offset), Some(epoch)) = (offset, epoch) else {
        return Ok(None);
    };
    let mut s = Sidecar {
        offset,
        epoch,
        version: 1,
        log: 0,
        boot: None,
        stream: None,
        incarnation: None,
        file: None,
        wal: None,
    };
    for token in it {
        match token.split_once('=') {
            Some(("v", v)) => match v.parse() {
                Ok(v) => s.version = v,
                Err(_) => return Ok(None),
            },
            Some(("log", v)) => match v.parse() {
                Ok(v) => s.log = v,
                Err(_) => return Ok(None),
            },
            Some(("boot", v)) => s.boot = Some(v.to_owned()),
            Some(("stream", v)) => s.stream = Some(v.to_owned()),
            Some(("incarnation", v)) => s.incarnation = Some(v.to_owned()),
            Some(("file", v)) => s.file = Some(v.to_owned()),
            Some(("wal", v)) => {
                let claim = v.split_once(':').and_then(|(salts, frame)| {
                    Some(WalClaim {
                        salts: u64::from_str_radix(salts, 16).ok()?,
                        frame: frame.parse().ok()?,
                    })
                });
                let Some(claim) = claim else {
                    return Ok(None);
                };
                s.wal = Some(claim);
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(s))
}

/// Empties a database's local files (a cache of the stream) so attach rebuilds them: never while
/// another process has the file open, and never the host lock (held). The db file is truncated, not
/// unlinked, and returned still locked (`lock_unused`) for the rebuild to write: a connection that
/// opened the path meanwhile gets SQLITE_BUSY until the lock drops (a busy timeout retries) and
/// then reads the rebuilt file, never a deleted inode. The attach then removes `-journal`, `-wal`
/// and `-shm` and writes a new sidecar before it replays anything: until then the db file holds no
/// data, so a crash midway leaves a path the next attach rebuilds. A snapshot temp file an older
/// version may have left is removed too.
pub(crate) fn discard_local(path: &str) -> Result<fs::File, Error> {
    let f = open_locked(path)?;
    remove_if_exists(&format!("{path}-ursula.snap"))?;
    f.set_len(0).map_err(|source| Error::Io {
        op: "truncate",
        path: path.to_owned(),
        source,
    })?;
    Ok(f)
}

/// Fails loudly: a stale WAL or journal left next to a rebuilt db file would be applied to it.
pub(crate) fn remove_if_exists(f: &str) -> Result<(), Error> {
    match fs::remove_file(f) {
        Err(source) if source.kind() != std::io::ErrorKind::NotFound => Err(Error::Io {
            op: "remove",
            path: f.to_owned(),
            source,
        }),
        _ => Ok(()),
    }
}

/// Keeps other processes off the db file `f` while attach rewrites it. Attach never opens local
/// files through SQLite before that (a trusted file may hold a torn page 1, see `fold_wal`), so it
/// does this without parsing pages: every SQLite connection on a WAL-format file (any process, any
/// unix-based VFS) takes a POSIX read lock in the db file's lock-byte range at its first read and
/// keeps it until it closes. Attach takes a write lock on that range (failing while any connection
/// holds it) and holds it until it is done writing the file: a connection reading after a mere
/// probe would recover the stale WAL, keep it open, and checkpoint it over the replayed pages at
/// its close, silently rolling the file back. Meanwhile a connection's first read fails with
/// SQLITE_BUSY. The file keeps its inode throughout (`discard_local` truncates, `install` writes
/// in place), so a connection that opened the path meanwhile reads the rewritten file once the
/// lock drops, not a deleted one, whose checkpoint would copy the shared `-wal`'s frames into the
/// dead inode and mark them checkpointed in the shared `-shm`.
///
/// The lock lives as long as `f` and any other descriptor of this process on the file: closing any
/// of them drops it, so the caller keeps `f` open and opens no other one meanwhile. No connection
/// of this process is open or can open (attach refuses otherwise, and `x_open` refuses the main db
/// while it attaches), so closing `f` drops only this lock. Another *attached* process is excluded
/// by the host lock already held.
#[expect(
    unsafe_code,
    reason = "POSIX byte-range locks (fcntl), which SQLite uses, have no safe std API"
)]
pub(crate) fn lock_unused(path: &str, f: &fs::File) -> Result<(), Error> {
    use std::os::fd::AsRawFd;
    // SAFETY: `flock` is a C struct of integers, for which all zeros is a valid value.
    let mut l: libc::flock = unsafe { std::mem::zeroed() };
    l.l_type = libc::F_WRLCK as _;
    l.l_whence = libc::SEEK_SET as _;
    l.l_start = 0x4000_0000; // PENDING_BYTE; RESERVED and the SHARED range follow (512 bytes)
    l.l_len = 512;
    // SAFETY: `f` is an open descriptor and F_SETLK reads the `flock` at the pointer.
    if unsafe { libc::fcntl(f.as_raw_fd(), libc::F_SETLK, &raw const l) } == 0 {
        return Ok(());
    }
    let e = std::io::Error::last_os_error();
    if !matches!(e.raw_os_error(), Some(libc::EAGAIN | libc::EACCES)) {
        return Err(Error::Io {
            op: "lock",
            path: path.to_owned(),
            source: e,
        });
    }
    // Name the holder, if it still holds the range.
    // SAFETY: `f` is an open descriptor and F_GETLK reads and writes the `flock` at the pointer.
    let held = unsafe { libc::fcntl(f.as_raw_fd(), libc::F_GETLK, &raw mut l) } == 0
        && l.l_type != libc::F_UNLCK as _;
    Err(Error::OpenElsewhere {
        path: path.to_owned(),
        pid: held.then_some(l.l_pid),
    })
}

/// Whether the db file at `path` holds data (an error reading its size counts as none, as attach's
/// own check takes it: `attach_files`). A WAL next to a db file without data holds nothing: SQLite
/// deletes it when it opens the file.
pub(crate) fn holds_data(path: &str) -> bool {
    fs::metadata(path).is_ok_and(|m| m.len() > 0)
}

/// Whether the db file at `path` is known to hold no data: missing, or empty. Any other error
/// reading its size is no such proof.
pub(crate) fn holds_no_data(path: &str) -> bool {
    match fs::metadata(path) {
        Ok(m) => m.len() == 0,
        Err(e) => e.kind() == std::io::ErrorKind::NotFound,
    }
}

/// Empties the db file of a path that held no data when an attach began, after that attach wrote
/// into it and then failed (`attach`): back to no bytes, keeping its inode (`lock_unused`). Only
/// what the attach wrote is undone, and only while nothing else can have written the file since:
/// under the lock that keeps other processes off it, and while nothing of this process outside the
/// extension has it open (`open_in_process`). The caller then removes the sidecar the attach
/// wrote, so nothing vouches for data written to the file later.
pub(crate) fn empty_again(path: &str) -> Result<(), Error> {
    if let Some((fd, file)) = open_in_process(path) {
        return Err(Error::OpenOutsideVfs {
            path: path.to_owned(),
            fd,
            file,
        });
    }
    open_locked(path)?.set_len(0).map_err(|source| Error::Io {
        op: "truncate",
        path: path.to_owned(),
        source,
    })
}

/// Opens the db file and locks it (`lock_unused`); the lock lasts while the descriptor is open.
fn open_locked(path: &str) -> Result<fs::File, Error> {
    let f = OpenOptions::new().read(true).write(true).open(path);
    let f = f.map_err(|source| Error::Io {
        op: "open",
        path: path.to_owned(),
        source,
    })?;
    lock_unused(path, &f)?;
    Ok(f)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::fs::OpenOptions;

    use super::DbFile;
    use super::Discarded;
    use super::Untrusted;
    use super::Verdict;
    use super::boot_id;
    use super::db_fingerprint;
    use super::empty_again;
    use super::holds_data;
    use super::holds_no_data;
    use super::open_in_process;
    use super::read_sidecar;
    use super::seal_sidecar;
    use super::sidecar_line;
    use super::stamp;
    use crate::error::Error;
    use crate::frame::PAGE;
    use crate::wal::WalClaim;
    use crate::wal::be32;
    use crate::wal::fold_wal;

    /// A WAL of generation `salt` (4 KiB pages, big-endian checksums) with one frame per entry:
    /// its page number, the db size after it (0: not a commit), and the byte its page is filled
    /// with.
    fn wal(salt: u32, frames: &[(u32, u32, u8)]) -> Vec<u8> {
        let sum = |mut s: (u32, u32), b: &[u8]| {
            for c in b.chunks_exact(8) {
                s.0 = s.0.wrapping_add(be32(c, 0).unwrap()).wrapping_add(s.1);
                s.1 = s.1.wrapping_add(be32(c, 4).unwrap()).wrapping_add(s.0);
            }
            s
        };
        let mut wal = Vec::new();
        for v in [0x377f_0683u32, 3_007_000, PAGE as u32, 0, 0, salt] {
            wal.extend(v.to_be_bytes());
        }
        let mut s = sum((0, 0), &wal);
        wal.extend(s.0.to_be_bytes());
        wal.extend(s.1.to_be_bytes());
        for &(pgno, commit, fill) in frames {
            let page = vec![fill; PAGE];
            let mut h = Vec::new();
            for v in [pgno, commit, 0, salt] {
                h.extend(v.to_be_bytes());
            }
            s = sum(sum(s, &h[..8]), &page);
            h.extend(s.0.to_be_bytes());
            h.extend(s.1.to_be_bytes());
            wal.extend(h);
            wal.extend(page);
        }
        wal
    }

    // Attach trusts local files only when the sidecar was written in this boot, for this stream
    // incarnation and this db file, and the files hold what it claims: the WAL's last commit is the
    // claimed one (or a padding copy of it), or for a frame 0 claim the WAL holds no commit and the
    // db file is as the extension last left it. Files that cannot be checked are discarded; files
    // that may hold data in no stream are refused.
    #[test]
    fn sidecar_verdict() {
        let dir = std::env::temp_dir().join(format!("ursula-sidecar-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("db").to_str().unwrap().to_owned();
        let sidecar = format!("{db}-ursula");
        fs::write(&db, b"x").unwrap();
        let verdict = |line: &str, boot: Option<&str>| {
            fs::write(&sidecar, line).unwrap();
            read_sidecar(&sidecar)
                .unwrap()
                .map(|s| s.verdict(&db, boot, "i1"))
        };
        let b1 = Some("b1");
        let offset = "00000000000000000007";
        let line = |claim: WalClaim| {
            sidecar_line(offset, 2, 5, &stamp(&db, "http://h:1/b/s", b1, "i1"), claim)
        };
        let trusted = |wal_end| Some(Verdict::Trust { wal_end });
        let refused = |why| Some(Verdict::Refuse(why));
        let discarded = |why| Some(Verdict::Discard(why));
        let here = line(WalClaim::NONE);
        assert!(here.starts_with("00000000000000000007 2 v=2 log=5 boot=b1 stream=/b/s file="));
        assert!(here.ends_with(" incarnation=i1 wal=0000000000000000:0\n"));
        // A frame 0 claim without a fingerprint (an older version's, or one an attach in progress
        // left) needs no commit in the WAL only; with one, the db file must be as it was then.
        assert_eq!(verdict(&here, b1), trusted(0));
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!((s.offset.as_str(), s.epoch, s.log), (offset, 2, 5));
        let fingerprint = db_fingerprint(&db).unwrap();
        let sealed = line(WalClaim::none_with(fingerprint));
        assert_eq!(verdict(&sealed, b1), trusted(0));
        // Another size: a write within the filesystem's timestamp granularity that keeps the size
        // goes unseen (Linux updates the modification time at a coarse clock's granularity).
        fs::write(&db, b"yy").unwrap();
        assert_ne!(db_fingerprint(&db), Some(fingerprint));
        assert_eq!(verdict(&sealed, b1), refused(Untrusted::DbFileChanged));
        // The stream deleted and recreated (another incarnation), a sidecar from before the claim
        // or the incarnation were recorded, or one of format 1 (a numeric offset; still parsed,
        // for the incarnation and the offset's read check before discarding).
        assert_eq!(
            read_sidecar(&sidecar)
                .unwrap()
                .map(|s| s.verdict(&db, b1, "i2")),
            discarded(Discarded::OtherIncarnation {
                old: "i1".into(),
                new: "i2".into()
            })
        );
        let legacy = here.replace(" incarnation=i1", "");
        assert_eq!(verdict(&legacy, b1), discarded(Discarded::OlderVersion));
        let unclaimed = here.replace(" wal=0000000000000000:0", "");
        assert_eq!(verdict(&unclaimed, b1), discarded(Discarded::OlderVersion));
        let v1 = here.replace(" v=2 log=5", "").replace(offset, "7");
        assert_eq!(verdict(&v1, b1), discarded(Discarded::Format(1)));
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!((s.version, s.offset.as_str()), (1, "7"));
        assert_eq!(s.incarnation.as_deref(), Some("i1"));
        // Another boot, or an unknown one.
        assert_eq!(verdict(&here, Some("b2")), discarded(Discarded::OtherBoot));
        assert_eq!(verdict(&here, None), discarded(Discarded::OtherBoot));
        let unknown = here.replace(" boot=b1", " boot=unknown");
        assert_eq!(
            unknown,
            sidecar_line(
                offset,
                2,
                5,
                &stamp(&db, "http://h:1/b/s", None, "i1"),
                WalClaim::NONE
            )
        );
        assert_eq!(verdict(&unknown, None), discarded(Discarded::OtherBoot));
        // A poisoned attachment emptied its WAL: discarded (after the read check).
        let stale = here.replace(" boot=b1", " boot=stale");
        assert_eq!(verdict(&stale, b1), discarded(Discarded::Stale));
        assert!(Discarded::Stale.same_stream());
        // A poison distrusted the files: refused, whatever else the sidecar says.
        let distrusted = here.replace(" boot=b1", " boot=distrusted");
        assert_eq!(verdict(&distrusted, b1), refused(Untrusted::Distrusted));
        assert_eq!(
            read_sidecar(&sidecar)
                .unwrap()
                .map(|s| s.verdict(&db, b1, "i2")),
            refused(Untrusted::Distrusted)
        );
        // Another db file (replaced by a rename), or one the sidecar does not name.
        let file = here
            .split_whitespace()
            .find(|t| t.starts_with("file="))
            .unwrap();
        let other_file = here.replace(file, "file=0");
        assert_eq!(verdict(&other_file, b1), refused(Untrusted::ReplacedFile));
        let no_file = here.replace(&format!(" {file}"), "");
        assert_eq!(verdict(&no_file, b1), refused(Untrusted::ReplacedFile));
        // A claim on WAL frames with no WAL there.
        let frames = line(WalClaim {
            salts: 0xaa,
            frame: 2,
        });
        assert_eq!(verdict(&frames, b1), refused(Untrusted::OtherGeneration));
        for torn in [
            "",
            "7",
            "7 2 boot",
            "7 2 x=1",
            "7 2 wal=12",
            "7 2 wal=zz:1",
            "7 2 v=x",
            "7 2 log=-1",
            "7/ 2",
        ] {
            assert_eq!(verdict(torn, b1), None);
        }
        fs::write(&sidecar, b"\xff\xfe 7 2").unwrap();
        assert!(read_sidecar(&sidecar).unwrap().is_none());
        // A WAL of generation 0xaa: two commits of page 1 (images of 1s, then 2s), the second
        // padded to a sector with two copies of its frame (synchronous=FULL without power-safe
        // overwrite). The claim names its generation and its last commit, which the copies after it
        // do not change; anything else past the claim is another writer's.
        let padded = wal(0xaa, &[(1, 1, 1), (1, 1, 2), (1, 1, 2), (1, 1, 2)]);
        fs::write(format!("{db}-wal"), &padded).unwrap();
        let claim = |salts, frame| line(WalClaim { salts, frame });
        assert_eq!(verdict(&claim(0xaa, 2), b1), trusted(4));
        assert_eq!(
            verdict(&claim(0xaa, 1), b1),
            refused(Untrusted::PastClaim { claim: 1, last: 4 })
        );
        assert_eq!(
            verdict(&claim(0xbb, 2), b1),
            refused(Untrusted::OtherGeneration)
        );
        assert_eq!(
            verdict(&claim(0xaa, 5), b1),
            discarded(Discarded::WalBehind)
        );
        assert_eq!(
            verdict(&here, b1),
            refused(Untrusted::CommitsUnderNone { last: 4 })
        );
        // A commit of the same page with other bytes, or of another db size, is no padding copy.
        for after in [(1, 1, 3), (1, 2, 2), (2, 2, 2)] {
            let more = wal(0xaa, &[(1, 1, 1), (1, 1, 2), after]);
            fs::write(format!("{db}-wal"), &more).unwrap();
            assert_eq!(
                verdict(&claim(0xaa, 2), b1),
                refused(Untrusted::PastClaim { claim: 2, last: 3 }),
                "{after:?}"
            );
        }
        // A frame past the claim that commits nothing is no commit: the claim still holds.
        let uncommitted = wal(0xaa, &[(1, 1, 1), (1, 1, 2), (2, 0, 5)]);
        fs::write(format!("{db}-wal"), &uncommitted).unwrap();
        assert_eq!(verdict(&claim(0xaa, 2), b1), trusted(2));
        // Folding the padded WAL leaves the last image, cut to its db size.
        fs::write(format!("{db}-wal"), &padded).unwrap();
        fs::write(&db, vec![9u8; 2 * PAGE]).unwrap();
        let f = OpenOptions::new().read(true).write(true).open(&db).unwrap();
        fold_wal(&db, &f).unwrap();
        assert_eq!(fs::read(&db).unwrap(), vec![2u8; PAGE]);
        fs::remove_file(&sidecar).unwrap();
        assert!(read_sidecar(&sidecar).is_err());
        fs::remove_dir_all(&dir).unwrap();
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert!(boot_id().is_some(), "the kernel's boot id is unreadable");
    }

    // After a failed attach of files that held data, the sidecar that says an attach is in
    // progress gets the db file's fingerprint, so a later write to the file outside the extension
    // refuses it; any other sidecar stays as it is.
    #[test]
    fn a_failed_attach_seals_its_sidecar() {
        let dir = std::env::temp_dir().join(format!("ursula-seal-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("app.db").to_string_lossy().into_owned();
        let sidecar = format!("{db}-ursula");
        fs::write(&db, vec![1u8; PAGE]).unwrap();
        let stamp = stamp(&db, "http://h/b/s", Some("b1"), "i1");
        let line = |claim| sidecar_line("00000000000000000007", 2, 5, &stamp, claim);
        fs::write(&sidecar, line(WalClaim::NONE)).unwrap();
        seal_sidecar(&db).unwrap();
        let fingerprint = db_fingerprint(&db).unwrap();
        assert_eq!(
            fs::read_to_string(&sidecar).unwrap(),
            line(WalClaim::none_with(fingerprint))
        );
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!(s.verdict(&db, Some("b1"), "i1"), Verdict::Trust {
            wal_end: 0
        });
        fs::write(&db, vec![2u8; 2 * PAGE]).unwrap();
        assert_eq!(
            s.verdict(&db, Some("b1"), "i1"),
            Verdict::Refuse(Untrusted::DbFileChanged)
        );
        let frames = line(WalClaim {
            salts: 0xaa,
            frame: 3,
        });
        fs::write(&sidecar, &frames).unwrap();
        seal_sidecar(&db).unwrap();
        assert_eq!(fs::read_to_string(&sidecar).unwrap(), frames);
        fs::remove_file(&sidecar).unwrap();
        seal_sidecar(&db).unwrap();
        assert!(!fs::exists(&sidecar).unwrap(), "no sidecar, none written");
        fs::remove_dir_all(&dir).unwrap();
    }

    // Any descriptor of this process on the db file, its -wal, -shm or -journal is found, in any
    // access mode (a read-only SQLite connection maps the wal-index too), by the path the kernel
    // records for it, links resolved, and its inode. One on another file of the same name is not,
    // nor any once closed.
    #[test]
    fn any_descriptor_on_the_db_files_is_found() {
        let dir = std::env::temp_dir().join(format!("ursula-fds-{}", std::process::id()));
        let other = dir.join("other");
        fs::create_dir_all(&other).unwrap();
        let path = dir.join("app.db").to_string_lossy().into_owned();
        assert_eq!(open_in_process(&path), None, "no file");
        fs::write(&path, b"").unwrap();
        assert_eq!(open_in_process(&path), None);
        let reader = fs::File::open(&path).unwrap();
        assert!(matches!(open_in_process(&path), Some((_, DbFile::Db))));
        // Through a link to its directory: the kernel records the resolved path.
        let link = dir.join("link");
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        let linked = link.join("app.db").to_string_lossy().into_owned();
        assert!(matches!(open_in_process(&linked), Some((_, DbFile::Db))));
        drop(reader);
        assert_eq!(open_in_process(&path), None);
        for (suffix, which) in [
            ("-wal", DbFile::Wal),
            ("-shm", DbFile::Shm),
            ("-journal", DbFile::Journal),
        ] {
            let file = format!("{path}{suffix}");
            fs::write(&file, b"").unwrap();
            let open = fs::File::open(&file).unwrap();
            assert_eq!(open_in_process(&path).map(|(_, w)| w), Some(which));
            drop(open);
            assert_eq!(open_in_process(&path), None, "{suffix}");
        }
        let same_name = other.join("app.db");
        fs::write(&same_name, b"").unwrap();
        let elsewhere = OpenOptions::new().write(true).open(&same_name).unwrap();
        assert_eq!(
            open_in_process(&path),
            None,
            "another file of the same name"
        );
        drop(elsewhere);
        fs::remove_dir_all(&dir).unwrap();
    }

    // A failed attach of a path whose db file held no data empties the db file it wrote into,
    // and nothing else. Never while a descriptor of this process outside the extension is open on
    // the file, in any mode.
    #[test]
    fn a_failed_attach_empties_only_the_db_file_it_wrote() {
        let dir = std::env::temp_dir().join(format!("ursula-fresh-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("app.db").to_string_lossy().into_owned();
        assert!(holds_no_data(&path), "missing");
        fs::write(&path, b"").unwrap();
        assert!(holds_no_data(&path) && !holds_data(&path), "empty");
        fs::write(&path, vec![1u8; PAGE]).unwrap();
        assert!(!holds_no_data(&path) && holds_data(&path));
        for suffix in ["-ursula", "-wal", "-shm"] {
            fs::write(format!("{path}{suffix}"), suffix).unwrap();
        }
        let outside = fs::File::open(&path).unwrap();
        assert!(matches!(
            empty_again(&path),
            Err(Error::OpenOutsideVfs {
                file: DbFile::Db,
                ..
            })
        ));
        assert!(holds_data(&path));
        drop(outside);
        empty_again(&path).unwrap();
        assert!(fs::exists(&path).unwrap() && holds_no_data(&path));
        for suffix in ["-ursula", "-wal", "-shm"] {
            assert_eq!(
                fs::read_to_string(format!("{path}{suffix}")).unwrap(),
                suffix
            );
        }
        fs::remove_dir_all(&dir).unwrap();
    }
}
