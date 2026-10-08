//! The local files of an attached database besides the WAL: the sidecar (`<db>-ursula`),
//! the boot it was written in, and the locks that keep other processes off the db file.

use std::ffi::CStr;
use std::fs::OpenOptions;
use std::fs::{self};
use std::ptr::null_mut;

use crate::client::offset_token;
use crate::error::Error;
use crate::wal::WalClaim;

/// The sidecar format (`sidecar_line`): 2 records offsets as the server's strings.
pub(crate) const SIDECAR_VERSION: u32 = 2;

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
/// (`Head::incarnation`), and the db file it describes (see `trusted`).
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
/// format version, the log since the latest snapshot (`Db::log`), the `stamp`, and the WAL claim.
pub(crate) fn sidecar_line(
    offset: &str,
    epoch: u64,
    log: u64,
    stamp: &str,
    wal: WalClaim,
) -> String {
    format!(
        "{offset} {epoch} v={SIDECAR_VERSION} log={log}{stamp} wal={:016x}:{}\n",
        wal.salts, wal.frame
    )
}

/// Replaces the sidecar (`sidecar_line`) atomically against a process crash: temp file, rename. No
/// fsync: `Sidecar::trusted` checks it against the files (a sidecar ahead of its WAL is rebuilt;
/// one behind it replays from its offset).
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

impl Sidecar {
    /// The local files hold at least the state at the sidecar's offset: written since this boot,
    /// from this incarnation of the stream (a stream deleted and recreated at the same path is
    /// another one, whatever its length), into this db file (one replaced behind our back would get
    /// the old one's WAL applied to it), and the local WAL still holds what the sidecar claims of
    /// it (a disk image restored without a reboot keeps boot and inode but may have lost any
    /// unsynced write). A sidecar of an older version (an older format, no boot, no incarnation,
    /// or no WAL claim) is not trusted, and nothing is when the current boot (`boot`, from
    /// `boot_id`) is unknown.
    pub(crate) fn trusted(&self, path: &str, boot: Option<&str>, incarnation: &str) -> bool {
        self.version == SIDECAR_VERSION
            && boot.is_some_and(|b| self.boot.as_deref() == Some(b))
            && self.incarnation.as_deref() == Some(incarnation)
            && self.file.is_some()
            && self.file == file_id(path)
            && self.wal.is_some_and(|w| w.covered(path))
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
/// then reads the rebuilt file, never a deleted
/// inode. Every attach then removes `-journal`, and the fresh path `-wal` and `-shm`, and rewrites
/// the sidecar: until then the sidecar marks whatever is left untrusted, so a crash midway discards
/// again. A snapshot temp file an older version may have left is removed too.
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

    use super::boot_id;
    use super::read_sidecar;
    use super::sidecar_line;
    use super::stamp;
    use crate::frame::PAGE;
    use crate::wal::WalClaim;
    use crate::wal::be32;
    use crate::wal::fold_wal;

    // Attach trusts local files only when the sidecar was written in this boot, from this stream
    // incarnation, for this db file, and the WAL holds what it claims; a legacy, torn, other-boot
    // or other-incarnation sidecar is discarded, and a missing one is an error (the file may be a
    // database that was never attached).
    #[test]
    fn sidecar_trust() {
        let dir = std::env::temp_dir().join(format!("ursula-sidecar-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let db = dir.join("db").to_str().unwrap().to_owned();
        let sidecar = format!("{db}-ursula");
        fs::write(&db, b"x").unwrap();
        let trust = |line: &[u8], boot: Option<&str>| {
            fs::write(&sidecar, line).unwrap();
            read_sidecar(&sidecar)
                .unwrap()
                .map(|s| s.trusted(&db, boot, "i1"))
        };
        let b1 = Some("b1");
        let none = " wal=0000000000000000:0\n";
        let offset = "00000000000000000007";
        let mine = sidecar_line(
            offset,
            2,
            5,
            &stamp(&db, "http://h:1/b/s", b1, "i1"),
            WalClaim::NONE,
        )
        .replace(none, "");
        let here = format!("{mine}{none}");
        assert!(here.starts_with("00000000000000000007 2 v=2 log=5 boot=b1 stream=/b/s file="));
        assert!(here.contains(" incarnation=i1 wal="));
        assert_eq!(trust(here.as_bytes(), b1), Some(true));
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!((s.offset.as_str(), s.epoch, s.log), (offset, 2, 5));
        // The stream deleted and recreated (another incarnation), a sidecar from before
        // incarnations were recorded, or one of format 1 (a numeric offset; still parsed, for the
        // incarnation and the offset's read check before discarding).
        assert!(!s.trusted(&db, b1, "i2"));
        let legacy = here.replace(" incarnation=i1", "");
        assert_eq!(trust(legacy.as_bytes(), b1), Some(false));
        let v1 = here.replace(" v=2 log=5", "").replace(offset, "7");
        assert_eq!(trust(v1.as_bytes(), b1), Some(false));
        let s = read_sidecar(&sidecar).unwrap().unwrap();
        assert_eq!((s.version, s.offset.as_str()), (1, "7"));
        assert_eq!(s.incarnation.as_deref(), Some("i1"));
        assert_eq!(trust(here.as_bytes(), Some("b2")), Some(false));
        assert_eq!(trust(here.as_bytes(), None), Some(false));
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
        assert_eq!(trust(unknown.as_bytes(), None), Some(false));
        // A claim on WAL frames that are not there (no WAL here), or no claim at all.
        let behind = format!("{mine} wal=00000000000000ff:3\n");
        assert_eq!(trust(behind.as_bytes(), b1), Some(false));
        assert_eq!(trust(format!("{mine}\n").as_bytes(), b1), Some(false));
        // Another db file (replaced by a rename) is not trusted.
        let other_file = format!("{offset} 2 v=2 boot=b1 stream=/b/s file=0 incarnation=i1");
        assert_eq!(
            trust(format!("{other_file}{none}").as_bytes(), b1),
            Some(false)
        );
        assert_eq!(trust(b"7 2\n", b1), Some(false));
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
            assert_eq!(trust(torn.as_bytes(), b1), None);
        }
        assert_eq!(trust(b"\xff\xfe 7 2", b1), None);
        // A WAL of generation 0xaa with two commit frames of page 1 (images of 1s, then 2s): a
        // claim holds only for this generation (an older WAL with as many frames proves nothing),
        // `:0` only without a commit; folding it leaves the last image, cut to its db size.
        let sum = |mut s: (u32, u32), b: &[u8]| {
            for c in b.chunks_exact(8) {
                s.0 = s.0.wrapping_add(be32(c, 0).unwrap()).wrapping_add(s.1);
                s.1 = s.1.wrapping_add(be32(c, 4).unwrap()).wrapping_add(s.0);
            }
            s
        };
        let mut wal = Vec::new();
        for v in [0x377f_0683u32, 3_007_000, PAGE as u32, 0, 0, 0xaa] {
            wal.extend(v.to_be_bytes());
        }
        let mut s = sum((0, 0), &wal);
        wal.extend(s.0.to_be_bytes());
        wal.extend(s.1.to_be_bytes());
        for image in [1u8, 2] {
            let page = vec![image; PAGE];
            let mut h = Vec::new();
            for v in [1u32, 1, 0, 0xaa] {
                h.extend(v.to_be_bytes());
            }
            s = sum(sum(s, &h[..8]), &page);
            h.extend(s.0.to_be_bytes());
            h.extend(s.1.to_be_bytes());
            wal.extend(h);
            wal.extend(page);
        }
        fs::write(format!("{db}-wal"), &wal).unwrap();
        for (claim, trusted) in [("00000000000000aa:2", true), ("00000000000000bb:2", false)] {
            let line = format!("{mine} wal={claim}\n");
            assert_eq!(trust(line.as_bytes(), b1), Some(trusted), "{claim}");
        }
        assert_eq!(trust(here.as_bytes(), b1), Some(false));
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
}
