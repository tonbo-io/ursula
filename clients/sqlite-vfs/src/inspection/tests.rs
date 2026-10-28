use std::fs;
use std::path::PathBuf;

use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::Inspect;
use super::LockPath;
use super::PassError;
use super::Reason;
use super::Shape;
use super::Strict;
use super::check_host_dir;
use super::effective_uid;
use super::holds_write_lock;
use super::inspect;
use super::lock_path;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentHeader;
use crate::generation::is_root_name;
use crate::hostdir;
use crate::hostdir::Entry;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::replog::Log;
use crate::testdir::TempDir;

const ROOT: &str = "notes-0123456789abcdef0123456789abcdef";

fn header() -> SegmentHeader {
    SegmentHeader {
        lineage: Lineage([7; 16]),
        first_position: Position(1),
        supersedes: None,
        cluster_id: "c1".into(),
        stream_path: "/acme/notes".into(),
        incarnation: Incarnation::try_from(1 << 52).unwrap(),
        base: Position(0),
        acked: Position(0),
        acked_offset: Offset::from_u64(0),
        log_bytes: 0,
        claim: None,
    }
}

fn ack(position: u64) -> RecordBody {
    RecordBody::Ack {
        acked: Position(position),
        acked_offset: Offset::from_u64(position.saturating_mul(10)),
        log_bytes: 0,
    }
}

/// A host directory and a cache directory recorded in it, with the root [`ROOT`]: its `lock` and
/// a generation whose log holds `bodies`.
struct Host {
    _tmp: TempDir,
    host: PathBuf,
    cache: PathBuf,
}

impl Host {
    fn new(label: &str, bodies: &[RecordBody]) -> Host {
        let tmp = TempDir::new(label);
        let host = tmp.path().join("host");
        let cache = tmp.path().join("cache");
        fs::create_dir_all(&host).unwrap();
        fs::create_dir_all(cache.join(ROOT)).unwrap();
        fs::write(cache.join(ROOT).join("lock"), b"").unwrap();
        hostdir::record_dir(&host, &cache).unwrap();
        let log_dir = cache
            .join(ROOT)
            .join("gen-00000000000000000000000000000001")
            .join("log");
        let mut log = Log::create(&log_dir, header()).unwrap();
        for body in bodies {
            log.append(body).unwrap();
        }
        log.sync().unwrap();
        Host {
            _tmp: tmp,
            host,
            cache,
        }
    }

    fn log_dir(&self) -> PathBuf {
        self.cache
            .join(ROOT)
            .join("gen-00000000000000000000000000000001")
            .join("log")
    }

    /// An inspection of the host directory whose lock check reads the processes `pids`.
    fn inspect(&self, pids: Vec<u32>) -> super::Inspection {
        inspect(&Inspect {
            dir: Some(self.host.clone()),
            pids: Some(pids),
            ..Inspect::default()
        })
    }
}

fn commit(fill: u8, len: usize) -> RecordBody {
    RecordBody::Commit {
        frame: vec![fill; len],
    }
}

#[test]
fn root_names_and_lock_paths_have_one_grammar() {
    assert!(is_root_name(ROOT));
    assert!(is_root_name("a-0123456789abcdef0123456789abcdef"));
    assert!(is_root_name("my-db_2-0123456789abcdef0123456789abcdef"));
    let label = "a".repeat(64);
    assert!(is_root_name(&format!(
        "{label}-0123456789abcdef0123456789abcdef"
    )));
    for name in [
        "-0123456789abcdef0123456789abcdef",
        "Notes-0123456789abcdef0123456789abcdef",
        "notes-0123456789ABCDEF0123456789abcdef",
        "notes-0123456789abcdef0123456789abcde",
        "notes0123456789abcdef0123456789abcdef",
        "reader-0",
        "0.8",
    ] {
        assert!(!is_root_name(name), "{name}");
    }
    let long = "a".repeat(65);
    assert!(!is_root_name(&format!(
        "{long}-0123456789abcdef0123456789abcdef"
    )));

    let lock = format!("/home/app/.cache/sqlite-ursula/0.8/{ROOT}/lock");
    assert_eq!(lock_path(lock.as_bytes()), LockPath::Root);
    assert_eq!(
        lock_path(format!("{lock} (deleted)").as_bytes()),
        LockPath::Root
    );
    assert_eq!(lock_path(b"lock"), LockPath::No);
    // A reader's lock, also while a cleanup deletes its directory: readers hold no commit.
    assert_eq!(
        lock_path(format!("/c/{ROOT}/reader-3/lock").as_bytes()),
        LockPath::No
    );
    let trash = "reader-trash-0123456789abcdef0123456789abcdef";
    assert!(is_root_name(trash));
    assert_eq!(
        lock_path(format!("/c/{ROOT}/{trash}/lock (deleted)").as_bytes()),
        LockPath::No
    );
    assert_eq!(
        lock_path(format!("/c/{ROOT}/lock2").as_bytes()),
        LockPath::No
    );
    assert_eq!(
        lock_path(format!("/c/{ROOT}/gen-1/log").as_bytes()),
        LockPath::No
    );
    // A root renamed out of the grammar, or a name that is not UTF-8: a root's only when held.
    for other in [
        b"/c/notes-old/lock".as_slice(),
        b"/c/reader-x/lock",
        b"/c/\xff-0123456789abcdef0123456789abcdef/lock",
        b"/lock",
    ] {
        assert_eq!(lock_path(other), LockPath::Unnamed);
    }

    // The `lock:` lines of /proc/<pid>/fdinfo/<n>.
    let fdinfo = |lock: &str| format!("pos:\t0\nflags:\t02100002\nmnt_id:\t30\nino:\t1234\n{lock}");
    assert!(holds_write_lock(&fdinfo(
        "lock:\t1: FLOCK  ADVISORY  WRITE 4242 00:2f:1234 0 EOF\n"
    )));
    assert!(holds_write_lock(&fdinfo(
        "lock:\t1: POSIX  ADVISORY  WRITE 4242 00:2f:1234 0 EOF\n"
    )));
    assert!(holds_write_lock(&fdinfo(
        "lock:\t1: OFDLCK ADVISORY  WRITE -1 00:2f:1234 0 EOF\n"
    )));
    assert!(!holds_write_lock(&fdinfo(
        "lock:\t1: FLOCK  ADVISORY  READ 4242 00:2f:1234 0 EOF\n"
    )));
    assert!(!holds_write_lock(&fdinfo("")));
}

#[test]
fn the_host_directory_knows_each_name() {
    let key = "0123456789abcdef0123456789abcdef";
    assert_eq!(hostdir::entry(&format!("dir-{key}")), Entry::Record);
    assert_eq!(hostdir::entry(".tmp-123-0123456789abcdef"), Entry::Tmp);
    assert_eq!(hostdir::entry("hold"), Entry::Host);
    assert_eq!(hostdir::entry("host-view"), Entry::Host);
    for name in [
        "dir-0123",
        "dir-0123456789ABCDEF0123456789abcdef",
        ".tmp--0123456789abcdef",
        ".tmp-12-0123",
        ".tmp-x-0123456789abcdef",
        "holds",
        "stray",
    ] {
        assert_eq!(hostdir::entry(name), Entry::Unrecognized, "{name}");
    }
}

// The pass reads a root's pending bytes as the open does: the frame lengths of the generation's
// own COMMIT records above the latest ACK. Once they are acknowledged it is clean.
#[test]
fn a_pass_counts_the_bytes_the_open_would_resend() {
    let h = Host::new("inspect-bytes", &[
        commit(1, 100),
        ack(1),
        commit(2, 300),
        commit(3, 50),
    ]);
    let (_, recovered) = Log::open(&h.log_dir()).unwrap();
    let want: u64 = recovered
        .pending()
        .iter()
        .map(|(_, frame)| frame.len() as u64)
        .sum();
    assert_eq!(want, 350);

    let seen = h.inspect(Vec::new());
    assert!(seen.complete(), "{}", seen.to_json());
    assert!(!seen.clean());
    assert_eq!(seen.unacknowledged_bytes(), want);
    let json = seen.to_json();
    assert!(json.contains("\"state\":\"pending\""), "{json}");
    assert!(json.contains("\"commits\":2"), "{json}");
    assert!(
        json.contains("\"stream\":\"/acme/notes\",\"cluster\":\"c1\""),
        "{json}"
    );
    assert!(
        json.contains("\"from\":\"record\",\"present\":true"),
        "{json}"
    );
    assert!(json.contains("\"held\":false"), "{json}");

    let mut log = Log::open(&h.log_dir()).unwrap().0;
    // Records 1 to 4 are the log so far: the commits at 3 and 4 are pending.
    log.append(&ack(4)).unwrap();
    log.sync().unwrap();
    let seen = h.inspect(Vec::new());
    assert!(seen.clean(), "{}", seen.to_json());
}

// A root with no generation holds no commit and is not listed, and a cache directory that is gone
// holds no roots: both clean. A scanned directory is read like a recorded one, once per real path.
#[test]
fn absent_directories_and_roots_without_a_generation_are_clean() {
    let h = Host::new("inspect-absent", &[commit(1, 10)]);
    let other = h.cache.parent().unwrap().join("other");
    fs::create_dir_all(other.join(ROOT)).unwrap();
    fs::write(other.join(ROOT).join("lock"), b"").unwrap();
    fs::create_dir_all(
        other
            .join(ROOT)
            .join("gen-00000000000000000000000000000002.tmp"),
    )
    .unwrap();
    let seen = inspect(&Inspect {
        dir: Some(h.host.clone()),
        scans: vec![other.clone(), h.cache.clone()],
        pids: Some(Vec::new()),
        ..Inspect::default()
    });
    assert!(seen.complete(), "{}", seen.to_json());
    assert_eq!(seen.roots.len(), 1);
    assert_eq!(seen.dirs.len(), 2, "{}", seen.to_json());

    fs::remove_dir_all(&h.cache).unwrap();
    let seen = h.inspect(Vec::new());
    assert!(seen.clean(), "{}", seen.to_json());
    assert!(seen.to_json().contains("\"present\":false"));
}

// A stray name or a record this line does not read is unrecognized, a damaged log unreadable, and
// the first reason in precedence order is the one reported.
#[test]
fn what_the_pass_cannot_read_makes_it_incomplete() {
    let h = Host::new("inspect-incomplete", &[commit(1, 10)]);
    // Another format's magic in the first segment: refused, never cut.
    let segment = fs::read_dir(h.log_dir())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut bytes = fs::read(&segment).unwrap();
    bytes[..4].copy_from_slice(b"XXXX");
    fs::write(&segment, &bytes).unwrap();
    let seen = h.inspect(Vec::new());
    assert_eq!(
        seen.reason(),
        Some(Reason::Unreadable),
        "{}",
        seen.to_json()
    );
    assert!(seen.to_json().contains("\"state\":\"unreadable\""));
    assert!(seen.to_json().contains("\"unacknowledged_bytes\":null"));

    fs::write(h.host.join("stray"), b"").unwrap();
    let seen = h.inspect(Vec::new());
    assert_eq!(seen.reason(), Some(Reason::Unrecognized));
    assert!(seen.to_json().contains("\"incomplete\":\"unrecognized\""));
    fs::remove_file(h.host.join("stray")).unwrap();

    // Records of another format, of a relative path, or under another directory's name.
    for (line, name) in [
        ("{\"v\":2,\"dir\":\"/x\"}\n", hostdir::record_name("/x")),
        ("{\"v\":1,\"dir\":\"x\"}\n", hostdir::record_name("x")),
        ("{\"v\":1,\"dir\":\"/y\"}\n", hostdir::record_name("/x")),
        ("not json\n", hostdir::record_name("/z")),
    ] {
        fs::write(h.host.join(&name), line).unwrap();
        let seen = h.inspect(Vec::new());
        assert_eq!(seen.reason(), Some(Reason::Unrecognized), "{line}");
        fs::remove_file(h.host.join(&name)).unwrap();
    }
    // A temp file and the host's names are ignored.
    fs::write(h.host.join(".tmp-1-0123456789abcdef"), b"").unwrap();
    fs::write(h.host.join("hold"), b"").unwrap();
    assert_eq!(h.inspect(Vec::new()).reason(), Some(Reason::Unreadable));
}

#[test]
fn a_missing_host_directory_is_reported_and_the_rest_read() {
    let h = Host::new("inspect-no-host", &[commit(1, 10)]);
    let seen = inspect(&Inspect {
        dir: Some(h.host.join("missing")),
        scans: vec![h.cache.clone()],
        pids: Some(Vec::new()),
        ..Inspect::default()
    });
    assert_eq!(seen.reason(), Some(Reason::HostDir));
    assert_eq!(seen.unacknowledged_bytes(), 10);
    assert!(seen.to_json().contains("\"incomplete\":\"host_dir\""));
    assert!(seen.to_json().contains("\"from\":\"scan\""));

    // A link in place of the host directory.
    let link = h.host.parent().unwrap().join("link");
    std::os::unix::fs::symlink(&h.host, &link).unwrap();
    let seen = inspect(&Inspect {
        dir: Some(link),
        pids: Some(Vec::new()),
        ..Inspect::default()
    });
    assert_eq!(seen.reason(), Some(Reason::HostDir), "{}", seen.to_json());
}

#[test]
fn a_spent_budget_stops_the_pass() {
    let h = Host::new("inspect-budget", &[commit(1, 10)]);
    let seen = inspect(&Inspect {
        dir: Some(h.host.clone()),
        pids: Some(Vec::new()),
        budget: Some(std::time::Duration::ZERO),
        ..Inspect::default()
    });
    assert_eq!(seen.reason(), Some(Reason::Budget));
    assert!(seen.to_json().contains("\"incomplete\":\"budget\""));
}

// The lock check: a lock this process holds open is the lock of a root the pass read, wherever it
// was opened from, until its directory moves away from every path the pass reads. Other tests of
// this process hold locks of their own, so only this test's are looked at.
#[cfg(target_os = "linux")]
#[test]
fn an_open_lock_is_held_or_unread() {
    let h = Host::new("inspect-locks", &[commit(1, 10)]);
    let base = h.cache.parent().unwrap().to_owned();
    let ours = |seen: &super::Inspection| {
        seen.unread_locks
            .iter()
            .filter(|l| l.path.starts_with(&base))
            .count()
    };
    let lock = fs::File::open(h.cache.join(ROOT).join("lock")).unwrap();
    let me = std::process::id();
    let seen = h.inspect(vec![me]);
    assert_eq!(seen.roots[0].held, Some(true), "{}", seen.to_json());
    assert_eq!(ours(&seen), 0, "{}", seen.to_json());

    let moved = base.join("moved");
    fs::rename(&h.cache, &moved).unwrap();
    let seen = h.inspect(vec![me]);
    assert_eq!(ours(&seen), 1, "{}", seen.to_json());
    assert!(!seen.complete());
    assert!(
        seen.to_json()
            .contains(&format!("{{\"pid\":{me},\"path\":"))
    );
    assert!(
        seen.unread_locks
            .iter()
            .any(|l| l.path.ends_with(format!("moved/{ROOT}/lock")))
    );
    drop(lock);
    assert_eq!(ours(&h.inspect(vec![me])), 0);
}

// A file named `lock` whose directory has no root's name is a root's lock while a process holds an
// exclusive lock on it: a root renamed under a live process stays an unread lock.
#[cfg(target_os = "linux")]
#[test]
fn a_held_lock_under_a_renamed_root_is_unread() {
    let h = Host::new("inspect-renamed-root", &[commit(1, 10)]);
    let renamed = h.cache.join("notes-old");
    fs::rename(h.cache.join(ROOT), &renamed).unwrap();
    let ours = |seen: &super::Inspection| {
        seen.unread_locks
            .iter()
            .filter(|l| l.path.starts_with(&renamed))
            .count()
    };
    let lock = fs::File::open(renamed.join("lock")).unwrap();
    let me = std::process::id();
    // Open and not locked: not a root's lock by its name alone.
    let seen = h.inspect(vec![me]);
    assert_eq!(ours(&seen), 0, "{}", seen.to_json());
    lock.lock().unwrap();
    let seen = h.inspect(vec![me]);
    assert_eq!(ours(&seen), 1, "{}", seen.to_json());
    assert_eq!(seen.reason(), Some(Reason::UnreadLock));
    lock.unlock().unwrap();
    assert_eq!(ours(&h.inspect(vec![me])), 0);
}

// The strict checks apply to the compiled-in directory, found by its device and inode whatever the
// spelling, and to its parent and ancestors along the path it is compiled in as. Here a test's
// directory and owner stand in for them.
#[test]
fn the_compiled_in_host_directory_is_checked_strictly() {
    use std::os::unix::fs::MetadataExt;
    use std::os::unix::fs::PermissionsExt;
    let tmp = TempDir::new("inspect-strict");
    let base = fs::canonicalize(tmp.path()).unwrap();
    let parent = base.join("lib");
    let host = parent.join("host");
    fs::create_dir_all(&host).unwrap();
    let mode = |path: &std::path::Path, mode: u32| {
        fs::set_permissions(path, fs::Permissions::from_mode(mode)).unwrap();
    };
    mode(&parent, 0o755);
    mode(&host, 0o1777);
    let uid = fs::metadata(&host).unwrap().uid();
    let strict = Strict { path: &host, uid };
    let shape = |r: Result<_, PassError>| match r {
        Err(PassError::HostDirShape { why, .. }) => Some(why),
        Err(e) => panic!("{e}"),
        Ok(_) => None,
    };
    check_host_dir(&host, strict).unwrap();
    // Another spelling of it is checked as strictly.
    let other = parent.join("..").join("lib").join("host");
    check_host_dir(&other, strict).unwrap();

    mode(&host, 0o755);
    assert!(matches!(
        shape(check_host_dir(&other, strict)),
        Some(Shape::Mode {
            mode: 0o755,
            want: 0o1777
        })
    ));
    // Any other directory needs only to be a directory.
    let elsewhere = base.join("elsewhere");
    fs::create_dir(&elsewhere).unwrap();
    mode(&elsewhere, 0o700);
    check_host_dir(&elsewhere, strict).unwrap();
    mode(&host, 0o1777);

    assert!(matches!(
        shape(check_host_dir(&host, Strict {
            path: &host,
            uid: uid.wrapping_add(1)
        })),
        Some(Shape::Owner { .. })
    ));
    mode(&parent, 0o775);
    assert!(matches!(
        shape(check_host_dir(&host, strict)),
        Some(Shape::Mode {
            mode: 0o775,
            want: 0o755
        })
    ));
    mode(&parent, 0o755);

    // A link among the ancestors of the compiled-in path.
    let link = base.join("link");
    std::os::unix::fs::symlink(&parent, &link).unwrap();
    let through = link.join("host");
    assert!(matches!(
        shape(check_host_dir(&through, Strict {
            path: &through,
            uid
        })),
        Some(Shape::NotADirectory)
    ));
    // A link in its place, or a file.
    assert!(matches!(
        shape(check_host_dir(&link, Strict { path: &link, uid })),
        Some(Shape::NotADirectory)
    ));
    let file = base.join("file");
    fs::write(&file, b"").unwrap();
    assert!(matches!(
        shape(check_host_dir(&file, strict)),
        Some(Shape::NotADirectory)
    ));
}

// An entry with a root's name that cannot be looked at may be a root: the directory is unreadable,
// not empty.
#[test]
fn a_root_that_cannot_be_looked_at_is_unreadable() {
    use std::os::unix::fs::PermissionsExt;
    let h = Host::new("inspect-unstattable", &[commit(1, 10)]);
    if effective_uid().is_ok_and(|uid| uid == 0) {
        // Root looks at it anyway.
        return;
    }
    // Listable and not searchable: the names are read, their files cannot be.
    fs::set_permissions(&h.cache, fs::Permissions::from_mode(0o444)).unwrap();
    let seen = h.inspect(Vec::new());
    fs::set_permissions(&h.cache, fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        seen.reason(),
        Some(Reason::Unreadable),
        "{}",
        seen.to_json()
    );
    assert!(seen.roots.is_empty());
}

// The check of every process needs root: another user's is incomplete, and `held` is null while the
// lock check did not run.
#[test]
fn the_check_of_every_process_needs_root() {
    let h = Host::new("inspect-not-root", &[commit(1, 10)]);
    let seen = inspect(&Inspect {
        dir: Some(h.host.clone()),
        ..Inspect::default()
    });
    if effective_uid().is_ok_and(|uid| uid == 0) {
        return;
    }
    assert_eq!(
        seen.reason(),
        Some(Reason::UnreadLock),
        "{}",
        seen.to_json()
    );
    assert_eq!(seen.roots[0].held, None);
    assert!(
        seen.to_json().contains("\"held\":null"),
        "{}",
        seen.to_json()
    );
}
