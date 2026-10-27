use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Barrier;

use super::Found;
use super::HostDirError;
use super::Malformed;
use super::record_dir;
use super::record_line;
use super::record_name;
use super::recorded_dir;
use super::resolve;
use super::utf8;
use crate::config::host_dir_hook;
use crate::generation::Root;
use crate::generation::canonical_path;
use crate::testdir::TempDir;

fn lstat(path: &Path) -> std::io::Result<()> {
    fs::symlink_metadata(path).map(drop)
}

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

/// The name of the record of `cache`, by its real path.
fn name_of(cache: &Path) -> String {
    record_name(fs::canonicalize(cache).unwrap().to_str().unwrap())
}

// A default path that `lstat` finds selects it, whatever it is, over the hook: a directory, a
// regular file or a symlink (which then fails the open). One `lstat` cannot reach counts as absent.
#[test]
fn the_default_wins_whenever_lstat_finds_it() {
    let t = TempDir::new("hostdir-resolve");
    let hook = t.path().join("hook");
    fs::create_dir(&hook).unwrap();
    let dir = t.path().join("dir");
    fs::create_dir(&dir).unwrap();
    let file = t.path().join("file");
    fs::write(&file, b"").unwrap();
    let link = t.path().join("link");
    symlink(t.path().join("nowhere"), &link).unwrap();
    for default in [&dir, &file, &link] {
        assert_eq!(
            resolve(default, Some(hook.as_path()), &lstat),
            Some(default.clone())
        );
        assert_eq!(resolve(default, None, &lstat), Some(default.clone()));
    }

    // EACCES (a confinement policy) and ENOTDIR (a file above it) count as absent.
    for errno in [libc::EACCES, libc::EPERM, libc::ENOTDIR, libc::ELOOP] {
        let denied = |path: &Path| {
            if path == dir {
                Err(std::io::Error::from_raw_os_error(errno))
            } else {
                lstat(path)
            }
        };
        assert_eq!(
            resolve(&dir, Some(hook.as_path()), &denied),
            Some(hook.clone())
        );
        assert_eq!(resolve(&dir, None, &denied), None);
    }
}

// Without a default, the hook decides: a path `lstat` finds, whatever it is, or nothing. A hook
// that is unset, empty or relative is none.
#[test]
fn without_the_default_the_hook_decides() {
    let t = TempDir::new("hostdir-hook");
    let missing = t.path().join("missing");
    let hook = t.path().join("hook");
    fs::write(&hook, b"").unwrap();
    assert_eq!(
        resolve(&missing, Some(hook.as_path()), &lstat),
        Some(hook.clone())
    );
    assert_eq!(
        resolve(&missing, Some(t.path().join("gone").as_path()), &lstat),
        None
    );
    assert_eq!(resolve(&missing, None, &lstat), None);

    assert_eq!(host_dir_hook(None), None);
    assert_eq!(host_dir_hook(Some(OsString::new())), None);
    assert_eq!(host_dir_hook(Some("relative/host".into())), None);
    assert_eq!(host_dir_hook(Some("./host".into())), None);
    assert_eq!(
        host_dir_hook(Some("/tmp/host".into())),
        Some(PathBuf::from("/tmp/host"))
    );
}

// The key is the first 32 hex digits of the BLAKE3 of the real path, and the record one line of
// JSON that names it. A reader takes the directory whatever the version, ignoring other keys.
#[test]
fn a_record_names_the_real_path_under_its_hash() {
    let a = "/home/app/.cache/sqlite-ursula/0.8";
    let name = record_name(a);
    let hash = blake3::hash(a.as_bytes()).to_hex();
    assert_eq!(name, format!("dir-{}", hash.as_str().get(..32).unwrap()));
    assert_ne!(record_name("/home/app/.cache/sqlite-ursula/0.9"), name);
    assert_eq!(
        record_line(a),
        "{\"v\":1,\"dir\":\"/home/app/.cache/sqlite-ursula/0.8\"}\n"
    );
    for path in [a, "/a \"quoted\"\\dir", "/caf\u{e9}/\u{1}"] {
        assert_eq!(recorded_dir(record_line(path).as_bytes()).unwrap(), path);
    }
    assert_eq!(
        recorded_dir(br#"{"v":2,"dir":"/x","host":"later"}"#).unwrap(),
        "/x"
    );
    assert!(matches!(recorded_dir(br#"{"v":1}"#), Err(Malformed::NoDir)));
    assert!(matches!(
        recorded_dir(br#"{"v":1,"dir":7}"#),
        Err(Malformed::NoDir)
    ));
    assert!(matches!(recorded_dir(b"\xff"), Err(Malformed::NotUtf8(_))));
    assert!(matches!(
        recorded_dir(b"{\"dir\""),
        Err(Malformed::Syntax(_))
    ));
    let long = format!("{{\"dir\":\"/x\",\"pad\":\"{}\"}}", "a".repeat(64 << 10));
    assert!(matches!(
        recorded_dir(long.as_bytes()),
        Err(Malformed::TooLong)
    ));
}

// A real path that is not UTF-8 cannot be recorded as JSON text: the open fails.
#[test]
fn a_real_path_that_is_not_utf8_fails() {
    let real = PathBuf::from(OsString::from_vec(b"/cache/\xff".to_vec()));
    assert!(matches!(
        utf8(Path::new("/host"), real),
        Err(HostDirError::NotUtf8 { .. })
    ));
}

// An owner open records the real path of its cache directory, given through a symlink: one record,
// mode 0444, and no temp file. Later opens in the directory leave it as it is.
#[test]
fn an_open_records_its_cache_directory_once() {
    let t = TempDir::new("hostdir-record");
    let host = t.path().join("host");
    fs::create_dir(&host).unwrap();
    let cache = t.path().join("cache");
    fs::create_dir(&cache).unwrap();
    let alias = t.path().join("alias");
    symlink(&cache, &alias).unwrap();

    record_dir(&host, &alias).unwrap();
    let name = name_of(&cache);
    assert_eq!(names(&host), vec![name.clone()]);
    let record = host.join(&name);
    let real = fs::canonicalize(&cache).unwrap();
    assert_eq!(
        fs::read_to_string(&record).unwrap(),
        record_line(real.to_str().unwrap())
    );
    let meta = fs::metadata(&record).unwrap();
    assert_eq!(meta.permissions().mode() & 0o7777, 0o444);

    record_dir(&host, &cache).unwrap();
    record_dir(&host, &alias).unwrap();
    assert_eq!(names(&host), vec![name]);
    let again = fs::metadata(&record).unwrap();
    assert_eq!(
        (again.ino(), again.mtime_nsec()),
        (meta.ino(), meta.mtime_nsec())
    );
}

// Eight threads that lock eight roots in one cache directory and record it at once all succeed,
// and leave one record and no temp file: the losers of the link check the winner's record.
#[test]
fn eight_threads_recording_eight_roots_leave_one_record() {
    let t = TempDir::new("hostdir-race");
    let host = t.path().join("host");
    fs::create_dir(&host).unwrap();
    let cache = t.path().join("cache");
    let barrier = Arc::new(Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let (host, cache, barrier) = (host.clone(), cache.clone(), Arc::clone(&barrier));
            std::thread::spawn(move || {
                let name = canonical_path(&format!("https://h/b/s{i}")).unwrap();
                let root = Root::lock(&cache, &name).unwrap();
                barrier.wait();
                let recorded = record_dir(&host, &cache);
                drop(root);
                recorded
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap().unwrap();
    }
    assert_eq!(names(&host), vec![name_of(&cache)]);
}

// A record under the directory's name that names another directory, or is not a record, fails the
// open, and so does a symlink in its place: it is never followed. A later line's record of the
// same directory serves.
#[test]
fn a_record_that_is_not_the_directorys_fails_the_open() {
    let t = TempDir::new("hostdir-conflict");
    let host = t.path().join("host");
    fs::create_dir(&host).unwrap();
    let cache = t.path().join("cache");
    fs::create_dir(&cache).unwrap();
    let record = host.join(name_of(&cache));

    fs::write(&record, record_line("/elsewhere")).unwrap();
    let err = record_dir(&host, &cache).unwrap_err();
    assert!(
        matches!(&err, HostDirError::Conflict { found: Found::Other(other), .. } if other == "/elsewhere"),
        "{err}"
    );

    fs::write(&record, b"not json\n").unwrap();
    let err = record_dir(&host, &cache).unwrap_err();
    assert!(
        matches!(&err, HostDirError::Conflict {
            found: Found::Malformed(Malformed::Syntax(_)),
            ..
        }),
        "{err}"
    );

    fs::remove_file(&record).unwrap();
    let target = t.path().join("target");
    fs::write(
        &target,
        record_line(fs::canonicalize(&cache).unwrap().to_str().unwrap()),
    )
    .unwrap();
    symlink(&target, &record).unwrap();
    let err = record_dir(&host, &cache).unwrap_err();
    assert!(matches!(&err, HostDirError::Io { op: "read", .. }), "{err}");

    fs::remove_file(&record).unwrap();
    let real = fs::canonicalize(&cache).unwrap();
    let later = format!(
        "{{\"v\":2,\"dir\":\"{}\",\"since\":\"a later line\"}}\n",
        real.to_str().unwrap()
    );
    fs::write(&record, &later).unwrap();
    record_dir(&host, &cache).unwrap();
    assert_eq!(fs::read_to_string(&record).unwrap(), later);
}

// A record a host deleted is written again by the next open that locks a root in the directory.
#[test]
fn a_deleted_record_is_written_again() {
    let t = TempDir::new("hostdir-deleted");
    let host = t.path().join("host");
    fs::create_dir(&host).unwrap();
    let cache = t.path().join("cache");
    fs::create_dir(&cache).unwrap();
    record_dir(&host, &cache).unwrap();
    let name = name_of(&cache);
    fs::remove_file(host.join(&name)).unwrap();
    assert!(names(&host).is_empty());
    record_dir(&host, &cache).unwrap();
    assert_eq!(names(&host), vec![name]);
}

// A host directory that is a regular file, or a symlink to a directory, fails the open, and nothing
// is written through the link.
#[test]
fn a_host_directory_that_is_not_a_directory_fails_the_open() {
    let t = TempDir::new("hostdir-type");
    let cache = t.path().join("cache");
    fs::create_dir(&cache).unwrap();

    let file = t.path().join("file");
    fs::write(&file, b"").unwrap();
    let err = record_dir(&file, &cache).unwrap_err();
    assert!(matches!(&err, HostDirError::Io { op: "open", .. }), "{err}");

    let target = t.path().join("target");
    fs::create_dir(&target).unwrap();
    let link = t.path().join("link");
    symlink(&target, &link).unwrap();
    let err = record_dir(&link, &cache).unwrap_err();
    assert!(matches!(&err, HostDirError::Io { op: "open", .. }), "{err}");
    assert!(names(&target).is_empty());
}

// A host directory the process cannot create files in fails the open, and leaves nothing. Skipped
// where the permission bits do not bind (root).
#[test]
fn a_host_directory_the_process_cannot_write_fails_the_open() {
    let t = TempDir::new("hostdir-readonly");
    let host = t.path().join("host");
    fs::create_dir(&host).unwrap();
    let cache = t.path().join("cache");
    fs::create_dir(&cache).unwrap();
    fs::set_permissions(&host, fs::Permissions::from_mode(0o555)).unwrap();
    let probe = host.join("probe");
    if fs::write(&probe, b"").is_ok() {
        fs::remove_file(&probe).unwrap();
        fs::set_permissions(&host, fs::Permissions::from_mode(0o755)).unwrap();
        return;
    }
    let err = record_dir(&host, &cache).unwrap_err();
    fs::set_permissions(&host, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        matches!(&err, HostDirError::Io { op: "create", .. }),
        "{err}"
    );
    assert!(names(&host).is_empty());
}
