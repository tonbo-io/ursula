use std::fs;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::symlink;
use std::path::Path;
use std::path::PathBuf;

use super::Failure;
use super::open_checked;
use super::remove_record;
use crate::hostdir::record_dir;
use crate::hostdir::record_line;
use crate::hostdir::record_name;
use crate::hostdir::tmp_pid;
use crate::inspection::FileId;
use crate::inspection::RecordError;
use crate::testdir::TempDir;

/// The names in `dir`, sorted.
fn names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
}

fn id(path: &Path) -> FileId {
    let meta = fs::symlink_metadata(path).unwrap();
    (meta.dev(), meta.ino())
}

/// A host directory in `t` holding the record of a cache directory that was deleted since: the
/// host directory, the record's name and the cache directory's real path.
fn recorded_then_deleted(t: &TempDir) -> (PathBuf, String, PathBuf) {
    let host = t.path().join("host");
    fs::create_dir(&host).unwrap();
    let cache = t.path().join("cache");
    fs::create_dir(&cache).unwrap();
    let real = fs::canonicalize(&cache).unwrap();
    record_dir(&host, &cache).unwrap();
    fs::remove_dir(&cache).unwrap();
    let name = record_name(real.to_str().unwrap());
    assert_eq!(names(&host), std::slice::from_ref(&name));
    (host, name, real)
}

/// Replaces the record `name` in `host` with `bytes`.
fn rewrite(host: &Path, name: &str, bytes: &str) {
    fs::remove_file(host.join(name)).unwrap();
    fs::write(host.join(name), bytes).unwrap();
}

// The record of a directory that is gone is unlinked through the descriptor of the host directory
// the pass checked.
#[test]
fn a_removal_unlinks_the_record_of_an_absent_directory() {
    let t = TempDir::new("prune-remove");
    let (host, name, real) = recorded_then_deleted(&t);
    let handle = open_checked(&host, id(&host)).unwrap();
    remove_record(&handle, &name, &real).unwrap();
    assert!(names(&host).is_empty());

    // Something that is not a directory at the path is absent too.
    let t = TempDir::new("prune-file");
    let (host, name, real) = recorded_then_deleted(&t);
    fs::write(&real, b"").unwrap();
    let handle = open_checked(&host, id(&host)).unwrap();
    remove_record(&handle, &name, &real).unwrap();
    assert!(names(&host).is_empty());
}

// A host directory whose path names another directory than the pass checked is refused before any
// removal, and the directory the pass read keeps its record.
#[test]
fn a_removal_refuses_a_host_directory_that_moved() {
    let t = TempDir::new("prune-moved");
    let (host, name, _) = recorded_then_deleted(&t);
    let checked = id(&host);
    let moved = t.path().join("moved");
    fs::rename(&host, &moved).unwrap();
    fs::create_dir(&host).unwrap();
    assert!(matches!(
        open_checked(&host, checked),
        Err(Failure::Moved { .. })
    ));
    // A link to the directory the pass read is not it either: the open never follows one.
    fs::remove_dir(&host).unwrap();
    symlink(&moved, &host).unwrap();
    assert!(matches!(
        open_checked(&host, checked),
        Err(Failure::HostDir { op: "open", .. })
    ));
    assert_eq!(names(&moved), [name]);
}

// A record is removed only while its directory is still absent and the record read again is still
// a record of that directory.
#[test]
fn a_removal_refuses_a_record_that_changed() {
    let t = TempDir::new("prune-changed");
    let (host, name, real) = recorded_then_deleted(&t);
    let handle = open_checked(&host, id(&host)).unwrap();

    // The directory came back.
    fs::create_dir(&real).unwrap();
    assert!(matches!(
        remove_record(&handle, &name, &real),
        Err(Failure::Present { .. })
    ));
    fs::remove_dir(&real).unwrap();

    // Another format's record, a record of another directory under this name, and a link.
    rewrite(&host, &name, "{\"v\":2,\"dir\":\"/elsewhere\"}\n");
    assert!(matches!(
        remove_record(&handle, &name, &real),
        Err(Failure::Changed {
            source: RecordError::Version { found: Some(2) },
            ..
        })
    ));
    rewrite(&host, &name, &record_line("/elsewhere"));
    assert!(matches!(
        remove_record(&handle, &name, &real),
        Err(Failure::Changed {
            source: RecordError::Misnamed { .. },
            ..
        })
    ));
    let target = t.path().join("target");
    fs::write(&target, record_line(real.to_str().unwrap())).unwrap();
    fs::remove_file(host.join(&name)).unwrap();
    symlink(&target, host.join(&name)).unwrap();
    assert!(matches!(
        remove_record(&handle, &name, &real),
        Err(Failure::Changed {
            source: RecordError::NotAFile,
            ..
        })
    ));
    assert_eq!(names(&host), std::slice::from_ref(&name));

    // Gone since the pass.
    fs::remove_file(host.join(&name)).unwrap();
    assert!(matches!(
        remove_record(&handle, &name, &real),
        Err(Failure::Read { source, .. }) if source.kind() == std::io::ErrorKind::NotFound
    ));
}

#[test]
fn a_temp_files_name_carries_its_writers_pid() {
    assert_eq!(tmp_pid(".tmp-4242-0123456789abcdef"), Some(4242));
    // These are not PIDs `kill` can probe, so their files stay.
    assert_eq!(tmp_pid(".tmp-0-0123456789abcdef"), None);
    assert_eq!(tmp_pid(".tmp-2147483648-0123456789abcdef"), None);
    assert_eq!(tmp_pid(".tmp-99999999999-0123456789abcdef"), None);
    assert_eq!(
        tmp_pid(".tmp-2147483647-0123456789abcdef"),
        Some(2_147_483_647)
    );
    assert_eq!(tmp_pid(".tmp-4242-0123456789abcde"), None);
    assert_eq!(tmp_pid("dir-0123456789abcdef0123456789abcdef"), None);
}
