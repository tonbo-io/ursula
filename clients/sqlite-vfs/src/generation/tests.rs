use std::fs;
use std::os::unix::fs::FileExt;

use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::GenerationError;
use super::PathError;
use super::Root;
use super::canonical_path;
use super::root_name;
use crate::codec::marker::empty_page_one;
use crate::codec::segment::SegmentHeader;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::testdir::TempDir;

fn path(url: &str) -> String {
    canonical_path(url).unwrap().as_str().to_owned()
}

#[test]
fn every_spelling_of_a_path_is_one_canonical_path() {
    let want = "/v1/stream/my%20db";
    for url in [
        "https://example.com/v1/stream/my%20db",
        "http://user:pw@other.host:8080/v1/stream/my%20db/",
        "HTTPS://example.com/v1/stream/my db",
        "https://example.com/v1/%73tream/my%20db//",
        "https://example.com/v1/x/../stream/./my%20db",
        "https://example.com/v1/stream/my%20db/x/%2e%2E",
    ] {
        assert_eq!(path(url), want, "{url}");
    }
    // encodeURIComponent's set stays as it is; everything else is escaped in upper case.
    assert_eq!(path("https://h/a/b-_.!~*'()"), "/a/b-_.!~*'()");
    assert_eq!(path("https://h/a/caf%c3%a9"), "/a/caf%C3%A9");
    assert_eq!(path("https://h/a/café"), "/a/caf%C3%A9");
    assert_eq!(path("https://h/a/%2F"), "/a/%2F");
    // A segment that does not decode stays as it is.
    assert_eq!(path("https://h/a/%zz"), "/a/%zz");
    assert_eq!(path("https://h/a/%ff"), "/a/%ff");
    // Empty middle segments are part of the path.
    assert_eq!(path("https://h/a//b"), "/a//b");
}

#[test]
fn urls_that_name_no_stream_are_refused() {
    assert!(matches!(
        canonical_path("ftp://h/a"),
        Err(PathError::Scheme { .. })
    ));
    assert!(matches!(
        canonical_path("/a/b"),
        Err(PathError::Scheme { .. })
    ));
    assert!(matches!(
        canonical_path("https://h/a?x=1"),
        Err(PathError::QueryOrFragment { .. })
    ));
    assert!(matches!(
        canonical_path("https://h/a#f"),
        Err(PathError::QueryOrFragment { .. })
    ));
    for url in ["https://h", "https://h/", "https://h/a/..", "https://h//"] {
        assert!(
            matches!(canonical_path(url), Err(PathError::NoPath { .. })),
            "{url}"
        );
    }
}

#[test]
fn the_root_name_is_a_label_and_the_path_hash() {
    let p = canonical_path("https://h/v1/stream/My%20Notes.db").unwrap();
    let name = root_name(&p);
    let hash = blake3::hash(b"/v1/stream/My%20Notes.db").to_hex();
    assert_eq!(
        name,
        format!("stream-my_notes_db-{}", hash.as_str().get(..32).unwrap())
    );
    // Host aliases share the root.
    assert_eq!(
        root_name(&canonical_path("http://other:1/v1/stream/My%20Notes.db/").unwrap()),
        name
    );
    let dashed = root_name(&canonical_path("https://h/-a").unwrap());
    assert!(dashed.starts_with("_a-"), "{dashed}");
    let long = root_name(&canonical_path(&format!("https://h/{}", "x".repeat(100))).unwrap());
    assert_eq!(long.len(), 64 + 1 + 32);
}

fn header(lineage: u8) -> SegmentHeader {
    SegmentHeader {
        lineage: Lineage([lineage; 16]),
        first_position: Position(1),
        supersedes: None,
        cluster_id: "cluster".into(),
        stream_path: "/v1/stream/db".into(),
        incarnation: Incarnation::try_from(1 << 52).unwrap(),
        base: Position(0),
        acked: Position(0),
        acked_offset: Offset::from_u64(0),
        log_bytes: 0,
        claim: None,
    }
}

#[test]
fn a_second_lock_on_the_root_is_in_use() {
    let dir = TempDir::new("gen-lock");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    assert!(matches!(
        Root::lock(dir.path(), &p),
        Err(GenerationError::InUse { .. })
    ));
    drop(root);
    Root::lock(dir.path(), &p).unwrap();
}

#[test]
fn a_built_generation_becomes_current_once_published() {
    let dir = TempDir::new("gen-build");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    assert_eq!(root.current().unwrap(), None);
    let building = root.build(header(1)).unwrap();
    assert_eq!(root.current().unwrap(), None);
    fs::write(building.db_path(), b"pages").unwrap();
    let g = building.publish().unwrap();
    assert_eq!(root.current().unwrap(), Some(g.clone()));
    assert_eq!(fs::read(g.db()).unwrap(), b"pages");
    assert!(g.log_dir().join("00000000000000000001.usl").exists());
    // The next one supersedes it.
    let g2 = root.build(header(2)).unwrap().publish().unwrap();
    assert_eq!(root.current().unwrap(), Some(g2.clone()));
    assert_eq!(g2.first_header().unwrap().supersedes, Some(g.id));
    assert!(!g.dir.exists());
}

#[test]
fn a_crash_after_each_publish_step_converges_to_one_generation() {
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let mut steps = 0;
    loop {
        let dir = TempDir::new("gen-crash");
        let root = Root::lock(dir.path(), &p).unwrap();
        let old = root.build(header(1)).unwrap().publish().unwrap();
        let done = root.build(header(2)).unwrap().crash_after(steps).unwrap();
        // The process dies, its lock with it, possibly midway through deleting the trash.
        drop(root);
        let trash = dir
            .path()
            .join(root_name(&p))
            .join(format!("gen-{}.trash", old.id.hex()));
        if trash.exists() {
            fs::remove_dir_all(trash.join("log")).unwrap();
        }
        let root = Root::lock(dir.path(), &p).unwrap();
        root.finish_leftovers().unwrap();
        let current = root.current().unwrap().unwrap();
        let entries: Vec<String> = fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n != "lock")
            .collect();
        assert_eq!(entries.len(), 1, "after {steps} steps: {entries:?}");
        // Until the rename (step 4) the old generation stays, after it the new one.
        let want = if steps >= 4 {
            Lineage([2; 16])
        } else {
            old.lineage
        };
        assert_eq!(current.lineage, want, "after {steps} steps");
        if done < steps {
            break;
        }
        steps += 1;
    }
    assert_eq!(steps, 10);
}

#[test]
fn an_unreadable_generation_another_supersedes_is_finished() {
    let dir = TempDir::new("gen-unreadable");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let old = root.build(header(1)).unwrap().publish().unwrap();
    // A crash right after the new generation's rename, then the old one lost its log.
    root.build(header(2)).unwrap().crash_after(5).unwrap();
    fs::remove_dir_all(old.log_dir()).unwrap();
    drop(root);
    let root = Root::lock(dir.path(), &p).unwrap();
    root.finish_leftovers().unwrap();
    assert_eq!(root.current().unwrap().unwrap().lineage, Lineage([2; 16]));
}

#[test]
fn an_unreadable_generation_nothing_supersedes_is_an_error() {
    let dir = TempDir::new("gen-orphan");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let a = root.build(header(1)).unwrap().publish().unwrap();
    let side = dir.path().join("side");
    fs::rename(&a.dir, &side).unwrap();
    let b = root.build(header(2)).unwrap().publish().unwrap();
    fs::rename(&side, &a.dir).unwrap();
    // `b` supersedes nothing (it was built with no current generation), and `a` is unreadable.
    fs::remove_dir_all(a.log_dir()).unwrap();
    assert_eq!(b.first_header().unwrap().supersedes, None);
    assert!(matches!(
        root.finish_leftovers(),
        Err(GenerationError::NoSegment { .. } | GenerationError::Io { .. })
    ));
}

// A generation directory is named by its id, and its lineage is read from its log: the current
// generation is found by its directory, whatever lineage it holds.
#[test]
fn a_generation_is_found_by_its_directory_id() {
    let dir = TempDir::new("gen-id");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let g = root.build(header(1)).unwrap().publish().unwrap();
    assert_eq!(
        g.dir.file_name().unwrap().to_str().unwrap(),
        format!("gen-{}", g.id.hex())
    );
    assert_ne!(
        g.id.0, g.lineage.0,
        "the directory is not named by the lineage"
    );
    let current = root.current().unwrap().unwrap();
    assert_eq!((current.id, current.lineage), (g.id, Lineage([1; 16])));
    assert_eq!(current.wal(), g.dir.join("db-uwal"));
}

// Two directories of one lineage (a copy keeps its lineage) are told apart by id: the one whose
// first segment names the other's id supersedes it, and without that the root is ambiguous.
#[test]
fn two_directories_of_one_lineage_resolve_by_supersedes() {
    let dir = TempDir::new("gen-same-lineage");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let old = root.build(header(1)).unwrap().publish().unwrap();
    root.build(header(1)).unwrap().crash_after(5).unwrap();
    drop(root);
    let root = Root::lock(dir.path(), &p).unwrap();
    root.finish_leftovers().unwrap();
    let current = root.current().unwrap().unwrap();
    assert_eq!(current.lineage, old.lineage);
    assert_ne!(current.id, old.id);
    assert_eq!(current.first_header().unwrap().supersedes, Some(old.id));
    assert!(!old.dir.exists());

    // A second directory of the lineage that supersedes nothing: neither wins.
    let side = dir.path().join("side");
    fs::rename(&current.dir, &side).unwrap();
    let other = root.build(header(1)).unwrap().publish().unwrap();
    fs::rename(&side, &current.dir).unwrap();
    assert_eq!(other.first_header().unwrap().supersedes, None);
    root.finish_leftovers().unwrap();
    assert!(matches!(
        root.current(),
        Err(GenerationError::Ambiguous { ids, .. }) if ids.len() == 2
    ));
}

// A generation nothing was applied to is published with the page 1 of an empty database, marked
// (design D7): no published db file is empty or unmarked. One a commit or a snapshot wrote keeps
// its pages.
#[test]
fn a_generation_is_published_with_page_one_written() {
    let dir = TempDir::new("gen-empty");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let g = root.build(header(1)).unwrap().publish().unwrap();
    assert_eq!(
        fs::read(g.db()).unwrap(),
        empty_page_one(g.lineage).to_vec()
    );
    let building = root.build(header(2)).unwrap();
    building.db.write_all_at(&[7u8; 8192], 0).unwrap();
    let g2 = building.publish().unwrap();
    assert_eq!(fs::read(g2.db()).unwrap(), vec![7u8; 8192]);
}
