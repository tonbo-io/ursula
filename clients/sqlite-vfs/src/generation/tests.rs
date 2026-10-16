use std::fs;
use std::os::unix::fs::FileExt;

use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::Fate;
use super::GenerationError;
use super::PathError;
use super::Root;
use super::canonical_path;
use super::root_name;
use crate::codec::marker::SQLITE_MAGIC;
use crate::codec::marker::empty_page_one;
use crate::codec::marker::is_marked;
use crate::codec::segment::RecordBody;
use crate::codec::segment::SegmentHeader;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::replog::Log;
use crate::salvage;
use crate::salvage::Manifest;
use crate::salvage::Reason;
use crate::salvage::State;
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

// A reader locks the first free `reader-<n>/` of the root, beside the owner's lock, and its
// generations stay there: the owner's open never sees them.
#[test]
fn readers_lock_the_first_free_reader_directory_beside_the_owner() {
    let dir = TempDir::new("gen-reader");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let owner = Root::lock(dir.path(), &p).unwrap();
    let r0 = Root::reader(dir.path(), &p).unwrap();
    let r1 = Root::reader(dir.path(), &p).unwrap();
    assert_eq!(r0.path(), owner.path().join("reader-0"));
    assert_eq!(r1.path(), owner.path().join("reader-1"));
    let g = r0.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    assert!(g.dir.starts_with(r0.path()));
    owner.finish_leftovers().unwrap();
    assert_eq!(owner.current().unwrap(), None);
    // A freed directory is taken again first, its generation kept.
    drop(r0);
    let again = Root::reader(dir.path(), &p).unwrap();
    assert_eq!(again.path(), owner.path().join("reader-0"));
    again.finish_leftovers().unwrap();
    assert_eq!(again.current().unwrap(), Some(g));
    drop(r1);
}

// Readers taking and letting go of reader directories on many threads, each taking one cleaning
// the others up: no two ever hold one directory at once, and no cleanup deletes a file in a held
// one.
#[test]
fn concurrent_readers_and_cleanups_never_share_or_delete_a_held_directory() {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex;

    let dir = TempDir::new("gen-reader-stress");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let held = Arc::new(Mutex::new(HashSet::new()));
    let threads: Vec<_> = (0..8)
        .map(|t| {
            let (dir, p, held) = (dir.path().to_owned(), p.clone(), Arc::clone(&held));
            std::thread::spawn(move || {
                for i in 0..150 {
                    let root = Root::reader(&dir, &p).unwrap();
                    let path = root.path().to_owned();
                    assert!(
                        held.lock().unwrap().insert(path.clone()),
                        "{} held twice",
                        path.display()
                    );
                    let mine = path.join(format!("mine-{t}-{i}"));
                    fs::write(&mine, b"x").unwrap();
                    std::thread::yield_now();
                    assert!(mine.exists(), "{} deleted under its holder", mine.display());
                    fs::remove_file(&mine).unwrap();
                    assert!(held.lock().unwrap().remove(&path));
                    drop(root);
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    // The last cleanup leaves at most the directories still in use, and no trash.
    let last = Root::reader(dir.path(), &p).unwrap();
    let root = last.path().parent().unwrap().to_owned();
    let left: Vec<String> = fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("reader-"))
        .collect();
    assert_eq!(left, ["reader-0"]);
}

// A reader deletes the reader directories nobody holds, and leaves the held ones alone.
#[test]
fn a_reader_deletes_the_reader_directories_nobody_holds() {
    let dir = TempDir::new("gen-reader-clean");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let readers: Vec<Root> = (0..4)
        .map(|_| Root::reader(dir.path(), &p).unwrap())
        .collect();
    let root = readers[0].path().parent().unwrap().to_owned();
    for r in &readers[1..] {
        r.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    }
    let mut readers = readers.into_iter();
    let r0 = readers.next().unwrap();
    let (r1, r2, r3) = (
        readers.next().unwrap(),
        readers.next().unwrap(),
        readers.next().unwrap(),
    );
    drop((r0, r1, r3));
    // A cleanup a crash cut short left a renamed directory: the next one deletes it.
    fs::create_dir_all(root.join("reader-trash-0123").join("log")).unwrap();
    let again = Root::reader(dir.path(), &p).unwrap();
    assert_eq!(again.path(), root.join("reader-0"));
    let mut left: Vec<String> = fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("reader-"))
        .collect();
    left.sort();
    assert_eq!(left, ["reader-0", "reader-2"]);
    // The held one keeps working, its generation whole.
    r2.finish_leftovers().unwrap();
    assert!(r2.current().unwrap().is_some());
    // Once it is let go, the next reader deletes it too.
    drop(r2);
    drop(again);
    let next = Root::reader(dir.path(), &p).unwrap();
    assert_eq!(next.path(), root.join("reader-0"));
    assert!(!root.join("reader-2").exists());
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
    let g = building.publish(Fate::Trash).unwrap();
    assert_eq!(root.current().unwrap(), Some(g.clone()));
    assert_eq!(fs::read(g.db()).unwrap(), b"pages");
    assert!(g.log_dir().join("00000000000000000001.usl").exists());
    // The next one supersedes it.
    let g2 = root.build(header(2)).unwrap().publish(Fate::Trash).unwrap();
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
        let old = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
        let done = root
            .build(header(2))
            .unwrap()
            .crash_after(Fate::Trash, steps)
            .unwrap();
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

// A build whose log rolled and was trimmed before its publish (its first segment gone) still names
// what it supersedes in every segment left: a crash after any publish step converges to one
// generation.
#[test]
fn a_rolled_and_trimmed_build_converges_after_a_crash_at_each_publish_step() {
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let mut steps = 0;
    loop {
        let dir = TempDir::new("gen-crash-trimmed");
        let root = Root::lock(dir.path(), &p).unwrap();
        let old = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
        let mut building = root.build(header(2)).unwrap();
        building.log.set_roll_bytes(256);
        for n in 0..12u8 {
            building
                .log
                .append(&RecordBody::Commit {
                    frame: vec![n; 200],
                })
                .unwrap();
        }
        let last = Position(building.log.next_position().0 - 1);
        building
            .log
            .append(&RecordBody::Ack {
                acked: last,
                acked_offset: Offset::from_u64(1),
                log_bytes: 0,
            })
            .unwrap();
        building
            .log
            .append(&RecordBody::Base { position: last })
            .unwrap();
        building
            .log
            .append(&RecordBody::Commit {
                frame: vec![99; 200],
            })
            .unwrap();
        assert!(building.log.trim().unwrap() > 0);
        let done = building.crash_after(Fate::Trash, steps).unwrap();
        drop(root);
        let root = Root::lock(dir.path(), &p).unwrap();
        root.finish_leftovers().unwrap();
        let current = root.current().unwrap().unwrap();
        let entries: Vec<String> = fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n != "lock")
            .collect();
        assert_eq!(entries.len(), 1, "after {steps} steps: {entries:?}");
        let want = if steps >= 4 {
            Lineage([2; 16])
        } else {
            old.lineage
        };
        assert_eq!(current.lineage, want, "after {steps} steps");
        if steps >= 4 {
            assert_eq!(current.first_header().unwrap().supersedes, Some(old.id));
        }
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
    let old = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    // A crash right after the new generation's rename, then the old one lost its log.
    root.build(header(2))
        .unwrap()
        .crash_after(Fate::Trash, 5)
        .unwrap();
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
    let a = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    let side = dir.path().join("side");
    fs::rename(&a.dir, &side).unwrap();
    let b = root.build(header(2)).unwrap().publish(Fate::Trash).unwrap();
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
    let g = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
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
    let old = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    root.build(header(1))
        .unwrap()
        .crash_after(Fate::Trash, 5)
        .unwrap();
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
    let other = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    fs::rename(&side, &current.dir).unwrap();
    assert_eq!(other.first_header().unwrap().supersedes, None);
    root.finish_leftovers().unwrap();
    assert!(matches!(
        root.current(),
        Err(GenerationError::Ambiguous { ids, .. }) if ids.len() == 2
    ));
}

// A generation nothing was applied to is published with the page 1 of an empty database, marked
// (design: Layout): no published db file is empty or unmarked. One a commit or a snapshot wrote
// keeps its pages.
#[test]
fn a_generation_is_published_with_page_one_written() {
    let dir = TempDir::new("gen-empty");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let g = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    assert_eq!(
        fs::read(g.db()).unwrap(),
        empty_page_one(g.lineage).to_vec()
    );
    let building = root.build(header(2)).unwrap();
    building.db.write_all_at(&[7u8; 8192], 0).unwrap();
    let g2 = building.publish(Fate::Trash).unwrap();
    assert_eq!(fs::read(g2.db()).unwrap(), vec![7u8; 8192]);
}

/// The manifest of a salvage of the generation of `header(n)`.
fn manifest(n: u8) -> Manifest {
    Manifest {
        reason: Reason::HistoryChanged,
        state: State::Moving,
        time: "2026-10-16T12:00:00Z".to_owned(),
        path: "/v1/stream/db".to_owned(),
        cluster_id: Some("cluster".to_owned()),
        lineage: Some(Lineage([n; 16])),
        incarnation: None,
        stream_incarnation: None,
        acknowledged: None,
        plain: true,
    }
}

/// A crash after every step of a publish that salvages the generation it supersedes converges,
/// at the next open, to one current generation and at most one salvage, complete and plain: the
/// old generation stays current (its manifest deleted) until the rename, and is salvaged after.
#[test]
fn a_crash_after_each_salvaging_publish_step_converges() {
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let mut steps = 0;
    loop {
        let dir = TempDir::new("gen-salvage-crash");
        let root = Root::lock(dir.path(), &p).unwrap();
        let old = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
        let done = root
            .build(header(2))
            .unwrap()
            .crash_after(Fate::Salvage(manifest(1)), steps)
            .unwrap();
        drop(root);
        let root = Root::lock(dir.path(), &p).unwrap();
        root.finish_leftovers().unwrap();
        let current = root.current().unwrap().unwrap();
        let salvages = salvage::list(root.path()).unwrap();
        // Until the rename (step 5) the old generation stays, after it the new one.
        if steps >= 5 {
            assert_eq!(current.lineage, Lineage([2; 16]), "after {steps} steps");
            assert_eq!(salvages.len(), 1, "after {steps} steps");
            let s = &salvages[0];
            assert_eq!(s.manifest.state, State::Complete);
            assert_eq!(fs::read(s.dir.join("db")).unwrap()[..16], SQLITE_MAGIC);
            assert!(s.dir.join("log").exists());
        } else {
            assert_eq!(current.lineage, old.lineage, "after {steps} steps");
            assert!(salvages.is_empty(), "after {steps} steps");
            assert!(!current.dir.join(salvage::MANIFEST).exists());
        }
        let gens: Vec<String> = fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.starts_with("gen-"))
            .collect();
        assert_eq!(gens.len(), 1, "after {steps} steps: {gens:?}");
        if done < steps {
            break;
        }
        steps += 1;
    }
    assert_eq!(steps, 9);
}

/// A crash while a manifest was written leaves `manifest.json.tmp`, in a generation or in a
/// salvage: the next open deletes it.
#[test]
fn leftover_temporary_manifests_are_deleted() {
    let dir = TempDir::new("gen-manifest-tmp");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let g = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    let salvaged = root
        .path()
        .join(salvage::DIR)
        .join("20261016T120000Z-replaced");
    fs::create_dir_all(&salvaged).unwrap();
    salvage::write_manifest(&salvaged, &Manifest {
        reason: Reason::Replaced,
        state: State::Complete,
        ..manifest(1)
    })
    .unwrap();
    for at in [&g.dir, &salvaged] {
        fs::write(at.join("manifest.json.tmp"), b"{").unwrap();
    }
    root.finish_leftovers().unwrap();
    for at in [&g.dir, &salvaged] {
        assert!(!at.join("manifest.json.tmp").exists(), "{}", at.display());
    }
    assert_eq!(root.current().unwrap().unwrap().dir, g.dir);
    assert_eq!(salvage::list(root.path()).unwrap().len(), 1);
}

/// A published generation of lineage 1 whose log holds `commits` records over several segments,
/// a WAL and a wal-index beside its db.
fn with_records(root: &Root, commits: u8) -> super::Generation {
    let g = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    let (mut log, _) = Log::open(&g.log_dir()).unwrap();
    log.set_roll_bytes(256);
    for n in 1..=commits {
        log.append(&RecordBody::Commit {
            frame: vec![n; 100],
        })
        .unwrap();
    }
    log.append(&RecordBody::Base {
        position: Position(2),
    })
    .unwrap();
    log.sync().unwrap();
    fs::write(g.wal(), b"wal").unwrap();
    fs::write(g.shm(), b"shm").unwrap();
    g
}

/// The records of the log in `dir`.
fn records(dir: &std::path::Path) -> Vec<(Position, RecordBody)> {
    Log::open(dir)
        .unwrap()
        .1
        .records
        .into_iter()
        .map(|r| (r.position, r.body))
        .collect()
}

// A hold on a generation lasts until it drops, and holds that generation only.
#[test]
fn a_generation_is_held_while_a_hold_lives() {
    let dir = TempDir::new("gen-hold");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let g = root.build(header(1)).unwrap().publish(Fate::Trash).unwrap();
    assert!(!root.held(&g));
    let hold = root.hold(&g);
    let again = root.hold(&g);
    assert!(root.held(&g));
    let other = root.build(header(2)).unwrap().publish(Fate::Trash).unwrap();
    assert!(!root.held(&other));
    drop(hold);
    assert!(root.held(&g), "the second hold still holds it");
    drop(again);
    assert!(!root.held(&g));
}

// The copy of a generation keeps its lineage, its db (page-1 marker included) and every log record,
// over several segments. Its first segment names the original as the generation it supersedes, and
// the WAL and wal-index stay behind. Published, it replaces the original.
#[test]
fn a_copy_keeps_the_lineage_the_marker_and_the_log() {
    let dir = TempDir::new("gen-copy");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let old = with_records(&root, 6);
    let segments = fs::read_dir(old.log_dir()).unwrap().count();
    assert!(segments > 2, "{segments} segments");
    let want = records(&old.log_dir());
    let db = fs::read(old.db()).unwrap();
    assert!(is_marked(&db[..16], old.lineage));
    let _hold = root.hold(&old);
    let copy = root.copy(&old).unwrap().publish(Fate::Trash).unwrap();
    assert_ne!(copy.id, old.id);
    assert_eq!(copy.lineage, old.lineage);
    assert_eq!(fs::read(copy.db()).unwrap(), db);
    assert_eq!(fs::read_dir(copy.log_dir()).unwrap().count(), segments);
    assert_eq!(records(&copy.log_dir()), want);
    let first = copy.first_header().unwrap();
    assert_eq!(first.supersedes, Some(old.id));
    assert_eq!(first.lineage, old.lineage);
    assert!(!copy.wal().exists());
    assert!(!copy.shm().exists());
    assert_eq!(root.current().unwrap(), Some(copy.clone()));
    assert!(!old.dir.exists());
    assert!(!root.held(&copy));
}

// A crash at any step of a copy and its publish leaves either the original generation current or
// the copy, never both or neither: before the rename only a `.tmp` exists (after the db was copied,
// or the log too), which the next open deletes.
#[test]
fn a_crash_during_a_copy_leaves_one_generation_current() {
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let converge = |dir: &TempDir, old: &super::Generation, want: &[(Position, RecordBody)]| {
        let root = Root::lock(dir.path(), &p).unwrap();
        root.finish_leftovers().unwrap();
        let current = root.current().unwrap().unwrap();
        let entries: Vec<String> = fs::read_dir(root.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n != "lock")
            .collect();
        assert_eq!(entries, vec![format!("gen-{}", current.id.hex())]);
        assert_eq!(current.lineage, old.lineage);
        assert_eq!(records(&current.log_dir()), want);
        current
    };
    // The crash after the db was copied, and after the log was too (`copy_db`, `copy_log`).
    for log_copied in [false, true] {
        let dir = TempDir::new("gen-copy-crash");
        let root = Root::lock(dir.path(), &p).unwrap();
        let old = with_records(&root, 3);
        let want = records(&old.log_dir());
        let building = root.copy(&old).unwrap();
        let tmp = building.tmp.clone();
        drop(building);
        if !log_copied {
            fs::remove_dir_all(tmp.join("log")).unwrap();
        }
        drop(root);
        assert_eq!(converge(&dir, &old, &want).id, old.id);
    }
    // Each step of the publish: the original until the rename (step 4), the copy after it.
    let mut steps = 0;
    loop {
        let dir = TempDir::new("gen-copy-crash");
        let root = Root::lock(dir.path(), &p).unwrap();
        let old = with_records(&root, 3);
        let want = records(&old.log_dir());
        let building = root.copy(&old).unwrap();
        let id = building.id;
        let done = building.crash_after(Fate::Trash, steps).unwrap();
        drop(root);
        let current = converge(&dir, &old, &want);
        let want = if steps >= 4 { id } else { old.id };
        assert_eq!(current.id, want, "after {steps} steps");
        if done < steps {
            break;
        }
        steps += 1;
    }
}

// A copy that fails deletes its `.tmp`: here the original's log is gone.
#[test]
fn a_failed_copy_leaves_no_tmp() {
    let dir = TempDir::new("gen-copy-fail");
    let p = canonical_path("https://h/v1/stream/db").unwrap();
    let root = Root::lock(dir.path(), &p).unwrap();
    let old = with_records(&root, 2);
    fs::remove_dir_all(old.log_dir()).unwrap();
    assert!(matches!(
        root.copy(&old),
        Err(GenerationError::Log(_) | GenerationError::Io { .. })
    ));
    let entries: Vec<String> = fs::read_dir(root.path())
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n != "lock")
        .collect();
    assert_eq!(entries, vec![format!("gen-{}", old.id.hex())]);
}
