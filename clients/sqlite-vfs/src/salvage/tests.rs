use ursula_headers::Incarnation;
use ursula_headers::Offset;

use super::Acknowledged;
use super::Manifest;
use super::ManifestError;
use super::Reason;
use super::State;
use super::complete;
use super::list;
use super::move_into;
use super::utc;
use super::write_manifest;
use crate::codec::marker;
use crate::codec::marker::SQLITE_MAGIC;
use crate::ids::Lineage;
use crate::ids::Position;
use crate::testdir::TempDir;

fn manifest(reason: Reason) -> Manifest {
    Manifest {
        reason,
        state: State::Moving,
        time: "2026-10-16T12:34:56Z".to_owned(),
        path: "/v1/stream/b/db".to_owned(),
        cluster_id: Some("cluster-a".to_owned()),
        lineage: Some(Lineage([0xab; 16])),
        incarnation: Some(Incarnation::try_from(4_503_599_627_370_497).unwrap()),
        stream_incarnation: Some(Incarnation::try_from(4_503_599_627_370_498).unwrap()),
        acknowledged: Some(Acknowledged {
            position: Position(7),
            offset: Offset::from_u64(1234),
            unacknowledged: 2,
        }),
        plain: true,
    }
}

#[test]
fn the_manifest_renders_as_its_golden_json() {
    assert_eq!(
        manifest(Reason::HistoryChanged).render(),
        concat!(
            r#"{"format":1,"state":"moving","reason":"history_changed","#,
            r#""time":"2026-10-16T12:34:56Z","path":"/v1/stream/b/db","#,
            r#""cluster_id":"cluster-a","lineage":"abababababababababababababababab","#,
            r#""incarnation":"4503599627370497","stream_incarnation":"4503599627370498","#,
            r#""acknowledged_position":7,"acknowledged_offset":1234,"unacknowledged":2,"#,
            r#""plain":true}"#
        )
    );
    let replaced = Manifest {
        cluster_id: None,
        lineage: None,
        incarnation: None,
        stream_incarnation: None,
        acknowledged: None,
        plain: false,
        ..manifest(Reason::Replaced)
    };
    assert_eq!(
        replaced.render(),
        concat!(
            r#"{"format":1,"state":"moving","reason":"replaced","#,
            r#""time":"2026-10-16T12:34:56Z","path":"/v1/stream/b/db","#,
            r#""cluster_id":null,"lineage":null,"incarnation":null,"stream_incarnation":null,"#,
            r#""acknowledged_position":null,"acknowledged_offset":null,"unacknowledged":null,"#,
            r#""plain":false}"#
        )
    );
}

#[test]
fn a_manifest_reads_back_what_it_renders() {
    for reason in [Reason::HistoryChanged, Reason::Replaced, Reason::Recreated] {
        let m = manifest(reason);
        assert_eq!(Manifest::parse(&m.render()).unwrap(), m);
    }
    let bare = Manifest {
        acknowledged: None,
        lineage: None,
        ..manifest(Reason::Replaced)
    };
    assert_eq!(Manifest::parse(&bare.render()).unwrap(), bare);
    // A later version may add members.
    let m = manifest(Reason::Recreated);
    let more = m.render().replacen('{', r#"{"later":1,"#, 1);
    assert_eq!(Manifest::parse(&more).unwrap(), m);
    let other = manifest(Reason::Recreated)
        .render()
        .replacen(r#""format":1"#, r#""format":2"#, 1);
    assert!(matches!(
        Manifest::parse(&other),
        Err(ManifestError::Format { found: 2 })
    ));
    let half = manifest(Reason::Recreated).render().replacen(
        r#""unacknowledged":2"#,
        r#""unacknowledged":null"#,
        1,
    );
    assert!(matches!(
        Manifest::parse(&half),
        Err(ManifestError::Field {
            field: "acknowledged_position"
        })
    ));
}

#[test]
fn utc_times_are_rfc_3339() {
    assert_eq!(utc(0), "1970-01-01T00:00:00Z");
    assert_eq!(utc(951_782_400), "2000-02-29T00:00:00Z");
    assert_eq!(utc(1_792_154_096), "2026-10-16T12:34:56Z");
}

/// A generation directory with a marked db, as an open materialized it.
fn generation(root: &std::path::Path, lineage: Lineage) -> std::path::PathBuf {
    let dir = root.join("gen-0123");
    std::fs::create_dir_all(dir.join("log")).unwrap();
    let mut page = marker::empty_page_one(lineage);
    page[100] = 13;
    std::fs::write(dir.join("db"), &page[..]).unwrap();
    std::fs::write(dir.join("log").join("00000000000000000001.usl"), b"log").unwrap();
    dir
}

#[test]
fn a_salvage_moves_the_whole_generation_and_restores_the_magic() {
    let tmp = TempDir::new("salvage-move");
    let m = manifest(Reason::HistoryChanged);
    let dir = generation(tmp.path(), m.lineage.unwrap());
    write_manifest(&dir, &m).unwrap();
    let moved = move_into(tmp.path(), &dir, &m).unwrap();
    assert_eq!(
        moved,
        tmp.path()
            .join("salvage")
            .join("20261016T123456Z-history_changed")
    );
    assert!(!dir.exists());
    assert!(moved.join("log").join("00000000000000000001.usl").exists());
    complete(&moved, m.clone()).unwrap();
    assert_eq!(std::fs::read(moved.join("db")).unwrap()[..16], SQLITE_MAGIC);
    let listed = list(tmp.path()).unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].manifest.state, State::Complete);
    // A second salvage of the same second gets a suffix.
    let again = generation(tmp.path(), m.lineage.unwrap());
    assert_eq!(
        move_into(tmp.path(), &again, &m).unwrap(),
        tmp.path()
            .join("salvage")
            .join("20261016T123456Z-history_changed-2")
    );
}

#[test]
fn a_replaced_db_is_left_as_it_was_found() {
    let tmp = TempDir::new("salvage-replaced");
    let m = manifest(Reason::Replaced);
    let dir = generation(tmp.path(), m.lineage.unwrap());
    let before = std::fs::read(dir.join("db")).unwrap();
    let moved = move_into(tmp.path(), &dir, &m).unwrap();
    complete(&moved, m).unwrap();
    assert_eq!(std::fs::read(moved.join("db")).unwrap(), before);
}
