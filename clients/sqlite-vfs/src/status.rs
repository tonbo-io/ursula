//! `ursula_status` and `ursula_stats` as JSON.

use crate::db::attached;
use crate::db::lock;
use crate::error::Error;
use crate::host::full_pathname;

fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(crate) fn status(path: &str) -> Result<String, Error> {
    let path = full_pathname(path)?;
    let db = attached(&path)?;
    let db = lock(&db);
    Ok(format!(
        "{{\"offset\":{},\"epoch\":{},\"poisoned\":{},\"fenced\":{},\"reason\":{},\"snapshot\":{},\"retained\":{},\"local\":{},\"installed\":{}}}",
        json_str(&db.offset),
        db.epoch,
        db.poisoned.is_some(),
        db.fenced(),
        db.poisoned
            .as_ref()
            .map_or("null".to_owned(), |e| json_str(&e.to_string())),
        json_str(&db.snapshot),
        json_str(&db.retained),
        json_str(&db.attached_from),
        json_str(&db.installed)
    ))
}

pub(crate) fn stats(path: &str) -> Result<String, Error> {
    let path = full_pathname(path)?;
    let db = attached(&path)?;
    let mut db = lock(&db);
    let commits: Vec<String> = db
        .stats
        .drain(..)
        .map(|s| {
            format!(
                "{{\"bytes\":{},\"raw\":{},\"pages\":{},\"attempts\":{},\"append_us\":{},\"vfs_us\":{}}}",
                s.bytes,
                s.raw,
                s.pages,
                s.attempts,
                s.append.as_micros(),
                s.vfs.as_micros()
            )
        })
        .collect();
    let checkpoints: Vec<String> = db
        .checkpoints
        .drain(..)
        .map(|d| d.as_micros().to_string())
        .collect();
    let snapshots: Vec<String> = db
        .snapshot_stats
        .drain(..)
        .map(|s| {
            format!(
                "{{\"offset\":{},\"bytes\":{},\"raw\":{},\"copy_us\":{},\"total_us\":{}}}",
                json_str(&s.offset),
                s.bytes,
                s.raw,
                s.copy.as_micros(),
                s.total.as_micros()
            )
        })
        .collect();
    Ok(format!(
        "{{\"commits\":[{}],\"checkpoints_us\":[{}],\"snapshots\":[{}]}}",
        commits.join(","),
        checkpoints.join(","),
        snapshots.join(",")
    ))
}
