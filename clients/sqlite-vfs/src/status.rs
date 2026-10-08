//! `ursula_status` and `ursula_stats` as JSON.

use std::fmt::Write as _;

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
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(crate) unsafe fn status(path: &str) -> Result<String, Error> {
    let path = unsafe { full_pathname(path)? };
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

pub(crate) unsafe fn stats(path: &str) -> Result<String, Error> {
    let path = unsafe { full_pathname(path)? };
    let db = attached(&path)?;
    let mut db = lock(&db);
    let mut out = String::from("{\"commits\":[");
    for (i, s) in db.stats.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"bytes\":{},\"raw\":{},\"pages\":{},\"attempts\":{},\"append_us\":{},\"vfs_us\":{}}}",
            s.bytes,
            s.raw,
            s.pages,
            s.attempts,
            s.append.as_micros(),
            s.vfs.as_micros()
        );
    }
    out.push_str("],\"checkpoints_us\":[");
    for (i, d) in db.checkpoints.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{}", d.as_micros());
    }
    out.push_str("],\"snapshots\":[");
    for (i, s) in db.snapshot_stats.drain(..).enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(
            out,
            "{{\"offset\":{},\"bytes\":{},\"raw\":{},\"copy_us\":{},\"total_us\":{}}}",
            json_str(&s.offset),
            s.bytes,
            s.raw,
            s.copy.as_micros(),
            s.total.as_micros()
        );
    }
    out.push_str("]}");
    Ok(out)
}
