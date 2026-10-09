//! `ursula_status` and `ursula_stats` as JSON.

use crate::db::Mode;
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
    let null = || "null".to_owned();
    let fields = [
        ("offset", json_str(&db.offset)),
        ("epoch", db.epoch.to_string()),
        ("poisoned", db.poisoned.is_some().to_string()),
        ("fenced", db.fenced().to_string()),
        (
            "reason",
            db.poisoned
                .as_ref()
                .map_or_else(null, |p| json_str(&p.first.to_string())),
        ),
        ("read_only", (db.mode == Mode::ReadOnly).to_string()),
        (
            "payment_required",
            db.payment_required.is_some().to_string(),
        ),
        (
            "payment_reason",
            db.payment_required
                .as_deref()
                .filter(|reason| !reason.is_empty())
                .map_or_else(null, json_str),
        ),
        ("snapshot", json_str(&db.snapshot)),
        ("retained", json_str(&db.retained)),
        ("local", json_str(&db.attached_from)),
        ("installed", json_str(&db.installed)),
        ("attach_ms", db.attach_ms.to_string()),
        ("commits", db.acked.to_string()),
        ("append_retries", db.append_retries.to_string()),
        ("log_bytes", db.log.to_string()),
        ("snapshot_due_bytes", db.snapshot_due_bytes().to_string()),
        (
            "snapshot_age_ms",
            db.snapshot_published_at
                .map_or_else(null, |at| at.elapsed().as_millis().to_string()),
        ),
        ("snapshot_failures", db.snapshot_failures.to_string()),
        (
            "snapshot_error",
            db.snapshot_error.as_deref().map_or_else(null, json_str),
        ),
    ];
    let body: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("\"{key}\":{value}"))
        .collect();
    Ok(format!("{{{}}}", body.join(",")))
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
