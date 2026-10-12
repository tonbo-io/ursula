//! `ursula_status` and `ursula_stats` as JSON.

use crate::db::Db;
use crate::db::Mode;
use crate::db::attached;
use crate::db::lock;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::error::WriteRefusal;
use crate::host::full_pathname;
use crate::stats::Drained;
use crate::stats::Dropped;

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
    Ok(status_json(&db))
}

/// `ursula_status` of an attached database.
fn status_json(db: &Db) -> String {
    let null = || "null".to_owned();
    // A 402 shows in the fields 0.7.1 introduced for it too.
    let payment = match &db.layer_refused {
        Some(WriteRefusal::PaymentRequired { body }) => Some(body.as_str()),
        _ => None,
    };
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
        ("payment_required", payment.is_some().to_string()),
        (
            "payment_reason",
            payment
                .filter(|reason| !reason.is_empty())
                .map_or_else(null, json_str),
        ),
        ("writes_refused", db.refused.is_some().to_string()),
        (
            "refused_kind",
            db.refused
                .as_ref()
                .map_or_else(null, |refusal| json_str(refusal.kind())),
        ),
        (
            "refused_reason",
            db.refused
                .as_ref()
                .and_then(WriteRefusal::reason)
                .map_or_else(null, |reason| json_str(&reason)),
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
        (
            "snapshot_failures",
            db.snapshot_failing
                .as_ref()
                .map_or(0, |failing| failing.count)
                .to_string(),
        ),
        (
            "snapshot_error",
            db.snapshot_failing
                .as_ref()
                .map_or_else(null, |failing| json_str(&failing.error)),
        ),
        (
            "snapshot_bytes",
            db.snapshot_bytes
                .map_or_else(null, |bytes| bytes.to_string()),
        ),
        ("error", error_json(db).unwrap_or_else(null)),
    ];
    let body: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("\"{key}\":{value}"))
        .collect();
    format!("{{{}}}", body.join(","))
}

/// Why writes to the database fail now, as `ursula_status` reports it (`error`):
/// `{"kind","action","message"}`, `None` while they do not. A poison's kind is `fenced` once a
/// fence was seen (`Poisoned::fenced`), whatever the first reason was, and otherwise the first
/// reason's (`Error::poison_kind`), whose text is the message (the `reason` field's). A write
/// refused before it reached the stream is `unreachable` when no attempt of it went out, and
/// `refused` otherwise (`refused_kind` says by what).
fn error_json(db: &Db) -> Option<String> {
    let (kind, message) = match (&db.poisoned, &db.refused) {
        (Some(p), _) if p.fenced => (ErrorKind::Fenced, p.first.to_string()),
        (Some(p), _) => (p.first.poison_kind(), p.first.to_string()),
        (None, Some(refusal @ WriteRefusal::Unreachable { .. })) => {
            (ErrorKind::Unreachable, refusal.to_string())
        }
        (None, Some(refusal)) => (ErrorKind::Refused, refusal.to_string()),
        (None, None) => return None,
    };
    Some(format!(
        "{{\"kind\":{},\"action\":{},\"message\":{}}}",
        json_str(kind.as_str()),
        json_str(kind.action().as_str()),
        json_str(&message)
    ))
}

/// `ursula_stats`: drains the attached database's measurements (see [`Stats`]).
///
/// [`Stats`]: crate::stats::Stats
pub(crate) fn stats(path: &str) -> Result<String, Error> {
    let path = full_pathname(path)?;
    let db = attached(&path)?;
    // Rendered once the mutex is released: a commit waits for it.
    let drained = lock(&db).stats.drain();
    Ok(stats_json(&drained))
}

/// `ursula_stats` of what a drain handed over.
fn stats_json(drained: &Drained) -> String {
    let commits: Vec<String> = drained
        .commits
        .iter()
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
    let checkpoints: Vec<String> = drained
        .checkpoints
        .iter()
        .map(|d| d.as_micros().to_string())
        .collect();
    let snapshots: Vec<String> = drained
        .snapshots
        .iter()
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
    let Dropped {
        commits: dropped_commits,
        commit_bytes,
        append_retries,
        checkpoints: dropped_checkpoints,
        snapshots: dropped_snapshots,
    } = drained.dropped;
    let dropped = format!(
        "{{\"commits\":{dropped_commits},\"commit_bytes\":{commit_bytes},\"append_retries\":{append_retries},\"checkpoints\":{dropped_checkpoints},\"snapshots\":{dropped_snapshots}}}"
    );
    let (commits, checkpoints, snapshots) = (
        commits.join(","),
        checkpoints.join(","),
        snapshots.join(","),
    );
    format!(
        "{{\"commits\":[{commits}],\"checkpoints_us\":[{checkpoints}],\"snapshots\":[{snapshots}],\"dropped\":{dropped}}}"
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::stats_json;
    use super::status_json;
    use crate::db::Db;
    use crate::error::Attempt;
    use crate::error::Error;
    use crate::error::Fence;
    use crate::error::WriteRefusal;
    use crate::stats::CommitStat;
    use crate::stats::KEPT;
    use crate::stats::Stats;

    // `ursula_stats` reports what was dropped under the names the TypeScript package reads
    // (`VfsDropped`): the commits, their frame bytes and append retries, checkpoints and snapshots.
    #[test]
    fn stats_report_what_was_dropped() {
        let mut s = Stats::default();
        for i in 0..KEPT + 2 {
            s.commit(CommitStat {
                bytes: 100 + i,
                raw: 0,
                pages: 1,
                attempts: 2,
                append: Duration::ZERO,
                vfs: Duration::ZERO,
            });
        }
        let json = stats_json(&s.drain());
        assert!(
            json.ends_with(
                r#""dropped":{"commits":2,"commit_bytes":201,"append_retries":2,"checkpoints":0,"snapshots":0}}"#
            ),
            "{}",
            json.get(json.len().saturating_sub(120)..).unwrap_or(&json)
        );
    }

    // Why writes fail now, as one object with a stable kind and what to do about it. The three
    // outcomes of a failed commit stay apart: refused before it reached the stream (retry on the
    // same handle), unknown after the retry budget (poisoned, attach again) and fenced by a newer
    // owner (stop). A fence seen after another poison makes the kind `fenced`.
    #[test]
    fn the_error_names_its_kind_and_what_to_do() {
        let error = |d: &Db| {
            let status = status_json(d);
            let (_, error) = status.split_once(r#","error":"#).unwrap();
            error.strip_suffix('}').unwrap().to_owned()
        };
        let mut d = Db::for_tests();
        assert_eq!(error(&d), "null");
        d.refuse(WriteRefusal::RateLimited {
            body: "slow down".into(),
        });
        assert_eq!(
            error(&d),
            r#"{"kind":"refused","action":"retry","message":"429 slow down: rate limited until the retry budget ran out"}"#
        );
        d.refuse(WriteRefusal::Unreachable {
            reason: "timeout: resolve".into(),
        });
        assert!(error(&d).starts_with(r#"{"kind":"unreachable","action":"retry","#));

        let mut d = Db::for_tests();
        d.poison(Error::AppendUnknown {
            attempts: 35,
            last: Attempt::Status {
                status: 503,
                body: String::new(),
            },
        });
        assert_eq!(
            error(&d),
            r#"{"kind":"outcome_unknown","action":"reattach","message":"append: 503 (outcome unknown after 35 attempts)"}"#
        );
        // An append's 403 that is neither the fence nor vouched may stand in for the stream's
        // answer: its poison is an unknown outcome too, never something to retry.
        let mut d = Db::for_tests();
        d.poison(Error::Unauthorized {
            op: "append",
            url: "http://h/b/s".into(),
            status: 403,
            body: String::new(),
        });
        assert!(error(&d).starts_with(r#"{"kind":"outcome_unknown","action":"reattach","#));
        d.poison(Error::Fenced(Fence::Superseded {
            epoch: 2,
            current: Some(3),
        }));
        assert!(error(&d).starts_with(r#"{"kind":"fenced","action":"stop","message":"append "#));

        let mut d = Db::for_tests();
        d.poison(Error::ForeignWrite { seen: 4, found: 6 });
        assert!(error(&d).starts_with(r#"{"kind":"foreign_write","action":"reattach","#));
    }

    // A write refused before it reached the stream shows in `writes_refused`, with its kind and
    // reason, until a commit is acknowledged; a 402 also shows in the fields 0.7.1 introduced.
    #[test]
    fn refused_writes_show_their_kind_and_reason() {
        let shows = |d: &Db, fields: &str| {
            let status = status_json(d);
            assert!(status.contains(fields), "{status}");
        };
        let mut d = Db::for_tests();
        shows(
            &d,
            r#""payment_required":false,"payment_reason":null,"writes_refused":false,"refused_kind":null,"refused_reason":null,"#,
        );
        d.refuse(WriteRefusal::NotApplied {
            status: 503,
            reason: "RaftLogPressure".into(),
        });
        shows(
            &d,
            r#""payment_required":false,"payment_reason":null,"writes_refused":true,"refused_kind":"not_applied","refused_reason":"RaftLogPressure","#,
        );
        d.refuse(WriteRefusal::Unauthorized {
            body: String::new(),
        });
        shows(
            &d,
            r#""writes_refused":true,"refused_kind":"unauthorized","refused_reason":null,"#,
        );
        d.refuse(WriteRefusal::PaymentRequired {
            body: r#"{"detail":"quota"}"#.into(),
        });
        shows(
            &d,
            r#""payment_required":true,"payment_reason":"{\"detail\":\"quota\"}","writes_refused":true,"refused_kind":"payment_required","refused_reason":"{\"detail\":\"quota\"}","#,
        );
        // A transaction the extension refuses itself is the latest refusal, while the 402 still
        // stands for the layer.
        d.refuse_unsent(WriteRefusal::frame_over_limit(40 << 20, 32 << 20));
        shows(
            &d,
            r#""payment_required":true,"payment_reason":"{\"detail\":\"quota\"}","writes_refused":true,"refused_kind":"too_large","refused_reason":"41943040-byte frame over the 33554432-byte frame limit","#,
        );
        d.writes_accepted();
        shows(
            &d,
            r#""payment_required":false,"payment_reason":null,"writes_refused":false,"refused_kind":null,"refused_reason":null,"#,
        );
    }
}
