//! The JSON the SQL functions return: `ursula_status` (the subset of design §9 that M1 has),
//! `ursula_stats`, `ursula_events` and the results of `ursula_open` and `ursula_synced`.

use std::fmt::Write as _;

use crate::db::Binding;
use crate::db::Opened;
use crate::db::by_id;
use crate::driver::DriverEvent;
use crate::driver::Life;
use crate::driver::SyncError;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::error::reset_action;
use crate::ids::Position;
use crate::open::OpenError;
use crate::replicator::BlockedReason;
use crate::replicator::Condition;
use crate::stats::Drained;
use crate::stats::Dropped;

pub(crate) fn json_str(s: &str) -> String {
    let mut out = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if u32::from(c) < 0x20 => {
                let _infallible = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A JSON object of `fields`, each value already JSON.
fn object(fields: &[(&str, String)]) -> String {
    let body: Vec<String> = fields
        .iter()
        .map(|(key, value)| format!("{}:{value}", json_str(key)))
        .collect();
    format!("{{{}}}", body.join(","))
}

fn null() -> String {
    "null".to_owned()
}

/// The token of a replication condition (design §4.7, §9).
fn condition(c: &Condition) -> &'static str {
    match c {
        Condition::Retrying => "retrying",
        Condition::Blocked(reason) => match reason {
            BlockedReason::RequestTooLarge => "request_too_large",
            BlockedReason::Unauthorized => "unauthorized",
            BlockedReason::PaymentRequired => "payment_required",
            BlockedReason::Forbidden => "forbidden",
            BlockedReason::Redirected { .. } => "redirected",
            BlockedReason::WrongCluster => "wrong_cluster",
            BlockedReason::Rejected { .. } => "rejected",
        },
    }
}

/// `ursula_status` of a bound database.
pub(crate) fn status_json(b: &Binding) -> String {
    let h = &b.handle;
    let replication = h.replication();
    let reset = h.reset();
    let logged = h.last_commit();
    let (confirmed, refused) = {
        let c = b.lock();
        (c.confirmed, c.refused.clone())
    };
    let commits = object(&[
        ("logged", logged.0.to_string()),
        ("confirmed", confirmed.0.to_string()),
        ("acknowledged", h.acked().0.to_string()),
        ("pending", replication.pending.to_string()),
        ("pending_bytes", h.pending_bytes().to_string()),
    ]);
    let repl = object(&[
        ("state", json_str(replication.state)),
        (
            "condition",
            replication
                .condition
                .as_ref()
                .map_or_else(null, |c| json_str(condition(c))),
        ),
    ]);
    let refused = refused.map_or_else(null, |r| {
        object(&[
            ("kind", json_str(r.kind().as_str())),
            ("action", json_str(r.kind().action().as_str())),
            ("message", json_str(&r.to_string())),
        ])
    });
    let reset_json = reset.map_or_else(null, |kind| {
        object(&[
            ("kind", json_str(kind.kind())),
            ("action", json_str(reset_action(kind).as_str())),
        ])
    });
    let lineage: String = h
        .generation
        .lineage
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    object(&[
        ("name", json_str(h.name.as_str())),
        ("path", json_str(&b.path)),
        ("role", json_str("owner")),
        (
            "state",
            json_str(match (reset, h.life()) {
                (Some(_), _) => "reset",
                (None, Life::Live) => "live",
                (None, Life::Retiring) => "retiring",
                (None, Life::Retired) => "retired",
            }),
        ),
        (
            "epoch",
            replication.epoch.map_or_else(null, |e| e.to_string()),
        ),
        ("cluster_id", json_str(&h.cluster_id)),
        ("incarnation", json_str(&h.incarnation)),
        ("lineage", json_str(&lineage)),
        ("commits", commits),
        ("replication", repl),
        ("refused", refused),
        ("reset", reset_json),
    ])
}

/// `ursula_stats`: what the binding measured since the last drain.
pub(crate) fn stats_json(drained: &Drained) -> String {
    let commits: Vec<String> = drained
        .commits
        .iter()
        .map(|s| {
            object(&[
                ("bytes", s.bytes.to_string()),
                ("raw", s.raw.to_string()),
                ("pages", s.pages.to_string()),
                ("log_us", s.log.as_micros().to_string()),
                ("vfs_us", s.vfs.as_micros().to_string()),
            ])
        })
        .collect();
    let checkpoints: Vec<String> = drained
        .checkpoints
        .iter()
        .map(|d| d.as_micros().to_string())
        .collect();
    let Dropped {
        commits: dropped_commits,
        commit_bytes,
        checkpoints: dropped_checkpoints,
    } = drained.dropped;
    object(&[
        ("commits", format!("[{}]", commits.join(","))),
        ("checkpoints_us", format!("[{}]", checkpoints.join(","))),
        (
            "dropped",
            object(&[
                ("commits", dropped_commits.to_string()),
                ("commit_bytes", commit_bytes.to_string()),
                ("checkpoints", dropped_checkpoints.to_string()),
            ]),
        ),
    ])
}

/// `ursula_open`'s result: `{done: true, path, name, state}`, or `{done: false, request}`.
pub(crate) fn opened_json(opened: &Opened) -> String {
    match opened {
        Opened::Bound(b) => object(&[
            ("done", "true".into()),
            ("path", json_str(&b.path)),
            ("name", json_str(b.handle.name.as_str())),
            ("state", json_str("live")),
        ]),
        Opened::Pending(open) => {
            object(&[("done", "false".into()), ("request", open.0.to_string())])
        }
    }
}

/// `ursula_synced`'s result.
pub(crate) fn synced_json(
    synced: bool,
    ticket: u64,
    target: Option<Position>,
    acknowledged: Position,
) -> String {
    object(&[
        ("synced", synced.to_string()),
        ("ticket", ticket.to_string()),
        ("target", target.map_or_else(null, |t| t.0.to_string())),
        ("acknowledged", acknowledged.0.to_string()),
    ])
}

/// An open's failure: `{kind, action, message}`.
pub(crate) fn error_json(e: &OpenError) -> String {
    let kind = e.error_kind();
    object(&[
        ("kind", json_str(kind.as_str())),
        ("action", json_str(kind.action().as_str())),
        ("message", json_str(&e.to_string())),
    ])
}

/// `ursula_events`'s result: the cursor after them and the events, each with the path of its
/// binding when the process still has it.
pub(crate) fn events_json(events: &[DriverEvent], cursor: u64) -> String {
    let path = |id| by_id(id).map_or_else(null, |b| json_str(&b.path));
    let rendered: Vec<String> = events
        .iter()
        .map(|event| match event {
            DriverEvent::Opened { open, result } => object(&[
                ("event", json_str("opened")),
                ("request", open.0.to_string()),
                ("ok", result.is_ok().to_string()),
                (
                    "error",
                    result.as_ref().err().map_or_else(null, |e| error_json(e)),
                ),
            ]),
            DriverEvent::Acked { binding, acked } => object(&[
                ("event", json_str("acked")),
                ("path", path(*binding)),
                ("acknowledged", acked.0.to_string()),
            ]),
            DriverEvent::Condition {
                binding,
                condition: c,
            } => object(&[
                ("event", json_str("condition")),
                ("path", path(*binding)),
                (
                    "condition",
                    c.as_ref().map_or_else(null, |c| json_str(condition(c))),
                ),
            ]),
            DriverEvent::Reset { binding, cause } => object(&[
                ("event", json_str("reset")),
                ("path", path(*binding)),
                ("kind", json_str(cause.kind())),
                ("action", json_str(reset_action(*cause).as_str())),
            ]),
            DriverEvent::Synced { binding, through } => object(&[
                ("event", json_str("synced")),
                ("path", path(*binding)),
                ("through", through.0.to_string()),
            ]),
            DriverEvent::SyncFailed { ticket, error } => {
                // A binding that is not bound (retired) is `not_bound`, not a reset.
                let kind = match error {
                    SyncError::NotBound => ErrorKind::NotBound,
                    error => Error::from(*error).kind(),
                };
                object(&[
                    ("event", json_str("sync_failed")),
                    ("ticket", ticket.0.to_string()),
                    ("kind", json_str(kind.as_str())),
                ])
            }
            DriverEvent::Retired { binding } => {
                object(&[("event", json_str("retired")), ("path", path(*binding))])
            }
            DriverEvent::Gap { dropped } => {
                object(&[("event", json_str("gap")), ("dropped", dropped.to_string())])
            }
        })
        .collect();
    object(&[
        ("cursor", cursor.to_string()),
        ("events", format!("[{}]", rendered.join(","))),
    ])
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::events_json;
    use super::json_str;
    use super::stats_json;
    use super::synced_json;
    use crate::driver::BindingId;
    use crate::driver::DriverEvent;
    use crate::driver::SyncError;
    use crate::driver::Ticket;
    use crate::ids::Position;
    use crate::replicator::Condition;
    use crate::replicator::ResetKind;
    use crate::stats::CommitStat;
    use crate::stats::KEPT;
    use crate::stats::Stats;

    #[test]
    fn strings_are_escaped() {
        assert_eq!(json_str("a\"b\\c\n"), r#""a\"b\\c\u000a""#);
    }

    #[test]
    fn stats_report_what_was_dropped() {
        let mut s = Stats::default();
        for i in 0..KEPT + 2 {
            s.commit(CommitStat {
                bytes: 100 + i,
                raw: 0,
                pages: 1,
                log: Duration::ZERO,
                vfs: Duration::ZERO,
            });
        }
        let json = stats_json(&s.drain());
        assert!(
            json.ends_with(r#""dropped":{"commits":2,"commit_bytes":201,"checkpoints":0}}"#),
            "{}",
            json.get(json.len().saturating_sub(120)..).unwrap_or(&json)
        );
    }

    #[test]
    fn synced_names_its_target() {
        assert_eq!(
            synced_json(false, 7, Some(Position(3)), Position(2)),
            r#"{"synced":false,"ticket":7,"target":3,"acknowledged":2}"#
        );
        assert_eq!(
            synced_json(true, 8, None, Position(0)),
            r#"{"synced":true,"ticket":8,"target":null,"acknowledged":0}"#
        );
    }

    // Events of a binding the process no longer has carry a null path; resets name their kind and
    // action.
    #[test]
    fn events_render_as_one_object() {
        let id = BindingId(u64::MAX);
        let json = events_json(
            &[
                DriverEvent::Reset {
                    binding: id,
                    cause: ResetKind::Fenced,
                },
                DriverEvent::Condition {
                    binding: id,
                    condition: Some(Condition::Retrying),
                },
                DriverEvent::SyncFailed {
                    ticket: Ticket(4),
                    error: SyncError::Reset(ResetKind::Fenced),
                },
                DriverEvent::Gap { dropped: 2 },
            ],
            9,
        );
        assert_eq!(
            json,
            concat!(
                r#"{"cursor":9,"events":["#,
                r#"{"event":"reset","path":null,"kind":"fenced","action":"stop"},"#,
                r#"{"event":"condition","path":null,"condition":"retrying"},"#,
                r#"{"event":"sync_failed","ticket":4,"kind":"fenced"},"#,
                r#"{"event":"gap","dropped":2}]}"#
            )
        );
    }
}
