//! The JSON the SQL functions return: `ursula_status` (design: Status), `ursula_stats`,
//! `ursula_events` and the results of `ursula_open` and `ursula_synced`.

use crate::db::Binding;
use crate::db::Opened;
use crate::db::by_id;
use crate::driver::Attach;
use crate::driver::DriverEvent;
use crate::driver::Life;
use crate::driver::SyncError;
use crate::error::Classified;
use crate::error::Error;
use crate::error::ErrorKind;
use crate::error::Surface;
use crate::error::reset_message;
use crate::error::sync_failed_message;
use crate::ids::Position;
use crate::json;
use crate::json::null;
use crate::json::object;
use crate::json::str as json_str;
use crate::open::OpenError;
use crate::replicator::BlockedReason;
use crate::replicator::Condition;
use crate::salvage::Salvaged;
use crate::stats::Drained;
use crate::stats::Dropped;

/// An object of `before`, a failure's kind with what to do about it and the error a host throws
/// for it as it surfaced at `surface` (design: Errors), then `after`.
fn with_kind(
    before: &[(&str, String)],
    kind: ErrorKind,
    surface: Surface,
    after: &[(&str, String)],
) -> String {
    let named = [
        ("kind", json_str(kind.as_str())),
        ("action", json_str(kind.action().as_str())),
        ("class", json_str(kind.class(surface).as_str())),
    ];
    let fields: Vec<(&str, String)> = before
        .iter()
        .cloned()
        .chain(named)
        .chain(after.iter().cloned())
        .collect();
    object(&fields)
}

/// The token of a replication condition (design: Answers, Status).
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
            BlockedReason::NoServerIdentity => "no_server_identity",
            BlockedReason::ProtocolMismatch { .. } => "protocol_mismatch",
            BlockedReason::Rejected { .. } => "rejected",
        },
    }
}

/// The `state` status reports (design: Status): a reset, else the binding's life, a live one
/// `provisional` until a claim of it applied (design: Open).
fn state(reset: bool, life: Life, provisional: bool) -> &'static str {
    match (reset, life) {
        (true, _) => "reset",
        (false, Life::HandedOut { .. } | Life::Live) if provisional => "provisional",
        (false, Life::HandedOut { .. } | Life::Live) => "live",
        (false, Life::Retiring | Life::GivenUp) => "retiring",
        (false, Life::Retired) => "retired",
    }
}

/// `ursula_status` of a bound database. Fails only when the root's `salvage/` cannot be listed.
pub(crate) fn status_json(b: &Binding) -> Result<String, Error> {
    let h = &b.handle;
    let replication = h.replication();
    let reset = h.reset();
    let logged = h.last_commit();
    let (confirmed, refused) = {
        let c = b.lock();
        (c.confirmed, c.standing_refusal().cloned())
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
    let snapshots = h.snapshots.status();
    let snapshot = object(&[
        (
            "offset",
            snapshots
                .published
                .map_or_else(null, |(offset, _)| offset.get().to_string()),
        ),
        (
            "age_ms",
            snapshots
                .published
                .map_or_else(null, |(_, at)| at.elapsed().as_millis().to_string()),
        ),
        (
            "failures",
            snapshots
                .failing
                .as_ref()
                .map_or(0, |f| f.count)
                .to_string(),
        ),
        ("error", snapshots.error().map_or_else(null, json_str)),
        ("log_bytes", replication.log_bytes.to_string()),
        ("due_bytes", h.snapshots.due_bytes().to_string()),
    ]);
    let attach = match h.attach {
        Attach::Resumed => object(&[("local", json_str("resumed")), ("snapshot_offset", null())]),
        Attach::ReKeyed => object(&[("local", json_str("rekeyed")), ("snapshot_offset", null())]),
        Attach::CaughtUp => object(&[
            ("local", json_str("caught_up")),
            ("snapshot_offset", null()),
        ]),
        Attach::Built { snapshot, salvaged } => object(&[
            (
                "local",
                json_str(if salvaged.is_some() {
                    "salvaged"
                } else {
                    "built"
                }),
            ),
            (
                "snapshot_offset",
                snapshot.map_or_else(null, |offset| offset.get().to_string()),
            ),
        ]),
    };
    let refused = refused.map_or_else(null, |r| {
        with_kind(&[], r.kind(), Surface::Bound, &[(
            "message",
            json_str(&r.to_string()),
        )])
    });
    let reset_json = reset.map_or_else(null, |kind| {
        with_kind(&[], ErrorKind::Reset(kind), Surface::Bound, &[(
            "message",
            json_str(&reset_message(kind)),
        )])
    });
    let salvage: Vec<String> = h.salvage()?.iter().map(salvaged_json).collect();
    let lineage: String = h
        .generation
        .lineage
        .0
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(object(&[
        ("name", json_str(h.name.as_str())),
        ("path", json_str(&b.path)),
        ("role", json_str(h.role.as_str())),
        ("closed", h.closed.to_string()),
        (
            "state",
            json_str(state(reset.is_some(), h.life(), h.provisional())),
        ),
        (
            "epoch",
            replication.epoch.map_or_else(null, |e| e.to_string()),
        ),
        ("cluster_id", json_str(&h.cluster_id)),
        ("incarnation", json_str(&h.incarnation().to_string())),
        ("lineage", json_str(&lineage)),
        ("commits", commits),
        ("replication", repl),
        ("attach", attach),
        ("snapshot", snapshot),
        ("refused", refused),
        ("salvage", json::array(&salvage)),
        ("reset", reset_json),
    ]))
}

/// A salvage in `status().salvage`: its directory and its manifest's fields.
fn salvaged_json(s: &Salvaged) -> String {
    let m = &s.manifest;
    let acked = m.acknowledged;
    object(&[
        ("dir", json_str(&s.dir.to_string_lossy())),
        ("reason", json_str(m.reason.as_str())),
        ("time", json_str(&m.time)),
        (
            "incarnation",
            m.incarnation
                .map_or_else(null, |t| json_str(&t.to_string())),
        ),
        (
            "stream_incarnation",
            m.stream_incarnation
                .map_or_else(null, |t| json_str(&t.to_string())),
        ),
        (
            "acknowledged_position",
            acked.map_or_else(null, |a| a.position.0.to_string()),
        ),
        (
            "unacknowledged",
            acked.map_or_else(null, |a| a.unacknowledged.to_string()),
        ),
        ("plain", m.plain.to_string()),
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
            (
                "state",
                json_str(state(
                    b.handle.reset().is_some(),
                    b.handle.life(),
                    b.handle.provisional(),
                )),
            ),
        ]),
        Opened::Pending(open) => {
            object(&[("done", "false".into()), ("request", open.0.to_string())])
        }
    }
}

/// `ursula_import`'s result.
pub(crate) fn imported_json(imported: &crate::import::Imported) -> String {
    object(&[
        ("done", "true".into()),
        ("offset", imported.offset.get().to_string()),
        ("digest", json_str(&imported.digest.to_string())),
        ("pages", imported.pages.to_string()),
        ("bytes", imported.bytes.to_string()),
    ])
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

/// An open's failure: `{kind, action, class, message}`.
pub(crate) fn error_json(e: &OpenError) -> String {
    with_kind(&[], e.error_kind(), Surface::Open, &[(
        "message",
        json_str(&e.to_string()),
    )])
}

/// `ursula_classify`'s result. `fix` and `request_id` are `null`: no failure of the extension
/// carries them.
pub(crate) fn classified_json(c: &Classified) -> String {
    object(&[
        ("kind", json_str(c.kind.as_str())),
        ("action", json_str(c.action.as_str())),
        ("class", json_str(c.class.as_str())),
        ("message", json_str(&c.message)),
        ("fix", null()),
        ("request_id", null()),
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
            DriverEvent::Reset { binding, cause } => with_kind(
                &[("event", json_str("reset")), ("path", path(*binding))],
                ErrorKind::Reset(*cause),
                Surface::Bound,
                &[("message", json_str(&reset_message(*cause)))],
            ),
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
                with_kind(
                    &[
                        ("event", json_str("sync_failed")),
                        ("ticket", ticket.0.to_string()),
                    ],
                    kind,
                    Surface::Bound,
                    &[("message", json_str(&sync_failed_message(kind)))],
                )
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
    use std::sync::Arc;
    use std::time::Duration;

    use super::events_json;
    use super::state;
    use super::stats_json;
    use super::synced_json;
    use crate::driver::BindingId;
    use crate::driver::DriverEvent;
    use crate::driver::Life;
    use crate::driver::OpenId;
    use crate::driver::SyncError;
    use crate::driver::Ticket;
    use crate::ids::Position;
    use crate::open::OpenError;
    use crate::open::StreamState;
    use crate::replicator::Condition;
    use crate::replicator::ResetKind;
    use crate::stats::CommitStat;
    use crate::stats::KEPT;
    use crate::stats::Stats;

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

    // Events of a binding the process no longer has carry a null path. Resets, failed tickets and
    // failed opens name their kind, action and class, a `stream_state` an open's setup error, and
    // the message a host shows for them.
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
                DriverEvent::SyncFailed {
                    ticket: Ticket(5),
                    error: SyncError::NotBound,
                },
                DriverEvent::Opened {
                    open: OpenId(6),
                    result: Err(Arc::new(OpenError::StreamState(StreamState::NotSqlite))),
                },
                DriverEvent::Gap { dropped: 2 },
            ],
            9,
        );
        assert_eq!(
            json,
            concat!(
                r#"{"cursor":9,"events":["#,
                r#"{"event":"reset","path":null,"kind":"fenced","action":"stop","class":"reset","#,
                r#""message":"the database was reset (fenced): another owner took the stream over: "#,
                r#"stop using this database, and never reopen it in a loop."},"#,
                r#"{"event":"condition","path":null,"condition":"retrying"},"#,
                r#"{"event":"sync_failed","ticket":4,"kind":"fenced","action":"stop","class":"reset","#,
                r#""message":"synced() failed: fenced: another owner took the stream over: stop "#,
                r#"using this database, and never reopen it in a loop."},"#,
                r#"{"event":"sync_failed","ticket":5,"kind":"not_bound","action":"fix","class":"setup","#,
                r#""message":"synced() failed: not_bound: fix the cause, then open() the database again."},"#,
                r#"{"event":"opened","request":6,"ok":false,"error":{"kind":"stream_state","action":"fix","class":"setup","message":"stream_state: NotSqlite"}},"#,
                r#"{"event":"gap","dropped":2}]}"#
            )
        );
    }

    // A binding handed out and not connected yet is live, or provisional before its claim; one
    // given up for another cache root retires.
    #[test]
    fn a_handed_out_binding_reports_live_or_provisional() {
        let handed_out = Life::HandedOut { awaiting: 1 };
        assert_eq!(state(false, handed_out, false), "live");
        assert_eq!(state(false, handed_out, true), "provisional");
        assert_eq!(state(false, Life::Live, true), "provisional");
        assert_eq!(state(false, Life::GivenUp, false), "retiring");
        assert_eq!(state(true, handed_out, true), "reset");
    }
}
