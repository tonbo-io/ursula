use std::sync::Arc;
use std::sync::Barrier;
use std::time::Duration;

use tokio::time::Instant;

use super::Ejection;
use super::Ejections;
use super::Failed;
use super::Pick;
use super::Table;

/// The gateway's default probe deadline: 5 s connect plus 5 s user
/// timeout.
const PROBE_DEADLINE: Duration = Duration::from_secs(10);

fn secs(secs: u64) -> Duration {
    Duration::from_secs(secs)
}

fn after(now: Instant, by: Duration) -> Instant {
    now.checked_add(by).unwrap()
}

fn upstreams() -> [String; 2] {
    ["a", "b"].map(String::from)
}

fn pick<'a>(
    table: &mut Table,
    cached: Option<&'a str>,
    upstreams: &'a [String],
    now: Instant,
) -> Pick<'a> {
    table
        .pick(cached, upstreams, now, PROBE_DEADLINE, &mut rand::rng())
        .unwrap()
}

/// `upstream` ejected once at `now`, by itself in the table.
fn ejected_at(upstream: &str, now: Instant) -> Table {
    let mut table = Table::default();
    table.failed(upstream, now, PROBE_DEADLINE);
    table
}

#[test]
fn a_first_failure_ejects_for_the_base_window() {
    let mut table = Table::default();
    let now = Instant::now();
    assert_eq!(table.failed("a", now, PROBE_DEADLINE), Failed::Ejected {
        window: secs(10),
        failures: 1,
    });
    assert_eq!(
        table.0.get("a"),
        Some(&Ejection::Out {
            until: after(now, secs(10)),
            failures: 1,
        })
    );
}

// Requests that were in flight before the ejection fail inside the
// window and do not lengthen it.
#[test]
fn failures_inside_the_window_change_nothing() {
    let now = Instant::now();
    let mut table = ejected_at("a", now);
    let before = table.0.get("a").copied();
    for at in [secs(0), secs(1), secs(9)] {
        assert_eq!(
            table.failed("a", after(now, at), PROBE_DEADLINE),
            Failed::AlreadyEjected
        );
    }
    assert_eq!(table.0.get("a").copied(), before);
}

#[test]
fn an_active_window_is_skipped_by_cached_routes_and_random_picks() {
    let now = Instant::now();
    let mut table = ejected_at("a", now);
    let upstreams = upstreams();
    for at in [secs(0), secs(9)] {
        let at = after(now, at);
        assert_eq!(pick(&mut table, Some("a"), &upstreams, at), Pick {
            upstream: "b",
            cached: false,
            probe: false,
        });
        assert_eq!(pick(&mut table, None, &upstreams, at).upstream, "b");
    }
    assert_eq!(table.active(after(now, secs(9)), PROBE_DEADLINE), 1);
}

#[test]
fn the_first_pick_after_the_window_claims_the_probe() {
    let now = Instant::now();
    let mut table = ejected_at("a", now);
    let upstreams = upstreams();
    let expired = after(now, secs(10));
    assert_eq!(table.active(expired, PROBE_DEADLINE), 0);

    assert_eq!(pick(&mut table, Some("a"), &upstreams, expired), Pick {
        upstream: "a",
        cached: true,
        probe: true,
    });
    assert_eq!(
        table.0.get("a"),
        Some(&Ejection::Probing {
            failures: 1,
            since: expired,
        })
    );
}

// Two callers at the same instant, both routed to the expired upstream:
// only the first probes it. The second goes elsewhere, and so does every
// request until the probe resolves or its deadline passes.
#[test]
fn two_callers_send_one_probe() {
    let now = Instant::now();
    let mut table = ejected_at("a", now);
    let upstreams = upstreams();
    let expired = after(now, secs(10));

    let first = pick(&mut table, Some("a"), &upstreams, expired);
    let second = pick(&mut table, Some("a"), &upstreams, expired);

    assert_eq!((first.upstream, first.probe), ("a", true));
    assert_eq!(second, Pick {
        upstream: "b",
        cached: false,
        probe: false,
    });
    let before_deadline = after(expired, PROBE_DEADLINE.saturating_sub(secs(1)));
    assert_eq!(
        pick(&mut table, None, &upstreams, before_deadline).upstream,
        "b"
    );
    assert_eq!(table.active(before_deadline, PROBE_DEADLINE), 1);
}

// A probe that neither answered nor failed by its deadline no longer
// holds the others back: the next request to select the upstream probes
// it again.
#[test]
fn a_probe_past_its_deadline_is_claimed_again() {
    let now = Instant::now();
    let mut table = ejected_at("a", now);
    let upstreams = upstreams();
    let expired = after(now, secs(10));
    pick(&mut table, Some("a"), &upstreams, expired);

    let stale = after(expired, PROBE_DEADLINE);
    assert_eq!(table.active(stale, PROBE_DEADLINE), 0);
    assert_eq!(pick(&mut table, Some("a"), &upstreams, stale), Pick {
        upstream: "a",
        cached: true,
        probe: true,
    });
    assert_eq!(
        table.0.get("a"),
        Some(&Ejection::Probing {
            failures: 1,
            since: stale,
        })
    );
}

// Each failure after the first comes from the probe of the window before.
#[test]
fn a_failed_probe_ejects_for_a_doubled_window_up_to_the_cap() {
    let mut now = Instant::now();
    let mut table = Table::default();
    let upstreams = upstreams();
    for (failures, window) in [(1, 10), (2, 20), (3, 40), (4, 60), (5, 60)] {
        assert_eq!(table.failed("a", now, PROBE_DEADLINE), Failed::Ejected {
            window: secs(window),
            failures,
        });
        assert_eq!(
            table.0.get("a"),
            Some(&Ejection::Out {
                until: after(now, secs(window)),
                failures,
            })
        );
        now = after(now, secs(window));
        assert!(pick(&mut table, Some("a"), &upstreams, now).probe);
        now = after(now, secs(1));
    }
}

// A request that did not go through selection, such as one that follows
// a redirect, can fail after the window too.
#[test]
fn a_failure_after_an_unclaimed_window_doubles_it() {
    let now = Instant::now();
    let mut table = ejected_at("a", now);
    assert_eq!(
        table.failed("a", after(now, secs(10)), PROBE_DEADLINE),
        Failed::Ejected {
            window: secs(20),
            failures: 2,
        }
    );
}

#[test]
fn any_answer_clears_the_ejection() {
    let now = Instant::now();
    let upstreams = upstreams();

    let mut out = ejected_at("a", now);
    assert!(out.answered("a"));
    assert!(out.0.is_empty());

    let mut probing = ejected_at("a", now);
    pick(&mut probing, Some("a"), &upstreams, after(now, secs(10)));
    assert!(probing.answered("a"));
    assert!(probing.0.is_empty());

    assert!(!probing.answered("a"));
    assert_eq!(probing.failed("a", now, PROBE_DEADLINE), Failed::Ejected {
        window: secs(10),
        failures: 1,
    });
}

// With every upstream ejected or being probed, requests still go out:
// along the cached route, which is kept, or to any upstream. None of
// them claims a probe.
#[test]
fn selection_falls_back_to_every_upstream_and_keeps_the_cached_route() {
    let now = Instant::now();
    let upstreams = upstreams();
    let mut table = ejected_at("a", now);
    table.failed("b", after(now, secs(5)), PROBE_DEADLINE);
    let probing_a = after(now, secs(10));
    pick(&mut table, Some("a"), &upstreams, probing_a);
    let before = table.0.clone();

    assert_eq!(pick(&mut table, Some("b"), &upstreams, probing_a), Pick {
        upstream: "b",
        cached: true,
        probe: false,
    });
    for _ in 0..20 {
        assert!(!pick(&mut table, None, &upstreams, probing_a).probe);
    }
    assert_eq!(table.0, before);
    assert_eq!(table.active(probing_a, PROBE_DEADLINE), 2);
}

#[test]
fn the_entry_count_follows_the_table() {
    let ejections = Ejections::new(PROBE_DEADLINE);
    let upstreams = upstreams();
    let now = Instant::now();
    assert_eq!(ejections.active(now), 0);
    assert!(!ejections.answered("a"));
    assert_eq!(
        ejections
            .pick(Some("a"), &upstreams, now, &mut rand::rng())
            .unwrap(),
        Pick {
            upstream: "a",
            cached: true,
            probe: false,
        }
    );

    ejections.failed("a", now);
    assert_eq!(ejections.active(now), 1);
    assert_eq!(
        ejections
            .pick(Some("a"), &upstreams, now, &mut rand::rng())
            .unwrap()
            .upstream,
        "b"
    );
    assert!(ejections.answered("a"));
    assert_eq!(ejections.active(now), 0);
    assert_eq!(
        ejections
            .pick(Some("a"), &upstreams, now, &mut rand::rng())
            .unwrap()
            .upstream,
        "a"
    );
}

// Callers on different threads at the same instant, all routed to the
// expired upstream: exactly one probes it.
#[test]
fn concurrent_callers_send_one_probe() {
    const CALLERS: usize = 16;
    let ejections = Arc::new(Ejections::new(PROBE_DEADLINE));
    let now = Instant::now();
    ejections.failed("a", now);
    let expired = after(now, secs(10));
    let start = Arc::new(Barrier::new(CALLERS));

    let callers: Vec<_> = (0..CALLERS)
        .map(|_| {
            let ejections = Arc::clone(&ejections);
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                let upstreams = upstreams();
                start.wait();
                let pick = ejections
                    .pick(Some("a"), &upstreams, expired, &mut rand::rng())
                    .unwrap();
                (pick.upstream.to_owned(), pick.probe)
            })
        })
        .collect();
    let picks: Vec<_> = callers.into_iter().map(|c| c.join().unwrap()).collect();

    assert_eq!(picks.iter().filter(|(_, probe)| *probe).count(), 1);
    assert_eq!(
        picks.iter().filter(|(upstream, _)| upstream == "a").count(),
        1
    );
}
