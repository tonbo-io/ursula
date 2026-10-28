//! The drain's pure parts as tables: the origin's rule, the plan, the outcome its bytes decide,
//! every kind with its `later`, and the JSON's keys.

use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use super::Concurrency;
use super::ConcurrencyError;
use super::Counts;
use super::Drained;
use super::Entry;
use super::Facts;
use super::FirstRead;
use super::LastRead;
use super::Later;
use super::NotAnOrigin;
use super::Opened;
use super::Origin;
use super::Outcome;
use super::Plan;
use super::Undrained;
use super::decide;
use super::later_of;
use super::plan;
use crate::error::ErrorKind;
use crate::generation::canonical_path;
use crate::generation::root_name;
use crate::json;
use crate::open::table::Declined;
use crate::replicator::BlockedReason;
use crate::replicator::Condition;
use crate::replicator::ResetKind;

const RESETS: [ResetKind; 7] = [
    ResetKind::Fenced,
    ResetKind::Stale,
    ResetKind::Rewound,
    ResetKind::Deleted,
    ResetKind::LocalDamage,
    ResetKind::StreamState,
    ResetKind::Internal,
];

/// Every [`ErrorKind`]. The match below fails to compile when a kind is added, so the list stays
/// whole.
fn kinds() -> Vec<ErrorKind> {
    let listed = |k: &ErrorKind| match k {
        ErrorKind::BacklogFull
        | ErrorKind::Unsupported
        | ErrorKind::TooLarge
        | ErrorKind::Reset(_)
        | ErrorKind::InvalidArgument
        | ErrorKind::InvalidUrl
        | ErrorKind::NotFound
        | ErrorKind::InUse
        | ErrorKind::BoundElsewhere
        | ErrorKind::LocalSetup
        | ErrorKind::StreamFormat
        | ErrorKind::NoServerIdentity
        | ErrorKind::ProtocolMismatch
        | ErrorKind::WrongCluster
        | ErrorKind::StreamState
        | ErrorKind::ClaimRaces
        | ErrorKind::Unreachable
        | ErrorKind::Unavailable
        | ErrorKind::RateLimited
        | ErrorKind::Unauthorized
        | ErrorKind::PaymentRequired
        | ErrorKind::Forbidden
        | ErrorKind::Cancelled
        | ErrorKind::NotBound
        | ErrorKind::Internal => true,
    };
    let mut all: Vec<ErrorKind> = [
        ErrorKind::BacklogFull,
        ErrorKind::Unsupported,
        ErrorKind::TooLarge,
        ErrorKind::InvalidArgument,
        ErrorKind::InvalidUrl,
        ErrorKind::NotFound,
        ErrorKind::InUse,
        ErrorKind::BoundElsewhere,
        ErrorKind::LocalSetup,
        ErrorKind::StreamFormat,
        ErrorKind::NoServerIdentity,
        ErrorKind::ProtocolMismatch,
        ErrorKind::WrongCluster,
        ErrorKind::StreamState,
        ErrorKind::ClaimRaces,
        ErrorKind::Unreachable,
        ErrorKind::Unavailable,
        ErrorKind::RateLimited,
        ErrorKind::Unauthorized,
        ErrorKind::PaymentRequired,
        ErrorKind::Forbidden,
        ErrorKind::Cancelled,
        ErrorKind::NotBound,
        ErrorKind::Internal,
    ]
    .to_vec();
    all.extend(RESETS.map(ErrorKind::Reset));
    assert!(all.iter().all(listed));
    all
}

/// Every condition a binding can report.
fn conditions() -> Vec<Condition> {
    let blocked = [
        BlockedReason::RequestTooLarge,
        BlockedReason::Unauthorized,
        BlockedReason::PaymentRequired,
        BlockedReason::Forbidden,
        BlockedReason::Redirected { location: None },
        BlockedReason::WrongCluster,
        BlockedReason::NoServerIdentity,
        BlockedReason::ProtocolMismatch { found: 7 },
        BlockedReason::Rejected { status: 418 },
    ];
    let mut all = vec![Condition::Retrying];
    all.extend(blocked.into_iter().map(Condition::Blocked));
    all
}

/// Every kind a drain reports, each way it can come about.
fn undrained() -> Vec<Undrained> {
    let mut all = vec![
        Undrained::NotOwner,
        Undrained::Misnamed,
        Undrained::Internal,
        Undrained::Budget,
        Undrained::Reopened,
        Undrained::Declined(Declined::Replaced),
        Undrained::Declined(Declined::NoClaim),
        Undrained::Declined(Declined::NothingPending),
    ];
    all.extend(kinds().into_iter().map(Undrained::Open));
    all.extend(RESETS.map(Undrained::Reset));
    all.extend(conditions().into_iter().map(Undrained::Condition));
    all
}

#[test]
fn every_kind_has_the_later_of_its_token_whatever_produced_it() {
    let mut seen: HashMap<&str, Later> = HashMap::new();
    for kind in undrained() {
        let later = kind.later();
        if let Some(before) = seen.insert(kind.token(), later) {
            assert_eq!(before, later, "{} from {kind:?}", kind.token());
        }
    }
    // The table of the drain's kinds (design: Hosts, Draining).
    let table = [
        ("not_owner", Later::Fix),
        ("misnamed", Later::No),
        ("in_use", Later::Yes),
        ("local_setup", Later::Fix),
        ("not_found", Later::No),
        ("stream_format", Later::No),
        ("stream_state", Later::No),
        ("wrong_cluster", Later::Fix),
        ("protocol_mismatch", Later::Fix),
        ("no_server_identity", Later::Fix),
        ("replaced", Later::No),
        ("fenced", Later::No),
        ("stale", Later::No),
        ("rewound", Later::No),
        ("deleted", Later::No),
        ("local_damage", Later::Fix),
        ("internal", Later::No),
        ("unreachable", Later::Yes),
        ("unavailable", Later::Yes),
        ("rate_limited", Later::Yes),
        ("retrying", Later::Yes),
        ("unauthorized", Later::Fix),
        ("payment_required", Later::Fix),
        ("forbidden", Later::Fix),
        ("request_too_large", Later::Fix),
        ("redirected", Later::Fix),
        ("rejected", Later::Fix),
        ("budget", Later::Yes),
        ("reopened", Later::Yes),
        // A drain cannot meet these: they map to `no`, so `later` is total.
        ("backlog_full", Later::No),
        ("unsupported", Later::No),
        ("too_large", Later::No),
        ("invalid_argument", Later::No),
        ("invalid_url", Later::No),
        ("bound_elsewhere", Later::No),
        ("claim_races", Later::No),
        ("cancelled", Later::No),
        ("not_bound", Later::No),
    ];
    for (token, later) in table {
        assert_eq!(seen.get(token), Some(&later), "{token}");
    }
    assert_eq!(seen.len(), table.len(), "{seen:?}");
    for kind in kinds() {
        assert_eq!(later_of(kind), Undrained::Open(kind).later());
    }
}

#[test]
fn the_origin_takes_a_scheme_a_host_and_a_port_only() {
    let accepted = [
        ("http://127.0.0.1:4437", "http://127.0.0.1:4437"),
        ("HTTPS://db.example.com/", "https://db.example.com"),
        ("http://[::1]:8080/", "http://[::1]:8080"),
        ("https://[2001:db8::1]", "https://[2001:db8::1]"),
        ("http://my_host-1.local:0", "http://my_host-1.local:0"),
    ];
    for (given, normal) in accepted {
        let origin = Origin::try_from(given).unwrap();
        assert_eq!(origin.as_str(), normal, "{given}");
    }
    let refused = [
        ("db.example.com", NotAnOrigin::Scheme),
        ("ftp://db.example.com", NotAnOrigin::Scheme),
        ("http://user@db.example.com", NotAnOrigin::Userinfo),
        ("http://db.example.com/v1", NotAnOrigin::Path),
        ("http://db.example.com//", NotAnOrigin::Path),
        ("http://db.example.com/?a=1", NotAnOrigin::QueryOrFragment),
        ("http://db.example.com#x", NotAnOrigin::QueryOrFragment),
        ("http://", NotAnOrigin::NoHost),
        ("http://:80", NotAnOrigin::NoHost),
        ("http://db example", NotAnOrigin::Host),
        ("http://::1", NotAnOrigin::NoHost),
        ("http://[::1", NotAnOrigin::Host),
        ("http://[not-v6]", NotAnOrigin::Host),
        ("http://[::1]x", NotAnOrigin::Host),
        ("http://db.example.com:", NotAnOrigin::Port),
        ("http://db.example.com:65536", NotAnOrigin::Port),
        ("http://db.example.com:+1", NotAnOrigin::Port),
    ];
    for (given, why) in refused {
        let e = Origin::try_from(given).unwrap_err();
        assert_eq!(e.why, why, "{given}");
    }
    // The stream URL of a root is the origin and its stream path, in canonical form.
    let origin = Origin::try_from("http://h:1/").unwrap();
    assert_eq!(origin.url("/b/s"), "http://h:1/b/s");
    assert_eq!(
        canonical_path(&origin.url("/b/s")).unwrap().as_str(),
        "/b/s"
    );
}

#[test]
fn concurrency_is_one_to_sixteen() {
    assert!(matches!(Concurrency::try_from(0), Err(ConcurrencyError(0))));
    assert_eq!(Concurrency::try_from(1).unwrap().0, 1);
    assert_eq!(Concurrency::try_from(16).unwrap().0, 16);
    assert!(matches!(
        Concurrency::try_from(17),
        Err(ConcurrencyError(17))
    ));
    assert_eq!(Concurrency::default().0, 4);
}

const STREAM: &str = "/b/notes";

fn counts(bytes: u64) -> Counts {
    Counts {
        bytes,
        commits: bytes.div_ceil(100),
        salvaged: 3,
    }
}

fn read(bytes: u64) -> Result<FirstRead, String> {
    Ok(FirstRead {
        stream: STREAM.to_owned(),
        cluster: "c1".to_owned(),
        counts: counts(bytes),
    })
}

/// The root of [`STREAM`] in the cache root `/cache`, by its own name.
fn own_root() -> PathBuf {
    let name = root_name(&canonical_path(&format!("http://h{STREAM}")).unwrap());
    Path::new("/cache").join(name)
}

fn kind(p: &Plan) -> Option<&Undrained> {
    match p {
        Plan::Done(Outcome::Pending { why, .. }) => Some(why),
        _ => None,
    }
}

#[test]
fn the_plan_opens_only_a_root_with_bytes_its_user_owns_under_its_own_name() {
    let origin = Origin::try_from("http://h").unwrap();
    let root = own_root();
    let facts = Facts {
        resolved: &root,
        owner: Some(1000),
        euid: Some(1000),
    };
    assert_eq!(plan(&read(500), facts, &origin), Plan::Drain {
        url: format!("http://h{STREAM}"),
        dir: PathBuf::from("/cache"),
    });
    assert_eq!(
        plan(&Err("torn".to_owned()), facts, &origin),
        Plan::Done(Outcome::Unreadable {
            error: "torn".to_owned()
        })
    );
    assert_eq!(
        plan(&read(0), facts, &origin),
        Plan::Done(Outcome::Clean(counts(0)))
    );
    // A clean root is clean whoever owns it.
    let other = Facts {
        owner: Some(0),
        ..facts
    };
    assert_eq!(
        plan(&read(0), other, &origin),
        Plan::Done(Outcome::Clean(counts(0)))
    );
    assert_eq!(
        kind(&plan(&read(500), other, &origin)),
        Some(&Undrained::NotOwner)
    );
    let unknown = Facts {
        owner: None,
        ..facts
    };
    assert_eq!(
        kind(&plan(&read(500), unknown, &origin)),
        Some(&Undrained::NotOwner)
    );
    let no_euid = Facts {
        euid: None,
        ..facts
    };
    assert_eq!(
        kind(&plan(&read(500), no_euid, &origin)),
        Some(&Undrained::Internal)
    );
    // Renamed, copied under another name, or a stream path the origin does not keep.
    let renamed = Path::new("/cache").join("notes-0123456789abcdef0123456789abcdef");
    let renamed = Facts {
        resolved: &renamed,
        ..facts
    };
    assert_eq!(
        kind(&plan(&read(500), renamed, &origin)),
        Some(&Undrained::Misnamed)
    );
    let odd = Ok(FirstRead {
        stream: "/b/./notes".to_owned(),
        ..read(500).unwrap()
    });
    assert_eq!(
        kind(&plan(&odd, facts, &origin)),
        Some(&Undrained::Misnamed)
    );
    // Every pending plan keeps the first read's numbers.
    match plan(&read(500), other, &origin) {
        Plan::Done(Outcome::Pending {
            counts: c, error, ..
        }) => {
            assert_eq!(c, counts(500));
            assert!(error.starts_with("not_owner: "), "{error}");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn the_bytes_of_the_last_read_decide_the_outcome() {
    let first = counts(500);
    let released = Opened::Released { reset: None };
    let refused = Opened::Refused {
        why: Undrained::Open(ErrorKind::InUse),
        error: "in_use: held".to_owned(),
    };
    assert_eq!(
        decide(released.clone(), LastRead::Read(counts(0)), first),
        Outcome::Drained(counts(0))
    );
    assert_eq!(
        decide(refused.clone(), LastRead::Read(counts(0)), first),
        Outcome::Clean(counts(0))
    );
    assert_eq!(
        decide(refused, LastRead::Read(counts(400)), first),
        Outcome::Pending {
            why: Undrained::Open(ErrorKind::InUse),
            counts: counts(400),
            error: "in_use: held".to_owned(),
        }
    );
    let reset = Opened::Released {
        reset: Some(ResetKind::Fenced),
    };
    let pending = |o: Outcome| match o {
        Outcome::Pending { why, counts, .. } => (why, counts),
        other => panic!("{other:?}"),
    };
    assert_eq!(
        pending(decide(reset.clone(), LastRead::Read(counts(500)), first)),
        (Undrained::Reset(ResetKind::Fenced), counts(500))
    );
    // A reset whose commits all landed meanwhile drained them.
    assert_eq!(
        decide(reset, LastRead::Read(counts(0)), first),
        Outcome::Drained(counts(0))
    );
    assert_eq!(
        pending(decide(released.clone(), LastRead::Read(counts(9)), first)),
        (Undrained::Reopened, counts(9))
    );
    assert_eq!(
        decide(
            released.clone(),
            LastRead::Unreadable("moved".to_owned()),
            first
        ),
        Outcome::Unreadable {
            error: "moved".to_owned()
        }
    );
    assert_eq!(
        pending(decide(released, LastRead::Cut, first)),
        (Undrained::Budget, first)
    );
}

fn entry(first: Result<FirstRead, String>, outcome: Option<Outcome>) -> Entry {
    Entry {
        id: "notes-0123456789abcdef0123456789abcdef".to_owned(),
        root: PathBuf::from("/cache/notes-0123456789abcdef0123456789abcdef"),
        uid: Some(1000),
        first,
        outcome,
    }
}

/// The keys of a flat object's JSON, in order, with their values.
fn fields(text: &str) -> Vec<(String, json::Value)> {
    json::parse_object(text).unwrap()
}

#[test]
fn the_json_has_its_keys_in_order_and_each_outcome_its_invariants() {
    let drained = Drained {
        origin: Origin::try_from("http://h").unwrap(),
        uid: Some(1000),
        dir: None,
        entries: Vec::new(),
        error: None,
    };
    assert_eq!(
        drained.to_json(),
        r#"{"v":1,"origin":"http://h","uid":1000,"dir":null,"roots":[],"error":null}"#
    );
    let keys = [
        "id",
        "root",
        "stream",
        "cluster",
        "uid",
        "outcome",
        "kind",
        "later",
        "before_bytes",
        "unacknowledged_bytes",
        "commits",
        "salvaged",
        "error",
    ];
    let cases = [
        (
            entry(read(500), Some(Outcome::Drained(counts(0)))),
            "drained",
        ),
        (entry(read(0), Some(Outcome::Clean(counts(0)))), "clean"),
        (
            entry(
                read(500),
                Some(Outcome::pending(
                    Undrained::Reset(ResetKind::Fenced),
                    counts(500),
                    "x",
                )),
            ),
            "pending",
        ),
        // Still draining at the deadline: its first read's numbers.
        (entry(read(500), None), "pending"),
        (
            entry(
                Err("torn".to_owned()),
                Some(Outcome::Unreadable {
                    error: "torn".to_owned(),
                }),
            ),
            "unreadable",
        ),
        (
            entry(
                read(500),
                Some(Outcome::Unreadable {
                    error: "moved".to_owned(),
                }),
            ),
            "unreadable",
        ),
    ];
    for (e, outcome) in cases {
        let text = super::entry_json(&e, &|p: &Path| json::str(&p.to_string_lossy()));
        let got = fields(&text);
        let names: Vec<&str> = got.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(names, keys, "{text}");
        let value = |key: &str| {
            got.iter()
                .find(|(k, _)| k == key)
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        assert_eq!(
            value("outcome"),
            json::Value::Text(outcome.to_owned()),
            "{text}"
        );
        let null = |key: &str| value(key) == json::Value::Null;
        let int = |key: &str| match value(key) {
            json::Value::Number(n) => json::whole(n),
            _ => None,
        };
        match outcome {
            "drained" | "clean" => {
                assert!(null("kind") && null("later") && null("error"), "{text}");
                assert_eq!(int("unacknowledged_bytes"), Some(0), "{text}");
            }
            "pending" => {
                assert!(!null("kind") && !null("later") && !null("error"), "{text}");
                assert_eq!(int("unacknowledged_bytes"), Some(500), "{text}");
                assert_eq!(int("before_bytes"), Some(500), "{text}");
            }
            _ => {
                assert!(null("kind") && null("later") && !null("error"), "{text}");
                assert!(null("unacknowledged_bytes") && null("commits") && null("salvaged"));
            }
        }
    }
}
