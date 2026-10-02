//! Runs `tests/vectors/keyed_batch_v1.json` against the reference parser and
//! fold (design §10 M0a).

#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test helpers outside #[test] functions index and do arithmetic on generated data"
)]

use serde::Deserialize;
use ursula_index::keyed::KeyedBatchErrorReason;
use ursula_index::keyed::KeyedOp;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::decode_key;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::parse_batch;
use ursula_index::keyed::validate_messages;

#[derive(Deserialize)]
struct Vectors {
    profile: String,
    messages: Vec<MessageVector>,
    sequences: Vec<SequenceVector>,
    folds: Vec<FoldVector>,
}

#[derive(Deserialize)]
struct MessageVector {
    name: String,
    message: String,
    valid: bool,
    #[serde(default)]
    ops: Option<Vec<Vec<String>>>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize)]
struct SequenceVector {
    name: String,
    messages: Vec<String>,
    first_invalid: Option<usize>,
}

#[derive(Deserialize)]
struct FoldVector {
    name: String,
    log: Vec<String>,
    states: Vec<StateVector>,
    reads: Vec<ReadVector>,
}

#[derive(Deserialize)]
struct StateVector {
    through: usize,
    rows: Vec<RowVector>,
}

#[derive(Deserialize)]
struct RowVector {
    key: String,
    record: u64,
    value: String,
}

#[derive(Deserialize)]
struct ReadVector {
    through: usize,
    lower: Option<LowerVector>,
    end: Option<String>,
    limit: usize,
    budget: Option<usize>,
    keys: Vec<String>,
    after: Option<String>,
    #[serde(default)]
    body: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum LowerVector {
    Start(String),
    After(String),
}

fn load() -> Vectors {
    let text = include_str!("vectors/keyed_batch_v1.json");
    serde_json::from_str(text).expect("vectors parse")
}

fn render_op(op: &KeyedOp<'_>) -> Vec<String> {
    match op {
        KeyedOp::Put { key, value } => vec!["p".into(), encode_key(key), value.get().to_owned()],
        KeyedOp::Delete { key } => vec!["d".into(), encode_key(key)],
        KeyedOp::DeleteRange { start, end } => {
            vec!["x".into(), encode_key(start), encode_key(end)]
        }
    }
}

fn state_at(log: &[String], through: usize) -> KeyedState {
    KeyedState::fold(log[..through].iter().map(String::as_str)).expect("fold log")
}

#[test]
fn message_vectors() {
    let vectors = load();
    assert_eq!(vectors.profile, ursula_index::keyed::KEYED_BATCH_PROFILE);
    assert!(vectors.messages.len() > 50);
    for vector in &vectors.messages {
        let result = parse_batch(&vector.message);
        if vector.valid {
            let batch = result.unwrap_or_else(|error| panic!("{}: rejected: {error}", vector.name));
            let ops: Vec<_> = batch.ops.iter().map(render_op).collect();
            assert_eq!(Some(&ops), vector.ops.as_ref(), "{}", vector.name);
        } else {
            let error = match result {
                Ok(batch) => panic!("{}: accepted: {batch:?}", vector.name),
                Err(error) => error,
            };
            let expected = vector.reason.as_deref().expect("reason");
            assert!(
                KeyedBatchErrorReason::from_name(expected).is_some(),
                "{}: unknown reason {expected}",
                vector.name
            );
            assert_eq!(
                error.reason().as_str(),
                expected,
                "{}: {error}",
                vector.name
            );
        }
    }
}

#[test]
fn sequence_vectors() {
    for vector in load().sequences {
        let result = validate_messages(vector.messages.iter().map(String::as_str));
        assert_eq!(
            result.err().map(|error| error.index),
            vector.first_invalid,
            "{}",
            vector.name
        );
    }
}

#[test]
fn fold_vectors() {
    for vector in load().folds {
        for expected in &vector.states {
            let state = state_at(&vector.log, expected.through);
            assert_eq!(state.through(), expected.through as u64, "{}", vector.name);
            let rows: Vec<(String, u64, String)> = state
                .rows()
                .iter()
                .map(|(key, row)| (encode_key(key), row.record, row.value.get().to_owned()))
                .collect();
            let want: Vec<(String, u64, String)> = expected
                .rows
                .iter()
                .map(|row| (row.key.clone(), row.record, row.value.clone()))
                .collect();
            assert_eq!(rows, want, "{} at D={}", vector.name, expected.through);
        }
        for read in &vector.reads {
            let state = state_at(&vector.log, read.through);
            let key = |text: &str| decode_key(text).expect("vector key");
            let query = RangeQuery {
                lower: match &read.lower {
                    None => Lower::First,
                    Some(LowerVector::Start(k)) => Lower::Start(key(k)),
                    Some(LowerVector::After(k)) => Lower::After(key(k)),
                },
                end: read.end.as_deref().map(key),
                limit: read.limit,
                budget: read.budget,
            };
            let page = state.range(&query);
            let keys: Vec<String> = page.rows.iter().map(|(k, _row)| encode_key(k)).collect();
            assert_eq!(keys, read.keys, "{}: {query:?}", vector.name);
            assert_eq!(
                page.after.as_deref().map(encode_key),
                read.after,
                "{}: {query:?}",
                vector.name
            );
            if let Some(body) = &read.body {
                assert_eq!(&page.body(), body, "{}", vector.name);
            }
        }
    }
}
