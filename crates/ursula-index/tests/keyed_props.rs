//! Property tests for `keyed-batch-v1` (design §11.6) and the reference fold
//! (§11.4): accepted ⇔ grammatical over generated batches and targeted
//! mutations, canonical base64url, no panics on arbitrary text, and the
//! `BTreeMap` fold against a naive scan.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test helpers outside #[test] functions index and do arithmetic on generated data"
)]

use proptest::prelude::*;
use serde_json::value::RawValue;
use ursula_index::keyed::KeyedBatchErrorReason;
use ursula_index::keyed::KeyedOp;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::MAX_KEY_OCTETS;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::decode_key;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::parse_batch;
use ursula_index::keyed::row_line;

// ---------------------------------------------------------------------------
// Grammatical batch model and renderer.

#[derive(Debug, Clone, PartialEq)]
enum ModelOp {
    Put(Vec<u8>, String),
    Delete(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
}

#[derive(Debug, Clone)]
struct ModelBatch {
    ops: Vec<ModelOp>,
    /// Other members as (raw name text including quotes, raw value text).
    extra_before: Vec<(String, String)>,
    extra_after: Vec<(String, String)>,
    /// Spelling of the `ops` member name, including quotes.
    ops_name: String,
    /// Op code spellings, by op: escaped or plain.
    escape_codes: Vec<bool>,
}

fn escape_all(text: &str) -> String {
    text.chars()
        .map(|c| format!("\\u{:04x}", c as u32))
        .collect()
}

fn code_text(code: &str, escaped: bool) -> String {
    if escaped {
        format!("\"{}\"", escape_all(code))
    } else {
        format!("\"{code}\"")
    }
}

impl ModelBatch {
    fn render_ops(&self) -> String {
        let ops: Vec<String> = self
            .ops
            .iter()
            .enumerate()
            .map(|(i, op)| {
                let escaped = self.escape_codes.get(i).copied().unwrap_or(false);
                match op {
                    ModelOp::Put(k, v) => {
                        format!("[{},\"{}\",{v}]", code_text("p", escaped), encode_key(k))
                    }
                    ModelOp::Delete(k) => {
                        format!("[{},\"{}\"]", code_text("d", escaped), encode_key(k))
                    }
                    ModelOp::Range(s, e) => format!(
                        "[{},\"{}\",\"{}\"]",
                        code_text("x", escaped),
                        encode_key(s),
                        encode_key(e)
                    ),
                }
            })
            .collect();
        format!("[{}]", ops.join(","))
    }

    fn render_with(&self, ops_member: Option<String>) -> String {
        let mut members: Vec<String> = Vec::new();
        members.extend(self.extra_before.iter().map(|(n, v)| format!("{n}:{v}")));
        if let Some(ops) = ops_member {
            members.push(ops);
        }
        members.extend(self.extra_after.iter().map(|(n, v)| format!("{n}:{v}")));
        format!("{{{}}}", members.join(","))
    }

    fn render(&self) -> String {
        self.render_with(Some(format!("{}:{}", self.ops_name, self.render_ops())))
    }
}

fn arb_key() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        8 => prop::collection::vec(any::<u8>(), 1..6),
        1 => prop::collection::vec(any::<u8>(), 1..=MAX_KEY_OCTETS),
    ]
}

/// JSON string literal text, possibly with escapes and lone surrogates.
fn arb_string_literal() -> impl Strategy<Value = String> {
    let piece = prop_oneof![
        "[a-zA-Z0-9 ]{0,4}",
        Just("\\\"".to_owned()),
        Just("\\\\".to_owned()),
        Just("\\n".to_owned()),
        Just("\\ud800".to_owned()),
        Just("\\udc00".to_owned()),
        Just("\\ud83d\\ude00".to_owned()),
        Just("é😀".to_owned()),
        Just("\\u0070".to_owned()),
    ];
    prop::collection::vec(piece, 0..4).prop_map(|parts| format!("\"{}\"", parts.concat()))
}

fn arb_json_value() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        Just("null".to_owned()),
        Just("true".to_owned()),
        Just("false".to_owned()),
        Just("1e400".to_owned()),
        Just("-0.0E+1".to_owned()),
        Just("1.50e3".to_owned()),
        any::<i64>().prop_map(|n| n.to_string()),
        arb_string_literal(),
    ];
    leaf.prop_recursive(4, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4)
                .prop_map(|items| format!("[{}]", items.join(","))),
            prop::collection::vec((arb_string_literal(), inner), 0..4).prop_map(|members| {
                let members: Vec<String> = members
                    .into_iter()
                    .map(|(n, v)| format!("{n}:{v}"))
                    .collect();
                format!("{{{}}}", members.join(","))
            }),
        ]
    })
}

fn arb_op() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        3 => (arb_key(), arb_json_value()).prop_map(|(k, v)| ModelOp::Put(k, v)),
        1 => arb_key().prop_map(ModelOp::Delete),
        1 => (arb_key(), arb_key())
            .prop_filter("distinct bounds", |(a, b)| a != b)
            .prop_map(|(a, b)| if a < b { ModelOp::Range(a, b) } else { ModelOp::Range(b, a) }),
    ]
}

/// A top-level member name that is not `ops` and has no unpaired surrogate.
fn arb_other_name() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-z]{0,5}"
            .prop_filter("not ops", |s| s != "ops")
            .prop_map(|s| format!("\"{s}\"")),
        Just("\"Ops\"".to_owned()),
        Just("\"op\\u0073x\"".to_owned()),
        Just("\"\\ud83d\\ude00\"".to_owned()),
        Just("\"p\"".to_owned()),
    ]
}

fn arb_ops_name() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("\"ops\"".to_owned()),
        Just("\"o\\u0070s\"".to_owned()),
        Just("\"\\u006f\\u0070\\u0073\"".to_owned()),
        Just("\"op\\u0073\"".to_owned()),
    ]
}

fn arb_batch(min_ops: usize) -> impl Strategy<Value = ModelBatch> {
    (
        prop::collection::vec(arb_op(), min_ops..6),
        prop::collection::vec((arb_other_name(), arb_json_value()), 0..3),
        prop::collection::vec((arb_other_name(), arb_json_value()), 0..3),
        arb_ops_name(),
        prop::collection::vec(any::<bool>(), 6),
    )
        .prop_map(
            |(ops, extra_before, extra_after, ops_name, escape_codes)| ModelBatch {
                ops,
                extra_before,
                extra_after,
                ops_name,
                escape_codes,
            },
        )
}

fn to_model(op: &KeyedOp<'_>) -> ModelOp {
    match op {
        KeyedOp::Put { key, value } => ModelOp::Put(key.clone(), value.get().to_owned()),
        KeyedOp::Delete { key } => ModelOp::Delete(key.clone()),
        KeyedOp::DeleteRange { start, end } => ModelOp::Range(start.clone(), end.clone()),
    }
}

// ---------------------------------------------------------------------------
// Targeted mutations: each produces a message that is not grammatical, with a
// known reason.

#[derive(Debug, Clone)]
enum Mutation {
    DropOps,
    DuplicateOps,
    LoneSurrogateName(bool),
    OpsNotArray,
    WrapInArray,
    BadKey(usize, &'static str),
    EscapedKey(usize),
    UnknownCode(usize, &'static str),
    ExtraElement(usize),
    MissingElement(usize),
    /// A range delete whose start is not below its end: (start, end).
    BadRange(Vec<u8>, Vec<u8>),
    TruncateText,
}

fn arb_mutation() -> impl Strategy<Value = Mutation> {
    let bad_key = prop_oneof![
        Just("AB"),
        Just("AA=="),
        Just("+/"),
        Just(""),
        Just("A"),
        Just("AQ="),
        Just("A Q"),
    ];
    let bad_code = prop_oneof![
        Just("\"P\""),
        Just("\"put\""),
        Just("1"),
        Just("null"),
        Just("\"\\ud800\"")
    ];
    prop_oneof![
        Just(Mutation::DropOps),
        Just(Mutation::DuplicateOps),
        any::<bool>().prop_map(Mutation::LoneSurrogateName),
        Just(Mutation::OpsNotArray),
        Just(Mutation::WrapInArray),
        (any::<usize>(), bad_key).prop_map(|(i, k)| Mutation::BadKey(i, k)),
        any::<usize>().prop_map(Mutation::EscapedKey),
        (any::<usize>(), bad_code).prop_map(|(i, c)| Mutation::UnknownCode(i, c)),
        any::<usize>().prop_map(Mutation::ExtraElement),
        any::<usize>().prop_map(Mutation::MissingElement),
        (arb_key(), arb_key()).prop_map(|(a, b)| if a >= b {
            Mutation::BadRange(a, b)
        } else {
            Mutation::BadRange(b, a)
        }),
        arb_key().prop_map(|k| Mutation::BadRange(k.clone(), k)),
        Just(Mutation::TruncateText),
    ]
}

fn op_parts(op: &ModelOp) -> (String, Vec<String>) {
    match op {
        ModelOp::Put(k, v) => ("\"p\"".into(), vec![
            format!("\"{}\"", encode_key(k)),
            v.clone(),
        ]),
        ModelOp::Delete(k) => ("\"d\"".into(), vec![format!("\"{}\"", encode_key(k))]),
        ModelOp::Range(s, e) => ("\"x\"".into(), vec![
            format!("\"{}\"", encode_key(s)),
            format!("\"{}\"", encode_key(e)),
        ]),
    }
}

/// Renders the batch with op `target` replaced by `replacement` raw text.
fn with_op(batch: &ModelBatch, target: usize, replacement: String) -> String {
    let ops: Vec<String> = batch
        .ops
        .iter()
        .enumerate()
        .map(|(i, op)| {
            if i == target {
                replacement.clone()
            } else {
                let (code, args) = op_parts(op);
                format!("[{code},{}]", args.join(","))
            }
        })
        .collect();
    batch.render_with(Some(format!("\"ops\":[{}]", ops.join(","))))
}

/// Applies a mutation; returns the text and the expected reason.
fn mutate(batch: &ModelBatch, mutation: &Mutation) -> (String, KeyedBatchErrorReason) {
    use KeyedBatchErrorReason as R;
    let n = batch.ops.len();
    let ops_member = format!("{}:{}", batch.ops_name, batch.render_ops());
    match mutation {
        Mutation::DropOps => (batch.render_with(None), R::MissingOps),
        Mutation::DuplicateOps => (
            batch.render_with(Some(format!("{ops_member},\"o\\u0070s\":[]"))),
            R::DuplicateOps,
        ),
        Mutation::LoneSurrogateName(before) => {
            let bad = "\"a\\udc00\":1".to_owned();
            let member = if *before {
                format!("{bad},{ops_member}")
            } else {
                format!("{ops_member},{bad}")
            };
            // Only extras placed before the bad name could fail first, and
            // they are grammatical, so the reason is fixed.
            (batch.render_with(Some(member)), R::InvalidMemberName)
        }
        Mutation::OpsNotArray => (
            batch.render_with(Some(format!("{}:{{}}", batch.ops_name))),
            R::OpsNotArray,
        ),
        Mutation::WrapInArray => (format!("[{}]", batch.render()), R::NotObject),
        Mutation::BadKey(i, key) => {
            let i = i % n;
            let (code, mut args) = op_parts(&batch.ops[i]);
            args[0] = format!("\"{key}\"");
            (
                with_op(batch, i, format!("[{code},{}]", args.join(","))),
                R::InvalidKey,
            )
        }
        Mutation::EscapedKey(i) => {
            let i = i % n;
            let (code, mut args) = op_parts(&batch.ops[i]);
            let inner = &args[0][1..args[0].len() - 1];
            args[0] = format!("\"{}\"", escape_all(inner));
            (
                with_op(batch, i, format!("[{code},{}]", args.join(","))),
                R::InvalidKey,
            )
        }
        Mutation::UnknownCode(i, code) => {
            let i = i % n;
            let (_code, args) = op_parts(&batch.ops[i]);
            (
                with_op(batch, i, format!("[{code},{}]", args.join(","))),
                R::UnknownOpCode,
            )
        }
        Mutation::ExtraElement(i) => {
            let i = i % n;
            let (code, args) = op_parts(&batch.ops[i]);
            (
                with_op(batch, i, format!("[{code},{},0]", args.join(","))),
                R::InvalidOp,
            )
        }
        Mutation::MissingElement(i) => {
            let i = i % n;
            let (code, mut args) = op_parts(&batch.ops[i]);
            args.pop();
            let text = if args.is_empty() {
                format!("[{code}]")
            } else {
                format!("[{code},{}]", args.join(","))
            };
            (with_op(batch, i, text), R::InvalidOp)
        }
        Mutation::BadRange(start, end) => {
            let text = format!("[\"x\",\"{}\",\"{}\"]", encode_key(start), encode_key(end));
            (with_op(batch, 0, text), R::EmptyRange)
        }
        Mutation::TruncateText => {
            let text = batch.render();
            (text[..text.len() - 1].to_owned(), R::InvalidJson)
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical base64url oracle, independent of the base64 crate.

const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn canonical_by_rule(text: &str) -> bool {
    let Some(values) = text
        .bytes()
        .map(|b| ALPHABET.iter().position(|a| *a == b))
        .collect::<Option<Vec<_>>>()
    else {
        return false;
    };
    let octets = values.len() * 6 / 8;
    if values.is_empty() || octets > MAX_KEY_OCTETS {
        return false;
    }
    match values.len() % 4 {
        1 => false,
        2 => values[values.len() - 1] & 0x0f == 0,
        3 => values[values.len() - 1] & 0x03 == 0,
        _ => true,
    }
}

// ---------------------------------------------------------------------------
// Naive fold and read model.

type NaiveRow = (u64, String);

fn naive_get(log: &[Vec<ModelOp>], key: &[u8]) -> Option<NaiveRow> {
    let mut row = None;
    for (r, ops) in log.iter().enumerate() {
        for op in ops {
            match op {
                ModelOp::Put(k, v) if k.as_slice() == key => row = Some((r as u64, v.clone())),
                ModelOp::Delete(k) if k.as_slice() == key => row = None,
                ModelOp::Range(s, e) if s.as_slice() <= key && key < e.as_slice() => row = None,
                _ => {}
            }
        }
    }
    row
}

fn naive_state(log: &[Vec<ModelOp>]) -> Vec<(Vec<u8>, NaiveRow)> {
    let mut keys: Vec<Vec<u8>> = log
        .iter()
        .flatten()
        .filter_map(|op| match op {
            ModelOp::Put(k, _) => Some(k.clone()),
            _ => None,
        })
        .collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|k| naive_get(log, &k).map(|row| (k, row)))
        .collect()
}

fn render_log(log: &[Vec<ModelOp>]) -> Vec<String> {
    log.iter()
        .map(|ops| {
            ModelBatch {
                ops: ops.clone(),
                extra_before: Vec::new(),
                extra_after: Vec::new(),
                ops_name: "\"ops\"".into(),
                escape_codes: Vec::new(),
            }
            .render()
        })
        .collect()
}

fn small_key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(
        prop_oneof![Just(0_u8), Just(1), Just(0x7f), Just(0x80), Just(0xff)],
        1..3,
    )
}

fn small_op() -> impl Strategy<Value = ModelOp> {
    prop_oneof![
        4 => (small_key(), 0_u32..100).prop_map(|(k, v)| ModelOp::Put(k, v.to_string())),
        2 => small_key().prop_map(ModelOp::Delete),
        1 => (small_key(), small_key())
            .prop_filter("distinct", |(a, b)| a != b)
            .prop_map(|(a, b)| if a < b { ModelOp::Range(a, b) } else { ModelOp::Range(b, a) }),
    ]
}

fn arb_query() -> impl Strategy<Value = RangeQuery> {
    (
        prop_oneof![
            Just(Lower::First),
            small_key().prop_map(Lower::Start),
            small_key().prop_map(Lower::After),
        ],
        prop::option::of(small_key()),
        0_usize..8,
        prop::option::of(0_usize..200),
    )
        .prop_map(|(lower, end, limit, budget)| RangeQuery {
            lower,
            end,
            limit,
            budget,
        })
}

fn naive_read(rows: &[(Vec<u8>, NaiveRow)], query: &RangeQuery) -> (Vec<Vec<u8>>, Option<Vec<u8>>) {
    let in_range: Vec<&(Vec<u8>, NaiveRow)> = rows
        .iter()
        .filter(|(k, _)| match &query.lower {
            Lower::First => true,
            Lower::Start(s) => k >= s,
            Lower::After(a) => k > a,
        })
        .filter(|(k, _)| query.end.as_ref().is_none_or(|e| k < e))
        .collect();
    let mut taken = Vec::new();
    let mut used = 0;
    for (k, (r, v)) in &in_range {
        if taken.len() >= query.limit {
            break;
        }
        let line = format!(
            "{{\"key\":\"{}\",\"record\":{r},\"value\":{v}}}\n",
            encode_key(k)
        );
        if let Some(budget) = query.budget
            && !taken.is_empty()
            && used + line.len() > budget
        {
            break;
        }
        used += line.len();
        taken.push(k.clone());
    }
    let after = if taken.len() < in_range.len() {
        taken.last().cloned()
    } else {
        None
    };
    (taken, after)
}

// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn grammatical_batches_are_accepted(batch in arb_batch(0)) {
        let text = batch.render();
        let parsed = parse_batch(&text).map_err(|e| TestCaseError::fail(format!("{text}: {e}")))?;
        let ops: Vec<ModelOp> = parsed.ops.iter().map(to_model).collect();
        prop_assert_eq!(ops, batch.ops);
    }

    #[test]
    fn mutated_batches_are_rejected(batch in arb_batch(1), mutation in arb_mutation()) {
        let (text, reason) = mutate(&batch, &mutation);
        match parse_batch(&text) {
            Ok(parsed) => prop_assert!(false, "accepted {text}: {parsed:?}"),
            Err(error) => prop_assert_eq!(error.reason(), reason, "{} ({:?}): {}", text, mutation, error),
        }
    }

    #[test]
    fn accepted_mutations_are_consistent(batch in arb_batch(0), cut in any::<usize>(), insert in "[\\[\\]{}\",:\\\\a-z0-9]{0,3}", drop in 0_usize..3) {
        // Arbitrary local edits: whatever is accepted is JSON text, and its
        // ops re-render to a message that parses to the same ops.
        let text = batch.render();
        let mut chars: Vec<char> = text.chars().collect();
        let at = cut % (chars.len() + 1);
        let end = (at + drop).min(chars.len());
        chars.splice(at..end, insert.chars());
        let edited: String = chars.into_iter().collect();
        if let Ok(parsed) = parse_batch(&edited) {
            prop_assert!(serde_json::from_str::<&RawValue>(&edited).is_ok());
            let model = ModelBatch {
                ops: parsed.ops.iter().map(to_model).collect(),
                extra_before: Vec::new(),
                extra_after: Vec::new(),
                ops_name: "\"ops\"".into(),
                escape_codes: Vec::new(),
            };
            let rendered = model.render();
            let again = parse_batch(&rendered).map_err(|e| TestCaseError::fail(e.to_string()))?;
            prop_assert_eq!(again.ops, parsed.ops);
        }
    }

    #[test]
    fn no_panic_on_arbitrary_text(text in any::<String>()) {
        let _result = parse_batch(&text);
    }

    #[test]
    fn no_panic_on_json_like_text(text in "[\\[\\]{}\",:\\\\uopdx0-9A-Za-z_=+/ -]{0,40}") {
        let result = parse_batch(&text);
        if result.is_ok() {
            prop_assert!(serde_json::from_str::<&RawValue>(&text).is_ok());
        }
    }

    #[test]
    fn base64url_round_trip(key in prop::collection::vec(any::<u8>(), 1..=MAX_KEY_OCTETS)) {
        let text = encode_key(&key);
        prop_assert!(!text.contains('='));
        prop_assert_eq!(decode_key(&text), Ok(key));
    }

    #[test]
    fn base64url_accepts_only_canonical(text in "[A-Za-z0-9_+/=-]{0,9}") {
        let accepted = decode_key(&text);
        prop_assert_eq!(accepted.is_ok(), canonical_by_rule(&text), "{}", text);
        if let Ok(key) = accepted {
            prop_assert_eq!(encode_key(&key), text);
        }
    }

    #[test]
    fn fold_matches_naive_model(
        log in prop::collection::vec(prop::collection::vec(small_op(), 0..6), 0..10),
        queries in prop::collection::vec(arb_query(), 1..6),
    ) {
        let texts = render_log(&log);
        for d in 0..=log.len() {
            let state = KeyedState::fold(texts[..d].iter().map(String::as_str))
                .map_err(|e| TestCaseError::fail(e.to_string()))?;
            let want = naive_state(&log[..d]);
            let got: Vec<(Vec<u8>, NaiveRow)> = state
                .rows()
                .iter()
                .map(|(k, row)| (k.clone(), (row.record, row.value.get().to_owned())))
                .collect();
            prop_assert_eq!(&got, &want, "D={}", d);
            for query in &queries {
                let page = state.range(query);
                let keys: Vec<Vec<u8>> = page.rows.iter().map(|(k, _)| k.to_vec()).collect();
                let lines: usize = page.rows.iter().map(|(k, row)| row_line(k, row).len()).sum();
                prop_assert_eq!(lines, page.body().len());
                prop_assert_eq!((keys, page.after.clone()), naive_read(&want, query), "{:?}", query);
            }
        }
    }
}
