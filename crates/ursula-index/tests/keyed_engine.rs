//! Engine model test (design I9, I11, I12): random `keyed-batch-v1` logs
//! folded through random flush, compaction and merge-into-oldest choices,
//! compared with the reference fold at every published `D` — full scans,
//! random range reads, `after` pagination under small limits and byte
//! budgets, and point reads.

#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::unwrap_used,
    clippy::panic,
    reason = "test helpers outside #[test] functions index and do arithmetic on generated data"
)]

use proptest::prelude::*;
use ursula_index::FsObjectStore;
use ursula_index::ObjectStore;
use ursula_index::keyed::BuiltRun;
use ursula_index::keyed::CompactionPolicy;
use ursula_index::keyed::KeyedManifest;
use ursula_index::keyed::KeyedNamespace;
use ursula_index::keyed::KeyedPage;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::MemoryParts;
use ursula_index::keyed::PartOpener;
use ursula_index::keyed::PartOptions;
use ursula_index::keyed::PublishOutcome;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::RunBuilder;
use ursula_index::keyed::compact;
use ursula_index::keyed::encode_key;
use ursula_index::keyed::get;
use ursula_index::keyed::plan_compaction;
use ursula_index::keyed::read_range;
use ursula_index::keyed::record_digest;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, &'static str),
    Delete(Vec<u8>),
    Range(Vec<u8>, Vec<u8>),
}

/// After each record: flush the open run, and then which compaction.
#[derive(Debug, Clone, Copy)]
enum Step {
    Keep,
    Flush,
    FlushCompactPolicy,
    /// Flush, then merge the newest `n` runs (when that many exist).
    FlushCompactNewest(usize),
    /// Flush, then merge everything into the oldest run.
    FlushCompactAll,
}

const VALUES: &[&str] = &[
    "null",
    "0",
    "true",
    "\"s\"",
    "\"\\ud800\"",
    "{\"b\":1,\"a\":[1,2]}",
    "[]",
    "\"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\"",
];

fn key() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(0_u8..5, 1..3)
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (key(), 0..VALUES.len()).prop_map(|(k, v)| Op::Put(k, VALUES[v])),
        2 => key().prop_map(Op::Delete),
        1 => (key(), key()).prop_filter_map("empty range", |(a, b)| {
            match a.cmp(&b) {
                std::cmp::Ordering::Less => Some(Op::Range(a, b)),
                std::cmp::Ordering::Greater => Some(Op::Range(b, a)),
                std::cmp::Ordering::Equal => None,
            }
        }),
    ]
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => Just(Step::Keep),
        3 => Just(Step::Flush),
        2 => Just(Step::FlushCompactPolicy),
        2 => (2_usize..4).prop_map(Step::FlushCompactNewest),
        1 => Just(Step::FlushCompactAll),
    ]
}

fn render(ops: &[Op]) -> String {
    let ops: Vec<String> = ops
        .iter()
        .map(|op| match op {
            Op::Put(k, v) => format!("[\"p\",\"{}\",{v}]", encode_key(k)),
            Op::Delete(k) => format!("[\"d\",\"{}\"]", encode_key(k)),
            Op::Range(s, e) => format!("[\"x\",\"{}\",\"{}\"]", encode_key(s), encode_key(e)),
        })
        .collect();
    format!("{{\"ops\":[{}]}}", ops.join(","))
}

fn tiny_parts() -> PartOptions {
    PartOptions {
        data_page_rows: 2,
        row_group_rows: 5,
        layout_block_bytes: 97,
        target_part_bytes: 60,
        read_batch_rows: 3,
    }
}

#[derive(Debug, Clone)]
struct Query {
    lower: u8,
    lower_key: Vec<u8>,
    end: Option<Vec<u8>>,
    limit: usize,
    budget: Option<usize>,
}

fn query() -> impl Strategy<Value = Query> {
    (
        0_u8..3,
        key(),
        prop::option::of(key()),
        1_usize..6,
        prop::option::of(1_usize..200),
    )
        .prop_map(|(lower, lower_key, end, limit, budget)| Query {
            lower,
            lower_key,
            end,
            limit,
            budget,
        })
}

impl Query {
    fn range_query(&self) -> RangeQuery {
        RangeQuery {
            lower: match self.lower {
                0 => Lower::First,
                1 => Lower::Start(self.lower_key.clone()),
                _ => Lower::After(self.lower_key.clone()),
            },
            end: self.end.clone(),
            limit: self.limit,
            budget: self.budget,
        }
    }
}

type Flat = Vec<(Vec<u8>, u64, String)>;

fn flat_model(state: &KeyedState, query: &RangeQuery) -> (Flat, Option<Vec<u8>>, String) {
    let page = state.range(query);
    (
        page.rows
            .iter()
            .map(|(k, row)| (k.to_vec(), row.record, row.value.get().to_owned()))
            .collect(),
        page.after.clone(),
        page.body(),
    )
}

fn flat_engine(page: &KeyedPage) -> (Flat, Option<Vec<u8>>, String) {
    (
        page.rows
            .iter()
            .map(|row| (row.key.clone(), row.record, row.value.clone()))
            .collect(),
        page.after.clone(),
        page.body(),
    )
}

/// Where parts live and how manifests are published.
trait Backend {
    fn opener(&self) -> &dyn PartOpener;
    async fn store(&mut self, run: &BuiltRun);
    async fn publish(&mut self, manifest: &KeyedManifest) -> KeyedManifest;
}

struct Memory {
    parts: MemoryParts,
}

impl Backend for Memory {
    fn opener(&self) -> &dyn PartOpener {
        &self.parts
    }

    async fn store(&mut self, run: &BuiltRun) {
        for part in &run.parts {
            self.parts.insert(part);
        }
    }

    async fn publish(&mut self, manifest: &KeyedManifest) -> KeyedManifest {
        manifest.validate().unwrap();
        // Simulated GC: objects a manifest obsoletes must never be read again.
        for key in &manifest.obsoleted {
            self.parts.remove(key);
        }
        manifest.clone()
    }
}

struct Store {
    namespace: KeyedNamespace,
    opener: ursula_index::keyed::StorePartOpener,
    _dir: tempfile::TempDir,
}

impl Backend for Store {
    fn opener(&self) -> &dyn PartOpener {
        &self.opener
    }

    async fn store(&mut self, run: &BuiltRun) {
        for part in &run.parts {
            self.namespace.put_part(part).await.unwrap();
        }
    }

    async fn publish(&mut self, manifest: &KeyedManifest) -> KeyedManifest {
        let base = self.namespace.load().await.unwrap();
        let PublishOutcome::Published(published) = self
            .namespace
            .publish(base.as_ref(), manifest)
            .await
            .unwrap()
        else {
            panic!("single writer conflicted");
        };
        for key in &published.manifest.obsoleted {
            self.namespace.delete(key).await.unwrap();
        }
        let loaded = self.namespace.load().await.unwrap().unwrap();
        assert_eq!(loaded.manifest, published.manifest);
        loaded.manifest
    }
}

fn source() -> KeyedSource {
    KeyedSource {
        bucket: "bucket".to_owned(),
        key: "aff/stream".to_owned(),
        incarnation: 7,
    }
}

async fn check(
    backend: &dyn PartOpener,
    manifest: &KeyedManifest,
    messages: &[String],
    queries: &[Query],
    options: &PartOptions,
) {
    let through = manifest.through_record as usize;
    let state = KeyedState::fold(messages[..through].iter().map(String::as_str)).unwrap();
    let runs = &manifest.runs;

    // Full scan.
    let full = RangeQuery {
        limit: usize::MAX,
        budget: None,
        ..RangeQuery::default()
    };
    let engine = read_range(backend, runs, &full, options).await.unwrap();
    assert_eq!(
        flat_engine(&engine),
        flat_model(&state, &full),
        "full scan at D={through}"
    );

    // Random range reads.
    for query in queries {
        let query = query.range_query();
        let engine = read_range(backend, runs, &query, options).await.unwrap();
        assert_eq!(
            flat_engine(&engine),
            flat_model(&state, &query),
            "range {query:?} at D={through}"
        );
    }

    // Pagination with `after` under a small limit and a tiny budget visits
    // every row exactly once.
    for (limit, budget) in [(2, None), (100, Some(1))] {
        let mut lower = Lower::First;
        let mut seen = Vec::new();
        loop {
            let page = read_range(
                backend,
                runs,
                &RangeQuery {
                    lower: lower.clone(),
                    end: None,
                    limit,
                    budget,
                },
                options,
            )
            .await
            .unwrap();
            seen.extend(page.rows.iter().map(|row| row.key.clone()));
            match page.after {
                Some(after) => lower = Lower::After(after),
                None => break,
            }
        }
        let expected: Vec<Vec<u8>> = state.rows().keys().cloned().collect();
        assert_eq!(seen, expected, "pagination at D={through}");
    }

    // Point reads, present and absent.
    for key in state
        .rows()
        .keys()
        .take(3)
        .cloned()
        .chain([vec![4, 4], vec![0]])
    {
        let engine = get(backend, runs, &key, options).await.unwrap();
        let model = state.get(&key);
        assert_eq!(
            engine.map(|row| (row.record, row.value)),
            model.map(|row| (row.record, row.value.get().to_owned())),
            "point read at D={through}"
        );
    }
}

async fn run_case<B: Backend>(
    backend: &mut B,
    log: &[(Vec<Op>, Step)],
    queries: &[Query],
    policy: &CompactionPolicy,
) {
    let options = tiny_parts();
    let messages: Vec<String> = log.iter().map(|(ops, _)| render(ops)).collect();
    let mut manifest = KeyedManifest::empty(source());
    let mut builder: Option<RunBuilder> = None;
    let mut now = 0_u64;
    for (record, ((_, step), message)) in log.iter().zip(&messages).enumerate() {
        let record = record as u64;
        let open = builder.get_or_insert_with(|| RunBuilder::new(record, manifest.runs.is_empty()));
        open.apply_message(record, message).unwrap();
        let last = record + 1 == log.len() as u64;
        if matches!(step, Step::Keep) && !last {
            continue;
        }
        let run = builder.take().unwrap().finish(&options).unwrap();
        backend.store(&run).await;
        now += 1;
        let next = manifest
            .after_ingest(
                Some(run.meta.clone()),
                record + 1,
                record_digest(message.as_bytes()),
                now,
            )
            .unwrap();
        manifest = backend.publish(&next).await;
        check(backend.opener(), &manifest, &messages, queries, &options).await;

        let plan = match step {
            Step::Keep | Step::Flush => None,
            Step::FlushCompactPolicy => plan_compaction(&manifest.runs, policy),
            Step::FlushCompactNewest(n) => {
                let count = manifest.runs.len();
                (count >= *n).then(|| count - n..count)
            }
            Step::FlushCompactAll => (manifest.runs.len() >= 2).then_some(0..manifest.runs.len()),
        };
        let Some(plan) = plan else {
            continue;
        };
        let output = compact(backend.opener(), &manifest.runs, plan, &options)
            .await
            .unwrap();
        backend.store(&output.output).await;
        now += 1;
        let next = manifest
            .after_compaction(&output.inputs, &output.output.meta, output.into_oldest, now)
            .unwrap()
            .expect("inputs are present");
        if output.into_oldest {
            for part in next.runs.first().into_iter().flat_map(|run| &run.parts) {
                assert!(
                    part.tombstones == 0,
                    "a merge into the oldest run keeps no tombstones"
                );
            }
        }
        manifest = backend.publish(&next).await;
        check(backend.opener(), &manifest, &messages, queries, &options).await;
    }
}

fn log() -> impl Strategy<Value = Vec<(Vec<Op>, Step)>> {
    prop::collection::vec((prop::collection::vec(op(), 0..6), step()), 1..30)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn engine_matches_fold_in_memory(
        log in log(),
        queries in prop::collection::vec(query(), 1..6),
        width in 2_usize..5,
    ) {
        let policy = CompactionPolicy { width, ..CompactionPolicy::default() };
        let mut backend = Memory { parts: MemoryParts::new() };
        runtime().block_on(run_case(&mut backend, &log, &queries, &policy));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn engine_matches_fold_through_the_object_store(
        log in log(),
        queries in prop::collection::vec(query(), 1..3),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let store = ObjectStore::from(FsObjectStore::new(dir.path()).unwrap());
        let namespace = KeyedNamespace::new(store, source());
        let opener = namespace.opener();
        let mut backend = Store { namespace, opener, _dir: dir };
        runtime().block_on(run_case(&mut backend, &log, &queries, &CompactionPolicy::default()));
    }
}

/// Deterministic regression: a tombstone-only run between two runs hides an
/// older row, survives a newer-runs compaction, and is dropped only by the
/// merge into the oldest run.
#[test]
fn tombstone_only_run_shadows_until_merged_into_oldest() {
    runtime().block_on(async {
        let options = tiny_parts();
        let messages = [
            r#"{"ops":[["p","AQ",1],["p","Ag",2]]}"#.to_owned(),
            r#"{"ops":[["x","AA","Ag"]]}"#.to_owned(),
            r#"{"ops":[["p","Aw",3]]}"#.to_owned(),
        ];
        let mut parts = MemoryParts::new();
        let mut manifest = KeyedManifest::empty(source());
        for (record, message) in messages.iter().enumerate() {
            let record = record as u64;
            let mut builder = RunBuilder::new(record, manifest.runs.is_empty());
            builder.apply_message(record, message).unwrap();
            let run = builder.finish(&options).unwrap();
            for part in &run.parts {
                parts.insert(part);
            }
            manifest = manifest
                .after_ingest(
                    Some(run.meta),
                    record + 1,
                    record_digest(message.as_bytes()),
                    1,
                )
                .unwrap();
        }
        assert_eq!(manifest.runs.len(), 3);
        let middle = &manifest.runs[1];
        assert_eq!(
            (
                middle.parts.len(),
                middle.parts[0].rows,
                middle.parts[0].tombstones
            ),
            (1, 0, 1)
        );
        check(&parts, &manifest, &messages, &[], &options).await;

        let output = compact(&parts, &manifest.runs, 1..3, &options)
            .await
            .unwrap();
        assert!(!output.into_oldest);
        assert!(
            output
                .output
                .meta
                .parts
                .iter()
                .any(|part| part.tombstones > 0)
        );
        for part in &output.output.parts {
            parts.insert(part);
        }
        manifest = manifest
            .after_compaction(&output.inputs, &output.output.meta, false, 2)
            .unwrap()
            .unwrap();
        check(&parts, &manifest, &messages, &[], &options).await;

        let output = compact(&parts, &manifest.runs, 0..2, &options)
            .await
            .unwrap();
        assert!(output.into_oldest);
        assert!(
            output
                .output
                .meta
                .parts
                .iter()
                .all(|part| part.tombstones == 0)
        );
        for part in &output.output.parts {
            parts.insert(part);
        }
        manifest = manifest
            .after_compaction(&output.inputs, &output.output.meta, true, 3)
            .unwrap()
            .unwrap();
        assert_eq!(manifest.runs.len(), 1);
        check(&parts, &manifest, &messages, &[], &options).await;
    });
}
