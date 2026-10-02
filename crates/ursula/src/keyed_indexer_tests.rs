//! The keyed engine against a real single-node Ursula (design §3.4, §11.4):
//! the indexer reads a keyed stream through P7 pages, HEAD and the bucket
//! listing of an in-process node, and its keyed state equals the reference
//! fold of the node's stored records.

use std::time::Duration;

use ursula_index::FsObjectStore;
use ursula_index::ObjectStore;
use ursula_index::keyed::KeyedEngine;
use ursula_index::keyed::KeyedEngineConfig;
use ursula_index::keyed::KeyedReadOutcome;
use ursula_index::keyed::KeyedReadRequest;
use ursula_index::keyed::KeyedSource;
use ursula_index::keyed::KeyedSourceClient;
use ursula_index::keyed::KeyedState;
use ursula_index::keyed::Lower;
use ursula_index::keyed::RangeQuery;
use ursula_index::keyed::Selection;
use ursula_index::keyed::encode_key;

use super::*;

const KEYED_CT: &str = "application/json; profile=keyed-batch-v1";

async fn serve_node() -> String {
    let app = router(
        spawn_runtime(
            &{
                let mut config = ursula_config::UrsulaConfig::default();
                config.runtime.core_count = 2;
                config.raft.group_count = 4;
                config
            },
            Persistence::InMemory,
            Topology::SingleNode {
                raft_group_count: 4,
            },
        )
        .expect("runtime")
        .runtime,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("address");
    tokio::spawn(axum::serve(listener, app).into_future());
    format!("http://{address}")
}

fn record(index: u64) -> String {
    let key = encode_key(format!("k{}", index % 7).as_bytes());
    let gone = encode_key(format!("k{}", (index + 3) % 7).as_bytes());
    let low = encode_key(b"k0");
    let high = encode_key(b"k2");
    match index % 5 {
        3 => format!(r#"{{"ops":[["d","{gone}"],["p","{key}",{{"i":{index},"n":1.50e3}}]]}}"#),
        4 => format!(r#"{{"ops":[["x","{low}","{high}"]],"note":"\ud800"}}"#),
        _ => format!(r#"{{ "ops" : [ ["p","{key}",[{index}, "v"]] ] }}"#),
    }
}

async fn tail(client: &reqwest::Client, stream_url: &str) -> u64 {
    let response = client.head(stream_url).send().await.expect("head");
    response.headers()["stream-record-next"]
        .to_str()
        .expect("header")
        .parse()
        .expect("tail")
}

async fn stored_records(client: &reqwest::Client, stream_url: &str) -> Vec<String> {
    let body = client
        .get(format!("{stream_url}?record=0"))
        .send()
        .await
        .expect("read")
        .text()
        .await
        .expect("body");
    body.lines().map(str::to_owned).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_engine_folds_a_real_node_log() {
    let base = serve_node().await;
    let client = reqwest::Client::new();
    let raised = client
        .post(format!("{base}/__ursula/feature-level"))
        .header("content-type", "application/json")
        .body(r#"{"level":1}"#)
        .send()
        .await
        .expect("feature level");
    assert!(raised.status().is_success(), "{}", raised.status());
    let stream_url = format!("{base}/kbkt/aff/s1");
    let created = client
        .put(&stream_url)
        .header("content-type", KEYED_CT)
        .send()
        .await
        .expect("create");
    assert!(created.status().is_success(), "{}", created.status());
    let listing = client
        .get(format!("{base}/kbkt/streams?prefix=aff/s1"))
        .send()
        .await
        .expect("listing")
        .bytes()
        .await
        .expect("listing body");
    let listing: serde_json::Value = serde_json::from_slice(&listing).expect("listing json");
    let incarnation = listing["streams"][0]["created_at_ms"]
        .as_u64()
        .expect("created_at_ms");

    let dir = tempfile::tempdir().expect("tempdir");
    let engine = KeyedEngine::new(
        ObjectStore::from(FsObjectStore::new(dir.path()).expect("store")),
        KeyedSourceClient::new(url::Url::parse(&base).expect("base")).expect("source"),
        None,
        KeyedEngineConfig {
            min_publish_interval: Duration::ZERO,
            source_page_bytes: 512,
            ..KeyedEngineConfig::default()
        },
    );
    let source = KeyedSource {
        bucket: "kbkt".to_owned(),
        key: "aff/s1".to_owned(),
        incarnation,
    };
    let mut appended = 0_u64;
    for round in 0..3 {
        for _ in 0..12 {
            let response = client
                .post(&stream_url)
                .header("content-type", KEYED_CT)
                .body(record(appended))
                .send()
                .await
                .expect("append");
            assert!(response.status().is_success(), "{}", response.status());
            appended += 1;
        }
        let n = tail(&client, &stream_url).await;
        assert_eq!(n, appended);
        let outcome = engine
            .read(KeyedReadRequest {
                source: source.clone(),
                source_next: n,
                selection: Selection::Range(RangeQuery {
                    lower: Lower::First,
                    end: None,
                    limit: 1000,
                    budget: None,
                }),
                min_through_record: Some(n),
                timeout: Duration::from_secs(10),
            })
            .await;
        let KeyedReadOutcome::Rows { through, page } = outcome else {
            panic!("round {round}: {outcome:?}");
        };
        assert_eq!(through, n);
        let records = stored_records(&client, &stream_url).await;
        assert_eq!(records.len() as u64, n);
        let state = KeyedState::fold(records.iter().map(String::as_str)).expect("fold");
        assert_eq!(
            page.body(),
            state
                .range(&RangeQuery {
                    lower: Lower::First,
                    end: None,
                    limit: 1000,
                    budget: None,
                })
                .body()
        );
        // The fold input is the node's stored text (P1): literal number
        // text and lone-surrogate escapes survive.
        assert!(records.iter().any(|text| text.contains("1.50e3")));
        assert!(records.iter().any(|text| text.contains("\\ud800")));
    }
    // The incarnation check found the stream: the namespace remains.
    assert!(
        dir.path()
            .join(".keyed/kbkt/aff%2Fs1")
            .join(format!("{incarnation:016x}"))
            .join("v1/CURRENT")
            .exists()
    );
}
