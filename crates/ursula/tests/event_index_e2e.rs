//! End to end: the experimental event-time indexer pool against an
//! in-process Ursula, over HTTP only. It lives here rather than in
//! `ursula-index` because an `ursula` dev-dependency of `ursula-index` would
//! form a cycle.
//!
//! The test reads offsets only as the opaque tokens Ursula and the indexer
//! mint, except where it computes the expected end of complete NDJSON lines.

#![expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]

use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use serde_json::Value;
use serde_json::json;
use tokio::time::Instant;
use ursula_index::service::IndexerArgs;
use ursula_runtime::RuntimeConfig;
use ursula_runtime::ShardRuntime;

#[derive(Parser)]
struct Cli {
    #[command(flatten)]
    args: IndexerArgs,
}

const SPAN_LATE: &str = r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"startTimeUnixNano":"1759482001000000000","endTimeUnixNano":"1759482004000000000"}]}]}]}"#;
const NO_SPANS: &str = r#"{"resourceSpans":[]}"#;
const SPAN_EARLY: &str = r#"{"resourceSpans":[{"scopeSpans":[{"spans":[{"startTimeUnixNano":"1759482000000000000","endTimeUnixNano":"1759482000500000000"}]}]}]}"#;

const SESSION_TEN: &str = "{\"entry\":{\"timestamp\":\"2026-10-03T10:00:00Z\",\"id\":\"a\"}}\n";
const SESSION_NINE: &str = "{\"entry\":{\"timestamp\":\"2026-10-03T09:00:00Z\",\"id\":\"b\"}}\n";
const SESSION_PARTIAL: &str = "{\"entry\":{\"timestamp\":\"2026-10-03T11:00:00Z\",\"id\":\"c\"}}";

struct Http {
    client: reqwest::Client,
}

impl Http {
    async fn send(
        &self,
        method: reqwest::Method,
        url: &str,
        content_type: Option<&str>,
        body: &str,
    ) -> anyhow::Result<reqwest::Response> {
        let mut request = self.client.request(method, url).body(body.to_owned());
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        Ok(request.send().await?)
    }

    async fn json(&self, url: &str) -> anyhow::Result<Value> {
        let response = self.client.get(url).send().await?;
        anyhow::ensure!(
            response.status().is_success(),
            "GET {url}: {}",
            response.status()
        );
        Ok(serde_json::from_slice(&response.bytes().await?)?)
    }

    async fn tail(&self, stream_url: &str) -> anyhow::Result<String> {
        let response = self.client.head(stream_url).send().await?;
        header(&response, "stream-next-offset")
    }

    async fn incarnation(&self, stream_url: &str) -> anyhow::Result<String> {
        let response = self.client.head(stream_url).send().await?;
        header(&response, "stream-incarnation")
    }

    /// Fetch one locator: read from its offset with `max_bytes`, continuing
    /// from `Stream-Next-Offset` until `len` bytes arrived.
    async fn fetch(&self, stream_url: &str, entry: &Value) -> anyhow::Result<String> {
        let mut next = entry["offset"].as_str().context("entry offset")?.to_owned();
        let len = usize::try_from(entry["len"].as_u64().context("entry len")?)?;
        let mut bytes = Vec::new();
        while bytes.len() < len {
            let remaining = len.saturating_sub(bytes.len());
            let response = self
                .client
                .get(format!("{stream_url}?offset={next}&max_bytes={remaining}"))
                .send()
                .await?;
            anyhow::ensure!(response.status().is_success(), "locator fetch failed");
            next = header(&response, "stream-next-offset")?;
            let body = response.bytes().await?;
            anyhow::ensure!(!body.is_empty(), "locator fetch made no progress");
            bytes.extend_from_slice(&body);
        }
        Ok(String::from_utf8(bytes)?)
    }

    /// Poll an index's status until `done` holds.
    async fn wait_status(
        &self,
        indexer: &str,
        id: &str,
        done: impl Fn(&Value) -> bool,
    ) -> anyhow::Result<Value> {
        let deadline = Instant::now()
            .checked_add(Duration::from_secs(60))
            .context("deadline")?;
        let url = format!("{indexer}/v1/indexes/{id}/status");
        loop {
            let status = self.json(&url).await.unwrap_or(Value::Null);
            if done(&status) {
                return Ok(status);
            }
            anyhow::ensure!(
                Instant::now() < deadline,
                "timed out waiting for index {id}; last status {status}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn header(response: &reqwest::Response, name: &str) -> anyhow::Result<String> {
    Ok(response
        .headers()
        .get(name)
        .with_context(|| format!("missing {name}"))?
        .to_str()?
        .to_owned())
}

fn offset_value(value: &Value) -> Option<u64> {
    value.as_str()?.parse().ok()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexer_pool_indexes_otlp_json_and_ndjson_streams_from_an_in_process_ursula()
-> anyhow::Result<()> {
    let runtime = ShardRuntime::spawn(RuntimeConfig::new(1, 1)).expect("spawn runtime");
    let ursula_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let ursula = format!("http://{}", ursula_listener.local_addr()?);
    let ursula_server = tokio::spawn(async move {
        axum::serve(ursula_listener, ursula::router(runtime))
            .await
            .expect("ursula serves");
    });

    let objects = tempfile::TempDir::new()?;
    let cache = tempfile::TempDir::new()?;
    let indexer_address = std::net::TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let indexer = format!("http://{indexer_address}");
    let cli = Cli::try_parse_from([
        "indexer",
        "--object-dir",
        objects.path().to_str().context("object dir")?,
        "--cache-dir",
        cache.path().to_str().context("cache dir")?,
        "--listen",
        &indexer_address.to_string(),
        "--poll-interval-ms",
        "20",
        "--tail-flush-interval-ms",
        "0",
        "--maintenance-interval-ms",
        "50",
        "--flush-entries",
        "100",
    ])?;
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let indexer_task = tokio::spawn(ursula_index::service::run_until(cli.args, async move {
        match stopped.await {
            Ok(()) | Err(_) => {}
        }
    }));
    let http = Http {
        client: reqwest::Client::new(),
    };
    let ready = Instant::now()
        .checked_add(Duration::from_secs(30))
        .context("deadline")?;
    while !http
        .client
        .get(format!("{indexer}/readyz"))
        .send()
        .await
        .is_ok_and(|response| response.status().is_success())
    {
        anyhow::ensure!(Instant::now() < ready, "indexer never became ready");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // An OTLP-JSON trace stream: one collector batch per message.
    let traces = format!("{ursula}/otel/traces-checkout");
    let response = http
        .send(reqwest::Method::PUT, &traces, Some("application/json"), "")
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let response = http
        .send(
            reqwest::Method::POST,
            &traces,
            Some("application/json"),
            &format!("[{SPAN_LATE},{NO_SPANS},{SPAN_EARLY}]"),
        )
        .await?;
    assert!(response.status().is_success());
    let registration = json!({
        "stream_url": traces,
        "extract": {
            "each": "/resourceSpans/*/scopeSpans/*/spans/*",
            "time": ["/startTimeUnixNano"],
            "end": ["/endTimeUnixNano"],
            "unit": "ns"
        }
    });
    let response = http
        .send(
            reqwest::Method::PUT,
            &format!("{indexer}/v1/indexes/traces"),
            Some("application/json"),
            &registration.to_string(),
        )
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let tail = http.tail(&traces).await?;
    let status = http
        .wait_status(&indexer, "traces", |status| {
            status["coverage"]["durable"].as_str() == Some(tail.as_str())
        })
        .await?;
    assert_eq!(status["skipped"]["missing"], 1);
    assert_eq!(status["coverage"]["complete"], true);

    let events = http
        .json(&format!(
            "{indexer}/v1/indexes/traces/events?from=1759482000000&until=1759482010000"
        ))
        .await?;
    let entries = events["entries"].as_array().context("entries")?;
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["t_ms"], 1_759_482_000_000_i64);
    assert_eq!(entries[0]["t_end_ms"], 1_759_482_000_500_i64);
    assert_eq!(entries[1]["t_ms"], 1_759_482_001_000_i64);
    assert_eq!(entries[1]["t_end_ms"], 1_759_482_004_000_i64);
    assert_eq!(
        http.fetch(&traces, &entries[0]).await?,
        format!("{SPAN_EARLY}\n")
    );
    assert_eq!(
        http.fetch(&traces, &entries[1]).await?,
        format!("{SPAN_LATE}\n")
    );
    let late_window =
        format!("{indexer}/v1/indexes/traces/events?from=1759482002000&until=1759482010000");
    assert_eq!(
        http.json(&late_window).await?["entries"]
            .as_array()
            .map(Vec::len),
        Some(0)
    );
    assert_eq!(
        http.json(&format!("{late_window}&match=overlap")).await?["entries"]
            .as_array()
            .map(Vec::len),
        Some(1)
    );

    // Retention up to the early span hides what came before it.
    let at = entries[0]["offset"].as_str().context("offset")?.to_owned();
    let response = http
        .send(
            reqwest::Method::PUT,
            &format!("{traces}/snapshot/{at}"),
            None,
            "state",
        )
        .await?;
    assert!(response.status().is_success(), "{}", response.status());
    let response = http
        .send(
            reqwest::Method::PUT,
            &format!("{traces}/retention/{at}"),
            None,
            "",
        )
        .await?;
    assert!(response.status().is_success(), "{}", response.status());
    http.wait_status(&indexer, "traces", |status| {
        status["coverage"]["floor"].as_str() == Some(at.as_str())
    })
    .await?;
    let events = http
        .json(&format!(
            "{indexer}/v1/indexes/traces/events?from=0&until=2000000000000"
        ))
        .await?;
    assert_eq!(events["entries"].as_array().map(Vec::len), Some(1));
    assert_eq!(events["coverage"]["complete"], true);

    // An NDJSON session stream with a missing time, a non-JSON line and an
    // unterminated last line.
    let sessions = format!("{ursula}/sessions/session-1");
    let response = http
        .send(
            reqwest::Method::PUT,
            &sessions,
            Some("application/x-ndjson"),
            "",
        )
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let body =
        format!("{SESSION_TEN}{{\"type\":\"summary\"}}\nnot json\n{SESSION_NINE}{SESSION_PARTIAL}");
    let response = http
        .send(
            reqwest::Method::POST,
            &sessions,
            Some("application/x-ndjson"),
            &body,
        )
        .await?;
    assert!(response.status().is_success());
    let response = http
        .send(
            reqwest::Method::PUT,
            &format!("{indexer}/v1/indexes/session-1"),
            Some("application/json"),
            &json!({"stream_url": sessions, "extract": {"time": ["/entry/timestamp"]}}).to_string(),
        )
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let tail = http
        .tail(&sessions)
        .await?
        .parse::<u64>()
        .context("tail offset")?;
    let complete_lines = tail
        .checked_sub(u64::try_from(SESSION_PARTIAL.len())?)
        .context("partial line")?;
    let status = http
        .wait_status(&indexer, "session-1", |status| {
            offset_value(&status["coverage"]["durable"]) == Some(complete_lines)
        })
        .await?;
    assert_eq!(status["skipped"]["missing"], 1);
    assert_eq!(status["skipped"]["unparseable"], 1);
    let events = http
        .json(&format!(
            "{indexer}/v1/indexes/session-1/events?from=2026-10-03T00:00:00Z&until=2026-10-04T00:00:00Z"
        ))
        .await?;
    let entries = events["entries"].as_array().context("entries")?;
    assert_eq!(entries.len(), 2);
    assert_eq!(http.fetch(&sessions, &entries[0]).await?, SESSION_NINE);
    assert_eq!(http.fetch(&sessions, &entries[1]).await?, SESSION_TEN);

    // Terminating the last line makes it a message.
    let response = http
        .send(
            reqwest::Method::POST,
            &sessions,
            Some("application/x-ndjson"),
            "\n",
        )
        .await?;
    assert!(response.status().is_success());
    let tail = http.tail(&sessions).await?;
    let status = http
        .wait_status(&indexer, "session-1", |status| {
            status["coverage"]["durable"].as_str() == Some(tail.as_str())
        })
        .await?;
    assert_eq!(status["skipped"]["unparseable"], 1);
    assert_eq!(status["skipped"]["oversize"], 0);
    let events = http
        .json(&format!(
            "{indexer}/v1/indexes/session-1/events?from=2026-10-03T00:00:00Z&until=2026-10-04T00:00:00Z"
        ))
        .await?;
    let entries = events["entries"].as_array().context("entries")?;
    assert_eq!(entries.len(), 3);
    assert_eq!(
        http.fetch(&sessions, &entries[2]).await?,
        format!("{SESSION_PARTIAL}\n")
    );

    // Delete and recreate: the registration restarts on the new stream.
    let previous = http.incarnation(&sessions).await?;
    let response = http
        .send(reqwest::Method::DELETE, &sessions, None, "")
        .await?;
    assert!(response.status().is_success());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let response = http
        .send(
            reqwest::Method::PUT,
            &sessions,
            Some("application/x-ndjson"),
            "{\"entry\":{\"timestamp\":\"2026-10-03T12:00:00Z\"}}\n",
        )
        .await?;
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    let incarnation = http.incarnation(&sessions).await?;
    assert_ne!(incarnation, previous);
    let tail = http.tail(&sessions).await?;
    let status = http
        .wait_status(&indexer, "session-1", |status| {
            status["source"]["incarnation"].as_str() == Some(incarnation.as_str())
                && status["coverage"]["durable"].as_str() == Some(tail.as_str())
        })
        .await?;
    assert_eq!(status["restarted_from_incarnation"], previous.as_str());
    let events = http
        .json(&format!(
            "{indexer}/v1/indexes/session-1/events?from=2026-10-03T00:00:00Z&until=2026-10-04T00:00:00Z"
        ))
        .await?;
    assert_eq!(events["entries"].as_array().map(Vec::len), Some(1));

    if stop.send(()).is_err() {
        anyhow::bail!("the indexer stopped early");
    }
    indexer_task.await??;
    ursula_server.abort();
    Ok(())
}
