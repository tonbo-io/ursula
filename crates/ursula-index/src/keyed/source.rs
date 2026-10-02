//! Source client of the keyed engine (design §6.1 U18, U21).
//!
//! The engine reads a keyed stream's log through the [`SourceClient`]
//! trait. [`KeyedSourceClient`] is the HTTP implementation: it reads from an
//! Ursula node or gateway in P7 pages, record-aware reads in the default
//! view (`application/x-ndjson`, one stored message per line) bounded by
//! `max_bytes`. Reads use the node's default (local) consistency, so a
//! rebuild may be served by followers; decisions that retire a namespace
//! confirm with a leader read. One HTTP client is shared by every namespace.
//! The simulator implements the trait over an in-process node and reuses
//! [`read_response`] to interpret its answers exactly as the HTTP client
//! does.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures_util::FutureExt;
use futures_util::future::BoxFuture;
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::header::HeaderMap;
use serde::Deserialize;

use super::batch::KEYED_BATCH_PROFILE;

/// Records requested per page when the node rejects `max_bytes` (an older
/// node, design §5.4 compatibility).
const FALLBACK_MAX_RECORDS: u64 = 256;

/// Failure of a source read, classified for the keyed-state status mapping.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// Temporarily unavailable (network, 5xx, 429, unexpected answers): 503.
    #[error("source unavailable: {0}")]
    Transient(String),
    /// The stream is absent (404).
    #[error("source stream is absent")]
    NotFound,
    /// Records below `first_record` were removed by retention (410).
    #[error("source records below {first_record} are no longer retained")]
    Retained {
        /// The first retained record.
        first_record: u64,
    },
    /// The requested record is beyond the source tail (400 with
    /// `Stream-Record-Next`).
    #[error("source tail is {next_record}")]
    BeyondTail {
        /// The source's record tail `N`.
        next_record: u64,
    },
    /// The source is not a `keyed-batch-v1` stream.
    #[error("source stream does not advertise keyed-batch-v1")]
    NotKeyed,
}

/// One page of stored records.
#[derive(Clone, Debug, Default)]
pub struct SourcePage {
    /// Ordinal of the first record of `records`.
    pub start_record: u64,
    /// Continuation: one past the last returned record.
    pub next_record: u64,
    /// Stored message text of each record, without its terminating LF.
    pub records: Vec<String>,
}

/// Whether a stream incarnation still exists at the source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncarnationState {
    /// The stream exists with this incarnation, or the answer is not
    /// conclusive.
    Present,
    /// The stream is absent, or exists with another incarnation.
    Gone,
}

/// The source log of keyed namespaces, as the engine reads it.
pub trait SourceClient: Send + Sync {
    /// Reads one page of records from `record`: at most `max_bytes` of
    /// stored bytes (at least one record when one exists), and at most
    /// `max_records` records when given. `leader` asks for a leader read.
    fn read<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
    ) -> BoxFuture<'a, Result<SourcePage, SourceError>>;

    /// Whether incarnation `incarnation` of the stream still exists.
    /// Inconclusive answers count as present.
    fn incarnation<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        incarnation: u64,
    ) -> BoxFuture<'a, Result<IncarnationState, SourceError>>;
}

impl<T: SourceClient + ?Sized> SourceClient for Arc<T> {
    fn read<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
    ) -> BoxFuture<'a, Result<SourcePage, SourceError>> {
        (**self).read(bucket, key, record, max_bytes, max_records, leader)
    }

    fn incarnation<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        incarnation: u64,
    ) -> BoxFuture<'a, Result<IncarnationState, SourceError>> {
        (**self).incarnation(bucket, key, incarnation)
    }
}

/// Interprets a node's answer to a record read from `record` (the request
/// carried `max_bytes`): the status mapping and page checks of
/// [`KeyedSourceClient`], for other transports.
pub fn read_response(
    record: u64,
    status: StatusCode,
    headers: &HeaderMap,
    body: &str,
) -> Result<SourcePage, SourceError> {
    read_status(record, false, status, headers)?;
    read_page(record, headers, body)
}

/// Maps a record read's status to its error, if any.
fn read_status(
    record: u64,
    fallback: bool,
    status: StatusCode,
    headers: &HeaderMap,
) -> Result<(), SourceError> {
    match status {
        StatusCode::NOT_FOUND => Err(SourceError::NotFound),
        StatusCode::GONE => Err(SourceError::Retained {
            first_record: header_u64(headers, "stream-record-first")
                .unwrap_or(record.saturating_add(1)),
        }),
        // A beyond-tail read names the tail; any other 400 on a
        // well-formed request is a node that rejects `max_bytes`.
        StatusCode::BAD_REQUEST => Err(match header_u64(headers, "stream-record-next") {
            Some(next_record) => SourceError::BeyondTail { next_record },
            None if !fallback => SourceError::Transient(MAX_BYTES_REJECTED.to_owned()),
            None => transient("source rejected a record read with 400"),
        }),
        status if !status.is_success() => Err(transient(format!("source returned HTTP {status}"))),
        _ if !advertises(headers, KEYED_BATCH_PROFILE) => Err(SourceError::NotKeyed),
        _ => Ok(()),
    }
}

/// Splits a successful record read's body into its records and checks them
/// against the response's record coordinates.
fn read_page(record: u64, headers: &HeaderMap, body: &str) -> Result<SourcePage, SourceError> {
    let start_record = header_u64(headers, "stream-record-start")
        .ok_or_else(|| transient("source read omitted Stream-Record-Start"))?;
    let next_record = header_u64(headers, "stream-record-next")
        .ok_or_else(|| transient("source read omitted Stream-Record-Next"))?;
    let records: Vec<String> = body
        .split('\n')
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect();
    let count = u64::try_from(records.len()).unwrap_or(u64::MAX);
    if start_record != record || start_record.checked_add(count) != Some(next_record) {
        return Err(transient(
            "source page does not match its record coordinates",
        ));
    }
    Ok(SourcePage {
        start_record,
        next_record,
        records,
    })
}

/// HTTP client of the source log.
#[derive(Clone, Debug)]
pub struct KeyedSourceClient {
    client: reqwest::Client,
    base: Url,
    max_bytes_rejected: Arc<AtomicBool>,
}

#[derive(Deserialize)]
struct Listing {
    #[serde(default)]
    streams: Vec<ListingEntry>,
}

#[derive(Deserialize)]
struct ListingEntry {
    stream_id: String,
    #[serde(default)]
    created_at_ms: Option<u64>,
}

fn transient(error: impl std::fmt::Display) -> SourceError {
    SourceError::Transient(error.to_string())
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

fn advertises(headers: &HeaderMap, token: &str) -> bool {
    headers
        .get_all("stream-extensions")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim() == token)
}

impl KeyedSourceClient {
    /// A client of the node or gateway at `base` (stream URLs are
    /// `{base}/{bucket}/{key}`).
    pub fn new(base: Url) -> Result<Self, SourceError> {
        if base.cannot_be_a_base() {
            return Err(transient("source base URL cannot carry a path"));
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::limited(4))
            .build()
            .map_err(transient)?;
        Ok(Self {
            client,
            base,
            max_bytes_rejected: Arc::new(AtomicBool::new(false)),
        })
    }

    /// The configured base URL.
    pub fn base(&self) -> &Url {
        &self.base
    }

    fn stream_url(&self, bucket: &str, key: &str) -> Result<Url, SourceError> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| transient("source base URL cannot carry a path"))?
            .pop_if_empty()
            .push(bucket)
            .extend(key.split('/'));
        Ok(url)
    }

    /// Reads one page of records from `record`: at most `max_bytes` of
    /// stored bytes (at least one record when one exists), and at most
    /// `max_records` records when given. `leader` asks for a leader read.
    pub async fn read(
        &self,
        bucket: &str,
        key: &str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
    ) -> Result<SourcePage, SourceError> {
        let fallback = self.max_bytes_rejected.load(Ordering::Relaxed);
        match self
            .read_once(
                bucket,
                key,
                record,
                max_bytes,
                max_records,
                leader,
                fallback,
            )
            .await
        {
            Err(SourceError::Transient(reason)) if reason == MAX_BYTES_REJECTED && !fallback => {
                tracing::warn!(
                    base = %self.base,
                    "source rejects max_bytes on record reads; falling back to max_records"
                );
                self.max_bytes_rejected.store(true, Ordering::Relaxed);
                self.read_once(bucket, key, record, max_bytes, max_records, leader, true)
                    .await
            }
            result => result,
        }
    }

    async fn read_once(
        &self,
        bucket: &str,
        key: &str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
        fallback: bool,
    ) -> Result<SourcePage, SourceError> {
        let mut url = self.stream_url(bucket, key)?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("record", &record.to_string());
            if fallback {
                let records = max_records.unwrap_or(FALLBACK_MAX_RECORDS);
                query.append_pair("max_records", &records.to_string());
            } else {
                query.append_pair("max_bytes", &max_bytes.max(1).to_string());
                if let Some(records) = max_records {
                    query.append_pair("max_records", &records.to_string());
                }
            }
            if leader {
                query.append_pair("consistency", "leader");
            }
        }
        let response = self.client.get(url).send().await.map_err(transient)?;
        read_status(record, fallback, response.status(), response.headers())?;
        let headers = response.headers().clone();
        let body = response.text().await.map_err(transient)?;
        read_page(record, &headers, &body)
    }

    /// The stream's record tail `N` (`Stream-Record-Next` of a HEAD).
    pub async fn tail(&self, bucket: &str, key: &str) -> Result<u64, SourceError> {
        let url = self.stream_url(bucket, key)?;
        let response = self.client.head(url).send().await.map_err(transient)?;
        match response.status() {
            StatusCode::NOT_FOUND => Err(SourceError::NotFound),
            status if !status.is_success() => {
                Err(transient(format!("source HEAD returned HTTP {status}")))
            }
            _ => header_u64(response.headers(), "stream-record-next")
                .ok_or_else(|| transient("source HEAD omitted Stream-Record-Next")),
        }
    }

    /// Whether incarnation `incarnation` of the stream still exists: a HEAD
    /// of the stream, then the bucket listing's `created_at_ms`. Absent or
    /// lagging listing entries are inconclusive and count as present: a
    /// listing entry older than `incarnation` comes from a replica that has
    /// not applied the recreate yet, and deleting the namespace on it would
    /// drop the current incarnation's projection.
    pub async fn incarnation(
        &self,
        bucket: &str,
        key: &str,
        incarnation: u64,
    ) -> Result<IncarnationState, SourceError> {
        let url = self.stream_url(bucket, key)?;
        let response = self.client.head(url).send().await.map_err(transient)?;
        match response.status() {
            StatusCode::NOT_FOUND => return Ok(IncarnationState::Gone),
            status if !status.is_success() => {
                return Err(transient(format!("source HEAD returned HTTP {status}")));
            }
            _ => {}
        }
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| transient("source base URL cannot carry a path"))?
            .pop_if_empty()
            .push(bucket)
            .push("streams");
        url.query_pairs_mut()
            .append_pair("prefix", key)
            .append_pair("limit", "1");
        let response = self.client.get(url).send().await.map_err(transient)?;
        if !response.status().is_success() {
            // Listing unavailable: the HEAD said the path exists.
            return Ok(IncarnationState::Present);
        }
        let body = response.bytes().await.map_err(transient)?;
        let listing: Listing = serde_json::from_slice(&body).map_err(transient)?;
        Ok(
            match listing
                .streams
                .iter()
                .find(|entry| entry.stream_id == key)
                .and_then(|entry| entry.created_at_ms)
            {
                // Incarnations are created with increasing `created_at_ms`.
                // The listing reads a possibly lagging replica, so an OLDER
                // incarnation there is inconclusive (a recreate it has not
                // applied yet); only a NEWER one proves this one is gone.
                Some(created) if created > incarnation => IncarnationState::Gone,
                _ => IncarnationState::Present,
            },
        )
    }
}

const MAX_BYTES_REJECTED: &str = "source rejected max_bytes on a record read";

impl SourceClient for KeyedSourceClient {
    fn read<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        record: u64,
        max_bytes: u64,
        max_records: Option<u64>,
        leader: bool,
    ) -> BoxFuture<'a, Result<SourcePage, SourceError>> {
        KeyedSourceClient::read(self, bucket, key, record, max_bytes, max_records, leader).boxed()
    }

    fn incarnation<'a>(
        &'a self,
        bucket: &'a str,
        key: &'a str,
        incarnation: u64,
    ) -> BoxFuture<'a, Result<IncarnationState, SourceError>> {
        KeyedSourceClient::incarnation(self, bucket, key, incarnation).boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A source whose HEAD always finds the stream and whose bucket listing
    /// reports `created_at_ms` for it.
    async fn source_listing(
        created_at_ms: u64,
    ) -> (KeyedSourceClient, tokio::task::JoinHandle<()>) {
        use axum::routing::get;
        let app = axum::Router::new()
            .route("/b/k", get(|| async { "" }).head(|| async { "" }))
            .route(
                "/b/streams",
                get(move || async move {
                    axum::Json(serde_json::json!({
                        "streams": [{"stream_id": "k", "created_at_ms": created_at_ms}],
                    }))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let client = KeyedSourceClient::new(Url::parse(&format!("http://{addr}/")).expect("url"))
            .expect("client");
        (client, server)
    }

    /// A lagging listing that still shows the previous incarnation (100)
    /// after a recreate (200) is not proof that 200 is gone.
    #[tokio::test]
    async fn older_listed_incarnation_is_inconclusive() {
        let (client, server) = source_listing(100).await;
        assert_eq!(
            client
                .incarnation("b", "k", 200)
                .await
                .expect("incarnation"),
            IncarnationState::Present
        );
        assert_eq!(
            client
                .incarnation("b", "k", 100)
                .await
                .expect("incarnation"),
            IncarnationState::Present
        );
        // A newer incarnation proves the older one was deleted.
        assert_eq!(
            client.incarnation("b", "k", 50).await.expect("incarnation"),
            IncarnationState::Gone
        );
        server.abort();
    }
}
