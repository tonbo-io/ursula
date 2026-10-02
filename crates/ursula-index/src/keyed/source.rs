//! Source client of the keyed engine (design §6.1 U18).
//!
//! Reads a keyed stream's log from an Ursula node or gateway in P7 pages:
//! record-aware reads in the default view (`application/x-ndjson`, one stored
//! message per line) bounded by `max_bytes`. Reads use the node's default
//! (local) consistency, so a rebuild may be served by followers; decisions
//! that retire a namespace confirm with a leader read. One HTTP client is
//! shared by every namespace.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use reqwest::StatusCode;
use reqwest::Url;
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

fn header_u64(headers: &reqwest::header::HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

fn advertises(headers: &reqwest::header::HeaderMap, token: &str) -> bool {
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
        let status = response.status();
        let headers = response.headers().clone();
        match status {
            StatusCode::NOT_FOUND => return Err(SourceError::NotFound),
            StatusCode::GONE => {
                return Err(SourceError::Retained {
                    first_record: header_u64(&headers, "stream-record-first")
                        .unwrap_or(record.saturating_add(1)),
                });
            }
            StatusCode::BAD_REQUEST => {
                // A beyond-tail read names the tail; any other 400 on a
                // well-formed request is a node that rejects `max_bytes`.
                return Err(match header_u64(&headers, "stream-record-next") {
                    Some(next_record) => SourceError::BeyondTail { next_record },
                    None if !fallback => SourceError::Transient(MAX_BYTES_REJECTED.to_owned()),
                    None => transient("source rejected a record read with 400"),
                });
            }
            status if !status.is_success() => {
                return Err(transient(format!("source returned HTTP {status}")));
            }
            _ => {}
        }
        if !advertises(&headers, KEYED_BATCH_PROFILE) {
            return Err(SourceError::NotKeyed);
        }
        let start_record = header_u64(&headers, "stream-record-start")
            .ok_or_else(|| transient("source read omitted Stream-Record-Start"))?;
        let next_record = header_u64(&headers, "stream-record-next")
            .ok_or_else(|| transient("source read omitted Stream-Record-Next"))?;
        let body = response.text().await.map_err(transient)?;
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

    /// Whether incarnation `incarnation` of the stream still exists: a HEAD
    /// of the stream, then the bucket listing's `created_at_ms`. Absent or
    /// lagging listing entries are inconclusive and count as present.
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
                Some(created) if created != incarnation => IncarnationState::Gone,
                _ => IncarnationState::Present,
            },
        )
    }
}

const MAX_BYTES_REJECTED: &str = "source rejected max_bytes on a record read";
