use anyhow::{Context, Result};
use bytes::Bytes;
use clap::ValueEnum;
use reqwest::Client;
use reqwest::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::Serialize;
use serde_json::json;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[clap(rename_all = "lower")]
pub enum ApiStyle {
    /// Ursula's native HTTP API: PUT /{bucket}/{stream}, POST /{b}/{s} raw, GET ?offset=N|now&live=sse.
    Ursula,
    /// Durable Streams reference protocol: /v1/stream/{stream} family, no bucket.
    Durable,
    /// S2 Lite REST: /v1/basins, /v1/streams/{s}/records with JSON bodies and Bearer auth.
    S2,
}

impl ApiStyle {
    pub fn as_str(self) -> &'static str {
        match self {
            ApiStyle::Ursula => "ursula",
            ApiStyle::Durable => "durable",
            ApiStyle::S2 => "s2",
        }
    }
}

#[derive(Clone)]
pub struct Backend {
    pub kind: ApiStyle,
    pub bases: Vec<String>,
    pub bucket: String,
    pub basin: String,
    pub client: Client,
}

#[derive(Clone, Copy, Debug)]
pub struct Producer<'a> {
    pub id: &'a str,
    pub epoch: u64,
    pub seq: u64,
}

impl Backend {
    pub fn new(kind: ApiStyle, target: &str, bucket: &str, basin: &str, client: Client) -> Self {
        let bases: Vec<String> = target
            .split(',')
            .map(|s| s.trim().trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        let bases = if bases.is_empty() {
            vec![target.trim_end_matches('/').to_string()]
        } else {
            bases
        };
        Self {
            kind,
            bases,
            bucket: bucket.to_string(),
            basin: basin.to_string(),
            client,
        }
    }

    pub fn base_for(&self, idx: usize) -> &str {
        &self.bases[idx % self.bases.len()]
    }

    pub fn first_base(&self) -> &str {
        &self.bases[0]
    }

    pub async fn ensure_namespace(&self) -> Result<()> {
        let base = self.first_base();
        match self.kind {
            ApiStyle::Ursula => {
                let url = format!("{base}/{}", self.bucket);
                let resp = self.client.put(&url).send().await?;
                if !resp.status().is_success() {
                    anyhow::bail!("PUT {url} -> {}", resp.status());
                }
            }
            ApiStyle::Durable => {
                // Durable Streams has no bucket layer; nothing to create.
            }
            ApiStyle::S2 => {
                let url = format!("{base}/v1/basins");
                let body = json!({ "basin": self.basin });
                let resp = self
                    .client
                    .post(&url)
                    .headers(self.s2_headers()?)
                    .json(&body)
                    .send()
                    .await?;
                // 200/201/409 (already exists) are all fine.
                let status = resp.status();
                if !(status.is_success() || status == reqwest::StatusCode::CONFLICT) {
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("POST {url} -> {status}: {body}");
                }
            }
        }
        Ok(())
    }

    pub async fn create_stream(&self, stream: &str, content_type: &str) -> Result<()> {
        let base = self.first_base();
        match self.kind {
            ApiStyle::Ursula => {
                let url = format!("{base}/{}/{}", self.bucket, stream);
                let resp = self
                    .client
                    .put(&url)
                    .header(CONTENT_TYPE, content_type)
                    .send()
                    .await
                    .with_context(|| format!("PUT {url}"))?;
                let status = resp.status();
                if !(status.is_success() || status == reqwest::StatusCode::CONFLICT) {
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("PUT {url} -> {status}: {body}");
                }
            }
            ApiStyle::Durable => {
                let url = format!("{base}/v1/stream/{stream}");
                let resp = self
                    .client
                    .put(&url)
                    .header(CONTENT_TYPE, content_type)
                    .send()
                    .await
                    .with_context(|| format!("PUT {url}"))?;
                let status = resp.status();
                if !(status.is_success()
                    || status == reqwest::StatusCode::CONFLICT
                    || status == reqwest::StatusCode::OK)
                {
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("PUT {url} -> {status}: {body}");
                }
            }
            ApiStyle::S2 => {
                let url = format!("{base}/v1/streams");
                let body = json!({ "stream": stream });
                let resp = self
                    .client
                    .post(&url)
                    .headers(self.s2_headers()?)
                    .json(&body)
                    .send()
                    .await
                    .with_context(|| format!("POST {url}"))?;
                let status = resp.status();
                if !(status.is_success() || status == reqwest::StatusCode::CONFLICT) {
                    let body = resp.text().await.unwrap_or_default();
                    anyhow::bail!("POST {url} -> {status}: {body}");
                }
            }
        }
        Ok(())
    }

    pub fn append_request(
        &self,
        base_idx: usize,
        stream: &str,
        payload: &[u8],
        producer: Option<Producer<'_>>,
        content_type: &str,
    ) -> reqwest::RequestBuilder {
        let base = self.base_for(base_idx);
        match self.kind {
            ApiStyle::Ursula => {
                let url = format!("{base}/{}/{}", self.bucket, stream);
                let mut req = self
                    .client
                    .post(&url)
                    .header(CONTENT_TYPE, content_type.to_string())
                    .body(Bytes::copy_from_slice(payload));
                if let Some(p) = producer {
                    req = req
                        .header("producer-id", p.id)
                        .header("producer-epoch", p.epoch.to_string())
                        .header("producer-seq", p.seq.to_string());
                }
                req
            }
            ApiStyle::Durable => {
                let url = format!("{base}/v1/stream/{stream}");
                let mut req = self
                    .client
                    .post(&url)
                    .header(CONTENT_TYPE, content_type.to_string())
                    .body(Bytes::copy_from_slice(payload));
                if let Some(p) = producer {
                    req = req
                        .header("producer-id", p.id)
                        .header("producer-epoch", p.epoch.to_string())
                        .header("producer-seq", p.seq.to_string());
                }
                req
            }
            ApiStyle::S2 => {
                let url = format!("{base}/v1/streams/{stream}/records");
                let body = json!({
                    "records": [{
                        "body": String::from_utf8_lossy(payload).to_string(),
                    }]
                });
                self.client
                    .post(&url)
                    .headers(self.s2_headers().unwrap_or_default())
                    .json(&body)
            }
        }
    }

    pub fn sse_url_for(&self, base_idx: usize, stream: &str) -> (String, HeaderMap) {
        let base = self.base_for(base_idx);
        match self.kind {
            ApiStyle::Ursula => (
                format!("{base}/{}/{}?offset=now&live=sse", self.bucket, stream),
                {
                    let mut h = HeaderMap::new();
                    h.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
                    h
                },
            ),
            ApiStyle::Durable => (format!("{base}/v1/stream/{stream}?offset=now&live=sse"), {
                let mut h = HeaderMap::new();
                h.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
                h
            }),
            ApiStyle::S2 => {
                let mut h = self.s2_headers().unwrap_or_default();
                h.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
                (
                    format!("{base}/v1/streams/{stream}/records?tail_offset=0"),
                    h,
                )
            }
        }
    }

    /// Returns the URL each backend uses to replay a stream from the start
    /// ("give me everything for this stream").
    ///
    /// - Ursula uses `/bootstrap` which returns a multipart of snapshot + tail.
    /// - Durable Streams reference reads from `offset=-1` (full event log).
    /// - S2 reads `/records?seq_num=0` (full record log).
    ///
    /// `total_bytes` lets the S2 caller cap the response size; it is ignored
    /// for Ursula / Durable.
    pub fn replay_request_for(
        &self,
        base_idx: usize,
        stream: &str,
        total_bytes: u64,
    ) -> Result<reqwest::RequestBuilder> {
        let base = self.base_for(base_idx);
        match self.kind {
            ApiStyle::Ursula => {
                let url = format!("{base}/{}/{}/bootstrap", self.bucket, stream);
                Ok(self.client.get(&url))
            }
            ApiStyle::Durable => {
                let url = format!("{base}/v1/stream/{stream}?offset=-1");
                Ok(self.client.get(&url))
            }
            ApiStyle::S2 => {
                let url = format!(
                    "{base}/v1/streams/{stream}/records?seq_num=0&bytes={}",
                    total_bytes.max(1)
                );
                Ok(self.client.get(&url).headers(self.s2_headers()?))
            }
        }
    }

    pub async fn delete_stream(&self, stream: &str) -> Result<()> {
        let base = self.first_base();
        let url = match self.kind {
            ApiStyle::Ursula => format!("{base}/{}/{}", self.bucket, stream),
            ApiStyle::Durable => format!("{base}/v1/stream/{stream}"),
            ApiStyle::S2 => format!("{base}/v1/streams/{stream}"),
        };
        let req = self.client.delete(&url);
        let req = match self.kind {
            ApiStyle::S2 => req.headers(self.s2_headers()?),
            _ => req,
        };
        let _ = req.send().await;
        Ok(())
    }

    pub fn publishable_snapshot(&self) -> bool {
        matches!(self.kind, ApiStyle::Ursula | ApiStyle::Durable)
    }

    pub async fn publish_snapshot(
        &self,
        stream: &str,
        offset_bytes: u64,
        body: Bytes,
    ) -> Result<()> {
        let base = self.first_base();
        let url = match self.kind {
            ApiStyle::Ursula => {
                format!("{base}/{}/{}/snapshot/{offset_bytes}", self.bucket, stream)
            }
            ApiStyle::Durable => format!("{base}/v1/stream/{stream}/snapshot/{offset_bytes}"),
            ApiStyle::S2 => anyhow::bail!("S2 does not have a /snapshot endpoint"),
        };
        let resp = self
            .client
            .put(&url)
            .header(CONTENT_TYPE, "application/octet-stream")
            .body(body)
            .send()
            .await
            .with_context(|| format!("PUT {url}"))?;
        let status = resp.status();
        if !(status.is_success() || status == reqwest::StatusCode::CONFLICT) {
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("PUT {url} -> {status}: {body}");
        }
        Ok(())
    }

    fn s2_headers(&self) -> Result<HeaderMap> {
        let mut h = HeaderMap::new();
        h.insert(AUTHORIZATION, HeaderValue::from_static("Bearer ignored"));
        h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        h.insert(
            "s2-basin",
            HeaderValue::from_str(&self.basin).context("basin header")?,
        );
        h.insert("s2-format", HeaderValue::from_static("raw"));
        Ok(h)
    }
}
