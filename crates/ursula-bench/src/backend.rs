use anyhow::Context;
use anyhow::Result;
use bytes::Bytes;
use clap::ValueEnum;
use reqwest::Client;
use reqwest::header::ACCEPT;
use reqwest::header::CONTENT_TYPE;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum, Serialize)]
#[clap(rename_all = "lower")]
pub enum ApiStyle {
    /// Ursula's native HTTP API: PUT /{bucket}/{stream}, POST /{b}/{s} raw, GET ?offset=N|now&live=sse.
    Ursula,
    /// Durable Streams reference protocol: /v1/stream/{stream} family, no bucket.
    Durable,
}

impl ApiStyle {
    pub fn as_str(self) -> &'static str {
        match self {
            ApiStyle::Ursula => "ursula",
            ApiStyle::Durable => "durable",
        }
    }
}

#[derive(Clone)]
pub struct Backend {
    pub kind: ApiStyle,
    pub bases: Vec<String>,
    pub bucket: String,
    pub client: Client,
}

#[derive(Clone, Copy, Debug)]
pub struct Producer<'a> {
    pub id: &'a str,
    pub epoch: u64,
    pub seq: u64,
}

impl Backend {
    pub fn new(kind: ApiStyle, target: &str, bucket: &str, client: Client) -> Self {
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
        let url = match self.kind {
            ApiStyle::Ursula => format!("{base}/{}/{}", self.bucket, stream),
            ApiStyle::Durable => format!("{base}/v1/stream/{stream}"),
        };
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

    pub fn sse_url_for(&self, base_idx: usize, stream: &str) -> (String, HeaderMap) {
        let base = self.base_for(base_idx);
        let url = match self.kind {
            ApiStyle::Ursula => {
                format!("{base}/{}/{}?offset=now&live=sse", self.bucket, stream)
            }
            ApiStyle::Durable => format!("{base}/v1/stream/{stream}?offset=now&live=sse"),
        };
        let mut headers = HeaderMap::new();
        headers.insert(ACCEPT, HeaderValue::from_static("text/event-stream"));
        (url, headers)
    }

    /// Returns the URL each backend uses to replay a stream from the start
    /// ("give me everything for this stream").
    ///
    /// - Ursula uses `/bootstrap` which returns a multipart of snapshot + tail.
    /// - Durable Streams reference reads from `offset=-1` (full event log).
    pub fn replay_request_for(&self, base_idx: usize, stream: &str) -> reqwest::RequestBuilder {
        let base = self.base_for(base_idx);
        let url = match self.kind {
            ApiStyle::Ursula => format!("{base}/{}/{}/bootstrap", self.bucket, stream),
            ApiStyle::Durable => format!("{base}/v1/stream/{stream}?offset=-1"),
        };
        self.client.get(&url)
    }

    pub async fn delete_stream(&self, stream: &str) -> Result<()> {
        let base = self.first_base();
        let url = match self.kind {
            ApiStyle::Ursula => format!("{base}/{}/{}", self.bucket, stream),
            ApiStyle::Durable => format!("{base}/v1/stream/{stream}"),
        };
        let _ = self.client.delete(&url).send().await;
        Ok(())
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
}
