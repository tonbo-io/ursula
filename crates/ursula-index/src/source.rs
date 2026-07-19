use reqwest::StatusCode;
use reqwest::Url;

use crate::IndexError;
use crate::SourceEnvelope;

const RECORD_COORDINATE_EXTENSION: &str = "json-record-coordinates-v1";

#[derive(Debug)]
pub enum SourceBatch {
    Records(Vec<SourceEnvelope>),
    RetentionGap { first_available_record: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SourceRecordRange {
    pub first_record: u64,
    pub next_record: u64,
}

#[derive(Clone)]
pub struct SourceClient {
    client: reqwest::Client,
    stream_url: Url,
    max_records: usize,
}

impl SourceClient {
    pub fn new(stream_url: Url, max_records: usize) -> Result<Self, IndexError> {
        if max_records == 0 {
            return Err(IndexError::InvalidConfig("max_records must be positive"));
        }
        Ok(Self {
            client: reqwest::Client::new(),
            stream_url,
            max_records,
        })
    }

    pub async fn read_from(&self, record: u64) -> Result<SourceBatch, IndexError> {
        self.read_range(record, self.max_records).await
    }

    pub async fn read_range(
        &self,
        record: u64,
        max_records: usize,
    ) -> Result<SourceBatch, IndexError> {
        if max_records == 0 {
            return Err(IndexError::InvalidConfig("max_records must be positive"));
        }
        let mut url = self.stream_url.clone();
        url.query_pairs_mut()
            .append_pair("record", &record.to_string())
            .append_pair("record_view", "envelope")
            .append_pair("max_records", &max_records.to_string());
        let response = self.client.get(url).send().await?;
        if response.status() == StatusCode::GONE {
            let first_available_record = response
                .headers()
                .get("stream-record-first")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(IndexError::InvalidSourceResponse(
                    "410 response omitted Stream-Record-First",
                ))?;
            return Ok(SourceBatch::RetentionGap {
                first_available_record,
            });
        }
        if !response.status().is_success() {
            return Err(IndexError::SourceStatus(response.status().as_u16()));
        }
        if !supports_record_coordinates(response.headers()) {
            return Err(IndexError::MissingRecordCoordinates);
        }
        let body = response.text().await?;
        let mut records = Vec::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            records.push(serde_json::from_str(line)?);
        }
        Ok(SourceBatch::Records(records))
    }

    pub async fn probe(&self) -> Result<(), IndexError> {
        let _range = self.record_range().await?;
        Ok(())
    }

    pub async fn record_range(&self) -> Result<SourceRecordRange, IndexError> {
        let response = self.client.head(self.stream_url.clone()).send().await?;
        if !response.status().is_success() {
            return Err(IndexError::SourceStatus(response.status().as_u16()));
        }
        let is_json = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"));
        if !is_json {
            return Err(IndexError::InvalidSourceResponse(
                "source stream is not application/json",
            ));
        }
        if !supports_record_coordinates(response.headers()) {
            return Err(IndexError::MissingRecordCoordinates);
        }
        let first_record = record_header(response.headers(), "stream-record-first")?;
        let next_record = record_header(response.headers(), "stream-record-next")?;
        if first_record > next_record {
            return Err(IndexError::InvalidSourceResponse(
                "source record range is reversed",
            ));
        }
        Ok(SourceRecordRange {
            first_record,
            next_record,
        })
    }
}

fn record_header(
    headers: &reqwest::header::HeaderMap,
    name: &'static str,
) -> Result<u64, IndexError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
        .ok_or(IndexError::InvalidSourceResponse(
            "source HEAD omitted a record range header",
        ))
}

fn supports_record_coordinates(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get_all("stream-extensions")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim() == RECORD_COORDINATE_EXTENSION)
}
