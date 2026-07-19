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
        let mut url = self.stream_url.clone();
        url.query_pairs_mut()
            .append_pair("record", &record.to_string())
            .append_pair("record_view", "envelope")
            .append_pair("max_records", &self.max_records.to_string());
        let response = self.client.get(url).send().await?;
        let supports_record_coordinates = response
            .headers()
            .get_all("stream-extensions")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|value| value.trim() == RECORD_COORDINATE_EXTENSION);
        if !supports_record_coordinates {
            return Err(IndexError::MissingRecordCoordinates);
        }
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
        let body = response.text().await?;
        let mut records = Vec::new();
        for line in body.lines().filter(|line| !line.trim().is_empty()) {
            records.push(serde_json::from_str(line)?);
        }
        Ok(SourceBatch::Records(records))
    }
}
