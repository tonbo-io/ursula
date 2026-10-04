//! HTTP client for the upstream stream, using only base-protocol reads.
//!
//! Messages are framed by LF for both `application/json` (whose stored form
//! is one message per line) and `application/x-ndjson`. A read may end
//! mid-message, so a message is assembled across reads, up to
//! [`ReadLimits::max_message_bytes`]; a longer one is read through and
//! counted as oversize. An unterminated last line is not covered yet.
//!
//! Internally an offset is a byte position: a message's offset is the read's
//! start plus the bytes consumed before it, and every read is checked against
//! `Stream-Next-Offset`. Clients only ever see offsets as opaque tokens.

use bytes::Bytes;
use reqwest::StatusCode;
use reqwest::Url;
use reqwest::header::HeaderMap;

use crate::IndexError;
use crate::extract::Extraction;
use crate::extract::Extractor;
use crate::extract::is_json_message;
use crate::store::EventEntry;
use crate::store::Segment;
use crate::store::Skip;
use crate::store::SkipKind;
use crate::store::offset_token;
use crate::store::parse_offset_token;

const HEADER_NEXT_OFFSET: &str = "stream-next-offset";
const HEADER_RETAINED_OFFSET: &str = "stream-retained-offset";
const HEADER_INCARNATION: &str = "stream-incarnation";
const HEADER_CLOSED: &str = "stream-closed";
const HEADER_UP_TO_DATE: &str = "stream-up-to-date";

/// Ursula's request body cap: no message can be longer.
pub const MAX_MESSAGE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceFormat {
    /// `application/json`: one stored message per LF-terminated line.
    Json,
    /// `application/x-ndjson`: the writer's own lines.
    Ndjson,
}

/// What a HEAD of the source reports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceHead {
    pub format: SourceFormat,
    pub next_offset: u64,
    pub retained_offset: u64,
    pub closed: bool,
    pub incarnation: Option<String>,
}

#[derive(Debug)]
pub enum SourceRead {
    Bytes {
        body: Bytes,
        next_offset: u64,
        up_to_date: bool,
    },
    /// The offset is below retention; reading resumes at `retained_offset`.
    Retained { retained_offset: u64 },
}

#[derive(Debug)]
pub enum SegmentRead {
    Segment(Segment),
    Retained { retained_offset: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadLimits {
    /// Stop after the message that reaches this many bytes past the start.
    pub segment_bytes: u64,
    /// Stop after this many entries.
    pub max_entries: usize,
    /// Longest message assembled across reads.
    pub max_message_bytes: usize,
}

#[derive(Clone)]
pub struct SourceClient {
    client: reqwest::Client,
    stream_url: Url,
}

impl SourceClient {
    /// `client` is shared by every source of one process.
    pub fn new(client: reqwest::Client, stream_url: Url) -> Self {
        Self { client, stream_url }
    }

    pub fn stream_url(&self) -> &Url {
        &self.stream_url
    }

    /// HEAD the source. `None` means the stream does not exist (404).
    pub async fn head(&self) -> Result<Option<SourceHead>, IndexError> {
        let response = self.client.head(self.stream_url.clone()).send().await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(IndexError::SourceStatus(response.status().as_u16()));
        }
        let headers = response.headers();
        let format = source_format(headers).ok_or(IndexError::InvalidSourceResponse(
            "source stream is neither application/json nor application/x-ndjson",
        ))?;
        let next_offset = offset_header(headers, HEADER_NEXT_OFFSET)?.ok_or(
            IndexError::InvalidSourceResponse("source HEAD omitted Stream-Next-Offset"),
        )?;
        let retained_offset = offset_header(headers, HEADER_RETAINED_OFFSET)?.unwrap_or(0);
        if retained_offset > next_offset {
            return Err(IndexError::InvalidSourceResponse(
                "source retained offset is beyond its tail",
            ));
        }
        Ok(Some(SourceHead {
            format,
            next_offset,
            retained_offset,
            closed: header_is_true(headers, HEADER_CLOSED),
            incarnation: headers
                .get(HEADER_INCARNATION)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        }))
    }

    /// One base-protocol offset read, without `max_bytes`.
    pub async fn read_at(&self, offset: u64) -> Result<SourceRead, IndexError> {
        let mut url = self.stream_url.clone();
        url.query_pairs_mut()
            .append_pair("offset", &offset_token(offset));
        let response = self.client.get(url).send().await?;
        if response.status() == StatusCode::GONE {
            let retained_offset = offset_header(response.headers(), HEADER_NEXT_OFFSET)?.ok_or(
                IndexError::InvalidSourceResponse("410 response omitted Stream-Next-Offset"),
            )?;
            if retained_offset <= offset {
                return Err(IndexError::InvalidSourceResponse(
                    "410 response did not advance the read offset",
                ));
            }
            return Ok(SourceRead::Retained { retained_offset });
        }
        if response.status() == StatusCode::NOT_FOUND {
            return Err(IndexError::SourceGone);
        }
        if !response.status().is_success() {
            return Err(IndexError::SourceStatus(response.status().as_u16()));
        }
        let next_offset = offset_header(response.headers(), HEADER_NEXT_OFFSET)?.ok_or(
            IndexError::InvalidSourceResponse("source read omitted Stream-Next-Offset"),
        )?;
        let up_to_date = header_is_true(response.headers(), HEADER_UP_TO_DATE);
        let body = response.bytes().await?;
        let consumed = u64::try_from(body.len())
            .ok()
            .and_then(|len| offset.checked_add(len));
        if consumed != Some(next_offset) {
            return Err(IndexError::InvalidSourceResponse(
                "source read length does not match Stream-Next-Offset",
            ));
        }
        Ok(SourceRead::Bytes {
            body,
            next_offset,
            up_to_date,
        })
    }

    /// Read and extract complete messages from `start`, which must be a
    /// message boundary unless `resync` is set: then a first line that is
    /// not a complete JSON value is a tail of a trimmed message and is
    /// discarded as trimmed bytes.
    pub async fn read_segment(
        &self,
        start: u64,
        resync: bool,
        extractor: &Extractor,
        limits: ReadLimits,
    ) -> Result<SegmentRead, IndexError> {
        let mut segment = Segment {
            start,
            end: start,
            entries: Vec::new(),
            skips: Vec::new(),
        };
        let mut first = true;
        let outcome = self
            .read_messages(start, limits, |message| {
                let resync_line = std::mem::take(&mut first) && resync;
                match message {
                    Framed::Oversize { offset, len } => segment.skips.push(Skip {
                        offset,
                        len,
                        kind: SkipKind::Oversize,
                    }),
                    Framed::Line { offset, bytes } => {
                        let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                        if resync_line && !is_json_message(bytes) {
                            segment.skips.push(Skip {
                                offset,
                                len,
                                kind: SkipKind::Trimmed,
                            });
                            return true;
                        }
                        match extractor.extract(bytes) {
                            Extraction::Event { t_ms, t_end_ms } => {
                                segment.entries.push(EventEntry {
                                    t_ms,
                                    t_end_ms,
                                    offset,
                                    len,
                                });
                            }
                            Extraction::Skip(kind) => {
                                segment.skips.push(Skip { offset, len, kind });
                            }
                        }
                    }
                }
                segment.entries.len() < limits.max_entries
            })
            .await?;
        match outcome {
            ReadOutcome::Ended { end } => {
                segment.end = end;
                Ok(SegmentRead::Segment(segment))
            }
            ReadOutcome::Retained { retained_offset } => {
                Ok(SegmentRead::Retained { retained_offset })
            }
        }
    }

    /// Frame messages from `start` until a limit, the tail, or a 410.
    /// `on_message` returns whether to continue after that message.
    async fn read_messages<F>(
        &self,
        start: u64,
        limits: ReadLimits,
        mut on_message: F,
    ) -> Result<ReadOutcome, IndexError>
    where
        F: FnMut(Framed<'_>) -> bool,
    {
        let mut read_offset = start;
        let mut line_start = start;
        let mut pending = Vec::<u8>::new();
        let mut oversize = false;
        'reads: loop {
            let (body, next_offset, up_to_date) = match self.read_at(read_offset).await? {
                SourceRead::Retained { retained_offset } => {
                    return Ok(ReadOutcome::Retained { retained_offset });
                }
                SourceRead::Bytes {
                    body,
                    next_offset,
                    up_to_date,
                } => (body, next_offset, up_to_date),
            };
            if body.is_empty() {
                break;
            }
            let mut cursor = 0_usize;
            while let Some(relative) = body
                .get(cursor..)
                .and_then(|rest| rest.iter().position(|byte| *byte == b'\n'))
            {
                let line_end_index = cursor
                    .checked_add(relative)
                    .and_then(|index| index.checked_add(1))
                    .ok_or(IndexError::InvalidConfig("source read is too large"))?;
                let line_end = u64::try_from(line_end_index)
                    .ok()
                    .and_then(|index| read_offset.checked_add(index))
                    .ok_or(IndexError::InvalidConfig("source offset overflowed"))?;
                let piece = body.get(cursor..line_end_index).unwrap_or_default();
                let line_len = line_end.saturating_sub(line_start);
                let too_long = usize::try_from(line_len)
                    .ok()
                    .is_none_or(|len| len > limits.max_message_bytes.saturating_add(1));
                let keep_going = if oversize || too_long {
                    oversize = false;
                    pending.clear();
                    on_message(Framed::Oversize {
                        offset: line_start,
                        len: line_len,
                    })
                } else if pending.is_empty() {
                    on_message(Framed::Line {
                        offset: line_start,
                        bytes: piece,
                    })
                } else {
                    pending.extend_from_slice(piece);
                    let keep_going = on_message(Framed::Line {
                        offset: line_start,
                        bytes: &pending,
                    });
                    pending.clear();
                    keep_going
                };
                line_start = line_end;
                cursor = line_end_index;
                if !keep_going || line_start.saturating_sub(start) >= limits.segment_bytes {
                    break 'reads;
                }
            }
            if !oversize {
                let rest = body.get(cursor..).unwrap_or_default();
                if pending.len().saturating_add(rest.len()) > limits.max_message_bytes {
                    oversize = true;
                    pending.clear();
                } else {
                    pending.extend_from_slice(rest);
                }
            }
            read_offset = next_offset;
            if up_to_date {
                break;
            }
        }
        Ok(ReadOutcome::Ended { end: line_start })
    }
}

enum Framed<'a> {
    /// A complete message, LF included.
    Line { offset: u64, bytes: &'a [u8] },
    /// A message longer than the assembly limit, read through.
    Oversize { offset: u64, len: u64 },
}

enum ReadOutcome {
    Ended { end: u64 },
    Retained { retained_offset: u64 },
}

fn source_format(headers: &HeaderMap) -> Option<SourceFormat> {
    let media_type = headers
        .get(reqwest::header::CONTENT_TYPE)?
        .to_str()
        .ok()?
        .split(';')
        .next()?
        .trim()
        .to_ascii_lowercase();
    match media_type.as_str() {
        "application/json" => Some(SourceFormat::Json),
        "application/x-ndjson" | "application/ndjson" | "application/jsonl" => {
            Some(SourceFormat::Ndjson)
        }
        _ => None,
    }
}

fn offset_header(headers: &HeaderMap, name: &'static str) -> Result<Option<u64>, IndexError> {
    let Some(value) = headers.get(name) else {
        return Ok(None);
    };
    value
        .to_str()
        .ok()
        .and_then(parse_offset_token)
        .map(Some)
        .ok_or(IndexError::InvalidSourceResponse(
            "source sent an invalid offset header",
        ))
}

fn header_is_true(headers: &HeaderMap, name: &'static str) -> bool {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("true"))
}
