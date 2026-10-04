#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use std::sync::Arc;
use std::sync::Mutex;

use axum::Router;
use axum::extract::Request;
use axum::http::Method;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use reqwest::Url;
use ursula_index::Extractor;
use ursula_index::IndexError;
use ursula_index::OversizeScan;
use ursula_index::ReadLimits;
use ursula_index::SegmentRead;
use ursula_index::SkipKind;
use ursula_index::SourceClient;

/// A source that serves `body` in reads of at most `chunk` bytes, like an
/// Ursula offset read that the server cut.
#[derive(Clone)]
struct ChunkedSource {
    body: Arc<Vec<u8>>,
    chunk: usize,
    content_type: &'static str,
    retained: u64,
    /// Report a `Stream-Next-Offset` one byte past the body it sends.
    lie: bool,
    /// The offset of every read served.
    reads: Arc<Mutex<Vec<u64>>>,
}

impl ChunkedSource {
    fn new(body: &str, chunk: usize, content_type: &'static str) -> Self {
        Self {
            body: Arc::new(body.as_bytes().to_vec()),
            chunk,
            content_type,
            retained: 0,
            lie: false,
            reads: Arc::default(),
        }
    }

    fn respond(&self, request: &Request) -> Response {
        let tail = u64::try_from(self.body.len()).expect("small body");
        let pad = |offset: u64| format!("{offset:020}");
        if request.method() == Method::HEAD {
            return (StatusCode::OK, [
                ("content-type", self.content_type.to_owned()),
                ("stream-next-offset", pad(tail)),
                ("stream-retained-offset", pad(self.retained)),
                ("stream-incarnation", "1759482000123".to_owned()),
            ])
                .into_response();
        }
        let query = request.uri().query().unwrap_or_default();
        assert!(
            query.split('&').any(|pair| pair == "consistency=leader"),
            "offset reads go to the leader"
        );
        let offset = query
            .split('&')
            .find_map(|pair| pair.strip_prefix("offset="))
            .and_then(|offset| offset.parse::<u64>().ok())
            .expect("an offset read");
        self.reads.lock().expect("lock").push(offset);
        if offset < self.retained {
            return (StatusCode::GONE, [(
                "stream-next-offset",
                pad(self.retained),
            )])
                .into_response();
        }
        let start = usize::try_from(offset).expect("small offset");
        let end = start.saturating_add(self.chunk).min(self.body.len());
        let chunk = self.body.get(start..end).unwrap_or_default().to_vec();
        let mut next = u64::try_from(end).expect("small offset");
        if self.lie {
            next = next.saturating_add(1);
        }
        let mut headers = vec![("stream-next-offset", pad(next))];
        if end == self.body.len() {
            headers.push(("stream-up-to-date", "true".to_owned()));
        }
        let mut response = (StatusCode::OK, chunk).into_response();
        for (name, value) in headers {
            response
                .headers_mut()
                .insert(name, value.parse().expect("valid header"));
        }
        response
    }

    async fn client(self) -> anyhow::Result<(SourceClient, tokio::task::JoinHandle<()>)> {
        let app = Router::new().route(
            "/stream",
            axum::routing::any(move |request: Request| {
                let source = self.clone();
                async move { source.respond(&request) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let server = tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("mock source serves");
        });
        let client = SourceClient::new(
            reqwest::Client::new(),
            Url::parse(&format!("http://{address}/stream"))?,
        );
        Ok((client, server))
    }
}

fn limits() -> ReadLimits {
    ReadLimits {
        segment_bytes: 1 << 20,
        max_entries: 1_000,
        max_message_bytes: 64,
    }
}

fn extractor() -> Extractor {
    Extractor::timestamp_field("t").expect("valid extractor")
}

async fn read(
    client: &SourceClient,
    start: u64,
    resync: bool,
    limits: ReadLimits,
) -> anyhow::Result<ursula_index::Segment> {
    match client
        .read_segment(start, resync, None, &extractor(), limits)
        .await?
    {
        SegmentRead::Segment { segment, .. } => Ok(segment),
        SegmentRead::Retained { retained_offset } => {
            anyhow::bail!("unexpected 410 to {retained_offset}")
        }
    }
}

#[tokio::test]
async fn head_reports_readability_offsets_incarnation_and_absence() -> anyhow::Result<()> {
    let (client, server) = ChunkedSource::new("{}\n", 8, "application/json; charset=utf-8")
        .client()
        .await?;
    let head = client.head().await?.expect("the stream exists");
    assert!(head.readable);
    assert_eq!(head.next_offset, 3);
    assert_eq!(head.retained_offset, 0);
    assert_eq!(head.incarnation.as_deref(), Some("1759482000123"));
    server.abort();

    let (client, server) = ChunkedSource::new("{}\n", 8, "application/octet-stream")
        .client()
        .await?;
    let head = client.head().await?.expect("the stream exists");
    assert!(!head.readable);
    assert_eq!(head.incarnation.as_deref(), Some("1759482000123"));
    assert!(matches!(
        head.ensure_readable(),
        Err(IndexError::InvalidSourceResponse(_))
    ));
    server.abort();

    let app = Router::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serves");
    });
    let missing = SourceClient::new(
        reqwest::Client::new(),
        Url::parse(&format!("http://{address}/missing"))?,
    );
    assert!(missing.head().await?.is_none());
    server.abort();
    Ok(())
}

#[tokio::test]
async fn messages_are_assembled_across_cut_reads_and_located_by_offset() -> anyhow::Result<()> {
    let body = concat!(
        "{\"t\":300,\"note\":\"\\ud800\"}\n",
        "{\"x\":1}\n",
        "not json\n",
        "{\"t\":100}\n"
    );
    let (client, server) = ChunkedSource::new(body, 5, "application/json")
        .client()
        .await?;
    let segment = read(&client, 0, false, limits()).await?;
    assert_eq!(segment.end, 53);
    let located = segment
        .entries
        .iter()
        .map(|entry| (entry.t_ms, entry.offset, entry.len))
        .collect::<Vec<_>>();
    assert_eq!(located, vec![(300, 0, 26), (100, 43, 10)]);
    let skips = segment
        .skips
        .iter()
        .map(|skip| (skip.kind, skip.offset))
        .collect::<Vec<_>>();
    assert_eq!(skips, vec![
        (SkipKind::Missing, 26),
        (SkipKind::Unparseable, 34)
    ]);
    // Every locator addresses exactly its message.
    for entry in &segment.entries {
        let start = usize::try_from(entry.offset)?;
        let end = start.saturating_add(usize::try_from(entry.len)?);
        assert!(
            body.get(start..end)
                .is_some_and(|line| line.ends_with('\n'))
        );
    }

    // A later segment starts at the previous end; limits stop at a boundary.
    let mut one = limits();
    one.max_entries = 1;
    let first = read(&client, 0, false, one).await?;
    assert_eq!((first.start, first.end), (0, 26));
    server.abort();
    Ok(())
}

#[tokio::test]
async fn an_unterminated_ndjson_tail_is_not_covered_and_oversize_lines_are_read_through()
-> anyhow::Result<()> {
    let long = format!("{{\"t\":5,\"pad\":\"{}\"}}\n", "x".repeat(100));
    let body = format!("{{\"t\":1}}\n{long}{{\"t\":2}}\n{{\"t\":3");
    let (client, server) = ChunkedSource::new(&body, 16, "application/x-ndjson")
        .client()
        .await?;
    assert!(client.head().await?.expect("exists").readable);
    let segment = read(&client, 0, false, limits()).await?;
    let complete = u64::try_from(body.len().saturating_sub("{\"t\":3".len()))?;
    assert_eq!(segment.end, complete);
    assert_eq!(
        segment
            .entries
            .iter()
            .map(|entry| entry.t_ms)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(segment.skips.len(), 1);
    assert_eq!(segment.skips[0].kind, SkipKind::Oversize);
    assert_eq!(segment.skips[0].offset, 8);
    assert_eq!(segment.skips[0].len, u64::try_from(long.len())?);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn a_read_ending_inside_an_oversize_line_is_resumed_where_it_stopped() -> anyhow::Result<()> {
    let long = format!("{{\"t\":5,\"pad\":\"{}\"}}\n", "x".repeat(100));
    let unterminated = format!("{{\"t\":1}}\n{}", long.trim_end());
    let source = ChunkedSource::new(&unterminated, 16, "application/x-ndjson");
    let (client, server) = source.clone().client().await?;
    let scanned_to = u64::try_from(unterminated.len())?;
    match client
        .read_segment(0, false, None, &extractor(), limits())
        .await?
    {
        SegmentRead::Segment {
            segment,
            oversize_scan,
        } => {
            assert_eq!(segment.end, 8);
            assert_eq!(
                oversize_scan,
                Some(OversizeScan {
                    line_start: 8,
                    scanned_to,
                })
            );
        }
        SegmentRead::Retained { .. } => anyhow::bail!("unexpected 410"),
    }
    server.abort();

    // Once the LF arrives, the next read starts where the scan stopped and
    // still counts the whole line as one oversize message.
    let source = ChunkedSource::new(
        &format!("{{\"t\":1}}\n{long}{{\"t\":2}}\n"),
        16,
        "application/x-ndjson",
    );
    let (client, server) = source.clone().client().await?;
    let resume = OversizeScan {
        line_start: 8,
        scanned_to,
    };
    let SegmentRead::Segment { segment, .. } = client
        .read_segment(8, false, Some(resume), &extractor(), limits())
        .await?
    else {
        anyhow::bail!("unexpected 410");
    };
    assert_eq!(
        source.reads.lock().expect("lock").first(),
        Some(&scanned_to)
    );
    assert_eq!(segment.skips.len(), 1);
    assert_eq!(segment.skips[0].kind, SkipKind::Oversize);
    assert_eq!(segment.skips[0].offset, 8);
    assert_eq!(segment.skips[0].len, u64::try_from(long.len())?);
    assert_eq!(
        segment
            .entries
            .iter()
            .map(|entry| entry.t_ms)
            .collect::<Vec<_>>(),
        vec![2]
    );
    server.abort();
    Ok(())
}

#[tokio::test]
async fn a_restart_inside_an_ndjson_line_discards_its_tail_as_trimmed() -> anyhow::Result<()> {
    let body = "{\"t\":1,\"a\":\"bc\"}\n{\"t\":2}\n";
    let (client, server) = ChunkedSource::new(body, 64, "application/x-ndjson")
        .client()
        .await?;
    let mid_line = read(&client, 8, true, limits()).await?;
    assert_eq!(mid_line.skips.len(), 1);
    assert_eq!(mid_line.skips[0].kind, SkipKind::Trimmed);
    assert_eq!(mid_line.skips[0].len, 9);
    assert_eq!(mid_line.entries.len(), 1);
    assert_eq!(mid_line.entries[0].offset, 17);
    // On a boundary the first line is a message like any other.
    let on_boundary = read(&client, 17, true, limits()).await?;
    assert!(on_boundary.skips.is_empty());
    assert_eq!(on_boundary.entries.len(), 1);
    server.abort();
    Ok(())
}

#[tokio::test]
async fn retention_gaps_and_inconsistent_reads_are_reported() -> anyhow::Result<()> {
    let mut source = ChunkedSource::new("{\"t\":1}\n{\"t\":2}\n", 64, "application/json");
    source.retained = 8;
    let (client, server) = source.clone().client().await?;
    match client
        .read_segment(0, false, None, &extractor(), limits())
        .await?
    {
        SegmentRead::Retained { retained_offset } => assert_eq!(retained_offset, 8),
        SegmentRead::Segment { .. } => anyhow::bail!("expected a 410"),
    }
    server.abort();

    source.retained = 0;
    source.lie = true;
    let (client, server) = source.client().await?;
    assert!(matches!(
        client.read_at(0).await,
        Err(IndexError::InvalidSourceResponse(_))
    ));
    server.abort();

    let app = Router::new().route(
        "/stream",
        axum::routing::get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "proxy failure") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serves");
    });
    let failing = SourceClient::new(
        reqwest::Client::new(),
        Url::parse(&format!("http://{address}/stream"))?,
    );
    assert!(matches!(
        failing.read_at(0).await,
        Err(IndexError::SourceStatus(500))
    ));
    server.abort();
    Ok(())
}
