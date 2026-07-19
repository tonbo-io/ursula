use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::routing::head;
use reqwest::Url;
use ursula_index::SourceBatch;
use ursula_index::SourceClient;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
async fn source_client_reads_envelopes_and_retention_gap() -> anyhow::Result<()> {
    let app = Router::new().route(
        "/stream",
        get(|request: axum::extract::Request| async move {
            let query = request.uri().query().unwrap_or_default();
            if query.contains("record=9") {
                return (
                    StatusCode::GONE,
                    [
                        ("stream-record-first", "12"),
                        ("stream-extensions", "json-record-coordinates-v1"),
                    ],
                    "",
                )
                    .into_response();
            }
            assert!(query.contains("record=0"));
            assert!(query.contains("record_view=envelope"));
            (
                StatusCode::OK,
                [("stream-extensions", "json-record-coordinates-v1")],
                concat!(
                    "{\"record\":0,\"value\":{\"captured_at\":\"2026-07-18T10:00:00Z\"}}\n",
                    "{\"record\":1,\"value\":{\"captured_at\":\"2026-07-18T09:00:00Z\"}}\n"
                ),
            )
                .into_response()
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = SourceClient::new(Url::parse(&format!("http://{address}/stream"))?, 100)?;

    match client.read_from(0).await? {
        SourceBatch::Records(records) => {
            assert_eq!(records.len(), 2);
            assert_eq!(records[0].record, 0);
            assert_eq!(records[1].record, 1);
        }
        SourceBatch::RetentionGap { .. } => anyhow::bail!("expected source records"),
    }
    match client.read_from(9).await? {
        SourceBatch::RetentionGap {
            first_available_record,
        } => assert_eq!(first_available_record, 12),
        SourceBatch::Records(_) => anyhow::bail!("expected retention gap"),
    }
    server.abort();
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
async fn source_client_rejects_missing_record_coordinate_capability() -> anyhow::Result<()> {
    let app = Router::new().route(
        "/stream",
        get(|| async { (StatusCode::OK, "{\"record\":0,\"value\":{}}\n") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = SourceClient::new(Url::parse(&format!("http://{address}/stream"))?, 100)?;

    let error = client
        .read_from(0)
        .await
        .expect_err("capability is required");
    assert!(matches!(
        error,
        ursula_index::IndexError::MissingRecordCoordinates
    ));
    server.abort();
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
async fn source_client_preserves_non_success_http_status() -> anyhow::Result<()> {
    let app = Router::new().route(
        "/stream",
        get(|| async { (StatusCode::INTERNAL_SERVER_ERROR, "temporary proxy failure") }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(axum::serve(listener, app).into_future());
    let client = SourceClient::new(Url::parse(&format!("http://{address}/stream"))?, 100)?;

    let error = client
        .read_from(0)
        .await
        .expect_err("500 must be preserved");
    assert!(matches!(error, ursula_index::IndexError::SourceStatus(500)));
    server.abort();
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
async fn source_client_probe_requires_json_record_coordinates() -> anyhow::Result<()> {
    let app = Router::new()
        .route(
            "/ready",
            head(|| async {
                (StatusCode::OK, [
                    ("content-type", "application/json; charset=utf-8"),
                    ("stream-extensions", "json-record-coordinates-v1"),
                    ("stream-record-first", "3"),
                    ("stream-record-next", "17"),
                ])
            }),
        )
        .route(
            "/binary",
            head(|| async {
                (StatusCode::OK, [
                    ("content-type", "application/octet-stream"),
                    ("stream-extensions", "json-record-coordinates-v1"),
                ])
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(axum::serve(listener, app).into_future());

    let ready = SourceClient::new(Url::parse(&format!("http://{address}/ready"))?, 100)?;
    ready.probe().await?;
    let range = ready.record_range().await?;
    assert_eq!(range.first_record, 3);
    assert_eq!(range.next_record, 17);
    let error = SourceClient::new(Url::parse(&format!("http://{address}/binary"))?, 100)?
        .probe()
        .await
        .expect_err("binary stream must be rejected");
    assert!(matches!(
        error,
        ursula_index::IndexError::InvalidSourceResponse("source stream is not application/json")
    ));
    server.abort();
    Ok(())
}
