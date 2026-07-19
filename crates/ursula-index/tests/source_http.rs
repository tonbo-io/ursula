use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
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
