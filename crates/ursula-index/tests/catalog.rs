use tempfile::TempDir;
use ursula_index::FsObjectStore;
use ursula_index::IndexCatalog;
use ursula_index::IndexRegistration;

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
async fn catalog_registration_is_dynamic_durable_and_idempotent() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    let registration = IndexRegistration {
        id: "browser-session-42".to_owned(),
        stream_url: "https://example.test/sessions/42".to_owned(),
        timestamp_field: "captured_at".to_owned(),
        indexed_from_record: 17,
    };

    catalog.register(&registration).await?;
    catalog.register(&registration).await?;
    let advanced_source_retry = IndexRegistration {
        stream_url: "https://EXAMPLE.TEST:443/sessions/42".to_owned(),
        indexed_from_record: 23,
        ..registration.clone()
    };
    catalog.register(&advanced_source_retry).await?;
    assert_eq!(catalog.get(&registration.id).await?, registration);
    assert_eq!(catalog.list().await?, vec![registration.clone()]);
    assert!(object_dir.path().join("CATALOG").exists());
    assert!(!object_dir.path().join("sources").exists());

    let conflicting = IndexRegistration {
        stream_url: "https://example.test/sessions/other".to_owned(),
        ..registration.clone()
    };
    let error = catalog
        .register(&conflicting)
        .await
        .expect_err("an id cannot be rebound to another source");
    assert!(matches!(
        error,
        ursula_index::IndexError::RegistrationConflict(_)
    ));

    let duplicate_source = IndexRegistration {
        id: "another-id".to_owned(),
        ..registration.clone()
    };
    let error = catalog
        .register(&duplicate_source)
        .await
        .expect_err("one source cannot be indexed twice under different ids");
    assert!(matches!(
        error,
        ursula_index::IndexError::RegistrationConflict(_)
    ));

    catalog.unregister(&registration.id).await?;
    assert!(catalog.list().await?.is_empty());
    Ok(())
}

#[tokio::test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test combines fallible setup with assertions"
)]
async fn concurrent_catalog_updates_do_not_lose_registrations() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8 {
        let catalog = catalog.clone();
        tasks.spawn(async move {
            catalog
                .register(&IndexRegistration {
                    id: format!("stream-{index}"),
                    stream_url: format!("https://example.test/streams/{index}"),
                    timestamp_field: "captured_at".to_owned(),
                    indexed_from_record: u64::try_from(index).unwrap_or(u64::MAX),
                })
                .await
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    assert_eq!(catalog.list().await?.len(), 8);
    Ok(())
}

#[tokio::test]
async fn corrupt_catalog_fails_closed_instead_of_appearing_empty() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    catalog
        .register(&IndexRegistration {
            id: "protected-stream".to_owned(),
            stream_url: "https://example.test/protected".to_owned(),
            timestamp_field: "captured_at".to_owned(),
            indexed_from_record: 0,
        })
        .await?;
    std::fs::write(object_dir.path().join("CATALOG"), b"not-json")?;

    if catalog.list().await.is_ok() {
        anyhow::bail!("a corrupt catalog was interpreted as an empty catalog");
    }
    Ok(())
}
