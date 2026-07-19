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
    let catalog = IndexCatalog::open_fs(FsObjectStore::new(object_dir.path())?);
    let registration = IndexRegistration {
        id: "browser-session-42".to_owned(),
        stream_url: "https://example.test/sessions/42".to_owned(),
        timestamp_field: "captured_at".to_owned(),
    };

    catalog.register(&registration).await?;
    catalog.register(&registration).await?;
    assert_eq!(catalog.get(&registration.id).await?, registration);
    assert_eq!(catalog.list().await?, vec![registration.clone()]);

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
