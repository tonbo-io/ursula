#![expect(
    clippy::panic_in_result_fn,
    reason = "integration tests combine fallible setup with assertions"
)]

use tempfile::TempDir;
use ursula_index::Extractor;
use ursula_index::FsObjectStore;
use ursula_index::IndexCatalog;
use ursula_index::IndexError;
use ursula_index::IndexRegistration;
use ursula_index::StartPosition;

fn registration(id: &str, stream_url: &str, incarnation: &str) -> IndexRegistration {
    IndexRegistration {
        id: id.to_owned(),
        stream_url: stream_url.to_owned(),
        extract: Extractor::timestamp_field("captured_at")
            .expect("valid extractor")
            .config()
            .clone(),
        start: StartPosition::Retained,
        indexed_from_offset: 17,
        incarnation: Some(incarnation.to_owned()),
        restarted_from_incarnation: None,
    }
}

#[tokio::test]
async fn one_pool_worker_owns_the_renewable_maintenance_lease() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let first = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    let second = first.clone();

    assert!(
        first
            .acquire_maintenance_lease("worker-a", 1_000, 1_000)
            .await?
    );
    assert!(
        !second
            .acquire_maintenance_lease("worker-b", 1_100, 1_000)
            .await?
    );
    assert!(
        first
            .acquire_maintenance_lease("worker-a", 1_200, 1_000)
            .await?
    );
    assert!(
        second
            .acquire_maintenance_lease("worker-b", 2_001, 1_000)
            .await?
    );
    Ok(())
}

#[tokio::test]
async fn catalog_registration_is_dynamic_durable_and_idempotent() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    let original = registration(
        "browser-session-42",
        "https://example.test/sessions/42",
        "7",
    );

    catalog.register(&original).await?;
    catalog.register(&original).await?;
    let advanced_source_retry = IndexRegistration {
        stream_url: "https://EXAMPLE.TEST:443/sessions/42".to_owned(),
        indexed_from_offset: 23,
        ..original.clone()
    };
    catalog.register(&advanced_source_retry).await?;
    assert_eq!(catalog.get(&original.id).await?, original);
    assert_eq!(catalog.list().await?, vec![original.clone()]);
    assert!(object_dir.path().join("CATALOG").exists());

    for conflicting in [
        IndexRegistration {
            stream_url: "https://example.test/sessions/other".to_owned(),
            ..original.clone()
        },
        IndexRegistration {
            extract: Extractor::timestamp_field("other")?.config().clone(),
            ..original.clone()
        },
        IndexRegistration {
            incarnation: Some("8".to_owned()),
            ..original.clone()
        },
        IndexRegistration {
            id: "another-id".to_owned(),
            ..original.clone()
        },
    ] {
        let error = catalog
            .register(&conflicting)
            .await
            .expect_err("a different identity conflicts");
        assert!(matches!(error, IndexError::RegistrationConflict(_)));
    }

    catalog.unregister(&original.id, 42).await?;
    assert!(catalog.list().await?.is_empty());
    let retired = catalog.retired_before(u64::MAX).await?;
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].namespace, original.namespace()?);
    let error = catalog
        .register(&original)
        .await
        .expect_err("a retired namespace cannot be reused before cleanup");
    assert!(matches!(error, IndexError::RegistrationConflict(_)));
    // Tombstones are keyed by namespace: the recreated stream registers at
    // once under its new incarnation.
    catalog
        .register(&registration(
            "browser-session-42",
            "https://example.test/sessions/42",
            "8",
        ))
        .await?;
    catalog.forget_retired(&original.namespace()?).await?;
    assert!(catalog.retired_before(u64::MAX).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn restart_rebinds_the_incarnation_once_and_retires_the_old_namespace() -> anyhow::Result<()>
{
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    let original = registration("restarted", "https://example.test/restarted", "1");
    catalog.register(&original).await?;

    let restarted = catalog
        .restart(&original.id, Some("1"), Some("2".to_owned()), 5, 100)
        .await?;
    assert_eq!(restarted.incarnation.as_deref(), Some("2"));
    assert_eq!(restarted.restarted_from_incarnation.as_deref(), Some("1"));
    assert_eq!(restarted.indexed_from_offset, 5);
    assert_ne!(restarted.namespace()?, original.namespace()?);
    // A second pod that saw the same change is a no-op.
    let again = catalog
        .restart(&original.id, Some("1"), Some("2".to_owned()), 9, 100)
        .await?;
    assert_eq!(again, restarted);
    let retired = catalog.retired_before(u64::MAX).await?;
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].namespace, original.namespace()?);
    Ok(())
}

#[tokio::test]
async fn concurrent_catalog_updates_do_not_lose_registrations() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..8 {
        let catalog = catalog.clone();
        tasks.spawn(async move {
            catalog
                .register(&registration(
                    &format!("stream-{index}"),
                    &format!("https://example.test/streams/{index}"),
                    "1",
                ))
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
async fn corrupt_or_older_catalogs_fail_closed_instead_of_appearing_empty() -> anyhow::Result<()> {
    let object_dir = TempDir::new()?;
    let catalog = IndexCatalog::new(FsObjectStore::new(object_dir.path())?);
    catalog
        .register(&registration(
            "protected-stream",
            "https://example.test/protected",
            "1",
        ))
        .await?;
    std::fs::write(object_dir.path().join("CATALOG"), b"not-json")?;
    if catalog.list().await.is_ok() {
        anyhow::bail!("a corrupt catalog was interpreted as an empty catalog");
    }
    std::fs::write(
        object_dir.path().join("CATALOG"),
        br#"{"version":1,"registrations":[],"retired":[]}"#,
    )?;
    assert!(matches!(
        catalog.list().await,
        Err(IndexError::ManifestVersion(1))
    ));
    Ok(())
}
