use adxlet::checkpoint::{CheckpointAccess, ObjectCheckpointStore};
use object_store::{memory::InMemory, path::Path as ObjectPath, ObjectStore, ObjectStoreExt};
use std::sync::Arc;

#[tokio::test]
async fn remote_checkpoint_survives_source_disk_loss_and_verifies_content() {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let source_dir = tempfile::tempdir().unwrap();
    let source = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        source_dir.path().into(),
        1024,
    )
    .unwrap();
    let staged = source.begin_capture().await.unwrap();
    std::fs::create_dir(staged.join("nested")).unwrap();
    std::fs::write(staged.join("nested/memory"), b"memory-state").unwrap();
    let artifact = source.seal(&staged).await.unwrap();
    assert_eq!(artifact.storage, "shared");
    assert!(!artifact
        .location
        .contains(source_dir.path().to_str().unwrap()));
    drop(source);
    drop(source_dir);
    let destination_dir = tempfile::tempdir().unwrap();
    let destination = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        destination_dir.path().into(),
        1024,
    )
    .unwrap();
    let lease = destination.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(lease.join("nested/memory")).unwrap(),
        b"memory-state"
    );
    assert!(
        destination.release(&artifact).await.is_err(),
        "active restore pins artifact"
    );
    drop(lease);
    destination.evict_unreferenced(0).await.unwrap();
    remote
        .put(
            &ObjectPath::from(format!("test/{}/files/nested/memory", artifact.location)),
            bytes::Bytes::from_static(b"corrupt-data").into(),
        )
        .await
        .unwrap();
    assert!(destination.prepare_restore(&artifact).await.is_err());
    destination.release(&artifact).await.unwrap();
    assert!(destination.prepare_restore(&artifact).await.is_err());
}

#[tokio::test]
async fn cache_budget_evicts_only_unpinned_downloads() {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote,
        "test".into(),
        dir.path().into(),
        12,
    )
    .unwrap();
    let mut artifacts = vec![];
    for value in [b'1', b'2', b'3'] {
        let staged = store.begin_capture().await.unwrap();
        std::fs::write(staged.join("memory"), vec![value; 6]).unwrap();
        let artifact = store.seal(&staged).await.unwrap();
        store.committed(&artifact).await.unwrap();
        artifacts.push(artifact);
    }
    let first = store.prepare_restore(&artifacts[0]).await.unwrap();
    let second = store.prepare_restore(&artifacts[1]).await.unwrap();
    let third = store.prepare_restore(&artifacts[2]).await.unwrap();
    assert!(
        third.join("memory").exists(),
        "cache quota cannot block restore"
    );
    let second_path = second.to_path_buf();
    drop(second);
    let third = store.prepare_restore(&artifacts[2]).await.unwrap();
    store.evict_unreferenced(0).await.unwrap();
    assert!(!second_path.exists());
    assert!(first.join("memory").exists());
    assert!(third.join("memory").exists());
}

#[tokio::test]
async fn committed_pause_seeds_cache_without_an_obs_read() {
    let remote: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().into(),
        1024 * 1024,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"local-pause").unwrap();
    let artifact = store.seal(&staged).await.unwrap();
    store.committed(&artifact).await.unwrap();
    store.committed(&artifact).await.unwrap();
    assert!(
        staged.exists(),
        "admission reuses the original files without copying"
    );
    // Remove the remote commit marker: a hot restore must need neither a
    // manifest request nor a checkpoint data GET.
    remote
        .delete(&ObjectPath::from(format!(
            "test/{}/manifest.json",
            artifact.location
        )))
        .await
        .unwrap();
    let restored = store.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"local-pause"
    );
}

#[tokio::test]
async fn checkpoint_larger_than_cache_still_restores() {
    let root = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(InMemory::new()),
        "test".into(),
        root.path().into(),
        1,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"bigger-than-cache").unwrap();
    let artifact = store.seal(&staged).await.unwrap();
    store.committed(&artifact).await.unwrap();
    let restored = store.prepare_restore(&artifact).await.unwrap();
    assert_eq!(
        std::fs::read(restored.join("memory")).unwrap(),
        b"bigger-than-cache"
    );
}

#[tokio::test]
async fn upload_rejects_symlinks_and_leaves_staging_for_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        Arc::new(InMemory::new()),
        "test".into(),
        dir.path().into(),
        1024,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"recover-me").unwrap();
    std::os::unix::fs::symlink("/etc/passwd", staged.join("escape")).unwrap();
    assert!(store.seal(&staged).await.is_err());
    assert_eq!(std::fs::read(staged.join("memory")).unwrap(), b"recover-me");
}

#[tokio::test]
async fn node_cache_release_leaves_shared_artifact_for_coordinator() {
    use adx_checkpoint_storage::reclaim::{ArtifactReclaimer, ObjectReclaimer};
    let root = tempfile::tempdir().unwrap();
    let remote = Arc::new(InMemory::new());
    let store = ObjectCheckpointStore::new(
        "shared".into(),
        remote.clone(),
        "test".into(),
        root.path().into(),
        1024,
    )
    .unwrap();
    let staged = store.begin_capture().await.unwrap();
    std::fs::write(staged.join("memory"), b"snapshot").unwrap();
    let artifact = store.seal(&staged).await.unwrap();
    store.release(&artifact).await.unwrap();
    let access = store.prepare_restore(&artifact).await.unwrap();
    assert_eq!(std::fs::read(access.join("memory")).unwrap(), b"snapshot");
    drop(access);
    let gc = ObjectReclaimer::new("shared".into(), remote, "test".into()).unwrap();
    gc.remove(&artifact).await.unwrap();
    store.release(&artifact).await.unwrap();
    assert!(store.prepare_restore(&artifact).await.is_err());
}

#[test]
fn s3_client_and_control_tls_build_in_the_same_process() {
    let root = tempfile::tempdir().unwrap();
    let store = adxlet::checkpoint::StorageConfig::S3 {
        alias: "shared".into(),
        bucket: "checkpoints".into(),
        region: "us-east-1".into(),
        endpoint: Some("https://objects.example.invalid".into()),
        allow_http: false,
        prefix: "test".into(),
        root: root.path().into(),
        cache_budget_bytes: 1024,
        cache_max_entries: 500,
    };
    let endpoint = tonic::transport::Endpoint::from_static("https://control.example.invalid")
        .tls_config(tonic::transport::ClientTlsConfig::new().domain_name("adx.internal"));
    assert!(endpoint.is_ok());
    assert!(store.build().is_ok());
}
