use super::*;
use adx_core::{CheckpointArtifact, RestorePoint};

fn artifact() -> CheckpointArtifact {
    CheckpointArtifact {
        storage: "shared".into(),
        location: uuid::Uuid::new_v4().to_string(),
        size_bytes: 8,
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn anonymous_artifact_follows_environment_but_live_inputs_delay_reclamation() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    let assignment = Assignment {
        environment_id: "owner".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    let specification = spec("owner");
    session
        .reserve(specification.clone(), assignment.clone())
        .await
        .unwrap();
    let a = artifact();
    let mut r = running(specification, assignment);
    r.checkpoint = Some(RestorePoint {
        origin: None,
        id: "pause".into(),
        artifact: a.clone(),
        expires_at_unix_seconds: None,
        source_runtime_id: r.runtime.id.clone(),
    });
    r.execution_artifacts.push(a.clone());
    session.commit(r.clone()).await.unwrap();
    r.checkpoint = None;
    r.revision += 1;
    session.commit(r.clone()).await.unwrap();
    assert!(
        session.claim_artifact_deletion(&a, None).await.is_err(),
        "old input is still mapped"
    );
    let session = store.begin(1).await.unwrap();
    assert_eq!(session.shared_artifacts().await.unwrap(), vec![a.clone()]);
    assert!(
        session.claim_artifact_deletion(&a, None).await.is_err(),
        "restart must preserve input protection"
    );
    r.state = EnvironmentState::Deleted;
    r.resources_held = false;
    r.runtime.ip = None;
    r.execution_artifacts.clear();
    r.revision += 1;
    session.commit(r).await.unwrap();
    session.claim_artifact_deletion(&a, None).await.unwrap();
    session.claim_artifact_deletion(&a, None).await.unwrap();
    session.forget_artifact(&a).await.unwrap();
    assert!(session.shared_artifacts().await.unwrap().is_empty());
    // A delayed task cannot resurrect the reclaimed identity under another Environment.
    let assignment = Assignment {
        environment_id: "late".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 2,
        devices: vec![],
    };
    session
        .reserve(spec("late"), assignment.clone())
        .await
        .unwrap();
    let mut late = running(spec("late"), assignment);
    late.execution_artifacts.push(a);
    assert_eq!(session.commit(late).await.unwrap_err(), Error::Conflict);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn node_loss_keeps_execution_inputs_until_reconciled_cleanup_acknowledgement() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let s = store.begin(1).await.unwrap();
    s.register_session(
        node("node"),
        "node:1".into(),
        "node:2".into(),
        Some(adx_coordinator::storage::NodeSession {
            id: "old".into(),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
    let assignment = Assignment {
        environment_id: "owner".into(),
        node_id: "node".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("owner"), assignment.clone()).await.unwrap();
    let a = artifact();
    let mut r = running(spec("owner"), assignment);
    r.execution_artifacts.push(a.clone());
    s.commit(r).await.unwrap();
    s.invalidate_node("node", "old").await.unwrap();
    let mut failed = s.get("owner").await.unwrap().result.unwrap();
    failed.state = EnvironmentState::Deleted;
    failed.execution_artifacts.clear();
    failed.revision += 1;
    s.commit(failed).await.unwrap();
    let s = store.begin(1).await.unwrap();
    assert_eq!(s.execution_holds().await.unwrap().len(), 1);
    assert!(s.claim_artifact_deletion(&a, None).await.is_err());
    assert!(
        s.release_execution_holds("node", "old").await.is_err(),
        "lost heartbeat is not a cleanup acknowledgement"
    );
    s.register_session(
        node("node"),
        "node:1".into(),
        "node:2".into(),
        Some(adx_coordinator::storage::NodeSession {
            id: "new".into(),
            sequence: 1,
            routable: true,
        }),
    )
    .await
    .unwrap();
    s.release_execution_holds("node", "new").await.unwrap();
    s.claim_artifact_deletion(&a, None).await.unwrap();
}

struct LostDeletionAck {
    inner: adx_checkpoint_storage::reclaim::ObjectReclaimer,
    lose_once: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl adx_checkpoint_storage::reclaim::ArtifactReclaimer for LostDeletionAck {
    fn storage_alias(&self) -> &str {
        "shared"
    }
    async fn remove(&self, a: &CheckpointArtifact) -> adx_core::Result<()> {
        self.inner.remove(a).await?;
        if self
            .lose_once
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::Unavailable(
                "deletion committed; acknowledgement lost".into(),
            ));
        }
        Ok(())
    }
}
fn reclaimers(
    store: std::sync::Arc<dyn adx_checkpoint_storage::reclaim::ArtifactReclaimer>,
) -> adx_checkpoint_storage::reclaim::Reclaimers {
    let mut stores = adx_checkpoint_storage::reclaim::Reclaimers::default();
    stores.insert(store).unwrap();
    stores
}
async fn rpc(
    session: adx_coordinator::storage::Session,
    stores: adx_checkpoint_storage::reclaim::Reclaimers,
) -> adx_coordinator::rpc::CoordinatorRpc {
    adx_coordinator::rpc::CoordinatorRpc::new(
        session,
        Placement::Pack,
        adx_protocol::auth::Peers::network(),
        adx_transport::rpc::RpcClient::network(adx_protocol::auth::Principal::Coordinator),
        std::time::Duration::from_secs(1),
    )
    .await
    .unwrap()
    .with_reclaimers(stores)
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_retries_committed_deletion_after_lost_ack_and_restart_without_source_node() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let s = store.begin(1).await.unwrap();
    register(&s, "source").await;
    let assignment = Assignment {
        environment_id: "owner".into(),
        node_id: "source".into(),
        shard_id: 0,
        generation: 1,
        devices: vec![],
    };
    s.reserve(spec("owner"), assignment.clone()).await.unwrap();
    let a = artifact();
    let mut r = running(spec("owner"), assignment);
    r.execution_artifacts.push(a.clone());
    s.commit(r.clone()).await.unwrap();
    r.state = EnvironmentState::Deleted;
    r.resources_held = false;
    r.runtime.ip = None;
    r.execution_artifacts.clear();
    r.revision += 1;
    s.commit(r).await.unwrap();
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let key = Key::from(format!("test/{}/files/memory", a.location));
    remote.put(&key, b"memory".to_vec().into()).await.unwrap();
    let reclaimer = std::sync::Arc::new(LostDeletionAck {
        inner: adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
        lose_once: std::sync::atomic::AtomicBool::new(true),
    });
    let first = rpc(s.clone(), reclaimers(reclaimer.clone())).await;
    assert!(first.collect_shared_artifacts().await.is_err());
    assert!(
        remote.get(&key).await.is_err(),
        "physical deletion already committed"
    );
    assert_eq!(s.shared_artifacts().await.unwrap(), vec![a]);
    let s = store.begin(1).await.unwrap();
    let second = rpc(s.clone(), reclaimers(reclaimer)).await;
    assert_eq!(second.collect_shared_artifacts().await.unwrap(), 1);
    assert!(s.shared_artifacts().await.unwrap().is_empty());
    assert_eq!(second.collect_shared_artifacts().await.unwrap(), 0);
}
#[tokio::test]
#[ignore = "requires dedicated real Redis"]
async fn coordinator_collects_shared_snapshot_with_source_node_offline() {
    use object_store::{path::Path as Key, ObjectStoreExt};
    let rig = common::Redis::new().await;
    let s = rig.store().await.begin(1).await.unwrap();
    let a = artifact();
    let snapshot = adx_core::snapshots::Snapshot::new(
        "saved".into(),
        vec![],
        spec("owner"),
        "offline-source".into(),
        "owner-1".into(),
        a.clone(),
    )
    .unwrap();
    s.publish_snapshot(snapshot).await.unwrap();
    s.delete_snapshot("saved", "tenant").await.unwrap();
    let remote = std::sync::Arc::new(object_store::memory::InMemory::new());
    let key = Key::from(format!("test/{}/files/memory", a.location));
    remote.put(&key, b"memory".to_vec().into()).await.unwrap();
    let reclaimer = std::sync::Arc::new(
        adx_checkpoint_storage::reclaim::ObjectReclaimer::new(
            "shared".into(),
            remote.clone(),
            "test".into(),
        )
        .unwrap(),
    );
    let coordinator = rpc(s.clone(), reclaimers(reclaimer)).await;
    assert_eq!(coordinator.collect_snapshots().await.unwrap(), 1);
    assert!(remote.get(&key).await.is_err());
    assert_eq!(
        s.get_snapshot("saved").await.unwrap().state,
        adx_core::snapshots::SnapshotState::Deleted
    );
}
