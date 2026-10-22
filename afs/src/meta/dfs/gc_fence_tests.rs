use super::*;
use crate::dfs::{ChunkReceipt, CommitMetadataDelta, Extent, GcFence, LayoutRootId};
use crate::meta::store::{NodeSessionLease, Store, journal::MemoryLog};
use crate::node::chunk::{LocalChunkStore, StagedChunk};

struct Fixture {
    _temp: tempfile::TempDir,
    store: Arc<Store>,
    service: DfsService,
    local: LocalChunkStore,
    staged: StagedChunk,
    commit: CommitFileVersion,
}
async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let local = LocalChunkStore::open(temp.path(), "writer").unwrap();
    let store = Arc::new(
        Store::open_log(Arc::new(MemoryLog::new(Default::default()).unwrap()))
            .await
            .unwrap(),
    );
    store
        .register_node_session(
            RequestKey::new("writer", "register"),
            NodeSessionLease {
                node_id: "writer".into(),
                session_id: "writer-session".into(),
                grpc_addr: "http://127.0.0.1:1".into(),
                data_addr: "http://127.0.0.1:2".into(),
                rest_addr: "http://127.0.0.1:3".into(),
                storage_devices: vec![local.device_descriptor().unwrap()],
                lease_ttl: std::time::Duration::from_secs(60),
            },
        )
        .await
        .unwrap();
    let service = DfsService::new(store.clone());
    service.initialize_replication_config().await.unwrap();
    let root = service
        .ensure_namespace_root(&NamespaceId::new("default"))
        .await
        .unwrap();
    let mut attributes = root.attributes;
    attributes.mode = 0o644;
    let (inode, write_lease) = service
        .create(CreateFileRequest {
            caller_id: "writer".into(),
            owner_session_id: "writer-session".into(),
            operation_id: OperationId::new("create-fenced"),
            namespace_id: NamespaceId::new("default"),
            parent_inode_id: root.inode_id,
            name: b"fenced.bin".to_vec(),
            attributes,
            lease_seconds: 60,
        })
        .await
        .unwrap();
    let fence = service.current_gc_fence().await.unwrap();
    let operation_id = OperationId::new("publish-fenced");
    let staged = StagedChunk::new(operation_id.clone(), b"fenced body".to_vec());
    let placement = service.placement_snapshot("writer".into()).await.unwrap();
    let group = placement
        .replica_groups
        .iter()
        .find(|g| g.targets[0].node_id == "writer")
        .unwrap();
    let ack = local
        .persist(
            &staged,
            &group.targets[0],
            placement.revision,
            group.placement_epoch,
        )
        .unwrap();
    let layout = LayoutRoot {
        id: LayoutRootId::new("fenced-layout"),
        file_length: staged.chunk.length,
        inline_extents: vec![Extent {
            file_offset: 0,
            length: staged.chunk.length,
            chunk_id: staged.chunk.id.clone(),
            chunk_offset: 0,
        }],
    };
    let commit = CommitFileVersion {
        gc_fence: Some(fence),
        operation_id: operation_id.clone(),
        inode_id: inode.inode_id.clone(),
        write_lease,
        expected_inode_revision: inode.revision,
        expected_head_version: inode.head_version.clone(),
        file_version: FileVersion {
            id: FileVersionId::new("fenced-version"),
            inode_id: inode.inode_id,
            parent_version: inode.head_version,
            length: staged.chunk.length,
            layout_root: layout.id.clone(),
            created_at_unix_ms: 1,
        },
        layout_root: layout,
        chunk_receipts: vec![ChunkReceipt {
            operation_id,
            chunk: staged.chunk.clone(),
            placement_revision: placement.revision,
            placement_epoch: group.placement_epoch,
            replica_group_id: group.id.clone(),
            durable_acks: vec![ack],
            external_commit: None,
        }],
        clone_source: None,
        metadata_delta: CommitMetadataDelta {
            mode: CommitMetadataMode::DataOnly,
            mtime_unix_ms: None,
            ctime_unix_ms: None,
            kill_suidgid: false,
        },
    };
    Fixture {
        _temp: temp,
        store,
        service,
        local,
        staged,
        commit,
    }
}
async fn schedule_gc(f: &Fixture) -> crate::dfs::GcState {
    let batch = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![f.staged.chunk.clone()],
        )
        .await
        .unwrap();
    assert!(batch.chunks.contains(&f.staged.chunk.id));
    batch
}
#[tokio::test]
async fn gc_fence_normal_real_persistence_commits_without_chunk_registrations() {
    let f = fixture().await;
    let inode = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    assert_eq!(inode.head_version, Some(f.commit.file_version.id));
    let mut bytes = vec![0; f.staged.chunk.length as usize];
    assert_eq!(
        f.local.read_at(&f.staged.chunk.id, 0, &mut bytes).unwrap(),
        bytes.len()
    );
    assert_eq!(bytes, b"fenced body");
}
#[tokio::test]
async fn gc_fence_completed_delete_cannot_reuse_old_receipt_after_list_clears() {
    let f = fixture().await;
    let batch = schedule_gc(&f).await;
    f.local
        .collect_generation(batch.generation, &batch.chunks)
        .unwrap();
    let cleared = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            batch.generation,
            vec![],
        )
        .await
        .unwrap();
    assert!(cleared.chunks.is_empty());
    assert_eq!(cleared.generation, batch.generation);
    let error = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code(), afs_error::META_DFS_GC_CHANGED);
    assert!(
        f.service
            .get_inode(f.commit.inode_id.clone())
            .await
            .unwrap()
            .head_version
            .is_none()
    );
}
#[tokio::test]
async fn gc_fence_old_inflight_delete_rejected_even_with_matching_state_version() {
    let mut f = fixture().await;
    let _batch = schedule_gc(&f).await;
    // Model a deletion already in flight when this write generation began.
    f.commit.gc_fence = Some(f.service.current_gc_fence().await.unwrap());
    assert!(
        f.service
            .commit_file_version("writer".into(), f.commit.clone())
            .await
            .is_err()
    );
    assert!(
        f.service
            .get_inode(f.commit.inode_id)
            .await
            .unwrap()
            .head_version
            .is_none()
    );
}
#[tokio::test]
async fn gc_fence_atomic_condition_rejects_gc_between_check_and_publish() {
    let f = fixture().await;
    let mut txn = MetaTxn::new(
        RequestKey::new("race", "publish"),
        StoreOperation::DfsCommitFileVersion,
    );
    txn.conditions
        .push(TxnCondition::RequestAbsent(txn.request.clone()));
    txn.mutations
        .push(TxnMutation::RecordRequestOutcome(RequestOutcome {
            request: txn.request.clone(),
            operation: StoreOperation::DfsCommitFileVersion,
            result: OperationResult::Empty,
        }));
    txn.conditions.push(f.commit.gc_fence.unwrap().condition());
    txn.mutations
        .push(TxnMutation::Put(MetaEntity::DfsFileVersion(
            f.commit.file_version.clone(),
        )));
    let _batch = schedule_gc(&f).await;
    assert!(matches!(
        f.store.compare_and_commit(txn).await.unwrap(),
        TxnOutcome::ConditionFailed { .. }
    ));
    assert!(
        f.store
            .read(MetaRead::DfsFileVersion(f.commit.file_version.id))
            .await
            .unwrap()
            .entity
            .is_none()
    );
}
#[tokio::test]
async fn gc_fence_publish_first_prevents_later_gc_selecting_chunk() {
    let f = fixture().await;
    f.service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    let batch = f
        .service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![f.staged.chunk.clone()],
        )
        .await
        .unwrap();
    assert!(!batch.chunks.contains(&f.staged.chunk.id));
}
#[tokio::test]
async fn gc_fence_lost_reply_replays_success_even_after_gc_state_changes() {
    let f = fixture().await;
    let original = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    let garbage = StagedChunk::new(OperationId::new("garbage"), b"unreferenced".to_vec());
    f.service
        .collect_garbage(
            "writer".into(),
            "writer-session".into(),
            0,
            vec![garbage.chunk],
        )
        .await
        .unwrap();
    assert_ne!(
        Some(f.service.current_gc_fence().await.unwrap()),
        f.commit.gc_fence
    );
    let replay = f
        .service
        .commit_file_version("writer".into(), f.commit.clone())
        .await
        .unwrap();
    assert_eq!(original, replay);
}
#[test]
fn gc_fence_wire_is_explicit_and_rejects_missing_kind() {
    for fence in [GcFence::Absent, GcFence::Revision(9)] {
        assert_eq!(GcFence::from_wire(fence.wire()).unwrap(), fence);
    }
    assert!(GcFence::from_wire(afs_protocol::meta::DfsGcFence { kind: None }).is_err());
}
