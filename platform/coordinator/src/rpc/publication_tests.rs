use super::*;

#[tokio::test]
#[ignore = "requires isolated Redis; run control-rpc suite"]
async fn retry_cannot_remove_a_success_being_published() {
    publication_race("retry").await;
}
#[tokio::test]
#[ignore = "requires isolated Redis; run control-rpc suite"]
async fn cleanup_cannot_remove_a_success_being_published() {
    publication_race("cleanup").await;
}
#[tokio::test]
#[ignore = "requires isolated Redis; run control-rpc suite"]
async fn publication_restores_a_rebuilt_catalog() {
    publication_race("rebuild").await;
}
async fn publication_race(action: &str) {
    let redis = cancellation_test_redis::Redis::new().await;
    let store = crate::storage::RedisStore::connect(&redis.url, "test", Duration::from_secs(5))
        .await
        .unwrap();
    let session = store.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let spec:EnvironmentSpec=serde_json::from_value(serde_json::json!({
            "id":"review-race","tenant_id":"tenant","image":"image","runtime_class":"runsc","priority":0,
            "resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0}
        })).unwrap();
    let node = Node {
        id: "node".into(),
        capacity: spec.resources,
        available: true,
        labels: Default::default(),
        devices: vec![],
        runtime_classes: vec!["runsc".into()],
    };
    let saved = session
        .register_session(
            node.clone(),
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            Some(NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let assignment = {
        let mut state = rpc.0.state.lock().await;
        state.scheduler.register(node).unwrap();
        state.nodes.insert("node".into(), saved);
        state.live.insert(
            "node".into(),
            LiveNode {
                expired: false,
                session: "boot".into(),
                sequence: 1,
                last_seen: tokio::time::Instant::now(),
                inspected: true,
                report: pb::RegisterNodeRequest::default(),
            },
        );
        state.scheduler.submit(spec.clone()).unwrap();
        state.specs.insert(spec.id.clone(), spec.clone());
        state
            .create_requests
            .insert(spec.id.clone(), ["review-request".into()].into());
        state.drive().await.unwrap();
        state.environments[&spec.id].assignment.clone()
    };
    let record = EnvironmentRecord {
        spec: spec.clone(),
        assignment: assignment.clone(),
        state: EnvironmentState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: format!("{}-{}", spec.id, assignment.generation),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
        restart_attempts: 0,
        restart_pending: false,
        failure: None,
        snapshot_source: None,
        execution_artifacts: vec![],
    };
    let client = redis::Client::open(redis.url.as_str()).unwrap();
    let mut control = client.get_multiplexed_async_connection().await.unwrap();
    let _: String = redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(3000)
        .arg("WRITE")
        .query_async(&mut control)
        .await
        .unwrap();
    let committing = tokio::spawn({
        let rpc = rpc.clone();
        let record = record.clone();
        async move { rpc.commit(record, "boot".into()).await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !committing.is_finished(),
        "publication must be blocked in Redis"
    );
    assert!(
        rpc.0
            .create_locks
            .lock()
            .unwrap()
            .get("publication:review-race")
            .and_then(std::sync::Weak::upgrade)
            .is_some(),
        "commit owns publication lock"
    );
    // Same path as a broken StartAssigned RPC while the node is publishing success.
    let retrying = tokio::spawn({
        let rpc = rpc.clone();
        let id = spec.id.clone();
        let assignment = assignment.clone();
        let spec = spec.clone();
        let action = action.to_owned();
        async move {
            if action == "cleanup" {
                rpc.discard_unconfirmed(&spec).await;
            } else if action == "rebuild" {
                let mut state = rpc.0.state.lock().await;
                state.scheduler.retry(&assignment).unwrap();
                state.environments.remove(&id);
            } else {
                rpc.retry_initial(&id, &assignment).await?;
            }
            Ok::<_, Status>(())
        }
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    if action != "rebuild" {
        assert!(!retrying.is_finished(), "cleanup must wait for publication");
    }
    let _: String = redis::cmd("CLIENT")
        .arg("UNPAUSE")
        .query_async(&mut control)
        .await
        .unwrap();
    assert_eq!(committing.await.unwrap().unwrap(), record);
    retrying.await.unwrap().unwrap();
    assert_eq!(
        session.get(&spec.id).await.unwrap().result,
        Some(record.clone())
    );
    let state = rpc.0.state.lock().await;
    assert_eq!(
        state
            .environments
            .get(&spec.id)
            .and_then(|entry| entry.result.as_ref()),
        Some(&record),
        "successful durable execution must remain in the authoritative reconciliation directory"
    );
    assert!(
        state
            .scheduler
            .snapshot()
            .environments()
            .contains_key(&spec.id),
        "published execution must retain its scheduling reservation"
    );
    drop(state);
    if action == "retry" {
        // A publication whose reply was lost is first discovered by retry.
        rpc.0
            .state
            .lock()
            .await
            .environments
            .get_mut(&spec.id)
            .unwrap()
            .result = None;
        let changed = rpc.0.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        rpc.retry_initial(&spec.id, &assignment).await.unwrap();
        tokio::time::timeout(Duration::from_millis(100), changed)
            .await
            .expect("observed success must wake a create waiting for node readiness");
    }
}
