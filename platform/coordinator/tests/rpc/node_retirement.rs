use super::*;
use adx_protocol::control::coordinator_service_server::CoordinatorService;
use adx_transport::rpc::RpcClient;

async fn metrics(rpc: &CoordinatorRpc) -> String {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match rpc.metrics().await {
                Ok(text) => return text,
                Err(adx_core::Error::Unavailable(message))
                    if message == "Coordinator state busy" =>
                {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("metrics failed: {error}"),
            }
        }
    })
    .await
    .unwrap()
}

fn request<T>(value: T, node_id: &str) -> Request<T> {
    let mut r = Request::new(value);
    r.metadata_mut()
        .insert("adx-component", "node".parse().unwrap());
    r.metadata_mut().insert_bin(
        "adx-node-id-bin",
        tonic::metadata::MetadataValue::from_bytes(node_id.as_bytes()),
    );
    r
}
fn report(boot: &str) -> pb::RegisterNodeRequest {
    pb::RegisterNodeRequest {
        node_id: "old".into(),
        node_address: "127.0.0.1:9000".into(),
        proxy_address: "127.0.0.1:9001".into(),
        session_id: boot.into(),
        heartbeat_sequence: 1,
        reconciling: true,
        accepting_allocations: false,
        capacity: Some(spec("x").resources.into()),
        runtime_classes: vec!["runc".into()],
        ..Default::default()
    }
}
async fn service(session: Session) -> CoordinatorRpc {
    CoordinatorRpc::with_heartbeat_timeout(
        session,
        Placement::Pack,
        Peers::network(),
        RpcClient::network(Principal::Coordinator),
        Duration::from_secs(1),
        Duration::from_millis(50),
    )
    .await
    .unwrap()
}
async fn ready(rpc: &CoordinatorRpc, boot: &str) {
    rpc.register_node(request(report(boot), "old"))
        .await
        .unwrap();
    rpc.inspect_node(request(
        pb::InspectNodeRequest {
            node_id: "old".into(),
            session_id: boot.into(),
        },
        "old",
    ))
    .await
    .unwrap();
    let mut r = report(boot);
    r.reconciling = false;
    r.accepting_allocations = true;
    r.heartbeat_sequence = 2;
    rpc.register_node(request(r, "old")).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn node_expiry_retirement_removes_metrics_without_reaping_healthy_nodes() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_nanos(1))
            .await
            .unwrap(),
        0
    );
    tokio::time::sleep(Duration::from_millis(80)).await;
    rpc.expire_nodes().await.unwrap();
    assert!(metrics(&rpc).await.contains("node_id=\"old\""));
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_secs(1800))
            .await
            .unwrap(),
        0
    );
    let since = session.snapshot().await.unwrap().nodes["old"]
        .retirement
        .as_ref()
        .unwrap()
        .since_unix_seconds;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    assert_eq!(
        session.snapshot().await.unwrap().nodes["old"]
            .retirement
            .as_ref()
            .unwrap()
            .since_unix_seconds,
        since
    );
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_nanos(1))
            .await
            .unwrap(),
        1
    );
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
    let publisher = RoutePublisher::new(session, Peers::network());
    publisher.refresh().await.unwrap();
    let restarted = service(store.begin(1).await.unwrap()).await;
    assert!(!metrics(&restarted).await.contains("node_id=\"old\""));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn unregister_node_is_authenticated_idempotent_and_cannot_remove_replacement() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    let payload = pb::UnregisterNodeRequest {
        node_id: "old".into(),
        session_id: "boot-one".into(),
    };
    assert_eq!(
        rpc.unregister_node(request(payload.clone(), "other"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    rpc.unregister_node(request(payload.clone(), "old"))
        .await
        .unwrap();
    rpc.unregister_node(request(payload.clone(), "old"))
        .await
        .unwrap();
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
    drop(rpc);
    let rpc = service(store.begin(1).await.unwrap()).await;
    assert_eq!(
        rpc.register_node(request(report("boot-one"), "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    ready(&rpc, "boot-two").await;
    assert_eq!(
        rpc.unregister_node(request(payload, "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(metrics(&rpc).await.contains("node_id=\"old\""));
    let rpc = service(store.begin(1).await.unwrap()).await;
    assert_eq!(
        rpc.register_node(request(report("boot-one"), "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert!(metrics(&rpc).await.contains("node_id=\"old\""));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn unregister_node_preserves_recovery_context_until_failed_environment_is_collected() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    let assignment = adx_core::Assignment {
        environment_id: "held".into(),
        node_id: "old".into(),
        shard_id: 0,
        generation: session.epoch() << 32 | 1,
        devices: vec![],
    };
    session
        .reserve(spec("held"), assignment.clone())
        .await
        .unwrap();
    let record = EnvironmentRecord {
        observation: None,
        execution_artifacts: vec![],
        snapshot_source: None,
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        spec: spec("held"),
        assignment,
        state: adx_core::EnvironmentState::Running,
        revision: 2,
        runtime: adx_core::Runtime {
            id: format!("held-{}", session.epoch() << 32 | 1),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    };
    session.commit(record).await.unwrap();
    rpc.unregister_node(request(
        pb::UnregisterNodeRequest {
            node_id: "old".into(),
            session_id: "boot-one".into(),
        },
        "old",
    ))
    .await
    .unwrap();
    let saved = session.get("held").await.unwrap();
    assert!(saved.invalidated && !saved.resources_held());
    assert!(session.snapshot().await.unwrap().nodes.contains_key("old"));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_nanos(1))
            .await
            .unwrap(),
        0
    );
    let rpc = service(store.begin(1).await.unwrap()).await;
    assert_eq!(
        rpc.register_node(request(report("boot-one"), "old"))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(rpc.collect_failed(Duration::ZERO).await.unwrap(), 1);
    assert_eq!(
        rpc.collect_retired_nodes(Duration::from_secs(1800))
            .await
            .unwrap(),
        1
    );
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py control-rpc"]
async fn unregister_node_finishes_after_its_rpc_caller_disconnects() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let rpc = service(session.clone()).await;
    ready(&rpc, "boot-one").await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(200)
        .arg("WRITE")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let service = rpc.clone();
    let caller = tokio::spawn(async move {
        service
            .unregister_node(request(
                pb::UnregisterNodeRequest {
                    node_id: "old".into(),
                    session_id: "boot-one".into(),
                },
                "old",
            ))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!caller.is_finished());
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while session.snapshot().await.unwrap().nodes.contains_key("old") {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cancelled caller abandoned node retirement");
    assert!(!metrics(&rpc).await.contains("node_id=\"old\""));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
}
