use super::*;
use adx_coordinator::storage::BusinessCounters;

async fn business_fields(rig: &common::Redis) -> Vec<String> {
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    redis::cmd("HKEYS")
        .arg("adx:{test}:control:v1:business")
        .query_async(&mut connection)
        .await
        .unwrap()
}

async fn create(
    session: &adx_coordinator::storage::Session,
    id: &str,
    generation: u64,
) -> EnvironmentRecord {
    let assignment = Assignment {
        environment_id: id.into(),
        node_id: "node".into(),
        shard_id: 0,
        generation,
        devices: vec![],
    };
    session.reserve(spec(id), assignment.clone()).await.unwrap();
    let record = running(spec(id), assignment);
    session.commit(record.clone()).await.unwrap();
    record
}

fn deletion(mut record: EnvironmentRecord) -> EnvironmentRecord {
    record.state = EnvironmentState::Deleted;
    record.resources_held = false;
    record.runtime.ip = None;
    record.revision += 1;
    record
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn retired_counter_storage_is_bounded_by_live_objects_and_fixed_buckets() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    for index in 0..512 {
        let record = create(&session, &format!("churn-{index}"), index + 1).await;
        session.commit(deletion(record)).await.unwrap();
    }
    assert!(session.snapshot().await.unwrap().environments.is_empty());
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [512, 0, 0]
    );
    let fields = business_fields(&rig).await;
    assert!(
        fields.len() <= 64,
        "historical counters grew to {} fields",
        fields.len()
    );
    assert!(fields.iter().all(|field| field.starts_with("retired:")));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn concurrent_retirement_and_delete_replay_preserve_counts_across_redis_restart() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut records = Vec::new();
    for index in 0..96 {
        records.push(create(&session, &format!("parallel-{index}"), index + 1).await);
    }
    for chunk in records.chunks(16) {
        let jobs = chunk.iter().map(|record| {
            let session = session.clone();
            let deleted = deletion(record.clone());
            async move {
                session.commit(deleted.clone()).await.unwrap();
                session.commit(deleted).await.unwrap();
            }
        });
        futures_util::future::join_all(jobs).await;
    }
    let expected = session.business_counters().await.unwrap();
    assert_eq!(expected.creations, [96, 0, 0]);
    assert!(business_fields(&rig).await.len() <= 64);
    rig.crash();
    rig.start().await;
    let restarted = rig.store().await.begin(1).await.unwrap();
    assert_eq!(restarted.business_counters().await.unwrap(), expected);
    assert!(restarted.snapshot().await.unwrap().environments.is_empty());
    assert!(business_fields(&rig).await.len() <= 64);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn recreated_name_counts_each_generation_once_and_fences_old_deletion() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut previous: Option<EnvironmentRecord> = None;
    for generation in 1..=32 {
        let record = create(&session, "reused", generation).await;
        if let Some(old) = previous.as_ref() {
            assert!(session.commit(old.clone()).await.is_err());
        }
        let deleted = deletion(record);
        session.commit(deleted.clone()).await.unwrap();
        session.commit(deleted.clone()).await.unwrap();
        previous = Some(deleted);
        assert_eq!(
            session.business_counters().await.unwrap().creations,
            [generation, 0, 0]
        );
        assert!(business_fields(&rig)
            .await
            .iter()
            .all(|field| field.starts_with("retired:")));
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn startup_compacts_orphaned_history_without_losing_live_or_node_facts() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    register(&session, "node").await;
    create(&session, "live", 1).await;
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    for index in 0..192 {
        let (field, value) = if index % 2 == 0 {
            (
                format!("environment:old-{index}"),
                BusinessCounters {
                    creations: [1, 2, 3],
                    ..Default::default()
                },
            )
        } else {
            (
                format!("node:old-{index}"),
                BusinessCounters {
                    worker_incidents: 1,
                    ..Default::default()
                },
            )
        };
        redis::cmd("HSET")
            .arg("adx:{test}:control:v1:business")
            .arg(field)
            .arg(serde_json::to_string(&value).unwrap())
            .query_async::<()>(&mut connection)
            .await
            .unwrap();
    }
    let expected = session.business_counters().await.unwrap();
    let restarted = store.begin(1).await.unwrap();
    assert_eq!(restarted.business_counters().await.unwrap(), expected);
    assert_eq!(expected.creations, [97, 192, 288]);
    assert_eq!(expected.worker_incidents, 96);
    assert!(restarted.get("live").await.is_ok());
    let fields = business_fields(&rig).await;
    assert!(fields.len() <= 65);
    assert_eq!(
        fields
            .iter()
            .filter(|field| field.starts_with("environment:"))
            .count(),
        1
    );
    assert!(fields
        .iter()
        .all(|field| field == "environment:live" || field.starts_with("retired:")));
    let again = store.begin(1).await.unwrap();
    assert_eq!(again.business_counters().await.unwrap(), expected);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_incident_replay_uses_bounded_retired_buckets() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    for index in 0..96 {
        let id = format!("node-{index}");
        session
            .register_session(
                node(&id),
                "node:9000".into(),
                "node:9001".into(),
                Some(adx_coordinator::storage::NodeSession {
                    id: "boot".into(),
                    sequence: 1,
                    routable: true,
                }),
            )
            .await
            .unwrap();
        session.invalidate_node(&id, "boot").await.unwrap();
        session.invalidate_node(&id, "boot").await.unwrap();
    }
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        96
    );
    assert!(business_fields(&rig).await.len() <= 64);
    assert!(business_fields(&rig)
        .await
        .iter()
        .all(|field| field.starts_with("retired:")));
    assert_eq!(
        store
            .begin(1)
            .await
            .unwrap()
            .business_counters()
            .await
            .unwrap()
            .worker_incidents,
        96
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn retirement_keeps_u64_precision_and_overflow_leaves_lifecycle_unchanged() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let first = create(&session, "large-count", 1).await;
    session.commit(deletion(first)).await.unwrap();
    let fields = business_fields(&rig).await;
    assert_eq!(fields.len(), 1);
    let mut connection = redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let seeded = BusinessCounters {
        creations: [u64::MAX - 1, 0, 0],
        ..Default::default()
    };
    redis::cmd("HSET")
        .arg("adx:{test}:control:v1:business")
        .arg(&fields[0])
        .arg(serde_json::to_string(&seeded).unwrap())
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let second = create(&session, "large-count", 2).await;
    session.commit(deletion(second)).await.unwrap();
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [u64::MAX, 0, 0]
    );
    let third = create(&session, "large-count", 3).await;
    assert_eq!(
        session.commit(deletion(third.clone())).await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session.get("large-count").await.unwrap().result,
        Some(third)
    );
    let raw: String = redis::cmd("HGET")
        .arg("adx:{test}:control:v1:business")
        .arg(&fields[0])
        .query_async(&mut connection)
        .await
        .unwrap();
    let retained: BusinessCounters = serde_json::from_str(&raw).unwrap();
    assert_eq!(retained.creations, [u64::MAX, 0, 0]);
}
