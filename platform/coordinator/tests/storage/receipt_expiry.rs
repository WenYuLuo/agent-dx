use super::*;
use sha2::{Digest, Sha256};
use std::time::Duration;

fn receipt_key(tenant: &str, request: &str) -> String {
    let identity = serde_json::to_string(&(tenant, request)).unwrap();
    format!(
        "adx:{{test}}:control:v1:create-receipt:{:x}",
        Sha256::digest(identity.as_bytes())
    )
}

fn record(id: &str, generation: u64) -> EnvironmentRecord {
    running(
        spec(id),
        Assignment {
            environment_id: id.into(),
            node_id: "node".into(),
            shard_id: 0,
            generation,
            devices: vec![],
        },
    )
}

async fn connection(rig: &common::Redis) -> redis::aio::MultiplexedConnection {
    redis::Client::open(rig.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

async fn ttl(redis: &mut redis::aio::MultiplexedConnection, key: &str) -> i64 {
    redis::cmd("PTTL")
        .arg(key)
        .query_async(redis)
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn receipt_has_ten_minute_ttl_and_replay_does_not_extend_it() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let original = record("first", 1);
    session
        .publish_initial(original.clone(), "request", "")
        .await
        .unwrap();
    let mut redis = connection(&rig).await;
    let key = receipt_key("tenant", "request");
    assert!((590_000..=600_000).contains(&ttl(&mut redis, &key).await));
    // Accelerate only the test deadline; production TTL is asserted above.
    let _: bool = redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(3000)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        session
            .published_request("tenant", "request")
            .await
            .unwrap(),
        Some(original.clone())
    );
    assert_eq!(
        session
            .publish_initial(record("loser", 2), "request", "")
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        session
            .publish_initial(record("first", 3), "alias", "")
            .await
            .unwrap(),
        original
    );
    assert!((1..=3000).contains(&ttl(&mut redis, &key).await));
    assert!((590_000..=600_000).contains(&ttl(&mut redis, &receipt_key("tenant", "alias")).await));
    let obsolete: u64 = redis::cmd("EXISTS")
        .arg("adx:{test}:control:v1:create-requests")
        .arg("adx:{test}:control:v1:create-receipts")
        .query_async(&mut redis)
        .await
        .unwrap();
    assert_eq!(
        obsolete, 0,
        "publication must not leave permanent request indexes"
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn receipt_expires_during_redis_restart_without_deleting_live_environment() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let original = record("live", 1);
    session
        .publish_initial(original.clone(), "request", "")
        .await
        .unwrap();
    let mut redis = connection(&rig).await;
    let key = receipt_key("tenant", "request");
    assert!((590_000..=600_000).contains(&ttl(&mut redis, &key).await));
    let _: bool = redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(100)
        .query_async(&mut redis)
        .await
        .unwrap();
    rig.crash();
    tokio::time::sleep(Duration::from_millis(150)).await;
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    let mut redis = connection(&rig).await;
    assert_eq!(
        ttl(&mut redis, &key).await,
        -2,
        "expired AOF entries must not revive"
    );
    assert!(recovered
        .published_request("tenant", "request")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        recovered.get("live").await.unwrap().result,
        Some(original.clone())
    );
    assert_eq!(
        recovered
            .publish_initial(record("live", 2), "request", "")
            .await
            .unwrap(),
        original
    );
    assert_eq!(
        recovered.business_counters().await.unwrap().creations,
        [1, 0, 0]
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn expired_deleted_receipt_allows_one_new_winner_and_preserves_other_tenants() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut first = record("old", 1);
    session
        .publish_initial(first.clone(), "request", "")
        .await
        .unwrap();
    let mut other = record("other-tenant", 2);
    other.spec.tenant_id = "other".into();
    session
        .publish_initial(other.clone(), "request", "")
        .await
        .unwrap();
    first.state = EnvironmentState::Deleted;
    first.resources_held = false;
    first.runtime.ip = None;
    first.revision += 1;
    session.commit(first).await.unwrap();
    assert_eq!(
        session
            .published_request("tenant", "request")
            .await
            .unwrap()
            .unwrap()
            .state,
        EnvironmentState::Deleted
    );
    let mut redis = connection(&rig).await;
    let key = receipt_key("tenant", "request");
    let changed: bool = redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(1)
        .query_async(&mut redis)
        .await
        .unwrap();
    assert!(changed);
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (left, right) = tokio::join!(
        session.publish_initial(record("new-left", 3), "request", ""),
        session.publish_initial(record("new-right", 4), "request", "")
    );
    let winner = left.unwrap();
    assert_eq!(winner, right.unwrap());
    assert_ne!(winner.spec.id, "old");
    assert_eq!(
        session.published_request("other", "request").await.unwrap(),
        Some(other)
    );
    assert_eq!(
        session.business_counters().await.unwrap().creations,
        [3, 0, 0]
    );
    assert!((590_000..=600_000).contains(&ttl(&mut redis, &key).await));
}
