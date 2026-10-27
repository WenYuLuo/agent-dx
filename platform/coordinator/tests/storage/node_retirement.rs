use super::*;
use adx_coordinator::storage::NodeSession;

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn expired_empty_node_is_retired_and_does_not_return_after_restart() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    session
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 7,
                routable: true,
            }),
        )
        .await
        .unwrap();
    session.invalidate_node("old", "boot-one").await.unwrap();
    assert!(!session
        .retire_expired_node("old", "boot-one", 0, 1800)
        .await
        .unwrap());
    assert!(session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
    assert!(!session.snapshot().await.unwrap().nodes.contains_key("old"));
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        1
    );
    let restarted = store.begin(1).await.unwrap();
    assert!(!restarted
        .snapshot()
        .await
        .unwrap()
        .nodes
        .contains_key("old"));
    assert_eq!(
        restarted
            .business_counters()
            .await
            .unwrap()
            .worker_incidents,
        1
    );
}

async fn registered(session: &adx_coordinator::storage::Session, boot: &str) {
    session
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: boot.into(),
                sequence: 7,
                routable: true,
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_preserves_administrative_policy_and_heartbeat_fencing() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(3).await.unwrap();
    registered(&session, "boot-one").await;
    session
        .set_node_scheduling("old", true, false)
        .await
        .unwrap();
    let shard = session.snapshot().await.unwrap().nodes["old"].shard_id;
    session.invalidate_node("old", "boot-one").await.unwrap();
    assert!(session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
    let restarted = store.begin(3).await.unwrap();
    let stale = restarted
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 7,
                routable: false,
            }),
        )
        .await;
    assert!(matches!(stale, Err(Error::Conflict)));
    let restored = restarted
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 8,
                routable: false,
            }),
        )
        .await
        .unwrap();
    assert_eq!(restored.shard_id, shard);
    assert!(restored.scheduling_paused && !restored.node.available);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_waits_for_environment_reference_cleanup() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    let assignment = Assignment {
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
    session
        .commit(running(spec("held"), assignment))
        .await
        .unwrap();
    session.invalidate_node("old", "boot-one").await.unwrap();
    assert!(!session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
    let failed = session.get("held").await.unwrap();
    session
        .retire_invalidated_failed(&failed, u64::MAX)
        .await
        .unwrap();
    assert!(session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_racing_a_new_registration_preserves_the_replacement() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    session.invalidate_node("old", "boot-one").await.unwrap();
    let (retired, new) = tokio::join!(
        session.retire_expired_node("old", "boot-one", u64::MAX, 1800),
        session.register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-two".into(),
                sequence: 1,
                routable: false
            })
        )
    );
    retired.unwrap();
    new.unwrap();
    let saved = session.snapshot().await.unwrap();
    assert_eq!(saved.nodes["old"].session.as_ref().unwrap().id, "boot-two");
    assert!(!session
        .retire_expired_node("old", "boot-one", u64::MAX, 1800)
        .await
        .unwrap());
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn planned_node_departure_is_not_an_incident_and_old_boot_cannot_return() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let session = store.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    session
        .invalidate_node_with_departure("old", "boot-one", true)
        .await
        .unwrap();
    assert!(session
        .retire_expired_node("old", "boot-one", 0, 1800)
        .await
        .unwrap());
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        0
    );
    let restarted = store.begin(1).await.unwrap();
    assert!(restarted
        .is_unregistered_node("old", "boot-one")
        .await
        .unwrap());
    let late = restarted
        .register_session(
            node("old"),
            "127.0.0.1:9000".into(),
            "127.0.0.1:9001".into(),
            Some(NodeSession {
                id: "boot-one".into(),
                sequence: 100,
                routable: false,
            }),
        )
        .await;
    assert!(matches!(late, Err(Error::Conflict)));
    registered(&restarted, "boot-two").await;
    assert!(!restarted
        .is_unregistered_node("old", "boot-one")
        .await
        .unwrap());
    assert!(matches!(
        session
            .retire_expired_node("old", "boot-two", u64::MAX, 1800)
            .await,
        Err(Error::Conflict)
    ));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn node_retirement_new_boot_readiness_closes_the_previous_departure_durably() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    registered(&session, "boot-one").await;
    session
        .invalidate_node_with_departure("old", "boot-one", true)
        .await
        .unwrap();
    for (sequence, routable) in [(1, false), (2, true)] {
        session
            .register_session(
                node("old"),
                "127.0.0.1:9000".into(),
                "127.0.0.1:9001".into(),
                Some(NodeSession {
                    id: "boot-two".into(),
                    sequence,
                    routable,
                }),
            )
            .await
            .unwrap();
    }
    session.invalidate_node("old", "boot-two").await.unwrap();
    assert_eq!(
        session.business_counters().await.unwrap().worker_incidents,
        1
    );
}
