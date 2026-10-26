use super::*;
use std::sync::Arc;

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn initial_success_is_atomic_by_environment_or_request_and_survives_restart() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let make = |id: &str, generation| {
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
    };
    let first = make("first", 1);
    let other = make("other", 2);
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let one = {
        let session = session.clone();
        let barrier = barrier.clone();
        let record = first.clone();
        async move {
            barrier.wait().await;
            session
                .publish_initial(record, "request", "")
                .await
                .unwrap()
        }
    };
    let two = {
        let session = session.clone();
        let record = other;
        async move {
            barrier.wait().await;
            session
                .publish_initial(record, "request", "")
                .await
                .unwrap()
        }
    };
    let (one, two) = tokio::join!(one, two);
    assert_eq!(one, two);
    assert_eq!(session.snapshot().await.unwrap().environments.len(), 1);
    let mut duplicate = one.clone();
    duplicate.assignment.generation += 10;
    duplicate.runtime.id = format!("{}-{}", duplicate.spec.id, duplicate.assignment.generation);
    assert_eq!(
        session
            .publish_initial(duplicate, "another-request", "")
            .await
            .unwrap(),
        one
    );
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        recovered
            .published_request("tenant", "request")
            .await
            .unwrap(),
        Some(one.clone())
    );
    assert_eq!(
        recovered
            .published_request("tenant", "another-request")
            .await
            .unwrap(),
        Some(one)
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn independent_environment_commits_do_not_retry_a_global_version() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut records = vec![];
    for generation in 1..=16 {
        let id = format!("independent-{generation}");
        let record = running(
            spec(&id),
            Assignment {
                environment_id: id,
                node_id: "node".into(),
                shard_id: 0,
                generation,
                devices: vec![],
            },
        );
        session
            .publish_initial(record.clone(), "", "")
            .await
            .unwrap();
        records.push(record);
    }
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let calls = |info: &str| -> u64 {
        info.lines()
            .find_map(|line| {
                line.strip_prefix("cmdstat_eval:calls=")
                    .and_then(|s| s.split(',').next())
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(0)
    };
    let before: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut connection)
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(records.len()));
    let mut tasks = vec![];
    for mut record in records {
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            record.revision += 1;
            barrier.wait().await;
            session.commit(record).await.unwrap();
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let after: String = redis::cmd("INFO")
        .arg("commandstats")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(
        calls(&after) - calls(&before),
        16,
        "unrelated records must each commit in one CAS attempt"
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn new_epoch_rejects_delayed_initial_publication_without_a_pending_record() {
    let rig = common::Redis::new().await;
    let old = rig.store().await.begin(1).await.unwrap();
    register(&old, "node").await;
    let record = running(
        spec("lost"),
        Assignment {
            environment_id: "lost".into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
    );
    let current = rig.store().await.begin(1).await.unwrap();
    assert!(old
        .publish_initial(record.clone(), "request", "")
        .await
        .is_err());
    assert!(current.snapshot().await.unwrap().environments.is_empty());
    current
        .publish_initial(record.clone(), "request", "")
        .await
        .unwrap();
    assert_eq!(
        current
            .published_request("tenant", "request")
            .await
            .unwrap(),
        Some(record)
    );
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn same_environment_racing_requests_reuse_the_winner_after_crash() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    register(&session, "node").await;
    let mut tasks = vec![];
    let barrier = Arc::new(tokio::sync::Barrier::new(12));
    for index in 0..12 {
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let mut candidate = spec("shared");
            candidate.image = format!("image-{index}");
            let record = running(
                candidate,
                Assignment {
                    environment_id: "shared".into(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: index + 1,
                    devices: vec![],
                },
            );
            barrier.wait().await;
            session
                .publish_initial(record, &format!("request-{index}"), "")
                .await
                .unwrap()
        }));
    }
    let mut winners = vec![];
    for task in tasks {
        winners.push(task.await.unwrap());
    }
    assert!(winners.iter().all(|winner| winner == &winners[0]));
    rig.crash();
    rig.start().await;
    let recovered = rig.store().await.begin(1).await.unwrap();
    for index in 0..12 {
        assert_eq!(
            recovered
                .published_request("tenant", &format!("request-{index}"))
                .await
                .unwrap(),
            Some(winners[0].clone())
        );
    }
    let mut deleted = winners[0].clone();
    deleted.state = EnvironmentState::Deleted;
    deleted.resources_held = false;
    deleted.runtime.ip = None;
    deleted.revision += 2;
    recovered.commit(deleted).await.unwrap();
    let receipt = recovered
        .published_request("tenant", "request-0")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.state, EnvironmentState::Deleted);
    assert!(!receipt.resources_held);
    assert_eq!(recovered.get("shared").await, Err(Error::NotFound));
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn node_expiry_cannot_miss_concurrent_initial_publications() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    session
        .register_session(
            node("node"),
            "127.0.0.1:1".into(),
            "127.0.0.1:2".into(),
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence: 1,
                routable: true,
            }),
        )
        .await
        .unwrap();
    let barrier = Arc::new(tokio::sync::Barrier::new(17));
    let mut tasks = vec![];
    for index in 0..16 {
        let session = session.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let id = format!("racing-{index}");
            let record = running(
                spec(&id),
                Assignment {
                    environment_id: id.clone(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation: index + 1,
                    devices: vec![],
                },
            );
            barrier.wait().await;
            session.publish_initial(record, &id, "boot").await
        }));
    }
    barrier.wait().await;
    session.invalidate_node("node", "boot").await.unwrap();
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) | Err(Error::Conflict) => (),
            Err(error) => panic!("{error:?}"),
        }
    }
    let saved = session.snapshot().await.unwrap();
    assert!(!saved.nodes["node"].node.available);
    for environment in saved.environments.values() {
        assert!(
            environment.invalidated,
            "expiry omitted an initial publication"
        );
        assert_eq!(
            environment.result.as_ref().unwrap().state,
            EnvironmentState::Failed
        );
        assert!(!environment.resources_held());
    }
}

#[tokio::test]
#[ignore = "requires isolated Redis; run storage suite"]
async fn returning_node_rearms_publication_after_reconciliation() {
    for initially_routable in [false, true] {
        let rig = common::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        let report = |sequence, routable| {
            Some(adx_coordinator::storage::NodeSession {
                id: "boot".into(),
                sequence,
                routable,
            })
        };
        let mut initial_node = node("node");
        initial_node.available = initially_routable;
        session
            .register_session(
                initial_node,
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(1, initially_routable),
            )
            .await
            .unwrap();
        session
            .register_session(
                node("node"),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(2, true),
            )
            .await
            .unwrap();
        session.invalidate_node("node", "boot").await.unwrap();
        let record = running(
            spec("after-return"),
            Assignment {
                environment_id: "after-return".into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        );
        assert_eq!(
            session
                .publish_initial(record.clone(), "request", "boot")
                .await
                .unwrap_err(),
            Error::Conflict
        );
        session
            .register_session(
                node("node"),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(3, false),
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .publish_initial(record.clone(), "request", "boot")
                .await
                .unwrap_err(),
            Error::Conflict
        );
        session
            .register_session(
                node("node"),
                "127.0.0.1:1".into(),
                "127.0.0.1:2".into(),
                report(4, true),
            )
            .await
            .unwrap();
        assert_eq!(
            session
                .publish_initial(record.clone(), "request", "boot")
                .await
                .unwrap(),
            record
        );
    }
}
