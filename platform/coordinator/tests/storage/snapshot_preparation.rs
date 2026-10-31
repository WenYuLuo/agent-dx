use super::*;
use adx_core::snapshot_preparation::PreparationState;

fn resources() -> Resources {
    Resources {
        cpu_millis: 1000,
        memory_bytes: 1024,
        disk_bytes: 0,
    }
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn preparation_registration_is_serial_and_preserves_family_identity() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (first, build) = session
        .register_snapshot_preparation("tenant", " Image ", resources())
        .await
        .unwrap();
    assert!(
        matches!(session.register_snapshot_preparation("tenant", "image", resources()).await,
        Err(Error::Invalid(message)) if message.contains("busy"))
    );
    session
        .update_snapshot_template(&first.id, |template| {
            template.preparation.state = PreparationState::Failed;
            Ok(())
        })
        .await
        .unwrap();
    let (again, second) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert_eq!(first.id, again.id);
    assert_ne!(build.id, second.id);
    assert_eq!(again.preparation.id, second.id);
    assert_eq!(again.revision, 3);
    assert_eq!(
        session
            .get_snapshot_template("tenant", "IMAGE")
            .await
            .unwrap(),
        again
    );
    assert_eq!(
        session.get_snapshot_template("other", &again.id).await,
        Err(Error::NotFound)
    );
    assert!(session
        .list_snapshot_templates("other")
        .await
        .unwrap()
        .is_empty());
    session
        .update_snapshot_template(&again.id, |template| {
            template.deleted = true;
            Ok(())
        })
        .await
        .unwrap();
    let (replacement, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert_ne!(replacement.id, again.id);
    assert_eq!(
        session
            .get_snapshot_template("tenant", "image")
            .await
            .unwrap()
            .id,
        replacement.id
    );
    assert!(
        session
            .get_snapshot_template("tenant", &again.id)
            .await
            .unwrap()
            .deleted
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn concurrent_registration_admits_one_build_and_cas_updates_preserve_every_commit() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        tasks.push(tokio::spawn(async move {
            session
                .register_snapshot_preparation("tenant", "image", resources())
                .await
        }));
    }
    let mut accepted = Vec::new();
    for task in tasks {
        match task.await.unwrap() {
            Ok((template, _)) => accepted.push(template),
            Err(Error::Invalid(message)) => assert!(message.contains("busy")),
            other => panic!("unexpected registration result: {other:?}"),
        }
    }
    assert_eq!(accepted.len(), 1);
    let id = accepted.pop().unwrap().id;
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let session = session.clone();
        let id = id.clone();
        tasks.push(tokio::spawn(async move {
            session
                .update_snapshot_template(&id, |template| {
                    template
                        .preparation
                        .error
                        .get_or_insert_with(String::new)
                        .push('x');
                    template.preparation.state = PreparationState::Failed;
                    Ok(())
                })
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        task.await.unwrap();
    }
    let template = session.get_snapshot_template("tenant", &id).await.unwrap();
    assert_eq!(template.revision, 9);
    assert_eq!(template.preparation.error.as_deref(), Some("xxxxxxxx"));
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn snapshot_preparation_persists_across_restart_and_fences_the_previous_epoch() {
    let mut rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (template, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    rig.crash();
    rig.start().await;
    let active = rig.store().await.begin(1).await.unwrap();
    assert_eq!(
        active
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap(),
        template
    );
    // The old connection manager reconnects asynchronously after the Redis crash.
    // Require an actual epoch rejection once transport recovers, not Unavailable.
    let stale_update = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let result = session
                .update_snapshot_template(&template.id, |_| Ok(()))
                .await;
            if matches!(result, Err(Error::Unavailable(_))) {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            break result;
        }
    })
    .await
    .expect("stale client reconnects within the test deadline");
    assert_eq!(stale_update, Err(Error::Conflict));
    assert_eq!(
        session
            .remove_finished_snapshot_template(&template.id)
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .register_snapshot_preparation("tenant", "other", resources())
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        active.list_snapshot_templates("tenant").await.unwrap(),
        vec![template]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn finished_tombstone_removal_waits_for_work_and_preserves_a_reused_name() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (template, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert!(!session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    let spec: adx_core::EnvironmentSpec = serde_json::from_value(serde_json::json!({
        "id":"seed", "tenant_id":"tenant", "image":"image", "runtime_class":"runsc",
        "priority":0, "resources":{"cpu_millis":1000,"memory_bytes":1024,"disk_bytes":0}
    }))
    .unwrap();
    for phase in [
        PreparationState::Creating,
        PreparationState::Snapshotting,
        PreparationState::Cleaning,
    ] {
        session
            .update_snapshot_template(&template.id, |template| {
                template.deleted = true;
                let build = &mut template.preparation;
                build.state = phase;
                build.spec = Some(spec.clone());
                build.expected_revision = Some(1);
                build.snapshot_id = Some("independent-golden".into());
                Ok(())
            })
            .await
            .unwrap();
        assert!(!session
            .remove_finished_snapshot_template(&template.id)
            .await
            .unwrap());
    }
    let (replacement, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&template.id, |template| {
            let build = &mut template.preparation;
            build.state = PreparationState::Failed;
            build.cleanup_pending = true;
            Ok(())
        })
        .await
        .unwrap();
    assert!(!session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    session
        .update_snapshot_template(&template.id, |template| {
            template.preparation.cleanup_pending = false;
            Ok(())
        })
        .await
        .unwrap();
    assert!(session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    assert!(!session
        .remove_finished_snapshot_template(&template.id)
        .await
        .unwrap());
    assert_eq!(
        session.get_snapshot_template("tenant", &template.id).await,
        Err(Error::NotFound)
    );
    assert_eq!(
        session
            .get_snapshot_template("tenant", "image")
            .await
            .unwrap(),
        replacement
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn deleted_preparation_cleanup_cannot_steal_a_reused_family_name() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (first, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&first.id, |template| {
            template.deleted = true;
            Ok(())
        })
        .await
        .unwrap();
    let (replacement, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&first.id, |template| {
            template.preparation.state = PreparationState::Failed;
            template.preparation.error = Some("cleaned".into());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        session
            .get_snapshot_template("tenant", "image")
            .await
            .unwrap(),
        replacement
    );
    assert_eq!(
        session
            .update_snapshot_template(&first.id, |template| {
                template.deleted = false;
                Ok(())
            })
            .await,
        Err(Error::Conflict)
    );
    assert_eq!(
        session
            .update_snapshot_template(&replacement.id, |template| {
                template.name = "renamed".into();
                Ok(())
            })
            .await,
        Err(Error::Conflict)
    );
    let (other, _) = session
        .register_snapshot_preparation("other", "image", resources())
        .await
        .unwrap();
    assert_ne!(other.id, replacement.id);
    assert_eq!(
        session.list_snapshot_templates("other").await.unwrap(),
        vec![other]
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn template_record_limit_fails_without_claiming_an_active_name() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let client = redis::Client::open(rig.url.as_str()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let mut command = redis::cmd("HSET");
    command.arg("adx:{test}:control:v1:snapshot-templates");
    for index in 0..4096 {
        command.arg(format!("reserved-{index}")).arg("{}");
    }
    command.query_async::<()>(&mut connection).await.unwrap();
    assert!(matches!(
        session
            .register_snapshot_preparation("tenant", "blocked", resources())
            .await,
        Err(Error::Invalid(_))
    ));
    let count: u64 = redis::cmd("HLEN")
        .arg("adx:{test}:control:v1:snapshot-template-names")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn invalid_preparation_state_does_not_advance_the_persisted_revision() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (template, _) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    assert!(matches!(
        session
            .update_snapshot_template(&template.id, |template| {
                template.preparation.state = PreparationState::Ready;
                Ok(())
            })
            .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        session
            .get_snapshot_template("tenant", &template.id)
            .await
            .unwrap(),
        template
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; ADX_TEST_REDIS_SERVER"]
async fn rebuilds_retain_only_current_and_latest_successful_without_a_generation_limit() {
    let rig = common::Redis::new().await;
    let session = rig.store().await.begin(1).await.unwrap();
    let (first, successful) = session
        .register_snapshot_preparation("tenant", "image", resources())
        .await
        .unwrap();
    session
        .update_snapshot_template(&first.id, |template| {
            template.preparation.state = PreparationState::Ready;
            template.preparation.snapshot_id = Some("native-golden".into());
            Ok(())
        })
        .await
        .unwrap();
    for _ in 0..300 {
        let (template, build) = session
            .register_snapshot_preparation("tenant", "image", resources())
            .await
            .unwrap();
        assert_eq!(template.id, first.id);
        assert_ne!(build.id, successful.id);
        assert_eq!(
            template.latest_successful.as_ref().unwrap().preparation_id,
            successful.id
        );
        session
            .update_snapshot_template(&template.id, |template| {
                template.preparation.state = PreparationState::Failed;
                Ok(())
            })
            .await
            .unwrap();
    }
    let template = session
        .get_snapshot_template("tenant", &first.id)
        .await
        .unwrap();
    assert_eq!(
        template.latest_successful.as_ref().unwrap().snapshot_id,
        "native-golden"
    );
    let updated = session
        .update_snapshot_template(&first.id, |template| {
            template.preparation.state = PreparationState::Ready;
            template.preparation.snapshot_id = Some("new-native-golden".into());
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        updated.latest_successful.as_ref().unwrap().preparation_id,
        updated.preparation.id
    );
    assert_eq!(
        updated.latest_successful.as_ref().unwrap().snapshot_id,
        "new-native-golden"
    );
}
