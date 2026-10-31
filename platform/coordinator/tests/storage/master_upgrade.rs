use super::*;
use adx_core::{
    snapshots::{Reference, Snapshot},
    CheckpointArtifact,
};
use sha2::{Digest, Sha256};

// master 54efe5a's compact EnvironmentRecord JSON, in its serializer's field order.
// Keep these bytes independent of the current serializer: they are the receipt's
// digest input, and neither artifacts nor the receipt contain the new fields.
fn master_deleted_result(with_artifacts: bool) -> String {
    let raw = concat!(
        r#"{"failure":null,"restart_attempts":0,"restart_pending":false,"#,
        r#""spec":{"runtime_profile":null,"snapshot_id":$snapshot,"#,
        r#""lifecycle":{"idle_timeout_seconds":0,"idle_action":"delete","pause":null,"restart":null},"#,
        r#""env":{},"id":"master-deleted","tenant_id":"tenant","image":"image","runtime_class":"runc","#,
        r#""resources":{"cpu_millis":100,"memory_bytes":128,"disk_bytes":0},"#,
        r#""priority":0,"scheduling":{"labels":{},"devices":[],"#,
        r#""required_node":[],"preferred_node":[],"#,
        r#""required_affinity":[],"required_anti_affinity":[],"#,
        r#""preferred_affinity":[],"preferred_anti_affinity":[],"#,
        r#""topology_spread":[],"placement_groups":[]},"#,
        r#""execution":{"readiness_probe":null,"healthcheck_probe":null,"#,
        r#""init_timeout_seconds":0,"rootfs":null,"mounts":[],"network":null,"#,
        r#""data_plane":{"tunnel":"Inherit","port_forward":"Inherit"},"#,
        r#""ports":[],"failover":false,"inherit_entrypoint":false,"#,
        r#""limits":{"cpu_millis":0,"memory_bytes":0,"disk_bytes":0},"extra_config":""}},"#,
        r#""assignment":{"environment_id":"master-deleted","node_id":"node","shard_id":0,"generation":1,"devices":[]},"#,
        r#""state":"Deleted","revision":3,"runtime":{"id":"master-deleted-1","ip":null},"#,
        r#""resources_held":false,"checkpoint":$checkpoint,"#,
        r#""execution_artifacts":$artifacts,"#,
        r#""snapshot_source":$source,"last_operation":null}"#,
    );
    if with_artifacts {
        raw.replace("$snapshot", r#""master-source""#)
            .replace(
                "$checkpoint",
                r#"{"origin":null,"id":"checkpoint","artifact":{"storage":"local","location":"/owned/checkpoint","size_bytes":1},"expires_at_unix_seconds":null,"source_runtime_id":"master-deleted-1"}"#,
            )
            .replace(
                "$artifacts",
                r#"[{"storage":"local","location":"/owned/input","size_bytes":1}]"#,
            )
            .replace(
                "$source",
                r#"{"origin":{"environment_id":"source","runtime_id":"source-1","ownership_generation":1},"id":"master-source","artifact":{"storage":"local","location":"/borrowed/source","size_bytes":1},"expires_at_unix_seconds":null,"source_runtime_id":"source-1"}"#,
            )
    } else {
        raw.replace("$snapshot", "null")
            .replace("$checkpoint", "null")
            .replace("$artifacts", "[]")
            .replace("$source", "null")
    }
}

async fn write_master_receipt(
    connection: &mut redis::aio::MultiplexedConnection,
    raw_result: &str,
    environment_id: &str,
    generation: u64,
) {
    let digest = format!("{:x}", Sha256::digest(raw_result.as_bytes()));
    // This is master's three-field receipt, not the new receipt serializer.
    let receipt = format!(
        r#"{{"environment_id":"{environment_id}","generation":{generation},"result_digest":"{digest}"}}"#
    );
    let _: () = redis::cmd("SET")
        .arg("adx:{test}:control:v1:deleted:master-deleted")
        .arg(receipt)
        .arg("PX")
        .arg(600_000)
        .query_async(connection)
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn master_deleted_receipts_replay_only_the_exact_result_without_new_cleanup() {
    for with_artifacts in [false, true] {
        let rig = common::Redis::new().await;
        let session = rig.store().await.begin(1).await.unwrap();
        register(&session, "node").await;
        let raw_result = master_deleted_result(with_artifacts);
        let deleted: EnvironmentRecord = serde_json::from_str(&raw_result).unwrap();
        if let Some(source) = &deleted.snapshot_source {
            let snapshot = Snapshot::new(
                "master-source".into(),
                vec![],
                spec("source"),
                "node".into(),
                "source-1".into(),
                source.artifact.clone(),
            )
            .unwrap();
            session.publish_snapshot(snapshot).await.unwrap();
            session
                .acquire_snapshot(
                    "master-source",
                    "tenant",
                    Reference::Restore {
                        environment_id: "master-deleted".into(),
                    },
                )
                .await
                .unwrap();
        }

        // Retire the catalog entry before installing the receipt from master.
        let mut initial_spec = deleted.spec.clone();
        initial_spec.snapshot_id = None;
        session
            .reserve(initial_spec.clone(), deleted.assignment.clone())
            .await
            .unwrap();
        let mut current = running(initial_spec, deleted.assignment.clone());
        session.commit(current.clone()).await.unwrap();
        current.state = EnvironmentState::Deleted;
        current.resources_held = false;
        current.runtime.ip = None;
        current.revision += 1;
        session.commit(current).await.unwrap();

        let client = redis::Client::open(rig.url.as_str()).unwrap();
        let mut connection = client.get_multiplexed_async_connection().await.unwrap();
        write_master_receipt(&mut connection, &raw_result, "master-deleted", 1).await;
        assert_eq!(session.get("master-deleted").await, Err(Error::NotFound));
        // Exercise receipt decoding and digest comparison through the real commit path.
        assert_eq!(session.commit(deleted.clone()).await.unwrap(), deleted);
        assert_eq!(serde_json::to_string(&deleted).unwrap(), raw_result);

        let mut wrong_revision = deleted.clone();
        wrong_revision.revision += 1;
        let mut wrong_generation = deleted.clone();
        wrong_generation.assignment.generation += 1;
        let mut wrong_content = deleted.clone();
        wrong_content.spec.image = "different".into();
        for changed in [wrong_revision, wrong_generation, wrong_content] {
            assert_eq!(session.commit(changed).await, Err(Error::Conflict));
        }
        let cleanup = CheckpointArtifact {
            storage: "local".into(),
            scope: String::new(),
            location: "/new/cleanup".into(),
            size_bytes: 1,
        };
        assert_eq!(
            session
                .commit_with_cleanup(deleted.clone(), &[cleanup])
                .await,
            Err(Error::Conflict)
        );
        assert!(session.checkpoint_cleanups().await.unwrap().is_empty());

        // The receipt's own identity fields remain mandatory even with an exact digest.
        for (id, generation) in [("different", 1), ("master-deleted", 2)] {
            write_master_receipt(&mut connection, &raw_result, id, generation).await;
            assert_eq!(session.commit(deleted.clone()).await, Err(Error::Conflict));
        }
    }
}
