use super::*;
use crate::{Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::sync::atomic::AtomicUsize;
fn pending_record() -> EnvironmentRecord {
    serde_json::from_value(serde_json::json!({
        "spec":{"id":"pending","tenant_id":"t","image":"image","runtime_class":"runc","priority":0,
            "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}},
        "assignment":{"environment_id":"pending","node_id":"n","shard_id":0,"generation":1,"devices":[]},
        "state":"Pending","revision":0,"runtime":{"id":"pending-1","ip":null},"resources_held":true,
        "checkpoint":null,"last_operation":null,"restart_attempts":0,"restart_pending":false,
        "execution_artifacts":[],"snapshot_source":null,"failure":null
    })).unwrap()
}

#[tokio::test]
async fn live_catalog_defers_a_claim_before_its_create_rpc_arrives() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
    );
    let record = pending_record();
    let adopted = node.adopt_comparison_record(&record).unwrap();
    assert!(
        adopted.is_none(),
        "an unstarted claim must remain available for its create RPC"
    );
    assert!(!node.environments.lock().unwrap().contains_key("pending"));
    assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn live_comparison_preserves_an_already_registered_unstarted_create() {
    for locally_reserved in [false, true] {
        let hooks = Arc::new(Hooks::default());
        let node = Adxlet::new(
            "n".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
        );
        node.update_capacity(
            adx_core::Resources {
                cpu_millis: 10,
                memory_bytes: 10,
                disk_bytes: 0,
            },
            std::time::Duration::from_secs(60),
        )
        .unwrap();
        let record = pending_record();
        if locally_reserved {
            node.reserve_local(&record.spec).unwrap();
        }
        let handle = node
            .environment(record.spec.clone(), record.assignment.clone())
            .unwrap();
        handle
            .check_authority(AuthorityCheck {
                record: Some(record),
                references: None,
            })
            .await
            .unwrap();
        assert_eq!(handle.view().0.state, EnvironmentState::Pending);
        assert_eq!(
            hooks.0.load(Ordering::SeqCst),
            0,
            "comparison cannot publish a failure for this create"
        );
    }
}

#[derive(Default)]
struct Hooks(AtomicUsize);
#[async_trait]
impl RuntimeDriver for Hooks {
    async fn inventory(&self) -> Result<Vec<crate::RuntimeObservation>> {
        Ok(vec![])
    }
    async fn start(
        &self,
        _: &adx_core::EnvironmentSpec,
        _: &str,
        _: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        panic!("adopting authority must not cold start");
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(true)
    }
    async fn remove(&self, _: &str) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn startup_still_retires_an_uncertain_old_pending_execution() {
    let hooks = Arc::new(Hooks::default());
    let node = Adxlet::new(
        "n".into(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
        hooks.clone(),
    );
    let record = pending_record();
    node.reconcile(vec![record.clone()]).await.unwrap();
    let handle = node.environment(record.spec, record.assignment).unwrap();
    assert_eq!(handle.view().0.state, EnvironmentState::Failed);
    assert!(hooks.0.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn live_comparison_does_not_exempt_pending_records_with_execution_evidence() {
    for revision in [false, true] {
        let hooks = Arc::new(Hooks::default());
        let node = Adxlet::new(
            "n".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
        );
        let mut record = pending_record();
        if revision {
            record.revision = 1;
        } else {
            record.runtime.ip = Some("127.0.0.1".parse().unwrap());
        }
        let handle = node.adopt_comparison_record(&record).unwrap().unwrap();
        handle
            .check_authority(AuthorityCheck {
                record: Some(record),
                references: None,
            })
            .await
            .unwrap();
        assert_eq!(handle.view().0.state, EnvironmentState::Failed);
    }
}

#[async_trait]
impl Readiness for Hooks {
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Hooks {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl StateSink for Hooks {
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Durability::Published)
    }
}

#[tokio::test]
async fn adopted_controller_requires_authority_check_before_exposing_lifecycle() {
    for terminal in [false, true] {
        let hooks = Arc::new(Hooks::default());
        let node = Adxlet::new(
            "n".into(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
            hooks.clone(),
        );
        let record: EnvironmentRecord = serde_json::from_value(serde_json::json!({
            "spec": {"id":"adopted", "tenant_id":"t", "image":"image", "runtime_class":"runc", "priority":0,
                "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":0}},
            "assignment":{"environment_id":"adopted","node_id":"n","shard_id":0,"generation":1,"devices":[]},
            "state": if terminal { "Failed" } else { "Running" }, "revision":2,
            "runtime":{"id":"adopted-1","ip":"127.0.0.1"}, "resources_held":!terminal,
            "checkpoint":null,"last_operation":null,"restart_attempts":0,"restart_pending":false,
            "execution_artifacts":[],"snapshot_source":null,"failure":null
        })).unwrap();
        let handle = node.adopt_comparison_record(&record).unwrap().unwrap();
        // This models a request racing the background task before its command
        // reaches the newly installed controller's queue.
        let early = if terminal {
            handle.delete().await
        } else {
            handle.ensure_running("early".into()).await
        };
        assert!(
            matches!(early, Err(Error::Unavailable(_))),
            "unverified adoption must reject lifecycle admission"
        );
        assert_eq!(hooks.0.load(Ordering::SeqCst), 0);
        let result = handle
            .check_authority(AuthorityCheck {
                record: Some(record),
                references: None,
            })
            .await
            .unwrap();
        assert!(result == AuthorityResult::Current);
        if terminal {
            assert_eq!(
                handle.delete().await.unwrap().record.state,
                EnvironmentState::Deleted
            );
        } else {
            assert_eq!(
                handle
                    .ensure_running("ready".into())
                    .await
                    .unwrap()
                    .record
                    .state,
                EnvironmentState::Running
            );
        }
    }
}
