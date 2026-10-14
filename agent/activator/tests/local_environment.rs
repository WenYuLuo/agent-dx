use adx_activator::local_environment::{EnvironmentApplication, LocalEnvironment};
use adx_agent_core::environment::*;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Application {
    creates: Mutex<Vec<(String, Value, String)>>,
    stall: bool,
}
#[async_trait]
impl EnvironmentApplication for Application {
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError> {
        self.creates
            .lock()
            .unwrap()
            .push((tenant.into(), input, operation_id.into()));
        if self.stall {
            std::future::pending().await
        } else {
            Ok(json!({"environmentId":"adx-id","status":"running"}))
        }
    }
    async fn instance(&self, _: &str, id: &str) -> Result<Option<Value>, EnvironmentError> {
        Ok(Some(json!({"id":id,"status":"running"})))
    }
    async fn delete_request(
        &self,
        _: &str,
        _: &str,
        _: Value,
        _: &str,
    ) -> Result<(), EnvironmentError> {
        Err(EnvironmentError::OutcomeUnknown("not confirmed".into()))
    }
}
fn request() -> CreateEnvironment {
    serde_json::from_value(json!({"tenant":"tenant","id":"adx-id","execution":{
        "image":"app:1","isolation_runtime":"runc","inherit_entrypoint":true,"entrypoint":[],"working_dir":"","user":null,"env":{"MODEL_KEY":"test-key"},"resources":{"cpu_millis":1000,"memory_mib":512},"service":[]
    }})).unwrap()
}
#[tokio::test]
async fn local_adapter_passes_the_public_request_and_preserves_unknown_delete() {
    let app = Arc::new(Application::default());
    let environment = LocalEnvironment::new(app.clone(), Duration::from_secs(60)).unwrap();
    assert!(environment.create(&request()).await.unwrap().ready);
    let calls = app.creates.lock().unwrap().clone();
    assert_eq!(calls[0].0, "tenant");
    assert_eq!(
        calls[0].1,
        json!({"namespace":"adx","name":"id","image":"app:1","runtime":"runc","inheritEntrypoint":true,"env":{"MODEL_KEY":"test-key"},"cpu":1000,"memory":512,"createTimeoutSeconds":60})
    );
    assert!(calls[0].2.starts_with("activator-create-"));
    assert!(
        environment
            .get("tenant", "adx-id")
            .await
            .unwrap()
            .unwrap()
            .ready
    );
    assert!(matches!(
        environment.delete("tenant", "adx-id").await,
        Err(EnvironmentError::OutcomeUnknown(_))
    ));
}
#[tokio::test]
async fn expired_calls_are_not_submitted_and_submitted_timeouts_remain_unknown() {
    let app = Arc::new(Application {
        stall: true,
        ..Default::default()
    });
    let environment = LocalEnvironment::new(app.clone(), Duration::from_secs(60)).unwrap();
    let mut request = request();
    request.deadline_unix_ms = Some(adx_agent_core::unix_time_millis().saturating_sub(1));
    assert!(matches!(
        environment.create(&request).await,
        Err(EnvironmentError::Unavailable(_))
    ));
    assert!(app.creates.lock().unwrap().is_empty());
    request.deadline_unix_ms = Some(adx_agent_core::unix_time_millis() + 50);
    assert!(matches!(
        environment.create(&request).await,
        Err(EnvironmentError::OutcomeUnknown(_))
    ));
    assert_eq!(app.creates.lock().unwrap().len(), 1);
}
