//! Composition bridge from Activator's local transport to the Environment application service.
//! Agent execution specifications are converted in the Activator crate.
use crate::{environment_service::EnvironmentService, http::instance_view, operations::Kind};
use adx_activator::local_environment::EnvironmentApplication;
use adx_agent_core::environment::EnvironmentError;
use adx_protocol::control as pb;
use async_trait::async_trait;
use serde_json::Value;
use tonic::{Code, Status};

#[async_trait]
impl EnvironmentApplication for EnvironmentService {
    async fn create_request(
        &self,
        tenant: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<Value, EnvironmentError> {
        Box::pin(EnvironmentService::create_request(
            self,
            input,
            operation_id,
            &caller(tenant),
        ))
        .await
        .map_err(|error| map_status(error, true))
    }
    async fn instance(&self, tenant: &str, id: &str) -> Result<Option<Value>, EnvironmentError> {
        let Some(owner) = self
            .inspect(tenant, id)
            .await
            .map_err(|error| map_status(error, false))?
        else {
            return Ok(None);
        };
        match instance_view(owner) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.code() == Code::NotFound => Ok(None),
            Err(error) => Err(map_status(error, false)),
        }
    }
    async fn delete_request(
        &self,
        tenant: &str,
        id: &str,
        input: Value,
        operation_id: &str,
    ) -> Result<(), EnvironmentError> {
        self.execute(Kind::Delete, id, operation_id, input, &caller(tenant))
            .await
            .map_err(|error| map_status(error, true))?;
        Ok(())
    }
}
fn caller(tenant: &str) -> pb::CallerContext {
    pb::CallerContext {
        tenant_id: tenant.to_owned(),
        administrator: false,
    }
}
fn map_status(status: Status, write: bool) -> EnvironmentError {
    match status.code() {
        Code::PermissionDenied => EnvironmentError::NotFound,
        Code::NotFound if !write => EnvironmentError::NotFound,
        Code::InvalidArgument => EnvironmentError::Invalid("API Server rejected request".into()),
        Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => {
            EnvironmentError::Conflict("Environment identity/specification conflict".into())
        }
        Code::Unimplemented => {
            EnvironmentError::Unsupported("Environment capability unavailable".into())
        }
        _ if write => EnvironmentError::OutcomeUnknown(
            "Environment write result is unknown; inspect the original ID".into(),
        ),
        _ => EnvironmentError::Unavailable("Environment read is unavailable".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_writes_and_private_reads_keep_distinct_errors() {
        assert_eq!(
            map_status(Status::permission_denied("private"), false),
            EnvironmentError::NotFound
        );
        assert!(matches!(
            map_status(Status::not_found("absent"), true),
            EnvironmentError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("lost response"), true),
            EnvironmentError::OutcomeUnknown(_)
        ));
        assert!(matches!(
            map_status(Status::unavailable("read failed"), false),
            EnvironmentError::Unavailable(_)
        ));
    }
}
