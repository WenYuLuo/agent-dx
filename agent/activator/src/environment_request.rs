//! Agent-side conversion to the public Environment management protocol.
//! No Platform model or RPC client is used here.
use adx_agent_core::{environment::*, limits};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub(crate) fn validate_identity(value: &str) -> Result<(), EnvironmentError> {
    if value.is_empty()
        || value.len() > limits::IDENTIFIER_BYTES
        || matches!(value, "." | "..")
        || value
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
    {
        return Err(EnvironmentError::Invalid(
            "invalid Environment identity".into(),
        ));
    }
    Ok(())
}
pub(crate) fn observation(
    tenant: &str,
    id: &str,
    phase: EnvironmentPhase,
) -> EnvironmentObservation {
    EnvironmentObservation {
        id: id.into(),
        tenant: tenant.into(),
        ready: phase == EnvironmentPhase::Running,
        phase,
        runtime_id: None,
        message: None,
    }
}
pub fn validate_timeout(timeout: Duration) -> Result<(), EnvironmentError> {
    if timeout.is_zero() || i64::try_from(timeout.as_secs()).is_err() {
        return Err(EnvironmentError::Invalid(
            "invalid Environment timeout".into(),
        ));
    }
    Ok(())
}
pub fn validate_execution(execution: &ExecutionSpec) -> Result<(), EnvironmentError> {
    execution.validate().map_err(EnvironmentError::Invalid)?;
    if !execution.inherit_entrypoint {
        return Err(EnvironmentError::Unsupported(
            "API Server adapter requires image process inheritance".into(),
        ));
    }
    if execution
        .env
        .keys()
        .any(|key| key.starts_with("ADX_") || key == "EXECD_HTTP_TOKEN")
    {
        return Err(EnvironmentError::Invalid("reserved environment key".into()));
    }
    if execution.resources.cpu_millis > i64::MAX as u64
        || execution.resources.memory_mib > (i64::MAX as u64 / 1048576)
    {
        return Err(EnvironmentError::Invalid(
            "Environment resources exceed API Server limits".into(),
        ));
    }
    Ok(())
}
/// Both HTTP and local adapters submit the same request body. Deadline is transport metadata.
pub fn create_input(
    request: &CreateEnvironment,
    timeout: Duration,
) -> Result<Value, EnvironmentError> {
    validate_execution(&request.execution)?;
    validate_identity(&request.tenant)?;
    validate_identity(&request.id)?;
    validate_timeout(timeout)?;
    // AgentBinding generations use adx-<UUID>. The public API builds namespace-name IDs.
    let name = request
        .id
        .strip_prefix("adx-")
        .filter(|name| !name.is_empty() && name.len() <= 256)
        .ok_or_else(|| {
            EnvironmentError::Invalid("managed Environment ID must be adx-<name>".into())
        })?;
    let execution = &request.execution;
    // Keep the submitted specification stable across retries. The caller's absolute
    // deadline bounds HTTP waiting; API Server owns its independent create budget.
    Ok(json!({
        "namespace":"adx", "name":name, "image":execution.image, "runtime":execution.isolation_runtime,
        "cpu":execution.resources.cpu_millis, "memory":execution.resources.memory_mib,
        "inheritEntrypoint":true, "env":execution.env,
        "createTimeoutSeconds":timeout.as_secs().max(1),
    }))
}
pub fn operation_id(action: &str, tenant: &str, id: &str) -> Result<String, EnvironmentError> {
    validate_identity(tenant)?;
    validate_identity(id)?;
    let identity = serde_json::to_vec(&(tenant, id))
        .map_err(|_| EnvironmentError::Invalid("invalid Environment identity".into()))?;
    Ok(format!("activator-{action}-{:x}", Sha256::digest(identity)))
}
pub(crate) fn created(
    tenant: &str,
    id: &str,
    value: &Value,
) -> Result<EnvironmentObservation, EnvironmentError> {
    if value["environmentId"] != id || value["status"] != "running" {
        return Err(EnvironmentError::OutcomeUnknown(
            "invalid API Server create response".into(),
        ));
    }
    Ok(observation(tenant, id, EnvironmentPhase::Running))
}
pub(crate) fn observed(
    tenant: &str,
    id: &str,
    row: &Value,
) -> Result<EnvironmentObservation, EnvironmentError> {
    if row["id"] != id {
        return Err(EnvironmentError::Unavailable(
            "API Server instance identity mismatch".into(),
        ));
    }
    let phase = match row["status"].as_str() {
        Some("running") => EnvironmentPhase::Running,
        Some("pending" | "starting") => EnvironmentPhase::Creating,
        Some("failed") => EnvironmentPhase::Failed,
        Some("paused" | "pausing" | "resuming" | "deleting") => {
            return Err(EnvironmentError::Conflict(
                "Environment is not available for Agent activation".into(),
            ))
        }
        _ => {
            return Err(EnvironmentError::Unavailable(
                "unknown API Server instance state".into(),
            ))
        }
    };
    Ok(observation(tenant, id, phase))
}
