//! E2B template protocol projected onto common platform snapshot preparation.
use super::network::configure_envd_service;
use crate::{contract, http::Api};
use adx_observability::trace;
use adx_protocol::control as pb;
use serde::Deserialize;
use serde_json::{json, Value};
use tonic::{Code, Status};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Registration {
    name: Option<String>,
    alias: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    cpu_count: Option<u32>,
    #[serde(rename = "memoryMB")]
    memory_mb: Option<u32>,
    min_free_disk_mb: Option<u64>,
    #[serde(rename = "teamID")]
    team_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Build {
    from_image: Option<String>,
    from_template: Option<String>,
    from_image_registry: Option<Value>,
    #[serde(default)]
    steps: Vec<Value>,
    start_cmd: Option<String>,
    ready_cmd: Option<String>,
    #[serde(default)]
    force: bool,
}

fn unsupported(message: &'static str) -> Status {
    super::http::validation(super::ValidationError::unsupported(message))
}

/// Only the SDK default selector is accepted; the platform resolves a family.
pub(super) fn resolution_request(
    reference: &str,
    caller: &pb::CallerContext,
    registration_only: bool,
) -> Result<pb::ResolveSnapshotTemplateRequest, Status> {
    let (family, selector) = reference
        .split_once(':')
        .map_or((reference, None), |(family, selector)| {
            (family, Some(selector))
        });
    if family.trim().is_empty()
        || selector.is_some_and(|selector| selector.is_empty() || selector.contains(':'))
    {
        return Err(Status::invalid_argument("invalid template reference"));
    }
    if selector.is_some_and(|selector| selector != "default") {
        return Err(unsupported(
            "only the default template selector is supported",
        ));
    }
    Ok(pb::ResolveSnapshotTemplateRequest {
        caller: Some(caller.clone()),
        reference: family.into(),
        registration_only: registration_only && selector.is_none(),
    })
}

fn registration_input(
    input: Registration,
    caller: &pb::CallerContext,
) -> Result<(String, pb::Resources), Status> {
    if input
        .team_id
        .as_ref()
        .is_some_and(|tenant| tenant != &caller.tenant_id)
    {
        return Err(Status::permission_denied(
            "template team is outside API key scope",
        ));
    }
    if input.min_free_disk_mb.is_some_and(|size| size != 0) {
        return Err(unsupported("template disk resizing is not supported"));
    }
    let reference = input
        .name
        .or(input.alias)
        .ok_or_else(|| Status::invalid_argument("template name required"))?;
    let (name, selected) = reference
        .rsplit_once(':')
        .map(|(name, tag)| (name, Some(tag)))
        .unwrap_or((&reference, None));
    let name = adx_core::snapshot_preparation::normalize_name(name)
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    if selected.is_some_and(|tag| tag != "default") || input.tags.iter().any(|tag| tag != "default")
    {
        return Err(unsupported(
            "only the default template selector is supported",
        ));
    }
    let cpu = input.cpu_count.unwrap_or(2);
    let memory = input.memory_mb.unwrap_or(1024);
    if cpu == 0
        || cpu > 32
        || (cpu != 1 && !cpu.is_multiple_of(2))
        || memory < 128
        || !memory.is_multiple_of(2)
    {
        return Err(Status::invalid_argument(
            "template cpuCount or memoryMB is invalid",
        ));
    }
    Ok((
        name,
        pb::Resources {
            cpu_millis: u64::from(cpu) * 1000,
            memory_bytes: u64::from(memory) * 1_048_576,
            disk_bytes: 0,
        },
    ))
}

impl Build {
    fn image(&self) -> Result<&str, Status> {
        if self
            .from_template
            .as_ref()
            .is_some_and(|value| !value.is_empty())
            || self.from_image_registry.is_some()
            || !self.steps.is_empty()
            || self
                .start_cmd
                .as_ref()
                .is_some_and(|value| !value.is_empty())
            || self
                .ready_cmd
                .as_ref()
                .is_some_and(|value| !value.is_empty())
        {
            return Err(unsupported(
                "only fromImage without build steps or start/ready commands is supported",
            ));
        }
        // Every preparation runs afresh; force does not weaken or bypass any checks.
        let _ = self.force;
        self.from_image
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Status::invalid_argument("fromImage required"))
    }
}

pub(super) async fn register(
    api: &Api,
    input: Registration,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let (name, resources) = registration_input(input, caller)?;
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    let value = api
        .clients
        .rpc(
            "apiserver.register_snapshot_preparation",
            client.register_snapshot_preparation(trace::inject(
                pb::RegisterSnapshotPreparationRequest {
                    caller: Some(caller.clone()),
                    name,
                    resources: Some(resources),
                },
            )),
        )
        .await?;
    Ok(
        json!({"templateID":value.template_id,"buildID":value.preparation_id,"public":false,"names":[value.name.clone()],"aliases":[value.name],"tags":["default"]}),
    )
}

async fn get(
    api: &Api,
    template: &str,
    build: &str,
    caller: &pb::CallerContext,
) -> Result<pb::SnapshotPreparationView, Status> {
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    api.clients
        .rpc(
            "apiserver.get_snapshot_preparation",
            client.get_snapshot_preparation(trace::inject(pb::GetSnapshotPreparationRequest {
                caller: Some(caller.clone()),
                template_id: template.into(),
                preparation_id: build.into(),
            })),
        )
        .await
}

pub(super) async fn start(
    api: &Api,
    template: &str,
    build: &str,
    input: Build,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let image = match input.image() {
        Ok(image) => image,
        Err(error) => return Err(error),
    };
    let config = api
        .clients
        .config
        .e2b
        .as_ref()
        .ok_or_else(|| Status::unimplemented("E2B API is disabled"))?;
    let current = get(api, template, build, caller).await?;
    let resources = current
        .resources
        .ok_or_else(|| Status::data_loss("template resources missing"))?;
    let memory_mb = resources.memory_bytes / 1_048_576;
    let mut spec = contract::create_spec_with_environment(
        json!({
            "namespace":"golden", "name":build, "image":image,
            "cpu":resources.cpu_millis, "memory":memory_mb,
            "idleTimeoutSeconds":0, "inheritEntrypoint":false,
        }),
        caller,
        api.clients.config.runtime_profile.as_ref(),
    )?;
    if !config.required_node_labels.is_empty() {
        let scheduling = spec.scheduling.get_or_insert_with(Default::default);
        if scheduling.required_node.is_empty() {
            scheduling.required_node.push(Default::default());
        }
        for selector in &mut scheduling.required_node {
            selector
                .match_labels
                .extend(config.required_node_labels.clone());
        }
    }
    configure_envd_service(
        &mut spec,
        pb::EnvdServiceConfig {
            version: config.envd_version.clone(),
            gateway_auth: true,
            access_token: None,
            ingress_sources: config.envd_ingress_sources.clone(),
        },
    )?;
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    api.clients
        .rpc(
            "apiserver.start_snapshot_preparation",
            client.start_snapshot_preparation(trace::inject(pb::StartSnapshotPreparationRequest {
                caller: Some(caller.clone()),
                template_id: template.into(),
                preparation_id: build.into(),
                spec: Some(spec),
            })),
        )
        .await?;
    Ok(Value::Null)
}

fn sdk_status(phase: i32) -> Result<&'static str, Status> {
    use pb::SnapshotPreparationPhase as Phase;
    match Phase::try_from(phase) {
        Ok(Phase::Registered) => Ok("waiting"),
        Ok(Phase::Creating | Phase::Snapshotting | Phase::Cleaning) => Ok("building"),
        Ok(Phase::Ready) => Ok("ready"),
        Ok(Phase::Failed) => Ok("error"),
        _ => Err(Status::data_loss("invalid snapshot preparation phase")),
    }
}

fn status_value(value: pb::SnapshotPreparationView) -> Result<Value, Status> {
    let mut response = json!({"templateID":value.template_id,"buildID":value.preparation_id,
        "status":sdk_status(value.phase)?,"logs":[],"logEntries":[]});
    if !value.error.is_empty() {
        response["reason"] = json!({"message":value.error});
    }
    Ok(response)
}

pub(super) async fn status(
    api: &Api,
    template: &str,
    build: &str,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    status_value(get(api, template, build, caller).await?)
}

/// A missing platform template can still be an existing E2B runtime snapshot ID.
pub(super) async fn delete(
    api: &Api,
    id: &str,
    caller: &pb::CallerContext,
) -> Result<bool, Status> {
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        api.clients.coordinator().await?,
    );
    match api
        .clients
        .rpc(
            "apiserver.delete_snapshot_template",
            client.delete_snapshot_template(trace::inject(pb::DeleteSnapshotTemplateRequest {
                caller: Some(caller.clone()),
                template_id: id.into(),
                require_unused: true,
            })),
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(error) if error.code() == Code::NotFound => Ok(false),
        // Upstream template deletion reports active/paused references as HTTP 400.
        Err(error) if error.code() == Code::FailedPrecondition => {
            Err(Status::invalid_argument(error.message()))
        }
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sdk_references_accept_only_default_and_reject_other_selectors() {
        let caller = pb::CallerContext::default();
        for reference in [
            "demo",
            "demo:default",
            "550e8400-e29b-41d4-a716-446655440010:default",
        ] {
            let request = resolution_request(reference, &caller, false).unwrap();
            assert!(!request.registration_only);
            assert!(!request.reference.contains(':'));
        }
        assert!(
            resolution_request("demo", &caller, true)
                .unwrap()
                .registration_only
        );
        assert!(
            !resolution_request("demo:default", &caller, true)
                .unwrap()
                .registration_only
        );
        for reference in ["demo:v1", "demo:550e8400-e29b-41d4-a716-446655440011"] {
            let error = resolution_request(reference, &caller, false).unwrap_err();
            assert_eq!(
                error.metadata().get("e2b-error-code").unwrap(),
                "capability_not_supported"
            );
        }
        for invalid in ["", ":default", "demo:", "demo:a:b"] {
            assert!(resolution_request(invalid, &caller, false).is_err());
        }
    }

    #[test]
    fn common_preparation_phases_map_to_sdk_build_states_only_at_the_adapter() {
        use pb::SnapshotPreparationPhase as Phase;
        for (phase, expected) in [
            (Phase::Registered, "waiting"),
            (Phase::Creating, "building"),
            (Phase::Snapshotting, "building"),
            (Phase::Cleaning, "building"),
            (Phase::Ready, "ready"),
            (Phase::Failed, "error"),
        ] {
            assert_eq!(sdk_status(phase as i32).unwrap(), expected);
        }
        assert!(sdk_status(Phase::Unspecified as i32).is_err());
        assert!(sdk_status(99).is_err());
    }

    #[test]
    fn from_image_accepts_empty_default_steps_and_rejects_unimplemented_operations() {
        let input: Build =
            serde_json::from_value(json!({"fromImage":"python:3.11","steps":[],"force":false}))
                .unwrap();
        assert_eq!(input.image().unwrap(), "python:3.11");
        for extra in [
            json!({"steps":[{"type":"RUN","args":["echo ignored"]}]}),
            json!({"startCmd":"server"}),
            json!({"readyCmd":"true"}),
            json!({"fromTemplate":"parent"}),
            json!({"fromImageRegistry":{}}),
        ] {
            let mut input = json!({"fromImage":"python:3.11"});
            input
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let error = serde_json::from_value::<Build>(input)
                .unwrap()
                .image()
                .unwrap_err();
            assert_eq!(
                error.metadata().get("e2b-error-code").unwrap(),
                "capability_not_supported"
            );
        }
    }

    #[test]
    fn registration_accepts_only_default_and_preserves_tenant_scope() {
        let caller = pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        };
        for settings in [
            json!({"name":" Demo "}),
            json!({"name":"demo:default"}),
            json!({"name":"demo","tags":["default"]}),
        ] {
            let (name, resources) =
                registration_input(serde_json::from_value(settings).unwrap(), &caller).unwrap();
            assert_eq!(name, "demo");
            assert_eq!(resources.cpu_millis, 2000);
            assert_eq!(resources.memory_bytes, 1024 * 1_048_576);
        }
        for settings in [
            json!({"name":"demo:v1"}),
            json!({"name":"demo","tags":["release"]}),
        ] {
            let error =
                registration_input(serde_json::from_value(settings).unwrap(), &caller).unwrap_err();
            assert_eq!(
                error.metadata().get("e2b-error-code").unwrap(),
                "capability_not_supported"
            );
        }
        for invalid in [
            json!({"name":"demo","cpuCount":3}),
            json!({"name":"demo","cpuCount":33}),
            json!({"name":"demo","memoryMB":129}),
        ] {
            assert_eq!(
                registration_input(serde_json::from_value(invalid).unwrap(), &caller)
                    .unwrap_err()
                    .code(),
                Code::InvalidArgument
            );
        }
        let input = serde_json::from_value(json!({"name":"demo","teamID":"other"})).unwrap();
        assert_eq!(
            registration_input(input, &caller).unwrap_err().code(),
            Code::PermissionDenied
        );
    }
}
