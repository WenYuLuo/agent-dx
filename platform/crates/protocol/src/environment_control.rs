use crate::control as pb;
use adx_core::environment_control::{
    EnvironmentCompatibility, EnvironmentControlRecord, RunExpiryAction, RunPolicy,
};
use std::fmt;

impl fmt::Debug for pb::EnvironmentCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentCompatibility")
            .field("template_id", &self.template_id)
            .field("credentials", &"[redacted]")
            .field("allow_public_traffic", &self.allow_public_traffic)
            .finish()
    }
}

impl fmt::Debug for pb::SandboxRouteCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SandboxRouteCompatibility")
            .field("envd_token_sha256", &self.envd_token_sha256)
            .field("traffic_token_sha256", &self.traffic_token_sha256)
            .field("allow_public_traffic", &self.allow_public_traffic)
            .field("envd_ready", &self.envd_ready)
            .field("envd_gateway_auth", &self.envd_gateway_auth)
            .field("envd_signing_key", &"[redacted]")
            .finish()
    }
}
impl From<pb::EnvironmentCompatibility> for EnvironmentCompatibility {
    fn from(v: pb::EnvironmentCompatibility) -> Self {
        Self {
            template_id: v.template_id,
            envd_access_token: v.envd_access_token,
            traffic_access_token: v.traffic_access_token,
            allow_public_traffic: v.allow_public_traffic,
        }
    }
}
impl From<EnvironmentCompatibility> for pb::EnvironmentCompatibility {
    fn from(v: EnvironmentCompatibility) -> Self {
        Self {
            template_id: v.template_id,
            envd_access_token: v.envd_access_token,
            traffic_access_token: v.traffic_access_token,
            allow_public_traffic: v.allow_public_traffic,
        }
    }
}
impl From<EnvironmentControlRecord> for pb::EnvironmentControlRecord {
    fn from(v: EnvironmentControlRecord) -> Self {
        Self {
            environment_id: v.environment_id,
            expires_at_unix_ms: v.expires_at_unix_ms,
            started_at_unix_ms: v.started_at_unix_ms,
            revision: v.revision,
            generation: v.generation,
        }
    }
}

impl TryFrom<pb::RunPolicy> for RunPolicy {
    type Error = adx_core::Error;

    fn try_from(value: pb::RunPolicy) -> Result<Self, Self::Error> {
        let action = match pb::RunExpiryAction::try_from(value.action)
            .map_err(|_| adx_core::Error::Invalid("invalid run expiry action".into()))?
        {
            pb::RunExpiryAction::Delete => RunExpiryAction::Delete,
            pb::RunExpiryAction::Pause => RunExpiryAction::Pause,
            _ => {
                return Err(adx_core::Error::Invalid(
                    "run expiry action is required".into(),
                ))
            }
        };
        let policy = Self {
            timeout_seconds: value.timeout_seconds,
            action,
        };
        policy.validate()?;
        Ok(policy)
    }
}

impl From<RunPolicy> for pb::RunPolicy {
    fn from(value: RunPolicy) -> Self {
        Self {
            timeout_seconds: value.timeout_seconds,
            action: match value.action {
                RunExpiryAction::Delete => pb::RunExpiryAction::Delete as i32,
                RunExpiryAction::Pause => pb::RunExpiryAction::Pause as i32,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::pb;

    #[test]
    fn compatibility_debug_redacts_credentials_when_nested_in_spec() {
        let spec = pb::EnvironmentSpec {
            compatibility: Some(pb::EnvironmentCompatibility {
                template_id: "template".into(),
                envd_access_token: "sdk-secret".into(),
                traffic_access_token: "traffic-secret".into(),
                allow_public_traffic: true,
            }),
            ..Default::default()
        };
        let debug = format!("{spec:?}");
        assert!(!debug.contains("sdk-secret"));
        assert!(!debug.contains("traffic-secret"));
    }

    #[test]
    fn route_compatibility_debug_redacts_signing_key() {
        let compatibility = pb::SandboxRouteCompatibility {
            envd_token_sha256: "envd-hash".into(),
            traffic_token_sha256: "traffic-hash".into(),
            allow_public_traffic: true,
            envd_ready: true,
            envd_gateway_auth: true,
            envd_signing_key: "sdk-secret".into(),
        };
        let debug = format!("{compatibility:?}");
        assert!(debug.contains("envd-hash"));
        assert!(!debug.contains("sdk-secret"));
    }
}
