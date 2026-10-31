use crate::control as pb;

impl std::fmt::Debug for pb::EnvdServiceConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(
            &adx_core::envd::EnvdServiceConfig::from(self.clone()),
            formatter,
        )
    }
}

impl From<pb::EnvdServiceConfig> for adx_core::envd::EnvdServiceConfig {
    fn from(value: pb::EnvdServiceConfig) -> Self {
        Self {
            version: value.version,
            gateway_auth: value.gateway_auth,
            access_token: value.access_token,
            ingress_sources: value.ingress_sources,
        }
    }
}

impl From<adx_core::envd::EnvdServiceConfig> for pb::EnvdServiceConfig {
    fn from(value: adx_core::envd::EnvdServiceConfig) -> Self {
        Self {
            version: value.version,
            gateway_auth: value.gateway_auth,
            access_token: value.access_token,
            ingress_sources: value.ingress_sources,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_debug_redacts_envd_access_token() {
        let config = pb::EnvdServiceConfig {
            version: "0.9.0".into(),
            access_token: Some("secret".repeat(8)),
            ..Default::default()
        };
        let spec = pb::EnvironmentSpec {
            envd: Some(config),
            ..Default::default()
        };
        assert!(!format!("{spec:?}").contains("secret"));
        assert!(format!("{spec:?}").contains("[redacted]"));
    }
}
