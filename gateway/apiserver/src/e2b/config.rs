use super::ValidationError;
use adx_core::runtime_profile::RuntimeProfile;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::Ipv4Addr;

/// Protocol configuration; template names and bindings belong to the platform directory.
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Config {
    pub domain: String,
    pub envd_version: String,
    /// Exact Relay or bridge source addresses permitted to reach envd.
    #[serde(default)]
    pub envd_ingress_sources: Vec<String>,
    /// Deployment-owned selection for workers with the matching trusted profile.
    #[serde(default)]
    pub required_node_labels: BTreeMap<String, String>,
}

impl Config {
    pub fn validate(&self, profile: Option<&RuntimeProfile>) -> Result<(), ValidationError> {
        if self.domain.trim().is_empty() || self.envd_version.trim().is_empty() {
            return Err(ValidationError::invalid(
                "E2B domain and envd version required",
            ));
        }
        adx_core::scheduling::LabelSelector {
            match_labels: self.required_node_labels.clone(),
            expressions: vec![],
        }
        .validate()
        .map_err(|_| ValidationError::invalid("invalid E2B worker selector"))?;
        let profile = profile.ok_or_else(|| {
            ValidationError::invalid("E2B requires the deployment runtime_profile")
        })?;
        profile
            .validate()
            .map_err(|_| ValidationError::invalid("invalid runtime_profile"))?;
        if !matches!(
            profile.rootfs.runtime_class.as_str(),
            "runsc" | "firecracker" | "firecracker-pvm"
        ) {
            return Err(ValidationError::invalid(
                "gateway envd authentication requires a supported runtime class",
            ));
        }
        if self.envd_ingress_sources.is_empty()
            || self
                .envd_ingress_sources
                .iter()
                .any(|source| !trusted_ingress_source(source))
        {
            return Err(ValidationError::invalid(
                "gateway envd authentication requires exact trusted IPv4 ingress sources",
            ));
        }
        let path = profile
            .env
            .get("ADX_ENVD_PATH")
            .map(String::as_str)
            .unwrap_or("");
        let digest = profile
            .env
            .get("ADX_ENVD_SHA256")
            .map(String::as_str)
            .unwrap_or("");
        let user = profile
            .env
            .get("ADX_ENVD_DEFAULT_USER")
            .map(String::as_str)
            .unwrap_or("");
        let cwd = profile
            .env
            .get("ADX_ENVD_DEFAULT_WORKDIR")
            .map(String::as_str)
            .unwrap_or("");
        if !path.starts_with('/')
            || path.contains('\0')
            || digest.len() != 64
            || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
            || user.trim().is_empty()
            || user.contains('\0')
            || !cwd.starts_with('/')
            || cwd.contains('\0')
        {
            return Err(ValidationError::invalid(
                "runtime_profile requires trusted envd path, digest, user and workdir",
            ));
        }
        Ok(())
    }
}

fn trusted_ingress_source(value: &str) -> bool {
    let Ok(address) = value.parse::<Ipv4Addr>() else {
        return false;
    };
    !address.is_unspecified() && !address.is_multicast()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn profile() -> RuntimeProfile {
        serde_json::from_value(json!({
            "rootfs":{"runtime_class":"runsc","type":"local","path":"/rootfs","readonly":false},
            "bootstrap":{"type":"erofs","root":"/rootfs","target":"/__adx","entrypoint":["/__adx/init"]},
            "env":{"ADX_ENVD_PATH":"/__adx/envd","ADX_ENVD_SHA256":"a".repeat(64),"ADX_ENVD_DEFAULT_USER":"user","ADX_ENVD_DEFAULT_WORKDIR":"/home/user"}
        })).unwrap()
    }
    #[test]
    fn protocol_configuration_requires_the_trusted_runtime_profile() {
        let config: Config = serde_json::from_value(json!({
            "domain":"sandbox.test","envdVersion":"0.9.0",
            "envdIngressSources":["10.0.0.2"],
            "requiredNodeLabels":{"adx.runtime.profile":"e2b-v1"}
        }))
        .unwrap();
        assert!(config.validate(Some(&profile())).is_ok());
        assert!(config.validate(None).is_err());
        let mut invalid = profile();
        invalid.env.remove("ADX_ENVD_DEFAULT_USER");
        assert!(config.validate(Some(&invalid)).is_err());
    }

    #[test]
    fn e2b_uses_gateway_auth_without_a_mode_switch() {
        let settings = json!({"domain":"sandbox.test", "envdVersion":"0.9.0",
            "envdIngressSources":["10.0.0.2"]});
        let config: Config = serde_json::from_value(settings.clone()).unwrap();
        assert!(config.validate(Some(&profile())).is_ok());
        let mut legacy = settings;
        legacy["gatewayEnvdAuth"] = json!(true);
        assert!(serde_json::from_value::<Config>(legacy).is_err());
    }

    #[test]
    fn gateway_envd_auth_requires_supported_profile_and_exact_sources() {
        let mut config: Config = serde_json::from_value(json!({
            "domain":"sandbox.test", "envdVersion":"0.9.0",

            "envdIngressSources":["10.0.0.2"]
        }))
        .unwrap();
        let mut gateway_profile = profile();
        gateway_profile.rootfs.runtime_class = "runsc".into();
        assert!(config.validate(Some(&gateway_profile)).is_ok());
        let mut runc_profile = profile();
        runc_profile.rootfs.runtime_class = "runc".into();
        assert!(config.validate(Some(&runc_profile)).is_err());

        for sources in [
            vec![],
            vec!["0.0.0.0"],
            vec!["10.0.0.0/24"],
            vec!["224.0.0.1"],
        ] {
            config.envd_ingress_sources = sources.into_iter().map(str::to_owned).collect();
            assert!(config.validate(Some(&gateway_profile)).is_err());
        }
        config.envd_ingress_sources = vec!["10.0.0.2".into()];
        let mut unsupported = profile();
        unsupported.rootfs.runtime_class = "kata".into();
        assert!(config.validate(Some(&unsupported)).is_err());
    }
}
