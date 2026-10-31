//! Per-environment configuration for the optional deployment-owned envd service.
//! Artifact and privileged bootstrap settings remain in the trusted RuntimeProfile.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{fmt, net::Ipv4Addr};

/// envd's fixed HTTP service port.
pub const ENVD_PORT: u16 = 49_983;
/// Reserved priorities for the service's direct-network protection.
pub const ENVD_GUARD_DENY_PRIORITY: u32 = u32::MAX - 2;
pub const ENVD_GUARD_ALLOW_PRIORITY: u32 = u32::MAX - 1;

/// Construct the canonical envd deny/allow rules from exact trusted IPv4 sources.
/// Returns Invalid for an empty list, malformed, unspecified or multicast address.
pub fn gateway_ingress_rules(sources: &[String]) -> Result<Vec<crate::execution::NetworkRule>> {
    use crate::execution::{
        NetworkAction, NetworkDirection, NetworkPeer, NetworkProtocol, NetworkRule, PortRange,
    };
    if sources.is_empty()
        || sources.iter().any(|source| {
            source.parse::<Ipv4Addr>().map_or(true, |address| {
                address.is_unspecified() || address.is_multicast()
            })
        })
    {
        return Err(Error::Invalid(
            "envd requires exact trusted ingress IPv4 sources".into(),
        ));
    }
    let rule = |action, priority, peer| NetworkRule {
        action,
        direction: NetworkDirection::Ingress,
        protocol: NetworkProtocol::Tcp,
        peer,
        environment_port: 0,
        environment_port_range: Some(PortRange {
            first: u32::from(ENVD_PORT),
            last: u32::from(ENVD_PORT),
        }),
        priority,
    };
    let mut rules = vec![rule(
        NetworkAction::Deny,
        ENVD_GUARD_DENY_PRIORITY,
        NetworkPeer::default(),
    )];
    rules.extend(sources.iter().map(|source| {
        rule(
            NetworkAction::Allow,
            ENVD_GUARD_ALLOW_PRIORITY,
            NetworkPeer {
                cidr: format!("{source}/32"),
                ..Default::default()
            },
        )
    }));
    Ok(rules)
}

/// Identify the reserved canonical rule shape when replacing trusted sources.
pub fn is_gateway_ingress_rule(rule: &crate::execution::NetworkRule) -> bool {
    use crate::execution::{NetworkDirection, NetworkProtocol};
    rule.direction == NetworkDirection::Ingress
        && rule.protocol == NetworkProtocol::Tcp
        && rule.environment_port == 0
        && rule.environment_port_range.as_ref().is_some_and(|range| {
            range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
        })
        && matches!(
            rule.priority,
            ENVD_GUARD_DENY_PRIORITY | ENVD_GUARD_ALLOW_PRIORITY
        )
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvdServiceConfig {
    pub version: String,
    #[serde(default)]
    pub gateway_auth: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    #[serde(default)]
    pub ingress_sources: Vec<String>,
}

impl fmt::Debug for EnvdServiceConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnvdServiceConfig")
            .field("version", &self.version)
            .field("gateway_auth", &self.gateway_auth)
            .field(
                "access_token",
                &self.access_token.as_ref().map(|_| "[redacted]"),
            )
            .field("ingress_sources", &self.ingress_sources)
            .finish()
    }
}

impl EnvdServiceConfig {
    /// Validate tokenless service exposure against its persisted protection.
    /// Generic node code validates the protected policy without knowing this protocol.
    pub fn validate_execution(&self, execution: &crate::execution::ExecutionOptions) -> Result<()> {
        self.validate()?;
        execution.validate_protected_ingress(execution.network.as_ref())?;
        execution.validate_private_service_ports()?;
        if !self.gateway_auth {
            return Ok(());
        }
        if !execution.private_service_ports.contains(&ENVD_PORT)
            || execution.protected_ingress.is_empty()
        {
            return Err(Error::Invalid(
                "gateway-authenticated envd requires private service ingress protection".into(),
            ));
        }
        let Some(traffic) = execution
            .network
            .as_ref()
            .and_then(|network| network.traffic.as_ref())
        else {
            return Err(Error::Invalid(
                "gateway-authenticated envd requires a trusted ingress ACL".into(),
            ));
        };
        use crate::execution::{NetworkAction, NetworkDirection, NetworkProtocol};
        let permits_envd = |rule: &crate::execution::NetworkRule| {
            matches!(
                rule.direction,
                NetworkDirection::Ingress | NetworkDirection::Both
            ) && matches!(rule.protocol, NetworkProtocol::Tcp | NetworkProtocol::Any)
                && match &rule.environment_port_range {
                    Some(range) => {
                        range.first <= u32::from(ENVD_PORT) && u32::from(ENVD_PORT) <= range.last
                    }
                    None => {
                        rule.environment_port == 0 || rule.environment_port == u32::from(ENVD_PORT)
                    }
                }
        };
        let expected = gateway_ingress_rules(&self.ingress_sources)?;
        if expected.iter().any(|rule| !traffic.rules.contains(rule)) {
            return Err(Error::Invalid(
                "gateway-authenticated envd canonical ingress rule is missing".into(),
            ));
        }
        if traffic.rules.iter().any(|rule| {
            permits_envd(rule)
                && rule.action == NetworkAction::Allow
                && rule.priority >= ENVD_GUARD_DENY_PRIORITY
                && !expected.contains(rule)
        }) {
            return Err(Error::Invalid(
                "gateway-authenticated envd ACL permits an untrusted ingress peer".into(),
            ));
        }
        Ok(())
    }

    /// Reject malformed versions, conflicting auth modes and untrusted ingress addresses.
    pub fn validate(&self) -> Result<()> {
        if self.version.trim().is_empty()
            || self.version.len() > 128
            || self.version.chars().any(char::is_whitespace)
            || self.version.chars().any(char::is_control)
            || if self.gateway_auth {
                self.access_token.is_some()
                    || self.ingress_sources.is_empty()
                    || self.ingress_sources.iter().any(|source| {
                        source.parse::<Ipv4Addr>().map_or(true, |address| {
                            address.is_unspecified() || address.is_multicast()
                        })
                    })
            } else {
                !self.ingress_sources.is_empty()
                    || !self.access_token.as_ref().is_some_and(|token| {
                        token.len() >= 32 && !token.chars().any(char::is_control)
                    })
            }
        {
            return Err(Error::Invalid("invalid envd service configuration".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> EnvdServiceConfig {
        EnvdServiceConfig {
            version: "0.9.0".into(),
            gateway_auth: false,
            access_token: Some("secret".repeat(8)),
            ingress_sources: Vec::new(),
        }
    }

    #[test]
    fn tokenless_envd_requires_private_port_and_persisted_source_protection() {
        use crate::execution::{
            ExecutionOptions, NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy,
            NetworkProtocol, NetworkRule, PortRange, TrafficMode, TrafficPolicy,
        };
        let service = EnvdServiceConfig {
            version: "0.9.0".into(),
            gateway_auth: true,
            access_token: None,
            ingress_sources: vec!["10.0.0.2".into()],
        };
        let rule = |action, priority, peer| NetworkRule {
            action,
            direction: NetworkDirection::Ingress,
            protocol: NetworkProtocol::Tcp,
            peer,
            environment_port: 0,
            environment_port_range: Some(PortRange {
                first: 49_983,
                last: 49_983,
            }),
            priority,
        };
        let rules = vec![
            rule(NetworkAction::Deny, u32::MAX - 2, NetworkPeer::default()),
            rule(
                NetworkAction::Allow,
                u32::MAX - 1,
                NetworkPeer {
                    cidr: "10.0.0.2/32".into(),
                    ..Default::default()
                },
            ),
        ];
        let mut execution = ExecutionOptions {
            private_service_ports: vec![49_983],
            protected_ingress: rules.clone(),
            network: Some(NetworkPolicy {
                traffic: Some(TrafficPolicy {
                    ingress_default_action: NetworkAction::Allow,
                    egress_default_action: NetworkAction::Allow,
                    rules,
                    mode: TrafficMode::Stateful,
                }),
                dns: None,
            }),
            ..Default::default()
        };
        assert!(service.validate_execution(&execution).is_ok());
        execution.private_service_ports.clear();
        assert!(service.validate_execution(&execution).is_err());
        execution.private_service_ports.push(49_983);
        execution.protected_ingress.clear();
        assert!(service.validate_execution(&execution).is_err());
        execution.protected_ingress = execution
            .network
            .as_ref()
            .unwrap()
            .traffic
            .as_ref()
            .unwrap()
            .rules
            .clone();
        execution
            .network
            .as_mut()
            .unwrap()
            .traffic
            .as_mut()
            .unwrap()
            .rules[1]
            .peer
            .cidr = "10.0.0.3/32".into();
        assert!(service.validate_execution(&execution).is_err());
    }

    #[test]
    fn validates_auth_mode_and_redacts_credentials() {
        let mut value = config();
        assert!(value.validate().is_ok());
        assert!(!format!("{value:?}").contains("secret"));
        value.gateway_auth = true;
        value.ingress_sources = vec!["10.0.0.2".into()];
        assert!(value.validate().is_err());
        value.access_token = None;
        assert!(value.validate().is_ok());
        for source in ["0.0.0.0", "224.0.0.1", "10.0.0.0/24", "invalid"] {
            value.ingress_sources = vec![source.into()];
            assert!(value.validate().is_err());
        }
    }

    #[test]
    fn validates_version_and_guest_token() {
        for version in ["", " ", "0.9.0\n", "0.9.0 dev"] {
            let mut value = config();
            value.version = version.into();
            assert!(value.validate().is_err());
        }
        let mut value = config();
        value.access_token = None;
        assert!(value.validate().is_err());
    }
}
