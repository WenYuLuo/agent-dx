//! E2B's restricted egress policy projection onto the native network model.
//!
//! The E2B update operation replaces its egress configuration, while an ADX
//! network policy also owns ingress and DNS.  Keep that distinction here so a
//! network update cannot silently discard native policy outside E2B's scope.
use adx_core::execution::{
    NetworkAction, NetworkDirection, NetworkPeer, NetworkPolicy, NetworkProtocol, NetworkRule,
    TrafficMode, TrafficPolicy,
};
use std::net::{IpAddr, Ipv4Addr};

const ALLOW_PRIORITY: u32 = 200;
const DENY_PRIORITY: u32 = 100;
use adx_core::envd::{gateway_ingress_rules, is_gateway_ingress_rule, ENVD_PORT};
#[cfg(test)]
use adx_core::envd::{ENVD_GUARD_ALLOW_PRIORITY, ENVD_GUARD_DENY_PRIORITY};

/// Enable the common service and persist its immutable network/exposure contract.
/// Preparation and instance creation share this exact construction.
pub(crate) fn configure_envd_service(
    spec: &mut adx_protocol::control::EnvironmentSpec,
    service: adx_protocol::control::EnvdServiceConfig,
) -> Result<(), tonic::Status> {
    let config: adx_core::envd::EnvdServiceConfig = service.clone().into();
    config
        .validate()
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    let mut execution = spec.execution.clone().unwrap_or_default();
    if !execution
        .private_service_ports
        .contains(&u32::from(ENVD_PORT))
    {
        execution.private_service_ports.push(u32::from(ENVD_PORT));
    }
    if service.gateway_auth {
        let baseline = execution
            .network
            .take()
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| tonic::Status::data_loss("invalid persisted network policy"))?
            .unwrap_or_default();
        let guarded = with_gateway_envd_ingress_guard(baseline, &service.ingress_sources)?;
        let protected: Vec<adx_protocol::control::NetworkRule> = guarded
            .traffic
            .as_ref()
            .into_iter()
            .flat_map(|traffic| &traffic.rules)
            .filter(|rule| rule.direction != NetworkDirection::Egress)
            .cloned()
            .map(Into::into)
            .collect();
        if !execution.protected_ingress.is_empty() && execution.protected_ingress != protected {
            return Err(tonic::Status::failed_precondition(
                "persisted protected ingress conflicts with envd configuration",
            ));
        }
        execution.protected_ingress = protected;
        execution.network = Some(guarded.into());
    }
    let core: adx_core::execution::ExecutionOptions = execution
        .clone()
        .try_into()
        .map_err(|error: adx_core::Error| tonic::Status::invalid_argument(error.to_string()))?;
    config
        .validate_execution(&core)
        .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?;
    spec.execution = Some(execution);
    spec.envd = Some(service);
    Ok(())
}

/// Adds the deployment-owned ingress policy that limits direct envd access to
/// the Relay and bridge addresses. Callers must validate the source addresses.
pub(crate) fn with_gateway_envd_ingress_guard(
    mut policy: NetworkPolicy,
    sources: &[String],
) -> Result<NetworkPolicy, tonic::Status> {
    let traffic = policy.traffic.get_or_insert(TrafficPolicy {
        ingress_default_action: NetworkAction::Allow,
        egress_default_action: NetworkAction::Allow,
        rules: Vec::new(),
        mode: TrafficMode::Stateful,
    });
    // Split bidirectional rules before ingress becomes immutable. E2B may later
    // replace egress while preserving the independently protected inbound half.
    traffic.rules = std::mem::take(&mut traffic.rules)
        .into_iter()
        .flat_map(|mut rule| {
            let egress = if rule.direction == NetworkDirection::Both {
                let mut egress = rule.clone();
                egress.direction = NetworkDirection::Egress;
                rule.direction = NetworkDirection::Ingress;
                Some(egress)
            } else {
                None
            };
            std::iter::once(rule).chain(egress)
        })
        .collect();
    traffic.rules.retain(|rule| !is_gateway_ingress_rule(rule));
    traffic.rules.extend(
        gateway_ingress_rules(sources)
            .map_err(|error| tonic::Status::invalid_argument(error.to_string()))?,
    );
    Ok(policy)
}

/// Typed subset of E2B's network request that ADX currently supports.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct EgressRequest {
    pub allow_out: Option<Vec<String>>,
    pub deny_out: Option<Vec<String>>,
    pub allow_internet_access: Option<bool>,
}

/// Fields that E2B exposes but which have no compatible ADX implementation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UnsupportedFeatures {
    pub egress_proxy: bool,
    pub rules: bool,
    pub mask_request_host: bool,
    pub https_ports: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ConversionError {
    code: &'static str,
    message: &'static str,
}

impl ConversionError {
    pub(crate) fn code(&self) -> &'static str {
        self.code
    }

    pub(crate) fn message(&self) -> &'static str {
        self.message
    }
}

fn unsupported(message: &'static str) -> ConversionError {
    ConversionError {
        code: "capability_not_supported",
        message,
    }
}

/// Replace only the E2B-managed egress part of `baseline`.
///
/// `None` is the create baseline: it receives an allow-by-default ingress
/// policy and stateful traffic mode.  Existing ingress (including `Both`
/// rules), DNS, and traffic mode are retained for updates.
pub(crate) fn convert_egress(
    request: &EgressRequest,
    unsupported_features: UnsupportedFeatures,
    baseline: Option<&NetworkPolicy>,
) -> Result<NetworkPolicy, ConversionError> {
    reject_unsupported(unsupported_features)?;
    let mut policy = baseline.cloned().unwrap_or_default();
    let existing = policy.traffic.take();
    let (ingress_default_action, mode, mut rules) = match existing {
        Some(traffic) => (
            traffic.ingress_default_action,
            traffic.mode,
            traffic
                .rules
                .into_iter()
                .filter_map(project_ingress)
                .collect(),
        ),
        None => (NetworkAction::Allow, TrafficMode::Stateful, Vec::new()),
    };

    rules.extend(rules_for(
        &request.allow_out,
        NetworkAction::Allow,
        ALLOW_PRIORITY,
    )?);
    rules.extend(rules_for(
        &request.deny_out,
        NetworkAction::Deny,
        DENY_PRIORITY,
    )?);
    policy.traffic = Some(TrafficPolicy {
        ingress_default_action,
        egress_default_action: if request.allow_internet_access == Some(false) {
            NetworkAction::Deny
        } else {
            NetworkAction::Allow
        },
        rules,
        mode,
    });
    Ok(policy)
}

fn reject_unsupported(features: UnsupportedFeatures) -> Result<(), ConversionError> {
    if features.egress_proxy {
        return Err(unsupported("egressProxy is not supported"));
    }
    if features.rules {
        return Err(unsupported("network rules are not supported"));
    }
    if features.mask_request_host {
        return Err(unsupported("maskRequestHost is not supported"));
    }
    if features.https_ports {
        return Err(unsupported("httpsPorts is not supported"));
    }
    Ok(())
}

fn project_ingress(mut rule: NetworkRule) -> Option<NetworkRule> {
    match rule.direction {
        NetworkDirection::Ingress => Some(rule),
        NetworkDirection::Both => {
            rule.direction = NetworkDirection::Ingress;
            Some(rule)
        }
        NetworkDirection::Egress => None,
    }
}

fn rules_for(
    values: &Option<Vec<String>>,
    action: NetworkAction,
    priority: u32,
) -> Result<Vec<NetworkRule>, ConversionError> {
    values
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|value| {
            Ok(NetworkRule {
                action,
                direction: NetworkDirection::Egress,
                protocol: NetworkProtocol::Any,
                peer: NetworkPeer {
                    cidr: ipv4_cidr(value)?,
                    ..NetworkPeer::default()
                },
                environment_port: 0,
                environment_port_range: None,
                priority,
            })
        })
        .collect()
}

fn ipv4_cidr(value: &str) -> Result<String, ConversionError> {
    let value = value.trim();
    if let Ok(IpAddr::V4(address)) = value.parse::<IpAddr>() {
        return Ok(format!("{address}/32"));
    }
    if let Ok(IpAddr::V6(_)) = value.parse::<IpAddr>() {
        return Err(unsupported("IPv6 egress rules are not supported"));
    }
    let Some((address, prefix)) = value.split_once('/') else {
        return Err(unsupported(
            "domain and wildcard egress rules are not supported",
        ));
    };
    if address.parse::<Ipv4Addr>().is_err() {
        if address.parse::<IpAddr>().is_ok() {
            return Err(unsupported("IPv6 egress rules are not supported"));
        }
        return Err(unsupported(
            "domain and wildcard egress rules are not supported",
        ));
    }
    if prefix
        .parse::<u8>()
        .ok()
        .filter(|prefix| *prefix <= 32)
        .is_none()
    {
        return Err(ConversionError {
            code: "invalid_request",
            message: "invalid IPv4 CIDR",
        });
    }
    Ok(value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envd_service_builder_persists_guard_and_rejects_source_replacement() {
        use adx_protocol::control as pb;
        let service = pb::EnvdServiceConfig {
            version: "0.9.0".into(),
            gateway_auth: true,
            access_token: None,
            ingress_sources: vec!["10.0.0.2".into()],
        };
        let mut spec = pb::EnvironmentSpec::default();
        configure_envd_service(&mut spec, service.clone()).unwrap();
        let execution = spec.execution.as_ref().unwrap();
        assert_eq!(execution.private_service_ports, vec![49_983]);
        assert_eq!(execution.protected_ingress.len(), 2);
        let core: adx_core::execution::ExecutionOptions = execution.clone().try_into().unwrap();
        let config: adx_core::envd::EnvdServiceConfig = service.clone().into();
        config.validate_execution(&core).unwrap();
        let previous = spec.clone();
        configure_envd_service(&mut spec, service.clone()).unwrap();
        assert_eq!(spec, previous);
        let mut changed = service;
        changed.ingress_sources = vec!["10.0.0.3".into()];
        assert!(configure_envd_service(&mut spec, changed).is_err());
        assert_eq!(spec, previous);
    }

    #[test]
    fn guarded_bidirectional_rules_remain_stable_across_egress_replacement() {
        let rule = NetworkRule {
            action: NetworkAction::Allow,
            direction: NetworkDirection::Both,
            protocol: NetworkProtocol::Tcp,
            peer: NetworkPeer::default(),
            environment_port: 8080,
            environment_port_range: None,
            priority: 10,
        };
        let baseline = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Allow,
                egress_default_action: NetworkAction::Allow,
                rules: vec![rule],
                mode: TrafficMode::Stateful,
            }),
            dns: None,
        };
        let guarded = with_gateway_envd_ingress_guard(baseline, &["10.0.0.2".into()]).unwrap();
        let protected: Vec<_> = guarded
            .traffic
            .as_ref()
            .unwrap()
            .rules
            .iter()
            .filter(|rule| rule.direction != NetworkDirection::Egress)
            .cloned()
            .collect();
        let changed = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures::default(),
            Some(&guarded),
        )
        .unwrap();
        let execution = adx_core::execution::ExecutionOptions {
            protected_ingress: protected,
            ..Default::default()
        };
        assert!(execution.validate_protected_ingress(Some(&changed)).is_ok());
    }

    #[test]
    fn e2b_egress_rules_preserve_native_ingress_dns_and_mode() {
        let baseline = NetworkPolicy {
            traffic: Some(TrafficPolicy {
                ingress_default_action: NetworkAction::Deny,
                egress_default_action: NetworkAction::Deny,
                mode: TrafficMode::Stateless,
                rules: vec![NetworkRule {
                    action: NetworkAction::Allow,
                    direction: NetworkDirection::Both,
                    protocol: NetworkProtocol::Tcp,
                    peer: NetworkPeer {
                        cidr: "10.0.0.0/8".into(),
                        ..NetworkPeer::default()
                    },
                    environment_port: 0,
                    environment_port_range: None,
                    priority: 7,
                }],
            }),
            dns: Some(adx_core::execution::DnsPolicy {
                default_action: NetworkAction::Allow,
                rules: Vec::new(),
            }),
        };
        let policy = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["1.2.3.4".into()]),
                deny_out: Some(vec!["10.0.0.0/8".into()]),
                allow_internet_access: Some(false),
            },
            UnsupportedFeatures::default(),
            Some(&baseline),
        )
        .unwrap();
        let traffic = policy.traffic.as_ref().unwrap();
        assert_eq!(traffic.ingress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.egress_default_action, NetworkAction::Deny);
        assert_eq!(traffic.mode, TrafficMode::Stateless);
        assert_eq!(traffic.rules[0].direction, NetworkDirection::Ingress);
        assert_eq!(traffic.rules[1].priority, ALLOW_PRIORITY);
        assert_eq!(traffic.rules[2].priority, DENY_PRIORITY);
        assert_eq!(policy.dns, baseline.dns);
    }

    #[test]
    fn empty_e2b_network_clears_only_egress_and_defaults_allow() {
        let policy = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap();
        let traffic = policy.traffic.unwrap();
        assert_eq!(traffic.ingress_default_action, NetworkAction::Allow);
        assert_eq!(traffic.egress_default_action, NetworkAction::Allow);
        assert!(traffic.rules.is_empty());
    }

    #[test]
    fn unsupported_egress_inputs_return_capability_error() {
        let error = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["*.example.test".into()]),
                ..Default::default()
            },
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap_err();
        assert_eq!(error.code(), "capability_not_supported");
        let error = convert_egress(
            &EgressRequest::default(),
            UnsupportedFeatures {
                https_ports: true,
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
        assert_eq!(error.code(), "capability_not_supported");
    }

    #[test]
    fn gateway_envd_guard_is_ingress_only_and_preserves_egress() {
        let baseline = convert_egress(
            &EgressRequest {
                allow_out: Some(vec!["1.2.3.4".into()]),
                ..Default::default()
            },
            UnsupportedFeatures::default(),
            None,
        )
        .unwrap();
        let guarded =
            with_gateway_envd_ingress_guard(baseline.clone(), &["10.0.0.2".into()]).unwrap();
        let traffic = guarded.traffic.unwrap();
        assert_eq!(
            traffic.egress_default_action,
            baseline.traffic.unwrap().egress_default_action
        );
        assert!(traffic.rules.iter().any(|rule| {
            rule.direction == NetworkDirection::Ingress
                && rule.action == NetworkAction::Deny
                && rule.environment_port_range.as_ref().is_some_and(|range| {
                    range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
                })
                && rule.priority == ENVD_GUARD_DENY_PRIORITY
        }));
        assert!(traffic.rules.iter().any(|rule| {
            rule.direction == NetworkDirection::Ingress
                && rule.action == NetworkAction::Allow
                && rule.peer.cidr == "10.0.0.2/32"
                && rule.environment_port_range.as_ref().is_some_and(|range| {
                    range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
                })
                && rule.priority == ENVD_GUARD_ALLOW_PRIORITY
        }));
    }

    #[test]
    fn gateway_envd_guard_replaces_only_prior_canonical_guard_sources() {
        let first = with_gateway_envd_ingress_guard(NetworkPolicy::default(), &["10.0.0.2".into()])
            .unwrap();
        let updated = with_gateway_envd_ingress_guard(first, &["10.0.0.3".into()]).unwrap();
        let rules = &updated.traffic.unwrap().rules;
        assert!(!rules.iter().any(|rule| rule.peer.cidr == "10.0.0.2/32"));
        assert!(rules.iter().any(|rule| {
            rule.peer.cidr == "10.0.0.3/32"
                && rule.priority == ENVD_GUARD_ALLOW_PRIORITY
                && rule.environment_port == 0
                && rule.environment_port_range.as_ref().is_some_and(|range| {
                    range.first == u32::from(ENVD_PORT) && range.last == u32::from(ENVD_PORT)
                })
        }));
    }
}
