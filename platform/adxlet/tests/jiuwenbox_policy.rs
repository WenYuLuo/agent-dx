use adx_core::execution::{DnsPolicy, NetworkAction, NetworkPolicy};
use adxlet::{jiuwenbox::policy::network_policy, sandboxd::Config};

#[test]
fn accepts_no_network_override() {
    assert!(network_policy("supervisor", None, &[], &Config::default()).is_ok());
}

#[test]
fn refuses_dns_enforcement_that_native_acl_cannot_provide() {
    let policy = NetworkPolicy {
        dns: Some(DnsPolicy {
            default_action: NetworkAction::Deny,
            rules: vec![],
        }),
        traffic: None,
    };
    assert!(network_policy("conch", Some(&policy), &[], &Config::default()).is_err());
}
