use adx_core::execution::{DnsPolicy, NetworkAction, NetworkPolicy};
use adxlet::jiuwenbox::policy::validate_network;

#[test]
fn accepts_no_network_override() {
    assert!(validate_network(None).is_ok());
}

#[test]
fn refuses_dns_enforcement_that_the_host_filter_cannot_provide() {
    let policy = NetworkPolicy {
        dns: Some(DnsPolicy {
            default_action: NetworkAction::Deny,
            rules: vec![],
        }),
        traffic: None,
    };
    assert!(validate_network(Some(&policy)).is_err());
}
