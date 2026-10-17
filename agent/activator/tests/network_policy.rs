use adx_activator::environment_request::network_input;
use adx_agent_core::network::SandboxNetworkPolicy;
use serde_json::{json, Value};

fn policy() -> SandboxNetworkPolicy {
    serde_json::from_value(json!({
        "sandbox_cidrs":["10.233.16.0/20"], "internal_cidrs":["192.168.0.0/16"],
        "public_egress":true,
        "egress_allow":[
            {"cidr":"10.247.60.120/32","protocol":"tcp","ports":{"first":4000,"last":4000}},
            {"cidr":"10.247.3.10/32","protocol":"udp","ports":{"first":53,"last":53}}
        ]
    }))
    .unwrap()
}
// Independent evaluation of the public rule contract, including priority and defaults.
fn action(network: &Value, address: &str, protocol: &str, port: u16) -> String {
    let ip: std::net::IpAddr = address.parse().unwrap();
    network["traffic"]["rules"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|rule| rule["direction"] == "egress" || rule["direction"] == "both")
        .filter(|rule| {
            rule["peer"]["cidr"]
                .as_str()
                .unwrap()
                .parse::<ipnet::IpNet>()
                .unwrap()
                .contains(&ip)
        })
        .filter(|rule| rule["protocol"] == "any" || rule["protocol"] == protocol)
        .filter(|rule| {
            rule["peer"]["portRange"].is_null()
                || (rule["peer"]["portRange"]["first"].as_u64().unwrap() <= u64::from(port)
                    && rule["peer"]["portRange"]["last"].as_u64().unwrap() >= u64::from(port))
        })
        .max_by_key(|rule| rule["priority"].as_u64().unwrap())
        .map_or_else(
            || {
                network["traffic"]["egressDefaultAction"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            },
            |rule| rule["action"].as_str().unwrap().to_owned(),
        )
}
#[test]
fn isolated_sandboxes_and_internal_services_stay_closed_while_model_dns_and_public_work() {
    let network = network_input(&policy()).unwrap();
    assert_eq!(network["traffic"]["ingressDefaultAction"], "deny");
    for (ip, protocol, port, expected) in [
        ("10.233.19.163", "tcp", 50090, "deny"),
        ("10.233.17.57", "tcp", 18092, "deny"),
        ("10.247.60.120", "tcp", 4000, "allow"),
        ("10.247.60.120", "tcp", 443, "deny"),
        ("10.247.3.10", "udp", 53, "allow"),
        ("10.247.3.10", "tcp", 6379, "deny"),
        ("192.168.7.251", "tcp", 17001, "deny"),
        ("169.254.169.254", "tcp", 80, "deny"),
        ("1.1.1.1", "tcp", 443, "allow"),
    ] {
        assert_eq!(
            action(&network, ip, protocol, port),
            expected,
            "{ip}:{port}"
        );
    }
    let mut closed = policy();
    closed.public_egress = false;
    assert_eq!(
        action(&network_input(&closed).unwrap(), "1.1.1.1", "tcp", 443),
        "deny"
    );
}
#[test]
fn invalid_or_overlapping_service_exceptions_fail_before_creation() {
    let mut p = policy();
    p.sandbox_cidrs.clear();
    assert!(network_input(&p).is_err());
    let mut p = policy();
    p.egress_allow[0].cidr = "10.233.19.163/32".into();
    assert!(network_input(&p).is_err());
    let mut p = policy();
    p.egress_allow[0].cidr = "0.0.0.0/0".into();
    assert!(network_input(&p).is_err());
    let mut p = policy();
    p.internal_cidrs.push("not-a-network".into());
    assert!(network_input(&p).is_err());
    let mut p = policy();
    p.egress_allow[0].ports.as_mut().unwrap().first = 0;
    assert!(network_input(&p).is_err());
}
