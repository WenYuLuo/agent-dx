use adxlet::{jiuwenbox::policy::filter_script, sandboxd::proto::*};

fn policy() -> NetworkPolicy {
    NetworkPolicy {
        traffic: Some(TrafficPolicy {
            ingress_default_action: NetworkPolicyAction::Deny as i32,
            egress_default_action: NetworkPolicyAction::Deny as i32,
            mode: TrafficPolicyMode::Stateful as i32,
            rules: vec![
                TrafficRule {
                    action: NetworkPolicyAction::Deny as i32,
                    direction: NetworkDirection::Both as i32,
                    protocol: NetworkProtocol::Any as i32,
                    peer: Some(NetworkEndpoint {
                        cidr: "10.0.0.0/8".into(),
                        ..Default::default()
                    }),
                    priority: 200,
                    ..Default::default()
                },
                TrafficRule {
                    action: NetworkPolicyAction::Allow as i32,
                    direction: NetworkDirection::Ingress as i32,
                    protocol: NetworkProtocol::Tcp as i32,
                    sandbox_port_range: Some(PortRange {
                        first: 50090,
                        last: 50090,
                    }),
                    priority: u32::MAX,
                    ..Default::default()
                },
            ],
            ..Default::default()
        }),
        dns: None,
        ..Default::default()
    }
}

#[test]
fn preserves_order_ports_defaults_and_two_sided_filtering() {
    let script = filter_script(
        "runtime-1",
        "192.0.2.2".parse().unwrap(),
        Some(&policy()),
        "",
        false,
    )
    .unwrap();
    assert!(
        script.find("--dport 50090:50090 -j RETURN").unwrap()
            < script.find("-s 10.0.0.0/8 -j DROP").unwrap()
    );
    assert!(script.contains("-d 10.0.0.0/8 -j DROP"));
    assert!(!script.contains("-j ACCEPT"));
    assert!(script.contains("-I FORWARD 1 -d 192.0.2.2/32"));
    assert!(script.contains("-I FORWARD 1 -s 192.0.2.2/32"));
    assert!(script.ends_with("COMMIT\n"));
}

#[test]
fn replacement_and_cleanup_touch_only_exact_owned_chains() {
    let installed = filter_script(
        "runtime-1",
        "192.0.2.2".parse().unwrap(),
        Some(&policy()),
        "",
        false,
    )
    .unwrap();
    let input = installed
        .lines()
        .find(|s| s.starts_with(":ADXJI"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .trim_start_matches(':');
    let output = installed
        .lines()
        .find(|s| s.starts_with(":ADXJE"))
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .trim_start_matches(':');
    let saved=format!("*filter\n:{input} - [0:0]\n:{output} - [0:0]\n:UNRELATED - [0:0]\n-A FORWARD -d 192.0.2.2/32 -j {input}\n-A INPUT -j UNRELATED\nCOMMIT\n");
    let replace = filter_script(
        "runtime-1",
        "192.0.2.2".parse().unwrap(),
        None,
        &saved,
        false,
    )
    .unwrap();
    assert!(replace.contains(&format!("-F {input}")));
    assert!(replace.contains(&format!("-D FORWARD -d 192.0.2.2/32 -j {input}")));
    assert!(!replace.contains("UNRELATED"));
    assert!(!replace.contains("10.0.0.0/8"));
    let cleanup = filter_script(
        "runtime-1",
        "192.0.2.2".parse().unwrap(),
        None,
        &saved,
        true,
    )
    .unwrap();
    assert!(cleanup.contains(&format!("-X {input}")));
    assert!(!cleanup.contains("-I "));
    assert!(!cleanup.contains("UNRELATED"));
}

#[test]
fn rejects_ipv6_and_invalid_ports_instead_of_emitting_partial_policy() {
    assert!(filter_script("runtime-1", "::1".parse().unwrap(), None, "", false).is_err());
    let mut invalid = policy();
    invalid.traffic.as_mut().unwrap().rules[1].sandbox_port_range = Some(PortRange {
        first: 1,
        last: 70000,
    });
    assert!(filter_script(
        "runtime-1",
        "192.0.2.2".parse().unwrap(),
        Some(&invalid),
        "",
        false
    )
    .is_err());
}
