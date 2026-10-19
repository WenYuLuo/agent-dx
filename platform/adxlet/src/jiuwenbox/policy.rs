//! Ordered host IPv4 rules shared by both Jiuwenbox backends.
use crate::sandboxd::proto;
use adx_core::{execution::NetworkPolicy, Error, Result};
use sha2::{Digest, Sha256};
use std::net::{IpAddr, Ipv4Addr};

/// DNS enforcement and IPv6 require capabilities outside this adapter.
pub fn validate_network(policy: Option<&NetworkPolicy>) -> Result<()> {
    let Some(policy) = policy else {
        return Ok(());
    };
    policy.validate()?;
    if policy.dns.is_some() {
        return Err(Error::Invalid(
            "Jiuwenbox host filtering does not support DNS policies".into(),
        ));
    }
    if let Some(traffic) = &policy.traffic {
        for rule in &traffic.rules {
            let peer = &rule.peer;
            use adx_core::execution::NetworkProtocol;
            let ports = peer.port > 0
                || peer.port_range.is_some()
                || rule.environment_port > 0
                || rule.environment_port_range.is_some();
            if (ports && !matches!(rule.protocol, NetworkProtocol::Tcp | NetworkProtocol::Udp))
                || (!peer.address.is_empty() && !peer.cidr.is_empty())
                || (peer.port > 0 && peer.port_range.is_some())
                || (rule.environment_port > 0 && rule.environment_port_range.is_some())
            {
                return Err(Error::Invalid(
                    "ambiguous network selector or ports without TCP/UDP".into(),
                ));
            }

            if !peer.domain.is_empty() {
                return Err(Error::Invalid(
                    "network domain matching is unsupported".into(),
                ));
            }
            for text in [&peer.address, &peer.cidr]
                .into_iter()
                .filter(|v| !v.is_empty())
            {
                validate_address(text)?;
            }
        }
    }
    Ok(())
}

fn validate_address(text: &str) -> Result<()> {
    let (ip, bits) = text
        .split_once('/')
        .map_or((text, None), |(a, b)| (a, Some(b)));
    ip.parse::<Ipv4Addr>()
        .map_err(|_| Error::Invalid("IPv4 policy address required".into()))?;
    if bits.is_some_and(|b| b.parse::<u8>().map_or(true, |n| n > 32)) {
        return Err(Error::Invalid("invalid IPv4 policy prefix".into()));
    }
    Ok(())
}

pub(super) fn chains(id: &str) -> (String, String) {
    let hash = format!("{:x}", Sha256::digest(id.as_bytes()));
    (
        format!("ADXJI{}", &hash[..20]),
        format!("ADXJE{}", &hash[..20]),
    )
}

fn target(action: i32) -> Result<&'static str> {
    match proto::NetworkPolicyAction::try_from(action) {
        Ok(proto::NetworkPolicyAction::Allow) => Ok("RETURN"),
        Ok(proto::NetworkPolicyAction::Deny) => Ok("DROP"),
        _ => Err(Error::Invalid("unknown traffic action".into())),
    }
}

fn rule_args(rule: &proto::TrafficRule, ingress: bool) -> Result<String> {
    let protocol = match proto::NetworkProtocol::try_from(rule.protocol) {
        Ok(proto::NetworkProtocol::Any) => "",
        Ok(proto::NetworkProtocol::Tcp) => " -p tcp",
        Ok(proto::NetworkProtocol::Udp) => " -p udp",
        Ok(proto::NetworkProtocol::Icmp) => " -p icmp",
        _ => return Err(Error::Invalid("unknown traffic protocol".into())),
    };
    let mut args = protocol.to_owned();
    if let Some(peer) = &rule.peer {
        if !peer.domain.is_empty() {
            return Err(Error::Invalid("network domains unsupported".into()));
        }
        let address = if peer.cidr.is_empty() {
            &peer.address
        } else {
            &peer.cidr
        };
        if !address.is_empty() {
            validate_address(address)?;
            args.push_str(&format!(" {} {address}", if ingress { "-s" } else { "-d" }));
        }
        let range = peer.port_range.or_else(|| {
            (peer.port > 0).then_some(proto::PortRange {
                first: peer.port,
                last: peer.port,
            })
        });
        append_port(
            &mut args,
            range.as_ref(),
            if ingress { "--sport" } else { "--dport" },
            rule.protocol,
        )?;
    }
    let port = rule.sandbox_port_range.or_else(|| {
        (rule.sandbox_port > 0).then_some(proto::PortRange {
            first: rule.sandbox_port,
            last: rule.sandbox_port,
        })
    });
    append_port(
        &mut args,
        port.as_ref(),
        if ingress { "--dport" } else { "--sport" },
        rule.protocol,
    )?;
    args.push_str(&format!(" -j {}", target(rule.action)?));
    Ok(args)
}

fn append_port(
    args: &mut String,
    range: Option<&proto::PortRange>,
    flag: &str,
    protocol: i32,
) -> Result<()> {
    if let Some(range) = range {
        if !(protocol == proto::NetworkProtocol::Tcp as i32
            || protocol == proto::NetworkProtocol::Udp as i32)
            || range.first == 0
            || range.first > range.last
            || range.last > 65535
        {
            return Err(Error::Invalid("TCP/UDP port range required".into()));
        }
        args.push_str(&format!(" {flag} {}:{}", range.first, range.last));
    }
    Ok(())
}

/// One restore transaction replaces this runtime's rules and exact hooks.
pub fn filter_script(
    id: &str,
    ip: IpAddr,
    policy: Option<&proto::NetworkPolicy>,
    saved: &str,
    remove: bool,
) -> Result<String> {
    if !ip.is_ipv4() {
        return Err(Error::Invalid("isolated IPv4 networking required".into()));
    }
    let (input, output) = chains(id);
    let mut script = String::from("*filter\n");
    if !remove {
        script.push_str(&format!(
            ":{input} - [0:0]\n:{output} - [0:0]\n-F {input}\n-F {output}\n"
        ));
    }
    for line in saved.lines().filter(|line| line.starts_with("-A ")) {
        let words: Vec<_> = line.split_whitespace().collect();
        if words
            .windows(2)
            .any(|w| w[0] == "-j" && (w[1] == input || w[1] == output))
        {
            script.push_str(&format!("-D {}\n", &line[3..]));
        }
    }
    if remove {
        for chain in [&input, &output] {
            if saved
                .lines()
                .any(|line| line.starts_with(&format!(":{chain} ")))
            {
                script.push_str(&format!("-F {chain}\n-X {chain}\n"));
            }
        }
    } else {
        if policy.is_some_and(|p| p.dns.is_some()) {
            return Err(Error::Invalid("DNS policies unsupported".into()));
        }
        for (chain, ingress) in [(&input, true), (&output, false)] {
            if let Some(traffic) = policy.and_then(|p| p.traffic.as_ref()) {
                if traffic.mode == proto::TrafficPolicyMode::Stateful as i32 {
                    script.push_str(&format!(
                        "-A {chain} -m conntrack --ctstate ESTABLISHED,RELATED -j RETURN\n"
                    ));
                }
                let mut rules: Vec<_> = traffic.rules.iter().collect();
                rules.sort_by_key(|r| std::cmp::Reverse(r.priority));
                for rule in rules {
                    let direction = proto::NetworkDirection::try_from(rule.direction)
                        .map_err(|_| Error::Invalid("invalid network direction".into()))?;
                    if direction == proto::NetworkDirection::Both
                        || (ingress && direction == proto::NetworkDirection::Ingress)
                        || (!ingress && direction == proto::NetworkDirection::Egress)
                    {
                        script.push_str(&format!("-A {chain}{}\n", rule_args(rule, ingress)?));
                    }
                }
                let default = if ingress {
                    traffic.ingress_default_action
                } else {
                    traffic.egress_default_action
                };
                script.push_str(&format!("-A {chain} -j {}\n", target(default)?));
            } else {
                script.push_str(&format!("-A {chain} -j RETURN\n"));
            }
        }
        for hook in ["FORWARD", "OUTPUT"] {
            script.push_str(&format!("-I {hook} 1 -d {ip}/32 -j {input}\n"));
        }
        for hook in ["FORWARD", "INPUT"] {
            script.push_str(&format!("-I {hook} 1 -s {ip}/32 -j {output}\n"));
        }
    }
    script.push_str("COMMIT\n");
    Ok(script)
}
