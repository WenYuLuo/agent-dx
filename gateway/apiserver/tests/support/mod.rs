pub fn network_policy() -> adx_agent_core::network::SandboxNetworkPolicy {
    serde_json::from_value(serde_json::json!({
        "sandbox_cidrs": ["10.233.16.0/20"],
        "internal_cidrs": [],
        "public_egress": false,
        "egress_allow": []
    }))
    .unwrap()
}
