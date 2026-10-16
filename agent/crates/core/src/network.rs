//! Deployment ACL uses IPv4 CIDRs supported by the current sandboxd backend.
//! Deployment-owned network policy for template-created sandboxes.
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxNetworkPolicy {
    /// All sandbox address pools; explicit service exceptions may not overlap them.
    pub sandbox_cidrs: Vec<String>,
    /// Additional cluster/host networks excluded from public egress.
    pub internal_cidrs: Vec<String>,
    pub public_egress: bool,
    pub egress_allow: Vec<EgressAllow>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EgressAllow {
    pub cidr: String,
    pub protocol: EgressProtocol,
    pub ports: Option<PortRange>,
}
#[derive(Debug, Clone, Copy, Deserialize, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum EgressProtocol {
    Tcp,
    Udp,
    Icmp,
}
#[derive(Debug, Clone, Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PortRange {
    pub first: u16,
    pub last: u16,
}
