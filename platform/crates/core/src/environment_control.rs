//! Optional compatibility configuration, run defaults, and live owner timer views.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentCompatibility {
    pub template_id: String,
    pub envd_access_token: String,
    pub traffic_access_token: String,
    pub allow_public_traffic: bool,
}
impl fmt::Debug for EnvironmentCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentCompatibility")
            .field("template_id", &self.template_id)
            .field("credentials", &"[redacted]")
            .finish()
    }
}
impl EnvironmentCompatibility {
    pub fn validate(&self) -> Result<()> {
        if self.template_id.is_empty()
            || self.envd_access_token.len() < 32
            || self.traffic_access_token.len() < 32
        {
            return Err(Error::Invalid(
                "invalid sandbox compatibility configuration".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunExpiryAction {
    Delete,
    Pause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunPolicy {
    pub timeout_seconds: u64,
    pub action: RunExpiryAction,
}

impl RunPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.timeout_seconds == 0 {
            return Err(Error::Invalid("run timeout must be positive".into()));
        }
        let _ = self
            .timeout_seconds
            .checked_mul(1000)
            .ok_or_else(|| Error::Invalid("run timeout milliseconds overflow".into()))?;
        std::time::Instant::now()
            .checked_add(std::time::Duration::from_secs(self.timeout_seconds))
            .ok_or_else(|| Error::Invalid("run timeout out of range".into()))?;
        Ok(())
    }
}

/// A live owner-process view. Wall-clock times are estimates for display only.
/// Scheduling and timeout enforcement use the owner's in-memory monotonic timer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentControlRecord {
    pub environment_id: String,
    pub expires_at_unix_ms: u64,
    pub started_at_unix_ms: u64,
    pub revision: u64,
    pub generation: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_policy_rejects_zero_and_overflow() {
        for timeout_seconds in [0, u64::MAX] {
            assert!(RunPolicy {
                timeout_seconds,
                action: RunExpiryAction::Delete
            }
            .validate()
            .is_err());
        }
        assert!(RunPolicy {
            timeout_seconds: 60,
            action: RunExpiryAction::Pause
        }
        .validate()
        .is_ok());
    }
}
