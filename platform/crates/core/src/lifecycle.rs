use crate::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LifecyclePolicy {
    /// Zero disables the idle action; this is not an Environment maximum lifetime.
    pub idle_timeout_seconds: u64,
    pub idle_action: IdleAction,
    pub pause: Option<PausePolicy>,
    pub restart: Option<RestartPolicy>,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdleAction {
    #[default]
    Delete,
    Pause,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStorage {
    /// Use the node's configured checkpoint backend.
    #[default]
    Configured,
    Local,
    /// Require publication to the configured shared object store.
    Shared,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PausePolicy {
    pub retention: RetentionPolicy,
    pub timeout_seconds: u64,
    pub storage: CheckpointStorage,
}
impl Default for PausePolicy {
    fn default() -> Self {
        Self {
            retention: RetentionPolicy::default(),
            timeout_seconds: 300,
            storage: CheckpointStorage::Configured,
        }
    }
}
impl PausePolicy {
    pub fn validate(&self) -> Result<()> {
        self.retention.validate()?;
        if self.timeout_seconds == 0 || self.timeout_seconds > 3600 {
            return Err(Error::Invalid(
                "pause requires positive retention and timeout at most 3600 seconds".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetentionPolicy {
    Ttl { ttl_seconds: u64 },
    Forever,
}
impl Default for RetentionPolicy {
    fn default() -> Self {
        Self::Ttl {
            ttl_seconds: 90_000,
        }
    }
}
impl RetentionPolicy {
    pub fn validate(self) -> Result<()> {
        if matches!(self, Self::Ttl { ttl_seconds: 0 }) {
            return Err(Error::Invalid("retention TTL must be positive".into()));
        }
        Ok(())
    }
    pub fn expires_at(self, now: u64) -> Result<Option<u64>> {
        self.validate()?;
        match self {
            Self::Forever => Ok(None),
            Self::Ttl { ttl_seconds } => now
                .checked_add(ttl_seconds)
                .map(Some)
                .ok_or_else(|| Error::Invalid("checkpoint TTL overflow".into())),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartPolicy {
    pub max_attempts: u32,
    pub initial_backoff_seconds: u64,
    pub max_backoff_seconds: u64,
}
impl LifecyclePolicy {
    pub fn validate(&self) -> Result<()> {
        if let Some(p) = &self.pause {
            p.validate()?;
        }
        if self.idle_action == IdleAction::Pause && self.idle_timeout_seconds == 0 {
            return Err(Error::Invalid(
                "idle pause requires a positive idle timeout".into(),
            ));
        }
        if let Some(p) = &self.restart {
            if p.max_attempts == 0
                || p.initial_backoff_seconds == 0
                || p.max_backoff_seconds < p.initial_backoff_seconds
            {
                return Err(Error::Invalid(
                    "restart requires attempts and 0 < initial backoff <= maximum backoff".into(),
                ));
            }
        }
        Ok(())
    }
}
impl RestartPolicy {
    pub fn backoff_seconds(&self, completed_attempts: u32) -> u64 {
        self.initial_backoff_seconds
            .saturating_mul(1_u64.checked_shl(completed_attempts).unwrap_or(u64::MAX))
            .min(self.max_backoff_seconds)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_pause_policy_has_independent_retention_and_storage() {
        let policy: LifecyclePolicy = serde_json::from_value(serde_json::json!({
            "idle_timeout_seconds": 30,
            "idle_action": "pause",
            "pause": {"retention": {"kind":"ttl", "ttl_seconds":3600}, "timeout_seconds": 120, "storage": "shared"}
        }))
        .expect("typed idle-pause policy must be accepted");
        policy.validate().unwrap();
    }
}
