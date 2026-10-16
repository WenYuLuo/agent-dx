//! Typed Agent template settings forwarded to the public Sandbox lifecycle API.
//! Platform owns idle detection, checkpoint retention and on-demand recovery.
use crate::ValidationResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LifecyclePolicy {
    /// Zero disables idle reclamation. Open business streams count as activity.
    pub idle_timeout_seconds: u64,
    pub idle_action: IdleAction,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pause: Option<PausePolicy>,
}
impl LifecyclePolicy {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }
    pub fn validate(&self) -> ValidationResult {
        if self.idle_timeout_seconds > i64::MAX as u64 {
            return Err("idle timeout exceeds the public Sandbox API range".into());
        }
        if self.idle_action == IdleAction::Pause && self.idle_timeout_seconds == 0 {
            return Err("idle pause requires a positive idle timeout".into());
        }
        if let Some(pause) = &self.pause {
            if !(1..=3600).contains(&pause.timeout_seconds) {
                return Err("pause timeout must be between 1 and 3600 seconds".into());
            }
            if matches!(pause.retention, RetentionPolicy::Ttl { ttl_seconds: 0 }) {
                return Err("pause retention TTL must be positive".into());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdleAction {
    #[default]
    Delete,
    Pause,
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
            retention: RetentionPolicy::Ttl {
                ttl_seconds: 90_000,
            },
            timeout_seconds: 300,
            storage: CheckpointStorage::Configured,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RetentionPolicy {
    Ttl { ttl_seconds: u64 },
    Forever {},
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStorage {
    #[default]
    Configured,
    Local,
    Shared,
}
