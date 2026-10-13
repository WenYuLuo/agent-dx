//! Workload checks executed inside Execd, with lifecycle decisions owned by Adxlet.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

pub const CONFIG_ENV: &str = "ADX_WORKLOAD_PROBES";
pub const MAX_FAILURE_BYTES: usize = 4096;
pub const fn default_init_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProbeAction {
    HttpGet { port: u16, path: String },
    TcpSocket { port: u16 },
    Exec { command: Vec<String> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeSpec {
    pub action: ProbeAction,
    pub initial_delay_seconds: u64,
    pub period_seconds: u64,
    pub timeout_seconds: u64,
    pub failure_threshold: u32,
}
impl ProbeSpec {
    pub fn validate(&self) -> Result<()> {
        if self.period_seconds == 0
            || self.timeout_seconds == 0
            || self.failure_threshold == 0
            || self.period_seconds > 86400
            || self.timeout_seconds > 3600
            || self.initial_delay_seconds > 86400
        {
            return Err(Error::Invalid(
                "probe timing or failure threshold is out of range".into(),
            ));
        }
        match &self.action {
            ProbeAction::HttpGet { port, path }
                if *port == 0
                    || !path.starts_with('/')
                    || path.starts_with("//")
                    || path.len() > 2048
                    || path.bytes().any(|c| c <= b' ' || c == 127) =>
            {
                return Err(Error::Invalid(
                    "HTTP probe requires a port and an origin-form path".into(),
                ));
            }
            ProbeAction::TcpSocket { port } if *port == 0 => {
                return Err(Error::Invalid("probe port must be positive".into()));
            }
            ProbeAction::Exec { command }
                if command.is_empty()
                    || command[0].is_empty()
                    || command.len() > 256
                    || command.iter().any(|arg| arg.contains('\0'))
                    || command.iter().map(String::len).sum::<usize>() > 16384 =>
            {
                return Err(Error::Invalid(
                    "exec probe requires a bounded nonempty argv without NUL".into(),
                ));
            }
            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeConfig {
    pub readiness: Option<ProbeSpec>,
    pub healthcheck: Option<ProbeSpec>,
    pub init_timeout_seconds: u64,
}
impl ProbeConfig {
    pub fn validate(&self) -> Result<()> {
        if self.init_timeout_seconds == 0 || self.init_timeout_seconds > 86400 {
            return Err(Error::Invalid(
                "init timeout must be between 1 and 86400 seconds".into(),
            ));
        }
        for probe in [&self.readiness, &self.healthcheck].into_iter().flatten() {
            probe.validate()?;
        }
        if self
            .readiness
            .as_ref()
            .is_some_and(|p| p.failure_threshold != 1)
        {
            return Err(Error::Invalid(
                "readiness retries until the initialization deadline".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeFailureKind {
    ReadinessTimeout,
    HealthcheckFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeFailure {
    pub kind: ProbeFailureKind,
    pub message: String,
    pub consecutive_failures: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
}
impl ProbeFailure {
    pub fn timeout(message: impl Into<String>) -> Self {
        Self {
            kind: ProbeFailureKind::ReadinessTimeout,
            message: message.into(),
            consecutive_failures: 0,
            exit_code: None,
            http_status: None,
        }
    }
}

impl From<ProbeFailure> for crate::RuntimeFailure {
    fn from(failure: ProbeFailure) -> Self {
        Self {
            reason: crate::RuntimeFailureReason::ProbeFailed,
            exit_code: failure.exit_code,
            message: failure.message.clone(),
            probe_failure: Some(failure),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_http_request_injection_and_empty_exec() {
        let mut probe = ProbeSpec {
            action: ProbeAction::HttpGet {
                port: 80,
                path: "/ready\r\nX: injected".into(),
            },
            initial_delay_seconds: 0,
            period_seconds: 2,
            timeout_seconds: 1,
            failure_threshold: 1,
        };
        assert!(probe.validate().is_err());
        probe.action = ProbeAction::Exec { command: vec![] };
        assert!(probe.validate().is_err());
    }
}
