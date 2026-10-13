use crate::control as pb;
use adx_core::{probes::*, Error, Result};

impl From<ProbeSpec> for pb::ProbeSpec {
    fn from(p: ProbeSpec) -> Self {
        use pb::probe_spec::Action;
        let action = match p.action {
            ProbeAction::HttpGet { port, path } => Action::HttpGet(pb::HttpGetProbe {
                port: port.into(),
                path,
            }),
            ProbeAction::TcpSocket { port } => {
                Action::TcpSocket(pb::TcpSocketProbe { port: port.into() })
            }
            ProbeAction::Exec { command } => Action::Exec(pb::ExecProbe { command }),
        };
        Self {
            action: Some(action),
            initial_delay_seconds: p.initial_delay_seconds,
            period_seconds: p.period_seconds,
            timeout_seconds: p.timeout_seconds,
            failure_threshold: p.failure_threshold,
        }
    }
}
impl TryFrom<pb::ProbeSpec> for ProbeSpec {
    type Error = Error;
    fn try_from(p: pb::ProbeSpec) -> Result<Self> {
        use pb::probe_spec::Action;
        let port =
            |v| u16::try_from(v).map_err(|_| Error::Invalid("probe port out of range".into()));
        let action = match p
            .action
            .ok_or_else(|| Error::Invalid("probe action required".into()))?
        {
            Action::HttpGet(p) => ProbeAction::HttpGet {
                port: port(p.port)?,
                path: p.path,
            },
            Action::TcpSocket(p) => ProbeAction::TcpSocket {
                port: port(p.port)?,
            },
            Action::Exec(p) => ProbeAction::Exec { command: p.command },
        };
        let value = Self {
            action,
            initial_delay_seconds: p.initial_delay_seconds,
            period_seconds: p.period_seconds,
            timeout_seconds: p.timeout_seconds,
            failure_threshold: p.failure_threshold,
        };
        value.validate()?;
        Ok(value)
    }
}
impl From<ProbeFailure> for pb::ProbeFailure {
    fn from(v: ProbeFailure) -> Self {
        Self {
            kind: match v.kind {
                ProbeFailureKind::ReadinessTimeout => "readiness_timeout",
                ProbeFailureKind::HealthcheckFailed => "healthcheck_failed",
            }
            .into(),
            message: v.message,
            consecutive_failures: v.consecutive_failures,
            exit_code: v.exit_code,
            http_status: v.http_status.map(u32::from),
        }
    }
}
impl TryFrom<pb::ProbeFailure> for ProbeFailure {
    type Error = Error;
    fn try_from(v: pb::ProbeFailure) -> Result<Self> {
        let kind = match v.kind.as_str() {
            "readiness_timeout" => ProbeFailureKind::ReadinessTimeout,
            "healthcheck_failed" => ProbeFailureKind::HealthcheckFailed,
            _ => return Err(Error::Invalid("unknown probe failure kind".into())),
        };
        Ok(Self {
            kind,
            message: v.message,
            consecutive_failures: v.consecutive_failures,
            exit_code: v.exit_code,
            http_status: v
                .http_status
                .map(|v| u16::try_from(v).map_err(|_| Error::Invalid("invalid HTTP status".into())))
                .transpose()?,
        })
    }
}

impl TryFrom<Box<pb::ProbeSpec>> for ProbeSpec {
    type Error = Error;
    fn try_from(value: Box<pb::ProbeSpec>) -> Result<Self> {
        (*value).try_into()
    }
}
impl TryFrom<Box<pb::ProbeFailure>> for ProbeFailure {
    type Error = Error;
    fn try_from(value: Box<pb::ProbeFailure>) -> Result<Self> {
        (*value).try_into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_probe_failure_survives_grpc_and_record_conversion() {
        let failure = ProbeFailure {
            kind: ProbeFailureKind::HealthcheckFailed,
            message: "exec probe exited 7".into(),
            consecutive_failures: 3,
            exit_code: Some(7),
            http_status: None,
        };
        let wire: pb::ProbeFailure = failure.clone().into();
        assert_eq!(ProbeFailure::try_from(wire).unwrap(), failure);
        let persisted: adx_core::RuntimeFailure = failure.clone().into();
        let wire: pb::RuntimeFailure = persisted.clone().into();
        assert_eq!(adx_core::RuntimeFailure::try_from(wire).unwrap(), persisted);
        let status = crate::status(Error::ProbeFailed(failure.clone()));
        assert_eq!(status.code(), tonic::Code::FailedPrecondition);
        assert_eq!(
            crate::dependency_status(status),
            Error::ProbeFailed(failure)
        );
    }
}
