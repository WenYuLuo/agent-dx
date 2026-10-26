use crate::{control as pb, Error, Result};
use adx_core::observation::{
    CreateStage, EvidenceSource, ExecutionObservation, InterruptionEvidence, InterruptionReason,
    OperationIntent, ReloadRetry,
};

impl TryFrom<pb::InterruptionEvidence> for InterruptionEvidence {
    type Error = Error;
    fn try_from(v: pb::InterruptionEvidence) -> Result<Self> {
        let value = Self {
            event_id: v.event_id,
            detected_at_unix_millis: v.detected_at_unix_millis,
            assignment_generation: v.assignment_generation,
            runtime_id: v.runtime_id,
            worker_id: v.worker_id,
            reason: InterruptionReason::parse(&v.reason)?,
            evidence_source: EvidenceSource::parse(&v.evidence_source)?,
            exit_code: v.exit_code,
            detail: v.detail,
        };
        value.validate()?;
        Ok(value)
    }
}
impl From<InterruptionEvidence> for pb::InterruptionEvidence {
    fn from(v: InterruptionEvidence) -> Self {
        Self {
            event_id: v.event_id,
            detected_at_unix_millis: v.detected_at_unix_millis,
            assignment_generation: v.assignment_generation,
            runtime_id: v.runtime_id,
            worker_id: v.worker_id,
            reason: v.reason.as_str().into(),
            evidence_source: v.evidence_source.as_str().into(),
            exit_code: v.exit_code,
            detail: v.detail,
        }
    }
}
impl TryFrom<pb::ReloadRetry> for ReloadRetry {
    type Error = Error;
    fn try_from(v: pb::ReloadRetry) -> Result<Self> {
        let value = Self {
            id: v.id,
            expected_revision: v.expected_revision,
            runtime_id: v.runtime_id,
            checkpoint_id: v.checkpoint_id,
        };
        value.validate()?;
        Ok(value)
    }
}
impl From<ReloadRetry> for pb::ReloadRetry {
    fn from(v: ReloadRetry) -> Self {
        Self {
            id: v.id,
            expected_revision: v.expected_revision,
            runtime_id: v.runtime_id,
            checkpoint_id: v.checkpoint_id,
        }
    }
}

impl TryFrom<pb::ExecutionObservation> for ExecutionObservation {
    type Error = Error;
    fn try_from(v: pb::ExecutionObservation) -> Result<Self> {
        Ok(Self {
            reload_retry: v.reload_retry.map(TryInto::try_into).transpose()?,
            interruption: v.interruption.map(TryInto::try_into).transpose()?,
            intent: if v.intent.is_empty() {
                None
            } else {
                Some(OperationIntent::parse(&v.intent)?)
            },
            create_stage: if v.create_stage.is_empty() {
                None
            } else {
                Some(CreateStage::parse(&v.create_stage)?)
            },
        })
    }
}
impl From<ExecutionObservation> for pb::ExecutionObservation {
    fn from(v: ExecutionObservation) -> Self {
        Self {
            reload_retry: v.reload_retry.map(Into::into),
            interruption: v.interruption.map(Into::into),
            intent: v.intent.map_or("", OperationIntent::as_str).into(),
            create_stage: v.create_stage.map_or("", CreateStage::as_str).into(),
        }
    }
}

/// Attach bounded, structured provenance without changing the status or details.
pub fn create_status(mut status: tonic::Status, stage: CreateStage) -> tonic::Status {
    status.metadata_mut().insert(
        "adx-create-stage",
        tonic::metadata::MetadataValue::from_static(stage.as_str()),
    );
    status
}
