use super::*;
use adx_core::observation::{
    bounded_detail, now_millis, CreateStage, EvidenceSource, InterruptionEvidence,
    InterruptionReason, OperationIntent,
};

impl Controller {
    pub(super) fn intended_execution_absence(&self) -> bool {
        self.record
            .observation
            .as_ref()
            .is_some_and(|o| o.intent.is_some())
            && self
                .record
                .failure
                .as_ref()
                .is_none_or(|failure| failure.reason == adx_core::RuntimeFailureReason::Missing)
    }

    pub(super) fn mark_interruption(&mut self, source: EvidenceSource) -> Result<()> {
        if self.record.state != EnvironmentState::Running {
            return Ok(());
        }
        let observation = self.record.observation.get_or_insert_with(Default::default);
        if observation.interruption.as_ref().is_some_and(|old| {
            old.runtime_id == self.record.runtime.id
                && old.assignment_generation == self.record.assignment.generation
        }) {
            return Ok(());
        }
        let reason = self.record.failure.as_ref().map_or_else(
            || {
                if source == EvidenceSource::Control {
                    InterruptionReason::ControlUnhealthy
                } else {
                    InterruptionReason::Unknown
                }
            },
            |failure| match failure.reason {
                adx_core::RuntimeFailureReason::OomKilled => InterruptionReason::Oom,
                adx_core::RuntimeFailureReason::Exited => InterruptionReason::RuntimeExit,
                adx_core::RuntimeFailureReason::Missing => InterruptionReason::ExecutionMissing,
                adx_core::RuntimeFailureReason::ProbeFailed => InterruptionReason::ProbeFailed,
                _ => InterruptionReason::Unknown,
            },
        );
        observation.intent = None;
        observation.interruption = Some(InterruptionEvidence {
            event_id: format!(
                "execution:{}:{}",
                self.record.assignment.generation, self.record.runtime.id
            ),
            detected_at_unix_millis: now_millis()?,
            assignment_generation: self.record.assignment.generation,
            runtime_id: self.record.runtime.id.clone(),
            worker_id: self.record.assignment.node_id.clone(),
            reason,
            evidence_source: source,
            exit_code: self.record.failure.as_ref().and_then(|f| f.exit_code),
            detail: bounded_detail(
                self.record
                    .failure
                    .as_ref()
                    .map_or("execution unavailable", |f| f.message.as_str()),
            ),
        });
        Ok(())
    }
    pub(super) fn create_stage(&mut self, stage: CreateStage) {
        self.record
            .observation
            .get_or_insert_with(Default::default)
            .create_stage = Some(stage);
    }
    pub(super) fn operation_intent(&mut self, intent: OperationIntent) {
        self.record
            .observation
            .get_or_insert_with(Default::default)
            .intent = Some(intent);
    }
    pub(super) async fn clear_operation_intent(&mut self) -> Result<()> {
        if self.record.state == EnvironmentState::Running
            && !self
                .cleanup_pending
                .load(std::sync::atomic::Ordering::Acquire)
            && self
                .record
                .observation
                .as_ref()
                .is_some_and(|o| o.intent.is_some())
        {
            self.record
                .observation
                .as_mut()
                .ok_or(Error::Conflict)?
                .intent = None;
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.durability = None;
            if self.sync().await?.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "operation intent publication unconfirmed".into(),
                ));
            }
        }
        Ok(())
    }
    pub(super) async fn publish_operation_intent(&mut self) -> Result<()> {
        if self.record.state == EnvironmentState::Running {
            self.record.revision = self.record.revision.checked_add(1).ok_or(Error::Conflict)?;
            self.durability = None;
            if self.sync().await?.durability != Durability::Published {
                return Err(Error::Unavailable(
                    "operation intent publication unconfirmed".into(),
                ));
            }
        }
        Ok(())
    }
}
