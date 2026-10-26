//! Cached aggregation and private queries over each env's most recent interruption.
use super::*;
use crate::storage::{BusinessCounters, CreationResult, RecoveryState};
use adx_core::observation::{now_millis, InterruptionReason};

pub(super) struct BusinessSnapshot {
    pub counters: BusinessCounters,
    pub refreshed: std::time::Instant,
}

fn operation_unknown(env: &StoredEnvironment) -> bool {
    env.invalidated
        && env
            .result
            .as_ref()
            .is_some_and(|r| r.observation.as_ref().is_some_and(|o| o.intent.is_some()))
}

impl CoordinatorRpc {
    pub(super) fn business_metrics(&self, state: &State) -> Result<String> {
        if self
            .0
            .business_fenced
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Conflict);
        }
        let snapshot = self
            .0
            .business
            .read()
            .map_err(|_| Error::Unavailable("business metrics cache unavailable".into()))?;
        let Some(snapshot) = snapshot.as_ref() else {
            return Err(Error::Unavailable(
                "business metrics not initialized".into(),
            ));
        };
        let c = &snapshot.counters;
        let history_known = u8::from(c.started_at_unix_millis != 0);
        let mut coverage = format!("# TYPE adx_observability_history_known gauge\nadx_observability_history_known {history_known}\n");
        if history_known == 1 {
            coverage.push_str(&format!("# TYPE adx_observability_started_timestamp_seconds gauge\nadx_observability_started_timestamp_seconds {}\n", c.started_at_unix_millis as f64 / 1000.0));
        }
        let mut output = coverage;
        output.push_str("# TYPE adx_environment_owned_creation_results_total counter\n");
        for (index, outcome) in ["success", "failure", "timeout"].iter().enumerate() {
            output.push_str(&format!(
                "adx_environment_owned_creation_results_total{{outcome=\"{outcome}\"}} {}\n",
                c.creations[index]
            ));
        }
        output.push_str("# TYPE adx_environment_interruptions_total counter\n");
        for (class_index, class) in ["runsc", "firecracker", "runc", "other"].iter().enumerate() {
            for (reason_index, reason) in InterruptionReason::ALL.iter().enumerate() {
                output.push_str(&format!("adx_environment_interruptions_total{{reason=\"{}\",runtime_class=\"{class}\"}} {}\n", reason.as_str(), c.interruptions[class_index][reason_index]));
            }
        }
        output.push_str(&format!("# TYPE adx_worker_incidents_total counter\nadx_worker_incidents_total{{reason=\"node_unreachable\"}} {}\n", c.worker_incidents));
        output.push_str("# TYPE adx_environment_recovery_results_total counter\n");
        for (index, result) in ["recovered", "unrecoverable", "closed_by_user"]
            .iter()
            .enumerate()
        {
            output.push_str(&format!(
                "adx_environment_recovery_results_total{{result=\"{result}\"}} {}\n",
                c.recoveries[index]
            ));
        }
        output.push_str(&format!("# TYPE adx_observability_snapshot_age_seconds gauge\nadx_observability_snapshot_age_seconds {}\n", snapshot.refreshed.elapsed().as_secs_f64()));
        let now = now_millis()?;
        let queued = state.scheduler.pending_requests();
        let queued_ages: Vec<_> = queued
            .iter()
            .filter(|item| !state.environments.contains_key(&item.spec.id))
            .map(|item| Some(item.enqueue_time_millis))
            .collect();
        let starting_ages: Vec<_> = state
            .environments
            .values()
            .filter(|env| {
                !env.invalidated
                    && env.creation.as_ref().map_or_else(
                        || env.result.is_none(),
                        |c| c.result == CreationResult::Unsettled,
                    )
            })
            .map(|env| env.creation.as_ref().map(|c| c.started_at_unix_millis))
            .collect();
        for name in [
            "adx_environment_creating",
            "adx_environment_create_oldest_wait_seconds",
            "adx_environment_create_wait_age_unknown",
        ] {
            output.push_str(&format!("# TYPE {name} gauge\n"));
        }
        for (name, ages) in [("queued", queued_ages), ("starting", starting_ages)] {
            let unknown = ages
                .iter()
                .filter(|age| age.is_none_or(|value| value == 0 || value > now))
                .count();
            output.push_str(&format!("adx_environment_creating{{state=\"{name}\"}} {}\nadx_environment_create_wait_age_unknown{{state=\"{name}\"}} {unknown}\n", ages.len()));
            if unknown == 0 {
                let oldest = ages
                    .into_iter()
                    .flatten()
                    .min()
                    .map_or(0, |start| now.saturating_sub(start));
                output.push_str(&format!(
                    "adx_environment_create_oldest_wait_seconds{{state=\"{name}\"}} {}\n",
                    oldest as f64 / 1000.0
                ));
            }
        }
        output.push_str("# TYPE adx_environment_unrecovered gauge\n");
        for status in [
            RecoveryState::Waiting,
            RecoveryState::Recovering,
            RecoveryState::Unrecoverable,
        ] {
            let count = state
                .environments
                .values()
                .filter(|e| {
                    e.last_interruption
                        .as_ref()
                        .is_some_and(|last| last.recovery_state == status)
                })
                .count();
            output.push_str(&format!(
                "adx_environment_unrecovered{{state=\"{}\"}} {count}\n",
                status.as_str()
            ));
        }
        let unknown = state
            .environments
            .values()
            .filter(|env| operation_unknown(env))
            .count();
        output.push_str(&format!("# TYPE adx_environment_operation_unknown gauge\nadx_environment_operation_unknown {unknown}\n"));
        Ok(output)
    }

    /// Private monitoring interface; excludes specs, credentials and arbitrary runtime data.
    pub async fn interruption_details(&self, query: &str) -> Result<serde_json::Value> {
        let mut filters = BTreeMap::new();
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if value.len() > 2048 {
                return Err(Error::Invalid("observation filter too long".into()));
            }
            if ![
                "reason",
                "worker_id",
                "recovery_state",
                "incident_id",
                "since",
                "until",
                "cursor",
                "limit",
                "include_resolved",
                "operation_unknown",
            ]
            .contains(&key.as_ref())
                || filters
                    .insert(key.into_owned(), value.into_owned())
                    .is_some()
            {
                return Err(Error::Invalid(
                    "invalid or duplicate observation filter".into(),
                ));
            }
        }
        let number = |name: &str, default: u64| -> Result<u64> {
            filters.get(name).map_or(Ok(default), |value| {
                value
                    .parse()
                    .map_err(|_| Error::Invalid(format!("invalid {name}")))
            })
        };
        let limit = number("limit", 100)?;
        let since = number("since", 0)?;
        let until = number("until", u64::MAX)?;
        if limit == 0 || limit > 1000 || since > until {
            return Err(Error::Invalid(
                "invalid observation range or page size".into(),
            ));
        }
        if let Some(reason) = filters.get("reason") {
            InterruptionReason::parse(reason)?;
        }
        if let Some(status) = filters.get("recovery_state") {
            if ![
                "waiting",
                "recovering",
                "unrecoverable",
                "recovered",
                "closed_by_user",
            ]
            .contains(&status.as_str())
            {
                return Err(Error::Invalid("invalid recovery state".into()));
            }
        }
        let resolved = match filters.get("include_resolved").map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            _ => return Err(Error::Invalid("invalid include_resolved".into())),
        };
        let unknown_operations = match filters.get("operation_unknown").map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            _ => return Err(Error::Invalid("invalid operation_unknown".into())),
        };
        if unknown_operations
            && [
                "reason",
                "recovery_state",
                "incident_id",
                "since",
                "until",
                "include_resolved",
            ]
            .iter()
            .any(|key| filters.contains_key(*key))
        {
            return Err(Error::Invalid(
                "operation_unknown supports worker_id and pagination only".into(),
            ));
        }
        let state = self
            .0
            .state
            .try_lock()
            .map_err(|_| Error::Unavailable("Coordinator state busy".into()))?;
        state.healthy()?;
        if self
            .0
            .business_fenced
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Conflict);
        }
        let matching: Vec<_> = state
            .environments
            .values()
            .filter(|env| {
                if unknown_operations {
                    return operation_unknown(env)
                        && filters
                            .get("worker_id")
                            .is_none_or(|wanted| wanted == &env.assignment.node_id);
                }
                let Some(last) = &env.last_interruption else {
                    return false;
                };
                let eq = |name: &str, actual: &str| {
                    filters.get(name).is_none_or(|wanted| wanted == actual)
                };
                (resolved
                    || filters.contains_key("recovery_state")
                    || last.recovery_state.unresolved())
                    && eq("reason", last.evidence.reason.as_str())
                    && eq("worker_id", &last.evidence.worker_id)
                    && eq("recovery_state", last.recovery_state.as_str())
                    && eq("incident_id", last.incident_id.as_deref().unwrap_or(""))
                    && last.evidence.detected_at_unix_millis >= since
                    && last.evidence.detected_at_unix_millis <= until
            })
            .collect();
        let workers: BTreeSet<_> = matching
            .iter()
            .map(|env| {
                if unknown_operations {
                    &env.assignment.node_id
                } else {
                    env.last_interruption
                        .as_ref()
                        .map_or(&env.assignment.node_id, |last| &last.evidence.worker_id)
                }
            })
            .collect();
        let after = filters.get("cursor").map(String::as_str).unwrap_or("");
        let mut page: Vec<_> = matching
            .iter()
            .filter(|env| env.spec.id.as_str() > after)
            .take(limit as usize + 1)
            .copied()
            .collect();
        let more = page.len() > limit as usize;
        if more {
            page.pop();
        }
        let cursor = more
            .then(|| page.last().map(|env| env.spec.id.clone()))
            .flatten();
        let items: Vec<_> = page.into_iter().map(|env| serde_json::json!({"environment_id":env.spec.id,
            "current_worker_id":env.assignment.node_id,"current_state":env.result.as_ref().map(|r| format!("{:?}",r.state)),
            "last_interruption":env.last_interruption,"operation_intent":env.result.as_ref().and_then(|r| r.observation.as_ref().and_then(|o| o.intent))})).collect();
        Ok(
            serde_json::json!({"snapshot_at_unix_millis":now_millis()?, "coordinator_epoch":state.session.epoch(),
            "coverage":if unknown_operations {"current_env_operation_unknown"} else {"current_env_latest_interruption"}, "pagination":"live_current_snapshot",
            "total_environments":matching.len(), "total_workers":workers.len(), "next_cursor":cursor, "items":items}),
        )
    }
}
