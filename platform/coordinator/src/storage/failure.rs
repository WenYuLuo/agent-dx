//! Persist node expiry and all retired execution results in one Redis script.
use super::*;

impl Session {
    pub async fn invalidate_node(&self, id: &str, session_id: &str) -> Result<StoredSnapshot> {
        for _ in 0..ATTEMPTS {
            let snapshots = self.retained_snapshots().await?;
            let raw = self.store.raw().await?;
            let header = self.header(&raw.get(HEADER).cloned())?;
            let mut counters = CounterUpdate::load(&self.store, format!("node:{id}")).await?;
            let mut saved = snapshot(&raw)?;
            saved.revision = self
                .store
                .connection
                .publication_sequence
                .load(Ordering::Acquire);
            let node = saved.nodes.get_mut(id).ok_or(Error::NotFound)?;
            if node.session.as_ref().is_none_or(|s| s.id != session_id) {
                return Err(Error::Conflict);
            }
            let incident_id = match &node.last_incident_id {
                Some(incident) => incident.clone(),
                None => format!("node:{id}:{session_id}:{}", self.next_generation()?),
            };
            if node.last_incident_id.is_none() {
                counters.value.worker_incidents = counters
                    .value
                    .worker_incidents
                    .checked_add(1)
                    .ok_or(Error::Conflict)?;
                node.last_incident_id = Some(incident_id.clone());
            }
            node.node.available = false;
            node.session.as_mut().ok_or(Error::Conflict)?.routable = false;
            let mut holds = Vec::new();
            let mut writes = vec![(format!("node:{id}"), encode(node)?)];
            for (environment_id, environment) in &mut saved.environments {
                if environment.assignment.node_id != id
                    || environment.invalidated
                    || environment
                        .result
                        .as_ref()
                        .is_some_and(|r| r.state == EnvironmentState::Deleted)
                {
                    continue;
                }
                environment.observe_node_loss(&incident_id, &mut counters.value)?;
                let mut result = environment
                    .result
                    .clone()
                    .unwrap_or_else(|| EnvironmentRecord {
                        observation: None,
                        execution_artifacts: vec![],
                        snapshot_source: None,
                        failure: None,
                        spec: environment.spec.clone(),
                        assignment: environment.assignment.clone(),
                        runtime: adx_core::Runtime {
                            id: format!("{}-{}", environment_id, environment.assignment.generation),
                            ip: None,
                        },
                        revision: 0,
                        state: EnvironmentState::Pending,
                        resources_held: true,
                        checkpoint: None,
                        last_operation: None,
                        restart_attempts: 0,
                        restart_pending: false,
                    });
                let mut inputs = result.execution_artifacts.clone();
                if result.resources_held {
                    if let Some(snapshot) = snapshots
                        .iter()
                        .find(|s| environment.spec.snapshot_id.as_deref() == Some(s.id.as_str()))
                    {
                        if !inputs.contains(&snapshot.artifact) {
                            inputs.push(snapshot.artifact.clone());
                        }
                    }
                    for point in result
                        .checkpoint
                        .iter()
                        .chain(result.snapshot_source.iter())
                    {
                        if !inputs.contains(&point.artifact) {
                            inputs.push(point.artifact.clone());
                        }
                    }
                }
                if !inputs.is_empty() {
                    holds.push((
                        artifacts::hold_key(&environment.assignment),
                        encode(&artifacts::ExecutionHold {
                            assignment: environment.assignment.clone(),
                            artifacts: inputs,
                        })?,
                    ));
                }
                // Losing a node retires execution ownership, not the user's
                // dormancy intent. A paused restore point is awakened on demand.
                let autonomous_recovery = environment.resume.is_none()
                    && environment.recovery.as_ref().is_some_and(|r| r.pending);
                if result.state != EnvironmentState::Paused || autonomous_recovery {
                    result.state = EnvironmentState::Failed;
                }
                result.revision = result.revision.checked_add(1).ok_or(Error::Conflict)?;
                result.resources_held = false;
                result.runtime.ip = None;
                result.restart_pending = false;
                result.last_operation = None;
                if let Some(observation) = result.observation.as_mut() {
                    observation.interruption = None;
                }
                environment.invalidated = true;
                environment.observe_result(&result, &mut counters.value)?;
                environment.result = Some(result);
                environment.track_failure(super::collection::now()?);
                environment.validate()?;
                writes.push((
                    format!("environment:{environment_id}"),
                    encode(environment)?,
                ));
            }
            if writes
                .iter()
                .all(|(field, value)| raw.get(field) == Some(value))
                && !raw.contains_key(&format!("node-session:{id}"))
            {
                return Ok(saved);
            }
            saved.revision = header.revision;
            saved.validate()?;
            // Validate/encode all data before the script. No fallible decoding or
            // floating point version comparisons occur between its writes.
            let mut expected: BTreeMap<_, _> = writes
                .iter()
                .map(|(field, _)| (field.clone(), raw.get(field).cloned().unwrap_or_default()))
                .collect();
            // Fence new assignments to this node that were absent from the scan.
            let membership = format!("node-ownership:{id}");
            expected.insert(
                membership.clone(),
                raw.get(&membership).cloned().unwrap_or_default(),
            );
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                if redis.call('HGET', KEYS[1], 'epoch') ~= ARGV[1] then return 0 end
                local expected = cjson.decode(ARGV[2])
                for field,value in pairs(expected) do if (redis.call('HGET',KEYS[1],field) or '') ~= value then return 0 end end
                local facts=cjson.decode(ARGV[3])
                if (redis.call('HGET',KEYS[3],facts[1]) or '') ~= facts[2] then return 0 end
                redis.call('HSET',KEYS[3],facts[1],facts[3])
                local count=tonumber(ARGV[4]); for i=5,4+count*2,2 do redis.call('HSET',KEYS[1],ARGV[i],ARGV[i+1]) end
                for i=5+count*2,#ARGV-1,2 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+1]) end
                redis.call('HDEL',KEYS[1],ARGV[#ARGV]); return 1
            "#,
                )
                .arg(3)
                .arg(&self.store.key)
                .arg(format!("{}:input-holds",self.store.key))
                .arg(format!("{}:business",self.store.key))
                .arg(self.epoch.to_string())
                .arg(encode(&expected)?)
                .arg(counters.argument()?)
                .arg(writes.len());
            let changed = writes.iter().map(|(field, _)| field.clone()).collect();
            for (field, value) in writes {
                command.arg(field).arg(value);
            }
            let cached_holds = holds
                .iter()
                .map(|(field, value)| {
                    Ok((field.clone(), decode::<artifacts::ExecutionHold>(value)?))
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            for (field, value) in holds {
                command.arg(field).arg(value);
            }
            let protected = cached_holds
                .values()
                .flat_map(|hold| hold.artifacts.clone())
                .collect::<Vec<_>>();
            let _access = self.store.lock_artifacts(&protected)?;
            command.arg(format!("node-session:{id}"));
            let mut inventory = self.inspection.lock().await;
            let mut committed = inventory.take();
            let applied = match self.store.query::<u8>(command).await {
                Ok(applied) => applied,
                Err(error) => {
                    *inventory = None;
                    return Err(error);
                }
            };
            if applied == 1 {
                if let Some(inventory) = committed.as_mut() {
                    inventory.holds.extend(cached_holds);
                }
                *inventory = committed;
                self.observe_node(saved.nodes[id].clone());
                self.store.notify_committed_change(header.revision, changed);
                saved.revision = self
                    .store
                    .connection
                    .publication_sequence
                    .load(Ordering::Acquire);
                for (id, env) in &saved.environments {
                    let previous = raw
                        .get(&format!("environment:{id}"))
                        .and_then(|value| decode::<StoredEnvironment>(value).ok());
                    env.log_observation_change(
                        previous
                            .as_ref()
                            .and_then(|env| env.last_interruption.as_ref()),
                    );
                }
                return Ok(saved);
            }
            *inventory = committed;
        }
        Err(Error::Unavailable(
            "concurrent node invalidation; retry request".into(),
        ))
    }
}
