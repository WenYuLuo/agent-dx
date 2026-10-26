//! Atomic ownership registration for a locally admitted candidate. This is a
//! storage primitive, not a scheduling/admission RPC: the coordinator must fence
//! heartbeat expiry, apply placement rules and update its scheduling ledger.
use super::*;
use adx_core::{
    scheduling::DeviceAllocation,
    snapshots::{Reference, Snapshot},
};
use std::collections::{BTreeMap, BTreeSet};

const CLAIM_BATCH: &str = r#"
if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
local cursor = 4
for _ = 1, tonumber(ARGV[3]) do
  if redis.call('HGET', KEYS[2], ARGV[cursor]) ~= ARGV[cursor + 1] then return 0 end
  cursor = cursor + 2
end
local writes = tonumber(ARGV[2])
redis.call('HSET', KEYS[1], 'header', ARGV[cursor])
cursor = cursor + 1
for _ = 1, writes do
  redis.call('HSET', KEYS[1], ARGV[cursor], ARGV[cursor + 1])
  cursor = cursor + 2
end
for i = cursor, #ARGV, 2 do redis.call('HSET', KEYS[3], ARGV[i], ARGV[i+1]) end
return 1
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalClaim {
    pub request_id: String,
    pub node_id: String,
    pub node_session_id: String,
    /// Cards already tentatively held by the node's shared Admission ledger.
    pub devices: Vec<DeviceAllocation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// This node owns a pending initial creation. Includes replay after a lost
    /// acknowledgement; start through the existing per-Environment controller.
    Owned(StoredEnvironment),
    /// Another node owns it, or a result/retirement/recovery already exists.
    /// Do not start a new runtime. Reuse/query this record and release only the
    /// losing attempt's tentative resources, never an existing execution's hold.
    Existing(StoredEnvironment),
}
impl ClaimOutcome {
    pub fn record(&self) -> &StoredEnvironment {
        match self {
            Self::Owned(record) | Self::Existing(record) => record,
        }
    }
}

impl Session {
    /// Fence CAS commands from completed-but-unacknowledged operations. Call only
    /// after their producer has returned; a plain NotFound read is not a barrier.
    pub async fn claim_barrier(&self) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let [header_value] = self.store.fields([HEADER.into()]).await?;
            let mut header = self.header(&header_value)?;
            header.advance()?;
            if self
                .store
                .cas(
                    header_value.as_deref().ok_or(Error::Conflict)?,
                    &header,
                    None,
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable("claim barrier contended".into()))
    }

    /// Register one globally unique owner for a normalized EnvironmentSpec.
    ///
    /// The spec (including tenant) must match on every retry. The Environment ID is
    /// the idempotency key; retries must not generate a new ID. Generation and
    /// ownership are committed in the same Redis CAS. On an unavailable result,
    /// retry this operation or query `get`; a timeout does not prove no write.
    /// This never transfers ownership or reactivates a completed Environment.
    pub async fn claim(
        &self,
        spec: EnvironmentSpec,
        candidate: &LocalClaim,
    ) -> Result<ClaimOutcome> {
        let mut results = self.claim_batch(vec![(spec, candidate.clone())]).await?;
        results.pop().ok_or(Error::Conflict)?
    }

    /// Commit independent local ownership requests with one header CAS. The
    /// returned vector preserves request order; a conflict in one entry does not
    /// discard other valid claims in the same batch.
    pub async fn claim_batch(
        &self,
        claims: Vec<(EnvironmentSpec, LocalClaim)>,
    ) -> Result<Vec<Result<ClaimOutcome>>> {
        if claims.is_empty() {
            return Ok(Vec::new());
        }
        for (spec, candidate) in &claims {
            spec.validate()?;
            if candidate.request_id.len() > 256 {
                return Err(Error::Invalid("request ID too long".into()));
            }
            if candidate.node_id.trim().is_empty() || candidate.node_session_id.trim().is_empty() {
                return Err(Error::Invalid(
                    "claim requires node and process session".into(),
                ));
            }
            validate_device_assignment(&spec.scheduling.devices, &candidate.devices)?;
        }
        let environment_fields: BTreeSet<_> = claims
            .iter()
            .map(|(spec, _)| format!("environment:{}", spec.id))
            .collect();
        let node_fields: BTreeSet<_> = claims
            .iter()
            .map(|(_, candidate)| format!("node:{}", candidate.node_id))
            .collect();
        for _ in 0..ATTEMPTS {
            let fields: Vec<_> = std::iter::once(HEADER.to_owned())
                .chain(environment_fields.iter().cloned())
                .chain(node_fields.iter().cloned())
                .collect();
            let values = self.store.fields_vec(&fields).await?;
            let mut by_field: BTreeMap<_, _> = fields.into_iter().zip(values).collect();
            let header_value = by_field.remove(HEADER).flatten().ok_or(Error::Conflict)?;
            let mut header = self.header(&Some(header_value.clone()))?;
            let mut environments = BTreeMap::new();
            for field in &environment_fields {
                if let Some(value) = by_field.remove(field).flatten() {
                    let record: StoredEnvironment = decode(&value)?;
                    record.validate()?;
                    environments.insert(record.spec.id.clone(), record);
                }
            }
            let mut nodes = BTreeMap::new();
            for field in &node_fields {
                let value = by_field.remove(field).flatten().ok_or(Error::NotFound)?;
                let node: StoredNode = decode(&value)?;
                nodes.insert(node.node.id.clone(), node);
            }
            let snapshot_ids: BTreeSet<_> = claims
                .iter()
                .filter(|(spec, _)| !environments.contains_key(&spec.id))
                .filter_map(|(spec, _)| spec.snapshot_id.clone())
                .collect();
            let mut snapshots = BTreeMap::new();
            for id in &snapshot_ids {
                let raw = self.snapshot_raw(id).await?.ok_or(Error::NotFound)?;
                let snapshot: Snapshot = decode(&raw)?;
                snapshot.validate()?;
                snapshots.insert(id.clone(), (raw, snapshot));
            }

            let request_fields: Vec<_> = claims
                .iter()
                .filter(|(_, c)| !c.request_id.is_empty())
                .map(|(s, c)| request_key(&s.tenant_id, &c.request_id))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            let mut requests: BTreeMap<String, String> = BTreeMap::new();
            if !request_fields.is_empty() {
                let mut cmd = redis::cmd("HMGET");
                cmd.arg(format!("{}:requests", self.store.key))
                    .arg(&request_fields);
                let values: Vec<Option<String>> = self.store.query(cmd).await?;
                for (key, value) in request_fields.iter().cloned().zip(values) {
                    if let Some(value) = value {
                        requests.insert(key, value);
                    }
                }
            }
            let mut request_writes = BTreeMap::new();
            let mut results = Vec::with_capacity(claims.len());
            let mut writes = Vec::new();
            for (spec, candidate) in &claims {
                let result = (|| {
                    let binding = (!candidate.request_id.is_empty())
                        .then(|| request_key(&spec.tenant_id, &candidate.request_id));
                    if binding
                        .as_ref()
                        .and_then(|key| requests.get(key))
                        .is_some_and(|id| id != &spec.id)
                    {
                        return Err(Error::Conflict);
                    }
                    let node = nodes.get(&candidate.node_id).ok_or(Error::NotFound)?;
                    if node.node.id != candidate.node_id
                        || node.shard_id >= header.shards
                        || node
                            .session
                            .as_ref()
                            .is_none_or(|session| session.id != candidate.node_session_id)
                    {
                        return Err(Error::Conflict);
                    }
                    if let Some(existing) = environments.get(&spec.id) {
                        if existing.spec != *spec {
                            return Err(Error::Conflict);
                        }
                        if existing.assignment.node_id != candidate.node_id
                            || existing.result.is_some()
                            || existing.invalidated
                            || existing.recovery.is_some()
                        {
                            return Ok(ClaimOutcome::Existing(existing.clone()));
                        }
                        if existing.assignment.devices != candidate.devices {
                            return Err(Error::Conflict);
                        }
                        claim_admission(node)?;
                        return Ok(ClaimOutcome::Owned(existing.clone()));
                    }
                    claim_admission(node)?;
                    if let Some(id) = &spec.snapshot_id {
                        let (_, snapshot) = snapshots.get(id).ok_or(Error::NotFound)?;
                        if snapshot.template.tenant_id != spec.tenant_id
                            || !snapshot.references.contains(&Reference::Restore {
                                environment_id: spec.id.clone(),
                            })
                        {
                            return Err(Error::Conflict);
                        }
                    }
                    let generation = header.generation.checked_add(1).ok_or(Error::Conflict)?;
                    header.generation = generation;
                    header.advance()?;
                    let record = StoredEnvironment {
                        creation: Some(CreationSummary::new(&candidate.request_id)?),
                        last_interruption: None,
                        resume: None,
                        failed_since_unix_seconds: None,
                        recovery: None,
                        invalidated: false,
                        spec: spec.clone(),
                        result: None,
                        assignment: Assignment {
                            environment_id: spec.id.clone(),
                            node_id: candidate.node_id.clone(),
                            shard_id: node.shard_id,
                            generation,
                            devices: candidate.devices.clone(),
                        },
                    };
                    record.validate()?;
                    if let Some(key) = binding {
                        requests.insert(key.clone(), spec.id.clone());
                        request_writes.insert(key, spec.id.clone());
                    }
                    environments.insert(spec.id.clone(), record.clone());
                    writes.push((
                        format!("environment:{}", spec.id),
                        encode(&record)?,
                        header.revision,
                    ));
                    Ok(ClaimOutcome::Owned(record))
                })();
                results.push(result);
            }
            if writes.is_empty() {
                return Ok(results);
            }
            let mut command = redis::cmd("EVAL");
            command
                .arg(CLAIM_BATCH)
                .arg(3)
                .arg(&self.store.key)
                .arg(self.snapshots_key())
                .arg(format!("{}:requests", self.store.key))
                .arg(&header_value)
                .arg(writes.len())
                .arg(snapshot_ids.len());
            for id in &snapshot_ids {
                command.arg(id).arg(&snapshots[id].0);
            }
            command.arg(encode(&header)?);
            for (field, value, _) in &writes {
                command.arg(field).arg(value);
            }
            for (key, value) in request_writes {
                command.arg(key).arg(value);
            }
            if self.store.query::<u8>(command).await? == 1 {
                for (field, _, revision) in writes {
                    self.store.notify_committed_change(revision, vec![field]);
                }
                return Ok(results);
            }
        }
        Err(Error::Unavailable(
            "concurrent ownership batch registration; retry same Environment IDs".into(),
        ))
    }
}

fn claim_admission(node: &StoredNode) -> Result<()> {
    if !node.node.available || node.session.as_ref().is_none_or(|s| !s.routable) {
        return Err(Error::Unavailable(
            "node is not accepting ownership claims".into(),
        ));
    }
    Ok(())
}
