//! Initial success is the durable creation boundary. Both public identities are
//! checked in the same transaction; unfinished executions remain node-owned.
use super::*;

impl Session {
    fn request_field(tenant: &str, request: &str) -> Result<String> {
        encode(&(tenant, request))
    }

    pub async fn published_request(
        &self,
        tenant: &str,
        request: &str,
    ) -> Result<Option<EnvironmentRecord>> {
        if request.is_empty() {
            return Ok(None);
        }
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("HGET");
        command
            .arg(format!("{}:create-receipts", self.store.key))
            .arg(Self::request_field(tenant, request)?);
        let Some(raw) = self.store.query::<Option<String>>(command).await? else {
            return Ok(None);
        };
        let receipt: StoredEnvironment = decode(&raw)?;
        let mut record = receipt.result.ok_or(Error::Conflict)?;
        match self.get(&record.spec.id).await {
            Ok(current) if current.assignment == record.assignment => {
                record = current.result.ok_or(Error::Conflict)?
            }
            Ok(_) | Err(Error::NotFound) => {
                record.state = EnvironmentState::Deleted;
                record.resources_held = false;
                record.runtime.ip = None;
            }
            Err(error) => return Err(error),
        }
        Ok(Some(record))
    }

    /// Remove only an unfinished optional Claim belonging to a losing execution.
    /// Successful or replacement records cannot be removed by a delayed loser.
    pub(crate) async fn abandon_initial(&self, assignment: &Assignment) -> Result<()> {
        let field = format!("environment:{}", assignment.environment_id);
        let [header, old] = self.store.fields([HEADER.into(), field.clone()]).await?;
        self.header(&header)?;
        let Some(old) = old else {
            return Ok(());
        };
        let stored: StoredEnvironment = decode(&old)?;
        if stored.assignment != *assignment || stored.result.is_some() || stored.recovery.is_some()
        {
            return Ok(());
        }
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            if redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
            redis.call('HDEL',KEYS[1],ARGV[2]); return 1
        "#,
            )
            .arg(1)
            .arg(&self.store.key)
            .arg(self.epoch.to_string())
            .arg(&field)
            .arg(old);
        if self.store.query::<u8>(command).await? == 1 {
            self.store.notify_committed_change(0, vec![field]);
        }
        Ok(())
    }

    /// Publish a completed initial execution, or return the previously published
    /// identity. Callers must retire their execution when another result wins.
    pub async fn publish_initial(
        &self,
        record: EnvironmentRecord,
        request: &str,
        node_session: &str,
    ) -> Result<EnvironmentRecord> {
        self.publish_initial_requests(record, &[request.to_owned()], node_session)
            .await
    }

    pub(crate) async fn publish_initial_requests(
        &self,
        record: EnvironmentRecord,
        requests: &[String],
        node_session: &str,
    ) -> Result<EnvironmentRecord> {
        self.publish_success(record, requests, node_session, false)
            .await
    }

    pub(crate) async fn publish_existing(
        &self,
        record: EnvironmentRecord,
        request: &str,
    ) -> Result<EnvironmentRecord> {
        self.publish_success(record, &[request.to_owned()], "", true)
            .await
    }

    async fn publish_success(
        &self,
        record: EnvironmentRecord,
        requests: &[String],
        node_session: &str,
        require_existing: bool,
    ) -> Result<EnvironmentRecord> {
        validate_result(&record)?;
        if record.state != EnvironmentState::Running {
            return Err(Error::Conflict);
        }
        let field = format!("environment:{}", record.spec.id);
        let request_fields = requests
            .iter()
            .filter(|request| !request.is_empty())
            .map(|request| Self::request_field(&record.spec.tenant_id, request))
            .collect::<Result<Vec<_>>>()?;
        let next = StoredEnvironment {
            resume: None,
            failed_since_unix_seconds: None,
            recovery: None,
            invalidated: false,
            spec: record.spec.clone(),
            assignment: record.assignment.clone(),
            result: Some(record.clone()),
        };
        let artifacts = artifacts::record_artifacts(&record);
        let _access = self.store.lock_artifacts(&artifacts)?;
        for _ in 0..ATTEMPTS {
            let [header, old] = self.store.fields([HEADER.into(), field.clone()]).await?;
            self.header(&header)?;
            let published = old
                .as_deref()
                .map(decode::<StoredEnvironment>)
                .transpose()?
                .is_some_and(|e| e.result.is_some());
            if require_existing && !published {
                return Err(Error::NotFound);
            }
            let mut command = redis::cmd("EVAL");
            command.arg(r#"
                if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return {-1,''} end
                local requested = cjson.decode(ARGV[5])
                local function aliases(value, field)
                    for _,request in ipairs(requested) do
                        if redis.call('HEXISTS',KEYS[3],request) == 0 then
                            redis.call('HSET',KEYS[2],request,field)
                            redis.call('HSET',KEYS[3],request,value)
                        end
                    end
                end
                for _,request in ipairs(requested) do
                    local receipt = redis.call('HGET',KEYS[3],request)
                    if receipt then
                        aliases(receipt, 'environment:' .. cjson.decode(receipt).spec.id)
                        return {2,receipt}
                    end
                end
                local old = redis.call('HGET',KEYS[1],ARGV[2])
                if ARGV[3] == '' then if old then return {0,''} end
                elseif old ~= ARGV[3] then return {0,''} end
                if ARGV[6] == 'published' then
                    aliases(old, ARGV[2])
                    return {2,old}
                end
                if ARGV[7] ~= '' and redis.call('HGET',KEYS[1],ARGV[8]) ~= ARGV[7] then return {-1,''} end
                redis.call('HSET',KEYS[1],ARGV[2],ARGV[4])
                redis.call('HINCRBY',KEYS[1],'node-ownership:' .. string.sub(ARGV[8],14),1)
                aliases(ARGV[4], ARGV[2])
                for i=9,#ARGV,2 do redis.call('HSET',KEYS[4],ARGV[i],ARGV[i+1]) end
                return {1,ARGV[4]}
            "#).arg(4).arg(&self.store.key)
                .arg(format!("{}:create-requests", self.store.key))
                .arg(format!("{}:create-receipts", self.store.key))
                .arg(format!("{}:artifacts", self.store.key))
                .arg(self.epoch.to_string()).arg(&field).arg(old.as_deref().unwrap_or(""))
                .arg(encode(&next)?).arg(encode(&request_fields)?)
                .arg(if published { "published" } else { "initial" }).arg(node_session)
                .arg(format!("node-session:{}", record.assignment.node_id));
            for artifact in &artifacts {
                command
                    .arg(artifacts::artifact_key(artifact))
                    .arg(encode(artifact)?);
            }
            let (code, raw): (i8, String) = self.store.query(command).await?;
            match code {
                1 | 2 => {
                    let winner: StoredEnvironment = decode(&raw)?;
                    self.store.notify_committed_change(
                        0,
                        vec![format!("environment:{}", winner.spec.id)],
                    );
                    if code == 2 {
                        let mut result = winner.result.ok_or(Error::Conflict)?;
                        match self.get(&result.spec.id).await {
                            Ok(current) if current.assignment == result.assignment => {
                                result = current.result.ok_or(Error::Conflict)?;
                            }
                            Ok(_) | Err(Error::NotFound) => {
                                result.state = EnvironmentState::Deleted;
                                result.resources_held = false;
                                result.runtime.ip = None;
                            }
                            Err(error) => return Err(error),
                        }
                        return Ok(result);
                    }
                    return winner.result.ok_or(Error::Conflict);
                }
                0 => (),
                _ => return Err(Error::Conflict),
            }
        }
        Err(Error::Unavailable(
            "initial result publication contended; retry original request".into(),
        ))
    }
}
