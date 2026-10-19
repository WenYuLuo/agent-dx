//! Driver ownership and tombstones are durable before any backend side effect.
use super::client::unavailable;
use adx_core::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(super) enum Phase {
    Creating,
    Ready,
    Failed,
    Uncertain,
    Deleting,
    Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct Record {
    pub runtime_id: String,
    pub environment_id: String,
    pub tenant_id: String,
    pub generation: u64,
    pub sandbox_id: String,
    pub backend: String,
    pub epoch: String,
    pub fingerprint: String,
    pub phase: Phase,
    pub ip: Option<std::net::IpAddr>,
    pub pid: Option<u32>,
    pub create_sent: bool,
    pub bootstrap_sha256: String,
}

#[derive(Clone)]
pub(super) struct Store {
    path: PathBuf,
}

impl Store {
    pub fn new(path: PathBuf) -> Result<Self> {
        if !path.is_absolute() {
            return Err(Error::Invalid(
                "absolute Jiuwenbox state path required".into(),
            ));
        }
        let parent = path
            .parent()
            .ok_or_else(|| unavailable("state path has no parent"))?;
        std::fs::create_dir_all(parent).map_err(unavailable)?;
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)
            .map_err(unavailable)?;
        let db = Connection::open(&path).map_err(unavailable)?;
        db.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(unavailable)?;
        // Initialize once while the driver holds its process-wide state lease.
        // Reapplying WAL/schema on every concurrent connection can fail BUSY.
        db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS runtime(id TEXT PRIMARY KEY, sandbox TEXT UNIQUE NOT NULL, payload TEXT NOT NULL);").map_err(unavailable)?;
        Ok(Self { path })
    }
    async fn database<T: Send + 'static>(
        &self,
        work: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let path = self.path.clone();
        tokio::task::spawn_blocking(move || {
            let mut db = Connection::open(path).map_err(unavailable)?;
            db.busy_timeout(std::time::Duration::from_secs(5))
                .map_err(unavailable)?;
            db.execute_batch("PRAGMA synchronous=FULL;")
                .map_err(unavailable)?;
            work(&mut db)
        })
        .await
        .map_err(unavailable)?
    }
    pub async fn get(&self, id: &str) -> Result<Option<Record>> {
        let id = id.to_owned();
        self.database(move |db| {
            let text: Option<String> = db
                .query_row("SELECT payload FROM runtime WHERE id=?1", [id], |r| {
                    r.get(0)
                })
                .optional()
                .map_err(unavailable)?;
            text.map(|s| serde_json::from_str(&s).map_err(unavailable))
                .transpose()
        })
        .await
    }
    pub async fn insert(&self, record: Record) -> Result<()> {
        self.database(move |db| {
            let result = db.execute(
                "INSERT INTO runtime(id,sandbox,payload) VALUES(?1,?2,?3)",
                params![
                    record.runtime_id,
                    record.sandbox_id,
                    serde_json::to_string(&record).map_err(unavailable)?
                ],
            );
            match result {
                Ok(_) => Ok(()),
                Err(rusqlite::Error::SqliteFailure(e, _))
                    if e.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    Err(Error::Conflict)
                }
                Err(e) => Err(unavailable(e)),
            }
        })
        .await
    }
    pub async fn update(
        &self,
        id: &str,
        change: impl FnOnce(&mut Record) + Send + 'static,
    ) -> Result<Record> {
        let id = id.to_owned();
        self.database(move |db| {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .map_err(unavailable)?;
            let text: String = tx
                .query_row("SELECT payload FROM runtime WHERE id=?1", [&id], |r| {
                    r.get(0)
                })
                .map_err(unavailable)?;
            let mut record: Record = serde_json::from_str(&text).map_err(unavailable)?;
            change(&mut record);
            tx.execute(
                "UPDATE runtime SET payload=?2 WHERE id=?1",
                params![id, serde_json::to_string(&record).map_err(unavailable)?],
            )
            .map_err(unavailable)?;
            tx.commit().map_err(unavailable)?;
            Ok(record)
        })
        .await
    }
    pub async fn all(&self) -> Result<BTreeMap<String, Record>> {
        self.database(|db| {
            let mut q = db
                .prepare("SELECT id,payload FROM runtime")
                .map_err(unavailable)?;
            let rows = q
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(unavailable)?;
            rows.map(|r| {
                let (id, text) = r.map_err(unavailable)?;
                Ok((id, serde_json::from_str(&text).map_err(unavailable)?))
            })
            .collect()
        })
        .await
    }
}
