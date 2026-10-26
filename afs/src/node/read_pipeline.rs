//! Bounded asynchronous read policy. The runtime supplies tasks and IO; AFS
//! retains ordering turns and byte leases until their real owners are retired.
use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};

pub type ReadFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

pub type ReadReplyFuture = ReadFuture<'static, prost::bytes::Bytes>;

/// Child work may retain ownership without creating another task or reply.
pub type ReadLifetime = std::sync::Arc<dyn Send + Sync>;

/// Scope is attached to work, not the thread polling it. Spawned children
/// explicitly clone it so cancellation cannot release dispatch/RPC ownership early.
#[cfg(feature = "dfs")]
#[derive(Clone)]
pub(crate) struct ReadScope {
    pub(crate) deadline: std::time::Instant,
    pub(crate) lifetime: ReadLifetime,
}
#[cfg(feature = "dfs")]
tokio::task_local! { static READ_SCOPE: ReadScope; }
#[cfg(feature = "dfs")]
pub(crate) fn current_read_scope() -> Option<ReadScope> {
    READ_SCOPE.try_with(Clone::clone).ok()
}
#[cfg(feature = "dfs")]
pub(crate) async fn in_read_scope<T>(
    scope: ReadScope,
    work: impl std::future::Future<Output = T>,
) -> T {
    READ_SCOPE.scope(scope, work).await
}

/// Only genuine synchronous backend adapters use this boundary. Native gRPC
/// overrides the async methods and never sends network waiting through it.
/// Multi-thread runtimes support a finite blocking region; current-thread
/// compatibility drivers use a scoped thread outside all runtime contexts.
#[cfg(feature = "dfs")]
pub(crate) fn sync_read_backend<T: Send>(work: impl FnOnce() -> Result<T> + Send) -> Result<T> {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(work)
        }
        Ok(_) => std::thread::scope(|scope| scope.spawn(work).join()).map_err(|_| {
            Error::coded(
                afs_error::CLIENT_WORKER_FAILED,
                "synchronous read backend panicked",
            )
        })?,
        Err(_) => work(),
    }
}

/// A private final buffer and its byte lease move together across the RPC and
/// the FUSE reply. Request metadata is released before publishing the reply.
#[cfg(feature = "dfs")]
pub struct OwnedNativeRead {
    pub(crate) batch: crate::node::dfs_read::PeerReadBatch,
    pub(crate) chunk: crate::dfs::ChunkObject,
    // Retire observation before the buffer can reenter its pool or credits wake a new owner.
    pub(crate) activity: Option<super::read_activity::ActivityGuard>,
    pub(crate) buffer: super::read_buffer::ReadBuffer,
    pub(crate) bytes: Vec<tokio::sync::OwnedSemaphorePermit>,
    pub(crate) deadline: std::time::Instant,
    pub(crate) group_handle: u64,
    pub(crate) _lifetime: ReadLifetime,
    pub(crate) allocation: Option<NativeReadAllocation>,
}

#[cfg(feature = "dfs")]
pub(crate) struct NativeReadAllocation {
    pub(crate) length: usize,
    pub(crate) pool: std::sync::Arc<super::read_buffer::ReadBufferPool>,
    pub(crate) admission: std::sync::Arc<tokio::sync::Semaphore>,
    pub(crate) local: std::sync::Arc<super::chunk::LocalChunkStore>,
}

#[cfg(feature = "dfs")]
impl OwnedNativeRead {
    pub(crate) fn body_len(&self) -> usize {
        self.allocation
            .as_ref()
            .map_or(self.buffer.len(), |a| a.length)
    }
    pub(crate) fn capacity_charge(&self) -> usize {
        self.allocation
            .as_ref()
            .map_or(self.buffer.capacity(), |a| a.length.max(1024 * 1024))
    }
    pub(crate) async fn allocate(&mut self) -> Result<()> {
        let Some(allocation) = self.allocation.take() else {
            return Ok(());
        };
        let reply = allocation.allocate(self.deadline).await?;
        self.activity = reply.activity;
        self.buffer = reply.buffer;
        self.bytes = reply.bytes;
        Ok(())
    }
    pub(crate) fn into_reply(self) -> prost::bytes::Bytes {
        NativeReadReply {
            activity: self.activity,
            buffer: self.buffer,
            bytes: self.bytes,
        }
        .into_bytes()
    }
}

#[cfg(feature = "dfs")]
impl NativeReadAllocation {
    pub(crate) async fn allocate(&self, deadline: std::time::Instant) -> Result<NativeReadReply> {
        let a = self;
        let charge = a.length.max(1024 * 1024);
        let count = u32::try_from(charge).map_err(|_| {
            Error::coded(afs_error::IO_CAPACITY, "reply capacity conversion overflow")
        })?;
        let wait = a.local.read_work_metrics().map(|m| {
            m.activities
                .enter(super::read_activity::Activity::ReplyWait, 0)
        });
        let lease = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            a.admission.clone().acquire_many_owned(count),
        )
        .await
        .map_err(|_| Error::coded(afs_error::IO_CAPACITY, "reply capacity wait expired"))?
        .map_err(|_| Error::coded(afs_error::IO_CAPACITY, "reply capacity closed"))?;
        let buffer = a
            .pool
            .take_until(a.length, a.local.read_work_metrics().cloned(), deadline)
            .await?;
        let mut bytes = vec![lease];
        if buffer.capacity() > charge {
            bytes.push(
                a.admission
                    .clone()
                    .try_acquire_many_owned(u32::try_from(buffer.capacity() - charge).map_err(
                        |_| {
                            Error::coded(
                                afs_error::IO_CAPACITY,
                                "reply allocator capacity overflow",
                            )
                        },
                    )?)
                    .map_err(|_| {
                        Error::coded(
                            afs_error::IO_CAPACITY,
                            "reply allocator capacity exceeds budget",
                        )
                    })?,
            );
        }
        drop(wait);
        let activity = a.local.read_work_metrics().map(|m| {
            m.activities.enter(
                super::read_activity::Activity::ReplyBytes,
                buffer.capacity() as u64,
            )
        });
        Ok(NativeReadReply {
            activity,
            buffer,
            bytes,
        })
    }
}

#[cfg(feature = "dfs")]
pub(crate) struct NativeReadReply {
    pub(crate) activity: Option<super::read_activity::ActivityGuard>,
    pub(crate) buffer: super::read_buffer::ReadBuffer,
    pub(crate) bytes: Vec<tokio::sync::OwnedSemaphorePermit>,
}
#[cfg(feature = "dfs")]
impl AsRef<[u8]> for NativeReadReply {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}
#[cfg(feature = "dfs")]
impl NativeReadReply {
    pub(crate) fn into_bytes(self) -> prost::bytes::Bytes {
        prost::bytes::Bytes::from_owner(self)
    }
}

/// Bounded per-key coordination. Idle keys may be evicted, active gates never
/// are; callers recheck their real cache after taking the owned async guard.
#[derive(Default)]
pub(crate) struct ReadKeyGates(
    std::sync::Mutex<std::collections::HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>>,
);
impl ReadKeyGates {
    pub(crate) fn entry(&self, key: String) -> Result<std::sync::Arc<tokio::sync::Mutex<()>>> {
        if key.len() > 4096 {
            return Err(Error::coded(
                afs_error::IO_CAPACITY,
                "read coordination key exceeds bound",
            ));
        }
        let mut entries = self
            .0
            .lock()
            .map_err(|_| Error::coded(afs_error::IO_UNAVAILABLE, "read coordination poisoned"))?;
        if let Some(entry) = entries.get(&key) {
            return Ok(entry.clone());
        }
        if entries.len() >= 128 {
            let old = entries
                .iter()
                .find(|(_, gate)| std::sync::Arc::strong_count(gate) == 1)
                .map(|(key, _)| key.clone());
            if let Some(old) = old {
                entries.remove(&old);
            } else {
                return Err(Error::coded(
                    afs_error::IO_CAPACITY,
                    "read coordination keys busy",
                ));
            }
        }
        let entry = std::sync::Arc::new(tokio::sync::Mutex::new(()));
        entries.insert(key, entry.clone());
        Ok(entry)
    }
}

/// Remaining local budget is authoritative across queueing and retries.
/// Send only a duration over the wire; monotonic instants are process-local.
#[cfg(feature = "dfs")]
pub(crate) fn remaining_read_budget(
    deadline: std::time::Instant,
    cap: std::time::Duration,
) -> Result<std::time::Duration> {
    let remaining = deadline
        .saturating_duration_since(std::time::Instant::now())
        .min(cap);
    if remaining.is_zero() {
        return Err(Error::coded(
            afs_error::CLIENT_DEADLINE_EXCEEDED,
            "native read deadline exceeded before RPC",
        ));
    }
    Ok(remaining)
}

/// Only genuinely blocking work belongs here. The owned permit stays inside
/// the closure even if its asynchronous caller is cancelled while it runs.
pub(crate) async fn blocking_read<T: Send + 'static>(
    permit: tokio::sync::OwnedSemaphorePermit,
    read: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read()
    })
    .await
    .map_err(|error| Error::coded(afs_error::CLIENT_WORKER_FAILED, error.to_string()))?
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReadPipelineLimits {
    pub max_rpcs: usize,
    pub max_reply_bytes: usize,
    pub max_disk_reads: usize,
    pub max_batch_ops: usize,
    pub max_pending: usize,
    pub task_timeout_ms: u64,
}

impl Default for ReadPipelineLimits {
    fn default() -> Self {
        Self {
            max_rpcs: 4,
            max_reply_bytes: 8 * 1024 * 1024,
            max_disk_reads: 2,
            max_batch_ops: 4,
            max_pending: 256,
            task_timeout_ms: 30_000,
        }
    }
}

impl ReadPipelineLimits {
    pub fn validate(self) -> Result<Self> {
        if !(1..=128).contains(&self.max_rpcs)
            || !(8 * 1024 * 1024..=1024 * 1024 * 1024).contains(&self.max_reply_bytes)
            || !(1..=128).contains(&self.max_disk_reads)
            || !(1..=128).contains(&self.max_batch_ops)
            || !(1..=65_536).contains(&self.max_pending)
            || !(1..=60_000).contains(&self.task_timeout_ms)
        {
            return Err(Error::coded(
                afs_error::CONFIG_INVALID,
                "read pipeline requires RPC/disk/batch limits 1..=128, reply bytes 8MiB..=1GiB and timeout 1..=60000ms",
            ));
        }
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn read_equivalence_lazy_reply_is_unallocated_until_selected() {
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(crate::node::chunk::LocalChunkStore::open(temp.path(), "b").unwrap());
        let pool = super::super::read_buffer::ReadBufferPool::new("reply");
        pool.configure_byte_limit(8 * 1024 * 1024).unwrap();
        let slots = Arc::new(tokio::sync::Semaphore::new(8 * 1024 * 1024));
        let chunk = crate::node::chunk::StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("lazy"),
            vec![3; 1024],
        )
        .unwrap()
        .chunk;
        let mut read = OwnedNativeRead {
            batch: crate::node::dfs_read::PeerReadBatch {
                file_version_id: crate::dfs::FileVersionId::new("v"),
                layout_root_id: crate::dfs::LayoutRootId::new("l"),
                operations: Vec::new(),
            },
            chunk,
            activity: None,
            buffer: Vec::new().into(),
            bytes: Vec::new(),
            deadline: std::time::Instant::now() + std::time::Duration::from_secs(1),
            group_handle: 1,
            _lifetime: Arc::new(()),
            allocation: Some(NativeReadAllocation {
                length: 1024,
                pool,
                admission: slots.clone(),
                local,
            }),
        };
        assert_eq!(read.buffer.len(), 0);
        assert_eq!(slots.available_permits(), 8 * 1024 * 1024);
        read.allocate().await.unwrap();
        assert_eq!(read.buffer.len(), 1024);
        let body = read.into_reply();
        let tail = body.clone();
        drop(body);
        assert!(slots.available_permits() < 8 * 1024 * 1024);
        drop(tail);
        assert_eq!(slots.available_permits(), 8 * 1024 * 1024);
    }

    #[test]
    fn read_concurrency_limits_are_bounded_without_algorithm_selection() {
        for max_disk_reads in [1, 2, 4] {
            assert!(
                ReadPipelineLimits {
                    max_disk_reads,
                    ..Default::default()
                }
                .validate()
                .is_ok()
            );
        }
        assert!(
            ReadPipelineLimits {
                max_rpcs: 0,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
        assert!(
            ReadPipelineLimits {
                max_reply_bytes: 1024,
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_concurrency_cancelled_wait_keeps_real_blocking_depth() {
        use std::sync::Arc;
        use std::time::Duration;
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = slots.clone().acquire_owned().await.unwrap();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(async move {
            blocking_read(permit, move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .await
        });
        started_rx.await.unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(slots.available_permits(), 0);
        release_tx.send(()).unwrap();
        let final_permit = tokio::time::timeout(Duration::from_secs(1), slots.acquire_owned())
            .await
            .unwrap()
            .unwrap();
        drop(final_permit);
    }
}

#[cfg(test)]
mod unified_contract_tests {
    use super::*;
    #[cfg(feature = "dfs")]
    #[test]
    fn unified_sync_transport_adapter_preserves_an_existing_runtime() {
        use crate::node::dfs_read::{ChunkTransfer, PeerReadBatch};
        struct Legacy {
            runtime: tokio::runtime::Runtime,
        }
        impl ChunkTransfer for Legacy {
            fn read_ranges(&self, _: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
                self.runtime.block_on(async {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                });
                out.fill(7);
                Ok(())
            }
        }
        let transfer = Legacy {
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        };
        let batch = PeerReadBatch {
            file_version_id: crate::dfs::FileVersionId::new("version"),
            layout_root_id: crate::dfs::LayoutRootId::new("layout"),
            operations: Vec::new(),
        };
        let mut output = [0; 8];
        transfer
            .block_on_read(Box::pin(async {
                transfer.read_ranges_async(&batch, &mut output).await
            }))
            .unwrap();
        assert_eq!(output, [7; 8]);
    }

    #[test]
    fn unified_read_limits_have_no_user_algorithm_switch() {
        let value = serde_json::to_value(ReadPipelineLimits::default()).unwrap();
        assert!(
            value.get("enabled").is_none(),
            "read resource policy must not choose sync or async"
        );
    }
}

/// Genuine local file/codec work may borrow the private response, never a
/// network wait. Production multi-thread runtimes replace this worker while
/// it is in the blocking region. Synchronous/current-thread test callers run
/// the same finite local operation inline.
#[cfg(feature = "dfs")]
pub(crate) fn local_io<T>(work: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current()
        .is_ok_and(|h| h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread)
    {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}
