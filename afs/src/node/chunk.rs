//! Crash-safe local storage for immutable DFS chunks.
//!
//! A batch becomes durable in three ordered barriers: every staging file is
//! synced, final names are installed without replacement and the chunk
//! directory is synced, then one LocalCatalog transaction is appended and
//! synced. Only the catalog revision produced by the final barrier may appear
//! in a `ReplicaAck`.

use std::{
    collections::{HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    ops::{Bound, Deref, RangeBounds},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub mod outboard;
pub use outboard::OutboardBudget;
use outboard::{
    ChunkOutboard, DEFAULT_OUTBOARD_BUDGET_BYTES, MAX_ENCODED_OUTBOARD_BYTES, OutboardBuilder,
    OutboardReadWork, SharedOutboard,
};

use afs_error::{Error, Result};
use afs_tracing::tracing;
use serde::{Deserialize, Serialize};
#[cfg(unix)]
use std::os::unix::fs::FileExt;

#[cfg(test)]
use crate::dfs::ReplicaGroupId;
use crate::{
    dfs::{
        ChunkEncoding, ChunkId, ChunkObject, ChunkReceipt, ContentDigest, DigestAlgorithm,
        OperationId, ReplicaAck, ReplicaTarget, StorageDeviceDescriptor,
    },
    node::{storage::localfs::capacity_from_statvfs_fd, vfs::types::FilesystemCapacity},
};

// A generation identifies one checkout, even when jemalloc or the idle pool
// reuses the same address. It is diagnostic identity, never persistent identity.
static WRITE_ALLOCATION_GENERATION: AtomicU64 = AtomicU64::new(1);
const MAX_IDLE_WRITE_BUFFERS: usize = 1024;

#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct WriteBufferPoolStats {
    pub live_bytes: usize,
    pub live_buffers: usize,
    pub live_peak_bytes: usize,
    pub idle_bytes: usize,
    pub idle_buffers: usize,
    pub allocations: u64,
    pub reuses: u64,
    pub released_bytes: u64,
    pub released_buffers: u64,
    pub copied_bytes: u64,
    pub copy_operations: u64,
}

#[derive(Debug, Default)]
struct WriteBufferPoolState {
    idle: Vec<Vec<u8>>,
    stats: WriteBufferPoolStats,
    stopped: bool,
    metrics: Option<WriteBufferPoolMetrics>,
    #[cfg(test)]
    fail_next_allocation: bool,
}

#[derive(Debug)]
struct WriteBufferPoolMetrics {
    live_bytes: afs_metrics::IntGauge,
    live_buffers: afs_metrics::IntGauge,
    live_peak_bytes: afs_metrics::IntGauge,
    idle_bytes: afs_metrics::IntGauge,
    idle_buffers: afs_metrics::IntGauge,
    allocations: afs_metrics::IntCounter,
    reuses: afs_metrics::IntCounter,
    released_bytes: afs_metrics::IntCounter,
    released_buffers: afs_metrics::IntCounter,
    copied_bytes: afs_metrics::IntCounter,
    copy_operations: afs_metrics::IntCounter,
    previous: WriteBufferPoolStats,
}

impl WriteBufferPoolMetrics {
    fn register(
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<Self, afs_metrics::MetricsError> {
        let live_bytes = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_live_bytes",
            "Actual capacity of final write allocations still held by any owner.",
        )?;
        let live_buffers = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_live_allocations",
            "Final write allocations still held by any owner.",
        )?;
        let live_peak_bytes = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_live_peak_bytes",
            "Peak actual live final write allocation capacity.",
        )?;
        let idle_bytes = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_idle_bytes",
            "Actual capacity retained only by the bounded idle list.",
        )?;
        let idle_buffers = afs_metrics::IntGauge::new(
            "afs_dfs_write_buffer_idle_allocations",
            "Allocations retained only by the bounded idle list.",
        )?;
        let allocations = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_allocations_total",
            "Successful new final write payload allocations.",
        )?;
        let reuses = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_reuses_total",
            "Idle allocations checked out without moving payload storage.",
        )?;
        let released_bytes = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_released_bytes_total",
            "Allocation capacity released from the pool to the allocator, not a physical RSS measurement.",
        )?;
        let released_buffers = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_released_allocations_total",
            "Final write allocations released to the allocator.",
        )?;
        let copied_bytes = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_input_copy_bytes_total",
            "Actual input bytes copied into final mutable write buffers.",
        )?;
        let copy_operations = afs_metrics::IntCounter::new(
            "afs_dfs_write_buffer_input_copies_total",
            "Actual slice-copy or extend entries into final mutable write buffers.",
        )?;
        afs_metrics::register_collector(registry, &live_bytes)?;
        afs_metrics::register_collector(registry, &live_buffers)?;
        afs_metrics::register_collector(registry, &live_peak_bytes)?;
        afs_metrics::register_collector(registry, &idle_bytes)?;
        afs_metrics::register_collector(registry, &idle_buffers)?;
        afs_metrics::register_collector(registry, &allocations)?;
        afs_metrics::register_collector(registry, &reuses)?;
        afs_metrics::register_collector(registry, &released_bytes)?;
        afs_metrics::register_collector(registry, &released_buffers)?;
        afs_metrics::register_collector(registry, &copied_bytes)?;
        afs_metrics::register_collector(registry, &copy_operations)?;
        Ok(Self {
            live_bytes,
            live_buffers,
            live_peak_bytes,
            idle_bytes,
            idle_buffers,
            allocations,
            reuses,
            released_bytes,
            released_buffers,
            copied_bytes,
            copy_operations,
            previous: WriteBufferPoolStats::default(),
        })
    }

    fn publish(&mut self, stats: WriteBufferPoolStats) {
        self.live_bytes
            .set(i64::try_from(stats.live_bytes).unwrap_or(i64::MAX));
        self.live_buffers
            .set(i64::try_from(stats.live_buffers).unwrap_or(i64::MAX));
        self.live_peak_bytes
            .set(i64::try_from(stats.live_peak_bytes).unwrap_or(i64::MAX));
        self.idle_bytes
            .set(i64::try_from(stats.idle_bytes).unwrap_or(i64::MAX));
        self.idle_buffers
            .set(i64::try_from(stats.idle_buffers).unwrap_or(i64::MAX));
        self.allocations
            .inc_by(stats.allocations.saturating_sub(self.previous.allocations));
        self.reuses
            .inc_by(stats.reuses.saturating_sub(self.previous.reuses));
        self.released_bytes.inc_by(
            stats
                .released_bytes
                .saturating_sub(self.previous.released_bytes),
        );
        self.released_buffers.inc_by(
            stats
                .released_buffers
                .saturating_sub(self.previous.released_buffers),
        );
        self.copied_bytes.inc_by(
            stats
                .copied_bytes
                .saturating_sub(self.previous.copied_bytes),
        );
        self.copy_operations.inc_by(
            stats
                .copy_operations
                .saturating_sub(self.previous.copy_operations),
        );
        self.previous = stats;
    }
}

impl WriteBufferPoolState {
    fn publish_metrics(&mut self) {
        if let Some(metrics) = &mut self.metrics {
            metrics.publish(self.stats);
        }
    }
}

/// Nonblocking reuse of released write allocations. The baseline bounds only
/// idle capacity; it never grants, waits for or limits live payload capacity.
#[derive(Debug)]
pub struct WriteBufferPool {
    idle_baseline_bytes: usize,
    outboard_budget: Option<Arc<OutboardBudget>>,
    state: Mutex<WriteBufferPoolState>,
}

impl WriteBufferPool {
    pub fn new(idle_baseline_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            idle_baseline_bytes,
            outboard_budget: None,
            state: Mutex::new(WriteBufferPoolState::default()),
        })
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn new_with_outboard_budget(
        idle_baseline_bytes: usize,
        outboard_budget: Arc<OutboardBudget>,
    ) -> Arc<Self> {
        Arc::new(Self {
            idle_baseline_bytes,
            outboard_budget: Some(outboard_budget),
            state: Mutex::new(WriteBufferPoolState::default()),
        })
    }

    /// Register exact lifecycle counters with the process-owned registry.
    pub fn register_metrics(
        &self,
        registry: &afs_metrics::Registry,
    ) -> std::result::Result<(), afs_metrics::MetricsError> {
        let metrics = WriteBufferPoolMetrics::register(registry)?;
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.metrics = Some(metrics);
        state.publish_metrics();
        Ok(())
    }

    pub fn snapshot(&self) -> WriteBufferPoolStats {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stats
    }

    /// Release retained idle allocations outside the pool lock. Live owners
    /// continue to own their allocation; future returns cannot enter the pool.
    pub fn stop(&self) {
        let idle = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stopped = true;
            state.stats.released_bytes = state
                .stats
                .released_bytes
                .saturating_add(state.stats.idle_bytes as u64);
            state.stats.released_buffers = state
                .stats
                .released_buffers
                .saturating_add(state.stats.idle_buffers as u64);
            state.stats.idle_bytes = 0;
            state.stats.idle_buffers = 0;
            state.publish_metrics();
            std::mem::take(&mut state.idle)
        };
        drop(idle);
    }

    fn checkout(self: &Arc<Self>, capacity: usize) -> Result<ChunkBuffer> {
        {
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("write buffer pool is poisoned"))?;
            let index = if state.stopped {
                None
            } else {
                state
                    .idle
                    .iter()
                    .enumerate()
                    .filter(|(_, bytes)| {
                        bytes.capacity() >= capacity
                            && bytes.capacity() < capacity.saturating_mul(2)
                    })
                    .min_by_key(|(_, bytes)| bytes.capacity())
                    .map(|(index, _)| index)
            };
            if let Some(index) = index {
                let bytes = state.idle.swap_remove(index);
                state.stats.idle_bytes -= bytes.capacity();
                state.stats.idle_buffers -= 1;
                state.stats.live_bytes += bytes.capacity();
                state.stats.live_buffers += 1;
                state.stats.live_peak_bytes =
                    state.stats.live_peak_bytes.max(state.stats.live_bytes);
                state.stats.reuses += 1;
                state.publish_metrics();
                drop(state);
                let allocation_id = WRITE_ALLOCATION_GENERATION.fetch_add(1, Ordering::Relaxed);
                afs_logging::debug!("dfs.write_buffer.checkout";
                    "event" => "checkout",
                    "reused" => true,
                    "allocation_id" => allocation_id,
                    "capacity" => bytes.capacity() as u64,
                    "body_pointer" => bytes.as_ptr() as usize,
                );
                return Ok(ChunkBuffer {
                    bytes,
                    pool: Arc::downgrade(self),
                    allocation_id,
                });
            }
            #[cfg(test)]
            if std::mem::take(&mut state.fail_next_allocation) {
                return Err(write_allocation_error());
            }
        }
        // Only metadata/list lookup is under the lock. Payload allocation and
        // allocator failure handling happen outside it.
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| write_allocation_error())?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| invalid("write buffer pool is poisoned"))?;
        state.stats.live_bytes += bytes.capacity();
        state.stats.live_buffers += 1;
        state.stats.live_peak_bytes = state.stats.live_peak_bytes.max(state.stats.live_bytes);
        state.stats.allocations += 1;
        state.publish_metrics();
        drop(state);
        let allocation_id = WRITE_ALLOCATION_GENERATION.fetch_add(1, Ordering::Relaxed);
        afs_logging::debug!("dfs.write_buffer.checkout";
            "event" => "checkout",
            "reused" => false,
            "allocation_id" => allocation_id,
            "capacity" => bytes.capacity() as u64,
            "body_pointer" => bytes.as_ptr() as usize,
        );
        Ok(ChunkBuffer {
            bytes,
            pool: Arc::downgrade(self),
            allocation_id,
        })
    }

    fn record_copy(&self, bytes: usize) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.stats.copied_bytes = state.stats.copied_bytes.saturating_add(bytes as u64);
        state.stats.copy_operations = state.stats.copy_operations.saturating_add(1);
        state.publish_metrics();
    }

    #[cfg(test)]
    pub(super) fn fail_next_allocation(&self) {
        self.state.lock().unwrap().fail_next_allocation = true;
    }
}

#[derive(Debug)]
struct ChunkBuffer {
    bytes: Vec<u8>,
    pool: Weak<WriteBufferPool>,
    allocation_id: u64,
}

impl ChunkBuffer {
    fn unpooled(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            pool: Weak::new(),
            allocation_id: WRITE_ALLOCATION_GENERATION.fetch_add(1, Ordering::Relaxed),
        }
    }
}

impl Drop for ChunkBuffer {
    fn drop(&mut self) {
        let capacity = self.bytes.capacity();
        let body_pointer = self.bytes.as_ptr() as usize;
        let Some(pool) = self.pool.upgrade() else {
            let bytes = std::mem::take(&mut self.bytes);
            drop(bytes);
            afs_logging::debug!("dfs.write_buffer.last_owner_release";
                "event" => "last_owner_release",
                "allocation_id" => self.allocation_id,
                "capacity" => capacity as u64,
                "body_pointer" => body_pointer,
                "reason" => "pool_unavailable",
            );
            return;
        };
        let mut bytes = std::mem::take(&mut self.bytes);
        // Clear the initialized logical length. On reuse, resize initializes
        // each newly exposed byte before any caller can read a previous body.
        bytes.clear();
        {
            let mut state = pool
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.stats.live_bytes -= capacity;
            state.stats.live_buffers -= 1;
            if !state.stopped
                && capacity != 0
                && state.idle.len() < MAX_IDLE_WRITE_BUFFERS
                && capacity
                    <= pool
                        .idle_baseline_bytes
                        .saturating_sub(state.stats.idle_bytes)
            {
                state.stats.idle_bytes += capacity;
                state.stats.idle_buffers += 1;
                state.idle.push(bytes);
                state.publish_metrics();
                drop(state);
                afs_logging::debug!("dfs.write_buffer.last_owner_return";
                    "event" => "last_owner_return",
                    "allocation_id" => self.allocation_id,
                    "capacity" => capacity as u64,
                    "body_pointer" => body_pointer,
                );
                return;
            }
            state.stats.released_bytes = state.stats.released_bytes.saturating_add(capacity as u64);
            state.stats.released_buffers = state.stats.released_buffers.saturating_add(1);
            state.publish_metrics();
        }
        // Ineligible allocations are freed only after releasing the pool lock.
        drop(bytes);
        afs_logging::debug!("dfs.write_buffer.last_owner_release";
            "event" => "last_owner_release",
            "allocation_id" => self.allocation_id,
            "capacity" => capacity as u64,
            "body_pointer" => body_pointer,
            "reason" => "idle_not_retained",
        );
    }
}

/// Shared immutable view of one final allocation. Cloning or slicing moves
/// ownership metadata only; the final reference retires the entire allocation.
#[derive(Clone, Debug)]
pub struct SharedChunkBytes {
    buffer: Arc<ChunkBuffer>,
    start: usize,
    end: usize,
}

impl SharedChunkBytes {
    pub fn from_vec(bytes: Vec<u8>) -> Self {
        Self::from_buffer(ChunkBuffer::unpooled(bytes))
    }

    fn from_buffer(buffer: ChunkBuffer) -> Self {
        let end = buffer.bytes.len();
        Self {
            buffer: Arc::new(buffer),
            start: 0,
            end,
        }
    }

    pub fn len(&self) -> usize {
        self.end - self.start
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn as_ptr(&self) -> *const u8 {
        self.as_ref().as_ptr()
    }
    pub fn capacity(&self) -> usize {
        self.buffer.bytes.capacity()
    }
    pub fn allocation_id(&self) -> u64 {
        self.buffer.allocation_id
    }

    /// Share a subrange without changing the retained allocation or its charge.
    ///
    /// # Panics
    /// Panics when the requested range is outside this view or overflows usize.
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value
                .checked_add(1)
                .expect("shared chunk range start overflow"),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => value
                .checked_add(1)
                .expect("shared chunk range end overflow"),
            Bound::Excluded(value) => *value,
            Bound::Unbounded => self.len(),
        };
        assert!(
            start <= end && end <= self.len(),
            "shared chunk range is outside its view"
        );
        Self {
            buffer: self.buffer.clone(),
            start: self.start + start,
            end: self.start + end,
        }
    }
}

impl From<Vec<u8>> for SharedChunkBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from_vec(bytes)
    }
}

impl AsRef<[u8]> for SharedChunkBytes {
    fn as_ref(&self) -> &[u8] {
        &self.buffer.bytes[self.start..self.end]
    }
}

impl Deref for SharedChunkBytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

/// One mutable final payload allocation. Pooled builders are fixed capacity;
/// the legacy Default builder remains fallibly growable for other callers.
#[derive(Debug)]
pub struct ChunkBuilder {
    buffer: ChunkBuffer,
    fixed_capacity: Option<usize>,
}

impl Default for ChunkBuilder {
    fn default() -> Self {
        Self {
            buffer: ChunkBuffer::unpooled(Vec::new()),
            fixed_capacity: None,
        }
    }
}

impl ChunkBuilder {
    /// Acquire capacity without a live quota. Allocation refusal returns ENOMEM.
    pub fn with_pool(pool: Arc<WriteBufferPool>, capacity: usize) -> Result<Self> {
        Ok(Self {
            buffer: pool.checkout(capacity)?,
            fixed_capacity: Some(capacity),
        })
    }

    pub fn len(&self) -> usize {
        self.buffer.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buffer.bytes.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.buffer.bytes.capacity()
    }
    /// Usable body limit; actual allocation capacity may be larger on reuse.
    pub fn writable_capacity(&self) -> usize {
        self.fixed_capacity.unwrap_or_else(|| self.capacity())
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer.bytes
    }
    pub fn data_ptr(&self) -> *const u8 {
        self.buffer.bytes.as_ptr()
    }
    pub fn allocation_id(&self) -> u64 {
        self.buffer.allocation_id
    }

    /// Copy accepted input directly to the final storage. Fixed builders never
    /// reallocate; allocation errors leave the previous bytes and length intact.
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        let offset = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let end = offset
            .checked_add(data.len())
            .ok_or_else(|| invalid("chunk write range overflow"))?;
        if self.fixed_capacity.is_some_and(|capacity| end > capacity) {
            return Err(write_allocation_error());
        }
        if end > self.buffer.bytes.capacity() {
            self.buffer
                .bytes
                .try_reserve_exact(end - self.buffer.bytes.len())
                .map_err(|_| write_allocation_error())?;
        }
        let old_len = self.buffer.bytes.len();
        let overwritten = old_len.saturating_sub(offset).min(data.len());
        if overwritten != 0 {
            self.buffer.bytes[offset..offset + overwritten].copy_from_slice(&data[..overwritten]);
            self.record_input_copy(offset, overwritten);
        }
        if overwritten < data.len() {
            if offset > old_len {
                self.buffer.bytes.resize(offset, 0);
            }
            // Capacity has already been proved sufficient, so this extend
            // initializes only accepted input and cannot move the allocation.
            self.buffer.bytes.extend_from_slice(&data[overwritten..]);
            self.record_input_copy(offset + overwritten, data.len() - overwritten);
        }
        Ok(data.len())
    }

    fn record_input_copy(&self, offset: usize, bytes: usize) {
        if let Some(pool) = self.buffer.pool.upgrade() {
            pool.record_copy(bytes);
        }
        afs_logging::debug!("dfs.write_buffer.input_copy";
            "event" => "input_copy",
            "allocation_id" => self.allocation_id(),
            "capacity" => self.capacity() as u64,
            "body_pointer" => self.data_ptr() as usize,
            "buffer_offset" => offset as u64,
            "copied_bytes" => bytes as u64,
        );
    }

    /// Shrink logical content without releasing or reallocating its storage.
    pub fn truncate(&mut self, length: usize) {
        self.buffer.bytes.truncate(length);
    }

    /// Legacy full-body ownership transfer; pooled builders are not used here.
    pub fn replace(&mut self, bytes: Vec<u8>) {
        self.buffer = ChunkBuffer::unpooled(bytes);
        self.fixed_capacity = None;
    }

    pub fn freeze(self) -> SharedChunkBytes {
        let pointer_before = self.data_ptr() as usize;
        let frozen = SharedChunkBytes::from_buffer(self.buffer);
        afs_logging::debug!("dfs.write_buffer.freeze";
            "event" => "freeze",
            "allocation_id" => frozen.allocation_id(),
            "capacity" => frozen.capacity() as u64,
            "body_bytes" => frozen.len() as u64,
            "pointer_before" => pointer_before,
            "pointer_after" => frozen.as_ptr() as usize,
        );
        frozen
    }

    pub fn stage(self, operation_id: OperationId) -> StagedChunk {
        StagedChunk::from_shared(operation_id, self.freeze())
    }
}

fn write_allocation_error() -> Error {
    Error::coded(
        afs_error::IO_OUT_OF_MEMORY,
        "write buffer capacity allocation refused",
    )
}

/// Shared producer/replica budget per immutable staged chunk, never a file limit.
#[cfg(feature = "dfs")]
pub(crate) const MAX_STAGED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StagedChunk {
    pub operation_id: OperationId,
    pub chunk: ChunkObject,
    bytes: SharedChunkBytes,
    outboard: Option<SharedOutboard>,
    // This private identity is produced only by the complete initial hash. Public
    // metadata remains compatible with replica callers, but is not itself proof.
    #[cfg(feature = "dfs")]
    sealed_chunk: ChunkObject,
    #[cfg(feature = "dfs")]
    write_budget: Vec<WriteBudgetReservation>,
}

impl StagedChunk {
    pub fn new(operation_id: OperationId, bytes: Vec<u8>) -> Self {
        Self::from_shared(
            operation_id,
            SharedChunkBytes::from_buffer(ChunkBuffer::unpooled(bytes)),
        )
    }

    pub fn from_shared(operation_id: OperationId, bytes: SharedChunkBytes) -> Self {
        tracing::info_span!(
            "node.dfs.chunk.stage",
            operation_id = %operation_id.0,
            bytes = bytes.len() as u64,
            allocation_id = bytes.allocation_id(),
            capacity = bytes.capacity() as u64,
            body_pointer = bytes.as_ptr() as usize,
            chunk_id = tracing::field::Empty,
        )
        .in_scope(|| {
            let bytes_len = bytes.len() as u64;
            let built = bytes
                .buffer
                .pool
                .upgrade()
                .and_then(|pool| pool.outboard_budget.clone())
                .map(|budget| {
                    tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                        .in_scope(|| outboard::build_borrowed(&bytes, &budget))
                });
            let content_digest = built
                .as_ref()
                .map(|built| built.digest.clone())
                .unwrap_or_else(|| {
                    tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                        .in_scope(|| digest(&bytes))
                });
            let outboard = built.and_then(|built| built.outboard);
            let chunk = ChunkObject {
                id: ChunkId::new(format!(
                    "blake3-{}-{}",
                    digest_hex(&content_digest),
                    bytes.len()
                )),
                length: bytes.len() as u64,
                content_digest,
                encoding: ChunkEncoding::Raw,
            };
            tracing::Span::current().record("chunk_id", chunk.id.0.as_str());
            Self {
                operation_id,
                #[cfg(feature = "dfs")]
                sealed_chunk: chunk.clone(),
                chunk,
                bytes,
                outboard,
                #[cfg(feature = "dfs")]
                write_budget: Vec::new(),
            }
        })
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Content proof only: operation identity, authorization and persistence
    /// still require their own checks. No mutable payload is exposed.
    #[cfg(feature = "dfs")]
    pub(super) fn has_sealed_identity(&self) -> bool {
        self.chunk == self.sealed_chunk && self.chunk.length == self.bytes.len() as u64
    }

    #[cfg(feature = "dfs")]
    pub(super) fn write_budget_reservations(&self) -> &[WriteBudgetReservation] {
        &self.write_budget
    }

    #[cfg(feature = "dfs")]
    pub(super) fn retain_write_budget(&mut self, reservations: &[WriteBudgetReservation]) {
        self.write_budget.extend_from_slice(reservations);
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn shared_bytes(&self) -> SharedChunkBytes {
        self.bytes.clone()
    }
}

#[cfg(feature = "dfs")]
impl AsRef<[u8]> for StagedChunk {
    fn as_ref(&self) -> &[u8] {
        self.bytes()
    }
}

// The VFS owns admission limits. These typed leases live beside their immutable
// payload so HTTP body/retry ownership can outlive a ticket or a dirty window.
#[cfg(feature = "dfs")]
#[derive(Debug, Default)]
pub(super) struct DeferredWriteBudget {
    pub(super) state: Mutex<DeferredWriteBudgetState>,
}

#[cfg(feature = "dfs")]
#[derive(Debug, Default)]
pub(super) struct DeferredWriteBudgetState {
    pub(super) normal_bytes: u64,
    pub(super) publisher_bytes: u64,
    pub(super) descriptor_bytes: u64,
    pub(super) descriptors: usize,
}

#[cfg(feature = "dfs")]
#[derive(Clone, Debug)]
pub(super) struct WriteBudgetReservation {
    _inner: Arc<WriteBudgetReservationInner>,
}

#[cfg(feature = "dfs")]
#[derive(Debug)]
struct WriteBudgetReservationInner {
    budget: Arc<DeferredWriteBudget>,
    normal_bytes: u64,
    publisher_bytes: u64,
    descriptor_bytes: u64,
    descriptors: usize,
}

#[cfg(feature = "dfs")]
impl WriteBudgetReservation {
    pub(super) fn reduce_normal_to(&mut self, bytes: u64) -> Result<()> {
        let inner = Arc::get_mut(&mut self._inner)
            .ok_or_else(|| invalid("shared payload lease cannot be resized"))?;
        if bytes > inner.normal_bytes {
            return Err(invalid("payload lease reduction cannot grow its charge"));
        }
        let mut state = inner.budget.state.lock().map_err(|_| {
            Error::coded(
                afs_error::NODE_VFS_UNAVAILABLE,
                "DFS deferred write budget is poisoned",
            )
        })?;
        state.normal_bytes = state
            .normal_bytes
            .saturating_sub(inner.normal_bytes - bytes);
        inner.normal_bytes = bytes;
        Ok(())
    }

    pub(super) fn new(
        budget: Arc<DeferredWriteBudget>,
        normal_bytes: u64,
        publisher_bytes: u64,
        descriptor_bytes: u64,
        descriptors: usize,
    ) -> Self {
        Self {
            _inner: Arc::new(WriteBudgetReservationInner {
                budget,
                normal_bytes,
                publisher_bytes,
                descriptor_bytes,
                descriptors,
            }),
        }
    }
}

#[cfg(feature = "dfs")]
impl Drop for WriteBudgetReservationInner {
    fn drop(&mut self) {
        if let Ok(mut state) = self.budget.state.lock() {
            state.normal_bytes = state.normal_bytes.saturating_sub(self.normal_bytes);
            state.publisher_bytes = state.publisher_bytes.saturating_sub(self.publisher_bytes);
            state.descriptor_bytes = state.descriptor_bytes.saturating_sub(self.descriptor_bytes);
            state.descriptors = state.descriptors.saturating_sub(self.descriptors);
        }
    }
}

pub trait ChunkStore: Send + Sync {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>>;

    fn supports_deferred_put(&self) -> bool {
        false
    }

    /// Submit owned chunks without waiting for persistence. Unsupported backends
    /// reject explicitly; callers must check supports_deferred_put first.
    fn put_batch_deferred(&self, _staged: Vec<StagedChunk>) -> Result<Box<dyn DeferredChunkPut>> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "deferred chunk PUT is unsupported for this backend",
        ))
    }

    fn put(&self, staged: StagedChunk) -> Result<ChunkReceipt> {
        self.put_batch(vec![staged])?
            .pop()
            .ok_or_else(|| invalid("ChunkStore returned no receipt for one staged Chunk"))
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize>;

    fn capacity(&self) -> Result<FilesystemCapacity> {
        Err(Error::coded(
            afs_error::NODE_VFS_UNIMPLEMENTED,
            "ChunkStore capacity is unsupported for this backend",
        ))
    }
}

pub trait DeferredChunkPut: Send {
    fn try_wait(&mut self) -> Result<Option<Vec<ChunkReceipt>>> {
        Ok(None)
    }

    fn wait(self: Box<Self>) -> Result<Vec<ChunkReceipt>>;
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum LocalChunkState {
    Durable,
    Quarantined,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PhysicalLocation {
    PerChunkFile { relative_path: String },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PhysicalEncoding {
    Raw,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LocalChunkRecord {
    pub chunk: ChunkObject,
    pub stored_length: u64,
    pub stored_checksum: ContentDigest,
    pub device_id: String,
    pub device_epoch: u64,
    pub location: PhysicalLocation,
    pub encoding: PhysicalEncoding,
    pub state: LocalChunkState,
    pub catalog_revision: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub removed_staging_files: u64,
    pub durable_chunks: u64,
    pub orphan_files: u64,
}

#[derive(Debug)]
pub struct PinnedChunkReader {
    file: File,
    chunk: ChunkObject,
    outboard_path: PathBuf,
    outboard_budget: Arc<OutboardBudget>,
    outboard: Mutex<Option<SharedOutboard>>,
    proof_identity: Mutex<PeerFileIdentity>,
    read_work: Option<super::read_work::ReadWorkMetrics>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedRangeCopy {
    pub chunk_offset: u64,
    pub length: usize,
    pub output_offset: usize,
}

impl PinnedChunkReader {
    #[cfg(feature = "dfs")]
    pub(crate) fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }

    fn load_outboard(&self) -> Option<SharedOutboard> {
        if self.chunk.length > 4 * 1024 * 1024 {
            return None;
        }
        // Canonical IDs anchor this optional optimization. Legacy IDs keep the
        // original complete-root path until explicitly migrated.
        match ChunkObject::from_content_addressed_id(&self.chunk.id) {
            Some(expected) if expected == self.chunk => {}
            _ => return None,
        }
        let mut cached = self.outboard.lock().ok()?;
        if let Some(proof) = cached.as_ref() {
            return Some(proof.clone());
        }
        let proof =
            load_outboard_file(&self.outboard_path, &self.chunk, &self.outboard_budget).ok()?;
        *cached = Some(proof.clone());
        Some(proof)
    }

    fn remember_outboard(&self, proof: Option<SharedOutboard>) {
        if let Some(proof) = proof {
            let _ = persist_outboard_file(&self.outboard_path, &proof);
            if let Ok(mut cached) = self.outboard.lock() {
                *cached = Some(proof);
            }
        }
    }

    fn full_scan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
        force: bool,
    ) -> Result<Vec<usize>> {
        let before = PeerFileIdentity::read(&self.file)?;
        let builder = (ChunkObject::from_content_addressed_id(&self.chunk.id).as_ref()
            == Some(&self.chunk))
        .then(|| OutboardBuilder::new(self.chunk.length, self.outboard_budget.clone()));
        let (counts, proof) = verified_ranges_into_building(
            &self.file,
            &self.chunk,
            ranges,
            out,
            force,
            self.read_work.as_ref(),
            builder,
        )?;
        if PeerFileIdentity::read(&self.file)? != before {
            return Err(corrupt(
                &self.chunk.id,
                "identity changed during full verification",
            ));
        }
        *self
            .proof_identity
            .lock()
            .map_err(|_| invalid("pinned proof identity lock poisoned"))? = before;
        self.remember_outboard(proof);
        Ok(counts)
    }

    fn verify_full_and_build(&self) -> Result<()> {
        self.full_scan(&[], &mut [], true).map(|_| ())
    }

    fn outboard_ranges(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Option<Result<Vec<usize>>> {
        let before = PeerFileIdentity::read(&self.file).ok()?;
        if self.proof_identity.lock().ok().as_deref() != Some(&before) {
            return None;
        }
        let proof = self.load_outboard()?;
        let mut work = OutboardReadWork::default();
        let result =
            proof.read_ranges_into_with_work(&self.file, 0, &self.chunk, ranges, out, &mut work);
        #[cfg(test)]
        VERIFIED_SCAN_BYTES.with(|count| count.set(count.get() + work.hash_bytes));
        if let Some(metrics) = &self.read_work {
            metrics.hash("outboard_body_hash", work.hash_bytes);
            metrics.hash("outboard_parent_hash", work.parent_hash_bytes);
            metrics.copy("outboard_range_delivery", work.copied_bytes);
        }
        match result {
            Ok(counts) if PeerFileIdentity::read(&self.file).ok().as_ref() == Some(&before) => {
                Some(Ok(counts))
            }
            _ => {
                // Proof metadata is optional. Authenticate the same private
                // result with the old full root, and rebuild only after success.
                if let Ok(mut cached) = self.outboard.lock() {
                    cached.take();
                }
                None
            }
        }
    }

    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        if offset >= self.chunk.length || out.is_empty() {
            return Ok(0);
        }
        let allowed = usize::try_from((self.chunk.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("Chunk read length is too large"))?;
        // A pinned descriptor protects identity across rename, not integrity
        // after disk damage. Return the exact bytes covered by this digest,
        // rather than verifying and then issuing another unverified read.
        let mut bytes = vec![0; allowed];
        let ranges = [VerifiedRangeCopy {
            chunk_offset: offset,
            length: allowed,
            output_offset: 0,
        }];
        if let Some(result) = self.outboard_ranges(&ranges, &mut bytes) {
            result?;
        } else {
            self.full_scan(&ranges, &mut bytes, true)?;
        }
        out[..allowed].copy_from_slice(&bytes);
        if let Some(metrics) = &self.read_work {
            metrics.copy("pinned_range_to_output", allowed as u64);
        }
        Ok(allowed)
    }

    /// Verify the pinned file once and scatter requested ranges into an
    /// uncommitted staging buffer. The staging buffer may be dirtied before an
    /// error is returned; callers must publish it only after the outer read
    /// batch succeeds.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_ranges_at_uncommitted(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        if let Some(result) = self.outboard_ranges(ranges, out) {
            return result;
        }
        self.full_scan(ranges, out, false)
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn read_ranges_at_uncommitted_fullscan(
        &self,
        ranges: &[VerifiedRangeCopy],
        out: &mut [u8],
    ) -> Result<Vec<usize>> {
        verified_ranges_into(
            &self.file,
            &self.chunk,
            ranges,
            out,
            false,
            self.read_work.as_ref(),
        )
    }
}

#[cfg(unix)]
fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    loop {
        match file.read_at(out, offset) {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            result => return result.map_err(Error::from),
        }
    }
}

#[cfg(not(unix))]
fn read_positioned(file: &File, offset: u64, out: &mut [u8]) -> Result<usize> {
    let mut file = file.try_clone().map_err(Error::from)?;
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start(offset)).map_err(Error::from)?;
    file.read(out).map_err(Error::from)
}

#[derive(Debug, Deserialize, Serialize)]
struct CatalogTxn {
    revision: u64,
    records: Vec<LocalChunkRecord>,
    checksum: String,
}

impl CatalogTxn {
    fn new(revision: u64, records: Vec<LocalChunkRecord>) -> Result<Self> {
        let checksum = catalog_checksum(revision, &records)?;
        Ok(Self {
            revision,
            records,
            checksum,
        })
    }

    fn verify(&self) -> Result<()> {
        if self.checksum != catalog_checksum(self.revision, &self.records)? {
            return Err(invalid("local chunk catalog transaction checksum mismatch"));
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct LocalCatalog {
    revision: u64,
    records: HashMap<ChunkId, LocalChunkRecord>,
}

impl LocalCatalog {
    pub fn revision(&self) -> u64 {
        self.revision
    }

    fn recover(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let file = File::open(path).map_err(Error::from)?;
        let mut catalog = Self::default();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(Error::from)?;
            if line.trim().is_empty() {
                continue;
            }
            let txn: CatalogTxn = serde_json::from_str(&line)
                .map_err(|error| invalid(format!("local chunk catalog is corrupt: {error}")))?;
            txn.verify()?;
            if txn.revision != catalog.revision.saturating_add(1) {
                return Err(invalid("local chunk catalog revision is not contiguous"));
            }
            for record in txn.records {
                if record.catalog_revision != txn.revision {
                    return Err(invalid("local chunk record has the wrong catalog revision"));
                }
                catalog.records.insert(record.chunk.id.clone(), record);
            }
            catalog.revision = txn.revision;
        }
        Ok(catalog)
    }
}

/// Shared across the current verified object, in-flight ranges and RDMA packed
/// payloads. This replaces independent per-request payload allowances; it does
/// not add a second retained-cache budget.
#[cfg(any(feature = "dfs", test))]
pub(crate) const PEER_READ_PAYLOAD_LIMIT: usize = 8 * 1024 * 1024;
#[cfg(any(feature = "dfs", test))]
pub(crate) const PEER_READ_OBJECT_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug, Default)]
#[cfg(any(feature = "dfs", test))]
struct PeerReadBudget {
    used: AtomicU64,
    peak: AtomicU64,
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerReadReservation {
    budget: Arc<PeerReadBudget>,
    bytes: u64,
    metrics: Option<super::read_work::ReadWorkMetrics>,
}

#[cfg(any(feature = "dfs", test))]
impl PeerReadReservation {
    // Vec::try_reserve_exact normally gives the requested capacity; still
    // account the allocator's actual capacity before handing a payload onward.
    pub(crate) fn retain_capacity(&mut self, capacity: usize) -> Result<()> {
        let capacity = u64::try_from(capacity).map_err(|_| peer_payload_capacity())?;
        if capacity > self.bytes {
            let extra = capacity - self.bytes;
            reserve_peer_bytes(&self.budget, extra, self.metrics.as_ref())?;
            self.bytes = capacity;
        }
        Ok(())
    }
}

#[cfg(any(feature = "dfs", test))]
impl Drop for PeerReadReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        if let Some(metrics) = &self.metrics {
            metrics.peer_payload_released(self.bytes);
        }
    }
}

#[cfg(any(feature = "dfs", test))]
fn peer_payload_capacity() -> Error {
    Error::coded(
        afs_error::IO_CAPACITY,
        "shared Peer read payload budget exhausted",
    )
}

#[cfg(any(feature = "dfs", test))]
fn reserve_peer_bytes(
    budget: &PeerReadBudget,
    bytes: u64,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<()> {
    let mut used = budget.used.load(Ordering::Acquire);
    loop {
        let next = used
            .checked_add(bytes)
            .filter(|next| *next <= PEER_READ_PAYLOAD_LIMIT as u64)
            .ok_or_else(peer_payload_capacity)?;
        match budget
            .used
            .compare_exchange_weak(used, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                let old_peak = budget.peak.fetch_max(next, Ordering::AcqRel);
                if let Some(metrics) = metrics {
                    metrics.peer_payload_reserved(bytes, next.saturating_sub(old_peak));
                }
                return Ok(());
            }
            Err(actual) => used = actual,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PeerFileIdentity {
    length: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified: (i64, i64),
    #[cfg(unix)]
    changed: (i64, i64),
}

impl PeerFileIdentity {
    fn read(file: &File) -> Result<Self> {
        let metadata = file.metadata().map_err(Error::from)?;
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Ok(Self {
            length: metadata.len(),
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            modified: (metadata.mtime(), metadata.mtime_nsec()),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        })
    }
}

#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
struct PeerVerifiedObject {
    bytes: Vec<u8>,
    outboard: Option<SharedOutboard>,
    _reservation: PeerReadReservation,
    // A range fallback is never resident and needs no reusable identity.
    identity: Option<(LocalChunkRecord, PeerFileIdentity)>,
}

/// A view into an immutable private allocation. Its shared reservation remains
/// charged even after the single cache slot is replaced or an RPC is cancelled.
#[derive(Debug)]
#[cfg(any(feature = "dfs", test))]
pub(crate) struct PeerReadPayload {
    object: Arc<PeerVerifiedObject>,
    start: usize,
    length: usize,
}

#[cfg(any(feature = "dfs", test))]
impl PeerReadPayload {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.object.bytes[self.start..self.start + self.length]
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn len(&self) -> usize {
        self.length
    }

    /// Content-only proof from the complete verified immutable allocation.
    /// A subrange or nonresident fallback still needs its own range digest;
    /// neither transport headers nor callers can manufacture this capability.
    pub(crate) fn verified_full_object_digest(&self) -> Option<ContentDigest> {
        let (record, identity) = self.object.identity.as_ref()?;
        if self.start != 0
            || self.length != self.object.bytes.len()
            || self.length as u64 != record.chunk.length
            || identity.length != record.chunk.length
            || record.stored_length != record.chunk.length
            || record.stored_checksum != record.chunk.content_digest
        {
            return None;
        }
        Some(record.chunk.content_digest.clone())
    }

    pub(crate) fn outboard(&self) -> Option<SharedOutboard> {
        self.verified_full_object_digest()?;
        self.object.outboard.clone()
    }

    /// Frame slices retain both immutable bytes and their original budget lease.
    pub(crate) fn into_shared_bytes(self) -> prost::bytes::Bytes {
        prost::bytes::Bytes::from_owner(self)
    }
}

#[cfg(any(feature = "dfs", test))]
impl AsRef<[u8]> for PeerReadPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Debug)]
pub struct LocalChunkStore {
    node_id: String,
    device_id: String,
    device_epoch: u64,
    root: PathBuf,
    root_dir: File,
    chunks: PathBuf,
    staging: PathBuf,
    outboards: PathBuf,
    outboard_budget: Arc<OutboardBudget>,
    catalog_path: PathBuf,
    catalog: Mutex<LocalCatalog>,
    next_staging: AtomicU64,
    read_work: Option<super::read_work::ReadWorkMetrics>,
    #[cfg(any(feature = "dfs", test))]
    peer_read_budget: Arc<PeerReadBudget>,
    #[cfg(any(feature = "dfs", test))]
    peer_read_cache: Mutex<Option<Arc<PeerVerifiedObject>>>,
}

impl LocalChunkStore {
    pub fn open(root: impl AsRef<Path>, node_id: impl Into<String>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let chunks = root.join("chunks");
        let staging = root.join("staging");
        let outboards = root.join("outboards");
        fs::create_dir_all(&outboards).map_err(Error::from)?;
        fs::create_dir_all(&chunks).map_err(Error::from)?;
        fs::create_dir_all(&staging).map_err(Error::from)?;
        let device_epoch = open_device_epoch(&root)?;
        let root_dir = File::open(&root).map_err(Error::from)?;
        let catalog_path = root.join("catalog.wal");
        let catalog = LocalCatalog::recover(&catalog_path)?;
        let store = Self {
            node_id: node_id.into(),
            device_id: "local-0".into(),
            device_epoch,
            root,
            root_dir,
            chunks,
            staging,
            outboards,
            outboard_budget: OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES),
            catalog_path,
            catalog: Mutex::new(catalog),
            next_staging: AtomicU64::new(1),
            read_work: None,
            #[cfg(any(feature = "dfs", test))]
            peer_read_budget: Arc::new(PeerReadBudget::default()),
            #[cfg(any(feature = "dfs", test))]
            peer_read_cache: Mutex::new(None),
        };
        store.recover()?;
        Ok(store)
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_outboard_budget(mut self, budget: Arc<OutboardBudget>) -> Self {
        self.outboard_budget = budget;
        self
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: super::read_work::ReadWorkMetrics,
    ) -> Self {
        self.read_work = Some(metrics);
        self
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_disk_cache_event(&self, result: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_disk_cache_event(result);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_cache_fill_stage(
        &self,
        stage: &'static str,
        wall_ns: u64,
        cpu_ns: Option<u64>,
    ) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_cache_fill_stage(stage, wall_ns, cpu_ns);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_disk_cache_fill_work(
        &self,
        bytes: usize,
        output: usize,
    ) -> Option<super::read_work::PeerCacheFillWork> {
        self.read_work
            .as_ref()
            .map(|metrics| metrics.peer_disk_cache_fill_work(bytes, output))
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn peer_disk_cache_stream_work(
        &self,
        output: usize,
    ) -> Option<super::read_work::PeerCacheStreamWork> {
        self.read_work
            .as_ref()
            .map(|metrics| metrics.peer_stream_pipeline(output))
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_read_hash(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.hash(stage, bytes);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_read_copy(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.copy(stage, bytes);
        }
    }

    #[cfg(feature = "dfs")]
    pub(crate) fn record_peer_read_event(&self, event: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_read_event(event);
        }
    }

    pub fn capacity(&self) -> Result<FilesystemCapacity> {
        capacity_from_statvfs_fd(&self.root_dir).map_err(Error::from)
    }

    pub fn device_descriptor(&self) -> Result<StorageDeviceDescriptor> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        Ok(StorageDeviceDescriptor {
            device_id: self.device_id.clone(),
            device_epoch: self.device_epoch,
            catalog_revision: catalog.revision,
            failure_domain: self.node_id.clone(),
        })
    }

    pub fn recover(&self) -> Result<RecoveryReport> {
        let mut report = RecoveryReport::default();
        for entry in fs::read_dir(&self.staging).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file() {
                fs::remove_file(entry.path()).map_err(Error::from)?;
                report.removed_staging_files = report.removed_staging_files.saturating_add(1);
            }
        }
        for entry in fs::read_dir(&self.outboards).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            if entry.file_type().map_err(Error::from)?.is_file()
                && entry.file_name().to_string_lossy().ends_with(".tmp")
            {
                fs::remove_file(entry.path()).map_err(Error::from)?;
            }
        }
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut damaged = Vec::new();
        for record in catalog.records.values() {
            if record.device_id != self.device_id
                || record.device_epoch != self.device_epoch
                || record.stored_length != record.chunk.length
                || record.stored_checksum != record.chunk.content_digest
            {
                return Err(invalid(
                    "durable local Chunk record is internally inconsistent",
                ));
            }
            if record.state != LocalChunkState::Durable {
                continue;
            }
            let path = self.path_for_record(record)?;
            match path.metadata() {
                Ok(metadata) if metadata.is_file() && metadata.len() == record.chunk.length => {
                    report.durable_chunks = report.durable_chunks.saturating_add(1);
                }
                Ok(_) => damaged.push(record.clone()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    damaged.push(record.clone());
                }
                Err(error) => return Err(Error::from(error)),
            }
        }
        if !damaged.is_empty() {
            self.quarantine_records(&mut catalog, damaged)?;
        }
        for entry in fs::read_dir(&self.chunks).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let id = ChunkId::new(entry.file_name().to_string_lossy().into_owned());
            if !catalog.records.contains_key(&id) {
                report.orphan_files = report.orphan_files.saturating_add(1);
            }
        }
        Ok(report)
    }

    fn chunk_path(&self, id: &ChunkId) -> PathBuf {
        self.chunks.join(&id.0)
    }

    fn path_for_record(&self, record: &LocalChunkRecord) -> Result<PathBuf> {
        match &record.location {
            PhysicalLocation::PerChunkFile { relative_path } => {
                if relative_path != &format!("chunks/{}", record.chunk.id.0) {
                    return Err(invalid("local chunk record has a non-canonical path"));
                }
                let path = self.root.join(relative_path);
                if !path.starts_with(&self.chunks) {
                    return Err(invalid("local chunk record escapes the chunk directory"));
                }
                Ok(path)
            }
        }
    }

    pub fn persist_batch(
        &self,
        staged: &[StagedChunk],
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<Vec<ReplicaAck>> {
        if target.node_id != self.node_id
            || target.device.device_id != self.device_id
            || target.device.device_epoch != self.device_epoch
        {
            return Err(invalid(
                "replica target does not identify this local chunk store",
            ));
        }
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        if catalog.revision < target.device.catalog_revision {
            return Err(invalid(
                "local catalog is older than the placement revision floor",
            ));
        }

        let mut new_chunks = Vec::new();
        let mut installed_chunk_ids = HashSet::new();
        for item in staged {
            let mut replace_existing = false;
            if let Some(existing) = catalog.records.get(&item.chunk.id) {
                if existing.chunk != item.chunk {
                    return Err(invalid(
                        "immutable Chunk ID collides with another local record",
                    ));
                }
                if existing.state == LocalChunkState::Quarantined {
                    replace_existing = true;
                } else {
                    match verify_file(&self.path_for_record(existing)?, &item.chunk) {
                        Ok(()) => continue,
                        Err(error) if verified_copy_is_invalid(&error) => replace_existing = true,
                        Err(error) => return Err(error),
                    }
                }
            }
            let temp_path = self.staging.join(format!(
                "{}.{}.tmp",
                safe_id(&item.operation_id.0),
                self.next_staging.fetch_add(1, Ordering::Relaxed)
            ));
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)
                .map_err(Error::from)?;
            file.write_all(item.bytes()).map_err(Error::from)?;
            file.sync_all().map_err(Error::from)?;
            let final_path = self.chunk_path(&item.chunk.id);
            if replace_existing {
                // Publish a new inode; never truncate a file held by readers.
                // Its immutable content identity is unchanged. Directory and
                // catalog barriers below still precede the durable ACK.
                fs::rename(&temp_path, &final_path).map_err(Error::from)?;
            } else {
                match fs::hard_link(&temp_path, &final_path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        verify_file(&final_path, &item.chunk)?;
                    }
                    Err(error) => return Err(Error::from(error)),
                }
                fs::remove_file(&temp_path).map_err(Error::from)?;
            }
            if installed_chunk_ids.insert(item.chunk.id.clone()) {
                new_chunks.push(item.chunk.clone());
            }
        }

        if !new_chunks.is_empty() {
            File::open(&self.chunks)
                .and_then(|directory| directory.sync_all())
                .map_err(Error::from)?;
            let revision = catalog.revision.saturating_add(1);
            let records = new_chunks
                .into_iter()
                .map(|chunk| LocalChunkRecord {
                    location: PhysicalLocation::PerChunkFile {
                        relative_path: format!("chunks/{}", chunk.id.0),
                    },
                    stored_length: chunk.length,
                    stored_checksum: chunk.content_digest.clone(),
                    device_id: self.device_id.clone(),
                    device_epoch: self.device_epoch,
                    chunk,
                    encoding: PhysicalEncoding::Raw,
                    state: LocalChunkState::Durable,
                    catalog_revision: revision,
                })
                .collect::<Vec<_>>();
            append_catalog_txn(
                &self.catalog_path,
                &CatalogTxn::new(revision, records.clone())?,
            )?;
            for record in records {
                catalog.records.insert(record.chunk.id.clone(), record);
            }
            catalog.revision = revision;
        }
        let catalog_revision = catalog.revision;
        let mut seen_outboards = HashSet::new();
        let mut sync_outboards = false;
        for item in staged {
            if let Some(proof) = &item.outboard
                && proof.uses_budget(&self.outboard_budget)
                && seen_outboards.insert(&item.chunk.id)
            {
                let path = self
                    .outboards
                    .join(format!("{}.bao", safe_id(&item.chunk.id.0)));
                // Compare every byte against the proof minted from this staged
                // body; a matching header alone does not validate the CVs.
                // Reused files still cross a file barrier, including after a
                // previous optional directory-sync failure.
                if sync_matching_outboard_file(&path, proof).unwrap_or(false)
                    || install_outboard_file(&path, proof).is_ok()
                {
                    sync_outboards = true;
                }
            }
        }
        if sync_outboards {
            // Proof metadata remains optional, but all successful installs and
            // reuses cross this batch's directory barrier before its ACKs.
            let _ = sync_outboard_directory(&self.outboards);
        }
        Ok(staged
            .iter()
            .map(|item| ReplicaAck {
                operation_id: item.operation_id.clone(),
                chunk_id: item.chunk.id.clone(),
                placement_revision,
                placement_epoch,
                node_id: target.node_id.clone(),
                node_epoch: target.node_epoch,
                device_id: target.device.device_id.clone(),
                device_epoch: target.device.device_epoch,
                catalog_revision,
                persisted_bytes: item.chunk.length,
                verified_digest: item.chunk.content_digest.clone(),
            })
            .collect())
    }

    pub fn persist(
        &self,
        staged: &StagedChunk,
        target: &ReplicaTarget,
        placement_revision: u64,
        placement_epoch: u64,
    ) -> Result<ReplicaAck> {
        self.persist_batch(
            std::slice::from_ref(staged),
            target,
            placement_revision,
            placement_epoch,
        )?
        .pop()
        .ok_or_else(|| invalid("local batch finalize returned no ReplicaAck"))
    }

    pub fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        self.open_verified(chunk_id)?
            .read_at(offset, out)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.try_quarantine(chunk_id);
                }
            })
    }

    /// Fill a caller-owned, unpublished range with one complete integrity scan.
    /// The output may be dirtied on failure and must not be returned to a peer
    /// until this operation succeeds. No verified reader or cache is retained.
    #[cfg(any(all(feature = "dfs", feature = "rdma"), test))]
    pub(crate) fn read_range_at_uncommitted(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        out: &mut [u8],
    ) -> Result<usize> {
        let reader = self.open_cataloged(chunk_id)?;
        let result = verified_ranges_into(
            &reader.file,
            &reader.chunk,
            &[VerifiedRangeCopy {
                chunk_offset: offset,
                length: out.len(),
                output_offset: 0,
            }],
            out,
            true,
            self.read_work.as_ref(),
        );
        result.map(|counts| counts[0]).inspect_err(|error| {
            if verified_copy_is_invalid(error) {
                self.try_quarantine(chunk_id);
            }
        })
    }

    #[cfg(any(feature = "dfs", test))]
    fn reserve_peer_read_payload_inner(&self, bytes: usize) -> Result<PeerReadReservation> {
        let bytes = u64::try_from(bytes).map_err(|_| peer_payload_capacity())?;
        reserve_peer_bytes(&self.peer_read_budget, bytes, self.read_work.as_ref())?;
        Ok(PeerReadReservation {
            budget: self.peer_read_budget.clone(),
            bytes,
            metrics: self.read_work.clone(),
        })
    }

    /// Admission is immediate. Eviction can reclaim only an unpinned resident
    /// object; payloads held by senders keep their charge until the last drop.
    #[cfg(any(all(feature = "dfs", feature = "rdma"), test))]
    pub(crate) fn reserve_peer_read_payload(&self, bytes: usize) -> Result<PeerReadReservation> {
        match self.reserve_peer_read_payload_inner(bytes) {
            Ok(reservation) => Ok(reservation),
            Err(error) => {
                if let Ok(mut slot) = self.peer_read_cache.try_lock()
                    && slot
                        .as_ref()
                        .is_some_and(|object| Arc::strong_count(object) == 1)
                {
                    slot.take();
                }
                self.reserve_peer_read_payload_inner(bytes)
                    .map_err(|_| error)
            }
        }
    }

    #[cfg(any(feature = "dfs", test))]
    fn peer_catalog_record(&self, chunk_id: &ChunkId) -> Result<LocalChunkRecord> {
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| invalid(format!("local Chunk '{}' is not cataloged", chunk_id.0)))?;
        if record.state != LocalChunkState::Durable {
            return Err(corrupt(chunk_id, "local copy is quarantined"));
        }
        if record.chunk.id != *chunk_id
            || record.device_id != self.device_id
            || record.device_epoch != self.device_epoch
            || record.stored_length != record.chunk.length
            || record.stored_checksum != record.chunk.content_digest
            || record.encoding != PhysicalEncoding::Raw
        {
            return Err(invalid(
                "durable local Chunk record is internally inconsistent",
            ));
        }
        self.path_for_record(&record)?;
        if let Some(expected) = ChunkObject::from_content_addressed_id(chunk_id)
            && expected != record.chunk
        {
            return Err(corrupt(
                chunk_id,
                "catalog differs from content-addressed identity",
            ));
        }
        Ok(record)
    }

    /// Reuse only the currently demanded object, never prefetch a next object.
    /// The slot mutex also supplies singleflight for a complete private fill.
    /// Authorization remains with each caller; this proof is content-only.
    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn read_peer_range(
        &self,
        chunk_id: &ChunkId,
        offset: u64,
        length: usize,
    ) -> Result<PeerReadPayload> {
        let mut slot = self
            .peer_read_cache
            .lock()
            .map_err(|_| invalid("Peer verified object lock is poisoned"))?;
        let result = (|| {
            let record = self.peer_catalog_record(chunk_id)?;
            let file = File::open(self.path_for_record(&record)?).map_err(Error::from)?;
            let before = PeerFileIdentity::read(&file)?;
            if before.length != record.chunk.length {
                return Err(corrupt(chunk_id, "length mismatch"));
            }
            let allowed = usize::try_from(
                record
                    .chunk
                    .length
                    .saturating_sub(offset)
                    .min(length as u64),
            )
            .map_err(|_| invalid("Chunk read length is too large"))?;
            if let Some(object) = slot.as_ref()
                && object.identity.as_ref() == Some(&(record.clone(), before.clone()))
            {
                if self.peer_catalog_record(chunk_id)? != record
                    || PeerFileIdentity::read(&file)? != before
                {
                    return Err(corrupt(chunk_id, "identity changed during cached read"));
                }
                if let Some(metrics) = &self.read_work {
                    metrics.peer_object_cache("hit");
                }
                return Ok(PeerReadPayload {
                    object: object.clone(),
                    start: usize::try_from(offset.min(record.chunk.length))
                        .map_err(|_| invalid("Chunk offset is too large"))?,
                    length: allowed,
                });
            }
            // A changed identity for the same formerly verified object must
            // re-establish its complete root before any proof can be reused.
            let invalidated_current = slot.as_ref().is_some_and(|object| {
                object
                    .identity
                    .as_ref()
                    .is_some_and(|(old, _)| old.chunk.id == *chunk_id)
            });
            // Never retain an unrelated current object while filling another.
            // Any externally held old allocation stays alive and fully charged.
            slot.take();
            if let Some(metrics) = &self.read_work {
                metrics.peer_object_cache("miss");
            }
            if !invalidated_current
                && (offset != 0 || allowed as u64 != record.chunk.length)
                && ChunkObject::from_content_addressed_id(chunk_id).as_ref() == Some(&record.chunk)
            {
                let reader = self.open_cataloged(chunk_id)?;
                if let Some(proof) = reader.load_outboard() {
                    let mut reservation = self.reserve_peer_read_payload_inner(allowed)?;
                    let mut bytes = Vec::new();
                    bytes
                        .try_reserve_exact(allowed)
                        .map_err(|_| peer_payload_capacity())?;
                    reservation.retain_capacity(bytes.capacity())?;
                    bytes.resize(allowed, 0);
                    let mut work = OutboardReadWork::default();
                    let verified = proof.read_ranges_into_with_work(
                        &file,
                        0,
                        &record.chunk,
                        &[VerifiedRangeCopy {
                            chunk_offset: offset,
                            length: allowed,
                            output_offset: 0,
                        }],
                        &mut bytes,
                        &mut work,
                    );
                    #[cfg(test)]
                    VERIFIED_SCAN_BYTES.with(|count| count.set(count.get() + work.hash_bytes));
                    if let Some(metrics) = &self.read_work {
                        metrics.hash("outboard_body_hash", work.hash_bytes);
                        metrics.hash("outboard_parent_hash", work.parent_hash_bytes);
                        metrics.copy("outboard_range_delivery", work.copied_bytes);
                    }
                    if verified.is_ok()
                        && PeerFileIdentity::read(&file)? == before
                        && self.peer_catalog_record(chunk_id)? == record
                    {
                        return Ok(PeerReadPayload {
                            object: Arc::new(PeerVerifiedObject {
                                bytes,
                                outboard: None,
                                _reservation: reservation,
                                identity: None,
                            }),
                            start: 0,
                            length: allowed,
                        });
                    }
                    // Drop the exact-range private allocation before the old
                    // full-root fallback attempts its original body admission.
                    drop(bytes);
                    drop(reservation);
                }
            }
            let resident = record.chunk.length <= PEER_READ_OBJECT_LIMIT as u64;
            let allocation = if resident {
                usize::try_from(record.chunk.length).map_err(|_| peer_payload_capacity())?
            } else {
                allowed
            };
            let mut reservation = self.reserve_peer_read_payload_inner(allocation)?;
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(allocation)
                .map_err(|_| peer_payload_capacity())?;
            reservation.retain_capacity(bytes.capacity())?;
            bytes.resize(allocation, 0);
            let builder = (ChunkObject::from_content_addressed_id(chunk_id).as_ref()
                == Some(&record.chunk))
            .then(|| OutboardBuilder::new(record.chunk.length, self.outboard_budget.clone()));
            let (_, outboard) = verified_ranges_into_building(
                &file,
                &record.chunk,
                &[VerifiedRangeCopy {
                    chunk_offset: if resident { 0 } else { offset },
                    length: allocation,
                    output_offset: 0,
                }],
                &mut bytes,
                true,
                self.read_work.as_ref(),
                builder,
            )?;
            if PeerFileIdentity::read(&file)? != before
                || self.peer_catalog_record(chunk_id)? != record
            {
                return Err(corrupt(chunk_id, "identity changed during verification"));
            }
            // Reopen the path as well: a pinned old descriptor must not seed a
            // cache after another inode has replaced its cataloged pathname.
            let current = File::open(self.path_for_record(&record)?).map_err(Error::from)?;
            if PeerFileIdentity::read(&current)? != before {
                return Err(corrupt(chunk_id, "path changed during verification"));
            }
            if let Some(proof) = &outboard {
                let _ = persist_outboard_file(
                    &self
                        .outboards
                        .join(format!("{}.bao", safe_id(&record.chunk.id.0))),
                    proof,
                );
            }
            let object = Arc::new(PeerVerifiedObject {
                bytes,
                outboard,
                _reservation: reservation,
                identity: resident.then_some((record, before)),
            });
            if resident {
                *slot = Some(object.clone());
            }
            Ok(PeerReadPayload {
                object,
                start: if resident {
                    usize::try_from(offset.min(allocation as u64))
                        .map_err(|_| invalid("Chunk offset is too large"))?
                } else {
                    0
                },
                length: allowed,
            })
        })();
        if result.is_err() {
            slot.take();
        }
        drop(slot);
        if let Err(error) = &result
            && verified_copy_is_invalid(error)
        {
            self.try_quarantine(chunk_id);
        }
        result
    }

    pub fn open_verified(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
        let reader = self.open_cataloged(chunk_id)?;
        reader.verify_full_and_build().inspect_err(|error| {
            if verified_copy_is_invalid(error) {
                self.try_quarantine(chunk_id);
            }
        })?;
        Ok(reader)
    }

    // Pin the cataloged inode without claiming its integrity. Both callers
    // perform a complete scan on this descriptor before returning any bytes.
    fn open_cataloged(&self, chunk_id: &ChunkId) -> Result<PinnedChunkReader> {
        let record = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?
            .records
            .get(chunk_id)
            .cloned()
            .ok_or_else(|| invalid(format!("local Chunk '{}' is not cataloged", chunk_id.0)))?;
        if record.state != LocalChunkState::Durable {
            return Err(corrupt(chunk_id, "local copy is quarantined"));
        }
        let file = File::open(self.path_for_record(&record)?)
            .map_err(Error::from)
            .inspect_err(|error| {
                if verified_copy_is_invalid(error) {
                    self.try_quarantine(chunk_id);
                }
            })?;
        if let Some(expected) = ChunkObject::from_content_addressed_id(chunk_id)
            && expected != record.chunk
        {
            return Err(corrupt(
                chunk_id,
                "catalog differs from content-addressed identity",
            ));
        }
        let proof_identity = Mutex::new(PeerFileIdentity::read(&file)?);
        Ok(PinnedChunkReader {
            file,
            proof_identity,
            outboard_path: self
                .outboards
                .join(format!("{}.bao", safe_id(&record.chunk.id.0))),
            outboard_budget: self.outboard_budget.clone(),
            outboard: Mutex::new(None),
            chunk: record.chunk,
            read_work: self.read_work.clone(),
        })
    }

    /// Recheck the current physical file under the publication lock. An old
    /// failed reader must not quarantine a healthy replacement inode.
    pub(crate) fn quarantine_if_invalid(&self, chunk_id: &ChunkId) -> Result<()> {
        let mut catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let Some(record) = catalog.records.get(chunk_id).cloned() else {
            return Ok(());
        };
        if record.state == LocalChunkState::Quarantined {
            return Ok(());
        }
        match verify_file(&self.path_for_record(&record)?, &record.chunk) {
            Ok(()) => Ok(()),
            Err(error) if verified_copy_is_invalid(&error) => {
                self.quarantine_records(&mut catalog, vec![record])
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn try_quarantine(&self, chunk_id: &ChunkId) {
        if let Err(error) = self.quarantine_if_invalid(chunk_id) {
            // Read fallback can still succeed. No durable quarantine or report
            // is claimed if its catalog barrier fails; every read rechecks.
            afs_logging::warn!("dfs.local_quarantine_failed";
                "chunk" => chunk_id.0.clone(), "error" => error.to_string());
        }
    }

    #[cfg(any(feature = "dfs", test))]
    pub(crate) fn quarantined_chunks(&self) -> Result<Vec<LocalChunkRecord>> {
        let catalog = self
            .catalog
            .lock()
            .map_err(|_| invalid("local chunk catalog lock is poisoned"))?;
        let mut records: Vec<_> = catalog
            .records
            .values()
            .filter(|record| record.state == LocalChunkState::Quarantined)
            .cloned()
            .collect();
        records.sort_by(|left, right| {
            (left.catalog_revision, &left.chunk.id).cmp(&(right.catalog_revision, &right.chunk.id))
        });
        Ok(records)
    }

    fn quarantine_records(
        &self,
        catalog: &mut LocalCatalog,
        mut records: Vec<LocalChunkRecord>,
    ) -> Result<()> {
        let revision = catalog
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("local catalog revision exhausted"))?;
        for record in &mut records {
            record.state = LocalChunkState::Quarantined;
            record.catalog_revision = revision;
        }
        append_catalog_txn(
            &self.catalog_path,
            &CatalogTxn::new(revision, records.clone())?,
        )?;
        for record in records {
            catalog.records.insert(record.chunk.id.clone(), record);
        }
        catalog.revision = revision;
        Ok(())
    }
}

static OUTBOARD_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
fn load_outboard_file(
    path: &Path,
    chunk: &ChunkObject,
    budget: &Arc<OutboardBudget>,
) -> Result<SharedOutboard> {
    use std::io::Read;
    let mut file = File::open(path).map_err(Error::from)?;
    let length = usize::try_from(file.metadata().map_err(Error::from)?.len())
        .map_err(|_| invalid("outboard file length too large"))?;
    if length > MAX_ENCODED_OUTBOARD_BYTES {
        return Err(corrupt(&chunk.id, "oversized outboard"));
    }
    let lease = budget
        .try_reserve(ChunkOutboard::resident_charge(MAX_ENCODED_OUTBOARD_BYTES)?)
        .ok_or_else(|| {
            Error::coded(afs_error::IO_CAPACITY, "outboard metadata budget exhausted")
        })?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| Error::coded(afs_error::IO_CAPACITY, "outboard allocation refused"))?;
    bytes.resize(length, 0);
    file.read_exact(&mut bytes).map_err(Error::from)?;
    if file.metadata().map_err(Error::from)?.len() != length as u64 {
        return Err(corrupt(&chunk.id, "outboard length changed"));
    }
    ChunkOutboard::from_encoded(chunk, bytes, lease)
}
fn persist_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<()> {
    install_outboard_file(path, proof)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("outboard path has no parent"))?;
    sync_outboard_directory(parent)
}

fn sync_matching_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<bool> {
    use std::io::Read;
    let mut file = File::open(path).map_err(Error::from)?;
    let metadata = file.metadata().map_err(Error::from)?;
    if !metadata.is_file() || metadata.len() != proof.encoded().len() as u64 {
        return Ok(false);
    }
    // Bounded stack scratch avoids a second proof allocation or metadata lease.
    let mut scratch = [0; 1024];
    for expected in proof.encoded().chunks(scratch.len()) {
        let observed = &mut scratch[..expected.len()];
        file.read_exact(observed).map_err(Error::from)?;
        if observed != expected {
            return Ok(false);
        }
    }
    if file.metadata().map_err(Error::from)?.len() != proof.encoded().len() as u64 {
        return Ok(false);
    }
    file.sync_all().map_err(Error::from)?;
    #[cfg(test)]
    OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().file_syncs += 1);
    Ok(true)
}

fn install_outboard_file(path: &Path, proof: &SharedOutboard) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| invalid("outboard path has no parent"))?;
    let sequence = OUTBOARD_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temp = parent.join(format!(".{}.{}.tmp", std::process::id(), sequence));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(Error::from)?;
        file.write_all(proof.encoded()).map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().writes += 1);
        file.sync_all().map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().file_syncs += 1);
        fs::rename(&temp, path).map_err(Error::from)?;
        #[cfg(test)]
        OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().renames += 1);
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn sync_outboard_directory(path: &Path) -> Result<()> {
    #[cfg(test)]
    if FAIL_OUTBOARD_DIRECTORY_SYNC.with(|fail| fail.replace(false)) {
        return Err(Error::from(std::io::Error::other(
            "injected outboard directory-sync failure",
        )));
    }
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    #[cfg(test)]
    OUTBOARD_PERSIST_IO.with(|counts| counts.borrow_mut().directory_syncs += 1);
    Ok(())
}

fn verified_copy_is_invalid(error: &Error) -> bool {
    matches!(
        error.kind(),
        afs_error::ErrorKind::DataLoss | afs_error::ErrorKind::NotFound
    )
}

#[cfg(test)]
impl ChunkStore for LocalChunkStore {
    fn put_batch(&self, staged: Vec<StagedChunk>) -> Result<Vec<ChunkReceipt>> {
        let descriptor = self.device_descriptor()?;
        let target = ReplicaTarget {
            node_id: self.node_id.clone(),
            node_epoch: 1,
            data_endpoint: String::new(),
            device: descriptor,
        };
        let acks = self.persist_batch(&staged, &target, 1, 1)?;
        Ok(staged
            .into_iter()
            .zip(acks)
            .map(|(item, ack)| ChunkReceipt {
                external_commit: None,
                operation_id: item.operation_id,
                chunk: item.chunk,
                placement_revision: 1,
                placement_epoch: 1,
                replica_group_id: ReplicaGroupId::new(format!("local:{}", self.node_id)),
                durable_acks: vec![ack],
            })
            .collect())
    }

    fn read_at(&self, chunk_id: &ChunkId, offset: u64, out: &mut [u8]) -> Result<usize> {
        LocalChunkStore::read_at(self, chunk_id, offset, out)
    }

    fn capacity(&self) -> Result<FilesystemCapacity> {
        LocalChunkStore::capacity(self)
    }
}

fn append_catalog_txn(path: &Path, txn: &CatalogTxn) -> Result<()> {
    let mut encoded = serde_json::to_vec(txn)
        .map_err(|error| invalid(format!("cannot encode local chunk catalog: {error}")))?;
    encoded.push(b'\n');
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(Error::from)?;
    file.write_all(&encoded).map_err(Error::from)?;
    file.sync_all().map_err(Error::from)?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("local chunk catalog has no parent directory"))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)
}

fn catalog_checksum(revision: u64, records: &[LocalChunkRecord]) -> Result<String> {
    let encoded = serde_json::to_vec(&(revision, records))
        .map_err(|error| invalid(format!("cannot checksum local chunk catalog: {error}")))?;
    Ok(blake3::hash(&encoded).to_hex().to_string())
}

fn open_device_epoch(root: &Path) -> Result<u64> {
    let path = root.join("device_epoch");
    if path.exists() {
        let value = fs::read_to_string(path).map_err(Error::from)?;
        return value
            .trim()
            .parse::<u64>()
            .map_err(|_| invalid("local chunk device epoch is corrupt"));
    }
    let epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| invalid("system clock is before UNIX epoch"))?
        .as_nanos() as u64
        ^ u64::from(std::process::id());
    let temp = root.join("device_epoch.tmp");
    if temp.exists() {
        fs::remove_file(&temp).map_err(Error::from)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)
        .map_err(Error::from)?;
    writeln!(file, "{epoch}").map_err(Error::from)?;
    file.sync_all().map_err(Error::from)?;
    fs::rename(&temp, &path).map_err(Error::from)?;
    File::open(root)
        .and_then(|directory| directory.sync_all())
        .map_err(Error::from)?;
    Ok(epoch)
}

fn verify_file(path: &Path, chunk: &ChunkObject) -> Result<()> {
    let file = File::open(path).map_err(Error::from)?;
    verified_range(&file, chunk, 0, 0, None).map(|_| ())
}

/// Hash the pinned file once and retain only the requested overlap. The
/// temporary range is bounded by the caller's output; scan memory is 64KiB.
/// Nothing is published to the caller until all content checks pass.
fn verified_range(
    file: &File,
    chunk: &ChunkObject,
    offset: u64,
    length: usize,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<Vec<u8>> {
    offset
        .checked_add(length as u64)
        .filter(|end| *end <= chunk.length)
        .ok_or_else(|| invalid("verified Chunk range exceeds its length"))?;
    let mut bytes = vec![0; length];
    verified_ranges_into(
        file,
        chunk,
        &[VerifiedRangeCopy {
            chunk_offset: offset,
            length,
            output_offset: 0,
        }],
        &mut bytes,
        true,
        metrics,
    )?;
    Ok(bytes)
}

fn verified_ranges_into(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
) -> Result<Vec<usize>> {
    verified_ranges_into_building(file, chunk, ranges, out, force_scan, metrics, None)
        .map(|(counts, _)| counts)
}

fn verified_ranges_into_building(
    file: &File,
    chunk: &ChunkObject,
    ranges: &[VerifiedRangeCopy],
    out: &mut [u8],
    force_scan: bool,
    metrics: Option<&super::read_work::ReadWorkMetrics>,
    mut builder: Option<OutboardBuilder>,
) -> Result<(Vec<usize>, Option<SharedOutboard>)> {
    let mut counts = Vec::with_capacity(ranges.len());
    let mut active = Vec::new();
    for range in ranges.iter().copied() {
        let allowed = if range.chunk_offset >= chunk.length || range.length == 0 {
            0
        } else {
            usize::try_from((chunk.length - range.chunk_offset).min(range.length as u64))
                .map_err(|_| invalid("Chunk read length is too large"))?
        };
        let end = range
            .output_offset
            .checked_add(allowed)
            .ok_or_else(|| invalid("verified Chunk output range overflow"))?;
        if end > out.len() {
            return Err(invalid("verified Chunk output range exceeds buffer"));
        }
        counts.push(allowed);
        if allowed != 0 {
            active.push((range, allowed));
        }
    }
    if active.is_empty() && !force_scan {
        return Ok((counts, None));
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length mismatch"));
    }
    let mut work = metrics.map(super::read_work::ReadWorkMetrics::scan);
    let mut scratch = [0; 64 * 1024];
    let mut hasher = blake3::Hasher::new();
    let mut position = 0;
    while position < chunk.length {
        let limit = (chunk.length - position).min(scratch.len() as u64) as usize;
        let count = read_positioned(file, position, &mut scratch[..limit])?;
        if count == 0 {
            return Err(corrupt(&chunk.id, "ended during verification"));
        }
        if let Some(builder) = &mut builder {
            builder.update(&scratch[..count])?;
        } else {
            hasher.update(&scratch[..count]);
        }
        if let Some(work) = &mut work {
            work.bytes += count as u64;
        }
        #[cfg(test)]
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(bytes.get() + count as u64));
        let next = position + count as u64;
        for (range, allowed) in &active {
            let range_end = range.chunk_offset + *allowed as u64;
            let overlap_start = position.max(range.chunk_offset);
            let overlap_end = next.min(range_end);
            if overlap_start < overlap_end {
                let output_start =
                    range.output_offset + (overlap_start - range.chunk_offset) as usize;
                let output_end = range.output_offset + (overlap_end - range.chunk_offset) as usize;
                if let Some(work) = &mut work {
                    work.copied += (output_end - output_start) as u64;
                }
                out[output_start..output_end].copy_from_slice(
                    &scratch
                        [(overlap_start - position) as usize..(overlap_end - position) as usize],
                );
            }
        }
        position = next;
    }
    if file.metadata().map_err(Error::from)?.len() != chunk.length {
        return Err(corrupt(&chunk.id, "length changed during verification"));
    }
    let (actual, proof) = if let Some(builder) = builder {
        let built = builder.finish()?;
        debug_assert_eq!(built.hash_body_bytes, position);
        if let Some(metrics) = metrics {
            metrics.hash("outboard_parent_hash", built.parent_hash_bytes);
        }
        (built.digest, built.outboard)
    } else {
        (
            ContentDigest {
                algorithm: DigestAlgorithm::Blake3,
                bytes: *hasher.finalize().as_bytes(),
            },
            None,
        )
    };
    if actual != chunk.content_digest {
        return Err(corrupt(&chunk.id, "digest mismatch"));
    }
    Ok((counts, proof))
}

fn digest(bytes: &[u8]) -> ContentDigest {
    ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: *blake3::hash(bytes).as_bytes(),
    }
}

fn digest_hex(digest: &ContentDigest) -> String {
    digest
        .bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn safe_id(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn corrupt(id: &ChunkId, detail: &str) -> Error {
    Error::coded(
        afs_error::NODE_TRANSFER_CORRUPT_DATA,
        format!("local Chunk '{}' {detail}", id.0),
    )
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_STORAGE_INVALID, message)
}

#[cfg(test)]
std::thread_local! {
    static VERIFIED_SCAN_BYTES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static OUTBOARD_PERSIST_IO: std::cell::RefCell<OutboardPersistIo> = const {
        std::cell::RefCell::new(OutboardPersistIo {
            writes: 0,
            file_syncs: 0,
            renames: 0,
            directory_syncs: 0,
        })
    };
    static FAIL_OUTBOARD_DIRECTORY_SYNC: std::cell::Cell<bool> = const {
        std::cell::Cell::new(false)
    };
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
// Count only successful real I/O operations; injected failures do not count.
struct OutboardPersistIo {
    writes: usize,
    file_syncs: usize,
    renames: usize,
    directory_syncs: usize,
}

#[cfg(test)]
mod replica_replay_tests {
    use super::*;

    fn pooled_outboard_chunks(
        budget: &Arc<OutboardBudget>,
        count: usize,
        length: usize,
    ) -> Vec<StagedChunk> {
        let pool = WriteBufferPool::new_with_outboard_budget(count * length, budget.clone());
        (0..count)
            .map(|index| {
                let mut builder = ChunkBuilder::with_pool(pool.clone(), length).unwrap();
                builder.write_at(0, &vec![index as u8; length]).unwrap();
                let staged = builder.stage(OperationId::new(format!("batch-proof-{index}")));
                assert!(staged.outboard.as_ref().unwrap().uses_budget(budget));
                staged
            })
            .collect()
    }

    fn take_outboard_persist_io() -> OutboardPersistIo {
        OUTBOARD_PERSIST_IO.with(|counts| std::mem::take(&mut *counts.borrow_mut()))
    }

    #[test]
    fn wi6_outboard_batch_syncs_directory_once_and_replay_skips_writes() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 16, 4 * 1024 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        take_outboard_persist_io();
        let receipts = store.put_batch(staged.clone()).unwrap();
        assert_eq!(receipts.len(), 16);
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 16,
                file_syncs: 16,
                renames: 16,
                directory_syncs: 1,
            }
        );
        for item in &staged {
            let path = store
                .outboards
                .join(format!("{}.bao", safe_id(&item.chunk.id.0)));
            assert_eq!(
                fs::read(path).unwrap(),
                item.outboard.as_ref().unwrap().encoded()
            );
        }
        let revision = store.device_descriptor().unwrap().catalog_revision;
        let mut replay = staged.clone();
        replay.extend(staged);
        assert_eq!(store.put_batch(replay).unwrap().len(), 32);
        assert_eq!(
            store.device_descriptor().unwrap().catalog_revision,
            revision
        );
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 0,
                file_syncs: 16,
                renames: 0,
                directory_syncs: 1,
            }
        );
    }

    #[test]
    fn wi6_outboard_batch_deduplicates_installs_and_tracks_actual_batches() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 4, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        take_outboard_persist_io();
        for pair in staged.chunks(2) {
            let mut batch = pair.to_vec();
            batch.extend_from_slice(pair);
            assert_eq!(store.put_batch(batch).unwrap().len(), 4);
        }
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 4,
                file_syncs: 4,
                renames: 4,
                directory_syncs: 2,
            }
        );
        store.put_batch(Vec::new()).unwrap();
        store
            .put(StagedChunk::new(
                OperationId::new("no-proof"),
                vec![8; 4096],
            ))
            .unwrap();
        assert_eq!(take_outboard_persist_io(), OutboardPersistIo::default());
    }

    #[test]
    fn wi6_outboard_batch_repairs_corrupt_cv_even_with_matching_header() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 1, 128 * 1024)
            .pop()
            .unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put(staged.clone()).unwrap();
        let path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        let mut corrupt = fs::read(&path).unwrap();
        corrupt[64] ^= 1;
        ChunkOutboard::validate_encoded_shape(&staged.chunk, &corrupt).unwrap();
        fs::write(&path, corrupt).unwrap();
        take_outboard_persist_io();
        store.put(staged.clone()).unwrap();
        assert_eq!(
            fs::read(path).unwrap(),
            staged.outboard.as_ref().unwrap().encoded()
        );
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 1,
                file_syncs: 1,
                renames: 1,
                directory_syncs: 1,
            }
        );
    }

    #[test]
    fn wi6_outboard_batch_optional_failures_keep_acks_and_retry_barriers() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let staged = pooled_outboard_chunks(&budget, 2, 128 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        let blocked = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged[0].chunk.id.0)));
        fs::create_dir(&blocked).unwrap();
        take_outboard_persist_io();
        assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 2,
                file_syncs: 2,
                renames: 1,
                directory_syncs: 1,
            }
        );
        assert!(fs::read_dir(&store.outboards).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        fs::remove_dir(blocked).unwrap();
        FAIL_OUTBOARD_DIRECTORY_SYNC.with(|fail| fail.set(true));
        assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 1,
                file_syncs: 2,
                renames: 1,
                directory_syncs: 0,
            }
        );
        assert_eq!(store.put_batch(staged.clone()).unwrap().len(), 2);
        assert_eq!(
            take_outboard_persist_io(),
            OutboardPersistIo {
                writes: 0,
                file_syncs: 2,
                renames: 0,
                directory_syncs: 1,
            }
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        for item in staged {
            let mut out = [0; 17];
            assert_eq!(recovered.read_at(&item.chunk.id, 3, &mut out).unwrap(), 17);
            assert_eq!(out, item.bytes()[3..20]);
        }
    }

    #[test]
    fn peer_object_digest_capability_requires_the_complete_verified_view() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("whole-proof"), vec![37; 150_001]);
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let whole = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert_eq!(
            whole.verified_full_object_digest(),
            Some(staged.chunk.content_digest.clone())
        );
        assert!(
            store
                .read_peer_range(&staged.chunk.id, 0, staged.bytes().len() - 1)
                .unwrap()
                .verified_full_object_digest()
                .is_none()
        );
        assert!(
            store
                .read_peer_range(&staged.chunk.id, 1, staged.bytes().len() - 1)
                .unwrap()
                .verified_full_object_digest()
                .is_none()
        );
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            staged.chunk.length
        );
    }

    #[test]
    fn peer_object_digest_capability_is_absent_for_full_range_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("fallback-proof"),
            vec![19; PEER_READ_OBJECT_LIMIT + 1],
        );
        store.put(staged.clone()).unwrap();
        let whole = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        assert_eq!(whole.as_slice(), staged.bytes());
        assert!(whole.object.identity.is_none());
        assert!(whole.verified_full_object_digest().is_none());
        assert!(store.peer_read_cache.lock().unwrap().is_none());
        drop(whole);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_digest_does_not_release_shared_frame_budget_early() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("proof-lease"), vec![29; 150_001]);
        store.put(staged.clone()).unwrap();
        let whole = store
            .read_peer_range(&staged.chunk.id, 0, staged.bytes().len())
            .unwrap();
        let digest = whole.verified_full_object_digest().unwrap();
        let bytes = whole.into_shared_bytes();
        let frame = bytes.slice(17..100);
        store.peer_read_cache.lock().unwrap().take();
        drop(bytes);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            staged.chunk.length
        );
        assert_eq!(digest, staged.chunk.content_digest);
        assert_eq!(&frame[..], &[29; 83]);
        drop(frame);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn wi6_pinned_small_reads_do_not_repeat_complete_object_scans() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("outboard-old-copy"),
            vec![19; 4 * 1024 * 1024],
        );
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        for offset in [3, 32771] {
            let mut bytes = [0; 17];
            assert_eq!(reader.read_at(offset, &mut bytes).unwrap(), 17);
            assert_eq!(bytes, [19; 17]);
        }
        let scanned = VERIFIED_SCAN_BYTES.with(|count| count.get());
        assert!(
            scanned <= 2 * 32768,
            "two small ranges scanned {scanned} bytes"
        );
    }

    #[test]
    fn wi6_legacy_and_half_sidecars_recover_without_rewriting_body_or_catalog() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("outboard-recovery"), vec![23; 150_001]);
        store.put(staged.clone()).unwrap();
        let original_revision = store.device_descriptor().unwrap().catalog_revision;
        let body_path = store.chunk_path(&staged.chunk.id);
        let identity = PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap();
        let proof_path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        assert!(
            !proof_path.exists(),
            "unpooled writes retain old root-only behavior"
        );
        let mut permissions = fs::metadata(&body_path).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&body_path, permissions).unwrap();
        // chmod changes ctime, record the identity after restricting the body.
        let identity = PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap_or(identity);
        fs::write(&proof_path, b"partial").unwrap();
        fs::write(store.outboards.join(".abandoned.tmp"), b"partial").unwrap();
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert!(!recovered.outboards.join(".abandoned.tmp").exists());
        let reader = recovered.open_verified(&staged.chunk.id).unwrap();
        assert_eq!(
            PeerFileIdentity::read(&File::open(&body_path).unwrap()).unwrap(),
            identity
        );
        assert_eq!(
            recovered.device_descriptor().unwrap().catalog_revision,
            original_revision
        );
        let encoded = fs::read(&proof_path).unwrap();
        ChunkOutboard::validate_encoded_shape(&staged.chunk, &encoded).unwrap();
        let mut out = [0; 19];
        assert_eq!(reader.read_at(3, &mut out).unwrap(), 19);
        assert_eq!(out, [23; 19]);
        assert!(fs::read_dir(&recovered.outboards).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        }));
        let empty_budget = OutboardBudget::new(0);
        let constrained = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(empty_budget.clone());
        let mut out = [0; 19];
        assert_eq!(
            constrained.read_at(&staged.chunk.id, 3, &mut out).unwrap(),
            19
        );
        assert_eq!(out, [23; 19]);
        assert_eq!(empty_budget.used(), 0);
    }

    #[test]
    fn wi6_canonical_catalog_mismatch_cannot_replace_the_trusted_root() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("canonical-anchor"), vec![3; 150_001]);
        store.put(staged.clone()).unwrap();
        let forged = StagedChunk::new(OperationId::new("forged-catalog"), vec![7; 150_001]);
        fs::write(store.chunk_path(&staged.chunk.id), forged.bytes()).unwrap();
        {
            let mut catalog = store.catalog.lock().unwrap();
            let record = catalog.records.get_mut(&staged.chunk.id).unwrap();
            record.chunk.content_digest = forged.chunk.content_digest.clone();
            record.stored_checksum = forged.chunk.content_digest.clone();
        }
        for error in [
            store.open_verified(&staged.chunk.id).unwrap_err(),
            store.read_peer_range(&staged.chunk.id, 3, 17).unwrap_err(),
        ] {
            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        }
    }

    #[test]
    fn wi6_range_work_budget_pressure_falls_back_to_complete_root() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(16 * 1024);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        let staged = StagedChunk::new(OperationId::new("work-budget-fallback"), vec![5; 150_001]);
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        assert!(reader.load_outboard().is_some());
        let before = budget.used();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let mut out = [0; 17];
        assert_eq!(reader.read_at(3, &mut out).unwrap(), 17);
        assert_eq!(out, [5; 17]);
        assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 150_001);
        assert_eq!(budget.used(), before);
    }

    #[test]
    fn wi6_pooled_stage_keeps_body_address_and_persists_optional_same_budget_proof() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let pool = WriteBufferPool::new_with_outboard_budget(32768, budget.clone());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 32768).unwrap();
        builder.write_at(0, &vec![7; 32768]).unwrap();
        let frozen = builder.freeze();
        let pointer = frozen.as_ptr();
        let staged = StagedChunk::from_shared(OperationId::new("outboard-pooled"), frozen);
        assert_eq!(staged.bytes().as_ptr(), pointer);
        assert!(staged.outboard.as_ref().unwrap().uses_budget(&budget));
        assert_eq!(pool.snapshot().copied_bytes, 32768);
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget.clone());
        store.put(staged.clone()).unwrap();
        let proof_path = store
            .outboards
            .join(format!("{}.bao", safe_id(&staged.chunk.id.0)));
        ChunkOutboard::validate_encoded_shape(&staged.chunk, &fs::read(proof_path).unwrap())
            .unwrap();
        let plain_pool = WriteBufferPool::new(32768);
        let mut plain = ChunkBuilder::with_pool(plain_pool, 32768).unwrap();
        plain.write_at(0, b"plain").unwrap();
        assert!(
            plain
                .stage(OperationId::new("root-only"))
                .outboard
                .is_none()
        );
    }

    #[test]
    fn wi6_peer_partial_uses_covering_groups_and_exact_payload_lease() {
        let temp = tempfile::tempdir().unwrap();
        let budget = OutboardBudget::new(DEFAULT_OUTBOARD_BUDGET_BYTES);
        let pool = WriteBufferPool::new_with_outboard_budget(4 * 1024 * 1024, budget.clone());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4 * 1024 * 1024).unwrap();
        builder.write_at(0, &vec![11; 4 * 1024 * 1024]).unwrap();
        let staged = builder.stage(OperationId::new("outboard-partial"));
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_outboard_budget(budget);
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        let payload = store.read_peer_range(&staged.chunk.id, 3, 17).unwrap();
        assert_eq!(payload.as_slice(), &[11; 17]);
        assert!(payload.verified_full_object_digest().is_none());
        assert!(payload.outboard().is_none());
        assert_eq!(VERIFIED_SCAN_BYTES.with(|count| count.get()), 32768);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 17);
        drop(payload);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_reuses_one_complete_scan_for_multiple_ranges() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes = vec![37; 4 * 1024 * 1024];
        let staged = StagedChunk::new(OperationId::new("peer-reuse"), bytes.clone());
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|count| count.set(0));
        for index in 0..4 {
            let payload = store
                .read_peer_range(&staged.chunk.id, index * 1024 * 1024, 1024 * 1024)
                .unwrap();
            assert_eq!(payload.as_slice(), &bytes[..1024 * 1024]);
        }
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|count| count.get()),
            bytes.len() as u64
        );
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            bytes.len() as u64
        );
    }

    #[test]
    fn peer_object_revalidates_replacement_missing_and_quarantined_files() {
        for change in [
            "replace",
            "missing",
            "quarantine",
            "corrupt",
            "epoch",
            "revision",
        ] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let bytes = vec![53; 150_001];
            let staged = StagedChunk::new(OperationId::new("peer-identity"), bytes.clone());
            store.put(staged.clone()).unwrap();
            drop(store.read_peer_range(&staged.chunk.id, 3, 19).unwrap());
            VERIFIED_SCAN_BYTES.with(|count| count.set(0));
            match change {
                "replace" => {
                    let replacement = temp.path().join("replacement");
                    fs::write(&replacement, &bytes).unwrap();
                    fs::rename(replacement, store.chunk_path(&staged.chunk.id)).unwrap();
                }
                "missing" => fs::remove_file(store.chunk_path(&staged.chunk.id)).unwrap(),
                "corrupt" => {
                    fs::write(store.chunk_path(&staged.chunk.id), vec![1; bytes.len()]).unwrap()
                }
                _ => {
                    let mut catalog = store.catalog.lock().unwrap();
                    let record = catalog.records.get_mut(&staged.chunk.id).unwrap();
                    match change {
                        "quarantine" => record.state = LocalChunkState::Quarantined,
                        "epoch" => record.device_epoch += 1,
                        "revision" => record.catalog_revision += 1,
                        _ => unreachable!(),
                    }
                }
            }
            let result = store.read_peer_range(&staged.chunk.id, 3, 19);
            if change == "replace" || change == "revision" {
                assert_eq!(result.unwrap().as_slice(), &bytes[3..22]);
                assert_eq!(
                    VERIFIED_SCAN_BYTES.with(|count| count.get()),
                    bytes.len() as u64
                );
            } else {
                assert!(result.is_err(), "{change} must not reuse a stale proof");
                assert!(store.peer_read_cache.lock().unwrap().is_none());
            }
        }
    }

    #[test]
    fn peer_object_failed_fill_releases_budget_and_never_publishes() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("peer-failed"), vec![5; 4096]);
        store.put(staged.clone()).unwrap();
        fs::write(store.chunk_path(&staged.chunk.id), vec![9; 4096]).unwrap();
        assert!(store.read_peer_range(&staged.chunk.id, 0, 3).is_err());
        assert!(store.peer_read_cache.lock().unwrap().is_none());
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_evicted_references_and_packed_payload_share_the_limit() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let chunks: Vec<_> = (1..=3)
            .map(|byte| {
                let chunk = StagedChunk::new(
                    OperationId::new(format!("peer-budget-{byte}")),
                    vec![byte; 4 * 1024 * 1024],
                );
                store.put(chunk.clone()).unwrap();
                chunk
            })
            .collect();
        let first = store.read_peer_range(&chunks[0].chunk.id, 0, 1).unwrap();
        let second = store.read_peer_range(&chunks[1].chunk.id, 0, 1).unwrap();
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            PEER_READ_PAYLOAD_LIMIT as u64
        );
        assert_eq!(
            store
                .read_peer_range(&chunks[2].chunk.id, 0, 1)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(
            store.reserve_peer_read_payload(1).unwrap_err().code(),
            afs_error::IO_CAPACITY
        );
        assert_eq!(first.as_slice(), &[1]);
        drop(first);
        drop(second);
        let mut packed = store.reserve_peer_read_payload(4 * 1024 * 1024).unwrap();
        packed.retain_capacity(4 * 1024 * 1024).unwrap();
        assert!(store.read_peer_range(&chunks[2].chunk.id, 0, 1).is_ok());
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            PEER_READ_PAYLOAD_LIMIT as u64
        );
        drop(packed);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            4 * 1024 * 1024
        );
        assert_eq!(
            store
                .reserve_peer_read_payload(PEER_READ_PAYLOAD_LIMIT + 1)
                .unwrap_err()
                .code(),
            afs_error::IO_CAPACITY
        );
    }

    #[test]
    fn peer_object_budget_reservation_drop_and_capacity_growth_are_charged() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let mut lease = store.reserve_peer_read_payload(19).unwrap();
        lease.retain_capacity(31).unwrap();
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        assert!(lease.retain_capacity(PEER_READ_PAYLOAD_LIMIT + 1).is_err());
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 31);
        drop(lease);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_concurrent_ranges_share_the_same_verified_payload() {
        let temp = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let staged = StagedChunk::new(
            OperationId::new("peer-singleflight"),
            vec![73; 4 * 1024 * 1024],
        );
        store.put(staged.clone()).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(4));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let store = store.clone();
                let id = staged.chunk.id.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    store.read_peer_range(&id, 0, 19).unwrap()
                })
            })
            .collect();
        let payloads: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        for payload in &payloads {
            assert!(Arc::ptr_eq(&payload.object, &payloads[0].object));
            assert_eq!(payload.as_slice(), &[73; 19]);
        }
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            4 * 1024 * 1024
        );
    }

    #[test]
    fn peer_object_shared_frame_slices_retain_allocation_and_lease() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("shared-frame-owner"),
            vec![29; 4 * 1024 * 1024],
        );
        store.put(staged.clone()).unwrap();
        let payload = store.read_peer_range(&staged.chunk.id, 17, 128).unwrap();
        let pointer = payload.as_slice().as_ptr();
        let bytes = payload.into_shared_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        let frame = bytes.slice(3..67);
        assert_eq!(frame.as_ptr(), bytes[3..].as_ptr());
        let retained = frame.clone();
        drop(frame);
        drop(bytes);
        store.peer_read_cache.lock().unwrap().take();
        assert_eq!(&retained[..], &[29; 64]);
        assert_eq!(
            store.peer_read_budget.used.load(Ordering::Acquire),
            4 * 1024 * 1024
        );
        drop(retained);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn peer_object_oversized_chunk_uses_charged_range_fallback_without_residency() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(
            OperationId::new("peer-fallback"),
            vec![91; PEER_READ_OBJECT_LIMIT + 1],
        );
        store.put(staged.clone()).unwrap();
        let range = store
            .read_peer_range(&staged.chunk.id, PEER_READ_OBJECT_LIMIT as u64, 19)
            .unwrap();
        assert_eq!(range.as_slice(), &[91]);
        assert!(store.peer_read_cache.lock().unwrap().is_none());
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 1);
        let pointer = range.as_slice().as_ptr();
        let bytes = range.into_shared_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        let retained = bytes.slice(..);
        drop(bytes);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 1);
        assert_eq!(&retained[..], &[91]);
        drop(retained);
        assert_eq!(store.peer_read_budget.used.load(Ordering::Acquire), 0);
    }

    #[test]
    fn private_range_reads_scan_once_and_copy_only_the_requested_overlap() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes: Vec<_> = (0..150_001).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(OperationId::new("single-scan"), bytes.clone());
        store.put(staged.clone()).unwrap();
        let mut out = [9; 19];
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
        assert_eq!(
            store.read_at(&staged.chunk.id, 65_531, &mut out).unwrap(),
            19
        );
        assert_eq!(
            VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()),
            bytes.len() as u64 + 2 * outboard::OUTBOARD_GROUP_BYTES as u64
        );
        for offset in [65_531, 149_990] {
            out.fill(9);
            VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
            let count = store
                .read_range_at_uncommitted(&staged.chunk.id, offset, &mut out)
                .unwrap();
            let expected = 19.min(bytes.len() - offset as usize);
            assert_eq!(count, expected);
            assert_eq!(
                &out[..count],
                &bytes[offset as usize..offset as usize + count]
            );
            assert!(out[count..].iter().all(|byte| *byte == 9));
            assert_eq!(
                VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()),
                bytes.len() as u64
            );
        }
    }

    #[test]
    fn private_range_pins_identity_before_its_only_scan() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("single-pin"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        VERIFIED_SCAN_BYTES.with(|bytes| bytes.set(0));
        let reader = store.open_cataloged(&staged.chunk.id).unwrap();
        assert_eq!(VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()), 0);
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        fs::rename(&path, temp.path().join("old-inode")).unwrap();
        fs::write(&path, b"abXdef").unwrap();
        let mut private = [9; 3];
        assert_eq!(
            reader
                .read_ranges_at_uncommitted(
                    &[VerifiedRangeCopy {
                        chunk_offset: 1,
                        length: private.len(),
                        output_offset: 0,
                    }],
                    &mut private,
                )
                .unwrap(),
            vec![3]
        );
        assert_eq!(&private, b"bcd");
        assert_eq!(VERIFIED_SCAN_BYTES.with(|bytes| bytes.get()), 6);
        assert!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 1, &mut private)
                .is_err()
        );
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn private_range_rejects_outside_damage_and_quarantines_until_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("single-corrupt"), vec![42; 150_001]);
        store.put(staged.clone()).unwrap();
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        let mut damaged = staged.bytes().to_vec();
        damaged[150_000] ^= 1;
        fs::write(&path, damaged).unwrap();
        let mut private = [9; 19];
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 65_531, &mut private)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
        store.put(staged.clone()).unwrap();
        assert!(store.quarantined_chunks().unwrap().is_empty());
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 65_531, &mut private)
                .unwrap(),
            private.len()
        );
        assert_eq!(private, [42; 19]);
    }

    #[test]
    fn private_range_and_verified_open_quarantine_missing_cataloged_files() {
        for private_read in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
            let staged = StagedChunk::new(OperationId::new("missing-file"), b"abcdef".to_vec());
            store.put(staged.clone()).unwrap();
            fs::remove_file(temp.path().join("chunks").join(&staged.chunk.id.0)).unwrap();
            if private_read {
                assert!(
                    store
                        .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [9; 3])
                        .is_err()
                );
            } else {
                assert!(store.open_verified(&staged.chunk.id).is_err());
            }
            assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
            store.put(staged.clone()).unwrap();
            assert!(store.quarantined_chunks().unwrap().is_empty());
            let mut out = [9; 3];
            assert_eq!(
                store
                    .read_range_at_uncommitted(&staged.chunk.id, 0, &mut out)
                    .unwrap(),
                3
            );
            assert_eq!(&out, b"abc");
        }
    }

    #[test]
    fn private_range_work_metrics_count_actual_scan_and_copy_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let store = LocalChunkStore::open(temp.path(), "node-a")
            .unwrap()
            .with_read_work_metrics(
                super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
            );
        let staged = StagedChunk::new(OperationId::new("work-counts"), vec![7; 65_537]);
        store.put(staged.clone()).unwrap();
        store.read_at(&staged.chunk.id, 0, &mut [0; 3]).unwrap();
        store
            .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [0; 3])
            .unwrap();
        let text = afs_metrics::encode_text(&registry).unwrap();
        assert!(text.contains("afs_dfs_read_scans_total 2\n"), "{text}");
        assert!(
            text.contains("afs_dfs_read_scan_bytes_total 131074\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_copy_bytes_total{stage=\"scan_to_range\"} 3\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_copy_bytes_total{stage=\"pinned_range_to_output\"} 3\n"),
            "{text}"
        );
        // The verified range passes through the official Bao codec before
        // exact extraction: two full covering-group copies plus three bytes.
        assert!(
            text.contains(
                "afs_dfs_read_copy_bytes_total{stage=\"outboard_range_delivery\"} 65539\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn private_range_rejects_truncated_files_and_keeps_eof_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("single-truncate"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let mut private = [9; 8];
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 4, &mut private)
                .unwrap(),
            2
        );
        assert_eq!(&private, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, u64::MAX, &mut private)
                .unwrap(),
            0
        );
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 0, &mut [])
                .unwrap(),
            0
        );
        fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
        assert_eq!(
            store
                .read_range_at_uncommitted(&staged.chunk.id, 0, &mut private)
                .unwrap_err()
                .code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(store.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn corrupt_cataloged_chunk_is_replaced_by_immutable_retry() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("retry-corrupt"), b"abcdef".to_vec());
        let first = store.put(staged.clone()).unwrap();
        let pin = store.open_verified(&staged.chunk.id).unwrap();
        fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            b"abXdef",
        )
        .unwrap();
        let repaired = store.put(staged.clone()).unwrap();
        assert!(repaired.durable_acks[0].catalog_revision > first.durable_acks[0].catalog_revision);
        let mut out = [9; 6];
        assert!(pin.read_at(0, &mut out).is_err());
        assert_eq!(out, [9; 6]);
        // Revalidating an old pin cannot quarantine the newer healthy file.
        store.quarantine_if_invalid(&staged.chunk.id).unwrap();
        assert!(store.quarantined_chunks().unwrap().is_empty());
        assert_eq!(store.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"abcdef");
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap(), 6);
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn capacity_reports_fd_backed_root_after_path_replacement_and_catalog_advance() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("store");
        let store = LocalChunkStore::open(&root, "node-a").unwrap();
        let first = store.capacity().unwrap();
        assert!(first.bsize > 0);
        assert!(first.frsize > 0);
        assert!(first.namelen > 0);
        assert!(first.blocks >= first.bfree);
        assert!(first.bfree >= first.bavail);

        store
            .put(StagedChunk::new(
                OperationId::new("capacity"),
                b"bytes".to_vec(),
            ))
            .unwrap();
        let descriptor_after_write = store.device_descriptor().unwrap();
        assert!(descriptor_after_write.catalog_revision > 0);

        let moved = temp.path().join("store-moved");
        fs::rename(&root, &moved).unwrap();
        // A path-based reopen must fail; the held directory FD remains valid.
        std::os::unix::fs::symlink(temp.path().join("missing"), &root).unwrap();

        let after_replacement = store.capacity().unwrap();
        assert!(after_replacement.bsize > 0);
        assert!(after_replacement.frsize > 0);
        assert_eq!(store.device_descriptor().unwrap(), descriptor_after_write);
    }

    #[test]
    fn quarantine_survives_restart_and_verified_replacement_clears_it() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("quarantine"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        fs::write(
            temp.path().join("chunks").join(&staged.chunk.id.0),
            b"abXdef",
        )
        .unwrap();
        assert!(store.open_verified(&staged.chunk.id).is_err());
        let records = store.quarantined_chunks().unwrap();
        assert_eq!(records.len(), 1);
        let quarantine_revision = records[0].catalog_revision;
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.quarantined_chunks().unwrap(), records);
        assert!(recovered.open_verified(&staged.chunk.id).is_err());
        let receipt = recovered.put(staged.clone()).unwrap();
        assert!(receipt.durable_acks[0].catalog_revision > quarantine_revision);
        assert!(recovered.quarantined_chunks().unwrap().is_empty());
        let mut out = [0; 6];
        recovered.read_at(&staged.chunk.id, 0, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn recovery_quarantines_missing_or_truncated_chunks_without_losing_healthy_files() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let items: Vec<_> = ["missing", "truncated", "healthy"]
            .into_iter()
            .map(|name| StagedChunk::new(OperationId::new(name), name.as_bytes().to_vec()))
            .collect();
        for item in &items {
            store.put(item.clone()).unwrap();
        }
        fs::remove_file(temp.path().join("chunks").join(&items[0].chunk.id.0)).unwrap();
        fs::write(temp.path().join("chunks").join(&items[1].chunk.id.0), b"x").unwrap();
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        assert_eq!(recovered.quarantined_chunks().unwrap().len(), 2);
        let mut out = [0; 7];
        recovered.read_at(&items[2].chunk.id, 0, &mut out).unwrap();
        assert_eq!(&out, b"healthy");
        for item in &items[..2] {
            recovered.put(item.clone()).unwrap();
        }
        assert!(recovered.quarantined_chunks().unwrap().is_empty());
    }

    #[test]
    fn pinned_reader_verifies_partial_ranges_and_rejects_damage_outside_them() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let bytes: Vec<_> = (0..524_289).map(|index| (index % 251) as u8).collect();
        let staged = StagedChunk::new(OperationId::new("range"), bytes.clone());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let mut out = [9; 19];
        // Cover both the old 64KiB and candidate 256KiB scan boundaries,
        // including a final short scan outside either requested range.
        for offset in [65_531, 262_139] {
            assert_eq!(reader.read_at(offset, &mut out).unwrap(), 19);
            assert_eq!(&out, &bytes[offset as usize..offset as usize + 19]);
        }
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        let mut damaged = bytes;
        damaged[524_288] ^= 1;
        fs::write(path, damaged).unwrap();
        out.fill(9);
        assert_eq!(
            reader.read_at(65_531, &mut out).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [9; 19]);
    }

    #[test]
    fn verify_file_zero_range_still_hashes_the_whole_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("verify-zero"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let path = temp.path().join("chunks").join(&staged.chunk.id.0);
        verify_file(&path, &staged.chunk).unwrap();
        fs::write(&path, b"abcXef").unwrap();
        assert_eq!(
            verify_file(&path, &staged.chunk).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
    }

    #[test]
    fn pinned_reader_rejects_truncation_without_publishing_a_partial_read() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("truncate"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        fs::write(temp.path().join("chunks").join(&staged.chunk.id.0), b"abc").unwrap();
        let mut out = [9; 6];
        assert_eq!(
            reader.read_at(0, &mut out).unwrap_err().code(),
            afs_error::NODE_TRANSFER_CORRUPT_DATA
        );
        assert_eq!(out, [9; 6]);
    }

    #[test]
    fn pinned_reader_preserves_descriptor_identity_and_eof_behavior() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let staged = StagedChunk::new(OperationId::new("pin"), b"abcdef".to_vec());
        store.put(staged.clone()).unwrap();
        let reader = store.open_verified(&staged.chunk.id).unwrap();
        let replacement = temp.path().join("replacement");
        fs::write(&replacement, b"abXdef").unwrap();
        fs::rename(
            replacement,
            temp.path().join("chunks").join(&staged.chunk.id.0),
        )
        .unwrap();
        let mut out = [9; 8];
        assert_eq!(reader.read_at(0, &mut out).unwrap(), 6);
        assert_eq!(&out, &[b'a', b'b', b'c', b'd', b'e', b'f', 9, 9]);
        out.fill(9);
        assert_eq!(reader.read_at(4, &mut out).unwrap(), 2);
        assert_eq!(&out, &[b'e', b'f', 9, 9, 9, 9, 9, 9]);
        assert_eq!(reader.read_at(u64::MAX, &mut out).unwrap(), 0);
        assert_eq!(reader.read_at(0, &mut []).unwrap(), 0);
        assert!(store.open_verified(&staged.chunk.id).is_err());
    }

    #[test]
    fn immutable_chunk_retry_preserves_content_after_catalog_advances_and_restart() {
        let temp = tempfile::tempdir().unwrap();
        let store = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let target = ReplicaTarget {
            node_id: "node-a".into(),
            node_epoch: 3,
            data_endpoint: "https://node-a".into(),
            device: store.device_descriptor().unwrap(),
        };
        let first = StagedChunk::new(OperationId::new("op-first"), b"first".to_vec());
        let ack = store.persist(&first, &target, 4, 5).unwrap();
        store
            .persist(
                &StagedChunk::new(OperationId::new("op-second"), b"second".to_vec()),
                &target,
                4,
                5,
            )
            .unwrap();
        let replay = store.persist(&first, &target, 4, 5).unwrap();
        assert_eq!(replay.chunk_id, ack.chunk_id);
        assert_eq!(replay.verified_digest, ack.verified_digest);
        assert_eq!(replay.persisted_bytes, ack.persisted_bytes);
        assert!(replay.catalog_revision > ack.catalog_revision);
        let mut newer_target = target.clone();
        newer_target.device = store.device_descriptor().unwrap();
        let floor_ack = store.persist(&first, &newer_target, 6, 7).unwrap();
        assert_eq!(
            floor_ack.catalog_revision,
            newer_target.device.catalog_revision
        );
        drop(store);
        let recovered = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let recovered_ack = recovered.persist(&first, &target, 4, 5).unwrap();
        assert_eq!(recovered_ack.chunk_id, ack.chunk_id);
        assert_eq!(recovered_ack.verified_digest, ack.verified_digest);
        assert_eq!(recovered_ack.catalog_revision, replay.catalog_revision);
        let mut bytes = [0; 5];
        assert_eq!(
            recovered.read_at(&first.chunk.id, 0, &mut bytes).unwrap(),
            5
        );
        assert_eq!(&bytes, b"first");
        assert_eq!(
            recovered.persist(&first, &newer_target, 6, 7).unwrap(),
            floor_ack
        );
    }
}

#[cfg(test)]
mod write_buffer_tests {
    use super::*;

    #[test]
    fn writable_capacity_keeps_requested_limit_separate_from_reused_allocation() {
        let pool = WriteBufferPool::new(8192);
        drop(ChunkBuilder::with_pool(pool.clone(), 4096).unwrap());
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 3000).unwrap();
        assert_eq!(builder.capacity(), 4096);
        assert_eq!(builder.writable_capacity(), 3000);
        builder.write_at(2999, &[7]).unwrap();
        assert_eq!(
            builder.write_at(3000, &[8]).unwrap_err().code(),
            afs_error::IO_OUT_OF_MEMORY
        );
        assert_eq!(pool.snapshot().live_bytes, 4096);
    }

    #[test]
    fn hot_large_allocation_cannot_inflate_a_small_patch() {
        let pool = WriteBufferPool::new(8 * 1024 * 1024);
        let large = ChunkBuilder::with_pool(pool.clone(), 4 * 1024 * 1024).unwrap();
        let large_pointer = large.data_ptr();
        drop(large);
        let mut patch = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        patch.write_at(0, &[3; 4096]).unwrap();
        assert_eq!(patch.capacity(), 4096);
        assert_ne!(patch.data_ptr(), large_pointer);
        assert_eq!(pool.snapshot().live_bytes, 4096);
        assert_eq!(pool.snapshot().idle_bytes, 4 * 1024 * 1024);
        assert_eq!(pool.snapshot().allocations, 2);
        assert_eq!(pool.snapshot().reuses, 0);
    }

    #[test]
    fn six_mebibyte_body_uses_two_final_allocations_without_freeze_copy() {
        const MIB: usize = 1024 * 1024;
        let pool = WriteBufferPool::new(8 * MIB);
        let mut first = ChunkBuilder::with_pool(pool.clone(), 4 * MIB).unwrap();
        let mut tail = ChunkBuilder::with_pool(pool.clone(), 4 * MIB).unwrap();
        let input = vec![37; MIB];
        for index in 0..6 {
            let (builder, offset) = if index < 4 {
                (&mut first, index * MIB)
            } else {
                (&mut tail, (index - 4) * MIB)
            };
            builder.write_at(offset as u64, &input).unwrap();
        }
        let first_pointer = first.data_ptr();
        let tail_pointer = tail.data_ptr();
        let first_generation = first.allocation_id();
        let tail_generation = tail.allocation_id();
        let first = first.stage(OperationId::new("four-mib"));
        let tail = tail.stage(OperationId::new("two-mib"));
        assert_eq!(first.bytes().len(), 4 * MIB);
        assert_eq!(tail.bytes().len(), 2 * MIB);
        assert_eq!(first.bytes().as_ptr(), first_pointer);
        assert_eq!(tail.bytes().as_ptr(), tail_pointer);
        assert_eq!(first.bytes.allocation_id(), first_generation);
        assert_eq!(tail.bytes.allocation_id(), tail_generation);
        assert!(
            first
                .bytes()
                .iter()
                .chain(tail.bytes())
                .all(|byte| *byte == 37)
        );
        let stats = pool.snapshot();
        assert_eq!(stats.live_bytes, 8 * MIB);
        assert_eq!(stats.copied_bytes, 6 * MIB as u64);
        assert_eq!(stats.copy_operations, 6);
        assert_eq!(stats.allocations, 2);
        drop(first);
        drop(tail);
        assert_eq!(pool.snapshot().live_bytes, 0);
        assert_eq!(pool.snapshot().idle_bytes, 8 * MIB);
    }

    #[test]
    fn actual_copy_entries_count_overlap_extension_and_zeroed_holes_separately() {
        let pool = WriteBufferPool::new(32);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 16).unwrap();
        builder.write_at(0, b"abc").unwrap();
        builder.write_at(2, b"def").unwrap();
        builder.write_at(8, b"g").unwrap();
        assert_eq!(builder.as_bytes(), b"abdef\0\0\0g");
        assert_eq!(pool.snapshot().copied_bytes, 7);
        // The extension crossing the initialized tail copies one existing and
        // one new range; sparse zero initialization is not input copying.
        assert_eq!(pool.snapshot().copy_operations, 4);
    }

    #[test]
    fn exact_metrics_track_last_owner_reuse_and_stop() {
        let registry = afs_metrics::Registry::new();
        let pool = WriteBufferPool::new(4096);
        pool.register_metrics(&registry).unwrap();
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"data").unwrap();
        let frozen = builder.freeze();
        let delayed = frozen.slice(..);
        drop(frozen);
        let before = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            before.contains("afs_dfs_write_buffer_live_bytes 4096\n"),
            "{before}"
        );
        assert!(
            before.contains("afs_dfs_write_buffer_idle_bytes 0\n"),
            "{before}"
        );
        drop(delayed);
        let reused = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        drop(reused);
        pool.stop();
        let after = afs_metrics::encode_text(&registry).unwrap();
        for expected in [
            "afs_dfs_write_buffer_live_bytes 0\n",
            "afs_dfs_write_buffer_idle_bytes 0\n",
            "afs_dfs_write_buffer_live_peak_bytes 4096\n",
            "afs_dfs_write_buffer_allocations_total 1\n",
            "afs_dfs_write_buffer_reuses_total 1\n",
            "afs_dfs_write_buffer_released_bytes_total 4096\n",
            "afs_dfs_write_buffer_input_copy_bytes_total 4\n",
            "afs_dfs_write_buffer_input_copies_total 1\n",
        ] {
            assert!(after.contains(expected), "missing {expected} in {after}");
        }
    }

    #[test]
    fn freeze_and_shared_ranges_keep_allocation_until_last_owner() {
        let pool = WriteBufferPool::new(8192);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"abcdef").unwrap();
        let pointer = builder.data_ptr();
        let generation = builder.allocation_id();
        let capacity = builder.capacity();
        let frozen = builder.freeze();
        assert_eq!(frozen.as_ptr(), pointer);
        assert_eq!(frozen.allocation_id(), generation);
        assert_eq!(frozen.capacity(), capacity);
        let range = frozen.slice(1..4);
        let staged = StagedChunk::from_shared(OperationId::new("shared-freeze"), range.clone());
        assert_eq!(staged.bytes(), b"bcd");
        assert_eq!(staged.bytes().as_ptr(), pointer.wrapping_add(1));
        assert_eq!(staged.chunk.length, 3);
        assert_eq!(pool.snapshot().copied_bytes, 6);
        drop(frozen);
        drop(range);
        assert_eq!(pool.snapshot().live_buffers, 1);
        assert_eq!(pool.snapshot().idle_bytes, 0);
        drop(staged);
        assert_eq!(pool.snapshot().live_bytes, 0);
        assert_eq!(pool.snapshot().idle_bytes, capacity);
    }

    #[test]
    fn reuse_has_new_generation_and_never_exposes_old_data() {
        let pool = WriteBufferPool::new(8192);
        let mut first = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        first.write_at(0, &[0xa5; 4096]).unwrap();
        let pointer = first.data_ptr();
        let generation = first.allocation_id();
        drop(first);
        let mut next = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        assert_eq!(next.data_ptr(), pointer);
        assert_ne!(next.allocation_id(), generation);
        assert_eq!(next.len(), 0);
        next.write_at(8, &[7]).unwrap();
        assert_eq!(next.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 7]);
        next.truncate(3);
        next.write_at(8, &[9]).unwrap();
        assert_eq!(next.as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 0, 9]);
        assert_eq!(pool.snapshot().allocations, 1);
        assert_eq!(pool.snapshot().reuses, 1);
    }

    #[test]
    fn idle_baseline_does_not_limit_live_and_stop_retires_late_owners() {
        let pool = WriteBufferPool::new(4096);
        let mut builders = Vec::new();
        for _ in 0..3 {
            let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
            builder.write_at(0, &[1; 4096]).unwrap();
            builders.push(builder);
        }
        assert_eq!(pool.snapshot().live_bytes, 3 * 4096);
        let retained = builders.pop().unwrap().freeze();
        drop(builders);
        assert_eq!(pool.snapshot().live_bytes, 4096);
        assert_eq!(pool.snapshot().idle_bytes, 4096);
        assert_eq!(pool.snapshot().released_bytes, 4096);
        pool.stop();
        assert_eq!(pool.snapshot().idle_bytes, 0);
        assert_eq!(pool.snapshot().live_bytes, 4096);
        drop(retained);
        assert_eq!(pool.snapshot().live_bytes, 0);
        assert_eq!(pool.snapshot().released_bytes, 3 * 4096);
        let after_stop = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        drop(after_stop);
        assert_eq!(pool.snapshot().idle_bytes, 0);
    }

    #[test]
    fn fixed_capacity_failure_keeps_previously_accepted_bytes() {
        let pool = WriteBufferPool::new(0);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4).unwrap();
        builder.write_at(0, b"abcd").unwrap();
        let pointer = builder.data_ptr();
        let error = builder.write_at(3, b"ef").unwrap_err();
        assert_eq!(error.code(), afs_error::IO_OUT_OF_MEMORY);
        assert_eq!(builder.as_bytes(), b"abcd");
        assert_eq!(builder.data_ptr(), pointer);
        assert_eq!(pool.snapshot().copied_bytes, 4);
        assert_eq!(pool.snapshot().copy_operations, 1);
        pool.fail_next_allocation();
        assert_eq!(
            ChunkBuilder::with_pool(pool.clone(), 8).unwrap_err().code(),
            afs_error::IO_OUT_OF_MEMORY
        );
        assert_eq!(pool.snapshot().live_bytes, 4);
    }

    #[test]
    fn pooled_builder_cannot_keep_pool_alive_or_modify_a_frozen_chunk() {
        let pool = WriteBufferPool::new(4096);
        let weak = Arc::downgrade(&pool);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"original").unwrap();
        let frozen = builder.freeze();
        drop(pool);
        assert!(weak.upgrade().is_none());
        assert_eq!(&*frozen, b"original");
        drop(frozen);
    }

    #[test]
    fn staged_vec_and_default_builder_keep_legacy_content_without_vec_to_arc_copy() {
        let bytes = b"owned".to_vec();
        let pointer = bytes.as_ptr();
        let staged = StagedChunk::new(OperationId::new("vec-owner"), bytes);
        assert_eq!(staged.bytes().as_ptr(), pointer);
        let mut builder = ChunkBuilder::default();
        builder.write_at(3, b"data").unwrap();
        assert_eq!(builder.as_bytes(), b"\0\0\0data");
        builder.replace(b"replacement".to_vec());
        assert_eq!(
            builder.stage(OperationId::new("legacy")).bytes(),
            b"replacement"
        );
    }

    #[test]
    fn mutable_overwrite_is_in_place_and_frozen_identity_is_immutable() {
        let pool = WriteBufferPool::new(8192);
        let mut builder = ChunkBuilder::with_pool(pool.clone(), 4096).unwrap();
        builder.write_at(0, b"abcdef").unwrap();
        let pointer = builder.data_ptr();
        builder.write_at(2, b"XY").unwrap();
        assert_eq!(builder.data_ptr(), pointer);
        let staged = builder.stage(OperationId::new("first"));
        assert_eq!(staged.bytes(), b"abXYef");
        let mut patch = ChunkBuilder::with_pool(pool.clone(), 2).unwrap();
        patch.write_at(0, b"zz").unwrap();
        assert_eq!(patch.stage(OperationId::new("patch")).bytes(), b"zz");
        assert_eq!(staged.bytes(), b"abXYef");
        assert_eq!(pool.snapshot().copied_bytes, 10);
    }
}
