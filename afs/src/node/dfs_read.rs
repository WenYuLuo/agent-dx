//! DFS fixed-version Chunk read path.
//!
//! DistributedFs resolves a file range into immutable Chunk ranges, then this
//! module chooses local or peer sources, or an explicitly configured external
//! backend. All paths publish into the caller buffer only after the whole
//! requested batch succeeds. External fixed-version reads reuse bounded
//! authenticated contexts; full immutable-object verification remains required.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use afs_error::{Error, Result};
use afs_tracing::tracing;

use crate::{
    dfs::{
        AuthorizeCurrentExternalRead, AuthorizeCurrentExternalReadReply,
        AuthorizeExternalVersionRead, AuthorizeExternalVersionReadReply, ChunkId, ChunkObject,
        ChunkSources, CopyLocation, CopyRole, CopyState, DfsChunkSourcesReply,
        DfsChunkSourcesRequest, DfsExternalAuthorizedRead, DfsMissingReadSource, DfsReadGrant,
        DfsReadValidation, FileVersionId, LayoutRootId, SourceCandidate,
    },
    node::chunk::{LocalChunkStore, PinnedChunkReader, VerifiedRangeCopy},
};

/// Hard protocol budgets shared by the scheduler, peer adapter and service.
pub const MAX_DFS_READ_OPS: usize = 128;
pub const MAX_DFS_READ_BYTES: u64 = 8 * 1024 * 1024;
/// External objects are fetched and verified one at a time, independently of
/// the requested range budget. This matches the producer's staged chunk limit.
pub const MAX_DFS_EXTERNAL_CHUNK_BYTES: u64 = crate::node::chunk::MAX_STAGED_CHUNK_BYTES as u64;
const MAX_EXTERNAL_AUTH_CACHE_ENTRIES: usize = 128;
const MAX_EXTERNAL_AUTH_CACHE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct DfsReadConfig {
    pub max_ops_per_batch: usize,
    pub max_inflight_bytes: u64,
    pub source_cache_ttl: Duration,
}

impl Default for DfsReadConfig {
    fn default() -> Self {
        Self {
            max_ops_per_batch: MAX_DFS_READ_OPS,
            max_inflight_bytes: MAX_DFS_READ_BYTES,
            source_cache_ttl: Duration::from_millis(500),
        }
    }
}

impl DfsReadConfig {
    /// Reject unsupported configuration before it becomes a peer-only failure.
    pub fn validate(&self) -> Result<()> {
        if self.max_ops_per_batch == 0
            || self.max_ops_per_batch > MAX_DFS_READ_OPS
            || self.max_inflight_bytes == 0
            || self.max_inflight_bytes > MAX_DFS_READ_BYTES
        {
            return Err(invalid(format!(
                "DFS read configuration requires 1..={MAX_DFS_READ_OPS} operations and 1..={MAX_DFS_READ_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkReadOp {
    pub chunk_id: ChunkId,
    pub chunk_offset: u64,
    pub length: u64,
    pub output_offset: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadBatch {
    pub file_version_id: Option<FileVersionId>,
    pub layout_root_id: LayoutRootId,
    pub ops: Vec<ChunkReadOp>,
}

impl ReadBatch {
    pub fn validate_for_output(&self, output_len: usize, config: &DfsReadConfig) -> Result<()> {
        config.validate()?;
        if self.layout_root_id.0.is_empty() {
            return Err(invalid("DFS read batch has no LayoutRoot"));
        }
        if self.ops.len() > config.max_ops_per_batch {
            return Err(invalid("DFS read batch contains too many ranges"));
        }
        let mut total = 0_u64;
        for op in &self.ops {
            if op.length == 0 {
                continue;
            }
            total = total
                .checked_add(op.length)
                .ok_or_else(|| invalid("DFS read batch byte count overflow"))?;
            let length = usize::try_from(op.length)
                .map_err(|_| invalid("DFS read range length is too large"))?;
            let end = op
                .output_offset
                .checked_add(length)
                .ok_or_else(|| invalid("DFS read output range overflow"))?;
            if end > output_len {
                return Err(invalid("DFS read output range exceeds buffer"));
            }
        }
        if total > config.max_inflight_bytes {
            return Err(invalid("DFS read batch exceeds inflight byte budget"));
        }
        Ok(())
    }

    fn source_request(
        &self,
        caller_id: String,
        namespace_id: crate::dfs::NamespaceId,
    ) -> Result<DfsChunkSourcesRequest> {
        let file_version_id = self
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let chunk_ids = self
            .ops
            .iter()
            .map(|op| op.chunk_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(DfsChunkSourcesRequest {
            caller_id,
            namespace_id,
            file_version_id,
            layout_root_id: self.layout_root_id.clone(),
            chunk_ids,
        })
    }
}

pub trait ReadSourceProvider: Send + Sync {
    fn sources_for(&self, request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply>;
}

/// Revalidates committed external copy identity and allowed ranges on every
/// read batch. A source-cache hit is never an authorization-cache hit.
pub trait ExternalReadAuthorizer: Send + Sync {
    fn authorize_current_external_read(
        &self,
        _request: AuthorizeCurrentExternalRead,
    ) -> Result<Option<AuthorizeCurrentExternalReadReply>> {
        Ok(None)
    }

    fn authorize_external_version_read(
        &self,
        _request: AuthorizeExternalVersionRead,
    ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
        Ok(None)
    }

    fn validate_external_read_grants(
        &self,
        caller_id: &str,
        validations: Vec<DfsReadValidation>,
    ) -> Result<Vec<DfsExternalAuthorizedRead>>;
}

/// Immutable full-object content whose length and BLAKE3 match its chunk identity.
/// Private fields prevent a reader from claiming verification of arbitrary bytes.
#[derive(Clone, Debug)]
pub struct VerifiedExternalChunk {
    chunk: ChunkObject,
    bytes: Arc<Vec<u8>>,
}

impl VerifiedExternalChunk {
    /// Verify the entire object before any requested range can become visible.
    /// Returns corrupt-data for a size/digest mismatch or an oversized object.
    pub fn verify(chunk: &ChunkObject, bytes: Vec<u8>) -> Result<Self> {
        Self::verify_with_digest(chunk, bytes, "serial", 1, 0, blake3::hash)
    }

    pub(super) fn verify_with_rayon_pool(
        chunk: &ChunkObject,
        bytes: Vec<u8>,
        pool: &rayon_core::ThreadPool,
        workers: usize,
        gate_wait_us: u64,
    ) -> Result<Self> {
        Self::verify_with_digest(
            chunk,
            bytes,
            "rayon",
            workers as u64,
            gate_wait_us,
            |bytes| {
                pool.install(|| {
                    let mut hasher = blake3::Hasher::new();
                    hasher.update_rayon(bytes);
                    hasher.finalize()
                })
            },
        )
    }

    fn verify_with_digest(
        chunk: &ChunkObject,
        bytes: Vec<u8>,
        execution: &'static str,
        workers: u64,
        gate_wait_us: u64,
        digest: impl FnOnce(&[u8]) -> blake3::Hash,
    ) -> Result<Self> {
        let span = tracing::info_span!(
            "node.dfs.external.verify",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            hash_bytes = bytes.len() as u64,
            whole_hash_bytes = bytes.len() as u64,
            hash_execution = tracing::field::Empty,
            hash_workers = tracing::field::Empty,
            gate_wait_us = tracing::field::Empty,
        );
        span.in_scope(|| {
            if chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES || bytes.len() as u64 != chunk.length {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            tracing::Span::current().record("hash_execution", execution);
            tracing::Span::current().record("hash_workers", workers);
            tracing::Span::current().record("gate_wait_us", gate_wait_us);
            if digest(&bytes).as_bytes() != &chunk.content_digest.bytes {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            Ok(())
        })?;
        Ok(Self {
            chunk: chunk.clone(),
            bytes: Arc::new(bytes),
        })
    }

    pub(super) fn load_with_rayon_pool(
        chunk: &ChunkObject,
        mut bytes: Vec<u8>,
        pool: &rayon_core::ThreadPool,
        gate_wait_us: u64,
        mut load_segment: impl FnMut(u64, &mut [u8], bool) -> Result<()> + Send,
        finalize_reader: impl FnOnce() -> Result<()>,
        mut before_segment_hash: impl FnMut(u64) + Send,
    ) -> Result<Self> {
        const READY_SEGMENT_BYTES: usize = 1024 * 1024;
        let span = tracing::info_span!(
            "node.dfs.external.verify",
            chunk_id = %chunk.id.0,
            chunk_bytes = chunk.length,
            hash_bytes = bytes.len() as u64,
            whole_hash_bytes = bytes.len() as u64,
            hash_execution = "ready_pipeline",
            hash_workers = pool.current_num_threads() as u64,
            gate_wait_us,
        );
        span.in_scope(|| {
            if chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES || bytes.len() as u64 != chunk.length {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            let total_len = bytes.len();

            let mut hasher = blake3::Hasher::new();
            let segment_count = total_len.div_ceil(READY_SEGMENT_BYTES);
            if segment_count > 0 {
                let first_len = total_len.min(READY_SEGMENT_BYTES);
                load_segment(0, &mut bytes[..first_len], segment_count == 1)?;
            }

            for segment_index in 0..segment_count {
                let segment_offset = segment_index * READY_SEGMENT_BYTES;
                let segment_end = (segment_offset + READY_SEGMENT_BYTES).min(total_len);
                let next_offset = segment_end;
                let (loaded, remaining) = bytes.split_at_mut(next_offset);
                let segment = &loaded[segment_offset..segment_end];
                let next_len = remaining.len().min(READY_SEGMENT_BYTES);
                let pipeline_span = tracing::info_span!(
                    "node.dfs.s3.cache.load.pipeline_scope",
                    segment_offset = segment_offset as u64,
                    segment_bytes = segment.len() as u64,
                    next_segment_offset = tracing::field::Empty,
                    next_segment_bytes = next_len as u64,
                );
                if next_len != 0 {
                    pipeline_span.record("next_segment_offset", next_offset as u64);
                }
                let pipeline_parent = pipeline_span.clone();
                pipeline_span.in_scope(|| {
                    pool.in_place_scope(|scope| {
                        let hash_parent = pipeline_parent.clone();
                        let hasher = &mut hasher;
                        let before_segment_hash = &mut before_segment_hash;
                        scope.spawn(move |_| {
                            let hash_span = tracing::info_span!(
                                parent: &hash_parent,
                                "node.dfs.s3.cache.load.segment_hash",
                                chunk_id = %chunk.id.0,
                                segment_offset = segment_offset as u64,
                                segment_bytes = segment.len() as u64,
                                hash_bytes = segment.len() as u64,
                            );
                            hash_span.in_scope(|| {
                                before_segment_hash(segment_offset as u64);
                                hasher.update_rayon(segment);
                            });
                        });
                        if next_len == 0 {
                            Ok(())
                        } else {
                            let tail = next_offset + next_len == total_len;
                            load_segment(next_offset as u64, &mut remaining[..next_len], tail)
                        }
                    })
                })?;
            }

            tracing::info_span!("node.dfs.s3.cache.load.finalize_reader")
                .in_scope(finalize_reader)?;
            let digest_matches = tracing::info_span!(
                "node.dfs.s3.cache.load.hash_finalize",
                hash_bytes = total_len as u64,
            )
            .in_scope(|| hasher.finalize().as_bytes() == &chunk.content_digest.bytes);
            if !digest_matches {
                return Err(corrupt("external Chunk length or content digest mismatch"));
            }
            Ok(Self {
                chunk: chunk.clone(),
                bytes: Arc::new(bytes),
            })
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn matches(&self, chunk: &ChunkObject) -> bool {
        self.chunk == *chunk
    }

    /// Strip verified identity only when no reader or fill still owns the payload.
    /// The caller must retain capacity accounting and fully reverify before reuse.
    pub(super) fn try_into_bytes(self) -> std::result::Result<Vec<u8>, Self> {
        match Arc::try_unwrap(self.bytes) {
            Ok(bytes) => Ok(bytes),
            Err(bytes) => Err(Self {
                chunk: self.chunk,
                bytes,
            }),
        }
    }

    pub(super) fn allocation_weak(&self) -> Weak<Vec<u8>> {
        Arc::downgrade(&self.bytes)
    }

    pub(super) fn allocation_bytes(&self) -> u64 {
        self.bytes.capacity() as u64
    }

    pub(super) fn same_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.bytes, &other.bytes)
    }
}

/// Fetch one complete immutable external chunk. Implementations must bound
/// response allocation and reject another store, key or generation. Content
/// crosses this boundary only through the validating immutable buffer above;
/// the engine still checks its full identity and authorizes every read batch.
pub trait ExternalChunkReader: Send + Sync {
    fn read_chunk(
        &self,
        chunk: &ChunkObject,
        source: &SourceCandidate,
    ) -> Result<VerifiedExternalChunk>;
}

struct ExternalReadBackend {
    reader: Arc<dyn ExternalChunkReader>,
    authorizer: Arc<dyn ExternalReadAuthorizer>,
}

/// Every operation keeps its own physical copy. A batch shares only the peer
/// connection and the same version and authenticated caller context; each copy retains its grant.
#[derive(Clone, Debug)]
pub struct ResolvedReadOp {
    pub op: ChunkReadOp,
    pub source: SourceCandidate,
}

#[derive(Clone, Debug)]
pub struct PeerReadBatch {
    pub file_version_id: FileVersionId,
    pub layout_root_id: LayoutRootId,
    pub operations: Vec<ResolvedReadOp>,
}

impl PeerReadBatch {
    pub fn validate(&self, output_len: usize) -> Result<()> {
        let first = self
            .operations
            .first()
            .ok_or_else(|| invalid("empty peer read batch"))?;
        ReadBatch {
            file_version_id: Some(self.file_version_id.clone()),
            layout_root_id: self.layout_root_id.clone(),
            ops: self.operations.iter().map(|item| item.op.clone()).collect(),
        }
        .validate_for_output(output_len, &DfsReadConfig::default())?;
        for item in &self.operations {
            let grant = &item.source.read_grant;
            if item.op.length == 0
                || item
                    .source
                    .data_endpoint
                    .as_deref()
                    .is_none_or(str::is_empty)
                || item.source.chunk_id != item.op.chunk_id
                || grant.file_version_id != self.file_version_id
                || grant.layout_root_id != self.layout_root_id
                || grant.expires_at_unix_ms <= now_unix_ms()
                || !same_peer_context(&first.source, &item.source)
            {
                return Err(invalid(
                    "peer read source does not match the original batch identity",
                ));
            }
        }
        Ok(())
    }
}

pub trait ChunkTransfer: Send + Sync {
    /// `out` belongs to this attempt; the scheduler publishes it only after
    /// every operation completes. An adapter must drain or retain any DMA
    /// resources before returning, including on cancellation or timeout.
    fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()>;
}

fn same_peer_context(left: &SourceCandidate, right: &SourceCandidate) -> bool {
    let same_node = matches!(
        (&left.location, &right.location),
        (CopyLocation::Node { node_id: a, node_epoch: ae, .. },
         CopyLocation::Node { node_id: b, node_epoch: be, .. }) if a == b && ae == be
    );
    let a = &left.read_grant;
    let b = &right.read_grant;
    same_node
        && left.data_endpoint == right.data_endpoint
        && a.namespace_id == b.namespace_id
        && a.file_version_id == b.file_version_id
        && a.layout_root_id == b.layout_root_id
        && a.caller_node_id == b.caller_node_id
        && a.caller_node_epoch == b.caller_node_epoch
}

fn range_covered(ranges: &[(u64, u64)], requested_start: u64, requested_len: u64) -> bool {
    ranges.iter().any(|(start, length)| {
        requested_start >= *start
            && start.checked_add(*length).is_some_and(|end| {
                requested_start
                    .checked_add(requested_len)
                    .is_some_and(|requested_end| requested_end <= end)
            })
    })
}

fn external_authority_entry_bytes(
    key: &ExternalAuthorityKey,
    chunk: &ChunkObject,
    grant: &DfsReadGrant,
    location: &CopyLocation,
    allowed_ranges: &[(u64, u64)],
) -> usize {
    let mut bytes = std::mem::size_of::<ExternalAuthorityEntry>()
        + key.namespace_id.0.len()
        + key.inode_id.0.len()
        + key.file_version_id.0.len()
        + key.layout_root_id.0.len()
        + key.chunk_id.0.len()
        + chunk.id.0.len()
        + grant.namespace_id.0.len()
        + grant.file_version_id.0.len()
        + grant.layout_root_id.0.len()
        + grant.caller_node_id.len()
        + grant.token.len()
        + std::mem::size_of_val(allowed_ranges);
    match location {
        CopyLocation::External {
            store_id,
            object_key,
            object_revision,
        } => {
            bytes = bytes
                .saturating_add(store_id.len())
                .saturating_add(object_key.len())
                .saturating_add(object_revision.len());
        }
        CopyLocation::Node {
            node_id, device_id, ..
        } => {
            bytes = bytes
                .saturating_add(node_id.len())
                .saturating_add(device_id.len());
        }
    }
    bytes
}

type SourceCacheKey = (FileVersionId, LayoutRootId, ChunkId);
const MAX_CACHED_SOURCES: usize = 1024;
const MAX_PINNED_READERS: usize = 256;

#[derive(Clone, Debug)]
struct LocalReadAuthoritySnapshot {
    epoch: u64,
    expires_at: Option<Instant>,
}

#[derive(Clone, Debug)]
struct LocalReadAuthorityState {
    epoch: u64,
    expires_at: Option<Instant>,
    stopped: bool,
}

/// Shared local reader authority state supplied by the Node lifecycle.
///
/// Product wiring should refresh it from registration/heartbeat and stop it
/// before FUSE output can be committed. Unit-only engines use an unbounded
/// unknown-epoch authority so legacy tests do not accidentally depend on Node
/// lifecycle plumbing.
pub(super) struct LocalReadAuthority {
    state: Mutex<LocalReadAuthorityState>,
}

impl LocalReadAuthority {
    pub(super) fn new(epoch: u64, expires_at: Instant) -> Self {
        Self {
            state: Mutex::new(LocalReadAuthorityState {
                epoch,
                expires_at: Some(expires_at),
                stopped: false,
            }),
        }
    }

    fn unbounded_for_tests_and_legacy() -> Self {
        Self {
            state: Mutex::new(LocalReadAuthorityState {
                epoch: 0,
                expires_at: None,
                stopped: false,
            }),
        }
    }

    pub(super) fn refresh(&self, epoch: u64, expires_at: Instant) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if state.stopped {
            return Err(external_denied("DFS read authority has stopped"));
        }
        if expires_at <= Instant::now() {
            return Err(external_denied("DFS read authority expired"));
        }
        if state.epoch != epoch {
            state.stopped = true;
            return Err(external_denied("DFS read authority epoch changed"));
        }
        state.expires_at = Some(expires_at);
        Ok(())
    }

    pub(super) fn stop(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.stopped = true;
        }
    }

    fn snapshot(&self) -> Result<LocalReadAuthoritySnapshot> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if state.stopped {
            return Err(external_denied("DFS read authority has stopped"));
        }
        if state
            .expires_at
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Err(external_denied("DFS read authority expired"));
        }
        Ok(LocalReadAuthoritySnapshot {
            epoch: state.epoch,
            expires_at: state.expires_at,
        })
    }
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct ExternalAuthorityKey {
    namespace_id: crate::dfs::NamespaceId,
    inode_id: crate::dfs::InodeId,
    file_version_id: FileVersionId,
    layout_root_id: LayoutRootId,
    chunk_id: ChunkId,
}

struct ExternalAuthorityEntry {
    key: ExternalAuthorityKey,
    chunk: ChunkObject,
    copy_id: crate::dfs::CopyId,
    location: CopyLocation,
    grant: DfsReadGrant,
    allowed_ranges: Vec<(u64, u64)>,
    expires_at_unix_ms: u64,
    deadline: Instant,
    node_epoch: u64,
    bytes: usize,
    invalidated: AtomicBool,
    _reservation: Option<Arc<ExternalAuthorityReservation>>,
}

struct ExternalAuthorityReservation {
    bytes: usize,
    budget: Weak<Mutex<ExternalAuthorityBudget>>,
}

impl Drop for ExternalAuthorityReservation {
    fn drop(&mut self) {
        if let Some(budget) = self.budget.upgrade()
            && let Ok(mut budget) = budget.lock()
        {
            budget.bytes = budget.bytes.saturating_sub(self.bytes);
            budget.entries = budget.entries.saturating_sub(1);
        }
    }
}

#[derive(Default)]
struct ExternalAuthorityBudget {
    bytes: usize,
    entries: usize,
}

#[derive(Default)]
struct ExternalAuthorityCache {
    // Local rejection serializes in-flight output, including a fresh context
    // that has not yet been retained. It does not stop the Node/session.
    invalidation_generation: u64,
    entries: HashMap<ExternalAuthorityKey, Arc<ExternalAuthorityEntry>>,
    budget: Arc<Mutex<ExternalAuthorityBudget>>,
}

impl ExternalAuthorityCache {
    fn lookup(
        &mut self,
        key: &ExternalAuthorityKey,
        requested_start: u64,
        requested_len: u64,
        local: &LocalReadAuthoritySnapshot,
    ) -> Option<Arc<ExternalAuthorityEntry>> {
        let now = Instant::now();
        self.entries
            .retain(|_, entry| entry.deadline > now || Arc::strong_count(entry) > 1);
        let entry = self.entries.get(key)?;
        if entry.invalidated.load(Ordering::Acquire)
            || entry.deadline <= now
            || (local.epoch != 0 && entry.node_epoch != local.epoch)
            || local.expires_at.is_some_and(|deadline| deadline <= now)
            || !range_covered(&entry.allowed_ranges, requested_start, requested_len)
        {
            return None;
        }
        Some(entry.clone())
    }

    fn insert(&mut self, mut entry: ExternalAuthorityEntry) -> Option<Arc<ExternalAuthorityEntry>> {
        // Never replace an in-flight context with a new one under the same key.
        // Expired in-flight entries remain reachable for known invalidation.
        if self.entries.contains_key(&entry.key) {
            return None;
        }
        if entry.bytes > MAX_EXTERNAL_AUTH_CACHE_BYTES {
            return None;
        }
        let full = self
            .budget
            .lock()
            .map(|budget| {
                budget.entries >= MAX_EXTERNAL_AUTH_CACHE_ENTRIES
                    || budget.bytes.saturating_add(entry.bytes) > MAX_EXTERNAL_AUTH_CACHE_BYTES
            })
            .unwrap_or(true);
        if full {
            self.entries.retain(|_, value| Arc::strong_count(value) > 1);
        }
        let mut budget = self.budget.lock().ok()?;
        if budget.entries >= MAX_EXTERNAL_AUTH_CACHE_ENTRIES
            || budget.bytes.saturating_add(entry.bytes) > MAX_EXTERNAL_AUTH_CACHE_BYTES
        {
            return None;
        }
        budget.bytes += entry.bytes;
        budget.entries += 1;
        entry._reservation = Some(Arc::new(ExternalAuthorityReservation {
            bytes: entry.bytes,
            budget: Arc::downgrade(&self.budget),
        }));
        drop(budget);
        let entry = Arc::new(entry);
        self.entries.insert(entry.key.clone(), entry.clone());
        Some(entry)
    }

    fn invalidate_inode_version(
        &mut self,
        namespace_id: &crate::dfs::NamespaceId,
        inode_id: &crate::dfs::InodeId,
        file_version_id: &FileVersionId,
    ) {
        self.invalidation_generation = self.invalidation_generation.wrapping_add(1);
        self.entries.retain(|key, entry| {
            let keep = &key.namespace_id != namespace_id
                || &key.inode_id != inode_id
                || &key.file_version_id != file_version_id;
            if !keep {
                entry.invalidated.store(true, Ordering::Release);
            }
            keep
        });
    }
}

pub struct DfsReadEngine {
    namespace_id: crate::dfs::NamespaceId,
    local_node_id: String,
    local: Arc<LocalChunkStore>,
    source_provider: Arc<dyn ReadSourceProvider>,
    transfer: Arc<dyn ChunkTransfer>,
    config: DfsReadConfig,
    external_backend: Option<ExternalReadBackend>,
    read_authority: Arc<LocalReadAuthority>,
    readers: Mutex<HashMap<ChunkId, Arc<PinnedChunkReader>>>,
    source_cache: Mutex<HashMap<SourceCacheKey, CachedSources>>,
    external_authority_cache: Mutex<ExternalAuthorityCache>,
}

#[derive(Clone)]
struct CachedSources {
    expires_at: Instant,
    sources: ChunkSources,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentExternalReadOutcome {
    Read,
    Unsupported,
    HeadChanged(crate::dfs::InodeRecord),
}

struct ExternalReadSelection {
    batch: ReadBatch,
    selected: Vec<ExternalReadSelected>,
    validations: Vec<DfsReadValidation>,
    missing_sources: Vec<DfsMissingReadSource>,
}

struct ExternalReadSelected {
    chunk_id: ChunkId,
    chunk_offset: u64,
    length: u64,
    output_offset: usize,
    chunk: Option<ChunkObject>,
    source: Option<SourceCandidate>,
}

struct AuthorizedExternalRead {
    chunk: ChunkObject,
    source: SourceCandidate,
    op: ChunkReadOp,
}

impl ExternalReadSelection {
    fn unique_chunk_count(&self) -> usize {
        self.selected
            .iter()
            .map(|entry| entry.chunk_id.clone())
            .collect::<BTreeSet<_>>()
            .len()
    }
}

impl DfsReadEngine {
    pub fn new(
        namespace_id: crate::dfs::NamespaceId,
        local_node_id: String,
        local: Arc<LocalChunkStore>,
        source_provider: Arc<dyn ReadSourceProvider>,
        transfer: Arc<dyn ChunkTransfer>,
        config: DfsReadConfig,
    ) -> Self {
        Self {
            namespace_id,
            local_node_id,
            local,
            source_provider,
            transfer,
            config,
            external_backend: None,
            read_authority: Arc::new(LocalReadAuthority::unbounded_for_tests_and_legacy()),
            readers: Mutex::new(HashMap::new()),
            source_cache: Mutex::new(HashMap::new()),
            external_authority_cache: Mutex::new(ExternalAuthorityCache::default()),
        }
    }

    /// Select external-backend-only reads. Healthy local chunks and peer
    /// candidates are deliberately bypassed; errors never fall back to them.
    pub fn with_external_backend(
        mut self,
        reader: Arc<dyn ExternalChunkReader>,
        authorizer: Arc<dyn ExternalReadAuthorizer>,
    ) -> Self {
        self.external_backend = Some(ExternalReadBackend { reader, authorizer });
        self
    }

    pub(super) fn with_read_authority(mut self, authority: Arc<LocalReadAuthority>) -> Self {
        self.read_authority = authority;
        self
    }

    pub fn has_external_backend(&self) -> bool {
        self.external_backend.is_some()
    }

    pub fn read_batch(&self, batch: &ReadBatch, out: &mut [u8]) -> Result<()> {
        let requested_bytes = batch
            .ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        let span = tracing::info_span!(
            "node.dfs.read.batch",
            ops = batch.ops.len(),
            requested_bytes,
            output_bytes = out.len() as u64,
            external = self.external_backend.is_some(),
        );
        let _entered = span.enter();
        batch.validate_for_output(out.len(), &self.config)?;
        // Stage only requested bytes, bounded by the validated batch budget.
        // Local successes and peer retries become visible together after every
        // range succeeds; a failed read never alters the caller's output.
        let mut staged_batch = batch.clone();
        let mut length = 0;
        for op in &mut staged_batch.ops {
            op.output_offset = length;
            length += op.length as usize;
        }
        let mut staged = vec![0; length];
        let expires_at = if let Some(backend) = &self.external_backend {
            self.read_external_batch(backend, &staged_batch, &mut staged)?
        } else {
            self.read_batch_into(&staged_batch, &mut staged)?;
            None
        };
        // Full-object fetches may outlive the authorization that allowed them.
        // Check again at the publication boundary, including cached sources.
        if expires_at.is_some_and(|deadline| deadline <= now_unix_ms()) {
            return Err(external_denied(
                "external read authority expired before publication",
            ));
        }
        for (original, packed) in batch.ops.iter().zip(&staged_batch.ops) {
            if original.length == 0 {
                continue;
            }
            let length = original.length as usize;
            tracing::info_span!(
                "node.dfs.read.copy",
                copy_stage = "staged_to_output",
                requested_bytes = original.length,
                copied_bytes = length as u64,
            )
            .in_scope(|| {
                out[original.output_offset..original.output_offset + length]
                    .copy_from_slice(&staged[packed.output_offset..packed.output_offset + length]);
            });
        }
        Ok(())
    }

    pub fn read_batch_current_external(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<CurrentExternalReadOutcome> {
        let Some(backend) = &self.external_backend else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        let requested_bytes = batch
            .ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        let span = tracing::info_span!(
            "node.dfs.read.current_external",
            inode_id = %inode_id.0,
            ops = batch.ops.len(),
            requested_bytes,
            output_bytes = out.len() as u64,
            selected_chunks = tracing::field::Empty,
            authorized_ranges = tracing::field::Empty,
            fetched_chunks = tracing::field::Empty,
            copied_bytes = tracing::field::Empty,
        );
        let _entered = span.enter();
        batch.validate_for_output(out.len(), &self.config)?;
        let mut staged_batch = batch.clone();
        let mut length = 0;
        for op in &mut staged_batch.ops {
            op.output_offset = length;
            length += op.length as usize;
        }
        let mut staged = vec![0; length];
        if staged_batch.file_version_id.is_none() {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        }
        let Some(selection) = self.select_current_external_reads(&staged_batch)? else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        let Some(expected_head_version) = selection.batch.file_version_id.clone() else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        span.record("selected_chunks", selection.unique_chunk_count() as u64);
        let reply = tracing::info_span!(
            "node.dfs.read.current_auth_rpc",
            validations = selection.validations.len(),
            missing_sources = selection.missing_sources.len(),
        )
        .in_scope(|| {
            backend
                .authorizer
                .authorize_current_external_read(AuthorizeCurrentExternalRead {
                    caller_id: self.local_node_id.clone(),
                    inode_id: inode_id.clone(),
                    namespace_id: self.namespace_id.clone(),
                    expected_head_version: expected_head_version.clone(),
                    layout_root_id: selection.batch.layout_root_id.clone(),
                    validations: selection.validations.clone(),
                    missing_sources: selection.missing_sources.clone(),
                })
        })?;
        let Some(reply) = reply else {
            return Ok(CurrentExternalReadOutcome::Unsupported);
        };
        let expires_at = match reply {
            AuthorizeCurrentExternalReadReply::Authorized {
                inode,
                authorizations,
            } => {
                self.validate_current_authorized_inode(&inode, inode_id, &expected_head_version)?;
                span.record("authorized_ranges", authorizations.len() as u64);
                self.read_external_authorized_selection(
                    backend,
                    &selection,
                    authorizations,
                    &mut staged,
                    None,
                )?
            }
            AuthorizeCurrentExternalReadReply::HeadChanged(inode) => {
                self.validate_current_head_changed_inode(&inode, inode_id, &expected_head_version)?;
                return Ok(CurrentExternalReadOutcome::HeadChanged(inode));
            }
        };
        if expires_at.is_some_and(|deadline| deadline <= now_unix_ms()) {
            return Err(external_denied(
                "external read authority expired before publication",
            ));
        }
        for (original, packed) in batch.ops.iter().zip(&staged_batch.ops) {
            if original.length == 0 {
                continue;
            }
            let length = original.length as usize;
            tracing::info_span!(
                "node.dfs.read.copy",
                copy_stage = "staged_to_output",
                requested_bytes = original.length,
                copied_bytes = length as u64,
            )
            .in_scope(|| {
                out[original.output_offset..original.output_offset + length]
                    .copy_from_slice(&staged[packed.output_offset..packed.output_offset + length]);
            });
        }
        Ok(CurrentExternalReadOutcome::Read)
    }

    /// Read one immutable version using bounded, authenticated object contexts.
    /// Missing or expired contexts require Meta; transport/authorization errors
    /// propagate without falling back to old tokens or a different version.
    pub fn read_batch_external_version(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<CurrentExternalReadOutcome> {
        batch.validate_for_output(out.len(), &self.config)?;
        let mut packed = batch.clone();
        let mut size = 0;
        for op in &mut packed.ops {
            op.output_offset = size;
            size += op.length as usize;
        }
        let published = self.with_external_version_result(inode_id, &packed, size, |staged| {
            for (original, op) in batch.ops.iter().zip(&packed.ops) {
                let length = original.length as usize;
                if length == 0 {
                    continue;
                }
                tracing::info_span!(
                    "node.dfs.read.copy",
                    copy_stage = "staged_to_output",
                    requested_bytes = original.length,
                    copied_bytes = length as u64,
                )
                .in_scope(|| {
                    out[original.output_offset..original.output_offset + length]
                        .copy_from_slice(&staged[op.output_offset..op.output_offset + length]);
                });
            }
        })?;
        Ok(if published.is_some() {
            CurrentExternalReadOutcome::Read
        } else {
            CurrentExternalReadOutcome::Unsupported
        })
    }

    /// Return the private final response after the same final authority checks
    /// as borrowed reads. Holes are zeroed; unsupported authorizers return None.
    /// Reject response allocations beyond the existing read budget before I/O.
    pub fn read_batch_external_version_owned(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        output_len: usize,
    ) -> Result<Option<Vec<u8>>> {
        if output_len as u64 > self.config.max_inflight_bytes {
            return Err(invalid("DFS owned read exceeds inflight byte budget"));
        }
        batch.validate_for_output(output_len, &self.config)?;
        // Object-grouped fetches may reorder overlapping writes. Keep the
        // borrowed packed/scatter path for those uncommon layouts.
        for (index, op) in batch
            .ops
            .iter()
            .enumerate()
            .filter(|(_, op)| op.length != 0)
        {
            let end = op.output_offset + op.length as usize;
            if batch.ops[..index].iter().any(|previous| {
                previous.length != 0
                    && previous.output_offset < end
                    && op.output_offset < previous.output_offset + previous.length as usize
            }) {
                return Ok(None);
            }
        }
        self.with_external_version_result(inode_id, batch, output_len, |bytes| bytes)
    }

    fn with_external_version_result<T>(
        &self,
        inode_id: &crate::dfs::InodeId,
        batch: &ReadBatch,
        output_len: usize,
        publish: impl FnOnce(Vec<u8>) -> T,
    ) -> Result<Option<T>> {
        let Some(backend) = &self.external_backend else {
            return Ok(None);
        };
        batch.validate_for_output(output_len, &self.config)?;
        let local = self.read_authority.snapshot()?;
        let version = batch
            .file_version_id
            .as_ref()
            .ok_or_else(|| invalid("external read requires a version"))?;
        let span = tracing::info_span!(
            "node.dfs.read.external_version",
            ops = batch.ops.len(),
            auth_hits = tracing::field::Empty,
            auth_misses = tracing::field::Empty
        );
        let _entered = span.enter();
        let control = tracing::info_span!("node.dfs.read.authority_control").entered();
        let mut packed = batch.clone();
        packed.ops.retain(|op| op.length != 0);
        let selection = ExternalReadSelection {
            batch: packed.clone(),
            selected: packed
                .ops
                .iter()
                .map(|op| ExternalReadSelected {
                    chunk_id: op.chunk_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                    output_offset: op.output_offset,
                    chunk: None,
                    source: None,
                })
                .collect(),
            validations: Vec::new(),
            missing_sources: Vec::new(),
        };
        let mut permits = Vec::with_capacity(packed.ops.len());
        let mut authorizations = Vec::with_capacity(packed.ops.len());
        let mut misses = Vec::new();
        let invalidation_generation;
        {
            let mut cache = self
                .external_authority_cache
                .lock()
                .map_err(|_| unavailable("DFS external authority cache is poisoned"))?;
            invalidation_generation = cache.invalidation_generation;
            for op in &packed.ops {
                let key = self.external_authority_key(inode_id, &packed, &op.chunk_id)?;
                let permit = cache.lookup(&key, op.chunk_offset, op.length, &local);
                authorizations.push(
                    permit
                        .as_ref()
                        .map(|entry| Self::authorization_from_cache(entry, op)),
                );
                if permit.is_none() {
                    misses.push(DfsMissingReadSource {
                        chunk_id: op.chunk_id.clone(),
                        chunk_offset: op.chunk_offset,
                        length: op.length,
                    });
                }
                permits.push(permit);
            }
        }
        span.record("auth_hits", (packed.ops.len() - misses.len()) as u64);
        span.record("auth_misses", misses.len() as u64);
        let rpc_started = Instant::now();
        let mut new_deadline = None;
        if !misses.is_empty() {
            let count = misses.len();
            let reply = tracing::info_span!("node.dfs.read.version_auth_rpc", requests = count)
                .in_scope(|| {
                    backend.authorizer.authorize_external_version_read(
                        AuthorizeExternalVersionRead {
                            caller_id: self.local_node_id.clone(),
                            inode_id: inode_id.clone(),
                            namespace_id: self.namespace_id.clone(),
                            file_version_id: version.clone(),
                            layout_root_id: packed.layout_root_id.clone(),
                            requests: misses,
                        },
                    )
                });
            let reply = match reply {
                Ok(Some(reply)) => reply,
                Ok(None) => return Ok(None),
                Err(error) => {
                    if matches!(
                        error.kind(),
                        afs_error::ErrorKind::PermissionDenied
                            | afs_error::ErrorKind::Unauthenticated
                    ) {
                        self.invalidate_external_authority(inode_id, version)?;
                    }
                    return Err(error);
                }
            };
            if reply.authorizations.len() != count
                || reply.remaining_validity_ms == 0
                || reply.remaining_validity_ms > 5_000
            {
                self.invalidate_external_authority(inode_id, version)?;
                return Err(external_denied(
                    "external version authority reply is incomplete or expired",
                ));
            }
            // SEND-time anchoring already subtracts the complete RPC duration.
            // Do not grant a fresh five seconds to a delayed response.
            let deadline = rpc_started + Duration::from_millis(reply.remaining_validity_ms);
            if deadline <= Instant::now() {
                return Err(external_denied(
                    "external version authority expired during RPC",
                ));
            }
            new_deadline = Some(deadline);
            let mut fresh = reply.authorizations.into_iter();
            for authorization in &mut authorizations {
                if authorization.is_none() {
                    *authorization = fresh.next();
                }
            }
        }
        let mut deadline = new_deadline;
        if let Some(session_deadline) = local.expires_at {
            deadline = Some(deadline.map_or(session_deadline, |value| value.min(session_deadline)));
        }
        for permit in permits.iter().flatten() {
            deadline = Some(deadline.map_or(permit.deadline, |value| value.min(permit.deadline)));
        }
        let authorized = authorizations
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| external_denied("external version authority reply omitted a range"))?;
        if local.epoch != 0
            && authorized
                .iter()
                .any(|entry| entry.authorized.validation.grant.caller_node_epoch != local.epoch)
        {
            self.invalidate_external_authority(inode_id, version)?;
            return Err(external_denied(
                "external version authority has a different Node epoch",
            ));
        }
        drop(control);
        let mut staged = vec![0; output_len];
        // Existing batch checks validate every object/range before any GET and
        // retain complete BLAKE3 and caller-output atomicity.
        let expires = self.read_external_authorized_selection(
            backend,
            &selection,
            authorized.clone(),
            &mut staged,
            deadline,
        )?;
        if expires.is_some_and(|value| value <= now_unix_ms())
            || deadline.is_some_and(|value| value <= Instant::now())
        {
            return Err(external_denied(
                "external version authority expired before publication",
            ));
        }
        let mut cache = self
            .external_authority_cache
            .lock()
            .map_err(|_| unavailable("DFS external authority cache is poisoned"))?;
        let state = self
            .read_authority
            .state
            .lock()
            .map_err(|_| unavailable("DFS read authority state is poisoned"))?;
        if cache.invalidation_generation != invalidation_generation
            || state.stopped
            || state.epoch != local.epoch
            || state
                .expires_at
                .is_some_and(|value| value <= Instant::now())
            || local
                .expires_at
                .is_some_and(|value| value <= Instant::now())
            || permits.iter().flatten().any(|entry| {
                entry.invalidated.load(Ordering::Acquire) || entry.deadline <= Instant::now()
            })
        {
            return Err(external_denied(
                "external version authority invalidated before publication",
            ));
        }
        // Cache only freshly admitted contexts. Hits never renew deadlines.
        for (index, authorization) in authorized.iter().enumerate() {
            if permits[index].is_none()
                && let Some(fresh_deadline) = new_deadline
            {
                let key =
                    self.external_authority_key(inode_id, &packed, &packed.ops[index].chunk_id)?;
                let mut cutoff = fresh_deadline;
                if let Some(session_deadline) = local.expires_at {
                    cutoff = cutoff.min(session_deadline);
                }
                let wall_remaining = authorization
                    .authorized
                    .expires_at_unix_ms
                    .saturating_sub(now_unix_ms());
                cutoff = cutoff.min(Instant::now() + Duration::from_millis(wall_remaining));
                let mut bytes = external_authority_entry_bytes(
                    &key,
                    &authorization.chunk,
                    &authorization.authorized.validation.grant,
                    &authorization.location,
                    &authorization.authorized.allowed_ranges,
                );
                // Conservatively reserve the key copy, Arc/map overhead, copy ID
                // and bounded projections held by the current batch as well.
                bytes = bytes
                    .saturating_add(authorization.authorized.validation.copy_id.0.capacity())
                    .saturating_mul(4)
                    .saturating_add(256);
                permits[index] = cache.insert(ExternalAuthorityEntry {
                    key,
                    chunk: authorization.chunk.clone(),
                    copy_id: authorization.authorized.validation.copy_id.clone(),
                    location: authorization.location.clone(),
                    grant: authorization.authorized.validation.grant.clone(),
                    allowed_ranges: authorization.authorized.allowed_ranges.clone(),
                    expires_at_unix_ms: authorization.authorized.expires_at_unix_ms,
                    deadline: cutoff,
                    node_epoch: authorization.authorized.validation.grant.caller_node_epoch,
                    bytes,
                    invalidated: AtomicBool::new(false),
                    _reservation: None,
                });
            }
        }
        // Invalidation takes the same locks: transferring this private result
        // or copying into a borrowed output is the publication boundary.
        Ok(Some(publish(staged)))
    }

    fn external_authority_key(
        &self,
        inode: &crate::dfs::InodeId,
        batch: &ReadBatch,
        chunk: &ChunkId,
    ) -> Result<ExternalAuthorityKey> {
        Ok(ExternalAuthorityKey {
            namespace_id: self.namespace_id.clone(),
            inode_id: inode.clone(),
            file_version_id: batch
                .file_version_id
                .clone()
                .ok_or_else(|| invalid("external read requires a version"))?,
            layout_root_id: batch.layout_root_id.clone(),
            chunk_id: chunk.clone(),
        })
    }

    fn authorization_from_cache(
        entry: &ExternalAuthorityEntry,
        op: &ChunkReadOp,
    ) -> DfsExternalAuthorizedRead {
        DfsExternalAuthorizedRead {
            authorized: crate::dfs::DfsAuthorizedRead {
                validation: DfsReadValidation {
                    grant: entry.grant.clone(),
                    chunk_id: entry.chunk.id.clone(),
                    copy_id: entry.copy_id.clone(),
                    chunk_offset: op.chunk_offset,
                    length: op.length,
                },
                allowed_ranges: entry.allowed_ranges.clone(),
                expires_at_unix_ms: entry.expires_at_unix_ms,
            },
            chunk: entry.chunk.clone(),
            location: entry.location.clone(),
        }
    }

    fn invalidate_external_authority(
        &self,
        inode: &crate::dfs::InodeId,
        version: &FileVersionId,
    ) -> Result<()> {
        self.external_authority_cache
            .lock()
            .map_err(|_| unavailable("DFS external authority cache is poisoned"))?
            .invalidate_inode_version(&self.namespace_id, inode, version);
        Ok(())
    }

    fn read_external_batch(
        &self,
        backend: &ExternalReadBackend,
        batch: &ReadBatch,
        out: &mut [u8],
    ) -> Result<Option<u64>> {
        let input_ops = batch.ops.len();
        let input_bytes = batch
            .ops
            .iter()
            .fold(0_u64, |sum, op| sum.saturating_add(op.length));
        let span = tracing::info_span!(
            "node.dfs.read.external",
            ops = input_ops,
            requested_bytes = input_bytes,
            selected_chunks = tracing::field::Empty,
            authorized_ranges = tracing::field::Empty,
            fetched_chunks = tracing::field::Empty,
            copied_bytes = tracing::field::Empty,
        );
        let _entered = span.enter();
        let Some(selection) = self.select_external_reads(batch)? else {
            // Holes and EOF do not require fetching or authorizing a chunk.
            return Ok(None);
        };
        span.record("selected_chunks", selection.unique_chunk_count() as u64);
        let authorized = tracing::info_span!(
            "node.dfs.read.auth_rpc",
            validations = selection.validations.len(),
        )
        .in_scope(|| {
            backend
                .authorizer
                .validate_external_read_grants(&self.local_node_id, selection.validations.clone())
        })?;
        span.record("authorized_ranges", authorized.len() as u64);
        self.read_external_authorized_selection(backend, &selection, authorized, out, None)
    }

    fn select_external_reads(&self, batch: &ReadBatch) -> Result<Option<ExternalReadSelection>> {
        let batch = ReadBatch {
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: batch
                .ops
                .iter()
                .filter(|op| op.length != 0)
                .cloned()
                .collect(),
        };
        if batch.ops.is_empty() {
            return Ok(None);
        }
        let sources = self.sources_for(&batch)?;
        self.select_external_reads_from_sources(batch, &sources, false)
    }

    fn select_current_external_reads(
        &self,
        batch: &ReadBatch,
    ) -> Result<Option<ExternalReadSelection>> {
        let batch = ReadBatch {
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: batch
                .ops
                .iter()
                .filter(|op| op.length != 0)
                .cloned()
                .collect(),
        };
        if batch.ops.is_empty() || batch.file_version_id.is_none() {
            return Ok(None);
        }
        let (sources, missing) = self.cached_sources_for(&batch)?;
        let mut selection = self
            .select_external_reads_from_sources(batch.clone(), &sources, true)?
            .unwrap_or(ExternalReadSelection {
                batch,
                selected: Vec::new(),
                validations: Vec::new(),
                missing_sources: Vec::new(),
            });
        for op in missing {
            selection.missing_sources.push(DfsMissingReadSource {
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
            });
            selection.selected.push(ExternalReadSelected {
                chunk_id: op.chunk_id,
                chunk_offset: op.chunk_offset,
                length: op.length,
                output_offset: op.output_offset,
                chunk: None,
                source: None,
            });
        }
        if selection.selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(selection))
    }

    fn select_external_reads_from_sources(
        &self,
        batch: ReadBatch,
        sources: &[ChunkSources],
        allow_missing: bool,
    ) -> Result<Option<ExternalReadSelection>> {
        let mut selected = Vec::with_capacity(batch.ops.len());
        let mut validations = Vec::with_capacity(batch.ops.len());
        for op in &batch.ops {
            let Some(set) = sources.iter().find(|set| set.chunk_id == op.chunk_id) else {
                if allow_missing {
                    continue;
                }
                return Err(unavailable(
                    "external source lookup omitted a requested Chunk",
                ));
            };
            let (chunk, source, validation) = self.select_external_read_source(set, op)?;
            validations.push(validation);
            selected.push(ExternalReadSelected {
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                output_offset: op.output_offset,
                chunk: Some(chunk),
                source: Some(source),
            });
        }
        if selected.is_empty() {
            return Ok(None);
        }
        Ok(Some(ExternalReadSelection {
            batch,
            selected,
            validations,
            missing_sources: Vec::new(),
        }))
    }

    fn select_external_read_source(
        &self,
        set: &ChunkSources,
        op: &ChunkReadOp,
    ) -> Result<(ChunkObject, SourceCandidate, DfsReadValidation)> {
        let Some(chunk) = set.chunk.as_ref() else {
            return Err(invalid("external source has no trusted Chunk identity"));
        };
        if chunk.id != op.chunk_id
            || chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES
            || op
                .chunk_offset
                .checked_add(op.length)
                .is_none_or(|end| end > chunk.length)
        {
            return Err(invalid(
                "external Chunk identity, size or requested range is invalid",
            ));
        }
        let Some(source) = set.sources.iter().find(|source| {
            source.role == CopyRole::ExternalCommitted
                && source.state.is_readable()
                && source.data_endpoint.is_none()
                && matches!(source.location, CopyLocation::External { .. })
        }) else {
            return Err(unavailable("committed Chunk has no readable external copy"));
        };
        let validation = DfsReadValidation {
            grant: source.read_grant.clone(),
            chunk_id: op.chunk_id.clone(),
            copy_id: source.copy_id.clone(),
            chunk_offset: op.chunk_offset,
            length: op.length,
        };
        Ok((chunk.clone(), source.clone(), validation))
    }

    fn cached_sources_for(
        &self,
        batch: &ReadBatch,
    ) -> Result<(Vec<ChunkSources>, Vec<ChunkReadOp>)> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let now = Instant::now();
        let mut resolved = Vec::new();
        let mut misses = Vec::new();
        let mut seen = BTreeSet::new();
        let mut cache = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
        cache.retain(|_, entry| entry.expires_at > now);
        let mut missing_chunks = BTreeSet::new();
        for op in &batch.ops {
            let key = (
                version.clone(),
                batch.layout_root_id.clone(),
                op.chunk_id.clone(),
            );
            match cache.get(&key) {
                Some(entry) => {
                    if seen.insert(op.chunk_id.clone()) {
                        resolved.push(entry.sources.clone());
                    }
                }
                None => {
                    missing_chunks.insert(op.chunk_id.clone());
                    misses.push(op.clone());
                }
            }
        }
        resolved.retain(|set| !missing_chunks.contains(&set.chunk_id));
        Ok((resolved, misses))
    }

    fn read_external_authorized_selection(
        &self,
        backend: &ExternalReadBackend,
        selection: &ExternalReadSelection,
        authorized: Vec<DfsExternalAuthorizedRead>,
        out: &mut [u8],
        authority_deadline: Option<Instant>,
    ) -> Result<Option<u64>> {
        if authorized.len() != selection.selected.len() {
            return Err(external_denied(
                "external read authorization reply is incomplete",
            ));
        }
        let now = now_unix_ms();
        let mut expires_at = u64::MAX;
        let mut reads = Vec::with_capacity(authorized.len());
        for (entry, selected) in authorized.into_iter().zip(&selection.selected) {
            let authorization = &entry.authorized;
            let validation = &authorization.validation;
            let covered = authorization.allowed_ranges.iter().any(|(start, length)| {
                validation.chunk_offset >= *start
                    && start.checked_add(*length).is_some_and(|end| {
                        validation
                            .chunk_offset
                            .checked_add(validation.length)
                            .is_some_and(|requested_end| requested_end <= end)
                    })
            });
            if validation.chunk_id != selected.chunk_id
                || validation.chunk_offset != selected.chunk_offset
                || validation.length != selected.length
                || authorization.expires_at_unix_ms <= now
                || authorization.expires_at_unix_ms > validation.grant.expires_at_unix_ms
                || authorization.allowed_ranges.len() > MAX_DFS_READ_OPS
                || authorization.allowed_ranges.iter().any(|(start, length)| {
                    *length == 0
                        || start
                            .checked_add(*length)
                            .is_none_or(|end| end > entry.chunk.length)
                })
                || !covered
                || entry.chunk.id != selected.chunk_id
                || entry.chunk.length > MAX_DFS_EXTERNAL_CHUNK_BYTES
                || validation
                    .chunk_offset
                    .checked_add(validation.length)
                    .is_none_or(|end| end > entry.chunk.length)
            {
                return Err(external_denied(
                    "external read authorization changed identity, range or expiry",
                ));
            }
            let source = SourceCandidate {
                copy_id: validation.copy_id.clone(),
                chunk_id: validation.chunk_id.clone(),
                role: CopyRole::ExternalCommitted,
                state: CopyState::Ready,
                location: entry.location.clone(),
                data_endpoint: None,
                load_hint: 0,
                read_grant: validation.grant.clone(),
            };
            if !self.source_matches(&selection.batch, &source)
                || !matches!(source.location, CopyLocation::External { .. })
            {
                return Err(external_denied(
                    "external read authorization returned an unusable source",
                ));
            }
            if let CopyLocation::External {
                store_id,
                object_key,
                object_revision,
            } = &source.location
            {
                let expected_key =
                    crate::dfs::external_object_key(store_id, object_revision, &entry.chunk.id)
                        .map_err(|error| external_denied(error.to_string()))?;
                if object_key != &expected_key {
                    return Err(external_denied(
                        "external read authorization returned a different object key",
                    ));
                }
            }
            if let (Some(expected_chunk), Some(expected_source)) =
                (&selected.chunk, &selected.source)
            {
                if entry.chunk != *expected_chunk || source.location != expected_source.location {
                    return Err(external_denied(
                        "external read authorization changed cached source identity",
                    ));
                }
                let expected_validation = DfsReadValidation {
                    grant: expected_source.read_grant.clone(),
                    chunk_id: expected_source.chunk_id.clone(),
                    copy_id: expected_source.copy_id.clone(),
                    chunk_offset: selected.chunk_offset,
                    length: selected.length,
                };
                if authorization.validation != expected_validation {
                    return Err(external_denied(
                        "external read authorization changed cached grant identity",
                    ));
                }
            }
            expires_at = expires_at.min(authorization.expires_at_unix_ms);
            reads.push(AuthorizedExternalRead {
                chunk: entry.chunk,
                source,
                op: ChunkReadOp {
                    chunk_id: selected.chunk_id.clone(),
                    chunk_offset: selected.chunk_offset,
                    length: selected.length,
                    output_offset: selected.output_offset,
                },
            });
        }
        self.cache_authorized_external_sources(selection, &reads)?;
        // Retain at most one full object plus the requested-byte staging buffer.
        // Several ranges of one chunk share this fetch and full-content digest.
        let mut fetched = BTreeSet::new();
        let mut fetched_chunks = 0_u64;
        let mut copied_bytes = 0_u64;
        for read in &reads {
            let chunk = &read.chunk;
            if !fetched.insert(chunk.id.clone()) {
                continue;
            }
            if expires_at <= now_unix_ms()
                || authority_deadline.is_some_and(|deadline| deadline <= Instant::now())
            {
                return Err(external_denied(
                    "external read authority expired before the next fetch",
                ));
            }
            let bytes = tracing::info_span!(
                "node.dfs.read.external_fetch",
                chunk_id = %chunk.id.0,
                chunk_bytes = chunk.length,
            )
            .in_scope(|| backend.reader.read_chunk(chunk, &read.source))?;
            fetched_chunks = fetched_chunks.saturating_add(1);
            if !bytes.matches(chunk) {
                return Err(corrupt(
                    "verified external Chunk identity does not match the request",
                ));
            }
            let bytes = bytes.as_slice();
            for op in reads
                .iter()
                .filter(|read| read.chunk.id == chunk.id)
                .map(|read| &read.op)
            {
                let start = usize::try_from(op.chunk_offset)
                    .map_err(|_| invalid("external Chunk offset is too large"))?;
                let length = usize::try_from(op.length)
                    .map_err(|_| invalid("external Chunk range is too large"))?;
                copied_bytes = copied_bytes.saturating_add(length as u64);
                tracing::info_span!(
                    "node.dfs.read.external_copy",
                    copy_stage = "external_to_staged",
                    chunk_id = %chunk.id.0,
                    chunk_offset = op.chunk_offset,
                    requested_bytes = op.length,
                    copied_bytes = length as u64,
                )
                .in_scope(|| {
                    out[op.output_offset..op.output_offset + length]
                        .copy_from_slice(&bytes[start..start + length]);
                });
            }
        }
        tracing::Span::current().record("fetched_chunks", fetched_chunks);
        tracing::Span::current().record("copied_bytes", copied_bytes);
        Ok(Some(expires_at))
    }

    fn cache_authorized_external_sources(
        &self,
        selection: &ExternalReadSelection,
        reads: &[AuthorizedExternalRead],
    ) -> Result<()> {
        let Some(version) = selection.batch.file_version_id.clone() else {
            return Ok(());
        };
        if reads.is_empty() {
            return Ok(());
        }
        let mut cache = self
            .source_cache
            .lock()
            .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
        if cache.len().saturating_add(reads.len()) > MAX_CACHED_SOURCES {
            cache.clear();
        }
        let expires_at = Instant::now() + self.config.source_cache_ttl;
        for read in reads {
            cache.insert(
                (
                    version.clone(),
                    selection.batch.layout_root_id.clone(),
                    read.chunk.id.clone(),
                ),
                CachedSources {
                    expires_at,
                    sources: ChunkSources {
                        chunk_id: read.chunk.id.clone(),
                        chunk: Some(read.chunk.clone()),
                        sources: vec![read.source.clone()],
                    },
                },
            );
        }
        Ok(())
    }

    fn validate_current_authorized_inode(
        &self,
        inode: &crate::dfs::InodeRecord,
        expected_inode_id: &crate::dfs::InodeId,
        expected_head_version: &FileVersionId,
    ) -> Result<()> {
        if inode.namespace_id != self.namespace_id
            || inode.inode_id != *expected_inode_id
            || inode.kind != crate::dfs::InodeKind::Regular
            || inode.head_version.as_ref() != Some(expected_head_version)
        {
            return Err(external_denied(
                "current external read authorization returned a different inode head",
            ));
        }
        Ok(())
    }

    fn validate_current_head_changed_inode(
        &self,
        inode: &crate::dfs::InodeRecord,
        expected_inode_id: &crate::dfs::InodeId,
        expected_head_version: &FileVersionId,
    ) -> Result<()> {
        if inode.namespace_id != self.namespace_id
            || inode.inode_id != *expected_inode_id
            || inode.kind != crate::dfs::InodeKind::Regular
            || inode.head_version.as_ref() == Some(expected_head_version)
        {
            return Err(external_denied(
                "current external read head-changed reply returned an invalid inode",
            ));
        }
        Ok(())
    }

    fn read_batch_into(&self, batch: &ReadBatch, out: &mut [u8]) -> Result<()> {
        let mut groups: Vec<(ChunkId, Vec<usize>)> = Vec::new();
        let mut positions: HashMap<ChunkId, usize> = HashMap::new();
        for (index, op) in batch.ops.iter().enumerate() {
            if op.length == 0 {
                continue;
            }
            if let Some(group_index) = positions.get(&op.chunk_id).copied() {
                groups[group_index].1.push(index);
            } else {
                positions.insert(op.chunk_id.clone(), groups.len());
                groups.push((op.chunk_id.clone(), vec![index]));
            }
        }
        let mut remote = vec![false; batch.ops.len()];
        for (chunk_id, indices) in groups {
            let reader = match self.local_reader(&chunk_id) {
                Ok(reader) => reader,
                Err(error) => {
                    self.handle_local_read_failure(&chunk_id, &error)?;
                    for index in indices {
                        remote[index] = true;
                    }
                    continue;
                }
            };
            let mut ranges = Vec::with_capacity(indices.len());
            for &index in &indices {
                let op = &batch.ops[index];
                let length = usize::try_from(op.length)
                    .map_err(|_| invalid("DFS read length is too large"))?;
                ranges.push(VerifiedRangeCopy {
                    chunk_offset: op.chunk_offset,
                    length,
                    output_offset: op.output_offset,
                });
            }
            match reader.read_ranges_at_uncommitted(&ranges, out) {
                Ok(counts) => {
                    let mut short = false;
                    for (&index, count) in indices.iter().zip(counts) {
                        let expected = usize::try_from(batch.ops[index].length)
                            .map_err(|_| invalid("DFS read length is too large"))?;
                        if count != expected {
                            remote[index] = true;
                            short = true;
                        }
                    }
                    if short {
                        let error = corrupt("local Chunk ended before the requested range");
                        self.handle_local_read_failure(&chunk_id, &error)?;
                    }
                }
                Err(error) => {
                    self.handle_local_read_failure(&chunk_id, &error)?;
                    for index in indices {
                        remote[index] = true;
                    }
                }
            }
        }
        let remote_ops = batch
            .ops
            .iter()
            .enumerate()
            .filter(|(index, _op)| remote[*index])
            .map(|(_, op)| op.clone())
            .collect::<Vec<_>>();
        if remote_ops.is_empty() {
            return Ok(());
        }
        let remote_batch = ReadBatch {
            file_version_id: batch.file_version_id.clone(),
            layout_root_id: batch.layout_root_id.clone(),
            ops: remote_ops,
        };
        let sources = self.sources_for(&remote_batch)?;
        self.read_remote_batch(&remote_batch, &sources, out)?;
        Ok(())
    }

    fn handle_local_read_failure(&self, chunk_id: &ChunkId, error: &Error) -> Result<()> {
        if matches!(
            error.kind(),
            afs_error::ErrorKind::DataLoss | afs_error::ErrorKind::NotFound
        ) {
            self.local.try_quarantine(chunk_id);
        }
        // A cached pin can outlive a corrupt file's replacement. Drop it on
        // failure so the next attempt opens the current physical copy instead
        // of retrying the retired descriptor.
        self.readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?
            .remove(chunk_id);
        Ok(())
    }

    fn local_reader(&self, chunk_id: &ChunkId) -> Result<Arc<PinnedChunkReader>> {
        if let Some(reader) = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?
            .get(chunk_id)
            .cloned()
        {
            return Ok(reader);
        }
        let reader = Arc::new(self.local.open_verified(chunk_id)?);
        let mut readers = self
            .readers
            .lock()
            .map_err(|_| unavailable("DFS local reader cache is poisoned"))?;
        if readers.len() >= MAX_PINNED_READERS {
            // Dropping the cached Arc does not invalidate an active read pin.
            readers.clear();
        }
        readers.insert(chunk_id.clone(), reader.clone());
        Ok(reader)
    }

    fn sources_for(&self, batch: &ReadBatch) -> Result<Vec<ChunkSources>> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let now = Instant::now();
        let mut resolved = Vec::new();
        let mut misses = Vec::new();
        let mut seen = BTreeSet::new();
        let span = tracing::info_span!(
            "node.dfs.read.sources",
            ops = batch.ops.len(),
            cache_hits = tracing::field::Empty,
            cache_misses = tracing::field::Empty,
            fresh_chunks = tracing::field::Empty,
        );
        let _entered = span.enter();
        let mut cache_hits = 0_u64;
        {
            let mut cache = self
                .source_cache
                .lock()
                .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
            cache.retain(|_, entry| entry.expires_at > now);
            for op in &batch.ops {
                if !seen.insert(op.chunk_id.clone()) {
                    continue;
                }
                let key = (
                    version.clone(),
                    batch.layout_root_id.clone(),
                    op.chunk_id.clone(),
                );
                match cache.get(&key).filter(|entry| {
                    entry
                        .sources
                        .sources
                        .iter()
                        .all(|source| self.source_matches(batch, source))
                }) {
                    Some(entry) => {
                        cache_hits = cache_hits.saturating_add(1);
                        resolved.push(entry.sources.clone())
                    }
                    None => misses.push(op.clone()),
                }
            }
        }
        span.record("cache_hits", cache_hits);
        span.record("cache_misses", misses.len() as u64);
        if misses.is_empty() {
            span.record("fresh_chunks", 0_u64);
            return Ok(resolved);
        }
        let request = ReadBatch {
            file_version_id: Some(version.clone()),
            layout_root_id: batch.layout_root_id.clone(),
            ops: misses,
        }
        .source_request(self.local_node_id.clone(), self.namespace_id.clone())?;
        let expected: BTreeSet<_> = request.chunk_ids.iter().cloned().collect();
        let fresh = self.source_provider.sources_for(request)?.chunks;
        span.record("fresh_chunks", fresh.len() as u64);
        let mut received = BTreeSet::new();
        for set in &fresh {
            if !expected.contains(&set.chunk_id)
                || !received.insert(set.chunk_id.clone())
                || set.sources.iter().any(|source| {
                    source.chunk_id != set.chunk_id || !self.source_matches(batch, source)
                })
            {
                return Err(invalid(
                    "DFS source reply changed the requested content or authorization identity",
                ));
            }
        }
        if expected != received {
            return Err(unavailable("DFS source lookup omitted a requested Chunk"));
        }
        {
            let mut cache = self
                .source_cache
                .lock()
                .map_err(|_| unavailable("DFS read source cache is poisoned"))?;
            if cache.len().saturating_add(fresh.len()) > MAX_CACHED_SOURCES {
                cache.clear();
            }
            for set in &fresh {
                cache.insert(
                    (
                        version.clone(),
                        batch.layout_root_id.clone(),
                        set.chunk_id.clone(),
                    ),
                    CachedSources {
                        expires_at: now + self.config.source_cache_ttl,
                        sources: set.clone(),
                    },
                );
            }
        }
        resolved.extend(fresh);
        Ok(resolved)
    }

    fn source_matches(&self, batch: &ReadBatch, source: &SourceCandidate) -> bool {
        let grant = &source.read_grant;
        Some(&grant.file_version_id) == batch.file_version_id.as_ref()
            && grant.layout_root_id == batch.layout_root_id
            && grant.namespace_id == self.namespace_id
            && grant.caller_node_id == self.local_node_id
            && grant.caller_node_epoch != 0
            && grant.expires_at_unix_ms > now_unix_ms()
            && !grant.token.is_empty()
    }

    fn read_remote_batch(
        &self,
        batch: &ReadBatch,
        sources: &[ChunkSources],
        out: &mut [u8],
    ) -> Result<()> {
        let version = batch
            .file_version_id
            .clone()
            .ok_or_else(|| invalid("DFS peer read requires a fixed FileVersion"))?;
        let candidates: Vec<Vec<SourceCandidate>> = batch.ops.iter().map(|op| {
            sources.iter().find(|set| set.chunk_id == op.chunk_id)
                .map(|set| set.sources.iter().filter(|source| {
                    source.state.is_readable()
                        && matches!(source.role, CopyRole::DurableReplica | CopyRole::VerifiedCache)
                        && matches!(&source.location, CopyLocation::Node { node_id, .. } if node_id != &self.local_node_id)
                }).cloned().collect()).unwrap_or_default()
        }).collect();
        let mut next = vec![0; batch.ops.len()];
        let mut pending: Vec<usize> = (0..batch.ops.len()).collect();
        let mut last_error = None;
        while !pending.is_empty() {
            let mut groups: Vec<Vec<usize>> = Vec::new();
            for &index in &pending {
                let Some(source) = candidates[index].get(next[index]) else {
                    return Err(last_error
                        .unwrap_or_else(|| corrupt("committed Chunk has no readable copy")));
                };
                if let Some(group) = groups.iter_mut().find(|group| {
                    let first = group[0];
                    same_peer_context(&candidates[first][next[first]], source)
                }) {
                    group.push(index);
                } else {
                    groups.push(vec![index]);
                }
            }
            pending.clear();
            for group in groups {
                let mut length = 0;
                let operations = group
                    .iter()
                    .map(|&index| {
                        let mut op = batch.ops[index].clone();
                        op.output_offset = length;
                        length += op.length as usize; // validated by ReadBatch
                        ResolvedReadOp {
                            op,
                            source: candidates[index][next[index]].clone(),
                        }
                    })
                    .collect();
                let attempt = PeerReadBatch {
                    file_version_id: version.clone(),
                    layout_root_id: batch.layout_root_id.clone(),
                    operations,
                };
                let mut scratch = vec![0; length];
                match self.transfer.read_ranges(&attempt, &mut scratch) {
                    Ok(()) => {
                        for (&index, item) in group.iter().zip(&attempt.operations) {
                            let original = &batch.ops[index];
                            let len = original.length as usize;
                            out[original.output_offset..original.output_offset + len]
                                .copy_from_slice(
                                    &scratch[item.op.output_offset..item.op.output_offset + len],
                                );
                        }
                    }
                    Err(error) => {
                        last_error = Some(error);
                        // A batch error does not identify which copy failed. Before
                        // discarding a valid source for its neighbours, retry each
                        // operation on that same source once with a fresh scratch.
                        // Each candidate gets at most one batch and one single-op
                        // attempt; failed singles advance exactly once.
                        for (index, item) in group.iter().copied().zip(&attempt.operations) {
                            if group.len() > 1 {
                                let mut single = item.clone();
                                single.op.output_offset = 0;
                                let single_batch = PeerReadBatch {
                                    file_version_id: version.clone(),
                                    layout_root_id: batch.layout_root_id.clone(),
                                    operations: vec![single],
                                };
                                let mut single_scratch = vec![0; item.op.length as usize];
                                match self
                                    .transfer
                                    .read_ranges(&single_batch, &mut single_scratch)
                                {
                                    Ok(()) => {
                                        let original = &batch.ops[index];
                                        out[original.output_offset
                                            ..original.output_offset + single_scratch.len()]
                                            .copy_from_slice(&single_scratch);
                                        continue;
                                    }
                                    Err(error) => last_error = Some(error),
                                }
                            }
                            next[index] += 1;
                            pending.push(index);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub struct UnimplementedReadSourceProvider;

impl ReadSourceProvider for UnimplementedReadSourceProvider {
    fn sources_for(&self, _request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
        Err(Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "DFS read source lookup is not wired",
        ))
    }
}

pub struct UnimplementedChunkTransfer;

impl ChunkTransfer for UnimplementedChunkTransfer {
    fn read_ranges(&self, _batch: &PeerReadBatch, _out: &mut [u8]) -> Result<()> {
        Err(Error::coded(
            afs_error::NODE_TRANSFER_UNSUPPORTED,
            "DFS peer range read is not wired",
        ))
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_INVALID, message)
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_UNAVAILABLE, message)
}

fn external_denied(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_PERMISSION_DENIED, message)
}

fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        dfs::{CopyId, CopyState, DfsReadGrant, OperationId},
        node::chunk::{ChunkBuilder, ChunkStore, StagedChunk},
    };
    use std::{
        sync::{Arc, Condvar},
        time::Duration,
    };

    #[derive(Default)]
    struct StaticSources {
        reply: Mutex<DfsChunkSourcesReply>,
        calls: Mutex<u64>,
    }

    impl StaticSources {
        fn new(reply: DfsChunkSourcesReply) -> Self {
            Self {
                reply: Mutex::new(reply),
                calls: Mutex::new(0),
            }
        }
    }

    impl ReadSourceProvider for StaticSources {
        fn sources_for(&self, _request: DfsChunkSourcesRequest) -> Result<DfsChunkSourcesReply> {
            *self.calls.lock().unwrap() += 1;
            Ok(self.reply.lock().unwrap().clone())
        }
    }

    struct StaticTransfer {
        bytes: Vec<u8>,
        fail_first: Mutex<bool>,
    }

    impl ChunkTransfer for StaticTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            let mut fail_first = self.fail_first.lock().unwrap();
            if *fail_first {
                *fail_first = false;
                return Err(unavailable("injected peer failure"));
            }
            for item in &batch.operations {
                let op = &item.op;
                let length = usize::try_from(op.length).unwrap();
                out[op.output_offset..op.output_offset + length]
                    .copy_from_slice(&self.bytes[..length]);
            }
            Ok(())
        }
    }

    #[derive(Default)]
    struct PipelineProbe {
        state: Mutex<PipelineProbeState>,
        wake: Condvar,
    }

    #[derive(Default)]
    struct PipelineProbeState {
        head_hash_started: bool,
        head_hash_released: bool,
        tail_read_before_head_release: bool,
        hash_finished: bool,
    }

    impl PipelineProbe {
        fn mark_head_hash_started_and_wait(&self) {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut state = self.state.lock().unwrap();
            state.head_hash_started = true;
            self.wake.notify_all();
            while !state.head_hash_released {
                let now = Instant::now();
                assert!(now < deadline, "tail read did not release head hash");
                let timeout = deadline.saturating_duration_since(now);
                let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
                state = next;
            }
        }

        fn mark_tail_read_and_release_head(&self) {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut state = self.state.lock().unwrap();
            while !state.head_hash_started {
                let now = Instant::now();
                assert!(now < deadline, "head hash did not start before tail read");
                let timeout = deadline.saturating_duration_since(now);
                let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
                state = next;
            }
            state.tail_read_before_head_release = !state.head_hash_released;
            state.head_hash_released = true;
            self.wake.notify_all();
        }

        fn release_head_hash(&self) {
            let deadline = Instant::now() + Duration::from_secs(10);
            let mut state = self.state.lock().unwrap();
            while !state.head_hash_started {
                let now = Instant::now();
                assert!(now < deadline, "head hash did not start before release");
                let timeout = deadline.saturating_duration_since(now);
                let (next, _) = self.wake.wait_timeout(state, timeout).unwrap();
                state = next;
            }
            state.head_hash_released = true;
            self.wake.notify_all();
        }

        fn mark_hash_finished(&self) {
            let mut state = self.state.lock().unwrap();
            state.hash_finished = true;
            self.wake.notify_all();
        }

        fn tail_read_before_head_release(&self) -> bool {
            self.state.lock().unwrap().tail_read_before_head_release
        }

        fn hash_finished(&self) -> bool {
            self.state.lock().unwrap().hash_finished
        }
    }

    fn two_worker_pool() -> rayon_core::ThreadPool {
        rayon_core::ThreadPoolBuilder::new()
            .num_threads(2)
            .build()
            .unwrap()
    }

    #[test]
    fn ready_pipeline_reads_tail_while_head_hash_is_in_flight() {
        let payload = (0..(2 * 1024 * 1024 + 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-overlap"), payload.clone());
        let pool = two_worker_pool();
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == 1024 * 1024 {
                    loader_probe.mark_tail_read_and_release_head();
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == 0 {
                    hash_probe.mark_head_hash_started_and_wait();
                }
            },
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert!(
            probe.tail_read_before_head_release(),
            "tail read must start while the head hash task is still in flight"
        );
    }

    #[test]
    fn ready_pipeline_overlaps_each_hash_with_next_segment_read() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let pool = two_worker_pool();

        for target_offset in [0, MIB, 2 * MIB] {
            let staged = StagedChunk::new(
                OperationId::new(format!("ready-overlap-{target_offset}")),
                payload.clone(),
            );
            let probe = Arc::new(PipelineProbe::default());
            let loader_probe = probe.clone();
            let hash_probe = probe.clone();

            let verified = VerifiedExternalChunk::load_with_rayon_pool(
                &staged.chunk,
                vec![0; payload.len()],
                &pool,
                0,
                |offset, segment, _tail| {
                    if offset == (target_offset + MIB) as u64 {
                        loader_probe.mark_tail_read_and_release_head();
                    }
                    let offset = usize::try_from(offset).unwrap();
                    segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                    Ok(())
                },
                || Ok(()),
                move |offset| {
                    if offset == target_offset as u64 {
                        hash_probe.mark_head_hash_started_and_wait();
                    }
                },
            )
            .unwrap();

            assert_eq!(verified.as_slice(), staged.bytes());
            assert!(
                probe.tail_read_before_head_release(),
                "read of the next segment must overlap blocked hash at offset {target_offset}"
            );
        }
    }

    #[test]
    fn ready_pipeline_waits_for_head_hash_after_next_read_error() {
        let payload = (0..(2 * 1024 * 1024 + 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-tail-error"), payload.clone());
        let pool = two_worker_pool();
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == 1024 * 1024 {
                    loader_probe.release_head_hash();
                    return Err(unavailable("injected next read failure"));
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == 0 {
                    hash_probe.mark_head_hash_started_and_wait();
                    hash_probe.mark_hash_finished();
                }
            },
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert!(
            probe.hash_finished(),
            "blocked head hash worker must be released before the next read error returns"
        );
    }

    #[test]
    fn ready_pipeline_loads_four_one_mib_segments_and_keeps_allocation() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-four-segments"), payload.clone());
        let pool = two_worker_pool();
        let buffer = vec![0; payload.len()];
        let buffer_ptr = buffer.as_ptr();
        let buffer_capacity = buffer.capacity();
        let loads = Arc::new(Mutex::new(Vec::new()));
        let observed_loads = loads.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            buffer,
            &pool,
            0,
            |offset, segment, tail| {
                observed_loads
                    .lock()
                    .unwrap()
                    .push((offset, segment.len(), tail));
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            |_| {},
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert_eq!(verified.as_slice().as_ptr(), buffer_ptr);
        assert_eq!(verified.allocation_bytes(), buffer_capacity as u64);
        assert_eq!(
            loads.lock().unwrap().as_slice(),
            &[
                (0, MIB, false),
                (MIB as u64, MIB, false),
                ((2 * MIB) as u64, MIB, false),
                ((3 * MIB) as u64, MIB, true),
            ]
        );
    }

    #[test]
    fn ready_pipeline_accepts_empty_chunk_without_segment_loads() {
        let staged = StagedChunk::new(OperationId::new("ready-empty"), Vec::new());
        let pool = two_worker_pool();
        let finalized = Arc::new(Mutex::new(false));
        let finalized_probe = finalized.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            Vec::new(),
            &pool,
            0,
            |_offset, _segment, _tail| panic!("empty ready chunk must not load a segment"),
            || {
                *finalized_probe.lock().unwrap() = true;
                Ok(())
            },
            |_| {},
        )
        .unwrap();

        assert!(verified.as_slice().is_empty());
        assert!(*finalized.lock().unwrap());
    }

    #[test]
    fn ready_pipeline_loads_short_tail_after_full_segment() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(MIB + 123))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-short-tail"), payload.clone());
        let pool = two_worker_pool();
        let loads = Arc::new(Mutex::new(Vec::new()));
        let observed_loads = loads.clone();

        let verified = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, tail| {
                observed_loads
                    .lock()
                    .unwrap()
                    .push((offset, segment.len(), tail));
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            |_| {},
        )
        .unwrap();

        assert_eq!(verified.as_slice(), staged.bytes());
        assert_eq!(
            loads.lock().unwrap().as_slice(),
            &[(0, MIB, false), (MIB as u64, 123, true)]
        );
    }

    #[test]
    fn ready_pipeline_rejects_corrupt_middle_and_final_segments() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let pool = two_worker_pool();

        for corrupt_offset in [MIB, 3 * MIB] {
            let staged = StagedChunk::new(
                OperationId::new(format!("ready-corrupt-{corrupt_offset}")),
                payload.clone(),
            );
            let error = VerifiedExternalChunk::load_with_rayon_pool(
                &staged.chunk,
                vec![0; payload.len()],
                &pool,
                0,
                |offset, segment, _tail| {
                    let offset = usize::try_from(offset).unwrap();
                    segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                    if offset == corrupt_offset {
                        segment[0] ^= 0x55;
                    }
                    Ok(())
                },
                || Ok(()),
                |_| {},
            )
            .unwrap_err();

            assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        }
    }

    #[test]
    fn ready_pipeline_joins_accepted_hash_before_late_read_failure_returns() {
        const MIB: usize = 1024 * 1024;
        let payload = (0..(4 * MIB))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-late-read-failure"), payload.clone());
        let pool = two_worker_pool();
        let probe = Arc::new(PipelineProbe::default());
        let loader_probe = probe.clone();
        let hash_probe = probe.clone();

        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                if offset == (3 * MIB) as u64 {
                    loader_probe.mark_tail_read_and_release_head();
                    return Err(unavailable("injected late ready read failure"));
                }
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Ok(()),
            move |offset| {
                if offset == (2 * MIB) as u64 {
                    hash_probe.mark_head_hash_started_and_wait();
                    hash_probe.mark_hash_finished();
                }
            },
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_UNAVAILABLE);
        assert!(
            probe.tail_read_before_head_release(),
            "failing read must wait for the middle hash worker to start before releasing it"
        );
        assert!(
            probe.hash_finished(),
            "accepted hash worker must be released and joined before the late read error returns"
        );
    }

    #[test]
    fn ready_pipeline_rejects_reader_growth_after_exact_length() {
        let payload = (0..(2 * 1024 * 1024 + 1024))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let staged = StagedChunk::new(OperationId::new("ready-eof-probe"), payload.clone());
        let pool = two_worker_pool();

        let error = VerifiedExternalChunk::load_with_rayon_pool(
            &staged.chunk,
            vec![0; payload.len()],
            &pool,
            0,
            |offset, segment, _tail| {
                let offset = usize::try_from(offset).unwrap();
                segment.copy_from_slice(&payload[offset..offset + segment.len()]);
                Ok(())
            },
            || Err(corrupt("S3 cache entry exceeds expected length")),
            |_| {},
        )
        .unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
    }

    fn source(copy: &str, chunk_id: &ChunkId, node_id: &str) -> SourceCandidate {
        SourceCandidate {
            copy_id: CopyId::new(copy),
            chunk_id: chunk_id.clone(),
            role: CopyRole::DurableReplica,
            state: CopyState::Ready,
            location: CopyLocation::Node {
                node_id: node_id.into(),
                node_epoch: 1,
                device_id: "local-0".into(),
                device_epoch: 1,
                catalog_revision: 1,
            },
            data_endpoint: Some(format!("http://{node_id}")),
            load_hint: 0,
            read_grant: DfsReadGrant {
                namespace_id: crate::dfs::NamespaceId::new("default"),
                file_version_id: FileVersionId::new("version"),
                layout_root_id: LayoutRootId::new("layout"),
                caller_node_id: "node-a".into(),
                caller_node_epoch: 1,
                expires_at_unix_ms: u64::MAX,
                fence: 1,
                token: "test-token".into(),
            },
        }
    }

    fn put_local_chunk(local: &LocalChunkStore, operation: &str, bytes: &[u8]) -> ChunkId {
        let mut builder = ChunkBuilder::default();
        builder.replace(bytes.to_vec());
        let staged = builder.stage(OperationId::new(operation));
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        chunk_id
    }

    #[test]
    fn local_read_uses_verified_reader_without_source_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "op", b"abcdef");
        let sources = Arc::new(StaticSources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let mut out = [0; 3];
        engine
            .read_batch(
                &ReadBatch {
                    file_version_id: Some(FileVersionId::new("version")),
                    layout_root_id: LayoutRootId::new("layout"),
                    ops: vec![ChunkReadOp {
                        chunk_id,
                        chunk_offset: 2,
                        length: 3,
                        output_offset: 0,
                    }],
                },
                &mut out,
            )
            .unwrap();
        assert_eq!(&out, b"cde");
        assert_eq!(*sources.calls.lock().unwrap(), 0);
    }

    #[test]
    fn same_chunk_group_preserves_overlapping_output_order() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "overlap", b"abcdef");
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 3,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id,
                    chunk_offset: 3,
                    length: 3,
                    output_offset: 0,
                },
            ],
        };
        let mut out = [b'!'; 3];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"def");
    }

    #[test]
    fn same_chunk_damage_outside_selected_ranges_falls_back_atomically() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let mut bytes = vec![b'a'; 128 * 1024 + 1];
        bytes[4096] = b'b';
        let chunk_id = put_local_chunk(local.as_ref(), "outside-damage", &bytes);
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 1,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 4096,
                    length: 1,
                    output_offset: 1,
                },
            ],
        };
        let mut out = [b'!'; 2];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"ab");
        let path = temp.path().join("chunks").join(&chunk_id.0);
        bytes[128 * 1024] ^= 1;
        std::fs::write(&path, bytes).unwrap();
        out = [b'!'; 2];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!");
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
        assert!(engine.readers.lock().unwrap().is_empty());
    }

    #[test]
    fn same_chunk_short_read_falls_back_only_for_short_ranges_in_original_order() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "short", b"abcdef");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![source("remote-short", &chunk_id, "node-b")],
            }],
        }));
        let transfer = Arc::new(RecordingTransfer::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources,
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 1,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: u64::MAX,
                    length: 1,
                    output_offset: 1,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 1,
                    length: 1,
                    output_offset: 2,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 5,
                    length: 2,
                    output_offset: 3,
                },
                ChunkReadOp {
                    chunk_id,
                    chunk_offset: 0,
                    length: 0,
                    output_offset: 0,
                },
            ],
        };
        let mut out = [b'!'; 5];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"arbrr");
        assert!(local.quarantined_chunks().unwrap().is_empty());
        assert!(engine.readers.lock().unwrap().is_empty());
        let calls = transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations.len(), 2);
        assert_eq!(calls[0].operations[0].op.chunk_offset, u64::MAX);
        assert_eq!(calls[0].operations[1].op.chunk_offset, 5);
        assert_eq!(calls[0].operations[1].op.length, 2);
    }

    #[test]
    fn same_chunk_cached_pin_survives_path_replacement_for_grouped_reads() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = put_local_chunk(local.as_ref(), "pin", b"abcdef");
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            Arc::new(StaticSources::default()),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 0,
                    length: 3,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: chunk_id.clone(),
                    chunk_offset: 3,
                    length: 3,
                    output_offset: 3,
                },
            ],
        };
        let mut out = [0; 6];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
        let replacement = temp.path().join("replacement");
        std::fs::write(&replacement, b"abcXef").unwrap();
        std::fs::rename(replacement, temp.path().join("chunks").join(&chunk_id.0)).unwrap();
        out.fill(b'!');
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
        assert!(local.open_verified(&chunk_id).is_err());
    }

    #[test]
    fn diagnostic_cold_corrupted_local_copy_uses_healthy_peer() {
        diagnostic_corrupted_local_copy(false);
    }

    #[test]
    fn diagnostic_warm_corrupted_local_copy_uses_healthy_peer() {
        diagnostic_corrupted_local_copy(true);
    }

    fn diagnostic_corrupted_local_copy(warm: bool) {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let mut builder = ChunkBuilder::default();
        builder.replace(b"abcdef".to_vec());
        let staged = builder.stage(OperationId::new("diagnostic"));
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![source("healthy-b", &chunk_id, "node-b")],
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources.clone(),
            Arc::new(StaticTransfer {
                bytes: b"abcdef".to_vec(),
                fail_first: Mutex::new(false),
            }),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk_id.clone(),
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut out = [0; 6];
        if warm {
            engine.read_batch(&batch, &mut out).unwrap();
            assert_eq!(&out, b"abcdef");
            assert_eq!(*sources.calls.lock().unwrap(), 0);
        }
        let path = temp.path().join("chunks").join(&chunk_id.0);
        std::fs::write(&path, b"abXdef").unwrap();
        std::fs::File::open(&path).unwrap().sync_all().unwrap();
        out.fill(9);
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(
            &out, b"abcdef",
            "corrupted cached local bytes must not reach the caller"
        );
        assert_eq!(*sources.calls.lock().unwrap(), 1);
        assert!(engine.readers.lock().unwrap().is_empty());
        // Replacement keeps the identity but creates a new physical inode.
        // The failed pin must not trap later reads on the retired corrupt fd.
        local
            .put(crate::node::chunk::StagedChunk::new(
                OperationId::new("restore"),
                b"abcdef".to_vec(),
            ))
            .unwrap();
        out.fill(9);
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"abcdef");
        assert_eq!(*sources.calls.lock().unwrap(), 1);
        assert_eq!(engine.readers.lock().unwrap().len(), 1);
        assert!(local.quarantined_chunks().unwrap().is_empty());
    }

    #[test]
    fn quarantined_local_copy_and_no_readable_peer_return_eio_without_output() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let staged =
            crate::node::chunk::StagedChunk::new(OperationId::new("all-bad"), b"abcdef".to_vec());
        let chunk_id = staged.chunk.id.clone();
        local.put(staged).unwrap();
        std::fs::write(temp.path().join("chunks").join(&chunk_id.0), b"abXdef").unwrap();
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: Vec::new(),
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local.clone(),
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id,
                chunk_offset: 0,
                length: 6,
                output_offset: 0,
            }],
        };
        let mut out = [9; 6];
        for _ in 0..2 {
            let error = engine.read_batch(&batch, &mut out).unwrap_err();
            assert_eq!(crate::error::errno(&error), libc::EIO);
            assert_eq!(out, [9; 6]);
        }
        assert_eq!(local.quarantined_chunks().unwrap().len(), 1);
    }

    #[test]
    fn remote_read_uses_scratch_and_falls_back_between_sources() {
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap());
        let chunk_id = ChunkId::new("missing");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                chunk: None,
                chunk_id: chunk_id.clone(),
                sources: vec![
                    source("copy-b", &chunk_id, "node-b"),
                    source("copy-c", &chunk_id, "node-c"),
                ],
            }],
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            local,
            sources,
            Arc::new(StaticTransfer {
                bytes: b"remote".to_vec(),
                fail_first: Mutex::new(true),
            }),
            DfsReadConfig::default(),
        );
        let mut out = [9; 8];
        engine
            .read_batch(
                &ReadBatch {
                    file_version_id: Some(FileVersionId::new("version")),
                    layout_root_id: LayoutRootId::new("layout"),
                    ops: vec![ChunkReadOp {
                        chunk_id,
                        chunk_offset: 0,
                        length: 6,
                        output_offset: 1,
                    }],
                },
                &mut out,
            )
            .unwrap();
        assert_eq!(&out, &[9, b'r', b'e', b'm', b'o', b't', b'e', 9]);
    }
    #[derive(Default)]
    struct RecordingTransfer {
        batches: Mutex<Vec<PeerReadBatch>>,
    }

    impl ChunkTransfer for RecordingTransfer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            batch.validate(out.len())?;
            self.batches.lock().unwrap().push(batch.clone());
            for item in &batch.operations {
                let start = item.op.output_offset;
                out[start..start + item.op.length as usize]
                    .fill(item.source.copy_id.0.as_bytes()[0]);
            }
            Ok(())
        }
    }

    #[test]
    fn peer_batch_keeps_each_copy_and_grant_while_coalescing_one_peer() {
        let temp = tempfile::tempdir().unwrap();
        let chunks = [ChunkId::new("one"), ChunkId::new("two")];
        let mut second = source("b-copy", &chunks[1], "node-b");
        second.read_grant.token = "another-copy-grant".into();
        second.read_grant.fence = 2;
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![
                ChunkSources {
                    chunk: None,
                    chunk_id: chunks[0].clone(),
                    sources: vec![source("a-copy", &chunks[0], "node-b")],
                },
                ChunkSources {
                    chunk: None,
                    chunk_id: chunks[1].clone(),
                    sources: vec![second],
                },
            ],
        }));
        let transfer = Arc::new(RecordingTransfer::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: chunks
                .into_iter()
                .enumerate()
                .map(|(index, chunk_id)| ChunkReadOp {
                    chunk_id,
                    chunk_offset: 0,
                    length: 2,
                    output_offset: index * 2,
                })
                .collect(),
        };
        let mut out = [0; 4];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"aabb");
        let calls = transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].operations.len(), 2);
        assert_ne!(
            calls[0].operations[0].source.read_grant.token,
            calls[0].operations[1].source.read_grant.token
        );
    }

    #[test]
    fn same_chunk_in_another_version_refreshes_grant_without_rewriting_identity() {
        let temp = tempfile::tempdir().unwrap();
        let chunk = ChunkId::new("shared");
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: vec![ChunkSources {
                chunk: None,
                chunk_id: chunk.clone(),
                sources: vec![source("a-copy", &chunk, "node-b")],
            }],
        }));
        let transfer = Arc::new(RecordingTransfer::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            transfer.clone(),
            DfsReadConfig::default(),
        );
        let mut batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![ChunkReadOp {
                chunk_id: chunk,
                chunk_offset: 0,
                length: 1,
                output_offset: 0,
            }],
        };
        engine.read_batch(&batch, &mut [0]).unwrap();
        batch.file_version_id = Some(FileVersionId::new("next-version"));
        // A source lookup that returns the old grant must fail rather than rewrite the batch.
        assert!(engine.read_batch(&batch, &mut [0]).is_err());
        sources.reply.lock().unwrap().chunks[0].sources[0]
            .read_grant
            .file_version_id = FileVersionId::new("next-version");
        engine.read_batch(&batch, &mut [0]).unwrap();
        assert_eq!(*sources.calls.lock().unwrap(), 3);
        let calls = transfer.batches.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[1].file_version_id, FileVersionId::new("next-version"));
    }

    struct PartlyStalePeer {
        all_fail: bool,
        fallback_fails: bool,
        attempts: Mutex<Vec<Vec<String>>>,
    }

    impl ChunkTransfer for PartlyStalePeer {
        fn read_ranges(&self, batch: &PeerReadBatch, out: &mut [u8]) -> Result<()> {
            self.attempts.lock().unwrap().push(
                batch
                    .operations
                    .iter()
                    .map(|item| item.source.copy_id.0.clone())
                    .collect(),
            );
            // A real stream may have produced bytes before another range fails.
            out.fill(b'?');
            if self.all_fail
                || batch.operations.len() > 1
                || batch.operations[0].source.copy_id.0 == "b-stale"
                || (self.fallback_fails && batch.operations[0].source.copy_id.0 == "b-good")
            {
                return Err(unavailable("selected copy is stale"));
            }
            out.fill(batch.operations[0].source.copy_id.0.as_bytes()[0]);
            Ok(())
        }
    }

    #[test]
    fn failed_batch_retries_each_original_copy_before_advancing_candidates() {
        for (all_fail, fallback_fails) in [(false, false), (true, false), (false, true)] {
            let temp = tempfile::tempdir().unwrap();
            let a = ChunkId::new("a");
            let b = ChunkId::new("b");
            let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![
                    ChunkSources {
                        chunk: None,
                        chunk_id: a.clone(),
                        sources: vec![source("a-only", &a, "node-x")],
                    },
                    ChunkSources {
                        chunk: None,
                        chunk_id: b.clone(),
                        sources: vec![
                            source("b-stale", &b, "node-x"),
                            source("b-good", &b, "node-y"),
                        ],
                    },
                ],
            }));
            let transfer = Arc::new(PartlyStalePeer {
                all_fail,
                fallback_fails,
                attempts: Mutex::new(Vec::new()),
            });
            let engine = DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources,
                transfer.clone(),
                DfsReadConfig::default(),
            );
            let batch = ReadBatch {
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![
                    ChunkReadOp {
                        chunk_id: a,
                        chunk_offset: 0,
                        length: 1,
                        output_offset: 0,
                    },
                    ChunkReadOp {
                        chunk_id: b,
                        chunk_offset: 0,
                        length: 1,
                        output_offset: 1,
                    },
                ],
            };
            let mut out = [b'!'; 2];
            let result = engine.read_batch(&batch, &mut out);
            let attempts = transfer.attempts.lock().unwrap();
            assert_eq!(attempts[0], vec!["a-only", "b-stale"]);
            assert_eq!(attempts[1], vec!["a-only"]);
            assert_eq!(attempts[2], vec!["b-stale"]);
            if all_fail {
                assert!(result.is_err());
                assert_eq!(attempts.len(), 3);
                assert_eq!(&out, b"!!");
            } else {
                assert_eq!(attempts.len(), 4);
                assert_eq!(attempts[3], vec!["b-good"]);
                if fallback_fails {
                    assert!(result.is_err());
                    assert_eq!(&out, b"!!", "A succeeded but B exhausted its candidates");
                } else {
                    result.unwrap();
                    assert_eq!(&out, b"ab");
                }
            }
        }
    }

    #[test]
    fn read_configuration_is_explicitly_bounded_by_the_wire_contract() {
        let maximum = DfsReadConfig::default();
        maximum.validate().unwrap();
        DfsReadConfig {
            max_ops_per_batch: 1,
            max_inflight_bytes: 1,
            ..maximum.clone()
        }
        .validate()
        .unwrap();
        for config in [
            DfsReadConfig {
                max_ops_per_batch: 0,
                ..maximum.clone()
            },
            DfsReadConfig {
                max_ops_per_batch: MAX_DFS_READ_OPS + 1,
                ..maximum.clone()
            },
            DfsReadConfig {
                max_inflight_bytes: 0,
                ..maximum.clone()
            },
            DfsReadConfig {
                max_inflight_bytes: MAX_DFS_READ_BYTES + 1,
                ..maximum.clone()
            },
        ] {
            assert!(config.validate().is_err());
            let empty = ReadBatch {
                file_version_id: None,
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![],
            };
            assert!(empty.validate_for_output(0, &config).is_err());
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum ExternalAuthorityFault {
        Chunk,
        Location,
        Validation,
        Range,
        GrantVersion,
        MissingReply,
        DuplicateReply,
    }

    struct ExternalFixture {
        chunk: crate::dfs::ChunkObject,
        candidate: SourceCandidate,
        bytes: Mutex<Vec<u8>>,
        authorizations: Mutex<u64>,
        fetches: Mutex<u64>,
        denial: Mutex<bool>,
        expires_at: Mutex<u64>,
        expires_after: Option<Duration>,
        authority_fault: Option<ExternalAuthorityFault>,
        delay: Duration,
    }

    impl ExternalFixture {
        fn new(bytes: &[u8]) -> Self {
            let staged = crate::node::chunk::StagedChunk::new(
                OperationId::new("external-fixture"),
                bytes.to_vec(),
            );
            let mut candidate = source("external-copy", &staged.chunk.id, "node-b");
            candidate.role = CopyRole::ExternalCommitted;
            candidate.data_endpoint = None;
            let object_revision = "immutable-v1".to_owned();
            candidate.location = CopyLocation::External {
                store_id: "store".into(),
                object_key: crate::dfs::external_object_key(
                    "store",
                    &object_revision,
                    &staged.chunk.id,
                )
                .unwrap(),
                object_revision,
            };
            Self {
                chunk: staged.chunk,
                candidate,
                bytes: Mutex::new(bytes.to_vec()),
                authorizations: Mutex::new(0),
                fetches: Mutex::new(0),
                denial: Mutex::new(false),
                expires_at: Mutex::new(u64::MAX),
                expires_after: None,
                authority_fault: None,
                delay: Duration::ZERO,
            }
        }

        fn sources(&self) -> Arc<StaticSources> {
            Arc::new(StaticSources::new(DfsChunkSourcesReply {
                revision: 1,
                chunks: vec![ChunkSources {
                    chunk: Some(self.chunk.clone()),
                    chunk_id: self.chunk.id.clone(),
                    sources: vec![self.candidate.clone()],
                }],
            }))
        }

        fn empty_sources(&self) -> Arc<StaticSources> {
            Arc::new(StaticSources::default())
        }

        fn current_authorizations(
            &self,
            request: crate::dfs::AuthorizeCurrentExternalRead,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            let mut validations = request.validations;
            validations.extend(request.missing_sources.into_iter().map(|missing| {
                crate::dfs::DfsReadValidation {
                    grant: self.candidate.read_grant.clone(),
                    chunk_id: missing.chunk_id,
                    copy_id: self.candidate.copy_id.clone(),
                    chunk_offset: missing.chunk_offset,
                    length: missing.length,
                }
            }));
            self.validate_external_read_grants(&request.caller_id, validations)
        }

        fn batch(&self) -> ReadBatch {
            ReadBatch {
                file_version_id: Some(FileVersionId::new("version")),
                layout_root_id: LayoutRootId::new("layout"),
                ops: vec![ChunkReadOp {
                    chunk_id: self.chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                }],
            }
        }

        fn engine(
            self: &Arc<Self>,
            local: Arc<LocalChunkStore>,
            sources: Arc<StaticSources>,
        ) -> DfsReadEngine {
            DfsReadEngine::new(
                crate::dfs::NamespaceId::new("default"),
                "node-a".into(),
                local,
                sources,
                Arc::new(UnimplementedChunkTransfer),
                DfsReadConfig {
                    source_cache_ttl: Duration::from_secs(60),
                    ..DfsReadConfig::default()
                },
            )
            .with_external_backend(self.clone(), self.clone())
        }
    }

    impl ExternalReadAuthorizer for ExternalFixture {
        fn authorize_external_version_read(
            &self,
            request: AuthorizeExternalVersionRead,
        ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
            let validations = request
                .requests
                .into_iter()
                .map(|op| {
                    let mut grant = self.candidate.read_grant.clone();
                    grant.namespace_id = request.namespace_id.clone();
                    grant.file_version_id = request.file_version_id.clone();
                    grant.layout_root_id = request.layout_root_id.clone();
                    DfsReadValidation {
                        grant,
                        chunk_id: op.chunk_id,
                        copy_id: self.candidate.copy_id.clone(),
                        chunk_offset: op.chunk_offset,
                        length: op.length,
                    }
                })
                .collect();
            let entries = self.validate_external_read_grants(&request.caller_id, validations)?;
            let remaining = self
                .expires_after
                .map_or(5_000, |value| value.as_millis() as u64);
            Ok(Some(AuthorizeExternalVersionReadReply {
                authorizations: entries,
                remaining_validity_ms: remaining,
            }))
        }

        fn validate_external_read_grants(
            &self,
            caller_id: &str,
            validations: Vec<crate::dfs::DfsReadValidation>,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            assert_eq!(caller_id, "node-a");
            *self.authorizations.lock().unwrap() += 1;
            if *self.denial.lock().unwrap() {
                return Err(unavailable("external authorization denied"));
            }
            if matches!(
                self.authority_fault,
                Some(ExternalAuthorityFault::MissingReply)
            ) {
                return Ok(Vec::new());
            }
            let expires_at = self.expires_after.map_or_else(
                || *self.expires_at.lock().unwrap(),
                |duration| now_unix_ms() + duration.as_millis() as u64,
            );
            let mut replies = validations
                .into_iter()
                .map(|validation| {
                    let mut entry = crate::dfs::DfsExternalAuthorizedRead {
                        authorized: crate::dfs::DfsAuthorizedRead {
                            validation,
                            allowed_ranges: vec![(0, self.chunk.length)],
                            expires_at_unix_ms: expires_at,
                        },
                        chunk: self.chunk.clone(),
                        location: self.candidate.location.clone(),
                    };
                    match self.authority_fault {
                        Some(ExternalAuthorityFault::Chunk) => {
                            entry.chunk.content_digest.bytes[0] ^= 1
                        }
                        Some(ExternalAuthorityFault::Location) => {
                            entry.location = CopyLocation::External {
                                store_id: "another-store".into(),
                                object_key: "generation/chunk".into(),
                                object_revision: "immutable-v1".into(),
                            }
                        }
                        Some(ExternalAuthorityFault::Validation) => {
                            entry.authorized.validation.copy_id = CopyId::new("another-copy")
                        }
                        Some(ExternalAuthorityFault::Range) => {
                            entry.authorized.allowed_ranges = vec![(0, 1)]
                        }
                        Some(ExternalAuthorityFault::GrantVersion) => {
                            entry.authorized.validation.grant.file_version_id =
                                FileVersionId::new("next-version")
                        }
                        _ => {}
                    }
                    entry
                })
                .collect::<Vec<_>>();
            if matches!(
                self.authority_fault,
                Some(ExternalAuthorityFault::DuplicateReply)
            ) && let Some(first) = replies.first().cloned()
            {
                replies.push(first);
            }
            Ok(replies)
        }
    }

    impl ExternalChunkReader for ExternalFixture {
        fn read_chunk(
            &self,
            chunk: &crate::dfs::ChunkObject,
            source: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            assert_eq!(chunk, &self.chunk);
            assert_eq!(source.location, self.candidate.location);
            *self.fetches.lock().unwrap() += 1;
            std::thread::sleep(self.delay);
            VerifiedExternalChunk::verify(chunk, self.bytes.lock().unwrap().clone())
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum CurrentReadMode {
        Authorized,
        AuthorizedWrongInode,
        Denied,
        HeadChanged,
        HeadChangedSameHead,
        DeadlineExceeded,
    }

    struct CurrentReadAuthorizer {
        fixture: Arc<ExternalFixture>,
        mode: CurrentReadMode,
        legacy_calls: Mutex<u64>,
        current_calls: Mutex<u64>,
        last_validations: Mutex<usize>,
        last_missing_sources: Mutex<usize>,
        legacy_fallback_allowed: bool,
    }

    impl CurrentReadAuthorizer {
        fn new(
            fixture: Arc<ExternalFixture>,
            mode: CurrentReadMode,
            legacy_fallback_allowed: bool,
        ) -> Self {
            Self {
                fixture,
                mode,
                legacy_calls: Mutex::new(0),
                current_calls: Mutex::new(0),
                last_validations: Mutex::new(0),
                last_missing_sources: Mutex::new(0),
                legacy_fallback_allowed,
            }
        }

        fn legacy_calls(&self) -> u64 {
            *self.legacy_calls.lock().unwrap()
        }

        fn current_calls(&self) -> u64 {
            *self.current_calls.lock().unwrap()
        }

        fn last_request_shape(&self) -> (usize, usize) {
            (
                *self.last_validations.lock().unwrap(),
                *self.last_missing_sources.lock().unwrap(),
            )
        }
    }

    impl ExternalReadAuthorizer for CurrentReadAuthorizer {
        fn authorize_current_external_read(
            &self,
            request: crate::dfs::AuthorizeCurrentExternalRead,
        ) -> Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
            *self.current_calls.lock().unwrap() += 1;
            assert_eq!(request.caller_id, "node-a");
            assert_eq!(
                request.namespace_id,
                crate::dfs::NamespaceId::new("default")
            );
            assert_eq!(request.expected_head_version, FileVersionId::new("version"));
            assert_eq!(request.layout_root_id, LayoutRootId::new("layout"));
            *self.last_validations.lock().unwrap() = request.validations.len();
            *self.last_missing_sources.lock().unwrap() = request.missing_sources.len();
            assert!(request.validations.len() + request.missing_sources.len() > 0);
            for validation in &request.validations {
                assert_eq!(validation.chunk_id, self.fixture.chunk.id);
            }
            for missing in &request.missing_sources {
                assert_eq!(missing.chunk_id, self.fixture.chunk.id);
            }
            match self.mode {
                CurrentReadMode::Authorized => {
                    let authorizations = self.fixture.current_authorizations(request)?;
                    Ok(Some(
                        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                            inode: inode_with_head("version"),
                            authorizations,
                        },
                    ))
                }
                CurrentReadMode::AuthorizedWrongInode => {
                    let authorizations = self.fixture.current_authorizations(request)?;
                    let mut inode = inode_with_head("version");
                    inode.inode_id = crate::dfs::InodeId::new("inode:other");
                    Ok(Some(
                        crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                            inode,
                            authorizations,
                        },
                    ))
                }
                CurrentReadMode::Denied => Err(external_denied(
                    "current external read authorization denied",
                )),
                CurrentReadMode::DeadlineExceeded => Err(Error::coded(
                    afs_error::CLIENT_DEADLINE_EXCEEDED,
                    "current external read authorization timed out",
                )),
                CurrentReadMode::HeadChanged => Ok(Some(
                    crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode_with_head(
                        "next-version",
                    )),
                )),
                CurrentReadMode::HeadChangedSameHead => Ok(Some(
                    crate::dfs::AuthorizeCurrentExternalReadReply::HeadChanged(inode_with_head(
                        "version",
                    )),
                )),
            }
        }

        fn validate_external_read_grants(
            &self,
            caller_id: &str,
            validations: Vec<crate::dfs::DfsReadValidation>,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            *self.legacy_calls.lock().unwrap() += 1;
            if self.legacy_fallback_allowed {
                self.fixture
                    .validate_external_read_grants(caller_id, validations)
            } else {
                Err(unavailable(
                    "legacy external read authorization must not be used",
                ))
            }
        }
    }

    fn prime_external_source_cache(engine: &DfsReadEngine, fixture: &ExternalFixture) {
        prime_external_source_cache_with(
            engine,
            fixture,
            fixture.candidate.clone(),
            Duration::from_secs(60),
        );
    }

    fn prime_external_source_cache_with(
        engine: &DfsReadEngine,
        fixture: &ExternalFixture,
        source: SourceCandidate,
        ttl: Duration,
    ) {
        engine.source_cache.lock().unwrap().insert(
            (
                FileVersionId::new("version"),
                LayoutRootId::new("layout"),
                fixture.chunk.id.clone(),
            ),
            CachedSources {
                expires_at: Instant::now() + ttl,
                sources: ChunkSources {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk: Some(fixture.chunk.clone()),
                    sources: vec![source],
                },
            },
        );
    }

    struct CurrentExternalChunks {
        fixtures: Vec<Arc<ExternalFixture>>,
        current_calls: Mutex<u64>,
        last_validations: Mutex<usize>,
        last_missing_sources: Mutex<usize>,
    }

    impl CurrentExternalChunks {
        fn new(fixtures: Vec<Arc<ExternalFixture>>) -> Self {
            Self {
                fixtures,
                current_calls: Mutex::new(0),
                last_validations: Mutex::new(0),
                last_missing_sources: Mutex::new(0),
            }
        }

        fn current_calls(&self) -> u64 {
            *self.current_calls.lock().unwrap()
        }

        fn last_request_shape(&self) -> (usize, usize) {
            (
                *self.last_validations.lock().unwrap(),
                *self.last_missing_sources.lock().unwrap(),
            )
        }
    }

    impl ExternalReadAuthorizer for CurrentExternalChunks {
        fn authorize_current_external_read(
            &self,
            request: crate::dfs::AuthorizeCurrentExternalRead,
        ) -> Result<Option<crate::dfs::AuthorizeCurrentExternalReadReply>> {
            *self.current_calls.lock().unwrap() += 1;
            *self.last_validations.lock().unwrap() = request.validations.len();
            *self.last_missing_sources.lock().unwrap() = request.missing_sources.len();
            let mut replies = Vec::new();
            let mut validations = request.validations;
            validations.extend(request.missing_sources.into_iter().map(|missing| {
                let fixture = self
                    .fixtures
                    .iter()
                    .find(|fixture| fixture.chunk.id == missing.chunk_id)
                    .unwrap();
                crate::dfs::DfsReadValidation {
                    grant: fixture.candidate.read_grant.clone(),
                    chunk_id: missing.chunk_id,
                    copy_id: fixture.candidate.copy_id.clone(),
                    chunk_offset: missing.chunk_offset,
                    length: missing.length,
                }
            }));
            for validation in validations {
                let fixture = self
                    .fixtures
                    .iter()
                    .find(|fixture| fixture.chunk.id == validation.chunk_id)
                    .unwrap();
                replies.extend(
                    fixture.validate_external_read_grants(&request.caller_id, vec![validation])?,
                );
            }
            Ok(Some(
                crate::dfs::AuthorizeCurrentExternalReadReply::Authorized {
                    inode: inode_with_head("version"),
                    authorizations: replies,
                },
            ))
        }

        fn validate_external_read_grants(
            &self,
            _caller_id: &str,
            _validations: Vec<crate::dfs::DfsReadValidation>,
        ) -> Result<Vec<crate::dfs::DfsExternalAuthorizedRead>> {
            Err(unavailable(
                "legacy external read authorization must not be used",
            ))
        }
    }

    impl ExternalChunkReader for CurrentExternalChunks {
        fn read_chunk(
            &self,
            chunk: &ChunkObject,
            source: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            self.fixtures
                .iter()
                .find(|fixture| fixture.chunk.id == chunk.id)
                .unwrap()
                .read_chunk(chunk, source)
        }
    }

    fn inode_with_head(version_id: &str) -> crate::dfs::InodeRecord {
        crate::dfs::InodeRecord {
            namespace_id: crate::dfs::NamespaceId::new("default"),
            inode_id: crate::dfs::InodeId::new("inode:test"),
            kind: crate::dfs::InodeKind::Regular,
            attributes: crate::dfs::InodeAttributes {
                mode: 0o640,
                uid: 1000,
                gid: 1000,
                nlink: 1,
                atime_unix_ms: 1,
                mtime_unix_ms: 1,
                ctime_unix_ms: 1,
            },
            head_version: Some(FileVersionId::new(version_id)),
            symlink_target: None,
            xattrs: Default::default(),
            revision: 1,
        }
    }

    #[test]
    fn external_backend_uses_current_head_authorization_when_available() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            false,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());
        prime_external_source_cache(&engine, &fixture);

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"bc");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(authorizer.last_request_shape(), (1, 0));
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_read_resolves_missing_source_without_source_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.empty_sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            false,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"bc");
        assert_eq!(authorizer.last_request_shape(), (0, 1));
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_read_resolves_each_missing_range_on_same_chunk() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.empty_sources();
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Authorized,
            false,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources.clone(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk_offset: 3,
                    length: 2,
                    output_offset: 2,
                },
            ],
        };

        let mut out = [b'!'; 4];
        let outcome = engine
            .read_batch_current_external(&crate::dfs::InodeId::new("inode:test"), &batch, &mut out)
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"bcde");
        assert_eq!(authorizer.last_request_shape(), (0, 2));
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_read_mixes_cached_and_missing_sources_in_original_output_order() {
        let temp = tempfile::tempdir().unwrap();
        let cached = Arc::new(ExternalFixture::new(b"abcdef"));
        let missing = Arc::new(ExternalFixture::new(b"uvwxyz"));
        let chunks = Arc::new(CurrentExternalChunks::new(vec![
            cached.clone(),
            missing.clone(),
        ]));
        let sources = Arc::new(StaticSources::default());
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig {
                source_cache_ttl: Duration::from_secs(60),
                ..DfsReadConfig::default()
            },
        )
        .with_external_backend(chunks.clone(), chunks.clone());
        prime_external_source_cache(&engine, &cached);
        let batch = ReadBatch {
            file_version_id: Some(FileVersionId::new("version")),
            layout_root_id: LayoutRootId::new("layout"),
            ops: vec![
                ChunkReadOp {
                    chunk_id: missing.chunk.id.clone(),
                    chunk_offset: 1,
                    length: 2,
                    output_offset: 0,
                },
                ChunkReadOp {
                    chunk_id: cached.chunk.id.clone(),
                    chunk_offset: 2,
                    length: 2,
                    output_offset: 2,
                },
                ChunkReadOp {
                    chunk_id: missing.chunk.id.clone(),
                    chunk_offset: 3,
                    length: 2,
                    output_offset: 4,
                },
            ],
        };

        let mut out = [b'!'; 6];
        let outcome = engine
            .read_batch_current_external(&crate::dfs::InodeId::new("inode:test"), &batch, &mut out)
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Read);
        assert_eq!(&out, b"vwcdxy");
        assert_eq!(chunks.current_calls(), 1);
        assert_eq!(chunks.last_request_shape(), (1, 2));
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*cached.fetches.lock().unwrap(), 1);
        assert_eq!(*missing.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_current_missing_authority_faults_do_not_fetch_or_fallback() {
        for fault in [
            ExternalAuthorityFault::Location,
            ExternalAuthorityFault::Range,
            ExternalAuthorityFault::GrantVersion,
            ExternalAuthorityFault::MissingReply,
            ExternalAuthorityFault::DuplicateReply,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let fixture = Arc::new(ExternalFixture {
                authority_fault: Some(fault),
                ..ExternalFixture::new(b"abcdef")
            });
            let sources = fixture.empty_sources();
            let authorizer = Arc::new(CurrentReadAuthorizer::new(
                fixture.clone(),
                CurrentReadMode::Authorized,
                true,
            ));
            let engine = fixture
                .engine(
                    Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                    sources.clone(),
                )
                .with_external_backend(fixture.clone(), authorizer.clone());

            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_current_external(
                        &crate::dfs::InodeId::new("inode:test"),
                        &fixture.batch(),
                        &mut out,
                    )
                    .is_err(),
                "fault {fault:?}"
            );

            assert_eq!(&out, b"!!", "fault {fault:?}");
            assert_eq!(authorizer.current_calls(), 1, "fault {fault:?}");
            assert_eq!(authorizer.legacy_calls(), 0, "fault {fault:?}");
            assert_eq!(authorizer.last_request_shape(), (0, 1), "fault {fault:?}");
            assert_eq!(*sources.calls.lock().unwrap(), 0, "fault {fault:?}");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0, "fault {fault:?}");
        }
    }

    #[test]
    fn external_backend_current_cached_invalid_grant_is_not_resolved_as_missing() {
        for case in 0..4 {
            let temp = tempfile::tempdir().unwrap();
            let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
            let sources = fixture.sources();
            let authorizer = Arc::new(CurrentReadAuthorizer::new(
                fixture.clone(),
                CurrentReadMode::Denied,
                true,
            ));
            let engine = fixture
                .engine(
                    Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                    sources.clone(),
                )
                .with_external_backend(fixture.clone(), authorizer.clone());
            let mut source = fixture.candidate.clone();
            match case {
                0 => source.read_grant.namespace_id = crate::dfs::NamespaceId::new("other"),
                1 => source.read_grant.caller_node_epoch = 0,
                2 => source.read_grant.expires_at_unix_ms = 0,
                _ => source.read_grant.token.clear(),
            }
            prime_external_source_cache_with(&engine, &fixture, source, Duration::from_secs(60));

            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_current_external(
                        &crate::dfs::InodeId::new("inode:test"),
                        &fixture.batch(),
                        &mut out,
                    )
                    .is_err(),
                "case {case}"
            );

            assert_eq!(&out, b"!!", "case {case}");
            assert_eq!(authorizer.current_calls(), 1, "case {case}");
            assert_eq!(authorizer.legacy_calls(), 0, "case {case}");
            assert_eq!(authorizer.last_request_shape(), (1, 0), "case {case}");
            assert_eq!(*sources.calls.lock().unwrap(), 0, "case {case}");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0, "case {case}");
        }
    }

    #[test]
    fn external_backend_current_auth_errors_do_not_fallback_or_fetch() {
        for mode in [CurrentReadMode::Denied, CurrentReadMode::DeadlineExceeded] {
            let temp = tempfile::tempdir().unwrap();
            let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
            let sources = fixture.empty_sources();
            let authorizer = Arc::new(CurrentReadAuthorizer::new(fixture.clone(), mode, true));
            let engine = fixture
                .engine(
                    Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                    sources.clone(),
                )
                .with_external_backend(fixture.clone(), authorizer.clone());

            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_current_external(
                        &crate::dfs::InodeId::new("inode:test"),
                        &fixture.batch(),
                        &mut out,
                    )
                    .is_err(),
                "mode {mode:?}"
            );

            assert_eq!(&out, b"!!", "mode {mode:?}");
            assert_eq!(authorizer.current_calls(), 1, "mode {mode:?}");
            assert_eq!(authorizer.legacy_calls(), 0, "mode {mode:?}");
            assert_eq!(authorizer.last_request_shape(), (0, 1), "mode {mode:?}");
            assert_eq!(*sources.calls.lock().unwrap(), 0, "mode {mode:?}");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0, "mode {mode:?}");
        }
    }

    #[test]
    fn external_backend_current_authorization_denial_is_not_legacy_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::Denied,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err()
        );

        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_head_changed_does_not_consume_speculative_plan() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::HeadChanged,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert!(matches!(
            outcome,
            CurrentExternalReadOutcome::HeadChanged(inode)
                if inode.head_version == Some(FileVersionId::new("next-version"))
        ));
        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_authorized_wrong_inode_is_rejected_without_output() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::AuthorizedWrongInode,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err()
        );

        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_fake_head_changed_is_rejected_without_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authorizer = Arc::new(CurrentReadAuthorizer::new(
            fixture.clone(),
            CurrentReadMode::HeadChangedSameHead,
            true,
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), authorizer.clone());

        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_current_external(
                    &crate::dfs::InodeId::new("inode:test"),
                    &fixture.batch(),
                    &mut out,
                )
                .is_err()
        );

        assert_eq!(&out, b"!!");
        assert_eq!(authorizer.current_calls(), 1);
        assert_eq!(authorizer.legacy_calls(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_current_read_reports_unsupported_without_legacy_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );

        let mut out = [b'!'; 2];
        let outcome = engine
            .read_batch_current_external(
                &crate::dfs::InodeId::new("inode:test"),
                &fixture.batch(),
                &mut out,
            )
            .unwrap();

        assert_eq!(outcome, CurrentExternalReadOutcome::Unsupported);
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_rejects_verified_content_for_another_chunk() {
        struct WrongChunk(VerifiedExternalChunk);
        impl ExternalChunkReader for WrongChunk {
            fn read_chunk(
                &self,
                _: &ChunkObject,
                _: &SourceCandidate,
            ) -> Result<VerifiedExternalChunk> {
                Ok(self.0.clone())
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let other = crate::node::chunk::StagedChunk::new(
            crate::dfs::OperationId::new("other"),
            b"ghijkl".to_vec(),
        );
        let verified = VerifiedExternalChunk::verify(&other.chunk, other.bytes().to_vec()).unwrap();
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(Arc::new(WrongChunk(verified)), fixture.clone());
        let mut out = [b'!'; 2];
        let error = engine.read_batch(&fixture.batch(), &mut out).unwrap_err();
        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_refuses_invalid_authority_before_fetch() {
        for failure in 0..4 {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            match failure {
                0 => *fixture.expires_at.lock().unwrap() = 1,
                1 => fixture.chunk.length = MAX_DFS_EXTERNAL_CHUNK_BYTES + 1,
                2 => fixture.chunk.id = ChunkId::new("unexpected-chunk"),
                _ => {
                    fixture.candidate.location = CopyLocation::Node {
                        node_id: "node-b".into(),
                        node_epoch: 1,
                        device_id: "local-0".into(),
                        device_epoch: 1,
                        catalog_revision: 1,
                    }
                }
            }
            let fixture = Arc::new(fixture);
            let mut batch = fixture.batch();
            batch.ops[0].chunk_id = fixture.candidate.chunk_id.clone();
            let sources = fixture.sources();
            sources.reply.lock().unwrap().chunks[0].chunk_id = fixture.candidate.chunk_id.clone();
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                sources,
            );
            let mut out = [b'!'; 2];
            assert!(engine.read_batch(&batch, &mut out).is_err());
            assert_eq!(&out, b"!!");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        }
    }

    #[test]
    fn external_backend_rechecks_authority_after_fetch() {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.delay = Duration::from_millis(150);
        fixture.expires_after = Some(Duration::from_millis(100));
        let fixture = Arc::new(fixture);
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut out = [b'!'; 2];
        assert!(engine.read_batch(&fixture.batch(), &mut out).is_err());
        assert_eq!(&out, b"!!");
        assert_eq!(*fixture.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_rejects_changed_authorization_identity_and_range() {
        for fault in [
            ExternalAuthorityFault::Chunk,
            ExternalAuthorityFault::Location,
            ExternalAuthorityFault::Validation,
            ExternalAuthorityFault::Range,
            ExternalAuthorityFault::MissingReply,
        ] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            fixture.authority_fault = Some(fault);
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            let mut out = [b'!'; 2];
            assert!(engine.read_batch(&fixture.batch(), &mut out).is_err());
            assert_eq!(&out, b"!!");
            assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        }
    }

    struct ExternalChunks(Vec<Arc<ExternalFixture>>);

    impl ExternalReadAuthorizer for ExternalChunks {
        fn validate_external_read_grants(
            &self,
            caller_id: &str,
            validations: Vec<DfsReadValidation>,
        ) -> Result<Vec<DfsExternalAuthorizedRead>> {
            let mut replies = Vec::new();
            for validation in validations {
                let fixture = self
                    .0
                    .iter()
                    .find(|fixture| fixture.chunk.id == validation.chunk_id)
                    .unwrap();
                replies.extend(fixture.validate_external_read_grants(caller_id, vec![validation])?);
            }
            Ok(replies)
        }
    }

    impl ExternalChunkReader for ExternalChunks {
        fn read_chunk(
            &self,
            chunk: &ChunkObject,
            source: &SourceCandidate,
        ) -> Result<VerifiedExternalChunk> {
            self.0
                .iter()
                .find(|fixture| fixture.chunk.id == chunk.id)
                .unwrap()
                .read_chunk(chunk, source)
        }
    }

    #[test]
    fn external_backend_late_chunk_failure_never_publishes_earlier_success() {
        let temp = tempfile::tempdir().unwrap();
        let first = Arc::new(ExternalFixture::new(b"abcdef"));
        let second = Arc::new(ExternalFixture::new(b"uvwxyz"));
        second.bytes.lock().unwrap()[5] ^= 1;
        let chunks = Arc::new(ExternalChunks(vec![first.clone(), second.clone()]));
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: [&first, &second]
                .iter()
                .map(|fixture| ChunkSources {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk: Some(fixture.chunk.clone()),
                    sources: vec![fixture.candidate.clone()],
                })
                .collect(),
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_external_backend(chunks.clone(), chunks);
        let mut batch = first.batch();
        let mut last_op = second.batch().ops.remove(0);
        last_op.output_offset = 2;
        batch.ops.push(last_op);
        let mut out = [b'!'; 4];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!!!");
        assert_eq!(*first.fetches.lock().unwrap(), 1);
        assert_eq!(*second.fetches.lock().unwrap(), 1);
    }

    #[test]
    fn external_backend_does_not_start_another_get_after_authority_expires() {
        let temp = tempfile::tempdir().unwrap();
        let mut first = ExternalFixture::new(b"abcdef");
        first.delay = Duration::from_millis(150);
        first.expires_after = Some(Duration::from_millis(100));
        let first = Arc::new(first);
        let second = Arc::new(ExternalFixture::new(b"uvwxyz"));
        let chunks = Arc::new(ExternalChunks(vec![first.clone(), second.clone()]));
        let sources = Arc::new(StaticSources::new(DfsChunkSourcesReply {
            revision: 1,
            chunks: [&first, &second]
                .iter()
                .map(|fixture| ChunkSources {
                    chunk_id: fixture.chunk.id.clone(),
                    chunk: Some(fixture.chunk.clone()),
                    sources: vec![fixture.candidate.clone()],
                })
                .collect(),
        }));
        let engine = DfsReadEngine::new(
            crate::dfs::NamespaceId::new("default"),
            "node-a".into(),
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources,
            Arc::new(UnimplementedChunkTransfer),
            DfsReadConfig::default(),
        )
        .with_external_backend(chunks.clone(), chunks);
        let mut batch = first.batch();
        let mut last_op = second.batch().ops.remove(0);
        last_op.output_offset = 2;
        batch.ops.push(last_op);
        let mut out = [b'!'; 4];
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!!!");
        assert_eq!(*first.fetches.lock().unwrap(), 1);
        assert_eq!(*second.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_new_version_requires_its_original_grant() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.sources();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        );
        let mut batch = fixture.batch();
        let mut out = [b'!'; 2];
        engine.read_batch(&batch, &mut out).unwrap();
        batch.file_version_id = Some(FileVersionId::new("next-version"));
        out.fill(b'!');
        assert!(engine.read_batch(&batch, &mut out).is_err());
        assert_eq!(&out, b"!!");
        sources.reply.lock().unwrap().chunks[0].sources[0]
            .read_grant
            .file_version_id = FileVersionId::new("next-version");
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(&out, b"bc");
        assert_eq!(*sources.calls.lock().unwrap(), 3);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 2);
        assert_eq!(
            *fixture.fetches.lock().unwrap(),
            2,
            "S1 has no content cache"
        );
    }

    #[test]
    fn external_version_owned_result_overlapping_groups_use_borrowed_fallback() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut batch = fixture.batch();
        let mut middle = batch.ops[0].clone();
        middle.chunk_id = ChunkId::new("other-object");
        batch.ops.push(middle);
        batch.ops.push(batch.ops[0].clone());
        assert!(
            engine
                .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 2,)
                .unwrap()
                .is_none(),
            "A/B/A overlap must retain original scatter ordering"
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_version_owned_result_holes_still_require_live_authority() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_read_authority(authority.clone());
        let mut batch = fixture.batch();
        batch.ops.clear();
        assert_eq!(
            engine
                .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 4,)
                .unwrap()
                .unwrap(),
            [0; 4]
        );
        authority.stop();
        assert!(
            engine
                .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 4,)
                .is_err()
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
    }

    #[test]
    fn external_version_owned_result_preserves_sparse_offsets_and_tail() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let mut batch = fixture.batch();
        batch.ops[0].output_offset = 1;
        let mut tail = batch.ops[0].clone();
        tail.chunk_offset = 7;
        tail.length = 1;
        tail.output_offset = 5;
        batch.ops.push(tail);
        let result = engine
            .read_batch_external_version_owned(&crate::dfs::InodeId::new("inode"), &batch, 7)
            .unwrap()
            .unwrap();
        assert_eq!(result, b"\0bc\0\0h\0");
        let mut borrowed = [b'!'; 7];
        engine
            .read_batch_external_version(&crate::dfs::InodeId::new("inode"), &batch, &mut borrowed)
            .unwrap();
        assert_eq!(&borrowed, b"!bc!!h!");
    }

    #[test]
    fn external_version_owned_result_rejects_oversized_allocation_before_fetch() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        assert!(
            engine
                .read_batch_external_version_owned(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    MAX_DFS_READ_BYTES as usize + 1,
                )
                .is_err()
        );
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
    }

    #[test]
    fn external_version_owned_result_final_checks_do_not_expose_bytes() {
        for corrupt_bytes in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            if corrupt_bytes {
                fixture.bytes.lock().unwrap()[5] ^= 1; // Outside requested range.
            } else {
                fixture.expires_after = Some(Duration::from_millis(20));
                fixture.delay = Duration::from_millis(60);
            }
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            assert!(
                engine
                    .read_batch_external_version_owned(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        2,
                    )
                    .is_err()
            );
            assert_eq!(*fixture.fetches.lock().unwrap(), 1);
        }
    }

    #[test]
    fn external_version_owned_result_stop_during_get_stays_private() {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.delay = Duration::from_millis(100);
        let fixture = Arc::new(fixture);
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_read_authority(authority.clone());
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while *fixture.fetches.lock().unwrap() == 0 {
                    std::thread::yield_now();
                }
                authority.stop();
            });
            assert!(
                engine
                    .read_batch_external_version_owned(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        2,
                    )
                    .is_err()
            );
        });
    }

    #[test]
    fn external_version_reuses_ranges_and_separates_inode_version_layout() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdefgh"));
        let sources = fixture.sources();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        );
        let inode = crate::dfs::InodeId::new("inode");
        let mut batch = fixture.batch();
        let mut out = [0; 2];
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(&out, b"bc");
        batch.ops[0].chunk_offset = 4;
        *fixture.denial.lock().unwrap() = true; // Meta disconnected after valid admission.
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(&out, b"ef");
        assert_eq!(*fixture.authorizations.lock().unwrap(), 1);
        assert_eq!(
            *sources.calls.lock().unwrap(),
            0,
            "source TTL must not force an RPC"
        );
        for change in 0..3 {
            let mut other = batch.clone();
            let key = if change == 0 {
                crate::dfs::InodeId::new("other")
            } else {
                inode.clone()
            };
            if change == 1 {
                other.file_version_id = Some(FileVersionId::new("other"));
            }
            if change == 2 {
                other.layout_root_id = LayoutRootId::new("other");
            }
            out.fill(b'!');
            assert!(
                engine
                    .read_batch_external_version(&key, &other, &mut out)
                    .is_err()
            );
            assert_eq!(out, [b'!'; 2]);
        }
    }

    #[test]
    fn external_version_monotonic_expiry_never_renews_on_hit() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        let inode = crate::dfs::InodeId::new("inode");
        let batch = fixture.batch();
        let mut out = [0; 2];
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        let original = engine
            .external_authority_cache
            .lock()
            .unwrap()
            .entries
            .values()
            .next()
            .unwrap()
            .deadline;
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(
            engine
                .external_authority_cache
                .lock()
                .unwrap()
                .entries
                .values()
                .next()
                .unwrap()
                .deadline,
            original
        );
        {
            let mut cache = engine.external_authority_cache.lock().unwrap();
            let entry = Arc::get_mut(cache.entries.values_mut().next().unwrap()).unwrap();
            entry.deadline = Instant::now() - Duration::from_millis(1);
            entry.expires_at_unix_ms = u64::MAX; // A backwards wall clock cannot extend monotonic authority.
        }
        *fixture.denial.lock().unwrap() = true;
        out.fill(b'!');
        assert!(
            engine
                .read_batch_external_version(&inode, &batch, &mut out)
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
        *fixture.denial.lock().unwrap() = false;
        engine
            .read_batch_external_version(&inode, &batch, &mut out)
            .unwrap();
        assert_eq!(*fixture.authorizations.lock().unwrap(), 3);
    }

    #[test]
    fn external_version_stop_during_get_prevents_output_and_cannot_resurrect() {
        let temp = tempfile::tempdir().unwrap();
        let mut fixture = ExternalFixture::new(b"abcdef");
        fixture.delay = Duration::from_millis(100);
        let fixture = Arc::new(fixture);
        let authority = Arc::new(LocalReadAuthority::new(
            1,
            Instant::now() + Duration::from_secs(30),
        ));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_read_authority(authority.clone());
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while *fixture.fetches.lock().unwrap() == 0 {
                    std::thread::yield_now();
                }
                authority.stop();
            });
            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_external_version(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        &mut out
                    )
                    .is_err()
            );
            assert_eq!(out, [b'!'; 2]);
        });
        assert!(
            authority
                .refresh(1, Instant::now() + Duration::from_secs(30))
                .is_err()
        );
    }

    #[test]
    fn external_version_expiring_fetch_and_bad_range_do_not_publish() {
        for fault in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            if fault {
                fixture.authority_fault = Some(ExternalAuthorityFault::Range);
            } else {
                fixture.expires_after = Some(Duration::from_millis(20));
                fixture.delay = Duration::from_millis(60);
            }
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            let mut out = [b'!'; 2];
            assert!(
                engine
                    .read_batch_external_version(
                        &crate::dfs::InodeId::new("inode"),
                        &fixture.batch(),
                        &mut out
                    )
                    .is_err()
            );
            assert_eq!(out, [b'!'; 2]);
            assert_eq!(*fixture.fetches.lock().unwrap(), if fault { 0 } else { 1 });
        }
    }

    #[test]
    fn external_version_metadata_budget_includes_pinned_entries_and_reclaims_on_drop() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            fixture.sources(),
        );
        engine
            .read_batch_external_version(
                &crate::dfs::InodeId::new("inode"),
                &fixture.batch(),
                &mut [0; 2],
            )
            .unwrap();
        let mut cache = engine.external_authority_cache.lock().unwrap();
        let pinned = cache.entries.values().next().unwrap().clone();
        let budget = cache.budget.clone();
        let charged = budget.lock().unwrap().bytes;
        assert!(charged > 0 && charged <= MAX_EXTERNAL_AUTH_CACHE_BYTES);
        cache.invalidate_inode_version(
            &crate::dfs::NamespaceId::new("default"),
            &crate::dfs::InodeId::new("inode"),
            &FileVersionId::new("version"),
        );
        assert!(pinned.invalidated.load(Ordering::Acquire));
        assert_eq!(
            budget.lock().unwrap().bytes,
            charged,
            "eviction cannot free a pinned allocation"
        );
        drop(pinned);
        assert_eq!(budget.lock().unwrap().bytes, 0);
        assert_eq!(budget.lock().unwrap().entries, 0);
    }

    #[test]
    fn external_version_cache_caps_entries_and_bytes_while_pinned() {
        for large_metadata in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut fixture = ExternalFixture::new(b"abcdef");
            // Exercise budget accounting independently of trusted transport decoding.
            if large_metadata {
                fixture.candidate.read_grant.token = "x".repeat(16_384);
            }
            let fixture = Arc::new(fixture);
            let engine = fixture.engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            );
            let mut pins = Vec::new();
            for i in 0..=MAX_EXTERNAL_AUTH_CACHE_ENTRIES {
                let inode = crate::dfs::InodeId::new(format!("inode-{i}"));
                engine
                    .read_batch_external_version(&inode, &fixture.batch(), &mut [0; 2])
                    .unwrap();
                let cache = engine.external_authority_cache.lock().unwrap();
                if let Some(entry) = cache
                    .entries
                    .values()
                    .find(|entry| entry.key.inode_id == inode)
                {
                    pins.push(entry.clone());
                }
                let budget = cache.budget.lock().unwrap();
                assert!(budget.entries <= MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
                assert!(budget.bytes <= MAX_EXTERNAL_AUTH_CACHE_BYTES);
            }
            assert_eq!(
                *fixture.authorizations.lock().unwrap(),
                (MAX_EXTERNAL_AUTH_CACHE_ENTRIES + 1) as u64
            );
            let mut cache = engine.external_authority_cache.lock().unwrap();
            if large_metadata {
                assert!(cache.entries.len() < MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
            } else {
                assert_eq!(cache.entries.len(), MAX_EXTERNAL_AUTH_CACHE_ENTRIES);
            }
            cache.entries.clear();
            assert!(cache.budget.lock().unwrap().bytes > 0);
            drop(pins);
            assert_eq!(cache.budget.lock().unwrap().bytes, 0);
        }
    }

    #[test]
    fn external_version_delayed_authorization_reply_cannot_reset_deadline() {
        struct Delayed(Arc<ExternalFixture>);
        impl ExternalReadAuthorizer for Delayed {
            fn authorize_external_version_read(
                &self,
                request: AuthorizeExternalVersionRead,
            ) -> Result<Option<AuthorizeExternalVersionReadReply>> {
                let mut reply = self.0.authorize_external_version_read(request)?.unwrap();
                reply.remaining_validity_ms = 20;
                std::thread::sleep(Duration::from_millis(50));
                Ok(Some(reply))
            }
            fn validate_external_read_grants(
                &self,
                caller: &str,
                validations: Vec<DfsReadValidation>,
            ) -> Result<Vec<DfsExternalAuthorizedRead>> {
                self.0.validate_external_read_grants(caller, validations)
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let engine = fixture
            .engine(
                Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
                fixture.sources(),
            )
            .with_external_backend(fixture.clone(), Arc::new(Delayed(fixture.clone())));
        let mut out = [b'!'; 2];
        assert!(
            engine
                .read_batch_external_version(
                    &crate::dfs::InodeId::new("inode"),
                    &fixture.batch(),
                    &mut out
                )
                .is_err()
        );
        assert_eq!(out, [b'!'; 2]);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }

    #[test]
    fn external_backend_preserves_holes_without_requesting_a_source() {
        let temp = tempfile::tempdir().unwrap();
        let fixture = Arc::new(ExternalFixture::new(b"abcdef"));
        let sources = fixture.sources();
        let engine = fixture.engine(
            Arc::new(LocalChunkStore::open(temp.path(), "node-a").unwrap()),
            sources.clone(),
        );
        let mut batch = fixture.batch();
        batch.ops.clear();
        let mut out = [0; 4];
        engine.read_batch(&batch, &mut out).unwrap();
        assert_eq!(out, [0; 4]);
        assert_eq!(*sources.calls.lock().unwrap(), 0);
        assert_eq!(*fixture.authorizations.lock().unwrap(), 0);
        assert_eq!(*fixture.fetches.lock().unwrap(), 0);
    }
}
