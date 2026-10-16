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
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
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

#[derive(Debug, Default)]
pub struct ChunkBuilder {
    bytes: Vec<u8>,
}

impl ChunkBuilder {
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        let offset = usize::try_from(offset).map_err(|_| invalid("chunk offset is too large"))?;
        let end = offset
            .checked_add(data.len())
            .ok_or_else(|| invalid("chunk write range overflow"))?;
        if end > self.bytes.len() {
            self.bytes.resize(end, 0);
        }
        self.bytes[offset..end].copy_from_slice(data);
        Ok(data.len())
    }

    pub fn replace(&mut self, bytes: Vec<u8>) {
        self.bytes = bytes;
    }

    pub fn stage(self, operation_id: OperationId) -> StagedChunk {
        StagedChunk::new(operation_id, self.bytes)
    }
}

/// Shared producer/replica budget per immutable staged chunk, never a file limit.
#[cfg(feature = "dfs")]
pub(crate) const MAX_STAGED_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct StagedChunk {
    pub operation_id: OperationId,
    pub chunk: ChunkObject,
    bytes: Arc<[u8]>,
    // This private identity is produced only by the complete initial hash. Public
    // metadata remains compatible with replica callers, but is not itself proof.
    #[cfg(feature = "dfs")]
    sealed_chunk: ChunkObject,
    #[cfg(feature = "dfs")]
    write_budget: Vec<WriteBudgetReservation>,
}

impl StagedChunk {
    pub fn new(operation_id: OperationId, bytes: Vec<u8>) -> Self {
        tracing::info_span!(
            "node.dfs.chunk.stage",
            operation_id = %operation_id.0,
            bytes = bytes.len() as u64,
            chunk_id = tracing::field::Empty,
        )
        .in_scope(|| {
            let bytes_len = bytes.len() as u64;
            let bytes: Arc<[u8]> =
                tracing::info_span!("node.dfs.chunk.vec_to_arc", bytes = bytes_len,)
                    .in_scope(|| bytes.into());
            let content_digest = tracing::info_span!("node.dfs.chunk.hash", bytes = bytes_len,)
                .in_scope(|| digest(&bytes));
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
    pub(crate) fn shared_bytes(&self) -> Arc<[u8]> {
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
    read_work: Option<super::read_work::ReadWorkMetrics>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedRangeCopy {
    pub chunk_offset: u64,
    pub length: usize,
    pub output_offset: usize,
}

impl PinnedChunkReader {
    pub fn read_at(&self, offset: u64, out: &mut [u8]) -> Result<usize> {
        if offset >= self.chunk.length || out.is_empty() {
            return Ok(0);
        }
        let allowed = usize::try_from((self.chunk.length - offset).min(out.len() as u64))
            .map_err(|_| invalid("Chunk read length is too large"))?;
        // A pinned descriptor protects identity across rename, not integrity
        // after disk damage. Return the exact bytes covered by this digest,
        // rather than verifying and then issuing another unverified read.
        let bytes = verified_range(
            &self.file,
            &self.chunk,
            offset,
            allowed,
            self.read_work.as_ref(),
        )?;
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
    pub(crate) fn read_ranges_at_uncommitted(
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
pub(crate) const PEER_READ_PAYLOAD_LIMIT: usize = 8 * 1024 * 1024;
pub(crate) const PEER_READ_OBJECT_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug, Default)]
struct PeerReadBudget {
    used: AtomicU64,
    peak: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct PeerReadReservation {
    budget: Arc<PeerReadBudget>,
    bytes: u64,
    metrics: Option<super::read_work::ReadWorkMetrics>,
}

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

impl Drop for PeerReadReservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
        if let Some(metrics) = &self.metrics {
            metrics.peer_payload_released(self.bytes);
        }
    }
}

fn peer_payload_capacity() -> Error {
    Error::coded(
        afs_error::IO_CAPACITY,
        "shared Peer read payload budget exhausted",
    )
}

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
struct PeerVerifiedObject {
    bytes: Vec<u8>,
    _reservation: PeerReadReservation,
    // A range fallback is never resident and needs no reusable identity.
    identity: Option<(LocalChunkRecord, PeerFileIdentity)>,
}

/// A view into an immutable private allocation. Its shared reservation remains
/// charged even after the single cache slot is replaced or an RPC is cancelled.
#[derive(Debug)]
pub(crate) struct PeerReadPayload {
    object: Arc<PeerVerifiedObject>,
    start: usize,
    length: usize,
}

impl PeerReadPayload {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.object.bytes[self.start..self.start + self.length]
    }

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

    /// Frame slices retain both immutable bytes and their original budget lease.
    pub(crate) fn into_shared_bytes(self) -> prost::bytes::Bytes {
        prost::bytes::Bytes::from_owner(self)
    }
}

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
    catalog_path: PathBuf,
    catalog: Mutex<LocalCatalog>,
    next_staging: AtomicU64,
    read_work: Option<super::read_work::ReadWorkMetrics>,
    peer_read_budget: Arc<PeerReadBudget>,
    peer_read_cache: Mutex<Option<Arc<PeerVerifiedObject>>>,
}

impl LocalChunkStore {
    pub fn open(root: impl AsRef<Path>, node_id: impl Into<String>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let chunks = root.join("chunks");
        let staging = root.join("staging");
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
            catalog_path,
            catalog: Mutex::new(catalog),
            next_staging: AtomicU64::new(1),
            read_work: None,
            peer_read_budget: Arc::new(PeerReadBudget::default()),
            peer_read_cache: Mutex::new(None),
        };
        store.recover()?;
        Ok(store)
    }

    pub(crate) fn with_read_work_metrics(
        mut self,
        metrics: super::read_work::ReadWorkMetrics,
    ) -> Self {
        self.read_work = Some(metrics);
        self
    }

    pub(crate) fn record_peer_disk_cache_event(&self, result: &'static str) {
        if let Some(metrics) = &self.read_work {
            metrics.peer_disk_cache_event(result);
        }
    }

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

    pub(crate) fn peer_disk_cache_fill_work(
        &self,
        bytes: usize,
        output: usize,
    ) -> Option<super::read_work::PeerCacheFillWork> {
        self.read_work
            .as_ref()
            .map(|metrics| metrics.peer_disk_cache_fill_work(bytes, output))
    }

    pub(crate) fn peer_disk_cache_stream_work(
        &self,
        output: usize,
    ) -> Option<super::read_work::PeerCacheStreamWork> {
        self.read_work
            .as_ref()
            .map(|metrics| metrics.peer_stream_pipeline(output))
    }

    pub(crate) fn record_read_hash(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.hash(stage, bytes);
        }
    }

    pub(crate) fn record_read_copy(&self, stage: &'static str, bytes: u64) {
        if let Some(metrics) = &self.read_work {
            metrics.copy(stage, bytes);
        }
    }

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
        Ok(record)
    }

    /// Reuse only the currently demanded object, never prefetch a next object.
    /// The slot mutex also supplies singleflight for a complete private fill.
    /// Authorization remains with each caller; this proof is content-only.
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
            // Never retain an unrelated current object while filling another.
            // Any externally held old allocation stays alive and fully charged.
            slot.take();
            if let Some(metrics) = &self.read_work {
                metrics.peer_object_cache("miss");
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
            verified_ranges_into(
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
            let object = Arc::new(PeerVerifiedObject {
                bytes,
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
        verified_range(&reader.file, &reader.chunk, 0, 0, self.read_work.as_ref()).inspect_err(
            |error| {
                if verified_copy_is_invalid(error) {
                    self.try_quarantine(chunk_id);
                }
            },
        )?;
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
        Ok(PinnedChunkReader {
            file,
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
        return Ok(counts);
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
        hasher.update(&scratch[..count]);
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
    let actual = ContentDigest {
        algorithm: DigestAlgorithm::Blake3,
        bytes: *hasher.finalize().as_bytes(),
    };
    if actual != chunk.content_digest {
        return Err(corrupt(&chunk.id, "digest mismatch"));
    }
    Ok(counts)
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
}

#[cfg(test)]
mod replica_replay_tests {
    use super::*;

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
            2 * bytes.len() as u64
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
        assert!(text.contains("afs_dfs_read_scans_total 3\n"), "{text}");
        assert!(
            text.contains("afs_dfs_read_scan_bytes_total 196611\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_copy_bytes_total{stage=\"scan_to_range\"} 6\n"),
            "{text}"
        );
        assert!(
            text.contains("afs_dfs_read_copy_bytes_total{stage=\"pinned_range_to_output\"} 3\n"),
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
