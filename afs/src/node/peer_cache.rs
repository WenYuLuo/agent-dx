//! Disposable native Peer object cache, never replicas or read authority.
//!
//! One serialized operation admits a bounded stream or complete-object fill.
//! Ready bodies live only on disk. Hits verify complete requested 1 MiB segments;
//! aligned private reads are direct, other ranges use bounded scratch. Changed identity requires a root scan.
//! Callers must discard private output on a miss or error.

use std::{
    collections::BTreeMap,
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::{
        fd::AsRawFd,
        unix::fs::{DirBuilderExt, FileExt, MetadataExt, OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    time::Instant,
};

use afs_error::{Error, Result};
use serde::{Deserialize, Serialize};

use crate::dfs::{ChunkEncoding, ChunkObject, DigestAlgorithm, NamespaceId};

pub const MAX_PEER_CACHE_ENTRIES: usize = 4096;
const MAX_OBJECT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_HEADER_BYTES: usize = 4096;
const SCRATCH_BYTES: usize = 64 * 1024;
pub(in crate::node) const PEER_CACHE_SEGMENT_BYTES: usize = 1024 * 1024;
pub const MAX_PEER_CACHE_PROOF_BYTES: u64 = 1024 * 1024;
const MAX_SEGMENTS: usize = 4;
const MARKER: &str = ".afs-peer-cache-v1";
const PENDING: &str = ".afs-peer-cache.pending";
const MAGIC: &[u8; 8] = b"AFSPC001";

#[derive(Clone, Debug)]
pub struct PeerCacheOptions {
    pub directory: PathBuf,
    pub capacity_bytes: u64,
    pub namespace_id: NamespaceId,
}

/// One complete BLAKE3 verification against trusted Meta identity, borrowing
/// the caller's temporary body. Neither proof nor cache retains a body.
#[derive(Debug)]
pub struct VerifiedPeerChunk<'a> {
    chunk: &'a ChunkObject,
    bytes: &'a [u8],
    segments: SegmentDigests,
    stream_origin: bool,
}

impl<'a> VerifiedPeerChunk<'a> {
    pub fn verify(chunk: &'a ChunkObject, bytes: &'a [u8]) -> Result<Self> {
        validate_chunk(chunk)?;
        if bytes.len() as u64 != chunk.length
            || blake3::hash(bytes).as_bytes() != &chunk.content_digest.bytes
        {
            return Err(corrupt(
                "Peer cache fill differs from trusted object identity",
            ));
        }
        let mut segments = SegmentDigests::default();
        if bytes.len() <= MAX_OBJECT_BYTES as usize {
            for (index, segment) in bytes.chunks(PEER_CACHE_SEGMENT_BYTES).enumerate() {
                segments.digests[index] = *blake3::hash(segment).as_bytes();
                segments.count += 1;
            }
        }
        Ok(Self {
            chunk,
            bytes,
            segments,
            stream_origin: false,
        })
    }

    pub(in crate::node) fn from_completed_stream(
        chunk: &'a ChunkObject,
        bytes: &'a [u8],
        proof: crate::node::rpc::peer::CompletedPeerObjectProof<'a>,
    ) -> Result<Self> {
        validate_chunk(chunk)?;
        let count = bytes.len().div_ceil(PEER_CACHE_SEGMENT_BYTES);
        if bytes.len() as u64 != chunk.length
            || count == 0
            || count > MAX_SEGMENTS
            || !std::ptr::eq(bytes, proof.bytes())
            || proof.root_digest() != &chunk.content_digest.bytes
            || proof.segment_digests().len() != count
        {
            return Err(corrupt(
                "Peer completed stream proof differs from trusted object",
            ));
        }
        let mut segments = SegmentDigests::default();
        segments.digests[..count].copy_from_slice(proof.segment_digests());
        segments.count = count as u8;
        Ok(Self {
            chunk,
            bytes,
            segments,
            stream_origin: true,
        })
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    pub fn chunk(&self) -> &'a ChunkObject {
        self.chunk
    }
    pub fn uses_completed_stream_proof(&self) -> bool {
        self.stream_origin
    }
}

/// Opaque per-fill binding. Only a sink can mint it; decoding carries it from
/// first byte through EOF so another same-object pending file cannot reuse proof.
#[derive(Clone, Debug)]
pub(in crate::node) struct PeerStreamBinding {
    identity: Arc<()>,
    pointer: usize,
    length: usize,
    offset: u64,
}

impl PeerStreamBinding {
    pub(in crate::node) fn matches_output(&self, offset: u64, output: &[u8]) -> bool {
        self.offset == offset
            && self.pointer == output.as_ptr() as usize
            && self.length == output.len()
    }
}

/// Complete stream receipt borrowing only the unpublished requested output.
/// The file proof is separately bound to its actual pending descriptor identity.
#[derive(Debug)]
pub struct CompletedPeerDiskRead<'output> {
    chunk: ChunkObject,
    requested_offset: u64,
    output: &'output [u8],
    root_digest: [u8; 32],
    segments: SegmentDigests,
    file_identity: Option<FileIdentity>,
    binding: PeerStreamBinding,
}

impl<'output> CompletedPeerDiskRead<'output> {
    pub fn chunk(&self) -> &ChunkObject {
        &self.chunk
    }
    pub fn requested_offset(&self) -> u64 {
        self.requested_offset
    }
    pub fn requested_length(&self) -> usize {
        self.output.len()
    }
    pub fn root_digest(&self) -> &[u8; 32] {
        &self.root_digest
    }
    pub fn private_output(&self) -> &'output [u8] {
        self.output
    }
    pub fn matches_private_output_identity(&self, pointer: usize, length: usize) -> bool {
        self.output.as_ptr() as usize == pointer && self.output.len() == length
    }
}

/// The synchronous operation retains its existing mutex and disk accounting.
/// Cache failures disable this sink; they never stop the same RPC verification.
pub struct PeerStreamSink<'a> {
    cache: &'a DiskPeerCache,
    state: &'a mut CacheState,
    chunk: ChunkObject,
    key: String,
    received: u64,
    header_length: u64,
    file: Option<File>,
    pending_identity: Option<(u64, u64)>,
    owns_pending: bool,
    committed: bool,
    identity: Arc<()>,
    #[cfg(test)]
    write_delay: std::time::Duration,
}

impl PeerStreamSink<'_> {
    pub(in crate::node) fn bind_output(
        &self,
        offset: u64,
        output: &[u8],
    ) -> Result<PeerStreamBinding> {
        if offset
            .checked_add(output.len() as u64)
            .is_none_or(|end| end > self.chunk.length)
        {
            return Err(invalid("Peer stream output binding exceeds object"));
        }
        Ok(PeerStreamBinding {
            identity: self.identity.clone(),
            pointer: output.as_ptr() as usize,
            length: output.len(),
            offset,
        })
    }

    pub(in crate::node) fn accepts_binding(&self, binding: &PeerStreamBinding) -> bool {
        Arc::ptr_eq(&self.identity, &binding.identity)
    }

    fn prepare(&mut self) -> Result<()> {
        self.cache.check_root()?;
        if self.state.pending_charge != 0 {
            self.cache.remove_pending(self.state)?;
        }
        if self.state.entries.contains_key(&self.key) {
            self.cache.remove_entry(self.state, &self.key, false)?;
        }
        let header = serde_json::to_vec(&Header {
            version: 1,
            namespace_id: self.cache.options.namespace_id.clone(),
            chunk: self.chunk.clone(),
        })
        .map_err(|_| invalid("Peer stream cache header cannot be encoded"))?;
        if header.len() > MAX_HEADER_BYTES {
            return Err(invalid("Peer stream cache header exceeds bound"));
        }
        self.header_length = (MAGIC.len() + 4 + header.len()) as u64;
        let length = self
            .header_length
            .checked_add(self.chunk.length)
            .ok_or_else(|| invalid("Peer stream file size overflow"))?;
        let unit = self
            .cache
            .root
            .metadata()
            .map_err(Error::from)?
            .blksize()
            .max(512);
        let reservation = length
            .checked_add(unit - 1)
            .and_then(|size| size.checked_div(unit))
            .and_then(|blocks| blocks.checked_add(2))
            .and_then(|blocks| blocks.checked_mul(unit))
            .ok_or_else(|| invalid("Peer stream reservation overflow"))?;
        if !self.cache.admit_space(self.state, reservation)? {
            return Ok(());
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(self.cache.directory().join(PENDING))
            .map_err(Error::from)?;
        // Charge ownership immediately after creation, before metadata/header
        // I/O can fail. Unidentifiable files remain charged and are not unlinked.
        self.file = Some(file);
        self.owns_pending = true;
        self.state.used_bytes += reservation;
        self.state.pending_charge = reservation;
        self.state.stats.disk_peak_bytes =
            self.state.stats.disk_peak_bytes.max(self.state.used_bytes);
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| unavailable("Peer stream file was not opened"))?;
        #[cfg(test)]
        if self.state.fail_stream_metadata {
            return Err(unavailable("injected pending descriptor metadata failure"));
        }
        let metadata = file.metadata().map_err(Error::from)?;
        self.pending_identity = Some((metadata.dev(), metadata.ino()));
        file.write_all(MAGIC).map_err(Error::from)?;
        file.write_all(&(header.len() as u32).to_le_bytes())
            .map_err(Error::from)?;
        file.write_all(&header).map_err(Error::from)?;
        Ok(())
    }

    fn abort_pending(&mut self) -> Result<()> {
        drop(self.file.take());
        if !self.owns_pending {
            return Ok(());
        }
        self.cache.check_root()?;
        match fs::symlink_metadata(self.cache.directory().join(PENDING)) {
            Ok(metadata) if Some((metadata.dev(), metadata.ino())) == self.pending_identity => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => {
                return Err(invalid(
                    "Peer stream cleanup refuses a replaced pending file",
                ));
            }
        }
        self.cache.remove_pending(self.state)?;
        self.owns_pending = false;
        Ok(())
    }

    fn disable(&mut self, error: &Error) {
        afs_logging::warn!("dfs.peer_stream_cache_disabled"; "error" => error.to_string());
        if let Err(cleanup) = self.abort_pending() {
            afs_logging::warn!("dfs.peer_stream_cache_cleanup_failed"; "error" => cleanup.to_string());
        }
    }

    #[cfg(test)]
    pub(in crate::node) fn delay_writes_for_test(&mut self, delay: std::time::Duration) {
        self.write_delay = delay;
    }

    pub(in crate::node) fn write(&mut self, offset: u64, bytes: &[u8]) -> Result<()> {
        if offset != self.received
            || offset
                .checked_add(bytes.len() as u64)
                .is_none_or(|end| end > self.chunk.length)
        {
            return Err(invalid("Peer stream sink range differs from the object"));
        }
        self.received += bytes.len() as u64;
        #[cfg(test)]
        std::thread::sleep(self.write_delay);
        #[cfg(test)]
        if self.state.fail_stream_write {
            self.disable(&unavailable("injected stream cache write failure"));
        }
        if let Some(file) = &mut self.file
            && let Err(error) = file.write_all(bytes)
        {
            self.disable(&Error::from(error));
        }
        Ok(())
    }

    pub(in crate::node) fn seal_completed<'output>(
        &mut self,
        proof: crate::node::rpc::peer::CompletedPeerStreamProof,
        requested_offset: u64,
        output: &'output [u8],
    ) -> Result<CompletedPeerDiskRead<'output>> {
        if !self.accepts_binding(proof.binding())
            || !proof.binding().matches_output(requested_offset, output)
            || proof.chunk() != &self.chunk
            || self.received != self.chunk.length
            || proof.root_digest() != &self.chunk.content_digest.bytes
            || requested_offset
                .checked_add(output.len() as u64)
                .is_none_or(|end| end > self.chunk.length)
            || proof.segment_digests().len()
                != (self.chunk.length as usize).div_ceil(PEER_CACHE_SEGMENT_BYTES)
        {
            return Err(corrupt(
                "Peer stream completion differs from the sink object",
            ));
        }
        let checked = (|| {
            let Some(file) = &mut self.file else {
                return Ok(None);
            };
            file.flush().map_err(Error::from)?;
            let metadata = file.metadata().map_err(Error::from)?;
            validate_private_file(&metadata)?;
            self.cache.check_root()?;
            let current =
                fs::symlink_metadata(self.cache.directory().join(PENDING)).map_err(Error::from)?;
            if metadata.len() != self.header_length + self.chunk.length
                || Some((metadata.dev(), metadata.ino())) != self.pending_identity
                || file_identity(&metadata) != file_identity(&current)
            {
                return Err(corrupt(
                    "Peer stream pending descriptor changed before sealing",
                ));
            }
            let charge = allocated(&metadata)?;
            if charge > self.state.pending_charge {
                self.state.used_bytes = self
                    .state
                    .used_bytes
                    .saturating_add(charge - self.state.pending_charge);
                self.state.pending_charge = charge;
                self.state.stats.disk_peak_bytes =
                    self.state.stats.disk_peak_bytes.max(self.state.used_bytes);
                return Err(unavailable(
                    "Peer stream allocation exceeds reservation before seal",
                ));
            }
            Ok(Some(file_identity(&metadata)))
        })();
        let file_identity = match checked {
            Ok(identity) => identity,
            Err(error) => {
                self.disable(&error);
                None
            }
        };
        let mut segments = SegmentDigests {
            count: proof.segment_digests().len() as u8,
            ..SegmentDigests::default()
        };
        segments.digests[..usize::from(segments.count)].copy_from_slice(proof.segment_digests());
        Ok(CompletedPeerDiskRead {
            chunk: self.chunk.clone(),
            requested_offset,
            output,
            root_digest: *proof.root_digest(),
            segments,
            file_identity,
            binding: proof.binding().clone(),
        })
    }

    pub fn commit(&mut self, completed: CompletedPeerDiskRead<'_>) -> Result<bool> {
        if !self.accepts_binding(&completed.binding)
            || completed.chunk != self.chunk
            || completed.root_digest != self.chunk.content_digest.bytes
        {
            return Err(corrupt("Peer disk completion belongs to another object"));
        }
        let Some(file) = &self.file else {
            return Ok(false);
        };
        self.cache.check_root()?;
        let metadata = file.metadata().map_err(Error::from)?;
        validate_private_file(&metadata)?;
        let current =
            fs::symlink_metadata(self.cache.directory().join(PENDING)).map_err(Error::from)?;
        if completed.file_identity != Some(file_identity(&metadata))
            || file_identity(&metadata) != file_identity(&current)
        {
            return Err(corrupt(
                "Peer disk completion does not bind this pending descriptor",
            ));
        }
        let charge = allocated(&metadata)?;
        let reservation = self.state.pending_charge;
        if charge > reservation {
            self.state.used_bytes = self.state.used_bytes.saturating_add(charge - reservation);
            self.state.pending_charge = charge;
            self.state.stats.disk_peak_bytes =
                self.state.stats.disk_peak_bytes.max(self.state.used_bytes);
            return Err(unavailable(
                "Peer stream allocation exceeds reserved blocks",
            ));
        }
        match fs::symlink_metadata(self.cache.path(&self.key)) {
            Ok(_) => return Err(invalid("Peer stream publication refuses an untracked file")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        fs::rename(
            self.cache.directory().join(PENDING),
            self.cache.path(&self.key),
        )
        .map_err(Error::from)?;
        self.committed = true;
        self.owns_pending = false;
        self.state.used_bytes = self.state.used_bytes - reservation + charge;
        self.state.pending_charge = 0;
        self.state.entries.insert(
            self.key.clone(),
            Entry {
                chunk: self.chunk.clone(),
                charge,
                identity: (metadata.dev(), metadata.ino()),
                touched: self.cache.bucket(),
                proof: None,
            },
        );
        refresh_metadata(self.state);
        if let Ok(published) = file.metadata() {
            install_proof(
                self.state,
                &self.key,
                file_identity(&published),
                completed.segments,
            );
        }
        Ok(true)
    }
}

impl Drop for PeerStreamSink<'_> {
    fn drop(&mut self) {
        if !self.committed
            && let Err(error) = self.abort_pending()
        {
            afs_logging::warn!("dfs.peer_stream_drop_cleanup_failed"; "error" => error.to_string());
        }
    }
}

#[derive(Clone, Debug, Default)]
struct SegmentDigests {
    digests: [[u8; 32]; MAX_SEGMENTS],
    count: u8,
}

type FileIdentity = (u64, u64, u64, i64, i64, i64, i64);

#[derive(Debug)]
struct VerifiedDiskSegments {
    identity: FileIdentity,
    segments: SegmentDigests,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PeerCacheHashWork {
    pub cache_segment_bytes: u64,
    pub cache_fullscan_bytes: u64,
    pub segment_index_bytes: u64,
}

/// Low-frequency aggregate wall time inside read_range only. Admission lock,
/// operation root checks, Meta, final engine authority and metric emission are
/// outside these boundaries. Streaming fallback cannot separate read and hash.
#[derive(Clone, Copy, Debug, Default)]
pub struct PeerCacheReadStages {
    pub prepare_ns: u64,
    pub data_read_ns: u64,
    pub segment_verify_ns: u64,
    pub stream_read_verify_ns: u64,
    pub finalize_ns: u64,
}

#[derive(Clone, Debug, Default)]
pub struct PeerCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub corruptions: u64,
    pub evictions: u64,
    pub hash_bytes_total: u64,
    /// Actual body-read attempts, excluding header and metadata reads.
    pub data_read_calls: u64,
    /// Explicit scratch-to-private-output copies, including failed reads.
    pub copied_bytes_total: u64,
    pub disk_bytes: u64,
    /// Peak charged ready and temporary blocks, including conservative reservations.
    pub disk_peak_bytes: u64,
    pub entries: usize,
    pub scratch_peak_bytes: u64,
    pub proof_metadata_bytes: u64,
    pub proof_metadata_peak_bytes: u64,
    pub hash_work: PeerCacheHashWork,
    pub read_stages: PeerCacheReadStages,
}

#[derive(Debug)]
pub struct DiskPeerCache {
    options: PeerCacheOptions,
    root: File,
    _marker: File,
    started: Instant,
    state: Mutex<CacheState>,
    read_work: Option<super::read_work::ReadWorkMetrics>,
}

#[derive(Debug, Default)]
struct CacheState {
    closed: bool,
    entries: BTreeMap<String, Entry>,
    used_bytes: u64,
    pending_charge: u64,
    stats: PeerCacheStats,
    #[cfg(test)]
    fail_remove: bool,
    #[cfg(test)]
    fail_stream_write: bool,
    #[cfg(test)]
    fail_stream_metadata: bool,
    #[cfg(test)]
    proof_limit: Option<u64>,
}

#[derive(Debug)]
struct Entry {
    chunk: ChunkObject,
    charge: u64,
    identity: (u64, u64),
    touched: u64,
    proof: Option<Box<VerifiedDiskSegments>>,
}

#[derive(Serialize, Deserialize)]
struct Header {
    version: u32,
    namespace_id: NamespaceId,
    chunk: ChunkObject,
}

pub struct PeerCacheOperation<'a> {
    cache: &'a DiskPeerCache,
    state: MutexGuard<'a, CacheState>,
    initial_work: PeerCacheHashWork,
    initial_reads: u64,
    initial_copies: u64,
    initial_stages: PeerCacheReadStages,
}

impl DiskPeerCache {
    pub fn open(options: PeerCacheOptions) -> Result<Self> {
        if !options.directory.is_absolute()
            || options.capacity_bytes == 0
            || options.namespace_id.0.is_empty()
            || options.namespace_id.0.len() > 512
        {
            return Err(invalid(
                "Peer cache requires an absolute directory and positive budget",
            ));
        }
        reject_symlink_components(&options.directory)?;
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&options.directory)
            .map_err(Error::from)?;
        reject_symlink_components(&options.directory)?;
        let root = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
            .open(&options.directory)
            .map_err(Error::from)?;
        let metadata = root.metadata().map_err(Error::from)?;
        if !metadata.is_dir() || metadata.mode() & 0o077 != 0 || metadata.uid() != effective_uid() {
            return Err(invalid(
                "Peer cache directory must be private and owned by this user",
            ));
        }
        reject_symlink_components(&options.directory)?;
        let current = fs::symlink_metadata(&options.directory).map_err(Error::from)?;
        if (current.dev(), current.ino()) != (metadata.dev(), metadata.ino()) {
            return Err(invalid("Peer cache directory changed during open"));
        }
        // All child operations resolve through the pinned Linux directory FD,
        // never through a configured pathname that can be replaced later.
        let directory = fd_directory(&root);
        let marker_path = directory.join(MARKER);
        let mut marker = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&marker_path)
        {
            Ok(mut file) => {
                if fs::read_dir(&directory).map_err(Error::from)?.any(|entry| {
                    entry
                        .map(|entry| entry.file_name() != MARKER)
                        .unwrap_or(true)
                }) {
                    drop(file);
                    fs::remove_file(marker_path).map_err(Error::from)?;
                    return Err(invalid(
                        "Peer cache refuses an unmarked non-empty directory",
                    ));
                }
                file.write_all(b"AFS disposable Peer cache v1\n")
                    .map_err(Error::from)?;
                file.sync_all().map_err(Error::from)?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .read(true)
                .write(true)
                .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
                .open(marker_path)
                .map_err(Error::from)?,
            Err(error) => return Err(Error::from(error)),
        };
        validate_private_file(&marker.metadata().map_err(Error::from)?)?;
        // SAFETY: the descriptor belongs to marker; flock does not own it.
        if unsafe { libc::flock(marker.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(unavailable("Peer cache directory is already in use"));
        }
        marker.seek(SeekFrom::Start(0)).map_err(Error::from)?;
        let mut contents = String::new();
        Read::by_ref(&mut marker)
            .take(128)
            .read_to_string(&mut contents)
            .map_err(Error::from)?;
        if contents != "AFS disposable Peer cache v1\n" {
            return Err(invalid("Peer cache marker is not recognized"));
        }
        let cache = Self {
            options,
            root,
            _marker: marker,
            started: Instant::now(),
            state: Mutex::new(CacheState::default()),
            read_work: None,
        };
        cache.recover()?;
        Ok(cache)
    }

    pub fn namespace_id(&self) -> &NamespaceId {
        &self.options.namespace_id
    }

    pub fn with_read_work_metrics(mut self, metrics: super::read_work::ReadWorkMetrics) -> Self {
        if let Ok(state) = self.state.lock() {
            report_work(
                &metrics,
                state.stats.hash_work,
                PeerCacheHashWork::default(),
            );
            metrics.peer_cache_data_reads(state.stats.data_read_calls);
            metrics.copy("cache_to_output", state.stats.copied_bytes_total);
            report_stages(
                &metrics,
                state.stats.read_stages,
                PeerCacheReadStages::default(),
            );
            metrics.peer_cache_metadata(
                state.stats.proof_metadata_bytes,
                state.stats.proof_metadata_peak_bytes,
            );
        }
        self.read_work = Some(metrics);
        self
    }

    pub fn operation(&self) -> Result<PeerCacheOperation<'_>> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        if state.closed {
            return Err(unavailable("Peer cache is stopped"));
        }
        self.check_root()?;
        let initial_work = state.stats.hash_work;
        let initial_reads = state.stats.data_read_calls;
        let initial_copies = state.stats.copied_bytes_total;
        let initial_stages = state.stats.read_stages;
        Ok(PeerCacheOperation {
            cache: self,
            state,
            initial_work,
            initial_reads,
            initial_copies,
            initial_stages,
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        state.closed = true;
        state.entries.clear();
        refresh_metadata(&mut state);
        if let Some(metrics) = &self.read_work {
            metrics.peer_cache_metadata(0, state.stats.proof_metadata_peak_bytes);
        }
        Ok(())
    }

    pub fn stats(&self) -> Result<PeerCacheStats> {
        let state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        let mut stats = state.stats.clone();
        stats.disk_bytes = state.used_bytes;
        stats.entries = state.entries.len();
        Ok(stats)
    }

    fn check_root(&self) -> Result<()> {
        reject_symlink_components(&self.options.directory)?;
        let current = fs::symlink_metadata(&self.options.directory).map_err(Error::from)?;
        let pinned = self.root.metadata().map_err(Error::from)?;
        if !current.is_dir()
            || current.mode() & 0o077 != 0
            || current.uid() != effective_uid()
            || pinned.mode() & 0o077 != 0
            || pinned.uid() != effective_uid()
            || (current.dev(), current.ino()) != (pinned.dev(), pinned.ino())
        {
            return Err(invalid("Peer cache root identity changed"));
        }
        Ok(())
    }

    fn key(&self, chunk: &ChunkObject) -> Result<String> {
        let input = serde_json::to_vec(&(&self.options.namespace_id, chunk))
            .map_err(|_| invalid("Peer cache object identity cannot be encoded"))?;
        Ok(blake3::hash(&input).to_hex().to_string())
    }

    fn directory(&self) -> PathBuf {
        fd_directory(&self.root)
    }

    fn bucket(&self) -> u64 {
        self.started.elapsed().as_secs()
    }

    fn path(&self, key: &str) -> PathBuf {
        self.directory().join(format!("{key}.ready"))
    }

    fn recover(&self) -> Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| unavailable("Peer cache lock poisoned"))?;
        // Preflight bounded names and allocated space before any private cleanup.
        let mut ready = Vec::new();
        let mut pending = None;
        let mut total = 0_u64;
        for entry in fs::read_dir(self.directory()).map_err(Error::from)? {
            let entry = entry.map_err(Error::from)?;
            let name = entry.file_name();
            if name == MARKER {
                continue;
            }
            let name = name
                .to_str()
                .ok_or_else(|| invalid("Peer cache contains a foreign name"))?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(Error::from)?;
            validate_private_file(&metadata)?;
            let charge = allocated(&metadata)?;
            total = total
                .checked_add(charge)
                .ok_or_else(|| invalid("Peer cache size overflow"))?;
            if name == PENDING {
                pending = Some(charge);
            } else if valid_ready_name(name) {
                ready.push((name.trim_end_matches(".ready").to_owned(), metadata));
            } else {
                return Err(invalid(
                    "Peer cache contains foreign files; preserved without cleanup",
                ));
            }
            if ready.len() > MAX_PEER_CACHE_ENTRIES || total > self.options.capacity_bytes {
                return Err(invalid("Peer cache recovery exceeds disk or index budget"));
            }
        }
        state.used_bytes = total;
        state.stats.disk_peak_bytes = state.stats.disk_peak_bytes.max(total);
        state.pending_charge = pending.unwrap_or(0);
        if pending.is_some() {
            self.remove_pending(&mut state)?;
        }
        for (key, metadata) in ready {
            let (mut file, _) = open_private(&self.path(&key))?;
            let (header, _) = read_header(&mut file)?;
            validate_chunk(&header.chunk)?;
            if header.version != 1
                || header.namespace_id != self.options.namespace_id
                || self.key(&header.chunk)? != key
                || header.chunk.length > MAX_OBJECT_BYTES
            {
                return Err(invalid("Peer cache ready identity is foreign; preserved"));
            }
            state.entries.insert(
                key.clone(),
                Entry {
                    chunk: header.chunk.clone(),
                    charge: allocated(&metadata)?,
                    identity: (metadata.dev(), metadata.ino()),
                    touched: 0,
                    proof: None,
                },
            );
            refresh_metadata(&mut state);
            let before = file.metadata().map_err(Error::from)?;
            let index = proof_can_fit(&state);
            let result = scan_body(
                &mut file,
                &header.chunk,
                0,
                &mut [],
                &mut state.stats,
                index,
            );
            match result {
                Ok(segments) => {
                    let after = file.metadata().map_err(Error::from)?;
                    let current = fs::symlink_metadata(self.path(&key)).map_err(Error::from)?;
                    if file_identity(&before) != file_identity(&after)
                        || file_identity(&after) != file_identity(&current)
                    {
                        return Err(corrupt("Peer cache file changed during recovery"));
                    }
                    if let Some(segments) = segments {
                        install_proof(&mut state, &key, file_identity(&after), segments);
                    }
                }
                Err(_) => {
                    state.stats.corruptions = state.stats.corruptions.saturating_add(1);
                    drop(file);
                    self.remove_entry(&mut state, &key, false)?;
                }
            }
        }
        Ok(())
    }

    fn remove_entry(&self, state: &mut CacheState, key: &str, eviction: bool) -> Result<()> {
        self.check_root()?;
        let Some(entry) = state.entries.get(key) else {
            return Ok(());
        };
        #[cfg(test)]
        if state.fail_remove {
            return Err(unavailable("injected Peer cache cleanup failure"));
        }
        match fs::symlink_metadata(self.path(key)) {
            Ok(metadata) => {
                validate_private_file(&metadata)?;
                if (metadata.dev(), metadata.ino()) != entry.identity {
                    return Err(invalid("Peer cache cleanup refuses a replaced file"));
                }
                fs::remove_file(self.path(key)).map_err(Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        let charge = entry.charge;
        state.entries.remove(key);
        refresh_metadata(state);
        state.used_bytes = state.used_bytes.saturating_sub(charge);
        if eviction {
            state.stats.evictions = state.stats.evictions.saturating_add(1);
        }
        Ok(())
    }

    fn remove_pending(&self, state: &mut CacheState) -> Result<()> {
        self.check_root()?;
        #[cfg(test)]
        if state.fail_remove {
            return Err(unavailable("injected Peer cache cleanup failure"));
        }
        match fs::symlink_metadata(self.directory().join(PENDING)) {
            Ok(metadata) => {
                validate_private_file(&metadata)?;
                fs::remove_file(self.directory().join(PENDING)).map_err(Error::from)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
        state.used_bytes = state.used_bytes.saturating_sub(state.pending_charge);
        state.pending_charge = 0;
        Ok(())
    }

    fn admit_space(&self, state: &mut CacheState, charge: u64) -> Result<bool> {
        if charge > self.options.capacity_bytes {
            return Ok(false);
        }
        while state.used_bytes.saturating_add(charge) > self.options.capacity_bytes
            || state.entries.len() >= MAX_PEER_CACHE_ENTRIES
        {
            // Whole-second buckets, bounded by 4096 entries; no append history.
            let Some(key) = state
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.touched)
                .map(|(key, _)| key.clone())
            else {
                return Ok(false);
            };
            self.remove_entry(state, &key, true)?;
        }
        Ok(true)
    }
}

impl PeerCacheOperation<'_> {
    pub fn hash_work(&self) -> PeerCacheHashWork {
        self.state.stats.hash_work
    }

    /// A miss/error may have modified private output; never publish that output.
    pub fn begin_stream<'operation>(
        &'operation mut self,
        chunk: &ChunkObject,
    ) -> Result<PeerStreamSink<'operation>> {
        validate_chunk(chunk)?;
        if chunk.length > MAX_OBJECT_BYTES {
            return Err(invalid("Peer stream object exceeds bound"));
        }
        let key = self.cache.key(chunk)?;
        let mut sink = PeerStreamSink {
            cache: self.cache,
            state: &mut self.state,
            chunk: chunk.clone(),
            key,
            received: 0,
            header_length: 0,
            file: None,
            pending_identity: None,
            owns_pending: false,
            committed: false,
            identity: Arc::new(()),
            #[cfg(test)]
            write_delay: std::time::Duration::ZERO,
        };
        if let Err(error) = sink.prepare() {
            sink.disable(&error);
        }
        Ok(sink)
    }

    pub fn read_range(&mut self, chunk: &ChunkObject, offset: u64, out: &mut [u8]) -> Result<bool> {
        let prepare_started = Instant::now();
        let selected = (|| {
            self.cache.check_root()?;
            validate_chunk(chunk)?;
            if offset
                .checked_add(out.len() as u64)
                .is_none_or(|end| end > chunk.length)
            {
                return Err(invalid("Peer cache range exceeds immutable object"));
            }
            let key = self.cache.key(chunk)?;
            let Some(entry) = self.state.entries.get(&key) else {
                self.state.stats.misses = self.state.stats.misses.saturating_add(1);
                return Ok(None);
            };
            if entry.chunk != *chunk {
                return Err(invalid("Peer cache index identity differs"));
            }
            Ok(Some((key, entry.identity)))
        })();
        let (key, expected) = match selected {
            Ok(Some(selected)) => selected,
            early => {
                self.state.stats.read_stages.prepare_ns = self
                    .state
                    .stats
                    .read_stages
                    .prepare_ns
                    .saturating_add(elapsed_ns(prepare_started));
                return early.map(|_| false);
            }
        };
        let path = self.cache.path(&key);
        let mut finalize_started = None;
        let result = (|| {
            let prepared = (|| {
                let (mut file, before) = open_private(&path)?;
                if (before.dev(), before.ino()) != expected {
                    return Err(invalid("Peer cache ready file was replaced"));
                }
                let (header, body_start) = read_header(&mut file)?;
                if header.version != 1
                    || header.namespace_id != self.cache.options.namespace_id
                    || header.chunk != *chunk
                {
                    return Err(corrupt("Peer cache header identity differs"));
                }
                let identity = file_identity(&before);
                let reusable = self.state.entries.get(&key).and_then(|entry| {
                    entry
                        .proof
                        .as_ref()
                        .filter(|proof| proof.identity == identity)
                        .map(|proof| proof.segments.clone())
                });
                Ok((file, before, body_start, reusable))
            })();
            self.state.stats.read_stages.prepare_ns = self
                .state
                .stats
                .read_stages
                .prepare_ns
                .saturating_add(elapsed_ns(prepare_started));
            let (mut file, before, body_start, reusable) = prepared?;
            let fresh = if let Some(segments) = reusable {
                scan_segments(
                    &mut file,
                    body_start,
                    chunk,
                    offset,
                    out,
                    &segments,
                    &mut self.state.stats,
                )?;
                None
            } else {
                // Metadata changes invalidate old summaries before a complete root scan.
                let started = Instant::now();
                if let Some(entry) = self.state.entries.get_mut(&key) {
                    entry.proof = None;
                }
                refresh_metadata(&mut self.state);
                let index = proof_can_fit(&self.state);
                let result = scan_body(&mut file, chunk, offset, out, &mut self.state.stats, index);
                self.state.stats.read_stages.stream_read_verify_ns = self
                    .state
                    .stats
                    .read_stages
                    .stream_read_verify_ns
                    .saturating_add(elapsed_ns(started));
                result?
            };
            finalize_started = Some(Instant::now());
            let after = file.metadata().map_err(Error::from)?;
            let current = fs::symlink_metadata(&path).map_err(Error::from)?;
            if file_identity(&before) != file_identity(&after)
                || file_identity(&before) != file_identity(&current)
            {
                return Err(corrupt("Peer cache file changed during verified read"));
            }
            if let Some(segments) = fresh {
                install_proof(&mut self.state, &key, file_identity(&after), segments);
            }
            Ok(())
        })();
        let finalize_started = finalize_started.unwrap_or_else(Instant::now);
        let outcome = (|| match result {
            Ok(()) => {
                if let Some(entry) = self.state.entries.get_mut(&key) {
                    entry.touched = self.cache.bucket();
                }
                self.state.stats.hits = self.state.stats.hits.saturating_add(1);
                Ok(true)
            }
            Err(error)
                if error.code() == afs_error::NODE_TRANSFER_CORRUPT_DATA
                    || error.kind() == afs_error::ErrorKind::NotFound =>
            {
                self.state.stats.corruptions = self.state.stats.corruptions.saturating_add(1);
                self.state.stats.misses = self.state.stats.misses.saturating_add(1);
                self.cache.remove_entry(&mut self.state, &key, false)?;
                Ok(false)
            }
            Err(error) => Err(error),
        })();
        self.state.stats.read_stages.finalize_ns = self
            .state
            .stats
            .read_stages
            .finalize_ns
            .saturating_add(elapsed_ns(finalize_started));
        outcome
    }

    /// Only borrowed typed proof is admitted; this method does not hash again.
    pub fn store_verified(&mut self, verified: &VerifiedPeerChunk<'_>) -> Result<bool> {
        let chunk = verified.chunk;
        if chunk.length > MAX_OBJECT_BYTES {
            return Ok(false);
        }
        self.cache.check_root()?;
        if self.state.pending_charge != 0 {
            self.cache.remove_pending(&mut self.state)?;
        }
        let key = self.cache.key(chunk)?;
        if self.state.entries.contains_key(&key) {
            self.cache.remove_entry(&mut self.state, &key, false)?;
        }
        let header = serde_json::to_vec(&Header {
            version: 1,
            namespace_id: self.cache.options.namespace_id.clone(),
            chunk: chunk.clone(),
        })
        .map_err(|_| invalid("Peer cache header cannot be encoded"))?;
        if header.len() > MAX_HEADER_BYTES {
            return Err(invalid("Peer cache header exceeds bound"));
        }
        let length = (MAGIC.len() + 4 + header.len()) as u64 + chunk.length;
        // Reserve staging rounding plus two filesystem units; actual stat.blocks
        // replaces this conservative charge only after atomic publication.
        let unit = self
            .cache
            .root
            .metadata()
            .map_err(Error::from)?
            .blksize()
            .max(512);
        let reservation = length
            .checked_add(unit - 1)
            .and_then(|size| size.checked_div(unit))
            .and_then(|blocks| blocks.checked_add(2))
            .and_then(|blocks| blocks.checked_mul(unit))
            .ok_or_else(|| invalid("Peer cache staging size overflow"))?;
        if !self.cache.admit_space(&mut self.state, reservation)? {
            return Ok(false);
        }
        let path = self.cache.directory().join(PENDING);
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&path)
            .map_err(Error::from)?;
        self.state.used_bytes += reservation;
        self.state.stats.disk_peak_bytes =
            self.state.stats.disk_peak_bytes.max(self.state.used_bytes);
        self.state.pending_charge = reservation;
        let written = (|| {
            file.write_all(MAGIC).map_err(Error::from)?;
            file.write_all(&(header.len() as u32).to_le_bytes())
                .map_err(Error::from)?;
            file.write_all(&header).map_err(Error::from)?;
            file.write_all(verified.bytes).map_err(Error::from)?;
            file.flush().map_err(Error::from)?;
            let metadata = file.metadata().map_err(Error::from)?;
            validate_private_file(&metadata)?;
            let charge = allocated(&metadata)?;
            if charge > reservation {
                self.state.used_bytes = self.state.used_bytes.saturating_add(charge - reservation);
                self.state.pending_charge = charge;
                self.state.stats.disk_peak_bytes =
                    self.state.stats.disk_peak_bytes.max(self.state.used_bytes);
                return Err(unavailable("Peer cache allocation exceeds reserved blocks"));
            }
            match fs::symlink_metadata(self.cache.path(&key)) {
                Ok(_) => return Err(invalid("Peer cache publication refuses an untracked file")),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(Error::from(error)),
            }
            self.cache.check_root()?;
            fs::rename(&path, self.cache.path(&key)).map_err(Error::from)?;
            // Renaming may change ctime. Bind summaries to post-publication
            // identity; inability to inspect it leaves a safe full-scan entry.
            let published = file
                .metadata()
                .ok()
                .map(|metadata| file_identity(&metadata));
            Ok((charge, (metadata.dev(), metadata.ino()), published))
        })();
        drop(file);
        match written {
            Ok((charge, identity, published)) => {
                self.state.used_bytes = self.state.used_bytes - reservation + charge;
                self.state.pending_charge = 0;
                self.state.entries.insert(
                    key.clone(),
                    Entry {
                        chunk: chunk.clone(),
                        charge,
                        identity,
                        touched: self.cache.bucket(),
                        proof: None,
                    },
                );
                refresh_metadata(&mut self.state);
                if let Some(identity) = published {
                    install_proof(&mut self.state, &key, identity, verified.segments.clone());
                }
                Ok(true)
            }
            Err(error) => {
                // Failed cleanup remains charged and blocks future admission.
                self.cache.remove_pending(&mut self.state)?;
                Err(error)
            }
        }
    }
}

fn scan_body(
    file: &mut File,
    chunk: &ChunkObject,
    offset: u64,
    out: &mut [u8],
    stats: &mut PeerCacheStats,
    index_segments: bool,
) -> Result<Option<SegmentDigests>> {
    let mut scratch = [0_u8; SCRATCH_BYTES];
    stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(SCRATCH_BYTES as u64);
    let mut hash = blake3::Hasher::new();
    let mut segment = blake3::Hasher::new();
    let mut segments = SegmentDigests::default();
    let mut position = 0_u64;
    while position < chunk.length {
        let segment_remaining =
            PEER_CACHE_SEGMENT_BYTES as u64 - position % PEER_CACHE_SEGMENT_BYTES as u64;
        let requested = (chunk.length - position)
            .min(SCRATCH_BYTES as u64)
            .min(segment_remaining) as usize;
        stats.data_read_calls = stats.data_read_calls.saturating_add(1);
        let read = file.read(&mut scratch[..requested]).map_err(Error::from)?;
        if read == 0 {
            return Err(corrupt("Peer cache body is truncated"));
        }
        hash.update(&scratch[..read]);
        stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(read as u64);
        stats.hash_work.cache_fullscan_bytes = stats
            .hash_work
            .cache_fullscan_bytes
            .saturating_add(read as u64);
        if index_segments {
            segment.update(&scratch[..read]);
            stats.hash_work.segment_index_bytes = stats
                .hash_work
                .segment_index_bytes
                .saturating_add(read as u64);
        }
        stats.copied_bytes_total = stats.copied_bytes_total.saturating_add(copy_overlap(
            position,
            &scratch[..read],
            offset,
            out,
        ));
        position += read as u64;
        if index_segments
            && (position.is_multiple_of(PEER_CACHE_SEGMENT_BYTES as u64)
                || position == chunk.length)
        {
            let index = usize::from(segments.count);
            if index >= MAX_SEGMENTS {
                return Err(invalid("Peer cache segment count exceeds bound"));
            }
            segments.digests[index] = *segment.finalize().as_bytes();
            segments.count += 1;
            segment = blake3::Hasher::new();
        }
    }
    if file.read(&mut scratch[..1]).map_err(Error::from)? != 0
        || hash.finalize().as_bytes() != &chunk.content_digest.bytes
    {
        return Err(corrupt("Peer cache complete body digest or length differs"));
    }
    Ok(index_segments.then_some(segments))
}

fn scan_segments(
    file: &mut File,
    body_start: u64,
    chunk: &ChunkObject,
    offset: u64,
    out: &mut [u8],
    segments: &SegmentDigests,
    stats: &mut PeerCacheStats,
) -> Result<()> {
    let count = (chunk.length as usize).div_ceil(PEER_CACHE_SEGMENT_BYTES);
    if count != usize::from(segments.count) || count > MAX_SEGMENTS {
        return Err(corrupt("Peer cache segment proof has invalid coverage"));
    }
    if out.len() == PEER_CACHE_SEGMENT_BYTES
        && offset.is_multiple_of(PEER_CACHE_SEGMENT_BYTES as u64)
    {
        let index = offset as usize / PEER_CACHE_SEGMENT_BYTES;
        return read_private_segment(
            file,
            body_start + offset,
            out,
            &segments.digests[index],
            stats,
        );
    }
    let started = Instant::now();
    let result =
        (|| {
            let mut scratch = [0_u8; SCRATCH_BYTES];
            stats.scratch_peak_bytes = stats.scratch_peak_bytes.max(SCRATCH_BYTES as u64);
            let first = offset as usize / PEER_CACHE_SEGMENT_BYTES;
            let last = (offset as usize + out.len()).div_ceil(PEER_CACHE_SEGMENT_BYTES);
            for index in first..last {
                let start = (index * PEER_CACHE_SEGMENT_BYTES) as u64;
                let stop = chunk.length.min(start + PEER_CACHE_SEGMENT_BYTES as u64);
                file.seek(SeekFrom::Start(body_start + start))
                    .map_err(Error::from)?;
                let mut position = start;
                let mut hash = blake3::Hasher::new();
                while position < stop {
                    let requested = (stop - position).min(SCRATCH_BYTES as u64) as usize;
                    stats.data_read_calls = stats.data_read_calls.saturating_add(1);
                    let read = file.read(&mut scratch[..requested]).map_err(Error::from)?;
                    if read == 0 {
                        return Err(corrupt("Peer cache segment is truncated"));
                    }
                    hash.update(&scratch[..read]);
                    stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(read as u64);
                    stats.hash_work.cache_segment_bytes = stats
                        .hash_work
                        .cache_segment_bytes
                        .saturating_add(read as u64);
                    stats.copied_bytes_total = stats
                        .copied_bytes_total
                        .saturating_add(copy_overlap(position, &scratch[..read], offset, out));
                    position += read as u64;
                }
                if hash.finalize().as_bytes() != &segments.digests[index] {
                    return Err(corrupt("Peer cache segment digest differs"));
                }
            }
            Ok(())
        })();
    stats.read_stages.stream_read_verify_ns = stats
        .read_stages
        .stream_read_verify_ns
        .saturating_add(elapsed_ns(started));
    result
}

fn read_private_segment(
    file: &File,
    position: u64,
    out: &mut [u8],
    expected: &[u8; 32],
    stats: &mut PeerCacheStats,
) -> Result<()> {
    // The original final result is private until integrity and late authority
    // checks succeed. Failed/partial reads may dirty it, never publish it.
    let started = Instant::now();
    let read_result = (|| {
        let mut cursor = 0;
        while cursor < out.len() {
            stats.data_read_calls = stats.data_read_calls.saturating_add(1);
            match file.read_at(&mut out[cursor..], position + cursor as u64) {
                Ok(0) => return Err(corrupt("Peer cache private segment is truncated")),
                Ok(read) => cursor += read,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(Error::from(error)),
            }
        }
        Ok(())
    })();
    stats.read_stages.data_read_ns = stats
        .read_stages
        .data_read_ns
        .saturating_add(elapsed_ns(started));
    read_result?;
    let started = Instant::now();
    let digest = blake3::hash(out);
    stats.hash_bytes_total = stats.hash_bytes_total.saturating_add(out.len() as u64);
    stats.hash_work.cache_segment_bytes = stats
        .hash_work
        .cache_segment_bytes
        .saturating_add(out.len() as u64);
    stats.read_stages.segment_verify_ns = stats
        .read_stages
        .segment_verify_ns
        .saturating_add(elapsed_ns(started));
    if digest.as_bytes() != expected {
        return Err(corrupt("Peer cache private segment digest differs"));
    }
    Ok(())
}

fn copy_overlap(position: u64, bytes: &[u8], offset: u64, out: &mut [u8]) -> u64 {
    let start = offset.max(position);
    let stop = (offset + out.len() as u64).min(position + bytes.len() as u64);
    if stop > start {
        out[(start - offset) as usize..(stop - offset) as usize]
            .copy_from_slice(&bytes[(start - position) as usize..(stop - position) as usize]);
        stop - start
    } else {
        0
    }
}

fn refresh_metadata(state: &mut CacheState) {
    let pointers = state.entries.len() * std::mem::size_of::<Option<Box<VerifiedDiskSegments>>>();
    let proofs = state
        .entries
        .values()
        .filter(|entry| entry.proof.is_some())
        .count()
        * proof_allocation_charge() as usize;
    state.stats.proof_metadata_bytes = (pointers + proofs) as u64;
    state.stats.proof_metadata_peak_bytes = state
        .stats
        .proof_metadata_peak_bytes
        .max(state.stats.proof_metadata_bytes);
}

fn proof_can_fit(state: &CacheState) -> bool {
    #[cfg(test)]
    let limit = state.proof_limit.unwrap_or(MAX_PEER_CACHE_PROOF_BYTES);
    #[cfg(not(test))]
    let limit = MAX_PEER_CACHE_PROOF_BYTES;
    let existing_pointers =
        (state.entries.len() * std::mem::size_of::<Option<Box<VerifiedDiskSegments>>>()) as u64;
    let all_pointers =
        (MAX_PEER_CACHE_ENTRIES * std::mem::size_of::<Option<Box<VerifiedDiskSegments>>>()) as u64;
    state
        .stats
        .proof_metadata_bytes
        .saturating_sub(existing_pointers)
        .saturating_add(all_pointers)
        .saturating_add(proof_allocation_charge())
        <= limit
}

fn proof_allocation_charge() -> u64 {
    // Conservatively round each allocation and include allocator bookkeeping.
    // This is an admission ledger, not an assertion about process RSS.
    let logical = std::mem::size_of::<VerifiedDiskSegments>() as u64;
    logical.div_ceil(64) * 64 + 64
}

fn install_proof(
    state: &mut CacheState,
    key: &str,
    identity: FileIdentity,
    segments: SegmentDigests,
) {
    if let Some(entry) = state.entries.get_mut(key) {
        entry.proof = None;
    }
    refresh_metadata(state);
    if proof_can_fit(state)
        && let Some(entry) = state.entries.get_mut(key)
    {
        entry.proof = Some(Box::new(VerifiedDiskSegments { identity, segments }));
    }
    refresh_metadata(state);
}

fn report_work(
    metrics: &super::read_work::ReadWorkMetrics,
    current: PeerCacheHashWork,
    before: PeerCacheHashWork,
) {
    metrics.hash(
        "cache_segment_hash",
        current
            .cache_segment_bytes
            .saturating_sub(before.cache_segment_bytes),
    );
    metrics.hash(
        "cache_full_hash",
        current
            .cache_fullscan_bytes
            .saturating_sub(before.cache_fullscan_bytes),
    );
    metrics.hash(
        "segment_index_bytes",
        current
            .segment_index_bytes
            .saturating_sub(before.segment_index_bytes),
    );
}

fn elapsed_ns(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

fn report_stages(
    metrics: &super::read_work::ReadWorkMetrics,
    current: PeerCacheReadStages,
    before: PeerCacheReadStages,
) {
    for (stage, current, before) in [
        ("prepare", current.prepare_ns, before.prepare_ns),
        ("data_read", current.data_read_ns, before.data_read_ns),
        (
            "segment_verify",
            current.segment_verify_ns,
            before.segment_verify_ns,
        ),
        (
            "stream_read_verify",
            current.stream_read_verify_ns,
            before.stream_read_verify_ns,
        ),
        ("finalize", current.finalize_ns, before.finalize_ns),
    ] {
        let delta = current.saturating_sub(before);
        if delta != 0 {
            metrics.peer_cache_stage_ns(stage, delta);
        }
    }
}

impl Drop for PeerCacheOperation<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = &self.cache.read_work {
            report_work(metrics, self.state.stats.hash_work, self.initial_work);
            metrics.peer_cache_data_reads(
                self.state
                    .stats
                    .data_read_calls
                    .saturating_sub(self.initial_reads),
            );
            metrics.copy(
                "cache_to_output",
                self.state
                    .stats
                    .copied_bytes_total
                    .saturating_sub(self.initial_copies),
            );
            report_stages(metrics, self.state.stats.read_stages, self.initial_stages);
            metrics.peer_cache_metadata(
                self.state.stats.proof_metadata_bytes,
                self.state.stats.proof_metadata_peak_bytes,
            );
        }
    }
}

impl Drop for DiskPeerCache {
    fn drop(&mut self) {
        if let Some(metrics) = &self.read_work
            && let Ok(state) = self.state.get_mut()
        {
            metrics.peer_cache_metadata(0, state.stats.proof_metadata_peak_bytes);
        }
    }
}

fn read_header(file: &mut File) -> Result<(Header, u64)> {
    let mut prefix = [0_u8; 12];
    file.read_exact(&mut prefix)
        .map_err(|_| corrupt("Peer cache header is truncated"))?;
    if &prefix[..8] != MAGIC {
        return Err(corrupt("Peer cache header magic differs"));
    }
    let count = u32::from_le_bytes([prefix[8], prefix[9], prefix[10], prefix[11]]) as usize;
    if count == 0 || count > MAX_HEADER_BYTES {
        return Err(corrupt("Peer cache header exceeds bound"));
    }
    let mut header = vec![0; count];
    file.read_exact(&mut header)
        .map_err(|_| corrupt("Peer cache header is truncated"))?;
    let header =
        serde_json::from_slice(&header).map_err(|_| corrupt("Peer cache header is invalid"))?;
    Ok((header, 12 + count as u64))
}

fn validate_chunk(chunk: &ChunkObject) -> Result<()> {
    if chunk.id.0.is_empty()
        || chunk.id.0.len() > 512
        || chunk.length == 0
        || chunk.encoding != ChunkEncoding::Raw
        || chunk.content_digest.algorithm != DigestAlgorithm::Blake3
    {
        return Err(invalid(
            "Peer cache requires a bounded immutable BLAKE3 object",
        ));
    }
    Ok(())
}

fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut prefix = PathBuf::new();
    for component in path.components() {
        if !matches!(component, Component::RootDir | Component::Normal(_)) {
            return Err(invalid("Peer cache directory is not normalized"));
        }
        prefix.push(component.as_os_str());
        match fs::symlink_metadata(&prefix) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(invalid("Peer cache path contains a symlink"));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(Error::from(error)),
        }
    }
    Ok(())
}

fn open_private(path: &Path) -> Result<(File, fs::Metadata)> {
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK)
        .open(path)
        .map_err(Error::from)?;
    let metadata = file.metadata().map_err(Error::from)?;
    validate_private_file(&metadata)?;
    Ok((file, metadata))
}

fn validate_private_file(metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != effective_uid()
        || metadata.nlink() != 1
    {
        return Err(invalid(
            "Peer cache file must be private, regular and not linked elsewhere",
        ));
    }
    Ok(())
}

fn effective_uid() -> u32 {
    // SAFETY: geteuid has no arguments and does not access borrowed memory.
    unsafe { libc::geteuid() }
}

fn fd_directory(root: &File) -> PathBuf {
    PathBuf::from(format!("/proc/self/fd/{}", root.as_raw_fd()))
}

fn allocated(metadata: &fs::Metadata) -> Result<u64> {
    metadata
        .blocks()
        .checked_mul(512)
        .ok_or_else(|| invalid("Peer cache allocated size overflow"))
}

fn file_identity(metadata: &fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

fn valid_ready_name(name: &str) -> bool {
    name.strip_suffix(".ready").is_some_and(|key| {
        key.len() == 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn invalid(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn unavailable(message: impl Into<String>) -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, message)
}
fn corrupt(message: impl Into<String>) -> Error {
    Error::coded(afs_error::NODE_TRANSFER_CORRUPT_DATA, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dfs::OperationId, node::chunk::StagedChunk};
    use std::os::unix::fs::{PermissionsExt, symlink};

    fn options(directory: PathBuf) -> PeerCacheOptions {
        PeerCacheOptions {
            directory,
            capacity_bytes: 256 * 1024,
            namespace_id: NamespaceId::new("test-namespace"),
        }
    }

    fn fill(cache: &DiskPeerCache, staged: &StagedChunk) -> Result<bool> {
        cache
            .operation()?
            .store_verified(&VerifiedPeerChunk::verify(&staged.chunk, staged.bytes())?)
    }

    #[test]
    fn peer_stream_sink_stays_private_and_charges_pending_before_body() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-pending"), vec![37; 64 * 1024]);
        let mut operation = cache.operation().unwrap();
        let mut sink = operation.begin_stream(&staged.chunk).unwrap();
        assert!(
            sink.state.pending_charge > 0,
            "private stream file must hold its disk reservation before body receipt"
        );
        sink.write(0, staged.bytes()).unwrap();
        assert!(
            sink.state.entries.is_empty(),
            "body completion alone cannot publish Ready"
        );
    }

    #[test]
    fn peer_stream_sink_complete_proof_stays_private_until_commit() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-commit"), vec![37; 64 * 1024]);
        let mut out = [0; 19];
        {
            let mut operation = cache.operation().unwrap();
            let mut sink = operation.begin_stream(&staged.chunk).unwrap();
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &staged.chunk,
                staged.bytes(),
                63,
                &mut out,
                &mut sink,
            )
            .unwrap();
            assert_eq!(proof.private_output(), &[37; 19]);
            assert!(sink.state.entries.is_empty());
            assert!(sink.commit(proof).unwrap());
        }
        assert_eq!(out, [37; 19]);
        assert_eq!(cache.stats().unwrap().entries, 1);
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 63, &mut out)
                .unwrap()
        );
    }

    #[test]
    fn peer_stream_completed_disk_receipt_cannot_commit_another_sink() {
        let temp = tempfile::tempdir().unwrap();
        let cache_a = DiskPeerCache::open(options(temp.path().join("a"))).unwrap();
        let cache_b = DiskPeerCache::open(options(temp.path().join("b"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("receipt-replay"), vec![19; 64 * 1024]);
        let mut operation_a = cache_a.operation().unwrap();
        let mut operation_b = cache_b.operation().unwrap();
        let mut sink_a = operation_a.begin_stream(&staged.chunk).unwrap();
        let mut sink_b = operation_b.begin_stream(&staged.chunk).unwrap();
        let mut out = [0; 8];
        let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
            &staged.chunk,
            staged.bytes(),
            0,
            &mut out,
            &mut sink_a,
        )
        .unwrap();
        sink_b.write(0, staged.bytes()).unwrap();
        assert!(sink_b.commit(proof).is_err());
        assert!(sink_b.state.entries.is_empty());
    }

    #[test]
    fn peer_stream_sink_quota_or_write_failure_keeps_verified_output_without_ready() {
        for write_failure in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let mut config = options(temp.path().join("cache"));
            if !write_failure {
                config.capacity_bytes = 4096;
            }
            let cache = DiskPeerCache::open(config).unwrap();
            let staged = StagedChunk::new(OperationId::new("stream-degraded"), vec![19; 64 * 1024]);
            let mut out = [0; 31];
            {
                let mut operation = cache.operation().unwrap();
                operation.state.fail_stream_write = write_failure;
                let mut sink = operation.begin_stream(&staged.chunk).unwrap();
                let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                    &staged.chunk,
                    staged.bytes(),
                    2,
                    &mut out,
                    &mut sink,
                )
                .unwrap();
                assert_eq!(proof.private_output(), &[19; 31]);
                assert!(!sink.commit(proof).unwrap());
            }
            assert_eq!(out, [19; 31]);
            assert_eq!(cache.stats().unwrap().entries, 0);
            assert_eq!(cache.state.lock().unwrap().pending_charge, 0);
        }
    }

    #[test]
    fn peer_stream_sink_unknown_created_file_and_failed_cleanup_remain_charged() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-unidentified"), vec![19; 64 * 1024]);
        {
            let mut operation = cache.operation().unwrap();
            operation.state.fail_stream_metadata = true;
            let sink = operation.begin_stream(&staged.chunk).unwrap();
            assert!(sink.file.is_none());
            assert!(sink.state.pending_charge > 0);
        }
        let state = cache.state.lock().unwrap();
        assert!(state.pending_charge > 0);
        assert_eq!(state.used_bytes, state.pending_charge);
        assert!(cache.directory().join(PENDING).exists());
    }

    #[test]
    fn peer_stream_sink_observed_extra_blocks_stay_charged_after_cleanup_failure() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("stream-charge"), vec![19; 64 * 1024]);
        let actual;
        {
            let mut operation = cache.operation().unwrap();
            let mut sink = operation.begin_stream(&staged.chunk).unwrap();
            let mut out = [0; 2];
            let proof = crate::node::rpc::peer::complete_peer_disk_read_for_test(
                &staged.chunk,
                staged.bytes(),
                2,
                &mut out,
                &mut sink,
            )
            .unwrap();
            actual = allocated(&sink.file.as_ref().unwrap().metadata().unwrap()).unwrap();
            sink.state.used_bytes = actual - 512;
            sink.state.pending_charge = actual - 512;
            sink.state.fail_remove = true;
            assert!(sink.commit(proof).is_err());
            assert_eq!(sink.state.used_bytes, actual);
            assert_eq!(sink.state.pending_charge, actual);
        }
        assert_eq!(cache.state.lock().unwrap().used_bytes, actual);
        assert!(cache.directory().join(PENDING).exists());
    }

    #[test]
    fn disk_peer_cache_admits_verified_object_and_reads_requested_range() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("cache-read"), vec![37; 64 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 63, &mut out)
                .unwrap()
        );
        assert_eq!(out, [37; 19]);
        let stats = cache.stats().unwrap();
        assert_eq!(stats.hash_bytes_total, staged.chunk.length);
        assert_eq!(stats.scratch_peak_bytes, 64 * 1024);
        assert_eq!(stats.entries, 1);
        assert!(stats.disk_bytes >= staged.chunk.length);
    }

    #[test]
    fn disk_peer_cache_complete_aligned_segment_reads_directly_without_payload_copy() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("direct-private-segment"),
            vec![73; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let before = cache.stats().unwrap();
        let mut out = vec![0; PEER_CACHE_SEGMENT_BYTES];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut out)
                .unwrap()
        );
        assert!(out.iter().all(|byte| *byte == 73));
        let after = cache.stats().unwrap();
        assert_eq!(
            (
                after.copied_bytes_total - before.copied_bytes_total,
                after.data_read_calls - before.data_read_calls
            ),
            (0, 1),
        );
        assert_eq!(
            after.hash_bytes_total - before.hash_bytes_total,
            PEER_CACHE_SEGMENT_BYTES as u64
        );
        assert!(after.read_stages.prepare_ns > before.read_stages.prepare_ns);
        assert!(after.read_stages.data_read_ns > before.read_stages.data_read_ns);
        assert!(after.read_stages.segment_verify_ns > before.read_stages.segment_verify_ns);
        assert!(after.read_stages.finalize_ns > before.read_stages.finalize_ns);
        assert_eq!(
            after.read_stages.stream_read_verify_ns,
            before.read_stages.stream_read_verify_ns
        );
    }

    #[test]
    fn disk_peer_cache_direct_private_short_file_returns_error_before_hashing() {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&[79; 17]).unwrap();
        let mut private = vec![0; PEER_CACHE_SEGMENT_BYTES];
        let mut stats = PeerCacheStats::default();

        let error = read_private_segment(&file, 0, &mut private, &[0; 32], &mut stats).unwrap_err();

        assert_eq!(error.code(), afs_error::NODE_TRANSFER_CORRUPT_DATA);
        assert!(stats.data_read_calls >= 2);
        assert_eq!(stats.hash_bytes_total, 0);
        assert_eq!(stats.hash_work.cache_segment_bytes, 0);
        assert_eq!(stats.read_stages.segment_verify_ns, 0);
        // A failed direct read can dirty private output; it is not a result
        // callers may publish, even though the available prefix was read.
        assert_eq!(&private[..17], &[79; 17]);
        assert!(private[17..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn disk_peer_cache_direct_private_digest_failure_retires_file_without_success() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("direct-private-failure"),
            vec![74; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let path = cache.path(&key);
        let mut bytes = fs::read(&path).unwrap();
        let body_start = bytes.len() - staged.bytes().len();
        bytes[body_start + PEER_CACHE_SEGMENT_BYTES + 17] ^= 1;
        fs::write(&path, bytes).unwrap();
        // Preserve trusted old digests while simulating an unobserved stat
        // change, so the failing current segment takes the direct-read path.
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key)
            .unwrap()
            .proof
            .as_mut()
            .unwrap()
            .identity = file_identity(&fs::metadata(&path).unwrap());
        let before = cache.stats().unwrap();
        let mut private = vec![0; PEER_CACHE_SEGMENT_BYTES];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut private,)
                .unwrap()
        );
        assert_ne!(private[17], 74);
        let after = cache.stats().unwrap();
        assert_eq!(after.copied_bytes_total - before.copied_bytes_total, 0);
        assert_eq!(after.data_read_calls - before.data_read_calls, 1);
        assert_eq!(
            after.hash_bytes_total - before.hash_bytes_total,
            PEER_CACHE_SEGMENT_BYTES as u64
        );
        assert_eq!(after.corruptions, 1);
        assert_eq!(after.proof_metadata_bytes, 0);
        assert!(!path.exists());
    }

    #[test]
    fn disk_peer_cache_stable_one_mib_hit_hashes_only_its_verified_segment() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(OperationId::new("segment-read"), vec![71; 4 * 1024 * 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let before = cache.stats().unwrap().hash_bytes_total;
        let mut out = vec![0; 1024 * 1024];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 1024 * 1024, &mut out)
                .unwrap()
        );
        assert!(out.iter().all(|byte| *byte == 71));
        assert_eq!(
            cache.stats().unwrap().hash_bytes_total - before,
            1024 * 1024
        );
    }

    #[test]
    fn disk_peer_cache_reopens_disk_objects_without_retained_payloads() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("cache-reopen"), vec![17; 64 * 1024]);
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
        }
        let cache = DiskPeerCache::open(config).unwrap();
        let mut out = [0; 31];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 2, &mut out)
                .unwrap()
        );
        assert_eq!(out, [17; 31]);
        assert_eq!(cache.stats().unwrap().entries, 1);
        assert_eq!(
            cache.stats().unwrap().hash_bytes_total,
            2 * staged.chunk.length
        );
    }

    #[test]
    fn disk_peer_cache_corrupt_or_truncated_body_is_retired_and_refillable() {
        for truncate in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
            let staged = StagedChunk::new(OperationId::new("cache-corrupt"), vec![33; 64 * 1024]);
            assert!(fill(&cache, &staged).unwrap());
            let path = cache.path(&cache.key(&staged.chunk).unwrap());
            let mut bytes = fs::read(&path).unwrap();
            if truncate {
                bytes.pop();
            } else {
                *bytes.last_mut().unwrap() ^= 1;
            }
            fs::write(&path, bytes).unwrap();
            let mut private = [0; 19];
            assert!(
                !cache
                    .operation()
                    .unwrap()
                    .read_range(&staged.chunk, 0, &mut private)
                    .unwrap()
            );
            assert!(!path.exists());
            assert_eq!(cache.stats().unwrap().disk_bytes, 0);
            assert!(fill(&cache, &staged).unwrap());
        }
    }

    #[test]
    fn disk_peer_cache_refuses_bad_proof_and_oversized_admission() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("bad-proof"), vec![21; 64 * 1024]);
        assert!(VerifiedPeerChunk::verify(&staged.chunk, &[0; 64 * 1024]).is_err());
        assert!(VerifiedPeerChunk::verify(&staged.chunk, &[]).is_err());
        let large = StagedChunk::new(
            OperationId::new("oversize"),
            vec![44; MAX_OBJECT_BYTES as usize + 1],
        );
        assert!(!fill(&cache, &large).unwrap());
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_evicts_with_actual_blocks_and_failed_delete_stays_charged() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 96 * 1024;
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        let first = StagedChunk::new(OperationId::new("first"), vec![1; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("second"), vec![2; 64 * 1024]);
        assert!(fill(&cache, &first).unwrap());
        let before = cache.stats().unwrap().disk_bytes;
        cache.state.lock().unwrap().fail_remove = true;
        assert!(fill(&cache, &second).is_err());
        assert_eq!(cache.stats().unwrap().disk_bytes, before);
        cache.state.lock().unwrap().fail_remove = false;
        assert!(fill(&cache, &second).unwrap());
        assert!(cache.stats().unwrap().disk_bytes <= config.capacity_bytes);
        assert_eq!(cache.stats().unwrap().evictions, 1);
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&first.chunk, 0, &mut [0; 1])
                .unwrap()
        );
    }

    #[test]
    fn disk_peer_cache_preserves_foreign_files_and_rejects_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("cache");
        fs::create_dir(&root).unwrap();
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        fs::write(root.join("foreign"), b"preserve").unwrap();
        assert!(DiskPeerCache::open(options(root.clone())).is_err());
        assert_eq!(fs::read(root.join("foreign")).unwrap(), b"preserve");
        let link = temp.path().join("link");
        symlink(&root, &link).unwrap();
        assert!(DiskPeerCache::open(options(link)).is_err());
    }

    #[test]
    fn disk_peer_cache_rejects_linked_ready_files_without_deleting_target() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("cache-link"), vec![27; 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let path = cache.path(&cache.key(&staged.chunk).unwrap());
        let target = temp.path().join("target");
        fs::write(&target, b"preserve").unwrap();
        fs::remove_file(&path).unwrap();
        symlink(&target, &path).unwrap();
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut [0; 1])
                .is_err()
        );
        assert_eq!(fs::read(target).unwrap(), b"preserve");
    }

    #[test]
    fn disk_peer_cache_recovery_retires_body_corruption_and_stale_private_temp() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("restart-corrupt"), vec![18; 64 * 1024]);
        let path;
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
            path = config
                .directory
                .join(format!("{}.ready", cache.key(&staged.chunk).unwrap()));
        }
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        let pending = config.directory.join(PENDING);
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&pending)
            .unwrap()
            .write_all(b"interrupted fill")
            .unwrap();
        let cache = DiskPeerCache::open(config).unwrap();
        assert!(!pending.exists());
        assert!(!path.exists());
        assert_eq!(cache.stats().unwrap().entries, 0);
        assert_eq!(cache.stats().unwrap().disk_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_namespace_identity_and_single_owner_are_enforced() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let staged = StagedChunk::new(OperationId::new("namespace"), vec![15; 1024]);
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
            assert!(DiskPeerCache::open(config.clone()).is_err());
        }
        let mut wrong = config;
        wrong.namespace_id = NamespaceId::new("other");
        assert!(DiskPeerCache::open(wrong).is_err());
    }

    #[test]
    fn disk_peer_cache_shutdown_stops_new_operations() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        cache.shutdown().unwrap();
        assert!(cache.operation().is_err());
    }

    #[test]
    fn disk_peer_cache_failed_pending_cleanup_keeps_reserved_blocks_charged() {
        let temp = tempfile::tempdir().unwrap();
        let cache = DiskPeerCache::open(options(temp.path().join("cache"))).unwrap();
        let staged = StagedChunk::new(OperationId::new("pending-failure"), vec![11; 64 * 1024]);
        let foreign = cache.path(&cache.key(&staged.chunk).unwrap());
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&foreign)
            .unwrap()
            .write_all(b"preserve foreign publication target")
            .unwrap();
        cache.state.lock().unwrap().fail_remove = true;
        assert!(fill(&cache, &staged).is_err());
        let stats = cache.stats().unwrap();
        assert!(stats.disk_bytes >= staged.chunk.length);
        assert!(stats.disk_bytes <= cache.options.capacity_bytes);
        assert!(cache.options.directory.join(PENDING).exists());
        assert_eq!(
            fs::read(&foreign).unwrap(),
            b"preserve foreign publication target"
        );
        cache.state.lock().unwrap().fail_remove = false;
        fs::remove_file(foreign).unwrap();
        assert!(fill(&cache, &staged).unwrap());
        assert!(!cache.options.directory.join(PENDING).exists());
    }

    #[test]
    fn disk_peer_cache_startup_limits_index_before_reading_unbounded_metadata() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 64 * 1024 * 1024;
        drop(DiskPeerCache::open(config.clone()).unwrap());
        for index in 0..=MAX_PEER_CACHE_ENTRIES {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(config.directory.join(format!("{index:064x}.ready")))
                .unwrap()
                .write_all(b"x")
                .unwrap();
        }
        let error = DiskPeerCache::open(config.clone()).unwrap_err();
        assert!(error.to_string().contains("index budget"));
        assert_eq!(
            fs::read_dir(config.directory).unwrap().count(),
            MAX_PEER_CACHE_ENTRIES + 2
        );
    }

    #[test]
    fn disk_peer_cache_pins_root_and_preserves_foreign_replacement() {
        let temp = tempfile::tempdir().unwrap();
        let config = options(temp.path().join("cache"));
        let cache = DiskPeerCache::open(config.clone()).unwrap();
        let staged = StagedChunk::new(OperationId::new("root-replacement"), vec![23; 1024]);
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let anchored = cache.path(&key);
        let original = fs::read(&anchored).unwrap();
        let mut operation = cache.operation().unwrap();
        fs::rename(&config.directory, temp.path().join("old-root")).unwrap();
        DirBuilder::new()
            .mode(0o700)
            .create(&config.directory)
            .unwrap();
        let foreign = config.directory.join(format!("{key}.ready"));
        fs::write(&foreign, b"foreign replacement must survive").unwrap();
        // The checked-before-rename operation cannot touch the new root.
        assert!(operation.read_range(&staged.chunk, 0, &mut [0; 1]).is_err());
        assert!(
            operation
                .store_verified(&VerifiedPeerChunk::verify(&staged.chunk, staged.bytes()).unwrap())
                .is_err()
        );
        // Even stale child paths still resolve to the originally pinned root.
        assert_eq!(fs::read(anchored).unwrap(), original);
        assert_eq!(
            fs::read(foreign).unwrap(),
            b"foreign replacement must survive"
        );
        assert!(!config.directory.join(MARKER).exists());
    }

    #[test]
    fn disk_peer_cache_segments_cover_cross_boundary_and_short_tail() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let bytes: Vec<_> = (0..2 * PEER_CACHE_SEGMENT_BYTES + 17)
            .map(|index| (index % 251) as u8)
            .collect();
        let staged = StagedChunk::new(OperationId::new("segment-boundaries"), bytes.clone());
        assert!(fill(&cache, &staged).unwrap());
        let mut cross = [0; 19];
        let start = PEER_CACHE_SEGMENT_BYTES - 7;
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, start as u64, &mut cross)
                .unwrap()
        );
        assert_eq!(cross, bytes[start..start + cross.len()]);
        let mut tail = [0; 14];
        let start = 2 * PEER_CACHE_SEGMENT_BYTES + 3;
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, start as u64, &mut tail)
                .unwrap()
        );
        assert_eq!(tail, bytes[start..]);
        let stats = cache.stats().unwrap();
        assert_eq!(
            stats.hash_work.cache_segment_bytes,
            (2 * PEER_CACHE_SEGMENT_BYTES + 17) as u64
        );
        assert_eq!(stats.hash_work.cache_fullscan_bytes, 0);
        assert_eq!(stats.hash_work.segment_index_bytes, 0);
        assert_eq!(stats.copied_bytes_total, 33);
        assert_eq!(stats.data_read_calls, 33);
        assert!(stats.read_stages.stream_read_verify_ns > 0);
        assert_eq!(stats.read_stages.data_read_ns, 0);
        assert_eq!(stats.read_stages.segment_verify_ns, 0);
    }

    #[test]
    fn disk_peer_cache_changed_metadata_rechecks_entire_root_before_range_return() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("changed-tail"),
            vec![51; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let path = cache.path(&cache.key(&staged.chunk).unwrap());
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        File::open(&path)
            .unwrap()
            .set_modified(std::time::UNIX_EPOCH)
            .unwrap();
        let mut private = [0; 19];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut private)
                .unwrap()
        );
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_unobserved_damage_is_detected_when_its_segment_is_read() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let staged = StagedChunk::new(
            OperationId::new("silent-tail"),
            vec![52; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let key = cache.key(&staged.chunk).unwrap();
        let path = cache.path(&key);
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        // Fault injection: model a disk-sector change with no observable stat
        // change. Preserve the trusted old digests, adjust only the observation.
        cache
            .state
            .lock()
            .unwrap()
            .entries
            .get_mut(&key)
            .unwrap()
            .proof
            .as_mut()
            .unwrap()
            .identity = file_identity(&fs::metadata(&path).unwrap());
        let mut first = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut first)
                .unwrap()
        );
        assert_eq!(first, [52; 19]);
        let mut private_tail = [0; 1];
        assert!(
            !cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, staged.chunk.length - 1, &mut private_tail)
                .unwrap()
        );
        assert_eq!(cache.stats().unwrap().corruptions, 1);
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_metadata_quota_falls_back_to_complete_verification() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let limit = std::mem::size_of::<Option<Box<VerifiedDiskSegments>>>() as u64;
        cache.state.lock().unwrap().proof_limit = Some(limit);
        let staged = StagedChunk::new(
            OperationId::new("metadata-quota"),
            vec![53; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(out, [53; 19]);
        let stats = cache.stats().unwrap();
        assert!(stats.proof_metadata_bytes <= limit);
        assert!(stats.proof_metadata_peak_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
        assert_eq!(stats.hash_work.cache_fullscan_bytes, staged.chunk.length);
        assert_eq!(stats.hash_work.cache_segment_bytes, 0);
        assert_eq!(stats.hash_work.segment_index_bytes, 0);
    }

    #[test]
    fn disk_peer_cache_restart_rebuilds_root_bound_segments_without_disk_digest_trust() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let staged = StagedChunk::new(
            OperationId::new("segment-restart"),
            vec![54; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        {
            let cache = DiskPeerCache::open(config.clone()).unwrap();
            assert!(fill(&cache, &staged).unwrap());
        }
        let cache = DiskPeerCache::open(config).unwrap();
        let recovered = cache.stats().unwrap();
        assert_eq!(
            recovered.hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(recovered.hash_work.segment_index_bytes, staged.chunk.length);
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_fullscan_bytes,
            staged.chunk.length
        );
        assert_eq!(
            cache.stats().unwrap().hash_work.cache_segment_bytes,
            PEER_CACHE_SEGMENT_BYTES as u64
        );
        assert!(cache.stats().unwrap().proof_metadata_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
    }

    #[test]
    fn disk_peer_cache_metadata_retires_on_replacement_eviction_and_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 96 * 1024;
        let cache = DiskPeerCache::open(config).unwrap();
        let first = StagedChunk::new(OperationId::new("metadata-first"), vec![55; 64 * 1024]);
        let second = StagedChunk::new(OperationId::new("metadata-second"), vec![56; 64 * 1024]);
        assert!(fill(&cache, &first).unwrap());
        let one = cache.stats().unwrap().proof_metadata_bytes;
        assert!(one > 0 && one <= MAX_PEER_CACHE_PROOF_BYTES);
        assert!(fill(&cache, &first).unwrap());
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, one);
        assert!(fill(&cache, &second).unwrap());
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, one);
        assert_eq!(cache.stats().unwrap().evictions, 1);
        cache.shutdown().unwrap();
        assert_eq!(cache.stats().unwrap().proof_metadata_bytes, 0);
        assert_eq!(cache.stats().unwrap().proof_metadata_peak_bytes, one);
    }

    #[test]
    fn disk_peer_cache_proof_quota_reserves_future_entry_pointers() {
        let staged = StagedChunk::new(OperationId::new("ledger-proof"), vec![3; 1]);
        let verified = VerifiedPeerChunk::verify(&staged.chunk, staged.bytes()).unwrap();
        let mut state = CacheState::default();
        for index in 0..MAX_PEER_CACHE_ENTRIES {
            let key = index.to_string();
            state.entries.insert(
                key.clone(),
                Entry {
                    chunk: staged.chunk.clone(),
                    charge: 0,
                    identity: (0, 0),
                    touched: 0,
                    proof: None,
                },
            );
            refresh_metadata(&mut state);
            install_proof(
                &mut state,
                &key,
                (0, 0, 1, 0, 0, 0, 0),
                verified.segments.clone(),
            );
            assert!(state.stats.proof_metadata_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
        }
        assert!(state.entries.values().any(|entry| entry.proof.is_none()));
        assert!(state.entries.values().any(|entry| entry.proof.is_some()));
        assert!(state.stats.proof_metadata_peak_bytes <= MAX_PEER_CACHE_PROOF_BYTES);
    }

    #[test]
    fn disk_peer_cache_reports_actual_segment_work_and_metadata_until_shutdown() {
        let temp = tempfile::tempdir().unwrap();
        let registry = afs_metrics::Registry::new();
        let mut config = options(temp.path().join("cache"));
        config.capacity_bytes = 8 * 1024 * 1024;
        let cache = DiskPeerCache::open(config).unwrap().with_read_work_metrics(
            super::super::read_work::ReadWorkMetrics::register(&registry).unwrap(),
        );
        let staged = StagedChunk::new(
            OperationId::new("metadata-metrics"),
            vec![57; 4 * PEER_CACHE_SEGMENT_BYTES],
        );
        assert!(fill(&cache, &staged).unwrap());
        let mut out = [0; 19];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, 0, &mut out)
                .unwrap()
        );
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(
            encoded.contains("afs_dfs_read_hash_bytes_total{stage=\"cache_segment_hash\"} 1048576")
        );
        assert!(encoded.contains("afs_dfs_read_hash_bytes_total{stage=\"cache_full_hash\"} 0"));
        assert!(encoded.contains("afs_dfs_peer_disk_cache_data_read_calls_total 16"));
        assert!(encoded.contains("afs_dfs_read_copy_bytes_total{stage=\"cache_to_output\"} 19"));
        assert!(encoded.contains(
            "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"stream_read_verify\"} 1"
        ));
        let mut direct = vec![0; PEER_CACHE_SEGMENT_BYTES];
        assert!(
            cache
                .operation()
                .unwrap()
                .read_range(&staged.chunk, PEER_CACHE_SEGMENT_BYTES as u64, &mut direct,)
                .unwrap()
        );
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(encoded.contains("afs_dfs_peer_disk_cache_data_read_calls_total 17"));
        assert!(encoded.contains("afs_dfs_read_copy_bytes_total{stage=\"cache_to_output\"} 19"));
        assert!(
            encoded.contains(
                "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"data_read\"} 1"
            )
        );
        assert!(encoded.contains(
            "afs_dfs_peer_disk_cache_stage_observations_total{stage=\"segment_verify\"} 1"
        ));
        let current = cache.stats().unwrap().proof_metadata_bytes;
        assert!(encoded.contains(&format!("afs_dfs_peer_disk_cache_metadata_bytes {current}")));
        cache.shutdown().unwrap();
        let encoded = afs_metrics::encode_text(&registry).unwrap();
        assert!(encoded.contains("afs_dfs_peer_disk_cache_metadata_bytes 0"));
        assert!(encoded.contains(&format!(
            "afs_dfs_peer_disk_cache_metadata_peak_bytes {current}"
        )));
    }
}
