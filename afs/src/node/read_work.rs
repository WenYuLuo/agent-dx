//! Fixed-cardinality work counters for immutable DFS reads, never authority or budgets.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

const FILL_STAGES: [&str; 7] = [
    "lock_wait",
    "lookup",
    "prepare",
    "receive_verify",
    "store",
    "deliver",
    "total",
];

/// Low-frequency synchronous scope clock. CPU belongs only to its calling
/// thread, not transport workers or remote nodes. Failure never affects reads.
#[derive(Clone, Copy)]
pub(crate) struct PeerFillClock {
    wall: std::time::Instant,
    cpu: Option<u64>,
    thread: std::thread::ThreadId,
}

impl PeerFillClock {
    pub(crate) fn start() -> Self {
        let wall = std::time::Instant::now();
        Self {
            wall,
            cpu: thread_cpu_ns(),
            thread: std::thread::current().id(),
        }
    }

    pub(crate) fn elapsed(&self) -> (u64, Option<u64>) {
        let cpu = if self.thread == std::thread::current().id() {
            self.cpu
                .and_then(|before| thread_cpu_ns()?.checked_sub(before))
        } else {
            None
        };
        let wall = u64::try_from(self.wall.elapsed().as_nanos()).unwrap_or(u64::MAX);
        (wall, cpu)
    }
}

fn thread_cpu_ns() -> Option<u64> {
    let mut value = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: value is writable, properly aligned and lives through this call.
    // CLOCK_THREAD_CPUTIME_ID reads the current thread and retains no pointer.
    if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut value) } != 0 {
        return None;
    }
    let seconds = u64::try_from(value.tv_sec).ok()?;
    let nanos = u64::try_from(value.tv_nsec)
        .ok()
        .filter(|n| *n < 1_000_000_000)?;
    seconds.checked_mul(1_000_000_000)?.checked_add(nanos)
}

#[derive(Clone, Debug)]
pub struct ReadWorkMetrics {
    scan_bytes: afs_metrics::IntCounter,
    hashes: afs_metrics::IntCounterVec,
    disk_cache_metadata_bytes: afs_metrics::IntGauge,
    disk_cache_metadata_peak_bytes: afs_metrics::IntGauge,
    scans: afs_metrics::IntCounter,
    copies: afs_metrics::IntCounterVec,
    peer_payload_bytes: afs_metrics::IntGauge,
    peer_payload_peak_bytes: afs_metrics::IntGauge,
    peer_payload_reserved: afs_metrics::IntCounter,
    peer_payload_released: afs_metrics::IntCounter,
    peer_object_cache: afs_metrics::IntCounterVec,
    read_rpc_calls: afs_metrics::IntCounterVec,
    peer_read_events: afs_metrics::IntCounterVec,
    disk_cache_events: afs_metrics::IntCounterVec,
    disk_cache_data_reads: afs_metrics::IntCounter,
    disk_cache_stage_ns: afs_metrics::IntCounterVec,
    disk_cache_stage_calls: afs_metrics::IntCounterVec,
    disk_cache_fill_stage_ns: afs_metrics::IntCounterVec,
    disk_cache_fill_stage_cpu_ns: afs_metrics::IntCounterVec,
    disk_cache_fill_stage_calls: afs_metrics::IntCounterVec,
    disk_cache_fill_stage_cpu_calls: afs_metrics::IntCounterVec,
    disk_cache_fill_bytes: afs_metrics::IntGauge,
    disk_cache_fill_peak_bytes: afs_metrics::IntGauge,
    disk_cache_private_peak_bytes: afs_metrics::IntGauge,
    stream_reserved_bytes: afs_metrics::IntGauge,
    stream_reserved_peak_bytes: afs_metrics::IntGauge,
    stream_private_peak_bytes: afs_metrics::IntGauge,
    stream_frame_bytes: afs_metrics::IntGauge,
    stream_frame_peak_bytes: afs_metrics::IntGauge,
    stream_frames: Arc<StreamFrameAccounting>,
}

impl ReadWorkMetrics {
    pub fn register(registry: &afs_metrics::Registry) -> Result<Self, afs_metrics::MetricsError> {
        registry.get_or_register(|registry| {
            let scan_bytes = afs_metrics::IntCounter::new(
                "afs_dfs_read_scan_bytes_total",
                "Logical file bytes read and hashed by complete immutable Chunk verification.",
            )?;
            let hashes = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_read_hash_bytes_total",
                    "Actual BLAKE3 input bytes by fixed read stage, including failed work; root and segment hashes are separate executions.",
                ),
                &["stage"],
            )?;
            for stage in [
                "root_local_scan", "stream_range_hash", "peer_range_in_receive",
                "root_in_receive", "root_fallback", "segment_index_bytes",
                "cache_segment_hash", "cache_full_hash",
            ] { hashes.with_label_values(&[stage]); }
            let disk_cache_metadata_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_disk_cache_metadata_bytes",
                "Charged segment digest and file identity metadata; excludes payload and directory bookkeeping.",
            )?;
            let disk_cache_metadata_peak_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_disk_cache_metadata_peak_bytes",
                "Peak charged segment digest and file identity metadata under its independent 1MiB limit.",
            )?;
            afs_metrics::register_collector(registry, &hashes)?;
            afs_metrics::register_collector(registry, &disk_cache_metadata_bytes)?;
            afs_metrics::register_collector(registry, &disk_cache_metadata_peak_bytes)?;
            let scans = afs_metrics::IntCounter::new(
                "afs_dfs_read_scans_total",
                "Immutable Chunk verification scans, including failed scans.",
            )?;
            let copies = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_read_copy_bytes_total",
                    "Explicit DFS read payload copies; excludes codec, TLS and kernel copies.",
                ),
                &["stage"],
            )?;
            let peer_payload_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_read_payload_bytes",
                "Charged Peer read payload capacity, including resident and still-pinned objects.",
            )?;
            let peer_payload_peak_bytes = afs_metrics::IntGauge::new(
                "afs_dfs_peer_read_payload_peak_bytes",
                "Peak charged capacity in the shared Peer read payload budget.",
            )?;
            let peer_payload_reserved = afs_metrics::IntCounter::new(
                "afs_dfs_peer_read_payload_reserved_bytes_total",
                "Peer read payload capacity admitted by the shared budget.",
            )?;
            let peer_payload_released = afs_metrics::IntCounter::new(
                "afs_dfs_peer_read_payload_released_bytes_total",
                "Peer read payload capacity released after its final owner drops.",
            )?;
            let peer_object_cache = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_read_object_cache_total",
                    "Current verified-object slot lookups; never a source of read authority.",
                ),
                &["result"],
            )?;
            let read_rpc_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_read_rpc_calls_total",
                    "Client read RPC invocations, including failures; not server success counts.",
                ),
                &["method"],
            )?;
            let peer_read_events = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_read_events_total",
                    "Peer read task submissions and body-frame events; not elapsed time or completion counts.",
                ),
                &["event"],
            )?;
            let disk_cache_events = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_events_total", "Disposable native Peer disk cache outcomes; no authority or durability."), &["result"],
            )?;
            let disk_cache_data_reads = afs_metrics::IntCounter::new(
                "afs_dfs_peer_disk_cache_data_read_calls_total",
                "Actual cache body-region read attempts, including failed or interrupted attempts; excludes header and trailing EOF probes.",
            )?;
            let disk_cache_stage_ns = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_disk_cache_stage_nanoseconds_total",
                    "Aggregated cache read-range wall time by fixed stage; not exclusive Node CPU or full FUSE latency.",
                ),
                &["stage"],
            )?;
            let disk_cache_stage_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new(
                    "afs_dfs_peer_disk_cache_stage_observations_total",
                    "Cache stage aggregates observed once per cache operation, including failed work.",
                ),
                &["stage"],
            )?;
            for stage in ["prepare", "data_read", "segment_verify", "stream_read_verify", "finalize"] {
                disk_cache_stage_ns.with_label_values(&[stage]);
                disk_cache_stage_calls.with_label_values(&[stage]);
            }
            afs_metrics::register_collector(registry, &disk_cache_data_reads)?;
            afs_metrics::register_collector(registry, &disk_cache_stage_ns)?;
            afs_metrics::register_collector(registry, &disk_cache_stage_calls)?;
            let disk_cache_fill_stage_ns = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_nanoseconds_total", "Whole-object fill wall time by fixed stage, including failures; total is a parent, not an extra exclusive stage."), &["stage"],
            )?;
            let disk_cache_fill_stage_cpu_ns = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_thread_cpu_nanoseconds_total", "Calling-thread CPU for synchronous fill scopes; excludes transport workers and remote CPU, never pure network wait."), &["stage"],
            )?;
            let disk_cache_fill_stage_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_observations_total", "Fill stage observations, including failed work; prepare can contain multiple disjoint scopes per fill."), &["stage"],
            )?;
            let disk_cache_fill_stage_cpu_calls = afs_metrics::IntCounterVec::new(
                afs_metrics::Opts::new("afs_dfs_peer_disk_cache_fill_stage_thread_cpu_observations_total", "Fill observations with a valid same-thread CPU sample; missing CPU is not an observed zero."), &["stage"],
            )?;
            for stage in FILL_STAGES {
                disk_cache_fill_stage_ns.with_label_values(&[stage]);
                disk_cache_fill_stage_cpu_ns.with_label_values(&[stage]);
                disk_cache_fill_stage_calls.with_label_values(&[stage]);
                disk_cache_fill_stage_cpu_calls.with_label_values(&[stage]);
            }
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_ns)?;
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_cpu_ns)?;
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_calls)?;
            afs_metrics::register_collector(registry, &disk_cache_fill_stage_cpu_calls)?;
            let disk_cache_fill_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_fill_bytes", "Temporary complete-object fill bytes, held until the body is dropped; no resident payload cache.")?;
            let disk_cache_fill_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_fill_peak_bytes", "Peak temporary complete-object fill bytes, serialized by the cache operation guard.")?;
            let disk_cache_private_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_private_peak_bytes", "Peak complete-object fill plus private output for one native read; not Node RSS.")?;
            let stream_reserved_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_reserved_bytes", "Conservative same-budget 4MiB streaming fill reservation held until producer and frames drain; not allocated RAM or admission.")?;
            let stream_reserved_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_reserved_peak_bytes", "Peak conservative streaming fill reservation; mutually exclusive with legacy full-body fill for one cache operation.")?;
            let stream_private_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_private_peak_bytes", "Peak conservative streaming fill reservation plus private output; not Node RSS.")?;
            let stream_frame_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_frame_bytes", "Owned reusable payload block capacity through free pool, producer, queue and consumer; excludes bounded control fields and transport buffers.")?;
            let stream_frame_peak_bytes = afs_metrics::IntGauge::new("afs_dfs_peer_disk_cache_stream_frame_peak_bytes", "Peak owned streaming payload backing capacity; not reservation or total transport RAM.")?;
            for gauge in [&stream_reserved_bytes, &stream_reserved_peak_bytes, &stream_private_peak_bytes, &stream_frame_bytes, &stream_frame_peak_bytes] {
                afs_metrics::register_collector(registry, gauge)?;
            }
            for result in ["hit", "miss", "fill", "fill_failed", "corrupt_or_io", "fallback"] { disk_cache_events.with_label_values(&[result]); }
            afs_metrics::register_collector(registry, &disk_cache_events)?;
            afs_metrics::register_collector(registry, &disk_cache_fill_bytes)?;
            afs_metrics::register_collector(registry, &disk_cache_fill_peak_bytes)?;
            afs_metrics::register_collector(registry, &disk_cache_private_peak_bytes)?;
            afs_metrics::register_collector(registry, &scan_bytes)?;
            afs_metrics::register_collector(registry, &scans)?;
            afs_metrics::register_collector(registry, &copies)?;
            afs_metrics::register_collector(registry, &peer_payload_bytes)?;
            afs_metrics::register_collector(registry, &peer_payload_peak_bytes)?;
            afs_metrics::register_collector(registry, &peer_payload_reserved)?;
            afs_metrics::register_collector(registry, &peer_payload_released)?;
            afs_metrics::register_collector(registry, &peer_object_cache)?;
            afs_metrics::register_collector(registry, &read_rpc_calls)?;
            afs_metrics::register_collector(registry, &peer_read_events)?;
            for event in [
                "stream_pipeline",
                "stream_frame",
                "stream_block_alloc",
                "stream_block_handoff",
                "stream_block_write",
                "blocking_authorize",
                "blocking_produce",
                "blocking_combined",
                "data_frame",
                "body_vec_allocation",
                "shared_frame",
            ] {
                peer_read_events.with_label_values(&[event]);
            }
            for method in [
                "get_inode",
                "get_file_version",
                "get_chunk_sources",
                "validate_read_grants",
                "peer_read_ranges",
            ] {
                read_rpc_calls.with_label_values(&[method]);
            }
            for result in ["hit", "miss"] {
                peer_object_cache.with_label_values(&[result]);
            }
            for stage in [
                "peer_frame_detach",
                "peer_frame_to_block",
                "scan_to_range",
                "pinned_range_to_output",
                "range_to_frame",
                "frame_to_attempt",
                "attempt_to_staging",
                "staging_to_output",
            ] {
                copies.with_label_values(&[stage]);
            }
            Ok(Self {
                scan_bytes,
                hashes,
                disk_cache_metadata_bytes,
                disk_cache_metadata_peak_bytes,
                scans,
                copies,
                peer_payload_bytes,
                peer_payload_peak_bytes,
                peer_payload_reserved,
                peer_payload_released,
                peer_object_cache,
                read_rpc_calls,
                peer_read_events,
                disk_cache_events,
                disk_cache_data_reads,
                disk_cache_stage_ns,
                disk_cache_stage_calls,
                disk_cache_fill_stage_ns,
                disk_cache_fill_stage_cpu_ns,
                disk_cache_fill_stage_calls,
                disk_cache_fill_stage_cpu_calls,
                disk_cache_fill_bytes,
                disk_cache_fill_peak_bytes,
                disk_cache_private_peak_bytes,
                stream_reserved_bytes,
                stream_reserved_peak_bytes,
                stream_private_peak_bytes,
                stream_frame_bytes,
                stream_frame_peak_bytes,
                stream_frames: Arc::new(StreamFrameAccounting::default()),
            })
        })
    }

    pub(crate) fn hash(&self, stage: &'static str, bytes: u64) {
        if matches!(
            stage,
            "root_local_scan"
                | "stream_range_hash"
                | "peer_range_in_receive"
                | "root_in_receive"
                | "root_fallback"
                | "segment_index_bytes"
                | "cache_segment_hash"
                | "cache_full_hash"
        ) {
            self.hashes.with_label_values(&[stage]).inc_by(bytes);
        }
    }

    /// Observes the cache's enforcing metadata ledger, never grants admission.
    pub(crate) fn peer_cache_metadata(&self, current: u64, peak: u64) {
        if let (Ok(current), Ok(peak)) = (i64::try_from(current), i64::try_from(peak)) {
            self.disk_cache_metadata_bytes.set(current);
            self.disk_cache_metadata_peak_bytes.set(peak);
        }
    }

    pub(crate) fn peer_cache_data_reads(&self, calls: u64) {
        self.disk_cache_data_reads.inc_by(calls);
    }

    pub(crate) fn peer_cache_stage_ns(&self, stage: &'static str, ns: u64) {
        if ns != 0
            && matches!(
                stage,
                "prepare" | "data_read" | "segment_verify" | "stream_read_verify" | "finalize"
            )
        {
            self.disk_cache_stage_ns
                .with_label_values(&[stage])
                .inc_by(ns);
            self.disk_cache_stage_calls
                .with_label_values(&[stage])
                .inc();
        }
    }

    pub(crate) fn peer_cache_fill_stage(
        &self,
        stage: &'static str,
        wall_ns: u64,
        cpu_ns: Option<u64>,
    ) {
        if !FILL_STAGES.contains(&stage) {
            return;
        }
        self.disk_cache_fill_stage_ns
            .with_label_values(&[stage])
            .inc_by(wall_ns);
        self.disk_cache_fill_stage_calls
            .with_label_values(&[stage])
            .inc();
        if let Some(cpu_ns) = cpu_ns {
            self.disk_cache_fill_stage_cpu_ns
                .with_label_values(&[stage])
                .inc_by(cpu_ns);
            self.disk_cache_fill_stage_cpu_calls
                .with_label_values(&[stage])
                .inc();
        }
    }

    pub(crate) fn peer_disk_cache_event(&self, result: &'static str) {
        if matches!(
            result,
            "hit" | "miss" | "fill" | "fill_failed" | "corrupt_or_io" | "fallback"
        ) {
            self.disk_cache_events.with_label_values(&[result]).inc();
        }
    }

    /// Observation only. The cache operation guard and read budget enforce admission.
    pub(crate) fn peer_disk_cache_fill_work(
        &self,
        bytes: usize,
        output: usize,
    ) -> PeerCacheFillWork {
        if let (Ok(bytes), Ok(total)) = (
            i64::try_from(bytes),
            i64::try_from(bytes.saturating_add(output)),
        ) {
            self.disk_cache_fill_bytes.add(bytes);
            self.disk_cache_fill_peak_bytes.set(
                self.disk_cache_fill_peak_bytes
                    .get()
                    .max(self.disk_cache_fill_bytes.get()),
            );
            self.disk_cache_private_peak_bytes
                .set(self.disk_cache_private_peak_bytes.get().max(total));
        }
        PeerCacheFillWork {
            metrics: self.clone(),
            bytes,
        }
    }

    /// Observation of the existing serialized fill allowance, never admission.
    pub(crate) fn peer_stream_pipeline(&self, output: usize) -> PeerCacheStreamWork {
        const RESERVED: i64 = 4 * 1024 * 1024;
        self.stream_reserved_bytes.add(RESERVED);
        self.stream_reserved_peak_bytes.set(
            self.stream_reserved_peak_bytes
                .get()
                .max(self.stream_reserved_bytes.get()),
        );
        let total = i64::try_from(output)
            .unwrap_or(i64::MAX)
            .saturating_add(RESERVED);
        self.stream_private_peak_bytes
            .set(self.stream_private_peak_bytes.get().max(total));
        PeerCacheStreamWork {
            metrics: self.clone(),
        }
    }

    /// Fixed payload owners and control permits enforce bounds; this guard follows ownership.
    pub(crate) fn peer_stream_frame(&self, backing_bytes: usize) -> PeerStreamFrameWork {
        let bytes = u64::try_from(backing_bytes)
            .ok()
            .filter(|v| *v <= i64::MAX as u64)
            .unwrap_or(0);
        let current = self
            .stream_frames
            .current
            .fetch_add(bytes, Ordering::Relaxed)
            + bytes;
        self.stream_frame_bytes.add(bytes as i64);
        let previous = self
            .stream_frames
            .peak
            .fetch_max(current, Ordering::Relaxed);
        if current > previous {
            // Commutative increments preserve the peak even when frame owners race.
            self.stream_frame_peak_bytes
                .add((current - previous) as i64);
        }
        PeerStreamFrameWork {
            metrics: self.clone(),
            bytes,
        }
    }

    pub(crate) fn scan(&self) -> ScanWork<'_> {
        ScanWork {
            metrics: self,
            bytes: 0,
            copied: 0,
        }
    }

    pub(crate) fn copy(&self, stage: &'static str, bytes: u64) {
        self.copies.with_label_values(&[stage]).inc_by(bytes);
    }

    pub(crate) fn peer_payload_reserved(&self, bytes: u64, peak_increment: u64) {
        // The enforcing budget admits at most 8MiB. Metrics remain optional
        // observations and cannot grant admission, even for an invalid input.
        if let (Ok(bytes_i64), Ok(peak_i64)) = (i64::try_from(bytes), i64::try_from(peak_increment))
        {
            self.peer_payload_bytes.add(bytes_i64);
            self.peer_payload_peak_bytes.add(peak_i64);
            self.peer_payload_reserved.inc_by(bytes);
        }
    }

    pub(crate) fn peer_payload_released(&self, bytes: u64) {
        if let Ok(bytes_i64) = i64::try_from(bytes) {
            self.peer_payload_bytes.sub(bytes_i64);
            self.peer_payload_released.inc_by(bytes);
        }
    }

    pub(crate) fn peer_object_cache(&self, result: &'static str) {
        if matches!(result, "hit" | "miss") {
            self.peer_object_cache.with_label_values(&[result]).inc();
        }
    }

    pub(crate) fn read_rpc(&self, method: &'static str) {
        if matches!(
            method,
            "get_inode"
                | "get_file_version"
                | "get_chunk_sources"
                | "validate_read_grants"
                | "peer_read_ranges"
        ) {
            self.read_rpc_calls.with_label_values(&[method]).inc();
        }
    }

    pub(crate) fn peer_read_event(&self, event: &'static str) {
        if matches!(
            event,
            "stream_pipeline"
                | "stream_frame"
                | "stream_block_alloc"
                | "stream_block_handoff"
                | "stream_block_write"
                | "blocking_authorize"
                | "blocking_produce"
                | "blocking_combined"
                | "data_frame"
                | "body_vec_allocation"
                | "shared_frame"
        ) {
            self.peer_read_events.with_label_values(&[event]).inc();
        }
    }
}

#[derive(Debug, Default)]
struct StreamFrameAccounting {
    current: AtomicU64,
    peak: AtomicU64,
}

pub(crate) struct PeerCacheStreamWork {
    metrics: ReadWorkMetrics,
}

impl Drop for PeerCacheStreamWork {
    fn drop(&mut self) {
        self.metrics.stream_reserved_bytes.sub(4 * 1024 * 1024);
    }
}

pub(crate) struct PeerStreamFrameWork {
    metrics: ReadWorkMetrics,
    bytes: u64,
}

impl Drop for PeerStreamFrameWork {
    fn drop(&mut self) {
        self.metrics
            .stream_frames
            .current
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.metrics.stream_frame_bytes.sub(self.bytes as i64);
    }
}

pub(crate) struct PeerCacheFillWork {
    metrics: ReadWorkMetrics,
    bytes: usize,
}

impl Drop for PeerCacheFillWork {
    fn drop(&mut self) {
        if let Ok(bytes) = i64::try_from(self.bytes) {
            self.metrics.disk_cache_fill_bytes.sub(bytes);
        }
    }
}

pub(crate) struct ScanWork<'a> {
    metrics: &'a ReadWorkMetrics,
    pub(crate) bytes: u64,
    pub(crate) copied: u64,
}

impl Drop for ScanWork<'_> {
    fn drop(&mut self) {
        // Aggregate once per scan rather than adding atomics to each 64KiB read.
        self.metrics.scans.inc();
        self.metrics.scan_bytes.inc_by(self.bytes);
        self.metrics.hash("root_local_scan", self.bytes);
        self.metrics.copy("scan_to_range", self.copied);
    }
}

#[cfg(test)]
mod tests {
    use super::ReadWorkMetrics;

    #[test]
    fn streaming_work_keeps_last_frame_charged_and_reservation_distinct() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        let pipeline = metrics.peer_stream_pipeline(1024 * 1024);
        let frame = metrics.peer_stream_frame(65536);
        let queued = same.peer_stream_frame(131072);
        assert_eq!(same.stream_frame_bytes.get(), 196608);
        assert_eq!(same.stream_frame_peak_bytes.get(), 196608);
        assert_eq!(same.stream_reserved_bytes.get(), 4 * 1024 * 1024);
        assert_eq!(same.stream_private_peak_bytes.get(), 5 * 1024 * 1024);
        assert_eq!(same.disk_cache_fill_bytes.get(), 0);
        std::thread::spawn(move || drop(queued)).join().unwrap();
        assert_eq!(same.stream_frame_bytes.get(), 65536);
        drop(frame);
        assert_eq!(same.stream_frame_bytes.get(), 0);
        assert_eq!(same.stream_frame_peak_bytes.get(), 196608);
        drop(pipeline);
        assert_eq!(same.stream_reserved_bytes.get(), 0);
    }

    #[test]
    fn fill_stage_metrics_keep_missing_cpu_separate_and_labels_bounded() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.peer_cache_fill_stage("receive_verify", 100, Some(30));
        metrics.peer_cache_fill_stage("receive_verify", 50, None);
        metrics.peer_cache_fill_stage("store", 0, Some(0));
        metrics.peer_cache_fill_stage("chunk-id-must-not-be-a-label", 999, Some(999));
        assert_eq!(
            same.disk_cache_fill_stage_ns
                .with_label_values(&["receive_verify"])
                .get(),
            150
        );
        assert_eq!(
            same.disk_cache_fill_stage_cpu_ns
                .with_label_values(&["receive_verify"])
                .get(),
            30
        );
        assert_eq!(
            same.disk_cache_fill_stage_calls
                .with_label_values(&["receive_verify"])
                .get(),
            2
        );
        assert_eq!(
            same.disk_cache_fill_stage_cpu_calls
                .with_label_values(&["receive_verify"])
                .get(),
            1
        );
        assert_eq!(
            same.disk_cache_fill_stage_cpu_calls
                .with_label_values(&["store"])
                .get(),
            1
        );
        for family in registry
            .gather()
            .into_iter()
            .filter(|f| f.name().starts_with("afs_dfs_peer_disk_cache_fill_stage_"))
        {
            assert_eq!(family.get_metric().len(), 7);
        }
    }

    #[test]
    fn fill_clock_never_subtracts_cpu_from_another_thread() {
        let clock = super::PeerFillClock::start();
        let cpu = std::thread::spawn(move || clock.elapsed().1)
            .join()
            .unwrap();
        assert_eq!(cpu, None);
    }

    #[test]
    fn disk_read_work_observes_actual_calls_and_fixed_stage_aggregates() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.peer_cache_data_reads(2);
        metrics.peer_cache_stage_ns("data_read", 100);
        metrics.peer_cache_stage_ns("data_read", 30);
        metrics.peer_cache_stage_ns("segment_verify", 40);
        metrics.peer_cache_stage_ns("chunk-id-must-not-be-a-label", 50);
        assert_eq!(same.disk_cache_data_reads.get(), 2);
        assert_eq!(
            same.disk_cache_stage_ns
                .with_label_values(&["data_read"])
                .get(),
            130
        );
        assert_eq!(
            same.disk_cache_stage_calls
                .with_label_values(&["data_read"])
                .get(),
            2
        );
        let stages = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_peer_disk_cache_stage_nanoseconds_total")
            .unwrap();
        assert_eq!(stages.get_metric().len(), 5);
    }

    #[test]
    fn hash_and_metadata_metrics_are_fixed_and_do_not_grant_budget() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.hash("root_in_receive", 4096);
        metrics.hash("segment_index_bytes", 4096);
        metrics.hash("request-id-must-not-be-a-label", 1);
        metrics.peer_cache_metadata(256, 512);
        assert_eq!(
            same.hashes.with_label_values(&["root_in_receive"]).get(),
            4096
        );
        assert_eq!(
            same.hashes
                .with_label_values(&["segment_index_bytes"])
                .get(),
            4096
        );
        assert_eq!(
            same.hashes.with_label_values(&["stream_range_hash"]).get(),
            0
        );
        assert_eq!(same.disk_cache_metadata_bytes.get(), 256);
        assert_eq!(same.disk_cache_metadata_peak_bytes.get(), 512);
        let hashes = registry
            .gather()
            .into_iter()
            .find(|family| family.name() == "afs_dfs_read_hash_bytes_total")
            .unwrap();
        assert_eq!(hashes.get_metric().len(), 8);
    }

    #[test]
    fn peer_disk_fill_observation_releases_only_when_work_guard_drops() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        {
            let _work = metrics.peer_disk_cache_fill_work(4 * 1024 * 1024, 1024 * 1024);
            assert_eq!(metrics.disk_cache_fill_bytes.get(), 4 * 1024 * 1024);
            assert_eq!(metrics.disk_cache_fill_peak_bytes.get(), 4 * 1024 * 1024);
            assert_eq!(metrics.disk_cache_private_peak_bytes.get(), 5 * 1024 * 1024);
        }
        assert_eq!(metrics.disk_cache_fill_bytes.get(), 0);
        assert_eq!(metrics.disk_cache_fill_peak_bytes.get(), 4 * 1024 * 1024);
    }

    #[test]
    fn peer_read_event_metrics_are_fixed_and_shared() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        for event in [
            "stream_pipeline",
            "stream_frame",
            "stream_block_alloc",
            "stream_block_handoff",
            "stream_block_write",
            "blocking_authorize",
            "blocking_produce",
            "blocking_combined",
            "data_frame",
            "body_vec_allocation",
            "shared_frame",
        ] {
            metrics.peer_read_event(event);
            assert_eq!(same.peer_read_events.with_label_values(&[event]).get(), 1);
        }
        metrics.peer_read_event("inode-or-request-id-must-not-be-a-label");
        let families = registry.gather();
        let events = families
            .iter()
            .find(|family| family.name() == "afs_dfs_peer_read_events_total")
            .unwrap();
        assert_eq!(events.get_metric().len(), 11);
    }

    #[test]
    fn native_rpc_metrics_have_fixed_methods_and_shared_registration() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        for method in [
            "get_inode",
            "get_file_version",
            "get_chunk_sources",
            "validate_read_grants",
            "peer_read_ranges",
        ] {
            metrics.read_rpc(method);
            assert_eq!(same.read_rpc_calls.with_label_values(&[method]).get(), 1);
        }
        metrics.read_rpc("inode-id-is-not-a-label");
        let families = registry.gather();
        let calls = families
            .iter()
            .find(|family| family.name() == "afs_dfs_read_rpc_calls_total")
            .unwrap();
        assert_eq!(calls.get_metric().len(), 5);
    }

    #[test]
    fn peer_payload_metrics_account_shared_lifetimes_and_reuse_registration() {
        let registry = afs_metrics::Registry::new();
        let metrics = ReadWorkMetrics::register(&registry).unwrap();
        let same = ReadWorkMetrics::register(&registry).unwrap();
        metrics.peer_payload_reserved(4 * 1024 * 1024, 4 * 1024 * 1024);
        same.peer_payload_reserved(1024 * 1024, 1024 * 1024);
        same.peer_payload_released(4 * 1024 * 1024);
        assert_eq!(metrics.peer_payload_bytes.get(), 1024 * 1024);
        assert_eq!(metrics.peer_payload_peak_bytes.get(), 5 * 1024 * 1024);
        metrics.peer_payload_released(1024 * 1024);
        assert_eq!(same.peer_payload_bytes.get(), 0);
        assert_eq!(
            metrics.peer_payload_reserved.get(),
            metrics.peer_payload_released.get()
        );
        metrics.peer_object_cache("hit");
        metrics.peer_object_cache("miss");
        metrics.peer_object_cache("unbounded-label");
        assert_eq!(same.peer_object_cache.with_label_values(&["hit"]).get(), 1);
        assert_eq!(same.peer_object_cache.with_label_values(&["miss"]).get(), 1);
    }
}
