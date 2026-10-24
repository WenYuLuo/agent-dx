//! Bounded object scheduling; bodies remain request output or disk.

use std::{
    future::Future,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use afs_error::{Error, Result};

use super::{
    chunk::outboard::{OutboardBudget, OutboardLease},
    read_work::ReadWorkMetrics,
};

const MAX_FETCH_TASKS: usize = 8;
const SEQUENTIAL_DISTANCE: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct PrefetchControl {
    cancellation: FetchCancellation,
    check: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

impl PrefetchControl {
    pub(super) fn new(
        cancellation: FetchCancellation,
        check: impl Fn() -> Result<()> + Send + Sync + 'static,
    ) -> Self {
        Self {
            cancellation,
            check: Arc::new(check),
        }
    }
    pub(super) fn check(&self) -> Result<()> {
        if self.cancellation.is_cancelled() {
            return Err(stopped());
        }
        (self.check)()
    }
    pub(super) async fn cancelled(&self) {
        // Quiet peers may produce no frames on which to recheck their handle,
        // cached grant or original deadline. This fixed metadata-only poll
        // bounds known invalidation latency without another task or payload.
        loop {
            if self.check().is_err() {
                return;
            }
            tokio::select! {
                _ = self.cancellation.cancelled() => return,
                _ = tokio::time::sleep(Duration::from_millis(25)) => {},
            }
        }
    }
}

/// One Node signal wakes producers even while the remote peer is silent.
/// Per-handle validity is checked by the job's bounded immutable context.
#[derive(Clone, Default)]
pub(super) struct FetchCancellation(Arc<FetchCancellationState>);

#[derive(Default)]
struct FetchCancellationState {
    cancelled: AtomicBool,
    changed: tokio::sync::Notify,
}

impl FetchCancellation {
    pub(super) fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub(super) async fn cancelled(&self) {
        loop {
            let changed = self.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.is_cancelled() {
                return;
            }
            changed.await;
        }
    }
}

/// Fixed-size handle hint. Its scope hash is advisory, never authority.
#[derive(Default)]
pub(super) struct ReadSequence {
    scope: Option<[u8; 32]>,
    last_read_end: Option<u64>,
    window: usize,
    generation: u64,
}

impl ReadSequence {
    pub(super) fn generation(&self) -> u64 {
        self.generation
    }

    pub(super) fn invalidate(&mut self) {
        self.scope = None;
        self.last_read_end = None;
        self.window = 0;
        self.generation = self.generation.saturating_add(1);
    }

    pub(super) fn record(
        &mut self,
        scope: [u8; 32],
        offset: u64,
        count: usize,
        limit: usize,
    ) -> usize {
        if limit == 0 || count == 0 || self.generation == u64::MAX {
            return 0;
        }
        let Some(end) = offset.checked_add(count as u64) else {
            self.invalidate();
            return 0;
        };
        let same_scope = self.scope == Some(scope);
        let sequential = same_scope
            && self
                .last_read_end
                .is_some_and(|previous| offset.abs_diff(previous) <= SEQUENTIAL_DISTANCE);
        if !same_scope {
            self.invalidate();
            self.window = 1;
        } else if sequential {
            self.window = self.window.saturating_mul(2).max(1).min(limit);
        } else {
            self.window = 0;
            self.generation = self.generation.saturating_add(1);
        }
        self.scope = Some(scope);
        self.last_read_end = Some(end);
        self.window.min(limit)
    }
}

/// Join handles contain no object key or body. Cache FillFlight remains the
/// sole object-key table. Completed background tasks are joined before reuse.
struct FetchState {
    closed: bool,
    foreground_waiters: usize,
    active: usize,
    metadata: usize,
    metadata_peak: usize,
    background: Vec<tokio::task::JoinHandle<()>>,
}

pub(super) struct ObjectFetcher {
    runtime: tokio::runtime::Handle,
    budget: Arc<OutboardBudget>,
    limit: usize,
    state: Mutex<FetchState>,
    changed: Condvar,
    asynchronous_change: tokio::sync::Notify,
    cancellation: FetchCancellation,
    metrics: Option<ReadWorkMetrics>,
    fixed_control_bytes: usize,
    // State and all task handles precede their resident metadata lease.
    _fixed_metadata: OutboardLease,
}

pub(super) struct FetchTaskPermit {
    owner: Arc<ObjectFetcher>,
    charge: usize,
    metadata: Option<OutboardLease>,
}

impl ObjectFetcher {
    pub(super) fn new(
        runtime: tokio::runtime::Handle,
        budget: Arc<OutboardBudget>,
        limit: usize,
        metrics: Option<ReadWorkMetrics>,
    ) -> Result<Option<Arc<Self>>> {
        if limit == 0 || limit > MAX_FETCH_TASKS {
            return Err(invalid("DFS fetch task limit must be 1..=8"));
        }
        if runtime.runtime_flavor() != tokio::runtime::RuntimeFlavor::MultiThread {
            return Ok(None);
        }
        let background = Vec::with_capacity(limit);
        let fixed_control_bytes = std::mem::size_of::<Self>()
            + background.capacity() * std::mem::size_of::<tokio::task::JoinHandle<()>>()
            + 2048;
        let Some(fixed_metadata) = budget.try_reserve_resident(fixed_control_bytes) else {
            return Ok(None);
        };
        Ok(Some(Arc::new(Self {
            runtime,
            budget,
            limit,
            state: Mutex::new(FetchState {
                closed: false,
                foreground_waiters: 0,
                active: 0,
                metadata: 0,
                metadata_peak: 0,
                background,
            }),
            changed: Condvar::new(),
            asynchronous_change: tokio::sync::Notify::new(),
            cancellation: FetchCancellation::default(),
            metrics,
            fixed_control_bytes,
            _fixed_metadata: fixed_metadata,
        })))
    }

    pub(super) fn cancellation(&self) -> FetchCancellation {
        self.cancellation.clone()
    }

    pub(super) fn event(&self, event: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics.fetch_event(event);
        }
    }

    fn report(&self, state: &FetchState) {
        if let Some(metrics) = &self.metrics {
            metrics.fetch_resources(
                state.active,
                state.metadata + self.fixed_control_bytes,
                state.metadata_peak + self.fixed_control_bytes,
                self.limit,
            );
        }
    }

    /// Call before cloning the bounded job context. Foreground caller keeps
    /// its original deadline/authority check; no new deadline starts here.
    pub(super) fn reserve_foreground(
        self: &Arc<Self>,
        charge: usize,
        deadline: Option<Instant>,
        check: impl Fn() -> Result<()>,
    ) -> Result<Option<FetchTaskPermit>> {
        let _waiting = ForegroundWaiter::new(self.clone())?;
        loop {
            check()?;
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("DFS fetch lock poisoned"))?;
            if state.closed {
                return Err(stopped());
            }
            let wait = match deadline {
                Some(deadline) => deadline
                    .checked_duration_since(Instant::now())
                    .filter(|remaining| !remaining.is_zero())
                    .ok_or_else(|| {
                        Error::coded(
                            afs_error::CLIENT_DEADLINE_EXCEEDED,
                            "DFS fetch admission context expired",
                        )
                    })?
                    .min(Duration::from_millis(25)),
                None => Duration::from_millis(25),
            };
            if state.active < self.limit {
                let Some(metadata) = self.budget.try_reserve(charge) else {
                    return Ok(None);
                };
                state.active += 1;
                state.metadata += charge;
                state.metadata_peak = state.metadata_peak.max(state.metadata);
                self.report(&state);
                return Ok(Some(FetchTaskPermit {
                    owner: self.clone(),
                    charge,
                    metadata: Some(metadata),
                }));
            }
            drop(
                self.changed
                    .wait_timeout(state, wait)
                    .map_err(|_| invalid("DFS fetch wait lock poisoned"))?,
            );
        }
    }

    pub(super) fn try_reserve_background(
        self: &Arc<Self>,
        charge: usize,
    ) -> Result<Option<FetchTaskPermit>> {
        if !self.join_finished_background()? {
            return Ok(None);
        }
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                return Err(invalid("DFS fetch lock poisoned"));
            }
        };
        if state.closed
            || state.foreground_waiters != 0
            || state.active >= self.limit
            || state.background.len() >= self.limit
        {
            return Ok(None);
        }
        let Some(metadata) = self.budget.try_reserve(charge) else {
            return Ok(None);
        };
        state.active += 1;
        state.metadata += charge;
        state.metadata_peak = state.metadata_peak.max(state.metadata);
        self.report(&state);
        Ok(Some(FetchTaskPermit {
            owner: self.clone(),
            charge,
            metadata: Some(metadata),
        }))
    }

    /// The result is only existing per-request scratch, never captured source
    /// metadata. Callers must join every returned handle before returning.
    pub(super) fn spawn_foreground<T: Send + 'static>(
        &self,
        permit: FetchTaskPermit,
        work: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> tokio::task::JoinHandle<Result<T>> {
        self.runtime.spawn_blocking(move || {
            let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
                Ok(result) => result.map_err(|error| {
                    let mut end = error.message().len().min(512);
                    while !error.message().is_char_boundary(end) {
                        end -= 1;
                    }
                    Error::new(error.code(), error.kind(), &error.message()[..end])
                }),
                Err(payload) => {
                    drop(payload);
                    Err(stopped())
                }
            };
            drop(permit);
            result
        })
    }

    pub(super) fn spawn_background(
        &self,
        permit: FetchTaskPermit,
        work: impl FnOnce() -> Result<()> + Send + 'static,
    ) -> Result<()> {
        let mut state = self.state.try_lock().map_err(|_| stopped())?;
        if state.closed || state.background.len() >= self.limit {
            return Err(stopped());
        }
        state.background.push(self.runtime.spawn_blocking(move || {
            // A completed optional task retains no remote Error or panic payload
            // after its context lease is refunded. Join results are fixed unit.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(work)) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    permit.owner.event("failed");
                    afs_logging::warn!("dfs.prefetch_failed"; "error" => error.to_string());
                }
                Err(payload) => {
                    drop(payload);
                    permit.owner.event("failed");
                }
            }
            drop(permit);
        }));
        Ok(())
    }

    pub(super) fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| self.runtime.block_on(future))
        } else {
            self.runtime.block_on(future)
        }
    }

    fn join_finished_background(&self) -> Result<bool> {
        let finished = {
            let mut state = match self.state.try_lock() {
                Ok(state) => state,
                Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(invalid("DFS fetch lock poisoned"));
                }
            };
            let mut finished = Vec::with_capacity(self.limit);
            let mut index = 0;
            while index < state.background.len() {
                if state.background[index].is_finished() {
                    finished.push(state.background.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            finished
        };
        for task in finished {
            // Optional prefetch failure is observed, never a foreground error.
            if self.block_on(task).is_err() {
                self.event("failed");
            }
        }
        Ok(true)
    }

    pub(super) fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.closed = true;
        }
        self.cancellation.cancel();
        self.changed.notify_all();
        self.asynchronous_change.notify_waiters();
    }

    #[cfg(test)]
    pub(super) fn join_background_for_test(&self) -> Result<()> {
        let tasks = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("DFS fetch lock poisoned"))?;
            std::mem::take(&mut state.background)
        };
        for task in tasks {
            self.block_on(task).map_err(|_| stopped())?;
        }
        Ok(())
    }

    pub(super) async fn shutdown(&self) -> Result<()> {
        self.close();
        let background = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| invalid("DFS fetch lock poisoned"))?;
            std::mem::take(&mut state.background)
        };
        let mut failure = None;
        for task in background {
            if task.await.is_err() {
                failure = Some(stopped());
            }
        }
        // Foreground handles are joined by their synchronous batch owner.
        loop {
            let changed = self.asynchronous_change.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .lock()
                .map_err(|_| invalid("DFS fetch lock poisoned"))?
                .active
                == 0
            {
                break;
            }
            changed.await;
        }
        failure.map_or(Ok(()), Err)
    }
}

struct ForegroundWaiter(Arc<ObjectFetcher>);
impl ForegroundWaiter {
    fn new(owner: Arc<ObjectFetcher>) -> Result<Self> {
        owner
            .state
            .lock()
            .map_err(|_| invalid("DFS fetch lock poisoned"))?
            .foreground_waiters += 1;
        Ok(Self(owner))
    }
}
impl Drop for ForegroundWaiter {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.state.lock() {
            state.foreground_waiters = state.foreground_waiters.saturating_sub(1);
        }
        self.0.changed.notify_all();
    }
}

impl Drop for FetchTaskPermit {
    fn drop(&mut self) {
        // Job context is already destroyed by work(). Retire its actual lease
        // before announcing the available slot to another task.
        drop(self.metadata.take());
        if let Ok(mut state) = self.owner.state.lock() {
            state.active = state.active.saturating_sub(1);
            state.metadata = state.metadata.saturating_sub(self.charge);
            self.owner.report(&state);
        }
        self.owner.changed.notify_all();
        self.owner.asynchronous_change.notify_waiters();
    }
}

fn invalid(message: &str) -> Error {
    Error::coded(afs_error::IO_INVALID, message)
}
fn stopped() -> Error {
    Error::coded(afs_error::IO_UNAVAILABLE, "DFS fetcher stopped")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_never_queues_behind_foreground_metadata_or_proof_budget() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let budget = OutboardBudget::new(1024 * 1024);
        let fetcher = ObjectFetcher::new(runtime.handle().clone(), budget.clone(), 2, None)
            .unwrap()
            .unwrap();
        let waiting = ForegroundWaiter::new(fetcher.clone()).unwrap();
        assert!(fetcher.try_reserve_background(1024).unwrap().is_none());
        drop(waiting);
        let state = fetcher.state.lock().unwrap();
        assert!(fetcher.try_reserve_background(1024).unwrap().is_none());
        drop(state);
        let occupied = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(fetcher.try_reserve_background(1024).unwrap().is_none());
        drop(occupied);
        let permit = fetcher.try_reserve_background(1024).unwrap().unwrap();
        assert_eq!(fetcher.state.lock().unwrap().active, 1);
        drop(permit);
        runtime.block_on(fetcher.shutdown()).unwrap();
        assert_eq!(fetcher.state.lock().unwrap().active, 0);
    }

    #[test]
    fn sequence_grows_resets_and_never_retains_version_strings() {
        let mut sequence = ReadSequence::default();
        let scope = [1; 32];
        for (index, expected) in [1, 2, 4, 8, 8].into_iter().enumerate() {
            assert_eq!(
                sequence.record(scope, index as u64 * 128 * 1024, 128 * 1024, 8),
                expected
            );
        }
        let generation = sequence.generation();
        assert_eq!(sequence.record(scope, 16 * 1024 * 1024, 128 * 1024, 8), 0);
        assert!(sequence.generation() > generation);
        assert_eq!(
            sequence.record(scope, 16 * 1024 * 1024 + 128 * 1024, 128 * 1024, 8),
            1
        );
        assert_eq!(sequence.record([2; 32], 0, 128 * 1024, 8), 1);
        assert_eq!(sequence.record([2; 32], 128 * 1024, 128 * 1024, 0), 0);
    }
}
