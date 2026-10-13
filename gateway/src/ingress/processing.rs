//! A request deadline shared by admission, demand wake, route delivery and forwarding.
use http::HeaderMap;
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::time::Instant;

pub const TIMEOUT_HEADER: &str = "x-adx-processing-timeout-ms";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_TIMEOUT_MS: u64 = 86_400_000;

#[derive(Debug, Clone)]
pub struct ProcessingDeadline {
    pub at: Instant,
    phase: Arc<AtomicU8>,
}

impl Default for ProcessingDeadline {
    fn default() -> Self {
        Self::new(DEFAULT_TIMEOUT)
    }
}

impl ProcessingDeadline {
    pub fn new(timeout: Duration) -> Self {
        Self {
            at: Instant::now() + timeout,
            phase: Arc::new(AtomicU8::new(0)),
        }
    }

    pub fn from_headers(headers: &HeaderMap) -> Result<Self, &'static str> {
        let mut values = headers.get_all(TIMEOUT_HEADER).iter();
        let Some(value) = values.next() else {
            return Ok(Self::default());
        };
        if values.next().is_some() {
            return Err("processing timeout must occur once");
        }
        let milliseconds = value
            .to_str()
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|n| *n > 0 && *n <= MAX_TIMEOUT_MS)
            .ok_or("processing timeout must be 1 through 86400000 milliseconds")?;
        Ok(Self::new(Duration::from_millis(milliseconds)))
    }

    pub fn remaining(&self) -> Duration {
        self.at.saturating_duration_since(Instant::now())
    }
    pub fn waiting_resume(&self) {
        self.phase.store(1, Ordering::Release);
    }
    pub fn prepared(&self) {
        self.phase.store(0, Ordering::Release);
    }
    pub fn forwarded(&self) {
        self.phase.store(2, Ordering::Release);
    }
    pub fn is_waiting_resume(&self) -> bool {
        self.phase.load(Ordering::Acquire) == 1
    }
    pub fn is_forwarded(&self) -> bool {
        self.phase.load(Ordering::Acquire) == 2
    }
}
