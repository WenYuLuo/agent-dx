use std::time::Duration;
use tokio::time::Instant;

/// Failure to establish or use an operation deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeadlineError {
    Elapsed,
    Overflow,
    ClockBeforeEpoch,
}
impl From<DeadlineError> for tonic::Status {
    fn from(error: DeadlineError) -> Self {
        match error {
            DeadlineError::Elapsed => Self::deadline_exceeded("operation deadline exceeded"),
            DeadlineError::Overflow => Self::invalid_argument("operation deadline overflow"),
            DeadlineError::ClockBeforeEpoch => Self::internal("system clock before epoch"),
        }
    }
}

/// One wire deadline with a local monotonic clock for operation waits.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    instant: Instant,
    unix_millis: u64,
}

impl Deadline {
    pub fn after(budget: Duration) -> Result<Self, DeadlineError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| DeadlineError::ClockBeforeEpoch)?;
        let end = now.checked_add(budget).ok_or(DeadlineError::Overflow)?;
        let unix_millis = u64::try_from(end.as_millis()).map_err(|_| DeadlineError::Overflow)?;
        Self::from_unix_millis(unix_millis)
    }

    pub fn from_unix_millis(unix_millis: u64) -> Result<Self, DeadlineError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| DeadlineError::ClockBeforeEpoch)?;
        let budget = Duration::from_millis(unix_millis)
            .checked_sub(now)
            .filter(|budget| !budget.is_zero())
            .ok_or(DeadlineError::Elapsed)?;
        let instant = Instant::now()
            .checked_add(budget)
            .ok_or(DeadlineError::Overflow)?;
        Ok(Self {
            instant,
            unix_millis,
        })
    }

    pub fn remaining(self) -> Result<Duration, DeadlineError> {
        remaining(self.instant, Duration::MAX).ok_or(DeadlineError::Elapsed)
    }

    pub fn instant(self) -> Instant {
        self.instant
    }
    pub fn unix_millis(self) -> u64 {
        self.unix_millis
    }
}

/// Return the smaller of the upstream remaining deadline and a local cap.
/// An elapsed upstream deadline is terminal and must not be replaced by a new
/// local timeout budget.
pub fn remaining(deadline: Instant, cap: Duration) -> Option<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .map(|remaining| remaining.min(cap))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn wire_deadline_retains_a_local_budget_without_renewal() {
        let deadline = Deadline::after(Duration::from_secs(5)).unwrap();
        let wire = Deadline::from_unix_millis(deadline.unix_millis()).unwrap();
        assert_eq!(wire.unix_millis(), deadline.unix_millis());
        assert!(wire.remaining().unwrap() <= Duration::from_secs(5));
        tokio::time::advance(Duration::from_secs(5)).await;
        assert_eq!(deadline.remaining().unwrap_err(), DeadlineError::Elapsed);
        assert_eq!(wire.remaining().unwrap_err(), DeadlineError::Elapsed);
        assert_eq!(
            Deadline::from_unix_millis(1).unwrap_err(),
            DeadlineError::Elapsed
        );
    }

    #[tokio::test(start_paused = true)]
    async fn preserves_the_upstream_budget() {
        let deadline = Instant::now() + Duration::from_secs(2);
        assert_eq!(
            remaining(deadline, Duration::from_secs(5)),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            remaining(deadline, Duration::from_secs(1)),
            Some(Duration::from_secs(1))
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(remaining(deadline, Duration::from_secs(1)), None);
    }
}
