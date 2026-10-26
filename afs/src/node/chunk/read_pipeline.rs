//! Async admission uses the existing shared body/control ledger. No memory
//! wait occupies a disk permit or a blocking-pool worker.
use super::*;

pub(crate) struct FixedReadReservation {
    pub(super) payload: PeerReadReservation,
    pub(super) control: PeerControlLease,
}

impl LocalChunkStore {
    pub(crate) fn configure_native_read_buffers(&self) -> Result<()> {
        self.peer_buffer_pool.configure_byte_limit(
            usize::try_from(self.peer_read_budget.limit).map_err(|_| peer_payload_capacity())?,
        )
    }
    pub(crate) fn try_fixed_read_reservation(
        &self,
        length: usize,
    ) -> Result<Option<FixedReadReservation>> {
        let body = length
            .max(1024 * 1024)
            .checked_add(crate::node::chunk_crc32c::SEGMENT)
            .ok_or_else(peer_payload_capacity)? as u64;
        let control = (std::mem::size_of::<PeerVerifiedObject>() + 128) as u64;
        let budget = &self.peer_read_budget;
        let _guard = budget
            .admission
            .lock()
            .map_err(|_| peer_serve_unavailable("Peer budget admission poisoned"))?;
        if budget.stopped.load(Ordering::Acquire) {
            return Err(peer_serve_unavailable("Peer serve is stopped"));
        }
        if body > budget.limit {
            return Err(peer_payload_capacity());
        }
        let control_limit =
            PEER_SERVE_CONTROL_LIMIT.saturating_sub(peer_serve_fixed_control_bytes()) as u64;
        let next_control = budget
            .control_used
            .load(Ordering::Acquire)
            .checked_add(control);
        let enough_body = budget
            .used
            .load(Ordering::Acquire)
            .checked_add(body)
            .is_some_and(|n| n <= budget.limit);
        if !enough_body || next_control.is_none_or(|n| n > control_limit) {
            return Ok(None);
        }
        reserve_peer_bytes_locked(budget, body, self.read_work.as_ref())?;
        let next = budget.control_used.fetch_add(control, Ordering::AcqRel) + control;
        let previous = budget.control_peak.fetch_max(next, Ordering::AcqRel);
        if let Some(metrics) = &self.read_work {
            metrics.peer_serve_control(control as i64, next.saturating_sub(previous));
        }
        Ok(Some(FixedReadReservation {
            payload: PeerReadReservation {
                budget: budget.clone(),
                bytes: body,
                metrics: self.read_work.clone(),
            },
            control: PeerControlLease {
                budget: budget.clone(),
                bytes: control,
                metrics: self.read_work.clone(),
            },
        }))
    }

    pub(crate) async fn reserve_fixed_read_until(
        &self,
        length: usize,
        deadline: Instant,
    ) -> Result<FixedReadReservation> {
        let _wait = self.read_work.as_ref().map(|m| {
            m.activities
                .enter(crate::node::read_activity::Activity::BodyWait, 0)
        });
        let admission_deadline = deadline.min(self.peer_serve_deadline());
        loop {
            let changed = self.peer_read_budget.async_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if Instant::now() >= deadline {
                return Err(peer_payload_capacity());
            }
            if let Some(reservation) = self.try_fixed_read_reservation(length)? {
                return Ok(reservation);
            }
            // Same eviction rule as the existing synchronous admission. Drop
            // owners outside the table lock so lease destruction cannot reenter.
            let victim = {
                let mut cache = self
                    .peer_read_cache
                    .lock()
                    .map_err(|_| peer_serve_unavailable("Peer serve state poisoned"))?;
                let index = cache
                    .residents
                    .iter()
                    .rposition(|object| object.as_ref().is_some_and(|o| Arc::strong_count(o) == 1));
                index.and_then(|i| cache.take_resident(i))
            };
            if let Some(victim) = victim {
                drop(victim);
                continue;
            }
            if Instant::now() >= admission_deadline {
                return Err(peer_payload_capacity());
            }
            tokio::time::timeout_at(tokio::time::Instant::from_std(admission_deadline), changed)
                .await
                .map_err(|_| peer_payload_capacity())?;
        }
    }

    pub(crate) fn read_fixed_peer_range_reserved(
        &self,
        reader: &PinnedChunkReader,
        offset: u64,
        length: usize,
        reservation: FixedReadReservation,
    ) -> Result<PeerReadPayload> {
        if self.peer_read_crc_mode != crate::config::DfsReadCrcMode::Off
            || offset
                .checked_add(length as u64)
                .is_none_or(|end| end > reader.chunk.length)
        {
            return Err(invalid("fixed open range or CRC policy differs"));
        }
        let FixedReadReservation {
            mut payload,
            control,
        } = reservation;
        let mut bytes = self.peer_buffer_pool.take(length, self.read_work.clone())?;
        payload.retain_capacity(
            bytes
                .capacity()
                .saturating_add(crate::node::chunk_crc32c::SEGMENT),
        )?;
        let mut done = 0;
        while done < length {
            let activity = self.read_work.as_ref().map(|m| {
                m.activities
                    .enter(crate::node::read_activity::Activity::SourceIo, 0)
            });
            let result = read_positioned(&reader.file, offset + done as u64, &mut bytes[done..]);
            if let (Some(activity), Ok(got)) = (&activity, &result) {
                activity.completed_bytes(*got as u64);
            }
            drop(activity);
            let got = result?;
            if got == 0 {
                return Err(corrupt(&reader.chunk.id, "fixed open object ended early"));
            }
            done += got;
        }
        Ok(PeerReadPayload {
            object: Arc::new(PeerVerifiedObject {
                bytes,
                outboard: None,
                _reservation: payload,
                identity: None,
                _control: control,
            }),
            start: 0,
            length,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_concurrency_async_reservation_records_control_high_water() {
        let temp = tempfile::tempdir().unwrap();
        let local = LocalChunkStore::open(temp.path(), "node-a").unwrap();
        let reservation = local.try_fixed_read_reservation(1024).unwrap().unwrap();
        let current = local.peer_read_budget.control_used.load(Ordering::Acquire);
        assert!(current > 0);
        assert_eq!(
            local.peer_read_budget.control_peak.load(Ordering::Acquire),
            current,
            "async control ledger must record high water"
        );
        drop(reservation);
        assert_eq!(
            local.peer_read_budget.control_used.load(Ordering::Acquire),
            0
        );
    }
}
