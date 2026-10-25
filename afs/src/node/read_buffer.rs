//! Private initialized buffers, recycled only after the last transport/reply owner.
use super::read_work::ReadWorkMetrics;
use afs_error::{Error, Result};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

const MAX_BUFFER: usize = 1024 * 1024;
const SLOTS: usize = 2;

#[derive(Debug)]
pub(crate) struct ReadBufferPool {
    free: Mutex<[Option<Vec<u8>>; SLOTS]>,
    role: &'static str,
}

impl ReadBufferPool {
    pub(crate) fn new(role: &'static str) -> Arc<Self> {
        Arc::new(Self {
            free: Mutex::new(std::array::from_fn(|_| None)),
            role,
        })
    }
    pub(crate) fn take(
        self: &Arc<Self>,
        size: usize,
        metrics: Option<ReadWorkMetrics>,
    ) -> Result<ReadBuffer> {
        let mut bytes = Vec::new();
        if size != 0 && size <= MAX_BUFFER {
            let mut free = self
                .free
                .lock()
                .map_err(|_| Error::coded(afs_error::IO_CAPACITY, "read buffer pool poisoned"))?;
            if let Some(slot) = free
                .iter_mut()
                .find(|v| v.as_ref().is_some_and(|b| b.len() >= size))
            {
                bytes = slot.take().unwrap_or_default();
                if let Some(m) = &metrics {
                    m.read_buffer(self.role, "reuse", bytes.capacity() as u64);
                }
            }
            self.observe_idle(&free, metrics.as_ref());
        }
        if bytes.len() < size {
            bytes.try_reserve_exact(size - bytes.len()).map_err(|_| {
                Error::coded(afs_error::IO_CAPACITY, "read buffer allocation failed")
            })?;
            bytes.resize(size, 0);
            if let Some(m) = &metrics {
                m.read_buffer(self.role, "allocate", bytes.capacity() as u64);
                m.read_buffer(self.role, "zero", size as u64);
            }
        }
        Ok(ReadBuffer {
            bytes,
            length: size,
            pool: Some(self.clone()),
            metrics,
        })
    }
    fn observe_idle(&self, free: &[Option<Vec<u8>>; SLOTS], metrics: Option<&ReadWorkMetrics>) {
        if let Some(m) = metrics {
            m.read_buffer_idle(
                self.role,
                free.iter().flatten().map(Vec::capacity).sum::<usize>() as u64,
            );
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReadBuffer {
    bytes: Vec<u8>,
    length: usize,
    pool: Option<Arc<ReadBufferPool>>,
    metrics: Option<ReadWorkMetrics>,
}
impl ReadBuffer {
    pub(crate) fn capacity(&self) -> usize {
        self.bytes.capacity()
    }
    pub(crate) fn into_vec(mut self) -> Vec<u8> {
        self.bytes.truncate(self.length);
        std::mem::take(&mut self.bytes)
    }
}
impl From<Vec<u8>> for ReadBuffer {
    fn from(bytes: Vec<u8>) -> Self {
        Self {
            length: bytes.len(),
            bytes,
            pool: None,
            metrics: None,
        }
    }
}
impl Deref for ReadBuffer {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[..self.length]
    }
}
impl DerefMut for ReadBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.bytes[..self.length]
    }
}
impl AsRef<[u8]> for ReadBuffer {
    fn as_ref(&self) -> &[u8] {
        self
    }
}
impl Drop for ReadBuffer {
    fn drop(&mut self) {
        let Some(pool) = &self.pool else {
            return;
        };
        if self.bytes.capacity() == 0 || self.bytes.capacity() > MAX_BUFFER {
            return;
        }
        if let Ok(mut free) = pool.free.lock() {
            if let Some(slot) = free.iter_mut().find(|v| v.is_none()) {
                *slot = Some(std::mem::take(&mut self.bytes));
                if let Some(m) = &self.metrics {
                    m.read_buffer(
                        pool.role,
                        "return",
                        slot.as_ref().map_or(0, Vec::capacity) as u64,
                    );
                }
            }
            pool.observe_idle(&free, self.metrics.as_ref());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn buffer_reuse_waits_for_last_shared_frame_and_preserves_initialized_storage() {
        let pool = ReadBufferPool::new("serve");
        let mut b = pool.take(1024, None).unwrap();
        b.fill(17);
        let ptr = b.as_ptr();
        let bytes = prost::bytes::Bytes::from_owner(b);
        let frame = bytes.slice(16..32);
        drop(bytes);
        assert!(pool.free.lock().unwrap().iter().all(Option::is_none));
        let other = pool.take(1024, None).unwrap();
        assert_ne!(other.as_ptr(), ptr);
        assert_eq!(&frame[..], &[17; 16]);
        drop(frame);
        let reused = pool.take(512, None).unwrap();
        assert_eq!(reused.as_ptr(), ptr);
        assert_eq!(reused.len(), 512);
        assert_eq!(&reused[..], &[17; 512]);
    }
    #[test]
    fn buffer_reuse_idle_capacity_is_bounded_and_error_drop_returns_owner() {
        let pool = ReadBufferPool::new("reply");
        let held: Vec<_> = (0..5)
            .map(|_| pool.take(MAX_BUFFER, None).unwrap())
            .collect();
        drop(held);
        assert_eq!(
            pool.free
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .map(Vec::capacity)
                .sum::<usize>(),
            2 * MAX_BUFFER
        );
        drop(pool.take(MAX_BUFFER + 1, None).unwrap());
        assert_eq!(
            pool.free
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .map(Vec::capacity)
                .sum::<usize>(),
            2 * MAX_BUFFER
        );
        let ptr = {
            let b = pool.take(MAX_BUFFER, None).unwrap();
            let ptr = b.as_ptr();
            drop(b);
            ptr
        };
        assert_eq!(pool.take(MAX_BUFFER, None).unwrap().as_ptr(), ptr);
    }
    #[test]
    fn buffer_reuse_vec_compatibility_transfers_without_copy_or_pool_alias() {
        let pool = ReadBufferPool::new("reply");
        let b = pool.take(16, None).unwrap();
        let ptr = b.as_ptr();
        let v = b.into_vec();
        assert_eq!(v.as_ptr(), ptr);
        assert!(pool.free.lock().unwrap().iter().all(Option::is_none));
    }
}
