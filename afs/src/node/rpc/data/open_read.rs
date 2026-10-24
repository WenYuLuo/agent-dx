use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
type Key = (String, String);
#[derive(Default)]
pub(super) struct OpenAuthorityState {
    entries: HashMap<Key, OpenEntry>,
    pending: HashMap<Key, (Arc<AtomicBool>, usize)>,
}
struct OpenEntry {
    grant: crate::dfs::DfsReadGrant,
    sources: HashMap<String, crate::dfs::OpenReadSource>,
    deadline: std::time::Instant,
    expires: u64,
    active: Arc<AtomicBool>,
    bytes: usize,
}
struct Pending<'a> {
    state: &'a std::sync::Mutex<OpenAuthorityState>,
    key: Key,
    active: Arc<AtomicBool>,
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if let Ok(mut s) = self.state.lock()
            && let Some((active, n)) = s.pending.get_mut(&self.key)
            && Arc::ptr_eq(active, &self.active)
        {
            *n -= 1;
            if *n == 0 {
                s.pending.remove(&self.key);
            }
        }
    }
}
fn same_open(a: &crate::dfs::DfsReadGrant, b: &afs_protocol::node_data::DfsReadGrant) -> bool {
    a.namespace_id.0 == b.namespace_id
        && a.file_version_id.0 == b.file_version_id
        && a.layout_root_id.0 == b.layout_root_id
        && a.caller_node_id == b.caller_node_id
        && a.caller_node_epoch == b.caller_node_epoch
        && a.token == b.token
}
fn covers(
    source: &crate::dfs::OpenReadSource,
    op: &afs_protocol::node_data::DfsChunkReadOp,
) -> bool {
    source.copy_ids.iter().any(|v| v.0 == op.source_copy_id)
        && op.length != 0
        && op.chunk_offset.checked_add(op.length).is_some_and(|end| {
            source.ranges.iter().any(|(start, n)| {
                op.chunk_offset >= *start && start.checked_add(*n).is_some_and(|limit| end <= limit)
            })
        })
}
impl CachedDfsReadAuthorizer {
    pub(super) fn close_open_context(&self, peer: &str, context: &str) -> afs_error::Result<()> {
        if !context.starts_with("dfs-open-v1:read-open:") || context.len() > 524 {
            return Err(replica_denied("invalid closed read context id"));
        }
        let key = (peer.into(), context.into());
        let mut state = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?;
        if let Some(entry) = state.entries.remove(&key) {
            entry.active.store(false, Ordering::Release);
        }
        if let Some((active, _)) = state.pending.get(&key) {
            active.store(false, Ordering::Release);
        }
        Ok(())
    }
    pub(super) fn authorize_open_context(
        &self,
        peer: &str,
        request: &DfsReadRangesRequest,
        metrics: Option<&super::super::super::read_work::ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        for op in &request.operations {
            self.authorize_open_range(peer, op, metrics)?;
        }
        Ok(())
    }
    fn authorize_open_range(
        &self,
        peer: &str,
        op: &afs_protocol::node_data::DfsChunkReadOp,
        metrics: Option<&super::super::super::read_work::ReadWorkMetrics>,
    ) -> afs_error::Result<()> {
        let grant = op
            .grant
            .as_ref()
            .ok_or_else(|| replica_denied("open read grant missing"))?;
        let now = crate::meta::store::now_unix_ms();
        if grant.caller_node_id != peer
            || !grant.token.starts_with("dfs-open-v1:read-open:")
            || grant.token.len() > 524
            || grant.expires_at_unix_ms <= now
        {
            return Err(replica_denied("open read peer, token or expiry differs"));
        }
        let key = (peer.to_owned(), grant.token.clone());
        let pending = {
            let mut state = self
                .open_cache
                .lock()
                .map_err(|_| replica_denied("open read cache poisoned"))?;
            if let Some(entry) = state.entries.get(&key) {
                if !same_open(&entry.grant, grant) {
                    return Err(replica_denied("open read immutable identity changed"));
                }
                if !entry.active.load(Ordering::Acquire) {
                    return Err(replica_denied("open read context is closed"));
                }
                if entry.deadline > std::time::Instant::now()
                    && entry.expires > now
                    && entry
                        .sources
                        .get(&op.chunk_id)
                        .is_some_and(|s| covers(s, op))
                {
                    return Ok(());
                }
            }
            if !state.pending.contains_key(&key) && state.pending.len() >= 128 {
                return Err(replica_denied("open read pending context budget exhausted"));
            }
            let (active, n) = state
                .pending
                .entry(key.clone())
                .or_insert_with(|| (Arc::new(AtomicBool::new(true)), 0));
            if !active.load(Ordering::Acquire) {
                return Err(replica_denied("open read context closed during admission"));
            }
            *n += 1;
            Pending {
                state: &self.open_cache,
                key: key.clone(),
                active: active.clone(),
            }
        };
        let check = crate::dfs::DfsReadValidation {
            grant: crate::dfs::DfsReadGrant {
                namespace_id: crate::dfs::NamespaceId::new(grant.namespace_id.clone()),
                file_version_id: crate::dfs::FileVersionId::new(grant.file_version_id.clone()),
                layout_root_id: crate::dfs::LayoutRootId::new(grant.layout_root_id.clone()),
                caller_node_id: grant.caller_node_id.clone(),
                caller_node_epoch: grant.caller_node_epoch,
                expires_at_unix_ms: grant.expires_at_unix_ms,
                fence: grant.fence,
                token: grant.token.clone(),
            },
            chunk_id: crate::dfs::ChunkId::new(op.chunk_id.clone()),
            copy_id: crate::dfs::CopyId::new(op.source_copy_id.clone()),
            chunk_offset: op.chunk_offset,
            length: op.length,
        };
        let timer = metrics.map(|m| m.authorization_scope("meta_rpc"));
        let mut reply = self.validator.validate(crate::dfs::ValidateDfsReadGrants {
            receiver_node_id: self.node_id.clone(),
            receiver_node_epoch: self.node_epoch,
            peer_node_id: peer.into(),
            validations: vec![check.clone()],
        })?;
        drop(timer);
        if reply.len() != 1 {
            return Err(replica_denied("open read authority reply count differs"));
        }
        let reply = reply.remove(0);
        let completed = crate::meta::store::now_unix_ms();
        if reply.validation != check
            || reply.expires_at_unix_ms <= completed
            || reply.expires_at_unix_ms > grant.expires_at_unix_ms
            || reply.open_sources.len() > 4 * 1024 * 1024
        {
            return Err(replica_denied(
                "open read reply identity, lease or budget differs",
            ));
        }
        let page: Vec<crate::dfs::OpenReadSource> = serde_json::from_slice(&reply.open_sources)
            .map_err(|_| replica_denied("open read source page invalid"))?;
        if page.is_empty() || page.len() > 512 {
            return Err(replica_denied("open read source page count exceeds budget"));
        }
        let mut sources = HashMap::new();
        for s in page {
            if s.chunk_id.0.is_empty()
                || s.chunk_id.0.len() > 512
                || s.copy_ids.is_empty()
                || s.copy_ids.len() > 64
                || s.copy_ids.iter().any(|v| v.0.is_empty() || v.0.len() > 512)
                || s.ranges.is_empty()
                || s.ranges.len() > 128
                || s.ranges
                    .iter()
                    .any(|(a, n)| *n == 0 || a.checked_add(*n).is_none())
            {
                return Err(replica_denied("open read source page shape differs"));
            }
            if sources.insert(s.chunk_id.0.clone(), s).is_some() {
                return Err(replica_denied("duplicate open read source entry"));
            }
        }
        if !sources.get(&op.chunk_id).is_some_and(|s| covers(s, op)) {
            return Err(replica_denied(
                "open read page does not cover requested source",
            ));
        }
        let bytes = reply
            .open_sources
            .len()
            .saturating_mul(3)
            .saturating_add(4096);
        let mut state = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?;
        if !pending.active.load(Ordering::Acquire) {
            return Err(replica_denied(
                "open read closed before authorization publication",
            ));
        }
        // Ready pages may be evicted without changing the pinned open identity.
        state.entries.remove(&key);
        while state.entries.len() >= 8
            || state
                .entries
                .values()
                .map(|v| v.bytes)
                .sum::<usize>()
                .saturating_add(bytes)
                > 32 * 1024 * 1024
        {
            let old = state
                .entries
                .keys()
                .next()
                .cloned()
                .ok_or_else(|| replica_denied("open read cache byte budget exceeded"))?;
            state.entries.remove(&old);
        }
        let remaining = reply
            .expires_at_unix_ms
            .saturating_sub(completed)
            .min(5_000);
        state.entries.insert(
            key,
            OpenEntry {
                grant: check.grant,
                sources,
                deadline: std::time::Instant::now() + std::time::Duration::from_millis(remaining),
                expires: reply.expires_at_unix_ms,
                active: pending.active.clone(),
                bytes,
            },
        );
        drop(state);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, atomic::AtomicUsize};
    struct PageValidator {
        calls: AtomicUsize,
        gate: Option<Arc<Barrier>>,
    }
    impl DfsReadGrantValidator for PageValidator {
        fn validate(
            &self,
            request: crate::dfs::ValidateDfsReadGrants,
        ) -> afs_error::Result<Vec<crate::dfs::DfsAuthorizedRead>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if let Some(gate) = &self.gate {
                gate.wait();
                gate.wait();
            }
            let validation = request.validations[0].clone();
            let sources = ["object-1", "object-2"].map(|id| crate::dfs::OpenReadSource {
                chunk_id: crate::dfs::ChunkId::new(id),
                copy_ids: vec![crate::dfs::CopyId::new("copy")],
                ranges: vec![(0, 4096)],
            });
            Ok(vec![crate::dfs::DfsAuthorizedRead {
                expires_at_unix_ms: crate::meta::store::now_unix_ms() + 4_000,
                validation,
                allowed_ranges: vec![(0, 4096)],
                open_sources: serde_json::to_vec(&sources).unwrap(),
            }])
        }
    }
    fn request() -> DfsReadRangesRequest {
        DfsReadRangesRequest {
            include_blake3_outboard: false,
            read_id: "read".into(),
            attempt_id: "attempt".into(),
            file_version_id: "v".into(),
            layout_root_id: "l".into(),
            operations: vec![afs_protocol::node_data::DfsChunkReadOp {
                operation_index: 0,
                chunk_id: "object-1".into(),
                source_copy_id: "copy".into(),
                chunk_offset: 0,
                length: 1024,
                destination_offset: 0,
                grant: Some(afs_protocol::node_data::DfsReadGrant {
                    namespace_id: "n".into(),
                    file_version_id: "v".into(),
                    layout_root_id: "l".into(),
                    caller_node_id: "reader".into(),
                    caller_node_epoch: 1,
                    expires_at_unix_ms: crate::meta::store::now_unix_ms() + 30_000,
                    fence: 1,
                    token: "dfs-open-v1:read-open:test".into(),
                }),
            }],
        }
    }
    #[test]
    fn open_authority_one_file_page_reuses_across_objects() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let mut r = request();
        authorizer.authorize("reader", &r).unwrap();
        r.operations[0].chunk_offset = 1024;
        authorizer.authorize("reader", &r).unwrap();
        r.operations[0].chunk_id = "object-2".into();
        authorizer.authorize("reader", &r).unwrap();
        let mut second = r.operations[0].clone();
        second.operation_index = 1;
        second.destination_offset = 1024;
        second.chunk_offset = 2048;
        r.operations.push(second);
        authorizer.authorize("reader", &r).unwrap();
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn open_authority_close_cancels_pending_meta_reply() {
        let gate = Arc::new(Barrier::new(2));
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: Some(gate.clone()),
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator,
            "receiver".into(),
            1,
        ));
        let a = authorizer.clone();
        let pending = std::thread::spawn(move || a.authorize("reader", &request()));
        gate.wait();
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        gate.wait();
        assert!(pending.join().unwrap().is_err());
        assert!(authorizer.open_cache.lock().unwrap().entries.is_empty());
    }
}
