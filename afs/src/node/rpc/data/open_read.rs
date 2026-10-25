use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
type Key = (String, String);
/// The durable B-side open pin, rather than the source-page TTL, owns this view.
/// Close prevents new admission; requests already holding an Arc may drain.
pub struct DfsOpenReadContext {
    version: String,
    layout: String,
    sources: HashMap<String, crate::dfs::OpenReadSource>,
    active: Arc<AtomicBool>,
    connection: Option<SocketAddr>,
    readers: crate::node::chunk::FixedPeerReaders,
    readers_registered: AtomicBool,
}
impl DfsOpenReadContext {
    fn admits(&self, request: &DfsReadRangesRequest) -> afs_error::Result<bool> {
        if !self.active.load(Ordering::Acquire)
            || request.file_version_id != self.version
            || request.layout_root_id != self.layout
            || request.operations.len() != 1
            || request.read_id.is_empty()
            || request.attempt_id.is_empty()
        {
            return Err(replica_denied(
                "fixed open context identity differs or is closed",
            ));
        }
        let op = &request.operations[0];
        if op.operation_index != 0
            || op.length == 0
            || op.length > super::MAX_DFS_READ_BYTES
            || op.destination_offset != 0
        {
            return Err(replica_denied("fixed open range shape differs"));
        }
        match self.sources.get(&op.chunk_id) {
            Some(source) if covers(source, op) => Ok(true),
            Some(_) => Err(replica_denied(
                "fixed open range exceeds its admitted object",
            )),
            None => Ok(false), // A missing source page is a cold-context path.
        }
    }
    pub(super) fn payload(
        &self,
        local: &LocalChunkStore,
        op: &afs_protocol::node_data::DfsChunkReadOp,
        deadline: std::time::Instant,
    ) -> afs_error::Result<crate::node::chunk::PeerReadPayload> {
        if !self.readers_registered.swap(true, Ordering::AcqRel)
            && let Err(error) = local.register_fixed_peer_readers(&self.readers)
        {
            self.readers_registered.store(false, Ordering::Release);
            return Err(error);
        }
        let id = crate::dfs::ChunkId::new(op.chunk_id.clone());
        let reader = {
            let mut readers = self
                .readers
                .lock()
                .map_err(|_| replica_denied("fixed open reader cache poisoned"))?;
            if let Some(reader) = readers.get(&id) {
                reader.clone()
            } else {
                let reader = Arc::new(local.open_fixed_peer_reader(&id)?);
                // 64 bounded descriptors per context, independently of payload budgets.
                if readers.len() >= 64
                    && let Some(old) = readers.keys().next().cloned()
                {
                    readers.remove(&old);
                }
                readers.insert(id.clone(), reader.clone());
                reader
            }
        };
        local
            .read_fixed_peer_range(
                &reader,
                op.chunk_offset,
                usize::try_from(op.length)
                    .map_err(|_| replica_denied("fixed open length exceeds address space"))?,
                deadline,
            )
            .inspect_err(|_| {
                if let Ok(mut readers) = self.readers.lock() {
                    readers.remove(&id);
                }
            })
    }
}

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
    fast: Option<Arc<DfsOpenReadContext>>,
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
    pub(super) fn fast_open_context(
        &self,
        peer: &str,
        request: &DfsReadRangesRequest,
        connection: Option<SocketAddr>,
    ) -> afs_error::Result<Option<Arc<DfsOpenReadContext>>> {
        let Some(grant) = request.operations.first().and_then(|op| op.grant.as_ref()) else {
            return Err(replica_denied("fixed open context is absent"));
        };
        if !grant.token.starts_with("dfs-open-v1:read-open:") || grant.token.len() > 524 {
            return Ok(None);
        }
        let key = (peer.to_owned(), grant.token.clone());
        let existing = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?
            .entries
            .get(&key)
            .and_then(|entry| entry.fast.clone());
        if let Some(context) = existing {
            if context.connection == connection && context.admits(request)? {
                if let Some(metrics) = self.validator.authorization_metrics() {
                    metrics.check(crate::node::read_work::ReadCheck::ContextReuse);
                }
                return Ok(Some(context));
            }
            if context.connection != connection && !grant.caller_node_id.is_empty() {
                // Reconnect is recovery: re-establish Meta authority instead of
                // letting a new connection inherit a dead caller's context.
                self.open_cache
                    .lock()
                    .map_err(|_| replica_denied("open read cache poisoned"))?
                    .entries
                    .remove(&key);
            }
        }
        if grant.caller_node_id.is_empty() {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNAVAILABLE,
                "fixed open setup required",
            ));
        }
        // First access (or an evicted/missing source page) uses the full Meta-
        // anchored validator. Its TTL never becomes a per-request refresh timer.
        self.authorize(peer, request)?;
        let mut state = self
            .open_cache
            .lock()
            .map_err(|_| replica_denied("open read cache poisoned"))?;
        let retained: usize = state.entries.values().map(|entry| entry.bytes).sum();
        let needs_context = state
            .entries
            .get(&key)
            .is_some_and(|entry| entry.fast.is_none());
        if needs_context && retained.saturating_add(512 * 1024) > 32 * 1024 * 1024 {
            return Err(replica_denied("fixed open metadata budget exhausted"));
        }
        let entry = state
            .entries
            .get_mut(&key)
            .ok_or_else(|| replica_denied("open read context disappeared during admission"))?;
        if entry.fast.is_none() {
            // Covers the second bounded source index and up to64 pinned
            // native descriptors, in addition to the original page charge.
            entry.bytes = entry.bytes.saturating_add(512 * 1024);
            entry.fast = Some(Arc::new(DfsOpenReadContext {
                version: entry.grant.file_version_id.0.clone(),
                layout: entry.grant.layout_root_id.0.clone(),
                sources: entry.sources.clone(),
                active: entry.active.clone(),
                connection,
                readers: Arc::new(std::sync::Mutex::new(HashMap::new())),
                readers_registered: AtomicBool::new(false),
            }));
        }
        let context = entry
            .fast
            .as_ref()
            .ok_or_else(|| replica_denied("fixed context was not published"))?
            .clone();
        if !context.admits(request)? {
            return Err(replica_denied("source page did not cover fixed open range"));
        }
        Ok(Some(context))
    }
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
                fast: None,
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
    fn fast_open_context_survives_source_ttl_and_close_retires_it() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let r = request();
        let first = authorizer.authorize_fast_open("reader", &r, None).unwrap();
        assert!(
            first.is_some(),
            "fast open must publish a reusable fixed context"
        );
        for entry in authorizer.open_cache.lock().unwrap().entries.values_mut() {
            entry.deadline = std::time::Instant::now() - std::time::Duration::from_secs(1);
        }
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, None)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            validator.calls.load(Ordering::SeqCst),
            1,
            "source cache TTL is not the open lifetime"
        );
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(authorizer.open_cache.lock().unwrap().entries.is_empty());
    }
    #[test]
    fn fast_open_compact_range_cannot_change_version_object_or_peer() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let mut r = request();
        let context = authorizer
            .authorize_fast_open("reader", &r, None)
            .unwrap()
            .unwrap();
        let token = r.operations[0].grant.as_ref().unwrap().token.clone();
        r.operations[0].grant = Some(afs_protocol::node_data::DfsReadGrant {
            token,
            ..Default::default()
        });
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, None)
                .unwrap()
                .is_some()
        );
        assert!(
            authorizer
                .authorize_fast_open("other-peer", &r, None)
                .is_err()
        );
        r.file_version_id = "other-version".into();
        assert!(authorizer.authorize_fast_open("reader", &r, None).is_err());
        r.file_version_id = "v".into();
        r.operations[0].chunk_offset = u64::MAX;
        assert!(authorizer.authorize_fast_open("reader", &r, None).is_err());
        r.operations[0].chunk_offset = 0;
        r.operations[0].chunk_id = "object-2".into();
        assert!(
            authorizer
                .authorize_fast_open("reader", &r, None)
                .unwrap()
                .is_some()
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(!context.active.load(Ordering::Acquire));
        assert!(
            context.admits(&r).is_err(),
            "close stops new admission while existing Arc may drain"
        );
        assert!(
            authorizer.authorize_fast_open("reader", &r, None).is_err(),
            "compact cache miss requires explicit same-version setup"
        );
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fast_open_grpc_recovers_lost_context_and_closed_owner_is_denied() {
        use crate::dfs::*;
        use crate::node::{
            chunk::{ChunkStore, LocalChunkStore, StagedChunk},
            dfs_read::{ChunkReadOp, ChunkTransfer, PeerReadBatch, ResolvedReadOp},
            rpc::peer::{GrpcChunkTransfer, PeerConnectionPool},
        };
        struct Peer;
        impl PeerAuthenticator for Peer {
            fn authenticate(
                &self,
                _: &tonic::metadata::MetadataMap,
                _: Option<std::net::SocketAddr>,
                _: Option<&[u8]>,
            ) -> afs_error::Result<String> {
                Ok("reader".into())
            }
        }
        struct Validator {
            calls: AtomicUsize,
            live: AtomicBool,
        }
        impl DfsReadGrantValidator for Validator {
            fn validate(
                &self,
                r: ValidateDfsReadGrants,
            ) -> afs_error::Result<Vec<DfsAuthorizedRead>> {
                if !self.live.load(Ordering::Acquire) {
                    return Err(replica_denied("open owner is closed"));
                }
                self.calls.fetch_add(1, Ordering::SeqCst);
                let validation = r.validations[0].clone();
                let sources = vec![OpenReadSource {
                    chunk_id: validation.chunk_id.clone(),
                    copy_ids: vec![validation.copy_id.clone()],
                    ranges: vec![(0, 65536)],
                }];
                Ok(vec![DfsAuthorizedRead {
                    expires_at_unix_ms: validation
                        .grant
                        .expires_at_unix_ms
                        .min(crate::meta::store::now_unix_ms() + 4000),
                    validation,
                    allowed_ranges: vec![(0, 65536)],
                    open_sources: serde_json::to_vec(&sources).unwrap(),
                }])
            }
        }
        let temp = tempfile::tempdir().unwrap();
        let local = Arc::new(
            LocalChunkStore::open(temp.path(), "node-a")
                .unwrap()
                .with_peer_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let staged =
            StagedChunk::new_crc32c(OperationId::new("grpc-fixed"), vec![27; 65536]).unwrap();
        local.put(staged.clone()).unwrap();
        let validator = Arc::new(Validator {
            calls: AtomicUsize::new(0),
            live: AtomicBool::new(true),
        });
        let authorizer = Arc::new(CachedDfsReadAuthorizer::new(
            validator.clone(),
            "receiver".into(),
            1,
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let service = make_dfs_chunks_server(Some(local), Arc::new(Peer), authorizer.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let peers = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                8,
            )
            .unwrap()
            .with_read_crc_mode(crate::config::DfsReadCrcMode::Off),
        );
        let transfer =
            Arc::new(GrpcChunkTransfer::new(peers, std::time::Duration::from_secs(5)).unwrap());
        let batch = PeerReadBatch {
            file_version_id: FileVersionId::new("v"),
            layout_root_id: LayoutRootId::new("l"),
            operations: vec![ResolvedReadOp {
                op: ChunkReadOp {
                    chunk_id: staged.chunk.id.clone(),
                    chunk_offset: 0,
                    length: 65536,
                    output_offset: 0,
                },
                source: SourceCandidate {
                    copy_id: CopyId::new("copy"),
                    chunk_id: staged.chunk.id.clone(),
                    role: CopyRole::DurableReplica,
                    state: CopyState::Ready,
                    location: CopyLocation::Node {
                        node_id: "node-a".into(),
                        node_epoch: 1,
                        device_id: "d".into(),
                        device_epoch: 1,
                        catalog_revision: 1,
                    },
                    data_endpoint: Some(endpoint),
                    load_hint: 0,
                    read_grant: DfsReadGrant {
                        namespace_id: NamespaceId::new("n"),
                        file_version_id: FileVersionId::new("v"),
                        layout_root_id: LayoutRootId::new("l"),
                        caller_node_id: "reader".into(),
                        caller_node_epoch: 1,
                        expires_at_unix_ms: crate::meta::store::now_unix_ms() + 30000,
                        fence: 1,
                        token: "dfs-open-v1:read-open:test".into(),
                    },
                },
            }],
        };
        async fn read(
            transfer: Arc<GrpcChunkTransfer>,
            batch: PeerReadBatch,
            chunk: ChunkObject,
        ) -> afs_error::Result<Vec<u8>> {
            tokio::task::spawn_blocking(move || {
                let mut out = vec![0; 65536];
                transfer.read_crc32c_range(&batch, &chunk, &mut out)?;
                Ok(out)
            })
            .await
            .unwrap()
        }
        assert_eq!(
            read(transfer.clone(), batch.clone(), staged.chunk.clone())
                .await
                .unwrap(),
            vec![27; 65536]
        );
        assert_eq!(validator.calls.load(Ordering::SeqCst), 1);
        let mut expired = batch.clone();
        expired.operations[0].source.read_grant.expires_at_unix_ms = 1;
        assert_eq!(
            read(transfer.clone(), expired, staged.chunk.clone())
                .await
                .unwrap(),
            vec![27; 65536],
            "fixed context outlives source grant TTL"
        );
        authorizer.open_cache.lock().unwrap().entries.clear(); // node cache loss, durable owner remains live.
        assert_eq!(
            read(transfer.clone(), batch.clone(), staged.chunk.clone())
                .await
                .unwrap(),
            vec![27; 65536]
        );
        assert_eq!(
            validator.calls.load(Ordering::SeqCst),
            2,
            "one full setup retry rebuilds the same version context"
        );
        validator.live.store(false, Ordering::Release);
        authorizer
            .close_open_context("reader", "dfs-open-v1:read-open:test")
            .unwrap();
        assert!(read(transfer.clone(), batch, staged.chunk).await.is_err());
        server.abort();
    }
    #[test]
    fn fast_open_new_connection_must_reestablish_file_authority() {
        let validator = Arc::new(PageValidator {
            calls: AtomicUsize::new(0),
            gate: None,
        });
        let authorizer = CachedDfsReadAuthorizer::new(validator.clone(), "receiver".into(), 1);
        let r = request();
        let a = Some("127.0.0.1:1001".parse().unwrap());
        let b = Some("127.0.0.1:1002".parse().unwrap());
        authorizer.authorize_fast_open("reader", &r, a).unwrap();
        let mut compact = r.clone();
        compact.operations[0].grant = Some(afs_protocol::node_data::DfsReadGrant {
            token: r.operations[0].grant.as_ref().unwrap().token.clone(),
            ..Default::default()
        });
        assert!(
            authorizer
                .authorize_fast_open("reader", &compact, a)
                .unwrap()
                .is_some()
        );
        assert!(
            authorizer
                .authorize_fast_open("reader", &compact, b)
                .is_err()
        );
        authorizer.authorize_fast_open("reader", &r, b).unwrap();
        assert_eq!(validator.calls.load(Ordering::SeqCst), 2);
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
