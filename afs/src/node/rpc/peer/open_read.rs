use super::*;
pub(super) type OpenReadPeers = std::collections::HashMap<String, OpenReadPeerSet>;
#[derive(Default)]
pub(super) struct OpenReadPeerSet {
    peers: std::collections::HashMap<String, (u64, String)>,
    closing: bool,
}

// Close retries retain only bounded peer identities, never data buffers. They
// run independently of subsequent reads and disappear with the transfer owner.
pub(super) fn start_close_retirement(
    runtime: &PeerRuntime,
    state: &Arc<std::sync::Mutex<OpenReadPeers>>,
    peers: Arc<PeerConnectionPool>,
    timeout: Duration,
) {
    let state = Arc::downgrade(state);
    let future = async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let references = {
                let Some(state) = state.upgrade() else {
                    break;
                };
                let Ok(state) = state.lock() else {
                    break;
                };
                state
                    .iter()
                    .filter(|(_, entry)| entry.closing)
                    .map(|(reference, _)| reference.clone())
                    .collect::<Vec<_>>()
            };
            for reference in references {
                if state.strong_count() == 0 {
                    return;
                }
                if let Err(error) = retire_closed_peers(&state, &peers, timeout, &reference).await {
                    afs_logging::debug!("dfs.open_read_close_retry_pending"; "error" => error.to_string());
                }
            }
        }
    };
    match runtime {
        PeerRuntime::Existing(handle) => {
            handle.spawn(future);
        }
        PeerRuntime::Owned(runtime) => {
            runtime.spawn(future);
        }
    }
}

async fn retire_closed_peers(
    state: &std::sync::Weak<std::sync::Mutex<OpenReadPeers>>,
    pool: &PeerConnectionPool,
    timeout: Duration,
    reference: &str,
) -> afs_error::Result<()> {
    let pending = {
        let Some(state) = state.upgrade() else {
            return Ok(());
        };
        let state = state
            .lock()
            .map_err(|_| dfs_protocol_error("open read peer table poisoned"))?;
        state
            .get(reference)
            .filter(|entry| entry.closing)
            .map(|entry| entry.peers.clone())
            .unwrap_or_default()
    };
    let mut failure = None;
    for (node, (epoch, endpoint)) in pending {
        if state.strong_count() == 0 {
            break;
        }
        let result = tokio::time::timeout(timeout, async {
            let channel = pool.channel(&node, epoch, &endpoint).await?;
            DfsChunksClient::new(channel)
                .close_read_context(afs_protocol::node_data::DfsCloseReadContextRequest {
                    context: format!("dfs-open-v1:{reference}"),
                })
                .await
                .map_err(afs_transport::grpc::error_status::status_to_error)?;
            Ok::<_, afs_error::Error>(())
        })
        .await
        .map_err(|_| dfs_protocol_error("open read close deadline expired"))
        .and_then(|result| result);
        match result {
            Ok(()) => {
                if let Some(metrics) = &pool.read_work_metrics {
                    metrics.read_rpc("peer_close_open_read_context");
                }
                if let Some(state) = state.upgrade() {
                    let mut state = state
                        .lock()
                        .map_err(|_| dfs_protocol_error("open read peer table poisoned"))?;
                    if let Some(entry) = state.get_mut(reference)
                        && entry.closing
                    {
                        if entry
                            .peers
                            .get(&node)
                            .is_some_and(|v| v == &(epoch, endpoint.clone()))
                        {
                            entry.peers.remove(&node);
                        }
                        if entry.peers.is_empty() {
                            state.remove(reference);
                        }
                    }
                }
            }
            Err(error) => {
                failure = Some(error);
            }
        }
    }
    if let Some(error) = failure {
        Err(error)
    } else {
        Ok(())
    }
}
impl GrpcChunkTransfer {
    pub(super) fn remember_open_read(
        &self,
        source: &crate::dfs::SourceCandidate,
    ) -> afs_error::Result<()> {
        let Some(reference) = source.read_grant.token.strip_prefix("dfs-open-v1:") else {
            return Ok(());
        };
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &source.location
        else {
            return Err(dfs_protocol_error("open read has no serving node"));
        };
        let endpoint = source
            .data_endpoint
            .as_ref()
            .ok_or_else(|| dfs_protocol_error("open read endpoint absent"))?;
        if reference.len() > 512 || endpoint.len() > 4096 {
            return Err(dfs_protocol_error("open read peer identity exceeds budget"));
        }
        let mut peers = self
            .open_read_peers
            .lock()
            .map_err(|_| dfs_protocol_error("open read peer table poisoned"))?;
        if !peers.contains_key(reference) && peers.len() >= 128 {
            return Err(dfs_protocol_error(
                "open read peer context budget exhausted",
            ));
        }
        let entry = peers.entry(reference.into()).or_default();
        if entry.closing {
            return Err(dfs_protocol_error("open read context is closing"));
        }
        let entries = &mut entry.peers;
        if !entries.contains_key(node_id) && entries.len() >= 32 {
            return Err(dfs_protocol_error(
                "open read serving peer budget exhausted",
            ));
        }
        if entries
            .get(node_id)
            .is_none_or(|(epoch, address)| epoch != node_epoch || address != endpoint)
        {
            if let Some(metrics) = &self.peers.read_work_metrics {
                metrics.check(crate::node::read_work::ReadCheck::PeerRegister);
            }
            entries.insert(node_id.clone(), (*node_epoch, endpoint.clone()));
        }
        Ok(())
    }
    pub(super) fn close_open_context_impl(&self, reference: &str) -> afs_error::Result<()> {
        self.forget_native_batch_context(reference);
        self.fast_open_ready
            .lock()
            .map_err(|_| dfs_protocol_error("fixed open readiness table poisoned"))?
            .retain(|(key, _, _)| key != &format!("dfs-open-v1:{reference}"));

        let _scope = self
            .peers
            .read_work_metrics
            .as_ref()
            .map(|m| m.authorization_scope("close_peer"));
        {
            let mut peers = self
                .open_read_peers
                .lock()
                .map_err(|_| dfs_protocol_error("open read peer table poisoned"))?;
            if let Some(entry) = peers.get_mut(reference) {
                entry.closing = true;
            }
        }
        // A failed notification stays in the same bounded table. The background
        // worker retries it even when the application never reads again.
        self.runtime.block_on(retire_closed_peers(
            &Arc::downgrade(&self.open_read_peers),
            &self.peers,
            self.timeout,
            reference,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retirement_failed_close_keeps_pending_peer() {
        let pool = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                2,
            )
            .unwrap(),
        );
        let transfer = GrpcChunkTransfer::new(pool, Duration::from_millis(20)).unwrap();
        transfer.open_read_peers.lock().unwrap().insert(
            "read-open:close-test".into(),
            OpenReadPeerSet {
                peers: std::collections::HashMap::from([(
                    "node-a".into(),
                    (1, "http://127.0.0.1:1".into()),
                )]),
                closing: false,
            },
        );
        assert!(
            transfer
                .close_open_context_impl("read-open:close-test")
                .is_err()
        );
        assert!(
            transfer
                .open_read_peers
                .lock()
                .unwrap()
                .contains_key("read-open:close-test"),
            "failed close must remain pending until acknowledgment"
        );
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    struct Peer;
    impl crate::node::rpc::data::PeerAuthenticator for Peer {
        fn authenticate(
            &self,
            _: &tonic::metadata::MetadataMap,
            _: Option<std::net::SocketAddr>,
            _: Option<&[u8]>,
        ) -> afs_error::Result<String> {
            Ok("reader".into())
        }
    }
    struct LostAcknowledgment {
        acknowledged: AtomicBool,
        calls: AtomicUsize,
    }
    impl crate::node::rpc::data::DfsReadAuthorizer for LostAcknowledgment {
        fn authorize(&self, _: &str, _: &DfsReadRangesRequest) -> afs_error::Result<()> {
            Ok(())
        }
        fn close_open_context(&self, _: &str, _: &str) -> afs_error::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.acknowledged.swap(true, Ordering::SeqCst) {
                // The close took effect, but its first acknowledgment was lost.
                return Err(dfs_protocol_error("injected lost close acknowledgment"));
            }
            Ok(())
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retirement_lost_close_ack_retries_without_another_read() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let authorizer = Arc::new(LostAcknowledgment {
            acknowledged: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        });
        let service = crate::node::rpc::data::make_dfs_chunks_server(
            None,
            Arc::new(Peer),
            authorizer.clone(),
        );
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let pool = Arc::new(
            PeerConnectionPool::new(
                afs_transport::GrpcConfig::default(),
                afs_transport::TlsConfig::Disabled,
                2,
            )
            .unwrap(),
        );
        let transfer = Arc::new(GrpcChunkTransfer::new(pool, Duration::from_secs(1)).unwrap());
        transfer.open_read_peers.lock().unwrap().insert(
            "read-open:lost-ack".into(),
            OpenReadPeerSet {
                peers: std::collections::HashMap::from([("node-a".into(), (1, endpoint))]),
                closing: false,
            },
        );
        let first = transfer.clone();
        assert!(tokio::task::spawn_blocking(move || first.close_open_context_impl("read-open:lost-ack")).await.unwrap().is_err());
        assert!(
            transfer
                .open_read_peers
                .lock()
                .unwrap()
                .contains_key("read-open:lost-ack")
        );
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                if transfer.open_read_peers.lock().unwrap().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("background close retry must retire the acknowledged peer");
        assert!(authorizer.calls.load(Ordering::SeqCst) >= 2);
        let last = transfer.clone();
        tokio::task::spawn_blocking(move || last.close_open_context_impl("read-open:lost-ack"))
            .await
            .unwrap()
            .unwrap();
        server.abort();
    }
}
