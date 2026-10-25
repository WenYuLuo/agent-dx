use super::*;
pub(super) type OpenReadPeers =
    std::collections::HashMap<String, std::collections::HashMap<String, (u64, String)>>;
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
        let entries = peers.entry(reference.into()).or_default();
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
        self.fast_open_ready
            .lock()
            .map_err(|_| dfs_protocol_error("fixed open readiness table poisoned"))?
            .retain(|(key, _, _)| key != &format!("dfs-open-v1:{reference}"));

        let _scope = self
            .peers
            .read_work_metrics
            .as_ref()
            .map(|m| m.authorization_scope("close_peer"));
        let peers = self
            .open_read_peers
            .lock()
            .map_err(|_| dfs_protocol_error("open read peer table poisoned"))?
            .remove(reference)
            .unwrap_or_default();
        let mut error = None;
        for (node, (epoch, endpoint)) in peers {
            let result = self.runtime.block_on(async {
                let channel = self.peers.channel(&node, epoch, &endpoint).await?;
                let mut client = DfsChunksClient::new(channel);
                tokio::time::timeout(
                    self.timeout,
                    client.close_read_context(
                        afs_protocol::node_data::DfsCloseReadContextRequest {
                            context: format!("dfs-open-v1:{reference}"),
                        },
                    ),
                )
                .await
                .map_err(|_| dfs_protocol_error("open read close deadline expired"))?
                .map_err(afs_transport::grpc::error_status::status_to_error)?;
                // Count acknowledged closes so the harness can detect a lost close.
                if let Some(metrics) = &self.peers.read_work_metrics {
                    metrics.read_rpc("peer_close_open_read_context");
                }
                Ok(())
            });
            if let Err(e) = result {
                error = Some(e);
            }
        }
        if let Some(e) = error {
            return Err(e);
        }
        Ok(())
    }
}
