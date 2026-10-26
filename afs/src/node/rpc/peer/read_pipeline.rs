//! Per-peer ready queues and owned native reads. An uncongested request uses
//! the direct async core; only an actual backlog starts bounded queue drivers.
use super::crc32c::peer_status_error;
use super::*;
use crate::node::read_pipeline::{OwnedNativeRead, ReadPipelineLimits};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PeerKey {
    node: String,
    epoch: u64,
    endpoint: String,
}

struct QueuedRead {
    read: OwnedNativeRead,
    done: tokio::sync::oneshot::Sender<afs_error::Result<OwnedNativeRead>>,
    _pending: tokio::sync::OwnedSemaphorePermit,
    _queue_activity: Option<crate::node::read_activity::ActivityGuard>,
    _pending_activity: Option<crate::node::read_activity::ActivityGuard>,
}
#[derive(Default)]
struct PeerQueue {
    items: VecDeque<QueuedRead>,
    drivers: usize,
    turn: Arc<tokio::sync::Mutex<()>>,
}
#[derive(Default)]
struct PipelineQueues {
    peers: HashMap<PeerKey, PeerQueue>,
    queued: usize,
    capabilities: HashMap<(PeerKey, String), usize>,
}
pub(super) struct PeerReadPipeline {
    limits: ReadPipelineLimits,
    rpc: Arc<tokio::sync::Semaphore>,
    pending: Arc<tokio::sync::Semaphore>,
    queues: Mutex<PipelineQueues>,
}

fn expired(message: &'static str) -> afs_error::Error {
    afs_error::Error::coded(afs_error::CLIENT_DEADLINE_EXCEEDED, message)
}
fn peer_key(read: &OwnedNativeRead) -> afs_error::Result<PeerKey> {
    if read.batch.operations.len() != 1 {
        return Err(dfs_protocol_error("native read needs one range"));
    }
    let source = &read.batch.operations[0].source;
    let crate::dfs::CopyLocation::Node {
        node_id,
        node_epoch,
        ..
    } = &source.location
    else {
        return Err(dfs_protocol_error("native read has no node peer"));
    };
    let endpoint = source
        .data_endpoint
        .as_ref()
        .ok_or_else(|| dfs_protocol_error("peer endpoint absent"))?;
    if endpoint.len() > 4096 || source.read_grant.token.len() > 524 {
        return Err(dfs_protocol_error("peer queue identity exceeds bound"));
    }
    Ok(PeerKey {
        node: node_id.clone(),
        epoch: *node_epoch,
        endpoint: endpoint.clone(),
    })
}
fn compatible(left: &OwnedNativeRead, right: &OwnedNativeRead) -> bool {
    let a = &left.batch.operations[0].source.read_grant;
    let b = &right.batch.operations[0].source.read_grant;
    left.group_handle == right.group_handle
        && left.batch.file_version_id == right.batch.file_version_id
        && left.batch.layout_root_id == right.batch.layout_root_id
        && a.token == b.token
        && a.namespace_id == b.namespace_id
        && a.caller_node_id == b.caller_node_id
        && a.caller_node_epoch == b.caller_node_epoch
}

impl GrpcChunkTransfer {
    /// Borrowed/cache/CRC paths share the same RPC and pending limits as the
    /// owned fast lane. A scope owner can be cloned by a streaming producer.
    pub(super) async fn admit_borrowed_read(
        &self,
    ) -> afs_error::Result<crate::node::read_pipeline::ReadLifetime> {
        let inherited = crate::node::read_pipeline::current_read_scope();
        let deadline = inherited
            .as_ref()
            .map_or_else(|| std::time::Instant::now() + self.timeout, |s| s.deadline);
        let Some(pipeline) = self.read_pipeline.get() else {
            return Ok(Arc::new(inherited));
        };
        let pending = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            pipeline.pending.clone().acquire_owned(),
        )
        .await
        .map_err(|_| expired("read pending admission expired"))?
        .map_err(|_| expired("read pending admission closed"))?;
        let rpc = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            pipeline.rpc.clone().acquire_owned(),
        )
        .await
        .map_err(|_| expired("read RPC admission expired"))?
        .map_err(|_| expired("read RPC admission closed"))?;
        Ok(Arc::new((pending, rpc, inherited.map(|s| s.lifetime))))
    }

    pub(super) fn configure_native_read_pipeline(
        &self,
        limits: ReadPipelineLimits,
    ) -> afs_error::Result<()> {
        let limits = limits.validate()?;
        self.read_pipeline
            .set(Arc::new(PeerReadPipeline {
                limits,
                rpc: Arc::new(tokio::sync::Semaphore::new(limits.max_rpcs)),
                pending: Arc::new(tokio::sync::Semaphore::new(limits.max_pending)),
                queues: Mutex::new(PipelineQueues::default()),
            }))
            .map_err(|_| {
                afs_error::Error::coded(
                    afs_error::CONFIG_INVALID,
                    "read pipeline already configured",
                )
            })
    }

    pub(super) fn note_native_batch_capability(
        &self,
        batch: &crate::node::dfs_read::PeerReadBatch,
        metadata: &tonic::metadata::MetadataMap,
    ) {
        let Some(pipeline) = self.read_pipeline.get() else {
            return;
        };
        let Some(item) = batch.operations.first() else {
            return;
        };
        let crate::dfs::CopyLocation::Node {
            node_id,
            node_epoch,
            ..
        } = &item.source.location
        else {
            return;
        };
        let Some(endpoint) = &item.source.data_endpoint else {
            return;
        };
        let fast = metadata.get("x-afs-open-fast").is_some_and(|v| v == "1");
        let count = metadata
            .get("x-afs-read-batch-v1")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| (1..=128).contains(n));
        let key = (
            PeerKey {
                node: node_id.clone(),
                epoch: *node_epoch,
                endpoint: endpoint.clone(),
            },
            item.source.read_grant.token.clone(),
        );
        let mut queues = pipeline
            .queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if queues.capabilities.len() >= 1024
            && !queues.capabilities.contains_key(&key)
            && let Some(old) = queues.capabilities.keys().next().cloned()
        {
            queues.capabilities.remove(&old);
        }
        if fast && let Some(count) = count {
            queues.capabilities.insert(key, count);
        } else {
            queues.capabilities.remove(&key);
        }
    }

    pub(super) fn forget_native_batch_context(&self, reference: &str) {
        if let Some(pipeline) = self.read_pipeline.get() {
            let token = format!("dfs-open-v1:{reference}");
            pipeline
                .queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .capabilities
                .retain(|(_, context), _| context != &token);
        }
    }

    async fn single_owned(&self, read: &mut OwnedNativeRead) -> afs_error::Result<()> {
        read.allocate().await?;
        tokio::time::timeout_at(
            tokio::time::Instant::from_std(read.deadline),
            self.read_crc32c_attempt_async(
                &read.batch,
                &read.chunk,
                &mut read.buffer,
                true,
                Some(read.deadline),
            ),
        )
        .await
        .map_err(|_| expired("peer read deadline exceeded"))?
    }

    pub(super) async fn read_owned_native_pipeline(
        self: Arc<Self>,
        mut read: OwnedNativeRead,
    ) -> afs_error::Result<OwnedNativeRead> {
        let pipeline = self.read_pipeline.get().cloned().ok_or_else(|| {
            afs_error::Error::coded(
                afs_error::CONFIG_INVALID,
                "native read pipeline is not configured",
            )
        })?;
        if self.peers.read_crc_mode != crate::config::DfsReadCrcMode::Off {
            return Err(afs_error::Error::coded(
                afs_error::NODE_TRANSFER_UNSUPPORTED,
                "async native read requires CRC off",
            ));
        }
        let deadline = tokio::time::Instant::from_std(read.deadline);
        let pending = tokio::time::timeout_at(deadline, pipeline.pending.clone().acquire_owned())
            .await
            .map_err(|_| expired("peer queue admission expired"))?
            .map_err(|_| dfs_protocol_error("peer queue admission closed"))?;
        let pending_activity = self.peers.read_work_metrics.as_ref().map(|m| {
            m.activities
                .enter(crate::node::read_activity::Activity::PeerPending, 0)
        });
        let key = peer_key(&read)?;
        let peer_turn = {
            let mut queues = pipeline
                .queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !queues.peers.contains_key(&key) && queues.peers.len() >= 128 {
                let old = queues
                    .peers
                    .iter()
                    .find(|(_, p)| {
                        p.items.is_empty() && p.drivers == 0 && Arc::strong_count(&p.turn) == 1
                    })
                    .map(|(key, _)| key.clone());
                if let Some(old) = old {
                    queues.peers.remove(&old);
                } else {
                    return Err(dfs_protocol_error("peer queue count exceeds128"));
                }
            }
            let no_backlog = queues.queued == 0;
            let peer = queues.peers.entry(key.clone()).or_default();
            if no_backlog && peer.items.is_empty() && peer.drivers == 0 {
                peer.turn.clone().try_lock_owned().ok()
            } else {
                None
            }
        };
        if let Some(_peer_turn) = peer_turn
            && let Ok(_rpc) = pipeline.rpc.clone().try_acquire_owned()
        {
            let _pending = pending;
            let _pending_activity = pending_activity;
            self.single_owned(&mut read).await?;
            return Ok(read);
        }
        let (done, result) = tokio::sync::oneshot::channel();
        let spawn = {
            let mut queues = pipeline
                .queues
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !queues.peers.contains_key(&key) && queues.peers.len() >= 128 {
                return Err(dfs_protocol_error("peer queue count exceeds 128"));
            }
            let peer = queues.peers.entry(key.clone()).or_default();
            peer.items.push_back(QueuedRead {
                read,
                done,
                _pending: pending,
                _queue_activity: self.peers.read_work_metrics.as_ref().map(|m| {
                    m.activities
                        .enter(crate::node::read_activity::Activity::PeerQueue, 0)
                }),
                _pending_activity: pending_activity,
            });
            let spawn = peer.drivers == 0;
            if spawn {
                peer.drivers += 1;
            }
            queues.queued += 1;
            spawn
        };
        if spawn {
            let transfer = self.clone();
            let state = pipeline.clone();
            let future = async move {
                transfer.drive_peer_queue(state, key).await;
            };
            match &self.runtime {
                PeerRuntime::Existing(handle) => {
                    handle.spawn(future);
                }
                PeerRuntime::Owned(runtime) => {
                    runtime.spawn(future);
                }
            }
        }
        // Drivers enforce the original deadline and acknowledge retirement.
        // Do not timeout/drop this receiver while a driver still owns the work.
        result
            .await
            .map_err(|_| dfs_protocol_error("peer queue driver stopped"))?
    }

    async fn drive_peer_queue(self: Arc<Self>, pipeline: Arc<PeerReadPipeline>, key: PeerKey) {
        loop {
            let (deadline, turn) = {
                let mut queues = pipeline
                    .queues
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(peer) = queues.peers.get_mut(&key) else {
                    return;
                };
                if let Some(item) = peer.items.front() {
                    (item.read.deadline, peer.turn.clone())
                } else {
                    peer.drivers = peer.drivers.saturating_sub(1);
                    return;
                }
            };
            let rpc_wait = self.peers.read_work_metrics.as_ref().map(|m| {
                m.activities
                    .enter(crate::node::read_activity::Activity::RpcWait, 0)
            });
            let rpc = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
                let peer = turn.lock_owned().await;
                pipeline
                    .rpc
                    .clone()
                    .acquire_owned()
                    .await
                    .map(|rpc| (peer, rpc))
            })
            .await;
            drop(rpc_wait);
            let (_peer_turn, _rpc) = match rpc {
                Ok(Ok(permit)) => permit,
                Err(_) | Ok(Err(_)) => {
                    let closed = pipeline.rpc.is_closed();
                    let mut expired_items = Vec::new();
                    {
                        let mut queues = pipeline
                            .queues
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if let Some(peer) = queues.peers.get_mut(&key) {
                            while peer.items.front().is_some_and(|item| {
                                closed
                                    || item.done.is_closed()
                                    || item.read.deadline <= std::time::Instant::now()
                            }) {
                                if let Some(item) = peer.items.pop_front() {
                                    expired_items.push(item);
                                }
                            }
                        }
                        queues.queued = queues.queued.saturating_sub(expired_items.len());
                    }
                    for item in expired_items {
                        let error = if closed {
                            dfs_protocol_error("peer RPC admission closed")
                        } else {
                            expired("peer queue expired")
                        };
                        let _ = item.done.send(Err(error));
                    }
                    continue;
                }
            };
            let selected = self.take_peer_group(&pipeline, &key);
            let mut group = Vec::with_capacity(selected.len());
            for mut item in selected {
                drop(item._queue_activity.take());
                if item.done.is_closed() {
                    continue;
                }
                if item.read.deadline <= std::time::Instant::now() {
                    let _ = item.done.send(Err(expired("peer queue expired")));
                } else {
                    group.push(item);
                }
            }
            if group.is_empty() {
                continue;
            }
            let statuses = if group.len() == 1 {
                vec![self.single_owned(&mut group[0].read).await.err()]
            } else {
                self.group_owned(&pipeline, &key, &mut group).await
            };
            let mut statuses = statuses.into_iter();
            for item in group {
                let QueuedRead {
                    read,
                    done,
                    _pending,
                    ..
                } = item;
                let result = match statuses.next() {
                    Some(None) => Ok(read),
                    Some(Some(error)) => Err(error),
                    None => Err(dfs_protocol_error("peer completion mapping missing")),
                };
                let _ = done.send(result);
            }
        }
    }

    fn take_peer_group(&self, pipeline: &PeerReadPipeline, key: &PeerKey) -> Vec<QueuedRead> {
        let mut queues = pipeline
            .queues
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let limit = queues
            .peers
            .get(key)
            .and_then(|peer| peer.items.front())
            .and_then(|first| {
                queues.capabilities.get(&(
                    key.clone(),
                    first.read.batch.operations[0]
                        .source
                        .read_grant
                        .token
                        .clone(),
                ))
            })
            .copied()
            .unwrap_or(1)
            .min(pipeline.limits.max_batch_ops);
        let mut group = Vec::new();
        if let Some(peer) = queues.peers.get_mut(key)
            && let Some(first) = peer.items.pop_front()
        {
            let mut bytes = first.read.body_len();
            let mut charge = first.read.capacity_charge();
            group.push(first);
            let mut index = 0;
            let mut scanned = 0;
            while index < peer.items.len() && group.len() < limit && scanned < 64 {
                scanned += 1;
                let item = &peer.items[index];
                if !item.done.is_closed()
                    && item.read.deadline > std::time::Instant::now()
                    && compatible(&group[0].read, &item.read)
                    && bytes.saturating_add(item.read.body_len())
                        <= crate::node::dfs_read::MAX_DFS_READ_BYTES as usize
                    && charge.saturating_add(item.read.capacity_charge())
                        <= pipeline.limits.max_reply_bytes
                {
                    if let Some(item) = peer.items.remove(index) {
                        bytes += item.read.body_len();
                        charge += item.read.capacity_charge();
                        group.push(item);
                    }
                } else {
                    index += 1;
                }
            }
        }
        queues.queued = queues.queued.saturating_sub(group.len());
        group
    }

    async fn group_owned(
        &self,
        pipeline: &PeerReadPipeline,
        key: &PeerKey,
        group: &mut [QueuedRead],
    ) -> Vec<Option<afs_error::Error>> {
        let deadline = group
            .iter()
            .map(|item| item.read.deadline)
            .min()
            .unwrap_or_else(std::time::Instant::now);
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.read_native_group(key, group),
        )
        .await
        .unwrap_or_else(|_| Err((expired("peer batch deadline exceeded"), false)));
        match result {
            Ok(()) => (0..group.len()).map(|_| None).collect(),
            Err((_, true)) => {
                // Capability/context recovery uses the unchanged single-range
                // protocol and original deadlines; no body has been published.
                if let Some(first) = group.first() {
                    let token = first.read.batch.operations[0]
                        .source
                        .read_grant
                        .token
                        .clone();
                    pipeline
                        .queues
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .capabilities
                        .remove(&(key.clone(), token.clone()));
                    self.fast_open_ready
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .retain(|(context, node, epoch)| {
                            context != &token || node != &key.node || epoch != &key.epoch
                        });
                }
                let mut result = Vec::with_capacity(group.len());
                for item in group {
                    result.push(self.single_owned(&mut item.read).await.err());
                }
                result
            }
            Err((error, false)) => (0..group.len()).map(|_| Some(error.clone())).collect(),
        }
    }

    async fn read_native_group(
        &self,
        key: &PeerKey,
        group: &mut [QueuedRead],
    ) -> std::result::Result<(), (afs_error::Error, bool)> {
        for item in group.iter_mut() {
            item.read.allocate().await.map_err(|e| (e, false))?;
        }

        let first = group
            .first()
            .ok_or_else(|| (dfs_protocol_error("empty peer group"), false))?;
        let sequence = self
            .next_read
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut wire = DfsReadRangesRequest {
            read_id: format!("batch-{sequence}"),
            attempt_id: format!("batch-attempt-{sequence}"),
            file_version_id: first.read.batch.file_version_id.0.clone(),
            layout_root_id: first.read.batch.layout_root_id.0.clone(),
            include_blake3_outboard: false,
            operations: Vec::with_capacity(group.len()),
        };
        let mut destination = 0_u64;
        for (index, item) in group.iter().enumerate() {
            if !compatible(&first.read, &item.read) {
                return Err((dfs_protocol_error("incompatible peer group"), false));
            }
            let item = &item.read.batch.operations[0];
            wire.operations
                .push(afs_protocol::node_data::DfsChunkReadOp {
                    operation_index: u32::try_from(index).map_err(|_| {
                        (dfs_protocol_error("peer operation index overflow"), false)
                    })?,
                    chunk_id: item.op.chunk_id.0.clone(),
                    chunk_offset: item.op.chunk_offset,
                    length: item.op.length,
                    destination_offset: destination,
                    source_copy_id: item.source.copy_id.0.clone(),
                    grant: Some(afs_protocol::node_data::DfsReadGrant {
                        token: item.source.read_grant.token.clone(),
                        ..Default::default()
                    }),
                });
            destination = destination
                .checked_add(item.op.length)
                .filter(|n| *n <= crate::node::dfs_read::MAX_DFS_READ_BYTES)
                .ok_or_else(|| (dfs_protocol_error("peer group byte budget exceeded"), false))?;
        }
        let channel = self
            .peers
            .channel(&key.node, key.epoch, &key.endpoint)
            .await
            .map_err(|error| (error, false))?;
        let mut client = DfsChunksClient::new(channel)
            .max_encoding_message_size(self.peers.config.max_encoding_message_bytes)
            .max_decoding_message_size(self.peers.config.max_decoding_message_bytes);
        let mut request = tonic::Request::new(wire.clone());
        request.metadata_mut().insert(
            "x-afs-read-crc-mode",
            tonic::metadata::MetadataValue::from_static("off"),
        );
        request.metadata_mut().insert(
            "x-afs-open-fast",
            tonic::metadata::MetadataValue::from_static("1"),
        );
        let deadline = group
            .iter()
            .map(|item| item.read.deadline)
            .min()
            .ok_or_else(|| (dfs_protocol_error("empty peer group"), false))?;
        request.set_timeout(
            crate::node::read_pipeline::remaining_read_budget(deadline, self.timeout)
                .map_err(|error| (error, false))?,
        );
        if let Some(metrics) = &self.peers.read_work_metrics {
            metrics.read_rpc("peer_read_ranges");
        }
        let _activity = self.peers.read_work_metrics.as_ref().map(|m| {
            m.activities
                .enter(crate::node::read_activity::Activity::RangeRpc, 0)
        });
        let response = client.read_ranges(request).await.map_err(|status| {
            let compatibility = matches!(
                status.code(),
                tonic::Code::Unimplemented
                    | tonic::Code::FailedPrecondition
                    | tonic::Code::PermissionDenied
                    | tonic::Code::Unavailable
                    | tonic::Code::ResourceExhausted
            );
            (peer_status_error(status), compatibility)
        })?;
        let accepted = response
            .metadata()
            .get("x-afs-open-fast")
            .is_some_and(|v| v == "1")
            && response
                .metadata()
                .get("x-afs-read-batch-v1")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<usize>().ok())
                .is_some_and(|limit| limit >= group.len());
        if !accepted {
            return Err((
                dfs_protocol_error("peer did not accept fixed-open batch"),
                true,
            ));
        }
        let mut stream = response.into_inner();
        for (index, queued) in group.iter_mut().enumerate() {
            let mut one = wire.clone();
            let mut operation = wire.operations[index].clone();
            operation.operation_index = 0;
            operation.destination_offset = 0;
            one.operations = vec![operation];
            let mut decoder = super::crc32c::CrcRangeDecoder::new_fast(
                &one,
                &queued.read.chunk,
                &mut queued.read.buffer,
                self.peers.read_work_metrics.clone(),
            )
            .map_err(|error| (error, false))?;
            loop {
                let mut frame = stream
                    .message()
                    .await
                    .map_err(|s| (peer_status_error(s), false))?
                    .ok_or_else(|| {
                        (
                            dfs_protocol_error("peer group ended before completion"),
                            false,
                        )
                    })?;
                let complete = match frame.body.as_mut() {
                    Some(dfs_read_ranges_frame::Body::Header(header)) => {
                        if header.operation_index as usize != index {
                            return Err((
                                dfs_protocol_error("peer group header order differs"),
                                false,
                            ));
                        }
                        header.operation_index = 0;
                        false
                    }
                    Some(dfs_read_ranges_frame::Body::Completion(done)) => {
                        if done.operation_index as usize != index {
                            return Err((
                                dfs_protocol_error("peer group completion order differs"),
                                false,
                            ));
                        }
                        done.operation_index = 0;
                        true
                    }
                    _ => false,
                };
                decoder.accept(frame).map_err(|error| (error, false))?;
                if complete {
                    break;
                }
            }
            decoder.finish().map_err(|error| (error, false))?;
        }
        if stream
            .message()
            .await
            .map_err(|s| (peer_status_error(s), false))?
            .is_some()
        {
            return Err((dfs_protocol_error("peer group has trailing data"), false));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{DiskStreamFixture, disk_stream_batch};
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::{Duration, Instant},
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;

    type NativeFixture = (
        Arc<GrpcChunkTransfer>,
        crate::dfs::ChunkObject,
        String,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    );
    async fn fixture(mode: u8) -> NativeFixture {
        fixture_with_rpcs(mode, 1).await
    }
    async fn fixture_with_rpcs(mode: u8, max_rpcs: usize) -> NativeFixture {
        let staged = crate::node::chunk::StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("native-pipeline-test"),
            vec![37; 1024 * 1024],
        )
        .unwrap();
        let chunk = staged.chunk.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let objects = std::collections::BTreeMap::from([(
            chunk.id.0.clone(),
            (chunk.clone(), staged.bytes().to_vec()),
        )]);
        let service = DiskStreamFixture {
            chunk: chunk.clone(),
            body: Arc::new(staged.bytes().to_vec()),
            mode,
            calls: calls.clone(),
            fetch_objects: Some(Arc::new(objects)),
            fetch_gate: None,
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    afs_protocol::node_data::dfs_chunks_server::DfsChunksServer::new(service),
                )
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap()
        .with_read_crc_mode(crate::config::DfsReadCrcMode::Off);
        let transfer =
            Arc::new(GrpcChunkTransfer::new(Arc::new(pool), Duration::from_secs(3)).unwrap());
        transfer
            .configure_native_read_pipeline(ReadPipelineLimits {
                max_rpcs,
                max_batch_ops: 4,
                ..Default::default()
            })
            .unwrap();
        (transfer, chunk, endpoint, calls, server)
    }
    fn read(chunk: &crate::dfs::ChunkObject, endpoint: &str, handle: u64) -> OwnedNativeRead {
        let mut batch = disk_stream_batch(chunk, endpoint.to_owned());
        batch.operations[0].source.read_grant.token = "dfs-open-v1:read-open:pipeline-test".into();
        OwnedNativeRead {
            batch,
            chunk: chunk.clone(),
            buffer: vec![0; chunk.length as usize].into(),
            bytes: Vec::new(),
            deadline: Instant::now() + Duration::from_secs(3),
            group_handle: handle,
            _lifetime: Arc::new(()),
            activity: None,
            allocation: None,
        }
    }
    async fn wait_queued(pipeline: &PeerReadPipeline, n: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if pipeline.queues.lock().unwrap().queued == n {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_equivalence_one_peer_keeps_one_stream_and_groups_waiting_ranges() {
        let (transfer, chunk, endpoint, calls, server) = fixture_with_rpcs(21, 4).await;
        let t = transfer.clone();
        let r = read(&chunk, &endpoint, 1);
        let first = tokio::spawn(async move { t.read_owned_native_pipeline(r).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let t = transfer.clone();
            let r = read(&chunk, &endpoint, 1);
            tasks.push(tokio::spawn(async move {
                t.read_owned_native_pipeline(r).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(30)).await;
        let while_first_active = calls.load(Ordering::SeqCst);
        assert!(first.await.unwrap().is_ok());
        for task in tasks {
            assert!(task.await.unwrap().is_ok());
        }
        server.abort();
        assert_eq!(
            while_first_active, 1,
            "one peer must not open more streams while its response is active"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "four waiting ranges must share the next RPC"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_concurrency_peer_group_preserves_each_buffer_and_falls_back_before_body() {
        for mode in [17, 18, 19, 20] {
            let (transfer, chunk, endpoint, calls, server) = fixture(mode).await;
            transfer
                .clone()
                .read_owned_native_pipeline(read(&chunk, &endpoint, 1))
                .await
                .unwrap();
            let pipeline = transfer.read_pipeline.get().unwrap().clone();
            let block = pipeline.rpc.clone().acquire_owned().await.unwrap();
            let mut tasks = Vec::new();
            for _ in 0..4 {
                let t = transfer.clone();
                let r = read(&chunk, &endpoint, 1);
                tasks.push(tokio::spawn(async move {
                    t.read_owned_native_pipeline(r).await
                }));
            }
            wait_queued(&pipeline, 4).await;
            drop(block);
            for task in tasks {
                let result = task.await.unwrap();
                if mode >= 19 {
                    assert!(result.is_err());
                } else {
                    assert!(result.unwrap().buffer.iter().all(|b| *b == 37));
                }
            }
            assert_eq!(calls.load(Ordering::SeqCst), if mode == 18 { 6 } else { 2 });
            wait_queued(&pipeline, 0).await;
            assert_eq!(
                pipeline.pending.available_permits(),
                pipeline.limits.max_pending
            );
            assert_eq!(pipeline.rpc.available_permits(), 1);
            server.abort();
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn read_concurrency_peer_queue_deadline_cancel_and_closed_admission_retire() {
        for scenario in 0..3 {
            let (transfer, chunk, endpoint, calls, server) = fixture(17).await;
            let pipeline = transfer.read_pipeline.get().unwrap().clone();
            let block = pipeline.rpc.clone().acquire_owned().await.unwrap();
            let mut r = read(&chunk, &endpoint, 1);
            r.deadline = Instant::now() + Duration::from_millis(80);
            let t = transfer.clone();
            let task = tokio::spawn(async move { t.read_owned_native_pipeline(r).await });
            wait_queued(&pipeline, 1).await;
            if scenario == 0 {
                task.abort();
                assert!(matches!(task.await, Err(error) if error.is_cancelled()));
            } else {
                if scenario == 1 {
                    pipeline.rpc.close();
                }
                assert!(task.await.unwrap().is_err());
            }
            drop(block);
            tokio::time::timeout(Duration::from_secs(1), async {
                loop {
                    if pipeline.pending.available_permits() == pipeline.limits.max_pending {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(pipeline.queues.lock().unwrap().queued, 0);
            server.abort();
        }
    }
    #[test]
    fn read_concurrency_group_selection_respects_handle_context_and_remote_limit() {
        let staged = crate::node::chunk::StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("selection"),
            vec![7; 1024],
        )
        .unwrap();
        let chunk = staged.chunk;
        let pool = PeerConnectionPool::new(
            afs_transport::GrpcConfig::default(),
            afs_transport::TlsConfig::Disabled,
            1,
        )
        .unwrap();
        let transfer = GrpcChunkTransfer::new(Arc::new(pool), Duration::from_secs(1)).unwrap();
        transfer
            .configure_native_read_pipeline(ReadPipelineLimits {
                max_batch_ops: 4,
                ..Default::default()
            })
            .unwrap();
        let pipeline = transfer.read_pipeline.get().unwrap();
        let first = read(&chunk, "http://127.0.0.1:1", 1);
        let key = peer_key(&first).unwrap();
        let token = first.batch.operations[0].source.read_grant.token.clone();
        let mut receivers = Vec::new();
        let mut requests = vec![
            first,
            read(&chunk, "http://127.0.0.1:1", 2),
            read(&chunk, "http://127.0.0.1:1", 1),
            read(&chunk, "http://127.0.0.1:1", 1),
        ];
        requests[3].batch.operations[0].source.read_grant.token = "different-context".into();
        {
            let mut q = pipeline.queues.lock().unwrap();
            q.capabilities.insert((key.clone(), token), 2);
            let peer = q.peers.entry(key.clone()).or_default();
            for r in requests {
                let (done, receiver) = tokio::sync::oneshot::channel();
                receivers.push(receiver);
                peer.items.push_back(QueuedRead {
                    read: r,
                    done,
                    _pending: pipeline.pending.clone().try_acquire_owned().unwrap(),
                    _queue_activity: None,
                    _pending_activity: None,
                });
            }
            q.queued = 4;
        }
        let group = transfer.take_peer_group(pipeline, &key);
        assert_eq!(group.len(), 2);
        assert!(group.iter().all(|v| v.read.group_handle == 1));
        let q = pipeline.queues.lock().unwrap();
        assert_eq!(q.queued, 2);
        assert_eq!(q.peers[&key].items[0].read.group_handle, 2);
        assert_eq!(
            q.peers[&key].items[1].read.batch.operations[0]
                .source
                .read_grant
                .token,
            "different-context"
        );
        drop(q);
        drop(group);
        drop(receivers);
    }
    #[test]
    fn read_concurrency_reply_capacity_follows_last_shared_reference() {
        use crate::node::read_activity::{Activities, Activity};
        use std::{
            future::Future,
            sync::atomic::AtomicU64,
            task::{Context, Poll, Wake, Waker},
        };
        struct RefundObserver {
            metrics: Activities,
            saw: AtomicU64,
        }
        impl Wake for RefundObserver {
            fn wake(self: Arc<Self>) {
                self.saw.store(
                    self.metrics.owned_bytes(Activity::ReplyBytes),
                    Ordering::SeqCst,
                );
            }
        }
        let staged = crate::node::chunk::StagedChunk::new_crc32c(
            crate::dfs::OperationId::new("reply-owner"),
            vec![9; 1024],
        )
        .unwrap();
        let mut r = read(&staged.chunk, "http://127.0.0.1:1", 1);
        let slots = Arc::new(tokio::sync::Semaphore::new(4096));
        r.bytes
            .push(slots.clone().try_acquire_many_owned(4096).unwrap());
        let metrics = Activities::register(&afs_metrics::Registry::default()).unwrap();
        r.activity = Some(metrics.enter(Activity::ReplyBytes, r.buffer.capacity() as u64));
        let observer = Arc::new(RefundObserver {
            metrics,
            saw: AtomicU64::new(u64::MAX),
        });
        let waker = Waker::from(observer.clone());
        let mut cx = Context::from_waker(&waker);
        let mut waiting = Box::pin(slots.clone().acquire_many_owned(4096));
        assert!(matches!(waiting.as_mut().poll(&mut cx), Poll::Pending));
        let body = r.into_reply();
        let last = body.slice(0..1);
        drop(body);
        assert_eq!(slots.available_permits(), 0);
        drop(last);
        assert_eq!(
            observer.saw.load(Ordering::SeqCst),
            0,
            "retire reply-byte observation before refund wakes a new owner"
        );
        drop(waiting);
        assert_eq!(slots.available_permits(), 4096);
    }
}
