//! Fixed-open async response producer. Blocking reads return their workers
//! before the bounded response stream waits for the consumer.
use super::*;

/// The 0..50ms peer-serve setting limits resource admission only. A response
/// stream follows the caller's remaining budget, capped by server policy.
pub(super) fn processing_deadline(
    metadata: &tonic::metadata::MetadataMap,
    cap: std::time::Duration,
) -> Result<std::time::Instant, Status> {
    let invalid = || Status::invalid_argument("invalid grpc-timeout budget");
    let budget = if let Some(value) = metadata.get("grpc-timeout") {
        if metadata.get_all("grpc-timeout").iter().count() != 1 {
            return Err(invalid());
        }
        let raw = value.to_str().map_err(|_| invalid())?;
        if !(2..=9).contains(&raw.len()) || !raw.is_ascii() {
            return Err(invalid());
        }
        let (digits, unit) = raw.split_at(raw.len() - 1);
        if !digits.bytes().all(|b| b.is_ascii_digit()) {
            return Err(invalid());
        }
        let n = digits.parse::<u64>().map_err(|_| invalid())?;
        let duration = match unit {
            "H" => std::time::Duration::from_secs(n.checked_mul(3600).ok_or_else(invalid)?),
            "M" => std::time::Duration::from_secs(n.checked_mul(60).ok_or_else(invalid)?),
            "S" => std::time::Duration::from_secs(n),
            "m" => std::time::Duration::from_millis(n),
            "u" => std::time::Duration::from_micros(n),
            "n" => std::time::Duration::from_nanos(n),
            _ => return Err(invalid()),
        };
        duration.min(cap)
    } else {
        cap
    };
    if budget.is_zero() {
        return Err(Status::deadline_exceeded(
            "native read processing budget expired",
        ));
    }
    std::time::Instant::now()
        .checked_add(budget)
        .ok_or_else(invalid)
}

struct ReadResponseStream {
    receiver: tokio_stream::wrappers::ReceiverStream<Result<DfsReadRangesFrame, Status>>,
    terminal: Arc<std::sync::Mutex<Option<Status>>>,
}

impl tokio_stream::Stream for ReadResponseStream {
    type Item = Result<DfsReadRangesFrame, Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match tokio_stream::Stream::poll_next(std::pin::Pin::new(&mut this.receiver), cx) {
            std::task::Poll::Ready(None) => {
                let error = this
                    .terminal
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                std::task::Poll::Ready(error.map(Err))
            }
            value => value,
        }
    }
}

async fn send_until(
    sender: &tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    body: dfs_read_ranges_frame::Body,
    deadline: std::time::Instant,
) -> Result<(), Status> {
    tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        sender.send(Ok(DfsReadRangesFrame { body: Some(body) })),
    )
    .await
    .map_err(|_| Status::deadline_exceeded("response stream deadline exceeded"))?
    .map_err(|_| Status::cancelled("fixed open consumer disconnected"))
}

impl DfsChunksService {
    pub(super) async fn read_fast_open_async(
        &self,
        peer: String,
        request: DfsReadRangesRequest,
        connection: Option<DfsOpenReadConnection>,
        deadline: std::time::Instant,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<Response<<Self as DfsChunks>::ReadRangesStream>, Status> {
        let local = self
            .local_chunks
            .clone()
            .ok_or_else(|| Status::unavailable("DFS local store absent"))?;
        local
            .configure_native_read_buffers()
            .map_err(error_to_status)?;
        let permit = Arc::new(permit);
        let cached = self
            .authorizer
            .cached_fast_open_context(&peer, &request, connection.as_ref())
            .map_err(error_to_status)?;
        let context = if cached.is_some() {
            cached
        } else {
            let guard = if let Some(connection) = connection.as_ref() {
                let token = request
                    .operations
                    .first()
                    .and_then(|op| op.grant.as_ref())
                    .map(|g| g.token.clone())
                    .unwrap_or_default();
                let key = serde_json::to_string(&(
                    peer.clone(),
                    token,
                    request.file_version_id.clone(),
                    request.layout_root_id.clone(),
                    connection.identity(),
                ))
                .map_err(|_| Status::invalid_argument("open setup coordination key"))?;
                let gate = self.open_setup_gates.entry(key).map_err(error_to_status)?;
                Some(
                    tokio::time::timeout_at(
                        tokio::time::Instant::from_std(deadline),
                        gate.lock_owned(),
                    )
                    .await
                    .map_err(|_| Status::deadline_exceeded("open setup coordination expired"))?,
                )
            } else {
                None
            };
            let checked = self
                .authorizer
                .cached_fast_open_context(&peer, &request, connection.as_ref())
                .map_err(error_to_status)?;
            if checked.is_some() {
                checked
            } else {
                let auth_permit = permit.clone();
                let authorizer = self.authorizer.clone();
                let auth_peer = peer.clone();
                let auth_request = request.clone();
                tokio::task::spawn_blocking(move || {
                    let _guard = guard;
                    let _permit = auth_permit;
                    authorizer.authorize_fast_open(&auth_peer, &auth_request, connection)
                })
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .map_err(error_to_status)?
            }
        };
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let terminal = Arc::new(std::sync::Mutex::new(None));
        let producer_terminal = terminal.clone();
        let metrics = self.payload_metrics.clone();
        let accepted = context.is_some();
        if let Some(context) = context {
            let disk = self.disk_slots.clone();
            let window = self.read_limits.max_disk_reads;
            tokio::spawn(async move {
                let producer = AsyncReadProducer {
                    local,
                    context,
                    request,
                    sender,
                    metrics,
                    deadline,
                    disk,
                    window,
                    service: permit,
                };
                let result = producer.produce().await;
                if let Err(error) = result {
                    *producer_terminal
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                }
            });
        } else {
            let authorizer = self.authorizer.clone();
            // No traceable fixed-open capability: preserve the original lane.
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let result = authorizer
                    .authorize(&peer, &request)
                    .map_err(error_to_status)
                    .and_then(|()| {
                        stream_dfs_ranges_until(
                            &local,
                            &request,
                            &sender,
                            metrics.as_ref(),
                            deadline,
                        )
                    });
                if let Err(error) = result {
                    *producer_terminal
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
                }
            });
        }
        let mut response = Response::new(Box::pin(ReadResponseStream {
            receiver: tokio_stream::wrappers::ReceiverStream::new(receiver),
            terminal,
        }) as <Self as DfsChunks>::ReadRangesStream);
        if accepted {
            response.metadata_mut().insert(
                "x-afs-open-fast",
                tonic::metadata::MetadataValue::from_static("1"),
            );
            response.metadata_mut().insert(
                "x-afs-read-batch-v1",
                tonic::metadata::MetadataValue::try_from(
                    self.read_limits.max_batch_ops.to_string(),
                )
                .map_err(|_| Status::internal("batch capability encoding failed"))?,
            );
        }
        Ok(response)
    }
}

pub(super) struct AsyncReadProducer {
    pub(super) local: Arc<LocalChunkStore>,
    pub(super) context: Arc<DfsOpenReadContext>,
    pub(super) request: DfsReadRangesRequest,
    pub(super) sender: tokio::sync::mpsc::Sender<Result<DfsReadRangesFrame, Status>>,
    pub(super) metrics: Option<DfsPayloadMetrics>,
    pub(super) deadline: std::time::Instant,
    pub(super) disk: Arc<tokio::sync::Semaphore>,
    pub(super) window: usize,
    pub(super) service: Arc<tokio::sync::OwnedSemaphorePermit>,
}

impl AsyncReadProducer {
    pub(super) async fn produce(&self) -> Result<(), Status> {
        if self.request.operations.len() == 1 {
            if self.sender.is_closed() {
                return Ok(());
            }
            let op = self.request.operations[0].clone();
            let length = usize::try_from(op.length)
                .map_err(|_| Status::invalid_argument("fixed range size overflow"))?;
            let reservation = self
                .local
                .reserve_fixed_read_until(length, self.deadline)
                .await
                .map_err(error_to_status)?;
            let payload = self
                .context
                .clone()
                .payload_reserved_async(
                    self.local.clone(),
                    op,
                    self.deadline,
                    self.disk.clone(),
                    reservation,
                    self.service.clone(),
                )
                .await
                .map_err(error_to_status)?;
            return self.send_payload(0, payload).await;
        }
        use std::collections::VecDeque;
        let mut pending = VecDeque::new();
        let mut next = 0_usize;
        let result = async {
            for index in 0..self.request.operations.len() {
                if self.sender.is_closed() {
                    return Ok(());
                }
                while next < self.request.operations.len() && pending.len() < self.window.max(1) {
                    let op = self.request.operations[next].clone();
                    let length = usize::try_from(op.length)
                        .map_err(|_| Status::invalid_argument("fixed range size overflow"))?;
                    let reservation = if pending.is_empty() {
                        self.local
                            .reserve_fixed_read_until(length, self.deadline)
                            .await
                            .map_err(error_to_status)?
                    } else {
                        let Some(reservation) = self
                            .local
                            .try_fixed_read_reservation(length)
                            .map_err(error_to_status)?
                        else {
                            break;
                        };
                        reservation
                    };
                    let context = self.context.clone();
                    let local = self.local.clone();
                    let disk = self.disk.clone();
                    let service = self.service.clone();
                    let deadline = self.deadline;
                    pending.push_back(tokio::spawn(async move {
                        context
                            .payload_reserved_async(local, op, deadline, disk, reservation, service)
                            .await
                    }));
                    next += 1;
                }
                let task = pending
                    .pop_front()
                    .ok_or_else(|| Status::internal("fixed read window is empty"))?;
                let payload = task
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .map_err(error_to_status)?;
                self.send_payload(index, payload).await?;
            }
            Ok(())
        }
        .await;
        // Ordinary cancellation/error never detaches admitted blocking reads.
        // Child tasks also hold the service lease if this producer panics.
        while let Some(task) = pending.pop_front() {
            let _ = task.await;
        }
        result
    }

    async fn send_payload(
        &self,
        index: usize,
        payload: crate::node::chunk::PeerReadPayload,
    ) -> Result<(), Status> {
        let _send = self.local.read_work_metrics().map(|m| {
            m.activities
                .enter(crate::node::read_activity::Activity::SendWait, 0)
        });
        let op = &self.request.operations[index];
        send_until(
            &self.sender,
            dfs_read_ranges_frame::Body::Header(DfsReadRangesHeader {
                read_id: self.request.read_id.clone(),
                attempt_id: self.request.attempt_id.clone(),
                operation_index: op.operation_index,
                chunk_id: op.chunk_id.clone(),
                chunk_offset: op.chunk_offset,
                length: op.length,
                source_copy_id: op.source_copy_id.clone(),
                blake3_outboard: Default::default(),
                crc32c_table: Default::default(),
            }),
            self.deadline,
        )
        .await?;
        let body = payload.into_shared_bytes();
        for start in (0..body.len()).step_by(DFS_REPLICA_FRAME_BYTES) {
            let data = body.slice(start..(start + DFS_REPLICA_FRAME_BYTES).min(body.len()));
            self.local.record_peer_read_event("data_frame");
            self.local.record_peer_read_event("shared_frame");
            if let Some(metrics) = &self.metrics {
                metrics.record("grpc", "send", "read", data.len() as u64);
            }
            send_until(
                &self.sender,
                dfs_read_ranges_frame::Body::Data(data),
                self.deadline,
            )
            .await?;
        }
        send_until(
            &self.sender,
            dfs_read_ranges_frame::Body::Completion(DfsReadRangesCompletion {
                read_id: self.request.read_id.clone(),
                attempt_id: self.request.attempt_id.clone(),
                operation_index: op.operation_index,
                source_copy_id: op.source_copy_id.clone(),
                transferred_bytes: op.length,
                range_checksum: Vec::new(),
                range_checksum_algorithm: afs_protocol::node_data::DfsDigestAlgorithm::ReadUnchecked
                    as i32,
                crc32c_table: Default::default(),
            }),
            self.deadline,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_concurrency_processing_budget_is_bounded_and_validates_wire_units() {
        use std::time::{Duration, Instant};
        let cap = Duration::from_secs(2);
        for header in ["1H", "1M", "1S", "100m", "100u", "100n", "99999999H"] {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert("grpc-timeout", header.parse().unwrap());
            let before = Instant::now();
            let deadline = processing_deadline(&metadata, cap).unwrap();
            assert!(deadline <= Instant::now() + cap);
            assert!(deadline >= before);
        }
        for header in ["0n", "", "H", "-1S", "+1S", "1.0S", "123456789S", "1x"] {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert("grpc-timeout", header.parse().unwrap());
            assert!(processing_deadline(&metadata, cap).is_err(), "{header}");
        }
        let mut duplicate = tonic::metadata::MetadataMap::new();
        duplicate.append("grpc-timeout", "1S".parse().unwrap());
        duplicate.append("grpc-timeout", "2S".parse().unwrap());
        assert!(processing_deadline(&duplicate, cap).is_err());
    }

    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn read_concurrency_stream_error_survives_a_full_response_queue() {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(Ok(DfsReadRangesFrame { body: None }))
            .await
            .unwrap();
        let terminal = Arc::new(std::sync::Mutex::new(Some(Status::deadline_exceeded(
            "fixed read expired",
        ))));
        drop(sender);
        let mut stream = ReadResponseStream {
            receiver: tokio_stream::wrappers::ReceiverStream::new(receiver),
            terminal,
        };
        assert!(stream.next().await.unwrap().is_ok());
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::DeadlineExceeded
        );
        assert!(stream.next().await.is_none());
    }
}
