//! Backend-independent node authority cycle, shared by startup and periodic reports.
use crate::{journal::JournalSink, reconciliation::ReconciliationOutcome, Adxlet};
use adx_core::EnvironmentRecord;
use adx_protocol::{control as pb, status};
use adx_transport::rpc::RpcChannel;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};
use tonic::{Request, Status};

type Coordinator = pb::coordinator_service_client::CoordinatorServiceClient<RpcChannel>;

async fn report(
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    timeout: Duration,
    handshake_required: &mut bool,
) -> Result<(), Status> {
    registration.heartbeat_sequence = registration
        .heartbeat_sequence
        .checked_add(1)
        .ok_or_else(|| Status::internal("heartbeat sequence exhausted"))?;
    let mut request = Request::new(registration.clone());
    request.set_timeout(timeout);
    let result = match tokio::time::timeout(timeout, coordinator.register_node(request)).await {
        Ok(response) => response.map(|_| ()),
        Err(_) => Err(Status::deadline_exceeded("node report timed out")),
    };
    if result
        .as_ref()
        .is_err_and(|error| error.code() == tonic::Code::FailedPrecondition)
    {
        *handshake_required = true;
    }
    result
}

async fn inspect(
    coordinator: &mut Coordinator,
    registration: &pb::RegisterNodeRequest,
    timeout: Duration,
) -> Result<pb::InspectNodeResponse, Status> {
    let mut request = Request::new(pb::InspectNodeRequest {
        node_id: registration.node_id.clone(),
        session_id: registration.session_id.clone(),
    });
    request.set_timeout(timeout);
    Ok(
        tokio::time::timeout(timeout, coordinator.inspect_node(request))
            .await
            .map_err(|_| Status::deadline_exceeded("authoritative node catalog timed out"))??
            .into_inner(),
    )
}

/// Obtain a complete current catalog with admission closed, reconcile every
/// configured runtime, and publish readiness before the next reporting interval.
/// Any failure keeps new admission closed; an unavailable catalog is never empty.
/// Before receiving fresh authority, transport failure preserves known controllers'
/// existing degradation path. Partial reconciliation or fencing keeps lifecycle closed.
pub async fn reconcile_node(
    coordinator: &mut Coordinator,
    registration: &mut pb::RegisterNodeRequest,
    manager: &Adxlet,
    journal: Option<&JournalSink>,
    timeout: Duration,
    report_interval: Duration,
    capacity_valid_until: Instant,
) -> Result<(), Status> {
    let was_live = *manager.lifecycle_ready.read().await;
    // Admission can close after a failed cleanup/report. That does not make
    // already recovered controllers need startup recovery again.
    let periodic = manager.authority_initialized.load(Ordering::Acquire)
        && registration.heartbeat_sequence > 0;
    manager.authority_reconciling.store(true, Ordering::Release);
    if !periodic {
        manager.pause_lifecycle().await;
    }
    // Startup withdraws routability until recovery completes. A validated live
    // session only closes new allocation while refreshing its authority catalog.
    let handshake = !periodic || registration.reconciling;
    registration.reconciling = handshake;
    registration.accepting_allocations = false;
    if timeout.is_zero() || report_interval.is_zero() {
        return Err(Status::invalid_argument(
            "positive node reconciliation intervals required",
        ));
    }
    let mut catalog_received = false;
    let mut catalog_stale = false;
    let mut handshake_required = false;
    let result = async {
        report(coordinator, registration, timeout, &mut handshake_required).await?;
        let mut catalog = inspect(coordinator, registration, timeout).await?;
        catalog_received = true;
        if let Some(journal) = journal {
            let records = catalog
                .records
                .iter()
                .cloned()
                .map(TryInto::try_into)
                .collect::<adx_core::Result<Vec<EnvironmentRecord>>>()
                .map_err(status)?;
            journal.recover(&records).await.map_err(status)?;
            // Replay may change execution/checkpoint ownership. Recover both
            // from the same fresh complete Coordinator snapshot.
            catalog = inspect(coordinator, registration, timeout).await?;
        }
        let records = catalog
            .records
            .into_iter()
            .map(TryInto::try_into)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(status)?;
        let snapshots = catalog
            .snapshots
            .into_iter()
            .map(TryInto::try_into)
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(status)?;
        let retained = catalog
            .retained_checkpoints
            .into_iter()
            .map(|cp| adx_core::RestorePoint::try_from(cp).map(|cp| cp.artifact))
            .collect::<adx_core::Result<Vec<_>>>()
            .map_err(status)?;
        let recovery = async {
            if periodic {
                if manager.compare_retained(records, snapshots, retained).await.map_err(status)?
                    == ReconciliationOutcome::StaleCatalog
                {
                    catalog_stale = true;
                    return Err(Status::aborted("node authority catalog advanced"));
                }
                Ok(())
            } else {
                manager
                    .reconcile_retained(records, snapshots, retained)
                    .await.map_err(status)
            }
        };
        tokio::pin!(recovery);
        let mut heartbeat =
            tokio::time::interval_at(Instant::now() + report_interval, report_interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                result = &mut recovery => { result?; break; },
                _ = heartbeat.tick() => { report(coordinator, registration, timeout, &mut handshake_required).await?; },
            }
        }
        registration.reconciling = false;
        registration.accepting_allocations = manager.locally_accepting_allocations()
            && !manager.is_draining()
            && Instant::now() < capacity_valid_until;
        // Coordinator may dispatch while processing this report, before replying.
        // Authority is already validated, so accept those confirmed assignments.
        manager
            .authority_reconciling
            .store(false, Ordering::Release);
        report(coordinator, registration, timeout, &mut handshake_required).await
    }
    .await;
    if result.is_err() {
        manager.authority_reconciling.store(true, Ordering::Release);
        if !catalog_stale {
            manager.pause_lifecycle().await;
        }
        // Coordinator restart/ownership invalidation can require a fresh
        // registration handshake, independently of local controller recovery.
        registration.reconciling = handshake || handshake_required;
        if was_live
            && !catalog_received
            && result.as_ref().is_err_and(|error| {
                matches!(
                    error.code(),
                    tonic::Code::Unavailable
                        | tonic::Code::DeadlineExceeded
                        | tonic::Code::Cancelled
                )
            })
        {
            // Do not invent a catalog or inspect/delete unknown resources. Existing
            // controllers may still perform their previously authorized operations.
            *manager.lifecycle_ready.write().await = true;
            registration.reconciling = handshake;
        }
        registration.accepting_allocations = false;
    }
    result
}
