use super::*;

#[tokio::test]
#[ignore = "requires isolated Redis and generated mTLS certificates"]
async fn preparation_rpc_keeps_registration_distinct_from_ready_and_tenant_scoped() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let rpc = CoordinatorRpc::new(
        session.clone(),
        Placement::Pack,
        peers(),
        client_tls("coordinator"),
        Duration::from_secs(2),
    )
    .await
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _servers = Servers(vec![tokio::spawn(async move {
        Server::builder()
            .tls_config(server_tls("coordinator"))
            .unwrap()
            .add_service(
                pb::snapshot_preparation_service_server::SnapshotPreparationServiceServer::new(rpc),
            )
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    })]);
    let mut client = pb::snapshot_preparation_service_client::SnapshotPreparationServiceClient::new(
        channel(address, "apiserver").await,
    );
    let input = pb::RegisterSnapshotPreparationRequest {
        caller: caller(),
        name: "image".into(),
        resources: Some(pb::Resources {
            cpu_millis: 1000,
            memory_bytes: 1024 * 1024 * 1024,
            disk_bytes: 0,
        }),
    };
    let first = client
        .register_snapshot_preparation(input.clone())
        .await
        .unwrap()
        .into_inner();
    let busy = client
        .register_snapshot_preparation(input.clone())
        .await
        .unwrap_err();
    assert!(busy.message().contains("busy"));
    let bare = pb::ResolveSnapshotTemplateRequest {
        caller: caller(),
        reference: "image".into(),
        registration_only: true,
    };
    let pending = client
        .resolve_snapshot_template(bare.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(pending.template_id, first.template_id);
    assert_eq!(
        pending.phase,
        pb::SnapshotPreparationPhase::Registered as i32
    );
    assert!(pending.snapshot_id.is_empty());
    let mut create = bare.clone();
    create.registration_only = false;
    assert_eq!(
        client
            .resolve_snapshot_template(create)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let mut foreign = bare;
    foreign.caller.as_mut().unwrap().tenant_id = "other".into();
    assert_eq!(
        client
            .resolve_snapshot_template(foreign)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    session
        .update_snapshot_template(&first.template_id, |template| {
            let build = &mut template.preparation;
            build.state = adx_core::snapshot_preparation::PreparationState::Failed;
            build.error = Some("failed input".into());
            Ok(())
        })
        .await
        .unwrap();
    let failed = client
        .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
            caller: caller(),
            template_id: first.template_id.clone(),
            preparation_id: first.preparation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(failed.phase, pb::SnapshotPreparationPhase::Failed as i32);
    session
        .update_snapshot_template(&first.template_id, |template| {
            let build = &mut template.preparation;
            build.state = adx_core::snapshot_preparation::PreparationState::Ready;
            build.snapshot_id = Some("snapshot-missing".into());
            build.cleanup_pending = false;
            Ok(())
        })
        .await
        .unwrap();
    let unavailable = client
        .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
            caller: caller(),
            reference: "image".into(),
            registration_only: false,
        })
        .await
        .unwrap_err();
    assert_eq!(unavailable.code(), tonic::Code::FailedPrecondition);
    // Usage restriction is caller policy. Unlinking keeps the independent Golden.
    let golden = adx_core::snapshots::Snapshot::new_with_kind(
        adx_core::snapshots::SnapshotKind::Golden,
        "snapshot-missing".into(),
        vec!["image".into()],
        spec("source"),
        "node".into(),
        "source-1".into(),
        adx_core::CheckpointArtifact {
            storage: "shared".into(),
            scope: String::new(),
            location: "artifact".into(),
            size_bytes: 1,
        },
    )
    .unwrap();
    session.publish_snapshot(golden).await.unwrap();
    let second = client
        .register_snapshot_preparation(input.clone())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.template_id, first.template_id);
    assert_ne!(second.preparation_id, first.preparation_id);
    let latest_successful = client
        .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
            caller: caller(),
            template_id: first.template_id.clone(),
            preparation_id: first.preparation_id.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        latest_successful.phase,
        pb::SnapshotPreparationPhase::Ready as i32
    );
    assert_eq!(
        latest_successful.resources.as_ref().unwrap().cpu_millis,
        spec("source").resources.cpu_millis
    );
    session
        .update_snapshot_template(&first.template_id, |template| {
            template.preparation.state = adx_core::snapshot_preparation::PreparationState::Failed;
            Ok(())
        })
        .await
        .unwrap();
    let third = client
        .register_snapshot_preparation(input)
        .await
        .unwrap()
        .into_inner();
    let selected = client
        .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
            caller: caller(),
            reference: "image".into(),
            registration_only: false,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(selected.preparation_id, first.preparation_id);
    assert_eq!(selected.snapshot_id, "snapshot-missing");
    assert_eq!(
        client
            .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
                caller: caller(),
                template_id: first.template_id.clone(),
                preparation_id: second.preparation_id,
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .get_snapshot_preparation(pb::GetSnapshotPreparationRequest {
                caller: caller(),
                template_id: first.template_id.clone(),
                preparation_id: third.preparation_id,
            })
            .await
            .unwrap()
            .into_inner()
            .phase,
        pb::SnapshotPreparationPhase::Registered as i32
    );

    session
        .acquire_snapshot(
            "snapshot-missing",
            "tenant",
            adx_core::snapshots::Reference::Template {
                node_id: "node".into(),
                template_id: "active-clone".into(),
            },
        )
        .await
        .unwrap();
    let mut delete = pb::DeleteSnapshotTemplateRequest {
        caller: caller(),
        template_id: first.template_id,
        require_unused: true,
    };
    assert_eq!(
        client
            .delete_snapshot_template(delete.clone())
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    delete.require_unused = false;
    client
        .delete_snapshot_template(delete.clone())
        .await
        .unwrap();
    let retained = session.get_snapshot("snapshot-missing").await.unwrap();
    assert_eq!(retained.kind, adx_core::snapshots::SnapshotKind::Golden);
    assert_eq!(retained.references.len(), 1);
    assert_eq!(
        client
            .delete_snapshot_template(delete)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    assert_eq!(
        client
            .resolve_snapshot_template(pb::ResolveSnapshotTemplateRequest {
                caller: caller(),
                reference: "image".into(),
                registration_only: true,
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
}
