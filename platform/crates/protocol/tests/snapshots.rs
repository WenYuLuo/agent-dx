use adx_core::{
    snapshots::{Reference, Snapshot},
    CheckpointArtifact, EnvironmentSpec, Resources,
};
fn snapshot() -> Snapshot {
    let spec = EnvironmentSpec {
        envd: None,
        run_policy: None,
        compatibility: None,
        runtime_profile: None,
        snapshot_id: None,
        id: "source".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "firecracker".into(),
        resources: Resources {
            cpu_millis: 100,
            memory_bytes: 128,
            disk_bytes: 0,
        },
        priority: 0,
        scheduling: Default::default(),
        env: Default::default(),
        lifecycle: Default::default(),
        execution: Default::default(),
    };
    Snapshot::new(
        "snapshot-1".into(),
        vec!["base".into()],
        spec,
        "node1".into(),
        "source-1".into(),
        CheckpointArtifact {
            storage: "shared".into(),
            scope: String::new(),
            location: "artifact".into(),
            size_bytes: 512,
        },
    )
    .unwrap()
}

#[test]
fn snapshot_wire_preserves_template_and_durable_references() {
    let mut record = snapshot();
    record
        .acquire(
            "tenant",
            Reference::Template {
                node_id: "node1".into(),
                template_id: "base".into(),
            },
        )
        .unwrap();
    assert!(record.delete("tenant").is_err());
    let wire: adx_protocol::control::ReusableSnapshot = record.clone().try_into().unwrap();
    assert_eq!(Snapshot::try_from(wire).unwrap(), record);
}

#[test]
fn snapshot_wire_preserves_golden_kind_and_capture_time_and_rejects_unknown_kind() {
    use adx_core::snapshots::SnapshotKind;
    use adx_protocol::control as pb;
    let mut saved = snapshot();
    saved.kind = SnapshotKind::Golden;
    saved.created_at_unix_ms = 1_730_000_000_123;
    let wire: pb::ReusableSnapshot = saved.clone().try_into().unwrap();
    assert_eq!(wire.kind, pb::SnapshotKind::Golden as i32);
    assert_eq!(wire.created_at_unix_ms, saved.created_at_unix_ms);
    assert_eq!(Snapshot::try_from(wire.clone()).unwrap(), saved);
    let mut invalid = wire.clone();
    invalid.kind = 42;
    assert!(Snapshot::try_from(invalid).is_err());
    let mut legacy = wire;
    legacy.kind = 0;
    legacy.created_at_unix_ms = 0;
    let legacy = Snapshot::try_from(legacy).unwrap();
    assert_eq!(legacy.kind, SnapshotKind::Regular);
    assert_eq!(legacy.created_at_unix_ms, 0);
}
