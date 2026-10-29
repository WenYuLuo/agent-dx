use adx_core::CheckpointArtifact;

#[test]
fn legacy_artifact_preserves_its_serialized_bytes_until_scoped() {
    let legacy = r#"{"storage":"local","location":"/checkpoint","size_bytes":1}"#;
    let mut artifact: CheckpointArtifact = serde_json::from_str(legacy).unwrap();
    assert!(artifact.scope.is_empty());
    assert_eq!(serde_json::to_string(&artifact).unwrap(), legacy);

    artifact.scope = "shared-store".into();
    let scoped = serde_json::to_string(&artifact).unwrap();
    assert_eq!(
        scoped,
        r#"{"storage":"local","scope":"shared-store","location":"/checkpoint","size_bytes":1}"#
    );
    assert_eq!(
        serde_json::from_str::<CheckpointArtifact>(&scoped).unwrap(),
        artifact
    );
}
