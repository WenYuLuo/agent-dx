use adx_agent_core::{environment::ExecutionSpec, TemplateVersion};
use serde_json::{json, Value};

fn template(lifecycle: Value) -> Value {
    json!({"name":"app","version":"1","image":"app:1","isolation_runtime":"runsc",
        "resources":{"cpu_millis":500,"memory_mib":1024},"lifecycle":lifecycle})
}

#[test]
fn lifecycle_is_typed_and_validated_before_binding_admission() {
    for policy in [
        json!({"idle_timeout_seconds":0,"idle_action":"pause"}),
        json!({"idle_timeout_seconds":u64::MAX}),
        json!({"pause":{"retention":{"kind":"ttl","ttl_seconds":0}}}),
        json!({"pause":{"timeout_seconds":0}}),
        json!({"pause":{"timeout_seconds":3601}}),
    ] {
        let parsed: TemplateVersion = serde_json::from_value(template(policy)).unwrap();
        assert!(parsed.validate().is_err());
        assert!(ExecutionSpec::from(&parsed).validate().is_err());
    }
    for policy in [
        json!({"idle_action":"suspend"}),
        json!({"idle_timeout_seconds":-1}),
        json!({"pause":{"storage":"unknown"}}),
        json!({"pause":{"retention":{"kind":"forever","ttl_seconds":60}}}),
    ] {
        assert!(serde_json::from_value::<TemplateVersion>(template(policy)).is_err());
    }
}

#[test]
fn pause_retention_is_independent_of_the_idle_window() {
    let parsed: TemplateVersion = serde_json::from_value(template(json!({
        "idle_timeout_seconds":600,"idle_action":"pause",
        "pause":{"retention":{"kind":"ttl","ttl_seconds":86400},"storage":"shared"}
    })))
    .unwrap();
    parsed.validate().unwrap();
    let execution = ExecutionSpec::from(&parsed);
    execution.validate().unwrap();
    let value = serde_json::to_value(execution).unwrap();
    assert_eq!(value["lifecycle"]["idle_timeout_seconds"], 600);
    assert_eq!(
        value["lifecycle"]["pause"]["retention"]["ttl_seconds"],
        86400
    );
    assert_eq!(value["lifecycle"]["pause"]["timeout_seconds"], 300);
}
