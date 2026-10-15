use adx_agent_core::TemplateVersion;
use serde_json::json;

fn template(path: &str) -> TemplateVersion {
    serde_json::from_value(json!({"name":"app","version":"1","image":"app:1",
        "isolation_runtime":"runsc","resources":{"cpu_millis":1000,"memory_mib":512},
        "mounts":[{"type":"bind","host_path":path,"target":"/home/jiuwen","options":["rw"],
            "create_if_missing":true}]}))
    .unwrap()
}

#[test]
fn rendering_rejects_missing_parameters_and_path_escape() {
    use std::collections::BTreeMap;
    let t = template("/mnt/sfs/workspaces/${workspace}");
    assert!(t.render_mounts(&BTreeMap::new()).is_err());
    for value in [
        "", "/etc", "..", "a/../b", "a//b", "a/./b", "a/", "a\\b", "${other}", "a\n",
    ] {
        let parameters = BTreeMap::from([("workspace".into(), value.into())]);
        assert!(t.render_mounts(&parameters).is_err(), "{value:?}");
    }
    let parameters = BTreeMap::from([("workspace".into(), "tenant/user-1".into())]);
    let mounts = t.render_mounts(&parameters).unwrap();
    assert_eq!(mounts[0].host_path, "/mnt/sfs/workspaces/tenant/user-1");
    assert_eq!(mounts[0].target, "/home/jiuwen");
    assert_eq!(mounts[0].options, ["rw"]);
    assert!(mounts[0].create_if_missing);
}

#[test]
fn malformed_templates_and_unresolved_execution_are_rejected() {
    for path in [
        "relative/${workspace}",
        "/${workspace}",
        "/mnt/${}",
        "/mnt/${workspace",
        "/mnt/$workspace",
        "/mnt/${a.b}",
        "/mnt/../data/${workspace}",
    ] {
        assert!(template(path).validate().is_err(), "{path}");
    }
    let t = template("/mnt/sfs/${workspace}");
    assert!(adx_agent_core::environment::ExecutionSpec::from(&t)
        .validate()
        .is_err());
    let mut t = template("/mnt/sfs/user");
    t.mounts.push(t.mounts[0].clone());
    assert!(t.validate().is_err());
}

#[test]
fn mount_options_and_rendered_size_are_bounded() {
    let mut t = template("/mnt/sfs/${workspace}");
    for options in [vec!["rw", "ro"], vec!["rw", "rw"], vec!["bind"]] {
        t.mounts[0].options = options.into_iter().map(str::to_owned).collect();
        assert!(t.validate().is_err());
    }
    t.mounts[0].options = vec!["rw".into(), "nodev".into()];
    t.validate().unwrap();
    let parameters = std::collections::BTreeMap::from([("workspace".into(), "a".repeat(4096))]);
    assert!(t.render_mounts(&parameters).is_err());
    let t = template("/mnt/sfs/user-${workspace}/data");
    let parameters = std::collections::BTreeMap::from([("workspace".into(), "abc".into())]);
    assert_eq!(
        t.render_mounts(&parameters).unwrap()[0].host_path,
        "/mnt/sfs/user-abc/data"
    );
}
