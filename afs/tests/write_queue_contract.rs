use afs::config::{Cli, Config, Role};
use clap::Parser;

#[test]
fn front_write_queue_default_and_cli_override_preserve_back_end_limits() {
    let default =
        Config::resolve(Role::Node, Cli::parse_from(["afs-node", "--fs", "dfs"])).unwrap();
    let json = serde_json::to_value(&default).unwrap();
    assert_eq!(json["dfs_write_queue_bytes"], 2147483648_u64);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    std::fs::write(&path, "fs='dfs'\ndfs_write_queue_bytes=67108864\n").unwrap();
    let file_only = Config::resolve(
        Role::Node,
        Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()]),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&file_only).unwrap()["dfs_write_queue_bytes"],
        67108864_u64
    );
    let config = Config::resolve(
        Role::Node,
        Cli::parse_from([
            "afs-node",
            "--config",
            path.to_str().unwrap(),
            "--dfs-write-queue-bytes",
            "268435456",
        ]),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_value(&config).unwrap()["dfs_write_queue_bytes"],
        268435456_u64
    );
    assert_eq!(
        config.dfs_dirty_budget_bytes,
        default.dfs_dirty_budget_bytes
    );
    assert_eq!(
        config.dfs_write_buffer_idle_bytes,
        default.dfs_write_buffer_idle_bytes
    );
}

#[test]
fn front_queue_invalid_limits_are_not_silently_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node.toml");
    for bytes in [0_u64, 8388607, 1099511627777, u64::MAX] {
        std::fs::write(&path, format!("fs='dfs'\ndfs_write_queue_bytes={bytes}\n")).unwrap();
        assert!(
            Config::resolve(
                Role::Node,
                Cli::parse_from(["afs-node", "--config", path.to_str().unwrap()])
            )
            .is_err()
        );
    }
}

#[test]
fn front_queue_valid_endpoints_do_not_allocate_the_payload_budget() {
    for bytes in [8388608_u64, 1099511627776] {
        let value = bytes.to_string();
        let config = Config::resolve(
            Role::Node,
            Cli::parse_from(["afs-node", "--fs", "dfs", "--dfs-write-queue-bytes", &value]),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(config).unwrap()["dfs_write_queue_bytes"],
            bytes
        );
    }
}
