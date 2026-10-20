//! Private protocol faultpoints: compiled into unit-test executables only.
//! Real child processes are SIGKILLed; this is not physical power-loss evidence.
use super::{
    MetaRead, MetaStore, NodeSessionLease, OperationResult, RequestKey, RequestOutcome, Store,
    StoreRevision, TxnOutcome, WatchBatch,
    journal::{LogLimits, MetaLog},
    local_log::LocalLog,
};
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};

const CHILD: &str = "AFS_P3_PRIVATE_CRASH_CHILD";
const POINT: &str = "AFS_P3_PRIVATE_CRASH_POINT";
const DIRECTORY: &str = "AFS_P3_PRIVATE_CRASH_DIRECTORY";
const TEST: &str = "meta::store::crash_tests::p3_process_crash_matrix";
const CHECKPOINT_TEST: &str = "meta::store::crash_tests::p3_checkpoint_process_crash_matrix";

pub(super) fn active(point: &str) -> bool {
    std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1"))
        && std::env::var(POINT).ok().as_deref() == Some(point)
        && std::env::var_os(DIRECTORY).is_some_and(|dir| PathBuf::from(dir).join("armed").is_file())
}

pub(super) fn pause(point: &str, detail: &str) {
    if !active(point) {
        return;
    }
    let directory = PathBuf::from(std::env::var_os(DIRECTORY).unwrap());
    fs::write(directory.join("reached.tmp"), format!("{point}: {detail}")).unwrap();
    fs::rename(directory.join("reached.tmp"), directory.join("reached")).unwrap();
    // Only the supervising test may end this deliberately paused child.
    loop {
        std::thread::park_timeout(Duration::from_secs(1));
    }
}

#[derive(Deserialize, Serialize)]
struct Witness {
    original: RequestOutcome,
    identity: Vec<u8>,
    wal_bytes: u64,
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
fn lease(session: &str) -> NodeSessionLease {
    NodeSessionLease {
        node_id: "node-a".into(),
        session_id: session.into(),
        grpc_addr: "grpc".into(),
        data_addr: "data".into(),
        rest_addr: "rest".into(),
        storage_devices: Vec::new(),
        lease_ttl: Duration::from_secs(60),
    }
}
async fn child(directory: &Path) {
    let root = directory.join("meta");
    let store = Store::open_log(Arc::new(
        LocalLog::open(&root, LogLimits::default()).unwrap(),
    ))
    .await
    .unwrap();
    let seed = RequestKey::new("node-a", "seed");
    assert!(matches!(
        store
            .register_node_session(seed.clone(), lease("seed"))
            .await
            .unwrap(),
        TxnOutcome::Committed { .. }
    ));
    let witness = Witness {
        original: store
            .read(MetaRead::RequestOutcome(seed))
            .await
            .unwrap()
            .request_outcome
            .unwrap(),
        identity: fs::read(root.join("IDENTITY")).unwrap(),
        wal_bytes: fs::metadata(root.join("wal-00000000000000000001.log"))
            .unwrap()
            .len(),
    };
    fs::write(
        directory.join("ack.json"),
        serde_json::to_vec(&witness).unwrap(),
    )
    .unwrap();
    fs::write(directory.join("armed"), b"seed ACK complete").unwrap();
    // A current-thread runtime enqueues both requests before the commit task
    // can collect them, so the paused append really contains a two-record group.
    let (one, two) = tokio::join!(
        store.register_node_session(RequestKey::new("node-a", "one"), lease("one")),
        store.register_node_session(RequestKey::new("node-a", "two"), lease("two")),
    );
    fs::write(directory.join("unexpected-ack"), format!("{one:?} {two:?}")).unwrap();
    panic!("selected faultpoint was not reached");
}

#[tokio::test(flavor = "current_thread")]
async fn p3_process_crash_matrix() {
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        child(&PathBuf::from(std::env::var_os(DIRECTORY).unwrap())).await;
        return;
    }
    for point in [
        "partial_second",
        "after_append_before_sync",
        "after_sync_before_publish",
    ] {
        // Preserve the directory on any assertion failure, including the child's
        // log and exact WAL; only a successful scenario removes its own fixture.
        let directory = tempfile::tempdir().unwrap().keep();
        eprintln!("P3 crash case={point} evidence={}", directory.display());
        let child_log = File::create(directory.join("child.log")).unwrap();
        let mut process = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", TEST, "--nocapture", "--test-threads=1"])
                .env(CHILD, "1")
                .env(POINT, point)
                .env(DIRECTORY, &directory)
                .stdout(Stdio::from(child_log.try_clone().unwrap()))
                .stderr(Stdio::from(child_log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !directory.join("reached").is_file() {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "child exited before {point}: {}",
                fs::read_to_string(directory.join("child.log")).unwrap()
            );
            assert!(
                Instant::now() < deadline,
                "child failed to reach {point}; evidence {}",
                directory.display()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let reached = fs::read_to_string(directory.join("reached")).unwrap();
        assert!(reached.starts_with(point));
        if point == "after_sync_before_publish" {
            assert!(reached.contains("visible=1 staged=3"), "{reached}");
        }
        assert!(!directory.join("unexpected-ack").exists());
        process.0.kill().unwrap();
        let status = process.0.wait().unwrap();
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        let witness: Witness =
            serde_json::from_slice(&fs::read(directory.join("ack.json")).unwrap()).unwrap();
        let root = directory.join("meta");
        let wal = root.join("wal-00000000000000000001.log");
        let before = fs::read(&wal).unwrap();
        let log = Arc::new(LocalLog::open(&root, LogLimits::default()).unwrap());
        let store = Store::open_log(log.clone()).await.unwrap();
        let count = if point == "partial_second" { 2 } else { 3 };
        assert_eq!(log.recover().await.unwrap().tip.sequence, count);
        let current = store
            .read(MetaRead::CurrentNodeSession {
                node_id: "node-a".into(),
            })
            .await
            .unwrap();
        assert_eq!(current.revision, StoreRevision(count));
        let Some(super::MetaEntity::NodeSession(session)) = current.entity else {
            panic!("missing recovered session");
        };
        assert_eq!(session.lease_epoch, count);
        assert_eq!(session.session_id, if count == 2 { "one" } else { "two" });
        assert_eq!(
            store
                .read(MetaRead::RequestOutcome(RequestKey::new("node-a", "seed")))
                .await
                .unwrap()
                .request_outcome,
            Some(witness.original)
        );
        for request in ["seed", "one"] {
            let key = RequestKey::new("node-a", request);
            let original = store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .unwrap()
                .request_outcome
                .unwrap();
            let TxnOutcome::ConditionFailed {
                existing_outcome: Some(replayed),
                ..
            } = store
                .register_node_session(key, lease("must-not-replace"))
                .await
                .unwrap()
            else {
                panic!("retry applied twice");
            };
            assert_eq!(replayed, original);
        }
        if count == 3 {
            let key = RequestKey::new("node-a", "two");
            let original = store
                .read(MetaRead::RequestOutcome(key.clone()))
                .await
                .unwrap()
                .request_outcome
                .unwrap();
            assert!(matches!(&original.result, OperationResult::NodeSession(_)));
            let TxnOutcome::ConditionFailed {
                existing_outcome: Some(replayed),
                ..
            } = store
                .register_node_session(key, lease("must-not-replace"))
                .await
                .unwrap()
            else {
                panic!("second unknown result applied twice");
            };
            assert_eq!(replayed, original);
        } else {
            assert!(
                store
                    .read(MetaRead::RequestOutcome(RequestKey::new("node-a", "two")))
                    .await
                    .unwrap()
                    .request_outcome
                    .is_none()
            );
        }
        let after = fs::read(&wal).unwrap();
        assert!(after.len() as u64 > witness.wal_bytes);
        if count == 2 {
            assert!(after.len() < before.len());
            let saved = fs::read_dir(&root)
                .unwrap()
                .map(|e| e.unwrap().path())
                .find(|p| {
                    p.file_name()
                        .unwrap()
                        .to_string_lossy()
                        .starts_with("incomplete-tail-")
                })
                .unwrap();
            assert_eq!(fs::read(saved).unwrap(), before[after.len()..]);
        } else {
            assert_eq!(after, before);
        }
        assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), witness.identity);
        let watch = store.watch(StoreRevision::ZERO, 100).await.unwrap();
        watch.validate_order().unwrap();
        let WatchBatch::Events { events, .. } = watch else {
            panic!("unexpected compaction");
        };
        assert!(events.iter().all(|event| event.revision.0 <= count));
        assert!(events.iter().any(|event| event.revision.0 == count));
        assert_eq!(fs::read(&wal).unwrap(), after, "retries cannot append");
        eprintln!(
            "P3 crash PASS case={point} signal={} seq={count} epoch={} recovered_bytes={} retained_tail_bytes={}",
            libc::SIGKILL,
            session.lease_epoch,
            after.len(),
            before.len() - after.len()
        );
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(log);
        drop(process);
        fs::remove_dir_all(directory).unwrap();
    }
}

fn checkpoint_log_limits() -> LogLimits {
    LogLimits {
        record_max_bytes: 1024,
        batch_max_bytes: 4096,
        wal_max_bytes: 128 * 1024,
        segment_bytes: 4608,
        segments_max: 32,
    }
}

fn checkpoint_image_limits() -> super::journal::CheckpointLimits {
    super::journal::CheckpointLimits {
        image_max_bytes: 64 * 1024,
        capture_charge_max_bytes: 256 * 1024,
        disk_max_bytes: 512 * 1024,
        files_max: 8,
    }
}

#[derive(Deserialize, Serialize)]
struct CheckpointWitness {
    tip: super::journal::LogTip,
    identity: Vec<u8>,
    state: Vec<u8>,
}

async fn checkpoint_child(directory: &Path) {
    let root = directory.join("meta");
    let log = LocalLog::open_with_checkpoint_limits(
        &root,
        checkpoint_log_limits(),
        checkpoint_image_limits(),
    )
    .unwrap();
    let mut state = super::state::State::default();
    let mut tip = super::journal::LogTip::EMPTY;
    let mut records = super::journal::test_records(13).into_iter();
    for number in 1..=12 {
        let record = records.next().unwrap();
        state = state.replay_record(&record).unwrap();
        tip = log.append_and_sync(tip, vec![record]).await.unwrap();
        if number == 4 || number == 8 {
            log.install_checkpoint(
                tip,
                super::log::StateImage::encode(&state, checkpoint_image_limits().image_max_bytes)
                    .unwrap(),
            )
            .await
            .unwrap();
        }
    }
    let witness = CheckpointWitness {
        tip,
        identity: fs::read(root.join("IDENTITY")).unwrap(),
        state: state.snapshot_bytes_with_batch(0).unwrap(),
    };
    fs::write(
        directory.join("ack.json"),
        serde_json::to_vec(&witness).unwrap(),
    )
    .unwrap();
    fs::write(
        directory.join("armed"),
        b"twelve durable decisions and two checkpoints",
    )
    .unwrap();
    if std::env::var(POINT).unwrap().starts_with("rotation_") {
        log.append_and_sync(tip, vec![records.next().unwrap()])
            .await
            .unwrap();
    } else {
        log.install_checkpoint(
            tip,
            super::log::StateImage::encode(&state, checkpoint_image_limits().image_max_bytes)
                .unwrap(),
        )
        .await
        .unwrap();
    }
    panic!("selected checkpoint/rotation faultpoint was not reached");
}

fn disk_accounting(root: &Path) -> (u64, u64, usize) {
    use std::os::unix::fs::MetadataExt;
    let mut bytes = 0;
    let mut blocks = 0;
    let mut files = 0;
    for entry in fs::read_dir(root).unwrap() {
        let metadata = entry.unwrap().metadata().unwrap();
        bytes += metadata.len();
        blocks += metadata.blocks() * 512;
        files += 1;
    }
    (bytes, blocks, files)
}

#[tokio::test(flavor = "current_thread")]
async fn p3_checkpoint_process_crash_matrix() {
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("1")) {
        checkpoint_child(&PathBuf::from(std::env::var_os(DIRECTORY).unwrap())).await;
        return;
    }
    for point in [
        "checkpoint_partial",
        "checkpoint_image_installed",
        "checkpoint_selected",
        "checkpoint_during_delete",
        "rotation_prepared",
        "rotation_selected",
    ] {
        let directory = tempfile::tempdir().unwrap().keep();
        eprintln!(
            "P3 checkpoint crash case={point} evidence={}",
            directory.display()
        );
        let child_log = File::create(directory.join("child.log")).unwrap();
        let mut process = ChildGuard(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    CHECKPOINT_TEST,
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(CHILD, "1")
                .env(POINT, point)
                .env(DIRECTORY, &directory)
                .stdout(Stdio::from(child_log.try_clone().unwrap()))
                .stderr(Stdio::from(child_log))
                .spawn()
                .unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(15);
        while !directory.join("reached").is_file() {
            assert!(
                process.0.try_wait().unwrap().is_none(),
                "child exited before {point}: {}",
                fs::read_to_string(directory.join("child.log")).unwrap()
            );
            assert!(
                Instant::now() < deadline,
                "child failed to reach {point}; evidence {}",
                directory.display()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!directory.join("unexpected-ack").exists());
        let root = directory.join("meta");
        let before = disk_accounting(&root);
        // Small deterministic fixture budget, not a production peak/RSS claim.
        let disk_bound =
            checkpoint_log_limits().wal_max_bytes + checkpoint_image_limits().disk_max_bytes + 8192;
        assert!(before.0 <= disk_bound && before.1 <= disk_bound);
        assert!(
            before.2
                <= checkpoint_log_limits().segments_max + checkpoint_image_limits().files_max + 2
        );
        process.0.kill().unwrap();
        assert_eq!(process.0.wait().unwrap().signal(), Some(libc::SIGKILL));
        let witness: CheckpointWitness =
            serde_json::from_slice(&fs::read(directory.join("ack.json")).unwrap()).unwrap();
        let partial = root.join("checkpoint.tmp");
        let partial_bytes = if point == "checkpoint_partial" {
            Some(fs::read(&partial).unwrap())
        } else {
            None
        };
        let started = Instant::now();
        let log = Arc::new(
            LocalLog::open_with_checkpoint_limits(
                &root,
                checkpoint_log_limits(),
                checkpoint_image_limits(),
            )
            .unwrap(),
        );
        let recovered = log.recover().await.unwrap();
        assert_eq!(
            recovered.tip, witness.tip,
            "no interrupted operation was appended"
        );
        let selected = recovered.checkpoint.unwrap();
        let expected = if matches!(point, "checkpoint_selected" | "checkpoint_during_delete") {
            12
        } else {
            8
        };
        assert_eq!(selected.covered.sequence, expected);
        let mut restored = super::log::StateImage::decode(
            &selected.bytes,
            checkpoint_image_limits().image_max_bytes,
        )
        .unwrap();
        for record in recovered.records {
            restored = restored.replay_record(&record).unwrap();
        }
        assert_eq!(
            restored.snapshot_bytes_with_batch(0).unwrap(),
            witness.state
        );
        assert_eq!(fs::read(root.join("IDENTITY")).unwrap(), witness.identity);
        let store = Store::open_log(log.clone()).await.unwrap();
        let original = store
            .read(MetaRead::RequestOutcome(RequestKey::new(
                "log-test",
                "request-0",
            )))
            .await
            .unwrap()
            .request_outcome
            .unwrap();
        let TxnOutcome::ConditionFailed {
            existing_outcome: Some(replayed),
            ..
        } = store
            .register_node_session(
                RequestKey::new("log-test", "request-0"),
                lease("must-not-replace"),
            )
            .await
            .unwrap()
        else {
            panic!("ACK retry applied twice")
        };
        assert_eq!(original, replayed);
        assert_eq!(log.recover().await.unwrap().tip, witness.tip);
        drop(store);
        tokio::time::timeout(Duration::from_secs(5), async {
            while Arc::strong_count(&log) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The exact empty prepared segment must be reusable; a partial image
        // must remain intact, and later installation may quarantine it safely.
        if let Some(bytes) = &partial_bytes {
            assert_eq!(&fs::read(&partial).unwrap(), bytes);
        }
        let record = super::journal::test_records(13).pop().unwrap();
        restored = restored.replay_record(&record).unwrap();
        let tip = log
            .append_and_sync(witness.tip, vec![record])
            .await
            .unwrap();
        log.install_checkpoint(
            tip,
            super::log::StateImage::encode(&restored, checkpoint_image_limits().image_max_bytes)
                .unwrap(),
        )
        .await
        .unwrap();
        if let Some(bytes) = partial_bytes {
            assert!(!partial.exists());
            assert!(fs::read_dir(&root).unwrap().any(|entry| {
                let path = entry.unwrap().path();
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("orphan-checkpoint.tmp-")
                    && fs::read(path).unwrap() == bytes
            }));
        }
        let after = disk_accounting(&root);
        assert!(after.0 <= disk_bound && after.1 <= disk_bound);
        eprintln!(
            "P3 checkpoint PASS case={point} signal={} selected={} ACK=12 final={} bytes_before={} blocks_before={} files_before={} bytes_after={} blocks_after={} files_after={} recovery_and_followup_us={}",
            libc::SIGKILL,
            expected,
            tip.sequence,
            before.0,
            before.1,
            before.2,
            after.0,
            after.1,
            after.2,
            started.elapsed().as_micros()
        );
        drop(log);
        drop(process);
        fs::remove_dir_all(directory).unwrap();
    }
}
