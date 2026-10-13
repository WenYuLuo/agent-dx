//! Constructor cwd initialization must complete before runtime readiness.
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct Runtime(Child);
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn start(cwd: &Path, log: &Path) -> (Runtime, u16) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let log = std::fs::File::create(log).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_adx-execd"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("EXECD_HTTP_ONLY", "1")
        .env("EXECD_HTTP_PORT", port.to_string())
        .env("EXECD_HTTP_TOKEN", "cwd-test")
        .env("ADX_ENVIRONMENT_ID", "cwd-test")
        .env("ADX_RUNTIME_ID", "cwd-test-1")
        .env("ADX_OWNERSHIP_GENERATION", "1")
        .env("ADX_SANDBOX_CWD", cwd)
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log))
        .spawn()
        .unwrap();
    (Runtime(child), port)
}
fn ready(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(
        &format!("127.0.0.1:{port}").parse().unwrap(),
        Duration::from_millis(50),
    ) else {
        return false;
    };
    stream
        .set_read_timeout(Some(Duration::from_millis(100)))
        .unwrap();
    stream.write_all(b"GET /control/v1/status HTTP/1.1\r\nHost: localhost\r\nX-Auth: cwd-test\r\nConnection: close\r\n\r\n").unwrap();
    let mut bytes = Vec::new();
    let _ = stream.read_to_end(&mut bytes);
    bytes.starts_with(b"HTTP/1.1 200 ")
}
#[test]
fn constructor_cwd_is_created_before_ready_and_existing_contents_survive() {
    let temp = tempfile::tempdir().unwrap();
    let cwd = temp.path().join("workspace/nested");
    let log = temp.path().join("execd.log");
    for existing in [false, true] {
        let (mut runtime, port) = start(&cwd, &log);
        let deadline = Instant::now() + Duration::from_secs(3);
        while !ready(port) {
            assert!(
                runtime.0.try_wait().unwrap().is_none(),
                "{}",
                std::fs::read_to_string(&log).unwrap()
            );
            assert!(Instant::now() < deadline, "Execd did not become ready");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            cwd.is_dir(),
            "Ready was published before constructor cwd existed"
        );
        if existing {
            assert_eq!(
                std::fs::read_to_string(cwd.join("marker")).unwrap(),
                "preserved"
            );
        } else {
            std::fs::write(cwd.join("marker"), "preserved").unwrap();
        }
    }
}
#[test]
fn invalid_constructor_cwd_exits_without_publishing_ready() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("file");
    std::fs::write(&file, "preserved").unwrap();
    let log = temp.path().join("execd.log");
    let (mut runtime, port) = start(&file.join("nested"), &log);
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        assert!(!ready(port), "invalid cwd must not publish Ready");
        if let Some(status) = runtime.0.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        assert!(Instant::now() < deadline, "invalid cwd must fail startup");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(std::fs::read_to_string(file).unwrap(), "preserved");
    assert!(std::fs::read_to_string(log)
        .unwrap()
        .contains("sandbox cwd"));
}
