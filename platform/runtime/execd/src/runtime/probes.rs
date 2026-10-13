//! Internal checks bypass user activity accounting and command registries.
use super::{control::Controller, entrypoint};
use adx_core::{
    probes::*,
    runtime::{RuntimeIdentity, RuntimePhase},
    Error, Result,
};
use std::{os::unix::process::CommandExt, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    process::Command,
};

pub(crate) fn configuration() -> Result<Option<ProbeConfig>> {
    let Some(raw) = std::env::var_os(CONFIG_ENV) else {
        return Ok(None);
    };
    std::env::remove_var(CONFIG_ENV);
    let raw = raw
        .to_str()
        .ok_or_else(|| Error::Invalid("probe config is not UTF-8".into()))?;
    if raw.len() > 32768 {
        return Err(Error::Invalid("probe config exceeds 32 KiB".into()));
    }
    let config: ProbeConfig = serde_json::from_str(raw)
        .map_err(|e| Error::Invalid(format!("invalid probe config: {e}")))?;
    config.validate()?;
    Ok(Some(config))
}

pub(crate) fn spawn(controller: Arc<Controller>, config: ProbeConfig) {
    tokio::spawn(async move {
        if let Err(error) = supervise(controller, config).await {
            execd_error!("[execd-probes] runner stopped: {error}");
        }
    });
}

async fn phase_change(
    controller: &Controller,
    identity: &RuntimeIdentity,
    epoch: u64,
    phase: RuntimePhase,
) {
    let mut changes = controller.subscribe();
    loop {
        let Ok((status, current_epoch)) = controller.probe_status() else {
            return;
        };
        if status.identity != *identity || current_epoch != epoch || status.phase != phase {
            return;
        }
        if changes.changed().await.is_err() {
            return;
        }
    }
}
async fn delay(
    controller: &Controller,
    identity: &RuntimeIdentity,
    epoch: u64,
    phase: RuntimePhase,
    seconds: u64,
) -> bool {
    tokio::select! { _ = tokio::time::sleep(Duration::from_secs(seconds)) => true, _ = phase_change(controller, identity, epoch, phase) => false }
}
fn failure(message: impl Into<String>) -> ProbeFailure {
    let mut message = message.into();
    if message.len() > MAX_FAILURE_BYTES {
        let mut end = MAX_FAILURE_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
    }
    ProbeFailure {
        kind: ProbeFailureKind::HealthcheckFailed,
        message,
        consecutive_failures: 0,
        exit_code: None,
        http_status: None,
    }
}
async fn attempt(action: &ProbeAction) -> std::result::Result<(), ProbeFailure> {
    match action {
        ProbeAction::TcpSocket { port } => {
            TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, *port))
                .await
                .map_err(|e| failure(format!("TCP probe: {e}")))?;
            Ok(())
        }
        ProbeAction::HttpGet { port, path } => {
            let mut stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, *port))
                .await
                .map_err(|e| failure(format!("HTTP probe connect: {e}")))?;
            stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n").as_bytes()).await.map_err(|e| failure(format!("HTTP probe write: {e}")))?;
            let mut line = String::new();
            BufReader::new(stream.take(4096))
                .read_line(&mut line)
                .await
                .map_err(|e| failure(format!("HTTP probe response: {e}")))?;
            let mut fields = line.split_whitespace();
            let protocol = fields.next();
            let status = fields
                .next()
                .and_then(|s| s.parse::<u16>().ok())
                .filter(|s| (100..600).contains(s));
            if !matches!(protocol, Some("HTTP/1.0" | "HTTP/1.1")) || status.is_none() {
                return Err(failure("invalid HTTP probe response"));
            }
            let status = status.ok_or_else(|| failure("missing HTTP probe status"))?;
            if (200..300).contains(&status) {
                Ok(())
            } else {
                let mut result = failure(format!("HTTP probe returned {status}"));
                result.http_status = Some(status);
                Err(result)
            }
        }
        ProbeAction::Exec { command } => {
            exec_attempt(entrypoint::probe_command(command).map_err(failure)?).await
        }
    }
}

struct ProcessGroup(i32);
impl Drop for ProcessGroup {
    fn drop(&mut self) {
        // SAFETY: the positive ID belongs to the private process group created for this
        // probe. A negative PID targets that group, including probe descendants.
        unsafe {
            libc::kill(-self.0, libc::SIGKILL);
        }
    }
}
async fn tail(mut stream: impl AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut result = Vec::new();
    let mut chunk = [0; 1024];
    loop {
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Ok(result);
        }
        result.extend_from_slice(&chunk[..count]);
        if result.len() > MAX_FAILURE_BYTES {
            result.drain(..result.len() - MAX_FAILURE_BYTES);
        }
    }
}
async fn exec_attempt(mut command: Command) -> std::result::Result<(), ProbeFailure> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = command
        .spawn()
        .map_err(|e| failure(format!("exec probe spawn: {e}")))?;
    let id = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .ok_or_else(|| failure("invalid exec probe PID"))?;
    let group = ProcessGroup(id);
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| failure("exec probe stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| failure("exec probe stderr unavailable"))?;
    let capture = async { tokio::join!(tail(stdout), tail(stderr)) };
    let status = async {
        let status = child.wait().await;
        drop(group);
        status
    };
    let (status, (stdout, stderr)) = tokio::join!(status, capture);
    let status = status.map_err(|e| failure(format!("exec probe wait: {e}")))?;
    if status.success() {
        return Ok(());
    }
    let output = stderr
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| stdout.ok())
        .unwrap_or_default();
    let mut result = failure(format!(
        "exec probe exited {}: {}",
        status,
        String::from_utf8_lossy(&output)
    ));
    result.exit_code = status.code();
    Err(result)
}

async fn checked_attempt(
    controller: &Controller,
    identity: &RuntimeIdentity,
    epoch: u64,
    phase: RuntimePhase,
    probe: &ProbeSpec,
) -> Option<std::result::Result<(), ProbeFailure>> {
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(probe.timeout_seconds), attempt(&probe.action)) => Some(result.unwrap_or_else(|_| Err(failure("probe attempt timed out")))),
        _ = phase_change(controller, identity, epoch, phase) => None,
    }
}
async fn initialize(
    controller: &Controller,
    identity: &RuntimeIdentity,
    epoch: u64,
    config: &ProbeConfig,
) -> Result<()> {
    let Some(probe) = &config.readiness else {
        return controller.probe_ready(identity, epoch);
    };
    let mut last = ProbeFailure::timeout("readiness initialization deadline exceeded");
    let checks = async {
        if !delay(
            controller,
            identity,
            epoch,
            RuntimePhase::Initializing,
            probe.initial_delay_seconds,
        )
        .await
        {
            return false;
        }
        loop {
            let Some(result) = checked_attempt(
                controller,
                identity,
                epoch,
                RuntimePhase::Initializing,
                probe,
            )
            .await
            else {
                return false;
            };
            match result {
                Ok(()) => return true,
                Err(mut result) => {
                    result.kind = ProbeFailureKind::ReadinessTimeout;
                    result.consecutive_failures = last.consecutive_failures.saturating_add(1);
                    last = result;
                }
            }
            if !delay(
                controller,
                identity,
                epoch,
                RuntimePhase::Initializing,
                probe.period_seconds,
            )
            .await
            {
                return false;
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(config.init_timeout_seconds), checks).await {
        Ok(true) => controller.probe_ready(identity, epoch),
        Ok(false) => Ok(()),
        Err(_) => {
            last.message =
                failure(format!("readiness deadline exceeded: {}", last.message)).message;
            controller.probe_failed(identity, epoch, last)
        }
    }
}
async fn healthcheck(
    controller: &Controller,
    identity: &RuntimeIdentity,
    epoch: u64,
    probe: &ProbeSpec,
) -> Result<()> {
    if !delay(
        controller,
        identity,
        epoch,
        RuntimePhase::Running,
        probe.initial_delay_seconds,
    )
    .await
    {
        return Ok(());
    }
    let mut failures = 0_u32;
    loop {
        let Some(result) =
            checked_attempt(controller, identity, epoch, RuntimePhase::Running, probe).await
        else {
            return Ok(());
        };
        match result {
            Ok(()) => failures = 0,
            Err(mut result) => {
                failures = failures.saturating_add(1);
                if failures >= probe.failure_threshold {
                    result.consecutive_failures = failures;
                    return controller.probe_failed(identity, epoch, result);
                }
            }
        }
        if !delay(
            controller,
            identity,
            epoch,
            RuntimePhase::Running,
            probe.period_seconds,
        )
        .await
        {
            return Ok(());
        }
    }
}
async fn supervise(controller: Arc<Controller>, config: ProbeConfig) -> Result<()> {
    let mut changes = controller.subscribe();
    loop {
        let (status, epoch) = controller.probe_status()?;
        let result = match status.phase {
            RuntimePhase::Initializing => {
                initialize(&controller, &status.identity, epoch, &config).await
            }
            RuntimePhase::Running if config.healthcheck.is_some() => {
                healthcheck(
                    &controller,
                    &status.identity,
                    epoch,
                    config.healthcheck.as_ref().ok_or(Error::Conflict)?,
                )
                .await
            }
            _ => {
                changes
                    .changed()
                    .await
                    .map_err(|_| Error::Unavailable("probe controller stopped".into()))?;
                Ok(())
            }
        };
        match result {
            Err(error) if error != Error::Conflict => return Err(error),
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn http_requires_success_and_does_not_follow_redirects() {
        for status in [200, 302, 503] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 1024];
                assert!(stream.read(&mut request).await.unwrap() > 0);
                stream
                    .write_all(
                        format!("HTTP/1.1 {status} Test\r\nContent-Length: 0\r\n\r\n").as_bytes(),
                    )
                    .await
                    .unwrap();
            });
            let result = attempt(&ProbeAction::HttpGet {
                port,
                path: "/ready".into(),
            })
            .await;
            assert_eq!(result.is_ok(), status == 200);
            if let Err(failure) = result {
                assert_eq!(failure.http_status, Some(status));
            }
        }
    }
    #[tokio::test]
    async fn exec_records_exit_and_cleans_up_on_timeout() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf failure >&2; exit 7"]);
        let result = exec_attempt(command).await.unwrap_err();
        assert_eq!(result.exit_code, Some(7));
        assert!(result.message.contains("failure"));
        let temp = tempfile::tempdir().unwrap();
        let pidfile = temp.path().join("pid");
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 60 & echo $! > \"$1\"; wait", "probe"])
            .arg(&pidfile);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), exec_attempt(command))
                .await
                .is_err()
        );
        let pid: i32 = std::fs::read_to_string(pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        // SAFETY: signal zero checks existence only for the recorded child PID.
        let alive = unsafe { libc::kill(pid, 0) } == 0;
        #[cfg(target_os = "linux")]
        let alive = alive
            && std::fs::read_to_string(format!("/proc/{pid}/stat"))
                .is_ok_and(|s| !s.contains(") Z "));
        assert!(!alive, "probe descendant remains alive: {pid}");
    }
}
