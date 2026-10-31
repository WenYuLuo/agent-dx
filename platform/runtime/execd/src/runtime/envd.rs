//! Deployment-owned lifecycle management for the optional envd service.
//!
//! This module intentionally has no public listener. Execd talks to envd on
//! loopback; ingress is responsible for exposing only the approved data-plane
//! routes after Adxlet observes this service as Ready.
use adx_core::{Error, Result};
use sha2::{Digest, Sha256};
use std::fmt;
use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    path::PathBuf,
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout, Instant},
};

const ENVD_PORT: u16 = 49983;
const HEALTH_DEADLINE: Duration = Duration::from_secs(15);
const PROBE_TIMEOUT: Duration = Duration::from_secs(2);
const FREEZE_MAX_WAIT_MS: u64 = 1_500;
const FREEZE_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const ENVD_LOG_DIRECTORY: &str = "/run/adx-execd/envd";

/// Observation recorded for one checkpoint-preparation attempt.
///
/// `Unconfirmed` means envd accepted an older no-content request or returned a
/// result that did not prove every requested cgroup stopped. Callers must keep
/// the normal best-effort checkpoint semantics in that case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FreezeOutcome {
    NotRequested,
    Confirmed,
    Unconfirmed,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct EnvdConfig {
    expected_version: String,
    path: PathBuf,
    sha256: String,
    args: Vec<String>,
    default_user: String,
    default_workdir: String,
    token: Option<String>,
    gateway_auth: bool,
    user_env: BTreeMap<String, String>,
    freeze_on_checkpoint: bool,
}

impl fmt::Debug for EnvdConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EnvdConfig")
            .field("expected_version", &self.expected_version)
            .field("path", &self.path)
            .field("sha256", &self.sha256)
            .field("args", &self.args)
            .field("default_user", &self.default_user)
            .field("default_workdir", &self.default_workdir)
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .field("gateway_auth", &self.gateway_auth)
            .field("user_env_count", &self.user_env.len())
            .finish()
    }
}

impl EnvdConfig {
    pub(crate) fn gateway_auth(&self) -> bool {
        self.gateway_auth
    }

    pub(crate) fn from_environment(environment: &BTreeMap<String, String>) -> Result<Option<Self>> {
        let Some(path) = environment
            .get("ADX_ENVD_PATH")
            .filter(|value| !value.is_empty())
        else {
            return Ok(None);
        };
        let required = |key: &str| {
            environment
                .get(key)
                .filter(|value| !value.is_empty())
                .cloned()
                .ok_or_else(|| {
                    Error::Invalid(format!("{key} is required when ADX_ENVD_PATH is set"))
                })
        };
        let expected_version = required("ADX_ENVD_VERSION")?;
        if expected_version.len() > 128
            || expected_version
                .chars()
                .any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(Error::Invalid(
                "ADX_ENVD_VERSION must be a protocol version".into(),
            ));
        }
        let sha256 = required("ADX_ENVD_SHA256")?;
        if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::Invalid(
                "ADX_ENVD_SHA256 must be a SHA-256 hex digest".into(),
            ));
        }
        let args = environment
            .get("ADX_ENVD_ARGS")
            .map(|value| serde_json::from_str(value))
            .transpose()
            .map_err(|_| Error::Invalid("ADX_ENVD_ARGS must be a JSON string array".into()))?
            .unwrap_or_else(|| vec!["--port".into(), ENVD_PORT.to_string()]);
        if args.iter().any(|arg| arg.contains('\0')) {
            return Err(Error::Invalid("ADX_ENVD_ARGS contains a NUL byte".into()));
        }
        let freeze_on_checkpoint = match environment.get("ADX_ENVD_FREEZE_ON_CHECKPOINT") {
            None => true,
            Some(value) => value.parse::<bool>().map_err(|_| {
                Error::Invalid("ADX_ENVD_FREEZE_ON_CHECKPOINT must be true or false".into())
            })?,
        };
        let gateway_auth = match environment.get("ADX_ENVD_GATEWAY_AUTH") {
            None => false,
            Some(value) => value.parse::<bool>().map_err(|_| {
                Error::Invalid("ADX_ENVD_GATEWAY_AUTH must be true or false".into())
            })?,
        };
        let token = if gateway_auth {
            None
        } else {
            Some(required("ADX_ENVD_ACCESS_TOKEN")?)
        };
        Ok(Some(Self {
            expected_version,
            path: PathBuf::from(path),
            sha256: sha256.to_ascii_lowercase(),
            args,
            default_user: required("ADX_ENVD_DEFAULT_USER")?,
            default_workdir: required("ADX_ENVD_DEFAULT_WORKDIR")?,
            token,
            gateway_auth,
            user_env: environment
                .iter()
                .filter(|(key, _)| !key.starts_with("ADX_") && !key.starts_with("EXECD_"))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            freeze_on_checkpoint,
        }))
    }

    fn verify_artifact(&self) -> Result<()> {
        let metadata = std::fs::metadata(&self.path)
            .map_err(|error| Error::Unavailable(format!("envd artifact: {error}")))?;
        #[cfg(unix)]
        if std::os::unix::fs::PermissionsExt::mode(&metadata.permissions()) & 0o111 == 0 {
            return Err(Error::Invalid("envd artifact is not executable".into()));
        }
        if !metadata.is_file() {
            return Err(Error::Invalid(
                "envd artifact must be a regular file".into(),
            ));
        }
        let bytes = std::fs::read(&self.path)
            .map_err(|error| Error::Unavailable(format!("read envd artifact: {error}")))?;
        let actual = format!("{:x}", Sha256::digest(bytes));
        if actual != self.sha256 {
            return Err(Error::Invalid(
                "envd artifact digest differs from deployment profile".into(),
            ));
        }
        Ok(())
    }
}

fn validate_default_identity(user: &str, workdir: &std::path::Path) -> Result<()> {
    if user.is_empty()
        || user.len() > 32
        || !user.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase()
                || byte == b'_'
                || (index > 0 && (byte.is_ascii_digit() || byte == b'-'))
        })
        || !workdir.is_absolute()
        || workdir.components().any(|part| {
            matches!(
                part,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(Error::Invalid(
            "invalid envd default user or workdir".into(),
        ));
    }
    Ok(())
}

pub(crate) struct EnvdManager {
    config: EnvdConfig,
    child: Option<Child>,
    version: Option<String>,
    port: u16,
    freeze_attempted: bool,
}

impl EnvdManager {
    pub(crate) fn new(config: EnvdConfig) -> Self {
        Self {
            config,
            child: None,
            version: None,
            port: ENVD_PORT,
            freeze_attempted: false,
        }
    }

    pub(crate) fn version(&self) -> Option<String> {
        self.version.clone()
    }

    pub(crate) fn gateway_auth(&self) -> bool {
        self.config.gateway_auth
    }

    /// Asks envd to freeze its user cgroups before a checkpoint.
    ///
    /// Any transport or HTTP failure leaves the cleanup marker set because
    /// envd may have frozen some cgroups before the failure became observable.
    pub(crate) async fn prepare_checkpoint(&mut self) -> Result<FreezeOutcome> {
        if !self.config.freeze_on_checkpoint {
            return Ok(FreezeOutcome::NotRequested);
        }
        self.freeze_attempted = true;
        let response = timeout(
            FREEZE_REQUEST_TIMEOUT,
            self.request(
                "POST",
                &format!("/freeze?maxWaitMs={FREEZE_MAX_WAIT_MS}"),
                None,
                self.config.token.as_deref(),
            ),
        )
        .await
        .map_err(|_| Error::Unavailable("envd freeze deadline exceeded".into()))??;
        if response.status == 204 {
            return Ok(FreezeOutcome::Unconfirmed);
        }
        if response.status != 200 {
            return Err(Error::Unavailable(format!(
                "envd freeze returned HTTP {}",
                response.status
            )));
        }
        Ok(parse_freeze_outcome(&response.body))
    }

    /// Rolls back a checkpoint preparation that did not reach the backend.
    pub(crate) async fn rollback_checkpoint_prepare(&mut self) -> Result<()> {
        self.unfreeze_if_attempted().await
    }

    /// Clears a best-effort envd freeze after the backend resumes the source.
    pub(crate) async fn resume_after_checkpoint(&mut self) -> Result<()> {
        self.unfreeze_if_attempted().await
    }

    pub(crate) async fn start_and_initialize(&mut self, lifecycle_id: &str) -> Result<()> {
        self.config.verify_artifact()?;
        self.prepare_default_user().await?;
        let (stdout, stderr) = protected_log_files()?;
        let mut command = Command::new(&self.config.path);
        command
            .args(&self.config.args)
            .env_clear()
            .env(
                "PATH",
                "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
            .env("HOME", &self.config.default_workdir)
            .env("LANG", "C.UTF-8")
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .kill_on_drop(true);
        if self.config.gateway_auth && !self.config.args.iter().any(|arg| arg == "--isnotfc") {
            // Upstream envd treats this as the explicit no-MMDS mode even in a VM.
            command.arg("--isnotfc");
        }
        self.child = Some(
            command
                .spawn()
                .map_err(|error| Error::Unavailable(format!("start envd: {error}")))?,
        );
        if let Err(error) = self.wait_health().await {
            self.stop().await;
            return Err(error);
        }
        if let Err(error) = self.initialize(lifecycle_id).await {
            self.stop().await;
            return Err(error);
        }
        Ok(())
    }

    // The trusted envd profile defines the execution user, independently of OCI USER/WORKDIR.
    // Image-only preparation must also work when the image has not pre-created that user.
    async fn prepare_default_user(&self) -> Result<()> {
        let user = &self.config.default_user;
        let workdir = std::path::Path::new(&self.config.default_workdir);
        validate_default_identity(user, workdir)?;
        if super::entrypoint::resolve_user(user).is_err() {
            let mut command = Command::new("useradd");
            command
                .env_clear()
                .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
                .args([
                    "--create-home",
                    "--home-dir",
                    &self.config.default_workdir,
                    "--shell",
                    "/bin/bash",
                    "--",
                    user,
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            let status = timeout(Duration::from_secs(10), command.status())
                .await
                .map_err(|_| Error::Unavailable("envd default user preparation timed out".into()))?
                .map_err(|_| {
                    Error::Invalid("image must provide useradd or the configured envd user".into())
                })?;
            if !status.success() {
                return Err(Error::Invalid(
                    "could not prepare configured envd user in image".into(),
                ));
            }
        }
        let (uid, gid, _) = super::entrypoint::resolve_user(user)
            .map_err(|_| Error::Invalid("configured envd user is unavailable".into()))?;
        if !workdir.exists() {
            std::fs::create_dir_all(workdir)
                .map_err(|_| Error::Unavailable("create envd default workdir failed".into()))?;
            std::os::unix::fs::chown(workdir, Some(uid), Some(gid))
                .map_err(|_| Error::Unavailable("set envd default workdir owner failed".into()))?;
        }
        if !workdir.is_dir() {
            return Err(Error::Invalid(
                "envd default workdir must be a directory".into(),
            ));
        }
        Ok(())
    }

    /// A restore keeps the envd process and its command map; its auth mode is
    /// immutable for the life of that process.
    pub(crate) async fn refresh_after_restore(
        &mut self,
        restored: &EnvdConfig,
        lifecycle_id: &str,
    ) -> Result<()> {
        if self.config.expected_version != restored.expected_version
            || self.config.path != restored.path
            || self.config.sha256 != restored.sha256
            || self.config.gateway_auth != restored.gateway_auth
        {
            return Err(Error::Conflict);
        }
        self.config = restored.clone();
        self.initialize(lifecycle_id).await?;
        self.freeze_attempted = false;
        Ok(())
    }

    pub(crate) async fn check_running(&mut self) -> Result<()> {
        self.check_child()?;
        self.health().await?;
        self.authenticated_probe().await
    }

    pub(crate) async fn stop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill().await;
            let _ = child.wait().await;
        }
        self.freeze_attempted = false;
    }

    async fn initialize(&mut self, lifecycle_id: &str) -> Result<()> {
        let mut payload = serde_json::json!({
            "defaultUser": self.config.default_user,
            "defaultWorkdir": self.config.default_workdir,
            "lifecycleID": lifecycle_id,
            "envVars": &self.config.user_env,
        });
        if let Some(token) = &self.config.token {
            payload["accessToken"] = serde_json::Value::String(token.clone());
        }
        let response = timeout(
            PROBE_TIMEOUT,
            self.request("POST", "/init", Some(payload.to_string()), None),
        )
        .await
        .map_err(|_| Error::Unavailable("envd init deadline exceeded".into()))??;
        if response.status != 204 {
            return Err(Error::Unavailable(format!(
                "envd init returned HTTP {}",
                response.status
            )));
        }
        self.wait_health().await?;
        // Pinned envd reports its running version on /init, not /health.
        // Execd owns protocol verification before it publishes Ready to the node.
        let version = response
            .headers
            .get("x-envd-version")
            .filter(|version| !version.is_empty())
            .ok_or_else(|| Error::Unavailable("envd init omitted its running version".into()))?;
        if version != &self.config.expected_version {
            self.version = None;
            return Err(Error::Unavailable(
                "envd running version differs from configured version".into(),
            ));
        }
        self.version = Some(version.clone());
        self.authenticated_probe().await
    }

    async fn authenticated_probe(&self) -> Result<()> {
        let probe = timeout(
            PROBE_TIMEOUT,
            self.request(
                "POST",
                "/process.Process/List",
                Some("{}".into()),
                self.config.token.as_deref(),
            ),
        )
        .await
        .map_err(|_| Error::Unavailable("envd authenticated probe deadline exceeded".into()))??;
        if !(200..300).contains(&probe.status) {
            return Err(Error::Unavailable(format!(
                "envd authenticated probe returned HTTP {}",
                probe.status
            )));
        }
        Ok(())
    }

    async fn wait_health(&mut self) -> Result<()> {
        let deadline = Instant::now() + HEALTH_DEADLINE;
        loop {
            match self.health().await {
                Ok(version) => {
                    self.version = version;
                    return Ok(());
                }
                Err(error) if Instant::now() < deadline => {
                    self.check_child()?;
                    sleep(Duration::from_millis(100)).await;
                    let _ = error;
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn check_child(&mut self) -> Result<()> {
        let child = self
            .child
            .as_mut()
            .ok_or_else(|| Error::Unavailable("envd is not running".into()))?;
        if let Some(status) = child
            .try_wait()
            .map_err(|error| Error::Unavailable(format!("observe envd: {error}")))?
        {
            return Err(Error::Unavailable(format!("envd exited with {status}")));
        }
        Ok(())
    }

    async fn health(&self) -> Result<Option<String>> {
        let response = timeout(PROBE_TIMEOUT, self.request("GET", "/health", None, None))
            .await
            .map_err(|_| Error::Unavailable("envd health deadline exceeded".into()))??;
        if response.status != 204 {
            return Err(Error::Unavailable(format!(
                "envd health returned HTTP {}",
                response.status
            )));
        }
        let version = response.headers.get("x-envd-version");
        if version.is_some_and(|version| version != &self.config.expected_version) {
            return Err(Error::Unavailable(
                "envd health version differs from configured version".into(),
            ));
        }
        Ok(version.cloned())
    }

    async fn unfreeze_if_attempted(&mut self) -> Result<()> {
        if !self.freeze_attempted {
            return Ok(());
        }
        let response = timeout(
            PROBE_TIMEOUT,
            self.request("POST", "/unfreeze", None, self.config.token.as_deref()),
        )
        .await
        .map_err(|_| Error::Unavailable("envd unfreeze deadline exceeded".into()))??;
        if !(200..300).contains(&response.status) {
            return Err(Error::Unavailable(format!(
                "envd unfreeze returned HTTP {}",
                response.status
            )));
        }
        self.freeze_attempted = false;
        Ok(())
    }

    async fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<String>,
        token: Option<&str>,
    ) -> Result<HttpResponse> {
        request_to(self.port, method, path, body, token).await
    }

    #[cfg(test)]
    fn new_on_port(config: EnvdConfig, port: u16) -> Self {
        Self {
            config,
            child: None,
            version: None,
            port,
            freeze_attempted: false,
        }
    }
}

fn parse_freeze_outcome(body: &str) -> FreezeOutcome {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return FreezeOutcome::Unconfirmed;
    };
    let count = |name: &str| value.get(name).and_then(serde_json::Value::as_u64);
    let Some(requested) = count("requested") else {
        return FreezeOutcome::Unconfirmed;
    };
    let Some(frozen) = count("frozen") else {
        return FreezeOutcome::Unconfirmed;
    };
    let Some(not_frozen) = count("notFrozen") else {
        return FreezeOutcome::Unconfirmed;
    };
    let Some(failed) = count("failed") else {
        return FreezeOutcome::Unconfirmed;
    };
    let Some(vanished) = count("vanished") else {
        return FreezeOutcome::Unconfirmed;
    };
    let Some(unobservable) = count("unobservable") else {
        return FreezeOutcome::Unconfirmed;
    };
    let Some(truncated) = value.get("truncated").and_then(serde_json::Value::as_bool) else {
        return FreezeOutcome::Unconfirmed;
    };
    if requested > 0
        && frozen == requested
        && not_frozen == 0
        && failed == 0
        && vanished == 0
        && unobservable == 0
        && !truncated
    {
        FreezeOutcome::Confirmed
    } else {
        FreezeOutcome::Unconfirmed
    }
}

fn protected_log_files() -> Result<(File, File)> {
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

    let directory = PathBuf::from(ENVD_LOG_DIRECTORY);
    std::fs::create_dir_all(&directory)
        .map_err(|error| Error::Unavailable(format!("create envd log directory: {error}")))?;
    std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))
        .map_err(|error| Error::Unavailable(format!("protect envd log directory: {error}")))?;
    let open = |name: &str| {
        OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(directory.join(name))
            .map_err(|error| Error::Unavailable(format!("open envd {name} log: {error}")))
    };
    Ok((open("stdout.log")?, open("stderr.log")?))
}

struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: String,
}

#[cfg(test)]
async fn request(
    method: &str,
    path: &str,
    body: Option<String>,
    token: Option<&str>,
) -> Result<HttpResponse> {
    request_to(ENVD_PORT, method, path, body, token).await
}

async fn request_to(
    port: u16,
    method: &str,
    path: &str,
    body: Option<String>,
    token: Option<&str>,
) -> Result<HttpResponse> {
    let mut stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port))
        .await
        .map_err(|error| Error::Unavailable(format!("connect envd: {error}")))?;
    let body = body.unwrap_or_default();
    let content_type = if body.is_empty() {
        ""
    } else if path.starts_with("/process.Process/") {
        "Content-Type: application/json\r\nConnect-Protocol-Version: 1\r\n"
    } else {
        "Content-Type: application/json\r\n"
    };
    let auth = token
        .map(|token| format!("X-Access-Token: {token}\r\n"))
        .unwrap_or_default();
    let request = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}{content_type}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(|error| Error::Unavailable(format!("write envd request: {error}")))?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .map_err(|error| Error::Unavailable(format!("read envd response: {error}")))?;
    let response = String::from_utf8(response)
        .map_err(|_| Error::Unavailable("envd returned non-HTTP response".into()))?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| Error::Unavailable("envd returned malformed HTTP response".into()))?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| Error::Unavailable("envd returned malformed HTTP status".into()))?;
    let mut headers = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().into());
        }
    }
    Ok(HttpResponse {
        status,
        headers,
        body: body.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_manager(port: u16, freeze_on_checkpoint: bool) -> EnvdManager {
        EnvdManager::new_on_port(
            EnvdConfig {
                expected_version: "0.9.0".into(),
                path: "/envd".into(),
                sha256: "a".repeat(64),
                args: vec![],
                default_user: "user".into(),
                default_workdir: "/home/user".into(),
                token: Some("fixture-token".into()),
                gateway_auth: false,
                user_env: BTreeMap::new(),
                freeze_on_checkpoint,
            },
            port,
        )
    }

    fn gateway_manager(port: u16, freeze_on_checkpoint: bool) -> EnvdManager {
        EnvdManager::new_on_port(
            EnvdConfig {
                expected_version: "0.9.0".into(),
                path: "/envd".into(),
                sha256: "a".repeat(64),
                args: vec![],
                default_user: "user".into(),
                default_workdir: "/home/user".into(),
                token: None,
                gateway_auth: true,
                user_env: BTreeMap::new(),
                freeze_on_checkpoint,
            },
            port,
        )
    }

    async fn freeze_fixture(
        responses: Vec<&'static str>,
    ) -> (u16, tokio::task::JoinHandle<Vec<String>>) {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let mut requests = Vec::with_capacity(responses.len());
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = vec![0; 4096];
                let size = stream.read(&mut request).await.unwrap();
                requests.push(String::from_utf8(request[..size].to_vec()).unwrap());
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            requests
        });
        (port, server)
    }

    async fn freeze_timeout_fixture() -> (u16, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            sleep(FREEZE_REQUEST_TIMEOUT + Duration::from_millis(50)).await;
        });
        (port, server)
    }

    #[tokio::test]
    async fn checkpoint_freeze_sends_token_and_confirms_structured_result() {
        let (port, server) = freeze_fixture(vec![
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 99\r\nConnection: close\r\n\r\n{\"requested\":2,\"frozen\":2,\"notFrozen\":0,\"failed\":0,\"vanished\":0,\"unobservable\":0,\"truncated\":false}",
        ])
        .await;
        let mut manager = test_manager(port, true);

        assert_eq!(
            manager.prepare_checkpoint().await.unwrap(),
            FreezeOutcome::Confirmed
        );
        assert!(manager.freeze_attempted);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("POST /freeze?maxWaitMs="));
        assert!(requests[0].contains("X-Access-Token: fixture-token\r\n"));
    }

    #[tokio::test]
    async fn gateway_auth_never_sends_sdk_token_to_init_or_probes() {
        let (port, server) = freeze_fixture(vec![
            "HTTP/1.1 204 No Content\r\nX-Envd-Version: 0.9.0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"requested\":0,\"frozen\":0,\"notFrozen\":0,\"failed\":0,\"vanished\":0,\"unobservable\":0,\"truncated\":false}",
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
        ])
        .await;
        let mut manager = gateway_manager(port, true);

        manager.initialize("gateway-runtime").await.unwrap();
        manager.prepare_checkpoint().await.unwrap();
        manager.rollback_checkpoint_prepare().await.unwrap();

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 5);
        assert!(requests
            .iter()
            .all(|request| !request.contains("X-Access-Token:")));
        assert!(!requests[0].contains("accessToken"));
    }

    #[tokio::test]
    async fn init_rejects_an_unexpected_running_version_before_reporting_ready() {
        let (port, server) = freeze_fixture(vec![
            "HTTP/1.1 204 No Content\r\nX-Envd-Version: 0.8.0\r\nConnection: close\r\n\r\n",
            "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n",
        ])
        .await;
        let mut manager = test_manager(port, true);
        assert!(manager.initialize("runtime").await.is_err());
        assert_eq!(manager.version(), None);
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn checkpoint_freeze_disabled_does_not_contact_envd() {
        let mut manager = test_manager(1, false);

        assert_eq!(
            manager.prepare_checkpoint().await.unwrap(),
            FreezeOutcome::NotRequested
        );
        assert!(!manager.freeze_attempted);
    }

    #[tokio::test]
    async fn checkpoint_freeze_204_is_unconfirmed_and_rolls_back_once() {
        let (port, server) = freeze_fixture(vec![
            ("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"),
            ("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"),
        ])
        .await;
        let mut manager = test_manager(port, true);

        assert_eq!(
            manager.prepare_checkpoint().await.unwrap(),
            FreezeOutcome::Unconfirmed
        );
        manager.rollback_checkpoint_prepare().await.unwrap();
        manager.resume_after_checkpoint().await.unwrap();
        assert!(!manager.freeze_attempted);

        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[1].starts_with("POST /unfreeze HTTP/1.1"));
    }

    #[tokio::test]
    async fn freeze_error_keeps_cleanup_marker_until_unfreeze_succeeds() {
        let (port, server) = freeze_fixture(vec![
            ("HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n"),
            ("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"),
        ])
        .await;
        let mut manager = test_manager(port, true);

        assert!(manager.prepare_checkpoint().await.is_err());
        assert!(manager.freeze_attempted);
        manager.rollback_checkpoint_prepare().await.unwrap();
        assert!(!manager.freeze_attempted);
        assert_eq!(server.await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn freeze_timeout_keeps_cleanup_marker() {
        let (port, server) = freeze_timeout_fixture().await;
        let mut manager = test_manager(port, true);

        assert!(manager.prepare_checkpoint().await.is_err());
        assert!(manager.freeze_attempted);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn unfreeze_failure_keeps_marker_for_a_later_retry() {
        let (port, server) = freeze_fixture(vec![
            ("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{\"requested\":1,\"frozen\":1,\"notFrozen\":0,\"failed\":0,\"vanished\":0,\"unobservable\":0,\"truncated\":false}"),
            ("HTTP/1.1 500 Internal Server Error\r\nConnection: close\r\n\r\n"),
            ("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"),
        ])
        .await;
        let mut manager = test_manager(port, true);

        assert_eq!(
            manager.prepare_checkpoint().await.unwrap(),
            FreezeOutcome::Confirmed
        );
        assert!(manager.rollback_checkpoint_prepare().await.is_err());
        assert!(manager.freeze_attempted);
        manager.resume_after_checkpoint().await.unwrap();
        assert!(!manager.freeze_attempted);
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn successful_restore_init_clears_freeze_marker_without_restarting_envd() {
        let (port, server) = freeze_fixture(vec![
            ("HTTP/1.1 204 No Content\r\nX-Envd-Version: 0.9.0\r\nConnection: close\r\n\r\n"),
            ("HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n"),
            ("HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n"),
        ])
        .await;
        let config = test_manager(port, true).config;
        let mut manager = EnvdManager::new_on_port(config.clone(), port);
        manager.freeze_attempted = true;

        manager
            .refresh_after_restore(&config, "restored")
            .await
            .unwrap();
        assert!(!manager.freeze_attempted);
        assert!(manager.child.is_none());
        assert_eq!(server.await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn failed_restore_init_keeps_freeze_marker() {
        let config = test_manager(1, true).config;
        let mut manager = EnvdManager::new_on_port(config.clone(), 1);
        manager.freeze_attempted = true;

        assert!(manager
            .refresh_after_restore(&config, "restored")
            .await
            .is_err());
        assert!(manager.freeze_attempted);
        assert!(manager.child.is_none());
    }

    #[tokio::test]
    /// This is a local envd component check, not proof of CCE checkpoint freeze
    /// behavior or Firecracker/Ingress network isolation.
    #[ignore = "requires a pinned native upstream envd binary and exclusive port 49983"]
    async fn real_envd_initialization_authentication_and_failure_detection() {
        real_envd_lifecycle(false).await;
        real_envd_lifecycle(true).await;
    }

    async fn real_envd_lifecycle(gateway_auth: bool) {
        let path =
            PathBuf::from(std::env::var("ADX_TEST_ENVD_BIN").expect("envd artifact required"));
        let workdir = tempfile::tempdir().unwrap();
        let mut manager = EnvdManager::new(EnvdConfig {
            expected_version: "0.9.0".into(),
            sha256: format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap())),
            path,
            args: if gateway_auth {
                vec![
                    "--port".into(),
                    ENVD_PORT.to_string(),
                    "--no-cgroups".into(),
                ]
            } else {
                vec!["--isnotfc".into(), "--no-cgroups".into()]
            },
            default_user: "root".into(),
            default_workdir: workdir.path().display().to_string(),
            token: (!gateway_auth).then(|| "test-envd-credential-0123456789abcdef".into()),
            gateway_auth,
            user_env: BTreeMap::from([("E2B_TEST_VALUE".into(), "ready".into())]),
            freeze_on_checkpoint: true,
        });
        manager
            .start_and_initialize("envd-component-test")
            .await
            .unwrap();
        manager.check_running().await.unwrap();
        assert_eq!(manager.version().as_deref(), Some("0.9.0"));
        let process_id = manager.child.as_ref().and_then(Child::id).unwrap();
        assert_eq!(
            manager.prepare_checkpoint().await.unwrap(),
            FreezeOutcome::Unconfirmed
        );
        assert!(manager.freeze_attempted);
        manager.rollback_checkpoint_prepare().await.unwrap();
        assert!(!manager.freeze_attempted);
        assert_eq!(
            manager.prepare_checkpoint().await.unwrap(),
            FreezeOutcome::Unconfirmed
        );
        let restored_config = manager.config.clone();
        manager
            .refresh_after_restore(&restored_config, "envd-component-test-restored")
            .await
            .unwrap();
        assert!(!manager.freeze_attempted);
        assert_eq!(manager.child.as_ref().and_then(Child::id), Some(process_id));
        let status = request(
            "POST",
            "/process.Process/List",
            Some("{}".into()),
            if gateway_auth {
                None
            } else {
                Some("incorrect-token")
            },
        )
        .await
        .unwrap()
        .status;
        assert_eq!(status, if gateway_auth { 200 } else { 401 });
        manager.stop().await;
        assert!(manager.check_running().await.is_err());
    }

    #[test]
    fn envd_configuration_requires_deployment_identity_and_token() {
        let values = BTreeMap::from([("ADX_ENVD_PATH".into(), "/envd".into())]);
        assert!(EnvdConfig::from_environment(&values).is_err());
    }

    #[test]
    fn envd_configuration_accepts_checkpoint_freeze_false() {
        let values = BTreeMap::from([
            ("ADX_ENVD_PATH".into(), "/envd".into()),
            ("ADX_ENVD_VERSION".into(), "0.9.0".into()),
            ("ADX_ENVD_SHA256".into(), "a".repeat(64)),
            ("ADX_ENVD_DEFAULT_USER".into(), "user".into()),
            ("ADX_ENVD_DEFAULT_WORKDIR".into(), "/home/user".into()),
            ("ADX_ENVD_ACCESS_TOKEN".into(), "fixture-token".into()),
            ("ADX_ENVD_FREEZE_ON_CHECKPOINT".into(), "false".into()),
        ]);
        let config = EnvdConfig::from_environment(&values).unwrap().unwrap();
        assert!(!config.freeze_on_checkpoint);
    }

    #[test]
    fn gateway_auth_configuration_omits_the_sdk_token() {
        let values = BTreeMap::from([
            ("ADX_ENVD_PATH".into(), "/envd".into()),
            ("ADX_ENVD_VERSION".into(), "0.9.0".into()),
            ("ADX_ENVD_SHA256".into(), "a".repeat(64)),
            ("ADX_ENVD_DEFAULT_USER".into(), "user".into()),
            ("ADX_ENVD_DEFAULT_WORKDIR".into(), "/home/user".into()),
            ("ADX_ENVD_GATEWAY_AUTH".into(), "true".into()),
        ]);
        let config = EnvdConfig::from_environment(&values).unwrap().unwrap();
        assert!(config.gateway_auth);
        assert_eq!(config.token, None);
    }

    #[test]
    fn envd_configuration_debug_redacts_access_token() {
        let config = EnvdConfig {
            expected_version: "0.9.0".into(),
            path: "/envd".into(),
            sha256: "a".repeat(64),
            args: vec![],
            default_user: "user".into(),
            default_workdir: "/home/user".into(),
            token: Some("secret-token".into()),
            gateway_auth: false,
            user_env: BTreeMap::new(),
            freeze_on_checkpoint: true,
        };
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("secret-token"));
        assert!(rendered.contains("[redacted]"));
    }
    #[test]
    fn restore_attempts_token_rotation_through_upstream_authorization() {
        let config = EnvdConfig {
            expected_version: "0.9.0".into(),
            path: "/envd".into(),
            sha256: "a".repeat(64),
            args: vec![],
            default_user: "user".into(),
            default_workdir: "/home/user".into(),
            token: Some("one".into()),
            gateway_auth: false,
            user_env: BTreeMap::new(),
            freeze_on_checkpoint: true,
        };
        let mut manager = EnvdManager::new(config.clone());
        let mut rotated = config;
        rotated.token = Some("two".into());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        // No child was launched in this unit test, so init cannot complete; the
        // error must come from the missing listener, not a local token rejection.
        match runtime.block_on(manager.refresh_after_restore(&rotated, "runtime")) {
            Err(Error::Unavailable(_)) => {}
            Err(error) => panic!("unexpected restore error: {error}"),
            Ok(()) => panic!("restore unexpectedly succeeded without envd"),
        }
    }

    #[test]
    fn restore_rejects_envd_authentication_mode_changes() {
        let config = test_manager(1, true).config;
        let mut manager = EnvdManager::new(config);
        let restored = gateway_manager(1, true).config;
        let runtime = tokio::runtime::Runtime::new().unwrap();
        assert!(matches!(
            runtime.block_on(manager.refresh_after_restore(&restored, "runtime")),
            Err(Error::Conflict)
        ));
    }
}

#[cfg(test)]
mod default_identity_tests {
    use super::*;
    #[test]
    fn validates_trusted_identity_without_accepting_command_options() {
        assert!(validate_default_identity("user", std::path::Path::new("/home/user")).is_ok());
        for user in ["", "--system", "root:root", "a b"] {
            assert!(validate_default_identity(user, std::path::Path::new("/home/user")).is_err());
        }
        assert!(validate_default_identity("user", std::path::Path::new("relative")).is_err());
        assert!(validate_default_identity("user", std::path::Path::new("/home/../root")).is_err());
    }
}
