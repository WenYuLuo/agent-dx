//! Jiuwenbox process and Conch adapters. The external backends are unmodified.
mod client;
pub mod host;
pub mod policy;
mod store;

use self::{
    client::{unavailable, Client},
    host::{HostOperations, LinuxHost},
    store::{Phase, Record, Store},
};
use crate::{
    sandboxd::{self, Config as LaunchConfig},
    RuntimeDriver, RuntimeObservation, RuntimeStatus,
};
use adx_core::{
    execution::{NetworkPolicy, StorageSource},
    scheduling::DeviceAllocation,
    EnvironmentSpec, Error, Result,
};
use async_trait::async_trait;
use hyper::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
use tokio::{sync::Mutex as AsyncMutex, time::Instant};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub image: PathBuf,
    pub sha256: String,
    pub target: String,
    pub command: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Process {
    pub args: Vec<String>,
    pub cwd: String,
    pub user: String,
}

/// An explicit deployment binding; an OCI name is never silently treated as a
/// host root or a Conch template. Versioned template names must be immutable.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Image {
    #[serde(default)]
    pub template_name: Option<String>,
    #[serde(default)]
    pub process: Option<Process>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Class {
    pub images: BTreeMap<String, Image>,
    /// Trusted operator policy; ADX replaces execution-owned fields below.
    #[serde(default = "empty_object")]
    pub policy: Value,
}
fn empty_object() -> Value {
    json!({})
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub socket: PathBuf,
    pub state_dir: PathBuf,
    pub bootstrap: Bootstrap,
    pub classes: BTreeMap<String, Class>,
    #[serde(default)]
    pub conch_socket: Option<PathBuf>,
    /// Read once at startup; the token itself is not persisted in the journal.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
}

type ExecutionGates = Arc<Mutex<BTreeMap<String, Arc<AsyncMutex<()>>>>>;

#[derive(Clone)]
pub struct Jiuwenbox {
    config: Arc<Config>,
    launch: LaunchConfig,
    client: Client,
    conch: Option<Client>,
    store: Store,
    host: Arc<dyn HostOperations>,
    gates: ExecutionGates,
    mount_gate: Arc<AsyncMutex<()>>,
    _lease: Arc<std::fs::File>,
}

fn absolute(path: &Path) -> bool {
    path.is_absolute()
        && path != Path::new("/")
        && path.components().all(|p| {
            matches!(
                p,
                std::path::Component::RootDir | std::path::Component::Normal(_)
            )
        })
}

// Mirrors the unmodified Conch volume manager's reserved guest paths.
fn conch_reserved_target(path: &Path) -> bool {
    path == Path::new("/")
        || ["/proc", "/sys", "/dev", "/run"]
            .iter()
            .any(|root| path.starts_with(root))
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        if !absolute(&self.socket)
            || !absolute(&self.state_dir)
            || !absolute(&self.bootstrap.image)
            || self.bootstrap.sha256.len() != 64
            || !self
                .bootstrap
                .sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || !absolute(Path::new(&self.bootstrap.target))
            || self.bootstrap.command.is_empty()
            || !Path::new(&self.bootstrap.command[0]).starts_with(&self.bootstrap.target)
            || self.bootstrap.command.iter().any(|v| v.contains('\0'))
            || self.classes.is_empty()
        {
            return Err(Error::Invalid(
                "Jiuwenbox requires absolute paths, pinned EROFS SHA256 and bootstrap command"
                    .into(),
            ));
        }
        if self.classes.contains_key("conch")
            && conch_reserved_target(Path::new(&self.bootstrap.target))
        {
            return Err(Error::Invalid("Conch bootstrap target is reserved".into()));
        }
        for (name, class) in &self.classes {
            if !matches!(name.as_str(), "supervisor" | "conch")
                || !class.policy.is_object()
                || class.images.is_empty()
            {
                return Err(Error::Invalid("Jiuwenbox classes require supervisor/conch, image bindings and an object policy".into()));
            }
            for field in [
                "filesystem_policy",
                "cgroup",
                "network",
                "namespace",
                "process",
                "conch",
            ] {
                if class.policy.get(field).is_some_and(|v| !v.is_object()) {
                    return Err(Error::Invalid(format!(
                        "Jiuwenbox policy.{field} must be an object"
                    )));
                }
            }
            if name == "conch" && self.conch_socket.as_ref().is_none_or(|p| !absolute(p)) {
                return Err(Error::Invalid("Conch recovery socket is required".into()));
            }
            for image in class.images.values() {
                if name == "conch"
                    && image
                        .template_name
                        .as_ref()
                        .is_none_or(|s| s.trim().is_empty())
                {
                    return Err(Error::Invalid(
                        "Conch image binding requires an immutable template_name".into(),
                    ));
                }
                if let Some(p) = &image.process {
                    if p.args.is_empty()
                        || p.args[0].trim().is_empty()
                        || p.args.iter().any(|s| s.contains('\0'))
                        || !absolute(Path::new(&p.cwd)) && p.cwd != "/"
                        || p.user.trim().is_empty()
                    {
                        return Err(Error::Invalid(
                            "image process requires argv, absolute cwd and user".into(),
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

impl Jiuwenbox {
    pub async fn connect(config: Config, launch: LaunchConfig) -> Result<Self> {
        let driver = Self::with_host(config, launch, Arc::new(LinuxHost)).await?;
        driver
            .host
            .prepare(&driver.config.bootstrap, &driver.root())
            .await?;
        Ok(driver)
    }
    pub async fn with_host(
        config: Config,
        mut launch: LaunchConfig,
        host: Arc<dyn HostOperations>,
    ) -> Result<Self> {
        config.validate()?;
        if launch.rpc_timeout.is_zero() {
            return Err(Error::Invalid("positive Jiuwenbox timeout required".into()));
        }
        if launch.command.is_empty() {
            launch.command = config.bootstrap.command.clone();
        }
        if let Some(profile) = &launch.runtime_profile {
            if profile.bootstrap.r#type != "erofs"
                || Path::new(&profile.bootstrap.root) != config.bootstrap.image
                || profile.bootstrap.target != config.bootstrap.target
                || profile.bootstrap.entrypoint != config.bootstrap.command
            {
                return Err(Error::Invalid(
                    "Jiuwenbox bootstrap must match the deployment RuntimeProfile EROFS".into(),
                ));
            }
        }
        let token = config
            .token_file
            .as_ref()
            .map(std::fs::read_to_string)
            .transpose()
            .map_err(unavailable)?
            .map(|s| s.trim().to_owned());
        if token
            .as_ref()
            .is_some_and(|s| s.is_empty() || s.chars().any(char::is_control))
        {
            return Err(Error::Invalid("invalid Jiuwenbox token file".into()));
        }
        let client = Client {
            socket: config.socket.clone(),
            token,
            timeout: launch.rpc_timeout,
        };
        let conch = config.conch_socket.as_ref().map(|socket| Client {
            socket: socket.clone(),
            token: None,
            timeout: launch.rpc_timeout,
        });
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        std::fs::create_dir_all(&config.state_dir).map_err(unavailable)?;
        std::fs::set_permissions(&config.state_dir, std::fs::Permissions::from_mode(0o700))
            .map_err(unavailable)?;
        let lease = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .mode(0o600)
            .open(config.state_dir.join("driver.lock"))
            .map_err(unavailable)?;
        lease
            .try_lock()
            .map_err(|_| unavailable("another adxlet owns the Jiuwenbox state directory"))?;
        let store = Store::new(config.state_dir.join("ownership.sqlite"))?;
        // A request owned by a previous adxlet must be rediscovered, never replayed.
        for r in store
            .all()
            .await?
            .values()
            .filter(|r| r.phase == Phase::Creating)
        {
            store
                .update(&r.runtime_id, |r| r.phase = Phase::Uncertain)
                .await?;
        }
        Ok(Self {
            config: Arc::new(config),
            launch,
            client,
            conch,
            store,
            host,
            gates: Arc::default(),
            mount_gate: Arc::default(),
            _lease: Arc::new(lease),
        })
    }
    fn gate(&self, id: &str) -> Arc<AsyncMutex<()>> {
        self.gates
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(id.into())
            .or_default()
            .clone()
    }
    pub async fn owns(&self, id: &str) -> Result<bool> {
        Ok(self.store.get(id).await?.is_some())
    }
    pub async fn available_runtime_classes(&self) -> Result<Vec<String>> {
        self.client
            .request(Method::GET, "/health", None, false)
            .await?;
        // The default health route may return plain text, so discovery uses the
        // actual sandbox API too and validates its complete collection shape.
        self.list().await?;
        if self.config.classes.contains_key("conch") {
            self.conch()?
                .request(Method::GET, "/health", None, false)
                .await?;
        }
        let timeout = self
            .client
            .request(Method::GET, "/api/v1/timeout", None, false)
            .await?;
        if timeout.get("idle_timeout") != Some(&Value::Null) {
            return Err(unavailable(
                "disable Jiuwenbox idle_timeout for ADX-managed execution",
            ));
        }
        self.inventory().await?;
        Ok(self.config.classes.keys().cloned().collect())
    }
    fn conch(&self) -> Result<&Client> {
        self.conch
            .as_ref()
            .ok_or_else(|| unavailable("Conch recovery client unavailable"))
    }
    async fn list(&self) -> Result<Vec<Value>> {
        self.client
            .request(Method::GET, "/api/v1/sandboxes", None, false)
            .await?
            .as_array()
            .cloned()
            .ok_or_else(|| unavailable("invalid Jiuwenbox inventory"))
    }
    fn root(&self) -> PathBuf {
        self.config
            .state_dir
            .join("bootstrap")
            .join(&self.config.bootstrap.sha256)
    }
    fn files(&self, id: &str) -> PathBuf {
        self.config.state_dir.join("launch").join(sandbox_id(id))
    }

    fn plan(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        root: &Path,
    ) -> Result<(Value, Value, Option<sandboxd::proto::NetworkPolicy>)> {
        let class = self
            .config
            .classes
            .get(&spec.runtime_class)
            .ok_or_else(|| Error::Invalid("Jiuwenbox runtime class is not configured".into()))?;
        let image = class
            .images
            .get(&spec.image)
            .ok_or_else(|| Error::Invalid("image is not bound on this Jiuwenbox node".into()))?;
        policy::validate_network(spec.execution.network.as_ref())?;
        if !devices.is_empty()
            || !spec.execution.extra_config.is_empty()
            || spec.execution.limits.disk_bytes > 0
            || spec.resources.disk_bytes > 0
        {
            return Err(Error::Invalid(
                "Jiuwenbox devices, disk limits and extra_config are unsupported".into(),
            ));
        }
        if spec.execution.rootfs.as_ref().is_some_and(|r| {
            r.readonly || !matches!(&r.source,StorageSource::Image(s) if s==&spec.image)
        }) {
            return Err(Error::Invalid(
                "Jiuwenbox requires a writable image binding for the workload filesystem".into(),
            ));
        }
        let mut common = self.launch.clone();
        common.runtime_logs = None;
        let mut request = sandboxd::start_request(spec, id, generation, devices, &common)?;
        request.envs.remove("ADX_CHECKPOINT_HANDOFF_FILE");
        request.envs.remove("ADX_ENV_FILE");
        request.envs.remove("ADX_EXECD_CONTROL_SOCKET_PATH");
        let mut mounts =
            vec![json!({"host_path":root,"sandbox_path":self.config.bootstrap.target,"mode":"ro"})];
        if spec.execution.inherit_entrypoint {
            if image.process.is_none() {
                return Err(Error::Invalid(
                    "image inheritance requires a configured process".into(),
                ));
            }
            mounts.push(
                json!({"host_path":self.files(id),"sandbox_path":"/__adx_launch","mode":"ro"}),
            );
            request.envs.insert(
                "ADX_IMAGE_PROCESS_CONFIG".into(),
                "/__adx_launch/process.json".into(),
            );
        }
        for mount in &spec.execution.mounts {
            let StorageSource::Local(source) = &mount.source else {
                return Err(Error::Invalid(
                    "Jiuwenbox requires Local bind mounts".into(),
                ));
            };
            if spec.runtime_class == "conch" && conch_reserved_target(Path::new(&mount.target)) {
                return Err(Error::Invalid("Conch volume target is reserved".into()));
            }
            if mount.kind != "bind"
                || mount
                    .options
                    .iter()
                    .any(|s| !matches!(s.as_str(), "ro" | "rw" | "rbind"))
                || Path::new(&mount.target).starts_with(&self.config.bootstrap.target)
                || Path::new(&self.config.bootstrap.target).starts_with(&mount.target)
                || Path::new(&mount.target).starts_with("/__adx_launch")
                || Path::new("/__adx_launch").starts_with(&mount.target)
            {
                return Err(Error::Invalid(
                    "unsupported or reserved Jiuwenbox mount".into(),
                ));
            }
            mounts.push(json!({"host_path":source,"sandbox_path":mount.target,"mode":if mount.options.iter().any(|s|s=="ro"){ "ro" }else{"rw"}}));
        }
        let cpu = spec
            .execution
            .limits
            .cpu_millis
            .max(spec.resources.cpu_millis);
        let memory = spec
            .execution
            .limits
            .memory_bytes
            .max(spec.resources.memory_bytes);
        if cpu == 0 || memory == 0 {
            return Err(Error::Invalid(
                "positive Jiuwenbox CPU and memory limits required".into(),
            ));
        }
        let mut backend_policy = class.policy.clone();
        if spec.runtime_class == "conch" {
            if !cpu.is_multiple_of(1000) || !memory.is_multiple_of(1_048_576) {
                return Err(Error::Invalid(
                    "Conch limits require integer vCPUs and MiB".into(),
                ));
            }
            let mut conch = class
                .policy
                .get("conch")
                .cloned()
                .unwrap_or_else(empty_object);
            let owned = json!({"template_name":image.template_name,"vcpu_num":cpu/1000,"vcpu_max":cpu/1000,"ram_mb":memory/1_048_576,"filesystem_policy":{"bind_mounts":mounts},"network":{"ingress":{"default":"allow"},"egress":{"default":"allow"}}});
            for (key, value) in owned
                .as_object()
                .ok_or_else(|| unavailable("invalid owned Conch policy"))?
            {
                conch[key] = value.clone();
            }
            backend_policy["conch"] = conch;
        } else {
            backend_policy["filesystem_policy"]["bind_mounts"] = json!(mounts);
            // A scalar replaces Jiuwenbox's base cpu_max tuple in append mode;
            // sending a list would concatenate/deduplicate the quota and period.
            let quota = cpu
                .checked_mul(100)
                .ok_or_else(|| Error::Invalid("CPU limit overflows".into()))?;
            backend_policy["cgroup"]["cpu_max"] = json!(format!("{quota} 100000"));
            backend_policy["cgroup"]["memory_max"] = json!(memory);
            backend_policy["network"]["mode"] = json!("isolated");
            backend_policy["network"]["ingress"] = json!({"default":"allow"});
            backend_policy["network"]["egress"] = json!({"default":"allow"});
        }
        let body = json!({"sandbox_id":sandbox_id(id),"sandbox_runtime":if spec.runtime_class=="conch"{"conch"}else{"bwrap"},"env":request.envs,"policy_mode":"append","policy":backend_policy});
        let exec = json!({"job_id":"adx-execd","command":self.config.bootstrap.command,"workdir":request.cwd,"env":request.envs});
        Ok((body, exec, request.network_policy))
    }

    async fn write_process(&self, spec: &EnvironmentSpec, id: &str) -> Result<()> {
        if !spec.execution.inherit_entrypoint {
            return Ok(());
        }
        let image = &self.config.classes[&spec.runtime_class].images[&spec.image];
        let process = image
            .process
            .as_ref()
            .ok_or_else(|| Error::Invalid("process binding missing".into()))?;
        let dir = self.files(id);
        use std::os::unix::fs::PermissionsExt;
        std::fs::create_dir_all(&dir).map_err(unavailable)?;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))
            .map_err(unavailable)?;
        let payload =
            json!({"version":1,"args":process.args,"cwd":process.cwd,"user":process.user});
        let file = dir.join("process.json");
        let bytes = serde_json::to_vec(&payload).map_err(unavailable)?;
        let mut out = std::fs::File::create(&file).map_err(unavailable)?;
        use std::io::Write;
        out.write_all(&bytes).map_err(unavailable)?;
        out.sync_all().map_err(unavailable)?;
        Ok(())
    }

    async fn start_owned(
        &self,
        spec: EnvironmentSpec,
        id: String,
        generation: u64,
        devices: Vec<DeviceAllocation>,
        deadline: Instant,
    ) -> Result<IpAddr> {
        let result = self
            .start_backend(&spec, &id, generation, &devices, deadline)
            .await;
        if result.is_err() {
            self.store.update(&id, |r| r.phase = Phase::Failed).await?;
            if let Err(error) = self.cleanup(&id).await {
                adx_observability::warn!(runtime_id=%id,error=%error,"Jiuwenbox failed start cleanup pending");
            }
        }
        result
    }
    async fn still_starting(&self, id: &str, deadline: Instant) -> Result<()> {
        if Instant::now() >= deadline {
            return Err(Error::DeadlineExceeded(
                "Jiuwenbox start budget exhausted".into(),
            ));
        }
        if self
            .store
            .get(id)
            .await?
            .is_none_or(|r| r.phase != Phase::Creating)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    async fn start_backend(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        deadline: Instant,
    ) -> Result<IpAddr> {
        self.still_starting(id, deadline).await?;
        let _mount = self.mount_gate.lock().await;
        let root = self
            .host
            .prepare(&self.config.bootstrap, &self.root())
            .await?;
        drop(_mount);
        crate::managed_mounts::prepare(spec).await?;
        self.write_process(spec, id).await?;
        let (body, exec, network) = self.plan(spec, id, generation, devices, &root)?;
        self.still_starting(id, deadline).await?;
        self.store.update(id, |r| r.create_sent = true).await?;
        let response = self
            .client
            .request(Method::POST, "/api/v1/sandboxes", Some(body), true)
            .await?;
        if response["phase"] != "ready" {
            return Err(unavailable("backend create did not reach ready"));
        }
        let ip: IpAddr = response["ip_address"]
            .as_str()
            .ok_or_else(|| unavailable("backend did not return an isolated IP"))?
            .parse()
            .map_err(unavailable)?;
        if !ip.is_ipv4() || ip.is_loopback() || ip.is_unspecified() {
            return Err(unavailable("invalid isolated backend address"));
        }
        let pid = response["pid"].as_u64().and_then(|v| u32::try_from(v).ok());
        self.store
            .update(id, move |r| {
                r.ip = Some(ip);
                r.pid = pid;
            })
            .await?;
        self.still_starting(id, deadline).await?;
        self.host.network(id, ip, network.as_ref(), false).await?;
        let response = self
            .client
            .request(
                Method::POST,
                &format!("/api/v1/sandboxes/{}/exec_background", sandbox_id(id)),
                Some(exec),
                true,
            )
            .await?;
        if response["started"] != true {
            return Err(unavailable("Execd background launch failed"));
        }
        self.still_starting(id, deadline).await?;
        self.store
            .update(id, |r| {
                if r.phase == Phase::Creating {
                    r.phase = Phase::Ready;
                }
            })
            .await?;
        Ok(ip)
    }

    async fn cleanup(&self, id: &str) -> Result<()> {
        let Some(record) = self.store.get(id).await? else {
            return Ok(());
        };
        if record.phase == Phase::Deleted {
            return self.release_if_idle().await;
        }
        let epoch = self.client.epoch().await?;
        let path = format!("/api/v1/sandboxes/{}", record.sandbox_id);
        let observed = self.client.request(Method::GET, &path, None, false).await;
        match observed {
            Ok(ref value) if value["phase"] == "provisioning" => {
                return Err(unavailable("create remains in flight"))
            }
            Ok(_) => {
                self.client
                    .request(Method::DELETE, &path, None, false)
                    .await?;
            }
            Err(Error::NotFound) if record.epoch != epoch || !record.create_sent => {}
            Err(Error::NotFound)
                if record.phase == Phase::Ready || record.phase == Phase::Deleting => {}
            Err(Error::NotFound) => return Err(unavailable(
                "unobserved create absent in the same backend epoch; restart backend to fence it",
            )),
            Err(e) => return Err(e),
        }
        self.store.update(id, |r| r.create_sent = false).await?;
        if record.backend == "conch" {
            let client = self.conch()?;
            let path = format!("/api/v1/sandboxes/{}", record.sandbox_id);
            match client.request(Method::GET, &path, None, false).await {
                Ok(_) => {
                    client.request(Method::DELETE, &path, None, false).await?;
                }
                Err(Error::NotFound) => {}
                Err(e) => return Err(e),
            }
            match client.request(Method::GET, &path, None, false).await {
                Err(Error::NotFound) => {}
                Ok(_) => return Err(unavailable("Conch delete not complete")),
                Err(e) => return Err(e),
            }
        } else {
            self.host.cleanup_supervisor(&record.sandbox_id).await?;
        }
        if let Some(ip) = record.ip {
            self.host.network(id, ip, None, true).await?;
        }
        self.store.update(id, |r| r.phase = Phase::Deleted).await?;
        match std::fs::remove_dir_all(
            self.config
                .state_dir
                .join("launch")
                .join(&record.sandbox_id),
        ) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(unavailable(e)),
        }
        self.release_if_idle().await
    }

    async fn release_if_idle(&self) -> Result<()> {
        let _mount = self.mount_gate.lock().await;
        let records = self.store.all().await?;
        let hashes: std::collections::BTreeSet<_> = records
            .values()
            .map(|r| r.bootstrap_sha256.clone())
            .chain(std::iter::once(self.config.bootstrap.sha256.clone()))
            .filter(|h| !h.is_empty())
            .collect();
        for hash in hashes {
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(unavailable("invalid stored bootstrap digest"));
            }
            if records
                .values()
                .all(|r| r.phase == Phase::Deleted || r.bootstrap_sha256 != hash)
            {
                self.host
                    .release(&self.config.state_dir.join("bootstrap").join(hash))
                    .await?;
            }
        }
        Ok(())
    }

    async fn observation(&self, r: &Record) -> Result<bool> {
        if r.phase == Phase::Deleted {
            return Ok(false);
        }
        if r.epoch != self.client.epoch().await? {
            return Err(unavailable(
                "backend epoch changed; reconciliation required",
            ));
        }
        if r.phase != Phase::Ready {
            return Err(unavailable("execution has not settled"));
        }
        let ip = r.ip.ok_or_else(|| unavailable("runtime IP missing"))?;
        let port = self
            .launch
            .env
            .get("EXECD_HTTP_PORT")
            .map(String::as_str)
            .unwrap_or("50090");
        let port = port
            .parse::<u16>()
            .map_err(|_| Error::Invalid("invalid Execd HTTP port".into()))?;
        let status = async {
            let status = client::execd_status(
                std::net::SocketAddr::new(ip, port),
                self.launch.env.get("EXECD_HTTP_TOKEN").map(String::as_str),
                self.launch.rpc_timeout,
            )
            .await?;
            if status.identity.environment_id != r.environment_id
                || status.identity.runtime_id != r.runtime_id
                || status.identity.ownership_generation != r.generation
                || status.revision == 0
            {
                return Err(Error::Conflict);
            }
            Ok(true)
        }
        .await;
        match status {
            Ok(v) => Ok(v),
            Err(Error::Conflict) => Err(Error::Conflict),
            Err(e) => {
                // Backend liveness and Execd readiness are distinct. Keep the controller's
                // readiness failure policy active when the backend is demonstrably alive.
                if r.backend == "supervisor" {
                    return Ok(!self.host.supervisor_absent(&r.sandbox_id).await?);
                }
                if r.backend == "conch" {
                    match self
                        .conch()?
                        .request(
                            Method::GET,
                            &format!("/api/v1/sandboxes/{}", r.sandbox_id),
                            None,
                            false,
                        )
                        .await
                    {
                        Err(Error::NotFound) => return Ok(false),
                        Err(error) => return Err(error),
                        Ok(_) => {
                            let running = self
                                .conch()?
                                .request(
                                    Method::GET,
                                    "/api/v1/sandboxes?state=running&limit=5000",
                                    None,
                                    false,
                                )
                                .await?;
                            let running = running
                                .as_array()
                                .ok_or_else(|| unavailable("invalid Conch running inventory"))?;
                            if running.len() >= 5000 {
                                return Err(unavailable(
                                    "Conch running inventory may be truncated",
                                ));
                            }
                            if running.iter().any(|v| {
                                v["sandboxID"].as_str().or_else(|| v["id"].as_str())
                                    == Some(r.sandbox_id.as_str())
                            }) {
                                return Ok(true);
                            }
                        }
                    }
                }
                Err(e)
            }
        }
    }
}

fn sandbox_id(id: &str) -> String {
    format!(
        "adx-{}",
        &format!("{:x}", Sha256::digest(id.as_bytes()))[..28]
    )
}

#[async_trait]
impl RuntimeDriver for Jiuwenbox {
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
    ) -> Result<IpAddr> {
        self.start_with_deadline(
            spec,
            id,
            generation,
            devices,
            Instant::now() + self.launch.rpc_timeout,
        )
        .await
    }
    async fn start_with_deadline(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        devices: &[DeviceAllocation],
        deadline: Instant,
    ) -> Result<IpAddr> {
        if Instant::now() >= deadline {
            return Err(Error::DeadlineExceeded(
                "Jiuwenbox create expired before admission".into(),
            ));
        }
        if !adx_core::valid_runtime_id(&spec.id, generation, id) {
            return Err(Error::Invalid(
                "runtime ID does not match Environment and generation".into(),
            ));
        }
        // Validate every parameter before reserving an ID or touching the host.
        self.plan(spec, id, generation, devices, &self.root())?;
        let gate = self.gate(id);
        let lock = tokio::time::timeout_at(deadline, gate.lock_owned())
            .await
            .map_err(|_| Error::DeadlineExceeded("waiting for prior Jiuwenbox operation".into()))?;
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(spec).map_err(unavailable)?)
        );
        if let Some(r) = self.store.get(id).await? {
            if r.fingerprint != fingerprint || r.generation != generation {
                return Err(Error::Conflict);
            }
            return if r.phase == Phase::Ready && r.epoch == self.client.epoch().await? {
                r.ip.ok_or_else(|| unavailable("runtime address missing"))
            } else {
                Err(Error::Conflict)
            };
        }
        self.store
            .insert(Record {
                runtime_id: id.into(),
                environment_id: spec.id.clone(),
                tenant_id: spec.tenant_id.clone(),
                generation,
                sandbox_id: sandbox_id(id),
                backend: spec.runtime_class.clone(),
                epoch: self.client.epoch().await?,
                fingerprint,
                phase: Phase::Creating,
                ip: None,
                pid: None,
                create_sent: false,
                bootstrap_sha256: self.config.bootstrap.sha256.clone(),
            })
            .await?;
        let driver = self.clone();
        let owned_spec = spec.clone();
        let owned_id = id.to_owned();
        let owned_devices = devices.to_vec();
        let task = tokio::spawn(async move {
            let _lock = lock;
            driver
                .start_owned(owned_spec, owned_id, generation, owned_devices, deadline)
                .await
        });
        match tokio::time::timeout_at(deadline, task).await {
            Ok(result) => result.map_err(unavailable)?,
            Err(_) => {
                self.store
                    .update(id, |r| {
                        if r.phase == Phase::Creating {
                            r.phase = Phase::Failed;
                        }
                    })
                    .await?;
                Err(Error::DeadlineExceeded(
                    "Jiuwenbox create deadline; backend cleanup remains tracked".into(),
                ))
            }
        }
    }
    async fn is_running(&self, id: &str) -> Result<bool> {
        match self.store.get(id).await? {
            Some(r) => self.observation(&r).await,
            None => Ok(false),
        }
    }
    async fn observe(&self, id: &str) -> Result<RuntimeStatus> {
        Ok(RuntimeStatus {
            running: self.is_running(id).await?,
            failure: None,
        })
    }
    async fn inventory(&self) -> Result<Vec<RuntimeObservation>> {
        let current = self.client.epoch().await?;
        let actual = self.list().await?;
        let mut records = self.store.all().await?;
        let mut seen = std::collections::BTreeSet::new();
        for value in &actual {
            let id = value["id"]
                .as_str()
                .ok_or_else(|| unavailable("Jiuwenbox inventory record has no ID"))?;
            seen.insert(id.to_owned());
        }
        if let Some(native) = &self.conch {
            let listed = native
                .request(Method::GET, "/api/v1/sandboxes?limit=5000", None, false)
                .await?;
            let listed = listed
                .as_array()
                .ok_or_else(|| unavailable("invalid Conch inventory"))?;
            if listed.len() >= 5000 {
                return Err(unavailable("Conch inventory may be truncated"));
            }
            for value in listed {
                let id = value["sandboxID"]
                    .as_str()
                    .or_else(|| value["id"].as_str())
                    .ok_or_else(|| unavailable("Conch inventory record has no ID"))?;
                seen.insert(id.to_owned());
            }
        }
        for id in seen.iter().filter(|id| id.starts_with("adx-")) {
            if !records.values().any(|r| r.sandbox_id == *id) {
                return Err(unavailable("managed backend absent from ownership journal"));
            }
        }
        for r in records
            .values_mut()
            .filter(|r| r.phase == Phase::Deleted && seen.contains(&r.sandbox_id))
        {
            if r.backend.is_empty() {
                return Err(unavailable("unexpected backend for an unowned tombstone"));
            }
            *r = self
                .store
                .update(&r.runtime_id, |r| r.phase = Phase::Deleting)
                .await?;
        }
        let mut observations = vec![];
        for r in records.values().filter(|r| r.phase != Phase::Deleted) {
            if r.phase == Phase::Creating {
                let gate = self.gate(&r.runtime_id);
                if let Ok(_lock) = gate.try_lock() {
                    self.cleanup(&r.runtime_id).await?;
                    continue;
                };
            }
            if r.epoch != current
                || matches!(r.phase, Phase::Failed | Phase::Uncertain | Phase::Deleting)
            {
                let gate = self.gate(&r.runtime_id);
                let _lock = gate
                    .try_lock()
                    .map_err(|_| unavailable("start cleanup is still executing"))?;
                self.cleanup(&r.runtime_id).await?;
                continue;
            }
            observations.push(RuntimeObservation {
                environment_id: r.environment_id.clone(),
                runtime_id: r.runtime_id.clone(),
                tenant_id: r.tenant_id.clone(),
                generation: r.generation,
                running: if r.phase == Phase::Creating {
                    false
                } else {
                    self.observation(r).await?
                },
            });
        }
        self.release_if_idle().await?;
        Ok(observations)
    }
    async fn remove(&self, id: &str) -> Result<()> {
        let gate = self.gate(id);
        let _lock = gate.lock().await;
        if let Some(r) = self.store.get(id).await? {
            if r.phase == Phase::Ready {
                self.store.update(id, |r| r.phase = Phase::Deleting).await?;
            }
            self.cleanup(id).await?;
        } else {
            // A delete before start fences future reuse, even when no backend exists.
            self.store
                .insert(Record {
                    runtime_id: id.into(),
                    environment_id: String::new(),
                    tenant_id: String::new(),
                    generation: 0,
                    sandbox_id: sandbox_id(id),
                    backend: String::new(),
                    epoch: String::new(),
                    fingerprint: String::new(),
                    phase: Phase::Deleted,
                    ip: None,
                    pid: None,
                    create_sent: false,
                    bootstrap_sha256: String::new(),
                })
                .await?;
        }
        Ok(())
    }
    async fn set_network_policy(
        &self,
        id: &str,
        network: Option<&NetworkPolicy>,
        ports: &[u16],
    ) -> Result<()> {
        policy::validate_network(network)?;
        let gate = self.gate(id);
        let _lock = gate.lock().await;
        let r = self.store.get(id).await?.ok_or(Error::NotFound)?;
        if r.phase != Phase::Ready {
            return Err(Error::Conflict);
        }
        let ip = r.ip.ok_or_else(|| unavailable("runtime IP missing"))?;
        let policy = network
            .map(|n| sandboxd::environment_network_policy(n, ports, &self.launch))
            .transpose()?;
        self.host.network(id, ip, policy.as_ref(), false).await
    }
}
