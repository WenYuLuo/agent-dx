//! Independent mount lifecycle manager for mkdir-triggered workspace bind mounts.
//!
//! Follows RFC 0001 §5: OwnerFs owns authoritative root state and P2P;
//! MountManager owns native egress creation, validation, and teardown.
//! Per-root entries are independent; same-root operations serialize via
//! the entry state mutex. `request_mount` is non-blocking (RFC 0001 §2).

use std::{
    collections::HashMap,
    ffi::OsString,
    path::PathBuf,
    sync::{Arc, Mutex, Weak},
    thread,
};

use afs_error::Result;

use super::{bind_mount, root::RootId, OwnerFs};

/// Manages per-root native bind mounts triggered by mkdir.
///
/// Each root gets an independent [`MountEntry`]; different roots never
/// block each other. Same-root operations serialize through the entry's
/// state mutex, preventing stacked mounts from repeated requests.
pub(in crate::node) struct MountManager {
    entries: Mutex<HashMap<RootId, Arc<MountEntry>>>,
    fuse_mount_path: Option<PathBuf>,
    owner: Mutex<Weak<OwnerFs>>,
}

struct MountEntry {
    state: Mutex<MountState>,
    root_id: RootId,
    name: OsString,
}

enum MountState {
    /// Backing directory ready; shared cache policy enabled.
    FuseReady,
    /// Async worker is binding in the recorded namespace.
    Mounting,
    /// Bind mount live; new paths resolve native.
    #[cfg(target_os = "linux")]
    NativeActive {
        bind: Box<bind_mount::AuthorizedWorkspaceBind>,
    },
    /// Bind failed; root remains fully accessible via FUSE (D2 fallback).
    #[allow(dead_code)]
    FuseOnly { reason: String },
}

fn poisoned() -> afs_error::Error {
    afs_error::Error::coded(afs_error::RUNTIME_INTERNAL, "MountManager state poisoned")
}

impl MountManager {
    /// Create a new MountManager. `fuse_mount_path` is the FUSE mount point
    /// under which workspace roots are created (D5). When `None`, all
    /// requests fall back to FUSE_ONLY.
    pub(in crate::node) fn new(fuse_mount_path: Option<PathBuf>) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            fuse_mount_path,
            owner: Mutex::new(Weak::new()),
        }
    }

    /// Set the weak back-reference to the owning OwnerFs. Called once after
    /// construction so workers can reach `native_home_export_for_current_namespace`.
    pub(in crate::node) fn attach_owner(&self, owner: &Arc<OwnerFs>) {
        let mut slot = self.owner.lock().unwrap_or_else(|e| e.into_inner());
        *slot = Arc::downgrade(owner);
    }

    /// Request an asynchronous native bind mount for the given root.
    ///
    /// Non-blocking: spawns a worker thread that prepares and activates the
    /// bind. Idempotent: repeated requests for an already-mounted or
    /// mounting root are no-ops. Failures only log (D2 fallback to FUSE).
    pub(in crate::node) fn request_mount(
        &self,
        root_id: RootId,
        name: OsString,
    ) -> Result<()> {
        let entry = {
            let mut entries = self.entries.lock().map_err(|_| poisoned())?;
            entries
                .entry(root_id.clone())
                .or_insert_with(|| {
                    Arc::new(MountEntry {
                        state: Mutex::new(MountState::FuseReady),
                        root_id: root_id.clone(),
                        name: name.clone(),
                    })
                })
                .clone()
        };

        // Check current state under the entry lock; skip if already active or mounting.
        {
            let mut state = entry.state.lock().map_err(|_| poisoned())?;
            match &*state {
                MountState::Mounting => return Ok(()),
                #[cfg(target_os = "linux")]
                MountState::NativeActive { .. } => return Ok(()),
                MountState::FuseReady | MountState::FuseOnly { .. } => {
                    *state = MountState::Mounting;
                }
            }
        }

        self.spawn_mount_worker(entry);
        Ok(())
    }

    /// Shut down all active mounts: detach every live bind and clear entries.
    ///
    /// Called from the Node shutdown path after the FUSE session ends.
    /// Without this call, active binds keep a strong OwnerFs reference and
    /// prevent clean drop.
    pub(in crate::node) fn shutdown(&self) {
        let entries: Vec<Arc<MountEntry>> = {
            let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
            entries.drain().map(|(_, v)| v).collect()
        };
        for entry in entries {
            Self::detach_entry(&entry);
        }
    }

    #[cfg(target_os = "linux")]
    fn detach_entry(entry: &MountEntry) {
        let mut state = match entry.state.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        let taken = std::mem::replace(
            &mut *state,
            MountState::FuseOnly {
                reason: "shutdown".to_string(),
            },
        );
        match taken {
            MountState::NativeActive { mut bind } => {
                if let Err(error) = bind.detach() {
                    afs_logging::warn!(
                        "ownerfs.mount_manager.detach_failed";
                        "root_id" => ?entry.root_id, "error" => error.to_string()
                    );
                }
            }
            other => {
                *state = other;
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn detach_entry(_entry: &MountEntry) {}

    fn spawn_mount_worker(&self, entry: Arc<MountEntry>) {
        let owner = match self
            .owner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .upgrade()
        {
            Some(o) => o,
            None => {
                Self::set_fuse_only(&entry, "OwnerFs dropped before mount");
                return;
            }
        };
        let mount_path = match &self.fuse_mount_path {
            Some(p) => p.clone(),
            None => {
                Self::set_fuse_only(&entry, "no FUSE mount path configured");
                return;
            }
        };

        if let Err(error) = thread::Builder::new()
            .name(format!("afs-mount-{}", entry.root_id.0))
            .spawn(move || {
                Self::mount_worker_loop(owner, mount_path, entry);
            })
        {
            afs_logging::warn!(
                "ownerfs.mount_manager.spawn_failed";
                "error" => error.to_string()
            );
        }
    }

    #[cfg(target_os = "linux")]
    fn mount_worker_loop(owner: Arc<OwnerFs>, mount_path: PathBuf, entry: Arc<MountEntry>) {
        let workspace = entry.name.clone();
        let mut bind = match bind_mount::AuthorizedWorkspaceBind::prepare(
            owner,
            &mount_path,
            &workspace,
        ) {
            Ok(bind) => bind,
            Err(error) => {
                Self::set_fuse_only(&entry, format!("prepare failed: {error}"));
                return;
            }
        };
        if let Err(error) = bind.activate() {
            Self::set_fuse_only(&entry, format!("activate failed: {error}"));
            return;
        }

        afs_logging::info!(
            "ownerfs.mount_manager.native_active";
            "root_id" => ?entry.root_id, "workspace" => ?entry.name
        );

        let mut state = match entry.state.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        match &*state {
            MountState::Mounting => {
                *state = MountState::NativeActive { bind: Box::new(bind) };
            }
            MountState::FuseOnly { .. } => {
                // Shutdown raced ahead; detach the bind we just activated.
                drop(state);
                let _ = bind.detach();
            }
            _ => {
                *state = MountState::NativeActive { bind: Box::new(bind) };
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn mount_worker_loop(_owner: Arc<OwnerFs>, _mount_path: PathBuf, entry: Arc<MountEntry>) {
        Self::set_fuse_only(&entry, "bind mount is only supported on Linux");
    }

    fn set_fuse_only(entry: &MountEntry, reason: impl Into<String>) {
        let reason = reason.into();
        afs_logging::warn!(
            "ownerfs.mount_manager.fuse_only";
            "root_id" => ?entry.root_id, "reason" => &reason
        );
        let mut state = match entry.state.lock() {
            Ok(s) => s,
            Err(e) => e.into_inner(),
        };
        // Don't overwrite NativeActive if a previous bind succeeded.
        #[cfg(target_os = "linux")]
        {
            if matches!(&*state, MountState::NativeActive { .. }) {
                return;
            }
        }
        *state = MountState::FuseOnly { reason };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsStr;
    use std::time::{Duration, Instant};

    fn root_id(name: &str) -> RootId {
        RootId(name.to_owned())
    }

    fn entry_state(manager: &MountManager, id: &RootId) -> Option<MountState> {
        let entries = manager.entries.lock().unwrap();
        let entry = entries.get(id)?;
        let state = entry.state.lock().unwrap();
        Some(match &*state {
            MountState::FuseReady => MountState::FuseReady,
            MountState::Mounting => MountState::Mounting,
            #[cfg(target_os = "linux")]
            MountState::NativeActive { .. } => {
                panic!("cannot snapshot NativeActive without a live bind")
            }
            MountState::FuseOnly { reason } => MountState::FuseOnly {
                reason: reason.clone(),
            },
        })
    }

    #[test]
    fn request_mount_without_owner_falls_back_to_fuse_only() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&root_id("agent1")).unwrap();
        assert_eq!(entry.root_id, root_id("agent1"));
        assert_eq!(entry.name.as_os_str(), OsStr::new("agent1"));
        match &*entry.state.lock().unwrap() {
            MountState::FuseOnly { reason } => assert!(
                reason.contains("OwnerFs dropped before mount"),
                "unexpected reason: {reason}"
            ),
            _ => panic!("expected FuseOnly fallback without an attached OwnerFs"),
        }
    }

    #[test]
    fn request_mount_without_mount_path_falls_back_to_fuse_only() {
        let (_temp, owner, _ctx, _disk) =
            crate::node::vfs::ownerfs::native_home_tests::fixture(true);
        let owner = Arc::new(owner);
        let manager = MountManager::new(None);
        manager.attach_owner(&owner);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&root_id("agent1")).unwrap();
        match &*entry.state.lock().unwrap() {
            MountState::FuseOnly { reason } => assert!(
                reason.contains("no FUSE mount path configured"),
                "unexpected reason: {reason}"
            ),
            _ => panic!("expected FuseOnly fallback without a FUSE mount path"),
        }
    }

    #[test]
    fn request_mount_is_idempotent_while_mounting() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        let entry = manager
            .entries
            .lock()
            .unwrap()
            .get(&root_id("agent1"))
            .unwrap()
            .clone();
        {
            let mut state = entry.state.lock().unwrap();
            *state = MountState::Mounting;
        }

        // A second request while Mounting must not spawn a second worker.
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();

        // No owner and no mount path mean any spawned worker would have flipped
        // the state synchronously; it staying Mounting proves the no-op path.
        std::thread::sleep(Duration::from_millis(50));
        assert!(matches!(*entry.state.lock().unwrap(), MountState::Mounting));
    }

    #[test]
    fn request_mount_for_distinct_roots_keeps_independent_entries() {
        let manager = MountManager::new(None);
        for name in ["agent1", "agent2"] {
            manager
                .request_mount(root_id(name), OsString::from(name))
                .unwrap();
        }

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 2);
        for name in ["agent1", "agent2"] {
            let entry = entries.get(&root_id(name)).expect("independent entry");
            assert_eq!(entry.root_id, root_id(name));
            assert_eq!(entry.name.as_os_str(), OsStr::new(name));
            assert!(matches!(
                *entry.state.lock().unwrap(),
                MountState::FuseOnly { .. }
            ));
        }
    }

    #[test]
    fn request_mount_for_same_root_reuses_entry() {
        let manager = MountManager::new(None);
        manager
            .request_mount(root_id("agent1"), OsString::from("agent1"))
            .unwrap();
        manager
            .request_mount(root_id("agent1"), OsString::from("renamed"))
            .unwrap();

        let entries = manager.entries.lock().unwrap();
        assert_eq!(entries.len(), 1);
        let entry = entries.get(&root_id("agent1")).unwrap();
        // The first name wins; a repeated request must not create a new entry.
        assert_eq!(entry.name.as_os_str(), OsStr::new("agent1"));
        assert!(matches!(
            *entry.state.lock().unwrap(),
            MountState::FuseOnly { .. }
        ));
    }

    #[test]
    fn shutdown_drains_all_entries() {
        let manager = MountManager::new(None);
        for name in ["agent1", "agent2"] {
            manager
                .request_mount(root_id(name), OsString::from(name))
                .unwrap();
        }
        assert_eq!(manager.entries.lock().unwrap().len(), 2);

        manager.shutdown();
        assert!(manager.entries.lock().unwrap().is_empty());
    }

    /// mkdir must return before the bind completes (RFC 0001 §2) and must not
    /// fail when the bind falls back to FUSE_ONLY (D2). The MountManager worker
    /// runs in the background and settles the entry to FuseOnly when the FUSE
    /// mount path is unusable.
    #[cfg(target_os = "linux")]
    #[test]
    fn mkdir_triggers_request_mount_and_bind_failure_does_not_fail_mkdir() {
        use super::super::{
            native_home_tests::fixture_with_mount_manager, root::root_id_from_name, Backend,
        };

        // A FUSE mount point that will never exist, so bind prepare must fail.
        let fuse_mount = tempfile::tempdir().unwrap();
        let fuse_mount_path = fuse_mount.path().join("fuse");
        let (_temp, owner, ctx) = fixture_with_mount_manager(fuse_mount_path);
        let owner = Arc::new(owner);
        owner.mount_manager().unwrap().attach_owner(&owner);
        let manager = owner.mount_manager().unwrap().clone();
        let expected_root_id = root_id_from_name(OsStr::new("agent1")).unwrap();

        // mkdir itself must succeed without waiting for the bind.
        let owner_fs: &OwnerFs = &owner;
        let entry = Backend::mkdir(
            owner_fs,
            &ctx,
            owner.root_inode(),
            OsStr::new("agent1"),
            0o755,
        )
        .expect("mkdir must not depend on bind mount success");
        assert!(entry.inode.value != 0);

        // The entry is registered synchronously; it must not be NATIVE_ACTIVE
        // because the FUSE mount path does not exist.
        let initial = entry_state(&manager, &expected_root_id);
        assert!(matches!(
            initial,
            Some(MountState::Mounting | MountState::FuseOnly { .. })
        ));

        // The worker eventually settles the entry to FuseOnly.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match entry_state(&manager, &expected_root_id) {
                Some(MountState::FuseOnly { .. }) => break,
                Some(MountState::Mounting) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                _state => {
                    assert!(
                        Instant::now() < deadline,
                        "bind worker did not settle to FuseOnly"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        // The fallback must carry the bind failure reason, not a shutdown path.
        match entry_state(&manager, &expected_root_id) {
            Some(MountState::FuseOnly { reason }) => assert!(
                reason.contains("prepare failed"),
                "unexpected fallback reason: {reason}"
            ),
            _ => panic!("expected FuseOnly with prepare failure"),
        }
    }
}
