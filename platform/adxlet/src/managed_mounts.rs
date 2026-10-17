//! Request-scoped creation of Local bind source directories.
use adx_core::{execution::StorageSource, EnvironmentSpec, Error, Result};
use std::fs::{DirBuilder, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Component, Path, PathBuf};

fn unavailable(error: impl std::fmt::Display) -> Error {
    Error::Unavailable(format!("Local mount directory: {error}"))
}

/// Prepare explicitly opted-in sources before start or restore. No Agent or user
/// identity is interpreted here. Existing directories retain data and ownership.
/// Missing parents, symlinks and I/O failures return an error; only the final
/// directory is created, so a missing storage mount cannot become a directory tree.
pub async fn prepare(spec: &EnvironmentSpec) -> Result<()> {
    let mut sources = Vec::new();
    for mount in &spec.execution.mounts {
        if mount.create_if_missing {
            mount.validate()?;
            if let StorageSource::Local(source) = &mount.source {
                sources.push(PathBuf::from(source));
            }
        }
    }
    if sources.is_empty() {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || {
        for source in sources {
            prepare_directory(&source)?;
        }
        Ok(())
    })
    .await
    .map_err(unavailable)?
}

fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(path)
}

fn prepare_directory(source: &Path) -> Result<()> {
    if !source.is_absolute()
        || source
            .components()
            .any(|part| !matches!(part, Component::RootDir | Component::Normal(_)))
    {
        return Err(Error::Invalid(
            "Local directory creation requires an absolute path without traversal".into(),
        ));
    }
    let parent = source
        .parent()
        .ok_or_else(|| Error::Invalid("Local source must have a parent directory".into()))?;
    let name = source
        .file_name()
        .ok_or_else(|| Error::Invalid("Local source directory name required".into()))?;
    let mut directory = open_directory(Path::new("/")).map_err(unavailable)?;
    for component in parent.components() {
        if let Component::Normal(name) = component {
            // The held descriptor anchors each lookup. O_NOFOLLOW prevents
            // symlink traversal in both parents and the final source directory.
            let child =
                PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name);
            directory = open_directory(&child).map_err(unavailable)?;
        }
    }
    let child = PathBuf::from(format!("/proc/self/fd/{}", directory.as_raw_fd())).join(name);
    match open_directory(&child) {
        Ok(_) => return Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(unavailable(error)),
    }
    match DirBuilder::new().mode(0o755).create(&child) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(unavailable(error)),
    }
    open_directory(&child).map_err(unavailable)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::os::unix::fs::symlink;

    #[test]
    fn first_use_creates_directory_and_reuse_preserves_data() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("w-first");
        prepare_directory(&source).unwrap();
        assert!(source.is_dir());
        fs::write(source.join("state"), "persistent").unwrap();
        prepare_directory(&source).unwrap();
        assert_eq!(
            fs::read_to_string(source.join("state")).unwrap(),
            "persistent"
        );
    }

    #[test]
    fn missing_parent_is_not_created() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("unmounted/users/w-first");
        assert!(prepare_directory(&source).is_err());
        assert!(!temp.path().join("unmounted").exists());
    }

    #[test]
    fn rejects_symlink_parents_sources_files_and_traversal() {
        let temp = tempfile::tempdir().unwrap();
        let outside = temp.path().join("outside");
        fs::create_dir(&outside).unwrap();
        symlink(&outside, temp.path().join("link")).unwrap();
        fs::write(temp.path().join("file"), "data").unwrap();
        for source in [
            temp.path().join("link"),
            temp.path().join("link/new"),
            temp.path().join("file"),
            temp.path().join("../escape"),
        ] {
            assert!(prepare_directory(&source).is_err());
        }
        assert_eq!(fs::read_dir(outside).unwrap().count(), 0);
    }

    #[test]
    fn concurrent_preparation_is_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        prepare_directory(&temp.path().join("shared")).unwrap();
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
        });
        assert!(temp.path().join("shared").is_dir());
    }

    #[tokio::test]
    async fn creation_requires_explicit_mount_opt_in() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("workspace");
        let mut spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"test", "tenant_id":"test", "image":"image", "runtime_class":"runsc",
            "resources":{"cpu_millis":1000,"memory_bytes":1048576,"disk_bytes":0}, "priority":0
        }))
        .unwrap();
        spec.execution.mounts.push(adx_core::execution::Mount {
            kind: "bind".into(),
            target: "/data".into(),
            options: vec!["rw".into()],
            source: StorageSource::Local(path.to_str().unwrap().into()),
            create_if_missing: false,
        });
        prepare(&spec).await.unwrap();
        assert!(!path.exists());
        spec.execution.mounts[0].create_if_missing = true;
        prepare(&spec).await.unwrap();
        assert!(path.is_dir());
    }
}
