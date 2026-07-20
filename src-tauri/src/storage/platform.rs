//! OS integration: revealing paths in the desktop file manager, opening files
//! with their default handler, and the local-filesystem effects of sync
//! operations (trash, rename, empty-directory pruning).
//!
//! Nothing here knows about sync state — it takes paths and acts on them.

use std::path::{Component, Path, PathBuf};

use super::profile_store::SyncPair;

#[allow(unreachable_code)]
pub(crate) fn reveal_in_file_manager(path: &Path, highlight_file: bool) -> Result<(), String> {
    #[cfg(target_os = "windows")]
    {
        let mut command = std::process::Command::new("explorer");
        if highlight_file {
            let target = path.canonicalize().map_err(|error| {
                format!(
                    "failed to canonicalize '{}' for reveal: {error}",
                    path.display()
                )
            })?;
            let mut args = std::ffi::OsString::from("/select,");
            args.push(&target);
            command.arg(args);
        } else {
            command.arg(path);
        }

        command.spawn().map_err(|error| {
            format!(
                "failed to reveal '{}' in the file manager: {error}",
                path.display()
            )
        })?;

        return Ok(());
    }

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut cmd = std::process::Command::new("open");
        if highlight_file {
            cmd.arg("-R").arg(path);
        } else {
            cmd.arg(path);
        }
        cmd
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut cmd = std::process::Command::new("xdg-open");
        let target = if highlight_file {
            path.parent().unwrap_or(path)
        } else {
            path
        };
        cmd.arg(target);
        cmd
    };

    #[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
    command.spawn().map_err(|error| {
        format!(
            "failed to reveal '{}' in the file manager: {error}",
            path.display()
        )
    })?;

    Ok(())
}

pub(crate) fn open_path_with_default_app(path: &Path) -> Result<(), String> {
    // Use `explorer <path>` rather than `cmd /C start`: cmd.exe re-parses its
    // command line, so a path containing shell metacharacters (`&`, `^`, `%`)
    // could execute. `explorer` receives the path as a single argument and
    // opens it with its default handler without a shell round-trip.
    #[cfg(target_os = "windows")]
    let mut command = {
        let mut cmd = std::process::Command::new("explorer");
        cmd.arg(path);
        cmd
    };

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut cmd = std::process::Command::new("open");
        cmd.arg(path);
        cmd
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut cmd = std::process::Command::new("xdg-open");
        cmd.arg(path);
        cmd
    };

    command.spawn().map_err(|error| {
        format!(
            "failed to open '{}' with the default app: {error}",
            path.display()
        )
    })?;

    Ok(())
}

/// Move a local file to the OS trash. A propagated delete must always be
/// recoverable, so this never hard-unlinks (backlog phase 1, ADR-2b).
pub(crate) fn trash_local_file_for_pair(pair: &SyncPair, path: &str) -> Result<(), String> {
    let local_path = resolve_local_download_path(&pair.local_folder, path)?;

    match std::fs::symlink_metadata(&local_path) {
        Ok(_) => trash::delete(&local_path).map_err(|error| {
            format!(
                "failed to move '{}' to the trash: {error}",
                local_path.display()
            )
        })?,
        // Already gone: the delete is satisfied.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "failed to inspect '{}' before deleting: {error}",
                local_path.display()
            ));
        }
    }

    cleanup_empty_ancestors(&local_path, Path::new(&pair.local_folder));
    Ok(())
}

/// Rename a local file, creating the destination's parent directories.
pub(crate) fn rename_local_file_for_pair(
    pair: &SyncPair,
    from: &str,
    to: &str,
) -> Result<(), String> {
    let source = resolve_local_download_path(&pair.local_folder, from)?;
    let destination = resolve_local_download_path(&pair.local_folder, to)?;

    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create directory for '{}': {error}",
                destination.display()
            )
        })?;
    }

    std::fs::rename(&source, &destination).map_err(|error| {
        format!(
            "failed to rename '{}' to '{}': {error}",
            source.display(),
            destination.display()
        )
    })?;

    cleanup_empty_ancestors(&source, Path::new(&pair.local_folder));
    Ok(())
}

#[cfg(test)]
pub(crate) fn remove_local_file_without_trash_for_pair(
    pair: &SyncPair,
    path: &str,
) -> Result<(), String> {
    let local_path = resolve_local_download_path(&pair.local_folder, path)?;
    match std::fs::remove_file(&local_path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "failed to remove '{}': {error}",
                local_path.display()
            ))
        }
    }
    cleanup_empty_ancestors(&local_path, Path::new(&pair.local_folder));
    Ok(())
}

/// Removes empty directories from `file_path`'s parent up to (but not including) `root`.
/// Stops as soon as a directory is non-empty or cannot be removed.
pub(crate) fn cleanup_empty_ancestors(file_path: &Path, root: &Path) {
    let mut current = file_path.parent();
    while let Some(dir) = current {
        if dir == root {
            break;
        }
        // remove_dir only succeeds on empty directories
        if std::fs::remove_dir(dir).is_err() {
            break;
        }
        current = dir.parent();
    }
}

pub(crate) fn resolve_local_upload_path(
    root: &str,
    relative_path: &str,
) -> Result<PathBuf, String> {
    let relative = Path::new(relative_path);
    let mut resolved = PathBuf::from(root);

    for component in relative.components() {
        match component {
            Component::Normal(part) => resolved.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "planned upload path '{relative_path}' is not a safe relative file path"
                ));
            }
        }
    }

    Ok(resolved)
}

pub(crate) fn resolve_local_download_path(
    root: &str,
    relative_path: &str,
) -> Result<PathBuf, String> {
    let relative = Path::new(relative_path);
    let mut resolved = PathBuf::from(root);

    for component in relative.components() {
        match component {
            Component::Normal(part) => resolved.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(format!(
                    "planned download path '{relative_path}' is not a safe relative file path"
                ));
            }
        }
    }

    Ok(resolved)
}

pub(crate) fn remove_local_directory_subtree(
    root: &str,
    relative_path: &str,
) -> Result<(), String> {
    let local_path = resolve_local_download_path(root, relative_path)?;

    match std::fs::remove_dir_all(&local_path) {
        Ok(()) => {
            cleanup_empty_ancestors(&local_path, Path::new(root));
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            cleanup_empty_ancestors(&local_path, Path::new(root));
            Ok(())
        }
        Err(error) => Err(format!(
            "Failed to remove '{}': {error}",
            local_path.display()
        )),
    }
}

pub(crate) fn normalize_directory_delete_path(path: &str) -> Result<String, String> {
    let normalized = path.replace('\\', "/").trim_matches('/').to_string();
    if normalized.is_empty() {
        return Err("Folder delete requires a non-empty relative path.".into());
    }

    resolve_local_download_path(".", &normalized)?;
    Ok(normalized)
}
