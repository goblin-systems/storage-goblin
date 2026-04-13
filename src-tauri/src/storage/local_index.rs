use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tauri::{AppHandle, Runtime};

use super::{app_storage_path, now_iso, system_time_to_iso, LOCAL_INDEX_FILE_NAME};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LocalIndexSummary {
    pub indexed_at: String,
    pub file_count: u64,
    pub directory_count: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalIndexEntry {
    pub relative_path: String,
    pub kind: String,
    pub size: u64,
    pub modified_at: Option<String>,
    #[serde(default)]
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalIndexSnapshot {
    pub version: u32,
    pub root_folder: String,
    pub summary: LocalIndexSummary,
    pub entries: Vec<LocalIndexEntry>,
}

pub fn read_local_index_snapshot<R: Runtime>(
    app: &AppHandle<R>,
) -> Result<Option<LocalIndexSnapshot>, String> {
    let path = app_storage_path(app, LOCAL_INDEX_FILE_NAME)?;
    if !path.exists() {
        return Ok(None);
    }

    read_local_index_snapshot_file(&path).map(Some)
}

pub fn write_local_index_snapshot<R: Runtime>(
    app: &AppHandle<R>,
    snapshot: &LocalIndexSnapshot,
) -> Result<(), String> {
    let path = app_storage_path(app, LOCAL_INDEX_FILE_NAME)?;
    write_local_index_snapshot_file(&path, snapshot)
}

pub fn read_local_index_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair_id: &str,
) -> Result<Option<LocalIndexSnapshot>, String> {
    let file_name = local_index_file_name_for_pair(pair_id);
    let path = app_storage_path(app, &file_name)?;
    if !path.exists() {
        return Ok(None);
    }

    read_local_index_snapshot_file(&path).map(Some)
}

pub fn write_local_index_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair_id: &str,
    snapshot: &LocalIndexSnapshot,
) -> Result<(), String> {
    let file_name = local_index_file_name_for_pair(pair_id);
    let path = app_storage_path(app, &file_name)?;
    write_local_index_snapshot_file(&path, snapshot)
}

pub fn scan_local_folder(root: &Path) -> Result<LocalIndexSnapshot, String> {
    ensure_scannable_root(root)?;

    let mut entries = Vec::new();
    let mut summary = LocalIndexSummary {
        indexed_at: now_iso(),
        file_count: 0,
        directory_count: 0,
        total_bytes: 0,
    };

    scan_directory_recursive(root, root, &mut entries, &mut summary)?;

    Ok(LocalIndexSnapshot {
        version: 2,
        root_folder: root.to_string_lossy().into_owned(),
        summary,
        entries,
    })
}

pub(crate) fn snapshot_matches_folder(snapshot: &LocalIndexSnapshot, folder: &str) -> bool {
    PathBuf::from(&snapshot.root_folder) == PathBuf::from(folder)
}

fn local_index_file_name_for_pair(pair_id: &str) -> String {
    format!("storage-goblin-local-index-{pair_id}.json")
}

fn ensure_scannable_root(root: &Path) -> Result<(), String> {
    if !root.exists() {
        return Err(format!(
            "Configured local folder was not found: {}",
            root.display()
        ));
    }

    let metadata = fs::metadata(root).map_err(|error| {
        format!(
            "Failed to inspect local folder '{}': {error}",
            root.display()
        )
    })?;

    if !metadata.is_dir() {
        return Err(format!(
            "Configured local folder is not a directory: {}",
            root.display()
        ));
    }

    fs::read_dir(root).map_err(|error| {
        format!(
            "Configured local folder is not readable: {} ({error})",
            root.display()
        )
    })?;

    Ok(())
}

fn scan_directory_recursive(
    root: &Path,
    current: &Path,
    entries: &mut Vec<LocalIndexEntry>,
    summary: &mut LocalIndexSummary,
) -> Result<(), String> {
    let mut children = fs::read_dir(current)
        .map_err(|error| format!("Failed to read directory '{}': {error}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "Failed to enumerate directory '{}': {error}",
                current.display()
            )
        })?;

    children.sort_by(|left, right| left.path().cmp(&right.path()));

    for child in children {
        let path = child.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            format!("Failed to read metadata for '{}': {error}", path.display())
        })?;

        if metadata.file_type().is_symlink() {
            continue;
        }

        if metadata.is_dir() {
            summary.directory_count += 1;
            entries.push(LocalIndexEntry {
                relative_path: relative_path(root, &path)?,
                kind: "directory".into(),
                size: 0,
                modified_at: metadata.modified().ok().and_then(system_time_to_iso),
                fingerprint: None,
            });

            scan_directory_recursive(root, &path, entries, summary)?;
            continue;
        }

        if metadata.is_file() {
            summary.file_count += 1;
            summary.total_bytes += metadata.len();
            entries.push(LocalIndexEntry {
                relative_path: relative_path(root, &path)?,
                kind: "file".into(),
                size: metadata.len(),
                modified_at: metadata.modified().ok().and_then(system_time_to_iso),
                fingerprint: Some(file_fingerprint(&path)?),
            });
        }
    }

    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String, String> {
    let relative = path.strip_prefix(root).map_err(|error| {
        format!(
            "Failed to calculate relative path for '{}': {error}",
            path.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

pub(crate) fn file_fingerprint(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| {
        format!(
            "failed to read local file '{}' for fingerprinting: {error}",
            path.display()
        )
    })?;
    Ok(hex_sha256(&bytes))
}

pub(crate) fn bytes_fingerprint(bytes: &[u8]) -> String {
    hex_sha256(bytes)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn read_local_index_snapshot_file(path: &Path) -> Result<LocalIndexSnapshot, String> {
    let raw = fs::read_to_string(path)
        .map_err(|error| format!("failed to read local index snapshot: {error}"))?;
    serde_json::from_str(&raw)
        .map_err(|error| format!("failed to parse local index snapshot: {error}"))
}

fn write_local_index_snapshot_file(
    path: &Path,
    snapshot: &LocalIndexSnapshot,
) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(snapshot)
        .map_err(|error| format!("failed to serialize local index snapshot: {error}"))?;
    fs::write(path, raw).map_err(|error| format!("failed to write local index snapshot: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        local_index_file_name_for_pair, read_local_index_snapshot_file, scan_local_folder,
        write_local_index_snapshot_file,
    };
    use std::{
        env, fs,
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_nanos();
        env::temp_dir().join(format!("storage-goblin-{name}-{}-{suffix}", process::id()))
    }

    #[test]
    fn scans_nested_files_and_directories() {
        let root = temp_path("scan");
        let nested = root.join("nested");

        fs::create_dir_all(&nested).expect("should create nested test directory");
        fs::write(root.join("alpha.txt"), b"alpha").expect("should write root file");
        fs::write(nested.join("beta.txt"), b"beta-data").expect("should write nested file");

        let snapshot = scan_local_folder(&root).expect("scan should succeed");

        assert_eq!(snapshot.summary.file_count, 2);
        assert_eq!(snapshot.summary.directory_count, 1);
        assert_eq!(snapshot.summary.total_bytes, 14);
        assert_eq!(snapshot.entries.len(), 3);
        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "nested"));
        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "nested/beta.txt" && entry.fingerprint.is_some()));

        fs::remove_dir_all(root).expect("should clean up scan test directory");
    }

    #[test]
    fn rejects_missing_root_folder() {
        let missing = temp_path("missing");
        let error = scan_local_folder(&missing).expect_err("scan should fail for missing root");
        assert!(error.contains("not found"));
    }

    #[test]
    fn round_trips_snapshot_json() {
        let root = temp_path("roundtrip-root");
        let snapshot_path = temp_path("roundtrip-json").with_extension("json");

        fs::create_dir_all(&root).expect("should create roundtrip directory");
        fs::write(root.join("file.txt"), b"hello").expect("should write roundtrip file");

        let snapshot = scan_local_folder(&root).expect("scan should succeed");
        write_local_index_snapshot_file(&snapshot_path, &snapshot)
            .expect("should write snapshot json");
        let restored =
            read_local_index_snapshot_file(&snapshot_path).expect("should read snapshot json");

        assert_eq!(restored.summary.file_count, 1);
        assert_eq!(restored.entries.len(), 1);
        assert!(restored.entries[0].fingerprint.is_some());

        fs::remove_file(snapshot_path).expect("should clean up snapshot json");
        fs::remove_dir_all(root).expect("should clean up roundtrip directory");
    }

    #[test]
    fn pair_file_name_includes_pair_id() {
        assert_eq!(
            local_index_file_name_for_pair("abc-123"),
            "storage-goblin-local-index-abc-123.json"
        );
    }

    #[test]
    fn pair_file_name_differs_from_global() {
        let pair_name = local_index_file_name_for_pair("default");
        assert_ne!(pair_name, super::super::LOCAL_INDEX_FILE_NAME);
    }

    #[test]
    fn fingerprint_changes_for_same_size_content() {
        let root = temp_path("fingerprint-same-size");
        fs::create_dir_all(&root).expect("should create test root");
        let file_path = root.join("note.txt");

        fs::write(&file_path, b"alpha").expect("should write first content");
        let first = scan_local_folder(&root).expect("first scan should succeed");

        fs::write(&file_path, b"bravo").expect("should write second content");
        let second = scan_local_folder(&root).expect("second scan should succeed");

        assert_eq!(first.entries[0].size, second.entries[0].size);
        assert_ne!(first.entries[0].fingerprint, second.entries[0].fingerprint);

        fs::remove_dir_all(root).expect("should clean up fingerprint test directory");
    }
}
