use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, path::Path};
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

/// Scan `root`, reusing fingerprints from `previous` for files whose size and
/// modification time are unchanged (backlog phase 2.3).
///
/// Hashing dominates scan cost — the phase-0 baseline measured ~7.8k files/s,
/// so a 100k-file tree spends ~13s hashing on every cycle even when nothing
/// changed. This is the standard rsync-style heuristic.
///
/// The tradeoff is deliberate: a file modified without its size or mtime
/// changing (a deliberate mtime restore, or an edit within the filesystem's
/// timestamp granularity) will not be re-hashed and so will not be detected
/// until a full rescan. That is the same bargain rsync, Dropbox, and every
/// other sync client make; the alternative is re-reading every byte forever.
pub fn scan_local_folder_with_cache(
    root: &Path,
    previous: Option<&LocalIndexSnapshot>,
) -> Result<LocalIndexSnapshot, String> {
    ensure_scannable_root(root)?;

    let cache = match previous {
        Some(snapshot) if snapshot_matches_folder(snapshot, &root.to_string_lossy()) => {
            FingerprintCache::from_snapshot(snapshot)
        }
        // A snapshot of a different folder tells us nothing about this one.
        _ => FingerprintCache::empty(),
    };

    let mut entries = Vec::new();
    let mut summary = LocalIndexSummary {
        indexed_at: now_iso(),
        file_count: 0,
        directory_count: 0,
        total_bytes: 0,
    };

    scan_directory_recursive(root, root, &mut entries, &mut summary, &cache)?;

    Ok(LocalIndexSnapshot {
        version: 2,
        root_folder: root.to_string_lossy().into_owned(),
        summary,
        entries,
    })
}

/// Fingerprints from the previous scan, keyed by path, valid only while a
/// file's size and modification time both match.
struct FingerprintCache {
    entries: std::collections::HashMap<String, (u64, Option<String>, String)>,
}

impl FingerprintCache {
    fn empty() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }

    fn from_snapshot(snapshot: &LocalIndexSnapshot) -> Self {
        let entries = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind == "file")
            .filter_map(|entry| {
                entry.fingerprint.as_ref().map(|fingerprint| {
                    (
                        entry.relative_path.clone(),
                        (entry.size, entry.modified_at.clone(), fingerprint.clone()),
                    )
                })
            })
            .collect();
        Self { entries }
    }

    /// The cached fingerprint, if this file looks untouched since last scan.
    fn reuse(
        &self,
        relative_path: &str,
        size: u64,
        modified_at: &Option<String>,
    ) -> Option<String> {
        let (cached_size, cached_modified_at, fingerprint) = self.entries.get(relative_path)?;
        // Both must match: size alone misses same-length edits, and mtime
        // alone misses filesystems that preserve it across a write.
        if *cached_size == size && cached_modified_at == modified_at {
            Some(fingerprint.clone())
        } else {
            None
        }
    }
}

pub(crate) fn snapshot_matches_folder(snapshot: &LocalIndexSnapshot, folder: &str) -> bool {
    Path::new(&snapshot.root_folder) == Path::new(folder)
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
    cache: &FingerprintCache,
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

    children.sort_by_key(|entry| entry.path());

    for child in children {
        let path = child.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            format!("Failed to read metadata for '{}': {error}", path.display())
        })?;

        if metadata.file_type().is_symlink() {
            continue;
        }

        // Never index an in-flight download's temp file: it is partial by
        // definition, and indexing it would upload the partial content as if
        // it were a real local file.
        if metadata.is_file() && super::transfer::is_temp_download_path(&path) {
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

            scan_directory_recursive(root, &path, entries, summary, cache)?;
            continue;
        }

        if metadata.is_file() {
            summary.file_count += 1;
            summary.total_bytes += metadata.len();

            let relative = relative_path(root, &path)?;
            let size = metadata.len();
            let modified_at = metadata.modified().ok().and_then(system_time_to_iso);
            let fingerprint = match cache.reuse(&relative, size, &modified_at) {
                Some(cached) => cached,
                None => file_fingerprint(&path)?,
            };

            entries.push(LocalIndexEntry {
                relative_path: relative,
                kind: "file".into(),
                size,
                modified_at,
                fingerprint: Some(fingerprint),
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

#[cfg(test)]
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
        bytes_fingerprint, local_index_file_name_for_pair, read_local_index_snapshot_file,
        scan_local_folder_with_cache, write_local_index_snapshot_file, LocalIndexEntry,
        LocalIndexSnapshot, LocalIndexSummary,
    };
    use std::{
        env, fs,
        path::{Path, PathBuf},
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

        let snapshot = scan_local_folder_with_cache(&root, None).expect("scan should succeed");

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
        let error = scan_local_folder_with_cache(&missing, None)
            .expect_err("scan should fail for missing root");
        assert!(error.contains("not found"));
    }

    #[test]
    fn round_trips_snapshot_json() {
        let root = temp_path("roundtrip-root");
        let snapshot_path = temp_path("roundtrip-json").with_extension("json");

        fs::create_dir_all(&root).expect("should create roundtrip directory");
        fs::write(root.join("file.txt"), b"hello").expect("should write roundtrip file");

        let snapshot = scan_local_folder_with_cache(&root, None).expect("scan should succeed");
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
        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");

        fs::write(&file_path, b"bravo").expect("should write second content");
        let second = scan_local_folder_with_cache(&root, None).expect("second scan should succeed");

        assert_eq!(first.entries[0].size, second.entries[0].size);
        assert_ne!(first.entries[0].fingerprint, second.entries[0].fingerprint);

        fs::remove_dir_all(root).expect("should clean up fingerprint test directory");
    }

    /// A fingerprint that could never be computed from the file's bytes, so
    /// seeing it in a scan result proves the value was reused rather than
    /// recalculated.
    const SENTINEL: &str = "cached-not-recomputed";

    fn snapshot_claiming(
        root: &Path,
        relative_path: &str,
        size: u64,
        modified_at: Option<String>,
    ) -> LocalIndexSnapshot {
        LocalIndexSnapshot {
            version: 2,
            root_folder: root.to_string_lossy().into_owned(),
            summary: LocalIndexSummary::default(),
            entries: vec![LocalIndexEntry {
                relative_path: relative_path.into(),
                kind: "file".into(),
                size,
                modified_at,
                fingerprint: Some(SENTINEL.into()),
            }],
        }
    }

    #[test]
    fn an_unchanged_file_reuses_its_cached_fingerprint_instead_of_rehashing() {
        let root = temp_path("cache-hit");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("stable.txt"), b"contents").expect("file should write");

        // Learn the real size/mtime, then claim a sentinel fingerprint for it.
        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first
            .entries
            .iter()
            .find(|entry| entry.relative_path == "stable.txt")
            .expect("file should be indexed");
        let previous =
            snapshot_claiming(&root, "stable.txt", entry.size, entry.modified_at.clone());

        let second = scan_local_folder_with_cache(&root, Some(&previous))
            .expect("cached scan should succeed");

        let cached = second
            .entries
            .iter()
            .find(|entry| entry.relative_path == "stable.txt")
            .expect("file should be indexed");
        assert_eq!(
            cached.fingerprint.as_deref(),
            Some(SENTINEL),
            "an untouched file should not be re-hashed"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_file_whose_size_changed_is_rehashed() {
        let root = temp_path("cache-size");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("grown.txt"), b"contents").expect("file should write");

        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first
            .entries
            .iter()
            .find(|entry| entry.relative_path == "grown.txt")
            .expect("file should be indexed");
        // Cache claims a stale, smaller size.
        let previous = snapshot_claiming(
            &root,
            "grown.txt",
            entry.size - 1,
            entry.modified_at.clone(),
        );

        let second =
            scan_local_folder_with_cache(&root, Some(&previous)).expect("scan should succeed");

        let rescanned = second
            .entries
            .iter()
            .find(|entry| entry.relative_path == "grown.txt")
            .expect("file should be indexed");
        assert_eq!(
            rescanned.fingerprint.as_deref(),
            Some(bytes_fingerprint(b"contents").as_str()),
            "a size change must invalidate the cached fingerprint"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_file_whose_mtime_changed_is_rehashed() {
        let root = temp_path("cache-mtime");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("touched.txt"), b"contents").expect("file should write");

        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first
            .entries
            .iter()
            .find(|entry| entry.relative_path == "touched.txt")
            .expect("file should be indexed");
        let previous = snapshot_claiming(
            &root,
            "touched.txt",
            entry.size,
            Some("1999-01-01T00:00:00Z".to_string()),
        );

        let second =
            scan_local_folder_with_cache(&root, Some(&previous)).expect("scan should succeed");

        let rescanned = second
            .entries
            .iter()
            .find(|entry| entry.relative_path == "touched.txt")
            .expect("file should be indexed");
        assert_eq!(
            rescanned.fingerprint.as_deref(),
            Some(bytes_fingerprint(b"contents").as_str()),
            "an mtime change must invalidate the cached fingerprint"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_snapshot_of_a_different_folder_is_not_used_as_a_cache() {
        let root = temp_path("cache-other-folder");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("file.txt"), b"contents").expect("file should write");

        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first.entries[0].clone();
        // Same path and stats, but recorded against a different root.
        let mut previous = snapshot_claiming(&root, "file.txt", entry.size, entry.modified_at);
        previous.root_folder = "C:/some/other/folder".into();

        let second =
            scan_local_folder_with_cache(&root, Some(&previous)).expect("scan should succeed");

        assert_eq!(
            second.entries[0].fingerprint.as_deref(),
            Some(bytes_fingerprint(b"contents").as_str()),
            "a cache from another folder must be ignored"
        );

        let _ = fs::remove_dir_all(&root);
    }
}
