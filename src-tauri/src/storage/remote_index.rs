use serde::{Deserialize, Serialize};
use std::{fs, path::Path};
use tauri::{AppHandle, Runtime};

use super::{app_storage_path, remote_bin::key_matches_excluded_prefix, REMOTE_INDEX_FILE_NAME};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RemoteIndexSummary {
    pub indexed_at: String,
    pub object_count: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteObjectEntry {
    pub key: String,
    pub relative_path: String,
    #[serde(default = "default_remote_entry_kind")]
    pub kind: String,
    pub size: u64,
    pub last_modified_at: Option<String>,
    pub etag: Option<String>,
    #[serde(default)]
    pub storage_class: Option<String>,
}

/// Returns true if the storage class represents an S3 Glacier tier.
pub fn is_glacier_storage_class(storage_class: Option<&str>) -> bool {
    matches!(
        storage_class,
        Some("GLACIER") | Some("DEEP_ARCHIVE") | Some("GLACIER_IR")
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteIndexSnapshot {
    pub version: u32,
    pub bucket: String,
    #[serde(default)]
    pub excluded_prefixes: Vec<String>,
    pub summary: RemoteIndexSummary,
    pub entries: Vec<RemoteObjectEntry>,
}

pub fn read_remote_index_snapshot<R: Runtime>(
    app: &AppHandle<R>,
) -> Result<Option<RemoteIndexSnapshot>, String> {
    let path = app_storage_path(app, REMOTE_INDEX_FILE_NAME)?;
    if !path.exists() {
        return Ok(None);
    }

    read_remote_index_snapshot_file(&path).map(Some)
}

pub fn write_remote_index_snapshot<R: Runtime>(
    app: &AppHandle<R>,
    snapshot: &RemoteIndexSnapshot,
) -> Result<(), String> {
    let path = app_storage_path(app, REMOTE_INDEX_FILE_NAME)?;
    write_remote_index_snapshot_file(&path, snapshot)
}

pub fn read_remote_index_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair_id: &str,
) -> Result<Option<RemoteIndexSnapshot>, String> {
    let file_name = remote_index_file_name_for_pair(pair_id);
    let path = app_storage_path(app, &file_name)?;
    if !path.exists() {
        return Ok(None);
    }

    read_remote_index_snapshot_file(&path).map(Some)
}

pub fn write_remote_index_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair_id: &str,
    snapshot: &RemoteIndexSnapshot,
) -> Result<(), String> {
    let file_name = remote_index_file_name_for_pair(pair_id);
    let path = app_storage_path(app, &file_name)?;
    write_remote_index_snapshot_file(&path, snapshot)
}

#[cfg(test)]
pub(crate) fn snapshot_matches_target_with_exclusions(
    snapshot: &RemoteIndexSnapshot,
    bucket: &str,
    excluded_prefixes: &[String],
) -> bool {
    snapshot.bucket.trim() == bucket.trim()
        && normalized_prefixes(&snapshot.excluded_prefixes)
            == normalized_prefixes(excluded_prefixes)
}

pub(crate) fn snapshot_matches_target(snapshot: &RemoteIndexSnapshot, bucket: &str) -> bool {
    snapshot.bucket.trim() == bucket.trim()
}

#[cfg(test)]
pub(crate) fn normalized_prefixes(prefixes: &[String]) -> Vec<String> {
    let mut prefixes: Vec<String> = prefixes
        .iter()
        .map(|prefix| prefix.replace('\\', "/").trim_matches('/').to_string())
        .filter(|prefix| !prefix.is_empty())
        .collect();
    prefixes.sort();
    prefixes.dedup();
    prefixes
}

pub(crate) fn should_exclude_remote_key(key: &str, excluded_prefixes: &[String]) -> bool {
    key_matches_excluded_prefix(key, excluded_prefixes)
}

pub(crate) fn relative_path_from_key(key: &str) -> String {
    key.replace('\\', "/")
}

pub(crate) fn directory_relative_paths_from_key(key: &str) -> Vec<String> {
    let relative_path = relative_path_from_key(key);
    directory_relative_paths_from_relative_path(&relative_path)
}

pub(crate) fn directory_relative_paths_from_relative_path(relative_path: &str) -> Vec<String> {
    let normalized = relative_path
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();

    if normalized.is_empty() {
        return Vec::new();
    }

    let parts: Vec<&str> = normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    if parts.is_empty() {
        return Vec::new();
    }

    let limit = if relative_path.ends_with('/') {
        parts.len()
    } else {
        parts.len().saturating_sub(1)
    };

    let mut directories = Vec::with_capacity(limit);
    for index in 0..limit {
        directories.push(parts[..=index].join("/"));
    }

    directories
}

fn remote_index_file_name_for_pair(pair_id: &str) -> String {
    format!("storage-goblin-remote-index-{pair_id}.json")
}

fn default_remote_entry_kind() -> String {
    "file".into()
}

fn read_remote_index_snapshot_file(path: &Path) -> Result<RemoteIndexSnapshot, String> {
    let raw = fs::read_to_string(path)
        .map_err(|error| format!("failed to read remote index snapshot: {error}"))?;
    serde_json::from_str(&raw)
        .map_err(|error| format!("failed to parse remote index snapshot: {error}"))
}

fn write_remote_index_snapshot_file(
    path: &Path,
    snapshot: &RemoteIndexSnapshot,
) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(snapshot)
        .map_err(|error| format!("failed to serialize remote index snapshot: {error}"))?;
    fs::write(path, raw).map_err(|error| format!("failed to write remote index snapshot: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        directory_relative_paths_from_key, directory_relative_paths_from_relative_path,
        normalized_prefixes, relative_path_from_key, remote_index_file_name_for_pair,
        should_exclude_remote_key, snapshot_matches_target_with_exclusions, RemoteIndexSnapshot,
        RemoteIndexSummary,
    };

    fn bucket_root_snapshot() -> RemoteIndexSnapshot {
        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo-bucket".into(),
            excluded_prefixes: vec![".storage-goblin-bin/pair-1/".into()],
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count: 0,
                total_bytes: 0,
            },
            entries: Vec::new(),
        }
    }

    #[test]
    fn relative_path_from_key_preserves_bucket_root_key() {
        assert_eq!(
            relative_path_from_key("photos/2026/alpha.txt"),
            "photos/2026/alpha.txt"
        );
        assert_eq!(relative_path_from_key("alpha.txt"), "alpha.txt");
    }

    #[test]
    fn directory_inference_uses_full_key_path() {
        assert_eq!(
            directory_relative_paths_from_key("photos/2026/nested/alpha.txt"),
            vec!["photos", "photos/2026", "photos/2026/nested"]
        );
    }

    #[test]
    fn directory_inference_walks_full_nested_path() {
        assert_eq!(
            directory_relative_paths_from_key("photos/2026/a/b/c.txt"),
            vec!["photos", "photos/2026", "photos/2026/a", "photos/2026/a/b"]
        );
    }

    #[test]
    fn preserves_explicit_directory_marker_path() {
        assert_eq!(
            directory_relative_paths_from_relative_path("nested/path/"),
            vec!["nested", "nested/path"]
        );
    }

    #[test]
    fn pair_file_name_includes_pair_id() {
        assert_eq!(
            remote_index_file_name_for_pair("abc-123"),
            "storage-goblin-remote-index-abc-123.json"
        );
    }

    #[test]
    fn pair_file_name_differs_from_global() {
        let pair_name = remote_index_file_name_for_pair("default");
        assert_ne!(pair_name, super::super::REMOTE_INDEX_FILE_NAME);
    }

    #[test]
    fn snapshot_target_matching_ignores_legacy_endpoint_and_prefix_in_bucket_root_contract() {
        let snapshot = bucket_root_snapshot();

        assert!(
            snapshot_matches_target_with_exclusions(
                &snapshot,
                "demo-bucket",
                &[".storage-goblin-bin/pair-1/".into()]
            ),
            "bucket-root snapshot identity should not depend on legacy endpoint or prefix"
        );
    }

    #[test]
    fn snapshot_target_matching_includes_excluded_prefixes() {
        let snapshot = bucket_root_snapshot();

        assert!(!snapshot_matches_target_with_exclusions(
            &snapshot,
            "demo-bucket",
            &[".storage-goblin-bin/pair-2/".into()]
        ));
    }

    #[test]
    fn excludes_remote_bin_keys_from_normal_inventory() {
        assert!(should_exclude_remote_key(
            ".storage-goblin-bin/pair-1/abc/docs/note.txt",
            &[".storage-goblin-bin/pair-1/".into()]
        ));
        assert!(!should_exclude_remote_key(
            "docs/note.txt",
            &[".storage-goblin-bin/pair-1/".into()]
        ));
    }

    #[test]
    fn normalized_prefixes_sort_and_trim() {
        assert_eq!(
            normalized_prefixes(&[
                " /a/ ".trim().to_string(),
                "b/".into(),
                "a".into(),
                "".into()
            ]),
            vec!["a".to_string(), "b".to_string()]
        );
    }
}
