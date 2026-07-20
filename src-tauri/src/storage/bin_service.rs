//! The remote bin: listing deleted entries, validating restore destinations,
//! and restoring or purging them.
//!
//! Two backends sit behind one surface — lifecycle-managed bin objects for
//! pairs without versioning, and delete-marker history for pairs with object
//! versioning enabled.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use tauri::AppHandle;

use super::commands::{
    list_remote_inventory_for_pair, reconcile_remote_bin_lifecycle_target, storage_config_for_pair,
    BinEntryMutationResult, BinEntryRequest, FileEntryResponse,
};
use super::credential_service::{
    provider_supports_runtime_object_versioning, sync_location_runtime_object_versioning_message,
};
use super::credentials_store::StoredCredentials;
use super::lifecycle_service::target_for_pair;
use super::object_store;
use super::platform::resolve_local_download_path;
use super::profile_store::SyncPair;
use super::remote_bin::{
    bin_prefix_contains_bin_key, deleted_directory_key, deleted_object_key, namespace_prefix,
    original_relative_path_from_bin_key_for_pair, pair_bin_prefix,
};
use super::remote_index::RemoteObjectEntry;
use super::remote_index::{relative_path_from_key, should_exclude_remote_key};
use super::s3_adapter;

pub(crate) fn normalize_restore_relative_path(path: &str) -> String {
    path.replace('\\', "/").trim_matches('/').to_string()
}

pub(crate) fn ancestor_restore_paths(path: &str) -> Vec<String> {
    let normalized = normalize_restore_relative_path(path);
    if normalized.is_empty() {
        return Vec::new();
    }

    let parts: Vec<&str> = normalized
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    if parts.len() <= 1 {
        return Vec::new();
    }

    (0..parts.len() - 1)
        .map(|index| parts[..=index].join("/"))
        .collect()
}

pub(crate) fn is_descendant_restore_path(candidate: &str, ancestor: &str) -> bool {
    let candidate = normalize_restore_relative_path(candidate);
    let ancestor = normalize_restore_relative_path(ancestor);

    !candidate.is_empty()
        && !ancestor.is_empty()
        && candidate != ancestor
        && candidate.starts_with(&format!("{ancestor}/"))
}

pub(crate) fn validate_local_restore_destination(
    root: &str,
    destination_path: &str,
) -> Result<(), String> {
    let root_path = Path::new(root);
    if let Ok(metadata) = std::fs::symlink_metadata(root_path) {
        if !metadata.is_dir() {
            return Err(format!(
                "Cannot restore to '{}' because local root '{}' is not a directory.",
                destination_path,
                root_path.display()
            ));
        }
    }

    let target = resolve_local_download_path(root, destination_path)?;

    match std::fs::symlink_metadata(&target) {
        Ok(metadata) => {
            let existing_kind = if metadata.is_dir() {
                "directory"
            } else {
                "file"
            };
            return Err(format!(
                "Cannot restore to '{}' because local destination already exists as a {}.",
                destination_path, existing_kind
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "Failed to inspect local destination '{}': {error}",
                target.display()
            ));
        }
    }

    for ancestor in ancestor_restore_paths(destination_path) {
        let ancestor_path = resolve_local_download_path(root, &ancestor)?;
        match std::fs::symlink_metadata(&ancestor_path) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(format!(
                    "Cannot restore to '{}' because local ancestor '{}' is a file.",
                    destination_path, ancestor
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "Failed to inspect local ancestor '{}': {error}",
                    ancestor_path.display()
                ));
            }
        }
    }

    Ok(())
}

pub(crate) fn validate_remote_restore_destination(
    entries: &[RemoteObjectEntry],
    destination_path: &str,
) -> Result<(), String> {
    let destination_path = normalize_restore_relative_path(destination_path);

    if let Some(conflict) = entries
        .iter()
        .find(|entry| normalize_restore_relative_path(&entry.relative_path) == destination_path)
    {
        return Err(format!(
            "Cannot restore to '{}' because remote destination already exists as a {}.",
            destination_path, conflict.kind
        ));
    }

    for ancestor in ancestor_restore_paths(&destination_path) {
        if let Some(conflict) = entries.iter().find(|entry| {
            normalize_restore_relative_path(&entry.relative_path) == ancestor
                && entry.kind != "directory"
        }) {
            return Err(format!(
                "Cannot restore to '{}' because remote ancestor '{}' exists as a {}.",
                destination_path, ancestor, conflict.kind
            ));
        }
    }

    if let Some(conflict) = entries
        .iter()
        .find(|entry| is_descendant_restore_path(&entry.relative_path, &destination_path))
    {
        return Err(format!(
            "Cannot restore to '{}' because remote descendant '{}' already exists.",
            destination_path, conflict.relative_path
        ));
    }

    Ok(())
}

pub(crate) async fn validate_restore_destination(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    client: &object_store::ObjectStoreClient,
    destination_path: &str,
) -> Result<(), String> {
    validate_local_restore_destination(&pair.local_folder, destination_path)?;

    let exact_file_key = s3_adapter::object_key(destination_path);
    let exact_directory_key = s3_adapter::directory_key(destination_path);

    if object_store::object_exists(client, &pair.bucket, &exact_file_key).await? {
        return Err(format!(
            "Cannot restore to '{}' because remote destination key already exists.",
            destination_path
        ));
    }

    if !exact_directory_key.is_empty()
        && exact_directory_key != exact_file_key
        && object_store::object_exists(client, &pair.bucket, &exact_directory_key).await?
    {
        return Err(format!(
            "Cannot restore to '{}' because remote directory placeholder already exists.",
            destination_path
        ));
    }

    let remote_snapshot = list_remote_inventory_for_pair(pair, credentials).await?;
    validate_remote_restore_destination(&remote_snapshot.entries, destination_path)
}

pub(crate) fn destination_key_for_bin_restore(
    bin_key: &str,
    original_relative_path: &str,
) -> String {
    if bin_key.replace('\\', "/").ends_with('/') {
        s3_adapter::directory_key(original_relative_path.trim_matches('/'))
    } else {
        s3_adapter::object_key(original_relative_path)
    }
}

pub(crate) fn versioned_bin_key(key: &str, version_id: &str) -> String {
    format!("versioned:{key}::{version_id}")
}

pub(crate) fn parse_versioned_bin_key(bin_key: &str) -> Result<(String, String), String> {
    let Some(payload) = bin_key.strip_prefix("versioned:") else {
        return Err(format!("Unsupported versioned bin entry key '{bin_key}'."));
    };

    let Some((key, version_id)) = payload.rsplit_once("::") else {
        return Err(format!("Unsupported versioned bin entry key '{bin_key}'."));
    };

    if key.trim().is_empty() || version_id.trim().is_empty() {
        return Err(format!("Unsupported versioned bin entry key '{bin_key}'."));
    }

    Ok((key.to_string(), version_id.to_string()))
}

pub(crate) fn bin_deleted_from(pair: &SyncPair) -> String {
    if pair.object_versioning_enabled {
        "object-versioning".into()
    } else {
        "remote-bin".into()
    }
}

pub(crate) fn add_retention_days(timestamp: &str, retention_days: u32) -> Option<String> {
    let deleted_at =
        time::OffsetDateTime::parse(timestamp, &time::format_description::well_known::Rfc3339)
            .ok()?;
    let retention = time::Duration::days(i64::from(retention_days));
    deleted_at.checked_add(retention).and_then(|value| {
        value
            .format(&time::format_description::well_known::Rfc3339)
            .ok()
    })
}

pub(crate) fn bin_entry_lifecycle_fields(
    pair: &SyncPair,
    deleted_at: Option<&str>,
) -> (Option<String>, Option<String>, Option<u32>, Option<String>) {
    let deleted_at = deleted_at.map(str::to_string);
    let deleted_from = Some(bin_deleted_from(pair));

    if pair.object_versioning_enabled {
        return (deleted_at, deleted_from, None, None);
    }

    if pair.remote_bin.enabled {
        let retention_days = Some(pair.remote_bin.retention_days);
        let expires_at = deleted_at
            .as_deref()
            .and_then(|value| add_retention_days(value, pair.remote_bin.retention_days));
        return (deleted_at, deleted_from, retention_days, expires_at);
    }

    (deleted_at, deleted_from, None, None)
}

pub(crate) fn normalize_bin_entry_kind(kind: &str) -> Result<&str, String> {
    match kind {
        "file" | "directory" => Ok(kind),
        other => Err(format!("Unsupported bin entry kind '{other}'.")),
    }
}

pub(crate) fn collect_remote_bin_keys_for_request(
    pair: &SyncPair,
    request: &BinEntryRequest,
    available_entries: &[RemoteObjectEntry],
) -> Result<Vec<RemoteObjectEntry>, String> {
    let request_kind = normalize_bin_entry_kind(&request.kind)?;
    let normalized_path = normalize_restore_relative_path(&request.path);
    if normalized_path.is_empty() {
        return Err("Bin path must reference a non-empty relative path.".into());
    }

    if let Some(bin_key) = request.bin_key.as_deref() {
        let normalized_bin_key = bin_key.replace('\\', "/");
        let entry = available_entries
            .iter()
            .find(|entry| entry.key == normalized_bin_key)
            .ok_or_else(|| format!("Bin entry '{bin_key}' was not found."))?;

        if normalize_restore_relative_path(&entry.relative_path) != normalized_path {
            return Err(format!(
                "Bin entry '{bin_key}' does not match requested path '{}'.",
                request.path
            ));
        }

        if entry.kind != request_kind {
            return Err(format!(
                "Bin entry '{}' does not match requested kind '{}'.",
                request.path, request.kind
            ));
        }

        return Ok(vec![entry.clone()]);
    }

    let matches: Vec<RemoteObjectEntry> = if request_kind == "file" {
        available_entries
            .iter()
            .filter(|entry| {
                entry.kind == "file"
                    && normalize_restore_relative_path(&entry.relative_path) == normalized_path
            })
            .cloned()
            .collect()
    } else {
        available_entries
            .iter()
            .filter(|entry| {
                path_matches_exact_or_descendant(&entry.relative_path, &normalized_path)
                    || bin_prefix_contains_bin_key(&pair.id, &normalized_path, &entry.key)
                        .unwrap_or(false)
            })
            .cloned()
            .collect()
    };

    if matches.is_empty() {
        return Err(format!("Bin path '{}' was not found.", request.path));
    }

    Ok(matches)
}

pub(crate) fn validate_bulk_restore_destinations(
    remote_entries: &[RemoteObjectEntry],
    destination_paths: &[String],
) -> Result<(), String> {
    for destination_path in destination_paths {
        validate_remote_restore_destination(remote_entries, destination_path)?;
    }

    Ok(())
}

pub(crate) fn validate_bin_batch_requests(
    requests: &[BinEntryRequest],
    action: &str,
) -> Result<(), String> {
    let mut planned: BTreeSet<String> = BTreeSet::new();

    for request in requests {
        normalize_bin_entry_kind(&request.kind)?;
        let normalized = normalize_restore_relative_path(&request.path);
        if normalized.is_empty() {
            return Err("Bin path must reference a non-empty relative path.".into());
        }

        if planned.contains(&normalized) {
            return Err(format!(
                "Cannot {action} '{}' more than once in the same batch.",
                request.path
            ));
        }

        for existing in &planned {
            if is_descendant_restore_path(existing, &normalized) {
                return Err(format!(
                    "Cannot {action} '{}' because the same batch already targets descendant '{}'.",
                    request.path, existing
                ));
            }
            if is_descendant_restore_path(&normalized, existing) {
                return Err(format!(
                    "Cannot {action} '{}' because the same batch already targets ancestor '{}'.",
                    request.path, existing
                ));
            }
        }

        planned.insert(normalized);
    }

    Ok(())
}

pub(crate) fn path_matches_exact_or_descendant(candidate: &str, root: &str) -> bool {
    let candidate = normalize_restore_relative_path(candidate);
    let root = normalize_restore_relative_path(root);

    candidate == root || is_descendant_restore_path(&candidate, &root)
}

pub(crate) fn collect_versioned_bin_entries_for_request(
    request: &BinEntryRequest,
    available_entries: &[VersionedBinEntry],
) -> Result<Vec<VersionedBinEntry>, String> {
    let request_kind = normalize_bin_entry_kind(&request.kind)?;
    let normalized_path = normalize_restore_relative_path(&request.path);
    if normalized_path.is_empty() {
        return Err("Bin path must reference a non-empty relative path.".into());
    }

    if let Some(bin_key) = request.bin_key.as_deref() {
        let (object_key, version_id) = parse_versioned_bin_key(bin_key)?;
        let object_path = normalize_restore_relative_path(&relative_path_from_key(&object_key));
        if object_path != normalized_path {
            return Err(format!(
                "Bin entry '{}' does not match requested path '{}'.",
                bin_key, request.path
            ));
        }

        let entry = available_entries
            .iter()
            .find(|entry| entry.key == object_key && entry.version_id == version_id)
            .ok_or_else(|| format!("Bin entry '{bin_key}' was not found."))?;

        if entry.kind != request_kind {
            return Err(format!(
                "Bin entry '{}' does not match requested kind '{}'.",
                request.path, request.kind
            ));
        }

        return Ok(vec![entry.clone()]);
    }

    let matches: Vec<VersionedBinEntry> = if request_kind == "file" {
        available_entries
            .iter()
            .filter(|entry| {
                entry.kind == "file"
                    && normalize_restore_relative_path(&entry.relative_path) == normalized_path
            })
            .cloned()
            .collect()
    } else {
        available_entries
            .iter()
            .filter(|entry| {
                path_matches_exact_or_descendant(&entry.relative_path, &normalized_path)
            })
            .cloned()
            .collect()
    };

    if matches.is_empty() {
        return Err(format!("Bin path '{}' was not found.", request.path));
    }

    Ok(matches)
}

pub(crate) fn collect_versioned_history_for_deleted_entries(
    deleted_entries: &[VersionedBinEntry],
    history: &[(String, String)],
) -> Vec<(String, String)> {
    let deleted_keys: BTreeSet<&str> = deleted_entries
        .iter()
        .map(|entry| entry.key.as_str())
        .collect();
    let mut seen = BTreeSet::new();

    history
        .iter()
        .filter(|(key, _)| deleted_keys.contains(key.as_str()))
        .filter_map(|entry| {
            let owned = entry.clone();
            seen.insert(owned.clone()).then_some(owned)
        })
        .collect()
}

pub(crate) fn build_versioned_bin_entry_responses(
    pair: &SyncPair,
    entries: &[VersionedBinEntry],
) -> Vec<FileEntryResponse> {
    entries
        .iter()
        .filter_map(|entry| {
            let path = match entry.kind.as_str() {
                "directory" => entry.relative_path.trim_matches('/').to_string(),
                _ => entry.relative_path.clone(),
            };

            if path.is_empty() {
                return None;
            }

            let (deleted_at, deleted_from, retention_days, expires_at) =
                bin_entry_lifecycle_fields(pair, entry.deleted_at.as_deref());

            Some(FileEntryResponse {
                path,
                kind: entry.kind.clone(),
                status: "deleted".into(),
                has_local_copy: false,
                storage_class: entry.storage_class.clone(),
                bin_key: Some(versioned_bin_key(&entry.key, &entry.version_id)),
                local_kind: None,
                remote_kind: None,
                local_size: None,
                remote_size: None,
                local_modified_at: None,
                remote_modified_at: None,
                remote_etag: None,
                deleted_at,
                deleted_from,
                retention_days,
                expires_at,
            })
        })
        .collect()
}

pub(crate) fn build_bin_entry_responses(
    pair: &SyncPair,
    entries: &[RemoteObjectEntry],
) -> Vec<FileEntryResponse> {
    entries
        .iter()
        .filter_map(|entry| {
            let path = match entry.kind.as_str() {
                "directory" => entry.relative_path.trim_matches('/').to_string(),
                _ => entry.relative_path.clone(),
            };

            if path.is_empty() {
                return None;
            }

            let (deleted_at, deleted_from, retention_days, expires_at) =
                bin_entry_lifecycle_fields(pair, entry.last_modified_at.as_deref());

            Some(FileEntryResponse {
                path,
                kind: entry.kind.clone(),
                status: "deleted".into(),
                has_local_copy: false,
                storage_class: entry.storage_class.clone(),
                bin_key: Some(entry.key.clone()),
                local_kind: None,
                remote_kind: None,
                local_size: None,
                remote_size: None,
                local_modified_at: None,
                remote_modified_at: None,
                remote_etag: None,
                deleted_at,
                deleted_from,
                retention_days,
                expires_at,
            })
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VersionedBinEntry {
    pub(crate) key: String,
    pub(crate) version_id: String,
    pub(crate) relative_path: String,
    pub(crate) kind: String,
    pub(crate) storage_class: Option<String>,
    pub(crate) deleted_at: Option<String>,
}

pub(crate) async fn list_versioned_bin_inventory_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<Vec<VersionedBinEntry>, String> {
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let mut key_marker: Option<String> = None;
    let mut version_id_marker: Option<String> = None;
    let mut deleted: BTreeMap<String, VersionedBinEntry> = BTreeMap::new();
    let mut live_keys = BTreeSet::new();

    loop {
        let page = object_store::list_object_versions_page(
            &client,
            &pair.bucket,
            key_marker.as_deref(),
            version_id_marker.as_deref(),
        )
        .await?;

        for version in &page.versions {
            if should_exclude_remote_key(&version.key, &[]) {
                continue;
            }

            if version.is_latest {
                live_keys.insert(version.key.clone());
                deleted.remove(&version.key);
            }
        }

        for marker in &page.delete_markers {
            if !marker.is_latest {
                continue;
            }

            if should_exclude_remote_key(
                &marker.key,
                &[pair_bin_prefix(&pair.id), namespace_prefix()],
            ) {
                continue;
            }

            if live_keys.contains(&marker.key) {
                continue;
            }

            let relative_path = relative_path_from_key(&marker.key);
            if relative_path.is_empty() {
                continue;
            }

            deleted.insert(
                marker.key.clone(),
                VersionedBinEntry {
                    key: marker.key.clone(),
                    version_id: marker.version_id.clone(),
                    relative_path: relative_path.clone(),
                    kind: if marker.key.ends_with('/') {
                        "directory".into()
                    } else {
                        "file".into()
                    },
                    storage_class: page
                        .versions
                        .iter()
                        .find(|version| version.key == marker.key)
                        .and_then(|version| version.storage_class.clone()),
                    deleted_at: marker.last_modified_at.clone(),
                },
            );
        }

        if !page.truncated {
            break;
        }

        key_marker = page.next_key_marker;
        version_id_marker = page.next_version_id_marker;
    }

    Ok(deleted.into_values().collect())
}

pub(crate) async fn list_versioned_object_history_for_prefix(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    prefix: &str,
) -> Result<Vec<(String, String)>, String> {
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let mut key_marker: Option<String> = None;
    let mut version_id_marker: Option<String> = None;
    let mut versions = Vec::new();

    loop {
        let page = object_store::list_object_versions_page_with_prefix(
            &client,
            &pair.bucket,
            Some(prefix),
            key_marker.as_deref(),
            version_id_marker.as_deref(),
        )
        .await?;

        versions.extend(
            page.versions
                .into_iter()
                .map(|version| (version.key, version.version_id)),
        );
        versions.extend(
            page.delete_markers
                .into_iter()
                .map(|marker| (marker.key, marker.version_id)),
        );

        if !page.truncated {
            break;
        }

        key_marker = page.next_key_marker;
        version_id_marker = page.next_version_id_marker;
    }

    Ok(versions)
}

pub(crate) async fn restore_versioned_bin_entries(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    requests: &[BinEntryRequest],
) -> Result<Vec<BinEntryMutationResult>, String> {
    validate_bin_batch_requests(requests, "restore")?;
    let available_entries = list_versioned_bin_inventory_for_pair(pair, credentials).await?;
    let mut grouped_paths = Vec::new();
    let mut ordered_requests: Vec<(BinEntryRequest, Vec<VersionedBinEntry>)> = Vec::new();

    for request in requests {
        let path = normalize_restore_relative_path(&request.path);
        if path.is_empty() {
            return Err("Bin path must reference a non-empty relative path.".into());
        }

        let matches = collect_versioned_bin_entries_for_request(request, &available_entries)?;

        grouped_paths.push(path.clone());
        ordered_requests.push((request.clone(), matches));
    }

    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    for path in &grouped_paths {
        validate_restore_destination(pair, credentials, &client, path).await?;
    }
    validate_bulk_restore_destinations(
        &list_remote_inventory_for_pair(pair, credentials)
            .await?
            .entries,
        &grouped_paths,
    )?;

    let mut results = Vec::with_capacity(ordered_requests.len());
    for (request, entries) in ordered_requests {
        let mut affected_count = 0usize;
        for entry in entries {
            object_store::delete_object_version(
                &client,
                &pair.bucket,
                &entry.key,
                &entry.version_id,
            )
            .await?;
            affected_count += 1;
        }

        results.push(BinEntryMutationResult {
            path: request.path,
            kind: request.kind,
            bin_key: request.bin_key,
            success: true,
            affected_count,
            error: None,
        });
    }

    Ok(results)
}

pub(crate) async fn restore_remote_bin_entries(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    requests: &[BinEntryRequest],
) -> Result<Vec<BinEntryMutationResult>, String> {
    validate_bin_batch_requests(requests, "restore")?;
    let available_entries = list_remote_bin_inventory_for_pair(pair, credentials).await?;
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let remote_snapshot = list_remote_inventory_for_pair(pair, credentials).await?;

    let mut ordered_requests: Vec<(BinEntryRequest, Vec<RemoteObjectEntry>)> = Vec::new();
    let mut destination_paths = Vec::new();

    for request in requests {
        let matches = collect_remote_bin_keys_for_request(pair, request, &available_entries)?;
        let destination_path = normalize_restore_relative_path(&request.path);
        validate_local_restore_destination(&pair.local_folder, &destination_path)?;
        destination_paths.push(destination_path);
        ordered_requests.push((request.clone(), matches));
    }

    validate_bulk_restore_destinations(&remote_snapshot.entries, &destination_paths)?;

    let mut results = Vec::with_capacity(ordered_requests.len());
    for (request, entries) in ordered_requests {
        let mut affected_count = 0usize;
        for entry in entries {
            let destination_key = destination_key_for_bin_restore(&entry.key, &entry.relative_path);
            object_store::move_object(&client, &pair.bucket, &entry.key, &destination_key, None)
                .await?;
            affected_count += 1;
        }

        results.push(BinEntryMutationResult {
            path: request.path,
            kind: request.kind,
            bin_key: request.bin_key,
            success: true,
            affected_count,
            error: None,
        });
    }

    Ok(results)
}

pub(crate) async fn purge_versioned_bin_entries(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    requests: &[BinEntryRequest],
) -> Result<Vec<BinEntryMutationResult>, String> {
    validate_bin_batch_requests(requests, "purge")?;
    let available_entries = list_versioned_bin_inventory_for_pair(pair, credentials).await?;
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let mut results = Vec::with_capacity(requests.len());

    for request in requests {
        let matches = collect_versioned_bin_entries_for_request(request, &available_entries)?;
        let request_kind = normalize_bin_entry_kind(&request.kind)?;
        let mut affected_count = 0usize;

        let history = if request_kind == "file" {
            let entry = matches
                .into_iter()
                .next()
                .ok_or_else(|| format!("Bin path '{}' was not found.", request.path))?;
            list_versioned_object_history_for_prefix(pair, credentials, &entry.key)
                .await?
                .into_iter()
                .filter(|(key, _)| key == &entry.key)
                .collect::<Vec<_>>()
        } else {
            let prefix = s3_adapter::directory_key(&request.path);
            let subtree_history =
                list_versioned_object_history_for_prefix(pair, credentials, &prefix).await?;
            collect_versioned_history_for_deleted_entries(&matches, &subtree_history)
        };

        for (key, version_id) in history {
            object_store::delete_object_version(&client, &pair.bucket, &key, &version_id).await?;
            affected_count += 1;
        }

        results.push(BinEntryMutationResult {
            path: request.path.clone(),
            kind: request.kind.clone(),
            bin_key: request.bin_key.clone(),
            success: true,
            affected_count,
            error: None,
        });
    }

    Ok(results)
}

pub(crate) async fn purge_remote_bin_entries(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    requests: &[BinEntryRequest],
) -> Result<Vec<BinEntryMutationResult>, String> {
    validate_bin_batch_requests(requests, "purge")?;
    let available_entries = list_remote_bin_inventory_for_pair(pair, credentials).await?;
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let mut results = Vec::with_capacity(requests.len());

    for request in requests {
        let matches = collect_remote_bin_keys_for_request(pair, request, &available_entries)?;
        let mut affected_count = 0usize;

        for entry in matches {
            object_store::delete_object(&client, &pair.bucket, &entry.key).await?;
            affected_count += 1;
        }

        results.push(BinEntryMutationResult {
            path: request.path.clone(),
            kind: request.kind.clone(),
            bin_key: request.bin_key.clone(),
            success: true,
            affected_count,
            error: None,
        });
    }

    Ok(results)
}

pub(crate) async fn list_remote_bin_inventory_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<Vec<RemoteObjectEntry>, String> {
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let mut entries: BTreeMap<String, RemoteObjectEntry> = BTreeMap::new();
    let prefixes = [pair_bin_prefix(&pair.id), namespace_prefix()];

    for prefix in prefixes {
        let objects = object_store::list_objects(&client, &pair.bucket, Some(&prefix))
            .await
            .map_err(|error| {
                format!(
                    "failed to list bin inventory for pair '{}': {error}",
                    pair.label
                )
            })?;

        for object in objects {
            let key = object.key;

            let Ok(original_relative_path) =
                original_relative_path_from_bin_key_for_pair(&pair.id, &key)
            else {
                continue;
            };

            let last_modified_at = object.last_modified_at;
            let etag = object.etag;
            let storage_class = object.storage_class;

            if key.ends_with('/') {
                let relative_path = original_relative_path.trim_matches('/').to_string();
                if relative_path.is_empty() {
                    continue;
                }

                entries.insert(
                    key.to_string(),
                    RemoteObjectEntry {
                        key: key.to_string(),
                        relative_path,
                        kind: "directory".into(),
                        size: 0,
                        last_modified_at,
                        etag,
                        storage_class: None,
                        fingerprint: None,
                    },
                );
                continue;
            }

            entries.insert(
                key.to_string(),
                RemoteObjectEntry {
                    key: key.to_string(),
                    relative_path: original_relative_path,
                    kind: "file".into(),
                    size: object.size,
                    last_modified_at,
                    etag,
                    storage_class,
                    fingerprint: None,
                },
            );
        }
    }

    Ok(entries.into_values().collect())
}

pub(crate) fn remote_bin_key_for_deleted_key(pair_id: &str, key: &str) -> String {
    let relative_path = relative_path_from_key(key);
    if key.ends_with('/') {
        deleted_directory_key(pair_id, relative_path.trim_matches('/'))
    } else {
        deleted_object_key(pair_id, &relative_path)
    }
}

pub(crate) async fn delete_remote_folder_subtree(
    app: &AppHandle,
    pair: &SyncPair,
    client: &object_store::ObjectStoreClient,
    folder_path: &str,
) -> Result<(), String> {
    let prefix = s3_adapter::directory_key(folder_path);
    let keys = object_store::list_object_keys_with_prefix(client, &pair.bucket, &prefix).await?;

    if pair.object_versioning_enabled {
        for key in keys {
            object_store::delete_object(client, &pair.bucket, &key).await?;
        }
        return Ok(());
    }

    if pair.remote_bin.enabled {
        if let Some(target) = target_for_pair(pair) {
            reconcile_remote_bin_lifecycle_target(app, &target).await?;
        }

        for key in keys {
            let bin_key = remote_bin_key_for_deleted_key(&pair.id, &key);
            object_store::move_object(client, &pair.bucket, &key, &bin_key, None).await?;
        }
        return Ok(());
    }

    for key in keys {
        object_store::delete_object(client, &pair.bucket, &key).await?;
    }

    Ok(())
}
