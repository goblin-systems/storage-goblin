//! Building the file browser's view of a sync location.
//!
//! Merges the local and remote snapshots with the anchors to classify every
//! path (synced / local-only / remote-only / conflict / …) and to refresh a
//! location's cached state after a change on either side.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use tauri::{AppHandle, Runtime};

use super::commands::{list_remote_inventory_for_pair, FileEntryResponse};
use super::credentials_store::StoredCredentials;
use super::local_index::{
    scan_local_folder, write_local_index_snapshot_for_pair, LocalIndexSnapshot,
};
use super::model::FileEntryStatus;
use super::now_iso;
use super::profile_store::SyncPair;
use super::remote_index::{
    read_remote_index_snapshot_for_pair, write_remote_index_snapshot_for_pair, RemoteIndexSnapshot,
    RemoteIndexSummary,
};
use super::sync_db::SyncAnchor;
use super::sync_planner;
use super::sync_service::{rebuild_durable_plan_for_pair, snapshot_for_pair};
use super::transfer_service::{local_fingerprint_for_path, remote_etag_for_path};

#[derive(Debug, Clone)]
pub(crate) struct IndexedFileEntry {
    kind: String,
    size: u64,
    storage_class: Option<String>,
    modified_at: Option<String>,
    etag: Option<String>,
}

pub(crate) async fn refresh_pair_state_after_remote_change<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<(), String> {
    let remote_snapshot = list_remote_inventory_for_pair(pair, credentials).await?;
    write_remote_index_snapshot_for_pair(app, &pair.id, &remote_snapshot)
        .map_err(|error| format!("Failed to save refreshed remote inventory: {error}"))?;

    let local_snapshot = match scan_local_folder(Path::new(&pair.local_folder)) {
        Ok(snapshot) => {
            let _ = write_local_index_snapshot_for_pair(app, &pair.id, &snapshot);
            Some(snapshot)
        }
        Err(_) => snapshot_for_pair(app, pair).0,
    };

    if let Some(local_snapshot) = local_snapshot.as_ref() {
        let _ = rebuild_durable_plan_for_pair(app, pair, local_snapshot, &remote_snapshot, true);
    }

    Ok(())
}

pub(crate) fn refresh_pair_state_after_local_change<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<(), String> {
    let local_snapshot = scan_local_folder(Path::new(&pair.local_folder)).map_err(|error| {
        format!(
            "Failed to scan local folder '{}' for sync pair '{}': {error}",
            pair.local_folder, pair.label
        )
    })?;

    write_local_index_snapshot_for_pair(app, &pair.id, &local_snapshot)
        .map_err(|error| format!("Failed to save refreshed local inventory: {error}"))?;

    let remote_snapshot = read_remote_index_snapshot_for_pair(app, &pair.id)
        .map_err(|error| format!("Failed to load remote inventory snapshot: {error}"))?
        .unwrap_or_else(|| RemoteIndexSnapshot {
            version: 1,
            bucket: pair.bucket.clone(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: now_iso(),
                object_count: 0,
                total_bytes: 0,
            },
            entries: Vec::new(),
        });

    let _ = rebuild_durable_plan_for_pair(app, pair, &local_snapshot, &remote_snapshot, true);

    Ok(())
}

pub(crate) fn build_file_entry_responses(
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    anchors: Option<&BTreeMap<String, SyncAnchor>>,
) -> Vec<FileEntryResponse> {
    let mut local_entries: BTreeMap<String, IndexedFileEntry> = BTreeMap::new();
    if let Some(snapshot) = local_snapshot {
        for entry in &snapshot.entries {
            local_entries.insert(
                entry.relative_path.clone(),
                IndexedFileEntry {
                    kind: entry.kind.clone(),
                    size: entry.size,
                    storage_class: None,
                    modified_at: entry.modified_at.clone(),
                    etag: None,
                },
            );
        }
    }

    let mut remote_entries: BTreeMap<String, IndexedFileEntry> = BTreeMap::new();
    if let Some(snapshot) = remote_snapshot {
        for entry in &snapshot.entries {
            remote_entries.insert(
                entry.relative_path.clone(),
                IndexedFileEntry {
                    kind: entry.kind.clone(),
                    size: entry.size,
                    storage_class: entry.storage_class.clone(),
                    modified_at: entry.last_modified_at.clone(),
                    etag: entry.etag.clone(),
                },
            );
        }
    }

    let all_paths: BTreeSet<&String> = local_entries.keys().chain(remote_entries.keys()).collect();

    all_paths
        .into_iter()
        .map(|path| {
            let in_local = local_entries.get(path);
            let in_remote = remote_entries.get(path);
            let anchor = anchors.and_then(|anchors| anchors.get(path.as_str()));
            let remote_is_glacier = in_remote.is_some_and(|remote| {
                super::remote_index::is_cold_storage_class(remote.storage_class.as_deref())
            });

            let status = match (in_local, in_remote) {
                (Some(local), Some(remote)) if local.kind != remote.kind => {
                    FileEntryStatus::Conflict
                }
                (Some(local), Some(_remote)) if local.kind == "directory" => {
                    if remote_is_glacier {
                        FileEntryStatus::Glacier
                    } else {
                        FileEntryStatus::Synced
                    }
                }
                (Some(_local), Some(_remote)) => {
                    if remote_is_glacier {
                        FileEntryStatus::Glacier
                    } else {
                        let current_local_fingerprint = local_snapshot
                            .and_then(|snapshot| local_fingerprint_for_path(snapshot, path));
                        let current_remote_etag = remote_snapshot
                            .and_then(|snapshot| remote_etag_for_path(snapshot, path));
                        sync_planner::file_entry_status(
                            anchor,
                            current_local_fingerprint.as_deref(),
                            current_remote_etag.as_deref(),
                        )
                    }
                }
                (Some(_), None) => FileEntryStatus::LocalOnly,
                (None, Some(_remote)) if remote_is_glacier => FileEntryStatus::Glacier,
                (None, Some(_)) => FileEntryStatus::RemoteOnly,
                (None, None) => unreachable!(),
            };

            FileEntryResponse {
                path: path.clone(),
                kind: in_local
                    .map(|entry| entry.kind.clone())
                    .or_else(|| in_remote.map(|entry| entry.kind.clone()))
                    .expect("listed entries must exist in either snapshot"),
                status: status.as_str().into(),
                has_local_copy: in_local.is_some(),
                storage_class: in_remote.and_then(|entry| entry.storage_class.clone()),
                bin_key: None,
                local_kind: in_local.map(|entry| entry.kind.clone()),
                remote_kind: in_remote.map(|entry| entry.kind.clone()),
                local_size: in_local.map(|entry| entry.size),
                remote_size: in_remote.map(|entry| entry.size),
                local_modified_at: in_local.and_then(|entry| entry.modified_at.clone()),
                remote_modified_at: in_remote.and_then(|entry| entry.modified_at.clone()),
                remote_etag: in_remote.and_then(|entry| entry.etag.clone()),
                deleted_at: None,
                deleted_from: None,
                retention_days: None,
                expires_at: None,
            }
        })
        .collect()
}
