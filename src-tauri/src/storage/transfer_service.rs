//! Executing a single planned operation against a sync location.
//!
//! Uploads and downloads move bytes; deletes, moves, conflict duplication and
//! anchor reconciliation change state without transferring. Everything here
//! acts on one queue item at a time and records the resulting anchor.

use std::collections::BTreeMap;
use std::path::Path;

use tauri::{AppHandle, Runtime};

use super::commands::{
    list_remote_inventory_for_pair, reconcile_remote_bin_lifecycle_target, storage_config_for_pair,
};
use super::credentials_store::StoredCredentials;
use super::lifecycle_service::target_for_pair;
use super::local_index::LocalIndexSnapshot;
use super::now_iso;
use super::object_store;
use super::platform::{
    rename_local_file_for_pair, resolve_local_download_path, trash_local_file_for_pair,
};
use super::profile_store::SyncPair;
use super::remote_bin::deleted_object_key;
use super::remote_index::{
    read_remote_index_snapshot_for_pair, write_remote_index_snapshot_for_pair, RemoteIndexSnapshot,
    RemoteObjectEntry,
};
use super::s3_adapter;
use super::sync_db::{
    delete_sync_anchor_for_pair, upsert_sync_anchor_for_pair, PlannedDownloadQueueItem,
    PlannedUploadQueueItem, SyncAnchor,
};
use super::sync_planner::Operation;

#[cfg(test)]
use super::platform::remove_local_file_without_trash_for_pair;

pub(crate) enum PairTransferExecutor {
    Real(object_store::ObjectStoreClient),
    #[cfg(test)]
    Mock,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct PlannedTransferTestHooks {
    pub(crate) upload_refresh_snapshots: BTreeMap<String, RemoteIndexSnapshot>,
    pub(crate) download_payloads: BTreeMap<String, Vec<u8>>,
}

#[cfg(test)]
pub(crate) fn planned_transfer_test_hooks(
) -> &'static std::sync::Mutex<Option<PlannedTransferTestHooks>> {
    use std::sync::{Mutex, OnceLock};

    static HOOKS: OnceLock<Mutex<Option<PlannedTransferTestHooks>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn set_planned_transfer_test_hooks(hooks: PlannedTransferTestHooks) {
    *planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned") = Some(hooks);
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn clear_planned_transfer_test_hooks() {
    *planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned") = None;
}

#[cfg(test)]
pub(crate) fn planned_transfer_test_mode_enabled() -> bool {
    planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned")
        .is_some()
}

#[cfg(test)]
pub(crate) fn mock_upload_refresh_snapshot(path: &str) -> Result<RemoteIndexSnapshot, String> {
    planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned")
        .as_mut()
        .and_then(|hooks| hooks.upload_refresh_snapshots.remove(path))
        .ok_or_else(|| format!("missing mocked upload refresh snapshot for '{path}'"))
}

#[cfg(test)]
pub(crate) fn mock_download_file(path: &str, local_path: &Path) -> Result<(), String> {
    let payload = planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned")
        .as_mut()
        .and_then(|hooks| hooks.download_payloads.remove(path))
        .ok_or_else(|| format!("missing mocked download payload for '{path}'"))?;
    std::fs::write(local_path, payload).map_err(|error| {
        format!(
            "failed to write mocked download to '{}': {error}",
            local_path.display()
        )
    })
}

pub(crate) async fn build_pair_transfer_executor(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<PairTransferExecutor, String> {
    #[cfg(test)]
    if planned_transfer_test_mode_enabled() {
        return Ok(PairTransferExecutor::Mock);
    }

    Ok(PairTransferExecutor::Real(
        object_store::build_client(&storage_config_for_pair(pair, credentials)).await?,
    ))
}

pub(crate) async fn perform_planned_upload_for_pair(
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    credentials: &StoredCredentials,
    _path: &str,
    key: &str,
    local_path: &Path,
    current_fingerprint: &str,
) -> Result<RemoteIndexSnapshot, String> {
    match executor {
        PairTransferExecutor::Real(client) => {
            object_store::upload_file(
                client,
                &pair.bucket,
                key,
                local_path,
                Some(
                    BTreeMap::from([(
                        s3_adapter::LOCAL_FINGERPRINT_METADATA_KEY.to_string(),
                        current_fingerprint.to_string(),
                    )])
                    .into_iter()
                    .collect(),
                ),
            )
            .await?;
            list_remote_inventory_for_pair(pair, credentials).await
        }
        #[cfg(test)]
        PairTransferExecutor::Mock => mock_upload_refresh_snapshot(_path),
    }
}

pub(crate) async fn perform_planned_download_for_pair(
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    key: &str,
    _path: &str,
    local_path: &Path,
) -> Result<(), String> {
    match executor {
        PairTransferExecutor::Real(client) => {
            object_store::download_file(client, &pair.bucket, key, local_path)
                .await
                .map_err(String::from)
        }
        #[cfg(test)]
        PairTransferExecutor::Mock => mock_download_file(_path, local_path),
    }
}

/// Remove a remote object, honoring the pair's protection setting: object
/// versioning leaves a delete marker, remote bin moves the object into the bin
/// namespace, and only an unprotected pair hard-deletes. Mirrors the manual
/// `delete_file` command so planned and manual deletes behave identically.
pub(crate) async fn delete_remote_object_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    path: &str,
) -> Result<(), String> {
    let key = s3_adapter::object_key(path);
    match executor {
        PairTransferExecutor::Real(client) => {
            if !pair.object_versioning_enabled && pair.remote_bin.enabled {
                if let Some(target) = target_for_pair(pair) {
                    reconcile_remote_bin_lifecycle_target(app, &target).await?;
                }
                let bin_key = deleted_object_key(&pair.id, path);
                object_store::move_object(client, &pair.bucket, &key, &bin_key, None)
                    .await
                    .map_err(String::from)
            } else {
                // Versioning leaves a restorable delete marker; without either
                // protection this is a plain delete.
                object_store::delete_object(client, &pair.bucket, &key)
                    .await
                    .map_err(String::from)
            }
        }
        #[cfg(test)]
        PairTransferExecutor::Mock => Ok(()),
    }
}

/// Write the anchor for `path` from the local file's fingerprint and the
/// object's current etag in `snapshot`.
pub(crate) fn anchor_path_from_snapshot<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    path: &str,
    local_fingerprint: &str,
    snapshot: &RemoteIndexSnapshot,
) -> Result<(), String> {
    upsert_sync_anchor_for_pair(
        app,
        pair,
        &sync_anchor_from_upload(
            path,
            local_fingerprint,
            remote_etag_for_path(snapshot, path),
        ),
    )
}

/// Execute the phase-1 operations that are not file transfers. Returns `None`
/// when `item.operation` is a transfer the caller should handle itself.
pub(crate) async fn perform_structural_upload_operation_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    credentials: &StoredCredentials,
    item: &PlannedUploadQueueItem,
) -> Option<Result<String, String>> {
    let operation = Operation::parse(&item.operation)?;

    let result = match operation {
        Operation::DeleteRemote => {
            match delete_remote_object_for_pair(app, executor, pair, &item.path).await {
                Ok(()) => delete_sync_anchor_for_pair(app, pair, &item.path)
                    .map(|()| format!("Deleted remote copy of '{}'.", item.path)),
                Err(error) => Err(error),
            }
        }
        Operation::MoveRemote => {
            let Some(target) = item.target_path.as_deref() else {
                return Some(Err(format!(
                    "planned remote move for '{}' is missing its destination",
                    item.path
                )));
            };
            move_remote_object_for_pair(app, executor, pair, credentials, &item.path, target).await
        }
        Operation::DuplicateConflict => {
            let Some(target) = item.target_path.as_deref() else {
                return Some(Err(format!(
                    "planned conflict copy for '{}' is missing its destination",
                    item.path
                )));
            };
            duplicate_conflict_for_pair(app, executor, pair, credentials, &item.path, target).await
        }
        Operation::AnchorOnly => anchor_only_for_pair(app, pair, &item.path),
        Operation::ForgetAnchor => delete_sync_anchor_for_pair(app, pair, &item.path)
            .map(|()| format!("Cleared stale sync record for '{}'.", item.path)),
        Operation::Upload | Operation::CreateDirectory => return None,
        // Download-queue operations never reach the upload executor.
        Operation::Download
        | Operation::DeleteLocal
        | Operation::MoveLocal
        | Operation::ConflictReview
        | Operation::ReviewRequired => return None,
    };

    Some(result)
}

/// Server-side copy + delete, then re-anchor the destination path.
pub(crate) async fn move_remote_object_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    credentials: &StoredCredentials,
    from: &str,
    to: &str,
) -> Result<String, String> {
    let from_key = s3_adapter::object_key(from);
    let to_key = s3_adapter::object_key(to);

    match executor {
        PairTransferExecutor::Real(client) => {
            object_store::move_object(client, &pair.bucket, &from_key, &to_key, None).await?;
        }
        #[cfg(test)]
        PairTransferExecutor::Mock => {}
    }

    delete_sync_anchor_for_pair(app, pair, from)?;

    // Anchor the destination so the next cycle sees a settled path rather than
    // an unanchored file it would have to review.
    let local_path = resolve_local_download_path(&pair.local_folder, to)?;
    if let Ok(fingerprint) = crate::storage::local_index::file_fingerprint(&local_path) {
        let snapshot = refresh_remote_snapshot_for_pair(executor, pair, credentials, to).await?;
        write_remote_index_snapshot_for_pair(app, &pair.id, &snapshot)?;
        anchor_path_from_snapshot(app, pair, to, &fingerprint, &snapshot)?;
    }

    Ok(format!("Moved remote copy of '{from}' to '{to}'."))
}

/// preserve-both: rename the local file to the conflict name and upload it.
/// The paired download restores the remote version at the original path.
pub(crate) async fn duplicate_conflict_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    credentials: &StoredCredentials,
    path: &str,
    target: &str,
) -> Result<String, String> {
    rename_local_file_for_pair(pair, path, target)?;

    let local_path = resolve_local_download_path(&pair.local_folder, target)?;
    let fingerprint = crate::storage::local_index::file_fingerprint(&local_path)?;
    let key = s3_adapter::object_key(target);

    let snapshot = perform_planned_upload_for_pair(
        executor,
        pair,
        credentials,
        target,
        &key,
        &local_path,
        &fingerprint,
    )
    .await?;

    write_remote_index_snapshot_for_pair(app, &pair.id, &snapshot)?;
    anchor_path_from_snapshot(app, pair, target, &fingerprint, &snapshot)?;
    // The original path is re-anchored by the paired download.
    delete_sync_anchor_for_pair(app, pair, path)?;

    Ok(format!(
        "Kept both versions of '{path}': your copy is now '{target}'."
    ))
}

/// Record an anchor for content that already matches on both sides, so a
/// first sync over pre-existing data transfers nothing.
pub(crate) fn anchor_only_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    path: &str,
) -> Result<String, String> {
    let local_path = resolve_local_download_path(&pair.local_folder, path)?;
    let fingerprint = crate::storage::local_index::file_fingerprint(&local_path)?;
    let snapshot = read_remote_index_snapshot_for_pair(app, &pair.id)?
        .ok_or_else(|| "remote snapshot missing while anchoring existing content".to_string())?;

    // Re-verify before recording a match. Anchoring two different files as
    // "already in sync" would silently strand one of them, so a stale or
    // fingerprint-less snapshot must fail rather than guess.
    let remote_fingerprint = snapshot
        .entries
        .iter()
        .find(|entry| entry.relative_path == path && entry.kind == "file")
        .and_then(|entry| entry.fingerprint.clone());
    if remote_fingerprint.as_deref() != Some(fingerprint.as_str()) {
        return Err(format!(
            "content for '{path}' no longer matches the remote copy; leaving it for review"
        ));
    }

    anchor_path_from_snapshot(app, pair, path, &fingerprint, &snapshot)?;
    Ok(format!("Matched existing content for '{path}'."))
}

pub(crate) async fn refresh_remote_snapshot_for_pair(
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    credentials: &StoredCredentials,
    _path: &str,
) -> Result<RemoteIndexSnapshot, String> {
    match executor {
        PairTransferExecutor::Real(_) => list_remote_inventory_for_pair(pair, credentials).await,
        #[cfg(test)]
        PairTransferExecutor::Mock => mock_upload_refresh_snapshot(_path),
    }
}

/// Execute the download-queue operations that are not file transfers.
pub(crate) fn perform_structural_download_operation_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    executor: &PairTransferExecutor,
    pair: &SyncPair,
    item: &PlannedDownloadQueueItem,
) -> Option<Result<String, String>> {
    let operation = Operation::parse(&item.operation)?;

    let result = match operation {
        Operation::DeleteLocal => {
            let removed = match executor {
                PairTransferExecutor::Real(_) => trash_local_file_for_pair(pair, &item.path),
                // Tests assert on the resulting tree, not on OS trash behavior.
                #[cfg(test)]
                PairTransferExecutor::Mock => {
                    remove_local_file_without_trash_for_pair(pair, &item.path)
                }
            };
            match removed {
                Ok(()) => delete_sync_anchor_for_pair(app, pair, &item.path)
                    .map(|()| format!("Deleted local copy of '{}'.", item.path)),
                Err(error) => Err(error),
            }
        }
        Operation::MoveLocal => {
            let Some(target) = item.target_path.as_deref() else {
                return Some(Err(format!(
                    "planned local move for '{}' is missing its destination",
                    item.path
                )));
            };
            match rename_local_file_for_pair(pair, &item.path, target) {
                Ok(()) => delete_sync_anchor_for_pair(app, pair, &item.path)
                    .map(|()| format!("Moved local copy of '{}' to '{target}'.", item.path)),
                Err(error) => Err(error),
            }
        }
        _ => return None,
    };

    Some(result)
}

pub(crate) fn persist_upload_success_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    path: &str,
    local_fingerprint: &str,
    refreshed_remote_snapshot: &RemoteIndexSnapshot,
) -> Result<(), String> {
    write_remote_index_snapshot_for_pair(app, &pair.id, refreshed_remote_snapshot)?;
    upsert_sync_anchor_for_pair(
        app,
        pair,
        &sync_anchor_from_upload(
            path,
            local_fingerprint,
            remote_etag_for_path(refreshed_remote_snapshot, path),
        ),
    )
}

pub(crate) fn persist_download_success_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    path: &str,
    local_path: &Path,
    current_remote_etag: Option<String>,
) -> Result<(), String> {
    let downloaded_fingerprint = crate::storage::local_index::file_fingerprint(local_path)?;
    upsert_sync_anchor_for_pair(
        app,
        pair,
        &sync_anchor_from_download(path, &downloaded_fingerprint, current_remote_etag),
    )
}

pub(crate) fn upload_stale_plan_error(
    local_path: &Path,
    current_local_fingerprint: &str,
    expected_local_fingerprint: Option<&str>,
    current_remote_etag: Option<&str>,
    expected_remote_etag: Option<&str>,
) -> Option<String> {
    if expected_local_fingerprint != Some(current_local_fingerprint) {
        return Some(format!(
            "planned upload source '{}' changed on disk since planning (fingerprint mismatch)",
            local_path.display()
        ));
    }

    if current_remote_etag != expected_remote_etag {
        return Some(format!(
            "planned upload target '{}' changed remotely since planning",
            local_path.display()
        ));
    }

    None
}

pub(crate) fn download_stale_plan_error(
    local_path: &Path,
    current_local_fingerprint: Option<&str>,
    expected_local_fingerprint: Option<&str>,
    current_remote_etag: Option<&str>,
    expected_remote_etag: Option<&str>,
) -> Option<String> {
    if current_remote_etag != expected_remote_etag {
        return Some(format!(
            "planned download source '{}' changed remotely since planning",
            local_path.display()
        ));
    }

    if current_local_fingerprint != expected_local_fingerprint {
        return Some(format!(
            "planned download destination '{}' changed locally since planning",
            local_path.display()
        ));
    }

    None
}

pub(crate) fn current_remote_entry<'a>(
    snapshot: &'a RemoteIndexSnapshot,
    path: &str,
) -> Option<&'a RemoteObjectEntry> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.relative_path == path)
}

pub(crate) fn remote_etag_for_path(snapshot: &RemoteIndexSnapshot, path: &str) -> Option<String> {
    current_remote_entry(snapshot, path).and_then(|entry| entry.etag.clone())
}

pub(crate) fn local_fingerprint_for_path(
    snapshot: &LocalIndexSnapshot,
    path: &str,
) -> Option<String> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.relative_path == path && entry.kind == "file")
        .and_then(|entry| entry.fingerprint.clone())
}

pub(crate) fn sync_anchor_from_upload(
    path: &str,
    fingerprint: &str,
    remote_etag: Option<String>,
) -> SyncAnchor {
    SyncAnchor {
        path: path.into(),
        kind: "file".into(),
        local_fingerprint: Some(fingerprint.into()),
        remote_etag,
        synced_at: now_iso(),
    }
}

pub(crate) fn sync_anchor_from_download(
    path: &str,
    fingerprint: &str,
    remote_etag: Option<String>,
) -> SyncAnchor {
    SyncAnchor {
        path: path.into(),
        kind: "file".into(),
        local_fingerprint: Some(fingerprint.into()),
        remote_etag,
        synced_at: now_iso(),
    }
}
