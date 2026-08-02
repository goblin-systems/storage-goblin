//! Manual conflict resolution: showing the user both sides of a conflicted
//! path and applying their choice.
//!
//! Resolution is deliberately explicit — the engine never picks a winner for
//! the cases that land here (see sync_planner's decision table).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use tauri::{AppHandle, Runtime};

use super::commands::resolve_credentials_for_pair;
use super::commands::{
    list_remote_inventory_for_pair, storage_config_for_pair, ConflictResolutionDetails,
    FileEntryResponse,
};
use super::compare_service::{finalize_conflict_compare_details, temp_compare_file_path};
use super::credentials_store::StoredCredentials;
use super::file_query_service::{
    build_file_entry_responses, refresh_pair_state_after_local_change,
};
use super::local_index::{read_local_index_snapshot_for_pair, LocalIndexSnapshot};
use super::location_service::sync_pair_for_location;
use super::object_store;
use super::platform::resolve_local_download_path;
use super::profile_store::{read_profile_from_disk, SyncPair};
use super::remote_index::{read_remote_index_snapshot_for_pair, RemoteIndexSnapshot};
use super::s3_adapter;

use super::sync_db::{load_sync_anchors_for_pair, SyncAnchor};
#[cfg(test)]
use super::transfer_service::{
    mock_download_file, mock_upload_refresh_snapshot, planned_transfer_test_mode_enabled,
};
use super::transfer_service::{
    persist_download_success_for_pair, persist_upload_success_for_pair, remote_etag_for_path,
};

pub(crate) fn file_entry_for_conflict(
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    anchors: Option<&BTreeMap<String, SyncAnchor>>,
    path: &str,
) -> Option<FileEntryResponse> {
    build_file_entry_responses(local_snapshot, remote_snapshot, anchors)
        .into_iter()
        .find(|entry| entry.path == path)
}

pub(crate) fn supports_manual_file_resolution(entry: &FileEntryResponse) -> bool {
    entry.kind == "file"
        && matches!(entry.status.as_str(), "conflict" | "review-required")
        && entry.local_kind.as_deref() == Some("file")
        && entry.remote_kind.as_deref() == Some("file")
}

pub(crate) async fn download_remote_file_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    _path: &str,
    key: &str,
    destination_path: &Path,
) -> Result<(), String> {
    #[cfg(test)]
    if planned_transfer_test_mode_enabled() {
        return mock_download_file(_path, destination_path);
    }

    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    object_store::download_file(&client, &pair.bucket, key, destination_path, None)
        .await
        .map_err(String::from)
}

pub(crate) async fn upload_local_file_for_pair_and_refresh_remote(
    pair: &SyncPair,
    credentials: &StoredCredentials,
    _path: &str,
    key: &str,
    local_path: &Path,
    local_fingerprint: &str,
) -> Result<RemoteIndexSnapshot, String> {
    #[cfg(test)]
    if planned_transfer_test_mode_enabled() {
        return mock_upload_refresh_snapshot(_path);
    }

    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    object_store::upload_file(
        &client,
        &pair.bucket,
        key,
        local_path,
        Some(
            BTreeMap::from([(
                s3_adapter::LOCAL_FINGERPRINT_METADATA_KEY.to_string(),
                local_fingerprint.to_string(),
            )])
            .into_iter()
            .collect(),
        ),
    )
    .await?;
    list_remote_inventory_for_pair(pair, credentials)
        .await
        .map_err(String::from)
}

pub(crate) async fn remote_snapshot_for_manual_resolution<R: Runtime>(
    _app: &AppHandle<R>,
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<RemoteIndexSnapshot, String> {
    #[cfg(test)]
    if planned_transfer_test_mode_enabled() {
        return read_remote_index_snapshot_for_pair(_app, &pair.id)?.ok_or_else(|| {
            format!(
                "Remote inventory snapshot for pair '{}' is unavailable.",
                pair.label
            )
        });
    }

    list_remote_inventory_for_pair(pair, credentials)
        .await
        .map_err(String::from)
}

pub(crate) async fn prepare_conflict_comparison_impl<R: Runtime>(
    app: AppHandle<R>,
    location_id: String,
    path: String,
) -> Result<ConflictResolutionDetails, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;
    let normalized_path = path.replace('\\', "/");
    let local_snapshot = read_local_index_snapshot_for_pair(&app, &pair.id)?;
    let remote_snapshot = read_remote_index_snapshot_for_pair(&app, &pair.id)?;
    let anchors = load_sync_anchors_for_pair(&app, &pair)?
        .into_iter()
        .map(|anchor| (anchor.path.clone(), anchor))
        .collect::<BTreeMap<_, _>>();
    let entry = file_entry_for_conflict(
        local_snapshot.as_ref(),
        remote_snapshot.as_ref(),
        Some(&anchors),
        &normalized_path,
    )
    .ok_or_else(|| format!("Resolvable file entry '{}' was not found.", normalized_path))?;

    if !matches!(entry.status.as_str(), "conflict" | "review-required") {
        return Err(format!(
            "'{}' is no longer marked for manual review.",
            normalized_path
        ));
    }

    if !supports_manual_file_resolution(&entry) {
        return Err(
            "This MVP only supports compare for file-vs-file conflict/review-required entries."
                .into(),
        );
    }

    let local_path = resolve_local_download_path(&pair.local_folder, &normalized_path)?;
    let local_path_value = if local_path.exists() {
        Some(local_path.to_string_lossy().into_owned())
    } else {
        None
    };

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let remote_temp_path = temp_compare_file_path(&app, &normalized_path)?;
    let key = s3_adapter::object_key(&normalized_path);
    download_remote_file_for_pair(
        &pair,
        &credentials,
        &normalized_path,
        &key,
        &remote_temp_path,
    )
    .await?;

    Ok(finalize_conflict_compare_details(
        location_id,
        normalized_path,
        local_path_value,
        Some(remote_temp_path.to_string_lossy().into_owned()),
    ))
}

pub(crate) async fn resolve_conflict_impl<R: Runtime>(
    app: AppHandle<R>,
    location_id: String,
    path: String,
    resolution: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;
    let normalized_path = path.replace('\\', "/");
    let local_snapshot = read_local_index_snapshot_for_pair(&app, &pair.id)?;
    let remote_snapshot = read_remote_index_snapshot_for_pair(&app, &pair.id)?;
    let anchors = load_sync_anchors_for_pair(&app, &pair)?
        .into_iter()
        .map(|anchor| (anchor.path.clone(), anchor))
        .collect::<BTreeMap<_, _>>();
    let entry = file_entry_for_conflict(
        local_snapshot.as_ref(),
        remote_snapshot.as_ref(),
        Some(&anchors),
        &normalized_path,
    )
    .ok_or_else(|| format!("Resolvable file entry '{}' was not found.", normalized_path))?;

    if !matches!(entry.status.as_str(), "conflict" | "review-required") {
        return Err(format!(
            "'{}' is no longer marked for manual review.",
            normalized_path
        ));
    }

    if !supports_manual_file_resolution(&entry) {
        return Err(
            "This MVP only supports keep-local/keep-remote for file-vs-file conflict/review-required entries.".into(),
        );
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let local_path = resolve_local_download_path(&pair.local_folder, &normalized_path)?;
    let remote_key = s3_adapter::object_key(&normalized_path);

    match resolution.as_str() {
        "keep-local" => {
            let metadata = fs::metadata(&local_path).map_err(|error| match error.kind() {
                std::io::ErrorKind::NotFound => format!(
                    "Local file '{}' does not exist, so Keep local cannot run.",
                    local_path.display()
                ),
                _ => format!(
                    "Failed to inspect local file '{}': {error}",
                    local_path.display()
                ),
            })?;

            if !metadata.is_file() {
                return Err(format!(
                    "Local conflict source '{}' is not a file. Directory conflicts are not supported in this MVP.",
                    local_path.display()
                ));
            }

            let local_fingerprint = crate::storage::local_index::file_fingerprint(&local_path)?;

            let refreshed_remote_snapshot = upload_local_file_for_pair_and_refresh_remote(
                &pair,
                &credentials,
                &normalized_path,
                &remote_key,
                &local_path,
                &local_fingerprint,
            )
            .await?;

            persist_upload_success_for_pair(
                &app,
                &pair,
                &normalized_path,
                &local_fingerprint,
                &refreshed_remote_snapshot,
            )?;

            refresh_pair_state_after_local_change(&app, &pair).map_err(|error| {
                format!(
                    "Resolved '{}' by keeping local, but refresh failed: {error}",
                    normalized_path
                )
            })
        }
        "keep-remote" => {
            download_remote_file_for_pair(
                &pair,
                &credentials,
                &normalized_path,
                &remote_key,
                &local_path,
            )
            .await?;

            let remote_snapshot =
                remote_snapshot_for_manual_resolution(&app, &pair, &credentials).await?;
            let remote_etag = remote_etag_for_path(&remote_snapshot, &normalized_path);
            persist_download_success_for_pair(
                &app,
                &pair,
                &normalized_path,
                &local_path,
                remote_etag,
            )?;

            refresh_pair_state_after_local_change(&app, &pair).map_err(|error| {
                format!(
                    "Resolved '{}' by keeping remote, but refresh failed: {error}",
                    normalized_path
                )
            })
        }
        _ => Err(format!("Unsupported conflict resolution '{resolution}'.")),
    }
}
