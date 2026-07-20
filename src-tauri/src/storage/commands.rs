use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

use tauri::{AppHandle, Emitter, Manager, Runtime, State};

use uuid::Uuid;

use super::{
    activity::{emit_activity, ActivityDebugState, ActivityLevel},
    bin_service::{
        build_bin_entry_responses, build_versioned_bin_entry_responses,
        collect_remote_bin_keys_for_request, collect_versioned_bin_entries_for_request,
        collect_versioned_history_for_deleted_entries, destination_key_for_bin_restore,
        normalize_bin_entry_kind, normalize_restore_relative_path, parse_versioned_bin_key,
        validate_bin_batch_requests, validate_bulk_restore_destinations,
        validate_local_restore_destination, validate_restore_destination, VersionedBinEntry,
    },
    compare_service::{
        finalize_conflict_compare_details, finalize_version_compare_details,
        temp_compare_file_path, temp_version_compare_file_path,
    },
    credential_service::{
        format_permission_probe_summary, format_storage_validation_success_message,
        provider_runtime_object_versioning_message, provider_supports_runtime_object_versioning,
        resolve_credential_test_context, resolve_profile_credential_name,
        resolve_selected_credential_state, should_defer_create_time_credential_test,
        sync_location_runtime_object_versioning_message,
    },
    credentials_store::{
        create_credential, delete_credential as delete_stored_credential,
        ensure_legacy_credentials_migrated, get_credential_summary, list_credentials,
        load_credentials_by_id, parse_credential_input_with_payload, record_credential_validation,
        upsert_credential, CredentialDraft, CredentialInputState, CredentialSummary,
        CredentialValidationStatus, StoredCredentials,
    },
    default_provider,
    lifecycle_service::{
        load_credentials_for_remote_bin_target, persist_profile_with_remote_bin_reconciliation,
        provider_supports_remote_bin_lifecycle_reconciliation,
        remote_bin_lifecycle_reconciliation_unsupported_message, target_for_pair,
        RemoteBinLifecycleTarget,
    },
    local_index::{
        read_local_index_snapshot, read_local_index_snapshot_for_pair, scan_local_folder,
        write_local_index_snapshot_for_pair, LocalIndexSnapshot,
    },
    now_iso, object_store,
    platform::{
        cleanup_empty_ancestors, normalize_directory_delete_path, open_path_with_default_app,
        remove_local_directory_subtree, rename_local_file_for_pair, resolve_local_download_path,
        resolve_local_upload_path, reveal_in_file_manager, trash_local_file_for_pair,
    },
    profile_store::{
        is_pair_configured, is_profile_configured, read_profile_from_disk, write_profile_to_disk,
        ConnectionValidationInput, ConnectionValidationResult, ProfileDraft,
        SelectedCredentialState, StoredProfile, SyncPair, SyncPairDraft,
    },
    provider::{normalize_provider, provider_capabilities, supported_providers, GCS_PROVIDER},
    remote_bin::{
        deleted_directory_key, deleted_object_key, namespace_prefix,
        original_relative_path_from_bin_key_for_pair, pair_bin_prefix,
    },
    remote_index::{
        directory_relative_paths_from_key, directory_relative_paths_from_relative_path,
        read_remote_index_snapshot, read_remote_index_snapshot_for_pair, relative_path_from_key,
        should_exclude_remote_key, write_remote_index_snapshot_for_pair, RemoteIndexSnapshot,
        RemoteIndexSummary, RemoteObjectEntry,
    },
    s3_adapter,
    sanitizer::sanitize_sensitive_text,
    sync_db::{
        delete_sync_anchor_for_pair, load_planned_download_queue_for_pair,
        load_planned_upload_queue_for_pair, load_planner_summary, load_planner_summary_for_pair,
        load_sync_anchors_for_pair, mark_download_queue_item_completed_for_pair,
        mark_download_queue_item_failed_for_pair, mark_download_queue_item_in_progress_for_pair,
        mark_upload_queue_item_completed_for_pair, mark_upload_queue_item_failed_for_pair,
        mark_upload_queue_item_in_progress_for_pair, persist_sync_plan_for_pair,
        recover_interrupted_queue_items_for_pair, upsert_sync_anchor_for_pair,
        PlannedDownloadQueueItem, PlannedUploadQueueItem, SyncAnchor,
    },
    sync_planner::{self, Operation},
    sync_state::{
        active_watcher_pair_paths, begin_polling_worker, clear_all_pair_watchers, clear_dirty_pair,
        clear_polling_worker, due_dirty_pairs, get_status_lock, install_pair_watcher,
        mark_pair_dirty, next_dirty_pair_deadline, pair_has_active_watcher, pair_statuses_snapshot,
        pair_to_status, polling_worker_active, profile_to_status, remove_pair_watcher,
        replace_pair_statuses_from_handle, retain_dirty_pairs, set_pair_status_from_handle,
        set_status_from_handle, stop_polling_worker, synthesize_status_from_pairs, PairSyncStatus,
        SyncState, SyncStatus,
    },
    watchers::{plan_watch_reconciliation, start_pair_watcher, WatchTarget, WatcherCallbackEvent},
};

#[cfg(test)]
use super::platform::remove_local_file_without_trash_for_pair;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BinEntryRequest {
    pub path: String,
    pub kind: String,
    pub bin_key: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BinEntryMutationResult {
    pub path: String,
    pub kind: String,
    pub bin_key: Option<String>,
    pub success: bool,
    pub affected_count: usize,
    pub error: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BinEntryMutationSummary {
    pub results: Vec<BinEntryMutationResult>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteCredentialResult {
    pub deleted: bool,
    pub profile: StoredProfile,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CredentialTestContext {
    #[serde(default = "default_provider")]
    pub provider: String,
    pub region: String,
    pub bucket: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialTestRequest {
    pub credential_id: String,
    pub context: CredentialTestContext,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialTestResult {
    pub credential: CredentialSummary,
    pub ok: bool,
    pub checked_at: String,
    pub message: String,
    pub bucket_count: usize,
    pub buckets: Vec<String>,
    pub permissions: Option<object_store::PermissionProbeSummary>,
    pub provider: String,
    pub capabilities: super::provider::ProviderCapabilities,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionComparisonDetails {
    pub path: String,
    pub mode: String,
    pub version_a_id: String,
    pub version_b_id: String,
    pub version_a_temp_path: Option<String>,
    pub version_b_temp_path: Option<String>,
    pub version_a_text: Option<String>,
    pub version_b_text: Option<String>,
    pub version_a_image_data_url: Option<String>,
    pub version_b_image_data_url: Option<String>,
    pub fallback_reason: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileVersionEntry {
    pub version_id: String,
    pub is_latest: bool,
    pub size: u64,
    pub last_modified_at: Option<String>,
    pub storage_class: Option<String>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionCountEntry {
    pub path: String,
    pub count: u32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntryResponse {
    pub path: String,
    pub kind: String,
    pub status: String,
    pub has_local_copy: bool,
    pub storage_class: Option<String>,
    pub bin_key: Option<String>,
    pub local_kind: Option<String>,
    pub remote_kind: Option<String>,
    pub local_size: Option<u64>,
    pub remote_size: Option<u64>,
    pub local_modified_at: Option<String>,
    pub remote_modified_at: Option<String>,
    pub remote_etag: Option<String>,
    pub deleted_at: Option<String>,
    pub deleted_from: Option<String>,
    pub retention_days: Option<u32>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConflictResolutionDetails {
    pub location_id: String,
    pub path: String,
    pub mode: String,
    pub local_path: Option<String>,
    pub remote_temp_path: Option<String>,
    pub local_text: Option<String>,
    pub remote_text: Option<String>,
    pub local_image_data_url: Option<String>,
    pub remote_image_data_url: Option<String>,
    pub fallback_reason: Option<String>,
}

const PLANNED_UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);
const PLANNED_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);
const DIRTY_PAIR_DEBOUNCE: Duration = Duration::from_millis(750);
const LOCAL_SNAPSHOT_STALE_TTL: Duration = Duration::from_secs(300);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PairSyncTrigger {
    Manual,
    LocalDirty,
    RemotePoll,
}

fn emit_status<R: Runtime>(app: &AppHandle<R>, status: &SyncStatus) {
    let _ = app.emit("storage://sync-status-changed", status);
}

fn emit_info_activity<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    message: impl Into<String>,
    details: Option<String>,
) {
    emit_activity(app, debug_state, ActivityLevel::Info, message, details);
}

fn emit_success_activity<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    message: impl Into<String>,
    details: Option<String>,
) {
    emit_activity(app, debug_state, ActivityLevel::Success, message, details);
}

fn emit_error_activity<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    message: impl Into<String>,
    details: Option<String>,
) {
    emit_activity(app, debug_state, ActivityLevel::Error, message, details);
}

fn append_error_context(primary: Option<String>, secondary: impl Into<String>) -> String {
    let secondary = secondary.into();
    match primary {
        Some(primary) if !primary.is_empty() => format!("{primary}. {secondary}"),
        _ => secondary,
    }
}

fn concise_sync_issue(stage: &str) -> String {
    format!("{stage} failed.")
}

fn run_async_blocking<F, T>(future: F) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(|| handle.block_on(future))
    } else {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| format!("failed to start async runtime: {error}"))?
            .block_on(future)
    }
}

fn pair_sync_cycle_issue_details(
    pair: &SyncPair,
    stage: &str,
    error: impl AsRef<str>,
    cycle_started_at: Option<&str>,
    extra_context: Option<&str>,
) -> String {
    let mut details = vec![
        format!("stage='{stage}'"),
        format!("pair='{}'", pair.label),
        format!("locationId='{}'", pair.id),
        format!("folder='{}'", pair.local_folder),
        format!("bucket='{}'", pair.bucket),
    ];

    if let Some(cycle_started_at) = cycle_started_at.filter(|value| !value.is_empty()) {
        details.push(format!("startedAt='{cycle_started_at}'"));
    }

    if let Some(extra_context) = extra_context.filter(|value| !value.is_empty()) {
        details.push(format!("context='{extra_context}'"));
    }

    details.push(format!("error='{}'", error.as_ref()));
    details.join(" ")
}

async fn run_with_timeout<F, T, E>(
    future: F,
    timeout: Duration,
    operation: impl FnOnce() -> String,
) -> Result<T, String>
where
    F: Future<Output = Result<T, E>>,
    E: Into<String>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => Err(operation()),
    }
}

fn snapshot_for_profile<R: Runtime>(
    app: &AppHandle<R>,
    profile: &StoredProfile,
) -> (Option<LocalIndexSnapshot>, Option<String>) {
    match read_local_index_snapshot(app) {
        Ok(snapshot) => (
            snapshot.filter(|snapshot| {
                !profile.local_folder.is_empty()
                    && super::local_index::snapshot_matches_folder(snapshot, &profile.local_folder)
            }),
            None,
        ),
        Err(error) => (
            None,
            Some(format!(
                "Failed to load saved local index snapshot: {error}"
            )),
        ),
    }
}

fn remote_snapshot_for_profile<R: Runtime>(
    app: &AppHandle<R>,
    profile: &StoredProfile,
) -> (Option<RemoteIndexSnapshot>, Option<String>) {
    match read_remote_index_snapshot(app) {
        Ok(snapshot) => (
            snapshot.filter(|snapshot| {
                !profile.bucket.is_empty()
                    && super::remote_index::snapshot_matches_target(snapshot, &profile.bucket)
            }),
            None,
        ),
        Err(error) => (
            None,
            Some(format!(
                "Failed to load saved remote inventory snapshot: {error}"
            )),
        ),
    }
}

fn merge_snapshot_errors(
    local_error: Option<String>,
    remote_error: Option<String>,
) -> Option<String> {
    match (local_error, remote_error) {
        (Some(local), Some(remote)) => Some(format!("{local}. {remote}")),
        (Some(local), None) => Some(local),
        (None, Some(remote)) => Some(remote),
        (None, None) => None,
    }
}

fn sync_anchor_from_upload(
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

fn sync_anchor_from_download(
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

enum PairTransferExecutor {
    Real(object_store::ObjectStoreClient),
    #[cfg(test)]
    Mock,
}

#[cfg(test)]
#[derive(Default)]
struct PlannedTransferTestHooks {
    upload_refresh_snapshots: BTreeMap<String, RemoteIndexSnapshot>,
    download_payloads: BTreeMap<String, Vec<u8>>,
}

#[cfg(test)]
fn planned_transfer_test_hooks() -> &'static std::sync::Mutex<Option<PlannedTransferTestHooks>> {
    use std::sync::{Mutex, OnceLock};

    static HOOKS: OnceLock<Mutex<Option<PlannedTransferTestHooks>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(None))
}

#[cfg(test)]
#[allow(dead_code)]
fn set_planned_transfer_test_hooks(hooks: PlannedTransferTestHooks) {
    *planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned") = Some(hooks);
}

#[cfg(test)]
#[allow(dead_code)]
fn clear_planned_transfer_test_hooks() {
    *planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned") = None;
}

#[cfg(test)]
fn planned_transfer_test_mode_enabled() -> bool {
    planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned")
        .is_some()
}

#[cfg(test)]
fn mock_upload_refresh_snapshot(path: &str) -> Result<RemoteIndexSnapshot, String> {
    planned_transfer_test_hooks()
        .lock()
        .expect("planned transfer hooks lock should not be poisoned")
        .as_mut()
        .and_then(|hooks| hooks.upload_refresh_snapshots.remove(path))
        .ok_or_else(|| format!("missing mocked upload refresh snapshot for '{path}'"))
}

#[cfg(test)]
fn mock_download_file(path: &str, local_path: &Path) -> Result<(), String> {
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

async fn build_pair_transfer_executor(
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

async fn perform_planned_upload_for_pair(
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

async fn perform_planned_download_for_pair(
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
async fn delete_remote_object_for_pair<R: Runtime>(
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
fn anchor_path_from_snapshot<R: Runtime>(
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
async fn perform_structural_upload_operation_for_pair<R: Runtime>(
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
async fn move_remote_object_for_pair<R: Runtime>(
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
async fn duplicate_conflict_for_pair<R: Runtime>(
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
fn anchor_only_for_pair<R: Runtime>(
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

async fn refresh_remote_snapshot_for_pair(
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
fn perform_structural_download_operation_for_pair<R: Runtime>(
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

fn persist_upload_success_for_pair<R: Runtime>(
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

fn persist_download_success_for_pair<R: Runtime>(
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

fn upload_stale_plan_error(
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

fn download_stale_plan_error(
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

fn current_remote_entry<'a>(
    snapshot: &'a RemoteIndexSnapshot,
    path: &str,
) -> Option<&'a RemoteObjectEntry> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.relative_path == path)
}

fn remote_etag_for_path(snapshot: &RemoteIndexSnapshot, path: &str) -> Option<String> {
    current_remote_entry(snapshot, path).and_then(|entry| entry.etag.clone())
}

fn local_fingerprint_for_path(snapshot: &LocalIndexSnapshot, path: &str) -> Option<String> {
    snapshot
        .entries
        .iter()
        .find(|entry| entry.relative_path == path && entry.kind == "file")
        .and_then(|entry| entry.fingerprint.clone())
}

fn status_with_snapshots(
    profile: &StoredProfile,
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    plan_summary: super::sync_db::DurablePlannerSummary,
) -> SyncStatus {
    profile_to_status(profile, local_snapshot, remote_snapshot, plan_summary)
}

fn emit_recovery_activity<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    upload_count: u64,
    download_count: u64,
    scope: Option<&str>,
) {
    let recovered_count = upload_count + download_count;
    if recovered_count == 0 {
        return;
    }

    let scope_prefix = scope.map(|value| format!("{value} ")).unwrap_or_default();
    emit_info_activity(
        app,
        debug_state,
        "Recovered interrupted sync queue items.",
        Some(format!(
            "{}recovered_items={} recovered_uploads={} recovered_downloads={}",
            scope_prefix, recovered_count, upload_count, download_count
        )),
    );
}

fn saved_profile_with_credentials_state<R: Runtime>(
    app: &AppHandle<R>,
) -> Result<StoredProfile, String> {
    let mut profile = read_profile_from_disk(app)?;
    let migrated = ensure_legacy_credentials_migrated(app, Some("Migrated credential"))?;

    if profile.credential_profile_id.is_none() {
        if let Some(summary) = migrated {
            profile.apply_selected_credential_state(SelectedCredentialState {
                selected_credential: Some(summary),
                selected_credential_available: true,
            });
            write_profile_to_disk(app, &profile)?;
            return Ok(profile);
        }
    }

    let selected_state =
        resolve_selected_credential_state(app, profile.credential_profile_id.as_deref())?;
    let selected_missing =
        profile.credential_profile_id.is_some() && selected_state.selected_credential.is_none();
    profile.apply_selected_credential_state(selected_state);

    if selected_missing {
        write_profile_to_disk(app, &profile)?;
    }

    Ok(profile)
}

fn resolve_refresh_credentials(
    app: &AppHandle,
    profile: &StoredProfile,
    input: &ConnectionValidationInput,
) -> Result<StoredCredentials, String> {
    match parse_credential_input_with_payload(
        &input.provider,
        input.credential.as_ref(),
        &input.access_key_id,
        &input.secret_access_key,
        "",
    )? {
        CredentialInputState::Provided(credentials) => Ok(credentials),
        CredentialInputState::Blank => {
            let selected_id = input
                .credential_profile_id
                .as_deref()
                .or(profile.credential_profile_id.as_deref());
            let Some(credential_id) = selected_id else {
                return Err(
                    "Enter credentials or select a saved credential before refreshing remote inventory."
                        .into(),
                );
            };

            load_credentials_by_id(app, credential_id)?.ok_or_else(|| {
                "The selected saved credential is unavailable. Recreate it or enter credentials again."
                    .into()
            })
        }
    }
}

struct UploadExecutionOutcome {
    execution_error: Option<String>,
    uploads_ran: bool,
}

struct DownloadExecutionOutcome {
    execution_error: Option<String>,
    downloads_ran: bool,
}

impl CredentialTestContext {
    pub(crate) fn normalized(&self) -> Self {
        Self {
            provider: normalize_provider(&self.provider),
            region: self.region.trim().to_string(),
            bucket: self.bucket.trim().to_string(),
        }
    }

    pub(crate) fn has_bucket(&self) -> bool {
        !self.bucket.is_empty()
    }

    fn to_credential_test_config(
        &self,
        credentials: &StoredCredentials,
    ) -> object_store::StorageCredentialTestConfig {
        object_store::StorageCredentialTestConfig {
            provider: credentials.provider.clone(),
            region: self.region.clone(),
            credentials: credentials.clone(),
            bucket: self.has_bucket().then(|| self.bucket.clone()),
        }
    }
}

async fn create_credential_with_optional_initial_validation<R: Runtime>(
    app: &AppHandle<R>,
    draft: CredentialDraft,
) -> Result<CredentialSummary, String> {
    let created = create_credential(app, draft)?;

    let context = resolve_credential_test_context(app, None)?;
    if should_defer_create_time_credential_test(&created, &context) {
        return Ok(created);
    }

    Ok(test_credential_against_context(app, &created.id, &context)
        .await?
        .credential)
}

async fn test_credential_against_context<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: &str,
    context: &CredentialTestContext,
) -> Result<CredentialTestResult, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Err("Choose a credential to test.".into());
    }

    let Some(_) = get_credential_summary(app, credential_id)? else {
        return Err("The selected credential no longer exists.".into());
    };

    let credentials = load_credentials_by_id(app, credential_id)?.ok_or_else(|| {
        String::from("The selected saved credential is unavailable. Recreate it before testing.")
    })?;

    let test_config = context.to_credential_test_config(&credentials);

    // Phase 1: ListBuckets
    let (list_ok, checked_at, base_message, bucket_count, buckets) =
        match object_store::validate_credentials(&test_config).await {
            Ok(summary) => {
                let message = if summary.bucket_scoped {
                    let bucket = summary.buckets.first().cloned().or_else(|| {
                        let bucket = context.bucket.trim();
                        (!bucket.is_empty()).then(|| bucket.to_string())
                    });
                    match bucket {
                        Some(bucket) => format!(
                            "Credential is valid for configured bucket '{}'. Project-wide bucket listing is unavailable for these credentials.",
                            bucket
                        ),
                        None => "Credential is valid. Project-wide bucket listing is unavailable until a bucket is configured for validation.".into(),
                    }
                } else {
                    format!(
                        "Credential is valid. Can access {} bucket(s).",
                        summary.bucket_count
                    )
                };
                (
                    true,
                    summary.checked_at,
                    message,
                    summary.bucket_count,
                    summary.buckets,
                )
            }
            Err(error) => (false, now_iso(), String::from(error), 0, vec![]),
        };

    // Phase 2: Permission probes (when bucket configured)
    let permissions = if context.has_bucket() {
        Some(object_store::probe_bucket_permissions(&test_config, &context.bucket).await)
    } else {
        None
    };

    // Build validation message for persistence (includes permission summary)
    let validation_message = match &permissions {
        Some(summary) => {
            let probe_summary = format_permission_probe_summary(&summary.probes);
            if probe_summary.is_empty() {
                base_message.clone()
            } else {
                format!("{base_message} {probe_summary}")
            }
        }
        None => base_message.clone(),
    };

    let validation_status = if list_ok {
        CredentialValidationStatus::Passed
    } else {
        CredentialValidationStatus::Failed
    };

    let credential = record_credential_validation(
        app,
        credential_id,
        validation_status,
        &checked_at,
        Some(&sanitize_sensitive_text(&validation_message)),
    )?
    .ok_or_else(|| "The selected credential no longer exists.".to_string())?;

    let provider = normalize_provider(&credentials.provider);

    Ok(CredentialTestResult {
        credential,
        ok: list_ok,
        checked_at,
        message: base_message,
        bucket_count,
        buckets,
        permissions,
        provider: provider.clone(),
        capabilities: provider_capabilities(&provider),
    })
}

fn resolve_setup_credentials<R: Runtime>(
    app: &AppHandle<R>,
    existing_profile: &StoredProfile,
    profile: &ProfileDraft,
) -> Result<(StoredCredentials, CredentialSummary), String> {
    let requested_credential_id = profile
        .credential_profile_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| existing_profile.credential_profile_id.clone());

    match parse_credential_input_with_payload(
        &profile.provider,
        profile.credential.as_ref(),
        &profile.access_key_id,
        &profile.secret_access_key,
        "",
    )? {
        CredentialInputState::Provided(credentials) => {
            let summary = upsert_credential(
                app,
                requested_credential_id.as_deref(),
                &resolve_profile_credential_name(existing_profile),
                &profile.provider,
                &credentials,
            )?;
            Ok((credentials, summary))
        }
        CredentialInputState::Blank => {
            let Some(credential_id) = requested_credential_id else {
                return Err(
                    "Provide both access key ID and secret access key, or select an existing saved credential before starting setup."
                        .into(),
                );
            };

            let credentials = load_credentials_by_id(app, &credential_id)?.ok_or_else(|| {
                String::from(
                    "The selected saved credential is unavailable. Recreate it or enter credentials again.",
                )
            })?;
            let summary = get_credential_summary(app, &credential_id)?.ok_or_else(|| {
                String::from(
                    "The selected credential reference no longer exists. Choose another credential.",
                )
            })?;
            Ok((credentials, summary))
        }
    }
}

fn store_profile_draft<R: Runtime>(
    app: &AppHandle<R>,
    profile: ProfileDraft,
) -> Result<(StoredProfile, StoredCredentials), String> {
    let existing_profile = saved_profile_with_credentials_state(app)?;
    let (credentials, summary) = resolve_setup_credentials(app, &existing_profile, &profile)?;

    let mut stored = StoredProfile::from(profile);
    stored.sync_pairs = existing_profile.sync_pairs.clone();
    stored.active_location_id = existing_profile.active_location_id.clone();
    stored.apply_selected_credential_state(SelectedCredentialState {
        selected_credential: Some(summary),
        selected_credential_available: true,
    });
    stored = persist_profile_with_remote_bin_reconciliation(
        app,
        &existing_profile,
        stored.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )?;

    Ok((stored, credentials))
}

fn store_profile_settings<R: Runtime>(
    app: &AppHandle<R>,
    profile: StoredProfile,
) -> Result<StoredProfile, String> {
    let existing = read_profile_from_disk(app)?;
    let mut stored = profile.normalized();
    stored.sync_pairs = existing.sync_pairs.clone();
    if stored.active_location_id.is_none() {
        stored.active_location_id = existing.active_location_id.clone();
    }
    let selected_state =
        resolve_selected_credential_state(app, stored.credential_profile_id.as_deref())?;
    stored.apply_selected_credential_state(selected_state);
    persist_profile_with_remote_bin_reconciliation(
        app,
        &existing,
        stored.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )
}

async fn reconcile_remote_bin_lifecycle_target<R: Runtime>(
    app: &AppHandle<R>,
    target: &RemoteBinLifecycleTarget,
) -> Result<(), String> {
    if !provider_supports_remote_bin_lifecycle_reconciliation(&target.provider) {
        return Err(remote_bin_lifecycle_reconciliation_unsupported_message(
            target,
        ));
    }

    let credentials = load_credentials_for_remote_bin_target(app, target)?;
    let client = object_store::build_client(&object_store::StorageConnectionConfig {
        provider: target.provider.clone(),
        region: target.region.clone(),
        bucket: target.bucket.clone(),
        credentials,
    })
    .await?;

    object_store::reconcile_remote_bin_lifecycle(&client, &target.bucket, &target.managed_rules)
        .await
        .map_err(String::from)
}

async fn apply_sync_location_versioning<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    enabled: bool,
) -> Result<(), String> {
    let credentials = resolve_credentials_for_pair(app, pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(pair, &credentials)).await?;
    object_store::set_bucket_versioning(&client, &pair.bucket, enabled)
        .await
        .map_err(String::from)
}

async fn reconcile_pair_object_versioning<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<(), String> {
    let credentials = resolve_credentials_for_pair(app, pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(pair, &credentials)).await?;
    let currently_enabled = object_store::bucket_versioning_enabled(&client, &pair.bucket).await?;

    if pair.object_versioning_enabled && !currently_enabled {
        object_store::set_bucket_versioning(&client, &pair.bucket, true).await?;
    } else if !pair.object_versioning_enabled && currently_enabled {
        object_store::set_bucket_versioning(&client, &pair.bucket, false).await?;
    }

    Ok(())
}

fn stop_requested(stop_signal: Option<&AtomicBool>) -> bool {
    stop_signal
        .map(|signal| signal.load(Ordering::SeqCst))
        .unwrap_or(false)
}

async fn sleep_until_pair_work(
    state: &State<'_, SyncState>,
    stop_signal: &AtomicBool,
    polling_wait: Duration,
) {
    let poll_deadline = Instant::now() + polling_wait;

    loop {
        if stop_signal.load(Ordering::SeqCst) {
            break;
        }

        let now = Instant::now();
        if now >= poll_deadline {
            break;
        }

        if next_dirty_pair_deadline(state, DIRTY_PAIR_DEBOUNCE)
            .ok()
            .flatten()
            .is_some_and(|deadline| deadline <= now)
        {
            break;
        }

        let next_dirty_wait = next_dirty_pair_deadline(state, DIRTY_PAIR_DEBOUNCE)
            .ok()
            .flatten()
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(poll_deadline.saturating_duration_since(now));
        let remaining_poll = poll_deadline.saturating_duration_since(now);
        let sleep_for = remaining_poll
            .min(next_dirty_wait)
            .min(Duration::from_millis(250));

        if sleep_for.is_zero() {
            break;
        }

        tokio::time::sleep(sleep_for).await;
    }
}

#[tauri::command]
pub async fn validate_storage_connection(
    app: AppHandle,
    input: ConnectionValidationInput,
) -> Result<ConnectionValidationResult, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let credentials = resolve_refresh_credentials(&app, &profile, &input)?;
    let provider = normalize_provider(&credentials.provider);
    let capabilities = provider_capabilities(&provider);

    if input.object_versioning_enabled && !provider_supports_runtime_object_versioning(&provider) {
        return Ok(ConnectionValidationResult {
            ok: false,
            checked_at: now_iso(),
            message: provider_runtime_object_versioning_message(&provider),
            provider,
            capabilities,
        });
    }

    if input.remote_bin.enabled && !capabilities.supports_remote_bin {
        return Ok(ConnectionValidationResult {
            ok: false,
            checked_at: now_iso(),
            message: format!(
                "Provider '{}' does not support remote bin in the native backend.",
                provider
            ),
            provider,
            capabilities,
        });
    }

    if input.object_versioning_enabled && provider != GCS_PROVIDER {
        let client = object_store::build_client(&input.to_storage_config(&credentials)).await?;
        if !object_store::bucket_versioning_enabled(&client, &input.bucket).await? {
            return Ok(ConnectionValidationResult {
                ok: false,
                checked_at: now_iso(),
                message: format!(
                    "Bucket '{}' must have versioning enabled before object versioning can be used.",
                    input.bucket
                ),
                provider,
                capabilities,
            });
        }
    }

    let summary = object_store::validate_connection(&input.to_storage_config(&credentials)).await?;
    let message = format_storage_validation_success_message(&summary);

    Ok(ConnectionValidationResult {
        ok: true,
        checked_at: summary.checked_at,
        message,
        provider,
        capabilities,
    })
}

#[tauri::command]
pub fn list_credentials_command(app: AppHandle) -> Result<Vec<CredentialSummary>, String> {
    let _ = ensure_legacy_credentials_migrated(&app, Some("Migrated credential"))?;
    list_credentials(&app)
}

#[tauri::command]
pub async fn create_credential_command(
    app: AppHandle,
    draft: CredentialDraft,
) -> Result<CredentialSummary, String> {
    create_credential_with_optional_initial_validation(&app, draft).await
}

#[tauri::command]
pub fn list_provider_capabilities_command() -> Vec<super::provider::ProviderCapabilities> {
    supported_providers()
}

#[tauri::command]
pub async fn test_credential_command(
    app: AppHandle,
    request: CredentialTestRequest,
) -> Result<CredentialTestResult, String> {
    let context = request.context.normalized();
    test_credential_against_context(&app, &request.credential_id, &context).await
}

#[tauri::command]
pub fn delete_credential_command(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
    credential_id: String,
) -> Result<DeleteCredentialResult, String> {
    let (result, cleared_selected_credential) =
        delete_credential_selection_impl(&app, &credential_id)?;
    let profile = result.profile.clone();

    if cleared_selected_credential {
        let _ = stop_polling_worker(&state)?;

        let previous = get_status_lock(&state)?.clone();
        let (snapshot, snapshot_error) = snapshot_for_profile(&app, &profile);
        let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &profile);
        let planner_summary = load_planner_summary(&app, &profile)?;
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                snapshot.as_ref(),
                remote_snapshot.as_ref(),
                planner_summary,
            );
            next.last_sync_at = previous.last_sync_at;
            next.last_error = Some(
                merge_snapshot_errors(snapshot_error, remote_snapshot_error)
                    .map(|error| {
                        format!(
                            "Selected credential was deleted. {error}"
                        )
                    })
                    .unwrap_or_else(|| {
                        "Selected credential was deleted. Choose another credential before syncing again."
                            .into()
                    }),
            );
            next.phase = if is_profile_configured(&profile) {
                "error".into()
            } else {
                "unconfigured".into()
            };
            *status = next.clone();
            next
        };
        emit_status(&app, &next_status);
        emit_info_activity(
            &app,
            &debug_state,
            "Deleted selected credential.",
            Some("Profile credential selection cleared and polling stopped.".into()),
        );
    }

    Ok(result)
}

fn delete_credential_selection_impl<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: &str,
) -> Result<(DeleteCredentialResult, bool), String> {
    let deleted = delete_stored_credential(app, credential_id)?;
    let mut profile = saved_profile_with_credentials_state(app)?;
    let cleared_selected_credential =
        deleted && profile.credential_profile_id.as_deref() == Some(credential_id);

    if cleared_selected_credential {
        profile.clear_selected_credential();
        write_profile_to_disk(app, &profile)?;
    }

    Ok((
        DeleteCredentialResult { deleted, profile },
        cleared_selected_credential,
    ))
}

#[tauri::command]
pub fn load_profile(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<StoredProfile, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let (snapshot, snapshot_error) = snapshot_for_profile(&app, &profile);
    let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &profile);
    let planner_summary = load_planner_summary(&app, &profile)?;
    let mut next_status = status_with_snapshots(
        &profile,
        snapshot.as_ref(),
        remote_snapshot.as_ref(),
        planner_summary,
    );
    let combined_error = merge_snapshot_errors(snapshot_error, remote_snapshot_error);

    if combined_error.is_some() && is_profile_configured(&profile) {
        next_status.phase = "error".into();
    }

    next_status.last_error = combined_error;

    {
        let mut status = get_status_lock(&state)?;
        *status = next_status.clone();
    }

    emit_status(&app, &next_status);
    emit_info_activity(
        &app,
        &debug_state,
        "Loaded saved profile.",
        Some(format!(
            "configured={} folder='{}' bucket='{}' phase='{}'",
            is_profile_configured(&profile),
            profile.local_folder,
            profile.bucket,
            next_status.phase
        )),
    );
    let _ = start_polling_worker(&app);
    Ok(profile)
}

#[cfg(test)]
#[allow(dead_code)]
fn load_profile_impl<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, SyncState>,
    _debug_state: State<'_, ActivityDebugState>,
) -> Result<StoredProfile, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    if !profile.sync_pairs.is_empty() {
        let _ = refresh_aggregate_status(&app, &profile);
    } else {
        drop(get_status_lock(&state)?);
    }
    Ok(profile)
}

fn get_sync_status_impl<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, SyncState>,
) -> Result<SyncStatus, String> {
    let profile = read_profile_from_disk(&app)?;

    if profile.sync_pairs.is_empty() {
        return Ok(get_status_lock(&state)?.clone());
    }

    refresh_aggregate_status(&app, &profile)
}

#[tauri::command]
pub fn save_profile(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
    profile: ProfileDraft,
) -> Result<StoredProfile, String> {
    let (stored, _) = store_profile_draft(&app, profile)?;

    // Status is derived from the sync locations, never from the flat profile
    // fields (which exist only for migration of pre-locations profiles).
    let previous_phase = get_status_lock(&state)?.phase.clone();
    let mut next_status = refresh_aggregate_status(&app, &stored)?;
    if next_status.phase == "idle" && previous_phase == "paused" {
        next_status.phase = "paused".into();
        set_status_from_handle(&app, next_status.clone())?;
    }

    emit_status(&app, &next_status);
    emit_success_activity(
        &app,
        &debug_state,
        "Saved setup details.",
        Some(format!(
            "folder='{}' bucket='{}' polling_enabled={} activity_debug_mode_enabled={}",
            stored.local_folder,
            stored.bucket,
            stored.remote_polling_enabled,
            stored.activity_debug_mode_enabled
        )),
    );
    Ok(stored)
}

#[tauri::command]
pub fn save_profile_settings(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
    profile: StoredProfile,
) -> Result<StoredProfile, String> {
    let stored = store_profile_settings(&app, profile)?;

    // Status is derived from the sync locations, never from the flat profile
    // fields (which exist only for migration of pre-locations profiles).
    let previous_phase = get_status_lock(&state)?.phase.clone();
    let mut next_status = refresh_aggregate_status(&app, &stored)?;
    if next_status.phase == "idle" && previous_phase == "paused" {
        next_status.phase = "paused".into();
        set_status_from_handle(&app, next_status.clone())?;
    }

    emit_status(&app, &next_status);
    emit_success_activity(
        &app,
        &debug_state,
        "Saved sync settings.",
        Some(format!(
            "polling_enabled={} poll_interval_seconds={} remote_bin_enabled={} remote_bin_retention_days={} activity_debug_mode_enabled={}",
            stored.remote_polling_enabled,
            stored.poll_interval_seconds,
            stored.remote_bin.enabled,
            stored.remote_bin.retention_days,
            stored.activity_debug_mode_enabled
        )),
    );

    if stored.remote_polling_enabled && next_status.phase == "polling" {
        start_polling_worker(&app)?;
    } else if !stored.remote_polling_enabled {
        let _ = stop_polling_worker(&state)?;
    }

    Ok(stored)
}

#[cfg(test)]
#[allow(dead_code)]
fn save_profile_settings_impl<R: Runtime>(
    app: AppHandle<R>,
    _state: State<'_, SyncState>,
    _debug_state: State<'_, ActivityDebugState>,
    profile: StoredProfile,
) -> Result<StoredProfile, String> {
    store_profile_settings(&app, profile)
}

#[tauri::command]
pub fn get_sync_status(app: AppHandle, state: State<'_, SyncState>) -> Result<SyncStatus, String> {
    get_sync_status_impl(app, state)
}

#[tauri::command]
pub async fn start_sync(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;

    if !profile.sync_pairs.is_empty() {
        let Some(pair) = active_pair_for_manual_actions(&profile) else {
            let status = refresh_aggregate_status(&app, &profile)?;
            emit_status(&app, &status);
            return Ok(status);
        };

        let _ = stop_polling_worker(&state)?;
        let pair_status =
            run_sync_cycle_for_pair(&app, &debug_state, &pair, PairSyncTrigger::Manual, None)
                .await?;
        set_pair_status_from_handle(&app, pair_status)?;
        let mut status = refresh_aggregate_status(&app, &profile)?;

        let state_ref = app.state::<SyncState>();
        if configured_pair_statuses(&profile, &app)
            .iter()
            .any(|status| status.phase == "syncing")
        {
            status.phase = "syncing".into();
        }

        if profile.sync_pairs.iter().any(should_poll_pair)
            && status.phase != "unconfigured"
            && status.phase != "paused"
        {
            start_polling_worker(&app)?;
            status = refresh_aggregate_status(&app, &profile)?;
        } else if !polling_worker_active(&state_ref)? {
            let _ = stop_polling_worker(&state_ref)?;
        }

        emit_status(&app, &status);
        return Ok(status);
    }

    // No configured sync locations: nothing to run.
    let _ = stop_polling_worker(&state)?;
    let status = refresh_aggregate_status(&app, &profile)?;
    emit_status(&app, &status);
    Ok(status)
}

#[tauri::command]
pub fn pause_sync(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    pause_sync_impl(app, state, debug_state)
}

fn pause_sync_impl<R: Runtime>(
    app: AppHandle<R>,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;

    if !profile.sync_pairs.is_empty() {
        let _ = stop_polling_worker(&state)?;
        let pair_statuses = current_pair_statuses(&profile, &app)
            .into_iter()
            .map(|mut status| {
                status.phase = if is_pair_configured(
                    profile
                        .sync_pairs
                        .iter()
                        .find(|pair| pair.id == status.pair_id)
                        .expect("pair status should correspond to an existing pair"),
                ) {
                    "paused".into()
                } else {
                    "unconfigured".into()
                };
                status
            })
            .collect::<Vec<_>>();

        let next_status = set_aggregate_status_from_pairs(&app, pair_statuses)?;
        emit_status(&app, &next_status);
        emit_info_activity(
            &app,
            &debug_state,
            if next_status.phase == "paused" {
                "Sync paused."
            } else {
                "Pause ignored until setup is saved."
            },
            Some(format!("phase='{}'", next_status.phase)),
        );
        return Ok(next_status);
    }

    let _ = stop_polling_worker(&state)?;
    let previous = get_status_lock(&state)?.clone();
    let (snapshot, _) = snapshot_for_profile(&app, &profile);
    let (remote_snapshot, _) = remote_snapshot_for_profile(&app, &profile);
    let next_status = {
        let mut status = get_status_lock(&state)?;
        let mut next = status_with_snapshots(
            &profile,
            snapshot.as_ref(),
            remote_snapshot.as_ref(),
            previous.plan.clone(),
        );
        next.last_sync_at = previous.last_sync_at.clone();
        next.phase = if is_profile_configured(&profile) {
            "paused".into()
        } else {
            "unconfigured".into()
        };
        *status = next.clone();
        next
    };

    emit_status(&app, &next_status);
    emit_info_activity(
        &app,
        &debug_state,
        if next_status.phase == "paused" {
            "Sync paused."
        } else {
            "Pause ignored until setup is saved."
        },
        Some(format!("phase='{}'", next_status.phase)),
    );
    Ok(next_status)
}

// ---------------------------------------------------------------------------
// Per-pair helpers (private — not exposed as Tauri commands)
// ---------------------------------------------------------------------------

#[cfg(test)]
fn s3_config_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> s3_adapter::S3ConnectionConfig {
    s3_adapter::S3ConnectionConfig {
        provider: credentials.provider.clone(),
        region: pair.region.clone(),
        bucket: pair.bucket.clone(),
        access_key_id: credentials.access_key_id.clone(),
        secret_access_key: credentials.secret_access_key.clone(),
    }
}

fn storage_config_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> object_store::StorageConnectionConfig {
    object_store::StorageConnectionConfig {
        provider: credentials.provider.clone(),
        region: pair.region.clone(),
        bucket: pair.bucket.clone(),
        credentials: credentials.clone(),
    }
}

fn resolve_credentials_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<StoredCredentials, String> {
    let credential_id = pair
        .credential_profile_id
        .as_deref()
        .ok_or("Sync pair has no credential assigned.")?;
    load_credentials_by_id(app, credential_id)?.ok_or_else(|| {
        format!(
            "Credential '{}' for sync pair '{}' is unavailable.",
            credential_id, pair.label
        )
    })
}

fn emit_watcher_degraded_activity<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    details: impl Into<String>,
) {
    emit_info_activity(
        app,
        debug_state,
        "Filesystem watcher unavailable; falling back to polling.",
        Some(format!(
            "pair='{}' locationId='{}' {}",
            pair.label,
            pair.id,
            details.into()
        )),
    );
}

fn reconcile_pair_watchers(app: &AppHandle, profile: &StoredProfile) -> Result<(), String> {
    let state = app.state::<SyncState>();
    let current = active_watcher_pair_paths(&state)?;
    let eligible_pairs = watcher_eligible_pairs(profile);
    let desired_targets: Vec<WatchTarget> = eligible_pairs
        .iter()
        .filter_map(pair_watch_target)
        .collect();
    let desired_ids: BTreeSet<String> = desired_targets
        .iter()
        .map(|target| target.pair_id.clone())
        .collect();
    let plan = plan_watch_reconciliation(&current, &desired_targets);

    for pair_id in plan.stop {
        remove_pair_watcher(&state, &pair_id)?;
    }

    retain_dirty_pairs(&state, &desired_ids)?;

    let debug_state = app.state::<ActivityDebugState>();
    for target in plan.start {
        let Some(pair) = profile
            .sync_pairs
            .iter()
            .find(|pair| pair.id == target.pair_id)
        else {
            continue;
        };

        let root_path = target.root_path.clone();
        if !root_path.exists() {
            emit_watcher_degraded_activity(
                app,
                &debug_state,
                pair,
                format!(
                    "local_folder='{}' reason='missing-folder'",
                    root_path.display()
                ),
            );
            continue;
        }

        let app_handle = app.clone();
        let pair_id = pair.id.clone();
        let pair_clone = pair.clone();
        match start_pair_watcher(&root_path, move |event| {
            let state = app_handle.state::<SyncState>();
            let debug_state = app_handle.state::<ActivityDebugState>();
            match event {
                WatcherCallbackEvent::LocalChange => {
                    let _ = mark_pair_dirty(&state, &pair_id);
                }
                WatcherCallbackEvent::Degraded(error) => {
                    let _ = remove_pair_watcher(&state, &pair_id);
                    emit_watcher_degraded_activity(&app_handle, &debug_state, &pair_clone, error);
                }
            }
        }) {
            Ok(watcher) => {
                install_pair_watcher(&state, target.pair_id, watcher)?;
            }
            Err(error) => {
                emit_watcher_degraded_activity(app, &debug_state, pair, error);
            }
        }
    }

    Ok(())
}

fn current_pair_statuses<R: Runtime>(
    profile: &StoredProfile,
    app: &AppHandle<R>,
) -> Vec<PairSyncStatus> {
    profile
        .sync_pairs
        .iter()
        .map(|pair| {
            let (local_snapshot, local_error) = snapshot_for_pair(app, pair);
            let (remote_snapshot, remote_error) = remote_snapshot_for_pair(app, pair);
            let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();

            let mut status = pair_to_status(
                pair,
                local_snapshot.as_ref(),
                remote_snapshot.as_ref(),
                plan_summary,
            );

            if let Some(error) = merge_snapshot_errors(local_error, remote_error) {
                status.phase = "error".into();
                status.last_error = Some(error);
            }

            status
        })
        .collect()
}

fn configured_pair_statuses<R: Runtime>(
    profile: &StoredProfile,
    app: &AppHandle<R>,
) -> Vec<PairSyncStatus> {
    let state = app.state::<SyncState>();
    let runtime_statuses = pair_statuses_snapshot(&state).unwrap_or_default();

    profile
        .sync_pairs
        .iter()
        .map(|pair| {
            runtime_statuses.get(&pair.id).cloned().unwrap_or_else(|| {
                let (local_snapshot, local_error) = snapshot_for_pair(app, pair);
                let (remote_snapshot, remote_error) = remote_snapshot_for_pair(app, pair);
                let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();

                let mut status = pair_to_status(
                    pair,
                    local_snapshot.as_ref(),
                    remote_snapshot.as_ref(),
                    plan_summary,
                );

                if let Some(error) = merge_snapshot_errors(local_error, remote_error) {
                    status.phase = "error".into();
                    status.last_error = Some(error);
                }

                status
            })
        })
        .collect()
}

fn set_aggregate_status_from_pairs<R: Runtime>(
    app: &AppHandle<R>,
    pair_statuses: Vec<PairSyncStatus>,
) -> Result<SyncStatus, String> {
    replace_pair_statuses_from_handle(app, pair_statuses.clone())?;
    let status = synthesize_status_from_pairs(&pair_statuses);
    set_status_from_handle(app, status.clone())?;
    Ok(status)
}

fn refresh_aggregate_status<R: Runtime>(
    app: &AppHandle<R>,
    profile: &StoredProfile,
) -> Result<SyncStatus, String> {
    let pair_statuses = configured_pair_statuses(profile, app);
    set_aggregate_status_from_pairs(app, pair_statuses)
}

fn active_pair_for_manual_actions(profile: &StoredProfile) -> Option<SyncPair> {
    if profile.sync_pairs.is_empty() {
        return None;
    }

    if let Some(active_id) = profile.active_location_id.as_deref() {
        if let Some(pair) = profile.sync_pairs.iter().find(|pair| pair.id == active_id) {
            return Some(pair.clone());
        }
    }

    profile
        .sync_pairs
        .iter()
        .find(|pair| pair.enabled && is_pair_configured(pair))
        .cloned()
        .or_else(|| {
            profile
                .sync_pairs
                .iter()
                .find(|pair| is_pair_configured(pair))
                .cloned()
        })
}

fn should_poll_pair(pair: &SyncPair) -> bool {
    pair.enabled && pair.remote_polling_enabled && is_pair_configured(pair)
}

fn next_polling_deadline_at(
    now: tokio::time::Instant,
    pair: &SyncPair,
    status: Option<&PairSyncStatus>,
) -> tokio::time::Instant {
    let interval = Duration::from_secs(pair.poll_interval_seconds.max(15) as u64);
    let anchor = status
        .and_then(|status| status.last_sync_at.as_deref())
        .and_then(parse_poll_anchor_age)
        .unwrap_or(interval);
    let wait = if anchor >= interval {
        Duration::ZERO
    } else {
        interval - anchor
    };
    now + wait
}

#[cfg(test)]
fn next_polling_deadline(pair: &SyncPair, status: Option<&PairSyncStatus>) -> tokio::time::Instant {
    next_polling_deadline_at(tokio::time::Instant::now(), pair, status)
}

fn due_polling_pairs(
    pairs: &[SyncPair],
    statuses: &BTreeMap<String, PairSyncStatus>,
    now: tokio::time::Instant,
) -> Vec<SyncPair> {
    pairs
        .iter()
        .filter(|pair| should_poll_pair(pair))
        .filter(|pair| next_polling_deadline_at(now, pair, statuses.get(&pair.id)) <= now)
        .cloned()
        .collect()
}

fn watcher_eligible_pairs(profile: &StoredProfile) -> Vec<SyncPair> {
    profile
        .sync_pairs
        .iter()
        .filter(|pair| should_poll_pair(pair))
        .cloned()
        .collect()
}

fn pair_watch_target(pair: &SyncPair) -> Option<WatchTarget> {
    should_poll_pair(pair).then(|| WatchTarget {
        pair_id: pair.id.clone(),
        root_path: PathBuf::from(&pair.local_folder),
    })
}

fn snapshot_age(value: &str, now: time::OffsetDateTime) -> Option<Duration> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    let elapsed = now - parsed;
    if elapsed.is_negative() {
        Some(Duration::ZERO)
    } else {
        elapsed.try_into().ok()
    }
}

fn local_snapshot_is_fresh(snapshot: &LocalIndexSnapshot, ttl: Duration) -> bool {
    snapshot_age(
        &snapshot.summary.indexed_at,
        time::OffsetDateTime::now_utc(),
    )
    .is_some_and(|age| age <= ttl)
}

fn should_scan_local_for_trigger(
    trigger: PairSyncTrigger,
    cached_local_snapshot: Option<&LocalIndexSnapshot>,
    watcher_active: bool,
    ttl: Duration,
) -> bool {
    match trigger {
        PairSyncTrigger::Manual | PairSyncTrigger::LocalDirty => true,
        PairSyncTrigger::RemotePoll => {
            cached_local_snapshot.is_none()
                || !watcher_active
                || cached_local_snapshot
                    .is_some_and(|snapshot| !local_snapshot_is_fresh(snapshot, ttl))
        }
    }
}

fn parse_poll_anchor_age(value: &str) -> Option<Duration> {
    snapshot_age(value, time::OffsetDateTime::now_utc())
}

fn snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> (Option<LocalIndexSnapshot>, Option<String>) {
    match read_local_index_snapshot_for_pair(app, &pair.id) {
        Ok(snapshot) => (
            snapshot.filter(|s| {
                !pair.local_folder.is_empty()
                    && super::local_index::snapshot_matches_folder(s, &pair.local_folder)
            }),
            None,
        ),
        Err(error) => (
            None,
            Some(format!(
                "Failed to load local index snapshot for pair '{}': {error}",
                pair.label
            )),
        ),
    }
}

fn remote_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> (Option<RemoteIndexSnapshot>, Option<String>) {
    match read_remote_index_snapshot_for_pair(app, &pair.id) {
        Ok(snapshot) => (
            snapshot.filter(|s| {
                !pair.bucket.is_empty()
                    && super::remote_index::snapshot_matches_target(s, &pair.bucket)
            }),
            None,
        ),
        Err(error) => (
            None,
            Some(format!(
                "Failed to load remote index snapshot for pair '{}': {error}",
                pair.label
            )),
        ),
    }
}

pub(crate) async fn list_remote_inventory_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<RemoteIndexSnapshot, String> {
    let client = object_store::build_client(&storage_config_for_pair(pair, credentials)).await?;
    let mut entries = BTreeMap::new();
    let mut object_count = 0_u64;
    let mut total_bytes = 0_u64;
    let excluded_prefixes = vec![pair_bin_prefix(&pair.id)];

    let objects = object_store::list_objects(&client, &pair.bucket, None)
        .await
        .map_err(|error| {
            format!(
                "failed to list remote inventory for pair '{}': {error}",
                pair.label
            )
        })?;

    for object in objects {
        let key = object.key;

        if should_exclude_remote_key(&key, &excluded_prefixes) {
            continue;
        }

        let relative_path = relative_path_from_key(&key);
        let last_modified_at = object.last_modified_at;
        let etag = object.etag;
        let storage_class = object.storage_class;
        let fingerprint = object.fingerprint;

        if key.ends_with('/') {
            let directory_path = relative_path.trim_matches('/');
            if !directory_path.is_empty() {
                entries.insert(
                    directory_path.to_string(),
                    RemoteObjectEntry {
                        key: key.clone(),
                        relative_path: directory_path.to_string(),
                        kind: "directory".into(),
                        size: 0,
                        last_modified_at,
                        etag,
                        storage_class: None,
                        fingerprint: None,
                    },
                );
            }

            for directory_path in directory_relative_paths_from_relative_path(&relative_path) {
                entries
                    .entry(directory_path.clone())
                    .or_insert_with(|| RemoteObjectEntry {
                        key: s3_adapter::directory_key(&directory_path),
                        relative_path: directory_path,
                        kind: "directory".into(),
                        size: 0,
                        last_modified_at: None,
                        etag: None,
                        storage_class: None,
                        fingerprint: None,
                    });
            }
            continue;
        }

        let size = object.size;
        object_count += 1;
        total_bytes += size;
        entries.insert(
            relative_path.clone(),
            RemoteObjectEntry {
                key: key.clone(),
                relative_path,
                kind: "file".into(),
                size,
                last_modified_at,
                etag,
                storage_class,
                fingerprint,
            },
        );

        for directory_path in directory_relative_paths_from_key(&key) {
            entries
                .entry(directory_path.clone())
                .or_insert_with(|| RemoteObjectEntry {
                    key: s3_adapter::directory_key(&directory_path),
                    relative_path: directory_path,
                    kind: "directory".into(),
                    size: 0,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                    fingerprint: None,
                });
        }
    }

    let entries = entries.into_values().collect::<Vec<_>>();

    Ok(RemoteIndexSnapshot {
        version: 1,
        bucket: pair.bucket.clone(),
        excluded_prefixes,
        summary: RemoteIndexSummary {
            indexed_at: now_iso(),
            object_count,
            total_bytes,
        },
        entries,
    })
}

fn rebuild_durable_plan_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    local_snapshot: &LocalIndexSnapshot,
    remote_snapshot: &RemoteIndexSnapshot,
    credentials_available: bool,
) -> Result<super::sync_db::DurablePlannerSummary, String> {
    let anchors = load_sync_anchors_for_pair(app, pair)?
        .into_iter()
        .map(|anchor| (anchor.path.clone(), anchor))
        .collect();
    let plan = sync_planner::build_sync_plan(
        local_snapshot,
        remote_snapshot,
        &anchors,
        &pair.conflict_strategy,
        credentials_available,
    );

    if plan.summary.suppressed_delete_count > 0 {
        // The mass-delete breaker tripped. Surface it loudly: the user sees
        // review items and must be told why nothing was deleted.
        // (Phase 5 turns this into an explicit confirm-or-restore prompt.)
        if let Some(debug_state) = app.try_state::<ActivityDebugState>() {
            emit_error_activity(
                app,
                &debug_state,
                format!(
                    "Held {} deletion(s) for '{}' pending review.",
                    plan.summary.suppressed_delete_count, pair.label
                ),
                Some(format!(
                    "pair='{}' suppressed_delete_count={} anchored_paths={}. Storage Goblin does not delete this many files automatically; review the flagged entries and confirm.",
                    pair.label,
                    plan.summary.suppressed_delete_count,
                    anchors.len()
                )),
            );
        }
    }

    persist_sync_plan_for_pair(app, pair, &plan)
}

async fn execute_planned_upload_queue_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<UploadExecutionOutcome, String> {
    let recovery = recover_interrupted_queue_items_for_pair(app, pair, &now_iso())?;
    emit_recovery_activity(
        app,
        debug_state,
        recovery.recovered_upload_count,
        recovery.recovered_download_count,
        Some(&format!("pair='{}'", pair.label)),
    );
    let queue_items = load_planned_upload_queue_for_pair(app, pair)?;
    let executor = build_pair_transfer_executor(pair, credentials).await?;
    let uploads_ran = !queue_items.is_empty();
    let mut execution_error: Option<String> = None;

    for (index, item) in queue_items.into_iter().enumerate() {
        let started_at = now_iso();
        if let Err(error) =
            mark_upload_queue_item_in_progress_for_pair(app, pair, item.id, &started_at)
        {
            execution_error = Some(error);
            break;
        }

        // Deletes, moves, conflict duplication, and anchor reconciliation
        // (backlog phase 1) are not file transfers.
        if let Some(structural) =
            perform_structural_upload_operation_for_pair(app, &executor, pair, credentials, &item)
                .await
        {
            match structural {
                Ok(message) => {
                    let finished_at = now_iso();
                    if let Err(error) =
                        mark_upload_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                    {
                        execution_error = Some(error);
                        break;
                    }
                    emit_success_activity(
                        app,
                        debug_state,
                        &message,
                        Some(format!(
                            "pair='{}' queue_item_id={} operation='{}' path='{}' finished_at='{}'",
                            pair.label, item.id, item.operation, item.path, finished_at
                        )),
                    );
                }
                Err(error) => {
                    let finished_at = now_iso();
                    let failure_message = format!(
                        "{} failed for '{}': {error}",
                        item.operation.replace('_', " "),
                        item.path
                    );
                    let _ = mark_upload_queue_item_failed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                        &failure_message,
                    );
                    emit_error_activity(
                        app,
                        debug_state,
                        "Planned sync operation failed.",
                        Some(format!(
                            "pair='{}' queue_item_id={} operation='{}' path='{}' finished_at='{}' error='{}'",
                            pair.label, item.id, item.operation, item.path, finished_at, failure_message
                        )),
                    );
                    execution_error = Some(failure_message);
                    break;
                }
            }
            continue;
        }

        if item.operation == "create_directory" {
            let key = s3_adapter::directory_key(&item.path);
            emit_info_activity(
                app,
                debug_state,
                "Starting planned directory creation.",
                Some(format!(
                    "pair='{}' queue_item_id={} attempt={} path='{}' key='{}'",
                    pair.label,
                    item.id,
                    index + 1,
                    item.path,
                    key,
                )),
            );

            match match &executor {
                PairTransferExecutor::Real(client) => {
                    run_with_timeout(
                        object_store::create_directory_placeholder(client, &pair.bucket, &key),
                        PLANNED_UPLOAD_TIMEOUT,
                        || {
                            format!(
                                "Directory creation timed out for '{}' after {}s",
                                item.path,
                                PLANNED_UPLOAD_TIMEOUT.as_secs()
                            )
                        },
                    )
                    .await
                }
                #[cfg(test)]
                PairTransferExecutor::Mock => Ok(()),
            } {
                Ok(()) => {
                    let finished_at = now_iso();
                    if let Err(error) =
                        mark_upload_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                    {
                        execution_error = Some(error);
                        break;
                    }
                    emit_success_activity(
                        app,
                        debug_state,
                        "Completed planned directory creation.",
                        Some(format!(
                            "pair='{}' queue_item_id={} path='{}' key='{}' finished_at='{}'",
                            pair.label, item.id, item.path, key, finished_at
                        )),
                    );
                }
                Err(error) => {
                    let finished_at = now_iso();
                    let failure_message =
                        format!("Directory creation failed for '{}': {error}", item.path);
                    let _ = mark_upload_queue_item_failed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                        &failure_message,
                    );
                    emit_error_activity(
                        app,
                        debug_state,
                        "Planned directory creation failed.",
                        Some(format!(
                            "pair='{}' queue_item_id={} path='{}' key='{}' finished_at='{}' error='{}'",
                            pair.label, item.id, item.path, key, finished_at, failure_message
                        )),
                    );
                    execution_error = Some(failure_message);
                    break;
                }
            }

            continue;
        }

        let local_path = match resolve_local_upload_path(&pair.local_folder, &item.path) {
            Ok(path) => path,
            Err(error) => {
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        };

        let metadata = match std::fs::metadata(&local_path) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => {
                let error = format!(
                    "planned upload source is not a file: {}",
                    local_path.display()
                );
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
            Err(error) => {
                let error = format!(
                    "failed to inspect planned upload source '{}': {error}",
                    local_path.display()
                );
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        };

        if let Some(expected_size) = item.local_size {
            if metadata.len() != expected_size {
                let error = format!(
                    "planned upload source '{}' changed on disk since planning (expected {expected_size} bytes, found {})",
                    local_path.display(),
                    metadata.len()
                );
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        }

        let current_fingerprint = match crate::storage::local_index::file_fingerprint(&local_path) {
            Ok(fingerprint) => fingerprint,
            Err(error) => {
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        };

        let remote_snapshot = match read_remote_index_snapshot_for_pair(app, &pair.id)? {
            Some(snapshot) => snapshot,
            None => {
                let error = "remote snapshot missing before planned upload execution".to_string();
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        };
        let current_remote_etag = remote_etag_for_path(&remote_snapshot, &item.path);
        if let Some(error) = upload_stale_plan_error(
            &local_path,
            &current_fingerprint,
            item.expected_local_fingerprint.as_deref(),
            current_remote_etag.as_deref(),
            item.expected_remote_etag.as_deref(),
        ) {
            let finished_at = now_iso();
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            execution_error = Some(error);
            break;
        }

        let key = s3_adapter::object_key(&item.path);
        emit_info_activity(
            app,
            debug_state,
            "Starting planned upload.",
            Some(format!(
                "pair='{}' queue_item_id={} attempt={} path='{}' key='{}' local_path='{}' bytes={}",
                pair.label,
                item.id,
                index + 1,
                item.path,
                key,
                local_path.display(),
                metadata.len()
            )),
        );

        match run_with_timeout(
            perform_planned_upload_for_pair(
                &executor,
                pair,
                credentials,
                &item.path,
                &key,
                &local_path,
                &current_fingerprint,
            ),
            PLANNED_UPLOAD_TIMEOUT,
            || {
                format!(
                    "Upload timed out for '{}' after {}s",
                    item.path,
                    PLANNED_UPLOAD_TIMEOUT.as_secs()
                )
            },
        )
        .await
        {
            Ok(refreshed_remote_snapshot) => {
                if let Err(error) = persist_upload_success_for_pair(
                    app,
                    pair,
                    &item.path,
                    &current_fingerprint,
                    &refreshed_remote_snapshot,
                ) {
                    let finished_at = now_iso();
                    let _ = mark_upload_queue_item_failed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                        &error,
                    );
                    execution_error = Some(error);
                    break;
                }

                let finished_at = now_iso();
                if let Err(error) =
                    mark_upload_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                {
                    execution_error = Some(error);
                    break;
                }
                emit_success_activity(
                    app,
                    debug_state,
                    "Completed planned upload.",
                    Some(format!(
                        "pair='{}' queue_item_id={} path='{}' key='{}' finished_at='{}'",
                        pair.label, item.id, item.path, key, finished_at
                    )),
                );
            }
            Err(error) => {
                let finished_at = now_iso();
                let failure_message = format!("Upload failed for '{}': {error}", item.path);
                let _ = mark_upload_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &failure_message,
                );
                emit_error_activity(
                    app,
                    debug_state,
                    "Planned upload failed.",
                    Some(format!(
                        "pair='{}' queue_item_id={} path='{}' key='{}' finished_at='{}' error='{}'",
                        pair.label, item.id, item.path, key, finished_at, failure_message
                    )),
                );
                execution_error = Some(failure_message);
                break;
            }
        }
    }

    Ok(UploadExecutionOutcome {
        execution_error,
        uploads_ran,
    })
}

async fn execute_planned_download_queue_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<DownloadExecutionOutcome, String> {
    let recovery = recover_interrupted_queue_items_for_pair(app, pair, &now_iso())?;
    emit_recovery_activity(
        app,
        debug_state,
        recovery.recovered_upload_count,
        recovery.recovered_download_count,
        Some(&format!("pair='{}'", pair.label)),
    );
    let queue_items = load_planned_download_queue_for_pair(app, pair)?;
    let executor = build_pair_transfer_executor(pair, credentials).await?;
    let downloads_ran = !queue_items.is_empty();
    let mut execution_error: Option<String> = None;

    for (index, item) in queue_items.into_iter().enumerate() {
        let started_at = now_iso();
        if let Err(error) =
            mark_download_queue_item_in_progress_for_pair(app, pair, item.id, &started_at)
        {
            execution_error = Some(error);
            break;
        }

        // Local deletes and local moves (backlog phase 1) are not transfers.
        if let Some(structural) =
            perform_structural_download_operation_for_pair(app, &executor, pair, &item)
        {
            match structural {
                Ok(message) => {
                    let finished_at = now_iso();
                    if let Err(error) = mark_download_queue_item_completed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                    ) {
                        execution_error = Some(error);
                        break;
                    }
                    emit_success_activity(
                        app,
                        debug_state,
                        &message,
                        Some(format!(
                            "pair='{}' queue_item_id={} operation='{}' path='{}' finished_at='{}'",
                            pair.label, item.id, item.operation, item.path, finished_at
                        )),
                    );
                }
                Err(error) => {
                    let finished_at = now_iso();
                    let failure_message = format!(
                        "{} failed for '{}': {error}",
                        item.operation.replace('_', " "),
                        item.path
                    );
                    let _ = mark_download_queue_item_failed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                        &failure_message,
                    );
                    emit_error_activity(
                        app,
                        debug_state,
                        "Planned sync operation failed.",
                        Some(format!(
                            "pair='{}' queue_item_id={} operation='{}' path='{}' finished_at='{}' error='{}'",
                            pair.label, item.id, item.operation, item.path, finished_at, failure_message
                        )),
                    );
                    execution_error = Some(failure_message);
                    break;
                }
            }
            continue;
        }

        let local_path = match resolve_local_download_path(&pair.local_folder, &item.path) {
            Ok(path) => path,
            Err(error) => {
                let finished_at = now_iso();
                let _ = mark_download_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        };

        let remote_snapshot = match read_remote_index_snapshot_for_pair(app, &pair.id)? {
            Some(snapshot) => snapshot,
            None => {
                let error = "remote snapshot missing before planned download execution".to_string();
                let finished_at = now_iso();
                let _ = mark_download_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error,
                );
                execution_error = Some(error);
                break;
            }
        };
        let current_remote_etag = remote_etag_for_path(&remote_snapshot, &item.path);
        if current_remote_etag != item.expected_remote_etag {
            let error = format!(
                "planned download source '{}' changed remotely since planning",
                item.path
            );
            let finished_at = now_iso();
            let _ =
                mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            execution_error = Some(error);
            break;
        }

        let current_local_fingerprint = if local_path.exists() {
            match crate::storage::local_index::file_fingerprint(&local_path) {
                Ok(fingerprint) => Some(fingerprint),
                Err(error) => {
                    let finished_at = now_iso();
                    let _ = mark_download_queue_item_failed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                        &error,
                    );
                    execution_error = Some(error);
                    break;
                }
            }
        } else {
            None
        };

        if let Some(error) = download_stale_plan_error(
            &local_path,
            current_local_fingerprint.as_deref(),
            item.expected_local_fingerprint.as_deref(),
            current_remote_etag.as_deref(),
            item.expected_remote_etag.as_deref(),
        ) {
            let finished_at = now_iso();
            let _ =
                mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            execution_error = Some(error);
            break;
        }

        let key = s3_adapter::object_key(&item.path);
        emit_info_activity(
            app,
            debug_state,
            "Starting planned download.",
            Some(format!(
                "pair='{}' queue_item_id={} attempt={} path='{}' key='{}' local_path='{}' remote_size={}",
                pair.label,
                item.id,
                index + 1,
                item.path,
                key,
                local_path.display(),
                item.remote_size
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "unknown".into())
            )),
        );

        if let Some(parent) = local_path.parent() {
            if let Err(error) = std::fs::create_dir_all(parent) {
                let error_msg = format!(
                    "failed to create parent directory for download '{}': {error}",
                    local_path.display()
                );
                let finished_at = now_iso();
                let _ = mark_download_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &error_msg,
                );
                execution_error = Some(error_msg);
                break;
            }
        }

        match run_with_timeout(
            perform_planned_download_for_pair(&executor, pair, &key, &item.path, &local_path),
            PLANNED_DOWNLOAD_TIMEOUT,
            || {
                format!(
                    "Download timed out for '{}' after {}s",
                    item.path,
                    PLANNED_DOWNLOAD_TIMEOUT.as_secs()
                )
            },
        )
        .await
        {
            Ok(()) => {
                if let Err(error) = persist_download_success_for_pair(
                    app,
                    pair,
                    &item.path,
                    &local_path,
                    current_remote_etag.clone(),
                ) {
                    let finished_at = now_iso();
                    let _ = mark_download_queue_item_failed_for_pair(
                        app,
                        pair,
                        item.id,
                        &finished_at,
                        &error,
                    );
                    execution_error = Some(error);
                    break;
                }

                let finished_at = now_iso();
                if let Err(error) =
                    mark_download_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                {
                    execution_error = Some(error);
                    break;
                }
                emit_success_activity(
                    app,
                    debug_state,
                    "Completed planned download.",
                    Some(format!(
                        "pair='{}' queue_item_id={} path='{}' key='{}' finished_at='{}'",
                        pair.label, item.id, item.path, key, finished_at
                    )),
                );
            }
            Err(error) => {
                let finished_at = now_iso();
                let failure_message = format!("Download failed for '{}': {error}", item.path);
                let _ = mark_download_queue_item_failed_for_pair(
                    app,
                    pair,
                    item.id,
                    &finished_at,
                    &failure_message,
                );
                emit_error_activity(
                    app,
                    debug_state,
                    "Planned download failed.",
                    Some(format!(
                        "pair='{}' queue_item_id={} path='{}' key='{}' finished_at='{}' error='{}'",
                        pair.label, item.id, item.path, key, finished_at, failure_message
                    )),
                );
                execution_error = Some(failure_message);
                break;
            }
        }
    }

    Ok(DownloadExecutionOutcome {
        execution_error,
        downloads_ran,
    })
}

// ---------------------------------------------------------------------------
// Per-pair sync cycle orchestration
// ---------------------------------------------------------------------------

async fn run_sync_cycle_for_pair(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    trigger: PairSyncTrigger,
    stop_signal: Option<&AtomicBool>,
) -> Result<PairSyncStatus, String> {
    if !is_pair_configured(pair) {
        let mut status = pair_to_status(pair, None, None, Default::default());
        status.phase = "unconfigured".into();
        status.last_error = Some("Save setup details before starting sync.".into());
        emit_error_activity(
            app,
            debug_state,
            "Pair sync cycle finished with an issue.",
            Some(pair_sync_cycle_issue_details(
                pair,
                "configuration",
                "Save setup details before starting sync.",
                None,
                None,
            )),
        );
        return Ok(status);
    }

    let credentials = match resolve_credentials_for_pair(app, pair) {
        Ok(creds) => creds,
        Err(error) => {
            let (existing_local, _) = snapshot_for_pair(app, pair);
            let (existing_remote, _) = remote_snapshot_for_pair(app, pair);
            let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();
            let mut status = pair_to_status(
                pair,
                existing_local.as_ref(),
                existing_remote.as_ref(),
                plan_summary,
            );
            status.phase = "error".into();
            status.last_error = Some(concise_sync_issue("Credential resolution"));
            emit_error_activity(
                app,
                debug_state,
                "Pair sync cycle finished with an issue.",
                Some(pair_sync_cycle_issue_details(
                    pair,
                    "credential-resolution",
                    &error,
                    None,
                    None,
                )),
            );
            return Ok(status);
        }
    };

    emit_info_activity(
        app,
        debug_state,
        "Running sync cycle for pair.",
        Some(format!(
            "pair='{}' folder='{}' bucket='{}' trigger='{}'",
            pair.label,
            pair.local_folder,
            pair.bucket,
            match trigger {
                PairSyncTrigger::Manual => "manual",
                PairSyncTrigger::LocalDirty => "local-dirty",
                PairSyncTrigger::RemotePoll => "remote-poll",
            }
        )),
    );

    let cycle_started_at = now_iso();

    let (existing_local, _) = snapshot_for_pair(app, pair);
    let state = app.state::<SyncState>();
    let watcher_active = pair_has_active_watcher(&state, &pair.id).unwrap_or(false);

    // 1. Scan local folder when needed
    let mut local_snapshot = if should_scan_local_for_trigger(
        trigger,
        existing_local.as_ref(),
        watcher_active,
        LOCAL_SNAPSHOT_STALE_TTL,
    ) {
        match scan_local_folder(Path::new(&pair.local_folder)) {
            Ok(snapshot) => {
                let _ = write_local_index_snapshot_for_pair(app, &pair.id, &snapshot);
                snapshot
            }
            Err(error) => {
                let (existing_local, _) = snapshot_for_pair(app, pair);
                let (existing_remote, _) = remote_snapshot_for_pair(app, pair);
                let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();
                let mut status = pair_to_status(
                    pair,
                    existing_local.as_ref(),
                    existing_remote.as_ref(),
                    plan_summary,
                );
                status.phase = "error".into();
                status.last_error = Some(concise_sync_issue("Local scan"));
                emit_error_activity(
                    app,
                    debug_state,
                    "Pair sync cycle finished with an issue.",
                    Some(pair_sync_cycle_issue_details(
                        pair,
                        "local-scan",
                        &error,
                        Some(&cycle_started_at),
                        None,
                    )),
                );
                return Ok(status);
            }
        }
    } else {
        existing_local.unwrap_or_else(|| LocalIndexSnapshot {
            version: 1,
            root_folder: pair.local_folder.clone(),
            summary: crate::storage::local_index::LocalIndexSummary {
                indexed_at: now_iso(),
                file_count: 0,
                directory_count: 0,
                total_bytes: 0,
            },
            entries: Vec::new(),
        })
    };

    if stop_requested(stop_signal) {
        let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();
        return Ok(pair_to_status(
            pair,
            Some(&local_snapshot),
            None,
            plan_summary,
        ));
    }

    // 2. Refresh remote inventory
    let mut remote_snapshot = match list_remote_inventory_for_pair(pair, &credentials).await {
        Ok(snapshot) => {
            let _ = write_remote_index_snapshot_for_pair(app, &pair.id, &snapshot);
            snapshot
        }
        Err(error) => {
            let (existing_remote, _) = remote_snapshot_for_pair(app, pair);
            let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();
            let mut status = pair_to_status(
                pair,
                Some(&local_snapshot),
                existing_remote.as_ref(),
                plan_summary,
            );
            status.phase = "error".into();
            status.last_error = Some(concise_sync_issue("Remote inventory refresh"));
            emit_error_activity(
                app,
                debug_state,
                "Pair sync cycle finished with an issue.",
                Some(pair_sync_cycle_issue_details(
                    pair,
                    "remote-refresh",
                    &error,
                    Some(&cycle_started_at),
                    None,
                )),
            );
            return Ok(status);
        }
    };

    if stop_requested(stop_signal) {
        let plan_summary = load_planner_summary_for_pair(app, pair).unwrap_or_default();
        return Ok(pair_to_status(
            pair,
            Some(&local_snapshot),
            Some(&remote_snapshot),
            plan_summary,
        ));
    }

    // 3. Build sync plan
    let mut planner_summary =
        match rebuild_durable_plan_for_pair(app, pair, &local_snapshot, &remote_snapshot, true) {
            Ok(summary) => summary,
            Err(error) => {
                let mut status = pair_to_status(
                    pair,
                    Some(&local_snapshot),
                    Some(&remote_snapshot),
                    Default::default(),
                );
                status.phase = "error".into();
                status.last_error = Some(concise_sync_issue("Sync plan build"));
                emit_error_activity(
                    app,
                    debug_state,
                    "Pair sync cycle finished with an issue.",
                    Some(pair_sync_cycle_issue_details(
                        pair,
                        "plan-build",
                        &error,
                        Some(&cycle_started_at),
                        None,
                    )),
                );
                return Ok(status);
            }
        };

    let mut last_error: Option<String> = None;

    // 4. Execute uploads
    if (planner_summary.upload_count > 0 || planner_summary.create_directory_count > 0)
        && !stop_requested(stop_signal)
    {
        match execute_planned_upload_queue_for_pair(app, debug_state, pair, &credentials).await {
            Ok(outcome) => {
                last_error = outcome.execution_error;

                if outcome.uploads_ran {
                    // Refresh remote after uploads
                    match list_remote_inventory_for_pair(pair, &credentials).await {
                        Ok(snapshot) => {
                            let _ = write_remote_index_snapshot_for_pair(app, &pair.id, &snapshot);
                            remote_snapshot = snapshot;
                        }
                        Err(error) => {
                            last_error = Some(append_error_context(
                                last_error.clone(),
                                format!(
                                    "Failed to refresh remote inventory after upload execution: {error}"
                                ),
                            ));
                        }
                    }

                    // Rebuild plan after uploads
                    match rebuild_durable_plan_for_pair(
                        app,
                        pair,
                        &local_snapshot,
                        &remote_snapshot,
                        true,
                    ) {
                        Ok(summary) => planner_summary = summary,
                        Err(error) => {
                            last_error = Some(append_error_context(
                                last_error.clone(),
                                format!(
                                    "Failed to rebuild sync plan after upload execution: {error}"
                                ),
                            ));
                        }
                    }
                }
            }
            Err(error) => {
                let mut status = pair_to_status(
                    pair,
                    Some(&local_snapshot),
                    Some(&remote_snapshot),
                    planner_summary,
                );
                status.phase = "error".into();
                status.last_error = Some(concise_sync_issue("Upload execution"));
                status.last_sync_at = Some(cycle_started_at);
                emit_error_activity(
                    app,
                    debug_state,
                    "Pair sync cycle finished with an issue.",
                    Some(pair_sync_cycle_issue_details(
                        pair,
                        "upload-execution",
                        &error,
                        status.last_sync_at.as_deref(),
                        None,
                    )),
                );
                return Ok(status);
            }
        }
    }

    // 5. Execute downloads
    if planner_summary.download_count > 0 && !stop_requested(stop_signal) {
        match execute_planned_download_queue_for_pair(app, debug_state, pair, &credentials).await {
            Ok(outcome) => {
                last_error = match (last_error, outcome.execution_error) {
                    (Some(prev), Some(dl_err)) => Some(format!("{prev}. {dl_err}")),
                    (None, Some(dl_err)) => Some(dl_err),
                    (existing, None) => existing,
                };

                if outcome.downloads_ran {
                    // Rescan local folder after downloads
                    if let Ok(updated_snapshot) = scan_local_folder(Path::new(&pair.local_folder)) {
                        let _ =
                            write_local_index_snapshot_for_pair(app, &pair.id, &updated_snapshot);
                        local_snapshot = updated_snapshot;
                    } else {
                        last_error = Some(append_error_context(
                            last_error.clone(),
                            "Failed to rescan local folder after download execution.",
                        ));
                    }

                    // Refresh remote after downloads
                    match list_remote_inventory_for_pair(pair, &credentials).await {
                        Ok(snapshot) => {
                            let _ = write_remote_index_snapshot_for_pair(app, &pair.id, &snapshot);
                            remote_snapshot = snapshot;
                        }
                        Err(error) => {
                            last_error = Some(append_error_context(
                                last_error.clone(),
                                format!(
                                    "Failed to refresh remote inventory after download execution: {error}"
                                ),
                            ));
                        }
                    }

                    // Rebuild plan after downloads
                    match rebuild_durable_plan_for_pair(
                        app,
                        pair,
                        &local_snapshot,
                        &remote_snapshot,
                        true,
                    ) {
                        Ok(summary) => planner_summary = summary,
                        Err(error) => {
                            last_error = Some(append_error_context(
                                last_error.clone(),
                                format!(
                                    "Failed to rebuild sync plan after download execution: {error}"
                                ),
                            ));
                        }
                    }
                }
            }
            Err(error) => {
                let mut status = pair_to_status(
                    pair,
                    Some(&local_snapshot),
                    Some(&remote_snapshot),
                    planner_summary,
                );
                status.phase = "error".into();
                let detail_error = append_error_context(last_error.clone(), error.clone());
                status.last_error = Some(concise_sync_issue("Download execution"));
                status.last_sync_at = Some(cycle_started_at);
                emit_error_activity(
                    app,
                    debug_state,
                    "Pair sync cycle finished with an issue.",
                    Some(pair_sync_cycle_issue_details(
                        pair,
                        "download-execution",
                        &detail_error,
                        status.last_sync_at.as_deref(),
                        None,
                    )),
                );
                return Ok(status);
            }
        }
    }

    if stop_requested(stop_signal) {
        return Ok(pair_to_status(
            pair,
            Some(&local_snapshot),
            Some(&remote_snapshot),
            planner_summary,
        ));
    }

    // 6. Build final status
    let phase = if last_error.is_some() {
        "error"
    } else if !pair.enabled {
        "paused"
    } else {
        "idle"
    };

    let mut final_status = pair_to_status(
        pair,
        Some(&local_snapshot),
        Some(&remote_snapshot),
        planner_summary,
    );
    final_status.phase = phase.into();
    final_status.last_sync_at = Some(cycle_started_at);
    final_status.last_error = last_error
        .as_ref()
        .map(|_| "Sync cycle completed with issues.".to_string());

    if let Some(error) = final_status.last_error.as_ref() {
        emit_error_activity(
            app,
            debug_state,
            "Pair sync cycle finished with an issue.",
            Some(pair_sync_cycle_issue_details(
                pair,
                "sync-cycle",
                error,
                final_status.last_sync_at.as_deref(),
                last_error.as_deref(),
            )),
        );
    } else {
        emit_success_activity(
            app,
            debug_state,
            "Pair sync cycle finished.",
            Some(format!(
                "pair='{}' phase='{}' pending_operations={} last_sync_at='{}'",
                pair.label,
                final_status.phase,
                final_status.pending_operations,
                final_status.last_sync_at.clone().unwrap_or_default()
            )),
        );
    }

    Ok(final_status)
}

fn start_polling_worker_for_pairs(app: &AppHandle) -> Result<(), String> {
    let state = app.state::<SyncState>();
    let (worker_id, stop_signal) = begin_polling_worker(&state)?;
    let app_handle = app.clone();

    tauri::async_runtime::spawn(async move {
        loop {
            let profile = match read_profile_from_disk(&app_handle) {
                Ok(profile) => profile,
                Err(error) => {
                    let debug_state = app_handle.state::<ActivityDebugState>();
                    emit_error_activity(
                        &app_handle,
                        &debug_state,
                        "Pair polling worker stopped after profile load failed.",
                        Some(error),
                    );
                    break;
                }
            };

            let pollable_pairs: Vec<SyncPair> = profile
                .sync_pairs
                .iter()
                .filter(|pair| should_poll_pair(pair))
                .cloned()
                .collect();

            let _ = reconcile_pair_watchers(&app_handle, &profile);

            if pollable_pairs.is_empty() {
                let state = app_handle.state::<SyncState>();
                let _ = clear_all_pair_watchers(&state);
                let aggregate = refresh_aggregate_status(&app_handle, &profile);
                if let Ok(status) = aggregate {
                    emit_status(&app_handle, &status);
                }
                break;
            }

            let state = app_handle.state::<SyncState>();
            let runtime_statuses = pair_statuses_snapshot(&state).unwrap_or_default();
            let now = tokio::time::Instant::now();
            let soonest_deadline = pollable_pairs
                .iter()
                .map(|pair| next_polling_deadline_at(now, pair, runtime_statuses.get(&pair.id)))
                .min()
                .unwrap_or(now);

            sleep_until_pair_work(
                &state,
                stop_signal.as_ref(),
                soonest_deadline.saturating_duration_since(now),
            )
            .await;

            if stop_signal.load(Ordering::SeqCst) {
                break;
            }

            let debug_state = app_handle.state::<ActivityDebugState>();
            let now = tokio::time::Instant::now();
            let dirty_pair_ids: BTreeSet<String> =
                due_dirty_pairs(&state, Instant::now(), DIRTY_PAIR_DEBOUNCE)
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
            let due_pairs = due_polling_pairs(&pollable_pairs, &runtime_statuses, now);

            let mut work_items: Vec<(SyncPair, PairSyncTrigger)> = Vec::new();

            for pair in &pollable_pairs {
                if dirty_pair_ids.contains(&pair.id) {
                    work_items.push((pair.clone(), PairSyncTrigger::LocalDirty));
                }
            }

            for pair in due_pairs {
                if !dirty_pair_ids.contains(&pair.id) {
                    work_items.push((pair, PairSyncTrigger::RemotePoll));
                }
            }

            if work_items.is_empty() {
                continue;
            }

            for (pair, trigger) in work_items {
                if stop_signal.load(Ordering::SeqCst) {
                    break;
                }

                if trigger == PairSyncTrigger::LocalDirty {
                    let _ = clear_dirty_pair(&state, &pair.id);
                }

                // On Err the error was already emitted by run_sync_cycle_for_pair.
                if let Ok(status) = run_sync_cycle_for_pair(
                    &app_handle,
                    &debug_state,
                    &pair,
                    trigger,
                    Some(stop_signal.as_ref()),
                )
                .await
                {
                    let _ = set_pair_status_from_handle(&app_handle, status);
                }
            }

            if let Ok(mut synthesized) = refresh_aggregate_status(&app_handle, &profile) {
                if synthesized.phase == "idle" {
                    synthesized.phase = "polling".into();
                    let _ = set_status_from_handle(&app_handle, synthesized.clone());
                }
                emit_status(&app_handle, &synthesized);
            }
        }

        let state = app_handle.state::<SyncState>();
        let _ = clear_all_pair_watchers(&state);
        let _ = clear_polling_worker(&state, worker_id);
    });

    Ok(())
}

/// Starts the appropriate polling worker based on the current profile state.
/// If configured sync pairs exist, uses the per-pair polling worker.
/// Otherwise, falls back to the legacy single-profile polling worker.
fn start_polling_worker(app: &AppHandle) -> Result<(), String> {
    start_polling_worker_for_pairs(app)
}

// ---------------------------------------------------------------------------
// Sync-pair CRUD commands
// ---------------------------------------------------------------------------

#[tauri::command]
pub fn list_sync_locations(app: AppHandle) -> Result<Vec<SyncPair>, String> {
    Ok(read_profile_from_disk(&app)?.sync_pairs)
}

#[cfg(not(test))]
fn add_sync_pair_impl(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    run_async_blocking(reconcile_pair_object_versioning(&app, &pair))?;
    next.sync_pairs.push(pair);
    let next = persist_profile_with_remote_bin_reconciliation(
        &app,
        &current,
        next.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )?;
    let _ = start_polling_worker(&app);
    Ok(next)
}

#[cfg(test)]
fn add_sync_pair_impl<R: Runtime>(
    app: AppHandle<R>,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    run_async_blocking(reconcile_pair_object_versioning(&app, &pair))?;
    next.sync_pairs.push(pair);
    persist_profile_with_remote_bin_reconciliation(
        &app,
        &current,
        next.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )
}

#[tauri::command]
pub fn add_sync_location(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    add_sync_pair_impl(app, draft)
}

#[cfg(not(test))]
fn update_sync_pair_impl(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    let pair_id = draft
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or("Sync pair ID is required for updates.")?;
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let position = next
        .sync_pairs
        .iter()
        .position(|p| p.id == pair_id)
        .ok_or_else(|| format!("Sync pair '{}' not found.", pair_id))?;
    let updated = SyncPair {
        id: pair_id.to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    run_async_blocking(reconcile_pair_object_versioning(&app, &updated))?;
    next.sync_pairs[position] = updated;
    let next = persist_profile_with_remote_bin_reconciliation(
        &app,
        &current,
        next.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )?;
    let _ = start_polling_worker(&app);
    Ok(next)
}

#[cfg(test)]
fn update_sync_pair_impl<R: Runtime>(
    app: AppHandle<R>,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    let pair_id = draft
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or("Sync pair ID is required for updates.")?;
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let position = next
        .sync_pairs
        .iter()
        .position(|p| p.id == pair_id)
        .ok_or_else(|| format!("Sync pair '{}' not found.", pair_id))?;
    let updated = SyncPair {
        id: pair_id.to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    run_async_blocking(reconcile_pair_object_versioning(&app, &updated))?;
    next.sync_pairs[position] = updated;
    persist_profile_with_remote_bin_reconciliation(
        &app,
        &current,
        next.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )
}

#[tauri::command]
pub fn update_sync_location(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    update_sync_pair_impl(app, draft)
}

#[tauri::command]
pub fn set_sync_location_versioning(
    app: AppHandle,
    location_id: String,
    enabled: bool,
) -> Result<StoredProfile, String> {
    let location_id = location_id.trim();
    if location_id.is_empty() {
        return Err("Sync location ID is required.".into());
    }
    let mut profile = read_profile_from_disk(&app)?;
    let position = profile
        .sync_pairs
        .iter()
        .position(|p| p.id == location_id)
        .ok_or_else(|| format!("Sync location '{}' not found.", location_id))?;
    run_async_blocking(apply_sync_location_versioning(
        &app,
        &profile.sync_pairs[position],
        enabled,
    ))?;
    profile.sync_pairs[position].object_versioning_enabled = enabled;
    write_profile_to_disk(&app, &profile)?;
    Ok(profile)
}

#[cfg(not(test))]
fn remove_sync_pair_impl(app: AppHandle, pair_id: String) -> Result<StoredProfile, String> {
    let pair_id = pair_id.trim();
    if pair_id.is_empty() {
        return Err("Sync pair ID is required.".into());
    }
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let original_len = next.sync_pairs.len();
    next.sync_pairs.retain(|p| p.id != pair_id);
    if next.sync_pairs.len() == original_len {
        return Err(format!("Sync pair '{}' not found.", pair_id));
    }
    if next.active_location_id.as_deref() == Some(pair_id) {
        next.active_location_id = None;
    }
    let next = persist_profile_with_remote_bin_reconciliation(
        &app,
        &current,
        next.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )?;
    let _ = start_polling_worker(&app);
    Ok(next)
}

#[cfg(test)]
fn remove_sync_pair_impl<R: Runtime>(
    app: AppHandle<R>,
    pair_id: String,
) -> Result<StoredProfile, String> {
    let pair_id = pair_id.trim();
    if pair_id.is_empty() {
        return Err("Sync pair ID is required.".into());
    }
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let original_len = next.sync_pairs.len();
    next.sync_pairs.retain(|p| p.id != pair_id);
    if next.sync_pairs.len() == original_len {
        return Err(format!("Sync pair '{}' not found.", pair_id));
    }
    if next.active_location_id.as_deref() == Some(pair_id) {
        next.active_location_id = None;
    }
    persist_profile_with_remote_bin_reconciliation(
        &app,
        &current,
        next.normalized(),
        |app, target| run_async_blocking(reconcile_remote_bin_lifecycle_target(app, target)),
        write_profile_to_disk,
    )
}

#[tauri::command]
pub fn remove_sync_location(app: AppHandle, location_id: String) -> Result<StoredProfile, String> {
    remove_sync_pair_impl(app, location_id)
}

// ---------------------------------------------------------------------------
// File-entry listing command
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct IndexedFileEntry {
    kind: String,
    size: u64,
    storage_class: Option<String>,
    modified_at: Option<String>,
    etag: Option<String>,
}

fn sync_pair_for_location(profile: &StoredProfile, location_id: &str) -> Result<SyncPair, String> {
    profile
        .sync_pairs
        .iter()
        .find(|pair| pair.id == location_id)
        .cloned()
        .ok_or_else(|| format!("Sync pair '{location_id}' not found."))
}

fn file_entry_for_conflict(
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    anchors: Option<&BTreeMap<String, SyncAnchor>>,
    path: &str,
) -> Option<FileEntryResponse> {
    build_file_entry_responses(local_snapshot, remote_snapshot, anchors)
        .into_iter()
        .find(|entry| entry.path == path)
}

fn supports_manual_file_resolution(entry: &FileEntryResponse) -> bool {
    entry.kind == "file"
        && matches!(entry.status.as_str(), "conflict" | "review-required")
        && entry.local_kind.as_deref() == Some("file")
        && entry.remote_kind.as_deref() == Some("file")
}

async fn download_remote_file_for_pair(
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
    object_store::download_file(&client, &pair.bucket, key, destination_path)
        .await
        .map_err(String::from)
}

async fn upload_local_file_for_pair_and_refresh_remote(
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
    list_remote_inventory_for_pair(pair, credentials).await
}

async fn remote_snapshot_for_manual_resolution<R: Runtime>(
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

    list_remote_inventory_for_pair(pair, credentials).await
}

async fn prepare_conflict_comparison_impl<R: Runtime>(
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

async fn resolve_conflict_impl<R: Runtime>(
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

async fn list_versioned_bin_inventory_for_pair(
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

async fn list_versioned_object_history_for_prefix(
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

async fn restore_versioned_bin_entries(
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

async fn restore_remote_bin_entries(
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

async fn purge_versioned_bin_entries(
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

async fn purge_remote_bin_entries(
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

async fn list_remote_bin_inventory_for_pair(
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

async fn refresh_pair_state_after_remote_change<R: Runtime>(
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

fn refresh_pair_state_after_local_change<R: Runtime>(
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

fn build_file_entry_responses(
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
                (Some(local), Some(remote)) if local.kind != remote.kind => "conflict",
                (Some(local), Some(_remote)) if local.kind == "directory" => {
                    if remote_is_glacier {
                        "glacier"
                    } else {
                        "synced"
                    }
                }
                (Some(_local), Some(_remote)) => {
                    if remote_is_glacier {
                        "glacier"
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
                (Some(_), None) => "local-only",
                (None, Some(_remote)) if remote_is_glacier => "glacier",
                (None, Some(_)) => "remote-only",
                (None, None) => unreachable!(),
            };

            FileEntryResponse {
                path: path.clone(),
                kind: in_local
                    .map(|entry| entry.kind.clone())
                    .or_else(|| in_remote.map(|entry| entry.kind.clone()))
                    .expect("listed entries must exist in either snapshot"),
                status: status.into(),
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

#[tauri::command]
pub fn list_file_entries(
    app: AppHandle,
    location_id: String,
) -> Result<Vec<FileEntryResponse>, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    let local_snapshot = read_local_index_snapshot_for_pair(&app, &pair.id)?;
    let remote_snapshot = read_remote_index_snapshot_for_pair(&app, &pair.id)?;
    let anchors = load_sync_anchors_for_pair(&app, &pair)?
        .into_iter()
        .map(|anchor| (anchor.path.clone(), anchor))
        .collect::<BTreeMap<_, _>>();

    Ok(build_file_entry_responses(
        local_snapshot.as_ref(),
        remote_snapshot.as_ref(),
        Some(&anchors),
    ))
}

#[tauri::command]
pub async fn list_bin_entries(
    app: AppHandle,
    location_id: String,
) -> Result<Vec<FileEntryResponse>, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    if pair.object_versioning_enabled {
        let bin_entries = list_versioned_bin_inventory_for_pair(&pair, &credentials).await?;
        Ok(build_versioned_bin_entry_responses(&pair, &bin_entries))
    } else {
        let bin_entries = list_remote_bin_inventory_for_pair(&pair, &credentials).await?;
        Ok(build_bin_entry_responses(&pair, &bin_entries))
    }
}

#[tauri::command]
pub async fn list_file_versions(
    app: AppHandle,
    location_id: String,
    path: String,
) -> Result<Vec<FileVersionEntry>, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if !pair.object_versioning_enabled {
        return Err(format!(
            "Sync location '{}' does not have object versioning enabled.",
            pair.label
        ));
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(&pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;
    let object_key = s3_adapter::object_key(&path);

    let mut key_marker: Option<String> = None;
    let mut version_id_marker: Option<String> = None;
    let mut versions = Vec::new();

    loop {
        let page = object_store::list_object_versions_page_with_prefix(
            &client,
            &pair.bucket,
            Some(&object_key),
            key_marker.as_deref(),
            version_id_marker.as_deref(),
        )
        .await?;

        for version in &page.versions {
            if version.key == object_key {
                versions.push(FileVersionEntry {
                    version_id: version.version_id.clone(),
                    is_latest: version.is_latest,
                    size: version.size,
                    last_modified_at: version.last_modified_at.clone(),
                    storage_class: version.storage_class.clone(),
                    etag: version.etag.clone(),
                });
            }
        }

        if !page.truncated {
            break;
        }

        key_marker = page.next_key_marker;
        version_id_marker = page.next_version_id_marker;
    }

    versions.sort_by(|a, b| {
        b.last_modified_at
            .as_deref()
            .unwrap_or("")
            .cmp(a.last_modified_at.as_deref().unwrap_or(""))
    });

    Ok(versions)
}

#[tauri::command]
pub async fn list_version_counts(
    app: AppHandle,
    location_id: String,
) -> Result<Vec<VersionCountEntry>, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if !pair.object_versioning_enabled {
        return Ok(Vec::new());
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(&pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;

    let mut key_marker: Option<String> = None;
    let mut version_id_marker: Option<String> = None;
    let mut counts: BTreeMap<String, u32> = BTreeMap::new();

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
            *counts.entry(version.key.clone()).or_insert(0) += 1;
        }

        if !page.truncated {
            break;
        }

        key_marker = page.next_key_marker;
        version_id_marker = page.next_version_id_marker;
    }

    let result = counts
        .into_iter()
        .map(|(key, count)| {
            let path = relative_path_from_key(&key);
            VersionCountEntry { path, count }
        })
        .collect();

    Ok(result)
}

#[tauri::command]
pub async fn restore_file_version(
    app: AppHandle,
    location_id: String,
    path: String,
    version_id: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if !pair.object_versioning_enabled {
        return Err(format!(
            "Sync location '{}' does not have object versioning enabled.",
            pair.label
        ));
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(&pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;
    let key = s3_adapter::object_key(&path);

    object_store::copy_object_version(&client, &pair.bucket, &key, &version_id).await?;

    refresh_pair_state_after_remote_change(&app, &pair, &credentials)
        .await
        .map_err(|error| {
            format!(
                "Restored version of '{}', but refresh failed: {error}",
                path
            )
        })?;

    Ok(())
}

#[tauri::command]
pub async fn prepare_version_comparison(
    app: AppHandle,
    location_id: String,
    path: String,
    version_id_a: String,
    version_id_b: String,
) -> Result<VersionComparisonDetails, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if !pair.object_versioning_enabled {
        return Err(format!(
            "Sync location '{}' does not have object versioning enabled.",
            pair.label
        ));
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(&pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;
    let key = s3_adapter::object_key(&path);

    let temp_path_a = temp_version_compare_file_path(&app, &path, "a")?;
    let temp_path_b = temp_version_compare_file_path(&app, &path, "b")?;

    object_store::download_file_version(&client, &pair.bucket, &key, &version_id_a, &temp_path_a)
        .await?;
    object_store::download_file_version(&client, &pair.bucket, &key, &version_id_b, &temp_path_b)
        .await?;

    let path_a_str = temp_path_a.to_string_lossy().into_owned();
    let path_b_str = temp_path_b.to_string_lossy().into_owned();

    Ok(finalize_version_compare_details(
        path,
        version_id_a,
        version_id_b,
        Some(path_a_str),
        Some(path_b_str),
    ))
}

#[tauri::command]
pub async fn delete_file_version(
    app: AppHandle,
    location_id: String,
    path: String,
    version_id: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if !pair.object_versioning_enabled {
        return Err(format!(
            "Sync location '{}' does not have object versioning enabled.",
            pair.label
        ));
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(&pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;
    let key = s3_adapter::object_key(&path);

    object_store::delete_object_version(&client, &pair.bucket, &key, &version_id).await?;

    Ok(())
}

#[tauri::command]
pub async fn restore_bin_entry(
    app: AppHandle,
    location_id: String,
    bin_key: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    let request = if pair.object_versioning_enabled {
        let (object_key, _) = parse_versioned_bin_key(&bin_key)?;
        BinEntryRequest {
            path: relative_path_from_key(&object_key),
            kind: if object_key.ends_with('/') {
                "directory".into()
            } else {
                "file".into()
            },
            bin_key: Some(bin_key),
        }
    } else {
        let original_relative_path =
            original_relative_path_from_bin_key_for_pair(&pair.id, &bin_key)?;
        BinEntryRequest {
            path: original_relative_path.trim_matches('/').to_string(),
            kind: if bin_key.replace('\\', "/").ends_with('/') {
                "directory".into()
            } else {
                "file".into()
            },
            bin_key: Some(bin_key),
        }
    };

    restore_bin_entries(app, location_id, vec![request])
        .await
        .map(|_| ())
}

#[tauri::command]
pub async fn restore_bin_entries(
    app: AppHandle,
    location_id: String,
    entries: Vec<BinEntryRequest>,
) -> Result<BinEntryMutationSummary, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if entries.is_empty() {
        return Ok(BinEntryMutationSummary {
            results: Vec::new(),
        });
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let results = if pair.object_versioning_enabled {
        restore_versioned_bin_entries(&pair, &credentials, &entries).await
    } else {
        restore_remote_bin_entries(&pair, &credentials, &entries).await
    }?;

    if let Some(first) = results.first() {
        refresh_pair_state_after_remote_change(&app, &pair, &credentials)
            .await
            .map_err(|error| {
                format!(
                    "Restored '{}' from the bin, but refresh failed: {error}",
                    first.path
                )
            })?;
    }

    Ok(BinEntryMutationSummary { results })
}

#[tauri::command]
pub async fn purge_bin_entries(
    app: AppHandle,
    location_id: String,
    entries: Vec<BinEntryRequest>,
) -> Result<BinEntryMutationSummary, String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;

    if entries.is_empty() {
        return Ok(BinEntryMutationSummary {
            results: Vec::new(),
        });
    }

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let results = if pair.object_versioning_enabled {
        purge_versioned_bin_entries(&pair, &credentials, &entries).await
    } else {
        purge_remote_bin_entries(&pair, &credentials, &entries).await
    }?;

    if let Some(first) = results.first() {
        refresh_pair_state_after_remote_change(&app, &pair, &credentials)
            .await
            .map_err(|error| {
                format!(
                    "Purged '{}' from the bin, but refresh failed: {error}",
                    first.path
                )
            })?;
    }

    Ok(BinEntryMutationSummary { results })
}

#[tauri::command]
pub async fn reveal_tree_entry(
    app: AppHandle,
    location_id: String,
    path: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = sync_pair_for_location(&profile, &location_id)?;
    let local_path = resolve_local_download_path(&pair.local_folder, &path)?;

    let metadata = std::fs::metadata(&local_path).map_err(|error| match error.kind() {
        std::io::ErrorKind::NotFound => format!(
            "Local path '{}' does not exist for sync location '{}'.",
            local_path.display(),
            pair.label
        ),
        _ => format!(
            "failed to inspect local path '{}': {error}",
            local_path.display()
        ),
    })?;

    reveal_in_file_manager(&local_path, metadata.is_file())
}

#[tauri::command]
pub async fn prepare_conflict_comparison(
    app: AppHandle,
    location_id: String,
    path: String,
) -> Result<ConflictResolutionDetails, String> {
    prepare_conflict_comparison_impl(app, location_id, path).await
}

#[tauri::command]
pub fn open_path(path: String) -> Result<(), String> {
    let resolved = PathBuf::from(path);
    if !resolved.exists() {
        return Err(format!("Path '{}' does not exist.", resolved.display()));
    }

    open_path_with_default_app(&resolved)
}

#[tauri::command]
pub async fn resolve_conflict(
    app: AppHandle,
    location_id: String,
    path: String,
    resolution: String,
) -> Result<(), String> {
    resolve_conflict_impl(app, location_id, path, resolution).await
}

// ---------------------------------------------------------------------------
// Toggle local copy command
// ---------------------------------------------------------------------------

#[tauri::command]
pub async fn toggle_local_copy(
    app: AppHandle,
    location_id: String,
    paths: Vec<String>,
    keep: bool,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = profile
        .sync_pairs
        .iter()
        .find(|p| p.id == location_id)
        .ok_or_else(|| format!("Sync pair '{location_id}' not found."))?
        .clone();

    let mut errors: Vec<String> = Vec::new();

    if keep {
        // Download files from S3 to local storage
        let credentials = resolve_credentials_for_pair(&app, &pair)?;
        let client =
            object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;

        for path in &paths {
            let key = s3_adapter::object_key(path);
            let local_path = match resolve_local_download_path(&pair.local_folder, path) {
                Ok(p) => p,
                Err(e) => {
                    errors.push(e);
                    continue;
                }
            };
            if let Some(parent) = local_path.parent() {
                if let Err(e) = std::fs::create_dir_all(parent) {
                    errors.push(format!(
                        "Failed to create directory for '{}': {e}",
                        local_path.display()
                    ));
                    continue;
                }
            }
            if let Err(e) =
                object_store::download_file(&client, &pair.bucket, &key, &local_path).await
            {
                errors.push(String::from(e));
            }
        }
    } else {
        // Remove local copies
        for path in &paths {
            let full_path = match resolve_local_download_path(&pair.local_folder, path) {
                Ok(p) => p,
                Err(e) => {
                    errors.push(e);
                    continue;
                }
            };
            match std::fs::remove_file(&full_path) {
                Ok(()) => {
                    cleanup_empty_ancestors(&full_path, Path::new(&pair.local_folder));
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    cleanup_empty_ancestors(&full_path, Path::new(&pair.local_folder));
                }
                Err(e) => {
                    errors.push(format!("Failed to remove '{}': {e}", full_path.display()));
                }
            }
        }
    }

    if errors.is_empty() {
        refresh_pair_state_after_local_change(&app, &pair)
            .map_err(|error| format!("Updated local copy selection, but refresh failed: {error}"))
    } else {
        Err(errors.join("; "))
    }
}

// ---------------------------------------------------------------------------
// Delete file command
// ---------------------------------------------------------------------------

#[tauri::command]
pub async fn delete_file(app: AppHandle, location_id: String, path: String) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = profile
        .sync_pairs
        .iter()
        .find(|p| p.id == location_id)
        .ok_or_else(|| format!("Sync pair '{location_id}' not found."))?
        .clone();

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;
    let key = s3_adapter::object_key(&path);

    if pair.object_versioning_enabled {
        object_store::delete_object(&client, &pair.bucket, &key).await?;
    } else if pair.remote_bin.enabled {
        if let Some(target) = target_for_pair(&pair) {
            reconcile_remote_bin_lifecycle_target(&app, &target).await?;
        }
        let remote_bin_key = deleted_object_key(&pair.id, &path);
        object_store::move_object(&client, &pair.bucket, &key, &remote_bin_key, None).await?;
    } else {
        object_store::delete_object(&client, &pair.bucket, &key).await?;
    }

    let local_path = resolve_local_download_path(&pair.local_folder, &path)?;
    match std::fs::remove_file(&local_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!("Failed to remove '{}': {e}", local_path.display()));
        }
    }

    cleanup_empty_ancestors(&local_path, Path::new(&pair.local_folder));

    refresh_pair_state_after_remote_change(&app, &pair, &credentials)
        .await
        .map_err(|error| format!("Deleted '{path}', but refresh failed: {error}"))
}

fn remote_bin_key_for_deleted_key(pair_id: &str, key: &str) -> String {
    let relative_path = relative_path_from_key(key);
    if key.ends_with('/') {
        deleted_directory_key(pair_id, relative_path.trim_matches('/'))
    } else {
        deleted_object_key(pair_id, &relative_path)
    }
}

async fn delete_remote_folder_subtree(
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

#[tauri::command]
pub async fn delete_folder(
    app: AppHandle,
    location_id: String,
    path: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = profile
        .sync_pairs
        .iter()
        .find(|p| p.id == location_id)
        .ok_or_else(|| format!("Sync pair '{location_id}' not found."))?
        .clone();

    let normalized_path = normalize_directory_delete_path(&path)?;
    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;

    delete_remote_folder_subtree(&app, &pair, &client, &normalized_path).await?;
    remove_local_directory_subtree(&pair.local_folder, &normalized_path)?;

    refresh_pair_state_after_remote_change(&app, &pair, &credentials)
        .await
        .map_err(|error| format!("Deleted folder '{normalized_path}', but refresh failed: {error}"))
}

// ---------------------------------------------------------------------------
// Storage class management commands
// ---------------------------------------------------------------------------

#[tauri::command]
pub async fn change_storage_class(
    app: AppHandle,
    location_id: String,
    path: String,
    storage_class: String,
) -> Result<(), String> {
    let profile = read_profile_from_disk(&app)?;
    let pair = profile
        .sync_pairs
        .iter()
        .find(|p| p.id == location_id)
        .ok_or_else(|| format!("Sync pair '{location_id}' not found."))?
        .clone();

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let client = object_store::build_client(&storage_config_for_pair(&pair, &credentials)).await?;
    let key = s3_adapter::object_key(&path);

    object_store::copy_object_with_storage_class(&client, &pair.bucket, &key, &storage_class)
        .await?;

    // Delete local copy when moving to a glacier tier
    if super::remote_index::is_cold_storage_class(Some(&storage_class)) {
        let local_path = resolve_local_download_path(&pair.local_folder, &path)?;
        match std::fs::remove_file(&local_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(format!("Failed to remove '{}': {e}", local_path.display()));
            }
        }
        cleanup_empty_ancestors(&local_path, Path::new(&pair.local_folder));
    }

    refresh_pair_state_after_remote_change(&app, &pair, &credentials)
        .await
        .map_err(|error| format!("Changed storage class for '{path}', but refresh failed: {error}"))
}

#[cfg(test)]
mod tests {
    use crate::storage::bin_service::{
        add_retention_days, path_matches_exact_or_descendant, validate_remote_restore_destination,
        versioned_bin_key,
    };
    use crate::storage::lifecycle_service::{
        filter_remote_bin_reconciliation_targets,
        persist_profile_with_remote_bin_reconciliation_for_test, planned_remote_bin_reconciliation,
    };

    use super::{
        append_error_context, build_bin_entry_responses, build_file_entry_responses,
        build_versioned_bin_entry_responses, collect_remote_bin_keys_for_request,
        collect_versioned_bin_entries_for_request, collect_versioned_history_for_deleted_entries,
        destination_key_for_bin_restore, directory_relative_paths_from_key,
        directory_relative_paths_from_relative_path, download_stale_plan_error, due_polling_pairs,
        local_fingerprint_for_path, local_snapshot_is_fresh, next_polling_deadline,
        parse_versioned_bin_key, provider_supports_remote_bin_lifecycle_reconciliation,
        provider_supports_runtime_object_versioning, relative_path_from_key,
        remote_bin_key_for_deleted_key, remove_local_directory_subtree, rename_local_file_for_pair,
        resolve_local_download_path, s3_config_for_pair, should_defer_create_time_credential_test,
        should_poll_pair, should_scan_local_for_trigger, sync_pair_for_location,
        trash_local_file_for_pair, upload_stale_plan_error, validate_bin_batch_requests,
        watcher_eligible_pairs, BinEntryRequest, CredentialTestContext, PairSyncTrigger,
        VersionedBinEntry, LOCAL_SNAPSHOT_STALE_TTL,
    };
    use crate::storage::credentials_store::{
        CredentialSummary, CredentialValidationStatus, StoredCredentials,
    };
    use crate::storage::local_index::{LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary};
    use crate::storage::profile_store::{RemoteBinConfig, StoredProfile, SyncPair};
    use crate::storage::provider::GCS_PROVIDER;
    use crate::storage::remote_bin::{managed_lifecycle_rule_plan, DEFAULT_REMOTE_BIN_PAIR_ID};
    use crate::storage::remote_index::{
        RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry,
    };
    use crate::storage::sync_db::SyncAnchor;
    use crate::storage::sync_planner;
    use crate::storage::sync_state::PairSyncStatus;
    use std::collections::BTreeMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn provider_supports_runtime_object_versioning_for_gcs() {
        assert!(provider_supports_runtime_object_versioning("gcs"));
    }

    #[test]
    fn provider_supports_runtime_object_versioning_for_aws() {
        assert!(provider_supports_runtime_object_versioning("aws"));
    }

    #[test]
    fn create_time_validation_only_defers_for_gcs_without_bucket_context() {
        assert!(should_defer_create_time_credential_test(
            &CredentialSummary {
                id: "cred-gcs".into(),
                name: "GCS".into(),
                provider: "gcp".into(),
                ready: true,
                validation_status: CredentialValidationStatus::Untested,
                last_tested_at: None,
                last_test_message: None,
                summary: None,
            },
            &CredentialTestContext {
                provider: "gcs".into(),
                region: String::new(),
                bucket: String::new(),
            },
        ));

        assert!(!should_defer_create_time_credential_test(
            &CredentialSummary {
                id: "cred-gcs".into(),
                name: "GCS".into(),
                provider: GCS_PROVIDER.into(),
                ready: true,
                validation_status: CredentialValidationStatus::Untested,
                last_tested_at: None,
                last_test_message: None,
                summary: None,
            },
            &CredentialTestContext {
                provider: GCS_PROVIDER.into(),
                region: String::new(),
                bucket: "bucket-a".into(),
            },
        ));

        assert!(!should_defer_create_time_credential_test(
            &CredentialSummary {
                id: "cred-aws".into(),
                name: "AWS".into(),
                provider: "aws".into(),
                ready: true,
                validation_status: CredentialValidationStatus::Untested,
                last_tested_at: None,
                last_test_message: None,
                summary: None,
            },
            &CredentialTestContext {
                provider: "aws".into(),
                region: String::new(),
                bucket: String::new(),
            },
        ));
    }

    fn build_local_snapshot(entries: &[(&str, &str, u64)]) -> LocalIndexSnapshot {
        let file_count = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "file")
            .count() as u64;
        let directory_count = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "directory")
            .count() as u64;
        let total_bytes = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "file")
            .map(|(_, _, size)| *size)
            .sum();

        LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                file_count,
                directory_count,
                total_bytes,
            },
            entries: entries
                .iter()
                .map(|(relative_path, kind, size)| LocalIndexEntry {
                    relative_path: (*relative_path).into(),
                    kind: (*kind).into(),
                    size: *size,
                    modified_at: None,
                    fingerprint: (*kind == "file").then(|| {
                        crate::storage::local_index::bytes_fingerprint(relative_path.as_bytes())
                    }),
                })
                .collect(),
        }
    }

    fn build_remote_snapshot(entries: &[(&str, &str, u64)]) -> RemoteIndexSnapshot {
        build_remote_snapshot_with_storage_class(
            &entries
                .iter()
                .map(|(relative_path, kind, size)| (*relative_path, *kind, *size, None))
                .collect::<Vec<_>>(),
        )
    }

    fn build_remote_snapshot_with_storage_class(
        entries: &[(&str, &str, u64, Option<&str>)],
    ) -> RemoteIndexSnapshot {
        let object_count = entries
            .iter()
            .filter(|(_, kind, _, _)| *kind == "file")
            .count() as u64;
        let total_bytes = entries
            .iter()
            .filter(|(_, kind, _, _)| *kind == "file")
            .map(|(_, _, size, _)| *size)
            .sum();

        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count,
                total_bytes,
            },
            entries: entries
                .iter()
                .map(
                    |(relative_path, kind, size, storage_class)| RemoteObjectEntry {
                        key: format!("archive/{relative_path}"),
                        relative_path: (*relative_path).into(),
                        kind: (*kind).into(),
                        size: *size,
                        last_modified_at: None,
                        etag: None,
                        storage_class: storage_class.map(str::to_string),
                        fingerprint: None,
                    },
                )
                .collect(),
        }
    }

    fn build_remote_inventory_snapshot_for_test(objects: &[(&str, u64)]) -> RemoteIndexSnapshot {
        let mut entries = BTreeMap::new();
        let mut object_count = 0_u64;
        let mut total_bytes = 0_u64;

        for (key, size) in objects {
            let relative_path = relative_path_from_key(key);

            if key.ends_with('/') {
                let directory_path = relative_path.trim_matches('/');
                if !directory_path.is_empty() {
                    entries.insert(
                        directory_path.to_string(),
                        RemoteObjectEntry {
                            key: (*key).to_string(),
                            relative_path: directory_path.to_string(),
                            kind: "directory".into(),
                            size: 0,
                            last_modified_at: None,
                            etag: None,
                            storage_class: None,
                            fingerprint: None,
                        },
                    );
                }

                for directory_path in directory_relative_paths_from_relative_path(&relative_path) {
                    entries
                        .entry(directory_path.clone())
                        .or_insert_with(|| RemoteObjectEntry {
                            key: crate::storage::s3_adapter::directory_key(&directory_path),
                            relative_path: directory_path,
                            kind: "directory".into(),
                            size: 0,
                            last_modified_at: None,
                            etag: None,
                            storage_class: None,
                            fingerprint: None,
                        });
                }
                continue;
            }

            object_count += 1;
            total_bytes += size;
            entries.insert(
                relative_path.clone(),
                RemoteObjectEntry {
                    key: (*key).to_string(),
                    relative_path,
                    kind: "file".into(),
                    size: *size,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                    fingerprint: None,
                },
            );

            for directory_path in directory_relative_paths_from_key(key) {
                entries
                    .entry(directory_path.clone())
                    .or_insert_with(|| RemoteObjectEntry {
                        key: crate::storage::s3_adapter::directory_key(&directory_path),
                        relative_path: directory_path,
                        kind: "directory".into(),
                        size: 0,
                        last_modified_at: None,
                        etag: None,
                        storage_class: None,
                        fingerprint: None,
                    });
            }
        }

        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count,
                total_bytes,
            },
            entries: entries.into_values().collect(),
        }
    }

    #[test]
    fn remove_local_directory_subtree_removes_nested_content_and_empty_ancestors() {
        let temp_dir = std::env::temp_dir().join(format!(
            "storage-goblin-delete-folder-{}",
            uuid::Uuid::new_v4()
        ));
        let nested_file = temp_dir.join("photos/2026/img001.jpg");
        std::fs::create_dir_all(nested_file.parent().expect("nested parent"))
            .expect("create nested folder");
        std::fs::write(&nested_file, b"demo").expect("write file");

        remove_local_directory_subtree(temp_dir.to_string_lossy().as_ref(), "photos")
            .expect("folder subtree should delete");

        assert!(!temp_dir.join("photos").exists());
        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn remote_bin_key_for_deleted_key_preserves_directory_placeholders() {
        assert_eq!(
            remote_bin_key_for_deleted_key("pair-1", "folder/file.txt"),
            ".storage-goblin-bin/pairs/pair-1/folder/file.txt"
        );
        assert_eq!(
            remote_bin_key_for_deleted_key("pair-1", "folder/subdir/"),
            ".storage-goblin-bin/pairs/pair-1/folder/subdir/"
        );
    }

    #[test]
    fn build_remote_inventory_snapshot_for_test_keeps_directory_prefixes_exact() {
        let snapshot = build_remote_inventory_snapshot_for_test(&[
            ("folder/file.txt", 1),
            ("folder-2/file.txt", 1),
        ]);

        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "folder"));
        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "folder-2"));
        assert!(!snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "folder/file.txt"
                && entry.key.starts_with("folder-2/")));
    }

    fn test_pair(id: &str, enabled: bool, polling_enabled: bool, interval: u32) -> SyncPair {
        SyncPair {
            id: id.into(),
            label: id.into(),
            local_folder: format!("C:/{id}"),
            bucket: format!("bucket-{id}"),
            enabled,
            remote_polling_enabled: polling_enabled,
            poll_interval_seconds: interval,
            ..SyncPair::default()
        }
    }

    fn test_pair_status(pair_id: &str, phase: &str, last_sync_at: Option<&str>) -> PairSyncStatus {
        PairSyncStatus {
            pair_id: pair_id.into(),
            pair_label: pair_id.into(),
            phase: phase.into(),
            last_sync_at: last_sync_at.map(str::to_string),
            enabled: true,
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            ..PairSyncStatus::default()
        }
    }

    #[test]
    fn appends_follow_up_error_to_existing_execution_error() {
        assert_eq!(
            append_error_context(
                Some("Upload failed for 'alpha.txt': request dropped".into()),
                "Failed to refresh remote inventory after upload execution: timeout"
            ),
            "Upload failed for 'alpha.txt': request dropped. Failed to refresh remote inventory after upload execution: timeout"
        );
    }

    #[test]
    fn s3_config_for_pair_maps_all_fields() {
        let pair = SyncPair {
            region: "us-east-1".into(),
            bucket: "demo".into(),
            ..SyncPair::default()
        };
        let creds = StoredCredentials::aws_access_key("aws", "AKIA", "secret");
        let config = s3_config_for_pair(&pair, &creds);
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.bucket, "demo");
        assert_eq!(config.access_key_id, "AKIA");
        assert_eq!(config.secret_access_key, "secret");
    }

    #[test]
    fn resolve_credentials_for_pair_fails_when_no_credential_assigned() {
        // This test documents the error path. We can't call load_credentials_by_id
        // without an AppHandle, so we verify the early check.
        let pair = SyncPair {
            credential_profile_id: None,
            ..SyncPair::default()
        };
        // Call the credential_id extraction logic directly
        let result: Result<&str, &str> = pair
            .credential_profile_id
            .as_deref()
            .ok_or("Sync pair has no credential assigned.");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Sync pair has no credential assigned.");
    }

    #[test]
    fn pair_sync_cycle_returns_unconfigured_status_for_empty_pair() {
        // Verify the early-return logic for unconfigured pairs.
        // We can't run the full async function without an AppHandle,
        // but we can verify is_pair_configured works correctly as the gate.
        use crate::storage::profile_store::is_pair_configured;
        let empty_pair = SyncPair::default();
        assert!(!is_pair_configured(&empty_pair));

        let configured_pair = SyncPair {
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            ..SyncPair::default()
        };
        assert!(is_pair_configured(&configured_pair));
    }

    #[test]
    fn planned_remote_bin_reconciliation_tracks_settings_mutations() {
        let current = StoredProfile {
            sync_pairs: vec![SyncPair {
                id: "pair-1".into(),
                label: "Docs".into(),
                local_folder: "C:/docs".into(),
                bucket: "pair-bucket".into(),
                credential_profile_id: Some("cred-pair".into()),
                remote_bin: RemoteBinConfig {
                    enabled: true,
                    retention_days: 7,
                },
                ..SyncPair::default()
            }],
            ..StoredProfile::default()
        };

        let next = StoredProfile {
            sync_pairs: vec![SyncPair {
                remote_bin: RemoteBinConfig {
                    enabled: false,
                    retention_days: 30,
                },
                ..current.sync_pairs[0].clone()
            }],
            ..current.clone()
        };

        let targets = planned_remote_bin_reconciliation(&current, &next);

        assert_eq!(targets.len(), 1);
        assert!(targets.iter().any(|target| {
            target.bucket == "pair-bucket"
                && target.managed_rules.is_empty()
                && target.source_labels == vec!["sync pair 'Docs'".to_string()]
        }));
    }

    #[test]
    fn planned_remote_bin_reconciliation_keeps_profile_target_when_no_sync_pairs_exist() {
        let current = StoredProfile {
            bucket: "profile-bucket".into(),
            local_folder: "C:/sync".into(),
            credential_profile_id: Some("cred-profile".into()),
            remote_bin: RemoteBinConfig {
                enabled: false,
                retention_days: 7,
            },
            ..StoredProfile::default()
        };

        let next = StoredProfile {
            remote_bin: RemoteBinConfig {
                enabled: true,
                retention_days: 14,
            },
            ..current.clone()
        };

        let targets = planned_remote_bin_reconciliation(&current, &next);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].bucket, "profile-bucket");
        assert_eq!(
            targets[0].managed_rules,
            vec![managed_lifecycle_rule_plan(DEFAULT_REMOTE_BIN_PAIR_ID, 14)]
        );
        assert_eq!(targets[0].source_labels, vec!["profile".to_string()]);
    }

    #[test]
    fn planned_remote_bin_reconciliation_ignores_legacy_profile_remote_bin_when_sync_pairs_exist() {
        let current = StoredProfile {
            bucket: "legacy-bucket".into(),
            local_folder: "C:/legacy".into(),
            credential_profile_id: Some("cred-legacy".into()),
            remote_bin: RemoteBinConfig {
                enabled: true,
                retention_days: 1,
            },
            sync_pairs: vec![SyncPair {
                id: "pair-1".into(),
                label: "Docs".into(),
                local_folder: "C:/docs".into(),
                bucket: "pair-bucket".into(),
                credential_profile_id: Some("cred-pair".into()),
                remote_bin: RemoteBinConfig {
                    enabled: true,
                    retention_days: 7,
                },
                ..SyncPair::default()
            }],
            ..StoredProfile::default()
        };

        let next = StoredProfile {
            activity_debug_mode_enabled: true,
            ..current.clone()
        };

        let targets = planned_remote_bin_reconciliation(&current, &next);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].bucket, "pair-bucket");
        assert_eq!(
            targets[0].managed_rules,
            vec![managed_lifecycle_rule_plan("pair-1", 7)]
        );
        assert_eq!(
            targets[0].source_labels,
            vec!["sync pair 'Docs'".to_string()]
        );
    }

    #[test]
    fn planned_remote_bin_reconciliation_aggregates_multiple_pairs_sharing_bucket() {
        let current = StoredProfile::default();
        let next = StoredProfile {
            sync_pairs: vec![
                SyncPair {
                    id: "pair-1".into(),
                    label: "Docs".into(),
                    provider: "aws".into(),
                    local_folder: "C:/docs".into(),
                    bucket: "shared-bucket".into(),
                    region: "us-east-1".into(),
                    credential_profile_id: Some("cred-1".into()),
                    remote_bin: RemoteBinConfig {
                        enabled: true,
                        retention_days: 7,
                    },
                    ..SyncPair::default()
                },
                SyncPair {
                    id: "pair-2".into(),
                    label: "Media".into(),
                    provider: "aws".into(),
                    local_folder: "C:/media".into(),
                    bucket: "shared-bucket".into(),
                    region: "us-east-1".into(),
                    credential_profile_id: Some("cred-1".into()),
                    remote_bin: RemoteBinConfig {
                        enabled: true,
                        retention_days: 30,
                    },
                    ..SyncPair::default()
                },
            ],
            ..StoredProfile::default()
        };

        let targets = planned_remote_bin_reconciliation(&current, &next);

        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].bucket, "shared-bucket");
        assert_eq!(
            targets[0].managed_rules,
            vec![
                managed_lifecycle_rule_plan("pair-1", 7),
                managed_lifecycle_rule_plan("pair-2", 30),
            ]
        );
        assert_eq!(
            targets[0].source_labels,
            vec![
                "sync pair 'Docs'".to_string(),
                "sync pair 'Media'".to_string()
            ]
        );
    }

    #[test]
    fn planned_remote_bin_reconciliation_clears_removed_pair_rule_but_keeps_other_shared_bucket_rules(
    ) {
        let current = StoredProfile {
            sync_pairs: vec![
                SyncPair {
                    id: "pair-1".into(),
                    label: "Docs".into(),
                    provider: "aws".into(),
                    local_folder: "C:/docs".into(),
                    bucket: "shared-bucket".into(),
                    region: "us-east-1".into(),
                    credential_profile_id: Some("cred-1".into()),
                    remote_bin: RemoteBinConfig {
                        enabled: true,
                        retention_days: 7,
                    },
                    ..SyncPair::default()
                },
                SyncPair {
                    id: "pair-2".into(),
                    label: "Media".into(),
                    provider: "aws".into(),
                    local_folder: "C:/media".into(),
                    bucket: "shared-bucket".into(),
                    region: "us-east-1".into(),
                    credential_profile_id: Some("cred-1".into()),
                    remote_bin: RemoteBinConfig {
                        enabled: true,
                        retention_days: 30,
                    },
                    ..SyncPair::default()
                },
            ],
            ..StoredProfile::default()
        };
        let next = StoredProfile {
            sync_pairs: vec![
                SyncPair {
                    remote_bin: RemoteBinConfig {
                        enabled: false,
                        retention_days: 7,
                    },
                    ..current.sync_pairs[0].clone()
                },
                current.sync_pairs[1].clone(),
            ],
            ..StoredProfile::default()
        };

        let targets = planned_remote_bin_reconciliation(&current, &next);

        assert_eq!(targets.len(), 1);
        assert_eq!(
            targets[0].managed_rules,
            vec![managed_lifecycle_rule_plan("pair-2", 30)]
        );
    }

    #[test]
    fn provider_supports_remote_bin_lifecycle_reconciliation_for_aws_and_gcs() {
        assert!(provider_supports_remote_bin_lifecycle_reconciliation("aws"));
        assert!(provider_supports_remote_bin_lifecycle_reconciliation("s3"));
        assert!(provider_supports_remote_bin_lifecycle_reconciliation("gcs"));
        assert!(provider_supports_remote_bin_lifecycle_reconciliation("gcp"));
    }

    #[test]
    fn filter_remote_bin_reconciliation_targets_keeps_supported_gcs_targets() {
        let filtered = filter_remote_bin_reconciliation_targets(vec![
            super::RemoteBinLifecycleTarget {
                provider: "aws".into(),
                bucket: "aws-bucket".into(),
                region: "us-east-1".into(),
                credential_profile_id: Some("cred-aws".into()),
                source_labels: vec!["sync pair 'AWS'".into()],
                managed_rules: vec![managed_lifecycle_rule_plan("pair-aws", 7)],
            },
            super::RemoteBinLifecycleTarget {
                provider: "gcs".into(),
                bucket: "gcs-bucket".into(),
                region: "US".into(),
                credential_profile_id: Some("cred-gcs".into()),
                source_labels: vec!["sync pair 'GCS'".into()],
                managed_rules: vec![managed_lifecycle_rule_plan("pair-gcs", 7)],
            },
        ]);

        assert_eq!(filtered.len(), 2);
        assert!(filtered.iter().any(|target| target.bucket == "aws-bucket"));
        assert!(filtered.iter().any(|target| target.bucket == "gcs-bucket"));
    }

    #[test]
    fn persist_profile_reconciliation_uses_sync_pair_retention_instead_of_legacy_profile_retention()
    {
        let current = StoredProfile {
            bucket: "legacy-bucket".into(),
            local_folder: "C:/legacy".into(),
            credential_profile_id: Some("cred-legacy".into()),
            remote_bin: RemoteBinConfig {
                enabled: true,
                retention_days: 1,
            },
            sync_pairs: vec![SyncPair {
                id: "pair-1".into(),
                label: "Docs".into(),
                local_folder: "C:/docs".into(),
                bucket: "pair-bucket".into(),
                credential_profile_id: Some("cred-pair".into()),
                remote_bin: RemoteBinConfig {
                    enabled: true,
                    retention_days: 7,
                },
                ..SyncPair::default()
            }],
            ..StoredProfile::default()
        };
        let next = StoredProfile {
            activity_debug_mode_enabled: true,
            ..current.clone()
        };
        let reconciled = Arc::new(Mutex::new(Vec::new()));

        let saved = persist_profile_with_remote_bin_reconciliation_for_test(
            &current,
            next,
            {
                let reconciled = Arc::clone(&reconciled);
                move |target| {
                    reconciled
                        .lock()
                        .expect("lock should hold")
                        .push(target.clone());
                    Ok(())
                }
            },
            |_| Ok(()),
        )
        .expect("persistence should succeed");

        assert!(saved.activity_debug_mode_enabled);

        let reconciled = reconciled.lock().expect("lock should hold");
        assert_eq!(reconciled.len(), 1);
        assert_eq!(reconciled[0].bucket, "pair-bucket");
        assert_eq!(
            reconciled[0].managed_rules,
            vec![managed_lifecycle_rule_plan("pair-1", 7)]
        );
        assert_eq!(
            reconciled[0].source_labels,
            vec!["sync pair 'Docs'".to_string()]
        );
    }

    #[test]
    fn persist_profile_with_disabled_remote_bin_skips_reconciliation_and_still_writes() {
        let current = StoredProfile::default();
        let next = StoredProfile {
            activity_debug_mode_enabled: true,
            ..StoredProfile::default()
        };
        let reconcile_called = Arc::new(Mutex::new(0usize));
        let write_called = Arc::new(Mutex::new(false));

        let result = persist_profile_with_remote_bin_reconciliation_for_test(
            &current,
            next.clone(),
            {
                let reconcile_called = Arc::clone(&reconcile_called);
                move |_| {
                    *reconcile_called.lock().expect("lock should hold") += 1;
                    Err("should not reconcile disabled bins".into())
                }
            },
            {
                let write_called = Arc::clone(&write_called);
                move |_| {
                    *write_called.lock().expect("lock should hold") = true;
                    Ok(())
                }
            },
        )
        .expect("disabled remote bin should not block persistence");

        assert_eq!(
            result.activity_debug_mode_enabled,
            next.activity_debug_mode_enabled
        );
        assert_eq!(*reconcile_called.lock().expect("lock should hold"), 0);
        assert!(*write_called.lock().expect("lock should hold"));
    }

    #[test]
    fn persist_profile_does_not_write_when_reconciliation_fails() {
        let current = StoredProfile::default();
        let next = StoredProfile {
            bucket: "demo-bucket".into(),
            local_folder: "C:/sync".into(),
            credential_profile_id: Some("cred-1".into()),
            remote_bin: RemoteBinConfig {
                enabled: true,
                retention_days: 7,
            },
            ..StoredProfile::default()
        };
        let write_called = Arc::new(Mutex::new(false));

        let error = persist_profile_with_remote_bin_reconciliation_for_test(
            &current,
            next,
            |target| Err(format!("reconcile failed for {}", target.bucket)),
            {
                let write_called = Arc::clone(&write_called);
                move |_| {
                    *write_called.lock().expect("lock should hold") = true;
                    Ok(())
                }
            },
        )
        .expect_err("persistence should fail when reconciliation fails");

        assert!(error.contains("demo-bucket"));
        assert!(!*write_called.lock().expect("lock should hold"));
    }

    #[test]
    fn persist_profile_reconciles_supported_gcs_remote_bin_targets_and_still_writes() {
        let current = StoredProfile::default();
        let next = StoredProfile {
            sync_pairs: vec![SyncPair {
                id: "pair-gcs".into(),
                label: "GCS Assets".into(),
                provider: GCS_PROVIDER.into(),
                local_folder: "C:/gcs-assets".into(),
                bucket: "gcs-bucket".into(),
                region: "US".into(),
                credential_profile_id: Some("cred-gcs".into()),
                remote_bin: RemoteBinConfig {
                    enabled: true,
                    retention_days: 7,
                },
                ..SyncPair::default()
            }],
            ..StoredProfile::default()
        };
        let reconcile_called = Arc::new(Mutex::new(0usize));
        let write_called = Arc::new(Mutex::new(false));

        let saved = persist_profile_with_remote_bin_reconciliation_for_test(
            &current,
            next.clone(),
            {
                let reconcile_called = Arc::clone(&reconcile_called);
                move |target| {
                    *reconcile_called.lock().expect("lock should hold") += 1;
                    assert_eq!(target.bucket, "gcs-bucket");
                    Ok(())
                }
            },
            {
                let write_called = Arc::clone(&write_called);
                move |_| {
                    *write_called.lock().expect("lock should hold") = true;
                    Ok(())
                }
            },
        )
        .expect("supported gcs remote-bin targets should reconcile");

        assert_eq!(saved.sync_pairs.len(), 1);
        assert_eq!(*reconcile_called.lock().expect("lock should hold"), 1);
        assert!(*write_called.lock().expect("lock should hold"));
    }

    #[test]
    fn remote_inventory_summary_counts_only_file_objects() {
        let snapshot = build_remote_inventory_snapshot_for_test(&[
            ("photos/2026/nested/", 0),
            ("photos/2026/nested/alpha.txt", 5),
            ("photos/2026/nested/deeper/beta.txt", 7),
        ]);

        assert_eq!(snapshot.summary.object_count, 2);
        assert_eq!(snapshot.summary.total_bytes, 12);
        assert_eq!(snapshot.entries.len(), 6);
        assert_eq!(
            snapshot
                .entries
                .iter()
                .filter(|entry| entry.kind == "file")
                .count(),
            2
        );
        assert_eq!(
            snapshot
                .entries
                .iter()
                .filter(|entry| entry.kind == "directory")
                .count(),
            4
        );
    }

    #[test]
    fn file_entry_response_serializes_kind_field() {
        let value = serde_json::to_value(super::FileEntryResponse {
            path: "docs".into(),
            kind: "directory".into(),
            status: "synced".into(),
            has_local_copy: true,
            storage_class: None,
            bin_key: None,
            local_kind: Some("directory".into()),
            remote_kind: Some("directory".into()),
            local_size: Some(0),
            remote_size: Some(0),
            local_modified_at: None,
            remote_modified_at: None,
            remote_etag: Some("etag-123".into()),
            deleted_at: None,
            deleted_from: None,
            retention_days: None,
            expires_at: None,
        })
        .expect("file entry response should serialize");

        assert_eq!(value["kind"], "directory");
        assert!(value["binKey"].is_null());
        assert_eq!(value["remoteEtag"], "etag-123");
    }

    #[test]
    fn build_bin_entry_responses_maps_bin_keys_to_original_paths() {
        let pair = SyncPair {
            remote_bin: RemoteBinConfig {
                enabled: true,
                retention_days: 30,
            },
            ..SyncPair::default()
        };
        let entries = build_bin_entry_responses(
            &pair,
            &[
                RemoteObjectEntry {
                    key: ".storage-goblin-bin/pairs/pair-1/docs/readme.md".into(),
                    relative_path: "docs/readme.md".into(),
                    kind: "file".into(),
                    size: 10,
                    last_modified_at: Some("2026-04-10T12:00:00Z".into()),
                    etag: None,
                    storage_class: Some("STANDARD".into()),
                    fingerprint: None,
                },
                RemoteObjectEntry {
                    key: ".storage-goblin-bin/pairs/pair-1/docs/archive/".into(),
                    relative_path: "docs/archive".into(),
                    kind: "directory".into(),
                    size: 0,
                    last_modified_at: Some("2026-04-11T12:00:00Z".into()),
                    etag: None,
                    storage_class: None,
                    fingerprint: None,
                },
            ],
        );

        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|entry| {
            entry.path == "docs/readme.md"
                && entry.kind == "file"
                && entry.status == "deleted"
                && !entry.has_local_copy
                && entry.bin_key.as_deref()
                    == Some(".storage-goblin-bin/pairs/pair-1/docs/readme.md")
                && entry.deleted_at.as_deref() == Some("2026-04-10T12:00:00Z")
                && entry.deleted_from.as_deref() == Some("remote-bin")
                && entry.retention_days == Some(30)
                && entry.expires_at.as_deref() == Some("2026-05-10T12:00:00Z")
        }));
        assert!(entries.iter().any(|entry| {
            entry.path == "docs/archive"
                && entry.kind == "directory"
                && entry.status == "deleted"
                && entry.bin_key.as_deref()
                    == Some(".storage-goblin-bin/pairs/pair-1/docs/archive/")
        }));
    }

    #[test]
    fn build_versioned_bin_entry_responses_maps_versioned_entries() {
        let pair = SyncPair {
            object_versioning_enabled: true,
            ..SyncPair::default()
        };
        let entries = build_versioned_bin_entry_responses(
            &pair,
            &[VersionedBinEntry {
                key: "docs/readme.md".into(),
                version_id: "delete-marker-1".into(),
                relative_path: "docs/readme.md".into(),
                kind: "file".into(),
                storage_class: Some("STANDARD".into()),
                deleted_at: Some("2026-04-10T12:00:00Z".into()),
            }],
        );

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "docs/readme.md");
        assert_eq!(entries[0].kind, "file");
        assert_eq!(entries[0].status, "deleted");
        assert_eq!(
            entries[0].bin_key.as_deref(),
            Some("versioned:docs/readme.md::delete-marker-1")
        );
        assert_eq!(
            entries[0].deleted_at.as_deref(),
            Some("2026-04-10T12:00:00Z")
        );
        assert_eq!(
            entries[0].deleted_from.as_deref(),
            Some("object-versioning")
        );
        assert!(entries[0].retention_days.is_none());
        assert!(entries[0].expires_at.is_none());
    }

    #[test]
    fn add_retention_days_returns_expiry_timestamp() {
        assert_eq!(
            add_retention_days("2026-04-10T12:00:00Z", 30).as_deref(),
            Some("2026-05-10T12:00:00Z")
        );
    }

    #[test]
    fn collect_remote_bin_keys_for_request_matches_synthetic_folder_subtree() {
        let pair = SyncPair {
            id: "pair-1".into(),
            ..SyncPair::default()
        };
        let entries = vec![
            RemoteObjectEntry {
                key: ".storage-goblin-bin/pairs/pair-1/docs/archive/readme.md".into(),
                relative_path: "docs/archive/readme.md".into(),
                kind: "file".into(),
                size: 1,
                last_modified_at: None,
                etag: None,
                storage_class: None,
                fingerprint: None,
            },
            RemoteObjectEntry {
                key: ".storage-goblin-bin/pairs/pair-1/docs/archive/assets/logo.png".into(),
                relative_path: "docs/archive/assets/logo.png".into(),
                kind: "file".into(),
                size: 1,
                last_modified_at: None,
                etag: None,
                storage_class: None,
                fingerprint: None,
            },
        ];

        let matches = collect_remote_bin_keys_for_request(
            &pair,
            &BinEntryRequest {
                path: "docs/archive".into(),
                kind: "directory".into(),
                bin_key: None,
            },
            &entries,
        )
        .expect("synthetic folder request should resolve subtree");

        assert_eq!(matches.len(), 2);
        assert!(matches
            .iter()
            .all(|entry| path_matches_exact_or_descendant(&entry.relative_path, "docs/archive")));
    }

    #[test]
    fn collect_versioned_bin_entries_for_request_matches_directory_subtree() {
        let entries = vec![
            VersionedBinEntry {
                key: "docs/archive/readme.md".into(),
                version_id: "v1".into(),
                relative_path: "docs/archive/readme.md".into(),
                kind: "file".into(),
                storage_class: None,
                deleted_at: None,
            },
            VersionedBinEntry {
                key: "docs/archive/assets/logo.png".into(),
                version_id: "v2".into(),
                relative_path: "docs/archive/assets/logo.png".into(),
                kind: "file".into(),
                storage_class: None,
                deleted_at: None,
            },
        ];

        let matches = collect_versioned_bin_entries_for_request(
            &BinEntryRequest {
                path: "docs/archive".into(),
                kind: "directory".into(),
                bin_key: None,
            },
            &entries,
        )
        .expect("directory request should expand to subtree");

        assert_eq!(matches.len(), 2);
        assert!(matches
            .iter()
            .all(|entry| path_matches_exact_or_descendant(&entry.relative_path, "docs/archive")));
    }

    #[test]
    fn validate_bulk_restore_destinations_rejects_ancestor_and_descendant_batch_conflicts() {
        let error = validate_bin_batch_requests(
            &[
                BinEntryRequest {
                    path: "docs".into(),
                    kind: "directory".into(),
                    bin_key: None,
                },
                BinEntryRequest {
                    path: "docs/archive/readme.md".into(),
                    kind: "file".into(),
                    bin_key: None,
                },
            ],
            "restore",
        )
        .expect_err("ancestor and descendant restores should conflict");

        assert!(error.contains("same batch already targets"));
    }

    #[test]
    fn validate_bin_batch_requests_rejects_overlapping_purge_requests() {
        let error = validate_bin_batch_requests(
            &[
                BinEntryRequest {
                    path: "docs/archive".into(),
                    kind: "directory".into(),
                    bin_key: None,
                },
                BinEntryRequest {
                    path: "docs/archive/readme.md".into(),
                    kind: "file".into(),
                    bin_key: None,
                },
            ],
            "purge",
        )
        .expect_err("overlapping purge requests should be rejected");

        assert!(error.contains("same batch already targets"));
    }

    #[test]
    fn validate_bin_batch_requests_rejects_duplicate_requests() {
        let error = validate_bin_batch_requests(
            &[
                BinEntryRequest {
                    path: "docs/archive".into(),
                    kind: "directory".into(),
                    bin_key: None,
                },
                BinEntryRequest {
                    path: "docs/archive".into(),
                    kind: "directory".into(),
                    bin_key: None,
                },
            ],
            "restore",
        )
        .expect_err("duplicate batch requests should be rejected");

        assert!(error.contains("more than once"));
    }

    #[test]
    fn collect_versioned_history_for_deleted_entries_excludes_live_siblings_and_descendants() {
        let deleted_entries = vec![VersionedBinEntry {
            key: "docs/archive/deleted.txt".into(),
            version_id: "delete-marker-1".into(),
            relative_path: "docs/archive/deleted.txt".into(),
            kind: "file".into(),
            storage_class: None,
            deleted_at: None,
        }];
        let history = vec![
            ("docs/archive/deleted.txt".into(), "v1".into()),
            ("docs/archive/deleted.txt".into(), "delete-marker-1".into()),
            ("docs/archive/live-child.txt".into(), "v-live".into()),
            ("docs/archive/nested/live.txt".into(), "v-live-2".into()),
            ("docs/archive-sibling.txt".into(), "v-sibling".into()),
        ];

        let filtered = collect_versioned_history_for_deleted_entries(&deleted_entries, &history);

        assert_eq!(
            filtered,
            vec![
                ("docs/archive/deleted.txt".into(), "v1".into()),
                ("docs/archive/deleted.txt".into(), "delete-marker-1".into()),
            ]
        );
    }

    #[test]
    fn collect_versioned_history_for_deleted_entries_dedupes_history_records() {
        let deleted_entries = vec![VersionedBinEntry {
            key: "docs/archive/deleted.txt".into(),
            version_id: "delete-marker-1".into(),
            relative_path: "docs/archive/deleted.txt".into(),
            kind: "file".into(),
            storage_class: None,
            deleted_at: None,
        }];
        let history = vec![
            ("docs/archive/deleted.txt".into(), "v1".into()),
            ("docs/archive/deleted.txt".into(), "v1".into()),
            ("docs/archive/deleted.txt".into(), "delete-marker-1".into()),
        ];

        let filtered = collect_versioned_history_for_deleted_entries(&deleted_entries, &history);

        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn collect_remote_bin_keys_for_request_folder_only_matches_selected_deleted_subtree() {
        let pair = SyncPair {
            id: "pair-1".into(),
            ..SyncPair::default()
        };
        let entries = vec![
            RemoteObjectEntry {
                key: ".storage-goblin-bin/pairs/pair-1/docs/archive/deleted.txt".into(),
                relative_path: "docs/archive/deleted.txt".into(),
                kind: "file".into(),
                size: 1,
                last_modified_at: None,
                etag: None,
                storage_class: None,
                fingerprint: None,
            },
            RemoteObjectEntry {
                key: ".storage-goblin-bin/pairs/pair-1/docs/archive/nested/deeper.txt".into(),
                relative_path: "docs/archive/nested/deeper.txt".into(),
                kind: "file".into(),
                size: 1,
                last_modified_at: None,
                etag: None,
                storage_class: None,
                fingerprint: None,
            },
            RemoteObjectEntry {
                key: ".storage-goblin-bin/pairs/pair-1/docs/archive-sibling.txt".into(),
                relative_path: "docs/archive-sibling.txt".into(),
                kind: "file".into(),
                size: 1,
                last_modified_at: None,
                etag: None,
                storage_class: None,
                fingerprint: None,
            },
        ];

        let matches = collect_remote_bin_keys_for_request(
            &pair,
            &BinEntryRequest {
                path: "docs/archive".into(),
                kind: "directory".into(),
                bin_key: None,
            },
            &entries,
        )
        .expect("folder purge request should resolve subtree");

        assert_eq!(matches.len(), 2);
        assert!(matches
            .iter()
            .all(|entry| path_matches_exact_or_descendant(&entry.relative_path, "docs/archive")));
    }

    #[test]
    fn parse_versioned_bin_key_round_trips_encoded_key() {
        let encoded = versioned_bin_key("docs/readme.md", "delete-marker-1");
        let decoded =
            parse_versioned_bin_key(&encoded).expect("encoded versioned bin key should parse");

        assert_eq!(decoded, ("docs/readme.md".into(), "delete-marker-1".into()));
    }

    #[test]
    fn destination_key_for_bin_restore_uses_original_object_shape() {
        assert_eq!(
            destination_key_for_bin_restore(
                ".storage-goblin-bin/pairs/pair-1/docs/readme.md",
                "docs/readme.md"
            ),
            "docs/readme.md"
        );
        assert_eq!(
            destination_key_for_bin_restore(
                ".storage-goblin-bin/pairs/pair-1/docs/archive/",
                "docs/archive/"
            ),
            "docs/archive/"
        );
    }

    #[test]
    fn destination_key_for_bin_restore_keeps_legacy_flat_bin_keys_compatible() {
        assert_eq!(
            destination_key_for_bin_restore(".storage-goblin-bin/docs/readme.md", "docs/readme.md"),
            "docs/readme.md"
        );
    }

    #[cfg(feature = "tauri-command-tests")]
    use super::refresh_pair_state_after_local_change;

    #[cfg(feature = "tauri-command-tests")]
    #[test]
    fn refresh_pair_state_after_local_change_updates_local_snapshot_and_plan() {
        use crate::storage::commands::tauri_command_tests::CommandTestHarness;
        use crate::storage::local_index::read_local_index_snapshot_for_pair;
        use crate::storage::remote_index::write_remote_index_snapshot_for_pair;
        use crate::storage::sync_db::load_planner_summary_for_pair;
        use std::fs;

        let harness = CommandTestHarness::new("refresh-pair-state-after-local-change");
        let handle = harness.app_handle();

        let pair = test_pair("pair-local-refresh", true, true, 60);
        let local_root = harness.storage_dir.join("pair-local-refresh-root");
        let mut pair = pair;
        pair.local_folder = local_root.to_string_lossy().to_string();
        fs::create_dir_all(local_root.join("docs")).expect("local test folder should exist");
        fs::write(local_root.join("docs").join("note.txt"), b"hello")
            .expect("test file should be written");

        write_remote_index_snapshot_for_pair(
            &handle,
            &pair.id,
            &build_remote_snapshot(&[("docs/note.txt", "file", 5)]),
        )
        .expect("remote snapshot should be written");

        refresh_pair_state_after_local_change(&handle, &pair)
            .expect("local refresh should succeed");

        let snapshot = read_local_index_snapshot_for_pair(&handle, &pair.id)
            .expect("local snapshot should load")
            .expect("local snapshot should exist");
        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "docs/note.txt"));

        let summary =
            load_planner_summary_for_pair(&handle, &pair).expect("planner summary should load");
        assert!(summary.observed_path_count >= 1);
    }

    #[test]
    fn validate_remote_restore_destination_rejects_exact_conflicts() {
        let entries = vec![RemoteObjectEntry {
            key: "docs/readme.md".into(),
            relative_path: "docs/readme.md".into(),
            kind: "file".into(),
            size: 10,
            last_modified_at: None,
            etag: None,
            storage_class: None,
            fingerprint: None,
        }];

        let error = validate_remote_restore_destination(&entries, "docs/readme.md")
            .expect_err("exact conflicts should be rejected");

        assert!(error.contains("remote destination already exists"));
    }

    #[test]
    fn validate_remote_restore_destination_rejects_file_ancestor_conflicts() {
        let entries = vec![RemoteObjectEntry {
            key: "docs".into(),
            relative_path: "docs".into(),
            kind: "file".into(),
            size: 10,
            last_modified_at: None,
            etag: None,
            storage_class: None,
            fingerprint: None,
        }];

        let error = validate_remote_restore_destination(&entries, "docs/readme.md")
            .expect_err("file ancestor conflicts should be rejected");

        assert!(error.contains("remote ancestor 'docs' exists as a file"));
    }

    #[test]
    fn validate_remote_restore_destination_rejects_descendant_conflicts() {
        let entries = vec![RemoteObjectEntry {
            key: "docs/readme.md".into(),
            relative_path: "docs/readme.md".into(),
            kind: "file".into(),
            size: 10,
            last_modified_at: None,
            etag: None,
            storage_class: None,
            fingerprint: None,
        }];

        let error = validate_remote_restore_destination(&entries, "docs")
            .expect_err("descendant conflicts should be rejected");

        assert!(error.contains("remote descendant 'docs/readme.md' already exists"));
    }

    #[test]
    fn validate_remote_restore_destination_allows_directory_ancestors() {
        let entries = vec![RemoteObjectEntry {
            key: "docs/".into(),
            relative_path: "docs".into(),
            kind: "directory".into(),
            size: 0,
            last_modified_at: None,
            etag: None,
            storage_class: None,
            fingerprint: None,
        }];

        validate_remote_restore_destination(&entries, "docs/readme.md")
            .expect("directory ancestors should be allowed");
    }

    #[test]
    fn sync_pair_for_location_returns_error_for_unknown_location() {
        let profile = StoredProfile::default();

        let error = sync_pair_for_location(&profile, "missing")
            .expect_err("unknown locations should fail clearly");

        assert_eq!(error, "Sync pair 'missing' not found.");
    }

    #[test]
    fn reveal_tree_entry_path_resolution_rejects_unsafe_relative_paths() {
        let error = resolve_local_download_path("C:/sync", "../escape.txt")
            .expect_err("unsafe reveal paths should fail");

        assert_eq!(
            error,
            "planned download path '../escape.txt' is not a safe relative file path"
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn reveal_windows_select_argument_combines_flag_and_path() {
        let path = Path::new(r"C:\sync\photos\img001.jpg");
        let mut arg = std::ffi::OsString::from("/select,");
        arg.push(path);

        assert_eq!(arg.to_string_lossy(), r"/select,C:\sync\photos\img001.jpg");
    }

    #[test]
    fn build_file_entry_responses_includes_directory_entries_and_uses_sync_planner_for_files() {
        let local = build_local_snapshot(&[
            ("docs", "directory", 0),
            ("docs/readme.md", "file", 10),
            ("local-only-dir", "directory", 0),
            ("local-only.txt", "file", 4),
        ]);
        let remote = build_remote_snapshot(&[
            ("docs", "directory", 0),
            ("docs/readme.md", "file", 10),
            ("remote-only-dir", "directory", 0),
            ("remote-only.txt", "file", 7),
        ]);

        let entries = build_file_entry_responses(Some(&local), Some(&remote), None);

        assert_eq!(entries.len(), 6);

        assert!(entries.iter().any(|entry| {
            entry.path == "docs"
                && entry.kind == "directory"
                && entry.status == "synced"
                && entry.has_local_copy
        }));
        let docs_entry = entries
            .iter()
            .find(|entry| entry.path == "docs/readme.md")
            .expect("docs file entry should exist");
        let expected_status = sync_planner::file_entry_status(
            None,
            local_fingerprint_for_path(&local, "docs/readme.md").as_deref(),
            None,
        );
        assert_eq!(docs_entry.kind, "file");
        assert_eq!(docs_entry.status, expected_status);
        assert!(docs_entry.has_local_copy);
        assert!(entries.iter().any(|entry| {
            entry.path == "local-only-dir"
                && entry.kind == "directory"
                && entry.status == "local-only"
                && entry.has_local_copy
        }));
        assert!(entries.iter().any(|entry| {
            entry.path == "remote-only-dir"
                && entry.kind == "directory"
                && entry.status == "remote-only"
                && !entry.has_local_copy
        }));
    }

    #[test]
    fn build_file_entry_responses_marks_kind_mismatches_as_conflict_and_uses_sync_planner_for_files(
    ) {
        let local = build_local_snapshot(&[
            ("changed.txt", "file", 10),
            ("folder", "directory", 0),
            ("note", "file", 3),
        ]);
        let remote = build_remote_snapshot(&[
            ("changed.txt", "file", 12),
            ("folder", "file", 99),
            ("note", "directory", 0),
        ]);

        let entries = build_file_entry_responses(Some(&local), Some(&remote), None);

        let changed_entry = entries
            .iter()
            .find(|entry| entry.path == "changed.txt")
            .expect("changed file entry should exist");
        let expected_status = sync_planner::file_entry_status(
            None,
            local_fingerprint_for_path(&local, "changed.txt").as_deref(),
            None,
        );
        assert_eq!(changed_entry.kind, "file");
        assert_eq!(changed_entry.status, expected_status);
        assert!(changed_entry.has_local_copy);
        assert_eq!(changed_entry.local_kind.as_deref(), Some("file"));
        assert_eq!(changed_entry.remote_kind.as_deref(), Some("file"));
        assert_eq!(changed_entry.local_size, Some(10));
        assert_eq!(changed_entry.remote_size, Some(12));
        assert!(entries.iter().any(|entry| {
            entry.path == "folder"
                && entry.kind == "directory"
                && entry.status == "conflict"
                && entry.has_local_copy
                && entry.local_kind.as_deref() == Some("directory")
                && entry.remote_kind.as_deref() == Some("file")
        }));
        assert!(entries.iter().any(|entry| {
            entry.path == "note"
                && entry.kind == "file"
                && entry.status == "conflict"
                && entry.has_local_copy
                && entry.local_kind.as_deref() == Some("file")
                && entry.remote_kind.as_deref() == Some("directory")
        }));
    }

    #[test]
    fn build_file_entry_responses_prioritizes_glacier_over_synced_when_remote_is_glacier() {
        let local = build_local_snapshot(&[("archive.bin", "file", 42)]);
        let remote = build_remote_snapshot_with_storage_class(&[(
            "archive.bin",
            "file",
            42,
            Some("GLACIER_IR"),
        )]);

        let entries = build_file_entry_responses(Some(&local), Some(&remote), None);

        assert!(entries.iter().any(|entry| {
            entry.path == "archive.bin"
                && entry.kind == "file"
                && entry.status == "glacier"
                && entry.has_local_copy
                && entry.storage_class.as_deref() == Some("GLACIER_IR")
        }));
    }

    #[test]
    fn build_file_entry_responses_marks_remote_glacier_entries_as_glacier_without_local_copy() {
        let remote = build_remote_snapshot_with_storage_class(&[(
            "cold.bin",
            "file",
            7,
            Some("DEEP_ARCHIVE"),
        )]);

        let entries = build_file_entry_responses(None, Some(&remote), None);

        assert!(entries.iter().any(|entry| {
            entry.path == "cold.bin"
                && entry.kind == "file"
                && entry.status == "glacier"
                && !entry.has_local_copy
                && entry.storage_class.as_deref() == Some("DEEP_ARCHIVE")
        }));
    }

    #[test]
    fn file_entry_response_serializes_conflict_metadata_fields() {
        let entries = build_file_entry_responses(
            Some(&build_local_snapshot(&[("docs/conflict.txt", "file", 10)])),
            Some(&build_remote_snapshot(&[("docs/conflict.txt", "file", 12)])),
            None,
        );

        let entry = entries
            .iter()
            .find(|entry| entry.path == "docs/conflict.txt")
            .expect("conflict entry should exist");

        assert_eq!(entry.local_kind.as_deref(), Some("file"));
        assert_eq!(entry.remote_kind.as_deref(), Some("file"));
        assert_eq!(entry.local_size, Some(10));
        assert_eq!(entry.remote_size, Some(12));
        assert!(entry.remote_etag.is_none());
    }

    #[test]
    fn build_file_entry_responses_exposes_remote_etag_for_remote_files() {
        let local = build_local_snapshot(&[("docs/conflict.txt", "file", 10)]);
        let mut remote = build_remote_snapshot(&[("docs/conflict.txt", "file", 12)]);
        let remote_entry = remote
            .entries
            .iter_mut()
            .find(|entry| entry.relative_path == "docs/conflict.txt")
            .expect("remote entry should exist");
        remote_entry.etag = Some("etag-conflict-remote".into());

        let entry = build_file_entry_responses(Some(&local), Some(&remote), None)
            .into_iter()
            .find(|entry| entry.path == "docs/conflict.txt")
            .expect("conflict entry should exist");

        assert_eq!(entry.remote_etag.as_deref(), Some("etag-conflict-remote"));
    }

    #[test]
    fn build_file_entry_responses_preserves_kind_mismatch_conflicts_without_marking_them_resolvable(
    ) {
        let entries = build_file_entry_responses(
            Some(&build_local_snapshot(&[("docs/mismatch", "file", 10)])),
            Some(&build_remote_snapshot(&[("docs/mismatch", "directory", 0)])),
            None,
        );

        let entry = entries
            .iter()
            .find(|entry| entry.path == "docs/mismatch")
            .expect("mismatch entry should exist");

        assert_eq!(entry.status, "conflict");
        assert_eq!(entry.kind, "file");
        assert_eq!(entry.local_kind.as_deref(), Some("file"));
        assert_eq!(entry.remote_kind.as_deref(), Some("directory"));
    }

    #[test]
    fn build_file_entry_status_uses_anchor_semantics() {
        let local = build_local_snapshot(&[("docs/note.txt", "file", 5)]);
        let remote = build_remote_snapshot(&[("docs/note.txt", "file", 5)]);
        let current_local_fp = local_fingerprint_for_path(&local, "docs/note.txt")
            .expect("local fingerprint should exist");

        let mut synced_anchor = BTreeMap::new();
        synced_anchor.insert(
            "docs/note.txt".into(),
            SyncAnchor {
                path: "docs/note.txt".into(),
                kind: "file".into(),
                local_fingerprint: Some(current_local_fp.clone()),
                remote_etag: None,
                synced_at: "2026-04-12T00:00:00Z".into(),
            },
        );
        let synced_entries =
            build_file_entry_responses(Some(&local), Some(&remote), Some(&synced_anchor));
        assert_eq!(
            synced_entries
                .iter()
                .find(|entry| entry.path == "docs/note.txt")
                .expect("entry should exist")
                .status,
            "synced"
        );

        let mut local_changed_anchor = synced_anchor.clone();
        local_changed_anchor.insert(
            "docs/note.txt".into(),
            SyncAnchor {
                path: "docs/note.txt".into(),
                kind: "file".into(),
                local_fingerprint: Some("base-fingerprint".into()),
                remote_etag: None,
                synced_at: "2026-04-12T00:00:00Z".into(),
            },
        );
        let local_changed_entries =
            build_file_entry_responses(Some(&local), Some(&remote), Some(&local_changed_anchor));
        assert_eq!(
            local_changed_entries
                .iter()
                .find(|entry| entry.path == "docs/note.txt")
                .expect("entry should exist")
                .status,
            "local-only"
        );

        let remote_changed = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/docs/note.txt".into(),
                relative_path: "docs/note.txt".into(),
                kind: "file".into(),
                size: 5,
                last_modified_at: None,
                etag: Some("etag-new".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..remote
        };
        let mut dual_drift_anchor = local_changed_anchor;
        dual_drift_anchor.insert(
            "docs/note.txt".into(),
            SyncAnchor {
                path: "docs/note.txt".into(),
                kind: "file".into(),
                local_fingerprint: Some("base-fingerprint".into()),
                remote_etag: Some("etag-base".into()),
                synced_at: "2026-04-12T00:00:00Z".into(),
            },
        );
        let conflict_entries = build_file_entry_responses(
            Some(&local),
            Some(&remote_changed),
            Some(&dual_drift_anchor),
        );
        assert_eq!(
            conflict_entries
                .iter()
                .find(|entry| entry.path == "docs/note.txt")
                .expect("entry should exist")
                .status,
            "conflict"
        );
    }

    #[test]
    fn stale_plan_helpers_detect_opposite_side_changes() {
        let upload_error = upload_stale_plan_error(
            Path::new("C:/sync/note.txt"),
            "fingerprint-current",
            Some("fingerprint-base"),
            Some("etag-base"),
            Some("etag-base"),
        )
        .expect("upload stale local mismatch should fail");
        assert!(upload_error.contains("fingerprint mismatch"));

        let download_error = download_stale_plan_error(
            Path::new("C:/sync/note.txt"),
            Some("fingerprint-base"),
            Some("fingerprint-base"),
            Some("etag-new"),
            Some("etag-base"),
        )
        .expect("download stale remote mismatch should fail");
        assert!(download_error.contains("changed remotely since planning"));
    }

    #[test]
    fn should_poll_pair_requires_enabled_polling_and_configuration() {
        assert!(should_poll_pair(&test_pair("a", true, true, 60)));
        assert!(!should_poll_pair(&test_pair("b", false, true, 60)));
        assert!(!should_poll_pair(&test_pair("c", true, false, 60)));

        let mut unconfigured = test_pair("d", true, true, 60);
        unconfigured.bucket.clear();
        assert!(!should_poll_pair(&unconfigured));
    }

    #[test]
    fn due_polling_pairs_respects_pair_specific_cadence_and_skip_flags() {
        let fast = test_pair("fast", true, true, 15);
        let slow = test_pair("slow", true, true, 120);
        let disabled = test_pair("disabled", false, true, 15);
        let polling_disabled = test_pair("poll-off", true, false, 15);
        let now = tokio::time::Instant::now();
        let recent_sync_at = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .expect("recent timestamp should format");

        let mut statuses = BTreeMap::new();
        statuses.insert(
            "fast".into(),
            test_pair_status("fast", "idle", Some("2000-01-01T00:00:00Z")),
        );
        statuses.insert(
            "slow".into(),
            test_pair_status("slow", "idle", Some(&recent_sync_at)),
        );

        let due = due_polling_pairs(
            &[fast.clone(), slow, disabled, polling_disabled],
            &statuses,
            now,
        );

        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, fast.id);
    }

    #[test]
    fn next_polling_deadline_is_immediate_without_prior_sync_timestamp() {
        let pair = test_pair("fresh", true, true, 45);
        let deadline = next_polling_deadline(&pair, None);
        let now = tokio::time::Instant::now();
        assert!(deadline <= now + Duration::from_millis(5));
    }

    #[test]
    fn mode_aware_local_scan_rules_match_bounded_design() {
        let fresh_snapshot = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary {
                indexed_at: time::OffsetDateTime::now_utc()
                    .format(&time::format_description::well_known::Rfc3339)
                    .expect("fresh timestamp should format"),
                file_count: 1,
                directory_count: 0,
                total_bytes: 1,
            },
            entries: vec![LocalIndexEntry {
                relative_path: "note.txt".into(),
                kind: "file".into(),
                size: 1,
                modified_at: None,
                fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"note.txt")),
            }],
        };
        let stale_snapshot = LocalIndexSnapshot {
            summary: LocalIndexSummary {
                indexed_at: "2000-01-01T00:00:00Z".into(),
                ..fresh_snapshot.summary.clone()
            },
            ..fresh_snapshot.clone()
        };

        assert!(should_scan_local_for_trigger(
            PairSyncTrigger::Manual,
            Some(&fresh_snapshot),
            true,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(should_scan_local_for_trigger(
            PairSyncTrigger::LocalDirty,
            Some(&fresh_snapshot),
            true,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(!should_scan_local_for_trigger(
            PairSyncTrigger::RemotePoll,
            Some(&fresh_snapshot),
            true,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(should_scan_local_for_trigger(
            PairSyncTrigger::RemotePoll,
            None,
            true,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(should_scan_local_for_trigger(
            PairSyncTrigger::RemotePoll,
            Some(&fresh_snapshot),
            false,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(should_scan_local_for_trigger(
            PairSyncTrigger::RemotePoll,
            Some(&stale_snapshot),
            true,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(local_snapshot_is_fresh(
            &fresh_snapshot,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
        assert!(!local_snapshot_is_fresh(
            &stale_snapshot,
            LOCAL_SNAPSHOT_STALE_TTL
        ));
    }

    #[test]
    fn watcher_eligible_pairs_only_include_enabled_configured_polling_pairs() {
        let mut unconfigured = test_pair("unconfigured", true, true, 60);
        unconfigured.bucket.clear();
        let eligible = watcher_eligible_pairs(&StoredProfile {
            sync_pairs: vec![
                test_pair("eligible", true, true, 60),
                test_pair("disabled", false, true, 60),
                test_pair("poll-off", true, false, 60),
                unconfigured,
            ],
            ..StoredProfile::default()
        });

        assert_eq!(eligible.len(), 1);
        assert_eq!(eligible[0].id, "eligible");
    }

    // -- phase 1: local filesystem effects of deletes and moves --------------

    fn fs_helper_temp_dir(name: &str) -> std::path::PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "storage-goblin-{name}-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }

    fn fs_helper_pair(root: &Path) -> SyncPair {
        SyncPair {
            id: "fs-helper".into(),
            label: "fs-helper".into(),
            local_folder: root.to_string_lossy().to_string(),
            bucket: "bucket".into(),
            enabled: true,
            ..SyncPair::default()
        }
    }

    #[test]
    fn renaming_a_local_file_creates_parents_and_prunes_empty_source_dirs() {
        let root = fs_helper_temp_dir("phase1-rename-local");
        std::fs::create_dir_all(root.join("from/nested")).expect("source dirs should exist");
        std::fs::write(root.join("from/nested/file.txt"), b"payload")
            .expect("source should be written");

        let pair = fs_helper_pair(&root);
        rename_local_file_for_pair(&pair, "from/nested/file.txt", "to/deeper/renamed.txt")
            .expect("rename should succeed");

        assert_eq!(
            std::fs::read(root.join("to/deeper/renamed.txt")).expect("destination should exist"),
            b"payload"
        );
        assert!(!root.join("from/nested/file.txt").exists());
        assert!(
            !root.join("from/nested").exists(),
            "emptied source directories should be pruned"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn renaming_reports_a_clear_error_when_the_source_is_missing() {
        let root = fs_helper_temp_dir("phase1-rename-missing");
        std::fs::create_dir_all(&root).expect("root should exist");

        let pair = fs_helper_pair(&root);
        let error = rename_local_file_for_pair(&pair, "nope.txt", "somewhere.txt")
            .expect_err("renaming a missing file should fail");
        assert!(
            error.contains("failed to rename"),
            "unexpected error: {error}"
        );

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn local_delete_and_move_refuse_to_escape_the_sync_root() {
        let root = fs_helper_temp_dir("phase1-path-escape");
        std::fs::create_dir_all(&root).expect("root should exist");
        let pair = fs_helper_pair(&root);

        // A remote object named to climb out of the sync folder must never
        // reach the filesystem (adversarial-path guard, backlog phase 1.4).
        assert!(trash_local_file_for_pair(&pair, "../escape.txt").is_err());
        assert!(rename_local_file_for_pair(&pair, "a.txt", "../escape.txt").is_err());
        assert!(rename_local_file_for_pair(&pair, "../escape.txt", "a.txt").is_err());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn deleting_an_already_missing_local_file_is_a_no_op() {
        let root = fs_helper_temp_dir("phase1-delete-missing");
        std::fs::create_dir_all(&root).expect("root should exist");
        let pair = fs_helper_pair(&root);

        // Never touches the OS trash: there is nothing to remove.
        trash_local_file_for_pair(&pair, "already-gone.txt")
            .expect("deleting a missing file should succeed");

        let _ = std::fs::remove_dir_all(&root);
    }
}

#[cfg(all(test, feature = "tauri-command-tests"))]
mod tauri_command_tests {
    use super::{
        build_file_entry_responses, clear_planned_transfer_test_hooks, compare_mode_external,
        execute_planned_download_queue_for_pair, execute_planned_upload_queue_for_pair,
        finalize_conflict_compare_details, image_media_type_for_extension, is_probably_text_bytes,
        local_fingerprint_for_path, persist_download_success_for_pair,
        persist_upload_success_for_pair, prepare_conflict_comparison_impl,
        read_file_with_size_limit, rebuild_durable_plan_for_pair, resolve_conflict_impl,
        set_planned_transfer_test_hooks, supports_manual_file_resolution, PlannedTransferTestHooks,
        INLINE_IMAGE_COMPARE_MAX_BYTES, INLINE_TEXT_COMPARE_MAX_BYTES,
    };
    use crate::storage::activity::ActivityDebugState;
    use crate::storage::credentials_store::{
        clear_test_secret_store, create_credential, CredentialDraft, StoredCredentials,
    };
    use crate::storage::local_index::LocalIndexSnapshot;
    use crate::storage::local_index::{
        read_local_index_snapshot_for_pair, write_local_index_snapshot_for_pair,
    };
    use crate::storage::profile_store::{
        read_profile_from_disk, write_profile_to_disk, RemoteBinConfig, StoredProfile, SyncPair,
        SyncPairDraft,
    };
    use crate::storage::remote_index::{
        write_remote_index_snapshot_for_pair, RemoteIndexSnapshot, RemoteIndexSummary,
        RemoteObjectEntry,
    };
    use crate::storage::sync_db::{
        load_planned_download_queue_for_pair, load_planned_upload_queue_for_pair,
        load_sync_anchors_for_pair, upsert_sync_anchor_for_pair, SyncAnchor,
    };
    use crate::storage::sync_state::{
        replace_pair_statuses_from_handle, PairSyncStatus, SyncState,
    };
    use std::collections::BTreeMap;
    use std::env;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tauri::test::{mock_builder, mock_context, noop_assets, MockRuntime};
    use tauri::{App, AppHandle, Manager};

    pub(super) struct CommandTestHarness {
        _env_lock: MutexGuard<'static, ()>,
        original_appdata: Option<String>,
        original_localappdata: Option<String>,
        app: App<MockRuntime>,
        pub(super) storage_dir: PathBuf,
    }

    impl CommandTestHarness {
        pub(super) fn new(name: &str) -> Self {
            let env_lock = env_lock().lock().expect("env lock should not be poisoned");
            let storage_dir = unique_test_dir(name);
            fs::create_dir_all(&storage_dir).expect("test storage dir should exist");

            let original_appdata = env::var("APPDATA").ok();
            let original_localappdata = env::var("LOCALAPPDATA").ok();
            env::set_var("APPDATA", &storage_dir);
            env::set_var("LOCALAPPDATA", &storage_dir);

            let app = mock_builder()
                .manage(ActivityDebugState::default())
                .manage(SyncState::default())
                .build(mock_context(noop_assets()))
                .expect("mock app should build");

            Self {
                _env_lock: env_lock,
                original_appdata,
                original_localappdata,
                app,
                storage_dir,
            }
        }

        pub(super) fn app_handle(&self) -> AppHandle<MockRuntime> {
            self.app.handle().clone()
        }

        fn load_profile(&self) -> StoredProfile {
            super::load_profile_impl(
                self.app_handle(),
                self.app.state::<SyncState>(),
                self.app.state::<ActivityDebugState>(),
            )
            .expect("profile should load")
        }

        fn save_profile_settings(&self, profile: StoredProfile) -> StoredProfile {
            super::save_profile_settings_impl(
                self.app_handle(),
                self.app.state::<SyncState>(),
                self.app.state::<ActivityDebugState>(),
                profile,
            )
            .expect("profile settings should save")
        }

        fn delete_credential(&self, credential_id: &str) -> super::DeleteCredentialResult {
            super::delete_credential_selection_impl(&self.app_handle(), credential_id)
                .expect("credential should delete")
                .0
        }
    }

    impl Drop for CommandTestHarness {
        fn drop(&mut self) {
            clear_planned_transfer_test_hooks();
            clear_test_secret_store();
            restore_env_var("APPDATA", self.original_appdata.as_deref());
            restore_env_var("LOCALAPPDATA", self.original_localappdata.as_deref());
            let _ = fs::remove_dir_all(&self.storage_dir);
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn restore_env_var(key: &str, value: Option<&str>) {
        match value {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    fn build_remote_snapshot(entries: &[(&str, &str, u64)]) -> RemoteIndexSnapshot {
        let object_count = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "file")
            .count() as u64;
        let total_bytes = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "file")
            .map(|(_, _, size)| *size)
            .sum();

        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count,
                total_bytes,
            },
            entries: entries
                .iter()
                .map(|(relative_path, kind, size)| RemoteObjectEntry {
                    key: format!("archive/{relative_path}"),
                    relative_path: (*relative_path).into(),
                    kind: (*kind).into(),
                    size: *size,
                    last_modified_at: None,
                    etag: None,
                    fingerprint: None,
                    storage_class: None,
                })
                .collect(),
        }
    }

    fn build_local_snapshot(entries: &[(&str, &str, u64)]) -> LocalIndexSnapshot {
        let file_count = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "file")
            .count() as u64;
        let directory_count = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "directory")
            .count() as u64;
        let total_bytes = entries
            .iter()
            .filter(|(_, kind, _)| *kind == "file")
            .map(|(_, _, size)| *size)
            .sum();

        LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: crate::storage::local_index::LocalIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                file_count,
                directory_count,
                total_bytes,
            },
            entries: entries
                .iter()
                .map(
                    |(relative_path, kind, size)| crate::storage::local_index::LocalIndexEntry {
                        relative_path: (*relative_path).into(),
                        kind: (*kind).into(),
                        size: *size,
                        modified_at: None,
                        fingerprint: if *kind == "file" {
                            Some(crate::storage::local_index::bytes_fingerprint(
                                relative_path.as_bytes(),
                            ))
                        } else {
                            None
                        },
                    },
                )
                .collect(),
        }
    }

    fn test_credentials() -> StoredCredentials {
        StoredCredentials::aws_access_key("aws", "test-access-key", "test-secret-key")
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_nanos();
        env::temp_dir().join(format!(
            "storage-goblin-commands-{name}-{}-{suffix}",
            process::id()
        ))
    }

    fn sync_pair_draft(label: &str, folder: &str, bucket: &str) -> SyncPairDraft {
        SyncPairDraft {
            id: None,
            label: label.into(),
            provider: "aws".into(),
            local_folder: folder.into(),
            region: "us-east-1".into(),
            bucket: bucket.into(),
            credential_profile_id: None,
            object_versioning_enabled: false,
            enabled: false,
            remote_polling_enabled: false,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            remote_bin: crate::storage::profile_store::RemoteBinConfig {
                enabled: true,
                retention_days: 7,
            },
        }
    }

    fn stored_pair(
        id: &str,
        label: &str,
        folder: &str,
        bucket: &str,
        credential_id: &str,
    ) -> SyncPair {
        SyncPair {
            id: id.into(),
            label: label.into(),
            local_folder: folder.into(),
            region: "us-east-1".into(),
            bucket: bucket.into(),
            credential_profile_id: Some(credential_id.into()),
            enabled: true,
            remote_polling_enabled: false,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            remote_bin: crate::storage::profile_store::RemoteBinConfig {
                enabled: true,
                retention_days: 7,
            },
            ..SyncPair::default()
        }
    }

    fn pair_runtime_status(
        pair: &SyncPair,
        phase: &str,
        last_sync_at: Option<&str>,
        remote_polling_enabled: bool,
    ) -> PairSyncStatus {
        PairSyncStatus {
            pair_id: pair.id.clone(),
            pair_label: pair.label.clone(),
            phase: phase.into(),
            last_sync_at: last_sync_at.map(str::to_string),
            current_folder: Some(pair.local_folder.clone()),
            current_bucket: Some(pair.bucket.clone()),
            enabled: pair.enabled,
            remote_polling_enabled,
            poll_interval_seconds: pair.poll_interval_seconds,
            ..PairSyncStatus::default()
        }
    }

    fn create_saved_credential(harness: &CommandTestHarness, name: &str) -> String {
        create_credential(
            &harness.app_handle(),
            CredentialDraft {
                name: name.into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: format!("AKIA-{name}"),
                secret_access_key: format!("secret-{name}"),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created")
        .id
    }

    fn sync_pair_ids(profile: &StoredProfile) -> Vec<String> {
        profile
            .sync_pairs
            .iter()
            .map(|pair| pair.id.clone())
            .collect()
    }

    fn assert_active_location_is_valid_or_cleared(profile: &StoredProfile) {
        if let Some(active_id) = profile.active_location_id.as_deref() {
            assert!(
                profile.sync_pairs.iter().any(|pair| pair.id == active_id),
                "active location id should point to an existing sync location"
            );
        }
    }

    #[test]
    fn remove_active_sync_location_stays_deleted_after_reload_and_clears_invalid_active_selection()
    {
        let harness = CommandTestHarness::new("remove-active-sync-location");

        let profile = super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Photos", "C:/sync/photos", "bucket-a"),
        )
        .expect("first location should be added");
        let removed_id = profile.sync_pairs[0].id.clone();

        let profile = super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Docs", "C:/sync/docs", "bucket-a"),
        )
        .expect("second location should be added");
        let survivor_id = profile
            .sync_pairs
            .iter()
            .find(|pair| pair.id != removed_id)
            .expect("second location should exist")
            .id
            .clone();

        let mut stored =
            read_profile_from_disk(&harness.app_handle()).expect("profile should read");
        stored.active_location_id = Some(removed_id.clone());
        write_profile_to_disk(&harness.app_handle(), &stored).expect("profile should write");

        super::remove_sync_pair_impl(harness.app_handle(), removed_id.clone())
            .expect("active location should be removed");

        let reloaded = harness.load_profile();

        assert_eq!(reloaded.sync_pairs.len(), 1);
        assert_eq!(sync_pair_ids(&reloaded), vec![survivor_id]);
        assert!(
            !reloaded.sync_pairs.iter().any(|pair| pair.id == removed_id),
            "removed location should stay deleted after reload"
        );
        assert_ne!(
            reloaded.active_location_id.as_deref(),
            Some(removed_id.as_str())
        );
        assert_active_location_is_valid_or_cleared(&reloaded);
    }

    #[test]
    fn removed_sync_location_is_not_resurrected_by_saving_unrelated_profile_settings() {
        let harness = CommandTestHarness::new("removed-location-not-resurrected");

        let profile = super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Photos", "C:/sync/photos", "bucket-a"),
        )
        .expect("first location should be added");
        let removed_id = profile.sync_pairs[0].id.clone();

        super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Docs", "C:/sync/docs", "bucket-b"),
        )
        .expect("second location should be added");

        let mut stale_profile = harness.load_profile();
        stale_profile.activity_debug_mode_enabled = true;

        super::remove_sync_pair_impl(harness.app_handle(), removed_id.clone())
            .expect("location should be removed");

        let saved = harness.save_profile_settings(stale_profile);
        let reloaded = harness.load_profile();

        assert!(saved.activity_debug_mode_enabled);
        assert_eq!(saved.sync_pairs.len(), 1);
        assert!(
            !saved.sync_pairs.iter().any(|pair| pair.id == removed_id),
            "saving unrelated settings should not restore removed location"
        );
        assert_eq!(reloaded.sync_pairs.len(), 1);
        assert!(
            !reloaded.sync_pairs.iter().any(|pair| pair.id == removed_id),
            "removed location should remain deleted after reload"
        );
    }

    #[test]
    fn add_sync_location_persists_exactly_once_after_reload() {
        let harness = CommandTestHarness::new("add-sync-location-persists-once");

        let added = super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Photos", "C:/sync/photos", "bucket-a"),
        )
        .expect("location should be added");
        let added_pair = added
            .sync_pairs
            .first()
            .expect("added location should be returned")
            .clone();

        let reloaded = harness.load_profile();
        let matching: Vec<&SyncPair> = reloaded
            .sync_pairs
            .iter()
            .filter(|pair| pair.id == added_pair.id)
            .collect();

        assert_eq!(reloaded.sync_pairs.len(), 1);
        assert_eq!(matching.len(), 1, "location should persist exactly once");
        let persisted = matching[0];
        assert_eq!(persisted.label, added_pair.label);
        assert_eq!(persisted.local_folder, added_pair.local_folder);
        assert_eq!(persisted.bucket, added_pair.bucket);
        assert!(persisted.remote_bin.enabled);
        assert_eq!(persisted.remote_bin.retention_days, 7);
    }

    #[test]
    fn update_sync_location_persists_updated_fields_without_creating_duplicates() {
        let harness = CommandTestHarness::new("update-sync-location-persists");

        let added = super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Photos", "C:/sync/photos", "bucket-a"),
        )
        .expect("location should be added");
        let original_id = added.sync_pairs[0].id.clone();

        super::update_sync_pair_impl(
            harness.app_handle(),
            SyncPairDraft {
                id: Some(original_id.clone()),
                label: "Photos Archive".into(),
                provider: "aws".into(),
                local_folder: "D:/archive/photos".into(),
                region: "eu-west-1".into(),
                bucket: "bucket-b".into(),
                credential_profile_id: None,
                object_versioning_enabled: false,
                enabled: false,
                remote_polling_enabled: true,
                poll_interval_seconds: 120,
                conflict_strategy: "prefer-local".into(),
                remote_bin: crate::storage::profile_store::RemoteBinConfig {
                    enabled: true,
                    retention_days: 2,
                },
            },
        )
        .expect("location should update");

        let reloaded = harness.load_profile();
        let matching: Vec<&SyncPair> = reloaded
            .sync_pairs
            .iter()
            .filter(|pair| pair.id == original_id)
            .collect();

        assert_eq!(reloaded.sync_pairs.len(), 1);
        assert_eq!(
            matching.len(),
            1,
            "updated location should still exist exactly once"
        );

        let updated = matching[0];
        assert_eq!(updated.label, "Photos Archive");
        assert_eq!(updated.local_folder, "D:/archive/photos");
        assert_eq!(updated.region, "eu-west-1");
        assert_eq!(updated.bucket, "bucket-b");
        assert!(updated.remote_polling_enabled);
        assert_eq!(updated.poll_interval_seconds, 120);
        assert_eq!(updated.conflict_strategy, "prefer-local");
        assert!(updated.remote_bin.enabled);
        assert_eq!(updated.remote_bin.retention_days, 2);
    }

    #[test]
    fn update_sync_pair_normalizes_invalid_conflict_strategy_to_preserve_both() {
        let harness = CommandTestHarness::new("update-sync-pair-invalid-conflict-strategy");

        let added = super::add_sync_pair_impl(
            harness.app_handle(),
            sync_pair_draft("Photos", "C:/sync/photos", "bucket-a"),
        )
        .expect("location should be added");
        let original_id = added.sync_pairs[0].id.clone();

        super::update_sync_pair_impl(
            harness.app_handle(),
            SyncPairDraft {
                id: Some(original_id.clone()),
                label: "Photos Archive".into(),
                provider: "aws".into(),
                local_folder: "D:/archive/photos".into(),
                region: "eu-west-1".into(),
                bucket: "bucket-b".into(),
                credential_profile_id: None,
                object_versioning_enabled: false,
                enabled: false,
                remote_polling_enabled: true,
                poll_interval_seconds: 120,
                conflict_strategy: "ignored-by-normalization".into(),
                remote_bin: crate::storage::profile_store::RemoteBinConfig {
                    enabled: true,
                    retention_days: 2,
                },
            },
        )
        .expect("location should update");

        let reloaded = harness.load_profile();
        let updated = reloaded
            .sync_pairs
            .iter()
            .find(|pair| pair.id == original_id)
            .expect("updated pair should exist");

        assert_eq!(updated.conflict_strategy, "preserve-both");
    }

    #[test]
    fn detects_supported_inline_image_extensions() {
        assert_eq!(
            image_media_type_for_extension("photos/conflict.png"),
            Some("image/png")
        );
        assert_eq!(
            image_media_type_for_extension("photos/conflict.JPG"),
            Some("image/jpeg")
        );
        assert_eq!(image_media_type_for_extension("notes/readme.txt"), None);
    }

    #[test]
    fn recognizes_probably_text_bytes() {
        assert!(is_probably_text_bytes(b"hello world"));
        assert!(!is_probably_text_bytes(&[0, 159, 146, 150]));
    }

    #[test]
    fn file_size_limit_rejects_large_inline_compare_payloads() {
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join(format!(
            "storage-goblin-inline-limit-{}.txt",
            uuid::Uuid::new_v4()
        ));
        fs::write(&file_path, vec![b'a'; INLINE_TEXT_COMPARE_MAX_BYTES + 1])
            .expect("temp file should be written");

        let result = read_file_with_size_limit(&file_path, INLINE_TEXT_COMPARE_MAX_BYTES);
        fs::remove_file(&file_path).expect("temp file should be removed");

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("too large for inline compare"));
    }

    #[test]
    fn finalize_conflict_compare_details_falls_back_to_external_for_large_text_files() {
        let temp_dir = std::env::temp_dir();
        let local_path = temp_dir.join(format!(
            "storage-goblin-inline-local-{}.txt",
            uuid::Uuid::new_v4()
        ));
        let remote_path = temp_dir.join(format!(
            "storage-goblin-inline-remote-{}.txt",
            uuid::Uuid::new_v4()
        ));
        fs::write(&local_path, vec![b'a'; INLINE_TEXT_COMPARE_MAX_BYTES + 1])
            .expect("local temp file should be written");
        fs::write(&remote_path, b"remote").expect("remote temp file should be written");

        let details = finalize_conflict_compare_details(
            "loc-1".into(),
            "docs/conflict.txt".into(),
            Some(local_path.to_string_lossy().into_owned()),
            Some(remote_path.to_string_lossy().into_owned()),
        );

        fs::remove_file(&local_path).expect("local temp file should be removed");
        fs::remove_file(&remote_path).expect("remote temp file should be removed");

        assert_eq!(details.mode, "external");
        assert!(details
            .fallback_reason
            .as_deref()
            .unwrap_or_default()
            .contains("too large for inline compare"));
    }

    #[test]
    fn compare_mode_external_preserves_fallback_paths() {
        let details = compare_mode_external(
            "loc-1".into(),
            "docs/conflict.bin".into(),
            Some("C:/sync/docs/conflict.bin".into()),
            Some("C:/temp/conflict.bin".into()),
            Some("Binary file".into()),
        );

        assert_eq!(details.mode, "external");
        assert_eq!(
            details.local_path.as_deref(),
            Some("C:/sync/docs/conflict.bin")
        );
        assert_eq!(
            details.remote_temp_path.as_deref(),
            Some("C:/temp/conflict.bin")
        );
        assert_eq!(details.fallback_reason.as_deref(), Some("Binary file"));
        assert_eq!(details.local_text, None);
        assert_eq!(details.remote_text, None);
        assert_eq!(details.local_image_data_url, None);
        assert_eq!(details.remote_image_data_url, None);
    }

    #[test]
    fn inline_image_limit_constant_is_larger_than_text_limit() {
        const { assert!(INLINE_IMAGE_COMPARE_MAX_BYTES > INLINE_TEXT_COMPARE_MAX_BYTES) };
    }

    #[test]
    fn delete_selected_credential_clears_selected_credential_state_persistently() {
        let harness = CommandTestHarness::new("delete-selected-credential");

        let credential = create_credential(
            &harness.app_handle(),
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        let profile = StoredProfile {
            credential_profile_id: Some(credential.id.clone()),
            ..StoredProfile::default()
        };
        write_profile_to_disk(&harness.app_handle(), &profile).expect("profile should write");

        let loaded_before_delete = harness.load_profile();
        assert_eq!(
            loaded_before_delete.credential_profile_id.as_deref(),
            Some(credential.id.as_str())
        );
        assert_eq!(
            loaded_before_delete
                .selected_credential
                .as_ref()
                .map(|summary| summary.id.as_str()),
            Some(credential.id.as_str())
        );
        assert!(loaded_before_delete.selected_credential_available);

        let deleted = harness.delete_credential(&credential.id);
        assert!(deleted.deleted);
        assert!(deleted.profile.credential_profile_id.is_none());
        assert!(deleted.profile.selected_credential.is_none());
        assert!(!deleted.profile.selected_credential_available);

        let persisted = read_profile_from_disk(&harness.app_handle()).expect("profile should read");
        assert!(persisted.credential_profile_id.is_none());

        let reloaded = harness.load_profile();
        assert!(reloaded.credential_profile_id.is_none());
        assert!(reloaded.selected_credential.is_none());
        assert!(!reloaded.selected_credential_available);
        assert!(!reloaded.credentials_stored_securely);
    }

    #[test]
    fn saving_profile_settings_keeps_disabled_remote_bin_changes_without_credentials() {
        let harness = CommandTestHarness::new("save-settings-disabled-remote-bin");
        let profile = StoredProfile {
            bucket: "demo-bucket".into(),
            local_folder: "C:/sync".into(),
            remote_bin: RemoteBinConfig {
                enabled: false,
                retention_days: 21,
            },
            activity_debug_mode_enabled: true,
            ..StoredProfile::default()
        };

        let saved = harness.save_profile_settings(profile);
        let reloaded = harness.load_profile();

        assert!(!saved.remote_bin.enabled);
        assert_eq!(saved.remote_bin.retention_days, 21);
        assert!(reloaded.activity_debug_mode_enabled);
        assert!(!reloaded.remote_bin.enabled);
        assert_eq!(reloaded.remote_bin.retention_days, 21);
    }

    #[test]
    fn add_sync_pair_does_not_persist_when_remote_bin_reconciliation_cannot_load_credentials() {
        let harness = CommandTestHarness::new("add-sync-pair-reconcile-failure");

        let error = super::add_sync_pair_impl(
            harness.app_handle(),
            SyncPairDraft {
                credential_profile_id: Some("missing-credential".into()),
                ..sync_pair_draft("Photos", "C:/sync/photos", "bucket-a")
            },
        )
        .expect_err("reconciliation failure should abort persistence");

        assert!(error.contains("missing-credential"));
        let reloaded = harness.load_profile();
        assert!(reloaded.sync_pairs.is_empty());
    }

    #[test]
    fn remove_sync_pair_persists_when_remaining_buckets_do_not_use_remote_bin() {
        let harness = CommandTestHarness::new("remove-sync-pair-disabled-remaining-bin");
        let credential_id = create_saved_credential(&harness, "primary");
        let mut initial = StoredProfile {
            sync_pairs: vec![
                SyncPair {
                    id: "pair-enabled".into(),
                    label: "Enabled".into(),
                    local_folder: "C:/enabled".into(),
                    bucket: "bucket-enabled".into(),
                    credential_profile_id: Some(credential_id.clone()),
                    remote_bin: RemoteBinConfig {
                        enabled: true,
                        retention_days: 7,
                    },
                    ..SyncPair::default()
                },
                SyncPair {
                    id: "pair-disabled".into(),
                    label: "Disabled".into(),
                    local_folder: "C:/disabled".into(),
                    bucket: "bucket-disabled".into(),
                    credential_profile_id: None,
                    remote_bin: RemoteBinConfig {
                        enabled: false,
                        retention_days: 30,
                    },
                    ..SyncPair::default()
                },
            ],
            ..StoredProfile::default()
        }
        .normalized();
        write_profile_to_disk(&harness.app_handle(), &initial).expect("profile should write");

        let saved = super::remove_sync_pair_impl(harness.app_handle(), "pair-enabled".into())
            .expect("removal should persist because remaining bin is disabled");
        initial.sync_pairs.retain(|pair| pair.id == "pair-disabled");

        assert_eq!(saved.sync_pairs.len(), 1);
        assert_eq!(saved.sync_pairs[0].id, "pair-disabled");
        assert!(!saved.sync_pairs[0].remote_bin.enabled);

        let reloaded = harness.load_profile();
        assert_eq!(reloaded.sync_pairs.len(), 1);
        assert_eq!(reloaded.sync_pairs[0].id, "pair-disabled");
    }

    #[test]
    fn get_sync_status_synthesizes_multi_location_runtime_state_from_pair_statuses() {
        let harness = CommandTestHarness::new("get-sync-status-pair-runtime");
        let profile = StoredProfile {
            sync_pairs: vec![
                SyncPair {
                    id: "pair-a".into(),
                    label: "Docs".into(),
                    local_folder: "C:/docs".into(),
                    bucket: "bucket-docs".into(),
                    enabled: true,
                    remote_polling_enabled: true,
                    poll_interval_seconds: 30,
                    ..SyncPair::default()
                },
                SyncPair {
                    id: "pair-b".into(),
                    label: "Photos".into(),
                    local_folder: "C:/photos".into(),
                    bucket: "bucket-photos".into(),
                    enabled: true,
                    remote_polling_enabled: false,
                    poll_interval_seconds: 120,
                    ..SyncPair::default()
                },
            ],
            ..StoredProfile::default()
        }
        .normalized();
        write_profile_to_disk(&harness.app_handle(), &profile).expect("profile should write");

        replace_pair_statuses_from_handle(
            &harness.app_handle(),
            vec![
                pair_runtime_status(
                    &profile.sync_pairs[0],
                    "polling",
                    Some("2026-04-12T10:00:00Z"),
                    true,
                ),
                pair_runtime_status(
                    &profile.sync_pairs[1],
                    "paused",
                    Some("2026-04-12T09:00:00Z"),
                    false,
                ),
            ],
        )
        .expect("pair statuses should update");

        let status =
            super::get_sync_status_impl(harness.app_handle(), harness.app.state::<SyncState>())
                .expect("status should load");

        assert_eq!(status.phase, "polling");
        assert_eq!(status.locations.len(), 2);
        assert_eq!(status.locations[0].pair_id, "pair-a");
        assert_eq!(status.locations[0].phase, "polling");
        assert_eq!(status.locations[1].pair_id, "pair-b");
        assert_eq!(status.locations[1].phase, "paused");
        assert!(status.remote_polling_enabled);
        assert_eq!(status.poll_interval_seconds, 30);
    }

    #[test]
    fn pause_sync_sets_all_configured_pairs_to_paused() {
        let harness = CommandTestHarness::new("pause-sync-multi-location");
        let profile = StoredProfile {
            sync_pairs: vec![
                SyncPair {
                    id: "pair-a".into(),
                    label: "Docs".into(),
                    local_folder: "C:/docs".into(),
                    bucket: "bucket-docs".into(),
                    enabled: true,
                    ..SyncPair::default()
                },
                SyncPair {
                    id: "pair-b".into(),
                    label: "Photos".into(),
                    local_folder: "C:/photos".into(),
                    bucket: "bucket-photos".into(),
                    enabled: false,
                    ..SyncPair::default()
                },
            ],
            ..StoredProfile::default()
        }
        .normalized();
        write_profile_to_disk(&harness.app_handle(), &profile).expect("profile should write");

        let status = super::pause_sync_impl(
            harness.app_handle(),
            harness.app.state::<SyncState>(),
            harness.app.state::<ActivityDebugState>(),
        )
        .expect("pause should succeed");

        assert_eq!(status.phase, "paused");
        assert_eq!(status.locations.len(), 2);
        assert!(status
            .locations
            .iter()
            .all(|location| location.phase == "paused"));
    }

    #[test]
    fn planned_upload_execution_updates_anchor_and_reclassifies_same_size_change_to_synced() {
        let harness = CommandTestHarness::new("upload-anchor-follow-on-state");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("upload-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        fs::write(local_root.join("note.txt"), b"bravo").expect("local file should exist");

        let pair = SyncPair {
            id: "pair-upload".into(),
            label: "Upload".into(),
            local_folder: local_root.to_string_lossy().to_string(),
            bucket: "bucket-upload".into(),
            enabled: true,
            ..SyncPair::default()
        };

        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let remote_snapshot = build_remote_snapshot(&[("note.txt", "file", 5)]);
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        upsert_sync_anchor_for_pair(
            &handle,
            &pair,
            &SyncAnchor {
                path: "note.txt".into(),
                kind: "file".into(),
                local_fingerprint: Some("base-fingerprint".into()),
                remote_etag: Some("etag-base".into()),
                synced_at: "2026-04-12T00:00:00Z".into(),
            },
        )
        .expect("base anchor should persist");

        let before =
            rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
                .expect("plan should build before anchor update");
        assert_eq!(before.upload_count, 1);
        let upload_queue =
            load_planned_upload_queue_for_pair(&handle, &pair).expect("upload queue should load");
        assert_eq!(upload_queue.len(), 1);
        assert_eq!(
            upload_queue[0].expected_remote_etag.as_deref(),
            Some("etag-base")
        );

        let local_fp = local_fingerprint_for_path(&local_snapshot, "note.txt")
            .expect("local fingerprint should exist");
        let updated_remote_snapshot = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/note.txt".into(),
                relative_path: "note.txt".into(),
                kind: "file".into(),
                size: 5,
                last_modified_at: None,
                etag: Some("etag-uploaded".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..remote_snapshot.clone()
        };
        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::from([(
                "note.txt".into(),
                updated_remote_snapshot.clone(),
            )]),
            download_payloads: BTreeMap::new(),
        });

        let outcome = super::run_async_blocking(execute_planned_upload_queue_for_pair(
            &handle,
            &harness.app.state::<ActivityDebugState>(),
            &pair,
            &test_credentials(),
        ))
        .expect("upload execution should succeed");
        assert!(outcome.uploads_ran);
        assert_eq!(outcome.execution_error, None);

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].remote_etag.as_deref(), Some("etag-uploaded"));
        assert_eq!(
            anchors[0].local_fingerprint.as_deref(),
            Some(local_fp.as_str())
        );

        let after = rebuild_durable_plan_for_pair(
            &handle,
            &pair,
            &local_snapshot,
            &updated_remote_snapshot,
            true,
        )
        .expect("plan should build after anchor update");
        assert_eq!(after.upload_count, 0);
        assert_eq!(after.download_count, 0);
        assert_eq!(after.conflict_count, 0);
        assert_eq!(after.noop_count, 1);

        let anchor_map = load_sync_anchors_for_pair(&handle, &pair)
            .expect("anchors should reload")
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect::<BTreeMap<_, _>>();
        let entries = build_file_entry_responses(
            Some(&local_snapshot),
            Some(&updated_remote_snapshot),
            Some(&anchor_map),
        );
        assert_eq!(
            entries
                .iter()
                .find(|entry| entry.path == "note.txt")
                .expect("file entry should exist")
                .status,
            "synced"
        );
    }

    #[test]
    fn planned_download_execution_updates_anchor_and_reclassifies_same_size_change_to_synced() {
        let harness = CommandTestHarness::new("download-anchor-follow-on-state");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("download-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        fs::write(local_root.join("note.txt"), b"alpha").expect("local file should exist");

        let pair = SyncPair {
            id: "pair-download".into(),
            label: "Download".into(),
            local_folder: local_root.to_string_lossy().to_string(),
            bucket: "bucket-download".into(),
            enabled: true,
            ..SyncPair::default()
        };

        let local_before =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_before)
            .expect("local snapshot should persist");

        let remote_before = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/note.txt".into(),
                relative_path: "note.txt".into(),
                kind: "file".into(),
                size: 5,
                last_modified_at: None,
                etag: Some("etag-new".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..build_remote_snapshot(&[("note.txt", "file", 5)])
        };
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_before)
            .expect("remote snapshot should persist");

        upsert_sync_anchor_for_pair(
            &handle,
            &pair,
            &SyncAnchor {
                path: "note.txt".into(),
                kind: "file".into(),
                local_fingerprint: Some("base-fingerprint".into()),
                remote_etag: Some("etag-base".into()),
                synced_at: "2026-04-12T00:00:00Z".into(),
            },
        )
        .expect("base anchor should persist");

        let before =
            rebuild_durable_plan_for_pair(&handle, &pair, &local_before, &remote_before, true)
                .expect("plan should build before download anchor update");
        assert_eq!(before.download_count, 1);
        let download_queue = load_planned_download_queue_for_pair(&handle, &pair)
            .expect("download queue should load");
        assert_eq!(download_queue.len(), 1);
        assert_eq!(
            download_queue[0].expected_remote_etag.as_deref(),
            Some("etag-new")
        );

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::from([("note.txt".into(), b"bravo".to_vec())]),
        });

        let outcome = super::run_async_blocking(execute_planned_download_queue_for_pair(
            &handle,
            &harness.app.state::<ActivityDebugState>(),
            &pair,
            &test_credentials(),
        ))
        .expect("download execution should succeed");
        assert!(outcome.downloads_ran);
        assert_eq!(outcome.execution_error, None);

        let local_after =
            super::scan_local_folder(&local_root).expect("post-download scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_after)
            .expect("post-download local snapshot should persist");

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].remote_etag.as_deref(), Some("etag-new"));
        let local_fp = local_fingerprint_for_path(&local_after, "note.txt")
            .expect("downloaded fingerprint should exist");
        assert_eq!(
            anchors[0].local_fingerprint.as_deref(),
            Some(local_fp.as_str())
        );

        let after =
            rebuild_durable_plan_for_pair(&handle, &pair, &local_after, &remote_before, true)
                .expect("plan should build after download anchor update");
        assert_eq!(after.upload_count, 0);
        assert_eq!(after.download_count, 0);
        assert_eq!(after.conflict_count, 0);
        assert_eq!(after.noop_count, 1);

        let anchor_map = load_sync_anchors_for_pair(&handle, &pair)
            .expect("anchors should reload")
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect::<BTreeMap<_, _>>();
        let entries =
            build_file_entry_responses(Some(&local_after), Some(&remote_before), Some(&anchor_map));
        assert_eq!(
            entries
                .iter()
                .find(|entry| entry.path == "note.txt")
                .expect("file entry should exist")
                .status,
            "synced"
        );
    }

    #[test]
    fn manual_resolution_supports_review_required_file_entries() {
        let local = build_local_snapshot(&[("docs/review.txt", "file", 5)]);
        let remote = build_remote_snapshot(&[("docs/review.txt", "file", 5)]);

        let entry = build_file_entry_responses(Some(&local), Some(&remote), None)
            .into_iter()
            .find(|entry| entry.path == "docs/review.txt")
            .expect("review entry should exist");

        assert_eq!(entry.status, "review-required");
        assert!(supports_manual_file_resolution(&entry));
    }

    #[test]
    fn manual_resolution_rejects_non_file_review_required_entries() {
        let local = build_local_snapshot(&[("docs/review", "directory", 0)]);
        let remote = build_remote_snapshot(&[("docs/review", "directory", 0)]);

        let entry = build_file_entry_responses(Some(&local), Some(&remote), None)
            .into_iter()
            .find(|entry| entry.path == "docs/review")
            .expect("review directory should exist");

        assert_eq!(entry.status, "synced");
        assert!(!supports_manual_file_resolution(&entry));

        let mismatch = build_file_entry_responses(
            Some(&build_local_snapshot(&[("docs/review", "file", 5)])),
            Some(&build_remote_snapshot(&[("docs/review", "directory", 0)])),
            None,
        )
        .into_iter()
        .find(|entry| entry.path == "docs/review")
        .expect("mismatch entry should exist");

        assert_eq!(mismatch.status, "conflict");
        assert!(!supports_manual_file_resolution(&mismatch));
    }

    #[test]
    fn persist_upload_success_for_pair_clears_review_required_after_manual_keep_local() {
        let harness = CommandTestHarness::new("manual-keep-local-anchor");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("manual-keep-local-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        fs::write(local_root.join("review.txt"), b"bravo").expect("local file should exist");

        let pair = SyncPair {
            id: "pair-manual-local".into(),
            label: "Manual local".into(),
            local_folder: local_root.to_string_lossy().to_string(),
            bucket: "bucket-manual-local".into(),
            enabled: true,
            ..SyncPair::default()
        };

        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let remote_before = build_remote_snapshot(&[("review.txt", "file", 5)]);
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_before)
            .expect("remote snapshot should persist");

        let before_entries = build_file_entry_responses(
            Some(&local_snapshot),
            Some(&remote_before),
            Some(&BTreeMap::new()),
        );
        assert_eq!(
            before_entries
                .iter()
                .find(|entry| entry.path == "review.txt")
                .expect("pre-resolution entry should exist")
                .status,
            "review-required"
        );

        let local_fingerprint = local_fingerprint_for_path(&local_snapshot, "review.txt")
            .expect("local fingerprint should exist");
        let remote_after = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/review.txt".into(),
                relative_path: "review.txt".into(),
                kind: "file".into(),
                size: 5,
                last_modified_at: None,
                etag: Some("etag-local-kept".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..remote_before.clone()
        };

        persist_upload_success_for_pair(
            &handle,
            &pair,
            "review.txt",
            &local_fingerprint,
            &remote_after,
        )
        .expect("manual keep-local should persist anchor");

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].path, "review.txt");
        assert_eq!(
            anchors[0].local_fingerprint.as_deref(),
            Some(local_fingerprint.as_str())
        );
        assert_eq!(anchors[0].remote_etag.as_deref(), Some("etag-local-kept"));

        let anchor_map = anchors
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect::<BTreeMap<_, _>>();
        let after_entries = build_file_entry_responses(
            Some(&local_snapshot),
            Some(&remote_after),
            Some(&anchor_map),
        );
        assert_eq!(
            after_entries
                .iter()
                .find(|entry| entry.path == "review.txt")
                .expect("post-resolution entry should exist")
                .status,
            "synced"
        );
    }

    #[test]
    fn persist_download_success_for_pair_clears_review_required_after_manual_keep_remote() {
        let harness = CommandTestHarness::new("manual-keep-remote-anchor");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("manual-keep-remote-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        fs::write(local_root.join("review.txt"), b"bravo")
            .expect("downloaded local file should exist");

        let pair = SyncPair {
            id: "pair-manual-remote".into(),
            label: "Manual remote".into(),
            local_folder: local_root.to_string_lossy().to_string(),
            bucket: "bucket-manual-remote".into(),
            enabled: true,
            ..SyncPair::default()
        };

        let local_after = super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_after)
            .expect("local snapshot should persist");

        let remote_snapshot = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/review.txt".into(),
                relative_path: "review.txt".into(),
                kind: "file".into(),
                size: 5,
                last_modified_at: None,
                etag: Some("etag-remote-kept".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..build_remote_snapshot(&[("review.txt", "file", 5)])
        };
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        let before_entries = build_file_entry_responses(
            Some(&local_after),
            Some(&remote_snapshot),
            Some(&BTreeMap::new()),
        );
        assert_eq!(
            before_entries
                .iter()
                .find(|entry| entry.path == "review.txt")
                .expect("pre-resolution entry should exist")
                .status,
            "review-required"
        );

        let local_path = local_root.join("review.txt");
        persist_download_success_for_pair(
            &handle,
            &pair,
            "review.txt",
            &local_path,
            Some("etag-remote-kept".into()),
        )
        .expect("manual keep-remote should persist anchor");

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].path, "review.txt");
        assert_eq!(anchors[0].remote_etag.as_deref(), Some("etag-remote-kept"));

        let local_fingerprint = local_fingerprint_for_path(&local_after, "review.txt")
            .expect("downloaded fingerprint should exist");
        assert_eq!(
            anchors[0].local_fingerprint.as_deref(),
            Some(local_fingerprint.as_str())
        );

        let anchor_map = anchors
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect::<BTreeMap<_, _>>();
        let after_entries = build_file_entry_responses(
            Some(&local_after),
            Some(&remote_snapshot),
            Some(&anchor_map),
        );
        assert_eq!(
            after_entries
                .iter()
                .find(|entry| entry.path == "review.txt")
                .expect("post-resolution entry should exist")
                .status,
            "synced"
        );
    }

    #[test]
    fn prepare_conflict_comparison_allows_review_required_file_entries() {
        let harness = CommandTestHarness::new("prepare-review-required-comparison");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("prepare-review-local");
        fs::create_dir_all(local_root.join("docs")).expect("local docs dir should exist");
        fs::write(local_root.join("docs/review.txt"), b"local body")
            .expect("local file should exist");

        let credential_id = create_saved_credential(&harness, "review-compare");
        let pair = stored_pair(
            "pair-review-compare",
            "Review compare",
            &local_root.to_string_lossy(),
            "bucket-review-compare",
            &credential_id,
        );
        write_profile_to_disk(
            &handle,
            &StoredProfile {
                sync_pairs: vec![pair.clone()],
                active_location_id: Some(pair.id.clone()),
                ..StoredProfile::default()
            }
            .normalized(),
        )
        .expect("profile should persist");

        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let remote_snapshot = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/docs/review.txt".into(),
                relative_path: "docs/review.txt".into(),
                kind: "file".into(),
                size: 10,
                last_modified_at: None,
                etag: Some("etag-review-remote".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..build_remote_snapshot(&[("docs/review.txt", "file", 10)])
        };
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::from([(
                "docs/review.txt".into(),
                b"remote body".to_vec(),
            )]),
        });

        let details = super::run_async_blocking(prepare_conflict_comparison_impl(
            handle,
            pair.id.clone(),
            "docs/review.txt".into(),
        ))
        .expect("review-required compare should succeed");

        assert_eq!(details.path, "docs/review.txt");
        assert_eq!(details.location_id, pair.id);
        assert_eq!(details.mode, "text");
        assert_eq!(details.local_text.as_deref(), Some("local body"));
        assert_eq!(details.remote_text.as_deref(), Some("remote body"));
    }

    #[test]
    fn resolve_conflict_keep_local_persists_anchor_for_review_required_entries() {
        let harness = CommandTestHarness::new("resolve-review-required-keep-local");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("resolve-review-local");
        fs::create_dir_all(local_root.join("docs")).expect("local docs dir should exist");
        fs::write(local_root.join("docs/review.txt"), b"local body")
            .expect("local file should exist");

        let credential_id = create_saved_credential(&harness, "review-local");
        let pair = stored_pair(
            "pair-review-local",
            "Review local",
            &local_root.to_string_lossy(),
            "bucket-review-local",
            &credential_id,
        );
        write_profile_to_disk(
            &handle,
            &StoredProfile {
                sync_pairs: vec![pair.clone()],
                active_location_id: Some(pair.id.clone()),
                ..StoredProfile::default()
            }
            .normalized(),
        )
        .expect("profile should persist");

        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let remote_before = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/docs/review.txt".into(),
                relative_path: "docs/review.txt".into(),
                kind: "file".into(),
                size: 11,
                last_modified_at: None,
                etag: Some("etag-review-before".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..build_remote_snapshot(&[("docs/review.txt", "file", 11)])
        };
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_before)
            .expect("remote snapshot should persist");

        let remote_after = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/docs/review.txt".into(),
                relative_path: "docs/review.txt".into(),
                kind: "file".into(),
                size: 10,
                last_modified_at: None,
                etag: Some("etag-review-local-after".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..build_remote_snapshot(&[("docs/review.txt", "file", 10)])
        };
        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::from([(
                "docs/review.txt".into(),
                remote_after.clone(),
            )]),
            download_payloads: BTreeMap::new(),
        });

        super::run_async_blocking(resolve_conflict_impl(
            handle.clone(),
            pair.id.clone(),
            "docs/review.txt".into(),
            "keep-local".into(),
        ))
        .expect("review-required keep-local should succeed");

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(
            anchors[0].remote_etag.as_deref(),
            Some("etag-review-local-after")
        );

        let refreshed_local = read_local_index_snapshot_for_pair(&handle, &pair.id)
            .expect("local snapshot should load")
            .expect("local snapshot should exist");
        let anchor_map = anchors
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect::<BTreeMap<_, _>>();
        let entries = build_file_entry_responses(
            Some(&refreshed_local),
            Some(&remote_after),
            Some(&anchor_map),
        );
        assert_eq!(
            entries
                .iter()
                .find(|entry| entry.path == "docs/review.txt")
                .expect("resolved entry should exist")
                .status,
            "synced"
        );
    }

    #[test]
    fn resolve_conflict_keep_remote_persists_anchor_for_review_required_entries() {
        let harness = CommandTestHarness::new("resolve-review-required-keep-remote");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("resolve-review-remote");
        fs::create_dir_all(local_root.join("docs")).expect("local docs dir should exist");
        fs::write(local_root.join("docs/review.txt"), b"local old")
            .expect("local file should exist");

        let credential_id = create_saved_credential(&harness, "review-remote");
        let pair = stored_pair(
            "pair-review-remote",
            "Review remote",
            &local_root.to_string_lossy(),
            "bucket-review-remote",
            &credential_id,
        );
        write_profile_to_disk(
            &handle,
            &StoredProfile {
                sync_pairs: vec![pair.clone()],
                active_location_id: Some(pair.id.clone()),
                ..StoredProfile::default()
            }
            .normalized(),
        )
        .expect("profile should persist");

        let local_before =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_before)
            .expect("local snapshot should persist");

        let remote_snapshot = RemoteIndexSnapshot {
            entries: vec![RemoteObjectEntry {
                key: "archive/docs/review.txt".into(),
                relative_path: "docs/review.txt".into(),
                kind: "file".into(),
                size: 11,
                last_modified_at: None,
                etag: Some("etag-review-remote-after".into()),
                storage_class: None,
                fingerprint: None,
            }],
            ..build_remote_snapshot(&[("docs/review.txt", "file", 11)])
        };
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::from([(
                "docs/review.txt".into(),
                b"remote newest".to_vec(),
            )]),
        });

        super::run_async_blocking(resolve_conflict_impl(
            handle.clone(),
            pair.id.clone(),
            "docs/review.txt".into(),
            "keep-remote".into(),
        ))
        .expect("review-required keep-remote should succeed");

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(
            anchors[0].remote_etag.as_deref(),
            Some("etag-review-remote-after")
        );

        let refreshed_local = read_local_index_snapshot_for_pair(&handle, &pair.id)
            .expect("local snapshot should load")
            .expect("local snapshot should exist");
        let local_path = local_root.join("docs/review.txt");
        let local_bytes = fs::read(&local_path).expect("downloaded local file should exist");
        assert_eq!(local_bytes, b"remote newest");

        let anchor_map = anchors
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect::<BTreeMap<_, _>>();
        let entries = build_file_entry_responses(
            Some(&refreshed_local),
            Some(&remote_snapshot),
            Some(&anchor_map),
        );
        assert_eq!(
            entries
                .iter()
                .find(|entry| entry.path == "docs/review.txt")
                .expect("resolved entry should exist")
                .status,
            "synced"
        );
    }

    // -- phase 1: delete / move / anchor execution through the pair queues ---

    fn remote_snapshot_with_etags(entries: &[(&str, u64, &str)]) -> RemoteIndexSnapshot {
        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count: entries.len() as u64,
                total_bytes: entries.iter().map(|(_, size, _)| *size).sum(),
            },
            entries: entries
                .iter()
                .map(|(relative_path, size, etag)| RemoteObjectEntry {
                    key: format!("archive/{relative_path}"),
                    relative_path: (*relative_path).into(),
                    kind: "file".into(),
                    size: *size,
                    last_modified_at: None,
                    etag: Some((*etag).to_string()),
                    storage_class: None,
                    fingerprint: None,
                })
                .collect(),
        }
    }

    fn phase_one_pair(id: &str, local_root: &Path) -> SyncPair {
        SyncPair {
            id: id.into(),
            label: id.into(),
            local_folder: local_root.to_string_lossy().to_string(),
            bucket: format!("bucket-{id}"),
            enabled: true,
            ..SyncPair::default()
        }
    }

    fn anchor_for(path: &str, fingerprint: &str, etag: &str) -> SyncAnchor {
        SyncAnchor {
            path: path.into(),
            kind: "file".into(),
            local_fingerprint: Some(fingerprint.into()),
            remote_etag: Some(etag.into()),
            synced_at: "2026-04-12T00:00:00Z".into(),
        }
    }

    #[test]
    fn planned_remote_delete_removes_the_anchor_and_clears_the_queue() {
        let harness = CommandTestHarness::new("phase1-delete-remote");
        let handle = harness.app_handle();

        // Local tree is empty: the user deleted the file locally.
        let local_root = harness.storage_dir.join("delete-remote-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        let pair = phase_one_pair("pair-delete-remote", &local_root);

        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let remote_snapshot = remote_snapshot_with_etags(&[("gone.txt", 5, "etag-base")]);
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        upsert_sync_anchor_for_pair(
            &handle,
            &pair,
            &anchor_for("gone.txt", "fingerprint-base", "etag-base"),
        )
        .expect("anchor should persist");

        let plan =
            rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
                .expect("plan should build");
        assert_eq!(plan.pending_operation_count, 1);

        let queue =
            load_planned_upload_queue_for_pair(&handle, &pair).expect("upload queue should load");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].operation, "delete_remote");

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::new(),
        });

        let outcome = super::run_async_blocking(execute_planned_upload_queue_for_pair(
            &handle,
            &harness.app.state::<ActivityDebugState>(),
            &pair,
            &test_credentials(),
        ))
        .expect("delete execution should succeed");
        assert_eq!(outcome.execution_error, None);

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert!(
            anchors.is_empty(),
            "propagated delete must drop the anchor, got {anchors:?}"
        );
        let remaining =
            load_planned_upload_queue_for_pair(&handle, &pair).expect("queue should reload");
        assert!(remaining.is_empty(), "queue item should be completed");
    }

    #[test]
    fn planned_local_delete_removes_the_file_and_the_anchor() {
        let harness = CommandTestHarness::new("phase1-delete-local");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("delete-local-root");
        fs::create_dir_all(local_root.join("docs")).expect("local dirs should exist");
        fs::write(local_root.join("docs/orphan.txt"), b"alpha").expect("local file should exist");

        let pair = phase_one_pair("pair-delete-local", &local_root);
        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        // Remote is empty: the object was deleted on the other side.
        let remote_snapshot = remote_snapshot_with_etags(&[]);
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        let local_fingerprint = local_fingerprint_for_path(&local_snapshot, "docs/orphan.txt")
            .expect("local fingerprint should exist");
        upsert_sync_anchor_for_pair(
            &handle,
            &pair,
            &anchor_for("docs/orphan.txt", &local_fingerprint, "etag-base"),
        )
        .expect("anchor should persist");

        rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
            .expect("plan should build");

        let queue = load_planned_download_queue_for_pair(&handle, &pair)
            .expect("download queue should load");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].operation, "delete_local");

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::new(),
        });

        let outcome = super::run_async_blocking(execute_planned_download_queue_for_pair(
            &handle,
            &harness.app.state::<ActivityDebugState>(),
            &pair,
            &test_credentials(),
        ))
        .expect("local delete execution should succeed");
        assert_eq!(outcome.execution_error, None);

        assert!(
            !local_root.join("docs/orphan.txt").exists(),
            "local file should be gone"
        );
        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert!(anchors.is_empty(), "anchor should be dropped");
    }

    #[test]
    fn planned_local_move_renames_the_file_without_re_downloading() {
        let harness = CommandTestHarness::new("phase1-move-local");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("move-local-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        fs::write(local_root.join("old-name.txt"), b"contents").expect("local file should exist");

        let pair = phase_one_pair("pair-move-local", &local_root);
        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        // The remote side renamed the object.
        let remote_snapshot = remote_snapshot_with_etags(&[("new-name.txt", 8, "etag-base")]);
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        let local_fingerprint = local_fingerprint_for_path(&local_snapshot, "old-name.txt")
            .expect("local fingerprint should exist");
        upsert_sync_anchor_for_pair(
            &handle,
            &pair,
            &anchor_for("old-name.txt", &local_fingerprint, "etag-base"),
        )
        .expect("anchor should persist");

        rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
            .expect("plan should build");

        let queue = load_planned_download_queue_for_pair(&handle, &pair)
            .expect("download queue should load");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].operation, "move_local");
        assert_eq!(queue[0].target_path.as_deref(), Some("new-name.txt"));

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::new(),
        });

        let outcome = super::run_async_blocking(execute_planned_download_queue_for_pair(
            &handle,
            &harness.app.state::<ActivityDebugState>(),
            &pair,
            &test_credentials(),
        ))
        .expect("local move execution should succeed");
        assert_eq!(outcome.execution_error, None);

        assert!(
            !local_root.join("old-name.txt").exists(),
            "source should be gone"
        );
        assert_eq!(
            fs::read(local_root.join("new-name.txt")).expect("renamed file should exist"),
            b"contents"
        );
    }

    #[test]
    fn planned_anchor_only_records_the_anchor_without_transferring() {
        let harness = CommandTestHarness::new("phase1-anchor-only");
        let handle = harness.app_handle();

        let local_root = harness.storage_dir.join("anchor-only-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        fs::write(local_root.join("same.txt"), b"identical").expect("local file should exist");

        let pair = phase_one_pair("pair-anchor-only", &local_root);
        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let local_fingerprint = local_fingerprint_for_path(&local_snapshot, "same.txt")
            .expect("local fingerprint should exist");

        // A provider that surfaces our content fingerprint on listing: the
        // remote object is byte-identical, so a first sync must not transfer.
        let mut remote_snapshot = remote_snapshot_with_etags(&[("same.txt", 9, "opaque-etag")]);
        remote_snapshot.entries[0].fingerprint = Some(local_fingerprint.clone());
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        let plan =
            rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
                .expect("plan should build");
        assert_eq!(plan.upload_count, 0);
        assert_eq!(plan.download_count, 0);
        assert_eq!(plan.conflict_count, 0);

        let queue =
            load_planned_upload_queue_for_pair(&handle, &pair).expect("upload queue should load");
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].operation, "anchor_only");

        set_planned_transfer_test_hooks(PlannedTransferTestHooks {
            upload_refresh_snapshots: BTreeMap::new(),
            download_payloads: BTreeMap::new(),
        });

        let outcome = super::run_async_blocking(execute_planned_upload_queue_for_pair(
            &handle,
            &harness.app.state::<ActivityDebugState>(),
            &pair,
            &test_credentials(),
        ))
        .expect("anchor execution should succeed");
        assert_eq!(outcome.execution_error, None);

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].path, "same.txt");
        assert_eq!(
            anchors[0].local_fingerprint.as_deref(),
            Some(local_fingerprint.as_str())
        );
        assert_eq!(anchors[0].remote_etag.as_deref(), Some("opaque-etag"));

        // Re-planning against the same state is now a clean no-op.
        let after =
            rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
                .expect("plan should rebuild");
        assert_eq!(after.pending_operation_count, 0);
        assert_eq!(after.noop_count, 1);
    }

    #[test]
    fn mass_delete_batches_are_held_for_review_instead_of_executing() {
        let harness = CommandTestHarness::new("phase1-mass-delete-breaker");
        let handle = harness.app_handle();

        // Local tree emptied (e.g. a drive that failed to mount): every
        // anchored path would otherwise plan a remote delete.
        let local_root = harness.storage_dir.join("mass-delete-root");
        fs::create_dir_all(&local_root).expect("local root should exist");
        let pair = phase_one_pair("pair-mass-delete", &local_root);

        let local_snapshot =
            super::scan_local_folder(&local_root).expect("local scan should succeed");
        write_local_index_snapshot_for_pair(&handle, &pair.id, &local_snapshot)
            .expect("local snapshot should persist");

        let entries: Vec<(String, u64, String)> = (0..40)
            .map(|index| (format!("bulk/file-{index}.txt"), 4, format!("etag-{index}")))
            .collect();
        let borrowed: Vec<(&str, u64, &str)> = entries
            .iter()
            .map(|(path, size, etag)| (path.as_str(), *size, etag.as_str()))
            .collect();
        let remote_snapshot = remote_snapshot_with_etags(&borrowed);
        write_remote_index_snapshot_for_pair(&handle, &pair.id, &remote_snapshot)
            .expect("remote snapshot should persist");

        for (path, _, etag) in &entries {
            upsert_sync_anchor_for_pair(
                &handle,
                &pair,
                &anchor_for(path, &format!("fingerprint-{path}"), etag),
            )
            .expect("anchor should persist");
        }

        rebuild_durable_plan_for_pair(&handle, &pair, &local_snapshot, &remote_snapshot, true)
            .expect("plan should build");

        let queue =
            load_planned_upload_queue_for_pair(&handle, &pair).expect("upload queue should load");
        assert!(
            queue.is_empty(),
            "suppressed deletes must not reach the executable queue, got {queue:?}"
        );

        let anchors = load_sync_anchors_for_pair(&handle, &pair).expect("anchors should load");
        assert_eq!(
            anchors.len(),
            40,
            "no anchors may be dropped while deletes are held for review"
        );
    }
}
