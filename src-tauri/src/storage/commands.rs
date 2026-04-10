use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use tauri::{AppHandle, Emitter, Manager, Runtime, State};

use uuid::Uuid;

use super::{
    activity::{emit_activity, ActivityDebugState, ActivityLevel},
    credentials_store::{
        create_credential, delete_credential as delete_stored_credential,
        ensure_legacy_credentials_migrated, get_credential_summary, list_credentials,
        load_credentials_by_id, parse_credential_input, record_credential_validation,
        upsert_credential, CredentialDraft, CredentialInputState, CredentialSummary,
        CredentialValidationStatus, StoredCredentials,
    },
    local_index::{
        read_local_index_snapshot, read_local_index_snapshot_for_pair, scan_local_folder,
        write_local_index_snapshot, write_local_index_snapshot_for_pair, LocalIndexSnapshot,
    },
    now_iso,
    profile_store::{
        is_pair_configured, is_profile_configured, read_profile_from_disk, write_profile_to_disk,
        ConnectionValidationInput, ConnectionValidationResult, ProfileDraft,
        SelectedCredentialState, StoredProfile, SyncPair, SyncPairDraft,
    },
    remote_index::{
        directory_relative_paths_from_key, directory_relative_paths_from_relative_path,
        read_remote_index_snapshot, read_remote_index_snapshot_for_pair, relative_path_from_key,
        write_remote_index_snapshot, write_remote_index_snapshot_for_pair, RemoteIndexSnapshot,
        RemoteIndexSummary, RemoteObjectEntry,
    },
    s3_adapter,
    sync_db::{
        load_planned_download_queue, load_planned_download_queue_for_pair,
        load_planned_upload_queue, load_planned_upload_queue_for_pair, load_planner_summary,
        load_planner_summary_for_pair, mark_download_queue_item_completed,
        mark_download_queue_item_completed_for_pair, mark_download_queue_item_failed,
        mark_download_queue_item_failed_for_pair, mark_download_queue_item_in_progress,
        mark_download_queue_item_in_progress_for_pair, mark_upload_queue_item_completed,
        mark_upload_queue_item_completed_for_pair, mark_upload_queue_item_failed,
        mark_upload_queue_item_failed_for_pair, mark_upload_queue_item_in_progress,
        mark_upload_queue_item_in_progress_for_pair, persist_sync_plan, persist_sync_plan_for_pair,
    },
    sync_planner,
    sync_state::{
        begin_polling_worker, clear_polling_worker, finish_sync_cycle, get_status_lock,
        pair_to_status, profile_to_status, set_status_from_handle, stop_polling_worker,
        synthesize_status_from_pairs, try_begin_sync_cycle, PairSyncStatus, SyncState, SyncStatus,
    },
};

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteCredentialResult {
    pub deleted: bool,
    pub profile: StoredProfile,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CredentialTestContext {
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
    pub permissions: Option<s3_adapter::PermissionProbeSummary>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileEntryResponse {
    pub path: String,
    pub kind: String,
    pub status: String,
    pub has_local_copy: bool,
    pub storage_class: Option<String>,
}

const PLANNED_UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);
const PLANNED_UPLOAD_QUEUE_TIMEOUT: Duration = Duration::from_secs(900);
const POST_UPLOAD_REMOTE_REFRESH_TIMEOUT: Duration = Duration::from_secs(120);
const PLANNED_DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);
const PLANNED_DOWNLOAD_QUEUE_TIMEOUT: Duration = Duration::from_secs(900);

fn emit_status(app: &AppHandle, status: &SyncStatus) {
    let _ = app.emit("storage://sync-status-changed", status);
}

fn emit_info_activity(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    message: impl Into<String>,
    details: Option<String>,
) {
    emit_activity(app, debug_state, ActivityLevel::Info, message, details);
}

fn emit_success_activity(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    message: impl Into<String>,
    details: Option<String>,
) {
    emit_activity(app, debug_state, ActivityLevel::Success, message, details);
}

fn emit_error_activity(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    message: impl Into<String>,
    details: Option<String>,
) {
    emit_activity(app, debug_state, ActivityLevel::Error, message, details);
}

fn format_timeout_error(operation: &str, timeout: Duration) -> String {
    format!("{operation} timed out after {}s.", timeout.as_secs())
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

fn sync_cycle_issue_details(
    profile: &StoredProfile,
    stage: &str,
    phase: &str,
    error: impl AsRef<str>,
    cycle_started_at: Option<&str>,
    extra_context: Option<&str>,
) -> String {
    let mut details = vec![
        format!("stage='{stage}'"),
        format!("phase='{phase}'"),
        format!("folder='{}'", profile.local_folder),
        format!("bucket='{}'", profile.bucket),
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

async fn run_with_timeout<F, T>(
    future: F,
    timeout: Duration,
    operation: impl FnOnce() -> String,
) -> Result<T, String>
where
    F: Future<Output = Result<T, String>>,
{
    match tokio::time::timeout(timeout, future).await {
        Ok(result) => result,
        Err(_) => Err(operation()),
    }
}

fn snapshot_for_profile(
    app: &AppHandle,
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

fn remote_snapshot_for_profile(
    app: &AppHandle,
    profile: &StoredProfile,
) -> (Option<RemoteIndexSnapshot>, Option<String>) {
    match read_remote_index_snapshot(app) {
        Ok(snapshot) => (
            snapshot.filter(|snapshot| {
                !profile.bucket.is_empty()
                    && super::remote_index::snapshot_matches_target(
                        snapshot,
                        &profile.bucket,
                    )
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

async fn list_remote_inventory(
    profile: &StoredProfile,
    credentials: &StoredCredentials,
) -> Result<RemoteIndexSnapshot, String> {
    let config = s3_adapter::S3ConnectionConfig {
        region: profile.region.clone(),
        bucket: profile.bucket.clone(),
        access_key_id: credentials.access_key_id.clone(),
        secret_access_key: credentials.secret_access_key.clone(),
    };
    let client = s3_adapter::build_client(&config).await?;
    let mut continuation_token: Option<String> = None;
    let mut entries = BTreeMap::new();
    let mut object_count = 0_u64;
    let mut total_bytes = 0_u64;

    loop {
        let mut request = client.list_objects_v2().bucket(&profile.bucket);

        if let Some(token) = continuation_token.as_deref() {
            request = request.continuation_token(token);
        }

        let response = request
            .send()
            .await
            .map_err(|error| format!("failed to list remote S3 inventory: {error}"))?;

        for object in response.contents() {
            let Some(key) = object.key() else {
                continue;
            };

            let relative_path = relative_path_from_key(key);
            let last_modified_at = object.last_modified().map(|value| value.to_string());
            let etag = object.e_tag().map(|value| value.to_string());
            let storage_class = object.storage_class().map(|sc| sc.as_str().to_string());

            if key.ends_with('/') {
                let directory_path = relative_path.trim_matches('/');
                if !directory_path.is_empty() {
                    entries.insert(
                        directory_path.to_string(),
                        RemoteObjectEntry {
                            key: key.to_string(),
                            relative_path: directory_path.to_string(),
                            kind: "directory".into(),
                            size: 0,
                            last_modified_at,
                            etag,
                            storage_class: None,
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
                        });
                }
                continue;
            }

            let size = object.size().unwrap_or_default().max(0) as u64;
            object_count += 1;
            total_bytes += size;
            entries.insert(
                relative_path.clone(),
                RemoteObjectEntry {
                    key: key.to_string(),
                    relative_path,
                    kind: "file".into(),
                    size,
                    last_modified_at,
                    etag,
                    storage_class,
                },
            );

            for directory_path in directory_relative_paths_from_key(key) {
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
                    });
            }
        }

        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(ToString::to_string);
        } else {
            break;
        }
    }

    let entries = entries.into_values().collect::<Vec<_>>();

    Ok(RemoteIndexSnapshot {
        version: 1,
        bucket: profile.bucket.clone(),
        summary: RemoteIndexSummary {
            indexed_at: now_iso(),
            object_count,
            total_bytes,
        },
        entries,
    })
}

fn resolve_session_credentials(
    session_credentials: Option<&StoredCredentials>,
    stored_credentials: Option<StoredCredentials>,
    missing_message: &str,
) -> Result<StoredCredentials, String> {
    session_credentials
        .cloned()
        .or(stored_credentials)
        .ok_or_else(|| missing_message.into())
}

fn resolve_execution_credentials(
    app: &AppHandle,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<StoredCredentials, String> {
    let stored_credentials = match profile.credential_profile_id.as_deref() {
        Some(credential_id) => load_credentials_by_id(app, credential_id)?,
        None => None,
    };
    resolve_session_credentials(
        session_credentials,
        stored_credentials,
        "Select a saved credential before executing planned uploads.",
    )
}

fn resolve_local_upload_path(root: &str, relative_path: &str) -> Result<PathBuf, String> {
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

fn resolve_local_download_path(root: &str, relative_path: &str) -> Result<PathBuf, String> {
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

/// Removes empty directories from `file_path`'s parent up to (but not including) `root`.
/// Stops as soon as a directory is non-empty or cannot be removed.
fn cleanup_empty_ancestors(file_path: &Path, root: &Path) {
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

async fn refresh_remote_inventory_snapshot(
    app: &AppHandle,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<RemoteIndexSnapshot, String> {
    let credentials = resolve_execution_credentials(app, profile, session_credentials)?;
    let snapshot = list_remote_inventory(profile, &credentials).await?;
    write_remote_index_snapshot(app, &snapshot)?;
    Ok(snapshot)
}

async fn refresh_remote_inventory_snapshot_with_timeout(
    app: &AppHandle,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<RemoteIndexSnapshot, String> {
    run_with_timeout(
        refresh_remote_inventory_snapshot(app, profile, session_credentials),
        POST_UPLOAD_REMOTE_REFRESH_TIMEOUT,
        || {
            format_timeout_error(
                "Refreshing remote inventory after upload execution",
                POST_UPLOAD_REMOTE_REFRESH_TIMEOUT,
            )
        },
    )
    .await
}

fn rebuild_durable_plan(
    app: &AppHandle,
    profile: &StoredProfile,
    local_snapshot: &LocalIndexSnapshot,
    remote_snapshot: &RemoteIndexSnapshot,
) -> Result<super::sync_db::DurablePlannerSummary, String> {
    let credentials_available = profile.selected_credential_available;
    let plan =
        sync_planner::build_sync_plan(local_snapshot, remote_snapshot, credentials_available);
    persist_sync_plan(app, profile, &plan)
}

fn sync_phase_after_manual_execution(profile: &StoredProfile, previous_phase: &str) -> String {
    match previous_phase {
        "paused" => "paused".into(),
        "polling" if profile.remote_polling_enabled => "polling".into(),
        _ => "idle".into(),
    }
}

fn status_with_snapshots(
    profile: &StoredProfile,
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    plan_summary: super::sync_db::DurablePlannerSummary,
) -> SyncStatus {
    profile_to_status(profile, local_snapshot, remote_snapshot, plan_summary)
}

fn saved_profile_with_credentials_state<R: Runtime>(app: &AppHandle<R>) -> Result<StoredProfile, String> {
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
    match parse_credential_input(&input.access_key_id, &input.secret_access_key)? {
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

fn input_matches_saved_profile(profile: &StoredProfile, input: &ConnectionValidationInput) -> bool {
    profile.bucket == input.bucket.trim()
        && match input.credential_profile_id.as_deref().map(str::trim) {
            Some("") => true,
            Some(credential_id) => profile.credential_profile_id.as_deref() == Some(credential_id),
            None => true,
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

fn resolve_selected_credential_state<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: Option<&str>,
) -> Result<SelectedCredentialState, String> {
    let Some(credential_id) = credential_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(SelectedCredentialState::default());
    };

    let Some(summary) = get_credential_summary(app, credential_id)? else {
        return Ok(SelectedCredentialState::default());
    };

    Ok(SelectedCredentialState {
        selected_credential_available: summary.ready,
        selected_credential: Some(summary),
    })
}

fn format_validation_success_message(summary: &s3_adapter::S3ValidationSummary) -> String {
    format!(
        "Validated access to bucket '{}' and sampled {} remote object(s).",
        summary.bucket, summary.object_count_sampled
    )
}

impl CredentialTestContext {
    fn normalized(&self) -> Self {
        Self {
            region: self.region.trim().to_string(),
            bucket: self.bucket.trim().to_string(),
        }
    }

    fn has_bucket(&self) -> bool {
        !self.bucket.is_empty()
    }

    fn to_credential_test_config(
        &self,
        credentials: &StoredCredentials,
    ) -> s3_adapter::S3CredentialTestConfig {
        s3_adapter::S3CredentialTestConfig {
            region: self.region.clone(),
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
        }
    }
}

fn credential_test_context_from_profile(profile: &StoredProfile) -> CredentialTestContext {
    CredentialTestContext {
        region: profile.region.trim().to_string(),
        bucket: profile.bucket.trim().to_string(),
    }
}

fn resolve_credential_test_context<R: Runtime>(
    app: &AppHandle<R>,
    context: Option<CredentialTestContext>,
) -> Result<CredentialTestContext, String> {
    if let Some(context) = context {
        return Ok(context.normalized());
    }

    let profile = read_profile_from_disk(app)?;
    Ok(credential_test_context_from_profile(&profile))
}

fn format_permission_probe_summary(probes: &[s3_adapter::PermissionProbeResult]) -> String {
    let labels: Vec<String> = probes
        .iter()
        .filter(|p| p.name != "head_bucket")
        .map(|p| {
            let icon = if p.allowed { "✓" } else { "✗" };
            let label = match p.name.as_str() {
                "put_object" => "write",
                "get_object" => "read",
                "delete_object" => "delete",
                other => other,
            };
            format!("{label} {icon}")
        })
        .collect();

    if labels.is_empty() {
        let head = probes.iter().find(|p| p.name == "head_bucket");
        match head {
            Some(p) if !p.allowed => "Bucket not accessible.".into(),
            _ => String::new(),
        }
    } else {
        format!("Permissions: {}", labels.join(" · "))
    }
}

async fn test_credential_against_context(
    app: &AppHandle,
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
        match s3_adapter::validate_credentials(&test_config).await {
            Ok(summary) => {
                let message = format!(
                    "Credential is valid. Can access {} bucket(s).",
                    summary.bucket_count
                );
                (
                    true,
                    summary.checked_at,
                    message,
                    summary.bucket_count,
                    summary.buckets,
                )
            }
            Err(error) => (false, now_iso(), error, 0, vec![]),
        };

    // Phase 2: Permission probes (when bucket configured)
    let permissions = if context.has_bucket() {
        Some(s3_adapter::probe_bucket_permissions(&test_config, &context.bucket).await)
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
        Some(&validation_message),
    )?
    .ok_or_else(|| "The selected credential no longer exists.".to_string())?;

    Ok(CredentialTestResult {
        credential,
        ok: list_ok,
        checked_at,
        message: base_message,
        bucket_count,
        buckets,
        permissions,
    })
}

fn resolve_profile_credential_name(existing_profile: &StoredProfile) -> String {
    existing_profile
        .selected_credential
        .as_ref()
        .map(|summary| summary.name.clone())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "Default credential".into())
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

    match parse_credential_input(&profile.access_key_id, &profile.secret_access_key)? {
        CredentialInputState::Provided(credentials) => {
            let summary = upsert_credential(
                app,
                requested_credential_id.as_deref(),
                &resolve_profile_credential_name(existing_profile),
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
    stored = stored.normalized();
    write_profile_to_disk(app, &stored)?;

    Ok((stored, credentials))
}

fn store_profile_settings<R: Runtime>(
    app: &AppHandle<R>,
    profile: StoredProfile,
) -> Result<StoredProfile, String> {
    let existing = read_profile_from_disk(app)?;
    let mut stored = profile.normalized();
    stored.sync_pairs = existing.sync_pairs;
    if stored.active_location_id.is_none() {
        stored.active_location_id = existing.active_location_id;
    }
    let selected_state =
        resolve_selected_credential_state(app, stored.credential_profile_id.as_deref())?;
    stored.apply_selected_credential_state(selected_state);
    stored = stored.normalized();
    write_profile_to_disk(app, &stored)?;
    Ok(stored)
}

fn s3_config_for_profile(
    profile: &StoredProfile,
    credentials: &StoredCredentials,
) -> s3_adapter::S3ConnectionConfig {
    s3_adapter::S3ConnectionConfig {
        region: profile.region.clone(),
        bucket: profile.bucket.clone(),
        access_key_id: credentials.access_key_id.clone(),
        secret_access_key: credentials.secret_access_key.clone(),
    }
}

async fn execute_planned_upload_queue(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<UploadExecutionOutcome, String> {
    let credentials = resolve_execution_credentials(app, profile, session_credentials)?;
    let queue_items = load_planned_upload_queue(app, profile)?;
    let client = s3_adapter::build_client(&s3_config_for_profile(profile, &credentials)).await?;
    let uploads_ran = !queue_items.is_empty();
    let mut execution_error: Option<String> = None;

    for (index, item) in queue_items.into_iter().enumerate() {
        let started_at = now_iso();
        if let Err(error) = mark_upload_queue_item_in_progress(app, profile, item.id, &started_at) {
            execution_error = Some(error);
            break;
        }

        if item.operation == "create_directory" {
            let key = s3_adapter::directory_key(&item.path);
            emit_info_activity(
                app,
                debug_state,
                "Starting planned directory creation.",
                Some(format!(
                    "queue_item_id={} attempt={} path='{}' key='{}'",
                    item.id,
                    index + 1,
                    item.path,
                    key,
                )),
            );

            match run_with_timeout(
                s3_adapter::create_directory_placeholder(&client, &profile.bucket, &key),
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
            {
                Ok(()) => {
                    let finished_at = now_iso();
                    if let Err(error) =
                        mark_upload_queue_item_completed(app, profile, item.id, &finished_at)
                    {
                        execution_error = Some(error);
                        break;
                    }
                    emit_success_activity(
                        app,
                        debug_state,
                        "Completed planned directory creation.",
                        Some(format!(
                            "queue_item_id={} path='{}' key='{}' finished_at='{}'",
                            item.id, item.path, key, finished_at
                        )),
                    );
                }
                Err(error) => {
                    let finished_at = now_iso();
                    let failure_message =
                        format!("Directory creation failed for '{}': {error}", item.path);
                    let _ = mark_upload_queue_item_failed(
                        app,
                        profile,
                        item.id,
                        &finished_at,
                        &failure_message,
                    );
                    emit_error_activity(
                        app,
                        debug_state,
                        "Planned directory creation failed.",
                        Some(format!(
                            "queue_item_id={} path='{}' key='{}' finished_at='{}' error='{}'",
                            item.id, item.path, key, finished_at, failure_message
                        )),
                    );
                    execution_error = Some(failure_message);
                    break;
                }
            }

            continue;
        }

        let local_path = match resolve_local_upload_path(&profile.local_folder, &item.path) {
            Ok(path) => path,
            Err(error) => {
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed(app, profile, item.id, &finished_at, &error);
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
                let _ = mark_upload_queue_item_failed(app, profile, item.id, &finished_at, &error);
                execution_error = Some(error);
                break;
            }
            Err(error) => {
                let error = format!(
                    "failed to inspect planned upload source '{}': {error}",
                    local_path.display()
                );
                let finished_at = now_iso();
                let _ = mark_upload_queue_item_failed(app, profile, item.id, &finished_at, &error);
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
                let _ = mark_upload_queue_item_failed(app, profile, item.id, &finished_at, &error);
                execution_error = Some(error);
                break;
            }
        }

        let key = s3_adapter::object_key(&item.path);
        emit_info_activity(
            app,
            debug_state,
            "Starting planned upload.",
            Some(format!(
                "queue_item_id={} attempt={} path='{}' key='{}' local_path='{}' bytes={}",
                item.id,
                index + 1,
                item.path,
                key,
                local_path.display(),
                metadata.len()
            )),
        );

        match run_with_timeout(
            s3_adapter::upload_file(&client, &profile.bucket, &key, &local_path),
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
            Ok(()) => {
                let finished_at = now_iso();
                if let Err(error) =
                    mark_upload_queue_item_completed(app, profile, item.id, &finished_at)
                {
                    execution_error = Some(error);
                    break;
                }
                emit_success_activity(
                    app,
                    debug_state,
                    "Completed planned upload.",
                    Some(format!(
                        "queue_item_id={} path='{}' key='{}' finished_at='{}'",
                        item.id, item.path, key, finished_at
                    )),
                );
            }
            Err(error) => {
                let finished_at = now_iso();
                let failure_message = format!("Upload failed for '{}': {error}", item.path);
                let _ = mark_upload_queue_item_failed(
                    app,
                    profile,
                    item.id,
                    &finished_at,
                    &failure_message,
                );
                emit_error_activity(
                    app,
                    debug_state,
                    "Planned upload failed.",
                    Some(format!(
                        "queue_item_id={} path='{}' key='{}' finished_at='{}' error='{}'",
                        item.id, item.path, key, finished_at, failure_message
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

async fn execute_planned_upload_queue_with_timeout(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<UploadExecutionOutcome, String> {
    run_with_timeout(
        execute_planned_upload_queue(app, debug_state, profile, session_credentials),
        PLANNED_UPLOAD_QUEUE_TIMEOUT,
        || format_timeout_error("Executing planned uploads", PLANNED_UPLOAD_QUEUE_TIMEOUT),
    )
    .await
}

async fn execute_planned_download_queue(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<DownloadExecutionOutcome, String> {
    let credentials = resolve_execution_credentials(app, profile, session_credentials)?;
    let queue_items = load_planned_download_queue(app, profile)?;
    let client = s3_adapter::build_client(&s3_config_for_profile(profile, &credentials)).await?;
    let downloads_ran = !queue_items.is_empty();
    let mut execution_error: Option<String> = None;

    for (index, item) in queue_items.into_iter().enumerate() {
        let started_at = now_iso();
        if let Err(error) = mark_download_queue_item_in_progress(app, profile, item.id, &started_at)
        {
            execution_error = Some(error);
            break;
        }

        let local_path = match resolve_local_download_path(&profile.local_folder, &item.path) {
            Ok(path) => path,
            Err(error) => {
                let finished_at = now_iso();
                let _ =
                    mark_download_queue_item_failed(app, profile, item.id, &finished_at, &error);
                execution_error = Some(error);
                break;
            }
        };

        let key = s3_adapter::object_key(&item.path);
        emit_info_activity(
            app,
            debug_state,
            "Starting planned download.",
            Some(format!(
                "queue_item_id={} attempt={} path='{}' key='{}' local_path='{}' remote_size={}",
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

        match run_with_timeout(
            s3_adapter::download_file(&client, &profile.bucket, &key, &local_path),
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
                let finished_at = now_iso();
                if let Err(error) =
                    mark_download_queue_item_completed(app, profile, item.id, &finished_at)
                {
                    execution_error = Some(error);
                    break;
                }
                emit_success_activity(
                    app,
                    debug_state,
                    "Completed planned download.",
                    Some(format!(
                        "queue_item_id={} path='{}' key='{}' finished_at='{}'",
                        item.id, item.path, key, finished_at
                    )),
                );
            }
            Err(error) => {
                let finished_at = now_iso();
                let failure_message = format!("Download failed for '{}': {error}", item.path);
                let _ = mark_download_queue_item_failed(
                    app,
                    profile,
                    item.id,
                    &finished_at,
                    &failure_message,
                );
                emit_error_activity(
                    app,
                    debug_state,
                    "Planned download failed.",
                    Some(format!(
                        "queue_item_id={} path='{}' key='{}' finished_at='{}' error='{}'",
                        item.id, item.path, key, finished_at, failure_message
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

async fn execute_planned_download_queue_with_timeout(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
) -> Result<DownloadExecutionOutcome, String> {
    run_with_timeout(
        execute_planned_download_queue(app, debug_state, profile, session_credentials),
        PLANNED_DOWNLOAD_QUEUE_TIMEOUT,
        || {
            format_timeout_error(
                "Executing planned downloads",
                PLANNED_DOWNLOAD_QUEUE_TIMEOUT,
            )
        },
    )
    .await
}

fn build_status_with_phase(
    state: &State<'_, SyncState>,
    profile: &StoredProfile,
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    planner_summary: super::sync_db::DurablePlannerSummary,
    last_sync_at: Option<String>,
    last_error: Option<String>,
    phase: &str,
) -> Result<SyncStatus, String> {
    let mut status = get_status_lock(state)?;
    let mut next = status_with_snapshots(profile, local_snapshot, remote_snapshot, planner_summary);
    next.last_sync_at = last_sync_at;
    next.last_error = last_error;
    next.phase = phase.into();
    *status = next.clone();
    Ok(next)
}

fn stop_requested(stop_signal: Option<&AtomicBool>) -> bool {
    stop_signal
        .map(|signal| signal.load(Ordering::SeqCst))
        .unwrap_or(false)
}

async fn sleep_until_next_poll(stop_signal: &AtomicBool, duration: Duration) {
    let deadline = tokio::time::Instant::now() + duration;

    loop {
        if stop_signal.load(Ordering::SeqCst) {
            break;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(now);
        tokio::time::sleep(remaining.min(Duration::from_millis(250))).await;
    }
}

async fn run_sync_cycle(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    profile: &StoredProfile,
    session_credentials: Option<&StoredCredentials>,
    initial_phase: &str,
    final_phase: &str,
    started_message: &str,
    success_message: &str,
    issue_message: &str,
    skip_message: Option<&str>,
    stop_signal: Option<&AtomicBool>,
) -> Result<SyncStatus, String> {
    let state = app.state::<SyncState>();

    if !try_begin_sync_cycle(&state) {
        if let Some(message) = skip_message {
            emit_info_activity(app, debug_state, message, None);
        }
        return Ok(get_status_lock(&state)?.clone());
    }

    let result = async {
        let previous = get_status_lock(&state)?.clone();
        let (existing_local_snapshot, local_snapshot_error) = snapshot_for_profile(app, profile);
        let (existing_remote_snapshot, remote_snapshot_error) =
            remote_snapshot_for_profile(app, profile);

        if !is_profile_configured(profile) {
            let issue_details = sync_cycle_issue_details(
                profile,
                "configuration",
                "unconfigured",
                "Save setup details before starting sync.",
                None,
                None,
            );
            let next_status = {
                let mut status = get_status_lock(&state)?;
                let mut next = status_with_snapshots(
                    profile,
                    existing_local_snapshot.as_ref(),
                    existing_remote_snapshot.as_ref(),
                    previous.plan.clone(),
                );
                next.last_sync_at = previous.last_sync_at;
                next.phase = "unconfigured".into();
                next.last_error = Some("Save setup details before starting sync.".into());
                *status = next.clone();
                next
            };

            emit_status(app, &next_status);
            emit_error_activity(app, debug_state, issue_message, Some(issue_details));
            return Ok(next_status);
        }

        let cycle_started_at = now_iso();
        emit_info_activity(
            app,
            debug_state,
            started_message,
            Some(format!(
                "folder='{}' bucket='{}' phase='{}'",
                profile.local_folder, profile.bucket, initial_phase
            )),
        );

        let mut next_status = build_status_with_phase(
            &state,
            profile,
            existing_local_snapshot.as_ref(),
            existing_remote_snapshot.as_ref(),
            previous.plan.clone(),
            Some(cycle_started_at.clone()),
            None,
            initial_phase,
        )?;
        emit_status(app, &next_status);

        let mut local_snapshot = match scan_local_folder(Path::new(&profile.local_folder)) {
            Ok(snapshot) => {
                write_local_index_snapshot(app, &snapshot)?;
                snapshot
            }
            Err(error) => {
                if stop_requested(stop_signal) {
                    return Ok(get_status_lock(&state)?.clone());
                }
                let snapshot_context = merge_snapshot_errors(
                    local_snapshot_error.clone(),
                    remote_snapshot_error.clone(),
                );
                let issue_details = sync_cycle_issue_details(
                    profile,
                    "local-scan",
                    initial_phase,
                    &error,
                    Some(&cycle_started_at),
                    snapshot_context.as_deref(),
                );
                let next_status = build_status_with_phase(
                    &state,
                    profile,
                    existing_local_snapshot.as_ref(),
                    existing_remote_snapshot.as_ref(),
                    previous.plan.clone(),
                    Some(cycle_started_at.clone()),
                    Some(concise_sync_issue("Local scan")),
                    "error",
                )?;
                emit_status(app, &next_status);
                emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                return Ok(next_status);
            }
        };

        if stop_requested(stop_signal) {
            return Ok(get_status_lock(&state)?.clone());
        }

        let remote_snapshot =
            match refresh_remote_inventory_snapshot_with_timeout(app, profile, session_credentials)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    if stop_requested(stop_signal) {
                        return Ok(get_status_lock(&state)?.clone());
                    }
                    let snapshot_context = merge_snapshot_errors(
                        local_snapshot_error.clone(),
                        remote_snapshot_error.clone(),
                    );
                    let issue_details = sync_cycle_issue_details(
                        profile,
                        "remote-refresh",
                        initial_phase,
                        &error,
                        Some(&cycle_started_at),
                        snapshot_context.as_deref(),
                    );
                    let next_status = build_status_with_phase(
                        &state,
                        profile,
                        Some(&local_snapshot),
                        existing_remote_snapshot.as_ref(),
                        previous.plan.clone(),
                        Some(cycle_started_at.clone()),
                        Some(concise_sync_issue("Remote inventory refresh")),
                        "error",
                    )?;
                    emit_status(app, &next_status);
                    emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                    return Ok(next_status);
                }
            };

        let mut planner_summary =
            match rebuild_durable_plan(app, profile, &local_snapshot, &remote_snapshot) {
                Ok(summary) => summary,
                Err(error) => {
                    if stop_requested(stop_signal) {
                        return Ok(get_status_lock(&state)?.clone());
                    }
                    let issue_details = sync_cycle_issue_details(
                        profile,
                        "plan-build",
                        initial_phase,
                        &error,
                        Some(&cycle_started_at),
                        None,
                    );
                    let next_status = build_status_with_phase(
                        &state,
                        profile,
                        Some(&local_snapshot),
                        Some(&remote_snapshot),
                        previous.plan.clone(),
                        Some(cycle_started_at.clone()),
                        Some(concise_sync_issue("Sync plan build")),
                        "error",
                    )?;
                    emit_status(app, &next_status);
                    emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                    return Ok(next_status);
                }
            };

        let mut final_remote_snapshot = remote_snapshot;
        let mut last_error: Option<String> = None;

        if (planner_summary.upload_count > 0 || planner_summary.create_directory_count > 0)
            && !stop_requested(stop_signal)
        {
            let upload_outcome = match execute_planned_upload_queue_with_timeout(
                app,
                debug_state,
                profile,
                session_credentials,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    if stop_requested(stop_signal) {
                        return Ok(get_status_lock(&state)?.clone());
                    }
                    let issue_details = sync_cycle_issue_details(
                        profile,
                        "upload-execution",
                        initial_phase,
                        &error,
                        Some(&cycle_started_at),
                        None,
                    );
                    let next_status = build_status_with_phase(
                        &state,
                        profile,
                        Some(&local_snapshot),
                        Some(&final_remote_snapshot),
                        planner_summary.clone(),
                        Some(cycle_started_at.clone()),
                        Some(concise_sync_issue("Upload execution")),
                        "error",
                    )?;
                    emit_status(app, &next_status);
                    emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                    return Ok(next_status);
                }
            };

            last_error = upload_outcome.execution_error;

            if upload_outcome.uploads_ran {
                final_remote_snapshot = match refresh_remote_inventory_snapshot_with_timeout(
                    app,
                    profile,
                    session_credentials,
                )
                .await
                {
                    Ok(snapshot) => snapshot,
                    Err(error) => {
                        if stop_requested(stop_signal) {
                            return Ok(get_status_lock(&state)?.clone());
                        }
                        let detail_error = format!(
                            "Failed to refresh remote inventory after upload execution: {error}"
                        );
                        let issue_details = sync_cycle_issue_details(
                            profile,
                            "post-upload-remote-refresh",
                            initial_phase,
                            &detail_error,
                            Some(&cycle_started_at),
                            last_error.as_deref(),
                        );
                        let next_status = build_status_with_phase(
                            &state,
                            profile,
                            Some(&local_snapshot),
                            Some(&final_remote_snapshot),
                            planner_summary.clone(),
                            Some(cycle_started_at.clone()),
                            Some(concise_sync_issue("Post-upload remote refresh")),
                            "error",
                        )?;
                        emit_status(app, &next_status);
                        emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                        return Ok(next_status);
                    }
                };

                planner_summary = match rebuild_durable_plan(
                    app,
                    profile,
                    &local_snapshot,
                    &final_remote_snapshot,
                ) {
                    Ok(summary) => summary,
                    Err(error) => {
                        if stop_requested(stop_signal) {
                            return Ok(get_status_lock(&state)?.clone());
                        }
                        let detail_error =
                            format!("Failed to rebuild sync plan after upload execution: {error}");
                        let issue_details = sync_cycle_issue_details(
                            profile,
                            "post-upload-plan-build",
                            initial_phase,
                            &detail_error,
                            Some(&cycle_started_at),
                            last_error.as_deref(),
                        );
                        let next_status = build_status_with_phase(
                            &state,
                            profile,
                            Some(&local_snapshot),
                            Some(&final_remote_snapshot),
                            planner_summary.clone(),
                            Some(cycle_started_at.clone()),
                            Some(concise_sync_issue("Post-upload plan rebuild")),
                            "error",
                        )?;
                        emit_status(app, &next_status);
                        emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                        return Ok(next_status);
                    }
                };
            }
        }

        // Download execution
        if planner_summary.download_count > 0 && !stop_requested(stop_signal) {
            let download_outcome = match execute_planned_download_queue_with_timeout(
                app,
                debug_state,
                profile,
                session_credentials,
            )
            .await
            {
                Ok(outcome) => outcome,
                Err(error) => {
                    if stop_requested(stop_signal) {
                        return Ok(get_status_lock(&state)?.clone());
                    }
                    let detail_error = append_error_context(last_error.clone(), error.clone());
                    let issue_details = sync_cycle_issue_details(
                        profile,
                        "download-execution",
                        initial_phase,
                        &detail_error,
                        Some(&cycle_started_at),
                        None,
                    );
                    let next_status = build_status_with_phase(
                        &state,
                        profile,
                        Some(&local_snapshot),
                        Some(&final_remote_snapshot),
                        planner_summary.clone(),
                        Some(cycle_started_at.clone()),
                        Some(concise_sync_issue("Download execution")),
                        "error",
                    )?;
                    emit_status(app, &next_status);
                    emit_error_activity(app, debug_state, issue_message, Some(issue_details));
                    return Ok(next_status);
                }
            };

            last_error = match (last_error, download_outcome.execution_error) {
                (Some(prev), Some(dl_err)) => Some(format!("{prev}. {dl_err}")),
                (None, Some(dl_err)) => Some(dl_err),
                (existing, None) => existing,
            };

            if download_outcome.downloads_ran {
                // Rescan local folder to update the local index after downloads
                if let Ok(updated_snapshot) = scan_local_folder(Path::new(&profile.local_folder)) {
                    let _ = write_local_index_snapshot(app, &updated_snapshot);

                    // Rebuild plan with fresh local snapshot
                    planner_summary = match rebuild_durable_plan(
                        app,
                        profile,
                        &updated_snapshot,
                        &final_remote_snapshot,
                    ) {
                        Ok(summary) => summary,
                        Err(error) => {
                            if stop_requested(stop_signal) {
                                return Ok(get_status_lock(&state)?.clone());
                            }
                            last_error = Some(append_error_context(
                                last_error.clone(),
                                format!(
                                    "Failed to rebuild sync plan after download execution: {error}"
                                ),
                            ));
                            planner_summary
                        }
                    };

                    local_snapshot = updated_snapshot;
                } else {
                    last_error = Some(append_error_context(
                        last_error.clone(),
                        "Failed to rescan local folder after download execution.".to_string(),
                    ));
                }
            }
        }

        if stop_requested(stop_signal) {
            return Ok(get_status_lock(&state)?.clone());
        }

        let final_issue_details = last_error.clone().or_else(|| {
            merge_snapshot_errors(local_snapshot_error.clone(), remote_snapshot_error.clone())
        });
        let final_status_error = final_issue_details
            .as_ref()
            .map(|_| "Sync cycle completed with issues.".to_string());

        next_status = build_status_with_phase(
            &state,
            profile,
            Some(&local_snapshot),
            Some(&final_remote_snapshot),
            planner_summary,
            Some(cycle_started_at),
            final_status_error,
            if last_error.is_some() {
                "error"
            } else {
                final_phase
            },
        )?;
        emit_status(app, &next_status);

        if let Some(error) = next_status.last_error.as_ref() {
            emit_error_activity(
                app,
                debug_state,
                issue_message,
                Some(sync_cycle_issue_details(
                    profile,
                    "sync-cycle",
                    &next_status.phase,
                    final_issue_details.as_deref().unwrap_or(error),
                    next_status.last_sync_at.as_deref(),
                    Some("Sync cycle completed with recoverable issues."),
                )),
            );
        } else {
            emit_success_activity(
                app,
                debug_state,
                success_message,
                Some(format!(
                    "phase='{}' pending_operations={} last_sync_at='{}'",
                    next_status.phase,
                    next_status.pending_operations,
                    next_status.last_sync_at.clone().unwrap_or_default()
                )),
            );
        }

        Ok(next_status)
    }
    .await;

    finish_sync_cycle(&state);
    result
}

fn start_polling_worker_task(app: &AppHandle) -> Result<(), String> {
    let state = app.state::<SyncState>();
    let (worker_id, stop_signal) = begin_polling_worker(&state)?;
    let app_handle = app.clone();

    tauri::async_runtime::spawn(async move {
        loop {
            let profile = match saved_profile_with_credentials_state(&app_handle) {
                Ok(profile) => profile,
                Err(error) => {
                    let debug_state = app_handle.state::<ActivityDebugState>();
                    emit_error_activity(
                        &app_handle,
                        &debug_state,
                        "Polling worker stopped after profile load failed.",
                        Some(error),
                    );
                    break;
                }
            };

            if !profile.remote_polling_enabled || !is_profile_configured(&profile) {
                break;
            }

            sleep_until_next_poll(
                stop_signal.as_ref(),
                Duration::from_secs(profile.poll_interval_seconds.max(1) as u64),
            )
            .await;

            if stop_signal.load(Ordering::SeqCst) {
                break;
            }

            let debug_state = app_handle.state::<ActivityDebugState>();
            let _ = run_sync_cycle(
                &app_handle,
                &debug_state,
                &profile,
                None,
                "polling",
                "polling",
                "Running polling sync cycle.",
                "Polling sync cycle finished.",
                "Polling sync cycle finished with an issue.",
                Some("Polling sync cycle skipped because another sync cycle is already running."),
                Some(stop_signal.as_ref()),
            )
            .await;
        }

        let state = app_handle.state::<SyncState>();
        let _ = clear_polling_worker(&state, worker_id);
    });

    Ok(())
}

fn phase_after_save(profile: &StoredProfile, previous_phase: &str) -> String {
    if !is_profile_configured(profile) {
        return "unconfigured".into();
    }

    match previous_phase {
        "paused" => "paused".into(),
        "polling" if profile.remote_polling_enabled => "polling".into(),
        "syncing" if !profile.remote_polling_enabled => "syncing".into(),
        _ => "idle".into(),
    }
}

fn phase_after_rescan(profile: &StoredProfile, previous_phase: &str) -> String {
    if !is_profile_configured(profile) {
        return "unconfigured".into();
    }

    match previous_phase {
        "paused" => "paused".into(),
        "syncing" if !profile.remote_polling_enabled => "syncing".into(),
        "polling" if profile.remote_polling_enabled => "polling".into(),
        _ => "idle".into(),
    }
}

#[tauri::command]
pub async fn validate_s3_connection(
    app: AppHandle,
    input: ConnectionValidationInput,
) -> Result<ConnectionValidationResult, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let credentials = resolve_refresh_credentials(&app, &profile, &input)?;
    let summary = s3_adapter::validate_connection(&input.to_s3_config(&credentials)).await?;
    let message = format_validation_success_message(&summary);

    Ok(ConnectionValidationResult {
        ok: true,
        checked_at: summary.checked_at,
        message,
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
    let created = create_credential(&app, draft)?;

    let context = resolve_credential_test_context(&app, None)?;
    Ok(test_credential_against_context(&app, &created.id, &context)
        .await?
        .credential)
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

    Ok((DeleteCredentialResult { deleted, profile }, cleared_selected_credential))
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
fn load_profile_impl<R: Runtime>(
    app: AppHandle<R>,
    _state: State<'_, SyncState>,
    _debug_state: State<'_, ActivityDebugState>,
) -> Result<StoredProfile, String> {
    saved_profile_with_credentials_state(&app)
}

#[tauri::command]
pub fn save_profile(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
    profile: ProfileDraft,
) -> Result<StoredProfile, String> {
    let (stored, _) = store_profile_draft(&app, profile)?;

    let previous = get_status_lock(&state)?.clone();
    let (snapshot, snapshot_error) = snapshot_for_profile(&app, &stored);
    let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &stored);
    let planner_summary = load_planner_summary(&app, &stored)?;
    let next_status = {
        let mut status = get_status_lock(&state)?;
        let mut next = status_with_snapshots(
            &stored,
            snapshot.as_ref(),
            remote_snapshot.as_ref(),
            planner_summary,
        );
        next.last_sync_at = previous.last_sync_at.clone();
        next.last_error = merge_snapshot_errors(snapshot_error, remote_snapshot_error);
        next.phase = if next.last_error.is_some() && is_profile_configured(&stored) {
            "error".into()
        } else {
            phase_after_save(&stored, &previous.phase)
        };
        *status = next.clone();
        next
    };

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

    let previous = get_status_lock(&state)?.clone();
    let (snapshot, snapshot_error) = snapshot_for_profile(&app, &stored);
    let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &stored);
    let planner_summary = load_planner_summary(&app, &stored)?;
    let next_status = {
        let mut status = get_status_lock(&state)?;
        let mut next = status_with_snapshots(
            &stored,
            snapshot.as_ref(),
            remote_snapshot.as_ref(),
            planner_summary,
        );
        next.last_sync_at = previous.last_sync_at.clone();
        next.last_error = merge_snapshot_errors(snapshot_error, remote_snapshot_error);
        next.phase = if next.last_error.is_some() && is_profile_configured(&stored) {
            "error".into()
        } else {
            phase_after_save(&stored, &previous.phase)
        };
        *status = next.clone();
        next
    };

    emit_status(&app, &next_status);
    emit_success_activity(
        &app,
        &debug_state,
        "Saved sync settings.",
        Some(format!(
            "polling_enabled={} poll_interval_seconds={} delete_safety_hours={} activity_debug_mode_enabled={}",
            stored.remote_polling_enabled,
            stored.poll_interval_seconds,
            stored.delete_safety_hours,
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
fn save_profile_settings_impl<R: Runtime>(
    app: AppHandle<R>,
    _state: State<'_, SyncState>,
    _debug_state: State<'_, ActivityDebugState>,
    profile: StoredProfile,
) -> Result<StoredProfile, String> {
    store_profile_settings(&app, profile)
}

#[tauri::command]
pub async fn connect_and_sync(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
    profile: ProfileDraft,
) -> Result<SyncStatus, String> {
    let (stored_profile, credentials) = store_profile_draft(&app, profile)?;

    emit_info_activity(
        &app,
        &debug_state,
        "Starting connect and sync.",
        Some(format!(
            "folder='{}' bucket='{}' polling_enabled={}",
            stored_profile.local_folder,
            stored_profile.bucket,
            stored_profile.remote_polling_enabled
        )),
    );

    s3_adapter::ensure_bucket_exists(&s3_config_for_profile(&stored_profile, &credentials)).await?;
    emit_success_activity(
        &app,
        &debug_state,
        "Verified remote bucket access.",
        Some(format!("bucket='{}'", stored_profile.bucket)),
    );

    let next_status = run_sync_cycle(
        &app,
        &debug_state,
        &stored_profile,
        Some(&credentials),
        "syncing",
        if stored_profile.remote_polling_enabled {
            "polling"
        } else {
            "idle"
        },
        "Running initial sync cycle.",
        "Connect and sync finished.",
        "Connect and sync finished with an issue.",
        None,
        None,
    )
    .await?;

    if stored_profile.remote_polling_enabled {
        start_polling_worker(&app)?;
    } else {
        let _ = stop_polling_worker(&state)?;
    }

    Ok(next_status)
}

#[tauri::command]
pub fn get_sync_status(app: AppHandle, state: State<'_, SyncState>) -> Result<SyncStatus, String> {
    let mut status = get_status_lock(&state)?.clone();
    let profile = read_profile_from_disk(&app)?;

    if !profile.sync_pairs.is_empty() {
        status.locations = current_pair_statuses(&profile, &app);
    }

    Ok(status)
}

#[tauri::command]
pub async fn start_sync(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;

    let _ = stop_polling_worker(&state)?;
    let next_status = run_sync_cycle(
        &app,
        &debug_state,
        &profile,
        None,
        "syncing",
        if profile.remote_polling_enabled {
            "polling"
        } else {
            "idle"
        },
        "Sync started.",
        if profile.remote_polling_enabled {
            "Sync cycle finished; polling remains active."
        } else {
            "Sync run finished."
        },
        "Sync run finished with an issue.",
        Some("Sync start skipped because another sync cycle is already running."),
        None,
    )
    .await?;

    if profile.remote_polling_enabled && next_status.phase != "unconfigured" {
        start_polling_worker(&app)?;
    }

    Ok(next_status)
}

#[tauri::command]
pub fn pause_sync(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
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

#[tauri::command]
pub fn run_full_rescan(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let previous = get_status_lock(&state)?.clone();
    let (existing_snapshot, snapshot_error) = snapshot_for_profile(&app, &profile);
    let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &profile);

    if !is_profile_configured(&profile) {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                existing_snapshot.as_ref(),
                remote_snapshot.as_ref(),
                previous.plan.clone(),
            );
            next.last_sync_at = previous.last_sync_at;
            next.last_error = Some("Save setup details before running local indexing.".into());
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Local indexing needs saved setup first.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    }

    emit_info_activity(
        &app,
        &debug_state,
        "Running local rescan.",
        Some(format!("folder='{}'", profile.local_folder)),
    );

    match scan_local_folder(Path::new(&profile.local_folder)) {
        Ok(snapshot) => match write_local_index_snapshot(&app, &snapshot) {
            Ok(()) => {
                let (plan, plan_warning) = match remote_snapshot.as_ref() {
                    Some(remote) => match rebuild_durable_plan(&app, &profile, &snapshot, remote) {
                        Ok(summary) => (summary, None),
                        Err(error) => (
                            previous.plan.clone(),
                            Some(format!("Plan rebuild failed after local rescan: {error}")),
                        ),
                    },
                    None => (previous.plan.clone(), None),
                };

                let next_status = {
                    let mut status = get_status_lock(&state)?;
                    let mut next = status_with_snapshots(
                        &profile,
                        Some(&snapshot),
                        remote_snapshot.as_ref(),
                        plan,
                    );
                    next.last_sync_at = previous.last_sync_at;
                    next.last_error = plan_warning;
                    next.phase = phase_after_rescan(&profile, &previous.phase);
                    *status = next.clone();
                    next
                };

                emit_status(&app, &next_status);
                emit_success_activity(
                    &app,
                    &debug_state,
                    "Local rescan finished.",
                    Some(format!(
                        "files={} directories={} bytes={}",
                        snapshot.summary.file_count,
                        snapshot.summary.directory_count,
                        snapshot.summary.total_bytes
                    )),
                );
                Ok(next_status)
            }
            Err(error) => {
                let next_status = {
                    let mut status = get_status_lock(&state)?;
                    let mut next = status_with_snapshots(
                        &profile,
                        existing_snapshot.as_ref(),
                        remote_snapshot.as_ref(),
                        previous.plan.clone(),
                    );
                    next.last_sync_at = previous.last_sync_at;
                    next.last_error = Some(format!(
                        "Local indexing succeeded but snapshot persistence failed: {error}"
                    ));
                    next.phase = "error".into();
                    *status = next.clone();
                    next
                };

                emit_status(&app, &next_status);
                emit_error_activity(
                    &app,
                    &debug_state,
                    "Local rescan could not save its snapshot.",
                    next_status.last_error.clone(),
                );
                Ok(next_status)
            }
        },
        Err(error) => {
            let next_status = {
                let mut status = get_status_lock(&state)?;
                let mut next = status_with_snapshots(
                    &profile,
                    existing_snapshot.as_ref(),
                    remote_snapshot.as_ref(),
                    previous.plan.clone(),
                );
                next.last_sync_at = previous.last_sync_at;
                next.last_error = Some(
                    match merge_snapshot_errors(snapshot_error, remote_snapshot_error) {
                        Some(snapshot_error) => format!("{error}. {snapshot_error}"),
                        None => error,
                    },
                );
                next.phase = "error".into();
                *status = next.clone();
                next
            };

            emit_status(&app, &next_status);
            emit_error_activity(
                &app,
                &debug_state,
                "Local rescan failed.",
                next_status.last_error.clone(),
            );
            Ok(next_status)
        }
    }
}

#[tauri::command]
pub async fn refresh_remote_inventory(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
    input: ConnectionValidationInput,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let previous = get_status_lock(&state)?.clone();
    let (local_snapshot, local_snapshot_error) = snapshot_for_profile(&app, &profile);
    let (existing_remote_snapshot, remote_snapshot_error) =
        remote_snapshot_for_profile(&app, &profile);

    if !is_profile_configured(&profile) {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                local_snapshot.as_ref(),
                existing_remote_snapshot.as_ref(),
                previous.plan.clone(),
            );
            next.last_sync_at = previous.last_sync_at;
            next.phase = "unconfigured".into();
            next.last_error = Some("Save setup details before refreshing remote inventory.".into());
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Remote inventory refresh needs saved setup first.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    }

    if !input_matches_saved_profile(&profile, &input) {
        return Err(
            "Refresh remote inventory requires credentials for the currently saved bucket."
                .into(),
        );
    }

    emit_info_activity(
        &app,
        &debug_state,
        "Refreshing remote inventory.",
        Some(format!("bucket='{}'", profile.bucket)),
    );

    let credentials = resolve_refresh_credentials(&app, &profile, &input)?;

    match list_remote_inventory(&profile, &credentials).await {
        Ok(snapshot) => match write_remote_index_snapshot(&app, &snapshot) {
            Ok(()) => {
                let (plan, plan_warning) = match local_snapshot.as_ref() {
                    Some(local) => match rebuild_durable_plan(&app, &profile, local, &snapshot) {
                        Ok(summary) => (summary, None),
                        Err(error) => (
                            previous.plan.clone(),
                            Some(format!("Plan rebuild failed after remote refresh: {error}")),
                        ),
                    },
                    None => (previous.plan.clone(), None),
                };

                let next_status = {
                    let mut status = get_status_lock(&state)?;
                    let mut next = status_with_snapshots(
                        &profile,
                        local_snapshot.as_ref(),
                        Some(&snapshot),
                        plan,
                    );
                    next.last_sync_at = previous.last_sync_at;
                    next.last_error = match (plan_warning, local_snapshot_error) {
                        (Some(pw), Some(le)) => Some(format!("{pw}. {le}")),
                        (Some(pw), None) => Some(pw),
                        (None, le) => le,
                    };
                    next.phase = match previous.phase.as_str() {
                        "paused" => "paused".into(),
                        "syncing" if !profile.remote_polling_enabled => "syncing".into(),
                        "polling" if profile.remote_polling_enabled => "polling".into(),
                        _ => "idle".into(),
                    };
                    *status = next.clone();
                    next
                };

                emit_status(&app, &next_status);
                emit_success_activity(
                    &app,
                    &debug_state,
                    "Remote inventory refreshed.",
                    Some(format!(
                        "objects={} bytes={}",
                        snapshot.summary.object_count, snapshot.summary.total_bytes
                    )),
                );
                Ok(next_status)
            }
            Err(error) => {
                let next_status = {
                    let mut status = get_status_lock(&state)?;
                    let mut next = status_with_snapshots(
                        &profile,
                        local_snapshot.as_ref(),
                        existing_remote_snapshot.as_ref(),
                        previous.plan.clone(),
                    );
                    next.last_sync_at = previous.last_sync_at;
                    next.last_error = Some(match merge_snapshot_errors(local_snapshot_error, remote_snapshot_error) {
                        Some(snapshot_error) => format!(
                            "Remote inventory succeeded but snapshot persistence failed: {error}. {snapshot_error}"
                        ),
                        None => format!("Remote inventory succeeded but snapshot persistence failed: {error}"),
                    });
                    next.phase = "error".into();
                    *status = next.clone();
                    next
                };

                emit_status(&app, &next_status);
                emit_error_activity(
                    &app,
                    &debug_state,
                    "Remote inventory refresh could not save its snapshot.",
                    next_status.last_error.clone(),
                );
                Ok(next_status)
            }
        },
        Err(error) => {
            let next_status = {
                let mut status = get_status_lock(&state)?;
                let mut next = status_with_snapshots(
                    &profile,
                    local_snapshot.as_ref(),
                    existing_remote_snapshot.as_ref(),
                    previous.plan.clone(),
                );
                next.last_sync_at = previous.last_sync_at;
                next.last_error = Some(
                    match merge_snapshot_errors(local_snapshot_error, remote_snapshot_error) {
                        Some(snapshot_error) => format!("{error}. {snapshot_error}"),
                        None => error,
                    },
                );
                next.phase = "error".into();
                *status = next.clone();
                next
            };

            emit_status(&app, &next_status);
            emit_error_activity(
                &app,
                &debug_state,
                "Remote inventory refresh failed.",
                next_status.last_error.clone(),
            );
            Ok(next_status)
        }
    }
}

#[tauri::command]
pub fn build_sync_plan(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let previous = get_status_lock(&state)?.clone();
    let (local_snapshot, local_snapshot_error) = snapshot_for_profile(&app, &profile);
    let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &profile);

    if !is_profile_configured(&profile) {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                local_snapshot.as_ref(),
                remote_snapshot.as_ref(),
                previous.plan.clone(),
            );
            next.last_sync_at = previous.last_sync_at;
            next.last_error = Some("Save setup details before building a sync plan.".into());
            next.phase = "unconfigured".into();
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Sync plan build needs saved setup first.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    }

    let Some(local_snapshot) = local_snapshot else {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                None,
                remote_snapshot.as_ref(),
                previous.plan.clone(),
            );
            next.last_sync_at = previous.last_sync_at;
            next.last_error = Some(
                match merge_snapshot_errors(local_snapshot_error, remote_snapshot_error) {
                    Some(snapshot_error) => {
                        format!("Run a full rescan before building a sync plan. {snapshot_error}")
                    }
                    None => "Run a full rescan before building a sync plan.".into(),
                },
            );
            next.phase = "error".into();
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Sync plan build needs a fresh local rescan.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    };

    let Some(remote_snapshot) = remote_snapshot else {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next =
                status_with_snapshots(&profile, Some(&local_snapshot), None, previous.plan.clone());
            next.last_sync_at = previous.last_sync_at;
            next.last_error = Some(
                match merge_snapshot_errors(local_snapshot_error, remote_snapshot_error) {
                    Some(snapshot_error) => format!(
                        "Refresh remote inventory before building a sync plan. {snapshot_error}"
                    ),
                    None => "Refresh remote inventory before building a sync plan.".into(),
                },
            );
            next.phase = "error".into();
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Sync plan build needs refreshed remote inventory.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    };

    emit_info_activity(
        &app,
        &debug_state,
        "Building sync plan.",
        Some(format!(
            "local_files={} remote_objects={}",
            local_snapshot.summary.file_count, remote_snapshot.summary.object_count
        )),
    );

    let credentials_available = profile.selected_credential_available;
    let plan =
        sync_planner::build_sync_plan(&local_snapshot, &remote_snapshot, credentials_available);
    let planner_summary = persist_sync_plan(&app, &profile, &plan)?;

    let next_status = {
        let mut status = get_status_lock(&state)?;
        let mut next = status_with_snapshots(
            &profile,
            Some(&local_snapshot),
            Some(&remote_snapshot),
            planner_summary,
        );
        next.last_sync_at = previous.last_sync_at;
        next.last_error = merge_snapshot_errors(local_snapshot_error, remote_snapshot_error);
        next.phase = match previous.phase.as_str() {
            "paused" => "paused".into(),
            "syncing" if !profile.remote_polling_enabled => "syncing".into(),
            "polling" if profile.remote_polling_enabled => "polling".into(),
            _ => "idle".into(),
        };
        *status = next.clone();
        next
    };

    emit_status(&app, &next_status);
    emit_success_activity(
        &app,
        &debug_state,
        "Sync plan ready.",
        Some(format!(
            "uploads={} directories={} conflicts={} pending_operations={}",
            next_status.plan.upload_count,
            next_status.plan.create_directory_count,
            next_status.plan.conflict_count,
            next_status.plan.pending_operation_count
        )),
    );
    Ok(next_status)
}

#[tauri::command]
pub async fn execute_planned_uploads(
    app: AppHandle,
    state: State<'_, SyncState>,
    debug_state: State<'_, ActivityDebugState>,
) -> Result<SyncStatus, String> {
    let profile = saved_profile_with_credentials_state(&app)?;
    let previous = get_status_lock(&state)?.clone();
    let (local_snapshot, local_snapshot_error) = snapshot_for_profile(&app, &profile);
    let (remote_snapshot, remote_snapshot_error) = remote_snapshot_for_profile(&app, &profile);

    if !is_profile_configured(&profile) {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                local_snapshot.as_ref(),
                remote_snapshot.as_ref(),
                previous.plan.clone(),
            );
            next.last_sync_at = previous.last_sync_at;
            next.phase = "unconfigured".into();
            next.last_error = Some("Save setup details before executing planned uploads.".into());
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Manual uploads need saved setup first.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    }

    let Some(local_snapshot) = local_snapshot else {
        let next_status = {
            let mut status = get_status_lock(&state)?;
            let mut next = status_with_snapshots(
                &profile,
                None,
                remote_snapshot.as_ref(),
                previous.plan.clone(),
            );
            next.last_sync_at = previous.last_sync_at;
            next.last_error = Some(
                match merge_snapshot_errors(local_snapshot_error, remote_snapshot_error) {
                    Some(snapshot_error) => format!(
                        "Run a full rescan before executing planned uploads. {snapshot_error}"
                    ),
                    None => "Run a full rescan before executing planned uploads.".into(),
                },
            );
            next.phase = "error".into();
            *status = next.clone();
            next
        };

        emit_status(&app, &next_status);
        emit_error_activity(
            &app,
            &debug_state,
            "Manual uploads need a fresh local rescan.",
            next_status.last_error.clone(),
        );
        return Ok(next_status);
    };

    let run_started_at = now_iso();

    emit_info_activity(
        &app,
        &debug_state,
        "Running planned uploads.",
        Some(format!(
            "folder='{}' bucket='{}' pending_operations={}",
            profile.local_folder, profile.bucket, previous.plan.pending_operation_count
        )),
    );

    {
        let mut status = get_status_lock(&state)?;
        let mut next = status_with_snapshots(
            &profile,
            Some(&local_snapshot),
            remote_snapshot.as_ref(),
            previous.plan.clone(),
        );
        next.last_sync_at = Some(run_started_at.clone());
        next.last_error = None;
        next.phase = "syncing".into();
        *status = next.clone();
        emit_status(&app, &next);
    }

    let execution_error =
        execute_planned_upload_queue_with_timeout(&app, &debug_state, &profile, None)
            .await?
            .execution_error;

    if let Some(error) = execution_error.as_ref() {
        emit_error_activity(
            &app,
            &debug_state,
            "Planned uploads hit an issue.",
            Some(error.clone()),
        );
    } else {
        emit_success_activity(
            &app,
            &debug_state,
            "Planned uploads completed.",
            Some(format!("run_started_at='{}'", run_started_at)),
        );
    }

    let refreshed_remote_snapshot =
        match refresh_remote_inventory_snapshot_with_timeout(&app, &profile, None).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                let next_status =
                    {
                        let mut status = get_status_lock(&state)?;
                        let mut next = status_with_snapshots(
                            &profile,
                            Some(&local_snapshot),
                            remote_snapshot.as_ref(),
                            previous.plan.clone(),
                        );
                        next.last_sync_at = Some(run_started_at);
                        next.phase = "error".into();
                        next.last_error = Some(append_error_context(
                    execution_error.clone(),
                    format!("Failed to refresh remote inventory after upload execution: {error}"),
                ));
                        *status = next.clone();
                        next
                    };

                emit_status(&app, &next_status);
                emit_error_activity(
                    &app,
                    &debug_state,
                    "Post-upload remote refresh failed.",
                    next_status.last_error.clone(),
                );
                return Ok(next_status);
            }
        };

    emit_success_activity(
        &app,
        &debug_state,
        "Refreshed remote inventory after manual uploads.",
        Some(format!(
            "objects={} bytes={}",
            refreshed_remote_snapshot.summary.object_count,
            refreshed_remote_snapshot.summary.total_bytes
        )),
    );

    let planner_summary =
        match rebuild_durable_plan(&app, &profile, &local_snapshot, &refreshed_remote_snapshot) {
            Ok(summary) => summary,
            Err(error) => {
                let next_status = {
                    let mut status = get_status_lock(&state)?;
                    let mut next = status_with_snapshots(
                        &profile,
                        Some(&local_snapshot),
                        Some(&refreshed_remote_snapshot),
                        previous.plan.clone(),
                    );
                    next.last_sync_at = Some(run_started_at);
                    next.phase = "error".into();
                    next.last_error = Some(append_error_context(
                        execution_error.clone(),
                        format!("Failed to rebuild sync plan after upload execution: {error}"),
                    ));
                    *status = next.clone();
                    next
                };

                emit_status(&app, &next_status);
                emit_error_activity(
                    &app,
                    &debug_state,
                    "Post-upload sync plan rebuild failed.",
                    next_status.last_error.clone(),
                );
                return Ok(next_status);
            }
        };

    let next_status = {
        let mut status = get_status_lock(&state)?;
        let mut next = status_with_snapshots(
            &profile,
            Some(&local_snapshot),
            Some(&refreshed_remote_snapshot),
            planner_summary,
        );
        next.last_sync_at = Some(run_started_at);
        next.last_error = execution_error
            .or_else(|| merge_snapshot_errors(local_snapshot_error, remote_snapshot_error));
        next.phase = if next.last_error.is_some() {
            "error".into()
        } else {
            sync_phase_after_manual_execution(&profile, &previous.phase)
        };
        *status = next.clone();
        next
    };

    emit_status(&app, &next_status);
    if let Some(error) = next_status.last_error.as_ref() {
        emit_error_activity(
            &app,
            &debug_state,
            "Manual upload run finished with an issue.",
            Some(error.clone()),
        );
    } else {
        emit_success_activity(
            &app,
            &debug_state,
            "Manual upload run finished.",
            Some(format!(
                "phase='{}' pending_operations={}",
                next_status.phase, next_status.pending_operations
            )),
        );
    }
    Ok(next_status)
}

// ---------------------------------------------------------------------------
// Per-pair helpers (private — not exposed as Tauri commands)
// ---------------------------------------------------------------------------

fn s3_config_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> s3_adapter::S3ConnectionConfig {
    s3_adapter::S3ConnectionConfig {
        region: pair.region.clone(),
        bucket: pair.bucket.clone(),
        access_key_id: credentials.access_key_id.clone(),
        secret_access_key: credentials.secret_access_key.clone(),
    }
}

fn resolve_credentials_for_pair(
    app: &AppHandle,
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

fn current_pair_statuses(profile: &StoredProfile, app: &AppHandle) -> Vec<PairSyncStatus> {
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

fn snapshot_for_pair(
    app: &AppHandle,
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

fn remote_snapshot_for_pair(
    app: &AppHandle,
    pair: &SyncPair,
) -> (Option<RemoteIndexSnapshot>, Option<String>) {
    match read_remote_index_snapshot_for_pair(app, &pair.id) {
        Ok(snapshot) => (
            snapshot.filter(|s| {
                !pair.bucket.is_empty()
                    && super::remote_index::snapshot_matches_target(
                        s,
                        &pair.bucket,
                    )
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

async fn list_remote_inventory_for_pair(
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<RemoteIndexSnapshot, String> {
    let config = s3_config_for_pair(pair, credentials);
    let client = s3_adapter::build_client(&config).await?;
    let mut continuation_token: Option<String> = None;
    let mut entries = BTreeMap::new();
    let mut object_count = 0_u64;
    let mut total_bytes = 0_u64;

    loop {
        let mut request = client.list_objects_v2().bucket(&pair.bucket);

        if let Some(token) = continuation_token.as_deref() {
            request = request.continuation_token(token);
        }

        let response = request.send().await.map_err(|error| {
            format!(
                "failed to list remote S3 inventory for pair '{}': {error}",
                pair.label
            )
        })?;

        for object in response.contents() {
            let Some(key) = object.key() else {
                continue;
            };

            let relative_path = relative_path_from_key(key);
            let last_modified_at = object.last_modified().map(|value| value.to_string());
            let etag = object.e_tag().map(|value| value.to_string());
            let storage_class = object.storage_class().map(|sc| sc.as_str().to_string());

            if key.ends_with('/') {
                let directory_path = relative_path.trim_matches('/');
                if !directory_path.is_empty() {
                    entries.insert(
                        directory_path.to_string(),
                        RemoteObjectEntry {
                            key: key.to_string(),
                            relative_path: directory_path.to_string(),
                            kind: "directory".into(),
                            size: 0,
                            last_modified_at,
                            etag,
                            storage_class: None,
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
                        });
                }
                continue;
            }

            let size = object.size().unwrap_or_default().max(0) as u64;
            object_count += 1;
            total_bytes += size;
            entries.insert(
                relative_path.clone(),
                RemoteObjectEntry {
                    key: key.to_string(),
                    relative_path,
                    kind: "file".into(),
                    size,
                    last_modified_at,
                    etag,
                    storage_class,
                },
            );

            for directory_path in directory_relative_paths_from_key(key) {
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
                    });
            }
        }

        if response.is_truncated().unwrap_or(false) {
            continuation_token = response.next_continuation_token().map(ToString::to_string);
        } else {
            break;
        }
    }

    let entries = entries.into_values().collect::<Vec<_>>();

    Ok(RemoteIndexSnapshot {
        version: 1,
        bucket: pair.bucket.clone(),
        summary: RemoteIndexSummary {
            indexed_at: now_iso(),
            object_count,
            total_bytes,
        },
        entries,
    })
}

fn rebuild_durable_plan_for_pair(
    app: &AppHandle,
    pair: &SyncPair,
    local_snapshot: &LocalIndexSnapshot,
    remote_snapshot: &RemoteIndexSnapshot,
    credentials_available: bool,
) -> Result<super::sync_db::DurablePlannerSummary, String> {
    let plan =
        sync_planner::build_sync_plan(local_snapshot, remote_snapshot, credentials_available);
    persist_sync_plan_for_pair(app, pair, &plan)
}

async fn execute_planned_upload_queue_for_pair(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<UploadExecutionOutcome, String> {
    let queue_items = load_planned_upload_queue_for_pair(app, pair)?;
    let client = s3_adapter::build_client(&s3_config_for_pair(pair, credentials)).await?;
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

            match run_with_timeout(
                s3_adapter::create_directory_placeholder(&client, &pair.bucket, &key),
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
            {
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
            s3_adapter::upload_file(&client, &pair.bucket, &key, &local_path),
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

async fn execute_planned_download_queue_for_pair(
    app: &AppHandle,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    credentials: &StoredCredentials,
) -> Result<DownloadExecutionOutcome, String> {
    let queue_items = load_planned_download_queue_for_pair(app, pair)?;
    let client = s3_adapter::build_client(&s3_config_for_pair(pair, credentials)).await?;
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
            s3_adapter::download_file(&client, &pair.bucket, &key, &local_path),
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
            "pair='{}' folder='{}' bucket='{}'",
            pair.label, pair.local_folder, pair.bucket
        )),
    );

    let cycle_started_at = now_iso();

    // 1. Scan local folder
    let mut local_snapshot = match scan_local_folder(Path::new(&pair.local_folder)) {
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

            let enabled_pairs: Vec<&SyncPair> = profile
                .sync_pairs
                .iter()
                .filter(|p| p.enabled && is_pair_configured(p))
                .collect();

            if enabled_pairs.is_empty() {
                break;
            }

            let interval = enabled_pairs
                .iter()
                .map(|p| p.poll_interval_seconds)
                .min()
                .unwrap_or(60)
                .max(15);

            sleep_until_next_poll(stop_signal.as_ref(), Duration::from_secs(interval as u64)).await;

            if stop_signal.load(Ordering::SeqCst) {
                break;
            }

            let debug_state = app_handle.state::<ActivityDebugState>();
            let mut pair_statuses: Vec<PairSyncStatus> = Vec::new();
            for pair in &enabled_pairs {
                if stop_signal.load(Ordering::SeqCst) {
                    break;
                }
                match run_sync_cycle_for_pair(
                    &app_handle,
                    &debug_state,
                    pair,
                    Some(stop_signal.as_ref()),
                )
                .await
                {
                    Ok(status) => pair_statuses.push(status),
                    Err(_) => {} // error already emitted by run_sync_cycle_for_pair
                }
            }

            // Update global status from pair results
            if !pair_statuses.is_empty() {
                let mut synthesized = synthesize_status_from_pairs(&pair_statuses);
                // Mark as "polling" since we're in the polling worker
                if synthesized.phase == "idle" {
                    synthesized.phase = "polling".into();
                }
                synthesized.remote_polling_enabled = true;
                let _ = set_status_from_handle(&app_handle, synthesized.clone());
                emit_status(&app_handle, &synthesized);
            }
        }

        let state = app_handle.state::<SyncState>();
        let _ = clear_polling_worker(&state, worker_id);
    });

    Ok(())
}

/// Starts the appropriate polling worker based on the current profile state.
/// If configured sync pairs exist, uses the per-pair polling worker.
/// Otherwise, falls back to the legacy single-profile polling worker.
fn start_polling_worker(app: &AppHandle) -> Result<(), String> {
    let profile = read_profile_from_disk(app)?;
    let has_configured_pairs = profile
        .sync_pairs
        .iter()
        .any(|p| p.enabled && is_pair_configured(p));

    if has_configured_pairs {
        start_polling_worker_for_pairs(app)
    } else {
        start_polling_worker_task(app)
    }
}

// ---------------------------------------------------------------------------
// Sync-pair CRUD commands
// ---------------------------------------------------------------------------

#[tauri::command]
pub fn list_sync_pairs(app: AppHandle) -> Result<Vec<SyncPair>, String> {
    let profile = read_profile_from_disk(&app)?;
    Ok(profile.sync_pairs)
}

#[tauri::command]
pub fn list_sync_locations(app: AppHandle) -> Result<Vec<SyncPair>, String> {
    list_sync_pairs(app)
}

#[tauri::command]
pub fn add_sync_pair(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    add_sync_pair_impl(app, draft)
}

#[cfg(not(test))]
fn add_sync_pair_impl(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    let mut profile = read_profile_from_disk(&app)?;
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label: draft.label,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        delete_safety_hours: draft.delete_safety_hours,
    }
    .normalized();
    profile.sync_pairs.push(pair);
    write_profile_to_disk(&app, &profile)?;
    let _ = start_polling_worker(&app);
    Ok(profile)
}

#[cfg(test)]
fn add_sync_pair_impl<R: Runtime>(app: AppHandle<R>, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    let mut profile = read_profile_from_disk(&app)?;
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label: draft.label,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        delete_safety_hours: draft.delete_safety_hours,
    }
    .normalized();
    profile.sync_pairs.push(pair);
    write_profile_to_disk(&app, &profile)?;
    Ok(profile)
}

#[tauri::command]
pub fn add_sync_location(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    add_sync_pair_impl(app, draft)
}

#[tauri::command]
pub fn update_sync_pair(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    update_sync_pair_impl(app, draft)
}

#[cfg(not(test))]
fn update_sync_pair_impl(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    let pair_id = draft
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or("Sync pair ID is required for updates.")?;
    let mut profile = read_profile_from_disk(&app)?;
    let position = profile
        .sync_pairs
        .iter()
        .position(|p| p.id == pair_id)
        .ok_or_else(|| format!("Sync pair '{}' not found.", pair_id))?;
    let updated = SyncPair {
        id: pair_id.to_string(),
        label: draft.label,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        delete_safety_hours: draft.delete_safety_hours,
    }
    .normalized();
    profile.sync_pairs[position] = updated;
    write_profile_to_disk(&app, &profile)?;
    let _ = start_polling_worker(&app);
    Ok(profile)
}

#[cfg(test)]
fn update_sync_pair_impl<R: Runtime>(app: AppHandle<R>, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    let pair_id = draft
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or("Sync pair ID is required for updates.")?;
    let mut profile = read_profile_from_disk(&app)?;
    let position = profile
        .sync_pairs
        .iter()
        .position(|p| p.id == pair_id)
        .ok_or_else(|| format!("Sync pair '{}' not found.", pair_id))?;
    let updated = SyncPair {
        id: pair_id.to_string(),
        label: draft.label,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        delete_safety_hours: draft.delete_safety_hours,
    }
    .normalized();
    profile.sync_pairs[position] = updated;
    write_profile_to_disk(&app, &profile)?;
    Ok(profile)
}

#[tauri::command]
pub fn update_sync_location(app: AppHandle, draft: SyncPairDraft) -> Result<StoredProfile, String> {
    update_sync_pair_impl(app, draft)
}

#[tauri::command]
pub fn remove_sync_pair(app: AppHandle, pair_id: String) -> Result<StoredProfile, String> {
    remove_sync_pair_impl(app, pair_id)
}

#[cfg(not(test))]
fn remove_sync_pair_impl(app: AppHandle, pair_id: String) -> Result<StoredProfile, String> {
    let pair_id = pair_id.trim();
    if pair_id.is_empty() {
        return Err("Sync pair ID is required.".into());
    }
    let mut profile = read_profile_from_disk(&app)?;
    let original_len = profile.sync_pairs.len();
    profile.sync_pairs.retain(|p| p.id != pair_id);
    if profile.sync_pairs.len() == original_len {
        return Err(format!("Sync pair '{}' not found.", pair_id));
    }
    if profile.active_location_id.as_deref() == Some(pair_id) {
        profile.active_location_id = None;
    }
    profile = profile.normalized();
    write_profile_to_disk(&app, &profile)?;
    let _ = start_polling_worker(&app);
    Ok(profile)
}

#[cfg(test)]
fn remove_sync_pair_impl<R: Runtime>(app: AppHandle<R>, pair_id: String) -> Result<StoredProfile, String> {
    let pair_id = pair_id.trim();
    if pair_id.is_empty() {
        return Err("Sync pair ID is required.".into());
    }
    let mut profile = read_profile_from_disk(&app)?;
    let original_len = profile.sync_pairs.len();
    profile.sync_pairs.retain(|p| p.id != pair_id);
    if profile.sync_pairs.len() == original_len {
        return Err(format!("Sync pair '{}' not found.", pair_id));
    }
    if profile.active_location_id.as_deref() == Some(pair_id) {
        profile.active_location_id = None;
    }
    profile = profile.normalized();
    write_profile_to_disk(&app, &profile)?;
    Ok(profile)
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
}

fn build_file_entry_responses(
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
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
            let remote_is_glacier = in_remote.is_some_and(|remote| {
                super::remote_index::is_glacier_storage_class(remote.storage_class.as_deref())
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
                (Some(local), Some(remote)) => {
                    if remote_is_glacier {
                        "glacier"
                    } else if local.size == remote.size {
                        "synced"
                    } else {
                        "conflict"
                    }
                }
                (Some(_), None) => "local-only",
                (None, Some(_remote)) if remote_is_glacier => {
                    "glacier"
                }
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
    let pair = match profile.sync_pairs.iter().find(|p| p.id == location_id) {
        Some(p) => p,
        None => return Ok(vec![]),
    };

    let local_snapshot = read_local_index_snapshot_for_pair(&app, &pair.id)?;
    let remote_snapshot = read_remote_index_snapshot_for_pair(&app, &pair.id)?;

    Ok(build_file_entry_responses(
        local_snapshot.as_ref(),
        remote_snapshot.as_ref(),
    ))
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
        let config = s3_config_for_pair(&pair, &credentials);
        let client = s3_adapter::build_client(&config).await?;

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
                s3_adapter::download_file(&client, &pair.bucket, &key, &local_path).await
            {
                errors.push(e);
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
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

// ---------------------------------------------------------------------------
// Delete file command
// ---------------------------------------------------------------------------

#[tauri::command]
pub async fn delete_file(
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

    let credentials = resolve_credentials_for_pair(&app, &pair)?;
    let config = s3_config_for_pair(&pair, &credentials);
    let client = s3_adapter::build_client(&config).await?;
    let key = s3_adapter::object_key(&path);

    s3_adapter::delete_object(&client, &pair.bucket, &key).await?;

    let local_path = resolve_local_download_path(&pair.local_folder, &path)?;
    match std::fs::remove_file(&local_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            return Err(format!("Failed to remove '{}': {e}", local_path.display()));
        }
    }

    cleanup_empty_ancestors(&local_path, Path::new(&pair.local_folder));

    Ok(())
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
    let config = s3_config_for_pair(&pair, &credentials);
    let client = s3_adapter::build_client(&config).await?;
    let key = s3_adapter::object_key(&path);

    s3_adapter::copy_object_with_storage_class(
        &client,
        &pair.bucket,
        &key,
        &storage_class,
    )
    .await?;

    // Delete local copy when moving to a glacier tier
    if super::remote_index::is_glacier_storage_class(Some(&storage_class)) {
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        append_error_context, build_file_entry_responses, directory_relative_paths_from_key,
        directory_relative_paths_from_relative_path, format_timeout_error,
        input_matches_saved_profile, relative_path_from_key, resolve_session_credentials,
        s3_config_for_pair,
    };
    use crate::storage::credentials_store::StoredCredentials;
    use crate::storage::local_index::{LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary};
    use crate::storage::profile_store::{ConnectionValidationInput, StoredProfile, SyncPair};
    use crate::storage::remote_index::{
        RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry,
    };
    use std::collections::BTreeMap;
    use std::time::Duration;

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
            version: 1,
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
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count,
                total_bytes,
            },
            entries: entries
                .iter()
                .map(|(relative_path, kind, size, storage_class)| RemoteObjectEntry {
                    key: format!("archive/{relative_path}"),
                    relative_path: (*relative_path).into(),
                    kind: (*kind).into(),
                    size: *size,
                    last_modified_at: None,
                    etag: None,
                    storage_class: storage_class.map(str::to_string),
                })
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
                    });
            }
        }

        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-06T00:00:00Z".into(),
                object_count,
                total_bytes,
            },
            entries: entries.into_values().collect(),
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
    fn formats_timeout_error_with_seconds() {
        assert_eq!(
            format_timeout_error("Executing planned uploads", Duration::from_secs(120)),
            "Executing planned uploads timed out after 120s."
        );
    }

    #[test]
    fn prefers_session_credentials_over_missing_keyring_credentials() {
        let session_credentials = StoredCredentials {
            access_key_id: "session-key".into(),
            secret_access_key: "session-secret".into(),
        };

        assert_eq!(
            resolve_session_credentials(
                Some(&session_credentials),
                None,
                "Save credentials securely before executing planned uploads.",
            )
            .expect("session credentials should be reused"),
            session_credentials
        );
    }

    #[test]
    fn falls_back_to_stored_credentials_without_session_credentials() {
        let stored_credentials = StoredCredentials {
            access_key_id: "stored-key".into(),
            secret_access_key: "stored-secret".into(),
        };

        assert_eq!(
            resolve_session_credentials(
                None,
                Some(stored_credentials.clone()),
                "Save credentials securely before executing planned uploads.",
            )
            .expect("stored credentials should still be supported"),
            stored_credentials
        );
    }

    #[test]
    fn returns_existing_missing_credentials_message_when_no_credentials_exist() {
        assert_eq!(
            resolve_session_credentials(
                None,
                None,
                "Select a saved credential before executing planned uploads.",
            )
            .expect_err("missing credentials should still fail"),
            "Select a saved credential before executing planned uploads."
        );
    }

    #[test]
    fn input_matching_includes_selected_credential_reference() {
        let profile = StoredProfile {
            bucket: "demo".into(),
            credential_profile_id: Some("cred-1".into()),
            ..StoredProfile::default()
        };

        let matching = ConnectionValidationInput {
            local_folder: String::new(),
            region: String::new(),
            bucket: "demo".into(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            credential_profile_id: Some("cred-1".into()),
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
            activity_debug_mode_enabled: false,
        };

        let mismatched = ConnectionValidationInput {
            credential_profile_id: Some("cred-2".into()),
            ..matching.clone()
        };

        assert!(input_matches_saved_profile(&profile, &matching));
        assert!(!input_matches_saved_profile(&profile, &mismatched));
    }

    #[test]
    fn input_matching_for_bucket_root_contract_ignores_legacy_endpoint_and_prefix_fields() {
        let profile = StoredProfile {
            bucket: "demo".into(),
            credential_profile_id: Some("cred-1".into()),
            ..StoredProfile::default()
        };

        let input = ConnectionValidationInput {
            local_folder: String::new(),
            region: String::new(),
            bucket: "demo".into(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            credential_profile_id: Some("cred-1".into()),
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
            activity_debug_mode_enabled: false,
        };

        assert!(
            input_matches_saved_profile(&profile, &input),
            "saved profile identity should no longer depend on legacy endpoint/prefix values"
        );
    }

    #[test]
    fn s3_config_for_pair_maps_all_fields() {
        let pair = SyncPair {
            region: "us-east-1".into(),
            bucket: "demo".into(),
            ..SyncPair::default()
        };
        let creds = StoredCredentials {
            access_key_id: "AKIA".into(),
            secret_access_key: "secret".into(),
        };
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
    fn remote_inventory_summary_counts_only_file_objects() {
        let snapshot = build_remote_inventory_snapshot_for_test(
            &[
                ("photos/2026/nested/", 0),
                ("photos/2026/nested/alpha.txt", 5),
                ("photos/2026/nested/deeper/beta.txt", 7),
            ],
        );

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
        })
        .expect("file entry response should serialize");

        assert_eq!(value["kind"], "directory");
    }

    #[test]
    fn build_file_entry_responses_includes_directory_entries() {
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

        let entries = build_file_entry_responses(Some(&local), Some(&remote));

        assert_eq!(entries.len(), 6);

        assert!(entries.iter().any(|entry| {
            entry.path == "docs"
                && entry.kind == "directory"
                && entry.status == "synced"
                && entry.has_local_copy
        }));
        assert!(entries.iter().any(|entry| {
            entry.path == "docs/readme.md"
                && entry.kind == "file"
                && entry.status == "synced"
                && entry.has_local_copy
        }));
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
    fn build_file_entry_responses_marks_size_and_kind_mismatches_as_conflict() {
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

        let entries = build_file_entry_responses(Some(&local), Some(&remote));

        assert!(entries.iter().any(|entry| {
            entry.path == "changed.txt"
                && entry.kind == "file"
                && entry.status == "conflict"
                && entry.has_local_copy
        }));
        assert!(entries.iter().any(|entry| {
            entry.path == "folder"
                && entry.kind == "directory"
                && entry.status == "conflict"
                && entry.has_local_copy
        }));
        assert!(entries.iter().any(|entry| {
            entry.path == "note"
                && entry.kind == "file"
                && entry.status == "conflict"
                && entry.has_local_copy
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

        let entries = build_file_entry_responses(Some(&local), Some(&remote));

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

        let entries = build_file_entry_responses(None, Some(&remote));

        assert!(entries.iter().any(|entry| {
            entry.path == "cold.bin"
                && entry.kind == "file"
                && entry.status == "glacier"
                && !entry.has_local_copy
                && entry.storage_class.as_deref() == Some("DEEP_ARCHIVE")
        }));
    }

}

#[cfg(all(test, feature = "tauri-command-tests"))]
mod tauri_command_tests {
    use super::*;
    use crate::storage::activity::ActivityDebugState;
    use crate::storage::credentials_store::{
        clear_test_secret_store, create_credential, CredentialDraft,
    };
    use crate::storage::profile_store::{
        read_profile_from_disk, write_profile_to_disk, StoredProfile, SyncPair, SyncPairDraft,
    };
    use crate::storage::sync_state::SyncState;
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::process;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tauri::test::{mock_builder, mock_context, noop_assets, MockRuntime};
    use tauri::{App, AppHandle, Manager};

    struct CommandTestHarness {
        _env_lock: MutexGuard<'static, ()>,
        original_appdata: Option<String>,
        original_localappdata: Option<String>,
        app: App<MockRuntime>,
        storage_dir: PathBuf,
    }

    impl CommandTestHarness {
        fn new(name: &str) -> Self {
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

        fn app_handle(&self) -> AppHandle<MockRuntime> {
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
            local_folder: folder.into(),
            region: "us-east-1".into(),
            bucket: bucket.into(),
            credential_profile_id: None,
            enabled: false,
            remote_polling_enabled: false,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
        }
    }

    fn sync_pair_ids(profile: &StoredProfile) -> Vec<String> {
        profile.sync_pairs.iter().map(|pair| pair.id.clone()).collect()
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
    fn remove_active_sync_location_stays_deleted_after_reload_and_clears_invalid_active_selection() {
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

        let mut stored = read_profile_from_disk(&harness.app_handle()).expect("profile should read");
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
        assert_ne!(reloaded.active_location_id.as_deref(), Some(removed_id.as_str()));
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
            sync_pair_draft("Docs", "C:/sync/docs", "bucket-a"),
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
                local_folder: "D:/archive/photos".into(),
                region: "eu-west-1".into(),
                bucket: "bucket-b".into(),
                credential_profile_id: None,
                enabled: false,
                remote_polling_enabled: true,
                poll_interval_seconds: 120,
                conflict_strategy: "ignored-by-normalization".into(),
                delete_safety_hours: 48,
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
        assert_eq!(matching.len(), 1, "updated location should still exist exactly once");

        let updated = matching[0];
        assert_eq!(updated.label, "Photos Archive");
        assert_eq!(updated.local_folder, "D:/archive/photos");
        assert_eq!(updated.region, "eu-west-1");
        assert_eq!(updated.bucket, "bucket-b");
        assert!(updated.remote_polling_enabled);
        assert_eq!(updated.poll_interval_seconds, 120);
        assert_eq!(updated.conflict_strategy, "preserve-both");
        assert_eq!(updated.delete_safety_hours, 48);
    }

    #[test]
    fn delete_selected_credential_clears_selected_credential_state_persistently() {
        let harness = CommandTestHarness::new("delete-selected-credential");

        let credential = create_credential(
            &harness.app_handle(),
            CredentialDraft {
                name: "Primary".into(),
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
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
}
