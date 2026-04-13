use std::{fs, path::Path};

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use tauri::{AppHandle, Runtime};

use super::{
    app_storage_path,
    profile_store::{StoredProfile, SyncPair},
    sync_planner::SyncPlan,
    SYNC_DB_FILE_NAME,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DurablePlannerSummary {
    pub last_planned_at: Option<String>,
    pub observed_path_count: u64,
    pub upload_count: u64,
    pub create_directory_count: u64,
    pub download_count: u64,
    pub conflict_count: u64,
    pub noop_count: u64,
    pub pending_operation_count: u64,
    pub credentials_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct QueueRecoverySummary {
    pub recovered_upload_count: u64,
    pub recovered_download_count: u64,
}

impl QueueRecoverySummary {
    pub fn recovered_count(&self) -> u64 {
        self.recovered_upload_count + self.recovered_download_count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedUploadQueueItem {
    pub id: i64,
    pub path: String,
    pub operation: String,
    pub local_size: Option<u64>,
    pub remote_size: Option<u64>,
    pub expected_local_fingerprint: Option<String>,
    pub expected_remote_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedDownloadQueueItem {
    pub id: i64,
    pub path: String,
    pub local_size: Option<u64>,
    pub remote_size: Option<u64>,
    pub expected_local_fingerprint: Option<String>,
    pub expected_remote_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncAnchor {
    pub path: String,
    pub kind: String,
    pub local_fingerprint: Option<String>,
    pub remote_etag: Option<String>,
    pub synced_at: String,
}

pub fn load_planner_summary(
    app: &AppHandle,
    profile: &StoredProfile,
) -> Result<DurablePlannerSummary, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_planner_summary_from_path(&path, &profile_key(profile))
}

pub fn persist_sync_plan(
    app: &AppHandle,
    profile: &StoredProfile,
    plan: &SyncPlan,
) -> Result<DurablePlannerSummary, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    persist_sync_plan_to_path(&path, &profile_key(profile), plan)
}

pub fn load_planned_upload_queue(
    app: &AppHandle,
    profile: &StoredProfile,
) -> Result<Vec<PlannedUploadQueueItem>, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_planned_upload_queue_from_path(&path, &profile_key(profile))
}

pub fn load_sync_anchors(
    app: &AppHandle,
    profile: &StoredProfile,
) -> Result<Vec<SyncAnchor>, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_sync_anchors_from_path(&path, &profile_key(profile))
}

pub fn upsert_sync_anchor(
    app: &AppHandle,
    profile: &StoredProfile,
    anchor: &SyncAnchor,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    upsert_sync_anchor_at_path(&path, &profile_key(profile), anchor)
}

pub fn recover_interrupted_queue_items(
    app: &AppHandle,
    profile: &StoredProfile,
    recovered_at: &str,
) -> Result<QueueRecoverySummary, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    recover_interrupted_queue_items_at_path(&path, &profile_key(profile), recovered_at)
}

pub fn mark_upload_queue_item_in_progress(
    app: &AppHandle,
    profile: &StoredProfile,
    queue_item_id: i64,
    started_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_upload_queue_item_in_progress_at_path(
        &path,
        &profile_key(profile),
        queue_item_id,
        started_at,
    )
}

pub fn mark_upload_queue_item_completed(
    app: &AppHandle,
    profile: &StoredProfile,
    queue_item_id: i64,
    finished_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_upload_queue_item_completed_at_path(
        &path,
        &profile_key(profile),
        queue_item_id,
        finished_at,
    )
}

pub fn mark_upload_queue_item_failed(
    app: &AppHandle,
    profile: &StoredProfile,
    queue_item_id: i64,
    finished_at: &str,
    error_message: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_upload_queue_item_failed_at_path(
        &path,
        &profile_key(profile),
        queue_item_id,
        finished_at,
        error_message,
    )
}

pub fn load_planned_download_queue(
    app: &AppHandle,
    profile: &StoredProfile,
) -> Result<Vec<PlannedDownloadQueueItem>, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_planned_download_queue_from_path(&path, &profile_key(profile))
}

pub fn load_sync_anchors_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<Vec<SyncAnchor>, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_sync_anchors_from_path(&path, &sync_pair_key(pair))
}

pub fn upsert_sync_anchor_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    anchor: &SyncAnchor,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    upsert_sync_anchor_at_path(&path, &sync_pair_key(pair), anchor)
}

pub fn mark_download_queue_item_in_progress(
    app: &AppHandle,
    profile: &StoredProfile,
    queue_item_id: i64,
    started_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_download_queue_item_in_progress_at_path(
        &path,
        &profile_key(profile),
        queue_item_id,
        started_at,
    )
}

pub fn mark_download_queue_item_completed(
    app: &AppHandle,
    profile: &StoredProfile,
    queue_item_id: i64,
    finished_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_download_queue_item_completed_at_path(
        &path,
        &profile_key(profile),
        queue_item_id,
        finished_at,
    )
}

pub fn mark_download_queue_item_failed(
    app: &AppHandle,
    profile: &StoredProfile,
    queue_item_id: i64,
    finished_at: &str,
    error_message: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_download_queue_item_failed_at_path(
        &path,
        &profile_key(profile),
        queue_item_id,
        finished_at,
        error_message,
    )
}

// Per-pair variants — accept &SyncPair instead of &StoredProfile

pub fn load_planner_summary_for_pair<R: tauri::Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<DurablePlannerSummary, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_planner_summary_from_path(&path, &sync_pair_key(pair))
}

pub fn persist_sync_plan_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    plan: &SyncPlan,
) -> Result<DurablePlannerSummary, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    persist_sync_plan_to_path(&path, &sync_pair_key(pair), plan)
}

pub fn load_planned_upload_queue_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<Vec<PlannedUploadQueueItem>, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_planned_upload_queue_from_path(&path, &sync_pair_key(pair))
}

pub fn recover_interrupted_queue_items_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    recovered_at: &str,
) -> Result<QueueRecoverySummary, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    recover_interrupted_queue_items_at_path(&path, &sync_pair_key(pair), recovered_at)
}

pub fn mark_upload_queue_item_in_progress_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    queue_item_id: i64,
    started_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_upload_queue_item_in_progress_at_path(
        &path,
        &sync_pair_key(pair),
        queue_item_id,
        started_at,
    )
}

pub fn mark_upload_queue_item_completed_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    queue_item_id: i64,
    finished_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_upload_queue_item_completed_at_path(
        &path,
        &sync_pair_key(pair),
        queue_item_id,
        finished_at,
    )
}

pub fn mark_upload_queue_item_failed_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    queue_item_id: i64,
    finished_at: &str,
    error_message: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_upload_queue_item_failed_at_path(
        &path,
        &sync_pair_key(pair),
        queue_item_id,
        finished_at,
        error_message,
    )
}

pub fn load_planned_download_queue_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<Vec<PlannedDownloadQueueItem>, String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    load_planned_download_queue_from_path(&path, &sync_pair_key(pair))
}

pub fn mark_download_queue_item_in_progress_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    queue_item_id: i64,
    started_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_download_queue_item_in_progress_at_path(
        &path,
        &sync_pair_key(pair),
        queue_item_id,
        started_at,
    )
}

pub fn mark_download_queue_item_completed_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    queue_item_id: i64,
    finished_at: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_download_queue_item_completed_at_path(
        &path,
        &sync_pair_key(pair),
        queue_item_id,
        finished_at,
    )
}

pub fn mark_download_queue_item_failed_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    queue_item_id: i64,
    finished_at: &str,
    error_message: &str,
) -> Result<(), String> {
    let path = app_storage_path(app, SYNC_DB_FILE_NAME)?;
    mark_download_queue_item_failed_at_path(
        &path,
        &sync_pair_key(pair),
        queue_item_id,
        finished_at,
        error_message,
    )
}

fn load_planner_summary_from_path(
    path: &Path,
    profile_key: &str,
) -> Result<DurablePlannerSummary, String> {
    let connection = open_connection(path)?;
    let mut statement = connection
        .prepare(
            "SELECT planned_at, observed_path_count, upload_count, create_directory_count, download_count, conflict_count, noop_count, pending_operation_count, used_stored_credentials
             FROM plan_runs
             WHERE profile_key = ?1
             ORDER BY id DESC
             LIMIT 1",
        )
        .map_err(|error| format!("failed to prepare planner summary query: {error}"))?;

    statement
        .query_row(params![profile_key], |row| {
            Ok(DurablePlannerSummary {
                last_planned_at: row.get(0)?,
                observed_path_count: i64_to_u64(row.get(1)?)?,
                upload_count: i64_to_u64(row.get(2)?)?,
                create_directory_count: i64_to_u64(row.get(3)?)?,
                download_count: i64_to_u64(row.get(4)?)?,
                conflict_count: i64_to_u64(row.get(5)?)?,
                noop_count: i64_to_u64(row.get(6)?)?,
                pending_operation_count: i64_to_u64(row.get(7)?)?,
                credentials_available: row.get::<_, i64>(8)? != 0,
            })
        })
        .optional()
        .map_err(|error| format!("failed to load planner summary: {error}"))?
        .map(Ok)
        .unwrap_or_else(|| Ok(DurablePlannerSummary::default()))
}

fn load_planned_upload_queue_from_path(
    path: &Path,
    profile_key: &str,
) -> Result<Vec<PlannedUploadQueueItem>, String> {
    let connection = open_connection(path)?;
    let mut statement = connection
        .prepare(
            "SELECT id, path, operation, local_size, remote_size, expected_local_fingerprint, expected_remote_etag
             FROM sync_queue
             WHERE profile_key = ?1 AND operation IN ('upload', 'create_directory') AND queue_status IN ('planned', 'interrupted')
             ORDER BY id ASC",
        )
        .map_err(|error| format!("failed to prepare planned upload queue query: {error}"))?;

    let rows = statement
        .query_map(params![profile_key], |row| {
            Ok(PlannedUploadQueueItem {
                id: row.get(0)?,
                path: row.get(1)?,
                operation: row.get(2)?,
                local_size: optional_i64_to_u64(row.get(3)?)?,
                remote_size: optional_i64_to_u64(row.get(4)?)?,
                expected_local_fingerprint: row.get(5)?,
                expected_remote_etag: row.get(6)?,
            })
        })
        .map_err(|error| format!("failed to load planned upload queue: {error}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to parse planned upload queue row: {error}"))
}

fn persist_sync_plan_to_path(
    path: &Path,
    profile_key: &str,
    plan: &SyncPlan,
) -> Result<DurablePlannerSummary, String> {
    let mut connection = open_connection(path)?;
    let transaction = connection
        .transaction()
        .map_err(|error| format!("failed to start planner transaction: {error}"))?;

    transaction
        .execute(
            "INSERT INTO plan_runs (
                profile_key,
                planned_at,
                run_status,
                superseded_at,
                superseded_by_run_id,
                observed_path_count,
                upload_count,
                create_directory_count,
                download_count,
                conflict_count,
                noop_count,
                pending_operation_count,
                used_stored_credentials
            ) VALUES (?1, ?2, ?3, NULL, NULL, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                profile_key,
                plan.summary.planned_at,
                if plan.queue_items.is_empty() {
                    "completed"
                } else {
                    "active"
                },
                u64_to_i64(plan.summary.observed_path_count)?,
                u64_to_i64(plan.summary.upload_count)?,
                u64_to_i64(plan.summary.create_directory_count)?,
                u64_to_i64(plan.summary.download_count)?,
                u64_to_i64(plan.summary.conflict_count)?,
                u64_to_i64(plan.summary.noop_count)?,
                u64_to_i64(plan.summary.pending_operation_count)?,
                if plan.summary.credentials_available {
                    1_i64
                } else {
                    0_i64
                },
            ],
        )
        .map_err(|error| format!("failed to insert planner run: {error}"))?;

    let plan_run_id = transaction.last_insert_rowid();

    transaction
        .execute(
            "UPDATE plan_runs
             SET run_status = 'superseded',
                 superseded_at = ?2,
                 superseded_by_run_id = ?3
             WHERE profile_key = ?1
               AND id != ?3
               AND run_status != 'superseded'
               AND EXISTS (
                   SELECT 1
                   FROM sync_queue queue
                   WHERE queue.plan_run_id = plan_runs.id
                     AND queue.queue_status IN ('planned', 'in_progress', 'interrupted')
               )",
            params![profile_key, plan.summary.planned_at, plan_run_id],
        )
        .map_err(|error| format!("failed to supersede prior planner runs: {error}"))?;

    transaction
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'superseded',
                 finished_at = ?2,
                 last_error = CASE
                     WHEN last_error IS NULL OR last_error = ''
                         THEN 'Superseded by a newer sync plan before execution completed.'
                     ELSE last_error
                 END
             WHERE profile_key = ?1
               AND plan_run_id != ?3
               AND queue_status IN ('planned', 'in_progress', 'interrupted')",
            params![profile_key, plan.summary.planned_at, plan_run_id],
        )
        .map_err(|error| format!("failed to supersede prior queue items: {error}"))?;

    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO observed_entries (
                    profile_key,
                    path,
                    local_size,
                    remote_size,
                    resolution,
                    observed_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                ON CONFLICT(profile_key, path) DO UPDATE SET
                    local_size = excluded.local_size,
                    remote_size = excluded.remote_size,
                    resolution = excluded.resolution,
                    observed_at = excluded.observed_at",
            )
            .map_err(|error| format!("failed to prepare observed entry insert: {error}"))?;

        for entry in &plan.observed_entries {
            statement
                .execute(params![
                    profile_key,
                    entry.path,
                    option_u64_to_i64(entry.local_size)?,
                    option_u64_to_i64(entry.remote_size)?,
                    entry.resolution,
                    plan.summary.planned_at,
                ])
                .map_err(|error| {
                    format!("failed to persist observed entry '{}': {error}", entry.path)
                })?;
        }
    }

    {
        let mut statement = transaction
            .prepare(
                "INSERT INTO sync_queue (
                    plan_run_id,
                    profile_key,
                    path,
                    operation,
                    local_size,
                    remote_size,
                    expected_local_fingerprint,
                    expected_remote_etag,
                    queue_status,
                    created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'planned', ?9)",
            )
            .map_err(|error| format!("failed to prepare queue insert: {error}"))?;

        for item in &plan.queue_items {
            statement
                .execute(params![
                    plan_run_id,
                    profile_key,
                    item.path,
                    item.operation,
                    option_u64_to_i64(item.local_size)?,
                    option_u64_to_i64(item.remote_size)?,
                    item.expected_local_fingerprint,
                    item.expected_remote_etag,
                    plan.summary.planned_at,
                ])
                .map_err(|error| {
                    format!("failed to persist sync queue item '{}': {error}", item.path)
                })?;
        }
    }

    transaction
        .commit()
        .map_err(|error| format!("failed to commit planner transaction: {error}"))?;

    let connection = open_connection(path)?;
    refresh_plan_run_statuses_for_profile(&connection, profile_key)?;

    Ok(DurablePlannerSummary {
        last_planned_at: Some(plan.summary.planned_at.clone()),
        observed_path_count: plan.summary.observed_path_count,
        upload_count: plan.summary.upload_count,
        create_directory_count: plan.summary.create_directory_count,
        download_count: plan.summary.download_count,
        conflict_count: plan.summary.conflict_count,
        noop_count: plan.summary.noop_count,
        pending_operation_count: plan.summary.pending_operation_count,
        credentials_available: plan.summary.credentials_available,
    })
}

fn mark_upload_queue_item_in_progress_at_path(
    path: &Path,
    profile_key: &str,
    queue_item_id: i64,
    started_at: &str,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    let changed = connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'in_progress',
                 started_at = ?3,
                 finished_at = NULL,
                 last_error = NULL
             WHERE id = ?1
               AND profile_key = ?2
               AND operation IN ('upload', 'create_directory')
               AND queue_status IN ('planned', 'interrupted')",
            params![queue_item_id, profile_key, started_at],
        )
        .map_err(|error| format!("failed to mark upload queue item in progress: {error}"))?;

    if changed == 1 {
        refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
        Ok(())
    } else {
        Err(format!(
            "upload queue item '{queue_item_id}' is no longer eligible for execution"
        ))
    }
}

fn mark_upload_queue_item_completed_at_path(
    path: &Path,
    profile_key: &str,
    queue_item_id: i64,
    finished_at: &str,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    let changed = connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'completed',
                 finished_at = ?3,
                 last_error = NULL
             WHERE id = ?1
               AND profile_key = ?2
               AND operation IN ('upload', 'create_directory')
               AND queue_status = 'in_progress'",
            params![queue_item_id, profile_key, finished_at],
        )
        .map_err(|error| format!("failed to mark upload queue item completed: {error}"))?;

    if changed == 1 {
        refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
        Ok(())
    } else {
        Err(format!(
            "upload queue item '{queue_item_id}' could not be completed from its current state"
        ))
    }
}

fn mark_upload_queue_item_failed_at_path(
    path: &Path,
    profile_key: &str,
    queue_item_id: i64,
    finished_at: &str,
    error_message: &str,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    let changed = connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'failed',
                 finished_at = ?3,
                 last_error = ?4
             WHERE id = ?1
               AND profile_key = ?2
               AND operation IN ('upload', 'create_directory')
               AND queue_status IN ('planned', 'in_progress', 'interrupted')",
            params![queue_item_id, profile_key, finished_at, error_message],
        )
        .map_err(|error| format!("failed to mark upload queue item failed: {error}"))?;

    if changed == 1 {
        refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
        Ok(())
    } else {
        Err(format!(
            "upload queue item '{queue_item_id}' could not be marked failed from its current state"
        ))
    }
}

fn load_planned_download_queue_from_path(
    path: &Path,
    profile_key: &str,
) -> Result<Vec<PlannedDownloadQueueItem>, String> {
    let connection = open_connection(path)?;
    let mut statement = connection
        .prepare(
            "SELECT id, path, local_size, remote_size
             , expected_local_fingerprint, expected_remote_etag
             FROM sync_queue
             WHERE profile_key = ?1 AND operation = 'download' AND queue_status IN ('planned', 'interrupted')
             ORDER BY id ASC",
        )
        .map_err(|error| format!("failed to prepare planned download queue query: {error}"))?;

    let rows = statement
        .query_map(params![profile_key], |row| {
            Ok(PlannedDownloadQueueItem {
                id: row.get(0)?,
                path: row.get(1)?,
                local_size: optional_i64_to_u64(row.get(2)?)?,
                remote_size: optional_i64_to_u64(row.get(3)?)?,
                expected_local_fingerprint: row.get(4)?,
                expected_remote_etag: row.get(5)?,
            })
        })
        .map_err(|error| format!("failed to load planned download queue: {error}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to parse planned download queue row: {error}"))
}

fn mark_download_queue_item_in_progress_at_path(
    path: &Path,
    profile_key: &str,
    queue_item_id: i64,
    started_at: &str,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    let changed = connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'in_progress',
                 started_at = ?3,
                 finished_at = NULL,
                 last_error = NULL
             WHERE id = ?1
               AND profile_key = ?2
               AND operation = 'download'
               AND queue_status IN ('planned', 'interrupted')",
            params![queue_item_id, profile_key, started_at],
        )
        .map_err(|error| format!("failed to mark download queue item in progress: {error}"))?;

    if changed == 1 {
        refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
        Ok(())
    } else {
        Err(format!(
            "download queue item '{queue_item_id}' is no longer eligible for execution"
        ))
    }
}

fn mark_download_queue_item_completed_at_path(
    path: &Path,
    profile_key: &str,
    queue_item_id: i64,
    finished_at: &str,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    let changed = connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'completed',
                 finished_at = ?3,
                 last_error = NULL
             WHERE id = ?1
               AND profile_key = ?2
               AND operation = 'download'
               AND queue_status = 'in_progress'",
            params![queue_item_id, profile_key, finished_at],
        )
        .map_err(|error| format!("failed to mark download queue item completed: {error}"))?;

    if changed == 1 {
        refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
        Ok(())
    } else {
        Err(format!(
            "download queue item '{queue_item_id}' could not be completed from its current state"
        ))
    }
}

fn mark_download_queue_item_failed_at_path(
    path: &Path,
    profile_key: &str,
    queue_item_id: i64,
    finished_at: &str,
    error_message: &str,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    let changed = connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'failed',
                 finished_at = ?3,
                 last_error = ?4
             WHERE id = ?1
               AND profile_key = ?2
               AND operation = 'download'
               AND queue_status IN ('planned', 'in_progress', 'interrupted')",
            params![queue_item_id, profile_key, finished_at, error_message],
        )
        .map_err(|error| format!("failed to mark download queue item failed: {error}"))?;

    if changed == 1 {
        refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
        Ok(())
    } else {
        Err(format!(
            "download queue item '{queue_item_id}' could not be marked failed from its current state"
        ))
    }
}

fn open_connection(path: &Path) -> Result<Connection, String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("failed to create sync database directory: {error}"))?;
    }

    let connection = Connection::open(path)
        .map_err(|error| format!("failed to open sync database '{}': {error}", path.display()))?;
    initialize_schema(&connection)?;
    Ok(connection)
}

fn initialize_schema(connection: &Connection) -> Result<(), String> {
    connection
        .execute_batch(
            "PRAGMA foreign_keys = ON;

             CREATE TABLE IF NOT EXISTS plan_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_key TEXT NOT NULL,
                planned_at TEXT NOT NULL,
                run_status TEXT NOT NULL DEFAULT 'completed',
                superseded_at TEXT,
                superseded_by_run_id INTEGER,
                observed_path_count INTEGER NOT NULL,
                upload_count INTEGER NOT NULL,
                create_directory_count INTEGER NOT NULL DEFAULT 0,
                download_count INTEGER NOT NULL,
                conflict_count INTEGER NOT NULL,
                noop_count INTEGER NOT NULL,
                pending_operation_count INTEGER NOT NULL,
                used_stored_credentials INTEGER NOT NULL
             );

             CREATE INDEX IF NOT EXISTS idx_plan_runs_profile_key ON plan_runs(profile_key, id DESC);

             CREATE TABLE IF NOT EXISTS observed_entries (
                profile_key TEXT NOT NULL,
                path TEXT NOT NULL,
                local_size INTEGER,
                remote_size INTEGER,
                resolution TEXT NOT NULL,
                observed_at TEXT NOT NULL,
                PRIMARY KEY (profile_key, path)
             );

              CREATE TABLE IF NOT EXISTS sync_queue (
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  plan_run_id INTEGER NOT NULL,
                  profile_key TEXT NOT NULL,
                  path TEXT NOT NULL,
                  operation TEXT NOT NULL,
                  local_size INTEGER,
                  remote_size INTEGER,
                  expected_local_fingerprint TEXT,
                  expected_remote_etag TEXT,
                  queue_status TEXT NOT NULL,
                  started_at TEXT,
                  finished_at TEXT,
                  last_error TEXT,
                  created_at TEXT NOT NULL,
                  FOREIGN KEY(plan_run_id) REFERENCES plan_runs(id) ON DELETE CASCADE
               );

              CREATE TABLE IF NOT EXISTS sync_anchors (
                  profile_key TEXT NOT NULL,
                  path TEXT NOT NULL,
                  kind TEXT NOT NULL,
                  local_fingerprint TEXT,
                  remote_etag TEXT,
                  synced_at TEXT NOT NULL,
                  PRIMARY KEY (profile_key, path)
              );

               CREATE INDEX IF NOT EXISTS idx_sync_queue_profile_key ON sync_queue(profile_key, queue_status, path);",
        )
        .map_err(|error| format!("failed to initialize sync database schema: {error}"))?;

    ensure_sync_queue_column(connection, "started_at", "TEXT")?;
    ensure_sync_queue_column(connection, "finished_at", "TEXT")?;
    ensure_sync_queue_column(connection, "last_error", "TEXT")?;
    ensure_sync_queue_column(connection, "expected_local_fingerprint", "TEXT")?;
    ensure_sync_queue_column(connection, "expected_remote_etag", "TEXT")?;
    ensure_plan_runs_column(
        connection,
        "create_directory_count",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_plan_runs_column(
        connection,
        "run_status",
        "TEXT NOT NULL DEFAULT 'completed'",
    )?;
    ensure_plan_runs_column(connection, "superseded_at", "TEXT")?;
    ensure_plan_runs_column(connection, "superseded_by_run_id", "INTEGER")?;

    refresh_plan_run_statuses(connection)?;

    Ok(())
}

fn recover_interrupted_queue_items_at_path(
    path: &Path,
    profile_key: &str,
    recovered_at: &str,
) -> Result<QueueRecoverySummary, String> {
    let connection = open_connection(path)?;
    let summary =
        recover_interrupted_queue_items_for_profile(&connection, profile_key, recovered_at)?;
    refresh_plan_run_statuses_for_profile(&connection, profile_key)?;
    Ok(summary)
}

fn recover_interrupted_queue_items_for_profile(
    connection: &Connection,
    profile_key: &str,
    recovered_at: &str,
) -> Result<QueueRecoverySummary, String> {
    let mut statement = connection
        .prepare(
            "SELECT operation, COUNT(*)
             FROM sync_queue
             WHERE profile_key = ?1 AND queue_status = 'in_progress'
             GROUP BY operation",
        )
        .map_err(|error| format!("failed to prepare interrupted queue recovery query: {error}"))?;

    let mut summary = QueueRecoverySummary::default();
    let rows = statement
        .query_map(params![profile_key], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })
        .map_err(|error| format!("failed to query interrupted queue items: {error}"))?;

    for row in rows {
        let (operation, count) =
            row.map_err(|error| format!("failed to parse interrupted queue item count: {error}"))?;
        let count = i64_to_u64(count)
            .map_err(|error| format!("failed to convert interrupted queue item count: {error}"))?;
        match operation.as_str() {
            "download" => summary.recovered_download_count += count,
            "upload" | "create_directory" => summary.recovered_upload_count += count,
            _ => {}
        }
    }

    if summary.recovered_count() == 0 {
        return Ok(summary);
    }

    connection
        .execute(
            "UPDATE sync_queue
             SET queue_status = 'interrupted',
                 finished_at = ?2,
                 last_error = CASE
                     WHEN last_error IS NULL OR last_error = ''
                         THEN 'Execution was interrupted before completion; item is safe to retry.'
                     ELSE last_error
                 END
             WHERE profile_key = ?1
               AND queue_status = 'in_progress'",
            params![profile_key, recovered_at],
        )
        .map_err(|error| format!("failed to recover interrupted queue items: {error}"))?;

    Ok(summary)
}

fn refresh_plan_run_statuses_for_profile(
    connection: &Connection,
    profile_key: &str,
) -> Result<(), String> {
    connection
        .execute(
            "UPDATE plan_runs
             SET run_status = CASE
                 WHEN run_status = 'superseded' THEN 'superseded'
                 WHEN EXISTS (
                     SELECT 1
                     FROM sync_queue queue
                     WHERE queue.plan_run_id = plan_runs.id
                       AND queue.queue_status = 'in_progress'
                 ) THEN 'executing'
                 WHEN EXISTS (
                     SELECT 1
                     FROM sync_queue queue
                     WHERE queue.plan_run_id = plan_runs.id
                       AND queue.queue_status IN ('planned', 'interrupted')
                 ) THEN 'active'
                 WHEN EXISTS (
                     SELECT 1
                     FROM sync_queue queue
                     WHERE queue.plan_run_id = plan_runs.id
                 ) THEN 'completed'
                 ELSE run_status
             END
             WHERE profile_key = ?1",
            params![profile_key],
        )
        .map_err(|error| format!("failed to refresh planner run status: {error}"))?;

    Ok(())
}

fn refresh_plan_run_statuses(connection: &Connection) -> Result<(), String> {
    connection
        .execute(
            "UPDATE plan_runs
             SET run_status = CASE
                 WHEN run_status = 'superseded' THEN 'superseded'
                 WHEN EXISTS (
                     SELECT 1
                     FROM sync_queue queue
                     WHERE queue.plan_run_id = plan_runs.id
                       AND queue.queue_status = 'in_progress'
                 ) THEN 'executing'
                 WHEN EXISTS (
                     SELECT 1
                     FROM sync_queue queue
                     WHERE queue.plan_run_id = plan_runs.id
                       AND queue.queue_status IN ('planned', 'interrupted')
                 ) THEN 'active'
                 WHEN EXISTS (
                     SELECT 1
                     FROM sync_queue queue
                     WHERE queue.plan_run_id = plan_runs.id
                 ) THEN 'completed'
                 ELSE run_status
             END",
            [],
        )
        .map_err(|error| format!("failed to refresh planner run statuses: {error}"))?;

    Ok(())
}

fn ensure_plan_runs_column(
    connection: &Connection,
    column_name: &str,
    column_definition: &str,
) -> Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(plan_runs)")
        .map_err(|error| format!("failed to inspect plan_runs schema: {error}"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| format!("failed to inspect plan_runs columns: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to parse plan_runs schema: {error}"))?;

    if columns.iter().any(|existing| existing == column_name) {
        return Ok(());
    }

    connection
        .execute(
            &format!("ALTER TABLE plan_runs ADD COLUMN {column_name} {column_definition}"),
            [],
        )
        .map_err(|error| format!("failed to migrate plan_runs.{column_name}: {error}"))?;

    Ok(())
}

fn ensure_sync_queue_column(
    connection: &Connection,
    column_name: &str,
    column_definition: &str,
) -> Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(sync_queue)")
        .map_err(|error| format!("failed to inspect sync_queue schema: {error}"))?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| format!("failed to inspect sync_queue columns: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to parse sync_queue schema: {error}"))?;

    if columns.iter().any(|existing| existing == column_name) {
        return Ok(());
    }

    connection
        .execute(
            &format!("ALTER TABLE sync_queue ADD COLUMN {column_name} {column_definition}"),
            [],
        )
        .map_err(|error| format!("failed to migrate sync_queue.{column_name}: {error}"))?;

    Ok(())
}

fn load_sync_anchors_from_path(path: &Path, profile_key: &str) -> Result<Vec<SyncAnchor>, String> {
    let connection = open_connection(path)?;
    let mut statement = connection
        .prepare(
            "SELECT path, kind, local_fingerprint, remote_etag, synced_at
             FROM sync_anchors
             WHERE profile_key = ?1
             ORDER BY path ASC",
        )
        .map_err(|error| format!("failed to prepare sync anchor query: {error}"))?;

    let rows = statement
        .query_map(params![profile_key], |row| {
            Ok(SyncAnchor {
                path: row.get(0)?,
                kind: row.get(1)?,
                local_fingerprint: row.get(2)?,
                remote_etag: row.get(3)?,
                synced_at: row.get(4)?,
            })
        })
        .map_err(|error| format!("failed to load sync anchors: {error}"))?;

    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to parse sync anchor row: {error}"))
}

fn upsert_sync_anchor_at_path(
    path: &Path,
    profile_key: &str,
    anchor: &SyncAnchor,
) -> Result<(), String> {
    let connection = open_connection(path)?;
    connection
        .execute(
            "INSERT INTO sync_anchors (
                profile_key,
                path,
                kind,
                local_fingerprint,
                remote_etag,
                synced_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(profile_key, path) DO UPDATE SET
                kind = excluded.kind,
                local_fingerprint = excluded.local_fingerprint,
                remote_etag = excluded.remote_etag,
                synced_at = excluded.synced_at",
            params![
                profile_key,
                anchor.path,
                anchor.kind,
                anchor.local_fingerprint,
                anchor.remote_etag,
                anchor.synced_at,
            ],
        )
        .map_err(|error| format!("failed to upsert sync anchor '{}': {error}", anchor.path))?;
    Ok(())
}

fn profile_key(profile: &StoredProfile) -> String {
    format!("{}|{}", profile.local_folder.trim(), profile.bucket.trim(),)
}

fn sync_pair_key(pair: &SyncPair) -> String {
    format!("{}|{}", pair.local_folder.trim(), pair.bucket.trim(),)
}

fn i64_to_u64(value: i64) -> rusqlite::Result<u64> {
    u64::try_from(value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Integer,
            Box::new(error),
        )
    })
}

fn optional_i64_to_u64(value: Option<i64>) -> rusqlite::Result<Option<u64>> {
    value.map(i64_to_u64).transpose()
}

fn u64_to_i64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("value '{value}' exceeds SQLite integer range"))
}

fn option_u64_to_i64(value: Option<u64>) -> Result<Option<i64>, String> {
    value.map(u64_to_i64).transpose()
}

#[cfg(test)]
mod tests {
    use super::{
        load_planned_download_queue_from_path, load_planned_upload_queue_from_path,
        load_planner_summary_from_path, load_sync_anchors_from_path,
        mark_download_queue_item_completed_at_path, mark_download_queue_item_in_progress_at_path,
        mark_upload_queue_item_completed_at_path, mark_upload_queue_item_failed_at_path,
        mark_upload_queue_item_in_progress_at_path, open_connection, persist_sync_plan_to_path,
        profile_key, recover_interrupted_queue_items_at_path, sync_pair_key,
        upsert_sync_anchor_at_path, SyncAnchor,
    };
    use crate::storage::profile_store::{StoredProfile, SyncPair};
    use crate::storage::sync_planner::{
        ObservedEntry, PlannedQueueItem, SyncPlan, SyncPlanSummary,
    };
    use rusqlite::{params, Connection};
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
        env::temp_dir().join(format!(
            "storage-goblin-{name}-{}-{suffix}.sqlite3",
            process::id()
        ))
    }

    #[test]
    fn persists_and_loads_latest_planner_summary() {
        let db_path = temp_path("planner-summary");
        let profile_key = "demo-profile";
        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 2,
                remote_object_count: 2,
                observed_path_count: 3,
                upload_count: 1,
                create_directory_count: 0,
                download_count: 1,
                conflict_count: 1,
                noop_count: 0,
                pending_operation_count: 3,
                credentials_available: true,
            },
            observed_entries: vec![
                ObservedEntry {
                    path: "alpha.txt".into(),
                    local_size: Some(5),
                    remote_size: None,
                    resolution: "upload".into(),
                },
                ObservedEntry {
                    path: "beta.txt".into(),
                    local_size: None,
                    remote_size: Some(7),
                    resolution: "download".into(),
                },
                ObservedEntry {
                    path: "gamma.txt".into(),
                    local_size: Some(9),
                    remote_size: Some(11),
                    resolution: "conflict_review".into(),
                },
            ],
            queue_items: vec![
                PlannedQueueItem {
                    path: "alpha.txt".into(),
                    operation: "upload".into(),
                    local_size: Some(5),
                    remote_size: None,
                    expected_local_fingerprint: Some("fp-alpha".into()),
                    expected_remote_etag: Some("etag-alpha".into()),
                },
                PlannedQueueItem {
                    path: "beta.txt".into(),
                    operation: "download".into(),
                    local_size: None,
                    remote_size: Some(7),
                    expected_local_fingerprint: Some("fp-beta".into()),
                    expected_remote_etag: Some("etag-beta".into()),
                },
                PlannedQueueItem {
                    path: "gamma.txt".into(),
                    operation: "conflict_review".into(),
                    local_size: Some(9),
                    remote_size: Some(11),
                    expected_local_fingerprint: Some("fp-gamma".into()),
                    expected_remote_etag: Some("etag-gamma".into()),
                },
            ],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &plan)
            .expect("planner state should persist");
        let summary = load_planner_summary_from_path(&db_path, profile_key)
            .expect("planner summary should load");

        assert_eq!(
            summary.last_planned_at.as_deref(),
            Some("2026-04-03T00:00:00Z")
        );
        assert_eq!(summary.observed_path_count, 3);
        assert_eq!(summary.upload_count, 1);
        assert_eq!(summary.create_directory_count, 0);
        assert_eq!(summary.download_count, 1);
        assert_eq!(summary.conflict_count, 1);
        assert_eq!(summary.pending_operation_count, 3);
        assert!(summary.credentials_available);

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn loads_and_transitions_only_planned_upload_queue_items() {
        let db_path = temp_path("upload-queue");
        let profile_key = "demo-profile";
        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 2,
                remote_object_count: 1,
                observed_path_count: 3,
                upload_count: 1,
                create_directory_count: 1,
                download_count: 1,
                conflict_count: 1,
                noop_count: 0,
                pending_operation_count: 4,
                credentials_available: true,
            },
            observed_entries: vec![],
            queue_items: vec![
                PlannedQueueItem {
                    path: "alpha.txt".into(),
                    operation: "upload".into(),
                    local_size: Some(5),
                    remote_size: None,
                    expected_local_fingerprint: Some("fp-alpha".into()),
                    expected_remote_etag: None,
                },
                PlannedQueueItem {
                    path: "nested".into(),
                    operation: "create_directory".into(),
                    local_size: None,
                    remote_size: None,
                    expected_local_fingerprint: None,
                    expected_remote_etag: None,
                },
                PlannedQueueItem {
                    path: "beta.txt".into(),
                    operation: "download".into(),
                    local_size: None,
                    remote_size: Some(7),
                    expected_local_fingerprint: Some("fp-beta".into()),
                    expected_remote_etag: Some("etag-beta".into()),
                },
                PlannedQueueItem {
                    path: "gamma.txt".into(),
                    operation: "conflict_review".into(),
                    local_size: Some(9),
                    remote_size: Some(11),
                    expected_local_fingerprint: Some("fp-gamma".into()),
                    expected_remote_etag: Some("etag-gamma".into()),
                },
            ],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &plan)
            .expect("planner state should persist");

        let uploads = load_planned_upload_queue_from_path(&db_path, profile_key)
            .expect("planned upload queue should load");
        assert_eq!(uploads.len(), 2);
        assert_eq!(uploads[0].path, "alpha.txt");
        assert_eq!(uploads[0].operation, "upload");
        assert_eq!(uploads[1].path, "nested");
        assert_eq!(uploads[1].operation, "create_directory");

        mark_upload_queue_item_in_progress_at_path(
            &db_path,
            profile_key,
            uploads[0].id,
            "2026-04-03T00:01:00Z",
        )
        .expect("upload should move to in-progress");
        mark_upload_queue_item_completed_at_path(
            &db_path,
            profile_key,
            uploads[0].id,
            "2026-04-03T00:02:00Z",
        )
        .expect("upload should move to completed");

        {
            let connection = open_connection(&db_path).expect("database should reopen");
            let completed_status: String = connection
                .query_row(
                    "SELECT queue_status FROM sync_queue WHERE id = ?1",
                    params![uploads[0].id],
                    |row| row.get(0),
                )
                .expect("completed queue row should exist");
            assert_eq!(completed_status, "completed");

            let untouched: Vec<(String, String)> = {
                let mut statement = connection
                    .prepare(
                        "SELECT path, queue_status FROM sync_queue WHERE operation NOT IN ('upload', 'create_directory') ORDER BY path ASC",
                    )
                    .expect("should prepare untouched queue query");
                statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .expect("untouched queue query should run")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("untouched queue rows should parse")
            };
            assert_eq!(
                untouched,
                vec![
                    ("beta.txt".into(), "planned".into()),
                    ("gamma.txt".into(), "planned".into()),
                ]
            );
        }

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn persists_upload_failures() {
        let db_path = temp_path("upload-failure");
        let profile_key = "demo-profile";
        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 1,
                remote_object_count: 0,
                observed_path_count: 1,
                upload_count: 1,
                create_directory_count: 0,
                download_count: 0,
                conflict_count: 0,
                noop_count: 0,
                pending_operation_count: 1,
                credentials_available: true,
            },
            observed_entries: vec![],
            queue_items: vec![PlannedQueueItem {
                path: "alpha.txt".into(),
                operation: "upload".into(),
                local_size: Some(5),
                remote_size: None,
                expected_local_fingerprint: Some("fp-alpha".into()),
                expected_remote_etag: None,
            }],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &plan)
            .expect("planner state should persist");
        let uploads = load_planned_upload_queue_from_path(&db_path, profile_key)
            .expect("planned upload queue should load");

        mark_upload_queue_item_in_progress_at_path(
            &db_path,
            profile_key,
            uploads[0].id,
            "2026-04-03T00:01:00Z",
        )
        .expect("upload should move to in-progress");
        mark_upload_queue_item_failed_at_path(
            &db_path,
            profile_key,
            uploads[0].id,
            "2026-04-03T00:02:00Z",
            "simulated upload failure",
        )
        .expect("upload should move to failed");

        {
            let connection = open_connection(&db_path).expect("database should reopen");
            let failed_row: (String, Option<String>) = connection
                .query_row(
                    "SELECT queue_status, last_error FROM sync_queue WHERE id = ?1",
                    params![uploads[0].id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("failed queue row should exist");
            assert_eq!(failed_row.0, "failed");
            assert_eq!(failed_row.1.as_deref(), Some("simulated upload failure"));
        }

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn loads_and_transitions_only_planned_download_queue_items() {
        let db_path = temp_path("download-queue");
        let profile_key = "demo-profile";
        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 2,
                remote_object_count: 1,
                observed_path_count: 3,
                upload_count: 1,
                create_directory_count: 0,
                download_count: 1,
                conflict_count: 1,
                noop_count: 0,
                pending_operation_count: 3,
                credentials_available: true,
            },
            observed_entries: vec![],
            queue_items: vec![
                PlannedQueueItem {
                    path: "alpha.txt".into(),
                    operation: "upload".into(),
                    local_size: Some(5),
                    remote_size: None,
                    expected_local_fingerprint: Some("fp-alpha".into()),
                    expected_remote_etag: None,
                },
                PlannedQueueItem {
                    path: "beta.txt".into(),
                    operation: "download".into(),
                    local_size: None,
                    remote_size: Some(7),
                    expected_local_fingerprint: Some("fp-beta".into()),
                    expected_remote_etag: Some("etag-beta".into()),
                },
                PlannedQueueItem {
                    path: "gamma.txt".into(),
                    operation: "conflict_review".into(),
                    local_size: Some(9),
                    remote_size: Some(11),
                    expected_local_fingerprint: Some("fp-gamma".into()),
                    expected_remote_etag: Some("etag-gamma".into()),
                },
            ],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &plan)
            .expect("planner state should persist");

        let downloads = load_planned_download_queue_from_path(&db_path, profile_key)
            .expect("planned download queue should load");
        assert_eq!(downloads.len(), 1);
        assert_eq!(downloads[0].path, "beta.txt");

        mark_download_queue_item_in_progress_at_path(
            &db_path,
            profile_key,
            downloads[0].id,
            "2026-04-03T00:01:00Z",
        )
        .expect("download should move to in-progress");
        mark_download_queue_item_completed_at_path(
            &db_path,
            profile_key,
            downloads[0].id,
            "2026-04-03T00:02:00Z",
        )
        .expect("download should move to completed");

        {
            let connection = open_connection(&db_path).expect("database should reopen");
            let completed_status: String = connection
                .query_row(
                    "SELECT queue_status FROM sync_queue WHERE id = ?1",
                    params![downloads[0].id],
                    |row| row.get(0),
                )
                .expect("completed queue row should exist");
            assert_eq!(completed_status, "completed");

            let untouched: Vec<(String, String)> = {
                let mut statement = connection
                    .prepare(
                        "SELECT path, queue_status FROM sync_queue WHERE operation != 'download' ORDER BY path ASC",
                    )
                    .expect("should prepare untouched queue query");
                statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                    .expect("untouched queue query should run")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("untouched queue rows should parse")
            };
            assert_eq!(
                untouched,
                vec![
                    ("alpha.txt".into(), "planned".into()),
                    ("gamma.txt".into(), "planned".into()),
                ]
            );
        }

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn planner_summary_migrates_create_directory_count_column() {
        let db_path = temp_path("plan-runs-migration");

        {
            let connection = Connection::open(&db_path).expect("database should open");
            connection
                .execute_batch(
                    "CREATE TABLE plan_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_key TEXT NOT NULL,
                        planned_at TEXT NOT NULL,
                        observed_path_count INTEGER NOT NULL,
                        upload_count INTEGER NOT NULL,
                        download_count INTEGER NOT NULL,
                        conflict_count INTEGER NOT NULL,
                        noop_count INTEGER NOT NULL,
                        pending_operation_count INTEGER NOT NULL,
                        used_stored_credentials INTEGER NOT NULL
                    );
                    CREATE TABLE observed_entries (
                        profile_key TEXT NOT NULL,
                        path TEXT NOT NULL,
                        local_size INTEGER,
                        remote_size INTEGER,
                        resolution TEXT NOT NULL,
                        observed_at TEXT NOT NULL,
                        PRIMARY KEY (profile_key, path)
                    );
                    CREATE TABLE sync_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        plan_run_id INTEGER NOT NULL,
                        profile_key TEXT NOT NULL,
                        path TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        local_size INTEGER,
                        remote_size INTEGER,
                        queue_status TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );",
                )
                .expect("legacy schema should be created");
        }

        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 1,
                remote_object_count: 1,
                observed_path_count: 2,
                upload_count: 0,
                create_directory_count: 1,
                download_count: 0,
                conflict_count: 0,
                noop_count: 1,
                pending_operation_count: 1,
                credentials_available: true,
            },
            observed_entries: vec![],
            queue_items: vec![PlannedQueueItem {
                path: "nested".into(),
                operation: "create_directory".into(),
                local_size: None,
                remote_size: None,
                expected_local_fingerprint: None,
                expected_remote_etag: None,
            }],
        };

        persist_sync_plan_to_path(&db_path, "demo-profile", &plan)
            .expect("planner state should persist with migrated schema");

        let summary = load_planner_summary_from_path(&db_path, "demo-profile")
            .expect("planner summary should load after migration");
        assert_eq!(summary.create_directory_count, 1);

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn rebuilding_plan_supersedes_unresolved_prior_queue_rows_without_deleting_them() {
        let db_path = temp_path("plan-supersession");
        let profile_key = "demo-profile";
        let first_plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 1,
                remote_object_count: 0,
                observed_path_count: 1,
                upload_count: 1,
                create_directory_count: 0,
                download_count: 0,
                conflict_count: 0,
                noop_count: 0,
                pending_operation_count: 1,
                credentials_available: true,
            },
            observed_entries: vec![ObservedEntry {
                path: "alpha.txt".into(),
                local_size: Some(5),
                remote_size: None,
                resolution: "upload".into(),
            }],
            queue_items: vec![PlannedQueueItem {
                path: "alpha.txt".into(),
                operation: "upload".into(),
                local_size: Some(5),
                remote_size: None,
                expected_local_fingerprint: Some("fp-alpha".into()),
                expected_remote_etag: None,
            }],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &first_plan)
            .expect("first planner state should persist");
        let uploads = load_planned_upload_queue_from_path(&db_path, profile_key)
            .expect("planned upload queue should load");
        mark_upload_queue_item_in_progress_at_path(
            &db_path,
            profile_key,
            uploads[0].id,
            "2026-04-03T00:01:00Z",
        )
        .expect("upload should move to in-progress");

        let second_plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:05:00Z".into(),
                local_file_count: 1,
                remote_object_count: 0,
                observed_path_count: 1,
                upload_count: 1,
                create_directory_count: 0,
                download_count: 0,
                conflict_count: 0,
                noop_count: 0,
                pending_operation_count: 1,
                credentials_available: true,
            },
            observed_entries: vec![ObservedEntry {
                path: "beta.txt".into(),
                local_size: Some(7),
                remote_size: None,
                resolution: "upload".into(),
            }],
            queue_items: vec![PlannedQueueItem {
                path: "beta.txt".into(),
                operation: "upload".into(),
                local_size: Some(7),
                remote_size: None,
                expected_local_fingerprint: Some("fp-beta".into()),
                expected_remote_etag: None,
            }],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &second_plan)
            .expect("second planner state should persist");

        {
            let connection = open_connection(&db_path).expect("database should reopen");
            let queue_rows: Vec<(String, String, i64)> = {
                let mut statement = connection
                    .prepare(
                        "SELECT path, queue_status, plan_run_id FROM sync_queue ORDER BY id ASC",
                    )
                    .expect("should prepare queue row query");
                statement
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
                    .expect("queue row query should run")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("queue rows should parse")
            };
            assert_eq!(queue_rows.len(), 2);
            assert_eq!(queue_rows[0].0, "alpha.txt");
            assert_eq!(queue_rows[0].1, "superseded");
            assert_eq!(queue_rows[1].0, "beta.txt");
            assert_eq!(queue_rows[1].1, "planned");
            assert_ne!(queue_rows[0].2, queue_rows[1].2);

            let observed_entries: Vec<(String, String)> = {
                let mut statement = connection
                    .prepare(
                        "SELECT path, resolution FROM observed_entries WHERE profile_key = ?1 ORDER BY path ASC",
                    )
                    .expect("should prepare observed entry query");
                statement
                    .query_map(params![profile_key], |row| Ok((row.get(0)?, row.get(1)?)))
                    .expect("observed entry query should run")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("observed entry rows should parse")
            };
            assert_eq!(
                observed_entries,
                vec![
                    ("alpha.txt".into(), "upload".into()),
                    ("beta.txt".into(), "upload".into())
                ]
            );

            let run_rows: Vec<(String, String, Option<i64>)> = {
                let mut statement = connection
                    .prepare(
                        "SELECT planned_at, run_status, superseded_by_run_id FROM plan_runs WHERE profile_key = ?1 ORDER BY id ASC",
                    )
                    .expect("should prepare run row query");
                statement
                    .query_map(params![profile_key], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .expect("run row query should run")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("run rows should parse")
            };
            assert_eq!(run_rows.len(), 2);
            assert_eq!(run_rows[0].0, "2026-04-03T00:00:00Z");
            assert_eq!(run_rows[0].1, "superseded");
            assert!(run_rows[0].2.is_some());
            assert_eq!(run_rows[1].0, "2026-04-03T00:05:00Z");
            assert_eq!(run_rows[1].1, "active");
            assert_eq!(run_rows[1].2, None);
        }

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn interrupted_queue_items_are_recovered_and_become_loadable_again() {
        let db_path = temp_path("queue-recovery");
        let profile_key = "demo-profile";
        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-03T00:00:00Z".into(),
                local_file_count: 2,
                remote_object_count: 1,
                observed_path_count: 3,
                upload_count: 1,
                create_directory_count: 1,
                download_count: 1,
                conflict_count: 0,
                noop_count: 0,
                pending_operation_count: 3,
                credentials_available: true,
            },
            observed_entries: vec![],
            queue_items: vec![
                PlannedQueueItem {
                    path: "alpha.txt".into(),
                    operation: "upload".into(),
                    local_size: Some(5),
                    remote_size: None,
                    expected_local_fingerprint: Some("fp-alpha".into()),
                    expected_remote_etag: None,
                },
                PlannedQueueItem {
                    path: "nested".into(),
                    operation: "create_directory".into(),
                    local_size: None,
                    remote_size: None,
                    expected_local_fingerprint: None,
                    expected_remote_etag: None,
                },
                PlannedQueueItem {
                    path: "beta.txt".into(),
                    operation: "download".into(),
                    local_size: None,
                    remote_size: Some(7),
                    expected_local_fingerprint: Some("fp-beta".into()),
                    expected_remote_etag: Some("etag-beta".into()),
                },
            ],
        };

        persist_sync_plan_to_path(&db_path, profile_key, &plan)
            .expect("planner state should persist");

        let uploads = load_planned_upload_queue_from_path(&db_path, profile_key)
            .expect("planned upload queue should load");
        let downloads = load_planned_download_queue_from_path(&db_path, profile_key)
            .expect("planned download queue should load");

        mark_upload_queue_item_in_progress_at_path(
            &db_path,
            profile_key,
            uploads[0].id,
            "2026-04-03T00:01:00Z",
        )
        .expect("upload should move to in-progress");
        mark_download_queue_item_in_progress_at_path(
            &db_path,
            profile_key,
            downloads[0].id,
            "2026-04-03T00:01:30Z",
        )
        .expect("download should move to in-progress");

        let recovered =
            recover_interrupted_queue_items_at_path(&db_path, profile_key, "2026-04-03T00:02:00Z")
                .expect("interrupted queue recovery should succeed");
        assert_eq!(recovered.recovered_upload_count, 1);
        assert_eq!(recovered.recovered_download_count, 1);

        let uploads = load_planned_upload_queue_from_path(&db_path, profile_key)
            .expect("recovered upload queue should load");
        let downloads = load_planned_download_queue_from_path(&db_path, profile_key)
            .expect("recovered download queue should load");
        assert_eq!(
            uploads
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha.txt", "nested"]
        );
        assert_eq!(
            downloads
                .iter()
                .map(|item| item.path.as_str())
                .collect::<Vec<_>>(),
            vec!["beta.txt"]
        );

        {
            let connection = open_connection(&db_path).expect("database should reopen");
            let recovered_rows: Vec<(String, String, Option<String>)> = {
                let mut statement = connection
                    .prepare(
                        "SELECT path, queue_status, last_error FROM sync_queue WHERE profile_key = ?1 ORDER BY id ASC",
                    )
                    .expect("should prepare recovered queue query");
                statement
                    .query_map(params![profile_key], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .expect("recovered queue query should run")
                    .collect::<Result<Vec<_>, _>>()
                    .expect("recovered queue rows should parse")
            };
            assert_eq!(
                recovered_rows,
                vec![
                    (
                        "alpha.txt".into(),
                        "interrupted".into(),
                        Some(
                            "Execution was interrupted before completion; item is safe to retry."
                                .into()
                        )
                    ),
                    ("nested".into(), "planned".into(), None),
                    (
                        "beta.txt".into(),
                        "interrupted".into(),
                        Some(
                            "Execution was interrupted before completion; item is safe to retry."
                                .into()
                        )
                    ),
                ]
            );

            let run_status: String = connection
                .query_row(
                    "SELECT run_status FROM plan_runs WHERE profile_key = ?1 ORDER BY id DESC LIMIT 1",
                    params![profile_key],
                    |row| row.get(0),
                )
                .expect("run status should exist");
            assert_eq!(run_status, "active");
        }

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }

    #[test]
    fn sync_pair_key_matches_profile_key_for_same_fields() {
        let profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            ..StoredProfile::default()
        };
        let pair = SyncPair {
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            ..SyncPair::default()
        };
        assert_eq!(profile_key(&profile), sync_pair_key(&pair));
    }

    #[test]
    fn sync_anchor_round_trips_and_migrates_queue_expectation_columns() {
        let db_path = temp_path("sync-anchor-roundtrip");

        {
            let connection = Connection::open(&db_path).expect("database should open");
            connection
                .execute_batch(
                    "CREATE TABLE plan_runs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        profile_key TEXT NOT NULL,
                        planned_at TEXT NOT NULL,
                        observed_path_count INTEGER NOT NULL,
                        upload_count INTEGER NOT NULL,
                        download_count INTEGER NOT NULL,
                        conflict_count INTEGER NOT NULL,
                        noop_count INTEGER NOT NULL,
                        pending_operation_count INTEGER NOT NULL,
                        used_stored_credentials INTEGER NOT NULL
                    );
                    CREATE TABLE observed_entries (
                        profile_key TEXT NOT NULL,
                        path TEXT NOT NULL,
                        local_size INTEGER,
                        remote_size INTEGER,
                        resolution TEXT NOT NULL,
                        observed_at TEXT NOT NULL,
                        PRIMARY KEY (profile_key, path)
                    );
                    CREATE TABLE sync_queue (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        plan_run_id INTEGER NOT NULL,
                        profile_key TEXT NOT NULL,
                        path TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        local_size INTEGER,
                        remote_size INTEGER,
                        queue_status TEXT NOT NULL,
                        created_at TEXT NOT NULL
                    );",
                )
                .expect("legacy schema should be created");
        }

        let plan = SyncPlan {
            summary: SyncPlanSummary {
                planned_at: "2026-04-12T10:00:00Z".into(),
                local_file_count: 1,
                remote_object_count: 1,
                observed_path_count: 1,
                upload_count: 1,
                create_directory_count: 0,
                download_count: 0,
                conflict_count: 0,
                noop_count: 0,
                pending_operation_count: 1,
                credentials_available: true,
            },
            observed_entries: vec![ObservedEntry {
                path: "note.txt".into(),
                local_size: Some(5),
                remote_size: Some(5),
                resolution: "upload".into(),
            }],
            queue_items: vec![PlannedQueueItem {
                path: "note.txt".into(),
                operation: "upload".into(),
                local_size: Some(5),
                remote_size: Some(5),
                expected_local_fingerprint: Some("fp-note".into()),
                expected_remote_etag: Some("etag-note".into()),
            }],
        };

        persist_sync_plan_to_path(&db_path, "demo-profile", &plan)
            .expect("planner state should persist with migrated queue expectation columns");

        upsert_sync_anchor_at_path(
            &db_path,
            "demo-profile",
            &SyncAnchor {
                path: "note.txt".into(),
                kind: "file".into(),
                local_fingerprint: Some("fp-note".into()),
                remote_etag: Some("etag-note-2".into()),
                synced_at: "2026-04-12T10:05:00Z".into(),
            },
        )
        .expect("sync anchor should upsert");

        let anchors = load_sync_anchors_from_path(&db_path, "demo-profile")
            .expect("sync anchors should load");
        assert_eq!(anchors.len(), 1);
        assert_eq!(anchors[0].path, "note.txt");
        assert_eq!(anchors[0].remote_etag.as_deref(), Some("etag-note-2"));

        let uploads = load_planned_upload_queue_from_path(&db_path, "demo-profile")
            .expect("planned uploads should load");
        assert_eq!(
            uploads[0].expected_local_fingerprint.as_deref(),
            Some("fp-note")
        );
        assert_eq!(
            uploads[0].expected_remote_etag.as_deref(),
            Some("etag-note")
        );

        if db_path.exists() {
            fs::remove_file(db_path).expect("should remove temp sqlite database");
        }
    }
}
