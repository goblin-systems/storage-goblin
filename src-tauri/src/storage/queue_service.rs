//! Draining the durable upload and download queues for one sync location.
//!
//! Each item is marked in-progress in SQLite before execution so a second
//! cycle cannot pick it up, then completed or failed with its error recorded.

use tauri::{AppHandle, Runtime};

use super::activity::ActivityDebugState;
use super::commands::{
    emit_error_activity, emit_info_activity, emit_success_activity, run_with_timeout,
    PLANNED_DOWNLOAD_TIMEOUT, PLANNED_UPLOAD_TIMEOUT,
};
use super::credentials_store::StoredCredentials;
use super::now_iso;
use super::object_store;
use super::platform::{resolve_local_download_path, resolve_local_upload_path};
use super::profile_store::SyncPair;
use super::remote_index::read_remote_index_snapshot_for_pair;
use super::s3_adapter;
use super::sync_db::{
    load_planned_download_queue_for_pair, load_planned_upload_queue_for_pair,
    mark_download_queue_item_completed_for_pair, mark_download_queue_item_failed_for_pair,
    mark_download_queue_item_in_progress_for_pair, mark_upload_queue_item_completed_for_pair,
    mark_upload_queue_item_failed_for_pair, mark_upload_queue_item_in_progress_for_pair,
    recover_interrupted_queue_items_for_pair,
};
use super::transfer_service::{
    build_pair_transfer_executor, download_stale_plan_error, perform_planned_download_for_pair,
    perform_planned_upload_for_pair, perform_structural_download_operation_for_pair,
    perform_structural_upload_operation_for_pair, persist_download_success_for_pair,
    persist_upload_success_for_pair, remote_etag_for_path, upload_stale_plan_error,
    PairTransferExecutor,
};

/// Collapse per-item failures into one reportable message.
///
/// The queue no longer stops at the first bad file, so the outcome has to say
/// how many failed rather than surfacing one error as if it were the whole
/// story. Each item's own error is already recorded on its queue row.
fn summarize_item_failures(failures: &[String]) -> Option<String> {
    match failures.len() {
        0 => None,
        1 => Some(failures[0].clone()),
        count => Some(format!(
            "{count} items failed; first: {}",
            failures.first().map(String::as_str).unwrap_or_default()
        )),
    }
}

pub(crate) struct UploadExecutionOutcome {
    pub(crate) execution_error: Option<String>,
    pub(crate) uploads_ran: bool,
}

pub(crate) struct DownloadExecutionOutcome {
    pub(crate) execution_error: Option<String>,
    pub(crate) downloads_ran: bool,
}

pub(crate) fn emit_recovery_activity<R: Runtime>(
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

pub(crate) async fn execute_planned_upload_queue_for_pair<R: Runtime>(
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
    // Per-item failures no longer stop the queue (backlog phase 2.2): each is
    // recorded against its own row and the remaining items still run.
    let mut item_failures: Vec<String> = Vec::new();

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
                    item_failures.push(failure_message);
                    continue;
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
                    item_failures.push(failure_message);
                    continue;
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
                item_failures.push(error);
                continue;
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
                item_failures.push(error);
                continue;
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
                item_failures.push(error);
                continue;
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
                item_failures.push(error);
                continue;
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
                item_failures.push(error);
                continue;
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
                item_failures.push(error);
                continue;
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
            item_failures.push(error);
            continue;
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
                    item_failures.push(error);
                    continue;
                }

                let finished_at = now_iso();
                if let Err(error) =
                    mark_upload_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                {
                    item_failures.push(error);
                    continue;
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
                item_failures.push(failure_message);
                continue;
            }
        }
    }

    Ok(UploadExecutionOutcome {
        // An infrastructure abort wins; otherwise report the item failures.
        execution_error: execution_error.or_else(|| summarize_item_failures(&item_failures)),
        uploads_ran,
    })
}

pub(crate) async fn execute_planned_download_queue_for_pair<R: Runtime>(
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
    let mut item_failures: Vec<String> = Vec::new();

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
                    item_failures.push(failure_message);
                    continue;
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
                item_failures.push(error);
                continue;
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
                item_failures.push(error);
                continue;
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
            item_failures.push(error);
            continue;
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
                    item_failures.push(error);
                    continue;
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
            item_failures.push(error);
            continue;
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
                item_failures.push(error_msg);
                continue;
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
                    item_failures.push(error);
                    continue;
                }

                let finished_at = now_iso();
                if let Err(error) =
                    mark_download_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                {
                    item_failures.push(error);
                    continue;
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
                item_failures.push(failure_message);
                continue;
            }
        }
    }

    Ok(DownloadExecutionOutcome {
        execution_error: execution_error.or_else(|| summarize_item_failures(&item_failures)),
        downloads_ran,
    })
}

#[cfg(test)]
mod tests {
    use super::summarize_item_failures;

    #[test]
    fn a_clean_queue_reports_no_error() {
        assert_eq!(summarize_item_failures(&[]), None);
    }

    #[test]
    fn a_single_failure_is_reported_verbatim() {
        assert_eq!(
            summarize_item_failures(&["upload failed for 'a.txt': denied".to_string()]),
            Some("upload failed for 'a.txt': denied".to_string())
        );
    }

    #[test]
    fn many_failures_report_a_count_rather_than_pretending_one_is_the_story() {
        // The queue now drains past failures, so the outcome must convey how
        // many there were — reporting only the first would understate it.
        let summary = summarize_item_failures(&[
            "upload failed for 'a.txt': denied".to_string(),
            "upload failed for 'b.txt': denied".to_string(),
            "upload failed for 'c.txt': denied".to_string(),
        ])
        .expect("failures should summarize");

        assert!(summary.starts_with("3 items failed"), "got: {summary}");
        assert!(
            summary.contains("a.txt"),
            "should name the first: {summary}"
        );
    }
}
