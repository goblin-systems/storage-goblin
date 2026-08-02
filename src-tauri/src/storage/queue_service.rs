//! Draining the durable upload and download queues for one sync location.
//!
//! Each item is marked in-progress in SQLite before execution so a second
//! cycle cannot pick it up, then completed or failed with its error recorded.

use std::path::Path;

use futures_util::stream::{self, StreamExt};
use tauri::{AppHandle, Manager, Runtime};

use super::activity::ActivityDebugState;
use super::commands::{
    emit_error_activity, emit_info_activity, emit_success_activity, emit_transfer_progress,
    run_with_timeout, PLANNED_UPLOAD_TIMEOUT,
};
use super::coordinator::SyncCoordinator;
use super::credentials_store::StoredCredentials;
use super::now_iso;
use super::object_store;
use super::platform::{
    available_disk_space, resolve_local_download_path, resolve_local_upload_path,
};
use super::profile_store::SyncPair;
use super::progress::{ProgressReporter, ProgressThrottle};
use super::queue_schedule::{plan_stages, Schedulable, StageMode};
use super::remote_index::read_remote_index_snapshot_for_pair;
use super::s3_adapter;
use super::sync_db::{
    load_planned_download_queue_for_pair, load_planned_upload_queue_for_pair,
    mark_download_queue_item_completed_for_pair, mark_download_queue_item_failed_for_pair,
    mark_download_queue_item_in_progress_for_pair, mark_upload_queue_item_completed_for_pair,
    mark_upload_queue_item_failed_for_pair, mark_upload_queue_item_in_progress_for_pair,
    recover_interrupted_queue_items_for_pair, PlannedDownloadQueueItem, PlannedUploadQueueItem,
};
use super::sync_state::{coordinator, SyncState};
use super::transfer::{insufficient_space, transfer_timeout};
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

impl Schedulable for PlannedUploadQueueItem {
    fn operation(&self) -> &str {
        &self.operation
    }
    fn path(&self) -> &str {
        &self.path
    }
    fn transfer_size(&self) -> Option<u64> {
        self.local_size
    }
}

impl Schedulable for PlannedDownloadQueueItem {
    fn operation(&self) -> &str {
        &self.operation
    }
    fn path(&self) -> &str {
        &self.path
    }
    fn transfer_size(&self) -> Option<u64> {
        self.remote_size
    }
}

/// Why an item did not complete.
///
/// The distinction is the whole point: one bad file must not stop the queue,
/// but a durable store that will not accept writes must stop it, because every
/// later item would then "succeed" without being recorded and be re-run
/// forever.
enum ItemError {
    /// This item failed. Record it and keep draining.
    Item(String),
    /// The queue's own bookkeeping failed. Stop the batch.
    Infrastructure(String),
}

/// Outcome of draining one stage.
#[derive(Default)]
struct StageOutcome {
    item_failures: Vec<String>,
    infrastructure_error: Option<String>,
}

impl StageOutcome {
    fn absorb(&mut self, result: Result<(), ItemError>) {
        match result {
            Ok(()) => {}
            Err(ItemError::Item(message)) => self.item_failures.push(message),
            // Keep the first: it is the one that explains the rest.
            Err(ItemError::Infrastructure(message)) => {
                self.infrastructure_error.get_or_insert(message);
            }
        }
    }

    fn should_stop(&self) -> bool {
        self.infrastructure_error.is_some()
    }
}

/// Where the global transfer budget lives while a queue drains.
///
/// Normally it is the coordinator on the managed `SyncState`, so two locations
/// draining at once share one budget. Tests that drive the queue directly have
/// no managed state, and get a private budget rather than a panic.
enum TransferBudget<'a> {
    Shared(&'a SyncCoordinator),
    Private(SyncCoordinator),
}

impl TransferBudget<'_> {
    fn coordinator(&self) -> &SyncCoordinator {
        match self {
            TransferBudget::Shared(coordinator) => coordinator,
            TransferBudget::Private(coordinator) => coordinator,
        }
    }
}

/// Resolve the transfer budget for this drain.
///
/// `managed` must outlive the returned borrow, which is why the caller binds it
/// rather than this taking the `AppHandle` directly.
fn transfer_budget<'a>(managed: &'a Option<tauri::State<'a, SyncState>>) -> TransferBudget<'a> {
    match managed {
        Some(state) => TransferBudget::Shared(coordinator(state)),
        None => TransferBudget::Private(SyncCoordinator::default()),
    }
}

/// Run one stage's items, honouring its concurrency mode.
///
/// Concurrent stages still pass through the global transfer budget, so the
/// number of connections open at once is bounded across every location rather
/// than per queue.
async fn drain_stage<T, F, Fut>(
    mode: StageMode,
    items: Vec<T>,
    budget: &SyncCoordinator,
    run_item: F,
) -> StageOutcome
where
    F: Fn(T) -> Fut,
    Fut: std::future::Future<Output = Result<(), ItemError>>,
{
    let mut outcome = StageOutcome::default();

    match mode {
        StageMode::Sequential => {
            for item in items {
                outcome.absorb(run_item(item).await);
                if outcome.should_stop() {
                    break;
                }
            }
        }
        StageMode::Concurrent => {
            // buffer_unordered bounds how many futures are alive; the budget's
            // semaphore bounds how many are actually transferring. Keeping the
            // first a little wider lets a finished transfer be replaced without
            // waiting for the stream to poll around again.
            let in_flight = budget.max_concurrent_transfers().saturating_mul(2).max(1);
            let results = stream::iter(items)
                .map(|item| async {
                    let _slot = budget.acquire_transfer_slot().await;
                    run_item(item).await
                })
                .buffer_unordered(in_flight)
                .collect::<Vec<_>>()
                .await;
            for result in results {
                outcome.absorb(result);
            }
        }
    }

    outcome
}

/// Execute one planned upload queue item.
///
/// Marks the row in progress, runs the operation, and records the outcome.
/// Returns [`ItemError::Item`] when this file failed and the queue should
/// carry on, or [`ItemError::Infrastructure`] when the durable store itself
/// failed and continuing would silently lose work.
async fn run_upload_item<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    credentials: &StoredCredentials,
    executor: &PairTransferExecutor,
    item: PlannedUploadQueueItem,
) -> Result<(), ItemError> {
    let started_at = now_iso();
    if let Err(error) = mark_upload_queue_item_in_progress_for_pair(app, pair, item.id, &started_at)
    {
        return Err(ItemError::Infrastructure(error));
    }

    // Deletes, moves, conflict duplication, and anchor reconciliation
    // (backlog phase 1) are not file transfers.
    if let Some(structural) =
        perform_structural_upload_operation_for_pair(app, executor, pair, credentials, &item).await
    {
        match structural {
            Ok(message) => {
                let finished_at = now_iso();
                if let Err(error) =
                    mark_upload_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                {
                    return Err(ItemError::Infrastructure(error));
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
                return Err(ItemError::Item(failure_message));
            }
        }
        return Ok(());
    }

    if item.operation == "create_directory" {
        let key = s3_adapter::directory_key(&item.path);
        emit_info_activity(
            app,
            debug_state,
            "Starting planned directory creation.",
            Some(format!(
                "pair='{}' queue_item_id={} attempt={} path='{}' key='{}'",
                pair.label, item.id, item.id, item.path, key,
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
                    return Err(ItemError::Infrastructure(error));
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
                return Err(ItemError::Item(failure_message));
            }
        }

        return Ok(());
    }

    let local_path = match resolve_local_upload_path(&pair.local_folder, &item.path) {
        Ok(path) => path,
        Err(error) => {
            let finished_at = now_iso();
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
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
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
        }
        Err(error) => {
            let error = format!(
                "failed to inspect planned upload source '{}': {error}",
                local_path.display()
            );
            let finished_at = now_iso();
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
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
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
        }
    }

    let current_fingerprint = match crate::storage::local_index::file_fingerprint(&local_path) {
        Ok(fingerprint) => fingerprint,
        Err(error) => {
            let finished_at = now_iso();
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
        }
    };

    let remote_snapshot = match read_remote_index_snapshot_for_pair(app, &pair.id)
        .map_err(ItemError::Infrastructure)?
    {
        Some(snapshot) => snapshot,
        None => {
            let error = "remote snapshot missing before planned upload execution".to_string();
            let finished_at = now_iso();
            let _ =
                mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
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
        let _ = mark_upload_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
        return Err(ItemError::Item(error));
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
            item.id,
            item.path,
            key,
            local_path.display(),
            metadata.len()
        )),
    );

    // Budget scales with the object: a flat cap made large files
    // impossible rather than merely slow (backlog phase 2.1).
    let upload_budget = transfer_timeout(metadata.len());
    match run_with_timeout(
        perform_planned_upload_for_pair(
            executor,
            pair,
            credentials,
            &item.path,
            &key,
            &local_path,
            &current_fingerprint,
        ),
        upload_budget,
        || {
            format!(
                "Upload timed out for '{}' after {}s",
                item.path,
                upload_budget.as_secs()
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
                return Err(ItemError::Item(error));
            }

            let finished_at = now_iso();
            if let Err(error) =
                mark_upload_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
            {
                return Err(ItemError::Item(error));
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
            return Err(ItemError::Item(failure_message));
        }
    }
    Ok(())
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

    let managed = app.try_state::<SyncState>();
    let budget = transfer_budget(&managed);

    // Stages run in order and never overlap; only the transfer stage runs its
    // items concurrently. See `queue_schedule` for why the others cannot.
    for stage in plan_stages(queue_items) {
        let outcome = drain_stage(stage.mode, stage.items, budget.coordinator(), |item| {
            run_upload_item(app, debug_state, pair, credentials, &executor, item)
        })
        .await;

        item_failures.extend(outcome.item_failures);
        if let Some(error) = outcome.infrastructure_error {
            execution_error = Some(error);
            break;
        }
    }

    Ok(UploadExecutionOutcome {
        // An infrastructure abort wins; otherwise report the item failures.
        execution_error: execution_error.or_else(|| summarize_item_failures(&item_failures)),
        uploads_ran,
    })
}

/// Execute one planned download queue item.
///
/// Mirrors [`run_upload_item`]: same in-progress marking, same split
/// between a failure that stops this item and one that stops the batch.
async fn run_download_item<R: Runtime>(
    app: &AppHandle<R>,
    debug_state: &ActivityDebugState,
    pair: &SyncPair,
    executor: &PairTransferExecutor,
    item: PlannedDownloadQueueItem,
) -> Result<(), ItemError> {
    let started_at = now_iso();
    if let Err(error) =
        mark_download_queue_item_in_progress_for_pair(app, pair, item.id, &started_at)
    {
        return Err(ItemError::Infrastructure(error));
    }

    // Local deletes and local moves (backlog phase 1) are not transfers.
    if let Some(structural) =
        perform_structural_download_operation_for_pair(app, executor, pair, &item)
    {
        match structural {
            Ok(message) => {
                let finished_at = now_iso();
                if let Err(error) =
                    mark_download_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
                {
                    return Err(ItemError::Infrastructure(error));
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
                return Err(ItemError::Item(failure_message));
            }
        }
        return Ok(());
    }

    let local_path = match resolve_local_download_path(&pair.local_folder, &item.path) {
        Ok(path) => path,
        Err(error) => {
            let finished_at = now_iso();
            let _ =
                mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
        }
    };

    let remote_snapshot = match read_remote_index_snapshot_for_pair(app, &pair.id)
        .map_err(ItemError::Infrastructure)?
    {
        Some(snapshot) => snapshot,
        None => {
            let error = "remote snapshot missing before planned download execution".to_string();
            let finished_at = now_iso();
            let _ =
                mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
            return Err(ItemError::Item(error));
        }
    };
    let current_remote_etag = remote_etag_for_path(&remote_snapshot, &item.path);
    if current_remote_etag != item.expected_remote_etag {
        let error = format!(
            "planned download source '{}' changed remotely since planning",
            item.path
        );
        let finished_at = now_iso();
        let _ = mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
        return Err(ItemError::Item(error));
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
                return Err(ItemError::Item(error));
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
        let _ = mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
        return Err(ItemError::Item(error));
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
            item.id,
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
            return Err(ItemError::Item(error_msg));
        }
    }

    // Refuse a download that obviously cannot fit, before moving a byte.
    // Discovering this at the last byte costs the whole transfer and reports a
    // cryptic OS error; discovering it now costs one syscall (phase 2.4).
    if let Some(error) = insufficient_space(
        available_disk_space(Path::new(&pair.local_folder)),
        item.remote_size.unwrap_or_default(),
    ) {
        let finished_at = now_iso();
        let _ = mark_download_queue_item_failed_for_pair(app, pair, item.id, &finished_at, &error);
        emit_error_activity(
            app,
            debug_state,
            "Not enough disk space for a planned download.",
            Some(format!(
                "pair='{}' queue_item_id={} path='{}' {error}",
                pair.label, item.id, item.path
            )),
        );
        return Err(ItemError::Item(error));
    }

    let download_budget = transfer_timeout(item.remote_size.unwrap_or_default());
    match run_with_timeout(
        perform_planned_download_for_pair(executor, pair, &key, &item.path, &local_path, || {
            let app = app.clone();
            Some(ProgressReporter::new(
                ProgressThrottle::new(
                    &pair.id,
                    &item.path,
                    item.remote_size,
                    std::time::Instant::now(),
                ),
                move |update| emit_transfer_progress(&app, &update),
            ))
        }),
        download_budget,
        || {
            format!(
                "Download timed out for '{}' after {}s",
                item.path,
                download_budget.as_secs()
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
                return Err(ItemError::Item(error));
            }

            let finished_at = now_iso();
            if let Err(error) =
                mark_download_queue_item_completed_for_pair(app, pair, item.id, &finished_at)
            {
                return Err(ItemError::Item(error));
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
            return Err(ItemError::Item(failure_message));
        }
    }
    Ok(())
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

    let managed = app.try_state::<SyncState>();
    let budget = transfer_budget(&managed);

    for stage in plan_stages(queue_items) {
        let outcome = drain_stage(stage.mode, stage.items, budget.coordinator(), |item| {
            run_download_item(app, debug_state, pair, &executor, item)
        })
        .await;

        item_failures.extend(outcome.item_failures);
        if let Some(error) = outcome.infrastructure_error {
            execution_error = Some(error);
            break;
        }
    }

    Ok(DownloadExecutionOutcome {
        execution_error: execution_error.or_else(|| summarize_item_failures(&item_failures)),
        downloads_ran,
    })
}

#[cfg(test)]
mod tests {
    use super::{drain_stage, summarize_item_failures, ItemError};
    use crate::storage::coordinator::SyncCoordinator;
    use crate::storage::queue_schedule::StageMode;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
    }

    #[test]
    fn a_sequential_stage_runs_items_in_order() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let coordinator = SyncCoordinator::default();

        let outcome = runtime().block_on(drain_stage(
            StageMode::Sequential,
            vec![1, 2, 3],
            &coordinator,
            |item| {
                let seen = Arc::clone(&seen);
                async move {
                    seen.lock().expect("lock").push(item);
                    Ok(())
                }
            },
        ));

        assert!(outcome.item_failures.is_empty());
        assert_eq!(*seen.lock().expect("lock"), vec![1, 2, 3]);
    }

    #[test]
    fn an_item_failure_does_not_stop_the_stage() {
        let attempted = Arc::new(AtomicUsize::new(0));
        let coordinator = SyncCoordinator::default();

        let outcome = runtime().block_on(drain_stage(
            StageMode::Sequential,
            vec![1, 2, 3],
            &coordinator,
            |item| {
                let attempted = Arc::clone(&attempted);
                async move {
                    attempted.fetch_add(1, Ordering::SeqCst);
                    if item == 2 {
                        Err(ItemError::Item(format!("item {item} failed")))
                    } else {
                        Ok(())
                    }
                }
            },
        ));

        assert_eq!(
            attempted.load(Ordering::SeqCst),
            3,
            "all items must be tried"
        );
        assert_eq!(outcome.item_failures, vec!["item 2 failed".to_string()]);
        assert!(outcome.infrastructure_error.is_none());
    }

    #[test]
    fn an_infrastructure_failure_stops_the_stage_immediately() {
        let attempted = Arc::new(AtomicUsize::new(0));
        let coordinator = SyncCoordinator::default();

        let outcome = runtime().block_on(drain_stage(
            StageMode::Sequential,
            vec![1, 2, 3],
            &coordinator,
            |item| {
                let attempted = Arc::clone(&attempted);
                async move {
                    attempted.fetch_add(1, Ordering::SeqCst);
                    if item == 2 {
                        Err(ItemError::Infrastructure("database is gone".into()))
                    } else {
                        Ok(())
                    }
                }
            },
        ));

        // Continuing past a store that will not accept writes would mean every
        // later item "succeeds" unrecorded and is re-run forever.
        assert_eq!(attempted.load(Ordering::SeqCst), 2, "item 3 must not run");
        assert_eq!(
            outcome.infrastructure_error,
            Some("database is gone".to_string())
        );
    }

    #[test]
    fn a_concurrent_stage_respects_the_transfer_budget() {
        const LIMIT: usize = 2;
        let coordinator = SyncCoordinator::with_transfer_limit(LIMIT);
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        let outcome = runtime().block_on(drain_stage(
            StageMode::Concurrent,
            (0..8).collect::<Vec<u32>>(),
            &coordinator,
            |_item| {
                let in_flight = Arc::clone(&in_flight);
                let peak = Arc::clone(&peak);
                async move {
                    let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    in_flight.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            },
        ));

        assert!(outcome.item_failures.is_empty());
        assert!(
            peak.load(Ordering::SeqCst) <= LIMIT,
            "peak was {}, over the budget of {LIMIT}",
            peak.load(Ordering::SeqCst)
        );
        assert!(
            peak.load(Ordering::SeqCst) > 1,
            "the stage did not actually run anything concurrently"
        );
    }

    #[test]
    fn a_concurrent_stage_collects_every_failure() {
        let coordinator = SyncCoordinator::default();

        let outcome = runtime().block_on(drain_stage(
            StageMode::Concurrent,
            (0..6).collect::<Vec<u32>>(),
            &coordinator,
            |item| async move {
                if item % 2 == 0 {
                    Err(ItemError::Item(format!("item {item} failed")))
                } else {
                    Ok(())
                }
            },
        ));

        // Losing failures would report a clean sync that silently did not work.
        assert_eq!(outcome.item_failures.len(), 3);
    }

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
