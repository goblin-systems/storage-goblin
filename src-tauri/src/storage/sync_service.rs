//! Running a sync cycle for one location: scan, inventory, plan, execute.
//!
//! Owns the durable queue execution loops and the polling worker that drives
//! them. Each cycle refreshes snapshots, rebuilds the plan, then drains the
//! upload and download queues.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tauri::{AppHandle, Manager, Runtime, State};

use super::activity::ActivityDebugState;
use super::commands::{
    append_error_context, concise_sync_issue, emit_error_activity, emit_info_activity, emit_status,
    emit_success_activity, list_remote_inventory_for_pair, pair_sync_cycle_issue_details,
    reconcile_pair_watchers, refresh_aggregate_status, resolve_credentials_for_pair,
    DIRTY_PAIR_DEBOUNCE, LOCAL_SNAPSHOT_STALE_TTL,
};
use super::local_index::{
    read_local_index_snapshot_for_pair, scan_local_folder, write_local_index_snapshot_for_pair,
    LocalIndexSnapshot,
};
use super::now_iso;
use super::polling_service::{
    due_polling_pairs, next_polling_deadline_at, should_poll_pair, should_scan_local_for_trigger,
    stop_requested, PairSyncTrigger,
};
use super::profile_store::{is_pair_configured, read_profile_from_disk, SyncPair};
use super::queue_service::{
    execute_planned_download_queue_for_pair, execute_planned_upload_queue_for_pair,
};
use super::remote_index::{
    read_remote_index_snapshot_for_pair, write_remote_index_snapshot_for_pair, RemoteIndexSnapshot,
};
use super::sync_db::{
    load_planner_summary_for_pair, load_sync_anchors_for_pair, persist_sync_plan_for_pair,
};
use super::sync_planner;
use super::sync_state::{
    begin_polling_worker, clear_all_pair_watchers, clear_dirty_pair, clear_polling_worker,
    due_dirty_pairs, next_dirty_pair_deadline, pair_has_active_watcher, pair_statuses_snapshot,
    pair_to_status, set_pair_status_from_handle, set_status_from_handle, PairSyncStatus, SyncState,
};
use super::transfer::cleanup_orphaned_temp_files;

pub(crate) fn snapshot_for_pair<R: Runtime>(
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

pub(crate) fn remote_snapshot_for_pair<R: Runtime>(
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

pub(crate) fn rebuild_durable_plan_for_pair<R: Runtime>(
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

pub(crate) async fn run_sync_cycle_for_pair(
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

    // Sweep temp files from downloads a previous run never finished, before
    // anything scans this tree (backlog phase 2.1 / 3.4 startup consistency).
    let swept = cleanup_orphaned_temp_files(Path::new(&pair.local_folder));
    if swept > 0 {
        emit_info_activity(
            app,
            debug_state,
            "Cleaned up interrupted downloads.",
            Some(format!("pair='{}' removed_temp_files={swept}", pair.label)),
        );
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

pub(crate) fn start_polling_worker_for_pairs(app: &AppHandle) -> Result<(), String> {
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
pub(crate) fn start_polling_worker(app: &AppHandle) -> Result<(), String> {
    start_polling_worker_for_pairs(app)
}

pub(crate) async fn sleep_until_pair_work(
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
