use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use tauri::{Manager, State};

use super::{
    inventory_compare::{compare_snapshots, InventoryComparisonSummary},
    local_index::LocalIndexSnapshot,
    profile_store::{is_pair_configured, is_profile_configured, StoredProfile, SyncPair},
    remote_index::RemoteIndexSnapshot,
    sync_db::DurablePlannerSummary,
    watchers::ActivePairWatcher,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatusStats {
    pub exact_match_count: u64,
    pub local_only_count: u64,
    pub remote_only_count: u64,
    pub size_mismatch_count: u64,
    pub upload_pending_count: u64,
    pub download_pending_count: u64,
    pub conflict_pending_count: u64,
}

impl Default for SyncStatusStats {
    fn default() -> Self {
        Self {
            exact_match_count: 0,
            local_only_count: 0,
            remote_only_count: 0,
            size_mismatch_count: 0,
            upload_pending_count: 0,
            download_pending_count: 0,
            conflict_pending_count: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncStatus {
    pub phase: String,
    pub last_sync_at: Option<String>,
    pub last_rescan_at: Option<String>,
    pub last_remote_refresh_at: Option<String>,
    pub last_error: Option<String>,
    pub current_folder: Option<String>,
    pub current_bucket: Option<String>,
    pub current_prefix: Option<String>,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub pending_operations: u64,
    pub indexed_file_count: u64,
    pub indexed_directory_count: u64,
    pub indexed_total_bytes: u64,
    pub remote_object_count: u64,
    pub remote_total_bytes: u64,
    pub locations: Vec<PairSyncStatus>,
    pub stats: SyncStatusStats,
    pub comparison: InventoryComparisonSummary,
    pub plan: DurablePlannerSummary,
}

impl Default for SyncStatus {
    fn default() -> Self {
        Self {
            phase: "unconfigured".into(),
            last_sync_at: None,
            last_rescan_at: None,
            last_remote_refresh_at: None,
            last_error: None,
            current_folder: None,
            current_bucket: None,
            current_prefix: None,
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            pending_operations: 0,
            indexed_file_count: 0,
            indexed_directory_count: 0,
            indexed_total_bytes: 0,
            remote_object_count: 0,
            remote_total_bytes: 0,
            locations: Vec::new(),
            stats: SyncStatusStats::default(),
            comparison: InventoryComparisonSummary::default(),
            plan: DurablePlannerSummary::default(),
        }
    }
}

#[derive(Default)]
struct PollingWorkerState {
    next_worker_id: u64,
    active_worker_id: Option<u64>,
    stop_signal: Option<Arc<AtomicBool>>,
}

struct DirtyPairState {
    last_marked_at: Instant,
}

#[derive(Default)]
struct WatcherRuntimeState {
    active_watchers: BTreeMap<String, ActivePairWatcher>,
}

#[derive(Default)]
pub struct SyncState {
    status: Mutex<SyncStatus>,
    pair_statuses: Mutex<BTreeMap<String, PairSyncStatus>>,
    polling_worker: Mutex<PollingWorkerState>,
    watcher_runtime: Mutex<WatcherRuntimeState>,
    dirty_pairs: Mutex<BTreeMap<String, DirtyPairState>>,
    cycle_running: AtomicBool,
}

pub(crate) fn get_status_lock<'a>(
    state: &'a State<'_, SyncState>,
) -> Result<std::sync::MutexGuard<'a, SyncStatus>, String> {
    state
        .status
        .lock()
        .map_err(|_| "sync status lock poisoned".to_string())
}

/// Update the global `SyncStatus` using an `AppHandle` (for use inside spawned tasks).
pub(crate) fn set_status_from_handle<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    status: SyncStatus,
) -> Result<(), String> {
    let state = app.state::<SyncState>();
    let mut lock = state
        .status
        .lock()
        .map_err(|_| "sync status lock poisoned".to_string())?;
    *lock = status;
    Ok(())
}

pub(crate) fn pair_statuses_snapshot(
    state: &State<'_, SyncState>,
) -> Result<BTreeMap<String, PairSyncStatus>, String> {
    pair_statuses_snapshot_inner(&**state)
}

fn pair_statuses_snapshot_inner(
    state: &SyncState,
) -> Result<BTreeMap<String, PairSyncStatus>, String> {
    state
        .pair_statuses
        .lock()
        .map(|lock| lock.clone())
        .map_err(|_| "pair sync status lock poisoned".to_string())
}

pub(crate) fn set_pair_status_from_handle<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    status: PairSyncStatus,
) -> Result<(), String> {
    let state = app.state::<SyncState>();
    let mut lock = state
        .pair_statuses
        .lock()
        .map_err(|_| "pair sync status lock poisoned".to_string())?;
    lock.insert(status.pair_id.clone(), status);
    Ok(())
}

pub(crate) fn replace_pair_statuses_from_handle<R: tauri::Runtime>(
    app: &tauri::AppHandle<R>,
    statuses: Vec<PairSyncStatus>,
) -> Result<(), String> {
    let state = app.state::<SyncState>();
    let mut lock = state
        .pair_statuses
        .lock()
        .map_err(|_| "pair sync status lock poisoned".to_string())?;
    *lock = statuses
        .into_iter()
        .map(|status| (status.pair_id.clone(), status))
        .collect();
    Ok(())
}

pub(crate) fn begin_polling_worker(
    state: &State<'_, SyncState>,
) -> Result<(u64, Arc<AtomicBool>), String> {
    begin_polling_worker_inner(&**state)
}

fn begin_polling_worker_inner(state: &SyncState) -> Result<(u64, Arc<AtomicBool>), String> {
    let mut worker = state
        .polling_worker
        .lock()
        .map_err(|_| "polling worker lock poisoned".to_string())?;

    if let Some(stop_signal) = worker.stop_signal.take() {
        stop_signal.store(true, Ordering::SeqCst);
    }

    worker.next_worker_id += 1;
    let worker_id = worker.next_worker_id;
    let stop_signal = Arc::new(AtomicBool::new(false));
    worker.active_worker_id = Some(worker_id);
    worker.stop_signal = Some(stop_signal.clone());
    Ok((worker_id, stop_signal))
}

pub(crate) fn stop_polling_worker(state: &State<'_, SyncState>) -> Result<bool, String> {
    stop_polling_worker_inner(&**state)
}

fn stop_polling_worker_inner(state: &SyncState) -> Result<bool, String> {
    let mut worker = state
        .polling_worker
        .lock()
        .map_err(|_| "polling worker lock poisoned".to_string())?;

    let Some(stop_signal) = worker.stop_signal.take() else {
        worker.active_worker_id = None;
        return Ok(false);
    };

    stop_signal.store(true, Ordering::SeqCst);
    worker.active_worker_id = None;
    state
        .watcher_runtime
        .lock()
        .map_err(|_| "watcher runtime lock poisoned".to_string())?
        .active_watchers
        .clear();
    state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())?
        .clear();
    Ok(true)
}

pub(crate) fn clear_polling_worker(
    state: &State<'_, SyncState>,
    worker_id: u64,
) -> Result<(), String> {
    clear_polling_worker_inner(&**state, worker_id)
}

fn clear_polling_worker_inner(state: &SyncState, worker_id: u64) -> Result<(), String> {
    let mut worker = state
        .polling_worker
        .lock()
        .map_err(|_| "polling worker lock poisoned".to_string())?;

    if worker.active_worker_id == Some(worker_id) {
        worker.active_worker_id = None;
        worker.stop_signal = None;
    }

    Ok(())
}

pub(crate) fn polling_worker_active(state: &State<'_, SyncState>) -> Result<bool, String> {
    state
        .polling_worker
        .lock()
        .map(|worker| worker.active_worker_id.is_some())
        .map_err(|_| "polling worker lock poisoned".to_string())
}

pub(crate) fn try_begin_sync_cycle(state: &State<'_, SyncState>) -> bool {
    try_begin_sync_cycle_inner(&**state)
}

fn try_begin_sync_cycle_inner(state: &SyncState) -> bool {
    state
        .cycle_running
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_ok()
}

pub(crate) fn finish_sync_cycle(state: &State<'_, SyncState>) {
    finish_sync_cycle_inner(&**state);
}

fn finish_sync_cycle_inner(state: &SyncState) {
    state.cycle_running.store(false, Ordering::SeqCst);
}

pub(crate) fn install_pair_watcher(
    state: &State<'_, SyncState>,
    pair_id: String,
    watcher: ActivePairWatcher,
) -> Result<(), String> {
    install_pair_watcher_inner(&**state, pair_id, watcher)
}

fn install_pair_watcher_inner(
    state: &SyncState,
    pair_id: String,
    watcher: ActivePairWatcher,
) -> Result<(), String> {
    state
        .watcher_runtime
        .lock()
        .map_err(|_| "watcher runtime lock poisoned".to_string())?
        .active_watchers
        .insert(pair_id, watcher);
    Ok(())
}

pub(crate) fn remove_pair_watcher(
    state: &State<'_, SyncState>,
    pair_id: &str,
) -> Result<(), String> {
    remove_pair_watcher_inner(&**state, pair_id)
}

fn remove_pair_watcher_inner(state: &SyncState, pair_id: &str) -> Result<(), String> {
    state
        .watcher_runtime
        .lock()
        .map_err(|_| "watcher runtime lock poisoned".to_string())?
        .active_watchers
        .remove(pair_id);
    state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())?
        .remove(pair_id);
    Ok(())
}

pub(crate) fn clear_all_pair_watchers(state: &State<'_, SyncState>) -> Result<(), String> {
    clear_all_pair_watchers_inner(&**state)
}

fn clear_all_pair_watchers_inner(state: &SyncState) -> Result<(), String> {
    state
        .watcher_runtime
        .lock()
        .map_err(|_| "watcher runtime lock poisoned".to_string())?
        .active_watchers
        .clear();
    Ok(())
}

pub(crate) fn active_watcher_pair_paths(
    state: &State<'_, SyncState>,
) -> Result<BTreeMap<String, std::path::PathBuf>, String> {
    active_watcher_pair_paths_inner(&**state)
}

fn active_watcher_pair_paths_inner(
    state: &SyncState,
) -> Result<BTreeMap<String, std::path::PathBuf>, String> {
    state
        .watcher_runtime
        .lock()
        .map_err(|_| "watcher runtime lock poisoned".to_string())
        .map(|runtime| {
            runtime
                .active_watchers
                .iter()
                .map(|(pair_id, watcher)| (pair_id.clone(), watcher.root_path().to_path_buf()))
                .collect()
        })
}

pub(crate) fn pair_has_active_watcher(
    state: &State<'_, SyncState>,
    pair_id: &str,
) -> Result<bool, String> {
    state
        .watcher_runtime
        .lock()
        .map_err(|_| "watcher runtime lock poisoned".to_string())
        .map(|runtime| runtime.active_watchers.contains_key(pair_id))
}

pub(crate) fn mark_pair_dirty(state: &State<'_, SyncState>, pair_id: &str) -> Result<(), String> {
    mark_pair_dirty_at_inner(&**state, pair_id, Instant::now())
}

fn mark_pair_dirty_at_inner(state: &SyncState, pair_id: &str, now: Instant) -> Result<(), String> {
    state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())?
        .insert(
            pair_id.to_string(),
            DirtyPairState {
                last_marked_at: now,
            },
        );
    Ok(())
}

pub(crate) fn due_dirty_pairs(
    state: &State<'_, SyncState>,
    now: Instant,
    debounce: Duration,
) -> Result<Vec<String>, String> {
    due_dirty_pairs_inner(&**state, now, debounce)
}

fn due_dirty_pairs_inner(
    state: &SyncState,
    now: Instant,
    debounce: Duration,
) -> Result<Vec<String>, String> {
    let dirty_pairs = state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())?;

    Ok(dirty_pairs
        .iter()
        .filter_map(|(pair_id, entry)| {
            (now.saturating_duration_since(entry.last_marked_at) >= debounce)
                .then(|| pair_id.clone())
        })
        .collect())
}

pub(crate) fn next_dirty_pair_deadline(
    state: &State<'_, SyncState>,
    debounce: Duration,
) -> Result<Option<Instant>, String> {
    state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())
        .map(|dirty_pairs| {
            dirty_pairs
                .values()
                .map(|entry| entry.last_marked_at + debounce)
                .min()
        })
}

pub(crate) fn clear_dirty_pair(state: &State<'_, SyncState>, pair_id: &str) -> Result<(), String> {
    clear_dirty_pair_inner(&**state, pair_id)
}

fn clear_dirty_pair_inner(state: &SyncState, pair_id: &str) -> Result<(), String> {
    state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())?
        .remove(pair_id);
    Ok(())
}

pub(crate) fn retain_dirty_pairs(
    state: &State<'_, SyncState>,
    pair_ids: &BTreeSet<String>,
) -> Result<(), String> {
    state
        .dirty_pairs
        .lock()
        .map_err(|_| "dirty pair lock poisoned".to_string())?
        .retain(|pair_id, _| pair_ids.contains(pair_id));
    Ok(())
}

pub(crate) fn profile_to_status(
    profile: &StoredProfile,
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    plan_summary: DurablePlannerSummary,
) -> SyncStatus {
    let matching_local_snapshot = local_snapshot.filter(|snapshot| {
        !profile.local_folder.is_empty()
            && super::local_index::snapshot_matches_folder(snapshot, &profile.local_folder)
    });

    let matching_remote_snapshot = remote_snapshot.filter(|snapshot| {
        !profile.bucket.is_empty()
            && super::remote_index::snapshot_matches_target(snapshot, &profile.bucket)
    });

    let comparison = compare_snapshots(matching_local_snapshot, matching_remote_snapshot);
    let stats = SyncStatusStats {
        exact_match_count: comparison.exact_match_count,
        local_only_count: comparison.local_only_count,
        remote_only_count: comparison.remote_only_count,
        size_mismatch_count: comparison.size_mismatch_count,
        upload_pending_count: plan_summary.upload_count,
        download_pending_count: plan_summary.download_count,
        conflict_pending_count: plan_summary.conflict_count,
    };

    SyncStatus {
        phase: if is_profile_configured(profile) {
            if profile
                .credential_profile_id
                .as_deref()
                .map(|value| !value.is_empty())
                .unwrap_or(false)
                && !profile.selected_credential_available
            {
                "error".into()
            } else {
                "idle".into()
            }
        } else {
            "unconfigured".into()
        },
        last_sync_at: None,
        last_rescan_at: matching_local_snapshot.map(|snapshot| snapshot.summary.indexed_at.clone()),
        last_remote_refresh_at: matching_remote_snapshot
            .map(|snapshot| snapshot.summary.indexed_at.clone()),
        last_error: None,
        current_folder: optional_text(&profile.local_folder),
        current_bucket: optional_text(&profile.bucket),
        current_prefix: None,
        remote_polling_enabled: profile.remote_polling_enabled,
        poll_interval_seconds: profile.poll_interval_seconds,
        pending_operations: plan_summary.pending_operation_count,
        indexed_file_count: matching_local_snapshot
            .map(|snapshot| snapshot.summary.file_count)
            .unwrap_or(0),
        indexed_directory_count: matching_local_snapshot
            .map(|snapshot| snapshot.summary.directory_count)
            .unwrap_or(0),
        indexed_total_bytes: matching_local_snapshot
            .map(|snapshot| snapshot.summary.total_bytes)
            .unwrap_or(0),
        remote_object_count: matching_remote_snapshot
            .map(|snapshot| snapshot.summary.object_count)
            .unwrap_or(0),
        remote_total_bytes: matching_remote_snapshot
            .map(|snapshot| snapshot.summary.total_bytes)
            .unwrap_or(0),
        locations: Vec::new(),
        stats,
        comparison,
        plan: plan_summary,
    }
}

fn optional_text(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

// ---------------------------------------------------------------------------
// Per-pair sync status
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PairSyncStatus {
    pub pair_id: String,
    pub pair_label: String,
    pub phase: String,
    pub last_sync_at: Option<String>,
    pub last_rescan_at: Option<String>,
    pub last_remote_refresh_at: Option<String>,
    pub last_error: Option<String>,
    pub current_folder: Option<String>,
    pub current_bucket: Option<String>,
    pub current_prefix: Option<String>,
    pub enabled: bool,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub pending_operations: u64,
    pub indexed_file_count: u64,
    pub indexed_directory_count: u64,
    pub indexed_total_bytes: u64,
    pub remote_object_count: u64,
    pub remote_total_bytes: u64,
    pub stats: SyncStatusStats,
    pub comparison: InventoryComparisonSummary,
    pub plan: DurablePlannerSummary,
}

impl Default for PairSyncStatus {
    fn default() -> Self {
        Self {
            pair_id: String::new(),
            pair_label: String::new(),
            phase: "unconfigured".into(),
            last_sync_at: None,
            last_rescan_at: None,
            last_remote_refresh_at: None,
            last_error: None,
            current_folder: None,
            current_bucket: None,
            current_prefix: None,
            enabled: true,
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            pending_operations: 0,
            indexed_file_count: 0,
            indexed_directory_count: 0,
            indexed_total_bytes: 0,
            remote_object_count: 0,
            remote_total_bytes: 0,
            stats: SyncStatusStats::default(),
            comparison: InventoryComparisonSummary::default(),
            plan: DurablePlannerSummary::default(),
        }
    }
}

pub(crate) fn pair_to_status(
    pair: &SyncPair,
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
    plan_summary: DurablePlannerSummary,
) -> PairSyncStatus {
    let matching_local_snapshot = local_snapshot.filter(|snapshot| {
        !pair.local_folder.is_empty()
            && super::local_index::snapshot_matches_folder(snapshot, &pair.local_folder)
    });

    let matching_remote_snapshot = remote_snapshot.filter(|snapshot| {
        !pair.bucket.is_empty()
            && super::remote_index::snapshot_matches_target(snapshot, &pair.bucket)
    });

    let comparison = compare_snapshots(matching_local_snapshot, matching_remote_snapshot);
    let stats = SyncStatusStats {
        exact_match_count: comparison.exact_match_count,
        local_only_count: comparison.local_only_count,
        remote_only_count: comparison.remote_only_count,
        size_mismatch_count: comparison.size_mismatch_count,
        upload_pending_count: plan_summary.upload_count,
        download_pending_count: plan_summary.download_count,
        conflict_pending_count: plan_summary.conflict_count,
    };

    let phase = if !is_pair_configured(pair) {
        "unconfigured"
    } else if !pair.enabled {
        "paused"
    } else {
        "idle"
    };

    PairSyncStatus {
        pair_id: pair.id.clone(),
        pair_label: pair.label.clone(),
        phase: phase.into(),
        last_sync_at: None,
        last_rescan_at: matching_local_snapshot.map(|snapshot| snapshot.summary.indexed_at.clone()),
        last_remote_refresh_at: matching_remote_snapshot
            .map(|snapshot| snapshot.summary.indexed_at.clone()),
        last_error: None,
        current_folder: optional_text(&pair.local_folder),
        current_bucket: optional_text(&pair.bucket),
        current_prefix: None,
        enabled: pair.enabled,
        remote_polling_enabled: pair.remote_polling_enabled,
        poll_interval_seconds: pair.poll_interval_seconds,
        pending_operations: plan_summary.pending_operation_count,
        indexed_file_count: matching_local_snapshot
            .map(|snapshot| snapshot.summary.file_count)
            .unwrap_or(0),
        indexed_directory_count: matching_local_snapshot
            .map(|snapshot| snapshot.summary.directory_count)
            .unwrap_or(0),
        indexed_total_bytes: matching_local_snapshot
            .map(|snapshot| snapshot.summary.total_bytes)
            .unwrap_or(0),
        remote_object_count: matching_remote_snapshot
            .map(|snapshot| snapshot.summary.object_count)
            .unwrap_or(0),
        remote_total_bytes: matching_remote_snapshot
            .map(|snapshot| snapshot.summary.total_bytes)
            .unwrap_or(0),
        stats,
        comparison,
        plan: plan_summary,
    }
}

// ---------------------------------------------------------------------------
// Synthesize a single SyncStatus from per-pair results (for the home screen)
// ---------------------------------------------------------------------------

pub(crate) fn synthesize_status_from_pairs(pair_statuses: &[PairSyncStatus]) -> SyncStatus {
    if pair_statuses.is_empty() {
        return SyncStatus::default();
    }

    // Phase priority: syncing > polling > error > paused > unconfigured > idle
    let phase = if pair_statuses.iter().any(|s| s.phase == "syncing") {
        "syncing"
    } else if pair_statuses.iter().any(|s| s.phase == "polling") {
        "polling"
    } else if pair_statuses.iter().any(|s| s.phase == "error") {
        "error"
    } else if pair_statuses
        .iter()
        .all(|s| s.phase == "paused" || s.phase == "unconfigured")
    {
        "paused"
    } else if pair_statuses.iter().all(|s| s.phase == "unconfigured") {
        "unconfigured"
    } else {
        "idle"
    };

    // Most-recent timestamp helper (lexicographic max of ISO-8601 strings)
    fn most_recent(opts: impl Iterator<Item = Option<String>>) -> Option<String> {
        opts.flatten().max()
    }

    let last_sync_at = most_recent(pair_statuses.iter().map(|s| s.last_sync_at.clone()));
    let last_rescan_at = most_recent(pair_statuses.iter().map(|s| s.last_rescan_at.clone()));
    let last_remote_refresh_at = most_recent(
        pair_statuses
            .iter()
            .map(|s| s.last_remote_refresh_at.clone()),
    );

    // Collect errors from pairs that have them
    let errors: Vec<&str> = pair_statuses
        .iter()
        .filter_map(|s| s.last_error.as_deref())
        .collect();
    let last_error = if errors.is_empty() {
        None
    } else {
        Some(errors.join("; "))
    };

    let n = pair_statuses.len();
    let current_folder = if n == 1 {
        pair_statuses[0].current_folder.clone()
    } else {
        Some(format!("{n} sync pairs"))
    };
    let current_bucket = if n == 1 {
        pair_statuses[0].current_bucket.clone()
    } else {
        Some(format!("{n} buckets"))
    };
    let current_prefix = if n == 1 {
        pair_statuses[0].current_prefix.clone()
    } else {
        None
    };

    let remote_polling_enabled = pair_statuses.iter().any(|s| s.remote_polling_enabled);
    let poll_interval_seconds = pair_statuses
        .iter()
        .map(|s| s.poll_interval_seconds)
        .min()
        .unwrap_or(60)
        .max(15);

    // Sum stats across all pairs
    let stats = SyncStatusStats {
        exact_match_count: pair_statuses
            .iter()
            .map(|s| s.stats.exact_match_count)
            .sum(),
        local_only_count: pair_statuses.iter().map(|s| s.stats.local_only_count).sum(),
        remote_only_count: pair_statuses
            .iter()
            .map(|s| s.stats.remote_only_count)
            .sum(),
        size_mismatch_count: pair_statuses
            .iter()
            .map(|s| s.stats.size_mismatch_count)
            .sum(),
        upload_pending_count: pair_statuses
            .iter()
            .map(|s| s.stats.upload_pending_count)
            .sum(),
        download_pending_count: pair_statuses
            .iter()
            .map(|s| s.stats.download_pending_count)
            .sum(),
        conflict_pending_count: pair_statuses
            .iter()
            .map(|s| s.stats.conflict_pending_count)
            .sum(),
    };

    // Sum comparison fields
    let comparison = InventoryComparisonSummary {
        compared_at: most_recent(
            pair_statuses
                .iter()
                .map(|s| Some(s.comparison.compared_at.clone())),
        )
        .unwrap_or_default(),
        local_file_count: pair_statuses
            .iter()
            .map(|s| s.comparison.local_file_count)
            .sum(),
        remote_object_count: pair_statuses
            .iter()
            .map(|s| s.comparison.remote_object_count)
            .sum(),
        exact_match_count: pair_statuses
            .iter()
            .map(|s| s.comparison.exact_match_count)
            .sum(),
        local_only_count: pair_statuses
            .iter()
            .map(|s| s.comparison.local_only_count)
            .sum(),
        remote_only_count: pair_statuses
            .iter()
            .map(|s| s.comparison.remote_only_count)
            .sum(),
        size_mismatch_count: pair_statuses
            .iter()
            .map(|s| s.comparison.size_mismatch_count)
            .sum(),
    };

    // Sum plan fields
    let plan = DurablePlannerSummary {
        last_planned_at: most_recent(pair_statuses.iter().map(|s| s.plan.last_planned_at.clone())),
        observed_path_count: pair_statuses
            .iter()
            .map(|s| s.plan.observed_path_count)
            .sum(),
        upload_count: pair_statuses.iter().map(|s| s.plan.upload_count).sum(),
        create_directory_count: pair_statuses
            .iter()
            .map(|s| s.plan.create_directory_count)
            .sum(),
        download_count: pair_statuses.iter().map(|s| s.plan.download_count).sum(),
        conflict_count: pair_statuses.iter().map(|s| s.plan.conflict_count).sum(),
        noop_count: pair_statuses.iter().map(|s| s.plan.noop_count).sum(),
        pending_operation_count: pair_statuses
            .iter()
            .map(|s| s.plan.pending_operation_count)
            .sum(),
        credentials_available: pair_statuses.iter().all(|s| s.plan.credentials_available),
    };

    SyncStatus {
        phase: phase.into(),
        last_sync_at,
        last_rescan_at,
        last_remote_refresh_at,
        last_error,
        current_folder,
        current_bucket,
        current_prefix,
        remote_polling_enabled,
        poll_interval_seconds,
        pending_operations: pair_statuses.iter().map(|s| s.pending_operations).sum(),
        indexed_file_count: pair_statuses.iter().map(|s| s.indexed_file_count).sum(),
        indexed_directory_count: pair_statuses
            .iter()
            .map(|s| s.indexed_directory_count)
            .sum(),
        indexed_total_bytes: pair_statuses.iter().map(|s| s.indexed_total_bytes).sum(),
        remote_object_count: pair_statuses.iter().map(|s| s.remote_object_count).sum(),
        remote_total_bytes: pair_statuses.iter().map(|s| s.remote_total_bytes).sum(),
        locations: pair_statuses.to_vec(),
        stats,
        comparison,
        plan,
    }
}

// ---------------------------------------------------------------------------
// Aggregate sync status across all pairs
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AggregateSyncStatus {
    pub pair_count: usize,
    pub enabled_pair_count: usize,
    pub configured_pair_count: usize,
    pub total_pending_operations: u64,
    pub total_indexed_file_count: u64,
    pub total_indexed_bytes: u64,
    pub total_remote_object_count: u64,
    pub total_remote_bytes: u64,
    pub aggregate_phase: String,
    pub pairs: Vec<PairSyncStatus>,
}

#[allow(dead_code)]
pub(crate) fn aggregate_pair_statuses(statuses: &[PairSyncStatus]) -> AggregateSyncStatus {
    let enabled_count = statuses.iter().filter(|s| s.enabled).count();
    let configured_count = statuses
        .iter()
        .filter(|s| s.phase != "unconfigured")
        .count();

    let aggregate_phase = if statuses.is_empty() {
        "unconfigured".into()
    } else if statuses.iter().any(|s| s.phase == "syncing") {
        "syncing".into()
    } else if statuses.iter().any(|s| s.phase == "polling") {
        "polling".into()
    } else if statuses.iter().any(|s| s.phase == "error") {
        "error".into()
    } else if statuses
        .iter()
        .all(|s| s.phase == "paused" || s.phase == "unconfigured")
    {
        "paused".into()
    } else {
        "idle".into()
    };

    AggregateSyncStatus {
        pair_count: statuses.len(),
        enabled_pair_count: enabled_count,
        configured_pair_count: configured_count,
        total_pending_operations: statuses.iter().map(|s| s.pending_operations).sum(),
        total_indexed_file_count: statuses.iter().map(|s| s.indexed_file_count).sum(),
        total_indexed_bytes: statuses.iter().map(|s| s.indexed_total_bytes).sum(),
        total_remote_object_count: statuses.iter().map(|s| s.remote_object_count).sum(),
        total_remote_bytes: statuses.iter().map(|s| s.remote_total_bytes).sum(),
        aggregate_phase,
        pairs: statuses.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        aggregate_pair_statuses, begin_polling_worker_inner, clear_dirty_pair_inner,
        clear_polling_worker_inner, due_dirty_pairs_inner, finish_sync_cycle_inner,
        mark_pair_dirty_at_inner, pair_to_status, profile_to_status, stop_polling_worker_inner,
        synthesize_status_from_pairs, try_begin_sync_cycle_inner, PairSyncStatus, SyncState,
        SyncStatusStats,
    };
    use crate::storage::{
        credentials_store::CredentialValidationStatus,
        local_index::{LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary},
        profile_store::{StoredProfile, SyncPair},
        remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry},
        sync_db::DurablePlannerSummary,
    };
    use serde_json::json;
    use std::{
        sync::atomic::Ordering,
        time::{Duration, Instant},
    };

    #[test]
    fn derives_simple_stats_from_comparison_and_plan() {
        let profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            credential_profile_id: Some("cred-1".into()),
            selected_credential_available: true,
            credentials_stored_securely: true,
            ..StoredProfile::default()
        };
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![
                LocalIndexEntry {
                    relative_path: "alpha.txt".into(),
                    kind: "file".into(),
                    size: 5,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"alpha")),
                },
                LocalIndexEntry {
                    relative_path: "beta.txt".into(),
                    kind: "file".into(),
                    size: 8,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"beta")),
                },
            ],
        };
        let remote = RemoteIndexSnapshot {
            version: 2,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary::default(),
            entries: vec![
                RemoteObjectEntry {
                    key: "alpha.txt".into(),
                    relative_path: "alpha.txt".into(),
                    kind: "file".into(),
                    size: 5,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
                RemoteObjectEntry {
                    key: "gamma.txt".into(),
                    relative_path: "gamma.txt".into(),
                    kind: "file".into(),
                    size: 11,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
            ],
        };
        let plan = DurablePlannerSummary {
            upload_count: 1,
            download_count: 1,
            conflict_count: 0,
            ..DurablePlannerSummary::default()
        };

        let status = profile_to_status(&profile, Some(&local), Some(&remote), plan);

        assert_eq!(status.stats.exact_match_count, 1);
        assert_eq!(status.stats.local_only_count, 1);
        assert_eq!(status.stats.remote_only_count, 1);
        assert_eq!(status.stats.size_mismatch_count, 0);
        assert_eq!(status.stats.upload_pending_count, 1);
        assert_eq!(status.stats.download_pending_count, 1);
        assert_eq!(status.stats.conflict_pending_count, 0);
    }

    #[test]
    fn reports_error_phase_when_selected_credential_is_missing() {
        let profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            credential_profile_id: Some("cred-1".into()),
            selected_credential_available: false,
            ..StoredProfile::default()
        };

        let status = profile_to_status(&profile, None, None, DurablePlannerSummary::default());
        assert_eq!(status.phase, "error");
    }

    #[test]
    fn keeps_idle_phase_when_credential_failed_validation_but_secret_is_present() {
        let profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            credential_profile_id: Some("cred-1".into()),
            selected_credential_available: true,
            selected_credential: Some(crate::storage::credentials_store::CredentialSummary {
                id: "cred-1".into(),
                name: "Primary".into(),
                ready: true,
                validation_status: CredentialValidationStatus::Failed,
                last_tested_at: Some("2026-04-04T12:00:00Z".into()),
                last_test_message: Some("Access denied".into()),
            }),
            credentials_stored_securely: true,
            ..StoredProfile::default()
        };

        let status = profile_to_status(&profile, None, None, DurablePlannerSummary::default());
        assert_eq!(status.phase, "idle");
    }

    #[test]
    fn stopping_polling_worker_marks_existing_signal() {
        let state = SyncState::default();
        let (_, stop_signal) = begin_polling_worker_inner(&state).expect("worker should start");

        assert!(!stop_signal.load(Ordering::SeqCst));
        assert!(stop_polling_worker_inner(&state).expect("worker should stop"));
        assert!(stop_signal.load(Ordering::SeqCst));
    }

    #[test]
    fn starting_new_polling_worker_stops_previous_signal() {
        let state = SyncState::default();
        let (first_id, first_signal) =
            begin_polling_worker_inner(&state).expect("first worker should start");
        let (second_id, second_signal) =
            begin_polling_worker_inner(&state).expect("second worker should start");

        assert_ne!(first_id, second_id);
        assert!(first_signal.load(Ordering::SeqCst));
        assert!(!second_signal.load(Ordering::SeqCst));

        clear_polling_worker_inner(&state, first_id)
            .expect("stale worker cleanup should be ignored");
        assert!(stop_polling_worker_inner(&state).expect("active worker should still exist"));
        assert!(second_signal.load(Ordering::SeqCst));
    }

    #[test]
    fn sync_cycle_lock_prevents_overlap_until_finished() {
        let state = SyncState::default();

        assert!(try_begin_sync_cycle_inner(&state));
        assert!(!try_begin_sync_cycle_inner(&state));

        finish_sync_cycle_inner(&state);

        assert!(try_begin_sync_cycle_inner(&state));
    }

    #[test]
    fn dirty_pairs_become_due_after_debounce_and_clear_after_processing() {
        let state = SyncState::default();
        let marked_at = Instant::now();

        mark_pair_dirty_at_inner(&state, "pair-a", marked_at).expect("pair should mark dirty");

        assert!(due_dirty_pairs_inner(
            &state,
            marked_at + Duration::from_millis(249),
            Duration::from_millis(250)
        )
        .expect("due pairs should read")
        .is_empty());
        assert_eq!(
            due_dirty_pairs_inner(
                &state,
                marked_at + Duration::from_millis(250),
                Duration::from_millis(250)
            )
            .expect("due pairs should read"),
            vec!["pair-a".to_string()]
        );

        mark_pair_dirty_at_inner(&state, "pair-a", marked_at).expect("pair should mark dirty");
        clear_dirty_pair_inner(&state, "pair-a").expect("dirty pair should clear");
        assert!(
            due_dirty_pairs_inner(&state, marked_at + Duration::from_secs(1), Duration::ZERO)
                .expect("due pairs should read")
                .is_empty()
        );
    }

    // --- PairSyncStatus tests ---

    #[test]
    fn pair_to_status_unconfigured_when_empty_folder_and_bucket() {
        let pair = SyncPair {
            id: "pair-1".into(),
            label: "Empty".into(),
            local_folder: String::new(),
            bucket: String::new(),
            ..SyncPair::default()
        };

        let status = pair_to_status(&pair, None, None, DurablePlannerSummary::default());
        assert_eq!(status.phase, "unconfigured");
    }

    #[test]
    fn pair_to_status_paused_when_disabled() {
        let pair = SyncPair {
            id: "pair-2".into(),
            label: "Paused".into(),
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            enabled: false,
            ..SyncPair::default()
        };

        let status = pair_to_status(&pair, None, None, DurablePlannerSummary::default());
        assert_eq!(status.phase, "paused");
    }

    #[test]
    fn pair_to_status_idle_when_configured_and_enabled() {
        let pair = SyncPair {
            id: "pair-3".into(),
            label: "Active".into(),
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            enabled: true,
            ..SyncPair::default()
        };

        let status = pair_to_status(&pair, None, None, DurablePlannerSummary::default());
        assert_eq!(status.phase, "idle");
    }

    #[test]
    fn pair_to_status_includes_pair_identity() {
        let pair = SyncPair {
            id: "pair-42".into(),
            label: "My Backup".into(),
            local_folder: "C:/data".into(),
            bucket: "backup-bucket".into(),
            ..SyncPair::default()
        };

        let status = pair_to_status(&pair, None, None, DurablePlannerSummary::default());
        assert_eq!(status.pair_id, "pair-42");
        assert_eq!(status.pair_label, "My Backup");
    }

    #[test]
    fn pair_to_status_populates_stats_from_snapshots() {
        let pair = SyncPair {
            id: "pair-5".into(),
            label: "Snap".into(),
            local_folder: "C:/sync".into(),
            bucket: "demo".into(),
            ..SyncPair::default()
        };
        let local = LocalIndexSnapshot {
            version: 1,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary {
                indexed_at: "2026-04-04T10:00:00Z".into(),
                file_count: 3,
                directory_count: 1,
                total_bytes: 100,
            },
            entries: vec![
                LocalIndexEntry {
                    relative_path: "alpha.txt".into(),
                    kind: "file".into(),
                    size: 10,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"alpha")),
                },
                LocalIndexEntry {
                    relative_path: "beta.txt".into(),
                    kind: "file".into(),
                    size: 20,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"beta")),
                },
                LocalIndexEntry {
                    relative_path: "gamma.txt".into(),
                    kind: "file".into(),
                    size: 70,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"gamma")),
                },
            ],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-04-04T10:05:00Z".into(),
                object_count: 2,
                total_bytes: 35,
            },
            entries: vec![
                RemoteObjectEntry {
                    key: "alpha.txt".into(),
                    relative_path: "alpha.txt".into(),
                    kind: "file".into(),
                    size: 10,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
                RemoteObjectEntry {
                    key: "delta.txt".into(),
                    relative_path: "delta.txt".into(),
                    kind: "file".into(),
                    size: 25,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
            ],
        };
        let plan = DurablePlannerSummary {
            upload_count: 2,
            download_count: 1,
            conflict_count: 0,
            pending_operation_count: 3,
            ..DurablePlannerSummary::default()
        };

        let status = pair_to_status(&pair, Some(&local), Some(&remote), plan);

        assert_eq!(status.indexed_file_count, 3);
        assert_eq!(status.indexed_directory_count, 1);
        assert_eq!(status.indexed_total_bytes, 100);
        assert_eq!(status.remote_object_count, 2);
        assert_eq!(status.remote_total_bytes, 35);
        assert_eq!(status.pending_operations, 3);
        assert_eq!(status.stats.exact_match_count, 1);
        assert_eq!(status.stats.local_only_count, 2);
        assert_eq!(status.stats.remote_only_count, 1);
        assert_eq!(status.stats.size_mismatch_count, 0);
        assert_eq!(status.stats.upload_pending_count, 2);
        assert_eq!(status.stats.download_pending_count, 1);
        assert_eq!(status.stats.conflict_pending_count, 0);
        assert_eq!(
            status.last_rescan_at.as_deref(),
            Some("2026-04-04T10:00:00Z")
        );
        assert_eq!(
            status.last_remote_refresh_at.as_deref(),
            Some("2026-04-04T10:05:00Z")
        );
    }

    // --- AggregateSyncStatus tests ---

    #[test]
    fn aggregate_reports_syncing_when_any_pair_syncing() {
        let statuses = vec![
            PairSyncStatus {
                pair_id: "a".into(),
                pair_label: "A".into(),
                phase: "idle".into(),
                ..PairSyncStatus::default()
            },
            PairSyncStatus {
                pair_id: "b".into(),
                pair_label: "B".into(),
                phase: "syncing".into(),
                ..PairSyncStatus::default()
            },
        ];

        let aggregate = aggregate_pair_statuses(&statuses);
        assert_eq!(aggregate.aggregate_phase, "syncing");
        assert_eq!(aggregate.pair_count, 2);
    }

    #[test]
    fn aggregate_reports_idle_when_all_idle() {
        let statuses = vec![
            PairSyncStatus {
                pair_id: "a".into(),
                pair_label: "A".into(),
                phase: "idle".into(),
                ..PairSyncStatus::default()
            },
            PairSyncStatus {
                pair_id: "b".into(),
                pair_label: "B".into(),
                phase: "idle".into(),
                ..PairSyncStatus::default()
            },
        ];

        let aggregate = aggregate_pair_statuses(&statuses);
        assert_eq!(aggregate.aggregate_phase, "idle");
    }

    #[test]
    fn aggregate_reports_unconfigured_when_no_pairs() {
        let aggregate = aggregate_pair_statuses(&[]);
        assert_eq!(aggregate.aggregate_phase, "unconfigured");
        assert_eq!(aggregate.pair_count, 0);
        assert_eq!(aggregate.enabled_pair_count, 0);
        assert_eq!(aggregate.configured_pair_count, 0);
    }

    #[test]
    fn aggregate_sums_pending_operations_across_pairs() {
        let statuses = vec![
            PairSyncStatus {
                pair_id: "a".into(),
                pair_label: "A".into(),
                phase: "idle".into(),
                pending_operations: 5,
                indexed_file_count: 10,
                indexed_total_bytes: 1000,
                remote_object_count: 8,
                remote_total_bytes: 800,
                enabled: true,
                ..PairSyncStatus::default()
            },
            PairSyncStatus {
                pair_id: "b".into(),
                pair_label: "B".into(),
                phase: "idle".into(),
                pending_operations: 3,
                indexed_file_count: 20,
                indexed_total_bytes: 2000,
                remote_object_count: 15,
                remote_total_bytes: 1500,
                enabled: true,
                ..PairSyncStatus::default()
            },
            PairSyncStatus {
                pair_id: "c".into(),
                pair_label: "C".into(),
                phase: "paused".into(),
                pending_operations: 2,
                indexed_file_count: 5,
                indexed_total_bytes: 500,
                remote_object_count: 3,
                remote_total_bytes: 300,
                enabled: false,
                ..PairSyncStatus::default()
            },
        ];

        let aggregate = aggregate_pair_statuses(&statuses);
        assert_eq!(aggregate.total_pending_operations, 10);
        assert_eq!(aggregate.total_indexed_file_count, 35);
        assert_eq!(aggregate.total_indexed_bytes, 3500);
        assert_eq!(aggregate.total_remote_object_count, 26);
        assert_eq!(aggregate.total_remote_bytes, 2600);
        assert_eq!(aggregate.pair_count, 3);
        assert_eq!(aggregate.enabled_pair_count, 2);
        assert_eq!(aggregate.configured_pair_count, 3);
        assert_eq!(aggregate.pairs.len(), 3);
    }

    // --- synthesize_status_from_pairs tests ---

    #[test]
    fn synthesize_empty_input_returns_default_unconfigured() {
        let status = synthesize_status_from_pairs(&[]);
        assert_eq!(status.phase, "unconfigured");
        assert_eq!(status.pending_operations, 0);
        assert_eq!(status.indexed_file_count, 0);
        assert!(status.last_sync_at.is_none());
        assert!(status.current_folder.is_none());
        assert!(status.locations.is_empty());
    }

    #[test]
    fn synthesize_single_pair_matches_pair_data() {
        let pair = PairSyncStatus {
            pair_id: "p1".into(),
            pair_label: "Pair One".into(),
            phase: "idle".into(),
            last_sync_at: Some("2026-04-04T10:00:00Z".into()),
            last_rescan_at: Some("2026-04-04T09:00:00Z".into()),
            last_remote_refresh_at: Some("2026-04-04T09:30:00Z".into()),
            last_error: None,
            current_folder: Some("C:/sync".into()),
            current_bucket: Some("my-bucket".into()),
            current_prefix: None,
            enabled: true,
            remote_polling_enabled: true,
            poll_interval_seconds: 30,
            pending_operations: 5,
            indexed_file_count: 10,
            indexed_directory_count: 2,
            indexed_total_bytes: 1000,
            remote_object_count: 8,
            remote_total_bytes: 800,
            stats: SyncStatusStats {
                exact_match_count: 6,
                local_only_count: 2,
                remote_only_count: 1,
                size_mismatch_count: 1,
                upload_pending_count: 3,
                download_pending_count: 1,
                conflict_pending_count: 1,
            },
            comparison: Default::default(),
            plan: DurablePlannerSummary {
                last_planned_at: Some("2026-04-04T09:50:00Z".into()),
                observed_path_count: 12,
                upload_count: 3,
                create_directory_count: 1,
                download_count: 1,
                conflict_count: 1,
                noop_count: 7,
                pending_operation_count: 5,
                credentials_available: true,
            },
        };

        let status = synthesize_status_from_pairs(&[pair]);
        assert_eq!(status.phase, "idle");
        assert_eq!(status.pending_operations, 5);
        assert_eq!(status.indexed_file_count, 10);
        assert_eq!(status.indexed_directory_count, 2);
        assert_eq!(status.indexed_total_bytes, 1000);
        assert_eq!(status.remote_object_count, 8);
        assert_eq!(status.remote_total_bytes, 800);
        assert_eq!(status.current_folder.as_deref(), Some("C:/sync"));
        assert_eq!(status.current_bucket.as_deref(), Some("my-bucket"));
        assert!(status.current_prefix.is_none());
        assert_eq!(status.last_sync_at.as_deref(), Some("2026-04-04T10:00:00Z"));
        assert_eq!(status.poll_interval_seconds, 30);
        assert!(status.remote_polling_enabled);
        assert_eq!(status.stats.upload_pending_count, 3);
        assert_eq!(status.plan.upload_count, 3);
        assert!(status.plan.credentials_available);
        assert_eq!(status.locations.len(), 1);
        assert_eq!(status.locations[0].pair_id, "p1");
    }

    #[test]
    fn synthesize_multiple_pairs_sums_and_picks_highest_priority_phase() {
        let pairs = vec![
            PairSyncStatus {
                pair_id: "a".into(),
                pair_label: "A".into(),
                phase: "idle".into(),
                last_sync_at: Some("2026-04-04T08:00:00Z".into()),
                last_error: None,
                current_folder: Some("C:/a".into()),
                current_bucket: Some("bucket-a".into()),
                current_prefix: None,
                remote_polling_enabled: false,
                poll_interval_seconds: 60,
                pending_operations: 3,
                indexed_file_count: 10,
                indexed_directory_count: 1,
                indexed_total_bytes: 500,
                remote_object_count: 7,
                remote_total_bytes: 400,
                stats: SyncStatusStats {
                    exact_match_count: 5,
                    local_only_count: 3,
                    remote_only_count: 2,
                    size_mismatch_count: 0,
                    upload_pending_count: 2,
                    download_pending_count: 1,
                    conflict_pending_count: 0,
                },
                plan: DurablePlannerSummary {
                    upload_count: 2,
                    create_directory_count: 1,
                    download_count: 1,
                    pending_operation_count: 3,
                    credentials_available: true,
                    ..DurablePlannerSummary::default()
                },
                ..PairSyncStatus::default()
            },
            PairSyncStatus {
                pair_id: "b".into(),
                pair_label: "B".into(),
                phase: "error".into(),
                last_sync_at: Some("2026-04-04T09:00:00Z".into()),
                last_error: Some("upload failed".into()),
                current_folder: Some("C:/b".into()),
                current_bucket: Some("bucket-b".into()),
                current_prefix: None,
                remote_polling_enabled: true,
                poll_interval_seconds: 30,
                pending_operations: 7,
                indexed_file_count: 20,
                indexed_directory_count: 3,
                indexed_total_bytes: 2000,
                remote_object_count: 15,
                remote_total_bytes: 1500,
                stats: SyncStatusStats {
                    exact_match_count: 10,
                    local_only_count: 5,
                    remote_only_count: 3,
                    size_mismatch_count: 2,
                    upload_pending_count: 4,
                    download_pending_count: 2,
                    conflict_pending_count: 1,
                },
                plan: DurablePlannerSummary {
                    upload_count: 4,
                    create_directory_count: 2,
                    download_count: 2,
                    conflict_count: 1,
                    pending_operation_count: 7,
                    credentials_available: true,
                    ..DurablePlannerSummary::default()
                },
                ..PairSyncStatus::default()
            },
        ];

        let status = synthesize_status_from_pairs(&pairs);

        // error > idle
        assert_eq!(status.phase, "error");

        // Sums
        assert_eq!(status.pending_operations, 10);
        assert_eq!(status.indexed_file_count, 30);
        assert_eq!(status.indexed_directory_count, 4);
        assert_eq!(status.indexed_total_bytes, 2500);
        assert_eq!(status.remote_object_count, 22);
        assert_eq!(status.remote_total_bytes, 1900);
        assert_eq!(status.stats.exact_match_count, 15);
        assert_eq!(status.stats.upload_pending_count, 6);
        assert_eq!(status.stats.download_pending_count, 3);
        assert_eq!(status.stats.conflict_pending_count, 1);

        // Most recent timestamp
        assert_eq!(status.last_sync_at.as_deref(), Some("2026-04-04T09:00:00Z"));

        // Error joined
        assert_eq!(status.last_error.as_deref(), Some("upload failed"));

        // Multi-pair labels
        assert_eq!(status.current_folder.as_deref(), Some("2 sync pairs"));
        assert_eq!(status.current_bucket.as_deref(), Some("2 buckets"));
        assert!(status.current_prefix.is_none());

        // Polling: true if any pair has it
        assert!(status.remote_polling_enabled);
        // Min of 60, 30 → 30
        assert_eq!(status.poll_interval_seconds, 30);

        // Plan sums
        assert_eq!(status.plan.upload_count, 6);
        assert_eq!(status.plan.create_directory_count, 3);
        assert_eq!(status.plan.download_count, 3);
        assert_eq!(status.plan.pending_operation_count, 10);
        assert_eq!(status.locations.len(), 2);
        assert_eq!(status.locations[0].pair_id, "a");
        assert_eq!(status.locations[1].pair_id, "b");
    }

    #[test]
    fn sync_status_serializes_locations_in_camel_case() {
        let status = synthesize_status_from_pairs(&[PairSyncStatus {
            pair_id: "pair-1".into(),
            pair_label: "Primary".into(),
            last_sync_at: Some("2026-04-04T10:00:00Z".into()),
            ..PairSyncStatus::default()
        }]);

        let value = serde_json::to_value(&status).expect("status should serialize");

        assert_eq!(
            value.get("locations"),
            Some(&json!([{
                "pairId": "pair-1",
                "pairLabel": "Primary",
                "phase": "unconfigured",
                "lastSyncAt": "2026-04-04T10:00:00Z",
                "lastRescanAt": null,
                "lastRemoteRefreshAt": null,
                "lastError": null,
                "currentFolder": null,
                "currentBucket": null,
                "currentPrefix": null,
                "enabled": true,
                "remotePollingEnabled": true,
                "pollIntervalSeconds": 60,
                "pendingOperations": 0,
                "indexedFileCount": 0,
                "indexedDirectoryCount": 0,
                "indexedTotalBytes": 0,
                "remoteObjectCount": 0,
                "remoteTotalBytes": 0,
                "stats": {
                    "exactMatchCount": 0,
                    "localOnlyCount": 0,
                    "remoteOnlyCount": 0,
                    "sizeMismatchCount": 0,
                    "uploadPendingCount": 0,
                    "downloadPendingCount": 0,
                    "conflictPendingCount": 0
                },
                "comparison": {
                    "comparedAt": "",
                    "localFileCount": 0,
                    "remoteObjectCount": 0,
                    "exactMatchCount": 0,
                    "localOnlyCount": 0,
                    "remoteOnlyCount": 0,
                    "sizeMismatchCount": 0
                },
                "plan": {
                    "lastPlannedAt": null,
                    "observedPathCount": 0,
                    "uploadCount": 0,
                    "createDirectoryCount": 0,
                    "downloadCount": 0,
                    "conflictCount": 0,
                    "noopCount": 0,
                    "pendingOperationCount": 0,
                    "credentialsAvailable": false
                }
            }]))
        );
    }
}
