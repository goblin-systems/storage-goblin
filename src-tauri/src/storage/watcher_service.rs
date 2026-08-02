//! Keeping filesystem watchers in step with the configured locations.
//!
//! Split out of `commands` (backlog phase 3.3): reconciling watchers is an
//! app-lifecycle concern, not part of the IPC surface, and it is the piece that
//! knows a removed location must also have its in-flight cycle cancelled and
//! its runtime state forgotten.

use std::collections::BTreeSet;

use tauri::{AppHandle, Manager, Runtime};

use super::activity::ActivityDebugState;
use super::commands::emit_info_activity;
use super::polling_service::{pair_watch_target, watcher_eligible_pairs};
use super::profile_store::{StoredProfile, SyncPair};
use super::sync_state::{
    active_watcher_pair_paths, coordinator, install_pair_watcher, mark_pair_dirty,
    remove_pair_watcher, retain_dirty_pairs, SyncState,
};
use super::watchers::{
    plan_watch_reconciliation, start_pair_watcher, WatchTarget, WatcherCallbackEvent,
};

pub(crate) fn emit_watcher_degraded_activity<R: Runtime>(
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

pub(crate) fn reconcile_pair_watchers(
    app: &AppHandle,
    profile: &StoredProfile,
) -> Result<(), String> {
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
        // A location that is no longer eligible must not keep syncing in the
        // background; cancel any cycle it still has in flight.
        coordinator(&state).cancel_pair(&pair_id);
        remove_pair_watcher(&state, &pair_id)?;
    }

    retain_dirty_pairs(&state, &desired_ids)?;
    // Runtime entries would otherwise accumulate for the life of the process
    // as locations are added and removed.
    coordinator(&state).retain_pairs(
        &profile
            .sync_pairs
            .iter()
            .map(|pair| pair.id.clone())
            .collect::<Vec<_>>(),
    );

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
                WatcherCallbackEvent::LocalChange(paths) => {
                    let _ = mark_pair_dirty(&state, &pair_id, &paths);
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
