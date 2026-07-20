//! Deciding when each sync location should run.
//!
//! Combines the configured poll interval with filesystem-watcher signals:
//! a location becomes due either when its interval elapses or when the
//! watcher marks it dirty and the debounce window closes.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::local_index::LocalIndexSnapshot;
use super::profile_store::{is_pair_configured, StoredProfile, SyncPair};
use super::sync_state::PairSyncStatus;
use super::watchers::WatchTarget;

pub(crate) fn should_poll_pair(pair: &SyncPair) -> bool {
    pair.enabled && pair.remote_polling_enabled && is_pair_configured(pair)
}

pub(crate) fn next_polling_deadline_at(
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
pub(crate) fn next_polling_deadline(
    pair: &SyncPair,
    status: Option<&PairSyncStatus>,
) -> tokio::time::Instant {
    next_polling_deadline_at(tokio::time::Instant::now(), pair, status)
}

pub(crate) fn due_polling_pairs(
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

pub(crate) fn watcher_eligible_pairs(profile: &StoredProfile) -> Vec<SyncPair> {
    profile
        .sync_pairs
        .iter()
        .filter(|pair| should_poll_pair(pair))
        .cloned()
        .collect()
}

pub(crate) fn pair_watch_target(pair: &SyncPair) -> Option<WatchTarget> {
    should_poll_pair(pair).then(|| WatchTarget {
        pair_id: pair.id.clone(),
        root_path: PathBuf::from(&pair.local_folder),
    })
}

pub(crate) fn snapshot_age(value: &str, now: time::OffsetDateTime) -> Option<Duration> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    let elapsed = now - parsed;
    if elapsed.is_negative() {
        Some(Duration::ZERO)
    } else {
        elapsed.try_into().ok()
    }
}

pub(crate) fn local_snapshot_is_fresh(snapshot: &LocalIndexSnapshot, ttl: Duration) -> bool {
    snapshot_age(
        &snapshot.summary.indexed_at,
        time::OffsetDateTime::now_utc(),
    )
    .is_some_and(|age| age <= ttl)
}

pub(crate) fn should_scan_local_for_trigger(
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

pub(crate) fn parse_poll_anchor_age(value: &str) -> Option<Duration> {
    snapshot_age(value, time::OffsetDateTime::now_utc())
}

pub(crate) fn stop_requested(stop_signal: Option<&AtomicBool>) -> bool {
    stop_signal
        .map(|signal| signal.load(Ordering::SeqCst))
        .unwrap_or(false)
}

pub(crate) fn active_pair_for_manual_actions(profile: &StoredProfile) -> Option<SyncPair> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PairSyncTrigger {
    Manual,
    LocalDirty,
    RemotePoll,
}
