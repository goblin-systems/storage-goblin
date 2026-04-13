use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WatchTarget {
    pub pair_id: String,
    pub root_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct WatchReconciliationPlan {
    pub start: Vec<WatchTarget>,
    pub stop: Vec<String>,
}

pub(crate) struct ActivePairWatcher {
    root_path: PathBuf,
    _watcher: RecommendedWatcher,
}

impl ActivePairWatcher {
    pub(crate) fn root_path(&self) -> &Path {
        &self.root_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WatcherCallbackEvent {
    LocalChange,
    Degraded(String),
}

pub(crate) fn start_pair_watcher<F>(
    root_path: &Path,
    mut callback: F,
) -> Result<ActivePairWatcher, String>
where
    F: FnMut(WatcherCallbackEvent) + Send + 'static,
{
    let watched_path = root_path.to_path_buf();
    let callback_path = watched_path.clone();

    let mut watcher =
        notify::recommended_watcher(move |result: notify::Result<Event>| match result {
            Ok(event) if event_should_mark_dirty(&event) => {
                callback(WatcherCallbackEvent::LocalChange);
            }
            Ok(_) => {}
            Err(error) => callback(WatcherCallbackEvent::Degraded(format!(
                "Filesystem watcher error for '{}': {error}",
                callback_path.display()
            ))),
        })
        .map_err(|error| {
            format!(
                "Failed to create filesystem watcher for '{}': {error}",
                watched_path.display()
            )
        })?;

    watcher
        .watch(root_path, RecursiveMode::Recursive)
        .map_err(|error| {
            format!(
                "Failed to watch local folder '{}': {error}",
                root_path.display()
            )
        })?;

    Ok(ActivePairWatcher {
        root_path: watched_path,
        _watcher: watcher,
    })
}

pub(crate) fn plan_watch_reconciliation(
    current: &BTreeMap<String, PathBuf>,
    desired: &[WatchTarget],
) -> WatchReconciliationPlan {
    let desired_map: BTreeMap<String, PathBuf> = desired
        .iter()
        .map(|target| (target.pair_id.clone(), target.root_path.clone()))
        .collect();

    let mut stop = Vec::new();
    let mut start = Vec::new();

    for (pair_id, current_path) in current {
        match desired_map.get(pair_id) {
            Some(desired_path) if desired_path == current_path => {}
            Some(_) | None => stop.push(pair_id.clone()),
        }
    }

    for target in desired {
        if current.get(&target.pair_id) != Some(&target.root_path) {
            start.push(target.clone());
        }
    }

    WatchReconciliationPlan { start, stop }
}

fn event_should_mark_dirty(event: &Event) -> bool {
    !matches!(event.kind, EventKind::Access(_))
}

#[cfg(test)]
mod tests {
    use super::{event_should_mark_dirty, plan_watch_reconciliation, WatchTarget};
    use notify::{event::AccessKind, Event, EventKind};
    use std::{collections::BTreeMap, path::PathBuf};

    #[test]
    fn reconciliation_starts_new_watchers_and_stops_removed_or_retargeted_pairs() {
        let current = BTreeMap::from([
            ("pair-a".to_string(), PathBuf::from("C:/a")),
            ("pair-b".to_string(), PathBuf::from("C:/b")),
        ]);
        let desired = vec![
            WatchTarget {
                pair_id: "pair-a".into(),
                root_path: PathBuf::from("C:/a"),
            },
            WatchTarget {
                pair_id: "pair-b".into(),
                root_path: PathBuf::from("D:/retargeted-b"),
            },
            WatchTarget {
                pair_id: "pair-c".into(),
                root_path: PathBuf::from("C:/c"),
            },
        ];

        let plan = plan_watch_reconciliation(&current, &desired);

        assert_eq!(plan.stop, vec!["pair-b".to_string()]);
        assert_eq!(plan.start.len(), 2);
        assert!(plan.start.iter().any(|target| {
            target.pair_id == "pair-b" && target.root_path == PathBuf::from("D:/retargeted-b")
        }));
        assert!(plan.start.iter().any(|target| {
            target.pair_id == "pair-c" && target.root_path == PathBuf::from("C:/c")
        }));
    }

    #[test]
    fn access_events_do_not_mark_pairs_dirty() {
        let event = Event {
            kind: EventKind::Access(AccessKind::Any),
            paths: Vec::new(),
            attrs: Default::default(),
        };

        assert!(!event_should_mark_dirty(&event));
    }

    #[test]
    fn content_events_mark_pairs_dirty() {
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: Vec::new(),
            attrs: Default::default(),
        };

        assert!(event_should_mark_dirty(&event));
    }
}
