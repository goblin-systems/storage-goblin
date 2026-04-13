use std::collections::{BTreeMap, BTreeSet};

use super::{
    local_index::LocalIndexSnapshot,
    now_iso,
    profile_store::normalize_conflict_strategy,
    remote_index::{is_glacier_storage_class, RemoteIndexSnapshot},
    sync_db::SyncAnchor,
};

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalIndexedEntry {
    kind: String,
    size: u64,
    fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteIndexedEntry {
    kind: String,
    size: u64,
    etag: Option<String>,
    storage_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedEntry {
    pub path: String,
    pub local_size: Option<u64>,
    pub remote_size: Option<u64>,
    pub resolution: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedQueueItem {
    pub path: String,
    pub operation: String,
    pub local_size: Option<u64>,
    pub remote_size: Option<u64>,
    pub expected_local_fingerprint: Option<String>,
    pub expected_remote_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPlanSummary {
    pub planned_at: String,
    pub local_file_count: u64,
    pub remote_object_count: u64,
    pub observed_path_count: u64,
    pub upload_count: u64,
    pub create_directory_count: u64,
    pub download_count: u64,
    pub conflict_count: u64,
    pub noop_count: u64,
    pub pending_operation_count: u64,
    pub credentials_available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncPlan {
    pub summary: SyncPlanSummary,
    pub observed_entries: Vec<ObservedEntry>,
    pub queue_items: Vec<PlannedQueueItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileSyncDecision {
    Noop,
    Upload,
    Download,
    ConflictReview,
    ReviewRequired,
}

pub(crate) fn file_entry_status(
    anchor: Option<&SyncAnchor>,
    current_local_fingerprint: Option<&str>,
    current_remote_etag: Option<&str>,
) -> &'static str {
    match decide_file_sync(
        anchor,
        current_local_fingerprint,
        current_remote_etag,
        "preserve-both",
    ) {
        FileSyncDecision::Noop => "synced",
        FileSyncDecision::Upload => "local-only",
        FileSyncDecision::Download => "remote-only",
        FileSyncDecision::ConflictReview => "conflict",
        FileSyncDecision::ReviewRequired => "review-required",
    }
}

pub fn build_sync_plan(
    local_snapshot: &LocalIndexSnapshot,
    remote_snapshot: &RemoteIndexSnapshot,
    anchors: &BTreeMap<String, SyncAnchor>,
    conflict_strategy: &str,
    credentials_available: bool,
) -> SyncPlan {
    let normalized_conflict_strategy = normalize_conflict_strategy(conflict_strategy);
    let local_entries = local_entry_map(local_snapshot);
    let remote_entries = remote_entry_map(remote_snapshot);
    let local_file_count = local_entries
        .values()
        .filter(|entry| entry.kind == "file")
        .count() as u64;
    let remote_object_count = remote_entries
        .values()
        .filter(|entry| {
            entry.kind == "file" && !is_glacier_storage_class(entry.storage_class.as_deref())
        })
        .count() as u64;

    let mut paths = BTreeSet::new();
    paths.extend(local_entries.keys().cloned());
    paths.extend(remote_entries.keys().cloned());

    let planned_at = now_iso();
    let mut observed_entries = Vec::with_capacity(paths.len());
    let mut queue_items = Vec::new();
    let mut upload_count = 0_u64;
    let mut create_directory_count = 0_u64;
    let mut download_count = 0_u64;
    let mut conflict_count = 0_u64;
    let mut noop_count = 0_u64;

    for path in paths {
        let local = local_entries.get(&path);
        let remote = remote_entries.get(&path);
        let anchor = anchors.get(&path);

        if remote.is_some_and(|entry| is_glacier_storage_class(entry.storage_class.as_deref())) {
            noop_count += 1;
            observed_entries.push(ObservedEntry {
                path,
                local_size: local
                    .map(|entry| entry.size)
                    .filter(|_| local.is_some_and(|entry| entry.kind == "file")),
                remote_size: remote
                    .map(|entry| entry.size)
                    .filter(|_| remote.is_some_and(|entry| entry.kind == "file")),
                resolution: "noop".into(),
            });
            continue;
        }

        match (local, remote) {
            (Some(local), None) if local.kind == "directory" => {
                create_directory_count += 1;
                observed_entries.push(ObservedEntry {
                    path: path.clone(),
                    local_size: None,
                    remote_size: None,
                    resolution: "create_directory".into(),
                });
                queue_items.push(PlannedQueueItem {
                    path,
                    operation: "create_directory".into(),
                    local_size: None,
                    remote_size: None,
                    expected_local_fingerprint: None,
                    expected_remote_etag: None,
                });
            }
            (Some(local), Some(remote))
                if local.kind == "directory" && remote.kind == "directory" =>
            {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: None,
                    remote_size: None,
                    resolution: "noop".into(),
                });
            }
            (None, Some(remote)) if remote.kind == "directory" => {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: None,
                    remote_size: None,
                    resolution: "noop".into(),
                });
            }
            (Some(local), Some(remote)) if local.kind != remote.kind => {
                conflict_count += 1;
                observed_entries.push(ObservedEntry {
                    path: path.clone(),
                    local_size: file_size(local),
                    remote_size: file_size(remote),
                    resolution: "conflict_review".into(),
                });
                queue_items.push(PlannedQueueItem {
                    path,
                    operation: "conflict_review".into(),
                    local_size: file_size(local),
                    remote_size: file_size(remote),
                    expected_local_fingerprint: None,
                    expected_remote_etag: None,
                });
            }
            (Some(local), None) if local.kind == "file" => {
                let decision = decide_file_sync(
                    anchor,
                    local.fingerprint.as_deref(),
                    None,
                    normalized_conflict_strategy.as_str(),
                );
                push_file_decision(
                    &mut observed_entries,
                    &mut queue_items,
                    &mut upload_count,
                    &mut download_count,
                    &mut conflict_count,
                    &mut noop_count,
                    &path,
                    Some(local.size),
                    None,
                    local.fingerprint.clone(),
                    None,
                    decision,
                );
            }
            (None, Some(remote)) if remote.kind == "file" => {
                let decision = decide_file_sync(
                    anchor,
                    None,
                    remote.etag.as_deref(),
                    normalized_conflict_strategy.as_str(),
                );
                push_file_decision(
                    &mut observed_entries,
                    &mut queue_items,
                    &mut upload_count,
                    &mut download_count,
                    &mut conflict_count,
                    &mut noop_count,
                    &path,
                    None,
                    Some(remote.size),
                    None,
                    remote.etag.clone(),
                    decision,
                );
            }
            (Some(local), Some(remote)) if local.kind == "file" && remote.kind == "file" => {
                let decision = decide_file_sync(
                    anchor,
                    local.fingerprint.as_deref(),
                    remote.etag.as_deref(),
                    normalized_conflict_strategy.as_str(),
                );
                push_file_decision(
                    &mut observed_entries,
                    &mut queue_items,
                    &mut upload_count,
                    &mut download_count,
                    &mut conflict_count,
                    &mut noop_count,
                    &path,
                    Some(local.size),
                    Some(remote.size),
                    local.fingerprint.clone(),
                    remote.etag.clone(),
                    decision,
                );
            }
            (None, None) => {}
            _ => {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: local.and_then(file_size),
                    remote_size: remote.and_then(file_size),
                    resolution: "noop".into(),
                });
            }
        }
    }

    SyncPlan {
        summary: SyncPlanSummary {
            planned_at,
            local_file_count,
            remote_object_count,
            observed_path_count: observed_entries.len() as u64,
            upload_count,
            create_directory_count,
            download_count,
            conflict_count,
            noop_count,
            pending_operation_count: queue_items.len() as u64,
            credentials_available,
        },
        observed_entries,
        queue_items,
    }
}

fn push_file_decision(
    observed_entries: &mut Vec<ObservedEntry>,
    queue_items: &mut Vec<PlannedQueueItem>,
    upload_count: &mut u64,
    download_count: &mut u64,
    conflict_count: &mut u64,
    noop_count: &mut u64,
    path: &str,
    local_size: Option<u64>,
    remote_size: Option<u64>,
    expected_local_fingerprint: Option<String>,
    expected_remote_etag: Option<String>,
    decision: FileSyncDecision,
) {
    let (resolution, operation) = match decision {
        FileSyncDecision::Noop => {
            *noop_count += 1;
            ("noop", None)
        }
        FileSyncDecision::Upload => {
            *upload_count += 1;
            ("upload", Some("upload"))
        }
        FileSyncDecision::Download => {
            *download_count += 1;
            ("download", Some("download"))
        }
        FileSyncDecision::ConflictReview => {
            *conflict_count += 1;
            ("conflict_review", Some("conflict_review"))
        }
        FileSyncDecision::ReviewRequired => {
            *conflict_count += 1;
            ("review_required", Some("review_required"))
        }
    };

    observed_entries.push(ObservedEntry {
        path: path.into(),
        local_size,
        remote_size,
        resolution: resolution.into(),
    });

    if let Some(operation) = operation {
        queue_items.push(PlannedQueueItem {
            path: path.into(),
            operation: operation.into(),
            local_size,
            remote_size,
            expected_local_fingerprint,
            expected_remote_etag,
        });
    }
}

fn decide_file_sync(
    anchor: Option<&SyncAnchor>,
    current_local_fingerprint: Option<&str>,
    current_remote_etag: Option<&str>,
    conflict_strategy: &str,
) -> FileSyncDecision {
    let Some(anchor) = anchor.filter(|anchor| anchor.kind == "file") else {
        return if current_local_fingerprint.is_some() && current_remote_etag.is_some() {
            FileSyncDecision::ReviewRequired
        } else if current_local_fingerprint.is_some() {
            FileSyncDecision::Upload
        } else if current_remote_etag.is_some() {
            FileSyncDecision::Download
        } else {
            FileSyncDecision::Noop
        };
    };

    let local_changed = anchor.local_fingerprint.as_deref() != current_local_fingerprint;
    let remote_changed = anchor.remote_etag.as_deref() != current_remote_etag;

    match (
        current_local_fingerprint,
        current_remote_etag,
        local_changed,
        remote_changed,
    ) {
        (Some(_), Some(_), false, false) => FileSyncDecision::Noop,
        (Some(_), Some(_), true, false) => FileSyncDecision::Upload,
        (Some(_), Some(_), false, true) => FileSyncDecision::Download,
        (Some(_), Some(_), true, true) => match conflict_strategy {
            "prefer-local" => FileSyncDecision::Upload,
            "prefer-remote" => FileSyncDecision::Download,
            _ => FileSyncDecision::ConflictReview,
        },
        (Some(_), None, true, false) if anchor.remote_etag.is_none() => FileSyncDecision::Upload,
        (None, Some(_), false, true) if anchor.local_fingerprint.is_none() => {
            FileSyncDecision::Download
        }
        (Some(_), None, false, false) if anchor.remote_etag.is_none() => FileSyncDecision::Noop,
        (None, Some(_), false, false) if anchor.local_fingerprint.is_none() => {
            FileSyncDecision::Noop
        }
        _ => FileSyncDecision::ReviewRequired,
    }
}

fn file_size<T>(entry: &T) -> Option<u64>
where
    T: FileSized,
{
    entry.file_size()
}

trait FileSized {
    fn file_size(&self) -> Option<u64>;
}

impl FileSized for LocalIndexedEntry {
    fn file_size(&self) -> Option<u64> {
        (self.kind == "file").then_some(self.size)
    }
}

impl FileSized for RemoteIndexedEntry {
    fn file_size(&self) -> Option<u64> {
        (self.kind == "file").then_some(self.size)
    }
}

fn local_entry_map(snapshot: &LocalIndexSnapshot) -> BTreeMap<String, LocalIndexedEntry> {
    snapshot
        .entries
        .iter()
        .map(|entry| {
            (
                entry.relative_path.clone(),
                LocalIndexedEntry {
                    kind: entry.kind.clone(),
                    size: entry.size,
                    fingerprint: entry.fingerprint.clone(),
                },
            )
        })
        .collect()
}

fn remote_entry_map(snapshot: &RemoteIndexSnapshot) -> BTreeMap<String, RemoteIndexedEntry> {
    snapshot
        .entries
        .iter()
        .map(|entry| {
            (
                entry.relative_path.clone(),
                RemoteIndexedEntry {
                    kind: entry.kind.clone(),
                    size: entry.size,
                    etag: entry.etag.clone(),
                    storage_class: entry.storage_class.clone(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::build_sync_plan;
    use crate::storage::{
        local_index::{bytes_fingerprint, LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary},
        remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry},
        sync_db::SyncAnchor,
    };

    fn local_file(path: &str, size: u64, content: &str) -> LocalIndexEntry {
        LocalIndexEntry {
            relative_path: path.into(),
            kind: "file".into(),
            size,
            modified_at: None,
            fingerprint: Some(bytes_fingerprint(content.as_bytes())),
        }
    }

    fn local_dir(path: &str) -> LocalIndexEntry {
        LocalIndexEntry {
            relative_path: path.into(),
            kind: "directory".into(),
            size: 0,
            modified_at: None,
            fingerprint: None,
        }
    }

    fn remote_file(path: &str, size: u64, etag: &str) -> RemoteObjectEntry {
        RemoteObjectEntry {
            key: path.into(),
            relative_path: path.into(),
            kind: "file".into(),
            size,
            last_modified_at: None,
            etag: Some(etag.into()),
            storage_class: None,
        }
    }

    fn remote_dir(path: &str) -> RemoteObjectEntry {
        RemoteObjectEntry {
            key: format!("{path}/"),
            relative_path: path.into(),
            kind: "directory".into(),
            size: 0,
            last_modified_at: None,
            etag: None,
            storage_class: None,
        }
    }

    fn local_snapshot(entries: Vec<LocalIndexEntry>) -> LocalIndexSnapshot {
        LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries,
        }
    }

    fn remote_snapshot(entries: Vec<RemoteObjectEntry>) -> RemoteIndexSnapshot {
        RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary::default(),
            entries,
        }
    }

    fn file_anchor(path: &str, local_content: &str, remote_etag: Option<&str>) -> SyncAnchor {
        SyncAnchor {
            path: path.into(),
            kind: "file".into(),
            local_fingerprint: Some(bytes_fingerprint(local_content.as_bytes())),
            remote_etag: remote_etag.map(str::to_string),
            synced_at: "2026-04-12T00:00:00Z".into(),
        }
    }

    #[test]
    fn anchored_local_only_edit_plans_upload() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "bravo")]);
        let remote = remote_snapshot(vec![remote_file("note.txt", 5, "etag-base")]);
        let mut anchors = BTreeMap::new();
        anchors.insert(
            "note.txt".into(),
            file_anchor("note.txt", "alpha", Some("etag-base")),
        );

        let plan = build_sync_plan(&local, &remote, &anchors, "preserve-both", true);

        assert_eq!(plan.summary.upload_count, 1);
        assert_eq!(plan.summary.conflict_count, 0);
        assert_eq!(plan.queue_items[0].operation, "upload");
    }

    #[test]
    fn anchored_remote_only_edit_plans_download() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "alpha")]);
        let remote = remote_snapshot(vec![remote_file("note.txt", 5, "etag-new")]);
        let mut anchors = BTreeMap::new();
        anchors.insert(
            "note.txt".into(),
            file_anchor("note.txt", "alpha", Some("etag-base")),
        );

        let plan = build_sync_plan(&local, &remote, &anchors, "preserve-both", true);

        assert_eq!(plan.summary.download_count, 1);
        assert_eq!(plan.summary.conflict_count, 0);
        assert_eq!(plan.queue_items[0].operation, "download");
    }

    #[test]
    fn anchored_dual_drift_plans_conflict() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "bravo")]);
        let remote = remote_snapshot(vec![remote_file("note.txt", 5, "etag-new")]);
        let mut anchors = BTreeMap::new();
        anchors.insert(
            "note.txt".into(),
            file_anchor("note.txt", "alpha", Some("etag-base")),
        );

        let plan = build_sync_plan(&local, &remote, &anchors, "preserve-both", true);

        assert_eq!(plan.summary.conflict_count, 1);
        assert_eq!(plan.queue_items[0].operation, "conflict_review");
    }

    #[test]
    fn anchored_same_size_changed_content_is_not_noop_when_local_changed() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "bravo")]);
        let remote = remote_snapshot(vec![remote_file("note.txt", 5, "etag-base")]);
        let mut anchors = BTreeMap::new();
        anchors.insert(
            "note.txt".into(),
            file_anchor("note.txt", "alpha", Some("etag-base")),
        );

        let plan = build_sync_plan(&local, &remote, &anchors, "preserve-both", true);

        assert_eq!(plan.queue_items[0].operation, "upload");
    }

    #[test]
    fn unanchored_same_path_file_file_is_review_required() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "alpha")]);
        let remote = remote_snapshot(vec![remote_file("note.txt", 5, "etag-base")]);

        let plan = build_sync_plan(&local, &remote, &BTreeMap::new(), "preserve-both", true);

        assert_eq!(plan.summary.conflict_count, 1);
        assert_eq!(plan.queue_items[0].operation, "review_required");
    }

    #[test]
    fn unanchored_local_only_file_uploads() {
        let local = local_snapshot(vec![local_file("alpha.txt", 5, "alpha")]);
        let remote = remote_snapshot(vec![]);

        let plan = build_sync_plan(&local, &remote, &BTreeMap::new(), "preserve-both", true);

        assert_eq!(plan.summary.upload_count, 1);
        assert_eq!(plan.queue_items[0].operation, "upload");
    }

    #[test]
    fn unanchored_remote_only_file_downloads() {
        let local = local_snapshot(vec![]);
        let remote = remote_snapshot(vec![remote_file("beta.txt", 7, "etag-1")]);

        let plan = build_sync_plan(&local, &remote, &BTreeMap::new(), "preserve-both", true);

        assert_eq!(plan.summary.download_count, 1);
        assert_eq!(plan.queue_items[0].operation, "download");
    }

    #[test]
    fn anchored_missing_remote_with_remote_base_is_review_required() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "alpha")]);
        let remote = remote_snapshot(vec![]);
        let mut anchors = BTreeMap::new();
        anchors.insert(
            "note.txt".into(),
            file_anchor("note.txt", "alpha", Some("etag-base")),
        );

        let plan = build_sync_plan(&local, &remote, &anchors, "preserve-both", true);

        assert_eq!(plan.summary.conflict_count, 1);
        assert_eq!(plan.queue_items[0].operation, "review_required");
    }

    #[test]
    fn directory_rules_remain_stable() {
        let local = local_snapshot(vec![local_dir("docs")]);
        let remote = remote_snapshot(vec![]);

        let plan = build_sync_plan(&local, &remote, &BTreeMap::new(), "preserve-both", true);

        assert_eq!(plan.summary.create_directory_count, 1);
        assert_eq!(plan.queue_items[0].operation, "create_directory");
    }

    #[test]
    fn file_directory_mismatch_is_conflict_review() {
        let local = local_snapshot(vec![local_file("mixed", 4, "test")]);
        let remote = remote_snapshot(vec![remote_dir("mixed")]);

        let plan = build_sync_plan(&local, &remote, &BTreeMap::new(), "preserve-both", true);

        assert_eq!(plan.summary.conflict_count, 1);
        assert_eq!(plan.queue_items[0].operation, "conflict_review");
    }

    #[test]
    fn conflict_strategy_applies_only_to_anchored_dual_drift() {
        let local = local_snapshot(vec![local_file("note.txt", 5, "bravo")]);
        let remote = remote_snapshot(vec![remote_file("note.txt", 5, "etag-new")]);
        let mut anchors = BTreeMap::new();
        anchors.insert(
            "note.txt".into(),
            file_anchor("note.txt", "alpha", Some("etag-base")),
        );

        let prefer_local = build_sync_plan(&local, &remote, &anchors, "prefer-local", true);
        let prefer_remote = build_sync_plan(&local, &remote, &anchors, "prefer-remote", true);

        assert_eq!(prefer_local.queue_items[0].operation, "upload");
        assert_eq!(prefer_remote.queue_items[0].operation, "download");
    }
}
