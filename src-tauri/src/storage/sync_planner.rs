use std::collections::{BTreeMap, BTreeSet};

use super::{
    local_index::LocalIndexSnapshot,
    now_iso,
    remote_index::{is_glacier_storage_class, RemoteIndexSnapshot},
};

#[derive(Debug, Clone, PartialEq, Eq)]
enum IndexedEntryKind {
    File { size: u64 },
    Directory,
    Glacier { size: u64 },
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

pub fn build_sync_plan(
    local_snapshot: &LocalIndexSnapshot,
    remote_snapshot: &RemoteIndexSnapshot,
    credentials_available: bool,
) -> SyncPlan {
    let local_entries = local_entry_map(local_snapshot);
    let remote_entries = remote_entry_map(remote_snapshot);
    let local_file_count = local_entries
        .values()
        .filter(|entry| matches!(entry, IndexedEntryKind::File { .. }))
        .count() as u64;
    let remote_object_count = remote_entries
        .values()
        .filter(|entry| matches!(entry, IndexedEntryKind::File { .. }))
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
        match (local_entries.get(&path), remote_entries.get(&path)) {
            (Some(IndexedEntryKind::File { size: local_size }), None) => {
                upload_count += 1;
                observed_entries.push(ObservedEntry {
                    path: path.clone(),
                    local_size: Some(*local_size),
                    remote_size: None,
                    resolution: "upload".into(),
                });
                queue_items.push(PlannedQueueItem {
                    path,
                    operation: "upload".into(),
                    local_size: Some(*local_size),
                    remote_size: None,
                });
            }
            (Some(IndexedEntryKind::Directory), None) => {
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
                });
            }
            (None, Some(IndexedEntryKind::File { size: remote_size })) => {
                download_count += 1;
                observed_entries.push(ObservedEntry {
                    path: path.clone(),
                    local_size: None,
                    remote_size: Some(*remote_size),
                    resolution: "download".into(),
                });
                queue_items.push(PlannedQueueItem {
                    path,
                    operation: "download".into(),
                    local_size: None,
                    remote_size: Some(*remote_size),
                });
            }
            (_, Some(IndexedEntryKind::Glacier { size: remote_size })) => {
                noop_count += 1;
                let local_size = match local_entries.get(&path) {
                    Some(IndexedEntryKind::File { size }) => Some(*size),
                    _ => None,
                };
                observed_entries.push(ObservedEntry {
                    path,
                    local_size,
                    remote_size: Some(*remote_size),
                    resolution: "noop".into(),
                });
            }
            (Some(IndexedEntryKind::Directory), Some(IndexedEntryKind::Directory)) => {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: None,
                    remote_size: None,
                    resolution: "noop".into(),
                });
            }
            (None, Some(IndexedEntryKind::Directory)) => {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: None,
                    remote_size: None,
                    resolution: "noop".into(),
                });
            }
            (
                Some(IndexedEntryKind::File { size: local_size }),
                Some(IndexedEntryKind::File { size: remote_size }),
            ) if local_size == remote_size => {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: Some(*local_size),
                    remote_size: Some(*remote_size),
                    resolution: "noop".into(),
                });
            }
            (Some(local_kind), Some(remote_kind)) => {
                conflict_count += 1;
                let (local_size, remote_size) = match (local_kind, remote_kind) {
                    (
                        IndexedEntryKind::File { size: local_size },
                        IndexedEntryKind::File { size: remote_size },
                    ) => (Some(*local_size), Some(*remote_size)),
                    (IndexedEntryKind::File { size: local_size }, IndexedEntryKind::Directory) => {
                        (Some(*local_size), None)
                    }
                    (IndexedEntryKind::Directory, IndexedEntryKind::File { size: remote_size }) => {
                        (None, Some(*remote_size))
                    }
                    (IndexedEntryKind::Directory, IndexedEntryKind::Directory) => (None, None),
                    // Glacier on remote side is handled by an earlier match arm;
                    // Glacier on local side cannot occur. Satisfy exhaustiveness.
                    _ => (None, None),
                };
                observed_entries.push(ObservedEntry {
                    path: path.clone(),
                    local_size,
                    remote_size,
                    resolution: "conflict_review".into(),
                });
                queue_items.push(PlannedQueueItem {
                    path,
                    operation: "conflict_review".into(),
                    local_size,
                    remote_size,
                });
            }
            (None, None) => {}
            // Glacier on the local side cannot occur — only remote entries produce this variant.
            (Some(IndexedEntryKind::Glacier { .. }), _) => {}
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

fn local_entry_map(snapshot: &LocalIndexSnapshot) -> BTreeMap<String, IndexedEntryKind> {
    snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry.kind.as_str() {
            "file" => Some((
                entry.relative_path.clone(),
                IndexedEntryKind::File { size: entry.size },
            )),
            "directory" => Some((entry.relative_path.clone(), IndexedEntryKind::Directory)),
            _ => None,
        })
        .collect()
}

fn remote_entry_map(snapshot: &RemoteIndexSnapshot) -> BTreeMap<String, IndexedEntryKind> {
    snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry.kind.as_str() {
            "file" => {
                let kind = if is_glacier_storage_class(entry.storage_class.as_deref()) {
                    IndexedEntryKind::Glacier { size: entry.size }
                } else {
                    IndexedEntryKind::File { size: entry.size }
                };
                Some((entry.relative_path.clone(), kind))
            }
            "directory" => Some((entry.relative_path.clone(), IndexedEntryKind::Directory)),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::build_sync_plan;
    use crate::storage::{
        local_index::{LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary},
        remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry},
    };

    #[test]
    fn builds_upload_download_conflict_and_noop_actions() {
        let local = LocalIndexSnapshot {
            version: 1,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![
                LocalIndexEntry {
                    relative_path: "nested".into(),
                    kind: "directory".into(),
                    size: 0,
                    modified_at: None,
                },
                LocalIndexEntry {
                    relative_path: "alpha.txt".into(),
                    kind: "file".into(),
                    size: 5,
                    modified_at: None,
                },
                LocalIndexEntry {
                    relative_path: "beta.txt".into(),
                    kind: "file".into(),
                    size: 10,
                    modified_at: None,
                },
                LocalIndexEntry {
                    relative_path: "same.txt".into(),
                    kind: "file".into(),
                    size: 12,
                    modified_at: None,
                },
            ],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            summary: RemoteIndexSummary::default(),
            entries: vec![
                RemoteObjectEntry {
                    key: "beta.txt".into(),
                    relative_path: "beta.txt".into(),
                    kind: "file".into(),
                    size: 11,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
                RemoteObjectEntry {
                    key: "gamma.txt".into(),
                    relative_path: "gamma.txt".into(),
                    kind: "file".into(),
                    size: 7,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
                RemoteObjectEntry {
                    key: "same.txt".into(),
                    relative_path: "same.txt".into(),
                    kind: "file".into(),
                    size: 12,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
            ],
        };

        let plan = build_sync_plan(&local, &remote, true);

        assert_eq!(plan.summary.local_file_count, 3);
        assert_eq!(plan.summary.remote_object_count, 3);
        assert_eq!(plan.summary.upload_count, 1);
        assert_eq!(plan.summary.create_directory_count, 1);
        assert_eq!(plan.summary.download_count, 1);
        assert_eq!(plan.summary.conflict_count, 1);
        assert_eq!(plan.summary.noop_count, 1);
        assert_eq!(plan.summary.pending_operation_count, 4);
        assert_eq!(
            plan.queue_items
                .iter()
                .map(|item| (item.path.as_str(), item.operation.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("alpha.txt", "upload"),
                ("beta.txt", "conflict_review"),
                ("gamma.txt", "download"),
                ("nested", "create_directory"),
            ]
        );
    }

    #[test]
    fn does_not_download_remote_only_directories() {
        let local = LocalIndexSnapshot {
            version: 1,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            summary: RemoteIndexSummary::default(),
            entries: vec![RemoteObjectEntry {
                key: "nested/".into(),
                relative_path: "nested".into(),
                kind: "directory".into(),
                size: 0,
                last_modified_at: None,
                etag: None,
                storage_class: None,
            }],
        };

        let plan = build_sync_plan(&local, &remote, true);

        assert_eq!(plan.summary.local_file_count, 0);
        assert_eq!(plan.summary.remote_object_count, 0);
        assert_eq!(plan.summary.download_count, 0);
        assert_eq!(plan.summary.pending_operation_count, 0);
        assert_eq!(plan.observed_entries.len(), 1);
        assert_eq!(plan.observed_entries[0].resolution, "noop");
    }

    #[test]
    fn glacier_remote_only_file_is_noop() {
        let local = LocalIndexSnapshot {
            version: 1,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            summary: RemoteIndexSummary::default(),
            entries: vec![RemoteObjectEntry {
                key: "frozen.txt".into(),
                relative_path: "frozen.txt".into(),
                kind: "file".into(),
                size: 50,
                last_modified_at: None,
                etag: None,
                storage_class: Some("GLACIER_IR".into()),
            }],
        };

        let plan = build_sync_plan(&local, &remote, true);

        assert_eq!(plan.summary.download_count, 0);
        assert_eq!(plan.summary.upload_count, 0);
        assert_eq!(plan.summary.noop_count, 1);
        assert_eq!(plan.summary.remote_object_count, 0);
        assert!(plan.queue_items.is_empty());
        assert_eq!(plan.observed_entries.len(), 1);
        assert_eq!(plan.observed_entries[0].resolution, "noop");
    }

    #[test]
    fn glacier_file_with_local_copy_is_noop() {
        let local = LocalIndexSnapshot {
            version: 1,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![LocalIndexEntry {
                relative_path: "archive.dat".into(),
                kind: "file".into(),
                size: 100,
                modified_at: None,
            }],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            summary: RemoteIndexSummary::default(),
            entries: vec![RemoteObjectEntry {
                key: "archive.dat".into(),
                relative_path: "archive.dat".into(),
                kind: "file".into(),
                size: 100,
                last_modified_at: None,
                etag: None,
                storage_class: Some("GLACIER".into()),
            }],
        };

        let plan = build_sync_plan(&local, &remote, true);

        assert_eq!(plan.summary.upload_count, 0);
        assert_eq!(plan.summary.download_count, 0);
        assert_eq!(plan.summary.noop_count, 1);
        assert!(plan.queue_items.is_empty());
        assert_eq!(plan.observed_entries[0].resolution, "noop");
        assert_eq!(plan.observed_entries[0].local_size, Some(100));
        assert_eq!(plan.observed_entries[0].remote_size, Some(100));
    }

    #[test]
    fn deep_archive_file_is_also_noop() {
        let local = LocalIndexSnapshot {
            version: 1,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            summary: RemoteIndexSummary::default(),
            entries: vec![RemoteObjectEntry {
                key: "deep.bin".into(),
                relative_path: "deep.bin".into(),
                kind: "file".into(),
                size: 200,
                last_modified_at: None,
                etag: None,
                storage_class: Some("DEEP_ARCHIVE".into()),
            }],
        };

        let plan = build_sync_plan(&local, &remote, true);

        assert_eq!(plan.summary.download_count, 0);
        assert_eq!(plan.summary.noop_count, 1);
    }
}
