use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::{
    local_index::LocalIndexSnapshot,
    now_iso,
    remote_index::{is_glacier_storage_class, RemoteIndexSnapshot},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IndexedEntryKind {
    File(u64),
    Directory,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct InventoryComparisonSummary {
    pub compared_at: String,
    pub local_file_count: u64,
    pub remote_object_count: u64,
    pub exact_match_count: u64,
    pub local_only_count: u64,
    pub remote_only_count: u64,
    pub size_mismatch_count: u64,
}

pub fn compare_snapshots(
    local_snapshot: Option<&LocalIndexSnapshot>,
    remote_snapshot: Option<&RemoteIndexSnapshot>,
) -> InventoryComparisonSummary {
    let local_entries = local_snapshot.map(local_entry_map).unwrap_or_default();
    let remote_entries = remote_snapshot.map(remote_entry_map).unwrap_or_default();
    let local_file_count = local_entries
        .values()
        .filter(|entry| matches!(entry, IndexedEntryKind::File(_)))
        .count() as u64;
    let remote_object_count = remote_entries
        .values()
        .filter(|entry| matches!(entry, IndexedEntryKind::File(_)))
        .count() as u64;

    let mut paths = BTreeSet::new();
    paths.extend(local_entries.keys().cloned());
    paths.extend(remote_entries.keys().cloned());

    let mut summary = InventoryComparisonSummary {
        compared_at: now_iso(),
        local_file_count,
        remote_object_count,
        exact_match_count: 0,
        local_only_count: 0,
        remote_only_count: 0,
        size_mismatch_count: 0,
    };

    for path in paths {
        match (local_entries.get(&path), remote_entries.get(&path)) {
            (Some(local_kind), Some(remote_kind)) => {
                if local_kind == remote_kind {
                    summary.exact_match_count += 1;
                } else {
                    summary.size_mismatch_count += 1;
                }
            }
            (Some(_), None) => summary.local_only_count += 1,
            (None, Some(_)) => summary.remote_only_count += 1,
            (None, None) => {}
        }
    }

    summary
}

fn local_entry_map(snapshot: &LocalIndexSnapshot) -> BTreeMap<String, IndexedEntryKind> {
    snapshot
        .entries
        .iter()
        .filter_map(|entry| match entry.kind.as_str() {
            "file" => Some((
                entry.relative_path.clone(),
                IndexedEntryKind::File(entry.size),
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
        .filter(|entry| !is_glacier_storage_class(entry.storage_class.as_deref()))
        .filter_map(|entry| match entry.kind.as_str() {
            "file" => Some((
                entry.relative_path.clone(),
                IndexedEntryKind::File(entry.size),
            )),
            "directory" => Some((entry.relative_path.clone(), IndexedEntryKind::Directory)),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::compare_snapshots;
    use crate::storage::{
        local_index::{LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary},
        remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry},
    };

    #[test]
    fn compares_local_files_and_remote_objects_conservatively() {
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary {
                indexed_at: "2026-01-01T00:00:00Z".into(),
                file_count: 2,
                directory_count: 0,
                total_bytes: 15,
            },
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
                    size: 10,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"beta")),
                },
            ],
        };

        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary {
                indexed_at: "2026-01-01T00:00:00Z".into(),
                object_count: 2,
                total_bytes: 16,
            },
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

        let summary = compare_snapshots(Some(&local), Some(&remote));

        assert_eq!(summary.local_file_count, 2);
        assert_eq!(summary.remote_object_count, 2);
        assert_eq!(summary.exact_match_count, 1);
        assert_eq!(summary.local_only_count, 1);
        assert_eq!(summary.remote_only_count, 1);
        assert_eq!(summary.size_mismatch_count, 0);
    }

    #[test]
    fn reports_size_mismatches_for_same_path() {
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![LocalIndexEntry {
                relative_path: "alpha.txt".into(),
                kind: "file".into(),
                size: 5,
                modified_at: None,
                fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"alpha")),
            }],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary::default(),
            entries: vec![RemoteObjectEntry {
                key: "alpha.txt".into(),
                relative_path: "alpha.txt".into(),
                kind: "file".into(),
                size: 9,
                last_modified_at: None,
                etag: None,
                storage_class: None,
            }],
        };

        let summary = compare_snapshots(Some(&local), Some(&remote));
        assert_eq!(summary.size_mismatch_count, 1);
        assert_eq!(summary.exact_match_count, 0);
    }

    #[test]
    fn compares_directories_without_treating_them_as_downloads() {
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![LocalIndexEntry {
                relative_path: "nested".into(),
                kind: "directory".into(),
                size: 0,
                modified_at: None,
                fingerprint: None,
            }],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
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

        let summary = compare_snapshots(Some(&local), Some(&remote));
        assert_eq!(summary.exact_match_count, 1);
        assert_eq!(summary.local_file_count, 0);
        assert_eq!(summary.remote_object_count, 0);
        assert_eq!(summary.local_only_count, 0);
        assert_eq!(summary.remote_only_count, 0);
        assert_eq!(summary.size_mismatch_count, 0);
    }

    #[test]
    fn keeps_legacy_counters_file_only_when_directories_are_present() {
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![
                LocalIndexEntry {
                    relative_path: "nested".into(),
                    kind: "directory".into(),
                    size: 0,
                    modified_at: None,
                    fingerprint: None,
                },
                LocalIndexEntry {
                    relative_path: "nested/alpha.txt".into(),
                    kind: "file".into(),
                    size: 5,
                    modified_at: None,
                    fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"alpha")),
                },
            ],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary::default(),
            entries: vec![
                RemoteObjectEntry {
                    key: "nested/".into(),
                    relative_path: "nested".into(),
                    kind: "directory".into(),
                    size: 0,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
                RemoteObjectEntry {
                    key: "nested/alpha.txt".into(),
                    relative_path: "nested/alpha.txt".into(),
                    kind: "file".into(),
                    size: 5,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
            ],
        };

        let summary = compare_snapshots(Some(&local), Some(&remote));

        assert_eq!(summary.local_file_count, 1);
        assert_eq!(summary.remote_object_count, 1);
        assert_eq!(summary.exact_match_count, 2);
    }

    #[test]
    fn glacier_files_excluded_from_comparison() {
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary::default(),
            entries: vec![
                RemoteObjectEntry {
                    key: "normal.txt".into(),
                    relative_path: "normal.txt".into(),
                    kind: "file".into(),
                    size: 42,
                    last_modified_at: None,
                    etag: None,
                    storage_class: None,
                },
                RemoteObjectEntry {
                    key: "frozen.txt".into(),
                    relative_path: "frozen.txt".into(),
                    kind: "file".into(),
                    size: 99,
                    last_modified_at: None,
                    etag: None,
                    storage_class: Some("GLACIER_IR".into()),
                },
            ],
        };

        let summary = compare_snapshots(Some(&local), Some(&remote));

        assert_eq!(summary.remote_object_count, 1);
        assert_eq!(summary.remote_only_count, 1);
        assert_eq!(summary.exact_match_count, 0);
        assert_eq!(summary.local_only_count, 0);
        assert_eq!(summary.size_mismatch_count, 0);
    }

    #[test]
    fn excluded_remote_bin_entries_do_not_affect_comparison_when_snapshot_is_filtered() {
        let local = LocalIndexSnapshot {
            version: 2,
            root_folder: "C:/sync".into(),
            summary: LocalIndexSummary::default(),
            entries: vec![LocalIndexEntry {
                relative_path: "docs/note.txt".into(),
                kind: "file".into(),
                size: 5,
                modified_at: None,
                fingerprint: Some(crate::storage::local_index::bytes_fingerprint(b"note")),
            }],
        };
        let remote = RemoteIndexSnapshot {
            version: 1,
            bucket: "demo".into(),
            excluded_prefixes: vec![".storage-goblin-bin/pair-1/".into()],
            summary: RemoteIndexSummary::default(),
            entries: vec![RemoteObjectEntry {
                key: "docs/note.txt".into(),
                relative_path: "docs/note.txt".into(),
                kind: "file".into(),
                size: 5,
                last_modified_at: None,
                etag: None,
                storage_class: None,
            }],
        };

        let summary = compare_snapshots(Some(&local), Some(&remote));
        assert_eq!(summary.exact_match_count, 1);
        assert_eq!(summary.remote_object_count, 1);
    }
}
