use std::collections::{BTreeMap, BTreeSet};

use super::{
    local_index::LocalIndexSnapshot,
    model::{ConflictStrategy, EntryKind, FileEntryStatus},
    now_iso,
    remote_index::{is_cold_storage_class, RemoteIndexSnapshot},
    sync_db::SyncAnchor,
};

/// Every operation the planner can emit. Stored in the durable queue as the
/// `as_str` form; parse back with [`Operation::parse`].
///
/// (Full typed plumbing through `sync_db`/`commands` lands with phase 3's
/// `engine::model`; until then the string boundary is these two functions.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Upload,
    Download,
    CreateDirectory,
    /// Local file was deleted while the remote copy is unchanged: delete the
    /// remote object (through remote-bin/versioning protection where enabled).
    DeleteRemote,
    /// Remote object was deleted while the local copy is unchanged: delete the
    /// local file (to the OS trash).
    DeleteLocal,
    /// Local rename detected: server-side copy `path` → `target_path`, then
    /// delete `path` remotely.
    MoveRemote,
    /// Remote rename detected: locally rename `path` → `target_path`.
    MoveLocal,
    /// preserve-both dual edit: rename the local file to `target_path` and
    /// upload it; a paired `Download` restores the remote version at `path`.
    DuplicateConflict,
    /// Identical content on both sides with no anchor: record the anchor,
    /// transfer nothing.
    AnchorOnly,
    /// Anchor exists but the file is gone on both sides: drop the anchor.
    ForgetAnchor,
    ConflictReview,
    ReviewRequired,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::Upload => "upload",
            Operation::Download => "download",
            Operation::CreateDirectory => "create_directory",
            Operation::DeleteRemote => "delete_remote",
            Operation::DeleteLocal => "delete_local",
            Operation::MoveRemote => "move_remote",
            Operation::MoveLocal => "move_local",
            Operation::DuplicateConflict => "duplicate_conflict",
            Operation::AnchorOnly => "anchor_only",
            Operation::ForgetAnchor => "forget_anchor",
            Operation::ConflictReview => "conflict_review",
            Operation::ReviewRequired => "review_required",
        }
    }

    pub fn parse(value: &str) -> Option<Operation> {
        Some(match value {
            "upload" => Operation::Upload,
            "download" => Operation::Download,
            "create_directory" => Operation::CreateDirectory,
            "delete_remote" => Operation::DeleteRemote,
            "delete_local" => Operation::DeleteLocal,
            "move_remote" => Operation::MoveRemote,
            "move_local" => Operation::MoveLocal,
            "duplicate_conflict" => Operation::DuplicateConflict,
            "anchor_only" => Operation::AnchorOnly,
            "forget_anchor" => Operation::ForgetAnchor,
            "conflict_review" => Operation::ConflictReview,
            "review_required" => Operation::ReviewRequired,
            _ => return None,
        })
    }
}

/// Automatic deletes above this count are suppressed into review items until
/// a user confirms ("fail-safe — never delete without certainty"). The UI ack
/// flow arrives in phase 5; until then the guard errs on the safe side.
pub const MAX_AUTO_DELETE_COUNT: u64 = 25;
/// Additionally suppress when deletes would touch more than this share of the
/// anchored tree (only applied once the tree is non-trivial).
pub const MAX_AUTO_DELETE_RATIO: f64 = 0.5;
const AUTO_DELETE_RATIO_MIN_ANCHORS: u64 = 10;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LocalIndexedEntry {
    kind: EntryKind,
    size: u64,
    fingerprint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteIndexedEntry {
    kind: EntryKind,
    size: u64,
    etag: Option<String>,
    storage_class: Option<String>,
    fingerprint: Option<String>,
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
    /// Destination path for `move_remote` / `move_local` / `duplicate_conflict`.
    pub target_path: Option<String>,
    pub local_size: Option<u64>,
    pub remote_size: Option<u64>,
    pub expected_local_fingerprint: Option<String>,
    pub expected_remote_etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
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
    pub delete_count: u64,
    pub move_count: u64,
    pub anchor_count: u64,
    /// Deletes converted to review items by the mass-delete circuit breaker.
    pub suppressed_delete_count: u64,
    pub pending_operation_count: u64,
    pub credentials_available: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SyncPlan {
    pub summary: SyncPlanSummary,
    pub observed_entries: Vec<ObservedEntry>,
    pub queue_items: Vec<PlannedQueueItem>,
}

/// Outcomes of the file decision table.
///
/// Note there is no `ConflictReview` variant: file-vs-directory kind
/// mismatches are structural and emit their queue item before the table runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileSyncDecision {
    Noop,
    Upload,
    Download,
    DeleteRemote,
    DeleteLocal,
    DuplicateConflict,
    AnchorOnly,
    ForgetAnchor,
    ReviewRequired,
}

/// One classified path, before post-passes (rename pairing, breaker).
#[derive(Debug, Clone)]
struct PlannedAction {
    path: String,
    decision: FileSyncDecision,
    local_size: Option<u64>,
    remote_size: Option<u64>,
    local_fingerprint: Option<String>,
    remote_etag: Option<String>,
    /// The anchor's identity for delete decisions (what the deleted side
    /// last looked like) — used for rename pairing.
    anchor_local_fingerprint: Option<String>,
    anchor_remote_etag: Option<String>,
    /// Set by rename pairing: this Delete* action is actually a move to the
    /// given destination path.
    move_target: Option<String>,
}

pub(crate) fn file_entry_status(
    anchor: Option<&SyncAnchor>,
    current_local_fingerprint: Option<&str>,
    current_remote_etag: Option<&str>,
) -> FileEntryStatus {
    match decide_file_sync(
        anchor,
        current_local_fingerprint,
        current_remote_etag,
        None,
        ConflictStrategy::PreserveBoth,
    ) {
        FileSyncDecision::Noop | FileSyncDecision::AnchorOnly | FileSyncDecision::ForgetAnchor => {
            FileEntryStatus::Synced
        }
        // A pending delete still shows as present on the surviving side.
        FileSyncDecision::Upload | FileSyncDecision::DeleteLocal => FileEntryStatus::LocalOnly,
        FileSyncDecision::Download | FileSyncDecision::DeleteRemote => FileEntryStatus::RemoteOnly,
        FileSyncDecision::DuplicateConflict => FileEntryStatus::Conflict,
        FileSyncDecision::ReviewRequired => FileEntryStatus::ReviewRequired,
    }
}

pub fn build_sync_plan(
    local_snapshot: &LocalIndexSnapshot,
    remote_snapshot: &RemoteIndexSnapshot,
    anchors: &BTreeMap<String, SyncAnchor>,
    conflict_strategy: &str,
    credentials_available: bool,
) -> SyncPlan {
    let strategy = ConflictStrategy::parse_or_default(conflict_strategy);
    let local_entries = local_entry_map(local_snapshot);
    let remote_entries = remote_entry_map(remote_snapshot);
    let local_file_count = local_entries
        .values()
        .filter(|entry| entry.kind == EntryKind::File)
        .count() as u64;
    let remote_object_count = remote_entries
        .values()
        .filter(|entry| {
            entry.kind == EntryKind::File && !is_cold_storage_class(entry.storage_class.as_deref())
        })
        .count() as u64;
    let anchored_file_count = anchors
        .values()
        .filter(|anchor| anchor.kind == "file")
        .count() as u64;

    let mut paths = BTreeSet::new();
    paths.extend(local_entries.keys().cloned());
    paths.extend(remote_entries.keys().cloned());
    paths.extend(anchors.keys().cloned());

    let planned_at = now_iso();
    let mut observed_entries = Vec::with_capacity(paths.len());
    let mut actions: Vec<PlannedAction> = Vec::new();
    let mut create_directory_count = 0_u64;
    let mut conflict_count = 0_u64;
    let mut noop_count = 0_u64;
    let mut queue_items: Vec<PlannedQueueItem> = Vec::new();

    for path in paths {
        let local = local_entries.get(&path);
        let remote = remote_entries.get(&path);
        let anchor = anchors.get(&path);

        if remote.is_some_and(|entry| is_cold_storage_class(entry.storage_class.as_deref())) {
            noop_count += 1;
            observed_entries.push(ObservedEntry {
                path,
                local_size: local.and_then(file_size),
                remote_size: remote.and_then(file_size),
                resolution: "noop".into(),
            });
            continue;
        }

        match (local, remote) {
            (Some(local), None) if local.kind.is_directory() => {
                create_directory_count += 1;
                observed_entries.push(ObservedEntry {
                    path: path.clone(),
                    local_size: None,
                    remote_size: None,
                    resolution: "create_directory".into(),
                });
                queue_items.push(PlannedQueueItem {
                    path,
                    operation: Operation::CreateDirectory.as_str().into(),
                    target_path: None,
                    local_size: None,
                    remote_size: None,
                    expected_local_fingerprint: None,
                    expected_remote_etag: None,
                });
            }
            (Some(local), Some(remote))
                if local.kind.is_directory() && remote.kind.is_directory() =>
            {
                noop_count += 1;
                observed_entries.push(ObservedEntry {
                    path,
                    local_size: None,
                    remote_size: None,
                    resolution: "noop".into(),
                });
            }
            (None, Some(remote)) if remote.kind.is_directory() => {
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
                    operation: Operation::ConflictReview.as_str().into(),
                    target_path: None,
                    local_size: file_size(local),
                    remote_size: file_size(remote),
                    expected_local_fingerprint: None,
                    expected_remote_etag: None,
                });
            }
            (local, remote)
                if local.is_none_or(|entry| entry.kind == EntryKind::File)
                    && remote.is_none_or(|entry| entry.kind == EntryKind::File) =>
            {
                if local.is_none() && remote.is_none() && anchor.is_none() {
                    continue;
                }
                let decision = decide_file_sync(
                    anchor,
                    local.and_then(|entry| entry.fingerprint.as_deref()),
                    remote.and_then(|entry| entry.etag.as_deref()),
                    remote.and_then(|entry| entry.fingerprint.as_deref()),
                    strategy,
                );
                actions.push(PlannedAction {
                    path,
                    decision,
                    local_size: local.and_then(file_size),
                    remote_size: remote.and_then(file_size),
                    local_fingerprint: local.and_then(|entry| entry.fingerprint.clone()),
                    remote_etag: remote.and_then(|entry| entry.etag.clone()),
                    anchor_local_fingerprint: anchor
                        .and_then(|anchor| anchor.local_fingerprint.clone()),
                    anchor_remote_etag: anchor.and_then(|anchor| anchor.remote_etag.clone()),
                    move_target: None,
                });
            }
            _ => {
                // Directory-vs-nothing combinations with a stale anchor, and
                // any remaining directory pairings: nothing to transfer.
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

    // -- post-pass: rename pairing ------------------------------------------
    pair_local_renames(&mut actions);
    pair_remote_renames(&mut actions);

    // -- post-pass: mass-delete circuit breaker -----------------------------
    let planned_delete_count = actions
        .iter()
        .filter(|action| {
            matches!(
                action.decision,
                FileSyncDecision::DeleteRemote | FileSyncDecision::DeleteLocal
            )
        })
        .count() as u64;
    let ratio_tripped = anchored_file_count >= AUTO_DELETE_RATIO_MIN_ANCHORS
        && planned_delete_count as f64 > anchored_file_count as f64 * MAX_AUTO_DELETE_RATIO;
    let suppress_deletes = planned_delete_count > MAX_AUTO_DELETE_COUNT || ratio_tripped;
    let mut suppressed_delete_count = 0_u64;

    // -- materialize --------------------------------------------------------
    let mut upload_count = 0_u64;
    let mut download_count = 0_u64;
    let mut delete_count = 0_u64;
    let mut move_count = 0_u64;
    let mut anchor_count = 0_u64;

    for action in actions {
        match action.decision {
            FileSyncDecision::Noop => {
                noop_count += 1;
                observed_entries.push(observed(&action, "noop"));
            }
            FileSyncDecision::Upload => {
                upload_count += 1;
                observed_entries.push(observed(&action, "upload"));
                queue_items.push(queue_item(&action, Operation::Upload, None));
            }
            FileSyncDecision::Download => {
                download_count += 1;
                observed_entries.push(observed(&action, "download"));
                queue_items.push(queue_item(&action, Operation::Download, None));
            }
            FileSyncDecision::DeleteRemote => {
                if let Some(target) = action.move_target.clone() {
                    observed_entries.push(observed(&action, "move_remote"));
                    queue_items.push(queue_item(&action, Operation::MoveRemote, Some(target)));
                } else if suppress_deletes {
                    suppressed_delete_count += 1;
                    conflict_count += 1;
                    observed_entries.push(observed(&action, "delete_suppressed"));
                    queue_items.push(queue_item(&action, Operation::ReviewRequired, None));
                } else {
                    delete_count += 1;
                    observed_entries.push(observed(&action, "delete_remote"));
                    queue_items.push(queue_item(&action, Operation::DeleteRemote, None));
                }
            }
            FileSyncDecision::DeleteLocal => {
                if let Some(target) = action.move_target.clone() {
                    observed_entries.push(observed(&action, "move_local"));
                    queue_items.push(queue_item(&action, Operation::MoveLocal, Some(target)));
                } else if suppress_deletes {
                    suppressed_delete_count += 1;
                    conflict_count += 1;
                    observed_entries.push(observed(&action, "delete_suppressed"));
                    queue_items.push(queue_item(&action, Operation::ReviewRequired, None));
                } else {
                    delete_count += 1;
                    observed_entries.push(observed(&action, "delete_local"));
                    queue_items.push(queue_item(&action, Operation::DeleteLocal, None));
                }
            }
            FileSyncDecision::DuplicateConflict => {
                // Keep both: the local edit moves to a conflict-suffixed name
                // and uploads; the remote edit is downloaded at the original
                // path. Both sides converge to both files.
                let conflict_path =
                    conflict_copy_path(&action.path, &planned_at, &local_entries, &remote_entries);
                upload_count += 1;
                download_count += 1;
                observed_entries.push(observed(&action, "duplicate_conflict"));
                queue_items.push(queue_item(
                    &action,
                    Operation::DuplicateConflict,
                    Some(conflict_path),
                ));
                queue_items.push(queue_item(&action, Operation::Download, None));
            }
            FileSyncDecision::AnchorOnly => {
                anchor_count += 1;
                observed_entries.push(observed(&action, "anchor"));
                queue_items.push(queue_item(&action, Operation::AnchorOnly, None));
            }
            FileSyncDecision::ForgetAnchor => {
                anchor_count += 1;
                observed_entries.push(observed(&action, "forget_anchor"));
                queue_items.push(queue_item(&action, Operation::ForgetAnchor, None));
            }
            FileSyncDecision::ReviewRequired => {
                conflict_count += 1;
                observed_entries.push(observed(&action, "review_required"));
                queue_items.push(queue_item(&action, Operation::ReviewRequired, None));
            }
        }
    }

    // Moves were materialized during pairing (they replace their two halves).
    for item in &queue_items {
        match Operation::parse(&item.operation) {
            Some(Operation::MoveRemote) | Some(Operation::MoveLocal) => move_count += 1,
            _ => {}
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
            delete_count,
            move_count,
            anchor_count,
            suppressed_delete_count,
            pending_operation_count: queue_items.len() as u64,
            credentials_available,
        },
        observed_entries,
        queue_items,
    }
}

/// Local rename: an unanchored local-only file (Upload) whose fingerprint and
/// size exactly match a pending DeleteRemote's last-synced local identity.
/// The pair becomes MoveRemote { path: old, target: new }. Ambiguous matches
/// (multiple candidates on either side) degrade to the unpaired operations.
fn pair_local_renames(actions: &mut Vec<PlannedAction>) {
    pair_renames(
        actions,
        |action| {
            (action.decision == FileSyncDecision::Upload
                && action.anchor_local_fingerprint.is_none()
                && action.remote_etag.is_none())
            .then(|| {
                (
                    action.local_fingerprint.clone().unwrap_or_default(),
                    action.local_size.unwrap_or_default(),
                )
            })
        },
        |action| {
            (action.decision == FileSyncDecision::DeleteRemote).then(|| {
                (
                    action.anchor_local_fingerprint.clone().unwrap_or_default(),
                    action.remote_size.unwrap_or_default(),
                )
            })
        },
    );
}

/// Remote rename: an unanchored remote-only object (Download) whose etag and
/// size exactly match a pending DeleteLocal's last-synced remote identity.
fn pair_remote_renames(actions: &mut Vec<PlannedAction>) {
    pair_renames(
        actions,
        |action| {
            (action.decision == FileSyncDecision::Download
                && action.anchor_remote_etag.is_none()
                && action.local_fingerprint.is_none())
            .then(|| {
                (
                    action.remote_etag.clone().unwrap_or_default(),
                    action.remote_size.unwrap_or_default(),
                )
            })
        },
        |action| {
            (action.decision == FileSyncDecision::DeleteLocal).then(|| {
                (
                    action.anchor_remote_etag.clone().unwrap_or_default(),
                    action.local_size.unwrap_or_default(),
                )
            })
        },
    );
}

fn pair_renames(
    actions: &mut Vec<PlannedAction>,
    new_side_identity: impl Fn(&PlannedAction) -> Option<(String, u64)>,
    old_side_identity: impl Fn(&PlannedAction) -> Option<(String, u64)>,
) {
    let mut new_by_identity: BTreeMap<(String, u64), Vec<usize>> = BTreeMap::new();
    let mut old_by_identity: BTreeMap<(String, u64), Vec<usize>> = BTreeMap::new();

    for (index, action) in actions.iter().enumerate() {
        if let Some(identity) = new_side_identity(action) {
            if !identity.0.is_empty() {
                new_by_identity.entry(identity).or_default().push(index);
            }
        }
        if let Some(identity) = old_side_identity(action) {
            if !identity.0.is_empty() {
                old_by_identity.entry(identity).or_default().push(index);
            }
        }
    }

    let mut consumed_new: Vec<usize> = Vec::new();
    let mut pairings: Vec<(usize, usize)> = Vec::new();

    for (identity, new_indexes) in &new_by_identity {
        let Some(old_indexes) = old_by_identity.get(identity) else {
            continue;
        };
        // Require a unique pairing on both sides; anything else is ambiguous
        // and degrades safely to upload/download + delete.
        if new_indexes.len() != 1 || old_indexes.len() != 1 {
            continue;
        }
        pairings.push((old_indexes[0], new_indexes[0]));
        consumed_new.push(new_indexes[0]);
    }

    for (old_index, new_index) in &pairings {
        let target = actions[*new_index].path.clone();
        let new_local_fingerprint = actions[*new_index].local_fingerprint.clone();
        let new_remote_etag = actions[*new_index].remote_etag.clone();
        let old_action = &mut actions[*old_index];
        old_action.move_target = Some(target);
        // Carry the surviving content identity so the executor can anchor the
        // destination path after the move.
        if old_action.local_fingerprint.is_none() {
            old_action.local_fingerprint = new_local_fingerprint;
        }
        if old_action.remote_etag.is_none() {
            old_action.remote_etag = new_remote_etag;
        }
    }

    consumed_new.sort_unstable();
    for index in consumed_new.into_iter().rev() {
        actions.remove(index);
    }
}

fn observed(action: &PlannedAction, resolution: &str) -> ObservedEntry {
    ObservedEntry {
        path: action.path.clone(),
        local_size: action.local_size,
        remote_size: action.remote_size,
        resolution: resolution.into(),
    }
}

fn queue_item(
    action: &PlannedAction,
    operation: Operation,
    target_path: Option<String>,
) -> PlannedQueueItem {
    PlannedQueueItem {
        path: action.path.clone(),
        operation: operation.as_str().into(),
        target_path,
        local_size: action.local_size,
        remote_size: action.remote_size,
        expected_local_fingerprint: action
            .local_fingerprint
            .clone()
            .or_else(|| action.anchor_local_fingerprint.clone()),
        expected_remote_etag: action
            .remote_etag
            .clone()
            .or_else(|| action.anchor_remote_etag.clone()),
    }
}

/// `name.ext` → `name (conflict 2026-07-19).ext`, with a numeric suffix on
/// collision. (Device-name suffixes need settings plumbing — phase 1.2 TODO.)
fn conflict_copy_path(
    path: &str,
    planned_at: &str,
    local_entries: &BTreeMap<String, LocalIndexedEntry>,
    remote_entries: &BTreeMap<String, RemoteIndexedEntry>,
) -> String {
    let date = planned_at.get(0..10).unwrap_or("conflict");
    let (stem, extension) = match path.rsplit_once('.') {
        // Treat a dot inside the final path segment as an extension split.
        Some((stem, extension)) if !extension.contains('/') && !stem.ends_with('/') => {
            (stem, Some(extension))
        }
        _ => (path, None),
    };

    for attempt in 0..100_u32 {
        let counter = if attempt == 0 {
            String::new()
        } else {
            format!(" {}", attempt + 1)
        };
        let candidate = match extension {
            Some(extension) => format!("{stem} (conflict {date}{counter}).{extension}"),
            None => format!("{stem} (conflict {date}{counter})"),
        };
        if !local_entries.contains_key(&candidate) && !remote_entries.contains_key(&candidate) {
            return candidate;
        }
    }
    format!("{path} (conflict {date} overflow)")
}

/// Does the remote object hold the same content the local fingerprint hashes?
///
/// Primary signal: a goblin fingerprint the store exposes (uploaded as object
/// metadata; surfaced by providers whose listings include metadata). Fallback:
/// an etag that *is* the content fingerprint (true for stores whose etag
/// function matches ours; never true for S3 md5/multipart etags — those
/// simply fail the match and fall through to review).
fn remote_content_matches(
    local_fingerprint: &str,
    remote_etag: Option<&str>,
    remote_fingerprint: Option<&str>,
) -> bool {
    if remote_fingerprint == Some(local_fingerprint) {
        return true;
    }
    remote_etag.is_some_and(|etag| etag.trim_matches('"') == local_fingerprint)
}

/// The decision table. Every arm is deliberate; there is no catch-all.
fn decide_file_sync(
    anchor: Option<&SyncAnchor>,
    current_local_fingerprint: Option<&str>,
    current_remote_etag: Option<&str>,
    current_remote_fingerprint: Option<&str>,
    strategy: ConflictStrategy,
) -> FileSyncDecision {
    let Some(anchor) = anchor.filter(|anchor| anchor.kind == "file") else {
        // No sync history for this path.
        return match (current_local_fingerprint, current_remote_etag) {
            (Some(local), Some(_)) => {
                if remote_content_matches(local, current_remote_etag, current_remote_fingerprint) {
                    FileSyncDecision::AnchorOnly
                } else {
                    FileSyncDecision::ReviewRequired
                }
            }
            (Some(_), None) => FileSyncDecision::Upload,
            (None, Some(_)) => FileSyncDecision::Download,
            (None, None) => FileSyncDecision::Noop,
        };
    };

    let anchor_local = anchor.local_fingerprint.as_deref();
    let anchor_remote = anchor.remote_etag.as_deref();

    match (current_local_fingerprint, current_remote_etag) {
        (Some(local), Some(remote)) => {
            let local_changed = anchor_local != Some(local);
            let remote_changed = anchor_remote != Some(remote);
            match (local_changed, remote_changed) {
                (false, false) => FileSyncDecision::Noop,
                (true, false) => FileSyncDecision::Upload,
                (false, true) => FileSyncDecision::Download,
                (true, true) => match strategy {
                    ConflictStrategy::PreferLocal => FileSyncDecision::Upload,
                    ConflictStrategy::PreferRemote => FileSyncDecision::Download,
                    ConflictStrategy::PreserveBoth => FileSyncDecision::DuplicateConflict,
                },
            }
        }
        (Some(local), None) => {
            if anchor_remote.is_none() {
                // The anchor never saw a remote object: this is still an
                // unsynced local file, changed or not.
                if anchor_local == Some(local) {
                    FileSyncDecision::Noop
                } else {
                    FileSyncDecision::Upload
                }
            } else if anchor_local == Some(local) {
                // Remote deleted, local untouched since last sync.
                FileSyncDecision::DeleteLocal
            } else {
                // Remote deleted AND local edited: a human must decide.
                FileSyncDecision::ReviewRequired
            }
        }
        (None, Some(remote)) => {
            if anchor_local.is_none() {
                // The anchor never saw a local file.
                if anchor_remote == Some(remote) {
                    FileSyncDecision::Noop
                } else {
                    FileSyncDecision::Download
                }
            } else if anchor_remote == Some(remote) {
                // Local deleted, remote untouched since last sync.
                FileSyncDecision::DeleteRemote
            } else {
                // Local deleted AND remote edited: a human must decide.
                FileSyncDecision::ReviewRequired
            }
        }
        (None, None) => FileSyncDecision::ForgetAnchor,
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
        (self.kind == EntryKind::File).then_some(self.size)
    }
}

impl FileSized for RemoteIndexedEntry {
    fn file_size(&self) -> Option<u64> {
        (self.kind == EntryKind::File).then_some(self.size)
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
                    kind: EntryKind::parse_or_file(&entry.kind),
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
                    kind: EntryKind::parse_or_file(&entry.kind),
                    size: entry.size,
                    etag: entry.etag.clone(),
                    storage_class: entry.storage_class.clone(),
                    fingerprint: entry.fingerprint.clone(),
                },
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{build_sync_plan, conflict_copy_path, Operation, SyncPlan, MAX_AUTO_DELETE_COUNT};
    use crate::storage::{
        local_index::{bytes_fingerprint, LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary},
        remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry},
        sync_db::SyncAnchor,
    };

    fn local_file(path: &str, content: &str) -> LocalIndexEntry {
        LocalIndexEntry {
            relative_path: path.into(),
            kind: "file".into(),
            size: content.len() as u64,
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
            fingerprint: None,
        }
    }

    fn remote_file_with_content(path: &str, content: &str, etag: &str) -> RemoteObjectEntry {
        RemoteObjectEntry {
            key: path.into(),
            relative_path: path.into(),
            kind: "file".into(),
            size: content.len() as u64,
            last_modified_at: None,
            etag: Some(etag.into()),
            storage_class: None,
            fingerprint: Some(bytes_fingerprint(content.as_bytes())),
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

    fn anchors_of(anchors: Vec<SyncAnchor>) -> BTreeMap<String, SyncAnchor> {
        anchors
            .into_iter()
            .map(|anchor| (anchor.path.clone(), anchor))
            .collect()
    }

    fn operations(plan: &SyncPlan) -> Vec<(&str, &str)> {
        plan.queue_items
            .iter()
            .map(|item| (item.path.as_str(), item.operation.as_str()))
            .collect()
    }

    // -- decision table: anchored states ------------------------------------

    #[test]
    fn anchored_unchanged_is_noop() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("a.txt", "v1")]),
            &remote_snapshot(vec![remote_file("a.txt", 2, "e1")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(plan.summary.noop_count, 1);
        assert!(plan.queue_items.is_empty());
    }

    #[test]
    fn anchored_local_edit_uploads() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("a.txt", "v2")]),
            &remote_snapshot(vec![remote_file("a.txt", 2, "e1")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "upload")]);
    }

    #[test]
    fn anchored_remote_edit_downloads() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("a.txt", "v1")]),
            &remote_snapshot(vec![remote_file("a.txt", 2, "e2")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "download")]);
    }

    #[test]
    fn anchored_dual_edit_duplicates_under_preserve_both() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("a.txt", "local v2")]),
            &remote_snapshot(vec![remote_file("a.txt", 9, "e2")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        let ops = operations(&plan);
        assert_eq!(ops.len(), 2);
        assert_eq!(ops[0], ("a.txt", "duplicate_conflict"));
        assert_eq!(ops[1], ("a.txt", "download"));
        let target = plan.queue_items[0].target_path.as_deref().expect("target");
        assert!(target.contains("(conflict "), "got '{target}'");
        assert!(target.ends_with(".txt"));
    }

    #[test]
    fn anchored_dual_edit_honors_prefer_strategies() {
        let local = local_snapshot(vec![local_file("a.txt", "local v2")]);
        let remote = remote_snapshot(vec![remote_file("a.txt", 9, "e2")]);
        let anchors = anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]);
        let prefer_local = build_sync_plan(&local, &remote, &anchors, "prefer-local", true);
        let prefer_remote = build_sync_plan(&local, &remote, &anchors, "prefer-remote", true);
        assert_eq!(operations(&prefer_local), vec![("a.txt", "upload")]);
        assert_eq!(operations(&prefer_remote), vec![("a.txt", "download")]);
    }

    #[test]
    fn anchored_local_delete_with_unchanged_remote_deletes_remote() {
        let plan = build_sync_plan(
            &local_snapshot(vec![]),
            &remote_snapshot(vec![remote_file("a.txt", 2, "e1")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "delete_remote")]);
        assert_eq!(plan.summary.delete_count, 1);
    }

    #[test]
    fn anchored_remote_delete_with_unchanged_local_deletes_local() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("a.txt", "v1")]),
            &remote_snapshot(vec![]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "delete_local")]);
    }

    #[test]
    fn anchored_local_delete_with_remote_edit_requires_review() {
        let plan = build_sync_plan(
            &local_snapshot(vec![]),
            &remote_snapshot(vec![remote_file("a.txt", 2, "e2")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "review_required")]);
    }

    #[test]
    fn anchored_remote_delete_with_local_edit_requires_review() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("a.txt", "v2")]),
            &remote_snapshot(vec![]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "review_required")]);
    }

    #[test]
    fn anchored_gone_on_both_sides_forgets_the_anchor() {
        let plan = build_sync_plan(
            &local_snapshot(vec![]),
            &remote_snapshot(vec![]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("a.txt", "forget_anchor")]);
    }

    // -- decision table: unanchored states ----------------------------------

    #[test]
    fn unanchored_local_only_uploads_and_remote_only_downloads() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("up.txt", "u")]),
            &remote_snapshot(vec![remote_file("down.txt", 1, "e1")]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        let ops = operations(&plan);
        assert!(ops.contains(&("up.txt", "upload")));
        assert!(ops.contains(&("down.txt", "download")));
    }

    #[test]
    fn unanchored_identical_content_anchors_without_transfer() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("same.txt", "bytes")]),
            &remote_snapshot(vec![remote_file_with_content(
                "same.txt", "bytes", "opaque",
            )]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("same.txt", "anchor_only")]);
        assert_eq!(plan.summary.anchor_count, 1);
    }

    #[test]
    fn unanchored_identical_content_matches_via_fingerprint_style_etag() {
        let fingerprint = bytes_fingerprint(b"bytes");
        let etag = format!("\"{fingerprint}\"");
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("same.txt", "bytes")]),
            &remote_snapshot(vec![remote_file("same.txt", 5, &etag)]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("same.txt", "anchor_only")]);
    }

    #[test]
    fn unanchored_different_content_requires_review() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("clash.txt", "mine")]),
            &remote_snapshot(vec![remote_file_with_content("clash.txt", "theirs", "e1")]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("clash.txt", "review_required")]);
    }

    // -- structure: directories, kind mismatch, cold storage ----------------

    #[test]
    fn local_directory_plans_remote_placeholder() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_dir("docs")]),
            &remote_snapshot(vec![]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("docs", "create_directory")]);
    }

    #[test]
    fn kind_mismatch_is_conflict_review() {
        let mut remote_dir_entry = remote_file("mixed", 0, "e");
        remote_dir_entry.kind = "directory".into();
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("mixed", "data")]),
            &remote_snapshot(vec![remote_dir_entry]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("mixed", "conflict_review")]);
    }

    #[test]
    fn cold_storage_class_is_skipped() {
        let mut frozen = remote_file("cold.bin", 10, "e1");
        frozen.storage_class = Some("GLACIER".into());
        let plan = build_sync_plan(
            &local_snapshot(vec![]),
            &remote_snapshot(vec![frozen]),
            &BTreeMap::new(),
            "preserve-both",
            true,
        );
        assert!(plan.queue_items.is_empty());
        assert_eq!(plan.summary.noop_count, 1);
    }

    // -- rename pairing ------------------------------------------------------

    #[test]
    fn local_rename_pairs_into_move_remote() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("new-name.txt", "contents")]),
            &remote_snapshot(vec![remote_file("old-name.txt", 8, "e1")]),
            &anchors_of(vec![file_anchor("old-name.txt", "contents", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("old-name.txt", "move_remote")]);
        assert_eq!(
            plan.queue_items[0].target_path.as_deref(),
            Some("new-name.txt")
        );
        assert_eq!(plan.summary.move_count, 1);
        assert_eq!(plan.summary.delete_count, 0);
    }

    #[test]
    fn remote_rename_pairs_into_move_local() {
        let plan = build_sync_plan(
            &local_snapshot(vec![local_file("old-name.txt", "contents")]),
            &remote_snapshot(vec![remote_file("new-name.txt", 8, "e1")]),
            &anchors_of(vec![file_anchor("old-name.txt", "contents", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(operations(&plan), vec![("old-name.txt", "move_local")]);
        assert_eq!(
            plan.queue_items[0].target_path.as_deref(),
            Some("new-name.txt")
        );
    }

    #[test]
    fn ambiguous_rename_degrades_to_upload_plus_delete() {
        // Two new local files with identical content: pairing is ambiguous,
        // so the old path deletes and both new paths upload.
        let plan = build_sync_plan(
            &local_snapshot(vec![
                local_file("copy-a.txt", "contents"),
                local_file("copy-b.txt", "contents"),
            ]),
            &remote_snapshot(vec![remote_file("old-name.txt", 8, "e1")]),
            &anchors_of(vec![file_anchor("old-name.txt", "contents", Some("e1"))]),
            "preserve-both",
            true,
        );
        let ops = operations(&plan);
        assert!(ops.contains(&("copy-a.txt", "upload")));
        assert!(ops.contains(&("copy-b.txt", "upload")));
        assert!(ops.contains(&("old-name.txt", "delete_remote")));
        assert_eq!(plan.summary.move_count, 0);
    }

    // -- mass-delete circuit breaker ----------------------------------------

    #[test]
    fn mass_delete_is_suppressed_into_review() {
        let mut anchors = Vec::new();
        let mut remote = Vec::new();
        for index in 0..(MAX_AUTO_DELETE_COUNT + 5) {
            let path = format!("bulk/file-{index}.txt");
            let content = format!("content-{index}");
            let etag = format!("etag-{index}");
            anchors.push(file_anchor(&path, &content, Some(&etag)));
            remote.push(remote_file(&path, content.len() as u64, &etag));
        }

        let plan = build_sync_plan(
            &local_snapshot(vec![]),
            &remote_snapshot(remote),
            &anchors_of(anchors),
            "preserve-both",
            true,
        );

        assert_eq!(plan.summary.delete_count, 0);
        assert_eq!(
            plan.summary.suppressed_delete_count,
            MAX_AUTO_DELETE_COUNT + 5
        );
        assert!(plan
            .queue_items
            .iter()
            .all(|item| item.operation == "review_required"));
    }

    #[test]
    fn small_delete_batches_pass_the_breaker() {
        let plan = build_sync_plan(
            &local_snapshot(vec![]),
            &remote_snapshot(vec![remote_file("a.txt", 2, "e1")]),
            &anchors_of(vec![file_anchor("a.txt", "v1", Some("e1"))]),
            "preserve-both",
            true,
        );
        assert_eq!(plan.summary.delete_count, 1);
        assert_eq!(plan.summary.suppressed_delete_count, 0);
    }

    // -- conflict copy naming ------------------------------------------------

    #[test]
    fn conflict_copy_naming_preserves_extension_and_avoids_collisions() {
        let empty_local = BTreeMap::new();
        let empty_remote = BTreeMap::new();
        assert_eq!(
            conflict_copy_path(
                "docs/report.txt",
                "2026-07-19T00:00:00Z",
                &empty_local,
                &empty_remote
            ),
            "docs/report (conflict 2026-07-19).txt"
        );
        assert_eq!(
            conflict_copy_path(
                "no-extension",
                "2026-07-19T00:00:00Z",
                &empty_local,
                &empty_remote
            ),
            "no-extension (conflict 2026-07-19)"
        );
    }

    #[test]
    fn operation_round_trips_through_strings() {
        for operation in [
            Operation::Upload,
            Operation::Download,
            Operation::CreateDirectory,
            Operation::DeleteRemote,
            Operation::DeleteLocal,
            Operation::MoveRemote,
            Operation::MoveLocal,
            Operation::DuplicateConflict,
            Operation::AnchorOnly,
            Operation::ForgetAnchor,
            Operation::ConflictReview,
            Operation::ReviewRequired,
        ] {
            assert_eq!(Operation::parse(operation.as_str()), Some(operation));
        }
        assert_eq!(Operation::parse("bogus"), None);
    }
}
