use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};
use tauri::{AppHandle, Runtime};

use super::{app_storage_path, now_iso, system_time_to_iso, LOCAL_INDEX_FILE_NAME};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LocalIndexSummary {
    pub indexed_at: String,
    pub file_count: u64,
    pub directory_count: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalIndexEntry {
    pub relative_path: String,
    pub kind: String,
    pub size: u64,
    pub modified_at: Option<String>,
    #[serde(default)]
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalIndexSnapshot {
    pub version: u32,
    pub root_folder: String,
    pub summary: LocalIndexSummary,
    pub entries: Vec<LocalIndexEntry>,
}

pub fn read_local_index_snapshot<R: Runtime>(
    app: &AppHandle<R>,
) -> Result<Option<LocalIndexSnapshot>, String> {
    let path = app_storage_path(app, LOCAL_INDEX_FILE_NAME)?;
    if !path.exists() {
        return Ok(None);
    }

    read_local_index_snapshot_file(&path).map(Some)
}

pub fn read_local_index_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair_id: &str,
) -> Result<Option<LocalIndexSnapshot>, String> {
    let file_name = local_index_file_name_for_pair(pair_id);
    let path = app_storage_path(app, &file_name)?;
    if !path.exists() {
        return Ok(None);
    }

    read_local_index_snapshot_file(&path).map(Some)
}

pub fn write_local_index_snapshot_for_pair<R: Runtime>(
    app: &AppHandle<R>,
    pair_id: &str,
    snapshot: &LocalIndexSnapshot,
) -> Result<(), String> {
    let file_name = local_index_file_name_for_pair(pair_id);
    let path = app_storage_path(app, &file_name)?;
    write_local_index_snapshot_file(&path, snapshot)
}

/// Scan `root`, reusing fingerprints from `previous` for files whose size and
/// modification time are unchanged (backlog phase 2.3).
///
/// Hashing dominates scan cost — the phase-0 baseline measured ~7.8k files/s,
/// so a 100k-file tree spends ~13s hashing on every cycle even when nothing
/// changed. This is the standard rsync-style heuristic.
///
/// The tradeoff is deliberate: a file modified without its size or mtime
/// changing (a deliberate mtime restore, or an edit within the filesystem's
/// timestamp granularity) will not be re-hashed and so will not be detected
/// until a full rescan. That is the same bargain rsync, Dropbox, and every
/// other sync client make; the alternative is re-reading every byte forever.
pub fn scan_local_folder_with_cache(
    root: &Path,
    previous: Option<&LocalIndexSnapshot>,
) -> Result<LocalIndexSnapshot, String> {
    ensure_scannable_root(root)?;

    let cache = match previous {
        Some(snapshot) if snapshot_matches_folder(snapshot, &root.to_string_lossy()) => {
            FingerprintCache::from_snapshot(snapshot)
        }
        // A snapshot of a different folder tells us nothing about this one.
        _ => FingerprintCache::empty(),
    };

    let mut entries = Vec::new();
    let mut summary = LocalIndexSummary {
        indexed_at: now_iso(),
        file_count: 0,
        directory_count: 0,
        total_bytes: 0,
    };

    scan_directory_recursive(root, root, &mut entries, &mut summary, &cache)?;

    Ok(LocalIndexSnapshot {
        version: 2,
        root_folder: root.to_string_lossy().into_owned(),
        summary,
        entries,
    })
}

/// Above this many distinct changed subtrees, a full rescan is cheaper than
/// many partial ones — and avoids pathological cases like an unpack of ten
/// thousand files producing ten thousand directory walks.
const MAX_INCREMENTAL_SUBTREES: usize = 64;

/// Rescan only the parts of `root` affected by `changed_paths`, carrying the
/// rest of `previous` forward unchanged (backlog phase 2.3).
///
/// The watcher knows exactly which paths moved, but the cycle used to throw
/// that away and walk the whole tree, so "near real-time sync" got slower in
/// proportion to how much data you had rather than how much you changed. This
/// makes the cost proportional to the change.
///
/// Returns `None` when the change set cannot be handled incrementally — too
/// many separate subtrees, a path outside the root, or the root itself. The
/// caller falls back to a full scan; a wrong answer here would mean silently
/// missing a file forever, so anything unclear declines.
pub fn rescan_changed_paths(
    root: &Path,
    previous: &LocalIndexSnapshot,
    changed_paths: &[PathBuf],
) -> Option<Result<LocalIndexSnapshot, String>> {
    if changed_paths.is_empty() || !snapshot_matches_folder(previous, &root.to_string_lossy()) {
        return None;
    }

    let subtrees = incremental_subtrees(root, changed_paths)?;

    Some(rescan_subtrees(root, previous, &subtrees))
}

/// The set of directories to re-walk, as paths relative to the root.
///
/// A change to `a/b/c.txt` is handled by re-walking `a/b`: that covers the file
/// being created, modified, *or* deleted, because a delete is only visible as
/// an absence from its parent's listing. The empty string means the root.
fn incremental_subtrees(root: &Path, changed_paths: &[PathBuf]) -> Option<Vec<String>> {
    let mut subtrees: BTreeSet<String> = BTreeSet::new();

    for path in changed_paths {
        // Watchers report absolute paths; anything outside the tree we are
        // indexing means our assumptions do not hold.
        let relative = path.strip_prefix(root).ok()?;
        let parent = relative.parent()?;

        let key = parent.to_string_lossy().replace('\\', "/");
        // The root itself as a subtree is just a full rescan.
        if key.is_empty() {
            subtrees.insert(String::new());
        } else {
            subtrees.insert(key);
        }
    }

    if subtrees.len() > MAX_INCREMENTAL_SUBTREES {
        return None;
    }

    // Drop any subtree already covered by an ancestor, so a directory is never
    // walked twice and its entries cannot be added twice.
    let all: Vec<String> = subtrees.into_iter().collect();
    let minimal: Vec<String> = all
        .iter()
        .filter(|candidate| {
            !all.iter()
                .any(|other| other != *candidate && is_under(candidate, other))
        })
        .cloned()
        .collect();

    Some(minimal)
}

/// Is `path` inside `ancestor` (or equal to it)? Both are relative, `/`-joined.
///
/// The empty ancestor is the root and contains everything.
fn is_under(path: &str, ancestor: &str) -> bool {
    if ancestor.is_empty() {
        return true;
    }
    if path == ancestor {
        return true;
    }
    path.starts_with(ancestor) && path.as_bytes().get(ancestor.len()) == Some(&b'/')
}

fn rescan_subtrees(
    root: &Path,
    previous: &LocalIndexSnapshot,
    subtrees: &[String],
) -> Result<LocalIndexSnapshot, String> {
    ensure_scannable_root(root)?;
    let cache = FingerprintCache::from_snapshot(previous);

    // Carry forward everything the change set did not touch.
    let mut entries: Vec<LocalIndexEntry> = previous
        .entries
        .iter()
        .filter(|entry| {
            !subtrees
                .iter()
                .any(|subtree| is_under(&entry.relative_path, subtree))
        })
        .cloned()
        .collect();

    let mut summary = LocalIndexSummary {
        indexed_at: now_iso(),
        file_count: 0,
        directory_count: 0,
        total_bytes: 0,
    };

    for subtree in subtrees {
        let absolute = if subtree.is_empty() {
            root.to_path_buf()
        } else {
            root.join(subtree)
        };

        // A subtree that no longer exists was deleted; its entries have already
        // been dropped above and there is nothing to re-walk.
        if !absolute.is_dir() {
            continue;
        }

        // The directory itself is an entry too, unless it is the root.
        if !subtree.is_empty() {
            entries.push(LocalIndexEntry {
                relative_path: subtree.clone(),
                kind: "directory".into(),
                size: 0,
                modified_at: fs::metadata(&absolute)
                    .ok()
                    .and_then(|metadata| metadata.modified().ok())
                    .and_then(system_time_to_iso),
                fingerprint: None,
            });
        }

        scan_directory_recursive(root, &absolute, &mut entries, &mut summary, &cache)?;
    }

    // The running summary only counted the rescanned parts, so recompute it
    // over the merged set rather than trying to patch the deltas.
    entries.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    entries.dedup_by(|left, right| left.relative_path == right.relative_path);
    let summary = LocalIndexSummary {
        indexed_at: summary.indexed_at,
        file_count: entries.iter().filter(|entry| entry.kind == "file").count() as u64,
        directory_count: entries
            .iter()
            .filter(|entry| entry.kind == "directory")
            .count() as u64,
        total_bytes: entries
            .iter()
            .filter(|entry| entry.kind == "file")
            .map(|entry| entry.size)
            .sum(),
    };

    Ok(LocalIndexSnapshot {
        version: 2,
        root_folder: root.to_string_lossy().into_owned(),
        summary,
        entries,
    })
}

/// Fingerprints from the previous scan, keyed by path, valid only while a
/// file's size and modification time both match.
struct FingerprintCache {
    entries: std::collections::HashMap<String, (u64, Option<String>, String)>,
}

impl FingerprintCache {
    fn empty() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
        }
    }

    fn from_snapshot(snapshot: &LocalIndexSnapshot) -> Self {
        let entries = snapshot
            .entries
            .iter()
            .filter(|entry| entry.kind == "file")
            .filter_map(|entry| {
                entry.fingerprint.as_ref().map(|fingerprint| {
                    (
                        entry.relative_path.clone(),
                        (entry.size, entry.modified_at.clone(), fingerprint.clone()),
                    )
                })
            })
            .collect();
        Self { entries }
    }

    /// The cached fingerprint, if this file looks untouched since last scan.
    fn reuse(
        &self,
        relative_path: &str,
        size: u64,
        modified_at: &Option<String>,
    ) -> Option<String> {
        let (cached_size, cached_modified_at, fingerprint) = self.entries.get(relative_path)?;
        // Both must match: size alone misses same-length edits, and mtime
        // alone misses filesystems that preserve it across a write.
        if *cached_size == size && cached_modified_at == modified_at {
            Some(fingerprint.clone())
        } else {
            None
        }
    }
}

pub(crate) fn snapshot_matches_folder(snapshot: &LocalIndexSnapshot, folder: &str) -> bool {
    Path::new(&snapshot.root_folder) == Path::new(folder)
}

fn local_index_file_name_for_pair(pair_id: &str) -> String {
    format!("storage-goblin-local-index-{pair_id}.json")
}

fn ensure_scannable_root(root: &Path) -> Result<(), String> {
    if !root.exists() {
        return Err(format!(
            "Configured local folder was not found: {}",
            root.display()
        ));
    }

    let metadata = fs::metadata(root).map_err(|error| {
        format!(
            "Failed to inspect local folder '{}': {error}",
            root.display()
        )
    })?;

    if !metadata.is_dir() {
        return Err(format!(
            "Configured local folder is not a directory: {}",
            root.display()
        ));
    }

    fs::read_dir(root).map_err(|error| {
        format!(
            "Configured local folder is not readable: {} ({error})",
            root.display()
        )
    })?;

    Ok(())
}

fn scan_directory_recursive(
    root: &Path,
    current: &Path,
    entries: &mut Vec<LocalIndexEntry>,
    summary: &mut LocalIndexSummary,
    cache: &FingerprintCache,
) -> Result<(), String> {
    let mut children = fs::read_dir(current)
        .map_err(|error| format!("Failed to read directory '{}': {error}", current.display()))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "Failed to enumerate directory '{}': {error}",
                current.display()
            )
        })?;

    children.sort_by_key(|entry| entry.path());

    for child in children {
        let path = child.path();
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            format!("Failed to read metadata for '{}': {error}", path.display())
        })?;

        if metadata.file_type().is_symlink() {
            continue;
        }

        // Never index an in-flight download's temp file: it is partial by
        // definition, and indexing it would upload the partial content as if
        // it were a real local file.
        if metadata.is_file() && super::transfer::is_temp_download_path(&path) {
            continue;
        }

        if metadata.is_dir() {
            summary.directory_count += 1;
            entries.push(LocalIndexEntry {
                relative_path: relative_path(root, &path)?,
                kind: "directory".into(),
                size: 0,
                modified_at: metadata.modified().ok().and_then(system_time_to_iso),
                fingerprint: None,
            });

            scan_directory_recursive(root, &path, entries, summary, cache)?;
            continue;
        }

        if metadata.is_file() {
            summary.file_count += 1;
            summary.total_bytes += metadata.len();

            let relative = relative_path(root, &path)?;
            let size = metadata.len();
            let modified_at = metadata.modified().ok().and_then(system_time_to_iso);
            let fingerprint = match cache.reuse(&relative, size, &modified_at) {
                Some(cached) => cached,
                None => file_fingerprint(&path)?,
            };

            entries.push(LocalIndexEntry {
                relative_path: relative,
                kind: "file".into(),
                size,
                modified_at,
                fingerprint: Some(fingerprint),
            });
        }
    }

    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<String, String> {
    let relative = path.strip_prefix(root).map_err(|error| {
        format!(
            "Failed to calculate relative path for '{}': {error}",
            path.display()
        )
    })?;
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

pub(crate) fn file_fingerprint(path: &Path) -> Result<String, String> {
    let bytes = fs::read(path).map_err(|error| {
        format!(
            "failed to read local file '{}' for fingerprinting: {error}",
            path.display()
        )
    })?;
    Ok(hex_sha256(&bytes))
}

#[cfg(test)]
pub(crate) fn bytes_fingerprint(bytes: &[u8]) -> String {
    hex_sha256(bytes)
}

fn hex_sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn read_local_index_snapshot_file(path: &Path) -> Result<LocalIndexSnapshot, String> {
    let raw = fs::read_to_string(path)
        .map_err(|error| format!("failed to read local index snapshot: {error}"))?;
    serde_json::from_str(&raw)
        .map_err(|error| format!("failed to parse local index snapshot: {error}"))
}

fn write_local_index_snapshot_file(
    path: &Path,
    snapshot: &LocalIndexSnapshot,
) -> Result<(), String> {
    let raw = serde_json::to_string_pretty(snapshot)
        .map_err(|error| format!("failed to serialize local index snapshot: {error}"))?;
    fs::write(path, raw).map_err(|error| format!("failed to write local index snapshot: {error}"))
}

#[cfg(test)]
mod tests {
    use super::{
        bytes_fingerprint, is_under, local_index_file_name_for_pair,
        read_local_index_snapshot_file, rescan_changed_paths, scan_local_folder_with_cache,
        write_local_index_snapshot_file, LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary,
        MAX_INCREMENTAL_SUBTREES,
    };
    use std::{
        env, fs,
        path::{Path, PathBuf},
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn temp_path(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_nanos();
        env::temp_dir().join(format!("storage-goblin-{name}-{}-{suffix}", process::id()))
    }

    /// Paths present in a snapshot, sorted — the whole contract of a scan.
    fn snapshot_paths(snapshot: &LocalIndexSnapshot) -> Vec<String> {
        let mut paths: Vec<String> = snapshot
            .entries
            .iter()
            .map(|entry| entry.relative_path.clone())
            .collect();
        paths.sort();
        paths
    }

    fn fingerprint_of(snapshot: &LocalIndexSnapshot, path: &str) -> Option<String> {
        snapshot
            .entries
            .iter()
            .find(|entry| entry.relative_path == path)
            .and_then(|entry| entry.fingerprint.clone())
    }

    /// Build a small tree: a.txt, deep/b.txt, deep/deeper/c.txt
    fn build_incremental_tree(root: &Path) {
        fs::create_dir_all(root.join("deep/deeper")).expect("should create tree");
        fs::write(root.join("a.txt"), b"a").expect("write a");
        fs::write(root.join("deep/b.txt"), b"bb").expect("write b");
        fs::write(root.join("deep/deeper/c.txt"), b"ccc").expect("write c");
    }

    #[test]
    fn an_incremental_rescan_matches_a_full_rescan_after_an_edit() {
        let root = temp_path("incremental-edit");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        fs::write(root.join("deep/b.txt"), b"bb-changed-longer").expect("edit b");

        let incremental = rescan_changed_paths(&root, &before, &[root.join("deep/b.txt")])
            .expect("a single in-tree edit should be handled incrementally")
            .expect("rescan should succeed");
        let full = scan_local_folder_with_cache(&root, None).expect("full scan");

        // The incremental answer must be indistinguishable from the full one,
        // or the planner would act on a different tree than actually exists.
        assert_eq!(snapshot_paths(&incremental), snapshot_paths(&full));
        assert_eq!(incremental.summary.file_count, full.summary.file_count);
        assert_eq!(incremental.summary.total_bytes, full.summary.total_bytes);
        assert_eq!(
            fingerprint_of(&incremental, "deep/b.txt"),
            fingerprint_of(&full, "deep/b.txt"),
            "the edited file must be re-hashed, not carried forward"
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn an_incremental_rescan_notices_a_deleted_file() {
        let root = temp_path("incremental-delete");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        fs::remove_file(root.join("deep/b.txt")).expect("delete b");

        let incremental = rescan_changed_paths(&root, &before, &[root.join("deep/b.txt")])
            .expect("handled incrementally")
            .expect("rescan should succeed");

        // A delete is only visible as an absence from the parent listing, so
        // this is the case a naive "rescan the changed path" would miss —
        // leaving a deleted file in the index and never propagating the delete.
        assert!(
            !snapshot_paths(&incremental).contains(&"deep/b.txt".to_string()),
            "deleted file still present: {:?}",
            snapshot_paths(&incremental)
        );
        assert_eq!(
            snapshot_paths(&incremental),
            snapshot_paths(&scan_local_folder_with_cache(&root, None).expect("full"))
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn an_incremental_rescan_notices_a_new_file_and_a_new_directory() {
        let root = temp_path("incremental-add");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        fs::create_dir_all(root.join("deep/fresh")).expect("create dir");
        fs::write(root.join("deep/fresh/d.txt"), b"dddd").expect("write d");

        let incremental = rescan_changed_paths(&root, &before, &[root.join("deep/fresh")])
            .expect("handled incrementally")
            .expect("rescan should succeed");
        let full = scan_local_folder_with_cache(&root, None).expect("full");

        assert_eq!(snapshot_paths(&incremental), snapshot_paths(&full));
        assert_eq!(incremental.summary.total_bytes, full.summary.total_bytes);

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn an_incremental_rescan_handles_a_deleted_directory_subtree() {
        let root = temp_path("incremental-rmdir");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        fs::remove_dir_all(root.join("deep/deeper")).expect("remove subtree");

        let incremental = rescan_changed_paths(&root, &before, &[root.join("deep/deeper")])
            .expect("handled incrementally")
            .expect("rescan should succeed");
        let full = scan_local_folder_with_cache(&root, None).expect("full");

        assert_eq!(snapshot_paths(&incremental), snapshot_paths(&full));
        assert!(!snapshot_paths(&incremental)
            .iter()
            .any(|path| path.starts_with("deep/deeper")));

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn untouched_subtrees_are_carried_forward_rather_than_rewalked() {
        let root = temp_path("incremental-carry");
        build_incremental_tree(&root);
        let mut before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        // Sentinel: if this survives, the entry was carried forward untouched
        // rather than being re-hashed — which is the entire point.
        for entry in &mut before.entries {
            if entry.relative_path == "deep/deeper/c.txt" {
                entry.fingerprint = Some("carried-forward-sentinel".into());
            }
        }

        fs::write(root.join("a.txt"), b"a-changed").expect("edit a");
        let incremental = rescan_changed_paths(&root, &before, &[root.join("a.txt")])
            .expect("handled incrementally")
            .expect("rescan should succeed");

        assert_eq!(
            fingerprint_of(&incremental, "deep/deeper/c.txt").as_deref(),
            Some("carried-forward-sentinel")
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn overlapping_subtrees_do_not_duplicate_entries() {
        let root = temp_path("incremental-overlap");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        // "deep" already covers "deep/deeper"; walking both would list the same
        // files twice and inflate every count in the summary.
        let incremental = rescan_changed_paths(
            &root,
            &before,
            &[
                root.join("deep/b.txt"),
                root.join("deep/deeper/c.txt"),
                root.join("deep/deeper/nested/x"),
            ],
        )
        .expect("handled incrementally")
        .expect("rescan should succeed");
        let full = scan_local_folder_with_cache(&root, None).expect("full");

        assert_eq!(snapshot_paths(&incremental), snapshot_paths(&full));
        assert_eq!(incremental.summary.file_count, full.summary.file_count);
        assert_eq!(
            incremental.summary.directory_count,
            full.summary.directory_count
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_change_outside_the_root_declines_rather_than_guessing() {
        let root = temp_path("incremental-outside");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        // Answering anything here would be answering about a different tree.
        assert!(
            rescan_changed_paths(&root, &before, &[PathBuf::from("/somewhere/else/x.txt")])
                .is_none()
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_snapshot_of_a_different_folder_declines() {
        let root = temp_path("incremental-mismatch");
        build_incremental_tree(&root);
        let mut before = scan_local_folder_with_cache(&root, None).expect("initial scan");
        before.root_folder = "/some/other/folder".into();

        assert!(rescan_changed_paths(&root, &before, &[root.join("a.txt")]).is_none());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn too_many_separate_subtrees_falls_back_to_a_full_scan() {
        let root = temp_path("incremental-flood");
        build_incremental_tree(&root);
        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");

        // Past this point many partial walks cost more than one full walk.
        let flood: Vec<PathBuf> = (0..=MAX_INCREMENTAL_SUBTREES)
            .map(|index| root.join(format!("dir{index}/file.txt")))
            .collect();

        assert!(rescan_changed_paths(&root, &before, &flood).is_none());

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn subtree_containment_does_not_match_sibling_prefixes() {
        // "deep2" starts with "deep" but is not inside it. Getting this wrong
        // would silently drop a sibling directory's entries on every rescan.
        assert!(is_under("deep/b.txt", "deep"));
        assert!(is_under("deep", "deep"));
        assert!(!is_under("deep2/b.txt", "deep"));
        assert!(!is_under("deeper", "deep"));
        assert!(is_under("anything/at/all", ""));
    }

    #[test]
    #[ignore = "measurement, not a correctness check; run with --ignored"]
    fn measure_incremental_versus_full_rescan() {
        let root = temp_path("incremental-bench");
        // 200 directories x 50 files = 10,000 files, one of which changes.
        for dir in 0..200 {
            let directory = root.join(format!("d{dir}"));
            fs::create_dir_all(&directory).expect("create dir");
            for file in 0..50 {
                fs::write(
                    directory.join(format!("f{file}.txt")),
                    format!("{dir}-{file}"),
                )
                .expect("write file");
            }
        }

        let before = scan_local_folder_with_cache(&root, None).expect("initial scan");
        let changed = root.join("d7/f13.txt");
        fs::write(&changed, b"changed content here").expect("edit");

        let started = std::time::Instant::now();
        let full = scan_local_folder_with_cache(&root, Some(&before)).expect("full");
        let full_elapsed = started.elapsed();

        let started = std::time::Instant::now();
        let incremental = rescan_changed_paths(&root, &before, &[changed])
            .expect("incremental")
            .expect("rescan");
        let incremental_elapsed = started.elapsed();

        assert_eq!(snapshot_paths(&incremental), snapshot_paths(&full));
        println!(
            "full (cached): {full_elapsed:?}  incremental: {incremental_elapsed:?}  files: {}",
            full.summary.file_count
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn scans_nested_files_and_directories() {
        let root = temp_path("scan");
        let nested = root.join("nested");

        fs::create_dir_all(&nested).expect("should create nested test directory");
        fs::write(root.join("alpha.txt"), b"alpha").expect("should write root file");
        fs::write(nested.join("beta.txt"), b"beta-data").expect("should write nested file");

        let snapshot = scan_local_folder_with_cache(&root, None).expect("scan should succeed");

        assert_eq!(snapshot.summary.file_count, 2);
        assert_eq!(snapshot.summary.directory_count, 1);
        assert_eq!(snapshot.summary.total_bytes, 14);
        assert_eq!(snapshot.entries.len(), 3);
        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "nested"));
        assert!(snapshot
            .entries
            .iter()
            .any(|entry| entry.relative_path == "nested/beta.txt" && entry.fingerprint.is_some()));

        fs::remove_dir_all(root).expect("should clean up scan test directory");
    }

    #[test]
    fn rejects_missing_root_folder() {
        let missing = temp_path("missing");
        let error = scan_local_folder_with_cache(&missing, None)
            .expect_err("scan should fail for missing root");
        assert!(error.contains("not found"));
    }

    #[test]
    fn round_trips_snapshot_json() {
        let root = temp_path("roundtrip-root");
        let snapshot_path = temp_path("roundtrip-json").with_extension("json");

        fs::create_dir_all(&root).expect("should create roundtrip directory");
        fs::write(root.join("file.txt"), b"hello").expect("should write roundtrip file");

        let snapshot = scan_local_folder_with_cache(&root, None).expect("scan should succeed");
        write_local_index_snapshot_file(&snapshot_path, &snapshot)
            .expect("should write snapshot json");
        let restored =
            read_local_index_snapshot_file(&snapshot_path).expect("should read snapshot json");

        assert_eq!(restored.summary.file_count, 1);
        assert_eq!(restored.entries.len(), 1);
        assert!(restored.entries[0].fingerprint.is_some());

        fs::remove_file(snapshot_path).expect("should clean up snapshot json");
        fs::remove_dir_all(root).expect("should clean up roundtrip directory");
    }

    #[test]
    fn pair_file_name_includes_pair_id() {
        assert_eq!(
            local_index_file_name_for_pair("abc-123"),
            "storage-goblin-local-index-abc-123.json"
        );
    }

    #[test]
    fn pair_file_name_differs_from_global() {
        let pair_name = local_index_file_name_for_pair("default");
        assert_ne!(pair_name, super::super::LOCAL_INDEX_FILE_NAME);
    }

    #[test]
    fn fingerprint_changes_for_same_size_content() {
        let root = temp_path("fingerprint-same-size");
        fs::create_dir_all(&root).expect("should create test root");
        let file_path = root.join("note.txt");

        fs::write(&file_path, b"alpha").expect("should write first content");
        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");

        fs::write(&file_path, b"bravo").expect("should write second content");
        let second = scan_local_folder_with_cache(&root, None).expect("second scan should succeed");

        assert_eq!(first.entries[0].size, second.entries[0].size);
        assert_ne!(first.entries[0].fingerprint, second.entries[0].fingerprint);

        fs::remove_dir_all(root).expect("should clean up fingerprint test directory");
    }

    /// A fingerprint that could never be computed from the file's bytes, so
    /// seeing it in a scan result proves the value was reused rather than
    /// recalculated.
    const SENTINEL: &str = "cached-not-recomputed";

    fn snapshot_claiming(
        root: &Path,
        relative_path: &str,
        size: u64,
        modified_at: Option<String>,
    ) -> LocalIndexSnapshot {
        LocalIndexSnapshot {
            version: 2,
            root_folder: root.to_string_lossy().into_owned(),
            summary: LocalIndexSummary::default(),
            entries: vec![LocalIndexEntry {
                relative_path: relative_path.into(),
                kind: "file".into(),
                size,
                modified_at,
                fingerprint: Some(SENTINEL.into()),
            }],
        }
    }

    #[test]
    fn an_unchanged_file_reuses_its_cached_fingerprint_instead_of_rehashing() {
        let root = temp_path("cache-hit");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("stable.txt"), b"contents").expect("file should write");

        // Learn the real size/mtime, then claim a sentinel fingerprint for it.
        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first
            .entries
            .iter()
            .find(|entry| entry.relative_path == "stable.txt")
            .expect("file should be indexed");
        let previous =
            snapshot_claiming(&root, "stable.txt", entry.size, entry.modified_at.clone());

        let second = scan_local_folder_with_cache(&root, Some(&previous))
            .expect("cached scan should succeed");

        let cached = second
            .entries
            .iter()
            .find(|entry| entry.relative_path == "stable.txt")
            .expect("file should be indexed");
        assert_eq!(
            cached.fingerprint.as_deref(),
            Some(SENTINEL),
            "an untouched file should not be re-hashed"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_file_whose_size_changed_is_rehashed() {
        let root = temp_path("cache-size");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("grown.txt"), b"contents").expect("file should write");

        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first
            .entries
            .iter()
            .find(|entry| entry.relative_path == "grown.txt")
            .expect("file should be indexed");
        // Cache claims a stale, smaller size.
        let previous = snapshot_claiming(
            &root,
            "grown.txt",
            entry.size - 1,
            entry.modified_at.clone(),
        );

        let second =
            scan_local_folder_with_cache(&root, Some(&previous)).expect("scan should succeed");

        let rescanned = second
            .entries
            .iter()
            .find(|entry| entry.relative_path == "grown.txt")
            .expect("file should be indexed");
        assert_eq!(
            rescanned.fingerprint.as_deref(),
            Some(bytes_fingerprint(b"contents").as_str()),
            "a size change must invalidate the cached fingerprint"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_file_whose_mtime_changed_is_rehashed() {
        let root = temp_path("cache-mtime");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("touched.txt"), b"contents").expect("file should write");

        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first
            .entries
            .iter()
            .find(|entry| entry.relative_path == "touched.txt")
            .expect("file should be indexed");
        let previous = snapshot_claiming(
            &root,
            "touched.txt",
            entry.size,
            Some("1999-01-01T00:00:00Z".to_string()),
        );

        let second =
            scan_local_folder_with_cache(&root, Some(&previous)).expect("scan should succeed");

        let rescanned = second
            .entries
            .iter()
            .find(|entry| entry.relative_path == "touched.txt")
            .expect("file should be indexed");
        assert_eq!(
            rescanned.fingerprint.as_deref(),
            Some(bytes_fingerprint(b"contents").as_str()),
            "an mtime change must invalidate the cached fingerprint"
        );

        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn a_snapshot_of_a_different_folder_is_not_used_as_a_cache() {
        let root = temp_path("cache-other-folder");
        fs::create_dir_all(&root).expect("root should create");
        fs::write(root.join("file.txt"), b"contents").expect("file should write");

        let first = scan_local_folder_with_cache(&root, None).expect("first scan should succeed");
        let entry = first.entries[0].clone();
        // Same path and stats, but recorded against a different root.
        let mut previous = snapshot_claiming(&root, "file.txt", entry.size, entry.modified_at);
        previous.root_folder = "C:/some/other/folder".into();

        let second =
            scan_local_folder_with_cache(&root, Some(&previous)).expect("scan should succeed");

        assert_eq!(
            second.entries[0].fingerprint.as_deref(),
            Some(bytes_fingerprint(b"contents").as_str()),
            "a cache from another folder must be ignored"
        );

        let _ = fs::remove_dir_all(&root);
    }
}
