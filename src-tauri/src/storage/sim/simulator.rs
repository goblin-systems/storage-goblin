use std::collections::{BTreeMap, BTreeSet};

use super::super::error::SyncError;
use super::super::local_index::{
    bytes_fingerprint, LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary,
};
use super::super::now_iso;
use super::super::remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry};
use super::super::sync_db::SyncAnchor;
use super::super::sync_planner::{build_sync_plan, SyncPlan};
use super::memory_store::MemoryObjectStore;
use super::object_storage::{ObjectRecord, ObjectStorage};

/// Outcome of one plan → execute cycle.
#[derive(Debug, Default)]
pub(crate) struct CycleOutcome {
    pub uploaded: Vec<String>,
    pub downloaded: Vec<String>,
    pub directories_created: Vec<String>,
    pub deleted_remote: Vec<String>,
    pub deleted_local: Vec<String>,
    /// (from, to) pairs executed as moves.
    pub moved: Vec<(String, String)>,
    /// Paths whose anchors were written or dropped without a transfer.
    pub anchored: Vec<String>,
    /// Paths the planner parked for human review (`conflict_review` /
    /// `review_required`).
    pub review: Vec<String>,
    pub errors: Vec<(String, SyncError)>,
    pub plan: Option<SyncPlan>,
}

impl CycleOutcome {
    pub fn transfer_count(&self) -> usize {
        self.uploaded.len() + self.downloaded.len()
    }

    /// Anything that changed state this cycle (transfers, deletes, moves,
    /// directory creation, anchor writes).
    pub fn mutation_count(&self) -> usize {
        self.transfer_count()
            + self.directories_created.len()
            + self.deleted_remote.len()
            + self.deleted_local.len()
            + self.moved.len()
            + self.anchored.len()
    }
}

/// Drives the sync planner against in-memory local and remote state.
///
/// Local files live in a `path -> bytes` map; the remote side is a
/// [`MemoryObjectStore`] addressed through the [`ObjectStorage`] trait.
/// Directory objects use the `path/` trailing-slash key convention, matching
/// the adapters' placeholder keys.
pub(crate) struct SyncSimulator {
    pub local_files: BTreeMap<String, Vec<u8>>,
    pub local_directories: BTreeSet<String>,
    pub store: MemoryObjectStore,
    pub anchors: BTreeMap<String, SyncAnchor>,
    pub conflict_strategy: String,
}

impl SyncSimulator {
    pub fn new() -> Self {
        Self::with_store(MemoryObjectStore::new())
    }

    pub fn with_store(store: MemoryObjectStore) -> Self {
        Self {
            local_files: BTreeMap::new(),
            local_directories: BTreeSet::new(),
            store,
            anchors: BTreeMap::new(),
            conflict_strategy: "preserve-both".into(),
        }
    }

    pub fn with_conflict_strategy(mut self, strategy: &str) -> Self {
        self.conflict_strategy = strategy.into();
        self
    }

    // -- fixture helpers ----------------------------------------------------

    pub fn write_local(&mut self, path: &str, contents: &[u8]) {
        self.local_files.insert(path.to_string(), contents.to_vec());
    }

    pub fn delete_local(&mut self, path: &str) {
        self.local_files.remove(path);
    }

    pub fn write_remote(&mut self, path: &str, contents: &[u8]) {
        self.store.seed(path, contents);
    }

    pub fn delete_remote(&mut self, path: &str) {
        self.store.seed_delete(path);
    }

    /// Record that `path` was previously synced with the given contents on
    /// both sides (i.e. establish the last-known-synced anchor).
    pub fn anchor_synced_file(&mut self, path: &str, contents: &[u8]) {
        let etag = self.store.etag(path);
        self.anchors.insert(
            path.to_string(),
            SyncAnchor {
                path: path.to_string(),
                kind: "file".into(),
                local_fingerprint: Some(bytes_fingerprint(contents)),
                remote_etag: etag,
                synced_at: now_iso(),
            },
        );
    }

    /// Seed identical content locally, remotely, and in the anchor — the
    /// steady state after a successful sync of `path`.
    pub fn seed_synced_file(&mut self, path: &str, contents: &[u8]) {
        self.write_local(path, contents);
        self.write_remote(path, contents);
        self.anchor_synced_file(path, contents);
    }

    // -- snapshots ----------------------------------------------------------

    pub fn local_snapshot(&self) -> LocalIndexSnapshot {
        let mut entries = Vec::new();
        let mut summary = LocalIndexSummary {
            indexed_at: now_iso(),
            file_count: 0,
            directory_count: 0,
            total_bytes: 0,
        };

        for directory in &self.local_directories {
            summary.directory_count += 1;
            entries.push(LocalIndexEntry {
                relative_path: directory.clone(),
                kind: "directory".into(),
                size: 0,
                modified_at: None,
                fingerprint: None,
            });
        }

        for (path, contents) in &self.local_files {
            summary.file_count += 1;
            summary.total_bytes += contents.len() as u64;
            entries.push(LocalIndexEntry {
                relative_path: path.clone(),
                kind: "file".into(),
                size: contents.len() as u64,
                modified_at: None,
                fingerprint: Some(bytes_fingerprint(contents)),
            });
        }

        LocalIndexSnapshot {
            version: 2,
            root_folder: "sim://local".into(),
            summary,
            entries,
        }
    }

    pub fn remote_snapshot(&mut self) -> Result<RemoteIndexSnapshot, SyncError> {
        let records = self.store.list(None)?;
        let entries: Vec<RemoteObjectEntry> = records
            .into_iter()
            .map(|record| record_to_entry(&record))
            .collect();

        let summary = RemoteIndexSummary {
            indexed_at: now_iso(),
            object_count: entries.iter().filter(|entry| entry.kind == "file").count() as u64,
            total_bytes: entries.iter().map(|entry| entry.size).sum(),
        };

        Ok(RemoteIndexSnapshot {
            version: 1,
            bucket: "sim-bucket".into(),
            excluded_prefixes: Vec::new(),
            summary,
            entries,
        })
    }

    // -- plan & execute -----------------------------------------------------

    pub fn plan(&mut self) -> Result<SyncPlan, SyncError> {
        let local = self.local_snapshot();
        let remote = self.remote_snapshot()?;
        Ok(build_sync_plan(
            &local,
            &remote,
            &self.anchors,
            &self.conflict_strategy,
            true,
        ))
    }

    /// Run one plan → execute cycle with per-item error isolation.
    pub fn run_cycle(&mut self) -> Result<CycleOutcome, SyncError> {
        let plan = self.plan()?;
        let mut outcome = CycleOutcome::default();

        for item in &plan.queue_items {
            match item.operation.as_str() {
                "upload" => {
                    let Some(contents) = self.local_files.get(&item.path).cloned() else {
                        outcome.errors.push((
                            item.path.clone(),
                            SyncError::storage(format!(
                                "planned upload source missing: '{}'",
                                item.path
                            )),
                        ));
                        continue;
                    };
                    match self.store.put(&item.path, &contents) {
                        Ok(record) => {
                            self.set_file_anchor(&item.path, &contents, &record);
                            outcome.uploaded.push(item.path.clone());
                        }
                        Err(error) => outcome.errors.push((item.path.clone(), error)),
                    }
                }
                "download" => match self.store.get(&item.path) {
                    Ok(contents) => {
                        let record = self.store.head(&item.path).ok().flatten();
                        self.local_files.insert(item.path.clone(), contents.clone());
                        if let Some(record) = record {
                            self.set_file_anchor(&item.path, &contents, &record);
                        }
                        outcome.downloaded.push(item.path.clone());
                    }
                    Err(error) => outcome.errors.push((item.path.clone(), error)),
                },
                "create_directory" => {
                    let key = format!("{}/", item.path.trim_end_matches('/'));
                    match self.store.put(&key, &[]) {
                        Ok(_) => outcome.directories_created.push(item.path.clone()),
                        Err(error) => outcome.errors.push((item.path.clone(), error)),
                    }
                }
                "delete_remote" => match self.store.delete(&item.path) {
                    Ok(()) => {
                        self.anchors.remove(&item.path);
                        outcome.deleted_remote.push(item.path.clone());
                    }
                    Err(error) => outcome.errors.push((item.path.clone(), error)),
                },
                "delete_local" => {
                    // Production sends this to the OS trash; the simulator just
                    // removes it from the in-memory tree.
                    self.local_files.remove(&item.path);
                    self.anchors.remove(&item.path);
                    outcome.deleted_local.push(item.path.clone());
                }
                "move_remote" => {
                    let Some(target) = item.target_path.clone() else {
                        outcome.errors.push((
                            item.path.clone(),
                            SyncError::internal("move_remote without target_path".to_string()),
                        ));
                        continue;
                    };
                    match self
                        .store
                        .copy(&item.path, &target)
                        .and_then(|record| self.store.delete(&item.path).map(|()| record))
                    {
                        Ok(record) => {
                            self.anchors.remove(&item.path);
                            if let Some(contents) = self.local_files.get(&target).cloned() {
                                self.set_file_anchor(&target, &contents, &record);
                            }
                            outcome.moved.push((item.path.clone(), target));
                        }
                        Err(error) => outcome.errors.push((item.path.clone(), error)),
                    }
                }
                "move_local" => {
                    let Some(target) = item.target_path.clone() else {
                        outcome.errors.push((
                            item.path.clone(),
                            SyncError::internal("move_local without target_path".to_string()),
                        ));
                        continue;
                    };
                    let Some(contents) = self.local_files.remove(&item.path) else {
                        outcome.errors.push((
                            item.path.clone(),
                            SyncError::storage(format!("move source missing: '{}'", item.path)),
                        ));
                        continue;
                    };
                    self.local_files.insert(target.clone(), contents.clone());
                    self.anchors.remove(&item.path);
                    if let Ok(Some(record)) = self.store.head(&target) {
                        self.set_file_anchor(&target, &contents, &record);
                    }
                    outcome.moved.push((item.path.clone(), target));
                }
                "duplicate_conflict" => {
                    // Keep both: local edit moves to the conflict name and
                    // uploads; the paired download restores the remote winner.
                    let Some(target) = item.target_path.clone() else {
                        outcome.errors.push((
                            item.path.clone(),
                            SyncError::internal(
                                "duplicate_conflict without target_path".to_string(),
                            ),
                        ));
                        continue;
                    };
                    let Some(contents) = self.local_files.remove(&item.path) else {
                        outcome.errors.push((
                            item.path.clone(),
                            SyncError::storage(format!(
                                "conflict duplicate source missing: '{}'",
                                item.path
                            )),
                        ));
                        continue;
                    };
                    self.local_files.insert(target.clone(), contents.clone());
                    self.anchors.remove(&item.path);
                    match self.store.put(&target, &contents) {
                        Ok(record) => {
                            self.set_file_anchor(&target, &contents, &record);
                            outcome.uploaded.push(target);
                        }
                        Err(error) => outcome.errors.push((item.path.clone(), error)),
                    }
                }
                "anchor_only" => {
                    let contents = self.local_files.get(&item.path).cloned();
                    let record = self.store.head(&item.path).ok().flatten();
                    if let (Some(contents), Some(record)) = (contents, record) {
                        self.set_file_anchor(&item.path, &contents, &record);
                        outcome.anchored.push(item.path.clone());
                    }
                }
                "forget_anchor" => {
                    self.anchors.remove(&item.path);
                    outcome.anchored.push(item.path.clone());
                }
                "conflict_review" | "review_required" => {
                    outcome.review.push(item.path.clone());
                }
                other => {
                    outcome.errors.push((
                        item.path.clone(),
                        SyncError::internal(format!(
                            "simulator cannot execute operation '{other}'"
                        )),
                    ));
                }
            }
        }

        outcome.plan = Some(plan);
        Ok(outcome)
    }

    /// Run cycles until a cycle performs no transfers, or fail after
    /// `max_cycles`. Returns the outcomes of every cycle run.
    pub fn run_until_settled(&mut self, max_cycles: usize) -> Result<Vec<CycleOutcome>, SyncError> {
        let mut outcomes = Vec::new();
        for _ in 0..max_cycles {
            let outcome = self.run_cycle()?;
            let settled = outcome.mutation_count() == 0 && outcome.errors.is_empty();
            outcomes.push(outcome);
            if settled {
                return Ok(outcomes);
            }
        }
        Err(SyncError::internal(format!(
            "simulation did not settle within {max_cycles} cycles"
        )))
    }

    // -- assertion helpers --------------------------------------------------

    /// File contents on the remote side, directory placeholders excluded.
    pub fn remote_files(&self) -> BTreeMap<String, Vec<u8>> {
        self.store
            .keys()
            .into_iter()
            .filter(|key| !key.ends_with('/'))
            .filter_map(|key| self.store.contents(&key).map(|bytes| (key, bytes)))
            .collect()
    }

    /// True when local and remote file sets are identical in path and content.
    pub fn is_converged(&self) -> bool {
        self.local_files == self.remote_files()
    }

    fn set_file_anchor(&mut self, path: &str, contents: &[u8], record: &ObjectRecord) {
        self.anchors.insert(
            path.to_string(),
            SyncAnchor {
                path: path.to_string(),
                kind: "file".into(),
                local_fingerprint: Some(bytes_fingerprint(contents)),
                remote_etag: Some(record.etag.clone()),
                synced_at: now_iso(),
            },
        );
    }
}

fn record_to_entry(record: &ObjectRecord) -> RemoteObjectEntry {
    let is_directory = record.key.ends_with('/');
    RemoteObjectEntry {
        key: record.key.clone(),
        relative_path: record.key.trim_end_matches('/').to_string(),
        kind: if is_directory { "directory" } else { "file" }.into(),
        size: record.size,
        last_modified_at: None,
        etag: Some(record.etag.clone()),
        storage_class: record.storage_class.clone(),
        fingerprint: if is_directory {
            None
        } else {
            record.fingerprint.clone()
        },
    }
}
