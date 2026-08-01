//! Streaming, atomic writes for downloads (backlog phase 2.1).
//!
//! Downloads used to buffer the whole object in memory and then write it
//! straight to its final path. That has two failure modes this module exists
//! to remove:
//!
//! 1. A multi-GB object meant a multi-GB memory spike.
//! 2. A crash (or a disk-full) part-way through the write left a *truncated
//!    file at the real path*. The next scan fingerprints it as a local edit
//!    and uploads the corruption over the good remote copy — silent data loss.
//!
//! [`DownloadWriter`] streams chunks to a sibling temp file, hashing as it
//! goes, then fsyncs and atomically renames into place. Until that rename the
//! destination path is untouched, so an interrupted download is a no-op rather
//! than a corruption. Temp files left by a hard kill are swept by
//! [`cleanup_orphaned_temp_files`].

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use sha2::{Digest, Sha256};

use super::error::SyncError;

/// Marks in-progress downloads. Chosen so it cannot collide with a real
/// synced file: the scanner skips this extension, and the sweeper removes it.
pub(crate) const TEMP_DOWNLOAD_EXTENSION: &str = "goblin-tmp";

/// What a completed download turned out to contain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DownloadOutcome {
    pub bytes_written: u64,
    /// SHA-256 of the received bytes, for post-transfer verification.
    pub fingerprint: String,
}

/// Streams a download to a temp file and atomically moves it into place.
///
/// Dropping without calling [`DownloadWriter::finish`] removes the temp file,
/// so an error path or an early return cannot leave debris behind.
pub(crate) struct DownloadWriter {
    final_path: PathBuf,
    temp_path: PathBuf,
    file: Option<File>,
    bytes_written: u64,
    hasher: Sha256,
}

impl DownloadWriter {
    /// Create the destination's parent directories and open a temp file
    /// beside the destination. Same-directory placement keeps the final
    /// rename on one filesystem, where it is atomic.
    pub fn create(final_path: &Path) -> Result<Self, SyncError> {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent).map_err(|error| {
                SyncError::storage(format!(
                    "failed to create parent directory for '{}': {error}",
                    final_path.display()
                ))
            })?;
        }

        let temp_path = temp_path_for(final_path);
        let file = File::create(&temp_path).map_err(|error| {
            SyncError::storage(format!(
                "failed to open temporary download file '{}': {error}",
                temp_path.display()
            ))
        })?;

        Ok(Self {
            final_path: final_path.to_path_buf(),
            temp_path,
            file: Some(file),
            bytes_written: 0,
            hasher: Sha256::new(),
        })
    }

    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), SyncError> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| SyncError::internal("download writer already finished"))?;

        file.write_all(chunk).map_err(|error| {
            SyncError::storage(format!(
                "failed to write downloaded data to '{}': {error}",
                self.temp_path.display()
            ))
        })?;

        self.hasher.update(chunk);
        self.bytes_written += chunk.len() as u64;
        Ok(())
    }

    /// Flush, fsync, and atomically rename into place.
    ///
    /// The fsync matters: without it the rename can land while the file's
    /// contents are still only in the page cache, so a power loss would leave
    /// an empty-but-correctly-named file — exactly the corruption this module
    /// exists to prevent.
    pub fn finish(mut self) -> Result<DownloadOutcome, SyncError> {
        let mut file = self
            .file
            .take()
            .ok_or_else(|| SyncError::internal("download writer already finished"))?;

        file.flush().map_err(|error| {
            SyncError::storage(format!(
                "failed to flush '{}': {error}",
                self.temp_path.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            SyncError::storage(format!(
                "failed to sync '{}' to disk: {error}",
                self.temp_path.display()
            ))
        })?;
        drop(file);

        // Windows will not rename onto an existing file.
        if self.final_path.exists() {
            fs::remove_file(&self.final_path).map_err(|error| {
                SyncError::storage(format!(
                    "failed to replace '{}': {error}",
                    self.final_path.display()
                ))
            })?;
        }

        fs::rename(&self.temp_path, &self.final_path).map_err(|error| {
            SyncError::storage(format!(
                "failed to move downloaded file into place at '{}': {error}",
                self.final_path.display()
            ))
        })?;

        let digest = self.hasher.clone().finalize();
        let mut fingerprint = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut fingerprint, "{byte:02x}");
        }

        Ok(DownloadOutcome {
            bytes_written: self.bytes_written,
            fingerprint,
        })
    }
}

impl Drop for DownloadWriter {
    fn drop(&mut self) {
        // Only reached when finish() was not called (error or early return).
        if self.file.take().is_some() {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

fn temp_path_for(final_path: &Path) -> PathBuf {
    let mut name = final_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "download".to_string());
    name.push('.');
    name.push_str(TEMP_DOWNLOAD_EXTENSION);
    final_path.with_file_name(name)
}

/// True when a path is one of our in-progress download temp files.
pub(crate) fn is_temp_download_path(path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == TEMP_DOWNLOAD_EXTENSION)
}

/// Remove temp files left behind by a hard kill, so they neither accumulate
/// nor get mistaken for real files. Returns how many were removed.
///
/// Errors on individual files are ignored: a sweep that cannot delete one
/// stale file must not stop the sync that follows it.
pub(crate) fn cleanup_orphaned_temp_files(root: &Path) -> u64 {
    fn sweep(dir: &Path, removed: &mut u64) {
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(file_type) if file_type.is_dir() => sweep(&path, removed),
                Ok(file_type) if file_type.is_file() && is_temp_download_path(&path) => {
                    if fs::remove_file(&path).is_ok() {
                        *removed += 1;
                    }
                }
                _ => {}
            }
        }
    }

    let mut removed = 0;
    sweep(root, &mut removed);
    removed
}

/// Wall-clock budget for transferring `size_bytes`.
///
/// A single fixed cap (300s for every file, regardless of size) meant a large
/// object on a slow link could never finish: it was killed mid-transfer and
/// retried forever, making multi-GB files permanently unsyncable rather than
/// merely slow. The budget now scales with size, assuming a pessimistic floor
/// throughput plus a fixed allowance for connection setup and provider
/// latency.
///
/// This is still wall-clock, not the idle-based timeout phase 2.1 ultimately
/// wants ("no bytes received for N seconds"). It removes the impossible-for-
/// large-files failure; promptly detecting a stalled-but-not-dead transfer
/// still needs byte-level progress plumbed through from the writer.
pub(crate) fn transfer_timeout(size_bytes: u64) -> Duration {
    /// Deliberately pessimistic: this is a backstop for a wedged transfer,
    /// not a performance target. Too tight and slow links break; too loose
    /// and a dead connection hangs the queue.
    const FLOOR_BYTES_PER_SEC: u64 = 128 * 1024;
    /// Covers connection setup, auth, and provider-side latency.
    const BASE: Duration = Duration::from_secs(60);
    /// Nothing should hold a queue slot longer than this.
    const MAX: Duration = Duration::from_secs(6 * 60 * 60);

    let transfer_seconds = size_bytes / FLOOR_BYTES_PER_SEC;
    BASE.saturating_add(Duration::from_secs(transfer_seconds))
        .min(MAX)
}

#[cfg(test)]
mod tests {
    use super::{
        cleanup_orphaned_temp_files, is_temp_download_path, DownloadWriter, TEMP_DOWNLOAD_EXTENSION,
    };
    use std::fs;
    use std::path::{Path, PathBuf};

    fn temp_dir(name: &str) -> PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time should be after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "storage-goblin-transfer-{name}-{}-{suffix}",
            std::process::id()
        ));
        fs::create_dir_all(&dir).expect("temp dir should create");
        dir
    }

    #[test]
    fn streams_chunks_and_reports_size_and_fingerprint() {
        let dir = temp_dir("stream");
        let target = dir.join("nested/deep/file.bin");

        let mut writer = DownloadWriter::create(&target).expect("writer should create");
        writer.write_chunk(b"hello ").expect("chunk should write");
        writer.write_chunk(b"world").expect("chunk should write");
        let outcome = writer.finish().expect("finish should succeed");

        assert_eq!(
            fs::read(&target).expect("file should exist"),
            b"hello world"
        );
        assert_eq!(outcome.bytes_written, 11);
        assert_eq!(
            outcome.fingerprint,
            crate::storage::local_index::bytes_fingerprint(b"hello world"),
            "fingerprint must match the engine's content hash so verification can compare them"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_destination_is_untouched_until_finish() {
        let dir = temp_dir("atomic");
        let target = dir.join("file.txt");
        fs::write(&target, b"original").expect("seed should write");

        let mut writer = DownloadWriter::create(&target).expect("writer should create");
        writer
            .write_chunk(b"replacement")
            .expect("chunk should write");

        // Mid-download the real file still holds the old contents.
        assert_eq!(fs::read(&target).expect("file should exist"), b"original");

        writer.finish().expect("finish should succeed");
        assert_eq!(
            fs::read(&target).expect("file should exist"),
            b"replacement"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_abandoned_download_leaves_neither_a_partial_file_nor_debris() {
        let dir = temp_dir("abandon");
        let target = dir.join("file.txt");
        fs::write(&target, b"original").expect("seed should write");

        {
            let mut writer = DownloadWriter::create(&target).expect("writer should create");
            writer.write_chunk(b"partial").expect("chunk should write");
            // Dropped without finish — the failure path.
        }

        assert_eq!(
            fs::read(&target).expect("file should exist"),
            b"original",
            "an interrupted download must not corrupt the existing file"
        );
        let leftovers: Vec<_> = fs::read_dir(&dir)
            .expect("dir should read")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| is_temp_download_path(path))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp file should be cleaned: {leftovers:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn downloading_a_brand_new_path_creates_it() {
        let dir = temp_dir("new");
        let target = dir.join("a/b/c.txt");

        let mut writer = DownloadWriter::create(&target).expect("writer should create");
        writer.write_chunk(b"fresh").expect("chunk should write");
        writer.finish().expect("finish should succeed");

        assert_eq!(fs::read(&target).expect("file should exist"), b"fresh");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sweeping_removes_stale_temp_files_recursively_and_spares_real_ones() {
        let dir = temp_dir("sweep");
        fs::create_dir_all(dir.join("nested")).expect("dirs should create");
        fs::write(dir.join("keep.txt"), b"real").expect("write");
        fs::write(
            dir.join(format!("orphan.txt.{TEMP_DOWNLOAD_EXTENSION}")),
            b"junk",
        )
        .expect("write");
        fs::write(
            dir.join(format!("nested/deep.bin.{TEMP_DOWNLOAD_EXTENSION}")),
            b"junk",
        )
        .expect("write");

        let removed = cleanup_orphaned_temp_files(&dir);

        assert_eq!(removed, 2);
        assert!(dir.join("keep.txt").exists(), "real files must survive");
        assert!(!dir
            .join(format!("orphan.txt.{TEMP_DOWNLOAD_EXTENSION}"))
            .exists());
        assert!(!dir
            .join(format!("nested/deep.bin.{TEMP_DOWNLOAD_EXTENSION}"))
            .exists());

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn sweeping_a_missing_directory_is_a_no_op() {
        assert_eq!(
            cleanup_orphaned_temp_files(Path::new("C:/definitely/not/a/real/sync/root")),
            0
        );
    }

    #[test]
    fn the_transfer_budget_scales_with_size() {
        use super::transfer_timeout;

        // A small file gets roughly the fixed allowance.
        assert_eq!(transfer_timeout(0).as_secs(), 60);
        assert_eq!(transfer_timeout(1024).as_secs(), 60);

        // A 1 GiB object gets far more than the old flat 300s, which it could
        // never have met on a slow link.
        let one_gib = transfer_timeout(1024 * 1024 * 1024).as_secs();
        assert!(one_gib > 300, "1 GiB budget was only {one_gib}s");
        assert!(one_gib >= 60 + 8192, "1 GiB budget was {one_gib}s");

        // Budgets never shrink as files grow.
        assert!(transfer_timeout(10_000_000) >= transfer_timeout(1_000_000));

        // …and are capped so a wedged transfer cannot hold a slot forever.
        assert_eq!(
            transfer_timeout(u64::MAX).as_secs(),
            6 * 60 * 60,
            "the budget must stay bounded"
        );
    }
}
