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

/// Turn a local write failure into something the user can act on
/// (backlog phase 2.4).
///
/// "failed to write: os error 112" tells a user nothing. Disk-full and
/// permission-denied are the two local failures that actually happen, they have
/// completely different remedies, and both are recoverable once named — so name
/// them. The raw error is kept for the log.
pub(crate) fn local_write_failure_message(
    path: &Path,
    error: &std::io::Error,
    bytes_written: u64,
) -> String {
    use std::io::ErrorKind;

    let explanation = match error.kind() {
        ErrorKind::StorageFull => Some("the disk is full"),
        ErrorKind::PermissionDenied => Some("permission was denied"),
        ErrorKind::ReadOnlyFilesystem => Some("the filesystem is read-only"),
        // Windows reports "not enough space" as a raw OS error rather than a
        // mapped ErrorKind, so match the code the platform actually returns.
        _ if is_disk_full_os_error(error) => Some("the disk is full"),
        _ => None,
    };

    match explanation {
        Some(reason) => format!(
            "Could not save '{}' because {reason} (after {bytes_written} bytes). \
             The partially downloaded file was discarded, so nothing was overwritten. \
             Underlying error: {error}",
            path.display()
        ),
        None => format!(
            "failed to write downloaded data to '{}': {error}",
            path.display()
        ),
    }
}

/// Windows: ERROR_DISK_FULL (112) and ERROR_HANDLE_DISK_FULL (39).
fn is_disk_full_os_error(error: &std::io::Error) -> bool {
    matches!(error.raw_os_error(), Some(112) | Some(39))
}

/// Refuse a download that obviously cannot fit (backlog phase 2.4).
///
/// Cheap insurance: finding out at byte zero costs one syscall, while finding
/// out at the last byte costs the whole transfer and leaves the user with a
/// cryptic write error. This is advisory only — a `None` answer (unsupported
/// platform, unreadable filesystem) never blocks the transfer, and a race
/// against another process filling the disk is still caught by `write_chunk`.
pub(crate) fn insufficient_space(
    available_bytes: Option<u64>,
    required_bytes: u64,
) -> Option<String> {
    // Leave headroom: filling a disk to the last byte breaks other software on
    // the machine, and journalled filesystems need slack to stay consistent.
    const HEADROOM_BYTES: u64 = 64 * 1024 * 1024;

    let available = available_bytes?;
    let needed = required_bytes.saturating_add(HEADROOM_BYTES);
    if available >= needed {
        return None;
    }

    Some(format!(
        "Not enough free disk space: {} needed (plus {} headroom), {} available.",
        format_bytes(required_bytes),
        format_bytes(HEADROOM_BYTES),
        format_bytes(available)
    ))
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

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
    /// Set only once the temp file has been renamed into place.
    ///
    /// Deliberately not inferred from `file` being taken: verification happens
    /// after the handle is closed but before the rename, so "handle closed" and
    /// "committed" are different facts. Conflating them leaked the temp file on
    /// every failed verification.
    committed: bool,
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
            committed: false,
        })
    }

    pub fn write_chunk(&mut self, chunk: &[u8]) -> Result<(), SyncError> {
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| SyncError::internal("download writer already finished"))?;

        file.write_all(chunk).map_err(|error| {
            SyncError::storage(local_write_failure_message(
                &self.temp_path,
                &error,
                self.bytes_written,
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
    #[cfg(test)]
    pub fn finish(self) -> Result<DownloadOutcome, SyncError> {
        self.finish_verified(&DownloadExpectation::default())
    }

    /// Check what landed against what was promised, then commit it
    /// (backlog phase 2.1 post-transfer verification).
    ///
    /// Verification happens **before** the rename, which is the entire point:
    /// once the file is at its real path a corrupt download is indistinguishable
    /// from a local edit, and the next cycle would faithfully upload the
    /// corruption over the good remote copy. Failing here leaves the
    /// destination untouched and the temp file discarded by `Drop`.
    pub fn finish_verified(
        mut self,
        expectation: &DownloadExpectation,
    ) -> Result<DownloadOutcome, SyncError> {
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

        let fingerprint = self.hex_fingerprint();
        expectation.check(&self.final_path, self.bytes_written, &fingerprint)?;

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
        self.committed = true;

        Ok(DownloadOutcome {
            bytes_written: self.bytes_written,
            fingerprint,
        })
    }

    fn hex_fingerprint(&self) -> String {
        let digest = self.hasher.clone().finalize();
        let mut fingerprint = String::with_capacity(digest.len() * 2);
        for byte in digest {
            use std::fmt::Write as _;
            let _ = write!(&mut fingerprint, "{byte:02x}");
        }
        fingerprint
    }
}

/// What a download was promised to contain, for verification before commit.
///
/// Every field is optional because the providers differ in what they will tell
/// us: GCS listings carry a content fingerprint, S3 listings do not, and an
/// S3 ETag is only a content hash for objects that were not uploaded in parts.
/// Whatever is known gets checked; nothing is invented.
#[derive(Debug, Clone, Default)]
pub(crate) struct DownloadExpectation {
    /// Byte count the provider said the object has.
    pub size: Option<u64>,
    /// SHA-256 the provider recorded for the content, if any.
    pub fingerprint: Option<String>,
}

impl DownloadExpectation {
    pub fn with_size(size: Option<u64>) -> Self {
        Self {
            size,
            fingerprint: None,
        }
    }

    fn check(
        &self,
        final_path: &Path,
        bytes_written: u64,
        fingerprint: &str,
    ) -> Result<(), SyncError> {
        // Size is the cheap check and catches the failure that actually
        // happens: a connection dropped mid-body, leaving a short file that
        // otherwise looks perfectly valid.
        if let Some(expected) = self.size {
            if expected != bytes_written {
                return Err(SyncError::transient(format!(
                    "Download of '{}' is incomplete: expected {expected} bytes, received \
                     {bytes_written}. The file was not written; the transfer will be retried.",
                    final_path.display()
                )));
            }
        }

        if let Some(expected) = self.fingerprint.as_deref() {
            if !expected.eq_ignore_ascii_case(fingerprint) {
                return Err(SyncError::transient(format!(
                    "Download of '{}' does not match the content the provider recorded \
                     (expected {expected}, got {fingerprint}). The file was not written.",
                    final_path.display()
                )));
            }
        }

        Ok(())
    }
}

impl Drop for DownloadWriter {
    fn drop(&mut self) {
        // Anything that did not reach the rename leaves debris behind: a
        // failed verification, an early return, a `?`, or a panic.
        if !self.committed {
            self.file.take();
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
    use super::{insufficient_space, local_write_failure_message, DownloadExpectation};

    #[test]
    fn a_truncated_download_never_reaches_the_destination() {
        let root = temp_dir("verify-truncated");
        let destination = root.join("important.bin");
        fs::write(&destination, b"the good original contents").expect("seed destination");

        let mut writer = DownloadWriter::create(&destination).expect("writer should open");
        writer.write_chunk(b"short").expect("write should succeed");

        // The provider said 1000 bytes; the connection died after 5.
        let error = writer
            .finish_verified(&DownloadExpectation::with_size(Some(1000)))
            .expect_err("a short body must be rejected");
        assert!(
            error.message.contains("incomplete"),
            "got: {}",
            error.message
        );
        // Retryable: a dropped connection deserves another attempt.
        assert!(error.is_retryable());

        // This is the assertion that matters. If the truncated file had landed,
        // the next scan would read it as a local edit and upload five bytes
        // over the good remote copy.
        assert_eq!(
            fs::read(&destination).expect("destination should still exist"),
            b"the good original contents",
            "the original file must be untouched after a failed verification"
        );
        assert_eq!(
            fs::read_dir(&root).expect("read dir").count(),
            1,
            "the temp file must be discarded, not left behind"
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_download_whose_content_does_not_match_is_rejected() {
        let root = temp_dir("verify-fingerprint");
        let destination = root.join("f.bin");

        let mut writer = DownloadWriter::create(&destination).expect("writer should open");
        writer.write_chunk(b"actual bytes").expect("write");

        let error = writer
            .finish_verified(&DownloadExpectation {
                size: None,
                fingerprint: Some(
                    "0000000000000000000000000000000000000000000000000000000000000000".into(),
                ),
            })
            .expect_err("a fingerprint mismatch must be rejected");

        assert!(
            error.message.contains("does not match"),
            "got: {}",
            error.message
        );
        assert!(
            !destination.exists(),
            "a corrupt download must not be written"
        );

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_download_matching_its_expectation_commits_normally() {
        let root = temp_dir("verify-ok");
        let destination = root.join("f.bin");

        let mut writer = DownloadWriter::create(&destination).expect("writer should open");
        writer.write_chunk(b"twelve bytes").expect("write");
        let probe = writer.hex_fingerprint();

        let mut writer = DownloadWriter::create(&destination).expect("writer should open");
        writer.write_chunk(b"twelve bytes").expect("write");
        let outcome = writer
            .finish_verified(&DownloadExpectation {
                size: Some(12),
                fingerprint: Some(probe.clone()),
            })
            .expect("a matching download should commit");

        assert_eq!(outcome.bytes_written, 12);
        assert_eq!(outcome.fingerprint, probe);
        assert_eq!(fs::read(&destination).expect("read"), b"twelve bytes");

        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn an_unknown_expectation_does_not_block_a_download() {
        // S3 listings carry no content hash; a missing expectation must mean
        // "cannot check", never "reject".
        let root = temp_dir("verify-unknown");
        let destination = root.join("f.bin");

        let mut writer = DownloadWriter::create(&destination).expect("writer should open");
        writer.write_chunk(b"anything").expect("write");
        writer
            .finish_verified(&DownloadExpectation::default())
            .expect("an unverifiable download must still be written");

        assert!(destination.exists());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn a_disk_full_write_says_so_and_says_nothing_was_overwritten() {
        let error = std::io::Error::from(std::io::ErrorKind::StorageFull);
        let message = local_write_failure_message(
            std::path::Path::new("C:/x/y.bin.goblin-tmp"),
            &error,
            4096,
        );

        assert!(message.contains("the disk is full"), "got: {message}");
        // The reassurance matters: the user's existing file is intact, and
        // saying so is the difference between a scare and an inconvenience.
        assert!(
            message.contains("nothing was overwritten"),
            "got: {message}"
        );
        assert!(message.contains("4096 bytes"), "got: {message}");
    }

    #[test]
    fn a_permission_error_names_the_cause_rather_than_the_errno() {
        let error = std::io::Error::from(std::io::ErrorKind::PermissionDenied);
        let message = local_write_failure_message(std::path::Path::new("/x/y"), &error, 0);
        assert!(message.contains("permission was denied"), "got: {message}");
    }

    #[test]
    fn windows_reports_disk_full_as_a_raw_os_error() {
        // ERROR_DISK_FULL does not map to ErrorKind::StorageFull on Windows,
        // so without the raw-code check the user would see "os error 112".
        let error = std::io::Error::from_raw_os_error(112);
        let message = local_write_failure_message(std::path::Path::new("/x/y"), &error, 10);
        assert!(message.contains("the disk is full"), "got: {message}");
    }

    #[test]
    fn an_unrecognized_write_error_still_reports_the_underlying_cause() {
        let error = std::io::Error::from(std::io::ErrorKind::InvalidInput);
        let message = local_write_failure_message(std::path::Path::new("/x/y"), &error, 0);
        assert!(
            message.contains("failed to write downloaded data"),
            "got: {message}"
        );
    }

    #[test]
    fn a_download_that_cannot_fit_is_refused_before_it_starts() {
        let refusal = insufficient_space(Some(100 * 1024 * 1024), 200 * 1024 * 1024)
            .expect("200 MiB cannot fit in 100 MiB");
        assert!(
            refusal.contains("Not enough free disk space"),
            "got: {refusal}"
        );
        assert!(refusal.contains("200.0 MiB"), "got: {refusal}");
    }

    #[test]
    fn a_download_that_fits_with_headroom_is_allowed() {
        assert!(insufficient_space(Some(10 * 1024 * 1024 * 1024), 1024 * 1024 * 1024).is_none());
    }

    #[test]
    fn a_download_that_would_fill_the_last_byte_is_refused() {
        // Filling a disk completely breaks other software on the machine and
        // leaves journalled filesystems no slack, so the headroom is required
        // rather than advisory.
        let exactly_enough = 500 * 1024 * 1024;
        assert!(
            insufficient_space(Some(exactly_enough), exactly_enough).is_some(),
            "a transfer that consumes every free byte must be refused"
        );
    }

    #[test]
    fn unknown_free_space_never_blocks_a_transfer() {
        // A preflight that cannot read the filesystem must not be the reason a
        // download is refused; write_chunk still reports a real disk-full.
        assert!(insufficient_space(None, u64::MAX).is_none());
    }

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
