//! S3 multipart upload (backlog phase 2.1).
//!
//! `put_object` caps a single object at 5 GB. That is not a slow path, it is a
//! wall: a 6 GB video or VM image simply could not be synced, and no amount of
//! retrying would change that. Multipart raises the ceiling to 5 TB and makes a
//! failed upload cost one part instead of the whole file.
//!
//! Split out of `s3_adapter` so the adapter stays inside the module-size gate,
//! mirroring `gcs_upload`.
//!
//! The wire calls are thin SDK wrappers; the part of this that can actually be
//! wrong is the *sequencing* — how bytes are divided into parts, that the
//! completion manifest lists every part in order, and that a failure anywhere
//! aborts the upload instead of leaking billable orphaned parts. That logic
//! lives in [`plan_parts`] and [`run_multipart_upload`], both of which are
//! driven directly by tests through the [`MultipartSink`] seam.

use std::collections::HashMap;
use std::future::Future;
use std::path::Path;

use aws_sdk_s3::primitives::{ByteStream, Length};
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;

use super::error::SyncError;

/// Objects at or above this size upload as multipart. Below it the single
/// `put_object` is one round trip instead of three and is simply faster.
pub(crate) const MULTIPART_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;

/// Preferred part size. S3 requires every part except the last to be at least
/// 5 MiB; 16 MiB keeps the part count low for typical files without making a
/// single retry expensive.
pub(crate) const TARGET_PART_SIZE_BYTES: u64 = 16 * 1024 * 1024;

/// S3 accepts at most 10,000 parts per upload. Beyond that the part size has
/// to grow, or the upload is rejected after the bytes have already been sent.
pub(crate) const MAX_PARTS: u64 = 10_000;

/// One part of a multipart upload: which slice of the file it carries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartPlan {
    /// S3 part numbers are 1-based.
    pub part_number: i32,
    pub offset: u64,
    pub length: u64,
}

/// Divide `total_size` bytes into parts of at most `target_part_size`,
/// growing the part size if that would exceed [`MAX_PARTS`].
///
/// A 5 TB object at 16 MiB parts would need 327,680 parts, so the size has to
/// scale with the object rather than being a constant. Growing it here — before
/// a single byte moves — is the difference between a slow upload and one that
/// is rejected at the finish line.
pub(crate) fn plan_parts(total_size: u64, target_part_size: u64) -> Vec<PartPlan> {
    let target_part_size = target_part_size.max(1);
    // Round up so the part count lands at or below the limit.
    let required = total_size.div_ceil(MAX_PARTS);
    let part_size = target_part_size.max(required);

    let mut parts = Vec::new();
    let mut offset = 0_u64;
    let mut part_number = 1_i32;
    while offset < total_size {
        let length = part_size.min(total_size - offset);
        parts.push(PartPlan {
            part_number,
            offset,
            length,
        });
        offset += length;
        part_number += 1;
    }
    parts
}

/// The four multipart operations, behind a seam so the sequencing above can be
/// tested without a live bucket or a hand-rolled fake of S3's XML protocol.
pub(crate) trait MultipartSink {
    fn create_upload(&self) -> impl Future<Output = Result<String, SyncError>> + Send;

    /// Returns the part's ETag, which the completion manifest must echo back.
    fn upload_part(
        &self,
        upload_id: &str,
        part: &PartPlan,
    ) -> impl Future<Output = Result<String, SyncError>> + Send;

    fn complete_upload(
        &self,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> impl Future<Output = Result<(), SyncError>> + Send;

    fn abort_upload(&self, upload_id: &str) -> impl Future<Output = Result<(), SyncError>> + Send;
}

/// Drive one multipart upload to completion, or abort it and report why.
///
/// Aborting on failure is not politeness: parts of an abandoned upload stay in
/// the bucket, invisible to `ListObjects` but billed as storage, until an
/// explicit abort or a lifecycle rule removes them. A sync client that retries
/// a failing 4 GB upload every poll would quietly accumulate charges the user
/// cannot even see.
pub(crate) async fn run_multipart_upload<S: MultipartSink>(
    sink: &S,
    parts: &[PartPlan],
) -> Result<(), SyncError> {
    let upload_id = sink.create_upload().await?;

    let mut completed: Vec<(i32, String)> = Vec::with_capacity(parts.len());
    for part in parts {
        match sink.upload_part(&upload_id, part).await {
            Ok(etag) => completed.push((part.part_number, etag)),
            Err(error) => return Err(abort_and_report(sink, &upload_id, error).await),
        }
    }

    match sink.complete_upload(&upload_id, &completed).await {
        Ok(()) => Ok(()),
        Err(error) => Err(abort_and_report(sink, &upload_id, error).await),
    }
}

/// Clean up the abandoned upload, then return the *original* failure — the
/// abort's own outcome is housekeeping and must not mask what actually broke.
async fn abort_and_report<S: MultipartSink>(
    sink: &S,
    upload_id: &str,
    error: SyncError,
) -> SyncError {
    if let Err(abort_error) = sink.abort_upload(upload_id).await {
        eprintln!(
            "[storage-goblin] failed to abort multipart upload {upload_id} after an error \
             ({abort_error}); orphaned parts may accrue storage charges until a lifecycle \
             rule removes them"
        );
    }
    error
}

/// The real sink: SDK calls against one bucket/key/file.
struct S3MultipartSink<'a> {
    client: &'a Client,
    bucket: &'a str,
    key: &'a str,
    path: &'a Path,
    metadata: Option<HashMap<String, String>>,
}

impl MultipartSink for S3MultipartSink<'_> {
    async fn create_upload(&self) -> Result<String, SyncError> {
        let mut request = self
            .client
            .create_multipart_upload()
            .bucket(self.bucket)
            .key(self.key);
        if let Some(metadata) = self.metadata.clone() {
            request = request.set_metadata(Some(metadata));
        }

        let response = request.send().await.map_err(|error| {
            super::s3_adapter::classify_sdk_error(
                &error,
                format!(
                    "failed to start multipart upload of '{}' to bucket '{}': {error}",
                    self.key, self.bucket
                ),
            )
        })?;

        response.upload_id().map(str::to_string).ok_or_else(|| {
            SyncError::internal(format!(
                "S3 started a multipart upload of '{}' without returning an upload id",
                self.key
            ))
        })
    }

    async fn upload_part(&self, upload_id: &str, part: &PartPlan) -> Result<String, SyncError> {
        // Read only this part's slice: peak memory is one part, not one file.
        let body = ByteStream::read_from()
            .path(self.path)
            .offset(part.offset)
            .length(Length::Exact(part.length))
            .build()
            .await
            .map_err(|error| {
                SyncError::storage(format!(
                    "failed to read part {} of upload source '{}': {error}",
                    part.part_number,
                    self.path.display()
                ))
            })?;

        let response = self
            .client
            .upload_part()
            .bucket(self.bucket)
            .key(self.key)
            .upload_id(upload_id)
            .part_number(part.part_number)
            .body(body)
            .send()
            .await
            .map_err(|error| {
                super::s3_adapter::classify_sdk_error(
                    &error,
                    format!(
                        "failed to upload part {} of '{}' to bucket '{}': {error}",
                        part.part_number, self.key, self.bucket
                    ),
                )
            })?;

        response.e_tag().map(str::to_string).ok_or_else(|| {
            SyncError::internal(format!(
                "S3 accepted part {} of '{}' without returning an ETag",
                part.part_number, self.key
            ))
        })
    }

    async fn complete_upload(
        &self,
        upload_id: &str,
        parts: &[(i32, String)],
    ) -> Result<(), SyncError> {
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(
                parts
                    .iter()
                    .map(|(part_number, etag)| {
                        CompletedPart::builder()
                            .part_number(*part_number)
                            .e_tag(etag)
                            .build()
                    })
                    .collect(),
            ))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(self.bucket)
            .key(self.key)
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|error| {
                super::s3_adapter::classify_sdk_error(
                    &error,
                    format!(
                        "failed to finalize multipart upload of '{}' to bucket '{}': {error}",
                        self.key, self.bucket
                    ),
                )
            })?;

        Ok(())
    }

    async fn abort_upload(&self, upload_id: &str) -> Result<(), SyncError> {
        self.client
            .abort_multipart_upload()
            .bucket(self.bucket)
            .key(self.key)
            .upload_id(upload_id)
            .send()
            .await
            .map_err(|error| {
                super::s3_adapter::classify_sdk_error(
                    &error,
                    format!(
                        "failed to abort multipart upload of '{}' in bucket '{}': {error}",
                        self.key, self.bucket
                    ),
                )
            })?;

        Ok(())
    }
}

/// Upload `path` as a multipart object.
pub(crate) async fn upload_file_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
    total_size: u64,
    metadata: Option<HashMap<String, String>>,
) -> Result<(), SyncError> {
    let sink = S3MultipartSink {
        client,
        bucket,
        key,
        path,
        metadata,
    };
    let parts = plan_parts(total_size, TARGET_PART_SIZE_BYTES);
    run_multipart_upload(&sink, &parts).await
}

#[cfg(test)]
mod tests {
    use super::{
        plan_parts, run_multipart_upload, MultipartSink, PartPlan, MAX_PARTS,
        TARGET_PART_SIZE_BYTES,
    };
    use crate::storage::error::SyncError;
    use std::future::Future;
    use std::sync::Mutex;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
    }

    #[test]
    fn parts_cover_every_byte_exactly_once() {
        let total = (TARGET_PART_SIZE_BYTES * 3) + 12_345;
        let parts = plan_parts(total, TARGET_PART_SIZE_BYTES);

        assert_eq!(parts.len(), 4, "three full parts and a remainder");
        assert_eq!(parts.iter().map(|part| part.length).sum::<u64>(), total);

        // Contiguous, in order, 1-based — S3 rejects gaps and duplicates.
        let mut expected_offset = 0;
        for (index, part) in parts.iter().enumerate() {
            assert_eq!(part.part_number, index as i32 + 1);
            assert_eq!(part.offset, expected_offset);
            expected_offset += part.length;
        }
        assert_eq!(parts.last().expect("last part").length, 12_345);
    }

    #[test]
    fn an_exact_multiple_does_not_produce_a_trailing_empty_part() {
        let parts = plan_parts(TARGET_PART_SIZE_BYTES * 2, TARGET_PART_SIZE_BYTES);
        assert_eq!(parts.len(), 2);
        assert!(parts
            .iter()
            .all(|part| part.length == TARGET_PART_SIZE_BYTES));
    }

    #[test]
    fn a_huge_object_grows_its_part_size_to_stay_under_the_part_limit() {
        // 5 TB at the default 16 MiB part size would need ~327k parts, which S3
        // would reject only after every byte had already been uploaded.
        let five_tb = 5_u64 * 1024 * 1024 * 1024 * 1024;
        let parts = plan_parts(five_tb, TARGET_PART_SIZE_BYTES);

        assert!(
            parts.len() as u64 <= MAX_PARTS,
            "planned {} parts, over the S3 limit",
            parts.len()
        );
        assert_eq!(parts.iter().map(|part| part.length).sum::<u64>(), five_tb);
    }

    /// Records the call sequence and fails a chosen step.
    #[derive(Default)]
    struct FakeSink {
        calls: Mutex<Vec<String>>,
        fail_on_part: Option<i32>,
        fail_on_complete: bool,
        fail_on_abort: bool,
    }

    impl FakeSink {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("calls lock").clone()
        }

        fn record(&self, call: String) {
            self.calls.lock().expect("calls lock").push(call);
        }
    }

    impl MultipartSink for FakeSink {
        fn create_upload(&self) -> impl Future<Output = Result<String, SyncError>> + Send {
            self.record("create".into());
            async { Ok("upload-1".to_string()) }
        }

        fn upload_part(
            &self,
            upload_id: &str,
            part: &PartPlan,
        ) -> impl Future<Output = Result<String, SyncError>> + Send {
            self.record(format!(
                "part {} [{}..{}) of {upload_id}",
                part.part_number,
                part.offset,
                part.offset + part.length
            ));
            let failed = self.fail_on_part == Some(part.part_number);
            let etag = format!("etag-{}", part.part_number);
            async move {
                if failed {
                    Err(SyncError::transient("connection reset mid-part"))
                } else {
                    Ok(etag)
                }
            }
        }

        fn complete_upload(
            &self,
            upload_id: &str,
            parts: &[(i32, String)],
        ) -> impl Future<Output = Result<(), SyncError>> + Send {
            let manifest = parts
                .iter()
                .map(|(number, etag)| format!("{number}={etag}"))
                .collect::<Vec<_>>()
                .join(",");
            self.record(format!("complete {upload_id} [{manifest}]"));
            let failed = self.fail_on_complete;
            async move {
                if failed {
                    Err(SyncError::storage("checksum mismatch"))
                } else {
                    Ok(())
                }
            }
        }

        fn abort_upload(
            &self,
            upload_id: &str,
        ) -> impl Future<Output = Result<(), SyncError>> + Send {
            self.record(format!("abort {upload_id}"));
            let failed = self.fail_on_abort;
            async move {
                if failed {
                    Err(SyncError::transient("abort failed too"))
                } else {
                    Ok(())
                }
            }
        }
    }

    #[test]
    fn a_successful_upload_completes_with_every_part_in_order() {
        let sink = FakeSink::default();
        let parts = plan_parts(25, 10);

        runtime()
            .block_on(run_multipart_upload(&sink, &parts))
            .expect("upload should succeed");

        assert_eq!(
            sink.calls(),
            vec![
                "create".to_string(),
                "part 1 [0..10) of upload-1".to_string(),
                "part 2 [10..20) of upload-1".to_string(),
                "part 3 [20..25) of upload-1".to_string(),
                "complete upload-1 [1=etag-1,2=etag-2,3=etag-3]".to_string(),
            ]
        );
    }

    #[test]
    fn a_failed_part_aborts_the_upload_and_reports_the_original_error() {
        let sink = FakeSink {
            fail_on_part: Some(2),
            ..FakeSink::default()
        };
        let parts = plan_parts(25, 10);

        let error = runtime()
            .block_on(run_multipart_upload(&sink, &parts))
            .expect_err("a failed part should fail the upload");

        assert!(
            error.message.contains("connection reset"),
            "cleanup must not mask the real failure, got: {}",
            error.message
        );
        // Part 3 is never attempted, and the abandoned upload is cleaned up.
        assert_eq!(
            sink.calls(),
            vec![
                "create".to_string(),
                "part 1 [0..10) of upload-1".to_string(),
                "part 2 [10..20) of upload-1".to_string(),
                "abort upload-1".to_string(),
            ]
        );
    }

    #[test]
    fn a_failed_completion_also_aborts() {
        let sink = FakeSink {
            fail_on_complete: true,
            ..FakeSink::default()
        };

        let error = runtime()
            .block_on(run_multipart_upload(&sink, &plan_parts(15, 10)))
            .expect_err("a failed completion should fail the upload");

        assert!(error.message.contains("checksum mismatch"));
        assert_eq!(
            sink.calls().last().map(String::as_str),
            Some("abort upload-1"),
            "uploaded parts must not be left behind after a failed completion"
        );
    }

    #[test]
    fn a_failing_abort_does_not_replace_the_error_that_caused_it() {
        let sink = FakeSink {
            fail_on_part: Some(1),
            fail_on_abort: true,
            ..FakeSink::default()
        };

        let error = runtime()
            .block_on(run_multipart_upload(&sink, &plan_parts(15, 10)))
            .expect_err("upload should fail");

        // The user needs to know the upload failed, not that our cleanup did.
        assert!(
            error.message.contains("connection reset"),
            "got: {}",
            error.message
        );
    }
}
