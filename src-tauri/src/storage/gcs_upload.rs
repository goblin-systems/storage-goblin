//! GCS upload paths (backlog phase 2.1).
//!
//! Split out of `gcs_adapter` so the adapter stays within the module-size
//! gate, and because uploading is a distinct concern from listing, auth, and
//! object metadata.
//!
//! Small objects go up in one multipart request. Anything at or above
//! [`RESUMABLE_UPLOAD_THRESHOLD_BYTES`] uses a resumable session instead: the
//! file is streamed in chunks so memory stays bounded regardless of object
//! size, and a failure can resume from the last committed offset rather than
//! restarting at byte zero.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use super::error::SyncError;
use super::gcs_adapter::{encode_component, render_http_error, GcsClient, GcsObjectMetadata};

/// Objects at or above this size upload through a resumable session rather
/// than one multipart request, bounding memory and allowing resume.
pub(crate) const RESUMABLE_UPLOAD_THRESHOLD_BYTES: u64 = 16 * 1024 * 1024;
/// Resumable chunk size. GCS requires a multiple of 256 KiB for every chunk
/// except the last.
pub(crate) const RESUMABLE_CHUNK_SIZE_BYTES: u64 = 8 * 1024 * 1024;

impl GcsClient {
    pub async fn upload_object(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<(), SyncError> {
        let size = std::fs::metadata(path)
            .map_err(|error| {
                SyncError::storage(format!(
                    "failed to inspect upload source '{}': {error}",
                    path.display()
                ))
            })?
            .len();

        // Large objects go through a resumable session so memory stays bounded
        // (the whole file is never held at once) and a failure can resume from
        // the last committed offset rather than byte 0.
        if size >= self.resumable_threshold_bytes {
            return self
                .upload_object_resumable(bucket, key, path, size, metadata)
                .await;
        }

        let body = std::fs::read(path).map_err(|error| {
            SyncError::storage(format!(
                "failed to read upload source '{}': {error}",
                path.display()
            ))
        })?;
        self.upload_object_bytes(bucket, key, body, metadata).await
    }

    /// Upload via the GCS resumable protocol: open a session, then send the
    /// file in chunks, each announced with a `Content-Range`. GCS answers 308
    /// ("resume incomplete") until the final chunk lands.
    async fn upload_object_resumable(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
        total_size: u64,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<(), SyncError> {
        let session_uri = self
            .begin_resumable_upload(bucket, key, total_size, metadata)
            .await?;

        let mut file = File::open(path).map_err(|error| {
            SyncError::storage(format!(
                "failed to open upload source '{}': {error}",
                path.display()
            ))
        })?;

        let mut offset: u64 = 0;
        let mut buffer = vec![0_u8; self.resumable_chunk_size_bytes as usize];

        // A zero-byte object still needs one request to create it.
        loop {
            let read = read_chunk(&mut file, &mut buffer).map_err(|error| {
                SyncError::storage(format!(
                    "failed to read upload source '{}': {error}",
                    path.display()
                ))
            })?;

            let chunk = &buffer[..read];
            let end = offset + read as u64;
            let content_range = if total_size == 0 {
                "bytes */0".to_string()
            } else {
                format!("bytes {}-{}/{}", offset, end.saturating_sub(1), total_size)
            };

            let response = self
                .http
                .put(&session_uri)
                .bearer_auth(&self.token)
                .header(reqwest::header::CONTENT_RANGE, content_range)
                .body(chunk.to_vec())
                .send()
                .await
                .map_err(|error| {
                    SyncError::transient(format!(
                        "failed to upload '{key}' chunk at offset {offset} to GCS bucket '{bucket}': {error}"
                    ))
                })?;

            let status = response.status();
            if status.is_success() {
                return Ok(());
            }
            // 308 Resume Incomplete: GCS accepted this chunk, send the next.
            if status.as_u16() != 308 {
                return Err(render_http_error(
                    response,
                    &format!("upload '{key}' to GCS bucket '{bucket}'"),
                )
                .await);
            }

            offset = end;
            if offset >= total_size {
                // Every byte was accepted but no terminal status arrived.
                return Err(SyncError::internal(format!(
                    "GCS did not finalize the resumable upload of '{key}' after {offset} bytes"
                )));
            }
        }
    }

    /// Open a resumable session and return its URI from the `Location` header.
    async fn begin_resumable_upload(
        &self,
        bucket: &str,
        key: &str,
        total_size: u64,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<String, SyncError> {
        let object_metadata = serde_json::to_string(&GcsObjectMetadata {
            name: key,
            metadata: metadata.unwrap_or_default(),
        })
        .map_err(|error| {
            SyncError::internal(format!("failed to serialize GCS object metadata: {error}"))
        })?;

        let response = self
            .http
            .post(self.storage_api_url(&format!(
                "/upload/storage/v1/b/{}/o?uploadType=resumable&name={}",
                encode_component(bucket),
                encode_component(key)
            )))
            .bearer_auth(&self.token)
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/json; charset=UTF-8",
            )
            .header("X-Upload-Content-Length", total_size)
            .body(object_metadata)
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!(
                    "failed to start resumable upload of '{key}' to GCS bucket '{bucket}': {error}"
                ))
            })?;

        if !response.status().is_success() {
            return Err(render_http_error(
                response,
                &format!("start resumable upload of '{key}' to GCS bucket '{bucket}'"),
            )
            .await);
        }

        response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
            .ok_or_else(|| {
                SyncError::internal(format!(
                    "GCS resumable upload of '{key}' returned no session location"
                ))
            })
    }

    pub async fn upload_object_bytes(
        &self,
        bucket: &str,
        key: &str,
        bytes: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
    ) -> Result<(), SyncError> {
        let boundary = "storage-goblin-gcs-boundary";
        let object_metadata = serde_json::to_string(&GcsObjectMetadata {
            name: key,
            metadata: metadata.unwrap_or_default(),
        })
        .map_err(|error| format!("failed to serialize GCS object metadata: {error}"))?;

        let mut payload = Vec::new();
        payload.extend_from_slice(format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{object_metadata}\r\n").as_bytes());
        payload.extend_from_slice(
            format!("--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").as_bytes(),
        );
        payload.extend_from_slice(&bytes);
        payload.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());

        let response = self
            .http
            .post(self.storage_api_url(&format!(
                "/upload/storage/v1/b/{}/o?uploadType=multipart",
                encode_component(bucket)
            )))
            .bearer_auth(&self.token)
            .header(
                reqwest::header::CONTENT_TYPE,
                format!("multipart/related; boundary={boundary}"),
            )
            .body(payload)
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!(
                    "failed to upload '{key}' to GCS bucket '{bucket}': {error}"
                ))
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(render_http_error(
                response,
                &format!("upload '{key}' to GCS bucket '{bucket}'"),
            )
            .await)
        }
    }
}

/// Fill `buffer` from `file`, tolerating short reads, and return how many
/// bytes were read (0 at EOF).
fn read_chunk(file: &mut File, buffer: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        let read = file.read(&mut buffer[filled..])?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    Ok(filled)
}
