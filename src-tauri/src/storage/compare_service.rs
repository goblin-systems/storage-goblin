//! Preparing local-vs-remote and version-vs-version comparisons for the UI.
//!
//! Small text and image payloads are inlined for display; anything larger (or
//! of an unrecognised type) is written to temp files and handed to the OS to
//! open externally.

use std::fs;
use std::path::{Path, PathBuf};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine as _;
use tauri::{AppHandle, Runtime};
use uuid::Uuid;

use super::app_storage_path;
use super::commands::{ConflictResolutionDetails, VersionComparisonDetails};

pub(crate) fn temp_compare_file_path<R: Runtime>(
    app: &AppHandle<R>,
    relative_path: &str,
) -> Result<PathBuf, String> {
    let extension = Path::new(relative_path)
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!(".{value}"))
        .unwrap_or_default();
    let file_name = format!(
        "storage-goblin-conflict-compare-{}{}",
        Uuid::new_v4(),
        extension
    );
    app_storage_path(app, &file_name)
}

pub(crate) fn compare_mode_external(
    location_id: String,
    path: String,
    local_path: Option<String>,
    remote_temp_path: Option<String>,
    fallback_reason: Option<String>,
) -> ConflictResolutionDetails {
    ConflictResolutionDetails {
        location_id,
        path,
        mode: "external".into(),
        local_path,
        remote_temp_path,
        local_text: None,
        remote_text: None,
        local_image_data_url: None,
        remote_image_data_url: None,
        fallback_reason,
    }
}

pub(crate) fn image_media_type_for_extension(path: &str) -> Option<&'static str> {
    let extension = Path::new(path)
        .extension()
        .and_then(|value| value.to_str())?
        .trim()
        .to_ascii_lowercase();

    match extension.as_str() {
        "png" => Some("image/png"),
        "jpg" | "jpeg" => Some("image/jpeg"),
        "gif" => Some("image/gif"),
        "webp" => Some("image/webp"),
        "bmp" => Some("image/bmp"),
        "svg" => Some("image/svg+xml"),
        "avif" => Some("image/avif"),
        _ => None,
    }
}

pub(crate) fn is_probably_text_bytes(bytes: &[u8]) -> bool {
    if bytes.is_empty() {
        return true;
    }

    if bytes.contains(&0) {
        return false;
    }

    std::str::from_utf8(bytes).is_ok()
}

pub(crate) fn read_file_with_size_limit(path: &Path, max_bytes: usize) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|error| {
        format!(
            "Failed to inspect compare file '{}': {error}",
            path.display()
        )
    })?;

    let file_size = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    if file_size > max_bytes {
        return Err(format!(
            "File '{}' is too large for inline compare ({} bytes > {} byte limit).",
            path.display(),
            metadata.len(),
            max_bytes
        ));
    }

    fs::read(path)
        .map_err(|error| format!("Failed to read compare file '{}': {error}", path.display()))
}

pub(crate) fn try_prepare_inline_image_details(
    location_id: String,
    path: String,
    local_path: Option<String>,
    remote_temp_path: Option<String>,
) -> Result<ConflictResolutionDetails, String> {
    let media_type = image_media_type_for_extension(&path).ok_or_else(|| {
        "Unsupported inline image type; using external compare instead.".to_string()
    })?;
    let local_path_ref = local_path
        .as_deref()
        .ok_or_else(|| "Local file is unavailable for inline image compare.".to_string())?;
    let remote_path_ref = remote_temp_path
        .as_deref()
        .ok_or_else(|| "Remote file is unavailable for inline image compare.".to_string())?;

    let local_bytes =
        read_file_with_size_limit(Path::new(local_path_ref), INLINE_IMAGE_COMPARE_MAX_BYTES)?;
    let remote_bytes =
        read_file_with_size_limit(Path::new(remote_path_ref), INLINE_IMAGE_COMPARE_MAX_BYTES)?;

    Ok(ConflictResolutionDetails {
        location_id,
        path,
        mode: "image".into(),
        local_path,
        remote_temp_path,
        local_text: None,
        remote_text: None,
        local_image_data_url: Some(format!(
            "data:{media_type};base64,{}",
            BASE64_STANDARD.encode(local_bytes)
        )),
        remote_image_data_url: Some(format!(
            "data:{media_type};base64,{}",
            BASE64_STANDARD.encode(remote_bytes)
        )),
        fallback_reason: None,
    })
}

pub(crate) fn try_prepare_inline_text_details(
    location_id: String,
    path: String,
    local_path: Option<String>,
    remote_temp_path: Option<String>,
) -> Result<ConflictResolutionDetails, String> {
    let local_path_ref = local_path
        .as_deref()
        .ok_or_else(|| "Local file is unavailable for inline text compare.".to_string())?;
    let remote_path_ref = remote_temp_path
        .as_deref()
        .ok_or_else(|| "Remote file is unavailable for inline text compare.".to_string())?;

    let local_bytes =
        read_file_with_size_limit(Path::new(local_path_ref), INLINE_TEXT_COMPARE_MAX_BYTES)?;
    let remote_bytes =
        read_file_with_size_limit(Path::new(remote_path_ref), INLINE_TEXT_COMPARE_MAX_BYTES)?;

    if !is_probably_text_bytes(&local_bytes) || !is_probably_text_bytes(&remote_bytes) {
        return Err("One or both files look binary, so inline text compare is unavailable.".into());
    }

    let local_text = String::from_utf8(local_bytes).map_err(|_| {
        "Local file is not valid UTF-8, so inline text compare is unavailable.".to_string()
    })?;
    let remote_text = String::from_utf8(remote_bytes).map_err(|_| {
        "Remote file is not valid UTF-8, so inline text compare is unavailable.".to_string()
    })?;

    Ok(ConflictResolutionDetails {
        location_id,
        path,
        mode: "text".into(),
        local_path,
        remote_temp_path,
        local_text: Some(local_text),
        remote_text: Some(remote_text),
        local_image_data_url: None,
        remote_image_data_url: None,
        fallback_reason: None,
    })
}

pub(crate) fn finalize_conflict_compare_details(
    location_id: String,
    path: String,
    local_path: Option<String>,
    remote_temp_path: Option<String>,
) -> ConflictResolutionDetails {
    if image_media_type_for_extension(&path).is_some() {
        return match try_prepare_inline_image_details(
            location_id.clone(),
            path.clone(),
            local_path.clone(),
            remote_temp_path.clone(),
        ) {
            Ok(details) => details,
            Err(error) => {
                compare_mode_external(location_id, path, local_path, remote_temp_path, Some(error))
            }
        };
    }

    match try_prepare_inline_text_details(
        location_id.clone(),
        path.clone(),
        local_path.clone(),
        remote_temp_path.clone(),
    ) {
        Ok(details) => details,
        Err(error) => {
            compare_mode_external(location_id, path, local_path, remote_temp_path, Some(error))
        }
    }
}

pub(crate) fn temp_version_compare_file_path<R: Runtime>(
    app: &AppHandle<R>,
    relative_path: &str,
    label: &str,
) -> Result<PathBuf, String> {
    let extension = Path::new(relative_path)
        .extension()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .map(|value| format!(".{value}"))
        .unwrap_or_default();
    let file_name = format!(
        "storage-goblin-version-compare-{}-{}{}",
        Uuid::new_v4(),
        label,
        extension
    );
    app_storage_path(app, &file_name)
}

pub(crate) fn finalize_version_compare_details(
    path: String,
    version_a_id: String,
    version_b_id: String,
    path_a: Option<String>,
    path_b: Option<String>,
) -> VersionComparisonDetails {
    let media_type = image_media_type_for_extension(&path);

    if media_type.is_some() {
        let result = (|| -> Result<(String, String), String> {
            let a = path_a
                .as_deref()
                .ok_or("Version A file is unavailable for inline image compare.")?;
            let b = path_b
                .as_deref()
                .ok_or("Version B file is unavailable for inline image compare.")?;
            let a_bytes = read_file_with_size_limit(Path::new(a), INLINE_IMAGE_COMPARE_MAX_BYTES)?;
            let b_bytes = read_file_with_size_limit(Path::new(b), INLINE_IMAGE_COMPARE_MAX_BYTES)?;
            let mt = media_type.unwrap();
            Ok((
                format!("data:{mt};base64,{}", BASE64_STANDARD.encode(a_bytes)),
                format!("data:{mt};base64,{}", BASE64_STANDARD.encode(b_bytes)),
            ))
        })();

        return match result {
            Ok((a_url, b_url)) => VersionComparisonDetails {
                path,
                mode: "image".into(),
                version_a_id,
                version_b_id,
                version_a_temp_path: path_a,
                version_b_temp_path: path_b,
                version_a_text: None,
                version_b_text: None,
                version_a_image_data_url: Some(a_url),
                version_b_image_data_url: Some(b_url),
                fallback_reason: None,
            },
            Err(reason) => VersionComparisonDetails {
                path,
                mode: "external".into(),
                version_a_id,
                version_b_id,
                version_a_temp_path: path_a,
                version_b_temp_path: path_b,
                version_a_text: None,
                version_b_text: None,
                version_a_image_data_url: None,
                version_b_image_data_url: None,
                fallback_reason: Some(reason),
            },
        };
    }

    let result = (|| -> Result<(String, String), String> {
        let a = path_a
            .as_deref()
            .ok_or("Version A file is unavailable for inline text compare.")?;
        let b = path_b
            .as_deref()
            .ok_or("Version B file is unavailable for inline text compare.")?;
        let a_bytes = read_file_with_size_limit(Path::new(a), INLINE_TEXT_COMPARE_MAX_BYTES)?;
        let b_bytes = read_file_with_size_limit(Path::new(b), INLINE_TEXT_COMPARE_MAX_BYTES)?;
        if !is_probably_text_bytes(&a_bytes) || !is_probably_text_bytes(&b_bytes) {
            return Err(
                "One or both versions look binary, so inline text compare is unavailable.".into(),
            );
        }
        let a_text =
            String::from_utf8(a_bytes).map_err(|_| "Version A is not valid UTF-8.".to_string())?;
        let b_text =
            String::from_utf8(b_bytes).map_err(|_| "Version B is not valid UTF-8.".to_string())?;
        Ok((a_text, b_text))
    })();

    match result {
        Ok((a_text, b_text)) => VersionComparisonDetails {
            path,
            mode: "text".into(),
            version_a_id,
            version_b_id,
            version_a_temp_path: path_a,
            version_b_temp_path: path_b,
            version_a_text: Some(a_text),
            version_b_text: Some(b_text),
            version_a_image_data_url: None,
            version_b_image_data_url: None,
            fallback_reason: None,
        },
        Err(reason) => VersionComparisonDetails {
            path,
            mode: "external".into(),
            version_a_id,
            version_b_id,
            version_a_temp_path: path_a,
            version_b_temp_path: path_b,
            version_a_text: None,
            version_b_text: None,
            version_a_image_data_url: None,
            version_b_image_data_url: None,
            fallback_reason: Some(reason),
        },
    }
}

pub(crate) const INLINE_TEXT_COMPARE_MAX_BYTES: usize = 128 * 1024;

pub(crate) const INLINE_IMAGE_COMPARE_MAX_BYTES: usize = 5 * 1024 * 1024;
