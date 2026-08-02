use std::{fs, path::PathBuf, time::SystemTime};

use tauri::{AppHandle, Manager, Runtime};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

pub mod activity;
#[cfg(test)]
mod architecture_test;
#[cfg(test)]
mod bench_baseline;
pub mod bin_service;
pub mod commands;
pub mod compare_service;
pub mod conflict_service;
pub mod coordinator;
pub mod credential_service;
pub mod credentials_store;
pub mod error;
pub mod file_query_service;
pub mod gcs_adapter;
pub mod gcs_upload;
pub mod inventory_compare;
pub mod lifecycle_service;
pub mod local_index;
pub mod location_service;
pub mod model;
#[cfg(test)]
mod module_size_test;
pub mod object_store;
pub mod pair_backoff;
pub mod platform;
pub mod polling_service;
pub mod profile_store;
pub mod provider;
pub mod queue_schedule;
pub mod queue_service;
pub mod remote_bin;
pub mod remote_index;
pub mod retry;
pub mod s3_adapter;
pub mod s3_upload;
pub mod sanitizer;
#[cfg(test)]
pub(crate) mod sim;
pub mod sync_db;
pub mod sync_planner;
pub mod sync_service;
pub mod sync_state;
pub mod transfer;
pub mod transfer_service;
#[cfg(test)]
mod ts_bindings;
pub mod watcher_service;
pub mod watchers;

pub use sync_state::SyncState;

pub(crate) const PROFILE_FILE_NAME: &str = "storage-goblin-profile.json";
pub(crate) const CREDENTIALS_INDEX_FILE_NAME: &str = "storage-goblin-credentials.json";
pub(crate) const LOCAL_INDEX_FILE_NAME: &str = "storage-goblin-local-index.json";
pub(crate) const REMOTE_INDEX_FILE_NAME: &str = "storage-goblin-remote-index.json";
pub(crate) const SYNC_DB_FILE_NAME: &str = "storage-goblin-sync.sqlite3";

pub(crate) fn now_iso() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".into())
}

pub(crate) fn system_time_to_iso(value: SystemTime) -> Option<String> {
    let value: OffsetDateTime = value.into();
    value.format(&Rfc3339).ok()
}

pub fn default_provider() -> String {
    provider::normalize_provider("aws")
}

pub(crate) fn app_storage_path<R: Runtime>(
    app: &AppHandle<R>,
    file_name: &str,
) -> Result<PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|error| format!("failed to resolve app config dir: {error}"))?;

    fs::create_dir_all(&dir)
        .map_err(|error| format!("failed to create app config dir: {error}"))?;

    Ok(dir.join(file_name))
}
