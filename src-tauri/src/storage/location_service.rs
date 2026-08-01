//! Creating, updating, removing, and reconfiguring sync locations.
//!
//! Each mutation reconciles the provider side (object versioning, remote-bin
//! lifecycle rules) before persisting the profile, so the durable state never
//! claims a configuration the bucket does not actually have.

use tauri::{AppHandle, Runtime};
use uuid::Uuid;

use super::commands::{resolve_credentials_for_pair, storage_config_for_pair};
use super::credential_service::{
    provider_supports_runtime_object_versioning, sync_location_runtime_object_versioning_message,
};
use super::lifecycle_service::persist_profile_with_remote_bin_reconciliation;
use super::object_store;
use super::profile_store::{
    read_profile_from_disk, write_profile_to_disk, StoredProfile, SyncPair, SyncPairDraft,
};
#[cfg(not(test))]
use super::sync_service::start_polling_worker;

pub(crate) async fn apply_sync_location_versioning<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
    enabled: bool,
) -> Result<(), String> {
    let credentials = resolve_credentials_for_pair(app, pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(pair, &credentials)).await?;
    object_store::set_bucket_versioning(&client, &pair.bucket, enabled)
        .await
        .map_err(String::from)
}

pub(crate) async fn reconcile_pair_object_versioning<R: Runtime>(
    app: &AppHandle<R>,
    pair: &SyncPair,
) -> Result<(), String> {
    let credentials = resolve_credentials_for_pair(app, pair)?;
    if !provider_supports_runtime_object_versioning(&pair.provider) {
        return Err(sync_location_runtime_object_versioning_message(pair));
    }
    let client = object_store::build_client(&storage_config_for_pair(pair, &credentials)).await?;
    let currently_enabled = object_store::bucket_versioning_enabled(&client, &pair.bucket).await?;

    if pair.object_versioning_enabled && !currently_enabled {
        object_store::set_bucket_versioning(&client, &pair.bucket, true).await?;
    } else if !pair.object_versioning_enabled && currently_enabled {
        object_store::set_bucket_versioning(&client, &pair.bucket, false).await?;
    }

    Ok(())
}

#[cfg(not(test))]
pub(crate) async fn add_sync_pair_impl(
    app: AppHandle,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    reconcile_pair_object_versioning(&app, &pair).await?;
    next.sync_pairs.push(pair);
    let next =
        persist_profile_with_remote_bin_reconciliation(&app, &current, next.normalized()).await?;
    let _ = start_polling_worker(&app);
    Ok(next)
}

#[cfg(test)]
pub(crate) async fn add_sync_pair_impl<R: Runtime>(
    app: AppHandle<R>,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    reconcile_pair_object_versioning(&app, &pair).await?;
    next.sync_pairs.push(pair);
    persist_profile_with_remote_bin_reconciliation(&app, &current, next.normalized()).await
}

#[tauri::command]
pub async fn add_sync_location(
    app: AppHandle,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    add_sync_pair_impl(app, draft).await
}

#[cfg(not(test))]
pub(crate) async fn update_sync_pair_impl(
    app: AppHandle,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    let pair_id = draft
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or("Sync pair ID is required for updates.")?;
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let position = next
        .sync_pairs
        .iter()
        .position(|p| p.id == pair_id)
        .ok_or_else(|| format!("Sync pair '{}' not found.", pair_id))?;
    let updated = SyncPair {
        id: pair_id.to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    reconcile_pair_object_versioning(&app, &updated).await?;
    next.sync_pairs[position] = updated;
    let next =
        persist_profile_with_remote_bin_reconciliation(&app, &current, next.normalized()).await?;
    let _ = start_polling_worker(&app);
    Ok(next)
}

#[cfg(test)]
pub(crate) async fn update_sync_pair_impl<R: Runtime>(
    app: AppHandle<R>,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    let pair_id = draft
        .id
        .as_deref()
        .filter(|id| !id.trim().is_empty())
        .ok_or("Sync pair ID is required for updates.")?;
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let position = next
        .sync_pairs
        .iter()
        .position(|p| p.id == pair_id)
        .ok_or_else(|| format!("Sync pair '{}' not found.", pair_id))?;
    let updated = SyncPair {
        id: pair_id.to_string(),
        label: draft.label,
        provider: draft.provider,
        local_folder: draft.local_folder,
        region: draft.region,
        bucket: draft.bucket,
        credential_profile_id: draft.credential_profile_id,
        object_versioning_enabled: draft.object_versioning_enabled,
        enabled: draft.enabled,
        remote_polling_enabled: draft.remote_polling_enabled,
        poll_interval_seconds: draft.poll_interval_seconds,
        conflict_strategy: draft.conflict_strategy,
        remote_bin: draft.remote_bin,
    }
    .normalized();
    reconcile_pair_object_versioning(&app, &updated).await?;
    next.sync_pairs[position] = updated;
    persist_profile_with_remote_bin_reconciliation(&app, &current, next.normalized()).await
}

#[tauri::command]
pub async fn update_sync_location(
    app: AppHandle,
    draft: SyncPairDraft,
) -> Result<StoredProfile, String> {
    update_sync_pair_impl(app, draft).await
}

#[tauri::command]
pub async fn set_sync_location_versioning(
    app: AppHandle,
    location_id: String,
    enabled: bool,
) -> Result<StoredProfile, String> {
    let location_id = location_id.trim();
    if location_id.is_empty() {
        return Err("Sync location ID is required.".into());
    }
    let mut profile = read_profile_from_disk(&app)?;
    let position = profile
        .sync_pairs
        .iter()
        .position(|p| p.id == location_id)
        .ok_or_else(|| format!("Sync location '{}' not found.", location_id))?;
    apply_sync_location_versioning(&app, &profile.sync_pairs[position], enabled).await?;
    profile.sync_pairs[position].object_versioning_enabled = enabled;
    write_profile_to_disk(&app, &profile)?;
    Ok(profile)
}

#[cfg(not(test))]
pub(crate) async fn remove_sync_pair_impl(
    app: AppHandle,
    pair_id: String,
) -> Result<StoredProfile, String> {
    let pair_id = pair_id.trim();
    if pair_id.is_empty() {
        return Err("Sync pair ID is required.".into());
    }
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let original_len = next.sync_pairs.len();
    next.sync_pairs.retain(|p| p.id != pair_id);
    if next.sync_pairs.len() == original_len {
        return Err(format!("Sync pair '{}' not found.", pair_id));
    }
    if next.active_location_id.as_deref() == Some(pair_id) {
        next.active_location_id = None;
    }
    let next =
        persist_profile_with_remote_bin_reconciliation(&app, &current, next.normalized()).await?;
    let _ = start_polling_worker(&app);
    Ok(next)
}

#[cfg(test)]
pub(crate) async fn remove_sync_pair_impl<R: Runtime>(
    app: AppHandle<R>,
    pair_id: String,
) -> Result<StoredProfile, String> {
    let pair_id = pair_id.trim();
    if pair_id.is_empty() {
        return Err("Sync pair ID is required.".into());
    }
    let current = read_profile_from_disk(&app)?;
    let mut next = current.clone();
    let original_len = next.sync_pairs.len();
    next.sync_pairs.retain(|p| p.id != pair_id);
    if next.sync_pairs.len() == original_len {
        return Err(format!("Sync pair '{}' not found.", pair_id));
    }
    if next.active_location_id.as_deref() == Some(pair_id) {
        next.active_location_id = None;
    }
    persist_profile_with_remote_bin_reconciliation(&app, &current, next.normalized()).await
}

#[tauri::command]
pub async fn remove_sync_location(
    app: AppHandle,
    location_id: String,
) -> Result<StoredProfile, String> {
    remove_sync_pair_impl(app, location_id).await
}

pub(crate) fn sync_pair_for_location(
    profile: &StoredProfile,
    location_id: &str,
) -> Result<SyncPair, String> {
    profile
        .sync_pairs
        .iter()
        .find(|pair| pair.id == location_id)
        .cloned()
        .ok_or_else(|| format!("Sync pair '{location_id}' not found."))
}
