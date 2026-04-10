use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use tauri::{AppHandle, Runtime};
use uuid::Uuid;

use super::{
    app_storage_path,
    credentials_store::{CredentialSummary, StoredCredentials},
    s3_adapter::S3ConnectionConfig,
    LOCAL_INDEX_FILE_NAME, PROFILE_FILE_NAME, REMOTE_INDEX_FILE_NAME,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionValidationResult {
    pub ok: bool,
    pub message: String,
    pub checked_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionValidationInput {
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub credential_profile_id: Option<String>,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
    pub activity_debug_mode_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SelectedCredentialState {
    pub selected_credential: Option<CredentialSummary>,
    pub selected_credential_available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct StoredProfile {
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
    pub activity_debug_mode_enabled: bool,
    pub credential_profile_id: Option<String>,
    pub selected_credential: Option<CredentialSummary>,
    pub selected_credential_available: bool,
    pub credentials_stored_securely: bool,
    #[serde(default, alias = "syncLocations")]
    pub sync_pairs: Vec<SyncPair>,
    #[serde(default)]
    pub active_location_id: Option<String>,
    #[serde(default)]
    pub flat_fields_migrated: bool,
}

impl Default for StoredProfile {
    fn default() -> Self {
        Self {
            local_folder: String::new(),
            region: String::new(),
            bucket: String::new(),
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
            activity_debug_mode_enabled: false,
            credential_profile_id: None,
            selected_credential: None,
            selected_credential_available: false,
            credentials_stored_securely: false,
            sync_pairs: Vec::new(),
            active_location_id: None,
            flat_fields_migrated: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct PersistedProfile {
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
    pub activity_debug_mode_enabled: bool,
    pub credential_profile_id: Option<String>,
    #[serde(default, alias = "syncLocations")]
    pub sync_pairs: Vec<PersistedSyncPair>,
    #[serde(default)]
    pub active_location_id: Option<String>,
    #[serde(default)]
    pub flat_fields_migrated: bool,
}

impl Default for PersistedProfile {
    fn default() -> Self {
        Self {
            local_folder: String::new(),
            region: String::new(),
            bucket: String::new(),
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
            activity_debug_mode_enabled: false,
            credential_profile_id: None,
            sync_pairs: Vec::new(),
            active_location_id: None,
            flat_fields_migrated: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileDraft {
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    pub credential_profile_id: Option<String>,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
    pub activity_debug_mode_enabled: bool,
}

impl StoredProfile {
    pub fn normalized(mut self) -> Self {
        self.local_folder = self.local_folder.trim().to_string();
        self.region = self.region.trim().to_string();
        self.bucket = self.bucket.trim().to_string();
        self.poll_interval_seconds = self.poll_interval_seconds.clamp(15, 3600);
        self.conflict_strategy = "preserve-both".into();
        self.delete_safety_hours = self.delete_safety_hours.clamp(1, 168);
        self.credential_profile_id = normalize_optional_id(self.credential_profile_id.take());
        if self
            .selected_credential
            .as_ref()
            .map(|summary| summary.id.as_str())
            != self.credential_profile_id.as_deref()
        {
            self.selected_credential = None;
        }
        if let Some(summary) = self.selected_credential.as_mut() {
            summary.name = summary.name.trim().to_string();
            summary.ready = self.selected_credential_available;
        }
        self.credentials_stored_securely = self.selected_credential_available;
        self.sync_pairs = self
            .sync_pairs
            .into_iter()
            .map(SyncPair::normalized)
            .collect();
        self.active_location_id = normalize_optional_id(self.active_location_id.take())
            .filter(|id| self.sync_pairs.iter().any(|pair| pair.id == *id));
        self
    }

    pub fn apply_selected_credential_state(&mut self, state: SelectedCredentialState) {
        self.selected_credential = state.selected_credential;
        self.selected_credential_available = state.selected_credential_available;
        if let Some(summary) = self.selected_credential.as_mut() {
            self.credential_profile_id = Some(summary.id.clone());
            summary.ready = state.selected_credential_available;
        } else {
            self.credential_profile_id = None;
            self.selected_credential_available = false;
        }
        self.credentials_stored_securely = self.selected_credential_available;
    }

    pub fn clear_selected_credential(&mut self) {
        self.credential_profile_id = None;
        self.selected_credential = None;
        self.selected_credential_available = false;
        self.credentials_stored_securely = false;
    }
}

impl ConnectionValidationInput {
    pub fn to_s3_config(&self, credentials: &StoredCredentials) -> S3ConnectionConfig {
        S3ConnectionConfig {
            region: self.region.trim().to_string(),
            bucket: self.bucket.trim().to_string(),
            access_key_id: credentials.access_key_id.trim().to_string(),
            secret_access_key: credentials.secret_access_key.trim().to_string(),
        }
    }
}

impl From<ProfileDraft> for StoredProfile {
    fn from(value: ProfileDraft) -> Self {
        Self {
            local_folder: value.local_folder.trim().to_string(),
            region: value.region.trim().to_string(),
            bucket: value.bucket.trim().to_string(),
            remote_polling_enabled: value.remote_polling_enabled,
            poll_interval_seconds: value.poll_interval_seconds.clamp(15, 3600),
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: value.delete_safety_hours.clamp(1, 168),
            activity_debug_mode_enabled: value.activity_debug_mode_enabled,
            credential_profile_id: normalize_optional_id(value.credential_profile_id),
            selected_credential: None,
            selected_credential_available: false,
            credentials_stored_securely: false,
            sync_pairs: Vec::new(),
            active_location_id: None,
            flat_fields_migrated: false,
        }
        .normalized()
    }
}

impl From<PersistedProfile> for StoredProfile {
    fn from(value: PersistedProfile) -> Self {
        let sync_pairs = value
            .sync_pairs
            .into_iter()
            .map(SyncPair::from)
            .map(|pair| pair.normalized())
            .collect();
        Self {
            local_folder: value.local_folder,
            region: value.region,
            bucket: value.bucket,
            remote_polling_enabled: value.remote_polling_enabled,
            poll_interval_seconds: value.poll_interval_seconds,
            conflict_strategy: value.conflict_strategy,
            delete_safety_hours: value.delete_safety_hours,
            activity_debug_mode_enabled: value.activity_debug_mode_enabled,
            credential_profile_id: normalize_optional_id(value.credential_profile_id),
            selected_credential: None,
            selected_credential_available: false,
            credentials_stored_securely: false,
            sync_pairs,
            active_location_id: value.active_location_id,
            flat_fields_migrated: value.flat_fields_migrated,
        }
        .normalized()
    }
}

impl From<&StoredProfile> for PersistedProfile {
    fn from(value: &StoredProfile) -> Self {
        Self {
            local_folder: value.local_folder.clone(),
            region: value.region.clone(),
            bucket: value.bucket.clone(),
            remote_polling_enabled: value.remote_polling_enabled,
            poll_interval_seconds: value.poll_interval_seconds,
            conflict_strategy: value.conflict_strategy.clone(),
            delete_safety_hours: value.delete_safety_hours,
            activity_debug_mode_enabled: value.activity_debug_mode_enabled,
            credential_profile_id: value.credential_profile_id.clone(),
            sync_pairs: value
                .sync_pairs
                .iter()
                .map(PersistedSyncPair::from)
                .collect(),
            active_location_id: value.active_location_id.clone(),
            flat_fields_migrated: value.flat_fields_migrated,
        }
    }
}

fn normalize_optional_id(value: Option<String>) -> Option<String> {
    value
        .map(|entry| entry.trim().to_string())
        .filter(|entry| !entry.is_empty())
}

fn contains_non_empty_unsupported_field(value: Option<&Value>) -> bool {
    match value {
        Some(Value::String(text)) => !text.trim().is_empty(),
        Some(Value::Null) | None => false,
        Some(_) => true,
    }
}

fn validate_unsupported_legacy_profile_shape(raw: &str) -> Result<(), String> {
    let value: Value =
        serde_json::from_str(raw).map_err(|error| format!("failed to parse profile: {error}"))?;

    if contains_non_empty_unsupported_field(value.get("prefix")) {
        return Err(
            "unsupported persisted profile field 'prefix'; bucket-root only profiles are required."
                .into(),
        );
    }

    if contains_non_empty_unsupported_field(value.get("endpointUrl")) {
        return Err(
            "unsupported persisted profile field 'endpointUrl'; AWS S3 bucket-root only profiles are required."
                .into(),
        );
    }

    for collection_name in ["syncPairs", "syncLocations"] {
        let Some(entries) = value.get(collection_name).and_then(Value::as_array) else {
            continue;
        };

        for entry in entries {
            if contains_non_empty_unsupported_field(entry.get("prefix")) {
                return Err(format!(
                    "unsupported persisted sync location field 'prefix' in '{collection_name}'; bucket-root only profiles are required."
                ));
            }

            if contains_non_empty_unsupported_field(entry.get("endpointUrl")) {
                return Err(format!(
                    "unsupported persisted sync location field 'endpointUrl' in '{collection_name}'; AWS S3 bucket-root only profiles are required."
                ));
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Multi-pair model (transitional until the frontend contract catches up)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPair {
    pub id: String,
    pub label: String,
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    pub credential_profile_id: Option<String>,
    pub enabled: bool,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
}

impl Default for SyncPair {
    fn default() -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            label: String::new(),
            local_folder: String::new(),
            region: String::new(),
            bucket: String::new(),
            credential_profile_id: None,
            enabled: true,
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
        }
    }
}

impl SyncPair {
    pub fn normalized(mut self) -> Self {
        self.id = self.id.trim().to_string();
        if self.id.is_empty() {
            self.id = Uuid::new_v4().to_string();
        }
        self.label = self.label.trim().to_string();
        self.local_folder = self.local_folder.trim().to_string();
        self.region = self.region.trim().to_string();
        self.bucket = self.bucket.trim().to_string();
        self.poll_interval_seconds = self.poll_interval_seconds.clamp(15, 3600);
        self.conflict_strategy = "preserve-both".into();
        self.delete_safety_hours = self.delete_safety_hours.clamp(1, 168);
        self.credential_profile_id = normalize_optional_id(self.credential_profile_id.take());
        self
    }
}

pub fn is_pair_configured(pair: &SyncPair) -> bool {
    !pair.local_folder.is_empty() && !pair.bucket.is_empty()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedSyncPair {
    pub id: String,
    pub label: String,
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    pub credential_profile_id: Option<String>,
    pub enabled: bool,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
}

impl From<&SyncPair> for PersistedSyncPair {
    fn from(value: &SyncPair) -> Self {
        Self {
            id: value.id.clone(),
            label: value.label.clone(),
            local_folder: value.local_folder.clone(),
            region: value.region.clone(),
            bucket: value.bucket.clone(),
            credential_profile_id: value.credential_profile_id.clone(),
            enabled: value.enabled,
            remote_polling_enabled: value.remote_polling_enabled,
            poll_interval_seconds: value.poll_interval_seconds,
            conflict_strategy: value.conflict_strategy.clone(),
            delete_safety_hours: value.delete_safety_hours,
        }
    }
}

impl From<PersistedSyncPair> for SyncPair {
    fn from(value: PersistedSyncPair) -> Self {
        Self {
            id: value.id,
            label: value.label,
            local_folder: value.local_folder,
            region: value.region,
            bucket: value.bucket,
            credential_profile_id: value.credential_profile_id,
            enabled: value.enabled,
            remote_polling_enabled: value.remote_polling_enabled,
            poll_interval_seconds: value.poll_interval_seconds,
            conflict_strategy: value.conflict_strategy,
            delete_safety_hours: value.delete_safety_hours,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPairDraft {
    pub id: Option<String>,
    pub label: String,
    pub local_folder: String,
    pub region: String,
    pub bucket: String,
    pub credential_profile_id: Option<String>,
    pub enabled: bool,
    pub remote_polling_enabled: bool,
    pub poll_interval_seconds: u32,
    pub conflict_strategy: String,
    pub delete_safety_hours: u32,
}

/// Migrate flat profile fields into a `SyncPair` when `sync_pairs` is empty
/// but the legacy flat fields contain a configured connection.
/// Returns `true` if a pair was created, `false` if nothing changed.
fn migrate_flat_fields_to_sync_pairs(profile: &mut StoredProfile) -> bool {
    if profile.flat_fields_migrated {
        return false;
    }
    if !profile.sync_pairs.is_empty() {
        return false;
    }
    if profile.local_folder.trim().is_empty() && profile.bucket.trim().is_empty() {
        return false;
    }
    let label = profile.bucket.clone();
    let pair = SyncPair {
        id: Uuid::new_v4().to_string(),
        label,
        local_folder: profile.local_folder.clone(),
        region: profile.region.clone(),
        bucket: profile.bucket.clone(),
        credential_profile_id: profile.credential_profile_id.clone(),
        enabled: true,
        remote_polling_enabled: profile.remote_polling_enabled,
        poll_interval_seconds: profile.poll_interval_seconds,
        conflict_strategy: profile.conflict_strategy.clone(),
        delete_safety_hours: profile.delete_safety_hours,
    }
    .normalized();
    profile.sync_pairs.push(pair);
    profile.flat_fields_migrated = true;
    true
}

#[cfg(test)]
mod tests {
    use super::{
        is_pair_configured, migrate_flat_fields_to_sync_pairs, PersistedProfile, PersistedSyncPair,
        ProfileDraft, SelectedCredentialState, StoredProfile, SyncPair,
    };
    use crate::storage::credentials_store::CredentialSummary;

    #[test]
    fn normalizes_profile_draft_and_preserves_selected_credential_reference() {
        let stored = StoredProfile::from(ProfileDraft {
            local_folder: "  C:/sync  ".into(),
            region: " us-east-1 ".into(),
            bucket: " demo-bucket ".into(),
            access_key_id: "AKIA123".into(),
            secret_access_key: "secret".into(),
            credential_profile_id: Some(" cred-1 ".into()),
            remote_polling_enabled: false,
            poll_interval_seconds: 1,
            conflict_strategy: "ignored".into(),
            delete_safety_hours: 999,
            activity_debug_mode_enabled: true,
        });

        assert_eq!(stored.local_folder, "C:/sync");
        assert_eq!(stored.bucket, "demo-bucket");
        assert_eq!(stored.poll_interval_seconds, 15);
        assert_eq!(stored.delete_safety_hours, 168);
        assert!(stored.activity_debug_mode_enabled);
        assert_eq!(stored.credential_profile_id.as_deref(), Some("cred-1"));
        assert!(stored.selected_credential.is_none());
        assert!(!stored.selected_credential_available);
        assert!(!stored.credentials_stored_securely);
    }

    #[test]
    fn persisted_profile_round_trips_selected_credential_id() {
        let stored = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo-bucket".into(),
            activity_debug_mode_enabled: true,
            credential_profile_id: Some("cred-1".into()),
            ..StoredProfile::default()
        };

        let persisted = PersistedProfile::from(&stored);
        assert_eq!(persisted.credential_profile_id.as_deref(), Some("cred-1"));
        assert!(persisted.activity_debug_mode_enabled);

        let restored = StoredProfile::from(persisted);
        assert!(restored.activity_debug_mode_enabled);
        assert_eq!(restored.credential_profile_id.as_deref(), Some("cred-1"));
    }

    #[test]
    fn selected_credential_state_populates_summary_and_availability() {
        let mut stored = StoredProfile {
            credential_profile_id: Some("cred-1".into()),
            ..StoredProfile::default()
        };

        stored.apply_selected_credential_state(SelectedCredentialState {
            selected_credential: Some(CredentialSummary {
                id: "cred-1".into(),
                name: "Primary".into(),
                ready: false,
                validation_status: Default::default(),
                last_tested_at: None,
                last_test_message: None,
            }),
            selected_credential_available: true,
        });

        assert_eq!(stored.credential_profile_id.as_deref(), Some("cred-1"));
        assert_eq!(
            stored
                .selected_credential
                .as_ref()
                .map(|entry| entry.name.as_str()),
            Some("Primary")
        );
        assert!(stored.selected_credential_available);
        assert!(stored.credentials_stored_securely);
        assert_eq!(
            stored.selected_credential.as_ref().map(|entry| entry.ready),
            Some(true)
        );

        stored.clear_selected_credential();
        assert!(stored.credential_profile_id.is_none());
        assert!(stored.selected_credential.is_none());
        assert!(!stored.selected_credential_available);
        assert!(!stored.credentials_stored_securely);
    }

    // --- SyncPair tests ---

    #[test]
    fn sync_pair_default_has_correct_values() {
        let pair = SyncPair::default();
        assert!(!pair.id.is_empty(), "default id should be a non-empty UUID");
        assert_eq!(pair.label, "");
        assert_eq!(pair.local_folder, "");
        assert_eq!(pair.region, "");
        assert_eq!(pair.bucket, "");
        assert!(pair.credential_profile_id.is_none());
        assert!(pair.enabled);
        assert!(pair.remote_polling_enabled);
        assert_eq!(pair.poll_interval_seconds, 60);
        assert_eq!(pair.conflict_strategy, "preserve-both");
        assert_eq!(pair.delete_safety_hours, 24);
    }

    #[test]
    fn sync_pair_normalized_trims_and_clamps() {
        let pair = SyncPair {
            id: " abc-123 ".into(),
            label: "  My Pair  ".into(),
            local_folder: "  C:/data  ".into(),
            region: " eu-west-1 ".into(),
            bucket: " my-bucket ".into(),
            credential_profile_id: Some("  cred-1  ".into()),
            enabled: true,
            remote_polling_enabled: false,
            poll_interval_seconds: 5, // below minimum 15
            conflict_strategy: "ignored".into(),
            delete_safety_hours: 500, // above maximum 168
        }
        .normalized();

        assert_eq!(pair.id, "abc-123");
        assert_eq!(pair.label, "My Pair");
        assert_eq!(pair.local_folder, "C:/data");
        assert_eq!(pair.region, "eu-west-1");
        assert_eq!(pair.bucket, "my-bucket");
        assert_eq!(pair.credential_profile_id.as_deref(), Some("cred-1"));
        assert_eq!(pair.poll_interval_seconds, 15);
        assert_eq!(pair.conflict_strategy, "preserve-both");
        assert_eq!(pair.delete_safety_hours, 168);
    }

    #[test]
    fn sync_pair_normalized_assigns_uuid_when_id_is_blank() {
        let pair = SyncPair {
            id: "  ".into(),
            ..SyncPair::default()
        }
        .normalized();
        assert!(!pair.id.is_empty());
    }

    #[test]
    fn sync_pair_normalized_clears_blank_credential_id() {
        let pair = SyncPair {
            credential_profile_id: Some("   ".into()),
            ..SyncPair::default()
        }
        .normalized();
        assert!(pair.credential_profile_id.is_none());
    }

    #[test]
    fn is_pair_configured_requires_local_folder_and_bucket() {
        let empty = SyncPair::default();
        assert!(!is_pair_configured(&empty));

        let folder_only = SyncPair {
            local_folder: "C:/data".into(),
            ..SyncPair::default()
        };
        assert!(!is_pair_configured(&folder_only));

        let bucket_only = SyncPair {
            bucket: "my-bucket".into(),
            ..SyncPair::default()
        };
        assert!(!is_pair_configured(&bucket_only));

        let configured = SyncPair {
            local_folder: "C:/data".into(),
            bucket: "my-bucket".into(),
            ..SyncPair::default()
        };
        assert!(is_pair_configured(&configured));
    }

    #[test]
    fn persisted_sync_pair_round_trips() {
        let original = SyncPair {
            id: "pair-1".into(),
            label: "Test".into(),
            local_folder: "C:/data".into(),
            region: "us-east-1".into(),
            bucket: "my-bucket".into(),
            credential_profile_id: Some("cred-1".into()),
            enabled: false,
            remote_polling_enabled: false,
            poll_interval_seconds: 120,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 48,
        };

        let persisted = PersistedSyncPair::from(&original);
        assert_eq!(persisted.id, "pair-1");
        assert_eq!(persisted.label, "Test");
        assert!(!persisted.enabled);

        let restored = SyncPair::from(persisted);
        assert_eq!(restored.id, "pair-1");
        assert_eq!(restored.bucket, "my-bucket");
        assert_eq!(restored.poll_interval_seconds, 120);
        assert!(!restored.enabled);
    }

    #[test]
    fn migration_creates_pair_from_flat_fields_when_sync_pairs_empty() {
        let mut profile = StoredProfile {
            local_folder: "C:/sync".into(),
            region: "us-east-1".into(),
            bucket: "demo-bucket".into(),
            credential_profile_id: Some("cred-1".into()),
            remote_polling_enabled: false,
            poll_interval_seconds: 120,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 48,
            ..StoredProfile::default()
        };

        let migrated = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(migrated);

        assert_eq!(profile.sync_pairs.len(), 1);
        let pair = &profile.sync_pairs[0];
        assert!(!pair.id.is_empty());
        assert_eq!(pair.label, "demo-bucket");
        assert_eq!(pair.local_folder, "C:/sync");
        assert_eq!(pair.region, "us-east-1");
        assert_eq!(pair.bucket, "demo-bucket");
        assert_eq!(pair.credential_profile_id.as_deref(), Some("cred-1"));
        assert!(pair.enabled);
        assert!(!pair.remote_polling_enabled);
        assert_eq!(pair.poll_interval_seconds, 120);
        assert_eq!(pair.delete_safety_hours, 48);
    }

    #[test]
    fn migration_uses_bucket_as_label() {
        let mut profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "my-bucket".into(),
            ..StoredProfile::default()
        };

        let migrated = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(migrated);

        assert_eq!(profile.sync_pairs.len(), 1);
        assert_eq!(profile.sync_pairs[0].label, "my-bucket");
    }

    #[test]
    fn migration_skips_when_sync_pairs_already_present() {
        let existing_pair = SyncPair {
            local_folder: "C:/existing".into(),
            bucket: "existing-bucket".into(),
            ..SyncPair::default()
        };

        let mut profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo-bucket".into(),
            sync_pairs: vec![existing_pair],
            ..StoredProfile::default()
        };

        let migrated = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(!migrated);

        assert_eq!(profile.sync_pairs.len(), 1);
        assert_eq!(profile.sync_pairs[0].bucket, "existing-bucket");
    }

    #[test]
    fn migration_skips_when_flat_fields_empty() {
        let mut profile = StoredProfile::default();
        let migrated = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(!migrated);
        assert!(profile.sync_pairs.is_empty());
    }

    #[test]
    fn stored_profile_round_trips_sync_pairs_through_persisted() {
        let pair = SyncPair {
            id: "pair-1".into(),
            label: "Test".into(),
            local_folder: "C:/data".into(),
            region: "us-east-1".into(),
            bucket: "my-bucket".into(),
            credential_profile_id: Some("cred-1".into()),
            enabled: true,
            remote_polling_enabled: true,
            poll_interval_seconds: 60,
            conflict_strategy: "preserve-both".into(),
            delete_safety_hours: 24,
        };

        let stored = StoredProfile {
            sync_pairs: vec![pair],
            ..StoredProfile::default()
        };

        let persisted = PersistedProfile::from(&stored);
        assert_eq!(persisted.sync_pairs.len(), 1);
        assert_eq!(persisted.sync_pairs[0].id, "pair-1");

        let restored = StoredProfile::from(persisted);
        assert_eq!(restored.sync_pairs.len(), 1);
        assert_eq!(restored.sync_pairs[0].id, "pair-1");
        assert_eq!(restored.sync_pairs[0].bucket, "my-bucket");
    }

    #[test]
    fn normalized_profile_clears_invalid_active_location_id() {
        let profile = StoredProfile {
            sync_pairs: vec![SyncPair {
                id: "pair-1".into(),
                bucket: "my-bucket".into(),
                local_folder: "C:/data".into(),
                ..SyncPair::default()
            }],
            active_location_id: Some("missing-pair".into()),
            ..StoredProfile::default()
        }
        .normalized();

        assert!(profile.active_location_id.is_none());
    }

    #[test]
    fn normalized_profile_preserves_valid_active_location_id() {
        let profile = StoredProfile {
            sync_pairs: vec![SyncPair {
                id: "pair-1".into(),
                bucket: "my-bucket".into(),
                local_folder: "C:/data".into(),
                ..SyncPair::default()
            }],
            active_location_id: Some(" pair-1 ".into()),
            ..StoredProfile::default()
        }
        .normalized();

        assert_eq!(profile.active_location_id.as_deref(), Some("pair-1"));
    }

    #[test]
    fn migration_returns_false_on_second_call_and_preserves_pair_id() {
        let mut profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo-bucket".into(),
            ..StoredProfile::default()
        };

        let first = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(first, "first migration should return true");
        assert_eq!(profile.sync_pairs.len(), 1);
        assert!(
            profile.flat_fields_migrated,
            "flag should be set after migration"
        );

        let pair_id = profile.sync_pairs[0].id.clone();
        assert!(!pair_id.is_empty(), "pair id should be a non-empty string");

        let second = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(!second, "second migration should return false");
        assert_eq!(profile.sync_pairs.len(), 1);
        assert_eq!(
            profile.sync_pairs[0].id, pair_id,
            "pair id should be stable across calls"
        );
    }

    #[test]
    fn stored_profile_deserializes_sync_locations_alias() {
        let json = r#"{
            "localFolder": "C:/sync",
            "region": "us-east-1",
            "bucket": "demo-bucket",
            "remotePollingEnabled": true,
            "pollIntervalSeconds": 60,
            "conflictStrategy": "preserve-both",
            "deleteSafetyHours": 24,
            "activityDebugModeEnabled": false,
            "syncLocations": [
                {
                    "id": "loc-1",
                    "label": "Test Location",
                    "localFolder": "C:/data",
                    "region": "us-east-1",
                    "bucket": "my-bucket",
                    "credentialProfileId": null,
                    "enabled": true,
                    "remotePollingEnabled": true,
                    "pollIntervalSeconds": 60,
                    "conflictStrategy": "preserve-both",
                    "deleteSafetyHours": 24
                }
            ]
        }"#;

        let profile: StoredProfile =
            serde_json::from_str(json).expect("should deserialize with syncLocations alias");
        assert_eq!(profile.sync_pairs.len(), 1);
        assert_eq!(profile.sync_pairs[0].id, "loc-1");
        assert_eq!(profile.sync_pairs[0].label, "Test Location");
        assert_eq!(profile.sync_pairs[0].bucket, "my-bucket");
    }

    #[test]
    fn stored_profile_deserializes_sync_pairs_field() {
        let json = r#"{
            "localFolder": "C:/sync",
            "region": "us-east-1",
            "bucket": "demo-bucket",
            "remotePollingEnabled": true,
            "pollIntervalSeconds": 60,
            "conflictStrategy": "preserve-both",
            "deleteSafetyHours": 24,
            "activityDebugModeEnabled": false,
            "syncPairs": [
                {
                    "id": "pair-1",
                    "label": "Test Pair",
                    "localFolder": "C:/data",
                    "region": "us-east-1",
                    "bucket": "my-bucket",
                    "credentialProfileId": null,
                    "enabled": true,
                    "remotePollingEnabled": true,
                    "pollIntervalSeconds": 60,
                    "conflictStrategy": "preserve-both",
                    "deleteSafetyHours": 24
                }
            ]
        }"#;

        let profile: StoredProfile =
            serde_json::from_str(json).expect("should deserialize with syncPairs field");
        assert_eq!(profile.sync_pairs.len(), 1);
        assert_eq!(profile.sync_pairs[0].id, "pair-1");
        assert_eq!(profile.sync_pairs[0].label, "Test Pair");
    }

    #[test]
    fn migration_skips_after_flag_is_set_even_with_flat_fields() {
        let mut profile = StoredProfile {
            local_folder: "C:/sync".into(),
            bucket: "demo-bucket".into(),
            flat_fields_migrated: true,
            ..StoredProfile::default()
        };

        let migrated = migrate_flat_fields_to_sync_pairs(&mut profile);
        assert!(!migrated, "should not migrate when flag is already set");
        assert!(
            profile.sync_pairs.is_empty(),
            "sync_pairs should remain empty"
        );
    }
}

#[cfg(all(test, feature = "tauri-command-tests"))]
mod hard_break_persisted_profile_loading_tests {
    use super::read_profile_from_disk;
    use crate::storage::{app_storage_path, PROFILE_FILE_NAME};
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use std::process;
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tauri::test::{mock_builder, mock_context, noop_assets, MockRuntime};
    use tauri::{App, AppHandle};

    struct ProfileStoreTestHarness {
        _env_lock: MutexGuard<'static, ()>,
        original_appdata: Option<String>,
        original_localappdata: Option<String>,
        app: App<MockRuntime>,
        storage_dir: PathBuf,
    }

    impl ProfileStoreTestHarness {
        fn new(name: &str) -> Self {
            let env_lock = env_lock().lock().expect("env lock should not be poisoned");
            let storage_dir = unique_test_dir(name);
            fs::create_dir_all(&storage_dir).expect("test storage dir should exist");

            let original_appdata = env::var("APPDATA").ok();
            let original_localappdata = env::var("LOCALAPPDATA").ok();
            env::set_var("APPDATA", &storage_dir);
            env::set_var("LOCALAPPDATA", &storage_dir);

            let app = mock_builder()
                .build(mock_context(noop_assets()))
                .expect("mock app should build");

            Self {
                _env_lock: env_lock,
                original_appdata,
                original_localappdata,
                app,
                storage_dir,
            }
        }

        fn app_handle(&self) -> AppHandle<MockRuntime> {
            self.app.handle().clone()
        }

        fn write_raw_profile(&self, raw: &str) {
            let path = app_storage_path(&self.app_handle(), PROFILE_FILE_NAME)
                .expect("profile path should resolve");
            fs::write(path, raw).expect("raw profile should write");
        }
    }

    impl Drop for ProfileStoreTestHarness {
        fn drop(&mut self) {
            restore_env_var("APPDATA", self.original_appdata.as_deref());
            restore_env_var("LOCALAPPDATA", self.original_localappdata.as_deref());
            let _ = fs::remove_dir_all(&self.storage_dir);
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn restore_env_var(key: &str, value: Option<&str>) {
        match value {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("current time should be after epoch")
            .as_nanos();
        env::temp_dir().join(format!(
            "storage-goblin-profile-store-{name}-{}-{suffix}",
            process::id()
        ))
    }

    fn assert_explicit_unsupported_field_error(error: &str, field_name: &str) {
        assert!(
            error.contains("unsupported"),
            "expected explicit unsupported-field error, got: {error}"
        );
        assert!(
            error.contains(field_name),
            "expected error to mention '{field_name}', got: {error}"
        );
    }

    #[test]
    fn hard_break_loading_persisted_profile_with_non_empty_top_level_prefix_fails_explicitly() {
        let harness = ProfileStoreTestHarness::new("reject-top-level-prefix");
        harness.write_raw_profile(
            r#"{
                "localFolder": "C:/sync",
                "region": "us-east-1",
                "bucket": "demo-bucket",
                "prefix": "legacy-prefix"
            }"#,
        );

        let error = read_profile_from_disk(&harness.app_handle())
            .expect_err("legacy top-level prefix should be rejected");

        assert_explicit_unsupported_field_error(&error, "prefix");
    }

    #[test]
    fn hard_break_loading_persisted_profile_with_non_empty_top_level_endpoint_fails_explicitly() {
        let harness = ProfileStoreTestHarness::new("reject-top-level-endpoint");
        harness.write_raw_profile(
            r#"{
                "localFolder": "C:/sync",
                "region": "us-east-1",
                "bucket": "demo-bucket",
                "endpointUrl": "https://s3.example.test"
            }"#,
        );

        let error = read_profile_from_disk(&harness.app_handle())
            .expect_err("legacy top-level endpointUrl should be rejected");

        assert_explicit_unsupported_field_error(&error, "endpointUrl");
    }

    #[test]
    fn hard_break_loading_persisted_profile_with_sync_location_prefix_fails_explicitly() {
        let harness = ProfileStoreTestHarness::new("reject-sync-location-prefix");
        harness.write_raw_profile(
            r#"{
                "syncPairs": [
                    {
                        "id": "pair-1",
                        "label": "Docs",
                        "localFolder": "C:/docs",
                        "region": "us-east-1",
                        "bucket": "demo-bucket",
                        "prefix": "legacy-prefix",
                        "credentialProfileId": null,
                        "enabled": true,
                        "remotePollingEnabled": true,
                        "pollIntervalSeconds": 60,
                        "conflictStrategy": "preserve-both",
                        "deleteSafetyHours": 24
                    }
                ]
            }"#,
        );

        let error = read_profile_from_disk(&harness.app_handle())
            .expect_err("legacy sync location prefix should be rejected");

        assert_explicit_unsupported_field_error(&error, "prefix");
    }

    #[test]
    fn hard_break_loading_persisted_profile_with_sync_location_endpoint_fails_explicitly() {
        let harness = ProfileStoreTestHarness::new("reject-sync-location-endpoint");
        harness.write_raw_profile(
            r#"{
                "syncPairs": [
                    {
                        "id": "pair-1",
                        "label": "Docs",
                        "localFolder": "C:/docs",
                        "endpointUrl": "https://s3.example.test",
                        "region": "us-east-1",
                        "bucket": "demo-bucket",
                        "credentialProfileId": null,
                        "enabled": true,
                        "remotePollingEnabled": true,
                        "pollIntervalSeconds": 60,
                        "conflictStrategy": "preserve-both",
                        "deleteSafetyHours": 24
                    }
                ]
            }"#,
        );

        let error = read_profile_from_disk(&harness.app_handle())
            .expect_err("legacy sync location endpointUrl should be rejected");

        assert_explicit_unsupported_field_error(&error, "endpointUrl");
    }

    #[test]
    fn hard_break_loading_canonical_bucket_root_profile_without_prefix_or_endpoint_succeeds() {
        let harness = ProfileStoreTestHarness::new("load-canonical-bucket-root-profile");
        harness.write_raw_profile(
            r#"{
                "syncPairs": [
                    {
                        "id": "pair-1",
                        "label": "Bucket Root",
                        "localFolder": "C:/bucket-root",
                        "region": "us-east-1",
                        "bucket": "demo-bucket",
                        "credentialProfileId": null,
                        "enabled": true,
                        "remotePollingEnabled": true,
                        "pollIntervalSeconds": 60,
                        "conflictStrategy": "preserve-both",
                        "deleteSafetyHours": 24
                    }
                ],
                "activeLocationId": "pair-1"
            }"#,
        );

        let profile = read_profile_from_disk(&harness.app_handle())
            .expect("canonical bucket-root profile should load");

        assert_eq!(profile.sync_pairs.len(), 1);
        assert_eq!(profile.active_location_id.as_deref(), Some("pair-1"));

        let pair = &profile.sync_pairs[0];
        assert_eq!(pair.id, "pair-1");
        assert_eq!(pair.label, "Bucket Root");
        assert_eq!(pair.local_folder, "C:/bucket-root");
        assert_eq!(pair.bucket, "demo-bucket");
        assert_eq!(pair.region, "us-east-1");
    }

    #[test]
    fn hard_break_loading_missing_profile_file_still_returns_default_profile() {
        let harness = ProfileStoreTestHarness::new("missing-profile-file-default");

        let profile = read_profile_from_disk(&harness.app_handle())
            .expect("missing profile should still load as default");

        assert_eq!(profile.local_folder, "");
        assert_eq!(profile.bucket, "");
        assert!(profile.sync_pairs.is_empty());
        assert!(profile.active_location_id.is_none());
    }
}

pub fn is_profile_configured(profile: &StoredProfile) -> bool {
    !profile.local_folder.is_empty() && !profile.bucket.is_empty()
}

pub fn read_profile_from_disk<R: Runtime>(app: &AppHandle<R>) -> Result<StoredProfile, String> {
    let path = app_storage_path(app, PROFILE_FILE_NAME)?;
    if !path.exists() {
        return Ok(StoredProfile::default());
    }

    let raw =
        fs::read_to_string(&path).map_err(|error| format!("failed to read profile: {error}"))?;
    validate_unsupported_legacy_profile_shape(&raw)?;
    let persisted: PersistedProfile =
        serde_json::from_str(&raw).map_err(|error| format!("failed to parse profile: {error}"))?;
    let mut profile = StoredProfile::from(persisted);

    if migrate_flat_fields_to_sync_pairs(&mut profile) {
        write_profile_to_disk(app, &profile)?;

        let pair_id = &profile.sync_pairs[0].id;

        let local_src = app_storage_path(app, LOCAL_INDEX_FILE_NAME)?;
        let local_dst =
            app_storage_path(app, &format!("storage-goblin-local-index-{pair_id}.json"))?;
        if local_src.exists() {
            let _ = std::fs::rename(&local_src, &local_dst);
        }

        let remote_src = app_storage_path(app, REMOTE_INDEX_FILE_NAME)?;
        let remote_dst =
            app_storage_path(app, &format!("storage-goblin-remote-index-{pair_id}.json"))?;
        if remote_src.exists() {
            let _ = std::fs::rename(&remote_src, &remote_dst);
        }
    }

    Ok(profile)
}

pub fn write_profile_to_disk<R: Runtime>(
    app: &AppHandle<R>,
    profile: &StoredProfile,
) -> Result<(), String> {
    let path = app_storage_path(app, PROFILE_FILE_NAME)?;
    let normalized = profile.clone().normalized();
    let raw = serde_json::to_string_pretty(&PersistedProfile::from(&normalized))
        .map_err(|error| format!("failed to serialize profile: {error}"))?;
    fs::write(path, raw).map_err(|error| format!("failed to write profile: {error}"))
}
