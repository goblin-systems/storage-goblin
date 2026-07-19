use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(not(test))]
use std::time::Duration;

#[cfg(not(test))]
use keyring::{Entry, Error as KeyringError};

#[cfg(test)]
use std::{
    collections::HashMap,
    sync::{Mutex, OnceLock},
};

use tauri::{AppHandle, Runtime};

use super::{
    app_storage_path, default_provider,
    gcs_adapter::compact_service_account_json,
    now_iso,
    provider::{normalize_provider, CredentialKind, GCS_PROVIDER},
    sanitizer::sanitize_sensitive_text,
    CREDENTIALS_INDEX_FILE_NAME,
};

const LEGACY_SERVICE_NAME: &str = "storage-goblin.sync-profile";
const LEGACY_USER_NAME: &str = "active-profile";
const CREDENTIALS_SERVICE_NAME: &str = "storage-goblin.credentials";
const DPAPI_SECRET_DIR_NAME: &str = "credential-secrets";
const SECURE_STORE_VERIFY_ATTEMPTS: usize = 4;
#[cfg(not(test))]
const SECURE_STORE_VERIFY_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretVerificationOutcome {
    Verified,
    Missing,
    Mismatched,
}

static CREDENTIAL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
enum CredentialSecretStorage {
    #[default]
    SecureStore,
    DpapiFile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredSecretState {
    storage: CredentialSecretStorage,
    credentials: StoredCredentials,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCredentials {
    pub provider: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub service_account_json: Option<String>,
    pub secret: CredentialSecret,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CredentialSecret {
    AwsAccessKey {
        access_key_id: String,
        secret_access_key: String,
    },
    GcsServiceAccount {
        service_account_json: String,
    },
}

impl CredentialSecret {
    fn into_aws_access_key_id(self) -> Option<String> {
        match self {
            CredentialSecret::AwsAccessKey { access_key_id, .. } => Some(access_key_id),
            CredentialSecret::GcsServiceAccount { .. } => None,
        }
    }

    fn into_aws_secret_access_key(self) -> Option<String> {
        match self {
            CredentialSecret::AwsAccessKey {
                secret_access_key, ..
            } => Some(secret_access_key),
            CredentialSecret::GcsServiceAccount { .. } => None,
        }
    }

    fn into_gcs_service_account_json(self) -> Option<String> {
        match self {
            CredentialSecret::AwsAccessKey { .. } => None,
            CredentialSecret::GcsServiceAccount {
                service_account_json,
            } => Some(service_account_json),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CredentialPayloadInput {
    AwsAccessKey {
        #[serde(rename = "accessKeyId", alias = "access_key_id")]
        access_key_id: String,
        #[serde(rename = "secretAccessKey", alias = "secret_access_key")]
        secret_access_key: String,
    },
    GcsServiceAccount {
        #[serde(rename = "serviceAccountJson", alias = "service_account_json")]
        service_account_json: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialInputState {
    Blank,
    Provided(StoredCredentials),
}

impl StoredCredentials {
    #[allow(dead_code)]
    pub fn kind(&self) -> CredentialKind {
        match self.secret {
            CredentialSecret::AwsAccessKey { .. } => CredentialKind::AwsAccessKey,
            CredentialSecret::GcsServiceAccount { .. } => CredentialKind::GcsServiceAccount,
        }
    }

    pub fn aws_access_key(provider: &str, access_key_id: &str, secret_access_key: &str) -> Self {
        Self {
            provider: normalize_provider(provider),
            access_key_id: access_key_id.trim().to_string(),
            secret_access_key: secret_access_key.trim().to_string(),
            service_account_json: None,
            secret: CredentialSecret::AwsAccessKey {
                access_key_id: access_key_id.trim().to_string(),
                secret_access_key: secret_access_key.trim().to_string(),
            },
        }
    }

    pub fn gcs_service_account(provider: &str, service_account_json: &str) -> Result<Self, String> {
        let service_account_json = compact_service_account_json(service_account_json)?;

        Ok(Self {
            provider: normalize_provider(provider),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            service_account_json: Some(service_account_json.clone()),
            secret: CredentialSecret::GcsServiceAccount {
                service_account_json,
            },
        })
    }

    #[allow(dead_code)]
    pub fn aws_access_key_id(&self) -> Option<&str> {
        match &self.secret {
            CredentialSecret::AwsAccessKey { access_key_id, .. } => Some(access_key_id.as_str()),
            CredentialSecret::GcsServiceAccount { .. } => None,
        }
    }

    #[allow(dead_code)]
    pub fn aws_secret_access_key(&self) -> Option<&str> {
        match &self.secret {
            CredentialSecret::AwsAccessKey {
                secret_access_key, ..
            } => Some(secret_access_key.as_str()),
            CredentialSecret::GcsServiceAccount { .. } => None,
        }
    }

    pub fn gcs_service_account_json(&self) -> Option<&str> {
        match &self.secret {
            CredentialSecret::AwsAccessKey { .. } => None,
            CredentialSecret::GcsServiceAccount {
                service_account_json,
            } => Some(service_account_json.as_str()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum CredentialValidationStatus {
    #[default]
    Untested,
    Passed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AwsCredentialSummaryDetails {
    pub access_key_id_preview: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GcsCredentialSummaryDetails {
    pub client_email: Option<String>,
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CredentialSummaryDetails {
    Aws(AwsCredentialSummaryDetails),
    Gcs(GcsCredentialSummaryDetails),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CredentialSummary {
    pub id: String,
    pub name: String,
    #[serde(default = "default_provider")]
    pub provider: String,
    pub ready: bool,
    pub validation_status: CredentialValidationStatus,
    pub last_tested_at: Option<String>,
    pub last_test_message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<CredentialSummaryDetails>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialDraft {
    pub name: String,
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(default)]
    pub credential: Option<CredentialPayloadInput>,
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    #[serde(default)]
    pub service_account_json: String,
}

#[derive(Debug, Clone, Serialize)]
struct CredentialSecretPayload {
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(flatten)]
    pub secret: CredentialSecret,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyAwsAccessKeySecret {
    access_key_id: String,
    secret_access_key: String,
}

#[derive(Debug, Clone, Deserialize)]
struct LegacyGcsServiceAccountSecret {
    service_account_json: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum CredentialSecretCompat {
    Tagged(CredentialSecret),
    LegacyAwsAccessKey(LegacyAwsAccessKeySecret),
    LegacyGcsServiceAccount(LegacyGcsServiceAccountSecret),
}

impl From<CredentialSecretCompat> for CredentialSecret {
    fn from(value: CredentialSecretCompat) -> Self {
        match value {
            CredentialSecretCompat::Tagged(secret) => secret,
            CredentialSecretCompat::LegacyAwsAccessKey(secret) => CredentialSecret::AwsAccessKey {
                access_key_id: secret.access_key_id,
                secret_access_key: secret.secret_access_key,
            },
            CredentialSecretCompat::LegacyGcsServiceAccount(secret) => {
                CredentialSecret::GcsServiceAccount {
                    service_account_json: secret.service_account_json,
                }
            }
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct CredentialSecretPayloadCompat {
    #[serde(default = "default_provider")]
    pub provider: String,
    #[serde(flatten)]
    pub secret: CredentialSecretCompat,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
struct CredentialMetadataIndex {
    version: u32,
    credentials: Vec<CredentialMetadata>,
}

impl Default for CredentialMetadataIndex {
    fn default() -> Self {
        Self {
            version: 1,
            credentials: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CredentialMetadata {
    id: String,
    name: String,
    #[serde(default = "default_provider")]
    provider: String,
    #[serde(default)]
    secret_storage: CredentialSecretStorage,
    created_at: String,
    updated_at: String,
    #[serde(default)]
    validation_status: CredentialValidationStatus,
    #[serde(default)]
    last_tested_at: Option<String>,
    #[serde(default)]
    last_test_message: Option<String>,
}

#[cfg(test)]
pub fn parse_credential_input(
    provider: &str,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<CredentialInputState, String> {
    parse_credential_input_with_payload(provider, None, access_key_id, secret_access_key, "")
}

pub fn parse_credential_input_with_payload(
    provider: &str,
    credential: Option<&CredentialPayloadInput>,
    access_key_id: &str,
    secret_access_key: &str,
    service_account_json: &str,
) -> Result<CredentialInputState, String> {
    let provider = normalize_provider(provider);
    let access_key_id = access_key_id.trim().to_string();
    let secret_access_key = secret_access_key.trim().to_string();
    let service_account_json = service_account_json.trim().to_string();

    if let Some(credential) = credential {
        return match credential {
            CredentialPayloadInput::AwsAccessKey {
                access_key_id,
                secret_access_key,
            } => {
                let access_key_id = access_key_id.trim();
                let secret_access_key = secret_access_key.trim();
                if access_key_id.is_empty() && secret_access_key.is_empty() {
                    Ok(CredentialInputState::Blank)
                } else if access_key_id.is_empty() || secret_access_key.is_empty() {
                    Err("Provide both access key ID and secret access key, or leave both blank to keep existing secure credentials.".into())
                } else {
                    Ok(CredentialInputState::Provided(
                        StoredCredentials::aws_access_key(
                            &provider,
                            access_key_id,
                            secret_access_key,
                        ),
                    ))
                }
            }
            CredentialPayloadInput::GcsServiceAccount {
                service_account_json,
            } => {
                let service_account_json = service_account_json.trim();
                if service_account_json.is_empty() {
                    Ok(CredentialInputState::Blank)
                } else {
                    Ok(CredentialInputState::Provided(
                        StoredCredentials::gcs_service_account(&provider, service_account_json)?,
                    ))
                }
            }
        };
    }

    if provider == "gcs" && !service_account_json.is_empty() {
        return Ok(CredentialInputState::Provided(
            StoredCredentials::gcs_service_account(&provider, &service_account_json)?,
        ));
    }

    match (access_key_id.is_empty(), secret_access_key.is_empty()) {
        (true, true) => Ok(CredentialInputState::Blank),
        (false, false) => Ok(CredentialInputState::Provided(StoredCredentials::aws_access_key(
            &provider,
            &access_key_id,
            &secret_access_key,
        ))),
        _ => Err(
            "Provide both access key ID and secret access key, or leave both blank to keep existing secure credentials."
                .into(),
        ),
    }
}

pub fn list_credentials<R: Runtime>(app: &AppHandle<R>) -> Result<Vec<CredentialSummary>, String> {
    let path = credentials_index_path(app)?;
    list_credentials_from_path(&path)
}

pub fn create_credential<R: Runtime>(
    app: &AppHandle<R>,
    draft: CredentialDraft,
) -> Result<CredentialSummary, String> {
    let path = credentials_index_path(app)?;
    create_credential_at_path(&path, draft)
}

pub fn upsert_credential<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: Option<&str>,
    name: &str,
    provider: &str,
    credentials: &StoredCredentials,
) -> Result<CredentialSummary, String> {
    let path = credentials_index_path(app)?;
    upsert_credential_at_path(&path, credential_id, name, provider, credentials)
}

pub fn delete_credential<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: &str,
) -> Result<bool, String> {
    let path = credentials_index_path(app)?;
    delete_credential_at_path(&path, credential_id)
}

pub fn get_credential_summary<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: &str,
) -> Result<Option<CredentialSummary>, String> {
    let path = credentials_index_path(app)?;
    get_credential_summary_from_path(&path, credential_id)
}

pub fn load_credentials_by_id<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: &str,
) -> Result<Option<StoredCredentials>, String> {
    let path = credentials_index_path(app)?;
    load_credentials_by_id_from_path(&path, credential_id)
}

pub fn ensure_legacy_credentials_migrated<R: Runtime>(
    app: &AppHandle<R>,
    preferred_name: Option<&str>,
) -> Result<Option<CredentialSummary>, String> {
    let path = credentials_index_path(app)?;
    ensure_legacy_credentials_migrated_at_path(&path, preferred_name)
}

pub fn record_credential_validation<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: &str,
    status: CredentialValidationStatus,
    checked_at: &str,
    message: Option<&str>,
) -> Result<Option<CredentialSummary>, String> {
    let path = credentials_index_path(app)?;
    record_credential_validation_at_path(path.as_path(), credential_id, status, checked_at, message)
}

fn credentials_index_path<R: Runtime>(app: &AppHandle<R>) -> Result<PathBuf, String> {
    app_storage_path(app, CREDENTIALS_INDEX_FILE_NAME)
}

fn list_credentials_from_path(path: &Path) -> Result<Vec<CredentialSummary>, String> {
    let secret_dir = secret_storage_dir(path);
    let mut summaries = read_index(path)?
        .credentials
        .into_iter()
        .map(|metadata| {
            let ready = secret_exists(&secret_dir, &metadata.id, metadata.secret_storage)?;
            let summary =
                load_credential_summary_details(&secret_dir, &metadata.id, metadata.secret_storage);
            Ok(metadata.to_summary(ready, summary))
        })
        .collect::<Result<Vec<_>, String>>()?;

    summaries.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    Ok(summaries)
}

fn create_credential_at_path(
    path: &Path,
    draft: CredentialDraft,
) -> Result<CredentialSummary, String> {
    let name = normalize_credential_name(&draft.name)?;
    let credentials = match parse_credential_input_with_payload(
        &draft.provider,
        draft.credential.as_ref(),
        &draft.access_key_id,
        &draft.secret_access_key,
        &draft.service_account_json,
    )? {
        CredentialInputState::Provided(credentials) => credentials,
        CredentialInputState::Blank => {
            return Err("Provide credentials when creating a credential.".into())
        }
    };

    upsert_credential_at_path(path, None, &name, &draft.provider, &credentials)
}

fn upsert_credential_at_path(
    path: &Path,
    credential_id: Option<&str>,
    name: &str,
    provider: &str,
    credentials: &StoredCredentials,
) -> Result<CredentialSummary, String> {
    let name = normalize_credential_name(name)?;
    let provider = normalize_provider(provider);
    let mut index = read_index(path)?;
    let secret_dir = secret_storage_dir(path);
    let now = now_iso();
    let id = credential_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(generate_credential_id);
    let existing_metadata = index
        .credentials
        .iter()
        .find(|entry| entry.id == id)
        .cloned();
    let is_existing = existing_metadata.is_some();
    let previous_secret = if let Some(metadata) = existing_metadata.as_ref() {
        load_secret_state(&secret_dir, &metadata.id, metadata.secret_storage)?
    } else {
        None
    };
    let secret_storage = preferred_secret_storage(&provider, &credentials.secret);
    let payload = CredentialSecretPayload {
        provider: provider.clone(),
        secret: credentials.secret.clone(),
    };

    let ready = store_secret_and_verify_round_trip(
        &secret_dir,
        &id,
        secret_storage,
        &payload,
        previous_secret.as_ref(),
    )?;

    if let Some(existing) = index.credentials.iter_mut().find(|entry| entry.id == id) {
        existing.name = name.clone();
        existing.provider = provider;
        existing.secret_storage = secret_storage;
        existing.updated_at = now;
        existing.validation_status = CredentialValidationStatus::Untested;
        existing.last_tested_at = None;
        existing.last_test_message = None;
    } else {
        index.credentials.push(CredentialMetadata {
            id: id.clone(),
            name: name.clone(),
            provider,
            secret_storage,
            created_at: now.clone(),
            updated_at: now,
            validation_status: CredentialValidationStatus::Untested,
            last_tested_at: None,
            last_test_message: None,
        });
    }

    if let Err(error) = write_index(path, &index) {
        if let Some(previous_secret) = previous_secret.as_ref() {
            let rollback_payload = CredentialSecretPayload {
                provider: previous_secret.credentials.provider.clone(),
                secret: previous_secret.credentials.secret.clone(),
            };
            let _ = store_secret_and_verify_round_trip(
                &secret_dir,
                &id,
                previous_secret.storage,
                &rollback_payload,
                None,
            );
            if previous_secret.storage != secret_storage {
                let _ = delete_secret(&secret_dir, &id, secret_storage);
            }
        } else if !is_existing {
            let _ = delete_secret(&secret_dir, &id, secret_storage);
        }
        return Err(error);
    }

    Ok(CredentialSummary {
        id,
        name,
        provider: payload.provider,
        ready,
        validation_status: CredentialValidationStatus::Untested,
        last_tested_at: None,
        last_test_message: None,
        summary: credential_summary_details_from_credentials(credentials),
    })
}

fn delete_credential_at_path(path: &Path, credential_id: &str) -> Result<bool, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Ok(false);
    }

    let mut index = read_index(path)?;
    let secret_dir = secret_storage_dir(path);
    let removed_metadata = index
        .credentials
        .iter()
        .position(|entry| entry.id == credential_id)
        .map(|position| index.credentials.remove(position));
    let removed = removed_metadata.is_some();

    if removed {
        write_index(path, &index)?;
    }

    if let Some(metadata) = removed_metadata {
        delete_secret(&secret_dir, credential_id, metadata.secret_storage)?;
    } else {
        delete_secret(
            &secret_dir,
            credential_id,
            CredentialSecretStorage::SecureStore,
        )?;
    }
    Ok(removed)
}

fn get_credential_summary_from_path(
    path: &Path,
    credential_id: &str,
) -> Result<Option<CredentialSummary>, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Ok(None);
    }

    let index = read_index(path)?;
    let secret_dir = secret_storage_dir(path);
    let Some(metadata) = index
        .credentials
        .into_iter()
        .find(|entry| entry.id == credential_id)
    else {
        return Ok(None);
    };

    let ready = secret_exists(&secret_dir, credential_id, metadata.secret_storage)?;
    let summary =
        load_credential_summary_details(&secret_dir, credential_id, metadata.secret_storage);
    Ok(Some(metadata.to_summary(ready, summary)))
}

fn record_credential_validation_at_path(
    path: &Path,
    credential_id: &str,
    status: CredentialValidationStatus,
    checked_at: &str,
    message: Option<&str>,
) -> Result<Option<CredentialSummary>, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Ok(None);
    }

    let mut index = read_index(path)?;
    let secret_dir = secret_storage_dir(path);
    let Some(metadata) = index
        .credentials
        .iter_mut()
        .find(|entry| entry.id == credential_id)
    else {
        return Ok(None);
    };

    metadata.validation_status = status;
    metadata.last_tested_at = normalize_optional_text(checked_at);
    metadata.last_test_message =
        normalize_optional_text_from_option(message).map(sanitize_sensitive_text);
    metadata.updated_at = now_iso();

    write_index(path, &index)?;
    let metadata = index
        .credentials
        .iter()
        .find(|entry| entry.id == credential_id)
        .ok_or_else(|| "credential metadata disappeared during validation update".to_string())?;
    let ready = secret_exists(&secret_dir, credential_id, metadata.secret_storage)?;
    let summary =
        load_credential_summary_details(&secret_dir, credential_id, metadata.secret_storage);
    Ok(Some(metadata.to_summary(ready, summary)))
}

fn load_credentials_by_id_from_path(
    path: &Path,
    credential_id: &str,
) -> Result<Option<StoredCredentials>, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Ok(None);
    }

    let index = read_index(path)?;
    let secret_dir = secret_storage_dir(path);
    let Some(metadata) = index
        .credentials
        .into_iter()
        .find(|entry| entry.id == credential_id)
    else {
        return Ok(None);
    };

    Ok(
        load_secret(&secret_dir, credential_id, metadata.secret_storage)?
            .map(|state| state.credentials),
    )
}

fn ensure_legacy_credentials_migrated_at_path(
    path: &Path,
    preferred_name: Option<&str>,
) -> Result<Option<CredentialSummary>, String> {
    let existing = read_index(path)?;
    if !existing.credentials.is_empty() {
        return Ok(None);
    }

    let Some(legacy_credentials) = load_legacy_credentials()? else {
        return Ok(None);
    };

    let summary = upsert_credential_at_path(
        path,
        None,
        preferred_name.unwrap_or("Migrated credential"),
        &legacy_credentials.provider,
        &legacy_credentials,
    )?;
    delete_legacy_credentials()?;
    Ok(Some(summary))
}

fn normalize_credential_name(value: &str) -> Result<String, String> {
    let name = value.trim();
    if name.is_empty() {
        Err("Provide a credential name.".into())
    } else {
        Ok(name.to_string())
    }
}

fn generate_credential_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_nanos())
        .unwrap_or_default();
    let counter = CREDENTIAL_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("cred-{nanos:x}-{counter:x}")
}

fn read_index(path: &Path) -> Result<CredentialMetadataIndex, String> {
    if !path.exists() {
        return Ok(CredentialMetadataIndex::default());
    }

    let raw = fs::read_to_string(path).map_err(|error| {
        format!(
            "failed to read credential index '{}': {error}",
            path.display()
        )
    })?;
    let mut index: CredentialMetadataIndex = serde_json::from_str(&raw).map_err(|error| {
        format!(
            "failed to parse credential index '{}': {error}",
            path.display()
        )
    })?;
    index
        .credentials
        .sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));
    Ok(index)
}

fn write_index(path: &Path, index: &CredentialMetadataIndex) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create credential index directory '{}': {error}",
                parent.display()
            )
        })?;
    }

    let raw = serde_json::to_string_pretty(index)
        .map_err(|error| format!("failed to serialize credential index: {error}"))?;
    fs::write(path, raw).map_err(|error| {
        format!(
            "failed to write credential index '{}': {error}",
            path.display()
        )
    })
}

fn parse_secret_payload(raw: &str, error_prefix: &str) -> Result<CredentialSecretPayload, String> {
    let payload: CredentialSecretPayloadCompat =
        serde_json::from_str(raw).map_err(|error| format!("{error_prefix}: {error}"))?;

    Ok(CredentialSecretPayload {
        provider: payload.provider,
        secret: payload.secret.into(),
    })
}

fn load_legacy_credentials() -> Result<Option<StoredCredentials>, String> {
    let raw = match get_secret_raw(LEGACY_SERVICE_NAME, LEGACY_USER_NAME) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(None),
        Err(error) => return Err(format!("failed to load legacy secure credentials: {error}")),
    };

    let payload = parse_secret_payload(&raw, "failed to parse legacy secure credentials payload")?;

    Ok(Some(StoredCredentials {
        provider: normalize_provider(&payload.provider),
        access_key_id: payload
            .secret
            .clone()
            .into_aws_access_key_id()
            .unwrap_or_default(),
        secret_access_key: payload
            .secret
            .clone()
            .into_aws_secret_access_key()
            .unwrap_or_default(),
        service_account_json: payload.secret.clone().into_gcs_service_account_json(),
        secret: payload.secret,
    }))
}

fn delete_legacy_credentials() -> Result<(), String> {
    delete_secret_raw(LEGACY_SERVICE_NAME, LEGACY_USER_NAME)
        .map_err(|error| format!("failed to remove migrated legacy credentials: {error}"))
}

fn secret_storage_dir(index_path: &Path) -> PathBuf {
    let parent = index_path.parent().unwrap_or_else(|| Path::new("."));
    let digest = Sha256::digest(index_path.to_string_lossy().as_bytes());
    parent
        .join(DPAPI_SECRET_DIR_NAME)
        .join(hex_encode(digest.as_slice()))
}

fn preferred_secret_storage(provider: &str, secret: &CredentialSecret) -> CredentialSecretStorage {
    if normalize_provider(provider) == GCS_PROVIDER
        && matches!(secret, CredentialSecret::GcsServiceAccount { .. })
        && windows_dpapi_file_backend_enabled()
    {
        CredentialSecretStorage::DpapiFile
    } else {
        CredentialSecretStorage::SecureStore
    }
}

fn windows_dpapi_file_backend_enabled() -> bool {
    #[cfg(test)]
    if let Ok(behavior) = current_mock_keyring_behavior() {
        if let Some(enabled) = behavior.prefer_dpapi_file_for_gcs {
            return enabled;
        }
    }

    cfg!(windows)
}

fn stored_credentials_from_payload(payload: &CredentialSecretPayload) -> StoredCredentials {
    StoredCredentials {
        provider: normalize_provider(&payload.provider),
        access_key_id: payload
            .secret
            .clone()
            .into_aws_access_key_id()
            .unwrap_or_default(),
        secret_access_key: payload
            .secret
            .clone()
            .into_aws_secret_access_key()
            .unwrap_or_default(),
        service_account_json: payload.secret.clone().into_gcs_service_account_json(),
        secret: payload.secret.clone(),
    }
}

fn credential_summary_details_from_credentials(
    credentials: &StoredCredentials,
) -> Option<CredentialSummaryDetails> {
    match &credentials.secret {
        CredentialSecret::AwsAccessKey { access_key_id, .. } => {
            Some(CredentialSummaryDetails::Aws(AwsCredentialSummaryDetails {
                access_key_id_preview: mask_access_key_id_preview(access_key_id),
            }))
        }
        CredentialSecret::GcsServiceAccount {
            service_account_json,
        } => {
            let summary = gcs_summary_details_from_service_account_json(service_account_json);
            if summary.client_email.is_none() && summary.project_id.is_none() {
                None
            } else {
                Some(CredentialSummaryDetails::Gcs(summary))
            }
        }
    }
}

fn load_credential_summary_details(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
) -> Option<CredentialSummaryDetails> {
    load_secret_state(secret_dir, credential_id, storage)
        .ok()
        .flatten()
        .and_then(|state| credential_summary_details_from_credentials(&state.credentials))
}

fn mask_access_key_id_preview(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let suffix = trimmed
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();

    Some(format!("****{suffix}"))
}

fn gcs_summary_details_from_service_account_json(value: &str) -> GcsCredentialSummaryDetails {
    let parsed = serde_json::from_str::<serde_json::Value>(value).ok();
    let object = parsed.as_ref().and_then(serde_json::Value::as_object);

    GcsCredentialSummaryDetails {
        client_email: object
            .and_then(|entry| entry.get("client_email"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ToOwned::to_owned),
        project_id: object
            .and_then(|entry| entry.get("project_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|entry| !entry.is_empty())
            .map(ToOwned::to_owned),
    }
}

fn store_secret_and_verify_round_trip(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
    payload: &CredentialSecretPayload,
    previous_secret: Option<&StoredSecretState>,
) -> Result<bool, String> {
    store_secret(secret_dir, credential_id, storage, payload)?;

    let expected = stored_credentials_from_payload(payload);

    let verification = verify_secret_round_trip(secret_dir, credential_id, storage, &expected)?;
    if verification == SecretVerificationOutcome::Verified {
        return Ok(true);
    }

    if verification == SecretVerificationOutcome::Missing
        && storage == CredentialSecretStorage::SecureStore
        && secure_store_write_accepts_delayed_visibility()
    {
        report_nonblocking_secure_store_visibility_delay(credential_id);
        return Ok(false);
    }

    rollback_after_failed_secret_verification(
        secret_dir,
        credential_id,
        storage,
        previous_secret,
        verification.failure_message(),
    )?;

    unreachable!("failed secure-store verification should always return an error")
}

fn verify_secret_round_trip(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
    expected: &StoredCredentials,
) -> Result<SecretVerificationOutcome, String> {
    let mut saw_mismatch = false;

    for attempt in 0..SECURE_STORE_VERIFY_ATTEMPTS {
        match load_secret(secret_dir, credential_id, storage)? {
            Some(stored) if stored.credentials == *expected => {
                return Ok(SecretVerificationOutcome::Verified)
            }
            Some(_) => saw_mismatch = true,
            None => {}
        }

        if attempt + 1 < SECURE_STORE_VERIFY_ATTEMPTS {
            pause_before_secure_store_verification_retry();
        }
    }

    Ok(if saw_mismatch {
        SecretVerificationOutcome::Mismatched
    } else {
        SecretVerificationOutcome::Missing
    })
}

impl SecretVerificationOutcome {
    fn failure_message(self) -> &'static str {
        match self {
            SecretVerificationOutcome::Verified => {
                "secure credential storage verification unexpectedly succeeded"
            }
            SecretVerificationOutcome::Missing => {
                "secure credential storage verification failed after write"
            }
            SecretVerificationOutcome::Mismatched => {
                "failed to verify secure credential storage after write"
            }
        }
    }
}

fn rollback_after_failed_secret_verification(
    secret_dir: &Path,
    credential_id: &str,
    attempted_storage: CredentialSecretStorage,
    previous_secret: Option<&StoredSecretState>,
    error: &str,
) -> Result<(), String> {
    if let Some(previous_secret) = previous_secret {
        let rollback_payload = CredentialSecretPayload {
            provider: previous_secret.credentials.provider.clone(),
            secret: previous_secret.credentials.secret.clone(),
        };

        store_secret(
            secret_dir,
            credential_id,
            previous_secret.storage,
            &rollback_payload,
        )
        .and_then(|_| {
            verify_secret_round_trip(
                secret_dir,
                credential_id,
                previous_secret.storage,
                &previous_secret.credentials,
            )
            .and_then(|outcome| {
                if outcome == SecretVerificationOutcome::Verified {
                    Ok(())
                } else {
                    Err(outcome.failure_message().to_string())
                }
            })
        })
        .and_then(|_| {
            if previous_secret.storage != attempted_storage {
                delete_secret(secret_dir, credential_id, attempted_storage)
            } else {
                Ok(())
            }
        })
        .map_err(|restore_error| {
            format!("{error}. Failed to restore previous secure credentials: {restore_error}")
        })?;

        return Err(error.into());
    }

    delete_secret(secret_dir, credential_id, attempted_storage).map_err(|cleanup_error| {
        format!("{error}. Failed to clean up unverified secure credentials: {cleanup_error}")
    })?;

    Err(error.into())
}

fn secure_store_write_accepts_delayed_visibility() -> bool {
    #[cfg(test)]
    if let Ok(behavior) = current_mock_keyring_behavior() {
        if let Some(accepts_delayed_visibility) = behavior.accepts_delayed_visibility {
            return accepts_delayed_visibility;
        }
    }

    cfg!(windows)
}

#[cfg(not(test))]
fn report_nonblocking_secure_store_visibility_delay(credential_id: &str) {
    eprintln!(
        "[WARN] secure credential '{credential_id}' was written but not immediately visible; continuing because Windows secure storage can delay read-after-write consistency"
    );
}

#[cfg(test)]
fn report_nonblocking_secure_store_visibility_delay(_credential_id: &str) {}

#[cfg(not(test))]
fn pause_before_secure_store_verification_retry() {
    std::thread::sleep(SECURE_STORE_VERIFY_RETRY_DELAY);
}

#[cfg(test)]
fn pause_before_secure_store_verification_retry() {}

fn store_secret(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
    payload: &CredentialSecretPayload,
) -> Result<(), String> {
    let raw = serde_json::to_string(payload)
        .map_err(|error| format!("failed to serialize secure credential payload: {error}"))?;

    match storage {
        CredentialSecretStorage::SecureStore => {
            set_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id, &raw)
                .map_err(|error| format!("failed to store secure credentials: {error}"))
        }
        CredentialSecretStorage::DpapiFile => {
            store_dpapi_file_secret(secret_dir, credential_id, &raw)
                .map_err(|error| format!("failed to store DPAPI credential file: {error}"))
        }
    }
}

fn load_secret(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
) -> Result<Option<StoredSecretState>, String> {
    let raw = match storage {
        CredentialSecretStorage::SecureStore => {
            match get_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id) {
                Ok(Some(raw)) => raw,
                Ok(None) => return Ok(None),
                Err(error) => return Err(format!("failed to load secure credentials: {error}")),
            }
        }
        CredentialSecretStorage::DpapiFile => {
            match load_dpapi_file_secret(secret_dir, credential_id) {
                Ok(Some(raw)) => raw,
                Ok(None) => return Ok(None),
                Err(error) => return Err(format!("failed to load DPAPI credential file: {error}")),
            }
        }
    };

    let payload = parse_secret_payload(&raw, "failed to parse secure credential payload")?;
    Ok(Some(StoredSecretState {
        storage,
        credentials: stored_credentials_from_payload(&payload),
    }))
}

fn load_secret_state(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
) -> Result<Option<StoredSecretState>, String> {
    load_secret(secret_dir, credential_id, storage)
}

fn secret_exists(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
) -> Result<bool, String> {
    match storage {
        CredentialSecretStorage::SecureStore => {
            match get_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id) {
                Ok(Some(_)) => Ok(true),
                Ok(None) => Ok(false),
                Err(error) => Err(format!("failed to inspect secure credentials: {error}")),
            }
        }
        CredentialSecretStorage::DpapiFile => dpapi_file_secret_exists(secret_dir, credential_id)
            .map_err(|error| format!("failed to inspect DPAPI credential file: {error}")),
    }
}

fn delete_secret(
    secret_dir: &Path,
    credential_id: &str,
    storage: CredentialSecretStorage,
) -> Result<(), String> {
    match storage {
        CredentialSecretStorage::SecureStore => {
            delete_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id)
                .map_err(|error| format!("failed to delete secure credentials: {error}"))
        }
        CredentialSecretStorage::DpapiFile => delete_dpapi_file_secret(secret_dir, credential_id)
            .map_err(|error| format!("failed to delete DPAPI credential file: {error}")),
    }
}

fn normalize_optional_text(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_optional_text_from_option(value: Option<&str>) -> Option<String> {
    value.and_then(normalize_optional_text)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn dpapi_secret_file_path(secret_dir: &Path, credential_id: &str) -> PathBuf {
    let digest = Sha256::digest(credential_id.as_bytes());
    secret_dir.join(format!("{}.bin", hex_encode(digest.as_slice())))
}

fn store_dpapi_file_secret(
    secret_dir: &Path,
    credential_id: &str,
    raw: &str,
) -> Result<(), String> {
    fs::create_dir_all(secret_dir).map_err(|error| {
        format!(
            "failed to create DPAPI credential directory '{}': {error}",
            secret_dir.display()
        )
    })?;

    let encrypted = dpapi_protect_bytes(raw.as_bytes())?;
    let path = dpapi_secret_file_path(secret_dir, credential_id);
    fs::write(&path, encrypted).map_err(|error| {
        format!(
            "failed to write DPAPI credential file '{}': {error}",
            path.display()
        )
    })
}

fn load_dpapi_file_secret(
    secret_dir: &Path,
    credential_id: &str,
) -> Result<Option<String>, String> {
    let path = dpapi_secret_file_path(secret_dir, credential_id);
    if !path.exists() {
        return Ok(None);
    }

    let encrypted = fs::read(&path).map_err(|error| {
        format!(
            "failed to read DPAPI credential file '{}': {error}",
            path.display()
        )
    })?;
    let decrypted = dpapi_unprotect_bytes(&encrypted)?;
    String::from_utf8(decrypted).map(Some).map_err(|error| {
        format!(
            "failed to decode DPAPI credential file '{}': {error}",
            path.display()
        )
    })
}

fn dpapi_file_secret_exists(secret_dir: &Path, credential_id: &str) -> Result<bool, String> {
    let path = dpapi_secret_file_path(secret_dir, credential_id);
    match fs::metadata(&path) {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(format!(
            "failed to inspect DPAPI credential file '{}': {error}",
            path.display()
        )),
    }
}

fn delete_dpapi_file_secret(secret_dir: &Path, credential_id: &str) -> Result<(), String> {
    let path = dpapi_secret_file_path(secret_dir, credential_id);
    match fs::remove_file(&path) {
        Ok(()) => {
            cleanup_empty_secret_dir(secret_dir);
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "failed to remove DPAPI credential file '{}': {error}",
            path.display()
        )),
    }
}

fn cleanup_empty_secret_dir(secret_dir: &Path) {
    let _ = fs::remove_dir(secret_dir);
    if let Some(parent) = secret_dir.parent() {
        let _ = fs::remove_dir(parent);
    }
}

#[cfg(all(windows, not(test)))]
fn dpapi_protect_bytes(raw: &[u8]) -> Result<Vec<u8>, String> {
    use std::ptr::{null, null_mut};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{CryptProtectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB},
    };

    let mut input = CRYPT_INTEGER_BLOB {
        cbData: raw.len() as u32,
        pbData: raw.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: null_mut(),
    };

    let ok = unsafe {
        CryptProtectData(
            &mut input,
            null(),
            null(),
            null_mut(),
            null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }

    let bytes =
        unsafe { std::slice::from_raw_parts(output.pbData, output.cbData as usize) }.to_vec();
    unsafe {
        LocalFree(output.pbData.cast());
    }
    Ok(bytes)
}

#[cfg(all(windows, not(test)))]
fn dpapi_unprotect_bytes(encrypted: &[u8]) -> Result<Vec<u8>, String> {
    use std::ptr::{null, null_mut};
    use windows_sys::Win32::{
        Foundation::LocalFree,
        Security::Cryptography::{
            CryptUnprotectData, CRYPTPROTECT_UI_FORBIDDEN, CRYPT_INTEGER_BLOB,
        },
    };

    let mut input = CRYPT_INTEGER_BLOB {
        cbData: encrypted.len() as u32,
        pbData: encrypted.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB {
        cbData: 0,
        pbData: null_mut(),
    };

    let ok = unsafe {
        CryptUnprotectData(
            &mut input,
            null_mut(),
            null(),
            null_mut(),
            null(),
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
    };
    if ok == 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }

    let bytes =
        unsafe { std::slice::from_raw_parts(output.pbData, output.cbData as usize) }.to_vec();
    unsafe {
        LocalFree(output.pbData.cast());
    }
    Ok(bytes)
}

#[cfg(any(not(windows), test))]
fn dpapi_protect_bytes(raw: &[u8]) -> Result<Vec<u8>, String> {
    #[cfg(test)]
    if current_mock_keyring_behavior()?.fail_dpapi_writes {
        return Err("simulated DPAPI file write failure".into());
    }

    let mut encrypted = b"mock-dpapi:\0".to_vec();
    encrypted.extend(raw.iter().map(|byte| byte ^ 0xa5));
    Ok(encrypted)
}

#[cfg(any(not(windows), test))]
fn dpapi_unprotect_bytes(encrypted: &[u8]) -> Result<Vec<u8>, String> {
    const PREFIX: &[u8] = b"mock-dpapi:\0";
    encrypted
        .strip_prefix(PREFIX)
        .map(|bytes| bytes.iter().map(|byte| byte ^ 0xa5).collect())
        .ok_or_else(|| "invalid mock DPAPI payload".to_string())
}

impl CredentialMetadata {
    fn to_summary(
        &self,
        ready: bool,
        summary: Option<CredentialSummaryDetails>,
    ) -> CredentialSummary {
        CredentialSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            provider: self.provider.clone(),
            ready,
            validation_status: self.validation_status.clone(),
            last_tested_at: self.last_tested_at.clone(),
            last_test_message: self.last_test_message.clone(),
            summary,
        }
    }
}

#[cfg(not(test))]
fn secret_write_cache(
) -> &'static std::sync::Mutex<std::collections::HashMap<(String, String), String>> {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    static CACHE: OnceLock<Mutex<HashMap<(String, String), String>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(not(test))]
fn get_secret_raw(service_name: &str, user_name: &str) -> Result<Option<String>, String> {
    let entry = keyring_entry(service_name, user_name)?;
    match entry.get_password() {
        Ok(raw) => Ok(Some(raw)),
        Err(KeyringError::NoEntry) => {
            if let Ok(cache) = secret_write_cache().lock() {
                if let Some(cached) = cache.get(&(service_name.to_string(), user_name.to_string()))
                {
                    return Ok(Some(cached.clone()));
                }
            }
            Ok(None)
        }
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(not(test))]
fn set_secret_raw(service_name: &str, user_name: &str, raw: &str) -> Result<(), String> {
    let entry = keyring_entry(service_name, user_name)?;
    entry.set_password(raw).map_err(|error| error.to_string())?;

    if let Ok(mut cache) = secret_write_cache().lock() {
        cache.insert(
            (service_name.to_string(), user_name.to_string()),
            raw.to_string(),
        );
    }

    Ok(())
}

#[cfg(not(test))]
fn delete_secret_raw(service_name: &str, user_name: &str) -> Result<(), String> {
    let entry = keyring_entry(service_name, user_name)?;
    match entry.delete_credential() {
        Ok(()) | Err(KeyringError::NoEntry) => {
            if let Ok(mut cache) = secret_write_cache().lock() {
                cache.remove(&(service_name.to_string(), user_name.to_string()));
            }
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(not(test))]
fn keyring_entry(service_name: &str, user_name: &str) -> Result<Entry, String> {
    Entry::new(service_name, user_name)
        .map_err(|error| format!("failed to open secure credential store: {error}"))
}

#[cfg(test)]
fn mock_keyring() -> &'static Mutex<HashMap<(String, String), String>> {
    static MOCK_KEYRING: OnceLock<Mutex<HashMap<(String, String), String>>> = OnceLock::new();
    MOCK_KEYRING.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn mock_keyring_invisible_reads() -> &'static Mutex<HashMap<(String, String), usize>> {
    static MOCK_KEYRING_INVISIBLE_READS: OnceLock<Mutex<HashMap<(String, String), usize>>> =
        OnceLock::new();
    MOCK_KEYRING_INVISIBLE_READS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MockKeyringBehavior {
    fail_writes: bool,
    fail_dpapi_writes: bool,
    drop_writes: bool,
    read_misses_after_write: usize,
    accepts_delayed_visibility: Option<bool>,
    max_password_utf16_len: Option<usize>,
    prefer_dpapi_file_for_gcs: Option<bool>,
}

#[cfg(test)]
fn mock_keyring_behaviors() -> &'static Mutex<HashMap<String, MockKeyringBehavior>> {
    static MOCK_KEYRING_BEHAVIORS: OnceLock<Mutex<HashMap<String, MockKeyringBehavior>>> =
        OnceLock::new();
    MOCK_KEYRING_BEHAVIORS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
fn current_test_thread_id() -> String {
    format!("{:?}", std::thread::current().id())
}

#[cfg(test)]
fn current_mock_keyring_behavior() -> Result<MockKeyringBehavior, String> {
    let store = mock_keyring_behaviors()
        .lock()
        .map_err(|_| "mock keyring behavior lock poisoned".to_string())?;
    Ok(store
        .get(&current_test_thread_id())
        .cloned()
        .unwrap_or_default())
}

#[cfg(test)]
fn get_secret_raw(service_name: &str, user_name: &str) -> Result<Option<String>, String> {
    let key = test_key(service_name, user_name);

    let mut invisible_reads = mock_keyring_invisible_reads()
        .lock()
        .map_err(|_| "mock keyring invisible reads lock poisoned".to_string())?;
    if let Some(remaining) = invisible_reads.get_mut(&key) {
        if *remaining > 0 {
            *remaining -= 1;
            if *remaining == 0 {
                invisible_reads.remove(&key);
            }
            return Ok(None);
        }
        invisible_reads.remove(&key);
    }

    let store = mock_keyring()
        .lock()
        .map_err(|_| "mock keyring lock poisoned".to_string())?;
    Ok(store.get(&key).cloned())
}

#[cfg(test)]
fn set_secret_raw(service_name: &str, user_name: &str, raw: &str) -> Result<(), String> {
    let behavior = current_mock_keyring_behavior()?;
    if behavior.fail_writes {
        return Err("simulated secure store write failure".into());
    }
    if let Some(limit) = behavior.max_password_utf16_len {
        let encoded_len = raw.encode_utf16().count();
        if encoded_len > limit {
            return Err(format!(
                "Attribute 'password encoded as UTF-16' is longer than platform limit of {limit} chars"
            ));
        }
    }
    if behavior.drop_writes {
        return Ok(());
    }

    let key = test_key(service_name, user_name);
    let mut store = mock_keyring()
        .lock()
        .map_err(|_| "mock keyring lock poisoned".to_string())?;
    store.insert(key.clone(), raw.to_string());
    drop(store);

    if behavior.read_misses_after_write > 0 {
        let mut invisible_reads = mock_keyring_invisible_reads()
            .lock()
            .map_err(|_| "mock keyring invisible reads lock poisoned".to_string())?;
        invisible_reads.insert(key, behavior.read_misses_after_write);
    }

    Ok(())
}

#[cfg(test)]
fn delete_secret_raw(service_name: &str, user_name: &str) -> Result<(), String> {
    let key = test_key(service_name, user_name);
    let mut store = mock_keyring()
        .lock()
        .map_err(|_| "mock keyring lock poisoned".to_string())?;
    store.remove(&key);
    drop(store);

    let mut invisible_reads = mock_keyring_invisible_reads()
        .lock()
        .map_err(|_| "mock keyring invisible reads lock poisoned".to_string())?;
    invisible_reads.remove(&key);

    Ok(())
}

#[cfg(test)]
fn test_key(service_name: &str, user_name: &str) -> (String, String) {
    (
        format!("{:?}:{service_name}", std::thread::current().id()),
        user_name.to_string(),
    )
}

#[cfg(test)]
pub(crate) fn clear_test_secret_store() {
    if let Ok(mut store) = mock_keyring().lock() {
        let prefix = format!("{:?}:", std::thread::current().id());
        store.retain(|(service_name, _), _| !service_name.starts_with(&prefix));
    }
    if let Ok(mut store) = mock_keyring_invisible_reads().lock() {
        let prefix = format!("{:?}:", std::thread::current().id());
        store.retain(|(service_name, _), _| !service_name.starts_with(&prefix));
    }
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        store.remove(&current_test_thread_id());
    }
}

#[cfg(test)]
pub(crate) fn set_test_fail_writes(enabled: bool) {
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        let thread_id = current_test_thread_id();
        let mut behavior = store.get(&thread_id).cloned().unwrap_or_default();
        behavior.fail_writes = enabled;
        if behavior == MockKeyringBehavior::default() {
            store.remove(&thread_id);
        } else {
            store.insert(thread_id, behavior);
        }
    }
}

#[cfg(test)]
pub(crate) fn set_test_fail_dpapi_writes(enabled: bool) {
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        let thread_id = current_test_thread_id();
        let mut behavior = store.get(&thread_id).cloned().unwrap_or_default();
        behavior.fail_dpapi_writes = enabled;
        if behavior == MockKeyringBehavior::default() {
            store.remove(&thread_id);
        } else {
            store.insert(thread_id, behavior);
        }
    }
}

#[cfg(test)]
pub(crate) fn set_test_post_write_read_misses(misses: usize) {
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        let thread_id = current_test_thread_id();
        let mut behavior = store.get(&thread_id).cloned().unwrap_or_default();
        behavior.read_misses_after_write = misses;
        if behavior == MockKeyringBehavior::default() {
            store.remove(&thread_id);
        } else {
            store.insert(thread_id, behavior);
        }
    }
}

#[cfg(test)]
pub(crate) fn set_test_prefer_dpapi_file_for_gcs(enabled: Option<bool>) {
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        let thread_id = current_test_thread_id();
        let mut behavior = store.get(&thread_id).cloned().unwrap_or_default();
        behavior.prefer_dpapi_file_for_gcs = enabled;
        if behavior == MockKeyringBehavior::default() {
            store.remove(&thread_id);
        } else {
            store.insert(thread_id, behavior);
        }
    }
}

#[cfg(test)]
pub(crate) fn set_test_accepts_delayed_secure_store_visibility(enabled: bool) {
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        let thread_id = current_test_thread_id();
        let mut behavior = store.get(&thread_id).cloned().unwrap_or_default();
        behavior.accepts_delayed_visibility = Some(enabled);
        if behavior == MockKeyringBehavior::default() {
            store.remove(&thread_id);
        } else {
            store.insert(thread_id, behavior);
        }
    }
}

#[cfg(test)]
pub(crate) fn set_test_max_password_utf16_len(limit: Option<usize>) {
    if let Ok(mut store) = mock_keyring_behaviors().lock() {
        let thread_id = current_test_thread_id();
        let mut behavior = store.get(&thread_id).cloned().unwrap_or_default();
        behavior.max_password_utf16_len = limit;
        if behavior == MockKeyringBehavior::default() {
            store.remove(&thread_id);
        } else {
            store.insert(thread_id, behavior);
        }
    }
}

#[cfg(test)]
pub(crate) fn set_test_legacy_credentials(credentials: &StoredCredentials) {
    let payload = serde_json::to_string(&CredentialSecretPayload {
        provider: credentials.provider.clone(),
        secret: credentials.secret.clone(),
    })
    .expect("legacy credential payload should serialize");
    set_secret_raw(LEGACY_SERVICE_NAME, LEGACY_USER_NAME, &payload)
        .expect("legacy test credentials should store");
}

#[cfg(test)]
mod tests {
    use super::{
        clear_test_secret_store, create_credential_at_path, delete_credential_at_path,
        delete_secret, dpapi_secret_file_path, ensure_legacy_credentials_migrated_at_path,
        gcs_summary_details_from_service_account_json, get_credential_summary_from_path,
        list_credentials_from_path, load_credentials_by_id_from_path, mock_keyring,
        parse_credential_input, parse_credential_input_with_payload, read_index,
        record_credential_validation_at_path, secret_storage_dir,
        set_test_accepts_delayed_secure_store_visibility, set_test_fail_dpapi_writes,
        set_test_fail_writes, set_test_legacy_credentials, set_test_max_password_utf16_len,
        set_test_post_write_read_misses, set_test_prefer_dpapi_file_for_gcs,
        upsert_credential_at_path, AwsCredentialSummaryDetails, CredentialDraft,
        CredentialInputState, CredentialPayloadInput, CredentialSecretStorage,
        CredentialSummaryDetails, CredentialValidationStatus, StoredCredentials,
        CREDENTIALS_SERVICE_NAME, SECURE_STORE_VERIFY_ATTEMPTS,
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
        env::temp_dir().join(format!(
            "storage-goblin-{name}-{}-{suffix}.json",
            process::id()
        ))
    }

    fn cleanup(path: &PathBuf) {
        clear_test_secret_store();
        if path.exists() {
            fs::remove_file(path).expect("temporary credential index should be removed");
        }
        let secret_dir = secret_storage_dir(path);
        if secret_dir.exists() {
            let _ = fs::remove_dir_all(&secret_dir);
        }
    }

    fn test_secret_count() -> usize {
        let prefix = format!(
            "{:?}:{CREDENTIALS_SERVICE_NAME}",
            std::thread::current().id()
        );
        mock_keyring()
            .lock()
            .expect("mock keyring should lock")
            .keys()
            .filter(|(service_name, _)| service_name == &prefix)
            .count()
    }

    fn test_dpapi_secret_payload(path: &PathBuf, credential_id: &str) -> Vec<u8> {
        let secret_dir = secret_storage_dir(path);
        let file_path = dpapi_secret_file_path(&secret_dir, credential_id);
        fs::read(file_path).expect("dpapi payload should exist")
    }

    #[test]
    fn accepts_blank_or_complete_credential_input() {
        assert_eq!(
            parse_credential_input("aws", "", "").expect("blank credentials should be accepted"),
            CredentialInputState::Blank
        );
        assert_eq!(
            parse_credential_input("aws", " AKIA123 ", " secret ")
                .expect("complete credentials should be accepted"),
            CredentialInputState::Provided(StoredCredentials::aws_access_key(
                "aws", "AKIA123", "secret",
            ))
        );
    }

    #[test]
    fn accepts_gcs_service_account_credential_input() {
        let input = CredentialPayloadInput::GcsServiceAccount {
            service_account_json: "{\"type\":\"service_account\",\"project_id\":\"demo-project\",\"client_email\":\"demo@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}".into(),
        };

        assert_eq!(
            parse_credential_input_with_payload("gcp", Some(&input), "", "", "")
                .expect("gcs payload should be accepted"),
            CredentialInputState::Provided(
                StoredCredentials::gcs_service_account(
                    "gcp",
                    "{\"type\":\"service_account\",\"project_id\":\"demo-project\",\"client_email\":\"demo@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}",
                )
                .expect("gcs credentials should canonicalize")
            )
        );
    }

    #[test]
    fn accepts_legacy_flat_gcs_service_account_input_for_compatibility() {
        assert_eq!(
            parse_credential_input_with_payload(
                "gcs",
                None,
                "",
                "",
                "{\"type\":\"service_account\",\"project_id\":\"legacy-project\",\"client_email\":\"legacy@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}"
            )
            .expect("legacy flat gcs payload should be accepted"),
            CredentialInputState::Provided(
                StoredCredentials::gcs_service_account(
                    "gcs",
                    "{\"type\":\"service_account\",\"project_id\":\"legacy-project\",\"client_email\":\"legacy@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}",
                )
                .expect("legacy gcs credentials should canonicalize")
            )
        );
    }

    #[test]
    fn rejects_partial_credential_input() {
        let error = parse_credential_input("aws", "AKIA123", "")
            .expect_err("partial credentials should be rejected");
        assert!(error.contains("Provide both access key ID and secret access key"));
    }

    #[test]
    fn creates_lists_loads_and_deletes_named_credentials() {
        let path = temp_path("credential-crud");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");
        assert_eq!(created.name, "Primary");
        assert!(created.ready);
        assert_eq!(
            created.validation_status,
            CredentialValidationStatus::Untested
        );

        let listed = list_credentials_from_path(&path).expect("credentials should list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.id);
        assert!(listed[0].ready);
        assert_eq!(
            listed[0].validation_status,
            CredentialValidationStatus::Untested
        );

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("credential should load")
            .expect("credential should exist");
        assert_eq!(loaded.access_key_id, "AKIA123");
        assert_eq!(loaded.secret_access_key, "secret-1");

        assert!(delete_credential_at_path(&path, &created.id).expect("credential should delete"));
        assert!(load_credentials_by_id_from_path(&path, &created.id)
            .expect("post-delete load should work")
            .is_none());
        assert!(list_credentials_from_path(&path)
            .expect("credentials should list after delete")
            .is_empty());

        cleanup(&path);
    }

    #[test]
    fn upsert_updates_existing_credential_without_creating_duplicate_metadata() {
        let path = temp_path("credential-upsert");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        let updated = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "Renamed credential",
            "aws",
            &StoredCredentials::aws_access_key("aws", "AKIA456", "secret-2"),
        )
        .expect("credential should update");

        assert_eq!(updated.id, created.id);
        let listed = list_credentials_from_path(&path).expect("credentials should list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "Renamed credential");

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("updated credential should load")
            .expect("updated credential should exist");
        assert_eq!(loaded.access_key_id, "AKIA456");
        assert_eq!(loaded.secret_access_key, "secret-2");

        cleanup(&path);
    }

    #[test]
    fn migrates_legacy_single_credential_into_named_store() {
        let path = temp_path("credential-migration");
        clear_test_secret_store();
        set_test_legacy_credentials(&StoredCredentials {
            ..StoredCredentials::aws_access_key("aws", "AKIA-MIGRATE", "migrated-secret")
        });

        let migrated =
            ensure_legacy_credentials_migrated_at_path(&path, Some("Imported credential"))
                .expect("migration should succeed")
                .expect("legacy credentials should migrate");
        assert_eq!(migrated.name, "Imported credential");

        let listed = list_credentials_from_path(&path).expect("credentials should list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, migrated.id);
        assert!(listed[0].ready);

        let loaded = load_credentials_by_id_from_path(&path, &migrated.id)
            .expect("migrated credential should load")
            .expect("migrated credential should exist");
        assert_eq!(loaded.access_key_id, "AKIA-MIGRATE");
        assert_eq!(loaded.secret_access_key, "migrated-secret");

        assert!(
            ensure_legacy_credentials_migrated_at_path(&path, Some("Ignored"))
                .expect("second migration should succeed")
                .is_none()
        );

        cleanup(&path);
    }

    #[test]
    fn credential_summary_separates_secret_readiness_from_validation_status() {
        let path = temp_path("credential-summary");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        let secret_dir = secret_storage_dir(&path);
        delete_secret(
            &secret_dir,
            &created.id,
            CredentialSecretStorage::SecureStore,
        )
        .expect("secret should be removable for test");

        let summary = get_credential_summary_from_path(&path, &created.id)
            .expect("summary should load")
            .expect("summary should exist");
        assert!(!summary.ready);
        assert_eq!(
            summary.validation_status,
            CredentialValidationStatus::Untested
        );

        cleanup(&path);
    }

    #[test]
    fn aws_credential_summary_exposes_only_masked_key_preview() {
        let path = temp_path("credential-summary-aws-preview");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA12345678".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        assert_eq!(
            created.summary,
            Some(CredentialSummaryDetails::Aws(AwsCredentialSummaryDetails {
                access_key_id_preview: Some("****5678".into()),
            }))
        );

        let listed = list_credentials_from_path(&path).expect("credentials should list");
        assert_eq!(
            listed[0].summary,
            Some(CredentialSummaryDetails::Aws(AwsCredentialSummaryDetails {
                access_key_id_preview: Some("****5678".into()),
            }))
        );

        let serialized = serde_json::to_string(&listed[0]).expect("summary should serialize");
        assert!(!serialized.contains("AKIA12345678"));
        assert!(!serialized.contains("secret-1"));
        assert!(serialized.contains("****5678"));

        cleanup(&path);
    }

    #[test]
    fn gcs_credential_summary_exposes_safe_identity_fields_only() {
        let path = temp_path("credential-summary-gcs-details");
        clear_test_secret_store();
        set_test_prefer_dpapi_file_for_gcs(Some(true));

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "GCS".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: "{\"type\":\"service_account\",\"project_id\":\"example-project\",\"client_email\":\"sync@example-project.iam.gserviceaccount.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}".into(),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        let serialized = serde_json::to_string(&created).expect("summary should serialize");
        assert!(serialized.contains("sync@example-project.iam.gserviceaccount.com"));
        assert!(serialized.contains("example-project"));
        assert!(!serialized.contains("private_key"));
        assert!(!serialized.contains("BEGIN PRIVATE KEY"));

        let listed = list_credentials_from_path(&path).expect("credentials should list");
        let listed_serialized =
            serde_json::to_string(&listed[0]).expect("summary should serialize");
        assert!(listed_serialized.contains("clientEmail"));
        assert!(listed_serialized.contains("projectId"));
        assert!(!listed_serialized.contains("service_account_json"));

        cleanup(&path);
    }

    #[test]
    fn gcs_summary_parser_ignores_missing_or_blank_identity_fields() {
        let summary = gcs_summary_details_from_service_account_json(
            "{\"type\":\"service_account\",\"project_id\":\"  \",\"client_email\":\"\"}",
        );

        assert!(summary.client_email.is_none());
        assert!(summary.project_id.is_none());
    }

    #[test]
    fn create_allows_delayed_secure_store_visibility_on_windows_like_backends() {
        let path = temp_path("credential-round-trip-windows");
        clear_test_secret_store();
        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_post_write_read_misses(SECURE_STORE_VERIFY_ATTEMPTS + 1);

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Windows delayed".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("creation should succeed when visibility is delayed");

        let listed = list_credentials_from_path(&path).expect("credential metadata should list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, created.id);
        assert_eq!(listed[0].name, "Windows delayed");
        assert!(!listed[0].ready);

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("credential should become readable after delayed visibility")
            .expect("credential should exist");
        assert_eq!(loaded.access_key_id, "AKIA123");
        assert_eq!(loaded.secret_access_key, "secret-1");

        cleanup(&path);
    }

    #[test]
    fn create_fails_safely_when_secure_store_write_returns_error() {
        let path = temp_path("credential-write-failure");
        clear_test_secret_store();
        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_fail_writes(true);

        let error = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Broken".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect_err("creation should fail when the secure store write errors");

        assert!(error.contains("failed to store secure credentials"));
        assert!(error.contains("simulated secure store write failure"));
        assert!(list_credentials_from_path(&path)
            .expect("failed create should not leave metadata behind")
            .is_empty());
        assert_eq!(test_secret_count(), 0);

        cleanup(&path);
    }

    #[test]
    fn create_fails_after_verification_retry_budget_is_exhausted_on_strict_backends() {
        let path = temp_path("credential-round-trip-budget");
        clear_test_secret_store();
        set_test_accepts_delayed_secure_store_visibility(false);
        set_test_post_write_read_misses(SECURE_STORE_VERIFY_ATTEMPTS);

        let error = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Delayed too long".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect_err("creation should fail once verification retries are exhausted");

        assert!(error.contains("verification failed after write"));
        assert!(list_credentials_from_path(&path)
            .expect("failed create should not leave metadata behind")
            .is_empty());
        assert_eq!(test_secret_count(), 0);

        cleanup(&path);
    }

    #[test]
    fn update_retries_delayed_visibility_without_losing_new_secret() {
        let path = temp_path("credential-update-retry");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_post_write_read_misses(SECURE_STORE_VERIFY_ATTEMPTS + 1);

        let updated = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "Primary",
            "aws",
            &StoredCredentials::aws_access_key("aws", "AKIA456", "secret-2"),
        )
        .expect("update should succeed after retrying delayed visibility");

        assert_eq!(updated.id, created.id);
        let listed = list_credentials_from_path(&path).expect("updated credentials should list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "Primary");
        assert!(!listed[0].ready);

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("updated credential should load")
            .expect("updated credential should exist");
        assert_eq!(loaded.access_key_id, "AKIA456");
        assert_eq!(loaded.secret_access_key, "secret-2");

        cleanup(&path);
    }

    #[test]
    fn update_preserves_previous_secret_when_new_secret_write_fails() {
        let path = temp_path("credential-update-rollback");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_fail_writes(true);

        let error = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "Renamed credential",
            "aws",
            &StoredCredentials::aws_access_key("aws", "AKIA456", "secret-2"),
        )
        .expect_err("update should fail when the new secret write errors");

        assert!(error.contains("failed to store secure credentials"));

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("original credential should still load")
            .expect("original credential should still exist");
        assert_eq!(loaded.access_key_id, "AKIA123");
        assert_eq!(loaded.secret_access_key, "secret-1");

        let listed = list_credentials_from_path(&path).expect("credentials should list");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].name, "Primary");

        cleanup(&path);
    }

    #[test]
    fn credential_validation_state_can_be_tested_and_retested() {
        let path = temp_path("credential-validation");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        let passed = record_credential_validation_at_path(
            &path,
            &created.id,
            CredentialValidationStatus::Passed,
            "2026-04-04T12:00:00Z",
            Some("Validated access to bucket 'demo-bucket'."),
        )
        .expect("passing validation should store")
        .expect("credential should exist");

        assert!(passed.ready);
        assert_eq!(passed.validation_status, CredentialValidationStatus::Passed);
        assert_eq!(
            passed.last_tested_at.as_deref(),
            Some("2026-04-04T12:00:00Z")
        );
        assert_eq!(
            passed.last_test_message.as_deref(),
            Some("Validated access to bucket 'demo-bucket'.")
        );

        let failed = record_credential_validation_at_path(
            &path,
            &created.id,
            CredentialValidationStatus::Failed,
            "2026-04-04T13:00:00Z",
            Some("AccessDenied while listing bucket 'demo-bucket'."),
        )
        .expect("failed validation should store")
        .expect("credential should exist");

        assert!(failed.ready);
        assert_eq!(failed.validation_status, CredentialValidationStatus::Failed);
        assert_eq!(
            failed.last_tested_at.as_deref(),
            Some("2026-04-04T13:00:00Z")
        );
        assert_eq!(
            failed.last_test_message.as_deref(),
            Some("AccessDenied while listing bucket 'demo-bucket'.")
        );

        cleanup(&path);
    }

    #[test]
    fn credential_validation_messages_are_sanitized_before_persisting() {
        let path = temp_path("credential-validation-redaction");
        clear_test_secret_store();

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Primary".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("credential should be created");

        let stored = record_credential_validation_at_path(
            &path,
            &created.id,
            CredentialValidationStatus::Failed,
            "2026-04-04T13:00:00Z",
            Some("accessKeyId=AKIA123 secretAccessKey=secret-1 client_email=test@example.com"),
        )
        .expect("validation state should store")
        .expect("credential should exist");

        let message = stored
            .last_test_message
            .expect("sanitized validation message should persist");
        assert!(!message.contains("AKIA123"));
        assert!(!message.contains("secret-1"));
        assert!(!message.contains("test@example.com"));
        assert!(message.contains("[redacted]"));

        cleanup(&path);
    }

    #[test]
    fn creates_gcs_credentials_from_nested_or_flat_contracts() {
        let nested_path = temp_path("credential-gcs-nested");
        let flat_path = temp_path("credential-gcs-flat");
        clear_test_secret_store();
        set_test_prefer_dpapi_file_for_gcs(Some(true));

        let nested = create_credential_at_path(
            &nested_path,
            CredentialDraft {
                name: "Nested GCS".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: "{\"type\":\"service_account\",\"project_id\":\"nested\",\"client_email\":\"nested@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\",\"client_id\":\"ignored-field\"}".into(),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("nested gcs credential should be created");

        let nested_loaded = load_credentials_by_id_from_path(&nested_path, &nested.id)
            .expect("nested credential should load")
            .expect("nested credential should exist");
        assert_eq!(nested_loaded.provider, "gcs");
        assert_eq!(
            nested_loaded.gcs_service_account_json(),
            Some(
                "{\"type\":\"service_account\",\"project_id\":\"nested\",\"client_email\":\"nested@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}"
            )
        );

        let flat = create_credential_at_path(
            &flat_path,
            CredentialDraft {
                name: "Flat GCS".into(),
                provider: "gcs".into(),
                credential: None,
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: "{\"type\":\"service_account\",\"project_id\":\"flat\",\"client_email\":\"flat@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\",\"auth_uri\":\"https://accounts.google.com/o/oauth2/auth\"}"
                    .into(),
            },
        )
        .expect("flat gcs credential should be created for compatibility");

        let flat_loaded = load_credentials_by_id_from_path(&flat_path, &flat.id)
            .expect("flat credential should load")
            .expect("flat credential should exist");
        assert_eq!(flat_loaded.provider, "gcs");
        assert_eq!(
            flat_loaded.gcs_service_account_json(),
            Some(
                "{\"type\":\"service_account\",\"project_id\":\"flat\",\"client_email\":\"flat@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}"
            )
        );

        cleanup(&nested_path);
        cleanup(&flat_path);
    }

    #[test]
    fn deserializes_nested_gcs_payloads_with_camel_case_fields() {
        let draft: CredentialDraft = serde_json::from_str(
            r#"{
                "name":"Nested GCS",
                "provider":"gcs",
                "credential":{
                    "kind":"gcsServiceAccount",
                    "serviceAccountJson":"{\"type\":\"service_account\",\"project_id\":\"nested\"}"
                }
            }"#,
        )
        .expect("camelCase nested gcs payload should deserialize");

        assert_eq!(
            draft.credential,
            Some(CredentialPayloadInput::GcsServiceAccount {
                service_account_json: "{\"type\":\"service_account\",\"project_id\":\"nested\"}"
                    .into(),
            })
        );
        assert!(draft.access_key_id.is_empty());
        assert!(draft.secret_access_key.is_empty());
        assert!(draft.service_account_json.is_empty());
    }

    #[test]
    fn deserializes_nested_gcs_payloads_with_snake_case_fields_for_compatibility() {
        let draft: CredentialDraft = serde_json::from_str(
            r#"{
                "name":"Nested GCS",
                "provider":"gcs",
                "credential":{
                    "kind":"gcsServiceAccount",
                    "service_account_json":"{\"type\":\"service_account\",\"project_id\":\"legacy\"}"
                }
            }"#,
        )
        .expect("snake_case nested gcs payload should deserialize");

        assert_eq!(
            draft.credential,
            Some(CredentialPayloadInput::GcsServiceAccount {
                service_account_json: "{\"type\":\"service_account\",\"project_id\":\"legacy\"}"
                    .into(),
            })
        );
    }

    #[test]
    fn deserializes_nested_aws_payloads_with_camel_case_fields() {
        let draft: CredentialDraft = serde_json::from_str(
            r#"{
                "name":"Nested AWS",
                "provider":"aws",
                "credential":{
                    "kind":"awsAccessKey",
                    "accessKeyId":"AKIA123",
                    "secretAccessKey":"secret-123"
                }
            }"#,
        )
        .expect("camelCase nested aws payload should deserialize");

        assert_eq!(
            draft.credential,
            Some(CredentialPayloadInput::AwsAccessKey {
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-123".into(),
            })
        );
    }

    #[test]
    fn deserializes_nested_aws_payloads_with_snake_case_fields_for_compatibility() {
        let draft: CredentialDraft = serde_json::from_str(
            r#"{
                "name":"Nested AWS",
                "provider":"aws",
                "credential":{
                    "kind":"awsAccessKey",
                    "access_key_id":"AKIA456",
                    "secret_access_key":"secret-456"
                }
            }"#,
        )
        .expect("snake_case nested aws payload should deserialize");

        assert_eq!(
            draft.credential,
            Some(CredentialPayloadInput::AwsAccessKey {
                access_key_id: "AKIA456".into(),
                secret_access_key: "secret-456".into(),
            })
        );
    }

    #[test]
    fn loads_legacy_aws_secure_payload_without_kind() {
        clear_test_secret_store();

        super::set_secret_raw(
            CREDENTIALS_SERVICE_NAME,
            "legacy-aws",
            r#"{"provider":"aws","access_key_id":"AKIA-LEGACY","secret_access_key":"legacy-secret"}"#,
        )
        .expect("legacy aws payload should store");

        let loaded = super::load_secret(
            Path::new("."),
            "legacy-aws",
            CredentialSecretStorage::SecureStore,
        )
        .expect("legacy aws payload should load")
        .expect("legacy aws payload should exist")
        .credentials;

        assert_eq!(
            loaded,
            StoredCredentials::aws_access_key("aws", "AKIA-LEGACY", "legacy-secret")
        );

        clear_test_secret_store();
    }

    #[test]
    fn loads_legacy_gcs_secure_payload_without_kind() {
        clear_test_secret_store();

        super::set_secret_raw(
            CREDENTIALS_SERVICE_NAME,
            "legacy-gcs",
            "{\"provider\":\"gcs\",\"service_account_json\":\"{\\\"type\\\":\\\"service_account\\\",\\\"project_id\\\":\\\"legacy\\\"}\"}",
        )
        .expect("legacy gcs payload should store");

        let loaded = super::load_secret(
            Path::new("."),
            "legacy-gcs",
            CredentialSecretStorage::SecureStore,
        )
        .expect("legacy gcs payload should load")
        .expect("legacy gcs payload should exist")
        .credentials;

        assert_eq!(loaded.provider, "gcs");
        assert_eq!(loaded.access_key_id, "");
        assert_eq!(loaded.secret_access_key, "");
        assert_eq!(
            loaded.gcs_service_account_json(),
            Some("{\"type\":\"service_account\",\"project_id\":\"legacy\"}")
        );

        clear_test_secret_store();
    }

    #[test]
    fn create_compacts_gcs_secret_before_secure_store_persistence() {
        let path = temp_path("credential-gcs-compact-store");
        clear_test_secret_store();
        set_test_prefer_dpapi_file_for_gcs(Some(true));

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Compact GCS".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: format!(
                        "{{\"type\":\"service_account\",\"project_id\":\"demo-project\",\"client_email\":\"demo@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\n{}\\n-----END PRIVATE KEY-----\\n\",\"private_key_id\":\"kid-1\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"client_id\":\"{}\",\"client_x509_cert_url\":\"https://example.com/cert\"}}",
                        "A".repeat(512),
                        "9".repeat(2048)
                    ),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("gcs credential should be created");

        let stored_payload = test_dpapi_secret_payload(&path, &created.id);
        let decrypted_payload =
            super::load_dpapi_file_secret(&secret_storage_dir(&path), &created.id)
                .expect("dpapi payload should decrypt")
                .expect("dpapi payload should exist");
        assert!(!stored_payload.is_empty());
        assert!(!decrypted_payload.contains("client_x509_cert_url"));
        assert!(!decrypted_payload.contains("client_id"));
        assert!(!decrypted_payload.contains("\"token_uri\""));

        let listed = list_credentials_from_path(&path).expect("gcs credentials should list");
        let index = read_index(&path).expect("index should load");
        assert_eq!(listed.len(), 1);
        assert_eq!(
            index.credentials[0].secret_storage,
            CredentialSecretStorage::DpapiFile
        );

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("canonical gcs credential should load")
            .expect("canonical gcs credential should exist");
        assert_eq!(loaded.provider, "gcs");
        let raw = loaded
            .gcs_service_account_json()
            .expect("gcs service account JSON should exist");
        assert!(raw.contains("\"project_id\":\"demo-project\""));
        assert!(raw.contains("\"client_email\":\"demo@example.com\""));
        assert!(raw.contains("\"private_key_id\":\"kid-1\""));
        assert!(!raw.contains("client_x509_cert_url"));
        assert!(!raw.contains("client_id"));

        cleanup(&path);
    }

    #[test]
    fn create_accepts_oversized_raw_gcs_json_when_compacted_payload_fits_secure_store() {
        let path = temp_path("credential-gcs-oversized");
        clear_test_secret_store();
        set_test_max_password_utf16_len(Some(2560));
        set_test_prefer_dpapi_file_for_gcs(Some(true));

        let oversized_client_id = "9".repeat(3000);
        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Oversized GCS".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: format!(
                        "{{\"type\":\"service_account\",\"project_id\":\"demo-project\",\"client_email\":\"demo@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\",\"private_key_id\":\"kid-1\",\"token_uri\":\"https://oauth2.googleapis.com/token\",\"client_id\":\"{oversized_client_id}\",\"client_x509_cert_url\":\"https://example.com/cert\"}}"
                    ),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("oversized raw gcs credential should store after compaction");

        let stored_payload = test_dpapi_secret_payload(&path, &created.id);
        assert!(!stored_payload.is_empty());

        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("compacted gcs credential should load")
            .expect("compacted gcs credential should exist");
        assert_eq!(loaded.provider, "gcs");
        assert!(!loaded
            .gcs_service_account_json()
            .expect("gcs json should exist")
            .contains("client_id"));

        cleanup(&path);
    }

    #[test]
    fn windows_gcs_prefers_dpapi_file_backend_and_keeps_aws_in_secure_store() {
        let gcs_path = temp_path("credential-gcs-dpapi");
        let aws_path = temp_path("credential-aws-secure-store");
        clear_test_secret_store();
        set_test_prefer_dpapi_file_for_gcs(Some(true));

        let gcs = create_credential_at_path(
            &gcs_path,
            CredentialDraft {
                name: "Windows GCS".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: "{\"type\":\"service_account\",\"project_id\":\"dpapi\",\"client_email\":\"dpapi@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}".into(),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("gcs credential should use dpapi file backend");

        let gcs_secret_dir = secret_storage_dir(&gcs_path);
        let gcs_secret_file = dpapi_secret_file_path(&gcs_secret_dir, &gcs.id);
        assert!(gcs_secret_file.exists());
        assert_eq!(test_secret_count(), 0);
        let gcs_index = read_index(&gcs_path).expect("gcs index should load");
        assert_eq!(
            gcs_index.credentials[0].secret_storage,
            CredentialSecretStorage::DpapiFile
        );

        let aws = create_credential_at_path(
            &aws_path,
            CredentialDraft {
                name: "AWS stays secure store".into(),
                provider: "aws".into(),
                credential: None,
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
                service_account_json: String::new(),
            },
        )
        .expect("aws credential should stay in secure store");

        assert!(!dpapi_secret_file_path(&secret_storage_dir(&aws_path), &aws.id).exists());
        assert_eq!(test_secret_count(), 1);
        let aws_index = read_index(&aws_path).expect("aws index should load");
        assert_eq!(
            aws_index.credentials[0].secret_storage,
            CredentialSecretStorage::SecureStore
        );

        cleanup(&gcs_path);
        cleanup(&aws_path);
    }

    #[test]
    fn missing_secret_storage_metadata_defaults_to_secure_store() {
        let path = temp_path("credential-secret-storage-default");
        clear_test_secret_store();

        fs::write(
            &path,
            r#"{
  "version": 1,
  "credentials": [
    {
      "id": "legacy-id",
      "name": "Legacy",
      "provider": "aws",
      "createdAt": "2026-04-01T00:00:00Z",
      "updatedAt": "2026-04-01T00:00:00Z"
    }
  ]
}"#,
        )
        .expect("legacy index should write");
        super::set_secret_raw(
            CREDENTIALS_SERVICE_NAME,
            "legacy-id",
            r#"{"provider":"aws","kind":"awsAccessKey","access_key_id":"AKIA-LEGACY","secret_access_key":"legacy-secret"}"#,
        )
        .expect("legacy secure-store secret should write");

        let listed = list_credentials_from_path(&path).expect("legacy metadata should list");
        assert_eq!(listed.len(), 1);
        assert!(listed[0].ready);

        let loaded = load_credentials_by_id_from_path(&path, "legacy-id")
            .expect("legacy credential should load")
            .expect("legacy credential should exist");
        assert_eq!(loaded.access_key_id, "AKIA-LEGACY");

        cleanup(&path);
    }

    #[test]
    fn delete_removes_dpapi_file_secret() {
        let path = temp_path("credential-gcs-delete-dpapi");
        clear_test_secret_store();
        set_test_prefer_dpapi_file_for_gcs(Some(true));

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Delete GCS".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: "{\"type\":\"service_account\",\"project_id\":\"delete\",\"client_email\":\"delete@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}".into(),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("gcs credential should create");

        let secret_file = dpapi_secret_file_path(&secret_storage_dir(&path), &created.id);
        assert!(secret_file.exists());

        assert!(delete_credential_at_path(&path, &created.id).expect("delete should succeed"));
        assert!(!secret_file.exists());

        cleanup(&path);
    }

    #[test]
    fn update_rolls_back_when_switch_to_dpapi_file_backend_fails() {
        let path = temp_path("credential-gcs-dpapi-failure");
        clear_test_secret_store();
        set_test_prefer_dpapi_file_for_gcs(Some(false));

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "GCS secure store".into(),
                provider: "gcs".into(),
                credential: Some(CredentialPayloadInput::GcsServiceAccount {
                    service_account_json: "{\"type\":\"service_account\",\"project_id\":\"rollback\",\"client_email\":\"rollback@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nabc\\n-----END PRIVATE KEY-----\\n\"}".into(),
                }),
                access_key_id: String::new(),
                secret_access_key: String::new(),
                service_account_json: String::new(),
            },
        )
        .expect("gcs credential should initially use secure store");

        set_test_prefer_dpapi_file_for_gcs(Some(true));
        set_test_fail_dpapi_writes(true);

        let error = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "GCS secure store",
            "gcs",
            &StoredCredentials::gcs_service_account(
                "gcs",
                "{\"type\":\"service_account\",\"project_id\":\"rollback-updated\",\"client_email\":\"rollback@example.com\",\"private_key\":\"-----BEGIN PRIVATE KEY-----\\nxyz\\n-----END PRIVATE KEY-----\\n\"}",
            )
            .expect("updated gcs credentials should canonicalize"),
        )
        .expect_err("dpapi failure should abort update");

        assert!(error.contains("failed to store DPAPI credential file"));
        let loaded = load_credentials_by_id_from_path(&path, &created.id)
            .expect("original gcs credential should load")
            .expect("original gcs credential should exist");
        assert!(loaded
            .gcs_service_account_json()
            .expect("gcs json should exist")
            .contains("rollback"));

        let index = read_index(&path).expect("index should load");
        assert_eq!(
            index.credentials[0].secret_storage,
            CredentialSecretStorage::SecureStore
        );

        cleanup(&path);
    }

    #[test]
    fn loads_current_tagged_secure_payload_with_kind() {
        clear_test_secret_store();

        super::set_secret_raw(
            CREDENTIALS_SERVICE_NAME,
            "tagged-aws",
            r#"{"provider":"aws","kind":"awsAccessKey","access_key_id":"AKIA-TAGGED","secret_access_key":"tagged-secret"}"#,
        )
        .expect("tagged aws payload should store");

        let loaded = super::load_secret(
            Path::new("."),
            "tagged-aws",
            CredentialSecretStorage::SecureStore,
        )
        .expect("tagged aws payload should load")
        .expect("tagged aws payload should exist")
        .credentials;

        assert_eq!(
            loaded,
            StoredCredentials::aws_access_key("aws", "AKIA-TAGGED", "tagged-secret")
        );

        clear_test_secret_store();
    }
}
