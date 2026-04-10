use serde::{Deserialize, Serialize};
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

use super::{app_storage_path, now_iso, CREDENTIALS_INDEX_FILE_NAME};

const LEGACY_SERVICE_NAME: &str = "storage-goblin.sync-profile";
const LEGACY_USER_NAME: &str = "active-profile";
const CREDENTIALS_SERVICE_NAME: &str = "storage-goblin.credentials";
const SECURE_STORE_VERIFY_ATTEMPTS: usize = 4;
#[cfg(not(test))]
const SECURE_STORE_VERIFY_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecureStoreVerificationOutcome {
    Verified,
    Missing,
    Mismatched,
}

static CREDENTIAL_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialInputState {
    Blank,
    Provided(StoredCredentials),
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
pub struct CredentialSummary {
    pub id: String,
    pub name: String,
    pub ready: bool,
    pub validation_status: CredentialValidationStatus,
    pub last_tested_at: Option<String>,
    pub last_test_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialDraft {
    pub name: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CredentialSecretPayload {
    access_key_id: String,
    secret_access_key: String,
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
    created_at: String,
    updated_at: String,
    #[serde(default)]
    validation_status: CredentialValidationStatus,
    #[serde(default)]
    last_tested_at: Option<String>,
    #[serde(default)]
    last_test_message: Option<String>,
}

pub fn parse_credential_input(
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<CredentialInputState, String> {
    let access_key_id = access_key_id.trim().to_string();
    let secret_access_key = secret_access_key.trim().to_string();

    match (access_key_id.is_empty(), secret_access_key.is_empty()) {
        (true, true) => Ok(CredentialInputState::Blank),
        (false, false) => Ok(CredentialInputState::Provided(StoredCredentials {
            access_key_id,
            secret_access_key,
        })),
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
    credentials: &StoredCredentials,
) -> Result<CredentialSummary, String> {
    let path = credentials_index_path(app)?;
    upsert_credential_at_path(&path, credential_id, name, credentials)
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
    let mut summaries = read_index(path)?
        .credentials
        .into_iter()
        .map(|metadata| {
            let ready = secret_exists(&metadata.id)?;
            Ok(metadata.to_summary(ready))
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
    let credentials = match parse_credential_input(&draft.access_key_id, &draft.secret_access_key)?
    {
        CredentialInputState::Provided(credentials) => credentials,
        CredentialInputState::Blank => {
            return Err(
                "Provide both access key ID and secret access key when creating a credential."
                    .into(),
            )
        }
    };

    upsert_credential_at_path(path, None, &name, &credentials)
}

fn upsert_credential_at_path(
    path: &Path,
    credential_id: Option<&str>,
    name: &str,
    credentials: &StoredCredentials,
) -> Result<CredentialSummary, String> {
    let name = normalize_credential_name(name)?;
    let mut index = read_index(path)?;
    let now = now_iso();
    let id = credential_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(generate_credential_id);
    let is_existing = index.credentials.iter().any(|entry| entry.id == id);
    let previous_secret = if is_existing { load_secret(&id)? } else { None };
    let payload = CredentialSecretPayload {
        access_key_id: credentials.access_key_id.trim().to_string(),
        secret_access_key: credentials.secret_access_key.trim().to_string(),
    };

    let ready = store_secret_and_verify_round_trip(&id, &payload, previous_secret.as_ref())?;

    if let Some(existing) = index.credentials.iter_mut().find(|entry| entry.id == id) {
        existing.name = name.clone();
        existing.updated_at = now;
        existing.validation_status = CredentialValidationStatus::Untested;
        existing.last_tested_at = None;
        existing.last_test_message = None;
    } else {
        index.credentials.push(CredentialMetadata {
            id: id.clone(),
            name: name.clone(),
            created_at: now.clone(),
            updated_at: now,
            validation_status: CredentialValidationStatus::Untested,
            last_tested_at: None,
            last_test_message: None,
        });
    }

    if let Err(error) = write_index(path, &index) {
        if !is_existing {
            let _ = delete_secret(&id);
        }
        return Err(error);
    }

    Ok(CredentialSummary {
        id,
        name,
        ready,
        validation_status: CredentialValidationStatus::Untested,
        last_tested_at: None,
        last_test_message: None,
    })
}

fn delete_credential_at_path(path: &Path, credential_id: &str) -> Result<bool, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Ok(false);
    }

    let mut index = read_index(path)?;
    let original_len = index.credentials.len();
    index.credentials.retain(|entry| entry.id != credential_id);
    let removed = index.credentials.len() != original_len;

    if removed {
        write_index(path, &index)?;
    }

    delete_secret(credential_id)?;
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
    let Some(metadata) = index
        .credentials
        .into_iter()
        .find(|entry| entry.id == credential_id)
    else {
        return Ok(None);
    };

    let ready = secret_exists(credential_id)?;
    Ok(Some(metadata.to_summary(ready)))
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
    let Some(metadata) = index
        .credentials
        .iter_mut()
        .find(|entry| entry.id == credential_id)
    else {
        return Ok(None);
    };

    metadata.validation_status = status;
    metadata.last_tested_at = normalize_optional_text(checked_at);
    metadata.last_test_message = normalize_optional_text_from_option(message);
    metadata.updated_at = now_iso();

    write_index(path, &index)?;
    let metadata = index
        .credentials
        .iter()
        .find(|entry| entry.id == credential_id)
        .ok_or_else(|| "credential metadata disappeared during validation update".to_string())?;
    let ready = secret_exists(credential_id)?;
    Ok(Some(metadata.to_summary(ready)))
}

fn load_credentials_by_id_from_path(
    path: &Path,
    credential_id: &str,
) -> Result<Option<StoredCredentials>, String> {
    let credential_id = credential_id.trim();
    if credential_id.is_empty() {
        return Ok(None);
    }

    let summary = get_credential_summary_from_path(path, credential_id)?;
    if summary.is_none() {
        return Ok(None);
    }

    load_secret(credential_id)
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

fn load_legacy_credentials() -> Result<Option<StoredCredentials>, String> {
    let raw = match get_secret_raw(LEGACY_SERVICE_NAME, LEGACY_USER_NAME) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(None),
        Err(error) => return Err(format!("failed to load legacy secure credentials: {error}")),
    };

    let payload: CredentialSecretPayload = serde_json::from_str(&raw)
        .map_err(|error| format!("failed to parse legacy secure credentials payload: {error}"))?;

    Ok(Some(StoredCredentials {
        access_key_id: payload.access_key_id.trim().to_string(),
        secret_access_key: payload.secret_access_key.trim().to_string(),
    }))
}

fn delete_legacy_credentials() -> Result<(), String> {
    delete_secret_raw(LEGACY_SERVICE_NAME, LEGACY_USER_NAME)
        .map_err(|error| format!("failed to remove migrated legacy credentials: {error}"))
}

fn store_secret_and_verify_round_trip(
    credential_id: &str,
    payload: &CredentialSecretPayload,
    previous_secret: Option<&StoredCredentials>,
) -> Result<bool, String> {
    store_secret(credential_id, payload)?;

    let expected = StoredCredentials {
        access_key_id: payload.access_key_id.trim().to_string(),
        secret_access_key: payload.secret_access_key.trim().to_string(),
    };

    let verification = verify_secret_round_trip(credential_id, &expected)?;
    if verification == SecureStoreVerificationOutcome::Verified {
        return Ok(true);
    }

    if verification == SecureStoreVerificationOutcome::Missing
        && secure_store_write_accepts_delayed_visibility()
    {
        report_nonblocking_secure_store_visibility_delay(credential_id);
        return Ok(false);
    }

    rollback_after_failed_secret_verification(
        credential_id,
        previous_secret,
        verification.failure_message(),
    )?;

    unreachable!("failed secure-store verification should always return an error")
}

fn verify_secret_round_trip(
    credential_id: &str,
    expected: &StoredCredentials,
) -> Result<SecureStoreVerificationOutcome, String> {
    let mut saw_mismatch = false;

    for attempt in 0..SECURE_STORE_VERIFY_ATTEMPTS {
        match load_secret(credential_id)? {
            Some(stored) if stored == *expected => {
                return Ok(SecureStoreVerificationOutcome::Verified)
            }
            Some(_) => saw_mismatch = true,
            None => {}
        }

        if attempt + 1 < SECURE_STORE_VERIFY_ATTEMPTS {
            pause_before_secure_store_verification_retry();
        }
    }

    Ok(if saw_mismatch {
        SecureStoreVerificationOutcome::Mismatched
    } else {
        SecureStoreVerificationOutcome::Missing
    })
}

impl SecureStoreVerificationOutcome {
    fn failure_message(self) -> &'static str {
        match self {
            SecureStoreVerificationOutcome::Verified => {
                "secure credential storage verification unexpectedly succeeded"
            }
            SecureStoreVerificationOutcome::Missing => {
                "secure credential storage verification failed after write"
            }
            SecureStoreVerificationOutcome::Mismatched => {
                "failed to verify secure credential storage after write"
            }
        }
    }
}

fn rollback_after_failed_secret_verification(
    credential_id: &str,
    previous_secret: Option<&StoredCredentials>,
    error: &str,
) -> Result<(), String> {
    if let Some(previous_secret) = previous_secret {
        let rollback_payload = CredentialSecretPayload {
            access_key_id: previous_secret.access_key_id.clone(),
            secret_access_key: previous_secret.secret_access_key.clone(),
        };

        store_secret(credential_id, &rollback_payload)
            .and_then(|_| {
                verify_secret_round_trip(credential_id, previous_secret).and_then(|outcome| {
                    if outcome == SecureStoreVerificationOutcome::Verified {
                        Ok(())
                    } else {
                        Err(outcome.failure_message().to_string())
                    }
                })
            })
            .map_err(|restore_error| {
                format!("{error}. Failed to restore previous secure credentials: {restore_error}")
            })?;

        return Err(error.into());
    }

    delete_secret(credential_id).map_err(|cleanup_error| {
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

fn store_secret(credential_id: &str, payload: &CredentialSecretPayload) -> Result<(), String> {
    let raw = serde_json::to_string(payload)
        .map_err(|error| format!("failed to serialize secure credential payload: {error}"))?;
    set_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id, &raw)
        .map_err(|error| format!("failed to store secure credentials: {error}"))
}

fn load_secret(credential_id: &str) -> Result<Option<StoredCredentials>, String> {
    let raw = match get_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id) {
        Ok(Some(raw)) => raw,
        Ok(None) => return Ok(None),
        Err(error) => return Err(format!("failed to load secure credentials: {error}")),
    };

    let payload: CredentialSecretPayload = serde_json::from_str(&raw)
        .map_err(|error| format!("failed to parse secure credential payload: {error}"))?;
    Ok(Some(StoredCredentials {
        access_key_id: payload.access_key_id.trim().to_string(),
        secret_access_key: payload.secret_access_key.trim().to_string(),
    }))
}

fn secret_exists(credential_id: &str) -> Result<bool, String> {
    match get_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id) {
        Ok(Some(_)) => Ok(true),
        Ok(None) => Ok(false),
        Err(error) => Err(format!("failed to inspect secure credentials: {error}")),
    }
}

fn delete_secret(credential_id: &str) -> Result<(), String> {
    delete_secret_raw(CREDENTIALS_SERVICE_NAME, credential_id)
        .map_err(|error| format!("failed to delete secure credentials: {error}"))
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

impl CredentialMetadata {
    fn to_summary(&self, ready: bool) -> CredentialSummary {
        CredentialSummary {
            id: self.id.clone(),
            name: self.name.clone(),
            ready,
            validation_status: self.validation_status.clone(),
            last_tested_at: self.last_tested_at.clone(),
            last_test_message: self.last_test_message.clone(),
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
    drop_writes: bool,
    read_misses_after_write: usize,
    accepts_delayed_visibility: Option<bool>,
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
pub(crate) fn set_test_legacy_credentials(credentials: &StoredCredentials) {
    let payload = serde_json::to_string(&CredentialSecretPayload {
        access_key_id: credentials.access_key_id.clone(),
        secret_access_key: credentials.secret_access_key.clone(),
    })
    .expect("legacy credential payload should serialize");
    set_secret_raw(LEGACY_SERVICE_NAME, LEGACY_USER_NAME, &payload)
        .expect("legacy test credentials should store");
}

#[cfg(test)]
mod tests {
    use super::{
        clear_test_secret_store, create_credential_at_path, delete_credential_at_path,
        ensure_legacy_credentials_migrated_at_path, get_credential_summary_from_path,
        list_credentials_from_path, load_credentials_by_id_from_path, mock_keyring,
        parse_credential_input, record_credential_validation_at_path,
        set_test_accepts_delayed_secure_store_visibility, set_test_fail_writes,
        set_test_legacy_credentials, set_test_post_write_read_misses, upsert_credential_at_path,
        CredentialDraft, CredentialInputState, CredentialValidationStatus, StoredCredentials,
        CREDENTIALS_SERVICE_NAME, SECURE_STORE_VERIFY_ATTEMPTS,
    };
    use std::{
        env, fs,
        path::PathBuf,
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

    #[test]
    fn accepts_blank_or_complete_credential_input() {
        assert_eq!(
            parse_credential_input("", "").expect("blank credentials should be accepted"),
            CredentialInputState::Blank
        );
        assert_eq!(
            parse_credential_input(" AKIA123 ", " secret ")
                .expect("complete credentials should be accepted"),
            CredentialInputState::Provided(StoredCredentials {
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret".into(),
            })
        );
    }

    #[test]
    fn rejects_partial_credential_input() {
        let error = parse_credential_input("AKIA123", "")
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
            },
        )
        .expect("credential should be created");

        let updated = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "Renamed credential",
            &StoredCredentials {
                access_key_id: "AKIA456".into(),
                secret_access_key: "secret-2".into(),
            },
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
            access_key_id: "AKIA-MIGRATE".into(),
            secret_access_key: "migrated-secret".into(),
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
            },
        )
        .expect("credential should be created");

        super::delete_secret(&created.id).expect("secret should be removable for test");

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
    fn create_allows_delayed_secure_store_visibility_on_windows_like_backends() {
        let path = temp_path("credential-round-trip-windows");
        clear_test_secret_store();
        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_post_write_read_misses(SECURE_STORE_VERIFY_ATTEMPTS + 1);

        let created = create_credential_at_path(
            &path,
            CredentialDraft {
                name: "Windows delayed".into(),
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
            },
        )
        .expect("credential should be created");

        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_post_write_read_misses(SECURE_STORE_VERIFY_ATTEMPTS + 1);

        let updated = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "Primary",
            &StoredCredentials {
                access_key_id: "AKIA456".into(),
                secret_access_key: "secret-2".into(),
            },
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
            },
        )
        .expect("credential should be created");

        set_test_accepts_delayed_secure_store_visibility(true);
        set_test_fail_writes(true);

        let error = upsert_credential_at_path(
            &path,
            Some(&created.id),
            "Renamed credential",
            &StoredCredentials {
                access_key_id: "AKIA456".into(),
                secret_access_key: "secret-2".into(),
            },
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
                access_key_id: "AKIA123".into(),
                secret_access_key: "secret-1".into(),
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
}
