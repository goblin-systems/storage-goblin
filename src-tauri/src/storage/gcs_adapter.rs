use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use percent_encoding::{utf8_percent_encode, AsciiSet, CONTROLS};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use super::error::SyncError;
use super::remote_bin::{namespace_prefix, ManagedLifecycleRulePlan};
use super::sanitizer::sanitize_sensitive_text;
use super::transfer::DownloadWriter;

const STORAGE_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.full_control";

const GCS_STORAGE_API_BASE_URL: &str = "https://storage.googleapis.com";
const ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'+')
    .add(b'?')
    .add(b'[')
    .add(b']')
    .add(b'{')
    .add(b'}')
    .add(b'/');

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsServiceAccountCredentials {
    pub project_id: String,
    pub client_email: String,
    pub private_key: String,
    pub private_key_id: Option<String>,
    pub token_uri: String,
}

#[derive(Debug, Clone)]
pub struct GcsClient {
    // Crate-visible so the upload paths in `gcs_upload` can drive the same
    // client without a second constructor.
    pub(crate) http: reqwest::Client,
    pub(crate) token: String,
    pub(crate) storage_api_base_url: String,
    pub credentials: GcsServiceAccountCredentials,
    /// Objects at or above this size upload through a resumable session.
    pub(crate) resumable_threshold_bytes: u64,
    /// Bytes per resumable chunk. GCS requires a multiple of 256 KiB for every
    /// chunk except the last.
    pub(crate) resumable_chunk_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsObject {
    pub name: String,
    pub size: u64,
    pub updated: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
    /// Custom object metadata. GCS returns full object resources on list, so
    /// the goblin content fingerprint we attach on upload survives round-trips
    /// without an extra request per object.
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsObjectVersionItem {
    pub name: String,
    pub generation: String,
    pub size: u64,
    pub updated: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
    pub time_deleted: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GcsObjectVersionPage {
    pub items: Vec<GcsObjectVersionItem>,
    pub next_page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcsBucketLifecycleConfiguration {
    #[serde(default)]
    pub rule: Vec<GcsLifecycleRule>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcsLifecycleRule {
    pub action: GcsLifecycleAction,
    pub condition: GcsLifecycleCondition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GcsLifecycleAction {
    #[serde(rename = "type")]
    pub action_type: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GcsLifecycleCondition {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub age: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub matches_prefix: Vec<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct GcsBucketLifecycleState {
    pub configuration: Option<GcsBucketLifecycleConfiguration>,
    pub metageneration: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum GcsLifecycleRulesChange {
    None,
    Replace(Vec<GcsLifecycleRule>),
    DeleteBucketLifecycle,
}

#[derive(Debug, Deserialize)]
struct GcsTokenResponse {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct GcsBucketListResponse {
    #[serde(default)]
    items: Vec<GcsBucketItem>,
}

#[derive(Debug, Deserialize)]
struct GcsBucketItem {
    name: String,
}

#[derive(Debug, Deserialize)]
struct GcsObjectListResponse {
    #[serde(default)]
    items: Vec<GcsObjectItem>,
    #[serde(rename = "nextPageToken")]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcsObjectItem {
    name: String,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    updated: Option<String>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default, rename = "storageClass")]
    storage_class: Option<String>,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsVersionedObjectItem {
    name: String,
    generation: String,
    #[serde(default)]
    size: Option<String>,
    #[serde(default)]
    updated: Option<String>,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    storage_class: Option<String>,
    #[serde(default)]
    time_deleted: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsVersionedObjectListResponse {
    #[serde(default)]
    items: Vec<GcsVersionedObjectItem>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsRewriteResponse {
    done: bool,
    rewrite_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GcsBucketVersioning {
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct GcsBucketDetail {
    #[serde(default)]
    metageneration: Option<String>,
    #[serde(default)]
    lifecycle: Option<GcsBucketLifecycleConfiguration>,
    versioning: Option<GcsBucketVersioning>,
}

#[allow(dead_code)]
#[derive(Debug, Serialize)]
struct GcsCreateBucketRequest<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct GcsObjectMetadata<'a> {
    pub(crate) name: &'a str,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub(crate) metadata: HashMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ServiceAccountJwtClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(Debug, Deserialize)]
struct RawServiceAccountJson {
    #[serde(default)]
    project_id: String,
    #[serde(default)]
    client_email: String,
    #[serde(default)]
    private_key: String,
    #[serde(default)]
    private_key_id: Option<String>,
    #[serde(default = "default_token_uri")]
    token_uri: String,
    #[serde(default)]
    r#type: String,
}

#[derive(Debug, Serialize)]
struct CanonicalServiceAccountJson {
    #[serde(rename = "type")]
    account_type: &'static str,
    project_id: String,
    client_email: String,
    private_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key_id: Option<String>,
    #[serde(
        default = "default_token_uri",
        skip_serializing_if = "is_default_token_uri"
    )]
    token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".into()
}

fn is_default_token_uri(value: &String) -> bool {
    value == default_token_uri().as_str()
}

impl GcsServiceAccountCredentials {
    pub fn from_json(raw: &str) -> Result<Self, SyncError> {
        let parsed: RawServiceAccountJson = serde_json::from_str(raw)
            .map_err(|error| format!("failed to parse GCS service account JSON: {error}"))?;

        if !parsed.r#type.is_empty() && parsed.r#type != "service_account" {
            return Err("GCS credentials must be a service account JSON key.".into());
        }
        if parsed.project_id.trim().is_empty() {
            return Err("GCS service account JSON is missing project_id.".into());
        }
        if parsed.client_email.trim().is_empty() {
            return Err("GCS service account JSON is missing client_email.".into());
        }
        if parsed.private_key.trim().is_empty() {
            return Err("GCS service account JSON is missing private_key.".into());
        }

        Ok(Self {
            project_id: parsed.project_id.trim().to_string(),
            client_email: parsed.client_email.trim().to_string(),
            private_key: parsed.private_key,
            private_key_id: parsed.private_key_id.map(|value| value.trim().to_string()),
            token_uri: parsed.token_uri.trim().to_string(),
        })
    }

    pub fn to_canonical_json(&self) -> Result<String, SyncError> {
        serde_json::to_string(&CanonicalServiceAccountJson {
            account_type: "service_account",
            project_id: self.project_id.clone(),
            client_email: self.client_email.clone(),
            private_key: self.private_key.clone(),
            private_key_id: self
                .private_key_id
                .clone()
                .filter(|value| !value.trim().is_empty()),
            token_uri: self.token_uri.clone(),
        })
        .map_err(|error| {
            SyncError::internal(format!(
                "failed to serialize canonical GCS service account JSON: {error}"
            ))
        })
    }
}

pub fn compact_service_account_json(raw: &str) -> Result<String, SyncError> {
    GcsServiceAccountCredentials::from_json(raw)?.to_canonical_json()
}

impl GcsClient {
    pub async fn new(credentials: &GcsServiceAccountCredentials) -> Result<Self, SyncError> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|error| format!("failed to build GCS HTTP client: {error}"))?;
        let token = fetch_access_token(&http, credentials).await?;

        Ok(Self {
            http,
            token,
            storage_api_base_url: GCS_STORAGE_API_BASE_URL.into(),
            credentials: credentials.clone(),
            resumable_threshold_bytes: super::gcs_upload::RESUMABLE_UPLOAD_THRESHOLD_BYTES,
            resumable_chunk_size_bytes: super::gcs_upload::RESUMABLE_CHUNK_SIZE_BYTES,
        })
    }

    #[cfg(test)]
    fn new_for_test(
        http: reqwest::Client,
        token: impl Into<String>,
        credentials: GcsServiceAccountCredentials,
        storage_api_base_url: impl Into<String>,
    ) -> Self {
        Self {
            http,
            token: token.into(),
            storage_api_base_url: storage_api_base_url.into(),
            credentials,
            resumable_threshold_bytes: super::gcs_upload::RESUMABLE_UPLOAD_THRESHOLD_BYTES,
            resumable_chunk_size_bytes: super::gcs_upload::RESUMABLE_CHUNK_SIZE_BYTES,
        }
    }

    /// Shrink the resumable thresholds so tests can exercise the multi-chunk
    /// protocol with byte-sized payloads.
    #[cfg(test)]
    fn with_resumable_sizing(mut self, threshold: u64, chunk_size: u64) -> Self {
        self.resumable_threshold_bytes = threshold;
        self.resumable_chunk_size_bytes = chunk_size;
        self
    }

    pub async fn list_buckets(&self) -> Result<Vec<String>, SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b?project={}",
                encode_component(&self.credentials.project_id)
            ))
            .await?;
        let body: GcsBucketListResponse = parse_json_response(response, "list GCS buckets").await?;
        Ok(body.items.into_iter().map(|item| item.name).collect())
    }

    #[allow(dead_code)]
    pub async fn get_object_metadata(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<GcsObject, SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
                encode_component(bucket),
                encode_component(key)
            ))
            .await?;

        let body: GcsObjectItem = parse_json_response(
            response,
            &format!("inspect GCS object '{key}' in bucket '{bucket}'"),
        )
        .await?;

        Ok(GcsObject {
            name: body.name,
            size: body
                .size
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .unwrap_or(0),
            updated: body.updated,
            etag: body.etag,
            storage_class: body.storage_class,
            metadata: body.metadata,
        })
    }

    pub async fn get_bucket(&self, bucket: &str) -> Result<(), SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}",
                encode_component(bucket)
            ))
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(render_http_error(response, &format!("inspect GCS bucket '{bucket}'")).await)
        }
    }

    #[allow(dead_code)]
    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}",
                encode_component(bucket)
            ))
            .await?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(render_http_error(response, &format!("inspect GCS bucket '{bucket}'")).await),
        }
    }

    #[allow(dead_code)]
    pub async fn create_bucket(&self, bucket: &str, region: &str) -> Result<(), SyncError> {
        let request = GcsCreateBucketRequest {
            name: bucket,
            location: normalize_region(region),
        };

        let response = self
            .http
            .post(format!(
                "https://storage.googleapis.com/storage/v1/b?project={}",
                encode_component(&self.credentials.project_id)
            ))
            .bearer_auth(&self.token)
            .json(&request)
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!("failed to create GCS bucket '{bucket}': {error}"))
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(render_http_error(response, &format!("create GCS bucket '{bucket}'")).await)
        }
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        max_results: Option<u32>,
    ) -> Result<Vec<GcsObject>, SyncError> {
        let mut page_token: Option<String> = None;
        let mut objects = Vec::new();

        loop {
            let mut url = format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o",
                encode_component(bucket)
            );
            let mut query = Vec::new();
            if let Some(prefix) = prefix.filter(|value| !value.is_empty()) {
                query.push(format!("prefix={}", encode_component(prefix)));
            }
            if let Some(max_results) = max_results {
                query.push(format!("maxResults={max_results}"));
            }
            if let Some(token) = page_token.as_deref() {
                query.push(format!("pageToken={}", encode_component(token)));
            }
            if !query.is_empty() {
                url.push('?');
                url.push_str(&query.join("&"));
            }

            let response = self.authorized_get(&url).await?;
            let body: GcsObjectListResponse =
                parse_json_response(response, &format!("list GCS objects in bucket '{bucket}'"))
                    .await?;

            objects.extend(body.items.into_iter().map(|item| {
                GcsObject {
                    name: item.name,
                    size: item
                        .size
                        .as_deref()
                        .and_then(|value| value.parse::<u64>().ok())
                        .unwrap_or(0),
                    updated: item.updated,
                    etag: item.etag,
                    storage_class: item.storage_class,
                    metadata: item.metadata,
                }
            }));

            if let Some(next) = body.next_page_token {
                page_token = Some(next);
            } else {
                break;
            }
        }

        Ok(objects)
    }

    pub async fn download_object(
        &self,
        bucket: &str,
        key: &str,
        path: &Path,
    ) -> Result<(), SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
                encode_component(bucket),
                encode_component(key)
            ))
            .await?;

        if !response.status().is_success() {
            return Err(render_http_error(
                response,
                &format!("download '{key}' from GCS bucket '{bucket}'"),
            )
            .await);
        }

        Self::stream_response_to_path(response, key, path).await
    }

    /// Stream a GCS response body to disk through the atomic download writer,
    /// rather than buffering the whole object in memory first.
    async fn stream_response_to_path(
        mut response: reqwest::Response,
        key: &str,
        path: &Path,
    ) -> Result<(), SyncError> {
        let mut writer = DownloadWriter::create(path)?;

        loop {
            let chunk = response.chunk().await.map_err(|error| {
                SyncError::transient(format!(
                    "failed to read GCS download body for '{key}': {error}"
                ))
            })?;
            match chunk {
                Some(bytes) => writer.write_chunk(&bytes)?,
                None => break,
            }
        }

        writer.finish()?;
        Ok(())
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), SyncError> {
        let response = self
            .http
            .delete(self.storage_api_url(&format!(
                "/storage/v1/b/{}/o/{}",
                encode_component(bucket),
                encode_component(key)
            )))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!(
                    "failed to delete '{key}' from GCS bucket '{bucket}': {error}"
                ))
            })?;

        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(render_http_error(
                response,
                &format!("delete '{key}' from GCS bucket '{bucket}'"),
            )
            .await)
        }
    }

    pub async fn object_exists(&self, bucket: &str, key: &str) -> Result<bool, SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
                encode_component(bucket),
                encode_component(key)
            ))
            .await?;

        match response.status() {
            StatusCode::OK => Ok(true),
            StatusCode::NOT_FOUND => Ok(false),
            _ => Err(render_http_error(
                response,
                &format!("inspect '{key}' in GCS bucket '{bucket}'"),
            )
            .await),
        }
    }

    pub async fn move_object(
        &self,
        bucket: &str,
        from_key: &str,
        to_key: &str,
    ) -> Result<(), SyncError> {
        let base_url = self.storage_api_url(&format!(
            "/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}",
            encode_component(bucket),
            encode_component(from_key),
            encode_component(bucket),
            encode_component(to_key)
        ));
        let action = format!("move '{from_key}' to '{to_key}' in GCS bucket '{bucket}'");

        self.execute_rewrite_loop(&base_url, &action, |url| {
            let action = action.clone();
            async move {
                let response = self
                    .http
                    .post(&url)
                    .bearer_auth(&self.token)
                    .send()
                    .await
                    .map_err(|error| {
                        SyncError::transient(format!("failed to {action}: {error}"))
                    })?;

                parse_json_response(response, &action).await
            }
        })
        .await?;

        self.delete_object(bucket, from_key).await
    }

    async fn authorized_get(&self, url: &str) -> Result<reqwest::Response, SyncError> {
        self.http
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| SyncError::transient(format!("failed GCS request '{url}': {error}")))
    }

    pub async fn bucket_versioning_enabled(&self, bucket: &str) -> Result<bool, SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}",
                encode_component(bucket)
            ))
            .await?;
        let body: GcsBucketDetail =
            parse_json_response(response, &format!("get GCS bucket '{bucket}' metadata")).await?;
        Ok(body.versioning.and_then(|v| v.enabled).unwrap_or(false))
    }

    pub async fn set_bucket_versioning(
        &self,
        bucket: &str,
        enabled: bool,
    ) -> Result<(), SyncError> {
        let response = self
            .http
            .patch(format!(
                "https://storage.googleapis.com/storage/v1/b/{}",
                encode_component(bucket)
            ))
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "versioning": { "enabled": enabled } }))
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!(
                    "failed to set versioning on GCS bucket '{bucket}': {error}"
                ))
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(render_http_error(
                response,
                &format!("set versioning on GCS bucket '{bucket}'"),
            )
            .await)
        }
    }

    pub async fn get_bucket_lifecycle_configuration(
        &self,
        bucket: &str,
    ) -> Result<GcsBucketLifecycleState, SyncError> {
        let response = self
            .authorized_get(&self.storage_api_url(&format!(
                "/storage/v1/b/{}?fields=lifecycle,metageneration",
                encode_component(bucket)
            )))
            .await?;
        let body: GcsBucketDetail = parse_json_response(
            response,
            &format!("get lifecycle configuration for GCS bucket '{bucket}'"),
        )
        .await?;

        Ok(GcsBucketLifecycleState {
            configuration: body
                .lifecycle
                .filter(|configuration| !configuration.rule.is_empty()),
            metageneration: body.metageneration,
        })
    }

    pub async fn patch_bucket_lifecycle_configuration(
        &self,
        bucket: &str,
        configuration: Option<&GcsBucketLifecycleConfiguration>,
        if_metageneration_match: Option<&str>,
    ) -> Result<(), SyncError> {
        let mut url = self.storage_api_url(&format!(
            "/storage/v1/b/{}?fields=lifecycle,metageneration",
            encode_component(bucket)
        ));
        if let Some(metageneration) = if_metageneration_match.filter(|value| !value.is_empty()) {
            url.push_str(&format!(
                "&ifMetagenerationMatch={}",
                encode_component(metageneration)
            ));
        }

        let lifecycle = configuration
            .cloned()
            .unwrap_or(GcsBucketLifecycleConfiguration { rule: Vec::new() });

        let response = self
            .http
            .patch(url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "lifecycle": lifecycle }))
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!(
                    "failed to update lifecycle configuration for GCS bucket '{bucket}': {error}"
                ))
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(render_http_error(
                response,
                &format!("update lifecycle configuration for GCS bucket '{bucket}'"),
            )
            .await)
        }
    }

    pub async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        page_token: Option<&str>,
    ) -> Result<GcsObjectVersionPage, SyncError> {
        let mut url = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o?versions=true",
            encode_component(bucket)
        );
        if let Some(prefix) = prefix.filter(|v| !v.is_empty()) {
            url.push_str(&format!("&prefix={}", encode_component(prefix)));
        }
        if let Some(token) = page_token {
            url.push_str(&format!("&pageToken={}", encode_component(token)));
        }

        let response = self.authorized_get(&url).await?;
        let body: GcsVersionedObjectListResponse = parse_json_response(
            response,
            &format!("list GCS object versions in bucket '{bucket}'"),
        )
        .await?;

        Ok(GcsObjectVersionPage {
            items: body
                .items
                .into_iter()
                .map(|item| GcsObjectVersionItem {
                    name: item.name,
                    generation: item.generation,
                    size: item
                        .size
                        .as_deref()
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(0),
                    updated: item.updated,
                    etag: item.etag,
                    storage_class: item.storage_class,
                    time_deleted: item.time_deleted,
                })
                .collect(),
            next_page_token: body.next_page_token,
        })
    }

    pub async fn download_object_version(
        &self,
        bucket: &str,
        key: &str,
        generation: &str,
        path: &Path,
    ) -> Result<(), SyncError> {
        let response = self
            .authorized_get(&format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media&generation={}",
                encode_component(bucket),
                encode_component(key),
                encode_component(generation)
            ))
            .await?;

        if !response.status().is_success() {
            return Err(render_http_error(
                response,
                &format!("download '{key}' generation {generation} from GCS bucket '{bucket}'"),
            )
            .await);
        }

        Self::stream_response_to_path(response, key, path).await
    }

    pub async fn copy_object_version(
        &self,
        bucket: &str,
        key: &str,
        generation: &str,
    ) -> Result<(), SyncError> {
        let encoded_bucket = encode_component(bucket);
        let encoded_key = encode_component(key);
        let response = self
            .http
            .post(format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}?sourceGeneration={}",
                encoded_bucket, encoded_key, encoded_bucket, encoded_key, encode_component(generation)
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!("failed to restore '{key}' generation {generation} in GCS bucket '{bucket}': {error}"))
            })?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(render_http_error(
                response,
                &format!("restore '{key}' generation {generation} in GCS bucket '{bucket}'"),
            )
            .await)
        }
    }

    pub async fn rewrite_storage_class(
        &self,
        bucket: &str,
        key: &str,
        storage_class: &str,
    ) -> Result<(), SyncError> {
        let base_url = self.storage_api_url(&format!(
            "/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}",
            encode_component(bucket),
            encode_component(key),
            encode_component(bucket),
            encode_component(key)
        ));
        let body = serde_json::json!({ "storageClass": storage_class });
        let action = format!("change storage class of '{key}' in GCS bucket '{bucket}'");

        self.execute_rewrite_loop(&base_url, &action, |url| {
            let action = action.clone();
            let body = body.clone();
            async move {
                let response = self
                    .http
                    .post(&url)
                    .bearer_auth(&self.token)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|error| {
                        SyncError::transient(format!("failed to {action}: {error}"))
                    })?;

                parse_json_response(response, &action).await
            }
        })
        .await
    }

    pub async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        generation: &str,
    ) -> Result<(), SyncError> {
        let response = self
            .http
            .delete(format!(
                "https://storage.googleapis.com/storage/v1/b/{}/o/{}?generation={}",
                encode_component(bucket),
                encode_component(key),
                encode_component(generation)
            ))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| {
                SyncError::transient(format!("failed to delete '{key}' generation {generation} from GCS bucket '{bucket}': {error}"))
            })?;

        if response.status().is_success() || response.status() == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(render_http_error(
                response,
                &format!("delete '{key}' generation {generation} from GCS bucket '{bucket}'"),
            )
            .await)
        }
    }
}

impl GcsClient {
    pub(crate) fn storage_api_url(&self, path: &str) -> String {
        format!("{}{}", self.storage_api_base_url, path)
    }

    async fn execute_rewrite_loop<F, Fut>(
        &self,
        base_url: &str,
        action: &str,
        mut send_request: F,
    ) -> Result<(), SyncError>
    where
        F: FnMut(String) -> Fut,
        Fut: Future<Output = Result<GcsRewriteResponse, SyncError>>,
    {
        let mut rewrite_token: Option<String> = None;

        loop {
            let url = rewrite_request_url(base_url, rewrite_token.as_deref());
            let parsed = send_request(url).await?;

            if parsed.done {
                return Ok(());
            }

            rewrite_token = parsed.rewrite_token;
            if rewrite_token.is_none() {
                return Err(SyncError::internal(format!(
                    "failed to {action}: rewrite did not complete and no rewrite token was returned"
                )));
            }
        }
    }
}

const LEGACY_NAMESPACE_RULE_KEY: &str = "__storage_goblin_legacy_namespace__";
const REMOTE_BIN_PAIRS_PREFIX: &str = ".storage-goblin-bin/pairs/";

pub fn reconcile_managed_lifecycle_rules(
    existing: Option<&[GcsLifecycleRule]>,
    managed_rules: &[ManagedLifecycleRulePlan],
) -> GcsLifecycleRulesChange {
    let existing_rules = existing.unwrap_or(&[]);

    if managed_rules.is_empty() {
        let cleaned = remove_managed_lifecycle_rules(existing_rules);
        if cleaned == existing_rules {
            GcsLifecycleRulesChange::None
        } else if cleaned.is_empty() {
            GcsLifecycleRulesChange::DeleteBucketLifecycle
        } else {
            GcsLifecycleRulesChange::Replace(cleaned)
        }
    } else {
        let merged = upsert_managed_lifecycle_rules(existing_rules, managed_rules);
        if merged == existing_rules {
            GcsLifecycleRulesChange::None
        } else {
            GcsLifecycleRulesChange::Replace(merged)
        }
    }
}

fn upsert_managed_lifecycle_rules(
    existing: &[GcsLifecycleRule],
    managed_rules: &[ManagedLifecycleRulePlan],
) -> Vec<GcsLifecycleRule> {
    let mut desired_rules = managed_rules
        .iter()
        .map(|plan| (plan.pair_id.clone(), managed_lifecycle_rule(plan)))
        .collect::<BTreeMap<_, _>>();
    let mut updated = Vec::with_capacity(existing.len() + desired_rules.len());

    for rule in existing {
        if let Some(rule_key) = managed_rule_key_for_existing_rule(rule) {
            if let Some(desired_rule) = desired_rules.remove(&rule_key) {
                updated.push(desired_rule);
            }
        } else {
            updated.push(rule.clone());
        }
    }

    updated.extend(desired_rules.into_values());
    updated
}

fn remove_managed_lifecycle_rules(existing: &[GcsLifecycleRule]) -> Vec<GcsLifecycleRule> {
    existing
        .iter()
        .filter(|rule| managed_rule_key_for_existing_rule(rule).is_none())
        .cloned()
        .collect()
}

fn managed_lifecycle_rule(plan: &ManagedLifecycleRulePlan) -> GcsLifecycleRule {
    GcsLifecycleRule {
        action: GcsLifecycleAction {
            action_type: "Delete".into(),
            extra: BTreeMap::new(),
        },
        condition: GcsLifecycleCondition {
            age: Some(plan.retention_days),
            matches_prefix: vec![plan.prefix.clone()],
            extra: BTreeMap::new(),
        },
    }
}

fn managed_rule_key_for_existing_rule(rule: &GcsLifecycleRule) -> Option<String> {
    if rule.action.action_type != "Delete"
        || !rule.action.extra.is_empty()
        || rule.condition.age.is_none()
        || !rule.condition.extra.is_empty()
    {
        return None;
    }

    match rule.condition.matches_prefix.as_slice() {
        [prefix] if prefix == &namespace_prefix() => Some(LEGACY_NAMESPACE_RULE_KEY.into()),
        [prefix] => pair_id_from_managed_prefix(prefix).map(str::to_string),
        _ => None,
    }
}

fn pair_id_from_managed_prefix(prefix: &str) -> Option<&str> {
    prefix
        .strip_prefix(REMOTE_BIN_PAIRS_PREFIX)?
        .strip_suffix('/')
        .filter(|pair_id| !pair_id.is_empty())
}

async fn fetch_access_token(
    http: &reqwest::Client,
    credentials: &GcsServiceAccountCredentials,
) -> Result<String, SyncError> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("failed to resolve current time for GCS auth: {error}"))?
        .as_secs();
    let claims = ServiceAccountJwtClaims {
        iss: &credentials.client_email,
        scope: STORAGE_SCOPE,
        aud: &credentials.token_uri,
        exp: now + 3600,
        iat: now,
    };

    let mut header = Header::new(Algorithm::RS256);
    header.kid = credentials.private_key_id.clone();
    let assertion = encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(credentials.private_key.as_bytes())
            .map_err(|error| format!("failed to parse GCS private key: {error}"))?,
    )
    .map_err(|error| format!("failed to sign GCS access token request: {error}"))?;

    let response = http
        .post(&credentials.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion.as_str()),
        ])
        .send()
        .await
        .map_err(|error| {
            SyncError::transient(format!("failed to request GCS access token: {error}"))
        })?;

    let token: GcsTokenResponse = parse_json_response(response, "request GCS access token").await?;
    Ok(token.access_token)
}

async fn parse_json_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
    action: &str,
) -> Result<T, SyncError> {
    if !response.status().is_success() {
        return Err(render_http_error(response, action).await);
    }

    response.json::<T>().await.map_err(|error| {
        SyncError::internal(format!("failed to parse response for {action}: {error}"))
    })
}

pub(crate) async fn render_http_error(response: reqwest::Response, action: &str) -> SyncError {
    let status = response.status();
    let retry_after_seconds = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());
    let body = sanitize_sensitive_text(response.text().await.unwrap_or_default());
    let message = if body.trim().is_empty() {
        format!("failed to {action}: HTTP {status}")
    } else {
        format!("failed to {action}: HTTP {status} {body}")
    };
    SyncError::from_http_status(status.as_u16(), message).with_retry_after(retry_after_seconds)
}

#[allow(dead_code)]
fn normalize_region(region: &str) -> Option<String> {
    let value = region.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_ascii_uppercase())
    }
}

pub(crate) fn encode_component(value: &str) -> String {
    utf8_percent_encode(value, ENCODE_SET).to_string()
}

fn rewrite_request_url(base_url: &str, rewrite_token: Option<&str>) -> String {
    match rewrite_token {
        Some(token) => format!("{}?rewriteToken={}", base_url, encode_component(token)),
        None => base_url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        compact_service_account_json, reconcile_managed_lifecycle_rules,
        GcsBucketLifecycleConfiguration, GcsClient, GcsLifecycleRule, GcsLifecycleRulesChange,
        GcsServiceAccountCredentials,
    };
    use crate::storage::remote_bin::managed_lifecycle_rule_plan;
    use serde_json::Value;
    use std::{
        collections::BTreeMap,
        io::{Read, Write},
        net::{TcpListener, TcpStream},
        sync::{Arc, Mutex},
        thread,
        time::Duration,
    };

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RecordedRequest {
        method: String,
        path: String,
        authorization: Option<String>,
        body: String,
        content_range: Option<String>,
    }

    #[derive(Debug, Clone)]
    struct TestResponse {
        status_code: u16,
        body: String,
        headers: Vec<(String, String)>,
    }

    impl TestResponse {
        fn new(status_code: u16, body: &str) -> Self {
            Self {
                status_code,
                body: body.to_string(),
                headers: Vec::new(),
            }
        }

        fn with_header(mut self, name: &str, value: &str) -> Self {
            self.headers.push((name.to_string(), value.to_string()));
            self
        }
    }

    struct TestServer {
        base_url: String,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        handle: thread::JoinHandle<()>,
    }

    impl TestServer {
        fn spawn(responses: Vec<TestResponse>) -> Self {
            Self::spawn_with(|_| responses)
        }

        /// Bind first, then let the caller build responses that reference the
        /// bound address (a resumable session URI must point back here).
        fn spawn_with(build: impl FnOnce(&str) -> Vec<TestResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("test server should bind");
            let base_url = format!(
                "http://{}",
                listener.local_addr().expect("address should resolve")
            );
            let responses = build(&base_url);
            let requests = Arc::new(Mutex::new(Vec::new()));
            let captured_requests = Arc::clone(&requests);

            let handle = thread::spawn(move || {
                for response in responses {
                    let (mut stream, _) = listener.accept().expect("request should arrive");
                    let request = read_http_request(&mut stream);
                    captured_requests
                        .lock()
                        .expect("requests lock should succeed")
                        .push(request);
                    write_http_response(&mut stream, &response);
                }
            });

            Self {
                base_url,
                requests,
                handle,
            }
        }

        fn finish(self) -> Vec<RecordedRequest> {
            self.handle.join().expect("server thread should finish");
            self.requests
                .lock()
                .expect("requests lock should succeed")
                .clone()
        }
    }

    fn write_http_response(stream: &mut TcpStream, response: &TestResponse) {
        let status_code = response.status_code;
        let body = response.body.as_str();
        let reason = match status_code {
            200 => "OK",
            204 => "No Content",
            308 => "Resume Incomplete",
            400 => "Bad Request",
            _ => "OK",
        };

        let extra = response
            .headers
            .iter()
            .map(|(name, value)| format!("{name}: {value}\r\n"))
            .collect::<String>();

        let response = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n{}Connection: close\r\n\r\n{}",
            status_code,
            reason,
            body.len(),
            extra,
            body
        );
        stream
            .write_all(response.as_bytes())
            .expect("response should write");
        stream.flush().expect("response should flush");
    }

    fn read_http_request(stream: &mut TcpStream) -> RecordedRequest {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("timeout should set");

        let mut buffer = Vec::new();
        let mut chunk = [0_u8; 1024];
        let header_end = loop {
            let bytes_read = stream.read(&mut chunk).expect("request should read");
            assert!(bytes_read > 0, "request should include headers");
            buffer.extend_from_slice(&chunk[..bytes_read]);

            if let Some(index) = find_bytes(&buffer, b"\r\n\r\n") {
                break index + 4;
            }
        };

        let header_text = String::from_utf8(buffer[..header_end].to_vec())
            .expect("headers should be valid utf-8");
        let mut lines = header_text.split("\r\n").filter(|line| !line.is_empty());
        let request_line = lines.next().expect("request line should exist");
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .expect("method should exist")
            .to_string();
        let path = request_parts.next().expect("path should exist").to_string();

        let mut authorization = None;
        let mut content_range = None;
        let mut content_length = 0usize;
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                let header_name = name.trim();
                let header_value = value.trim().to_string();
                if header_name.eq_ignore_ascii_case("authorization") {
                    authorization = Some(header_value.clone());
                }
                if header_name.eq_ignore_ascii_case("content-range") {
                    content_range = Some(header_value.clone());
                }
                if header_name.eq_ignore_ascii_case("content-length") {
                    content_length = header_value
                        .parse::<usize>()
                        .expect("content length should parse");
                }
            }
        }

        let mut body = buffer[header_end..].to_vec();
        while body.len() < content_length {
            let bytes_read = stream.read(&mut chunk).expect("request body should read");
            assert!(bytes_read > 0, "request body should be complete");
            body.extend_from_slice(&chunk[..bytes_read]);
        }
        body.truncate(content_length);

        RecordedRequest {
            method,
            path,
            authorization,
            body: String::from_utf8_lossy(&body).into_owned(),
            content_range,
        }
    }

    fn resumable_temp_file(name: &str, size: usize) -> std::path::PathBuf {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time should be after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "storage-goblin-gcs-upload-{name}-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("temp dir should create");
        let path = dir.join("payload.bin");
        // Deterministic ASCII so the recorded request bodies are readable and
        // a mis-ordered or dropped chunk is obvious.
        let payload: Vec<u8> = (0..size).map(|index| b'a' + (index % 26) as u8).collect();
        std::fs::write(&path, &payload).expect("payload should write");
        path
    }

    fn test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
    }

    #[test]
    fn large_uploads_use_a_resumable_session_and_send_every_byte_in_order() {
        // 10 bytes with a 4-byte chunk: two full chunks plus a short final one.
        let total = 10_usize;
        let path = resumable_temp_file("resumable", total);

        let server = TestServer::spawn_with(|base_url| {
            vec![
                // Session initiation answers with the session URI, which must
                // point back here so the chunks land on this server.
                TestResponse::new(200, "")
                    .with_header("Location", &format!("{base_url}/resumable-session/abc")),
                TestResponse::new(308, ""),
                TestResponse::new(308, ""),
                TestResponse::new(200, "{}"),
            ]
        });
        let client = test_client(&server.base_url).with_resumable_sizing(8, 4);

        let outcome = test_runtime().block_on(client.upload_object(
            "demo-bucket",
            "big.bin",
            &path,
            Some(std::collections::HashMap::from([(
                "goblin".to_string(),
                "fingerprint".to_string(),
            )])),
        ));

        let requests = server.finish();
        outcome.expect("resumable upload should succeed");

        assert_eq!(requests.len(), 4, "initiate + three chunks");

        // Session initiation carries the object metadata.
        assert_eq!(requests[0].method, "POST");
        assert!(
            requests[0].path.contains("uploadType=resumable"),
            "expected a resumable initiation, got {}",
            requests[0].path
        );
        assert!(
            requests[0].body.contains("fingerprint"),
            "object metadata should be sent when the session opens"
        );

        // Chunks are PUTs with contiguous, correctly framed Content-Ranges.
        let ranges: Vec<Option<&str>> = requests[1..]
            .iter()
            .map(|request| request.content_range.as_deref())
            .collect();
        assert_eq!(
            ranges,
            vec![
                Some("bytes 0-3/10"),
                Some("bytes 4-7/10"),
                Some("bytes 8-9/10"),
            ]
        );
        assert!(requests[1..].iter().all(|request| request.method == "PUT"));

        // Every byte arrived exactly once, in order.
        let sent: String = requests[1..]
            .iter()
            .map(|request| request.body.clone())
            .collect();
        assert_eq!(sent, "abcdefghij");

        let _ = std::fs::remove_dir_all(path.parent().expect("temp dir"));
    }

    #[test]
    fn small_uploads_stay_on_the_single_request_path() {
        let path = resumable_temp_file("small", 4);
        let server = TestServer::spawn(vec![TestResponse::new(200, "{}")]);
        let client = test_client(&server.base_url).with_resumable_sizing(8, 4);

        let outcome =
            test_runtime().block_on(client.upload_object("demo-bucket", "small.bin", &path, None));

        let requests = server.finish();
        outcome.expect("small upload should succeed");

        assert_eq!(requests.len(), 1, "a small object needs one request");
        assert!(
            requests[0].path.contains("uploadType=multipart"),
            "small objects should not open a resumable session, got {}",
            requests[0].path
        );

        let _ = std::fs::remove_dir_all(path.parent().expect("temp dir"));
    }

    #[test]
    fn a_rejected_chunk_fails_the_upload_with_the_provider_message() {
        let path = resumable_temp_file("chunk-fail", 10);

        let server = TestServer::spawn_with(|base_url| {
            vec![
                TestResponse::new(200, "")
                    .with_header("Location", &format!("{base_url}/resumable-session/abc")),
                TestResponse::new(400, r#"{"error":{"message":"bad chunk"}}"#),
            ]
        });
        let client = test_client(&server.base_url).with_resumable_sizing(8, 4);

        let outcome =
            test_runtime().block_on(client.upload_object("demo-bucket", "big.bin", &path, None));

        let _ = server.finish();
        let error = outcome.expect_err("a rejected chunk should fail the upload");
        assert!(
            error.message.contains("upload 'big.bin'"),
            "unexpected error: {}",
            error.message
        );

        let _ = std::fs::remove_dir_all(path.parent().expect("temp dir"));
    }

    #[test]
    fn a_session_without_a_location_header_is_an_error_not_a_hang() {
        let path = resumable_temp_file("no-location", 10);
        let server = TestServer::spawn(vec![TestResponse::new(200, "{}")]);
        let client = test_client(&server.base_url).with_resumable_sizing(8, 4);

        let outcome =
            test_runtime().block_on(client.upload_object("demo-bucket", "big.bin", &path, None));

        let _ = server.finish();
        let error = outcome.expect_err("a session with no location should fail");
        assert!(
            error.message.contains("no session location"),
            "unexpected error: {}",
            error.message
        );

        let _ = std::fs::remove_dir_all(path.parent().expect("temp dir"));
    }

    fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    fn test_credentials() -> GcsServiceAccountCredentials {
        GcsServiceAccountCredentials {
            project_id: "demo-project".into(),
            client_email: "demo@example.com".into(),
            private_key: "test-private-key".into(),
            private_key_id: Some("kid-1".into()),
            token_uri: "https://oauth2.googleapis.com/token".into(),
        }
    }

    fn test_client(base_url: &str) -> GcsClient {
        GcsClient::new_for_test(
            reqwest::Client::builder()
                .build()
                .expect("test client should build"),
            "test-token",
            test_credentials(),
            base_url.to_string(),
        )
    }

    #[test]
    fn parses_service_account_json() {
        let credentials = GcsServiceAccountCredentials::from_json(
            r#"{
                "type":"service_account",
                "project_id":"demo-project",
                "private_key_id":"kid-1",
                "private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
                "client_email":"demo@example.com",
                "token_uri":"https://oauth2.googleapis.com/token"
            }"#,
        )
        .expect("service account JSON should parse");

        assert_eq!(credentials.project_id, "demo-project");
        assert_eq!(credentials.client_email, "demo@example.com");
        assert_eq!(credentials.private_key_id.as_deref(), Some("kid-1"));
    }

    #[test]
    fn rejects_missing_required_fields() {
        let error = GcsServiceAccountCredentials::from_json("{}")
            .expect_err("missing required service account fields should fail");

        assert!(error.message.contains("project_id") || error.message.contains("client_email"));
    }

    #[test]
    fn compacts_service_account_json_to_canonical_subset() {
        let compact = compact_service_account_json(
            r#"{
                "type":"service_account",
                "project_id":"demo-project",
                "private_key_id":"kid-1",
                "private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
                "client_email":"demo@example.com",
                "token_uri":"https://oauth2.googleapis.com/token",
                "auth_uri":"https://accounts.google.com/o/oauth2/auth",
                "client_id":"1234567890",
                "client_x509_cert_url":"https://example.com/cert"
            }"#,
        )
        .expect("service account JSON should compact");

        assert_eq!(
            compact,
            r#"{"type":"service_account","project_id":"demo-project","client_email":"demo@example.com","private_key":"-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n","private_key_id":"kid-1"}"#
        );

        let reparsed = GcsServiceAccountCredentials::from_json(&compact)
            .expect("canonical service account JSON should parse");
        assert_eq!(reparsed.project_id, "demo-project");
        assert_eq!(reparsed.client_email, "demo@example.com");
        assert_eq!(reparsed.private_key_id.as_deref(), Some("kid-1"));
        assert_eq!(reparsed.token_uri, "https://oauth2.googleapis.com/token");
    }

    #[test]
    fn gcs_object_version_item_maps_generation_as_version_id() {
        let item = super::GcsObjectVersionItem {
            name: "test.txt".into(),
            generation: "1234567890123456".into(),
            size: 42,
            updated: Some("2026-04-25T10:00:00Z".into()),
            etag: Some("etag".into()),
            storage_class: Some("STANDARD".into()),
            time_deleted: None,
        };
        assert_eq!(item.generation, "1234567890123456");
        assert!(item.time_deleted.is_none()); // current version
    }

    #[test]
    fn move_object_retries_rewrite_until_done_then_deletes_source() {
        let server = TestServer::spawn(vec![
            TestResponse::new(200, r#"{"done":false,"rewriteToken":"token-2"}"#),
            TestResponse::new(200, r#"{"done":true}"#),
            TestResponse::new(204, ""),
        ]);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        runtime.block_on(async {
            test_client(&server.base_url)
                .move_object("demo-bucket", "folder/source.txt", "folder/dest.txt")
                .await
                .expect("move should complete");
        });

        let requests = server.finish();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0].path,
            "/storage/v1/b/demo-bucket/o/folder%2Fsource.txt/rewriteTo/b/demo-bucket/o/folder%2Fdest.txt"
        );
        assert_eq!(
            requests[0].authorization.as_deref(),
            Some("Bearer test-token")
        );
        assert!(requests[0].body.is_empty());

        assert_eq!(requests[1].method, "POST");
        assert_eq!(
            requests[1].path,
            "/storage/v1/b/demo-bucket/o/folder%2Fsource.txt/rewriteTo/b/demo-bucket/o/folder%2Fdest.txt?rewriteToken=token-2"
        );
        assert!(requests[1].body.is_empty());

        assert_eq!(requests[2].method, "DELETE");
        assert_eq!(
            requests[2].path,
            "/storage/v1/b/demo-bucket/o/folder%2Fsource.txt"
        );
    }

    #[test]
    fn rewrite_storage_class_retries_rewrite_until_done() {
        let server = TestServer::spawn(vec![
            TestResponse::new(200, r#"{"done":false,"rewriteToken":"token-2"}"#),
            TestResponse::new(200, r#"{"done":true}"#),
        ]);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        runtime.block_on(async {
            test_client(&server.base_url)
                .rewrite_storage_class("demo-bucket", "folder/object.txt", "COLDLINE")
                .await
                .expect("storage class rewrite should complete");
        });

        let requests = server.finish();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, "POST");
        assert_eq!(
            requests[0].path,
            "/storage/v1/b/demo-bucket/o/folder%2Fobject.txt/rewriteTo/b/demo-bucket/o/folder%2Fobject.txt"
        );
        assert_eq!(requests[1].method, "POST");
        assert_eq!(
            requests[1].path,
            "/storage/v1/b/demo-bucket/o/folder%2Fobject.txt/rewriteTo/b/demo-bucket/o/folder%2Fobject.txt?rewriteToken=token-2"
        );

        let first_body: Value = serde_json::from_str(&requests[0].body)
            .expect("first rewrite request body should be valid json");
        let second_body: Value = serde_json::from_str(&requests[1].body)
            .expect("second rewrite request body should be valid json");
        assert_eq!(
            first_body,
            serde_json::json!({ "storageClass": "COLDLINE" })
        );
        assert_eq!(
            second_body,
            serde_json::json!({ "storageClass": "COLDLINE" })
        );
    }

    #[test]
    fn rewrite_loop_fails_when_incomplete_response_has_no_token() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        runtime.block_on(async {
            let client = test_client("http://127.0.0.1:1");
            let error = client
                .execute_rewrite_loop(
                    "http://example.test/rewrite",
                    "move test object",
                    |_| async {
                        Ok(super::GcsRewriteResponse {
                            done: false,
                            rewrite_token: None,
                        })
                    },
                )
                .await
                .expect_err("missing rewrite token should fail");

            assert!(error
                .message
                .contains("rewrite did not complete and no rewrite token was returned"));
        });
    }

    #[test]
    fn gets_bucket_lifecycle_configuration_and_tracks_metageneration() {
        let server = TestServer::spawn(vec![TestResponse::new(
            200,
            r#"{"metageneration":"12","lifecycle":{"rule":[{"action":{"type":"SetStorageClass","storageClass":"ARCHIVE"},"condition":{"age":60,"matchesPrefix":["archive/"]}}]}}"#,
        )]);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        let state = runtime.block_on(async {
            test_client(&server.base_url)
                .get_bucket_lifecycle_configuration("demo-bucket")
                .await
                .expect("lifecycle get should succeed")
        });

        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "GET");
        assert_eq!(
            requests[0].path,
            "/storage/v1/b/demo-bucket?fields=lifecycle,metageneration"
        );
        assert_eq!(state.metageneration.as_deref(), Some("12"));
        assert_eq!(
            state
                .configuration
                .expect("configuration should exist")
                .rule
                .len(),
            1
        );
    }

    #[test]
    fn patches_bucket_lifecycle_configuration_with_metageneration_match() {
        let server = TestServer::spawn(vec![TestResponse::new(
            200,
            r#"{"metageneration":"13","lifecycle":{"rule":[{"action":{"type":"Delete"},"condition":{"age":7,"matchesPrefix":[".storage-goblin-bin/pairs/pair-1/"]}}]}}"#,
        )]);

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build");

        runtime.block_on(async {
            test_client(&server.base_url)
                .patch_bucket_lifecycle_configuration(
                    "demo-bucket",
                    Some(&GcsBucketLifecycleConfiguration {
                        rule: vec![GcsLifecycleRule {
                            action: super::GcsLifecycleAction {
                                action_type: "Delete".into(),
                                extra: BTreeMap::new(),
                            },
                            condition: super::GcsLifecycleCondition {
                                age: Some(7),
                                matches_prefix: vec![".storage-goblin-bin/pairs/pair-1/".into()],
                                extra: BTreeMap::new(),
                            },
                        }],
                    }),
                    Some("12"),
                )
                .await
                .expect("lifecycle patch should succeed");
        });

        let requests = server.finish();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "PATCH");
        assert_eq!(
            requests[0].path,
            "/storage/v1/b/demo-bucket?fields=lifecycle,metageneration&ifMetagenerationMatch=12"
        );
        let body: Value = serde_json::from_str(&requests[0].body)
            .expect("patch request body should be valid json");
        assert_eq!(
            body,
            serde_json::json!({
                "lifecycle": {
                    "rule": [{
                        "action": { "type": "Delete" },
                        "condition": {
                            "age": 7,
                            "matchesPrefix": [".storage-goblin-bin/pairs/pair-1/"]
                        }
                    }]
                }
            })
        );
    }

    #[test]
    fn lifecycle_reconcile_preserves_unrelated_rules_for_gcs() {
        let existing = vec![GcsLifecycleRule {
            action: super::GcsLifecycleAction {
                action_type: "SetStorageClass".into(),
                extra: BTreeMap::from([("storageClass".into(), serde_json::json!("ARCHIVE"))]),
            },
            condition: super::GcsLifecycleCondition {
                age: Some(60),
                matches_prefix: vec!["archive/".into()],
                extra: BTreeMap::new(),
            },
        }];

        let GcsLifecycleRulesChange::Replace(updated) = reconcile_managed_lifecycle_rules(
            Some(&existing),
            &[managed_lifecycle_rule_plan("pair-1", 7)],
        ) else {
            panic!("gcs lifecycle rules should be replaced");
        };

        assert_eq!(updated.len(), 2);
        assert_eq!(updated[0], existing[0]);
        assert_eq!(updated[1].action.action_type, "Delete");
        assert_eq!(updated[1].condition.age, Some(7));
        assert_eq!(
            updated[1].condition.matches_prefix,
            vec![".storage-goblin-bin/pairs/pair-1/".to_string()]
        );
    }

    #[test]
    fn lifecycle_reconcile_deletes_last_managed_gcs_rule() {
        let existing = vec![GcsLifecycleRule {
            action: super::GcsLifecycleAction {
                action_type: "Delete".into(),
                extra: BTreeMap::new(),
            },
            condition: super::GcsLifecycleCondition {
                age: Some(7),
                matches_prefix: vec![".storage-goblin-bin/pairs/pair-1/".into()],
                extra: BTreeMap::new(),
            },
        }];

        assert_eq!(
            reconcile_managed_lifecycle_rules(Some(&existing), &[]),
            GcsLifecycleRulesChange::DeleteBucketLifecycle
        );
    }
}
