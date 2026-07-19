use std::{collections::{BTreeMap, HashMap}, path::Path};

use serde::{Deserialize, Serialize};

use super::{
    credentials_store::StoredCredentials,
    gcs_adapter::{
        self, GcsBucketLifecycleConfiguration, GcsClient,
        GcsLifecycleRulesChange, GcsObjectVersionPage, GcsServiceAccountCredentials,
    },
    now_iso,
    provider::{
        normalize_provider, provider_capabilities, runtime_provider_capabilities, AWS_PROVIDER,
        GCS_PROVIDER,
    },
    remote_bin::{reconcile_lifecycle_rules, LifecycleRulesChange, ManagedLifecycleRulePlan},
    s3_adapter,
    sanitizer::sanitize_sensitive_text,
};

use aws_sdk_s3::types::{BucketLifecycleConfiguration, TransitionDefaultMinimumObjectSize};

#[derive(Debug, Clone)]
pub struct StorageConnectionConfig {
    pub provider: String,
    pub region: String,
    pub bucket: String,
    pub credentials: StoredCredentials,
}

#[derive(Debug, Clone)]
pub struct StorageCredentialTestConfig {
    pub provider: String,
    pub region: String,
    pub credentials: StoredCredentials,
    pub bucket: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialTestSummary {
    pub checked_at: String,
    pub bucket_count: usize,
    pub buckets: Vec<String>,
    pub bucket_scoped: bool,
}

fn summarize_gcs_credential_validation(
    configured_bucket: Option<&str>,
    listed_buckets: Result<Vec<String>, String>,
    bucket_probe: Option<Result<(), String>>,
) -> Result<CredentialTestSummary, String> {
    match listed_buckets {
        Ok(buckets) => Ok(CredentialTestSummary {
            checked_at: now_iso(),
            bucket_count: buckets.len(),
            buckets,
            bucket_scoped: false,
        }),
        Err(error) => {
            let Some(bucket) = configured_bucket
                .map(str::trim)
                .filter(|bucket| !bucket.is_empty())
            else {
                return Err(error);
            };
            match bucket_probe
                .unwrap_or_else(|| Err("Configured bucket probe was not attempted.".into()))
            {
                Ok(()) => Ok(CredentialTestSummary {
                    checked_at: now_iso(),
                    bucket_count: 1,
                    buckets: vec![bucket.to_string()],
                    bucket_scoped: true,
                }),
                Err(bucket_error) => Err(sanitize_sensitive_text(format!(
                    "Failed to validate access to configured bucket '{bucket}': {bucket_error}"
                ))),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionProbeResult {
    pub name: String,
    pub allowed: bool,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionProbeSummary {
    pub checked_at: String,
    pub bucket: String,
    pub probes: Vec<PermissionProbeResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ObjectMetadata {
    pub key: String,
    pub size: u64,
    pub last_modified_at: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationSummary {
    pub checked_at: String,
    pub bucket: String,
    pub object_count_sampled: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketEnsureSummary {
    pub checked_at: String,
    pub bucket: String,
    pub created: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub last_modified_at: Option<String>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectVersionEntry {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub size: u64,
    pub last_modified_at: Option<String>,
    pub storage_class: Option<String>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteMarkerEntry {
    pub key: String,
    pub version_id: String,
    pub is_latest: bool,
    pub last_modified_at: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectVersionPage {
    pub versions: Vec<ObjectVersionEntry>,
    pub delete_markers: Vec<DeleteMarkerEntry>,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<String>,
    pub truncated: bool,
}

#[derive(Debug, Clone)]
pub enum ObjectStoreClient {
    Aws(aws_sdk_s3::Client),
    Gcs(GcsClient),
}

#[derive(Debug, Clone)]
pub enum BucketLifecycleConfigurationState {
    Aws {
        configuration: Option<BucketLifecycleConfiguration>,
        transition_default_minimum_object_size: Option<TransitionDefaultMinimumObjectSize>,
    },
    Gcs {
        configuration: Option<GcsBucketLifecycleConfiguration>,
        metageneration: Option<String>,
    },
}

#[allow(dead_code)]
fn provider_for_client(client: &ObjectStoreClient) -> &'static str {
    match client {
        ObjectStoreClient::Aws(_) => "aws",
        ObjectStoreClient::Gcs(_) => GCS_PROVIDER,
    }
}

#[allow(dead_code)]
fn runtime_capability_message(provider: &str, capability: &str, fallback: &str) -> String {
    let runtime = runtime_provider_capabilities(provider);
    match capability {
        "object-versioning" => runtime.object_versioning.message,
        "storage-class" => runtime.archive_storage.message,
        _ => None,
    }
    .unwrap_or_else(|| fallback.to_string())
}

fn map_s3_object_version_page(page: s3_adapter::ObjectVersionPage) -> ObjectVersionPage {
    ObjectVersionPage {
        versions: page
            .versions
            .into_iter()
            .map(|entry| ObjectVersionEntry {
                key: entry.key,
                version_id: entry.version_id,
                is_latest: entry.is_latest,
                size: entry.size,
                last_modified_at: entry.last_modified_at,
                storage_class: entry.storage_class,
                etag: entry.etag,
            })
            .collect(),
        delete_markers: page
            .delete_markers
            .into_iter()
            .map(|entry| DeleteMarkerEntry {
                key: entry.key,
                version_id: entry.version_id,
                is_latest: entry.is_latest,
                last_modified_at: entry.last_modified_at,
            })
            .collect(),
        next_key_marker: page.next_key_marker,
        next_version_id_marker: page.next_version_id_marker,
        truncated: page.truncated,
    }
}

fn map_gcs_object_version_page(page: GcsObjectVersionPage) -> ObjectVersionPage {
    let next_key_marker = page.next_page_token;
    let truncated = next_key_marker.is_some();
    ObjectVersionPage {
        versions: page
            .items
            .into_iter()
            .map(|item| ObjectVersionEntry {
                key: item.name,
                version_id: item.generation,
                is_latest: item.time_deleted.is_none(),
                size: item.size,
                last_modified_at: item.updated,
                storage_class: item.storage_class,
                etag: item.etag,
            })
            .collect(),
        delete_markers: vec![],
        next_key_marker,
        next_version_id_marker: None,
        truncated,
    }
}

pub async fn build_client(config: &StorageConnectionConfig) -> Result<ObjectStoreClient, String> {
    match normalize_provider(&config.provider).as_str() {
        GCS_PROVIDER => {
            let raw = config
                .credentials
                .gcs_service_account_json()
                .ok_or_else(|| {
                    "GCS service account JSON is required for GCS access.".to_string()
                })?;
            let credentials = GcsServiceAccountCredentials::from_json(raw)?;
            Ok(ObjectStoreClient::Gcs(GcsClient::new(&credentials).await?))
        }
        _ => Ok(ObjectStoreClient::Aws(
            s3_adapter::build_client(&s3_adapter::S3ConnectionConfig {
                provider: normalize_provider(&config.provider),
                region: config.region.clone(),
                bucket: config.bucket.clone(),
                access_key_id: config.credentials.access_key_id.clone(),
                secret_access_key: config.credentials.secret_access_key.clone(),
            })
            .await?,
        )),
    }
}

pub async fn validate_credentials(
    config: &StorageCredentialTestConfig,
) -> Result<CredentialTestSummary, String> {
    match normalize_provider(&config.provider).as_str() {
        GCS_PROVIDER => {
            let client = build_client(&StorageConnectionConfig {
                provider: config.provider.clone(),
                region: config.region.clone(),
                bucket: String::new(),
                credentials: config.credentials.clone(),
            })
            .await?;
            let ObjectStoreClient::Gcs(client) = client else {
                unreachable!()
            };
            let listed_buckets = client.list_buckets().await;
            let bucket_probe = if listed_buckets.is_err() {
                if let Some(bucket) = config
                    .bucket
                    .as_deref()
                    .map(str::trim)
                    .filter(|bucket| !bucket.is_empty())
                {
                    Some(client.get_bucket(bucket).await)
                } else {
                    None
                }
            } else {
                None
            };
            summarize_gcs_credential_validation(
                config.bucket.as_deref(),
                listed_buckets,
                bucket_probe,
            )
        }
        _ => s3_adapter::validate_credentials(&s3_adapter::S3CredentialTestConfig {
            provider: normalize_provider(&config.provider),
            region: config.region.clone(),
            access_key_id: config.credentials.access_key_id.clone(),
            secret_access_key: config.credentials.secret_access_key.clone(),
        })
        .await
        .map(|summary| CredentialTestSummary {
            checked_at: summary.checked_at,
            bucket_count: summary.bucket_count,
            buckets: summary.buckets,
            bucket_scoped: false,
        }),
    }
}

pub async fn probe_bucket_permissions(
    config: &StorageCredentialTestConfig,
    bucket: &str,
) -> PermissionProbeSummary {
    match normalize_provider(&config.provider).as_str() {
        GCS_PROVIDER => probe_gcs_bucket_permissions(config, bucket).await,
        _ => {
            let summary = s3_adapter::probe_bucket_permissions(
                &s3_adapter::S3CredentialTestConfig {
                    provider: normalize_provider(&config.provider),
                    region: config.region.clone(),
                    access_key_id: config.credentials.access_key_id.clone(),
                    secret_access_key: config.credentials.secret_access_key.clone(),
                },
                bucket,
            )
            .await;
            PermissionProbeSummary {
                checked_at: summary.checked_at,
                bucket: summary.bucket,
                probes: summary
                    .probes
                    .into_iter()
                    .map(|probe| PermissionProbeResult {
                        name: probe.name,
                        allowed: probe.allowed,
                        message: probe.message,
                    })
                    .collect(),
            }
        }
    }
}

pub async fn validate_connection(
    config: &StorageConnectionConfig,
) -> Result<ValidationSummary, String> {
    match normalize_provider(&config.provider).as_str() {
        GCS_PROVIDER => {
            let client = build_client(config).await?;
            let ObjectStoreClient::Gcs(client) = client else {
                unreachable!()
            };
            client.get_bucket(&config.bucket).await?;
            let count = client
                .list_objects(&config.bucket, None, Some(1))
                .await?
                .len();
            Ok(ValidationSummary {
                checked_at: now_iso(),
                bucket: config.bucket.trim().to_string(),
                object_count_sampled: count,
            })
        }
        _ => s3_adapter::validate_connection(&s3_adapter::S3ConnectionConfig {
            provider: normalize_provider(&config.provider),
            region: config.region.clone(),
            bucket: config.bucket.clone(),
            access_key_id: config.credentials.access_key_id.clone(),
            secret_access_key: config.credentials.secret_access_key.clone(),
        })
        .await
        .map(|summary| ValidationSummary {
            checked_at: summary.checked_at,
            bucket: summary.bucket,
            object_count_sampled: summary.object_count_sampled,
        }),
    }
}

pub async fn ensure_bucket_exists(
    config: &StorageConnectionConfig,
) -> Result<BucketEnsureSummary, String> {
    match normalize_provider(&config.provider).as_str() {
        GCS_PROVIDER => {
            let client = build_client(config).await?;
            let ObjectStoreClient::Gcs(client) = client else {
                unreachable!()
            };
            if client.bucket_exists(&config.bucket).await? {
                return Ok(BucketEnsureSummary {
                    checked_at: now_iso(),
                    bucket: config.bucket.clone(),
                    created: false,
                });
            }
            client.create_bucket(&config.bucket, &config.region).await?;
            Ok(BucketEnsureSummary {
                checked_at: now_iso(),
                bucket: config.bucket.clone(),
                created: true,
            })
        }
        _ => s3_adapter::ensure_bucket_exists(&s3_adapter::S3ConnectionConfig {
            provider: normalize_provider(&config.provider),
            region: config.region.clone(),
            bucket: config.bucket.clone(),
            access_key_id: config.credentials.access_key_id.clone(),
            secret_access_key: config.credentials.secret_access_key.clone(),
        })
        .await
        .map(|summary| BucketEnsureSummary {
            checked_at: summary.checked_at,
            bucket: summary.bucket,
            created: summary.created,
        }),
    }
}

pub async fn list_objects(
    client: &ObjectStoreClient,
    bucket: &str,
    prefix: Option<&str>,
) -> Result<Vec<ObjectEntry>, String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            let mut continuation_token: Option<String> = None;
            let mut objects = Vec::new();

            loop {
                let mut request = client.list_objects_v2().bucket(bucket);
                if let Some(prefix) = prefix {
                    request = request.prefix(prefix);
                }
                if let Some(token) = continuation_token.as_deref() {
                    request = request.continuation_token(token);
                }

                let response = request.send().await.map_err(|error| {
                    format!("failed to list objects in bucket '{bucket}': {error}")
                })?;

                objects.extend(response.contents().iter().filter_map(|object| {
                    Some(ObjectEntry {
                        key: object.key()?.to_string(),
                        size: object.size().unwrap_or_default().max(0) as u64,
                        last_modified_at: object.last_modified().map(|value| value.to_string()),
                        etag: object.e_tag().map(|value| value.to_string()),
                        storage_class: object
                            .storage_class()
                            .map(|value| value.as_str().to_string()),
                    })
                }));

                if response.is_truncated().unwrap_or(false) {
                    continuation_token =
                        response.next_continuation_token().map(ToString::to_string);
                } else {
                    break;
                }
            }

            Ok(objects)
        }
        ObjectStoreClient::Gcs(client) => {
            client
                .list_objects(bucket, prefix, None)
                .await
                .map(|objects| {
                    objects
                        .into_iter()
                        .map(|object| ObjectEntry {
                            key: object.name,
                            size: object.size,
                            last_modified_at: object.updated,
                            etag: object.etag,
                            storage_class: object.storage_class,
                        })
                        .collect()
                })
        }
    }
}

pub async fn upload_file(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
    path: &Path,
    metadata: Option<HashMap<String, String>>,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::upload_file(client, bucket, key, path, metadata).await
        }
        ObjectStoreClient::Gcs(client) => client.upload_object(bucket, key, path, metadata).await,
    }
}

pub async fn create_directory_placeholder(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::create_directory_placeholder(client, bucket, key).await
        }
        ObjectStoreClient::Gcs(client) => {
            client
                .upload_object_bytes(bucket, key, Vec::new(), None)
                .await
        }
    }
}

pub async fn download_file(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
    path: &Path,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::download_file(client, bucket, key, path).await
        }
        ObjectStoreClient::Gcs(client) => client.download_object(bucket, key, path).await,
    }
}

pub async fn delete_object(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => s3_adapter::delete_object(client, bucket, key).await,
        ObjectStoreClient::Gcs(client) => client.delete_object(bucket, key).await,
    }
}

pub async fn move_object(
    client: &ObjectStoreClient,
    bucket: &str,
    from_key: &str,
    to_key: &str,
    metadata: Option<HashMap<String, String>>,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::move_object(client, bucket, from_key, to_key, metadata).await
        }
        ObjectStoreClient::Gcs(client) => {
            let _ = metadata;
            client.move_object(bucket, from_key, to_key).await
        }
    }
}

pub async fn list_object_keys_with_prefix(
    client: &ObjectStoreClient,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>, String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::list_object_keys_with_prefix(client, bucket, prefix).await
        }
        ObjectStoreClient::Gcs(client) => client
            .list_objects(bucket, Some(prefix), None)
            .await
            .map(|objects| objects.into_iter().map(|object| object.name).collect()),
    }
}

pub async fn object_exists(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
) -> Result<bool, String> {
    match client {
        ObjectStoreClient::Aws(client) => s3_adapter::object_exists(client, bucket, key).await,
        ObjectStoreClient::Gcs(client) => client.object_exists(bucket, key).await,
    }
}

#[allow(dead_code)]
pub async fn get_object_metadata(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
) -> Result<ObjectMetadata, String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            let response = client
                .head_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|error| {
                    sanitize_sensitive_text(format!(
                        "failed to inspect '{key}' in bucket '{bucket}': {error}"
                    ))
                })?;

            Ok(ObjectMetadata {
                key: key.to_string(),
                size: response.content_length().unwrap_or_default().max(0) as u64,
                last_modified_at: response.last_modified().map(|value| value.to_string()),
                etag: response.e_tag().map(ToString::to_string),
                storage_class: response
                    .storage_class()
                    .map(|value| value.as_str().to_string()),
            })
        }
        ObjectStoreClient::Gcs(client) => {
            client
                .get_object_metadata(bucket, key)
                .await
                .map(|object| ObjectMetadata {
                    key: object.name,
                    size: object.size,
                    last_modified_at: object.updated,
                    etag: object.etag,
                    storage_class: object.storage_class,
                })
        }
    }
}

pub async fn bucket_versioning_enabled(
    client: &ObjectStoreClient,
    bucket: &str,
) -> Result<bool, String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::bucket_versioning_enabled(client, bucket).await
        }
        ObjectStoreClient::Gcs(client) => client.bucket_versioning_enabled(bucket).await,
    }
}

pub async fn set_bucket_versioning(
    client: &ObjectStoreClient,
    bucket: &str,
    enabled: bool,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::set_bucket_versioning(client, bucket, enabled).await
        }
        ObjectStoreClient::Gcs(client) => client.set_bucket_versioning(bucket, enabled).await,
    }
}

pub fn supports_remote_bin_lifecycle_reconciliation(provider: &str) -> bool {
    matches!(normalize_provider(provider).as_str(), AWS_PROVIDER | GCS_PROVIDER)
}

pub async fn get_bucket_lifecycle_configuration_state(
    client: &ObjectStoreClient,
    bucket: &str,
) -> Result<BucketLifecycleConfigurationState, String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            let state = s3_adapter::get_bucket_lifecycle_configuration(client, bucket).await?;
            Ok(BucketLifecycleConfigurationState::Aws {
                configuration: state.configuration,
                transition_default_minimum_object_size: state.transition_default_minimum_object_size,
            })
        }
        ObjectStoreClient::Gcs(client) => {
            let state = client.get_bucket_lifecycle_configuration(bucket).await?;
            Ok(BucketLifecycleConfigurationState::Gcs {
                configuration: state.configuration,
                metageneration: state.metageneration,
            })
        }
    }
}

pub async fn put_bucket_lifecycle_configuration_state(
    client: &ObjectStoreClient,
    bucket: &str,
    configuration: BucketLifecycleConfiguration,
    state: &BucketLifecycleConfigurationState,
) -> Result<(), String> {
    match (client, state) {
        (
            ObjectStoreClient::Aws(client),
            BucketLifecycleConfigurationState::Aws {
                transition_default_minimum_object_size,
                ..
            },
        ) => {
            s3_adapter::put_bucket_lifecycle_configuration(
                client,
                bucket,
                configuration,
                transition_default_minimum_object_size.clone(),
            )
            .await
        }
        (
            ObjectStoreClient::Gcs(client),
            BucketLifecycleConfigurationState::Gcs { metageneration, .. },
        ) => {
            let rules = configuration.rules();
            client
                .patch_bucket_lifecycle_configuration(
                    bucket,
                    Some(&GcsBucketLifecycleConfiguration {
                        rule: rules.iter().map(map_aws_lifecycle_rule_to_gcs).collect(),
                    }),
                    metageneration.as_deref(),
                )
                .await
        }
        _ => Err("Lifecycle configuration state does not match object store provider.".into()),
    }
}

pub async fn delete_bucket_lifecycle_configuration(
    client: &ObjectStoreClient,
    bucket: &str,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => s3_adapter::delete_bucket_lifecycle(client, bucket).await,
        ObjectStoreClient::Gcs(client) => client
            .patch_bucket_lifecycle_configuration(bucket, None, None)
            .await,
    }
}

pub async fn reconcile_remote_bin_lifecycle(
    client: &ObjectStoreClient,
    bucket: &str,
    managed_rules: &[ManagedLifecycleRulePlan],
) -> Result<(), String> {
    if !managed_rules.is_empty() && bucket_versioning_enabled(client, bucket).await? {
        return Err(format!(
            "Remote bin requires bucket versioning to be disabled for bucket '{}'.",
            bucket
        ));
    }

    let lifecycle_state = get_bucket_lifecycle_configuration_state(client, bucket).await?;

    let existing_rules = match &lifecycle_state {
        BucketLifecycleConfigurationState::Aws { configuration, .. } => {
            configuration.as_ref().map(|configuration| configuration.rules())
        }
        BucketLifecycleConfigurationState::Gcs {
            configuration,
            metageneration,
        } => {
            let ObjectStoreClient::Gcs(gcs_client) = client else {
                return Err("Lifecycle configuration state does not match object store provider.".into());
            };

            return match gcs_adapter::reconcile_managed_lifecycle_rules(
                configuration.as_ref().map(|configuration| configuration.rule.as_slice()),
                managed_rules,
            ) {
                GcsLifecycleRulesChange::None => Ok(()),
                GcsLifecycleRulesChange::Replace(rules) => {
                    let configuration = GcsBucketLifecycleConfiguration { rule: rules };
                    gcs_client
                        .patch_bucket_lifecycle_configuration(
                            bucket,
                            Some(&configuration),
                            metageneration.as_deref(),
                        )
                        .await
                }
                GcsLifecycleRulesChange::DeleteBucketLifecycle => {
                    gcs_client
                        .patch_bucket_lifecycle_configuration(
                            bucket,
                            None,
                            metageneration.as_deref(),
                        )
                        .await
                }
            };
        }
    };

    match reconcile_lifecycle_rules(existing_rules, managed_rules) {
        LifecycleRulesChange::None => Ok(()),
        LifecycleRulesChange::Replace(rules) => {
            let configuration = BucketLifecycleConfiguration::builder()
                .set_rules(Some(rules))
                .build()
                .map_err(|error| {
                    format!(
                        "failed to build lifecycle configuration for bucket '{}': {error}",
                        bucket
                    )
                })?;
            put_bucket_lifecycle_configuration_state(client, bucket, configuration, &lifecycle_state)
                .await
        }
        LifecycleRulesChange::DeleteBucketLifecycle => {
            delete_bucket_lifecycle_configuration(client, bucket).await
        }
    }
}

fn map_aws_lifecycle_rule_to_gcs(
    rule: &aws_sdk_s3::types::LifecycleRule,
) -> gcs_adapter::GcsLifecycleRule {
    #[allow(deprecated)]
    let prefix = rule
        .filter()
        .and_then(aws_sdk_s3::types::LifecycleRuleFilter::prefix)
        .or(rule.prefix())
        .unwrap_or_default()
        .to_string();
    let age = rule
        .expiration()
        .and_then(aws_sdk_s3::types::LifecycleExpiration::days)
        .and_then(|days| u32::try_from(days).ok());

    gcs_adapter::GcsLifecycleRule {
        action: gcs_adapter::GcsLifecycleAction {
            action_type: "Delete".into(),
            extra: BTreeMap::new(),
        },
        condition: gcs_adapter::GcsLifecycleCondition {
            age,
            matches_prefix: if prefix.is_empty() {
                Vec::new()
            } else {
                vec![prefix]
            },
            extra: BTreeMap::new(),
        },
    }
}

pub async fn list_object_versions_page(
    client: &ObjectStoreClient,
    bucket: &str,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
) -> Result<ObjectVersionPage, String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::list_object_versions_page(client, bucket, key_marker, version_id_marker)
                .await
                .map(map_s3_object_version_page)
        }
        ObjectStoreClient::Gcs(client) => client
            .list_object_versions(bucket, None, key_marker)
            .await
            .map(map_gcs_object_version_page),
    }
}

pub async fn list_object_versions_page_with_prefix(
    client: &ObjectStoreClient,
    bucket: &str,
    prefix: Option<&str>,
    key_marker: Option<&str>,
    version_id_marker: Option<&str>,
) -> Result<ObjectVersionPage, String> {
    match client {
        ObjectStoreClient::Aws(client) => s3_adapter::list_object_versions_page_with_prefix(
            client,
            bucket,
            prefix,
            key_marker,
            version_id_marker,
        )
        .await
        .map(map_s3_object_version_page),
        ObjectStoreClient::Gcs(client) => client
            .list_object_versions(bucket, prefix, key_marker)
            .await
            .map(map_gcs_object_version_page),
    }
}

pub async fn copy_object_version(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::copy_object_version(client, bucket, key, version_id).await
        }
        ObjectStoreClient::Gcs(client) => client.copy_object_version(bucket, key, version_id).await,
    }
}

pub async fn delete_object_version(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
    version_id: &str,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::delete_object_version(client, bucket, key, version_id).await
        }
        ObjectStoreClient::Gcs(client) => {
            client.delete_object_version(bucket, key, version_id).await
        }
    }
}

pub async fn download_file_version(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
    version_id: &str,
    path: &Path,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::download_file_version(client, bucket, key, version_id, path).await
        }
        ObjectStoreClient::Gcs(client) => {
            client
                .download_object_version(bucket, key, version_id, path)
                .await
        }
    }
}

pub async fn copy_object_with_storage_class(
    client: &ObjectStoreClient,
    bucket: &str,
    key: &str,
    storage_class: &str,
) -> Result<(), String> {
    match client {
        ObjectStoreClient::Aws(client) => {
            s3_adapter::copy_object_with_storage_class(client, bucket, key, storage_class).await
        }
        ObjectStoreClient::Gcs(client) => {
            client
                .rewrite_storage_class(bucket, key, storage_class)
                .await
        }
    }
}

async fn probe_gcs_bucket_permissions(
    config: &StorageCredentialTestConfig,
    bucket: &str,
) -> PermissionProbeSummary {
    let checked_at = now_iso();
    let mut probes = Vec::new();
    let client = match build_client(&StorageConnectionConfig {
        provider: config.provider.clone(),
        region: config.region.clone(),
        bucket: bucket.to_string(),
        credentials: config.credentials.clone(),
    })
    .await
    {
        Ok(ObjectStoreClient::Gcs(client)) => client,
        Ok(ObjectStoreClient::Aws(_)) => unreachable!(),
        Err(error) => {
            probes.push(PermissionProbeResult {
                name: "head_bucket".into(),
                allowed: false,
                message: sanitize_sensitive_text(format!("Failed to build GCS client: {error}")),
            });
            return PermissionProbeSummary {
                checked_at,
                bucket: bucket.to_string(),
                probes,
            };
        }
    };

    let head_ok = match client.get_bucket(bucket).await {
        Ok(()) => {
            probes.push(PermissionProbeResult {
                name: "head_bucket".into(),
                allowed: true,
                message: format!("Bucket '{bucket}' is accessible."),
            });
            true
        }
        Err(error) => {
            probes.push(PermissionProbeResult {
                name: "head_bucket".into(),
                allowed: false,
                message: sanitize_sensitive_text(format!(
                    "Bucket '{bucket}' is not accessible: {error}"
                )),
            });
            false
        }
    };

    if head_ok {
        let read_probe = match client.list_objects(bucket, None, Some(1)).await {
            Ok(objects) => PermissionProbeResult {
                name: "list_objects".into(),
                allowed: true,
                message: format!(
                    "Can list objects in bucket '{bucket}' (sampled {} object(s)).",
                    objects.len()
                ),
            },
            Err(error) => PermissionProbeResult {
                name: "list_objects".into(),
                allowed: false,
                message: sanitize_sensitive_text(format!(
                    "Cannot list objects in bucket '{bucket}': {error}"
                )),
            },
        };
        probes.push(read_probe);

        let capabilities = provider_capabilities(GCS_PROVIDER);
        probes.push(PermissionProbeResult {
            name: "get_bucket_lifecycle_configuration".into(),
            allowed: capabilities.supports_bucket_lifecycle,
            message: if capabilities.supports_bucket_lifecycle {
                "Lifecycle configuration supported.".into()
            } else {
                "Lifecycle configuration is not supported by the native GCS backend yet.".into()
            },
        });
        probes.push(match client.bucket_versioning_enabled(bucket).await {
            Ok(enabled) => PermissionProbeResult {
                name: "get_bucket_versioning".into(),
                allowed: true,
                message: format!(
                    "Bucket versioning is {} on '{bucket}'.",
                    if enabled { "enabled" } else { "disabled" }
                ),
            },
            Err(error) => PermissionProbeResult {
                name: "get_bucket_versioning".into(),
                allowed: false,
                message: sanitize_sensitive_text(error),
            },
        });
    }

    PermissionProbeSummary {
        checked_at,
        bucket: bucket.to_string(),
        probes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::provider::{
        provider_capabilities, runtime_provider_capabilities,
    };
    use crate::storage::remote_bin::managed_lifecycle_rule_plan;

    #[test]
    fn gcs_credential_validation_uses_bucket_fallback_when_listing_is_unavailable() {
        let summary = summarize_gcs_credential_validation(
            Some("least-privilege-bucket"),
            Err("list denied".into()),
            Some(Ok(())),
        )
        .expect("bucket-scoped validation should pass");

        assert_eq!(summary.bucket_count, 1);
        assert_eq!(summary.buckets, vec!["least-privilege-bucket"]);
        assert!(summary.bucket_scoped);
    }

    #[test]
    fn gcs_credential_validation_without_bucket_still_surfaces_listing_error() {
        let error = summarize_gcs_credential_validation(None, Err("list denied".into()), None)
            .expect_err("listing failure without bucket should fail");

        assert_eq!(error, "list denied");
    }

    #[test]
    fn gcs_credential_validation_reports_bucket_probe_failure_clearly() {
        let error = summarize_gcs_credential_validation(
            Some("least-privilege-bucket"),
            Err("list denied".into()),
            Some(Err("403 forbidden".into())),
        )
        .expect_err("failed bucket probe should fail validation");

        assert_eq!(
            error,
            "Failed to validate access to configured bucket 'least-privilege-bucket': 403 forbidden"
        );
    }

    #[test]
    fn gcs_permission_probe_versioning_message_reflects_runtime_limitation() {
        let capabilities = provider_capabilities(GCS_PROVIDER);
        let runtime = runtime_provider_capabilities(GCS_PROVIDER);

        let probe = PermissionProbeResult {
            name: "get_bucket_versioning".into(),
            allowed: runtime.object_versioning.status == "supported",
            message: if let Some(message) = runtime.object_versioning.message {
                message
            } else if capabilities.supports_object_versioning {
                "Object versioning is supported by the native GCS backend.".into()
            } else {
                "Object versioning is not supported by the native GCS backend yet.".into()
            },
        };

        assert!(probe.allowed);
        assert_eq!(
            probe.message,
            "Object versioning is supported by the native GCS backend."
        );
    }

    #[test]
    fn gcs_runtime_capability_and_provider_capability_stay_aligned() {
        let provider = provider_capabilities(GCS_PROVIDER);
        let runtime = runtime_provider_capabilities(GCS_PROVIDER);

        assert!(provider.supports_object_versioning);
        assert!(provider.supports_file_versions);
        assert_eq!(runtime.object_versioning.status, "supported");
    }

    #[test]
    fn maps_s3_version_pages_to_provider_neutral_contract() {
        let page = map_s3_object_version_page(s3_adapter::ObjectVersionPage {
            versions: vec![s3_adapter::ObjectVersionSummary {
                key: "docs/readme.txt".into(),
                version_id: "v1".into(),
                is_latest: true,
                size: 42,
                last_modified_at: Some("2026-04-25T10:00:00Z".into()),
                storage_class: Some("STANDARD".into()),
                etag: Some("etag".into()),
            }],
            delete_markers: vec![s3_adapter::DeleteMarkerSummary {
                key: "docs/old.txt".into(),
                version_id: "dm1".into(),
                is_latest: true,
                last_modified_at: Some("2026-04-25T11:00:00Z".into()),
            }],
            next_key_marker: Some("next-key".into()),
            next_version_id_marker: Some("next-version".into()),
            truncated: true,
        });

        assert_eq!(page.versions.len(), 1);
        assert_eq!(page.versions[0].version_id, "v1");
        assert_eq!(page.delete_markers.len(), 1);
        assert_eq!(page.delete_markers[0].version_id, "dm1");
        assert_eq!(page.next_key_marker.as_deref(), Some("next-key"));
        assert!(page.truncated);
    }

    #[test]
    fn gcs_versioning_maps_generation_to_version_page() {
        use super::super::gcs_adapter::{GcsObjectVersionItem, GcsObjectVersionPage};

        let page = map_gcs_object_version_page(GcsObjectVersionPage {
            items: vec![
                GcsObjectVersionItem {
                    name: "docs/readme.txt".into(),
                    generation: "1714000000000000".into(),
                    size: 42,
                    updated: Some("2026-04-25T10:00:00Z".into()),
                    etag: Some("etag-1".into()),
                    storage_class: Some("STANDARD".into()),
                    time_deleted: None,
                },
                GcsObjectVersionItem {
                    name: "docs/readme.txt".into(),
                    generation: "1713000000000000".into(),
                    size: 30,
                    updated: Some("2026-04-24T10:00:00Z".into()),
                    etag: Some("etag-2".into()),
                    storage_class: Some("NEARLINE".into()),
                    time_deleted: Some("2026-04-25T10:00:00Z".into()),
                },
            ],
            next_page_token: Some("token-abc".into()),
        });

        assert_eq!(page.versions.len(), 2);
        assert_eq!(page.versions[0].version_id, "1714000000000000");
        assert!(page.versions[0].is_latest);
        assert_eq!(page.versions[1].version_id, "1713000000000000");
        assert!(!page.versions[1].is_latest);
        assert!(page.delete_markers.is_empty());
        assert_eq!(page.next_key_marker.as_deref(), Some("token-abc"));
        assert!(page.next_version_id_marker.is_none());
        assert!(page.truncated);
    }

    #[test]
    fn gcs_storage_class_is_now_supported_at_runtime() {
        let runtime = runtime_provider_capabilities(GCS_PROVIDER);
        assert_eq!(runtime.archive_storage.status, "supported");
        assert!(runtime.archive_storage.message.is_none());
    }

    #[test]
    fn remote_bin_lifecycle_reconciliation_support_covers_aws_and_gcs() {
        assert!(supports_remote_bin_lifecycle_reconciliation("aws"));
        assert!(supports_remote_bin_lifecycle_reconciliation("s3"));
        assert!(supports_remote_bin_lifecycle_reconciliation("gcs"));
    }

    #[test]
    fn gcs_lifecycle_state_carries_configuration_and_metageneration() {
        let state = BucketLifecycleConfigurationState::Gcs {
            configuration: Some(GcsBucketLifecycleConfiguration { rule: Vec::new() }),
            metageneration: Some("12".into()),
        };

        match state {
            BucketLifecycleConfigurationState::Gcs {
                configuration,
                metageneration,
            } => {
                assert!(configuration.is_some());
                assert_eq!(metageneration.as_deref(), Some("12"));
            }
            BucketLifecycleConfigurationState::Aws { .. } => {
                panic!("expected gcs lifecycle state")
            }
        }
    }

    #[test]
    fn lifecycle_plan_payload_stays_provider_neutral() {
        let plan = managed_lifecycle_rule_plan("pair-1", 14);

        assert_eq!(plan.pair_id, "pair-1");
        assert_eq!(plan.prefix, ".storage-goblin-bin/pairs/pair-1/");
        assert_eq!(plan.retention_days, 14);
    }
}
