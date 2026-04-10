use aws_config::BehaviorVersion;
use aws_credential_types::{provider::SharedCredentialsProvider, Credentials};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration, MetadataDirective, StorageClass};
use aws_sdk_s3::{primitives::ByteStream, Client};
use aws_types::region::Region;
use serde::{Deserialize, Serialize};
use std::path::Path;

use super::now_iso;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3ConnectionConfig {
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct S3CredentialTestConfig {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialTestSummary {
    pub checked_at: String,
    pub bucket_count: usize,
    pub buckets: Vec<String>,
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
pub struct S3ValidationSummary {
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

pub async fn validate_connection(
    config: &S3ConnectionConfig,
) -> Result<S3ValidationSummary, String> {
    validate_required_fields(config)?;

    let client = build_client(config).await?;
    let response = client
        .list_objects_v2()
        .bucket(&config.bucket)
        .max_keys(1)
        .send()
        .await
        .map_err(|error| format!("failed to validate S3 connection: {error}"))?;

    Ok(S3ValidationSummary {
        checked_at: now_iso(),
        bucket: config.bucket.trim().to_string(),
        object_count_sampled: response.key_count().unwrap_or(0) as usize,
    })
}

pub async fn build_client(config: &S3ConnectionConfig) -> Result<Client, String> {
    validate_required_fields(config)?;

    let credentials = Credentials::new(
        config.access_key_id.trim(),
        config.secret_access_key.trim(),
        None,
        None,
        "storage-goblin",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .region(Region::new(region_or_default(&config.region)))
        .load()
        .await;

    Ok(Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared_config).build(),
    ))
}

pub async fn build_credential_test_client(
    config: &S3CredentialTestConfig,
) -> Result<Client, String> {
    let access_key_id = config.access_key_id.trim();
    let secret_access_key = config.secret_access_key.trim();

    if access_key_id.is_empty() {
        return Err("Access key ID is required for credential testing.".into());
    }
    if secret_access_key.is_empty() {
        return Err("Secret access key is required for credential testing.".into());
    }

    let credentials = Credentials::new(
        access_key_id,
        secret_access_key,
        None,
        None,
        "storage-goblin",
    );

    let shared_config = aws_config::defaults(BehaviorVersion::latest())
        .credentials_provider(SharedCredentialsProvider::new(credentials))
        .region(Region::new(region_or_default(&config.region)))
        .load()
        .await;

    Ok(Client::from_conf(
        aws_sdk_s3::config::Builder::from(&shared_config).build(),
    ))
}

pub async fn validate_credentials(
    config: &S3CredentialTestConfig,
) -> Result<CredentialTestSummary, String> {
    let client = build_credential_test_client(config).await?;

    let response = client
        .list_buckets()
        .send()
        .await
        .map_err(|error| format!("Credential test failed: {error}"))?;

    let buckets: Vec<String> = response
        .buckets()
        .iter()
        .filter_map(|bucket| bucket.name().map(|name| name.to_string()))
        .collect();

    Ok(CredentialTestSummary {
        checked_at: now_iso(),
        bucket_count: buckets.len(),
        buckets,
    })
}

pub async fn ensure_bucket_exists(
    config: &S3ConnectionConfig,
) -> Result<BucketEnsureSummary, String> {
    validate_required_fields(config)?;

    let client = build_client(config).await?;
    let bucket = config.bucket.trim().to_string();

    match client.head_bucket().bucket(&bucket).send().await {
        Ok(_) => {
            return Ok(BucketEnsureSummary {
                checked_at: now_iso(),
                bucket,
                created: false,
            });
        }
        Err(error) if !is_missing_bucket_error(&error) => {
            return Err(format!("failed to verify bucket '{bucket}': {error}"));
        }
        Err(_) => {}
    }

    let mut request = client.create_bucket().bucket(&bucket);

    if let Some(configuration) = create_bucket_configuration(config) {
        request = request.create_bucket_configuration(configuration);
    }

    match request.send().await {
        Ok(_) => Ok(BucketEnsureSummary {
            checked_at: now_iso(),
            bucket,
            created: true,
        }),
        Err(error) if bucket_already_exists_for_caller(&error) => Ok(BucketEnsureSummary {
            checked_at: now_iso(),
            bucket,
            created: false,
        }),
        Err(error) => Err(format!("failed to create bucket '{bucket}': {error}")),
    }
}

pub async fn upload_file(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
) -> Result<(), String> {
    let body = ByteStream::from_path(path)
        .await
        .map_err(|error| format!("failed to read upload source '{}': {error}", path.display()))?;

    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await
        .map_err(|error| format!("failed to upload '{key}' to bucket '{bucket}': {error}"))?;

    Ok(())
}

pub async fn create_directory_placeholder(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<(), String> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(&[]))
        .send()
        .await
        .map_err(|error| {
            format!("failed to create directory placeholder '{key}' in bucket '{bucket}': {error}")
        })?;

    Ok(())
}

pub async fn download_file(
    client: &Client,
    bucket: &str,
    key: &str,
    path: &Path,
) -> Result<(), String> {
    let response = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|error| format!("failed to download '{key}' from bucket '{bucket}': {error}"))?;

    let bytes = response
        .body
        .collect()
        .await
        .map_err(|error| format!("failed to read download body for '{key}': {error}"))?
        .into_bytes();

    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| {
            format!(
                "failed to create parent directory for '{}': {error}",
                path.display()
            )
        })?;
    }

    std::fs::write(path, &bytes).map_err(|error| {
        format!(
            "failed to write downloaded file '{}': {error}",
            path.display()
        )
    })?;

    Ok(())
}

pub async fn delete_object(client: &Client, bucket: &str, key: &str) -> Result<(), String> {
    client
        .delete_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|error| format!("failed to delete '{key}' from bucket '{bucket}': {error}"))?;

    Ok(())
}

pub fn object_key(relative_path: &str) -> String {
    let normalized_path = relative_path
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();

    normalized_path
}

pub fn directory_key(relative_path: &str) -> String {
    let normalized_path = relative_path
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();

    let directory_path = if normalized_path.is_empty() {
        String::new()
    } else {
        format!("{normalized_path}/")
    };

    directory_path
}

pub fn region_or_default(region: &str) -> String {
    let trimmed = region.trim();
    if trimmed.is_empty() {
        "us-east-1".to_string()
    } else {
        trimmed.to_string()
    }
}

fn validate_required_fields(config: &S3ConnectionConfig) -> Result<(), String> {
    if config.bucket.trim().is_empty() {
        return Err("Bucket is required for S3 access.".into());
    }

    if config.access_key_id.trim().is_empty() {
        return Err("Access key ID is required for S3 access.".into());
    }

    if config.secret_access_key.trim().is_empty() {
        return Err("Secret access key is required for S3 access.".into());
    }

    Ok(())
}

fn create_bucket_configuration(config: &S3ConnectionConfig) -> Option<CreateBucketConfiguration> {
    if should_skip_bucket_location_constraint(&config.region) {
        return None;
    }

    Some(
        CreateBucketConfiguration::builder()
            .location_constraint(BucketLocationConstraint::from(
                region_or_default(&config.region).as_str(),
            ))
            .build(),
    )
}

fn should_skip_bucket_location_constraint(region: &str) -> bool {
    region_or_default(region) == "us-east-1"
}

fn is_missing_bucket_error<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(|service_error| service_error.meta().code())
        .map(|code| matches!(code, "404" | "NotFound" | "NoSuchBucket"))
        .unwrap_or_else(|| {
            let rendered = error.to_string();
            rendered.contains("NoSuchBucket")
                || rendered.contains("NotFound")
                || rendered.contains("status code: 404")
        })
}

fn bucket_already_exists_for_caller<E>(error: &aws_sdk_s3::error::SdkError<E>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(|service_error| service_error.meta().code())
        .map(|code| matches!(code, "BucketAlreadyOwnedByYou" | "BucketAlreadyExists"))
        .unwrap_or_else(|| {
            let rendered = error.to_string();
            rendered.contains("BucketAlreadyOwnedByYou") || rendered.contains("BucketAlreadyExists")
        })
}

const PERMISSION_TEST_OBJECT_NAME: &str = ".storage-goblin-permission-test";

fn permission_test_key() -> String {
    PERMISSION_TEST_OBJECT_NAME.to_string()
}

pub async fn probe_bucket_permissions(
    config: &S3CredentialTestConfig,
    bucket: &str,
) -> PermissionProbeSummary {
    let checked_at = now_iso();
    let mut probes = Vec::new();

    let client = match build_credential_test_client(config).await {
        Ok(client) => client,
        Err(error) => {
            probes.push(PermissionProbeResult {
                name: "head_bucket".into(),
                allowed: false,
                message: format!("Failed to build S3 client: {error}"),
            });
            return PermissionProbeSummary {
                checked_at,
                bucket: bucket.to_string(),
                probes,
            };
        }
    };

    // Phase 1: HeadBucket
    let head_ok = match client.head_bucket().bucket(bucket).send().await {
        Ok(_) => {
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
                message: format!("Bucket '{bucket}' is not accessible: {error}"),
            });
            false
        }
    };

    if !head_ok {
        return PermissionProbeSummary {
            checked_at,
            bucket: bucket.to_string(),
            probes,
        };
    }

    let key = permission_test_key();

    // Phase 2: PutObject
    let put_ok = match client
        .put_object()
        .bucket(bucket)
        .key(&key)
        .body(ByteStream::from_static(b"permission-test"))
        .send()
        .await
    {
        Ok(_) => {
            probes.push(PermissionProbeResult {
                name: "put_object".into(),
                allowed: true,
                message: format!("Can write to '{key}'."),
            });
            true
        }
        Err(error) => {
            probes.push(PermissionProbeResult {
                name: "put_object".into(),
                allowed: false,
                message: format!("Cannot write to '{key}': {error}"),
            });
            false
        }
    };

    // Phase 3: GetObject
    match client.get_object().bucket(bucket).key(&key).send().await {
        Ok(_) => {
            probes.push(PermissionProbeResult {
                name: "get_object".into(),
                allowed: true,
                message: format!("Can read '{key}'."),
            });
        }
        Err(error) => {
            let message = if put_ok {
                format!("Cannot read '{key}': {error}")
            } else {
                format!("Cannot read '{key}' (test object may not exist): {error}")
            };
            probes.push(PermissionProbeResult {
                name: "get_object".into(),
                allowed: false,
                message,
            });
        }
    }

    // Phase 4: DeleteObject (always attempt cleanup)
    match client.delete_object().bucket(bucket).key(&key).send().await {
        Ok(_) => {
            probes.push(PermissionProbeResult {
                name: "delete_object".into(),
                allowed: true,
                message: format!("Can delete '{key}'."),
            });
        }
        Err(error) => {
            probes.push(PermissionProbeResult {
                name: "delete_object".into(),
                allowed: false,
                message: format!("Cannot delete '{key}': {error}"),
            });
        }
    }

    PermissionProbeSummary {
        checked_at,
        bucket: bucket.to_string(),
        probes,
    }
}

pub async fn copy_object_with_storage_class(
    client: &Client,
    bucket: &str,
    key: &str,
    storage_class: &str,
) -> Result<(), String> {
    let copy_source = format!("{bucket}/{key}");

    client
        .copy_object()
        .bucket(bucket)
        .key(key)
        .copy_source(&copy_source)
        .storage_class(StorageClass::from(storage_class))
        .metadata_directive(MetadataDirective::Copy)
        .send()
        .await
        .map_err(|error| {
            format!(
                "failed to change storage class of '{key}' in bucket '{bucket}' to '{storage_class}': {error}"
            )
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        directory_key, object_key, region_or_default, should_skip_bucket_location_constraint,
    };

    #[test]
    fn defaults_empty_region() {
        assert_eq!(region_or_default(""), "us-east-1");
        assert_eq!(region_or_default("eu-west-2"), "eu-west-2");
    }

    #[test]
    fn builds_object_key_with_optional_prefix() {
        assert_eq!(object_key("alpha.txt"), "alpha.txt");
        assert_eq!(object_key("nested\\beta.txt"), "nested/beta.txt");
    }

    #[test]
    fn builds_directory_key_with_optional_prefix() {
        assert_eq!(directory_key("nested"), "nested/");
        assert_eq!(directory_key("nested\\beta"), "nested/beta/");
    }

    #[test]
    fn bucket_root_object_and_directory_keys_have_no_prefix_assumptions() {
        assert_eq!(object_key("nested\\beta.txt"), "nested/beta.txt");
        assert_eq!(object_key("/nested/beta.txt/"), "nested/beta.txt");
        assert_eq!(directory_key("nested\\beta"), "nested/beta/");
        assert_eq!(directory_key("/nested/beta/"), "nested/beta/");
        assert_eq!(directory_key(""), "");
    }

    #[test]
    fn omits_location_constraint_for_us_east_1() {
        assert!(should_skip_bucket_location_constraint("us-east-1"));
        assert!(should_skip_bucket_location_constraint(""));
        assert!(!should_skip_bucket_location_constraint("eu-west-2"));
    }

    #[test]
    fn builds_bucket_root_permission_test_key() {
        assert_eq!(super::permission_test_key(), ".storage-goblin-permission-test");
    }
}
