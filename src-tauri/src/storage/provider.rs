use serde::{Deserialize, Serialize};

pub const AWS_PROVIDER: &str = "aws";
pub const GCS_PROVIDER: &str = "gcs";

pub fn normalize_provider(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "" => AWS_PROVIDER.into(),
        "gcp" | "gcs" | "google-cloud-storage" | "google cloud storage" => GCS_PROVIDER.into(),
        "aws" | "s3" => AWS_PROVIDER.into(),
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CredentialKind {
    AwsAccessKey,
    GcsServiceAccount,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilities {
    pub provider: String,
    pub display_name: String,
    pub aliases: Vec<String>,
    pub credential_kind: CredentialKind,
    pub supports_bucket_creation: bool,
    pub supports_object_versioning: bool,
    pub supports_remote_bin: bool,
    pub supports_storage_class: bool,
    pub supports_file_versions: bool,
    pub supports_bucket_lifecycle: bool,
    pub supports_manual_credentials: bool,
    pub supports_native_validation: bool,
    pub location_kind: String,
    pub location_label: String,
    pub location_help: String,
    pub location_placeholder: String,
    pub location_optional: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderCapabilityStatus {
    pub status: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeProviderCapabilities {
    pub object_versioning: ProviderCapabilityStatus,
    pub remote_bin: ProviderCapabilityStatus,
    pub archive_storage: ProviderCapabilityStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct ProviderLocationMetadata {
    pub provider: String,
    pub kind: String,
    pub label: String,
    pub help: String,
    pub placeholder: String,
    pub value: String,
    pub optional: bool,
}

fn supported_capability() -> ProviderCapabilityStatus {
    ProviderCapabilityStatus {
        status: "supported".into(),
        message: None,
    }
}

#[allow(dead_code)]
fn runtime_unavailable_capability(message: &str) -> ProviderCapabilityStatus {
    ProviderCapabilityStatus {
        status: "runtime-unavailable".into(),
        message: Some(message.into()),
    }
}

pub fn runtime_provider_capabilities(provider: &str) -> RuntimeProviderCapabilities {
    match normalize_provider(provider).as_str() {
        GCS_PROVIDER => RuntimeProviderCapabilities {
            object_versioning: supported_capability(),
            remote_bin: supported_capability(),
            archive_storage: supported_capability(),
        },
        _ => RuntimeProviderCapabilities {
            object_versioning: supported_capability(),
            remote_bin: supported_capability(),
            archive_storage: supported_capability(),
        },
    }
}

#[allow(dead_code)]
pub fn provider_location_metadata(provider: &str, value: &str) -> ProviderLocationMetadata {
    let capabilities = provider_capabilities(provider);
    ProviderLocationMetadata {
        provider: capabilities.provider.clone(),
        kind: capabilities.location_kind.clone(),
        label: capabilities.location_label.clone(),
        help: capabilities.location_help.clone(),
        placeholder: capabilities.location_placeholder.clone(),
        value: value.trim().to_string(),
        optional: capabilities.location_optional,
    }
}

pub fn provider_capabilities(provider: &str) -> ProviderCapabilities {
    match normalize_provider(provider).as_str() {
        GCS_PROVIDER => ProviderCapabilities {
            provider: GCS_PROVIDER.into(),
            display_name: "Google Cloud Storage".into(),
            aliases: vec![
                "gcp".into(),
                "google-cloud-storage".into(),
                "google cloud storage".into(),
            ],
            credential_kind: CredentialKind::GcsServiceAccount,
            supports_bucket_creation: true,
            supports_object_versioning: true,
            supports_remote_bin: true,
            supports_storage_class: true,
            supports_file_versions: true,
            supports_bucket_lifecycle: true,
            supports_manual_credentials: true,
            supports_native_validation: true,
            location_kind: "gcs-location".into(),
            location_label: "Bucket location".into(),
            location_help: "Use the bucket location or leave blank when Google Cloud Storage can infer it automatically.".into(),
            location_placeholder:
                "Auto-detect or enter a GCS location such as US, EU, us-central1, or europe-west2"
                    .into(),
            location_optional: true,
        },
        _ => ProviderCapabilities {
            provider: AWS_PROVIDER.into(),
            display_name: "Amazon S3".into(),
            aliases: vec!["s3".into()],
            credential_kind: CredentialKind::AwsAccessKey,
            supports_bucket_creation: true,
            supports_object_versioning: true,
            supports_remote_bin: true,
            supports_storage_class: true,
            supports_file_versions: true,
            supports_bucket_lifecycle: true,
            supports_manual_credentials: true,
            supports_native_validation: true,
            location_kind: "aws-region".into(),
            location_label: "Region".into(),
            location_help:
                "Choose the AWS region for this bucket when creation or validation requires it."
                    .into(),
            location_placeholder: "Auto-detect".into(),
            location_optional: true,
        },
    }
}

pub fn supported_providers() -> Vec<ProviderCapabilities> {
    vec![
        provider_capabilities(AWS_PROVIDER),
        provider_capabilities(GCS_PROVIDER),
    ]
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_provider, provider_capabilities, provider_location_metadata,
        runtime_provider_capabilities, CredentialKind, GCS_PROVIDER,
    };

    #[test]
    fn normalizes_provider_aliases() {
        assert_eq!(normalize_provider("aws"), "aws");
        assert_eq!(normalize_provider("s3"), "aws");
        assert_eq!(normalize_provider("gcp"), "gcs");
        assert_eq!(normalize_provider(" GCS "), "gcs");
        assert_eq!(normalize_provider("google-cloud-storage"), "gcs");
        assert_eq!(normalize_provider("Google Cloud Storage"), "gcs");
    }

    #[test]
    fn reports_gcs_capabilities() {
        let capabilities = provider_capabilities(GCS_PROVIDER);

        assert_eq!(capabilities.provider, "gcs");
        assert_eq!(
            capabilities.credential_kind,
            CredentialKind::GcsServiceAccount
        );
        assert!(capabilities.supports_bucket_creation);
        assert!(capabilities.supports_object_versioning);
        assert!(capabilities.supports_remote_bin);
        assert!(capabilities.supports_storage_class);
        assert!(capabilities.supports_file_versions);
        assert!(capabilities.supports_bucket_lifecycle);
        assert!(capabilities.supports_native_validation);
        assert_eq!(capabilities.location_kind, "gcs-location");
        assert_eq!(capabilities.location_label, "Bucket location");
    }

    #[test]
    fn reports_runtime_gcs_capabilities_for_ui() {
        let capabilities = runtime_provider_capabilities(GCS_PROVIDER);

        assert_eq!(capabilities.object_versioning.status, "supported");
        assert!(capabilities.object_versioning.message.is_none());
        assert_eq!(capabilities.remote_bin.status, "supported");
        assert_eq!(capabilities.archive_storage.status, "supported");
    }

    #[test]
    fn builds_provider_location_metadata() {
        let location = provider_location_metadata("gcp", " us-central1 ");

        assert_eq!(location.provider, "gcs");
        assert_eq!(location.kind, "gcs-location");
        assert_eq!(location.label, "Bucket location");
        assert_eq!(location.value, "us-central1");
        assert!(location.optional);
    }
}
