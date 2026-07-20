//! Resolving, testing, and describing stored credentials.
//!
//! Owns the rules for which credential a sync location uses, what a
//! validation result means, and how permission probes are summarised — none
//! of which belong in the IPC layer.

use tauri::{AppHandle, Runtime};

use super::commands::CredentialTestContext;
use super::credentials_store::{get_credential_summary, CredentialSummary};
use super::object_store;
use super::profile_store::{
    read_profile_from_disk, SelectedCredentialState, StoredProfile, SyncPair,
};
use super::provider::{normalize_provider, runtime_provider_capabilities, GCS_PROVIDER};

pub(crate) fn format_storage_validation_success_message(
    summary: &object_store::ValidationSummary,
) -> String {
    format!(
        "Validated access to bucket '{}' and sampled {} remote object(s).",
        summary.bucket, summary.object_count_sampled
    )
}

pub(crate) fn credential_test_context_from_profile(
    profile: &StoredProfile,
) -> CredentialTestContext {
    CredentialTestContext {
        provider: profile.provider.clone(),
        region: profile.region.trim().to_string(),
        bucket: profile.bucket.trim().to_string(),
    }
}

pub(crate) fn resolve_credential_test_context<R: Runtime>(
    app: &AppHandle<R>,
    context: Option<CredentialTestContext>,
) -> Result<CredentialTestContext, String> {
    if let Some(context) = context {
        return Ok(context.normalized());
    }

    let profile = read_profile_from_disk(app)?;
    Ok(credential_test_context_from_profile(&profile))
}

pub(crate) fn should_defer_create_time_credential_test(
    credential: &CredentialSummary,
    context: &CredentialTestContext,
) -> bool {
    normalize_provider(&credential.provider) == GCS_PROVIDER && !context.has_bucket()
}

pub(crate) fn format_permission_probe_summary(
    probes: &[object_store::PermissionProbeResult],
) -> String {
    let labels: Vec<String> = probes
        .iter()
        .filter(|p| p.name != "head_bucket")
        .map(|p| {
            let icon = if p.allowed { "✓" } else { "✗" };
            let label = match p.name.as_str() {
                "put_object" => "write",
                "get_object" => "read",
                "delete_object" => "delete",
                other => other,
            };
            format!("{label} {icon}")
        })
        .collect();

    if labels.is_empty() {
        let head = probes.iter().find(|p| p.name == "head_bucket");
        match head {
            Some(p) if !p.allowed => "Bucket not accessible.".into(),
            _ => String::new(),
        }
    } else {
        format!("Permissions: {}", labels.join(" · "))
    }
}

pub(crate) fn resolve_profile_credential_name(existing_profile: &StoredProfile) -> String {
    existing_profile
        .selected_credential
        .as_ref()
        .map(|summary| summary.name.clone())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| "Default credential".into())
}

pub(crate) fn resolve_selected_credential_state<R: Runtime>(
    app: &AppHandle<R>,
    credential_id: Option<&str>,
) -> Result<SelectedCredentialState, String> {
    let Some(credential_id) = credential_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(SelectedCredentialState::default());
    };

    let Some(summary) = get_credential_summary(app, credential_id)? else {
        return Ok(SelectedCredentialState::default());
    };

    Ok(SelectedCredentialState {
        selected_credential_available: summary.ready,
        selected_credential: Some(summary),
    })
}

pub(crate) fn provider_supports_runtime_object_versioning(provider: &str) -> bool {
    runtime_provider_capabilities(provider)
        .object_versioning
        .status
        == "supported"
}

pub(crate) fn provider_runtime_object_versioning_message(provider: &str) -> String {
    let normalized = normalize_provider(provider);
    let runtime = runtime_provider_capabilities(&normalized);
    runtime.object_versioning.message.unwrap_or_else(|| {
        format!(
            "Provider '{}' does not support object versioning.",
            normalized
        )
    })
}

pub(crate) fn sync_location_runtime_object_versioning_message(pair: &SyncPair) -> String {
    format!(
        "Sync location '{}' cannot use object versioning right now. {}",
        pair.label,
        provider_runtime_object_versioning_message(&pair.provider)
    )
}
