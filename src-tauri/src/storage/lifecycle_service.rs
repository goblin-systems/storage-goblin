//! Reconciling provider lifecycle rules with the remote-bin configuration of
//! each sync location, and applying object-versioning changes.
//!
//! Lifecycle rules are per-bucket while retention is configured per location,
//! so several locations sharing a bucket must be merged into one rule set
//! before anything is written.

use std::collections::{BTreeMap, BTreeSet};

use tauri::{AppHandle, Runtime};

use super::credentials_store::{load_credentials_by_id, StoredCredentials};
use super::object_store;
use super::profile_store::{is_pair_configured, is_profile_configured, StoredProfile, SyncPair};
use super::provider::normalize_provider;
use super::remote_bin::{
    managed_lifecycle_rule_plan, ManagedLifecycleRulePlan, DEFAULT_REMOTE_BIN_PAIR_ID,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteBinLifecycleTarget {
    pub(crate) provider: String,
    pub(crate) bucket: String,
    pub(crate) region: String,
    pub(crate) credential_profile_id: Option<String>,
    pub(crate) source_labels: Vec<String>,
    pub(crate) managed_rules: Vec<ManagedLifecycleRulePlan>,
}

pub(crate) fn target_for_profile(profile: &StoredProfile) -> Option<RemoteBinLifecycleTarget> {
    if !is_profile_configured(profile) {
        return None;
    }

    Some(RemoteBinLifecycleTarget {
        provider: profile.provider.clone(),
        bucket: profile.bucket.clone(),
        region: profile.region.clone(),
        credential_profile_id: profile.credential_profile_id.clone(),
        source_labels: vec!["profile".into()],
        managed_rules: if profile.remote_bin.enabled {
            vec![managed_lifecycle_rule_plan(
                DEFAULT_REMOTE_BIN_PAIR_ID,
                profile.remote_bin.retention_days,
            )]
        } else {
            vec![]
        },
    })
}

pub(crate) fn target_for_pair(pair: &SyncPair) -> Option<RemoteBinLifecycleTarget> {
    if !is_pair_configured(pair) {
        return None;
    }

    Some(RemoteBinLifecycleTarget {
        provider: pair.provider.clone(),
        bucket: pair.bucket.clone(),
        region: pair.region.clone(),
        credential_profile_id: pair.credential_profile_id.clone(),
        source_labels: vec![format!("sync pair '{}'", pair.label)],
        managed_rules: if pair.remote_bin.enabled {
            vec![managed_lifecycle_rule_plan(
                &pair.id,
                pair.remote_bin.retention_days,
            )]
        } else {
            vec![]
        },
    })
}

pub(crate) fn remote_bin_targets_by_bucket(
    profile: &StoredProfile,
) -> BTreeMap<String, RemoteBinLifecycleTarget> {
    let mut targets = BTreeMap::new();

    if profile.sync_pairs.is_empty() {
        if let Some(target) = target_for_profile(profile) {
            merge_remote_bin_target(&mut targets, target);
        }
    } else {
        for pair in &profile.sync_pairs {
            if let Some(target) = target_for_pair(pair) {
                merge_remote_bin_target(&mut targets, target);
            }
        }
    }

    targets
}

pub(crate) fn merge_remote_bin_target(
    targets: &mut BTreeMap<String, RemoteBinLifecycleTarget>,
    target: RemoteBinLifecycleTarget,
) {
    let target_key = bucket_key_for_target(&target);
    match targets.get_mut(&target_key) {
        Some(existing) => {
            existing.source_labels.extend(target.source_labels);
            existing.managed_rules.extend(target.managed_rules);
            if existing.credential_profile_id.is_none() {
                existing.credential_profile_id = target.credential_profile_id;
            }
            existing.managed_rules.sort();
            existing.managed_rules.dedup();
            existing.source_labels.sort();
            existing.source_labels.dedup();
        }
        None => {
            targets.insert(target_key, target);
        }
    }
}

pub(crate) fn bucket_key_for_target(target: &RemoteBinLifecycleTarget) -> String {
    format!(
        "{}\n{}\n{}",
        normalize_provider(&target.provider),
        target.bucket,
        target.region
    )
}

pub(crate) fn target_source_label(target: &RemoteBinLifecycleTarget) -> String {
    match target.source_labels.as_slice() {
        [] => "remote-bin target".into(),
        [only] => only.clone(),
        many => many.join(", "),
    }
}

pub(crate) fn planned_remote_bin_reconciliation(
    current: &StoredProfile,
    next: &StoredProfile,
) -> Vec<RemoteBinLifecycleTarget> {
    let current_targets = remote_bin_targets_by_bucket(current);
    let next_targets = remote_bin_targets_by_bucket(next);
    let buckets = current_targets
        .keys()
        .chain(next_targets.keys())
        .cloned()
        .collect::<BTreeSet<_>>();

    buckets
        .into_iter()
        .filter_map(
            |bucket| match (current_targets.get(&bucket), next_targets.get(&bucket)) {
                (_, Some(target)) if !target.managed_rules.is_empty() => Some(target.clone()),
                (Some(current_target), Some(next_target))
                    if !current_target.managed_rules.is_empty() =>
                {
                    let mut disabled_target = next_target.clone();
                    disabled_target.managed_rules.clear();
                    Some(disabled_target)
                }
                (Some(current_target), None) if !current_target.managed_rules.is_empty() => {
                    let mut disabled_target = current_target.clone();
                    disabled_target.managed_rules.clear();
                    Some(disabled_target)
                }
                _ => None,
            },
        )
        .collect()
}

pub(crate) fn persist_profile_with_remote_bin_reconciliation<R, Reconcile, Write>(
    app: &AppHandle<R>,
    current: &StoredProfile,
    next: StoredProfile,
    mut reconcile_bucket: Reconcile,
    write_profile: Write,
) -> Result<StoredProfile, String>
where
    R: Runtime,
    Reconcile: FnMut(&AppHandle<R>, &RemoteBinLifecycleTarget) -> Result<(), String>,
    Write: FnOnce(&AppHandle<R>, &StoredProfile) -> Result<(), String>,
{
    persist_profile_with_remote_bin_reconciliation_inner(
        planned_remote_bin_reconciliation(current, &next),
        next,
        |target| reconcile_bucket(app, target),
        |profile| write_profile(app, profile),
    )
}

pub(crate) fn persist_profile_with_remote_bin_reconciliation_inner<Reconcile, Write>(
    targets: Vec<RemoteBinLifecycleTarget>,
    next: StoredProfile,
    mut reconcile_bucket: Reconcile,
    write_profile: Write,
) -> Result<StoredProfile, String>
where
    Reconcile: FnMut(&RemoteBinLifecycleTarget) -> Result<(), String>,
    Write: FnOnce(&StoredProfile) -> Result<(), String>,
{
    for target in filter_remote_bin_reconciliation_targets(targets) {
        reconcile_bucket(&target)?;
    }

    write_profile(&next)?;
    Ok(next)
}

pub(crate) fn load_credentials_for_remote_bin_target<R: Runtime>(
    app: &AppHandle<R>,
    target: &RemoteBinLifecycleTarget,
) -> Result<StoredCredentials, String> {
    let credential_id = target.credential_profile_id.as_deref().ok_or_else(|| {
        format!(
            "A saved credential is required to reconcile remote bin lifecycle for {} on bucket '{}'.",
            target_source_label(target), target.bucket
        )
    })?;

    load_credentials_by_id(app, credential_id)?.ok_or_else(|| {
        format!(
            "Credential '{}' for {} is unavailable.",
            credential_id,
            target_source_label(target)
        )
    })
}

pub(crate) fn provider_supports_remote_bin_lifecycle_reconciliation(provider: &str) -> bool {
    object_store::supports_remote_bin_lifecycle_reconciliation(provider)
}

pub(crate) fn remote_bin_lifecycle_reconciliation_unsupported_message(
    target: &RemoteBinLifecycleTarget,
) -> String {
    format!(
        "Provider '{}' does not support remote-bin lifecycle reconciliation for {} on bucket '{}'.",
        normalize_provider(&target.provider),
        target_source_label(target),
        target.bucket
    )
}

pub(crate) fn filter_remote_bin_reconciliation_targets(
    targets: Vec<RemoteBinLifecycleTarget>,
) -> Vec<RemoteBinLifecycleTarget> {
    targets
        .into_iter()
        .filter(|target| provider_supports_remote_bin_lifecycle_reconciliation(&target.provider))
        .collect()
}

#[cfg(test)]
pub(crate) fn persist_profile_with_remote_bin_reconciliation_for_test<Reconcile, Write>(
    current: &StoredProfile,
    next: StoredProfile,
    reconcile_bucket: Reconcile,
    write_profile: Write,
) -> Result<StoredProfile, String>
where
    Reconcile: FnMut(&RemoteBinLifecycleTarget) -> Result<(), String>,
    Write: FnOnce(&StoredProfile) -> Result<(), String>,
{
    persist_profile_with_remote_bin_reconciliation_inner(
        planned_remote_bin_reconciliation(current, &next),
        next,
        reconcile_bucket,
        write_profile,
    )
}
