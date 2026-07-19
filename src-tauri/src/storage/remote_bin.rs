use aws_sdk_s3::types::{
    ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
};
use std::collections::BTreeMap;

pub const REMOTE_BIN_NAMESPACE: &str = ".storage-goblin-bin";
pub const REMOTE_BIN_LIFECYCLE_RULE_ID: &str = "storage-goblin-remote-bin-expiration";
pub const DEFAULT_REMOTE_BIN_PAIR_ID: &str = "default";
const REMOTE_BIN_PAIRS_SEGMENT: &str = "pairs";
const LEGACY_NAMESPACE_RULE_KEY: &str = "__storage_goblin_legacy_namespace__";

#[derive(Debug, Clone, PartialEq)]
pub enum LifecycleRulesChange {
    None,
    Replace(Vec<LifecycleRule>),
    DeleteBucketLifecycle,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ManagedLifecycleRulePlan {
    pub pair_id: String,
    pub prefix: String,
    pub retention_days: u32,
}

pub fn namespace_prefix() -> String {
    format!("{REMOTE_BIN_NAMESPACE}/")
}

fn pair_namespace_prefix() -> String {
    format!("{}{REMOTE_BIN_PAIRS_SEGMENT}/", namespace_prefix())
}

pub fn pair_bin_prefix(pair_id: &str) -> String {
    format!("{}{}/", pair_namespace_prefix(), pair_id.trim_matches('/'))
}

pub fn managed_lifecycle_rule_plan(pair_id: &str, retention_days: u32) -> ManagedLifecycleRulePlan {
    let pair_id = normalized_pair_id(pair_id);
    ManagedLifecycleRulePlan {
        prefix: pair_bin_prefix(&pair_id),
        pair_id,
        retention_days,
    }
}

pub fn deleted_object_key(pair_id: &str, relative_path: &str) -> String {
    let normalized_path = relative_path
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();
    format!("{}{}", pair_bin_prefix(pair_id), normalized_path)
}

pub fn deleted_directory_key(pair_id: &str, relative_path: &str) -> String {
    let normalized_path = relative_path
        .replace('\\', "/")
        .trim_matches('/')
        .to_string();

    if normalized_path.is_empty() {
        pair_bin_prefix(pair_id)
    } else {
        format!("{}{}/", pair_bin_prefix(pair_id), normalized_path)
    }
}

pub fn original_relative_path_from_bin_key(bin_key: &str) -> Result<String, String> {
    let normalized_key = bin_key.replace('\\', "/");
    let prefix = namespace_prefix();

    if !normalized_key.starts_with(&prefix) {
        return Err(format!(
            "Bin key '{bin_key}' is outside the reserved '{prefix}' namespace."
        ));
    }

    let scoped_relative_key = normalized_key[prefix.len()..].trim_start_matches('/');

    let relative_path = if let Some(pair_scoped_key) =
        scoped_relative_key.strip_prefix(&format!("{REMOTE_BIN_PAIRS_SEGMENT}/"))
    {
        let (pair_id, relative_path) = pair_scoped_key.split_once('/').ok_or_else(|| {
            format!("Bin key '{bin_key}' does not reference a restorable object.")
        })?;

        if pair_id.trim_matches('/').is_empty() {
            return Err(format!(
                "Bin key '{bin_key}' does not reference a restorable object."
            ));
        }

        relative_path.trim_start_matches('/').to_string()
    } else {
        scoped_relative_key.to_string()
    };

    if relative_path.trim_matches('/').is_empty() {
        return Err(format!(
            "Bin key '{bin_key}' does not reference a restorable object."
        ));
    }

    Ok(relative_path)
}

pub fn original_relative_path_from_bin_key_for_pair(
    pair_id: &str,
    bin_key: &str,
) -> Result<String, String> {
    let normalized_key = bin_key.replace('\\', "/");
    let pair_prefix = pair_bin_prefix(pair_id);

    if normalized_key.starts_with(&pair_prefix) {
        return original_relative_path_from_bin_key(&normalized_key);
    }

    if normalized_key.starts_with(&pair_namespace_prefix()) {
        return Err(format!(
            "Bin key '{bin_key}' does not belong to sync pair '{pair_id}'."
        ));
    }

    original_relative_path_from_bin_key(&normalized_key)
}

pub fn normalize_bin_path(path: &str) -> Result<String, String> {
    let normalized = path.replace('\\', "/").trim_matches('/').to_string();

    if normalized.is_empty() {
        return Err("Bin path must reference a non-empty relative path.".into());
    }

    Ok(normalized)
}

pub fn bin_prefix_for_path(pair_id: &str, path: &str) -> Result<String, String> {
    let normalized = normalize_bin_path(path)?;
    Ok(format!("{}{normalized}", pair_bin_prefix(pair_id)))
}

pub fn bin_prefix_contains_bin_key(
    pair_id: &str,
    path: &str,
    bin_key: &str,
) -> Result<bool, String> {
    let normalized_key = bin_key.replace('\\', "/");
    let prefix = bin_prefix_for_path(pair_id, path)?;

    Ok(normalized_key == prefix || normalized_key.starts_with(&format!("{prefix}/")))
}

pub fn managed_lifecycle_rule(plan: &ManagedLifecycleRulePlan) -> LifecycleRule {
    managed_lifecycle_rule_with_id(
        &managed_lifecycle_rule_id(&plan.pair_id),
        &plan.prefix,
        plan.retention_days,
    )
}

fn legacy_managed_lifecycle_rule(retention_days: u32) -> LifecycleRule {
    managed_lifecycle_rule_with_id(
        REMOTE_BIN_LIFECYCLE_RULE_ID,
        &namespace_prefix(),
        retention_days,
    )
}

fn managed_lifecycle_rule_with_id(
    rule_id: &str,
    prefix: &str,
    retention_days: u32,
) -> LifecycleRule {
    let expiration_days = i32::try_from(retention_days).unwrap_or(i32::MAX);
    LifecycleRule::builder()
        .id(rule_id)
        .filter(LifecycleRuleFilter::builder().prefix(prefix).build())
        .expiration(LifecycleExpiration::builder().days(expiration_days).build())
        .status(ExpirationStatus::Enabled)
        .build()
        .expect("storage goblin lifecycle rule should be valid")
}

pub fn upsert_lifecycle_rules(
    existing: &[LifecycleRule],
    managed_rules: &[ManagedLifecycleRulePlan],
) -> Vec<LifecycleRule> {
    let mut desired_rules = managed_rules
        .iter()
        .map(|plan| {
            (
                managed_rule_key(&plan.pair_id),
                managed_lifecycle_rule(plan),
            )
        })
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

pub fn remove_managed_lifecycle_rules(existing: &[LifecycleRule]) -> Vec<LifecycleRule> {
    existing
        .iter()
        .filter(|rule| managed_rule_key_for_existing_rule(rule).is_none())
        .cloned()
        .collect()
}

pub fn reconcile_lifecycle_rules(
    existing: Option<&[LifecycleRule]>,
    managed_rules: &[ManagedLifecycleRulePlan],
) -> LifecycleRulesChange {
    let existing_rules = existing.unwrap_or(&[]);

    if managed_rules.is_empty() {
        let cleaned = remove_managed_lifecycle_rules(existing_rules);
        if cleaned == existing_rules {
            LifecycleRulesChange::None
        } else if cleaned.is_empty() {
            LifecycleRulesChange::DeleteBucketLifecycle
        } else {
            LifecycleRulesChange::Replace(cleaned)
        }
    } else {
        let merged = upsert_lifecycle_rules(existing_rules, managed_rules);
        if merged == existing_rules {
            LifecycleRulesChange::None
        } else {
            LifecycleRulesChange::Replace(merged)
        }
    }
}

fn normalized_pair_id(pair_id: &str) -> String {
    let normalized = pair_id.trim_matches('/');
    if normalized.is_empty() {
        DEFAULT_REMOTE_BIN_PAIR_ID.into()
    } else {
        normalized.into()
    }
}

fn managed_lifecycle_rule_id(pair_id: &str) -> String {
    format!(
        "{REMOTE_BIN_LIFECYCLE_RULE_ID}-{}",
        normalized_pair_id(pair_id)
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() || character == '-' {
                    character
                } else {
                    '-'
                }
            })
            .collect::<String>()
    )
}

fn managed_rule_key(pair_id: &str) -> String {
    normalized_pair_id(pair_id)
}

fn managed_rule_key_for_existing_rule(rule: &LifecycleRule) -> Option<String> {
    if is_legacy_storage_goblin_rule(rule) {
        Some(LEGACY_NAMESPACE_RULE_KEY.into())
    } else {
        rule.id()
            .and_then(pair_id_from_managed_rule_id)
            .map(str::to_string)
    }
}

fn pair_id_from_managed_rule_id(rule_id: &str) -> Option<&str> {
    rule_id
        .strip_prefix(&format!("{REMOTE_BIN_LIFECYCLE_RULE_ID}-"))
        .filter(|pair_id| !pair_id.is_empty())
}

#[allow(deprecated)]
fn lifecycle_rule_prefix(rule: &LifecycleRule) -> Option<&str> {
    rule.filter()
        .and_then(LifecycleRuleFilter::prefix)
        .or(rule.prefix())
}

#[allow(deprecated)]
fn is_legacy_storage_goblin_rule(rule: &LifecycleRule) -> bool {
    let namespace_prefix = namespace_prefix();
    rule.id() == Some(REMOTE_BIN_LIFECYCLE_RULE_ID)
        || lifecycle_rule_prefix(rule) == Some(namespace_prefix.as_str())
}

pub fn key_matches_excluded_prefix(key: &str, excluded_prefixes: &[String]) -> bool {
    let normalized_key = key.replace('\\', "/");
    excluded_prefixes
        .iter()
        .map(|prefix| prefix.replace('\\', "/").trim_matches('/').to_string())
        .filter(|prefix| !prefix.is_empty())
        .any(|prefix| normalized_key == prefix || normalized_key.starts_with(&format!("{prefix}/")))
}

#[cfg(test)]
fn legacy_lifecycle_rule_for_test(retention_days: u32) -> LifecycleRule {
    legacy_managed_lifecycle_rule(retention_days)
}

#[cfg(test)]
fn managed_lifecycle_rule_id_for_test(pair_id: &str) -> String {
    managed_lifecycle_rule_id(pair_id)
}

#[cfg(test)]
fn managed_rule_key_for_test(rule: &LifecycleRule) -> Option<String> {
    managed_rule_key_for_existing_rule(rule)
}

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::{
        ExpirationStatus, LifecycleExpiration, LifecycleRule, LifecycleRuleFilter,
    };

    use super::{
        bin_prefix_contains_bin_key, bin_prefix_for_path, deleted_directory_key,
        deleted_object_key, key_matches_excluded_prefix, legacy_lifecycle_rule_for_test,
        managed_lifecycle_rule, managed_lifecycle_rule_id_for_test, managed_lifecycle_rule_plan,
        managed_rule_key_for_test, normalize_bin_path, original_relative_path_from_bin_key,
        original_relative_path_from_bin_key_for_pair, pair_bin_prefix, reconcile_lifecycle_rules,
        remove_managed_lifecycle_rules, upsert_lifecycle_rules, LifecycleRulesChange,
        DEFAULT_REMOTE_BIN_PAIR_ID,
    };

    #[test]
    fn pair_prefix_uses_pair_scoped_reserved_namespace() {
        assert_eq!(
            pair_bin_prefix("pair-1"),
            ".storage-goblin-bin/pairs/pair-1/"
        );
    }

    #[test]
    fn managed_lifecycle_rule_plan_scopes_to_pair_prefix() {
        let plan = managed_lifecycle_rule_plan("pair-1", 30);

        assert_eq!(plan.pair_id, "pair-1");
        assert_eq!(plan.prefix, ".storage-goblin-bin/pairs/pair-1/");
        assert_eq!(plan.retention_days, 30);
    }

    #[test]
    fn managed_lifecycle_rule_plan_defaults_empty_pair_id() {
        let plan = managed_lifecycle_rule_plan("", 14);

        assert_eq!(plan.pair_id, DEFAULT_REMOTE_BIN_PAIR_ID);
        assert_eq!(plan.prefix, ".storage-goblin-bin/pairs/default/");
    }

    #[test]
    fn deleted_object_key_nests_original_path_under_pair_namespace() {
        let key = deleted_object_key("pair-1", "docs\\note.txt");
        assert_eq!(key, ".storage-goblin-bin/pairs/pair-1/docs/note.txt");
    }

    #[test]
    fn deleted_directory_key_preserves_directory_placeholder_shape() {
        let key = deleted_directory_key("pair-1", "docs\\drafts");
        assert_eq!(key, ".storage-goblin-bin/pairs/pair-1/docs/drafts/");
    }

    #[test]
    fn original_relative_path_from_bin_key_maps_pair_scoped_and_legacy_keys_back_to_original_path()
    {
        assert_eq!(
            original_relative_path_from_bin_key(".storage-goblin-bin/pairs/pair-1/docs/note.txt")
                .expect("bin key should map to original path"),
            "docs/note.txt"
        );
        assert_eq!(
            original_relative_path_from_bin_key(".storage-goblin-bin/pairs/pair-1/docs/")
                .expect("directory placeholder should map to original path"),
            "docs/"
        );
        assert_eq!(
            original_relative_path_from_bin_key(".storage-goblin-bin/docs/note.txt")
                .expect("legacy bin key should still map to original path"),
            "docs/note.txt"
        );
    }

    #[test]
    fn original_relative_path_from_bin_key_rejects_non_bin_keys() {
        assert!(original_relative_path_from_bin_key("docs/note.txt").is_err());
        assert!(original_relative_path_from_bin_key(".storage-goblin-bin/").is_err());
        assert!(original_relative_path_from_bin_key(".storage-goblin-bin/pairs/pair-1/").is_err());
    }

    #[test]
    fn original_relative_path_from_bin_key_for_pair_rejects_other_pair_namespace() {
        let error = original_relative_path_from_bin_key_for_pair(
            "pair-1",
            ".storage-goblin-bin/pairs/pair-2/docs/note.txt",
        )
        .expect_err("other pair namespace should be rejected");

        assert!(error.contains("does not belong to sync pair 'pair-1'"));
    }

    #[test]
    fn original_relative_path_from_bin_key_for_pair_allows_legacy_flat_keys() {
        assert_eq!(
            original_relative_path_from_bin_key_for_pair(
                "pair-1",
                ".storage-goblin-bin/docs/note.txt"
            )
            .expect("legacy flat key should remain restorable"),
            "docs/note.txt"
        );
    }

    #[test]
    fn normalize_bin_path_rejects_empty_or_root_values() {
        assert_eq!(normalize_bin_path("docs\\drafts/").unwrap(), "docs/drafts");
        assert!(normalize_bin_path("").is_err());
        assert!(normalize_bin_path("/").is_err());
    }

    #[test]
    fn bin_prefix_for_path_scopes_prefix_to_pair_namespace() {
        assert_eq!(
            bin_prefix_for_path("pair-1", "docs/drafts").unwrap(),
            ".storage-goblin-bin/pairs/pair-1/docs/drafts"
        );
    }

    #[test]
    fn bin_prefix_contains_bin_key_matches_exact_and_descendant_keys() {
        assert!(bin_prefix_contains_bin_key(
            "pair-1",
            "docs/drafts",
            ".storage-goblin-bin/pairs/pair-1/docs/drafts/file.txt"
        )
        .unwrap());
        assert!(bin_prefix_contains_bin_key(
            "pair-1",
            "docs/drafts",
            ".storage-goblin-bin/pairs/pair-1/docs/drafts"
        )
        .unwrap());
        assert!(!bin_prefix_contains_bin_key(
            "pair-1",
            "docs/drafts",
            ".storage-goblin-bin/pairs/pair-1/docs/drafts-2/file.txt"
        )
        .unwrap());
    }

    #[test]
    fn excluded_prefix_matching_filters_nested_bin_objects() {
        assert!(key_matches_excluded_prefix(
            ".storage-goblin-bin/docs/note.txt",
            &[".storage-goblin-bin/".into()]
        ));
        assert!(!key_matches_excluded_prefix(
            "docs/note.txt",
            &[".storage-goblin-bin/".into()]
        ));
    }

    #[test]
    fn managed_lifecycle_rule_targets_pair_prefix() {
        let expected_id = managed_lifecycle_rule_id_for_test("pair-1");
        let rule = managed_lifecycle_rule(&managed_lifecycle_rule_plan("pair-1", 30));

        assert_eq!(rule.id(), Some(expected_id.as_str()));
        assert_eq!(
            rule.filter().and_then(|filter| filter.prefix()),
            Some(".storage-goblin-bin/pairs/pair-1/")
        );
        assert_eq!(
            rule.expiration().and_then(|expiration| expiration.days()),
            Some(30)
        );
        assert_eq!(rule.status(), &ExpirationStatus::Enabled);
    }

    #[test]
    fn lifecycle_rule_key_recognizes_pair_scoped_and_legacy_rules() {
        let managed_rule = managed_lifecycle_rule(&managed_lifecycle_rule_plan("pair-1", 7));
        let legacy_rule = legacy_lifecycle_rule_for_test(7);

        assert_eq!(
            managed_rule_key_for_test(&managed_rule).as_deref(),
            Some("pair-1")
        );
        assert!(managed_rule_key_for_test(&legacy_rule).is_some());
    }

    #[test]
    fn lifecycle_upsert_preserves_unrelated_rules_and_adds_managed_rules() {
        let unrelated_rule = LifecycleRule::builder()
            .id("user-archive-expiration")
            .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
            .expiration(LifecycleExpiration::builder().days(90).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .expect("user rule should build");

        let updated = upsert_lifecycle_rules(
            &[unrelated_rule.clone()],
            &[
                managed_lifecycle_rule_plan("pair-1", 14),
                managed_lifecycle_rule_plan("pair-2", 21),
            ],
        );

        assert_eq!(updated.len(), 3);
        assert_eq!(updated[0], unrelated_rule);
        assert_eq!(
            managed_rule_key_for_test(&updated[1]).as_deref(),
            Some("pair-1")
        );
        assert_eq!(
            managed_rule_key_for_test(&updated[2]).as_deref(),
            Some("pair-2")
        );
    }

    #[test]
    fn lifecycle_upsert_replaces_existing_storage_goblin_rules_and_removes_legacy_namespace_rule() {
        let old_pair_rule = managed_lifecycle_rule(&managed_lifecycle_rule_plan("pair-1", 7));
        let legacy_rule = legacy_lifecycle_rule_for_test(7);
        let unrelated_rule = LifecycleRule::builder()
            .id("user-archive-expiration")
            .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
            .expiration(LifecycleExpiration::builder().days(90).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .expect("user rule should build");

        let updated = upsert_lifecycle_rules(
            &[old_pair_rule, legacy_rule, unrelated_rule.clone()],
            &[
                managed_lifecycle_rule_plan("pair-1", 21),
                managed_lifecycle_rule_plan("pair-2", 30),
            ],
        );

        assert_eq!(updated.len(), 3);
        assert_eq!(
            updated[0]
                .expiration()
                .and_then(|expiration| expiration.days()),
            Some(21)
        );
        assert_eq!(updated[1], unrelated_rule);
        assert_eq!(
            managed_rule_key_for_test(&updated[2]).as_deref(),
            Some("pair-2")
        );
    }

    #[test]
    fn lifecycle_remove_managed_rules_preserves_unrelated_rules() {
        let managed_rule = managed_lifecycle_rule(&managed_lifecycle_rule_plan("pair-1", 7));
        let legacy_rule = legacy_lifecycle_rule_for_test(7);
        let unrelated_rule = LifecycleRule::builder()
            .id("user-archive-expiration")
            .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
            .expiration(LifecycleExpiration::builder().days(90).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .expect("user rule should build");

        let updated =
            remove_managed_lifecycle_rules(&[managed_rule, legacy_rule, unrelated_rule.clone()]);

        assert_eq!(updated, vec![unrelated_rule]);
    }

    #[test]
    fn lifecycle_reconcile_returns_none_when_desired_rules_already_match() {
        let existing = vec![
            managed_lifecycle_rule(&managed_lifecycle_rule_plan("pair-1", 30)),
            managed_lifecycle_rule(&managed_lifecycle_rule_plan("pair-2", 14)),
        ];

        assert_eq!(
            reconcile_lifecycle_rules(
                Some(&existing),
                &[
                    managed_lifecycle_rule_plan("pair-1", 30),
                    managed_lifecycle_rule_plan("pair-2", 14),
                ]
            ),
            LifecycleRulesChange::None
        );
    }

    #[test]
    fn lifecycle_reconcile_returns_none_when_disabled_and_no_managed_rule_exists() {
        let unrelated_rule = LifecycleRule::builder()
            .id("user-archive-expiration")
            .filter(LifecycleRuleFilter::builder().prefix("archive/").build())
            .expiration(LifecycleExpiration::builder().days(90).build())
            .status(ExpirationStatus::Enabled)
            .build()
            .expect("user rule should build");

        assert_eq!(
            reconcile_lifecycle_rules(Some(&[unrelated_rule]), &[]),
            LifecycleRulesChange::None
        );
        assert_eq!(
            reconcile_lifecycle_rules(None, &[]),
            LifecycleRulesChange::None
        );
    }

    #[test]
    fn lifecycle_reconcile_deletes_bucket_lifecycle_when_disabling_last_managed_rule() {
        let existing = vec![managed_lifecycle_rule(&managed_lifecycle_rule_plan(
            "pair-1", 7,
        ))];

        assert_eq!(
            reconcile_lifecycle_rules(Some(&existing), &[]),
            LifecycleRulesChange::DeleteBucketLifecycle
        );
    }

    #[test]
    fn lifecycle_reconcile_replaces_legacy_namespace_rule_with_pair_scoped_rules() {
        let existing = vec![legacy_lifecycle_rule_for_test(7)];

        let LifecycleRulesChange::Replace(updated) = reconcile_lifecycle_rules(
            Some(&existing),
            &[
                managed_lifecycle_rule_plan("pair-1", 7),
                managed_lifecycle_rule_plan("pair-2", 14),
            ],
        ) else {
            panic!("legacy namespace rule should be replaced");
        };

        assert_eq!(updated.len(), 2);
        assert_eq!(
            managed_rule_key_for_test(&updated[0]).as_deref(),
            Some("pair-1")
        );
        assert_eq!(
            managed_rule_key_for_test(&updated[1]).as_deref(),
            Some("pair-2")
        );
    }
}
