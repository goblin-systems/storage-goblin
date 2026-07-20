//! The typed domain vocabulary (backlog phase 3.2).
//!
//! Every variant serializes to the exact string the wire format and the
//! SQLite schema already used, so this is a compile-time change only —
//! persisted data and the IPC contract are untouched.
//!
//! The point is exhaustiveness: a `match` over these cannot silently miss a
//! case the way `==` against a string literal can, and adding a variant
//! becomes a compile error at every decision point instead of a runtime
//! surprise.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Whether an indexed entry is a file or a directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryKind {
    File,
    Directory,
}

impl EntryKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EntryKind::File => "file",
            EntryKind::Directory => "directory",
        }
    }

    /// Unrecognised values are treated as files: an entry we cannot classify
    /// is safer handled as content than as a container we might recurse into.
    pub fn parse_or_file(value: &str) -> Self {
        match value {
            "directory" => EntryKind::Directory,
            _ => EntryKind::File,
        }
    }

    pub fn is_directory(self) -> bool {
        matches!(self, EntryKind::Directory)
    }
}

impl fmt::Display for EntryKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Aggregate state of a sync location (and of the app as a whole).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncPhase {
    Unconfigured,
    Idle,
    Polling,
    Syncing,
    Paused,
    Error,
}

impl SyncPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncPhase::Unconfigured => "unconfigured",
            SyncPhase::Idle => "idle",
            SyncPhase::Polling => "polling",
            SyncPhase::Syncing => "syncing",
            SyncPhase::Paused => "paused",
            SyncPhase::Error => "error",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "unconfigured" => SyncPhase::Unconfigured,
            "idle" => SyncPhase::Idle,
            "polling" => SyncPhase::Polling,
            "syncing" => SyncPhase::Syncing,
            "paused" => SyncPhase::Paused,
            "error" => SyncPhase::Error,
            _ => return None,
        })
    }
}

/// Collapse several locations' phases into the one headline phase.
///
/// Active work outranks failure deliberately: a location still syncing is the
/// more useful thing to show while another has errored, and the error surfaces
/// through the activity log and that location's own row.
///
/// The "all" rule is not expressible as a ranking, which is why this is a
/// function: a set that is entirely paused or unconfigured reports paused,
/// while a single configured-and-idle location makes the whole set idle.
///
/// KNOWN QUIRK (preserved deliberately): a set of locations that are *all*
/// unconfigured also reports `Paused`, because it satisfies the paused-or-
/// unconfigured rule. The original code had an `all unconfigured` branch
/// after this one, which was therefore unreachable — it is dropped here
/// rather than reordered, because reordering would change what the UI shows.
/// Showing "Paused" for an app with no configured location is a real (if
/// minor) UX bug; phase 5 owns the status model and should fix it there.
pub fn aggregate_phase(phases: &[SyncPhase]) -> SyncPhase {
    if phases.is_empty() {
        return SyncPhase::Unconfigured;
    }
    if phases.contains(&SyncPhase::Syncing) {
        return SyncPhase::Syncing;
    }
    if phases.contains(&SyncPhase::Polling) {
        return SyncPhase::Polling;
    }
    if phases.contains(&SyncPhase::Error) {
        return SyncPhase::Error;
    }
    if phases
        .iter()
        .all(|phase| matches!(phase, SyncPhase::Paused | SyncPhase::Unconfigured))
    {
        return SyncPhase::Paused;
    }
    SyncPhase::Idle
}

impl fmt::Display for SyncPhase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a location resolves a file edited on both sides since the last sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ConflictStrategy {
    /// Keep both versions, renaming the local one (the product default).
    #[default]
    PreserveBoth,
    PreferLocal,
    PreferRemote,
}

impl ConflictStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            ConflictStrategy::PreserveBoth => "preserve-both",
            ConflictStrategy::PreferLocal => "prefer-local",
            ConflictStrategy::PreferRemote => "prefer-remote",
        }
    }

    /// Unknown strategies fall back to the safest option — keeping both
    /// versions never destroys data.
    pub fn parse_or_default(value: &str) -> Self {
        match value {
            "prefer-local" => ConflictStrategy::PreferLocal,
            "prefer-remote" => ConflictStrategy::PreferRemote,
            _ => ConflictStrategy::PreserveBoth,
        }
    }
}

impl fmt::Display for ConflictStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Per-file state shown in the file browser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FileEntryStatus {
    Synced,
    LocalOnly,
    RemoteOnly,
    ReviewRequired,
    Conflict,
    /// Held in a cold storage class, so it is skipped rather than synced.
    Glacier,
    /// Present in the remote bin, awaiting restore or expiry.
    Deleted,
}

impl FileEntryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            FileEntryStatus::Synced => "synced",
            FileEntryStatus::LocalOnly => "local-only",
            FileEntryStatus::RemoteOnly => "remote-only",
            FileEntryStatus::ReviewRequired => "review-required",
            FileEntryStatus::Conflict => "conflict",
            FileEntryStatus::Glacier => "glacier",
            FileEntryStatus::Deleted => "deleted",
        }
    }
}

impl fmt::Display for FileEntryStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle of one durable queue item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueStatus {
    Planned,
    InProgress,
    Completed,
    Failed,
    /// Was in progress when the app stopped; recovered on the next run.
    Interrupted,
}

impl QueueStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            QueueStatus::Planned => "planned",
            QueueStatus::InProgress => "in_progress",
            QueueStatus::Completed => "completed",
            QueueStatus::Failed => "failed",
            QueueStatus::Interrupted => "interrupted",
        }
    }

    /// Items an executor may pick up.
    pub fn is_runnable(self) -> bool {
        matches!(self, QueueStatus::Planned | QueueStatus::Interrupted)
    }
}

impl fmt::Display for QueueStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::{ConflictStrategy, EntryKind, FileEntryStatus, QueueStatus, SyncPhase};

    /// The wire format is load-bearing: these strings appear in persisted
    /// JSON, the SQLite schema, and the TypeScript union types.
    #[test]
    fn variants_serialize_to_their_established_wire_strings() {
        assert_eq!(EntryKind::File.as_str(), "file");
        assert_eq!(EntryKind::Directory.as_str(), "directory");
        assert_eq!(SyncPhase::Unconfigured.as_str(), "unconfigured");
        assert_eq!(SyncPhase::Polling.as_str(), "polling");
        assert_eq!(ConflictStrategy::PreserveBoth.as_str(), "preserve-both");
        assert_eq!(ConflictStrategy::PreferRemote.as_str(), "prefer-remote");
        assert_eq!(FileEntryStatus::LocalOnly.as_str(), "local-only");
        assert_eq!(FileEntryStatus::ReviewRequired.as_str(), "review-required");
        assert_eq!(QueueStatus::InProgress.as_str(), "in_progress");
    }

    #[test]
    fn serde_matches_as_str_for_every_variant() {
        fn round_trip<T>(value: T, expected: &str)
        where
            T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug + Copy,
        {
            let json = serde_json::to_string(&value).expect("serialize");
            assert_eq!(json, format!("\"{expected}\""));
            let back: T = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(back, value);
        }

        round_trip(EntryKind::File, "file");
        round_trip(EntryKind::Directory, "directory");
        for phase in [
            SyncPhase::Unconfigured,
            SyncPhase::Idle,
            SyncPhase::Polling,
            SyncPhase::Syncing,
            SyncPhase::Paused,
            SyncPhase::Error,
        ] {
            round_trip(phase, phase.as_str());
        }
        for strategy in [
            ConflictStrategy::PreserveBoth,
            ConflictStrategy::PreferLocal,
            ConflictStrategy::PreferRemote,
        ] {
            round_trip(strategy, strategy.as_str());
        }
        for status in [
            FileEntryStatus::Synced,
            FileEntryStatus::LocalOnly,
            FileEntryStatus::RemoteOnly,
            FileEntryStatus::ReviewRequired,
            FileEntryStatus::Conflict,
            FileEntryStatus::Glacier,
            FileEntryStatus::Deleted,
        ] {
            round_trip(status, status.as_str());
        }
        for status in [
            QueueStatus::Planned,
            QueueStatus::InProgress,
            QueueStatus::Completed,
            QueueStatus::Failed,
            QueueStatus::Interrupted,
        ] {
            round_trip(status, status.as_str());
        }
    }

    #[test]
    fn parsing_round_trips_and_rejects_unknown_values() {
        for phase in [SyncPhase::Idle, SyncPhase::Error] {
            assert_eq!(SyncPhase::parse(phase.as_str()), Some(phase));
        }
        assert_eq!(SyncPhase::parse("nonsense"), None);
    }

    #[test]
    fn unknown_kinds_and_strategies_fall_back_to_the_safe_option() {
        // An unclassifiable entry is content, not a container.
        assert_eq!(EntryKind::parse_or_file("weird"), EntryKind::File);
        // An unknown strategy must never destroy a version.
        assert_eq!(
            ConflictStrategy::parse_or_default("weird"),
            ConflictStrategy::PreserveBoth
        );
    }

    #[test]
    fn aggregate_phase_matches_the_established_precedence() {
        use super::aggregate_phase;

        assert_eq!(aggregate_phase(&[]), SyncPhase::Unconfigured);
        // Active work outranks failure.
        assert_eq!(
            aggregate_phase(&[SyncPhase::Error, SyncPhase::Syncing]),
            SyncPhase::Syncing
        );
        assert_eq!(
            aggregate_phase(&[SyncPhase::Error, SyncPhase::Polling]),
            SyncPhase::Polling
        );
        assert_eq!(
            aggregate_phase(&[SyncPhase::Idle, SyncPhase::Error]),
            SyncPhase::Error
        );
        // "All" rules: a mixed paused/unconfigured set reads as paused...
        assert_eq!(
            aggregate_phase(&[SyncPhase::Paused, SyncPhase::Unconfigured]),
            SyncPhase::Paused
        );
        // ...and so does an entirely unconfigured one — see the KNOWN QUIRK
        // on aggregate_phase. This asserts current behavior, not ideal
        // behavior; phase 5 should change it deliberately.
        assert_eq!(
            aggregate_phase(&[SyncPhase::Unconfigured, SyncPhase::Unconfigured]),
            SyncPhase::Paused
        );
        // ...and one working location makes the set idle.
        assert_eq!(
            aggregate_phase(&[SyncPhase::Unconfigured, SyncPhase::Idle]),
            SyncPhase::Idle
        );
    }

    #[test]
    fn only_planned_and_interrupted_items_are_runnable() {
        assert!(QueueStatus::Planned.is_runnable());
        assert!(QueueStatus::Interrupted.is_runnable());
        assert!(!QueueStatus::InProgress.is_runnable());
        assert!(!QueueStatus::Completed.is_runnable());
        assert!(!QueueStatus::Failed.is_runnable());
    }
}
