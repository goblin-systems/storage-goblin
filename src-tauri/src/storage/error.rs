use serde::Serialize;
use std::fmt;

/// Stable error classification for sync operations (backlog phase 0.4).
///
/// The kind determines retry/recovery behavior:
/// - `Auth` / `Config`: fail fast, pause the pair, require user action.
/// - `NotFound`: usually means the plan is stale; re-plan.
/// - `Precondition`: an If-Match / generation guard failed; re-plan.
/// - `Offline`: the network itself is unreachable; retry, but say so.
/// - `Transient`: throttling / 5xx; retry with backoff.
/// - `Storage`: local disk problems (permissions, disk full, missing files).
/// - `Internal`: everything not yet classified. The `From<String>` escape hatch
///   lands here so `Result<_, String>` call sites can migrate incrementally.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SyncErrorKind {
    Auth,
    NotFound,
    Precondition,
    Transient,
    Offline,
    Storage,
    Config,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncError {
    pub kind: SyncErrorKind,
    pub message: String,
    /// Server-requested retry delay (from Retry-After), when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_seconds: Option<u64>,
}

impl SyncError {
    pub fn new(kind: SyncErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            retry_after_seconds: None,
        }
    }

    pub fn auth(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Auth, message)
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::NotFound, message)
    }

    pub fn precondition(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Precondition, message)
    }

    pub fn transient(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Transient, message)
    }

    /// The network is unreachable — no DNS, no route, connection refused.
    ///
    /// Separate from `Transient` because the *cause* is on this side and the
    /// user can be told something true and useful ("waiting for a connection")
    /// instead of a provider error they cannot act on.
    pub fn offline(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Offline, message)
    }

    pub fn storage(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Storage, message)
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Config, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(SyncErrorKind::Internal, message)
    }

    pub fn with_retry_after(mut self, seconds: Option<u64>) -> Self {
        self.retry_after_seconds = seconds;
        self
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self.kind, SyncErrorKind::Transient | SyncErrorKind::Offline)
    }

    /// Does this mean we could not reach the provider at all?
    pub fn is_offline(&self) -> bool {
        self.kind == SyncErrorKind::Offline
    }

    /// Classify a `reqwest` failure.
    ///
    /// A connect failure means we never reached the server; a timeout usually
    /// means the same in practice (a dead link looks like a stalled one). Both
    /// are reported as offline so the pair can say why it is waiting rather
    /// than showing a transport error the user cannot act on.
    pub fn from_reqwest(error: &reqwest::Error, message: impl Into<String>) -> Self {
        if error.is_connect() || error.is_timeout() {
            Self::offline(message)
        } else {
            Self::transient(message)
        }
    }

    /// Classify an HTTP status code (provider REST APIs).
    pub fn from_http_status(status: u16, message: impl Into<String>) -> Self {
        let kind = match status {
            401 | 403 => SyncErrorKind::Auth,
            404 | 410 => SyncErrorKind::NotFound,
            409 | 412 => SyncErrorKind::Precondition,
            408 | 425 | 429 | 500..=599 => SyncErrorKind::Transient,
            _ => SyncErrorKind::Internal,
        };
        Self::new(kind, message)
    }
}

impl fmt::Display for SyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Display is exactly the message so the String escape hatch and the
        // existing activity/UI surfaces keep their current wording.
        f.write_str(&self.message)
    }
}

impl std::error::Error for SyncError {}

/// Escape hatch: unclassified legacy `String` errors become `Internal`.
impl From<String> for SyncError {
    fn from(message: String) -> Self {
        Self::internal(message)
    }
}

impl From<&str> for SyncError {
    fn from(message: &str) -> Self {
        Self::internal(message.to_string())
    }
}

/// Escape hatch in the other direction: callers still on `Result<_, String>`
/// receive the plain message, exactly as before the taxonomy existed.
impl From<SyncError> for String {
    fn from(error: SyncError) -> Self {
        error.message
    }
}

impl From<std::io::Error> for SyncError {
    fn from(error: std::io::Error) -> Self {
        Self::storage(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{SyncError, SyncErrorKind};

    #[test]
    fn http_status_classification_covers_the_retry_relevant_ranges() {
        assert_eq!(
            SyncError::from_http_status(401, "x").kind,
            SyncErrorKind::Auth
        );
        assert_eq!(
            SyncError::from_http_status(403, "x").kind,
            SyncErrorKind::Auth
        );
        assert_eq!(
            SyncError::from_http_status(404, "x").kind,
            SyncErrorKind::NotFound
        );
        assert_eq!(
            SyncError::from_http_status(412, "x").kind,
            SyncErrorKind::Precondition
        );
        assert_eq!(
            SyncError::from_http_status(429, "x").kind,
            SyncErrorKind::Transient
        );
        assert_eq!(
            SyncError::from_http_status(503, "x").kind,
            SyncErrorKind::Transient
        );
        assert_eq!(
            SyncError::from_http_status(400, "x").kind,
            SyncErrorKind::Internal
        );
    }

    #[test]
    fn string_round_trip_preserves_message() {
        let error: SyncError = String::from("boom").into();
        assert_eq!(error.kind, SyncErrorKind::Internal);
        let back: String = error.into();
        assert_eq!(back, "boom");
    }

    #[test]
    fn only_self_resolving_errors_are_retryable() {
        assert!(SyncError::transient("x").is_retryable());
        // Offline resolves itself the moment the network returns.
        assert!(SyncError::offline("x").is_retryable());
        assert!(!SyncError::auth("x").is_retryable());
        assert!(!SyncError::storage("x").is_retryable());
    }

    #[test]
    fn offline_is_distinguishable_from_a_provider_fault() {
        // The two are both retryable, but only one of them is something the
        // user can be told a true and useful thing about.
        assert!(SyncError::offline("no route to host").is_offline());
        assert!(!SyncError::transient("503 from provider").is_offline());
        assert!(!SyncError::auth("denied").is_offline());
    }

    #[test]
    fn a_server_that_answered_at_all_is_never_classified_offline() {
        // Any HTTP status means we reached something, so none of these may be
        // reported as "no network" — that would be a lie the user acts on.
        for status in [401, 403, 404, 409, 412, 429, 500, 503] {
            assert!(
                !SyncError::from_http_status(status, "x").is_offline(),
                "status {status} must not be classified offline"
            );
        }
    }

    #[test]
    fn serializes_a_stable_wire_shape() {
        let json =
            serde_json::to_value(SyncError::transient("slow down").with_retry_after(Some(2)))
                .expect("serialize");
        assert_eq!(json["kind"], "transient");
        assert_eq!(json["message"], "slow down");
        assert_eq!(json["retryAfterSeconds"], 2);
    }
}
