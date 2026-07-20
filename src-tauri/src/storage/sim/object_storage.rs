use super::super::error::SyncError;

/// A stored object as the storage backend reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ObjectRecord {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub storage_class: Option<String>,
    /// Goblin content fingerprint, as a provider exposing our upload metadata
    /// would report it (ADR-4).
    pub fingerprint: Option<String>,
}

/// Provider-neutral object storage operations.
///
/// This is the interface implied by the current `object_store.rs` seam
/// (list / head / get / put / delete / copy). Phase 3 promotes it out of the
/// test harness, makes it async, and implements it for the S3 and GCS
/// adapters; the simulator's `MemoryObjectStore` then doubles as the
/// conformance fixture for new providers (phase 6.5).
// delete/copy become live when phase 1 adds delete propagation and moves.
#[allow(dead_code)]
pub(crate) trait ObjectStorage {
    fn list(&mut self, prefix: Option<&str>) -> Result<Vec<ObjectRecord>, SyncError>;
    fn head(&mut self, key: &str) -> Result<Option<ObjectRecord>, SyncError>;
    fn get(&mut self, key: &str) -> Result<Vec<u8>, SyncError>;
    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<ObjectRecord, SyncError>;
    fn delete(&mut self, key: &str) -> Result<(), SyncError>;
    fn copy(&mut self, from: &str, to: &str) -> Result<ObjectRecord, SyncError>;
}
