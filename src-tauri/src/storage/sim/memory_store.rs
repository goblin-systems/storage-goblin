use std::collections::BTreeMap;

use super::super::error::SyncError;
use super::super::local_index::bytes_fingerprint;
use super::object_storage::{ObjectRecord, ObjectStorage};

/// Which operation a call maps to, for failure injection and op accounting.
/// Variants for operations phase 1 introduces (delete/copy) are pre-declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum Op {
    List,
    Head,
    Get,
    Put,
    Delete,
    Copy,
}

/// How the store synthesizes etags.
///
/// `MultipartLike` proves that nothing in the engine may treat etags as
/// content hashes: real S3 multipart etags are not the object's md5.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum EtagMode {
    #[default]
    Md5Like,
    MultipartLike,
}

#[derive(Debug, Clone)]
struct StoredObject {
    bytes: Vec<u8>,
    etag: String,
    storage_class: Option<String>,
    fingerprint: String,
}

#[derive(Debug)]
struct InjectedFailure {
    op: Op,
    key_fragment: Option<String>,
    error: SyncError,
}

/// In-memory `ObjectStorage` with injectable failures and op accounting.
#[derive(Debug, Default)]
pub(crate) struct MemoryObjectStore {
    objects: BTreeMap<String, StoredObject>,
    failures: Vec<InjectedFailure>,
    etag_mode: EtagMode,
    put_generation: u64,
    pub op_counts: BTreeMap<&'static str, u64>,
}

impl MemoryObjectStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_etag_mode(etag_mode: EtagMode) -> Self {
        Self {
            etag_mode,
            ..Self::default()
        }
    }

    /// Queue a one-shot failure for the next matching operation.
    /// `key_fragment: None` matches any key.
    pub fn inject_failure(&mut self, op: Op, key_fragment: Option<&str>, error: SyncError) {
        self.failures.push(InjectedFailure {
            op,
            key_fragment: key_fragment.map(str::to_string),
            error,
        });
    }

    /// Seed an object directly (bypasses failure injection and op accounting),
    /// as if some other client had written it.
    pub fn seed(&mut self, key: &str, bytes: &[u8]) {
        let object = self.build_object(bytes);
        self.objects.insert(key.to_string(), object);
    }

    pub fn seed_with_storage_class(&mut self, key: &str, bytes: &[u8], storage_class: &str) {
        let mut object = self.build_object(bytes);
        object.storage_class = Some(storage_class.to_string());
        self.objects.insert(key.to_string(), object);
    }

    /// Remove an object directly, as if some other client had deleted it.
    pub fn seed_delete(&mut self, key: &str) {
        self.objects.remove(key);
    }

    pub fn keys(&self) -> Vec<String> {
        self.objects.keys().cloned().collect()
    }

    pub fn contents(&self, key: &str) -> Option<Vec<u8>> {
        self.objects.get(key).map(|object| object.bytes.clone())
    }

    pub fn etag(&self, key: &str) -> Option<String> {
        self.objects.get(key).map(|object| object.etag.clone())
    }

    fn build_object(&mut self, bytes: &[u8]) -> StoredObject {
        self.put_generation += 1;
        let etag = match self.etag_mode {
            EtagMode::Md5Like => format!("\"{}\"", bytes_fingerprint(bytes)),
            EtagMode::MultipartLike => {
                // Opaque, content-independent, changes on every write.
                format!("\"sim-multipart-{}-2\"", self.put_generation)
            }
        };
        StoredObject {
            bytes: bytes.to_vec(),
            etag,
            storage_class: None,
            fingerprint: bytes_fingerprint(bytes),
        }
    }

    fn record(&mut self, key: &'static str) {
        *self.op_counts.entry(key).or_insert(0) += 1;
    }

    fn take_failure(&mut self, op: Op, key: &str) -> Option<SyncError> {
        let index = self.failures.iter().position(|failure| {
            failure.op == op
                && failure
                    .key_fragment
                    .as_deref()
                    .is_none_or(|fragment| key.contains(fragment))
        })?;
        Some(self.failures.remove(index).error)
    }

    fn record_for(&self, key: &str, object: &StoredObject) -> ObjectRecord {
        ObjectRecord {
            key: key.to_string(),
            size: object.bytes.len() as u64,
            etag: object.etag.clone(),
            storage_class: object.storage_class.clone(),
            fingerprint: Some(object.fingerprint.clone()),
        }
    }
}

impl ObjectStorage for MemoryObjectStore {
    fn list(&mut self, prefix: Option<&str>) -> Result<Vec<ObjectRecord>, SyncError> {
        self.record("list");
        if let Some(error) = self.take_failure(Op::List, prefix.unwrap_or("")) {
            return Err(error);
        }
        Ok(self
            .objects
            .iter()
            .filter(|(key, _)| prefix.is_none_or(|prefix| key.starts_with(prefix)))
            .map(|(key, object)| self.record_for(key, object))
            .collect())
    }

    fn head(&mut self, key: &str) -> Result<Option<ObjectRecord>, SyncError> {
        self.record("head");
        if let Some(error) = self.take_failure(Op::Head, key) {
            return Err(error);
        }
        Ok(self
            .objects
            .get(key)
            .map(|object| self.record_for(key, object)))
    }

    fn get(&mut self, key: &str) -> Result<Vec<u8>, SyncError> {
        self.record("get");
        if let Some(error) = self.take_failure(Op::Get, key) {
            return Err(error);
        }
        self.objects
            .get(key)
            .map(|object| object.bytes.clone())
            .ok_or_else(|| SyncError::not_found(format!("no such key '{key}'")))
    }

    fn put(&mut self, key: &str, bytes: &[u8]) -> Result<ObjectRecord, SyncError> {
        self.record("put");
        if let Some(error) = self.take_failure(Op::Put, key) {
            return Err(error);
        }
        let object = self.build_object(bytes);
        let record = self.record_for(key, &object);
        self.objects.insert(key.to_string(), object);
        Ok(record)
    }

    fn delete(&mut self, key: &str) -> Result<(), SyncError> {
        self.record("delete");
        if let Some(error) = self.take_failure(Op::Delete, key) {
            return Err(error);
        }
        self.objects.remove(key);
        Ok(())
    }

    fn copy(&mut self, from: &str, to: &str) -> Result<ObjectRecord, SyncError> {
        self.record("copy");
        if let Some(error) = self.take_failure(Op::Copy, from) {
            return Err(error);
        }
        let source = self
            .objects
            .get(from)
            .cloned()
            .ok_or_else(|| SyncError::not_found(format!("no such key '{from}'")))?;
        let record = self.record_for(to, &source);
        self.objects.insert(to.to_string(), source);
        Ok(record)
    }
}
