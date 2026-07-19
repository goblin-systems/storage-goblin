//! Sync-engine simulation harness (backlog phase 0.2).
//!
//! Lets sync behavior be exercised without cloud credentials: an in-memory
//! object store with injectable failures, plus a scenario runner that drives
//! `sync_planner::build_sync_plan` through plan → execute → assert cycles
//! against purely in-memory local and remote state.
//!
//! Scope notes:
//! - The [`object_storage::ObjectStorage`] trait is the seam the phase-3
//!   backend decomposition will promote out of this module and implement for
//!   the real S3/GCS adapters (async at that point). For now it is sync and
//!   test-only, consumed by the simulator.
//! - The simulator's executor applies planned operations with per-item error
//!   isolation. That is deliberately *better* than the current production
//!   executor (which aborts the queue on first failure — phase 2.2); the
//!   simulator documents planner semantics, not executor quirks.
//! - Any change to sync behavior must come with coverage here (CONTRIBUTING).

pub(crate) mod memory_store;
pub(crate) mod object_storage;
pub(crate) mod simulator;

mod scenarios;
