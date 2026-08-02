//! Architecture invariants enforced as tests (backlog phase 3.3).
//!
//! The phase-3 target is a layered backend with a Tauri-free core. The full
//! `engine/ services/ ipc/` directory move has not happened, but the invariant
//! that matters — that the pure decision logic never reaches for the app
//! framework — is enforced here regardless of where the files sit.
//!
//! If one of these modules starts needing `tauri`, that is a design smell:
//! the Tauri-dependent part belongs in a service, and the module should take
//! plain data. Splitting it that way is the fix, not adding `tauri` here.

#![cfg(test)]

use std::path::Path;

/// Modules that form the Tauri-free core: pure sync decisions, the domain
/// vocabulary, error taxonomy, inventory diffing, path sanitizing, and
/// lifecycle-rule algebra. None may depend on the Tauri app framework.
const TAURI_FREE_CORE: &[&str] = &[
    "model.rs",
    "sync_planner.rs",
    "error.rs",
    "inventory_compare.rs",
    "sanitizer.rs",
    "remote_bin.rs",
    "pair_backoff.rs",
    "coordinator.rs",
    "queue_schedule.rs",
    "retry.rs",
    "s3_upload.rs",
];

/// Tokens that signal a dependency on the Tauri app framework.
const TAURI_TOKENS: &[&str] = &["tauri::", "AppHandle", "tauri_plugin", "use tauri"];

#[test]
fn the_tauri_free_core_never_imports_tauri() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage");
    let mut offenders = Vec::new();

    for module in TAURI_FREE_CORE {
        let path = dir.join(module);
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("core module {module} should be readable: {error}"));

        for (line_no, line) in source.lines().enumerate() {
            // Comments and doc-comments may mention Tauri when explaining why
            // something is kept out of the core.
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") {
                continue;
            }
            for token in TAURI_TOKENS {
                if line.contains(token) {
                    offenders.push(format!("{module}:{}: {}", line_no + 1, line.trim()));
                }
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "the Tauri-free core reached for the app framework:\n{}\n\n\
         Move the Tauri-dependent logic into a service and pass plain data in.",
        offenders.join("\n")
    );
}

/// The simulator drives the same planner the app does, so the core must be
/// exercisable without a Tauri runtime. This is a canary: it fails to compile
/// if the core grows a Tauri dependency the test above somehow missed.
#[test]
fn the_core_planner_runs_without_a_tauri_runtime() {
    use super::sync_planner::build_sync_plan;
    use crate::storage::local_index::{LocalIndexSnapshot, LocalIndexSummary};
    use crate::storage::remote_index::{RemoteIndexSnapshot, RemoteIndexSummary};
    use std::collections::BTreeMap;

    let plan = build_sync_plan(
        &LocalIndexSnapshot {
            version: 2,
            root_folder: "sim://local".into(),
            summary: LocalIndexSummary::default(),
            entries: Vec::new(),
        },
        &RemoteIndexSnapshot {
            version: 1,
            bucket: "b".into(),
            excluded_prefixes: Vec::new(),
            summary: RemoteIndexSummary::default(),
            entries: Vec::new(),
        },
        &BTreeMap::new(),
        "preserve-both",
        true,
    );
    assert_eq!(plan.summary.pending_operation_count, 0);
}
