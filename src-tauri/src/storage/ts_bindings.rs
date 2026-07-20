//! Generates the TypeScript union types for the domain enums (phase 3.2).
//!
//! The frontend needs the same vocabulary as the backend, and hand-mirroring
//! it is how the two drift. Rather than pull in a codegen dependency and
//! regenerate all of `types.ts` (which carries hand-written normalizers we
//! want to keep), this renders only the enums into a small generated module.
//!
//! `generated_domain_bindings_are_up_to_date` fails if the checked-in file no
//! longer matches the enums, so adding or renaming a variant without
//! regenerating is a test failure rather than a runtime surprise. Regenerate
//! with:
//!
//! ```sh
//! cargo test --manifest-path src-tauri/Cargo.toml -- --ignored regenerate_
//! ```

#![cfg(test)]

use super::model::{ConflictStrategy, EntryKind, FileEntryStatus, QueueStatus, SyncPhase};

const GENERATED_PATH: &str = "../src/app/generated/domain.ts";

fn union(name: &str, values: &[&str]) -> String {
    let body = values
        .iter()
        .map(|value| format!("  | \"{value}\""))
        .collect::<Vec<_>>()
        .join("\n");
    format!("export type {name} =\n{body};\n")
}

fn const_array(name: &str, type_name: &str, values: &[&str]) -> String {
    let body = values
        .iter()
        .map(|value| format!("\"{value}\""))
        .collect::<Vec<_>>()
        .join(", ");
    format!("export const {name}: readonly {type_name}[] = [{body}] as const;\n")
}

fn render() -> String {
    let entry_kinds = [EntryKind::File, EntryKind::Directory].map(EntryKind::as_str);
    let phases = [
        SyncPhase::Unconfigured,
        SyncPhase::Idle,
        SyncPhase::Polling,
        SyncPhase::Syncing,
        SyncPhase::Paused,
        SyncPhase::Error,
    ]
    .map(SyncPhase::as_str);
    let strategies = [
        ConflictStrategy::PreserveBoth,
        ConflictStrategy::PreferLocal,
        ConflictStrategy::PreferRemote,
    ]
    .map(ConflictStrategy::as_str);
    let statuses = [
        FileEntryStatus::Synced,
        FileEntryStatus::LocalOnly,
        FileEntryStatus::RemoteOnly,
        FileEntryStatus::ReviewRequired,
        FileEntryStatus::Conflict,
        FileEntryStatus::Glacier,
        FileEntryStatus::Deleted,
    ]
    .map(FileEntryStatus::as_str);
    let queue_statuses = [
        QueueStatus::Planned,
        QueueStatus::InProgress,
        QueueStatus::Completed,
        QueueStatus::Failed,
        QueueStatus::Interrupted,
    ]
    .map(QueueStatus::as_str);

    let mut out = String::new();
    out.push_str(
        "// GENERATED FILE — do not edit by hand.\n\
         //\n\
         // Rendered from the Rust domain enums in src-tauri/src/storage/model.rs by\n\
         // storage::ts_bindings. Run `cargo test -- --ignored regenerate_` to update.\n\n",
    );
    out.push_str(&union("EntryKind", &entry_kinds));
    out.push('\n');
    out.push_str(&union("SyncPhase", &phases));
    out.push('\n');
    out.push_str(&union("ConflictStrategy", &strategies));
    out.push('\n');
    out.push_str(&const_array(
        "CONFLICT_STRATEGIES",
        "ConflictStrategy",
        &strategies,
    ));
    out.push('\n');
    out.push_str(&union("FileEntryStatus", &statuses));
    out.push('\n');
    out.push_str(&union("QueueStatus", &queue_statuses));
    out
}

#[test]
fn generated_domain_bindings_are_up_to_date() {
    let expected = render();
    let actual = std::fs::read_to_string(GENERATED_PATH)
        .expect("generated domain bindings should exist; run the regenerate_ test");
    assert_eq!(
        actual.replace("\r\n", "\n"),
        expected,
        "src/app/generated/domain.ts is stale — the Rust domain enums changed. \
         Regenerate with: cargo test --manifest-path src-tauri/Cargo.toml -- --ignored regenerate_"
    );
}

#[test]
#[ignore = "writes src/app/generated/domain.ts — run explicitly after changing the domain enums"]
fn regenerate_domain_bindings() {
    let path = std::path::Path::new(GENERATED_PATH);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("generated directory should be creatable");
    }
    std::fs::write(path, render()).expect("generated bindings should be writable");
    println!("[ts-bindings] wrote {GENERATED_PATH}");
}
