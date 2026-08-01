//! Module-size gate (backlog phase 3.3).
//!
//! The overhaul started with a single 12,404-line god module. This test keeps
//! that from creeping back: no storage module may exceed [`MAX_MODULE_LINES`]
//! lines of production code (the `#[cfg(test)]` module at the foot of a file
//! is not counted).
//!
//! `commands.rs` is allowlisted. It is the Tauri command surface — broad but
//! shallow — and the remaining reductions (splitting the credential,
//! version, and live-file command groups into services) are entangled with
//! profile/status helpers in a way that is not worth the churn on an
//! unvalidated branch. Its own cap keeps it from regressing while that work
//! is deferred.

#![cfg(test)]

use std::path::Path;

/// The ceiling for an ordinary storage module's production code.
const MAX_MODULE_LINES: usize = 1_300;

/// Modules with a documented higher ceiling. Each is a cohesive unit whose
/// size is inherent, not a mixing of concerns; the cap sits just above where
/// it is today so it can only shrink, never regrow.
///
/// - `commands.rs`: the Tauri command surface. Splitting the remaining
///   credential/version/live-file command groups into services is entangled
///   with profile/status helpers — deferred, not abandoned.
/// - `credentials_store.rs`: OS secure-store + Windows DPAPI crypto +
///   credential parsing/validation/migration. A real split (storage vs
///   platform crypto) is worthwhile but is its own task.
fn module_cap(name: &str) -> usize {
    match name {
        "commands.rs" => 2_200,
        "credentials_store.rs" => 1_850,
        _ => MAX_MODULE_LINES,
    }
}

/// Count lines up to the first test *module* (`#[cfg(test)] mod …`), so the
/// trailing test module does not count against production size. Scattered
/// test-only `use`s and helpers above it are a rounding error and are left in.
fn production_line_count(source: &str) -> usize {
    let lines: Vec<&str> = source.lines().collect();
    for (index, line) in lines.iter().enumerate() {
        let trimmed = line.trim_start();
        let is_test_cfg =
            trimmed.starts_with("#[cfg(test)]") || trimmed.starts_with("#[cfg(all(test");
        if !is_test_cfg {
            continue;
        }
        // Only a cfg(test) that introduces a module ends the production region.
        if let Some(next) = lines.get(index + 1) {
            if next.trim_start().starts_with("mod ") {
                return index;
            }
        }
    }
    lines.len()
}

#[test]
fn no_storage_module_grows_back_into_a_god_module() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/storage");
    let mut offenders = Vec::new();

    for entry in std::fs::read_dir(&dir).expect("storage dir should be readable") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string();
        // Test-only harnesses are not production surface.
        if name.ends_with("_test.rs") || name == "bench_baseline.rs" {
            continue;
        }

        let source = std::fs::read_to_string(&path).expect("module should be readable");
        let lines = production_line_count(&source);
        let cap = module_cap(&name);

        if lines > cap {
            offenders.push(format!("{name}: {lines} production lines (cap {cap})"));
        }
    }

    assert!(
        offenders.is_empty(),
        "modules exceeded their size cap — split them into cohesive services:\n{}",
        offenders.join("\n")
    );
}
