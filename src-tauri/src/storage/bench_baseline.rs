//! Baseline performance probes (backlog phase 0.5).
//!
//! Not micro-benchmarks — coarse, rerunnable timings that phase 2 and phase 6
//! compare against. Run via `scripts/bench/run-benches.ps1`, or directly:
//!
//! ```sh
//! cargo test --release --manifest-path src-tauri/Cargo.toml -- --ignored bench_ --nocapture
//! ```
//!
//! Results are recorded in backlog/phase-0-foundations.md.

#![cfg(test)]

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use super::local_index::{
    scan_local_folder, LocalIndexEntry, LocalIndexSnapshot, LocalIndexSummary,
};
use super::now_iso;
use super::remote_index::{RemoteIndexSnapshot, RemoteIndexSummary, RemoteObjectEntry};
use super::sync_planner::build_sync_plan;

fn bench_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("storage-goblin-bench-{name}"))
}

/// Times a full local scan (walk + SHA-256 fingerprinting) of 10,000 small
/// files spread over 100 directories.
#[test]
#[ignore = "baseline bench — run explicitly with --ignored --nocapture"]
fn bench_scan_10k_files() {
    let root = bench_root("scan-10k");
    let _ = fs::remove_dir_all(&root);

    let setup_started = Instant::now();
    for dir_index in 0..100 {
        let dir = root.join(format!("dir-{dir_index:03}"));
        fs::create_dir_all(&dir).expect("create bench dir");
        for file_index in 0..100 {
            fs::write(
                dir.join(format!("file-{file_index:03}.txt")),
                format!("bench contents {dir_index}/{file_index}"),
            )
            .expect("write bench file");
        }
    }
    println!("[bench] fixture setup: {:?}", setup_started.elapsed());

    let started = Instant::now();
    let snapshot = scan_local_folder(&root).expect("scan");
    let elapsed = started.elapsed();

    assert_eq!(snapshot.summary.file_count, 10_000);
    println!(
        "[bench] scan_local_folder: 10,000 files in {elapsed:?} ({:.0} files/s)",
        10_000.0 / elapsed.as_secs_f64()
    );

    let _ = fs::remove_dir_all(&root);
}

/// Times plan building over synthetic 100k-entry snapshots (no filesystem):
/// 50k in sync, 25k local-only, 25k remote-only.
#[test]
#[ignore = "baseline bench — run explicitly with --ignored --nocapture"]
fn bench_plan_100k_entries() {
    let mut local_entries = Vec::new();
    let mut remote_entries = Vec::new();
    let mut anchors = BTreeMap::new();

    for index in 0..75_000_u32 {
        // 0..50k exist on both sides and are anchored; 50k..75k local-only.
        let path = format!("tree/{}/file-{index}.dat", index % 500);
        local_entries.push(LocalIndexEntry {
            relative_path: path.clone(),
            kind: "file".into(),
            size: 1_024,
            modified_at: None,
            fingerprint: Some(format!("fp-{index}")),
        });
        if index < 50_000 {
            remote_entries.push(RemoteObjectEntry {
                key: path.clone(),
                relative_path: path.clone(),
                kind: "file".into(),
                size: 1_024,
                last_modified_at: None,
                etag: Some(format!("etag-{index}")),
                storage_class: None,
            });
            anchors.insert(
                path.clone(),
                super::sync_db::SyncAnchor {
                    path,
                    kind: "file".into(),
                    local_fingerprint: Some(format!("fp-{index}")),
                    remote_etag: Some(format!("etag-{index}")),
                    synced_at: now_iso(),
                },
            );
        }
    }
    for index in 75_000..100_000_u32 {
        // remote-only
        let path = format!("tree/{}/file-{index}.dat", index % 500);
        remote_entries.push(RemoteObjectEntry {
            key: path.clone(),
            relative_path: path,
            kind: "file".into(),
            size: 1_024,
            last_modified_at: None,
            etag: Some(format!("etag-{index}")),
            storage_class: None,
        });
    }

    let local = LocalIndexSnapshot {
        version: 2,
        root_folder: "bench://local".into(),
        summary: LocalIndexSummary {
            indexed_at: now_iso(),
            file_count: local_entries.len() as u64,
            directory_count: 0,
            total_bytes: 0,
        },
        entries: local_entries,
    };
    let remote = RemoteIndexSnapshot {
        version: 1,
        bucket: "bench".into(),
        excluded_prefixes: Vec::new(),
        summary: RemoteIndexSummary {
            indexed_at: now_iso(),
            object_count: remote_entries.len() as u64,
            total_bytes: 0,
        },
        entries: remote_entries,
    };

    let started = Instant::now();
    let plan = build_sync_plan(&local, &remote, &anchors, "preserve-both", true);
    let elapsed = started.elapsed();

    assert_eq!(plan.summary.upload_count, 25_000);
    assert_eq!(plan.summary.download_count, 25_000);
    assert_eq!(plan.summary.noop_count, 50_000);
    println!(
        "[bench] build_sync_plan: 100,000 observed paths in {elapsed:?} ({:.0} paths/s)",
        100_000.0 / elapsed.as_secs_f64()
    );
}
