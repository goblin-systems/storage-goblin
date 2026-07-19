# Baseline benchmark runner (backlog phase 0.5).
# Records: Rust scan/plan timings, frontend build time, bundle size, test-suite time.
# Cloud transfer baselines (1 GB up/down per provider) require credentials and a
# manual run — see backlog/phase-0-foundations.md.

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $repo

Write-Host "== Rust baseline probes (release) =="
cargo test --release --manifest-path src-tauri/Cargo.toml -- --ignored bench_ --nocapture --test-threads 1

Write-Host "`n== Frontend build time =="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
bun run build | Out-Null
$sw.Stop()
Write-Host ("[bench] bun run build: {0:n1}s" -f $sw.Elapsed.TotalSeconds)

if (Test-Path "$repo\dist") {
    $bytes = (Get-ChildItem "$repo\dist" -Recurse -File | Measure-Object Length -Sum).Sum
    Write-Host ("[bench] dist/ size: {0:n1} KB" -f ($bytes / 1KB))
}

Write-Host "`n== Frontend test suite time =="
$sw = [System.Diagnostics.Stopwatch]::StartNew()
bun run test | Out-Null
$sw.Stop()
Write-Host ("[bench] vitest suite: {0:n1}s" -f $sw.Elapsed.TotalSeconds)
