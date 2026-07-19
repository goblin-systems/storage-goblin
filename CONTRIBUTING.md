# Contributing to Storage Goblin

## Toolchain

- **Package manager: [bun](https://bun.sh)** — the only supported package manager.
  `bun.lock` is the single lockfile; do not create `package-lock.json` or `yarn.lock`.
- **Rust**: stable toolchain (see CI). The Tauri backend lives in `src-tauri/`.
- **Node tooling** runs through bun (`bun run <script>`, `bunx <tool>`).

## Setup

```sh
bun install
```

For the Tauri desktop shell you also need the platform prerequisites from
<https://v2.tauri.app/start/prerequisites/>.

## Running

```sh
bun run tauri dev     # full desktop app (Vite + Rust backend)
bun run dev           # frontend only (no IPC backend — limited usefulness)
```

## Tests & checks

Everything CI runs, runnable locally:

```sh
bun run test          # frontend unit tests (vitest)
bun run test:rust     # backend tests (cargo test)
bun run typecheck     # tsc --noEmit
bun run lint          # ESLint
bun run format:check  # Prettier (use `bun run format` to fix)
cargo fmt --manifest-path src-tauri/Cargo.toml --check
cargo clippy --manifest-path src-tauri/Cargo.toml --all-targets -- -D warnings
```

Coverage:

```sh
bun run test:coverage                              # vitest coverage → coverage/
cargo llvm-cov --manifest-path src-tauri/Cargo.toml  # requires cargo-llvm-cov
```

## Sync-engine changes

The sync engine (planner, queue execution, provider adapters) is covered by a simulator
harness in `src-tauri/src/storage/sim/` that runs plans against an in-memory object store —
no cloud credentials needed. **Any change to sync behavior must come with simulator
coverage.** See `backlog/phase-0-foundations.md` (workstream 0.2).

## Branching & commits

- Overhaul work happens on `overhaul/phase-N` epic branches; PRs into `master`; CI must be
  green (all gates above).
- Conventional-commit style subjects (`feat:`, `fix:`, `chore:`, `docs:`, `refactor:`).
- The roadmap/backlog lives in `backlog/`; update the relevant phase doc when you complete
  or reshape a task. Agent-facing conventions live in `AGENTS.md`.
