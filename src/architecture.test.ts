/**
 * Frontend structural gates (backlog phase 4.3).
 *
 * The Rust side has `module_size_test.rs`, and it has already paid for itself
 * twice by catching a module regrowing past its cap mid-change. This is the
 * same idea for `src/`: the 4,300-line `bootstrap.ts` closure is exactly what
 * happens without one.
 *
 * Every over-cap file is listed explicitly with its current size as the ceiling,
 * so each can only shrink. Deleting an entry as a file drops under the general
 * cap is the visible progress marker for phase 4.
 */

import { readdirSync, readFileSync, statSync } from "node:fs";
import { join, relative, sep } from "node:path";

import { describe, expect, it } from "vitest";

// Resolved from the vitest root rather than import.meta.url, which is not a
// file URL under the jsdom environment this suite runs in.
const SRC_ROOT = join(process.cwd(), "src");

/** Lines allowed in an ordinary source file. */
const MAX_LINES = 500;

/**
 * Files that predate the gate, capped at their size when it was introduced.
 *
 * These are the phase-4 worklist. Each number may only go down; lowering one
 * after a change is the point, and raising one needs a reason in review.
 */
const LEGACY_CAPS: Record<string, number> = {
  // The monolith phase 4 exists to dismantle, plus its mirror-image test file.
  "app/bootstrap.ts": 4330,
  "app/bootstrap.test.ts": 4530,
  // Components with real internal complexity; phase 4.2 moves them under
  // components/ and splits rendering from state.
  "app/file-tree-virtual.ts": 915,
  "app/file-tree-virtual.test.ts": 845,
  "app/file-tree.ts": 775,
  "app/file-tree.test.ts": 905,
  // Hand-written types + normalizers; shrinks as generation expands (4.1).
  "app/types.ts": 810,
  // The IPC client; splits by domain in 4.1.
  "app/client.ts": 600,
  "app/client.test.ts": 605,
};

function sourceFiles(directory: string, found: string[] = []): string[] {
  for (const entry of readdirSync(directory)) {
    const full = join(directory, entry);
    if (statSync(full).isDirectory()) {
      // Generated output is exempt: its size is the generator's business.
      if (entry === "generated" || entry === "node_modules") continue;
      sourceFiles(full, found);
      continue;
    }
    if (entry.endsWith(".ts")) found.push(full);
  }
  return found;
}

function lineCount(path: string): number {
  return readFileSync(path, "utf8").split("\n").length;
}

describe("frontend module sizes", () => {
  it("keeps every file under its cap", () => {
    const violations: string[] = [];

    for (const file of sourceFiles(SRC_ROOT)) {
      const key = relative(SRC_ROOT, file).split(sep).join("/");
      const cap = LEGACY_CAPS[key] ?? MAX_LINES;
      const lines = lineCount(file);
      if (lines > cap) {
        violations.push(`${key}: ${lines} lines (cap ${cap})`);
      }
    }

    expect(violations, "split these into focused modules").toEqual([]);
  });

  it("has no stale entries in the legacy list", () => {
    // A cap for a file that no longer exists hides the fact that it was split,
    // and a cap far above the real size stops being a ratchet.
    const stale: string[] = [];

    for (const key of Object.keys(LEGACY_CAPS)) {
      const full = join(SRC_ROOT, ...key.split("/"));
      let lines: number;
      try {
        lines = lineCount(full);
      } catch {
        stale.push(`${key}: capped but no longer exists — remove the entry`);
        continue;
      }
      if (lines <= MAX_LINES) {
        stale.push(`${key}: now ${lines} lines, under the general cap — remove the entry`);
      }
    }

    expect(stale, "update LEGACY_CAPS").toEqual([]);
  });
});

describe("frontend layering", () => {
  it("keeps state, lib and ipc free of DOM access", () => {
    // These layers must be testable without jsdom and reusable by any view.
    // A `document` reference here is how a store turns into a renderer.
    const domTokens = ["document.", "window.", "HTMLElement"];
    const violations: string[] = [];

    for (const layer of ["state", "lib", "ipc"]) {
      let files: string[];
      try {
        files = sourceFiles(join(SRC_ROOT, layer));
      } catch {
        continue;
      }

      for (const file of files) {
        if (file.endsWith(".test.ts")) continue;
        const source = readFileSync(file, "utf8");
        for (const token of domTokens) {
          if (source.includes(token)) {
            violations.push(`${relative(SRC_ROOT, file).split(sep).join("/")} references ${token}`);
          }
        }
      }
    }

    expect(violations, "move DOM work into a view").toEqual([]);
  });
});
