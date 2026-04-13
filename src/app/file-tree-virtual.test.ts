import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// ---------------------------------------------------------------------------
// Mock the design system BEFORE any imports that use it
// ---------------------------------------------------------------------------

const { applyIconsMock, bindCheckboxTreeMock, createIconMock } = vi.hoisted(
  () => ({
    applyIconsMock: vi.fn(),
    bindCheckboxTreeMock: vi.fn(() => ({
      expand: vi.fn(),
      collapse: vi.fn(),
      expandAll: vi.fn(),
      collapseAll: vi.fn(),
      destroy: vi.fn(),
    })),
    createIconMock: vi.fn(() => {
      const svg = document.createElementNS("http://www.w3.org/2000/svg", "svg");
      return svg;
    }),
  }),
);

vi.mock("@goblin-systems/goblin-design-system", () => ({
  applyIcons: applyIconsMock,
  bindCheckboxTree: bindCheckboxTreeMock,
  createIcon: createIconMock,
}));

// ---------------------------------------------------------------------------
// Imports under test
// ---------------------------------------------------------------------------

import {
  flattenVisible,
  computeVisibleRange,
  VIRTUAL_THRESHOLD,
  renderFileTreeVirtual,
} from "./file-tree-virtual";
import { getStatusTooltip, renderFileTree, type FileEntry } from "./file-tree";
import { buildTree } from "./file-tree";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function fileEntry(overrides: Partial<FileEntry> & Pick<FileEntry, "path">): FileEntry {
  return {
    kind: "file",
    status: "synced",
    hasLocalCopy: true,
    ...overrides,
  };
}

function directoryEntry(overrides: Partial<FileEntry> & Pick<FileEntry, "path">): FileEntry {
  return {
    kind: "directory",
    status: "synced",
    hasLocalCopy: true,
    ...overrides,
  };
}

function generateEntries(count: number): FileEntry[] {
  return Array.from({ length: count }, (_, i) => fileEntry({
    path: `dir-${Math.floor(i / 100)}/file-${i}.txt`,
    hasLocalCopy: i % 2 === 0,
  }));
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("flattenVisible", () => {
  it("returns only root-level items when all collapsed", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "dir-a/file-1.txt" }),
      fileEntry({ path: "dir-a/file-2.txt", hasLocalCopy: false }),
      fileEntry({ path: "dir-b/file-3.txt" }),
    ];

    const roots = buildTree(entries);
    const expandedPaths = new Set<string>();
    const checkedPaths = new Set<string>(["dir-a/file-1.txt", "dir-b/file-3.txt"]);
    const dirCheckState = new Map<string, boolean | "indeterminate">();

    const rows = flattenVisible(roots, expandedPaths, checkedPaths, dirCheckState);

    // Should only see the two root directories, not their children
    expect(rows.length).toBe(2);
    expect(rows[0].node.name).toBe("dir-a");
    expect(rows[1].node.name).toBe("dir-b");
    expect(rows.every((r) => r.isDirectory)).toBe(true);
  });

  it("includes children of expanded directories", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "dir-a/file-1.txt" }),
      fileEntry({ path: "dir-a/file-2.txt", hasLocalCopy: false }),
      fileEntry({ path: "dir-b/file-3.txt" }),
    ];

    const roots = buildTree(entries);
    const expandedPaths = new Set<string>(["dir-a"]);
    const checkedPaths = new Set<string>(["dir-a/file-1.txt"]);
    const dirCheckState = new Map<string, boolean | "indeterminate">();

    const rows = flattenVisible(roots, expandedPaths, checkedPaths, dirCheckState);

    // dir-a (expanded) + file-1 + file-2 + dir-b (collapsed)
    expect(rows.length).toBe(4);
    expect(rows[0].node.name).toBe("dir-a");
    expect(rows[0].expanded).toBe(true);
    expect(rows[1].node.name).toBe("file-1.txt");
    expect(rows[2].node.name).toBe("file-2.txt");
    expect(rows[3].node.name).toBe("dir-b");
    expect(rows[3].expanded).toBe(false);
  });

  it("includes explicit empty directories when parents are expanded", () => {
    const entries: FileEntry[] = [
      directoryEntry({ path: "dir-a/empty" }),
      fileEntry({ path: "dir-b/file-3.txt" }),
    ];

    const roots = buildTree(entries);
    const rows = flattenVisible(roots, new Set<string>(["dir-a"]), new Set<string>(), new Map());

    expect(rows.find((row) => row.node.path === "dir-a/empty")?.isDirectory).toBe(true);
  });
});

describe("computeVisibleRange", () => {
  it("returns correct range for given scroll position", () => {
    // scrollTop=280 means we've scrolled past 10 rows (28px each)
    // containerHeight=280 means we can see 10 rows
    // So visible rows are 10..19, with buffer (10) that's 0..29
    const result = computeVisibleRange(280, 280, 100, 28, 10);

    expect(result.start).toBe(0); // max(0, 10 - 10)
    expect(result.end).toBe(30); // min(100, 10 + 10 + 10)
  });

  it("clamps to bounds", () => {
    // Scroll near the end
    const result = computeVisibleRange(2800, 280, 100, 28, 10);

    // firstVisible = 100, visibleCount = 10, start = max(0, 90) = 90
    // end = min(100, 100 + 10 + 10) = 100
    expect(result.start).toBe(90);
    expect(result.end).toBe(100);

    // Zero total rows
    const empty = computeVisibleRange(0, 280, 0, 28, 10);
    expect(empty.start).toBe(0);
    expect(empty.end).toBe(0);
  });
});

describe("renderFileTreeVirtual", () => {
  let treeEl: HTMLUListElement;
  let emptyStateEl: HTMLElement;
  let containerEl: HTMLDivElement;

  beforeEach(() => {
    containerEl = document.createElement("div");
    containerEl.className = "file-tree-section";
    document.body.appendChild(containerEl);

    treeEl = document.createElement("ul");
    treeEl.className = "tree tree--dot-left";
    treeEl.hidden = true;
    containerEl.appendChild(treeEl);

    emptyStateEl = document.createElement("div");
    emptyStateEl.className = "empty-state";
    emptyStateEl.hidden = false;
    containerEl.appendChild(emptyStateEl);

    applyIconsMock.mockClear();
    bindCheckboxTreeMock.mockClear();
    createIconMock.mockClear();
    createIconMock.mockImplementation(() => {
      return document.createElementNS("http://www.w3.org/2000/svg", "svg");
    });
    bindCheckboxTreeMock.mockReturnValue({
      expand: vi.fn(),
      collapse: vi.fn(),
      expandAll: vi.fn(),
      collapseAll: vi.fn(),
      destroy: vi.fn(),
    });
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("creates .vtree container for large entry sets", () => {
    const entries = generateEntries(2500);

    renderFileTree({ treeEl, emptyStateEl, entries });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();
    expect(bindCheckboxTreeMock).not.toHaveBeenCalled();
    expect(treeEl.hidden).toBe(true);
  });

  it("uses classic path for small entry sets", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "readme.txt" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).toBeNull();
    expect(bindCheckboxTreeMock).toHaveBeenCalled();
  });

  it("destroy removes .vtree from DOM", () => {
    const entries = generateEntries(2500);

    const handle = renderFileTree({ treeEl, emptyStateEl, entries });

    expect(containerEl.querySelector(".vtree")).not.toBeNull();

    handle.destroy();

    expect(containerEl.querySelector(".vtree")).toBeNull();
  });

  it("checkbox change fires onChange with correct paths", () => {
    const onChange = vi.fn();
    const entries = generateEntries(2500);

    renderFileTree({ treeEl, emptyStateEl, entries, onChange });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();

    // Find a rendered checkbox and toggle it
    const checkbox = vtree!.querySelector<HTMLInputElement>(".tree-check");
    if (checkbox) {
      const wasChecked = checkbox.checked;
      checkbox.checked = !wasChecked;
      checkbox.dispatchEvent(new Event("change", { bubbles: true }));

      expect(onChange).toHaveBeenCalled();
      const paths = onChange.mock.calls[0][0] as string[];
      expect(Array.isArray(paths)).toBe(true);
    }
  });

  it("adds tooltip attributes to virtual file status indicators", () => {
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `file-${i}.txt`,
        status: i === 0
          ? "synced"
          : i === 1
            ? "local-only"
            : i === 2
              ? "remote-only"
              : i === 3
                ? "review-required"
                : i === 4
                  ? "conflict"
                  : i === 5
                    ? "glacier"
                    : "deleted",
        hasLocalCopy: i === 2 || i >= 3 ? false : true,
        binKey: i >= 6 ? `bin-${i}` : undefined,
      }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin" });

    const indicator = (path: string) =>
      containerEl
        .querySelector(`.vtree-row[data-value="${path}"] .status-indicator`) as HTMLElement;

    expect(indicator("file-0.txt")?.getAttribute("title")).toBe(getStatusTooltip("synced"));
    expect(indicator("file-1.txt")?.getAttribute("title")).toBe(getStatusTooltip("local-only"));
    expect(indicator("file-2.txt")?.getAttribute("title")).toBe(getStatusTooltip("remote-only"));
    expect(indicator("file-3.txt")?.getAttribute("title")).toBe(getStatusTooltip("review-required"));
    expect(indicator("file-4.txt")?.getAttribute("title")).toBe(getStatusTooltip("conflict"));
    expect(indicator("file-5.txt")?.getAttribute("title")).toBe(getStatusTooltip("glacier"));
    expect(indicator("file-6.txt")?.getAttribute("title")).toBe(getStatusTooltip("deleted"));
    expect(indicator("file-0.txt")?.getAttribute("aria-label")).toBe(getStatusTooltip("synced"));
    expect(indicator("file-0.txt")?.querySelector(".status-dot")?.getAttribute("aria-hidden")).toBe("true");
  });

  it("adds tooltip attributes to virtual directory status indicators", () => {
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `dir-${Math.floor(i / 100)}/file-${i}.txt`,
        status: i === 0 ? "remote-only" : "synced",
        hasLocalCopy: i === 0 ? false : true,
      }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries });

    const indicator = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="dir-0"] .status-indicator');
    expect(indicator).not.toBeNull();
    expect(indicator?.getAttribute("title")).toBe("Contains unsynced items");
    expect(indicator?.getAttribute("aria-label")).toBe("Contains unsynced items");
  });

  it("disables conflict file checkboxes and renders resolve buttons in virtual live tree", () => {
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `file-${i}.txt`,
        status: i === 0
          ? "conflict"
          : i === 1
            ? "review-required"
            : i === 2
              ? "conflict"
              : "synced",
        localKind: i <= 2 ? "file" : undefined,
        remoteKind: i === 2 ? "directory" : i <= 1 ? "file" : undefined,
        hasLocalCopy: i === 1 ? false : true,
      }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries });

    const conflictRow = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="file-0.txt"]');
    const conflictCheckbox = conflictRow?.querySelector<HTMLInputElement>(".tree-check");
    const resolveBtn = conflictRow?.querySelector<HTMLButtonElement>(".tree-resolve-btn");
    const reviewResolveBtn = containerEl.querySelector('.vtree-row[data-value="file-1.txt"] .tree-resolve-btn');
    const unsupportedResolveBtn = containerEl.querySelector('.vtree-row[data-value="file-2.txt"] .tree-resolve-btn');
    const normalResolveBtn = containerEl.querySelector('.vtree-row[data-value="file-3.txt"] .tree-resolve-btn');

    expect(conflictCheckbox?.disabled).toBe(true);
    expect(resolveBtn?.classList.contains("icon-btn")).toBe(true);
    expect(resolveBtn?.classList.contains("icon-btn-sm")).toBe(true);
    expect(resolveBtn?.classList.contains("secondary-btn")).toBe(false);
    expect(resolveBtn?.getAttribute("title")).toBe("Resolve file conflict");
    expect(resolveBtn?.getAttribute("aria-label")).toBe("Resolve file conflict");
    expect(resolveBtn?.querySelector(".tree-action-label")?.textContent).toBe("Resolve");
    expect(resolveBtn?.querySelector("svg")).not.toBeNull();
    expect(resolveBtn?.childElementCount).toBe(2);
    expect(reviewResolveBtn).not.toBeNull();
    expect(unsupportedResolveBtn).toBeNull();
    expect(normalResolveBtn).toBeNull();
  });

  it("renders review-required entries safely in virtual live tree and disables their checkboxes", () => {
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `file-${i}.txt`,
        status: i === 0 ? "review-required" : "synced",
        hasLocalCopy: i === 0 ? false : true,
      }),
    );

    expect(() => renderFileTree({ treeEl, emptyStateEl, entries })).not.toThrow();

    const reviewRow = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="file-0.txt"]');
    const checkbox = reviewRow?.querySelector<HTMLInputElement>(".tree-check");
    const indicator = reviewRow?.querySelector<HTMLElement>(".status-indicator");

    expect(checkbox?.disabled).toBe(true);
    expect(checkbox?.checked).toBe(false);
    expect(indicator?.getAttribute("title")).toBe(getStatusTooltip("review-required"));
    expect(reviewRow?.querySelector(".tree-resolve-btn")).toBeNull();
    expect(reviewRow?.querySelector(".tree-storage-class-btn")).toBeNull();
    expect(reviewRow?.querySelector(".tree-delete-btn")).toBeNull();
  });

  it("preserves live mutating actions for non-review rows in virtual tree", () => {
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `file-${i}.txt`,
        status: "remote-only",
        hasLocalCopy: false,
      }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries });

    const row = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="file-0.txt"]');
    expect(row?.querySelector('.tree-storage-class-btn')).not.toBeNull();
    expect(row?.querySelector('.tree-delete-btn')).not.toBeNull();
  });

  it("disables review-required directory checkboxes in virtual live tree", () => {
    const entries = [
      directoryEntry({ path: "review-dir", status: "review-required", hasLocalCopy: false }),
      ...Array.from({ length: 2499 }, (_, i) => fileEntry({ path: `file-${i}.txt` })),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const reviewRow = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="review-dir"]');
    const checkbox = reviewRow?.querySelector<HTMLInputElement>(".tree-check");

    expect(checkbox?.disabled).toBe(true);
    expect(checkbox?.checked).toBe(false);
  });

  it("disables conflict directory checkboxes in virtual live tree", () => {
    const entries = [
      directoryEntry({ path: "conflict-dir", status: "conflict", hasLocalCopy: true }),
      ...Array.from({ length: 2499 }, (_, i) => fileEntry({ path: `file-${i}.txt` })),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const conflictRow = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="conflict-dir"]');
    const conflictCheckbox = conflictRow?.querySelector<HTMLInputElement>(".tree-check");

    expect(conflictCheckbox?.disabled).toBe(true);
  });

  it("clicking resolve button calls onResolveConflict in virtual tree", () => {
    const onResolveConflict = vi.fn();
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `file-${i}.txt`,
        status: i === 0 ? "conflict" : "synced",
        localKind: i === 0 ? "file" : undefined,
        remoteKind: i === 0 ? "file" : undefined,
        hasLocalCopy: true,
      }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, onResolveConflict });

    containerEl.querySelector<HTMLButtonElement>('.vtree-row[data-value="file-0.txt"] .tree-resolve-btn')?.click();

    expect(onResolveConflict).toHaveBeenCalledOnce();
    expect(onResolveConflict.mock.calls[0]?.[0]).toMatchObject({ path: "file-0.txt", status: "conflict" });
  });

  it("clicking resolve button calls onResolveConflict for review-required entries in virtual tree", () => {
    const onResolveConflict = vi.fn();
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({
        path: `file-${i}.txt`,
        status: i === 0 ? "review-required" : "synced",
        localKind: i === 0 ? "file" : undefined,
        remoteKind: i === 0 ? "file" : undefined,
        hasLocalCopy: i === 0 ? false : true,
      }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, onResolveConflict });

    containerEl.querySelector<HTMLButtonElement>('.vtree-row[data-value="file-0.txt"] .tree-resolve-btn')?.click();

    expect(onResolveConflict).toHaveBeenCalledOnce();
    expect(onResolveConflict.mock.calls[0]?.[0]).toMatchObject({ path: "file-0.txt", status: "review-required" });
  });
});

describe("VIRTUAL_THRESHOLD", () => {
  it("is 2000", () => {
    expect(VIRTUAL_THRESHOLD).toBe(2_000);
  });
});

describe("delete button", () => {
  let treeEl: HTMLUListElement;
  let emptyStateEl: HTMLElement;
  let containerEl: HTMLDivElement;

  beforeEach(() => {
    containerEl = document.createElement("div");
    containerEl.className = "file-tree-section";
    document.body.appendChild(containerEl);

    treeEl = document.createElement("ul");
    treeEl.className = "tree tree--dot-left";
    treeEl.hidden = true;
    containerEl.appendChild(treeEl);

    emptyStateEl = document.createElement("div");
    emptyStateEl.className = "empty-state";
    emptyStateEl.hidden = false;
    containerEl.appendChild(emptyStateEl);

    applyIconsMock.mockClear();
    bindCheckboxTreeMock.mockClear();
    createIconMock.mockClear();
    createIconMock.mockImplementation(() => {
      return document.createElementNS("http://www.w3.org/2000/svg", "svg");
    });
    bindCheckboxTreeMock.mockReturnValue({
      expand: vi.fn(),
      collapse: vi.fn(),
      expandAll: vi.fn(),
      collapseAll: vi.fn(),
      destroy: vi.fn(),
    });
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("renders for file rows in virtual tree", () => {
    // Use root-level files so they appear immediately without expanding directories
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();

    const deleteBtns = vtree!.querySelectorAll<HTMLButtonElement>(".tree-delete-btn");
    expect(deleteBtns.length).toBeGreaterThan(0);

    // Every delete button should carry the correct data-delete-path
    for (const btn of deleteBtns) {
      const path = btn.getAttribute("data-delete-path");
      expect(path).toBeTruthy();
      expect(path).toMatch(/^file-\d+\.txt$/);
    }
  });

  it("renders delete buttons for safe directory rows", () => {
    // generateEntries creates paths like dir-X/file-Y.txt; initially all
    // directories are collapsed so only directory rows are visible.
    const entries = generateEntries(2500);

    renderFileTree({ treeEl, emptyStateEl, entries });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();

    // All rendered rows should be directories (collapsed root dirs)
    const rows = vtree!.querySelectorAll(".vtree-row");
    expect(rows.length).toBeGreaterThan(0);

    const deleteBtns = vtree!.querySelectorAll(".tree-delete-btn");
    expect(deleteBtns.length).toBeGreaterThan(0);
    for (const btn of deleteBtns) {
      expect(btn.getAttribute("data-delete-kind")).toBe("directory");
    }
  });

  it("does NOT render directory delete buttons for blocked directory rows", () => {
    const entries = [
      directoryEntry({ path: "review-dir", status: "review-required", hasLocalCopy: false }),
      ...Array.from({ length: 2499 }, (_, i) => fileEntry({ path: `file-${i}.txt` })),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const reviewRow = containerEl.querySelector<HTMLElement>('.vtree-row[data-value="review-dir"]');
    expect(reviewRow?.querySelector(".tree-delete-btn")).toBeNull();
  });

  it("clicking delete button calls onDelete with correct path", () => {
    const onDelete = vi.fn();
    // Root-level files so file rows (with delete buttons) render immediately
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, onDelete });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();

    const deleteBtn = vtree!.querySelector<HTMLButtonElement>(".tree-delete-btn");
    expect(deleteBtn).not.toBeNull();

    const expectedPath = deleteBtn!.getAttribute("data-delete-path")!;
    deleteBtn!.click();

    expect(onDelete).toHaveBeenCalledTimes(1);
    expect(onDelete).toHaveBeenCalledWith({ path: expectedPath, kind: "file" });
  });

  it("clicking directory delete button calls onDelete with correct target", () => {
    const onDelete = vi.fn();
    const entries = generateEntries(2500);

    renderFileTree({ treeEl, emptyStateEl, entries, onDelete });

    const deleteBtn = containerEl.querySelector<HTMLButtonElement>('.vtree-row[data-value="dir-0"] .tree-delete-btn');
    expect(deleteBtn).not.toBeNull();

    deleteBtn!.click();

    expect(onDelete).toHaveBeenCalledTimes(1);
    expect(onDelete).toHaveBeenCalledWith({ path: "dir-0", kind: "directory" });
  });
});

describe("restore button", () => {
  let treeEl: HTMLUListElement;
  let emptyStateEl: HTMLElement;
  let containerEl: HTMLDivElement;

  beforeEach(() => {
    containerEl = document.createElement("div");
    containerEl.className = "file-tree-section";
    document.body.appendChild(containerEl);

    treeEl = document.createElement("ul");
    treeEl.className = "tree tree--dot-left";
    treeEl.hidden = true;
    containerEl.appendChild(treeEl);

    emptyStateEl = document.createElement("div");
    emptyStateEl.className = "empty-state";
    emptyStateEl.hidden = false;
    containerEl.appendChild(emptyStateEl);

    createIconMock.mockClear();
    createIconMock.mockImplementation(() => document.createElementNS("http://www.w3.org/2000/svg", "svg"));
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("renders restore buttons for file rows in virtual bin tree", () => {
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt`, status: "deleted", hasLocalCopy: false, binKey: `bin-${i}` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin" });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();
    expect(vtree!.querySelectorAll(".tree-restore-btn").length).toBeGreaterThan(0);
    expect(vtree!.querySelector(".tree-delete-btn")).toBeNull();
  });

  it("clicking restore button calls onRestore with opaque bin key", () => {
    const onRestore = vi.fn();
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt`, status: "deleted", hasLocalCopy: false, binKey: `opaque-${i}` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onRestore });

    const restoreBtn = containerEl.querySelector<HTMLButtonElement>(".tree-restore-btn");
    expect(restoreBtn).not.toBeNull();

    const expectedPath = restoreBtn!.getAttribute("data-restore-path")!;
    restoreBtn!.click();

    expect(onRestore).toHaveBeenCalledWith(expect.objectContaining({ path: expectedPath }));
  });

  it("shows restore button loading state while virtual restore is pending", async () => {
    const restore = createDeferred<void>();
    const onRestore = vi.fn(() => restore.promise);
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt`, status: "deleted", hasLocalCopy: false, binKey: `opaque-${i}` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onRestore });

    const restoreBtn = containerEl.querySelector<HTMLButtonElement>(".tree-restore-btn");
    expect(restoreBtn).not.toBeNull();

    restoreBtn!.click();

    expect(onRestore).toHaveBeenCalledOnce();
    expect(restoreBtn?.classList.contains("is-loading")).toBe(true);
    expect(restoreBtn?.disabled).toBe(true);
    expect(restoreBtn?.getAttribute("aria-busy")).toBe("true");
    expect(restoreBtn?.querySelector<HTMLElement>(".tree-action-spinner")?.hidden).toBe(false);

    restoreBtn!.click();
    expect(onRestore).toHaveBeenCalledOnce();

    restore.resolve(undefined);
    await Promise.resolve();
    await Promise.resolve();

    expect(restoreBtn?.classList.contains("is-loading")).toBe(false);
    expect(restoreBtn?.disabled).toBe(false);
    expect(restoreBtn?.getAttribute("aria-busy")).toBe("false");
    expect(restoreBtn?.querySelector<HTMLElement>(".tree-action-spinner")?.hidden).toBe(true);
  });

  it("allows checkbox selection in virtual bin tree", () => {
    const onChange = vi.fn();
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt`, status: "deleted", hasLocalCopy: false, binKey: `opaque-${i}` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onChange, checkedPaths: [] });

    const checkbox = containerEl.querySelector<HTMLInputElement>(".tree-check");
    expect(checkbox?.disabled).toBe(false);

    checkbox!.checked = true;
    checkbox!.dispatchEvent(new Event("change", { bubbles: true }));

    expect(onChange).toHaveBeenCalled();
  });

  it("supports selecting empty directory rows in virtual bin mode", () => {
    const onChange = vi.fn();
    const entries: FileEntry[] = [
      directoryEntry({ path: "empty-dir", status: "deleted", hasLocalCopy: false }),
      ...Array.from({ length: 2499 }, (_, i) =>
        fileEntry({ path: `folder-${i}/file-${i}.txt`, status: "deleted", hasLocalCopy: false, binKey: `opaque-${i}` }),
      ),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onChange, checkedPaths: [] });

    const emptyDirCheckbox = containerEl.querySelector<HTMLInputElement>('.vtree-row[data-value="empty-dir"] .tree-check');
    expect(emptyDirCheckbox?.disabled).toBe(false);

    emptyDirCheckbox!.checked = true;
    emptyDirCheckbox!.dispatchEvent(new Event("change", { bubbles: true }));

    expect(onChange).toHaveBeenCalledWith(expect.arrayContaining(["empty-dir"]));
  });
});

describe("reveal button", () => {
  let treeEl: HTMLUListElement;
  let emptyStateEl: HTMLElement;
  let containerEl: HTMLDivElement;

  beforeEach(() => {
    containerEl = document.createElement("div");
    containerEl.className = "file-tree-section";
    document.body.appendChild(containerEl);

    treeEl = document.createElement("ul");
    treeEl.className = "tree tree--dot-left";
    treeEl.hidden = true;
    containerEl.appendChild(treeEl);

    emptyStateEl = document.createElement("div");
    emptyStateEl.className = "empty-state";
    emptyStateEl.hidden = false;
    containerEl.appendChild(emptyStateEl);

    createIconMock.mockClear();
    createIconMock.mockImplementation(() => document.createElementNS("http://www.w3.org/2000/svg", "svg"));
  });

  afterEach(() => {
    document.body.innerHTML = "";
  });

  it("renders reveal buttons for file and directory rows in virtual tree", () => {
    const entries = generateEntries(2500);

    renderFileTree({ treeEl, emptyStateEl, entries });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();
    const revealBtns = vtree!.querySelectorAll<HTMLButtonElement>(".tree-reveal-btn");
    expect(revealBtns.length).toBeGreaterThan(0);
    for (const btn of revealBtns) {
      expect(btn.dataset.revealPath).toBeTruthy();
    }
  });

  it("clicking reveal button calls onReveal with correct path", () => {
    const onReveal = vi.fn();
    const entries = Array.from({ length: 2500 }, (_, i) =>
      fileEntry({ path: `file-${i}.txt` }),
    );

    renderFileTree({ treeEl, emptyStateEl, entries, onReveal });

    const revealBtn = containerEl.querySelector<HTMLButtonElement>(".tree-reveal-btn");
    expect(revealBtn).not.toBeNull();

    const expectedPath = revealBtn!.dataset.revealPath;
    revealBtn!.click();

    expect(onReveal).toHaveBeenCalledWith(expectedPath);
  });
});

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}
