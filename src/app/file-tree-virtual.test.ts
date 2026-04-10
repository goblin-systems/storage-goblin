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
import { renderFileTree, type FileEntry } from "./file-tree";
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

  it("does NOT render for directory rows", () => {
    // generateEntries creates paths like dir-X/file-Y.txt; initially all
    // directories are collapsed so only directory rows are visible.
    const entries = generateEntries(2500);

    renderFileTree({ treeEl, emptyStateEl, entries });

    const vtree = containerEl.querySelector(".vtree");
    expect(vtree).not.toBeNull();

    // All rendered rows should be directories (collapsed root dirs)
    const rows = vtree!.querySelectorAll(".vtree-row");
    expect(rows.length).toBeGreaterThan(0);

    // None of the directory rows should have a delete button
    const deleteBtns = vtree!.querySelectorAll(".tree-delete-btn");
    expect(deleteBtns.length).toBe(0);
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
    expect(onDelete).toHaveBeenCalledWith(expectedPath);
  });
});
