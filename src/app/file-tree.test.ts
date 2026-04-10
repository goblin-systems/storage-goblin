import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { applyIconsMock, bindCheckboxTreeMock } = vi.hoisted(() => ({
  applyIconsMock: vi.fn(),
  bindCheckboxTreeMock: vi.fn(() => ({
    expand: vi.fn(),
    collapse: vi.fn(),
    expandAll: vi.fn(),
    collapseAll: vi.fn(),
    destroy: vi.fn(),
  })),
}));

vi.mock("@goblin-systems/goblin-design-system", () => ({
  applyIcons: applyIconsMock,
  bindCheckboxTree: bindCheckboxTreeMock,
}));

import {
  renderFileTree,
  deriveDirectoryStatus,
  buildTree,
  type FileEntry,
} from "./file-tree";

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

describe("renderFileTree", () => {
  let treeEl: HTMLUListElement;
  let emptyStateEl: HTMLElement;

  beforeEach(() => {
    treeEl = document.createElement("ul");
    treeEl.className = "tree tree--dot-left";
    treeEl.hidden = true;
    document.body.append(treeEl);

    emptyStateEl = document.createElement("div");
    emptyStateEl.className = "empty-state";
    emptyStateEl.hidden = false;
    document.body.append(emptyStateEl);

    applyIconsMock.mockClear();
    bindCheckboxTreeMock.mockClear();
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

  it("shows empty state when entries are empty", () => {
    renderFileTree({ treeEl, emptyStateEl, entries: [] });

    expect(emptyStateEl.hidden).toBe(false);
    expect(treeEl.hidden).toBe(true);
    expect(treeEl.innerHTML).toBe("");
  });

  it("hides empty state and shows tree when entries exist", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "readme.txt" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    expect(emptyStateEl.hidden).toBe(true);
    expect(treeEl.hidden).toBe(false);
    expect(treeEl.innerHTML).not.toBe("");
  });

  it("renders flat files as leaf tree items", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "readme.txt" }),
      fileEntry({ path: "config.json", status: "local-only", hasLocalCopy: false }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const items = treeEl.querySelectorAll(".tree-item");
    expect(items.length).toBe(2);

    for (const item of items) {
      expect(item.querySelector(".tree-leaf")).not.toBeNull();
      expect(item.querySelector(".tree-toggle")).toBeNull();
    }

    const values = Array.from(items).map((item) => item.getAttribute("data-value"));
    expect(values).toContain("readme.txt");
    expect(values).toContain("config.json");
  });

  it("renders nested directory structure from path segments", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "photos/vacation/img.jpg" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const items = treeEl.querySelectorAll(".tree-item");
    expect(items.length).toBe(3);

    // Top-level "photos" directory should have a toggle button
    const topItem = treeEl.querySelector('.tree-item[data-value="photos"]');
    expect(topItem).not.toBeNull();
    expect(topItem!.querySelector(".tree-toggle")).not.toBeNull();

    // Deepest item "img.jpg" should be a leaf
    const leafItem = treeEl.querySelector('.tree-item[data-value="photos/vacation/img.jpg"]');
    expect(leafItem).not.toBeNull();
    expect(leafItem!.querySelector(".tree-leaf")).not.toBeNull();

    // Check --tree-depth CSS variable on each tree-row
    const photosRow = topItem!.querySelector<HTMLElement>(".tree-row");
    expect(photosRow!.style.getPropertyValue("--tree-depth")).toBe("0");

    const vacationItem = treeEl.querySelector('.tree-item[data-value="photos/vacation"]');
    const vacationRow = vacationItem!.querySelector<HTMLElement>(".tree-row");
    expect(vacationRow!.style.getPropertyValue("--tree-depth")).toBe("1");

    const fileRow = leafItem!.querySelector<HTMLElement>(".tree-row");
    expect(fileRow!.style.getPropertyValue("--tree-depth")).toBe("2");
  });

  it("renders explicit empty directories", () => {
    const entries: FileEntry[] = [
      directoryEntry({ path: "photos/empty" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const emptyDir = treeEl.querySelector('.tree-item[data-value="photos/empty"]');
    expect(emptyDir).not.toBeNull();
    expect(emptyDir?.querySelector(".tree-toggle")).not.toBeNull();
    expect(emptyDir?.querySelector(".tree-branch")?.children.length).toBe(0);
  });

  it("sets checkbox checked state from hasLocalCopy", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "local.txt" }),
      fileEntry({ path: "remote.txt", status: "remote-only", hasLocalCopy: false }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const checkboxes = treeEl.querySelectorAll<HTMLInputElement>(".tree-check");
    expect(checkboxes.length).toBe(2);

    const localCheckbox = treeEl
      .querySelector('.tree-item[data-value="local.txt"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;
    const remoteCheckbox = treeEl
      .querySelector('.tree-item[data-value="remote.txt"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;

    expect(localCheckbox.checked).toBe(true);
    expect(remoteCheckbox.checked).toBe(false);
  });

  it("applies correct status-indicator classes", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "synced.txt" }),
      fileEntry({ path: "local.txt", status: "local-only" }),
      fileEntry({ path: "remote.txt", status: "remote-only", hasLocalCopy: false }),
      fileEntry({ path: "conflict.txt", status: "conflict" }),
      fileEntry({ path: "glacier.txt", status: "glacier", hasLocalCopy: false }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const indicator = (path: string) =>
      treeEl
        .querySelector(`.tree-item[data-value="${path}"]`)!
        .querySelector(".status-indicator")!;

    expect(indicator("synced.txt").classList.contains("connected")).toBe(true);
    expect(indicator("local.txt").classList.contains("untested")).toBe(true);
    expect(indicator("remote.txt").classList.contains("untested")).toBe(true);
    expect(indicator("conflict.txt").classList.contains("error")).toBe(true);
    expect(indicator("glacier.txt").classList.contains("glacier")).toBe(true);
  });

  it("derives directory status from leaf descendants", () => {
    // All synced → directory should be "connected"
    const syncedEntries: FileEntry[] = [
      fileEntry({ path: "docs/a.txt" }),
      fileEntry({ path: "docs/b.txt" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries: syncedEntries });

    const docsIndicator = treeEl
      .querySelector('.tree-item[data-value="docs"]')!
      .querySelector(".status-indicator")!;
    expect(docsIndicator.classList.contains("connected")).toBe(true);

    // Reset for second scenario
    treeEl.innerHTML = "";

    // Mix of synced and conflict → directory should be "error"
    const mixedEntries: FileEntry[] = [
      fileEntry({ path: "mixed/ok.txt" }),
      fileEntry({ path: "mixed/bad.txt", status: "conflict" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries: mixedEntries });

    const mixedIndicator = treeEl
      .querySelector('.tree-item[data-value="mixed"]')!
      .querySelector(".status-indicator")!;
    expect(mixedIndicator.classList.contains("error")).toBe(true);
  });

  it("prioritizes directory statuses as conflict, synced, glacier, then yellow fallback", () => {
    const conflictTree = buildTree([
      fileEntry({ path: "conflict/ok.txt" }),
      fileEntry({ path: "conflict/bad.txt", status: "conflict" }),
      fileEntry({ path: "conflict/cold.txt", status: "glacier", hasLocalCopy: false }),
    ]);
    expect(deriveDirectoryStatus(conflictTree[0]!)).toBe("error");

    const syncedTree = buildTree([
      fileEntry({ path: "synced/a.txt" }),
      fileEntry({ path: "synced/b.txt" }),
    ]);
    expect(deriveDirectoryStatus(syncedTree[0]!)).toBe("connected");

    const glacierTree = buildTree([
      fileEntry({ path: "glacier/a.txt" }),
      fileEntry({ path: "glacier/b.txt", status: "glacier", hasLocalCopy: false }),
    ]);
    expect(deriveDirectoryStatus(glacierTree[0]!)).toBe("glacier");

    const unsyncedTree = buildTree([
      fileEntry({ path: "unsynced/local.txt", status: "local-only" }),
      fileEntry({ path: "unsynced/remote.txt", status: "remote-only", hasLocalCopy: false }),
    ]);
    expect(deriveDirectoryStatus(unsyncedTree[0]!)).toBe("untested");
  });

  it("disables glacier file checkboxes", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "cold.txt", status: "glacier", hasLocalCopy: false }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const checkbox = treeEl
      .querySelector('.tree-item[data-value="cold.txt"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;

    expect(checkbox.disabled).toBe(true);
  });

  it("calls applyIcons after rendering", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "file.txt" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    expect(applyIconsMock).toHaveBeenCalled();
  });

  it("calls bindCheckboxTree with the tree element", () => {
    const onChange = vi.fn();
    const entries: FileEntry[] = [
      fileEntry({ path: "file.txt" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, onChange });

    expect(bindCheckboxTreeMock).toHaveBeenCalledWith({
      el: treeEl,
      onChange,
    });
  });

  it("destroy() calls the tree handle destroy", () => {
    const mockDestroy = vi.fn();
    bindCheckboxTreeMock.mockReturnValueOnce({
      expand: vi.fn(),
      collapse: vi.fn(),
      expandAll: vi.fn(),
      collapseAll: vi.fn(),
      destroy: mockDestroy,
    });

    const entries: FileEntry[] = [
      fileEntry({ path: "file.txt" }),
    ];

    const handle = renderFileTree({ treeEl, emptyStateEl, entries });
    handle.destroy();

    expect(mockDestroy).toHaveBeenCalled();
  });

  describe("delete button", () => {
    it("renders for file rows with correct data-delete-path", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "readme.txt" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>(".tree-delete-btn");
      expect(deleteBtn).not.toBeNull();
      expect(deleteBtn!.getAttribute("data-delete-path")).toBe("readme.txt");
    });

    it("does NOT render for directory rows", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "docs/file.txt" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const dirItem = treeEl.querySelector('.tree-item[data-value="docs"]');
      expect(dirItem).not.toBeNull();

      const dirRow = dirItem!.querySelector(".tree-row");
      expect(dirRow).not.toBeNull();
      expect(dirRow!.querySelector(".tree-delete-btn")).toBeNull();
    });

    it("clicking delete button calls onDelete with correct path", () => {
      const onDelete = vi.fn();
      const entries: FileEntry[] = [
        fileEntry({ path: "photos/img.jpg" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries, onDelete });

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>(".tree-delete-btn");
      expect(deleteBtn).not.toBeNull();
      deleteBtn!.click();

      expect(onDelete).toHaveBeenCalledOnce();
      expect(onDelete).toHaveBeenCalledWith("photos/img.jpg");
    });

    it("does not throw when clicking delete button without onDelete handler", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "file.txt" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>(".tree-delete-btn");
      expect(deleteBtn).not.toBeNull();
      expect(() => deleteBtn!.click()).not.toThrow();
    });
  });
});
