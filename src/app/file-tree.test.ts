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
  deriveDirectoryStatusTooltip,
  buildTree,
  getBinLifecycleParts,
  getStatusTooltip,
  isEntryCheckboxDisabled,
  isResolvableConflictFileEntry,
  canMutateLiveFileEntry,
  canMutateLiveDirectoryNode,
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
      fileEntry({ path: "review.txt", status: "review-required", hasLocalCopy: false }),
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
    expect(indicator("review.txt").classList.contains("untested")).toBe(true);
    expect(indicator("conflict.txt").classList.contains("error")).toBe(true);
    expect(indicator("glacier.txt").classList.contains("glacier")).toBe(true);
  });

  it("adds tooltip attributes to file status indicators", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "synced.txt", status: "synced" }),
      fileEntry({ path: "local.txt", status: "local-only" }),
      fileEntry({ path: "remote.txt", status: "remote-only", hasLocalCopy: false }),
      fileEntry({ path: "review.txt", status: "review-required", hasLocalCopy: false }),
      fileEntry({ path: "conflict.txt", status: "conflict" }),
      fileEntry({ path: "glacier.txt", status: "glacier", hasLocalCopy: false }),
      fileEntry({ path: "deleted.txt", status: "deleted", hasLocalCopy: false, binKey: "bin-1" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin" });

    const indicator = (path: string) =>
      treeEl
        .querySelector(`.tree-item[data-value="${path}"]`)!
        .querySelector<HTMLElement>(".status-indicator")!;

    expect(indicator("synced.txt").getAttribute("title")).toBe(getStatusTooltip("synced"));
    expect(indicator("synced.txt").getAttribute("aria-label")).toBe(getStatusTooltip("synced"));
    expect(indicator("local.txt").getAttribute("title")).toBe(getStatusTooltip("local-only"));
    expect(indicator("remote.txt").getAttribute("title")).toBe(getStatusTooltip("remote-only"));
    expect(indicator("review.txt").getAttribute("title")).toBe(getStatusTooltip("review-required"));
    expect(indicator("conflict.txt").getAttribute("title")).toBe(getStatusTooltip("conflict"));
    expect(indicator("glacier.txt").getAttribute("title")).toBe(getStatusTooltip("glacier"));
    expect(indicator("deleted.txt").getAttribute("title")).toBe(getStatusTooltip("deleted"));
    expect(indicator("deleted.txt").querySelector(".status-dot")?.getAttribute("aria-hidden")).toBe("true");
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

  it("adds tooltip attributes to directory status indicators", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "docs/a.txt" }),
      fileEntry({ path: "docs/b.txt" }),
      fileEntry({ path: "mixed/remote.txt", status: "remote-only", hasLocalCopy: false }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const docsIndicator = treeEl
      .querySelector('.tree-item[data-value="docs"]')!
      .querySelector<HTMLElement>(".status-indicator")!;
    const mixedIndicator = treeEl
      .querySelector('.tree-item[data-value="mixed"]')!
      .querySelector<HTMLElement>(".status-indicator")!;

    expect(docsIndicator.getAttribute("title")).toBe("All items synced");
    expect(docsIndicator.getAttribute("aria-label")).toBe("All items synced");
    expect(mixedIndicator.getAttribute("title")).toBe("Contains unsynced items");
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

  it("derives concise directory tooltip copy from descendant statuses", () => {
    const conflictTree = buildTree([
      fileEntry({ path: "conflict/ok.txt" }),
      fileEntry({ path: "conflict/bad.txt", status: "conflict" }),
    ]);
    expect(deriveDirectoryStatusTooltip(conflictTree[0]!)).toBe("Contains sync conflicts");

    const deletedTree = buildTree([
      fileEntry({ path: "deleted/a.txt", status: "deleted", hasLocalCopy: false, binKey: "bin-a" }),
      fileEntry({ path: "deleted/b.txt", status: "deleted", hasLocalCopy: false, binKey: "bin-b" }),
    ]);
    expect(deriveDirectoryStatusTooltip(deletedTree[0]!)).toBe("All items deleted");

    const glacierTree = buildTree([
      fileEntry({ path: "glacier/a.txt" }),
      fileEntry({ path: "glacier/b.txt", status: "glacier", hasLocalCopy: false }),
    ]);
    expect(deriveDirectoryStatusTooltip(glacierTree[0]!)).toBe("Contains archived items");

    const reviewTree = buildTree([
      fileEntry({ path: "review/a.txt", status: "review-required", hasLocalCopy: false }),
      fileEntry({ path: "review/b.txt" }),
    ]);
    expect(deriveDirectoryStatusTooltip(reviewTree[0]!)).toBe("Contains items requiring review");
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

  it("disables conflict file checkboxes", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "conflict.txt", status: "conflict" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const checkbox = treeEl
      .querySelector('.tree-item[data-value="conflict.txt"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;

    expect(checkbox.disabled).toBe(true);
    expect(isEntryCheckboxDisabled(entries[0]!, "live")).toBe(true);
  });

  it("renders review-required entries safely and disables their checkboxes", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "review.txt", status: "review-required", hasLocalCopy: false }),
      fileEntry({ path: "review-folder/file.txt", status: "review-required", hasLocalCopy: false }),
    ];

    expect(() => renderFileTree({ treeEl, emptyStateEl, entries })).not.toThrow();

    const fileCheckbox = treeEl
      .querySelector('.tree-item[data-value="review.txt"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;
    const dirIndicator = treeEl
      .querySelector('.tree-item[data-value="review-folder"] .status-indicator')!;

    expect(fileCheckbox.disabled).toBe(true);
    expect(fileCheckbox.checked).toBe(false);
    expect(fileCheckbox.closest('.tree-item')?.querySelector('.tree-resolve-btn')).toBeNull();
    expect(fileCheckbox.closest('.tree-item')?.querySelector('.tree-storage-class-btn')).toBeNull();
    expect(fileCheckbox.closest('.tree-item')?.querySelector('.tree-delete-btn')).toBeNull();
    expect(dirIndicator.getAttribute("title")).toBe("Contains items requiring review");
    expect(isEntryCheckboxDisabled(entries[0]!, "live")).toBe(true);
    expect(canMutateLiveFileEntry(entries[0]!)).toBe(false);
  });

  it("preserves live mutating actions for non-review file rows", () => {
    const entry = fileEntry({ path: "normal.txt", status: "remote-only", hasLocalCopy: false });

    renderFileTree({ treeEl, emptyStateEl, entries: [entry] });

    const row = treeEl.querySelector('.tree-item[data-value="normal.txt"]');
    expect(row?.querySelector('.tree-storage-class-btn')).not.toBeNull();
    expect(row?.querySelector('.tree-delete-btn')).not.toBeNull();
    expect(canMutateLiveFileEntry(entry)).toBe(true);
  });

  it("disables review-required directory checkboxes", () => {
    const entries: FileEntry[] = [
      directoryEntry({ path: "review-dir", status: "review-required", hasLocalCopy: false }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const checkbox = treeEl
      .querySelector('.tree-item[data-value="review-dir"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;

    expect(checkbox.disabled).toBe(true);
    expect(checkbox.checked).toBe(false);
  });

  it("disables conflict directory checkboxes", () => {
    const entries: FileEntry[] = [
      directoryEntry({ path: "conflict-dir", status: "conflict", hasLocalCopy: true }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const checkbox = treeEl
      .querySelector('.tree-item[data-value="conflict-dir"]')!
      .querySelector<HTMLInputElement>(".tree-check")!;

    expect(checkbox.disabled).toBe(true);
  });

  it("renders resolve button for conflict file rows only", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "conflict.txt", status: "conflict", localKind: "file", remoteKind: "file" }),
      fileEntry({ path: "review.txt", status: "review-required", hasLocalCopy: false, localKind: "file", remoteKind: "file" }),
      fileEntry({ path: "kind-mismatch.txt", status: "conflict", localKind: "file", remoteKind: "directory" }),
      fileEntry({ path: "ok.txt", status: "synced" }),
      fileEntry({ path: "folder/file.txt", status: "synced" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries });

    const conflictResolveButton = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="conflict.txt"] .tree-resolve-btn');
    const reviewResolveButton = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="review.txt"] .tree-resolve-btn');

    expect(conflictResolveButton).not.toBeNull();
    expect(conflictResolveButton?.classList.contains("icon-btn")).toBe(true);
    expect(conflictResolveButton?.classList.contains("icon-btn-sm")).toBe(true);
    expect(conflictResolveButton?.classList.contains("secondary-btn")).toBe(false);
    expect(conflictResolveButton?.getAttribute("title")).toBe("Resolve file conflict");
    expect(conflictResolveButton?.getAttribute("aria-label")).toBe("Resolve file conflict");
    expect(conflictResolveButton?.querySelector("[data-lucide=\"triangle-alert\"]")).not.toBeNull();
    expect(conflictResolveButton?.querySelector(".tree-action-label")?.textContent).toBe("Resolve");
    expect(conflictResolveButton?.childElementCount).toBe(2);
    expect(reviewResolveButton).not.toBeNull();
    expect(treeEl.querySelector('.tree-item[data-value="kind-mismatch.txt"] .tree-resolve-btn')).toBeNull();
    expect(treeEl.querySelector('.tree-item[data-value="ok.txt"] .tree-resolve-btn')).toBeNull();
    expect(treeEl.querySelector('.tree-item[data-value="folder"] .tree-resolve-btn')).toBeNull();
    expect(isResolvableConflictFileEntry(entries[0]!)).toBe(true);
    expect(isResolvableConflictFileEntry(entries[1]!)).toBe(true);
    expect(isResolvableConflictFileEntry(entries[2]!)).toBe(false);
  });

  it("clicking resolve button calls onResolveConflict with the entry", () => {
    const onResolveConflict = vi.fn();
    const entry = fileEntry({
      path: "conflict.txt",
      status: "conflict",
      localKind: "file",
      remoteKind: "file",
      localSize: 10,
      remoteSize: 12,
    });

    renderFileTree({ treeEl, emptyStateEl, entries: [entry], onResolveConflict });

    treeEl.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();

    expect(onResolveConflict).toHaveBeenCalledOnce();
    expect(onResolveConflict).toHaveBeenCalledWith(entry);
  });

  it("clicking resolve button calls onResolveConflict for review-required file entries", () => {
    const onResolveConflict = vi.fn();
    const entry = fileEntry({
      path: "review.txt",
      status: "review-required",
      hasLocalCopy: false,
      localKind: "file",
      remoteKind: "file",
      localSize: 10,
      remoteSize: 12,
    });

    renderFileTree({ treeEl, emptyStateEl, entries: [entry], onResolveConflict });

    treeEl.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();

    expect(onResolveConflict).toHaveBeenCalledOnce();
    expect(onResolveConflict).toHaveBeenCalledWith(entry);
  });

  it("renders deleted entries with error status styling", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "deleted.txt", status: "deleted", hasLocalCopy: false, binKey: "bin-1" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin" });

    const indicator = treeEl
      .querySelector('.tree-item[data-value="deleted.txt"]')!
      .querySelector(".status-indicator")!;
    expect(indicator.classList.contains("error")).toBe(true);
  });

  it("renders restore button instead of delete button in bin mode", () => {
    const entries: FileEntry[] = [
      fileEntry({ path: "deleted.txt", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin" });

    const restoreBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="deleted.txt"] .tree-restore-btn');
    expect(restoreBtn).not.toBeNull();
    expect(restoreBtn?.textContent).toBe("Restore");
    expect(restoreBtn?.dataset.restoreBinKey).toBe("opaque-bin-key");
    expect(treeEl.querySelector(".tree-delete-btn")).toBeNull();
    expect(treeEl.querySelector(".tree-storage-class-btn")).toBeNull();
  });

  it("clicking restore button calls onRestore with the full entry", () => {
    const onRestore = vi.fn();
    const entry = fileEntry({ path: "photos/img.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" });
    const entries: FileEntry[] = [entry];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onRestore });

    const restoreBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img.jpg"] .tree-restore-btn');
    expect(restoreBtn).not.toBeNull();
    restoreBtn!.click();

    expect(onRestore).toHaveBeenCalledOnce();
    expect(onRestore).toHaveBeenCalledWith(expect.objectContaining(entry));
  });

  it("renders lifecycle metadata for bin entries", () => {
    const entry = fileEntry({
      path: "photos/img.jpg",
      status: "deleted",
      hasLocalCopy: false,
      binKey: "opaque-bin-key",
      deletedFrom: "remote-bin",
      deletedAt: "2026-04-12T08:00:00.000Z",
      retentionDays: 7,
      expiresAt: "2026-04-19T08:00:00.000Z",
    });

    renderFileTree({ treeEl, emptyStateEl, entries: [entry], mode: "bin" });

    const lifecycle = treeEl.querySelector<HTMLElement>(".tree-bin-lifecycle");
    expect(lifecycle?.textContent).toContain("Remote bin");
    expect(lifecycle?.textContent).toContain("Deleted");
    expect(lifecycle?.textContent).toContain("Expires");
    expect(getBinLifecycleParts(entry)).toHaveLength(3);
  });

  it("renders restore control for directory rows in bin mode", () => {
    renderFileTree({
      treeEl,
      emptyStateEl,
      mode: "bin",
      entries: [
        directoryEntry({ path: "photos", status: "deleted", hasLocalCopy: false, deletedFrom: "object-versioning" }),
      ],
    });

    expect(treeEl.querySelector('.tree-item[data-value="photos"] .tree-restore-btn')).not.toBeNull();
  });

  it("shows restore button loading state while restore is pending", async () => {
    const restore = createDeferred<void>();
    const onRestore = vi.fn(() => restore.promise);
    const entries: FileEntry[] = [
      fileEntry({ path: "photos/img.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onRestore });

    const restoreBtn = treeEl.querySelector<HTMLButtonElement>(".tree-restore-btn");
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

  it("resets restore button loading state when restore fails", async () => {
    const restore = createDeferred<void>();
    const onRestore = vi.fn(() => restore.promise.catch(() => undefined));
    const entries: FileEntry[] = [
      fileEntry({ path: "photos/img.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
    ];

    renderFileTree({ treeEl, emptyStateEl, entries, mode: "bin", onRestore });

    const restoreBtn = treeEl.querySelector<HTMLButtonElement>(".tree-restore-btn");
    restoreBtn!.click();

    restore.reject(new Error("restore failed"));
    await Promise.resolve();
    await Promise.resolve();

    expect(restoreBtn?.classList.contains("is-loading")).toBe(false);
    expect(restoreBtn?.disabled).toBe(false);
    expect(restoreBtn?.getAttribute("aria-busy")).toBe("false");
    expect(restoreBtn?.querySelector<HTMLElement>(".tree-action-spinner")?.hidden).toBe(true);
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

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="readme.txt"] .tree-delete-btn');
      expect(deleteBtn).not.toBeNull();
      expect(deleteBtn!.getAttribute("data-delete-path")).toBe("readme.txt");
    });

    it("renders for safe directory rows with correct data-delete attributes", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "docs/file.txt" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const dirItem = treeEl.querySelector('.tree-item[data-value="docs"]');
      expect(dirItem).not.toBeNull();

      const dirRow = dirItem!.querySelector(".tree-row");
      expect(dirRow).not.toBeNull();
      const deleteBtn = dirRow!.querySelector<HTMLButtonElement>(".tree-delete-btn");
      expect(deleteBtn).not.toBeNull();
      expect(deleteBtn?.dataset.deletePath).toBe("docs");
      expect(deleteBtn?.dataset.deleteKind).toBe("directory");
      expect(canMutateLiveDirectoryNode(buildTree(entries)[0]!)).toBe(true);
    });

    it("does NOT render for blocked directory rows", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "review-folder/file.txt", status: "review-required", hasLocalCopy: false }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const dirRow = treeEl.querySelector('.tree-item[data-value="review-folder"] .tree-row');
      expect(dirRow?.querySelector(".tree-delete-btn")).toBeNull();
    });

    it("clicking delete button calls onDelete with correct path", () => {
      const onDelete = vi.fn();
      const entries: FileEntry[] = [
        fileEntry({ path: "photos/img.jpg" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries, onDelete });

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img.jpg"] .tree-delete-btn');
      expect(deleteBtn).not.toBeNull();
      deleteBtn!.click();

      expect(onDelete).toHaveBeenCalledOnce();
      expect(onDelete).toHaveBeenCalledWith({ path: "photos/img.jpg", kind: "file" });
    });

    it("clicking directory delete button calls onDelete with directory target", () => {
      const onDelete = vi.fn();
      const entries: FileEntry[] = [
        fileEntry({ path: "photos/img.jpg" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries, onDelete });

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="photos"] .tree-delete-btn');
      expect(deleteBtn).not.toBeNull();
      deleteBtn!.click();

      expect(onDelete).toHaveBeenCalledOnce();
      expect(onDelete).toHaveBeenCalledWith({ path: "photos", kind: "directory" });
    });

    it("does not throw when clicking delete button without onDelete handler", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "file.txt" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const deleteBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="file.txt"] .tree-delete-btn');
      expect(deleteBtn).not.toBeNull();
      expect(() => deleteBtn!.click()).not.toThrow();
    });
  });

  describe("reveal button", () => {
    it("renders for file and directory rows with correct data-reveal-path", () => {
      const entries: FileEntry[] = [
        fileEntry({ path: "docs/readme.txt" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries });

      const docsRevealBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="docs"] .tree-reveal-btn');
      const fileRevealBtn = treeEl.querySelector<HTMLButtonElement>('.tree-item[data-value="docs/readme.txt"] .tree-reveal-btn');

      expect(docsRevealBtn?.dataset.revealPath).toBe("docs");
      expect(fileRevealBtn?.dataset.revealPath).toBe("docs/readme.txt");
    });

    it("clicking reveal button calls onReveal with correct path", () => {
      const onReveal = vi.fn();
      const entries: FileEntry[] = [
        fileEntry({ path: "photos/img.jpg" }),
      ];

      renderFileTree({ treeEl, emptyStateEl, entries, onReveal });

      const revealBtn = treeEl.querySelector<HTMLButtonElement>(".tree-reveal-btn");
      expect(revealBtn).not.toBeNull();
      revealBtn!.click();

      expect(onReveal).toHaveBeenCalledOnce();
      expect(onReveal).toHaveBeenCalledWith(revealBtn!.dataset.revealPath);
    });
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
