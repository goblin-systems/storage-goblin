import { createIcon } from "@goblin-systems/goblin-design-system";
import type {
  FileEntry,
  FileTreeHandle,
  FileTreeOptions,
} from "./file-tree";
import {
  buildTree,
  canMutateLiveFileEntry,
  canMutateLiveDirectoryNode,
  deriveDirectoryStatus,
  deriveDirectoryStatusTooltip,
  getBinLifecycleParts,
  getStatusTooltip,
  isEntryCheckboxDisabled,
  isResolvableConflictFileEntry,
  isDirectoryNode,
  isFileNode,
  runRestoreAction,
  STATUS_CLASS_MAP,
  type TreeNode,
} from "./file-tree";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const VIRTUAL_THRESHOLD = 2_000;
const ROW_HEIGHT_FALLBACK = 28;
const BUFFER_ROWS = 10;

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

interface FlatRow {
  index: number;
  node: TreeNode;
  depth: number;
  isDirectory: boolean;
  expanded: boolean;
  checked: boolean | "indeterminate";
  statusClass: string;
}

interface VirtualTreeState {
  roots: TreeNode[];
  nodesByPath: Map<string, TreeNode>;
  flatRows: FlatRow[];
  expandedPaths: Set<string>;
  checkedPaths: Set<string>;
  dirCheckState: Map<string, boolean | "indeterminate">;
  containerHeight: number;
  scrollTop: number;
  rowHeight: number;
  renderedRange: { start: number; end: number };
  onChange?: (checkedPaths: string[]) => void;
  onReveal?: (path: string) => void;
  onDelete?: (target: { path: string; kind: "file" | "directory" }) => void;
  onRestore?: (entry: FileEntry) => void;
  onStorageClass?: (path: string, currentStorageClass: string | null) => void;
  onResolveConflict?: (entry: FileEntry) => void;
  mode: "live" | "bin";
}

function getInitialCheckedPaths(entries: FileEntry[], mode: "live" | "bin", provided?: string[]): Set<string> {
  if (mode === "bin") {
    return new Set(provided ?? []);
  }

  return new Set(
    entries
      .filter((entry) => entry.kind === "file" && entry.hasLocalCopy)
      .map((entry) => entry.path),
  );
}

// ---------------------------------------------------------------------------
// Pure helpers (exported for testability)
// ---------------------------------------------------------------------------

/**
 * Walk the tree and produce a flat list of visible rows.
 * A directory's children are only included if the directory is in
 * `expandedPaths`.
 */
export function flattenVisible(
  roots: TreeNode[],
  expandedPaths: Set<string>,
  checkedPaths: Set<string>,
  dirCheckState: Map<string, boolean | "indeterminate">,
): FlatRow[] {
  const rows: FlatRow[] = [];

  function walk(nodes: TreeNode[], depth: number): void {
    for (const node of nodes) {
      let checked: boolean | "indeterminate";
      let statusClass: string;
      let isDir = false;

      if (isFileNode(node)) {
        checked = checkedPaths.has(node.path);
        statusClass = STATUS_CLASS_MAP[node.entry.status];
      } else {
        isDir = true;
        checked = dirCheckState.get(node.path) ?? false;
        statusClass = deriveDirectoryStatus(node);
      }

      const expanded = isDir && expandedPaths.has(node.path);

      rows.push({
        index: rows.length,
        node,
        depth,
        isDirectory: isDir,
        expanded,
        checked,
        statusClass,
      });

      if (isDir && expanded) {
        walk(node.children, depth + 1);
      }
    }
  }

  walk(roots, 0);
  return rows;
}

/**
 * Compute the start/end indices of rows that should be rendered, including
 * a buffer above and below the visible viewport.
 */
export function computeVisibleRange(
  scrollTop: number,
  containerHeight: number,
  totalRows: number,
  rowHeight: number,
  bufferRows: number,
): { start: number; end: number } {
  if (totalRows === 0 || rowHeight === 0) {
    return { start: 0, end: 0 };
  }

  const firstVisible = Math.floor(scrollTop / rowHeight);
  const visibleCount = Math.ceil(containerHeight / rowHeight);

  const start = Math.max(0, firstVisible - bufferRows);
  const end = Math.min(totalRows, firstVisible + visibleCount + bufferRows);

  return { start, end };
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/**
 * Index all nodes by path for quick lookups.
 */
function indexNodes(roots: TreeNode[]): Map<string, TreeNode> {
  const map = new Map<string, TreeNode>();

  function walk(nodes: TreeNode[]): void {
    for (const node of nodes) {
      map.set(node.path, node);
      if (node.children.length > 0) {
        walk(node.children);
      }
    }
  }

  walk(roots);
  return map;
}

/**
 * Bottom-up traversal computing whether each directory is `true`, `false`,
 * or `'indeterminate'` based on its leaf descendants' check state.
 */
function recomputeDirCheckStates(state: VirtualTreeState): void {
  state.dirCheckState.clear();

  function computeForNode(node: TreeNode): boolean | "indeterminate" {
    if (isFileNode(node)) {
      // Leaf — just return its checked state
      return state.checkedPaths.has(node.path);
    }

    // Directory: recurse into children
    const childStates = node.children.map(computeForNode);

    if (childStates.length === 0) {
      const result = state.checkedPaths.has(node.path) || (node.entry?.hasLocalCopy ?? false);
      state.dirCheckState.set(node.path, result);
      return result;
    }

    const allTrue = childStates.every((s) => s === true);
    const allFalse = childStates.every((s) => s === false);

    let result: boolean | "indeterminate";
    if (allTrue) {
      result = true;
    } else if (allFalse) {
      result = false;
    } else {
      result = "indeterminate";
    }

    state.dirCheckState.set(node.path, result);
    return result;
  }

  for (const root of state.roots) {
    computeForNode(root);
  }
}

/**
 * Collect all leaf paths under a given node.
 */
function collectLeafPaths(node: TreeNode): string[] {
  if (isFileNode(node)) {
    return [node.path];
  }
  const result: string[] = [];
  for (const child of node.children) {
    result.push(...collectLeafPaths(child));
  }
  return result;
}

function collectSelectablePaths(node: TreeNode): string[] {
  if (isFileNode(node)) {
    return [node.path];
  }

  const result = [node.path];
  for (const child of node.children) {
    result.push(...collectSelectablePaths(child));
  }
  return result;
}

/**
 * Collect all descendant directory paths (for collapsing).
 */
function collectDescendantDirPaths(node: TreeNode): string[] {
  const result: string[] = [];
  for (const child of node.children) {
    if (isDirectoryNode(child)) {
      result.push(child.path);
      result.push(...collectDescendantDirPaths(child));
    }
  }
  return result;
}

/**
 * Create a probe row element, measure its height, remove it.
 * Falls back to ROW_HEIGHT_FALLBACK.
 */
function measureRowHeight(container: HTMLElement): number {
  const probe = document.createElement("div");
  probe.className = "vtree-row";
  probe.style.visibility = "hidden";
  probe.style.position = "absolute";

  const inner = document.createElement("div");
  inner.className = "tree-row";
  inner.style.setProperty("--tree-depth", "0");
  inner.textContent = "M"; // Measurement character
  probe.appendChild(inner);

  container.appendChild(probe);
  const height = probe.getBoundingClientRect().height;
  container.removeChild(probe);

  return height > 0 ? height : ROW_HEIGHT_FALLBACK;
}

/**
 * Compute the checked paths to report to onChange — all checked leaf paths
 * plus all fully-checked directory paths.
 */
function getCheckedPathsForCallback(state: VirtualTreeState): string[] {
  const result: string[] = [...state.checkedPaths];

  for (const [dirPath, checkState] of state.dirCheckState) {
    if (checkState === true) {
      result.push(dirPath);
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Row element creation
// ---------------------------------------------------------------------------

function createRowElement(row: FlatRow, mode: "live" | "bin"): HTMLElement {
  // .vtree-row container
  const vtreeRow = document.createElement("div");
  vtreeRow.className = "vtree-row";
  vtreeRow.setAttribute("data-row-index", String(row.index));
  vtreeRow.setAttribute("data-value", row.node.path);

  // .tree-row inner div
  const treeRow = document.createElement("div");
  treeRow.className = "tree-row";
  treeRow.style.setProperty("--tree-depth", String(row.depth));
  vtreeRow.appendChild(treeRow);

  // Status indicator
  const statusIndicator = document.createElement("span");
  statusIndicator.className = `status-indicator ${row.statusClass}`;
  const statusTooltip = row.isDirectory
    ? deriveDirectoryStatusTooltip(row.node)
    : getStatusTooltip(row.node.entry!.status);
  statusIndicator.setAttribute("title", statusTooltip);
  statusIndicator.setAttribute("aria-label", statusTooltip);
  statusIndicator.setAttribute("role", "img");
  const statusDot = document.createElement("span");
  statusDot.className = "status-dot";
  statusDot.setAttribute("aria-hidden", "true");
  statusIndicator.appendChild(statusDot);
  treeRow.appendChild(statusIndicator);

  // Checkbox
  const checkbox = document.createElement("input");
  checkbox.type = "checkbox";
  checkbox.className = "tree-check";
  if (row.checked === true) {
    checkbox.checked = true;
    checkbox.indeterminate = false;
  } else if (row.checked === "indeterminate") {
    checkbox.checked = false;
    checkbox.indeterminate = true;
  } else {
    checkbox.checked = false;
    checkbox.indeterminate = false;
  }
  // Disable checkbox for non-destructive review/conflict states and bin mode
  if (row.isDirectory && (row.node.entry?.status === "conflict" || row.node.entry?.status === "review-required")) {
    checkbox.disabled = true;
  }
  if (!row.isDirectory && row.node.entry && isEntryCheckboxDisabled(row.node.entry, mode)) {
    checkbox.disabled = true;
  }
  treeRow.appendChild(checkbox);

  // Toggle button (directory) or leaf button (file)
  const button = document.createElement("button");
  button.type = "button";

  if (row.isDirectory) {
    button.className = "tree-toggle";
    const icon = createIcon("chevron-right");
    if (icon) button.appendChild(icon);
  } else {
    button.className = "tree-leaf";
    const icon = createIcon("file");
    if (icon) button.appendChild(icon);
  }

  const nameSpan = document.createElement("span");
  nameSpan.textContent = row.node.name;
  button.appendChild(nameSpan);

  treeRow.appendChild(button);

  const revealBtn = document.createElement("button");
  revealBtn.className = "icon-btn icon-btn-sm tree-reveal-btn";
  revealBtn.type = "button";
  revealBtn.setAttribute("data-reveal-path", row.node.path);
  revealBtn.setAttribute("title", "Reveal in file manager");
  revealBtn.setAttribute("aria-label", "Reveal in file manager");
  const revealIcon = createIcon("folder-open");
  if (revealIcon) revealBtn.appendChild(revealIcon);
  treeRow.appendChild(revealBtn);

  if (mode === "bin" && row.node.entry) {
    const lifecycle = document.createElement("span");
    lifecycle.className = "tree-bin-lifecycle";
    lifecycle.textContent = getBinLifecycleParts(row.node.entry).join(" • ");
    if (lifecycle.textContent) {
      lifecycle.setAttribute("title", lifecycle.textContent);
      treeRow.appendChild(lifecycle);
    }
  }

  // Storage class button for file rows only in live mode
  if (!row.isDirectory && mode === "live") {
    if (row.node.entry && isResolvableConflictFileEntry(row.node.entry)) {
      const resolveBtn = document.createElement("button");
      resolveBtn.className = "icon-btn icon-btn-sm tree-resolve-btn";
      resolveBtn.type = "button";
      resolveBtn.setAttribute("data-resolve-path", row.node.path);
      resolveBtn.setAttribute("title", "Resolve file conflict");
      resolveBtn.setAttribute("aria-label", "Resolve file conflict");
      const resolveIcon = createIcon("triangle-alert");
      if (resolveIcon) resolveBtn.appendChild(resolveIcon);
      const resolveLabel = document.createElement("span");
      resolveLabel.className = "tree-action-label";
      resolveLabel.textContent = "Resolve";
      resolveBtn.appendChild(resolveLabel);
      treeRow.appendChild(resolveBtn);
    }

    if (row.node.entry && canMutateLiveFileEntry(row.node.entry)) {
      const storageClassBtn = document.createElement("button");
      storageClassBtn.className = "icon-btn icon-btn-sm tree-storage-class-btn";
      storageClassBtn.type = "button";
      storageClassBtn.setAttribute("data-storage-class-path", row.node.path);
      const snowflakeIcon = createIcon("snowflake");
      if (snowflakeIcon) storageClassBtn.appendChild(snowflakeIcon);
      treeRow.appendChild(storageClassBtn);
    }
  }

  // Delete button for file rows only in live mode
  if (!row.isDirectory && mode === "live" && row.node.entry && canMutateLiveFileEntry(row.node.entry)) {
    const deleteBtn = document.createElement("button");
    deleteBtn.className = "icon-btn icon-btn-sm tree-delete-btn";
    deleteBtn.type = "button";
    deleteBtn.setAttribute("data-delete-path", row.node.path);
    deleteBtn.setAttribute("data-delete-kind", "file");
    const deleteIcon = createIcon("trash-2");
    if (deleteIcon) deleteBtn.appendChild(deleteIcon);
    treeRow.appendChild(deleteBtn);
  }

  if (row.isDirectory && mode === "live" && canMutateLiveDirectoryNode(row.node)) {
    const deleteBtn = document.createElement("button");
    deleteBtn.className = "icon-btn icon-btn-sm tree-delete-btn";
    deleteBtn.type = "button";
    deleteBtn.setAttribute("data-delete-path", row.node.path);
    deleteBtn.setAttribute("data-delete-kind", "directory");
    const deleteIcon = createIcon("trash-2");
    if (deleteIcon) deleteBtn.appendChild(deleteIcon);
    treeRow.appendChild(deleteBtn);
  }

  if (!row.isDirectory && mode === "bin") {
    const restoreBtn = document.createElement("button");
    restoreBtn.className = "secondary-btn slim-btn tree-restore-btn";
    restoreBtn.type = "button";
    restoreBtn.setAttribute("data-restore-path", row.node.path);
    restoreBtn.setAttribute("data-restore-bin-key", row.node.entry?.binKey ?? "");
    restoreBtn.setAttribute("data-restore-entry", JSON.stringify(row.node.entry));
    restoreBtn.setAttribute("aria-busy", "false");

    const restoreSpinner = document.createElement("span");
    restoreSpinner.className = "tree-action-spinner";
    restoreSpinner.setAttribute("aria-hidden", "true");
    restoreSpinner.hidden = true;

    const restoreLabel = document.createElement("span");
    restoreLabel.className = "tree-action-label";
    restoreLabel.textContent = "Restore";

    restoreBtn.append(restoreSpinner, restoreLabel);
    treeRow.appendChild(restoreBtn);
  }

  if (row.isDirectory && mode === "bin") {
    const restoreBtn = document.createElement("button");
    restoreBtn.className = "secondary-btn slim-btn tree-restore-btn";
    restoreBtn.type = "button";
    restoreBtn.setAttribute("data-restore-path", row.node.path);
    restoreBtn.setAttribute("data-restore-bin-key", row.node.entry?.binKey ?? "");
    restoreBtn.setAttribute("data-restore-entry", JSON.stringify(row.node.entry ?? {
      path: row.node.path,
      kind: "directory",
      status: "deleted",
      hasLocalCopy: false,
    }));
    restoreBtn.setAttribute("aria-busy", "false");

    const restoreSpinner = document.createElement("span");
    restoreSpinner.className = "tree-action-spinner";
    restoreSpinner.setAttribute("aria-hidden", "true");
    restoreSpinner.hidden = true;

    const restoreLabel = document.createElement("span");
    restoreLabel.className = "tree-action-label";
    restoreLabel.textContent = "Restore";

    restoreBtn.append(restoreSpinner, restoreLabel);
    treeRow.appendChild(restoreBtn);
  }

  return vtreeRow;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function renderFileTreeVirtual(
  options: FileTreeOptions,
): FileTreeHandle {
  const {
    treeEl,
    emptyStateEl,
    entries,
    onChange,
    checkedPaths: providedCheckedPaths,
    onReveal,
    onDelete,
    onRestore,
    onStorageClass,
    onResolveConflict,
    mode = "live",
  } = options;

  emptyStateEl.hidden = true;
  treeEl.hidden = true;
  treeEl.innerHTML = "";

  // Build tree and node index
  const roots = buildTree(entries);
  const nodesByPath = indexNodes(roots);

  // Initialize checked state from entries
  const checkedPaths = getInitialCheckedPaths(entries, mode, providedCheckedPaths);

  // Create virtual container
  const container = treeEl.parentElement!;
  const vtreeEl = document.createElement("div");
  vtreeEl.className = "vtree tree--dot-left";
  container.appendChild(vtreeEl);

  // Sentinel (creates scrollable area)
  const sentinelEl = document.createElement("div");
  sentinelEl.className = "vtree-sentinel";
  vtreeEl.appendChild(sentinelEl);

  // Viewport (holds rendered rows)
  const viewportEl = document.createElement("div");
  viewportEl.className = "vtree-viewport";
  vtreeEl.appendChild(viewportEl);

  // Initialize state
  const state: VirtualTreeState = {
    roots,
    nodesByPath,
    flatRows: [],
    expandedPaths: new Set<string>(),
    checkedPaths,
    dirCheckState: new Map(),
    containerHeight: 0,
    scrollTop: 0,
    rowHeight: ROW_HEIGHT_FALLBACK,
    renderedRange: { start: 0, end: 0 },
    onChange,
    onReveal,
    onDelete,
    onRestore,
    onStorageClass,
    onResolveConflict,
    mode,
  };

  // Compute initial directory check states
  recomputeDirCheckStates(state);

  // Flatten visible rows (all collapsed initially)
  state.flatRows = flattenVisible(
    state.roots,
    state.expandedPaths,
    state.checkedPaths,
    state.dirCheckState,
  );

  // Measure row height
  state.rowHeight = measureRowHeight(vtreeEl);

  // Update sentinel height
  function updateSentinelHeight(): void {
    sentinelEl.style.height = `${state.flatRows.length * state.rowHeight}px`;
  }

  // Render visible rows into the viewport
  function renderVisibleRows(): void {
    const range = computeVisibleRange(
      state.scrollTop,
      state.containerHeight,
      state.flatRows.length,
      state.rowHeight,
      BUFFER_ROWS,
    );

    // Only re-render if range actually changed
    if (
      range.start === state.renderedRange.start &&
      range.end === state.renderedRange.end
    ) {
      return;
    }

    state.renderedRange = range;

    // Clear and rebuild viewport rows
    viewportEl.innerHTML = "";

    for (let i = range.start; i < range.end; i++) {
      const row = state.flatRows[i];
      const el = createRowElement(row, state.mode);
      el.style.position = "absolute";
      el.style.top = `${i * state.rowHeight}px`;
      el.style.left = "0";
      el.style.right = "0";
      viewportEl.appendChild(el);
    }
  }

  // Re-flatten, update sentinel, and re-render
  function reFlattenAndRender(): void {
    state.flatRows = flattenVisible(
      state.roots,
      state.expandedPaths,
      state.checkedPaths,
      state.dirCheckState,
    );
    updateSentinelHeight();
    // Force re-render by clearing the cached range
    state.renderedRange = { start: -1, end: -1 };
    renderVisibleRows();
  }

  // Update checkbox states for currently visible rows
  function updateVisibleCheckboxes(): void {
    const checkboxes =
      viewportEl.querySelectorAll<HTMLInputElement>(".tree-check");
    for (const cb of checkboxes) {
      const vtreeRow = cb.closest(".vtree-row");
      if (!vtreeRow) continue;
      const path = vtreeRow.getAttribute("data-value");
      if (!path) continue;

      const node = state.nodesByPath.get(path);
      if (!node) continue;

      const isDir = isDirectoryNode(node);
      if (isDir) {
        const dirState = state.dirCheckState.get(path) ?? false;
        cb.checked = dirState === true;
        cb.indeterminate = dirState === "indeterminate";
      } else {
        cb.checked = state.checkedPaths.has(path);
        cb.indeterminate = false;
      }
    }
  }

  // --- Event delegation ---

  viewportEl.addEventListener("click", (e) => {
    const target = e.target as HTMLElement;

    // Handle storage class button clicks
    const storageClassBtn = target.closest(".tree-storage-class-btn");
    if (storageClassBtn) {
      e.stopPropagation();
      const storageClassPath = (storageClassBtn as HTMLElement).getAttribute("data-storage-class-path");
      if (storageClassPath && state.onStorageClass) {
        const node = state.nodesByPath.get(storageClassPath);
        const currentStorageClass = node?.entry?.storageClass ?? null;
        state.onStorageClass(storageClassPath, currentStorageClass);
      }
      return;
    }

    const revealBtn = target.closest(".tree-reveal-btn");
    if (revealBtn) {
      e.stopPropagation();
      const revealPath = (revealBtn as HTMLElement).getAttribute("data-reveal-path");
      if (revealPath && state.onReveal) {
        state.onReveal(revealPath);
      }
      return;
    }

    // Handle delete button clicks
    const deleteBtn = target.closest(".tree-delete-btn");
    if (deleteBtn) {
      e.stopPropagation();
      const deletePath = (deleteBtn as HTMLElement).getAttribute("data-delete-path");
      const deleteKind = (deleteBtn as HTMLElement).getAttribute("data-delete-kind");
      if (deletePath && (deleteKind === "file" || deleteKind === "directory") && state.onDelete) {
        state.onDelete({ path: deletePath, kind: deleteKind });
      }
      return;
    }

    const restoreBtn = target.closest(".tree-restore-btn");
    if (restoreBtn) {
      e.stopPropagation();
      const restorePath = (restoreBtn as HTMLElement).getAttribute("data-restore-path");
      const restoreEntry = (restoreBtn as HTMLElement).getAttribute("data-restore-entry");
      if (restorePath && restoreEntry && state.onRestore) {
        runRestoreAction(restoreBtn as HTMLButtonElement, () => state.onRestore?.(JSON.parse(restoreEntry) as FileEntry));
      }
      return;
    }

    const resolveBtn = target.closest(".tree-resolve-btn");
    if (resolveBtn) {
      e.stopPropagation();
      const resolvePath = (resolveBtn as HTMLElement).getAttribute("data-resolve-path");
      if (resolvePath && state.onResolveConflict) {
        const node = state.nodesByPath.get(resolvePath);
        if (node?.entry) {
          state.onResolveConflict(node.entry as FileEntry);
        }
      }
      return;
    }

    const toggleBtn = target.closest(".tree-toggle");
    if (!toggleBtn) return;

    const vtreeRow = toggleBtn.closest(".vtree-row");
    if (!vtreeRow) return;
    const path = vtreeRow.getAttribute("data-value");
    if (!path) return;

    // Toggle expand/collapse
    if (state.expandedPaths.has(path)) {
      // Collapse: remove this path and all descendant dir paths
      state.expandedPaths.delete(path);
      const node = state.nodesByPath.get(path);
      if (node) {
        for (const descendantPath of collectDescendantDirPaths(node)) {
          state.expandedPaths.delete(descendantPath);
        }
      }
    } else {
      state.expandedPaths.add(path);
    }

    reFlattenAndRender();
  });

  viewportEl.addEventListener("change", (e) => {
    const target = e.target as HTMLElement;
    if (!target.classList.contains("tree-check")) return;

    const checkbox = target as HTMLInputElement;
    const vtreeRow = checkbox.closest(".vtree-row");
    if (!vtreeRow) return;
    const path = vtreeRow.getAttribute("data-value");
    if (!path) return;

    const node = state.nodesByPath.get(path);
    if (!node) return;

    const isDir = isDirectoryNode(node);
    const isChecked = checkbox.checked;

    if (isDir) {
      const selectablePaths = state.mode === "bin"
        ? collectSelectablePaths(node)
        : collectLeafPaths(node);
      for (const leafPath of selectablePaths) {
        if (isChecked) {
          state.checkedPaths.add(leafPath);
        } else {
          state.checkedPaths.delete(leafPath);
        }
      }
    } else {
      // Leaf checkbox
      if (isChecked) {
        state.checkedPaths.add(path);
      } else {
        state.checkedPaths.delete(path);
      }
    }

    recomputeDirCheckStates(state);
    updateVisibleCheckboxes();

    if (state.onChange) {
      state.onChange(getCheckedPathsForCallback(state));
    }
  });

  // --- Scroll handling ---

  let rafPending = false;
  let rafId = 0;

  function onScroll(): void {
    if (rafPending) return;
    rafPending = true;
    rafId = requestAnimationFrame(() => {
      rafPending = false;
      state.scrollTop = vtreeEl.scrollTop;
      renderVisibleRows();
    });
  }

  vtreeEl.addEventListener("scroll", onScroll, { passive: true });

  // --- Resize handling ---

  let resizeObserver: ResizeObserver | null = null;
  if (typeof ResizeObserver !== "undefined") {
    resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        if (entry.target === vtreeEl) {
          state.containerHeight = entry.contentRect.height;
          // Force re-render by invalidating range
          state.renderedRange = { start: -1, end: -1 };
          renderVisibleRows();
        }
      }
    });
    resizeObserver.observe(vtreeEl);
  }

  // --- Initial render ---

  // Set initial container height (may be 0 if not yet in DOM)
  state.containerHeight = vtreeEl.clientHeight || 600;
  updateSentinelHeight();
  renderVisibleRows();

  // --- Return handle ---

  return {
    destroy() {
      vtreeEl.removeEventListener("scroll", onScroll);
      if (rafPending) {
        cancelAnimationFrame(rafId);
      }
      if (resizeObserver) {
        resizeObserver.disconnect();
        resizeObserver = null;
      }
      vtreeEl.remove();
    },
  };
}
