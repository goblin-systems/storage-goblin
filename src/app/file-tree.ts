import { applyIcons, bindCheckboxTree } from "@goblin-systems/goblin-design-system";
import type { TreeHandle } from "@goblin-systems/goblin-design-system";
import type { BinEntrySource } from "./types";
import {
  VIRTUAL_THRESHOLD,
  renderFileTreeVirtual,
} from "./file-tree-virtual";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type FileEntryStatus = "synced" | "local-only" | "remote-only" | "review-required" | "conflict" | "glacier" | "deleted";

export type FileTreeMode = "live" | "bin";

export interface FileEntry {
  /** Relative path from the sync root, e.g. "photos/vacation/img001.jpg" */
  path: string;
  /** Entry kind returned by the backend */
  kind: "file" | "directory";
  /** Sync status */
  status: FileEntryStatus;
  /** Whether this entry currently exists locally (drives checkbox state) */
  hasLocalCopy: boolean;
  /** S3 storage class (e.g. "STANDARD", "GLACIER_IR") */
  storageClass?: string | null;
  /** Opaque backend bin identifier for restore actions */
  binKey?: string | null;
  /** Bin source metadata */
  deletedFrom?: BinEntrySource | null;
  /** When this entry was deleted */
  deletedAt?: string | null;
  /** Bin retention in days, if known */
  retentionDays?: number | null;
  /** When this entry expires from the bin, if known */
  expiresAt?: string | null;
  /** Source-side kind details for conflict resolution UI */
  localKind?: "file" | "directory" | null;
  remoteKind?: "file" | "directory" | null;
  /** Source-side metadata for conflict resolution UI */
  localSize?: number | null;
  remoteSize?: number | null;
  localModifiedAt?: string | null;
  remoteModifiedAt?: string | null;
  remoteEtag?: string | null;
}

export interface FileTreeHandle {
  destroy(): void;
}

export interface DeleteTarget {
  path: string;
  kind: "file" | "directory";
}

export interface FileTreeOptions {
  treeEl: HTMLUListElement;
  emptyStateEl: HTMLElement;
  entries: FileEntry[];
  mode?: FileTreeMode;
  onChange?: (checkedPaths: string[]) => void;
  checkedPaths?: string[];
  onReveal?: (path: string) => void;
  onDelete?: (target: DeleteTarget) => void;
  onRestore?: (entry: FileEntry) => void | Promise<void>;
  onStorageClass?: (path: string, currentStorageClass: string | null) => void;
  onResolveConflict?: (entry: FileEntry) => void;
  /** Version counts per file path. When provided, version badges and history buttons are shown. */
  versionCounts?: Map<string, number>;
  /** Called when the user clicks the version history button for a file. */
  onViewVersions?: (entry: FileEntry) => void;
}

const RESTORE_BUTTON_LABEL = "Restore";
const RESOLVE_BUTTON_LABEL = "Resolve";

function createRestoreButtonHtml(escapedPath: string, escapedBinKey: string, escapedEntry: string): string {
  return [
    `<button class="secondary-btn slim-btn tree-restore-btn" type="button" data-restore-path="${escapedPath}" data-restore-bin-key="${escapedBinKey}" data-restore-entry="${escapedEntry}" aria-busy="false">`,
    `<span class="tree-action-spinner" aria-hidden="true" hidden></span>`,
    `<span class="tree-action-label">${RESTORE_BUTTON_LABEL}</span>`,
    `</button>`,
  ].join("");
}

function createResolveButtonHtml(escapedPath: string): string {
  return [
    `<button class="icon-btn icon-btn-sm tree-resolve-btn" type="button" data-resolve-path="${escapedPath}" title="Resolve file conflict" aria-label="Resolve file conflict">`,
    `<i data-lucide="triangle-alert"></i>`,
    `<span class="tree-action-label">${RESOLVE_BUTTON_LABEL}</span>`,
    `</button>`,
  ].join("");
}

function isPromiseLike(value: unknown): value is PromiseLike<unknown> {
  return typeof value === "object" && value !== null && "then" in value && typeof value.then === "function";
}

export function setRestoreButtonBusy(button: HTMLButtonElement, busy: boolean) {
  button.classList.toggle("is-loading", busy);
  button.disabled = busy;
  button.setAttribute("aria-busy", busy ? "true" : "false");
  button.querySelector<HTMLElement>(".tree-action-spinner")?.toggleAttribute("hidden", !busy);
}

export function runRestoreAction(
  button: HTMLButtonElement,
  action: () => void | Promise<void>,
) {
  if (button.disabled) {
    return;
  }

  setRestoreButtonBusy(button, true);

  try {
    const result = action();
    if (!isPromiseLike(result)) {
      setRestoreButtonBusy(button, false);
      return;
    }

    void Promise.resolve(result).finally(() => {
      if (button.isConnected) {
        setRestoreButtonBusy(button, false);
      }
    });
  } catch (error) {
    setRestoreButtonBusy(button, false);
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Internal tree data structure
// ---------------------------------------------------------------------------

export interface TreeNode {
  /** Display name of this segment (e.g. "vacation" or "img001.jpg") */
  name: string;
  /** Full path from the sync root (e.g. "photos/vacation") */
  path: string;
  /** Depth in the tree (0-based) */
  depth: number;
  /** Direct child nodes of this node */
  children: TreeNode[];
  /** Explicit backend entry for this node, if one exists */
  entry: FileEntry | null;
}

type FileTreeNode = TreeNode & { entry: FileEntry & { kind: "file" } };

// ---------------------------------------------------------------------------
// Status helpers
// ---------------------------------------------------------------------------

export const STATUS_CLASS_MAP: Record<FileEntryStatus, string> = {
  "synced": "connected",
  "local-only": "untested",
  "remote-only": "untested",
  "review-required": "untested",
  "conflict": "error",
  "glacier": "glacier",
  "deleted": "error",
};

export const STATUS_TOOLTIP_MAP: Record<FileEntryStatus, string> = {
  "synced": "Synced locally and in cloud",
  "local-only": "Only on this device",
  "remote-only": "Only in cloud storage",
  "review-required": "Requires review before syncing changes",
  "conflict": "Sync conflict needs attention",
  "glacier": "Archived in Glacier storage",
  "deleted": "Deleted and available to restore",
};

export function getStatusTooltip(status: FileEntryStatus): string {
  return STATUS_TOOLTIP_MAP[status];
}

export function isResolvableConflictFileEntry(entry: FileEntry): boolean {
  return entry.kind === "file"
    && (entry.status === "conflict" || entry.status === "review-required")
    && entry.localKind === "file"
    && entry.remoteKind === "file";
}

export function canMutateLiveFileEntry(entry: FileEntry): boolean {
  return entry.status !== "review-required";
}

export function canMutateLiveDirectoryNode(node: TreeNode): boolean {
  const statuses = collectLeafStatuses(node.children);
  const nodeStatus = node.entry?.status;
  return nodeStatus !== "review-required"
    && nodeStatus !== "conflict"
    && !statuses.includes("review-required")
    && !statuses.includes("conflict");
}

export function isEntryCheckboxDisabled(entry: FileEntry, mode: FileTreeMode): boolean {
  return entry.status === "glacier"
    || entry.status === "review-required"
    || entry.status === "conflict";
}

function isDirectoryCheckboxDisabled(node: TreeNode, mode: FileTreeMode): boolean {
  return node.entry?.status === "conflict" || node.entry?.status === "review-required";
}

export function isFileNode(node: TreeNode): node is FileTreeNode {
  return node.entry?.kind === "file";
}

export function isDirectoryNode(node: TreeNode): boolean {
  return !isFileNode(node);
}

export function deriveDirectoryStatus(node: TreeNode): string {
  const statuses = collectLeafStatuses(node.children);
  if (statuses.length === 0) {
    return node.entry ? STATUS_CLASS_MAP[node.entry.status] : STATUS_CLASS_MAP["synced"];
  }
  if (statuses.some((s) => s === "conflict")) return STATUS_CLASS_MAP["conflict"];
  if (statuses.every((s) => s === "deleted")) return STATUS_CLASS_MAP["deleted"];
  if (statuses.every((s) => s === "synced")) return STATUS_CLASS_MAP["synced"];
  if (statuses.some((s) => s === "review-required")) return STATUS_CLASS_MAP["review-required"];
  if (statuses.some((s) => s === "glacier")) return STATUS_CLASS_MAP["glacier"];
  return STATUS_CLASS_MAP["local-only"];
}

export function deriveDirectoryStatusTooltip(node: TreeNode): string {
  const statuses = collectLeafStatuses(node.children);
  if (statuses.length === 0) {
    return node.entry ? getStatusTooltip(node.entry.status) : getStatusTooltip("synced");
  }
  if (statuses.some((s) => s === "conflict")) return "Contains sync conflicts";
  if (statuses.every((s) => s === "deleted")) return "All items deleted";
  if (statuses.every((s) => s === "synced")) return "All items synced";
  if (statuses.some((s) => s === "review-required")) return "Contains items requiring review";
  if (statuses.some((s) => s === "glacier")) return "Contains archived items";
  return "Contains unsynced items";
}

function collectLeafStatuses(nodes: TreeNode[]): FileEntryStatus[] {
  const result: FileEntryStatus[] = [];
  for (const node of nodes) {
    if (isFileNode(node)) {
      result.push(node.entry.status);
    } else {
      result.push(...collectLeafStatuses(node.children));
    }
  }
  return result;
}

function deriveDirectoryChecked(node: TreeNode, checkedLeafPaths: Set<string>, mode: FileTreeMode): boolean {
  const leafEntries = collectLeafEntries(node.children);
  if (leafEntries.length > 0) {
    return leafEntries.every((entry) => checkedLeafPaths.has(entry.path));
  }

  return mode === "bin"
    ? checkedLeafPaths.has(node.path)
    : node.entry?.hasLocalCopy ?? false;
}

export function collectLeafEntries(nodes: TreeNode[]): FileEntry[] {
  const result: FileEntry[] = [];
  for (const node of nodes) {
    if (isFileNode(node)) {
      result.push(node.entry);
    } else {
      result.push(...collectLeafEntries(node.children));
    }
  }
  return result;
}

// ---------------------------------------------------------------------------
// Build tree from flat entries
// ---------------------------------------------------------------------------

export function buildTree(entries: FileEntry[]): TreeNode[] {
  const root: TreeNode[] = [];

  /** Map from directory path → TreeNode for that directory */
  const directoryMap = new Map<string, TreeNode>();

  function ensureDirectory(segments: string[], depth: number): TreeNode {
    const dirPath = segments.slice(0, depth + 1).join("/");
    const existing = directoryMap.get(dirPath);
    if (existing) return existing;

    const node: TreeNode = {
      name: segments[depth],
      path: dirPath,
      depth,
      children: [],
      entry: null,
    };

    directoryMap.set(dirPath, node);

    if (depth === 0) {
      root.push(node);
    } else {
      const parent = ensureDirectory(segments, depth - 1);
      parent.children.push(node);
    }

    return node;
  }

  for (const entry of entries) {
    const segments = entry.path.split("/");

    if (entry.kind === "directory") {
      const directory = ensureDirectory(segments, segments.length - 1);
      directory.entry = entry;
      continue;
    }

    if (segments.length === 1) {
      // Top-level file
      root.push({
        name: segments[0],
        path: entry.path,
        depth: 0,
        children: [],
        entry,
      });
    } else {
      // File inside directories
      const parent = ensureDirectory(segments, segments.length - 2);
      parent.children.push({
        name: segments[segments.length - 1],
        path: entry.path,
        depth: segments.length - 1,
        children: [],
        entry,
      });
    }
  }

  return root;
}

// ---------------------------------------------------------------------------
// HTML generation
// ---------------------------------------------------------------------------

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function renderStatusIndicatorHtml(statusClass: string, tooltip: string): string {
  const escapedTooltip = escapeHtml(tooltip);
  return `<span class="status-indicator ${statusClass}" title="${escapedTooltip}" aria-label="${escapedTooltip}" role="img"><span class="status-dot" aria-hidden="true"></span></span>`;
}

function formatBinSource(source: BinEntrySource | null | undefined): string | null {
  if (source === "remote-bin") return "Remote bin";
  if (source === "object-versioning") return "Object versioning";
  return null;
}

function formatBinTimestamp(value: string | null | undefined): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function formatBinRetentionDays(value: number | null | undefined): string | null {
  if (typeof value !== "number" || !Number.isFinite(value)) return null;
  return value === 1 ? "1 day retention" : `${value} day retention`;
}

export function getBinLifecycleParts(entry: FileEntry): string[] {
  const parts: string[] = [];
  const source = formatBinSource(entry.deletedFrom ?? null);
  const deletedAt = formatBinTimestamp(entry.deletedAt ?? null);
  const expiresAt = formatBinTimestamp(entry.expiresAt ?? null);
  const retention = formatBinRetentionDays(entry.retentionDays ?? null);

  if (source) parts.push(source);
  if (deletedAt) parts.push(`Deleted ${deletedAt}`);
  if (expiresAt) {
    parts.push(`Expires ${expiresAt}`);
  } else if (retention) {
    parts.push(retention);
  }

  return parts;
}

function renderBinLifecycleHtml(entry: FileEntry): string {
  const parts = getBinLifecycleParts(entry);
  if (parts.length === 0) {
    return "";
  }

  const text = escapeHtml(parts.join(" • "));
  return `<span class="tree-bin-lifecycle" title="${text}">${text}</span>`;
}

function getCheckedLeafPaths(entries: FileEntry[], checkedPaths: string[] | undefined, mode: FileTreeMode): Set<string> {
  if (mode === "bin") {
    return new Set(checkedPaths ?? []);
  }

  return new Set(
    entries
      .filter((entry) => entry.kind === "file" && entry.hasLocalCopy)
      .map((entry) => entry.path),
  );
}

function encodeRestoreEntry(entry: FileEntry): string {
  return escapeHtml(JSON.stringify(entry));
}

function renderVersionBadgeHtml(count: number): string {
  const label = count === 1 ? "1 version" : `${count} versions`;
  return `<span class="badge default tree-version-badge" title="${label}">${label}</span>`;
}

function renderVersionsButtonHtml(escapedPath: string): string {
  return [
    `<button class="icon-btn icon-btn-sm tree-versions-btn" type="button" data-versions-path="${escapedPath}" title="View version history" aria-label="View version history">`,
    `<i data-lucide="history"></i>`,
    `</button>`,
  ].join("");
}

function renderNode(node: TreeNode, mode: FileTreeMode, checkedLeafPaths: Set<string>, versionCounts?: Map<string, number>): string {
  const isLeaf = isFileNode(node);
  const escapedPath = escapeHtml(node.path);
  const escapedName = escapeHtml(node.name);

  if (isLeaf) {
    const statusClass = STATUS_CLASS_MAP[node.entry.status];
    const statusTooltip = getStatusTooltip(node.entry.status);
    const checked = checkedLeafPaths.has(node.path) ? " checked" : "";
    const disabled = isEntryCheckboxDisabled(node.entry, mode) ? " disabled" : "";
    const versionCount = versionCounts?.get(node.path);
    return [
      `<li class="tree-item" data-value="${escapedPath}">`,
      `<div class="tree-row" style="--tree-depth: ${node.depth}">`,
      renderStatusIndicatorHtml(statusClass, statusTooltip),
      `<input type="checkbox" class="tree-check"${checked}${disabled} />`,
      `<button class="tree-leaf" type="button">`,
      `<i data-lucide="file"></i>`,
      `<span>${escapedName}</span>`,
      `</button>`,
      ...(mode === "live" && versionCounts !== undefined
        ? [renderVersionBadgeHtml(versionCount ?? 0)]
        : []),
      `<button class="icon-btn icon-btn-sm tree-reveal-btn" type="button" data-reveal-path="${escapedPath}" title="Reveal in file manager" aria-label="Reveal in file manager">`,
      `<i data-lucide="folder-open"></i>`,
      `</button>`,
      ...(mode === "bin" ? [renderBinLifecycleHtml(node.entry)] : []),
      ...(mode === "bin"
        ? [
          createRestoreButtonHtml(escapedPath, escapeHtml(node.entry.binKey ?? ""), encodeRestoreEntry(node.entry)),
        ]
        : [
          ...(mode === "live" && versionCounts !== undefined
            ? [renderVersionsButtonHtml(escapedPath)]
            : []),
          ...(isResolvableConflictFileEntry(node.entry)
            ? [createResolveButtonHtml(escapedPath)]
            : []),
          ...(canMutateLiveFileEntry(node.entry)
            ? [
              `<button class="icon-btn icon-btn-sm tree-storage-class-btn" type="button" data-storage-class-path="${escapedPath}">`,
              `<i data-lucide="snowflake"></i>`,
              `</button>`,
              `<button class="icon-btn icon-btn-sm tree-delete-btn" type="button" data-delete-path="${escapedPath}" data-delete-kind="file">`,
              `<i data-lucide="trash-2"></i>`,
              `</button>`,
            ]
            : []),
        ]),
      `</div>`,
      `</li>`,
    ].join("");
  }

  // Directory node
  const statusClass = deriveDirectoryStatus(node);
  const statusTooltip = deriveDirectoryStatusTooltip(node);
  const checked = deriveDirectoryChecked(node, checkedLeafPaths, mode) ? " checked" : "";
  const disabled = isDirectoryCheckboxDisabled(node, mode) ? " disabled" : "";
  const childrenHtml = node.children.map((child) => renderNode(child, mode, checkedLeafPaths, versionCounts)).join("");

  const restoreEntry: FileEntry = node.entry ?? {
    path: node.path,
    kind: "directory",
    status: "deleted",
    hasLocalCopy: false,
  };

  return [
    `<li class="tree-item" data-value="${escapedPath}">`,
    `<div class="tree-row" style="--tree-depth: ${node.depth}">`,
    renderStatusIndicatorHtml(statusClass, statusTooltip),
    `<input type="checkbox" class="tree-check"${checked}${disabled} />`,
    `<button class="tree-toggle" type="button">`,
    `<i data-lucide="chevron-right"></i>`,
    `<span>${escapedName}</span>`,
    `</button>`,
    `<button class="icon-btn icon-btn-sm tree-reveal-btn" type="button" data-reveal-path="${escapedPath}" title="Reveal in file manager" aria-label="Reveal in file manager">`,
    `<i data-lucide="folder-open"></i>`,
    `</button>`,
    ...(mode === "bin" ? [renderBinLifecycleHtml(restoreEntry)] : []),
    ...(mode === "bin"
      ? [createRestoreButtonHtml(escapedPath, escapeHtml(restoreEntry.binKey ?? ""), encodeRestoreEntry(restoreEntry))]
      : []),
    ...(mode === "live" && canMutateLiveDirectoryNode(node)
      ? [
        `<button class="icon-btn icon-btn-sm tree-delete-btn" type="button" data-delete-path="${escapedPath}" data-delete-kind="directory">`,
        `<i data-lucide="trash-2"></i>`,
        `</button>`,
      ]
      : []),
    `</div>`,
    `<ul class="tree-branch">`,
    childrenHtml,
    `</ul>`,
    `</li>`,
  ].join("");
}

function renderTreeHtml(roots: TreeNode[], mode: FileTreeMode, checkedLeafPaths: Set<string>, versionCounts?: Map<string, number>): string {
  return roots.map((node) => renderNode(node, mode, checkedLeafPaths, versionCounts)).join("");
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function renderFileTree(options: FileTreeOptions): FileTreeHandle {
  const { treeEl, emptyStateEl, entries, onChange, checkedPaths, mode = "live" } = options;

  // Empty state
  if (entries.length === 0) {
    emptyStateEl.hidden = false;
    treeEl.hidden = true;
    treeEl.innerHTML = "";
    return { destroy() {} };
  }

  // Virtual scrolling for large trees
  if (entries.length >= VIRTUAL_THRESHOLD) {
    return renderFileTreeVirtual(options);
  }

  // Show tree, hide empty state
  emptyStateEl.hidden = true;
  treeEl.hidden = false;

  // Build and inject HTML
  const tree = buildTree(entries);
  const checkedLeafPaths = getCheckedLeafPaths(entries, checkedPaths, mode);
  treeEl.innerHTML = renderTreeHtml(tree, mode, checkedLeafPaths, options.versionCounts);

  // Render lucide icon placeholders into SVGs
  applyIcons();

  // Wire keyboard navigation, expand/collapse, and checkbox propagation
  let treeHandle: TreeHandle | null = bindCheckboxTree({
    el: treeEl,
    onChange,
  });

  // Delete button delegation – fire onDelete for .tree-delete-btn clicks
  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-reveal-btn");
    if (!btn) return;
    e.stopPropagation();
    const path = btn.dataset.revealPath;
    if (path && options.onReveal) {
      options.onReveal(path);
    }
  });

  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-delete-btn");
    if (!btn) return;
    e.stopPropagation();
    const path = btn.dataset.deletePath;
    const kind = btn.dataset.deleteKind;
    if (path && (kind === "file" || kind === "directory") && options.onDelete) {
      options.onDelete({ path, kind });
    }
  });

  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-restore-btn");
    if (!btn) return;
    e.stopPropagation();
    const encodedEntry = btn.dataset.restoreEntry;
    if (encodedEntry && options.onRestore) {
      const entry = JSON.parse(encodedEntry) as FileEntry;
      runRestoreAction(btn, () => options.onRestore?.(entry));
    }
  });

  // Storage class button delegation
  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-storage-class-btn");
    if (!btn) return;
    e.stopPropagation();
    const path = btn.dataset.storageClassPath;
    if (path && options.onStorageClass) {
      const entry = entries.find((entry) => entry.path === path);
      options.onStorageClass(path, entry?.storageClass ?? null);
    }
  });

  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-resolve-btn");
    if (!btn) return;
    e.stopPropagation();
    const path = btn.dataset.resolvePath;
    if (path && options.onResolveConflict) {
      const entry = entries.find((candidate) => candidate.path === path);
      if (entry) {
        options.onResolveConflict(entry);
      }
    }
  });

  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-versions-btn");
    if (!btn) return;
    e.stopPropagation();
    const path = btn.dataset.versionsPath;
    if (path && options.onViewVersions) {
      const entry = entries.find((candidate) => candidate.path === path);
      if (entry) {
        options.onViewVersions(entry);
      }
    }
  });

  // Reconcile parent checkbox state bottom-up (no events, no onChange firing).
  // Walk all directory items (those with a .tree-branch child), deepest first,
  // and set checked/indeterminate based on direct children.
  const dirItems = Array.from(
    treeEl.querySelectorAll<HTMLLIElement>(".tree-item:has(> .tree-branch)"),
  );
  // Process deepest first by reversing document order (which is top-down)
  dirItems.reverse();
  for (const dirItem of dirItems) {
    const parentCheck = dirItem.querySelector<HTMLInputElement>(
      ":scope > .tree-row > .tree-check",
    );
    if (!parentCheck) continue;
    const childChecks = Array.from(
      dirItem.querySelectorAll<HTMLInputElement>(
        ":scope > .tree-branch > .tree-item > .tree-row > .tree-check",
      ),
    );
    if (childChecks.length === 0) continue;
    const allChecked = childChecks.every((c) => c.checked && !c.indeterminate);
    const noneChecked = childChecks.every(
      (c) => !c.checked && !c.indeterminate,
    );
    if (allChecked) {
      parentCheck.checked = true;
      parentCheck.indeterminate = false;
    } else if (noneChecked) {
      parentCheck.checked = false;
      parentCheck.indeterminate = false;
    } else {
      parentCheck.checked = false;
      parentCheck.indeterminate = true;
    }
  }

  return {
    destroy() {
      if (treeHandle) {
        treeHandle.destroy();
        treeHandle = null;
      }
    },
  };
}
