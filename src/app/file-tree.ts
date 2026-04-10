import { applyIcons, bindCheckboxTree } from "@goblin-systems/goblin-design-system";
import type { TreeHandle } from "@goblin-systems/goblin-design-system";
import {
  VIRTUAL_THRESHOLD,
  renderFileTreeVirtual,
} from "./file-tree-virtual";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export type FileEntryStatus = "synced" | "local-only" | "remote-only" | "conflict" | "glacier";

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
}

export interface FileTreeHandle {
  destroy(): void;
}

export interface FileTreeOptions {
  treeEl: HTMLUListElement;
  emptyStateEl: HTMLElement;
  entries: FileEntry[];
  onChange?: (checkedPaths: string[]) => void;
  onDelete?: (path: string) => void;
  onStorageClass?: (path: string, currentStorageClass: string | null) => void;
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
  "conflict": "error",
  "glacier": "glacier",
};

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
  if (statuses.every((s) => s === "synced")) return STATUS_CLASS_MAP["synced"];
  if (statuses.some((s) => s === "glacier")) return STATUS_CLASS_MAP["glacier"];
  return STATUS_CLASS_MAP["local-only"];
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

function deriveDirectoryChecked(node: TreeNode): boolean {
  const leafEntries = collectLeafEntries(node.children);
  if (leafEntries.length > 0) {
    return leafEntries.every((entry) => entry.hasLocalCopy);
  }
  return node.entry?.hasLocalCopy ?? false;
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

function renderNode(node: TreeNode): string {
  const isLeaf = isFileNode(node);
  const escapedPath = escapeHtml(node.path);
  const escapedName = escapeHtml(node.name);

  if (isLeaf) {
    const statusClass = STATUS_CLASS_MAP[node.entry.status];
    const checked = node.entry.hasLocalCopy ? " checked" : "";
    const disabled = node.entry.status === "glacier" ? " disabled" : "";
    return [
      `<li class="tree-item" data-value="${escapedPath}">`,
      `<div class="tree-row" style="--tree-depth: ${node.depth}">`,
      `<span class="status-indicator ${statusClass}"><span class="status-dot"></span></span>`,
      `<input type="checkbox" class="tree-check"${checked}${disabled} />`,
      `<button class="tree-leaf" type="button">`,
      `<i data-lucide="file"></i>`,
      `<span>${escapedName}</span>`,
      `</button>`,
      `<button class="icon-btn icon-btn-sm tree-storage-class-btn" type="button" data-storage-class-path="${escapedPath}">`,
      `<i data-lucide="snowflake"></i>`,
      `</button>`,
      `<button class="icon-btn icon-btn-sm tree-delete-btn" type="button" data-delete-path="${escapedPath}">`,
      `<i data-lucide="trash-2"></i>`,
      `</button>`,
      `</div>`,
      `</li>`,
    ].join("");
  }

  // Directory node
  const statusClass = deriveDirectoryStatus(node);
  const checked = deriveDirectoryChecked(node) ? " checked" : "";
  const childrenHtml = node.children.map(renderNode).join("");

  return [
    `<li class="tree-item" data-value="${escapedPath}">`,
    `<div class="tree-row" style="--tree-depth: ${node.depth}">`,
    `<span class="status-indicator ${statusClass}"><span class="status-dot"></span></span>`,
    `<input type="checkbox" class="tree-check"${checked} />`,
    `<button class="tree-toggle" type="button">`,
    `<i data-lucide="chevron-right"></i>`,
    `<span>${escapedName}</span>`,
    `</button>`,
    `</div>`,
    `<ul class="tree-branch">`,
    childrenHtml,
    `</ul>`,
    `</li>`,
  ].join("");
}

function renderTreeHtml(roots: TreeNode[]): string {
  return roots.map(renderNode).join("");
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export function renderFileTree(options: FileTreeOptions): FileTreeHandle {
  const { treeEl, emptyStateEl, entries, onChange } = options;

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
  treeEl.innerHTML = renderTreeHtml(tree);

  // Render lucide icon placeholders into SVGs
  applyIcons();

  // Wire keyboard navigation, expand/collapse, and checkbox propagation
  let treeHandle: TreeHandle | null = bindCheckboxTree({
    el: treeEl,
    onChange,
  });

  // Delete button delegation – fire onDelete for .tree-delete-btn clicks
  treeEl.addEventListener("click", (e) => {
    const btn = (e.target as HTMLElement).closest<HTMLButtonElement>(".tree-delete-btn");
    if (!btn) return;
    e.stopPropagation();
    const path = btn.dataset.deletePath;
    if (path && options.onDelete) {
      options.onDelete(path);
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
