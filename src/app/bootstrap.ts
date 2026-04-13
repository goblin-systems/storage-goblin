import {
  applyIcons,
  bindNavigation,
  closeModal,
  openModal,
  setupWindowControls,
  showToast,
} from "@goblin-systems/goblin-design-system";
import { createNativeActivity, createUiActivity } from "./activity";
import { createStorageGoblinClient } from "./client";
import { createAppDom } from "./dom";
import { renderFileTree, type DeleteTarget, type FileEntry, type FileTreeHandle, type FileTreeMode } from "./file-tree";
import {
  applyStoredProfile,
  DEFAULT_REMOTE_BIN_RETENTION_DAYS,
  DEFAULT_PROFILE_DRAFT,
  normalizeProfileDraft,
  toStoredProfile,
} from "./profile";
import { createProfilePersistence } from "./persistence";
import { describeSyncStatus, formatTimestamp, getSyncOverviewStats } from "./status";
import type {
  ActivityDebugLogState,
  ActivityItem,
  BinEntryMutationResult,
  BinEntryMutationSummary,
  BinEntryRequest,
  BinEntrySource,
  ConflictResolutionDetails,
  CredentialDraft,
  CredentialSummary,
  CredentialTestContext,
  LocationSyncStatus,
  PermissionProbeSummary,
  StorageProfileDraft,
  SyncLocation,
  SyncLocationDraft,
  SyncStatus,
} from "./types";

type DialogId = "credentials" | "locations" | "activity" | "settings";

type SyncStatusWithLocations = SyncStatus & {
  locations?: LocationSyncStatus[];
};

type LocationViewSelection = {
  locationId: string | null;
  mode: FileTreeMode;
};

type StatusMetric = {
  label: string;
  value: string;
};

type FileTreeSnapshot = {
  viewKey: string;
  entries: FileEntry[];
  entriesJson: string;
};

type AsyncConfirmOptions = {
  title: string;
  message: string;
  acceptLabel: string;
  rejectLabel: string;
  variant?: "danger";
  onAccept: () => Promise<void>;
};

type AsyncConfirmController = {
  open: (options: AsyncConfirmOptions) => Promise<boolean>;
  destroy: () => void;
};

type ConflictResolution = "keep-local" | "keep-remote";

type InlineCompareMode = ConflictResolutionDetails["mode"];

type InlineCompareState = {
  status: "idle" | "loading" | "ready" | "error";
  mode: InlineCompareMode | null;
  details: ConflictResolutionDetails | null;
  message: string;
};

type ConflictResolutionModalOptions = {
  locationLabel: string;
  entry: FileEntry;
  onCompare: (entry: FileEntry) => Promise<ConflictResolutionDetails>;
  onResolve: (entry: FileEntry, resolution: ConflictResolution) => Promise<void>;
};

type ConflictResolutionModalController = {
  open: (options: ConflictResolutionModalOptions) => void;
  close: () => void;
  destroy: () => void;
};

class HandledAsyncConfirmError extends Error {}

function encodeLocationSelectValue(locationId: string, mode: FileTreeMode): string {
  return `${mode}:${locationId}`;
}

function decodeLocationSelectValue(value: string): LocationViewSelection {
  if (!value) {
    return { locationId: null, mode: "live" };
  }

  const separatorIndex = value.indexOf(":");
  if (separatorIndex <= 0) {
    return { locationId: value, mode: "live" };
  }

  const mode = value.slice(0, separatorIndex) === "bin" ? "bin" : "live";
  const locationId = value.slice(separatorIndex + 1) || null;
  return { locationId, mode };
}

function describeRemoteBinBehavior(enabled: boolean, retentionDays: number): string {
  if (!enabled) {
    return "Deleting a file removes the local copy immediately and permanently deletes the remote object.";
  }

  return retentionDays === 1
    ? "Deleting a file removes the local copy immediately and moves the remote object into the remote bin for 1 day."
    : `Deleting a file removes the local copy immediately and moves the remote object into the remote bin for ${retentionDays} days.`;
}

function describeDeleteBehavior(location: SyncLocation): string {
  if (location.objectVersioningEnabled) {
    return "Deleting a file removes the local copy immediately and adds an S3 delete marker. You can restore deleted objects from bucket version history.";
  }

  return describeRemoteBinBehavior(location.remoteBin.enabled, location.remoteBin.retentionDays);
}

function parseRemoteBinRetentionDays(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_REMOTE_BIN_RETENTION_DAYS;
  }

  return Math.min(3650, Math.max(1, Math.round(parsed)));
}

function getDeleteConfirmationMessage(path: string, location: SyncLocation): string {
  if (location.objectVersioningEnabled) {
    return `"${path}" will be removed from local storage immediately. The remote object will be deleted using S3 object versioning so it can be restored from version history.`;
  }

  if (!location.remoteBin.enabled) {
    return `"${path}" will be removed from local storage immediately and permanently deleted from the remote bucket. This cannot be undone.`;
  }

  const retentionDays = location.remoteBin.retentionDays;
  const retentionLabel = retentionDays === 1 ? "1 day" : `${retentionDays} days`;
  return `"${path}" will be removed from local storage immediately. The remote object will be moved into this sync location's remote bin for ${retentionLabel}.`;
}

function getFolderDeleteConfirmationMessage(path: string, location: SyncLocation): string {
  if (location.objectVersioningEnabled) {
    return `Folder "${path}" and all nested contents will be removed from local storage immediately. Remote objects in this folder will be deleted using S3 object versioning so they can be restored from version history.`;
  }

  if (!location.remoteBin.enabled) {
    return `Folder "${path}" and all nested contents will be removed from local storage immediately and permanently deleted from the remote bucket. This cannot be undone.`;
  }

  const retentionDays = location.remoteBin.retentionDays;
  const retentionLabel = retentionDays === 1 ? "1 day" : `${retentionDays} days`;
  return `Folder "${path}" and all nested contents will be removed from local storage immediately. Remote objects in this folder will be moved into this sync location's remote bin for ${retentionLabel}.`;
}

function getLocationSyncStatusId(location: LocationSyncStatus): string {
  if (typeof location.pairId === "string") {
    return location.pairId;
  }

  if (typeof location.locationId === "string") {
    return location.locationId;
  }

  throw new Error("Location sync status is missing an identifier.");
}

function createInitialStatus(): SyncStatus {
  return {
    phase: "unconfigured",
    lastSyncAt: null,
    lastRescanAt: null,
    lastRemoteRefreshAt: null,
    lastError: null,
    currentFolder: null,
    currentBucket: null,
    currentPrefix: null,
    remotePollingEnabled: true,
    pollIntervalSeconds: 60,
    pendingOperations: 0,
    indexedFileCount: 0,
    indexedDirectoryCount: 0,
    indexedTotalBytes: 0,
    remoteObjectCount: 0,
    remoteTotalBytes: 0,
    comparison: {
      comparedAt: "",
      localFileCount: 0,
      remoteObjectCount: 0,
      exactMatchCount: 0,
      localOnlyCount: 0,
      remoteOnlyCount: 0,
      sizeMismatchCount: 0,
    },
    overview: {
      localFiles: 0,
      remoteFiles: 0,
      inSync: 0,
      notInSync: 0,
    },
    plan: {
      lastPlannedAt: null,
      observedPathCount: 0,
      uploadCount: 0,
      downloadCount: 0,
      conflictCount: 0,
      noopCount: 0,
      pendingOperationCount: 0,
      credentialsAvailable: false,
    },
  };
}

function setButtonBusy(button: HTMLButtonElement, busy: boolean) {
  button.classList.toggle("is-loading", busy);
  button.disabled = busy;
}

const FILE_TREE_LOADING_DELAY_MS = 150;

function createHandledAsyncConfirmError(message: string): HandledAsyncConfirmError {
  return new HandledAsyncConfirmError(message);
}

function formatConflictSize(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "Unavailable";
  return new Intl.NumberFormat().format(value);
}

function formatConflictTimestamp(value: string | null | undefined): string {
  return value ? formatTimestamp(value) : "Unavailable";
}

function formatConflictEtag(value: string | null | undefined): string {
  return value?.trim() ? value : "Unavailable";
}

function describeConflictKind(value: string | null | undefined): string {
  return value === "directory" ? "Directory" : value === "file" ? "File" : "Unavailable";
}

function isResolvableConflictEntry(entry: FileEntry): boolean {
  return entry.kind === "file"
    && (entry.status === "conflict" || entry.status === "review-required")
    && entry.localKind === "file"
    && entry.remoteKind === "file";
}

function createInitialInlineCompareState(): InlineCompareState {
  return {
    status: "idle",
    mode: null,
    details: null,
    message: "Select Compare to load inline previews or open external apps when inline compare is unavailable.",
  };
}

function getInlineCompareLoadingMessage(): string {
  return "Loading conflict comparison…";
}

function getInlineCompareExternalMessage(details: ConflictResolutionDetails | null): string {
  return details?.fallbackReason
    ?? "This file type uses your OS default app for comparison.";
}

function getInlineCompareErrorMessage(message: string): string {
  return `Compare failed: ${message}`;
}

function createConflictResolutionModalController(
  toastMessage: (message: string, variant?: "success" | "error" | "info") => void,
): ConflictResolutionModalController {
  const backdrop = document.createElement("section");
  backdrop.className = "modal-backdrop storage-modal storage-conflict-resolution-modal";
  backdrop.hidden = true;

  const dialog = document.createElement("div");
  dialog.className = "modal-card storage-modal-card storage-modal-card-wide storage-conflict-modal-card";

  const titleId = `storage-conflict-title-${Math.random().toString(36).slice(2)}`;
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");
  dialog.setAttribute("aria-labelledby", titleId);

  const header = document.createElement("div");
  header.className = "modal-header";

  const title = document.createElement("h3");
  title.id = titleId;

  const closeButton = document.createElement("button");
  closeButton.className = "icon-btn modal-close-btn";
  closeButton.type = "button";
  closeButton.setAttribute("aria-label", "Close conflict resolution dialog");
  closeButton.innerHTML = '<i data-lucide="x"></i>';

  header.append(title, closeButton);

  const body = document.createElement("div");
  body.className = "storage-conflict-modal-body";

  const intro = document.createElement("p");
  intro.className = "modal-body-text";

  const pathCallout = document.createElement("div");
  pathCallout.className = "callout storage-conflict-path-callout";

  const compareHint = document.createElement("p");
  compareHint.className = "hint storage-conflict-compare-hint";

  const compareState = document.createElement("div");
  compareState.className = "callout storage-conflict-compare-state";
  compareState.setAttribute("aria-live", "polite");

  const compareSurface = document.createElement("section");
  compareSurface.className = "storage-conflict-compare-surface";
  compareSurface.hidden = true;

  const compareColumns = document.createElement("div");
  compareColumns.className = "storage-conflict-compare-columns";

  const localComparePanel = document.createElement("section");
  localComparePanel.className = "settings-section mini-panel storage-conflict-compare-panel";
  const localCompareTitle = document.createElement("h4");
  localCompareTitle.textContent = "Local";
  const localCompareContent = document.createElement("div");
  localCompareContent.className = "storage-conflict-compare-content";
  localComparePanel.append(localCompareTitle, localCompareContent);

  const remoteComparePanel = document.createElement("section");
  remoteComparePanel.className = "settings-section mini-panel storage-conflict-compare-panel";
  const remoteCompareTitle = document.createElement("h4");
  remoteCompareTitle.textContent = "Remote";
  const remoteCompareContent = document.createElement("div");
  remoteCompareContent.className = "storage-conflict-compare-content";
  remoteComparePanel.append(remoteCompareTitle, remoteCompareContent);

  compareColumns.append(localComparePanel, remoteComparePanel);
  compareSurface.append(compareColumns);

  const metadataGrid = document.createElement("div");
  metadataGrid.className = "compact-list-grid details-grid storage-conflict-grid";

  const localPanel = document.createElement("section");
  localPanel.className = "settings-section mini-panel storage-conflict-panel";
  const localTitle = document.createElement("h4");
  localTitle.textContent = "Local";
  const localMeta = document.createElement("ul");
  localMeta.className = "compact-list status-list";
  localPanel.append(localTitle, localMeta);

  const remotePanel = document.createElement("section");
  remotePanel.className = "settings-section mini-panel storage-conflict-panel";
  const remoteTitle = document.createElement("h4");
  remoteTitle.textContent = "Remote";
  const remoteMeta = document.createElement("ul");
  remoteMeta.className = "compact-list status-list";
  remotePanel.append(remoteTitle, remoteMeta);

  metadataGrid.append(localPanel, remotePanel);
  body.append(intro, pathCallout, compareHint, compareState, compareSurface, metadataGrid);

  const footer = document.createElement("div");
  footer.className = "modal-footer storage-conflict-footer";

  const cancelButton = document.createElement("button");
  cancelButton.className = "secondary-btn";
  cancelButton.type = "button";
  cancelButton.textContent = "Cancel";

  const compareButton = document.createElement("button");
  compareButton.className = "secondary-btn";
  compareButton.type = "button";

  const keepLocalButton = document.createElement("button");
  keepLocalButton.className = "secondary-btn";
  keepLocalButton.type = "button";

  const keepRemoteButton = document.createElement("button");
  keepRemoteButton.className = "secondary-btn";
  keepRemoteButton.type = "button";

  const createConflictActionContent = (label: string) => {
    const content = document.createElement("span");
    content.className = "storage-conflict-action-content";

    const spinner = document.createElement("span");
    spinner.className = "storage-conflict-action-spinner";
    spinner.setAttribute("aria-hidden", "true");
    spinner.hidden = true;

    const labelEl = document.createElement("span");
    labelEl.className = "storage-conflict-action-label";
    labelEl.textContent = label;

    content.append(spinner, labelEl);
    return { content, spinner, labelEl };
  };

  const compareButtonContent = createConflictActionContent("Compare");
  const keepLocalButtonContent = createConflictActionContent("Keep local");
  const keepRemoteButtonContent = createConflictActionContent("Keep remote");

  compareButton.append(compareButtonContent.content);
  keepLocalButton.append(keepLocalButtonContent.content);
  keepRemoteButton.append(keepRemoteButtonContent.content);

  footer.append(cancelButton, compareButton, keepLocalButton, keepRemoteButton);
  dialog.append(header, body, footer);
  backdrop.append(dialog);
  document.body.append(backdrop);
  applyIcons();

  let visible = false;
  let busyAction: "compare" | ConflictResolution | null = null;
  let currentOptions: ConflictResolutionModalOptions | null = null;
  let inlineCompareState: InlineCompareState = createInitialInlineCompareState();

  const renderMetaList = (
    list: HTMLUListElement,
    values: Array<[string, string]>,
  ) => {
    list.innerHTML = "";
    for (const [label, value] of values) {
      const item = document.createElement("li");
      const labelEl = document.createElement("span");
      labelEl.textContent = label;
      const valueEl = document.createElement("strong");
      valueEl.textContent = value;
      item.append(labelEl, valueEl);
      list.append(item);
    }
  };

  const setComparePanelText = (container: HTMLDivElement, text: string) => {
    container.innerHTML = "";
    const pre = document.createElement("pre");
    pre.className = "storage-conflict-text-pane";
    pre.textContent = text;
    container.append(pre);
  };

  const setComparePanelImage = (container: HTMLDivElement, src: string, alt: string) => {
    container.innerHTML = "";
    const frame = document.createElement("div");
    frame.className = "storage-conflict-image-frame";
    const image = document.createElement("img");
    image.className = "storage-conflict-image-preview";
    image.src = src;
    image.alt = alt;
    frame.append(image);
    container.append(frame);
  };

  const renderInlineCompareState = () => {
    compareState.textContent = inlineCompareState.message;
    compareState.classList.toggle("danger", inlineCompareState.status === "error");
    compareSurface.hidden = inlineCompareState.status !== "ready" || inlineCompareState.mode === "external";

    if (inlineCompareState.status !== "ready" || !inlineCompareState.details) {
      localCompareContent.innerHTML = "";
      remoteCompareContent.innerHTML = "";
      return;
    }

    if (inlineCompareState.mode === "image") {
      setComparePanelImage(
        localCompareContent,
        inlineCompareState.details.localImageDataUrl ?? "",
        `Local preview for ${inlineCompareState.details.path}`,
      );
      setComparePanelImage(
        remoteCompareContent,
        inlineCompareState.details.remoteImageDataUrl ?? "",
        `Remote preview for ${inlineCompareState.details.path}`,
      );
      return;
    }

    if (inlineCompareState.mode === "text") {
      setComparePanelText(localCompareContent, inlineCompareState.details.localText ?? "");
      setComparePanelText(remoteCompareContent, inlineCompareState.details.remoteText ?? "");
      return;
    }

    localCompareContent.innerHTML = "";
    remoteCompareContent.innerHTML = "";
  };

  const syncBusyState = () => {
    const busy = busyAction !== null;
    cancelButton.disabled = busy;
    closeButton.disabled = busy;
    compareButton.disabled = busy || compareButton.dataset.compareEnabled !== "true";
    keepLocalButton.disabled = busy;
    keepRemoteButton.disabled = busy;
    compareButton.classList.toggle("is-loading", busyAction === "compare");
    keepLocalButton.classList.toggle("is-loading", busyAction === "keep-local");
    keepRemoteButton.classList.toggle("is-loading", busyAction === "keep-remote");
    compareButton.setAttribute("aria-busy", busyAction === "compare" ? "true" : "false");
    keepLocalButton.setAttribute("aria-busy", busyAction === "keep-local" ? "true" : "false");
    keepRemoteButton.setAttribute("aria-busy", busyAction === "keep-remote" ? "true" : "false");
    compareButtonContent.spinner.hidden = busyAction !== "compare";
    keepLocalButtonContent.spinner.hidden = busyAction !== "keep-local";
    keepRemoteButtonContent.spinner.hidden = busyAction !== "keep-remote";
  };

  const close = () => {
    if (!visible || busyAction) return;
    visible = false;
    currentOptions = null;
    inlineCompareState = createInitialInlineCompareState();
    renderInlineCompareState();
    closeModal({ backdrop });
  };

  const runAction = async (action: "compare" | ConflictResolution) => {
    if (!currentOptions || busyAction) return;
    busyAction = action;
    syncBusyState();
    try {
      if (action === "compare") {
        inlineCompareState = {
          status: "loading",
          mode: null,
          details: null,
          message: getInlineCompareLoadingMessage(),
        };
        renderInlineCompareState();

        const details = await currentOptions.onCompare(currentOptions.entry);
        inlineCompareState = {
          status: "ready",
          mode: details.mode,
          details,
          message: details.mode === "image"
            ? "Showing inline image previews."
            : details.mode === "text"
              ? "Showing inline text comparison."
              : getInlineCompareExternalMessage(details),
        };
        renderInlineCompareState();
      } else {
        await currentOptions.onResolve(currentOptions.entry, action);
        visible = false;
        currentOptions = null;
        closeModal({ backdrop });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (action === "compare") {
        inlineCompareState = {
          status: "error",
          mode: null,
          details: null,
          message: getInlineCompareErrorMessage(message),
        };
        renderInlineCompareState();
      }
      toastMessage(message, "error");
    } finally {
      busyAction = null;
      syncBusyState();
    }
  };

  backdrop.addEventListener("click", (event) => {
    if (event.target === backdrop) {
      close();
    }
  });
  closeButton.addEventListener("click", close);
  cancelButton.addEventListener("click", close);
  compareButton.addEventListener("click", () => void runAction("compare"));
  keepLocalButton.addEventListener("click", () => void runAction("keep-local"));
  keepRemoteButton.addEventListener("click", () => void runAction("keep-remote"));

  return {
    open(options) {
      currentOptions = options;
      busyAction = null;
      title.textContent = `Resolve file review · ${options.locationLabel}`;
      intro.textContent = "Choose which version should win for this file before syncing continues.";
      pathCallout.textContent = options.entry.path;

      renderMetaList(localMeta, [
        ["Kind", describeConflictKind(options.entry.localKind)],
        ["Size", formatConflictSize(options.entry.localSize)],
        ["Modified", formatConflictTimestamp(options.entry.localModifiedAt)],
      ]);

      renderMetaList(remoteMeta, [
        ["Kind", describeConflictKind(options.entry.remoteKind)],
        ["Size", formatConflictSize(options.entry.remoteSize)],
        ["Modified", formatConflictTimestamp(options.entry.remoteModifiedAt)],
        ["ETag", formatConflictEtag(options.entry.remoteEtag)],
      ]);

      const compareEnabled = options.entry.localKind === "file" && options.entry.remoteKind === "file";
      compareButton.dataset.compareEnabled = compareEnabled ? "true" : "false";
      compareHint.textContent = "Compare loads inline image/text previews when available and otherwise opens the local file plus a downloaded remote temp copy externally.";
      inlineCompareState = createInitialInlineCompareState();
      renderInlineCompareState();

      syncBusyState();
      visible = true;
      openModal({ backdrop });
      compareButton.focus();
    },
    close() {
      close();
    },
    destroy() {
      busyAction = null;
      currentOptions = null;
      backdrop.remove();
    },
  };
}

function createAsyncConfirmController(): AsyncConfirmController {
  const backdrop = document.createElement("section");
  backdrop.className = "modal-backdrop storage-modal storage-async-confirm-modal";
  backdrop.hidden = true;

  const dialog = document.createElement("div");
  dialog.className = "modal-card storage-modal-card";

  const titleId = `storage-async-confirm-title-${Math.random().toString(36).slice(2)}`;
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");
  dialog.setAttribute("aria-labelledby", titleId);

  const header = document.createElement("div");
  header.className = "modal-header";

  const title = document.createElement("h3");
  title.id = titleId;

  const closeButton = document.createElement("button");
  closeButton.className = "icon-btn modal-close-btn modal-btn-reject";
  closeButton.type = "button";
  closeButton.setAttribute("aria-label", "Close confirmation dialog");
  closeButton.innerHTML = '<i data-lucide="x"></i>';

  header.append(title, closeButton);

  const message = document.createElement("p");
  message.className = "modal-body-text";

  const footer = document.createElement("div");
  footer.className = "modal-footer";

  const rejectButton = document.createElement("button");
  rejectButton.className = "secondary-btn modal-btn-reject";
  rejectButton.type = "button";

  const acceptButton = document.createElement("button");
  acceptButton.className = "secondary-btn modal-btn-accept";
  acceptButton.type = "button";

  const acceptButtonLabel = document.createElement("span");
  acceptButtonLabel.className = "modal-btn-label";

  const acceptButtonSpinner = document.createElement("span");
  acceptButtonSpinner.className = "modal-btn-spinner";
  acceptButtonSpinner.setAttribute("aria-hidden", "true");
  acceptButtonSpinner.hidden = true;

  const acceptButtonBusyText = document.createElement("span");
  acceptButtonBusyText.className = "modal-btn-busy-text";
  acceptButtonBusyText.textContent = "Loading";
  acceptButtonBusyText.hidden = true;

  acceptButton.append(acceptButtonSpinner, acceptButtonLabel, acceptButtonBusyText);

  footer.append(rejectButton, acceptButton);
  dialog.append(header, message, footer);
  backdrop.append(dialog);
  document.body.append(backdrop);
  applyIcons();

  let busy = false;
  let visible = false;
  let bodyWasModalOpen = false;
  let currentPromise: Promise<boolean> | null = null;
  let currentResolve: ((accepted: boolean) => void) | null = null;
  let currentOnAccept: (() => Promise<void>) | null = null;

  const syncBusyState = () => {
    setButtonBusy(acceptButton, busy);
    acceptButton.setAttribute("aria-busy", busy ? "true" : "false");
    acceptButtonSpinner.hidden = !busy;
    acceptButtonBusyText.hidden = !busy;
    rejectButton.disabled = busy;
    closeButton.disabled = busy;
  };

  const hide = () => {
    visible = false;
    busy = false;
    syncBusyState();
    backdrop.hidden = true;

    if (!bodyWasModalOpen) {
      document.body.classList.remove("modal-open");
    }

    currentPromise = null;
    currentResolve = null;
    currentOnAccept = null;
  };

  const resolveAndHide = (accepted: boolean) => {
    if (!visible || busy) return;
    const resolve = currentResolve;
    hide();
    resolve?.(accepted);
  };

  const handleKeyDown = (event: KeyboardEvent) => {
    if (event.key !== "Escape" || !visible || busy) return;
    event.preventDefault();
    resolveAndHide(false);
  };

  backdrop.addEventListener("click", (event) => {
    if (event.target === backdrop && visible && !busy) {
      resolveAndHide(false);
    }
  });

  rejectButton.addEventListener("click", () => {
    resolveAndHide(false);
  });

  closeButton.addEventListener("click", () => {
    resolveAndHide(false);
  });

  acceptButton.addEventListener("click", async () => {
    if (!visible || busy || !currentOnAccept) return;

    busy = true;
    syncBusyState();

    try {
      await currentOnAccept();
      const resolve = currentResolve;
      hide();
      resolve?.(true);
    } catch {
      busy = false;
      syncBusyState();
    }
  });

  document.addEventListener("keydown", handleKeyDown, true);

  return {
    open(options) {
      if (currentPromise) {
        return currentPromise;
      }

      title.textContent = options.title;
      message.textContent = options.message;
      rejectButton.textContent = options.rejectLabel;
      acceptButtonLabel.textContent = options.acceptLabel;
      acceptButton.classList.toggle("danger", options.variant === "danger");

      busy = false;
      syncBusyState();
      currentOnAccept = options.onAccept;
      bodyWasModalOpen = document.body.classList.contains("modal-open");
      backdrop.hidden = false;
      document.body.classList.add("modal-open");
      visible = true;
      rejectButton.focus();

      currentPromise = new Promise<boolean>((resolve) => {
        currentResolve = resolve;
      });

      return currentPromise;
    },
    destroy() {
      document.removeEventListener("keydown", handleKeyDown, true);
      hide();
      backdrop.remove();
    },
  };
}

function createUnavailableCredential(id: string, name?: string | null): CredentialSummary {
  return {
    id,
    name: name?.trim() || "Missing credential",
    ready: false,
    validationStatus: "untested",
    lastTestedAt: null,
    lastTestMessage: null,
  };
}

function getCredentialValidationLabel(credential: CredentialSummary): string {
  switch (credential.validationStatus) {
    case "passed":
      return "test passed";
    case "failed":
      return "test failed";
    default:
      return "untested";
  }
}

function getCredentialTestActionLabel(credential: CredentialSummary): string {
  return credential.validationStatus === "untested" ? "Test" : "Re-test";
}

function getCredentialStorageLabel(credential: CredentialSummary): string {
  return credential.ready ? "stored securely" : "stored secret missing";
}

function getCredentialStorageBadgeLabel(credential: CredentialSummary): string {
  return credential.ready ? "stored" : "needs repair";
}

function getCredentialStorageBadgeTone(credential: CredentialSummary): "success" | "danger" {
  return credential.ready ? "success" : "danger";
}

function getCredentialValidationBadgeTone(credential: CredentialSummary): "success" | "danger" | "default" {
  return credential.validationStatus === "passed"
    ? "success"
    : credential.validationStatus === "failed"
      ? "danger"
      : "default";
}

function getCredentialTestSentence(credential: CredentialSummary): string {
  switch (credential.validationStatus) {
    case "passed":
      return "Its last test passed.";
    case "failed":
      return "Its last test failed.";
    default:
      return "It has not been tested yet.";
  }
}

function formatPermissionSummary(permissions: PermissionProbeSummary | null): string {
  if (!permissions) return "";

  const probeLabels: Record<string, string> = {
    put_object: "write",
    get_object: "read",
    delete_object: "delete",
  };

  const headBucket = permissions.probes.find((p) => p.name === "head_bucket");
  if (headBucket && !headBucket.allowed) {
    return `Bucket "${permissions.bucket}" is not accessible.`;
  }

  const labels = permissions.probes
    .filter((p) => p.name !== "head_bucket")
    .map((p) => `${probeLabels[p.name] ?? p.name} ${p.allowed ? "✓" : "✗"}`);

  return labels.length > 0 ? `Permissions: ${labels.join(" · ")}` : "";
}

function describeSelectedCredentialState(profile: StorageProfileDraft): string {
  if (!profile.credentialProfileId || !profile.selectedCredential) {
    return "Choose a saved credential before connecting";
  }

  if (!profile.selectedCredential.ready) {
    return "Selected credential reference exists, but its stored secret is missing. Recreate or replace it.";
  }

  return `Selected credential is stored securely. ${getCredentialTestSentence(profile.selectedCredential)}`;
}

function buildCredentialTestContext(profile: StorageProfileDraft): CredentialTestContext {
  return {
    region: profile.region,
    bucket: profile.bucket,
  };
}

function buildCredentialCreateMessage(credential: CredentialSummary): string {
  const savedState = credential.ready
    ? `Saved credential "${credential.name}" securely.`
    : `Saved credential "${credential.name}", but its stored secret needs attention.`;

  if (credential.validationStatus === "untested") {
    return `${savedState} It was not tested yet.`;
  }

  if (credential.validationStatus === "passed") {
    return `${savedState} It was tested and is valid.`;
  }

  return `${savedState} It was tested and failed.`;
}

function getCredentialDisplayName(profile: StorageProfileDraft): string {
  if (!profile.credentialProfileId) {
    return "No credential selected";
  }

  if (profile.selectedCredential) {
    return profile.selectedCredential.ready
      ? profile.selectedCredential.name
      : `${profile.selectedCredential.name} (stored secret missing)`;
  }

  return "Selected credential missing";
}

function syncProfileCredentialState(
  profile: StorageProfileDraft,
  credentials: CredentialSummary[],
): StorageProfileDraft {
  const credentialProfileId = profile.credentialProfileId?.trim() || null;

  if (!credentialProfileId) {
    return normalizeProfileDraft({
      ...profile,
      credentialProfileId: null,
      selectedCredential: null,
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: false,
    });
  }

  const availableCredential = credentials.find((credential) => credential.id === credentialProfileId) ?? null;
  const selectedCredential = availableCredential
    ?? (profile.selectedCredential?.id === credentialProfileId
      ? createUnavailableCredential(credentialProfileId, profile.selectedCredential.name)
      : createUnavailableCredential(credentialProfileId));

  return normalizeProfileDraft({
    ...profile,
    credentialProfileId,
    selectedCredential,
    selectedCredentialAvailable: Boolean(availableCredential?.ready),
    credentialsStoredSecurely: Boolean(availableCredential?.ready),
  });
}

type BootstrapCleanup = () => void;

type DebouncedFn<T extends (...args: unknown[]) => void> = T & {
  cancel: () => void;
};

export async function bootstrapStorageGoblin(): Promise<BootstrapCleanup> {
  setupWindowControls();
  applyIcons();

  const dom = createAppDom();
  const client = createStorageGoblinClient();
  const persistence = createProfilePersistence(client);

  const state: {
    activeDialog: DialogId | null;
    activeLocationId: string | null;
    activeLocationViewMode: FileTreeMode;
    profile: StorageProfileDraft;
    credentials: CredentialSummary[];
    syncLocations: SyncLocation[];
    status: SyncStatusWithLocations;
    activity: ActivityItem[];
    lastConnectAt: string | null;
    debugLogState: ActivityDebugLogState;
  } = {
    activeDialog: null,
    activeLocationId: null,
    activeLocationViewMode: "live",
    profile: DEFAULT_PROFILE_DRAFT,
    credentials: [],
    syncLocations: [],
    status: createInitialStatus(),
    activity: [],
    lastConnectAt: null,
    debugLogState: {
      enabled: false,
      logFilePath: null,
      logDirectoryPath: null,
    },
  };

  let fileTreeHandle: FileTreeHandle | null = null;
  const fileTreeSnapshots = new Map<string, FileTreeSnapshot>();
  let selectedBinPaths = new Set<string>();
  let fileTreeRequestSequence = 0;
  const asyncConfirm = createAsyncConfirmController();
  const conflictResolutionModal = createConflictResolutionModalController(toast);
  let fileTreeLoadingTimer: ReturnType<typeof setTimeout> | null = null;

  let fileTreeChangeTimer: ReturnType<typeof setTimeout> | null = null;
  function debouncedFileTreeChange(checkedPaths: string[]) {
    if (fileTreeChangeTimer !== null) clearTimeout(fileTreeChangeTimer);
    fileTreeChangeTimer = setTimeout(() => {
      fileTreeChangeTimer = null;
      void handleFileTreeChange(checkedPaths);
    }, 500);
  }

  function debounce<T extends (...args: unknown[]) => void>(fn: T, ms: number): DebouncedFn<T> {
    let timer: ReturnType<typeof setTimeout> | null = null;
    const debounced = ((...args: unknown[]) => {
      if (timer !== null) clearTimeout(timer);
      timer = setTimeout(() => { timer = null; fn(...args); }, ms);
    }) as DebouncedFn<T>;

    debounced.cancel = () => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    };

    return debounced;
  }

  const debouncedRefreshFileTree = debounce(() => void refreshFileTree(), 300);
  const debouncedRenderActivity = debounce(renderActivity, 150);

  function formatCount(value: number): string {
    return new Intl.NumberFormat().format(value);
  }

  function getFileTreeViewKey(locationId: string | null = state.activeLocationId, mode: FileTreeMode = state.activeLocationViewMode): string {
    return `${locationId ?? "none"}:${mode}`;
  }

  function getActiveLocation() {
    return state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId)
      : undefined;
  }

  function getSavedActiveLocation() {
    return state.activeLocationId
      ? state.profile.syncLocations.find((location) => location.id === state.activeLocationId)
      : undefined;
  }

  function getActiveLocationStatus() {
    return state.activeLocationId
      ? state.status.locations?.find((location) => getLocationSyncStatusId(location) === state.activeLocationId)
      : undefined;
  }

  function getViewSnapshot(viewKey: string = getFileTreeViewKey()): FileTreeSnapshot | null {
    return fileTreeSnapshots.get(viewKey) ?? null;
  }

  function getCurrentViewEntries(): FileEntry[] | null {
    return getViewSnapshot()?.entries ?? null;
  }

  function setCurrentViewEntries(entries: FileEntry[], viewKey: string = getFileTreeViewKey()) {
    fileTreeSnapshots.set(viewKey, {
      viewKey,
      entries,
      entriesJson: JSON.stringify(entries),
    });
  }

  function clearViewSnapshot(viewKey: string) {
    fileTreeSnapshots.delete(viewKey);
  }

  function clearLocationViewSnapshots(locationId: string) {
    clearViewSnapshot(getFileTreeViewKey(locationId, "live"));
    clearViewSnapshot(getFileTreeViewKey(locationId, "bin"));
  }

  async function refreshLocationViews(locationId: string, options: { clearCache?: boolean } = {}) {
    if (options.clearCache) {
      clearLocationViewSnapshots(locationId);
    }

    await refreshStatus();

    if (state.activeLocationId === locationId) {
      await refreshFileTree();
    }
  }

  function destroyFileTree() {
    if (fileTreeHandle) {
      fileTreeHandle.destroy();
      fileTreeHandle = null;
    }
  }

  function setFileTreeLoadingVisible(visible: boolean) {
    dom.fileTreeSection.classList.toggle("is-loading-tree", visible);
    dom.fileTreeSection.setAttribute("aria-busy", visible ? "true" : "false");
    dom.fileTreeLoadingIndicator.hidden = !visible;
  }

  function clearFileTreeLoadingTimer() {
    if (fileTreeLoadingTimer !== null) {
      clearTimeout(fileTreeLoadingTimer);
      fileTreeLoadingTimer = null;
    }
  }

  function beginFileTreeLoading(requestSequence: number) {
    clearFileTreeLoadingTimer();
    setFileTreeLoadingVisible(false);
    fileTreeLoadingTimer = setTimeout(() => {
      fileTreeLoadingTimer = null;
      if (requestSequence === fileTreeRequestSequence) {
        setFileTreeLoadingVisible(true);
      }
    }, FILE_TREE_LOADING_DELAY_MS);
  }

  function endFileTreeLoading(requestSequence?: number) {
    if (typeof requestSequence === "number" && requestSequence !== fileTreeRequestSequence) {
      return;
    }

    clearFileTreeLoadingTimer();
    setFileTreeLoadingVisible(false);
  }

  function renderFileTreeEntries(entries: FileEntry[], mode: FileTreeMode) {
    destroyFileTree();
    fileTreeHandle = renderFileTree({
      treeEl: dom.fileTree,
      emptyStateEl: dom.fileTreeEmptyState,
      entries,
      mode,
      checkedPaths: mode === "bin" ? Array.from(selectedBinPaths) : undefined,
      onChange: mode === "live"
        ? debouncedFileTreeChange
        : (checkedPaths) => {
          selectedBinPaths = new Set(checkedPaths);
          renderBinToolbar();
        },
      onReveal: handleReveal,
      onDelete: mode === "live" ? handleDelete : undefined,
      onRestore: mode === "bin" ? handleBinRestore : undefined,
      onStorageClass: mode === "live" ? handleStorageClassChange : undefined,
      onResolveConflict: mode === "live" ? handleResolveConflict : undefined,
    });
    renderBinToolbar();
  }

  function updateCurrentViewEntries(entries: FileEntry[]) {
    setCurrentViewEntries(entries);
    renderFileTreeEntries(entries, state.activeLocationViewMode);
    renderStatus();
  }

  function renderStatusMetrics(metrics: [StatusMetric, StatusMetric, StatusMetric, StatusMetric]) {
    const [local, remote, inSync, notInSync] = metrics;
    dom.statusOverviewLocalLabel.textContent = local.label;
    dom.statusOverviewLocal.textContent = local.value;
    dom.statusOverviewRemoteLabel.textContent = remote.label;
    dom.statusOverviewRemote.textContent = remote.value;
    dom.statusOverviewInSyncLabel.textContent = inSync.label;
    dom.statusOverviewInSync.textContent = inSync.value;
    dom.statusOverviewNotInSyncLabel.textContent = notInSync.label;
    dom.statusOverviewNotInSync.textContent = notInSync.value;
  }

  function getLiveStatusMetrics(status: SyncStatus | LocationSyncStatus): [StatusMetric, StatusMetric, StatusMetric, StatusMetric] {
    const overview = getSyncOverviewStats(status);
    return [
      { label: "Local", value: formatCount(overview.localFiles) },
      { label: "Remote", value: formatCount(overview.remoteFiles) },
      { label: "In sync", value: formatCount(overview.inSync) },
      { label: "Changes", value: formatCount(overview.notInSync) },
    ];
  }

  function isActionableLiveEntry(entry: FileEntry): boolean {
    return entry.status === "local-only"
      || entry.status === "remote-only"
      || entry.status === "review-required"
      || entry.status === "conflict";
  }

  function getLiveStatusMetricsFromEntries(entries: FileEntry[]): [StatusMetric, StatusMetric, StatusMetric, StatusMetric] {
    const files = entries.filter((entry) => entry.kind === "file");
    const localFiles = files.filter((entry) => entry.hasLocalCopy).length;
    const remoteFiles = files.filter((entry) => entry.status !== "local-only").length;
    const inSync = files.filter((entry) => entry.status === "synced").length;
    const changes = files.filter(isActionableLiveEntry).length;

    return [
      { label: "Local", value: formatCount(localFiles) },
      { label: "Remote", value: formatCount(remoteFiles) },
      { label: "In sync", value: formatCount(inSync) },
      { label: "Changes", value: formatCount(changes) },
    ];
  }

  function getBinStatusMetrics(): [StatusMetric, StatusMetric, StatusMetric, StatusMetric] {
    const activeLocation = getActiveLocation();
    const savedActiveLocation = getSavedActiveLocation();
    const activeLocationStatus = getActiveLocationStatus();
    const binEntries = state.activeLocationId
      ? getViewSnapshot(getFileTreeViewKey(state.activeLocationId, "bin"))?.entries ?? null
      : null;
    const remoteBinConfig = savedActiveLocation?.remoteBin ?? activeLocation?.remoteBin;
    const retentionValue = remoteBinConfig?.enabled
      ? `${remoteBinConfig.retentionDays}d`
      : "Off";
    const livePhase = activeLocationStatus
      ? describeSyncStatus(activeLocationStatus).badgeLabel
      : describeSyncStatus(state.status).badgeLabel;
    const pendingCount = activeLocationStatus?.plan.pendingOperationCount
      ?? activeLocationStatus?.pendingOperations
      ?? 0;

    return [
      { label: "Bin items", value: formatCount(binEntries?.length ?? 0) },
      { label: "Retention", value: retentionValue },
      { label: "Live phase", value: livePhase },
      { label: "Pending", value: formatCount(pendingCount) },
    ];
  }

  function getSelectedBinEntries(): FileEntry[] {
    const entries = getCurrentViewEntries() ?? [];
    return entries.filter((entry) => selectedBinPaths.has(entry.path) || (entry.kind === "directory" && selectedBinPaths.has(entry.path)));
  }

  function getBinSelectionSummaryText(): string {
    const count = selectedBinPaths.size;
    if (count === 0) {
      return "Select bin entries to restore or purge.";
    }

    return count === 1
      ? "1 bin entry selected."
      : `${formatCount(count)} bin entries selected.`;
  }

  function renderBinToolbar() {
    const isBinView = state.activeLocationViewMode === "bin" && Boolean(state.activeLocationId);
    dom.binToolbar.hidden = !isBinView;
    dom.binSelectionSummary.textContent = getBinSelectionSummaryText();
    dom.restoreSelectedBtn.disabled = !isBinView || selectedBinPaths.size === 0;
    dom.purgeSelectedBtn.disabled = !isBinView || selectedBinPaths.size === 0;
  }

  function clearBinSelection() {
    selectedBinPaths = new Set<string>();
    renderBinToolbar();
  }

  function buildBinEntryRequest(entry: FileEntry): BinEntryRequest {
    return {
      path: entry.path,
      kind: entry.kind,
      binKey: entry.binKey ?? null,
    };
  }

  function getSelectedBinPathsForEntries(entries: FileEntry[]): Set<string> {
    return new Set(entries.map((entry) => entry.path));
  }

  function isBinMutationSummary(value: unknown): value is BinEntryMutationSummary {
    return typeof value === "object"
      && value !== null
      && "results" in value
      && Array.isArray((value as { results?: unknown }).results);
  }

  function partitionBinMutationResults(
    requestedEntries: FileEntry[],
    summary: BinEntryMutationSummary | null,
  ): { successful: BinEntryMutationResult[]; failed: BinEntryMutationResult[] } {
    if (!summary) {
      return {
        successful: requestedEntries.map((entry) => ({
          path: entry.path,
          kind: entry.kind,
          binKey: entry.binKey ?? null,
          success: true,
          affectedCount: 1,
          error: null,
        })),
        failed: [],
      };
    }

    const successful = summary.results.filter((result) => result.success);
    const failed = summary.results.filter((result) => !result.success);
    return { successful, failed };
  }

  function getBinMutationOutcomeMessage(options: {
    action: "restore" | "purge";
    location: SyncLocation;
    requestedCount: number;
    successCount: number;
    failureCount: number;
  }): { toastMessage: string; toastVariant: "success" | "error" | "info"; activityMessage: string } {
    const { action, location, requestedCount, successCount, failureCount } = options;
    const noun = requestedCount === 1 ? "entry" : "entries";

    if (failureCount === 0) {
      if (action === "restore") {
        return {
          toastMessage: getBinRestoreToastMessage(location, successCount),
          toastVariant: "success",
          activityMessage: `Restored ${successCount} bin ${noun}`,
        };
      }

      return {
        toastMessage: successCount === 1 ? "Purged 1 bin entry permanently." : `Purged ${successCount} bin entries permanently.`,
        toastVariant: "success",
        activityMessage: `Purged ${successCount} bin ${noun} permanently`,
      };
    }

    if (successCount === 0) {
      return {
        toastMessage: action === "restore"
          ? `Restore failed for ${failureCount} bin ${noun}.`
          : `Purge failed for ${failureCount} bin ${noun}.`,
        toastVariant: "error",
        activityMessage: action === "restore"
          ? `Restore failed for ${failureCount} bin ${noun}`
          : `Purge failed for ${failureCount} bin ${noun}`,
      };
    }

    return {
      toastMessage: action === "restore"
        ? `Restored ${successCount} of ${requestedCount} bin ${noun}; ${failureCount} failed.`
        : `Purged ${successCount} of ${requestedCount} bin ${noun}; ${failureCount} failed.`,
      toastVariant: "info",
      activityMessage: action === "restore"
        ? `Partially restored bin ${noun}`
        : `Partially purged bin ${noun}`,
    };
  }

  function formatBinMutationFailureDetails(failed: BinEntryMutationResult[]): string | null {
    if (failed.length === 0) {
      return null;
    }

    return failed
      .map((result) => `${result.path}: ${result.error ?? "Unknown error"}`)
      .join("\n");
  }

  function getBinSourceLabel(source: BinEntrySource | null | undefined): string {
    return source === "object-versioning" ? "object version history" : "remote bin";
  }

  function getBinRestoreToastMessage(location: SyncLocation, count: number): string {
    const subject = count === 1 ? "entry" : "entries";
    return location.objectVersioningEnabled
      ? `Restored ${count} ${subject} from object version history.`
      : `Restored ${count} ${subject} from the remote bin.`;
  }

  function getBinPurgeConfirmationMessage(location: SyncLocation, count: number): string {
    const itemLabel = count === 1 ? "selected bin entry" : `${count} selected bin entries`;
    return location.objectVersioningEnabled
      ? `Purge ${itemLabel}? This permanently deletes the selected object versions from S3 version history. This cannot be undone.`
      : `Purge ${itemLabel}? This permanently deletes them from the remote bin. This cannot be undone.`;
  }

  function toast(message: string, variant: "success" | "error" | "info" = "info") {
    showToast(message, variant, 2200, "app-toast");
  }

  const dialogs: Record<DialogId, HTMLElement> = {
    credentials: dom.credentialsScreen,
    locations: dom.locationsScreen,
    activity: dom.activityScreen,
    settings: dom.settingsScreen,
  };

  function closeAllDialogs() {
    for (const dialog of Object.values(dialogs)) {
      if (!dialog.hidden) {
        closeModal({ backdrop: dialog });
      }
    }
    state.activeDialog = null;
  }

  function openDialog(dialogId: DialogId) {
    closeAllDialogs();
    if (dialogId === "locations") {
      renderLocationCredentialOptions();
      resetLocationForm();
    }
    state.activeDialog = dialogId;
    openModal({
      backdrop: dialogs[dialogId],
      onClose: () => {
        if (state.activeDialog === dialogId) {
          state.activeDialog = null;
        }
      },
    });
  }

  function addActivityItem(item: ActivityItem) {
    state.activity = [item, ...state.activity].slice(0, 36);
    debouncedRenderActivity();
  }

  function addActivity(level: ActivityItem["level"], message: string, details?: string | null) {
    addActivityItem(createUiActivity(level, message, details));
  }

  function renderDebugLogState() {
    const { enabled, logDirectoryPath, logFilePath } = state.debugLogState;

    dom.activityDebugModeInput.checked = state.profile.activityDebugModeEnabled;
    dom.debugLogStatusBadge.textContent = enabled ? "Enabled" : client.supportsNativeProfilePersistence ? "Disabled" : "Unavailable";
    dom.debugLogStatusBadge.className = `badge ${enabled ? "success" : "default"}`;
    dom.debugLogStatusText.textContent = client.supportsNativeProfilePersistence
      ? enabled
        ? "Detailed native activity logging is on. Open Activity from the menu to inspect richer event details."
        : "Detailed native activity logging is off. Turn it on and save settings to capture extra troubleshooting detail."
      : "Debug logging is not available in the browser preview.";
    dom.debugLogFilePath.textContent = logFilePath ?? logDirectoryPath ?? "Unavailable";
    dom.openDebugLogFolderBtn.disabled = !logDirectoryPath;
  }

  function renderActivity() {
    dom.activityList.innerHTML = "";
    const hasItems = state.activity.length > 0;
    dom.activityEmptyState.hidden = hasItems;
    dom.activityList.hidden = !hasItems;

    for (const item of state.activity) {
      const li = document.createElement("li");
      li.className = "activity-item";

      const message = document.createElement("span");
      message.className = "activity-message";
      message.textContent = item.message;

      const meta = document.createElement("span");
      meta.className = "activity-meta";
      meta.textContent = `${item.level.toUpperCase()} · ${formatTimestamp(item.timestamp)}`;

      li.append(message, meta);

      if (item.details) {
        const details = document.createElement("details");
        details.className = "activity-details";

        const summary = document.createElement("summary");
        summary.textContent = "Debug details";

        const pre = document.createElement("pre");
        pre.className = "activity-detail-text";
        pre.textContent = item.details;

        details.append(summary, pre);
        li.append(details);
      }

      dom.activityList.append(li);
    }
  }

  function renderCredentialsList() {
    dom.credentialsList.innerHTML = "";

    const count = state.credentials.length;
    dom.credentialsCountBadge.textContent = `${count} saved`;
    dom.credentialsSupportBadge.textContent = client.supportsNativeProfilePersistence ? "Desktop app" : "Preview only";
    dom.credentialsSupportBadge.className = `badge ${client.supportsNativeProfilePersistence ? "success" : "default"}`;
    dom.credentialsSupportText.textContent = client.supportsNativeProfilePersistence
      ? "Create a named credential once, then reuse it across sync locations without re-entering raw keys."
      : "Browser preview shows the credential workflow but does not create or store real credentials.";
    dom.createCredentialBtn.disabled = !client.supportsNativeProfilePersistence;

    dom.credentialsListStatus.textContent = count > 0
      ? "Saved credentials show secure storage state and test state separately."
      : client.supportsNativeProfilePersistence
        ? "Create your first named credential, then assign it to a sync location."
        : "Open the desktop app to create and manage credentials.";

    dom.credentialsEmptyState.hidden = count > 0;
    dom.credentialsList.hidden = count === 0;

    for (const credential of state.credentials) {
      const li = document.createElement("li");
      li.className = "credential-item";

      const meta = document.createElement("div");
      meta.className = "credential-item-meta";

      const name = document.createElement("strong");
      name.textContent = credential.name;

      const hint = document.createElement("span");
      hint.className = "hint";
      hint.textContent = credential.id === state.profile.credentialProfileId
        ? `Selected for this bucket · ${getCredentialStorageLabel(credential)} · ${getCredentialValidationLabel(credential)}`
        : `${getCredentialStorageLabel(credential)} · ${getCredentialValidationLabel(credential)}`;

      meta.append(name, hint);

      const actions = document.createElement("div");
      actions.className = "credential-item-actions";

      const availabilityBadge = document.createElement("span");
      availabilityBadge.className = `badge ${getCredentialStorageBadgeTone(credential)}`;
      availabilityBadge.textContent = getCredentialStorageBadgeLabel(credential);
      actions.append(availabilityBadge);

      const validationBadge = document.createElement("span");
      validationBadge.className = `badge ${getCredentialValidationBadgeTone(credential)}`;
      validationBadge.textContent = getCredentialValidationLabel(credential);
      actions.append(validationBadge);

      if (credential.id === state.profile.credentialProfileId) {
        const selectedBadge = document.createElement("span");
        selectedBadge.className = "badge default";
        selectedBadge.textContent = "selected";
        actions.append(selectedBadge);
      }

      const testButton = document.createElement("button");
      testButton.className = "secondary-btn slim-btn";
      testButton.type = "button";
      testButton.textContent = getCredentialTestActionLabel(credential);
      testButton.disabled = !client.supportsNativeProfilePersistence;
      testButton.title = !client.supportsNativeProfilePersistence
        ? "Credential testing is only available in the desktop app."
        : "";
      testButton.addEventListener("click", async () => {
        setButtonBusy(testButton, true);

        try {
          const result = await client.testCredential({
            credentialId: credential.id,
            context: buildCredentialTestContext(state.profile),
          });

          state.credentials = state.credentials.map((item) => item.id === result.credential.id ? result.credential : item);
          if (!state.credentials.some((item) => item.id === result.credential.id)) {
            state.credentials = [...state.credentials, result.credential];
          }

          state.profile = syncProfileCredentialState(normalizeProfileDraft({
            ...state.profile,
            selectedCredential: state.profile.credentialProfileId === result.credential.id
              ? result.credential
              : state.profile.selectedCredential,
          }), state.credentials);
          renderProfileSummary();

          const baseMessage = result.ok
            ? `Credential "${result.credential.name}" test passed. Can access ${result.bucketCount} bucket(s).`
            : `Credential "${result.credential.name}" test failed.`;

          const permissionLine = formatPermissionSummary(result.permissions);
          const displayMessage = permissionLine ? `${baseMessage} ${permissionLine}` : baseMessage;

          dom.credentialsResult.textContent = displayMessage;
          toast(displayMessage, result.ok ? "success" : "error");
          addActivity(result.ok ? "success" : "error", displayMessage);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const surfacedMessage = `Credential test failed: ${message}`;
          dom.credentialsResult.textContent = surfacedMessage;
          toast(surfacedMessage, "error");
          addActivity("error", surfacedMessage);
        } finally {
          setButtonBusy(testButton, false);
        }
      });
      actions.append(testButton);

      const deleteButton = document.createElement("button");
      deleteButton.className = "secondary-btn slim-btn";
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.disabled = !client.supportsNativeProfilePersistence;
      deleteButton.addEventListener("click", async () => {
        const wasSelected = credential.id === state.profile.credentialProfileId;
        await asyncConfirm.open({
          title: "Delete credential?",
          message: wasSelected
            ? `"${credential.name}" will be deleted. This bucket will need a different credential before it can sync again.`
            : `"${credential.name}" will be permanently deleted.`,
          acceptLabel: "Delete",
          rejectLabel: "Cancel",
          variant: "danger",
          onAccept: async () => {
            try {
              const result = await client.deleteCredential(credential.id);
              if (!result.deleted) {
                const message = client.supportsNativeProfilePersistence
                  ? `Could not delete credential \"${credential.name}\".`
                  : "Credential deletion is only available in the desktop app.";
                dom.credentialsResult.textContent = message;
                toast(message, "info");
                throw createHandledAsyncConfirmError(message);
              }

              if (wasSelected) {
                state.profile = syncProfileCredentialState(normalizeProfileDraft({
                  ...state.profile,
                  credentialProfileId: result.profile.credentialProfileId,
                  selectedCredential: result.profile.selectedCredential,
                  selectedCredentialAvailable: result.profile.selectedCredentialAvailable,
                  credentialsStoredSecurely: result.profile.credentialsStoredSecurely,
                }), state.credentials.filter((item) => item.id !== credential.id));
              }

              const message = `Deleted credential \"${credential.name}\".`;
              dom.credentialsResult.textContent = message;
              addActivity("info", message);
              toast(message, "success");
              await refreshCredentials();
              renderProfileSummary();
            } catch (error) {
              if (error instanceof HandledAsyncConfirmError) {
                throw error;
              }

              const message = error instanceof Error ? error.message : String(error);
              const surfacedMessage = `Delete credential failed: ${message}`;
              dom.credentialsResult.textContent = surfacedMessage;
              addActivity("error", surfacedMessage);
              toast(surfacedMessage, "error");
              throw createHandledAsyncConfirmError(surfacedMessage);
            }
          },
        });
      });
      actions.append(deleteButton);

      li.append(meta, actions);
      dom.credentialsList.append(li);
    }
  }

  function renderStatus() {
    const activeLocationStatus = getActiveLocationStatus();
    const activeLocation = getActiveLocation();
    const effectiveStatus = activeLocationStatus ?? state.status;
    const aggregatePresentation = describeSyncStatus(state.status);
    const effectivePresentation = describeSyncStatus(effectiveStatus);
    const headerPresentation = activeLocationStatus ? effectivePresentation : aggregatePresentation;

    dom.syncPhaseBadge.textContent = headerPresentation.badgeLabel;
    dom.syncPhaseBadge.className = `badge ${headerPresentation.badgeTone}`;
    dom.statusPhaseInline.textContent = effectivePresentation.badgeLabel;
    dom.statusPhaseInline.className = `badge ${effectivePresentation.badgeTone}`;
    dom.statusSummary.textContent = effectivePresentation.summary;
    dom.windowSubtitle.textContent = headerPresentation.summary;

    const cachedLiveEntries = state.activeLocationId
      ? getViewSnapshot(getFileTreeViewKey(state.activeLocationId, "live"))?.entries ?? null
      : null;

    renderStatusMetrics(
      activeLocationStatus || !cachedLiveEntries
        ? getLiveStatusMetrics(effectiveStatus)
        : getLiveStatusMetricsFromEntries(cachedLiveEntries),
    );

    if (state.activeLocationViewMode === "bin" && state.activeLocationId) {
      const label = activeLocation?.label || activeLocation?.bucket || "selected location";
      dom.statusPhaseInline.textContent = "Bin";
      dom.statusPhaseInline.className = "badge danger";
      dom.statusSummary.textContent = `Viewing ${label} Bin. Restore entries back into the live sync location.`;
      dom.windowSubtitle.textContent = `Viewing ${label} Bin.`;
      renderStatusMetrics(getBinStatusMetrics());
    }
  }

  function renderFileTreeViewState() {
    dom.fileTreeSection.classList.toggle("is-bin-view", state.activeLocationViewMode === "bin");
    renderBinToolbar();
    const emptyStateText = state.activeLocationViewMode === "bin"
      ? "Select a sync location bin to browse deleted files."
      : "Select a sync location to browse files.";
    const emptyStateCard = dom.fileTreeEmptyState.querySelector<HTMLElement>(".empty-state-card");
    if (emptyStateCard) {
      emptyStateCard.textContent = emptyStateText;
    } else {
      dom.fileTreeEmptyState.textContent = emptyStateText;
    }
  }

  function renderProfileSummary() {
    renderCredentialsList();
    renderStatus();
    renderLocationCredentialOptions();
  }

  function writeSettingsToDom() {
    dom.remotePollingInput.checked = state.profile.remotePollingEnabled;
    dom.pollIntervalInput.value = String(state.profile.pollIntervalSeconds);
    dom.conflictStrategySelect.value = state.profile.conflictStrategy;
    dom.activityDebugModeInput.checked = state.profile.activityDebugModeEnabled;
  }

  function readSettingsFromDom() {
    state.profile = normalizeProfileDraft({
      ...state.profile,
      remotePollingEnabled: dom.remotePollingInput.checked,
      pollIntervalSeconds: Number(dom.pollIntervalInput.value),
      conflictStrategy: dom.conflictStrategySelect.value as StorageProfileDraft["conflictStrategy"],
      activityDebugModeEnabled: dom.activityDebugModeInput.checked,
    });
  }

  function renderLocationRemoteBinState() {
    const objectVersioningEnabled = dom.locationObjectVersioningEnabledInput.checked;
    const enabled = objectVersioningEnabled ? false : dom.locationRemoteBinEnabledInput.checked;
    const retentionDays = parseRemoteBinRetentionDays(dom.locationRemoteBinRetentionInput.value);
    if (objectVersioningEnabled) {
      dom.locationRemoteBinEnabledInput.checked = false;
    }
    dom.locationRemoteBinEnabledInput.disabled = objectVersioningEnabled;
    dom.locationRemoteBinRetentionInput.value = String(retentionDays);
    dom.locationRemoteBinRetentionInput.disabled = objectVersioningEnabled || !enabled;
    dom.locationRemoteBinHint.textContent = objectVersioningEnabled
      ? "Object versioning is enabled for this sync location. Remote bin is unavailable in this mode; deleted objects will be recovered from S3 version history instead."
      : describeRemoteBinBehavior(enabled, retentionDays);
  }

  async function refreshStatus() {
    state.status = await client.getSyncStatus();
    renderStatus();
  }

  async function refreshDebugLogState() {
    state.debugLogState = await client.getActivityDebugLogState();
    renderDebugLogState();
  }

  async function refreshCredentials() {
    state.credentials = await client.listCredentials();
    state.profile = syncProfileCredentialState(state.profile, state.credentials);
    renderProfileSummary();
  }

  function mergeSyncLocationsWithStoredProfile(listedLocations: SyncLocation[]): SyncLocation[] {
    const storedLocations = state.profile.syncLocations ?? [];
    const storedLocationIds = new Set(storedLocations.map((location) => location.id));

    if (storedLocationIds.size === 0 && !state.profile.activeLocationId) {
      return listedLocations;
    }

    const listedLocationsById = new Map(listedLocations.map((location) => [location.id, location]));
    return storedLocations.map((storedLocation) => {
      const listedLocation = listedLocationsById.get(storedLocation.id);
      if (!listedLocation) {
        return storedLocation;
      }

      return {
        ...listedLocation,
        objectVersioningEnabled: storedLocation.objectVersioningEnabled,
        remoteBin: storedLocation.remoteBin,
      };
    });
  }

  function applySyncLocationState(syncLocations: SyncLocation[], preferredActiveLocationId: string | null = state.activeLocationId) {
    const activeLocationExists = preferredActiveLocationId
      ? syncLocations.some((location) => location.id === preferredActiveLocationId)
      : false;

    state.syncLocations = syncLocations;
    state.activeLocationId = syncLocations.length === 0
      ? null
      : activeLocationExists
        ? preferredActiveLocationId
        : syncLocations[0].id;
    if (state.activeLocationId === null) {
      state.activeLocationViewMode = "live";
    }
    state.profile = normalizeProfileDraft({
      ...state.profile,
      syncLocations,
      activeLocationId: state.activeLocationId,
    });
  }

  function renderLocationDropdown() {
    const select = dom.activeLocationSelect;
    select.innerHTML = "";

    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Select a sync location";
    select.append(defaultOption);

    for (const location of state.syncLocations) {
      const liveOption = document.createElement("option");
      liveOption.value = encodeLocationSelectValue(location.id, "live");
      liveOption.textContent = location.label || `${location.bucket}`;
      select.append(liveOption);

      const binOption = document.createElement("option");
      binOption.value = encodeLocationSelectValue(location.id, "bin");
      binOption.textContent = `${location.label || location.bucket} Bin`;
      select.append(binOption);
    }

    select.value = state.activeLocationId
      ? encodeLocationSelectValue(state.activeLocationId, state.activeLocationViewMode)
      : "";
  }

  async function handleSaveSettings() {
    readSettingsFromDom();
    setButtonBusy(dom.saveSettingsBtn, true);

    try {
      const stored = await persistence.saveSettings(toStoredProfile(state.profile));
      state.profile = syncProfileCredentialState(normalizeProfileDraft({
        ...state.profile,
        ...stored,
      }), state.credentials);
      writeSettingsToDom();
      renderProfileSummary();
      await refreshStatus();
      await refreshDebugLogState();

      const message = client.supportsNativeProfilePersistence
        ? "Settings saved. Activity debug and polling preferences are updated."
        : "Settings saved locally in the browser preview.";

      dom.settingsResult.textContent = message;
      toast(message, "success");
      addActivity("success", message);
    } finally {
      setButtonBusy(dom.saveSettingsBtn, false);
    }
  }

  async function handleCreateCredential() {
    const draft: CredentialDraft = {
      name: dom.credentialNameInput.value.trim(),
      accessKeyId: dom.credentialAccessKeyInput.value.trim(),
      secretAccessKey: dom.credentialSecretKeyInput.value.trim(),
    };

    if (!client.supportsNativeProfilePersistence) {
      const message = "Credential management is only available in the desktop app.";
      dom.credentialsResult.textContent = message;
      toast(message, "info");
      return;
    }

    if (!draft.name || !draft.accessKeyId || !draft.secretAccessKey) {
      const message = "Enter a name, access key ID, and secret access key to create a credential.";
      dom.credentialsResult.textContent = message;
      toast(message, "error");
      return;
    }

    setButtonBusy(dom.createCredentialBtn, true);

    try {
      const created = await client.createCredential(draft);
      dom.credentialNameInput.value = "";
      dom.credentialAccessKeyInput.value = "";
      dom.credentialSecretKeyInput.value = "";
      await refreshCredentials();
      state.profile = syncProfileCredentialState(normalizeProfileDraft({
        ...state.profile,
        credentialProfileId: created.id,
        selectedCredential: created,
      }), state.credentials);
      renderProfileSummary();

      const message = buildCredentialCreateMessage(created);
      dom.credentialsResult.textContent = `${message} It is now selected for this bucket.`;
      toast(`Created credential \"${created.name}\".`, "success");
      addActivity("success", `Created credential \"${created.name}\".`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `Create credential failed: ${message}`;
      dom.credentialsResult.textContent = surfacedMessage;
      toast(surfacedMessage, "error");
      addActivity("error", surfacedMessage);
    } finally {
      setButtonBusy(dom.createCredentialBtn, false);
    }
  }

  function renderLocationCredentialOptions() {
    const select = dom.locationCredentialSelect;
    const currentValue = select.value;
    select.innerHTML = "";

    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Choose a credential";
    select.append(defaultOption);

    for (const credential of state.credentials) {
      const option = document.createElement("option");
      option.value = credential.id;
      option.textContent = credential.name;
      select.append(option);
    }

    select.value = currentValue;
    select.disabled = state.credentials.length === 0;
  }

  function resetLocationForm() {
    dom.locationEditingId.value = "";
    dom.locationFormTitle.textContent = "Create sync location";
    dom.locationLabelInput.value = "";
    dom.locationLocalFolderInput.value = "";
    dom.locationRegionSelect.value = "";
    dom.locationBucketInput.value = "";
    dom.locationCredentialSelect.value = "";
    dom.locationObjectVersioningEnabledInput.checked = false;
    dom.locationEnabledInput.checked = true;
    dom.locationPollingInput.checked = true;
    dom.locationPollIntervalInput.value = "60";
    dom.locationConflictStrategySelect.value = state.profile.conflictStrategy;
    dom.locationRemoteBinEnabledInput.checked = true;
    dom.locationRemoteBinRetentionInput.value = String(DEFAULT_REMOTE_BIN_RETENTION_DAYS);
    renderLocationRemoteBinState();
    dom.cancelEditLocationBtn.hidden = true;

    const saveBtnLabel = dom.saveLocationBtn.querySelector("span");
    if (saveBtnLabel) saveBtnLabel.textContent = "Create sync location";
    const saveBtnIcon = dom.saveLocationBtn.querySelector("i");
    if (saveBtnIcon) saveBtnIcon.setAttribute("data-lucide", "plus");
    applyIcons();
  }

  function populateLocationForm(location: SyncLocation) {
    dom.locationEditingId.value = location.id;
    dom.locationFormTitle.textContent = "Edit sync location";
    dom.locationLabelInput.value = location.label;
    dom.locationLocalFolderInput.value = location.localFolder;
    dom.locationRegionSelect.value = location.region;
    dom.locationBucketInput.value = location.bucket;
    dom.locationCredentialSelect.value = location.credentialProfileId ?? "";
    dom.locationObjectVersioningEnabledInput.checked = location.objectVersioningEnabled;
    dom.locationEnabledInput.checked = location.enabled;
    dom.locationPollingInput.checked = location.remotePollingEnabled;
    dom.locationPollIntervalInput.value = String(location.pollIntervalSeconds);
    dom.locationConflictStrategySelect.value = location.conflictStrategy;
    dom.locationRemoteBinEnabledInput.checked = location.remoteBin.enabled;
    dom.locationRemoteBinRetentionInput.value = String(location.remoteBin.retentionDays);
    renderLocationRemoteBinState();
    dom.cancelEditLocationBtn.hidden = false;

    const saveBtnLabel = dom.saveLocationBtn.querySelector("span");
    if (saveBtnLabel) saveBtnLabel.textContent = "Update sync location";
    const saveBtnIcon = dom.saveLocationBtn.querySelector("i");
    if (saveBtnIcon) saveBtnIcon.setAttribute("data-lucide", "save");
    applyIcons();
  }

  function readLocationDraftFromForm(): SyncLocationDraft {
    const editingId = dom.locationEditingId.value.trim() || null;
    return {
      id: editingId,
      label: dom.locationLabelInput.value.trim(),
      localFolder: dom.locationLocalFolderInput.value.trim(),
      region: dom.locationRegionSelect.value,
      bucket: dom.locationBucketInput.value.trim(),
      credentialProfileId: dom.locationCredentialSelect.value || null,
      objectVersioningEnabled: dom.locationObjectVersioningEnabledInput.checked,
      enabled: dom.locationEnabledInput.checked,
      remotePollingEnabled: dom.locationPollingInput.checked,
      pollIntervalSeconds: Number(dom.locationPollIntervalInput.value) || 60,
      conflictStrategy: dom.locationConflictStrategySelect.value as SyncLocationDraft["conflictStrategy"],
      remoteBin: {
        enabled: dom.locationObjectVersioningEnabledInput.checked ? false : dom.locationRemoteBinEnabledInput.checked,
        retentionDays: parseRemoteBinRetentionDays(dom.locationRemoteBinRetentionInput.value),
      },
    };
  }

  function renderLocationsList() {
    dom.locationsList.innerHTML = "";

    const count = state.syncLocations.length;
    dom.locationsCountBadge.textContent = `${count} sync location${count !== 1 ? "s" : ""}`;

    dom.locationsEmptyState.hidden = count > 0;
    dom.locationsList.hidden = count === 0;

    for (const location of state.syncLocations) {
      const li = document.createElement("li");
      li.className = "credential-item";

      const meta = document.createElement("div");
      meta.className = "credential-item-meta";

      const name = document.createElement("strong");
      name.textContent = location.label || `${location.bucket}`;

      const hint = document.createElement("span");
      hint.className = "hint";
      const folder = location.localFolder || "No folder";
      const bucket = location.bucket || "No bucket";
      hint.textContent = `${folder} → ${bucket}`;

      meta.append(name, hint);

      const actions = document.createElement("div");
      actions.className = "credential-item-actions";

      const enabledBadge = document.createElement("span");
      enabledBadge.className = `badge ${location.enabled ? "success" : "default"}`;
      enabledBadge.textContent = location.enabled ? "enabled" : "paused";
      actions.append(enabledBadge);

      const remoteBinBadge = document.createElement("span");
      remoteBinBadge.className = `badge ${location.objectVersioningEnabled || location.remoteBin.enabled ? "success" : "default"}`;
      remoteBinBadge.textContent = location.objectVersioningEnabled
        ? "object versioning"
        : location.remoteBin.enabled
          ? `remote bin ${location.remoteBin.retentionDays}d`
          : "hard delete";
      actions.append(remoteBinBadge);

      const editButton = document.createElement("button");
      editButton.className = "secondary-btn slim-btn";
      editButton.type = "button";
      editButton.textContent = "Edit";
      editButton.addEventListener("click", () => {
        renderLocationCredentialOptions();
        populateLocationForm(location);
      });
      actions.append(editButton);

      const deleteButton = document.createElement("button");
      deleteButton.className = "secondary-btn slim-btn";
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.addEventListener("click", async () => {
        await asyncConfirm.open({
          title: "Delete sync location?",
          message: `"${location.label || location.bucket}" will be permanently deleted.`,
          acceptLabel: "Delete",
          rejectLabel: "Cancel",
          variant: "danger",
          onAccept: async () => {
            try {
              const updatedProfile = await client.removeSyncLocation(location.id);
              applySyncLocationState(updatedProfile.syncLocations, updatedProfile.activeLocationId ?? state.activeLocationId);
              renderLocationsList();
              renderLocationDropdown();
              resetLocationForm();

              const message = `Deleted sync location "${location.label || location.bucket}".`;
              dom.locationsResult.textContent = message;
              toast(message, "success");
              addActivity("info", message);
            } catch (error) {
              const message = error instanceof Error ? error.message : String(error);
              dom.locationsResult.textContent = `Delete failed: ${message}`;
              toast(`Delete failed: ${message}`, "error");
              addActivity("error", `Delete sync location failed: ${message}`);
              throw createHandledAsyncConfirmError(message);
            }
          },
        });
      });
      actions.append(deleteButton);

      li.append(meta, actions);
      dom.locationsList.append(li);
    }
  }

  async function handleSaveLocation() {
    const draft = readLocationDraftFromForm();
    const isEditing = Boolean(draft.id);

    if (!draft.localFolder || !draft.bucket) {
      const message = "Enter a local folder and bucket name to create a sync location.";
      dom.locationsResult.textContent = message;
      toast(message, "error");
      return;
    }

    setButtonBusy(dom.saveLocationBtn, true);

    try {
      const updatedProfile = isEditing
        ? await client.updateSyncLocation(draft)
        : await client.addSyncLocation(draft);

      applySyncLocationState(updatedProfile.syncLocations, updatedProfile.activeLocationId ?? state.activeLocationId);
      renderLocationsList();
      renderLocationDropdown();
      resetLocationForm();

      const action = isEditing ? "Updated" : "Created";
      const message = `${action} sync location "${draft.label || draft.bucket}".`;
      dom.locationsResult.textContent = message;
      toast(message, "success");
      addActivity("success", message);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `${isEditing ? "Update" : "Create"} sync location failed: ${message}`;
      dom.locationsResult.textContent = surfacedMessage;
      toast(surfacedMessage, "error");
      addActivity("error", surfacedMessage);
    } finally {
      setButtonBusy(dom.saveLocationBtn, false);
    }
  }

  async function refreshSyncLocations() {
    try {
      const listedLocations = await client.listSyncLocations();
      applySyncLocationState(
        mergeSyncLocationsWithStoredProfile(listedLocations),
        state.profile.activeLocationId ?? state.activeLocationId,
      );
    } catch (error) {
      applySyncLocationState(state.profile.syncLocations ?? [], state.profile.activeLocationId ?? state.activeLocationId);
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `Load sync locations failed: ${message}`;
      dom.locationsResult.textContent = surfacedMessage;
      addActivity("error", surfacedMessage);
    }
    renderLocationsList();
    renderLocationDropdown();
    renderStatus();
    renderFileTreeViewState();
    void refreshFileTree();
  }

  async function handleFileTreeChange(checkedPaths: string[]) {
    if (!state.activeLocationId || state.activeLocationViewMode === "bin") return;

    // Get current entries to know the full set of file paths
    const cachedEntries = getCurrentViewEntries();
    const entries: FileEntry[] = cachedEntries ?? await client.listFileEntries(state.activeLocationId);
    const checkedSet = new Set(checkedPaths);
    const mutableEntries = entries.filter((entry) => entry.kind === "file"
      && entry.status !== "conflict"
      && entry.status !== "review-required"
      && entry.status !== "glacier");

    // Files that are newly checked (want local copy) - were remote-only before
    const toDownload = mutableEntries
      .filter((e) => checkedSet.has(e.path) && !e.hasLocalCopy)
      .map((e) => e.path);

    // Files that are newly unchecked (remove local copy) - had local copy before
    const toRemove = mutableEntries
      .filter((e) => !checkedSet.has(e.path) && e.hasLocalCopy)
      .map((e) => e.path);

    try {
      if (toDownload.length > 0) {
        addActivity("info", `Downloading ${toDownload.length} file(s) to local storage...`);
        await client.toggleLocalCopy(state.activeLocationId, toDownload, true);
        addActivity("info", `Downloaded ${toDownload.length} file(s).`);
      }
      if (toRemove.length > 0) {
        addActivity("info", `Removing ${toRemove.length} local file(s)...`);
        await client.toggleLocalCopy(state.activeLocationId, toRemove, false);
        addActivity("info", `Removed ${toRemove.length} local file(s).`);
      }
    } catch (err) {
      addActivity("error", `Local copy toggle error: ${err}`);
      await refreshLocationViews(state.activeLocationId, { clearCache: true });
      return;
    }

    await refreshLocationViews(state.activeLocationId, { clearCache: true });
  }

  async function handleDelete(target: DeleteTarget) {
    if (target.kind === "directory") {
      await handleFolderDelete(target.path);
      return;
    }

    await handleFileDelete(target.path);
  }

  async function mutateBinEntries(options: {
    title: string;
    actionLabel: "restore" | "purge";
    entries: FileEntry[];
    location: SyncLocation;
    confirmMessage?: string;
    acceptLabel?: string;
    variant?: "danger";
    action: (entries: BinEntryRequest[]) => Promise<unknown>;
  }) {
    const run = async () => {
      const response = await options.action(options.entries.map(buildBinEntryRequest));
      const summary = isBinMutationSummary(response) ? response : null;
      const { successful, failed } = partitionBinMutationResults(options.entries, summary);
      const outcome = getBinMutationOutcomeMessage({
        action: options.actionLabel,
        location: options.location,
        requestedCount: options.entries.length,
        successCount: successful.length,
        failureCount: failed.length,
      });

      toast(outcome.toastMessage, outcome.toastVariant);
      addActivity(
        failed.length > 0 ? (successful.length > 0 ? "info" : "error") : "info",
        outcome.activityMessage,
        formatBinMutationFailureDetails(failed) ?? options.entries.map((entry) => entry.path).join("\n"),
      );

      if (failed.length === 0) {
        clearBinSelection();
      } else {
        selectedBinPaths = getSelectedBinPathsForEntries(
          options.entries.filter((entry) => failed.some((result) => result.path === entry.path && result.kind === entry.kind)),
        );
        renderBinToolbar();
      }

      await refreshLocationViews(options.location.id, { clearCache: true });
    };

    if (options.confirmMessage) {
      await asyncConfirm.open({
        title: options.title,
        message: options.confirmMessage,
        acceptLabel: options.acceptLabel ?? "Confirm",
        rejectLabel: "Cancel",
        variant: options.variant,
        onAccept: async () => {
          try {
            await run();
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            toast(message, "error");
            addActivity("error", message);
            throw createHandledAsyncConfirmError(message);
          }
        },
      });
      return;
    }

    try {
      await run();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      toast(message, "error");
      addActivity("error", message);
    }
  }

  async function handleFileDelete(path: string) {
    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    await asyncConfirm.open({
      title: "Delete file?",
      message: getDeleteConfirmationMessage(path, activeLocation),
      acceptLabel: "Delete",
      rejectLabel: "Cancel",
      variant: "danger",
      onAccept: async () => {
        try {
          await client.deleteFile(activeLocation.id, path);
          const toastMessage = activeLocation.objectVersioningEnabled
            ? "File deleted locally and marked deleted in S3 version history."
            : activeLocation.remoteBin.enabled
              ? "File deleted locally and moved to the remote bin."
              : "File permanently deleted.";
          const activityMessage = activeLocation.objectVersioningEnabled
            ? "Deleted file locally and added S3 delete marker"
            : activeLocation.remoteBin.enabled
              ? "Deleted file locally and moved remote object to remote bin"
              : "Permanently deleted file";
          toast(toastMessage, "success");
          addActivity("info", activityMessage, path);
          await refreshLocationViews(activeLocation.id, { clearCache: true });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          toast(`Failed to delete file: ${message}`, "error");
          addActivity("error", "Failed to delete file", String(error));
          throw createHandledAsyncConfirmError(message);
        }
      },
    });
  }

  async function handleFolderDelete(path: string) {
    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    await asyncConfirm.open({
      title: "Delete folder?",
      message: getFolderDeleteConfirmationMessage(path, activeLocation),
      acceptLabel: "Delete folder",
      rejectLabel: "Cancel",
      variant: "danger",
      onAccept: async () => {
        try {
          await client.deleteFolder(activeLocation.id, path);
          const toastMessage = activeLocation.objectVersioningEnabled
            ? "Folder deleted locally and marked deleted in S3 version history."
            : activeLocation.remoteBin.enabled
              ? "Folder deleted locally and moved to the remote bin."
              : "Folder permanently deleted.";
          const activityMessage = activeLocation.objectVersioningEnabled
            ? "Deleted folder locally and added S3 delete markers"
            : activeLocation.remoteBin.enabled
              ? "Deleted folder locally and moved remote subtree to remote bin"
              : "Permanently deleted folder";
          toast(toastMessage, "success");
          addActivity("info", activityMessage, path);
          await refreshLocationViews(activeLocation.id, { clearCache: true });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          toast(`Failed to delete folder: ${message}`, "error");
          addActivity("error", "Failed to delete folder", String(error));
          throw createHandledAsyncConfirmError(message);
        }
      },
    });
  }

  async function handleBinRestore(entry: FileEntry) {
    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    if (entry.binKey && entry.kind === "file") {
      try {
        await client.restoreBinEntry(activeLocation.id, entry.binKey);
        toast(`Restored ${entry.kind} from ${getBinSourceLabel(entry.deletedFrom)}.`, "success");
        addActivity("info", `Restored ${entry.kind} from ${getBinSourceLabel(entry.deletedFrom)}`, entry.path);
        clearBinSelection();
        await refreshLocationViews(activeLocation.id, { clearCache: true });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        toast(message, "error");
        addActivity("error", message);
      }
      return;
    }

    await mutateBinEntries({
      title: "Restore bin entry?",
      actionLabel: "restore",
      entries: [entry],
      location: activeLocation,
      action: (entries) => client.restoreBinEntries(activeLocation.id, entries),
    });
  }

  async function handleBulkBinRestore() {
    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;
    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    const entries = getSelectedBinEntries();
    if (entries.length === 0) {
      toast("Select at least one bin entry to restore.", "info");
      return;
    }

    await mutateBinEntries({
      title: "Restore selected bin entries?",
      actionLabel: "restore",
      entries,
      location: activeLocation,
      action: (requests) => client.restoreBinEntries(activeLocation.id, requests),
    });
  }

  async function handleBulkBinPurge() {
    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;
    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    const entries = getSelectedBinEntries();
    if (entries.length === 0) {
      toast("Select at least one bin entry to purge.", "info");
      return;
    }

    await mutateBinEntries({
      title: "Purge selected bin entries?",
      actionLabel: "purge",
      entries,
      location: activeLocation,
      confirmMessage: getBinPurgeConfirmationMessage(activeLocation, entries.length),
      acceptLabel: activeLocation.objectVersioningEnabled ? "Purge permanently" : "Delete permanently",
      variant: "danger",
      action: (requests) => client.purgeBinEntries(activeLocation.id, requests),
    });
  }

  async function compareConflictEntry(entry: FileEntry): Promise<ConflictResolutionDetails> {
    if (!state.activeLocationId) {
      throw new Error("No active sync location selected.");
    }

    const details = await client.prepareConflictComparison(state.activeLocationId, entry.path);
    if (details.mode !== "external") {
      addActivity("info", `Loaded inline ${details.mode} comparison`, entry.path);
      return details;
    }

    const openTasks: Promise<void>[] = [];

    if (details.localPath) {
      openTasks.push(client.openPath(details.localPath));
    }

    if (details.remoteTempPath) {
      openTasks.push(client.openPath(details.remoteTempPath));
    }

    if (openTasks.length === 0) {
      throw new Error("No compare targets were available for this conflict.");
    }

    await Promise.all(openTasks);
    addActivity("info", "Opened conflict comparison targets", entry.path);
    return details;
  }

  async function handleResolveConflict(entry: FileEntry) {
    if (!isResolvableConflictEntry(entry)) {
      toast("This MVP only resolves file-vs-file conflict or review-required entries.", "info");
      return;
    }

    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    conflictResolutionModal.open({
      locationLabel: activeLocation.label || activeLocation.bucket,
      entry,
      onCompare: async (currentEntry) => {
        const details = await compareConflictEntry(currentEntry);
        toast(
          details.mode === "external"
            ? "Opened local and remote versions for comparison."
            : details.mode === "image"
              ? "Loaded inline image comparison."
              : "Loaded inline text comparison.",
          "info",
        );
        return details;
      },
      onResolve: async (currentEntry, resolution) => {
        await client.resolveConflict(activeLocation.id, currentEntry.path, resolution);
        const entryLabel = currentEntry.status === "review-required"
          ? "Review cleared"
          : "Conflict resolved";
        const message = resolution === "keep-local"
          ? `${entryLabel} by keeping the local version.`
          : `${entryLabel} by keeping the remote version.`;
        toast(message, "success");
        addActivity("success", message, currentEntry.path);
        await refreshLocationViews(activeLocation.id, { clearCache: true });
      },
    });
  }

  async function handleReveal(path: string) {
    const activeLocation = state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    try {
      await client.revealTreeEntry(activeLocation.id, path);
      addActivity("info", "Revealed local path in file manager", path);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      toast(message, "error");
      addActivity("error", "Failed to reveal local path", message);
    }
  }

  async function handleStorageClassChange(path: string, currentStorageClass: string | null) {
    const isGlacier =
      currentStorageClass === "GLACIER_IR" ||
      currentStorageClass === "DEEP_ARCHIVE" ||
      currentStorageClass === "GLACIER";

    if (isGlacier) {
      // Currently in Glacier — offer to restore to Standard
      await asyncConfirm.open({
        title: "Restore from Glacier?",
        message: `"${path}" is currently in Glacier storage. Restore it to Standard storage? This will make the file available for syncing again.`,
        acceptLabel: "Restore to Standard",
        rejectLabel: "Cancel",
        onAccept: async () => {
          if (!state.activeLocationId) {
            toast("No active sync location selected.", "error");
            throw createHandledAsyncConfirmError("No active sync location selected.");
          }

          try {
            await client.changeStorageClass(state.activeLocationId, path, "STANDARD");
            toast("File restored to Standard storage.", "success");
            addActivity("info", "Restored file from Glacier", path);
            await refreshLocationViews(state.activeLocationId, { clearCache: true });
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            toast(`Failed to restore file: ${message}`, "error");
            addActivity("error", "Failed to restore file from Glacier", String(error));
            throw createHandledAsyncConfirmError(message);
          }
        },
      });
    } else {
      // Currently in Standard (or unknown) — offer to move to Glacier
      await asyncConfirm.open({
        title: "Move to Glacier storage?",
        message: `"${path}" will be moved to Amazon S3 Glacier Instant Retrieval. The local copy will not be available after this transition. The file will remain accessible on-demand from Glacier.`,
        acceptLabel: "Move to Glacier",
        rejectLabel: "Cancel",
        variant: "danger",
        onAccept: async () => {
          if (!state.activeLocationId) {
            toast("No active sync location selected.", "error");
            throw createHandledAsyncConfirmError("No active sync location selected.");
          }

          try {
            await client.changeStorageClass(state.activeLocationId, path, "GLACIER_IR");
            toast("File moved to Glacier storage.", "success");
            addActivity("info", "Moved file to Glacier storage", path);
            await refreshLocationViews(state.activeLocationId, { clearCache: true });
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            toast(`Failed to move file to Glacier: ${message}`, "error");
            addActivity("error", "Failed to move file to Glacier", String(error));
            throw createHandledAsyncConfirmError(message);
          }
        },
      });
    }
  }

  async function refreshFileTree() {
    if (fileTreeChangeTimer !== null) {
      clearTimeout(fileTreeChangeTimer);
      fileTreeChangeTimer = null;
    }

    if (!state.activeLocationId) {
      endFileTreeLoading();
      destroyFileTree();
      dom.fileTreeEmptyState.hidden = false;
      dom.fileTree.hidden = true;
      dom.fileTree.innerHTML = "";
      renderStatus();
      return;
    }

    const locationId = state.activeLocationId;
    const mode = state.activeLocationViewMode;
    if (mode !== "bin") {
      clearBinSelection();
    }
    const viewKey = getFileTreeViewKey(locationId, mode);
    const requestSequence = ++fileTreeRequestSequence;
    beginFileTreeLoading(requestSequence);

    try {
      const entries = mode === "bin"
        ? await client.listBinEntries(locationId)
        : await client.listFileEntries(locationId);

      if (requestSequence !== fileTreeRequestSequence) {
        return;
      }

      if (state.activeLocationId !== locationId || state.activeLocationViewMode !== mode) {
        return;
      }

      const entriesJson = JSON.stringify(entries);
      const cachedSnapshot = getViewSnapshot(viewKey);
      if (cachedSnapshot?.entriesJson === entriesJson && fileTreeHandle) {
        renderStatus();
        return;
      }

      fileTreeSnapshots.set(viewKey, { viewKey, entries, entriesJson });
      renderFileTreeEntries(entries, mode);
      renderStatus();
    } finally {
      endFileTreeLoading(requestSequence);
    }
  }

  dom.activeLocationSelect.addEventListener("change", () => {
    conflictResolutionModal.close();
    const selection = decodeLocationSelectValue(dom.activeLocationSelect.value);
    clearBinSelection();
    state.activeLocationId = selection.locationId;
    state.activeLocationViewMode = selection.mode;
    state.profile = { ...state.profile, activeLocationId: state.activeLocationId };
    void persistence.saveSettings(toStoredProfile(state.profile));
    renderProfileSummary();
    renderFileTreeViewState();
    const cachedSnapshot = getViewSnapshot();
    if (cachedSnapshot) {
      renderFileTreeEntries(cachedSnapshot.entries, state.activeLocationViewMode);
      renderStatus();
    }
    void refreshFileTree();
  });

  dom.createCredentialBtn.addEventListener("click", () => void handleCreateCredential());
  dom.restoreSelectedBtn.addEventListener("click", () => void handleBulkBinRestore());
  dom.purgeSelectedBtn.addEventListener("click", () => void handleBulkBinPurge());
  dom.saveSettingsBtn.addEventListener("click", () => void handleSaveSettings());
  dom.saveLocationBtn.addEventListener("click", () => void handleSaveLocation());
  dom.cancelEditLocationBtn.addEventListener("click", () => {
    resetLocationForm();
    dom.locationsResult.textContent = "Edit cancelled.";
  });
  dom.locationObjectVersioningEnabledInput.addEventListener("change", renderLocationRemoteBinState);
  dom.locationRemoteBinEnabledInput.addEventListener("change", renderLocationRemoteBinState);
  dom.locationRemoteBinRetentionInput.addEventListener("input", renderLocationRemoteBinState);
  resetLocationForm();
  dom.locationBrowseFolderBtn.addEventListener("click", async () => {
    const selected = await client.chooseLocalFolder();
    if (!selected) {
      toast("Folder picker is available in the desktop app.", "info");
      return;
    }
    dom.locationLocalFolderInput.value = selected;
  });
  dom.openDebugLogFolderBtn.addEventListener("click", async () => {
    await client.openActivityDebugLogFolder();
  });

  bindNavigation({
    root: dom.nav,
    onSelect: (id) => {
      switch (id) {
        case "nav-home": closeAllDialogs(); break;
        case "nav-credentials": openDialog("credentials"); break;
        case "nav-locations": openDialog("locations"); break;
        case "nav-activity": openDialog("activity"); break;
        case "nav-settings": openDialog("settings"); break;
      }
    },
  });

  const unlistenStatus = await client.listenSyncStatus((status) => {
    state.status = status;
    renderStatus();
    addActivity("info", `Status updated: ${describeSyncStatus(status).badgeLabel}. ${new Intl.NumberFormat().format(getSyncOverviewStats(status).inSync)} files are in sync.`);
    debouncedRefreshFileTree();
  });

  const unlistenActivity = await client.listenNativeActivity((event) => {
    addActivityItem(createNativeActivity(event));
  });

  const handleBeforeUnload = () => {
    void unlistenStatus();
    void unlistenActivity();
  };

  window.addEventListener("beforeunload", handleBeforeUnload);

  const storedProfile = await persistence.load();
  state.profile = applyStoredProfile(storedProfile);
  state.activeLocationId = storedProfile.activeLocationId ?? null;
  writeSettingsToDom();
  renderLocationRemoteBinState();
  renderFileTreeViewState();
  await refreshCredentials();
  await refreshStatus();
  await refreshDebugLogState();
  await refreshSyncLocations();

  dom.homeScreen.hidden = false;
  dom.navTriggerLabel.textContent = "Menu";
  dom.settingsResult.textContent = client.supportsNativeProfilePersistence
    ? "Settings changes stay local until you save them."
    : "Settings changes stay local to this browser preview after you save them.";
  dom.credentialsResult.textContent = client.supportsNativeProfilePersistence
    ? "Create named credentials here. The UI will show whether each one was saved and tested."
    : "Credential management is shown here for preview, but real credentials are desktop-only.";
  if (!dom.locationsResult.textContent?.trim()) {
    dom.locationsResult.textContent = client.supportsNativeProfilePersistence
      ? "Create sync locations to connect local folders to remote buckets."
      : "Sync location management is shown here for preview, but real sync locations are desktop-only.";
  }

  addActivity("info", client.supportsNativeProfilePersistence
    ? "Ready to connect a folder, bucket, and named credential."
    : "Browser preview loaded. Credential management and sync stay desktop-only here.");

  return () => {
    debouncedRefreshFileTree.cancel();
    debouncedRenderActivity.cancel();

    if (fileTreeChangeTimer !== null) {
      clearTimeout(fileTreeChangeTimer);
      fileTreeChangeTimer = null;
    }

    endFileTreeLoading();

    destroyFileTree();
    asyncConfirm.destroy();
    conflictResolutionModal.destroy();
  
    window.removeEventListener("beforeunload", handleBeforeUnload);
    void unlistenStatus();
    void unlistenActivity();
  };
}
