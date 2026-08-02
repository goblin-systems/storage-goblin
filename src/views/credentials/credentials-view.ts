/**
 * The credentials screen: list, create, test, delete (backlog phase 4.2).
 *
 * The third view out of the closure and the first with per-row actions, which
 * is what makes it a real test of the contract: every row builds two buttons
 * with their own async handlers, and a naive extraction leaks a listener per
 * row on every re-render.
 *
 * It does not: the list is rebuilt wholesale into a detached fragment and
 * swapped in, so the old rows (and their listeners) are dropped with the nodes
 * they were attached to.
 */

import {
  buildCredentialCreateMessage,
  buildCredentialTestContext,
  describeCredentialSummary,
  getCredentialStorageBadgeLabel,
  getCredentialStorageBadgeTone,
  getCredentialStorageLabel,
  getCredentialTestActionLabel,
  getCredentialValidationBadgeTone,
  getCredentialValidationLabel,
  getSelectedCredentialContextLabel,
  formatPermissionSummary,
} from "../../app/credential-labels";
import { getProviderLabel } from "../../app/types";
import type {
  CredentialDraft,
  CredentialSummary,
  CredentialTestResult,
  DeleteCredentialResult,
  Provider,
  StorageProfileDraft,
} from "../../app/types";
import { setButtonBusy } from "../../lib/dom";
import type { AppStore } from "../../state/app-state";

export interface CredentialsDom {
  credentialsList: HTMLElement;
  credentialsEmptyState: HTMLElement;
  credentialsCountBadge: HTMLElement;
  credentialsSupportBadge: HTMLElement;
  credentialsSupportText: HTMLElement;
  credentialsListStatus: HTMLElement;
  credentialsResult: HTMLElement;
  createCredentialBtn: HTMLButtonElement;
  credentialProviderSelect: HTMLSelectElement;
  credentialNameInput: HTMLInputElement;
  credentialAccessKeyInput: HTMLInputElement;
  credentialSecretKeyInput: HTMLInputElement;
  credentialServiceAccountInput: HTMLTextAreaElement;
}

/** Confirmation prompt, supplied by the shell. */
export interface ConfirmRequest {
  title: string;
  message: string;
  acceptLabel: string;
  rejectLabel: string;
  /** `danger` styles the accept button as destructive. */
  variant?: "danger";
  onAccept: () => Promise<void>;
}

export interface CredentialsViewDeps {
  dom: CredentialsDom;
  store: AppStore;
  supportsNativePersistence: boolean;
  createCredential(draft: CredentialDraft): Promise<CredentialSummary>;
  testCredential(request: {
    credentialId: string;
    context: ReturnType<typeof buildCredentialTestContext>;
  }): Promise<CredentialTestResult>;
  deleteCredential(credentialId: string): Promise<DeleteCredentialResult>;
  /** Reload the credential list from the backend. */
  refreshCredentials(): Promise<void>;
  confirm(request: ConfirmRequest): Promise<unknown>;
  /** Wraps an error so the confirm dialog knows it was already reported. */
  handledError(message: string): Error;
  isHandledError(error: unknown): boolean;
  /** Re-render anything outside this screen that a credential change affects. */
  onCredentialsChanged(): void;
  normalizeProvider(value: string): Provider;
  normalizeProfileDraft(profile: StorageProfileDraft): StorageProfileDraft;
  syncProfileCredentialState(
    profile: StorageProfileDraft,
    credentials: CredentialSummary[],
  ): StorageProfileDraft;
  /** Show/hide the provider-specific credential fields. */
  renderFormState(provider: Provider): void;
  toast(message: string, variant: "success" | "error" | "info"): void;
  addActivity(level: "success" | "error" | "info", message: string): void;
}

export interface CredentialsView {
  destroy(): void;
}

export function createCredentialsView(deps: CredentialsViewDeps): CredentialsView {
  const { dom, store } = deps;
  const cleanups: (() => void)[] = [];

  function on<K extends keyof HTMLElementEventMap>(
    element: HTMLElement,
    type: K,
    handler: (event: HTMLElementEventMap[K]) => void,
  ): void {
    element.addEventListener(type, handler);
    cleanups.push(() => {
      element.removeEventListener(type, handler);
    });
  }

  function report(
    message: string,
    variant: "success" | "error" | "info",
    level: "success" | "error" | "info" = variant,
  ): void {
    dom.credentialsResult.textContent = message;
    deps.toast(message, variant);
    deps.addActivity(level, message);
  }

  function describeRow(credential: CredentialSummary, profile: StorageProfileDraft): string {
    const summaryText = describeCredentialSummary(credential);
    // provider · storage state · test state · summary — three independent
    // facts a user needs separately: which cloud, is the secret safe, does it
    // actually work.
    const base = [
      getProviderLabel(credential.provider),
      getCredentialStorageLabel(credential),
      getCredentialValidationLabel(credential),
      ...(summaryText ? [summaryText] : []),
    ].join(" · ");
    // The selected credential says so first: it is the one whose failure
    // explains why a location is not syncing.
    return credential.id === profile.credentialProfileId
      ? `${getSelectedCredentialContextLabel(profile)} · ${base}`
      : base;
  }

  async function runTest(credential: CredentialSummary, button: HTMLButtonElement): Promise<void> {
    setButtonBusy(button, true);
    try {
      const result = await deps.testCredential({
        credentialId: credential.id,
        context: buildCredentialTestContext(store.getState().profile),
      });

      store.setState((current) => {
        const known = current.credentials.some((item) => item.id === result.credential.id);
        const credentials = known
          ? current.credentials.map((item) =>
              item.id === result.credential.id ? result.credential : item,
            )
          : [...current.credentials, result.credential];

        return {
          credentials,
          profile: deps.syncProfileCredentialState(
            deps.normalizeProfileDraft({
              ...current.profile,
              selectedCredential:
                current.profile.credentialProfileId === result.credential.id
                  ? result.credential
                  : current.profile.selectedCredential,
            }),
            credentials,
          ),
        };
      });
      deps.onCredentialsChanged();

      const outcome = result.ok
        ? `Credential "${result.credential.name}" test passed. Can access ${result.bucketCount} bucket(s).`
        : `Credential "${result.credential.name}" test failed.`;
      const permissionLine = formatPermissionSummary(result.permissions);
      report(
        permissionLine ? `${outcome} ${permissionLine}` : outcome,
        result.ok ? "success" : "error",
      );
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      report(`Credential test failed: ${detail}`, "error");
    } finally {
      setButtonBusy(button, false);
    }
  }

  function requestDelete(credential: CredentialSummary): void {
    const wasSelected = credential.id === store.getState().profile.credentialProfileId;

    void deps.confirm({
      title: "Delete credential?",
      message: wasSelected
        ? `"${credential.name}" will be deleted. This bucket will need a different credential before it can sync again.`
        : `"${credential.name}" will be permanently deleted.`,
      acceptLabel: "Delete",
      rejectLabel: "Cancel",
      variant: "danger",
      onAccept: async () => {
        try {
          const result = await deps.deleteCredential(credential.id);
          if (!result.deleted) {
            const message = deps.supportsNativePersistence
              ? `Could not delete credential "${credential.name}".`
              : "Credential deletion is only available in the desktop app.";
            dom.credentialsResult.textContent = message;
            deps.toast(message, "info");
            throw deps.handledError(message);
          }

          if (wasSelected) {
            store.setState((current) => ({
              profile: deps.syncProfileCredentialState(
                deps.normalizeProfileDraft({
                  ...current.profile,
                  credentialProfileId: result.profile.credentialProfileId,
                  selectedCredential: result.profile.selectedCredential,
                  selectedCredentialAvailable: result.profile.selectedCredentialAvailable,
                  credentialsStoredSecurely: result.profile.credentialsStoredSecurely,
                }),
                current.credentials.filter((item) => item.id !== credential.id),
              ),
            }));
          }

          report(`Deleted credential "${credential.name}".`, "success", "info");
          await deps.refreshCredentials();
          deps.onCredentialsChanged();
        } catch (error) {
          // Already reported: re-throw so the dialog stays closed without
          // double-reporting the same failure.
          if (deps.isHandledError(error)) throw error;

          const detail = error instanceof Error ? error.message : String(error);
          const message = `Delete credential failed: ${detail}`;
          report(message, "error");
          throw deps.handledError(message);
        }
      },
    });
  }

  function buildRow(credential: CredentialSummary, profile: StorageProfileDraft): HTMLLIElement {
    const li = document.createElement("li");
    li.className = "credential-item";

    const meta = document.createElement("div");
    meta.className = "credential-item-meta";

    const name = document.createElement("strong");
    name.textContent = credential.name;

    const hint = document.createElement("span");
    hint.className = "hint";
    hint.textContent = describeRow(credential, profile);

    meta.append(name, hint);

    const actions = document.createElement("div");
    actions.className = "credential-item-actions";

    const storageBadge = document.createElement("span");
    storageBadge.className = `badge ${getCredentialStorageBadgeTone(credential)}`;
    storageBadge.textContent = getCredentialStorageBadgeLabel(credential);

    const validationBadge = document.createElement("span");
    validationBadge.className = `badge ${getCredentialValidationBadgeTone(credential)}`;
    validationBadge.textContent = getCredentialValidationLabel(credential);

    actions.append(storageBadge, validationBadge);

    if (credential.id === profile.credentialProfileId) {
      const selectedBadge = document.createElement("span");
      selectedBadge.className = "badge default";
      selectedBadge.textContent = "selected";
      actions.append(selectedBadge);
    }

    const testButton = document.createElement("button");
    testButton.className = "secondary-btn slim-btn";
    testButton.type = "button";
    testButton.textContent = getCredentialTestActionLabel(credential);
    testButton.disabled = !deps.supportsNativePersistence;
    testButton.title = deps.supportsNativePersistence
      ? ""
      : "Credential testing is only available in the desktop app.";
    // Not registered in `cleanups`: these listeners die with their row when the
    // list is replaced, and tracking them would grow without bound.
    testButton.addEventListener("click", () => void runTest(credential, testButton));

    const deleteButton = document.createElement("button");
    deleteButton.className = "secondary-btn slim-btn";
    deleteButton.type = "button";
    deleteButton.textContent = "Delete";
    deleteButton.disabled = !deps.supportsNativePersistence;
    deleteButton.addEventListener("click", () => {
      requestDelete(credential);
    });

    actions.append(testButton, deleteButton);
    li.append(meta, actions);
    return li;
  }

  function render(): void {
    const { credentials, profile } = store.getState();
    const count = credentials.length;

    dom.credentialsCountBadge.textContent = `${count} saved`;
    dom.credentialsSupportBadge.textContent = deps.supportsNativePersistence
      ? "Desktop app"
      : "Preview only";
    dom.credentialsSupportBadge.className = `badge ${
      deps.supportsNativePersistence ? "success" : "default"
    }`;
    dom.credentialsSupportText.textContent = deps.supportsNativePersistence
      ? "Create provider-specific named credentials once, then reuse them across sync locations without re-entering secrets."
      : "Browser preview shows the credential workflow but does not create or store real credentials.";
    dom.createCredentialBtn.disabled = !deps.supportsNativePersistence;
    deps.renderFormState(deps.normalizeProvider(dom.credentialProviderSelect.value));

    dom.credentialsListStatus.textContent =
      count > 0
        ? "Saved credentials show secure storage state and test state separately."
        : deps.supportsNativePersistence
          ? "Create your first named credential, then assign it to a sync location."
          : "Open the desktop app to create and manage credentials.";

    dom.credentialsEmptyState.hidden = count > 0;
    dom.credentialsList.hidden = count === 0;

    const fragment = document.createDocumentFragment();
    for (const credential of credentials) fragment.append(buildRow(credential, profile));
    dom.credentialsList.replaceChildren(fragment);
  }

  function readDraft(): CredentialDraft {
    const provider = deps.normalizeProvider(dom.credentialProviderSelect.value);
    const name = dom.credentialNameInput.value.trim();

    return provider === "aws"
      ? {
          name,
          provider: "aws",
          accessKeyId: dom.credentialAccessKeyInput.value.trim(),
          secretAccessKey: dom.credentialSecretKeyInput.value.trim(),
        }
      : {
          name,
          provider: "gcs",
          credential: {
            kind: "gcsServiceAccount",
            serviceAccountJson: dom.credentialServiceAccountInput.value.trim(),
          },
        };
  }

  /** The reason this draft cannot be submitted, or null. */
  function validationError(draft: CredentialDraft): string | null {
    if (draft.provider === "aws") {
      return !draft.name || !draft.accessKeyId || !draft.secretAccessKey
        ? "Enter a name, access key ID, and secret access key to create an AWS credential."
        : null;
    }
    return !draft.name || !draft.credential.serviceAccountJson
      ? "Enter a name and paste the full service account JSON to create a GCS credential."
      : null;
  }

  function clearForm(): void {
    dom.credentialNameInput.value = "";
    dom.credentialAccessKeyInput.value = "";
    dom.credentialSecretKeyInput.value = "";
    dom.credentialServiceAccountInput.value = "";
  }

  async function create(): Promise<void> {
    if (!deps.supportsNativePersistence) {
      const message = "Credential management is only available in the desktop app.";
      dom.credentialsResult.textContent = message;
      deps.toast(message, "info");
      return;
    }

    const draft = readDraft();
    const invalid = validationError(draft);
    if (invalid) {
      dom.credentialsResult.textContent = invalid;
      deps.toast(invalid, "error");
      return;
    }

    setButtonBusy(dom.createCredentialBtn, true);
    try {
      const created = await deps.createCredential(draft);
      // Cleared only after the call succeeded, so a failure does not make the
      // user re-enter a service account JSON blob they just pasted.
      clearForm();
      await deps.refreshCredentials();

      store.setState((current) => ({
        profile: deps.syncProfileCredentialState(
          deps.normalizeProfileDraft({
            ...current.profile,
            provider: created.provider,
            credentialProfileId: created.id,
            selectedCredential: created,
          }),
          current.credentials,
        ),
      }));
      deps.onCredentialsChanged();

      dom.credentialsResult.textContent = `${buildCredentialCreateMessage(created)} It is now selected for this setup.`;
      deps.toast(`Created credential "${created.name}".`, "success");
      deps.addActivity("success", `Created credential "${created.name}".`);
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      report(`Create credential failed: ${detail}`, "error");
    } finally {
      setButtonBusy(dom.createCredentialBtn, false);
    }
  }

  on(dom.createCredentialBtn, "click", () => void create());
  on(dom.credentialProviderSelect, "change", () => {
    deps.renderFormState(deps.normalizeProvider(dom.credentialProviderSelect.value));
  });

  cleanups.push(store.select((state) => state.credentials, render));
  cleanups.push(store.select((state) => state.profile, render));
  render();

  return {
    destroy() {
      for (const cleanup of cleanups.splice(0)) cleanup();
    },
  };
}
