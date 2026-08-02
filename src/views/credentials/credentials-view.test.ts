import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  DEFAULT_PROFILE_DRAFT,
  normalizeProfileDraft,
  syncProfileCredentialState,
} from "../../app/profile";
import { normalizeProvider } from "../../app/types";
import type { CredentialDraft, CredentialSummary } from "../../app/types";
import { createAppStore, type AppState, type AppStore } from "../../state/app-state";
import {
  createCredentialsView,
  type ConfirmRequest,
  type CredentialsDom,
  type CredentialsViewDeps,
} from "./credentials-view";

function createDom(): CredentialsDom {
  const make = <T extends HTMLElement>(tag: string, init?: (el: T) => void): T => {
    const element = document.createElement(tag) as T;
    init?.(element);
    document.body.append(element);
    return element;
  };

  return {
    credentialsList: make<HTMLElement>("ul"),
    credentialsEmptyState: make<HTMLElement>("div"),
    credentialsCountBadge: make<HTMLElement>("span"),
    credentialsSupportBadge: make<HTMLElement>("span"),
    credentialsSupportText: make<HTMLElement>("p"),
    credentialsListStatus: make<HTMLElement>("p"),
    credentialsResult: make<HTMLElement>("div"),
    createCredentialBtn: make<HTMLButtonElement>("button"),
    credentialProviderSelect: make<HTMLSelectElement>("select", (el) => {
      for (const value of ["aws", "gcs"]) {
        const option = document.createElement("option");
        option.value = value;
        el.append(option);
      }
    }),
    credentialNameInput: make<HTMLInputElement>("input"),
    credentialAccessKeyInput: make<HTMLInputElement>("input"),
    credentialSecretKeyInput: make<HTMLInputElement>("input"),
    credentialServiceAccountInput: make<HTMLTextAreaElement>("textarea"),
  };
}

/**
 * An AWS credential summary.
 *
 * Built explicitly rather than by spreading a Partial: CredentialSummary is a
 * discriminated union, and spreading erases the discriminant.
 */
function credential(
  overrides: Partial<Extract<CredentialSummary, { provider: "aws" }>> = {},
): CredentialSummary {
  return {
    id: "cred-1",
    name: "Prod AWS",
    provider: "aws",
    ready: true,
    validationStatus: "passed",
    lastTestedAt: "2026-08-01T00:00:00Z",
    lastTestMessage: null,
    summary: null,
    ...overrides,
  };
}

function createState(credentials: CredentialSummary[] = []): AppState {
  return {
    activeDialog: null,
    activeLocationId: null,
    activeLocationViewMode: "live",
    profile: DEFAULT_PROFILE_DRAFT,
    providerDefinitions: [],
    credentials,
    syncLocations: [],
    status: { phase: "unconfigured" } as AppState["status"],
    activity: [],
    lastConnectAt: null,
    debugLogState: { enabled: false, logFilePath: null, logDirectoryPath: null },
  };
}

class TestHandledError extends Error {}

function harness(
  credentials: CredentialSummary[] = [],
  overrides: Partial<CredentialsViewDeps> = {},
) {
  const dom = createDom();
  const store: AppStore = createAppStore(createState(credentials));

  const createCredential = vi.fn(async (_draft: CredentialDraft): Promise<CredentialSummary> =>
    credential({ id: "new", name: "New" }),
  );
  const testCredential = vi.fn(async () => ({
    ok: true,
    bucketCount: 3,
    credential: credential({ validationStatus: "passed" }),
    permissions: null,
  }));
  const deleteCredential = vi.fn(async () => ({
    deleted: true,
    profile: DEFAULT_PROFILE_DRAFT,
  }));
  const refreshCredentials = vi.fn(async () => undefined);
  const confirm = vi.fn(async (request: ConfirmRequest): Promise<boolean> => {
    await request.onAccept();
    return true;
  });
  const onCredentialsChanged = vi.fn();
  const renderFormState = vi.fn();
  const toast = vi.fn();
  const addActivity = vi.fn();

  const deps: CredentialsViewDeps = {
    dom,
    store,
    supportsNativePersistence: true,
    createCredential,
    testCredential,
    deleteCredential,
    refreshCredentials,
    confirm,
    handledError: (message) => new TestHandledError(message),
    isHandledError: (error) => error instanceof TestHandledError,
    onCredentialsChanged,
    normalizeProvider,
    normalizeProfileDraft,
    syncProfileCredentialState,
    renderFormState,
    toast,
    addActivity,
    ...overrides,
  } as CredentialsViewDeps;

  const view = createCredentialsView(deps);
  return {
    dom,
    store,
    view,
    createCredential,
    testCredential,
    deleteCredential,
    refreshCredentials,
    confirm,
    onCredentialsChanged,
    renderFormState,
    toast,
    addActivity,
  };
}

beforeEach(() => {
  document.body.innerHTML = "";
});

describe("createCredentialsView", () => {
  it("shows the empty state with no credentials", () => {
    const { dom } = harness();

    expect(dom.credentialsEmptyState.hidden).toBe(false);
    expect(dom.credentialsList.hidden).toBe(true);
    expect(dom.credentialsCountBadge.textContent).toBe("0 saved");
  });

  it("renders a row per credential with its provider, storage and test state", () => {
    // Three independent facts: which cloud, is the secret safe, does it work.
    // Collapsing any of them hides a state the user has to act on.
    const { dom } = harness([credential({ name: "Prod AWS", provider: "aws" })]);

    expect(dom.credentialsList.children).toHaveLength(1);
    const hint = dom.credentialsList.querySelector(".hint");
    expect(hint?.textContent).toContain("AWS");
    expect(dom.credentialsList.querySelectorAll(".badge").length).toBeGreaterThanOrEqual(2);
  });

  it("marks the credential the profile has selected", () => {
    const { dom, store } = harness([credential({ id: "cred-1" })]);
    store.setState({
      profile: normalizeProfileDraft({ ...DEFAULT_PROFILE_DRAFT, credentialProfileId: "cred-1" }),
    });

    const badges = [...dom.credentialsList.querySelectorAll(".badge")].map((b) => b.textContent);
    expect(badges).toContain("selected");
  });

  it("disables actions in the browser preview", () => {
    const { dom } = harness([credential()], { supportsNativePersistence: false });

    expect(dom.createCredentialBtn.disabled).toBe(true);
    const buttons = [...dom.credentialsList.querySelectorAll("button")];
    expect(buttons.every((button) => button.disabled)).toBe(true);
    expect(dom.credentialsSupportBadge.textContent).toBe("Preview only");
  });

  it("does not leak listeners when the list re-renders", () => {
    // Every row builds two buttons with their own handlers. Rebuilding into a
    // fresh fragment is what stops them accumulating on every state change.
    const { dom, store, testCredential } = harness([credential()]);

    for (let i = 0; i < 5; i += 1) {
      store.setState({ credentials: [credential({ name: `Rename ${i}` })] });
    }

    const testButton = dom.credentialsList.querySelector("button");
    testButton?.dispatchEvent(new MouseEvent("click"));

    expect(testCredential).toHaveBeenCalledTimes(1);
  });

  it("tests a credential and reports the permission summary", async () => {
    const { dom, testCredential, toast } = harness([credential()]);

    const testButton = dom.credentialsList.querySelector("button")!;
    testButton.click();

    await vi.waitFor(() => {
      expect(dom.credentialsResult.textContent).toMatch(/test passed/);
    });
    expect(testCredential).toHaveBeenCalledTimes(1);
    expect(dom.credentialsResult.textContent).toContain("3 bucket(s)");
    expect(toast).toHaveBeenCalledWith(expect.stringMatching(/test passed/), "success");
  });

  it("reports a failed test rather than swallowing it", async () => {
    const testCredential = vi.fn(async () => {
      throw new Error("network down");
    });
    const { dom, toast } = harness([credential()], { testCredential });

    dom.credentialsList.querySelector("button")!.click();

    await vi.waitFor(() => {
      expect(dom.credentialsResult.textContent).toMatch(/test failed/);
    });
    expect(dom.credentialsResult.textContent).toContain("network down");
    expect(toast).toHaveBeenCalledWith(expect.stringMatching(/network down/), "error");
  });

  it("asks for confirmation before deleting, and warns when it is the selected one", async () => {
    const { dom, store, confirm } = harness([credential({ id: "cred-1", name: "Prod" })]);
    store.setState({
      profile: normalizeProfileDraft({ ...DEFAULT_PROFILE_DRAFT, credentialProfileId: "cred-1" }),
    });

    const buttons = [...dom.credentialsList.querySelectorAll("button")];
    buttons[buttons.length - 1].dispatchEvent(new MouseEvent("click"));

    await vi.waitFor(() => {
      expect(confirm).toHaveBeenCalled();
    });
    const [request] = confirm.mock.calls[0];
    expect(request.variant).toBe("danger");
    // The consequence, not just the action: this bucket stops syncing.
    expect(request.message).toMatch(/need a different credential/);
  });

  it("refreshes the list after a successful delete", async () => {
    const { dom, refreshCredentials } = harness([credential()]);

    const buttons = [...dom.credentialsList.querySelectorAll("button")];
    buttons[buttons.length - 1].dispatchEvent(new MouseEvent("click"));

    await vi.waitFor(() => {
      expect(refreshCredentials).toHaveBeenCalledTimes(1);
    });
  });

  it("reports a refused delete without claiming it succeeded", async () => {
    const deleteCredential = vi.fn(async () => ({
      deleted: false,
      profile: DEFAULT_PROFILE_DRAFT,
    }));
    const { dom, refreshCredentials } = harness([credential()], { deleteCredential });

    const buttons = [...dom.credentialsList.querySelectorAll("button")];
    buttons[buttons.length - 1].dispatchEvent(new MouseEvent("click"));

    await vi.waitFor(() => {
      expect(dom.credentialsResult.textContent).toMatch(/Could not delete/);
    });
    expect(refreshCredentials).not.toHaveBeenCalled();
  });

  it("refuses an incomplete AWS draft with a message naming the missing fields", async () => {
    const { dom, createCredential, toast } = harness();

    dom.credentialProviderSelect.value = "aws";
    dom.credentialNameInput.value = "Prod";
    dom.createCredentialBtn.click();

    await vi.waitFor(() => {
      expect(toast).toHaveBeenCalled();
    });
    expect(createCredential).not.toHaveBeenCalled();
    expect(dom.credentialsResult.textContent).toMatch(/access key ID, and secret access key/);
  });

  it("refuses an incomplete GCS draft with provider-specific wording", async () => {
    const { dom, createCredential } = harness();

    dom.credentialProviderSelect.value = "gcs";
    dom.credentialNameInput.value = "Prod";
    dom.createCredentialBtn.click();

    await vi.waitFor(() => {
      expect(dom.credentialsResult.textContent).toMatch(/service account/);
    });
    expect(createCredential).not.toHaveBeenCalled();
  });

  it("creates a credential and clears the form", async () => {
    const { dom, createCredential } = harness();

    dom.credentialProviderSelect.value = "aws";
    dom.credentialNameInput.value = "Prod";
    dom.credentialAccessKeyInput.value = "AKIA123";
    dom.credentialSecretKeyInput.value = "secret";
    dom.createCredentialBtn.click();

    await vi.waitFor(() => {
      expect(createCredential).toHaveBeenCalledTimes(1);
    });
    expect(createCredential.mock.calls[0][0]).toMatchObject({
      name: "Prod",
      provider: "aws",
      accessKeyId: "AKIA123",
    });
    await vi.waitFor(() => {
      expect(dom.credentialNameInput.value).toBe("");
    });
  });

  it("keeps the form filled when creation fails", async () => {
    // Re-pasting a service account JSON blob because the network blipped is a
    // genuinely miserable thing to ask of someone.
    const createCredential = vi.fn(async () => {
      throw new Error("keyring locked");
    });
    const { dom } = harness([], { createCredential });

    dom.credentialProviderSelect.value = "gcs";
    dom.credentialNameInput.value = "Prod";
    dom.credentialServiceAccountInput.value = '{"type":"service_account"}';
    dom.createCredentialBtn.click();

    await vi.waitFor(() => {
      expect(dom.credentialsResult.textContent).toMatch(/keyring locked/);
    });
    expect(dom.credentialServiceAccountInput.value).toBe('{"type":"service_account"}');
    expect(dom.credentialNameInput.value).toBe("Prod");
  });

  it("re-renders the provider-specific fields when the provider changes", () => {
    const { dom, renderFormState } = harness();
    renderFormState.mockClear();

    dom.credentialProviderSelect.value = "gcs";
    dom.credentialProviderSelect.dispatchEvent(new Event("change"));

    expect(renderFormState).toHaveBeenCalledWith("gcs");
  });

  it("stops responding after destroy", () => {
    const { dom, store, view, createCredential } = harness([credential()]);
    view.destroy();

    dom.createCredentialBtn.click();
    store.setState({ credentials: [] });

    expect(createCredential).not.toHaveBeenCalled();
    // The last render before teardown is still on screen; nothing new is drawn.
    expect(dom.credentialsList.children).toHaveLength(1);
  });
});
