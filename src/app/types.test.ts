import { describe, expect, it } from "vitest";

import {
  capabilitiesFromProviderDefinition,
  defaultProviderCapabilities,
  defaultProviderDefinition,
  normalizeCredentialSummaryRecord,
  normalizeProvider,
} from "./types";

describe("provider capability defaults", () => {
  it("keeps the GCS provider definition aligned with backend advertised support", () => {
    const definition = defaultProviderDefinition("gcs");

    expect(definition.supportsObjectVersioning).toBe(true);
    expect(definition.supportsFileVersions).toBe(true);
    expect(definition.supportsRemoteBin).toBe(true);
    expect(definition.supportsStorageClass).toBe(true);
    expect(definition.supportsBucketLifecycle).toBe(true);
  });

  it("uses runtime capability defaults for GCS frontend gating", () => {
    const capabilities = defaultProviderCapabilities("gcs");

    expect(capabilities.objectVersioning).toEqual({
      status: "supported",
      message: null,
    });
    expect(capabilities.remoteBin).toEqual({
      status: "supported",
      message: null,
    });
    expect(capabilities.archiveStorage).toEqual({
      status: "supported",
      message: null,
    });
  });

  it("derives GCS runtime gating from provider metadata without flipping support off", () => {
    const definition = defaultProviderDefinition("gcs");
    const capabilities = capabilitiesFromProviderDefinition(definition, "gcs");

    expect(capabilities.objectVersioning.status).toBe("supported");
    expect(capabilities.archiveStorage.status).toBe("supported");
    expect(capabilities.remoteBin.status).toBe("supported");
  });

  it("normalizes the same practical GCS aliases as the backend", () => {
    expect(normalizeProvider("gcp")).toBe("gcs");
    expect(normalizeProvider("gcs")).toBe("gcs");
    expect(normalizeProvider("google-cloud-storage")).toBe("gcs");
    expect(normalizeProvider("Google Cloud Storage")).toBe("gcs");
  });

  it("advertises backend-aligned GCS aliases in the default definition", () => {
    const definition = defaultProviderDefinition("gcs");

    expect(definition.aliases).toEqual([
      "gcp",
      "google-cloud-storage",
      "google cloud storage",
    ]);
  });

  it("normalizes provider-aware credential summaries from native responses", () => {
    expect(normalizeCredentialSummaryRecord({
      id: "cred-aws",
      name: "AWS",
      provider: "aws",
      ready: true,
      validationStatus: "untested",
      lastTestedAt: null,
      lastTestMessage: null,
      summary: {
        accessKeyIdPreview: "AKIA12345678",
      },
    })).toEqual({
      id: "cred-aws",
      name: "AWS",
      provider: "aws",
      ready: true,
      validationStatus: "untested",
      lastTestedAt: null,
      lastTestMessage: null,
      summary: {
        accessKeyIdPreview: "••••5678",
      },
    });

    expect(normalizeCredentialSummaryRecord({
      id: "cred-gcs",
      name: "GCS",
      provider: "gcp",
      ready: true,
      validationStatus: "untested",
      lastTestedAt: null,
      lastTestMessage: null,
      summary: {
        client_email: "sync@example-project.iam.gserviceaccount.com",
        project_id: "example-project",
      },
    })).toEqual({
      id: "cred-gcs",
      name: "GCS",
      provider: "gcs",
      ready: true,
      validationStatus: "untested",
      lastTestedAt: null,
      lastTestMessage: null,
      summary: {
        clientEmail: "sync@example-project.iam.gserviceaccount.com",
        projectId: "example-project",
      },
    });
  });
});
