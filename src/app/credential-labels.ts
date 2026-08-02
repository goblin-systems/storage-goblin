/**
 * How a credential is described to the user (backlog phase 4.2).
 *
 * Pure label and tone functions, lifted out of the bootstrap module scope so
 * the credentials view can use them without importing the app — and so the
 * wording is testable directly, which matters because these strings are what
 * a user relies on to tell "saved but never tested" from "tested and broken".
 */

import type {
  CredentialSummary,
  CredentialTestContext,
  PermissionProbeSummary,
  Provider,
  StorageProfileDraft,
} from "./types";

/** The provider a profile will actually authenticate with. */
export function getEffectiveProfileProvider(profile: StorageProfileDraft): Provider {
  // The selected credential wins: it is what the request will be signed with,
  // regardless of what the profile's own provider field still says.
  return profile.selectedCredential?.provider ?? profile.provider;
}

export function describeCredentialSummary(credential: CredentialSummary): string | null {
  if (credential.provider === "aws") {
    return credential.summary?.accessKeyIdPreview ?? null;
  }

  return credential.summary?.clientEmail ?? credential.summary?.projectId ?? null;
}

export function getSelectedCredentialContextLabel(profile: StorageProfileDraft): string {
  return profile.bucket ? `Selected for bucket "${profile.bucket}"` : "Selected for current setup";
}

export function getCredentialValidationLabel(credential: CredentialSummary): string {
  switch (credential.validationStatus) {
    case "passed":
      return "test passed";
    case "failed":
      return "test failed";
    case "untested":
      return "untested";
  }
}

export function getCredentialTestActionLabel(credential: CredentialSummary): string {
  return credential.validationStatus === "untested" ? "Test" : "Re-test";
}

export function getCredentialStorageLabel(credential: CredentialSummary): string {
  return credential.ready ? "stored securely" : "stored secret missing";
}

export function getCredentialStorageBadgeLabel(credential: CredentialSummary): string {
  return credential.ready ? "stored" : "needs repair";
}

export function getCredentialStorageBadgeTone(credential: CredentialSummary): "success" | "danger" {
  return credential.ready ? "success" : "danger";
}

export function getCredentialValidationBadgeTone(
  credential: CredentialSummary,
): "success" | "danger" | "default" {
  return credential.validationStatus === "passed"
    ? "success"
    : credential.validationStatus === "failed"
      ? "danger"
      : "default";
}

export function formatPermissionSummary(permissions: PermissionProbeSummary | null): string {
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

export function buildCredentialTestContext(profile: StorageProfileDraft): CredentialTestContext {
  return {
    provider: getEffectiveProfileProvider(profile),
    region: profile.region,
    bucket: profile.bucket,
  };
}

export function buildCredentialCreateMessage(credential: CredentialSummary): string {
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
