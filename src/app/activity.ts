import type { ActivityItem, NativeActivityEvent } from "./types";

const REDACTED_VALUE = "[redacted]";

function redactKnownAssignment(text: string): string {
  return text
    .replace(/(access[_-]?key(?:[_-]?id)?\s*[:=]\s*)([^\s,;]+)/gi, `$1${REDACTED_VALUE}`)
    .replace(/(secret[_-]?access[_-]?key\s*[:=]\s*)([^\s,;]+)/gi, `$1${REDACTED_VALUE}`)
    .replace(/(session[_-]?token\s*[:=]\s*)([^\s,;]+)/gi, `$1${REDACTED_VALUE}`)
    .replace(/(authorization\s*[:=]\s*)([^\r\n]+)/gi, `$1${REDACTED_VALUE}`)
    .replace(/(bearer\s+)([^\s]+)/gi, `$1${REDACTED_VALUE}`);
}

function redactQuotedSecrets(text: string): string {
  return text
    .replace(/("(?:accessKeyId|secretAccessKey|sessionToken|authorization)"\s*:\s*")([^"]+)(")/gi, `$1${REDACTED_VALUE}$3`)
    .replace(/('(?:accessKeyId|secretAccessKey|sessionToken|authorization)'\s*:\s*')([^']+)(')/gi, `$1${REDACTED_VALUE}$3`);
}

function redactAwsStyleKey(text: string): string {
  return text.replace(/\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/g, REDACTED_VALUE);
}

export function sanitizeActivityText(text: string | null | undefined): string | null {
  if (!text) return null;
  return redactAwsStyleKey(redactQuotedSecrets(redactKnownAssignment(text)));
}

export function createUiActivity(level: ActivityItem["level"], message: string, details?: string | null): ActivityItem {
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: new Date().toISOString(),
    level,
    message: sanitizeActivityText(message) ?? "",
    details: sanitizeActivityText(details),
    source: "ui",
  };
}

export function createNativeActivity(event: NativeActivityEvent): ActivityItem {
  return {
    id: `${event.timestamp}-${Math.random().toString(36).slice(2, 8)}`,
    timestamp: event.timestamp,
    level: event.level,
    message: sanitizeActivityText(event.message) ?? "",
    details: sanitizeActivityText(event.details),
    source: "native",
  };
}
