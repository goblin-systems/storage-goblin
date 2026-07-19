import { describe, expect, it } from "vitest";
import { createNativeActivity, sanitizeActivityText } from "./activity";

describe("activity helpers", () => {
  it("redacts likely credential values from activity text", () => {
    expect(
      sanitizeActivityText("accessKeyId=AKIA1234567890ABCDEF secret_access_key=topsecret"),
    ).toBe("accessKeyId=[redacted] secret_access_key=[redacted]");
  });

  it("redacts bearer tokens and aws-style keys", () => {
    expect(sanitizeActivityText("Authorization: Bearer abc123 token AKIA1234567890ABCDEF")).toBe(
      "Authorization: [redacted]",
    );
  });

  it("maps native activity details into sanitized activity items", () => {
    const item = createNativeActivity({
      timestamp: "2026-04-04T10:30:00.000Z",
      level: "info",
      message: "Connected with accessKeyId=AKIA1234567890ABCDEF",
      details: '{"secretAccessKey":"super-secret"}',
    });

    expect(item.source).toBe("native");
    expect(item.message).toBe("Connected with accessKeyId=[redacted]");
    expect(item.details).toBe('{"secretAccessKey":"[redacted]"}');
  });

  it("redacts GCS service account fields in assignments and JSON payloads", () => {
    expect(
      sanitizeActivityText(
        "client_email=sync@example.iam.gserviceaccount.com private_key=-----BEGIN PRIVATE KEY-----abc",
      ),
    ).toBe("client_email=[redacted] private_key=[redacted]");
    expect(
      sanitizeActivityText(
        '{"client_email":"sync@example.iam.gserviceaccount.com","private_key":"super-secret","private_key_id":"abc123"}',
      ),
    ).toBe(
      '{"client_email":"[redacted]","private_key":"[redacted]","private_key_id":"[redacted]"}',
    );
  });
});
