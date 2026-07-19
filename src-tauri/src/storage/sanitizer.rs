use regex::Regex;
use serde_json::Value;
use std::sync::OnceLock;

const REDACTED: &str = "[redacted]";

pub fn sanitize_sensitive_text(value: impl AsRef<str>) -> String {
    let raw = value.as_ref();

    if let Ok(mut json) = serde_json::from_str::<Value>(raw) {
        redact_json_value(&mut json);
        if let Ok(serialized) = serde_json::to_string(&json) {
            return serialized;
        }
    }

    let mut text = raw.to_string();

    for regex in secret_patterns() {
        text = regex.replace_all(&text, REDACTED).into_owned();
    }

    text
}

fn secret_patterns() -> &'static [Regex] {
    static PATTERNS: OnceLock<Vec<Regex>> = OnceLock::new();
    PATTERNS.get_or_init(|| {
        [
            r#"(?i)(\"(?:accessKeyId|secretAccessKey|sessionToken|authorization|privateKey|private_key|clientEmail|client_email|tokenUri|token_uri|serviceAccountJson|service_account_json|secret_access_key|access_key_id)\"\s*:\s*\")([^\"]+)(\")"#,
            r#"(?i)('(?:accessKeyId|secretAccessKey|sessionToken|authorization|privateKey|private_key|clientEmail|client_email|tokenUri|token_uri|serviceAccountJson|service_account_json|secret_access_key|access_key_id)'\s*:\s*')([^']+)(')"#,
            r#"(?i)\b(accessKeyId|secretAccessKey|sessionToken|authorization|privateKey|private_key|clientEmail|client_email|tokenUri|token_uri|secret_access_key|access_key_id)\s*[=:]\s*([^\s,;]+)"#,
            r#"-----BEGIN PRIVATE KEY-----[\s\S]*?-----END PRIVATE KEY-----"#,
        ]
        .into_iter()
        .map(|pattern| Regex::new(pattern).expect("secret redaction regex should compile"))
        .collect()
    })
}

fn redact_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                if is_sensitive_key(key) && !entry.is_null() {
                    *entry = Value::String(REDACTED.into());
                } else {
                    redact_json_value(entry);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_json_value(item);
            }
        }
        _ => {}
    }
}

fn is_sensitive_key(key: &str) -> bool {
    matches!(
        key.trim().to_lowercase().as_str(),
        "accesskeyid"
            | "secretaccesskey"
            | "sessiontoken"
            | "authorization"
            | "privatekey"
            | "private_key"
            | "clientemail"
            | "client_email"
            | "tokenuri"
            | "token_uri"
            | "serviceaccountjson"
            | "service_account_json"
            | "secret_access_key"
            | "access_key_id"
    )
}

#[cfg(test)]
mod tests {
    use super::sanitize_sensitive_text;

    #[test]
    fn redacts_key_value_pairs() {
        let text = sanitize_sensitive_text(
            "accessKeyId=AKIA123456 secret_access_key=super-secret client_email=test@example.com",
        );

        assert!(!text.contains("AKIA123456"));
        assert!(!text.contains("super-secret"));
        assert!(!text.contains("test@example.com"));
        assert!(text.contains("[redacted]"));
    }

    #[test]
    fn redacts_json_payloads() {
        let text = sanitize_sensitive_text(
            r#"{"private_key":"secret","client_email":"person@example.com","nested":{"authorization":"token"}}"#,
        );

        assert_eq!(
            text,
            r#"{"client_email":"[redacted]","nested":{"authorization":"[redacted]"},"private_key":"[redacted]"}"#
        );
    }

    #[test]
    fn redacts_pem_blocks() {
        let text =
            sanitize_sensitive_text("-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----");

        assert_eq!(text, "[redacted]");
    }
}
