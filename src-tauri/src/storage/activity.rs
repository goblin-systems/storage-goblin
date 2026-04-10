use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    process::Command,
    sync::Mutex,
};

use serde::Serialize;
use tauri::{AppHandle, Emitter, Manager};
use time::macros::format_description;

use super::{now_iso, profile_store::read_profile_from_disk};

const ACTIVITY_EVENT_NAME: &str = "storage://activity";
const DEBUG_LOG_FILE_NAME: &str = "activity-debug.log";
const MAX_DEBUG_LOG_LINES: usize = 2000;
const LOG_TIMESTAMP_FORMAT: &[time::format_description::FormatItem<'static>] =
    format_description!("[year]-[month]-[day]T[hour]:[minute]:[second]");

pub struct ActivityDebugState {
    write_lock: Mutex<()>,
}

impl Default for ActivityDebugState {
    fn default() -> Self {
        Self {
            write_lock: Mutex::new(()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ActivityLevel {
    Info,
    Success,
    Error,
}

impl ActivityLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Success => "success",
            Self::Error => "error",
        }
    }

    fn log_label(self) -> &'static str {
        match self {
            Self::Info => "INFO",
            Self::Success => "SUCCESS",
            Self::Error => "ERROR",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NativeActivityEvent {
    pub timestamp: String,
    pub level: String,
    pub message: String,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ActivityDebugLogState {
    pub enabled: bool,
    pub log_file_path: String,
    pub log_directory_path: String,
}

#[tauri::command]
pub fn get_activity_debug_log_state(app: AppHandle) -> Result<ActivityDebugLogState, String> {
    let log_file_path = resolve_log_file_path(&app)?;
    let log_directory_path = log_file_path
        .parent()
        .ok_or_else(|| "failed to resolve activity debug log directory".to_string())?
        .to_path_buf();

    Ok(ActivityDebugLogState {
        enabled: is_debug_mode_enabled(&app),
        log_file_path: log_file_path.to_string_lossy().to_string(),
        log_directory_path: log_directory_path.to_string_lossy().to_string(),
    })
}

#[tauri::command]
pub fn open_activity_debug_log_folder(app: AppHandle) -> Result<(), String> {
    let log_file_path = resolve_log_file_path(&app)?;
    let dir = log_file_path
        .parent()
        .ok_or_else(|| "failed to resolve activity debug log directory".to_string())?;

    #[cfg(target_os = "windows")]
    let mut command = {
        let mut cmd = Command::new("explorer");
        cmd.arg(dir);
        cmd
    };

    #[cfg(target_os = "macos")]
    let mut command = {
        let mut cmd = Command::new("open");
        cmd.arg(dir);
        cmd
    };

    #[cfg(all(unix, not(target_os = "macos")))]
    let mut command = {
        let mut cmd = Command::new("xdg-open");
        cmd.arg(dir);
        cmd
    };

    command
        .spawn()
        .map_err(|error| format!("failed to open activity debug log directory: {error}"))?;

    Ok(())
}

pub(crate) fn emit_activity(
    app: &AppHandle,
    state: &ActivityDebugState,
    level: ActivityLevel,
    message: impl Into<String>,
    details: Option<String>,
) {
    let message = message.into();
    let event = build_activity_event(level, message.clone(), details);

    let _ = app.emit(ACTIVITY_EVENT_NAME, &event);

    if let Err(error) = write_debug_log_entry(app, state, level, &message, event.details.as_deref())
    {
        eprintln!("[ERROR] failed to write activity debug log entry: {error}");
    }
}

fn build_activity_event(
    level: ActivityLevel,
    message: String,
    details: Option<String>,
) -> NativeActivityEvent {
    NativeActivityEvent {
        timestamp: now_iso(),
        level: level.as_str().to_string(),
        message,
        details: details
            .map(sanitize_log_text)
            .filter(|value| !value.is_empty()),
    }
}

fn is_debug_mode_enabled(app: &AppHandle) -> bool {
    read_profile_from_disk(app)
        .map(|profile| profile.activity_debug_mode_enabled)
        .unwrap_or(false)
}

fn resolve_log_file_path(app: &AppHandle) -> Result<PathBuf, String> {
    let dir = app
        .path()
        .app_local_data_dir()
        .map_err(|error| format!("failed to resolve activity debug log dir: {error}"))?;
    fs::create_dir_all(&dir)
        .map_err(|error| format!("failed to create activity debug log dir: {error}"))?;
    Ok(dir.join(DEBUG_LOG_FILE_NAME))
}

fn write_debug_log_entry(
    app: &AppHandle,
    state: &ActivityDebugState,
    level: ActivityLevel,
    message: &str,
    details: Option<&str>,
) -> Result<(), String> {
    if !is_debug_mode_enabled(app) {
        return Ok(());
    }

    let path = resolve_log_file_path(app)?;
    let _guard = state
        .write_lock
        .lock()
        .map_err(|_| "failed to lock activity debug log writer".to_string())?;

    append_log_line(&path, level, message, details)?;
    prune_log_lines(&path)
}

fn append_log_line(
    path: &PathBuf,
    level: ActivityLevel,
    message: &str,
    details: Option<&str>,
) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|error| format!("failed to open activity debug log file: {error}"))?;

    let timestamp = time::OffsetDateTime::now_utc()
        .format(LOG_TIMESTAMP_FORMAT)
        .map_err(|error| format!("failed to format activity debug log timestamp: {error}"))?;

    match details {
        Some(details) if !details.is_empty() => writeln!(
            file,
            "{} [{}] {} | {}",
            timestamp,
            level.log_label(),
            sanitize_log_text(message),
            sanitize_log_text(details)
        )
        .map_err(|error| format!("failed to write activity debug log entry: {error}")),
        _ => writeln!(
            file,
            "{} [{}] {}",
            timestamp,
            level.log_label(),
            sanitize_log_text(message)
        )
        .map_err(|error| format!("failed to write activity debug log entry: {error}")),
    }
}

fn prune_log_lines(path: &PathBuf) -> Result<(), String> {
    let content = fs::read_to_string(path)
        .map_err(|error| format!("failed to read activity debug log for pruning: {error}"))?;
    let lines: Vec<&str> = content.lines().collect();
    if lines.len() <= MAX_DEBUG_LOG_LINES {
        return Ok(());
    }

    let keep_from = lines.len().saturating_sub(MAX_DEBUG_LOG_LINES);
    let mut trimmed = lines[keep_from..].join("\n");
    trimmed.push('\n');
    fs::write(path, trimmed)
        .map_err(|error| format!("failed to prune activity debug log file: {error}"))
}

fn sanitize_log_text(value: impl AsRef<str>) -> String {
    value
        .as_ref()
        .replace(['\r', '\n'], " ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{build_activity_event, sanitize_log_text, ActivityLevel};

    #[test]
    fn sanitize_log_text_flattens_multiline_whitespace() {
        assert_eq!(sanitize_log_text(" one\n two\r\n three "), "one two three");
    }

    #[test]
    fn build_activity_event_keeps_sanitized_details() {
        let event = build_activity_event(
            ActivityLevel::Error,
            "Failure".into(),
            Some(" line one\n line two ".into()),
        );

        assert_eq!(event.details.as_deref(), Some("line one line two"));
    }

    #[test]
    fn build_activity_event_drops_empty_details_after_sanitizing() {
        let event =
            build_activity_event(ActivityLevel::Info, "Message".into(), Some("  \n  ".into()));

        assert!(event.details.is_none());
    }
}
