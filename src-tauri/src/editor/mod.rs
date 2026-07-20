//! External-editor round-trip for remote files.
//!
//! `open_in_external_editor` downloads a remote file to a temp path, opens it in
//! VS Code, and starts a background task that polls the temp file. Whenever the
//! user saves (the content hash changes), the file is re-uploaded to the remote
//! host. The sync loop runs until `close_external_editor_session` is called (or
//! the app exits — stale temp dirs are swept on the next startup, see `lib.rs`).
//!
//! Polling (instead of the `notify` crate) is deliberate: editors such as VS Code
//! save atomically via write-then-rename, which frequently confuses inode-based
//! FS watchers. Reading the file by path once per second is simple, dependency
//! free, and robust to that pattern.

use crate::connection_manager::ConnectionManager;
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tauri::{AppHandle, Emitter, State};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Poll interval for detecting saves in the temp file.
const POLL_INTERVAL: Duration = Duration::from_millis(1000);

/// After a failed upload, wait this many poll ticks before retrying the *same*
/// unchanged content — avoids hammering the network (and re-toasting) once a
/// second during an outage while still recovering automatically.
const ERROR_BACKOFF_TICKS: u32 = 4;

/// Registry of active edit sessions, managed by Tauri.
#[derive(Default)]
pub struct EditSessionRegistry {
    sessions: Mutex<HashMap<String, EditSession>>,
    /// Monotonic counter so each open gets a unique temp dir even when the
    /// (deterministic) session id is reused across re-opens.
    next_instance: AtomicU64,
}

struct EditSession {
    connection_id: String,
    remote_path: String,
    file_name: String,
    local_path: PathBuf,
    temp_dir: PathBuf,
    cancel: CancellationToken,
}

#[derive(Serialize, Clone)]
pub struct EditSessionInfo {
    session_id: String,
    connection_id: String,
    remote_path: String,
    file_name: String,
    local_path: String,
}

#[derive(Serialize, Clone)]
struct SyncEvent {
    session_id: String,
    file_name: String,
    remote_path: String,
    bytes: u64,
}

#[derive(Serialize, Clone)]
struct SyncErrorEvent {
    session_id: String,
    file_name: String,
    error: String,
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

/// Deterministic session id from (connection_id, remote_path) so re-opening the
/// same remote file dedups to the existing session instead of spawning a second.
fn session_id_for(connection_id: &str, remote_path: &str) -> String {
    let mut hasher = DefaultHasher::new();
    connection_id.hash(&mut hasher);
    b"\0".hash(&mut hasher);
    remote_path.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

/// Keep only the basename so a crafted name can't escape the temp dir.
fn sanitize_file_name(name: &str) -> String {
    let base = name.rsplit(['/', '\\']).next().unwrap_or(name).trim();
    if base.is_empty() {
        "untitled".to_string()
    } else {
        base.to_string()
    }
}

/// Download a remote file to a local path, dispatching by protocol.
/// Mirrors `commands::download_remote_file` but works on a plain `&ConnectionManager`
/// so the background sync task can reuse it.
async fn download_to_path(
    cm: &ConnectionManager,
    connection_id: &str,
    remote_path: &str,
    local_path: &str,
) -> Result<u64, String> {
    let conn_type = cm.get_connection_type(connection_id).await;
    let result = match conn_type.as_deref() {
        Some("SFTP") => {
            let sftp_map = cm.get_sftp_connection().await;
            let connections = sftp_map.read().await;
            let client = connections
                .get(connection_id)
                .ok_or_else(|| "SFTP connection not found".to_string())?;
            client.download_file(remote_path, local_path).await
        }
        Some("FTP") => {
            let ftp_map = cm.get_ftp_connection().await;
            let mut connections = ftp_map.write().await;
            let client = connections
                .get_mut(connection_id)
                .ok_or_else(|| "FTP connection not found".to_string())?;
            client.download_file(remote_path, local_path).await
        }
        Some(other) => return Err(format!("Unsupported protocol: {}", other)),
        None => {
            let connection = cm
                .get_connection(connection_id)
                .await
                .ok_or_else(|| format!("No connection found for '{}'", connection_id))?;
            let client = connection.read().await;
            client.download_file(remote_path, local_path).await
        }
    };
    result.map_err(|e| e.to_string())
}

/// Upload a local file to a remote path, dispatching by protocol.
async fn upload_from_path(
    cm: &ConnectionManager,
    connection_id: &str,
    local_path: &str,
    remote_path: &str,
) -> Result<u64, String> {
    let conn_type = cm.get_connection_type(connection_id).await;
    let result = match conn_type.as_deref() {
        Some("SFTP") => {
            let sftp_map = cm.get_sftp_connection().await;
            let connections = sftp_map.read().await;
            let client = connections
                .get(connection_id)
                .ok_or_else(|| "SFTP connection not found".to_string())?;
            client.upload_file(local_path, remote_path).await
        }
        Some("FTP") => {
            let ftp_map = cm.get_ftp_connection().await;
            let mut connections = ftp_map.write().await;
            let client = connections
                .get_mut(connection_id)
                .ok_or_else(|| "FTP connection not found".to_string())?;
            client.upload_file(local_path, remote_path).await
        }
        Some(other) => return Err(format!("Unsupported protocol: {}", other)),
        None => {
            let connection = cm
                .get_connection(connection_id)
                .await
                .ok_or_else(|| format!("No connection found for '{}'", connection_id))?;
            let client = connection.read().await;
            client.upload_file(local_path, remote_path).await
        }
    };
    result.map_err(|e| e.to_string())
}

/// Launch VS Code on `local_path` (non-blocking). Tries, in order: the macOS VS
/// Code CLI (works even when `code` is not on PATH, which is common for
/// GUI-launched apps), then `code` on PATH, then `open -a "Visual Studio Code"`.
fn spawn_vscode(local_path: &Path) -> Result<(), String> {
    use std::process::Command;

    const MAC_VSCODE_CLI: &str =
        "/Applications/Visual Studio Code.app/Contents/Resources/app/bin/code";

    if Path::new(MAC_VSCODE_CLI).exists() {
        return Command::new(MAC_VSCODE_CLI)
            .arg(local_path)
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("Failed to launch VS Code: {}", e));
    }

    if Command::new("code").arg(local_path).spawn().is_ok() {
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .arg("-a")
            .arg("Visual Studio Code")
            .arg(local_path)
            .spawn()
            .map(|_| ())
            .map_err(|e| format!("Failed to open in VS Code: {}", e))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Err("VS Code executable not found. Ensure `code` is on your PATH.".to_string())
    }
}

/// Background loop: poll the temp file and re-upload whenever it changes.
#[allow(clippy::too_many_arguments)]
async fn run_sync_loop(
    app: AppHandle,
    cm: Arc<ConnectionManager>,
    cancel: CancellationToken,
    connection_id: String,
    remote_path: String,
    file_name: String,
    local_path: PathBuf,
    temp_dir: PathBuf,
    session_id: String,
    initial_hash: u64,
) {
    // Hash of the content last successfully uploaded (or the initial download).
    let mut last_synced = initial_hash;
    // Hash of content whose upload is currently failing, plus a backoff counter.
    let mut failing: Option<u64> = None;
    let mut backoff: u32 = 0;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(POLL_INTERVAL) => {}
        }

        let bytes = match tokio::fs::read(&local_path).await {
            Ok(b) => b,
            // File may be momentarily absent during an atomic save; retry next tick.
            Err(_) => continue,
        };
        let current = hash_bytes(&bytes);

        if current == last_synced {
            // Nothing new since the last successful sync.
            failing = None;
            backoff = 0;
            continue;
        }

        // Content differs from the last successful sync. If this exact content is
        // already known to be failing, wait out the backoff before retrying.
        if failing == Some(current) && backoff > 0 {
            backoff -= 1;
            continue;
        }

        let lp = local_path.to_string_lossy().to_string();
        match upload_from_path(&cm, &connection_id, &lp, &remote_path).await {
            Ok(n) => {
                last_synced = current;
                failing = None;
                backoff = 0;
                tracing::info!("editor sync: re-uploaded {} ({} bytes)", remote_path, n);
                let _ = app.emit(
                    "editor-sync",
                    SyncEvent {
                        session_id: session_id.clone(),
                        file_name: file_name.clone(),
                        remote_path: remote_path.clone(),
                        bytes: n,
                    },
                );
            }
            Err(e) => {
                let is_new_failure = failing != Some(current);
                failing = Some(current);
                backoff = ERROR_BACKOFF_TICKS;
                tracing::warn!("editor sync failed for {}: {}", remote_path, e);
                // Only surface an error toast on the transition into a new failing
                // state — not once per second for the same unchanged content.
                if is_new_failure {
                    let _ = app.emit(
                        "editor-sync-error",
                        SyncErrorEvent {
                            session_id: session_id.clone(),
                            file_name: file_name.clone(),
                            error: e,
                        },
                    );
                }
            }
        }
    }

    // Each session owns a UNIQUE temp dir, so cleaning it up here cannot affect a
    // different (e.g. re-opened) session.
    let _ = tokio::fs::remove_dir_all(&temp_dir).await;
    tracing::info!("editor sync loop ended for {}", remote_path);
}

/// Open a remote file in VS Code and keep it synced: every save is uploaded back
/// to the server until the session is closed. Returns the session id.
#[tauri::command]
pub async fn open_in_external_editor(
    app: AppHandle,
    state: State<'_, Arc<ConnectionManager>>,
    registry: State<'_, EditSessionRegistry>,
    connection_id: String,
    remote_path: String,
    file_name: String,
) -> Result<String, String> {
    let session_id = session_id_for(&connection_id, &remote_path);
    let cancel = CancellationToken::new();

    // Unique temp layout per open: <tmp>/r-shell-edit/<session_id>-<instance>/<file>
    let instance = registry.next_instance.fetch_add(1, Ordering::Relaxed);
    let safe_name = sanitize_file_name(&file_name);
    let temp_dir = std::env::temp_dir()
        .join("r-shell-edit")
        .join(format!("{}-{}", session_id, instance));
    let local_path = temp_dir.join(&safe_name);

    // Atomic check-and-reserve: hold the lock across both so two concurrent opens
    // of the same file can't each spawn a task.
    {
        let mut sessions = registry.sessions.lock().await;
        if let Some(existing) = sessions.get(&session_id) {
            let local = existing.local_path.clone();
            drop(sessions);
            // Re-focus in VS Code, but only once the file actually exists (a
            // concurrent open may still be downloading it).
            if local.exists() {
                spawn_vscode(&local)?;
            }
            return Ok(session_id);
        }
        sessions.insert(
            session_id.clone(),
            EditSession {
                connection_id: connection_id.clone(),
                remote_path: remote_path.clone(),
                file_name: file_name.clone(),
                local_path: local_path.clone(),
                temp_dir: temp_dir.clone(),
                cancel: cancel.clone(),
            },
        );
    }

    let cm = state.inner().clone();

    // Setup: create temp dir, download, open in VS Code. On any failure, roll back
    // the reservation and remove the temp dir so nothing leaks.
    let setup: Result<u64, String> = async {
        tokio::fs::create_dir_all(&temp_dir)
            .await
            .map_err(|e| format!("Failed to create temp dir: {}", e))?;
        let local_path_str = local_path.to_string_lossy().to_string();
        download_to_path(&cm, &connection_id, &remote_path, &local_path_str).await?;
        let initial = tokio::fs::read(&local_path)
            .await
            .map_err(|e| format!("Failed to read downloaded file: {}", e))?;
        spawn_vscode(&local_path)?;
        Ok(hash_bytes(&initial))
    }
    .await;

    let initial_hash = match setup {
        Ok(h) => h,
        Err(e) => {
            let mut sessions = registry.sessions.lock().await;
            sessions.remove(&session_id);
            drop(sessions);
            let _ = tokio::fs::remove_dir_all(&temp_dir).await;
            return Err(e);
        }
    };

    // If the session was closed while we were downloading, don't start syncing.
    if cancel.is_cancelled() {
        let _ = tokio::fs::remove_dir_all(&temp_dir).await;
        return Ok(session_id);
    }

    tokio::spawn(run_sync_loop(
        app,
        cm,
        cancel,
        connection_id,
        remote_path,
        file_name,
        local_path,
        temp_dir,
        session_id.clone(),
        initial_hash,
    ));

    Ok(session_id)
}

/// Stop syncing a session: cancel its watcher (which removes its temp dir on exit).
#[tauri::command]
pub async fn close_external_editor_session(
    registry: State<'_, EditSessionRegistry>,
    session_id: String,
) -> Result<(), String> {
    let mut sessions = registry.sessions.lock().await;
    if let Some(session) = sessions.remove(&session_id) {
        session.cancel.cancel();
    }
    Ok(())
}

/// List active edit sessions (for a "syncing…" UI affordance / stop button).
#[tauri::command]
pub async fn list_external_editor_sessions(
    registry: State<'_, EditSessionRegistry>,
) -> Result<Vec<EditSessionInfo>, String> {
    let sessions = registry.sessions.lock().await;
    Ok(sessions
        .iter()
        .map(|(id, s)| EditSessionInfo {
            session_id: id.clone(),
            connection_id: s.connection_id.clone(),
            remote_path: s.remote_path.clone(),
            file_name: s.file_name.clone(),
            local_path: s.local_path.to_string_lossy().to_string(),
        })
        .collect())
}
