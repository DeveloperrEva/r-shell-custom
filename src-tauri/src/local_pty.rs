//! Local PTY: spawn the user's own shell and expose it as a `PtySession`, so the
//! existing WebSocket streaming + xterm terminal component drive it exactly like
//! an SSH PTY. The only difference from `ssh::create_pty_session` is the transport
//! (a local shell process via `portable-pty` instead of an SSH channel).
//!
//! `portable-pty`'s reader/writer are BLOCKING (`std::io::Read`/`Write`), so we
//! bridge them to the same tokio `mpsc` channels the connection manager expects
//! using dedicated OS threads (never `spawn_blocking`, which would pin a
//! tokio blocking-pool thread for the lifetime of the session).

use crate::ssh::PtySession;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use std::io::{Read, Write};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// How often the output reader re-checks the cancel token while idle (ms).
const OUTPUT_POLL_TIMEOUT_MS: i32 = 200;

/// Pick the user's login shell: `$SHELL`, else the first of zsh/bash/sh that exists.
fn pick_shell() -> String {
    if let Ok(sh) = std::env::var("SHELL") {
        if !sh.trim().is_empty() {
            return sh;
        }
    }
    for candidate in ["/bin/zsh", "/bin/bash", "/bin/sh"] {
        if std::path::Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }
    "/bin/sh".to_string()
}

/// Resolve the starting directory for a new local shell. Prefers `requested`
/// (a directory a restored tab was last in) when it exists and is a directory;
/// otherwise falls back to the user's home. Returns `None` only if neither is
/// available, in which case the shell inherits the app's cwd.
fn resolve_start_dir(requested: Option<String>) -> Option<std::path::PathBuf> {
    if let Some(dir) = requested {
        let p = std::path::PathBuf::from(&dir);
        if p.is_dir() {
            return Some(p);
        }
        tracing::warn!("Requested local shell cwd {:?} is not a directory; using home", dir);
    }
    dirs::home_dir()
}

/// Spawn a local shell in a PTY and return a `PtySession` wired to it. Channel
/// capacities match the SSH path (input 1000, output 128 for back-pressure parity,
/// resize 16). `cwd` is the directory to start the shell in (a restored tab's last
/// directory); when `None` or invalid, the shell starts in the user's home.
pub fn create_local_pty_session(
    cols: u32,
    rows: u32,
    cwd: Option<String>,
) -> anyhow::Result<PtySession> {
    let pair = native_pty_system().openpty(PtySize {
        rows: rows as u16,
        cols: cols as u16,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let mut cmd = CommandBuilder::new(pick_shell());
    cmd.env("TERM", "xterm-256color"); // parity with the SSH PTY
    if let Some(dir) = resolve_start_dir(cwd) {
        cmd.cwd(dir);
    }

    let child = pair.slave.spawn_command(cmd)?;
    // Capture the shell's pid so we can later read its working directory
    // (see `read_process_cwd`) and persist it for restore.
    let shell_pid = child.process_id();
    // Dropping the slave is REQUIRED: otherwise the master read never sees EOF when
    // the shell exits, and the output thread would leak.
    drop(pair.slave);

    let writer = pair.master.take_writer()?;
    // Raw master fd (Unix) so the output reader can be woken via poll() + cancel,
    // guaranteeing the thread is reclaimed on teardown even if EOF never arrives.
    #[cfg(unix)]
    let master_raw_fd = pair.master.as_raw_fd();
    #[cfg(not(unix))]
    let fallback_reader = pair.master.try_clone_reader()?;
    let master = pair.master; // kept for resize + implicit cleanup

    let (input_tx, mut input_rx) = mpsc::channel::<Vec<u8>>(1000);
    let (output_tx, output_rx) = mpsc::channel::<Vec<u8>>(128);
    let (resize_tx, mut resize_rx) = mpsc::channel::<(u32, u32)>(16);
    let cancel = CancellationToken::new();

    // (A) OUTPUT: master read -> output_tx (dedicated OS thread).
    #[cfg(unix)]
    {
        use std::os::unix::io::FromRawFd;
        let raw = master_raw_fd.ok_or_else(|| anyhow::anyhow!("PTY master has no raw fd"))?;
        let cancel_out = cancel.clone();
        std::thread::spawn(move || {
            // Independent dup of the master fd so we own it and close it on exit.
            // Kept BLOCKING; poll() with a timeout lets us re-check the cancel token
            // so the thread + fd are reclaimed on teardown even if a backgrounded job
            // keeps the slave open and EOF never comes.
            let dup_fd = unsafe { libc::dup(raw) };
            if dup_fd < 0 {
                return;
            }
            let mut file = unsafe { std::fs::File::from_raw_fd(dup_fd) };
            let mut buf = [0u8; 8192];
            loop {
                if cancel_out.is_cancelled() {
                    break;
                }
                let mut pfd = libc::pollfd {
                    fd: dup_fd,
                    events: libc::POLLIN,
                    revents: 0,
                };
                let ready = unsafe { libc::poll(&mut pfd, 1, OUTPUT_POLL_TIMEOUT_MS) };
                if ready < 0 {
                    // EINTR: retry; any other error: give up.
                    if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    break;
                }
                if ready == 0 {
                    continue; // timeout -> re-check cancel
                }
                match file.read(&mut buf) {
                    Ok(0) => break, // EOF (slave fully closed)
                    Ok(n) => {
                        if output_tx.blocking_send(buf[..n].to_vec()).is_err() {
                            break; // receiver dropped -> session torn down
                        }
                    }
                    Err(_) => break,
                }
            }
            // file drops here -> dup_fd closed
        });
    }
    #[cfg(not(unix))]
    {
        let mut reader = fallback_reader;
        std::thread::spawn(move || {
            let mut buf = [0u8; 8192];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        if output_tx.blocking_send(buf[..n].to_vec()).is_err() {
                            break;
                        }
                    }
                }
            }
        });
    }

    // (B) INPUT: input_rx -> blocking master write (dedicated OS thread).
    {
        let mut writer = writer;
        std::thread::spawn(move || {
            while let Some(data) = input_rx.blocking_recv() {
                if writer.write_all(&data).is_err() {
                    break;
                }
                let _ = writer.flush();
            }
        });
    }

    // (C) RESIZE + KILL: async task owns the master and child. resize() is a fast ioctl.
    {
        let cancel = cancel.clone();
        let mut child = child;
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        let _ = child.kill();
                        break;
                    }
                    r = resize_rx.recv() => match r {
                        Some((c, rw)) => {
                            let _ = master.resize(PtySize {
                                rows: rw as u16,
                                cols: c as u16,
                                pixel_width: 0,
                                pixel_height: 0,
                            });
                        }
                        None => {
                            // resize_tx dropped -> session torn down
                            let _ = child.kill();
                            break;
                        }
                    }
                }
            }
            let _ = child.wait(); // reap the shell process
        });
    }

    Ok(PtySession {
        input_tx,
        output_rx: Arc::new(tokio::sync::Mutex::new(output_rx)),
        resize_tx,
        cancel,
        shell_pid,
    })
}

/// Read the current working directory of a process by pid.
///
/// macOS has no `/proc`, so we use `proc_pidinfo(PROC_PIDVNODEPATHINFO)` which
/// returns the process's current-directory vnode path — this tracks the shell's
/// `cd`s in real time. Reading a same-user child needs no elevated privileges.
/// Returns `None` if the pid is gone or the syscall fails.
#[cfg(target_os = "macos")]
pub fn read_process_cwd(pid: u32) -> Option<String> {
    // SAFETY: we zero-initialise the POD struct, hand libc a correctly sized
    // buffer, and only read `pvi_cdir.vip_path` back on a full-size success.
    unsafe {
        let mut info: libc::proc_vnodepathinfo = std::mem::zeroed();
        let size = std::mem::size_of::<libc::proc_vnodepathinfo>() as libc::c_int;
        let ret = libc::proc_pidinfo(
            pid as libc::c_int,
            libc::PROC_PIDVNODEPATHINFO,
            0,
            &mut info as *mut _ as *mut libc::c_void,
            size,
        );
        if ret != size {
            return None; // pid gone, or partial/failed read
        }
        // `vip_path` is a fixed MAXPATHLEN byte buffer (modelled as [[c_char;32];32]
        // in libc). Flatten it and read up to the NUL terminator.
        let raw = &info.pvi_cdir.vip_path;
        let bytes: &[u8] =
            std::slice::from_raw_parts(raw.as_ptr() as *const u8, std::mem::size_of_val(raw));
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        if end == 0 {
            return None;
        }
        String::from_utf8(bytes[..end].to_vec()).ok()
    }
}

/// Non-macOS fallback: cwd restoration is a macOS-only feature for now.
#[cfg(not(target_os = "macos"))]
pub fn read_process_cwd(_pid: u32) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_start_dir_falls_back_to_home_for_missing_dir() {
        let home = dirs::home_dir().expect("home dir");
        let got = resolve_start_dir(Some("/nonexistent/definitely/not/here".to_string()));
        assert_eq!(got, Some(home));
    }

    #[test]
    fn resolve_start_dir_uses_a_valid_directory() {
        // A directory guaranteed to exist.
        let tmp = std::env::temp_dir();
        let got = resolve_start_dir(Some(tmp.to_string_lossy().into_owned()));
        assert_eq!(got, Some(tmp));
    }

    #[test]
    fn resolve_start_dir_none_yields_home() {
        assert_eq!(resolve_start_dir(None), dirs::home_dir());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn read_process_cwd_matches_our_own_cwd() {
        let pid = std::process::id();
        let got = read_process_cwd(pid).expect("should read our own cwd");
        // proc_pidinfo resolves symlinks (e.g. /tmp -> /private/tmp on macOS), so
        // compare canonicalised paths rather than raw strings.
        let expected = std::fs::canonicalize(std::env::current_dir().unwrap()).unwrap();
        let got_canon = std::fs::canonicalize(&got).unwrap();
        assert_eq!(got_canon, expected);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn read_process_cwd_returns_none_for_dead_pid() {
        // PID 0 is the kernel scheduler; proc_pidinfo(PROC_PIDVNODEPATHINFO) has no
        // vnode path for it, so we expect None (never a panic).
        assert_eq!(read_process_cwd(0), None);
    }
}
