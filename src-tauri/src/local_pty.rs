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

/// Spawn a local shell in a PTY and return a `PtySession` wired to it. Channel
/// capacities match the SSH path (input 1000, output 128 for back-pressure parity,
/// resize 16).
pub fn create_local_pty_session(cols: u32, rows: u32) -> anyhow::Result<PtySession> {
    let pair = native_pty_system().openpty(PtySize {
        rows: rows as u16,
        cols: cols as u16,
        pixel_width: 0,
        pixel_height: 0,
    })?;

    let mut cmd = CommandBuilder::new(pick_shell());
    cmd.env("TERM", "xterm-256color"); // parity with the SSH PTY
    if let Some(home) = dirs::home_dir() {
        cmd.cwd(home);
    }

    let child = pair.slave.spawn_command(cmd)?;
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
    })
}
