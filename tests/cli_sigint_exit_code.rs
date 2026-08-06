//! Process-level regression tests for SIGINT (Ctrl+C) exit-code handling.
//!
//! `s3util cp` / `s3util mv` catch Ctrl+C, cancel the transfer pipeline, and
//! must then exit gracefully with code 130 (128 + SIGINT), the conventional
//! shell encoding for a run interrupted by the user — even when the forced
//! shutdown surfaces a non-cancellation error on its way down. These tests
//! run the real binary against a minimal in-process S3 endpoint (no AWS
//! access needed): the endpoint serves object bodies that keep trickling (or
//! stall entirely) so the transfer runs until the test sends SIGINT, and
//! require a graceful exit 130 — `code()` is `None` for a raw signal kill,
//! so this also proves the signal was caught rather than terminating the
//! process directly — plus a no-SIGINT control that must still complete the
//! transfer and exit 0.
//!
//! Covers `src/bin/s3util/cli/ctrl_c_handler.rs` (`CTRL_C_RECEIVED` flag)
//! and `src/bin/s3util/cli/mod.rs` (`run_copy_phase`'s Ctrl+C-takes-
//! precedence exit decision). Same fix as nidor1998/s3rm-rs#100 and
//! nidor1998/s3ls-rs#34.

use std::io::{Read as _, Write as _};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Block size / pacing for the trickling body: fast enough that the SDK's
/// stalled-stream protection (minimum 1 B/s) never trips, slow enough that
/// the body outlives the test's SIGINT by a wide margin.
#[cfg(target_family = "unix")]
const TRICKLE_BLOCK: usize = 8 * 1024;
#[cfg(target_family = "unix")]
const TRICKLE_PAUSE: Duration = Duration::from_millis(10);

/// GET body bytes the fake endpoint must have served before SIGINT is sent,
/// proving the child is inside the transfer's body loop (its Ctrl+C handler
/// is installed before the pipeline starts, thus long since registered).
#[cfg(target_family = "unix")]
const MIN_BYTES_BEFORE_SIGINT: usize = 64 * 1024;

/// Body served by the `Complete` plan, with its real single-part ETag.
const COMPLETE_BODY: &[u8] = b"hello";
const COMPLETE_BODY_MD5: &str = "5d41402abc4b2a76b9719d911017c592";

/// Fake ETag for bodies whose verification never runs (the transfer is
/// interrupted or fails before the ETag check).
const FAKE_MD5: &str = "0123456789abcdef0123456789abcdef";

/// How the fake endpoint serves a GET object body.
#[derive(Clone, Copy)]
enum GetBodyPlan {
    /// Declare `declared` bytes and serve them `TRICKLE_BLOCK` at a time
    /// with a pause between blocks, so the child's read loop keeps
    /// iterating (and keeps observing the cancellation token) until the
    /// test interrupts it.
    #[cfg(target_family = "unix")]
    Trickle { declared: usize },
    /// Declare `declared` bytes, serve only `prelude`, then hold the
    /// connection open without ever sending another byte. The child's next
    /// body read blocks until the SDK's stalled-stream protection fails it
    /// with a NON-cancellation error — the exact case where Ctrl+C must
    /// take precedence over the transfer error.
    #[cfg(target_family = "unix")]
    StallAfterPrelude { declared: usize, prelude: usize },
    /// Serve `COMPLETE_BODY` in full with its correct length and MD5 ETag,
    /// letting the transfer succeed end-to-end.
    Complete,
}

impl GetBodyPlan {
    fn declared_len(&self) -> usize {
        match self {
            #[cfg(target_family = "unix")]
            GetBodyPlan::Trickle { declared } => *declared,
            #[cfg(target_family = "unix")]
            GetBodyPlan::StallAfterPrelude { declared, .. } => *declared,
            GetBodyPlan::Complete => COMPLETE_BODY.len(),
        }
    }

    fn e_tag(&self) -> &'static str {
        match self {
            GetBodyPlan::Complete => COMPLETE_BODY_MD5,
            #[cfg(target_family = "unix")]
            _ => FAKE_MD5,
        }
    }
}

/// Handle to a fake S3 endpoint running on background threads.
struct FakeS3 {
    endpoint: String,
    /// Total GET-object body bytes written to child connections so far.
    get_body_bytes_served: Arc<AtomicUsize>,
    /// Number of DeleteObject requests received (mv's source delete).
    delete_requests_served: Arc<AtomicUsize>,
}

/// Serve canned S3 responses over plain HTTP/1.1, one request per
/// connection (every response says `connection: close`):
///
/// - `HEAD` (HeadObject) → 200 with the plan's declared content-length and
///   ETag, no body.
/// - `GET` (GetObject) → 200 with the body served per `GetBodyPlan`.
/// - `PUT` (PutObject, mv's copy target) → drain the request until the
///   child stops sending, then 200 with a fake ETag. Interrupted runs
///   abort mid-request, so this is reached only if the copy outraces the
///   test's SIGINT — the exit-code assertions hold either way.
/// - `DELETE` (DeleteObject, mv's source delete) → 204, counted. An
///   interrupted mv must never send one.
fn spawn_fake_s3(plan: GetBodyPlan) -> FakeS3 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind fake S3 listener");
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let get_body_bytes_served = Arc::new(AtomicUsize::new(0));
    let delete_requests_served = Arc::new(AtomicUsize::new(0));

    let body_bytes = Arc::clone(&get_body_bytes_served);
    let deletes = Arc::clone(&delete_requests_served);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { break };
            let body_bytes = Arc::clone(&body_bytes);
            let deletes = Arc::clone(&deletes);
            std::thread::spawn(move || serve_connection(stream, plan, &body_bytes, &deletes));
        }
    });

    FakeS3 {
        endpoint,
        get_body_bytes_served,
        delete_requests_served,
    }
}

fn serve_connection(
    mut stream: TcpStream,
    plan: GetBodyPlan,
    get_body_bytes_served: &AtomicUsize,
    delete_requests_served: &AtomicUsize,
) {
    let _ = stream.set_nodelay(true);
    let _ = stream.set_read_timeout(Some(Duration::from_secs(30)));
    let Some(request_line) = read_request_head(&mut stream) else {
        return;
    };
    let method = request_line.split_whitespace().next().unwrap_or("");

    match method {
        "HEAD" => {
            let _ = write_response_head(&mut stream, "200 OK", plan.declared_len(), plan.e_tag());
        }
        "GET" => {
            if write_response_head(&mut stream, "200 OK", plan.declared_len(), plan.e_tag())
                .is_err()
            {
                return;
            }
            serve_get_body(&mut stream, plan, get_body_bytes_served);
        }
        "PUT" => {
            // Drain whatever the child sends; a 2s idle read means the
            // child aborted (or finished) the upload.
            let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 16 * 1024];
            loop {
                match stream.read(&mut buf) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = write_response_head(&mut stream, "200 OK", 0, FAKE_MD5);
        }
        "DELETE" => {
            delete_requests_served.fetch_add(1, Ordering::SeqCst);
            let _ = stream.write_all(
                b"HTTP/1.1 204 No Content\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            );
        }
        _ => {
            let _ = stream.write_all(
                b"HTTP/1.1 400 Bad Request\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            );
        }
    }
    let _ = stream.flush();
}

/// Read one request's head (through `\r\n\r\n`) and return its request line.
fn read_request_head(stream: &mut TcpStream) -> Option<String> {
    let mut data = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) | Err(_) => return None,
            Ok(n) => {
                data.extend_from_slice(&buf[..n]);
                if data.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
        }
    }
    let head = String::from_utf8_lossy(&data).to_string();
    Some(head.lines().next().unwrap_or("").to_string())
}

fn write_response_head(
    stream: &mut TcpStream,
    status: &str,
    content_length: usize,
    e_tag: &str,
) -> std::io::Result<()> {
    // `last-modified` is mandatory: the transfer pipeline unwraps it from
    // GetObject/HeadObject responses (real S3 always sends it).
    let head = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/octet-stream\r\n\
         content-length: {content_length}\r\netag: \"{e_tag}\"\r\n\
         last-modified: Thu, 01 Jan 2026 00:00:00 GMT\r\nconnection: close\r\n\r\n"
    );
    stream.write_all(head.as_bytes())?;
    stream.flush()
}

fn serve_get_body(stream: &mut TcpStream, plan: GetBodyPlan, bytes_served: &AtomicUsize) {
    match plan {
        GetBodyPlan::Complete => {
            if stream.write_all(COMPLETE_BODY).is_ok() {
                bytes_served.fetch_add(COMPLETE_BODY.len(), Ordering::SeqCst);
            }
            let _ = stream.flush();
        }
        #[cfg(target_family = "unix")]
        GetBodyPlan::Trickle { declared } => {
            let block = vec![b'x'; TRICKLE_BLOCK];
            let mut remaining = declared;
            while remaining > 0 {
                let n = remaining.min(block.len());
                if stream.write_all(&block[..n]).is_err() || stream.flush().is_err() {
                    return; // child exited; its socket is gone
                }
                bytes_served.fetch_add(n, Ordering::SeqCst);
                remaining -= n;
                std::thread::sleep(TRICKLE_PAUSE);
            }
        }
        #[cfg(target_family = "unix")]
        GetBodyPlan::StallAfterPrelude { prelude, .. } => {
            let block = vec![b'x'; prelude];
            if stream.write_all(&block).is_err() || stream.flush().is_err() {
                return;
            }
            bytes_served.fetch_add(prelude, Ordering::SeqCst);
            // Hold the connection open, sending nothing, until the child
            // goes away. Probe with a read: it returns 0/Err once the
            // child's socket is closed, and times out (30s, looping)
            // while the child is still attached.
            let mut probe = [0u8; 16];
            loop {
                match stream.read(&mut probe) {
                    Ok(0) => return,
                    Ok(_) => {}
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(_) => return,
                }
            }
        }
    }
}

/// Client-config flags binding one side (`source` / `target`) of the child
/// to the fake endpoint with static credentials, so no AWS configuration on
/// the host is consulted.
fn side_flags(side: &str, endpoint: &str) -> Vec<String> {
    vec![
        format!("--{side}-endpoint-url"),
        endpoint.to_string(),
        format!("--{side}-access-key"),
        "AKIAIOSFODNN7EXAMPLE".to_string(),
        format!("--{side}-secret-access-key"),
        "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY".to_string(),
        format!("--{side}-region"),
        "us-east-1".to_string(),
    ]
}

/// Spawn the s3util binary. `stdout` is `Stdio::null()` for interrupted
/// runs (an unread stdout pipe could otherwise block the child's writes and
/// keep it out of the body-read loop under test).
fn spawn_s3util(args: &[String], stdout: Stdio) -> Child {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
        .args(args)
        .args(["--aws-max-attempts", "1"])
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn s3util")
}

/// Wait until the fake endpoint has served at least `at_least` GET body
/// bytes, so the child is provably inside the transfer. Fails fast with the
/// child's stderr if it exits early.
#[cfg(target_family = "unix")]
fn wait_for_body_bytes(child: &mut Child, fake: &FakeS3, at_least: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while fake.get_body_bytes_served.load(Ordering::SeqCst) < at_least {
        if let Some(status) = child.try_wait().expect("failed to poll s3util") {
            let stderr = read_stderr(child);
            panic!(
                "s3util exited ({status:?}) before the fake S3 served {at_least} body bytes\nstderr: {stderr}"
            );
        }
        assert!(
            Instant::now() < deadline,
            "fake S3 served only {} body bytes before timeout",
            fake.get_body_bytes_served.load(Ordering::SeqCst)
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Wait for the child to exit within `deadline` — SIGINT must terminate the
/// process promptly, not hang it. Kills the child on timeout.
fn wait_with_deadline(child: &mut Child, deadline: Duration) -> std::process::ExitStatus {
    let end = Instant::now() + deadline;
    loop {
        if let Some(status) = child.try_wait().expect("failed to poll s3util") {
            return status;
        }
        if Instant::now() >= end {
            let _ = child.kill();
            let _ = child.wait();
            panic!("s3util did not exit within {deadline:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Read the child's stderr after it has exited.
fn read_stderr(child: &mut Child) -> String {
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    stderr
}

#[cfg(target_family = "unix")]
fn send_sigint(child: &Child) {
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(child.id() as i32),
        nix::sys::signal::Signal::SIGINT,
    )
    .expect("failed to send SIGINT to s3util");
}

/// Assert a graceful, quiet exit 130: the Ctrl+C handler observed the
/// signal (`code()` would be `None` for a raw signal kill), and the run was
/// reported as an interruption, not a failure or a panic.
#[cfg(target_family = "unix")]
fn assert_graceful_130(status: std::process::ExitStatus, stderr: &str, label: &str) {
    assert_eq!(
        status.code(),
        Some(130),
        "[{label}] expected graceful exit 130 after SIGINT, got {status:?}\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("ctrl-c received, shutting down."),
        "[{label}] the Ctrl+C handler must have observed the signal\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "[{label}] SIGINT must not panic\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("copy failed."),
        "[{label}] an interrupted run must not be reported as a failure\nstderr: {stderr}"
    );
}

/// Ctrl+C while an S3-to-stdout copy (`cp s3://… -`, serial path) is inside
/// its body-read loop: the loop observes the cancellation and the process
/// must exit 130.
#[test]
#[cfg(target_family = "unix")]
fn sigint_during_s3_to_stdout_download_exits_130() {
    let fake = spawn_fake_s3(GetBodyPlan::Trickle {
        declared: 64 * 1024 * 1024,
    });

    let mut args = vec![
        "cp".to_string(),
        "s3://sigint-test-bucket/big.bin".to_string(),
        "-".to_string(),
        // Pin the pure serial path (single non-ranged GET, no HEAD).
        "--max-parallel-uploads".to_string(),
        "1".to_string(),
    ];
    args.extend(side_flags("source", &fake.endpoint));
    let mut child = spawn_s3util(&args, Stdio::null());

    wait_for_body_bytes(&mut child, &fake, MIN_BYTES_BEFORE_SIGINT);
    send_sigint(&child);

    let status = wait_with_deadline(&mut child, Duration::from_secs(30));
    let stderr = read_stderr(&mut child);
    assert_graceful_130(status, &stderr, "s3-to-stdout");
}

/// Ctrl+C while the body read is BLOCKED on a stalled connection: no read
/// completes after the signal, so the pipeline cannot observe the
/// cancellation token — the read eventually fails with the SDK's
/// stalled-stream-protection error instead. Ctrl+C must take precedence
/// over that transfer error: exit 130 with no "copy failed." report.
/// (Before the CTRL_C_RECEIVED flag existed, this run exited 1.)
#[test]
#[cfg(target_family = "unix")]
fn sigint_takes_precedence_over_stalled_transfer_error() {
    let fake = spawn_fake_s3(GetBodyPlan::StallAfterPrelude {
        declared: 4 * 1024 * 1024,
        prelude: 64 * 1024,
    });

    let mut args = vec![
        "cp".to_string(),
        "s3://sigint-test-bucket/big.bin".to_string(),
        "-".to_string(),
        "--max-parallel-uploads".to_string(),
        "1".to_string(),
    ];
    args.extend(side_flags("source", &fake.endpoint));
    let mut child = spawn_s3util(&args, Stdio::null());

    wait_for_body_bytes(&mut child, &fake, 64 * 1024);
    // Give the child time to consume the prelude and block on the next
    // body read. (If it were somehow still between reads, the run would
    // exit 130 via the cancellation token instead — same assertion.)
    std::thread::sleep(Duration::from_millis(500));
    send_sigint(&child);

    // The stalled read is failed by the SDK only after its grace period;
    // allow a generous margin.
    let status = wait_with_deadline(&mut child, Duration::from_secs(90));
    let stderr = read_stderr(&mut child);
    assert_graceful_130(status, &stderr, "stalled-body");
}

/// Ctrl+C while an S3-to-local copy (HEAD + full GET below the multipart
/// threshold) is writing the destination file: exit 130.
#[test]
#[cfg(target_family = "unix")]
fn sigint_during_s3_to_local_download_exits_130() {
    let fake = spawn_fake_s3(GetBodyPlan::Trickle {
        declared: 4 * 1024 * 1024,
    });

    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.bin").to_string_lossy().to_string();

    let mut args = vec![
        "cp".to_string(),
        "s3://sigint-test-bucket/big.bin".to_string(),
        target,
    ];
    args.extend(side_flags("source", &fake.endpoint));
    let mut child = spawn_s3util(&args, Stdio::null());

    wait_for_body_bytes(&mut child, &fake, MIN_BYTES_BEFORE_SIGINT);
    send_sigint(&child);

    let status = wait_with_deadline(&mut child, Duration::from_secs(30));
    let stderr = read_stderr(&mut child);
    assert_graceful_130(status, &stderr, "s3-to-local");
}

/// Ctrl+C during `mv`'s copy phase: exit 130, and the source object must
/// survive — an interrupted mv must never reach its source-delete step.
#[test]
#[cfg(target_family = "unix")]
fn sigint_during_mv_exits_130_and_source_survives() {
    let fake = spawn_fake_s3(GetBodyPlan::Trickle {
        declared: 2 * 1024 * 1024,
    });

    let mut args = vec![
        "mv".to_string(),
        "s3://sigint-src-bucket/big.bin".to_string(),
        "s3://sigint-dst-bucket/big.bin".to_string(),
        // Keep the fake endpoint's API surface minimal: no GetObjectTagging.
        "--disable-tagging".to_string(),
    ];
    args.extend(side_flags("source", &fake.endpoint));
    args.extend(side_flags("target", &fake.endpoint));
    let mut child = spawn_s3util(&args, Stdio::null());

    wait_for_body_bytes(&mut child, &fake, MIN_BYTES_BEFORE_SIGINT);
    send_sigint(&child);

    let status = wait_with_deadline(&mut child, Duration::from_secs(30));
    let stderr = read_stderr(&mut child);

    assert_eq!(
        status.code(),
        Some(130),
        "[mv] expected graceful exit 130 after SIGINT, got {status:?}\nstderr: {stderr}"
    );
    assert!(
        stderr.contains("ctrl-c received, shutting down."),
        "[mv] the Ctrl+C handler must have observed the signal\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("panicked"),
        "[mv] SIGINT must not panic\nstderr: {stderr}"
    );
    assert_eq!(
        fake.delete_requests_served.load(Ordering::SeqCst),
        0,
        "[mv] an interrupted mv must not delete the source object\nstderr: {stderr}"
    );
    assert!(
        !stderr.contains("Source delete completed."),
        "[mv] an interrupted mv must not report a source delete\nstderr: {stderr}"
    );
}

/// Control: the same harness without SIGINT completes the transfer and
/// exits 0 with the full body on stdout — the SIGINT handling must not
/// affect uninterrupted runs.
#[test]
fn download_without_sigint_completes_and_exits_zero() {
    let fake = spawn_fake_s3(GetBodyPlan::Complete);

    let mut args = vec![
        "cp".to_string(),
        "s3://sigint-test-bucket/hello.txt".to_string(),
        "-".to_string(),
        "--max-parallel-uploads".to_string(),
        "1".to_string(),
    ];
    args.extend(side_flags("source", &fake.endpoint));
    let mut child = spawn_s3util(&args, Stdio::piped());

    let status = wait_with_deadline(&mut child, Duration::from_secs(60));
    let stderr = read_stderr(&mut child);

    assert_eq!(
        status.code(),
        Some(0),
        "expected exit 0, got {status:?}\nstderr: {stderr}"
    );
    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .expect("stdout must be piped")
        .read_to_end(&mut stdout)
        .expect("failed to read s3util stdout");
    assert_eq!(
        stdout, COMPLETE_BODY,
        "the full body must reach stdout\nstderr: {stderr}"
    );
    assert_eq!(
        fake.get_body_bytes_served.load(Ordering::SeqCst),
        COMPLETE_BODY.len(),
        "the fake S3 must have served the body exactly once"
    );
    assert_eq!(
        fake.delete_requests_served.load(Ordering::SeqCst),
        0,
        "cp must never send delete requests"
    );
    assert!(
        !stderr.contains("panicked"),
        "an uninterrupted run must not panic\nstderr: {stderr}"
    );
}
