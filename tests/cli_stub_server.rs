//! Process-level CLI tests backed by a minimal local stub S3 endpoint.
//!
//! `cli_unreachable_endpoint.rs` drives every wrapper's `Other` arm with a
//! dead endpoint (connect refused — no S3 error code at all). These tests
//! complete the matrix with arms that need a *live* answer carrying a
//! specific S3 error code or success body that real AWS cannot be made to
//! produce on demand:
//!
//! - `NoSuchAnnotation` from delete-object-annotation: real S3 answers
//!   success (idempotent delete), so the CLI's AnnotationNotFound arms are
//!   unreachable in e2e (see `e2e_delete_object_annotation.rs`).
//! - `ServerSideEncryptionConfigurationNotFoundError`: since 2023 every real
//!   bucket has default encryption, so GetBucketEncryption never 404s.
//! - `NoSuchBucket` from rename: needs an S3 Express directory-bucket
//!   endpoint to answer with that code.
//! - put-bucket-replication success: real success requires fully provisioned
//!   IAM + versioned buckets (out of scope for e2e, which asserts failure).
//! - Crafted get-object-annotation responses (SSE, ETag, checksum-type and
//!   oversized payloads) that drive the integrity-verification branches.
//!
//! The stub answers **every** request on its listener with one canned
//! response; each command under test issues exactly one S3 API call.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;

/// Canned HTTP response served for every request the stub receives.
#[derive(Clone)]
struct StubResponse {
    status: u16,
    /// Extra headers beyond content-length / connection.
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

impl StubResponse {
    fn xml_error(status: u16, code: &str) -> Self {
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
             <Error><Code>{code}</Code><Message>stub error for {code}</Message>\
             <RequestId>stub-request-id</RequestId></Error>"
        )
        .into_bytes();
        StubResponse {
            status,
            headers: vec![("content-type", "application/xml".to_string())],
            body,
        }
    }

    fn ok_empty() -> Self {
        StubResponse {
            status: 200,
            headers: vec![],
            body: Vec::new(),
        }
    }
}

/// Serve one HTTP/1.1 request on `stream` with the canned response.
/// The request (headers + body per content-length) is fully read first so
/// the client never sees a connection reset while still writing.
fn serve_one(stream: TcpStream, response: &StubResponse) {
    let mut reader = BufReader::new(stream);

    let mut line = String::new();
    if reader.read_line(&mut line).is_err() || line.is_empty() {
        return;
    }
    let mut content_length: usize = 0;
    loop {
        let mut header = String::new();
        match reader.read_line(&mut header) {
            Ok(0) => return,
            Ok(_) => {
                let header = header.trim_end();
                if header.is_empty() {
                    break;
                }
                if let Some((name, value)) = header.split_once(':')
                    && name.eq_ignore_ascii_case("content-length")
                    && let Ok(len) = value.trim().parse()
                {
                    content_length = len;
                }
            }
            Err(_) => return,
        }
    }
    if content_length > 0 {
        let mut body = vec![0u8; content_length];
        if reader.read_exact(&mut body).is_err() {
            return;
        }
    }

    let mut stream = reader.into_inner();
    let mut head = format!(
        "HTTP/1.1 {} stub\r\ncontent-length: {}\r\nconnection: close\r\n",
        response.status,
        response.body.len()
    );
    for (name, value) in &response.headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&response.body);
    let _ = stream.flush();
}

/// Start a stub S3 endpoint answering every request with `response`.
/// Returns `host:port`. The accept loop runs on a detached thread for the
/// rest of the test process's lifetime (each test binds its own port 0).
fn spawn_stub(response: StubResponse) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
    let addr = listener.local_addr().expect("stub local addr").to_string();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let response = response.clone();
            thread::spawn(move || serve_one(stream, &response));
        }
    });
    addr
}

/// Run the s3util binary against the stub endpoint with static dummy
/// credentials (no profile / IMDS lookup) and no retries. `side` selects the
/// client-config flag family the subcommand exposes (`target` for most
/// commands, `source` for rename). `-v` raises logging to info so success
/// logs are observable on stderr.
fn run_side_against_stub(side: &str, addr: &str, args: &[&str]) -> (Option<i32>, String, String) {
    let endpoint = format!("http://{addr}");
    let output = Command::new(env!("CARGO_BIN_EXE_s3util"))
        // Directory-bucket (…--x-s3) requests would otherwise trigger the S3
        // Express CreateSession auth flow before the operation under test,
        // wrapping the stub's answer in an identity-resolution dispatch error.
        .env("AWS_S3_DISABLE_EXPRESS_SESSION_AUTH", "true")
        .args(args)
        .args([
            &format!("--{side}-endpoint-url"),
            &endpoint,
            &format!("--{side}-access-key"),
            "AKIAIOSFODNN7EXAMPLE",
            &format!("--{side}-secret-access-key"),
            "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",
            &format!("--{side}-region"),
            "us-east-1",
            "--aws-max-attempts",
            "1",
            "-v",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stdout).to_string(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn run_against_stub(addr: &str, args: &[&str]) -> (Option<i32>, String, String) {
    run_side_against_stub("target", addr, args)
}

// ---------------------------------------------------------------------------
// delete-object-annotation: NoSuchAnnotation → AnnotationNotFound arm.
// Real S3 treats deleting a missing annotation as idempotent success, so
// this arm is reachable only from a service that does report the code.
// ---------------------------------------------------------------------------

#[test]
fn delete_object_annotation_no_such_annotation_exits_4() {
    let addr = spawn_stub(StubResponse::xml_error(404, "NoSuchAnnotation"));
    let (code, stdout, stderr) = run_against_stub(
        &addr,
        &[
            "delete-object-annotation",
            "s3://cov-bucket/key",
            "--annotation-name",
            "cov_note",
        ],
    );
    assert_eq!(
        code,
        Some(4),
        "NoSuchAnnotation must exit 4; stderr: {stderr}"
    );
    assert!(stdout.is_empty(), "stdout must be empty; got: {stdout}");
    assert!(
        stderr.contains("annotation cov_note not found for s3://cov-bucket/key"),
        "expected 'annotation … not found' on stderr; got: {stderr}"
    );
    assert!(
        !stderr.contains("versionId="),
        "no version was requested, so the message must not carry one; got: {stderr}"
    );
}

#[test]
fn delete_object_annotation_no_such_annotation_with_version_id_exits_4() {
    let addr = spawn_stub(StubResponse::xml_error(404, "NoSuchAnnotation"));
    let (code, _stdout, stderr) = run_against_stub(
        &addr,
        &[
            "delete-object-annotation",
            "s3://cov-bucket/key",
            "--annotation-name",
            "cov_note",
            "--target-version-id",
            "cov-version-1",
        ],
    );
    assert_eq!(
        code,
        Some(4),
        "NoSuchAnnotation must exit 4; stderr: {stderr}"
    );
    assert!(
        stderr.contains(
            "annotation cov_note not found for s3://cov-bucket/key (versionId=cov-version-1)"
        ),
        "expected the versioned 'annotation … not found' message; got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// get-bucket-encryption: ServerSideEncryptionConfigurationNotFoundError →
// HeadError::NotFound arm ("encryption configuration … not found").
// ---------------------------------------------------------------------------

#[test]
fn get_bucket_encryption_configuration_not_found_exits_4() {
    let addr = spawn_stub(StubResponse::xml_error(
        404,
        "ServerSideEncryptionConfigurationNotFoundError",
    ));
    let (code, stdout, stderr) =
        run_against_stub(&addr, &["get-bucket-encryption", "s3://cov-bucket"]);
    assert_eq!(
        code,
        Some(4),
        "missing encryption configuration must exit 4; stderr: {stderr}"
    );
    assert!(stdout.is_empty(), "stdout must be empty; got: {stdout}");
    assert!(
        stderr.contains("encryption configuration for s3://cov-bucket not found"),
        "expected 'encryption configuration … not found' on stderr; got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// rename: NoSuchBucket → HeadError::BucketNotFound arm (exit 1 with a
// "bucket … not found" message, not the NotFound exit 4).
// ---------------------------------------------------------------------------

#[test]
fn rename_no_such_bucket_exits_1_with_bucket_message() {
    let addr = spawn_stub(StubResponse::xml_error(404, "NoSuchBucket"));
    // rename requires an S3 Express One Zone bucket name (…--x-s3) and
    // exposes the source-side client flags.
    let (code, _stdout, stderr) = run_side_against_stub(
        "source",
        &addr,
        &[
            "rename",
            "s3://cov-bucket--apne1-az4--x-s3/src-key",
            "s3://cov-bucket--apne1-az4--x-s3/dst-key",
        ],
    );
    assert_eq!(code, Some(1), "NoSuchBucket must exit 1; stderr: {stderr}");
    assert!(
        stderr.contains("bucket s3://cov-bucket--apne1-az4--x-s3 not found"),
        "expected 'bucket … not found' on stderr; got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// put-bucket-replication: success path ("Bucket replication set." + exit 0).
// Real success needs provisioned IAM + versioning, so e2e only pins the
// failure shape; the stub answers 200 like S3 would after full setup.
// ---------------------------------------------------------------------------

#[test]
fn put_bucket_replication_success_exits_0_and_logs() {
    let addr = spawn_stub(StubResponse::ok_empty());

    let config = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(
        config.path(),
        r#"{
            "Role": "arn:aws:iam::000000000000:role/s3-replication-test",
            "Rules": [
                {
                    "ID": "rule-1",
                    "Priority": 1,
                    "Filter": {},
                    "Status": "Enabled",
                    "DeleteMarkerReplication": { "Status": "Disabled" },
                    "Destination": { "Bucket": "arn:aws:s3:::cov-dest-bucket" }
                }
            ]
        }"#,
    )
    .unwrap();
    let config_path = config.path().to_string_lossy().to_string();

    let (code, stdout, stderr) = run_against_stub(
        &addr,
        &["put-bucket-replication", "s3://cov-bucket", &config_path],
    );
    assert_eq!(
        code,
        Some(0),
        "stubbed success must exit 0; stderr: {stderr}"
    );
    assert!(stdout.is_empty(), "stdout must be empty; got: {stdout}");
    assert!(
        stderr.contains("Bucket replication set."),
        "expected the success log on stderr; got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// get-object-annotation: crafted 200 responses driving the integrity
// verification branches that real AWS answers cannot reach on demand.
// The payload "hello" has MD5 5d41402abc4b2a76b9719d911017c592.
// ---------------------------------------------------------------------------

const HELLO_MD5_ETAG: &str = "\"5d41402abc4b2a76b9719d911017c592\"";

fn annotation_response(
    body: &[u8],
    e_tag: &str,
    sse: Option<&str>,
    extra: Vec<(&'static str, String)>,
) -> StubResponse {
    let mut headers = vec![("etag", e_tag.to_string())];
    if let Some(sse) = sse {
        headers.push(("x-amz-server-side-encryption", sse.to_string()));
    }
    headers.extend(extra);
    StubResponse {
        status: 200,
        headers,
        body: body.to_vec(),
    }
}

/// SSE-KMS payload: `verify_etag_md5` only applies to AES256, and with no
/// additional checksum present nothing else can verify the payload — the CLI
/// must warn, still write the file, and exit 0.
#[test]
fn get_object_annotation_kms_payload_unverifiable_warns_and_exits_0() {
    let addr = spawn_stub(annotation_response(
        b"hello",
        HELLO_MD5_ETAG,
        Some("aws:kms"),
        vec![],
    ));

    let dir = tempfile::tempdir().unwrap();
    let outfile = dir.path().join("annotation.bin");
    let outfile_path = outfile.to_string_lossy().to_string();

    let (code, stdout, stderr) = run_against_stub(
        &addr,
        &[
            "get-object-annotation",
            "s3://cov-bucket/key",
            &outfile_path,
            "--annotation-name",
            "cov_note",
        ],
    );
    assert_eq!(
        code,
        Some(0),
        "unverifiable payload still exits 0; stderr: {stderr}"
    );
    assert!(
        stderr.contains("payload integrity could not be verified"),
        "expected the unverifiable warning; got: {stderr}"
    );
    assert!(
        stderr.contains("written, but integrity could NOT be verified"),
        "expected the unverified outcome log; got: {stderr}"
    );
    assert_eq!(
        std::fs::read(&outfile).unwrap(),
        b"hello",
        "payload must still be written"
    );
    assert!(
        stdout.contains("ContentLength"),
        "metadata JSON must be printed to stdout; got: {stdout}"
    );
}

/// AES256 + an ETag that is not the payload's MD5: the pre-write integrity
/// check must fail hard (exit 1) and nothing may be written.
#[test]
fn get_object_annotation_etag_mismatch_exits_1() {
    let addr = spawn_stub(annotation_response(
        b"hello",
        "\"00000000000000000000000000000000\"",
        Some("AES256"),
        vec![],
    ));

    let dir = tempfile::tempdir().unwrap();
    let outfile = dir.path().join("annotation.bin");
    let outfile_path = outfile.to_string_lossy().to_string();

    let (code, _stdout, stderr) = run_against_stub(
        &addr,
        &[
            "get-object-annotation",
            "s3://cov-bucket/key",
            &outfile_path,
            "--annotation-name",
            "cov_note",
        ],
    );
    assert_eq!(code, Some(1), "ETag mismatch must exit 1; stderr: {stderr}");
    assert!(
        stderr.contains("ETag (MD5) verification failed for s3://cov-bucket/key"),
        "expected the ETag mismatch error; got: {stderr}"
    );
    assert!(
        !outfile.exists(),
        "a payload that failed pre-write verification must not be written"
    );
}

/// COMPOSITE checksum type: the composite value cannot be recomputed from the
/// payload alone, so it must be ignored, and verification falls back to the
/// AES256 ETag (which matches here) — success end-to-end.
#[test]
fn get_object_annotation_composite_checksum_verifies_via_etag() {
    let addr = spawn_stub(annotation_response(
        b"hello",
        HELLO_MD5_ETAG,
        Some("AES256"),
        vec![
            ("x-amz-checksum-type", "COMPOSITE".to_string()),
            ("x-amz-checksum-crc32", "AAAAAA==-2".to_string()),
        ],
    ));

    let dir = tempfile::tempdir().unwrap();
    let outfile = dir.path().join("annotation.bin");
    let outfile_path = outfile.to_string_lossy().to_string();

    let (code, _stdout, stderr) = run_against_stub(
        &addr,
        &[
            "get-object-annotation",
            "s3://cov-bucket/key",
            &outfile_path,
            "--annotation-name",
            "cov_note",
        ],
    );
    assert_eq!(
        code,
        Some(0),
        "composite checksum must be skipped and the ETag must verify; stderr: {stderr}"
    );
    assert!(
        stderr.contains("written and verified"),
        "expected the verified outcome log; got: {stderr}"
    );
    assert_eq!(std::fs::read(&outfile).unwrap(), b"hello");
}

/// A payload larger than the 1 MiB annotation limit must be rejected while
/// streaming (bounded read), before anything is written.
#[test]
fn get_object_annotation_payload_over_limit_exits_1() {
    let oversized = vec![b'x'; 1024 * 1024 + 1];
    let addr = spawn_stub(annotation_response(
        &oversized,
        HELLO_MD5_ETAG,
        Some("AES256"),
        vec![],
    ));

    let dir = tempfile::tempdir().unwrap();
    let outfile = dir.path().join("annotation.bin");
    let outfile_path = outfile.to_string_lossy().to_string();

    let (code, _stdout, stderr) = run_against_stub(
        &addr,
        &[
            "get-object-annotation",
            "s3://cov-bucket/key",
            &outfile_path,
            "--annotation-name",
            "cov_note",
        ],
    );
    assert_eq!(
        code,
        Some(1),
        "oversized payload must exit 1; stderr: {stderr}"
    );
    assert!(
        stderr.contains("exceeds the 1 MiB limit"),
        "expected the 1 MiB limit error; got: {stderr}"
    );
    assert!(!outfile.exists(), "oversized payload must not be written");
}
