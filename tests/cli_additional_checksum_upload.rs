//! Process-level tests of the additional checksum that `cp` sends with each
//! upload request, against a local stub S3 endpoint that records requests.
//!
//! The SDK calculates request checksums only for CRC32/CRC32C/CRC64NVME/SHA1/
//! SHA256 and silently sends a CRC32 when `MD5` is requested, so s3util
//! calculates the value itself and sends it in the header of the requested
//! algorithm. These tests check, for every algorithm and every upload path
//! (single-part and multipart, from a file and from stdin), that:
//!
//! - the request carries `x-amz-checksum-<algorithm>` with the checksum of the
//!   exact bytes sent, calculated here independently with the hash crates;
//! - no checksum header of another algorithm is sent;
//! - `CompleteMultipartUpload` lists the per-part checksum of every part;
//! - the upload succeeds when the stub answers like S3, and fails when the
//!   final checksum returned by the stub does not match.
//!
//! The stub answers like S3 does: it echoes the checksum header of
//! PutObject/UploadPart, and on CompleteMultipartUpload it returns the
//! composite ETag and the composite checksum calculated from the part
//! checksums it received.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha1::Digest as _;

const ALL_ALGORITHMS: [&str; 10] = [
    "CRC32",
    "CRC32C",
    "CRC64NVME",
    "SHA1",
    "SHA256",
    "SHA512",
    "MD5",
    "XXHASH128",
    "XXHASH3",
    "XXHASH64",
];

// Algorithms with a composite multipart checksum. CRC64NVME is always a full
// object checksum, so its multipart upload is not covered by this stub.
const COMPOSITE_ALGORITHMS: [&str; 9] = [
    "CRC32",
    "CRC32C",
    "SHA1",
    "SHA256",
    "SHA512",
    "MD5",
    "XXHASH128",
    "XXHASH3",
    "XXHASH64",
];

const NEW_ALGORITHMS: [&str; 5] = ["SHA512", "MD5", "XXHASH128", "XXHASH3", "XXHASH64"];

const MIB: usize = 1024 * 1024;

/// The raw digest of `data`, calculated with the hash crates directly.
/// CRC and XXHash values are big-endian, as S3 encodes them.
fn raw_digest(algorithm: &str, data: &[u8]) -> Vec<u8> {
    match algorithm {
        "CRC32" => crc32fast::hash(data).to_be_bytes().to_vec(),
        "CRC32C" => crc32c::crc32c(data).to_be_bytes().to_vec(),
        "CRC64NVME" => crc_fast::checksum(crc_fast::CrcAlgorithm::Crc64Nvme, data)
            .to_be_bytes()
            .to_vec(),
        "SHA1" => sha1::Sha1::digest(data).to_vec(),
        "SHA256" => sha2::Sha256::digest(data).to_vec(),
        "SHA512" => sha2::Sha512::digest(data).to_vec(),
        "MD5" => md5::compute(data).0.to_vec(),
        "XXHASH128" => twox_hash::XxHash3_128::oneshot(data).to_be_bytes().to_vec(),
        "XXHASH3" => twox_hash::XxHash3_64::oneshot(data).to_be_bytes().to_vec(),
        "XXHASH64" => twox_hash::XxHash64::oneshot(0, data).to_be_bytes().to_vec(),
        _ => panic!("unknown algorithm {algorithm}"),
    }
}

fn checksum_of(algorithm: &str, data: &[u8]) -> String {
    STANDARD.encode(raw_digest(algorithm, data))
}

fn checksum_header(algorithm: &str) -> String {
    format!("x-amz-checksum-{}", algorithm.to_ascii_lowercase())
}

/// Deterministic, non-uniform test data.
fn test_data(len: usize, seed: u32) -> Vec<u8> {
    (0..len)
        .map(|i| ((i as u32).wrapping_mul(31).wrapping_add(seed) % 251) as u8)
        .collect()
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: String,
    target: String,
    /// Header names are lower-cased.
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl RecordedRequest {
    fn query_has(&self, name: &str) -> bool {
        self.target
            .split_once('?')
            .map(|(_, query)| {
                query
                    .split('&')
                    .any(|pair| pair == name || pair.starts_with(&format!("{name}=")))
            })
            .unwrap_or(false)
    }

    fn query_value(&self, name: &str) -> Option<String> {
        let (_, query) = self.target.split_once('?')?;
        query
            .split('&')
            .find_map(|pair| pair.strip_prefix(&format!("{name}=")))
            .map(|value| value.to_string())
    }

    fn is_put_object(&self) -> bool {
        self.method == "PUT" && !self.query_has("partNumber")
    }

    fn is_upload_part(&self) -> bool {
        self.method == "PUT" && self.query_has("partNumber")
    }

    fn is_create_multipart_upload(&self) -> bool {
        self.method == "POST" && self.query_has("uploads")
    }

    fn is_complete_multipart_upload(&self) -> bool {
        self.method == "POST" && self.query_has("uploadId")
    }

    fn part_number(&self) -> i32 {
        self.query_value("partNumber").unwrap().parse().unwrap()
    }

    /// Every `x-amz-checksum-<algorithm>` header (not `x-amz-checksum-type`
    /// or `x-amz-checksum-algorithm`).
    fn checksum_headers(&self) -> Vec<(String, String)> {
        let mut headers: Vec<(String, String)> = self
            .headers
            .iter()
            .filter(|(name, _)| {
                name.starts_with("x-amz-checksum-")
                    && *name != "x-amz-checksum-type"
                    && *name != "x-amz-checksum-algorithm"
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        headers.sort();
        headers
    }
}

#[derive(Clone, Default)]
struct StubOptions {
    /// Return this final checksum from CompleteMultipartUpload/PutObject
    /// instead of the correct one.
    wrong_final_checksum: Option<String>,
}

struct Stub {
    addr: String,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    shutdown: Arc<AtomicBool>,
}

/// Stops the accept loop, on a normal return as well as on a panic, so the
/// listener and the recorded request bodies do not outlive the test case.
impl Drop for Stub {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Wake the blocking accept so that the loop sees the flag.
        let _ = TcpStream::connect(&self.addr);
    }
}

impl Stub {
    fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().unwrap().clone()
    }
}

fn read_request(stream: &TcpStream) -> Option<RecordedRequest> {
    let mut reader = BufReader::new(stream);

    let mut line = String::new();
    if reader.read_line(&mut line).ok()? == 0 {
        return None;
    }
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();

    let mut headers = HashMap::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 {
            return None;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
        }
    }

    // A streamed (chunked) body would not be recorded correctly. Every upload
    // path sets an explicit checksum, which keeps the body unchunked.
    assert!(
        !headers.contains_key("transfer-encoding"),
        "unexpected chunked request: {method} {target} {headers:?}"
    );

    let content_length: usize = headers
        .get("content-length")
        .map(|value| value.parse().unwrap())
        .unwrap_or(0);
    let mut body = vec![0u8; content_length];
    reader.read_exact(&mut body).ok()?;

    Some(RecordedRequest {
        method,
        target,
        headers,
        body,
    })
}

fn write_response(mut stream: TcpStream, status: u16, headers: &[(String, String)], body: &[u8]) {
    let mut head = format!(
        "HTTP/1.1 {status} stub\r\ncontent-length: {}\r\nconnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn quoted_md5_hex(data: &[u8]) -> String {
    format!("\"{:x}\"", md5::compute(data))
}

/// The response S3 would send. `algorithm` is the upload's checksum algorithm.
fn respond(
    request: &RecordedRequest,
    recorded: &[RecordedRequest],
    algorithm: &str,
    options: &StubOptions,
) -> (u16, Vec<(String, String)>, Vec<u8>) {
    if request.method == "HEAD" || request.method == "GET" {
        return (404, vec![], vec![]);
    }

    if request.is_put_object() || request.is_upload_part() {
        let mut headers = vec![("etag".to_string(), quoted_md5_hex(&request.body))];
        // S3 echoes the checksum of the request.
        for (name, value) in request.checksum_headers() {
            let value = match (&options.wrong_final_checksum, request.is_put_object()) {
                (Some(wrong), true) => wrong.clone(),
                _ => value,
            };
            headers.push((name, value));
        }
        return (200, headers, vec![]);
    }

    if request.is_create_multipart_upload() {
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
            <InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Bucket>bucket</Bucket><Key>key</Key><UploadId>upload-1</UploadId>\
            </InitiateMultipartUploadResult>";
        return (
            200,
            vec![("content-type".to_string(), "application/xml".to_string())],
            body.as_bytes().to_vec(),
        );
    }

    if request.is_complete_multipart_upload() {
        let mut parts: Vec<&RecordedRequest> =
            recorded.iter().filter(|r| r.is_upload_part()).collect();
        parts.sort_by_key(|r| r.part_number());

        let concatenated_md5: Vec<u8> = parts
            .iter()
            .flat_map(|part| md5::compute(&part.body).0)
            .collect();
        let e_tag = format!(
            "&quot;{:x}-{}&quot;",
            md5::compute(&concatenated_md5),
            parts.len()
        );

        // Like S3, reject the completion if a part was uploaded without a
        // checksum of the upload's algorithm.
        let part_checksums: Option<Vec<Vec<u8>>> = parts
            .iter()
            .map(|part| {
                let value = part.headers.get(&checksum_header(algorithm))?;
                STANDARD.decode(value).ok()
            })
            .collect();
        let Some(part_checksums) = part_checksums else {
            let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                <Error><Code>InvalidPart</Code><Message>missing part checksum</Message></Error>";
            return (
                400,
                vec![("content-type".to_string(), "application/xml".to_string())],
                body.as_bytes().to_vec(),
            );
        };
        let concatenated_checksums: Vec<u8> = part_checksums.concat();
        let checksum = options.wrong_final_checksum.clone().unwrap_or_else(|| {
            format!(
                "{}-{}",
                checksum_of(algorithm, &concatenated_checksums),
                parts.len()
            )
        });

        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
            <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
            <Location>http://stub/bucket/key</Location><Bucket>bucket</Bucket><Key>key</Key>\
            <ETag>{e_tag}</ETag><Checksum{algorithm}>{checksum}</Checksum{algorithm}>\
            <ChecksumType>COMPOSITE</ChecksumType></CompleteMultipartUploadResult>"
        );
        return (
            200,
            vec![("content-type".to_string(), "application/xml".to_string())],
            body.into_bytes(),
        );
    }

    (400, vec![], vec![])
}

fn spawn_stub(algorithm: &'static str, options: StubOptions) -> Stub {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub listener");
    let addr = listener.local_addr().expect("stub local addr").to_string();
    let requests = Arc::new(Mutex::new(Vec::<RecordedRequest>::new()));

    let shutdown = Arc::new(AtomicBool::new(false));

    let shared = Arc::clone(&requests);
    let stop = Arc::clone(&shutdown);
    thread::spawn(move || {
        for stream in listener.incoming() {
            if stop.load(Ordering::SeqCst) {
                break;
            }
            let Ok(stream) = stream else { continue };
            let requests = Arc::clone(&shared);
            let options = options.clone();
            thread::spawn(move || {
                let Some(request) = read_request(&stream) else {
                    return;
                };
                let (status, headers, body) = {
                    let mut recorded = requests.lock().unwrap();
                    recorded.push(request.clone());
                    respond(&request, &recorded, algorithm, &options)
                };
                write_response(stream, status, &headers, &body);
            });
        }
    });

    Stub {
        addr,
        requests,
        shutdown,
    }
}

/// Run `s3util cp <source> s3://bucket/key` against the stub. `stdin` is
/// written to the child's stdin (for `cp -`).
fn run_cp(stub: &Stub, source: &str, extra_args: &[&str], stdin: Option<&[u8]>) -> (i32, String) {
    let endpoint = format!("http://{}", stub.addr);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3util"));
    cmd.args(["cp", source, "s3://bucket/key"])
        .args(extra_args)
        .args([
            "--target-endpoint-url",
            &endpoint,
            "--target-force-path-style",
            "--target-access-key",
            "AKIAIOSFODNN7EXAMPLE",
            "--target-secret-access-key",
            "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",
            "--target-region",
            "us-east-1",
            "--aws-max-attempts",
            "1",
        ])
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("failed to spawn s3util binary");
    if let Some(data) = stdin {
        let mut child_stdin = child.stdin.take().unwrap();
        child_stdin.write_all(data).unwrap();
        // Dropping closes stdin so that the child sees EOF.
    }
    let output = child.wait_with_output().expect("failed to wait for s3util");
    (
        output.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn write_temp_file(data: &[u8]) -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    file.write_all(data).unwrap();
    file.flush().unwrap();
    file
}

/// The `<Part>` entries of a CompleteMultipartUpload request body, as
/// (part number, value of `<Checksum{algorithm}>`).
fn completed_parts(body: &[u8], algorithm: &str) -> Vec<(i32, Option<String>)> {
    let body = String::from_utf8(body.to_vec()).unwrap();
    let element = |part: &str, name: &str| -> Option<String> {
        let start = part.find(&format!("<{name}>"))? + name.len() + 2;
        let end = part[start..].find(&format!("</{name}>"))? + start;
        Some(part[start..end].to_string())
    };
    body.split("<Part>")
        .skip(1)
        .map(|part| {
            (
                element(part, "PartNumber").unwrap().parse().unwrap(),
                element(part, &format!("Checksum{algorithm}")),
            )
        })
        .collect()
}

/// Assert that `request` carries exactly the checksum of its body for
/// `algorithm`, and no checksum of another algorithm.
fn assert_request_checksum(request: &RecordedRequest, algorithm: &str) {
    assert_eq!(
        request.checksum_headers(),
        vec![(
            checksum_header(algorithm),
            checksum_of(algorithm, &request.body)
        )],
        "{algorithm}: {} {}",
        request.method,
        request.target
    );
    assert_eq!(
        request
            .headers
            .get("x-amz-sdk-checksum-algorithm")
            .map(String::as_str),
        Some(algorithm),
        "{algorithm}: {} {}",
        request.method,
        request.target
    );
}

/// Assert the multipart upload recorded by `stub` was made of `expected_parts`
/// with checksums of `algorithm`, and that CompleteMultipartUpload listed them.
fn assert_multipart_upload(stub: &Stub, algorithm: &str, expected_parts: &[&[u8]]) {
    let requests = stub.requests();

    let create = requests
        .iter()
        .find(|r| r.is_create_multipart_upload())
        .expect("CreateMultipartUpload");
    assert_eq!(
        create
            .headers
            .get("x-amz-checksum-algorithm")
            .map(String::as_str),
        Some(algorithm)
    );

    let mut parts: Vec<&RecordedRequest> = requests.iter().filter(|r| r.is_upload_part()).collect();
    parts.sort_by_key(|r| r.part_number());
    assert_eq!(parts.len(), expected_parts.len(), "{algorithm}");
    for (i, (part, expected)) in parts.iter().zip(expected_parts).enumerate() {
        // The completion below lists parts 1..=N, so the uploaded parts must
        // carry exactly those numbers (S3 rejects a completion that names a
        // part that was never uploaded).
        assert_eq!(part.part_number(), i as i32 + 1, "{algorithm}");
        assert_eq!(part.body.as_slice(), *expected, "{algorithm}");
        assert_request_checksum(part, algorithm);
    }

    let complete = requests
        .iter()
        .find(|r| r.is_complete_multipart_upload())
        .expect("CompleteMultipartUpload");
    let listed = completed_parts(&complete.body, algorithm);
    let expected: Vec<(i32, Option<String>)> = expected_parts
        .iter()
        .enumerate()
        .map(|(i, data)| (i as i32 + 1, Some(checksum_of(algorithm, data))))
        .collect();
    assert_eq!(
        listed,
        expected,
        "{algorithm}: {}",
        String::from_utf8_lossy(&complete.body)
    );
}

// ---------------------------------------------------------------------------
// Local file, single part (PutObject).
// ---------------------------------------------------------------------------

#[test]
fn local_single_part_sends_checksum_of_every_algorithm() {
    let data = test_data(70_000, 1);
    let file = write_temp_file(&data);

    for algorithm in ALL_ALGORITHMS {
        let stub = spawn_stub(algorithm, StubOptions::default());
        let (code, stderr) = run_cp(
            &stub,
            file.path().to_str().unwrap(),
            &["--additional-checksum-algorithm", algorithm],
            None,
        );
        assert_eq!(code, 0, "{algorithm}: {stderr}");

        let requests = stub.requests();
        let puts: Vec<&RecordedRequest> = requests.iter().filter(|r| r.is_put_object()).collect();
        assert_eq!(puts.len(), 1, "{algorithm}");
        assert_eq!(puts[0].body, data, "{algorithm}");
        assert_request_checksum(puts[0], algorithm);
        // Content-MD5 is still sent for transit integrity.
        assert_eq!(
            puts[0].headers.get("content-md5"),
            Some(&STANDARD.encode(md5::compute(&data).0)),
            "{algorithm}"
        );
    }
}

// The MD5 checksum must not depend on the Content-MD5 header.
#[test]
fn local_single_part_md5_checksum_without_content_md5_header() {
    let data = test_data(12_345, 2);
    let file = write_temp_file(&data);

    let stub = spawn_stub("MD5", StubOptions::default());
    let (code, stderr) = run_cp(
        &stub,
        file.path().to_str().unwrap(),
        &[
            "--additional-checksum-algorithm",
            "MD5",
            "--disable-content-md5-header",
        ],
        None,
    );
    assert_eq!(code, 0, "{stderr}");

    let requests = stub.requests();
    let put = requests.iter().find(|r| r.is_put_object()).unwrap();
    assert!(!put.headers.contains_key("content-md5"));
    assert_request_checksum(put, "MD5");
}

#[test]
fn local_single_part_checksum_mismatch_is_an_error() {
    let data = test_data(4_096, 3);
    let file = write_temp_file(&data);

    for algorithm in NEW_ALGORITHMS {
        let stub = spawn_stub(
            algorithm,
            StubOptions {
                wrong_final_checksum: Some(checksum_of(algorithm, b"other data")),
            },
        );
        let (code, stderr) = run_cp(
            &stub,
            file.path().to_str().unwrap(),
            &["--additional-checksum-algorithm", algorithm],
            None,
        );
        assert_eq!(code, 1, "{algorithm}: {stderr}");
        assert!(
            stderr.contains("additional checksum mismatch"),
            "{algorithm}: {stderr}"
        );
    }
}

// ---------------------------------------------------------------------------
// Local file, multipart (CreateMultipartUpload / UploadPart / Complete).
// ---------------------------------------------------------------------------

#[test]
fn local_multipart_sends_part_checksums_of_every_composite_algorithm() {
    let data = test_data(2 * 5 * MIB + 1_234, 4);
    let file = write_temp_file(&data);
    let expected_parts: Vec<&[u8]> = data.chunks(5 * MIB).collect();

    for algorithm in COMPOSITE_ALGORITHMS {
        let stub = spawn_stub(algorithm, StubOptions::default());
        let (code, stderr) = run_cp(
            &stub,
            file.path().to_str().unwrap(),
            &[
                "--additional-checksum-algorithm",
                algorithm,
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
            ],
            None,
        );
        // Success also means that s3util's own composite checksum of the file
        // matched the one the stub calculated from the part checksums.
        assert_eq!(code, 0, "{algorithm}: {stderr}");
        assert_multipart_upload(&stub, algorithm, &expected_parts);
    }
}

#[test]
fn local_multipart_md5_checksum_without_content_md5_header() {
    let data = test_data(5 * MIB + 77, 5);
    let file = write_temp_file(&data);
    let expected_parts: Vec<&[u8]> = data.chunks(5 * MIB).collect();

    let stub = spawn_stub("MD5", StubOptions::default());
    let (code, stderr) = run_cp(
        &stub,
        file.path().to_str().unwrap(),
        &[
            "--additional-checksum-algorithm",
            "MD5",
            "--disable-content-md5-header",
            "--multipart-threshold",
            "5MiB",
            "--multipart-chunksize",
            "5MiB",
        ],
        None,
    );
    assert_eq!(code, 0, "{stderr}");
    assert_multipart_upload(&stub, "MD5", &expected_parts);
    for part in stub.requests().iter().filter(|r| r.is_upload_part()) {
        assert!(!part.headers.contains_key("content-md5"));
    }
}

#[test]
fn local_multipart_checksum_mismatch_is_an_error() {
    let data = test_data(5 * MIB + 10, 6);
    let file = write_temp_file(&data);

    for algorithm in NEW_ALGORITHMS {
        let stub = spawn_stub(
            algorithm,
            StubOptions {
                wrong_final_checksum: Some(format!("{}-2", checksum_of(algorithm, b"other data"))),
            },
        );
        let (code, stderr) = run_cp(
            &stub,
            file.path().to_str().unwrap(),
            &[
                "--additional-checksum-algorithm",
                algorithm,
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
            ],
            None,
        );
        assert_eq!(code, 1, "{algorithm}: {stderr}");
        assert!(
            stderr.contains("additional checksum mismatch"),
            "{algorithm}: {stderr}"
        );
    }
}

// ---------------------------------------------------------------------------
// stdin: buffered single part, and the streaming multipart path.
// ---------------------------------------------------------------------------

#[test]
fn stdin_single_part_sends_checksum_of_new_algorithms() {
    let data = test_data(33_333, 7);

    for algorithm in NEW_ALGORITHMS {
        let stub = spawn_stub(algorithm, StubOptions::default());
        let (code, stderr) = run_cp(
            &stub,
            "-",
            &["--additional-checksum-algorithm", algorithm],
            Some(&data),
        );
        assert_eq!(code, 0, "{algorithm}: {stderr}");

        let requests = stub.requests();
        let put = requests.iter().find(|r| r.is_put_object()).unwrap();
        assert_eq!(put.body, data, "{algorithm}");
        assert_request_checksum(put, algorithm);
    }
}

#[test]
fn stdin_multipart_streaming_sends_part_checksums_of_new_algorithms() {
    let data = test_data(2 * 5 * MIB + 999, 8);
    let expected_parts: Vec<&[u8]> = data.chunks(5 * MIB).collect();

    for algorithm in NEW_ALGORITHMS {
        let stub = spawn_stub(algorithm, StubOptions::default());
        let (code, stderr) = run_cp(
            &stub,
            "-",
            &[
                "--additional-checksum-algorithm",
                algorithm,
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
            ],
            Some(&data),
        );
        assert_eq!(code, 0, "{algorithm}: {stderr}");
        assert_multipart_upload(&stub, algorithm, &expected_parts);
    }
}

#[test]
fn stdin_multipart_streaming_checksum_mismatch_is_an_error() {
    let data = test_data(5 * MIB + 10, 9);

    for algorithm in NEW_ALGORITHMS {
        let stub = spawn_stub(
            algorithm,
            StubOptions {
                wrong_final_checksum: Some(format!("{}-2", checksum_of(algorithm, b"other data"))),
            },
        );
        let (code, stderr) = run_cp(
            &stub,
            "-",
            &[
                "--additional-checksum-algorithm",
                algorithm,
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
            ],
            Some(&data),
        );
        assert_eq!(code, 1, "{algorithm}: {stderr}");
        assert!(
            stderr.contains("additional checksum mismatch"),
            "{algorithm}: {stderr}"
        );
    }
}
