//! E2E: `--source-no-sign-request` against a real AWS public bucket.
//!
//! Uses `s3://nyc-tlc/misc/taxi_zone_lookup.csv` (~12 KiB) from the AWS
//! Registry of Open Data. Requires network access; does NOT require AWS
//! credentials or config — that is the whole point of the test.
//!
//! If nyc-tlc is ever retired, replace with another Registry of Open Data
//! object and update the size lower bound below.
#![cfg(e2e_test)]

use std::process::{Command, Stdio};

#[test]
fn cp_from_public_bucket_without_credentials() {
    let bin = env!("CARGO_BIN_EXE_s3util");
    let tmp = tempfile::NamedTempFile::new().unwrap();

    let output = Command::new(bin)
        .args([
            "cp",
            "--source-no-sign-request",
            "--source-region",
            "us-east-1",
            "s3://nyc-tlc/misc/taxi_zone_lookup.csv",
            tmp.path().to_str().unwrap(),
        ])
        // Intentionally clear any AWS_* env vars that could interfere with
        // the "no credentials" guarantee the flag makes.
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_PROFILE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "s3util cp failed.\nstatus: {:?}\nstderr:\n{stderr}",
        output.status.code(),
    );

    let len = std::fs::metadata(tmp.path()).unwrap().len();
    assert!(
        len > 1_000,
        "downloaded file suspiciously small: {len} bytes\nstderr:\n{stderr}",
    );
}
