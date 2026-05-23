#![cfg(e2e_test)]

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::io::Read;
    use std::process::{Command, Stdio};

    fn run_s3util(args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("spawn s3util")
    }

    /// HTTP GET via `ureq` and return `(http_status, body_bytes)`. Used to
    /// fetch a presigned URL and assert both the status code (success vs.
    /// expired/AccessDenied) and the body content. Going through ureq (with
    /// rustls + bundled webpki-roots) keeps these tests portable across
    /// Linux / macOS / Windows e2e runs without depending on a system
    /// `curl` binary.
    fn http_get(url: &str) -> (u16, Vec<u8>) {
        match ureq::get(url).call() {
            Ok(mut resp) => {
                let status = resp.status();
                let mut body = Vec::new();
                resp.body_mut()
                    .as_reader()
                    .read_to_end(&mut body)
                    .expect("read presigned-URL body");
                (status.into(), body)
            }
            // ureq returns Err for any 4xx/5xx; we still want the status code
            // so the test can distinguish `expired` (403) from `missing` (404).
            Err(ureq::Error::StatusCode(code)) => (code, Vec::new()),
            Err(e) => panic!("HTTP transport error fetching presigned URL: {e}"),
        }
    }

    /// presign an object that exists, then GET the URL and assert the body.
    /// Also asserts the URL has the SigV4 query shape and the bucket/key
    /// embedded — pinning the structure so a future SDK change that produced
    /// a malformed URL would fail loudly, not just at the HTTP-fetch step.
    #[tokio::test]
    async fn presign_get_returns_url_that_downloads_object_body() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "presign-test.txt";
        let body = b"hello presigned world".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "presign",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);

        // Capture the URL before the cascade-delete so the URL itself
        // remains stable regardless of teardown order.
        let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let success = out.status.success();
        let status_code = out.status.code();

        // Fetch first, then teardown — fetching after delete would race
        // S3's eventually-consistent path.
        let (http_status, fetched_body) = if success {
            http_get(&url)
        } else {
            (0, Vec::new())
        };

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(success, "presign should succeed; stderr: {stderr}");
        assert_eq!(status_code, Some(0));
        assert!(
            url.starts_with("https://"),
            "presigned URL must be HTTPS; got: {url}"
        );
        assert!(
            url.contains(&bucket),
            "presigned URL must contain the bucket name; got: {url}"
        );
        assert!(
            url.contains(key),
            "presigned URL must contain the key; got: {url}"
        );
        assert!(
            url.contains("X-Amz-Signature="),
            "presigned URL must carry a SigV4 signature; got: {url}"
        );
        assert!(
            url.contains("X-Amz-Expires=3600"),
            "default --expires-in must be 3600; got: {url}"
        );
        assert_eq!(http_status, 200, "GET on presigned URL must return 200");
        assert_eq!(
            fetched_body, body,
            "presigned URL must return the original object body"
        );
    }

    /// `--expires-in N` must be reflected in the `X-Amz-Expires` query param.
    #[tokio::test]
    async fn presign_expires_in_propagates_to_url_query() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "expires-test.txt";
        helper.put_object(&bucket, key, b"x".to_vec()).await;

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "presign",
            "--target-profile",
            "s3util-e2e-test",
            "--expires-in",
            "120",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "presign should succeed; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
        assert!(
            url.contains("X-Amz-Expires=120"),
            "presigned URL must carry the requested --expires-in; got: {url}"
        );
    }

    /// Presigning is a local-only operation — no S3 call is made — so a URL
    /// for a non-existent key should still be generated successfully. Fetching
    /// it should then return 404 NoSuchKey.
    #[tokio::test]
    async fn presign_for_missing_key_succeeds_then_get_404s() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let out = run_s3util(&[
            "presign",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);

        let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let success = out.status.success();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();

        let http_status = if success { http_get(&url).0 } else { 0 };

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            success,
            "presign for missing key must succeed (signing is local); stderr: {stderr}"
        );
        assert_eq!(
            http_status, 404,
            "fetching presigned URL for a missing key must return 404"
        );
    }

    /// presign on a non-existent bucket must also succeed at signing time —
    /// the SDK never calls S3 for presign — and 404 at fetch time.
    #[tokio::test]
    async fn presign_for_missing_bucket_succeeds_then_get_404s() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let object_arg = format!("s3://{nonexistent}/key");
        let out = run_s3util(&[
            "presign",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);

        assert!(
            out.status.success(),
            "presign for missing bucket must succeed (signing is local); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let url = String::from_utf8_lossy(&out.stdout).trim().to_string();
        let (http_status, _) = http_get(&url);
        assert_eq!(
            http_status, 404,
            "fetching presigned URL for missing bucket must return 404"
        );
    }

    /// A 1-second URL must reject (HTTP 403 AccessDenied) once the validity
    /// window has elapsed. Confirms the X-Amz-Expires value is enforced
    /// server-side, not just included as cosmetic metadata.
    #[tokio::test]
    async fn presign_short_expiry_url_rejected_after_window() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "short-expiry.txt";
        helper.put_object(&bucket, key, b"transient".to_vec()).await;

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "presign",
            "--target-profile",
            "s3util-e2e-test",
            "--expires-in",
            "1",
            &object_arg,
        ]);
        let success = out.status.success();
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let url = String::from_utf8_lossy(&out.stdout).trim().to_string();

        // Sleep past the 1s window. Two seconds of slack covers clock skew
        // between the local machine and S3's signing-time check.
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        let http_status = if success { http_get(&url).0 } else { 0 };

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(success, "presign should succeed; stderr: {stderr}");
        assert_eq!(
            http_status, 403,
            "expired presigned URL must return 403 AccessDenied; got: {http_status}"
        );
    }

    /// presign with the `Commands::Presign` dispatch path — bucket-only
    /// (no key) target. `bucket_key()` rejects this post-parse, so the
    /// command exits 1 (validation error) rather than producing a URL.
    #[tokio::test]
    async fn presign_bucket_only_target_exits_1() {
        let bucket = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let object_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "presign",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);
        assert!(
            !out.status.success(),
            "presign on bucket-only path must fail"
        );
        assert_eq!(
            out.status.code(),
            Some(1),
            "bucket-only path should exit 1 (validation)"
        );
    }
}
