# End-to-End Tests

## Warning

These tests will create and delete AWS resources (S3 buckets and objects), which will result in costs on your AWS account.

These tests are designed to be run against a real AWS account. If any of the tests fail, they may leave resources in your AWS account, such as S3 buckets and their contents.

## Running the tests against AWS

Before running the tests, you need to set up your AWS credentials. Create a profile named `s3util-e2e-test` with the AWS CLI:

```bash
aws configure --profile s3util-e2e-test
```

Then run the tests with the `e2e_test` cfg flag:

```bash
# Run all E2E tests
RUSTFLAGS='--cfg e2e_test' cargo test --test 'e2e_*'

# Run a specific test suite
RUSTFLAGS='--cfg e2e_test' cargo test --test e2e_local_to_s3
```

### Region

The test helpers hard-code the region and the Express One Zone availability zone in `tests/common/mod.rs`:

- `REGION` — `ap-northeast-1` (used as the `LocationConstraint` when creating buckets)
- `EXPRESS_ONE_ZONE_AZ` — `apne1-az4` (used by the directory-bucket tests)

To run against a different region, edit these constants. The `s3util-e2e-test` profile's region should match `REGION`.

### Environment variables

Some tests use environment variables:

| Variable | Used by | Behavior if unset |
|---|---|---|
| `S3UTIL_E2E_REPLICATION_ROLE_ARN` | `e2e_dry_run` (`delete_bucket_replication_dry_run_does_not_change_state`) | The test fails. Set it to an IAM role ARN whose trust policy allows `s3.amazonaws.com` to `AssumeRole`, and on which the e2e profile has `iam:PassRole` permission. |
| `S3UTIL_E2E_ACCOUNT_ID` | `e2e_create_delete_bucket` (`create_and_delete_account_regional_bucket_round_trip`) | The test is skipped. Set it to your 12-digit AWS account ID to exercise account-regional bucket creation. |
| `S3UTIL_E2E_LOCATION_CONSTRAINT` | `e2e_create_delete_bucket` (`create_and_delete_account_regional_bucket_round_trip`) | Defaults to `ap-northeast-1`. Region used for the `LocationConstraint` in the account-regional bucket test. |

## Notes

These tests create and delete S3 buckets. Occasionally tests may fail due to eventual consistency in AWS (for example, a newly created bucket may not be immediately visible). In such cases, the tests will typically pass on a subsequent run.

Test data (seed files for deterministic checksums) lives in `test_data/` and is checked into the repository. Temporary working files are created under `./playground/` and are not cleaned up automatically.
