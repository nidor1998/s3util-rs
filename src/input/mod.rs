//! User-supplied JSON input deserialisation for `put-*` subcommands.
//!
//! Mirrors `src/output/json.rs` in spirit: hand-written serde-derived
//! types matching the AWS-CLI input JSON shape (PascalCase) for each
//! resource that `put_bucket_*` accepts as a typed SDK struct rather
//! than a raw body.

pub mod json;
