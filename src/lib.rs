//! This crate is intended to be used as a binary crate (`s3util`) and is not
//! intended for use as a library in any way. The public items below exist
//! only to support the binary and integration tests; no API stability is
//! provided and external consumers should not depend on them.

#![allow(clippy::collapsible_if)]
#![allow(clippy::assertions_on_constants)]
#![allow(clippy::unnecessary_unwrap)]

pub mod config;
pub mod input;
pub mod output;
pub mod storage;
pub mod transfer;
pub mod types;

pub use config::Config;
