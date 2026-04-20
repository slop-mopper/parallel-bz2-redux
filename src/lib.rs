// lib.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

// Enable the coverage(off) attribute under cargo-llvm-cov so that
// test-only helpers don't inflate coverage metrics.
#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Correct, high-performance parallel bzip2 compression and decompression.
//!
//! # Decompression
//!
//! ```no_run
//! use std::io::Read;
//! use parallel_bz2_redux::ParBz2Decoder;
//!
//! // From a file:
//! let mut decoder = ParBz2Decoder::open("file.bz2").unwrap();
//! let mut output = Vec::new();
//! decoder.read_to_end(&mut output).unwrap();
//! ```

pub(crate) mod bits;
pub mod compress;
pub(crate) mod crc;
pub mod decompress;
pub mod error;
pub(crate) mod scanner;

pub use compress::ParBz2Encoder;
pub use compress::ParBz2EncoderBuilder;
pub use decompress::ParBz2Decoder;
pub use decompress::ParBz2DecoderBuilder;
pub use rayon::ThreadPool;
