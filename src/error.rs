// error.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Error types for the parallel bzip2 library.

use thiserror::Error;

/// Errors produced by the parallel bzip2 library.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Bz2Error
{
	/// An I/O error occurred while reading or writing data.
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),

	/// The input is not valid bzip2 data (e.g. missing or corrupt header).
	#[error("invalid bzip2 format: {0}")]
	InvalidFormat(String),

	/// A block could not be decompressed.
	///
	/// This typically indicates corrupted compressed data or a false-positive
	/// block boundary that could not be resolved by the merge-and-retry logic.
	#[error("decompression failed at bit offset {offset}: {source}")]
	DecompressionFailed
	{
		/// Bit offset in the compressed data where the block starts.
		offset: u64,
		/// The underlying decompression error from libbzip2.
		source: std::io::Error,
	},

	/// A block's stored CRC-32 does not match the CRC computed over
	/// the decompressed output.
	#[error("CRC mismatch at bit offset {offset}: stored={stored:#010x} computed={computed:#010x}")]
	CrcMismatch
	{
		/// Bit offset in the compressed data where the block starts.
		offset: u64,
		/// CRC-32 stored in the block header.
		stored: u32,
		/// CRC-32 computed over the decompressed data.
		computed: u32,
	},
}

/// A specialized [`Result`](std::result::Result) type for parallel bzip2 operations.
pub type Result<T> = std::result::Result<T, Bz2Error>;
