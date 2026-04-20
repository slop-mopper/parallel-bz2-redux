// error.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

use thiserror::Error;

/// Errors produced by the parallel bzip2 library.
#[derive(Debug, Error)]
pub enum Bz2Error
{
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),

	#[error("invalid bzip2 format: {0}")]
	InvalidFormat(String),

	#[error("decompression failed at bit offset {offset}: {source}")]
	DecompressionFailed
	{
		offset: u64, source: std::io::Error
	},

	#[error("CRC mismatch at bit offset {offset}: stored={stored:#010x} computed={computed:#010x}")]
	CrcMismatch
	{
		offset: u64, stored: u32, computed: u32
	},

	#[error("memory mapping failed: {0}")]
	MmapFailed(std::io::Error),
}

pub type Result<T> = std::result::Result<T, Bz2Error>;
