// decompress/mod.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Parallel bzip2 decompression.
//!
//! The main entry point is [`ParBz2Decoder`], which implements [`Read`]
//! for streaming parallel decompression of bzip2 data.
//!
//! # Examples
//!
//! ```no_run
//! use std::io::Read;
//! use parallel_bz2_redux::decompress::ParBz2Decoder;
//!
//! // From a file:
//! let mut decoder = ParBz2Decoder::open("file.bz2").unwrap();
//! let mut output = Vec::new();
//! decoder.read_to_end(&mut output).unwrap();
//!
//! // From in-memory data:
//! # let compressed_data: Vec<u8> = Vec::new();
//! let data: std::sync::Arc<[u8]> = std::sync::Arc::from(compressed_data);
//! let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();
//! ```

pub mod block;
pub mod pipeline;

use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use self::pipeline::DecompressPipeline;
use self::pipeline::PipelineConfig;
use crate::bits::read_u32_at_bit;
use crate::crc::combine_stream_crc;
use crate::error::Bz2Error;
use crate::error::Result;
use crate::scanner::MarkerType;
use crate::scanner::Scanner;

// ── Header parsing ─────────────────────────────────────────────────

/// Parse the 4-byte bzip2 stream header and return the compression level (1–9).
fn parse_header(data: &[u8]) -> Result<u8>
{
	if data.len() < 4 {
		return Err(Bz2Error::InvalidFormat("data too short for bzip2 header".into()));
	}
	if &data[0..3] != b"BZh" {
		return Err(Bz2Error::InvalidFormat("missing BZh header magic".into()));
	}
	let level_byte = data[3];
	if !(b'1'..=b'9').contains(&level_byte) {
		return Err(Bz2Error::InvalidFormat(format!(
			"invalid block size level byte: {:#04x}",
			level_byte
		)));
	}
	Ok(level_byte - b'0')
}

// ── ParBz2Decoder ──────────────────────────────────────────────────

/// Parallel bzip2 decoder implementing [`Read`].
///
/// Decompresses bzip2 data using multiple threads.  Blocks are decompressed
/// in parallel via Rayon and streamed in order through bounded channels —
/// only a small window of blocks is ever in memory.
///
/// # Stream CRC verification
///
/// By default, the stream CRC is verified after all blocks have been
/// consumed.  If the CRC does not match, the final `read()` call returns
/// an I/O error with [`ErrorKind::InvalidData`](std::io::ErrorKind::InvalidData).
/// Disable this via [`ParBz2DecoderBuilder::verify_stream_crc(false)`].
pub struct ParBz2Decoder
{
	pipeline: DecompressPipeline,
	/// Current block's decompressed data.
	current_block: Vec<u8>,
	/// Read cursor within `current_block`.
	cursor: usize,
	/// Running combined stream CRC (`rol1(combined) ^ block_crc`).
	stream_crc: u32,
	/// Stream CRC stored in the bzip2 EOS marker.
	stored_stream_crc: u32,
	/// Whether to verify the stream CRC when EOF is reached.
	verify_stream_crc: bool,
	/// `true` once the pipeline is exhausted and all data has been read.
	done: bool,
}

impl ParBz2Decoder
{
	/// Open a bzip2 file for parallel decompression.
	///
	/// Reads the entire file into memory, scans for block boundaries,
	/// and starts the decompression pipeline.
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
	{
		let data = std::fs::read(path.as_ref()).map_err(Bz2Error::Io)?;
		Self::from_bytes(Arc::from(data))
	}

	/// Create a decoder from in-memory compressed data.
	pub fn from_bytes(data: Arc<[u8]>) -> Result<Self>
	{
		Self::build(data, PipelineConfig::default(), true)
	}

	/// Returns a builder for configuring decompression options.
	pub fn builder() -> ParBz2DecoderBuilder
	{
		ParBz2DecoderBuilder::new()
	}

	/// Internal constructor shared by all entry points.
	fn build(data: Arc<[u8]>, config: PipelineConfig, verify_stream_crc: bool) -> Result<Self>
	{
		let level = parse_header(&data)?;

		let scanner = Scanner::new();
		let candidates = scanner.scan_parallel(&data);

		// Read stored stream CRC from the EOS marker (32 bits after the 48-bit magic).
		let stored_stream_crc = candidates
			.iter()
			.find(|c| c.marker_type == MarkerType::Eos)
			.map(|eos| read_u32_at_bit(&data, eos.bit_offset + 48))
			.unwrap_or(0); // pair_candidates will catch missing EOS

		let pipeline = DecompressPipeline::start(data, candidates, level, config)?;

		Ok(Self {
			pipeline,
			current_block: Vec::new(),
			cursor: 0,
			stream_crc: 0,
			stored_stream_crc,
			verify_stream_crc,
			done: false,
		})
	}

	/// Returns the combined stream CRC computed from blocks consumed so far.
	pub fn stream_crc(&self) -> u32
	{
		self.stream_crc
	}

	/// Returns the stream CRC stored in the bzip2 stream's EOS marker.
	pub fn stored_stream_crc(&self) -> u32
	{
		self.stored_stream_crc
	}

	/// Pull the next block from the pipeline into `current_block`.
	///
	/// Returns `Ok(true)` if a block was received, `Ok(false)` if the
	/// pipeline is exhausted, or `Err` on decompression failure.
	fn pull_next_block(&mut self) -> std::io::Result<bool>
	{
		match self.pipeline.recv() {
			Some(Ok(block)) => {
				self.stream_crc = combine_stream_crc(self.stream_crc, block.crc);
				self.current_block = block.data;
				self.cursor = 0;
				Ok(true)
			}
			Some(Err(e)) => {
				self.done = true;
				Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))
			}
			None => Ok(false),
		}
	}
}

impl Read for ParBz2Decoder
{
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>
	{
		if self.done || buf.is_empty() {
			return Ok(0);
		}

		loop {
			// Drain the current block.
			let remaining = self.current_block.len() - self.cursor;
			if remaining > 0 {
				let n = remaining.min(buf.len());
				buf[..n].copy_from_slice(&self.current_block[self.cursor..self.cursor + n]);
				self.cursor += n;

				// Free block memory once fully consumed.
				if self.cursor == self.current_block.len() {
					self.current_block = Vec::new();
					self.cursor = 0;
				}

				return Ok(n);
			}

			// Current block exhausted — pull the next one.
			if !self.pull_next_block()? {
				// Pipeline exhausted.  Verify stream CRC if requested.
				self.done = true;
				if self.verify_stream_crc && self.stream_crc != self.stored_stream_crc {
					return Err(std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!(
							"stream CRC mismatch: stored={:#010x} computed={:#010x}",
							self.stored_stream_crc, self.stream_crc
						),
					));
				}
				return Ok(0);
			}
		}
	}
}

// ── Builder ────────────────────────────────────────────────────────

/// Builder for configuring a [`ParBz2Decoder`].
///
/// Obtain via [`ParBz2Decoder::builder()`].
pub struct ParBz2DecoderBuilder
{
	config: PipelineConfig,
	verify_stream_crc: bool,
}

impl ParBz2DecoderBuilder
{
	fn new() -> Self
	{
		Self { config: PipelineConfig::default(), verify_stream_crc: true }
	}

	/// Set the pipeline channel capacities.
	///
	/// See [`PipelineConfig`] for details on how channel capacities affect
	/// memory usage and throughput.
	pub fn pipeline_config(mut self, config: PipelineConfig) -> Self
	{
		self.config = config;
		self
	}

	/// Whether to verify the stream CRC after all blocks are consumed.
	///
	/// When `true` (the default), the final `read()` call that would
	/// otherwise return `Ok(0)` returns an error if the stream CRC does
	/// not match.
	pub fn verify_stream_crc(mut self, verify: bool) -> Self
	{
		self.verify_stream_crc = verify;
		self
	}

	/// Open a bzip2 file for parallel decompression.
	pub fn open<P: AsRef<Path>>(self, path: P) -> Result<ParBz2Decoder>
	{
		let data = std::fs::read(path.as_ref()).map_err(Bz2Error::Io)?;
		ParBz2Decoder::build(Arc::from(data), self.config, self.verify_stream_crc)
	}

	/// Build a decoder from in-memory compressed data.
	pub fn from_bytes(self, data: Arc<[u8]>) -> Result<ParBz2Decoder>
	{
		ParBz2Decoder::build(data, self.config, self.verify_stream_crc)
	}
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests
{
	use std::io::Write;

	use super::*;

	// ── Helpers ─────────────────────────────────────────────────────

	fn compress(data: &[u8], level: u32) -> Vec<u8>
	{
		let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
		enc.write_all(data).unwrap();
		enc.finish().unwrap()
	}

	fn reference_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let mut dec = bzip2::read::BzDecoder::new(std::io::Cursor::new(compressed));
		let mut out = Vec::new();
		dec.read_to_end(&mut out).unwrap();
		out
	}

	// ── Header parsing ─────────────────────────────────────────────

	#[test]
	fn test_parse_header_valid()
	{
		for level in 1..=9u8 {
			let data = [b'B', b'Z', b'h', b'0' + level];
			assert_eq!(parse_header(&data).unwrap(), level);
		}
	}

	#[test]
	fn test_parse_header_too_short()
	{
		assert!(matches!(parse_header(b"BZ"), Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_parse_header_bad_magic()
	{
		assert!(matches!(parse_header(b"GZh9"), Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_parse_header_bad_level()
	{
		assert!(matches!(parse_header(b"BZh0"), Err(Bz2Error::InvalidFormat(_))));
		assert!(matches!(parse_header(b"BZhA"), Err(Bz2Error::InvalidFormat(_))));
	}

	// ── ParBz2Decoder: basic ───────────────────────────────────────

	#[test]
	fn test_decoder_small_text()
	{
		let original = b"Hello, parallel bzip2 world!";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_empty()
	{
		let compressed = compress(b"", 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert!(output.is_empty());
	}

	#[test]
	fn test_decoder_single_byte()
	{
		let original = b"X";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_medium_text()
	{
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(4096).collect();
		let compressed = compress(&original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: multi-block ─────────────────────────────────

	#[test]
	fn test_decoder_multi_block()
	{
		// ~250KB random at level 1 to force multiple blocks.
		let mut rng: u64 = 0x1234_5678_ABCD_EF01;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: all levels ──────────────────────────────────

	#[test]
	fn test_decoder_all_levels()
	{
		let original = b"Decoder level sweep test data - enough bytes to compress reliably.";
		for level in 1..=9u32 {
			let compressed = compress(original, level);
			let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
			let mut output = Vec::new();
			decoder.read_to_end(&mut output).unwrap();
			assert_eq!(output, original.as_slice(), "mismatch at level {level}");
		}
	}

	// ── ParBz2Decoder: binary data ─────────────────────────────────

	#[test]
	fn test_decoder_binary_all_values()
	{
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		let compressed = compress(&original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: small reads ─────────────────────────────────

	#[test]
	fn test_decoder_byte_at_a_time()
	{
		let original = b"Read me one byte at a time!";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		let mut byte = [0u8; 1];
		loop {
			match decoder.read(&mut byte) {
				Ok(0) => break,
				Ok(1) => output.push(byte[0]),
				Ok(n) => panic!("read returned {n} for 1-byte buffer"),
				Err(e) => panic!("unexpected error: {e}"),
			}
		}

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: stream CRC ──────────────────────────────────

	#[test]
	fn test_decoder_stream_crc_valid()
	{
		let original = b"CRC verification test data.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		// Stream CRC should match stored CRC.
		assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
	}

	#[test]
	fn test_decoder_stream_crc_matches_reference()
	{
		// For a multi-block file, verify our stream CRC matches what
		// libbzip2 produced.
		let mut rng: u64 = 0xCAFE_BABE;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
		// The stored stream CRC was produced by libbzip2 — if our
		// computed stream CRC matches, both are correct.
		assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
	}

	#[test]
	fn test_decoder_stream_crc_disabled()
	{
		let original = b"CRC disabled test.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::builder()
			.verify_stream_crc(false)
			.from_bytes(Arc::from(compressed))
			.unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: builder ─────────────────────────────────────

	#[test]
	fn test_builder_custom_config()
	{
		let original = b"Builder config test.";
		let compressed = compress(original, 5);

		let config = PipelineConfig { result_channel_cap: 2, output_channel_cap: 1 };
		let mut decoder = ParBz2Decoder::builder()
			.pipeline_config(config)
			.verify_stream_crc(true)
			.from_bytes(Arc::from(compressed))
			.unwrap();

		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: reference compatibility ─────────────────────

	#[test]
	fn test_decoder_matches_reference_decompressor()
	{
		let line = "Reference decompressor compatibility test.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(8192).collect();
		let compressed = compress(&original, 6);

		let reference = reference_decompress(&compressed);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, reference);
	}

	// ── ParBz2Decoder: repeated reads after EOF ────────────────────

	#[test]
	fn test_decoder_read_after_eof()
	{
		let original = b"EOF test.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);

		// Further reads should return 0.
		let mut buf = [0u8; 64];
		assert_eq!(decoder.read(&mut buf).unwrap(), 0);
		assert_eq!(decoder.read(&mut buf).unwrap(), 0);
	}

	// ── ParBz2Decoder: error cases ─────────────────────────────────

	#[test]
	fn test_decoder_invalid_header_not_bzip2()
	{
		let data: Arc<[u8]> = Arc::from(b"not bzip2 data at all".to_vec());
		assert!(ParBz2Decoder::from_bytes(data).is_err());
	}

	#[test]
	fn test_decoder_invalid_header_bad_level()
	{
		let data: Arc<[u8]> = Arc::from(b"BZh0xxxx".to_vec());
		assert!(matches!(
			ParBz2Decoder::from_bytes(data),
			Err(Bz2Error::InvalidFormat(_))
		));
	}

	#[test]
	fn test_decoder_truncated_header()
	{
		let data: Arc<[u8]> = Arc::from(b"BZ".to_vec());
		assert!(matches!(
			ParBz2Decoder::from_bytes(data),
			Err(Bz2Error::InvalidFormat(_))
		));
	}
}
