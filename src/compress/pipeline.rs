// compress/pipeline.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Parallel compression pipeline: splitter + Rayon pool + bit assembler.
//!
//! Splits input data into block-sized chunks, compresses each chunk in
//! parallel via Rayon, and assembles the compressed blocks into a valid
//! bzip2 stream.
//!
//! # Architecture
//!
//! ```text
//! [Input &[u8]]
//!     | split_blocks(data, level) → Vec<&[u8]>
//!     v
//! [Rayon par_iter: compress_block() each chunk independently]
//!     | Vec<CompressedBlock> (ordered, indexed)
//!     v
//! [Assembler: assemble_stream()]
//!     | Write "BZh" + level          (4 bytes, byte-aligned)
//!     | Concatenate block bits       (bit-level via BitWriter)
//!     | Compute stream CRC           (rol1 ^ block_crc for each block)
//!     | Write EOS magic + stream CRC (48 + 32 bits)
//!     | Pad to byte boundary         (0–7 bits)
//!     v
//! [Output Vec<u8>: complete bzip2 stream]
//! ```

use rayon::prelude::*;

use crate::bits::BitWriter;
use crate::compress::block::CompressedBlock;
use crate::compress::block::compress_block;
use crate::compress::block::max_block_bytes;
use crate::crc::combine_stream_crc;
use crate::error::Bz2Error;
use crate::error::Result;

/// The 48-bit end-of-stream magic: `0x177245385090`.
const EOS_MAGIC: u64 = 0x177245385090;

// ── Splitter ───────────────────────────────────────────────────────

/// Split input data into block-sized chunks for compression.
///
/// Each chunk is at most [`max_block_bytes(level)`] bytes, which is the
/// effective maximum that libbzip2 can fit into a single block at the
/// given compression level.
///
/// Returns an empty `Vec` for empty input.
pub fn split_blocks(data: &[u8], level: u8) -> Vec<&[u8]>
{
	if data.is_empty() {
		return Vec::new();
	}
	let block_size = max_block_bytes(level);
	data.chunks(block_size).collect()
}

// ── Parallel compression ───────────────────────────────────────────

/// Compress multiple data chunks in parallel using Rayon.
///
/// Each chunk is compressed independently via [`compress_block()`].
/// Results are returned in the same order as the input chunks.
///
/// # Errors
///
/// Returns the first error encountered by any worker.  Rayon's
/// `par_iter().map().collect::<Result<_>>()` short-circuits on the
/// first `Err`.
pub fn compress_blocks_parallel(chunks: &[&[u8]], level: u8) -> Result<Vec<CompressedBlock>>
{
	chunks.par_iter().map(|chunk| compress_block(chunk, level)).collect()
}

// ── Assembler ──────────────────────────────────────────────────────

/// Assemble a complete bzip2 stream from compressed blocks.
///
/// Writes the 4-byte stream header, concatenates all block bits
/// (bit-level using [`BitWriter`]), computes the stream CRC, and
/// appends the EOS marker with CRC and padding.
///
/// If `blocks` is empty, produces a minimal valid bzip2 stream
/// containing only the header and EOS marker (matching the output
/// of compressing empty data with standard `bzip2`).
pub fn assemble_stream(blocks: &[CompressedBlock], level: u8) -> Vec<u8>
{
	// Estimate output size: header(4) + block bytes + EOS/CRC/pad(~11).
	let total_block_bits: u64 = blocks.iter().map(|b| b.bit_len).sum();
	let est_bytes = 4 + ((total_block_bits + 7) / 8) as usize + 11;
	let mut writer = BitWriter::with_capacity(est_bytes);

	// ── Stream header: "BZh" + level digit ─────────────────────────
	writer.write_bytes(&[b'B', b'Z', b'h', b'0' + level]);

	// ── Block bits + stream CRC ────────────────────────────────────
	let mut stream_crc: u32 = 0;
	for block in blocks {
		writer.copy_bits_from(&block.bits, 0, block.bit_len);
		stream_crc = combine_stream_crc(stream_crc, block.block_crc);
	}

	// ── EOS marker + stream CRC + padding ──────────────────────────
	writer.write_bits(EOS_MAGIC, 48);
	writer.write_bits(stream_crc as u64, 32);
	writer.pad_to_byte();

	writer.into_bytes()
}

// ── High-level entry point ─────────────────────────────────────────

/// Compress data into a complete bzip2 stream in parallel.
///
/// This is the high-level entry point that combines splitting,
/// parallel compression, and stream assembly.
///
/// # Arguments
///
/// * `data` — uncompressed input (may be empty).
/// * `level` — bzip2 compression level (`1`–`9`).
///
/// # Returns
///
/// A complete bzip2 stream as a byte vector, decompressible by any
/// standard bzip2 implementation.
///
/// # Errors
///
/// - [`Bz2Error::InvalidFormat`] if `level` is out of range.
/// - [`Bz2Error::Io`] if libbzip2 fails internally during compression.
pub fn compress_parallel(data: &[u8], level: u8) -> Result<Vec<u8>>
{
	if !(1..=9).contains(&level) {
		return Err(Bz2Error::InvalidFormat(format!(
			"compression level must be 1-9, got {level}"
		)));
	}

	let chunks = split_blocks(data, level);
	if chunks.is_empty() {
		// Empty input → valid stream with header + EOS only.
		return Ok(assemble_stream(&[], level));
	}

	let blocks = compress_blocks_parallel(&chunks, level)?;
	Ok(assemble_stream(&blocks, level))
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use std::io::Read;
	use std::sync::Arc;

	use super::*;
	use crate::compress::block::max_block_bytes;
	use crate::crc::bz2_crc32;
	use crate::decompress::ParBz2Decoder;

	// ── Helpers ─────────────────────────────────────────────────────

	/// Decompress a bzip2 stream using the standard bzip2 crate.
	fn reference_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let cursor = std::io::Cursor::new(compressed);
		let mut dec = bzip2::read::BzDecoder::new(cursor);
		let mut out = Vec::new();
		dec.read_to_end(&mut out).expect("reference decompression failed");
		out
	}

	/// Decompress using our ParBz2Decoder.
	fn par_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut out = Vec::new();
		decoder.read_to_end(&mut out).unwrap();
		out
	}

	/// Simple deterministic PRNG for reproducible test data.
	fn lcg_bytes(seed: u64, len: usize) -> Vec<u8>
	{
		let mut rng = seed;
		(0..len)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect()
	}

	// ── split_blocks ────────────────────────────────────────────────

	#[test]
	fn test_split_blocks_empty()
	{
		assert!(split_blocks(b"", 9).is_empty());
	}

	#[test]
	fn test_split_blocks_single()
	{
		let data = b"Hello";
		let chunks = split_blocks(data, 9);
		assert_eq!(chunks.len(), 1);
		assert_eq!(chunks[0], b"Hello");
	}

	#[test]
	fn test_split_blocks_exact_boundary()
	{
		let max = max_block_bytes(1);
		let data = vec![0u8; max];
		let chunks = split_blocks(&data, 1);
		assert_eq!(chunks.len(), 1);
		assert_eq!(chunks[0].len(), max);
	}

	#[test]
	fn test_split_blocks_multiple()
	{
		let max = max_block_bytes(1);
		let data = vec![0u8; max * 3 + 100];
		let chunks = split_blocks(&data, 1);
		assert_eq!(chunks.len(), 4);
		assert_eq!(chunks[0].len(), max);
		assert_eq!(chunks[1].len(), max);
		assert_eq!(chunks[2].len(), max);
		assert_eq!(chunks[3].len(), 100);
	}

	#[test]
	fn test_split_blocks_all_levels()
	{
		let data = vec![0u8; 500_000];
		for level in 1..=9u8 {
			let max = max_block_bytes(level);
			let chunks = split_blocks(&data, level);
			// All chunks except possibly the last should be max size.
			for chunk in &chunks[..chunks.len().saturating_sub(1)] {
				assert_eq!(chunk.len(), max, "level {level}: non-final chunk wrong size");
			}
			// Concatenation covers all input.
			let total: usize = chunks.iter().map(|c| c.len()).sum();
			assert_eq!(total, 500_000, "level {level}: split doesn't cover all input");
		}
	}

	// ── assemble_stream ─────────────────────────────────────────────

	#[test]
	fn test_assemble_empty_stream()
	{
		let stream = assemble_stream(&[], 9);
		// Must start with "BZh9".
		assert_eq!(&stream[..4], b"BZh9");
		// Must be decompressible.
		let output = reference_decompress(&stream);
		assert!(output.is_empty());
	}

	#[test]
	fn test_assemble_single_block()
	{
		let original = b"Single block assembly test.";
		let block = compress_block(original, 9).unwrap();
		let stream = assemble_stream(&[block], 9);

		assert_eq!(&stream[..4], b"BZh9");
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_assemble_multiple_blocks()
	{
		let chunk1 = b"First block of data.";
		let chunk2 = b"Second block of data.";
		let b1 = compress_block(chunk1, 9).unwrap();
		let b2 = compress_block(chunk2, 9).unwrap();
		let stream = assemble_stream(&[b1, b2], 9);

		assert_eq!(&stream[..4], b"BZh9");
		let output = reference_decompress(&stream);

		let mut expected = Vec::new();
		expected.extend_from_slice(chunk1);
		expected.extend_from_slice(chunk2);
		assert_eq!(output, expected);
	}

	#[test]
	fn test_assemble_stream_crc_correct()
	{
		// Verify the stream CRC is correct by decomposing it.
		let chunk1 = b"CRC test one.";
		let chunk2 = b"CRC test two.";
		let b1 = compress_block(chunk1, 9).unwrap();
		let b2 = compress_block(chunk2, 9).unwrap();

		let expected_crc = combine_stream_crc(combine_stream_crc(0, b1.block_crc), b2.block_crc);

		let stream = assemble_stream(&[b1, b2], 9);

		// Decompress with our decoder to get the stored + computed CRC.
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(stream)).unwrap();
		let mut out = Vec::new();
		decoder.read_to_end(&mut out).unwrap();
		assert_eq!(decoder.stream_crc(), expected_crc);
		assert_eq!(decoder.stored_stream_crc(), expected_crc);
	}

	// ── compress_parallel ───────────────────────────────────────────

	#[test]
	fn test_compress_parallel_empty()
	{
		let stream = compress_parallel(b"", 9).unwrap();
		assert_eq!(&stream[..4], b"BZh9");
		let output = reference_decompress(&stream);
		assert!(output.is_empty());
	}

	#[test]
	fn test_compress_parallel_small_text()
	{
		let original = b"Hello, parallel bzip2 compression!";
		let stream = compress_parallel(original, 9).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_medium_text()
	{
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(8192).collect();
		let stream = compress_parallel(&original, 9).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_multi_block()
	{
		// 250KB random at level 1 → forces 3+ blocks.
		let original = lcg_bytes(0xCAFE_BABE, 250_000);
		let stream = compress_parallel(&original, 1).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_all_levels()
	{
		let original = b"Level sweep test data - enough bytes to compress reliably.";
		for level in 1..=9u8 {
			let stream = compress_parallel(original, level).unwrap();
			assert_eq!(&stream[..3], b"BZh", "level {level}: bad header");
			assert_eq!(stream[3], b'0' + level, "level {level}: bad level byte");
			let output = reference_decompress(&stream);
			assert_eq!(output, original.as_slice(), "level {level}: roundtrip mismatch");
		}
	}

	#[test]
	fn test_compress_parallel_single_byte()
	{
		let original = [0x42u8];
		let stream = compress_parallel(&original, 9).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, &original);
	}

	#[test]
	fn test_compress_parallel_all_zeros()
	{
		let original = vec![0u8; 8192];
		let stream = compress_parallel(&original, 9).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_all_same_byte()
	{
		let original = vec![0xBBu8; 8192];
		let stream = compress_parallel(&original, 5).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_binary_all_values()
	{
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		let stream = compress_parallel(&original, 9).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_random_data()
	{
		let original = lcg_bytes(0xDEAD_BEEF, 4096);
		let stream = compress_parallel(&original, 7).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	// ── Roundtrip with ParBz2Decoder ────────────────────────────────

	#[test]
	fn test_roundtrip_small()
	{
		let original = b"Roundtrip test: our encoder -> our decoder.";
		let stream = compress_parallel(original, 9).unwrap();
		let output = par_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_roundtrip_multi_block()
	{
		let original = lcg_bytes(0x1234_5678, 250_000);
		let stream = compress_parallel(&original, 1).unwrap();
		let output = par_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_roundtrip_all_levels()
	{
		let original = b"Full roundtrip through all compression levels.";
		for level in 1..=9u8 {
			let stream = compress_parallel(original, level).unwrap();
			let output = par_decompress(&stream);
			assert_eq!(output, original.as_slice(), "level {level}: roundtrip mismatch");
		}
	}

	// ── CRC consistency ─────────────────────────────────────────────

	#[test]
	fn test_block_crcs_match_direct_computation()
	{
		let data = b"Verify that block CRCs match direct bz2_crc32 computation.";
		let chunks = split_blocks(data, 9);
		let blocks = compress_blocks_parallel(&chunks, 9).unwrap();
		for (i, (chunk, block)) in chunks.iter().zip(blocks.iter()).enumerate() {
			assert_eq!(block.block_crc, bz2_crc32(chunk), "block {i}: CRC mismatch");
		}
	}

	// ── Error cases ─────────────────────────────────────────────────

	#[test]
	fn test_compress_parallel_invalid_level_zero()
	{
		let result = compress_parallel(b"test", 0);
		assert!(matches!(result, Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_compress_parallel_invalid_level_too_high()
	{
		let result = compress_parallel(b"test", 10);
		assert!(matches!(result, Err(Bz2Error::InvalidFormat(_))));
	}

	// ── Exact block boundary ────────────────────────────────────────

	#[test]
	fn test_compress_parallel_exact_block_size()
	{
		// Data exactly equal to one block → exactly 1 block, no remainder.
		let max = max_block_bytes(1);
		let original: Vec<u8> = (0..max).map(|i| (i % 251) as u8).collect();
		let stream = compress_parallel(&original, 1).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_exact_two_blocks()
	{
		// Data exactly 2× block size → exactly 2 blocks.
		let max = max_block_bytes(1);
		let original: Vec<u8> = (0..max * 2).map(|i| (i % 251) as u8).collect();
		let stream = compress_parallel(&original, 1).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_compress_parallel_block_plus_one()
	{
		// Data = block_size + 1 → 2 blocks (1 full + 1 single-byte).
		let max = max_block_bytes(1);
		let original: Vec<u8> = (0..max + 1).map(|i| (i % 251) as u8).collect();
		let chunks = split_blocks(&original, 1);
		assert_eq!(chunks.len(), 2);
		assert_eq!(chunks[0].len(), max);
		assert_eq!(chunks[1].len(), 1);

		let stream = compress_parallel(&original, 1).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	// ── Cross-compatibility ─────────────────────────────────────────

	#[test]
	fn test_our_compress_reference_decompress()
	{
		// Our compressor → standard bzip2 decompressor.
		let original = lcg_bytes(0xFACE_FEED, 50_000);
		let stream = compress_parallel(&original, 6).unwrap();
		let output = reference_decompress(&stream);
		assert_eq!(output, original);
	}

	#[test]
	fn test_reference_compress_our_decompress()
	{
		// Standard bzip2 compressor → our decompressor.
		use std::io::Write;

		let original = lcg_bytes(0xBEEF_CAFE, 50_000);
		let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(6));
		enc.write_all(&original).unwrap();
		let compressed = enc.finish().unwrap();

		let output = par_decompress(&compressed);
		assert_eq!(output, original);
	}
}
