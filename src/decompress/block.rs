// decompress/block.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Single-block decompression with CRC validation.
//!
//! Extracts one bzip2 block from the source data, wraps it in a synthetic
//! single-block bzip2 stream, decompresses it via libbzip2, and validates
//! the block CRC.

use std::io::Read;

use crate::bits::BitWriter;
use crate::bits::read_u32_at_bit;
use crate::crc::bz2_crc32;
use crate::error::Bz2Error;
use crate::error::Result;

/// The 48-bit end-of-stream magic: `0x177245385090`.
const EOS_MAGIC: u64 = 0x177245385090;

/// Result of decompressing a single bzip2 block.
#[derive(Debug)]
pub struct BlockResult
{
	/// Decompressed block data.
	pub data: Vec<u8>,
	/// CRC stored in the block header (32 bits after the 48-bit block magic).
	pub stored_crc: u32,
	/// CRC computed over the decompressed output.
	pub computed_crc: u32,
	/// Whether `stored_crc == computed_crc`.
	pub crc_valid: bool,
}

/// Decompress a single bzip2 block and validate its CRC.
///
/// # Arguments
///
/// * `data` — full compressed file as a byte slice.
/// * `block_start_bit` — bit offset of the 48-bit block magic (`0x314159265359`).
/// * `block_end_bit` — bit offset of the next marker (block magic or EOS magic). This is exclusive: the
///   block's compressed bits span `[block_start_bit, block_end_bit)`.
/// * `level` — bzip2 compression level (`1`–`9`) from the stream header.
///
/// # Returns
///
/// `Ok(BlockResult)` on successful decompression.  The caller should inspect
/// [`BlockResult::crc_valid`] to decide whether the block is trustworthy.
///
/// `Err(Bz2Error::DecompressionFailed { .. })` if libbzip2 rejects the data
/// (false-positive scanner hit, truncation, corruption).
pub fn decompress_block(data: &[u8], block_start_bit: u64, block_end_bit: u64, level: u8) -> Result<BlockResult>
{
	debug_assert!((1..=9).contains(&level));
	debug_assert!(block_end_bit > block_start_bit);

	// ── 1. Read the stored block CRC (32 bits after the 48-bit magic). ──
	let stored_crc = read_u32_at_bit(data, block_start_bit + 48);

	// ── 2. Build a synthetic single-block bzip2 stream. ─────────────────
	let block_bits = block_end_bit - block_start_bit;
	// Estimate: 4-byte header + block bytes + 6-byte EOS + 4-byte CRC + 1 pad.
	let estimated_bytes = 4 + ((block_bits + 7) / 8) as usize + 11;
	let mut writer = BitWriter::with_capacity(estimated_bytes);

	// Stream header: "BZh" + level digit.
	writer.write_bytes(&[b'B', b'Z', b'h', b'0' + level]);

	// Block bits verbatim (includes the block magic, CRC, and payload).
	writer.copy_bits_from(data, block_start_bit, block_bits);

	// End-of-stream marker (48 bits).
	writer.write_bits(EOS_MAGIC, 48);

	// Stream CRC: for a single-block stream, stream_crc = block_crc.
	// (combine_stream_crc(0, block_crc) = rotate_left(0,1) ^ block_crc = block_crc)
	writer.write_bits(stored_crc as u64, 32);

	// Pad to byte boundary.
	writer.pad_to_byte();

	let synthetic = writer.into_bytes();

	// ── 3. Decompress via libbzip2. ─────────────────────────────────────
	let cursor = std::io::Cursor::new(&synthetic);
	let mut decoder = bzip2::read::BzDecoder::new(cursor);
	let mut decompressed = Vec::new();

	decoder
		.read_to_end(&mut decompressed)
		.map_err(|e| Bz2Error::DecompressionFailed { offset: block_start_bit, source: e })?;

	// ── 4. Compute CRC over decompressed data. ─────────────────────────
	let computed_crc = bz2_crc32(&decompressed);

	Ok(BlockResult {
		data: decompressed,
		stored_crc,
		computed_crc,
		crc_valid: stored_crc == computed_crc,
	})
}

#[cfg(test)]
mod tests
{
	use std::io::Write;

	use super::*;
	use crate::crc::bz2_crc32;
	use crate::scanner::MarkerType;
	use crate::scanner::Scanner;

	// ── Helpers ──────────────────────────────────────────────────────

	/// Compress `data` with the given bzip2 level (1–9).
	fn compress_to_bz2(data: &[u8], level: u32) -> Vec<u8>
	{
		let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
		encoder.write_all(data).expect("bz2 compress write");
		encoder.finish().expect("bz2 compress finish")
	}

	/// Reference-decompress an entire bzip2 stream.
	fn reference_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let mut decoder = bzip2::read::BzDecoder::new(std::io::Cursor::new(compressed));
		let mut out = Vec::new();
		decoder.read_to_end(&mut out).expect("reference decompress");
		out
	}

	/// Scan `compressed` and return (block_start_bits sorted, eos_bit).
	fn find_block_boundaries(compressed: &[u8]) -> (Vec<u64>, u64)
	{
		let scanner = Scanner::new();
		let candidates = scanner.scan(compressed);

		let mut block_starts: Vec<u64> = Vec::new();
		let mut eos_bit: Option<u64> = None;

		for c in &candidates {
			match c.marker_type {
				MarkerType::Block => block_starts.push(c.bit_offset),
				MarkerType::Eos => {
					assert!(eos_bit.is_none(), "multiple EOS markers");
					eos_bit = Some(c.bit_offset);
				}
			}
		}

		block_starts.sort();
		let eos = eos_bit.expect("no EOS marker found");
		(block_starts, eos)
	}

	/// Decompress a single-block bz2 stream via our function and return
	/// the `BlockResult`. Assumes exactly one block in `compressed`.
	fn decompress_single(compressed: &[u8], level: u8) -> BlockResult
	{
		let (blocks, eos_bit) = find_block_boundaries(compressed);
		assert_eq!(blocks.len(), 1, "expected 1 block, got {}", blocks.len());
		decompress_block(compressed, blocks[0], eos_bit, level).expect("decompress_block failed")
	}

	// ── Normal cases ─────────────────────────────────────────────────

	#[test]
	fn test_decompress_small_text()
	{
		let original = b"Hello, World!";
		let compressed = compress_to_bz2(original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_medium_text()
	{
		// ~4KB of repeated text.
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(4096).collect();
		let compressed = compress_to_bz2(&original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_single_byte()
	{
		let original = [0x42u8];
		let compressed = compress_to_bz2(&original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_level_1()
	{
		let original = b"Level 1 compression test data, small payload.";
		let compressed = compress_to_bz2(original, 1);
		let result = decompress_single(&compressed, 1);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_level_5()
	{
		let original = b"Level 5 compression test data, small payload.";
		let compressed = compress_to_bz2(original, 5);
		let result = decompress_single(&compressed, 5);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_all_zeros()
	{
		let original = vec![0u8; 4096];
		let compressed = compress_to_bz2(&original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_all_same_byte()
	{
		let original = vec![0xAAu8; 4096];
		let compressed = compress_to_bz2(&original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_random_bytes()
	{
		// Deterministic pseudo-random via LCG.
		let mut rng: u64 = 0xDEAD_BEEF_CAFE_BABE;
		let original: Vec<u8> = (0..1024)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();
		let compressed = compress_to_bz2(&original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	#[test]
	fn test_decompress_binary_all_values()
	{
		// Every byte value 0x00..=0xFF repeated 4 times (1024 bytes).
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		assert_eq!(original.len(), 1024);
		let compressed = compress_to_bz2(&original, 9);
		let result = decompress_single(&compressed, 9);
		assert_eq!(result.data, original);
		assert!(result.crc_valid);
	}

	// ── Multi-block integration ──────────────────────────────────────

	#[test]
	fn test_decompress_multi_block_roundtrip()
	{
		// Compress ~250KB at level 1 (100KB blocks) to force multiple blocks.
		let mut rng: u64 = 0x1234_5678_9ABC_DEF0;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress_to_bz2(&original, 1);

		// Reference decompression.
		let reference = reference_decompress(&compressed);
		assert_eq!(reference, original);

		// Scan for boundaries.
		let (block_starts, eos_bit) = find_block_boundaries(&compressed);
		assert!(
			block_starts.len() >= 2,
			"expected ≥2 blocks at level 1 with 250KB, got {}",
			block_starts.len()
		);

		// Decompress each block individually and concatenate.
		let mut reassembled = Vec::new();
		for (i, &start) in block_starts.iter().enumerate() {
			let end = if i + 1 < block_starts.len() {
				block_starts[i + 1]
			} else {
				eos_bit
			};
			let result = decompress_block(&compressed, start, end, 1)
				.unwrap_or_else(|e| panic!("block {i} (bit {start}) failed: {e}"));
			assert!(result.crc_valid, "block {i} CRC mismatch");
			reassembled.extend_from_slice(&result.data);
		}

		assert_eq!(
			reassembled.len(),
			original.len(),
			"reassembled length mismatch: {} vs {}",
			reassembled.len(),
			original.len()
		);
		assert_eq!(reassembled, original, "reassembled data differs from original");
	}

	// ── CRC validation ──────────────────────────────────────────────

	#[test]
	fn test_decompress_crc_matches_independent()
	{
		let original = b"CRC cross-check payload";
		let compressed = compress_to_bz2(original, 9);
		let result = decompress_single(&compressed, 9);

		// Our CRC over the original should match what the block header stored.
		let expected_crc = bz2_crc32(original);
		assert_eq!(result.stored_crc, expected_crc, "stored CRC vs independent CRC");
		assert_eq!(result.computed_crc, expected_crc, "computed CRC vs independent CRC");
	}

	#[test]
	fn test_decompress_crc_valid_all_levels()
	{
		let original = b"CRC valid at every level";
		for level in 1..=9u32 {
			let compressed = compress_to_bz2(original, level);
			let result = decompress_single(&compressed, level as u8);
			assert!(result.crc_valid, "crc_valid false at level {level}");
			assert_eq!(result.data, original, "data mismatch at level {level}");
		}
	}

	// ── Error cases ─────────────────────────────────────────────────

	#[test]
	fn test_decompress_invalid_range()
	{
		// Compress valid data, then point at a non-block region.
		let compressed = compress_to_bz2(b"some data", 9);
		// Bit 0 is inside the "BZh9" header, not a block magic.
		let result = decompress_block(&compressed, 0, 100, 9);
		assert!(result.is_err(), "expected DecompressionFailed for bogus range");
		assert!(
			matches!(result.unwrap_err(), Bz2Error::DecompressionFailed { .. }),
			"wrong error variant"
		);
	}

	#[test]
	fn test_decompress_truncated_block()
	{
		let original = b"Truncation test with enough data to compress.";
		let compressed = compress_to_bz2(original, 9);
		let (blocks, _eos_bit) = find_block_boundaries(&compressed);
		assert!(!blocks.is_empty());

		// Use block start but end halfway through.
		let start = blocks[0];
		let midpoint = start + ((_eos_bit - start) / 2);
		let result = decompress_block(&compressed, start, midpoint, 9);
		assert!(result.is_err(), "expected error for truncated block");
		assert!(
			matches!(result.unwrap_err(), Bz2Error::DecompressionFailed { .. }),
			"wrong error variant"
		);
	}

	#[test]
	fn test_decompress_wrong_level_too_low()
	{
		// Compress at level 9, decompress declaring level 1.
		// For small data that fits in a level-1 block, libbzip2 may
		// still succeed (block is smaller than 100KB).  We just verify
		// the function doesn't panic and the CRC is still valid if it
		// does succeed.
		let original = b"Small data that fits in any block size.";
		let compressed = compress_to_bz2(original, 9);
		let (blocks, eos_bit) = find_block_boundaries(&compressed);
		let result = decompress_block(&compressed, blocks[0], eos_bit, 1);
		match result {
			Ok(r) => {
				// libbzip2 accepted it — data and CRC should still be correct.
				assert_eq!(r.data, original);
				assert!(r.crc_valid);
			}
			Err(Bz2Error::DecompressionFailed { .. }) => {
				// Also acceptable: libbzip2 rejected the level mismatch.
			}
			Err(e) => panic!("unexpected error variant: {e}"),
		}
	}

	// ── Edge case: empty input ──────────────────────────────────────

	#[test]
	fn test_compress_empty_produces_no_blocks()
	{
		let compressed = compress_to_bz2(b"", 9);
		let scanner = Scanner::new();
		let candidates = scanner.scan(&compressed);

		let block_count = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).count();
		let eos_count = candidates.iter().filter(|c| c.marker_type == MarkerType::Eos).count();

		// Empty input: no blocks, but there should be an EOS marker.
		assert_eq!(block_count, 0, "empty input should produce no blocks");
		assert_eq!(eos_count, 1, "empty input should still have EOS");
	}
}
