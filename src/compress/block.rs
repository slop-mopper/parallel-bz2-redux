// compress/block.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Single-block compression via libbzip2.
//!
//! Takes a chunk of uncompressed data (at most `level * 100_000` bytes),
//! compresses it into a single bzip2 block, and extracts the raw block
//! bits ready for the assembler to concatenate into a multi-block stream.

use std::io::Write;

use crate::bits::BitWriter;
use crate::bits::read_u32_at_bit;
use crate::crc::bz2_crc32;
use crate::error::Bz2Error;
use crate::error::Result;

/// The 48-bit block magic: `0x314159265359`.
const BLOCK_MAGIC: u64 = 0x314159265359;

/// The 48-bit end-of-stream magic: `0x177245385090`.
const EOS_MAGIC: u64 = 0x177245385090;

/// Maximum bzip2 compression level.
const MAX_LEVEL: u8 = 9;

/// Bytes of uncompressed data per block-size unit.
const BLOCK_SIZE_UNIT: usize = 100_000;

/// libbzip2 reserves 19 bytes of the block buffer for internal sentinels.
/// The effective BWT array capacity is `level * BLOCK_SIZE_UNIT - BLOCK_SENTINEL_OVERHEAD`.
const BLOCK_SENTINEL_OVERHEAD: usize = 19;

/// Maximum uncompressed bytes that fit in a single block at the given level.
///
/// libbzip2's BWT buffer holds at most `level * 100_000 - 19` *slots*.
/// However, the initial RLE pass can expand runs of 4 identical bytes:
/// 4 input bytes consume 5 BWT slots (the 4 bytes + 1 run-length byte).
/// In the worst case (input entirely composed of 4-byte runs), the
/// expansion ratio is 5/4.  To guarantee that any input up to this limit
/// produces exactly one bzip2 block, we apply the inverse factor (4/5).
///
/// This is conservative — real data rarely hits the worst case — but it
/// guarantees single-block output regardless of input content.
pub fn max_block_bytes(level: u8) -> usize
{
	let nblock_max = level as usize * BLOCK_SIZE_UNIT - BLOCK_SENTINEL_OVERHEAD;
	// Worst-case RLE expansion: 4 input bytes → 5 BWT slots.
	// Safe input limit = nblock_max * 4 / 5.
	nblock_max * 4 / 5
}

/// Result of compressing a single block of data.
#[derive(Debug, Clone)]
pub struct CompressedBlock
{
	/// Raw block bits starting with the 48-bit block magic (`0x314159265359`).
	/// The trailing byte may have unused low bits; [`bit_len`](Self::bit_len)
	/// determines the exact number of valid bits.
	pub bits: Vec<u8>,
	/// Exact number of valid bits in [`bits`](Self::bits).
	pub bit_len: u64,
	/// CRC-32 of the original uncompressed data (bzip2 MSB-first polynomial).
	/// Used by the assembler to compute the stream CRC.
	pub block_crc: u32,
}

/// Compress a chunk of uncompressed data into a single bzip2 block.
///
/// # Arguments
///
/// * `data` -- uncompressed input, must be non-empty and at most `level * 100_000` bytes.
/// * `level` -- bzip2 compression level (`1`--`9`).
///
/// # Returns
///
/// `Ok(CompressedBlock)` with the raw block bits (including the 48-bit
/// block magic and 32-bit block CRC) and the block CRC computed over the
/// uncompressed data.
///
/// # Errors
///
/// - [`Bz2Error::InvalidFormat`] if `data` is empty, `level` is out of range, or `data` exceeds the block
///   size for the given level.
/// - [`Bz2Error::Io`] if libbzip2 fails internally.
pub fn compress_block(data: &[u8], level: u8) -> Result<CompressedBlock>
{
	// ── 1. Validate inputs. ─────────────────────────────────────────
	if data.is_empty() {
		return Err(Bz2Error::InvalidFormat("compress_block: empty input".into()));
	}
	if !(1..=MAX_LEVEL).contains(&level) {
		return Err(Bz2Error::InvalidFormat(format!(
			"compress_block: level must be 1-{MAX_LEVEL}, got {level}"
		)));
	}
	let max_bytes = max_block_bytes(level);
	if data.len() > max_bytes {
		return Err(Bz2Error::InvalidFormat(format!(
			"compress_block: data length {} exceeds block size {} for level {level}",
			data.len(),
			max_bytes
		)));
	}

	// ── 2. Compute the block CRC over uncompressed data. ────────────
	let block_crc = bz2_crc32(data);

	// ── 3. Compress via libbzip2 → full single-block bzip2 stream. ──
	let mut encoder = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level as u32));
	encoder.write_all(data)?;
	let stream = encoder.finish()?;

	// ── 4. Locate the EOS marker from the end of the stream. ────────
	let eos_bit = find_eos_bit(&stream)
		.ok_or_else(|| Bz2Error::InvalidFormat("compress_block: EOS marker not found in compressed output".into()))?;

	// ── 5. Verify the block magic is at bit 32 (after 4-byte header). ──
	// DEFENSIVE: libbzip2 always emits the block magic immediately after
	// the 4-byte stream header, so this check cannot fail with a correct
	// libbzip2 implementation.  Kept as a safety net.
	if read_48_at_bit(&stream, 32) != BLOCK_MAGIC {
		return Err(Bz2Error::InvalidFormat(
			"compress_block: block magic not found at expected position".into(),
		));
	}

	// ── 6. Verify stored block CRC matches our computed CRC. ────────
	// DEFENSIVE: With correct size validation (data.len() ≤ max_block_bytes),
	// libbzip2 always produces exactly one block, so the stored CRC at
	// bit 80 always matches our computed CRC.  A mismatch would indicate
	// multi-block output (exceeded effective block capacity) or a libbzip2
	// bug.  Kept as a safety net.
	let stored_crc = read_u32_at_bit(&stream, 80); // bit 32 (header) + 48 (magic) = 80
	if stored_crc != block_crc {
		return Err(Bz2Error::InvalidFormat(format!(
			"compress_block: stored CRC {stored_crc:#010x} != computed CRC {block_crc:#010x} \
			 (likely multi-block output; reduce input size)"
		)));
	}

	// ── 7. Extract block bits [bit 32, eos_bit) via BitWriter. ──────
	let block_bit_len = eos_bit - 32;
	let mut writer = BitWriter::with_capacity(block_bit_len.div_ceil(8) as usize);
	writer.copy_bits_from(&stream, 32, block_bit_len);
	let bits = writer.into_bytes();

	Ok(CompressedBlock { bits, bit_len: block_bit_len, block_crc })
}

/// Find the bit offset of the EOS marker in a single-block bzip2 stream.
///
/// The stream ends with `[EOS magic 48b] [stream CRC 32b] [padding 0-7b]`.
/// We probe 8 candidate positions (one per possible padding value) from
/// the end of the stream.  This is O(1) and avoids any scanning through
/// the compressed block data.
fn find_eos_bit(stream: &[u8]) -> Option<u64>
{
	let stream_bits = stream.len() as u64 * 8;
	if stream_bits < 80 {
		return None;
	}
	for padding in 0u64..8 {
		let candidate = stream_bits - 80 - padding;
		if read_48_at_bit(stream, candidate) == EOS_MAGIC {
			return Some(candidate);
		}
	}
	None
}

/// Read 48 bits at an arbitrary bit offset, returned as the low 48 bits
/// of a `u64`.
fn read_48_at_bit(data: &[u8], bit_offset: u64) -> u64
{
	let hi = read_u32_at_bit(data, bit_offset) as u64;
	let lo = read_u32_at_bit(data, bit_offset + 32) as u64;
	(hi << 16) | (lo >> 16)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use std::io::Read;

	use super::*;
	use crate::crc::bz2_crc32;
	use crate::decompress::block::decompress_block;
	use crate::scanner::MarkerType;
	use crate::scanner::Scanner;

	// ── Helpers ──────────────────────────────────────────────────────

	/// Verify that the compressed block's bits start with the block magic.
	fn assert_starts_with_magic(block: &CompressedBlock)
	{
		assert!(block.bit_len >= 48, "block too short for magic");
		let magic = read_48_at_bit(&block.bits, 0);
		assert_eq!(magic, BLOCK_MAGIC, "block bits don't start with block magic");
	}

	/// Synthesize a full bzip2 stream from a CompressedBlock and decompress
	/// it, returning the decompressed bytes.
	fn roundtrip_via_synthetic(block: &CompressedBlock, level: u8) -> Vec<u8>
	{
		let mut writer = BitWriter::with_capacity(4 + block.bits.len() + 11);
		// Stream header.
		writer.write_bytes(&[b'B', b'Z', b'h', b'0' + level]);
		// Block bits.
		writer.copy_bits_from(&block.bits, 0, block.bit_len);
		// EOS marker.
		writer.write_bits(EOS_MAGIC, 48);
		// Stream CRC (single block → stream_crc = block_crc).
		writer.write_bits(block.block_crc as u64, 32);
		// Pad to byte boundary.
		writer.pad_to_byte();

		let stream = writer.into_bytes();
		let cursor = std::io::Cursor::new(&stream);
		let mut decoder = bzip2::read::BzDecoder::new(cursor);
		let mut out = Vec::new();
		decoder.read_to_end(&mut out).expect("synthetic stream decompression failed");
		out
	}

	// ── Normal cases ─────────────────────────────────────────────────

	#[test]
	fn test_compress_small_text()
	{
		let original = b"Hello, World!";
		let block = compress_block(original, 9).unwrap();
		assert_starts_with_magic(&block);
		assert_eq!(block.block_crc, bz2_crc32(original));

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_compress_medium_text()
	{
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(4096).collect();
		let block = compress_block(&original, 9).unwrap();
		assert_starts_with_magic(&block);

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_compress_all_levels()
	{
		let original = b"Testing all compression levels 1 through 9.";
		for level in 1..=9u8 {
			let block = compress_block(original, level).unwrap();
			assert_starts_with_magic(&block);
			assert_eq!(block.block_crc, bz2_crc32(original));

			let decompressed = roundtrip_via_synthetic(&block, level);
			assert_eq!(decompressed, original, "mismatch at level {level}");
		}
	}

	#[test]
	fn test_compress_binary_all_values()
	{
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		assert_eq!(original.len(), 1024);
		let block = compress_block(&original, 9).unwrap();
		assert_starts_with_magic(&block);
		assert_eq!(block.block_crc, bz2_crc32(&original));

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_compress_all_zeros()
	{
		let original = vec![0u8; 4096];
		let block = compress_block(&original, 9).unwrap();
		assert_starts_with_magic(&block);

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_compress_all_same_byte()
	{
		let original = vec![0xBBu8; 4096];
		let block = compress_block(&original, 9).unwrap();
		assert_starts_with_magic(&block);

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_compress_random_bytes()
	{
		let mut rng: u64 = 0xCAFE_BABE_DEAD_BEEF;
		let original: Vec<u8> = (0..2048)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();
		let block = compress_block(&original, 9).unwrap();
		assert_starts_with_magic(&block);

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_compress_single_byte()
	{
		let original = [0x42u8];
		let block = compress_block(&original, 9).unwrap();
		assert_starts_with_magic(&block);
		assert_eq!(block.block_crc, bz2_crc32(&original));

		let decompressed = roundtrip_via_synthetic(&block, 9);
		assert_eq!(decompressed, &original);
	}

	#[test]
	fn test_compress_max_block_size()
	{
		// Level 1 → max block = (100_000 - 19) * 4 / 5 = 79_984 bytes.
		let max = max_block_bytes(1);
		let original: Vec<u8> = (0u32..max as u32).map(|i| (i % 251) as u8).collect();
		assert_eq!(original.len(), max);
		let block = compress_block(&original, 1).unwrap();
		assert_starts_with_magic(&block);

		let decompressed = roundtrip_via_synthetic(&block, 1);
		assert_eq!(decompressed, original);
	}

	// ── Cross-validate with decompress_block ─────────────────────────

	#[test]
	fn test_compress_then_decompress_block()
	{
		let original = b"Cross-validate compress and decompress block functions.";
		let compressed = compress_block(original, 9).unwrap();

		// Build a synthetic stream to find scanner boundaries for decompress_block.
		let mut writer = BitWriter::with_capacity(4 + compressed.bits.len() + 11);
		writer.write_bytes(b"BZh9");
		writer.copy_bits_from(&compressed.bits, 0, compressed.bit_len);
		writer.write_bits(EOS_MAGIC, 48);
		writer.write_bits(compressed.block_crc as u64, 32);
		writer.pad_to_byte();
		let stream = writer.into_bytes();

		// Verify scanner finds one block + one EOS.
		let scanner = Scanner::new();
		let candidates = scanner.scan(&stream);
		let blocks: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).collect();
		let eos: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Eos).collect();
		assert_eq!(blocks.len(), 1);
		assert_eq!(eos.len(), 1);

		// Decompress via decompress_block and compare.
		let result = decompress_block(&stream, blocks[0].bit_offset, eos[0].bit_offset, 9).unwrap();
		assert!(result.crc_valid);
		assert_eq!(result.data, original);
	}

	// ── Error cases ─────────────────────────────────────────────────

	#[test]
	fn test_compress_empty_input()
	{
		let result = compress_block(b"", 9);
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), Bz2Error::InvalidFormat(_)));
	}

	#[test]
	fn test_compress_exceeds_block_size()
	{
		// Level 1 → max is max_block_bytes(1).  Try max + 1 bytes.
		let data = vec![0u8; max_block_bytes(1) + 1];
		let result = compress_block(&data, 1);
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), Bz2Error::InvalidFormat(_)));
	}

	#[test]
	fn test_compress_invalid_level_zero()
	{
		let result = compress_block(b"test", 0);
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), Bz2Error::InvalidFormat(_)));
	}

	#[test]
	fn test_compress_invalid_level_too_high()
	{
		let result = compress_block(b"test", 10);
		assert!(result.is_err());
		assert!(matches!(result.unwrap_err(), Bz2Error::InvalidFormat(_)));
	}

	// ── find_eos_bit edge cases ─────────────────────────────────────

	#[test]
	fn test_find_eos_bit_too_short()
	{
		// Stream shorter than 80 bits (10 bytes) → None.
		assert_eq!(find_eos_bit(&[0u8; 9]), None);
		assert_eq!(find_eos_bit(&[]), None);
		assert_eq!(find_eos_bit(&[0u8; 5]), None);
	}

	#[test]
	fn test_find_eos_bit_no_match()
	{
		// 16 bytes of zeros — no EOS magic pattern in any of the 8 candidate positions.
		assert_eq!(find_eos_bit(&[0u8; 16]), None);
		// 16 bytes of 0xFF — also no match.
		assert_eq!(find_eos_bit(&[0xFF; 16]), None);
	}
}
