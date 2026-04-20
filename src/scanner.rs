// scanner.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Parallel scanner for bzip2 block boundaries.
//!
//! Scans compressed data for the 48-bit block magic (`0x314159265359`)
//! and end-of-stream magic (`0x177245385090`) at arbitrary bit offsets.
//!
//! Uses Aho-Corasick multi-pattern matching with 16 pre-shifted byte
//! patterns (8 bit offsets × 2 magic types) for fast scanning.  Data is
//! processed in parallel 1 MB chunks via Rayon.

use aho_corasick::AhoCorasick;
use rayon::prelude::*;

/// Block magic: pi digits `0x314159265359` (48 bits).
const BLOCK_MAGIC: u64 = 0x0000_3141_5926_5359;

/// End-of-stream magic: sqrt(pi) digits `0x177245385090` (48 bits).
const EOS_MAGIC: u64 = 0x0000_1772_4538_5090;

/// Chunk size for parallel scanning (1 MB).
const CHUNK_SIZE: usize = 1024 * 1024;

/// Overlap between adjacent chunks (bytes).  Must be >= 6 so that a
/// 7-byte shifted pattern straddling a chunk boundary is found by at
/// least one chunk.
const OVERLAP: usize = 8;

/// Type of marker found by the scanner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerType
{
	/// Block header magic `0x314159265359`.
	Block,
	/// End-of-stream magic `0x177245385090`.
	Eos,
}

/// A candidate block boundary found by the scanner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Candidate
{
	/// Bit offset from the start of the data where the 48-bit magic begins.
	pub bit_offset: u64,
	/// Whether this is a block header or end-of-stream marker.
	pub marker_type: MarkerType,
}

impl PartialOrd for Candidate
{
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering>
	{
		Some(self.cmp(other))
	}
}

impl Ord for Candidate
{
	fn cmp(&self, other: &Self) -> std::cmp::Ordering
	{
		self.bit_offset.cmp(&other.bit_offset)
	}
}

/// Pre-compiled scanner for bzip2 block boundaries.
///
/// Holds a compiled Aho-Corasick automaton with 16 patterns: 8 bit-shifted
/// variants of the block magic and 8 of the EOS magic.
pub struct Scanner
{
	/// Compiled multi-pattern automaton.
	ac: AhoCorasick,
	/// For each pattern index (0..16), the (bit_shift, marker_type).
	pattern_info: Vec<(u32, MarkerType)>,
}

impl Default for Scanner
{
	fn default() -> Self
	{
		Self::new()
	}
}

impl Scanner
{
	/// Build a new scanner with pre-compiled patterns.
	pub fn new() -> Self
	{
		let mut patterns = Vec::with_capacity(16);
		let mut pattern_info = Vec::with_capacity(16);

		for magic_type in [MarkerType::Block, MarkerType::Eos] {
			let magic = match magic_type {
				MarkerType::Block => BLOCK_MAGIC,
				MarkerType::Eos => EOS_MAGIC,
			};

			for shift in 0..8u32 {
				let pattern = build_shifted_pattern(magic, shift);
				patterns.push(pattern);
				pattern_info.push((shift, magic_type));
			}
		}

		let ac = AhoCorasick::new(&patterns).expect("pattern compilation should succeed");

		Self { ac, pattern_info }
	}

	/// Scan a byte slice for all block/EOS candidates.
	///
	/// Returns candidates sorted by bit offset, deduplicated.
	/// This is the single-threaded entry point (used for small data or testing).
	pub fn scan(&self, data: &[u8]) -> Vec<Candidate>
	{
		self.scan_chunk(data, 0, data.len())
	}

	/// Scan the data in parallel using Rayon.
	///
	/// Splits the data into ~1 MB chunks with overlap, scans each chunk
	/// in parallel, then merges and deduplicates the results.
	pub fn scan_parallel(&self, data: &[u8]) -> Vec<Candidate>
	{
		if data.len() <= CHUNK_SIZE + OVERLAP {
			// Small enough for single-threaded scan.
			return self.scan(data);
		}

		// Build chunk boundaries: (start, end) for each chunk.
		// Each chunk extends OVERLAP bytes into the next chunk's territory
		// to catch patterns that straddle boundaries.
		let mut chunks: Vec<(usize, usize)> = Vec::new();
		let mut start = 0;
		while start < data.len() {
			let end = (start + CHUNK_SIZE + OVERLAP).min(data.len());
			chunks.push((start, end));
			start += CHUNK_SIZE;
		}

		// Scan all chunks in parallel.
		let chunk_results: Vec<(usize, Vec<Candidate>)> = chunks
			.par_iter()
			.map(|&(start, end)| {
				let candidates = self.scan_chunk(data, start, end);
				(start, candidates)
			})
			.collect();

		// Merge results.  For each chunk, only keep candidates whose bit
		// offset falls within [chunk_start_bit, chunk_end_bit) where
		// chunk_end_bit is the NON-OVERLAPPING boundary.  The last chunk
		// keeps everything.
		let mut all_candidates = Vec::new();

		for (i, (start, candidates)) in chunk_results.into_iter().enumerate() {
			let chunk_end_byte = if i < chunks.len() - 1 {
				start + CHUNK_SIZE // non-overlapping boundary
			} else {
				data.len() // last chunk: keep all
			};
			let chunk_end_bit = (chunk_end_byte as u64) * 8;

			for c in candidates {
				if c.bit_offset < chunk_end_bit {
					all_candidates.push(c);
				}
			}
		}

		all_candidates.sort();
		all_candidates.dedup();
		all_candidates
	}

	/// Scan a single chunk of data for candidates.
	///
	/// `start` and `end` define the byte range within `data` to scan.
	fn scan_chunk(&self, data: &[u8], start: usize, end: usize) -> Vec<Candidate>
	{
		let chunk = &data[start..end];
		let mut candidates = Vec::new();

		for mat in self.ac.find_overlapping_iter(chunk) {
			let pattern_id = mat.pattern().as_usize();
			let (bit_shift, marker_type) = self.pattern_info[pattern_id];
			let match_byte_in_chunk = mat.start();

			// Compute absolute bit offset of the 48-bit magic.
			//
			// For shift=0: the 6-byte pattern IS the magic, so the magic
			//   starts at match_byte.
			// For shift>0: the 5-byte pattern matches the INNER bytes of
			//   a 7-byte window.  The magic starts one byte before the
			//   match, at bit offset `shift` within that byte.
			let magic_byte = if bit_shift == 0 {
				start + match_byte_in_chunk
			} else {
				// The inner bytes start at window[1], so the window
				// (and the magic's first partial byte) starts one byte earlier.
				if match_byte_in_chunk == 0 && start == 0 {
					continue; // can't look back before the start of data
				}
				start + match_byte_in_chunk - 1
			};

			let abs_bit = (magic_byte as u64) * 8 + bit_shift as u64;

			// Skip matches in the stream header region (first 32 bits = 4 bytes).
			if abs_bit < 32 {
				continue;
			}

			// For shifted patterns, verify the edge bits.
			if bit_shift > 0
				&& !verify_match(
					data,
					start + match_byte_in_chunk,
					bit_shift,
					match marker_type {
						MarkerType::Block => BLOCK_MAGIC,
						MarkerType::Eos => EOS_MAGIC,
					},
				) {
				continue;
			}

			candidates.push(Candidate { bit_offset: abs_bit, marker_type });
		}

		candidates
	}
}

/// Build a byte pattern for a 48-bit magic value shifted right by `shift` bits.
///
/// The magic occupies bits `[shift, shift+48)` within a 7-byte (56-bit) window.
/// The surrounding bits are zero, and the pattern must match exactly on these
/// zeros too — so we use a mask-based approach... except Aho-Corasick doesn't
/// support masks.
///
/// Instead, we generate the exact byte sequence that would appear in the
/// data when the magic starts at the given bit offset within a byte.  The
/// caller must verify that any partial leading/trailing bits outside the
/// 48-bit magic also match (which they do by construction since the pattern
/// includes them).
///
/// Actually, this approach has a subtlety: the bits outside the 48-bit magic
/// in the pattern bytes could be anything in the actual data.  Pure
/// Aho-Corasick matching would require those bits to be zero, which they
/// won't be.  To handle this correctly, we need to match on the INNER bytes
/// only (those fully covered by the 48-bit magic) and verify the edge bytes
/// separately.
///
/// For shift=0: pattern is 6 bytes (exactly the magic), all bits meaningful.
/// For shift=1..7: pattern is the 5 inner bytes that are fully determined by
///   the magic.  Edge bytes have unknown bits and must be verified after matching.
fn build_shifted_pattern(magic: u64, shift: u32) -> Vec<u8>
{
	// Place the 48-bit magic into a 64-bit value, shifted right by `shift` bits.
	// The magic occupies bits [shift, shift+48) of the 56-bit window.
	let shifted = magic << (64 - 48 - shift);

	// Extract the 7 bytes that span the shifted magic.
	let bytes: [u8; 8] = shifted.to_be_bytes();

	if shift == 0 {
		// Perfectly aligned: all 6 bytes are fully determined.
		bytes[0..6].to_vec()
	} else {
		// Shifted: bytes[0] has `shift` unknown high bits, bytes[6] has
		// `(8-shift)` unknown low bits.  Only bytes[1..6] are fully
		// determined by the magic (5 inner bytes).
		bytes[1..6].to_vec()
	}
}

/// Verify that a match at the given byte position with the given shift
/// actually corresponds to the expected magic value.
///
/// For shift=0, the Aho-Corasick match on 6 bytes is definitive.
/// For shift>0, the match was on 5 inner bytes; we need to verify the
/// edge bits (partial bytes at the start and end of the 48-bit magic).
fn verify_match(data: &[u8], match_byte: usize, shift: u32, magic: u64) -> bool
{
	if shift == 0 {
		return true; // 6-byte exact match, nothing to verify
	}

	// Read 7 bytes starting at match_byte-1 (the pattern matched bytes[1..6],
	// so the full 7-byte window starts one byte earlier).
	let window_start = if match_byte > 0 { match_byte - 1 } else { return false };
	if window_start + 7 > data.len() {
		return false;
	}

	let mut window = [0u8; 8];
	window[0..7].copy_from_slice(&data[window_start..window_start + 7]);
	let actual = u64::from_be_bytes(window);

	// Extract the 48 bits at position [shift, shift+48) from the window.
	let mask = ((1u64 << 48) - 1) << (64 - 48 - shift);
	let expected = magic << (64 - 48 - shift);

	(actual & mask) == expected
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use super::*;

	#[test]
	fn test_build_shifted_pattern_aligned()
	{
		let pattern = build_shifted_pattern(BLOCK_MAGIC, 0);
		assert_eq!(pattern.len(), 6);
		assert_eq!(pattern, [0x31, 0x41, 0x59, 0x26, 0x53, 0x59]);
	}

	#[test]
	fn test_build_shifted_pattern_shifted()
	{
		// Shift=4: the 48-bit magic is placed at bits 4..52 of a 56-bit window.
		// Inner bytes (bytes 1..6 of the 7-byte window) are fully determined.
		let pattern = build_shifted_pattern(BLOCK_MAGIC, 4);
		assert_eq!(pattern.len(), 5);

		// The magic 0x314159265359 shifted right by 4 bits in a 64-bit value:
		// 0x0314_1592_6535_9000 >> 8 → bytes[1..6] should be the inner bytes.
		let shifted = BLOCK_MAGIC << (64 - 48 - 4);
		let bytes = shifted.to_be_bytes();
		assert_eq!(pattern, &bytes[1..6]);
	}

	#[test]
	fn test_scan_finds_aligned_block_magic()
	{
		// Construct data with the block magic at byte offset 4 (bit offset 32).
		let mut data = vec![0u8; 20];
		data[4] = 0x31;
		data[5] = 0x41;
		data[6] = 0x59;
		data[7] = 0x26;
		data[8] = 0x53;
		data[9] = 0x59;

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		assert_eq!(candidates.len(), 1);
		assert_eq!(candidates[0].bit_offset, 32);
		assert_eq!(candidates[0].marker_type, MarkerType::Block);
	}

	#[test]
	fn test_scan_finds_aligned_eos_magic()
	{
		let mut data = vec![0u8; 20];
		data[4] = 0x17;
		data[5] = 0x72;
		data[6] = 0x45;
		data[7] = 0x38;
		data[8] = 0x50;
		data[9] = 0x90;

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		assert_eq!(candidates.len(), 1);
		assert_eq!(candidates[0].bit_offset, 32);
		assert_eq!(candidates[0].marker_type, MarkerType::Eos);
	}

	#[test]
	fn test_scan_finds_shifted_magic()
	{
		// Place block magic at bit offset 36 (byte 4, shift 4).
		let mut data = vec![0u8; 20];
		let shifted = BLOCK_MAGIC << (64 - 48 - 4);
		let bytes = shifted.to_be_bytes();
		data[4..11].copy_from_slice(&bytes[..7]);

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		assert_eq!(
			candidates.len(),
			1,
			"should find exactly one candidate, found {:?}",
			candidates
		);
		assert_eq!(candidates[0].bit_offset, 36);
		assert_eq!(candidates[0].marker_type, MarkerType::Block);
	}

	#[test]
	fn test_scan_empty()
	{
		let scanner = Scanner::new();
		let candidates = scanner.scan(&[]);
		assert!(candidates.is_empty());
	}

	#[test]
	fn test_scan_no_matches()
	{
		let data = vec![0xAA; 100];
		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);
		assert!(candidates.is_empty());
	}

	#[test]
	fn test_verify_match_aligned()
	{
		let data = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];
		assert!(verify_match(&data, 0, 0, BLOCK_MAGIC));
	}

	#[test]
	fn test_verify_match_shifted()
	{
		// Place magic at shift=4 within a 7-byte window.
		let shifted = BLOCK_MAGIC << (64 - 48 - 4);
		let bytes = shifted.to_be_bytes();
		let data: Vec<u8> = bytes[0..7].to_vec();

		// The inner bytes are at positions 1..6 in the data.
		// verify_match is called with match_byte=1 (start of inner bytes).
		assert!(verify_match(&data, 1, 4, BLOCK_MAGIC));
	}

	#[test]
	fn test_scan_parallel_matches_sequential()
	{
		// Create data with multiple block magics at known positions.
		let mut data = vec![0u8; 3 * CHUNK_SIZE]; // 3 MB to force chunking

		// Place block magic at various byte-aligned positions.
		let positions = [100, CHUNK_SIZE - 3, CHUNK_SIZE + 500, 2 * CHUNK_SIZE + 100];
		for &pos in &positions {
			if pos + 6 <= data.len() {
				data[pos] = 0x31;
				data[pos + 1] = 0x41;
				data[pos + 2] = 0x59;
				data[pos + 3] = 0x26;
				data[pos + 4] = 0x53;
				data[pos + 5] = 0x59;
			}
		}

		let scanner = Scanner::new();
		let seq = scanner.scan(&data);
		let par = scanner.scan_parallel(&data);

		assert_eq!(seq, par, "parallel scan should match sequential scan");
	}

	#[test]
	fn test_scan_real_bz2_header()
	{
		// Compress some data with bzip2, then verify the scanner finds
		// the block magic right after the 4-byte stream header.
		use std::io::Write;

		use bzip2::Compression;
		use bzip2::write::BzEncoder;

		let mut encoder = BzEncoder::new(Vec::new(), Compression::default());
		encoder.write_all(b"hello world, this is test data for scanning").unwrap();
		let compressed = encoder.finish().unwrap();

		let scanner = Scanner::new();
		let candidates = scanner.scan(&compressed);

		// Should find at least one Block marker and one EOS marker.
		let blocks: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).collect();
		let eos: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Eos).collect();

		assert!(!blocks.is_empty(), "should find at least one block magic");
		assert!(!eos.is_empty(), "should find at least one EOS magic");

		// The first block magic should be at bit 32 (right after the 4-byte header "BZh9").
		assert_eq!(blocks[0].bit_offset, 32, "first block should start at bit 32");
	}

	#[test]
	fn test_scan_multiple_shifts()
	{
		// Test all 8 bit shifts to ensure each produces the correct bit offset.
		let scanner = Scanner::new();

		for shift in 0..8u32 {
			let mut data = vec![0u8; 20];
			let shifted = BLOCK_MAGIC << (64 - 48 - shift);
			let bytes = shifted.to_be_bytes();
			// Place at byte offset 4 to avoid the <32 bit skip.
			for i in 0..7 {
				if 4 + i < data.len() {
					data[4 + i] = bytes[i];
				}
			}

			let candidates = scanner.scan(&data);
			let expected_bit = 32 + shift as u64;

			assert!(
				candidates
					.iter()
					.any(|c| c.bit_offset == expected_bit && c.marker_type == MarkerType::Block),
				"shift={}: expected block at bit {}, found {:?}",
				shift,
				expected_bit,
				candidates
			);
		}
	}

	// ── Coverage gap tests ─────────────────────────────────────────

	#[test]
	fn test_scan_shifted_match_at_start_cant_look_back()
	{
		// Shifted match where match_byte_in_chunk==0 && start==0 → continue (line 197).
		// Place a shifted magic so the inner bytes start at byte 0.
		// For shift>0, the inner 5-byte pattern would need to match at chunk[0],
		// but looking back one byte is impossible.
		let scanner = Scanner::new();

		// Build the 5 inner bytes for block magic at shift=4.
		let shifted = BLOCK_MAGIC << (64 - 48 - 4);
		let bytes = shifted.to_be_bytes();
		let inner = &bytes[1..6]; // 5 inner bytes

		// Place inner bytes at data[0..5]. The scanner finds a match at
		// match_byte_in_chunk=0, start=0 → can't look back → skip.
		let mut data = vec![0u8; 20];
		data[0..5].copy_from_slice(inner);

		let candidates = scanner.scan(&data);
		// Should NOT find a block at bit offset 4 (can't verify without preceding byte).
		assert!(
			!candidates.iter().any(|c| c.bit_offset == 4 && c.marker_type == MarkerType::Block),
			"should not find shifted match at start of data: {:?}",
			candidates
		);
	}

	#[test]
	fn test_scan_magic_in_header_region_skipped()
	{
		// Magic within the first 32 bits (header region) is skipped (line 206).
		// Place block magic at bit offset 0 (byte 0).
		let mut data = vec![0u8; 20];
		data[0] = 0x31;
		data[1] = 0x41;
		data[2] = 0x59;
		data[3] = 0x26;
		data[4] = 0x53;
		data[5] = 0x59;

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		// Magic at bit 0 is in the header region → skipped.
		assert!(
			!candidates.iter().any(|c| c.bit_offset == 0),
			"should skip magic in header region: {:?}",
			candidates
		);
	}

	#[test]
	fn test_verify_match_wrong_edge_bits()
	{
		// Shifted pattern with correct inner bytes but wrong edge bits → verify_match fails (line 220).
		let scanner = Scanner::new();

		// Build data where inner bytes match but edge bytes are wrong.
		let shifted = BLOCK_MAGIC << (64 - 48 - 4);
		let bytes = shifted.to_be_bytes();
		let mut data = vec![0u8; 20];

		// Place the full 7-byte window at offset 4, but corrupt the edge bytes.
		data[4..11].copy_from_slice(&bytes[0..7]);
		// Corrupt the leading edge byte (has 4 bits of magic + 4 unknown bits).
		data[4] ^= 0x08; // Flip a bit in the magic portion of the leading byte.

		let candidates = scanner.scan(&data);
		// The inner bytes still match the AC pattern, but verify_match should reject it.
		assert!(
			!candidates.iter().any(|c| c.bit_offset == 36 && c.marker_type == MarkerType::Block),
			"should reject shifted match with wrong edge bits: {:?}",
			candidates
		);
	}

	#[test]
	fn test_verify_match_window_past_data_end()
	{
		// verify_match where window_start + 7 > data.len() → returns false (line 289).
		// For this we need data where the inner pattern matches but there aren't
		// enough bytes for the full 7-byte window.
		let shifted = BLOCK_MAGIC << (64 - 48 - 4);
		let bytes = shifted.to_be_bytes();

		// Create data with just enough for header + inner bytes but not the full window.
		// Inner bytes are bytes[1..6]. Place at byte 5 so window_start=4, window needs 4..11.
		// If data is only 10 bytes, window_start+7=11 > 10.
		let mut data = vec![0u8; 10];
		// Place inner bytes at data[5..10]
		let inner = &bytes[1..6];
		data[5..10].copy_from_slice(inner);

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		// Any shifted match near the end should be rejected by verify_match.
		assert!(
			!candidates.iter().any(|c| c.bit_offset == 36 && c.marker_type == MarkerType::Block),
			"should reject match with window past data end: {:?}",
			candidates
		);
	}

	#[test]
	fn test_verify_match_at_match_byte_zero()
	{
		// verify_match with match_byte=0 → window_start underflow check (line 287).
		assert!(!verify_match(&[0u8; 10], 0, 4, BLOCK_MAGIC));
	}

	#[test]
	fn test_verify_match_short_window()
	{
		// window_start + 7 > data.len() (line 288-289).
		let data = [0u8; 5];
		assert!(!verify_match(&data, 1, 4, BLOCK_MAGIC));
	}
}
