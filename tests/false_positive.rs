// tests/false_positive.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6
//
// Integration tests: verify that the decoder correctly handles bzip2
// streams containing data that happens to include the 48-bit block
// magic pattern (0x314159265359) as a false positive.

use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use parallel_bz2_redux::ParBz2Decoder;

// ── Helpers ────────────────────────────────────────────────────────

fn bz2_compress(data: &[u8], level: u32) -> Vec<u8>
{
	let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
	enc.write_all(data).unwrap();
	enc.finish().unwrap()
}

fn par_decompress(compressed: &[u8]) -> Vec<u8>
{
	let data: Arc<[u8]> = Arc::from(compressed.to_vec());
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	out
}

fn bz2_reference_decompress(compressed: &[u8]) -> Vec<u8>
{
	let mut dec = bzip2::read::BzDecoder::new(compressed);
	let mut out = Vec::new();
	dec.read_to_end(&mut out).unwrap();
	out
}

fn lcg_bytes(seed: u64, len: usize) -> Vec<u8>
{
	let mut state = seed;
	(0..len)
		.map(|_| {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(state >> 33) as u8
		})
		.collect()
}

// ── False-positive tests ───────────────────────────────────────────

/// Embed the 48-bit block magic (0x314159265359) as raw bytes in the
/// input data.  When compressed, the magic pattern may or may not
/// survive in the compressed stream, but the decoder must still produce
/// correct output regardless.
#[test]
fn input_contains_block_magic_bytes()
{
	// 0x314159265359 as big-endian bytes.
	let magic: [u8; 6] = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];

	let mut original = Vec::with_capacity(100_000);
	// Prefix with random data.
	original.extend_from_slice(&lcg_bytes(100, 50_000));
	// Inject magic pattern multiple times.
	for _ in 0..100 {
		original.extend_from_slice(&magic);
		original.extend_from_slice(&lcg_bytes(200, 500));
	}
	// Suffix with random data.
	original.extend_from_slice(&lcg_bytes(300, 50_000));

	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

/// Also embed the EOS magic (0x177245385090) in the input data.
#[test]
fn input_contains_eos_magic_bytes()
{
	let eos_magic: [u8; 6] = [0x17, 0x72, 0x45, 0x38, 0x50, 0x90];

	let mut original = Vec::with_capacity(100_000);
	original.extend_from_slice(&lcg_bytes(400, 50_000));
	for _ in 0..50 {
		original.extend_from_slice(&eos_magic);
		original.extend_from_slice(&lcg_bytes(500, 1000));
	}
	original.extend_from_slice(&lcg_bytes(600, 50_000));

	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

/// Embed both magic patterns in the input data.
#[test]
fn input_contains_both_magic_patterns()
{
	let block_magic: [u8; 6] = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];
	let eos_magic: [u8; 6] = [0x17, 0x72, 0x45, 0x38, 0x50, 0x90];

	let mut original = Vec::with_capacity(200_000);
	original.extend_from_slice(&lcg_bytes(700, 50_000));
	for i in 0..100 {
		if i % 2 == 0 {
			original.extend_from_slice(&block_magic);
		} else {
			original.extend_from_slice(&eos_magic);
		}
		original.extend_from_slice(&lcg_bytes(800 + i, 1000));
	}
	original.extend_from_slice(&lcg_bytes(900, 50_000));

	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

/// Large multi-block file with magic patterns embedded.  At level 1,
/// blocks are ~100KB, so 500KB of data should produce several blocks.
/// This tests the pipeline's false-positive handling across block
/// boundaries.
#[test]
fn multi_block_with_embedded_magic()
{
	let block_magic: [u8; 6] = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];

	let mut original = Vec::with_capacity(500 * 1024);
	for i in 0..500 {
		original.extend_from_slice(&lcg_bytes(1000 + i, 1000));
		if i % 10 == 0 {
			original.extend_from_slice(&block_magic);
		}
	}

	let compressed = bz2_compress(&original, 1);
	let reference = bz2_reference_decompress(&compressed);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, reference);
	assert_eq!(decompressed, original);
}

/// Worst case: the 6 magic bytes repeated back-to-back in the input.
#[test]
fn dense_magic_pattern()
{
	let block_magic: [u8; 6] = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];

	let mut original = Vec::with_capacity(60_000);
	// 10,000 repetitions of the magic = 60,000 bytes.
	for _ in 0..10_000 {
		original.extend_from_slice(&block_magic);
	}

	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

/// The magic pattern split across different byte alignments.
/// Input data with the pattern at offsets 0, 1, 2, ..., 7 to test
/// all bit-shift scenarios.
#[test]
fn magic_at_every_byte_alignment()
{
	let block_magic: [u8; 6] = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];

	let mut original = Vec::with_capacity(10_000);
	for offset in 0..8 {
		// Pad to desired byte alignment.
		original.extend(std::iter::repeat_n(0xAA, offset));
		original.extend_from_slice(&block_magic);
	}
	// Pad to a reasonable size.
	original.extend_from_slice(&lcg_bytes(1234, 5_000));

	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}
