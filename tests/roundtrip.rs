// tests/roundtrip.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6
//
// Integration tests: compress with the bzip2 crate, decompress with
// ParBz2Decoder, and verify byte-for-byte equality.

use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use parallel_bz2_redux::ParBz2Decoder;

// ── Helpers ────────────────────────────────────────────────────────

/// Compress data using the bzip2 crate at the given level.
fn bz2_compress(data: &[u8], level: u32) -> Vec<u8>
{
	let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
	enc.write_all(data).unwrap();
	enc.finish().unwrap()
}

/// Decompress data using the standard bzip2 crate (reference implementation).
fn bz2_reference_decompress(compressed: &[u8]) -> Vec<u8>
{
	let mut dec = bzip2::read::BzDecoder::new(compressed);
	let mut out = Vec::new();
	dec.read_to_end(&mut out).unwrap();
	out
}

/// Decompress data using our parallel decoder.
fn par_decompress(compressed: &[u8]) -> Vec<u8>
{
	let data: Arc<[u8]> = Arc::from(compressed.to_vec());
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	out
}

/// Simple LCG pseudo-random data generator (deterministic, no dependencies).
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

// ── Roundtrip tests ────────────────────────────────────────────────

#[test]
fn roundtrip_empty()
{
	let original = b"";
	let compressed = bz2_compress(original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_single_byte()
{
	let original = b"X";
	let compressed = bz2_compress(original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_small_text()
{
	let original = b"Hello, parallel bzip2 world!";
	let compressed = bz2_compress(original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_100_bytes()
{
	let original = lcg_bytes(42, 100);
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_1kb()
{
	let original = lcg_bytes(123, 1024);
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_100kb()
{
	let original = lcg_bytes(456, 100 * 1024);
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_1mb()
{
	let original = lcg_bytes(789, 1024 * 1024);
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

/// 10 MB roundtrip -- forces multiple bzip2 blocks even at level 9.
#[test]
fn roundtrip_10mb()
{
	let original = lcg_bytes(1337, 10 * 1024 * 1024);
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

// ── Level sweep ────────────────────────────────────────────────────

/// Test all compression levels (1-9) with a 50 KB payload.
#[test]
fn roundtrip_all_levels()
{
	let original = lcg_bytes(2025, 50 * 1024);
	for level in 1..=9 {
		let compressed = bz2_compress(&original, level);
		let decompressed = par_decompress(&compressed);
		assert_eq!(decompressed, original, "level {level} mismatch");
	}
}

/// Level 1 with data large enough to force multiple blocks (~100KB block size).
#[test]
fn roundtrip_level1_multi_block()
{
	let original = lcg_bytes(9999, 500 * 1024);
	let compressed = bz2_compress(&original, 1);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

// ── Data patterns ──────────────────────────────────────────────────

#[test]
fn roundtrip_all_zeros()
{
	let original = vec![0u8; 100 * 1024];
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_all_ones()
{
	let original = vec![0xFFu8; 100 * 1024];
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_repeating_pattern()
{
	let pattern: Vec<u8> = (0..=255).collect();
	let original: Vec<u8> = pattern.iter().copied().cycle().take(100 * 1024).collect();
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_binary_all_byte_values()
{
	let original: Vec<u8> = (0..=255u8).collect();
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

#[test]
fn roundtrip_highly_compressible()
{
	// Long run of 'a' followed by 'b' — tests RLE.
	let original: Vec<u8> = std::iter::repeat(b'a')
		.take(500_000)
		.chain(std::iter::repeat(b'b').take(500_000))
		.collect();
	let compressed = bz2_compress(&original, 9);
	let decompressed = par_decompress(&compressed);
	assert_eq!(decompressed, original);
}

// ── Reference compatibility ────────────────────────────────────────

/// Verify our output matches the reference bzip2 crate exactly.
#[test]
fn matches_reference_decoder_small()
{
	let original = lcg_bytes(555, 10 * 1024);
	let compressed = bz2_compress(&original, 6);
	let reference = bz2_reference_decompress(&compressed);
	let ours = par_decompress(&compressed);
	assert_eq!(ours, reference);
	assert_eq!(ours, original);
}

/// Multi-block reference comparison.
#[test]
fn matches_reference_decoder_multi_block()
{
	let original = lcg_bytes(777, 2 * 1024 * 1024);
	let compressed = bz2_compress(&original, 1);
	let reference = bz2_reference_decompress(&compressed);
	let ours = par_decompress(&compressed);
	assert_eq!(ours, reference);
}

// ── Stream CRC verification ───────────────────────────────────────

/// Verify that stream_crc() returns the correct value after decompression.
#[test]
fn stream_crc_matches_stored()
{
	let original = lcg_bytes(888, 50 * 1024);
	let compressed = bz2_compress(&original, 5);
	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
	assert_eq!(out, original);
}

// ── Incremental read ───────────────────────────────────────────────

/// Read the output one byte at a time to exercise the buffer management.
#[test]
fn read_byte_at_a_time()
{
	let original = b"The quick brown fox jumps over the lazy dog.";
	let compressed = bz2_compress(original, 9);
	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();

	let mut out = Vec::new();
	let mut buf = [0u8; 1];
	loop {
		let n = decoder.read(&mut buf).unwrap();
		if n == 0 {
			break;
		}
		out.extend_from_slice(&buf[..n]);
	}
	assert_eq!(out, original);
}

/// Read with a small buffer (7 bytes) to test partial block draining.
#[test]
fn read_small_buffer()
{
	let original = lcg_bytes(111, 10 * 1024);
	let compressed = bz2_compress(&original, 9);
	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();

	let mut out = Vec::new();
	let mut buf = [0u8; 7];
	loop {
		let n = decoder.read(&mut buf).unwrap();
		if n == 0 {
			break;
		}
		out.extend_from_slice(&buf[..n]);
	}
	assert_eq!(out, original);
}

// ── File-based open ────────────────────────────────────────────────

/// Test ParBz2Decoder::open() with a temporary file.
#[test]
fn open_from_file()
{
	let original = lcg_bytes(321, 50 * 1024);
	let compressed = bz2_compress(&original, 9);

	let dir = std::env::temp_dir().join("parallel_bz2_test");
	std::fs::create_dir_all(&dir).unwrap();
	let path = dir.join("roundtrip_test.bz2");
	std::fs::write(&path, &compressed).unwrap();

	let mut decoder = ParBz2Decoder::open(&path).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);

	// Cleanup.
	let _ = std::fs::remove_file(&path);
	let _ = std::fs::remove_dir(&dir);
}

// ── Multi-stream roundtrip ─────────────────────────────────────────

/// Two concatenated streams at different levels — full roundtrip.
#[test]
fn roundtrip_multi_stream_two_levels()
{
	let a = lcg_bytes(0xAA, 8192);
	let b = lcg_bytes(0xBB, 16384);

	let mut compressed = bz2_compress(&a, 3);
	compressed.extend(bz2_compress(&b, 7));

	let mut expected = a;
	expected.extend(&b);

	let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, expected);
}

/// Three streams, each with enough data to produce multiple blocks.
#[test]
fn roundtrip_multi_stream_multi_block()
{
	let parts: Vec<Vec<u8>> = (0..3u64).map(|i| lcg_bytes(i * 111 + 1, 250_000)).collect();

	let mut compressed = Vec::new();
	let mut expected = Vec::new();
	for (i, part) in parts.iter().enumerate() {
		compressed.extend(bz2_compress(part, (i as u32 % 9) + 1));
		expected.extend_from_slice(part);
	}

	let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out.len(), expected.len());
	assert_eq!(out, expected);
}

/// Multi-stream matches independent reference decompression of each stream.
#[test]
fn multi_stream_matches_reference()
{
	let chunks: Vec<(Vec<u8>, u32)> = vec![
		(lcg_bytes(42, 4096), 5),
		(lcg_bytes(99, 8192), 2),
		(lcg_bytes(7, 2048), 9),
	];

	let mut compressed = Vec::new();
	let mut expected = Vec::new();
	for (data, level) in &chunks {
		let c = bz2_compress(data, *level);
		expected.extend(bz2_reference_decompress(&c));
		compressed.extend(c);
	}

	let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, expected);
}
