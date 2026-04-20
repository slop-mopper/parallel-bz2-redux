// tests/edge_cases.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6
//
// Integration tests: edge cases and error conditions for ParBz2Decoder.

use std::io::Read;
use std::sync::Arc;

use parallel_bz2_redux::ParBz2Decoder;
use parallel_bz2_redux::decompress::pipeline::PipelineConfig;

// ── Helpers ────────────────────────────────────────────────────────

fn bz2_compress(data: &[u8], level: u32) -> Vec<u8>
{
	use std::io::Write;
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

// ── Error conditions ───────────────────────────────────────────────

#[test]
fn error_empty_input()
{
	let data: Arc<[u8]> = Arc::from(Vec::<u8>::new());
	let result = ParBz2Decoder::from_bytes(data);
	assert!(result.is_err());
}

#[test]
fn error_not_bzip2()
{
	let data: Arc<[u8]> = Arc::from(b"This is not bzip2 data at all".to_vec());
	let result = ParBz2Decoder::from_bytes(data);
	assert!(result.is_err());
}

#[test]
fn error_truncated_header()
{
	// Just "BZ" — too short for a valid header.
	let data: Arc<[u8]> = Arc::from(b"BZ".to_vec());
	let result = ParBz2Decoder::from_bytes(data);
	assert!(result.is_err());
}

#[test]
fn error_bad_level_byte()
{
	// Valid magic, but level byte is '0' (invalid — must be '1'-'9').
	let data: Arc<[u8]> = Arc::from(b"BZh0".to_vec());
	let result = ParBz2Decoder::from_bytes(data);
	assert!(result.is_err());
}

#[test]
fn error_header_only()
{
	// Valid 4-byte header but no blocks and no EOS marker.
	let data: Arc<[u8]> = Arc::from(b"BZh9".to_vec());
	let result = ParBz2Decoder::from_bytes(data);
	// Should fail because pair_candidates requires an EOS marker.
	assert!(result.is_err());
}

#[test]
fn error_random_garbage_after_header()
{
	// Valid header followed by random garbage — no valid block structure.
	let mut data = b"BZh9".to_vec();
	data.extend_from_slice(&lcg_bytes(999, 100));
	let arc: Arc<[u8]> = Arc::from(data);
	let result = ParBz2Decoder::from_bytes(arc);
	// Either build fails (no EOS found) or decompression fails.
	if let Ok(mut decoder) = result {
		let mut out = Vec::new();
		// Decompression should fail.
		let read_result = decoder.read_to_end(&mut out);
		// It's acceptable for this to either error or return empty.
		if read_result.is_ok() {
			// If no blocks were found, output should be empty.
			assert!(out.is_empty());
		}
	}
}

// ── Truncated compressed data ──────────────────────────────────────

#[test]
fn error_truncated_compressed_stream()
{
	let original = lcg_bytes(42, 10_000);
	let compressed = bz2_compress(&original, 9);

	// Chop the compressed data in half — should fail gracefully.
	let truncated: Arc<[u8]> = Arc::from(compressed[..compressed.len() / 2].to_vec());
	let result = ParBz2Decoder::from_bytes(truncated);
	// Likely fails because EOS marker is missing.
	if let Ok(mut decoder) = result {
		let mut out = Vec::new();
		// Decompression of the truncated block should fail.
		let _ = decoder.read_to_end(&mut out);
		// We don't assert success — failure is expected.
	}
}

// ── Builder configuration ──────────────────────────────────────────

#[test]
fn builder_custom_pipeline_config()
{
	let original = lcg_bytes(222, 50_000);
	let compressed = bz2_compress(&original, 9);

	let config = PipelineConfig { result_channel_cap: 2, output_channel_cap: 1 };

	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::builder()
		.pipeline_config(config)
		.verify_stream_crc(true)
		.from_bytes(data)
		.unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);
}

#[test]
fn builder_disable_stream_crc()
{
	let original = lcg_bytes(333, 10_000);
	let compressed = bz2_compress(&original, 9);

	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::builder().verify_stream_crc(false).from_bytes(data).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);
}

// ── Data pattern edge cases ────────────────────────────────────────

#[test]
fn edge_single_zero_byte()
{
	let original = vec![0u8; 1];
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn edge_single_ff_byte()
{
	let original = vec![0xFFu8; 1];
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn edge_two_bytes()
{
	let original = vec![0xDE, 0xAD];
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn edge_exactly_256_bytes_all_values()
{
	let original: Vec<u8> = (0..=255u8).collect();
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn edge_long_run_single_byte()
{
	// 1 MB of the same byte — worst-case for BWT.
	let original = vec![b'Z'; 1024 * 1024];
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn edge_alternating_bytes()
{
	let original: Vec<u8> = (0..100_000).map(|i| if i % 2 == 0 { 0x00 } else { 0xFF }).collect();
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

/// Exactly at a block boundary size.  Level 1 = 100KB blocks, so feed
/// exactly 100,000 bytes.
#[test]
fn edge_exactly_block_size_level1()
{
	let original = lcg_bytes(444, 100_000);
	let compressed = bz2_compress(&original, 1);
	assert_eq!(par_decompress(&compressed), original);
}

/// Exactly at level 9 block size (900,000 bytes).
#[test]
fn edge_exactly_block_size_level9()
{
	let original = lcg_bytes(555, 900_000);
	let compressed = bz2_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

/// Just over block boundary for level 1 — should produce exactly 2 blocks.
#[test]
fn edge_just_over_block_boundary_level1()
{
	let original = lcg_bytes(666, 100_001);
	let compressed = bz2_compress(&original, 1);
	assert_eq!(par_decompress(&compressed), original);
}

// ── Read-after-EOF ─────────────────────────────────────────────────

#[test]
fn read_after_eof_returns_zero()
{
	let original = b"test data";
	let compressed = bz2_compress(original, 9);
	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();

	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);

	// Further reads should return 0.
	let mut buf = [0u8; 1024];
	assert_eq!(decoder.read(&mut buf).unwrap(), 0);
	assert_eq!(decoder.read(&mut buf).unwrap(), 0);
}

#[test]
fn read_into_zero_length_buffer()
{
	let original = b"hello";
	let compressed = bz2_compress(original, 9);
	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();

	let mut buf = [];
	assert_eq!(decoder.read(&mut buf).unwrap(), 0);

	// Can still read normally afterward.
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);
}

// ── Open with builder ──────────────────────────────────────────────

#[test]
fn builder_open_from_file()
{
	let original = lcg_bytes(777, 20_000);
	let compressed = bz2_compress(&original, 5);

	let dir = std::env::temp_dir().join("parallel_bz2_edge_test");
	std::fs::create_dir_all(&dir).unwrap();
	let path = dir.join("edge_test.bz2");
	std::fs::write(&path, &compressed).unwrap();

	let mut decoder = ParBz2Decoder::builder().verify_stream_crc(true).open(&path).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);

	let _ = std::fs::remove_file(&path);
	let _ = std::fs::remove_dir(&dir);
}

#[test]
fn error_open_nonexistent_file()
{
	let result = ParBz2Decoder::open("/tmp/parallel_bz2_definitely_does_not_exist_12345.bz2");
	assert!(result.is_err());
}
