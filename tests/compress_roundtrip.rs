// tests/compress_roundtrip.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6
//
// Integration tests: compress with ParBz2Encoder, decompress with both
// the reference bzip2 crate and our ParBz2Decoder, verify byte-for-byte
// equality.  Also tests full bidirectional roundtrips.

use std::io::Read;
use std::io::Write;
use std::sync::Arc;

use parallel_bz2_redux::ParBz2Decoder;
use parallel_bz2_redux::ParBz2Encoder;
use parallel_bz2_redux::ParBz2EncoderBuilder;
use parallel_bz2_redux::compress::max_block_bytes;

// ── Helpers ────────────────────────────────────────────────────────

/// Compress data using the reference bzip2 crate.
fn bz2_compress(data: &[u8], level: u32) -> Vec<u8>
{
	let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
	enc.write_all(data).unwrap();
	enc.finish().unwrap()
}

/// Decompress using the reference bzip2 crate.
fn bz2_decompress(compressed: &[u8]) -> Vec<u8>
{
	let mut dec = bzip2::read::BzDecoder::new(compressed);
	let mut out = Vec::new();
	dec.read_to_end(&mut out).unwrap();
	out
}

/// Compress data using our parallel encoder.
fn par_compress(data: &[u8], level: u8) -> Vec<u8>
{
	let mut output = Vec::new();
	{
		let mut enc = ParBz2Encoder::new(&mut output, level).unwrap();
		enc.write_all(data).unwrap();
		enc.finish().unwrap();
	}
	output
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

/// Simple LCG pseudo-random data generator (deterministic).
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

// ── Our encoder → reference decoder (cross-compat) ─────────────────

#[test]
fn par_compress_ref_decompress_small()
{
	let original = b"Hello, parallel bzip2 compression!";
	let compressed = par_compress(original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn par_compress_ref_decompress_1kb()
{
	let original = lcg_bytes(10, 1024);
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn par_compress_ref_decompress_100kb()
{
	let original = lcg_bytes(20, 100 * 1024);
	let compressed = par_compress(&original, 6);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn par_compress_ref_decompress_1mb()
{
	let original = lcg_bytes(30, 1024 * 1024);
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

/// 5 MB — forces many blocks even at level 9.
#[test]
fn par_compress_ref_decompress_5mb()
{
	let original = lcg_bytes(40, 5 * 1024 * 1024);
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn par_compress_ref_decompress_all_levels()
{
	let original = lcg_bytes(50, 50_000);
	for level in 1..=9u8 {
		let compressed = par_compress(&original, level);
		// Header must have the correct level byte.
		assert_eq!(compressed[3], b'0' + level, "level {level}: wrong header");
		let decompressed = bz2_decompress(&compressed);
		assert_eq!(decompressed, original, "level {level}: mismatch");
	}
}

/// Level 1 with enough data to produce many blocks (~5 blocks).
#[test]
fn par_compress_ref_decompress_level1_multi_block()
{
	let original = lcg_bytes(60, 500_000);
	let compressed = par_compress(&original, 1);
	assert_eq!(bz2_decompress(&compressed), original);
}

// ── Full roundtrip: our encoder → our decoder ───────────────────

#[test]
fn full_roundtrip_small()
{
	let original = b"Full roundtrip integration test.";
	let compressed = par_compress(original, 9);
	assert_eq!(par_decompress(&compressed), original.as_slice());
}

#[test]
fn full_roundtrip_100kb()
{
	let original = lcg_bytes(70, 100 * 1024);
	let compressed = par_compress(&original, 6);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn full_roundtrip_1mb()
{
	let original = lcg_bytes(80, 1024 * 1024);
	let compressed = par_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn full_roundtrip_5mb()
{
	let original = lcg_bytes(90, 5 * 1024 * 1024);
	let compressed = par_compress(&original, 9);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn full_roundtrip_all_levels()
{
	let original = lcg_bytes(100, 50_000);
	for level in 1..=9u8 {
		let compressed = par_compress(&original, level);
		let decompressed = par_decompress(&compressed);
		assert_eq!(decompressed, original, "level {level}: mismatch");
	}
}

#[test]
fn full_roundtrip_multi_block_level1()
{
	let original = lcg_bytes(110, 500_000);
	let compressed = par_compress(&original, 1);
	assert_eq!(par_decompress(&compressed), original);
}

// ── Bidirectional cross-compatibility ───────────────────────────

/// bzip2 → us, and us → bzip2, produce the same output for the same input.
#[test]
fn bidirectional_cross_compat()
{
	let original = lcg_bytes(120, 100_000);

	// Reference compresses, we decompress.
	let ref_compressed = bz2_compress(&original, 6);
	let ours_from_ref = par_decompress(&ref_compressed);

	// We compress, reference decompresses.
	let our_compressed = par_compress(&original, 6);
	let ref_from_ours = bz2_decompress(&our_compressed);

	// Both must produce the same original data.
	assert_eq!(ours_from_ref, original);
	assert_eq!(ref_from_ours, original);
}

/// Bidirectional for a multi-block scenario.
#[test]
fn bidirectional_cross_compat_multi_block()
{
	let original = lcg_bytes(130, 500_000);

	let ref_compressed = bz2_compress(&original, 1);
	let our_compressed = par_compress(&original, 1);

	// Cross-decode.
	assert_eq!(bz2_decompress(&our_compressed), original);
	assert_eq!(par_decompress(&ref_compressed), original);
}

// ── Data patterns ───────────────────────────────────────────────

#[test]
fn compress_roundtrip_all_zeros()
{
	let original = vec![0u8; 100_000];
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn compress_roundtrip_all_0xff()
{
	let original = vec![0xFFu8; 100_000];
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
	assert_eq!(par_decompress(&compressed), original);
}

#[test]
fn compress_roundtrip_repeating_pattern()
{
	let pattern: Vec<u8> = (0..=255).collect();
	let original: Vec<u8> = pattern.iter().copied().cycle().take(200_000).collect();
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn compress_roundtrip_highly_compressible()
{
	// Long runs of same byte — highly compressible.
	let original: Vec<u8> = std::iter::repeat_n(b'a', 500_000).chain(std::iter::repeat_n(b'b', 500_000)).collect();
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn compress_roundtrip_binary_all_byte_values()
{
	let original: Vec<u8> = (0..=255u8).collect();
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

#[test]
fn compress_roundtrip_single_byte()
{
	let original = b"X";
	let compressed = par_compress(original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

// ── Block boundary edge cases ──────────────────────────────────

/// Exactly at the block boundary for level 1.
#[test]
fn compress_exact_block_boundary_level1()
{
	let max = max_block_bytes(1);
	let original = lcg_bytes(200, max);
	let compressed = par_compress(&original, 1);
	assert_eq!(bz2_decompress(&compressed), original);
}

/// One byte over the block boundary — should produce exactly 2 blocks.
#[test]
fn compress_just_over_block_boundary_level1()
{
	let max = max_block_bytes(1);
	let original = lcg_bytes(210, max + 1);
	let compressed = par_compress(&original, 1);
	assert_eq!(bz2_decompress(&compressed), original);
	assert_eq!(par_decompress(&compressed), original);
}

/// Exactly at block boundary for level 9.
#[test]
fn compress_exact_block_boundary_level9()
{
	let max = max_block_bytes(9);
	let original = lcg_bytes(220, max);
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

/// One byte over the block boundary for level 9.
#[test]
fn compress_just_over_block_boundary_level9()
{
	let max = max_block_bytes(9);
	let original = lcg_bytes(230, max + 1);
	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

// ── Incremental writes ─────────────────────────────────────────

/// Write one byte at a time to the encoder.
#[test]
fn compress_incremental_byte_at_a_time()
{
	let original = lcg_bytes(300, 4096);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
		for &b in &original {
			enc.write_all(&[b]).unwrap();
		}
		enc.finish().unwrap();
	}
	assert_eq!(bz2_decompress(&output), original);
}

/// Write in small chunks (13 bytes) to test buffer boundary handling.
#[test]
fn compress_incremental_small_chunks()
{
	let original = lcg_bytes(310, 50_000);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2Encoder::new(&mut output, 6).unwrap();
		for chunk in original.chunks(13) {
			enc.write_all(chunk).unwrap();
		}
		enc.finish().unwrap();
	}
	assert_eq!(bz2_decompress(&output), original);
}

/// Incremental writes crossing multiple block boundaries.
#[test]
fn compress_incremental_crossing_blocks()
{
	let original = lcg_bytes(320, 500_000);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2Encoder::new(&mut output, 1).unwrap();
		for chunk in original.chunks(997) {
			enc.write_all(chunk).unwrap();
		}
		enc.finish().unwrap();
	}
	assert_eq!(bz2_decompress(&output), original);
	assert_eq!(par_decompress(&output), original);
}

// ── Multi-stream encoding (concatenated) ────────────────────────

/// Encode two separate streams with our encoder, concatenate,
/// decode with our multi-stream decoder.
#[test]
fn multi_stream_encode_decode()
{
	let a = lcg_bytes(400, 10_000);
	let b = lcg_bytes(410, 20_000);

	let mut combined = Vec::new();
	combined.extend(par_compress(&a, 5));
	combined.extend(par_compress(&b, 7));

	let mut expected = a;
	expected.extend(&b);

	assert_eq!(par_decompress(&combined), expected);
}

/// Three streams at different levels, each multi-block.
#[test]
fn multi_stream_encode_decode_multi_block()
{
	let parts: Vec<(Vec<u8>, u8)> = vec![
		(lcg_bytes(420, 250_000), 1),
		(lcg_bytes(430, 100_000), 5),
		(lcg_bytes(440, 200_000), 3),
	];

	let mut combined = Vec::new();
	let mut expected = Vec::new();
	for (data, level) in &parts {
		combined.extend(par_compress(data, *level));
		expected.extend_from_slice(data);
	}

	assert_eq!(par_decompress(&combined), expected);
}

/// Our encoder → reference decoder for multi-stream.
#[test]
fn multi_stream_our_encode_ref_decode()
{
	let a = lcg_bytes(450, 8192);
	let b = lcg_bytes(460, 16384);

	let stream_a = par_compress(&a, 9);
	let stream_b = par_compress(&b, 9);

	// Each stream should be independently decodable by the reference.
	assert_eq!(bz2_decompress(&stream_a), a);
	assert_eq!(bz2_decompress(&stream_b), b);
}

/// Mixed: our encoder + reference encoder, concatenated → our decoder.
#[test]
fn multi_stream_mixed_encoders()
{
	let a = lcg_bytes(470, 10_000);
	let b = lcg_bytes(480, 20_000);
	let c = lcg_bytes(490, 15_000);

	let mut combined = Vec::new();
	combined.extend(par_compress(&a, 9)); // our encoder
	combined.extend(bz2_compress(&b, 6)); // reference encoder
	combined.extend(par_compress(&c, 3)); // our encoder

	let mut expected = a;
	expected.extend(&b);
	expected.extend(&c);

	assert_eq!(par_decompress(&combined), expected);
}

// ── Encoder with embedded magic patterns ────────────────────────

/// Input that contains the block magic pattern (0x314159265359).
/// Our encoder must handle this correctly.
#[test]
fn compress_input_with_block_magic()
{
	let magic: [u8; 6] = [0x31, 0x41, 0x59, 0x26, 0x53, 0x59];
	let mut original = Vec::with_capacity(100_000);
	original.extend_from_slice(&lcg_bytes(500, 50_000));
	for _ in 0..100 {
		original.extend_from_slice(&magic);
		original.extend_from_slice(&lcg_bytes(510, 500));
	}
	original.extend_from_slice(&lcg_bytes(520, 50_000));

	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
	assert_eq!(par_decompress(&compressed), original);
}

/// Input with EOS magic pattern (0x177245385090).
#[test]
fn compress_input_with_eos_magic()
{
	let eos_magic: [u8; 6] = [0x17, 0x72, 0x45, 0x38, 0x50, 0x90];
	let mut original = Vec::with_capacity(100_000);
	original.extend_from_slice(&lcg_bytes(530, 50_000));
	for _ in 0..50 {
		original.extend_from_slice(&eos_magic);
		original.extend_from_slice(&lcg_bytes(540, 1000));
	}
	original.extend_from_slice(&lcg_bytes(550, 50_000));

	let compressed = par_compress(&original, 9);
	assert_eq!(bz2_decompress(&compressed), original);
}

// ── Builder integration tests ───────────────────────────────────

#[test]
fn builder_compress_decompress()
{
	let original = lcg_bytes(600, 50_000);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2EncoderBuilder::new().level(4).build(&mut output).unwrap();
		enc.write_all(&original).unwrap();
		enc.finish().unwrap();
	}
	assert_eq!(output[3], b'4');
	assert_eq!(bz2_decompress(&output), original);
	assert_eq!(par_decompress(&output), original);
}

#[test]
fn builder_default_level_roundtrip()
{
	let original = lcg_bytes(610, 10_000);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2EncoderBuilder::default().build(&mut output).unwrap();
		enc.write_all(&original).unwrap();
		enc.finish().unwrap();
	}
	// Default level is 9.
	assert_eq!(output[3], b'9');
	assert_eq!(par_decompress(&output), original);
}

// ── File-based compression ──────────────────────────────────────

#[test]
fn compress_to_file_and_decode()
{
	let original = lcg_bytes(700, 50_000);

	let dir = std::env::temp_dir().join("parallel_bz2_compress_test");
	std::fs::create_dir_all(&dir).unwrap();
	let path = dir.join("compress_roundtrip.bz2");

	// Compress to file.
	{
		let file = std::fs::File::create(&path).unwrap();
		let mut enc = ParBz2Encoder::new(file, 9).unwrap();
		enc.write_all(&original).unwrap();
		enc.finish().unwrap();
	}

	// Decode from file with our decoder.
	let mut decoder = ParBz2Decoder::open(&path).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);

	// Also verify with reference decoder.
	let compressed = std::fs::read(&path).unwrap();
	assert_eq!(bz2_decompress(&compressed), original);

	let _ = std::fs::remove_file(&path);
	let _ = std::fs::remove_dir(&dir);
}

// ── Stream CRC verification ────────────────────────────────────

/// Verify that data compressed with our encoder passes the decoder's
/// stream CRC check.
#[test]
fn stream_crc_verified_after_roundtrip()
{
	let original = lcg_bytes(800, 100_000);
	let compressed = par_compress(&original, 6);

	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::builder().verify_stream_crc(true).from_bytes(data).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);
	assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
}

/// Multi-block CRC verification.
#[test]
fn stream_crc_verified_multi_block()
{
	let original = lcg_bytes(810, 500_000);
	let compressed = par_compress(&original, 1);

	let data: Arc<[u8]> = Arc::from(compressed);
	let mut decoder = ParBz2Decoder::builder().verify_stream_crc(true).from_bytes(data).unwrap();
	let mut out = Vec::new();
	decoder.read_to_end(&mut out).unwrap();
	assert_eq!(out, original);
	assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
}

// ── Drop finalization integration ───────────────────────────────

/// Encoder dropped without finish() still produces valid output.
#[test]
fn drop_finalization_produces_valid_output()
{
	let original = lcg_bytes(900, 10_000);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
		enc.write_all(&original).unwrap();
		// No finish() call — drop should finalize.
	}
	assert_eq!(&output[..3], b"BZh");
	assert_eq!(bz2_decompress(&output), original);
	assert_eq!(par_decompress(&output), original);
}

/// Drop finalization with multi-block data.
#[test]
fn drop_finalization_multi_block()
{
	let original = lcg_bytes(910, 300_000);
	let mut output = Vec::new();
	{
		let mut enc = ParBz2Encoder::new(&mut output, 1).unwrap();
		enc.write_all(&original).unwrap();
		// Drop without finish.
	}
	assert_eq!(bz2_decompress(&output), original);
}

// ── Empty data ─────────────────────────────────────────────────

/// Compressing empty data should produce a valid (empty) bzip2 stream.
#[test]
fn compress_empty_data()
{
	let compressed = par_compress(b"", 9);
	assert_eq!(&compressed[..3], b"BZh");
	assert_eq!(bz2_decompress(&compressed), b"");
}

/// Empty data roundtrip through our encoder → our decoder.
#[test]
fn compress_empty_full_roundtrip()
{
	let compressed = par_compress(b"", 9);
	assert_eq!(par_decompress(&compressed), b"");
}

// ── Streaming output ───────────────────────────────────────────

/// Verify that writing multi-block data produces incremental output
/// (not everything buffered until finish).
#[test]
fn encoder_streams_incrementally()
{
	let original = lcg_bytes(1000, 500_000);
	let mut output = Vec::new();
	let mut enc = ParBz2EncoderBuilder::new().level(1).min_blocks(1).build(&mut output).unwrap();

	// Write enough data for multiple blocks at level 1.
	enc.write_all(&original).unwrap();

	// Output should already contain some compressed data.
	let partial_len = enc.get_ref().len();
	assert!(partial_len > 0, "encoder should stream output incrementally");

	let compressed = enc.finish().unwrap();
	assert!(compressed.len() > partial_len);
	assert_eq!(bz2_decompress(compressed), original);
}
