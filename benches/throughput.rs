// benches/throughput.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Compression and decompression throughput benchmarks.
//!
//! Compares parallel (this crate) against serial (`bzip2` crate) performance
//! across four data types and six input sizes.
//!
//! # Running
//!
//! ```sh
//! cargo bench                           # full suite (takes a while)
//! cargo bench -- "compress/lipsum"      # one data type
//! cargo bench -- "decompress"           # all decompression
//! cargo bench -- "100MB"                # one size across all groups
//! cargo bench -- "parallel"             # only parallel variants
//! ```

use std::io::Read;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::SamplingMode;
use criterion::Throughput;
use criterion::criterion_group;
use criterion::criterion_main;
use parallel_bz2_redux::ParBz2Decoder;
use parallel_bz2_redux::ParBz2Encoder;

const LEVEL: u8 = 9;

const SIZES: [(usize, &str); 6] = [
	(100_000, "100KB"),
	(1_000_000, "1MB"),
	(10_000_000, "10MB"),
	(50_000_000, "50MB"),
	(100_000_000, "100MB"),
	(200_000_000, "200MB"),
];

// ── Data generators ────────────────────────────────────────────────

/// Text data via lipsum Markov chain (high compressibility, ~3-4x).
fn gen_lipsum(size: usize) -> Vec<u8>
{
	// Average lipsum word is ~6 bytes including space.
	// Overshoot to ensure we have enough, then truncate.
	let est_words = size / 5 + 1000;
	let text = lipsum::lipsum(est_words);
	let mut bytes = text.into_bytes();
	bytes.truncate(size);
	// Safety net: pad if lipsum was somehow too short.
	bytes.resize(size, b' ');
	bytes
}

/// Binary data with restricted alphabet — 64 distinct byte values
/// (6 bits of entropy per byte). Moderate compressibility.
fn gen_restricted(size: usize) -> Vec<u8>
{
	let mut data = Vec::with_capacity(size);
	let mut s: u64 = 0xDEAD_BEEF_CAFE_1234;
	for _ in 0..size {
		s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		data.push(((s >> 33) & 0x3F) as u8);
	}
	data
}

/// Binary data with byte-to-byte correlation — each byte is close to
/// its predecessor (delta -8..+7). Mimics sensor or audio data.
fn gen_correlated(size: usize) -> Vec<u8>
{
	let mut data = Vec::with_capacity(size);
	let mut s: u64 = 0x1234_5678_9ABC_DEF0;
	let mut prev: u8 = 128;
	for _ in 0..size {
		s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		let delta = ((s >> 33) as i8) >> 4; // arithmetic shift → -8..+7
		prev = prev.wrapping_add(delta as u8);
		data.push(prev);
	}
	data
}

/// Uniform random bytes — 8 bits of entropy per byte.
/// Near-zero compressibility (output ≈ input size).
fn gen_random(size: usize) -> Vec<u8>
{
	let mut data = Vec::with_capacity(size);
	let mut s: u64 = 0xFEED_FACE_DEAD_BEEF;
	for _ in 0..size {
		s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		data.push((s >> 33) as u8);
	}
	data
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Compress data with the standard serial bzip2 encoder (for pre-compressing
/// decompression benchmark inputs).
fn compress_reference(data: &[u8]) -> Vec<u8>
{
	let mut enc = bzip2::write::BzEncoder::new(Vec::with_capacity(data.len()), bzip2::Compression::new(LEVEL as u32));
	enc.write_all(data).unwrap();
	enc.finish().unwrap()
}

/// Apply criterion settings for the given input size.
fn configure_group(group: &mut criterion::BenchmarkGroup<criterion::measurement::WallTime>, size: usize)
{
	group.throughput(Throughput::Bytes(size as u64));
	group.sampling_mode(SamplingMode::Flat);
	group.sample_size(20);
	group.warm_up_time(Duration::from_secs(10));
	// group.measurement_time(Duration::from_secs(120));
}

// ── Benchmarks ──────────────────────────────────────────────────────

type DataGen = fn(usize) -> Vec<u8>;

const DATA_TYPES: [(&str, DataGen); 4] = [
	("lipsum", gen_lipsum as DataGen),
	("restricted", gen_restricted as DataGen),
	("correlated", gen_correlated as DataGen),
	("random", gen_random as DataGen),
];

/// Benchmark compression: parallel (this crate) vs serial (bzip2 crate).
fn bench_compress(c: &mut Criterion)
{
	for (dtype, make_data) in &DATA_TYPES {
		let mut group = c.benchmark_group(format!("compress/{dtype}"));

		for &(size, label) in &SIZES {
			configure_group(&mut group, size);
			let data = make_data(size);

			// Parallel encoder
			group.bench_with_input(BenchmarkId::new("parallel", label), &data, |b, data| {
				b.iter(|| {
					let mut out = Vec::with_capacity(data.len());
					{
						let mut enc = ParBz2Encoder::new(&mut out, LEVEL).unwrap();
						enc.write_all(data).unwrap();
						enc.finish().unwrap();
					}
					out
				});
			});

			// Serial reference encoder
			group.bench_with_input(BenchmarkId::new("serial", label), &data, |b, data| {
				b.iter(|| {
					let mut enc = bzip2::write::BzEncoder::new(
						Vec::with_capacity(data.len()),
						bzip2::Compression::new(LEVEL as u32),
					);
					enc.write_all(data).unwrap();
					enc.finish().unwrap()
				});
			});
		}

		group.finish();
	}
}

/// Benchmark decompression: parallel (this crate) vs serial (bzip2 crate).
///
/// Input data is pre-compressed with the serial encoder during setup
/// (not measured).
fn bench_decompress(c: &mut Criterion)
{
	for (dtype, make_data) in &DATA_TYPES {
		let mut group = c.benchmark_group(format!("decompress/{dtype}"));

		for &(size, label) in &SIZES {
			configure_group(&mut group, size);

			// Generate and pre-compress (not measured).
			let data = make_data(size);
			let compressed: Arc<[u8]> = Arc::from(compress_reference(&data));

			// Parallel decoder
			group.bench_with_input(BenchmarkId::new("parallel", label), &compressed, |b, compressed| {
				b.iter(|| {
					let mut dec = ParBz2Decoder::from_bytes(Arc::clone(compressed)).unwrap();
					let mut out = Vec::with_capacity(size);
					dec.read_to_end(&mut out).unwrap();
					out
				});
			});

			// Serial reference decoder
			group.bench_with_input(BenchmarkId::new("serial", label), &compressed, |b, compressed| {
				b.iter(|| {
					let mut dec = bzip2::read::BzDecoder::new(&**compressed);
					let mut out = Vec::with_capacity(size);
					dec.read_to_end(&mut out).unwrap();
					out
				});
			});
		}

		group.finish();
	}
}

criterion_group!(benches, bench_compress, bench_decompress);
criterion_main!(benches);
