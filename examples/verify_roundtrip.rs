// examples/verify_roundtrip.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6
//
//! Full round-trip verification: compresses files with `ParBz2Encoder`,
//! decompresses with `ParBz2Decoder`, and verifies the output matches
//! the original input byte-for-byte.
//!
//! Usage:
//!     cargo run --example verify_roundtrip -- file1 file2 ...
//!
//! Exits 0 if all files match, 1 if any mismatch or error is found.

use std::env;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use parallel_bz2_redux::ParBz2Decoder;
use parallel_bz2_redux::ParBz2Encoder;

/// Chunk size for streaming I/O (64 KiB).
const CHUNK_SIZE: usize = 64 * 1024;

/// Maximum number of files to verify concurrently.
const MAX_CONCURRENT: usize = 8;

// ── Helpers ────────────────────────────────────────────────────────

struct HumanBytes(u64);

impl fmt::Display for HumanBytes
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
	{
		let b = self.0;
		if b < 1024 {
			write!(f, "{b} B")
		} else if b < 1024 * 1024 {
			write!(f, "{:.1} KiB", b as f64 / 1024.0)
		} else if b < 1024 * 1024 * 1024 {
			write!(f, "{:.1} MiB", b as f64 / (1024.0 * 1024.0))
		} else {
			write!(f, "{:.2} GiB", b as f64 / (1024.0 * 1024.0 * 1024.0))
		}
	}
}

fn basename(path: &Path) -> &str
{
	path.file_name().and_then(|n| n.to_str()).unwrap_or("?")
}

fn spinner_style() -> ProgressStyle
{
	ProgressStyle::with_template("{spinner:.cyan} {prefix}  {msg}").unwrap()
}

fn bar_style() -> ProgressStyle
{
	ProgressStyle::with_template(
		"{spinner:.cyan} {prefix}  {msg}  [{bar:20.green/dim}] {bytes}/{total_bytes} ({percent}%)",
	)
	.unwrap()
	.progress_chars("=> ")
}

fn read_exact_or_eof(reader: &mut impl Read, buf: &mut [u8]) -> std::io::Result<usize>
{
	let mut filled = 0;
	while filled < buf.len() {
		match reader.read(&mut buf[filled..]) {
			Ok(0) => break,
			Ok(n) => filled += n,
			Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
			Err(e) => return Err(e),
		}
	}
	Ok(filled)
}

// ── Per-file verification ──────────────────────────────────────────

/// Returns (original_bytes, compressed_bytes, compress_ms, decompress_ms).
fn verify_file(path: &Path, pb: &ProgressBar) -> Result<(u64, u64, u128, u128), String>
{
	let original_size = std::fs::metadata(path).map(|m| m.len()).map_err(|e| format!("stat: {e}"))?;

	// ── Pass 1: compress with ParBz2Encoder ────────────────────
	pb.set_style(bar_style());
	pb.set_length(original_size);
	pb.set_position(0);
	pb.set_message("compress");
	pb.enable_steady_tick(std::time::Duration::from_millis(100));

	let mut input = BufReader::new(File::open(path).map_err(|e| format!("open: {e}"))?);
	let mut compressed = Vec::new();
	let mut encoder = ParBz2Encoder::new(&mut compressed, 9).map_err(|e| format!("encoder init: {e}"))?;

	let compress_start = Instant::now();
	let mut buf = vec![0u8; CHUNK_SIZE];
	let mut written: u64 = 0;
	loop {
		match input.read(&mut buf) {
			Ok(0) => break,
			Ok(n) => {
				encoder.write_all(&buf[..n]).map_err(|e| format!("compress write: {e}"))?;
				written += n as u64;
				pb.set_position(written);
			}
			Err(e) => return Err(format!("read input: {e}")),
		}
	}
	encoder.finish().map_err(|e| format!("encoder finish: {e}"))?;
	let compress_ms = compress_start.elapsed().as_millis();
	let compressed_size = compressed.len() as u64;

	// Keep compressed data as Arc for decoder use.
	let compressed: Arc<[u8]> = Arc::from(compressed);

	// ── Pass 2: decompress with ParBz2Decoder (timed) ──────────
	pb.set_position(0);
	pb.set_length(original_size);
	pb.set_message("decompress");

	let decompress_start = Instant::now();
	let mut decoder = ParBz2Decoder::from_bytes(Arc::clone(&compressed)).map_err(|e| format!("decoder init: {e}"))?;
	let mut dec_bytes: u64 = 0;
	loop {
		match decoder.read(&mut buf) {
			Ok(0) => break,
			Ok(n) => {
				dec_bytes += n as u64;
				pb.set_position(dec_bytes);
			}
			Err(e) => return Err(format!("par decode: {e}")),
		}
	}
	let decompress_ms = decompress_start.elapsed().as_millis();
	drop(decoder);

	if dec_bytes != original_size {
		return Err(format!(
			"length mismatch: original={} roundtrip={}",
			HumanBytes(original_size),
			HumanBytes(dec_bytes),
		));
	}

	// ── Pass 3: decompress again and compare against original ──
	pb.set_position(0);
	pb.set_message("comparing");

	let mut orig = BufReader::new(File::open(path).map_err(|e| format!("reopen: {e}"))?);
	let mut decoder2 = ParBz2Decoder::from_bytes(compressed).map_err(|e| format!("decoder2 init: {e}"))?;

	let mut orig_buf = vec![0u8; CHUNK_SIZE];
	let mut dec_buf = vec![0u8; CHUNK_SIZE];
	let mut offset: u64 = 0;

	loop {
		let orig_n = read_exact_or_eof(&mut orig, &mut orig_buf).map_err(|e| format!("read original: {e}"))?;
		let dec_n = read_exact_or_eof(&mut decoder2, &mut dec_buf).map_err(|e| format!("decode cmp: {e}"))?;

		if orig_n != dec_n {
			return Err(format!(
				"length mismatch at offset {}: original={} decoded={}",
				offset,
				offset + orig_n as u64,
				offset + dec_n as u64,
			));
		}
		if orig_n == 0 {
			break;
		}

		if orig_buf[..orig_n] != dec_buf[..dec_n] {
			for i in 0..orig_n {
				if orig_buf[i] != dec_buf[i] {
					return Err(format!("mismatch at byte offset {}", offset + i as u64));
				}
			}
		}
		offset += orig_n as u64;
		pb.set_position(offset);
	}

	pb.finish_and_clear();
	Ok((original_size, compressed_size, compress_ms, decompress_ms))
}

// ── Main ───────────────────────────────────────────────────────────

fn main()
{
	let args: Vec<String> = env::args().collect();
	if args.len() < 2 {
		eprintln!("Usage: {} <file1> [file2 ...]", args[0]);
		std::process::exit(2);
	}

	let files: Vec<PathBuf> = args[1..].iter().map(PathBuf::from).collect();
	let total = files.len();
	let failures = AtomicUsize::new(0);
	let successes = AtomicUsize::new(0);

	let mp = MultiProgress::new();
	println!("Verifying round-trip of {total} file(s), up to {MAX_CONCURRENT} concurrently\n");

	let failures = &failures;
	let successes = &successes;
	let mp = &mp;

	for batch in files.chunks(MAX_CONCURRENT) {
		let bars: Vec<ProgressBar> = batch
			.iter()
			.map(|path| {
				let pb = mp.add(ProgressBar::new(0));
				pb.set_style(spinner_style());
				pb.set_prefix(basename(path).to_string());
				pb
			})
			.collect();

		std::thread::scope(|s| {
			let handles: Vec<_> = batch
				.iter()
				.zip(bars.iter())
				.map(|(path, pb)| {
					s.spawn(move || {
						let name = basename(path);

						match verify_file(path, pb) {
							Ok((original_size, compressed_size, compress_ms, decompress_ms)) => {
								let ratio = if original_size > 0 {
									compressed_size as f64 / original_size as f64 * 100.0
								} else {
									0.0
								};
								pb.finish_and_clear();
								mp.suspend(|| {
									println!(
										"[ OK ] {name}  original={orig}  compressed={cmp} ({ratio:.1}%)  \
										 enc={compress_ms}ms  dec={decompress_ms}ms",
										orig = HumanBytes(original_size),
										cmp = HumanBytes(compressed_size),
									);
								});
								successes.fetch_add(1, Ordering::Relaxed);
							}
							Err(msg) => {
								pb.finish_and_clear();
								mp.suspend(|| {
									println!("[FAIL] {name}: {msg}");
								});
								failures.fetch_add(1, Ordering::Relaxed);
							}
						}
					})
				})
				.collect();

			for h in handles {
				h.join().expect("worker thread panicked");
			}
		});
	}

	let ok = successes.load(Ordering::Relaxed);
	let fail = failures.load(Ordering::Relaxed);
	println!("\n{ok}/{total} passed, {fail} failed");

	if fail > 0 {
		std::process::exit(1);
	}
}
