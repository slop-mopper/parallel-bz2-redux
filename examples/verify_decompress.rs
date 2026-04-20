// examples/verify_decompress.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6
//
//! End-to-end verification binary: decompresses bz2 files with both
//! `ParBz2Decoder` (parallel) and `bzip2::read::BzDecoder` (reference),
//! comparing output byte-by-byte in a streaming fashion.
//!
//! Usage:
//!     cargo run --example verify_decompress -- file1.bz2 file2.bz2 ...
//!
//! Exits 0 if all files match, 1 if any mismatch or error is found.

use std::env;
use std::fmt;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Instant;

use indicatif::MultiProgress;
use indicatif::ProgressBar;
use indicatif::ProgressStyle;
use parallel_bz2_redux::ParBz2Decoder;

/// Chunk size for streaming comparison (64 KiB).
const CHUNK_SIZE: usize = 64 * 1024;

/// Maximum number of files to verify concurrently.
const MAX_CONCURRENT: usize = 8;

// ── Helpers ────────────────────────────────────────────────────────

/// Human-readable byte count.
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

/// Style for the spinner phase (unknown total, e.g. reference decode).
fn spinner_style() -> ProgressStyle
{
	ProgressStyle::with_template("{spinner:.cyan} {prefix}  {msg}").unwrap()
}

/// Style for the bar phase (known total, e.g. parallel decode / compare).
fn bar_style() -> ProgressStyle
{
	ProgressStyle::with_template(
		"{spinner:.cyan} {prefix}  {msg}  [{bar:20.green/dim}] {bytes}/{total_bytes} ({percent}%)",
	)
	.unwrap()
	.progress_chars("=> ")
}

// ── Per-file verification ──────────────────────────────────────────

/// Decompress `path` with both our parallel decoder and the reference
/// `bzip2` crate, comparing chunks as they are produced.
///
/// Progress is reported on `pb`.
fn verify_file(path: &Path, pb: &ProgressBar) -> Result<(u64, u128, u128), String>
{
	// ── Pass 1: reference decode (unknown total) ───────────────
	pb.set_style(spinner_style());
	pb.set_message("ref decode");
	pb.enable_steady_tick(std::time::Duration::from_millis(100));

	let ref_file = File::open(path).map_err(|e| format!("open for ref decoder: {e}"))?;
	let mut ref_decoder = bzip2::read::BzDecoder::new(BufReader::new(ref_file));

	let ref_start = Instant::now();
	let mut ref_bytes: u64 = 0;
	let mut buf = vec![0u8; CHUNK_SIZE];
	loop {
		match ref_decoder.read(&mut buf) {
			Ok(0) => break,
			Ok(n) => {
				ref_bytes += n as u64;
				pb.set_message(format!("ref decode  {}", HumanBytes(ref_bytes)));
			}
			Err(e) => return Err(format!("reference decoder: {e}")),
		}
	}
	let reference_ms = ref_start.elapsed().as_millis();
	drop(ref_decoder);

	// ── Pass 2: parallel decode (total known) ──────────────────
	pb.set_style(bar_style());
	pb.set_length(ref_bytes);
	pb.set_position(0);
	pb.set_message("par decode");

	let par_start = Instant::now();
	let mut par_decoder = ParBz2Decoder::open(path).map_err(|e| format!("ParBz2Decoder::open: {e}"))?;
	let mut par_bytes: u64 = 0;
	loop {
		match par_decoder.read(&mut buf) {
			Ok(0) => break,
			Ok(n) => {
				par_bytes += n as u64;
				pb.set_position(par_bytes);
			}
			Err(e) => return Err(format!("parallel decoder: {e}")),
		}
	}
	let parallel_ms = par_start.elapsed().as_millis();
	drop(par_decoder);

	if par_bytes != ref_bytes {
		return Err(format!(
			"length mismatch: parallel={} reference={}",
			HumanBytes(par_bytes),
			HumanBytes(ref_bytes),
		));
	}

	// ── Pass 3: streaming comparison ───────────────────────────
	pb.set_position(0);
	pb.set_message("comparing");

	let ref_file2 = File::open(path).map_err(|e| format!("open for comparison: {e}"))?;
	let mut ref_dec2 = bzip2::read::BzDecoder::new(BufReader::new(ref_file2));
	let mut par_dec2 = ParBz2Decoder::open(path).map_err(|e| format!("ParBz2Decoder::open (cmp): {e}"))?;

	let mut par_cmp = vec![0u8; CHUNK_SIZE];
	let mut ref_cmp = vec![0u8; CHUNK_SIZE];
	let mut offset: u64 = 0;

	loop {
		let par_n = read_exact_or_eof(&mut par_dec2, &mut par_cmp).map_err(|e| format!("parallel read (cmp): {e}"))?;
		let ref_n = read_exact_or_eof(&mut ref_dec2, &mut ref_cmp).map_err(|e| format!("reference read (cmp): {e}"))?;

		if par_n != ref_n {
			return Err(format!(
				"length mismatch at comparison: parallel={} reference={}",
				HumanBytes(offset + par_n as u64),
				HumanBytes(offset + ref_n as u64),
			));
		}
		if par_n == 0 {
			break;
		}

		if par_cmp[..par_n] != ref_cmp[..ref_n] {
			for i in 0..par_n {
				if par_cmp[i] != ref_cmp[i] {
					return Err(format!("mismatch at byte offset {}", offset + i as u64));
				}
			}
		}
		offset += par_n as u64;
		pb.set_position(offset);
	}

	pb.finish_and_clear();

	Ok((ref_bytes, parallel_ms, reference_ms))
}

/// Read exactly `buf.len()` bytes, or fewer only at EOF.
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

// ── Main ───────────────────────────────────────────────────────────

fn main()
{
	let args: Vec<String> = env::args().collect();
	if args.len() < 2 {
		eprintln!("Usage: {} <file1.bz2> [file2.bz2 ...]", args[0]);
		std::process::exit(2);
	}

	let files: Vec<PathBuf> = args[1..].iter().map(PathBuf::from).collect();
	let total = files.len();
	let failures = AtomicUsize::new(0);
	let successes = AtomicUsize::new(0);

	let mp = MultiProgress::new();
	println!("Verifying {total} file(s), up to {MAX_CONCURRENT} concurrently\n");

	let failures = &failures;
	let successes = &successes;
	let mp = &mp;

	for batch in files.chunks(MAX_CONCURRENT) {
		// Pre-create a progress bar for each file in the batch.
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
						let compressed_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

						match verify_file(path, pb) {
							Ok((decompressed_bytes, parallel_ms, reference_ms)) => {
								pb.finish_and_clear();
								mp.suspend(|| {
									println!(
										"[ OK ] {name}  compressed={cmp}  decompressed={dec}  \
										 ref={reference_ms}ms  par={parallel_ms}ms",
										cmp = HumanBytes(compressed_size),
										dec = HumanBytes(decompressed_bytes),
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
