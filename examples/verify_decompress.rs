// examples/verify_decompress.rs
// SPDX-License-Identifier: CC0-1.0
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

use parallel_bz2_redux::ParBz2Decoder;

/// Chunk size for streaming comparison (64 KiB).
const CHUNK_SIZE: usize = 64 * 1024;

/// Maximum number of files to verify concurrently.
/// Each file spawns its own thread; this caps the parallelism
/// to avoid overwhelming the system on huge file lists.
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

/// Result of verifying a single file.
enum VerifyResult
{
	/// Outputs matched, with total decompressed byte count.
	Ok
	{
		decompressed_bytes: u64,
		parallel_ms: u128,
		reference_ms: u128,
	},
	/// Mismatch found at the given byte offset.
	Mismatch
	{
		offset: u64
	},
	/// Length disagreement: one decoder produced more output than the other.
	LengthMismatch
	{
		parallel_bytes: u64, reference_bytes: u64
	},
	/// An error occurred.
	Error(String),
}

// ── Streaming comparison ───────────────────────────────────────────

/// Decompress `path` with both our parallel decoder and the reference
/// `bzip2` crate, comparing chunks as they are produced.
///
/// This never holds more than ~2 * CHUNK_SIZE of decompressed data at
/// a time, so it works for arbitrarily large files.
fn verify_file(path: &Path) -> VerifyResult
{
	// --- Reference decoder (bzip2 crate, streaming from file) ---
	let ref_file = match File::open(path) {
		Ok(f) => f,
		Err(e) => return VerifyResult::Error(format!("open for ref decoder: {e}")),
	};
	let mut ref_decoder = bzip2::read::BzDecoder::new(BufReader::new(ref_file));

	// Time the reference decompression into a sink to get a speed baseline.
	// We do two passes: first reference, then parallel, so each gets a
	// clean timing window without contention.
	let ref_start = Instant::now();
	let mut ref_bytes: u64 = 0;
	let mut ref_buf = vec![0u8; CHUNK_SIZE];
	loop {
		match ref_decoder.read(&mut ref_buf) {
			Ok(0) => break,
			Ok(n) => ref_bytes += n as u64,
			Err(e) => return VerifyResult::Error(format!("reference decoder: {e}")),
		}
	}
	let reference_ms = ref_start.elapsed().as_millis();
	drop(ref_decoder);

	// --- Parallel decoder ---
	let par_start = Instant::now();
	let mut par_decoder = match ParBz2Decoder::open(path) {
		Ok(d) => d,
		Err(e) => return VerifyResult::Error(format!("ParBz2Decoder::open: {e}")),
	};
	let mut par_bytes: u64 = 0;
	let mut par_buf = vec![0u8; CHUNK_SIZE];
	loop {
		match par_decoder.read(&mut par_buf) {
			Ok(0) => break,
			Ok(n) => par_bytes += n as u64,
			Err(e) => return VerifyResult::Error(format!("parallel decoder: {e}")),
		}
	}
	let parallel_ms = par_start.elapsed().as_millis();
	drop(par_decoder);

	// Quick length check.
	if par_bytes != ref_bytes {
		return VerifyResult::LengthMismatch { parallel_bytes: par_bytes, reference_bytes: ref_bytes };
	}

	// --- Streaming byte-by-byte comparison (third pass) ---
	// Re-open both decoders for the comparison pass.
	let ref_file2 = match File::open(path) {
		Ok(f) => f,
		Err(e) => return VerifyResult::Error(format!("open for comparison: {e}")),
	};
	let mut ref_dec2 = bzip2::read::BzDecoder::new(BufReader::new(ref_file2));
	let mut par_dec2 = match ParBz2Decoder::open(path) {
		Ok(d) => d,
		Err(e) => return VerifyResult::Error(format!("ParBz2Decoder::open (cmp): {e}")),
	};

	let mut par_cmp = vec![0u8; CHUNK_SIZE];
	let mut ref_cmp = vec![0u8; CHUNK_SIZE];
	let mut offset: u64 = 0;

	loop {
		let par_n = match read_exact_or_eof(&mut par_dec2, &mut par_cmp) {
			Ok(n) => n,
			Err(e) => return VerifyResult::Error(format!("parallel read (cmp): {e}")),
		};
		let ref_n = match read_exact_or_eof(&mut ref_dec2, &mut ref_cmp) {
			Ok(n) => n,
			Err(e) => return VerifyResult::Error(format!("reference read (cmp): {e}")),
		};

		if par_n != ref_n {
			return VerifyResult::LengthMismatch {
				parallel_bytes: offset + par_n as u64,
				reference_bytes: offset + ref_n as u64,
			};
		}
		if par_n == 0 {
			break;
		}

		// Compare the chunk.
		if par_cmp[..par_n] != ref_cmp[..ref_n] {
			// Find the exact mismatch offset.
			for i in 0..par_n {
				if par_cmp[i] != ref_cmp[i] {
					return VerifyResult::Mismatch { offset: offset + i as u64 };
				}
			}
		}
		offset += par_n as u64;
	}

	VerifyResult::Ok { decompressed_bytes: offset, parallel_ms, reference_ms }
}

/// Read exactly `buf.len()` bytes, or fewer only at EOF.
/// Returns the number of bytes actually read.
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

	println!("Verifying {total} file(s), up to {MAX_CONCURRENT} concurrently\n");

	// Process files in batches of MAX_CONCURRENT using scoped threads.
	// Take references so the `move` closures capture Copy-able refs,
	// not the owned AtomicUsize values.
	let failures = &failures;
	let successes = &successes;

	for batch in files.chunks(MAX_CONCURRENT) {
		std::thread::scope(|s| {
			let handles: Vec<_> = batch
				.iter()
				.map(|path| {
					s.spawn(move || {
						let name = path.display();
						let file_start = Instant::now();

						let compressed_size = match std::fs::metadata(path) {
							Ok(m) => m.len(),
							Err(e) => {
								eprintln!("[FAIL] {name}: cannot stat: {e}");
								failures.fetch_add(1, Ordering::Relaxed);
								return;
							}
						};

						print_status(&name, compressed_size, "verifying...");

						match verify_file(path) {
							VerifyResult::Ok { decompressed_bytes, parallel_ms, reference_ms } => {
								let wall = file_start.elapsed().as_millis();
								println!(
									"[ OK ] {name}  compressed={cmp}  decompressed={dec}  \
									 ref={reference_ms}ms  par={parallel_ms}ms  total={wall}ms",
									cmp = HumanBytes(compressed_size),
									dec = HumanBytes(decompressed_bytes),
								);
								successes.fetch_add(1, Ordering::Relaxed);
							}
							VerifyResult::Mismatch { offset } => {
								eprintln!("[FAIL] {name}: mismatch at byte offset {offset}");
								failures.fetch_add(1, Ordering::Relaxed);
							}
							VerifyResult::LengthMismatch { parallel_bytes, reference_bytes } => {
								eprintln!(
									"[FAIL] {name}: length mismatch: parallel={par} reference={rf}",
									par = HumanBytes(parallel_bytes),
									rf = HumanBytes(reference_bytes),
								);
								failures.fetch_add(1, Ordering::Relaxed);
							}
							VerifyResult::Error(msg) => {
								eprintln!("[FAIL] {name}: {msg}");
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

fn print_status(name: &impl fmt::Display, compressed_size: u64, status: &str)
{
	println!("[    ] {name}  ({cmp})  {status}", cmp = HumanBytes(compressed_size));
}
