// compress/mod.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Parallel bzip2 compression.
//!
//! The main entry point is [`ParBz2Encoder`], which implements [`Write`]
//! for streaming parallel compression of data into the bzip2 format.
//!
//! # Examples
//!
//! ```
//! use std::io::Write;
//! use parallel_bz2_redux::compress::ParBz2Encoder;
//!
//! let mut compressed = Vec::new();
//! {
//!     let mut encoder = ParBz2Encoder::new(&mut compressed, 9).unwrap();
//!     encoder.write_all(b"Hello, parallel bzip2!").unwrap();
//!     encoder.finish().unwrap();
//! }
//! assert_eq!(&compressed[..3], b"BZh");
//! ```

pub(crate) mod block;
pub(crate) mod pipeline;

use std::fmt;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rayon::ThreadPool;

use self::block::compress_block;
pub use self::block::max_block_bytes;
use self::pipeline::compress_blocks_parallel;
pub use self::pipeline::compress_parallel;
use crate::bits::BitWriter;
use crate::crc::combine_stream_crc;
use crate::error::Bz2Error;
use crate::error::Result;

/// The 48-bit end-of-stream magic: `0x177245385090`.
const EOS_MAGIC: u64 = 0x177245385090;

// ── EncoderStatsSnapshot ───────────────────────────────────────────

/// Frozen point-in-time snapshot of encoder statistics.
///
/// Obtained via [`ParBz2Encoder::stats_snapshot()`].  All fields are
/// plain values so the snapshot is cheap to copy and pass around.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EncoderStatsSnapshot
{
	/// Total uncompressed bytes received via [`Write::write()`].
	pub total_in: u64,
	/// Total compressed bytes written to the inner writer.
	pub total_out: u64,
	/// Number of blocks compressed so far.
	pub blocks_completed: u64,
	/// Number of parallel compression batches flushed.
	pub batches_completed: u64,
	/// Wall-clock time elapsed since the encoder was created.
	pub elapsed: Duration,
}

impl EncoderStatsSnapshot
{
	/// Compression ratio (`total_out / total_in`).
	///
	/// Returns `0.0` if no input has been received.
	pub fn compression_ratio(&self) -> f64
	{
		if self.total_in == 0 {
			return 0.0;
		}
		self.total_out as f64 / self.total_in as f64
	}

	/// Compression throughput in bytes per second, measured as
	/// uncompressed input bytes divided by elapsed wall-clock time.
	///
	/// Returns `0.0` if no time has elapsed.
	pub fn compression_rate(&self) -> f64
	{
		let secs = self.elapsed.as_secs_f64();
		if secs == 0.0 {
			return 0.0;
		}
		self.total_in as f64 / secs
	}
}

// ── ParBz2Encoder ──────────────────────────────────────────────────

/// Parallel bzip2 encoder implementing [`Write`].
///
/// Compresses data using multiple threads via Rayon.  Input is buffered
/// until enough full blocks have accumulated to saturate the thread pool,
/// then all pending blocks are compressed in parallel.
///
/// The number of blocks buffered before triggering parallel compression
/// is controlled by `min_blocks` (default: number of Rayon threads).
/// Set it to `1` via the builder if you prefer lower latency at the
/// cost of serial compression.
///
/// # Usage
///
/// ```
/// use std::io::Write;
/// use parallel_bz2_redux::compress::ParBz2Encoder;
///
/// let mut output = Vec::new();
/// let mut encoder = ParBz2Encoder::new(&mut output, 9).unwrap();
/// encoder.write_all(b"Hello, world!").unwrap();
/// let compressed = encoder.finish().unwrap();
/// assert_eq!(&compressed[..3], b"BZh");
/// ```
///
/// Call [`finish()`](Self::finish) to complete the stream.  If the
/// encoder is dropped without calling `finish()`, the stream is
/// finalized automatically (errors silently ignored).
pub struct ParBz2Encoder<W: Write>
{
	/// Inner writer (wrapped in `Option` so `finish()` can take it).
	inner: Option<W>,
	/// Compression level (1–9).
	level: u8,
	/// Maximum uncompressed bytes per block at this level.
	block_size: usize,
	/// Minimum number of full blocks to accumulate before triggering
	/// parallel compression.  Defaults to the Rayon thread count so
	/// that each batch saturates the pool.
	min_blocks: usize,
	/// Accumulates uncompressed input until full blocks are ready.
	buffer: Vec<u8>,
	/// Running output bitstream, flushed incrementally to inner.
	output: BitWriter,
	/// Combined stream CRC (updated as each block is compressed).
	stream_crc: u32,
	/// Whether the 4-byte stream header has been written.
	header_written: bool,
	/// Total uncompressed bytes received via `write()`.
	total_in: u64,
	/// Total compressed bytes written to the inner writer.
	total_out: u64,
	/// Whether `try_finish()` has been called.
	done: bool,
	/// Panic guard: prevents `Drop` from finalizing if a write panicked.
	panicked: bool,
	/// Custom Rayon thread pool for parallel block compression.
	pool: Option<Arc<ThreadPool>>,
	/// Number of blocks compressed so far.
	blocks_completed: u64,
	/// Number of parallel compression batches flushed.
	batches_completed: u64,
	/// Wall-clock time when the encoder was created.
	start_time: Instant,
	/// Optional progress callback, invoked after each parallel batch.
	on_progress: Option<Arc<dyn Fn(&EncoderStatsSnapshot) + Send + Sync>>,
}

impl<W: Write> ParBz2Encoder<W>
{
	/// Create a new parallel bzip2 encoder.
	///
	/// # Arguments
	///
	/// * `inner` — the writer that receives compressed output.
	/// * `level` — compression level (`1`–`9`).  Level 1 uses the smallest blocks (~100 KB uncompressed) and
	///   is fastest; level 9 uses the largest blocks (~900 KB) and achieves best compression.
	///
	/// # Errors
	///
	/// Returns [`Bz2Error::InvalidFormat`] if `level` is out of range.
	pub fn new(inner: W, level: u8) -> Result<Self>
	{
		if !(1..=9).contains(&level) {
			return Err(Bz2Error::InvalidFormat(format!(
				"compression level must be 1-9, got {level}"
			)));
		}
		let block_size = max_block_bytes(level);
		Ok(Self {
			inner: Some(inner),
			level,
			block_size,
			min_blocks: rayon::current_num_threads(),
			buffer: Vec::new(),
			output: BitWriter::new(),
			stream_crc: 0,
			header_written: false,
			total_in: 0,
			total_out: 0,
			done: false,
			panicked: false,
			pool: None,
			blocks_completed: 0,
			batches_completed: 0,
			start_time: Instant::now(),
			on_progress: None,
		})
	}

	/// Returns a builder for configuring the encoder.
	pub fn builder() -> ParBz2EncoderBuilder
	{
		ParBz2EncoderBuilder::new()
	}

	/// Returns a reference to the inner writer.
	pub fn get_ref(&self) -> &W
	{
		self.inner.as_ref().expect("encoder already finished")
	}

	/// Returns a mutable reference to the inner writer.
	///
	/// Note: writing directly to the inner writer will corrupt the
	/// bzip2 stream.
	pub fn get_mut(&mut self) -> &mut W
	{
		self.inner.as_mut().expect("encoder already finished")
	}

	/// Returns the total number of uncompressed bytes received.
	pub fn total_in(&self) -> u64
	{
		self.total_in
	}

	/// Returns the total number of compressed bytes written to the inner writer.
	pub fn total_out(&self) -> u64
	{
		self.total_out
	}

	/// Number of blocks compressed so far.
	pub fn blocks_completed(&self) -> u64
	{
		self.blocks_completed
	}

	/// Number of parallel compression batches flushed so far.
	pub fn batches_completed(&self) -> u64
	{
		self.batches_completed
	}

	/// Wall-clock time elapsed since the encoder was created.
	pub fn elapsed(&self) -> Duration
	{
		self.start_time.elapsed()
	}

	/// Compression ratio (`total_out / total_in`).
	///
	/// Returns `0.0` if no input has been received.
	pub fn compression_ratio(&self) -> f64
	{
		if self.total_in == 0 {
			return 0.0;
		}
		self.total_out as f64 / self.total_in as f64
	}

	/// Compression throughput in bytes per second, measured as
	/// uncompressed input bytes divided by elapsed wall-clock time.
	///
	/// Returns `0.0` if no time has elapsed.
	pub fn compression_rate(&self) -> f64
	{
		let secs = self.elapsed().as_secs_f64();
		if secs == 0.0 {
			return 0.0;
		}
		self.total_in as f64 / secs
	}

	/// Create a frozen point-in-time snapshot of all encoder statistics.
	pub fn stats_snapshot(&self) -> EncoderStatsSnapshot
	{
		EncoderStatsSnapshot {
			total_in: self.total_in,
			total_out: self.total_out,
			blocks_completed: self.blocks_completed,
			batches_completed: self.batches_completed,
			elapsed: self.elapsed(),
		}
	}

	/// Finalize the bzip2 stream and return the inner writer.
	///
	/// Compresses any remaining buffered data, writes the end-of-stream
	/// marker and stream CRC, and flushes the inner writer.
	///
	/// # Errors
	///
	/// Returns an I/O error if compression or writing fails.
	pub fn finish(mut self) -> std::io::Result<W>
	{
		self.try_finish()?;
		Ok(self.inner.take().expect("inner writer already taken"))
	}

	/// Attempt to finalize the bzip2 stream without consuming `self`.
	///
	/// Called automatically by [`Drop`] if `finish()` was not called.
	/// Errors from this method are silently ignored during drop.
	pub fn try_finish(&mut self) -> std::io::Result<()>
	{
		if self.done {
			return Ok(());
		}
		self.done = true;

		self.ensure_header();

		// ── Compress all remaining buffered data ───────────────────
		// First, compress any full blocks in parallel.
		let saved = self.min_blocks;
		self.min_blocks = 1;
		self.compress_pending()?;
		self.min_blocks = saved;

		// Then compress the remaining partial block (if any) serially.
		if !self.buffer.is_empty() {
			let block = compress_block(&self.buffer, self.level).map_err(|e| std::io::Error::other(e.to_string()))?;
			self.output.copy_bits_from(&block.bits, 0, block.bit_len);
			self.stream_crc = combine_stream_crc(self.stream_crc, block.block_crc);
			self.buffer.clear();
			self.blocks_completed += 1;
		}

		// ── EOS marker + stream CRC + padding ──────────────────────
		self.output.write_bits(EOS_MAGIC, 48);
		self.output.write_bits(self.stream_crc as u64, 32);
		self.output.pad_to_byte();

		// ── Flush everything to the inner writer ───────────────────
		let inner = self.inner.as_mut().expect("inner writer already taken");
		let bytes = self.output.as_bytes();
		inner.write_all(bytes)?;
		self.total_out += bytes.len() as u64;
		self.output = BitWriter::new();
		inner.flush()?;

		Ok(())
	}

	/// Write the 4-byte stream header if not yet written.
	fn ensure_header(&mut self)
	{
		if !self.header_written {
			self.output.write_bytes(&[b'B', b'Z', b'h', b'0' + self.level]);
			self.header_written = true;
		}
	}

	/// Compress pending blocks when enough have accumulated to
	/// saturate the thread pool.
	fn compress_pending(&mut self) -> std::io::Result<()>
	{
		let n_blocks = self.buffer.len() / self.block_size;
		if n_blocks < self.min_blocks {
			return Ok(());
		}

		self.ensure_header();

		let n_bytes = n_blocks * self.block_size;
		let chunks: Vec<&[u8]> = self.buffer[..n_bytes].chunks(self.block_size).collect();
		let blocks = match &self.pool {
			Some(p) => p.install(|| compress_blocks_parallel(&chunks, self.level)),
			None => compress_blocks_parallel(&chunks, self.level),
		}
		.map_err(|e| std::io::Error::other(e.to_string()))?;

		for block in &blocks {
			self.output.copy_bits_from(&block.bits, 0, block.bit_len);
			self.stream_crc = combine_stream_crc(self.stream_crc, block.block_crc);
		}

		self.blocks_completed += blocks.len() as u64;
		self.batches_completed += 1;

		self.buffer.drain(..n_bytes);

		// Flush complete bytes to inner writer.
		let inner = self.inner.as_mut().expect("inner writer already taken");
		let flushed = self.output.flush_to(inner)?;
		self.total_out += flushed as u64;

		if let Some(ref cb) = self.on_progress {
			cb(&self.stats_snapshot());
		}

		Ok(())
	}
}

impl<W: Write> Write for ParBz2Encoder<W>
{
	fn write(&mut self, buf: &[u8]) -> std::io::Result<usize>
	{
		if self.done {
			return Err(std::io::Error::other("write called after encoder finished"));
		}
		if buf.is_empty() {
			return Ok(0);
		}

		self.panicked = true;
		self.buffer.extend_from_slice(buf);
		self.total_in += buf.len() as u64;
		self.compress_pending()?;
		self.panicked = false;

		Ok(buf.len())
	}

	fn flush(&mut self) -> std::io::Result<()>
	{
		// Flush any complete bytes from the output buffer.
		let inner = self.inner.as_mut().expect("inner writer already taken");
		let flushed = self.output.flush_to(inner)?;
		self.total_out += flushed as u64;
		inner.flush()
	}
}

impl<W: Write> Drop for ParBz2Encoder<W>
{
	fn drop(&mut self)
	{
		if self.inner.is_some() && !self.panicked && !self.done {
			let _ = self.try_finish();
		}
	}
}

// ── Builder ────────────────────────────────────────────────────────

/// Builder for configuring a [`ParBz2Encoder`].
///
/// Obtain via [`ParBz2Encoder::builder()`].
///
/// ```
/// use std::io::Write;
/// use parallel_bz2_redux::compress::ParBz2EncoderBuilder;
///
/// let mut output = Vec::new();
/// let mut encoder = ParBz2EncoderBuilder::new()
///     .level(6)
///     .build(&mut output)
///     .unwrap();
/// encoder.write_all(b"test").unwrap();
/// encoder.finish().unwrap();
/// ```
pub struct ParBz2EncoderBuilder
{
	level: u8,
	min_blocks: Option<usize>,
	pool: Option<Arc<ThreadPool>>,
	on_progress: Option<Arc<dyn Fn(&EncoderStatsSnapshot) + Send + Sync>>,
}

impl fmt::Debug for ParBz2EncoderBuilder
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
	{
		f.debug_struct("ParBz2EncoderBuilder")
			.field("level", &self.level)
			.field("min_blocks", &self.min_blocks)
			.field("pool", &self.pool)
			.field("on_progress", &self.on_progress.as_ref().map(|_| ".."))
			.finish()
	}
}

impl Clone for ParBz2EncoderBuilder
{
	fn clone(&self) -> Self
	{
		Self {
			level: self.level,
			min_blocks: self.min_blocks,
			pool: self.pool.clone(),
			on_progress: self.on_progress.clone(),
		}
	}
}

impl ParBz2EncoderBuilder
{
	/// Create a new builder with default settings (level 9).
	pub fn new() -> Self
	{
		Self { level: 9, min_blocks: None, pool: None, on_progress: None }
	}

	/// Set the compression level (`1`–`9`).
	///
	/// Level 1 is fastest with least compression; level 9 achieves
	/// the best compression.  Default: `9`.
	pub fn level(mut self, level: u8) -> Self
	{
		self.level = level;
		self
	}

	/// Set the minimum number of full blocks to accumulate before
	/// triggering parallel compression.
	///
	/// Higher values give better thread utilisation at the cost of
	/// higher memory usage and latency before the first compressed
	/// bytes appear.  Set to `1` for lowest latency (effectively
	/// serial compression).
	///
	/// Default: number of threads in the Rayon pool (global or custom).
	pub fn min_blocks(mut self, n: usize) -> Self
	{
		self.min_blocks = Some(n.max(1));
		self
	}

	/// Set a custom Rayon thread pool for parallel compression.
	///
	/// By default the global Rayon pool is used.  Providing a custom pool
	/// lets you control thread count, stack size, and priority without
	/// affecting the global pool.
	pub fn thread_pool(mut self, pool: Arc<ThreadPool>) -> Self
	{
		self.pool = Some(pool);
		self
	}

	/// Register a progress callback invoked after each parallel
	/// compression batch completes.
	///
	/// The callback runs on the thread that calls [`Write::write()`],
	/// so it does not interfere with the parallel compression workers.
	///
	/// ```
	/// use std::io::Write;
	/// use parallel_bz2_redux::compress::ParBz2EncoderBuilder;
	///
	/// let mut output = Vec::new();
	/// let mut encoder = ParBz2EncoderBuilder::new()
	///     .on_progress(|snap| {
	///         eprintln!("compressed {} blocks", snap.blocks_completed);
	///     })
	///     .build(&mut output)
	///     .unwrap();
	/// encoder.write_all(b"test").unwrap();
	/// encoder.finish().unwrap();
	/// ```
	pub fn on_progress(mut self, cb: impl Fn(&EncoderStatsSnapshot) + Send + Sync + 'static) -> Self
	{
		self.on_progress = Some(Arc::new(cb));
		self
	}

	/// Build the encoder, writing compressed output to `inner`.
	///
	/// # Errors
	///
	/// Returns [`Bz2Error::InvalidFormat`] if the level is out of range.
	pub fn build<W: Write>(self, inner: W) -> Result<ParBz2Encoder<W>>
	{
		let mut enc = ParBz2Encoder::new(inner, self.level)?;
		let min_blocks = self
			.min_blocks
			.unwrap_or_else(|| self.pool.as_ref().map_or(rayon::current_num_threads(), |p| p.current_num_threads()));
		enc.min_blocks = min_blocks;
		enc.pool = self.pool;
		enc.on_progress = self.on_progress;
		Ok(enc)
	}
}

impl Default for ParBz2EncoderBuilder
{
	fn default() -> Self
	{
		Self::new()
	}
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use std::io::Read;
	use std::sync::Arc;

	use super::*;
	use crate::decompress::ParBz2Decoder;

	// ── Helpers ─────────────────────────────────────────────────────

	fn reference_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let cursor = std::io::Cursor::new(compressed);
		let mut dec = bzip2::read::BzDecoder::new(cursor);
		let mut out = Vec::new();
		dec.read_to_end(&mut out).expect("reference decompression failed");
		out
	}

	fn par_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut out = Vec::new();
		decoder.read_to_end(&mut out).unwrap();
		out
	}

	fn lcg_bytes(seed: u64, len: usize) -> Vec<u8>
	{
		let mut rng = seed;
		(0..len)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect()
	}

	// ── Basic encoding ──────────────────────────────────────────────

	#[test]
	fn test_encoder_small_text()
	{
		let original = b"Hello, parallel bzip2 encoder!";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(&output[..3], b"BZh");
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_encoder_empty()
	{
		let mut output = Vec::new();
		{
			let enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(&output[..4], b"BZh9");
		let decompressed = reference_decompress(&output);
		assert!(decompressed.is_empty());
	}

	#[test]
	fn test_encoder_single_byte()
	{
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(&[0x42]).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, &[0x42]);
	}

	#[test]
	fn test_encoder_medium_text()
	{
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(8192).collect();
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, original);
	}

	// ── Multi-block ─────────────────────────────────────────────────

	#[test]
	fn test_encoder_multi_block()
	{
		// 250KB random at level 1 → forces 3+ blocks.
		let original = lcg_bytes(0xCAFE_BABE, 250_000);
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 1).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, original);
	}

	// ── All levels ──────────────────────────────────────────────────

	#[test]
	fn test_encoder_all_levels()
	{
		let original = b"Encoder level sweep test data - enough to compress.";
		for level in 1..=9u8 {
			let mut output = Vec::new();
			{
				let mut enc = ParBz2Encoder::new(&mut output, level).unwrap();
				enc.write_all(original).unwrap();
				enc.finish().unwrap();
			}
			assert_eq!(output[3], b'0' + level, "level {level}: wrong level byte");
			let decompressed = reference_decompress(&output);
			assert_eq!(decompressed, original.as_slice(), "level {level}: mismatch");
		}
	}

	// ── Data patterns ───────────────────────────────────────────────

	#[test]
	fn test_encoder_all_zeros()
	{
		let original = vec![0u8; 8192];
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	#[test]
	fn test_encoder_binary_all_values()
	{
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	#[test]
	fn test_encoder_random_data()
	{
		let original = lcg_bytes(0xDEAD_BEEF, 4096);
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 7).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	// ── Incremental writes ──────────────────────────────────────────

	#[test]
	fn test_encoder_byte_at_a_time()
	{
		let original = b"Write one byte at a time.";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			for &b in original.iter() {
				enc.write_all(&[b]).unwrap();
			}
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	#[test]
	fn test_encoder_small_writes()
	{
		// Write in 7-byte chunks.
		let original = lcg_bytes(0xABCD, 2048);
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			for chunk in original.chunks(7) {
				enc.write_all(chunk).unwrap();
			}
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	#[test]
	fn test_encoder_large_then_small_writes()
	{
		// One large write followed by small ones — exercises compress_pending.
		let part1 = lcg_bytes(0x1111, 200_000);
		let part2 = lcg_bytes(0x2222, 50_000);
		let mut expected = part1.clone();
		expected.extend_from_slice(&part2);

		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 1).unwrap();
			enc.write_all(&part1).unwrap();
			enc.write_all(&part2).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), expected);
	}

	// ── Roundtrip with ParBz2Decoder ────────────────────────────────

	#[test]
	fn test_roundtrip_encoder_decoder_small()
	{
		let original = b"Our encoder -> our decoder roundtrip.";
		let mut compressed = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut compressed, 9).unwrap();
			enc.write_all(original).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = par_decompress(&compressed);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_roundtrip_encoder_decoder_multi_block()
	{
		let original = lcg_bytes(0x5678, 250_000);
		let mut compressed = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut compressed, 1).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = par_decompress(&compressed);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_roundtrip_all_levels()
	{
		let original = b"Full encoder-decoder roundtrip through all levels.";
		for level in 1..=9u8 {
			let mut compressed = Vec::new();
			{
				let mut enc = ParBz2Encoder::new(&mut compressed, level).unwrap();
				enc.write_all(original).unwrap();
				enc.finish().unwrap();
			}
			let decompressed = par_decompress(&compressed);
			assert_eq!(decompressed, original.as_slice(), "level {level}: mismatch");
		}
	}

	// ── Counters ────────────────────────────────────────────────────

	#[test]
	fn test_encoder_total_in()
	{
		let data = lcg_bytes(0x9999, 4096);
		let mut output = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
		enc.write_all(&data[..1000]).unwrap();
		assert_eq!(enc.total_in(), 1000);
		enc.write_all(&data[1000..]).unwrap();
		assert_eq!(enc.total_in(), 4096);
		enc.finish().unwrap();
	}

	#[test]
	fn test_encoder_total_out_after_finish()
	{
		let data = b"Some data to compress.";
		let mut output = Vec::new();
		let total_out;
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(data).unwrap();
			enc.try_finish().unwrap();
			assert!(enc.total_out() > 0);
			total_out = enc.total_out();
		}
		assert_eq!(total_out as usize, output.len());
	}

	// ── Builder ─────────────────────────────────────────────────────

	#[test]
	fn test_builder_default_level()
	{
		let mut output = Vec::new();
		let enc = ParBz2EncoderBuilder::new().build(&mut output).unwrap();
		enc.finish().unwrap();
		assert_eq!(output[3], b'9'); // default level 9
	}

	#[test]
	fn test_builder_custom_level()
	{
		let original = b"Builder with level 3.";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2EncoderBuilder::new().level(3).build(&mut output).unwrap();
			enc.write_all(original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(output[3], b'3');
		assert_eq!(reference_decompress(&output), original);
	}

	#[test]
	fn test_builder_default_trait()
	{
		let builder: ParBz2EncoderBuilder = Default::default();
		let mut output = Vec::new();
		let enc = builder.build(&mut output).unwrap();
		enc.finish().unwrap();
		assert_eq!(output[3], b'9');
	}

	// ── Error cases ─────────────────────────────────────────────────

	#[test]
	fn test_encoder_invalid_level_zero()
	{
		let mut output = Vec::new();
		assert!(matches!(
			ParBz2Encoder::new(&mut output, 0),
			Err(Bz2Error::InvalidFormat(_))
		));
	}

	#[test]
	fn test_encoder_invalid_level_too_high()
	{
		let mut output = Vec::new();
		assert!(matches!(
			ParBz2Encoder::new(&mut output, 10),
			Err(Bz2Error::InvalidFormat(_))
		));
	}

	#[test]
	fn test_encoder_write_after_finish()
	{
		let mut output = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
		enc.try_finish().unwrap();
		let result = enc.write(b"more data");
		assert!(result.is_err());
	}

	// ── Drop finalization ───────────────────────────────────────────

	#[test]
	fn test_encoder_drop_finalizes()
	{
		let original = b"Drop should finalize.";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(original).unwrap();
			// Drop without calling finish().
		}
		// Output should still be a valid bzip2 stream.
		assert_eq!(&output[..3], b"BZh");
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, original);
	}

	// ── Accessors ───────────────────────────────────────────────────

	#[test]
	fn test_encoder_get_ref()
	{
		let mut output = Vec::new();
		let enc = ParBz2Encoder::new(&mut output, 9).unwrap();
		let _r: &Vec<u8> = enc.get_ref();
	}

	#[test]
	fn test_encoder_get_mut()
	{
		let mut output = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
		let _r: &mut Vec<u8> = enc.get_mut();
	}

	// ── Flush ───────────────────────────────────────────────────────

	#[test]
	fn test_encoder_flush()
	{
		let original = b"Flush test data.";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(original).unwrap();
			enc.flush().unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	// ── Write empty buffer ──────────────────────────────────────────

	#[test]
	fn test_encoder_write_empty()
	{
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			assert_eq!(enc.write(&[]).unwrap(), 0);
			enc.write_all(b"data").unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), b"data");
	}

	// ── Cross-compatibility ─────────────────────────────────────────

	#[test]
	fn test_encoder_output_matches_reference_decompressor()
	{
		let original = lcg_bytes(0xFACE, 50_000);
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 6).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_reference_compress_our_decode()
	{
		let original = lcg_bytes(0xBEEF, 50_000);
		let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(6));
		enc.write_all(&original).unwrap();
		let compressed = enc.finish().unwrap();
		let decompressed = par_decompress(&compressed);
		assert_eq!(decompressed, original);
	}

	// ── Streaming output verification ───────────────────────────────

	#[test]
	fn test_encoder_streams_output_incrementally()
	{
		// With min_blocks(1), the encoder should write some output
		// as soon as a full block is ready — before finish() is called.
		let original = lcg_bytes(0xAAAA, 250_000);
		let mut output = Vec::new();
		let mut enc = ParBz2EncoderBuilder::new().level(1).min_blocks(1).build(&mut output).unwrap();
		enc.write_all(&original).unwrap();

		// After write_all with 250KB at level 1 (block_size bytes/block),
		// at least 2 full blocks should have been compressed and
		// streamed to the output.  Check via get_ref().
		let partial_len = enc.get_ref().len();
		assert!(
			partial_len > 0,
			"encoder should have written some output before finish()"
		);

		let compressed = enc.finish().unwrap();

		// After finish, the output should be larger.
		assert!(compressed.len() > partial_len, "finish() should write EOS + remaining");
		assert_eq!(reference_decompress(compressed), original);
	}

	// ── ParBz2Encoder::builder() ────────────────────────────────────

	#[test]
	fn test_encoder_builder_method()
	{
		// Exercise the ParBz2Encoder::builder() associated function.
		let original = b"Test via ParBz2Encoder::builder()";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::<Vec<u8>>::builder().level(5).build(&mut output).unwrap();
			enc.write_all(original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(output[3], b'5');
		assert_eq!(reference_decompress(&output), original);
	}

	// ── try_finish() idempotency ────────────────────────────────────

	#[test]
	fn test_encoder_try_finish_twice()
	{
		// Calling try_finish() twice should be a no-op the second time.
		let original = b"Double try_finish test.";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2Encoder::new(&mut output, 9).unwrap();
			enc.write_all(original).unwrap();
			enc.try_finish().unwrap();
			// Second call hits the `if self.done { return Ok(()) }` path.
			enc.try_finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	// ── Custom thread pool ──────────────────────────────────────────

	#[test]
	fn test_encoder_custom_pool_small()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let original = b"Custom thread pool encoder test!";
		let mut output = Vec::new();
		{
			let mut enc = ParBz2EncoderBuilder::new().level(9).thread_pool(pool).build(&mut output).unwrap();
			enc.write_all(original).unwrap();
			enc.finish().unwrap();
		}
		assert_eq!(reference_decompress(&output), original);
	}

	#[test]
	fn test_encoder_custom_pool_multi_block()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let original = lcg_bytes(0xB001_DA7A, 250_000);
		let mut output = Vec::new();
		{
			let mut enc = ParBz2EncoderBuilder::new().level(1).thread_pool(pool).build(&mut output).unwrap();
			enc.write_all(&original).unwrap();
			enc.finish().unwrap();
		}
		let decompressed = reference_decompress(&output);
		assert_eq!(decompressed, original);
	}

	#[test]
	fn test_encoder_custom_pool_roundtrip()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let original = b"Pool roundtrip: our encoder -> our decoder.";
		let mut compressed = Vec::new();
		{
			let mut enc = ParBz2EncoderBuilder::new()
				.level(9)
				.thread_pool(pool.clone())
				.build(&mut compressed)
				.unwrap();
			enc.write_all(original).unwrap();
			enc.finish().unwrap();
		}
		let mut decoder = crate::decompress::ParBz2Decoder::builder()
			.thread_pool(pool)
			.from_bytes(Arc::from(compressed))
			.unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	// ── EncoderStats ──────────────────────────────────────────────

	#[test]
	fn test_encoder_stats_initial()
	{
		let mut buf = Vec::new();
		let enc = ParBz2Encoder::new(&mut buf, 9).unwrap();
		assert_eq!(enc.blocks_completed(), 0);
		assert_eq!(enc.batches_completed(), 0);
		assert_eq!(enc.total_in(), 0);
		assert_eq!(enc.total_out(), 0);
		assert_eq!(enc.compression_ratio(), 0.0);
		// elapsed() should return a valid duration (just verify it doesn't panic).
		let _ = enc.elapsed();
	}

	#[test]
	fn test_encoder_stats_after_finish()
	{
		let original = b"Stats test: small data.";
		let mut compressed = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut compressed, 9).unwrap();
		enc.write_all(original).unwrap();
		// Before finish, the partial block hasn't been compressed yet.
		assert_eq!(enc.total_in(), original.len() as u64);
		enc.try_finish().unwrap();

		// After finish, at least one block should be completed.
		assert!(enc.blocks_completed() >= 1);
		assert!(enc.total_out() > 0);
		assert!(enc.compression_ratio() > 0.0);
	}

	#[test]
	fn test_encoder_stats_snapshot()
	{
		let original = b"Snapshot test data.";
		let mut compressed = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut compressed, 9).unwrap();
		enc.write_all(original).unwrap();
		enc.try_finish().unwrap();

		let snap = enc.stats_snapshot();
		assert_eq!(snap.total_in, original.len() as u64);
		assert_eq!(snap.total_out, enc.total_out());
		assert_eq!(snap.blocks_completed, enc.blocks_completed());
		assert_eq!(snap.batches_completed, enc.batches_completed());
		assert!(snap.elapsed.as_nanos() > 0);
		assert!(snap.compression_ratio() > 0.0);
	}

	#[test]
	fn test_encoder_stats_multi_block()
	{
		// Use level 1 (smallest blocks: ~100KB) with enough data to force multiple blocks.
		let block_size = max_block_bytes(1);
		let original = vec![0xABu8; block_size * 3 + 1000];
		let mut compressed = Vec::new();
		{
			let mut enc = ParBz2EncoderBuilder::new()
				.level(1)
				.min_blocks(1)
				.build(&mut compressed)
				.unwrap();
			enc.write_all(&original).unwrap();
			enc.try_finish().unwrap();

			// Should have compressed at least 3 blocks + 1 partial.
			assert!(enc.blocks_completed() >= 4, "blocks_completed = {}", enc.blocks_completed());
			assert!(enc.batches_completed() >= 1);
		}
	}

	#[test]
	fn test_encoder_stats_snapshot_compression_rate()
	{
		use std::time::Duration;
		let snap = EncoderStatsSnapshot {
			total_in: 1_000_000,
			total_out: 500_000,
			blocks_completed: 10,
			batches_completed: 2,
			elapsed: Duration::from_secs(2),
		};
		let rate = snap.compression_rate();
		assert!((rate - 500_000.0).abs() < 0.1);
		assert!((snap.compression_ratio() - 0.5).abs() < 0.001);
	}

	#[test]
	fn test_encoder_stats_snapshot_zero_elapsed()
	{
		use std::time::Duration;
		let snap = EncoderStatsSnapshot {
			total_in: 1000,
			total_out: 500,
			blocks_completed: 1,
			batches_completed: 1,
			elapsed: Duration::ZERO,
		};
		assert_eq!(snap.compression_rate(), 0.0);
	}

	#[test]
	fn test_encoder_stats_snapshot_zero_input()
	{
		use std::time::Duration;
		let snap = EncoderStatsSnapshot {
			total_in: 0,
			total_out: 0,
			blocks_completed: 0,
			batches_completed: 0,
			elapsed: Duration::from_secs(1),
		};
		assert_eq!(snap.compression_ratio(), 0.0);
		assert_eq!(snap.compression_rate(), 0.0);
	}

	#[test]
	fn test_encoder_on_progress_callback()
	{
		use std::sync::atomic::{AtomicU64, Ordering};

		let call_count = Arc::new(AtomicU64::new(0));
		let last_blocks = Arc::new(AtomicU64::new(0));
		let cc = Arc::clone(&call_count);
		let lb = Arc::clone(&last_blocks);

		// Use level 1 + min_blocks 1 to trigger callbacks per batch.
		let block_size = max_block_bytes(1);
		let original = vec![0xCDu8; block_size * 3 + 500];
		let mut compressed = Vec::new();
		{
			let mut enc = ParBz2EncoderBuilder::new()
				.level(1)
				.min_blocks(1)
				.on_progress(move |snap| {
					cc.fetch_add(1, Ordering::Relaxed);
					lb.store(snap.blocks_completed, Ordering::Relaxed);
				})
				.build(&mut compressed)
				.unwrap();
			enc.write_all(&original).unwrap();
			enc.try_finish().unwrap();
		}

		// The callback should have been called at least once.
		assert!(call_count.load(Ordering::Relaxed) >= 1);
		assert!(last_blocks.load(Ordering::Relaxed) >= 1);
	}

	#[test]
	fn test_encoder_builder_on_progress_is_cloneable()
	{
		let builder = ParBz2EncoderBuilder::new().on_progress(|_snap| {});
		let _clone = builder.clone();
	}

	#[test]
	fn test_encoder_builder_on_progress_is_debuggable()
	{
		let builder = ParBz2EncoderBuilder::new().on_progress(|_snap| {});
		let debug = format!("{:?}", builder);
		assert!(debug.contains("ParBz2EncoderBuilder"));
	}

	#[test]
	fn test_encoder_compression_rate_after_finish()
	{
		let original = b"Compression rate test data with enough content.";
		let mut compressed = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut compressed, 9).unwrap();
		enc.write_all(original).unwrap();
		enc.try_finish().unwrap();

		// After some real work, compression_rate should be positive.
		let rate = enc.compression_rate();
		assert!(rate > 0.0, "compression_rate should be > 0 after encoding, got {rate}");
	}

	#[test]
	fn test_encoder_compression_rate_no_data()
	{
		// Immediately after construction, total_in is 0 so rate should be 0.
		let mut buf = Vec::new();
		let enc = ParBz2Encoder::new(&mut buf, 9).unwrap();
		// Even though elapsed > 0, total_in == 0, so 0.0 / elapsed == 0.0.
		assert_eq!(enc.compression_rate(), 0.0);
	}
}
