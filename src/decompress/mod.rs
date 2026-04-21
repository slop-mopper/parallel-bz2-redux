// decompress/mod.rs
// SPDX-License-Identifier: Unlicense
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Parallel bzip2 decompression.
//!
//! The main entry point is [`ParBz2Decoder`], which implements [`Read`]
//! for streaming parallel decompression of bzip2 data.
//!
//! # Examples
//!
//! ```no_run
//! use std::io::Read;
//! use parallel_bz2_redux::decompress::ParBz2Decoder;
//!
//! // From a file:
//! let mut decoder = ParBz2Decoder::open("file.bz2").unwrap();
//! let mut output = Vec::new();
//! decoder.read_to_end(&mut output).unwrap();
//!
//! // From in-memory data:
//! # let compressed_data: Vec<u8> = Vec::new();
//! let data: std::sync::Arc<[u8]> = std::sync::Arc::from(compressed_data);
//! let mut decoder = ParBz2Decoder::from_bytes(data).unwrap();
//! ```

pub(crate) mod block;
pub(crate) mod pipeline;

use std::collections::VecDeque;
use std::fmt;
use std::io::Read;
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use memmap2::Mmap;
use rayon::ThreadPool;

use self::pipeline::DecompressPipeline;
pub use self::pipeline::PipelineConfig;
use self::pipeline::pair_candidates;
use crate::bits::read_u32_at_bit;
use crate::crc::combine_stream_crc;
use crate::error::Bz2Error;
use crate::error::Result;
use crate::scanner::Candidate;
use crate::scanner::MarkerType;
use crate::scanner::Scanner;

// ── DataSource ─────────────────────────────────────────────────────

/// Shared, reference-counted handle to the compressed data.
///
/// Either owned in-memory data or a memory-mapped file.  Cheap to
/// clone (just an `Arc` increment) and dereferences to `&[u8]`.
#[derive(Clone, Debug)]
pub(crate) enum DataSource
{
	/// Heap-allocated data (from [`std::fs::read`] or user-supplied).
	Owned(Arc<[u8]>),
	/// Memory-mapped file.
	Mapped(Arc<Mmap>),
}

impl Deref for DataSource
{
	type Target = [u8];

	fn deref(&self) -> &[u8]
	{
		match self {
			DataSource::Owned(d) => d,
			DataSource::Mapped(m) => m,
		}
	}
}

/// Open a file as a [`DataSource`], preferring mmap.
///
/// Falls back to [`std::fs::read`] if the file cannot be memory-mapped
/// (e.g. it is a pipe, device, or on a filesystem that does not support mmap).
fn open_data_source(path: &Path) -> Result<DataSource>
{
	let file = std::fs::File::open(path).map_err(Bz2Error::Io)?;

	// Try mmap first.
	match unsafe { Mmap::map(&file) } {
		Ok(mmap) => Ok(DataSource::Mapped(Arc::new(mmap))),
		Err(_) => {
			// Mmap failed — fall back to reading the entire file.
			let data = std::fs::read(path).map_err(Bz2Error::Io)?;
			Ok(DataSource::Owned(Arc::from(data)))
		}
	}
}

// ── Header parsing ─────────────────────────────────────────────────

/// Parse the 4-byte bzip2 stream header and return the compression level (1–9).
fn parse_header(data: &[u8]) -> Result<u8>
{
	if data.len() < 4 {
		return Err(Bz2Error::InvalidFormat("data too short for bzip2 header".into()));
	}
	if &data[0..3] != b"BZh" {
		return Err(Bz2Error::InvalidFormat("missing BZh header magic".into()));
	}
	let level_byte = data[3];
	if !(b'1'..=b'9').contains(&level_byte) {
		return Err(Bz2Error::InvalidFormat(format!(
			"invalid block size level byte: {:#04x}",
			level_byte
		)));
	}
	Ok(level_byte - b'0')
}

// ── Multi-stream partitioning ──────────────────────────────────────

/// Information about a single bzip2 stream within potentially concatenated
/// multi-stream data.
struct StreamInfo
{
	/// Compression level (1–9) from this stream's header.
	level: u8,
	/// Scanner candidates belonging to this stream (blocks + EOS).
	candidates: Vec<Candidate>,
	/// Stream CRC stored in the EOS marker.
	stored_stream_crc: u32,
}

/// Partition sorted scanner candidates into per-stream groups.
///
/// Walks candidates in bit-offset order, splitting at each EOS marker.
/// For each stream, parses the `BZh` header to get the compression level
/// and reads the stored stream CRC from the EOS marker.
fn find_streams(data: &[u8], candidates: &[Candidate]) -> Result<Vec<StreamInfo>>
{
	let mut streams = Vec::new();
	let mut stream_candidates: Vec<Candidate> = Vec::new();
	let mut header_byte_offset: usize = 0;

	for &c in candidates {
		stream_candidates.push(c);

		if c.marker_type == MarkerType::Eos {
			let level = parse_header(data.get(header_byte_offset..).unwrap_or(&[]))?;
			let eos_bit = c.bit_offset;
			let stored_stream_crc = read_u32_at_bit(data, eos_bit + 48);

			streams.push(StreamInfo {
				level,
				candidates: std::mem::take(&mut stream_candidates),
				stored_stream_crc,
			});

			// Next stream header starts at the next byte boundary after
			// the EOS marker (48-bit magic + 32-bit CRC + 0–7 padding).
			header_byte_offset = (eos_bit + 80).div_ceil(8) as usize;
		}
	}

	if !stream_candidates.is_empty() {
		return Err(Bz2Error::InvalidFormat("trailing blocks without EOS marker".into()));
	}

	if streams.is_empty() {
		return Err(Bz2Error::InvalidFormat("no bzip2 streams found".into()));
	}

	Ok(streams)
}

// ── DecoderStats ───────────────────────────────────────────────────

/// Live statistics for a [`ParBz2Decoder`].
///
/// An `Arc<DecoderStats>` is created when the decoder is constructed and
/// shared with the pipeline's validator thread.  Immutable totals are set
/// once at construction; mutable counters use [`AtomicU64`] and are
/// updated with [`Relaxed`](Ordering::Relaxed) ordering — perfectly fine
/// for progress reporting but not suitable as a synchronisation barrier.
///
/// Use [`snapshot()`](Self::snapshot) for a consistent point-in-time view
/// of all counters.
pub struct DecoderStats
{
	/// Total bytes of compressed input data.
	compressed_bytes_total: u64,
	/// Total number of bzip2 blocks across all streams.
	blocks_total: u64,
	/// Total number of bzip2 streams.
	streams_total: u64,
	/// Cumulative decompressed output bytes emitted so far.
	decompressed_bytes: AtomicU64,
	/// Number of blocks successfully decompressed and consumed.
	blocks_completed: AtomicU64,
	/// Number of streams fully consumed.
	streams_completed: AtomicU64,
	/// Bit offset of the last validated block's end (updated by the
	/// validator thread).
	compressed_bits_consumed: AtomicU64,
	/// Wall-clock time when the decoder was created.
	start_time: Instant,
}

impl DecoderStats
{
	/// Create a new `DecoderStats` with the given totals and zeroed
	/// counters.  Intended for internal construction.
	pub(crate) fn new(
		compressed_bytes_total: u64,
		blocks_total: u64,
		streams_total: u64,
	) -> Self
	{
		Self {
			compressed_bytes_total,
			blocks_total,
			streams_total,
			decompressed_bytes: AtomicU64::new(0),
			blocks_completed: AtomicU64::new(0),
			streams_completed: AtomicU64::new(0),
			compressed_bits_consumed: AtomicU64::new(0),
			start_time: Instant::now(),
		}
	}

	/// Total bytes of compressed input data.
	pub fn compressed_bytes_total(&self) -> u64
	{
		self.compressed_bytes_total
	}

	/// Total number of bzip2 blocks across all streams.
	pub fn blocks_total(&self) -> u64
	{
		self.blocks_total
	}

	/// Total number of bzip2 streams.
	pub fn streams_total(&self) -> u64
	{
		self.streams_total
	}

	/// Cumulative decompressed output bytes emitted so far.
	pub fn decompressed_bytes(&self) -> u64
	{
		self.decompressed_bytes.load(Ordering::Relaxed)
	}

	/// Number of blocks successfully decompressed and consumed.
	pub fn blocks_completed(&self) -> u64
	{
		self.blocks_completed.load(Ordering::Relaxed)
	}

	/// Number of streams fully consumed.
	pub fn streams_completed(&self) -> u64
	{
		self.streams_completed.load(Ordering::Relaxed)
	}

	/// Bit offset just past the last validated block emitted by the
	/// pipeline.  Useful for estimating progress through the compressed
	/// data.
	pub fn compressed_bits_consumed(&self) -> u64
	{
		self.compressed_bits_consumed.load(Ordering::Relaxed)
	}

	/// Wall-clock time elapsed since the decoder was created.
	pub fn elapsed(&self) -> Duration
	{
		self.start_time.elapsed()
	}

	/// Decompression throughput in bytes per second, measured as
	/// decompressed output bytes divided by elapsed wall-clock time.
	///
	/// Returns `0.0` if no time has elapsed.
	pub fn decompression_rate(&self) -> f64
	{
		let elapsed = self.elapsed().as_secs_f64();
		if elapsed == 0.0 {
			return 0.0;
		}
		self.decompressed_bytes() as f64 / elapsed
	}

	/// Fraction of blocks completed (`0.0..=1.0`).
	///
	/// Returns `0.0` if `blocks_total` is zero.
	pub fn progress(&self) -> f64
	{
		if self.blocks_total == 0 {
			return 0.0;
		}
		self.blocks_completed() as f64 / self.blocks_total as f64
	}

	/// Create a frozen point-in-time snapshot of all counters.
	pub fn snapshot(&self) -> DecoderStatsSnapshot
	{
		DecoderStatsSnapshot {
			compressed_bytes_total: self.compressed_bytes_total,
			blocks_total: self.blocks_total,
			streams_total: self.streams_total,
			decompressed_bytes: self.decompressed_bytes(),
			blocks_completed: self.blocks_completed(),
			streams_completed: self.streams_completed(),
			compressed_bits_consumed: self.compressed_bits_consumed(),
			elapsed: self.elapsed(),
		}
	}
}

impl fmt::Debug for DecoderStats
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
	{
		f.debug_struct("DecoderStats")
			.field("compressed_bytes_total", &self.compressed_bytes_total)
			.field("blocks_total", &self.blocks_total)
			.field("streams_total", &self.streams_total)
			.field("decompressed_bytes", &self.decompressed_bytes())
			.field("blocks_completed", &self.blocks_completed())
			.field("streams_completed", &self.streams_completed())
			.field("compressed_bits_consumed", &self.compressed_bits_consumed())
			.field("elapsed", &self.elapsed())
			.finish()
	}
}

/// Frozen point-in-time snapshot of decoder statistics.
///
/// Obtained via [`DecoderStats::snapshot()`].  All fields are plain
/// values — no atomics — so the snapshot is cheap to copy and pass
/// around.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DecoderStatsSnapshot
{
	/// Total bytes of compressed input data.
	pub compressed_bytes_total: u64,
	/// Total number of bzip2 blocks across all streams.
	pub blocks_total: u64,
	/// Total number of bzip2 streams.
	pub streams_total: u64,
	/// Cumulative decompressed output bytes emitted so far.
	pub decompressed_bytes: u64,
	/// Number of blocks successfully decompressed and consumed.
	pub blocks_completed: u64,
	/// Number of streams fully consumed.
	pub streams_completed: u64,
	/// Bit offset just past the last validated block.
	pub compressed_bits_consumed: u64,
	/// Wall-clock time elapsed since the decoder was created.
	pub elapsed: Duration,
}

impl DecoderStatsSnapshot
{
	/// Decompression throughput in bytes per second.
	///
	/// Returns `0.0` if no time has elapsed.
	pub fn decompression_rate(&self) -> f64
	{
		let secs = self.elapsed.as_secs_f64();
		if secs == 0.0 {
			return 0.0;
		}
		self.decompressed_bytes as f64 / secs
	}

	/// Fraction of blocks completed (`0.0..=1.0`).
	pub fn progress(&self) -> f64
	{
		if self.blocks_total == 0 {
			return 0.0;
		}
		self.blocks_completed as f64 / self.blocks_total as f64
	}
}

// ── ParBz2Decoder ──────────────────────────────────────────────────

/// Parallel bzip2 decoder implementing [`Read`].
///
/// Decompresses bzip2 data using multiple threads.  Blocks are decompressed
/// in parallel via Rayon and streamed in order through bounded channels —
/// only a small window of blocks is ever in memory.
///
/// Supports concatenated multi-stream bzip2 files: each stream is
/// decompressed through its own pipeline, sequentially.
///
/// # Stream CRC verification
///
/// By default, each stream's CRC is verified when that stream ends.
/// If a CRC does not match, the next `read()` call returns an I/O error
/// with [`ErrorKind::InvalidData`](std::io::ErrorKind::InvalidData).
/// Disable this via [`ParBz2DecoderBuilder::verify_stream_crc`].
pub struct ParBz2Decoder
{
	/// Full compressed data (shared with pipelines).
	data: DataSource,
	/// Pipeline configuration (reused for each stream's pipeline).
	config: PipelineConfig,
	/// Custom Rayon thread pool (reused for each stream's pipeline).
	pool: Option<Arc<ThreadPool>>,
	/// Remaining streams to decompress (front = next).
	streams: VecDeque<StreamInfo>,
	/// Active pipeline for the current stream, or `None` between streams.
	pipeline: Option<DecompressPipeline>,
	/// Current block's decompressed data.
	current_block: Vec<u8>,
	/// Read cursor within `current_block`.
	cursor: usize,
	/// Running combined stream CRC for the current stream.
	stream_crc: u32,
	/// Stream CRC stored in the current stream's EOS marker.
	stored_stream_crc: u32,
	/// Whether to verify each stream's CRC when it ends.
	verify_stream_crc: bool,
	/// `true` once all streams are exhausted and all data has been read.
	done: bool,
	/// Live statistics shared with the pipeline.
	stats: Arc<DecoderStats>,
	/// Optional progress callback, invoked on the reader thread after
	/// each block is consumed.
	on_progress: Option<Arc<dyn Fn(&DecoderStats) + Send + Sync>>,
}

impl ParBz2Decoder
{
	/// Open a bzip2 file for parallel decompression.
	///
	/// Attempts to memory-map the file for lower RSS.  Falls back to
	/// reading the entire file into memory if mmap is not available
	/// (e.g. the path is a pipe or special file).
	pub fn open<P: AsRef<Path>>(path: P) -> Result<Self>
	{
		let data = open_data_source(path.as_ref())?;
		Self::build(data, PipelineConfig::default(), true, None, None)
	}

	/// Create a decoder from in-memory compressed data.
	pub fn from_bytes(data: Arc<[u8]>) -> Result<Self>
	{
		Self::build(DataSource::Owned(data), PipelineConfig::default(), true, None, None)
	}

	/// Returns a builder for configuring decompression options.
	pub fn builder() -> ParBz2DecoderBuilder
	{
		ParBz2DecoderBuilder::new()
	}

	/// Internal constructor shared by all entry points.
	fn build(
		data: DataSource,
		config: PipelineConfig,
		verify_stream_crc: bool,
		pool: Option<Arc<ThreadPool>>,
		on_progress: Option<Arc<dyn Fn(&DecoderStats) + Send + Sync>>,
	) -> Result<Self>
	{
		let scanner = Scanner::new();
		let candidates = match &pool {
			Some(p) => p.install(|| scanner.scan_parallel(&data)),
			None => scanner.scan_parallel(&data),
		};
		let all_streams = find_streams(&data, &candidates)?;
		let mut streams = VecDeque::from(all_streams);

		// Count total blocks across all streams for stats.
		let mut blocks_total: u64 = 0;
		let streams_total = streams.len() as u64;
		// We only need the block count; pair_candidates is cheap (just iterates candidates).
		for stream in streams.iter() {
			let (ranges, _) = pair_candidates(&stream.candidates)?;
			blocks_total += ranges.len() as u64;
		}

		// Start the first stream's pipeline immediately.
		let first = streams.pop_front().expect("find_streams guarantees at least one stream");
		let stored_stream_crc = first.stored_stream_crc;

		let stats = Arc::new(DecoderStats::new(
			data.len() as u64,
			blocks_total,
			streams_total,
		));

		let pipeline = DecompressPipeline::start(
			data.clone(),
			first.candidates,
			first.level,
			config.clone(),
			pool.clone(),
			Arc::clone(&stats),
		)?;

		Ok(Self {
			data,
			config,
			pool,
			streams,
			pipeline: Some(pipeline),
			current_block: Vec::new(),
			cursor: 0,
			stream_crc: 0,
			stored_stream_crc,
			verify_stream_crc,
			done: false,
			stats,
			on_progress,
		})
	}

	/// Returns the combined stream CRC computed from blocks consumed so far.
	pub fn stream_crc(&self) -> u32
	{
		self.stream_crc
	}

	/// Returns the stream CRC stored in the bzip2 stream's EOS marker.
	pub fn stored_stream_crc(&self) -> u32
	{
		self.stored_stream_crc
	}

	/// Returns a reference to the shared statistics.
	///
	/// The returned `Arc` can be cloned and handed to another thread
	/// (e.g. a progress-bar thread) that polls the counters while
	/// decompression is in progress.
	pub fn stats(&self) -> &Arc<DecoderStats>
	{
		&self.stats
	}

	/// Pull the next block from the pipeline into `current_block`.
	///
	/// Handles stream transitions: when the current stream's pipeline is
	/// exhausted, verifies its CRC and starts the next stream's pipeline.
	///
	/// Returns `Ok(true)` if a block was received, `Ok(false)` if all
	/// streams are exhausted, or `Err` on decompression/CRC failure.
	fn pull_next_block(&mut self) -> std::io::Result<bool>
	{
		loop {
			if let Some(ref mut pipeline) = self.pipeline {
				match pipeline.recv() {
					Some(Ok(block)) => {
						self.stream_crc = combine_stream_crc(self.stream_crc, block.crc);
						self.stats
							.decompressed_bytes
							.fetch_add(block.data.len() as u64, Ordering::Relaxed);
						self.stats.blocks_completed.fetch_add(1, Ordering::Relaxed);
						self.current_block = block.data;
						self.cursor = 0;
						if let Some(ref cb) = self.on_progress {
							cb(&self.stats);
						}
						return Ok(true);
					}
					Some(Err(e)) => {
						self.done = true;
						return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
					}
					None => {
						// Current stream exhausted.  Verify its CRC.
						if self.verify_stream_crc && self.stream_crc != self.stored_stream_crc {
							self.done = true;
							return Err(std::io::Error::new(
								std::io::ErrorKind::InvalidData,
								format!(
									"stream CRC mismatch: stored={:#010x} computed={:#010x}",
									self.stored_stream_crc, self.stream_crc
								),
							));
						}
						self.stats.streams_completed.fetch_add(1, Ordering::Relaxed);
						self.pipeline = None;
					}
				}
			}

			// Try to start the next stream's pipeline.
			if let Some(stream) = self.streams.pop_front() {
				self.stream_crc = 0;
				self.stored_stream_crc = stream.stored_stream_crc;
				let pipeline = DecompressPipeline::start(
					self.data.clone(),
					stream.candidates,
					stream.level,
					self.config.clone(),
					self.pool.clone(),
					Arc::clone(&self.stats),
				)
				.map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
				self.pipeline = Some(pipeline);
				continue;
			}

			// No more streams.
			return Ok(false);
		}
	}
}

impl Read for ParBz2Decoder
{
	fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize>
	{
		if self.done || buf.is_empty() {
			return Ok(0);
		}

		loop {
			// Drain the current block.
			let remaining = self.current_block.len() - self.cursor;
			if remaining > 0 {
				let n = remaining.min(buf.len());
				buf[..n].copy_from_slice(&self.current_block[self.cursor..self.cursor + n]);
				self.cursor += n;

				// Free block memory once fully consumed.
				if self.cursor == self.current_block.len() {
					self.current_block = Vec::new();
					self.cursor = 0;
				}

				return Ok(n);
			}

			// Current block exhausted — pull the next one (may start next stream).
			if !self.pull_next_block()? {
				self.done = true;
				return Ok(0);
			}
		}
	}
}

// ── Builder ────────────────────────────────────────────────────────

/// Builder for configuring a [`ParBz2Decoder`].
///
/// Obtain via [`ParBz2Decoder::builder()`].
pub struct ParBz2DecoderBuilder
{
	config: Option<PipelineConfig>,
	pool: Option<Arc<ThreadPool>>,
	verify_stream_crc: bool,
	on_progress: Option<Arc<dyn Fn(&DecoderStats) + Send + Sync>>,
}

impl fmt::Debug for ParBz2DecoderBuilder
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result
	{
		f.debug_struct("ParBz2DecoderBuilder")
			.field("config", &self.config)
			.field("pool", &self.pool)
			.field("verify_stream_crc", &self.verify_stream_crc)
			.field("on_progress", &self.on_progress.as_ref().map(|_| ".."))
			.finish()
	}
}

impl Clone for ParBz2DecoderBuilder
{
	fn clone(&self) -> Self
	{
		Self {
			config: self.config.clone(),
			pool: self.pool.clone(),
			verify_stream_crc: self.verify_stream_crc,
			on_progress: self.on_progress.clone(),
		}
	}
}

impl ParBz2DecoderBuilder
{
	fn new() -> Self
	{
		Self { config: None, pool: None, verify_stream_crc: true, on_progress: None }
	}

	/// Set the pipeline channel capacities.
	///
	/// See [`PipelineConfig`] for details on how channel capacities affect
	/// memory usage and throughput.
	pub fn pipeline_config(mut self, config: PipelineConfig) -> Self
	{
		self.config = Some(config);
		self
	}

	/// Set a custom Rayon thread pool for parallel decompression.
	///
	/// By default the global Rayon pool is used.  Providing a custom pool
	/// lets you control thread count, stack size, and priority without
	/// affecting the global pool.
	///
	/// If no [`pipeline_config`](Self::pipeline_config) has been set, channel
	/// capacities are automatically sized for the pool's thread count.
	pub fn thread_pool(mut self, pool: Arc<ThreadPool>) -> Self
	{
		self.pool = Some(pool);
		self
	}

	/// Whether to verify the stream CRC after all blocks are consumed.
	///
	/// When `true` (the default), the final `read()` call that would
	/// otherwise return `Ok(0)` returns an error if the stream CRC does
	/// not match.
	pub fn verify_stream_crc(mut self, verify: bool) -> Self
	{
		self.verify_stream_crc = verify;
		self
	}

	/// Register a progress callback invoked after each block is consumed.
	///
	/// The callback runs on the thread that calls [`Read::read()`] — it
	/// never runs on a Rayon worker thread, so it cannot stall the
	/// parallel pipeline.
	///
	/// ```no_run
	/// use parallel_bz2_redux::ParBz2Decoder;
	///
	/// let decoder = ParBz2Decoder::builder()
	///     .on_progress(|stats| {
	///         eprintln!("progress: {:.1}%", stats.progress() * 100.0);
	///     })
	///     .open("file.bz2")
	///     .unwrap();
	/// ```
	pub fn on_progress(mut self, cb: impl Fn(&DecoderStats) + Send + Sync + 'static) -> Self
	{
		self.on_progress = Some(Arc::new(cb));
		self
	}

	/// Resolve the pipeline config: explicit if set, otherwise sized for
	/// the custom pool (if any) or the global pool.
	fn resolve_config(&self) -> PipelineConfig
	{
		if let Some(ref config) = self.config {
			return config.clone();
		}
		match &self.pool {
			Some(p) => PipelineConfig::for_pool(p),
			None => PipelineConfig::default(),
		}
	}

	/// Open a bzip2 file for parallel decompression.
	pub fn open<P: AsRef<Path>>(self, path: P) -> Result<ParBz2Decoder>
	{
		let data = open_data_source(path.as_ref())?;
		let config = self.resolve_config();
		ParBz2Decoder::build(data, config, self.verify_stream_crc, self.pool, self.on_progress)
	}

	/// Build a decoder from in-memory compressed data.
	pub fn from_bytes(self, data: Arc<[u8]>) -> Result<ParBz2Decoder>
	{
		let config = self.resolve_config();
		ParBz2Decoder::build(DataSource::Owned(data), config, self.verify_stream_crc, self.pool, self.on_progress)
	}
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use std::io::Write;

	use super::*;

	// ── Helpers ─────────────────────────────────────────────────────

	fn compress(data: &[u8], level: u32) -> Vec<u8>
	{
		let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
		enc.write_all(data).unwrap();
		enc.finish().unwrap()
	}

	fn reference_decompress(compressed: &[u8]) -> Vec<u8>
	{
		let mut dec = bzip2::read::BzDecoder::new(std::io::Cursor::new(compressed));
		let mut out = Vec::new();
		dec.read_to_end(&mut out).unwrap();
		out
	}

	// ── Header parsing ─────────────────────────────────────────────

	#[test]
	fn test_parse_header_valid()
	{
		for level in 1..=9u8 {
			let data = [b'B', b'Z', b'h', b'0' + level];
			assert_eq!(parse_header(&data).unwrap(), level);
		}
	}

	#[test]
	fn test_parse_header_too_short()
	{
		assert!(matches!(parse_header(b"BZ"), Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_parse_header_bad_magic()
	{
		assert!(matches!(parse_header(b"GZh9"), Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_parse_header_bad_level()
	{
		assert!(matches!(parse_header(b"BZh0"), Err(Bz2Error::InvalidFormat(_))));
		assert!(matches!(parse_header(b"BZhA"), Err(Bz2Error::InvalidFormat(_))));
	}

	// ── ParBz2Decoder: basic ───────────────────────────────────────

	#[test]
	fn test_decoder_small_text()
	{
		let original = b"Hello, parallel bzip2 world!";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_empty()
	{
		let compressed = compress(b"", 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert!(output.is_empty());
	}

	#[test]
	fn test_decoder_single_byte()
	{
		let original = b"X";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_medium_text()
	{
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(4096).collect();
		let compressed = compress(&original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: multi-block ─────────────────────────────────

	#[test]
	fn test_decoder_multi_block()
	{
		// ~250KB random at level 1 to force multiple blocks.
		let mut rng: u64 = 0x1234_5678_ABCD_EF01;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: all levels ──────────────────────────────────

	#[test]
	fn test_decoder_all_levels()
	{
		let original = b"Decoder level sweep test data - enough bytes to compress reliably.";
		for level in 1..=9u32 {
			let compressed = compress(original, level);
			let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
			let mut output = Vec::new();
			decoder.read_to_end(&mut output).unwrap();
			assert_eq!(output, original.as_slice(), "mismatch at level {level}");
		}
	}

	// ── ParBz2Decoder: binary data ─────────────────────────────────

	#[test]
	fn test_decoder_binary_all_values()
	{
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		let compressed = compress(&original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: small reads ─────────────────────────────────

	#[test]
	fn test_decoder_byte_at_a_time()
	{
		let original = b"Read me one byte at a time!";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		let mut byte = [0u8; 1];
		loop {
			match decoder.read(&mut byte) {
				Ok(0) => break,
				Ok(1) => output.push(byte[0]),
				Ok(n) => panic!("read returned {n} for 1-byte buffer"),
				Err(e) => panic!("unexpected error: {e}"),
			}
		}

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: stream CRC ──────────────────────────────────

	#[test]
	fn test_decoder_stream_crc_valid()
	{
		let original = b"CRC verification test data.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		// Stream CRC should match stored CRC.
		assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
	}

	#[test]
	fn test_decoder_stream_crc_matches_reference()
	{
		// For a multi-block file, verify our stream CRC matches what
		// libbzip2 produced.
		let mut rng: u64 = 0xCAFE_BABE;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
		// The stored stream CRC was produced by libbzip2 — if our
		// computed stream CRC matches, both are correct.
		assert_eq!(decoder.stream_crc(), decoder.stored_stream_crc());
	}

	#[test]
	fn test_decoder_stream_crc_disabled()
	{
		let original = b"CRC disabled test.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::builder()
			.verify_stream_crc(false)
			.from_bytes(Arc::from(compressed))
			.unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: builder ─────────────────────────────────────

	#[test]
	fn test_builder_custom_config()
	{
		let original = b"Builder config test.";
		let compressed = compress(original, 5);

		let config = PipelineConfig { result_channel_cap: 2, output_channel_cap: 1 };
		let mut decoder = ParBz2Decoder::builder()
			.pipeline_config(config)
			.verify_stream_crc(true)
			.from_bytes(Arc::from(compressed))
			.unwrap();

		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	// ── ParBz2Decoder: reference compatibility ─────────────────────

	#[test]
	fn test_decoder_matches_reference_decompressor()
	{
		let line = "Reference decompressor compatibility test.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(8192).collect();
		let compressed = compress(&original, 6);

		let reference = reference_decompress(&compressed);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, reference);
	}

	// ── ParBz2Decoder: repeated reads after EOF ────────────────────

	#[test]
	fn test_decoder_read_after_eof()
	{
		let original = b"EOF test.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);

		// Further reads should return 0.
		let mut buf = [0u8; 64];
		assert_eq!(decoder.read(&mut buf).unwrap(), 0);
		assert_eq!(decoder.read(&mut buf).unwrap(), 0);
	}

	// ── ParBz2Decoder: error cases ─────────────────────────────────

	#[test]
	fn test_decoder_invalid_header_not_bzip2()
	{
		let data: Arc<[u8]> = Arc::from(b"not bzip2 data at all".to_vec());
		assert!(ParBz2Decoder::from_bytes(data).is_err());
	}

	#[test]
	fn test_decoder_invalid_header_bad_level()
	{
		let data: Arc<[u8]> = Arc::from(b"BZh0xxxx".to_vec());
		assert!(matches!(
			ParBz2Decoder::from_bytes(data),
			Err(Bz2Error::InvalidFormat(_))
		));
	}

	#[test]
	fn test_decoder_truncated_header()
	{
		let data: Arc<[u8]> = Arc::from(b"BZ".to_vec());
		assert!(matches!(
			ParBz2Decoder::from_bytes(data),
			Err(Bz2Error::InvalidFormat(_))
		));
	}

	// ── find_streams ──────────────────────────────────────────────

	#[test]
	fn test_find_streams_single()
	{
		let original = b"Single stream test.";
		let compressed = compress(original, 9);
		let scanner = Scanner::new();
		let candidates = scanner.scan_parallel(&compressed);
		let streams = find_streams(&compressed, &candidates).unwrap();
		assert_eq!(streams.len(), 1);
		assert_eq!(streams[0].level, 9);
	}

	#[test]
	fn test_find_streams_multi()
	{
		let c1 = compress(b"stream one", 5);
		let c2 = compress(b"stream two", 3);
		let mut combined = c1;
		combined.extend(&c2);

		let scanner = Scanner::new();
		let candidates = scanner.scan_parallel(&combined);
		let streams = find_streams(&combined, &candidates).unwrap();
		assert_eq!(streams.len(), 2);
		assert_eq!(streams[0].level, 5);
		assert_eq!(streams[1].level, 3);
	}

	#[test]
	fn test_find_streams_no_candidates()
	{
		// No candidates at all → error.
		let data = b"BZh9";
		let result = find_streams(data, &[]);
		assert!(matches!(result, Err(Bz2Error::InvalidFormat(_))));
	}

	// ── ParBz2Decoder: multi-stream ───────────────────────────────

	#[test]
	fn test_decoder_multi_stream_same_level()
	{
		let a = b"First stream content.";
		let b_data = b"Second stream content.";

		let mut compressed = compress(a, 9);
		compressed.extend(compress(b_data, 9));

		let mut expected = Vec::new();
		expected.extend_from_slice(a);
		expected.extend_from_slice(b_data);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, expected);
	}

	#[test]
	fn test_decoder_multi_stream_different_levels()
	{
		let a = b"Level three data here.";
		let b_data = b"Level seven data here.";

		let mut compressed = compress(a, 3);
		compressed.extend(compress(b_data, 7));

		let mut expected = Vec::new();
		expected.extend_from_slice(a);
		expected.extend_from_slice(b_data);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, expected);
	}

	#[test]
	fn test_decoder_three_streams()
	{
		let parts: [&[u8]; 3] = [b"one", b"two", b"three"];

		let mut compressed = Vec::new();
		let mut expected = Vec::new();
		for (i, part) in parts.iter().enumerate() {
			compressed.extend(compress(part, (i as u32 % 9) + 1));
			expected.extend_from_slice(part);
		}

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, expected);
	}

	#[test]
	fn test_decoder_multi_stream_multi_block()
	{
		// Two streams, each with multiple blocks (level 1, ~250KB random).
		let mut rng: u64 = 0xDEAD_BEEF;
		let make_random = |rng: &mut u64, len: usize| -> Vec<u8> {
			(0..len)
				.map(|_| {
					*rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
					(*rng >> 33) as u8
				})
				.collect()
		};

		let a = make_random(&mut rng, 250_000);
		let b_data = make_random(&mut rng, 250_000);

		let mut compressed = compress(&a, 1);
		compressed.extend(compress(&b_data, 1));

		let mut expected = Vec::new();
		expected.extend_from_slice(&a);
		expected.extend_from_slice(&b_data);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output.len(), expected.len());
		assert_eq!(output, expected);
	}

	#[test]
	fn test_decoder_multi_stream_empty_first()
	{
		// Empty stream followed by a non-empty stream.
		let mut compressed = compress(b"", 9);
		compressed.extend(compress(b"after empty", 5));

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, b"after empty");
	}

	#[test]
	fn test_decoder_multi_stream_empty_last()
	{
		// Non-empty stream followed by an empty stream.
		let mut compressed = compress(b"before empty", 5);
		compressed.extend(compress(b"", 9));

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, b"before empty");
	}

	#[test]
	fn test_decoder_multi_stream_byte_at_a_time()
	{
		let mut compressed = compress(b"AAA", 9);
		compressed.extend(compress(b"BBB", 9));

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		let mut byte = [0u8; 1];
		loop {
			match decoder.read(&mut byte) {
				Ok(0) => break,
				Ok(1) => output.push(byte[0]),
				Ok(n) => panic!("read returned {n} for 1-byte buffer"),
				Err(e) => panic!("unexpected error: {e}"),
			}
		}

		assert_eq!(output, b"AAABBB");
	}

	#[test]
	fn test_decoder_multi_stream_crc_valid()
	{
		let mut compressed = compress(b"CRC stream one.", 9);
		compressed.extend(compress(b"CRC stream two.", 7));

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		// CRC is verified per-stream inside pull_next_block; if it fails,
		// read_to_end returns an error.
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, b"CRC stream one.CRC stream two.");
	}

	#[test]
	fn test_decoder_multi_stream_matches_reference()
	{
		let mut compressed = compress(b"ref check A", 4);
		compressed.extend(compress(b"ref check B", 6));

		// Reference: decompress each stream independently and concatenate.
		let ref_a = reference_decompress(&compress(b"ref check A", 4));
		let ref_b = reference_decompress(&compress(b"ref check B", 6));
		let mut expected = ref_a;
		expected.extend(ref_b);

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		assert_eq!(output, expected);
	}

	// ── Coverage gap tests ─────────────────────────────────────────

	#[test]
	fn test_decoder_stream_crc_mismatch()
	{
		// Corrupt the stream CRC bytes → triggers CRC mismatch path (lines 250-258).
		let original = b"CRC mismatch test data.";
		let mut compressed = compress(original, 9);

		// The stream CRC is stored right after the EOS magic (0x177245385090).
		// Find the EOS marker, then corrupt a byte safely in the middle of
		// the 32-bit CRC (avoiding overlap with EOS magic bits).
		let scanner = Scanner::new();
		let candidates = scanner.scan_parallel(&compressed);
		let eos = candidates.iter().find(|c| c.marker_type == MarkerType::Eos).unwrap();

		// CRC starts at bit eos.bit_offset + 48.
		// Target the 3rd byte of the CRC (16 bits in) to avoid EOS overlap.
		let target_bit = eos.bit_offset + 48 + 16;
		let target_byte = (target_bit / 8) as usize;

		assert!(
			target_byte < compressed.len(),
			"CRC byte {target_byte} past end of compressed data (len {})",
			compressed.len()
		);
		compressed[target_byte] ^= 0xFF;

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		let result = decoder.read_to_end(&mut output);
		assert!(result.is_err(), "should fail with CRC mismatch");
		let err = result.unwrap_err();
		assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
		assert!(
			err.to_string().contains("CRC mismatch") || err.to_string().contains("stream CRC mismatch"),
			"error message should mention CRC: {}",
			err
		);
	}

	#[test]
	fn test_decoder_stream_crc_mismatch_disabled()
	{
		// Same corruption but with CRC verification disabled → should succeed.
		let original = b"CRC disabled corruption test.";
		let mut compressed = compress(original, 9);

		let scanner = Scanner::new();
		let candidates = scanner.scan_parallel(&compressed);
		let eos = candidates.iter().find(|c| c.marker_type == MarkerType::Eos).unwrap();

		let target_bit = eos.bit_offset + 48 + 16;
		let target_byte = (target_bit / 8) as usize;
		assert!(target_byte < compressed.len());
		compressed[target_byte] ^= 0xFF;

		let mut decoder = ParBz2Decoder::builder()
			.verify_stream_crc(false)
			.from_bytes(Arc::from(compressed))
			.unwrap();
		let mut output = Vec::new();
		// Should succeed since CRC verification is disabled.
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_pipeline_error_propagation()
	{
		// Corrupt compressed block data → triggers pipeline error (lines 244-246).
		let original = b"Pipeline error propagation test data.";
		let mut compressed = compress(original, 9);

		// Find the block magic and corrupt the data right after the block CRC.
		let scanner = Scanner::new();
		let candidates = scanner.scan_parallel(&compressed);
		let block = candidates.iter().find(|c| c.marker_type == MarkerType::Block).unwrap();

		// Block CRC is at block.bit_offset + 48, so data starts at +80.
		// Corrupt bytes in the middle of the block.
		let data_start_byte = ((block.bit_offset + 80) / 8) as usize;
		if data_start_byte + 10 < compressed.len() {
			for i in 0..10 {
				compressed[data_start_byte + i] ^= 0xFF;
			}
		}

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		let result = decoder.read_to_end(&mut output);
		// Should fail — either DecompressionFailed or CrcMismatch propagated through.
		assert!(result.is_err(), "should fail with corrupted block data");
	}

	#[test]
	fn test_find_streams_trailing_without_eos()
	{
		// Blocks without a trailing EOS → error.
		let candidates = vec![Candidate { bit_offset: 32, marker_type: MarkerType::Block }];
		let data = b"BZh9xxxxxxxxxxxxxxxx";
		let result = find_streams(data, &candidates);
		assert!(matches!(result, Err(Bz2Error::InvalidFormat(_))));
	}

	// ── Custom thread pool ────────────────────────────────────────

	#[test]
	fn test_decoder_custom_pool_small()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let original = b"Custom thread pool test data!";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::builder().thread_pool(pool).from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_custom_pool_multi_block()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let mut rng: u64 = 0xC0FF_EE42;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();
		let compressed = compress(&original, 1);

		let mut decoder = ParBz2Decoder::builder().thread_pool(pool).from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	#[test]
	fn test_decoder_custom_pool_multi_stream()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let a = b"Pool stream one.";
		let b_data = b"Pool stream two.";
		let mut compressed = compress(a, 9);
		compressed.extend(compress(b_data, 5));

		let mut expected = Vec::new();
		expected.extend_from_slice(a);
		expected.extend_from_slice(b_data);

		let mut decoder = ParBz2Decoder::builder().thread_pool(pool).from_bytes(Arc::from(compressed)).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, expected);
	}

	#[test]
	fn test_decoder_custom_pool_with_config()
	{
		let pool = Arc::new(rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap());
		let config = PipelineConfig { result_channel_cap: 2, output_channel_cap: 1 };
		let original = b"Pool with custom config.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::builder()
			.thread_pool(pool)
			.pipeline_config(config)
			.from_bytes(Arc::from(compressed))
			.unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output, original);
	}

	#[test]
	fn test_pipeline_config_for_pool()
	{
		let pool = rayon::ThreadPoolBuilder::new().num_threads(3).build().unwrap();
		let config = PipelineConfig::for_pool(&pool);
		assert_eq!(config.result_channel_cap, 6); // 3 * 2
		assert_eq!(config.output_channel_cap, 4);
	}

	// ── DecoderStats ──────────────────────────────────────────────

	#[test]
	fn test_stats_totals_single_stream()
	{
		let original = b"Hello, stats!";
		let compressed = compress(original, 9);
		let decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();
		let stats = decoder.stats();
		assert_eq!(stats.compressed_bytes_total(), compressed.len() as u64);
		assert_eq!(stats.streams_total(), 1);
		assert!(stats.blocks_total() >= 1);
	}

	#[test]
	fn test_stats_after_full_read()
	{
		let original = b"Hello, world! This is a test of decoder statistics.";
		let compressed = compress(original, 9);
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output.as_slice(), original);

		let stats = decoder.stats();
		assert_eq!(stats.decompressed_bytes(), original.len() as u64);
		assert!(stats.blocks_completed() >= 1);
		assert_eq!(stats.blocks_completed(), stats.blocks_total());
		assert_eq!(stats.streams_completed(), 1);
		assert_eq!(stats.streams_completed(), stats.streams_total());
		assert!(stats.compressed_bits_consumed() > 0);
		assert!(stats.elapsed().as_nanos() > 0);
	}

	#[test]
	fn test_stats_progress()
	{
		let original = b"Some data for progress tracking.";
		let compressed = compress(original, 9);
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();

		// Before reading: progress should be 0.
		assert_eq!(decoder.stats().progress(), 0.0);

		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		// After reading: progress should be 1.0.
		assert_eq!(decoder.stats().progress(), 1.0);
	}

	#[test]
	fn test_stats_snapshot()
	{
		let original = b"Snapshot test data";
		let compressed = compress(original, 9);
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		let snap = decoder.stats().snapshot();
		assert_eq!(snap.compressed_bytes_total, compressed.len() as u64);
		assert_eq!(snap.decompressed_bytes, original.len() as u64);
		assert_eq!(snap.blocks_completed, snap.blocks_total);
		assert_eq!(snap.streams_completed, snap.streams_total);
		assert!(snap.compressed_bits_consumed > 0);
		assert_eq!(snap.progress(), 1.0);
	}

	#[test]
	fn test_stats_multi_stream()
	{
		// Concatenate two bzip2 streams.
		let data1 = b"First stream data.";
		let data2 = b"Second stream data.";
		let mut compressed = compress(data1, 9);
		compressed.extend_from_slice(&compress(data2, 9));

		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();
		assert_eq!(decoder.stats().streams_total(), 2);

		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		let expected: Vec<u8> = [data1.as_slice(), data2.as_slice()].concat();
		assert_eq!(output, expected);

		let stats = decoder.stats();
		assert_eq!(stats.streams_completed(), 2);
		assert_eq!(stats.decompressed_bytes(), expected.len() as u64);
		assert_eq!(stats.blocks_completed(), stats.blocks_total());
	}

	#[test]
	fn test_stats_decompression_rate()
	{
		let stats = DecoderStats::new(100, 1, 1);
		// Initially decompressed_bytes is 0 so rate should be 0.
		assert_eq!(stats.decompression_rate(), 0.0);
	}

	#[test]
	fn test_stats_snapshot_decompression_rate()
	{
		let snap = DecoderStatsSnapshot {
			compressed_bytes_total: 100,
			blocks_total: 1,
			streams_total: 1,
			decompressed_bytes: 1_000_000,
			blocks_completed: 1,
			streams_completed: 1,
			compressed_bits_consumed: 800,
			elapsed: Duration::from_secs(2),
		};
		let rate = snap.decompression_rate();
		assert!((rate - 500_000.0).abs() < 0.1);
		assert_eq!(snap.progress(), 1.0);
	}

	#[test]
	fn test_stats_snapshot_zero_elapsed()
	{
		let snap = DecoderStatsSnapshot {
			compressed_bytes_total: 100,
			blocks_total: 1,
			streams_total: 1,
			decompressed_bytes: 500,
			blocks_completed: 1,
			streams_completed: 1,
			compressed_bits_consumed: 800,
			elapsed: Duration::ZERO,
		};
		assert_eq!(snap.decompression_rate(), 0.0);
	}

	#[test]
	fn test_stats_progress_zero_blocks()
	{
		let stats = DecoderStats::new(0, 0, 0);
		assert_eq!(stats.progress(), 0.0);
	}

	#[test]
	fn test_stats_debug_impl()
	{
		let stats = DecoderStats::new(100, 5, 2);
		let debug = format!("{:?}", stats);
		assert!(debug.contains("DecoderStats"));
		assert!(debug.contains("compressed_bytes_total: 100"));
		assert!(debug.contains("blocks_total: 5"));
		assert!(debug.contains("streams_total: 2"));
	}

	#[test]
	fn test_stats_arc_cloneable()
	{
		let original = b"Arc clone test";
		let compressed = compress(original, 9);
		let decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();
		// The stats Arc can be cloned for use from another thread.
		let stats_clone = Arc::clone(decoder.stats());
		assert_eq!(stats_clone.compressed_bytes_total(), compressed.len() as u64);
	}

	#[test]
	fn test_on_progress_callback()
	{
		use std::sync::atomic::{AtomicU64, Ordering};

		let call_count = Arc::new(AtomicU64::new(0));
		let last_blocks = Arc::new(AtomicU64::new(0));
		let cc = Arc::clone(&call_count);
		let lb = Arc::clone(&last_blocks);

		let original = b"Callback test data.";
		let compressed = compress(original, 9);

		let mut decoder = ParBz2Decoder::builder()
			.on_progress(move |stats| {
				cc.fetch_add(1, Ordering::Relaxed);
				lb.store(stats.blocks_completed(), Ordering::Relaxed);
			})
			.from_bytes(Arc::from(compressed.as_slice()))
			.unwrap();

		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();
		assert_eq!(output.as_slice(), original);

		// Callback should have been called at least once (once per block).
		assert!(call_count.load(Ordering::Relaxed) >= 1);
		assert!(last_blocks.load(Ordering::Relaxed) >= 1);
	}

	#[test]
	fn test_builder_on_progress_is_cloneable()
	{
		let builder = ParBz2Decoder::builder().on_progress(|_stats| {});
		let _clone = builder.clone();
	}

	#[test]
	fn test_builder_on_progress_is_debuggable()
	{
		let builder = ParBz2Decoder::builder().on_progress(|_stats| {});
		let debug = format!("{:?}", builder);
		assert!(debug.contains("ParBz2DecoderBuilder"));
	}

	#[test]
	fn test_stats_snapshot_progress_zero_blocks()
	{
		let snap = DecoderStatsSnapshot {
			compressed_bytes_total: 0,
			blocks_total: 0,
			streams_total: 0,
			decompressed_bytes: 0,
			blocks_completed: 0,
			streams_completed: 0,
			compressed_bits_consumed: 0,
			elapsed: Duration::from_secs(1),
		};
		assert_eq!(snap.progress(), 0.0);
	}

	#[test]
	fn test_stats_snapshot_decompression_rate_zero_elapsed()
	{
		// Redundant with test_stats_snapshot_zero_elapsed but explicitly
		// targets the DecoderStatsSnapshot::decompression_rate() path.
		let snap = DecoderStatsSnapshot {
			compressed_bytes_total: 100,
			blocks_total: 1,
			streams_total: 1,
			decompressed_bytes: 5000,
			blocks_completed: 1,
			streams_completed: 1,
			compressed_bits_consumed: 800,
			elapsed: Duration::ZERO,
		};
		assert_eq!(snap.decompression_rate(), 0.0);
	}

	#[test]
	fn test_stats_decompression_rate_after_read()
	{
		let original = b"Rate test data.";
		let compressed = compress(original, 9);
		let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed.as_slice())).unwrap();
		let mut output = Vec::new();
		decoder.read_to_end(&mut output).unwrap();

		// After real decompression, rate should be positive.
		let rate = decoder.stats().decompression_rate();
		assert!(rate > 0.0, "decompression_rate should be > 0 after read, got {rate}");
	}
}
