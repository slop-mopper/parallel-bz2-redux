// decompress/pipeline.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Streaming speculative decompression pipeline.
//!
//! Decompresses bzip2 blocks in parallel using Rayon, validates CRCs,
//! handles false-positive scanner hits via merge-and-retry, and streams
//! validated blocks through bounded channels with backpressure.
//!
//! # Architecture
//!
//! ```text
//! Scanner candidates
//!   → pair_candidates() → Vec<BlockRange>
//!   → Dispatcher thread (rayon::scope_fifo)
//!       → workers decompress blocks in parallel
//!       → bounded result channel (capacity ~2× threads)
//!   → Validator thread
//!       → reorder buffer (HashMap)
//!       → CRC validation
//!       → false-positive merge-and-retry
//!       → bounded output channel (capacity ~4)
//!   → recv() pulls validated blocks lazily
//! ```
//!
//! Backpressure flows naturally: when the reader stalls, the output
//! channel fills, the validator blocks, the result channel fills, and
//! Rayon workers block on send.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread;

use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use crossbeam_channel::bounded;

use crate::decompress::block::BlockResult;
use crate::decompress::block::decompress_block;
use crate::error::Bz2Error;
use crate::error::Result;
use crate::scanner::Candidate;
use crate::scanner::MarkerType;

// ── Types ───────────────────────────────────────────────────────────

/// A range of bits representing one candidate block: `[start_bit, end_bit)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockRange
{
	/// Block index in the sequence (0-based).
	pub index: usize,
	/// Bit offset of the 48-bit block magic (inclusive).
	pub start_bit: u64,
	/// Bit offset of the next marker (exclusive).
	pub end_bit: u64,
}

/// Configuration for the decompression pipeline.
#[derive(Debug, Clone)]
pub struct PipelineConfig
{
	/// Capacity of the result channel (workers → validator).
	/// Default: `2 × rayon::current_num_threads()`.
	pub result_channel_cap: usize,
	/// Capacity of the output channel (validator → reader).
	/// Default: `4`.
	pub output_channel_cap: usize,
}

impl Default for PipelineConfig
{
	fn default() -> Self
	{
		let threads = rayon::current_num_threads();
		Self { result_channel_cap: threads * 2, output_channel_cap: 4 }
	}
}

/// A validated, decompressed block ready for consumption.
#[derive(Debug)]
pub struct ValidatedBlock
{
	/// Block index in the original sequence (0-based).
	///
	/// When a contiguous failure group is merged and re-decompressed,
	/// the resulting block carries the index of the *first* block in
	/// the group.
	pub index: usize,
	/// Decompressed block data.
	pub data: Vec<u8>,
	/// Validated block CRC.
	pub crc: u32,
}

// ── Candidate pairing ───────────────────────────────────────────────

/// Pair sorted scanner candidates into block ranges.
///
/// Separates `Block` and `Eos` markers, filters blocks past the EOS,
/// and creates `[start, end)` ranges from consecutive block starts.
///
/// Returns `(ranges, eos_bit)`.
pub fn pair_candidates(candidates: &[Candidate]) -> Result<(Vec<BlockRange>, u64)>
{
	let mut block_starts: Vec<u64> = Vec::new();
	let mut eos_bit: Option<u64> = None;

	for c in candidates {
		match c.marker_type {
			MarkerType::Block => block_starts.push(c.bit_offset),
			MarkerType::Eos => {
				if eos_bit.is_some() {
					return Err(Bz2Error::InvalidFormat("multiple EOS markers found".into()));
				}
				eos_bit = Some(c.bit_offset);
			}
		}
	}

	let eos = eos_bit.ok_or_else(|| Bz2Error::InvalidFormat("no EOS marker found".into()))?;

	block_starts.retain(|&b| b < eos);
	block_starts.sort();

	let mut ranges = Vec::with_capacity(block_starts.len());
	for (i, &start) in block_starts.iter().enumerate() {
		let end = if i + 1 < block_starts.len() {
			block_starts[i + 1]
		} else {
			eos
		};
		ranges.push(BlockRange { index: i, start_bit: start, end_bit: end });
	}

	Ok((ranges, eos))
}

// ── Pipeline ────────────────────────────────────────────────────────

/// Handle to the streaming decompression pipeline.
///
/// Owns the dispatcher and validator background threads.  The caller
/// pulls validated blocks via [`recv()`](Self::recv).  Dropping the
/// handle signals the pipeline to stop and joins the background threads.
pub struct DecompressPipeline
{
	/// `None` after the pipeline has been shut down.
	output_rx: Option<Receiver<Result<ValidatedBlock>>>,
	dispatcher_handle: Option<thread::JoinHandle<()>>,
	validator_handle: Option<thread::JoinHandle<()>>,
}

impl DecompressPipeline
{
	/// Start the pipeline.
	///
	/// `data` is the full compressed file.  `candidates` must be sorted
	/// by bit offset (as returned by [`Scanner::scan`] /
	/// [`Scanner::scan_parallel`]).  `level` is the bzip2 compression
	/// level from the stream header (`1`–`9`).
	pub fn start(data: Arc<[u8]>, candidates: Vec<Candidate>, level: u8, config: PipelineConfig) -> Result<Self>
	{
		let (ranges, eos_bit) = pair_candidates(&candidates)?;

		if ranges.is_empty() {
			// No blocks — immediately-exhausted pipeline.
			let (_tx, rx) = bounded(1);
			return Ok(Self {
				output_rx: Some(rx),
				dispatcher_handle: None,
				validator_handle: None,
			});
		}

		let (result_tx, result_rx) = bounded::<(usize, Result<BlockResult>)>(config.result_channel_cap);
		let (output_tx, output_rx) = bounded::<Result<ValidatedBlock>>(config.output_channel_cap);

		let ranges_for_validator = ranges.clone();
		let data_for_validator = Arc::clone(&data);

		// ── Dispatcher: Rayon FIFO tasks for each block range ───────
		let dispatcher_handle = thread::Builder::new()
			.name("bz2-dispatcher".into())
			.spawn(move || {
				rayon::scope_fifo(|s| {
					for range in ranges.iter().copied() {
						let data = Arc::clone(&data);
						let tx = result_tx.clone();
						s.spawn_fifo(move |_| {
							let result = decompress_block(&data, range.start_bit, range.end_bit, level);
							// Silently drop if receiver is gone (pipeline shutting down).
							let _ = tx.send((range.index, result));
						});
					}
					// All clones created; original `result_tx` drops when
					// this closure returns → channel closes after scope
					// waits for all tasks.
				});
			})
			.expect("failed to spawn dispatcher thread");

		// ── Validator: reorder, CRC-check, merge-and-retry ─────────
		let validator_handle = thread::Builder::new()
			.name("bz2-validator".into())
			.spawn(move || {
				Validator {
					data: data_for_validator,
					ranges: ranges_for_validator,
					level,
					eos_bit,
					buffer: HashMap::new(),
					next_expected: 0,
					failure_group_start: None,
					output_tx,
				}
				.run(result_rx);
			})
			.expect("failed to spawn validator thread");

		Ok(Self {
			output_rx: Some(output_rx),
			dispatcher_handle: Some(dispatcher_handle),
			validator_handle: Some(validator_handle),
		})
	}

	/// Receive the next validated block in order, or `None` if done.
	///
	/// Blocks until a block is available or the pipeline has finished.
	pub fn recv(&self) -> Option<Result<ValidatedBlock>>
	{
		self.output_rx.as_ref()?.recv().ok()
	}
}

impl Drop for DecompressPipeline
{
	fn drop(&mut self)
	{
		// Close the output channel to unblock the validator.
		self.output_rx.take();

		// Join background threads (validator first — it stops quickly
		// once the output channel is closed; then dispatcher — it waits
		// for in-progress workers to finish).
		if let Some(h) = self.validator_handle.take() {
			let _ = h.join();
		}
		if let Some(h) = self.dispatcher_handle.take() {
			let _ = h.join();
		}
	}
}

// ── Validator ───────────────────────────────────────────────────────

/// Internal state for the validator thread.
///
/// Receives worker results out of order, buffers them, and emits
/// validated blocks strictly in sequence.  When a contiguous group of
/// failures is followed by a success, the group is merged into a single
/// range and re-decompressed (handling false-positive scanner hits).
struct Validator
{
	data: Arc<[u8]>,
	ranges: Vec<BlockRange>,
	level: u8,
	eos_bit: u64,

	/// Out-of-order results waiting to be processed.
	buffer: HashMap<usize, Result<BlockResult>>,
	/// Next block index the validator expects to emit.
	next_expected: usize,
	/// First block index of the current contiguous failure group.
	failure_group_start: Option<usize>,

	output_tx: Sender<Result<ValidatedBlock>>,
}

impl Validator
{
	/// Run the validator loop until all blocks are processed or the
	/// reader drops the pipeline.
	fn run(mut self, result_rx: Receiver<(usize, Result<BlockResult>)>)
	{
		// Receive results and process consecutive ready blocks.
		for (idx, result) in result_rx.iter() {
			self.buffer.insert(idx, result);
			if !self.drain_ready() {
				return;
			}
		}

		// All workers finished — drain any remaining buffered results
		// (handles the case where the last results arrived out of order
		// and were buffered but not yet drained).
		while self.buffer.contains_key(&self.next_expected) {
			if !self.drain_ready() {
				return;
			}
		}

		// Handle trailing failure group (all remaining blocks through
		// EOS failed — merge to [first_fail.start, eos) and retry).
		self.resolve_trailing_failures();
	}

	/// Process consecutive ready blocks starting from `next_expected`.
	///
	/// Returns `false` if the pipeline should stop (reader dropped or
	/// fatal error emitted).
	fn drain_ready(&mut self) -> bool
	{
		while let Some(result) = self.buffer.remove(&self.next_expected) {
			let is_success = match &result {
				Ok(br) => br.crc_valid,
				Err(_) => false,
			};

			if is_success {
				// SAFETY: is_success guarantees Ok.
				let br = result.unwrap();

				// Resolve any pending failure group first.
				if let Some(fg_start) = self.failure_group_start.take() {
					if !self.resolve_failure_group(fg_start) {
						return false;
					}
				}

				// Emit the successful block.
				if !self.emit(ValidatedBlock { index: self.next_expected, data: br.data, crc: br.stored_crc }) {
					return false;
				}
			} else {
				// Failure — accumulate into the current failure group.
				if self.failure_group_start.is_none() {
					self.failure_group_start = Some(self.next_expected);
				}
			}

			self.next_expected += 1;
		}
		true
	}

	/// Merge a contiguous failure group `[fg_start .. next_expected)`
	/// into one range and re-decompress.
	///
	/// The merged range is `[ranges[fg_start].start_bit,
	/// ranges[next_expected].start_bit)`, which spans the true block
	/// that was split by false-positive boundaries.
	fn resolve_failure_group(&self, fg_start: usize) -> bool
	{
		let merged_start = self.ranges[fg_start].start_bit;
		let merged_end = self.ranges[self.next_expected].start_bit;

		match decompress_block(&self.data, merged_start, merged_end, self.level) {
			Ok(br) if br.crc_valid => self.emit(ValidatedBlock { index: fg_start, data: br.data, crc: br.stored_crc }),
			// DEFENSIVE: This branch is unreachable with a correct libbzip2
			// implementation.  libbzip2 validates the block CRC internally
			// during decompression — if the CRC doesn't match, it returns
			// BZ_DATA_ERROR (mapped to Err), never Ok.  Our stored_crc is
			// read from the same bits that libbzip2 reads, so a successful
			// decompression guarantees crc_valid == true.  Kept as a safety
			// net in case of libbzip2 behavioural changes.
			Ok(br) => self.emit_error(Bz2Error::CrcMismatch {
				offset: merged_start,
				stored: br.stored_crc,
				computed: br.computed_crc,
			}),
			Err(e) => self.emit_error(e),
		}
	}

	/// Handle trailing failures: all blocks from `fg_start` through
	/// EOS failed.  Merge to `[first_fail.start, eos_bit)` and retry.
	fn resolve_trailing_failures(&self)
	{
		let Some(fg_start) = self.failure_group_start else {
			return;
		};

		let merged_start = self.ranges[fg_start].start_bit;
		let merged_end = self.eos_bit;

		match decompress_block(&self.data, merged_start, merged_end, self.level) {
			Ok(br) if br.crc_valid => {
				let _ = self.output_tx.send(Ok(ValidatedBlock {
					index: fg_start,
					data: br.data,
					crc: br.stored_crc,
				}));
			}
			// DEFENSIVE: Unreachable for the same reason as in
			// resolve_failure_group — see comment there.
			Ok(br) => {
				let _ = self.output_tx.send(Err(Bz2Error::CrcMismatch {
					offset: merged_start,
					stored: br.stored_crc,
					computed: br.computed_crc,
				}));
			}
			Err(e) => {
				let _ = self.output_tx.send(Err(e));
			}
		}
	}

	/// Send a validated block to the output channel.
	///
	/// Returns `false` if the receiver has been dropped.
	fn emit(&self, block: ValidatedBlock) -> bool
	{
		self.output_tx.send(Ok(block)).is_ok()
	}

	/// Send a fatal error to the output channel.  Always returns
	/// `false` (the pipeline should stop).
	fn emit_error(&self, err: Bz2Error) -> bool
	{
		let _ = self.output_tx.send(Err(err));
		false
	}
}

// ── Tests ───────────────────────────────────────────────────────────

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use std::io::Write;

	use super::*;
	use crate::scanner::Scanner;

	// ── Helpers ─────────────────────────────────────────────────────

	fn compress(data: &[u8], level: u32) -> Vec<u8>
	{
		let mut enc = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::new(level));
		enc.write_all(data).unwrap();
		enc.finish().unwrap()
	}

	fn pipeline_decompress(compressed: &[u8], level: u8) -> Vec<u8>
	{
		let data: Arc<[u8]> = Arc::from(compressed);
		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);
		let pipeline = DecompressPipeline::start(data, candidates, level, PipelineConfig::default()).unwrap();
		let mut output = Vec::new();
		while let Some(result) = pipeline.recv() {
			let block = result.unwrap();
			output.extend_from_slice(&block.data);
		}
		output
	}

	// ── pair_candidates ─────────────────────────────────────────────

	#[test]
	fn test_pair_candidates_single_block()
	{
		let candidates = vec![
			Candidate { bit_offset: 32, marker_type: MarkerType::Block },
			Candidate { bit_offset: 1000, marker_type: MarkerType::Eos },
		];
		let (ranges, eos) = pair_candidates(&candidates).unwrap();
		assert_eq!(eos, 1000);
		assert_eq!(ranges.len(), 1);
		assert_eq!(ranges[0], BlockRange { index: 0, start_bit: 32, end_bit: 1000 });
	}

	#[test]
	fn test_pair_candidates_multi_block()
	{
		let candidates = vec![
			Candidate { bit_offset: 32, marker_type: MarkerType::Block },
			Candidate { bit_offset: 500, marker_type: MarkerType::Block },
			Candidate { bit_offset: 1000, marker_type: MarkerType::Block },
			Candidate { bit_offset: 1500, marker_type: MarkerType::Eos },
		];
		let (ranges, eos) = pair_candidates(&candidates).unwrap();
		assert_eq!(eos, 1500);
		assert_eq!(ranges.len(), 3);
		assert_eq!(ranges[0], BlockRange { index: 0, start_bit: 32, end_bit: 500 });
		assert_eq!(ranges[1], BlockRange { index: 1, start_bit: 500, end_bit: 1000 });
		assert_eq!(ranges[2], BlockRange { index: 2, start_bit: 1000, end_bit: 1500 });
	}

	#[test]
	fn test_pair_candidates_no_blocks()
	{
		let candidates = vec![Candidate { bit_offset: 32, marker_type: MarkerType::Eos }];
		let (ranges, eos) = pair_candidates(&candidates).unwrap();
		assert_eq!(eos, 32);
		assert!(ranges.is_empty());
	}

	#[test]
	fn test_pair_candidates_no_eos()
	{
		let candidates = vec![Candidate { bit_offset: 32, marker_type: MarkerType::Block }];
		assert!(matches!(pair_candidates(&candidates), Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_pair_candidates_multiple_eos()
	{
		let candidates = vec![
			Candidate { bit_offset: 32, marker_type: MarkerType::Block },
			Candidate { bit_offset: 1000, marker_type: MarkerType::Eos },
			Candidate { bit_offset: 2000, marker_type: MarkerType::Eos },
		];
		assert!(matches!(pair_candidates(&candidates), Err(Bz2Error::InvalidFormat(_))));
	}

	#[test]
	fn test_pair_candidates_unsorted()
	{
		// Candidates out of order — pair_candidates sorts block_starts.
		let candidates = vec![
			Candidate { bit_offset: 1000, marker_type: MarkerType::Block },
			Candidate { bit_offset: 32, marker_type: MarkerType::Block },
			Candidate { bit_offset: 1500, marker_type: MarkerType::Eos },
		];
		let (ranges, _) = pair_candidates(&candidates).unwrap();
		assert_eq!(ranges[0].start_bit, 32);
		assert_eq!(ranges[1].start_bit, 1000);
	}

	#[test]
	fn test_pair_candidates_block_after_eos_filtered()
	{
		let candidates = vec![
			Candidate { bit_offset: 32, marker_type: MarkerType::Block },
			Candidate { bit_offset: 500, marker_type: MarkerType::Eos },
			Candidate { bit_offset: 800, marker_type: MarkerType::Block }, // past EOS
		];
		let (ranges, eos) = pair_candidates(&candidates).unwrap();
		assert_eq!(eos, 500);
		assert_eq!(ranges.len(), 1);
		assert_eq!(ranges[0].end_bit, 500);
	}

	// ── Pipeline: happy path ────────────────────────────────────────

	#[test]
	fn test_pipeline_single_block()
	{
		let original = b"Hello, pipeline world!";
		let compressed = compress(original, 9);
		let output = pipeline_decompress(&compressed, 9);
		assert_eq!(output, original);
	}

	#[test]
	fn test_pipeline_medium_text()
	{
		let line = "The quick brown fox jumps over the lazy dog.\n";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(4096).collect();
		let compressed = compress(&original, 9);
		let output = pipeline_decompress(&compressed, 9);
		assert_eq!(output, original);
	}

	#[test]
	fn test_pipeline_multi_block()
	{
		// ~250KB at level 1 (100KB blocks) to force multiple blocks.
		let mut rng: u64 = 0x1234_5678_ABCD_EF01;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		// Verify multiple blocks exist.
		let block_count = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).count();
		assert!(block_count >= 2, "expected ≥2 blocks at level 1, got {block_count}");

		let pipeline = DecompressPipeline::start(data, candidates, 1, PipelineConfig::default()).unwrap();

		let mut output = Vec::new();
		let mut block_indices = Vec::new();
		while let Some(result) = pipeline.recv() {
			let block = result.unwrap();
			block_indices.push(block.index);
			output.extend_from_slice(&block.data);
		}

		// Blocks must arrive in order.
		for w in block_indices.windows(2) {
			assert!(w[0] < w[1], "blocks out of order: {} >= {}", w[0], w[1]);
		}

		assert_eq!(output, original);
	}

	#[test]
	fn test_pipeline_all_levels()
	{
		let original = b"Pipeline level sweep test data - enough bytes to compress.";
		for level in 1..=9u32 {
			let compressed = compress(original, level);
			let output = pipeline_decompress(&compressed, level as u8);
			assert_eq!(output, original.as_slice(), "mismatch at level {level}");
		}
	}

	// ── Pipeline: empty input ───────────────────────────────────────

	#[test]
	fn test_pipeline_empty()
	{
		let compressed = compress(b"", 9);
		let output = pipeline_decompress(&compressed, 9);
		assert!(output.is_empty());
	}

	// ── Pipeline: early drop ────────────────────────────────────────

	#[test]
	fn test_pipeline_drop_before_consuming()
	{
		// Verify that dropping the pipeline without consuming all
		// blocks doesn't hang or panic.
		let original: Vec<u8> = vec![0xAA; 4096];
		let compressed = compress(&original, 9);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		let pipeline = DecompressPipeline::start(data, candidates, 9, PipelineConfig::default()).unwrap();

		// Read one block (if available) then drop.
		let _ = pipeline.recv();
		drop(pipeline); // should not hang
	}

	// ── Pipeline: config ────────────────────────────────────────────

	#[test]
	fn test_pipeline_small_channels()
	{
		// Minimal channel capacities to stress backpressure.
		let original = b"Small channel pipeline test.";
		let compressed = compress(original, 9);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		let config = PipelineConfig { result_channel_cap: 1, output_channel_cap: 1 };
		let pipeline = DecompressPipeline::start(data, candidates, 9, config).unwrap();

		let mut output = Vec::new();
		while let Some(result) = pipeline.recv() {
			output.extend_from_slice(&result.unwrap().data);
		}
		assert_eq!(output, original);
	}

	#[test]
	fn test_pipeline_multi_block_small_channels()
	{
		// Multi-block with minimal channels — maximum backpressure.
		let mut rng: u64 = 0xFEED_FACE_DEAD_BEEF;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		let config = PipelineConfig { result_channel_cap: 1, output_channel_cap: 1 };
		let pipeline = DecompressPipeline::start(data, candidates, 1, config).unwrap();

		let mut output = Vec::new();
		while let Some(result) = pipeline.recv() {
			output.extend_from_slice(&result.unwrap().data);
		}
		assert_eq!(output, original);
	}

	// ── Pipeline: binary data ───────────────────────────────────────

	#[test]
	fn test_pipeline_binary_all_values()
	{
		let original: Vec<u8> = (0..4).flat_map(|_| 0u8..=255).collect();
		let compressed = compress(&original, 9);
		let output = pipeline_decompress(&compressed, 9);
		assert_eq!(output, original);
	}

	// ── Coverage gap tests: false-positive merge-and-retry ─────────

	/// Inject a fake Block candidate into the middle of a real block,
	/// causing two consecutive failures that must be merged and retried.
	#[test]
	fn test_pipeline_false_positive_merged()
	{
		// Compress data large enough for a single real block.
		let line = "False positive merge test data. ";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(2048).collect();
		let compressed = compress(&original, 9);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let mut candidates = scanner.scan(&data);

		// Find the real block and EOS.
		let block_candidates: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).collect();
		let eos_candidate = candidates.iter().find(|c| c.marker_type == MarkerType::Eos).unwrap();
		assert_eq!(block_candidates.len(), 1, "need exactly 1 real block");

		// Inject a fake Block candidate halfway between the real block and EOS.
		let fake_bit = (block_candidates[0].bit_offset + eos_candidate.bit_offset) / 2;
		// Round to byte boundary to avoid collisions.
		let fake_bit = (fake_bit / 8) * 8;
		candidates.push(Candidate { bit_offset: fake_bit, marker_type: MarkerType::Block });
		candidates.sort();

		// The pipeline should: fail on both sub-ranges, merge [real_start, eos),
		// and succeed on the merged range.
		let pipeline = DecompressPipeline::start(data, candidates, 9, PipelineConfig::default()).unwrap();
		let mut output = Vec::new();
		while let Some(result) = pipeline.recv() {
			let block = result.unwrap();
			output.extend_from_slice(&block.data);
		}
		assert_eq!(output, original);
	}

	/// Inject a fake Block candidate after the last real block (before EOS),
	/// causing a trailing failure group that must be merged with EOS.
	#[test]
	fn test_pipeline_false_positive_trailing()
	{
		let original = b"Trailing false positive test data for pipeline coverage.";
		let compressed = compress(original, 9);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let mut candidates = scanner.scan(&data);

		let eos_candidate = *candidates.iter().find(|c| c.marker_type == MarkerType::Eos).unwrap();

		// Inject a fake Block candidate near the EOS (but before it).
		let fake_bit = eos_candidate.bit_offset - 16;
		let fake_bit = (fake_bit / 8) * 8;
		candidates.push(Candidate { bit_offset: fake_bit, marker_type: MarkerType::Block });
		candidates.sort();

		let pipeline = DecompressPipeline::start(data, candidates, 9, PipelineConfig::default()).unwrap();
		let mut output = Vec::new();
		while let Some(result) = pipeline.recv() {
			let block = result.unwrap();
			output.extend_from_slice(&block.data);
		}
		assert_eq!(output, original);
	}

	/// Multi-block file with a fake candidate injected between two real blocks.
	/// Tests resolve_failure_group() where a success follows the failure group.
	#[test]
	fn test_pipeline_false_positive_between_real_blocks()
	{
		// ~250KB random at level 1 → forces multiple real blocks.
		let mut rng: u64 = 0xC0DE_CAFE;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let mut candidates = scanner.scan(&data);

		let blocks: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).copied().collect();
		assert!(blocks.len() >= 2, "need ≥2 real blocks, got {}", blocks.len());

		// Inject a fake block halfway between the first two real blocks.
		let fake_bit = (blocks[0].bit_offset + blocks[1].bit_offset) / 2;
		let fake_bit = (fake_bit / 8) * 8;
		candidates.push(Candidate { bit_offset: fake_bit, marker_type: MarkerType::Block });
		candidates.sort();

		let pipeline = DecompressPipeline::start(data, candidates, 1, PipelineConfig::default()).unwrap();
		let mut output = Vec::new();
		while let Some(result) = pipeline.recv() {
			let block = result.unwrap();
			output.extend_from_slice(&block.data);
		}
		assert_eq!(output, original);
	}

	/// Test the validator early-return path when reader drops mid-stream (line 292).
	#[test]
	fn test_pipeline_validator_early_return()
	{
		// Multi-block, drop after receiving first block.
		let mut rng: u64 = 0xBEEF_F00D;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let compressed = compress(&original, 1);
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let candidates = scanner.scan(&data);

		let config = PipelineConfig { result_channel_cap: 1, output_channel_cap: 1 };
		let pipeline = DecompressPipeline::start(data, candidates, 1, config).unwrap();

		// Consume only one block, then drop — validator should exit cleanly.
		let first = pipeline.recv();
		assert!(first.is_some());
		drop(pipeline); // Should not hang or panic.
	}

	// ── Coverage: corrupt data + false positive → emit_error ────────

	/// Helper: corrupt payload bytes inside a compressed block, leaving
	/// the block magic, EOS magic, and their surrounding structure intact
	/// so the scanner still finds all markers.
	fn corrupt_block_payload(compressed: &mut [u8], block_bit: u64)
	{
		// Block layout: 48-bit magic + 32-bit CRC + 1-bit randomised + 24-bit orig_ptr + ...
		// Payload starts around bit_offset + 105.  Corrupt 10 bytes well into
		// the Huffman/RLE data.
		let payload_byte = ((block_bit + 160) / 8) as usize;
		for i in 0..10 {
			if payload_byte + i < compressed.len() {
				compressed[payload_byte + i] ^= 0xFF;
			}
		}
	}

	/// Corrupt data + fake candidate mid-block → resolve_failure_group
	/// gets Err from the merged re-decompress → emit_error (path A/C).
	#[test]
	fn test_pipeline_corrupt_data_with_false_positive_mid_block()
	{
		let line = "Corrupt data merge-and-retry error test. ";
		let original: Vec<u8> = line.as_bytes().iter().copied().cycle().take(4096).collect();
		let mut compressed = compress(&original, 9);
		let data_arc: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let mut candidates = scanner.scan(&data_arc);

		let block = candidates.iter().find(|c| c.marker_type == MarkerType::Block).unwrap();
		let eos = candidates.iter().find(|c| c.marker_type == MarkerType::Eos).unwrap();
		let block_bit = block.bit_offset;
		let eos_bit = eos.bit_offset;

		// Corrupt the block payload in the mutable copy.
		corrupt_block_payload(&mut compressed, block_bit);

		// Inject a fake Block candidate mid-block.
		let fake_bit = ((block_bit + eos_bit) / 2 / 8) * 8;
		candidates.push(Candidate { bit_offset: fake_bit, marker_type: MarkerType::Block });
		candidates.sort();

		// Use the corrupted data.
		let data: Arc<[u8]> = Arc::from(compressed.as_slice());
		let pipeline = DecompressPipeline::start(data, candidates, 9, PipelineConfig::default()).unwrap();

		// Should receive an error (merged range also fails to decompress).
		let mut got_error = false;
		while let Some(result) = pipeline.recv() {
			if result.is_err() {
				got_error = true;
				break;
			}
		}
		assert!(
			got_error,
			"pipeline should propagate error for corrupt data with false positive"
		);
	}

	/// Corrupt data + fake candidate near EOS → resolve_trailing_failures
	/// gets Err → sends error to output channel (path B/C).
	#[test]
	fn test_pipeline_corrupt_data_with_trailing_false_positive()
	{
		let original = b"Trailing failure with corrupt data for emit_error coverage.";
		let mut compressed = compress(original, 9);
		let data_arc: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let mut candidates = scanner.scan(&data_arc);

		let block = candidates.iter().find(|c| c.marker_type == MarkerType::Block).unwrap();
		let eos = candidates.iter().find(|c| c.marker_type == MarkerType::Eos).unwrap();
		let block_bit = block.bit_offset;
		let eos_bit = eos.bit_offset;

		// Corrupt payload.
		corrupt_block_payload(&mut compressed, block_bit);

		// Inject fake trailing candidate near EOS (but before it).
		let fake_bit = ((eos_bit - 16) / 8) * 8;
		candidates.push(Candidate { bit_offset: fake_bit, marker_type: MarkerType::Block });
		candidates.sort();

		let data: Arc<[u8]> = Arc::from(compressed.as_slice());
		let pipeline = DecompressPipeline::start(data, candidates, 9, PipelineConfig::default()).unwrap();

		let mut got_error = false;
		while let Some(result) = pipeline.recv() {
			if result.is_err() {
				got_error = true;
				break;
			}
		}
		assert!(
			got_error,
			"pipeline should propagate error for corrupt trailing failure"
		);
	}

	/// Multi-block: corrupt first block + inject fake between blocks 1 and 2.
	/// Tests resolve_failure_group error path when a successful block follows.
	#[test]
	fn test_pipeline_corrupt_first_block_with_false_positive()
	{
		// Generate multi-block data.
		let mut rng: u64 = 0xBADD_A7A1;
		let original: Vec<u8> = (0..250_000)
			.map(|_| {
				rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
				(rng >> 33) as u8
			})
			.collect();

		let mut compressed = compress(&original, 1);
		let data_arc: Arc<[u8]> = Arc::from(compressed.as_slice());

		let scanner = Scanner::new();
		let mut candidates = scanner.scan(&data_arc);

		let blocks: Vec<_> = candidates.iter().filter(|c| c.marker_type == MarkerType::Block).copied().collect();
		assert!(blocks.len() >= 2, "need ≥2 blocks, got {}", blocks.len());

		// Corrupt only the first block's payload.
		corrupt_block_payload(&mut compressed, blocks[0].bit_offset);

		// Inject fake candidate between first and second real blocks.
		let fake_bit = ((blocks[0].bit_offset + blocks[1].bit_offset) / 2 / 8) * 8;
		candidates.push(Candidate { bit_offset: fake_bit, marker_type: MarkerType::Block });
		candidates.sort();

		let data: Arc<[u8]> = Arc::from(compressed.as_slice());
		let pipeline = DecompressPipeline::start(data, candidates, 1, PipelineConfig::default()).unwrap();

		// The first block's failure group (block 0 + fake) will fail even after
		// merge.  The pipeline should emit an error for it.
		let mut got_error = false;
		while let Some(result) = pipeline.recv() {
			if result.is_err() {
				got_error = true;
				break;
			}
		}
		assert!(
			got_error,
			"pipeline should emit error for corrupt block even after merge attempt"
		);
	}
}
