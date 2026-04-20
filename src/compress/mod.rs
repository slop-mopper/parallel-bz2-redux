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

use std::io::Write;

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

// ── ParBz2Encoder ──────────────────────────────────────────────────

/// Parallel bzip2 encoder implementing [`Write`].
///
/// Compresses data using multiple threads via Rayon.  Input is buffered
/// and compressed in block-sized chunks in parallel as data arrives.
/// Compressed output is streamed to the inner writer incrementally.
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
			buffer: Vec::new(),
			output: BitWriter::new(),
			stream_crc: 0,
			header_written: false,
			total_in: 0,
			total_out: 0,
			done: false,
			panicked: false,
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

		// ── Compress remaining buffered data (partial block) ────────
		if !self.buffer.is_empty() {
			let block = compress_block(&self.buffer, self.level).map_err(|e| std::io::Error::other(e.to_string()))?;
			self.output.copy_bits_from(&block.bits, 0, block.bit_len);
			self.stream_crc = combine_stream_crc(self.stream_crc, block.block_crc);
			self.buffer.clear();
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

	/// Compress all full blocks in the buffer, streaming output to the
	/// inner writer incrementally.
	fn compress_pending(&mut self) -> std::io::Result<()>
	{
		let n_blocks = self.buffer.len() / self.block_size;
		if n_blocks == 0 {
			return Ok(());
		}

		self.ensure_header();

		let n_bytes = n_blocks * self.block_size;
		let chunks: Vec<&[u8]> = self.buffer[..n_bytes].chunks(self.block_size).collect();
		let blocks = compress_blocks_parallel(&chunks, self.level).map_err(|e| std::io::Error::other(e.to_string()))?;

		for block in &blocks {
			self.output.copy_bits_from(&block.bits, 0, block.bit_len);
			self.stream_crc = combine_stream_crc(self.stream_crc, block.block_crc);
		}

		self.buffer.drain(..n_bytes);

		// Flush complete bytes to inner writer.
		let inner = self.inner.as_mut().expect("inner writer already taken");
		let flushed = self.output.flush_to(inner)?;
		self.total_out += flushed as u64;

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
#[derive(Debug, Clone)]
pub struct ParBz2EncoderBuilder
{
	level: u8,
}

impl ParBz2EncoderBuilder
{
	/// Create a new builder with default settings (level 9).
	pub fn new() -> Self
	{
		Self { level: 9 }
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

	/// Build the encoder, writing compressed output to `inner`.
	///
	/// # Errors
	///
	/// Returns [`Bz2Error::InvalidFormat`] if the level is out of range.
	pub fn build<W: Write>(self, inner: W) -> Result<ParBz2Encoder<W>>
	{
		ParBz2Encoder::new(inner, self.level)
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
		// For multi-block data, the encoder should write some output
		// before finish() is called.
		let original = lcg_bytes(0xAAAA, 250_000);
		let mut output = Vec::new();
		let mut enc = ParBz2Encoder::new(&mut output, 1).unwrap();
		enc.write_all(&original).unwrap();

		// After write_all with 250KB at level 1 (99,981 bytes/block),
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
}
