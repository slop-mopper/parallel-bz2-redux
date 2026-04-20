// bits.rs
// SPDX-License-Identifier: CC0-1.0
// This file was created entirely or mostly by an AI tool: claude-opus-4-6

//! Bit-level extraction and insertion utilities for bzip2 bitstreams.
//!
//! bzip2 blocks are bit-aligned: a block can start at any bit position
//! within the byte stream.  These utilities handle extracting arbitrary
//! bit ranges and inserting bits into output buffers.

/// Extract a range of bits from a byte slice and append them to `out`.
///
/// `start_bit` and `end_bit` are zero-indexed from the MSB of the first byte.
/// Bit 0 is the MSB of `data[0]`, bit 7 is the LSB of `data[0]`, bit 8 is the
/// MSB of `data[1]`, etc.
///
/// The extracted bits are written MSB-first to `out`, padded with zero bits at
/// the end of the last byte if the range is not a multiple of 8 bits.
pub fn extract_bits(data: &[u8], start_bit: u64, end_bit: u64, out: &mut Vec<u8>)
{
	if end_bit <= start_bit {
		return;
	}

	let total_bits = end_bit - start_bit;
	let full_bytes = (total_bits / 8) as usize;
	let remaining_bits = (total_bits % 8) as u32;

	// Reserve space for the output.
	out.reserve(full_bytes + if remaining_bits > 0 { 1 } else { 0 });

	let start_byte = (start_bit / 8) as usize;
	let bit_offset = (start_bit % 8) as u32;

	if bit_offset == 0 {
		// Byte-aligned: fast path — direct copy.
		let end_byte = start_byte + full_bytes;
		if end_byte <= data.len() {
			out.extend_from_slice(&data[start_byte..end_byte]);
		} else {
			// Partial: copy what's available, zero-pad the rest.
			let clamped_start = start_byte.min(data.len());
			let available = (data.len() - clamped_start).min(full_bytes);
			out.extend_from_slice(&data[clamped_start..clamped_start + available]);
			out.resize(out.len() + full_bytes - available, 0);
		}
		if remaining_bits > 0 {
			let byte_idx = start_byte + full_bytes;
			if byte_idx < data.len() {
				// Mask off the bits we don't want.
				let mask = 0xFFu8 << (8 - remaining_bits);
				out.push(data[byte_idx] & mask);
			} else {
				out.push(0);
			}
		}
	} else {
		// Non-aligned: shift pairs of bytes.
		let shift = bit_offset;
		let inv_shift = 8 - shift;

		for i in 0..full_bytes {
			let byte_idx = start_byte + i;
			let hi = if byte_idx < data.len() { data[byte_idx] } else { 0 };
			let lo = if byte_idx + 1 < data.len() {
				data[byte_idx + 1]
			} else {
				0
			};
			out.push((hi << shift) | (lo >> inv_shift));
		}

		if remaining_bits > 0 {
			let byte_idx = start_byte + full_bytes;
			let hi = if byte_idx < data.len() { data[byte_idx] } else { 0 };
			let lo = if byte_idx + 1 < data.len() {
				data[byte_idx + 1]
			} else {
				0
			};
			let raw = (hi << shift) | (lo >> inv_shift);
			let mask = 0xFFu8 << (8 - remaining_bits);
			out.push(raw & mask);
		}
	}
}

/// Read a big-endian u32 starting at the given bit offset.
///
/// Used to extract the 32-bit block CRC located immediately after the
/// 48-bit block magic.
pub fn read_u32_at_bit(data: &[u8], bit_offset: u64) -> u32
{
	let mut buf = Vec::with_capacity(4);
	extract_bits(data, bit_offset, bit_offset + 32, &mut buf);
	if buf.len() >= 4 {
		u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]])
	} else {
		0
	}
}

/// Read a single byte (8 bits) starting at an arbitrary bit position.
///
/// Used internally by [`BitWriter::copy_bits_from`] for non-aligned copies.
fn read_byte_at_bit(src: &[u8], bit_pos: u64) -> u8
{
	let byte_idx = (bit_pos / 8) as usize;
	let bit_off = (bit_pos % 8) as u32;
	if bit_off == 0 {
		src.get(byte_idx).copied().unwrap_or(0)
	} else {
		let hi = src.get(byte_idx).copied().unwrap_or(0);
		let lo = src.get(byte_idx + 1).copied().unwrap_or(0);
		(hi << bit_off) | (lo >> (8 - bit_off))
	}
}

/// A bit-level buffer writer for constructing bzip2 bitstreams.
///
/// Bits are packed MSB-first (matching bzip2 convention): bit 0 of each
/// byte is the most significant bit.
///
/// # Example
///
/// ```
/// use parallel_bz2_redux::bits::BitWriter;
///
/// let mut w = BitWriter::new();
/// w.write_bytes(b"BZh9");             // 4 byte-aligned bytes
/// w.write_bits(0x314159265359, 48);   // 48-bit block magic
/// w.write_bits(0xDEADBEEF, 32);       // 32-bit CRC
/// w.pad_to_byte();
/// assert_eq!(w.bit_len(), 112);
/// assert_eq!(&w.into_bytes()[..4], b"BZh9");
/// ```
pub struct BitWriter
{
	buf: Vec<u8>,
	bit_len: u64,
}

impl Default for BitWriter
{
	fn default() -> Self
	{
		Self::new()
	}
}

impl BitWriter
{
	/// Create a new empty `BitWriter`.
	pub fn new() -> Self
	{
		BitWriter { buf: Vec::new(), bit_len: 0 }
	}

	/// Create a new `BitWriter` with pre-allocated capacity (in bytes).
	pub fn with_capacity(bytes: usize) -> Self
	{
		BitWriter { buf: Vec::with_capacity(bytes), bit_len: 0 }
	}

	/// Total number of bits written so far.
	pub fn bit_len(&self) -> u64
	{
		self.bit_len
	}

	/// Borrow the underlying byte buffer.
	///
	/// If the total bit count is not a multiple of 8, the final byte
	/// contains zero-padded trailing bits.
	pub fn as_bytes(&self) -> &[u8]
	{
		&self.buf
	}

	/// Consume the writer and return the byte buffer.
	pub fn into_bytes(self) -> Vec<u8>
	{
		self.buf
	}

	/// Write complete bytes to the buffer.
	///
	/// If the current position is byte-aligned this is a fast memcpy;
	/// otherwise each byte is shifted into place across the bit boundary.
	pub fn write_bytes(&mut self, data: &[u8])
	{
		if data.is_empty() {
			return;
		}

		let bit_off = (self.bit_len % 8) as u32;
		if bit_off == 0 {
			// Byte-aligned fast path.
			self.buf.extend_from_slice(data);
			self.bit_len += (data.len() as u64) * 8;
		} else {
			// Non-aligned: shift each byte across the boundary.
			let shift = bit_off;
			let inv_shift = 8 - shift;

			for &b in data {
				let byte_pos = (self.bit_len / 8) as usize;
				if byte_pos >= self.buf.len() {
					self.buf.push(0);
				}
				self.buf[byte_pos] |= b >> shift;
				if byte_pos + 1 >= self.buf.len() {
					self.buf.push(0);
				}
				self.buf[byte_pos + 1] |= b << inv_shift;
				self.bit_len += 8;
			}
		}
	}

	/// Write `n` bits from the least-significant end of `value`, MSB-first.
	///
	/// For example, `write_bits(0b101, 3)` writes bits 1, 0, 1 in order.
	/// `n` must be in the range `0..=64`.
	pub fn write_bits(&mut self, value: u64, n: u32)
	{
		if n == 0 {
			return;
		}
		assert!(n <= 64, "write_bits: n={n} exceeds 64");

		// Left-justify: the n bits we want sit at the top of `val`.
		let mask = if n < 64 { (1u64 << n) - 1 } else { u64::MAX };
		let mut val = (value & mask) << (64 - n);
		let mut remaining = n;

		while remaining > 0 {
			let bit_off = (self.bit_len % 8) as u32;
			let avail = 8 - bit_off;
			let chunk = remaining.min(avail);

			// Extract the top `chunk` bits from val.
			let bits = (val >> (64 - chunk)) as u8;
			val <<= chunk;

			// Write into the current byte.
			let byte_pos = (self.bit_len / 8) as usize;
			if byte_pos >= self.buf.len() {
				self.buf.push(0);
			}
			self.buf[byte_pos] |= bits << (avail - chunk);

			self.bit_len += chunk as u64;
			remaining -= chunk;
		}
	}

	/// Copy `num_bits` bits from `src` starting at `src_start_bit`.
	///
	/// Three internal paths, fastest first:
	/// 1. Both source and dest byte-aligned → memcpy + tail.
	/// 2. Dest byte-aligned, source not → shifted read, direct push.
	/// 3. General → byte-at-a-time through [`write_bits`].
	pub fn copy_bits_from(&mut self, src: &[u8], src_start_bit: u64, num_bits: u64)
	{
		if num_bits == 0 {
			return;
		}

		let src_off = (src_start_bit % 8) as u32;
		let dst_off = (self.bit_len % 8) as u32;
		let full_bytes = (num_bits / 8) as usize;
		let tail_bits = (num_bits % 8) as u32;

		if src_off == 0 && dst_off == 0 {
			// Path 1: both byte-aligned — fast memcpy.
			let src_byte = (src_start_bit / 8) as usize;
			let end = (src_byte + full_bytes).min(src.len());
			self.buf.extend_from_slice(&src[src_byte..end]);
			let copied = end - src_byte;
			if copied < full_bytes {
				self.buf.resize(self.buf.len() + (full_bytes - copied), 0);
			}
			self.bit_len += (full_bytes as u64) * 8;

			if tail_bits > 0 {
				let idx = src_byte + full_bytes;
				let b = src.get(idx).copied().unwrap_or(0);
				let mask = 0xFFu8 << (8 - tail_bits);
				self.buf.push(b & mask);
				self.bit_len += tail_bits as u64;
			}
		} else if dst_off == 0 {
			// Path 2: dest byte-aligned, source not — shifted reads.
			self.buf.reserve(full_bytes + if tail_bits > 0 { 1 } else { 0 });
			let mut src_pos = src_start_bit;
			for _ in 0..full_bytes {
				self.buf.push(read_byte_at_bit(src, src_pos));
				src_pos += 8;
			}
			self.bit_len += (full_bytes as u64) * 8;

			if tail_bits > 0 {
				let b = read_byte_at_bit(src, src_pos);
				let mask = 0xFFu8 << (8 - tail_bits);
				self.buf.push(b & mask);
				self.bit_len += tail_bits as u64;
			}
		} else {
			// Path 3: general non-aligned — write_bits byte-at-a-time.
			let needed = ((self.bit_len + num_bits + 7) / 8) as usize;
			if needed > self.buf.capacity() {
				self.buf.reserve(needed - self.buf.len());
			}

			let mut src_pos = src_start_bit;
			for _ in 0..full_bytes {
				let b = read_byte_at_bit(src, src_pos);
				self.write_bits(b as u64, 8);
				src_pos += 8;
			}
			if tail_bits > 0 {
				let b = read_byte_at_bit(src, src_pos);
				// Take top tail_bits from the byte, right-justified for write_bits.
				let val = (b >> (8 - tail_bits)) as u64;
				self.write_bits(val, tail_bits);
			}
		}
	}

	/// Pad with zero bits to the next byte boundary.
	///
	/// No-op if already byte-aligned.
	pub fn pad_to_byte(&mut self)
	{
		let remainder = (self.bit_len % 8) as u32;
		if remainder != 0 {
			// Trailing bits are already zero (we always zero-init new bytes),
			// so just advance the bit count.
			self.bit_len += (8 - remainder) as u64;
		}
	}
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests
{
	use super::*;

	#[test]
	fn test_extract_bits_aligned()
	{
		let data = [0xAB, 0xCD, 0xEF, 0x01];
		let mut out = Vec::new();
		extract_bits(&data, 0, 16, &mut out);
		assert_eq!(out, [0xAB, 0xCD]);
	}

	#[test]
	fn test_extract_bits_shifted()
	{
		// Extract bits 4..20 (16 bits, offset by 4 from byte boundary).
		let data = [0xAB, 0xCD, 0xEF];
		let mut out = Vec::new();
		extract_bits(&data, 4, 20, &mut out);
		// data in binary: 1010_1011 1100_1101 1110_1111
		// bits 4..20:     1011 1100_1101 1110
		// = 0xBC 0xDE
		assert_eq!(out, [0xBC, 0xDE]);
	}

	#[test]
	fn test_extract_bits_partial_byte()
	{
		// Extract 5 bits starting at bit 0.
		let data = [0b1101_0110];
		let mut out = Vec::new();
		extract_bits(&data, 0, 5, &mut out);
		// Expect: 1101_0 followed by 000 padding = 0b1101_0000 = 0xD0
		assert_eq!(out, [0xD0]);
	}

	#[test]
	fn test_extract_bits_empty_range()
	{
		let data = [0xFF];
		let mut out = Vec::new();
		extract_bits(&data, 5, 5, &mut out);
		assert!(out.is_empty());

		extract_bits(&data, 5, 3, &mut out);
		assert!(out.is_empty());
	}

	#[test]
	fn test_extract_bits_full_copy()
	{
		let data = [0x01, 0x02, 0x03, 0x04];
		let mut out = Vec::new();
		extract_bits(&data, 0, 32, &mut out);
		assert_eq!(out, data);
	}

	#[test]
	fn test_read_u32_at_bit_aligned()
	{
		let data = [0x12, 0x34, 0x56, 0x78, 0x9A];
		assert_eq!(read_u32_at_bit(&data, 0), 0x12345678);
		assert_eq!(read_u32_at_bit(&data, 8), 0x3456789A);
	}

	#[test]
	fn test_read_u32_at_bit_shifted()
	{
		let data = [0x12, 0x34, 0x56, 0x78, 0x9A];
		let val = read_u32_at_bit(&data, 4);
		// bits 4..36 of 0x123456789A:
		// 0001_0010 0011_0100 0101_0110 0111_1000 1001_1010
		// starting at bit 4: 0010 0011_0100 0101_0110 0111_1000 1
		// = 0x23456789 (top 32 bits of the shifted view)
		assert_eq!(val, 0x2345_6789);
	}

	// ── BitWriter tests ──────────────────────────────────────────────

	#[test]
	fn test_bitwriter_aligned_bytes()
	{
		let mut w = BitWriter::new();
		w.write_bytes(b"BZh9");
		assert_eq!(w.bit_len(), 32);
		assert_eq!(w.as_bytes(), b"BZh9");
	}

	#[test]
	fn test_bitwriter_bits_within_byte()
	{
		let mut w = BitWriter::new();
		w.write_bits(0b101, 3);
		assert_eq!(w.bit_len(), 3);
		assert_eq!(w.as_bytes(), &[0b1010_0000]);

		w.write_bits(0b11, 2);
		assert_eq!(w.bit_len(), 5);
		assert_eq!(w.as_bytes(), &[0b1011_1000]);
	}

	#[test]
	fn test_bitwriter_cross_byte_bits()
	{
		let mut w = BitWriter::new();
		w.write_bits(0b101, 3);
		w.write_bits(0xFF, 8);
		assert_eq!(w.bit_len(), 11);
		// bit stream: 101_11111111
		// bytes: 10111111 11100000
		assert_eq!(w.as_bytes(), &[0xBF, 0xE0]);
	}

	#[test]
	fn test_bitwriter_write_bytes_nonaligned()
	{
		let mut w = BitWriter::new();
		w.write_bits(0b101, 3);
		w.write_bytes(&[0xAB]);
		assert_eq!(w.bit_len(), 11);
		// 101 + 10101011 → 10110101 01100000
		assert_eq!(w.as_bytes(), &[0xB5, 0x60]);
	}

	#[test]
	fn test_bitwriter_copy_bits_aligned()
	{
		let src = [0xAB, 0xCD, 0xEF];
		let mut w = BitWriter::new();
		w.copy_bits_from(&src, 0, 24);
		assert_eq!(w.as_bytes(), &[0xAB, 0xCD, 0xEF]);
		assert_eq!(w.bit_len(), 24);
	}

	#[test]
	fn test_bitwriter_copy_bits_shifted_src()
	{
		// Dest byte-aligned, source not (path 2).
		let src = [0xAB, 0xCD, 0xEF];
		let mut w = BitWriter::new();
		w.copy_bits_from(&src, 4, 16);
		// src bits 4..20: same as extract_bits test → 0xBC 0xDE
		assert_eq!(w.as_bytes(), &[0xBC, 0xDE]);
		assert_eq!(w.bit_len(), 16);
	}

	#[test]
	fn test_bitwriter_copy_bits_partial()
	{
		let src = [0b1101_0110];
		let mut w = BitWriter::new();
		w.copy_bits_from(&src, 0, 5);
		assert_eq!(w.bit_len(), 5);
		// Top 5 bits: 11010 → 11010_000 = 0xD0
		assert_eq!(w.as_bytes(), &[0xD0]);
	}

	#[test]
	fn test_bitwriter_copy_bits_dst_nonaligned()
	{
		// Both src and dst non-aligned (path 3).
		let src = [0xAB, 0xCD];
		let mut w = BitWriter::new();
		w.write_bits(0b101, 3);
		w.copy_bits_from(&src, 0, 16);
		assert_eq!(w.bit_len(), 19);
		// 101 + 10101011_11001101
		// = 10110101 01111001 10100000
		assert_eq!(w.as_bytes(), &[0xB5, 0x79, 0xA0]);
	}

	#[test]
	fn test_bitwriter_mixed_synthetic_stream()
	{
		let mut w = BitWriter::new();
		// Header (byte-aligned).
		w.write_bytes(b"BZh9");
		assert_eq!(w.bit_len(), 32);

		// 48-bit block magic.
		w.write_bits(0x314159265359, 48);
		assert_eq!(w.bit_len(), 80);

		// Copy 20 bits from a source buffer.
		let src = [0xFF, 0x00, 0xAA];
		w.copy_bits_from(&src, 0, 20);
		assert_eq!(w.bit_len(), 100);

		// Verify header intact.
		assert_eq!(&w.as_bytes()[0..4], b"BZh9");

		// Verify magic: 0x314159265359.
		assert_eq!(&w.as_bytes()[4..10], &[0x31, 0x41, 0x59, 0x26, 0x53, 0x59]);

		// 20 bits of src: 0xFF 0x00 then top 4 bits of 0xAA (1010) + padding.
		assert_eq!(&w.as_bytes()[10..13], &[0xFF, 0x00, 0xA0]);
	}

	#[test]
	fn test_bitwriter_pad()
	{
		let mut w = BitWriter::new();
		w.write_bits(0b101, 3);
		assert_eq!(w.bit_len(), 3);

		w.pad_to_byte();
		assert_eq!(w.bit_len(), 8);
		assert_eq!(w.as_bytes(), &[0b1010_0000]);

		// Already aligned — no-op.
		w.pad_to_byte();
		assert_eq!(w.bit_len(), 8);
		assert_eq!(w.as_bytes().len(), 1);
	}

	#[test]
	fn test_bitwriter_48bit_value()
	{
		let mut w = BitWriter::new();
		w.write_bits(0x177245385090, 48);
		assert_eq!(w.bit_len(), 48);
		assert_eq!(w.as_bytes(), &[0x17, 0x72, 0x45, 0x38, 0x50, 0x90]);
	}

	#[test]
	fn test_bitwriter_32bit_value()
	{
		let mut w = BitWriter::new();
		w.write_bits(0xDEADBEEF, 32);
		assert_eq!(w.bit_len(), 32);
		assert_eq!(w.as_bytes(), &[0xDE, 0xAD, 0xBE, 0xEF]);
	}

	// ── Coverage gap tests ─────────────────────────────────────────

	#[test]
	fn test_extract_bits_aligned_past_data_end()
	{
		// Data shorter than the requested range — triggers zero-pad path (lines 41-44).
		let data = [0xAB, 0xCD];
		let mut out = Vec::new();
		// Request 4 bytes (32 bits) from a 2-byte buffer.
		extract_bits(&data, 0, 32, &mut out);
		assert_eq!(out, [0xAB, 0xCD, 0x00, 0x00]);
	}

	#[test]
	fn test_extract_bits_aligned_remaining_past_end()
	{
		// Byte-aligned, remaining bits past data end (line 53).
		let data = [0xFF];
		let mut out = Vec::new();
		// Request 12 bits (1 full byte + 4 remaining) starting at bit 8 (past end).
		extract_bits(&data, 8, 20, &mut out);
		assert_eq!(out, [0x00, 0x00]);
	}

	#[test]
	fn test_extract_bits_shifted_past_data_end()
	{
		// Shifted path with data shorter than range (lines 67, 73-82).
		let data = [0xAB];
		let mut out = Vec::new();
		// Request bits 4..28 (24 bits, shifted) from 1-byte buffer.
		extract_bits(&data, 4, 28, &mut out);
		// From bit 4: first byte = (0xAB << 4) | (0 >> 4) = 0xB0
		// second byte = 0, third byte partial = 0
		assert_eq!(out, [0xB0, 0x00, 0x00]);
	}

	#[test]
	fn test_extract_bits_shifted_remaining_past_end()
	{
		// Shifted, remaining bits with hi/lo both past data (lines 73-82).
		let data = [0xFF, 0xAA];
		let mut out = Vec::new();
		// Request bits 4..36 (32 bits, shifted, 4 full bytes) from 2-byte buffer.
		// Bytes 2,3,4 will be past data end.
		extract_bits(&data, 4, 36, &mut out);
		assert_eq!(out.len(), 4);
		// First byte: (0xFF << 4) | (0xAA >> 4) = 0xFA
		assert_eq!(out[0], 0xFA);
		// Second byte: (0xAA << 4) | (0 >> 4) = 0xA0
		assert_eq!(out[1], 0xA0);
		// Remaining: zeros
		assert_eq!(out[2], 0x00);
		assert_eq!(out[3], 0x00);
	}

	#[test]
	fn test_read_u32_at_bit_short_buffer()
	{
		// Buffer shorter than 32 bits — extract_bits zero-pads, so we
		// get partial data in the high bytes.
		let data = [0xAB, 0xCD];
		// Reading 32 bits from bit 8: gets 0xCD from data[1], rest zero-padded.
		assert_eq!(read_u32_at_bit(&data, 8), 0xCD00_0000);
	}

	#[test]
	fn test_read_u32_at_bit_empty()
	{
		// Empty buffer — zero-padded to 4 bytes = 0.
		assert_eq!(read_u32_at_bit(&[], 0), 0);
	}

	#[test]
	fn test_read_u32_at_bit_past_end()
	{
		// Start beyond buffer end — all zero-pad.
		let data = [0xFF; 4];
		assert_eq!(read_u32_at_bit(&data, 64), 0);
	}

	#[test]
	fn test_bitwriter_default()
	{
		// Exercise the Default impl (lines 144-147).
		let w: BitWriter = Default::default();
		assert_eq!(w.bit_len(), 0);
		assert!(w.as_bytes().is_empty());
	}

	#[test]
	fn test_bitwriter_write_bytes_empty()
	{
		// Empty write_bytes early return (line 192).
		let mut w = BitWriter::new();
		w.write_bytes(&[]);
		assert_eq!(w.bit_len(), 0);
		assert!(w.as_bytes().is_empty());
	}

	#[test]
	fn test_bitwriter_write_bits_zero()
	{
		// write_bits with n=0 early return (line 227).
		let mut w = BitWriter::new();
		w.write_bits(0xFFFF, 0);
		assert_eq!(w.bit_len(), 0);
		assert!(w.as_bytes().is_empty());
	}

	#[test]
	fn test_bitwriter_copy_bits_zero()
	{
		// copy_bits_from with num_bits=0 early return (line 266).
		let src = [0xFF, 0xAA];
		let mut w = BitWriter::new();
		w.copy_bits_from(&src, 0, 0);
		assert_eq!(w.bit_len(), 0);
		assert!(w.as_bytes().is_empty());
	}

	#[test]
	fn test_bitwriter_copy_bits_aligned_short_src()
	{
		// Path 1 (both aligned), source shorter than full_bytes → zero-pad (line 281).
		let src = [0xAB];
		let mut w = BitWriter::new();
		// Copy 24 bits from a 1-byte source (both aligned).
		w.copy_bits_from(&src, 0, 24);
		assert_eq!(w.bit_len(), 24);
		assert_eq!(w.as_bytes(), &[0xAB, 0x00, 0x00]);
	}

	#[test]
	fn test_bitwriter_copy_bits_path3_with_tail()
	{
		// Path 3 (both non-aligned) with tail bits (lines 321-325).
		let src = [0xAB, 0xCD, 0xEF];
		let mut w = BitWriter::new();
		w.write_bits(0b101, 3); // Make dst non-aligned
		// Copy 13 bits (1 full byte + 5 tail bits) from bit 4 of src (src non-aligned too).
		w.copy_bits_from(&src, 4, 13);
		assert_eq!(w.bit_len(), 16); // 3 + 13 = 16

		// Stream: 101 + 13 bits from src starting at bit 4
		// src bits 4..17: from 0xAB=10101011 0xCD=11001101 0xEF
		//   bit4: 1011 11001101 1 (13 bits) = BCDE shifted... let me compute carefully
		// src bit 4..12 (8 bits): read_byte_at_bit(src, 4) = (0xAB<<4)|(0xCD>>4) = 0xBC
		// src bit 12..17 (5 bits): read_byte_at_bit(src, 12) = (0xCD<<4)|(0xEF>>4) = 0xDE
		//   top 5 bits of 0xDE = 11011 → right-justified = 0b11011 = 27
		//   write_bits(27, 5)
		// So stream: 101 + 10111100 + 11011 = 10110111100_11011 = 16 bits
		// bytes: 10110111 10011011 = 0xB7 0x9B
		assert_eq!(w.as_bytes(), &[0xB7, 0x9B]);
	}

	#[test]
	fn test_bitwriter_copy_bits_path3_full_bytes_only()
	{
		// Path 3 (both non-aligned) with only full bytes, no tail (line 312 reserve path).
		let src = [0xFF, 0x00];
		let mut w = BitWriter::new();
		w.write_bits(0b1, 1); // 1 bit → dst non-aligned
		// Copy exactly 8 bits from aligned source (but dst is non-aligned → path 3).
		w.copy_bits_from(&src, 0, 8);
		assert_eq!(w.bit_len(), 9);
		// Stream: 1 + 11111111 = 1_11111111 = 0xFF, 0x80
		assert_eq!(w.as_bytes(), &[0xFF, 0x80]);
	}

	#[test]
	fn test_bitwriter_write_bits_64()
	{
		// Write all 64 bits.
		let mut w = BitWriter::new();
		w.write_bits(u64::MAX, 64);
		assert_eq!(w.bit_len(), 64);
		assert_eq!(w.as_bytes(), &[0xFF; 8]);
	}

	#[test]
	fn test_bitwriter_with_capacity()
	{
		let w = BitWriter::with_capacity(128);
		assert_eq!(w.bit_len(), 0);
		assert!(w.as_bytes().is_empty());
	}

	#[test]
	fn test_extract_bits_shifted_remaining_past_end_nonzero()
	{
		// Shifted path where remaining_bits > 0 and bytes are past data end (lines 74-83).
		let data = [0xAB];
		let mut out = Vec::new();
		// Request bits 3..14 (11 bits, shifted): full_bytes=1, remaining_bits=3.
		// start_byte=0, shift=3.
		// full byte 0: hi=0xAB, lo=0 → (0xAB<<3)|(0>>5) = 0x58 (01011000... wait)
		// 0xAB = 10101011. 0xAB << 3 = 01011000 = 0x58 (with wraparound). Actually:
		// (0xAB << 3) as u8 = (10101011 << 3) & 0xFF = 01011000 = 0x58
		// lo=0 so 0x58 | 0 = 0x58
		// remaining byte: byte_idx=0+1=1, past data end: hi=0, lo=0
		// raw=0, mask=0xE0, push 0
		extract_bits(&data, 3, 14, &mut out);
		assert_eq!(out.len(), 2);
		assert_eq!(out[0], 0x58);
		assert_eq!(out[1], 0x00);
	}

	#[test]
	fn test_extract_bits_shifted_remaining_within_data()
	{
		// Shifted path with remaining_bits > 0, bytes available.
		let data = [0xAB, 0xCD, 0xEF];
		let mut out = Vec::new();
		// Request bits 2..13 (11 bits, shifted): full_bytes=1, remaining_bits=3.
		// start_byte=0, shift=2.
		// full byte 0: hi=0xAB, lo=0xCD → (0xAB<<2)|(0xCD>>6) = 0xAF
		// remaining: byte_idx=1, hi=0xCD, lo=0xEF → (0xCD<<2)|(0xEF>>6) = 0x37
		// mask = 0xE0, pushed = 0x37 & 0xE0 = 0x20
		extract_bits(&data, 2, 13, &mut out);
		assert_eq!(out.len(), 2);
		assert_eq!(out[0], 0xAF);
		assert_eq!(out[1], 0x20);
	}
}
