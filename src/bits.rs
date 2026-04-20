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
			let available = data.len().saturating_sub(start_byte);
			out.extend_from_slice(&data[start_byte..start_byte + available]);
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

#[cfg(test)]
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
}
