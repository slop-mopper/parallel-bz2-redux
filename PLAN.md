# parallel-bz2-redux

A correct, high-performance parallel bzip2 compression and decompression
library in Rust.

## Motivation

The `parallel_bzip2_decoder` crate (v0.2.1) has a critical bug: when
the scanner encounters a false-positive block boundary match (the 48-bit
magic number `0x314159265359` appearing coincidentally in compressed
data), `try_for_each_init` aborts ALL remaining blocks, `let _ =`
discards the error, and the reader sees premature EOF.  On a 449 MB
simplewiki dump, 21% of decompressed data was silently lost.

This library fixes the problem at its root: **CRC-32 validation after
decompression** definitively distinguishes real blocks from false-positive
matches.  False positives are detected, the bit range is merged with the
next candidate, and decompression is retried.

---

## Architecture

### Decompression: Speculative Parallel + CRC Validation

```
[Scanner: 1MB chunks, Aho-Corasick, Rayon pool]
    | candidates: Vec<(bit_offset, MarkerType)>, reordered by position
    v
[Candidate pairing: consecutive candidates -> (start, end) block ranges]
    | all candidate ranges dispatched to Rayon
    v
[Speculative decompress: Rayon for_each on ALL ranges]
    | each block: decompress -> compute CRC -> compare with stored CRC
    | emit (block_idx, data, crc_valid) to result channel
    v
[Validator thread: sequential, receives results in order]
    | CRC valid -> emit to reader buffer
    | CRC invalid -> discard speculative result, merge with next range,
    |                re-decompress extended range (rare path)
    v
[Reader: HashMap<idx, Vec<u8>> reorder -> Read trait]
```

**Speculative parallel:** ALL candidate ranges are decompressed in
parallel.  Most are valid (~1 false positive per 2^48 bits of data).
When the validator detects a CRC mismatch, it discards the speculative
result, merges the bit ranges, and re-decompresses the merged range
sequentially (rare path, single extra decompression per false positive).

### Compression: Parallel Block Compression

```
[Input: Read trait, split at blockSize * 100000 bytes]
    | chunks
    v
[Rayon pool: BZ2_bzCompress each chunk independently]
    | (block_idx, compressed_bits, block_crc)
    v
[Assembler: sequential]
    | Write stream header (BZh + level)
    | Concatenate compressed blocks in order (bit-level)
    | Compute stream CRC: combined = rol1(combined) ^ block_crc
    | Write EOS marker + stream CRC
    | Pad to byte boundary -> Write trait output
```

---

## Bzip2 Format Summary

```
STREAM:
  +--------------------------------------------------+
  | 'B' 'Z' 'h' level                               | 4 bytes (byte-aligned)
  +--------------------------------------------------+
  | BLOCK 1 (bit-aligned)                            |
  |   +-- magic: 0x314159265359            48 bits   |
  |   +-- block_crc                        32 bits   |
  |   +-- randomised                        1 bit    |
  |   +-- orig_ptr                         24 bits   |
  |   +-- symbol_map                    32-272 bits  |
  |   +-- num_huffman_groups                3 bits   |
  |   +-- num_selectors                    15 bits   |
  |   +-- selector_list                  variable    |
  |   +-- huffman_trees                  variable    |
  |   +-- compressed_data               variable    |
  +--------------------------------------------------+
  | BLOCK 2 ... BLOCK N (bit-aligned)                |
  +--------------------------------------------------+
  | EOS:                                             |
  |   +-- magic: 0x177245385090            48 bits   |
  |   +-- stream_crc                       32 bits   |
  |   +-- padding                        0-7 bits   |
  +--------------------------------------------------+
```

Key properties:
- All block boundaries are **bit-aligned** (not byte-aligned)
- Block CRC covers **original uncompressed data** (must decompress to validate)
- Stream CRC = `combined = rol1(combined) ^ block_crc` for each block in order
- CRC uses MSB-first polynomial 0x04C11DB7 (not the zlib/Ethernet reflected variant)
- Blocks are fully independent (no inter-block state)
- The 48-bit magic can appear in compressed data (~2^-48 per bit position)

---

## Crate Structure

```
parallel-bz2-redux/
+-- Cargo.toml
+-- src/
|   +-- lib.rs              # Public API, re-exports
|   +-- error.rs            # Error types (thiserror)
|   +-- crc.rs              # bzip2 CRC-32 (MSB-first, poly 0x04C11DB7)
|   +-- bits.rs             # Bit extraction/insertion utilities
|   +-- scanner.rs          # Block boundary scanner (Aho-Corasick, Rayon)
|   +-- decompress/
|   |   +-- mod.rs          # ParBz2Decoder: Read impl, public API
|   |   +-- block.rs        # Single-block decompress + CRC validation
|   |   +-- pipeline.rs     # Speculative parallel pipeline + validator
|   +-- compress/
|       +-- mod.rs          # ParBz2Encoder: Write impl, public API
|       +-- block.rs        # Single-block compress via libbzip2 BZ_FLUSH
|       +-- pipeline.rs     # Input splitter + Rayon pool + bit assembler
+-- tests/
    +-- roundtrip.rs        # Compress -> decompress -> compare (0B to 1GB)
    +-- compat_decompress.rs # Compare output with bzcat on real bz2 files
    +-- compat_compress.rs   # Verify our output is decompressible by bzip2
    +-- false_positive.rs    # Synthetic test with injected false magic bytes
    +-- edge_cases.rs        # Empty, single-byte, truncated, multi-stream
    +-- fixtures/            # Real bz2 files for testing
```

---

## Public API

```rust
// ---- Decompression ----
use parallel_bz2_redux::ParBz2Decoder;

// Simple: open file with mmap
let mut decoder = ParBz2Decoder::open("file.bz2")?;
let mut output = Vec::new();
decoder.read_to_end(&mut output)?;

// From bytes
let mut decoder = ParBz2Decoder::from_bytes(Arc::new(data))?;

// Builder with options
let decoder = ParBz2Decoder::builder()
    .threads(4)
    .verify_stream_crc(true)
    .on_progress(|p| { /* ... */ })
    .open("file.bz2")?;

// ---- Compression ----
use parallel_bz2_redux::ParBz2Encoder;

let mut encoder = ParBz2Encoder::new(output_writer, 9)?;  // level 1-9
encoder.write_all(&input_data)?;
encoder.finish()?;

// Builder
let encoder = ParBz2Encoder::builder()
    .level(9)
    .threads(4)
    .on_progress(|p| { /* ... */ })
    .build(output_writer)?;

// ---- Progress ----
pub struct Progress {
    pub blocks_completed: u64,
    pub blocks_total: Option<u64>,  // known after scan/split
    pub bytes_in: u64,              // compressed bytes (decompress) or uncompressed input (compress)
    pub bytes_out: u64,             // decompressed bytes (decompress) or compressed output (compress)
}
```

---

## Dependencies

```toml
[dependencies]
bzip2 = "0.5"               # libbzip2 FFI for per-block compress/decompress
rayon = "1.10"               # Work-stealing thread pool
crossbeam-channel = "0.5"    # Bounded MPMC channels
memmap2 = "0.9"              # Memory-mapped I/O for large files
aho-corasick = "1"           # Fast multi-pattern bit-shifted magic scanning
thiserror = "2"              # Error types
```

---

## Implementation Phases

| Phase | What                                                  | Est. |
|-------|-------------------------------------------------------|------|
| 1     | Project scaffold, `error.rs`, `crc.rs`, `bits.rs`    | 2h   |
| 2     | `scanner.rs` -- parallel block boundary scanning      | 3h   |
| 3     | `decompress/block.rs` -- single-block + CRC validate  | 2h   |
| 4     | `decompress/pipeline.rs` -- speculative parallel      | 4h   |
| 5     | `decompress/mod.rs` -- `ParBz2Decoder`, Read impl     | 2h   |
| 6     | Decompress tests: roundtrip, compat, false-positive   | 3h   |
| 7     | `compress/block.rs` -- single-block via libbzip2      | 2h   |
| 8     | `compress/pipeline.rs` -- splitter + Rayon + assemble | 4h   |
| 9     | `compress/mod.rs` -- `ParBz2Encoder`, Write impl      | 2h   |
| 10    | Compress tests + full roundtrip suite                 | 2h   |
|       | **Total**                                             | ~26h |

---

## Key Design Decisions

1. **FFI to libbzip2** for actual BWT/Huffman codec (via `bzip2` crate).
   Reimplementing from scratch would be a separate multi-week project.

2. **CRC validation after decompression** to reject false-positive block
   boundaries.  The bzip2 block CRC covers uncompressed data, so there
   is no way to validate without decompressing first.

3. **Speculative parallel decompression** -- all candidate ranges are
   decompressed in parallel.  On the rare occasion that a false positive
   is detected (CRC mismatch), the speculative result is discarded, the
   bit range is merged with the next candidate, and decompression is
   retried.  This wastes one extra decompression per false positive
   (~1 per 2^48 bits = negligible) but keeps the common path fully
   parallel.

4. **Merge + retry** for false positives rather than skip.  This recovers
   the ~900KB of data that would otherwise be lost at the false-positive
   boundary.

5. **Rayon thread pool** for CPU-bound block decompression/compression.
   Each block is ~900KB of uncompressed data, so the work per task is
   substantial enough to amortise scheduling overhead.

6. **Progress callbacks** via `on_progress(FnMut(Progress))` so callers
   can drive progress bars or log import progress.

7. **No artificial block size limit**.  The old crate had a 2MB
   `MAX_BLOCK_SIZE` that would abort on legitimate blocks.  bzip2 level 9
   blocks decompress to at most ~900KB; there is no need for a limit.

---

## Test Strategy

1. **Roundtrip**: Random data at 0B, 1B, 100KB, 10MB, 100MB, 1GB.
   Compress with our encoder, decompress with our decoder, compare
   byte-for-byte.  The 1GB test is gated behind `PARALLEL_BZ2_LARGE_TESTS=1`.

2. **Decompress compat**: Decompress files produced by standard `bzip2`
   and compare with `bzcat` output (SHA-256).

3. **Compress compat**: Compress data with our encoder, decompress with
   standard `bzip2`, verify identical output.

4. **False-positive injection**: Craft a bz2 file with the 48-bit magic
   pattern embedded in compressed data.  Verify our decoder still
   produces correct output.

5. **Edge cases**: Empty file, single byte, truncated stream,
   multi-stream concatenation, all block sizes (1-9).

6. **Large-file regression**: Decompress the 449MB simplewiki dump and
   compare with `bzcat` output (the exact test case that exposed the
   original bug).
