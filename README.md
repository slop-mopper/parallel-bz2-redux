# parallel-bz2-redux

> **AI-generated code.** This crate was written almost entirely by an AI
> assistant (Claude claude-opus-4-6). Human involvement was limited to directing
> the design, reviewing output, and testing. Treat the code with the same
> scrutiny you would give any unaudited dependency.

Correct, high-performance parallel bzip2 compression and decompression for Rust.

Uses [Rayon](https://docs.rs/rayon) to compress and decompress bzip2 blocks in
parallel while producing output that is fully compatible with the standard bzip2
format.  The decompressor handles concatenated multi-stream files and performs
CRC verification at both the block and stream level.

## Features

- **Parallel decompression** -- scans for block boundaries with Aho-Corasick,
  then decompresses blocks concurrently through a streaming pipeline with
  bounded memory usage.
- **Parallel compression** -- splits input into block-sized chunks, compresses
  them in parallel, and assembles a valid bzip2 stream.
- **Standard `Read`/`Write` traits** -- `ParBz2Decoder` implements `Read`;
  `ParBz2Encoder` implements `Write`.  Drop-in replacements for serial
  decoders/encoders.
- **Multi-stream support** -- concatenated bzip2 streams are decompressed
  sequentially, each through its own parallel pipeline.
- **False-positive resilience** -- the decompression pipeline automatically
  detects block-boundary false positives and resolves them via merge-and-retry.
- **Custom thread pools** -- supply your own `rayon::ThreadPool` via the
  builder API to control thread count and isolate work from the global pool.
- **Stream CRC verification** -- enabled by default; can be disabled via the
  builder.

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
parallel-bz2-redux = "0.1"
```

### Decompression

```rust
use std::io::Read;
use parallel_bz2_redux::ParBz2Decoder;

// From a file:
let mut decoder = ParBz2Decoder::open("data.bz2")?;
let mut output = Vec::new();
decoder.read_to_end(&mut output)?;
# Ok::<(), parallel_bz2_redux::error::Bz2Error>(())
```

```rust
use std::io::Read;
use std::sync::Arc;
use parallel_bz2_redux::ParBz2Decoder;

// From in-memory data:
let compressed: Vec<u8> = std::fs::read("data.bz2")?;
let mut decoder = ParBz2Decoder::from_bytes(Arc::from(compressed))?;
let mut output = Vec::new();
decoder.read_to_end(&mut output)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Compression

```rust
use std::io::Write;
use parallel_bz2_redux::ParBz2Encoder;

let mut compressed = Vec::new();
{
    let mut encoder = ParBz2Encoder::new(&mut compressed, 9)?;
    encoder.write_all(b"Hello, parallel bzip2!")?;
    encoder.finish()?;
}
assert_eq!(&compressed[..3], b"BZh");
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Builder API

Both encoder and decoder expose builders for advanced configuration:

```rust
use std::io::{Read, Write};
use std::sync::Arc;
use parallel_bz2_redux::{ParBz2Decoder, ParBz2DecoderBuilder, ParBz2Encoder, ParBz2EncoderBuilder};

// Custom Rayon thread pool
let pool = Arc::new(
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap(),
);

// Encoder with custom pool and compression level
let mut compressed = Vec::new();
{
    let mut encoder = ParBz2EncoderBuilder::new()
        .level(6)
        .thread_pool(Arc::clone(&pool))
        .build(&mut compressed)?;
    encoder.write_all(b"data to compress")?;
    encoder.finish()?;
}

// Decoder with custom pool, CRC verification disabled
let data: Arc<[u8]> = Arc::from(compressed);
let mut decoder = ParBz2DecoderBuilder::new()
    .thread_pool(pool)
    .verify_stream_crc(false)
    .from_bytes(data)?;
let mut output = Vec::new();
decoder.read_to_end(&mut output)?;
assert_eq!(&output, b"data to compress");
# Ok::<(), Box<dyn std::error::Error>>(())
```

## How it works

### Decompression

1. **Scan** -- An Aho-Corasick automaton finds all candidate block-magic and
   end-of-stream markers in the compressed data (parallelised over 1 MB
   chunks).
2. **Partition** -- Candidates are grouped into per-stream sets, each bounded
   by an EOS marker.
3. **Dispatch** -- Block ranges are decompressed in parallel via
   `rayon::scope_fifo`.  Each block is wrapped in a synthetic mini bzip2
   stream and decompressed through libbzip2.
4. **Validate** -- A validator thread reorders results, checks CRCs, and
   resolves false-positive boundaries by merging and retrying adjacent ranges.
5. **Stream** -- Validated blocks flow through a bounded channel to the `Read`
   implementation.  Only a small window of blocks is in memory at a time.

### Compression

1. **Split** -- Input is divided into chunks at the maximum block size for the
   chosen compression level.
2. **Compress** -- Chunks are compressed in parallel with `rayon::par_iter`.
   Each produces a single-block bzip2 stream; the raw block bits are extracted.
3. **Assemble** -- Block bits are concatenated into a single bzip2 stream with
   the correct header, end-of-stream marker, and combined stream CRC.

## Requirements

- **Rust nightly** (edition 2024).  A `rust-toolchain.toml` is included.
- Links against **libbzip2** via the `bzip2` crate (C library built
  automatically by `bzip2-sys`).

## License

This project is released under the [Unlicense](https://unlicense.org/). See [LICENSE.md](LICENSE.md) for the full text.
