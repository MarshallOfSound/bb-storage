# ZSTD CPU benchmarks

Measurements behind the ZSTD related branches. Everything here was
measured on one 4 core machine that also ran the client, so read the
numbers as relative, not as predictions for a production cluster.

## Branches

| Branch | Contents | Base |
|---|---|---|
| `claude/zstd-1-advertise-batch-compressors` | Advertise `supported_batch_update_compressors` | `main` |
| `claude/zstd-2-keep-frame` | `reuse_client_zstd_frames`: keep validated client frames | `main` |
| `claude/zstd-6a-skip-chunk-digest-verification` | `skip_chunk_digest_verification` on gRPC CS clients | `main` |
| `claude/zstd-6b-lazy-chunk-decompression` | Lazy decompression of chunks from trusted backends | 6a |
| `claude/zstd-extras` | Three further changes, see below | all of the above |
| `claude/bold-ramanujan-jfnr96` | All of the above, plus this directory | |

All options default to off. With none of them set, behaviour is
unchanged.

## Setup

- One frontend (TLS) and two shards (storage + storage-cms each) on
  localhost, with configuration mirroring the production layout
  (`harness/tools/config`). The frontend talks to the shards with
  `enableCompression: true`.
- `casbench` behaves like siso: `FindMissingBlobs` followed by
  `BatchUpdateBlobs` in 64 concurrent calls of up to 32 blobs,
  ByteStream for blobs of 2 MiB and up, and reads everything back,
  verifying digests. Like siso it only compresses batch uploads if
  `supported_batch_update_compressors` is advertised.
- Corpus: 4,827 distinct blobs, 247 MB. 4,819 blobs (207 MB) go through the
  batch RPCs: Go sources, compiled Go objects and shared libraries.
  8 binaries (40 MB) go through ByteStream. The "small" corpus is
  only the 4,186 source files (77.5 MB, 18.5 KB on average).
- Every cell runs on a fresh cluster, 7 times with fresh data. Numbers
  are medians. CPU is user + system time from `/proc`, for the
  frontend and for all four shard processes together.
- Raw output: `results/specs.txt` and `results/final.txt`.

## Results: CPU

CPU seconds for 207 MB through the batch RPCs, no link emulation.
"zstd client" means `-batch_compress=1024 -stream_compress`.

| Configuration | Upload frontend | Upload shards | Read frontend | Read shards | Wire up | Wire down |
|---|---|---|---|---|---|---|
| `main`, raw client | 2.95 | 3.38 | 1.37 | 1.26 | 208 MB | 207 MB |
| `main`, zstd client | 2.80 | 3.27 | 1.16 | 1.21 | 208 MB | 62 MB |
| spec 1, zstd client | 3.03 | 3.39 | 1.13 | 1.16 | 62 MB | 62 MB |
| + spec 2 on frontend | 1.20 | 3.02 | 1.12 | 1.15 | 62 MB | 62 MB |
| + spec 2 on shards | 1.34 | 1.69 | 1.13 | 1.42 | 62 MB | 62 MB |
| spec 6A only, zstd client | | | 1.05 | 1.24 | | 62 MB |
| spec 6A + 6B, zstd client | | | 0.45 | 1.06 | | 62 MB |
| all specs | 1.20 | 1.66 | 0.50 | 1.41 | 62 MB | 62 MB |
| all specs + extras | 1.26 | 1.63 | 0.42 | 1.34 | 62 MB | 62 MB |
| all specs + extras, `GOGC=400` | 1.17 | 1.31 | 0.43 | 1.20 | 62 MB | 62 MB |
| all specs + extras, raw client | 2.82 | 1.71 | 1.10 | 1.35 | 208 MB | 207 MB |

Frontend plus shards, upload plus read: 8.96 s on `main`, 4.65 s with
everything enabled, 4.11 s with `GOGC=400` on top.

ByteStream (40 MB, compressed-blobs/zstd): frontend read CPU goes from
0.17 s to 0.12 s (6A) to 0.04 s (6B). Uploads through ByteStream are
unaffected on the frontend (0.36 s); shards go from 0.63 s to 0.54 s
because they keep the frontend's frames.

Small corpus (77.5 MB):

| Configuration | Upload frontend | Upload shards | Read frontend | Read shards |
|---|---|---|---|---|
| `main`, raw client | 1.18 | 1.49 | 0.64 | 0.63 |
| `main`, zstd client | 1.21 | 1.44 | 0.55 | 0.60 |
| all specs + extras | 0.65 | 0.87 | 0.30 | 0.73 |
| all specs + extras, `GOGC=400` | 0.62 | 0.74 | 0.26 | 0.59 |

## Results: wall time over a link

Through `linkproxy`. The proxy competes for the same four cores, so
CPU numbers in these cells are inflated; see `results/final.txt`.

| Configuration | Upload, 40 ms / 300 Mbps | Read, 40 ms / 300 Mbps | Upload, 2 ms / 2 Gbps | Read, 2 ms / 2 Gbps |
|---|---|---|---|---|
| `main`, raw client | 6.47 s | 5.97 s | 1.98 s | 1.28 s |
| `main`, zstd client | 6.35 s | 2.09 s | 1.88 s | 0.91 s |
| all specs + extras | 2.33 s | 2.05 s | 1.35 s | 0.80 s |
| all specs + extras, client at SpeedFastest | 2.34 s | 2.17 s | 1.22 s | 0.76 s |

## What the profiles say

- On `main`, ZSTD encoding is 43% of frontend CPU and 49% of shard CPU
  during uploads. The frontend encodes every chunk for the hop to the
  shard, and the shard decodes it and encodes it again. Spec 2 on the
  shards removes the second encode even for clients that upload
  uncompressed data (last row of the first table).
- A faster encoder level is not the answer. On this corpus
  (`harness/zbench`) SpeedFastest costs 4.9 core-s/GB against 6.5 for
  the default, at a 7% worse ratio and slower decoding. Not encoding at
  all is what pays.
- Per byte costs measured here: decode 1.75 core-s/GB, SHA-256 0.75
  core-s/GB.
- Shard read CPU goes *up* by about 0.2 s once shards reuse frames.
  The stored frames are byte for byte identical in both cases. The
  difference is garbage collection: without an encoder holding on to
  scratch buffers the live heap of a shard drops from 138 MB to 19 MB,
  so with the default `GOGC=100` the collector runs 4.5 times as often
  for the same amount of allocation. `GOGC=400` more than recovers it.

## The extras

1. `BatchReadBlobs()` no longer copies single chunk blobs into a
   temporary buffer. Frontend read CPU 0.50 s to 0.42 s, shards 1.41 s
   to 1.34 s.
2. `zstd.Encode()` no longer zero initializes its result. Within noise
   here; it removes a `memclr` of every compressed chunk.
3. Lazily decompressed chunks (6B) are limited to data that is exactly
   one well formed frame, using the checks of spec 2. This is a
   hardening change, not an optimization: a frontend that skips
   verification never relays bytes that are not accounted for by the
   announced chunk size. No measurable cost.

## Security

- Digests are always computed over decompressed data, on every server
  that accepts an upload. Spec 2 does not change what can be stored,
  only which compressed representation of it is kept.
- A kept frame must be a single frame without dictionary, reserved
  bits, skippable frames or trailing bytes, with a window of at most
  8 MiB, a matching frame content size, blocks of at most 128 KiB, and
  a size of at most the data plus 1/256th plus 32 bytes. The last rule
  is looser than "no larger than the data": incompressible and tiny
  blobs are stored in raw blocks and are a few bytes larger than their
  contents, including the ones our own encoder emits. All 4,846 chunks
  of the corpus were reused; none were rejected.
- A checksum mismatch in a client frame is rejected at decode time.
  A test pins this, as reuse depends on it.
- Shards validate for themselves. Enabling reuse on a shard does not
  rely on a frontend having checked anything.
- 6A and 6B are for frontends only. The trust anchor is that a shard
  decompresses and rehashes every chunk it reads from local storage,
  which is untouched. 6B only goes lazy when the frame header announces
  the expected size; a frame that turns out to hold a different amount
  of data fails to decode.
- Residual risk of spec 2 on shards: a frame that our decoder and a
  client's decoder interpret differently. Clients that verify digests
  would see a failed read, not wrong data.

## Not done, with estimates

- Batching the frontend to shard hop. The frontend issues one unary
  RPC per chunk. Sending small corpus blobs one per RPC instead of 32
  per RPC costs the frontend an extra 60 to 100 microseconds of CPU
  per RPC. Once the per byte work is gone, RPC overhead towards the
  shards is roughly a third of what is left on the frontend. Doing
  this properly needs a batch operation on `BlobAccess`, as requests
  from different clients must not share credentials.
- Shards allocate and zero a buffer the size of the decompressed chunk
  on every read, only to hash it when the reader wants ZSTD. Verifying
  into a pooled buffer would save an estimated 10% of shard read CPU,
  but needs to know which encoding the reader wants.
- `GOGC` / `GOMEMLIMIT` on the storage pods is a configuration change
  and is worth doing regardless: 10 to 20% of shard CPU here.

## Reproducing

Bazel was not available where this was measured, so binaries were
built with plain `go build`, using a `go.work` that replaces the four
modules patched under `patches/` and mocks generated with
`harness/tools/genmocks.py`. With Bazel, build `//cmd/bb_storage` at
each revision instead.

```sh
# Layout: bin/{bb_storage-<variant>,casbench,linkproxy} next to harness/,
# harness/corpus.txt, and a self-signed certificate for 127.0.0.1 in
# harness/tls/tls.{crt,key}.
go run harness/zbench/main.go harness/corpus.txt
python3 harness/tools/bench.py harness/cells/final.json 7
```

`corpus.txt` holds one file path per line. Cells are described in
`harness/cells/*.json`: frontend and shard binary, environment
(`FE_REUSE`, `SHARD_REUSE`, `FE_SKIP_VERIFY`, `GOGC`), client flags and
optionally a link (`a`: 2 ms / 2 Gbps, `b`: 40 ms / 300 Mbps).
