package chunk_test

import (
	"bytes"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestChunkEncodedRoundTrip(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		require.Equal(t, []byte("hello world"), util.Must(c.GetBytes()))
		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_IDENTITY)
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), encoded)
	})

	t.Run("ZSTD", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		decoded, err := zstd.Decode(encoded, len("hello world"))
		require.NoError(t, err)
		require.Equal(t, []byte("hello world"), decoded)
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		c := chunk.NewChunk([]byte("hello world"))

		_, err := c.GetEncodedBytes(remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

func TestChunkFromEncoded(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		c, err := chunk.NewChunkFromEncoded([]byte("test data"), 100, remoteexecution.Compressor_IDENTITY)
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))
	})

	t.Run("IdentityBufferTooLarge", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("test data"), 4, remoteexecution.Compressor_IDENTITY)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("NegativeMaximumDecodedSize", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("test data"), -1, remoteexecution.Compressor_IDENTITY)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ZSTD", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		c, err := chunk.NewChunkFromEncoded(compressed, len([]byte("test data")), remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))

		// The chunk must hand back the exact bytes it was created
		// from, not a fresh encoding of the decoded data.
		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressed, encoded)
	})

	t.Run("ZSTDDecodeFailure", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("not valid zstd data"), 100, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("ZSTDBufferTooLarge", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		_, err := chunk.NewChunkFromEncoded(compressed, 4, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		_, err := chunk.NewChunkFromEncoded([]byte("test data"), 100, remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

func TestChunkFromUntrustedEncoded(t *testing.T) {
	t.Run("Identity", func(t *testing.T) {
		c, reused, err := chunk.NewChunkFromUntrustedEncoded([]byte("test data"), 100, remoteexecution.Compressor_IDENTITY)
		require.NoError(t, err)
		require.False(t, reused)
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))
	})

	t.Run("ZSTDReused", func(t *testing.T) {
		// A well formed frame is handed back as is.
		compressed := zstd.Encode([]byte("test data"))
		c, reused, err := chunk.NewChunkFromUntrustedEncoded(compressed, 100, remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.True(t, reused)
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))

		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressed, encoded)
	})

	t.Run("ZSTDNotReused", func(t *testing.T) {
		// Data that decompresses properly, but has trailing
		// data in the form of a skippable frame. The chunk must
		// be encoded again, so that the trailing data is not
		// handed out to others.
		compressed := append(zstd.Encode([]byte("test data")), 0x50, 0x2a, 0x4d, 0x18, 0x02, 0x00, 0x00, 0x00, 'H', 'i')
		c, reused, err := chunk.NewChunkFromUntrustedEncoded(compressed, 100, remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.False(t, reused)
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))

		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, zstd.Encode([]byte("test data")), encoded)
	})

	t.Run("ZSTDDecodeFailure", func(t *testing.T) {
		_, _, err := chunk.NewChunkFromUntrustedEncoded([]byte("not valid zstd data"), 100, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("ZSTDBufferTooLarge", func(t *testing.T) {
		compressed := zstd.Encode([]byte("test data"))
		_, _, err := chunk.NewChunkFromUntrustedEncoded(compressed, 4, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		_, _, err := chunk.NewChunkFromUntrustedEncoded([]byte("test data"), 100, remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

func TestChunkFromTrustedEncoded(t *testing.T) {
	data := bytes.Repeat([]byte("Hello"), 1000)
	md5 := digest.MustNewFunction("", remoteexecution.DigestFunction_MD5)

	t.Run("Identity", func(t *testing.T) {
		c, err := chunk.NewChunkFromTrustedEncoded([]byte("test data"), 9, remoteexecution.Compressor_IDENTITY)
		require.NoError(t, err)
		require.Equal(t, []byte("test data"), util.Must(c.GetBytes()))
	})

	t.Run("IdentitySizeMismatch", func(t *testing.T) {
		_, err := chunk.NewChunkFromTrustedEncoded([]byte("test data"), 10, remoteexecution.Compressor_IDENTITY)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Size mismatch, expected 10 bytes, got 9 bytes"), err)
		_, err = chunk.NewChunkFromTrustedEncoded([]byte("test data"), 8, remoteexecution.Compressor_IDENTITY)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ZSTD", func(t *testing.T) {
		compressed := zstd.Encode(data)
		c, err := chunk.NewChunkFromTrustedEncoded(compressed, len(data), remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)

		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressed, encoded)

		require.Equal(t, data, util.Must(c.GetBytes()))
		require.Equal(t, data, util.Must(c.GetEncodedBytes(remoteexecution.Compressor_IDENTITY)))
		require.Equal(t, util.Must(chunk.NewChunk(data).ComputeDigest(md5)), util.Must(c.ComputeDigest(md5)))
	})

	t.Run("ZSTDNotDecodedUnlessNeeded", func(t *testing.T) {
		// Corrupt the contents of the frame, leaving its header
		// intact. As requesting the chunk in compressed form
		// does not cause it to be decompressed, this goes by
		// unnoticed. Any attempt to access the uncompressed data
		// fails.
		compressed := zstd.Encode(data)
		for i := 12; i < len(compressed); i++ {
			compressed[i] ^= 0xff
		}
		c, err := chunk.NewChunkFromTrustedEncoded(compressed, len(data), remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)

		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressed, encoded)

		_, err = c.GetBytes()
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = c.GetEncodedBytes(remoteexecution.Compressor_IDENTITY)
		require.Equal(t, codes.Internal, status.Code(err))
		_, err = c.ComputeDigest(md5)
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("ZSTDConcurrentDecode", func(t *testing.T) {
		c, err := chunk.NewChunkFromTrustedEncoded(zstd.Encode(data), len(data), remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)

		// All callers must observe the very same slice.
		results := make([][]byte, 16)
		var wg sync.WaitGroup
		for i := range results {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i] = util.Must(c.GetBytes())
			}()
		}
		wg.Wait()
		for _, result := range results {
			require.Equal(t, data, result)
			require.Same(t, &results[0][0], &result[0])
		}
	})

	t.Run("ZSTDFrameContentSizeMismatch", func(t *testing.T) {
		// The size announced by the frame does not match the
		// size of the chunk. This is detected without
		// decompressing.
		compressed := zstd.Encode(data)
		_, err := chunk.NewChunkFromTrustedEncoded(compressed, len(data)+1, remoteexecution.Compressor_ZSTD)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Size mismatch, expected 5001 bytes, got 5000 bytes"), err)
		_, err = chunk.NewChunkFromTrustedEncoded(compressed, len(data)-1, remoteexecution.Compressor_ZSTD)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Size mismatch, expected 4999 bytes, got 5000 bytes"), err)
	})

	t.Run("ZSTDFrameContentSizeIncorrect", func(t *testing.T) {
		// A frame that announces the right size, but contains
		// fewer bytes. The single byte frame content size of a
		// single segment frame is patched from 4 to 5.
		compressed := []byte{0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x05, 0x21, 0x00, 0x00, 'H', 'e', 'l', 'l'}
		c, err := chunk.NewChunkFromTrustedEncoded(compressed, 5, remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		_, err = c.GetBytes()
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("ZSTDWithoutFrameContentSize", func(t *testing.T) {
		// Frames that don't announce their size are
		// decompressed immediately, so that the size can be
		// validated.
		compressed := zstd.Encode([]byte("Hello"))
		c, err := chunk.NewChunkFromTrustedEncoded(compressed, 5, remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), util.Must(c.GetBytes()))
		encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressed, encoded)

		_, err = chunk.NewChunkFromTrustedEncoded(compressed, 6, remoteexecution.Compressor_ZSTD)
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Size mismatch, expected 6 bytes, got 5 bytes"), err)
		_, err = chunk.NewChunkFromTrustedEncoded(compressed, 4, remoteexecution.Compressor_ZSTD)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ZSTDTrailingData", func(t *testing.T) {
		// Data that announces the right size, but carries a
		// skippable frame at the end. It is decompressed
		// immediately, as it must not be relayed without having
		// been looked at. A corrupted copy is therefore
		// rejected at creation time.
		compressed := append(zstd.Encode(data), 0x50, 0x2a, 0x4d, 0x18, 0x02, 0x00, 0x00, 0x00, 'H', 'i')
		c, err := chunk.NewChunkFromTrustedEncoded(compressed, len(data), remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, data, util.Must(c.GetBytes()))

		corrupted := append([]byte(nil), compressed...)
		for i := 12; i < len(corrupted)-10; i++ {
			corrupted[i] ^= 0xff
		}
		_, err = chunk.NewChunkFromTrustedEncoded(corrupted, len(data), remoteexecution.Compressor_ZSTD)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("ZSTDDecodeFailure", func(t *testing.T) {
		_, err := chunk.NewChunkFromTrustedEncoded([]byte("not valid zstd data"), 100, remoteexecution.Compressor_ZSTD)
		require.Error(t, err)
	})

	t.Run("UnsupportedCompressor", func(t *testing.T) {
		_, err := chunk.NewChunkFromTrustedEncoded([]byte("test data"), 9, remoteexecution.Compressor_DEFLATE)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})
}

func TestChunkComputeDigest(t *testing.T) {
	c := chunk.NewChunk([]byte("Hello"))

	require.Equal(
		t,
		digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5),
		util.Must(c.ComputeDigest(digest.MustNewFunction("", remoteexecution.DigestFunction_MD5))),
	)
}
