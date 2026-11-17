package chunk

import (
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Chunk is an abstraction over a single chunk in the Chunk Storage (CS)
// that may or may not be compressed.
//
// For most chunks the uncompressed data is always available, as
// creators of chunks are required to decompress and verify the chunk
// against its digest prior to creation. The exception to this are
// chunks created using NewChunkFromTrustedEncoded(), which are only
// decompressed when the uncompressed data is requested.
//
// Chunk objects are thread safe but not trivially copyable, refer to
// them by pointer.
type Chunk struct {
	data []byte

	zstdData atomic.Pointer[[]byte]

	// Only set for chunks that are decompressed lazily, in which
	// case the data field above is unused.
	lazy *lazyData
}

// lazyData holds the state of a chunk whose uncompressed data is only
// computed when requested.
type lazyData struct {
	sizeBytes int

	once sync.Once
	data []byte
	err  error
}

// GetBytes gets the underlying bytes of the chunk in uncompressed
// format.
//
// This method can only fail for chunks created using
// NewChunkFromTrustedEncoded(), in case the encoded data turns out to
// be malformed.
func (c *Chunk) GetBytes() ([]byte, error) {
	l := c.lazy
	if l == nil {
		return c.data, nil
	}
	l.once.Do(func() {
		data, err := zstd.Decode(*c.zstdData.Load(), l.sizeBytes)
		if err != nil {
			l.err = util.StatusWrapWithCode(err, codes.Internal, "Failed to decode chunk")
		} else if len(data) != l.sizeBytes {
			l.err = status.Errorf(codes.Internal, "Size mismatch, expected %d bytes, got %d bytes", l.sizeBytes, len(data))
		} else {
			l.data = data
		}
	})
	return l.data, l.err
}

// GetEncodedBytes gets the underlying bytes of the chunk in the given
// encoded format. The IDENTITY compressor yields the uncompressed
// bytes.
func (c *Chunk) GetEncodedBytes(compressor remoteexecution.Compressor_Value) ([]byte, error) {
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return c.GetBytes()
	case remoteexecution.Compressor_ZSTD:
		if encodedData := c.zstdData.Load(); encodedData != nil {
			return *encodedData, nil
		}
		encodedData := zstd.Encode(c.data)
		c.zstdData.Store(&encodedData)
		return encodedData, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}

// ComputeDigest computes the digest of the underlying chunk.
//
// This method can only fail for chunks created using
// NewChunkFromTrustedEncoded(), in case the encoded data turns out to
// be malformed.
func (c *Chunk) ComputeDigest(digestFunction digest.Function) (digest.Digest, error) {
	data, err := c.GetBytes()
	if err != nil {
		return digest.BadDigest, err
	}
	generator := digestFunction.NewGenerator(int64(len(data)))
	if _, err := generator.Write(data); err != nil {
		panic("Should not be possible, got error while computing digest of fixed size byte slice: " + err.Error())
	}
	return generator.Sum(), nil
}

// NewChunk creates a chunk from an uncompressed byte slice. Ownership
// of the provided slice is transferred to the chunk, callers must not
// modify or reuse it afterwards.
func NewChunk(data []byte) *Chunk {
	return &Chunk{data: data}
}

// NewChunkFromEncoded creates a chunk from a byte slice in the given
// encoded format. The IDENTITY compressor accepts uncompressed bytes.
// In both cases the decoded data may not exceed
// maximumDecodedSizeBytes. Ownership of the provided slice is
// transferred to the chunk, callers must not modify it afterwards.
func NewChunkFromEncoded(encodedData []byte, maximumDecodedSizeBytes int, compressor remoteexecution.Compressor_Value) (*Chunk, error) {
	if maximumDecodedSizeBytes < 0 {
		return nil, status.Error(codes.InvalidArgument, "Maximum decoded size cannot be negative")
	}
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		if len(encodedData) > maximumDecodedSizeBytes {
			return nil, status.Errorf(codes.InvalidArgument, "Chunk data is %d bytes in size, while a maximum of %d bytes is permitted", len(encodedData), maximumDecodedSizeBytes)
		}
		return &Chunk{data: encodedData}, nil
	case remoteexecution.Compressor_ZSTD:
		data, err := zstd.Decode(encodedData, maximumDecodedSizeBytes)
		if err != nil {
			return nil, err
		}

		c := &Chunk{data: data}
		c.zstdData.Store(&encodedData)
		return c, nil
	default:
		return nil, status.Errorf(codes.Unimplemented, "Chunk does not support compression type: %s", compressor)
	}
}

// NewChunkFromTrustedEncoded creates a chunk of a given size from a
// byte slice in the given encoded format. Contrary to
// NewChunkFromEncoded(), it does not decompress ZSTD data upfront.
// Decompression is deferred until the uncompressed data is requested
// through GetBytes(), GetEncodedBytes() or ComputeDigest(). Consumers
// that only request the chunk in ZSTD format therefore do not cause
// any decompression to take place.
//
// As the resulting chunk cannot be validated against its digest without
// decompressing it, this function may only be used if the encoded data
// was obtained from a source that is trusted to have validated it
// (e.g., a storage server that is accessed over an authenticated and
// integrity protected connection).
//
// To still be able to guarantee that the chunk has the right size
// without decompressing it, ZSTD data is only decompressed lazily if
// its frame header announces the size of the decompressed data. If the
// announced size is incorrect, decompression fails.
func NewChunkFromTrustedEncoded(encodedData []byte, sizeBytes int, compressor remoteexecution.Compressor_Value) (*Chunk, error) {
	if compressor == remoteexecution.Compressor_ZSTD {
		if frameContentSize, ok := zstd.GetFrameContentSize(encodedData); ok {
			if frameContentSize != uint64(sizeBytes) {
				return nil, status.Errorf(codes.InvalidArgument, "Size mismatch, expected %d bytes, got %d bytes", sizeBytes, frameContentSize)
			}
			c := &Chunk{lazy: &lazyData{sizeBytes: sizeBytes}}
			c.zstdData.Store(&encodedData)
			return c, nil
		}
	}

	// Uncompressed data, or ZSTD data whose size cannot be
	// determined without decompressing it.
	c, err := NewChunkFromEncoded(encodedData, sizeBytes, compressor)
	if err != nil {
		return nil, err
	}
	if len(c.data) != sizeBytes {
		return nil, status.Errorf(codes.InvalidArgument, "Size mismatch, expected %d bytes, got %d bytes", sizeBytes, len(c.data))
	}
	return c, nil
}
