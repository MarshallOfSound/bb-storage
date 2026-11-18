package grpcservers_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcservers"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	klauspost_zstd "github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	status_pb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestContentAddressableStorageServerBatchReadBlobsSuccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 123)
	digest2 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 234)
	digest3 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6", 345)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 123,
			},
			{
				Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
				SizeBytes: 234,
			},
			{
				Hash:      "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6",
				SizeBytes: 345,
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	singleChunkParameters := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("ubuntu1804")).Return(singleChunkParameters, nil).Times(1)
	a := make([]byte, 123)
	chunkStorage.EXPECT().Get(ctx, digest1).Return(chunk.NewChunk(a), nil)
	b := make([]byte, 234)
	chunkStorage.EXPECT().Get(ctx, digest2).Return(chunk.NewChunk(b), nil)
	chunkStorage.EXPECT().Get(ctx, digest3).Return(nil, status.Error(codes.NotFound, "The object you requested could not be found"))

	maximumMessageSizeBytes := 4 << 20
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000, false)

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchReadBlobsResponse{
		Responses: []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest: &remoteexecution.Digest{
					Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
					SizeBytes: 123,
				},
				Data: a,
			},
			{
				Digest: &remoteexecution.Digest{
					Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
					SizeBytes: 234,
				},
				Data: b,
			},
			{
				Digest: &remoteexecution.Digest{
					Hash:      "7821919ee052d21515cf4e36788138a301c18c36931290270aece8d79ea2cca6",
					SizeBytes: 345,
				},
				Status: &status_pb.Status{
					Code:    int32(codes.NotFound),
					Message: "The object you requested could not be found",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchReadBlobsFailure(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 123,
			},
			{
				Hash:      "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0",
				SizeBytes: 234,
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	maximumMessageSizeBytes := 200
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000, false)

	_, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Attempted to read a total of at least 357 bytes, while a maximum of 200 bytes is permitted"), err)
}

func TestContentAddressableStorageServerBatchReadBlobsZSTD(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	request := &remoteexecution.BatchReadBlobsRequest{
		Digests:               []*remoteexecution.Digest{digest1.GetProto()},
		InstanceName:          "ubuntu1804",
		AcceptableCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)

	data := []byte("Hello")
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)
	chunkStorage.EXPECT().Get(ctx, digest1).Return(chunk.NewChunk(data), nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	require.Equal(t, digest1.GetProto(), response.Responses[0].Digest)
	require.NoError(t, status.ErrorProto(response.Responses[0].Status))
	require.Equal(t, remoteexecution.Compressor_ZSTD, response.Responses[0].Compressor)

	decompressed, err := zstd.Decode(response.Responses[0].Data, len(data))
	require.NoError(t, err)
	require.Equal(t, data, decompressed)
}

func TestContentAddressableStorageServerBatchUpdateBlobs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digest2 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "f5a7924e621e84c9280a9a27e1bcb7f6", 5)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest1.GetProto(),
				Data:   []byte("Hello"),
			},
			{
				Digest: digest2.GetProto(),
				Data:   []byte("World"),
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	chunkStorage.EXPECT().Put(ctx, digest1, gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(util.Must(chunk.GetBytes()), []byte("Hello"))
	})).Return(nil)
	chunkStorage.EXPECT().Put(ctx, digest2, gomock.Cond(func(x any) bool {
		chunk, ok := x.(*chunk.Chunk)
		if !ok {
			return false
		}
		return bytes.Equal(util.Must(chunk.GetBytes()), []byte("World"))
	})).Return(status.Error(codes.Internal, "Hard disk has a case of the Mondays"))

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)

	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
		Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
			{
				Digest: digest1.GetProto(),
			},
			{
				Digest: digest2.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.Internal),
					Message: "Failed to save chunk: Hard disk has a case of the Mondays",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchUpdateBlobsTooLarge(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// The digest advertises a blob that is larger than the maximum
	// message size. The compressed data itself is small, so without
	// the size check it would be decompressed into memory far beyond
	// what the backend is willing to accept.
	data := zstd.Encode(bytes.Repeat([]byte("A"), 100))
	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "e6065c4aa2ab1603008fc18410f579d4", 1<<20)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: "ubuntu1804",
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest:     digest1.GetProto(),
				Data:       data,
				Compressor: remoteexecution.Compressor_ZSTD,
			},
		},
	}

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, mock.NewMockReaderPutter(ctrl), allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)
	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	testutil.RequireEqualStatus(
		t,
		status.Error(codes.InvalidArgument, "Attempted to upload blob of 1048576 bytes but this backend only supports uploading blobs up to 200 bytes"),
		status.ErrorProto(response.Responses[0].Status),
	)
}

func TestContentAddressableStorageServerBatchUpdateBlobsMultiChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest: digest1.GetProto(),
				Data:   []byte("Hello"),
			},
		},
		InstanceName: "ubuntu1804",
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)

	// The blob is large enough relative to the minimum chunk size
	// that it cannot be stored as a single chunk, causing it to be
	// delegated to the ReaderPutter.
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 2, HorizonSizeBytes: 4}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)
	readerPutter.EXPECT().PutReaderAt(gomock.Any(), digest1, gomock.Any(), params).DoAndReturn(
		func(ctx context.Context, d digest.Digest, r io.ReaderAt, params *remoteexecution.RepMaxCdcParams) error {
			data, err := io.ReadAll(io.NewSectionReader(r, 0, d.GetSizeBytes()))
			require.NoError(t, err)
			require.Equal(t, []byte("Hello"), data)
			return status.Error(codes.Internal, "Storage backend on fire")
		},
	)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)

	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
		Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
			{
				Digest: digest1.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.Internal),
					Message: "Storage backend on fire",
				},
			},
		},
	}, response)
}

func TestContentAddressableStorageServerBatchUpdateBlobsZSTD(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	data := []byte("Hello")
	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	zstdPool := zstd.NewPoolFromConfiguration(nil)

	var compressed bytes.Buffer
	encoder, err := zstdPool.NewEncoder(ctx, &compressed)
	require.NoError(t, err)
	_, err = encoder.Write(data)
	require.NoError(t, err)
	require.NoError(t, encoder.Close())

	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(params, nil)
	chunkStorage.EXPECT().Put(ctx, digest1, gomock.Any()).Return(nil)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: "ubuntu1804",
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest:     digest1.GetProto(),
				Data:       compressed.Bytes(),
				Compressor: remoteexecution.Compressor_ZSTD,
			},
		},
	}

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)
	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	require.Equal(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
}

func TestContentAddressableStorageServerBatchUpdateBlobsReuseClientZSTDFrames(t *testing.T) {
	data := []byte("Hello")
	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}

	// A frame created by a client using its own encoder settings.
	encoder, err := klauspost_zstd.NewWriter(nil, klauspost_zstd.WithEncoderLevel(klauspost_zstd.SpeedFastest))
	require.NoError(t, err)
	clientFrame := encoder.EncodeAll(data, nil)
	require.NotEqual(t, zstd.Encode(data), clientFrame)

	// The same frame, followed by a skippable frame. It decompresses
	// to the right data, but must not be handed out to others.
	paddedFrame := append(append([]byte(nil), clientFrame...), 0x50, 0x2a, 0x4d, 0x18, 0x02, 0x00, 0x00, 0x00, 'H', 'i')

	update := func(t *testing.T, reuseClientZSTDFrames bool, requestData []byte, expectPut func(c *chunk.Chunk)) *remoteexecution.BatchUpdateBlobsResponse {
		ctrl, ctx := gomock.WithContext(context.Background(), t)
		chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
		chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
		cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
		readerPutter := mock.NewMockReaderPutter(ctrl)
		cdcParametersFetcher.EXPECT().FetchCDCParameters(
			gomock.Any(),
			mustNewInstanceName("ubuntu1804"),
		).Return(params, nil)
		if expectPut != nil {
			chunkStorage.EXPECT().Put(ctx, digest1, gomock.Any()).DoAndReturn(
				func(ctx context.Context, d digest.Digest, c *chunk.Chunk) error {
					require.Equal(t, data, util.Must(c.GetBytes()))
					expectPut(c)
					return nil
				},
			)
		}

		contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, reuseClientZSTDFrames)
		response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, &remoteexecution.BatchUpdateBlobsRequest{
			InstanceName: "ubuntu1804",
			Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
				{
					Digest:     digest1.GetProto(),
					Data:       requestData,
					Compressor: remoteexecution.Compressor_ZSTD,
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Responses, 1)
		return response
	}
	requireEncoded := func(t *testing.T, expected []byte) func(c *chunk.Chunk) {
		return func(c *chunk.Chunk) {
			encoded, err := c.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
			require.NoError(t, err)
			require.Equal(t, expected, encoded)
		}
	}

	t.Run("Disabled", func(t *testing.T) {
		// By default the frame provided by the client is
		// discarded, causing the chunk to be compressed again.
		response := update(t, false, clientFrame, requireEncoded(t, zstd.Encode(data)))
		require.Equal(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
	})

	t.Run("Reused", func(t *testing.T) {
		// The exact frame provided by the client is used as the
		// compressed representation of the chunk.
		response := update(t, true, clientFrame, requireEncoded(t, clientFrame))
		require.Equal(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
	})

	t.Run("GuardRejected", func(t *testing.T) {
		// The upload is still accepted, but the chunk is
		// compressed again.
		response := update(t, true, paddedFrame, requireEncoded(t, zstd.Encode(data)))
		require.Equal(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
	})

	t.Run("DigestMismatch", func(t *testing.T) {
		// A well formed frame that contains different data must
		// never reach storage.
		response := update(t, true, encoder.EncodeAll([]byte("Jello"), nil), nil)
		testutil.RequireEqualProto(t, &status_pb.Status{
			Code:    int32(codes.InvalidArgument),
			Message: "Blob digest mismatch, advertised 3-8b1a9953c4611296a827abf8c47804d7-5-ubuntu1804, actual 3-bedad9eef4de4b391cc5aeb8ddbe6387-5-ubuntu1804",
		}, response.Responses[0].Status)
	})

	t.Run("CorruptFrame", func(t *testing.T) {
		response := update(t, true, []byte("This is not a ZSTD stream"), nil)
		require.Equal(t, codes.InvalidArgument, codes.Code(response.Responses[0].Status.GetCode()))
	})

	t.Run("TooLarge", func(t *testing.T) {
		// Frames that decompress to more data than announced
		// by the digest are rejected without being decompressed
		// in full.
		response := update(t, true, encoder.EncodeAll(bytes.Repeat([]byte("Hello"), 1000), nil), nil)
		require.Equal(t, codes.InvalidArgument, codes.Code(response.Responses[0].Status.GetCode()))
	})
}

func TestContentAddressableStorageServerBatchUpdateBlobsCorruptZSTD(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	// Decompression must fail before any interaction with the
	// Content Addressable Storage takes place.
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("ubuntu1804"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

	request := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName: "ubuntu1804",
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{
				Digest:     digest1.GetProto(),
				Data:       []byte("This is not a ZSTD stream"),
				Compressor: remoteexecution.Compressor_ZSTD,
			},
		},
	}

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)
	response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.Responses, 1)
	require.NotEqual(t, codes.OK, codes.Code(response.Responses[0].Status.GetCode()))
}

// allowAllAuthorizer authorizes all requests, so that authorization
// does not interfere with the behaviors under test.
var allowAllAuthorizer = auth.NewStaticAuthorizer(func(digest.InstanceName) bool { return true })

func TestContentAddressableStorageServerFindMissingBlobs(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 16)
	digest2 := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 256)

	request := &remoteexecution.FindMissingBlobsRequest{
		InstanceName: "my_instance_name",
		BlobDigests: []*remoteexecution.Digest{
			{Hash: digest1.GetHashString(), SizeBytes: digest1.GetSizeBytes()},
			{Hash: digest2.GetHashString(), SizeBytes: digest2.GetSizeBytes()},
		},
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	digests := digest.NewSetBuilder(2)
	digests.Add(digest1)
	digests.Add(digest2)

	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("my_instance_name")).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digests.Build()).Return(digest.EmptySet, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)

	maximumMessageSizeBytes := 200
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000, false)

	response, err := contentAddressableStorageServer.FindMissingBlobs(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetMissingBlobDigests())
}

func TestContentAddressableStorageServerFindMissingBlobsEmptyBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	emptyDigest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)
	request := &remoteexecution.FindMissingBlobsRequest{
		InstanceName: "my_instance_name",
		BlobDigests:  []*remoteexecution.Digest{emptyDigest.GetProto()},
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	// The empty blob is always considered present. It may never be
	// reported as missing. The digest is filtered out before any
	// storage is consulted.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)
	chunkStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)
	chunkMappingStorage.EXPECT().FindMissing(gomock.Any(), digest.EmptySet).Return(digest.EmptySet, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)

	response, err := contentAddressableStorageServer.FindMissingBlobs(ctx, request)
	require.NoError(t, err)
	require.Empty(t, response.GetMissingBlobDigests())
}

func TestContentAddressableStorageServerBatchReadBlobsEmptyBlob(t *testing.T) {
	emptyDigest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)

	for _, tc := range []struct {
		name       string
		compressor remoteexecution.Compressor_Value
	}{
		{name: "IDENTITY", compressor: remoteexecution.Compressor_IDENTITY},
		{name: "ZSTD", compressor: remoteexecution.Compressor_ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)

			request := &remoteexecution.BatchReadBlobsRequest{
				Digests:               []*remoteexecution.Digest{emptyDigest.GetProto()},
				InstanceName:          "ubuntu1804",
				AcceptableCompressors: []remoteexecution.Compressor_Value{tc.compressor},
			}

			chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
			chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
			cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
			zstdPool := zstd.NewPoolFromConfiguration(nil)

			// The empty blob is always present. Reading it yields
			// empty data, possibly compressed, without contacting
			// storage.
			cdcParametersFetcher.EXPECT().FetchCDCParameters(
				gomock.Any(),
				mustNewInstanceName("ubuntu1804"),
			).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

			readerPutter := mock.NewMockReaderPutter(ctrl)
			contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)

			response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, request)
			require.NoError(t, err)
			require.Len(t, response.Responses, 1)
			require.Equal(t, emptyDigest.GetProto(), response.Responses[0].Digest)
			require.NoError(t, status.ErrorProto(response.Responses[0].Status))
			require.Equal(t, tc.compressor, response.Responses[0].Compressor)
			if tc.compressor == remoteexecution.Compressor_IDENTITY {
				require.Empty(t, response.Responses[0].Data)
			} else {
				// The response contains a compressed stream that
				// decodes to no data at all.
				require.NotEmpty(t, response.Responses[0].Data)
				decoder, err := zstdPool.NewDecoder(ctx, bytes.NewReader(response.Responses[0].Data))
				require.NoError(t, err)
				decompressed, err := io.ReadAll(decoder)
				require.NoError(t, err)
				decoder.Close()
				require.Empty(t, decompressed)
			}
		})
	}
}

func TestContentAddressableStorageServerBatchUpdateBlobsEmptyBlob(t *testing.T) {
	emptyDigest := digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", 0)

	for _, tc := range []struct {
		name       string
		compressor remoteexecution.Compressor_Value
	}{
		{name: "IDENTITY", compressor: remoteexecution.Compressor_IDENTITY},
		{name: "ZSTD", compressor: remoteexecution.Compressor_ZSTD},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl, ctx := gomock.WithContext(context.Background(), t)

			chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
			chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
			cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
			readerPutter := mock.NewMockReaderPutter(ctrl)
			zstdPool := zstd.NewPoolFromConfiguration(nil)

			// The empty blob decomposes into no chunks at all.
			// Uploading it succeeds without storing anything.
			cdcParametersFetcher.EXPECT().FetchCDCParameters(
				gomock.Any(),
				mustNewInstanceName("ubuntu1804"),
			).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

			var data []byte
			if tc.compressor == remoteexecution.Compressor_ZSTD {
				var compressed bytes.Buffer
				encoder, err := zstdPool.NewEncoder(ctx, &compressed)
				require.NoError(t, err)
				require.NoError(t, encoder.Close())
				data = compressed.Bytes()
			}

			request := &remoteexecution.BatchUpdateBlobsRequest{
				Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
					{
						Digest:     emptyDigest.GetProto(),
						Data:       data,
						Compressor: tc.compressor,
					},
				},
				InstanceName: "ubuntu1804",
			}

			contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)

			response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, request)
			require.NoError(t, err)
			testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
				Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
					{
						Digest: emptyDigest.GetProto(),
					},
				},
			}, response)
		})
	}
}

func TestContentAddressableStorageServerSplitBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunk1Digest, err := digestFunction.NewDigestFromProto(&remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 8,
	})
	require.NoError(t, err)
	chunk2Digest, err := digestFunction.NewDigestFromProto(&remoteexecution.Digest{
		Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
		SizeBytes: 8,
	})
	require.NoError(t, err)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 4, HorizonSizeBytes: 8}, nil)
	storedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, 16, false)
	require.NoError(t, err)
	chunkMappingStorage.EXPECT().Get(ctx, blobDigest).Return(
		storedChunkMapping,
		nil,
	)

	maximumMessageSizeBytes := 200
	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000, false)

	response, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	require.NoError(t, err)
	require.Len(t, response.ChunkDigests, 2)
	require.Equal(t, remoteexecution.ChunkingFunction_REP_MAX_CDC, response.ChunkingFunction)
}

func TestContentAddressableStorageServerSplitBlobSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	// The blob is small enough to fit in a single chunk. Such blobs
	// have no chunk mapping in storage, so splitting them is refused.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)

	_, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings"), err)
}

func TestContentAddressableStorageServerSplitBlobNotFound(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SplitBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		InstanceName:   "my_instance_name",
		DigestFunction: remoteexecution.DigestFunction_SHA256,
	}

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	// A single-chunk blob that does not exist. The trivial split is
	// refused before any storage interaction.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(
		gomock.Any(),
		mustNewInstanceName("my_instance_name"),
	).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1 << 20, HorizonSizeBytes: 2 << 20}, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)

	_, err := contentAddressableStorageServer.SplitBlob(ctx, request)
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings"), err)
}

func TestContentAddressableStorageServerSpliceBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	request := &remoteexecution.SpliceBlobRequest{
		BlobDigest: &remoteexecution.Digest{
			Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
			SizeBytes: 16,
		},
		ChunkDigests: []*remoteexecution.Digest{
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 8,
			},
			{
				Hash:      "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49",
				SizeBytes: 8,
			},
		},
		InstanceName: "my_instance_name",
	}

	instanceName, err := digest.NewInstanceName(request.InstanceName)
	require.NoError(t, err)
	digestFunction, err := instanceName.GetDigestFunction(request.DigestFunction, len(request.BlobDigest.Hash))
	require.NoError(t, err)
	blobDigest, err := digestFunction.NewDigestFromProto(request.BlobDigest)
	require.NoError(t, err)

	chunk1Digest, err := digestFunction.NewDigestFromProto(request.ChunkDigests[0])
	require.NoError(t, err)
	chunk2Digest, err := digestFunction.NewDigestFromProto(request.ChunkDigests[1])
	require.NoError(t, err)

	expectedChunkMapping, err := chunk.NewMappingFromDigests([]digest.Digest{chunk1Digest, chunk2Digest}, 16, false)
	require.NoError(t, err)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	chunkMappingStorage.EXPECT().Put(ctx, blobDigest, expectedChunkMapping).Return(nil)
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), blobDigest.GetInstanceName()).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 2, HorizonSizeBytes: 4}, nil)

	maximumMessageSizeBytes := 200

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, int64(maximumMessageSizeBytes), 1000, false)
	response, err := contentAddressableStorageServer.SpliceBlob(ctx, request)
	require.NoError(t, err)
	require.Equal(t, request.BlobDigest, response.BlobDigest)
}

func TestContentAddressableStorageServerRegisterChunkMappingSingleChunk(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// A chunk mapping with fewer than two chunks is trivial. Blobs
	// that fit within a single chunk have no chunk mapping in storage,
	// so such registrations are rejected outright.
	blobDigest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "e1bb3ab0d9402ae576a4f07d81f314f90a3e70672f5da6c21647e8ab507e511b", 18)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)

	for _, chunkDigests := range [][]*remoteexecution.Digest{
		{},
		{blobDigest.GetProto()},
	} {
		_, err := contentAddressableStorageServer.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
			InstanceName: "my_instance_name",
			BlobDigest:   blobDigest.GetProto(),
			ChunkDigests: chunkDigests,
		})
		require.Error(t, err)
		require.Equal(t, codes.Unimplemented, status.Code(err))
	}
}

// TestContentAddressableStorageServerRegisterChunkMappingSmallBlob
// verifies that a decomposition of a blob that fits in a single chunk is
// rejected, even if the client provides two or more chunks. Such blobs
// are stored as a chunk and never obtain a chunk mapping, so there is
// nothing to register.
func TestContentAddressableStorageServerRegisterChunkMappingSmallBlob(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	blobDigest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "e1bb3ab0d9402ae576a4f07d81f314f90a3e70672f5da6c21647e8ab507e511b", 18)
	chunk1Digest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "409a7f83ac6b31dc8c77e3ec18038f209bd2f545e0f4177c2e2381aa4e067b49", 9)
	chunk2Digest := digest.MustNewDigest("my_instance_name", remoteexecution.DigestFunction_SHA256, "0479688f99e8cbc70291ce272876ff8e0db71a0889daf2752884b0996056b4a0", 9)

	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	readerPutter := mock.NewMockReaderPutter(ctrl)

	// 18 bytes is below twice the minimum chunk size of 16 bytes.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), blobDigest.GetInstanceName()).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 16, HorizonSizeBytes: 32}, nil)

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)
	_, err := contentAddressableStorageServer.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
		InstanceName: "my_instance_name",
		BlobDigest:   blobDigest.GetProto(),
		ChunkDigests: []*remoteexecution.Digest{chunk1Digest.GetProto(), chunk2Digest.GetProto()},
	})
	testutil.RequireEqualStatus(t, status.Error(codes.Unimplemented, "This server does not implement RegisterChunkMapping calls for blobs that fit in a single chunk"), err)
}

func TestContentAddressableStorageServerRegisterChunkMappingSizeOverflow(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// The sizes of the chunks in a chunk mapping must add up to the size
	// of the blob. As sizes are 64 bit values, adding them up may
	// overflow. Three chunks of size 2^63 - 1 and one of size 18 add up
	// to a total size of 2^64 + 16 bytes, which wraps around to 16. The
	// server should reject such lists instead of treating them as
	// composing into a blob of 16 bytes.
	chunkStorage := mock.NewMockBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)

	chunkDigests := make([]*remoteexecution.Digest, 4)
	for i := 0; i < 3; i++ {
		chunkDigests[i] = &remoteexecution.Digest{
			Hash:      fmt.Sprintf("%064d", i),
			SizeBytes: math.MaxInt64,
		}
	}
	chunkDigests[3] = &remoteexecution.Digest{
		Hash:      "0000000000000000000000000000000000000000000000000000000000000012",
		SizeBytes: 18,
	}

	// The blob is large enough to not be rejected as a single chunk.
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("my_instance_name")).Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1, HorizonSizeBytes: 1}, nil)

	readerPutter := mock.NewMockReaderPutter(ctrl)
	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 200, 1000, false)
	_, err := contentAddressableStorageServer.SpliceBlob(ctx, &remoteexecution.SpliceBlobRequest{
		InstanceName: "my_instance_name",
		BlobDigest: &remoteexecution.Digest{
			Hash:      "0000000000000000000000000000000000000000000000000000000000000010",
			SizeBytes: 16,
		},
		ChunkDigests: chunkDigests,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

// mockBatchBlobAccess is a BlobAccess that also implements
// BatchBlobAccess.
type mockBatchBlobAccess[T any] struct {
	*mock.MockBlobAccess[T]
	*mock.MockBatchBlobAccess[T]
}

func newMockBatchBlobAccess[T any](ctrl *gomock.Controller) mockBatchBlobAccess[T] {
	return mockBatchBlobAccess[T]{
		MockBlobAccess:      mock.NewMockBlobAccess[T](ctrl),
		MockBatchBlobAccess: mock.NewMockBatchBlobAccess[T](ctrl),
	}
}

func newBatchTestBlob(sizeBytes int, fill byte) (digest.Digest, []byte) {
	data := bytes.Repeat([]byte{fill}, sizeBytes)
	hash := sha256.Sum256(data)
	return digest.MustNewDigest("ubuntu1804", remoteexecution.DigestFunction_SHA256, hex.EncodeToString(hash[:]), int64(sizeBytes)), data
}

func TestContentAddressableStorageServerBatchReadBlobsBatchBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// If the Chunk Storage supports batches, all blobs that are
	// stored as a single chunk should be requested at once. Empty
	// blobs and blobs consisting of multiple chunks should not be
	// part of the batch.
	digest1, data1 := newBatchTestBlob(123, 'a')
	digest2, _ := newBatchTestBlob(234, 'b')
	digestEmpty, _ := newBatchTestBlob(0, 'c')
	digestLarge, _ := newBatchTestBlob(3000, 'd')
	digest3, data3 := newBatchTestBlob(345, 'e')

	chunkStorage := newMockBatchBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("ubuntu1804")).
		Return(&remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1024, HorizonSizeBytes: 2048}, nil)
	chunkStorage.MockBatchBlobAccess.EXPECT().GetBatch(ctx, []digest.Digest{digest1, digest2, digest3}).Return(
		[]*chunk.Chunk{chunk.NewChunk(data1), nil, chunk.NewChunk(data3)},
		[]error{nil, status.Error(codes.NotFound, "The object you requested could not be found"), nil},
	)
	chunkMappingStorage.EXPECT().Get(ctx, digestLarge).Return(chunk.Mapping{}, status.Error(codes.NotFound, "Chunk mapping not found"))

	contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, mock.NewMockReaderPutter(ctrl), allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)

	response, err := contentAddressableStorageServer.BatchReadBlobs(ctx, &remoteexecution.BatchReadBlobsRequest{
		InstanceName: "ubuntu1804",
		Digests: []*remoteexecution.Digest{
			digest1.GetProto(),
			digest2.GetProto(),
			digestEmpty.GetProto(),
			digestLarge.GetProto(),
			digest3.GetProto(),
		},
	})
	require.NoError(t, err)
	testutil.RequireEqualProto(t, &remoteexecution.BatchReadBlobsResponse{
		Responses: []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest: digest1.GetProto(),
				Data:   data1,
			},
			{
				Digest: digest2.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.NotFound),
					Message: "The object you requested could not be found",
				},
			},
			{
				Digest: digestEmpty.GetProto(),
			},
			{
				Digest: digestLarge.GetProto(),
				Status: &status_pb.Status{
					Code:    int32(codes.NotFound),
					Message: "Chunk mapping not found",
				},
			},
			{
				Digest: digest3.GetProto(),
				Data:   data3,
			},
		},
	}, response)
}

// chunksWithData matches a list of chunks against their expected
// contents.
func chunksWithData(expected ...[]byte) gomock.Matcher {
	return gomock.Cond(func(x any) bool {
		chunks, ok := x.([]*chunk.Chunk)
		if !ok || len(chunks) != len(expected) {
			return false
		}
		for i, c := range chunks {
			if !bytes.Equal(util.Must(c.GetBytes()), expected[i]) {
				return false
			}
		}
		return true
	})
}

func TestContentAddressableStorageServerBatchUpdateBlobsBatchBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1, data1 := newBatchTestBlob(123, 'a')
	digest2, data2 := newBatchTestBlob(234, 'b')
	digestEmpty, _ := newBatchTestBlob(0, 'c')
	digestLarge, dataLarge := newBatchTestBlob(3000, 'd')
	digest3, data3 := newBatchTestBlob(345, 'e')
	digest4, data4 := newBatchTestBlob(456, 'f')

	chunkStorage := newMockBatchBlobAccess[*chunk.Chunk](ctrl)
	chunkMappingStorage := mock.NewMockBlobAccess[chunk.Mapping](ctrl)
	cdcParametersFetcher := mock.NewMockCDCParametersFetcher(ctrl)
	params := &remoteexecution.RepMaxCdcParams{MinChunkSizeBytes: 1024, HorizonSizeBytes: 2048}
	cdcParametersFetcher.EXPECT().FetchCDCParameters(gomock.Any(), mustNewInstanceName("ubuntu1804")).
		Return(params, nil).AnyTimes()
	readerPutter := mock.NewMockReaderPutter(ctrl)

	t.Run("SingleBatch", func(t *testing.T) {
		// All blobs that are stored as a single chunk should be
		// written at once, except for the ones that are
		// invalid. Failures to store individual chunks should
		// only affect the blobs in question.
		chunkStorage.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{digest1, digest2, digest4}, chunksWithData(data1, data2, data4)).
			Return([]error{nil, status.Error(codes.Internal, "Hard disk has a case of the Mondays"), nil})
		readerPutter.EXPECT().PutReaderAt(ctx, digestLarge, gomock.Any(), testutil.EqProto(t, params)).Return(nil)

		contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 4<<20, 1000, false)
		response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, &remoteexecution.BatchUpdateBlobsRequest{
			InstanceName: "ubuntu1804",
			Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
				{Digest: digest1.GetProto(), Data: data1},
				{Digest: digest2.GetProto(), Data: data2},
				{Digest: digestEmpty.GetProto()},
				{Digest: digestLarge.GetProto(), Data: dataLarge},
				// Data that does not match the digest.
				{Digest: digest3.GetProto(), Data: data3[1:]},
				{Digest: digest4.GetProto(), Data: data4},
			},
		})
		require.NoError(t, err)
		require.Len(t, response.Responses, 6)
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: digest1.GetProto(),
		}, response.Responses[0])
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: digest2.GetProto(),
			Status: &status_pb.Status{
				Code:    int32(codes.Internal),
				Message: "Failed to save chunk: Hard disk has a case of the Mondays",
			},
		}, response.Responses[1])
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: digestEmpty.GetProto(),
		}, response.Responses[2])
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: digestLarge.GetProto(),
		}, response.Responses[3])
		require.Equal(t, int32(codes.InvalidArgument), response.Responses[4].Status.GetCode())
		require.Contains(t, response.Responses[4].Status.GetMessage(), "Blob digest mismatch")
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse_Response{
			Digest: digest4.GetProto(),
		}, response.Responses[5])
	})

	t.Run("SplitOnMaximumMessageSize", func(t *testing.T) {
		// The total size of the chunks that are gathered
		// should not exceed the maximum message size, so that
		// the amount of memory used by a request is bounded,
		// regardless of how well the data it contains
		// compresses.
		gomock.InOrder(
			chunkStorage.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{digest1, digest2, digest3}, chunksWithData(data1, data2, data3)).
				Return([]error{nil, nil, status.Error(codes.Internal, "Hard disk has a case of the Mondays")}),
			chunkStorage.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{digest4}, chunksWithData(data4)).
				Return([]error{nil}),
		)

		contentAddressableStorageServer := grpcservers.NewContentAddressableStorageServer(chunkStorage, chunkMappingStorage, cdcParametersFetcher, readerPutter, allowAllAuthorizer, allowAllAuthorizer, 1000, 1000, false)
		response, err := contentAddressableStorageServer.BatchUpdateBlobs(ctx, &remoteexecution.BatchUpdateBlobsRequest{
			InstanceName: "ubuntu1804",
			Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
				{Digest: digest1.GetProto(), Data: data1},
				{Digest: digest2.GetProto(), Data: data2},
				{Digest: digest3.GetProto(), Data: zstd.Encode(data3), Compressor: remoteexecution.Compressor_ZSTD},
				{Digest: digest4.GetProto(), Data: data4},
			},
		})
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsResponse{
			Responses: []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{Digest: digest1.GetProto()},
				{Digest: digest2.GetProto()},
				{
					Digest: digest3.GetProto(),
					Status: &status_pb.Status{
						Code:    int32(codes.Internal),
						Message: "Failed to save chunk: Hard disk has a case of the Mondays",
					},
				},
				{Digest: digest4.GetProto()},
			},
		}, response)
	})
}
