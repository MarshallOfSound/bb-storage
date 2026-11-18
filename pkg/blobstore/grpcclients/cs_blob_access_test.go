package grpcclients_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/blobstore/grpcclients"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.uber.org/mock/gomock"
)

func expectGetCapabilitiesWithZSTD(client *mock.MockClientConnInterface) {
	client.EXPECT().Invoke(
		gomock.Any(),
		"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
		proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: digest.SupportedDigestFunctions,
				SupportedCompressors: []remoteexecution.Compressor_Value{
					remoteexecution.Compressor_ZSTD,
				},
				SplitBlobSupport:  true,
				SpliceBlobSupport: true,
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 256 << 10,
					HorizonSizeBytes:  8 * 256 << 10,
				},
			},
		})
		return nil
	}).AnyTimes()
}

func TestCSBlobAccessPut(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, false, false)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("InitialFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(status.Error(codes.Internal, "Failed to create outgoing connection"))

		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to create outgoing connection"),
			blobAccess.Put(ctx, blobDigest, chunk.NewChunk([]byte("Hello"))))
	})

	t.Run("ServerFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchUpdateBlobsResponse)
			resp.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.Unavailable, "Disk on fire").Proto(),
				},
			}
			return nil
		})

		testutil.RequireEqualStatus(t,
			status.Error(codes.Unavailable, "Disk on fire"),
			blobAccess.Put(ctx, blobDigest, chunk.NewChunk([]byte("Hello"))))
	})

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			testutil.EqProto(t, &remoteexecution.BatchUpdateBlobsRequest{
				InstanceName:   "hello",
				DigestFunction: remoteexecution.DigestFunction_MD5,
				Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
					{
						Digest:     blobDigest.GetProto(),
						Data:       []byte("Hello"),
						Compressor: remoteexecution.Compressor_IDENTITY,
					},
				},
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchUpdateBlobsResponse)
			resp.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.OK, "").Proto(),
				},
			}
			return nil
		})

		err := blobAccess.Put(ctx, blobDigest, chunk.NewChunk([]byte("Hello")))
		require.NoError(t, err)
	})
}

func TestCSBlobAccessGet(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, false, false)

	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			testutil.EqProto(t, &remoteexecution.BatchReadBlobsRequest{
				InstanceName:          "hello",
				Digests:               []*remoteexecution.Digest{blobDigest.GetProto()},
				AcceptableCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_IDENTITY},
				DigestFunction:        remoteexecution.DigestFunction_MD5,
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchReadBlobsResponse)
			resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
				{
					Digest:     blobDigest.GetProto(),
					Data:       []byte("Hello"),
					Status:     status.New(codes.OK, "").Proto(),
					Compressor: remoteexecution.Compressor_IDENTITY,
				},
			}
			return nil
		})

		chunk, err := blobAccess.Get(ctx, blobDigest)
		require.NoError(t, err)
		data := util.Must(chunk.GetBytes())
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("DigestMismatch", func(t *testing.T) {
		expectBatchReadBlobs(ctx, client, blobDigest, []byte("Jello"), remoteexecution.Compressor_IDENTITY)

		_, err := blobAccess.Get(ctx, blobDigest)
		actualDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "bedad9eef4de4b391cc5aeb8ddbe6387", 5)
		testutil.RequireEqualStatus(t,
			status.Errorf(codes.Internal, "Digest mismatch, expected %s, got %s", blobDigest.String(), actualDigest.String()),
			err)
	})

	t.Run("InitialFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).Return(status.Error(codes.Internal, "Failed to create outgoing connection"))

		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to create outgoing connection"),
			err)
	})

	t.Run("ServerFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchReadBlobsResponse)
			resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.Internal, "Lost connection to server").Proto(),
				},
			}
			return nil
		})

		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Lost connection to server"),
			err)
	})
}

func TestCSBlobAccessGetCapabilities(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, false, false)

	t.Run("BackendFailure", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
			testutil.EqProto(t, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "hello/world",
			}),
			gomock.Any(),
			gomock.Any(),
		).Return(status.Error(codes.Unavailable, "Server offline"))

		_, err := blobAccess.GetCapabilities(ctx, util.Must(digest.NewInstanceName("hello/world")))
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), err)
	})

	t.Run("OnlyExecution", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
			testutil.EqProto(t, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "hello/world",
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
				DeprecatedApiVersion: &semver.SemVer{Major: 2},
				LowApiVersion:        &semver.SemVer{Major: 2},
				HighApiVersion:       &semver.SemVer{Major: 2},
			})
			return nil
		})

		_, err := blobAccess.GetCapabilities(ctx, util.Must(digest.NewInstanceName("hello/world")))
		testutil.RequireEqualStatus(t, status.Error(codes.InvalidArgument, "Instance name \"hello/world\" does not support remote caching"), err)
	})

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.Capabilities/GetCapabilities",
			testutil.EqProto(t, &remoteexecution.GetCapabilitiesRequest{
				InstanceName: "hello/world",
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
			proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
				CacheCapabilities: &remoteexecution.CacheCapabilities{
					DigestFunctions: []remoteexecution.DigestFunction_Value{
						remoteexecution.DigestFunction_SHA256,
						remoteexecution.DigestFunction_VSO,
					},
					ActionCacheUpdateCapabilities: &remoteexecution.ActionCacheUpdateCapabilities{
						UpdateEnabled: true,
					},
					MaxBatchTotalSizeBytes:      1 << 20,
					SymlinkAbsolutePathStrategy: remoteexecution.SymlinkAbsolutePathStrategy_ALLOWED,
					SupportedCompressors: []remoteexecution.Compressor_Value{
						remoteexecution.Compressor_ZSTD,
					},
					SplitBlobSupport:  true,
					SpliceBlobSupport: true,
					RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
						MinChunkSizeBytes: 256 << 10,
						HorizonSizeBytes:  8 * 256 << 10,
					},
				},
				ExecutionCapabilities: &remoteexecution.ExecutionCapabilities{
					DigestFunction:  remoteexecution.DigestFunction_SHA256,
					DigestFunctions: digest.SupportedDigestFunctions,
					ExecEnabled:     true,
				},
				DeprecatedApiVersion: &semver.SemVer{Major: 2},
				LowApiVersion:        &semver.SemVer{Major: 2},
				HighApiVersion:       &semver.SemVer{Major: 2},
			})
			return nil
		})

		serverCapabilities, err := blobAccess.GetCapabilities(ctx, util.Must(digest.NewInstanceName("hello/world")))
		require.NoError(t, err)
		testutil.RequireEqualProto(t, &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions: []remoteexecution.DigestFunction_Value{
					remoteexecution.DigestFunction_SHA256,
				},
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 256 << 10,
					HorizonSizeBytes:  8 * 256 << 10,
				},
			},
			DeprecatedApiVersion: &semver.SemVer{Major: 2},
			LowApiVersion:        &semver.SemVer{Major: 2},
			HighApiVersion:       &semver.SemVer{Major: 2},
		}, serverCapabilities)
	})
}

func TestCSBlobAccessPutWithCompression(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, true, false)

	expectGetCapabilitiesWithZSTD(client)
	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

	t.Run("SuccessWithCompression", func(t *testing.T) {
		largeData := make([]byte, 1000)
		for i := range largeData {
			largeData[i] = byte('A' + (i % 26))
		}

		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			r := req.(*remoteexecution.BatchUpdateBlobsRequest)
			require.Len(t, r.Requests, 1)
			require.Equal(t, remoteexecution.Compressor_ZSTD, r.Requests[0].Compressor)
			require.Less(t, len(r.Requests[0].Data), 1000, "Compressed data should be smaller than original")

			resp := reply.(*remoteexecution.BatchUpdateBlobsResponse)
			resp.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{
				{
					Digest: blobDigest.GetProto(),
					Status: status.New(codes.OK, "").Proto(),
				},
			}
			return nil
		})

		err := blobAccess.Put(ctx, blobDigest, chunk.NewChunk(largeData))
		require.NoError(t, err)
	})
}

func TestCSBlobAccessGetWithCompression(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, true, false)

	expectGetCapabilitiesWithZSTD(client)

	t.Run("SuccessWithCompression", func(t *testing.T) {
		expectedData := make([]byte, 1000)
		for i := range expectedData {
			expectedData[i] = byte('A' + (i % 26))
		}
		largeDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		compressedData := encoder.EncodeAll(expectedData, nil)

		client.EXPECT().Invoke(
			ctx,
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
			testutil.EqProto(t, &remoteexecution.BatchReadBlobsRequest{
				InstanceName:          "hello",
				Digests:               []*remoteexecution.Digest{largeDigest.GetProto()},
				AcceptableCompressors: []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
				DigestFunction:        remoteexecution.DigestFunction_MD5,
			}),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			resp := reply.(*remoteexecution.BatchReadBlobsResponse)
			resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
				{
					Digest:     largeDigest.GetProto(),
					Data:       compressedData,
					Status:     status.New(codes.OK, "").Proto(),
					Compressor: remoteexecution.Compressor_ZSTD,
				},
			}
			return nil
		})

		chunk, err := blobAccess.Get(ctx, largeDigest)
		require.NoError(t, err)
		data := util.Must(chunk.GetBytes())
		require.Equal(t, expectedData, data)
	})
}

func expectBatchReadBlobs(ctx context.Context, client *mock.MockClientConnInterface, blobDigest digest.Digest, data []byte, compressor remoteexecution.Compressor_Value) {
	client.EXPECT().Invoke(
		ctx,
		"/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs",
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
		resp := reply.(*remoteexecution.BatchReadBlobsResponse)
		resp.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{
			{
				Digest:     blobDigest.GetProto(),
				Data:       data,
				Status:     status.New(codes.OK, "").Proto(),
				Compressor: compressor,
			},
		}
		return nil
	})
}

func TestCSBlobAccessGetSkipDigestVerification(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, true, true)

	expectGetCapabilitiesWithZSTD(client)
	blobDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("NotRehashed", func(t *testing.T) {
		// The server is trusted to have verified the chunk, so
		// contents that don't match the digest are not detected.
		expectBatchReadBlobs(ctx, client, blobDigest, []byte("Jello"), remoteexecution.Compressor_IDENTITY)

		chunk, err := blobAccess.Get(ctx, blobDigest)
		require.NoError(t, err)
		require.Equal(t, []byte("Jello"), util.Must(chunk.GetBytes()))
	})

	t.Run("CompressedPassthrough", func(t *testing.T) {
		// The ZSTD frame sent by the server is retained, so that
		// it can be returned to clients without recompressing.
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		compressedData := encoder.EncodeAll([]byte("Hello"), nil)
		expectBatchReadBlobs(ctx, client, blobDigest, compressedData, remoteexecution.Compressor_ZSTD)

		chunk, err := blobAccess.Get(ctx, blobDigest)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), util.Must(chunk.GetBytes()))
		encodedData, err := chunk.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressedData, encodedData)
	})

	t.Run("CompressedNotDecoded", func(t *testing.T) {
		// Compressed chunks are not decompressed unless their
		// uncompressed contents are requested. A chunk whose
		// frame is corrupted can therefore still be relayed in
		// compressed form, but not be read.
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		data := bytes.Repeat([]byte("Hello"), 1000)
		largerDigest := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", int64(len(data)))
		compressedData := encoder.EncodeAll(data, nil)
		for i := 12; i < len(compressedData); i++ {
			compressedData[i] ^= 0xff
		}
		expectBatchReadBlobs(ctx, client, largerDigest, compressedData, remoteexecution.Compressor_ZSTD)

		chunk, err := blobAccess.Get(ctx, largerDigest)
		require.NoError(t, err)
		encodedData, err := chunk.GetEncodedBytes(remoteexecution.Compressor_ZSTD)
		require.NoError(t, err)
		require.Equal(t, compressedData, encodedData)
		_, err = chunk.GetBytes()
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("SizeMismatch", func(t *testing.T) {
		// Truncated chunks are still rejected.
		expectBatchReadBlobs(ctx, client, blobDigest, []byte("Hell"), remoteexecution.Compressor_IDENTITY)

		_, err := blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to decode upstream response: Size mismatch, expected 5 bytes, got 4 bytes"),
			err)
	})

	t.Run("CompressedSizeMismatch", func(t *testing.T) {
		// The same holds for compressed chunks that decompress
		// to fewer bytes than expected.
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		expectBatchReadBlobs(ctx, client, blobDigest, encoder.EncodeAll([]byte("Hell"), nil), remoteexecution.Compressor_ZSTD)

		_, err = blobAccess.Get(ctx, blobDigest)
		testutil.RequireEqualStatus(t,
			status.Error(codes.Internal, "Failed to decode upstream response: Size mismatch, expected 5 bytes, got 4 bytes"),
			err)
	})

	t.Run("CompressedTooLarge", func(t *testing.T) {
		// Chunks that decompress to more bytes than expected
		// are rejected while decompressing.
		encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		require.NoError(t, err)
		expectBatchReadBlobs(ctx, client, blobDigest, encoder.EncodeAll([]byte("Hello world"), nil), remoteexecution.Compressor_ZSTD)

		_, err = blobAccess.Get(ctx, blobDigest)
		require.Equal(t, codes.Internal, status.Code(err))
	})
}

func TestCSBlobAccessFindMissing(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	client := mock.NewMockClientConnInterface(ctrl)
	blobAccess := grpcclients.NewCSBlobAccess(client, false, false)

	digest1 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)
	digest2 := digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, "1411ffd5854fa029dc4d231aa89311eb", 1000)

	t.Run("Success", func(t *testing.T) {
		client.EXPECT().Invoke(
			gomock.Any(),
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			r := req.(*remoteexecution.FindMissingBlobsRequest)
			require.Equal(t, "hello", r.InstanceName)
			require.Len(t, r.BlobDigests, 2)
			resp := reply.(*remoteexecution.FindMissingBlobsResponse)
			resp.MissingBlobDigests = []*remoteexecution.Digest{digest2.GetProto()}
			return nil
		})

		digestsToSearch := digest.NewSetBuilder(0).Add(digest1).Add(digest2).Build()
		missing, err := blobAccess.FindMissing(ctx, digestsToSearch)
		require.NoError(t, err)
		require.Equal(t, digest.NewSetBuilder(0).Add(digest2).Build(), missing)
	})

	// Requests with more digests than fit in a single message are
	// split into multiple concurrent calls.
	t.Run("MultipleBatches", func(t *testing.T) {
		digestCount := blobstore.RecommendedFindMissingDigestsCount + 1
		digestsToSearch := digest.NewSetBuilder(digestCount)
		for i := 0; i < digestCount; i++ {
			digestsToSearch.Add(digest.MustNewDigest("hello", remoteexecution.DigestFunction_MD5, fmt.Sprintf("%032x", i), 1))
		}

		var lock sync.Mutex
		batchSizes := []int{}
		client.EXPECT().Invoke(
			gomock.Any(),
			"/build.bazel.remote.execution.v2.ContentAddressableStorage/FindMissingBlobs",
			gomock.Any(),
			gomock.Any(),
			gomock.Any(),
		).DoAndReturn(func(ctx context.Context, method string, req, reply interface{}, opts ...grpc.CallOption) error {
			r := req.(*remoteexecution.FindMissingBlobsRequest)
			require.Equal(t, "hello", r.InstanceName)
			lock.Lock()
			batchSizes = append(batchSizes, len(r.BlobDigests))
			lock.Unlock()
			resp := reply.(*remoteexecution.FindMissingBlobsResponse)
			resp.MissingBlobDigests = r.BlobDigests
			return nil
		}).Times(2)

		missing, err := blobAccess.FindMissing(ctx, digestsToSearch.Build())
		require.NoError(t, err)
		require.Equal(t, digestsToSearch.Build(), missing)
		require.ElementsMatch(t, []int{blobstore.RecommendedFindMissingDigestsCount, 1}, batchSizes)
	})
}

// fakeContentAddressableStorage is a minimal Content Addressable
// Storage that is used to test batching. It records the number of blobs
// and the size of each of the requests it receives.
type fakeContentAddressableStorage struct {
	t *testing.T

	capabilitiesErr        error
	maxBatchTotalSizeBytes int64
	supportedCompressors   []remoteexecution.Compressor_Value
	blobs                  map[string][]byte

	// Hooks for altering the behavior of calls.
	readErr        func(request *remoteexecution.BatchReadBlobsRequest) error
	readResponse   func(response *remoteexecution.BatchReadBlobsResponse)
	updateErr      func(request *remoteexecution.BatchUpdateBlobsRequest) error
	updateStatus   func(request *remoteexecution.BatchUpdateBlobsRequest_Request) error
	updateResponse func(response *remoteexecution.BatchUpdateBlobsResponse)

	readRequests   []*remoteexecution.BatchReadBlobsRequest
	updateRequests []*remoteexecution.BatchUpdateBlobsRequest
}

func (f *fakeContentAddressableStorage) invoke(ctx context.Context, method string, args, reply interface{}, opts ...grpc.CallOption) error {
	switch method {
	case "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities":
		if f.capabilitiesErr != nil {
			return f.capabilitiesErr
		}
		proto.Merge(reply.(proto.Message), &remoteexecution.ServerCapabilities{
			CacheCapabilities: &remoteexecution.CacheCapabilities{
				DigestFunctions:        digest.SupportedDigestFunctions,
				MaxBatchTotalSizeBytes: f.maxBatchTotalSizeBytes,
				SupportedCompressors:   f.supportedCompressors,
				SplitBlobSupport:       true,
				SpliceBlobSupport:      true,
				RepMaxCdcParams: &remoteexecution.RepMaxCdcParams{
					MinChunkSizeBytes: 256 << 10,
					HorizonSizeBytes:  8 * 256 << 10,
				},
			},
		})
		return nil
	case "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchReadBlobs":
		request := args.(*remoteexecution.BatchReadBlobsRequest)
		f.readRequests = append(f.readRequests, request)
		if f.readErr != nil {
			if err := f.readErr(request); err != nil {
				return err
			}
		}
		require.Len(f.t, request.AcceptableCompressors, 1)
		compressor := request.AcceptableCompressors[0]
		response := &remoteexecution.BatchReadBlobsResponse{}
		for _, d := range request.Digests {
			r := &remoteexecution.BatchReadBlobsResponse_Response{
				Digest:     d,
				Compressor: compressor,
			}
			if data, ok := f.blobs[d.Hash]; !ok {
				r.Status = status.New(codes.NotFound, "Blob not found").Proto()
			} else if compressor == remoteexecution.Compressor_ZSTD {
				r.Data = zstdCompress(f.t, data)
			} else {
				r.Data = data
			}
			response.Responses = append(response.Responses, r)
		}
		if f.readResponse != nil {
			f.readResponse(response)
		}
		proto.Merge(reply.(proto.Message), response)
		return nil
	case "/build.bazel.remote.execution.v2.ContentAddressableStorage/BatchUpdateBlobs":
		request := args.(*remoteexecution.BatchUpdateBlobsRequest)
		f.updateRequests = append(f.updateRequests, request)
		if f.updateErr != nil {
			if err := f.updateErr(request); err != nil {
				return err
			}
		}
		response := &remoteexecution.BatchUpdateBlobsResponse{}
		for _, r := range request.Requests {
			var err error
			if f.updateStatus != nil {
				err = f.updateStatus(r)
			}
			response.Responses = append(response.Responses, &remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: r.Digest,
				Status: status.Convert(err).Proto(),
			})
		}
		if f.updateResponse != nil {
			f.updateResponse(response)
		}
		proto.Merge(reply.(proto.Message), response)
		return nil
	default:
		f.t.Fatalf("Unexpected method %#v", method)
		return nil
	}
}

func (f *fakeContentAddressableStorage) readBatchSizes() []int {
	sizes := make([]int, 0, len(f.readRequests))
	for _, request := range f.readRequests {
		sizes = append(sizes, len(request.Digests))
	}
	f.readRequests = nil
	return sizes
}

func (f *fakeContentAddressableStorage) updateBatchSizes() []int {
	sizes := make([]int, 0, len(f.updateRequests))
	for _, request := range f.updateRequests {
		sizes = append(sizes, len(request.Requests))
	}
	f.updateRequests = nil
	return sizes
}

func zstdCompress(t *testing.T, data []byte) []byte {
	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	return encoder.EncodeAll(data, nil)
}

// newBatchTestBlobs creates blobs with distinct contents of a given
// size, returning their digests.
func newBatchTestBlobs(f *fakeContentAddressableStorage, instanceName string, count, sizeBytes int) ([]digest.Digest, [][]byte) {
	digests := make([]digest.Digest, 0, count)
	blobs := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		data := bytes.Repeat([]byte{byte('a' + i)}, sizeBytes)
		hash := sha256.Sum256(data)
		d := digest.MustNewDigest(instanceName, remoteexecution.DigestFunction_SHA256, hex.EncodeToString(hash[:]), int64(sizeBytes))
		if f.blobs == nil {
			f.blobs = map[string][]byte{}
		}
		f.blobs[d.GetHashString()] = data
		digests = append(digests, d)
		blobs = append(blobs, data)
	}
	return digests, blobs
}

func newBatchTestBlobAccess(t *testing.T, f *fakeContentAddressableStorage, preferCompression, skipDigestVerification bool) (context.Context, blobstore.BatchBlobAccess[*chunk.Chunk]) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)
	f.t = t
	client := mock.NewMockClientConnInterface(ctrl)
	client.EXPECT().Invoke(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(f.invoke).AnyTimes()
	return ctx, grpcclients.NewCSBlobAccess(client, preferCompression, skipDigestVerification).(blobstore.BatchBlobAccess[*chunk.Chunk])
}

func requireChunksEqual(t *testing.T, blobs [][]byte, chunks []*chunk.Chunk, errs []error) {
	require.Len(t, chunks, len(blobs))
	require.Len(t, errs, len(blobs))
	for i, blob := range blobs {
		require.NoError(t, errs[i])
		require.Equal(t, blob, util.Must(chunks[i].GetBytes()))
	}
}

func TestCSBlobAccessGetBatch(t *testing.T) {
	t.Run("NoLimitAnnounced", func(t *testing.T) {
		// If the server does not announce a maximum batch
		// size, every chunk should be requested separately.
		f := &fakeContentAddressableStorage{}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 3, 100)

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		requireChunksEqual(t, blobs, chunks, errs)
		require.Equal(t, []int{1, 1, 1}, f.readBatchSizes())
	})

	t.Run("CapabilitiesFailure", func(t *testing.T) {
		f := &fakeContentAddressableStorage{
			capabilitiesErr: status.Error(codes.Unavailable, "Server offline"),
		}
		digests, blobs := newBatchTestBlobs(f, "hello", 2, 100)

		// Without compression, Get() does not depend on the
		// capabilities of the server. Batches shouldn't either.
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		chunks, errs := blobAccess.GetBatch(ctx, digests)
		requireChunksEqual(t, blobs, chunks, errs)
		require.Equal(t, []int{1, 1}, f.readBatchSizes())

		// With compression the capabilities are required.
		ctx, blobAccess = newBatchTestBlobAccess(t, f, true, false)
		chunks, errs = blobAccess.GetBatch(ctx, digests)
		require.Equal(t, []*chunk.Chunk{nil, nil}, chunks)
		require.Len(t, errs, 2)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), errs[1])
		require.Empty(t, f.readBatchSizes())
	})

	t.Run("SingleBatch", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 5, 100)

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		requireChunksEqual(t, blobs, chunks, errs)
		require.Len(t, f.readRequests, 1)
		require.Equal(t, "hello", f.readRequests[0].InstanceName)
		require.Equal(t, remoteexecution.DigestFunction_SHA256, f.readRequests[0].DigestFunction)
		require.Equal(t, []remoteexecution.Compressor_Value{remoteexecution.Compressor_IDENTITY}, f.readRequests[0].AcceptableCompressors)
		require.Equal(t, []int{5}, f.readBatchSizes())

		// The same digest may be requested more than once.
		chunks, errs = blobAccess.GetBatch(ctx, []digest.Digest{digests[0], digests[1], digests[0]})
		requireChunksEqual(t, [][]byte{blobs[0], blobs[1], blobs[0]}, chunks, errs)
		require.Equal(t, []int{3}, f.readBatchSizes())

		// Empty batches should not lead to any requests.
		chunks, errs = blobAccess.GetBatch(ctx, nil)
		require.Empty(t, chunks)
		require.Empty(t, errs)
		require.Empty(t, f.readBatchSizes())
	})

	t.Run("Compression", func(t *testing.T) {
		for _, skipDigestVerification := range []bool{false, true} {
			f := &fakeContentAddressableStorage{
				maxBatchTotalSizeBytes: 1 << 20,
				supportedCompressors:   []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
			}
			ctx, blobAccess := newBatchTestBlobAccess(t, f, true, skipDigestVerification)
			digests, blobs := newBatchTestBlobs(f, "hello", 5, 100)

			chunks, errs := blobAccess.GetBatch(ctx, digests)
			requireChunksEqual(t, blobs, chunks, errs)
			require.Len(t, f.readRequests, 1)
			require.Equal(t, []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD}, f.readRequests[0].AcceptableCompressors)
		}
	})

	t.Run("SplitOnServerLimit", func(t *testing.T) {
		// Every chunk is accounted as 300 + 300/128 + 256 = 558
		// bytes, meaning that three of them fit in a request.
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 2000}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 7, 300)

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		requireChunksEqual(t, blobs, chunks, errs)
		require.Equal(t, []int{3, 3, 1}, f.readBatchSizes())

		// Chunks that are larger than the limit should still
		// be requested, each in a request of their own. This
		// is consistent with Get().
		largeDigests, largeBlobs := newBatchTestBlobs(f, "hello", 2, 3000)
		chunks, errs = blobAccess.GetBatch(ctx, []digest.Digest{digests[0], largeDigests[0], largeDigests[1], digests[1], digests[2]})
		requireChunksEqual(t, [][]byte{blobs[0], largeBlobs[0], largeBlobs[1], blobs[1], blobs[2]}, chunks, errs)
		require.Equal(t, []int{1, 1, 1, 2}, f.readBatchSizes())
	})

	t.Run("SplitOnResponseSize", func(t *testing.T) {
		// Even if the server permits larger batches, responses
		// must not exceed the 4 MiB that the gRPC client is
		// willing to receive.
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 64 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 5, 1500000)

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		requireChunksEqual(t, blobs, chunks, errs)
		require.Equal(t, []int{2, 2, 1}, f.readBatchSizes())
	})

	t.Run("SplitOnDigestFunction", func(t *testing.T) {
		// Requests can only contain digests that use the same
		// instance name and digest function.
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests1, blobs1 := newBatchTestBlobs(f, "hello", 2, 100)
		digests2, blobs2 := newBatchTestBlobs(f, "world", 3, 200)

		chunks, errs := blobAccess.GetBatch(ctx, append(append([]digest.Digest(nil), digests1...), digests2...))
		requireChunksEqual(t, append(append([][]byte(nil), blobs1...), blobs2...), chunks, errs)
		require.Len(t, f.readRequests, 2)
		require.Equal(t, "hello", f.readRequests[0].InstanceName)
		require.Equal(t, "world", f.readRequests[1].InstanceName)
		require.Equal(t, []int{2, 3}, f.readBatchSizes())
	})

	t.Run("PerBlobErrors", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 5, 100)

		// One chunk that is absent, one that is corrupted, and
		// one for which the server returns data of the wrong
		// size. The other chunks should be unaffected.
		delete(f.blobs, digests[1].GetHashString())
		f.blobs[digests[2].GetHashString()] = bytes.Repeat([]byte{'x'}, 100)
		f.blobs[digests[3].GetHashString()] = bytes.Repeat([]byte{'x'}, 101)

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		require.Len(t, chunks, 5)
		require.Len(t, errs, 5)
		require.NoError(t, errs[0])
		require.Equal(t, blobs[0], util.Must(chunks[0].GetBytes()))
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Blob not found"), errs[1])
		require.Nil(t, chunks[1])
		require.Equal(t, codes.Internal, status.Code(errs[2]))
		require.Contains(t, errs[2].Error(), "Digest mismatch")
		require.Nil(t, chunks[2])
		require.Equal(t, codes.Internal, status.Code(errs[3]))
		require.Contains(t, errs[3].Error(), "Failed to decode upstream response")
		require.Nil(t, chunks[3])
		require.NoError(t, errs[4])
		require.Equal(t, blobs[4], util.Must(chunks[4].GetBytes()))
		require.Equal(t, []int{5}, f.readBatchSizes())
	})

	t.Run("RPCFailure", func(t *testing.T) {
		// Failures of the call as a whole should be returned
		// for the chunks that were part of it.
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 2000}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 5, 300)
		f.readErr = func(request *remoteexecution.BatchReadBlobsRequest) error {
			if len(request.Digests) == 3 {
				return status.Error(codes.Unavailable, "Server offline")
			}
			return nil
		}

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		require.Len(t, errs, 5)
		for i := 0; i < 3; i++ {
			testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), errs[i])
			require.Nil(t, chunks[i])
		}
		requireChunksEqual(t, blobs[3:], chunks[3:], errs[3:])
		require.Equal(t, []int{3, 2}, f.readBatchSizes())
	})

	t.Run("BatchRejectedAsTooLarge", func(t *testing.T) {
		// If the server rejects a batch even though it stays
		// below the limit that was announced, the chunks should
		// be requested one by one.
		for _, code := range []codes.Code{codes.InvalidArgument, codes.ResourceExhausted} {
			f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
			ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
			digests, blobs := newBatchTestBlobs(f, "hello", 3, 100)
			f.readErr = func(request *remoteexecution.BatchReadBlobsRequest) error {
				if len(request.Digests) > 1 {
					return status.Error(code, "Request too large")
				}
				return nil
			}

			chunks, errs := blobAccess.GetBatch(ctx, digests)
			requireChunksEqual(t, blobs, chunks, errs)
			require.Equal(t, []int{3, 1, 1, 1}, f.readBatchSizes())
		}
	})

	t.Run("ResponsesOutOfOrder", func(t *testing.T) {
		// Responses need to be matched against the digests
		// that were requested. Chunks for which no response is
		// returned should fail.
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 4, 100)
		f.readResponse = func(response *remoteexecution.BatchReadBlobsResponse) {
			r := response.Responses
			response.Responses = []*remoteexecution.BatchReadBlobsResponse_Response{r[3], r[0], r[2]}
		}

		chunks, errs := blobAccess.GetBatch(ctx, digests)
		require.Len(t, chunks, 4)
		require.Len(t, errs, 4)
		for _, i := range []int{0, 2, 3} {
			require.NoError(t, errs[i])
			require.Equal(t, blobs[i], util.Must(chunks[i].GetBytes()))
		}
		testutil.RequireEqualStatus(t, status.Errorf(codes.Internal, "Upstream server did not return a response for blob %s", digests[1]), errs[1])
		require.Nil(t, chunks[1])
	})
}

func TestCSBlobAccessPutBatch(t *testing.T) {
	newChunks := func(blobs [][]byte) []*chunk.Chunk {
		chunks := make([]*chunk.Chunk, 0, len(blobs))
		for _, blob := range blobs {
			chunks = append(chunks, chunk.NewChunk(blob))
		}
		return chunks
	}
	requireNoErrors := func(t *testing.T, count int, errs []error) {
		require.Len(t, errs, count)
		for _, err := range errs {
			require.NoError(t, err)
		}
	}

	t.Run("NoLimitAnnounced", func(t *testing.T) {
		f := &fakeContentAddressableStorage{}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 3, 100)

		requireNoErrors(t, 3, blobAccess.PutBatch(ctx, digests, newChunks(blobs)))
		require.Equal(t, []int{1, 1, 1}, f.updateBatchSizes())
	})

	t.Run("SingleBatch", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 3, 100)

		requireNoErrors(t, 3, blobAccess.PutBatch(ctx, digests, newChunks(blobs)))
		require.Len(t, f.updateRequests, 1)
		testutil.RequireEqualProto(t, &remoteexecution.BatchUpdateBlobsRequest{
			InstanceName:   "hello",
			DigestFunction: remoteexecution.DigestFunction_SHA256,
			Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
				{Digest: digests[0].GetProto(), Data: blobs[0]},
				{Digest: digests[1].GetProto(), Data: blobs[1]},
				{Digest: digests[2].GetProto(), Data: blobs[2]},
			},
		}, f.updateRequests[0])

		require.Empty(t, blobAccess.PutBatch(ctx, nil, nil))
		require.Equal(t, []int{3}, f.updateBatchSizes())
	})

	t.Run("SplitOnServerLimit", func(t *testing.T) {
		// 2000 bytes, minus 5 + 256 bytes for the request
		// itself, leaves space for three chunks of 300 + 256
		// bytes.
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 2000}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 7, 300)

		requireNoErrors(t, 7, blobAccess.PutBatch(ctx, digests, newChunks(blobs)))
		for _, request := range f.updateRequests {
			require.LessOrEqual(t, proto.Size(request), 2000)
		}
		require.Equal(t, []int{3, 3, 1}, f.updateBatchSizes())

		// Chunks that are larger than the limit should be sent
		// in a request of their own, consistent with Put().
		largeDigests, largeBlobs := newBatchTestBlobs(f, "hello", 1, 3000)
		requireNoErrors(t, 4, blobAccess.PutBatch(
			ctx,
			[]digest.Digest{digests[0], largeDigests[0], digests[1], digests[2]},
			newChunks([][]byte{blobs[0], largeBlobs[0], blobs[1], blobs[2]})))
		require.Equal(t, []int{1, 1, 2}, f.updateBatchSizes())
	})

	t.Run("SplitOnEncodedSize", func(t *testing.T) {
		// Chunks that we received in compressed form may be
		// far smaller than what needs to be sent to a server
		// if compression towards that server is not used. The
		// size of what is actually sent is what matters.
		blobs := [][]byte{}
		chunks := []*chunk.Chunk{}
		digests := []digest.Digest{}
		for i := 0; i < 5; i++ {
			blob := bytes.Repeat([]byte{byte('a' + i)}, 100<<10)
			compressed := zstdCompress(t, blob)
			require.Less(t, len(compressed), 1000)
			c, err := chunk.NewChunkFromEncoded(compressed, len(blob), remoteexecution.Compressor_ZSTD)
			require.NoError(t, err)
			hash := sha256.Sum256(blob)
			blobs = append(blobs, blob)
			chunks = append(chunks, c)
			digests = append(digests, digest.MustNewDigest("hello", remoteexecution.DigestFunction_SHA256, hex.EncodeToString(hash[:]), int64(len(blob))))
		}

		// Without compression, only two chunks of 100 KiB fit
		// in a request of 256 KiB.
		f := &fakeContentAddressableStorage{
			maxBatchTotalSizeBytes: 256 << 10,
			supportedCompressors:   []remoteexecution.Compressor_Value{remoteexecution.Compressor_ZSTD},
		}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		requireNoErrors(t, 5, blobAccess.PutBatch(ctx, digests, chunks))
		for _, request := range f.updateRequests {
			require.LessOrEqual(t, proto.Size(request), 256<<10)
			for _, r := range request.Requests {
				require.Equal(t, remoteexecution.Compressor_IDENTITY, r.Compressor)
				require.Len(t, r.Data, 100<<10)
			}
		}
		require.Equal(t, []int{2, 2, 1}, f.updateBatchSizes())

		// With compression, all of them fit.
		ctx, blobAccess = newBatchTestBlobAccess(t, f, true, false)
		requireNoErrors(t, 5, blobAccess.PutBatch(ctx, digests, chunks))
		for _, r := range f.updateRequests[0].Requests {
			require.Equal(t, remoteexecution.Compressor_ZSTD, r.Compressor)
			require.Less(t, len(r.Data), 1000)
		}
		require.Equal(t, []int{5}, f.updateBatchSizes())
	})

	t.Run("SplitOnDigestFunction", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests1, blobs1 := newBatchTestBlobs(f, "hello", 2, 100)
		digests2, blobs2 := newBatchTestBlobs(f, "world", 3, 200)

		requireNoErrors(t, 5, blobAccess.PutBatch(
			ctx,
			append(append([]digest.Digest(nil), digests1...), digests2...),
			newChunks(append(append([][]byte(nil), blobs1...), blobs2...))))
		require.Len(t, f.updateRequests, 2)
		require.Equal(t, "hello", f.updateRequests[0].InstanceName)
		require.Equal(t, "world", f.updateRequests[1].InstanceName)
		require.Equal(t, []int{2, 3}, f.updateBatchSizes())
	})

	t.Run("PerBlobErrors", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 3, 100)
		f.updateStatus = func(r *remoteexecution.BatchUpdateBlobsRequest_Request) error {
			if r.Digest.Hash == digests[1].GetHashString() {
				return status.Error(codes.Internal, "Disk on fire")
			}
			return nil
		}

		errs := blobAccess.PutBatch(ctx, digests, newChunks(blobs))
		require.Len(t, errs, 3)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), errs[1])
		require.NoError(t, errs[2])
		require.Equal(t, []int{3}, f.updateBatchSizes())
	})

	t.Run("RPCFailure", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 2000}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 5, 300)
		f.updateErr = func(request *remoteexecution.BatchUpdateBlobsRequest) error {
			if len(request.Requests) == 2 {
				return status.Error(codes.Unavailable, "Server offline")
			}
			return nil
		}

		errs := blobAccess.PutBatch(ctx, digests, newChunks(blobs))
		require.Len(t, errs, 5)
		requireNoErrors(t, 3, errs[:3])
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), errs[3])
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Server offline"), errs[4])
		require.Equal(t, []int{3, 2}, f.updateBatchSizes())
	})

	t.Run("BatchRejectedAsTooLarge", func(t *testing.T) {
		for _, code := range []codes.Code{codes.InvalidArgument, codes.ResourceExhausted} {
			f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
			ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
			digests, blobs := newBatchTestBlobs(f, "hello", 3, 100)
			f.updateErr = func(request *remoteexecution.BatchUpdateBlobsRequest) error {
				if len(request.Requests) > 1 {
					return status.Error(code, "Request too large")
				}
				return nil
			}

			requireNoErrors(t, 3, blobAccess.PutBatch(ctx, digests, newChunks(blobs)))
			require.Equal(t, []int{3, 1, 1, 1}, f.updateBatchSizes())
		}
	})

	t.Run("ResponsesOutOfOrder", func(t *testing.T) {
		f := &fakeContentAddressableStorage{maxBatchTotalSizeBytes: 1 << 20}
		ctx, blobAccess := newBatchTestBlobAccess(t, f, false, false)
		digests, blobs := newBatchTestBlobs(f, "hello", 4, 100)
		f.updateStatus = func(r *remoteexecution.BatchUpdateBlobsRequest_Request) error {
			if r.Digest.Hash == digests[3].GetHashString() {
				return status.Error(codes.Internal, "Disk on fire")
			}
			return nil
		}
		f.updateResponse = func(response *remoteexecution.BatchUpdateBlobsResponse) {
			r := response.Responses
			response.Responses = []*remoteexecution.BatchUpdateBlobsResponse_Response{r[3], r[0], r[2]}
		}

		errs := blobAccess.PutBatch(ctx, digests, newChunks(blobs))
		require.Len(t, errs, 4)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Errorf(codes.Internal, "Upstream server did not return a response for blob %s-100", digests[1].GetHashString()), errs[1])
		require.NoError(t, errs[2])
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), errs[3])
	})
}
