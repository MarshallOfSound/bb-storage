package grpcclients_test

import (
	"bytes"
	"context"
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
