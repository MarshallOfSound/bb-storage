package sharding_test

import (
	"context"
	"sync"
	"testing"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/sharding"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

func TestShardingBlobAccess(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	shard0 := mock.NewMockBlobAccess[[]byte](ctrl)
	shard1 := mock.NewMockBlobAccess[[]byte](ctrl)
	shardSelector := mock.NewMockShardSelector(ctrl)
	blobAccess := sharding.NewShardingBlobAccess(
		[]sharding.ShardBackend[[]byte]{
			{
				Backend: shard0,
				Key:     "shard0",
			},
			{
				Backend: shard1,
				Key:     "shard1",
			},
		},
		shardSelector,
	)

	helloDigest := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "8b1a9953c4611296a827abf8c47804d7", 5)

	t.Run("GetFailure", func(t *testing.T) {
		// Errors should be prefixed with the shard key.
		shardSelector.EXPECT().GetShard(uint64(0x8b1a9953c4611296)).Return(1)
		shard1.EXPECT().Get(ctx, helloDigest).
			Return(nil, status.Error(codes.Unavailable, "Server offline"))

		_, err := blobAccess.Get(ctx, helloDigest)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Shard shard1: Server offline"), err)
	})

	t.Run("GetSuccess", func(t *testing.T) {
		shardSelector.EXPECT().GetShard(uint64(0x8b1a9953c4611296)).Return(0)
		shard0.EXPECT().Get(ctx, helloDigest).
			Return([]byte("Hello"), nil)

		data, err := blobAccess.Get(ctx, helloDigest)
		require.NoError(t, err)
		require.Equal(t, []byte("Hello"), data)
	})

	t.Run("PutFailure", func(t *testing.T) {
		// Errors should be prefixed with a shard key.
		shardSelector.EXPECT().GetShard(uint64(0x8b1a9953c4611296)).Return(1)
		shard1.EXPECT().Put(ctx, helloDigest, gomock.Any()).Return(status.Error(codes.Unavailable, "Server offline"))

		testutil.RequireEqualStatus(
			t,
			status.Error(codes.Unavailable, "Shard shard1: Server offline"),
			blobAccess.Put(ctx, helloDigest, []byte("Hello")),
		)
	})

	t.Run("PutSuccess", func(t *testing.T) {
		shardSelector.EXPECT().GetShard(uint64(0x8b1a9953c4611296)).Return(0)
		shard0.EXPECT().Put(ctx, helloDigest, gomock.Any()).DoAndReturn(
			func(ctx context.Context, digest digest.Digest, value []byte) error {
				require.Equal(t, []byte("Hello"), value)
				return nil
			},
		)

		require.NoError(t, blobAccess.Put(ctx, helloDigest, []byte("Hello")))
	})

	digest1 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "21f843aefbfb88627ec2cad9e8f1f49a", 1)
	digest2 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "48f2503cf369373b0631da97fb9de1c1", 2)
	digest3 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "942a5b4164c26ae5d57a4f9526dcfca4", 3)
	digest4 := digest.MustNewDigest("", remoteexecution.DigestFunction_MD5, "f8f3da00ff2862082bddbb15300343f6", 4)

	t.Run("FindMissingFailure", func(t *testing.T) {
		// Digests provided to FindMissing() are partitioned,
		// causing up to one call per backend. If one of the
		// backends reports failure, we immediately cancel the
		// context for remaining requests, and return the first
		// error that occurred.
		shardSelector.EXPECT().GetShard(uint64(0x21f843aefbfb8862)).Return(0)
		shardSelector.EXPECT().GetShard(uint64(0x48f2503cf369373b)).Return(1)
		shard0.EXPECT().FindMissing(
			gomock.Any(),
			digest1.ToSingletonSet(),
		).Return(digest.EmptySet, status.Error(codes.Unavailable, "Server offline"))
		shard1.EXPECT().FindMissing(
			gomock.Any(),
			digest2.ToSingletonSet(),
		).DoAndReturn(func(ctx context.Context, digests digest.Set) (digest.Set, error) {
			<-ctx.Done()
			require.Equal(t, context.Canceled, ctx.Err())
			return digest.EmptySet, status.Error(codes.Canceled, "Context canceled")
		})

		_, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder(0).Add(digest1).Add(digest2).Build(),
		)
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Shard shard0: Server offline"), err)
	})

	t.Run("FindMissingSuccess", func(t *testing.T) {
		shardSelector.EXPECT().GetShard(uint64(0x21f843aefbfb8862)).Return(0)
		shardSelector.EXPECT().GetShard(uint64(0x48f2503cf369373b)).Return(0)
		shardSelector.EXPECT().GetShard(uint64(0x942a5b4164c26ae5)).Return(1)
		shardSelector.EXPECT().GetShard(uint64(0xf8f3da00ff286208)).Return(1)
		shard0.EXPECT().FindMissing(
			gomock.Any(),
			digest.NewSetBuilder(0).Add(digest1).Add(digest2).Build(),
		).Return(digest1.ToSingletonSet(), nil)
		shard1.EXPECT().FindMissing(
			gomock.Any(),
			digest.NewSetBuilder(0).Add(digest3).Add(digest4).Build(),
		).Return(digest3.ToSingletonSet(), nil)

		missing, err := blobAccess.FindMissing(
			ctx,
			digest.NewSetBuilder(0).
				Add(digest1).Add(digest2).
				Add(digest3).Add(digest4).
				Build(),
		)
		require.NoError(t, err)
		require.Equal(t, digest.NewSetBuilder(0).Add(digest1).Add(digest3).Build(), missing)
	})
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

func TestShardingBlobAccessBatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digest1 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "11111111111111110000000000000000", 1)
	digest2 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "22222222222222220000000000000000", 2)
	digest3 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "33333333333333330000000000000000", 3)
	digest4 := digest.MustNewDigest("example", remoteexecution.DigestFunction_MD5, "44444444444444440000000000000000", 4)
	digests := []digest.Digest{digest1, digest2, digest3, digest4}

	// Place digests 1 and 4 on shard 0, digest 2 on shard 1 and
	// digest 3 on shard 2.
	shardSelector := mock.NewMockShardSelector(ctrl)
	shardSelector.EXPECT().GetShard(uint64(0x1111111111111111)).Return(0).AnyTimes()
	shardSelector.EXPECT().GetShard(uint64(0x2222222222222222)).Return(1).AnyTimes()
	shardSelector.EXPECT().GetShard(uint64(0x3333333333333333)).Return(2).AnyTimes()
	shardSelector.EXPECT().GetShard(uint64(0x4444444444444444)).Return(0).AnyTimes()

	t.Run("NoBatchBackends", func(t *testing.T) {
		// If none of the backends support batches, there is no
		// point in announcing support for it.
		blobAccess := sharding.NewShardingBlobAccess(
			[]sharding.ShardBackend[string]{
				{Backend: mock.NewMockBlobAccess[string](ctrl), Key: "shard0"},
				{Backend: mock.NewMockBlobAccess[string](ctrl), Key: "shard1"},
			},
			shardSelector,
		)
		_, ok := blobAccess.(blobstore.BatchBlobAccess[string])
		require.False(t, ok)
	})

	// Two shards that support batches, and one that doesn't.
	shard0 := newMockBatchBlobAccess[string](ctrl)
	shard1 := newMockBatchBlobAccess[string](ctrl)
	shard2 := mock.NewMockBlobAccess[string](ctrl)
	blobAccess := sharding.NewShardingBlobAccess(
		[]sharding.ShardBackend[string]{
			{Backend: shard0, Key: "shard0"},
			{Backend: shard1, Key: "shard1"},
			{Backend: shard2, Key: "shard2"},
		},
		shardSelector,
	)

	t.Run("GetBatchSingleShard", func(t *testing.T) {
		shard0.MockBatchBlobAccess.EXPECT().GetBatch(ctx, []digest.Digest{digest1, digest4}).
			Return([]string{"1", ""}, []error{nil, status.Error(codes.NotFound, "Object not found")})

		values, errs := blobstore.GetBatch(ctx, blobAccess, []digest.Digest{digest1, digest4})
		require.Equal(t, []string{"1", ""}, values)
		require.Len(t, errs, 2)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Shard shard0: Object not found"), errs[1])
	})

	t.Run("GetBatchMultipleShards", func(t *testing.T) {
		// Shards should be called concurrently. Let the calls
		// against the first two shards wait for each other.
		var wg sync.WaitGroup
		wg.Add(2)
		shard0.MockBatchBlobAccess.EXPECT().GetBatch(ctx, []digest.Digest{digest1, digest4}).
			DoAndReturn(func(ctx context.Context, digests []digest.Digest) ([]string, []error) {
				wg.Done()
				wg.Wait()
				return []string{"1", "4"}, []error{nil, nil}
			})
		shard1.MockBatchBlobAccess.EXPECT().GetBatch(ctx, []digest.Digest{digest2}).
			DoAndReturn(func(ctx context.Context, digests []digest.Digest) ([]string, []error) {
				wg.Done()
				wg.Wait()
				return []string{""}, []error{status.Error(codes.Unavailable, "Server offline")}
			})
		// The shard that doesn't support batches should be
		// called through the regular interface.
		shard2.EXPECT().Get(ctx, digest3).Return("3", nil)

		values, errs := blobstore.GetBatch(ctx, blobAccess, digests)
		require.Equal(t, []string{"1", "", "3", "4"}, values)
		require.Len(t, errs, 4)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Shard shard1: Server offline"), errs[1])
		require.NoError(t, errs[2])
		require.NoError(t, errs[3])
	})

	t.Run("PutBatchSingleShard", func(t *testing.T) {
		shard1.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{digest2}, []string{"2"}).
			Return([]error{status.Error(codes.Internal, "Disk on fire")})

		errs := blobstore.PutBatch(ctx, blobAccess, []digest.Digest{digest2}, []string{"2"})
		require.Len(t, errs, 1)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Shard shard1: Disk on fire"), errs[0])
	})

	t.Run("PutBatchMultipleShards", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(2)
		shard0.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{digest1, digest4}, []string{"1", "4"}).
			DoAndReturn(func(ctx context.Context, digests []digest.Digest, values []string) []error {
				wg.Done()
				wg.Wait()
				return []error{status.Error(codes.Internal, "Disk on fire"), nil}
			})
		shard1.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{digest2}, []string{"2"}).
			DoAndReturn(func(ctx context.Context, digests []digest.Digest, values []string) []error {
				wg.Done()
				wg.Wait()
				return []error{nil}
			})
		shard2.EXPECT().Put(ctx, digest3, "3").Return(status.Error(codes.Unavailable, "Server offline"))

		errs := blobstore.PutBatch(ctx, blobAccess, digests, []string{"1", "2", "3", "4"})
		require.Len(t, errs, 4)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Shard shard0: Disk on fire"), errs[0])
		require.NoError(t, errs[1])
		testutil.RequireEqualStatus(t, status.Error(codes.Unavailable, "Shard shard2: Server offline"), errs[2])
		require.NoError(t, errs[3])
	})

	t.Run("Empty", func(t *testing.T) {
		values, errs := blobstore.GetBatch(ctx, blobAccess, nil)
		require.Empty(t, values)
		require.Empty(t, errs)
		require.Empty(t, blobstore.PutBatch(ctx, blobAccess, nil, nil))
	})
}
