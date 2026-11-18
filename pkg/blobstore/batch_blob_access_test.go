package blobstore_test

import (
	"context"
	"testing"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/internal/mock"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/eviction"
	"github.com/buildbarn/bb-storage/pkg/testutil"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.uber.org/mock/gomock"
)

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

var (
	batchDigest1 = digest.MustNewDigest("beep", remoteexecution.DigestFunction_SHA256, "693d8db7b05e99c6b7a7c0616456039d89c555029026936248085193559a0b5d", 16)
	batchDigest2 = digest.MustNewDigest("bop/bip", remoteexecution.DigestFunction_SHA256, "da95ccd92a874d2169839cd90d9045be61d17df779fb28fe520a7465c6063723", 3)
	batchDigest3 = digest.MustNewDigest("beep", remoteexecution.DigestFunction_SHA256, "185f8db32271fe25f561a6fc938b2e264306ec304eda518007d1764826381969", 5)
)

func TestGetBatchAndPutBatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	digests := []digest.Digest{batchDigest1, batchDigest2, batchDigest3}

	t.Run("Fallback", func(t *testing.T) {
		// Backends that don't implement BatchBlobAccess should
		// be called for each of the objects sequentially. The
		// failure of one should not affect the others.
		blobAccess := mock.NewMockBlobAccess[string](ctrl)
		gomock.InOrder(
			blobAccess.EXPECT().Get(ctx, batchDigest1).Return("a", nil),
			blobAccess.EXPECT().Get(ctx, batchDigest2).Return("", status.Error(codes.NotFound, "Object not found")),
			blobAccess.EXPECT().Get(ctx, batchDigest3).Return("c", nil),
		)
		values, errs := blobstore.GetBatch[string](ctx, blobAccess, digests)
		require.Equal(t, []string{"a", "", "c"}, values)
		require.Len(t, errs, 3)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), errs[1])
		require.NoError(t, errs[2])

		gomock.InOrder(
			blobAccess.EXPECT().Put(ctx, batchDigest1, "a").Return(status.Error(codes.Internal, "Disk on fire")),
			blobAccess.EXPECT().Put(ctx, batchDigest2, "b"),
			blobAccess.EXPECT().Put(ctx, batchDigest3, "c"),
		)
		errs = blobstore.PutBatch[string](ctx, blobAccess, digests, []string{"a", "b", "c"})
		require.Len(t, errs, 3)
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), errs[0])
		require.NoError(t, errs[1])
		require.NoError(t, errs[2])
	})

	t.Run("Batch", func(t *testing.T) {
		// Backends that implement BatchBlobAccess should
		// receive all objects through a single call.
		blobAccess := newMockBatchBlobAccess[string](ctrl)
		blobAccess.MockBatchBlobAccess.EXPECT().GetBatch(ctx, digests).
			Return([]string{"a", "b", "c"}, []error{nil, nil, nil})
		values, errs := blobstore.GetBatch[string](ctx, blobAccess, digests)
		require.Equal(t, []string{"a", "b", "c"}, values)
		require.Equal(t, []error{nil, nil, nil}, errs)

		blobAccess.MockBatchBlobAccess.EXPECT().PutBatch(ctx, digests, []string{"a", "b", "c"}).
			Return([]error{nil, nil, nil})
		require.Equal(t, []error{nil, nil, nil}, blobstore.PutBatch[string](ctx, blobAccess, digests, []string{"a", "b", "c"}))
	})
}

func TestDecoratorsOnlyImplementBatchBlobAccessIfBaseDoes(t *testing.T) {
	ctrl := gomock.NewController(t)

	authorizer := mock.NewMockAuthorizer(ctrl)
	existenceCache := digest.NewExistenceCache(clock.SystemClock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet[string]())
	for name, newDecorator := range map[string]func(base blobstore.BlobAccess[string]) blobstore.BlobAccess[string]{
		"Authorizing": func(base blobstore.BlobAccess[string]) blobstore.BlobAccess[string] {
			return blobstore.NewAuthorizingBlobAccess(base, authorizer, authorizer, authorizer)
		},
		"ExistenceCaching": func(base blobstore.BlobAccess[string]) blobstore.BlobAccess[string] {
			return blobstore.NewExistenceCachingBlobAccess(base, existenceCache)
		},
		"Metrics": func(base blobstore.BlobAccess[string]) blobstore.BlobAccess[string] {
			return blobstore.NewMetricsBlobAccess(base, clock.SystemClock, "cs", "test")
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, ok := newDecorator(mock.NewMockBlobAccess[string](ctrl)).(blobstore.BatchBlobAccess[string])
			require.False(t, ok)
			_, ok = newDecorator(newMockBatchBlobAccess[string](ctrl)).(blobstore.BatchBlobAccess[string])
			require.True(t, ok)
		})
	}
}

func TestAuthorizingBlobAccessBatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	baseBlobAccess := newMockBatchBlobAccess[string](ctrl)
	getAuthorizer := mock.NewMockAuthorizer(ctrl)
	putAuthorizer := mock.NewMockAuthorizer(ctrl)
	findMissingAuthorizer := mock.NewMockAuthorizer(ctrl)
	blobAccess := blobstore.NewAuthorizingBlobAccess[string](baseBlobAccess, getAuthorizer, putAuthorizer, findMissingAuthorizer)

	beep := util.Must(digest.NewInstanceName("beep"))
	bopBip := util.Must(digest.NewInstanceName("bop/bip"))
	digests := []digest.Digest{batchDigest1, batchDigest2, batchDigest3}

	// The non-batch methods should remain functional.
	t.Run("Get", func(t *testing.T) {
		getAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{beep}).Return([]error{nil})
		baseBlobAccess.MockBlobAccess.EXPECT().Get(ctx, batchDigest1).Return("a", nil)
		value, err := blobAccess.Get(ctx, batchDigest1)
		require.NoError(t, err)
		require.Equal(t, "a", value)
	})

	t.Run("GetBatch-Allowed", func(t *testing.T) {
		// Every instance name should only be authorized once.
		getAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{beep, bopBip}).Return([]error{nil, nil})
		baseBlobAccess.MockBatchBlobAccess.EXPECT().GetBatch(ctx, digests).
			Return([]string{"a", "", "c"}, []error{nil, status.Error(codes.NotFound, "Object not found"), nil})

		values, errs := blobstore.GetBatch(ctx, blobAccess, digests)
		require.Equal(t, []string{"a", "", "c"}, values)
		require.Len(t, errs, 3)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), errs[1])
		require.NoError(t, errs[2])
	})

	t.Run("GetBatch-PartiallyDenied", func(t *testing.T) {
		// Objects for which access is denied should not be
		// requested from the backend. The others should.
		getAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{beep, bopBip}).
			Return([]error{nil, status.Error(codes.PermissionDenied, "You shall not pass")})
		baseBlobAccess.MockBatchBlobAccess.EXPECT().GetBatch(ctx, []digest.Digest{batchDigest1, batchDigest3}).
			Return([]string{"a", "c"}, []error{nil, nil})

		values, errs := blobstore.GetBatch(ctx, blobAccess, digests)
		require.Equal(t, []string{"a", "", "c"}, values)
		require.Len(t, errs, 3)
		require.NoError(t, errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), errs[1])
		require.NoError(t, errs[2])
	})

	t.Run("GetBatch-Denied", func(t *testing.T) {
		getAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{beep}).
			Return([]error{status.Error(codes.PermissionDenied, "You shall not pass")})

		values, errs := blobstore.GetBatch(ctx, blobAccess, []digest.Digest{batchDigest1, batchDigest3})
		require.Equal(t, []string{"", ""}, values)
		require.Len(t, errs, 2)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), errs[0])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), errs[1])
	})

	t.Run("PutBatch-Allowed", func(t *testing.T) {
		putAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{beep, bopBip}).Return([]error{nil, nil})
		baseBlobAccess.MockBatchBlobAccess.EXPECT().PutBatch(ctx, digests, []string{"a", "b", "c"}).
			Return([]error{nil, nil, status.Error(codes.Internal, "Disk on fire")})

		errs := blobstore.PutBatch(ctx, blobAccess, digests, []string{"a", "b", "c"})
		require.Len(t, errs, 3)
		require.NoError(t, errs[0])
		require.NoError(t, errs[1])
		testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), errs[2])
	})

	t.Run("PutBatch-PartiallyDenied", func(t *testing.T) {
		putAuthorizer.EXPECT().Authorize(ctx, []digest.InstanceName{beep, bopBip}).
			Return([]error{status.Error(codes.PermissionDenied, "You shall not pass"), nil})
		baseBlobAccess.MockBatchBlobAccess.EXPECT().PutBatch(ctx, []digest.Digest{batchDigest2}, []string{"b"}).
			Return([]error{nil})

		errs := blobstore.PutBatch(ctx, blobAccess, digests, []string{"a", "b", "c"})
		require.Len(t, errs, 3)
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), errs[0])
		require.NoError(t, errs[1])
		testutil.RequireEqualStatus(t, status.Error(codes.PermissionDenied, "Authorization: You shall not pass"), errs[2])
	})
}

func TestExistenceCachingAndMetricsBlobAccessBatch(t *testing.T) {
	ctrl, ctx := gomock.WithContext(context.Background(), t)

	// These decorators don't alter Get() and Put(), so batches
	// should be forwarded as is, including errors of individual
	// objects.
	baseBlobAccess := newMockBatchBlobAccess[string](ctrl)
	blobAccess := blobstore.NewMetricsBlobAccess(
		blobstore.NewExistenceCachingBlobAccess[string](
			baseBlobAccess,
			digest.NewExistenceCache(clock.SystemClock, digest.KeyWithoutInstance, 10, time.Minute, eviction.NewLRUSet[string]()),
		),
		clock.SystemClock,
		"cs",
		"test",
	)
	digests := []digest.Digest{batchDigest1, batchDigest2}

	baseBlobAccess.MockBatchBlobAccess.EXPECT().GetBatch(ctx, digests).
		Return([]string{"", "b"}, []error{status.Error(codes.NotFound, "Object not found"), nil})
	values, errs := blobstore.GetBatch(ctx, blobAccess, digests)
	require.Equal(t, []string{"", "b"}, values)
	require.Len(t, errs, 2)
	testutil.RequireEqualStatus(t, status.Error(codes.NotFound, "Object not found"), errs[0])
	require.NoError(t, errs[1])

	baseBlobAccess.MockBatchBlobAccess.EXPECT().PutBatch(ctx, digests, []string{"a", "b"}).
		Return([]error{nil, status.Error(codes.Internal, "Disk on fire")})
	errs = blobstore.PutBatch(ctx, blobAccess, digests, []string{"a", "b"})
	require.Len(t, errs, 2)
	require.NoError(t, errs[0])
	testutil.RequireEqualStatus(t, status.Error(codes.Internal, "Disk on fire"), errs[1])

	// Empty batches should not reach the backend.
	values, errs = blobstore.GetBatch(ctx, blobAccess, nil)
	require.Empty(t, values)
	require.Empty(t, errs)
	require.Empty(t, blobstore.PutBatch(ctx, blobAccess, nil, nil))
}
