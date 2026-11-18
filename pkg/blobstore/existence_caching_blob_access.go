package blobstore

import (
	"context"

	"github.com/buildbarn/bb-storage/pkg/digest"
)

type existenceCachingBlobAccess[T any] struct {
	BlobAccess[T]
	existenceCache *digest.ExistenceCache
}

// NewExistenceCachingBlobAccess creates a decorator for BlobAccess that
// adds caching to the FindMissing() operation.
//
// Clients such as Bazel tend to frequently call
// ContentAddressableStorage.FindMissingBlobs() with overlapping sets of
// digests. They don't seem to have a local cache of which digests they
// queried recently. This decorator adds such a cache.
//
// This decorator may be useful to run on instances that act as
// frontends for a mirrored/sharding storage pool, as it may reduce the
// load observed on the storage pool.
//
// The resulting BlobAccess implements BatchBlobAccess if the base
// BlobAccess does.
func NewExistenceCachingBlobAccess[T any](base BlobAccess[T], existenceCache *digest.ExistenceCache) BlobAccess[T] {
	ba := &existenceCachingBlobAccess[T]{
		BlobAccess:     base,
		existenceCache: existenceCache,
	}
	if batchBase, ok := base.(BatchBlobAccess[T]); ok {
		// Get() and Put() are forwarded to the base BlobAccess
		// as is, so the same can be done for batches.
		return &existenceCachingBatchBlobAccess[T]{
			existenceCachingBlobAccess: ba,
			BatchBlobAccess:            batchBase,
		}
	}
	return ba
}

// existenceCachingBatchBlobAccess is used instead of
// existenceCachingBlobAccess if the base BlobAccess implements
// BatchBlobAccess.
type existenceCachingBatchBlobAccess[T any] struct {
	*existenceCachingBlobAccess[T]
	BatchBlobAccess[T]
}

func (ba *existenceCachingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Determine which digests don't need to be checked, because
	// they have already been requested recently.
	maybeMissing := ba.existenceCache.RemoveExisting(digests)

	// Check existence of the remaining digests.
	missing, err := ba.BlobAccess.FindMissing(ctx, maybeMissing)
	if err != nil {
		return digest.EmptySet, err
	}

	// Insert the digests that were present for future calls.
	present, _, _ := digest.GetDifferenceAndIntersection(maybeMissing, missing)
	ba.existenceCache.Add(present)
	return missing, nil
}
