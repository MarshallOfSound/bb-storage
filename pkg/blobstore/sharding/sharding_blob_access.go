package sharding

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"
)

type shardingBlobAccess[T any] struct {
	backends             []ShardBackend[T]
	shardSelector        ShardSelector
	getCapabilitiesRound atomic.Uint64
}

// ShardBackend is the Backend together with its key, the key is used for error
// messages.
type ShardBackend[T any] struct {
	Backend blobstore.BlobAccess[T]
	Key     string
}

// NewShardingBlobAccess is an adapter for BlobAccess that partitions
// requests across backends by hashing the digest. A ShardSelector is
// used to map hashes to backends.
//
// The resulting BlobAccess implements blobstore.BatchBlobAccess if at
// least one of the backends does. Batches are partitioned by shard,
// after which the shards are called concurrently.
func NewShardingBlobAccess[T any](backends []ShardBackend[T], shardSelector ShardSelector) blobstore.BlobAccess[T] {
	ba := &shardingBlobAccess[T]{
		backends:      backends,
		shardSelector: shardSelector,
	}
	for _, backend := range backends {
		if _, ok := backend.Backend.(blobstore.BatchBlobAccess[T]); ok {
			return &shardingBatchBlobAccess[T]{
				shardingBlobAccess: ba,
			}
		}
	}
	return ba
}

func (ba *shardingBlobAccess[T]) getBackendIndexByDigest(blobDigest digest.Digest) int {
	// Use the first 8 bytes of the digest hash for calculating backend.
	hb := blobDigest.GetHashBytes()
	h := binary.BigEndian.Uint64(hb[:8])
	return ba.shardSelector.GetShard(h)
}

func (ba *shardingBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	index := ba.getBackendIndexByDigest(digest)
	var zero T
	ret, err := ba.backends[index].Backend.Get(ctx, digest)
	if err != nil {
		return zero, util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return ret, nil
}

func (ba *shardingBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	index := ba.getBackendIndexByDigest(digest)
	if err := ba.backends[index].Backend.Put(ctx, digest, value); err != nil {
		return util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return nil
}

func (ba *shardingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Partition all digests by shard.
	digestsPerBackend := make([]digest.SetBuilder, 0, len(ba.backends))
	for range ba.backends {
		digestsPerBackend = append(digestsPerBackend, digest.NewSetBuilder(digests.Length()/len(ba.backends)+1))
	}
	for _, blobDigest := range digests.Items() {
		digestsPerBackend[ba.getBackendIndexByDigest(blobDigest)].Add(blobDigest)
	}

	// Asynchronously call FindMissing() on backends.
	missingPerBackend := make([]digest.Set, 0, len(ba.backends))
	group, ctxWithCancel := errgroup.WithContext(ctx)
	for indexIter, digestsIter := range digestsPerBackend {
		index, digests := indexIter, digestsIter
		if digests.Length() > 0 {
			missingPerBackend = append(missingPerBackend, digest.EmptySet)
			missingOut := &missingPerBackend[len(missingPerBackend)-1]
			group.Go(func() error {
				missing, err := ba.backends[index].Backend.FindMissing(ctxWithCancel, digests.Build())
				if err != nil {
					return util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
				}
				*missingOut = missing
				return nil
			})
		}
	}

	// Recombine results.
	if err := group.Wait(); err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion(missingPerBackend), nil
}

func (ba *shardingBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	// Spread requests across shards.
	index := ba.shardSelector.GetShard(ba.getCapabilitiesRound.Add(1))
	capabilities, err := ba.backends[index].Backend.GetCapabilities(ctx, instanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Shard %s", ba.backends[index].Key)
	}
	return capabilities, nil
}

// shardingBatchBlobAccess is used instead of shardingBlobAccess if one
// or more backends implement blobstore.BatchBlobAccess.
type shardingBatchBlobAccess[T any] struct {
	*shardingBlobAccess[T]
}

// shardBatch contains the objects of a batch that belong to a single
// shard.
type shardBatch[T any] struct {
	backendIndex int
	indices      []int
	digests      []digest.Digest
	values       []T
}

// partitionBatch partitions the digests of a batch by shard, preserving
// their order. If all digests belong to the same shard, a single batch
// is returned that refers to the provided slices.
func (ba *shardingBatchBlobAccess[T]) partitionBatch(digests []digest.Digest, values []T) []shardBatch[T] {
	backendIndices := make([]int, 0, len(digests))
	singleShard := true
	for _, blobDigest := range digests {
		backendIndex := ba.getBackendIndexByDigest(blobDigest)
		backendIndices = append(backendIndices, backendIndex)
		if backendIndex != backendIndices[0] {
			singleShard = false
		}
	}
	if singleShard {
		return []shardBatch[T]{{
			backendIndex: backendIndices[0],
			digests:      digests,
			values:       values,
		}}
	}

	batchIndices := make([]int, len(ba.backends))
	var batches []shardBatch[T]
	for i, backendIndex := range backendIndices {
		batchIndex := batchIndices[backendIndex] - 1
		if batchIndex < 0 {
			batchIndex = len(batches)
			batchIndices[backendIndex] = batchIndex + 1
			batches = append(batches, shardBatch[T]{backendIndex: backendIndex})
		}
		batch := &batches[batchIndex]
		batch.indices = append(batch.indices, i)
		batch.digests = append(batch.digests, digests[i])
		if values != nil {
			batch.values = append(batch.values, values[i])
		}
	}
	return batches
}

// forEachBatch calls a function for each of the batches. If there are
// multiple, they are processed concurrently.
func forEachBatch[T any](batches []shardBatch[T], f func(batch *shardBatch[T])) {
	var wg sync.WaitGroup
	for i := 1; i < len(batches); i++ {
		batch := &batches[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(batch)
		}()
	}
	f(&batches[0])
	wg.Wait()
}

func (ba *shardingBatchBlobAccess[T]) wrapBatchErrors(batch *shardBatch[T], errs []error) {
	for i, err := range errs {
		if err != nil {
			errs[i] = util.StatusWrapf(err, "Shard %s", ba.backends[batch.backendIndex].Key)
		}
	}
}

func (ba *shardingBatchBlobAccess[T]) GetBatch(ctx context.Context, digests []digest.Digest) ([]T, []error) {
	if len(digests) == 0 {
		return nil, nil
	}
	batches := ba.partitionBatch(digests, nil)
	if len(batches) == 1 {
		batch := &batches[0]
		values, errs := blobstore.GetBatch(ctx, ba.backends[batch.backendIndex].Backend, digests)
		ba.wrapBatchErrors(batch, errs)
		return values, errs
	}

	values := make([]T, len(digests))
	errs := make([]error, len(digests))
	forEachBatch(batches, func(batch *shardBatch[T]) {
		batchValues, batchErrs := blobstore.GetBatch(ctx, ba.backends[batch.backendIndex].Backend, batch.digests)
		ba.wrapBatchErrors(batch, batchErrs)
		for i, index := range batch.indices {
			values[index], errs[index] = batchValues[i], batchErrs[i]
		}
	})
	return values, errs
}

func (ba *shardingBatchBlobAccess[T]) PutBatch(ctx context.Context, digests []digest.Digest, values []T) []error {
	if len(digests) == 0 {
		return nil
	}
	batches := ba.partitionBatch(digests, values)
	if len(batches) == 1 {
		batch := &batches[0]
		errs := blobstore.PutBatch(ctx, ba.backends[batch.backendIndex].Backend, digests, values)
		ba.wrapBatchErrors(batch, errs)
		return errs
	}

	errs := make([]error, len(digests))
	forEachBatch(batches, func(batch *shardBatch[T]) {
		batchErrs := blobstore.PutBatch(ctx, ba.backends[batch.backendIndex].Backend, batch.digests, batch.values)
		ba.wrapBatchErrors(batch, batchErrs)
		for i, index := range batch.indices {
			errs[index] = batchErrs[i]
		}
	})
	return errs
}
