package blobstore

import (
	"context"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/digest"
)

// capabilitiesProvider is a copy of the capabilities.Provider interface
// which must be written inline for go mockgen to function in source
// mode.
type capabilitiesProvider interface {
	GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error)
}

var (
	_ capabilitiesProvider  = capabilities.Provider(nil)
	_ capabilities.Provider = capabilitiesProvider(nil)
)

// BlobAccess is an abstraction for a data store that can be used to
// hold an Action Cache (AC), Chunk Storage (CS), or any other data
// store that uses keys in the form of digests.
type BlobAccess[T any] interface {
	capabilitiesProvider

	Get(ctx context.Context, digest digest.Digest) (T, error)
	Put(ctx context.Context, digest digest.Digest, value T) error
	FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error)
}

// BatchBlobAccess is an optional interface that may be implemented by
// BlobAccess backends that are capable of reading or writing multiple
// objects more efficiently than by calling Get() and Put() for each of
// them. An example is a backend that forwards requests over the
// network, as it can place multiple objects in a single request.
//
// All objects that are part of a single call are requested using the
// same context. Callers must therefore not combine objects that are
// requested on behalf of different clients into a single call, as the
// context may carry credentials, request metadata and tracing state
// that get forwarded to the backend.
//
// Both methods return one error for each of the digests provided, in
// the same order. The failure to read or write one object does not
// affect the others. Digests may be provided more than once.
//
// Decorators should only implement this interface if the backends they
// wrap do, so that callers can use a type assertion to determine
// whether batching is of any use. Callers that merely want to process
// multiple objects can use the GetBatch() and PutBatch() functions,
// which fall back to calling Get() and Put() sequentially.
type BatchBlobAccess[T any] interface {
	GetBatch(ctx context.Context, digests []digest.Digest) ([]T, []error)
	PutBatch(ctx context.Context, digests []digest.Digest, values []T) []error
}

// GetBatch reads multiple objects from a BlobAccess. If the BlobAccess
// implements BatchBlobAccess, the objects are requested as a single
// batch. If not, BlobAccess.Get() is called for each of the digests
// sequentially.
func GetBatch[T any](ctx context.Context, blobAccess BlobAccess[T], digests []digest.Digest) ([]T, []error) {
	if batchBlobAccess, ok := blobAccess.(BatchBlobAccess[T]); ok {
		return batchBlobAccess.GetBatch(ctx, digests)
	}
	values := make([]T, len(digests))
	errs := make([]error, len(digests))
	for i, digest := range digests {
		values[i], errs[i] = blobAccess.Get(ctx, digest)
	}
	return values, errs
}

// PutBatch writes multiple objects into a BlobAccess. If the BlobAccess
// implements BatchBlobAccess, the objects are written as a single
// batch. If not, BlobAccess.Put() is called for each of the objects
// sequentially.
func PutBatch[T any](ctx context.Context, blobAccess BlobAccess[T], digests []digest.Digest, values []T) []error {
	if batchBlobAccess, ok := blobAccess.(BatchBlobAccess[T]); ok {
		return batchBlobAccess.PutBatch(ctx, digests, values)
	}
	errs := make([]error, len(digests))
	for i, digest := range digests {
		errs[i] = blobAccess.Put(ctx, digest, values[i])
	}
	return errs
}

// RecommendedFindMissingDigestsCount corresponds to the maximum number
// of digests that is safe to provide to BlobAccess.FindMissing()
// without running into size limits of underlying protocols.
//
// Assuming that a typical REv2 Digest message is ~70 bytes in size,
// this will cause us to generate gRPC calls with messages of up to ~700
// KB in size. This seems like a safe limit, because most gRPC
// implementations limit the maximum message size to a small number of
// megabytes (4 MB for Java, 16 MB for Go).
//
// TODO: Would it make sense to replace BlobAccess.FindMissing() with a
// streaming API that abstracts away the maximum digests count? That way
// we can ensure proper batching of digests, even when sharding is used,
// ExistenceCachingBlobAccess has a high hit rate, etc..
//
// It would be nice if such an API also supported decomposition of large
// objects natively. See the "Future work" section in ADR#3 for details:
// https://github.com/buildbarn/bb-adrs/blob/main/0003-cas-decomposition.md#future-work
const RecommendedFindMissingDigestsCount = 10000
