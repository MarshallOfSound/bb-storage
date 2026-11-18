package blobstore

import (
	"context"
	"slices"

	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
)

type authorizingBlobAccess[T any] struct {
	BlobAccess[T]

	getAuthorizer         auth.Authorizer
	putAuthorizer         auth.Authorizer
	findMissingAuthorizer auth.Authorizer
}

// NewAuthorizingBlobAccess creates a new BlobAccess which guards blob
// accesses by checks with Authorizers. Calls to GetCapabilities() are
// not checked, for the reason that the exact logic for this differs
// between the Action Cache (AC) and Content Addressable Storage (CAS).
//
// The resulting BlobAccess implements BatchBlobAccess if the base
// BlobAccess does.
func NewAuthorizingBlobAccess[T any](base BlobAccess[T], getAuthorizer, putAuthorizer, findMissingAuthorizer auth.Authorizer) BlobAccess[T] {
	ba := &authorizingBlobAccess[T]{
		BlobAccess:            base,
		getAuthorizer:         getAuthorizer,
		putAuthorizer:         putAuthorizer,
		findMissingAuthorizer: findMissingAuthorizer,
	}
	if batchBase, ok := base.(BatchBlobAccess[T]); ok {
		return &authorizingBatchBlobAccess[T]{
			authorizingBlobAccess: ba,
			batchBase:             batchBase,
		}
	}
	return ba
}

func (ba *authorizingBlobAccess[T]) Get(ctx context.Context, d digest.Digest) (T, error) {
	var zero T
	if err := auth.AuthorizeSingleInstanceName(ctx, ba.getAuthorizer, d.GetInstanceName()); err != nil {
		return zero, util.StatusWrap(err, "Authorization")
	}
	return ba.BlobAccess.Get(ctx, d)
}

func (ba *authorizingBlobAccess[T]) Put(ctx context.Context, d digest.Digest, value T) error {
	if err := auth.AuthorizeSingleInstanceName(ctx, ba.putAuthorizer, d.GetInstanceName()); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	return ba.BlobAccess.Put(ctx, d, value)
}

func (ba *authorizingBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	instanceNamesSet := make(map[digest.InstanceName]struct{})
	for _, digest := range digests.Items() {
		instanceNamesSet[digest.GetInstanceName()] = struct{}{}
	}
	instanceNames := make([]digest.InstanceName, 0, len(instanceNamesSet))
	for instanceName := range instanceNamesSet {
		instanceNames = append(instanceNames, instanceName)
	}

	errs := ba.findMissingAuthorizer.Authorize(ctx, instanceNames)
	for i, err := range errs {
		if err != nil {
			return digest.EmptySet, util.StatusWrapf(err, "Authorization of instance name %#v", instanceNames[i].String())
		}
	}
	return ba.BlobAccess.FindMissing(ctx, digests)
}

// authorizingBatchBlobAccess is used instead of authorizingBlobAccess
// if the base BlobAccess implements BatchBlobAccess.
type authorizingBatchBlobAccess[T any] struct {
	*authorizingBlobAccess[T]
	batchBase BatchBlobAccess[T]
}

// authorizeBatch authorizes all of the instance names that are
// referenced by a list of digests. It returns nil if all of them are
// permitted. If not, it returns one error for each of the digests.
func authorizeBatch(ctx context.Context, authorizer auth.Authorizer, digests []digest.Digest) []error {
	// Requests tend to use a single instance name, so there is no
	// need to use a map for deduplication.
	var instanceNames []digest.InstanceName
	instanceNameIndices := make([]int, 0, len(digests))
	for _, d := range digests {
		instanceName := d.GetInstanceName()
		index := slices.Index(instanceNames, instanceName)
		if index < 0 {
			index = len(instanceNames)
			instanceNames = append(instanceNames, instanceName)
		}
		instanceNameIndices = append(instanceNameIndices, index)
	}

	instanceNameErrs := authorizer.Authorize(ctx, instanceNames)
	if !slices.ContainsFunc(instanceNameErrs, func(err error) bool { return err != nil }) {
		return nil
	}
	for i, err := range instanceNameErrs {
		if err != nil {
			instanceNameErrs[i] = util.StatusWrap(err, "Authorization")
		}
	}
	errs := make([]error, 0, len(digests))
	for _, index := range instanceNameIndices {
		errs = append(errs, instanceNameErrs[index])
	}
	return errs
}

func (ba *authorizingBatchBlobAccess[T]) GetBatch(ctx context.Context, digests []digest.Digest) ([]T, []error) {
	if len(digests) == 0 {
		return nil, nil
	}
	errs := authorizeBatch(ctx, ba.getAuthorizer, digests)
	if errs == nil {
		return ba.batchBase.GetBatch(ctx, digests)
	}

	// Only forward the digests for which access is permitted.
	values := make([]T, len(digests))
	var permittedIndices []int
	var permittedDigests []digest.Digest
	for i, err := range errs {
		if err == nil {
			permittedIndices = append(permittedIndices, i)
			permittedDigests = append(permittedDigests, digests[i])
		}
	}
	if len(permittedDigests) > 0 {
		permittedValues, permittedErrs := ba.batchBase.GetBatch(ctx, permittedDigests)
		for i, index := range permittedIndices {
			values[index], errs[index] = permittedValues[i], permittedErrs[i]
		}
	}
	return values, errs
}

func (ba *authorizingBatchBlobAccess[T]) PutBatch(ctx context.Context, digests []digest.Digest, values []T) []error {
	if len(digests) == 0 {
		return nil
	}
	errs := authorizeBatch(ctx, ba.putAuthorizer, digests)
	if errs == nil {
		return ba.batchBase.PutBatch(ctx, digests, values)
	}

	// Only forward the objects for which access is permitted.
	var permittedIndices []int
	var permittedDigests []digest.Digest
	var permittedValues []T
	for i, err := range errs {
		if err == nil {
			permittedIndices = append(permittedIndices, i)
			permittedDigests = append(permittedDigests, digests[i])
			permittedValues = append(permittedValues, values[i])
		}
	}
	if len(permittedDigests) > 0 {
		permittedErrs := ba.batchBase.PutBatch(ctx, permittedDigests, permittedValues)
		for i, index := range permittedIndices {
			errs[index] = permittedErrs[i]
		}
	}
	return errs
}
