package grpcclients

import (
	"context"
	"slices"
	"sync/atomic"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"

	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	// The maximum size of a response message that the gRPC client
	// is willing to accept by default. Responses of BatchReadBlobs()
	// need to stay below this limit, regardless of what the server
	// permits.
	csBlobAccessMaximumResponseSizeBytes = 4 * 1024 * 1024
	// Upper bound of the amount of space consumed by an entry in
	// BatchReadBlobsResponse and BatchUpdateBlobsRequest, in
	// addition to the data of the blob: the digest, the compressor,
	// the status and any framing.
	csBlobAccessEntryOverheadBytes = 256
)

// csServerProperties contains the properties of the server that were
// obtained through GetCapabilities() and that affect how requests are
// sent to it.
type csServerProperties struct {
	supportedCompressors   []remoteexecution.Compressor_Value
	maxBatchTotalSizeBytes int64
}

type csBlobAccess struct {
	contentAddressableStorageClient remoteexecution.ContentAddressableStorageClient
	capabilitiesClient              remoteexecution.CapabilitiesClient
	serverProperties                atomic.Pointer[csServerProperties]
	preferCompression               bool
	skipDigestVerification          bool
}

var _ blobstore.BatchBlobAccess[*chunk.Chunk] = (*csBlobAccess)(nil)

// NewCSBlobAccess creates a BlobAccess handle that relays any requests
// to a gRPC service that implements the
// remoteexecution.ContentAddressableStorage services. Those are the
// services that Bazel uses to access blobs stored in the Content
// Addressable Storage.
//
// If preferCompression is true, the client will use ZSTD compression
// for operations if the server supports it.
//
// If skipDigestVerification is true, chunks returned by the server are
// only checked against their expected size, not rehashed. Compressed
// chunks are furthermore only decompressed when their uncompressed
// contents are requested. This should only be used if the server is
// trusted to have verified the chunks itself, and the connection to it
// is integrity protected.
//
// The resulting BlobAccess implements blobstore.BatchBlobAccess. If
// the server announces 'max_batch_total_size_bytes' as part of its
// capabilities, multiple chunks are placed in a single BatchReadBlobs
// or BatchUpdateBlobs request, as long as the request and response stay
// below that size. If the server does not announce a limit, every chunk
// is placed in a request of its own, identical to calling Get() and
// Put() for each of them.
func NewCSBlobAccess(client grpc.ClientConnInterface, preferCompression, skipDigestVerification bool) blobstore.BlobAccess[*chunk.Chunk] {
	return &csBlobAccess{
		contentAddressableStorageClient: remoteexecution.NewContentAddressableStorageClient(client),
		capabilitiesClient:              remoteexecution.NewCapabilitiesClient(client),
		preferCompression:               preferCompression,
		skipDigestVerification:          skipDigestVerification,
	}
}

// getServerProperties returns the properties of the server that were
// obtained through GetCapabilities(), calling it if this has not been
// done before.
func (ba *csBlobAccess) getServerProperties(ctx context.Context, instanceName digest.InstanceName) (*csServerProperties, error) {
	if serverProperties := ba.serverProperties.Load(); serverProperties != nil {
		return serverProperties, nil
	}
	// GetCapabilities() will populate the server properties atomic.
	if _, err := ba.GetCapabilities(ctx, instanceName); err != nil {
		return nil, err
	}
	return ba.serverProperties.Load(), nil
}

// shouldUseZSTDCompression checks if ZSTD compression should be used.
// It ensures GetCapabilities has been called to negotiate compression support.
func (ba *csBlobAccess) shouldUseZSTDCompression(ctx context.Context, digest digest.Digest) (bool, error) {
	if !ba.preferCompression {
		return false, nil
	}
	serverProperties, err := ba.getServerProperties(ctx, digest.GetInstanceName())
	if err != nil {
		return false, err
	}
	return slices.Contains(serverProperties.supportedCompressors, remoteexecution.Compressor_ZSTD), nil
}

// getBatchParameters returns the compressor to use for a batch of
// chunks and the maximum total size of a batch. A maximum size of zero
// indicates that every chunk needs to be placed in a request of its
// own.
func (ba *csBlobAccess) getBatchParameters(ctx context.Context, instanceName digest.InstanceName) (remoteexecution.Compressor_Value, int64, error) {
	serverProperties, err := ba.getServerProperties(ctx, instanceName)
	if err != nil {
		if ba.preferCompression {
			return remoteexecution.Compressor_IDENTITY, 0, err
		}
		// Get() and Put() don't depend on the capabilities of
		// the server if compression is disabled. Make sure that
		// also applies to batches.
		return remoteexecution.Compressor_IDENTITY, 0, nil
	}
	compressor := remoteexecution.Compressor_IDENTITY
	if ba.preferCompression && slices.Contains(serverProperties.supportedCompressors, remoteexecution.Compressor_ZSTD) {
		compressor = remoteexecution.Compressor_ZSTD
	}
	return compressor, serverProperties.maxBatchTotalSizeBytes, nil
}

func (ba *csBlobAccess) Get(ctx context.Context, digest digest.Digest) (*chunk.Chunk, error) {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		return nil, err
	}

	compressor := remoteexecution.Compressor_IDENTITY
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
	}

	digestFunction := digest.GetDigestFunction()
	req := &remoteexecution.BatchReadBlobsRequest{
		InstanceName:          digest.GetInstanceName().String(),
		Digests:               []*remoteexecution.Digest{digest.GetProto()},
		AcceptableCompressors: []remoteexecution.Compressor_Value{compressor},
		DigestFunction:        digestFunction.GetEnumValue(),
	}

	resp, err := ba.contentAddressableStorageClient.BatchReadBlobs(ctx, req)
	if err != nil {
		return nil, err
	}

	if len(resp.Responses) != 1 {
		return nil, status.Errorf(codes.Internal, "Expected 1 response, got %d", len(resp.Responses))
	}

	r := resp.Responses[0]
	if err := status.ErrorProto(r.Status); err != nil {
		return nil, err
	}

	return ba.newChunkFromResponse(digest, r)
}

// newChunkFromResponse converts the data of a chunk that was returned
// by BatchReadBlobs() to a chunk, verifying that it corresponds to the
// digest that was requested.
func (ba *csBlobAccess) newChunkFromResponse(digest digest.Digest, r *remoteexecution.BatchReadBlobsResponse_Response) (*chunk.Chunk, error) {
	if ba.skipDigestVerification {
		// The server is trusted to have verified the chunk. Only
		// validate its size, and don't decompress it until
		// somebody requests the uncompressed data. This allows
		// compressed chunks to be relayed to clients as is.
		c, err := chunk.NewChunkFromTrustedEncoded(r.Data, int(digest.GetSizeBytes()), r.Compressor)
		if err != nil {
			return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to decode upstream response")
		}
		return c, nil
	}

	c, err := chunk.NewChunkFromEncoded(r.Data, int(digest.GetSizeBytes()), r.Compressor)
	if err != nil {
		return nil, util.StatusWrapWithCode(err, codes.Internal, "Failed to decode upstream response")
	}

	if actualDigest, err := c.ComputeDigest(digest.GetDigestFunction()); err != nil {
		return nil, err
	} else if actualDigest != digest {
		return nil, status.Errorf(codes.Internal, "Digest mismatch, expected %s, got %s", digest.String(), actualDigest.String())
	}

	return c, nil
}

func (ba *csBlobAccess) Put(ctx context.Context, digest digest.Digest, value *chunk.Chunk) error {
	useCompression, err := ba.shouldUseZSTDCompression(ctx, digest)
	if err != nil {
		return err
	}

	compressor := remoteexecution.Compressor_IDENTITY
	if useCompression {
		compressor = remoteexecution.Compressor_ZSTD
	}
	data, err := value.GetEncodedBytes(compressor)
	if err != nil {
		return err
	}

	req := &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName:   digest.GetInstanceName().String(),
		DigestFunction: digest.GetDigestFunction().GetEnumValue(),
		Requests: []*remoteexecution.BatchUpdateBlobsRequest_Request{
			{Digest: digest.GetProto(), Data: data, Compressor: compressor},
		},
	}

	resp, err := ba.contentAddressableStorageClient.BatchUpdateBlobs(ctx, req)
	if err != nil {
		return err
	}
	if len(resp.Responses) != 1 {
		return status.Errorf(codes.Internal, "Expected 1 response, got %d", len(resp.Responses))
	}
	r := resp.Responses[0]
	if r.GetStatus().GetCode() != int32(codes.OK) {
		return status.ErrorProto(r.Status)
	}
	return nil
}

// csBatchSplitter is used to split up a list of chunks that use the same
// digest function into batches whose total size stays below a limit.
type csBatchSplitter struct {
	maximumSizeBytes int64
	sizeBytes        int64
	count            int
}

// add a chunk whose entry in the request or response consumes a given
// amount of space. It returns true if the chunks that were added
// previously need to be sent first, as the new chunk would cause the
// limit to be exceeded. Chunks that exceed the limit by themselves end
// up in a batch of their own, so that they behave the same as when
// requested individually.
func (s *csBatchSplitter) add(entrySizeBytes int64) bool {
	if s.count > 0 && entrySizeBytes > s.maximumSizeBytes-s.sizeBytes {
		s.sizeBytes = entrySizeBytes
		s.count = 1
		return true
	}
	s.sizeBytes += entrySizeBytes
	s.count++
	return false
}

// isBatchTooLargeError returns true if a batch may have been rejected
// by the server or the transport for the reason that it is too large.
// This should not happen if the server reports its limits accurately,
// but can occur if the capabilities that were obtained are out of date.
func isBatchTooLargeError(err error) bool {
	code := status.Code(err)
	return code == codes.InvalidArgument || code == codes.ResourceExhausted
}

// forEachDigestFunction calls a function for each maximal run of
// digests that use the same instance name and digest function, as these
// cannot be combined into a single request.
func forEachDigestFunction(digests []digest.Digest, f func(start, end int)) {
	for start := 0; start < len(digests); {
		end := start + 1
		for end < len(digests) && digests[end].GetDigestFunction() == digests[start].GetDigestFunction() {
			end++
		}
		f(start, end)
		start = end
	}
}

func setErrors(errs []error, err error) {
	for i := range errs {
		errs[i] = err
	}
}

func (ba *csBlobAccess) GetBatch(ctx context.Context, digests []digest.Digest) ([]*chunk.Chunk, []error) {
	chunks := make([]*chunk.Chunk, len(digests))
	errs := make([]error, len(digests))
	forEachDigestFunction(digests, func(start, end int) {
		compressor, maximumSizeBytes, err := ba.getBatchParameters(ctx, digests[start].GetInstanceName())
		if err != nil {
			setErrors(errs[start:end], err)
			return
		}

		// The server limits the total size of the chunks
		// requested. The response containing them also needs to
		// be small enough for us to accept it.
		splitter := csBatchSplitter{
			maximumSizeBytes: min(maximumSizeBytes, csBlobAccessMaximumResponseSizeBytes),
		}
		batchStart := start
		for i := start; i < end; i++ {
			// Compressed chunks may be slightly larger than
			// the data they contain.
			sizeBytes := digests[i].GetSizeBytes()
			if splitter.add(sizeBytes + sizeBytes/128 + csBlobAccessEntryOverheadBytes) {
				ba.getBatch(ctx, compressor, digests[batchStart:i], chunks[batchStart:i], errs[batchStart:i])
				batchStart = i
			}
		}
		ba.getBatch(ctx, compressor, digests[batchStart:end], chunks[batchStart:end], errs[batchStart:end])
	})
	return chunks, errs
}

// getBatch reads one or more chunks that use the same digest function
// through a single call to BatchReadBlobs().
func (ba *csBlobAccess) getBatch(ctx context.Context, compressor remoteexecution.Compressor_Value, digests []digest.Digest, chunks []*chunk.Chunk, errs []error) {
	protoDigests := make([]*remoteexecution.Digest, 0, len(digests))
	for _, digest := range digests {
		protoDigests = append(protoDigests, digest.GetProto())
	}
	digestFunction := digests[0].GetDigestFunction()
	resp, err := ba.contentAddressableStorageClient.BatchReadBlobs(ctx, &remoteexecution.BatchReadBlobsRequest{
		InstanceName:          digestFunction.GetInstanceName().String(),
		Digests:               protoDigests,
		AcceptableCompressors: []remoteexecution.Compressor_Value{compressor},
		DigestFunction:        digestFunction.GetEnumValue(),
	})
	if err != nil {
		if len(digests) > 1 && isBatchTooLargeError(err) {
			for i := range digests {
				ba.getBatch(ctx, compressor, digests[i:i+1], chunks[i:i+1], errs[i:i+1])
			}
			return
		}
		setErrors(errs, err)
		return
	}

	matchBatchResponses(
		protoDigests,
		len(resp.Responses),
		func(i int) *remoteexecution.Digest { return resp.Responses[i].Digest },
		func(i, responseIndex int) {
			if responseIndex < 0 {
				errs[i] = status.Errorf(codes.Internal, "Upstream server did not return a response for blob %s", digests[i])
				return
			}
			r := resp.Responses[responseIndex]
			if err := status.ErrorProto(r.Status); err != nil {
				errs[i] = err
				return
			}
			chunks[i], errs[i] = ba.newChunkFromResponse(digests[i], r)
		},
	)
}

// matchBatchResponses determines which response of a BatchReadBlobs()
// or BatchUpdateBlobs() call corresponds to each of the blobs that were
// part of the request. Servers tend to return responses in the same
// order as the request, but are not required to do so.
func matchBatchResponses(requestDigests []*remoteexecution.Digest, responseCount int, getResponseDigest func(responseIndex int) *remoteexecution.Digest, f func(requestIndex, responseIndex int)) {
	matches := func(requestIndex, responseIndex int) bool {
		requestDigest, responseDigest := requestDigests[requestIndex], getResponseDigest(responseIndex)
		return requestDigest.Hash == responseDigest.GetHash() && requestDigest.SizeBytes == responseDigest.GetSizeBytes()
	}

	// Fast path: responses are in the same order as the request.
	if responseCount == len(requestDigests) {
		inOrder := true
		for i := range requestDigests {
			if !matches(i, i) {
				inOrder = false
				break
			}
		}
		if inOrder {
			for i := range requestDigests {
				f(i, i)
			}
			return
		}
	}

	// Slow path: look up responses by digest. Requests may contain
	// the same digest more than once, in which case any response
	// for that digest suffices.
	type digestKey struct {
		hash      string
		sizeBytes int64
	}
	responseIndices := make(map[digestKey]int, responseCount)
	for responseIndex := 0; responseIndex < responseCount; responseIndex++ {
		responseDigest := getResponseDigest(responseIndex)
		responseIndices[digestKey{hash: responseDigest.GetHash(), sizeBytes: responseDigest.GetSizeBytes()}] = responseIndex
	}
	for i, requestDigest := range requestDigests {
		if responseIndex, ok := responseIndices[digestKey{hash: requestDigest.Hash, sizeBytes: requestDigest.SizeBytes}]; ok {
			f(i, responseIndex)
		} else {
			f(i, -1)
		}
	}
}

func (ba *csBlobAccess) PutBatch(ctx context.Context, digests []digest.Digest, values []*chunk.Chunk) []error {
	errs := make([]error, len(digests))
	forEachDigestFunction(digests, func(start, end int) {
		compressor, maximumSizeBytes, err := ba.getBatchParameters(ctx, digests[start].GetInstanceName())
		if err != nil {
			setErrors(errs[start:end], err)
			return
		}

		// The server rejects requests that are larger than its
		// maximum message size. What matters is the size of the
		// chunks in the encoding that is sent to the server,
		// which may differ from the one in which we received
		// them.
		splitter := csBatchSplitter{
			maximumSizeBytes: maximumSizeBytes - int64(len(digests[start].GetInstanceName().String())) - csBlobAccessEntryOverheadBytes,
		}
		digestFunction := digests[start].GetDigestFunction()
		requests := make([]*remoteexecution.BatchUpdateBlobsRequest_Request, 0, end-start)
		requestIndices := make([]int, 0, end-start)
		flush := func() {
			if len(requests) > 0 {
				for i, err := range ba.putBatch(ctx, digestFunction, requests) {
					errs[requestIndices[i]] = err
				}
				requests = requests[:0]
				requestIndices = requestIndices[:0]
			}
		}
		for i := start; i < end; i++ {
			data, err := values[i].GetEncodedBytes(compressor)
			if err != nil {
				errs[i] = err
				continue
			}
			if splitter.add(int64(len(data)) + csBlobAccessEntryOverheadBytes) {
				flush()
			}
			requests = append(requests, &remoteexecution.BatchUpdateBlobsRequest_Request{
				Digest:     digests[i].GetProto(),
				Data:       data,
				Compressor: compressor,
			})
			requestIndices = append(requestIndices, i)
		}
		flush()
	})
	return errs
}

// putBatch writes one or more chunks that use the same digest function
// through a single call to BatchUpdateBlobs().
func (ba *csBlobAccess) putBatch(ctx context.Context, digestFunction digest.Function, requests []*remoteexecution.BatchUpdateBlobsRequest_Request) []error {
	errs := make([]error, len(requests))
	resp, err := ba.contentAddressableStorageClient.BatchUpdateBlobs(ctx, &remoteexecution.BatchUpdateBlobsRequest{
		InstanceName:   digestFunction.GetInstanceName().String(),
		DigestFunction: digestFunction.GetEnumValue(),
		Requests:       requests,
	})
	if err != nil {
		if len(requests) > 1 && isBatchTooLargeError(err) {
			for i := range requests {
				errs[i] = ba.putBatch(ctx, digestFunction, requests[i:i+1])[0]
			}
			return errs
		}
		setErrors(errs, err)
		return errs
	}

	protoDigests := make([]*remoteexecution.Digest, 0, len(requests))
	for _, request := range requests {
		protoDigests = append(protoDigests, request.Digest)
	}
	matchBatchResponses(
		protoDigests,
		len(resp.Responses),
		func(i int) *remoteexecution.Digest { return resp.Responses[i].Digest },
		func(i, responseIndex int) {
			if responseIndex < 0 {
				errs[i] = status.Errorf(codes.Internal, "Upstream server did not return a response for blob %s-%d", protoDigests[i].Hash, protoDigests[i].SizeBytes)
			} else {
				errs[i] = status.ErrorProto(resp.Responses[responseIndex].Status)
			}
		},
	)
	return errs
}

func (ba *csBlobAccess) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	return findMissingBlobsInternal(ctx, digests, ba.contentAddressableStorageClient)
}

func findMissingBlobsInternal(ctx context.Context, digests digest.Set, cas remoteexecution.ContentAddressableStorageClient) (digest.Set, error) {
	type findMissingBatch struct {
		digestFunction digest.Function
		blobDigests    []*remoteexecution.Digest
	}

	// Partition all digests by digest function, as the
	// FindMissingBlobs() RPC can only process digests for a single
	// instance name and digest function. Partition each of the
	// resulting lists into batches as well, so that the request
	// messages stay below the maximum message size.
	perFunctionDigests := map[digest.Function][]*remoteexecution.Digest{}
	for _, digest := range digests.Items() {
		digestFunction := digest.GetDigestFunction()
		perFunctionDigests[digestFunction] = append(perFunctionDigests[digestFunction], digest.GetProto())
	}
	var batches []findMissingBatch
	for digestFunction, blobDigests := range perFunctionDigests {
		for len(blobDigests) > 0 {
			n := min(len(blobDigests), blobstore.RecommendedFindMissingDigestsCount)
			batches = append(batches, findMissingBatch{
				digestFunction: digestFunction,
				blobDigests:    blobDigests[:n],
			})
			blobDigests = blobDigests[n:]
		}
	}

	// Call FindMissingBlobs() for all batches concurrently.
	missingPerBatch := make([]digest.Set, len(batches))
	group, groupCtx := errgroup.WithContext(ctx)
	for indexIter, batchIter := range batches {
		index, batch := indexIter, batchIter
		group.Go(func() error {
			response, err := cas.FindMissingBlobs(groupCtx, &remoteexecution.FindMissingBlobsRequest{
				InstanceName:   batch.digestFunction.GetInstanceName().String(),
				BlobDigests:    batch.blobDigests,
				DigestFunction: batch.digestFunction.GetEnumValue(),
			})
			if err != nil {
				return err
			}

			// Convert results back.
			missingDigests := digest.NewSetBuilder(len(response.MissingBlobDigests))
			for _, proto := range response.MissingBlobDigests {
				blobDigest, err := batch.digestFunction.NewDigestFromProto(proto)
				if err != nil {
					return err
				}
				missingDigests.Add(blobDigest)
			}
			missingPerBatch[index] = missingDigests.Build()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return digest.EmptySet, err
	}
	return digest.GetUnion(missingPerBatch), nil
}

func (ba *csBlobAccess) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	serverCapabilities, err := getServerCapabilitiesWithCacheCapabilities(ctx, ba.capabilitiesClient, instanceName)
	if err != nil {
		return nil, err
	}

	cacheCapabilities := serverCapabilities.CacheCapabilities

	// Store supported compressors for compression negotiation, and
	// the maximum batch size for GetBatch() and PutBatch().
	ba.serverProperties.Store(&csServerProperties{
		supportedCompressors:   cacheCapabilities.SupportedCompressors,
		maxBatchTotalSizeBytes: cacheCapabilities.MaxBatchTotalSizeBytes,
	})

	if !cacheCapabilities.SplitBlobSupport {
		return nil, status.Errorf(codes.Internal, "Upstream server does not support split blob requests")
	}
	if !cacheCapabilities.SpliceBlobSupport {
		return nil, status.Errorf(codes.Internal, "Upstream server does not support splice blob requests")
	}
	repMaxCdcParams := cacheCapabilities.RepMaxCdcParams
	if repMaxCdcParams == nil {
		return nil, status.Errorf(codes.Internal, "Upstream server does not support RepMaxCDC")
	}

	// Only return fields that pertain to the Content Addressable
	// Storage. Don't set 'max_batch_total_size_bytes', as that is
	// a property of the server that is called by us, not of the
	// one that provides these capabilities to its own clients.
	return &remoteexecution.ServerCapabilities{
		CacheCapabilities: &remoteexecution.CacheCapabilities{
			DigestFunctions: digest.RemoveUnsupportedDigestFunctions(cacheCapabilities.DigestFunctions),
			RepMaxCdcParams: cacheCapabilities.RepMaxCdcParams,
		},
		DeprecatedApiVersion: serverCapabilities.DeprecatedApiVersion,
		LowApiVersion:        serverCapabilities.LowApiVersion,
		HighApiVersion:       serverCapabilities.HighApiVersion,
	}, nil
}
