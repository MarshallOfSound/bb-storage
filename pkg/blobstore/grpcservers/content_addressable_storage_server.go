package grpcservers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/auth"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/capabilities"
	"github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/buildbarn/bb-storage/pkg/zstd"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	contentAddressableStorageServerPrometheusMetrics sync.Once

	contentAddressableStorageServerZSTDFramesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "content_addressable_storage_server_zstd_frames_total",
			Help:      "Number of ZSTD compressed chunks received through BatchUpdateBlobs(), and whether the compressed data provided by the client was reused, rejected for reuse or compressed again because reuse is disabled.",
		},
		[]string{"outcome"},
	)
	contentAddressableStorageServerZSTDFramesTotalReused        = contentAddressableStorageServerZSTDFramesTotal.WithLabelValues("reused")
	contentAddressableStorageServerZSTDFramesTotalGuardRejected = contentAddressableStorageServerZSTDFramesTotal.WithLabelValues("guard_rejected")
	contentAddressableStorageServerZSTDFramesTotalReencoded     = contentAddressableStorageServerZSTDFramesTotal.WithLabelValues("reencoded")
)

type contentAddressableStorageServer struct {
	chunkStorage              blobstore.BlobAccess[*chunk.Chunk]
	chunkMappingStorage       blobstore.BlobAccess[chunk.Mapping]
	chunkMappingFetcher       chunk.MappingFetcher
	cdcParametersFetcher      capabilities.CDCParametersFetcher
	readerPutter              cas.ReaderPutter
	getChunkMappingAuthorizer auth.Authorizer
	putChunkMappingAuthorizer auth.Authorizer
	maximumMessageSizeBytes   int64
	maximumChunkCount         int
	reuseClientZSTDFrames     bool
}

// NewContentAddressableStorageServer creates a GRPC service for serving
// the contents of a Bazel Content Addressable Storage (CAS) to Bazel.
//
// If reuseClientZSTDFrames is true, ZSTD compressed chunks uploaded
// through BatchUpdateBlobs() retain the compressed data provided by the
// client, as long as it is safe to do so. This prevents the need for
// compressing chunks again when they are written into storage.
func NewContentAddressableStorageServer(chunkStorage blobstore.BlobAccess[*chunk.Chunk], chunkMappingStorage blobstore.BlobAccess[chunk.Mapping], cdcParametersFetcher capabilities.CDCParametersFetcher, readerPutter cas.ReaderPutter, getChunkMappingAuthorizer, putChunkMappingAuthorizer auth.Authorizer, maximumMessageSizeBytes int64, maximumChunkCount int, reuseClientZSTDFrames bool) remoteexecution.ContentAddressableStorageServer {
	contentAddressableStorageServerPrometheusMetrics.Do(func() {
		prometheus.MustRegister(contentAddressableStorageServerZSTDFramesTotal)
	})

	return &contentAddressableStorageServer{
		chunkStorage:              chunkStorage,
		chunkMappingStorage:       chunkMappingStorage,
		cdcParametersFetcher:      cdcParametersFetcher,
		readerPutter:              readerPutter,
		getChunkMappingAuthorizer: getChunkMappingAuthorizer,
		putChunkMappingAuthorizer: putChunkMappingAuthorizer,
		maximumMessageSizeBytes:   maximumMessageSizeBytes,
		maximumChunkCount:         maximumChunkCount,
		reuseClientZSTDFrames:     reuseClientZSTDFrames,
	}
}

func (s *contentAddressableStorageServer) FindMissingBlobs(ctx context.Context, in *remoteexecution.FindMissingBlobsRequest) (*remoteexecution.FindMissingBlobsResponse, error) {
	if len(in.BlobDigests) == 0 {
		return &remoteexecution.FindMissingBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigests[0].GetHash()))
	if err != nil {
		return nil, err
	}

	inDigests := digest.NewSetBuilder(len(in.BlobDigests))
	for _, inDigest := range in.BlobDigests {
		d, err := digestFunction.NewDigestFromProto(inDigest)
		if err != nil {
			return nil, err
		}
		inDigests.Add(d)
	}

	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}
	missing, err := cas.FindMissing(ctx, s.chunkStorage, s.chunkMappingStorage, params, inDigests.Build())
	if err != nil {
		return nil, err
	}

	outDigests := make([]*remoteexecution.Digest, 0, missing.Length())
	for _, outDigest := range missing.Items() {
		outDigests = append(outDigests, outDigest.GetProto())
	}

	return &remoteexecution.FindMissingBlobsResponse{
		MissingBlobDigests: outDigests,
	}, nil
}

// readBlobFromBatch reads a blob that is either empty or consists of
// multiple chunks. Blobs that are stored as a single chunk are read by
// BatchReadBlobs() directly.
func (s *contentAddressableStorageServer) readBlobFromBatch(ctx context.Context, blobDigest digest.Digest, params *remoteexecution.RepMaxCdcParams, compressor remoteexecution.Compressor_Value) ([]byte, error) {
	if blobDigest.GetSizeBytes() == 0 {
		switch compressor {
		case remoteexecution.Compressor_IDENTITY:
			return nil, nil
		case remoteexecution.Compressor_ZSTD:
			return []byte{0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x58, 0x01, 0x00, 0x00}, nil
		default:
			panic("Unsupported compression algorithm should not be reachable")
		}
	}
	chunkMapping, err := s.chunkMappingStorage.Get(ctx, blobDigest)
	if err != nil {
		return nil, err
	}
	instanceName := blobDigest.GetInstanceName()
	var buf bytes.Buffer
	buf.Grow(int(blobDigest.GetSizeBytes()))
	for i := range chunkMapping.Length() {
		c, err := s.chunkStorage.Get(ctx, chunkMapping.GetDigestAtIndex(instanceName, i))
		if err != nil {
			return nil, err
		}
		data, err := c.GetEncodedBytes(compressor)
		if err != nil {
			return nil, err
		}
		buf.Write(data)
	}
	return buf.Bytes(), nil
}

func (s *contentAddressableStorageServer) BatchReadBlobs(ctx context.Context, in *remoteexecution.BatchReadBlobsRequest) (*remoteexecution.BatchReadBlobsResponse, error) {
	if len(in.Digests) == 0 {
		return &remoteexecution.BatchReadBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.Digests[0].GetHash()))
	if err != nil {
		return nil, err
	}

	// TODO: Compensate for message overhead.
	bytesRemaining := s.maximumMessageSizeBytes
	digests := make([]digest.Digest, 0, len(in.Digests))
	for _, reqDigest := range in.Digests {
		digest, err := digestFunction.NewDigestFromProto(reqDigest)
		if err != nil {
			return nil, err
		}
		sizeBytes := digest.GetSizeBytes()
		if sizeBytes > bytesRemaining {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"Attempted to read a total of at least %d bytes, while a maximum of %d bytes is permitted",
				uint64(s.maximumMessageSizeBytes-bytesRemaining)+uint64(sizeBytes),
				s.maximumMessageSizeBytes,
			)
		}
		bytesRemaining -= sizeBytes
		digests = append(digests, digest)
	}

	response := &remoteexecution.BatchReadBlobsResponse{
		Responses: make([]*remoteexecution.BatchReadBlobsResponse_Response, 0, len(in.Digests)),
	}
	compressor := remoteexecution.Compressor_IDENTITY
	if slices.Contains(in.AcceptableCompressors, remoteexecution.Compressor_ZSTD) {
		compressor = remoteexecution.Compressor_ZSTD
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	// Blobs that fit in a single chunk are stored as the chunk
	// itself. Request all of them from the Chunk Storage at once,
	// so that backends that support it can fetch them using fewer
	// requests.
	var chunkDigests []digest.Digest
	for _, digest := range digests {
		if digest.GetSizeBytes() > 0 && cas.IsSingleChunk(params, digest) {
			chunkDigests = append(chunkDigests, digest)
		}
	}
	var chunks []*chunk.Chunk
	var chunkErrs []error
	if len(chunkDigests) > 0 {
		chunks, chunkErrs = blobstore.GetBatch(ctx, s.chunkStorage, chunkDigests)
	}

	for i, digest := range digests {
		var data []byte
		var err error
		if digest.GetSizeBytes() > 0 && cas.IsSingleChunk(params, digest) {
			// Chunks are immutable, so their data can be
			// placed in the response without copying it.
			if err = chunkErrs[0]; err == nil {
				data, err = chunks[0].GetEncodedBytes(compressor)
			}
			chunks, chunkErrs = chunks[1:], chunkErrs[1:]
		} else {
			data, err = s.readBlobFromBatch(ctx, digest, params, compressor)
		}
		response.Responses = append(
			response.Responses,
			&remoteexecution.BatchReadBlobsResponse_Response{
				Digest:     in.Digests[i],
				Data:       data,
				Compressor: compressor,
				Status:     status.Convert(err).Proto(),
			},
		)
	}

	return response, nil
}

func (s *contentAddressableStorageServer) BatchUpdateBlobs(ctx context.Context, in *remoteexecution.BatchUpdateBlobsRequest) (*remoteexecution.BatchUpdateBlobsResponse, error) {
	if len(in.Requests) == 0 {
		return &remoteexecution.BatchUpdateBlobsResponse{}, nil
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.Requests[0].Digest.GetHash()))
	if err != nil {
		return nil, err
	}

	response := &remoteexecution.BatchUpdateBlobsResponse{
		Responses: make([]*remoteexecution.BatchUpdateBlobsResponse_Response, 0, len(in.Requests)),
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	// Blobs that fit in a single chunk are stored as the chunk
	// itself. Instead of writing these chunks one by one, they are
	// gathered and written into the Chunk Storage at once, so that
	// backends that support it can store them using fewer requests.
	//
	// As chunks may need to be decompressed to validate them, the
	// total size of the chunks that are gathered is limited. This
	// prevents small requests from consuming large amounts of
	// memory.
	var pendingResponseIndices []int
	var pendingDigests []digest.Digest
	var pendingChunks []*chunk.Chunk
	var pendingSizeBytes int64
	flushPendingChunks := func() {
		if len(pendingDigests) == 0 {
			return
		}
		for i, err := range blobstore.PutBatch(ctx, s.chunkStorage, pendingDigests, pendingChunks) {
			if err != nil {
				response.Responses[pendingResponseIndices[i]].Status = status.Convert(util.StatusWrap(err, "Failed to save chunk")).Proto()
			}
		}
		pendingResponseIndices = nil
		pendingDigests = nil
		pendingChunks = nil
		pendingSizeBytes = 0
	}

	for _, request := range in.Requests {
		digest, err := digestFunction.NewDigestFromProto(request.Digest)
		if err == nil {
			if digest.GetSizeBytes() == 0 {
				// Empty blob is always present in storage.
				err = nil
			} else if digest.GetSizeBytes() > s.maximumMessageSizeBytes {
				err = status.Errorf(codes.InvalidArgument, "Attempted to upload blob of %d bytes but this backend only supports uploading blobs up to %d bytes", digest.GetSizeBytes(), s.maximumMessageSizeBytes)
			} else if cas.IsSingleChunk(params, digest) {
				var c *chunk.Chunk
				if c, err = s.newChunkFromRequest(digest, request.Data, request.Compressor); err == nil {
					// The status of the response will
					// be overwritten if storing the
					// chunk fails.
					if len(pendingDigests) > 0 && digest.GetSizeBytes() > s.maximumMessageSizeBytes-pendingSizeBytes {
						flushPendingChunks()
					}
					pendingResponseIndices = append(pendingResponseIndices, len(response.Responses))
					pendingDigests = append(pendingDigests, digest)
					pendingChunks = append(pendingChunks, c)
					pendingSizeBytes += digest.GetSizeBytes()
				}
			} else {
				err = s.updateBlob(ctx, digest, request.Data, request.Compressor, params)
			}
		}
		response.Responses = append(response.Responses,
			&remoteexecution.BatchUpdateBlobsResponse_Response{
				Digest: request.Digest,
				Status: status.Convert(err).Proto(),
			})
	}
	flushPendingChunks()
	return response, nil
}

// newChunkFromRequest converts the data of a blob that was provided to
// BatchUpdateBlobs() to a chunk that can be written into the Chunk
// Storage, verifying that it corresponds to the digest.
func (s *contentAddressableStorageServer) newChunkFromRequest(d digest.Digest, data []byte, compressor remoteexecution.Compressor_Value) (*chunk.Chunk, error) {
	var c *chunk.Chunk
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		c = chunk.NewChunk(data)
	case remoteexecution.Compressor_ZSTD:
		if s.reuseClientZSTDFrames {
			// Retain the compressed data provided by the
			// client, so that the chunk does not need to be
			// compressed again when stored. As the client is
			// untrusted, this is only done if the data is
			// well formed and not arbitrarily poorly
			// compressed. The digest below is still computed
			// over the decompressed data.
			var reused bool
			var err error
			if c, reused, err = chunk.NewChunkFromUntrustedEncoded(data, int(d.GetSizeBytes()), compressor); err != nil {
				return nil, util.StatusWrap(err, "Failed to decompress ZSTD data")
			}
			if reused {
				contentAddressableStorageServerZSTDFramesTotalReused.Inc()
			} else {
				contentAddressableStorageServerZSTDFramesTotalGuardRejected.Inc()
			}
		} else {
			// Don't reuse the compression, as it comes from
			// an untrusted client. Even if it represents the
			// correct blob it could still be an arbitrarily
			// poor compression.
			decompressedData, err := zstd.Decode(data, int(d.GetSizeBytes()))
			if err != nil {
				return nil, util.StatusWrap(err, "Failed to decompress ZSTD data")
			}
			c = chunk.NewChunk(decompressedData)
			contentAddressableStorageServerZSTDFramesTotalReencoded.Inc()
		}
	default:
		return nil, status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}
	if actual, err := c.ComputeDigest(d.GetDigestFunction()); err != nil {
		return nil, err
	} else if actual != d {
		return nil, status.Errorf(codes.InvalidArgument, "Blob digest mismatch, advertised %s, actual %s", d, actual)
	}
	return c, nil
}

func (s *contentAddressableStorageServer) updateBlob(ctx context.Context, d digest.Digest, data []byte, compressor remoteexecution.Compressor_Value, params *remoteexecution.RepMaxCdcParams) error {
	switch compressor {
	case remoteexecution.Compressor_IDENTITY:
		return s.readerPutter.PutReaderAt(ctx, d, bytes.NewReader(data), params)
	case remoteexecution.Compressor_ZSTD:
		decompressedData, err := zstd.Decode(data, int(d.GetSizeBytes()))
		if err != nil {
			return util.StatusWrap(err, "Failed to decompress ZSTD data")
		}
		return s.readerPutter.PutReaderAt(ctx, d, bytes.NewReader(decompressedData), params)
	default:
		return status.Errorf(codes.Unimplemented, "This service does not support uploading compression type: %s", compressor)
	}
}

func (contentAddressableStorageServer) GetTree(in *remoteexecution.GetTreeRequest, stream remoteexecution.ContentAddressableStorage_GetTreeServer) error {
	return status.Error(codes.Unimplemented, "This service does not support downloading directory trees")
}

func (s *contentAddressableStorageServer) registerChunkMapping(ctx context.Context, d digest.Digest, digests []*remoteexecution.Digest) error {
	if len(digests) < 2 {
		return status.Error(codes.Unimplemented, "This server does not implement trivial RegisterChunkMapping calls")
	}
	// Blobs that fit in a single chunk are stored as a chunk and never
	// obtain a chunk mapping, even if a client provides a decomposition
	// into multiple chunks that are all below the minimum chunk size.
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, d.GetInstanceName())
	if err != nil {
		return err
	}
	if cas.IsSingleChunk(params, d) {
		return status.Error(codes.Unimplemented, "This server does not implement RegisterChunkMapping calls for blobs that fit in a single chunk")
	}
	// Store the chunk mapping.
	chunkMapping, err := chunk.NewMappingFromProtoDigests(d.GetDigestFunction(), digests, uint64(d.GetSizeBytes()), false)
	if err != nil {
		return err
	}
	if err := s.chunkMappingStorage.Put(ctx, d, chunkMapping); err != nil {
		return util.StatusWrap(err, "Could not save chunk mapping for blob")
	}
	return nil
}

func (s *contentAddressableStorageServer) RegisterChunkMapping(stream remoteexecution.ContentAddressableStorage_RegisterChunkMappingServer) error {
	ctx := stream.Context()

	in, err := stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return status.Error(codes.InvalidArgument, "Did not receive initial message")
		}
		return err
	}
	if in.BlobDigest == nil {
		return status.Error(codes.InvalidArgument, "The first request does not contain a blob digest")
	}
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(ctx, s.putChunkMappingAuthorizer, instanceName); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	blobDigestProto := in.BlobDigest

	var chunkDigests []*remoteexecution.Digest
	for {
		chunkDigests = append(chunkDigests, in.ChunkDigests...)
		if len(chunkDigests) > s.maximumChunkCount {
			return status.Errorf(
				codes.InvalidArgument,
				"Attempted to register a blob that consists of a total of at least %d chunks, while a maximum of %d chunks is permitted",
				len(chunkDigests),
				s.maximumChunkCount,
			)
		}

		in, err = stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	blobDigest, err := digestFunction.NewDigestFromProto(blobDigestProto)
	if err != nil {
		return err
	}

	if err := s.registerChunkMapping(ctx, blobDigest, chunkDigests); err != nil {
		return err
	}

	return stream.SendAndClose(&remoteexecution.RegisterChunkMappingResponse{
		BlobDigest: blobDigestProto,
	})
}

func (s *contentAddressableStorageServer) SpliceBlob(ctx context.Context, in *remoteexecution.SpliceBlobRequest) (*remoteexecution.SpliceBlobResponse, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		return nil, err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(ctx, s.putChunkMappingAuthorizer, instanceName); err != nil {
		return nil, util.StatusWrap(err, "Authorization")
	}

	if len(in.ChunkDigests) > s.maximumChunkCount {
		return nil, status.Errorf(
			codes.InvalidArgument,
			"Attempted to splice a total of at least %d chunks, while a maximum of %d chunks is permitted",
			len(in.ChunkDigests),
			s.maximumChunkCount,
		)
	}

	if err := s.registerChunkMapping(ctx, blobDigest, in.ChunkDigests); err != nil {
		return nil, err
	}

	return &remoteexecution.SpliceBlobResponse{
		BlobDigest: in.BlobDigest,
	}, nil
}

func (s *contentAddressableStorageServer) SplitBlob(ctx context.Context, in *remoteexecution.SplitBlobRequest) (*remoteexecution.SplitBlobResponse, error) {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return nil, util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return nil, err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		return nil, err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(ctx, s.getChunkMappingAuthorizer, instanceName); err != nil {
		return nil, util.StatusWrap(err, "Authorization")
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(ctx, instanceName)
	if err != nil {
		return nil, err
	}

	if cas.IsSingleChunk(params, blobDigest) {
		return nil, status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings")
	}
	chunkMapping, err := s.chunkMappingStorage.Get(ctx, blobDigest)
	if err != nil {
		return nil, err
	}

	return &remoteexecution.SplitBlobResponse{
		ChunkDigests:     chunkMapping.GetProtoDigests(),
		ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
	}, nil
}

func (s *contentAddressableStorageServer) GetChunkMapping(in *remoteexecution.GetChunkMappingRequest, stream remoteexecution.ContentAddressableStorage_GetChunkMappingServer) error {
	instanceName, err := digest.NewInstanceName(in.InstanceName)
	if err != nil {
		return util.StatusWrapf(err, "Invalid instance name %#v", in.InstanceName)
	}
	digestFunction, err := instanceName.GetDigestFunction(in.DigestFunction, len(in.BlobDigest.GetHash()))
	if err != nil {
		return err
	}
	blobDigest, err := digestFunction.NewDigestFromProto(in.BlobDigest)
	if err != nil {
		return err
	}
	// We have to authorize manually here to prevent us from short
	// circuiting during the trivial case validation. Alternatively we
	// could skip authorizing but that would leak information about the
	// instances chunk size (Admittedly a very minor leak).
	if err := auth.AuthorizeSingleInstanceName(stream.Context(), s.getChunkMappingAuthorizer, instanceName); err != nil {
		return util.StatusWrap(err, "Authorization")
	}
	params, err := s.cdcParametersFetcher.FetchCDCParameters(stream.Context(), instanceName)
	if err != nil {
		return err
	}

	if cas.IsSingleChunk(params, blobDigest) {
		return status.Error(codes.Unimplemented, "This server does not implement trivial GetChunkMappings")
	}
	chunkMapping, err := s.chunkMappingStorage.Get(stream.Context(), blobDigest)
	if err != nil {
		return err
	}

	protoDigests := chunkMapping.GetProtoDigests()

	batchSize := int(blobstore.RecommendedFindMissingDigestsCount)
	for len(protoDigests) > 0 {
		n := min(len(protoDigests), batchSize)
		if err := stream.Send(&remoteexecution.GetChunkMappingResponse{
			ChunkDigests:     protoDigests[:n],
			ChunkingFunction: remoteexecution.ChunkingFunction_REP_MAX_CDC,
		}); err != nil {
			return err
		}
		protoDigests = protoDigests[n:]
	}
	return nil
}
