package blobstore

import (
	"context"
	"sync"
	"time"

	remoteexecution "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/clock"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/prometheus/client_golang/prometheus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	blobAccessOperationsPrometheusMetrics sync.Once

	blobAccessOperationsBlobSizeBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_blob_size_bytes",
			Help:      "Size of blobs being inserted/retrieved, in bytes. Only for the Chunk Store (CS) does this correspond to the actual size of the blobs inserted/retrieved, and even then compression is not taken into consideration for this.",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 33),
		},
		[]string{"storage_type", "backend_type", "operation"},
	)
	blobAccessOperationsFindMissingBatchSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_find_missing_batch_size",
			Help:      "Number of digests provided to FindMissing().",
			Buckets:   prometheus.ExponentialBuckets(1.0, 2.0, 17),
		},
		[]string{"storage_type", "backend_type"},
	)
	blobAccessOperationsDurationSeconds = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "buildbarn",
			Subsystem: "blobstore",
			Name:      "blob_access_operations_duration_seconds",
			Help:      "Amount of time spent per operation on blob access objects, in seconds.",
			Buckets:   util.DecimalExponentialBuckets(-3, 6, 2),
		},
		[]string{"storage_type", "backend_type", "operation", "grpc_code"},
	)
)

type metricsBlobAccess[T any] struct {
	blobAccess BlobAccess[T]
	clock      clock.Clock

	getBlobSizeBytes           prometheus.Observer
	getDurationSeconds         prometheus.ObserverVec
	putBlobSizeBytes           prometheus.Observer
	putDurationSeconds         prometheus.ObserverVec
	getBatchDurationSeconds    prometheus.ObserverVec
	putBatchDurationSeconds    prometheus.ObserverVec
	findMissingBatchSize       prometheus.Observer
	findMissingDurationSeconds prometheus.ObserverVec
	getCapabilitiesSeconds     prometheus.ObserverVec
}

// NewMetricsBlobAccess creates an adapter for BlobAccess that adds
// basic instrumentation in the form of Prometheus metrics.
//
// The resulting BlobAccess implements BatchBlobAccess if the provided
// BlobAccess does. Batches are reported as operations "GetBatch" and
// "PutBatch", so that their durations don't get mixed up with those of
// individual objects.
func NewMetricsBlobAccess[T any](blobAccess BlobAccess[T], clock clock.Clock, storageType, backendType string) BlobAccess[T] {
	blobAccessOperationsPrometheusMetrics.Do(func() {
		prometheus.MustRegister(blobAccessOperationsBlobSizeBytes)
		prometheus.MustRegister(blobAccessOperationsFindMissingBatchSize)
		prometheus.MustRegister(blobAccessOperationsDurationSeconds)
	})

	ba := &metricsBlobAccess[T]{
		blobAccess: blobAccess,
		clock:      clock,

		getBlobSizeBytes:           blobAccessOperationsBlobSizeBytes.WithLabelValues(storageType, backendType, "Get"),
		getDurationSeconds:         blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "Get"}),
		putBlobSizeBytes:           blobAccessOperationsBlobSizeBytes.WithLabelValues(storageType, backendType, "Put"),
		putDurationSeconds:         blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "Put"}),
		getBatchDurationSeconds:    blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "GetBatch"}),
		putBatchDurationSeconds:    blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "PutBatch"}),
		findMissingBatchSize:       blobAccessOperationsFindMissingBatchSize.WithLabelValues(storageType, backendType),
		findMissingDurationSeconds: blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "FindMissing"}),
		getCapabilitiesSeconds:     blobAccessOperationsDurationSeconds.MustCurryWith(map[string]string{"storage_type": storageType, "backend_type": backendType, "operation": "GetCapabilities"}),
	}
	if batchBlobAccess, ok := blobAccess.(BatchBlobAccess[T]); ok {
		return &metricsBatchBlobAccess[T]{
			metricsBlobAccess: ba,
			batchBlobAccess:   batchBlobAccess,
		}
	}
	return ba
}

func (ba *metricsBlobAccess[T]) updateDurationSeconds(vec prometheus.ObserverVec, code codes.Code, timeStart time.Time) {
	vec.WithLabelValues(code.String()).Observe(ba.clock.Now().Sub(timeStart).Seconds())
}

func (ba *metricsBlobAccess[T]) Get(ctx context.Context, digest digest.Digest) (T, error) {
	timeStart := ba.clock.Now()
	ba.getBlobSizeBytes.Observe(float64(digest.GetSizeBytes()))
	ret, err := ba.blobAccess.Get(ctx, digest)
	ba.updateDurationSeconds(ba.getDurationSeconds, status.Code(err), timeStart)
	return ret, err
}

func (ba *metricsBlobAccess[T]) Put(ctx context.Context, digest digest.Digest, value T) error {
	timeStart := ba.clock.Now()
	ba.putBlobSizeBytes.Observe(float64(digest.GetSizeBytes()))
	err := ba.blobAccess.Put(ctx, digest, value)
	ba.updateDurationSeconds(ba.putDurationSeconds, status.Code(err), timeStart)
	return err
}

func (ba *metricsBlobAccess[T]) FindMissing(ctx context.Context, digests digest.Set) (digest.Set, error) {
	// Discard zero-sized FindMissing() requests. These may, for
	// example, be generated by SizeDistinguishingBlobAccess. Such
	// calls would skew the batch size and duration metrics.
	if digests.Empty() {
		return digest.EmptySet, nil
	}

	ba.findMissingBatchSize.Observe(float64(digests.Length()))
	timeStart := ba.clock.Now()
	digests, err := ba.blobAccess.FindMissing(ctx, digests)
	ba.updateDurationSeconds(ba.findMissingDurationSeconds, status.Code(err), timeStart)
	return digests, err
}

func (ba *metricsBlobAccess[T]) GetCapabilities(ctx context.Context, instanceName digest.InstanceName) (*remoteexecution.ServerCapabilities, error) {
	timeStart := ba.clock.Now()
	capabilities, err := ba.blobAccess.GetCapabilities(ctx, instanceName)
	ba.updateDurationSeconds(ba.getCapabilitiesSeconds, status.Code(err), timeStart)
	return capabilities, err
}

// metricsBatchBlobAccess is used instead of metricsBlobAccess if the
// underlying BlobAccess implements BatchBlobAccess.
type metricsBatchBlobAccess[T any] struct {
	*metricsBlobAccess[T]
	batchBlobAccess BatchBlobAccess[T]
}

// getBatchCode returns the status code that is reported for a batch as
// a whole: that of the first object that failed.
func getBatchCode(errs []error) codes.Code {
	for _, err := range errs {
		if err != nil {
			return status.Code(err)
		}
	}
	return codes.OK
}

func (ba *metricsBatchBlobAccess[T]) GetBatch(ctx context.Context, digests []digest.Digest) ([]T, []error) {
	if len(digests) == 0 {
		return nil, nil
	}
	timeStart := ba.clock.Now()
	for _, digest := range digests {
		ba.getBlobSizeBytes.Observe(float64(digest.GetSizeBytes()))
	}
	values, errs := ba.batchBlobAccess.GetBatch(ctx, digests)
	ba.updateDurationSeconds(ba.getBatchDurationSeconds, getBatchCode(errs), timeStart)
	return values, errs
}

func (ba *metricsBatchBlobAccess[T]) PutBatch(ctx context.Context, digests []digest.Digest, values []T) []error {
	if len(digests) == 0 {
		return nil
	}
	timeStart := ba.clock.Now()
	for _, digest := range digests {
		ba.putBlobSizeBytes.Observe(float64(digest.GetSizeBytes()))
	}
	errs := ba.batchBlobAccess.PutBatch(ctx, digests, values)
	ba.updateDurationSeconds(ba.putBatchDurationSeconds, getBatchCode(errs), timeStart)
	return errs
}
