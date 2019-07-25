package metrics

import (
	"context"
	"github.com/btcsuite/btcutil/base58"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
	"time"
)

// Keys
var (
	keyLocalId, _     = tag.NewKey("local_dht_id")
	keyBucketIndex, _ = tag.NewKey("bucket_index")
)

// Constants for measure names are exported and can be used to
// fetch views from the `DefaultViews` map.
const (
	MeasureBucketsFull              = "libp2p.io/dht/k-bucket/full_buckets"
	MeasureBucketsNonEmpty          = "libp2p.io/dht/k-bucket/non_empty_buckets"
	MeasureBucketUtilization        = "libp2p.io/dht/k-bucket/utilization"
	MeasurePeerLatency              = "libp2p.io/dht/k-bucket/peer_latency"
	MeasurePeersAdded               = "libp2p.io/dht/k-bucket/peers_added"
	MeasurePeersRejectedHighLatency = "libp2p.io/dht/k-bucket/peers_rejected_high_latency"
	MeasurePeersRejectedNoCapacity  = "libp2p.io/dht/k-bucket/peers_rejected_capacity"
	MeasurePeersRefreshed           = "libp2p.io/dht/k-bucket/peers_refreshed"
	MeasurePeersRemoved             = "libp2p.io/dht/k-bucket/peers_removed"
)

// Measures are exported so that consumers can create their own views if the `DefaultViews`
// aren't sufficient. However, they should be updated using the functions below to avoid
// leaking OpenCensus cruft throughout the rest of the code.
var (
	KBucketsFull = stats.Int64(MeasureBucketsFull,
		"Number of k-buckets that are at capacity (have k entries).", stats.UnitDimensionless)
	KBucketsNonEmpty = stats.Int64(MeasureBucketsNonEmpty,
		"Number of k-buckets with at least one entry.", stats.UnitDimensionless)
	KBucketUtilization = stats.Int64(MeasureBucketUtilization,
		"Number of entries per k-bucket.", stats.UnitDimensionless)
	KBucketPeerLatency = stats.Float64(MeasurePeerLatency,
		"Recorded latency measurements for all peers added to each bucket, per bucket.", stats.UnitMilliseconds)
	KBucketPeersAdded = stats.Int64(MeasurePeersAdded,
		"Number of peers added to each k-bucket (cumulative). Note that peers can be counted twice if added and removed.", stats.UnitDimensionless)
	KBucketPeersRejectedHighLatency = stats.Int64(MeasurePeersRejectedHighLatency,
		"Number of peers rejected from k-buckets due to high latency.", stats.UnitDimensionless)
	KBucketPeersRejectedNoCapacity = stats.Int64(MeasurePeersRejectedNoCapacity,
		"Number of peers rejected from the routing table because their k-bucket was full.", stats.UnitDimensionless)
	KBucketPeersRefreshed = stats.Int64(MeasurePeersRefreshed,
		"Number of peers moved to the front of a k-bucket that they were already in.", stats.UnitDimensionless)
	KBucketPeersRemoved = stats.Int64(MeasurePeersRemoved,
		"Number of peers removed from each k-bucket (cumulative). "+
			"Peers may be counted twice if added and removed.", stats.UnitDimensionless)
)

// LocalContext returns `ctx` tagged with the local dht `id` for metrics reporting.
func LocalContext(ctx context.Context, id []byte) context.Context {
	pretty := base58.Encode(id)
	ctx, _ = tag.New(ctx, tag.Upsert(keyLocalId, pretty))
	return ctx
}

// RecordBucketsFull records the current number of buckets `n` that are at capacity.
func RecordBucketsFull(ctx context.Context, n int) {
	stats.Record(ctx, KBucketsFull.M(int64(n)))
}

// RecordBucketsNonEmpty records the current number of buckets `n` that have at least one entry.
func RecordBucketsNonEmpty(ctx context.Context, n int) {
	stats.Record(ctx, KBucketsNonEmpty.M(int64(n)))
}

// recordWithBucketIndex is a helper func that applies a tag to the measurements `ms`
// indicating the index of the bucket to which the measurement applies.
func recordWithBucketIndex(ctx context.Context, bucketIndex int, ms ...stats.Measurement) {
	_ = stats.RecordWithTags(ctx,
		[]tag.Mutator{tag.Upsert(keyBucketIndex, string(bucketIndex))},
		ms...,
	)
}

// RecordBucketUtilization records the current number of entries `n` for
// the given `bucketIndex`.
func RecordBucketUtilization(ctx context.Context, bucketIndex int, n int) {
	recordWithBucketIndex(ctx, bucketIndex, KBucketUtilization.M(int64(n)))
}

// RecordPeerAdded records that a peer was added to the bucket with index `bucketIndex`.
// It also records the `measuredLatency` of the peer at the time they were added.
func RecordPeerAdded(ctx context.Context, bucketIndex int, measuredLatency time.Duration) {
	recordWithBucketIndex(ctx, bucketIndex,
		KBucketPeersAdded.M(1),
		KBucketPeerLatency.M(float64(measuredLatency/time.Millisecond)))
}

// RecordPeerRejectedHighLatency records that a peer was rejected from the bucket with
// index `bucketIndex` because their measured connection latency was too high.
func RecordPeerRejectedHighLatency(ctx context.Context, bucketIndex int) {
	recordWithBucketIndex(ctx, bucketIndex, KBucketPeersRejectedHighLatency.M(1))
}

// RecordPeerRejectedNoCapacity records that a peer was rejected from the bucket with
// index `bucketIndex` because the bucket was full, and no members were eligible for eviction.
func RecordPeerRejectedNoCapacity(ctx context.Context, bucketIndex int) {
	recordWithBucketIndex(ctx, bucketIndex, KBucketPeersRejectedNoCapacity.M(1))
}

// RecordPeerRefreshed records that a peer in the bucket with index `bucketIndex`
// had its "last seen" status updated, moving it to the head of its bucket.
func RecordPeerRefreshed(ctx context.Context, bucketIndex int) {
	recordWithBucketIndex(ctx, bucketIndex, KBucketPeersRefreshed.M(1))
}

// RecordPeerRemoved records that a peer was removed from the bucket with
// index `bucketIndex`.
func RecordPeerRemoved(ctx context.Context, bucketIndex int) {
	recordWithBucketIndex(ctx, bucketIndex, KBucketPeersRemoved.M(1))
}

var DefaultViews = map[string]*view.View{
	MeasureBucketsFull: {
		Measure:     KBucketsFull,
		TagKeys:     []tag.Key{keyLocalId},
		Aggregation: view.LastValue(),
	},

	MeasureBucketsNonEmpty: {
		Measure:     KBucketsNonEmpty,
		TagKeys:     []tag.Key{keyLocalId},
		Aggregation: view.LastValue(),
	},

	MeasureBucketUtilization: {
		Measure:     KBucketUtilization,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.LastValue(),
	},

	MeasurePeersAdded: {
		Measure:     KBucketPeersAdded,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	},

	MeasurePeersRejectedHighLatency: {
		Measure:     KBucketPeersRejectedHighLatency,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	},

	MeasurePeersRejectedNoCapacity: {
		Measure:     KBucketPeersRejectedNoCapacity,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	},

	MeasurePeersRemoved: {
		Measure:     KBucketPeersRemoved,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	},
}
