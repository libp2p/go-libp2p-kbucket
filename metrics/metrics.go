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

// Default Views are exported individually and in the DefaultViews slice for
// convenient registration.
var (
	ViewBucketUtilization = &view.View{
		Measure:     KBucketUtilization,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.LastValue(),
	}
	ViewPeersAdded = &view.View{
		Measure:     KBucketPeersAdded,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	}
	ViewPeersRejectedHighLatency = &view.View{
		Measure:     KBucketPeersRejectedHighLatency,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	}
	ViewPeersRejectedNoCapacity = &view.View{
		Measure:     KBucketPeersRejectedNoCapacity,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	}
	ViewPeersRemoved = &view.View{
		Measure:     KBucketPeersRemoved,
		TagKeys:     []tag.Key{keyLocalId, keyBucketIndex},
		Aggregation: view.Count(),
	}
	DefaultViews = []*view.View{
		ViewBucketUtilization,
		ViewPeersAdded,
		ViewPeersRejectedHighLatency,
		ViewPeersRejectedNoCapacity,
		ViewPeersRemoved,
	}
)


// LocalContext returns `ctx` tagged with the local dht `id` for metrics reporting.
func LocalContext(ctx context.Context, id []byte) context.Context {
	pretty := base58.Encode(id)
	ctx, _ = tag.New(ctx, tag.Upsert(keyLocalId, pretty))
	return ctx
}

// BucketContext returns `ctx` tagged with the current `bucketIndex`, and it
// should be used for recording all metrics that correspond to a specific bucket.
func BucketContext(ctx context.Context, bucketIndex int) context.Context {
	ctx, _ = tag.New(ctx, tag.Upsert(keyBucketIndex, string(bucketIndex)))
	return ctx
}

// RecordBucketUtilization records the current number of entries `n` contained within
// a bucket. It is assumed that `ctx` is tagged with the current bucket id - see BucketContext.
func RecordBucketUtilization(ctx context.Context, n int) {
	stats.Record(ctx, KBucketUtilization.M(int64(n)))
}

// RecordPeerAdded records that a peer was added to a bucket, also recording the
// `measuredLatency` of the peer at the time they were added.
// It is assumed that `ctx` is tagged with the current bucket id - see BucketContext.
func RecordPeerAdded(ctx context.Context, measuredLatency time.Duration) {
	stats.Record(ctx,
		KBucketPeersAdded.M(1),
		KBucketPeerLatency.M(float64(measuredLatency/time.Millisecond)))
}

// RecordPeerRejectedHighLatency records that a peer was rejected from a bucket
// because their measured connection latency was too high.
// It is assumed that `ctx` is tagged with the current bucket id - see BucketContext.
func RecordPeerRejectedHighLatency(ctx context.Context, bucketIndex int) {
	stats.Record(ctx, KBucketPeersRejectedHighLatency.M(1))
}

// RecordPeerRejectedNoCapacity records that a peer was rejected from a bucket
// because the bucket was full, and no members were eligible for eviction.
// It is assumed that `ctx` is tagged with the current bucket id - see BucketContext.
func RecordPeerRejectedNoCapacity(ctx context.Context) {
	stats.Record(ctx, KBucketPeersRejectedNoCapacity.M(1))
}

// RecordPeerRefreshed records that a peer in a bucket had its "last seen" status updated,
// moving it to the head of its bucket.
// It is assumed that `ctx` is tagged with the current bucket id - see BucketContext.
func RecordPeerRefreshed(ctx context.Context) {
	stats.Record(ctx, KBucketPeersRefreshed.M(1))
}

// RecordPeerRemoved records that a peer was removed from a bucket.
// It is assumed that `ctx` is tagged with the current bucket id - see BucketContext.
func RecordPeerRemoved(ctx context.Context, bucketIndex int) {
	stats.Record(ctx, KBucketPeersRemoved.M(1))
}

