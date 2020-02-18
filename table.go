// package kbucket implements a kademlia 'k-bucket' routing table.
package kbucket

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"

	logging "github.com/ipfs/go-log"
	"github.com/jbenet/goprocess"
	goprocessctx "github.com/jbenet/goprocess/context"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("table")

var ErrPeerRejectedHighLatency = errors.New("peer rejected; latency too high")
var ErrPeerRejectedNoCapacity = errors.New("peer rejected; insufficient capacity")

// PeerSelectionFunc is the signature of a function that selects zero or more peers from the given peers
// based on some criteria.
type PeerSelectionFunc func(peers []PeerInfo) []PeerInfo

// PeerValidationFunc is the signature of a function that determines the validity a peer for Routing Table membership.
type PeerValidationFunc func(ctx context.Context, p peer.ID) bool

// maxCplForRefresh is the maximum cpl we support for refresh.
// This limit exists because we can only generate 'maxCplForRefresh' bit prefixes for now.
const maxCplForRefresh uint = 15

// CplRefresh contains a CPL(common prefix length) with the host & the last time
// we refreshed that cpl/searched for an ID which has that cpl with the host.
type CplRefresh struct {
	Cpl           uint
	LastRefreshAt time.Time
}

// RoutingTable defines the routing table.
type RoutingTable struct {
	ctx context.Context
	// ID of the local peer
	local ID

	// Blanket lock, refine later for better performance
	tabLock sync.RWMutex

	// latency metrics
	metrics peerstore.Metrics

	// Maximum acceptable latency for peers in this cluster
	maxLatency time.Duration

	// kBuckets define all the fingers to other nodes.
	buckets    []*bucket
	bucketsize int

	cplRefreshLk   sync.RWMutex
	cplRefreshedAt map[uint]time.Time

	// replacement candidates for a Cpl
	cplReplacementCache *cplReplacementCache

	// notification functions
	PeerRemoved func(peer.ID)
	PeerAdded   func(peer.ID)

	// function to determine the validity of a peer for RT membership
	peerValidationFnc PeerValidationFunc

	// timeout for a single call to the peer validation function
	peerValidationTimeout time.Duration
	// interval between two runs of the table cleanup routine
	tableCleanupInterval time.Duration
	// function to select peers that need to be validated
	peersForValidationFnc PeerSelectionFunc

	proc goprocess.Process
}

// NewRoutingTable creates a new routing table with a given bucketsize, local ID, and latency tolerance.
// Passing a nil PeerValidationFunc disables periodic table cleanup.
func NewRoutingTable(bucketsize int, localID ID, latency time.Duration, m peerstore.Metrics,
	options ...Option) (*RoutingTable, error) {

	var cfg Options
	if err := cfg.Apply(append([]Option{Defaults}, options...)...); err != nil {
		return nil, err
	}

	rt := &RoutingTable{
		ctx:        context.Background(),
		buckets:    []*bucket{newBucket()},
		bucketsize: bucketsize,
		local:      localID,

		maxLatency: latency,
		metrics:    m,

		cplRefreshedAt: make(map[uint]time.Time),

		PeerRemoved: func(peer.ID) {},
		PeerAdded:   func(peer.ID) {},

		peerValidationFnc:     cfg.TableCleanup.PeerValidationFnc,
		peersForValidationFnc: cfg.TableCleanup.PeersForValidationFnc,
		peerValidationTimeout: cfg.TableCleanup.PeerValidationTimeout,
		tableCleanupInterval:  cfg.TableCleanup.Interval,
	}

	rt.cplReplacementCache = newCplReplacementCache(rt.local, rt.bucketsize)
	rt.proc = goprocessctx.WithContext(rt.ctx)

	// schedule periodic RT cleanup if peer validation function has been passed
	if rt.peerValidationFnc != nil {
		rt.proc.Go(rt.cleanup)
	}

	return rt, nil
}

// Close shuts down the Routing Table & all associated processes.
// It is safe to call this multiple times.
func (rt *RoutingTable) Close() error {
	return rt.proc.Close()
}

func (rt *RoutingTable) cleanup(proc goprocess.Process) {
	validatePeerF := func(p peer.ID) bool {
		queryCtx, cancel := context.WithTimeout(rt.ctx, rt.peerValidationTimeout)
		defer cancel()
		return rt.peerValidationFnc(queryCtx, p)
	}

	cleanupTickr := time.NewTicker(rt.tableCleanupInterval)
	defer cleanupTickr.Stop()
	for {
		select {
		case <-cleanupTickr.C:
			ps := rt.peersToValidate()
			for _, pinfo := range ps {
				// continue if we are able to successfully validate the peer
				// it will be marked alive in the RT when the DHT connection notification handler calls RT.HandlePeerAlive()
				// TODO Should we revisit this ? It makes more sense for the RT to mark it as active here
				if validatePeerF(pinfo.Id) {
					log.Infof("successfully validated missing peer=%s", pinfo.Id)
					continue
				}

				// peer does not seem to be alive, let's try candidates now
				log.Infof("failed to validate missing peer=%s, will try candidates now...", pinfo.Id)
				// evict missing peer
				rt.HandlePeerDead(pinfo.Id)

				// keep trying replacement candidates for the missing peer till we get a successful validation or
				// we run out of candidates
				cpl := uint(CommonPrefixLen(ConvertPeerID(pinfo.Id), rt.local))
				c, notEmpty := rt.cplReplacementCache.pop(cpl)
				for notEmpty {
					if validatePeerF(c) {
						log.Infof("successfully validated candidate=%s for missing peer=%s", c, pinfo.Id)
						break
					}
					log.Infof("failed to validated candidate=%s", c)
					// remove candidate
					rt.HandlePeerDead(c)

					c, notEmpty = rt.cplReplacementCache.pop(cpl)
				}

				if !notEmpty {
					log.Infof("failed to replace missing peer=%s as all candidates were invalid", pinfo.Id)
				}
			}
		case <-proc.Closing():
			return
		}
	}
}

// returns the peers that need to be validated.
func (rt *RoutingTable) peersToValidate() []PeerInfo {
	rt.tabLock.RLock()
	defer rt.tabLock.RUnlock()

	var peers []PeerInfo
	for _, b := range rt.buckets {
		peers = append(peers, b.peers()...)
	}
	return rt.peersForValidationFnc(peers)
}

// GetTrackedCplsForRefresh returns the Cpl's we are tracking for refresh.
// Caller is free to modify the returned slice as it is a defensive copy.
func (rt *RoutingTable) GetTrackedCplsForRefresh() []CplRefresh {
	rt.cplRefreshLk.RLock()
	defer rt.cplRefreshLk.RUnlock()

	cpls := make([]CplRefresh, 0, len(rt.cplRefreshedAt))

	for c, t := range rt.cplRefreshedAt {
		cpls = append(cpls, CplRefresh{c, t})
	}

	return cpls
}

// GenRandPeerID generates a random peerID for a given Cpl
func (rt *RoutingTable) GenRandPeerID(targetCpl uint) (peer.ID, error) {
	if targetCpl > maxCplForRefresh {
		return "", fmt.Errorf("cannot generate peer ID for Cpl greater than %d", maxCplForRefresh)
	}

	localPrefix := binary.BigEndian.Uint16(rt.local)

	// For host with ID `L`, an ID `K` belongs to a bucket with ID `B` ONLY IF CommonPrefixLen(L,K) is EXACTLY B.
	// Hence, to achieve a targetPrefix `T`, we must toggle the (T+1)th bit in L & then copy (T+1) bits from L
	// to our randomly generated prefix.
	toggledLocalPrefix := localPrefix ^ (uint16(0x8000) >> targetCpl)
	randPrefix := uint16(rand.Uint32())

	// Combine the toggled local prefix and the random bits at the correct offset
	// such that ONLY the first `targetCpl` bits match the local ID.
	mask := (^uint16(0)) << (16 - (targetCpl + 1))
	targetPrefix := (toggledLocalPrefix & mask) | (randPrefix & ^mask)

	// Convert to a known peer ID.
	key := keyPrefixMap[targetPrefix]
	id := [34]byte{mh.SHA2_256, 32}
	binary.BigEndian.PutUint32(id[2:], key)
	return peer.ID(id[:]), nil
}

// ResetCplRefreshedAtForID resets the refresh time for the Cpl of the given ID.
func (rt *RoutingTable) ResetCplRefreshedAtForID(id ID, newTime time.Time) {
	cpl := CommonPrefixLen(id, rt.local)
	if uint(cpl) > maxCplForRefresh {
		return
	}

	rt.cplRefreshLk.Lock()
	defer rt.cplRefreshLk.Unlock()

	rt.cplRefreshedAt[uint(cpl)] = newTime
}

// HandlePeerDisconnect should be called when the caller detects a disconnection with the peer.
// This enables the Routing Table to mark the peer as missing.
func (rt *RoutingTable) HandlePeerDisconnect(p peer.ID) {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()

	bucketId := rt.bucketIdForPeer(p)
	// mark the peer as missing
	b := rt.buckets[bucketId]
	if peer := b.getPeer(p); peer != nil {
		peer.State = PeerStateMissing
	}
}

// HandlePeerAlive should be called when the caller detects that a peer is alive.
// This could be a successful incoming/outgoing connection with the peer or even a successful message delivery to/from the peer.
// This enables the RT to update it's internal state to mark the peer as active.
func (rt *RoutingTable) HandlePeerAlive(p peer.ID) (evicted peer.ID, err error) {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()

	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]
	if peer := bucket.getPeer(p); peer != nil {
		// mark the peer as active
		peer.State = PeerStateActive

		return "", nil
	}

	if rt.metrics.LatencyEWMA(p) > rt.maxLatency {
		// Connection doesnt meet requirements, skip!
		return "", ErrPeerRejectedHighLatency
	}

	// We have enough space in the bucket (whether spawned or grouped).
	if bucket.len() < rt.bucketsize {
		bucket.pushFront(&PeerInfo{p, PeerStateActive})
		rt.PeerAdded(p)
		return "", nil
	}

	if bucketID == len(rt.buckets)-1 {
		// if the bucket is too large and this is the last bucket (i.e. wildcard), unfold it.
		rt.nextBucket()
		// the structure of the table has changed, so let's recheck if the peer now has a dedicated bucket.
		bucketID = rt.bucketIdForPeer(p)
		bucket = rt.buckets[bucketID]

		// push the peer only if the bucket isn't overflowing after slitting
		if bucket.len() < rt.bucketsize {
			bucket.pushFront(&PeerInfo{p, PeerStateActive})
			rt.PeerAdded(p)
			return "", nil
		}
	}
	// try to push it as a candidate in the replacement cache
	rt.cplReplacementCache.push(p)

	return "", ErrPeerRejectedNoCapacity
}

// HandlePeerDead should be called when the caller is sure that a peer is dead/not dialable.
// It evicts the peer from the Routing Table and also removes it as a replacement candidate if it is one.
func (rt *RoutingTable) HandlePeerDead(p peer.ID) {
	// remove it as a candidate
	rt.cplReplacementCache.remove(p)

	// remove it from the RT
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()
	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]
	if bucket.remove(p) {
		rt.PeerRemoved(p)
	}
}

func (rt *RoutingTable) nextBucket() {
	// This is the last bucket, which allegedly is a mixed bag containing peers not belonging in dedicated (unfolded) buckets.
	// _allegedly_ is used here to denote that *all* peers in the last bucket might feasibly belong to another bucket.
	// This could happen if e.g. we've unfolded 4 buckets, and all peers in folded bucket 5 really belong in bucket 8.
	bucket := rt.buckets[len(rt.buckets)-1]
	newBucket := bucket.split(len(rt.buckets)-1, rt.local)
	rt.buckets = append(rt.buckets, newBucket)

	// The newly formed bucket still contains too many peers. We probably just unfolded a empty bucket.
	if newBucket.len() >= rt.bucketsize {
		// Keep unfolding the table until the last bucket is not overflowing.
		rt.nextBucket()
	}
}

// Find a specific peer by ID or return nil
func (rt *RoutingTable) Find(id peer.ID) peer.ID {
	srch := rt.NearestPeers(ConvertPeerID(id), 1)
	if len(srch) == 0 || srch[0] != id {
		return ""
	}
	return srch[0]
}

// NearestPeer returns a single peer that is nearest to the given ID
func (rt *RoutingTable) NearestPeer(id ID) peer.ID {
	peers := rt.NearestPeers(id, 1)
	if len(peers) > 0 {
		return peers[0]
	}

	log.Debugf("NearestPeer: Returning nil, table size = %d", rt.Size())
	return ""
}

// NearestPeers returns a list of the 'count' closest peers to the given ID
func (rt *RoutingTable) NearestPeers(id ID, count int) []peer.ID {
	// This is the number of bits _we_ share with the key. All peers in this
	// bucket share cpl bits with us and will therefore share at least cpl+1
	// bits with the given key. +1 because both the target and all peers in
	// this bucket differ from us in the cpl bit.
	cpl := CommonPrefixLen(id, rt.local)

	// It's assumed that this also protects the buckets.
	rt.tabLock.RLock()

	// Get bucket index or last bucket
	if cpl >= len(rt.buckets) {
		cpl = len(rt.buckets) - 1
	}

	pds := peerDistanceSorter{
		peers:  make([]peerDistance, 0, count+rt.bucketsize),
		target: id,
	}

	// Add peers from the target bucket (cpl+1 shared bits).
	pds.appendPeersFromList(rt.buckets[cpl].list)

	// If we're short, add peers from buckets to the right until we have
	// enough. All buckets to the right share exactly cpl bits (as opposed
	// to the cpl+1 bits shared by the peers in the cpl bucket).
	//
	// Unfortunately, to be completely correct, we can't just take from
	// buckets until we have enough peers because peers because _all_ of
	// these peers will be ~2**(256-cpl) from us.
	//
	// However, we're going to do that anyways as it's "good enough"

	for i := cpl + 1; i < len(rt.buckets) && pds.Len() < count; i++ {
		pds.appendPeersFromList(rt.buckets[i].list)
	}

	// If we're still short, add in buckets that share _fewer_ bits. We can
	// do this bucket by bucket because each bucket will share 1 fewer bit
	// than the last.
	//
	// * bucket cpl-1: cpl-1 shared bits.
	// * bucket cpl-2: cpl-2 shared bits.
	// ...
	for i := cpl - 1; i >= 0 && pds.Len() < count; i-- {
		pds.appendPeersFromList(rt.buckets[i].list)
	}
	rt.tabLock.RUnlock()

	// Sort by distance to local peer
	pds.sort()

	if count < pds.Len() {
		pds.peers = pds.peers[:count]
	}

	out := make([]peer.ID, 0, pds.Len())
	for _, p := range pds.peers {
		out = append(out, p.p)
	}

	return out
}

// Size returns the total number of peers in the routing table
func (rt *RoutingTable) Size() int {
	var tot int
	rt.tabLock.RLock()
	for _, buck := range rt.buckets {
		tot += buck.len()
	}
	rt.tabLock.RUnlock()
	return tot
}

// ListPeers takes a RoutingTable and returns a list of all peers from all buckets in the table.
func (rt *RoutingTable) ListPeers() []peer.ID {
	var peers []peer.ID
	rt.tabLock.RLock()
	for _, buck := range rt.buckets {
		peers = append(peers, buck.peerIds()...)
	}
	rt.tabLock.RUnlock()
	return peers
}

// Print prints a descriptive statement about the provided RoutingTable
func (rt *RoutingTable) Print() {
	fmt.Printf("Routing Table, bs = %d, Max latency = %d\n", rt.bucketsize, rt.maxLatency)
	rt.tabLock.RLock()

	for i, b := range rt.buckets {
		fmt.Printf("\tbucket: %d\n", i)

		b.lk.RLock()
		for e := b.list.Front(); e != nil; e = e.Next() {
			p := e.Value.(peer.ID)
			fmt.Printf("\t\t- %s %s\n", p.Pretty(), rt.metrics.LatencyEWMA(p).String())
		}
		b.lk.RUnlock()
	}
	rt.tabLock.RUnlock()
}

// the caller is responsible for the locking
func (rt *RoutingTable) bucketIdForPeer(p peer.ID) int {
	peerID := ConvertPeerID(p)
	cpl := CommonPrefixLen(peerID, rt.local)
	bucketID := cpl
	if bucketID >= len(rt.buckets) {
		bucketID = len(rt.buckets) - 1
	}
	return bucketID
}
