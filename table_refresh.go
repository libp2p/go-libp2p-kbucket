package kbucket

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"

	mh "github.com/multiformats/go-multihash"
)

// maxCplForRefresh is the maximum cpl we support for refresh.
// This limit exists because we can only generate 'maxCplForRefresh' bit prefixes for now.
const maxCplForRefresh uint = 15

// CplRefresh contains a CPL(common prefix length) with the host & the last time
// we refreshed that cpl/searched for an ID which has that cpl with the host.
type CplRefresh struct {
	Cpl           uint
	LastRefreshAt time.Time
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
