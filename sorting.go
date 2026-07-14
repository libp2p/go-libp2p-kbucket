package kbucket

import (
	"container/list"
	"slices"

	"github.com/libp2p/go-libp2p/core/peer"
)

// A helper struct to sort peers by their distance to the local node
type peerDistance struct {
	p        peer.ID
	distance ID
}

// peerDistanceSorter implements sort.Interface to sort peers by xor distance
type peerDistanceSorter struct {
	peers  []peerDistance
	target ID
}

func (pds *peerDistanceSorter) Len() int { return len(pds.peers) }

// Append the peer.ID to the sorter's slice. It may no longer be sorted.
func (pds *peerDistanceSorter) appendPeer(p peer.ID, pDhtId ID) {
	pds.peers = append(pds.peers, peerDistance{
		p:        p,
		distance: Xor(pds.target, pDhtId),
	})
}

// Append the peer.ID values in the list to the sorter's slice. It may no longer be sorted.
func (pds *peerDistanceSorter) appendPeersFromList(l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		pds.appendPeer(e.Value.(*PeerInfo).Id, e.Value.(*PeerInfo).dhtId)
	}
}

func (pds *peerDistanceSorter) sort() {
	slices.SortFunc(pds.peers, func(a, b peerDistance) int {
		return a.distance.cmp(b.distance)
	})
}

// SortClosestPeers sorts the given peers by their ascending distance from the
// target. A new slice is returned.
func SortClosestPeers(peers []peer.ID, target ID) []peer.ID {
	out := slices.Clone(peers)
	slices.SortFunc(out, func(a, b peer.ID) int {
		aDist := Xor(target, ConvertPeerID(a))
		bDist := Xor(target, ConvertPeerID(b))
		return aDist.cmp(bDist)
	})
	return out
}
