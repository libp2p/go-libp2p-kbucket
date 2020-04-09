//go:generate go run ./generate

package kbucket

import (
	"container/list"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

// PeerInfo holds all related information for a peer in the K-Bucket.
type PeerInfo struct {
	Id peer.ID

	// LastUsefulAt is the time instant at which the peer was last "useful" to us.
	// Please see the DHT docs for the definition of usefulness.
	LastUsefulAt time.Time

	// LastSuccessfulOutboundQueryAt is the time instant at which we last got a
	// successful query response from the peer.
	LastSuccessfulOutboundQueryAt time.Time

	// Id of the peer in the DHT XOR keyspace
	dhtId ID
}

// bucket holds a list of peers.
// we synchronize on the Routing Table lock for all access to the bucket
// and so do not need any locks in the bucket.
// if we want/need to avoid locking the table for accessing a bucket in the future,
// it WILL be the caller's responsibility to synchronize all access to a bucket.
type bucket struct {
	list *list.List
}

func newBucket() *bucket {
	b := new(bucket)
	b.list = list.New()
	return b
}

// returns all peers in the bucket
// it is safe for the caller to modify the returned objects as it is a defensive copy
func (b *bucket) peers() []PeerInfo {
	var ps []PeerInfo
	for e := b.list.Front(); e != nil; e = e.Next() {
		p := e.Value.(*PeerInfo)
		ps = append(ps, *p)
	}
	return ps
}

// return the Ids of all the peers in the bucket.
func (b *bucket) peerIds() []peer.ID {
	ps := make([]peer.ID, 0, b.list.Len())
	for e := b.list.Front(); e != nil; e = e.Next() {
		p := e.Value.(*PeerInfo)
		ps = append(ps, p.Id)
	}
	return ps
}

// returns the peer with the given Id if it exists
// returns nil if the peerId does not exist
func (b *bucket) getPeer(p peer.ID) *PeerInfo {
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == p {
			return e.Value.(*PeerInfo)
		}
	}
	return nil
}

// removes the peer with the given Id from the bucket.
// returns true if successful, false otherwise.
func (b *bucket) remove(id peer.ID) bool {
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == id {
			b.list.Remove(e)
			return true
		}
	}
	return false
}

func (b *bucket) moveToFront(id peer.ID) {

	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == id {
			b.list.MoveToFront(e)
		}
	}
}

func (b *bucket) pushFront(p *PeerInfo) {
	b.list.PushFront(p)
}

func (b *bucket) len() int {
	return b.list.Len()
}

// splits a buckets peers into two buckets, the methods receiver will have
// peers with CPL equal to cpl, the returned bucket will have peers with CPL
// greater than cpl (returned bucket has closer peers)
func (b *bucket) split(cpl int, target ID) *bucket {
	out := list.New()
	newbuck := newBucket()
	newbuck.list = out
	e := b.list.Front()
	for e != nil {
		pDhtId := e.Value.(*PeerInfo).dhtId
		peerCPL := CommonPrefixLen(pDhtId, target)
		if peerCPL > cpl {
			cur := e
			out.PushBack(e.Value)
			e = e.Next()
			b.list.Remove(cur)
			continue
		}
		e = e.Next()
	}
	return newbuck
}

// maxCommonPrefix returns the maximum common prefix length between any peer in
// the bucket with the target ID.
func (b *bucket) maxCommonPrefix(target ID) uint {
	maxCpl := uint(0)
	for e := b.list.Front(); e != nil; e = e.Next() {
		cpl := uint(CommonPrefixLen(e.Value.(*PeerInfo).dhtId, target))
		if cpl > maxCpl {
			maxCpl = cpl
		}
	}
	return maxCpl
}
