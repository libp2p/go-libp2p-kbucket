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
	// lastSuccessfulOutboundQuery is the time instant when we last made a successful
	// outbound query to this peer
	lastSuccessfulOutboundQuery time.Time
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
		peerID := ConvertPeerID(e.Value.(*PeerInfo).Id)
		peerCPL := CommonPrefixLen(peerID, target)
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
