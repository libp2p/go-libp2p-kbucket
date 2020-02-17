//go:generate go run ./generate

package kbucket

import (
	"container/list"
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
)

// PeerState is the state of the peer as seen by the Routing Table.
type PeerState int

const (
	// PeerStateActive indicates that we know the peer is active/alive.
	PeerStateActive PeerState = iota
	// PeerStateMissing indicates that we do not know the state of the peer.
	PeerStateMissing
)

// PeerInfo holds all related information for a peer in the K-Bucket.
type PeerInfo struct {
	Id    peer.ID
	State PeerState
}

// bucket holds a list of peers.
type bucket struct {
	lk   sync.RWMutex
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
	b.lk.RLock()
	defer b.lk.RUnlock()
	var ps []PeerInfo
	for e := b.list.Front(); e != nil; e = e.Next() {
		p := e.Value.(*PeerInfo)
		ps = append(ps, *p)
	}
	return ps
}

// return the Ids of all the peers in the bucket.
func (b *bucket) peerIds() []peer.ID {
	b.lk.RLock()
	defer b.lk.RUnlock()
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
	b.lk.RLock()
	defer b.lk.RUnlock()
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
	b.lk.Lock()
	defer b.lk.Unlock()
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == id {
			b.list.Remove(e)
			return true
		}
	}
	return false
}

func (b *bucket) moveToFront(id peer.ID) {
	b.lk.Lock()
	defer b.lk.Unlock()

	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == id {
			b.list.MoveToFront(e)
		}
	}
}

func (b *bucket) pushFront(p *PeerInfo) {
	b.lk.Lock()
	b.list.PushFront(p)
	b.lk.Unlock()
}

func (b *bucket) len() int {
	b.lk.RLock()
	defer b.lk.RUnlock()
	return b.list.Len()
}

// splits a buckets peers into two buckets, the methods receiver will have
// peers with CPL equal to cpl, the returned bucket will have peers with CPL
// greater than cpl (returned bucket has closer peers)
func (b *bucket) split(cpl int, target ID) *bucket {
	b.lk.Lock()
	defer b.lk.Unlock()

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
