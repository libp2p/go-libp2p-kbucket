package kbucket

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"
)

// TODO Should ideally use a Circular queue for this
// maintains a bounded, de-duplicated and FIFO peer candidate queue for each Cpl
type cplReplacementCache struct {
	localPeer    ID
	maxQueueSize int

	sync.Mutex
	candidates map[uint][]peer.ID // candidates for a Cpl
}

func newCplReplacementCache(localPeer ID, maxQueueSize int) *cplReplacementCache {
	return &cplReplacementCache{
		localPeer:    localPeer,
		maxQueueSize: maxQueueSize,
		candidates:   make(map[uint][]peer.ID),
	}
}

// pushes a candidate to the end of the queue for the corresponding Cpl
// returns false if the queue is full or it already has the peer
// returns true if was successfully added
func (c *cplReplacementCache) push(p peer.ID) bool {
	c.Lock()
	defer c.Unlock()

	cpl := uint(CommonPrefixLen(c.localPeer, ConvertPeerID(p)))

	// queue is full
	if len(c.candidates[cpl]) >= c.maxQueueSize {
		return false
	}
	// queue already has the peer
	for _, pr := range c.candidates[cpl] {
		if pr == p {
			return false
		}
	}

	// push
	c.candidates[cpl] = append(c.candidates[cpl], p)
	return true
}

// pops a candidate from the top of the candidate queue for the given Cpl
// returns false if the queue is empty
// returns the peerId and true if successful
func (c *cplReplacementCache) pop(cpl uint) (peer.ID, bool) {
	c.Lock()
	defer c.Unlock()

	if len(c.candidates[cpl]) != 0 {
		p := c.candidates[cpl][0]
		c.candidates[cpl] = c.candidates[cpl][1:]

		// delete the queue if it's empty
		if len(c.candidates[cpl]) == 0 {
			delete(c.candidates, cpl)
		}

		return p, true
	}
	return "", false
}

// removes a given peer if it's present
// returns false if the peer is absent
func (c *cplReplacementCache) remove(p peer.ID) bool {
	c.Lock()
	defer c.Unlock()

	cpl := uint(CommonPrefixLen(c.localPeer, ConvertPeerID(p)))

	if len(c.candidates[cpl]) != 0 {
		// remove the peer if it's present
		for i, pr := range c.candidates[cpl] {
			if pr == p {
				c.candidates[cpl] = append(c.candidates[cpl][:i], c.candidates[cpl][i+1:]...)
			}
		}

		// remove the queue if it's empty
		if len(c.candidates[cpl]) == 0 {
			delete(c.candidates, cpl)
		}

		return true
	}
	return false
}
