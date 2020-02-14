package kbucket

import (
	"sync"

	"github.com/libp2p/go-libp2p-core/peer"

	"github.com/wangjia184/sortedset"
)

// TODO Should ideally use a Circular queue for this
// maintains a bounded, de-duplicated and FIFO peer candidate queue for each Cpl
type cplReplacementCache struct {
	localPeer    ID
	maxQueueSize int

	sync.Mutex
	candidates map[uint]*sortedset.SortedSet // candidates for a Cpl
}

func newCplReplacementCache(localPeer ID, maxQueueSize int) *cplReplacementCache {
	return &cplReplacementCache{
		localPeer:    localPeer,
		maxQueueSize: maxQueueSize,
		candidates:   make(map[uint]*sortedset.SortedSet),
	}
}

// pushes a candidate to the end of the queue for the corresponding Cpl
// returns false if the queue is full or it already has the peer
// returns true if was successfully added
func (c *cplReplacementCache) push(p peer.ID) bool {
	c.Lock()
	defer c.Unlock()

	// create queue if not created
	cpl := uint(CommonPrefixLen(c.localPeer, ConvertPeerID(p)))
	if c.candidates[cpl] == nil {
		c.candidates[cpl] = sortedset.New()
	}

	q := c.candidates[cpl]

	// queue is full
	if (q.GetCount()) >= c.maxQueueSize {
		return false
	}
	// queue already has the peer
	if q.GetByKey(string(p)) != nil {
		return false
	}

	// push
	q.AddOrUpdate(string(p), sortedset.SCORE(q.GetCount()+1), nil)
	return true
}

// pops a candidate from the top of the candidate queue for the given Cpl
// returns false if the queue is empty
// returns the peerId and true if successful
func (c *cplReplacementCache) pop(cpl uint) (peer.ID, bool) {
	c.Lock()
	c.Unlock()

	q := c.candidates[cpl]
	if q != nil && q.GetCount() > 0 {
		n := q.PopMin()

		// delete the queue if it's empty
		if q.GetCount() == 0 {
			delete(c.candidates, cpl)
		}

		return peer.ID(n.Key()), true
	}
	return "", false
}

// removes a given peer if it's present
// returns false if the peer is absent
func (c *cplReplacementCache) remove(p peer.ID) bool {
	c.Lock()
	defer c.Unlock()

	cpl := uint(CommonPrefixLen(c.localPeer, ConvertPeerID(p)))
	q := c.candidates[cpl]
	if q != nil {
		q.Remove(string(p))

		// remove the queue if it's empty
		if q.GetCount() == 0 {
			delete(c.candidates, cpl)
		}

		return true
	}
	return false
}
