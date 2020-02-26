package kbucket

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	"github.com/stretchr/testify/require"
)

func TestCplReplacementCache(t *testing.T) {
	t.Parallel()

	maxQSize := 2
	local := ConvertPeerID(test.RandPeerIDFatal(t))
	c := newCplReplacementCache(local, maxQSize)

	// pop an empty queue fails
	p, b := c.pop(1)
	require.Empty(t, p)
	require.False(t, b)

	// push two elements to an empty queue works
	testPeer1 := genPeer(t, local, 1)
	testPeer2 := genPeer(t, local, 1)

	// pushing first peer works
	require.True(t, c.push(testPeer1))
	// pushing a duplicate fails
	require.False(t, c.push(testPeer1))
	// pushing another peers works
	require.True(t, c.push(testPeer2))

	// popping the above pushes works
	p, b = c.pop(1)
	require.True(t, b)
	require.Equal(t, testPeer1, p)
	p, b = c.pop(1)
	require.True(t, b)
	require.Equal(t, testPeer2, p)

	// pushing & popping again works
	require.True(t, c.push(testPeer1))
	require.True(t, c.push(testPeer2))
	p, b = c.pop(1)
	require.True(t, b)
	require.Equal(t, testPeer1, p)
	p, b = c.pop(1)
	require.True(t, b)
	require.Equal(t, testPeer2, p)

	// fill up a queue
	p1 := genPeer(t, local, 2)
	p2 := genPeer(t, local, 2)
	require.True(t, c.push(p1))
	require.True(t, c.push(p2))

	// push should not work on a full queue
	p3 := genPeer(t, local, 2)
	require.False(t, c.push(p3))

	// remove a peer & verify it's been removed
	p4 := genPeer(t, local, 0)
	require.True(t, c.push(p4))

	c.Lock()
	require.Len(t, c.candidates[0], 1)
	require.True(t, c.candidates[0][0] == p4)
	c.Unlock()

	require.True(t, c.remove(p4))

	c.Lock()
	require.Len(t, c.candidates[0], 0)
	c.Unlock()
}

func genPeer(t *testing.T, local ID, cpl int) peer.ID {
	var p peer.ID
	for {
		p = test.RandPeerIDFatal(t)
		if CommonPrefixLen(local, ConvertPeerID(p)) == cpl {
			return p
		}
	}
}
