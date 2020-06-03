package kbucket

import (
	"github.com/libp2p/go-libp2p-core/test"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBucketFindFirst(t *testing.T) {
	t.Parallel()

	b := newBucket()
	require.Nil(t, b.findFirst(func(p1 *PeerInfo) bool { return true }))

	pid1 := test.RandPeerIDFatal(t)
	pid2 := test.RandPeerIDFatal(t)
	pid3 := test.RandPeerIDFatal(t)

	// first is replacable
	b.pushFront(&PeerInfo{Id: pid1, replaceable: true})
	require.Equal(t, pid1, b.findFirst(func(p *PeerInfo) bool {
		return p.replaceable
	}).Id)

	// above peer is stll the replacable one
	b.pushFront(&PeerInfo{Id: pid2, replaceable: false})
	require.Equal(t, pid1, b.findFirst(func(p *PeerInfo) bool {
		return p.replaceable
	}).Id)

	// new peer is replacable.
	b.pushFront(&PeerInfo{Id: pid3, replaceable: true})
	require.Equal(t, pid3, b.findFirst(func(p *PeerInfo) bool {
		return p.replaceable
	}).Id)
}

func TestUpdateAllWith(t *testing.T) {
	t.Parallel()

	b := newBucket()
	// dont crash
	b.updateAllWith(func(p *PeerInfo) {})

	pid1 := test.RandPeerIDFatal(t)
	pid2 := test.RandPeerIDFatal(t)
	pid3 := test.RandPeerIDFatal(t)

	// peer1
	b.pushFront(&PeerInfo{Id: pid1, replaceable: false})
	b.updateAllWith(func(p *PeerInfo) {
		p.replaceable = true
	})
	require.True(t, b.getPeer(pid1).replaceable)

	// peer2
	b.pushFront(&PeerInfo{Id: pid2, replaceable: false})
	b.updateAllWith(func(p *PeerInfo) {
		if p.Id == pid1 {
			p.replaceable = false
		} else {
			p.replaceable = true
		}
	})
	require.True(t, b.getPeer(pid2).replaceable)
	require.False(t, b.getPeer(pid1).replaceable)

	// peer3
	b.pushFront(&PeerInfo{Id: pid3, replaceable: false})
	require.False(t, b.getPeer(pid3).replaceable)
	b.updateAllWith(func(p *PeerInfo) {
		p.replaceable = true
	})
	require.True(t, b.getPeer(pid1).replaceable)
	require.True(t, b.getPeer(pid2).replaceable)
	require.True(t, b.getPeer(pid3).replaceable)
}
