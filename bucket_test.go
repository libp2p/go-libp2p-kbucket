package kbucket

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/test"

	"github.com/stretchr/testify/require"
)

func TestBucketMinimum(t *testing.T) {
	t.Parallel()

	b := newBucket()
	require.Nil(t, b.min(func(p1 *PeerInfo, p2 *PeerInfo) bool { return true }))

	pid1 := test.RandPeerIDFatal(t)
	pid2 := test.RandPeerIDFatal(t)
	pid3 := test.RandPeerIDFatal(t)

	// first is min
	b.pushFront(&PeerInfo{Id: pid1, LastUsefulAt: time.Now()})
	require.Equal(t, pid1, b.min(func(first *PeerInfo, second *PeerInfo) bool {
		return first.LastUsefulAt.Before(second.LastUsefulAt)
	}).Id)

	// first is till min
	b.pushFront(&PeerInfo{Id: pid2, LastUsefulAt: time.Now().AddDate(1, 0, 0)})
	require.Equal(t, pid1, b.min(func(first *PeerInfo, second *PeerInfo) bool {
		return first.LastUsefulAt.Before(second.LastUsefulAt)
	}).Id)

	// second is the min
	b.pushFront(&PeerInfo{Id: pid3, LastUsefulAt: time.Now().AddDate(-1, 0, 0)})
	require.Equal(t, pid3, b.min(func(first *PeerInfo, second *PeerInfo) bool {
		return first.LastUsefulAt.Before(second.LastUsefulAt)
	}).Id)
}
