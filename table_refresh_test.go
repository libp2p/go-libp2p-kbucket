package kbucket

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/test"

	pstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/stretchr/testify/require"
)

func TestGenRandPeerID(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold)
	require.NoError(t, err)

	// generate above maxCplForRefresh fails
	p, err := rt.GenRandPeerID(maxCplForRefresh + 1)
	require.Error(t, err)
	require.Empty(t, p)

	// test generate rand peer ID
	for cpl := uint(0); cpl <= maxCplForRefresh; cpl++ {
		peerID, err := rt.GenRandPeerID(cpl)
		require.NoError(t, err)

		require.True(t, uint(CommonPrefixLen(ConvertPeerID(peerID), rt.local)) == cpl, "failed for cpl=%d", cpl)
	}
}

func TestRefreshAndGetTrackedCpls(t *testing.T) {
	t.Parallel()
	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold)
	require.NoError(t, err)

	// push cpl's for tracking
	for cpl := uint(0); cpl < maxCplForRefresh; cpl++ {
		peerID, err := rt.GenRandPeerID(cpl)
		require.NoError(t, err)
		rt.ResetCplRefreshedAtForID(ConvertPeerID(peerID), time.Now())
	}

	// fetch cpl's
	trackedCpls := rt.GetTrackedCplsForRefresh()
	require.Len(t, trackedCpls, int(maxCplForRefresh))
	actualCpls := make(map[uint]struct{})
	for i := 0; i < len(trackedCpls); i++ {
		actualCpls[trackedCpls[i].Cpl] = struct{}{}
	}

	for i := uint(0); i < maxCplForRefresh; i++ {
		_, ok := actualCpls[i]
		require.True(t, ok, "tracked cpl's should have cpl %d", i)
	}
}
