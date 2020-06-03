package kbucket

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	pstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/stretchr/testify/require"
)

func TestGenRandPeerID(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
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

	const (
		minCpl  = 8
		testCpl = 10
		maxCpl  = 12
	)

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(2, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	// fetch cpl's
	trackedCpls := rt.GetTrackedCplsForRefresh()
	// should have nothing.
	require.Len(t, trackedCpls, 1)

	var peerIDs []peer.ID
	for i := minCpl; i <= maxCpl; i++ {
		id, err := rt.GenRandPeerID(uint(i))
		require.NoError(t, err)
		peerIDs = append(peerIDs, id)
	}

	// add peer IDs.
	for i, id := range peerIDs {
		added, err := rt.TryAddPeer(id, true, false)
		require.NoError(t, err)
		require.True(t, added)
		require.Len(t, rt.GetTrackedCplsForRefresh(), minCpl+i+1)
	}

	// and remove down to the test CPL
	for i := maxCpl; i > testCpl; i-- {
		rt.RemovePeer(peerIDs[i-minCpl])
		require.Len(t, rt.GetTrackedCplsForRefresh(), i)
	}

	// should be tracking testCpl
	trackedCpls = rt.GetTrackedCplsForRefresh()
	require.Len(t, trackedCpls, testCpl+1)
	// they should all be zero
	for _, refresh := range trackedCpls {
		require.True(t, refresh.IsZero(), "tracked cpl's should be zero")
	}

	// add our peer ID to max out the table
	added, err := rt.TryAddPeer(local, true, false)
	require.NoError(t, err)
	require.True(t, added)

	// should be tracking the max
	trackedCpls = rt.GetTrackedCplsForRefresh()
	require.Len(t, trackedCpls, int(maxCplForRefresh)+1)

	// and not refreshed
	for _, refresh := range trackedCpls {
		require.True(t, refresh.IsZero(), "tracked cpl's should be zero")
	}

	now := time.Now()
	// reset the test peer ID.
	rt.ResetCplRefreshedAtForID(ConvertPeerID(peerIDs[testCpl-minCpl]), now)

	// should still be tracking all buckets
	trackedCpls = rt.GetTrackedCplsForRefresh()
	require.Len(t, trackedCpls, int(maxCplForRefresh)+1)

	for i, refresh := range trackedCpls {
		if i == testCpl {
			require.True(t, now.Equal(refresh), "test cpl should have the correct refresh time")
		} else {
			require.True(t, refresh.IsZero(), "other cpl's should be 0")
		}
	}
}
