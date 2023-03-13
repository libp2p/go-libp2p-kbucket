package kbucket

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"

	pstore "github.com/libp2p/go-libp2p/p2p/host/peerstore"

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

func TestGenRandomKey(t *testing.T) {
	// test can be run in parallel
	t.Parallel()

	// run multiple occurences to make sure the test wasn't just lucky
	for i := 0; i < 100; i++ {
		// generate routing table with random local peer ID
		local := test.RandPeerIDFatal(t)
		m := pstore.NewMetrics()
		rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
		require.NoError(t, err)

		// GenRandomKey fails for cpl >= 256
		_, err = rt.GenRandomKey(256)
		require.Error(t, err)
		_, err = rt.GenRandomKey(300)
		require.Error(t, err)

		// bitwise comparison legend:
		// O for same bit, X for different bit, ? for don't care

		// we compare the returned generated key with the local key
		// for CPL = X, the first X bits should be the same, bit X+1 should be
		// different, and the rest should be random / don't care

		// cpl = 0 should return a different first bit
		// X??????? ???...
		key0, err := rt.GenRandomKey(0)
		require.NoError(t, err)
		// most significant bit should be different
		require.NotEqual(t, key0[0]>>7, rt.local[0]>>7)

		// cpl = 1 should return a different second bit
		// OX?????? ???...
		key1, err := rt.GenRandomKey(1)
		require.NoError(t, err)
		// MSB should be equal, as cpl = 1
		require.Equal(t, key1[0]>>7, rt.local[0]>>7)
		// 2nd MSB should be different
		require.NotEqual(t, (key1[0]<<1)>>6, (rt.local[0]<<1)>>6)

		// cpl = 2 should return a different third bit
		// OOX????? ???...
		key2, err := rt.GenRandomKey(2)
		require.NoError(t, err)
		// 2 MSB should be equal, as cpl = 2
		require.Equal(t, key2[0]>>6, rt.local[0]>>6)
		// 3rd MSB should be different
		require.NotEqual(t, (key2[0]<<2)>>5, (rt.local[0]<<2)>>5)

		// cpl = 7 should return a different eighth bit
		// OOOOOOOX ???...
		key7, err := rt.GenRandomKey(7)
		require.NoError(t, err)
		// 7 MSB should be equal, as cpl = 7
		require.Equal(t, key7[0]>>1, rt.local[0]>>1)
		// 8th MSB should be different
		require.NotEqual(t, key7[0]<<7, rt.local[0]<<7)

		// cpl = 8 should return a different ninth bit
		// OOOOOOOO X???...
		key8, err := rt.GenRandomKey(8)
		require.NoError(t, err)
		// 8 MSB should be equal, as cpl = 8
		require.Equal(t, key8[0], rt.local[0])
		// 9th MSB should be different
		require.NotEqual(t, key8[1]>>7, rt.local[1]>>7)

		// cpl = 53 should return a different 54th bit
		// OOOOOOOO OOOOOOOO OOOOOOOO OOOOOOOO OOOOOOOO OOOOOOOO OOOOOX?? ???...
		key53, err := rt.GenRandomKey(53)
		require.NoError(t, err)
		// 53 MSB should be equal, as cpl = 53
		require.Equal(t, key53[:6], rt.local[:6])
		require.Equal(t, key53[6]>>3, rt.local[6]>>3)
		// 54th MSB should be different
		require.NotEqual(t, (key53[6]<<5)>>7, (rt.local[6]<<5)>>7)
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
