package kbucket

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	pstore "github.com/libp2p/go-libp2p-peerstore"

	"github.com/stretchr/testify/require"
)

func TestTableCleanup(t *testing.T) {
	t.Parallel()
	local := test.RandPeerIDFatal(t)

	// Generate:
	// 6 peers with CPL 0
	// 6 peers with CPL 1
	cplPeerMap := make(map[int][]peer.ID)
	for cpl := 0; cpl < 2; cpl++ {
		i := 0

		for {
			p := test.RandPeerIDFatal(t)
			if CommonPrefixLen(ConvertPeerID(local), ConvertPeerID(p)) == cpl {
				cplPeerMap[cpl] = append(cplPeerMap[cpl], p)
				i++
				if i == 6 {
					break
				}
			}
		}
	}

	// mock peer validation fnc that successfully validates p[1], p[3] & p[5]

	f := func(ctx context.Context, p peer.ID) bool {
		cpl := CommonPrefixLen(ConvertPeerID(local), ConvertPeerID(p))
		if cplPeerMap[cpl][1] == p || cplPeerMap[cpl][3] == p || cplPeerMap[cpl][5] == p {
			return true
		} else {
			return false
		}
	}

	// create RT with a very short cleanup interval
	rt, err := NewRoutingTable(3, ConvertPeerID(local), time.Hour, pstore.NewMetrics(), PeerValidationFnc(f),
		TableCleanupInterval(100*time.Millisecond))
	require.NoError(t, err)

	// for each CPL, p[0], p[1] & p[2] got the bucket & p[3], p[4] & p[5] become candidates
	for _, peers := range cplPeerMap {
		for _, p := range peers {
			rt.HandlePeerAlive(p)

		}
	}

	// validate current TABLE state
	require.Len(t, rt.ListPeers(), 6)
	rt.tabLock.RLock()
	ps0 := rt.buckets[0].peerIds()
	require.Len(t, ps0, 3)
	ps1 := rt.buckets[1].peerIds()
	require.Len(t, ps1, 3)
	require.Contains(t, ps0, cplPeerMap[0][0])
	require.Contains(t, ps0, cplPeerMap[0][1])
	require.Contains(t, ps0, cplPeerMap[0][2])
	require.Contains(t, ps1, cplPeerMap[1][0])
	require.Contains(t, ps1, cplPeerMap[1][1])
	require.Contains(t, ps1, cplPeerMap[1][2])
	rt.tabLock.RUnlock()

	// validate current state of replacement cache
	rt.cplReplacementCache.Lock()
	require.Len(t, rt.cplReplacementCache.candidates, 2)
	require.Len(t, rt.cplReplacementCache.candidates[0], 3)
	require.Len(t, rt.cplReplacementCache.candidates[1], 3)
	rt.cplReplacementCache.Unlock()

	// now disconnect peers 0 1 & 2 from both buckets so it has only peer 1 left after it gets validated
	for _, peers := range cplPeerMap {
		rt.HandlePeerDisconnect(peers[0])
		rt.HandlePeerDisconnect(peers[1])
		rt.HandlePeerDisconnect(peers[2])
	}

	// let RT cleanup complete
	time.Sleep(2 * time.Second)

	// verify RT state
	require.Len(t, rt.ListPeers(), 6)
	rt.tabLock.RLock()
	ps0 = rt.buckets[0].peerIds()
	require.Len(t, ps0, 3)
	ps1 = rt.buckets[1].peerIds()
	require.Len(t, ps1, 3)
	require.Contains(t, ps0, cplPeerMap[0][1])
	require.Contains(t, ps0, cplPeerMap[0][3])
	require.Contains(t, ps0, cplPeerMap[0][5])
	require.Contains(t, ps1, cplPeerMap[1][1])
	require.Contains(t, ps1, cplPeerMap[1][1])
	require.Contains(t, ps1, cplPeerMap[1][3])
	require.Contains(t, ps1, cplPeerMap[1][5])
	rt.tabLock.RUnlock()

	// verify candidates were removed from the replacement cache
	rt.cplReplacementCache.Lock()
	require.Empty(t, rt.cplReplacementCache.candidates)
	rt.cplReplacementCache.Unlock()

	// close RT
	require.NoError(t, rt.Close())
}
