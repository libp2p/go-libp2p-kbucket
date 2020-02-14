package kbucket

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	pstore "github.com/libp2p/go-libp2p-peerstore"
	"github.com/stretchr/testify/require"
)

var PeerAlwaysValidFnc = func(ctx context.Context, p peer.ID) bool {
	return true
}

// Test basic features of the bucket struct
func TestBucket(t *testing.T) {
	t.Parallel()

	b := newBucket()

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		b.pushFront(PeerInfo{peers[i], PeerStateActive})
	}

	local := test.RandPeerIDFatal(t)
	localID := ConvertPeerID(local)

	infos := b.peers()
	require.Len(t, infos, 100)

	i := rand.Intn(len(peers))
	p, has := b.getPeer(peers[i])
	require.True(t, has)
	require.Equal(t, peers[i], p.Id)
	require.Equal(t, PeerStateActive, p.State)

	// replace
	require.True(t, b.replace(PeerInfo{peers[i], PeerStateMissing}))
	p, has = b.getPeer(peers[i])
	require.True(t, has)
	require.Equal(t, PeerStateMissing, p.State)

	spl := b.split(0, ConvertPeerID(local))
	llist := b.list
	for e := llist.Front(); e != nil; e = e.Next() {
		p := ConvertPeerID(e.Value.(PeerInfo).Id)
		cpl := CommonPrefixLen(p, localID)
		if cpl > 0 {
			t.Fatalf("split failed. found id with cpl > 0 in 0 bucket")
		}
	}

	rlist := spl.list
	for e := rlist.Front(); e != nil; e = e.Next() {
		p := ConvertPeerID(e.Value.(PeerInfo).Id)
		cpl := CommonPrefixLen(p, localID)
		if cpl == 0 {
			t.Fatalf("split failed. found id with cpl == 0 in non 0 bucket")
		}
	}
}

func TestGenRandPeerID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 1, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
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
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 1, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
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

func TestHandlePeerDead(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 2, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	// push 3 peers  -> 2 for the first bucket, and 1 as candidates
	var peers []peer.ID
	for i := 0; i < 3; i++ {
		p, err := rt.GenRandPeerID(uint(0))
		require.NoError(t, err)
		require.NotEmpty(t, p)
		rt.HandlePeerAlive(p)
		peers = append(peers, p)
	}

	// ensure we have 1 candidate
	rt.cplReplacementCache.Lock()
	require.NotNil(t, rt.cplReplacementCache.candidates[uint(0)])
	require.True(t, rt.cplReplacementCache.candidates[uint(0)].GetCount() == 1)
	rt.cplReplacementCache.Unlock()

	// mark a peer as dead and ensure it's not in the RT
	require.NotEmpty(t, rt.Find(peers[0]))
	rt.HandlePeerDead(peers[0])
	require.Empty(t, rt.Find(peers[0]))

	// mark the peer as dead & verify we don't get it as a candidate
	rt.HandlePeerDead(peers[2])

	rt.cplReplacementCache.Lock()
	require.Nil(t, rt.cplReplacementCache.candidates[uint(0)])
	rt.cplReplacementCache.Unlock()
}

func TestTableCallbacks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 10, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
	}

	pset := make(map[peer.ID]struct{})
	rt.PeerAdded = func(p peer.ID) {
		pset[p] = struct{}{}
	}
	rt.PeerRemoved = func(p peer.ID) {
		delete(pset, p)
	}

	rt.HandlePeerAlive(peers[0])
	if _, ok := pset[peers[0]]; !ok {
		t.Fatal("should have this peer")
	}

	rt.HandlePeerDead(peers[0])
	if _, ok := pset[peers[0]]; ok {
		t.Fatal("should not have this peer")
	}

	for _, p := range peers {
		rt.HandlePeerAlive(p)
	}

	out := rt.ListPeers()
	for _, outp := range out {
		if _, ok := pset[outp]; !ok {
			t.Fatal("should have peer in the peerset")
		}
		delete(pset, outp)
	}

	if len(pset) > 0 {
		t.Fatal("have peers in peerset that were not in the table", len(pset))
	}
}

func TestHandlePeerDisconnect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 10, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	p := test.RandPeerIDFatal(t)
	// mark a peer as alive
	rt.HandlePeerAlive(p)

	// verify it's active
	rt.tabLock.Lock()
	bp, has := rt.Buckets[0].getPeer(p)
	require.True(t, has)
	require.NotNil(t, bp)
	require.Equal(t, PeerStateActive, bp.State)
	rt.tabLock.Unlock()

	//now mark it as disconnected & verify it's in missing state
	rt.HandlePeerDisconnect(p)
	rt.tabLock.Lock()
	bp, has = rt.Buckets[0].getPeer(p)
	require.True(t, has)
	require.NotNil(t, bp)
	require.Equal(t, PeerStateMissing, bp.State)
	rt.tabLock.Unlock()
}

// Right now, this just makes sure that it doesnt hang or crash
func TestHandlePeerAlive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 10, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
	}

	// Testing HandlePeerAlive
	for i := 0; i < 10000; i++ {
		rt.HandlePeerAlive(peers[rand.Intn(len(peers))])
	}

	for i := 0; i < 100; i++ {
		id := ConvertPeerID(test.RandPeerIDFatal(t))
		ret := rt.NearestPeers(id, 5)
		if len(ret) == 0 {
			t.Fatal("Failed to find node near ID.")
		}
	}
}

func TestTableFind(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 10, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 5; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		rt.HandlePeerAlive(peers[i])
	}

	t.Logf("Searching for peer: '%s'", peers[2])
	found := rt.NearestPeer(ConvertPeerID(peers[2]))
	if !(found == peers[2]) {
		t.Fatalf("Failed to lookup known node...")
	}
}

func TestCandidateAddition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 3, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	// generate 6 peers for the first bucket, 3 to push to it, and 3 as candidates
	var peers []peer.ID
	for i := 0; i < 6; i++ {
		p, err := rt.GenRandPeerID(uint(0))
		require.NoError(t, err)
		require.NotEmpty(t, p)
		rt.HandlePeerAlive(p)
		peers = append(peers, p)
	}

	// fetch & verify candidates
	for _, p := range peers[3:] {
		ap, b := rt.cplReplacementCache.pop(0)
		require.True(t, b)
		require.Equal(t, p, ap)
	}

	// now pop should fail as queue should be empty
	_, b := rt.cplReplacementCache.pop(0)
	require.False(t, b)
}

func TestTableEldestPreferred(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 10, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	// generate size + 1 peers to saturate a bucket
	peers := make([]peer.ID, 15)
	for i := 0; i < 15; {
		if p := test.RandPeerIDFatal(t); CommonPrefixLen(ConvertPeerID(local), ConvertPeerID(p)) == 0 {
			peers[i] = p
			i++
		}
	}

	// test 10 first peers are accepted.
	for _, p := range peers[:10] {
		if _, err := rt.HandlePeerAlive(p); err != nil {
			t.Errorf("expected all 10 peers to be accepted; instead got: %v", err)
		}
	}

	// test next 5 peers are rejected.
	for _, p := range peers[10:] {
		if _, err := rt.HandlePeerAlive(p); err != ErrPeerRejectedNoCapacity {
			t.Errorf("expected extra 5 peers to be rejected; instead got: %v", err)
		}
	}
}

func TestTableFindMultiple(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(ctx, 20, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 18; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		rt.HandlePeerAlive(peers[i])
	}

	t.Logf("Searching for peer: '%s'", peers[2])
	found := rt.NearestPeers(ConvertPeerID(peers[2]), 15)
	if len(found) != 15 {
		t.Fatalf("Got back different number of peers than we expected.")
	}
}

func assertSortedPeerIdsEqual(t *testing.T, a, b []peer.ID) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatal("slices aren't the same length")
	}
	for i, p := range a {
		if p != b[i] {
			t.Fatalf("unexpected peer %d", i)
		}
	}
}

func TestTableFindMultipleBuckets(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := test.RandPeerIDFatal(t)
	localID := ConvertPeerID(local)
	m := pstore.NewMetrics()

	rt, err := NewRoutingTable(ctx, 5, localID, time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		rt.HandlePeerAlive(peers[i])
	}

	targetID := ConvertPeerID(peers[2])

	closest := SortClosestPeers(rt.ListPeers(), targetID)
	targetCpl := CommonPrefixLen(localID, targetID)

	// split the peers into closer, same, and further from the key (than us).
	var (
		closer, same, further []peer.ID
	)
	var i int
	for i = 0; i < len(closest); i++ {
		cpl := CommonPrefixLen(ConvertPeerID(closest[i]), targetID)
		if targetCpl >= cpl {
			break
		}
	}
	closer = closest[:i]

	var j int
	for j = len(closer); j < len(closest); j++ {
		cpl := CommonPrefixLen(ConvertPeerID(closest[j]), targetID)
		if targetCpl > cpl {
			break
		}
	}
	same = closest[i:j]
	further = closest[j:]

	// should be able to find at least 30
	// ~31 (logtwo(100) * 5)
	found := rt.NearestPeers(targetID, 20)
	if len(found) != 20 {
		t.Fatalf("asked for 20 peers, got %d", len(found))
	}

	// We expect this list to include:
	// * All peers with a common prefix length > than the CPL between our key
	//   and the target (peers in the target bucket).
	// * Then a subset of the peers with the _same_ CPL (peers in buckets to the right).
	// * Finally, other peers to the left of the target bucket.

	// First, handle the peers that are _closer_ than us.
	if len(found) <= len(closer) {
		// All of our peers are "closer".
		assertSortedPeerIdsEqual(t, found, closer[:len(found)])
		return
	}
	assertSortedPeerIdsEqual(t, found[:len(closer)], closer)

	// We've worked through closer peers, let's work through peers at the
	// "same" distance.
	found = found[len(closer):]

	// Next, handle peers that are at the same distance as us.
	if len(found) <= len(same) {
		// Ok, all the remaining peers are at the same distance.
		// unfortunately, that means we may be missing peers that are
		// technically closer.

		// Make sure all remaining peers are _somewhere_ in the "closer" set.
		pset := peer.NewSet()
		for _, p := range same {
			pset.Add(p)
		}
		for _, p := range found {
			if !pset.Contains(p) {
				t.Fatalf("unexpected peer %s", p)
			}
		}
		return
	}
	assertSortedPeerIdsEqual(t, found[:len(same)], same)

	// We've worked through closer peers, let's work through the further
	// peers.
	found = found[len(same):]

	// All _further_ peers will be correctly sorted.
	assertSortedPeerIdsEqual(t, found, further[:len(found)])

	// Finally, test getting _all_ peers. These should be in the correct
	// order.

	// Ok, now let's try finding all of them.
	found = rt.NearestPeers(ConvertPeerID(peers[2]), 100)
	if len(found) != rt.Size() {
		t.Fatalf("asked for %d peers, got %d", rt.Size(), len(found))
	}

	// We should get _all_ peers in sorted order.
	for i, p := range found {
		if p != closest[i] {
			t.Fatalf("unexpected peer %d", i)
		}
	}
}

// Looks for race conditions in table operations. For a more 'certain'
// test, increase the loop counter from 1000 to a much higher number
// and set GOMAXPROCS above 1
func TestTableMultithreaded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	local := peer.ID("localPeer")
	m := pstore.NewMetrics()
	tab, err := NewRoutingTable(ctx, 20, ConvertPeerID(local), time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(t, err)
	var peers []peer.ID
	for i := 0; i < 500; i++ {
		peers = append(peers, test.RandPeerIDFatal(t))
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			n := rand.Intn(len(peers))
			tab.HandlePeerAlive(peers[n])
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			n := rand.Intn(len(peers))
			tab.HandlePeerAlive(peers[n])
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			n := rand.Intn(len(peers))
			tab.Find(peers[n])
		}
		done <- struct{}{}
	}()
	<-done
	<-done
	<-done
}

func TestTableCleanup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
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

	// create RT with a very short cleanup interval
	rt, err := NewRoutingTable(ctx, 3, ConvertPeerID(local), time.Hour, pstore.NewMetrics(), PeerAlwaysValidFnc,
		TableCleanupInterval(100*time.Millisecond))
	require.NoError(t, err)

	// mock peer validation fnc that successfully validates p[1], p[3] & p[5]
	f := func(ctx context.Context, p peer.ID) bool {
		cpl := CommonPrefixLen(rt.local, ConvertPeerID(p))
		if cplPeerMap[cpl][1] == p || cplPeerMap[cpl][3] == p || cplPeerMap[cpl][5] == p {
			rt.HandlePeerAlive(p)
			return true

		} else {
			return false
		}
	}

	// for each CPL, p[0], p[1] & p[2] got the bucket & p[3], p[4] & p[5] become candidates
	for _, peers := range cplPeerMap {
		for _, p := range peers {
			rt.HandlePeerAlive(p)

		}
	}

	// validate current state
	rt.tabLock.RLock()
	require.Len(t, rt.ListPeers(), 6)
	ps0 := rt.Buckets[0].peerIds()
	require.Len(t, ps0, 3)
	ps1 := rt.Buckets[1].peerIds()
	require.Len(t, ps1, 3)
	require.Contains(t, ps0, cplPeerMap[0][0])
	require.Contains(t, ps0, cplPeerMap[0][1])
	require.Contains(t, ps0, cplPeerMap[0][2])
	require.Contains(t, ps1, cplPeerMap[1][0])
	require.Contains(t, ps1, cplPeerMap[1][1])
	require.Contains(t, ps1, cplPeerMap[1][2])
	rt.tabLock.RUnlock()

	// now change peer validation fnc
	rt.PeerValidationFnc = f

	// now mark p[0],p[1] & p[2] as dead so p[3] & p[5] replace p[0] and p[2]
	for _, peers := range cplPeerMap {
		rt.HandlePeerDisconnect(peers[0])
		rt.HandlePeerDisconnect(peers[1])
		rt.HandlePeerDisconnect(peers[2])
	}

	// let RT cleanup complete
	time.Sleep(1 * time.Second)

	// verify RT state
	rt.tabLock.RLock()
	require.Len(t, rt.ListPeers(), 6)
	ps0 = rt.Buckets[0].peerIds()
	require.Len(t, ps0, 3)
	ps1 = rt.Buckets[1].peerIds()
	require.Len(t, ps1, 3)
	require.Contains(t, ps0, cplPeerMap[0][1])
	require.Contains(t, ps0, cplPeerMap[0][3])
	require.Contains(t, ps0, cplPeerMap[0][5])
	require.Contains(t, ps1, cplPeerMap[1][1])
	require.Contains(t, ps1, cplPeerMap[1][3])
	require.Contains(t, ps1, cplPeerMap[1][5])
	rt.tabLock.RUnlock()
}

func BenchmarkHandlePeerAlive(b *testing.B) {
	ctx := context.Background()
	b.StopTimer()
	local := ConvertKey("localKey")
	m := pstore.NewMetrics()
	tab, err := NewRoutingTable(ctx, 20, local, time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(b, err)

	var peers []peer.ID
	for i := 0; i < b.N; i++ {
		peers = append(peers, test.RandPeerIDFatal(b))
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tab.HandlePeerAlive(peers[i])
	}
}

func BenchmarkFinds(b *testing.B) {
	ctx := context.Background()
	b.StopTimer()
	local := ConvertKey("localKey")
	m := pstore.NewMetrics()
	tab, err := NewRoutingTable(ctx, 20, local, time.Hour, m, PeerAlwaysValidFnc)
	require.NoError(b, err)

	var peers []peer.ID
	for i := 0; i < b.N; i++ {
		peers = append(peers, test.RandPeerIDFatal(b))
		tab.HandlePeerAlive(peers[i])
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tab.Find(peers[i])
	}
}
