package kbucket

import (
	"math/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/test"

	"github.com/libp2p/go-libp2p-kbucket/peerdiversity"
	pstore "github.com/libp2p/go-libp2p-peerstore"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var NoOpThreshold = 100 * time.Hour

func TestPrint(t *testing.T) {
	t.Parallel()
	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)
	rt.Print()
}

// Test basic features of the bucket struct
func TestBucket(t *testing.T) {
	t.Parallel()
	testTime1 := time.Now()
	testTime2 := time.Now().AddDate(1, 0, 0)

	b := newBucket()

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		b.pushFront(&PeerInfo{
			Id:                            peers[i],
			LastUsefulAt:                  testTime1,
			LastSuccessfulOutboundQueryAt: testTime2,
			AddedAt:                       testTime1,
			dhtId:                         ConvertPeerID(peers[i]),
		})
	}

	local := test.RandPeerIDFatal(t)
	localID := ConvertPeerID(local)

	infos := b.peers()
	require.Len(t, infos, 100)

	i := rand.Intn(len(peers))
	p := b.getPeer(peers[i])
	require.NotNil(t, p)
	require.Equal(t, peers[i], p.Id)
	require.Equal(t, ConvertPeerID(peers[i]), p.dhtId)
	require.EqualValues(t, testTime1, p.LastUsefulAt)
	require.EqualValues(t, testTime2, p.LastSuccessfulOutboundQueryAt)

	t2 := time.Now().Add(1 * time.Hour)
	t3 := t2.Add(1 * time.Hour)
	p.LastSuccessfulOutboundQueryAt = t2
	p.LastUsefulAt = t3
	p = b.getPeer(peers[i])
	require.NotNil(t, p)
	require.EqualValues(t, t2, p.LastSuccessfulOutboundQueryAt)
	require.EqualValues(t, t3, p.LastUsefulAt)

	spl := b.split(0, ConvertPeerID(local))
	llist := b.list
	for e := llist.Front(); e != nil; e = e.Next() {
		p := ConvertPeerID(e.Value.(*PeerInfo).Id)
		cpl := CommonPrefixLen(p, localID)
		if cpl > 0 {
			t.Fatalf("split failed. found id with cpl > 0 in 0 bucket")
		}
	}

	rlist := spl.list
	for e := rlist.Front(); e != nil; e = e.Next() {
		p := ConvertPeerID(e.Value.(*PeerInfo).Id)
		cpl := CommonPrefixLen(p, localID)
		if cpl == 0 {
			t.Fatalf("split failed. found id with cpl == 0 in non 0 bucket")
		}
	}

}

func TestNPeersForCpl(t *testing.T) {
	t.Parallel()
	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(2, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	require.Equal(t, 0, rt.NPeersForCpl(0))
	require.Equal(t, 0, rt.NPeersForCpl(1))

	// one peer with cpl 1
	p, _ := rt.GenRandPeerID(1)
	rt.TryAddPeer(p, true, false)
	require.Equal(t, 0, rt.NPeersForCpl(0))
	require.Equal(t, 1, rt.NPeersForCpl(1))
	require.Equal(t, 0, rt.NPeersForCpl(2))

	// one peer with cpl 0
	p, _ = rt.GenRandPeerID(0)
	rt.TryAddPeer(p, true, false)
	require.Equal(t, 1, rt.NPeersForCpl(0))
	require.Equal(t, 1, rt.NPeersForCpl(1))
	require.Equal(t, 0, rt.NPeersForCpl(2))

	// split the bucket with a peer with cpl 1
	p, _ = rt.GenRandPeerID(1)
	rt.TryAddPeer(p, true, false)
	require.Equal(t, 1, rt.NPeersForCpl(0))
	require.Equal(t, 2, rt.NPeersForCpl(1))
	require.Equal(t, 0, rt.NPeersForCpl(2))

	p, _ = rt.GenRandPeerID(0)
	rt.TryAddPeer(p, true, false)
	require.Equal(t, 2, rt.NPeersForCpl(0))
}

func TestEmptyBucketCollapse(t *testing.T) {
	t.Parallel()
	local := test.RandPeerIDFatal(t)

	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	// generate peers with cpl 0,1,2 & 3
	p1, _ := rt.GenRandPeerID(0)
	p2, _ := rt.GenRandPeerID(1)
	p3, _ := rt.GenRandPeerID(2)
	p4, _ := rt.GenRandPeerID(3)

	// remove peer on an empty bucket should not panic.
	rt.RemovePeer(p1)

	// add peer with cpl 0 and remove it..bucket should still exist as it's the ONLY bucket we have
	b, err := rt.TryAddPeer(p1, true, false)
	require.True(t, b)
	require.NoError(t, err)
	rt.RemovePeer(p1)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 1)
	rt.tabLock.Unlock()
	require.Empty(t, rt.ListPeers())

	// add peer with cpl 0 and cpl 1 and verify we have two buckets.
	b, err = rt.TryAddPeer(p1, true, false)
	require.True(t, b)
	b, err = rt.TryAddPeer(p2, true, false)
	require.True(t, b)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 2)
	rt.tabLock.Unlock()

	// removing a peer from the last bucket collapses it.
	rt.RemovePeer(p2)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 1)
	rt.tabLock.Unlock()
	require.Len(t, rt.ListPeers(), 1)
	require.Contains(t, rt.ListPeers(), p1)

	// add p2 again
	b, err = rt.TryAddPeer(p2, true, false)
	require.True(t, b)
	require.NoError(t, err)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 2)
	rt.tabLock.Unlock()

	// now remove a peer from the second-last i.e. first bucket and ensure it collapses
	rt.RemovePeer(p1)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 1)
	rt.tabLock.Unlock()
	require.Len(t, rt.ListPeers(), 1)
	require.Contains(t, rt.ListPeers(), p2)

	// let's have a total of 4 buckets now
	rt.TryAddPeer(p1, true, false)
	rt.TryAddPeer(p2, true, false)
	rt.TryAddPeer(p3, true, false)
	rt.TryAddPeer(p4, true, false)

	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 4)
	rt.tabLock.Unlock()

	// removing from 2,3 and then 4 leaves us with ONLY one bucket
	rt.RemovePeer(p2)
	rt.RemovePeer(p3)
	rt.RemovePeer(p4)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 1)
	rt.tabLock.Unlock()

	// an empty bucket in the middle DOES NOT collapse buckets
	rt.TryAddPeer(p1, true, false)
	rt.TryAddPeer(p2, true, false)
	rt.TryAddPeer(p3, true, false)
	rt.TryAddPeer(p4, true, false)

	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 4)
	rt.tabLock.Unlock()

	rt.RemovePeer(p2)
	rt.tabLock.Lock()
	require.Len(t, rt.buckets, 4)
	rt.tabLock.Unlock()
	require.NotContains(t, rt.ListPeers(), p2)
}

func TestRemovePeer(t *testing.T) {
	t.Parallel()
	local := test.RandPeerIDFatal(t)

	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(2, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	p1, _ := rt.GenRandPeerID(0)
	p2, _ := rt.GenRandPeerID(0)
	b, err := rt.TryAddPeer(p1, true, false)
	require.True(t, b)
	require.NoError(t, err)
	b, err = rt.TryAddPeer(p2, true, false)
	require.True(t, b)
	require.NoError(t, err)

	// ensure p1 & p2 are in the RT
	require.Len(t, rt.ListPeers(), 2)
	require.Contains(t, rt.ListPeers(), p1)
	require.Contains(t, rt.ListPeers(), p2)

	// remove a peer and ensure it's not in the RT
	require.NotEmpty(t, rt.Find(p1))
	rt.RemovePeer(p1)
	require.Empty(t, rt.Find(p1))
	require.NotEmpty(t, rt.Find(p2))
}

func TestTableCallbacks(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
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

	rt.TryAddPeer(peers[0], true, false)
	if _, ok := pset[peers[0]]; !ok {
		t.Fatal("should have this peer")
	}

	rt.RemovePeer(peers[0])
	if _, ok := pset[peers[0]]; ok {
		t.Fatal("should not have this peer")
	}

	for _, p := range peers {
		rt.TryAddPeer(p, true, false)
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

// Right now, this just makes sure that it doesnt hang or crash
func TestTryAddPeerLoad(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
	}

	for i := 0; i < 10000; i++ {
		rt.TryAddPeer(peers[rand.Intn(len(peers))], true, false)
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

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 5; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		rt.TryAddPeer(peers[i], true, false)
	}

	t.Logf("Searching for peer: '%s'", peers[2])
	found := rt.NearestPeer(ConvertPeerID(peers[2]))
	if !(found == peers[2]) {
		t.Fatalf("Failed to lookup known node...")
	}
}

func TestUpdateLastSuccessfulOutboundQueryAt(t *testing.T) {
	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	p := test.RandPeerIDFatal(t)
	b, err := rt.TryAddPeer(p, true, false)
	require.True(t, b)
	require.NoError(t, err)

	// increment and assert
	t2 := time.Now().Add(1 * time.Hour)
	rt.UpdateLastSuccessfulOutboundQueryAt(p, t2)
	rt.tabLock.Lock()
	pi := rt.buckets[0].getPeer(p)
	require.NotNil(t, pi)
	require.EqualValues(t, t2, pi.LastSuccessfulOutboundQueryAt)
	rt.tabLock.Unlock()
}

func TestUpdateLastUsefulAt(t *testing.T) {
	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	p := test.RandPeerIDFatal(t)
	b, err := rt.TryAddPeer(p, true, false)
	require.True(t, b)
	require.NoError(t, err)

	// increment and assert
	t2 := time.Now().Add(1 * time.Hour)
	rt.UpdateLastUsefulAt(p, t2)
	rt.tabLock.Lock()
	pi := rt.buckets[0].getPeer(p)
	require.NotNil(t, pi)
	require.EqualValues(t, t2, pi.LastUsefulAt)
	rt.tabLock.Unlock()
}

func TestTryAddPeer(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(2, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	// generate 2 peers to saturate the first bucket for cpl=0
	p1, _ := rt.GenRandPeerID(0)
	b, err := rt.TryAddPeer(p1, true, false)
	require.NoError(t, err)
	require.True(t, b)
	p2, _ := rt.GenRandPeerID(0)
	b, err = rt.TryAddPeer(p2, true, true)
	require.NoError(t, err)
	require.True(t, b)
	require.Equal(t, p1, rt.Find(p1))
	require.Equal(t, p2, rt.Find(p2))

	// trying to add a peer with cpl=0 works as p2 is replacable
	p3, _ := rt.GenRandPeerID(0)
	b, err = rt.TryAddPeer(p3, true, false)
	require.NoError(t, err)
	require.True(t, b)
	require.Equal(t, p3, rt.Find(p3))
	// p2 has been removed
	require.Empty(t, rt.Find(p2))

	// however adding peer fails as there are no more replacable peers.
	p5, err := rt.GenRandPeerID(0)
	require.NoError(t, err)
	b, err = rt.TryAddPeer(p5, true, false)
	require.Error(t, err)
	require.False(t, b)

	// however, trying to add peer with cpl=1 works
	p4, _ := rt.GenRandPeerID(1)
	b, err = rt.TryAddPeer(p4, true, false)
	require.NoError(t, err)
	require.True(t, b)
	require.Equal(t, p4, rt.Find(p4))

	// adding non query peer
	p6, err := rt.GenRandPeerID(3)
	require.NoError(t, err)
	b, err = rt.TryAddPeer(p6, false, false)
	require.NoError(t, err)
	require.True(t, b)
	rt.tabLock.Lock()
	pi := rt.buckets[rt.bucketIdForPeer(p6)].getPeer(p6)
	require.NotNil(t, p6)
	require.True(t, pi.LastUsefulAt.IsZero())
	rt.tabLock.Unlock()

}

func TestMarkAllPeersIrreplaceable(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(2, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	// generate 2 peers
	p1, _ := rt.GenRandPeerID(0)
	b, err := rt.TryAddPeer(p1, true, true)
	require.NoError(t, err)
	require.True(t, b)
	p2, _ := rt.GenRandPeerID(0)
	b, err = rt.TryAddPeer(p2, true, true)
	require.NoError(t, err)
	require.True(t, b)
	require.Equal(t, p1, rt.Find(p1))
	require.Equal(t, p2, rt.Find(p2))

	rt.MarkAllPeersIrreplaceable()
	ps := rt.GetPeerInfos()
	for i := range ps {
		require.False(t, ps[i].replaceable)
	}

}

func TestTableFindMultiple(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(20, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 18; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		rt.TryAddPeer(peers[i], true, false)
	}

	t.Logf("Searching for peer: '%s'", peers[2])
	found := rt.NearestPeers(ConvertPeerID(peers[2]), 15)
	if len(found) != 15 {
		t.Fatalf("Got back different number of peers than we expected.")
	}
}

func TestTableFindMultipleBuckets(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()

	rt, err := NewRoutingTable(5, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	peers := make([]peer.ID, 100)
	for i := 0; i < 100; i++ {
		peers[i] = test.RandPeerIDFatal(t)
		rt.TryAddPeer(peers[i], true, false)
	}

	closest := SortClosestPeers(rt.ListPeers(), ConvertPeerID(peers[2]))

	t.Logf("Searching for peer: '%s'", peers[2])

	// should be able to find at least 30
	// ~31 (logtwo(100) * 5)
	found := rt.NearestPeers(ConvertPeerID(peers[2]), 20)
	if len(found) != 20 {
		t.Fatalf("asked for 20 peers, got %d", len(found))
	}
	for i, p := range found {
		if p != closest[i] {
			t.Fatalf("unexpected peer %d", i)
		}
	}

	// Ok, now let's try finding all of them.
	found = rt.NearestPeers(ConvertPeerID(peers[2]), 100)
	if len(found) != rt.Size() {
		t.Fatalf("asked for %d peers, got %d", rt.Size(), len(found))
	}

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

	local := peer.ID("localPeer")
	m := pstore.NewMetrics()
	tab, err := NewRoutingTable(20, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)
	var peers []peer.ID
	for i := 0; i < 500; i++ {
		peers = append(peers, test.RandPeerIDFatal(t))
	}

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			n := rand.Intn(len(peers))
			tab.TryAddPeer(peers[n], true, false)
		}
		done <- struct{}{}
	}()

	go func() {
		for i := 0; i < 1000; i++ {
			n := rand.Intn(len(peers))
			tab.TryAddPeer(peers[n], true, false)
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

type mockPeerGroupFilter struct {
	peerAddressFunc func(p peer.ID) []ma.Multiaddr
	allowFnc        func(g peerdiversity.PeerGroupInfo) bool

	incrementFnc func(g peerdiversity.PeerGroupInfo)
	decrementFnc func(p peerdiversity.PeerGroupInfo)
}

func (m *mockPeerGroupFilter) Allow(g peerdiversity.PeerGroupInfo) (allow bool) {
	return m.allowFnc(g)
}

func (m *mockPeerGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	return m.peerAddressFunc(p)
}

func (m *mockPeerGroupFilter) Increment(g peerdiversity.PeerGroupInfo) {
	if m.incrementFnc != nil {
		m.incrementFnc(g)
	}
}

func (m *mockPeerGroupFilter) Decrement(g peerdiversity.PeerGroupInfo) {
	if m.decrementFnc != nil {
		m.decrementFnc(g)
	}
}

func TestDiversityFiltering(t *testing.T) {
	local := test.RandPeerIDFatal(t)
	cplCount := make(map[int]int)
	mg := &mockPeerGroupFilter{}
	mg.peerAddressFunc = func(p peer.ID) []ma.Multiaddr {
		return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")}
	}
	mg.allowFnc = func(g peerdiversity.PeerGroupInfo) bool {
		return cplCount[g.Cpl] < 1
	}

	mg.incrementFnc = func(g peerdiversity.PeerGroupInfo) {
		cplCount[g.Cpl] = cplCount[g.Cpl] + 1
	}

	mg.decrementFnc = func(g peerdiversity.PeerGroupInfo) {
		cplCount[g.Cpl] = cplCount[g.Cpl] - 1
	}

	df, err := peerdiversity.NewFilter(mg, "appname", func(p peer.ID) int {
		return CommonPrefixLen(ConvertPeerID(local), ConvertPeerID(p))
	})
	require.NoError(t, err)

	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, pstore.NewMetrics(), NoOpThreshold, df)
	require.NoError(t, err)
	p, _ := rt.GenRandPeerID(2)
	b, err := rt.TryAddPeer(p, true, false)
	require.NoError(t, err)
	require.True(t, b)

	p2, _ := rt.GenRandPeerID(2)
	b, err = rt.TryAddPeer(p2, true, false)
	require.Error(t, err)
	require.False(t, b)

	rt.RemovePeer(p)
	b, err = rt.TryAddPeer(p2, true, false)
	require.NoError(t, err)
	require.True(t, b)
}

func TestGetPeerInfos(t *testing.T) {
	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(10, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)

	require.Empty(t, rt.GetPeerInfos())

	p1 := test.RandPeerIDFatal(t)
	p2 := test.RandPeerIDFatal(t)

	b, err := rt.TryAddPeer(p1, false, false)
	require.True(t, b)
	require.NoError(t, err)
	b, err = rt.TryAddPeer(p2, true, false)
	require.True(t, b)
	require.NoError(t, err)

	ps := rt.GetPeerInfos()
	require.Len(t, ps, 2)
	ms := make(map[peer.ID]PeerInfo)
	for _, p := range ps {
		ms[p.Id] = p
	}

	require.Equal(t, p1, ms[p1].Id)
	require.True(t, ms[p1].LastUsefulAt.IsZero())
	require.Equal(t, p2, ms[p2].Id)
	require.False(t, ms[p2].LastUsefulAt.IsZero())
}

func TestPeerRemovedNotificationWhenPeerIsEvicted(t *testing.T) {
	t.Parallel()

	local := test.RandPeerIDFatal(t)
	m := pstore.NewMetrics()
	rt, err := NewRoutingTable(1, ConvertPeerID(local), time.Hour, m, NoOpThreshold, nil)
	require.NoError(t, err)
	pset := make(map[peer.ID]struct{})
	rt.PeerAdded = func(p peer.ID) {
		pset[p] = struct{}{}
	}
	rt.PeerRemoved = func(p peer.ID) {
		delete(pset, p)
	}

	p1, _ := rt.GenRandPeerID(0)
	p2, _ := rt.GenRandPeerID(0)

	// first peer works
	b, err := rt.TryAddPeer(p1, true, false)
	require.NoError(t, err)
	require.True(t, b)

	// second is rejected because of capacity
	b, err = rt.TryAddPeer(p2, true, false)
	require.False(t, b)
	require.Error(t, err)

	// pset has first peer
	require.Contains(t, pset, p1)
	require.NotContains(t, pset, p2)

	// mark peers as replacable so we can evict.
	i := rt.bucketIdForPeer(p1)
	rt.tabLock.Lock()
	bucket := rt.buckets[i]
	rt.tabLock.Unlock()
	bucket.getPeer(p1).replaceable = true

	b, err = rt.TryAddPeer(p2, true, false)
	require.NoError(t, err)
	require.True(t, b)
	require.Contains(t, pset, p2)
	require.NotContains(t, pset, p1)
}

func BenchmarkAddPeer(b *testing.B) {
	b.StopTimer()
	local := ConvertKey("localKey")
	m := pstore.NewMetrics()
	tab, err := NewRoutingTable(20, local, time.Hour, m, NoOpThreshold, nil)
	require.NoError(b, err)

	var peers []peer.ID
	for i := 0; i < b.N; i++ {
		peers = append(peers, test.RandPeerIDFatal(b))
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tab.TryAddPeer(peers[i], true, false)
	}
}

func BenchmarkFinds(b *testing.B) {
	b.StopTimer()
	local := ConvertKey("localKey")
	m := pstore.NewMetrics()
	tab, err := NewRoutingTable(20, local, time.Hour, m, NoOpThreshold, nil)
	require.NoError(b, err)

	var peers []peer.ID
	for i := 0; i < b.N; i++ {
		peers = append(peers, test.RandPeerIDFatal(b))
		tab.TryAddPeer(peers[i], true, false)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		tab.Find(peers[i])
	}
}
