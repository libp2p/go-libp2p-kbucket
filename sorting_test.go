package kbucket

import (
	"slices"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/test"
)

func randPeers(t testing.TB, n int) []peer.ID {
	t.Helper()
	peers := make([]peer.ID, n)
	for i := range peers {
		p, err := test.RandPeerID()
		if err != nil {
			t.Fatal(err)
		}
		peers[i] = p
	}
	return peers
}

func TestSortClosestPeersIsSorted(t *testing.T) {
	target := ConvertPeerID(randPeers(t, 1)[0])

	for _, n := range []int{0, 1, 2, 3, 17, 200} {
		peers := randPeers(t, n)

		sorted := SortClosestPeers(peers, target)

		if len(sorted) != n {
			t.Fatalf("n=%d: got %d peers, want %d", n, len(sorted), n)
		}
		for i := 1; i < len(sorted); i++ {
			prev := Xor(target, ConvertPeerID(sorted[i-1]))
			cur := Xor(target, ConvertPeerID(sorted[i]))
			if prev.compare(cur) > 0 {
				t.Fatalf("n=%d: not sorted at index %d", n, i)
			}
		}
	}
}

func TestSortClosestPeersDoesNotMutateInput(t *testing.T) {
	peers := randPeers(t, 64)
	original := slices.Clone(peers)
	target := ConvertPeerID(peers[0])

	SortClosestPeers(peers, target)

	for i := range peers {
		if peers[i] != original[i] {
			t.Fatalf("input mutated at index %d", i)
		}
	}
}
