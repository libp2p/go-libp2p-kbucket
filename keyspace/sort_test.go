package keyspace

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
)

func randKeys(rng *rand.Rand, n int) []Key {
	keys := make([]Key, n)
	for i := range keys {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint64(buf, rng.Uint64())
		binary.BigEndian.PutUint64(buf[8:], rng.Uint64())
		keys[i] = XORKeySpace.Key(buf)
	}
	return keys
}

func TestSortByDistanceIsSorted(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 17, 200} {
		keys := randKeys(rand.New(rand.NewPCG(99, 99)), n)
		center := XORKeySpace.Key([]byte("center"))

		sorted := SortByDistance(XORKeySpace, center, keys)

		if len(sorted) != n {
			t.Fatalf("n=%d: got %d keys, want %d", n, len(sorted), n)
		}
		for i := 1; i < len(sorted); i++ {
			if center.Distance(sorted[i-1]).Cmp(center.Distance(sorted[i])) > 0 {
				t.Fatalf("n=%d: not sorted at index %d", n, i)
			}
		}
	}
}

func TestSortByDistanceDoesNotMutateInput(t *testing.T) {
	keys := randKeys(rand.New(rand.NewPCG(7, 7)), 64)
	original := slices.Clone(keys)
	center := XORKeySpace.Key([]byte("center"))

	SortByDistance(XORKeySpace, center, keys)

	for i := range keys {
		if !bytes.Equal(keys[i].Bytes, original[i].Bytes) {
			t.Fatalf("input mutated at index %d", i)
		}
	}
}

func BenchmarkSortByDistance(b *testing.B) {
	center := XORKeySpace.Key([]byte("center"))
	for _, n := range []int{16, 64, 256, 1024} {
		keys := randKeys(rand.New(rand.NewPCG(1, 1)), n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				SortByDistance(XORKeySpace, center, keys)
			}
		})
	}
}
