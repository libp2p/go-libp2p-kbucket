package kbucket

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	mh "github.com/multiformats/go-multihash"
)

// maxCplForRefresh is the maximum cpl we support for refresh.
// This limit exists because we can only generate 'maxCplForRefresh' bit prefixes for now.
const maxCplForRefresh uint = 15

// GetTrackedCplsForRefresh returns the Cpl's we are tracking for refresh.
// Caller is free to modify the returned slice as it is a defensive copy.
func (rt *RoutingTable) GetTrackedCplsForRefresh() []time.Time {
	maxCommonPrefix := rt.maxCommonPrefix()
	if maxCommonPrefix > maxCplForRefresh {
		maxCommonPrefix = maxCplForRefresh
	}

	rt.cplRefreshLk.RLock()
	defer rt.cplRefreshLk.RUnlock()

	cpls := make([]time.Time, maxCommonPrefix+1)
	for i := uint(0); i <= maxCommonPrefix; i++ {
		// defaults to the zero value if we haven't refreshed it yet.
		cpls[i] = rt.cplRefreshedAt[i]
	}
	return cpls
}

func randUint16() (uint16, error) {
	// Read a random prefix.
	var prefixBytes [2]byte
	_, err := rand.Read(prefixBytes[:])
	return binary.BigEndian.Uint16(prefixBytes[:]), err
}

// GenRandPeerID generates a random peerID for a given Cpl
func (rt *RoutingTable) GenRandPeerID(targetCpl uint) (peer.ID, error) {
	if targetCpl > maxCplForRefresh {
		return "", fmt.Errorf("cannot generate peer ID for Cpl greater than %d", maxCplForRefresh)
	}

	localPrefix := binary.BigEndian.Uint16(rt.local)

	// For host with ID `L`, an ID `K` belongs to a bucket with ID `B` ONLY IF CommonPrefixLen(L,K) is EXACTLY B.
	// Hence, to achieve a targetPrefix `T`, we must toggle the (T+1)th bit in L & then copy (T+1) bits from L
	// to our randomly generated prefix.
	toggledLocalPrefix := localPrefix ^ (uint16(0x8000) >> targetCpl)
	randPrefix, err := randUint16()
	if err != nil {
		return "", err
	}

	// Combine the toggled local prefix and the random bits at the correct offset
	// such that ONLY the first `targetCpl` bits match the local ID.
	mask := (^uint16(0)) << (16 - (targetCpl + 1))
	targetPrefix := (toggledLocalPrefix & mask) | (randPrefix & ^mask)

	// Convert to a known peer ID.
	key := keyPrefixMap[targetPrefix]
	id := [Keysize + 2]byte{mh.SHA2_256, Keysize}
	binary.BigEndian.PutUint32(id[2:], key)
	return peer.ID(id[:]), nil
}

// GenRandomKey generates a random key matching a provided Common Prefix Length (Cpl) wrt. the local identity
func (rt *RoutingTable) GenRandomKey(targetCpl uint) (ID, error) {
	// targetCpl cannot be larger than the key size in bits
	if targetCpl >= Keysize*8 {
		return nil, fmt.Errorf("cannot generate peer ID for Cpl greater than key length")
	}

	// the generated key must match the targetCpl first bits of the local key,
	// the following bit is the inverse of the local key's bit at position targetCpl+1
	// and the remaining bits are randomly generated
	//
	// The returned ID takes the first targetCpl/8 bytes from the local key, the next byte is
	// targetCpl%8 bits from the local key, 1 bit from the local key inverted and the remaining
	// bits are random, and the last bytes are random

	// generate random bytes
	nRandBytes := Keysize - (targetCpl+1)/8
	randBytes := make([]byte, nRandBytes)
	_, err := rand.Read(randBytes)
	if err != nil {
		return nil, err
	}

	// byte and bit offset for the given cpl
	byteOffset := targetCpl / 8
	bitOffset := targetCpl % 8

	// copy the local key to a new slice
	randKey := make([]byte, len(rt.local))
	copy(randKey, []byte(rt.local))

	// compute the mask for the local key, the first targetCpl bits must be the same as the local key
	// hence the mask is 1s for bits up to targetCpl and 0s for the rest
	localMask := (^byte(0)) << (8 - bitOffset)
	// compute the bit that is flipped in the local key (at position targetCpl+1)
	bucketBit := (byte(0x80) >> bitOffset) & ^rt.local[targetCpl/8]

	// first bitOffset bits are the same as the local key, the next bit is flipped and the remaining bits are random
	randKey[byteOffset] = (localMask & randKey[byteOffset]) | bucketBit | (randBytes[0] & ((^localMask) >> 1))
	// the remaining bytes are random
	for i := uint(1); i < nRandBytes; i++ {
		randKey[byteOffset+i] = randBytes[i]
	}

	return randKey, nil
}

// ResetCplRefreshedAtForID resets the refresh time for the Cpl of the given ID.
func (rt *RoutingTable) ResetCplRefreshedAtForID(id ID, newTime time.Time) {
	cpl := CommonPrefixLen(id, rt.local)
	if uint(cpl) > maxCplForRefresh {
		return
	}

	rt.cplRefreshLk.Lock()
	defer rt.cplRefreshLk.Unlock()

	rt.cplRefreshedAt[uint(cpl)] = newTime
}
