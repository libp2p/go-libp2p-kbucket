package kbucket

import (
	"testing"

	"github.com/libp2p/go-libp2p/core/test"
	"github.com/stretchr/testify/require"
)

func TestCloser(t *testing.T) {
	Pa := test.RandPeerIDFatal(t)
	Pb := test.RandPeerIDFatal(t)
	var X string

	// returns true if d(Pa, X) < d(Pb, X)
	for {
		X = string(test.RandPeerIDFatal(t))
		if Xor(ConvertPeerID(Pa), ConvertKey(X)).less(Xor(ConvertPeerID(Pb), ConvertKey(X))) {
			break
		}
	}

	require.True(t, Closer(Pa, Pb, X))

	// returns false if d(Pa,X) > d(Pb, X)
	for {
		X = string(test.RandPeerIDFatal(t))
		if Xor(ConvertPeerID(Pb), ConvertKey(X)).less(Xor(ConvertPeerID(Pa), ConvertKey(X))) {
			break
		}

	}
	require.False(t, Closer(Pa, Pb, X))
}
