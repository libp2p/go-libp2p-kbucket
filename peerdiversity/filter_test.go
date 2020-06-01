package peerdiversity

import (
	"net"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p-core/peer"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"
)

var _ PeerIPGroupFilter = (*mockPeerGroupFilter)(nil)

type mockPeerGroupFilter struct {
	mu         sync.Mutex
	increments map[peer.ID]struct{}
	decrements map[peer.ID]struct{}

	peerAddressFunc func(p peer.ID) []ma.Multiaddr
	allowFnc        func(g PeerGroupInfo) bool
	incrementFunc   func(g PeerGroupInfo)
	dectementFunc   func(g PeerGroupInfo)
}

func (m *mockPeerGroupFilter) Allow(g PeerGroupInfo) (allow bool) {
	return m.allowFnc(g)
}

func (m *mockPeerGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	return m.peerAddressFunc(p)
}

func (m *mockPeerGroupFilter) Increment(g PeerGroupInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.increments[g.Id] = struct{}{}
}

func (m *mockPeerGroupFilter) Decrement(g PeerGroupInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.decrements[g.Id] = struct{}{}
}

func newMockPeerGroupFilter() *mockPeerGroupFilter {
	m := &mockPeerGroupFilter{
		increments: map[peer.ID]struct{}{},
		decrements: map[peer.ID]struct{}{},

		peerAddressFunc: func(p peer.ID) []ma.Multiaddr {
			return nil
		},
		allowFnc: func(g PeerGroupInfo) bool {
			return false
		},
	}

	return m
}

func TestDiversityFilter(t *testing.T) {
	tcs := map[string]struct {
		peersForTest func() []peer.ID
		mFnc         func(m *mockPeerGroupFilter)
		fFnc         func(f *Filter)
		allowed      map[peer.ID]bool
	}{
		"simple allow": {
			peersForTest: func() []peer.ID {
				return []peer.ID{"p1", "p2"}
			},
			mFnc: func(m *mockPeerGroupFilter) {
				m.peerAddressFunc = func(id peer.ID) []ma.Multiaddr {
					return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0")}
				}
				m.allowFnc = func(g PeerGroupInfo) bool {
					if g.Id == "p1" {
						return true
					}
					return false
				}
			},
			allowed: map[peer.ID]bool{
				"p1": true,
				"p2": false,
			},
			fFnc: func(f *Filter) {},
		},

		"one address is allowed, one isn't": {
			peersForTest: func() []peer.ID {
				return []peer.ID{"p1", "p2"}
			},
			mFnc: func(m *mockPeerGroupFilter) {
				m.peerAddressFunc = func(id peer.ID) []ma.Multiaddr {
					if id == "p1" {
						return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0"),
							ma.StringCast("/ip4/127.0.0.1/tcp/0")}
					} else {
						return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0"),
							ma.StringCast("/ip4/192.168.1.1/tcp/0")}
					}

				}
				m.allowFnc = func(g PeerGroupInfo) bool {
					if g.IPGroupKey == "127.0.0.0" {
						return true
					}

					return false
				}
			},
			allowed: map[peer.ID]bool{
				"p1": true,
				"p2": false,
			},
			fFnc: func(f *Filter) {},
		},

		"whitelisting": {
			peersForTest: func() []peer.ID {
				return []peer.ID{"p1", "p2"}
			},
			mFnc: func(m *mockPeerGroupFilter) {
				m.peerAddressFunc = func(id peer.ID) []ma.Multiaddr {
					if id == "p1" {
						return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0"),
							ma.StringCast("/ip4/127.0.0.1/tcp/0")}
					} else {
						return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0"),
							ma.StringCast("/ip4/192.168.1.1/tcp/0")}
					}
				}

				m.allowFnc = func(g PeerGroupInfo) bool {
					return false
				}
			},
			allowed: map[peer.ID]bool{
				"p1": true,
				"p2": false,
			},
			fFnc: func(f *Filter) {
				err := f.WhitelistIPNetwork("127.0.0.1/16")
				if err != nil {
					t.Fatal(err)
				}
			},
		},

		"blacklisting": {
			peersForTest: func() []peer.ID {
				return []peer.ID{"p1", "p2"}
			},
			mFnc: func(m *mockPeerGroupFilter) {
				m.peerAddressFunc = func(id peer.ID) []ma.Multiaddr {
					if id == "p1" {
						return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0"),
							ma.StringCast("/ip4/127.0.0.1/tcp/0")}
					} else {
						return []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/0"),
							ma.StringCast("/ip4/192.168.1.1/tcp/0")}
					}
				}

				m.allowFnc = func(g PeerGroupInfo) bool {
					return true
				}
			},
			allowed: map[peer.ID]bool{
				"p1": true,
				"p2": false,
			},
			fFnc: func(f *Filter) {
				err := f.BlacklistIPNetwork("192.168.1.1/16")
				if err != nil {
					t.Fatal(err)
				}
			},
		},

		"peer has no addresses": {
			peersForTest: func() []peer.ID {
				return []peer.ID{"p1"}
			},
			mFnc: func(m *mockPeerGroupFilter) {
				m.peerAddressFunc = func(id peer.ID) []ma.Multiaddr {
					return nil
				}
				m.allowFnc = func(g PeerGroupInfo) bool {
					return true
				}
			},
			allowed: map[peer.ID]bool{
				"p1": false,
			},
			fFnc: func(f *Filter) {},
		},
	}

	for name, tc := range tcs {
		t.Run(name, func(t *testing.T) {
			m := newMockPeerGroupFilter()
			tc.mFnc(m)
			f, err := NewFilter(m, "test", func(p peer.ID) int { return 1 })
			require.NoError(t, err, name)
			tc.fFnc(f)

			for _, p := range tc.peersForTest() {
				b := f.AddIfAllowed(p)
				v, ok := tc.allowed[p]
				require.True(t, ok, string(p))
				require.Equal(t, v, b, string(p))

				if v {
					m.mu.Lock()
					_, ok := m.increments[p]
					m.mu.Unlock()
					require.True(t, ok)
					f.Remove(p)
					m.mu.Lock()
					_, ok = m.decrements[p]
					require.True(t, ok)
					m.mu.Unlock()
				}
			}
		})
	}
}

func TestIPGroupKey(t *testing.T) {
	f, err := NewFilter(newMockPeerGroupFilter(), "test", func(p peer.ID) int { return 1 })
	require.NoError(t, err)

	// case 1 legacy /8
	ip := net.ParseIP("17.111.0.1")
	require.NotNil(t, ip.To4())
	g, err := f.ipGroupKey(ip)
	require.NoError(t, err)
	require.Equal(t, "17.0.0.0", string(g))

	// case2 ip4 /8
	ip = net.ParseIP("192.168.1.1")
	require.NotNil(t, ip.To4())
	g, err = f.ipGroupKey(ip)
	require.NoError(t, err)
	require.Equal(t, "192.168.0.0", string(g))

	// case3 ipv6
	ip = net.ParseIP("2a03:2880:f003:c07:face:b00c::2")
	g, err = f.ipGroupKey(ip)
	require.NoError(t, err)
	require.Equal(t, "32934", string(g))
}
