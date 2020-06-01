package peerdiversity

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"

	asnutil "github.com/libp2p/go-libp2p-asn-util"
	"github.com/libp2p/go-libp2p-core/peer"

	logging "github.com/ipfs/go-log"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr-net"

	"github.com/yl2chen/cidranger"
)

var dfLog = logging.Logger("diversityFilter")

// PeerIPGroupKey is a unique key that represents ONE of the IP Groups the peer belongs to.
// A peer has one PeerIPGroupKey per address. Thus, a peer can belong to MULTIPLE Groups if it has
// multiple addresses.
// For now, given a peer address, our grouping mechanism is as follows:
// 1. For IPv6 addresses, we group by the ASN of the IP address.
// 2. For IPv4 addresses, all addresses that belong to same legacy (Class A)/8 allocations
//    OR share the same /16 prefix are in the same group.
type PeerIPGroupKey string

// https://en.wikipedia.org/wiki/List_of_assigned_/8_IPv4_address_blocks
var legacyClassA = []string{"12.0.0.0/8", "17.0.0.0/8", "19.0.0.0/8", "38.0.0.0/8", "48.0.0.0/8", "56.0.0.0/8", "73.0.0.0/8", "53.0.0.0/8"}

// PeerGroupInfo represents the grouping info for a Peer.
type PeerGroupInfo struct {
	Id         peer.ID
	Cpl        int
	IPGroupKey PeerIPGroupKey
}

// PeerIPGroupFilter is the interface that must be implemented by callers who want to
// instantiate a `peerdiversity.Filter`. This interface provides the function hooks
// that are used/called by the `peerdiversity.Filter`.
type PeerIPGroupFilter interface {
	// Allow is called by the Filter to test if a peer with the given
	// grouping info should be allowed/rejected by the Filter. This will be called ONLY
	// AFTER the peer has successfully passed all of the Filter's internal checks.
	// Note: If the peer is deemed accepted because of a whitelisting criteria configured on the Filter,
	// the peer will be allowed by the Filter without calling this function.
	// Similarly, if the peer is deemed rejected because of a blacklisting criteria
	// configured on the Filter, the peer will be rejected without calling this function.
	Allow(PeerGroupInfo) (allow bool)

	// Increment is called by the Filter when a peer with the given Grouping Info.
	// is added to the Filter state. This will happen after the peer has passed
	// all of the Filter's internal checks and the Allow function defined above for all of it's Groups.
	Increment(PeerGroupInfo)

	// Decrement is called by the Filter when a peer with the given
	// Grouping Info is removed from the Filter. This will happen when the caller/user of the Filter
	// no longer wants the peer and the IP groups it belongs to to count towards the Filter state.
	Decrement(PeerGroupInfo)

	// PeerAddresses is called by the Filter to determine the addresses of the given peer
	// it should use to determine the IP groups it belongs to.
	PeerAddresses(peer.ID) []ma.Multiaddr
}

// Filter is a peer diversity filter that accepts or rejects peers based on the blacklisting/whitelisting
// rules configured AND the diversity policies defined by the implementation of the PeerIPGroupFilter interface
// passed to it.
type Filter struct {
	mu sync.Mutex
	// An implementation of the `PeerIPGroupFilter` interface defined above.
	pgm        PeerIPGroupFilter
	peerGroups map[peer.ID][]PeerGroupInfo
	// whitelisted Networks
	wls cidranger.Ranger
	// blacklisted Networks.
	bls cidranger.Ranger
	// legacy IPv4 Class A networks.
	legacyCidrs cidranger.Ranger

	logKey string

	cplFnc func(peer.ID) int

	cplPeerGroups map[int]map[peer.ID][]PeerIPGroupKey
	// TODO This is for testing/logging purpose ONLY and can/should be removed later.
	//cplRejections map[int]map[peer.ID]string
}

func NewFilter(pgm PeerIPGroupFilter, appName string, cplFnc func(peer.ID) int) (*Filter, error) {
	if pgm == nil {
		return nil, errors.New("peergroup implementation can not be nil")
	}

	// Crate a Trie for legacy Class N networks
	legacyCidrs := cidranger.NewPCTrieRanger()
	for _, cidr := range legacyClassA {
		_, nn, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, err
		}
		if err := legacyCidrs.Insert(cidranger.NewBasicRangerEntry(*nn)); err != nil {
			return nil, err
		}
	}

	return &Filter{
		pgm:           pgm,
		peerGroups:    make(map[peer.ID][]PeerGroupInfo),
		wls:           cidranger.NewPCTrieRanger(),
		bls:           cidranger.NewPCTrieRanger(),
		legacyCidrs:   legacyCidrs,
		logKey:        appName,
		cplFnc:        cplFnc,
		cplPeerGroups: make(map[int]map[peer.ID][]PeerIPGroupKey),
		//cplRejections: make(map[int]map[peer.ID]string),
	}, nil
}

func (f *Filter) Remove(p peer.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	cpl := f.cplFnc(p)

	for _, info := range f.peerGroups[p] {
		f.pgm.Decrement(info)
	}
	f.peerGroups[p] = nil
	delete(f.peerGroups, p)
	delete(f.cplPeerGroups[cpl], p)

	if len(f.cplPeerGroups[cpl]) == 0 {
		delete(f.cplPeerGroups, cpl)
	}
}

// returns true if peer was accepted and added to the Filter state.
func (f *Filter) AddIfAllowed(p peer.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	cpl := f.cplFnc(p)

	// don't allow peers for which we can't determine addresses.
	addrs := f.pgm.PeerAddresses(p)
	if len(addrs) == 0 {
		dfLog.Debugw("no addresses found for peer", "appKey", f.logKey, "peer", p.Pretty())
		return false
	}

	peerGroups := make([]PeerGroupInfo, 0, len(addrs))
	for _, a := range addrs {
		// if the IP belongs to a whitelisted network, allow it straight away.
		// if the IP belongs to a blacklisted network, reject it.
		// Otherwise, call the `PeerIPGroupFilter.Allow` hook to determine if we should allow/reject the peer.
		ip, err := manet.ToIP(a)
		if err != nil {
			dfLog.Errorw("failed to parse IP from multiaddr", "appKey", f.logKey,
				"multiaddr", a.String(), "err", err)
			return false
		}

		// reject the peer if we can't determine a grouping for one of it's address.
		key, err := f.ipGroupKey(ip)
		if err != nil {
			dfLog.Errorw("failed to find Group Key", "appKey", f.logKey, "ip", ip.String(), "peer", p,
				"err", err)
			return false
		}
		if len(key) == 0 {
			dfLog.Errorw("group key is empty", "appKey", f.logKey, "ip", ip.String(), "peer", p)
			return false
		}
		group := PeerGroupInfo{Id: p, Cpl: cpl, IPGroupKey: key}

		if rs, _ := f.wls.ContainingNetworks(ip); len(rs) != 0 {
			peerGroups = append(peerGroups, group)
			continue
		}

		if rs, _ := f.bls.ContainingNetworks(ip); len(rs) != 0 {
			return false
		}

		if !f.pgm.Allow(group) {
			/*_, ok := f.cplRejections[cpl]
			if !ok {
				f.cplRejections[cpl] = make(map[peer.ID]string)
			}
			f.cplRejections[cpl][p] = ip.String()*/
			return false
		}

		peerGroups = append(peerGroups, group)
	}

	_, ok := f.cplPeerGroups[cpl]
	if !ok {
		f.cplPeerGroups[cpl] = make(map[peer.ID][]PeerIPGroupKey)
	}

	// add
	for _, g := range peerGroups {
		f.pgm.Increment(g)
		f.peerGroups[p] = append(f.peerGroups[p], g)
		f.cplPeerGroups[cpl][p] = append(f.cplPeerGroups[cpl][p], g.IPGroupKey)
	}

	//delete(f.cplRejections[cpl], p)

	return true
}

// BlacklistIPv4Network will blacklist the IPv4/6 network with the given IP CIDR.
func (f *Filter) BlacklistIPNetwork(cidr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, nn, err := net.ParseCIDR(cidr)
	if err != nil {
		return err
	}
	return f.bls.Insert(cidranger.NewBasicRangerEntry(*nn))
}

// WhitelistIPNetwork will always allow IP addresses from networks with the given CIDR.
// This will always override the blacklist.
func (f *Filter) WhitelistIPNetwork(cidr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	_, nn, err := net.ParseCIDR(cidr)
	if err != nil {
		return err
	}

	return f.wls.Insert(cidranger.NewBasicRangerEntry(*nn))
}

// returns the PeerIPGroupKey to which the given IP belongs.
func (f *Filter) ipGroupKey(ip net.IP) (PeerIPGroupKey, error) {
	switch bz := ip.To4(); bz {
	case nil:
		// TODO Clean up the ASN codebase
		// ipv6 Address -> get ASN
		s, err := asnutil.Store.AsnForIPv6(ip)
		if err != nil {
			return "", fmt.Errorf("failed to fetch ASN for IPv6 addr %s: %w", ip.String(), err)
		}
		return PeerIPGroupKey(s), nil
	default:
		// If it belongs to a legacy Class 8, we return the /8 prefix as the key
		rs, _ := f.legacyCidrs.ContainingNetworks(ip)
		if len(rs) != 0 {
			key := ip.Mask(net.IPv4Mask(255, 0, 0, 0)).String()
			return PeerIPGroupKey(key), nil
		}

		// otherwise -> /16 prefix
		key := ip.Mask(net.IPv4Mask(255, 255, 0, 0)).String()
		return PeerIPGroupKey(key), nil
	}
}

func (f *Filter) PrintStats() {
	f.mu.Lock()
	defer f.mu.Unlock()

	fmt.Printf("\n --------------Peer Diversity Stats for [%s] At %v----------------", f.logKey, time.Now().String())

	var sortedCpls []int
	for cpl := range f.cplPeerGroups {
		sortedCpls = append(sortedCpls, cpl)
	}
	sort.Ints(sortedCpls)

	for cpl := range sortedCpls {
		fmt.Printf("\n\t Cpl=%d\tTotalPeers=%d", cpl, len(f.cplPeerGroups[cpl]))
		for p, groups := range f.cplPeerGroups[cpl] {
			fmt.Printf("\n\t\t\t - Peer=%s\tGroups=%v", p.Pretty(), groups)
		}
	}
	fmt.Println("\n-------------------------------------------------------------------")

	/*fmt.Printf("\n-------- Rejection Stats till now -----------------------------------")
	var sortedRejectedCpls []int
	for cpl := range f.cplRejections {
		sortedRejectedCpls = append(sortedRejectedCpls, cpl)
	}
	sort.Ints(sortedRejectedCpls)

	for cpl := range sortedRejectedCpls {
		fmt.Printf("\n\t Cpl=%d\tTotalRejectedPeers=%d", cpl, len(f.cplRejections[cpl]))
		for p, a := range f.cplRejections[cpl] {
			fmt.Printf("\n\t\t\t - Peer=%s\tAddress=%v", p.Pretty(), a)
		}
	}
	fmt.Printf("\n-----------------------------------------------------------------------------")*/
}
