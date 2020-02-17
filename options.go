package kbucket

import (
	"fmt"
	"time"
)

// Option is the Routing Table functional option type.
type Option func(*Options) error

// Options is a structure containing all the functional options that can be used when constructing a Routing Table.
type Options struct {
	TableCleanup struct {
		PeerValidationFnc     PeerValidationFunc
		PeersForValidationFnc PeerSelectionFunc
		PeerValidationTimeout time.Duration
		Interval              time.Duration
	}
}

// Apply applies the given options to this Option.
func (o *Options) Apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(o); err != nil {
			return fmt.Errorf("routing table option %d failed: %s", i, err)
		}
	}
	return nil
}

// PeerValidationFnc configures the Peer Validation function used for RT cleanup.
// Not configuring this disables Routing Table cleanup.
func PeerValidationFnc(f PeerValidationFunc) Option {
	return func(o *Options) error {
		o.TableCleanup.PeerValidationFnc = f
		return nil
	}
}

// PeersForValidationFnc configures the function that will be used to select the peers that need to be validated during cleanup.
func PeersForValidationFnc(f PeerSelectionFunc) Option {
	return func(o *Options) error {
		o.TableCleanup.PeersForValidationFnc = f
		return nil
	}
}

// TableCleanupInterval configures the interval between two runs of the Routing Table cleanup routine.
func TableCleanupInterval(i time.Duration) Option {
	return func(o *Options) error {
		o.TableCleanup.Interval = i
		return nil
	}
}

// PeerValidationTimeout sets the timeout for a single peer validation during cleanup.
func PeerValidationTimeout(timeout time.Duration) Option {
	return func(o *Options) error {
		o.TableCleanup.PeerValidationTimeout = timeout
		return nil
	}
}

// Defaults are the default options. This option will be automatically
// prepended to any options you pass to the Routing Table constructor.
var Defaults = func(o *Options) error {
	o.TableCleanup.PeerValidationTimeout = 30 * time.Second
	o.TableCleanup.Interval = 2 * time.Minute

	// default selector function selects all peers that are in missing state.
	o.TableCleanup.PeersForValidationFnc = func(peers []PeerInfo) []PeerInfo {
		var selectedPeers []PeerInfo
		for _, p := range peers {
			if p.State == PeerStateMissing {
				selectedPeers = append(selectedPeers, p)
			}
		}
		return selectedPeers
	}

	return nil
}
