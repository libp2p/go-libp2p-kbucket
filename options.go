package kbucket

import (
	"fmt"
	"math"
)

// Option is the Routing Table functional option type.
type Option func(*options) error

// options is a structure containing all the functional options that can be used when constructing a Routing Table.
type options struct {
	usefulnessCounter struct {
		defaultValue float64
		// threshold below which we will prefer a new peer to an existing peer
		minThreshold float64
	}
}

// apply applies the given options to this option.
func (o *options) apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(o); err != nil {
			return fmt.Errorf("routing table option %d failed: %s", i, err)
		}
	}
	return nil
}

// DefaultUsefulnessCounter configures the default value for the
// usefulness counter metric for a newly added peer to the Routing Table.
func DefaultUsefulnessCounter(val float64) Option {
	return func(o *options) error {
		o.usefulnessCounter.defaultValue = val
		return nil
	}
}

// UsefulnessCounterMinThreshold configures the min threshold for the
// usefulness counter value. We will prefer an existing peer over a "new" peer
// ONLY if the usefulness counter value of the existing peer is above this
// threshold.
func UsefulnessCounterMinThreshold(val float64) Option {
	return func(o *options) error {
		o.usefulnessCounter.minThreshold = val
		return nil
	}
}

// Defaults are the default options. This option will be automatically
// prepended to any options you pass to the Routing Table constructor.
var Defaults = func(o *options) error {
	// TODO Is this a meaningful default ?
	o.usefulnessCounter.defaultValue = 1.0
	// Please refer to https://docs.google.com/document/d/1AZoU2FLa8ko1UWStt1kvNkBDvqTUwIRdwUvCROMBoXk
	o.usefulnessCounter.minThreshold = math.Pow(math.E, -math.Ln2)
	return nil
}
