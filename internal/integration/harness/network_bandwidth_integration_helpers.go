//go:build all || integration

package harness

import (
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
)

const (
	BandwidthPool = "bandwidth"
	// BandwidthObject is large enough that one read through the limit takes
	// several seconds, so that the session's setup and the first window of
	// a TCP connection do not decide the measured throughput.
	BandwidthObject = 16 << 20
	BandwidthRate   = 16 * fault.MegabitPerSecond
	// A limited read must stay at or below the rate, with a margin for the
	// clock, and keep at least this fraction of it: bounded, not stalled.
	BandwidthCeiling = 1.1
	BandwidthFloor   = 0.4
	// An unlimited read must be at least this many times the rate.
	BandwidthMargin = 4.0
)

// RateBytes is the limit in bytes per second.
func RateBytes(rate fault.NetworkRate) float64 {
	return float64(rate) / 8
}
