package multisite

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// The fixture selects the zone link's network and peer itself: a request
// that names its own, a zone's link to itself, or unknown zones are refused
// before anything runs.
func TestTemporaryZoneLinkDelayRefusesBeforeAnythingRuns(t *testing.T) {
	probe := &clustertest.ContextLockRuntime{}
	f := &Fixture{sourceClient: probe, destinationClient: probe}
	loss := fault.NetworkProfile{HelperImage: "netem-helper:test", Loss: 100}
	for name, edit := range map[string]func(*fault.NetworkProfile){
		"a plane": func(d *fault.NetworkProfile) { d.Plane = ceph.PublicNetworkPlane },
		"a peer":  func(d *fault.NetworkProfile) { d.Peer = netip.MustParseAddr("10.88.0.7") },
		"a port":  func(d *fault.NetworkProfile) { d.PeerPort = 7480 },
		"peers":   func(d *fault.NetworkProfile) { d.Peers = []fault.NetworkPeer{} },
	} {
		delay := loss
		edit(&delay)
		if lease, err := f.ApplyZoneLinkProfile(t.Context(), "b", "a", delay); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
			t.Errorf("%s: %v, %v", name, lease, err)
		}
	}
	if _, err := f.ApplyZoneLinkProfile(t.Context(), "a", "a", loss); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("a link to itself: %v", err)
	}
	// Without the multisite bridge there is no link to delay.
	if _, err := f.ApplyZoneLinkProfile(t.Context(), "b", "a", loss); err == nil {
		t.Error("a fixture without a bridge accepted a zone link delay")
	}
	if probe.Execs.Load() != 0 {
		t.Fatal("a refused zone link delay ran a native command")
	}
}
