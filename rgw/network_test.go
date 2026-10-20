package rgw

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// The fixture selects the zone link's network and peer itself: a request
// that names its own, a zone's link to itself, or unknown zones are refused
// before anything runs.
func TestTemporaryZoneLinkDelayRefusesBeforeAnythingRuns(t *testing.T) {
	probe := &clustertest.ContextLockRuntime{}
	f := &Multisite{sourceClient: probe, destinationClient: probe}
	loss := ceph.NetworkDelay{HelperImage: "netem-helper:test", Loss: 100}
	for name, edit := range map[string]func(*ceph.NetworkDelay){
		"a plane": func(d *ceph.NetworkDelay) { d.Plane = ceph.PublicNetworkPlane },
		"a peer":  func(d *ceph.NetworkDelay) { d.Peer = netip.MustParseAddr("10.88.0.7") },
		"a port":  func(d *ceph.NetworkDelay) { d.PeerPort = 7480 },
		"peers":   func(d *ceph.NetworkDelay) { d.Peers = []ceph.NetworkPeer{} },
	} {
		delay := loss
		edit(&delay)
		if lease, err := f.TemporaryZoneLinkDelay(t.Context(), "b", "a", delay); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
			t.Errorf("%s: %v, %v", name, lease, err)
		}
	}
	if _, err := f.TemporaryZoneLinkDelay(t.Context(), "a", "a", loss); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Errorf("a link to itself: %v", err)
	}
	// Without the multisite bridge there is no link to delay.
	if _, err := f.TemporaryZoneLinkDelay(t.Context(), "b", "a", loss); err == nil {
		t.Error("a fixture without a bridge accepted a zone link delay")
	}
	if probe.Execs.Load() != 0 {
		t.Fatal("a refused zone link delay ran a native command")
	}
}
