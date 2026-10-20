package ceph

import (
	"errors"
	"net/netip"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// A link delay names its bridge instead of a plane, and admits only an
// endpoint on a bridge network.
func TestTemporaryLinkDelay(t *testing.T) {
	peer := []NetworkPeer{{Addr: netip.MustParseAddr("10.88.0.7"), Port: 7480}}
	link := NetworkDelay{HelperImage: "netem-helper:test", Loss: 100, Peers: peer}
	planed := link
	planed.Plane = PublicNetworkPlane
	osd := &fakeNetemHelper{id: "osd-0"}
	for name, test := range map[string]struct {
		delay   NetworkDelay
		network string
		target  testcontainers.Container
	}{
		"a plane":         {planed, "tc-public", osd},
		"no network":      {link, " ", osd},
		"no container":    {link, "tc-public", nil},
		"another network": {link, "elsewhere", osd},
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			lease, err := temporaryLinkDelay(t.Context(), netem, test.target, test.network, test.delay)
			if !errors.Is(err, ErrInvalidRequest) || lease != nil || netem.liveHelpers() != 0 || len(tcCommands(netem, "tc ")) != 0 {
				t.Fatalf("got %v, %v with %d helpers", lease, err, netem.liveHelpers())
			}
		})
	}
	// A network that is not a bridge is refused before any helper starts.
	netem := newFakeNetem()
	netem.driver = "macvlan"
	if lease, err := temporaryLinkDelay(t.Context(), netem, osd, "tc-public", link); !errors.Is(err, ErrInvalidRequest) || lease != nil || len(netem.helpers) != 0 {
		t.Fatalf("a macvlan network: %v, %v", lease, err)
	}
	netem = newFakeNetem()
	lease, err := temporaryLinkDelay(t.Context(), netem, osd, "tc-public", link)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || lease.NetworkName != "tc-public" {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if link := netem.eth0("osd-0"); link.root != nil || netem.liveHelpers() != 0 {
		t.Fatalf("after Restore: root %+v, %d helpers", link.root, netem.liveHelpers())
	}
}
