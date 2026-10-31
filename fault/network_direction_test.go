package fault

import (
	"context"
	"errors"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
)

// directionCluster is delayCluster with a second container, osd-1, on the
// same bridge: the peer whose endpoint an Inbound delay shapes.
func directionCluster(netem *fakeNetem) (*delayTestCluster, *fakeNetemHelper) {
	netem.targets["osd-1"] = &fakeNetemTarget{running: true, mode: "tc-public", sandbox: 1, namespace: netem.namespace("10.88.0.6", "02:42:0a:58:00:06")}
	return delayCluster(netem)
}

var (
	directionTarget = netip.MustParseAddr("10.88.0.5")
	directionPeer   = netip.MustParseAddr("10.88.0.6")
)

// The source-port and SYN keys are the words that tc printed for
// "match ip sport 7000 0xffff" and "match u8 0x02 0x02 at 33" (at 53 over
// IPv6) in the netem helper's selector profiles.
func TestPeerFilterKeysSourcePortAndSetup(t *testing.T) {
	v4, v6 := netip.MustParseAddr("10.0.0.2"), netip.MustParseAddr("fd00::2")
	tests := []struct {
		name  string
		peer  netip.Addr
		port  uint16
		match peerMatch
		tail  []u32Key
	}{
		{"ipv4 source port", v4, 7000, peerMatch{source: true}, []u32Key{{8, 6 << 16, 0xff << 16}, {20, 7000 << 16, 0xffff0000}}},
		{"ipv6 source port", v6, 7000, peerMatch{source: true}, []u32Key{{4, 6 << 8, 0xff << 8}, {40, 7000 << 16, 0xffff0000}}},
		{"ipv4 setup", v4, 7000, peerMatch{setup: true}, []u32Key{{8, 6 << 16, 0xff << 16}, {20, 7000, 0xffff}, {32, 0x00020000, 0x00020000}}},
		{"ipv6 setup", v6, 7000, peerMatch{setup: true}, []u32Key{{4, 6 << 8, 0xff << 8}, {40, 7000, 0xffff}, {52, 0x00020000, 0x00020000}}},
		{"ipv4 setup without a port", v4, 0, peerMatch{setup: true}, []u32Key{{8, 6 << 16, 0xff << 16}, {32, 0x00020000, 0x00020000}}},
		{"ipv4 source setup", v4, 6800, peerMatch{source: true, setup: true}, []u32Key{{8, 6 << 16, 0xff << 16}, {20, 6800 << 16, 0xffff0000}, {32, 0x00020000, 0x00020000}}},
		{"ipv4 source without a port", v4, 0, peerMatch{source: true}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			keys := peerFilterKeys(test.peer, test.port, test.match)
			address := 1
			if test.peer.Is6() {
				address = 4
			}
			if got := keys[address:]; !slices.Equal(got, test.tail) {
				t.Fatalf("keys after the address %v, want %v", got, test.tail)
			}
			// The fake encodes the tc arguments as tc prints them.
			argv := peerFilterArgs("eth0", "7e00:", peerLeaf{peer: NetworkPeer{Addr: test.peer, Port: test.port}}, test.match)
			var printed []u32Key
			for _, key := range fakeU32Keys(argv) {
				printed = append(printed, decodeFakeKey(t, key))
			}
			if !slices.Equal(printed, keys) {
				t.Fatalf("tc arguments %q print %v, want %v", argv, printed, keys)
			}
		})
	}
}

func decodeFakeKey(t *testing.T, key map[string]any) u32Key {
	t.Helper()
	value, err1 := strconv.ParseUint(key["value"].(string), 16, 32)
	mask, err2 := strconv.ParseUint(key["mask"].(string), 16, 32)
	if err1 != nil || err2 != nil {
		t.Fatalf("key %v", key)
	}
	return u32Key{Off: key["off"].(int), Value: uint32(value), Mask: uint32(mask)}
}

func TestNetworkDirectionAndPhaseValidation(t *testing.T) {
	peer := directionPeer
	for name, delay := range map[string]NetworkProfile{
		"inbound without peers":  {Direction: Inbound},
		"both without peers":     {Direction: BothDirections},
		"setup without peers":    {Phase: ConnectionSetup},
		"unknown direction":      {Peer: peer, Direction: BothDirections + 1},
		"negative direction":     {Peer: peer, Direction: -1},
		"unknown phase":          {Peer: peer, Phase: ConnectionSetup + 1},
		"unknown phase no peers": {Phase: -1},
	} {
		delay.HelperImage, delay.Plane, delay.Delay = "h", ceph.PublicNetworkPlane, 10*time.Millisecond
		if err := validateNetworkDelay(delay); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
	for name, delay := range map[string]NetworkProfile{
		"inbound with a peer":    {Peer: peer, Direction: Inbound},
		"both with peers":        {Peers: []NetworkPeer{{Addr: peer, Port: 6800}}, Direction: BothDirections},
		"setup with a peer port": {Peer: peer, PeerPort: 6800, Phase: ConnectionSetup},
		"inbound setup":          {Peer: peer, Direction: Inbound, Phase: ConnectionSetup},
	} {
		delay.HelperImage, delay.Plane, delay.Delay = "h", ceph.PublicNetworkPlane, 10*time.Millisecond
		if err := validateNetworkDelay(delay); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// An Inbound delay applies nothing to the target: one lease on the peer's
// endpoint selects what the peer sends to the target's address from its own
// port, and the outer lease restores it.
func TestInboundNetworkDelayShapesThePeersEndpoint(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := directionCluster(netem)
	delay := testDelay
	delay.Peer, delay.PeerPort, delay.Direction = directionPeer, 6800, Inbound
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	if err != nil {
		t.Fatal(err)
	}
	if netem.eth0("osd-0").root != nil {
		t.Fatalf("the target's endpoint was shaped: %+v", netem.eth0("osd-0").root)
	}
	if len(lease.PeerLeases) != 1 || lease.Direction != Inbound {
		t.Fatalf("lease %+v", lease)
	}
	peer := lease.PeerLeases[0]
	if peer.ContainerID != "osd-1" || !peer.SourcePorts || peer.Direction != Outbound ||
		!slices.Equal(peer.Peers, []NetworkPeer{{Addr: directionTarget, Port: 6800}}) {
		t.Fatalf("peer lease %+v", peer)
	}
	want := "tc filter add dev eth0 parent " + peer.handle + " protocol ip pref 10 handle 800::1 u32 match ip dst 10.88.0.5/32 match ip protocol 6 0xff match ip sport 6800 0xffff flowid " + peer.handle + "3"
	if filters := tcCommands(netem, "tc filter"); !slices.Equal(filters, []string{want}) {
		t.Fatalf("filters %q, want %q", filters, want)
	}
	if _, err := lease.Status(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("the Inbound lease's own Status: %v", err)
	}
	status, err := peer.Status(t.Context())
	if err != nil || !status.Applied || status.RootKind != "prio" {
		t.Fatalf("peer status %+v, %v", status, err)
	}
	if held := cluster.faultsOf(faulthook.NetworkDelay); len(held) != 1 {
		t.Fatalf("held delays %v, want the peer's endpoint only", held)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if link := netem.eth0("osd-1"); link.root != nil || len(link.filters) != 0 || netem.liveHelpers() != 0 {
		t.Fatalf("after Restore: root %+v filters %v, %d helpers", link.root, link.filters, netem.liveHelpers())
	}
	if held := cluster.faultsOf(faulthook.NetworkDelay); len(held) != 0 {
		t.Fatalf("held delays after Restore: %v", held)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatalf("a second Restore: %v", err)
	}
}

// BothDirections shapes the target's endpoint toward the peer's port and the
// peer's endpoint from that port, with the SYN key on both for
// ConnectionSetup; Terminate restores both.
func TestBothDirectionsConnectionSetupDelay(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := directionCluster(netem)
	delay := testDelay
	delay.Peer, delay.PeerPort, delay.Direction, delay.Phase, delay.Loss = directionPeer, 6800, BothDirections, ConnectionSetup, 100
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	if err != nil {
		t.Fatal(err)
	}
	if len(lease.PeerLeases) != 1 || lease.Phase != ConnectionSetup || lease.PeerLeases[0].Phase != ConnectionSetup {
		t.Fatalf("lease %+v", lease)
	}
	filters := tcCommands(netem, "tc filter")
	if len(filters) != 2 ||
		!strings.Contains(filters[0], "match ip dst 10.88.0.6/32 match ip protocol 6 0xff match ip dport 6800 0xffff match u8 0x02 0x02 at 33 ") ||
		!strings.Contains(filters[1], "match ip dst 10.88.0.5/32 match ip protocol 6 0xff match ip sport 6800 0xffff match u8 0x02 0x02 at 33 ") {
		t.Fatalf("filters %q", filters)
	}
	for _, l := range []*NetworkProfileLease{lease, lease.PeerLeases[0]} {
		if status, err := l.Status(t.Context()); err != nil || !status.Applied || status.Loss != 100 {
			t.Fatalf("status of %s: %+v, %v", l.ContainerID, status, err)
		}
	}
	if !strings.Contains(strings.Join(tcCommands(netem, "python3"), "\n"), "peers setup") {
		t.Fatalf("the preflight did not check the SYN key: %q", tcCommands(netem, "python3"))
	}
	if held := cluster.faultsOf(faulthook.NetworkDelay); len(held) != 2 {
		t.Fatalf("held delays %v", held)
	}
	if err := cluster.Terminate(t.Context()); err != nil && !errors.Is(err, nativeTerminated()) {
		t.Fatal(err)
	}
	for _, id := range []string{"osd-0", "osd-1"} {
		if link := netem.eth0(id); link.root != nil || len(link.filters) != 0 {
			t.Fatalf("%s after Terminate: root %+v filters %v", id, link.root, link.filters)
		}
	}
}

// A peer that is not a container on the bridge, or the target itself, is
// refused before anything starts.
func TestInboundNetworkDelayRefusesUnresolvedPeers(t *testing.T) {
	for name, peer := range map[string]netip.Addr{
		"unknown address": netip.MustParseAddr("10.88.0.99"),
		"target itself":   directionTarget,
		"ipv6 without":    netip.MustParseAddr("fd62:93fe:2c9e::9"),
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := directionCluster(netem)
			if name == "ipv6 without" {
				netem.targets["osd-1"].address6 = peer
			}
			delay := testDelay
			delay.Peer, delay.Direction = peer, BothDirections
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
			if lease != nil || !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("lease %v, error %v", lease, err)
			}
			if len(netem.helpers) != 0 || netem.eth0("osd-0").root != nil || netem.eth0("osd-1").root != nil {
				t.Fatalf("something started or changed: %d helpers", len(netem.helpers))
			}
		})
	}
}

// Two peer addresses of one container share one lease on its endpoint, one
// filter per family.
func TestInboundNetworkDelayGroupsAContainersAddresses(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := directionCluster(netem)
	netem.targets["osd-0"].address6 = netip.MustParseAddr("fd62:93fe:2c9e::2")
	peer6 := netip.MustParseAddr("fd62:93fe:2c9e::6")
	netem.targets["osd-1"].address6 = peer6
	delay := testDelay
	delay.Peers, delay.Direction = []NetworkPeer{{Addr: directionPeer, Port: 6800}, {Addr: peer6, Port: 6800}}, Inbound
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	if err != nil {
		t.Fatal(err)
	}
	if len(lease.PeerLeases) != 1 || !slices.Equal(lease.PeerLeases[0].Peers,
		[]NetworkPeer{{Addr: directionTarget, Port: 6800}, {Addr: netip.MustParseAddr("fd62:93fe:2c9e::2"), Port: 6800}}) {
		t.Fatalf("lease %+v", lease.PeerLeases)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A kernel that does not keep the SYN key as the lease reads it back is
// refused before the target changes.
func TestConnectionSetupPreflightRefusesChangedKeys(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := directionCluster(netem)
	netem.editPreflight = func(out map[string]any) {
		if shown, ok := out["u32_shown"].(string); ok {
			out["u32_shown"] = strings.Replace(shown, `"20000"`, `"0"`, 2)
		}
	}
	delay := testDelay
	delay.Peer, delay.Phase = directionPeer, ConnectionSetup
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	if lease != nil || !errors.Is(err, ErrNetemKernelUnsupported) {
		t.Fatalf("lease %v, error %v", lease, err)
	}
	if netem.eth0("osd-0").root != nil || netem.liveHelpers() != 0 {
		t.Fatal("the target changed")
	}
}

// With BothDirections the outer lease is held for Terminate as soon as the
// target's endpoint is shaped, before the peer leases are published. A
// Terminate that restores it while a peer lease is being published must not
// race with that publication, and the peer lease published after it is still
// restored by the outer lease's next Restore.
func TestBothDirectionsPeerPublicationRacesWithRestore(t *testing.T) {
	netem := newFakeNetem()
	directionCluster(netem)
	delay := testDelay
	delay.Peer, delay.Direction, delay.Loss = directionPeer, BothDirections, 100
	var outer *NetworkProfileLease
	restored := make(chan error, 1)
	hold := func(id string, apply func() (*NetworkProfileLease, error)) (*NetworkProfileLease, error) {
		lease, err := apply()
		if outer == nil {
			outer = lease
			return lease, err
		}
		// Terminate's undo of the outer lease, while this peer lease is
		// being published.
		go func() { restored <- outer.Restore(context.Background()) }()
		return lease, err
	}
	top, err := applyDirectedDelay(t.Context(), netem, "osd-0", "tc-public", delay, hold)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-restored; err != nil {
		t.Fatal(err)
	}
	if top != outer || len(top.peerLeases()) != 1 {
		t.Fatalf("outer lease %p, returned %p, peers %d", outer, top, len(top.peerLeases()))
	}
	if err := top.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"osd-0", "osd-1"} {
		if link := netem.eth0(id); link.root != nil || len(link.filters) != 0 {
			t.Fatalf("%s after Restore: root %+v filters %v", id, link.root, link.filters)
		}
	}
	if netem.liveHelpers() != 0 {
		t.Fatalf("%d helpers left", netem.liveHelpers())
	}
}
