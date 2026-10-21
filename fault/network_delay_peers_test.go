package fault

import (
	"encoding/json"
	"errors"
	"net/netip"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// manyPeers selects two IPv4 peers, one of them on one port, and one IPv6
// peer on one port.
var manyPeers = []NetworkPeer{
	{Addr: netip.MustParseAddr("10.88.0.7"), Port: 6800},
	{Addr: netip.MustParseAddr("fd62:93fe:2c9e::3"), Port: 6801},
	{Addr: netip.MustParseAddr("10.88.0.8")},
}

// peersCluster is delayCluster with an IPv6 address on osd-0's endpoint.
func peersCluster(netem *fakeNetem) (*delayTestCluster, *fakeNetemHelper) {
	netem.targets["osd-0"].address6 = netip.MustParseAddr("fd62:93fe:2c9e::2")
	return delayCluster(netem)
}

func tcCommands(netem *fakeNetem, prefix string) []string {
	netem.mu.Lock()
	defer netem.mu.Unlock()
	var commands []string
	for _, command := range netem.commands {
		if strings.HasPrefix(command, prefix) {
			commands = append(commands, command)
		}
	}
	return commands
}

// One lease holds a filter for each peer: the IPv4 peers in one preference
// and table, the IPv6 peer in the next, all into the one netem child.
func TestNetworkDelayPeersApplyOneFilterEach(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := peersCluster(netem)
	delay := testDelay
	delay.Peers = manyPeers
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	if err != nil {
		t.Fatal(err)
	}
	filters := tcCommands(netem, "tc filter")
	flow := " flowid " + lease.handle + "3"
	want := []string{
		"tc filter add dev eth0 parent " + lease.handle + " protocol ip pref 10 handle 800::1 u32 match ip dst 10.88.0.7/32 match ip protocol 6 0xff match ip dport 6800 0xffff" + flow,
		"tc filter add dev eth0 parent " + lease.handle + " protocol ip pref 10 handle 800::2 u32 match ip dst 10.88.0.8/32" + flow,
		"tc filter add dev eth0 parent " + lease.handle + " protocol ipv6 pref 11 handle 801::1 u32 match ip6 dst fd62:93fe:2c9e::3/128 match ip6 protocol 6 0xff match ip6 dport 6801 0xffff" + flow,
	}
	if !slices.Equal(filters, want) {
		t.Fatalf("filters\n%q\nwant\n%q", filters, want)
	}
	// The root and the child come first: nothing is selected until then.
	if tc := tcCommands(netem, "tc "); len(tc) != 5 || !strings.Contains(tc[0], " prio bands 3 ") || !strings.Contains(tc[1], " netem ") {
		t.Fatalf("tc commands %q", tc)
	}
	if !slices.Equal(lease.Peers, manyPeers) || lease.Peer.IsValid() {
		t.Fatalf("lease selection %v, Peer %v", lease.Peers, lease.Peer)
	}
	status, err := lease.Status(t.Context())
	if err != nil || !status.Applied || status.RootKind != "prio" || status.Delay != delay.Delay {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if link := netem.eth0("osd-0"); link.root != nil || link.child != nil || len(link.filters) != 0 || netem.liveHelpers() != 0 {
		t.Fatalf("after Restore: root %+v child %+v filters %v, %d helpers", link.root, link.child, link.filters, netem.liveHelpers())
	}
}

// An apply that stops at any filter returns its lease: the hierarchy so far
// is the lease's, is not reported applied, and Restore removes it.
func TestNetworkDelayPeersPartialApplyIsRestored(t *testing.T) {
	for _, refused := range []int{1, 2, 3} {
		t.Run(strings.Repeat("filter ", refused), func(t *testing.T) {
			netem := newFakeNetem()
			seen := 0
			netem.onSelective = func(argv []string) (supervised.Report, bool) {
				if argv[1] == "filter" {
					if seen++; seen == refused {
						return supervised.Report{ExitCode: 2, Stderr: []byte("Error: refused by the test.")}, true
					}
				}
				return supervised.Report{}, false
			}
			cluster, osd := peersCluster(netem)
			delay := testDelay
			delay.Peers = manyPeers
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
			if err == nil || lease == nil {
				t.Fatalf("a partial apply returned %v, %v", lease, err)
			}
			status, err := lease.Status(t.Context())
			if err != nil || status.Applied || status.RootHandle != lease.handle {
				t.Fatalf("status %+v %v", status, err)
			}
			if _, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("a second delay while the partial one is held: %v", err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if link := netem.eth0("osd-0"); link.root != nil || link.child != nil || len(link.filters) != 0 || netem.liveHelpers() != 0 {
				t.Fatalf("after Restore: %+v %+v %v, %d helpers", link.root, link.child, link.filters, netem.liveHelpers())
			}
		})
	}
}

// A kernel that numbers the second preference's u32 table otherwise
// refuses the filter that names 801:; the lease stays, is not applied, and
// is restored.
func TestNetworkDelayPeersRefuseAnotherTableNumber(t *testing.T) {
	netem := newFakeNetem()
	netem.tableGap = 1
	cluster, osd := peersCluster(netem)
	delay := testDelay
	delay.Peers = manyPeers
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	var failure *ceph.NativeFailure
	if !errors.As(err, &failure) || lease == nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if status, err := lease.Status(t.Context()); err != nil || status.Applied {
		t.Fatalf("status %+v %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// The peers preflight runs in the preflight helper: a kernel without prio or
// u32 is refused before anything runs on the target, while a delay of the
// whole interface does not need either.
func TestNetworkDelayPeersPreflightTheClassifier(t *testing.T) {
	for name, setup := range map[string]func(*fakeNetem){
		"no prio": func(n *fakeNetem) { n.noPrio = true },
		"no u32":  func(n *fakeNetem) { n.noU32 = true },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			setup(netem)
			cluster, osd := peersCluster(netem)
			delay := testDelay
			delay.Peers = manyPeers
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
			if !errors.Is(err, ErrNetemKernelUnsupported) || !errors.Is(err, ceph.ErrEnvironmentUnavailable) || errors.Is(err, ceph.ErrImageCapability) || lease != nil {
				t.Fatalf("got %v, %v", lease, err)
			}
			if tc := tcCommands(netem, "tc "); len(tc) != 0 || netem.liveHelpers() != 0 || len(cluster.faultsOf(faulthook.NetworkDelay)) != 0 {
				t.Fatalf("the refusal ran %q, left %d helpers and %v", tc, netem.liveHelpers(), cluster.faultsOf(faulthook.NetworkDelay))
			}
			whole, err := cluster.ApplyNetworkProfile(t.Context(), osd, testDelay)
			if err != nil {
				t.Fatalf("a whole-interface delay: %v", err)
			}
			if err := whole.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
	for name, edit := range map[string]func(map[string]any){
		"no prio result":  func(out map[string]any) { delete(out, "prio") },
		"no u32 result":   func(out map[string]any) { delete(out, "u32") },
		"u32 not shown":   func(out map[string]any) { out["u32_shown"] = `[]` },
		"no prio removal": func(out map[string]any) { delete(out, "prio_delete") },
		"an unknown u32 kind": func(out map[string]any) {
			out["u32"] = []any{1, `Unknown filter "u32", hence option "match" is unparsable`}
		},
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			netem.editPreflight = edit
			cluster, osd := peersCluster(netem)
			delay := testDelay
			delay.Peers = manyPeers
			if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay); !errors.Is(err, ErrNetemImageUnsupported) || lease != nil {
				t.Fatalf("got %v, %v", lease, err)
			}
			if tc := tcCommands(netem, "tc "); len(tc) != 0 {
				t.Fatalf("ran %q on the target", tc)
			}
		})
	}
}

func TestNetworkDelayPeersValidation(t *testing.T) {
	v4 := netip.MustParseAddr("10.88.0.7")
	tooMany := make([]NetworkPeer, MaxNetworkPeers+1)
	for i := range tooMany {
		tooMany[i] = NetworkPeer{Addr: v4, Port: uint16(6800 + i)}
	}
	for name, edit := range map[string]func(*NetworkProfile){
		"Peer and Peers":  func(d *NetworkProfile) { d.Peer = v4 },
		"empty Peers":     func(d *NetworkProfile) { d.Peers = []NetworkPeer{} },
		"too many peers":  func(d *NetworkProfile) { d.Peers = tooMany },
		"duplicate peer":  func(d *NetworkProfile) { d.Peers = []NetworkPeer{{Addr: v4}, {Addr: v4}} },
		"invalid peer":    func(d *NetworkProfile) { d.Peers = []NetworkPeer{{Addr: v4}, {Port: 6800}} },
		"mapped peer":     func(d *NetworkProfile) { d.Peers = []NetworkPeer{{Addr: netip.MustParseAddr("::ffff:10.88.0.7")}} },
		"zoned peer":      func(d *NetworkProfile) { d.Peers = []NetworkPeer{{Addr: netip.MustParseAddr("fe80::1%eth0")}} },
		"unspecified":     func(d *NetworkProfile) { d.Peers = []NetworkPeer{{Addr: netip.IPv6Unspecified()}} },
		"port and Peers":  func(d *NetworkProfile) { d.PeerPort = 6800 },
		"IPv6 on IPv4 if": nil,
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			delay := testDelay
			delay.Peers = manyPeers
			if edit != nil {
				edit(&delay)
			}
			// Without edit, the IPv6 peer of an endpoint without IPv6.
			if lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay); !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
				t.Fatalf("%+v: %v, %v", delay.Peers, lease, err)
			}
			if len(netem.helpers) != 0 || len(tcCommands(netem, "tc ")) != 0 {
				t.Fatal("a refused request started a helper or ran tc")
			}
		})
	}
	// The same address on two ports is two peers.
	if err := validateNetworkDelay(NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Loss: 100,
		Peers: []NetworkPeer{{Addr: v4, Port: 6800}, {Addr: v4, Port: 6801}}}); err != nil {
		t.Fatal(err)
	}
}

// nativeIPv4PeerFilters is nativeIPv4PeerFilter with a second key node in
// the same table, for 172.19.0.4 on any port.
var nativeIPv4PeerFilters = strings.TrimSuffix(nativeIPv4PeerFilter, "]") +
	`,{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0,"options":{"fh":"800::2","order":2,"key_ht":"800","bkt":"0","flowid":"7e00:3","not_in_hw":true,"matches":[{"value":"ac130004","mask":"ffffffff","offmask":"","off":16}]}}]`

func TestCheckPeerFiltersOfSeveralPeers(t *testing.T) {
	first := NetworkPeer{Addr: netip.MustParseAddr("172.19.0.3"), Port: 7000}
	second := NetworkPeer{Addr: netip.MustParseAddr("172.19.0.4")}
	peers := []NetworkPeer{first, second}
	if err := checkPeerFilter(json.RawMessage(nativeIPv4PeerFilters), "7e00:", peers); err != nil {
		t.Fatal(err)
	}
	// Only the first filter, as after an apply that stopped before the
	// second: owned, but not applied.
	if err := checkPeerFilters(json.RawMessage(nativeIPv4PeerFilter), "7e00:", peers, false); err != nil {
		t.Fatalf("a prefix: %v", err)
	}
	if err := checkPeerFilter(json.RawMessage(nativeIPv4PeerFilter), "7e00:", peers); err == nil {
		t.Fatal("a missing filter was accepted")
	}
	for name, dump := range map[string]string{
		"swapped peers":        strings.Replace(strings.Replace(nativeIPv4PeerFilters, `"fh":"800::2"`, `"fh":"800::X"`, 1), `"fh":"800::1"`, `"fh":"800::2"`, 1),
		"another table":        strings.Replace(nativeIPv4PeerFilters, `"fh":"800:","ht_divisor":1`, `"fh":"802:","ht_divisor":1`, 1),
		"a third key node":     strings.Replace(nativeIPv4PeerFilters, `"fh":"800::2"`, `"fh":"800::3"`, 1),
		"the same node twice":  strings.Replace(nativeIPv4PeerFilters, `"fh":"800::2","order":2`, `"fh":"800::1","order":2`, 1),
		"second to band 1":     strings.Replace(nativeIPv4PeerFilters, `"order":2,"key_ht":"800","bkt":"0","flowid":"7e00:3"`, `"order":2,"key_ht":"800","bkt":"0","flowid":"7e00:1"`, 1),
		"second in pref 11":    strings.Replace(nativeIPv4PeerFilters, `"pref":10,"kind":"u32","chain":0,"options":{"fh":"800::2"`, `"pref":11,"kind":"u32","chain":0,"options":{"fh":"800::2"`, 1),
		"second with a port":   strings.Replace(nativeIPv4PeerFilters, `"value":"ac130004","mask":"ffffffff","offmask":"","off":16}`, `"value":"ac130004","mask":"ffffffff","offmask":"","off":16},{"value":"1b58","mask":"ffff","offmask":"","off":20}`, 1),
		"second to another IP": strings.Replace(nativeIPv4PeerFilters, `"ac130004"`, `"ac130005"`, 1),
	} {
		if err := checkPeerFilter(json.RawMessage(dump), "7e00:", peers); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// The IPv6 filter of a mixed selection is in preference 11 and table
	// 801:, not in the single-family 10 and 800:.
	v6 := NetworkPeer{Addr: netip.MustParseAddr("fd62:93fe:2c9e::3"), Port: 7000}
	if err := checkPeerFilter(json.RawMessage(nativeIPv6PeerFilter), "7e00:", []NetworkPeer{first, v6}); err == nil {
		t.Fatal("an IPv6 filter in preference 10 was accepted for a mixed selection")
	}
	mixed := strings.TrimSuffix(nativeIPv4PeerFilter, "]") + "," + strings.TrimPrefix(
		strings.NewReplacer(`"pref":10`, `"pref":11`, `"fh":"800::1"`, `"fh":"801::1"`, `"key_ht":"800"`, `"key_ht":"801"`).Replace(nativeIPv6PeerFilter), "[")
	if err := checkPeerFilter(json.RawMessage(mixed), "7e00:", []NetworkPeer{first, v6}); err != nil {
		t.Fatalf("mixed: %v", err)
	}
}
