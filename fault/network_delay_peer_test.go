package fault

import (
	"encoding/json"
	"errors"
	"math"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

var peerDelay = func() NetworkProfile {
	delay := testDelay
	delay.Peer, delay.PeerPort = netip.MustParseAddr("10.88.0.7"), 6800
	return delay
}()

// link returns osd-0's endpoint in the model.
func (n *fakeNetem) eth0(id string) *fakeLink {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.targets[id].namespace.links[1]
}

func TestPeerNetworkDelayAppliesTheHierarchyAndRestores(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, peerDelay)
	if err != nil {
		t.Fatal(err)
	}
	link := netem.eth0("osd-0")
	if link.root == nil || link.root.kind != "prio" || link.root.handle != lease.handle ||
		link.child == nil || link.child.handle != lease.child || link.parent != lease.handle+"3" ||
		link.child.delay != peerDelay.Delay || link.child.jitter != peerDelay.Jitter || len(link.filters) != 3 {
		t.Fatalf("hierarchy root %+v child %+v under %s, %d filter entries", link.root, link.child, link.parent, len(link.filters))
	}
	// The filter is added last, after the root and the child: until then
	// nothing selects a packet.
	var tc []string
	for _, command := range netem.commands {
		if strings.HasPrefix(command, "tc ") {
			tc = append(tc, command)
		}
	}
	if len(tc) != 3 || !strings.Contains(tc[0], " root handle "+lease.handle+" prio bands 3 priomap 0 0 ") ||
		!strings.Contains(tc[1], " parent "+lease.handle+"3 handle "+lease.child+" netem delay 200000us 20000us") ||
		tc[2] != "tc filter add dev eth0 parent "+lease.handle+" protocol ip pref 10 handle 800::1 u32 match ip dst 10.88.0.7/32 match ip protocol 6 0xff match ip dport 6800 0xffff flowid "+lease.handle+"3" {
		t.Fatalf("tc commands %q", tc)
	}
	status, err := lease.Status(t.Context())
	if err != nil || !status.Applied || status.Delay != peerDelay.Delay || status.Jitter != peerDelay.Jitter || status.Packets != 6 || len(status.Filters) == 0 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if lease.Peer != peerDelay.Peer || lease.PeerPort != 6800 {
		t.Fatalf("lease selection %v:%d", lease.Peer, lease.PeerPort)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if link.root != nil || link.child != nil || len(link.filters) != 0 || netem.liveHelpers() != 0 {
		t.Fatalf("after Restore: root %+v child %+v filters %v, %d helpers", link.root, link.child, link.filters, netem.liveHelpers())
	}
}

func TestPeerNetworkDelayIPv6NeedsAnIPv6Endpoint(t *testing.T) {
	delay := testDelay
	delay.Peer = netip.MustParseAddr("fd62:93fe:2c9e::3")
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	if _, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("IPv6 peer of an IPv4-only endpoint: %v", err)
	}
	if netem.liveHelpers() != 0 || netem.eth0("osd-0").root != nil {
		t.Fatal("a refused delay changed something")
	}
	netem.targets["osd-0"].address6 = netip.MustParseAddr("fd62:93fe:2c9e::2")
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
	if err != nil {
		t.Fatal(err)
	}
	last := netem.commands[len(netem.commands)-1]
	for _, command := range netem.commands {
		if strings.HasPrefix(command, "tc filter") {
			last = command
		}
	}
	if !strings.Contains(last, " protocol ipv6 pref 10 handle 800::1 u32 match ip6 dst fd62:93fe:2c9e::3/128 flowid ") {
		t.Fatalf("IPv6 filter %q", last)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied {
		t.Fatalf("status %+v %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPeerNetworkDelayPartialApplyIsRestored(t *testing.T) {
	for _, step := range []string{"parent", "filter"} {
		t.Run(step, func(t *testing.T) {
			netem := newFakeNetem()
			netem.onSelective = func(argv []string) (supervised.Report, bool) {
				if argv[1] == step || argv[5] == step {
					return supervised.Report{ExitCode: 2, Stderr: []byte("Error: refused by the test.")}, true
				}
				return supervised.Report{}, false
			}
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, peerDelay)
			if err == nil || lease == nil {
				t.Fatalf("a partial apply returned %v, %v", lease, err)
			}
			// The root is the lease's, so the incomplete hierarchy is not
			// applied but is restored.
			if status, err := lease.Status(t.Context()); err != nil || status.Applied || status.RootHandle != lease.handle {
				t.Fatalf("status %+v %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if link := netem.eth0("osd-0"); link.root != nil || link.child != nil || len(link.filters) != 0 {
				t.Fatalf("after Restore: %+v %+v %v", link.root, link.child, link.filters)
			}
		})
	}
}

func TestPeerNetworkDelayValidation(t *testing.T) {
	for name, edit := range map[string]func(*NetworkProfile){
		"port without peer": func(d *NetworkProfile) { d.Peer = netip.Addr{} },
		"mapped peer":       func(d *NetworkProfile) { d.Peer = netip.MustParseAddr("::ffff:10.88.0.7") },
		"zoned peer":        func(d *NetworkProfile) { d.Peer = netip.MustParseAddr("fe80::1%eth0") },
		"unspecified peer":  func(d *NetworkProfile) { d.Peer = netip.IPv4Unspecified() },
	} {
		delay := peerDelay
		edit(&delay)
		if err := validateNetworkDelay(delay); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// Filter dumps that tc 6.1 printed for these keys, with
// the repeated "match" members collected under "matches" as netemScript does.
const (
	nativeIPv4PeerFilter = `[{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0},{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0,"options":{"fh":"800:","ht_divisor":1}},{"parent":"7e00:","protocol":"ip","pref":10,"kind":"u32","chain":0,"options":{"fh":"800::1","order":1,"key_ht":"800","bkt":"0","flowid":"7e00:3","not_in_hw":true,"matches":[{"value":"ac130003","mask":"ffffffff","offmask":"","off":16},{"value":"60000","mask":"ff0000","offmask":"","off":8},{"value":"1b58","mask":"ffff","offmask":"","off":20}]}}]`
	nativeIPv6PeerFilter = `[{"parent":"7e00:","protocol":"ipv6","pref":10,"kind":"u32","chain":0,"options":{"fh":"800::1","order":1,"key_ht":"800","bkt":"0","flowid":"7e00:3","not_in_hw":true,"matches":[{"value":"fd6293fe","mask":"ffffffff","offmask":"","off":24},{"value":"2c9e0000","mask":"ffffffff","offmask":"","off":28},{"value":"0","mask":"ffffffff","offmask":"","off":32},{"value":"3","mask":"ffffffff","offmask":"","off":36},{"value":"600","mask":"ff00","offmask":"","off":4},{"value":"1b58","mask":"ffff","offmask":"","off":40}]}}]`
)

func TestCheckPeerFilterAgainstNativeDumps(t *testing.T) {
	v4, v6 := netip.MustParseAddr("172.19.0.3"), netip.MustParseAddr("fd62:93fe:2c9e::3")
	if err := checkPeerFilter(json.RawMessage(nativeIPv4PeerFilter), "7e00:", []NetworkPeer{{v4, 7000}}, peerMatch{}); err != nil {
		t.Fatalf("IPv4: %v", err)
	}
	if err := checkPeerFilter(json.RawMessage(nativeIPv6PeerFilter), "7e00:", []NetworkPeer{{v6, 7000}}, peerMatch{}); err != nil {
		t.Fatalf("IPv6: %v", err)
	}
	for name, test := range map[string]struct {
		dump string
		root string
		peer netip.Addr
		port uint16
	}{
		"another peer":           {nativeIPv4PeerFilter, "7e00:", netip.MustParseAddr("172.19.0.4"), 7000},
		"another port":           {nativeIPv4PeerFilter, "7e00:", v4, 7001},
		"no port selected":       {nativeIPv4PeerFilter, "7e00:", v4, 0},
		"another root":           {nativeIPv4PeerFilter, "7e01:", v4, 7000},
		"another family":         {nativeIPv4PeerFilter, "7e00:", v6, 7000},
		"a wider protocol mask":  {strings.Replace(nativeIPv4PeerFilter, `"mask":"ff0000"`, `"mask":"ffff0000"`, 1), "7e00:", v4, 7000},
		"an offmask":             {strings.Replace(nativeIPv4PeerFilter, `"offmask":"","off":20`, `"offmask":"ff","off":20`, 1), "7e00:", v4, 7000},
		"another flowid":         {strings.Replace(nativeIPv4PeerFilter, `"flowid":"7e00:3"`, `"flowid":"7e00:1"`, 1), "7e00:", v4, 7000},
		"a missing address word": {strings.Replace(nativeIPv6PeerFilter, `{"value":"0","mask":"ffffffff","offmask":"","off":32},`, ``, 1), "7e00:", v6, 7000},
		"two leaves":             {strings.TrimSuffix(nativeIPv4PeerFilter, "]") + "," + strings.TrimPrefix(nativeIPv4PeerFilter, "[{\"parent\":\"7e00:\",\"protocol\":\"ip\",\"pref\":10,\"kind\":\"u32\",\"chain\":0},{\"parent\":\"7e00:\",\"protocol\":\"ip\",\"pref\":10,\"kind\":\"u32\",\"chain\":0,\"options\":{\"fh\":\"800:\",\"ht_divisor\":1}},"), "7e00:", v4, 7000},
		"a foreign filter":       {strings.Replace(nativeIPv4PeerFilter, `[{"parent":"7e00:","protocol":"ip","pref":10,`, `[{"parent":"7e00:","protocol":"ip","pref":20,`, 1), "7e00:", v4, 7000},
		"no filter":              {`[]`, "7e00:", v4, 7000},
	} {
		if err := checkPeerFilter(json.RawMessage(test.dump), test.root, []NetworkPeer{{test.peer, test.port}}, peerMatch{}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A filter or qdisc that someone else attached under the lease's root is not
// the lease's: Restore refuses rather than remove it with the root, keeps the
// lease, and succeeds once the owner removed it.
func TestPeerNetworkDelayRestoreKeepsForeignState(t *testing.T) {
	for name, tc := range map[string]struct {
		delay  NetworkProfile
		attach func(link *fakeLink, root string) func()
	}{
		"filter": {peerDelay, func(link *fakeLink, root string) func() {
			foreign := map[string]any{"parent": root, "protocol": "ip", "pref": 20, "kind": "u32", "chain": 0,
				"options": map[string]any{"fh": "801::1", "flowid": root + "1", "matches": fakeU32Keys([]string{"match", "ip", "dst", "192.0.2.1/32"})}}
			link.filters = append(link.filters, foreign)
			return func() { link.filters = link.filters[:len(link.filters)-1] }
		}},
		"replaced child": {peerDelay, func(link *fakeLink, root string) func() {
			saved := link.child
			link.child, link.parent = &fakeQdisc{kind: "tbf", handle: "9999:"}, root+"1"
			return func() { link.child, link.parent = saved, root+"3" }
		}},
		// The delay's own child and filter stay intact: the hierarchy is
		// complete, and still not all the lease's.
		"added qdisc": {peerDelay, func(link *fakeLink, root string) func() {
			link.extra = append(link.extra, fakeExtraQdisc{kind: "tbf", handle: "9999:", parent: root + "1"})
			return func() { link.extra = nil }
		}},
		"added qdisc under a nested one": {peerDelay, func(link *fakeLink, root string) func() {
			link.extra = append(link.extra, fakeExtraQdisc{kind: "pfifo", handle: "9998:", parent: link.child.handle + "1"})
			return func() { link.extra = nil }
		}},
		"qdisc under a whole-interface delay": {testDelay, func(link *fakeLink, root string) func() {
			link.extra = append(link.extra, fakeExtraQdisc{kind: "tbf", handle: "9999:", parent: root + "1"})
			return func() { link.extra = nil }
		}},
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, tc.delay)
			if err != nil {
				t.Fatal(err)
			}
			link := netem.eth0("osd-0")
			netem.mu.Lock()
			child := link.child
			detach := tc.attach(link, lease.handle)
			netem.mu.Unlock()
			if status, err := lease.Status(t.Context()); err != nil || status.Applied {
				t.Fatalf("status with foreign %s: %+v %v", name, status, err)
			}
			if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("Restore with foreign %s: %v", name, err)
			}
			if link.root == nil || link.root.handle != lease.handle || netem.liveHelpers() != 1 {
				t.Fatalf("Restore removed the root %+v or the helper (%d)", link.root, netem.liveHelpers())
			}
			if link.child == nil && child != nil || len(link.extra) == 0 && strings.HasPrefix(name, "added") {
				t.Fatalf("Restore removed the delay's child %+v or the foreign qdisc %v", link.child, link.extra)
			}
			if _, err := lease.Status(t.Context()); err != nil {
				t.Fatalf("the refused lease is not retryable: %v", err)
			}
			netem.mu.Lock()
			detach()
			netem.mu.Unlock()
			if status, err := lease.Status(t.Context()); err != nil || !status.Applied {
				t.Fatalf("status after the owner removed its %s: %+v %v", name, status, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if link.root != nil || len(link.filters) != 0 || netem.liveHelpers() != 0 {
				t.Fatalf("after Restore: root %+v, filters %v, %d helpers", link.root, link.filters, netem.liveHelpers())
			}
		})
	}
}

// TestNetworkDelayRestoreIgnoresQdiscsOutsideTheRoot keeps the ownership
// check to what removing the root removes: an ingress qdisc is not under it.
func TestNetworkDelayRestoreIgnoresQdiscsOutsideTheRoot(t *testing.T) {
	for name, delay := range map[string]NetworkProfile{"peer": peerDelay, "interface": testDelay} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
			if err != nil {
				t.Fatal(err)
			}
			link := netem.eth0("osd-0")
			netem.mu.Lock()
			link.extra = append(link.extra, fakeExtraQdisc{kind: "ingress", handle: "ffff:", parent: "ffff:fff1"})
			netem.mu.Unlock()
			if status, err := lease.Status(t.Context()); err != nil || !status.Applied {
				t.Fatalf("status with an ingress qdisc: %+v %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if link.root != nil || len(link.extra) != 1 || netem.liveHelpers() != 0 {
				t.Fatalf("after Restore: root %+v, other qdiscs %v, %d helpers", link.root, link.extra, netem.liveHelpers())
			}
		})
	}
}

func TestNetworkDelayLossValidation(t *testing.T) {
	for name, test := range map[string]struct {
		delay NetworkProfile
		ok    bool
	}{
		"loss alone":           {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Loss: 100}, true},
		"loss with a delay":    {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Millisecond, Loss: 12.5}, true},
		"no delay and no loss": {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane}, false},
		"loss over 100":        {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Loss: 100.5}, false},
		"negative loss":        {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Millisecond, Loss: -1}, false},
		"NaN loss":             {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Loss: math.NaN()}, false},
		"jitter without delay": {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Jitter: time.Millisecond, Loss: 50}, false},
		"a delay below 1ms":    {NetworkProfile{HelperImage: "h", Plane: ceph.PublicNetworkPlane, Delay: time.Microsecond, Loss: 50}, false},
	} {
		if err := validateNetworkDelay(test.delay); (err == nil) != test.ok {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestPeerNetworkDelayBlackhole(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	blackhole := NetworkProfile{HelperImage: "netem-helper:test", Plane: ceph.PublicNetworkPlane, Peer: peerDelay.Peer, Loss: 100}
	lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, blackhole)
	if err != nil {
		t.Fatal(err)
	}
	var child string
	for _, command := range netem.commands {
		if strings.Contains(command, " parent ") && strings.Contains(command, " netem") {
			child = command
		}
	}
	if !strings.HasSuffix(child, " handle "+lease.child+" netem loss 100%") {
		t.Fatalf("netem child command %q", child)
	}
	status, err := lease.Status(t.Context())
	if err != nil || !status.Applied || status.Loss != 100 || status.Delay != 0 || lease.Loss != 100 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A whole-interface loss with a delay.
	partial := NetworkProfile{HelperImage: "netem-helper:test", Plane: ceph.PublicNetworkPlane, Delay: 5 * time.Millisecond, Loss: 25}
	lease, err = cluster.ApplyNetworkProfile(t.Context(), osd, partial)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || status.Loss != 25 || status.Delay != 5*time.Millisecond {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A loss without a delay keeps every selection check: an invalid Peer or a
// PeerPort without one is refused before any helper or tc command.
func TestNetworkDelayLossKeepsSelectionChecks(t *testing.T) {
	loss := NetworkProfile{HelperImage: "netem-helper:test", Plane: ceph.PublicNetworkPlane, Loss: 100}
	for name, edit := range map[string]func(*NetworkProfile){
		"port without peer": func(d *NetworkProfile) { d.PeerPort = 6800 },
		"mapped peer":       func(d *NetworkProfile) { d.Peer = netip.MustParseAddr("::ffff:10.88.0.7") },
		"zoned peer":        func(d *NetworkProfile) { d.Peer = netip.MustParseAddr("fe80::1%eth0") },
		"unspecified peer":  func(d *NetworkProfile) { d.Peer = netip.IPv4Unspecified() },
	} {
		t.Run(name, func(t *testing.T) {
			delay := loss
			edit(&delay)
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
			if !errors.Is(err, ceph.ErrInvalidRequest) || lease != nil {
				t.Fatalf("%+v: %v, %v", delay, lease, err)
			}
			if len(netem.commands) != 0 || len(netem.helpers) != 0 {
				t.Fatalf("a refused request ran %v with %d helpers", netem.commands, len(netem.helpers))
			}
		})
	}
	// The valid loss-only requests still apply, whole-interface and to a peer.
	for _, delay := range []NetworkProfile{loss, {HelperImage: "netem-helper:test", Plane: ceph.PublicNetworkPlane, Loss: 100, Peer: peerDelay.Peer, PeerPort: 6800}} {
		netem := newFakeNetem()
		cluster, osd := delayCluster(netem)
		lease, err := cluster.ApplyNetworkProfile(t.Context(), osd, delay)
		if err != nil {
			t.Fatalf("%+v: %v", delay, err)
		}
		if status, err := lease.Status(t.Context()); err != nil || !status.Applied || status.Loss != 100 {
			t.Fatalf("%+v: status %+v %v", delay, status, err)
		}
		if err := lease.Restore(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}
