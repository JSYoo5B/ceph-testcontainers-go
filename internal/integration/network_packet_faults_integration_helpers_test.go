//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// transportScript prints a network namespace's kernel transport counters
// (/proc/net/netstat and /proc/net/snmp, prefixed as nstat names them, and
// /proc/net/snmp6) and the offload settings of the interface argv[1], read
// with the ethtool ioctls that need no capability.
const transportScript = `import array,fcntl,json,socket,struct,sys
def table(path):
 out={}
 lines=open(path).read().splitlines()
 for head,values in zip(lines[0::2],lines[1::2]):
  h,v=head.split(),values.split()
  if h and v and h[0]==v[0]:
   for k,x in zip(h[1:],v[1:]): out[h[0].rstrip(":")+k]=int(x)
 return out
c=table("/proc/net/netstat");c.update(table("/proc/net/snmp"))
try:
 for line in open("/proc/net/snmp6"):
  f=line.split()
  if len(f)==2: c[f[0]]=int(f[1])
except OSError: pass
off={}
s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
for name,cmd in (("rx-checksum",0x14),("tx-checksum",0x16),("tso",0x1e),("gso",0x23),("gro",0x2b)):
 buf=array.array("B",struct.pack("II",cmd,0))
 try:
  fcntl.ioctl(s.fileno(),0x8946,struct.pack("16sP",sys.argv[1].encode(),buf.buffer_info()[0]))
  off[name]=struct.unpack("II",buf.tobytes())[1]
 except OSError as e: off[name]="errno %d"%e.errno
print(json.dumps({"counters":c,"offload":off}))
`

// Receiver counters that show each packet effect.
var (
	// reorderCounters count segments that arrived after a later one and
	// waited in the receiver's out-of-order queue.
	reorderCounters = []string{"TcpExtTCPOFOQueue"}
	// duplicateCounters count segments that arrived again after the
	// receiver had them, which it answered with a D-SACK.
	duplicateCounters = []string{"TcpExtTCPDSACKOldSent"}
	// corruptCounters count TCP segments and IPv4 headers whose checksum
	// failed at the receiver.
	corruptCounters = []string{"TcpInCsumErrors", "IpExtInCsumErrors"}
	// recordedCounters are recorded for each window, besides the effects:
	// the receiver's other reorder, duplicate and header errors, and the
	// sender's view of reordering, D-SACKs and retransmissions.
	recordedCounters = []string{"TcpExtTCPOFOQueue", "TcpExtTCPOFOMerge", "TcpExtTCPDSACKOldSent", "TcpExtTCPDSACKOfoSent", "TcpExtDelayedACKLost",
		"TcpInCsumErrors", "IpExtInCsumErrors", "IpInHdrErrors", "IpInAddrErrors", "Ip6InHdrErrors", "Ip6InAddrErrors",
		"TcpExtTCPSACKReorder", "TcpExtTCPTSReorder", "TcpExtTCPRenoReorder", "TcpExtTCPDSACKRecv", "TcpRetransSegs"}
)

type transportSnapshot struct {
	Counters map[string]int64 `json:"counters"`
	Offload  map[string]any   `json:"offload"`
}

// transportObserver reads a container's namespace from the test's own
// helper in it, without NET_ADMIN, so the counters are the kernel's for that
// namespace, not the container's view of its application.
type transportObserver struct {
	name, iface string
	ctr         testcontainers.Container
}

func (o transportObserver) snapshot(t *testing.T, ctx context.Context) transportSnapshot {
	t.Helper()
	out := delayExec(t, ctx, o.ctr, "python3", "-c", transportScript, o.iface)
	var snapshot transportSnapshot
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &snapshot); err != nil || len(snapshot.Counters) == 0 {
		t.Fatalf("parse %s's transport counters %q: %v", o.name, out, err)
	}
	return snapshot
}

// delta is the sum of keys' increases from before to after.
func delta(before, after transportSnapshot, keys ...string) int64 {
	var sum int64
	for _, key := range keys {
		sum += after.Counters[key] - before.Counters[key]
	}
	return sum
}

// deltas formats every recorded counter that changed.
func deltas(before, after transportSnapshot) string {
	var parts []string
	for _, key := range recordedCounters {
		if d := delta(before, after, key); d != 0 {
			parts = append(parts, fmt.Sprintf("%s=%d", key, d))
		}
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}

// packetFaultProfile is one egress packet fault on the OSD's path to the
// client, the receiver counters that each must show it, and how many reads
// of the object the window holds. netem decides per queued packet, and a
// segmentation offload packet carries many TCP segments, so a duplicate or a
// corrupt affects far fewer packets per read than a reorder under a delay,
// which keeps packets small.
type packetFaultProfile struct {
	name    string
	fault   ceph.NetworkDelay
	effects [][]string
	reads   int
}

var packetFaultProfiles = []packetFaultProfile{
	{"reorder", ceph.NetworkDelay{Delay: 10 * time.Millisecond, Reorder: ceph.NetworkPacketFault{Percent: 25}}, [][]string{reorderCounters}, 1},
	{"duplicate", ceph.NetworkDelay{Duplicate: ceph.NetworkPacketFault{Percent: 50}}, [][]string{duplicateCounters}, 4},
	{"corrupt", ceph.NetworkDelay{Corrupt: ceph.NetworkPacketFault{Percent: 5}}, [][]string{corruptCounters}, 8},
}

// composedProfile composes a duplicate with a correlation and a corrupt
// with a delay and a rate in the same netem qdisc.
var composedProfile = packetFaultProfile{"duplicate+corrupt with delay and rate",
	ceph.NetworkDelay{Delay: 5 * time.Millisecond, Rate: 256 * ceph.MegabitPerSecond,
		Duplicate: ceph.NetworkPacketFault{Percent: 30, Correlation: 25}, Corrupt: ceph.NetworkPacketFault{Percent: 5}},
	[][]string{duplicateCounters, corruptCounters}, 2}

// engineContext describes the engine and kernel that the counters come
// from.
func engineContext(t *testing.T, ctx context.Context) string {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("engine %s, kernel %s, %s, %s", info.Info.ServerVersion, info.Info.KernelVersion, info.Info.OperatingSystem, info.Info.Architecture)
}

const (
	// packetFaultObject is the object that each window reads: hundreds of
	// TCP segments, and a reorder window of a few seconds.
	packetFaultObject = 4 << 20
	// naturalEffects is what a receiver may count after Restore besides
	// what the test's baseline counted: a local bridge reorders a segment
	// now and then, and a sender's tail loss probe can make the receiver send
	// a D-SACK.
	naturalEffects = 5
	// minEffects is what the selected receiver must count at least, and
	// effectMargin how many times the unaffected receiver's count in the
	// same window and the baseline's.
	minEffects   = 10
	effectMargin = 4
)

// packetFaultFixture reads one object, whose primary is the faulted OSD,
// from the client and the control container, and observes the three
// namespaces.
type packetFaultFixture struct {
	bandwidthFixture
	name, sha256 string
	peer         netip.Addr
	observers    map[string]transportObserver
	// baseline is the most that either receiver counted of each effect
	// (counters joined with "+") in four reads without a fault.
	baseline map[string]int64
}

// readObject reads the object count times from ctr, each in its own
// session, checks every SHA-256 and returns the durations.
func (p packetFaultFixture) readObject(t *testing.T, ctx context.Context, what string, ctr testcontainers.Container, count int) string {
	t.Helper()
	var seconds []string
	for range count {
		read := readSevere(t, ctx, ctr, p.name, packetFaultObject, 0)
		read.complete(t, what, packetFaultObject, p.sha256)
		seconds = append(seconds, fmt.Sprintf("%.3f", read.Seconds))
	}
	return strings.Join(seconds, ", ") + " s"
}

func (p packetFaultFixture) snapshots(t *testing.T, ctx context.Context, names ...string) map[string]transportSnapshot {
	t.Helper()
	out := map[string]transportSnapshot{}
	for _, name := range names {
		out[name] = p.observers[name].snapshot(t, ctx)
	}
	return out
}

// window applies profile to the OSD's packets to the client, reads the
// object profile.reads times from the client and from the control container while
// the three namespaces' counters are read before and after, and restores the
// lease. It checks the kernel's installed faults against the request, each
// effect at the client and none at the control container, that the root
// qdisc and the helpers are gone, and that the next read shows none of the
// effects. It returns the receipt.
func (p packetFaultFixture) window(t *testing.T, ctx context.Context, profile packetFaultProfile) string {
	t.Helper()
	request := profile.fault
	request.HelperImage, request.Plane, request.Peer = p.image, ceph.PublicNetworkPlane, p.peer
	lease, err := p.cluster.TemporaryNetworkDelay(ctx, p.limited.Container, request)
	if lease != nil {
		defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	wantGap := 0
	if !request.Reorder.IsZero() {
		wantGap = 1
	}
	installed, err := lease.Status(ctx)
	if err != nil || !installed.Applied || installed.Reorder != request.Reorder || installed.Duplicate != request.Duplicate ||
		installed.Corrupt != request.Corrupt || installed.Gap != wantGap || installed.Delay != request.Delay || installed.Rate != request.Rate {
		t.Fatalf("%s: installed %+v, %v", profile.name, installed, err)
	}
	before := p.snapshots(t, ctx, "client", "control", "sender")
	start := time.Now()
	client := p.readObject(t, ctx, profile.name+": the client's read", p.client, profile.reads)
	control := p.readObject(t, ctx, profile.name+": the control container's read", p.control, profile.reads)
	window := time.Since(start)
	after := p.snapshots(t, ctx, "client", "control", "sender")
	status, err := lease.Status(ctx)
	if err != nil || !status.Applied {
		t.Fatalf("%s: status after the reads %+v, %v", profile.name, status, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if root := rootQdisc(t, ctx, p.image, p.limited.Container, lease.Interface); root != "noqueue 0:" {
		t.Fatalf("root qdisc after Restore: %s", root)
	}
	if helpers := netemHelpers(t, ctx); helpers != 0 {
		t.Fatalf("%d netem helpers after Restore", helpers)
	}
	var effects []string
	for _, counters := range profile.effects {
		selected, unselected := delta(before["client"], after["client"], counters...), delta(before["control"], after["control"], counters...)
		effects = append(effects, fmt.Sprintf("%s client +%d control +%d", strings.Join(counters, "+"), selected, unselected))
		noise := max(unselected, p.baseline[strings.Join(counters, "+")])
		if selected < max(minEffects, effectMargin*noise) {
			t.Errorf("%s: %s client +%d, control +%d, baseline +%d over %.1f s; the client must count at least %d and %d times the others; client %s; control %s",
				profile.name, strings.Join(counters, "+"), selected, unselected, p.baseline[strings.Join(counters, "+")], window.Seconds(), minEffects, effectMargin,
				deltas(before["client"], after["client"]), deltas(before["control"], after["control"]))
		}
	}

	// After Restore the same reads are fast again, and count at most a
	// quarter of the effects, or what a receiver counts without a fault.
	recovering := p.snapshots(t, ctx, "client")
	start = time.Now()
	restored := p.readObject(t, ctx, profile.name+": the read after Restore", p.client, profile.reads)
	if elapsed := time.Since(start); elapsed > time.Duration(profile.reads)*time.Second {
		t.Errorf("%s: %d reads after Restore took %v", profile.name, profile.reads, elapsed)
	}
	recovered := p.snapshots(t, ctx, "client")
	for _, counters := range profile.effects {
		d, selected := delta(recovering["client"], recovered["client"], counters...), delta(before["client"], after["client"], counters...)
		if d > max(naturalEffects, p.baseline[strings.Join(counters, "+")], selected/effectMargin) {
			t.Errorf("%s: %s +%d at the client after Restore, +%d under the fault", profile.name, strings.Join(counters, "+"), d, selected)
		}
	}
	return fmt.Sprintf("%s (installed reorder %v, duplicate %v, corrupt %v, gap %d, delay %v, rate %v): %s; client reads %s, control reads %s (all exact SHA-256) in a %.1f s window; netem sent %d packets, requeues %d, drops %d; client %s; control %s; sender %s; after Restore client reads %s, client %s",
		profile.name, installed.Reorder, installed.Duplicate, installed.Corrupt, installed.Gap, installed.Delay, installed.Rate, strings.Join(effects, ", "),
		client, control, window.Seconds(), status.Packets-installed.Packets, status.Requeues-installed.Requeues, status.Drops-installed.Drops,
		deltas(before["client"], after["client"]), deltas(before["control"], after["control"]), deltas(before["sender"], after["sender"]),
		restored, deltas(recovering["client"], recovered["client"]))
}

func testNetworkPacketFaults(t *testing.T, ipv6 bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	var customizers []testcontainers.ContainerCustomizer
	family := "ipv4"
	if ipv6 {
		customizers, family = append(customizers, ceph.WithAddressFamily(ceph.AddressIPv6)), "ipv6"
	}
	p := packetFaultFixture{bandwidthFixture: newBandwidthFixture(t, ctx, customizers...), observers: map[string]transportObserver{}}
	p.name, p.sha256 = putBandwidthObject(t, ctx, p.cluster, p.client, p.limited.ID, "faults", packetFaultObject)
	inspection, err := p.client.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspection.NetworkSettings.Networks[p.cluster.NetworkName()]
	if endpoint == nil || !endpoint.IPAddress.Is4() || ipv6 && !endpoint.GlobalIPv6Address.IsValid() {
		t.Fatalf("no address of the family on %s", p.cluster.NetworkName())
	}
	p.peer = endpoint.IPAddress
	if ipv6 {
		p.peer = endpoint.GlobalIPv6Address
	}
	for name, ctr := range map[string]testcontainers.Container{"client": p.client, "control": p.control, "sender": p.limited.Container} {
		observer := startNetemObserver(t, ctx, p.image, ctr, false)
		defer func() { _ = observer.Terminate(context.WithoutCancel(ctx)) }()
		p.observers[name] = transportObserver{name: name, iface: "eth0", ctr: observer}
	}
	t.Logf("NETWORK_PACKET_FAULT_CONTEXT family=%s: %s; helper %s; offload client %v, sender %v", family, engineContext(t, ctx), p.image,
		p.observers["client"].snapshot(t, ctx).Offload, p.observers["sender"].snapshot(t, ctx).Offload)

	// Without a fault the same reads count few of the effects at either
	// receiver; each window must count far more.
	before := p.snapshots(t, ctx, "client", "control")
	baseline := p.readObject(t, ctx, "the baseline read", p.client, 4)
	control := p.readObject(t, ctx, "the control container's baseline read", p.control, 4)
	after := p.snapshots(t, ctx, "client", "control")
	p.baseline = map[string]int64{}
	for _, counters := range [][]string{reorderCounters, duplicateCounters, corruptCounters} {
		for _, name := range []string{"client", "control"} {
			key := strings.Join(counters, "+")
			p.baseline[key] = max(p.baseline[key], delta(before[name], after[name], counters...))
		}
	}
	t.Logf("NETWORK_PACKET_FAULT_RECEIPT family=%s baseline: client reads %s, control reads %s; client %s; control %s",
		family, baseline, control, deltas(before["client"], after["client"]), deltas(before["control"], after["control"]))

	profiles := slices.Clone(packetFaultProfiles)
	if !ipv6 {
		profiles = append(profiles, composedProfile)
	}
	for _, profile := range profiles {
		t.Logf("NETWORK_PACKET_FAULT_RECEIPT family=%s osd=%d object=%d peer=%s %s", family, p.limited.ID, packetFaultObject, p.peer, p.window(t, ctx, profile))
	}
}
