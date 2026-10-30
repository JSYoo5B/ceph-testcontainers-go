//go:build all || integration

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/testcontainers/testcontainers-go"
)

const (
	BandwidthPool = "bandwidth"
	// bandwidthObject is large enough that one read through the limit takes
	// several seconds, so that the session's setup and the first window of
	// a TCP connection do not decide the measured throughput.
	bandwidthObject = 16 << 20
	bandwidthRate   = 16 * fault.MegabitPerSecond
	// A limited read must stay at or below the rate, with a margin for the
	// clock, and keep at least this fraction of it: bounded, not stalled.
	BandwidthCeiling = 1.1
	bandwidthFloor   = 0.4
	// An unlimited read must be at least this many times the rate.
	BandwidthMargin = 4.0
)

// bandwidthReadScript opens one session, reads one byte of the object so
// that the session and its OSD connection exist, then reads the whole object
// count times and prints each read's duration and the payload's SHA-256.
const bandwidthReadScript = `import hashlib,json,rados,sys,time
pool,name,size,count=sys.argv[1],sys.argv[2],int(sys.argv[3]),int(sys.argv[4])
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.connect(timeout=30)
try:
 io=c.open_ioctx(pool)
 io.read(name,1)
 out={"seconds":[],"sha256":[]}
 for _ in range(count):
  start=time.monotonic(); data=io.read(name,size); out["seconds"].append(time.monotonic()-start)
  assert len(data)==size,len(data)
  out["sha256"].append(hashlib.sha256(data).hexdigest())
 io.close()
finally:
 c.shutdown()
print(json.dumps(out))
`

// bandwidthRead is one measured read run: its throughputs in bytes per
// second, slowest first.
type bandwidthRead struct {
	Seconds []float64 `json:"seconds"`
	SHA256  []string  `json:"sha256"`
}

func (r bandwidthRead) throughputs() []float64 {
	var out []float64
	for _, seconds := range r.Seconds {
		out = append(out, bandwidthObject/seconds)
	}
	slices.Sort(out)
	return out
}

func (r bandwidthRead) median() float64 {
	throughputs := r.throughputs()
	return throughputs[len(throughputs)/2]
}

func (r bandwidthRead) String() string {
	var parts []string
	for _, throughput := range r.throughputs() {
		parts = append(parts, fmt.Sprintf("%.2f", throughput/1e6))
	}
	return strings.Join(parts, ", ") + " MB/s"
}

// RateBytes is the limit in bytes per second.
func RateBytes(rate fault.NetworkRate) float64 {
	return float64(rate) / 8
}

// BandwidthFixture is a cluster with one 16 MiB object whose primary is the
// limited OSD, in a single-replica pool.
type BandwidthFixture struct {
	Image           string
	Cluster         *ceph.Container
	Client, Control testcontainers.Container
	Limited         *ceph.OSDContainer
	object, sha256  string
}

func NewBandwidthFixture(t *testing.T, ctx context.Context, customizers ...testcontainers.ContainerCustomizer) BandwidthFixture {
	t.Helper()
	image := NetemImage(t)
	cluster, client := NewServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithPoolDefaults(1, 1))}, customizers...)...)
	if _, err := cluster.Ceph(ctx, "osd", "pool", "create", BandwidthPool, "16"); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Ceph(ctx, "osd", "pool", "application", "enable", BandwidthPool, "rados"); err != nil {
		t.Fatal(err)
	}
	limited := clusteraccess.OSDs(t, cluster)[0]
	object, sha256 := PutBandwidthObject(t, ctx, cluster, client, limited.ID, "bulk", bandwidthObject)
	return BandwidthFixture{Image: image, Cluster: cluster, Client: client, Control: ToolkitContainer(t, cluster), Limited: limited, object: object, sha256: sha256}
}

// PutBandwidthObject writes size random bytes to an object, named with
// prefix, whose primary is osd, and returns its name and SHA-256. The tests
// write only before any limit.
func PutBandwidthObject(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, osd int, prefix string, size int) (string, string) {
	t.Helper()
	// The placement of a name, which needs no object.
	object := ""
	for i := 0; i < 64 && object == ""; i++ {
		name := fmt.Sprintf("%s-%d", prefix, i)
		out, err := cluster.Ceph(ctx, "osd", "map", BandwidthPool, name, "--format", "json")
		var placement struct {
			Primary int `json:"acting_primary"`
		}
		if err != nil || json.Unmarshal(out, &placement) != nil {
			t.Fatalf("osd map %s: %v: %s", name, err, out)
		}
		if placement.Primary == osd {
			object = name
		}
	}
	if object == "" {
		t.Fatalf("no name maps to osd.%d", osd)
	}
	out := DelayExec(t, ctx, client, "sh", "-c", fmt.Sprintf(`head -c %d /dev/urandom > /tmp/bulk && rados -p %s put %s /tmp/bulk && sha256sum /tmp/bulk && rm /tmp/bulk`,
		size, BandwidthPool, object))
	sha256, _, _ := strings.Cut(strings.TrimSpace(out), " ")
	return object, sha256
}

// read reads the object count times from ctr and checks every payload.
func (f BandwidthFixture) read(t *testing.T, ctx context.Context, ctr testcontainers.Container, count int) bandwidthRead {
	t.Helper()
	out := DelayExec(t, ctx, ctr, "python3", "-c", bandwidthReadScript, BandwidthPool, f.object, fmt.Sprint(bandwidthObject), fmt.Sprint(count))
	var read bandwidthRead
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &read); err != nil || len(read.Seconds) != count {
		t.Fatalf("parse the read %q: %v", out, err)
	}
	for _, sum := range read.SHA256 {
		if sum != f.sha256 {
			t.Fatalf("read %s, wrote %s", sum, f.sha256)
		}
	}
	return read
}

// limitedRead applies limit to the limited OSD, reads the object once from
// the client while the kernel's counters of the lease's netem qdisc are read
// before and after, restores the lease, and checks that the root qdisc and
// the helpers are gone. It returns the read and the counters' difference.
func (f BandwidthFixture) limitedRead(t *testing.T, ctx context.Context, limit fault.NetworkProfile, during func()) (bandwidthRead, fault.NetworkProfileStatus, fault.NetworkProfileStatus) {
	t.Helper()
	lease, err := fault.ApplyNetworkProfile(ctx, f.Cluster, f.Limited.Container, limit)
	if lease != nil {
		defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	before, err := lease.Status(ctx)
	if err != nil || !before.Applied || before.Rate != limit.Rate || before.Delay != limit.Delay || lease.Rate != limit.Rate ||
		before.QueueLimit != fault.DefaultNetworkQueueLimit || lease.QueueLimit != fault.DefaultNetworkQueueLimit {
		t.Fatalf("status of the rate limit: %+v, lease rate %v queue %d, %v", before, lease.Rate, lease.QueueLimit, err)
	}
	read := f.read(t, ctx, f.Client, 1)
	if during != nil {
		during()
	}
	after, err := lease.Status(ctx)
	if err != nil || !after.Applied || after.Bytes < before.Bytes {
		t.Fatalf("status after the read: %+v, %v", after, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if root := RootQdisc(t, ctx, f.Image, f.Limited.Container, lease.Interface); root != "noqueue 0:" {
		t.Fatalf("root qdisc after Restore: %s", root)
	}
	if helpers := NetemHelpers(t, ctx); helpers != 0 {
		t.Fatalf("%d netem helpers after Restore", helpers)
	}
	return read, before, after
}

func checkBounded(t *testing.T, what string, read bandwidthRead, rate fault.NetworkRate) {
	t.Helper()
	throughput, limit := read.throughputs()[0], RateBytes(rate)
	if throughput > BandwidthCeiling*limit || throughput < bandwidthFloor*limit {
		t.Fatalf("%s: %s, limit %v (%.2f MB/s)", what, read, rate, limit/1e6)
	}
}

func checkUnlimited(t *testing.T, what string, read bandwidthRead, rate fault.NetworkRate) {
	t.Helper()
	if read.median() < BandwidthMargin*RateBytes(rate) {
		t.Fatalf("%s: %s, not %.0f times the limit %v", what, read, BandwidthMargin, rate)
	}
}

func RunNetworkBandwidth(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	f := NewBandwidthFixture(t, ctx)
	address, err := f.Client.ContainerIP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peer := netip.MustParseAddr(address)
	limit := fault.NetworkProfile{HelperImage: f.Image, Plane: ceph.PublicNetworkPlane, Peer: peer, Rate: bandwidthRate}

	baseline := f.read(t, ctx, f.Client, 3)
	checkUnlimited(t, "baseline", baseline, bandwidthRate)

	// Only osd.N's packets to the client pass the limit: the client's read
	// of the object is bounded, the control container's read of the same
	// object from the same OSD is not.
	var unselected bandwidthRead
	limited, before, after := f.limitedRead(t, ctx, limit, func() { unselected = f.read(t, ctx, f.Control, 3) })
	checkBounded(t, "the client's read through the peer limit", limited, bandwidthRate)
	checkUnlimited(t, "the control container's read while the client's path was limited", unselected, bandwidthRate)
	// The IPv4 filter carried the transfer: its netem child sent at least the
	// object, and dropped nothing.
	if sent := after.Bytes - before.Bytes; sent < bandwidthObject || after.Drops != before.Drops {
		t.Fatalf("netem sent %d bytes and dropped %d packets of the selected path", sent, after.Drops-before.Drops)
	}
	restored := f.read(t, ctx, f.Client, 3)
	checkUnlimited(t, "after Restore", restored, bandwidthRate)

	// The rate composes with a delay in the same netem qdisc.
	composed := limit
	composed.Delay = 20 * time.Millisecond
	delayed, _, _ := f.limitedRead(t, ctx, composed, nil)
	checkBounded(t, "the client's read through the delayed peer limit", delayed, bandwidthRate)

	// Without a peer the whole endpoint is limited, the control container's
	// path too.
	whole := limit
	whole.Peer = netip.Addr{}
	var wholeControl bandwidthRead
	wholeRead, _, _ := f.limitedRead(t, ctx, whole, func() { wholeControl = f.read(t, ctx, f.Control, 1) })
	checkBounded(t, "the client's read through the interface limit", wholeRead, bandwidthRate)
	checkBounded(t, "the control container's read through the interface limit", wholeControl, bandwidthRate)
	final := f.read(t, ctx, f.Client, 3)
	checkUnlimited(t, "after the last Restore", final, bandwidthRate)

	t.Logf("NETWORK_BANDWIDTH_RECEIPT family=ipv4 osd=%d object=%d rate=%v: baseline %s; peer limit to %s: client %s, control %s, netem sent %d bytes in %d packets (drops %d, overlimits %d, backlog %d); restored %s; with %v delay: client %s; interface limit: client %s, control %s; final %s",
		f.Limited.ID, bandwidthObject, bandwidthRate, baseline, peer, limited, unselected, after.Bytes-before.Bytes, after.Packets-before.Packets,
		after.Drops-before.Drops, after.Overlimits-before.Overlimits, after.Backlog, restored, composed.Delay, delayed, wholeRead, wholeControl, final)
}

// RunNetworkBandwidthIPv6 limits an OSD of an IPv6 cluster to the client's
// IPv6 address, and then to the client's IPv4 address. The family that
// Ceph's session uses is observed, not inferred from the configuration: the
// IPv6 filter's netem child carries the read and bounds it, the IPv4
// filter's carries nothing of it and leaves it unlimited.
func RunNetworkBandwidthIPv6(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	f := NewBandwidthFixture(t, ctx, ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressIPv6)))
	inspection, err := f.Client.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspection.NetworkSettings.Networks[StaticFacts(f.Cluster).NetworkName]
	if endpoint == nil || !endpoint.GlobalIPv6Address.IsValid() || !endpoint.IPAddress.Is4() {
		t.Fatalf("no IPv4 and IPv6 address on %s", StaticFacts(f.Cluster).NetworkName)
	}
	client4, client6 := endpoint.IPAddress, endpoint.GlobalIPv6Address

	baseline := f.read(t, ctx, f.Client, 3)
	checkUnlimited(t, "baseline", baseline, bandwidthRate)

	limit := fault.NetworkProfile{HelperImage: f.Image, Plane: ceph.PublicNetworkPlane, Peer: client6, Rate: bandwidthRate}
	var unselected bandwidthRead
	limited, before, after := f.limitedRead(t, ctx, limit, func() { unselected = f.read(t, ctx, f.Control, 3) })
	checkBounded(t, "the client's read through the IPv6 peer limit", limited, bandwidthRate)
	checkUnlimited(t, "the control container's read while the client's IPv6 path was limited", unselected, bandwidthRate)
	sent6 := after.Bytes - before.Bytes
	if sent6 < bandwidthObject {
		t.Fatalf("the IPv6 filter's netem child sent %d bytes of a %d-byte read", sent6, bandwidthObject)
	}

	other := limit
	other.Peer = client4
	unlimited, before4, after4 := f.limitedRead(t, ctx, other, nil)
	checkUnlimited(t, "the client's read with only its IPv4 address limited", unlimited, bandwidthRate)
	sent4 := after4.Bytes - before4.Bytes
	if sent4 >= bandwidthObject/4 {
		t.Fatalf("the IPv4 filter's netem child sent %d bytes of a %d-byte read over IPv6", sent4, bandwidthObject)
	}
	restored := f.read(t, ctx, f.Client, 3)
	checkUnlimited(t, "after Restore", restored, bandwidthRate)

	t.Logf("NETWORK_BANDWIDTH_RECEIPT family=ipv6 osd=%d object=%d rate=%v: baseline %s; limit to %s: client %s, control %s, netem sent %d bytes in %d packets (drops %d, overlimits %d); limit to %s: client %s, netem sent %d bytes; restored %s",
		f.Limited.ID, bandwidthObject, bandwidthRate, baseline, client6, limited, unselected, sent6, after.Packets-before.Packets, after.Drops-before.Drops,
		after.Overlimits-before.Overlimits, client4, unlimited, sent4, restored)
}
