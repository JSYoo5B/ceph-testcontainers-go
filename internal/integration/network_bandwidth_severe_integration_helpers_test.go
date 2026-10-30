//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"net/netip"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/testcontainers/testcontainers-go"
)

const (
	// severeRate is the lowest rate that the module accepts.
	severeRate = fault.MinNetworkRate
	// severeOpTimeout is the client's rados_osd_op_timeout in the timeout
	// profile, in seconds; only that profile's client session sets it.
	severeOpTimeout = 10
	// timeoutObject would take about 131 s at severeRate, far over the
	// client's operation timeout.
	timeoutObject = 1 << 20
	// slowObject takes about 33 s at severeRate.
	slowObject = 256 << 10
	// The 1 MiB timeout object's payload takes 0.08 s at queueRate. With
	// queueDelay, the path holds 250 KB in flight; queueLimit packets hold
	// less, so netem's queue overflows.
	queueRate  = 100 * fault.MegabitPerSecond
	queueDelay = 20 * time.Millisecond
	queueLimit = 2
	// The small queue's read must take at least this many times as long.
	queueSlowdown = 1.25
)

// severeReadScript opens one session, with rados_osd_op_timeout set to
// argv[4] seconds unless it is 0, reads one byte of the object so that the
// session and its OSD connection exist, then reads the whole object once and
// prints the errno (0 on success), the read's duration, the bytes it got and
// their SHA-256.
const severeReadScript = `import hashlib,json,rados,sys,time
pool,name,size,timeout=sys.argv[1],sys.argv[2],int(sys.argv[3]),sys.argv[4]
conf={"rados_osd_op_timeout":timeout} if timeout!="0" else {}
c=rados.Rados(conffile="/etc/ceph/ceph.conf",conf=conf)
c.connect(timeout=30)
try:
 io=c.open_ioctx(pool)
 io.read(name,1)
 out={"errno":0,"bytes":0,"sha256":""}
 start=time.monotonic()
 try:
  data=io.read(name,size)
  out["bytes"],out["sha256"]=len(data),hashlib.sha256(data).hexdigest()
 except rados.Error as e:
  out["errno"]=abs(getattr(e,"errno",None) or -1)
 out["seconds"]=time.monotonic()-start
 print(json.dumps(out))
 io.close()
finally:
 c.shutdown()
`

type severeRead struct {
	Errno   int     `json:"errno"`
	Seconds float64 `json:"seconds"`
	Bytes   int     `json:"bytes"`
	SHA256  string  `json:"sha256"`
}

func readSevere(t *testing.T, ctx context.Context, ctr testcontainers.Container, name string, size, timeout int) severeRead {
	t.Helper()
	out := harness.DelayExec(t, ctx, ctr, "python3", "-c", severeReadScript, harness.BandwidthPool, name, fmt.Sprint(size), fmt.Sprint(timeout))
	var read severeRead
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &read); err != nil {
		t.Fatalf("parse the read %q: %v", out, err)
	}
	return read
}

// complete checks that a read returned exactly the written object.
func (r severeRead) complete(t *testing.T, what string, size int, sha256 string) {
	t.Helper()
	if r.Errno != 0 || r.Bytes != size || r.SHA256 != sha256 {
		t.Fatalf("%s: %+v, want %d bytes with SHA-256 %s", what, r, size, sha256)
	}
}

// underLimit applies limit to the limited OSD, runs during, reads the
// lease's status before and after it, and restores the lease, checking that
// the root qdisc and the helpers are gone.
func underLimit(f harness.BandwidthFixture, t *testing.T, ctx context.Context, limit fault.NetworkProfile, during func()) (fault.NetworkProfileStatus, fault.NetworkProfileStatus) {
	t.Helper()
	lease, err := fault.ApplyNetworkProfile(ctx, f.Cluster, f.Limited.Container, limit)
	if lease != nil {
		defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	before, err := lease.Status(ctx)
	if err != nil || !before.Applied || before.Rate != limit.Rate || before.QueueLimit != lease.QueueLimit {
		t.Fatalf("status of the rate limit: %+v, %v", before, err)
	}
	during()
	after, err := lease.Status(ctx)
	if err != nil || !after.Applied {
		t.Fatalf("status after the read: %+v, %v", after, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if root := harness.RootQdisc(t, ctx, f.Image, f.Limited.Container, lease.Interface); root != "noqueue 0:" {
		t.Fatalf("root qdisc after Restore: %s", root)
	}
	if helpers := harness.NetemHelpers(t, ctx); helpers != 0 {
		t.Fatalf("%d netem helpers after Restore", helpers)
	}
	return before, after
}

// transferSeconds is how long size bytes take at rate, without headers.
func transferSeconds(size int, rate fault.NetworkRate) float64 {
	return float64(size) * 8 / float64(rate)
}

func testNetworkBandwidthSevere(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	f := harness.NewBandwidthFixture(t, ctx)
	address, err := f.Client.ContainerIP(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peer := netip.MustParseAddr(address)
	// Every object is written before any limit; under a limit there are
	// only reads, which are idempotent.
	timeoutName, timeoutSHA := harness.PutBandwidthObject(t, ctx, f.Cluster, f.Client, f.Limited.ID, "timeout", timeoutObject)
	slowName, slowSHA := harness.PutBandwidthObject(t, ctx, f.Cluster, f.Client, f.Limited.ID, "slow", slowObject)
	severe := fault.NetworkProfile{HelperImage: f.Image, Plane: ceph.PublicNetworkPlane, Peer: peer, Rate: severeRate}

	t.Run("op timeout", func(t *testing.T) {
		// At 64 Kbit/s the 1 MiB reply needs about 131 s; the client's
		// 10-second operation timeout ends the read first with ETIMEDOUT.
		var failed severeRead
		before, after := underLimit(f, t, ctx, severe, func() {
			failed = readSevere(t, ctx, f.Client, timeoutName, timeoutObject, severeOpTimeout)
		})
		sent := after.Bytes - before.Bytes
		if failed.Errno != 110 || failed.Bytes != 0 || failed.Seconds < 0.9*severeOpTimeout || failed.Seconds > severeOpTimeout+10 {
			t.Fatalf("the read through %v with a %d s operation timeout: %+v", severeRate, severeOpTimeout, failed)
		}
		// netem had not sent the reply when the client gave up.
		if sent >= timeoutObject {
			t.Fatalf("netem sent %d bytes of a %d-byte reply before the timeout", sent, timeoutObject)
		}
		// The read is idempotent: a fresh one after Restore gets the object.
		fresh := readSevere(t, ctx, f.Client, timeoutName, timeoutObject, severeOpTimeout)
		fresh.complete(t, "the fresh read after Restore", timeoutObject, timeoutSHA)
		t.Logf("NETWORK_BANDWIDTH_SEVERE_RECEIPT op-timeout osd=%d object=%d rate=%v op_timeout=%ds: errno %d after %.2f s (transfer needs %.0f s); netem sent %d bytes in %d packets, backlog %d bytes in %d packets, drops %d, overlimits %d; fresh read after Restore %.3f s with the exact SHA-256",
			f.Limited.ID, timeoutObject, severeRate, severeOpTimeout, failed.Errno, failed.Seconds, transferSeconds(timeoutObject, severeRate),
			sent, after.Packets-before.Packets, after.Backlog, after.QueueLength, after.Drops-before.Drops, after.Overlimits-before.Overlimits, fresh.Seconds)
	})

	t.Run("extreme slowdown", func(t *testing.T) {
		// Without an operation timeout the 256 KiB read completes, at least
		// as slowly as its payload alone takes at 64 Kbit/s, while the
		// control container reads it from the same OSD at once.
		baseline := readSevere(t, ctx, f.Client, slowName, slowObject, 0)
		baseline.complete(t, "the baseline read", slowObject, slowSHA)
		var limited, control severeRead
		before, after := underLimit(f, t, ctx, severe, func() {
			limited = readSevere(t, ctx, f.Client, slowName, slowObject, 0)
			control = readSevere(t, ctx, f.Control, slowName, slowObject, 0)
		})
		limited.complete(t, "the limited read", slowObject, slowSHA)
		control.complete(t, "the control container's read", slowObject, slowSHA)
		restored := readSevere(t, ctx, f.Client, slowName, slowObject, 0)
		restored.complete(t, "the read after Restore", slowObject, slowSHA)
		floor := transferSeconds(slowObject, severeRate)
		if limited.Seconds < 0.8*floor || limited.Seconds > 5*floor {
			t.Fatalf("the read through %v took %.2f s; its payload alone needs %.1f s", severeRate, limited.Seconds, floor)
		}
		for what, read := range map[string]severeRead{"baseline": baseline, "control container": control, "after Restore": restored} {
			if read.Seconds > 1 {
				t.Fatalf("the %s read took %.2f s", what, read.Seconds)
			}
		}
		t.Logf("NETWORK_BANDWIDTH_SEVERE_RECEIPT slowdown osd=%d object=%d rate=%v: client %.2f s (payload needs %.1f s, %.0f times the baseline %.4f s), control %.4f s, after Restore %.4f s; netem sent %d bytes in %d packets, drops %d",
			f.Limited.ID, slowObject, severeRate, limited.Seconds, floor, limited.Seconds/baseline.Seconds, baseline.Seconds, control.Seconds, restored.Seconds,
			after.Bytes-before.Bytes, after.Packets-before.Packets, after.Drops-before.Drops)
	})

	t.Run("queue overflow", func(t *testing.T) {
		// The same rate and delay with netem's default queue and with a
		// queue of two packets. The queue holds the packets that wait for
		// the delay too, so two packets cannot cover the path's bandwidth
		// and delay: it overflows, TCP retransmits what netem dropped, and
		// the read completes, more slowly.
		limit := severe
		limit.Rate, limit.Delay = queueRate, queueDelay
		var wide, narrow severeRead
		wideBefore, wideAfter := underLimit(f, t, ctx, limit, func() {
			wide = readSevere(t, ctx, f.Client, timeoutName, timeoutObject, 0)
		})
		limit.QueueLimit = queueLimit
		narrowBefore, narrowAfter := underLimit(f, t, ctx, limit, func() {
			narrow = readSevere(t, ctx, f.Client, timeoutName, timeoutObject, 0)
		})
		wide.complete(t, "the read with the default queue", timeoutObject, timeoutSHA)
		narrow.complete(t, "the read with the small queue", timeoutObject, timeoutSHA)
		wideDrops, narrowDrops := wideAfter.Drops-wideBefore.Drops, narrowAfter.Drops-narrowBefore.Drops
		if narrowDrops == 0 || narrow.Seconds < queueSlowdown*wide.Seconds {
			t.Fatalf("queue %d: %.2f s, %d drops; default queue: %.2f s, %d drops", queueLimit, narrow.Seconds, narrowDrops, wide.Seconds, wideDrops)
		}
		t.Logf("NETWORK_BANDWIDTH_SEVERE_RECEIPT queue osd=%d object=%d rate=%v delay=%v: default queue %d packets %.2f s (payload needs %.2f s), %d drops; queue %d packets %.2f s (%.1f times), %d drops, %d bytes sent in %d packets; both reads returned the exact SHA-256",
			f.Limited.ID, timeoutObject, queueRate, queueDelay, fault.DefaultNetworkQueueLimit, wide.Seconds, transferSeconds(timeoutObject, queueRate), wideDrops,
			queueLimit, narrow.Seconds, narrow.Seconds/wide.Seconds, narrowDrops, narrowAfter.Bytes-narrowBefore.Bytes, narrowAfter.Packets-narrowBefore.Packets)
	})
}
