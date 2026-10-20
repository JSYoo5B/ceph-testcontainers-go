package fault

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/moby/moby/api/types/container"
)

// testRate limits osd-0's packets to 32 Mbit/s, without a delay.
var testRate = NetworkDelay{HelperImage: "netem-helper:test", Plane: ceph.PublicNetworkPlane, Rate: 32 * MegabitPerSecond}

// rateRequests are rate limits of each shape that the lease installs: on the
// whole interface, with a delay and a loss in the same qdisc, and through the
// selective hierarchy of one peer and of peers of both families.
var rateRequests = map[string]NetworkDelay{
	"interface": testRate,
	"with delay and loss": func() NetworkDelay {
		delay := testRate
		delay.Delay, delay.Jitter, delay.Loss, delay.QueueLimit = 20*time.Millisecond, 2*time.Millisecond, 1, 250
		return delay
	}(),
	"peer": func() NetworkDelay {
		delay := testRate
		delay.Peer = netip.MustParseAddr("10.88.0.7")
		return delay
	}(),
	"peers": func() NetworkDelay {
		delay := testRate
		delay.Peers = []NetworkPeer{{Addr: netip.MustParseAddr("10.88.0.7")}, {Addr: netip.MustParseAddr("fd00::7"), Port: 6800}}
		return delay
	}(),
}

// rateCluster is delayCluster with an IPv6 address on osd-0's endpoint.
func rateCluster(netem *fakeNetem) (*delayTestCluster, *fakeNetemHelper) {
	netem.targets["osd-0"].address6 = netip.MustParseAddr("fd00::5")
	return delayCluster(netem)
}

// rateQdisc is the netem qdisc that carries the lease's rate in the model.
func (n *fakeNetem) rateQdisc(id string) *fakeQdisc {
	link := n.eth0(id)
	n.mu.Lock()
	defer n.mu.Unlock()
	if link.child != nil {
		return link.child
	}
	return link.root
}

func TestNetworkRateValidation(t *testing.T) {
	for name, change := range map[string]func(*NetworkDelay){
		"below the minimum":      func(d *NetworkDelay) { d.Rate = MinNetworkRate - 8 },
		"above the maximum":      func(d *NetworkDelay) { d.Rate = MaxNetworkRate + 8 },
		"not whole bytes":        func(d *NetworkDelay) { d.Rate = MinNetworkRate + 1 },
		"queue limit, no rate":   func(d *NetworkDelay) { d.Rate, d.Delay, d.QueueLimit = 0, time.Millisecond, 10 },
		"negative queue limit":   func(d *NetworkDelay) { d.QueueLimit = -1 },
		"queue limit too large":  func(d *NetworkDelay) { d.QueueLimit = MaxNetworkQueueLimit + 1 },
		"jitter without a delay": func(d *NetworkDelay) { d.Jitter = time.Millisecond },
		"short delay":            func(d *NetworkDelay) { d.Delay = time.Microsecond },
		"peer port, no peer":     func(d *NetworkDelay) { d.PeerPort = 6800 },
		"no helper":              func(d *NetworkDelay) { d.HelperImage = "" },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			request := testRate
			change(&request)
			if lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request); err == nil || lease != nil {
				t.Fatalf("accepted %+v", request)
			}
			if len(netem.helpers) != 0 || len(cluster.faultsOf(faulthook.NetworkDelay)) != 0 {
				t.Fatalf("%d helpers started, reservations %v for an invalid rate", len(netem.helpers), cluster.faultsOf(faulthook.NetworkDelay))
			}
		})
	}
	for _, rate := range []NetworkRate{MinNetworkRate, MaxNetworkRate} {
		request := testRate
		request.Rate = rate
		if err := validateNetworkDelay(request); err != nil {
			t.Fatalf("rate %v: %v", rate, err)
		}
	}
}

func TestNetworkRateString(t *testing.T) {
	for rate, want := range map[NetworkRate]string{
		32 * MegabitPerSecond: "32Mbit/s", 10 * GigabitPerSecond: "10Gbit/s", 64 * KilobitPerSecond: "64Kbit/s",
		1500 * KilobitPerSecond: "1500Kbit/s", 8: "8bit/s", 0: "0bit/s",
	} {
		if got := rate.String(); got != want {
			t.Errorf("%d: %s, want %s", uint64(rate), got, want)
		}
	}
}

// Each shape installs the rate and queue limit in the lease's netem qdisc,
// next to the delay and loss, and Status reports the kernel's values apart
// from the request.
func TestNetworkRateAppliesAndRestores(t *testing.T) {
	for name, request := range rateRequests {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := rateCluster(netem)
			lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
			if err != nil {
				t.Fatal(err)
			}
			wantLimit := request.QueueLimit
			if wantLimit == 0 {
				wantLimit = DefaultNetworkQueueLimit
			}
			if lease.Rate != request.Rate || lease.QueueLimit != wantLimit {
				t.Fatalf("lease rate %v, queue limit %d", lease.Rate, lease.QueueLimit)
			}
			qdisc := netem.rateQdisc("osd-0")
			if qdisc == nil || qdisc.kind != "netem" || qdisc.rate != uint64(request.Rate)/8 || qdisc.limit != wantLimit ||
				qdisc.delay != request.Delay || qdisc.jitter != request.Jitter {
				t.Fatalf("netem qdisc %+v", qdisc)
			}
			// What waits in the kernel's queue is reported as counters.
			netem.mu.Lock()
			qdisc.overlimits, qdisc.requeues, qdisc.backlog, qdisc.qlen = 3, 1, 9000, 6
			netem.mu.Unlock()
			status, err := lease.Status(t.Context())
			if err != nil || !status.Applied || status.Rate != request.Rate || status.QueueLimit != wantLimit || status.Delay != request.Delay ||
				status.Loss != request.Loss || status.Overlimits != 3 || status.Requeues != 1 || status.Backlog != 9000 || status.QueueLength != 6 {
				t.Fatalf("status %+v, %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
				t.Fatalf("root %+v and %d helpers after Restore", netem.root("osd-0"), netem.liveHelpers())
			}
		})
	}
}

// A delay without a Rate neither checks nor installs one: an image whose tc
// cannot parse netem's rate still delays.
func TestNetworkDelayWithoutRateNeedsNoRate(t *testing.T) {
	netem := newFakeNetem()
	netem.noRateParse, netem.rateIgnored = true, true
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || status.Rate != 0 || lease.Rate != 0 || lease.QueueLimit != 0 {
		t.Fatalf("status %+v, lease rate %v, %v", status, lease.Rate, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A tc that cannot parse netem's rate is the image's failure, and a kernel
// that does not keep it the environment's; both are found in the preflight,
// before the target changes.
func TestNetworkRatePreflight(t *testing.T) {
	for name, tc := range map[string]struct {
		setup      func(*fakeNetem)
		want, kind error
	}{
		"tc without rate":     {func(n *fakeNetem) { n.noRateParse = true }, ErrNetemImageUnsupported, ceph.ErrImageCapability},
		"kernel without rate": {func(n *fakeNetem) { n.rateIgnored = true }, ErrNetemKernelUnsupported, ceph.ErrEnvironmentUnavailable},
		"no rate result":      {func(n *fakeNetem) { n.editPreflight = func(out map[string]any) { delete(out, "rate") } }, ErrNetemImageUnsupported, ceph.ErrImageCapability},
		"rate not shown": {func(n *fakeNetem) {
			n.editPreflight = func(out map[string]any) { out["rate_shown"] = `[{"kind":"noqueue","handle":"0:","root":true}]` }
		}, ErrNetemImageUnsupported, ceph.ErrImageCapability},
		"another limit kept": {func(n *fakeNetem) {
			n.editPreflight = func(out map[string]any) {
				out["rate_shown"] = `[{"kind":"netem","handle":"c7e3:","root":true,"options":{"limit":1000,"rate":{"rate":125000}}}]`
			}
		}, ErrNetemKernelUnsupported, ceph.ErrEnvironmentUnavailable},
		"no delete result": {func(n *fakeNetem) { n.editPreflight = func(out map[string]any) { delete(out, "rate_delete") } }, ErrNetemImageUnsupported, ceph.ErrImageCapability},
	} {
		for shape, request := range rateRequests {
			t.Run(name+"/"+shape, func(t *testing.T) {
				netem := newFakeNetem()
				tc.setup(netem)
				cluster, osd := rateCluster(netem)
				lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
				if !errors.Is(err, tc.want) || !errors.Is(err, tc.kind) || lease != nil {
					t.Fatalf("got %v, lease %v; want %v", err, lease, tc.want)
				}
				for _, command := range netem.commands {
					if strings.HasPrefix(command, "tc ") {
						t.Fatalf("ran %q on the target", command)
					}
				}
				if netem.root("osd-0") != nil || netem.liveHelpers() != 0 || len(cluster.faultsOf(faulthook.NetworkDelay)) != 0 {
					t.Fatalf("root %+v, %d helpers, reservations %v", netem.root("osd-0"), netem.liveHelpers(), cluster.faultsOf(faulthook.NetworkDelay))
				}
			})
		}
	}
}

// A target whose kernel does not keep the rate that the preflight's kept,
// such as a target that the engine schedules on another kernel, returns the
// lease with an error: the qdisc is the lease's, and Restore removes it.
func TestNetworkRateKernelMismatchKeepsTheLease(t *testing.T) {
	for name, request := range rateRequests {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			netem.onStart = func(mode container.NetworkMode) {
				if mode != "none" {
					netem.rateIgnored = true
				}
			}
			cluster, osd := rateCluster(netem)
			lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
			if err == nil || lease == nil || !strings.Contains(err.Error(), "rate 0bit/s") {
				t.Fatalf("got %v, %v", lease, err)
			}
			if status, err := lease.Status(t.Context()); err != nil || !status.Applied || status.Rate != 0 || lease.Rate != request.Rate {
				t.Fatalf("status %+v, %v", status, err)
			}
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
				t.Fatal("Restore left the qdisc or the helper")
			}
		})
	}
}

// A rate that someone else changed on the lease's own qdisc is reported as
// the kernel's value, not the requested one.
func TestNetworkRateStatusReportsTheKernelsRate(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, testRate)
	if err != nil {
		t.Fatal(err)
	}
	netem.mu.Lock()
	netem.targets["osd-0"].namespace.links[1].root.rate = 1000
	netem.mu.Unlock()
	if status, err := lease.Status(t.Context()); err != nil || status.Rate != 8*KilobitPerSecond || lease.Rate != testRate.Rate {
		t.Fatalf("status %+v, lease rate %v, %v", status, lease.Rate, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A qdisc that someone else added under a rate limit's root is refused by
// Restore and kept, and Restore succeeds after its owner removed it.
func TestNetworkRateRestoreKeepsForeignState(t *testing.T) {
	for name, request := range rateRequests {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := rateCluster(netem)
			lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
			if err != nil {
				t.Fatal(err)
			}
			link := netem.eth0("osd-0")
			foreign := fakeExtraQdisc{kind: "tbf", handle: "9999:", parent: lease.handle + "1"}
			netem.mu.Lock()
			link.extra = append(link.extra, foreign)
			netem.mu.Unlock()
			if status, err := lease.Status(t.Context()); err != nil || status.Applied {
				t.Fatalf("status with a foreign qdisc %+v, %v", status, err)
			}
			if err := lease.Restore(t.Context()); !errors.Is(err, ceph.ErrInvalidRequest) {
				t.Fatalf("Restore with a foreign qdisc: %v", err)
			}
			if netem.root("osd-0") == nil || len(link.extra) != 1 || netem.liveHelpers() != 1 {
				t.Fatal("the refused Restore removed state")
			}
			netem.mu.Lock()
			link.extra = nil
			netem.mu.Unlock()
			if err := lease.Restore(t.Context()); err != nil {
				t.Fatal(err)
			}
			if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
				t.Fatal("Restore left the rate limit")
			}
		})
	}
}

// A rate limit whose tc command was canceled returns its lease, and one whose
// stop is unconfirmed fences it until the canceler confirms the stop.
func TestNetworkRateUncertainApply(t *testing.T) {
	netem := newFakeNetem()
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		apply()
		return supervised.Report{}, context.Canceled
	}
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, testRate)
	if !errors.Is(err, context.Canceled) || lease == nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || status.Rate != testRate.Rate {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Terminate left the rate limit")
	}

	netem = newFakeNetem()
	netem.onAdd = func(func() supervised.Report) (supervised.Report, error) {
		return supervised.Report{}, &supervised.NotStoppedError{Marker: "m1", Err: errors.New("cancel failed")}
	}
	netem.cancelErr = errors.New("the helper is frozen")
	cluster, osd = delayCluster(netem)
	lease, err = cluster.TemporaryNetworkDelay(t.Context(), osd, testRate)
	if lease == nil || err == nil || lease.pending != "m1" {
		t.Fatalf("got %+v, %v", lease, err)
	}
	commands := len(netem.commands)
	if err := lease.Restore(t.Context()); err == nil || len(netem.commands) != commands {
		t.Fatalf("Restore ran %v before the earlier tc was confirmed stopped: %v", netem.commands[commands:], err)
	}
	// The late command lands; once confirmed, Restore removes it.
	netem.setRoot("osd-0", &fakeQdisc{kind: "netem", handle: lease.handle, rate: uint64(testRate.Rate) / 8, limit: DefaultNetworkQueueLimit})
	netem.cancelErr = nil
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Restore left the rate limit")
	}
}
