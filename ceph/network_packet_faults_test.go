package ceph

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
)

// faultRequests are packet faults of each supported shape: a reorder with
// its delay, a duplicate without a delay, a corrupt with a rate and a loss,
// and all three through the selective hierarchy of peers of both families.
var faultRequests = map[string]NetworkDelay{
	"reorder": {HelperImage: "netem-helper:test", Plane: PublicNetworkPlane, Delay: 10 * time.Millisecond,
		Reorder: NetworkPacketFault{Percent: 25, Correlation: 50}},
	"duplicate": {HelperImage: "netem-helper:test", Plane: PublicNetworkPlane, Duplicate: NetworkPacketFault{Percent: 10, Correlation: 30}},
	"corrupt with rate and loss": {HelperImage: "netem-helper:test", Plane: PublicNetworkPlane, Rate: 32 * MegabitPerSecond, Loss: 1,
		Corrupt: NetworkPacketFault{Percent: 5, Correlation: 20}},
	"all on peers": {HelperImage: "netem-helper:test", Plane: PublicNetworkPlane, Delay: 10 * time.Millisecond,
		Peers:   []NetworkPeer{{Addr: netip.MustParseAddr("10.88.0.7")}, {Addr: netip.MustParseAddr("fd00::7"), Port: 6800}},
		Reorder: NetworkPacketFault{Percent: 25}, Duplicate: NetworkPacketFault{Percent: 5, Correlation: 10}, Corrupt: NetworkPacketFault{Percent: 0.5}},
}

// requestedFault is the request's fault for a netem keyword.
func requestedFault(request NetworkDelay, name string) NetworkPacketFault {
	for _, fault := range request.packetFaults() {
		if fault.name == name {
			return fault.fault
		}
	}
	return NetworkPacketFault{}
}

func TestNetworkPacketFaultValidation(t *testing.T) {
	base := NetworkDelay{HelperImage: "netem-helper:test", Plane: PublicNetworkPlane, Delay: 10 * time.Millisecond}
	for name, change := range map[string]func(*NetworkDelay){
		"reorder without a delay":     func(d *NetworkDelay) { d.Delay, d.Loss, d.Reorder.Percent = 0, 1, 25 },
		"reorder with jitter":         func(d *NetworkDelay) { d.Jitter, d.Reorder.Percent = time.Millisecond, 25 },
		"reorder with a rate":         func(d *NetworkDelay) { d.Rate, d.Reorder.Percent = MinNetworkRate, 25 },
		"reorder over 100":            func(d *NetworkDelay) { d.Reorder.Percent = 100.5 },
		"negative duplicate":          func(d *NetworkDelay) { d.Duplicate.Percent = -1 },
		"NaN corrupt":                 func(d *NetworkDelay) { d.Corrupt.Percent = math.NaN() },
		"correlation over 100":        func(d *NetworkDelay) { d.Duplicate = NetworkPacketFault{Percent: 1, Correlation: 101} },
		"negative correlation":        func(d *NetworkDelay) { d.Corrupt = NetworkPacketFault{Percent: 1, Correlation: -1} },
		"NaN correlation":             func(d *NetworkDelay) { d.Reorder = NetworkPacketFault{Percent: 1, Correlation: math.NaN()} },
		"correlation without percent": func(d *NetworkDelay) { d.Corrupt.Correlation = 10 },
		"short delay with duplicate":  func(d *NetworkDelay) { d.Delay, d.Duplicate.Percent = time.Microsecond, 1 },
		"jitter, no delay, duplicate": func(d *NetworkDelay) { d.Delay, d.Jitter, d.Duplicate.Percent = 0, time.Millisecond, 1 },
	} {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := delayCluster(netem)
			request := base
			change(&request)
			if lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request); !errors.Is(err, ErrInvalidRequest) || lease != nil {
				t.Fatalf("accepted %+v: %v", request, err)
			}
			if len(netem.helpers) != 0 || len(cluster.faultsOf(faultNetworkDelay)) != 0 {
				t.Fatalf("%d helpers started, reservations %v for an invalid fault", len(netem.helpers), cluster.faultsOf(faultNetworkDelay))
			}
		})
	}
	for name, request := range faultRequests {
		if err := validateNetworkDelay(request); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	for _, request := range []NetworkDelay{
		{HelperImage: "x", Plane: PublicNetworkPlane, Corrupt: NetworkPacketFault{Percent: 100, Correlation: 100}},
		{HelperImage: "x", Plane: PublicNetworkPlane, Delay: time.Millisecond, Jitter: time.Millisecond, Duplicate: NetworkPacketFault{Percent: 1}, Rate: MinNetworkRate},
	} {
		if err := validateNetworkDelay(request); err != nil {
			t.Fatalf("%+v: %v", request, err)
		}
	}
}

func TestNetworkPacketFaultString(t *testing.T) {
	for fault, want := range map[NetworkPacketFault]string{
		{Percent: 25}: "25%", {Percent: 0.5, Correlation: 10}: "0.5% 10%", {Percent: 100, Correlation: 100}: "100% 100%",
	} {
		if got := fault.String(); got != want {
			t.Errorf("%+v: %s, want %s", fault, got, want)
		}
	}
}

// Each shape installs its faults in the lease's netem qdisc, and Status
// reports the kernel's faults and gap apart from the request.
func TestNetworkPacketFaultsApplyAndRestore(t *testing.T) {
	for name, request := range faultRequests {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := rateCluster(netem)
			lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
			if err != nil {
				t.Fatal(err)
			}
			if lease.Reorder != request.Reorder || lease.Duplicate != request.Duplicate || lease.Corrupt != request.Corrupt {
				t.Fatalf("lease faults %+v %+v %+v", lease.Reorder, lease.Duplicate, lease.Corrupt)
			}
			qdisc := netem.rateQdisc("osd-0")
			for _, keyword := range []string{"reorder", "duplicate", "corrupt"} {
				if qdisc.faults[keyword] != requestedFault(request, keyword) {
					t.Fatalf("netem %s %+v, requested %+v", keyword, qdisc.faults[keyword], requestedFault(request, keyword))
				}
			}
			status, err := lease.Status(t.Context())
			wantGap := 0
			if !request.Reorder.IsZero() {
				wantGap = 1
			}
			if err != nil || !status.Applied || !request.Reorder.matches(status.Reorder) || !request.Duplicate.matches(status.Duplicate) ||
				!request.Corrupt.matches(status.Corrupt) || status.Gap != wantGap || status.Delay != request.Delay || status.Rate != request.Rate {
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

// A delay without packet faults neither checks nor installs them: an image
// whose tc cannot parse them, on a kernel that drops them, still delays.
func TestNetworkDelayWithoutFaultsNeedsNoFaults(t *testing.T) {
	netem := newFakeNetem()
	netem.faultUnparsed = "reorder"
	netem.faultIgnored = map[string]bool{"reorder": true, "duplicate": true, "corrupt": true, "gap": true}
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, testDelay)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || !status.Reorder.IsZero() || status.Gap != 0 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A tc that cannot parse a requested fault is the image's failure, and a
// kernel that does not keep it, its correlation or a reorder's gap the
// environment's; all are found in the preflight, before the target changes.
func TestNetworkPacketFaultPreflight(t *testing.T) {
	type refusal struct {
		setup      func(*fakeNetem, string)
		want, kind error
	}
	refusals := map[string]refusal{
		"tc without the fault":       {func(n *fakeNetem, fault string) { n.faultUnparsed = fault }, ErrNetemImageUnsupported, ErrImageCapability},
		"kernel without the fault":   {func(n *fakeNetem, fault string) { n.faultIgnored = map[string]bool{fault: true} }, ErrNetemKernelUnsupported, ErrEnvironmentUnavailable},
		"kernel without correlation": {func(n *fakeNetem, fault string) { n.faultChanged = map[string]bool{fault: true} }, ErrNetemKernelUnsupported, ErrEnvironmentUnavailable},
		"no faults result": {func(n *fakeNetem, _ string) { n.editPreflight = func(out map[string]any) { delete(out, "faults") } },
			ErrNetemImageUnsupported, ErrImageCapability},
		"faults not shown": {func(n *fakeNetem, _ string) {
			n.editPreflight = func(out map[string]any) { out["faults_shown"] = `[{"kind":"noqueue","handle":"0:","root":true}]` }
		}, ErrNetemImageUnsupported, ErrImageCapability},
		"faults unparsable": {func(n *fakeNetem, _ string) { n.editPreflight = func(out map[string]any) { out["faults_shown"] = "{" } },
			ErrNetemImageUnsupported, ErrImageCapability},
		"no delete result": {func(n *fakeNetem, _ string) {
			n.editPreflight = func(out map[string]any) { delete(out, "faults_delete") }
		},
			ErrNetemImageUnsupported, ErrImageCapability},
		"delete failed": {func(n *fakeNetem, _ string) {
			n.editPreflight = func(out map[string]any) {
				out["faults_delete"] = []any{2, "RTNETLINK answers: Operation not permitted"}
			}
		}, ErrNetemPermissionDenied, ErrEnvironmentUnavailable},
	}
	for name, tc := range refusals {
		for shape, request := range faultRequests {
			for _, fault := range request.requestedFaults() {
				t.Run(name+"/"+shape+"/"+fault, func(t *testing.T) {
					netem := newFakeNetem()
					tc.setup(netem, fault)
					cluster, osd := rateCluster(netem)
					lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
					if !errors.Is(err, tc.want) || !errors.Is(err, tc.kind) || lease != nil {
						t.Fatalf("got %v, lease %v; want %v", err, lease, tc.want)
					}
					checkNothingApplied(t, netem, cluster)
				})
			}
		}
	}
	// A kernel that keeps a reorder without its gap of 1 reorders nothing.
	netem := newFakeNetem()
	netem.faultIgnored = map[string]bool{"gap": true}
	cluster, osd := rateCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, faultRequests["reorder"])
	if !errors.Is(err, ErrNetemKernelUnsupported) || !strings.Contains(err.Error(), "gap") || lease != nil {
		t.Fatalf("got %v, lease %v", err, lease)
	}
	checkNothingApplied(t, netem, cluster)
}

// checkNothingApplied checks that no tc command ran on the target and that
// no root qdisc, helper or reservation remains.
func checkNothingApplied(t *testing.T, netem *fakeNetem, cluster *Container) {
	t.Helper()
	for _, command := range netem.commands {
		if strings.HasPrefix(command, "tc ") {
			t.Fatalf("ran %q on the target", command)
		}
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 || len(cluster.faultsOf(faultNetworkDelay)) != 0 {
		t.Fatalf("root %+v, %d helpers, reservations %v", netem.root("osd-0"), netem.liveHelpers(), cluster.faultsOf(faultNetworkDelay))
	}
}

// A target whose kernel does not keep the faults that the preflight's kept
// returns the lease with an error: the qdisc is the lease's, and Restore
// removes it.
func TestNetworkPacketFaultKernelMismatchKeepsTheLease(t *testing.T) {
	for name, request := range faultRequests {
		for _, ignored := range append(request.requestedFaults(), "gap") {
			if ignored == "gap" && request.Reorder.IsZero() {
				continue
			}
			t.Run(name+"/"+ignored, func(t *testing.T) {
				netem := newFakeNetem()
				netem.onStart = func(mode container.NetworkMode) {
					if mode != "none" {
						netem.faultIgnored = map[string]bool{ignored: true}
					}
				}
				cluster, osd := rateCluster(netem)
				lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
				if err == nil || lease == nil || !strings.Contains(err.Error(), ignored) {
					t.Fatalf("got %v, %v", lease, err)
				}
				status, err := lease.Status(t.Context())
				if err != nil || !status.Applied {
					t.Fatalf("status %+v, %v", status, err)
				}
				if ignored == "gap" && status.Gap != 0 || ignored == "reorder" && !status.Reorder.IsZero() ||
					ignored == "duplicate" && !status.Duplicate.IsZero() || ignored == "corrupt" && !status.Corrupt.IsZero() {
					t.Fatalf("status reports the request, not the kernel: %+v", status)
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
}

// A kernel that installs a fault that was not requested fails the apply too.
func TestNetworkPacketFaultUnrequestedFaultFailsTheApply(t *testing.T) {
	netem := newFakeNetem()
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		report := apply()
		netem.mu.Lock()
		defer netem.mu.Unlock()
		root := netem.targets["osd-0"].namespace.links[1].root
		root.faults = map[string]NetworkPacketFault{"duplicate": {Percent: 1}}
		return report, nil
	}
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, testDelay)
	if err == nil || lease == nil || !strings.Contains(err.Error(), "duplicate 1%") {
		t.Fatalf("got %v, %v", lease, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A fault that someone else changed on the lease's own qdisc is reported
// as the kernel's value, not the requested one.
func TestNetworkPacketFaultStatusReportsTheKernelsFaults(t *testing.T) {
	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	request := faultRequests["reorder"]
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
	if err != nil {
		t.Fatal(err)
	}
	netem.mu.Lock()
	root := netem.targets["osd-0"].namespace.links[1].root
	root.faults["reorder"] = NetworkPacketFault{Percent: 1.5}
	root.faults["corrupt"] = NetworkPacketFault{Percent: 2, Correlation: 3}
	netem.mu.Unlock()
	status, err := lease.Status(t.Context())
	if err != nil || status.Reorder != (NetworkPacketFault{Percent: 1.5}) || status.Corrupt != (NetworkPacketFault{Percent: 2, Correlation: 3}) ||
		lease.Reorder != request.Reorder || !lease.Corrupt.IsZero() {
		t.Fatalf("status %+v, lease %+v, %v", status, lease.Reorder, err)
	}
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// Members named like netem's faults on a qdisc that is not netem, outside
// the lease's root, are not netem's: they neither break the dump nor count.
func TestNetworkPacketFaultForeignMembers(t *testing.T) {
	var qdisc netemQdisc
	if err := json.Unmarshal([]byte(`{"kind":"foreign","options":{"duplicate":3,"corrupt":true,"reorder":"x","gap":"none"}}`), &qdisc); err != nil {
		t.Fatal(err)
	}
	if qdisc.Options.Duplicate.fault("duplicate") != (NetworkPacketFault{}) || qdisc.Options.Gap != 0 {
		t.Fatalf("decoded %+v", qdisc.Options)
	}
	if err := json.Unmarshal([]byte(`{"kind":"netem","options":{"gap":1.5,"reorder":{"reorder":0.25,"correlation":0.5}}}`), &qdisc); err != nil {
		t.Fatal(err)
	}
	if qdisc.Options.Gap != 0 || qdisc.Options.Reorder.fault("reorder") != (NetworkPacketFault{Percent: 25, Correlation: 50}) {
		t.Fatalf("decoded %+v", qdisc.Options)
	}

	netem := newFakeNetem()
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, faultRequests["duplicate"])
	if err != nil {
		t.Fatal(err)
	}
	link := netem.eth0("osd-0")
	netem.mu.Lock()
	link.extra = append(link.extra, fakeExtraQdisc{kind: "faultlike", handle: "ffff:", parent: "ffff:fff1"})
	netem.mu.Unlock()
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || status.Duplicate != (NetworkPacketFault{Percent: 10, Correlation: 30}) {
		t.Fatalf("status with a foreign qdisc outside the root %+v, %v", status, err)
	}
	netem.mu.Lock()
	link.extra = nil
	netem.mu.Unlock()
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// A qdisc that someone else added under a fault's root is refused by
// Restore and kept, and Restore succeeds after its owner removed it.
func TestNetworkPacketFaultRestoreKeepsForeignState(t *testing.T) {
	for name, request := range faultRequests {
		t.Run(name, func(t *testing.T) {
			netem := newFakeNetem()
			cluster, osd := rateCluster(netem)
			lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
			if err != nil {
				t.Fatal(err)
			}
			link := netem.eth0("osd-0")
			netem.mu.Lock()
			link.extra = append(link.extra, fakeExtraQdisc{kind: "tbf", handle: "9999:", parent: lease.handle + "1"})
			netem.mu.Unlock()
			if status, err := lease.Status(t.Context()); err != nil || status.Applied {
				t.Fatalf("status with a foreign qdisc %+v, %v", status, err)
			}
			if err := lease.Restore(t.Context()); !errors.Is(err, ErrInvalidRequest) {
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
				t.Fatal("Restore left the faults")
			}
		})
	}
}

// A fault whose tc command was canceled returns its lease, and one whose
// stop is unconfirmed fences it until the canceler confirms the stop.
func TestNetworkPacketFaultUncertainApply(t *testing.T) {
	request := faultRequests["reorder"]
	netem := newFakeNetem()
	netem.onAdd = func(apply func() supervised.Report) (supervised.Report, error) {
		apply()
		return supervised.Report{}, context.Canceled
	}
	cluster, osd := delayCluster(netem)
	lease, err := cluster.TemporaryNetworkDelay(t.Context(), osd, request)
	if !errors.Is(err, context.Canceled) || lease == nil {
		t.Fatalf("got %v, %v", lease, err)
	}
	if status, err := lease.Status(t.Context()); err != nil || !status.Applied || !request.Reorder.matches(status.Reorder) || status.Gap != 1 {
		t.Fatalf("status %+v, %v", status, err)
	}
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Terminate left the faults")
	}

	netem = newFakeNetem()
	netem.onAdd = func(func() supervised.Report) (supervised.Report, error) {
		return supervised.Report{}, &supervised.NotStoppedError{Marker: "m1", Err: errors.New("cancel failed")}
	}
	netem.cancelErr = errors.New("the helper is frozen")
	cluster, osd = delayCluster(netem)
	lease, err = cluster.TemporaryNetworkDelay(t.Context(), osd, request)
	if lease == nil || err == nil || lease.pending != "m1" {
		t.Fatalf("got %+v, %v", lease, err)
	}
	commands := len(netem.commands)
	if err := lease.Restore(t.Context()); err == nil || len(netem.commands) != commands {
		t.Fatalf("Restore ran %v before the earlier tc was confirmed stopped: %v", netem.commands[commands:], err)
	}
	// The late command lands; once confirmed, Restore removes it.
	netem.setRoot("osd-0", fakeNetemQdisc(lease.handle, []string{"delay", "10000us", "reorder", "25%", "50%"}))
	netem.cancelErr = nil
	if err := lease.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if netem.root("osd-0") != nil || netem.liveHelpers() != 0 {
		t.Fatal("Restore left the faults")
	}
}
