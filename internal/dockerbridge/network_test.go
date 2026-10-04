package dockerbridge

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"reflect"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

type bridgeFake struct {
	created, removed []*testcontainers.DockerNetwork
	requested        []*dockernetwork.IPAM
	active           map[string]dockernetwork.Inspect
	createHook       func(context.Context, *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error, bool)
	inspectHook      func(context.Context, string, dockernetwork.Inspect) (dockernetwork.Inspect, error)
	removeHook       func(context.Context, *testcontainers.DockerNetwork) error
	closeErr         error
	closes           int
	probes           int
}

func (f *bridgeFake) create(ctx context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error) {
	f.requested = append(f.requested, ipam)
	if f.createHook != nil {
		if n, err, handled := f.createHook(ctx, ipam); handled {
			return n, err
		}
	}
	if ipam == nil {
		f.probes++
		ipam = &dockernetwork.IPAM{Driver: "default", Options: map[string]string{"fixture": "custom-address-pool"}, Config: []dockernetwork.IPAMConfig{{
			Subnet: netip.MustParsePrefix(fmt.Sprintf("10.23.%d.0/24", f.probes)), Gateway: netip.MustParseAddr(fmt.Sprintf("10.23.%d.1", f.probes)),
		}}}
	}
	n := &testcontainers.DockerNetwork{ID: fmt.Sprintf("owned-%d", len(f.created)+1), Name: fmt.Sprintf("fixture-%d", len(f.created)+1), Driver: "bridge"}
	f.created = append(f.created, n)
	if f.active == nil {
		f.active = make(map[string]dockernetwork.Inspect)
	}
	f.active[n.ID] = dockernetwork.Inspect{Network: dockernetwork.Network{ID: n.ID, Name: n.Name, Driver: "bridge", IPAM: *ipam}}
	return n, nil
}

func (f *bridgeFake) NetworkInspect(ctx context.Context, id string, _ mobycl.NetworkInspectOptions) (mobycl.NetworkInspectResult, error) {
	n := f.active[id]
	if f.inspectHook != nil {
		var err error
		n, err = f.inspectHook(ctx, id, n)
		if err != nil {
			return mobycl.NetworkInspectResult{}, err
		}
	}
	return mobycl.NetworkInspectResult{Network: n}, nil
}

func (f *bridgeFake) remove(ctx context.Context, n *testcontainers.DockerNetwork) error {
	f.removed = append(f.removed, n)
	if f.removeHook != nil {
		if err := f.removeHook(ctx, n); err != nil {
			return err
		}
	}
	delete(f.active, n.ID)
	return nil
}

func (f *bridgeFake) Close() error { f.closes++; return f.closeErr }

func (f *bridgeFake) run(ctx context.Context) (*testcontainers.DockerNetwork, error) {
	return newNetwork(ctx, f, f.create, f.remove)
}

func TestBridgeRetainsDaemonSelectedAddressPool(t *testing.T) {
	f := &bridgeFake{}
	n, err := f.run(t.Context())
	if err != nil || n == nil || n != f.created[1] {
		t.Fatalf("create bridge: network=%v error=%v", n, err)
	}
	if len(f.created) != 2 || len(f.removed) != 1 || f.removed[0] != f.created[0] || len(f.active) != 1 || f.closes != 1 {
		t.Fatal("probe leaked or final bridge was discarded")
	}
	if f.requested[0] != nil || f.requested[1] == nil || f.requested[1].Config[0].Subnet != netip.MustParsePrefix("10.23.1.0/24") || f.requested[1].Config[0].Gateway != netip.MustParseAddr("10.23.1.1") {
		t.Fatal("explicit bridge did not use the daemon's selected subnet and gateway")
	}
	if f.requested[1].Options["fixture"] != "custom-address-pool" {
		t.Fatal("allocated IPAM configuration lost")
	}
}

func TestBridgeRetriesOnlyPoolCollisionWithFreshAllocation(t *testing.T) {
	f := &bridgeFake{}
	finalCreates := 0
	f.createHook = func(_ context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error, bool) {
		if ipam != nil {
			finalCreates++
			if finalCreates == 1 {
				return nil, errdefs.ErrInvalidArgument.WithMessage("Pool overlaps with other one on this address space"), true
			}
		}
		return nil, nil, false
	}
	n, err := f.run(t.Context())
	if err != nil || n == nil || f.probes != 2 || finalCreates != 2 || len(f.removed) != 2 || len(f.active) != 1 {
		t.Fatalf("collision retry: network=%v error=%v", n, err)
	}
	if f.requested[1].Config[0].Subnet == f.requested[3].Config[0].Subnet || f.requested[3].Config[0].Subnet != netip.MustParsePrefix("10.23.2.0/24") {
		t.Fatal("retried a stale subnet instead of asking the daemon again")
	}
	for _, failure := range []error{
		errdefs.ErrInvalidArgument.WithMessage("invalid bridge options"),
		errors.New("transport: Pool overlaps with other one on this address space"),
		context.DeadlineExceeded,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			f := &bridgeFake{createHook: func(_ context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error, bool) {
				return nil, failure, ipam != nil
			}}
			if n, err := f.run(t.Context()); n != nil || !errors.Is(err, failure) || len(f.requested) != 2 || len(f.active) != 0 {
				t.Fatalf("retried a non-collision or leaked probe: network=%v error=%v", n, err)
			}
		})
	}
}

func TestBridgeCollisionAttemptsAreBounded(t *testing.T) {
	f := &bridgeFake{createHook: func(_ context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error, bool) {
		return nil, errdefs.ErrInvalidArgument.WithMessage("Pool overlaps with other one on this address space"), ipam != nil
	}}
	n, err := f.run(t.Context())
	if n != nil || err == nil || len(f.requested) != 2*allocationAttempts || len(f.removed) != allocationAttempts || len(f.active) != 0 || f.closes != 1 {
		t.Fatalf("unbounded or leaking collision retry: network=%v error=%v", n, err)
	}
}

func TestBridgeCancellationCleansProbeWithIndependentBound(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	f := &bridgeFake{inspectHook: func(_ context.Context, _ string, n dockernetwork.Inspect) (dockernetwork.Inspect, error) {
		cancel()
		return n, nil
	}}
	f.removeHook = func(cleanupCtx context.Context, _ *testcontainers.DockerNetwork) error {
		deadline, ok := cleanupCtx.Deadline()
		if cleanupCtx.Err() != nil || !ok || time.Until(deadline) > cleanupTimeout {
			t.Fatal("canceled or unbounded failure cleanup context")
		}
		return nil
	}
	n, err := f.run(ctx)
	if n != nil || !errors.Is(err, context.Canceled) || len(f.requested) != 1 || len(f.removed) != 1 || len(f.active) != 0 {
		t.Fatalf("canceled probe leaked or final creation continued: network=%v error=%v", n, err)
	}
}

func TestBridgeInspectionAndRemovalFailuresRetainOwnership(t *testing.T) {
	inspectFailure := errors.New("inspection unavailable")
	removeFailure := errors.New("removal unavailable")
	for _, inspectFails := range []bool{true, false} {
		t.Run(fmt.Sprint(inspectFails), func(t *testing.T) {
			f := &bridgeFake{removeHook: func(context.Context, *testcontainers.DockerNetwork) error { return removeFailure }}
			if inspectFails {
				f.inspectHook = func(_ context.Context, _ string, n dockernetwork.Inspect) (dockernetwork.Inspect, error) {
					return n, inspectFailure
				}
			}
			n, err := f.run(t.Context())
			if n == nil || n != f.created[0] || !errors.Is(err, removeFailure) || len(f.requested) != 1 || len(f.active) != 1 || f.closes != 1 {
				t.Fatalf("lost failed probe ownership: network=%v error=%v", n, err)
			}
			if inspectFails && !errors.Is(err, inspectFailure) {
				t.Fatal("inspection failure was overwritten by cleanup failure")
			}
		})
	}
	f := &bridgeFake{inspectHook: func(_ context.Context, _ string, n dockernetwork.Inspect) (dockernetwork.Inspect, error) {
		return n, inspectFailure
	}}
	if n, err := f.run(t.Context()); n != nil || !errors.Is(err, inspectFailure) || len(f.active) != 0 {
		t.Fatal("successful failed-probe cleanup did not release ownership")
	}
}

func TestBridgeRejectsChangedNativeIdentityAndInvalidIPAM(t *testing.T) {
	cases := map[string]func(*dockernetwork.Inspect){
		"ID":     func(n *dockernetwork.Inspect) { n.ID = "foreign-id" },
		"name":   func(n *dockernetwork.Inspect) { n.Name = "foreign-name" },
		"driver": func(n *dockernetwork.Inspect) { n.Driver = "overlay" },
		"IPv6":   func(n *dockernetwork.Inspect) { n.EnableIPv6 = true },
		"attached": func(n *dockernetwork.Inspect) {
			n.Containers = map[string]dockernetwork.EndpointResource{"foreign-container": {}}
		},
		"missing": func(n *dockernetwork.Inspect) { n.IPAM.Config = nil },
		"subnet":  func(n *dockernetwork.Inspect) { n.IPAM.Config[0].Subnet = netip.Prefix{} },
		"noncanonical": func(n *dockernetwork.Inspect) {
			n.IPAM.Config[0].Subnet = netip.MustParsePrefix("10.23.1.4/24")
		},
		"gateway": func(n *dockernetwork.Inspect) { n.IPAM.Config[0].Gateway = netip.MustParseAddr("10.99.0.1") },
		"range": func(n *dockernetwork.Inspect) {
			n.IPAM.Config[0].IPRange = netip.MustParsePrefix("10.99.0.0/24")
		},
		"multiple": func(n *dockernetwork.Inspect) { n.IPAM.Config = append(n.IPAM.Config, n.IPAM.Config[0]) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := &bridgeFake{inspectHook: func(_ context.Context, _ string, n dockernetwork.Inspect) (dockernetwork.Inspect, error) {
				mutate(&n)
				return n, nil
			}}
			if n, err := f.run(t.Context()); n != nil || err == nil || len(f.requested) != 1 || len(f.active) != 0 || f.removed[0].ID != "owned-1" {
				t.Fatalf("accepted native replacement/invalid network or removed foreign ID: network=%v error=%v", n, err)
			}
		})
	}
}

func TestBridgeVerifiesFinalAllocationAndPreservesFailedCleanup(t *testing.T) {
	f := &bridgeFake{inspectHook: func(_ context.Context, id string, n dockernetwork.Inspect) (dockernetwork.Inspect, error) {
		if id == "owned-2" {
			n.IPAM.Config = []dockernetwork.IPAMConfig{{Subnet: netip.MustParsePrefix("10.99.0.0/24"), Gateway: netip.MustParseAddr("10.99.0.1")}}
		}
		return n, nil
	}}
	cleanupFailure := errors.New("final removal failed")
	f.removeHook = func(_ context.Context, n *testcontainers.DockerNetwork) error {
		if n.ID == "owned-2" {
			return cleanupFailure
		}
		return nil
	}
	n, err := f.run(t.Context())
	if n == nil || n != f.created[1] || !errors.Is(err, cleanupFailure) || len(f.active) != 1 || len(f.requested) != 2 {
		t.Fatalf("unchecked final allocation or lost owned final bridge: network=%v error=%v", n, err)
	}
}

func TestBridgeCloseFailureRetainsFinalNetwork(t *testing.T) {
	failure := errors.New("client close failed")
	f := &bridgeFake{closeErr: failure}
	n, err := f.run(t.Context())
	if n == nil || !errors.Is(err, failure) || len(f.active) != 1 || n != f.created[1] {
		t.Fatal("client close error discarded final network ownership")
	}
}

func TestBridgeCopiesObservedIPAMAndRejectsCanceledAdmission(t *testing.T) {
	f := &bridgeFake{}
	probe, err := f.create(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	native := f.active[probe.ID]
	native.IPAM.Config[0].AuxAddress = map[string]netip.Addr{"reserved": netip.MustParseAddr("10.23.1.9")}
	f.active[probe.ID] = native
	selected, err := inspectBridge(t.Context(), f, probe)
	if err != nil {
		t.Fatal(err)
	}
	selected.Options["fixture"] = "changed"
	selected.Config[0].AuxAddress["reserved"] = netip.MustParseAddr("10.23.1.10")
	if !reflect.DeepEqual(native.IPAM.Options, map[string]string{"fixture": "custom-address-pool"}) || !maps.Equal(native.IPAM.Config[0].AuxAddress, map[string]netip.Addr{"reserved": netip.MustParseAddr("10.23.1.9")}) {
		t.Fatal("IPAM recreation mutated observed Docker state")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n, err := New(ctx); n != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("canceled admission reached Docker")
	}
}
