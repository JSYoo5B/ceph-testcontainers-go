package dockerbridge

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"reflect"
	"slices"
	"sync"
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
	return newNetwork(ctx, f, IPv4Only, f.create, f.remove)
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
	selected, err := inspectBridge(t.Context(), f, probe, IPv4Only)
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

func TestDualStackBridgeKeepsBothSubnets(t *testing.T) {
	v4 := dockernetwork.IPAMConfig{Subnet: netip.MustParsePrefix("10.23.1.0/24"), Gateway: netip.MustParseAddr("10.23.1.1")}
	v6 := dockernetwork.IPAMConfig{Subnet: netip.MustParsePrefix("fd62:93fe:2c9e::/64"), Gateway: netip.MustParseAddr("fd62:93fe:2c9e::1")}
	inspect := func(enableIPv6 bool, family Family, configs ...dockernetwork.IPAMConfig) (*dockernetwork.IPAM, error) {
		n := &testcontainers.DockerNetwork{ID: "owned", Name: "fixture", Driver: "bridge"}
		f := &bridgeFake{active: map[string]dockernetwork.Inspect{"owned": {Network: dockernetwork.Network{ID: "owned", Name: "fixture", Driver: "bridge",
			EnableIPv6: enableIPv6, IPAM: dockernetwork.IPAM{Driver: "default", Config: configs}}}}}
		return inspectBridge(t.Context(), f, n, family)
	}
	// Docker may list the IPv6 subnet first; the result is IPv4 then IPv6.
	ipam, err := inspect(true, DualStack, v6, v4)
	if err != nil || len(ipam.Config) != 2 || ipam.Config[0].Subnet != v4.Subnet || ipam.Config[1].Subnet != v6.Subnet {
		t.Fatalf("dual-stack IPAM %+v, %v", ipam, err)
	}
	for name, test := range map[string]struct {
		enableIPv6 bool
		family     Family
		configs    []dockernetwork.IPAMConfig
	}{
		"IPv6 on an IPv4 bridge":      {true, IPv4Only, []dockernetwork.IPAMConfig{v4, v6}},
		"IPv6 disabled on dual stack": {false, DualStack, []dockernetwork.IPAMConfig{v4, v6}},
		"no IPv6 subnet":              {true, DualStack, []dockernetwork.IPAMConfig{v4}},
		"no IPv4 subnet":              {true, DualStack, []dockernetwork.IPAMConfig{v6}},
		"two IPv6 subnets":            {true, DualStack, []dockernetwork.IPAMConfig{v4, v6, {Subnet: netip.MustParsePrefix("fd00:1::/64"), Gateway: netip.MustParseAddr("fd00:1::1")}}},
		"IPv4 gateway in IPv6 subnet": {true, DualStack, []dockernetwork.IPAMConfig{v4, {Subnet: v6.Subnet, Gateway: netip.MustParseAddr("10.23.1.1")}}},
		"IPv4-mapped IPv6 subnet":     {true, DualStack, []dockernetwork.IPAMConfig{v4, {Subnet: netip.MustParsePrefix("::ffff:10.23.1.0/120"), Gateway: netip.MustParseAddr("::ffff:10.23.1.1")}}},
	} {
		t.Run(name, func(t *testing.T) {
			if ipam, err := inspect(test.enableIPv6, test.family, test.configs...); err == nil {
				t.Fatalf("accepted %+v", ipam)
			}
		})
	}
}

// overlapError is the error that Docker returns for an explicit subnet that
// another network holds: HTTP 403, which the client reports as permission
// denied, not as an invalid argument.
func overlapError() error {
	return errdefs.ErrPermissionDenied.WithMessage("create network: Error response from daemon: invalid pool request: Pool overlaps with other one on this address space: failed to create network")
}

func TestBridgeRetriesTheOverlapThatDockerReportsAsForbidden(t *testing.T) {
	f := &bridgeFake{}
	finalCreates := 0
	f.createHook = func(_ context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error, bool) {
		if ipam != nil {
			finalCreates++
			if finalCreates == 1 {
				return nil, overlapError(), true
			}
		}
		return nil, nil, false
	}
	if n, err := f.run(t.Context()); err != nil || n == nil || finalCreates != 2 || len(f.active) != 1 {
		t.Fatalf("a forbidden pool overlap was not retried: network=%v error=%v", n, err)
	}
	other := &bridgeFake{createHook: func(_ context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error, bool) {
		return nil, errdefs.ErrPermissionDenied.WithMessage("permission denied"), ipam != nil
	}}
	if n, err := other.run(t.Context()); n != nil || err == nil || len(other.requested) != 2 {
		t.Fatalf("another forbidden error was retried: network=%v error=%v", n, err)
	}
}

// poolEngine models Docker's address pool: a network without IPAM gets the
// lowest free /24, and an explicit subnet that a network holds is refused
// as an overlap. It is safe for concurrent use. beforeExplicit runs before
// a bridge with an explicit subnet is created.
type poolEngine struct {
	mu             sync.Mutex
	networks       map[string]netip.Prefix
	next           int
	overlaps       int
	beforeExplicit func()
}

func (e *poolEngine) create(_ context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error) {
	if ipam != nil && e.beforeExplicit != nil {
		e.beforeExplicit()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	taken := slices.Collect(maps.Values(e.networks))
	var subnet netip.Prefix
	if ipam == nil {
		for i := 1; ; i++ {
			if subnet = netip.MustParsePrefix(fmt.Sprintf("10.24.%d.0/24", i)); !slices.Contains(taken, subnet) {
				break
			}
		}
	} else if subnet = ipam.Config[0].Subnet; slices.Contains(taken, subnet) {
		e.overlaps++
		return nil, overlapError()
	}
	e.next++
	id := fmt.Sprintf("net-%d", e.next)
	e.networks[id] = subnet
	return &testcontainers.DockerNetwork{ID: id, Name: "fixture-" + id, Driver: "bridge"}, nil
}

func (e *poolEngine) remove(_ context.Context, n *testcontainers.DockerNetwork) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.networks, n.ID)
	return nil
}

func (e *poolEngine) NetworkInspect(_ context.Context, id string, _ mobycl.NetworkInspectOptions) (mobycl.NetworkInspectResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	subnet, ok := e.networks[id]
	if !ok {
		return mobycl.NetworkInspectResult{}, errdefs.ErrNotFound
	}
	return mobycl.NetworkInspectResult{Network: dockernetwork.Inspect{Network: dockernetwork.Network{ID: id, Name: "fixture-" + id, Driver: "bridge",
		IPAM: dockernetwork.IPAM{Driver: "default", Config: []dockernetwork.IPAMConfig{{Subnet: subnet, Gateway: subnet.Addr().Next()}}}}}}, nil
}

func (e *poolEngine) Close() error { return nil }

// Two bridges of one process, as of parallel tests, must not take the same
// allocated subnet. The second allocation starts after the first removed its
// probe and before it recreated the bridge, which is when the subnet is free
// in between; the first waits up to 300 ms for the second's probe.
func TestConcurrentBridgesDoNotCollide(t *testing.T) {
	engine := &poolEngine{networks: map[string]netip.Prefix{}}
	secondProbed := make(chan struct{})
	probes := 0
	create := func(ctx context.Context, ipam *dockernetwork.IPAM) (*testcontainers.DockerNetwork, error) {
		n, err := engine.create(ctx, ipam)
		engine.mu.Lock()
		if ipam == nil {
			if probes++; probes == 2 {
				close(secondProbed)
			}
		}
		engine.mu.Unlock()
		return n, err
	}
	var once sync.Once
	second := make(chan error, 1)
	engine.beforeExplicit = func() {
		once.Do(func() {
			go func() {
				_, err := newNetwork(t.Context(), engine, IPv4Only, create, engine.remove)
				second <- err
			}()
			select {
			case <-secondProbed:
			case <-time.After(300 * time.Millisecond):
			}
		})
	}
	if _, err := newNetwork(t.Context(), engine, IPv4Only, create, engine.remove); err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
	if len(engine.networks) != 2 || engine.overlaps != 0 {
		t.Fatalf("%d networks, %d overlapping requests", len(engine.networks), engine.overlaps)
	}
}
