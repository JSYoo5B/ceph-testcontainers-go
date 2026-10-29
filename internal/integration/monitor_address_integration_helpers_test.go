//go:build all || integration

package integration_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// monitorEndpoint returns ctr's endpoint on network, nil when it is detached.
func monitorEndpoint(t *testing.T, ctx context.Context, ctr testcontainers.Container, network string) *dockernetwork.EndpointSettings {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	inspection, err := docker.ContainerInspect(ctx, ctr.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if inspection.Container.NetworkSettings == nil {
		return nil
	}
	return inspection.Container.NetworkSettings.Networks[network]
}

// testMonitorAddressAcrossNetworkInterruption interrupts the public endpoint
// of a MON, once with the cluster-bound function and once with the standalone function,
// and stops and starts the MON. Start must refuse to run the MON while its
// endpoint is interrupted and must not reattach it; after Restore the MON
// starts at its original IPv4 and IPv6 addresses and aliases.
func testMonitorAddressAcrossNetworkInterruption(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	image, roles := integrationImages(t)
	cluster, _ := newServiceClusterWithOptions(t, image, roles.WithOptions(ceph.WithMonitorCount(3), ceph.WithNoInitialOSDs(),
		ceph.WithAddressFamily(ceph.AddressIPv6)))
	waitQuorumNames(t, ctx, cluster, "a", "b", "c")
	monitors := map[string]*ceph.MonitorContainer{}
	for _, mon := range clusteraccess.Monitors(t, cluster) {
		monitors[mon.DaemonName] = mon
	}
	network := staticFacts(cluster).NetworkName
	type restorer interface{ Restore(context.Context) error }
	for _, form := range []struct {
		name, mon string
		interrupt func(testcontainers.Container) (restorer, error)
	}{
		{"cluster-bound function", "b", func(ctr testcontainers.Container) (restorer, error) {
			return fault.InterruptNetwork(ctx, cluster, ctr, ceph.PublicNetworkPlane)
		}},
		{"standalone function", "c", func(ctr testcontainers.Container) (restorer, error) {
			return fault.InterruptLink(ctx, ctr, network)
		}},
	} {
		mon := monitors[form.mon]
		original := monitorEndpoint(t, ctx, mon.Container, network)
		if original == nil || !original.IPAddress.IsValid() || !original.GlobalIPv6Address.IsValid() {
			t.Fatalf("%s: mon.%s has no dual-stack endpoint: %+v", form.name, form.mon, original)
		}
		link, err := form.interrupt(mon.Container)
		if err != nil {
			t.Fatalf("%s: %v", form.name, err)
		}
		stop := 10 * time.Second
		if err := mon.Stop(ctx, &stop); err != nil {
			t.Fatal(err)
		}
		if err := mon.Start(ctx); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Fatalf("%s: Start of an interrupted mon.%s returned %v", form.name, form.mon, err)
		}
		if endpoint := monitorEndpoint(t, ctx, mon.Container, network); endpoint != nil {
			t.Fatalf("%s: Start reattached interrupted mon.%s at %s before Restore", form.name, form.mon, endpoint.IPAddress)
		}
		others := []string{"a", "b", "c"}
		others = slices.DeleteFunc(others, func(name string) bool { return name == form.mon })
		waitQuorumNames(t, ctx, cluster, others...)
		if err := link.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if err := mon.Start(ctx); err != nil {
			t.Fatalf("%s: Start after Restore: %v", form.name, err)
		}
		waitQuorumNames(t, ctx, cluster, "a", "b", "c")
		restored := monitorEndpoint(t, ctx, mon.Container, network)
		if restored == nil || restored.IPAddress != original.IPAddress || restored.GlobalIPv6Address != original.GlobalIPv6Address || !slices.Equal(restored.Aliases, original.Aliases) {
			t.Fatalf("%s: mon.%s came back at %+v, not %+v", form.name, form.mon, restored, original)
		}
		t.Logf("%s: Start refused interrupted mon.%s and left it detached; after Restore it came back at %s and %s", form.name, form.mon, restored.IPAddress, restored.GlobalIPv6Address)
	}
}
