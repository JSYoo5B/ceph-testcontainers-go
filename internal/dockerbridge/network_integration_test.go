//go:build all || (integration && (!ci || (ci_environment && (!ci_batch || ci_batch_sdk_network))))

//ci: timeout=15m job-timeout=25

package dockerbridge_test

import (
	"context"
	"net/netip"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/internal/dockerbridge"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// Docker 28 refuses a static reconnect address on a bridge whose subnet was
// only automatically allocated. Exercise the real endpoint restoration against
// New's explicit-IPAM bridge, without a Ceph daemon or an image build.
func TestRecoverableBridgeEndpointIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := docker.Close(); err != nil {
			t.Errorf("close bridge regression Docker client: %v", err)
		}
	})
	newBridge := func() *testcontainers.DockerNetwork {
		t.Helper()
		bridge, err := dockerbridge.New(ctx)
		if bridge != nil {
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				if err := bridge.Remove(cleanupCtx); err != nil {
					t.Errorf("remove owned regression bridge: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatalf("create recoverable bridge: %v", err)
		}
		if bridge == nil || bridge.ID == "" || bridge.Name == "" {
			t.Fatal("recoverable bridge lacks native identity")
		}
		return bridge
	}
	first, second := newBridge(), newBridge()
	if first.ID == second.ID || first.Name == second.Name {
		t.Fatal("two bridge allocations reused the same native identity")
	}
	inspectSubnet := func(bridge *testcontainers.DockerNetwork) netip.Prefix {
		t.Helper()
		inspection, err := docker.NetworkInspect(ctx, bridge.ID, mobycl.NetworkInspectOptions{})
		if err != nil {
			t.Fatalf("inspect recoverable bridge: %v", err)
		}
		native := inspection.Network
		if native.ID != bridge.ID || native.Name != bridge.Name || native.Driver != "bridge" || len(native.IPAM.Config) != 1 {
			t.Fatal("created bridge has unexpected native identity, driver or IPAM configuration")
		}
		ipam := native.IPAM.Config[0]
		if !ipam.Subnet.IsValid() || !ipam.Subnet.Addr().Is4() || !ipam.Gateway.Is4() || !ipam.Subnet.Contains(ipam.Gateway) {
			t.Fatal("Docker-allocated bridge lacks a valid IPv4 subnet and gateway")
		}
		return ipam.Subnet
	}
	firstSubnet, secondSubnet := inspectSubnet(first), inspectSubnet(second)
	if firstSubnet.Overlaps(secondSubnet) {
		t.Fatal("two Docker-allocated bridge subnets overlap")
	}
	const priority = 7
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          "busybox:1.37.0",
			Entrypoint:     []string{"/bin/sh", "-c"},
			Cmd:            []string{"exec sleep 300"},
			Networks:       []string{first.Name},
			NetworkAliases: map[string][]string{first.Name: {"restore-original"}},
			EndpointSettingsModifier: func(endpoints map[string]*dockernetwork.EndpointSettings) {
				if endpoint := endpoints[first.Name]; endpoint != nil {
					endpoint.GwPriority = priority
				}
			},
		},
		Started: true,
	})
	if ctr != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := ctr.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate owned bridge regression container: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatalf("start bridge regression container: %v", err)
	}
	if ctr == nil || ctr.GetContainerID() == "" {
		t.Fatal("bridge regression container lacks native identity")
	}
	if _, err := docker.NetworkConnect(ctx, second.ID, mobycl.NetworkConnectOptions{
		Container:      ctr.GetContainerID(),
		EndpointConfig: &dockernetwork.EndpointSettings{Aliases: []string{"retained-secondary"}},
	}); err != nil {
		t.Fatalf("attach second bridge endpoint: %v", err)
	}
	inspectContainer := func() mobycl.ContainerInspectResult {
		t.Helper()
		inspection, err := docker.ContainerInspect(ctx, ctr.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatalf("inspect bridge regression container: %v", err)
		}
		if inspection.Container.State == nil || !inspection.Container.State.Running || inspection.Container.State.Pid <= 0 || inspection.Container.NetworkSettings == nil {
			t.Fatal("bridge regression container lacks a running native process or network settings")
		}
		return inspection
	}
	before := inspectContainer()
	firstEndpoint := before.Container.NetworkSettings.Networks[first.Name]
	secondEndpoint := before.Container.NetworkSettings.Networks[second.Name]
	if firstEndpoint == nil || secondEndpoint == nil || !firstSubnet.Contains(firstEndpoint.IPAddress) || !secondSubnet.Contains(secondEndpoint.IPAddress) || firstEndpoint.GwPriority != priority || !slices.Contains(firstEndpoint.Aliases, "restore-original") || !slices.Contains(secondEndpoint.Aliases, "retained-secondary") {
		t.Fatal("initial endpoints lack their allocated addresses, requested aliases or gateway priority")
	}
	checkRetained := func(inspection mobycl.ContainerInspectResult) {
		t.Helper()
		retained := inspection.Container.NetworkSettings.Networks[second.Name]
		if inspection.Container.State.Pid != before.Container.State.Pid || retained == nil || retained.IPAddress != secondEndpoint.IPAddress || !slices.Equal(retained.Aliases, secondEndpoint.Aliases) || retained.GwPriority != secondEndpoint.GwPriority {
			t.Fatal("endpoint operation changed the native process or retained bridge endpoint")
		}
	}
	link, err := ceph.InterruptNetwork(ctx, ctr, first.Name)
	if link != nil {
		// Registered last: restore before terminating the caller-owned process
		// and before removing either owned network, even after a partial failure.
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
			defer cleanupCancel()
			if err := link.Restore(cleanupCtx); err != nil {
				t.Errorf("restore regression endpoint during cleanup: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatalf("interrupt original bridge endpoint: %v", err)
	}
	if link == nil || link.ContainerID != ctr.GetContainerID() || link.NetworkName != first.Name || link.OriginalAddress != firstEndpoint.IPAddress {
		t.Fatal("interruption handle does not identify the exact original endpoint")
	}
	isolated := inspectContainer()
	if isolated.Container.NetworkSettings.Networks[first.Name] != nil {
		t.Fatal("interrupted bridge endpoint remains attached")
	}
	checkRetained(isolated)
	for _, phase := range []string{"restore", "idempotent restore"} {
		if err := link.Restore(ctx); err != nil {
			t.Fatalf("%s original bridge endpoint: %v", phase, err)
		}
		restored := inspectContainer()
		endpoint := restored.Container.NetworkSettings.Networks[first.Name]
		if endpoint == nil || endpoint.IPAddress != firstEndpoint.IPAddress || !slices.Equal(endpoint.Aliases, firstEndpoint.Aliases) || endpoint.GwPriority != firstEndpoint.GwPriority {
			t.Fatalf("%s changed the exact original IP, aliases or gateway priority", phase)
		}
		checkRetained(restored)
	}
	t.Log("two distinct Docker-allocated IPv4 bridges: endpoint restored twice at its original IP with identical aliases, gateway priority and native process; second endpoint preserved")
}
