package ceph

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func TestSeparateClusterNetworkRejectsHostBeforeCreation(t *testing.T) {
	cluster, err := Run(t.Context(), DefaultImage, WithSeparateClusterNetwork(), WithHostNetwork())
	if cluster != nil || err == nil {
		t.Fatal("incompatible traffic planes allocated resources")
	}
}

func TestFamilySubnetSelectsThePlaneOfTheAddressFamily(t *testing.T) {
	ipv4, ipv6 := netip.MustParsePrefix("172.20.0.0/16"), netip.MustParsePrefix("fd5e:1c4f:9a7b::/64")
	dualStack := []dockernetwork.IPAMConfig{{Subnet: ipv4}, {Subnet: ipv6}}
	for family, want := range map[AddressFamily]netip.Prefix{AddressIPv4: ipv4, AddressIPv6: ipv6} {
		if got, err := familySubnet(dualStack, family); err != nil || got != want {
			t.Errorf("%s: %v %v, want %v", family, got, err, want)
		}
	}
	// Docker lists the IPv6 configuration first on some engines.
	if got, err := familySubnet([]dockernetwork.IPAMConfig{{Subnet: ipv6}, {Subnet: ipv4}}, AddressIPv4); err != nil || got != ipv4 {
		t.Errorf("IPv4 after IPv6: %v %v", got, err)
	}
	if _, err := familySubnet([]dockernetwork.IPAMConfig{{Subnet: ipv4}}, AddressIPv6); err == nil {
		t.Error("an IPv4-only bridge gave an IPv6 subnet")
	}
	if _, err := familySubnet([]dockernetwork.IPAMConfig{{Subnet: netip.MustParsePrefix("::ffff:172.20.0.0/112")}}, AddressIPv6); err == nil {
		t.Error("an IPv4-mapped prefix was taken for an IPv6 subnet")
	}
}

type linkDockerFake struct {
	endpoint                         *dockernetwork.EndpointSettings
	inspectErr, connectErr, closeErr error
	connects, closes                 int
	applyOnError                     bool
	missingAfterConnect              bool
}

func (d *linkDockerFake) ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	if d.inspectErr != nil {
		return mobycl.ContainerInspectResult{}, d.inspectErr
	}
	networks := map[string]*dockernetwork.EndpointSettings{}
	if d.endpoint != nil {
		networks["cluster-network"] = d.endpoint
	}
	return mobycl.ContainerInspectResult{Container: container.InspectResponse{NetworkSettings: &container.NetworkSettings{Networks: networks}}}, nil
}

func (d *linkDockerFake) NetworkDisconnect(context.Context, string, mobycl.NetworkDisconnectOptions) (mobycl.NetworkDisconnectResult, error) {
	panic("not used by restoration tests")
}

func (d *linkDockerFake) NetworkConnect(_ context.Context, _ string, opts mobycl.NetworkConnectOptions) (mobycl.NetworkConnectResult, error) {
	d.connects++
	if d.missingAfterConnect {
		d.inspectErr = errdefs.ErrNotFound
	}
	if d.connectErr == nil || d.applyOnError {
		d.endpoint = opts.EndpointConfig.Copy()
		d.endpoint.IPAddress = opts.EndpointConfig.IPAMConfig.IPv4Address
		d.endpoint.GlobalIPv6Address = opts.EndpointConfig.IPAMConfig.IPv6Address
	}
	return mobycl.NetworkConnectResult{}, d.connectErr
}

func (d *linkDockerFake) Close() error { d.closes++; return d.closeErr }

func newLinkFake() (*NetworkInterruption, *linkDockerFake) {
	ip := netip.MustParseAddr("172.22.0.8")
	original := &dockernetwork.EndpointSettings{IPAddress: ip, Aliases: []string{"osd-0"}, NetworkID: "old-network-id", EndpointID: "old-endpoint-id", Gateway: netip.MustParseAddr("172.22.0.1"), GwPriority: 5}
	d := &linkDockerFake{}
	return &NetworkInterruption{ContainerID: "osd-0", NetworkName: "cluster-network", OriginalAddress: ip, endpoint: restoreEndpoint(original), docker: d}, d
}

// newDualStackLinkFake is newLinkFake for an endpoint of a dual-stack bridge.
func newDualStackLinkFake() (*NetworkInterruption, *linkDockerFake) {
	ip := netip.MustParseAddr("172.22.0.8")
	original := &dockernetwork.EndpointSettings{IPAddress: ip, GlobalIPv6Address: netip.MustParseAddr("fd62:93fe:2c9e::8"), Aliases: []string{"osd-0"}, GwPriority: 5}
	d := &linkDockerFake{}
	return &NetworkInterruption{ContainerID: "osd-0", NetworkName: "cluster-network", OriginalAddress: ip, endpoint: restoreEndpoint(original), docker: d}, d
}

func TestInterruptedDualStackEndpointRestoresBothAddresses(t *testing.T) {
	link, d := newDualStackLinkFake()
	if err := link.Restore(t.Context()); err != nil || !link.complete {
		t.Fatal(err)
	}
	if d.endpoint.IPAddress != link.OriginalAddress || d.endpoint.GlobalIPv6Address != netip.MustParseAddr("fd62:93fe:2c9e::8") {
		t.Fatalf("restored endpoint lost an advertised address: %+v", d.endpoint)
	}
}

// An external reattachment with the original IPv4 address, aliases and
// gateway priority but another or no IPv6 address is refused, and the handle
// can restore once the endpoint is back at its original addresses.
func TestInterruptedDualStackEndpointRefusesChangedIPv6(t *testing.T) {
	link, d := newDualStackLinkFake()
	for _, ipv6 := range []netip.Addr{netip.MustParseAddr("fd62:93fe:2c9e::9"), {}} {
		d.endpoint = &dockernetwork.EndpointSettings{IPAddress: link.OriginalAddress, GlobalIPv6Address: ipv6, Aliases: []string{"osd-0"}, GwPriority: 5}
		if err := link.Restore(t.Context()); err == nil || link.complete || d.connects != 0 || d.closes != 0 || d.endpoint.GlobalIPv6Address != ipv6 {
			t.Fatalf("accepted or overwrote an endpoint with IPv6 %v: %v", ipv6, err)
		}
	}
	d.endpoint.GlobalIPv6Address = netip.MustParseAddr("fd62:93fe:2c9e::8")
	if err := link.Restore(t.Context()); err != nil || !link.complete || d.connects != 0 || d.closes != 1 {
		t.Fatalf("restoring the original addresses failed: %v", err)
	}
}

func TestInterruptedEndpointRestoresAdvertisedIdentity(t *testing.T) {
	link, d := newLinkFake()
	if err := link.Restore(t.Context()); err != nil {
		t.Fatal(err)
	}
	if d.endpoint.IPAddress != link.OriginalAddress || d.endpoint.Aliases[0] != "osd-0" || d.endpoint.GwPriority != 5 || d.endpoint.NetworkID != "" || d.endpoint.EndpointID != "" || d.endpoint.Gateway.IsValid() {
		t.Fatalf("restoration changed advertised address/aliases or reused operational state: %+v", d.endpoint)
	}
	if err := link.Restore(t.Context()); err != nil || d.connects != 1 || d.closes != 1 {
		t.Fatal("completed endpoint restoration was repeated")
	}
}

func TestInterruptedEndpointReconcilesUncertainReconnectAndClose(t *testing.T) {
	link, d := newLinkFake()
	d.connectErr, d.applyOnError = errors.New("transport lost response"), true
	if err := link.Restore(t.Context()); err == nil || d.closes != 0 {
		t.Fatal("uncertain reconnect lost retryable ownership")
	}
	d.connectErr, d.closeErr = nil, errors.New("close failed")
	if err := link.Restore(t.Context()); err == nil || d.connects != 1 {
		t.Fatal("retry reconnected the already restored endpoint")
	}
	d.closeErr = nil
	if err := link.Restore(t.Context()); err != nil || !link.complete || d.connects != 1 || d.closes != 2 {
		t.Fatalf("close retry: %v", err)
	}
}

func TestInterruptedEndpointPreservesExternalReplacementAndMissingContainer(t *testing.T) {
	link, d := newLinkFake()
	d.endpoint = &dockernetwork.EndpointSettings{IPAddress: netip.MustParseAddr("172.22.0.9")}
	if err := link.Restore(t.Context()); err == nil || d.connects != 0 || d.closes != 0 {
		t.Fatal("overwrote an externally replaced endpoint")
	}
	d.inspectErr = errdefs.ErrNotFound
	if err := link.Restore(t.Context()); err != nil || !link.complete || d.connects != 0 || d.closes != 1 {
		t.Fatalf("removed container cleanup: %v", err)
	}
}

func TestInterruptedEndpointDoesNotMistakeMissingNetworkForRemovedContainer(t *testing.T) {
	link, d := newLinkFake()
	d.connectErr = errdefs.ErrNotFound
	if err := link.Restore(t.Context()); err == nil || link.complete || d.closes != 0 {
		t.Fatal("missing network discarded restoration of a live caller container")
	}
	d.missingAfterConnect = true
	if err := link.Restore(t.Context()); err != nil || !link.complete || d.closes != 1 {
		t.Fatalf("container removed during reconnect could not be cleaned up: %v", err)
	}
}

func TestInterruptedEndpointRequiresOriginalPeerAliases(t *testing.T) {
	link, d := newLinkFake()
	d.endpoint = &dockernetwork.EndpointSettings{IPAddress: link.OriginalAddress, GwPriority: 5}
	if err := link.Restore(t.Context()); err == nil || link.complete || d.closes != 0 {
		t.Fatal("accepted a reattached IP without the original peer DNS identity")
	}
}

func TestClusterCleanupRetainsNetworkAfterCallerRestoreFailure(t *testing.T) {
	link, d := newLinkFake()
	d.connectErr = errors.New("temporary reconnect failure")
	cluster := &Container{network: &testcontainers.DockerNetwork{Name: link.NetworkName}, interruptions: map[string]*NetworkInterruption{"client": link}}
	if err := cluster.Terminate(t.Context()); err == nil || cluster.networkRemoved || link.complete || d.closes != 0 {
		t.Fatal("cleanup removed the original network needed to restore a caller-owned client")
	}
}
