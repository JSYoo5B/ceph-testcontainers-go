package cluster

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
