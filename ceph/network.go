package ceph

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sync"

	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// NetworkPlane selects the client/public or OSD replication network.
type NetworkPlane string

const (
	PublicNetworkPlane  NetworkPlane = "public"
	ClusterNetworkPlane NetworkPlane = "cluster"
)

// ClusterNetworkName returns the replication bridge. With a single bridge it
// returns NetworkName; in host mode both traffic planes share "host".
func (c *Container) ClusterNetworkName() string {
	if c.clusterNetwork != nil {
		return c.clusterNetwork.Name
	}
	return c.NetworkName()
}

// HasSeparateClusterNetwork reports whether OSDs have a dedicated second bridge.
func (c *Container) HasSeparateClusterNetwork() bool { return c.clusterNetwork != nil }

func inspectNetworkSubnets(ctx context.Context, public, cluster string) (string, string, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return "", "", err
	}
	subnets := make([]netip.Prefix, 2)
	for i, name := range []string{public, cluster} {
		inspection, inspectErr := docker.NetworkInspect(ctx, name, mobycl.NetworkInspectOptions{})
		if inspectErr != nil {
			err = fmt.Errorf("inspect Ceph network %s: %w", name, inspectErr)
			break
		}
		for _, config := range inspection.Network.IPAM.Config {
			if config.Subnet.IsValid() && config.Subnet.Addr().Is4() {
				subnets[i] = config.Subnet.Masked()
				break
			}
		}
		if !subnets[i].IsValid() {
			err = fmt.Errorf("Ceph network %s has no IPv4 subnet", name)
			break
		}
	}
	if err == nil && subnets[0].Overlaps(subnets[1]) {
		err = errors.New("Ceph public and cluster subnets overlap")
	}
	err = errors.Join(err, docker.Close())
	if err != nil {
		return "", "", err
	}
	return subnets[0].String(), subnets[1].String(), nil
}

type networkLinkClient interface {
	ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error)
	NetworkDisconnect(context.Context, string, mobycl.NetworkDisconnectOptions) (mobycl.NetworkDisconnectResult, error)
	NetworkConnect(context.Context, string, mobycl.NetworkConnectOptions) (mobycl.NetworkConnectResult, error)
	Close() error
}

// NetworkInterruption owns one disconnected Docker endpoint. Restore reconnects
// its original addresses and aliases without restarting the container or Ceph
// daemon. It is an endpoint-to-network interruption, not a pairwise packet filter.
// A non-nil handle returned with an error must be restored: transport failure
// cannot prove that Docker did not disconnect it. Restore is retryable.
type NetworkInterruption struct {
	ContainerID, NetworkName string
	OriginalAddress          netip.Addr
	mu                       sync.Mutex
	docker                   networkLinkClient
	endpoint                 *dockernetwork.EndpointSettings
	complete                 bool
}

// InterruptNetwork disconnects a container from one of this cluster's bridges.
// The container may be an owned daemon or a caller-owned WithClient container.
// With a single bridge, both planes select that same endpoint. Host mode is
// rejected because Docker cannot disconnect a host-network endpoint.
// The fixture retains the handle and attempts restoration during Terminate.
// Restore explicitly before destroying caller-owned containers or the cluster.
func (c *Container) InterruptNetwork(ctx context.Context, ctr testcontainers.Container, plane NetworkPlane) (*NetworkInterruption, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.UsesHostNetwork() || ctr == nil || ctr.GetContainerID() == "" {
		return nil, errors.New("network interruption requires a live bridge cluster and an attached container")
	}
	var name string
	switch plane {
	case PublicNetworkPlane:
		name = c.NetworkName()
	case ClusterNetworkPlane:
		name = c.ClusterNetworkName()
	default:
		return nil, errors.New("invalid Ceph network plane")
	}
	if name == "" {
		return nil, errors.New("Ceph bridge is not initialized")
	}
	id := ctr.GetContainerID()
	key := name + "/" + id
	if previous := c.interruptions[key]; previous != nil && !previous.isComplete() {
		return previous, errors.New("restore the existing endpoint interruption first")
	}
	link, err := InterruptNetwork(ctx, ctr, name)
	if link != nil {
		if c.interruptions == nil {
			c.interruptions = make(map[string]*NetworkInterruption)
		}
		c.interruptions[key] = link
	}
	return link, err
}

// InterruptNetwork disconnects one existing bridge endpoint. The caller owns
// the container and the returned restoration handle; this function does not
// stop a daemon, change Ceph policies or remove the network. Fixture-specific
// helpers also register restoration with their cleanup. Host networking cannot
// be interrupted through Docker endpoint operations.
func InterruptNetwork(ctx context.Context, ctr testcontainers.Container, name string) (*NetworkInterruption, error) {
	if ctr == nil || ctr.GetContainerID() == "" || name == "" || name == "host" || name == "none" || name == "bridge" {
		return nil, errors.New("network interruption requires a container and a named bridge")
	}
	id := ctr.GetContainerID()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return nil, err
	}
	inspection, err := docker.ContainerInspect(ctx, id, mobycl.ContainerInspectOptions{})
	if err != nil {
		return nil, errors.Join(err, docker.Close())
	}
	var endpoint *dockernetwork.EndpointSettings
	if inspection.Container.NetworkSettings != nil {
		endpoint = inspection.Container.NetworkSettings.Networks[name]
	}
	if endpoint == nil || !endpoint.IPAddress.IsValid() || !endpoint.IPAddress.Is4() {
		return nil, errors.Join(fmt.Errorf("container is not attached to Ceph network %s with IPv4", name), docker.Close())
	}
	link := &NetworkInterruption{ContainerID: id, NetworkName: name, OriginalAddress: endpoint.IPAddress, docker: docker, endpoint: restoreEndpoint(endpoint)}
	// Return ownership even after uncertain transport outcomes.
	_, err = docker.NetworkDisconnect(ctx, name, mobycl.NetworkDisconnectOptions{Container: id})
	if err != nil {
		return link, fmt.Errorf("disconnect Ceph network endpoint: %w", err)
	}
	return link, nil
}

func restoreEndpoint(original *dockernetwork.EndpointSettings) *dockernetwork.EndpointSettings {
	endpoint := original.Copy()
	endpoint.NetworkID, endpoint.EndpointID = "", ""
	endpoint.Gateway, endpoint.IPv6Gateway = netip.Addr{}, netip.Addr{}
	endpoint.IPPrefixLen, endpoint.GlobalIPv6PrefixLen = 0, 0
	endpoint.DNSNames = nil
	if endpoint.IPAMConfig == nil {
		endpoint.IPAMConfig = &dockernetwork.EndpointIPAMConfig{}
	}
	endpoint.IPAMConfig.IPv4Address = original.IPAddress
	if original.GlobalIPv6Address.IsValid() {
		endpoint.IPAMConfig.IPv6Address = original.GlobalIPv6Address
	}
	endpoint.IPAddress, endpoint.GlobalIPv6Address = netip.Addr{}, netip.Addr{}
	return endpoint
}

func (link *NetworkInterruption) isComplete() bool {
	link.mu.Lock()
	defer link.mu.Unlock()
	return link.complete
}

// Restore reattaches the original endpoint, preserving the IP advertised by
// Ceph. Removed containers need no restoration. An externally reattached
// endpoint with a different address is refused rather than overwritten.
func (link *NetworkInterruption) Restore(ctx context.Context) error {
	link.mu.Lock()
	defer link.mu.Unlock()
	if link.complete {
		return nil
	}
	inspection, err := link.docker.ContainerInspect(ctx, link.ContainerID, mobycl.ContainerInspectOptions{})
	if err != nil && !onlyMissingHostResource(err) {
		return fmt.Errorf("inspect interrupted endpoint: %w", err)
	}
	if err == nil {
		var current *dockernetwork.EndpointSettings
		if inspection.Container.NetworkSettings != nil {
			current = inspection.Container.NetworkSettings.Networks[link.NetworkName]
		}
		if current != nil {
			if current.IPAddress != link.endpoint.IPAMConfig.IPv4Address || current.GwPriority != link.endpoint.GwPriority {
				return errors.New("interrupted endpoint was externally reattached with a different address or gateway priority")
			}
			for _, alias := range link.endpoint.Aliases {
				if !slices.Contains(current.Aliases, alias) {
					return errors.New("interrupted endpoint was externally reattached without its original aliases")
				}
			}
		} else {
			if _, err := link.docker.NetworkConnect(ctx, link.NetworkName, mobycl.NetworkConnectOptions{Container: link.ContainerID, EndpointConfig: link.endpoint}); err != nil {
				// A missing network is not a restored live caller container. Only
				// a container removed during reconnect needs no restoration.
				if !onlyMissingHostResource(err) {
					return fmt.Errorf("restore Ceph network endpoint: %w", err)
				}
				if _, inspectErr := link.docker.ContainerInspect(ctx, link.ContainerID, mobycl.ContainerInspectOptions{}); inspectErr == nil || !onlyMissingHostResource(inspectErr) {
					return fmt.Errorf("restore Ceph network endpoint: %w", errors.Join(err, inspectErr))
				}
			}
		}
	}
	if err := link.docker.Close(); err != nil {
		return fmt.Errorf("close interruption Docker client: %w", err)
	}
	link.complete = true
	return nil
}
