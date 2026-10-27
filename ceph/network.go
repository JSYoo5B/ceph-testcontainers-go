package ceph

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"

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
	return c.settings.clusterNetworkName(c.nets.public, c.nets.cluster)
}

// HasSeparateClusterNetwork reports whether OSDs have a dedicated second bridge.
func (c *Container) HasSeparateClusterNetwork() bool { return c.nets.cluster != nil }

// inspectNetworkSubnets returns the subnets of the public and cluster bridges
// that Ceph's public and cluster networks select, as Ceph network lists: the
// IPv6 subnets of the dual-stack bridges for AddressIPv6, both the IPv4 and
// the IPv6 subnet for AddressDualStack, the IPv4 subnets otherwise. With one
// bridge, public and cluster name the same network and both lists are equal.
func inspectNetworkSubnets(ctx context.Context, public, cluster string, family AddressFamily) (string, string, error) {
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return "", "", err
	}
	subnets := make([][]netip.Prefix, 2)
	for i, name := range []string{public, cluster} {
		inspection, inspectErr := docker.NetworkInspect(ctx, name, mobycl.NetworkInspectOptions{})
		if inspectErr != nil {
			err = fmt.Errorf("inspect Ceph network %s: %w", name, inspectErr)
			break
		}
		if subnets[i], err = familySubnets(inspection.Network.IPAM.Config, family); err != nil {
			err = fmt.Errorf("Ceph network %s: %w", name, err)
			break
		}
	}
	if err == nil && public != cluster && subnetsOverlap(subnets[0], subnets[1]) {
		err = errors.New("Ceph public and cluster subnets overlap")
	}
	err = errors.Join(err, docker.Close())
	if err != nil {
		return "", "", err
	}
	return networkList(subnets[0]), networkList(subnets[1]), nil
}

// familySubnets returns the subnets of a bridge's IPAM configuration that
// have the addresses Ceph binds for family: one subnet, or for
// AddressDualStack the IPv4 subnet followed by the IPv6 subnet.
func familySubnets(configs []dockernetwork.IPAMConfig, family AddressFamily) ([]netip.Prefix, error) {
	if family != AddressDualStack {
		subnet, err := familySubnet(configs, family)
		if err != nil {
			return nil, err
		}
		return []netip.Prefix{subnet}, nil
	}
	v4, err := familySubnet(configs, AddressIPv4)
	if err != nil {
		return nil, err
	}
	v6, err := familySubnet(configs, AddressIPv6)
	if err != nil {
		return nil, err
	}
	return []netip.Prefix{v4, v6}, nil
}

// familySubnet returns the first subnet of a bridge's IPAM configuration
// that has the addresses Ceph binds for a single-family family.
func familySubnet(configs []dockernetwork.IPAMConfig, family AddressFamily) (netip.Prefix, error) {
	ipv6 := family == AddressIPv6
	for _, config := range configs {
		if config.Subnet.IsValid() && config.Subnet.Addr().Is6() == ipv6 && !config.Subnet.Addr().Is4In6() {
			return config.Subnet.Masked(), nil
		}
	}
	if ipv6 {
		return netip.Prefix{}, errors.New("no IPv6 subnet")
	}
	return netip.Prefix{}, errors.New("no IPv4 subnet")
}

// subnetsOverlap reports whether a subnet of one plane overlaps a subnet of
// the other. Subnets of different families never overlap.
func subnetsOverlap(public, cluster []netip.Prefix) bool {
	for _, a := range public {
		for _, b := range cluster {
			if a.Overlaps(b) {
				return true
			}
		}
	}
	return false
}

// networkList formats subnets as a Ceph public_network or cluster_network
// value.
func networkList(subnets []netip.Prefix) string {
	values := make([]string, len(subnets))
	for i, subnet := range subnets {
		values[i] = subnet.String()
	}
	return strings.Join(values, ",")
}
