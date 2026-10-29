package ceph

import (
	"fmt"
	"net"
	"slices"
)

// networkField names one setting of NetworkConfig.
type networkField uint8

const (
	networkAddressFamily networkField = 1 << iota
	networkSeparateCluster
	networkHostAddress
	networkMessengerMode
	networkMessengerPolicy
)

// NetworkConfig is the network and Messenger group of the Ceph configuration:
// which addresses the daemons bind and advertise, whether OSD replication
// traffic uses its own bridge, the host address in host-network mode and the
// Messenger protocol and connection-mode policy. It is a plain value with
// chainable, value-returning methods, not a customizer; pass it to Run through
// its one grouped option function, WithNetworkConfig. The zero value changes
// nothing, and each field records whether it was set, so an explicit setting
// (including the default family AddressIPv4 or MessengerDefault) is different
// from an unspecified one.
//
//	network := ceph.NetworkConfig{}.WithAddressFamily(ceph.AddressDualStack).WithSeparateClusterNetwork()
//	cluster, err := ceph.Run(ctx, ceph.DefaultImage, ceph.WithNetworkConfig(network))
//
// Within one value a later call of the same method replaces the earlier one.
// Run refuses a field that two WithNetworkConfig options both set, even when
// they agree, so argument order never decides a conflict. Incompatible
// combinations (host network with a separate cluster network or a non-IPv4
// family, a host address without WithHostNetwork, a Messenger policy with
// MessengerV2Secure) are refused before any resource is created. Docker
// execution policy such as WithHostNetwork stays a separate option function.
// The composition rules are in docs/design/value-configuration-composition.md.
type NetworkConfig struct {
	set             networkField
	addressFamily   AddressFamily
	hostAddress     string
	messengerMode   MessengerMode
	messengerPolicy MessengerPolicy
}

// WithAddressFamily selects the addresses of the Messenger listeners of every
// MON, MGR, OSD and service daemon. Every family other than AddressIPv4 needs
// bridge mode, and AddressIPv6 and AddressDualStack a Docker Engine that can
// allocate IPv6 subnets. With WithSeparateClusterNetwork, AddressIPv6 makes
// both bridges dual-stack and their IPv6 subnets Ceph's public and cluster
// networks, and AddressDualStack makes both subnets of each bridge its
// network; AddressIPv4MappedIPv6 cannot be combined with it.
//
// AddressDualStack sets ms_bind_ipv4, ms_bind_ipv6 and ms_bind_prefer_ipv4,
// and public_network to both subnets of the bridge. Every address vector has
// an entry of each family for each enabled Messenger protocol: v2, and v1
// unless WithMessengerMode removes it. MON vectors list the IPv6 address
// first, which keeps a MGR from advertising the IPv6 wildcard address [::]
// that clients cannot reach; the other daemons list the IPv4 address first.
// Native clients connect to the first entry, so they reach MGRs, OSDs and
// MDSs over IPv4; ms_bind_prefer_ipv4 = false in a daemon's section of a
// WithConfigFile file makes that daemon list its IPv6 address first.
// Run, AddManager and ManagerContainer.Remove check that the MONs, the active MGR and
// the up OSDs advertise a concrete address of each family, and fail with
// ErrUnsupported otherwise. An unknown family is refused.
func (n NetworkConfig) WithAddressFamily(family AddressFamily) NetworkConfig {
	n.set |= networkAddressFamily
	n.addressFamily = family
	return n
}

// WithSeparateClusterNetwork creates a second owned bridge for OSD replication,
// recovery and heartbeat traffic. MON/MGR/MDS/RGW and WithClient use only the
// public bridge. Docker selects both subnets; with WithAddressFamily(AddressIPv6)
// both bridges are dual-stack and Ceph uses their IPv6 subnets. This setting
// requires bridge mode.
func (n NetworkConfig) WithSeparateClusterNetwork() NetworkConfig {
	n.set |= networkSeparateCluster
	return n
}

// WithHostAddress selects a local IPv4 address to bind and advertise in host
// mode, typically the Docker host's address when Docker is remote. The address
// must be assigned on the Docker daemon host and reachable from the test
// process; Run verifies both. Requires WithHostNetwork. An empty, unspecified,
// multicast, broadcast or non-IPv4 address is refused.
func (n NetworkConfig) WithHostAddress(address string) NetworkConfig {
	n.set |= networkHostAddress
	n.hostAddress = address
	return n
}

// WithMessengerMode selects the Messenger policy at bootstrap. MessengerDefault
// retains Ceph's image defaults and v1/v2 addresses. MessengerV2Secure requires
// encrypted v2 connections without CRC-mode or v1 fallback, including later
// daemons and WithClient containers. MessengerV2Only removes the v1 listeners
// and addresses in the same way but leaves the connection modes to Ceph's
// defaults and the Messenger policy. This setting does not enable RGW HTTP TLS.
// An unknown mode is refused.
func (n NetworkConfig) WithMessengerMode(mode MessengerMode) NetworkConfig {
	n.set |= networkMessengerMode
	n.messengerMode = mode
	return n
}

// WithMessengerPolicy selects the connection modes that MONs, MGRs and OSDs
// accept from clients. Run stores the policy in the central configuration
// right after the first MON starts, before any other daemon, so every daemon
// starts with it. override.SetMessengerPolicy changes it on a running cluster.
// It cannot be combined with MessengerV2Secure, whose local configuration
// fixes every mode. The value keeps its own copy of the lists, so changing the
// policy's slices afterwards does not change it; a nil list keeps Ceph's
// default and a non-nil empty list is refused.
func (n NetworkConfig) WithMessengerPolicy(policy MessengerPolicy) NetworkConfig {
	n.set |= networkMessengerPolicy
	n.messengerPolicy = MessengerPolicy{Monitors: slices.Clone(policy.Monitors), Managers: slices.Clone(policy.Managers), OSDs: slices.Clone(policy.OSDs)}
	return n
}

// WithNetworkConfig is the grouped option function that carries a NetworkConfig
// to Run. Like every Option it changes the cluster settings only and never
// reaches a container request.
func WithNetworkConfig(network NetworkConfig) Option {
	return func(o *options) error { return network.apply(o) }
}

// apply validates the fields n set and, only if all are valid, writes them to
// the settings. It refuses a field that an earlier WithNetworkConfig already set.
func (n NetworkConfig) apply(o *options) error {
	if clash := n.set & o.networkSet; clash != 0 {
		return fmt.Errorf("network %s set by more than one WithNetworkConfig", networkFieldName(clash))
	}
	var publicAddress string
	if n.set&networkAddressFamily != 0 {
		f := n.addressFamily
		if f != AddressIPv4 && f != AddressIPv6 && f != AddressIPv4MappedIPv6 && f != AddressDualStack {
			return fmt.Errorf("unknown address family %d", f)
		}
	}
	if n.set&networkHostAddress != 0 {
		ip := net.ParseIP(n.hostAddress)
		if ip == nil || ip.To4() == nil || ip.IsUnspecified() || ip.IsMulticast() || ip.Equal(net.IPv4bcast) {
			return fmt.Errorf("host address must be a specific IPv4 address")
		}
		publicAddress = ip.String()
	}
	if n.set&networkMessengerMode != 0 {
		if m := n.messengerMode; m != MessengerDefault && m != MessengerV2Secure && m != MessengerV2Only {
			return fmt.Errorf("unsupported Messenger mode %d", m)
		}
	}
	if n.set&networkMessengerPolicy != 0 {
		if _, err := n.messengerPolicy.settings(); err != nil {
			return fmt.Errorf("messenger policy: %w", err)
		}
	}
	if n.set&networkAddressFamily != 0 {
		o.addressFamily = n.addressFamily
	}
	if n.set&networkSeparateCluster != 0 {
		o.separateClusterNetwork = true
	}
	if n.set&networkHostAddress != 0 {
		o.publicAddress, o.hostAddressSet = publicAddress, true
	}
	if n.set&networkMessengerMode != 0 {
		o.messengerMode = n.messengerMode
	}
	if n.set&networkMessengerPolicy != 0 {
		p := n.messengerPolicy
		o.messengerPolicy = MessengerPolicy{Monitors: slices.Clone(p.Monitors), Managers: slices.Clone(p.Managers), OSDs: slices.Clone(p.OSDs)}
	}
	o.networkSet |= n.set
	return nil
}

func networkFieldName(f networkField) string {
	switch {
	case f&networkAddressFamily != 0:
		return "address family"
	case f&networkSeparateCluster != 0:
		return "separate cluster network"
	case f&networkHostAddress != 0:
		return "host address"
	case f&networkMessengerMode != 0:
		return "Messenger mode"
	default:
		return "Messenger policy"
	}
}
