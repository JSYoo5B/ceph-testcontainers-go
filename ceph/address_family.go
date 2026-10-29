package ceph

import (
	"fmt"
	"net/netip"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/dockerbridge"
)

const addressFamilyEnvironment = "CEPH_ADDRESS_FAMILY"

// AddressFamily selects the addresses that the Messenger of every daemon
// binds and advertises.
type AddressFamily int

const (
	// AddressIPv4 binds and advertises the containers' IPv4 addresses.
	AddressIPv4 AddressFamily = iota
	// AddressIPv6 runs the cluster on a dual-stack bridge and binds and
	// advertises only the containers' IPv6 addresses.
	AddressIPv6
	// AddressIPv4MappedIPv6 keeps an IPv4 bridge and binds and advertises the
	// containers' IPv4 addresses as IPv4-mapped IPv6 addresses
	// (::ffff:a.b.c.d) on IPv6 sockets.
	AddressIPv4MappedIPv6
	// AddressDualStack runs the cluster on a dual-stack bridge, and every
	// Messenger binds and advertises both the container's IPv4 and its IPv6
	// address, so that clients of either family can connect. MON address
	// vectors list the IPv6 address first; see NetworkConfig.WithAddressFamily.
	AddressDualStack
)

func (family AddressFamily) String() string {
	switch family {
	case AddressIPv4:
		return "ipv4"
	case AddressIPv6:
		return "ipv6"
	case AddressIPv4MappedIPv6:
		return "ipv4-mapped"
	case AddressDualStack:
		return "dual-stack"
	}
	return fmt.Sprintf("AddressFamily(%d)", int(family))
}

func (o options) bridgeFamily() dockerbridge.Family {
	if o.addressFamily == AddressIPv6 || o.addressFamily == AddressDualStack {
		return dockerbridge.DualStack
	}
	return dockerbridge.IPv4Only
}

// cephAddress formats ip as Ceph parses it. Ceph prints an IPv4-mapped IPv6
// address in dotted form but accepts only hexadecimal groups.
func cephAddress(ip netip.Addr) string {
	if ip.Is4In6() {
		v4 := ip.As16()
		return fmt.Sprintf("::ffff:%x:%x", uint16(v4[12])<<8|uint16(v4[13]), uint16(v4[14])<<8|uint16(v4[15]))
	}
	return ip.String()
}
