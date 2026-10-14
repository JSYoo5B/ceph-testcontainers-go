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
)

func (family AddressFamily) String() string {
	switch family {
	case AddressIPv4:
		return "ipv4"
	case AddressIPv6:
		return "ipv6"
	case AddressIPv4MappedIPv6:
		return "ipv4-mapped"
	}
	return fmt.Sprintf("AddressFamily(%d)", int(family))
}

// WithAddressFamily selects the addresses of the Messenger listeners of every
// MON, MGR, OSD and service daemon. AddressIPv6 and AddressIPv4MappedIPv6
// need bridge mode, and AddressIPv6 a Docker Engine that can allocate IPv6
// subnets. With WithSeparateClusterNetwork, AddressIPv6 makes both bridges
// dual-stack and their IPv6 subnets Ceph's public and cluster networks;
// AddressIPv4MappedIPv6 cannot be combined with it.
func WithAddressFamily(family AddressFamily) Option {
	return func(o *options) error {
		if family != AddressIPv4 && family != AddressIPv6 && family != AddressIPv4MappedIPv6 {
			return fmt.Errorf("unknown address family %d", family)
		}
		o.addressFamily = family
		return nil
	}
}

// AddressFamily returns the family that the cluster was created with. A nil
// Container reports AddressIPv4.
func (c *Container) AddressFamily() AddressFamily {
	if c == nil {
		return AddressIPv4
	}
	return c.settings.addressFamily
}

func (o options) bridgeFamily() dockerbridge.Family {
	if o.addressFamily == AddressIPv6 {
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
