package ceph

import (
	"net/netip"
	"testing"

	dockernetwork "github.com/moby/moby/api/types/network"
)

func TestSeparateClusterNetworkRejectsHostBeforeCreation(t *testing.T) {
	cluster, err := Run(t.Context(), RunOptions{}.WithOptions(WithSeparateClusterNetwork(), WithHostNetwork()))
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
