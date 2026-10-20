package dockerbridge

import (
	"net/netip"

	dockernetwork "github.com/moby/moby/api/types/network"
)

// RestoreEndpoint copies the advertised addresses and requested aliases for reconnection.
func RestoreEndpoint(original *dockernetwork.EndpointSettings) *dockernetwork.EndpointSettings {
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
