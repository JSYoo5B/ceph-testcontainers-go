//go:build all || integration

package harness

import (
	"net"
	"net/netip"
	"testing"
)

// AddressIP returns the IP of a Ceph "ip:port" address.
func AddressIP(t *testing.T, address string) netip.Addr {
	t.Helper()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		t.Fatal(err)
	}
	return ip
}
