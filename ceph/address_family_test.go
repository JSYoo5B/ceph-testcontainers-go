package ceph

import (
	"context"
	"encoding/json"
	"net/netip"
	"strings"
	"testing"
)

func TestCephAddressFormatsMappedAddressesInHex(t *testing.T) {
	for input, want := range map[string]string{
		"::ffff:172.19.0.2": "::ffff:ac13:2",
		"::ffff:10.0.255.1": "::ffff:a00:ff01",
		"fd62:93fe::2":      "fd62:93fe::2",
		"172.19.0.2":        "172.19.0.2",
	} {
		if got := cephAddress(netip.MustParseAddr(input)); got != want {
			t.Errorf("%s: %s, want %s", input, got, want)
		}
	}
}

func TestMonitorBootstrapAddressesKeepAddressFamilies(t *testing.T) {
	raw := `{"quorum_names":["a","b"],"monmap":{"mons":[
		{"name":"a","public_addrs":{"addrvec":[{"type":"v2","addr":"[::ffff:172.19.0.2]:3300","nonce":0},{"type":"v1","addr":"[::ffff:172.19.0.2]:6789","nonce":0}]}},
		{"name":"b","public_addrs":{"addrvec":[{"type":"v2","addr":"[fd62:93fe:2c9e::4]:3300/0","nonce":0}]}}]}}`
	var status QuorumStatus
	if err := json.Unmarshal([]byte(raw), &status); err != nil {
		t.Fatal(err)
	}
	addresses, err := monitorBootstrapAddresses(status)
	want := "[v2:[::ffff:ac13:2]:3300,v1:[::ffff:ac13:2]:6789] [v2:[fd62:93fe:2c9e::4]:3300]"
	if err != nil || addresses != want {
		t.Fatalf("addresses %q, %v", addresses, err)
	}
}

func TestAddressFamilyOptions(t *testing.T) {
	if err := WithAddressFamily(AddressFamily(7))(&options{}); err == nil {
		t.Fatal("unknown family accepted")
	}
	var settings options
	if err := WithAddressFamily(AddressIPv6)(&settings); err != nil || settings.addressFamily != AddressIPv6 || settings.bridgeFamily() == 0 {
		t.Fatalf("IPv6: %v %+v", err, settings.addressFamily)
	}
	if (*Container)(nil).AddressFamily() != AddressIPv4 || AddressIPv4MappedIPv6.String() != "ipv4-mapped" {
		t.Fatal("defaults or names changed")
	}
	for name, opts := range map[string][]Option{
		"host network":                    {WithAddressFamily(AddressIPv6), WithHostNetwork()},
		"dual-stack host network":         {WithAddressFamily(AddressDualStack), WithHostNetwork()},
		"mapped separate cluster network": {WithAddressFamily(AddressIPv4MappedIPv6), WithSeparateClusterNetwork()},
	} {
		cluster, err := Run(context.Background(), DefaultImage, opts[0], opts[1])
		if err == nil || cluster != nil || !strings.Contains(err.Error(), "WithAddressFamily") {
			t.Fatalf("%s: Run accepted the combination: %v", name, err)
		}
	}
}
