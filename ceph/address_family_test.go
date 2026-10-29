package ceph

import (
	"context"
	"encoding/json"
	"errors"
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

func TestDualStackOrderConfigRequiresDualStack(t *testing.T) {
	path := writeUserConfig(t, "[osd.1]\nms bind prefer ipv4 = false\n[mgr.b]\nms-bind-prefer-ipv4 = false\n")
	var settings options
	if err := WithConfigFile(path)(&settings); err != nil {
		t.Fatalf("WithConfigFile rejected ms_bind_prefer_ipv4: %v", err)
	}
	if !userConfigSets(settings.userConfig, dualStackOrderKey) || userConfigSets(settings.userConfig, "ms_bind_ipv4") {
		t.Fatalf("parsed sections %+v", settings.userConfig)
	}
	if got := string(renderUserConfig(settings.userConfig)); got != "[osd.1]\nms_bind_prefer_ipv4 = false\n[mgr.b]\nms_bind_prefer_ipv4 = false\n" {
		t.Fatalf("rendered %q", got)
	}
	for _, key := range []string{"ms_bind_ipv4", "ms_bind_ipv6", "ms_bind_msgr1", "ms_bind_msgr2"} {
		if err := WithConfigFile(writeUserConfig(t, "[osd.1]\n"+key+" = true\n"))(&options{}); err == nil {
			t.Fatalf("WithConfigFile accepted %s", key)
		}
	}
	for _, family := range []AddressFamily{AddressIPv4, AddressIPv6, AddressIPv4MappedIPv6} {
		cluster, err := Run(context.Background(), DefaultImage, WithNetworkConfig(NetworkConfig{}.WithAddressFamily(family)), WithConfigFile(path))
		if cluster != nil || !errors.Is(err, ErrInvalidRequest) || !strings.Contains(err.Error(), "ms_bind_prefer_ipv4, which requires NetworkConfig.WithAddressFamily(dual-stack)") {
			t.Fatalf("%s: Run accepted ms_bind_prefer_ipv4: %v", family, err)
		}
	}
}

func TestAddressFamilyOptions(t *testing.T) {
	if err := WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressFamily(7)))(&options{}); err == nil {
		t.Fatal("unknown family accepted")
	}
	var settings options
	if err := WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv6))(&settings); err != nil || settings.addressFamily != AddressIPv6 || settings.bridgeFamily() == 0 {
		t.Fatalf("IPv6: %v %+v", err, settings.addressFamily)
	}
	if AddressIPv4MappedIPv6.String() != "ipv4-mapped" {
		t.Fatal("names changed")
	}
	for name, opts := range map[string][]Option{
		"host network":                    {WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv6)), WithHostNetwork()},
		"dual-stack host network":         {WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressDualStack)), WithHostNetwork()},
		"mapped separate cluster network": {WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv4MappedIPv6)), WithNetworkConfig(NetworkConfig{}.WithSeparateClusterNetwork())},
	} {
		cluster, err := Run(context.Background(), DefaultImage, opts[0], opts[1])
		if err == nil || cluster != nil || !strings.Contains(err.Error(), "NetworkConfig.WithAddressFamily") {
			t.Fatalf("%s: Run accepted the combination: %v", name, err)
		}
	}
}
