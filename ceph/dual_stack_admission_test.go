package ceph

import (
	"net/netip"
	"strings"
	"testing"

	dockernetwork "github.com/moby/moby/api/types/network"
)

// dualStackFixture is the MGRMap, monmap and OSDMap content of a healthy
// dual-stack cluster, as DaemonAddresses decodes it.
func dualStackFixture() DaemonAddresses {
	pair := func(v4, v6 string, nonce uint32) []EntityAddress {
		return []EntityAddress{{"v2", v4, nonce}, {"v2", v6, nonce}}
	}
	return DaemonAddresses{
		Monitors: []DaemonAddress{{Name: "a", Addrs: []EntityAddress{
			{"v2", "[fd62:93fe:2c9e::2]:3300", 0}, {"v1", "[fd62:93fe:2c9e::2]:6789", 0},
			{"v2", "172.19.0.2:3300", 0}, {"v1", "172.19.0.2:6789", 0}}}},
		ActiveManager:    &DaemonAddress{Name: "a", Addrs: pair("172.19.0.3:6800", "[fd62:93fe:2c9e::3]:6801", 7)},
		ManagerAvailable: true,
		OSDs: []OSDAddress{{ID: 0, Up: true, In: true,
			Public: pair("172.19.0.4:6800", "[fd62:93fe:2c9e::4]:6801", 9), Cluster: pair("172.19.0.4:6802", "[fd62:93fe:2c9e::4]:6803", 9),
			HeartbeatFront: pair("172.19.0.4:6804", "[fd62:93fe:2c9e::4]:6805", 9), HeartbeatBack: pair("172.19.0.4:6806", "[fd62:93fe:2c9e::4]:6807", 9)}},
	}
}

func TestDualStackAdmissionAcceptsConcreteAddresses(t *testing.T) {
	if err := dualStackAddressProblem(dualStackFixture()); err != nil {
		t.Fatal(err)
	}
	// A MGR that is not available yet and a down OSD's stale vectors are not
	// judged.
	addresses := dualStackFixture()
	addresses.ManagerAvailable = false
	addresses.ActiveManager.Addrs = nil
	addresses.OSDs = append(addresses.OSDs, OSDAddress{ID: 1})
	if err := dualStackAddressProblem(addresses); err != nil {
		t.Fatal(err)
	}
}

// The MGR vector observed on Tentacle 20.2.4 and Squid 19.2.5 when the MON
// vector lists IPv4 first: its IPv6 entry is the wildcard.
func TestDualStackAdmissionRejectsWildcardManager(t *testing.T) {
	for name, mutate := range map[string]func(*DaemonAddresses){
		"MGR IPv6 wildcard": func(a *DaemonAddresses) {
			a.ActiveManager.Addrs = []EntityAddress{{"v2", "[::]:6800", 751301906}, {"v2", "172.19.0.3:6801", 751301906}}
		},
		"MGR IPv4 wildcard": func(a *DaemonAddresses) {
			a.ActiveManager.Addrs = []EntityAddress{{"v2", "0.0.0.0:6800", 7}, {"v2", "[fd62:93fe:2c9e::3]:6801", 7}}
		},
		"MGR single family": func(a *DaemonAddresses) {
			a.ActiveManager.Addrs = []EntityAddress{{"v2", "172.19.0.3:6800", 7}}
		},
		"MGR empty vector": func(a *DaemonAddresses) { a.ActiveManager.Addrs = nil },
		"MON v1 single family": func(a *DaemonAddresses) {
			a.Monitors[0].Addrs = a.Monitors[0].Addrs[:3]
		},
		"MON mapped address": func(a *DaemonAddresses) {
			a.Monitors[0].Addrs = []EntityAddress{{"v2", "[::ffff:172.19.0.2]:3300", 0}, {"v2", "172.19.0.2:3300", 0}}
		},
		"OSD heartbeat link-local": func(a *DaemonAddresses) {
			a.OSDs[0].HeartbeatBack = []EntityAddress{{"v2", "172.19.0.4:6806", 9}, {"v2", "[fe80::4]:6807", 9}}
		},
		"OSD cluster two IPv4": func(a *DaemonAddresses) {
			a.OSDs[0].Cluster = []EntityAddress{{"v2", "172.19.0.4:6802", 9}, {"v2", "172.20.0.4:6803", 9}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			addresses := dualStackFixture()
			mutate(&addresses)
			err := dualStackAddressProblem(addresses)
			if err == nil {
				t.Fatal("accepted")
			}
			t.Log(err)
		})
	}
	addresses := dualStackFixture()
	addresses.ActiveManager.Addrs[1].Addr = "[::]:6801"
	if err := dualStackAddressProblem(addresses); err == nil || !strings.Contains(err.Error(), "mgr.a advertises v2:[::]:6801") {
		t.Fatalf("the error does not name the MGR and its wildcard: %v", err)
	}
}

func TestDualStackOptionsAndSubnets(t *testing.T) {
	if AddressDualStack.String() != "dual-stack" || AddressDualStack != 3 {
		t.Fatal("dual-stack value or name changed")
	}
	var settings options
	if err := WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressDualStack))(&settings); err != nil || settings.addressFamily != AddressDualStack {
		t.Fatalf("dual-stack: %v %v", err, settings.addressFamily)
	}
	if settings.bridgeFamily() == (options{}).bridgeFamily() {
		t.Fatal("dual-stack does not create a dual-stack bridge")
	}
	v4 := dockernetwork.IPAMConfig{Subnet: netip.MustParsePrefix("172.19.0.0/16")}
	v6 := dockernetwork.IPAMConfig{Subnet: netip.MustParsePrefix("fd62:93fe:2c9e::/64")}
	// Docker may list the IPv6 configuration first; Ceph gets IPv4 first.
	subnets, err := familySubnets([]dockernetwork.IPAMConfig{v6, v4}, AddressDualStack)
	if err != nil || networkList(subnets) != "172.19.0.0/16,fd62:93fe:2c9e::/64" {
		t.Fatalf("subnets %v, %v", subnets, err)
	}
	for _, configs := range [][]dockernetwork.IPAMConfig{{v4}, {v6}} {
		if _, err := familySubnets(configs, AddressDualStack); err == nil {
			t.Fatalf("a single-family bridge %v was accepted", configs)
		}
	}
	for family, want := range map[AddressFamily]string{AddressIPv4: "172.19.0.0/16", AddressIPv6: "fd62:93fe:2c9e::/64"} {
		if subnets, err := familySubnets([]dockernetwork.IPAMConfig{v6, v4}, family); err != nil || networkList(subnets) != want {
			t.Fatalf("%s: %v, %v", family, subnets, err)
		}
	}
	public, _ := familySubnets([]dockernetwork.IPAMConfig{v4, v6}, AddressDualStack)
	other, _ := familySubnets([]dockernetwork.IPAMConfig{{Subnet: netip.MustParsePrefix("172.20.0.0/16")}, {Subnet: netip.MustParsePrefix("fd62:93fe:2c9e::/48")}}, AddressDualStack)
	if !subnetsOverlap(public, other) {
		t.Fatal("overlapping IPv6 subnets of two planes were accepted")
	}
	other[1] = netip.MustParsePrefix("fd00:1::/64")
	if subnetsOverlap(public, other) {
		t.Fatal("disjoint planes overlap")
	}
}
