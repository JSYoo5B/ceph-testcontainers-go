package ceph

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

func networkOptions(t *testing.T, networks ...NetworkConfig) (*options, error) {
	t.Helper()
	o := &options{publicAddress: "127.0.0.1"}
	for _, n := range networks {
		if err := WithNetworkConfig(n)(o); err != nil {
			return o, err
		}
	}
	return o, nil
}

// Zero, one and several values.
func TestNetworkConfigZeroOneAndSeveralValues(t *testing.T) {
	o, err := networkOptions(t, NetworkConfig{})
	if err != nil || o.addressFamily != AddressIPv4 || o.separateClusterNetwork || o.hostAddressSet || o.publicAddress != "127.0.0.1" ||
		o.messengerMode != MessengerDefault || o.messengerPolicy.Monitors != nil || o.networkSet != 0 {
		t.Fatalf("the zero value changed the settings: %+v %v", o, err)
	}
	o, err = networkOptions(t, NetworkConfig{}.WithAddressFamily(AddressIPv6))
	if err != nil || o.addressFamily != AddressIPv6 || o.separateClusterNetwork || o.messengerMode != MessengerDefault {
		t.Fatalf("one field: %+v %v", o, err)
	}
	o, err = networkOptions(t, NetworkConfig{}.WithAddressFamily(AddressDualStack).WithSeparateClusterNetwork().WithMessengerMode(MessengerV2Only))
	if err != nil || o.addressFamily != AddressDualStack || !o.separateClusterNetwork || o.messengerMode != MessengerV2Only {
		t.Fatalf("chained fields: %+v %v", o, err)
	}
	// Different fields in different values compose in either order, around an empty value.
	policy := MessengerPolicy{Monitors: []MessengerConnectionMode{MessengerCRC}}
	for _, order := range [][]NetworkConfig{
		{NetworkConfig{}.WithHostAddress("192.0.2.10"), NetworkConfig{}.WithMessengerPolicy(policy), NetworkConfig{}},
		{NetworkConfig{}, NetworkConfig{}.WithMessengerPolicy(policy), NetworkConfig{}.WithHostAddress("192.0.2.10")},
	} {
		o, err = networkOptions(t, order...)
		if err != nil || !o.hostAddressSet || o.publicAddress != "192.0.2.10" || len(o.messengerPolicy.Monitors) != 1 {
			t.Fatalf("separate values: %+v %v", o, err)
		}
	}
}

// An explicit default is a setting, an explicit invalid value is refused, and
// unspecified keeps the default.
func TestNetworkConfigExplicitZeroIsNotUnspecified(t *testing.T) {
	o, err := networkOptions(t, NetworkConfig{}.WithAddressFamily(AddressIPv4).WithMessengerMode(MessengerDefault))
	if err != nil || o.networkSet&networkAddressFamily == 0 || o.networkSet&networkMessengerMode == 0 {
		t.Fatalf("an explicit default was not recorded as a setting: %+v %v", o, err)
	}
	// Because the default family was set explicitly, a second carrier cannot set it.
	if _, err := networkOptions(t, NetworkConfig{}.WithAddressFamily(AddressIPv4), NetworkConfig{}.WithAddressFamily(AddressIPv6)); err == nil {
		t.Error("an explicit default family was silently overridden by a later carrier")
	}
	for name, n := range map[string]NetworkConfig{
		"unknown family":          NetworkConfig{}.WithAddressFamily(AddressFamily(99)),
		"negative family":         NetworkConfig{}.WithAddressFamily(AddressFamily(-1)),
		"unknown mode":            NetworkConfig{}.WithMessengerMode(MessengerMode(99)),
		"empty host address":      NetworkConfig{}.WithHostAddress(""),
		"unspecified address":     NetworkConfig{}.WithHostAddress("0.0.0.0"),
		"ipv6 host address":       NetworkConfig{}.WithHostAddress("::1"),
		"multicast host address":  NetworkConfig{}.WithHostAddress("224.0.0.1"),
		"broadcast host address":  NetworkConfig{}.WithHostAddress("255.255.255.255"),
		"empty monitors list":     NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{Monitors: []MessengerConnectionMode{}}),
		"unknown connection mode": NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{OSDs: []MessengerConnectionMode{"plain"}}),
		"duplicate mode":          NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{Managers: []MessengerConnectionMode{MessengerCRC, MessengerCRC}}),
		"valid then invalid":      NetworkConfig{}.WithHostAddress("192.0.2.10").WithHostAddress(""),
	} {
		if _, err := networkOptions(t, n); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A nil list stays the default and is not an empty list.
	o, err = networkOptions(t, NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{}))
	if err != nil || o.messengerPolicy.Monitors != nil || o.messengerPolicy.Managers != nil || o.messengerPolicy.OSDs != nil {
		t.Fatalf("a nil policy list changed: %+v %v", o, err)
	}
}

// A refused value changes nothing, even for the fields that were valid.
func TestNetworkConfigRefusalChangesNothing(t *testing.T) {
	o, err := networkOptions(t, NetworkConfig{}.WithAddressFamily(AddressIPv6).WithSeparateClusterNetwork().WithMessengerMode(MessengerMode(99)))
	if err == nil || o.addressFamily != AddressIPv4 || o.separateClusterNetwork || o.networkSet != 0 {
		t.Fatalf("a refused value was partly applied: %+v %v", o, err)
	}
}

// A field set by two values is refused, also when they agree; within one value
// the later call replaces the earlier one.
func TestNetworkConfigConflictsAreRefused(t *testing.T) {
	policy := MessengerPolicy{OSDs: []MessengerConnectionMode{MessengerSecure}}
	for name, values := range map[string][]NetworkConfig{
		"family twice":           {NetworkConfig{}.WithAddressFamily(AddressIPv6), NetworkConfig{}.WithAddressFamily(AddressDualStack)},
		"equal families":         {NetworkConfig{}.WithAddressFamily(AddressIPv6), NetworkConfig{}.WithAddressFamily(AddressIPv6)},
		"separate network twice": {NetworkConfig{}.WithSeparateClusterNetwork(), NetworkConfig{}.WithSeparateClusterNetwork()},
		"host address twice":     {NetworkConfig{}.WithHostAddress("192.0.2.10"), NetworkConfig{}.WithHostAddress("192.0.2.11")},
		"mode twice":             {NetworkConfig{}.WithMessengerMode(MessengerV2Only), NetworkConfig{}.WithMessengerMode(MessengerV2Secure)},
		"policy twice":           {NetworkConfig{}.WithMessengerPolicy(policy), NetworkConfig{}.WithMessengerPolicy(policy)},
		"gap between":            {NetworkConfig{}.WithSeparateClusterNetwork(), NetworkConfig{}, NetworkConfig{}.WithSeparateClusterNetwork()},
		"overlap in a mixed":     {NetworkConfig{}.WithAddressFamily(AddressIPv6).WithSeparateClusterNetwork(), NetworkConfig{}.WithSeparateClusterNetwork()},
	} {
		if _, err := networkOptions(t, values...); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	o, err := networkOptions(t, NetworkConfig{}.WithAddressFamily(AddressIPv6).WithAddressFamily(AddressDualStack))
	if err != nil || o.addressFamily != AddressDualStack {
		t.Fatalf("later call of one value did not replace the earlier: %+v %v", o, err)
	}
}

// Derived values leave their base and each other unchanged, and a policy value
// owns its lists: nil and non-nil empty stay apart, and neither the caller's
// slices nor the settings written from the value share storage with it.
func TestNetworkConfigDerivedValuesAreIndependent(t *testing.T) {
	base := NetworkConfig{}.WithAddressFamily(AddressIPv6)
	first, second := base.WithSeparateClusterNetwork(), base.WithMessengerMode(MessengerV2Only)
	moved := base.WithAddressFamily(AddressDualStack)
	if o, err := networkOptions(t, base); err != nil || o.addressFamily != AddressIPv6 || o.separateClusterNetwork || o.messengerMode != MessengerDefault {
		t.Fatalf("base changed by its derived values: %+v %v", o, err)
	}
	if o, err := networkOptions(t, first); err != nil || !o.separateClusterNetwork || o.messengerMode != MessengerDefault {
		t.Fatalf("first: %+v %v", o, err)
	}
	if o, err := networkOptions(t, second); err != nil || o.separateClusterNetwork || o.messengerMode != MessengerV2Only {
		t.Fatalf("second: %+v %v", o, err)
	}
	if o, err := networkOptions(t, moved); err != nil || o.addressFamily != AddressDualStack {
		t.Fatalf("moved: %+v %v", o, err)
	}

	monitors := []MessengerConnectionMode{MessengerCRC}
	withPolicy := NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{Monitors: monitors})
	monitors[0] = MessengerSecure // the caller reuses its slice
	o1, err := networkOptions(t, withPolicy)
	if err != nil || o1.messengerPolicy.Monitors[0] != MessengerCRC {
		t.Fatalf("the value shares the caller's slice: %+v %v", o1, err)
	}
	o1.messengerPolicy.Monitors[0] = MessengerSecure // the settings are changed in place
	o2, err := networkOptions(t, withPolicy)
	if err != nil || o2.messengerPolicy.Monitors[0] != MessengerCRC {
		t.Fatalf("the settings share storage with the value: %+v %v", o2, err)
	}
	// An explicit empty list survives the copy and is refused; a nil list stays nil.
	if _, err := networkOptions(t, NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{OSDs: []MessengerConnectionMode{}})); err == nil {
		t.Error("an explicit empty list was turned into the default by the copy")
	}
	if o, err := networkOptions(t, NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{Managers: []MessengerConnectionMode{MessengerSecure}})); err != nil || o.messengerPolicy.Monitors != nil || o.messengerPolicy.OSDs != nil {
		t.Fatalf("a nil list became non-nil: %+v %v", o, err)
	}
}

// NetworkConfig is a plain value that reaches Run only through WithNetworkConfig.
func TestNetworkConfigIsNotACustomizer(t *testing.T) {
	if _, ok := any(NetworkConfig{}).(testcontainers.ContainerCustomizer); ok {
		t.Fatal("NetworkConfig is itself a customizer; it must be carried by WithNetworkConfig")
	}
	var carrier testcontainers.ContainerCustomizer = WithNetworkConfig(NetworkConfig{}.WithSeparateClusterNetwork())
	request := &testcontainers.GenericContainerRequest{}
	if err := carrier.Customize(request); err != nil || request.Image != "" || request.Env != nil {
		t.Fatalf("the carrier changed the request: %v %+v", err, request)
	}
}

// Through Run: a refusal happens before any customizer runs, the composition
// rules between the network settings and Docker execution policy hold, and a
// valid configuration keeps the other settings (a service setting is never dropped).
func TestRunNetworkConfigRefusesBeforeEffectsAndKeepsOtherSettings(t *testing.T) {
	calls := 0
	raw := testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { calls++; return nil })
	secure := NetworkConfig{}.WithMessengerMode(MessengerV2Secure)
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"invalid family":             {WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressFamily(99))), raw},
		"conflict":                   {WithNetworkConfig(secure), WithNetworkConfig(secure), raw},
		"host address needs host":    {WithNetworkConfig(NetworkConfig{}.WithHostAddress("192.0.2.10")), raw},
		"host with separate network": {WithHostNetwork(), WithNetworkConfig(NetworkConfig{}.WithSeparateClusterNetwork()), raw},
		"host with ipv6":             {WithHostNetwork(), WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv6)), raw},
		"mapped with separate":       {WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv4MappedIPv6).WithSeparateClusterNetwork()), raw},
		"secure with policy": {WithNetworkConfig(secure), raw,
			WithNetworkConfig(NetworkConfig{}.WithMessengerPolicy(MessengerPolicy{Monitors: []MessengerConnectionMode{MessengerCRC}}))},
	} {
		calls = 0
		_, err := admit(t, "image", opts...)
		wantRefused(t, err)
		if calls != 0 {
			t.Errorf("%s: a customizer ran before the refusal", name)
		}
	}
	effective, err := admit(t, "image",
		WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressDualStack).WithSeparateClusterNetwork()),
		WithNetworkConfig(NetworkConfig{}.WithMessengerMode(MessengerV2Only)),
		roleImageOption("rgw", "selected-gateway"), WithClusterConfig(ClusterConfig{}.WithOSDCount(3)), raw)
	wantAdmitted(t, err)
	if effective.addressFamily != AddressDualStack || !effective.separateClusterNetwork || effective.messengerMode != MessengerV2Only ||
		effective.rgwOverride != "selected-gateway" || effective.osds != 3 || calls != 0 {
		t.Fatalf("a setting was dropped or reached a container: %+v calls=%d", effective, calls)
	}
	effective, err = admit(t, "image", WithHostNetwork(), WithNetworkConfig(NetworkConfig{}.WithHostAddress("192.0.2.10")))
	wantAdmitted(t, err)
	if !effective.hostNetwork || effective.publicAddress != "192.0.2.10" {
		t.Fatalf("host network and host address: %+v", effective)
	}
}
