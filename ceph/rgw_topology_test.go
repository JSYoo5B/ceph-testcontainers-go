package ceph

import (
	"context"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
)

func TestRGWConfigurationRejectsUnsafeNamesBeforeStarting(t *testing.T) {
	for _, config := range []RGWConfig{
		{Name: "--gateway"}, {Name: "bad/name"}, {Name: "with space"},
		{Realm: "--realm"}, {Zonegroup: "zone\nother"}, {Zone: " trailing "}, {Region: "region\x00"},
	} {
		cluster := &Container{}
		if gateway, err := cluster.StartRGWWithConfig(t.Context(), config); err == nil || gateway != nil {
			t.Fatalf("unsafe gateway configuration was accepted: %+v", config)
		}
	}
	config, err := normalizeRGWConfig(RGWConfig{})
	if err != nil || config.Name != "default" || config.Region != "us-east-1" || rgwServiceName(config) != "rgw" {
		t.Fatalf("default gateway compatibility changed: %+v error=%v", config, err)
	}
}

func TestGatewayRemovalRetainsFailedCleanup(t *testing.T) {
	ctr := &hostNetworkFixtureContainer{failTerminationOnce: true}
	gateway := &RGWContainer{Container: ctr, GatewayName: "a"}
	cluster := &Container{settings: options{startupTimeout: time.Second},
		services: map[string]testcontainers.Container{"rgw:a": ctr},
		gateways: map[string]*RGWContainer{"a": gateway}}
	if err := cluster.RemoveRGW(t.Context(), "unknown"); err == nil || ctr.terminations != 0 {
		t.Fatal("unknown gateway attempted container removal")
	}
	if err := cluster.RemoveRGW(t.Context(), "a"); err == nil || len(cluster.gateways) != 1 || len(cluster.services) != 1 {
		t.Fatal("failed cleanup lost gateway ownership")
	}
	if err := cluster.RemoveRGW(t.Context(), "a"); err != nil || len(cluster.gateways) != 0 || len(cluster.services) != 0 || ctr.terminations != 2 {
		t.Fatalf("gateway cleanup did not recover: %v", err)
	}
}

func TestNamedRGWOptionsPreserveScopeAndHostListener(t *testing.T) {
	for _, host := range []bool{false, true} {
		port := 7480
		if host {
			port = 42791
		}
		cluster := &Container{settings: options{startupTimeout: time.Second, hostNetwork: host, publicAddress: "127.0.0.1"}}
		config := RGWConfig{Name: "gateway-b", Realm: "fixture-realm", Zonegroup: "fixture-zonegroup", Zone: "fixture-zone"}
		var request testcontainers.GenericContainerRequest
		for _, option := range cluster.namedRGWDaemonOptions(port, 0, config) {
			if err := option.Customize(&request); err != nil {
				t.Fatal(err)
			}
		}
		if rgwServiceName(config) != "rgw:gateway-b" || request.Env["CEPH_RGW_REALM"] != config.Realm || request.Env["CEPH_RGW_ZONEGROUP"] != config.Zonegroup || request.Env["CEPH_RGW_ZONE"] != config.Zone || request.Env["CEPH_RGW_PORT"] != strconv.Itoa(port) {
			t.Fatal("gateway lost its instance, scope or listener settings")
		}
		if host && (len(request.ExposedPorts) != 0 || request.Env["CEPH_RGW_ENDPOINT"] != "127.0.0.1:42791") {
			t.Fatal("host gateway advertised a mapped port or lost its allocated listener")
		}
		if !host && (len(request.ExposedPorts) != 1 || request.ExposedPorts[0] != "7480/tcp") {
			t.Fatal("bridge gateway did not expose its container-local listener")
		}
	}
}

func TestNamedRGWBridgePrefersPublicGatewayWithoutChangingClientOptions(t *testing.T) {
	// A peer sorting before the public network must not become the published
	// S3 ingress gateway solely because its randomly generated name is lower.
	const public, peer = "z-public-network", "a-peer-network"
	cluster := &Container{network: &testcontainers.DockerNetwork{Name: public}, settings: options{startupTimeout: time.Second}}
	var clientRequest testcontainers.GenericContainerRequest
	if err := cluster.WithClient().Customize(&clientRequest); err != nil {
		t.Fatal(err)
	}
	if clientRequest.EndpointSettingsModifier != nil {
		t.Fatal("ordinary Ceph client configuration acquired an RGW gateway policy")
	}
	request := clientRequest
	for _, option := range cluster.namedRGWDaemonOptions(7480, 0, RGWConfig{Name: "gateway"}) {
		if err := option.Customize(&request); err != nil {
			t.Fatal(err)
		}
	}
	if err := tcnetwork.WithNetworkName([]string{"peer-alias"}, peer).Customize(&request); err != nil {
		t.Fatal(err)
	}
	if len(request.Networks) != 2 || request.Networks[0] != public || request.Networks[1] != peer || request.EndpointSettingsModifier == nil {
		t.Fatal("RGW public endpoint is not first at Docker creation or peer composition changed")
	}
	// Caller modifiers remain composed after the module defaults, as with
	// other Testcontainers request customizers.
	if err := testcontainers.WithEndpointSettingsModifier(func(endpoints map[string]*mobynetwork.EndpointSettings) {
		endpoints[public].DriverOpts = map[string]string{"caller-option": "preserved"}
	}).Customize(&request); err != nil {
		t.Fatal(err)
	}
	endpoints := map[string]*mobynetwork.EndpointSettings{
		public: {Aliases: []string{"local-alias"}},
		peer:   {Aliases: []string{"peer-alias"}},
	}
	request.EndpointSettingsModifier(endpoints)
	if endpoints[public].GwPriority != 1 || endpoints[peer].GwPriority != 0 || endpoints[public].Aliases[0] != "local-alias" || endpoints[peer].Aliases[0] != "peer-alias" || endpoints[public].DriverOpts["caller-option"] != "preserved" {
		t.Fatal("RGW gateway policy changed peer/client settings or lost caller modifier composition")
	}
	if err := testcontainers.WithEndpointSettingsModifier(func(endpoints map[string]*mobynetwork.EndpointSettings) {
		endpoints[public].GwPriority = 7
	}).Customize(&request); err != nil {
		t.Fatal(err)
	}
	request.EndpointSettingsModifier(endpoints)
	if endpoints[public].GwPriority != 7 || endpoints[peer].GwPriority != 0 {
		t.Fatal("RGW default prevented an explicit subsequent caller gateway policy")
	}
}

func TestNamedRGWHostDoesNotConfigureBridgeGatewayPriority(t *testing.T) {
	cluster := &Container{settings: options{startupTimeout: time.Second, hostNetwork: true, publicAddress: "127.0.0.1"}}
	var request testcontainers.GenericContainerRequest
	for _, option := range cluster.namedRGWDaemonOptions(42791, 0, RGWConfig{Name: "gateway"}) {
		if err := option.Customize(&request); err != nil {
			t.Fatal(err)
		}
	}
	if request.EndpointSettingsModifier != nil || len(request.ExposedPorts) != 0 || request.Env["CEPH_RGW_ENDPOINT"] != "127.0.0.1:42791" {
		t.Fatal("host listener configuration acquired bridge routing or port publication")
	}
}

func TestNamedRGWConflictCleanupKeepsOtherGateways(t *testing.T) {
	first := &rgwTestContainer{id: "gateway-a"}
	second := &rgwTestContainer{id: "gateway-b"}
	cluster := &Container{services: map[string]testcontainers.Container{"rgw:gateway-a": first, "rgw:gateway-b": second}}
	if err := cluster.discardNamedRGWAttempt(t.Context(), "rgw:gateway-b", second); err != nil || cluster.services["rgw:gateway-a"] != first {
		t.Fatal("named retry cleanup removed another gateway")
	}
	if _, exists := cluster.services["rgw:gateway-b"]; exists {
		t.Fatal("named retry did not clear the failed service")
	}
}

func TestRGWDaemonEndpointUsesCephNetworkAndAllocatedHostPort(t *testing.T) {
	ctr := &rgwTopologyEndpointContainer{inspect: &container.InspectResponse{NetworkSettings: &container.NetworkSettings{Networks: map[string]*mobynetwork.EndpointSettings{
		"ceph-network": {IPAddress: netip.MustParseAddr("172.22.0.8")},
		"http-bridge":  {IPAddress: netip.MustParseAddr("172.23.0.8")},
	}}}}
	for _, test := range []struct {
		gateway RGWContainer
		want    string
	}{
		{RGWContainer{Container: ctr, port: 7480, networkName: "ceph-network"}, "http://172.22.0.8:7480"},
		{RGWContainer{Container: ctr, port: 42791, publicAddress: "127.0.0.1", networkName: "host"}, "http://127.0.0.1:42791"},
		{RGWContainer{Container: ctr, port: 42791, publicAddress: "192.0.2.34", networkName: "host"}, "http://192.0.2.34:42791"},
	} {
		endpoint, err := test.gateway.DaemonEndpoint(t.Context())
		if err != nil || endpoint != test.want {
			t.Fatalf("peer endpoint used wrong network/port: endpoint=%q error=%v", endpoint, err)
		}
	}
	missing := &RGWContainer{Container: ctr, networkName: "missing-network"}
	if _, err := missing.DaemonEndpoint(t.Context()); err == nil {
		t.Fatal("missing Ceph network was replaced with an unrelated bridge endpoint")
	}
}

type rgwTopologyEndpointContainer struct {
	testcontainers.Container
	inspect *container.InspectResponse
}

func (ctr *rgwTopologyEndpointContainer) Inspect(context.Context) (*container.InspectResponse, error) {
	return ctr.inspect, nil
}
