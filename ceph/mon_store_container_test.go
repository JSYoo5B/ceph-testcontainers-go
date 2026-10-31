package ceph

import (
	"context"
	"errors"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	dockernetwork "github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
)

// generationDouble is a created MON container: only its ID is read.
type generationDouble struct {
	testcontainers.Container
	id string
}

func (d generationDouble) GetContainerID() string { return d.id }

func TestMonitorStorageVolumeNeedsBridgeIPv4AndOneCarrier(t *testing.T) {
	volume := WithMonitorStorage(MonitorStorage{}.WithVolume())
	settings, err := resolveOptions("example.invalid/ceph:x", []testcontainers.ContainerCustomizer{volume})
	if err != nil || !settings.monitorVolume {
		t.Fatalf("bridge IPv4 volume: %v %v", settings.monitorVolume, err)
	}
	if settings, err := resolveOptions("example.invalid/ceph:x", []testcontainers.ContainerCustomizer{WithMonitorStorage(MonitorStorage{})}); err != nil || settings.monitorVolume {
		t.Fatalf("the zero value changed the settings: %v %v", settings.monitorVolume, err)
	}
	for name, opts := range map[string][]testcontainers.ContainerCustomizer{
		"twice":        {volume, volume},
		"host network": {volume, WithHostNetwork()},
		"IPv6":         {volume, WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressIPv6))},
		"dual stack":   {volume, WithNetworkConfig(NetworkConfig{}.WithAddressFamily(AddressDualStack))},
	} {
		if _, err := resolveOptions("example.invalid/ceph:x", opts); !errors.Is(err, ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// generationRequest applies what Create passes to the runner to a request.
func generationRequest(t *testing.T, launch monStoreLaunch) (testcontainers.GenericContainerRequest, error) {
	t.Helper()
	c := &Container{}
	c.nets.public = &testcontainers.DockerNetwork{Name: "tc-public"}
	var req testcontainers.GenericContainerRequest
	c.daemonRunner = func(_ context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		req.Image = image
		for _, opt := range opts {
			if err := opt.Customize(&req); err != nil {
				return nil, err
			}
		}
		return generationDouble{id: "gen-1"}, nil
	}
	gens := c.monitorGenerations("b", nil, []string{"true"})
	_, err := gens.Create(t.Context(), launch, "example.invalid/ceph:new")
	return req, err
}

func TestMonitorGenerationCreatesOnTheStoreVolumeWithoutStarting(t *testing.T) {
	first, err := generationRequest(t, monStoreLaunch{volume: "tc-ceph-mon-volume-1", mode: monStoreFormat, generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	host := first.HostConfigModifier
	if host == nil || first.Started || first.Image != "example.invalid/ceph:new" {
		t.Fatalf("first generation request: started=%t image=%q", first.Started, first.Image)
	}
	hc := &container.HostConfig{}
	host(hc)
	if len(hc.Mounts) != 1 || hc.Mounts[0].Type != mount.TypeVolume || hc.Mounts[0].Source != "tc-ceph-mon-volume-1" ||
		hc.Mounts[0].Target != "/var/lib/ceph/mon/ceph-b" || hc.Mounts[0].VolumeOptions == nil || !hc.Mounts[0].VolumeOptions.NoCopy {
		t.Fatalf("store mount %+v", hc.Mounts)
	}
	if _, set := first.Env["CEPH_MON_STORE_MODE"]; set {
		t.Fatal("a first generation must be allowed to format")
	}
	endpoint := &monStoreEndpoint{ip: "172.30.0.5", network: "tc-public"}
	later, err := generationRequest(t, monStoreLaunch{volume: "tc-ceph-mon-volume-1", mode: monStoreRequire, generation: 2, endpoint: endpoint})
	if err != nil {
		t.Fatal(err)
	}
	if later.Env["CEPH_MON_STORE_MODE"] != "require" {
		t.Fatalf("a later generation must require the store: %v", later.Env)
	}
	settings := map[string]*dockernetwork.EndpointSettings{}
	if later.EndpointSettingsModifier == nil {
		t.Fatal("a later generation has no address request")
	}
	later.EndpointSettingsModifier(settings)
	if settings["tc-public"] == nil || settings["tc-public"].IPAMConfig == nil || settings["tc-public"].IPAMConfig.IPv4Address.String() != "172.30.0.5" {
		t.Fatalf("address request %+v", settings["tc-public"])
	}
	for name, e := range map[string]*monStoreEndpoint{
		"IPv6":          {ip: "fd00::5", network: "tc-public"},
		"other network": {ip: "172.30.0.5", network: "elsewhere"},
		"host ports":    {ip: "127.0.0.1", ports: []int{3300}},
	} {
		if _, err := generationRequest(t, monStoreLaunch{volume: "v", mode: monStoreRequire, generation: 2, endpoint: e}); err == nil {
			t.Errorf("%s: an address the bridge cannot give was requested", name)
		}
	}
}

func TestMonitorGenerationClearsBeforeCreating(t *testing.T) {
	c := &Container{}
	c.nets.public = &testcontainers.DockerNetwork{Name: "tc-public"}
	created := 0
	c.daemonRunner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		created++
		return generationDouble{id: "gen-1"}, nil
	}
	gens := c.monitorGenerations("b", nil, []string{"true"})
	gens.beforeCreate = func(context.Context) error { return errors.New("socket busy") }
	if _, err := gens.Create(t.Context(), monStoreLaunch{volume: "v", mode: monStoreFormat, generation: 1}, "img"); err == nil || created != 0 {
		t.Fatalf("a failed preparation still created a container: %v %d", err, created)
	}
}

func TestReplaceMonitorRefusesBeforeTouchingAnything(t *testing.T) {
	c := &Container{}
	mon := &MonitorContainer{DaemonName: "b", owner: c}
	if _, err := mon.Replace(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("without MON volume storage: %v", err)
	}
	c.settings.monitorVolume = true
	if _, err := mon.Replace(t.Context(), DaemonOptions{}.WithImage("")); err == nil {
		t.Fatal("an invalid image was accepted")
	}
	c.closed = true
	if _, err := mon.Replace(t.Context()); !errors.Is(err, errClusterTerminated) {
		t.Fatalf("closed cluster: %v", err)
	}
	c.closed = false
	if _, err := mon.Replace(t.Context()); err == nil {
		t.Fatal("a handle the cluster does not own was replaced")
	}
	if c.monStores.count() != 0 {
		t.Fatal("a refused replacement published a store")
	}
	if _, err := (*MonitorContainer)(nil).Replace(t.Context()); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("nil handle: %v", err)
	}
}
