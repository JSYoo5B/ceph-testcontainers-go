package ceph

import (
	"context"
	"errors"
	"net/netip"
	"testing"

	"github.com/moby/moby/api/types/container"
	dockernetwork "github.com/moby/moby/api/types/network"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// monitorEndpointModel is one MON container on the public bridge as Docker
// keeps it: whether it runs, and its endpoint, nil while detached. Like
// Docker, it assigns the address when the container starts: the requested
// one, or the lowest free one for a dynamic endpoint.
type monitorEndpointModel struct {
	running     bool
	endpoint    *dockernetwork.EndpointSettings
	taken       map[netip.Addr]bool
	connects    int
	disconnects int
}

func (m *monitorEndpointModel) ContainerInspect(context.Context, string, mobycl.ContainerInspectOptions) (mobycl.ContainerInspectResult, error) {
	networks := map[string]*dockernetwork.EndpointSettings{}
	if m.endpoint != nil {
		networks["public"] = m.endpoint.Copy()
	}
	return mobycl.ContainerInspectResult{Container: container.InspectResponse{
		State: &container.State{Running: m.running}, NetworkSettings: &container.NetworkSettings{Networks: networks},
	}}, nil
}

func (m *monitorEndpointModel) NetworkDisconnect(context.Context, string, mobycl.NetworkDisconnectOptions) (mobycl.NetworkDisconnectResult, error) {
	m.disconnects++
	m.endpoint = nil
	return mobycl.NetworkDisconnectResult{}, nil
}

func (m *monitorEndpointModel) NetworkConnect(_ context.Context, _ string, opts mobycl.NetworkConnectOptions) (mobycl.NetworkConnectResult, error) {
	m.connects++
	m.endpoint = opts.EndpointConfig.Copy()
	return mobycl.NetworkConnectResult{}, nil
}

func (m *monitorEndpointModel) Close() error { return nil }

// start runs the container: the endpoint gets its requested address, or the
// lowest one that no other container holds.
func (m *monitorEndpointModel) start() {
	m.running = true
	if m.endpoint == nil {
		return
	}
	if m.endpoint.IPAMConfig != nil && m.endpoint.IPAMConfig.IPv4Address.IsValid() {
		m.endpoint.IPAddress = m.endpoint.IPAMConfig.IPv4Address
		return
	}
	for address := netip.MustParseAddr("172.19.0.2"); ; address = address.Next() {
		if !m.taken[address] {
			m.endpoint.IPAddress = address
			return
		}
	}
}

func (m *monitorEndpointModel) stop() {
	m.running = false
	if m.endpoint != nil {
		m.endpoint.IPAddress = netip.Addr{}
	}
}

type monitorAddressContainer struct{ testcontainers.Container }

func (monitorAddressContainer) GetContainerID() string { return "mon-a" }

// monitorAddressHooks returns the record and pin hooks of one MON container,
// with Docker replaced by model.
func monitorAddressHooks(t *testing.T, model *monitorEndpointModel) (record, pin func() error) {
	t.Helper()
	previous := newMonitorAddressDocker
	newMonitorAddressDocker = func(context.Context) (networkLinkClient, error) { return model, nil }
	t.Cleanup(func() { newMonitorAddressDocker = previous })
	var req testcontainers.GenericContainerRequest
	if err := keepMonitorAddress("public").Customize(&req); err != nil {
		t.Fatal(err)
	}
	hooks := req.LifecycleHooks[len(req.LifecycleHooks)-1]
	ctr := monitorAddressContainer{}
	return func() error { return hooks.PostReadies[0](t.Context(), ctr) },
		func() error { return hooks.PreStarts[0](t.Context(), ctr) }
}

// startedMonitor is a MON that started once at 172.19.0.2 with a dynamic
// endpoint, and whose hooks recorded it.
func startedMonitor(t *testing.T) (*monitorEndpointModel, func() error) {
	t.Helper()
	model := &monitorEndpointModel{taken: map[netip.Addr]bool{}, endpoint: &dockernetwork.EndpointSettings{Aliases: []string{"ceph-mon"}}}
	record, pin := monitorAddressHooks(t, model)
	model.start()
	if err := record(); err != nil {
		t.Fatal(err)
	}
	return model, pin
}

func TestMonitorAddressSurvivesARestartAfterAnotherContainerTookIt(t *testing.T) {
	model, pin := startedMonitor(t)
	model.stop()
	if err := pin(); err != nil {
		t.Fatal(err)
	}
	// Another container starts first and takes the lowest free address,
	// 172.19.0.2 for a dynamic endpoint; the pinned MON still gets it back.
	model.taken[netip.MustParseAddr("172.19.0.2")] = true
	model.start()
	if model.endpoint == nil || model.endpoint.IPAddress != netip.MustParseAddr("172.19.0.2") ||
		model.endpoint.IPAMConfig == nil || model.endpoint.IPAMConfig.IPv4Address != netip.MustParseAddr("172.19.0.2") ||
		len(model.endpoint.Aliases) != 1 || model.endpoint.Aliases[0] != "ceph-mon" {
		t.Fatalf("the MON did not get its recorded address back: %+v", model.endpoint)
	}
	// Once pinned, later starts leave the endpoint alone.
	model.stop()
	connects := model.connects
	if err := pin(); err != nil || model.connects != connects {
		t.Fatalf("a pinned endpoint was reconnected: %v, %d connects", err, model.connects)
	}
}

func TestMonitorAddressKeepsANetworkInterruptionUntilItsOwnerRestores(t *testing.T) {
	model, pin := startedMonitor(t)
	// InterruptNetwork detached the endpoint, then the MON stopped.
	model.endpoint = nil
	model.stop()
	err := pin()
	if !errors.Is(err, ErrInvalidRequest) || model.endpoint != nil || model.connects != 0 || model.disconnects != 0 {
		t.Fatalf("Start reattached an interrupted MON: %v, endpoint %+v, %d connects", err, model.endpoint, model.connects)
	}
	// Restore reattaches the original address explicitly; the MON then starts
	// there without another reconnect.
	model.endpoint = restoreEndpoint(&dockernetwork.EndpointSettings{IPAddress: netip.MustParseAddr("172.19.0.2"), Aliases: []string{"ceph-mon"}})
	if err := pin(); err != nil || model.connects != 0 {
		t.Fatalf("a restored endpoint: %v, %d connects", err, model.connects)
	}
	model.start()
	if model.endpoint.IPAddress != netip.MustParseAddr("172.19.0.2") {
		t.Fatalf("the restored MON started at %s", model.endpoint.IPAddress)
	}
}

func TestMonitorAddressRefusesAnEndpointPinnedElsewhere(t *testing.T) {
	model, pin := startedMonitor(t)
	model.stop()
	model.endpoint.IPAMConfig = &dockernetwork.EndpointIPAMConfig{IPv4Address: netip.MustParseAddr("172.19.0.9")}
	if err := pin(); err == nil || model.connects != 0 || model.disconnects != 0 || model.endpoint.IPAMConfig.IPv4Address != netip.MustParseAddr("172.19.0.9") {
		t.Fatalf("an endpoint pinned elsewhere was replaced: %v, %+v", err, model.endpoint)
	}
}

func TestMonitorAddressLeavesARunningMonitorAlone(t *testing.T) {
	model, pin := startedMonitor(t)
	if err := pin(); err != nil || model.connects != 0 || model.disconnects != 0 {
		t.Fatalf("Start on a running MON touched its endpoint: %v, %d connects, %d disconnects", err, model.connects, model.disconnects)
	}
}
