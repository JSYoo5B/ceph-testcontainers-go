package ceph

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// hostGenerations is the host-network container side of MON b on 127.0.0.1
// with fakes for the allocator, the runner and the listener read. reserved
// records each reservation: nil for chosen ports, else the exact ones.
type hostGenerations struct {
	*monitorGenerations
	recipes   [][]int
	reserved  [][]int
	allocator *hostNetworkFixtureContainer
	listeners []monitorListener
}

func newHostGenerations(t *testing.T) *hostGenerations {
	t.Helper()
	c := &Container{}
	c.settings.hostNetwork, c.settings.publicAddress = true, "127.0.0.1"
	c.daemonRunner = func(_ context.Context, _ string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		var req testcontainers.GenericContainerRequest
		for _, opt := range opts {
			if err := opt.Customize(&req); err != nil {
				return nil, err
			}
		}
		return generationDouble{id: "gen-1"}, nil
	}
	h := &hostGenerations{allocator: &hostNetworkFixtureContainer{}}
	h.monitorGenerations = c.monitorGenerations("b", func(ports []int) []testcontainers.ContainerCustomizer {
		h.recipes = append(h.recipes, slices.Clone(ports))
		return nil
	}, []string{"true"})
	h.reserve = func(_ context.Context, count int, exact []int) (*hostPortLease, error) {
		h.reserved = append(h.reserved, slices.Clone(exact))
		ports := exact
		if ports == nil {
			ports = []int{41000, 41001}[:count]
		}
		return &hostPortLease{Ports: slices.Clone(ports), ctr: h.allocator}, nil
	}
	h.listening = func(context.Context, testcontainers.Container) ([]monitorListener, error) { return h.listeners, nil }
	return h
}

func TestHostMonitorGenerationReservesItsFirstPortsUntilCreated(t *testing.T) {
	h := newHostGenerations(t)
	id, err := h.Create(t.Context(), monStoreLaunch{volume: "v", mode: monStoreFormat, generation: 1}, "img")
	if err != nil || id != "gen-1" {
		t.Fatalf("create: %q %v", id, err)
	}
	if len(h.reserved) != 1 || h.reserved[0] != nil {
		t.Fatalf("a first generation must reserve chosen ports once: %v", h.reserved)
	}
	if len(h.recipes) != 1 || !slices.Equal(h.recipes[0], []int{41000, 41001}) || !slices.Equal(h.ports[id], []int{41000, 41001}) {
		t.Fatalf("recipe ports %v, recorded %v", h.recipes, h.ports[id])
	}
	if h.allocator.terminations != 1 {
		t.Fatalf("the reservation must be released once the container is created: %d", h.allocator.terminations)
	}
	if h.c.portLeases.count() != 1 {
		t.Fatal("the reservation was not published for Terminate")
	}
}

func TestHostMonitorGenerationKeepsAFailedReleaseOwned(t *testing.T) {
	h := newHostGenerations(t)
	h.allocator.terminationErr = errors.New("allocator removal failed")
	if _, err := h.Create(t.Context(), monStoreLaunch{volume: "v", mode: monStoreFormat, generation: 1}, "img"); err == nil {
		t.Fatal("a reservation that was not released did not fail the creation")
	}
	if h.c.portLeases.count() != 1 {
		t.Fatal("the unreleased reservation is not owned")
	}
}

func TestHostMonitorGenerationRecreatesOnTheRecordedPorts(t *testing.T) {
	h := newHostGenerations(t)
	endpoint := &monStoreEndpoint{ip: "127.0.0.1", ports: []int{41000, 41001}}
	id, err := h.Create(t.Context(), monStoreLaunch{volume: "v", mode: monStoreRequire, generation: 2, endpoint: endpoint}, "img")
	if err != nil {
		t.Fatal(err)
	}
	if len(h.reserved) != 0 {
		t.Fatalf("a later generation reserved ports itself: %v", h.reserved)
	}
	if !slices.Equal(h.recipes[0], endpoint.ports) || !slices.Equal(h.ports[id], endpoint.ports) {
		t.Fatalf("recipe ports %v, recorded %v", h.recipes, h.ports[id])
	}
	for name, e := range map[string]*monStoreEndpoint{
		"bridge network": {ip: "127.0.0.1", network: "tc-public"},
		"other address":  {ip: "127.0.0.2", ports: []int{41000, 41001}},
		"one port":       {ip: "127.0.0.1", ports: []int{41000}},
	} {
		if _, err := h.Create(t.Context(), monStoreLaunch{volume: "v", mode: monStoreRequire, generation: 2, endpoint: e}, "img"); err == nil {
			t.Errorf("%s: an endpoint the host network cannot give was used", name)
		}
	}
}

func TestHostMonitorHoldTakesTheExactPorts(t *testing.T) {
	h := newHostGenerations(t)
	hold, err := h.Hold(t.Context(), monStoreEndpoint{ip: "127.0.0.1", ports: []int{41001, 41000}})
	if err != nil || hold == nil {
		t.Fatalf("hold: %v %v", hold, err)
	}
	if len(h.reserved) != 1 || !slices.Equal(h.reserved[0], []int{41001, 41000}) || h.c.portLeases.count() != 1 {
		t.Fatalf("reserved %v, published %d", h.reserved, h.c.portLeases.count())
	}
	if err := hold.Release(t.Context()); err != nil || h.allocator.terminations != 1 {
		t.Fatalf("release: %v %d", err, h.allocator.terminations)
	}
	if _, err := h.Hold(t.Context(), monStoreEndpoint{ip: "127.0.0.2", ports: []int{41000, 41001}}); err == nil || len(h.reserved) != 1 {
		t.Fatal("another address was held")
	}
	h.reserve = func(context.Context, int, []int) (*hostPortLease, error) { return nil, errors.New("port 41000 in use") }
	if hold, err := h.Hold(t.Context(), monStoreEndpoint{ip: "127.0.0.1", ports: []int{41000, 41001}}); err == nil || hold != nil {
		t.Fatalf("a failed hold returned %v %v", hold, err)
	}
}

func TestHostMonitorObserveRequiresTheCreatedPorts(t *testing.T) {
	h := newHostGenerations(t)
	id, err := h.Create(t.Context(), monStoreLaunch{volume: "v", mode: monStoreFormat, generation: 1}, "img")
	if err != nil {
		t.Fatal(err)
	}
	h.created[id] = generationDouble{id: id}
	h.listeners = []monitorListener{{IP: "127.0.0.1", Port: 41001}, {IP: "127.0.0.1", Port: 41000}}
	got, err := h.Observe(t.Context(), id)
	if err != nil || !got.equal(monStoreEndpoint{ip: "127.0.0.1", ports: []int{41000, 41001}}) {
		t.Fatalf("observe: %+v %v", got, err)
	}
	for name, listeners := range map[string][]monitorListener{
		"missing port":  {{IP: "127.0.0.1", Port: 41000}},
		"extra port":    {{IP: "127.0.0.1", Port: 41000}, {IP: "127.0.0.1", Port: 41001}, {IP: "127.0.0.1", Port: 6800}},
		"other port":    {{IP: "127.0.0.1", Port: 41000}, {IP: "127.0.0.1", Port: 3300}},
		"wildcard":      {{IP: "0.0.0.0", Port: 41000}, {IP: "0.0.0.0", Port: 41001}},
		"other address": {{IP: "127.0.0.2", Port: 41000}, {IP: "127.0.0.2", Port: 41001}},
		"nothing":       nil,
	} {
		h.listeners = listeners
		if _, err := h.Observe(t.Context(), id); err == nil {
			t.Errorf("%s: accepted %v", name, listeners)
		}
	}
	if _, err := h.Observe(t.Context(), "unknown"); err == nil || !strings.Contains(err.Error(), "not created") {
		t.Fatalf("an unknown container was observed: %v", err)
	}
}
