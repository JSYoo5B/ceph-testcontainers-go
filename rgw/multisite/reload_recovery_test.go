package multisite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// reloadGatewayModel records a finite Docker request and generation history.
// A stopped state can be observed before its pending Stop callback completes.
type reloadGatewayModel struct {
	testcontainers.Container
	id, generation        string
	running               bool
	stopError, startError error
	startApplied          bool
	stopCanceled          context.CancelFunc
	stopRelease           <-chan struct{}
	stops, starts         int
}

func (m *reloadGatewayModel) GetContainerID() string { return m.id }
func (m *reloadGatewayModel) State(context.Context) (*container.State, error) {
	return &container.State{Running: m.running, StartedAt: m.generation}, nil
}
func (m *reloadGatewayModel) Stop(context.Context, *time.Duration) error {
	m.stops++
	m.running = false
	if m.stopCanceled != nil {
		m.stopCanceled()
		<-m.stopRelease
	}
	return m.stopError
}
func (m *reloadGatewayModel) Start(context.Context) error {
	m.starts++
	if m.startError == nil || m.startApplied {
		m.running = true
		m.generation = "reloaded"
	}
	return m.startError
}

func TestZoneReloadWaitsForCanceledStopCompletion(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, stopCanceled: cancel, stopRelease: release}
	zone := &rgwZoneState{Zone: Zone{Name: "a", Gateway: &rgw.Gateway{Container: m}}}
	topology := &rgwZoneTopology{}
	if err := topology.reloadZoneGateways(ctx, []*rgwZoneState{zone}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("caller cancellation: %v", err)
	}
	// Stop changed Running=false, but has not completed its request.
	if m.running {
		t.Fatal("test Stop did not stop the generation")
	}
	retry, end := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer end()
	if err := topology.reloadZoneGateways(retry, []*rgwZoneState{zone}, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unfinished Stop was inferred complete from stopped state: %v", err)
	}
	if m.stops != 1 || m.starts != 0 {
		t.Fatalf("overlapping reload: stops=%d starts=%d", m.stops, m.starts)
	}
	finish()
	if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
		t.Fatal(err)
	}
	if !m.running || m.stops != 1 || m.starts != 1 || len(topology.reloadNeeded) != 0 {
		t.Fatalf("recovery state: %+v", m)
	}
}

func TestZoneReloadTransportLossRetainsStopFence(t *testing.T) {
	lost := errors.New("Docker response lost")
	m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, stopError: lost}
	zone := &rgwZoneState{Zone: Zone{Name: "a", Gateway: &rgw.Gateway{Container: m}}}
	topology := &rgwZoneTopology{}
	if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	for _, generation := range []string{"initial", "external"} {
		m.generation = generation
		if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); !errors.Is(err, lost) {
			t.Fatalf("lost Stop reply settled: %v", err)
		}
		if m.running || m.stops != 1 || m.starts != 0 {
			t.Fatalf("uncertain stop overlapped: %+v", m)
		}
	}
}

func TestZoneReloadRecoversInterruptedStart(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			lost := errors.New("Start response lost")
			m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, startError: lost, startApplied: applied}
			zone := &rgwZoneState{Zone: Zone{Name: "a", Gateway: &rgw.Gateway{Container: m}}}
			topology := &rgwZoneTopology{}
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); !errors.Is(err, lost) {
				t.Fatal(err)
			}
			m.startError = nil
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
				t.Fatal(err)
			}
			want := 2
			if applied {
				want = 1
			}
			if !m.running || m.stops != 1 || m.starts != want || len(topology.reloadNeeded) != 0 {
				t.Fatalf("Start recovery: %+v", m)
			}
		})
	}
}

func TestZoneReloadPreservesExternalFencesAndReplacement(t *testing.T) {
	for _, change := range []string{"generation", "container", "descriptor"} {
		t.Run(change, func(t *testing.T) {
			m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, startError: errors.New("start refused")}
			zone := &rgwZoneState{Zone: Zone{Name: "a", Gateway: &rgw.Gateway{Container: m}}}
			topology := &rgwZoneTopology{}
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err == nil {
				t.Fatal("failed Start accepted")
			}
			m.startError = nil
			switch change {
			case "generation":
				m.generation = "external"
			case "container":
				m.id = "replacement"
			case "descriptor":
				m = &reloadGatewayModel{id: "replacement", generation: "external"}
				zone.Gateway = &rgw.Gateway{Container: m}
			}
			starts, stops := m.starts, m.stops
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err == nil {
				t.Fatal("external fence was accepted as pending reload")
			}
			if m.running || m.starts != starts || m.stops != stops {
				t.Fatalf("external gateway changed: %+v", m)
			}
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
				t.Fatal(err)
			}
			if m.running || m.starts != starts {
				t.Fatal("later reload revived external fence")
			}
		})
	}
}
