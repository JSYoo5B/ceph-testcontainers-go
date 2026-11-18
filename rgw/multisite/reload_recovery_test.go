package multisite

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// reloadGatewayModel records a finite Docker request and generation history.
// A pending stop affects only its original generation when the test settles it.
type reloadGatewayModel struct {
	testcontainers.Container
	id, generation            string
	running                   bool
	stopError, startError     error
	stopPending, startApplied bool
	stops, starts             int
}

func (m *reloadGatewayModel) GetContainerID() string { return m.id }
func (m *reloadGatewayModel) State(context.Context) (*container.State, error) {
	return &container.State{Running: m.running, StartedAt: m.generation}, nil
}
func (m *reloadGatewayModel) Stop(context.Context, *time.Duration) error {
	m.stops++
	if !m.stopPending {
		m.running = false
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

func TestZoneReloadRecoversLostStopAndStartReplies(t *testing.T) {
	for _, action := range []string{"stop", "pending-stop", "start", "applied-start"} {
		t.Run(action, func(t *testing.T) {
			m := &reloadGatewayModel{id: "owned", generation: "initial", running: true}
			zone := &rgwZoneState{Zone: Zone{Name: "a", Gateway: &rgw.Gateway{Container: m}}}
			topology := &rgwZoneTopology{}
			lost := errors.New("reply lost")
			switch action {
			case "stop":
				m.stopError = lost
			case "pending-stop":
				m.stopError = lost
				m.stopPending = true
			case "start":
				m.startError = lost
			case "applied-start":
				m.startError = lost
				m.startApplied = true
			}
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); !errors.Is(err, lost) {
				t.Fatalf("lost reply: %v", err)
			}
			if m.stopPending {
				if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err == nil {
					t.Fatal("pending stop accepted")
				}
				if m.stops != 1 || m.starts != 0 {
					t.Fatalf("pending stop overlapped: stops=%d starts=%d", m.stops, m.starts)
				}
				m.running = false
				m.stopPending = false
			}
			m.stopError = nil
			m.startError = nil
			if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
				t.Fatal(err)
			}
			if !m.running || m.stops != 1 || len(topology.reloadNeeded) != 0 {
				t.Fatalf("recovery state: %+v", m)
			}
			wantStarts := 1
			if action == "start" {
				wantStarts = 2
			}
			if m.starts != wantStarts {
				t.Fatalf("starts=%d want=%d", m.starts, wantStarts)
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
