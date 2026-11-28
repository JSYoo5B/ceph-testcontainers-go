package multisite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/internal/gwbase"
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
		m.generation = fmt.Sprintf("reloaded-%d", m.starts)
	}
	return m.startError
}

// reloadZone is zone "a" of a synthetic cluster whose gateway container is
// m, and the topology that reloads it.
func reloadZone(m *reloadGatewayModel) (*rgwZoneState, *rgwZoneTopology) {
	cluster := testhelper.TestCluster(nil, testhelper.TestConfig{})
	return &rgwZoneState{Zone: Zone{Name: "a", Gateway: &rgw.Gateway{Container: m}}, cluster: cluster}, &rgwZoneTopology{}
}

// pending reports whether the zone's gateway container still has a restart
// record.
func pending(zone *rgwZoneState) bool {
	return gwbase.RestartTarget{Cluster: zone.cluster, Container: zone.Gateway.Container}.Pending()
}

func TestZoneReloadWaitsForCanceledStopCompletion(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	finish := func() { once.Do(func() { close(release) }) }
	t.Cleanup(finish)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, stopCanceled: cancel, stopRelease: release}
	zone, topology := reloadZone(m)
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
	if !m.running || m.stops != 1 || m.starts != 1 || pending(zone) {
		t.Fatalf("recovery state: %+v", m)
	}
}

// A Stop whose reply was lost is decided by the run state: while the
// original generation runs, no second restart starts; once it has stopped,
// the next reload starts it.
func TestZoneReloadDecidesALostStopReplyByTheGeneration(t *testing.T) {
	lost := errors.New("Docker response lost")
	m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, stopError: lost}
	zone, topology := reloadZone(m)
	m.stopCanceled = func() { m.running = true }
	m.stopRelease = closedChannel()
	if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); !errors.Is(err, lost) {
		t.Fatal(err)
	}
	for range 2 {
		if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err == nil {
			t.Fatal("a restart started while the original generation ran on")
		}
		if !m.running || m.stops != 1 || m.starts != 0 || !pending(zone) {
			t.Fatalf("uncertain stop overlapped: %+v", m)
		}
	}
	m.running = false
	if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
		t.Fatal(err)
	}
	if !m.running || m.stops != 1 || m.starts != 1 || pending(zone) {
		t.Fatalf("the stopped generation was not resumed once: %+v", m)
	}
}

func closedChannel() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

func TestZoneReloadRecoversInterruptedStart(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			lost := errors.New("Start response lost")
			m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, startError: lost, startApplied: applied}
			zone, topology := reloadZone(m)
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
			if !m.running || m.stops != 1 || m.starts != want || pending(zone) {
				t.Fatalf("Start recovery: %+v", m)
			}
		})
	}
}

func TestZoneReloadPreservesExternalFencesAndReplacement(t *testing.T) {
	for _, change := range []string{"generation", "container", "descriptor"} {
		t.Run(change, func(t *testing.T) {
			m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, startError: errors.New("start refused")}
			zone, topology := reloadZone(m)
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
			err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil)
			// The record belongs to the container: another generation of
			// it was stopped outside the reload, while a replaced container
			// has no record and stays as it is.
			if (change == "generation") != (err != nil) {
				t.Fatalf("an external fence returned %v", err)
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

// TestZoneReloadResumesAPendingRestartOfAnotherRequester leaves a restart
// that another requester, as a placement activation does, stopped without a
// start. The reload resumes it, with the requester's check before Start,
// instead of stopping the gateway a second time; a restart of the other
// requester that already runs is followed by the reload's own.
func TestZoneReloadResumesAPendingRestartOfAnotherRequester(t *testing.T) {
	m := &reloadGatewayModel{id: "owned", generation: "initial", running: true, startError: errors.New("start refused")}
	zone, topology := reloadZone(m)
	checked := 0
	other := gwbase.Restart{Owner: "placement", BeforeStart: func(context.Context) error { checked++; return nil }, AwaitCompletion: true}
	target := gwbase.RestartTarget{Cluster: zone.cluster, Container: m}
	if err := target.Restart(t.Context(), other); err == nil || m.running {
		t.Fatalf("the other restart did not stay stopped: %v", err)
	}
	m.startError = nil
	if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
		t.Fatal(err)
	}
	if !m.running || m.stops != 1 || m.starts != 2 || checked != 1 {
		t.Fatalf("the pending restart was not resumed once: stops=%d starts=%d checks=%d", m.stops, m.starts, checked)
	}
	// The started generation still waits for its requester's verification.
	resumed, err := target.Resume(t.Context())
	if err != nil || resumed.Owner != "placement" || !resumed.Running || !pending(zone) {
		t.Fatalf("the other requester's verification was lost: %+v, %v", resumed, err)
	}
	if err := topology.reloadZoneGateways(t.Context(), []*rgwZoneState{zone}, nil); err != nil {
		t.Fatal(err)
	}
	if m.stops != 2 || m.starts != 3 || pending(zone) {
		t.Fatalf("a later reload did not restart once: stops=%d starts=%d", m.stops, m.starts)
	}
}
