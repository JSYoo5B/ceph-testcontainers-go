package fault

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
)

// closingTarget is a caller-owned target whose network inspection overlaps
// a Terminate: the cluster closes between the admission and the reservation.
type closingTarget struct {
	contextLockClient
	c *ceph.Container
}

func (target closingTarget) Networks(context.Context) ([]string, error) {
	view := servicehook.TestState(target.c)
	view.Mutex.Lock()
	*view.Closed = true
	view.Mutex.Unlock()
	return []string{target.c.NetworkName()}, nil
}

func TestNetworkDelayReservationRefusesClusterClosedDuringAdmission(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{NetworkName: "public"})
	_, terminated := faulthook.Terminated()
	if _, token, err := reserveNetworkDelay(t.Context(), c, closingTarget{c: c}, ceph.PublicNetworkPlane); !errors.Is(err, terminated) || token != (faulthook.Token{}) || len(faulthook.TestFaults(c, faulthook.NetworkDelay)) != 0 {
		t.Fatalf("a delay was reserved on a cluster closed meanwhile: %v", err)
	}
}

// The public package-level entry points must gate before validating zero
// requests or touching Docker, just as their former Container methods did.
func TestPublicFaultsRefuseSharedCluster(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{Shared: true})
	for name, apply := range map[string]func() error{
		"fault.TemporaryNetworkDelay": func() error { _, err := TemporaryNetworkDelay(t.Context(), c, nil, NetworkDelay{}); return err },
		"fault.TemporaryBlocklist":    func() error { _, err := TemporaryBlocklist(t.Context(), c, "", time.Minute); return err },
		"fault.Pause":                 func() error { _, err := Pause(t.Context(), c, nil); return err },
		"fault.InterruptNetwork":      func() error { _, err := InterruptNetwork(t.Context(), c, nil, ""); return err },
	} {
		err := apply()
		var op *ceph.OperationError
		if !errors.Is(err, ceph.ErrSharedCluster) || !errors.As(err, &op) || op.Op != name {
			t.Errorf("%s bypassed admission: %v", name, err)
		}
	}
}

func TestClosedClusterRefusesNetworkDelayReservation(t *testing.T) {
	c := servicehook.TestCluster(nil, servicehook.TestConfig{Closed: true})
	_, terminated := faulthook.Terminated()
	if _, _, err := reserveNetworkDelay(t.Context(), c, contextLockClient{}, ceph.PublicNetworkPlane); !errors.Is(err, terminated) {
		t.Fatalf("closed cluster admitted a delay: %v", err)
	}
}
