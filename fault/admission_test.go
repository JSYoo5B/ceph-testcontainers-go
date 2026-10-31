package fault

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
)

// closingTarget is a caller-owned target whose network inspection overlaps
// a Terminate: the cluster closes between the admission and the reservation.
type closingTarget struct {
	contextLockClient
	c *ceph.Container
}

func (target closingTarget) Networks(context.Context) ([]string, error) {
	view := testhelper.TestState(target.c)
	view.Mutex.Lock()
	testhelper.TestSetClosed(target.c, true)
	view.Mutex.Unlock()
	facts, err := ceph.FactsOf(target.c)
	return []string{facts.NetworkName}, err
}

func TestNetworkDelayReservationRefusesClusterClosedDuringAdmission(t *testing.T) {
	c := testhelper.TestCluster(nil, testhelper.TestConfig{NetworkName: "public"})
	_, terminated := faulthook.Terminated()
	name, err := admitNetworkDelayTarget(t.Context(), c, closingTarget{c: c}, ceph.PublicNetworkPlane)
	var token faulthook.Token
	if err == nil {
		token, err = reserveNetworkDelay(t.Context(), c, name, "osd-0")
	}
	if !errors.Is(err, terminated) || token != (faulthook.Token{}) || len(testhelper.TestFaults(c, faulthook.NetworkDelay)) != 0 {
		t.Fatalf("a delay was reserved on a cluster closed meanwhile: %v", err)
	}
}

func TestClosedClusterRefusesNetworkDelayReservation(t *testing.T) {
	c := testhelper.TestCluster(nil, testhelper.TestConfig{Closed: true})
	_, terminated := faulthook.Terminated()
	if _, err := admitNetworkDelayTarget(t.Context(), c, contextLockClient{}, ceph.PublicNetworkPlane); !errors.Is(err, terminated) {
		t.Fatalf("closed cluster admitted a delay: %v", err)
	}
}
