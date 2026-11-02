package ceph

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/testcontainers/testcontainers-go"
)

// Only Terminate supplies an injected teardown failure; inventory must only
// copy tracked handles without invoking Docker methods.
type inventoryHandle struct {
	testcontainers.Container
	terminationError error
}

func (h *inventoryHandle) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	return h.terminationError
}

func TestObserverServiceInventoryAfterFailedTerminationAndRetry(t *testing.T) {
	failure := errors.New("injected service teardown failure")
	service := &inventoryHandle{terminationError: failure}
	cluster := &Container{}
	cluster.members.roster.setService("rgw.a", service)
	observer, _ := Observe(cluster)
	if err := cluster.Terminate(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("termination failure not retained: %v", err)
	}
	services, err := observer.ServiceContainers(t.Context())
	if err != nil || !slices.Equal(services, []testcontainers.Container{service}) {
		t.Fatalf("terminated cluster hid retained handle: %v %v", services, err)
	}
	service.terminationError = nil
	if err := cluster.Terminate(t.Context()); err != nil {
		t.Fatal(err)
	}
	services, err = observer.ServiceContainers(t.Context())
	if err != nil || len(services) != 0 {
		t.Fatalf("successful teardown did not forget handle: %v %v", services, err)
	}
}

func TestObserverServiceInventoryPreservesTrackedHandlesWithoutLiveness(t *testing.T) {
	first, second := &inventoryHandle{}, &inventoryHandle{}
	cluster := &Container{}
	cluster.members.roster.setService("rgw.z", second)
	cluster.members.roster.setService("mds.a", first)
	observer, err := Observe(cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, closed := range []bool{false, true} {
		cluster.closed = closed // Partial startup and retained post-termination records.
		services, err := observer.ServiceContainers(t.Context())
		if err != nil || !slices.Equal(services, []testcontainers.Container{first, second}) {
			t.Fatalf("closed=%v: inventory=%v err=%v", closed, services, err)
		}
		services[0] = second
		again, err := observer.ServiceContainers(t.Context())
		if err != nil || again[0] != first {
			t.Fatalf("inventory slice aliases registry: %v %v", again, err)
		}
	}
	cluster.members.roster.dropService("mds.a")
	services, err := observer.ServiceContainers(t.Context())
	if err != nil || !slices.Equal(services, []testcontainers.Container{second}) {
		t.Fatalf("forgotten handle still listed: %v %v", services, err)
	}
}

func TestObserverServiceInventoryDistinguishesEmptyUnavailableAndCanceled(t *testing.T) {
	cluster := &Container{}
	observer, _ := Observe(cluster)
	services, err := observer.ServiceContainers(t.Context())
	if err != nil || services == nil || len(services) != 0 {
		t.Fatalf("empty tracked roster: %v %v", services, err)
	}
	cluster.members.roster.setService("unknown", nil)
	services, err = observer.ServiceContainers(t.Context())
	if err == nil || services != nil {
		t.Fatalf("missing handle became inventory success: %v %v", services, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	services, err = observer.ServiceContainers(ctx)
	if !errors.Is(err, context.Canceled) || services != nil {
		t.Fatalf("canceled inventory: %v %v", services, err)
	}
}

func TestObserverServiceInventoryLockWaitIsContextBounded(t *testing.T) {
	cluster := &Container{}
	observer, _ := Observe(cluster)
	waitContextHeldGate(t, cluster.mu.Lock, cluster.mu.Unlock, cluster.mu.TryLock, func(ctx context.Context) error {
		_, err := observer.ServiceContainers(ctx)
		return err
	})
}

func TestObserverInventoryAndWaitsRefuseUnconstructedObservers(t *testing.T) {
	for _, observer := range []*Observer{nil, {}} {
		services, err := observer.ServiceContainers(t.Context())
		if services != nil || !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid inventory: %v %v", services, err)
		}
		waits := []func(context.Context) error{observer.WaitForClean, observer.WaitForPGClean, observer.WaitForQuorum,
			func(ctx context.Context) error { return observer.WaitMGRModuleReady(ctx, "volumes") }}
		for _, wait := range waits {
			if err := wait(t.Context()); !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("invalid wait: %v", err)
			}
		}
	}
}
