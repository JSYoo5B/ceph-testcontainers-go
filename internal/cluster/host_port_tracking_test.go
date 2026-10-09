package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func trackedHostPortFixture() *Container {
	return &Container{settings: options{hostNetwork: true, controlImage: "fixture-control", publicAddress: "127.0.0.1", startupTimeout: time.Second}}
}

func TestTrackedHostPortsRejectBusyOwnerBeforeAllocation(t *testing.T) {
	cluster := trackedHostPortFixture()
	var allocations atomic.Int32
	assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error {
		lease, err := cluster.reserveTrackedHostPortsWith(ctx, 1, func(context.Context, string, string, int, time.Duration) (*hostPortLease, error) {
			allocations.Add(1)
			return nil, errors.New("allocator must not run while owner is busy")
		})
		if lease != nil {
			t.Error("failed admission returned an allocator")
		}
		return err
	})
	if allocations.Load() != 0 || len(cluster.portLeases) != 0 || cluster.closed {
		t.Fatal("busy owner admission allocated or changed cleanup inventory")
	}
}

func TestTrackedHostPortsRejectClosedAndCanceledClusterBeforeAllocation(t *testing.T) {
	for _, condition := range []string{"closed", "canceled"} {
		t.Run(condition, func(t *testing.T) {
			cluster := trackedHostPortFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if condition == "closed" {
				cluster.closed = true
			} else {
				cancel()
			}
			allocations := 0
			lease, err := cluster.reserveTrackedHostPortsWith(ctx, 1, func(context.Context, string, string, int, time.Duration) (*hostPortLease, error) {
				allocations++
				return nil, errors.New("unexpected allocator")
			})
			if lease != nil || err == nil || allocations != 0 || len(cluster.portLeases) != 0 {
				t.Fatalf("rejected admission allocated: lease=%v err=%v calls=%d", lease, err, allocations)
			}
			if condition == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("caller cancellation was lost: %v", err)
			}
		})
	}
}

func TestTrackedHostPortsRetainNativeErrorAndCanceledPartialLease(t *testing.T) {
	for _, result := range []string{"native-error", "canceled-success", "canceled-native-error", "success"} {
		t.Run(result, func(t *testing.T) {
			cluster := trackedHostPortFixture()
			allocator := &hostNetworkFixtureContainer{failTerminationOnce: true}
			lease := &hostPortLease{Ports: []int{42791, 42792}, ctr: allocator}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			nativeErr := errors.New("reservation cleanup could not remove allocator")
			got, err := cluster.reserveTrackedHostPortsWith(ctx, 2, func(_ context.Context, image, address string, count int, timeout time.Duration) (*hostPortLease, error) {
				if image != cluster.settings.controlImage || address != cluster.PublicAddress() || count != 2 || timeout != cluster.settings.startupTimeout {
					t.Error("reservation parameters changed")
				}
				if cluster.mu.TryLock() {
					cluster.mu.Unlock()
					t.Error("allocation did not hold topology owner")
				}
				if result == "canceled-success" || result == "canceled-native-error" {
					cancel()
				}
				if result == "native-error" || result == "canceled-native-error" {
					return lease, nativeErr
				}
				return lease, nil
			})
			if got != lease || len(cluster.portLeases) != 1 || cluster.portLeases[0] != lease || allocator.terminations != 0 {
				t.Fatal("partial reservation lost lease identity or attempted unowned cleanup")
			}
			if !cluster.mu.TryLock() {
				t.Fatal("reservation retained owner after returning")
			}
			cluster.mu.Unlock()
			if (result == "native-error" || result == "canceled-native-error") && !errors.Is(err, nativeErr) {
				t.Fatalf("native reservation cause was lost: %v", err)
			}
			if (result == "canceled-success" || result == "canceled-native-error") && !errors.Is(err, context.Canceled) {
				t.Fatalf("post-allocation cancellation was swallowed: %v", err)
			}
			if result == "success" && err != nil {
				t.Fatal(err)
			}
			// The caller may have a canceled operation context. Fresh fixture
			// cleanup retries only its tracked allocator, including native errors.
			if err := cluster.Terminate(t.Context()); err == nil || allocator.terminations != 1 || lease.ctr != allocator {
				t.Fatalf("failed cleanup lost ownership: calls=%d err=%v", allocator.terminations, err)
			}
			if err := cluster.Terminate(t.Context()); err != nil || allocator.terminations != 2 || lease.ctr != nil {
				t.Fatalf("fresh cleanup could not retry tracked allocator: calls=%d err=%v", allocator.terminations, err)
			}
			if err := cluster.Terminate(t.Context()); err != nil || allocator.terminations != 2 {
				t.Fatalf("successful allocator cleanup repeated: calls=%d err=%v", allocator.terminations, err)
			}
		})
	}
}

func TestTrackedHostPortsNilFailureAndMissingSuccessDoNotPublishLease(t *testing.T) {
	for _, failure := range []bool{true, false} {
		cluster := trackedHostPortFixture()
		nativeErr := errors.New("allocation failed before any retained resource")
		got, err := cluster.reserveTrackedHostPortsWith(t.Context(), 1, func(context.Context, string, string, int, time.Duration) (*hostPortLease, error) {
			if failure {
				return nil, nativeErr
			}
			return nil, nil
		})
		if got != nil || err == nil || len(cluster.portLeases) != 0 {
			t.Fatal("nil reservation published ownership or reported success")
		}
		if failure && !errors.Is(err, nativeErr) {
			t.Fatalf("pre-allocation native cause was lost: %v", err)
		}
	}
}

func TestTrackedHostPortsPublishBeforeConcurrentTerminationSnapshot(t *testing.T) {
	cluster := trackedHostPortFixture()
	allocator := &hostNetworkFixtureContainer{}
	lease := &hostPortLease{Ports: []int{42791}, ctr: allocator}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	allowReturn := func() { releaseOnce.Do(func() { close(release) }) }
	defer allowReturn()
	type result struct {
		lease *hostPortLease
		err   error
	}
	reservationDone := make(chan result, 1)
	reservationExited := make(chan struct{})
	var cleanupExited chan struct{}
	var stopCleanup context.CancelFunc
	t.Cleanup(func() {
		cancel()
		allowReturn()
		if stopCleanup != nil {
			stopCleanup()
		}
		for _, done := range []chan struct{}{reservationExited, cleanupExited} {
			if done == nil {
				continue
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error("allocator/cleanup worker did not exit after fixture release")
			}
		}
	})
	go func() {
		defer close(reservationExited)
		got, err := cluster.reserveTrackedHostPortsWith(ctx, 1, func(context.Context, string, string, int, time.Duration) (*hostPortLease, error) {
			close(entered)
			<-release
			return lease, nil
		})
		reservationDone <- result{got, err}
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("allocator did not enter")
	}
	cancel()
	cleanupCtx, stop := context.WithTimeout(t.Context(), time.Second)
	stopCleanup = stop
	defer stop()
	cleanupDone := make(chan error, 1)
	cleanupExited = make(chan struct{})
	go func() {
		defer close(cleanupExited)
		cleanupDone <- cluster.Terminate(cleanupCtx)
	}()
	select {
	case err := <-cleanupDone:
		t.Fatalf("termination completed before allocator publication: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	allowReturn()
	select {
	case outcome := <-reservationDone:
		if outcome.lease != lease || !errors.Is(outcome.err, context.Canceled) {
			t.Fatalf("reservation swallowed ownership/cancellation: %v", outcome.err)
		}
	case <-time.After(time.Second):
		t.Fatal("allocator publication remained blocked after native return")
	}
	select {
	case err := <-cleanupDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("termination could not consume tracked allocator")
	}
	if len(cluster.portLeases) != 1 || cluster.portLeases[0] != lease || lease.ctr != nil || allocator.terminations != 1 || !cluster.closed {
		t.Fatal("termination missed a late-created allocator or lost lease identity")
	}
}

func TestTrackedHostRGWStartRejectsBusyOwnerBeforeAllocator(t *testing.T) {
	cluster := trackedHostPortFixture()
	assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error {
		gateway, err := cluster.startRGW(ctx, RGWConfig{SkipUserCreation: true})
		if gateway != nil {
			t.Error("busy host owner returned a gateway")
		}
		return err
	})
	if len(cluster.portLeases) != 0 || len(cluster.services) != 0 || len(cluster.gateways) != 0 {
		t.Fatal("busy host startup created container or cleanup inventory")
	}
}
