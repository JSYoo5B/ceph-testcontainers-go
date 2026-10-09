//go:build all || (integration && topology && multicluster)

package integration_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// A private test wrapper holds a real public mutation before any native work.
// All other native/container mutation attempts are rejected, including during
// regression drainage, so a broken queued consumer cannot create resources.
var errTopologySnapshotBarrier = errors.New("test topology owner barrier")

type topologySnapshotBarrier struct {
	testcontainers.Container
	hold                      bool
	entered, release          chan struct{}
	once                      sync.Once
	queries, copies, cleanups atomic.Int32
}

func (barrier *topologySnapshotBarrier) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if barrier.hold && slices.Equal(args, []string{"ceph-authtool", "--gen-print-key"}) {
		barrier.once.Do(func() { close(barrier.entered) })
		select {
		case <-barrier.release:
			return 0, nil, errTopologySnapshotBarrier
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	barrier.queries.Add(1)
	return 0, nil, errTopologySnapshotBarrier
}

func (barrier *topologySnapshotBarrier) CopyToContainer(context.Context, []byte, string, int64) error {
	barrier.copies.Add(1)
	return errTopologySnapshotBarrier
}

func (barrier *topologySnapshotBarrier) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	barrier.cleanups.Add(1)
	return errTopologySnapshotBarrier
}

// All embedded handle assignments happen before workers start and after they
// join. The wrapper is for default single-MON fixtures only, whose stable CLI
// handle is the embedded container. Never use it after ensureControlPlane.
func topologySnapshotHoldOwner(t *testing.T, ctx context.Context, clusters []*ceph.Container, busy *ceph.Container) (func(), func(), []*topologySnapshotBarrier) {
	t.Helper()
	originals := make([]testcontainers.Container, len(clusters))
	for i, cluster := range clusters {
		originals[i] = cluster.Container
		if cluster.ControlContainer() != originals[i] {
			t.Fatal("barrier requires a default single-MON control handle")
		}
	}
	if !slices.Contains(clusters, busy) {
		t.Fatal("busy owner must belong to the supplied fixture pair")
	}
	wrappers := make([]*topologySnapshotBarrier, len(clusters))
	var blocker *topologySnapshotBarrier
	for i, cluster := range clusters {
		wrappers[i] = &topologySnapshotBarrier{Container: originals[i], hold: cluster == busy, entered: make(chan struct{}), release: make(chan struct{})}
		if cluster == busy {
			blocker = wrappers[i]
		}
		cluster.Container = wrappers[i]
	}
	ownerCtx, cancel := context.WithTimeout(ctx, time.Minute)
	type outcome struct {
		osd *ceph.OSDContainer
		err error
	}
	done := make(chan outcome, 1)
	go func() { osd, err := busy.AddOSD(ownerCtx); done <- outcome{osd, err} }()
	var releaseOnce sync.Once
	releaseOwner := func() { releaseOnce.Do(func() { close(blocker.release) }) }
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			releaseOwner()
			select {
			case result := <-done:
				if result.osd != nil || !errors.Is(result.err, errTopologySnapshotBarrier) {
					t.Errorf("owner unexpectedly created native/container state: osd=%v err=%v", result.osd, result.err)
				}
			case <-time.After(3 * time.Second):
				t.Error("owner did not leave the released pre-native barrier")
			}
			cancel()
			for i, cluster := range clusters {
				cluster.Container = originals[i]
			}
		})
	}
	select {
	case <-blocker.entered:
		return releaseOwner, finish, wrappers
	case result := <-done:
		cancel()
		for i, cluster := range clusters {
			cluster.Container = originals[i]
		}
		t.Fatalf("owner did not reach the pre-native barrier: osd=%v error=%v", result.osd, result.err)
	case <-time.After(5 * time.Second):
		finish()
		t.Fatal("owner did not enter the pre-native barrier")
	}
	return nil, nil, nil
}

type topologySnapshotPartial interface {
	Terminate(context.Context, ...testcontainers.TerminateOption) error
}

func topologySnapshotQueue(t *testing.T, releaseOwner func(), operation func(context.Context) (topologySnapshotPartial, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	type outcome struct {
		fixture topologySnapshotPartial
		err     error
	}
	done := make(chan outcome, 1)
	go func() { fixture, err := operation(ctx); done <- outcome{fixture, err} }()
	cleanupPartial := func(result outcome) {
		if result.fixture == nil {
			return
		}
		releaseOwner()
		cleanup := func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if err := result.fixture.Terminate(cleanupCtx); err != nil {
				t.Errorf("cleanup unexpected partial constructor result: %v", err)
			}
		}
		// A failed future constructor must not lose a non-nil ownership handle.
		// This cleanup runs after the test body releases the owner barrier.
		t.Cleanup(cleanup)
	}
	select {
	case result := <-done:
		cleanupPartial(result)
		if result.fixture != nil || !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("busy consumer published state or lost caller deadline: published=%v error=%v", result.fixture != nil, result.err)
		}
	case <-time.After(400 * time.Millisecond):
		// Drain while wrappers still reject native work. Restore occurs only
		// in the parent's deferred finish, after this queued worker joins.
		releaseOwner()
		select {
		case result := <-done:
			cleanupPartial(result)
		case <-time.After(3 * time.Second):
		}
		t.Fatal("consumer remained queued past the caller deadline")
	}
}
