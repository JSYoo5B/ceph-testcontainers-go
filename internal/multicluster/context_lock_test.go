package multicluster

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

func TestResourcesCleanupNestedGateDeadlinePreservesRetry(t *testing.T) {
	probe := &clustertest.NativeCallCounter{}
	var owned Resources
	owned.AddContainer(probe)
	operation := func(ctx context.Context) error { return owned.Terminate(ctx) }
	clustertest.AssertContextLockQueued(t, &owned.mu, operation)
	if probe.Terminations.Load() != 0 || owned.actions[0].done {
		t.Fatal("nested deadline lost cleanup ownership")
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := operation(ctx); err != nil || probe.Terminations.Load() != 1 {
		t.Fatalf("nested cleanup could not retry with fresh context: %v", err)
	}
}
