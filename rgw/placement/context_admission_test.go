package placement

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// Every placement operation waits for the cluster mutex only until the
// caller's context ends. A queued call sends no native command, restarts no
// gateway and changes no placement state, leaves no waiter behind, and the
// same call succeeds with a fresh context afterwards.
func TestRGWPlacementOperationsWaitForTheClusterOnlyUntilTheContextEnds(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		g, f := newRGWPlacementFixture()
		clustertest.AssertContextLockQueued(t, testhelper.TestState(g.Cluster).Mutex, func(ctx context.Context) error {
			_, err := create(ctx, g.Core, testConfig())
			return err
		})
		if len(f.Calls) != 0 {
			t.Fatalf("a queued Create reached native commands: %v", f.Calls)
		}
		if p, err := create(t.Context(), g.Core, testConfig()); err != nil || !p.confirmed {
			t.Fatalf("a fresh context after the queued deadline: %v", err)
		}
	})
	g, f, p, user := newRGWUserPlacementFixture(t)
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"Status", func(ctx context.Context) error { _, err := p.Status(ctx); return err }},
		{"Apply", func(ctx context.Context) error { return apply(ctx, g.Core, p) }},
		{"Reload", func(ctx context.Context) error { return reload(ctx, g.Core, p) }},
		{"SetUserDefault", func(ctx context.Context) error {
			return setUserDefault(ctx, p, user, UserDefault{StorageClass: "STANDARD_IA"})
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			calls, stops, starts := len(f.Calls), f.StopCount, f.StartCount
			defaultPlacement := f.user["default_placement"]
			clustertest.AssertContextLockQueued(t, testhelper.TestState(g.Cluster).Mutex, operation.call)
			if len(f.Calls) != calls || f.StopCount != stops || f.StartCount != starts || f.user["default_placement"] != defaultPlacement || !p.confirmed {
				t.Fatalf("a queued call reached native commands, restarted the gateway or changed state: %v", f.Calls[calls:])
			}
			if err := operation.call(t.Context()); err != nil {
				t.Fatalf("a fresh context after the queued deadline: %v", err)
			}
		})
	}
	if f.user["default_placement"] != "tiered" {
		t.Fatal("the fresh SetUserDefault did not set the user default")
	}
}
