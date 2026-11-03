package account

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// Every account operation waits for the cluster mutex only until the caller's
// context ends. A queued call sends no native command and changes no account
// state, leaves no waiter behind, and the same call succeeds with a fresh
// context afterwards.
func TestAccountOperationsWaitForTheClusterOnlyUntilTheContextEnds(t *testing.T) {
	t.Run("Create", func(t *testing.T) {
		g, f := newAccountTestGateway()
		config := Config{ID: "RGW12345678901234567", Name: "fixture account"}
		clustertest.AssertContextLockQueued(t, testhelper.TestState(g.Cluster).Mutex, func(ctx context.Context) error {
			_, err := create(ctx, g.Core, config)
			return err
		})
		if len(f.Calls) != 0 || f.Account != nil {
			t.Fatalf("a queued Create reached native commands: %v", f.Calls)
		}
		if a, err := create(t.Context(), g.Core, config); err != nil || !a.State.Confirmed {
			t.Fatalf("a fresh context after the queued deadline: %v", err)
		}
	})
	g, f := newAccountTestGateway()
	created, err := create(t.Context(), g.Core, Config{ID: "RGW12345678901234567", Name: "fixture account"})
	if err != nil {
		t.Fatal(err)
	}
	a := newAccount(created)
	q := rgw.Quota{Enabled: true, MaxSizeBytes: 2048, MaxObjects: 3}
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"Info", func(ctx context.Context) error { _, err := a.Info(ctx); return err }},
		{"SetQuota", func(ctx context.Context) error { return a.SetQuota(ctx, q) }},
		{"SetBucketQuota", func(ctx context.Context) error { return a.SetBucketQuota(ctx, q) }},
		{"Remove", func(ctx context.Context) error { return a.Remove(ctx) }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			calls, state := len(f.Calls), *created.State
			clustertest.AssertContextLockQueued(t, testhelper.TestState(g.Cluster).Mutex, operation.call)
			if len(f.Calls) != calls || *created.State != state {
				t.Fatalf("a queued call reached native commands or changed the account: %v", f.Calls[calls:])
			}
			if err := operation.call(t.Context()); err != nil {
				t.Fatalf("a fresh context after the queued deadline: %v", err)
			}
		})
	}
	if !created.State.Removed {
		t.Fatal("the fresh Remove did not remove the account")
	}
}
