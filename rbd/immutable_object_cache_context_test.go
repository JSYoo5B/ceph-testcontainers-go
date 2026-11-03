package rbd

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// Status, Stop and Restore wait for another call on the same lease only until
// the caller's context ends. A queued call stops no daemon and removes no
// file, keeps the lease open, leaves no waiter behind, and the same call
// succeeds with a fresh context afterwards.
func TestImmutableObjectCacheLeaseWaitsOnlyUntilTheContextEnds(t *testing.T) {
	client := newFakeCacheClient()
	lease, err := StartImmutableObjectCache(t.Context(), client, ImmutableObjectCache{})
	if err != nil {
		t.Fatal(err)
	}
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"Status", func(ctx context.Context) error { _, err := lease.Status(ctx); return err }},
		{"Stop", lease.Stop},
		{"Restore", lease.Restore},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			files, running := len(client.files), client.daemons[0].running
			clustertest.AssertContextLockQueued(t, &lease.mu, operation.call)
			if len(client.files) != files || client.daemons[0].running != running || lease.complete {
				t.Fatal("a queued call stopped the daemon, removed files or ended the lease")
			}
			if err := operation.call(t.Context()); err != nil {
				t.Fatalf("a fresh context after the queued deadline: %v", err)
			}
		})
	}
	if !lease.complete || client.daemons[0].running || len(client.files) != 1 {
		t.Fatalf("the fresh calls did not stop and restore the cache: %v", client.files)
	}
}
