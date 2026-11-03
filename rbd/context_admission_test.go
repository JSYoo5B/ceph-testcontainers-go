package rbd

import (
	"context"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// The pool, namespace and image client operations wait for the cluster mutex
// only until the caller's context ends. A queued call sends no native command
// and changes no namespace state, leaves no waiter behind, and the same call
// succeeds with a fresh context afterwards.
func TestRBDOperationsWaitForTheClusterOnlyUntilTheContextEnds(t *testing.T) {
	control := newRBDTestControl()
	cluster := rbdTestCluster(control)
	var ns *Namespace
	operations := []struct {
		name string
		call func(context.Context) error
	}{
		{"InitPool", func(ctx context.Context) error { return initPool(ctx, cluster, "images") }},
		{"CreateNamespace", func(ctx context.Context) error {
			created, err := createNamespace(ctx, cluster, "images", "blue")
			if created != nil {
				ns = created
			}
			return err
		}},
		{"ListNamespaces", func(ctx context.Context) error { _, err := listNamespaces(ctx, cluster, "images"); return err }},
		{"ImageClients", func(ctx context.Context) error {
			_, err := imageClients(ctx, cluster, "images", "", "disk")
			return err
		}},
		{"RemoveNamespace", func(ctx context.Context) error { return removeNamespace(ctx, cluster, ns) }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			calls, namespaces := len(control.calls), len(control.namespaces)
			before := ns
			clustertest.AssertContextLockQueued(t, testhelper.TestState(cluster).Mutex, operation.call)
			if len(control.calls) != calls || len(control.namespaces) != namespaces || ns != before || (ns != nil && ns.state.removed) {
				t.Fatalf("a queued call reached native commands or changed namespace state: %v", control.calls[calls:])
			}
			if err := operation.call(t.Context()); err != nil {
				t.Fatalf("a fresh context after the queued deadline: %v", err)
			}
		})
	}
	if ns == nil || !ns.state.removed || len(control.namespaces) != 0 {
		t.Fatal("the fresh calls did not create and remove the namespace")
	}
}
