package ceph

import (
	"errors"
	"testing"
)

func TestContainerOpsTopologyHoldsTheClusterMutex(t *testing.T) {
	c := &Container{}
	port := containerOps{c: c}
	sentinel := errors.New("fn failed")

	var heldInside bool
	err := port.WithTopology(func() error {
		heldInside = !c.mu.TryLock()
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("WithTopology returned %v, want the error of fn", err)
	}
	if !heldInside {
		t.Fatal("fn ran without the cluster mutex")
	}
	if !c.mu.TryLock() {
		t.Fatal("WithTopology left the cluster mutex held")
	}
	c.mu.Unlock()
}

func TestContainerOpsExtensionIsKeyedByTheCluster(t *testing.T) {
	c, other := &Container{}, &Container{}
	port := containerOps{c: c}
	created := 0
	create := func() any { created++; return &struct{ n int }{} }

	first := port.Extension("kind", create)
	if again := port.Extension("kind", create); again != first || created != 1 {
		t.Fatalf("a second lookup returned a new state (created %d)", created)
	}
	if viaHook := (serviceHooks{}).Extension(c, "kind", create); viaHook != first {
		t.Fatal("the port and the hook see different state for one cluster")
	}
	if (containerOps{c: other}).Extension("kind", create) == first {
		t.Fatal("two clusters share one state")
	}
	if port.Extension("other-kind", create) == first {
		t.Fatal("two kinds share one state")
	}
}
