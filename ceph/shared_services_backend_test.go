package ceph

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// The backend runs the service callbacks that the shared harness gives it, in
// the order given and with its own cluster pointer. The set that orders and
// wraps them lives in ceph/harness.

func TestBackendRunsServiceCallbacksInOrderWithItsOwnCluster(t *testing.T) {
	own := &Container{}
	var order []string
	var pointers []*Container
	callback := func(name string, err error) func(context.Context, *Container) error {
		return func(_ context.Context, c *Container) error {
			order = append(order, name)
			pointers = append(pointers, c)
			return err
		}
	}
	cause := errors.New("no zone")
	err := containerBackend{c: own}.PrepareServices(t.Context(), []func(context.Context, *Container) error{
		callback("a", nil), callback("b", cause), callback("c", nil)})
	if !errors.Is(err, cause) || !slices.Equal(order, []string{"a", "b"}) {
		t.Fatalf("prepare stopped at %v with %v", order, err)
	}
	for _, p := range pointers {
		if p != own {
			t.Fatal("a callback got a pointer other than the backend's own cluster")
		}
	}
}

func TestBackendResourcesMergeInGivenOrderAndStopAtTheFirstFailure(t *testing.T) {
	probe := func(found map[string]string, err error) func(context.Context, *Container) (map[string]string, error) {
		return func(context.Context, *Container) (map[string]string, error) { return found, err }
	}
	b := containerBackend{c: &Container{}}
	got, err := b.Resources(t.Context(), []func(context.Context, *Container) (map[string]string, error){
		probe(map[string]string{"shared": "first", "a": "t1"}, nil), probe(map[string]string{"shared": "second"}, nil)})
	if err != nil || got["shared"] != "second" || got["a"] != "t1" {
		t.Fatalf("resources = %v, %v; the later probe wins a shared key", got, err)
	}
	cause := errors.New("gateway gone")
	if got, err := b.Resources(t.Context(), []func(context.Context, *Container) (map[string]string, error){probe(nil, cause), probe(nil, nil)}); got != nil || !errors.Is(err, cause) {
		t.Fatalf("a failed probe gave %v, %v", got, err)
	}
	if got, err := b.Resources(t.Context(), nil); err != nil || got == nil || len(got) != 0 {
		t.Fatalf("no probes listed %v, %v; want an empty non-nil map", got, err)
	}
}

func TestSharedRunResultKeepsAPartialCluster(t *testing.T) {
	partial := &Container{}
	backend, err := sharedRunResult(partial, errors.New("bootstrap failed"))
	if err == nil || backend == nil || (harnessHooks{}).Lend(backend) != partial {
		t.Fatalf("backend=%v err=%v, want the partial cluster with the error", backend, err)
	}
	if backend, err := sharedRunResult(nil, errors.New("create failed")); backend != nil || err == nil {
		t.Fatalf("backend=%v err=%v, want no backend", backend, err)
	}
	if (harnessHooks{}).Lend(struct{}{}) != nil {
		t.Fatal("Lend returned a cluster for a value that is not a container backend")
	}
}
