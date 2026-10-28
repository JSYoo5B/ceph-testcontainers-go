package ceph

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// These controls characterize the semantics of the init-time service registry
// before it is replaced by instance-owned service support (#155), so that the
// migration is provably equivalent. They run on a zero Container: the service
// callbacks only receive the pointer.

// swapSharedServices empties the process-global registry for one test and
// restores it afterwards. Tests that use it must not run in parallel.
func swapSharedServices(t *testing.T) {
	t.Helper()
	sharedServices.mu.Lock()
	saved := sharedServices.byKind
	sharedServices.byKind = nil
	sharedServices.mu.Unlock()
	t.Cleanup(func() {
		sharedServices.mu.Lock()
		sharedServices.byKind = saved
		sharedServices.mu.Unlock()
	})
}

type serviceCalls struct {
	prepared []string
	probed   []string
	pointers []*Container
}

func (c *serviceCalls) service(kind string, prepareErr error, resources map[string]string, probeErr error) sharedService {
	return sharedService{
		kind: kind,
		prepare: func(_ context.Context, ctr *Container) error {
			c.prepared = append(c.prepared, kind)
			c.pointers = append(c.pointers, ctr)
			return prepareErr
		},
		probe: func(_ context.Context, ctr *Container) (map[string]string, error) {
			c.probed = append(c.probed, kind)
			c.pointers = append(c.pointers, ctr)
			return resources, probeErr
		},
	}
}

func TestRegistryPreparesInKindOrderWithTheBackendPointer(t *testing.T) {
	swapSharedServices(t)
	calls := &serviceCalls{}
	for _, kind := range []string{"rgw", "cephfs", "rbd"} {
		registerSharedService(calls.service(kind, nil, nil, nil))
	}
	registerSharedService(sharedService{kind: "aaa-no-prepare", probe: calls.service("aaa-no-prepare", nil, nil, nil).probe})
	own := &Container{}
	if err := (containerBackend{c: own}).prepareServices(t.Context()); err != nil {
		t.Fatal(err)
	}
	if want := []string{"cephfs", "rbd", "rgw"}; !slices.Equal(calls.prepared, want) {
		t.Fatalf("prepared %v, want %v (kind order, a nil prepare skipped)", calls.prepared, want)
	}
	for _, p := range calls.pointers {
		if p != own {
			t.Fatal("a prepare callback got a pointer other than the backend's own cluster")
		}
	}
}

func TestRegistryPrepareFailureStopsAndKeepsContext(t *testing.T) {
	swapSharedServices(t)
	calls := &serviceCalls{}
	cause := errors.New("no zone")
	registerSharedService(calls.service("cephfs", nil, nil, nil))
	registerSharedService(calls.service("rbd", cause, nil, nil))
	registerSharedService(calls.service("rgw", nil, nil, nil))
	err := (containerBackend{c: &Container{}}).prepareServices(t.Context())
	if err == nil || err.Error() != "prepare rbd for a shared cluster: no zone" || !errors.Is(err, cause) {
		t.Fatalf("prepare failure = %v", err)
	}
	if want := []string{"cephfs", "rbd"}; !slices.Equal(calls.prepared, want) {
		t.Fatalf("prepared %v after a failure, want %v (later kinds not prepared)", calls.prepared, want)
	}
}

func TestRegistryResourcesMergeInKindOrder(t *testing.T) {
	swapSharedServices(t)
	calls := &serviceCalls{}
	registerSharedService(calls.service("rgw", nil, map[string]string{"shared": "from rgw", "rgw user": "t1"}, nil))
	registerSharedService(calls.service("cephfs", nil, map[string]string{"shared": "from cephfs", "cephfs group": "t1"}, nil))
	registerSharedService(sharedService{kind: "rbd"})
	got, err := (containerBackend{c: &Container{}}).resources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"shared": "from rgw", "rgw user": "t1", "cephfs group": "t1"}
	if len(got) != len(want) {
		t.Fatalf("resources = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("resources = %v, want %v (the later kind wins a shared key)", got, want)
		}
	}
	if !slices.Equal(calls.probed, []string{"cephfs", "rgw"}) {
		t.Fatalf("probed %v", calls.probed)
	}
}

func TestRegistryProbeFailureStopsAndKeepsContext(t *testing.T) {
	swapSharedServices(t)
	calls := &serviceCalls{}
	cause := errors.New("gateway gone")
	registerSharedService(calls.service("cephfs", nil, nil, cause))
	registerSharedService(calls.service("rgw", nil, nil, nil))
	got, err := (containerBackend{c: &Container{}}).resources(t.Context())
	if got != nil || err == nil || err.Error() != "list the cephfs resources: gateway gone" || !errors.Is(err, cause) {
		t.Fatalf("probe failure = %v, %v", got, err)
	}
	if !slices.Equal(calls.probed, []string{"cephfs"}) {
		t.Fatalf("probed %v after a failure", calls.probed)
	}
}

func TestRegistryWithoutServicesPreparesAndProbesNothing(t *testing.T) {
	swapSharedServices(t)
	b := containerBackend{c: &Container{}}
	if err := b.prepareServices(t.Context()); err != nil {
		t.Fatal(err)
	}
	got, err := b.resources(t.Context())
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("an unconfigured registry listed %v, %v; want an empty non-nil map", got, err)
	}
}

func TestRegistrySecondRegistrationOfAKindReplacesTheFirst(t *testing.T) {
	swapSharedServices(t)
	first, second := &serviceCalls{}, &serviceCalls{}
	registerSharedService(first.service("rbd", nil, nil, nil))
	registerSharedService(second.service("rbd", nil, nil, nil))
	if err := (containerBackend{c: &Container{}}).prepareServices(t.Context()); err != nil {
		t.Fatal(err)
	}
	if len(first.prepared) != 0 || len(second.prepared) != 1 {
		t.Fatalf("first %v, second %v: a second registration must replace the first", first.prepared, second.prepared)
	}
}

func TestRegistryIsReadAtCallTime(t *testing.T) {
	// Today the registry is read at each start and release, so a service
	// registered after a backend exists is used by its next call.
	swapSharedServices(t)
	b := containerBackend{c: &Container{}}
	calls := &serviceCalls{}
	registerSharedService(calls.service("late", nil, nil, nil))
	if err := b.prepareServices(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls.prepared, []string{"late"}) {
		t.Fatalf("a service registered later was not seen: %v", calls.prepared)
	}
}
