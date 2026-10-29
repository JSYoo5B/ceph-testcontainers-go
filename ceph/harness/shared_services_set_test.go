package harness

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The service set is the only source of a shared cluster's service callbacks.
// These controls cover what the set does with them: kind order, the error
// context of each callback, and the ceph.Container pointer they receive. The
// backend's run of them is in ceph/shared_services_backend_test.go.

// withSharedServices gives a model cluster exactly these services. Production
// code takes them from WithServices; the tests build the unexported form.
func withSharedServices(services ...sharedService) SharedOption {
	return func(s *sharedSettings) { s.serviceList = append(s.serviceList, services...) }
}

type serviceCalls struct {
	prepared []string
	probed   []string
	pointers []*ceph.Container
}

func (c *serviceCalls) service(kind string, prepareErr error, resources map[string]string, probeErr error) sharedService {
	return sharedService{
		kind: kind,
		prepare: func(_ context.Context, ctr *ceph.Container) error {
			c.prepared = append(c.prepared, kind)
			c.pointers = append(c.pointers, ctr)
			return prepareErr
		},
		probe: func(_ context.Context, ctr *ceph.Container) (map[string]string, error) {
			c.probed = append(c.probed, kind)
			c.pointers = append(c.pointers, ctr)
			return resources, probeErr
		},
	}
}

// runPrepares and runProbes run a set's callbacks as the core does: in order,
// with the cluster pointer, stopping at the first failure.
func runPrepares(ctx context.Context, c *ceph.Container, prepares []sharedPrepare) error {
	for _, prepare := range prepares {
		if err := prepare(ctx, c); err != nil {
			return err
		}
	}
	return nil
}

func runProbes(ctx context.Context, c *ceph.Container, probes []sharedProbe) (map[string]string, error) {
	resources := map[string]string{}
	for _, probe := range probes {
		found, err := probe(ctx, c)
		if err != nil {
			return nil, err
		}
		for key, owner := range found {
			resources[key] = owner
		}
	}
	return resources, nil
}

func newSet(t *testing.T, services ...sharedService) sharedServiceSet {
	t.Helper()
	set, err := newSharedServiceSet(services)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestServiceSetPreparesInKindOrderWithTheGivenPointer(t *testing.T) {
	calls := &serviceCalls{}
	set := newSet(t, calls.service("rgw", nil, nil, nil), calls.service("cephfs", nil, nil, nil), calls.service("rbd", nil, nil, nil),
		sharedService{kind: "aaa-no-prepare", probe: calls.service("aaa-no-prepare", nil, nil, nil).probe})
	own := &ceph.Container{}
	if err := runPrepares(t.Context(), own, set.prepares); err != nil {
		t.Fatal(err)
	}
	if want := []string{"cephfs", "rbd", "rgw"}; !slices.Equal(calls.prepared, want) {
		t.Fatalf("prepared %v, want %v (kind order, a nil prepare skipped)", calls.prepared, want)
	}
	for _, p := range calls.pointers {
		if p != own {
			t.Fatal("a prepare callback got a pointer other than the cluster's own")
		}
	}
}

func TestServiceSetPrepareFailureStopsAndKeepsContext(t *testing.T) {
	calls := &serviceCalls{}
	cause := errors.New("no zone")
	set := newSet(t, calls.service("cephfs", nil, nil, nil), calls.service("rbd", cause, nil, nil), calls.service("rgw", nil, nil, nil))
	err := runPrepares(t.Context(), nil, set.prepares)
	if err == nil || err.Error() != "prepare rbd for a shared cluster: no zone" || !errors.Is(err, cause) {
		t.Fatalf("prepare failure = %v", err)
	}
	if want := []string{"cephfs", "rbd"}; !slices.Equal(calls.prepared, want) {
		t.Fatalf("prepared %v after a failure, want %v (later kinds not prepared)", calls.prepared, want)
	}
}

func TestServiceSetResourcesMergeInKindOrder(t *testing.T) {
	calls := &serviceCalls{}
	set := newSet(t,
		calls.service("rgw", nil, map[string]string{"shared": "from rgw", "rgw user": "t1"}, nil),
		calls.service("cephfs", nil, map[string]string{"shared": "from cephfs", "cephfs group": "t1"}, nil),
		sharedService{kind: "rbd"})
	got, err := runProbes(t.Context(), nil, set.probes)
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

func TestServiceSetProbeFailureStopsAndKeepsContext(t *testing.T) {
	calls := &serviceCalls{}
	cause := errors.New("gateway gone")
	set := newSet(t, calls.service("cephfs", nil, nil, cause), calls.service("rgw", nil, nil, nil))
	got, err := runProbes(t.Context(), nil, set.probes)
	if got != nil || err == nil || err.Error() != "list the cephfs resources: gateway gone" || !errors.Is(err, cause) {
		t.Fatalf("probe failure = %v, %v", got, err)
	}
	if !slices.Equal(calls.probed, []string{"cephfs"}) {
		t.Fatalf("probed %v after a failure", calls.probed)
	}
}
