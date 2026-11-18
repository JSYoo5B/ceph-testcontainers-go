//go:build all || (integration && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
)

// Only map reads, owned handles and instance inspections are substituted.
// The real collector and classifier execute together.
type managerObservationSource struct {
	controlSources
	gid      uint64
	failure  bool
	removed  bool
	instance string
}

func (s *managerObservationSource) ManagerStatus(context.Context) (ceph.ManagerStatus, error) {
	if s.failure {
		return ceph.ManagerStatus{}, errors.New("map unavailable")
	}
	if s.gid == 0 {
		return ceph.ManagerStatus{}, nil
	}
	return ceph.ManagerStatus{ActiveName: "a", ActiveGID: s.gid}, nil
}
func (s *managerObservationSource) Managers(context.Context) ([]*ceph.ManagerContainer, error) {
	if s.removed {
		return nil, nil
	}
	return []*ceph.ManagerContainer{{Container: managerInstanceFake{started: s.instance}, DaemonName: "a"}}, nil
}

type managerInstanceFake struct {
	testcontainers.Container
	started string
}

func (managerInstanceFake) GetContainerID() string { return "owned" }
func (f managerInstanceFake) State(context.Context) (*container.State, error) {
	return &container.State{StartedAt: f.started}, nil
}

func TestAuthManagerCollectorTargetPhases(t *testing.T) {
	for _, test := range []struct {
		name          string
		before, after managerObservationSource
		want          string
	}{
		{"registered", managerObservationSource{gid: 4191, instance: "one"}, managerObservationSource{gid: 4191, instance: "one"}, "registered"},
		{"owned absent", managerObservationSource{instance: "one"}, managerObservationSource{instance: "one"}, "absent"},
		{"unreadable map", managerObservationSource{gid: 4191, instance: "one"}, managerObservationSource{failure: true, instance: "one"}, "unavailable"},
		{"removed handle", managerObservationSource{gid: 4191, instance: "one"}, managerObservationSource{gid: 4191, removed: true}, "unavailable"},
		{"changed GID", managerObservationSource{gid: 4191, instance: "one"}, managerObservationSource{gid: 4192, instance: "one"}, "changed"},
		{"changed instance", managerObservationSource{gid: 4191, instance: "one"}, managerObservationSource{gid: 4191, instance: "two"}, "changed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := modeledRetainedBundle(t)
			b.Queries = nil
			c := authCollector{sources: &test.before, ctx: t.Context(), bound: time.Second, bundle: &b}
			b.Managers = c.observeManagers("before", nil)
			c.sources = &test.after
			b.ManagersAfter = c.observeManagers("after", nil)
			got := classifyAuthBundle(b)
			if got.Target != test.want || got.Result != -13 || got.GenerationEquality != authGenerationUnavailable {
				t.Fatalf("verdict %+v", got)
			}
		})
	}
}
