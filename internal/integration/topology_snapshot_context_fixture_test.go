//go:build all || (integration && topology && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestTopologySnapshotUnjoinedActorKeepsRejectingHandles(t *testing.T) {
	original := new(actorLifecycleContainer)
	barrier := &topologySnapshotBarrier{Container: original}
	cluster := &ceph.Container{Container: barrier}
	pending := new(atomic.Int32)
	pending.Add(1)
	if err := restoreTopologySnapshotHandles([]*ceph.Container{cluster}, []testcontainers.Container{original}, pending); err == nil {
		t.Fatal("restoration accepted an unjoined actor")
	}
	if cluster.Container != barrier {
		t.Fatal("unjoined worker lost the rejecting handle")
	}
	_, _, err := cluster.Container.Exec(context.Background(), []string{"late-native-write"})
	if !errors.Is(err, errTopologySnapshotBarrier) || barrier.queries.Load() != 1 {
		t.Fatal("late native work was not rejected")
	}
	pending.Add(-1)
	if err := restoreTopologySnapshotHandles([]*ceph.Container{cluster}, []testcontainers.Container{original}, pending); err != nil || cluster.Container != original {
		t.Fatalf("joined handles were not restored: %v", err)
	}
}
