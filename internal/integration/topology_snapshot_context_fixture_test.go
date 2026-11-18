//go:build all || (integration && topology && multicluster && (!ci || (ci_code && (!ci_batch || ci_batch_pure))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestTopologySnapshotUnjoinedActorKeepsRejectingHandles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		original := new(actorLifecycleContainer)
		barrier := &topologySnapshotBarrier{Container: original}
		cluster := &ceph.Container{Container: barrier}
		pending := new(atomic.Int32)
		pending.Add(1)
		release := make(chan struct{})
		actor := launchIntegrationActor(context.Background(), func(context.Context) error {
			<-release
			_, _, err := cluster.Container.Exec(context.Background(), []string{"late-native-write"})
			pending.Add(-1)
			return err
		})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer func() {
			unblock()
			_, _ = actor.wait(context.Background())
			actor.cancel()
		}()
		joinCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := actor.wait(joinCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("delayed worker unexpectedly joined:", err)
		}
		if err := restoreTopologySnapshotHandles([]*ceph.Container{cluster}, []testcontainers.Container{original}, pending); err == nil {
			t.Fatal("restoration accepted an unjoined actor")
		}
		if cluster.Container != barrier {
			t.Fatal("unjoined worker lost the rejecting handle")
		}
		unblock()
		err, joinErr := actor.wait(context.Background())
		if joinErr != nil || !errors.Is(err, errTopologySnapshotBarrier) || barrier.queries.Load() != 1 {
			t.Fatalf("late native work was not rejected: %v %v", err, joinErr)
		}

		if err := restoreTopologySnapshotHandles([]*ceph.Container{cluster}, []testcontainers.Container{original}, pending); err != nil || cluster.Container != original {
			t.Fatalf("joined handles were not restored: %v", err)
		}
	})
}
