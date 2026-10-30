package fault

import (
	"context"
	"errors"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/faulthook"
	"github.com/testcontainers/testcontainers-go"
)

type contextLockClient struct{ testcontainers.Container }

func (contextLockClient) GetContainerID() string { return "osd-0" }
func TestEndpointRestoreQueuedDeadline(t *testing.T) {
	t.Run("network endpoint", func(t *testing.T) {
		link, docker := newLinkFake()
		assertContextLockQueued(t, &link.mu, link.Restore)
		if docker.connects != 0 || docker.closes != 0 || link.complete {
			t.Fatal("queued endpoint restore mutated Docker or completion state")
		}
		if err := link.Restore(t.Context()); err != nil || docker.connects != 1 || docker.closes != 1 || !link.complete || docker.endpoint.IPAddress != link.OriginalAddress {
			t.Fatalf("fresh endpoint restore could not preserve its original advertised address: %v", err)
		}
	})
}

func TestSerializedNetworkNestedGateReturnsOriginalHandle(t *testing.T) {
	link, docker := newLinkFake()
	cluster := testhelper.TestCluster(nil, testhelper.TestConfig{NetworkName: link.NetworkName})
	trackInterruption(cluster, link.NetworkName+"/osd-0", link)
	var result *NetworkInterruption
	assertContextLockQueued(t, &link.mu, func(ctx context.Context) error {
		var err error
		result, err = InterruptNetwork(ctx, cluster, contextLockClient{}, ceph.PublicNetworkPlane)
		return err
	})
	if result != link || docker.connects != 0 || docker.closes != 0 || link.complete || testhelper.TestFaults(cluster, faulthook.NetworkInterruption)[link.NetworkName+"/osd-0"] != link {
		t.Fatal("nested endpoint queue lost the retry handle or changed its endpoint")
	}
	if err := link.Restore(t.Context()); err != nil || docker.connects != 1 || !link.complete {
		t.Fatalf("fresh context could not restore the retained endpoint: %v", err)
	}
}

func assertContextLockQueued(t *testing.T, mutex *sync.Mutex, operation func(context.Context) error) {
	t.Helper()
	mutex.Lock()
	locked := true
	defer func() {
		if locked {
			mutex.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- operation(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued operation returned %v; want caller deadline", err)
		}
	case <-time.After(500 * time.Millisecond):
		// Release before failing so a regression leaves no abandoned worker.
		mutex.Unlock()
		locked = false
		select {
		case _ = <-result:
		case <-time.After(time.Second):
		}
		t.Fatal("queued operation did not honor its short caller deadline")
	}
	if mutex.TryLock() {
		mutex.Unlock()
		t.Fatal("operation stole or unlocked the busy fixture gate")
	}
	mutex.Unlock()
	locked = false
}

// Deadline expiry while waiting for the cluster gate must not reach Docker
// or reserve an endpoint; a later operation can still acquire the same gate.
func TestNetworkInterruptionQueuedTopologyDeadline(t *testing.T) {
	c := testhelper.TestCluster(nil, testhelper.TestConfig{NetworkName: "cluster-network"})
	view := testhelper.TestState(c)
	assertContextLockQueued(t, view.Mutex, func(ctx context.Context) error {
		link, err := InterruptNetwork(ctx, c, contextLockClient{}, ceph.PublicNetworkPlane)
		if link != nil {
			t.Error("queued operation returned a new endpoint handle")
		}
		return err
	})
	if err := faulthook.WithTopology(t.Context(), c, func() error {
		if len(testhelper.TestFaults(c, faulthook.NetworkInterruption)) != 0 || faulthook.Closed(c) {
			t.Error("queued interruption changed cluster state")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
