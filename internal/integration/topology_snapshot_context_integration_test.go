//go:build integration && topology && multicluster

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// A private test wrapper holds a real public mutation before any native work.
// All other native/container mutation attempts are rejected, including during
// regression drainage, so a broken queued consumer cannot create resources.
var errTopologySnapshotBarrier = errors.New("test topology owner barrier")

type topologySnapshotBarrier struct {
	testcontainers.Container
	hold                      bool
	entered, release          chan struct{}
	once                      sync.Once
	queries, copies, cleanups atomic.Int32
}

func (barrier *topologySnapshotBarrier) Exec(ctx context.Context, args []string, _ ...tcexec.ProcessOption) (int, io.Reader, error) {
	if barrier.hold && slices.Equal(args, []string{"ceph-authtool", "--gen-print-key"}) {
		barrier.once.Do(func() { close(barrier.entered) })
		select {
		case <-barrier.release:
			return 0, nil, errTopologySnapshotBarrier
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	barrier.queries.Add(1)
	return 0, nil, errTopologySnapshotBarrier
}
func (barrier *topologySnapshotBarrier) CopyToContainer(context.Context, []byte, string, int64) error {
	barrier.copies.Add(1)
	return errTopologySnapshotBarrier
}
func (barrier *topologySnapshotBarrier) Terminate(context.Context, ...testcontainers.TerminateOption) error {
	barrier.cleanups.Add(1)
	return errTopologySnapshotBarrier
}

// All embedded handle assignments happen before workers start and after they
// join. The wrapper is for default single-MON fixtures only, whose stable CLI
// handle is the embedded container. Never use it after ensureControlPlane.
func topologySnapshotHoldOwner(t *testing.T, ctx context.Context, clusters []*ceph.Container, busy *ceph.Container) (func(), func(), []*topologySnapshotBarrier) {
	t.Helper()
	originals := make([]testcontainers.Container, len(clusters))
	for i, cluster := range clusters {
		originals[i] = cluster.Container
		if cluster.ControlContainer() != originals[i] {
			t.Fatal("barrier requires a default single-MON control handle")
		}
	}
	if !slices.Contains(clusters, busy) {
		t.Fatal("busy owner must belong to the supplied fixture pair")
	}
	wrappers := make([]*topologySnapshotBarrier, len(clusters))
	var blocker *topologySnapshotBarrier
	for i, cluster := range clusters {
		wrappers[i] = &topologySnapshotBarrier{Container: originals[i], hold: cluster == busy, entered: make(chan struct{}), release: make(chan struct{})}
		if cluster == busy {
			blocker = wrappers[i]
		}
		cluster.Container = wrappers[i]
	}
	ownerCtx, cancel := context.WithTimeout(ctx, time.Minute)
	type outcome struct {
		osd *ceph.OSDContainer
		err error
	}
	done := make(chan outcome, 1)
	go func() { osd, err := busy.AddOSD(ownerCtx); done <- outcome{osd, err} }()
	var releaseOnce sync.Once
	releaseOwner := func() { releaseOnce.Do(func() { close(blocker.release) }) }
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			releaseOwner()
			select {
			case result := <-done:
				if result.osd != nil || !errors.Is(result.err, errTopologySnapshotBarrier) {
					t.Errorf("owner unexpectedly created native/container state: osd=%v err=%v", result.osd, result.err)
				}
			case <-time.After(3 * time.Second):
				t.Error("owner did not leave the released pre-native barrier")
			}
			cancel()
			for i, cluster := range clusters {
				cluster.Container = originals[i]
			}
		})
	}
	select {
	case <-blocker.entered:
		return releaseOwner, finish, wrappers
	case result := <-done:
		cancel()
		for i, cluster := range clusters {
			cluster.Container = originals[i]
		}
		t.Fatalf("owner did not reach the pre-native barrier: osd=%v error=%v", result.osd, result.err)
	case <-time.After(5 * time.Second):
		finish()
		t.Fatal("owner did not enter the pre-native barrier")
	}
	return nil, nil, nil
}

type topologySnapshotPartial interface {
	Terminate(context.Context, ...testcontainers.TerminateOption) error
}

func topologySnapshotQueue(t *testing.T, releaseOwner func(), operation func(context.Context) (topologySnapshotPartial, error)) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	type outcome struct {
		fixture topologySnapshotPartial
		err     error
	}
	done := make(chan outcome, 1)
	go func() { fixture, err := operation(ctx); done <- outcome{fixture, err} }()
	cleanupPartial := func(result outcome) {
		if result.fixture == nil {
			return
		}
		releaseOwner()
		cleanup := func() {
			cleanupCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if err := result.fixture.Terminate(cleanupCtx); err != nil {
				t.Errorf("cleanup unexpected partial constructor result: %v", err)
			}
		}
		// A failed future constructor must not lose a non-nil ownership handle.
		// This cleanup runs after the test body releases the owner barrier.
		t.Cleanup(cleanup)
	}
	select {
	case result := <-done:
		cleanupPartial(result)
		if result.fixture != nil || !errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("busy consumer published state or lost caller deadline: published=%v error=%v", result.fixture != nil, result.err)
		}
	case <-time.After(400 * time.Millisecond):
		// Drain while wrappers still reject native work. Restore occurs only
		// in the parent's deferred finish, after this queued worker joins.
		releaseOwner()
		select {
		case result := <-done:
			cleanupPartial(result)
		case <-time.After(3 * time.Second):
		}
		t.Fatal("consumer remained queued past the caller deadline")
	}
}

func TestMultiClusterTopologySnapshotsHonorBusyOwners(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	const pool = "tc-context-snapshot"
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t,
		ceph.WithMonitorCount(1), ceph.WithOSDCount(1),
		ceph.WithPools(ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 1, MinSize: 1}))
	type state struct {
		config, keyring []byte
		managers        []*ceph.ManagerContainer
		gateways        []*ceph.RGWContainer
		osdIDs          []int
	}
	before := make([]state, 2)
	clusters := []*ceph.Container{source, destination}
	clients := []testcontainers.Container{sourceClient, destinationClient}
	payload := []byte("busy constructor admission retains exact Ceph client data")
	for i, cluster := range clusters {
		var err error
		before[i].config, before[i].keyring, err = cluster.ConnectionConfigContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before[i].managers, err = cluster.ManagersContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		before[i].gateways, err = cluster.GatewaysContext(ctx)
		if err != nil {
			t.Fatal(err)
		}
		data, err := cluster.Ceph(ctx, "osd", "ls", "--format", "json")
		if err != nil || json.Unmarshal(data, &before[i].osdIDs) != nil {
			t.Fatalf("read original OSD membership: %v", err)
		}
		if err := clients[i].CopyToContainer(ctx, payload, "/tmp/tc-context-input", 0o600); err != nil {
			t.Fatal(err)
		}
		multiClusterExecOutput(t, ctx, clients[i], "rados", "-p", pool, "put", "retained", "/tmp/tc-context-input")
	}
	for index, busy := range clusters {
		t.Run([]string{"source", "destination"}[index], func(t *testing.T) {
			releaseOwner, finish, wrappers := topologySnapshotHoldOwner(t, ctx, clusters, busy)
			defer finish()
			for _, kind := range []string{"rbd", "cephfs", "rgw", "rgw-topology"} {
				t.Run(kind, func(t *testing.T) {
					topologySnapshotQueue(t, releaseOwner, func(callCtx context.Context) (topologySnapshotPartial, error) {
						switch kind {
						case "rbd":
							result, err := multicluster.RunRBDMirror(callCtx, source.ControlImage(), multicluster.RBDMirrorConfig{Source: source, Destination: destination, Pool: pool})
							if result == nil {
								return nil, err
							}
							return result, err
						case "cephfs":
							result, err := multicluster.RunCephFSMirror(callCtx, source.ControlImage(), multicluster.CephFSMirrorConfig{Source: source, Destination: destination, SourceFilesystem: "fs", DestinationFilesystem: "fs", Directories: []string{"/owned"}})
							if result == nil {
								return nil, err
							}
							return result, err
						case "rgw":
							result, err := multicluster.RunRGWMultisite(callCtx, source.ControlImage(), multicluster.RGWMultisiteConfig{Source: source, Destination: destination})
							if result == nil {
								return nil, err
							}
							return result, err
						default:
							result, err := multicluster.RunRGWTopology(callCtx, source.ControlImage(), multicluster.RGWTopologyConfig{Zones: []multicluster.RGWZoneConfig{{Name: "a", Cluster: source}, {Name: "b", Cluster: destination}}})
							if result == nil {
								return nil, err
							}
							return result, err
						}
					})
				})
				if t.Failed() {
					return
				}
			}
			for _, wrapper := range wrappers {
				if wrapper.queries.Load() != 0 || wrapper.copies.Load() != 0 || wrapper.cleanups.Load() != 0 {
					t.Errorf("queued consumers attempted native work/copy/cleanup: %d/%d/%d", wrapper.queries.Load(), wrapper.copies.Load(), wrapper.cleanups.Load())
				}
			}
		})
		if t.Failed() {
			return
		}
	}
	for i, cluster := range clusters {
		config, keyring, err := cluster.ConnectionConfigContext(ctx)
		if err != nil || !bytes.Equal(config, before[i].config) || !bytes.Equal(keyring, before[i].keyring) {
			t.Fatalf("fresh bootstrap snapshot changed after canceled admission: %v", err)
		}
		managers, err := cluster.ManagersContext(ctx)
		if err != nil || !slices.Equal(managers, before[i].managers) {
			t.Fatalf("manager descriptors changed: %v", err)
		}
		gateways, err := cluster.GatewaysContext(ctx)
		if err != nil || !slices.Equal(gateways, before[i].gateways) {
			t.Fatalf("gateway descriptors changed: %v", err)
		}
		data, err := cluster.Ceph(ctx, "osd", "ls", "--format", "json")
		var current []int
		if err != nil || json.Unmarshal(data, &current) != nil || !slices.Equal(current, before[i].osdIDs) {
			t.Fatalf("OSD membership changed: %v", err)
		}
		multiClusterExecOutput(t, ctx, clients[i], "rados", "-p", pool, "get", "retained", "/tmp/tc-context-output")
		if !bytes.Equal(multiClusterReadFile(t, ctx, clients[i], "/tmp/tc-context-output"), payload) {
			t.Fatal("retained RADOS bytes differ after owner admission cancellation")
		}
	}
}
