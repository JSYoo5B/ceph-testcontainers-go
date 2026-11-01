// Package fstest holds the unit-test fixtures that the tests of package
// cephfs and its feature packages share: the canned native output of one
// synthetic filesystem, a foundation core over it for the feature packages,
// which cannot construct a cephfs.Filesystem, and the assertions that an
// operation waits for a held admission gate without native effects. Only
// _test.go files import it.
package fstest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"
)

// Name is the name of the synthetic filesystem.
//
//testdouble:shared-double
const Name = "fixture"

// Output returns a fresh copy of the canned native output of the synthetic
// filesystem "fixture" (FSMap ID 41; metadata pool 1, default data pool 2 and
// additional data pool 3) with the volumes module enabled, the group "group"
// and the namespace-isolated subvolume "volume" in it.
//
//testdouble:shared-double
func Output() map[string]string {
	return map[string]string{
		"mgr module ls --format json":                                       `{"enabled_modules":["volumes"],"always_on_modules":[]}`,
		"fs volume ls --format json":                                        `[{"name":"fixture"}]`,
		"fs dump --format json":                                             `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2,3],"info":{}}}],"standbys":[]}`,
		"osd pool ls detail --format json":                                  `[{"pool_id":1,"pool_name":"metadata","type":1,"size":2,"min_size":1,"pg_num":8},{"pool_id":2,"pool_name":"data","type":1,"size":2,"min_size":1,"pg_num":8},{"pool_id":3,"pool_name":"additional","type":1,"size":2,"min_size":1,"pg_num":8}]`,
		"fs subvolumegroup info fixture group --format json":                `{"bytes_quota":65536,"bytes_used":0,"data_pool":"additional","created_at":"2026-10-03 00:00:00.123456"}`,
		"fs subvolumegroup getpath fixture group":                           "/volumes/group\n",
		"fs subvolume info fixture volume --group_name group --format json": `{"path":"/volumes/group/volume/unique-id","bytes_quota":32768,"bytes_used":8192,"data_pool":"additional","pool_namespace":"fsvolumens_group_volume","created_at":"2026-10-03 00:00:01.123456","state":"complete","type":"subvolume"}`,
	}
}

// Cluster returns a synthetic cluster with two owned OSDs whose control
// container is ctr and whose startup timeout is one second.
//
//testdouble:shared-double
func Cluster(ctr *clustertest.CannedCommandContainer) *ceph.Container {
	return testhelper.TestCluster(ctr, testhelper.TestConfig{StartupTimeout: time.Second, OSDs: 2})
}

// Filesystem is the foundation core of the synthetic filesystem over a
// synthetic cluster, with the registry state that package cephfs would inject
// held in its fields: the setup gate, the exported descriptor name, the
// liveness of the registry entry and the reader of the owned active ranks.
// The native identity is captured as FSMap ID 41 with pools 1 and 2.
//
//testdouble:shared-double
type Filesystem struct {
	Core    *fsbase.Core
	Control *clustertest.CannedCommandContainer
	// SetupMu is the setup gate of the core.
	SetupMu *sync.Mutex
	// Descriptor is the exported descriptor name; editing it makes
	// DescriptorIntact false.
	Descriptor string
	// Unregistered makes the registry no longer hold the core.
	Unregistered bool
	// ActiveRanks answers the core's rank reader; nil reports one owned
	// active rank 0.
	ActiveRanks func(context.Context) (*fsbase.ActiveRanks, error)
}

// NewFilesystem returns the synthetic filesystem with the canned Output.
//
//testdouble:shared-double
func NewFilesystem() *Filesystem {
	f := &Filesystem{Control: &clustertest.CannedCommandContainer{Output: Output()}, SetupMu: &sync.Mutex{}, Descriptor: Name}
	f.Core = fsbase.NewCore(fsbase.Params{
		Cluster: Cluster(f.Control), Name: Name, DescriptorName: &f.Descriptor,
		MetadataPool: "metadata", DataPool: "data", SetupMu: f.SetupMu,
		Live: func(*fsbase.Core) bool { return !f.Unregistered },
		ActiveRanks: func(ctx context.Context) (*fsbase.ActiveRanks, error) {
			if f.ActiveRanks != nil {
				return f.ActiveRanks(ctx)
			}
			return &fsbase.ActiveRanks{FilesystemID: 41, MaxMDS: 1, Active: []fsbase.ActiveRank{{Rank: 0, State: "up:active", Owned: true}}}, nil
		},
	})
	f.Core.ReplaceNative(&fsbase.NativeIdentity{ID: 41, MetadataPool: 1, DefaultPool: 2, Attachments: make(map[string]*fsbase.PoolAttachment)})
	return f
}

// AssertSnapshotReadLockQueued holds mutex as a writer and checks that
// operation waits for it, returns the caller's deadline and leaves the gate to
// the writer.
//
//testdouble:shared-assertion
func AssertSnapshotReadLockQueued(t testing.TB, mutex *sync.RWMutex, operation func(context.Context) error) {
	t.Helper()
	mutex.Lock()
	locked := true
	defer func() {
		if locked {
			mutex.Unlock()
		}
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- operation(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("queued snapshot lost caller deadline: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		mutex.Unlock()
		locked = false
		select {
		case _ = <-done:
		case <-time.After(time.Second):
		}
		t.Fatal("snapshot remained queued after caller deadline")
	}
	if mutex.TryRLock() {
		mutex.RUnlock()
		t.Fatal("snapshot unlocked the busy writer gate")
	}
	mutex.Unlock()
	locked = false
}

// AssertAdmissionGate checks that operation waits for one held admission gate
// of cluster, the filesystem's setup gate setupMu ("setup"), the cluster mutex
// ("owner") or the control cache gate ("control"), and returns the caller's
// deadline.
//
//testdouble:shared-assertion
func AssertAdmissionGate(t testing.TB, cluster *ceph.Container, setupMu *sync.Mutex, gate string, operation func(context.Context) error) {
	t.Helper()
	switch gate {
	case "setup":
		clustertest.AssertContextLockQueued(t, setupMu, operation)
	case "owner":
		clustertest.AssertContextLockQueued(t, testhelper.TestState(cluster).Mutex, operation)
	case "control":
		AssertSnapshotReadLockQueued(t, testhelper.TestState(cluster).ControlMutex, operation)
	default:
		t.Fatalf("unknown admission gate %q", gate)
	}
}

// AssertAdmissionReleased checks that no operation retained the setup gate
// setupMu, the cluster mutex or the control cache gate of cluster.
//
//testdouble:shared-assertion
func AssertAdmissionReleased(t testing.TB, cluster *ceph.Container, setupMu *sync.Mutex) {
	t.Helper()
	for name, mutex := range map[string]*sync.Mutex{"setup": setupMu, "owner": testhelper.TestState(cluster).Mutex} {
		if !mutex.TryLock() {
			t.Fatalf("canceled admission retained the %s gate", name)
		}
		mutex.Unlock()
	}
	if !testhelper.TestState(cluster).ControlMutex.TryLock() {
		t.Fatal("canceled admission retained the control cache gate")
	}
	testhelper.TestState(cluster).ControlMutex.Unlock()
}

// AssertLateOwnerGate takes mutex from the native callback that install
// receives, once the operation's earlier entry gate has completed, cancels the
// caller and checks that operation returns the cancellation without stealing
// or releasing mutex. On failure it releases the gate and joins the worker.
//
//testdouble:shared-assertion
func AssertLateOwnerGate(t testing.TB, mutex *sync.Mutex, install func(func()), operation func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{})
	hookFailure := make(chan error, 1)
	var once sync.Once
	var publicationMu sync.Mutex
	held, disabled := false, false
	install(func() {
		once.Do(func() {
			publicationMu.Lock()
			defer publicationMu.Unlock()
			if disabled {
				return
			}
			// A recursive owner acquisition must fail without blocking the
			// fake native callback or allowing later resource creation.
			if !mutex.TryLock() {
				hookFailure <- errors.New("native boundary unexpectedly retained the owner mutex")
				cancel()
				return
			}
			held = true
			close(entered)
		})
	})
	result, exited := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(exited)
		result <- operation(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		publicationMu.Lock()
		disabled = true
		if held {
			mutex.Unlock()
			held = false
		}
		publicationMu.Unlock()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("native boundary worker did not exit after fixture release")
		}
	})
	var outcome error
	completed := false
	select {
	case <-entered:
	case outcome = <-result:
		// Entry and result can both be pending when this goroutine resumes.
		select {
		case <-entered:
			completed = true
		default:
			t.Fatalf("operation returned before its native admission boundary: %v", outcome)
		}
	case err := <-hookFailure:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("operation did not reach its native admission boundary")
	}
	cancel()
	if !completed {
		select {
		case outcome = <-result:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("late admission remained blocked after caller cancellation")
		}
	}
	if !errors.Is(outcome, context.Canceled) {
		t.Fatalf("late owner admission lost caller cancellation: %v", outcome)
	}
	if mutex.TryLock() {
		mutex.Unlock()
		t.Fatal("late admission stole or unlocked the busy owner gate")
	}
	publicationMu.Lock()
	disabled = true
	mutex.Unlock()
	held = false
	publicationMu.Unlock()
}

// AssertWaitHeldGate holds a gate through lock and checks that wait returns
// the caller's deadline without stealing it; tryLock (and readUnlock for a
// read lock) probes the gate afterwards. Cleanup is registered before launch,
// so even an ordinary blocking Lock does not leak a worker.
//
//testdouble:shared-assertion
func AssertWaitHeldGate(t testing.TB, lock, unlock func(), tryLock func() bool, wait func(context.Context) error, readUnlock ...func()) {
	t.Helper()
	lock()
	var release sync.Once
	releaseGate := func() { release.Do(unlock) }
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	result, exited := make(chan error, 1), make(chan struct{})
	join := func() {
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("wait worker did not exit after gate release")
		}
	}
	t.Cleanup(func() { cancel(); releaseGate(); join() })
	go func() { defer close(exited); result <- wait(ctx) }()
	select {
	case err := <-result:
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("held admission lost deadline: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		cancel()
		releaseGate()
		join()
		t.Fatal("wait blocked beyond caller deadline while gate remained held")
	}
	if tryLock() {
		if len(readUnlock) != 0 {
			readUnlock[0]()
		} else {
			unlock()
		}
		t.Fatal("waiter released or stole original held gate")
	}
	releaseGate()
	join()
}
