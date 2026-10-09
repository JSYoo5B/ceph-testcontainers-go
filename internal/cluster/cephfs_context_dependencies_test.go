package cluster

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSDirectControlSnapshotsHonorBusyPublication(t *testing.T) {
	t.Run("pin", func(t *testing.T) {
		fs, control, _, _ := cephFSPinFixture()
		assertSnapshotReadLockQueued(t, &fs.cluster.controlMu, func(ctx context.Context) error {
			policy, err := fs.readPinPolicy(ctx, "/volumes/group/volume")
			if policy != nil {
				t.Error("busy control returned a pin policy")
			}
			return err
		})
		if len(control.calls) != 0 || len(control.mutations) != 0 {
			t.Fatal("busy control queried or mutated pin policy")
		}
		policy, err := fs.readPinPolicy(t.Context(), "/volumes/group/volume")
		if err != nil || policy == nil || policy.Inode != 103 || len(control.mutations) != 0 {
			t.Fatalf("fresh context could not read original pin identity: policy=%v err=%v", policy, err)
		}
	})
	t.Run("clone", func(t *testing.T) {
		fs, control, clone := cloneLifecycleFixture(t)
		before := *clone.identity.incarnation
		assertSnapshotReadLockQueued(t, &fs.cluster.controlMu, func(ctx context.Context) error {
			incarnation, err := fs.readCloneIncarnation(ctx, clone.identity)
			if incarnation != nil {
				t.Error("busy control returned clone incarnation")
			}
			return err
		})
		if len(control.calls) != 0 || *clone.identity.incarnation != before {
			t.Fatal("busy control queried or changed captured clone identity")
		}
		incarnation, err := fs.readCloneIncarnation(t.Context(), clone.identity)
		if err != nil || incarnation == nil || *incarnation != before {
			t.Fatalf("fresh context could not inspect original clone incarnation: %v", err)
		}
	})
}

func TestCephFSDataPoolFinalRADOSReadHonorsBusyControl(t *testing.T) {
	fs, native := dataPoolsFixture(t)
	entered := make(chan struct{})
	var publicationMu sync.Mutex
	held, disabled := false, false
	hooked := &cephFSDependencyControl{Container: native}
	hooked.afterRead = func(args []string) {
		if strings.Join(args, " ") == "ceph --connect-timeout 5 fs subvolume ls fixture --format json" {
			publicationMu.Lock()
			defer publicationMu.Unlock()
			if disabled {
				return
			}
			fs.cluster.controlMu.Lock()
			held = true
			close(entered)
		}
	}
	fs.cluster.Container = hooked
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result, exited := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(exited)
		result <- fs.checkUnusedDataPool(ctx, "extra")
	}()
	t.Cleanup(func() {
		cancel()
		publicationMu.Lock()
		disabled = true
		if held {
			fs.cluster.controlMu.Unlock()
			held = false
		}
		publicationMu.Unlock()
		select {
		case <-exited:
		case <-time.After(time.Second):
			t.Error("data-pool control waiter remained after fixture release")
		}
	})
	select {
	case <-entered:
	case err := <-result:
		t.Fatalf("pool preflight did not reach final control snapshot: %v", err)
	case <-time.After(time.Second):
		t.Fatal("pool preflight did not reach final control snapshot")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("final data-pool snapshot lost observed caller cancellation: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("final data-pool control wait ignored observed caller cancellation")
	}
	if slices.Contains(native.calls, "rados -p extra --all ls") {
		t.Fatal("busy final control sent RADOS command")
	}
	if fs.cluster.controlMu.TryLock() {
		fs.cluster.controlMu.Unlock()
		t.Fatal("operation stole or unlocked held control gate")
	}
	publicationMu.Lock()
	disabled = true
	fs.cluster.controlMu.Unlock()
	held = false
	publicationMu.Unlock()
	if err := fs.checkUnusedDataPool(t.Context(), "extra"); err != nil || !slices.Contains(native.calls, "rados -p extra --all ls") {
		t.Fatalf("fresh context could not finish original pool emptiness check: %v", err)
	}
}

func TestCephFSInheritedCreatePoolAdmissionHonorsBusyOwner(t *testing.T) {
	native := &poolFixtureContainer{}
	cluster := poolFixtureCluster(native, 2)
	assertContextLockQueued(t, &cluster.mu, func(ctx context.Context) error {
		pool, err := cluster.CreatePool(ctx, PoolConfig{Name: "queued-cephfs"})
		if pool != nil {
			t.Error("busy owner returned attempted pool")
		}
		return err
	})
	if len(native.calls) != 0 {
		t.Fatal("queued pool creation sent native commands")
	}
	pool, err := cluster.CreatePool(t.Context(), PoolConfig{Name: "queued-cephfs"})
	if err != nil || pool == nil || pool.Name != "queued-cephfs" || len(native.calls) == 0 {
		t.Fatalf("fresh context could not create pool with same fixture: pool=%v err=%v", pool, err)
	}
}

func TestCephFSCanceledNativeIdentityReadbackStillPublishesOwnership(t *testing.T) {
	fs, native := subvolumeFixture()
	fs.nativeIdentity = nil
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	hooked := &cephFSDependencyControl{Container: native}
	hooked.afterRead = func(args []string) {
		if strings.Join(args, " ") == "ceph --connect-timeout 5 osd pool ls detail --format json" {
			cancel()
		}
	}
	fs.cluster.Container = hooked
	if err := fs.captureNativePoolIdentity(ctx); err != nil {
		t.Fatalf("successful native identity readback was discarded after cancellation: %v", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) || fs.nativeIdentity == nil || fs.nativeIdentity.id != 41 || fs.nativeIdentity.metadataPool != 1 || fs.nativeIdentity.defaultPool != 2 || fs.nativeIdentity.attachments == nil {
		t.Fatal("cancellation lost original filesystem/pool ownership")
	}
	original := fs.nativeIdentity
	hooked.afterRead = nil
	if _, err := fs.readNativePools(t.Context()); err != nil || fs.nativeIdentity != original {
		t.Fatalf("fresh native read could not use exact published identity: %v", err)
	}
}

// Run hooks after complete native output consumption, when control snapshots
// are no longer held. No hook changes native data or bypasses runtime methods.
type cephFSDependencyControl struct {
	testcontainers.Container
	afterRead func([]string)
}

func (control *cephFSDependencyControl) Exec(ctx context.Context, args []string, options ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := control.Container.Exec(ctx, args, options...)
	if reader == nil || err != nil || control.afterRead == nil {
		return code, reader, err
	}
	return code, &cephFSDependencyReader{Reader: reader, after: func() { control.afterRead(args) }}, err
}

type cephFSDependencyReader struct {
	io.Reader
	after func()
	once  sync.Once
}

func (reader *cephFSDependencyReader) Read(target []byte) (int, error) {
	n, err := reader.Reader.Read(target)
	if err == io.EOF {
		reader.once.Do(reader.after)
	}
	return n, err
}
