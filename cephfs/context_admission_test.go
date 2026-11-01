package cephfs

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fsbase"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/internal/fstest"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/testhelper"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/servicehook"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSContextPublicAdmissionDeadline(t *testing.T) {
	operations := map[string]func(context.Context, *Filesystem, *DataPool) error{
		"ScaleMDS": func(ctx context.Context, fs *Filesystem, _ *DataPool) error {
			return fs.ScaleMDS(ctx, 1, 1)
		},
		"DataPools": func(ctx context.Context, fs *Filesystem, _ *DataPool) error {
			_, err := fs.DataPools(ctx)
			return err
		},
		"AddDataPool": func(ctx context.Context, fs *Filesystem, _ *DataPool) error {
			_, err := fs.AddDataPool(ctx, "extra")
			return err
		},
		"DataPool.RemoveUnused": func(ctx context.Context, fs *Filesystem, pool *DataPool) error {
			return pool.RemoveUnused(ctx)
		},
	}
	for name, operation := range operations {
		for _, gate := range []string{"setup", "owner", "control"} {
			t.Run(name+"/"+gate, func(t *testing.T) {
				fs, control, daemon, attachment := cephFSContextAdmissionFixture()
				cluster := fs.base().Cluster()
				originalIdentity := fs.base().Native()
				originalConfig := fs.config
				originalMDSS := slices.Clone(fs.mds.roster.daemons)
				attempt := attachment.identity.record
				assertCephFSContextAdmissionGate(t, cluster, gate, func(ctx context.Context) error {
					return operation(ctx, fs, attachment)
				})
				if len(control.Calls) != 0 || daemon.states != 0 {
					t.Fatalf("queued admission sent CLI or daemon queries: calls=%v states=%d", control.Calls, daemon.states)
				}
				if servicehook.Closed(cluster) || len(filesystems(cluster).byName) != 1 || filesystems(cluster).byName["fixture"] != fs ||
					len(servicehook.Services(cluster)) != 1 || servicehook.Service(cluster, "mds.a") != daemon ||
					fs.Container != daemon || !slices.Equal(fs.mds.roster.daemons, originalMDSS) ||
					!reflect.DeepEqual(fs.config, originalConfig) || fs.base().Native() != originalIdentity ||
					len(originalIdentity.Attachments) != 1 || originalIdentity.Attachments["additional"] != attempt ||
					!attempt.Confirmed || attempt.Used || attempt.Removed || attempt.RemovalAttempted {
					t.Fatal("queued admission changed filesystem, configuration, daemon or pool ownership")
				}
				assertCephFSContextAdmissionReleased(t, cluster)
				// Query the same fixture after the canceled waiter has returned.
				// This path contains only fake CLI calls and cannot start Docker.
				if states, err := fs.DataPools(t.Context()); err != nil || len(states) == 0 {
					t.Fatalf("fresh context could not reuse the same filesystem: pools=%v error=%v", states, err)
				}
			})
		}
	}
}

func TestCephFSContextSetupAdmissionDeadline(t *testing.T) {
	for _, gate := range []string{"setup", "owner", "control"} {
		t.Run(gate, func(t *testing.T) {
			control := &poolFixtureContainer{Fail: "fs dump --format json"}
			cluster := poolFixtureCluster(control, 2)
			var result *Filesystem
			assertCephFSContextAdmissionGate(t, cluster, gate, func(ctx context.Context) error {
				var err error
				result, err = startFilesystem(ctx, cluster, Config{Name: "tenant"})
				return err
			})
			if result != nil || len(control.Calls) != 0 || len(filesystems(cluster).byName) != 0 ||
				len(servicehook.Services(cluster)) != 0 || servicehook.Closed(cluster) || testhelper.TestOSDCount(cluster) != 2 {
				t.Fatal("queued setup registered an attempt, ran native commands or changed cluster ownership")
			}
			assertCephFSContextAdmissionReleased(t, cluster)
			// The fake fails the first native preflight, before pool or MDS creation.
			if result, err := startFilesystem(t.Context(), cluster, Config{Name: "tenant"}); err == nil || result != nil {
				t.Fatalf("fresh setup failed to reach its harmless native probe: fs=%v error=%v", result, err)
			}
			if len(control.Calls) != 1 || !slices.Equal(control.Calls[0], []string{"fs", "dump", "--format", "json"}) ||
				len(filesystems(cluster).byName) != 0 || len(servicehook.Services(cluster)) != 0 {
				t.Fatal("fresh setup went beyond the fake FSMap failure")
			}
		})
	}
}

func TestCephFSContextCanceledFreeAdmissionHasNoEffects(t *testing.T) {
	for _, operation := range []string{"setup", "scale", "data-pools", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			fs, control, daemon, _ := cephFSContextAdmissionFixture()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var err error
			switch operation {
			case "setup":
				_, err = startFilesystem(ctx, fs.base().Cluster(), Config{Name: "new-filesystem"})
			case "scale":
				err = fs.ScaleMDS(ctx, 1, 1)
			case "data-pools":
				_, err = fs.DataPools(ctx)
			case "snapshot":
				_, err = fs.fleet().mdsSnapshot(ctx)
			}
			if !errors.Is(err, context.Canceled) || len(control.Calls) != 0 || daemon.states != 0 ||
				len(filesystems(fs.base().Cluster()).byName) != 1 || fs.config.ActiveMDS != 1 || fs.config.StandbyMDS != 0 {
				t.Fatalf("canceled free admission acquired resources or lost cancellation: %v", err)
			}
			assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
			if _, err := fs.DataPools(t.Context()); err != nil {
				t.Fatalf("canceled free admission blocked the next query: %v", err)
			}
		})
	}
}

func TestCephFSContextMDSSnapshotPreservesOwnedDescriptors(t *testing.T) {
	fs, control, _, _ := cephFSContextAdmissionFixture()
	first := &MDS{ID: "z", FilesystemName: "fixture"}
	second := &MDS{ID: "a", FilesystemName: "fixture"}
	fs.mds.roster.daemons = []*MDS{first, second}
	assertContextLockQueued(t, testhelper.TestState(fs.base().Cluster()).Mutex, func(ctx context.Context) error {
		_, err := fs.fleet().mdsSnapshot(ctx)
		return err
	})
	if len(control.Calls) != 0 || fs.mds.roster.daemons[0] != first || fs.mds.roster.daemons[1] != second {
		t.Fatal("queued snapshot queried native state or altered the owned creation order")
	}
	for _, closed := range []bool{false, true} {
		testhelper.TestSetClosed(fs.base().Cluster(), closed)
		snapshot, err := fs.fleet().mdsSnapshot(t.Context())
		if err != nil || len(snapshot) != 2 || snapshot[0] != first || snapshot[1] != second {
			t.Fatalf("snapshot lost pointers, creation order or terminated-fixture inspection: %+v %v", snapshot, err)
		}
		snapshot[0] = nil
		if fs.mds.roster.daemons[0] != first {
			t.Fatal("returned MDS slice aliases the owned inventory")
		}
	}
}

func TestCephFSContextMDSStatusSnapshotsBeforeNativeQuery(t *testing.T) {
	fs, control, _, _ := cephFSContextAdmissionFixture()
	assertContextLockQueued(t, testhelper.TestState(fs.base().Cluster()).Mutex, func(ctx context.Context) error {
		_, err := fs.MDSStatus(ctx)
		return err
	})
	if len(control.Calls) != 0 {
		t.Fatal("MDSStatus queried FSMap before its bounded owner snapshot", control.Calls)
	}
	status, err := fs.MDSStatus(t.Context())
	if err != nil || len(status.Active) != 1 || !status.Active[0].Owned || status.Active[0].Name != "a" {
		t.Fatalf("fresh status lost the original owned rank: status=%+v error=%v", status, err)
	}
	if len(control.Calls) != 1 || !slices.Equal(control.Calls[0], []string{"fs", "dump", "--format", "json"}) {
		t.Fatal("fresh status did not use exactly one native FSMap", control.Calls)
	}
}

func TestCephFSContextSetupRegistrationAdmissionCancellation(t *testing.T) {
	native := &poolFixtureContainer{Output: map[string]string{
		"fs dump --format json": `{"standbys":[],"filesystems":[]}`,
	}}
	control := &cephFSContextBoundaryControl{native: native, query: "fs dump --format json"}
	cluster := poolFixtureCluster(control, 2)
	var result *Filesystem
	assertCephFSContextLateOwnerGate(t, testhelper.TestState(cluster).Mutex, func(hook func()) {
		control.afterQuery = hook
	}, func(ctx context.Context) error {
		var err error
		result, err = startFilesystem(ctx, cluster, Config{Name: "tenant"})
		return err
	})
	if result != nil || len(filesystems(cluster).byName) != 0 || len(servicehook.Services(cluster)) != 0 ||
		len(native.Calls) != 1 || !slices.Equal(native.Calls[0], []string{"fs", "dump", "--format", "json"}) {
		t.Fatal("expired registration admission published an attempt or created native resources")
	}
	assertCephFSContextAdmissionReleased(t, cluster)
	// Retry stays before pool creation regardless of the caller's deadline.
	native.Fail = "fs dump --format json"
	if result, err := startFilesystem(t.Context(), cluster, Config{Name: "tenant"}); err == nil || result != nil ||
		len(filesystems(cluster).byName) != 0 || len(native.Calls) != 2 {
		t.Fatalf("fresh registration retry did not stop at the fake preflight: fs=%v error=%v", result, err)
	}
}

func TestCephFSContextScaleDesiredConfigAdmissionCancellation(t *testing.T) {
	fs, control, daemon, _ := cephFSContextAdmissionFixture()
	originalConfig := fs.config
	originalIdentity := fs.base().Native()
	assertCephFSContextLateOwnerGate(t, testhelper.TestState(fs.base().Cluster()).Mutex, func(hook func()) {
		daemon.afterState = hook
	}, func(ctx context.Context) error {
		return fs.ScaleMDS(ctx, 1, 1)
	})
	if daemon.states != 1 || !reflect.DeepEqual(fs.config, originalConfig) ||
		fs.base().Native() != originalIdentity || len(fs.mds.roster.daemons) != 1 ||
		servicehook.Service(fs.base().Cluster(), "mds.a") != daemon || len(filesystems(fs.base().Cluster()).byName) != 1 {
		t.Fatal("expired scale intent admission changed configuration or native ownership")
	}
	assertCephFSContextOnlyReadQueries(t, control.Calls)
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
	// Equal counts exercise a complete fresh scale without launching a daemon.
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil {
		t.Fatalf("fresh scale could not reuse the original daemon: %v", err)
	}
}

func TestCephFSContextScaleStateRetainsCausalError(t *testing.T) {
	fs, control, daemon, _ := cephFSContextAdmissionFixture()
	cause := errors.New("daemon state inspection failed")
	daemon.stateErr = errors.Join(context.DeadlineExceeded, cause)
	originalConfig := fs.config
	if err := fs.ScaleMDS(t.Context(), 1, 1); !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, cause) {
		t.Fatalf("scale masked its causal daemon State failure: %v", err)
	}
	if daemon.states != 1 || !reflect.DeepEqual(fs.config, originalConfig) ||
		len(fs.mds.roster.daemons) != 1 || servicehook.Service(fs.base().Cluster(), "mds.a") != daemon {
		t.Fatal("failed daemon inspection published desired counts or changed ownership")
	}
	assertCephFSContextOnlyReadQueries(t, control.Calls)
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
	daemon.stateErr = nil
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil {
		t.Fatalf("fresh scale could not recover after the State failure: %v", err)
	}
}

func TestCephFSContextWaitReadyConfigAdmissionCancellation(t *testing.T) {
	fs, native, _, _ := cephFSContextAdmissionFixture()
	control := &cephFSContextBoundaryControl{native: native, query: "fs dump --format json"}
	fs.base().Cluster().Container = control
	originalConfig := fs.config
	assertCephFSContextLateOwnerGate(t, testhelper.TestState(fs.base().Cluster()).Mutex, func(hook func()) {
		control.afterQuery = hook
	}, fs.WaitReady)
	if !reflect.DeepEqual(fs.config, originalConfig) || len(native.Calls) != 1 {
		t.Fatal("expired readiness snapshot changed desired topology or started another native query")
	}
	assertCephFSContextOnlyReadQueries(t, native.Calls)
	assertCephFSContextAdmissionReleased(t, fs.base().Cluster())
	if err := fs.WaitReady(t.Context()); err != nil {
		t.Fatalf("fresh readiness could not observe the original active rank: %v", err)
	}
}

func cephFSContextAdmissionFixture() (*Filesystem, *poolFixtureContainer, *cephFSContextAdmissionDaemon, *DataPool) {
	fs, control := filesystemFixture()
	control.Output["fs dump --format json"] = `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2,3],"info":{"a":{"name":"a","rank":0,"gid":101,"state":"up:active","join_fscid":41}}}}],"standbys":[]}`
	control.Output["fs subvolumegroup ls fixture --format json"] = "[]"
	control.Output["fs subvolume ls fixture --format json"] = "[]"
	fs.config.ActiveMDS, fs.config.StandbyMDS = 1, 0
	daemon := &cephFSContextAdmissionDaemon{id: "fixture-mds-a"}
	fs.Container = daemon
	fs.mds.roster.daemons = []*MDS{{Container: daemon, ID: "a", FilesystemName: "fixture"}}
	setServices(fs.base().Cluster(), map[string]testcontainers.Container{"mds.a": daemon})
	identity := &fsbase.PoolAttachment{Name: "additional", ID: 3, Confirmed: true}
	fs.base().Native().Attachments["additional"] = identity
	return fs, control, daemon, cephFSPoolHandle(fs, identity)
}

type cephFSContextAdmissionDaemon struct {
	testcontainers.Container
	id         string
	states     int
	stateErr   error
	afterState func()
}

func (daemon *cephFSContextAdmissionDaemon) GetContainerID() string { return daemon.id }

func (daemon *cephFSContextAdmissionDaemon) State(context.Context) (*container.State, error) {
	daemon.states++
	if daemon.afterState != nil {
		daemon.afterState()
	}
	if daemon.stateErr != nil {
		return nil, daemon.stateErr
	}
	return &container.State{Running: true}, nil
}

type cephFSContextBoundaryControl struct {
	testcontainers.Container
	native     *poolFixtureContainer
	query      string
	afterQuery func()
}

func (control *cephFSContextBoundaryControl) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := control.native.Exec(ctx, args, opts...)
	if err == nil && strings.Join(args[3:], " ") == control.query && control.afterQuery != nil {
		control.afterQuery()
	}
	return code, reader, err
}

func assertCephFSContextAdmissionGate(t *testing.T, cluster *ceph.Container, gate string, operation func(context.Context) error) {
	t.Helper()
	fstest.AssertAdmissionGate(t, cluster, &filesystems(cluster).setupMu, gate, operation)
}

func assertCephFSContextAdmissionReleased(t *testing.T, cluster *ceph.Container) {
	t.Helper()
	fstest.AssertAdmissionReleased(t, cluster, &filesystems(cluster).setupMu)
}

func assertCephFSContextOnlyReadQueries(t *testing.T, calls [][]string) {
	t.Helper()
	for _, call := range calls {
		if !slices.Equal(call, []string{"fs", "dump", "--format", "json"}) &&
			!slices.Equal(call, []string{"osd", "pool", "ls", "detail", "--format", "json"}) {
			t.Fatal("failed admission continued with a native mutation", call)
		}
	}
}

func assertCephFSContextLateOwnerGate(t *testing.T, mutex *sync.Mutex, install func(func()), operation func(context.Context) error) {
	t.Helper()
	fstest.AssertLateOwnerGate(t, mutex, install, operation)
}
