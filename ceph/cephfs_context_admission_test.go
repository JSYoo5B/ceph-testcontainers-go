package ceph

import (
	"context"
	"errors"
	"io"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSContextPublicAdmissionDeadline(t *testing.T) {
	operations := map[string]func(context.Context, *CephFSContainer, *CephFSDataPool) error{
		"ScaleMDS": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			return fs.ScaleMDS(ctx, 1, 1)
		},
		"SubvolumeGroups": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			_, err := fs.SubvolumeGroups(ctx)
			return err
		},
		"Subvolumes": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			_, err := fs.Subvolumes(ctx, "group")
			return err
		},
		"CreateSubvolumeGroup": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			_, err := fs.CreateSubvolumeGroup(ctx, CephFSSubvolumeGroupConfig{Name: "new-group"})
			return err
		},
		"CreateSubvolume": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			_, err := fs.CreateSubvolume(ctx, CephFSSubvolumeConfig{Name: "new-volume"})
			return err
		},
		"DataPools": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			_, err := fs.DataPools(ctx)
			return err
		},
		"AddDataPool": func(ctx context.Context, fs *CephFSContainer, _ *CephFSDataPool) error {
			_, err := fs.AddDataPool(ctx, "extra")
			return err
		},
		"RemoveUnusedDataPool": func(ctx context.Context, fs *CephFSContainer, pool *CephFSDataPool) error {
			return fs.RemoveUnusedDataPool(ctx, pool)
		},
	}
	for name, operation := range operations {
		for _, gate := range []string{"setup", "owner", "control"} {
			t.Run(name+"/"+gate, func(t *testing.T) {
				fs, control, daemon, attachment := cephFSContextAdmissionFixture()
				cluster := fs.cluster
				originalIdentity := fs.nativeIdentity
				originalConfig := fs.config
				originalMDSS := slices.Clone(fs.mdss)
				attempt := attachment.identity
				assertCephFSContextAdmissionGate(t, cluster, gate, func(ctx context.Context) error {
					return operation(ctx, fs, attachment)
				})
				if len(control.calls) != 0 || daemon.states != 0 {
					t.Fatalf("queued admission sent CLI or daemon queries: calls=%v states=%d", control.calls, daemon.states)
				}
				if cluster.closed || len(cluster.filesystems) != 1 || cluster.filesystems["fixture"] != fs ||
					len(cluster.services) != 1 || cluster.services["mds.a"] != daemon ||
					fs.Container != daemon || !slices.Equal(fs.mdss, originalMDSS) ||
					!reflect.DeepEqual(fs.config, originalConfig) || fs.nativeIdentity != originalIdentity ||
					len(originalIdentity.attachments) != 1 || originalIdentity.attachments["additional"] != attempt ||
					!attempt.confirmed || attempt.used || attempt.removed || attempt.removalAttempted {
					t.Fatal("queued admission changed filesystem, configuration, daemon or pool ownership")
				}
				assertCephFSContextAdmissionReleased(t, cluster)
				// Query the same fixture after the canceled waiter has returned.
				// This path contains only fake CLI calls and cannot start Docker.
				if names, err := fs.SubvolumeGroups(t.Context()); err != nil || len(names) != 0 {
					t.Fatalf("fresh context could not reuse the same filesystem: names=%v error=%v", names, err)
				}
			})
		}
	}
}

func TestCephFSContextSetupAdmissionDeadline(t *testing.T) {
	for _, gate := range []string{"setup", "owner", "control"} {
		t.Run(gate, func(t *testing.T) {
			control := &poolFixtureContainer{fail: "fs dump --format json"}
			cluster := poolFixtureCluster(control, 2)
			var result *CephFSContainer
			assertCephFSContextAdmissionGate(t, cluster, gate, func(ctx context.Context) error {
				var err error
				result, err = cluster.StartCephFSWithConfig(ctx, CephFSConfig{Name: "tenant"})
				return err
			})
			if result != nil || len(control.calls) != 0 || len(cluster.filesystems) != 0 ||
				len(cluster.services) != 0 || cluster.closed || len(cluster.osds) != 2 {
				t.Fatal("queued setup registered an attempt, ran native commands or changed cluster ownership")
			}
			assertCephFSContextAdmissionReleased(t, cluster)
			// The fake fails the first native preflight, before pool or MDS creation.
			if result, err := cluster.StartCephFSWithConfig(t.Context(), CephFSConfig{Name: "tenant"}); err == nil || result != nil {
				t.Fatalf("fresh setup failed to reach its harmless native probe: fs=%v error=%v", result, err)
			}
			if len(control.calls) != 1 || !slices.Equal(control.calls[0], []string{"fs", "dump", "--format", "json"}) ||
				len(cluster.filesystems) != 0 || len(cluster.services) != 0 {
				t.Fatal("fresh setup went beyond the fake FSMap failure")
			}
		})
	}
}

func TestCephFSContextCanceledFreeAdmissionHasNoEffects(t *testing.T) {
	for _, operation := range []string{"setup", "scale", "subvolume", "data-pools", "snapshot"} {
		t.Run(operation, func(t *testing.T) {
			fs, control, daemon, _ := cephFSContextAdmissionFixture()
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			var err error
			switch operation {
			case "setup":
				_, err = fs.cluster.StartCephFSWithConfig(ctx, CephFSConfig{Name: "new-filesystem"})
			case "scale":
				err = fs.ScaleMDS(ctx, 1, 1)
			case "subvolume":
				_, err = fs.SubvolumeGroups(ctx)
			case "data-pools":
				_, err = fs.DataPools(ctx)
			case "snapshot":
				_, err = fs.mdsSnapshot(ctx)
			}
			if !errors.Is(err, context.Canceled) || len(control.calls) != 0 || daemon.states != 0 ||
				len(fs.cluster.filesystems) != 1 || fs.config.ActiveMDS != 1 || fs.config.StandbyMDS != 0 {
				t.Fatalf("canceled free admission acquired resources or lost cancellation: %v", err)
			}
			assertCephFSContextAdmissionReleased(t, fs.cluster)
			if _, err := fs.DataPools(t.Context()); err != nil {
				t.Fatalf("canceled free admission blocked the next query: %v", err)
			}
		})
	}
}

func TestCephFSContextPinAdmissionDeadline(t *testing.T) {
	for _, operation := range []string{"read", "temporary", "restore"} {
		for _, gate := range []string{"setup", "owner", "control"} {
			t.Run(operation+"/"+gate, func(t *testing.T) {
				fs, control, volume, _ := cephFSPinFixture()
				change := &CephFSPinOverride{
					filesystem: fs, target: cephFSPinTarget{identity: volume.identity},
					setting: CephFSPinSetting{Type: CephFSPinExport, Value: 1},
					state: &cephFSPinOverrideState{
						path: "/volumes/group/volume", inode: 103, previous: 0, applied: 1,
					},
				}
				key := change.state.path + "\x00" + string(change.setting.Type)
				fs.pinOverrides = map[string]*CephFSPinOverride{key: change}
				control.policies[change.state.path].ExportRank = 1
				assertCephFSContextAdmissionGate(t, fs.cluster, gate, func(ctx context.Context) error {
					switch operation {
					case "read":
						_, err := fs.SubvolumePinPolicy(ctx, volume)
						return err
					case "temporary":
						_, err := fs.TemporarySubvolumePin(ctx, volume, CephFSPinSetting{Type: CephFSPinDistributed, Value: 1})
						return err
					default:
						return change.Restore(ctx)
					}
				})
				if len(control.calls) != 0 || len(control.native.calls) != 0 || len(control.mutations) != 0 ||
					change.state.restored || len(fs.pinOverrides) != 1 || fs.pinOverrides[key] != change ||
					control.policies[change.state.path].ExportRank != 1 ||
					control.policies[change.state.path].Distributed {
					t.Fatal("queued pin operation queried native state, changed policy or lost the recovery handle")
				}
				assertCephFSContextAdmissionReleased(t, fs.cluster)
				if policy, err := fs.SubvolumePinPolicy(t.Context(), volume); err != nil || policy.ExportRank != 1 {
					t.Fatalf("fresh pin read could not reuse the original directory: policy=%+v error=%v", policy, err)
				}
			})
		}
	}
}

func TestCephFSContextMDSSnapshotPreservesOwnedDescriptors(t *testing.T) {
	fs, control, _, _ := cephFSContextAdmissionFixture()
	first := &MDSContainer{ID: "z", FilesystemName: "fixture"}
	second := &MDSContainer{ID: "a", FilesystemName: "fixture"}
	fs.mdss = []*MDSContainer{first, second}
	assertContextLockQueued(t, &fs.cluster.mu, func(ctx context.Context) error {
		_, err := fs.mdsSnapshot(ctx)
		return err
	})
	if len(control.calls) != 0 || fs.mdss[0] != first || fs.mdss[1] != second {
		t.Fatal("queued snapshot queried native state or altered the owned creation order")
	}
	for _, closed := range []bool{false, true} {
		fs.cluster.closed = closed
		snapshot, err := fs.mdsSnapshot(t.Context())
		if err != nil || len(snapshot) != 2 || snapshot[0] != first || snapshot[1] != second {
			t.Fatalf("snapshot lost pointers, creation order or terminated-fixture inspection: %+v %v", snapshot, err)
		}
		snapshot[0] = nil
		if fs.mdss[0] != first {
			t.Fatal("returned MDS slice aliases the owned inventory")
		}
	}
}

func TestCephFSContextMDSStatusSnapshotsBeforeNativeQuery(t *testing.T) {
	fs, control, _, _ := cephFSContextAdmissionFixture()
	assertContextLockQueued(t, &fs.cluster.mu, func(ctx context.Context) error {
		_, err := fs.MDSStatus(ctx)
		return err
	})
	if len(control.calls) != 0 {
		t.Fatal("MDSStatus queried FSMap before its bounded owner snapshot", control.calls)
	}
	status, err := fs.MDSStatus(t.Context())
	if err != nil || len(status.Active) != 1 || !status.Active[0].Owned || status.Active[0].Name != "a" {
		t.Fatalf("fresh status lost the original owned rank: status=%+v error=%v", status, err)
	}
	if len(control.calls) != 1 || !slices.Equal(control.calls[0], []string{"fs", "dump", "--format", "json"}) {
		t.Fatal("fresh status did not use exactly one native FSMap", control.calls)
	}
}

func TestCephFSContextSetupRegistrationAdmissionCancellation(t *testing.T) {
	native := &poolFixtureContainer{output: map[string]string{
		"fs dump --format json": `{"standbys":[],"filesystems":[]}`,
	}}
	control := &cephFSContextBoundaryControl{native: native, query: "fs dump --format json"}
	cluster := poolFixtureCluster(control, 2)
	var result *CephFSContainer
	assertCephFSContextLateOwnerGate(t, &cluster.mu, func(hook func()) {
		control.afterQuery = hook
	}, func(ctx context.Context) error {
		var err error
		result, err = cluster.StartCephFSWithConfig(ctx, CephFSConfig{Name: "tenant"})
		return err
	})
	if result != nil || len(cluster.filesystems) != 0 || len(cluster.services) != 0 ||
		len(native.calls) != 1 || !slices.Equal(native.calls[0], []string{"fs", "dump", "--format", "json"}) {
		t.Fatal("expired registration admission published an attempt or created native resources")
	}
	assertCephFSContextAdmissionReleased(t, cluster)
	// Retry stays before pool creation regardless of the caller's deadline.
	native.fail = "fs dump --format json"
	if result, err := cluster.StartCephFSWithConfig(t.Context(), CephFSConfig{Name: "tenant"}); err == nil || result != nil ||
		len(cluster.filesystems) != 0 || len(native.calls) != 2 {
		t.Fatalf("fresh registration retry did not stop at the fake preflight: fs=%v error=%v", result, err)
	}
}

func TestCephFSContextScaleDesiredConfigAdmissionCancellation(t *testing.T) {
	fs, control, daemon, _ := cephFSContextAdmissionFixture()
	originalConfig := fs.config
	originalIdentity := fs.nativeIdentity
	assertCephFSContextLateOwnerGate(t, &fs.cluster.mu, func(hook func()) {
		daemon.afterState = hook
	}, func(ctx context.Context) error {
		return fs.ScaleMDS(ctx, 1, 1)
	})
	if daemon.states != 1 || !reflect.DeepEqual(fs.config, originalConfig) ||
		fs.nativeIdentity != originalIdentity || len(fs.mdss) != 1 ||
		fs.cluster.services["mds.a"] != daemon || len(fs.cluster.filesystems) != 1 {
		t.Fatal("expired scale intent admission changed configuration or native ownership")
	}
	assertCephFSContextOnlyReadQueries(t, control.calls)
	assertCephFSContextAdmissionReleased(t, fs.cluster)
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
		len(fs.mdss) != 1 || fs.cluster.services["mds.a"] != daemon {
		t.Fatal("failed daemon inspection published desired counts or changed ownership")
	}
	assertCephFSContextOnlyReadQueries(t, control.calls)
	assertCephFSContextAdmissionReleased(t, fs.cluster)
	daemon.stateErr = nil
	if err := fs.ScaleMDS(t.Context(), 1, 0); err != nil {
		t.Fatalf("fresh scale could not recover after the State failure: %v", err)
	}
}

func TestCephFSContextWaitReadyConfigAdmissionCancellation(t *testing.T) {
	fs, native, _, _ := cephFSContextAdmissionFixture()
	control := &cephFSContextBoundaryControl{native: native, query: "fs dump --format json"}
	fs.cluster.Container = control
	originalConfig := fs.config
	assertCephFSContextLateOwnerGate(t, &fs.cluster.mu, func(hook func()) {
		control.afterQuery = hook
	}, fs.WaitReady)
	if !reflect.DeepEqual(fs.config, originalConfig) || len(native.calls) != 1 {
		t.Fatal("expired readiness snapshot changed desired topology or started another native query")
	}
	assertCephFSContextOnlyReadQueries(t, native.calls)
	assertCephFSContextAdmissionReleased(t, fs.cluster)
	if err := fs.WaitReady(t.Context()); err != nil {
		t.Fatalf("fresh readiness could not observe the original active rank: %v", err)
	}
}

func cephFSContextAdmissionFixture() (*CephFSContainer, *poolFixtureContainer, *cephFSContextAdmissionDaemon, *CephFSDataPool) {
	fs, control := subvolumeFixture()
	control.output["fs dump --format json"] = `{"filesystems":[{"id":41,"mdsmap":{"fs_name":"fixture","max_mds":1,"metadata_pool":1,"data_pools":[2,3],"info":{"a":{"name":"a","rank":0,"gid":101,"state":"up:active","join_fscid":41}}}}],"standbys":[]}`
	control.output["fs subvolumegroup ls fixture --format json"] = "[]"
	control.output["fs subvolume ls fixture --format json"] = "[]"
	fs.config.ActiveMDS, fs.config.StandbyMDS = 1, 0
	daemon := &cephFSContextAdmissionDaemon{id: "fixture-mds-a"}
	fs.Container = daemon
	fs.mdss = []*MDSContainer{{Container: daemon, ID: "a", FilesystemName: "fixture"}}
	fs.cluster.services = map[string]testcontainers.Container{"mds.a": daemon}
	identity := &cephFSDataPoolIdentity{filesystem: fs, name: "additional", id: 3, confirmed: true}
	fs.nativeIdentity.attachments["additional"] = identity
	return fs, control, daemon, cephFSPoolHandle(identity)
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

func assertCephFSContextAdmissionGate(t *testing.T, cluster *Container, gate string, operation func(context.Context) error) {
	t.Helper()
	switch gate {
	case "setup":
		assertContextLockQueued(t, &cluster.cephfsSetupMu, operation)
	case "owner":
		assertContextLockQueued(t, &cluster.mu, operation)
	case "control":
		assertSnapshotReadLockQueued(t, &cluster.controlMu, operation)
	default:
		t.Fatalf("unknown admission gate %q", gate)
	}
}

func assertCephFSContextAdmissionReleased(t *testing.T, cluster *Container) {
	t.Helper()
	for name, mutex := range map[string]*sync.Mutex{"setup": &cluster.cephfsSetupMu, "owner": &cluster.mu} {
		if !mutex.TryLock() {
			t.Fatalf("canceled admission retained the %s gate", name)
		}
		mutex.Unlock()
	}
	if !cluster.controlMu.TryLock() {
		t.Fatal("canceled admission retained the control cache gate")
	}
	cluster.controlMu.Unlock()
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

// The native callback acquires the owner after the earlier entry/preflight gate
// has completed. Cancel only after observing that gate, and retain the gate
// until the operation returns its caller cancellation.
// On failure release it and join the worker, following the existing gate tests.
func assertCephFSContextLateOwnerGate(t *testing.T, mutex *sync.Mutex, install func(func()), operation func(context.Context) error) {
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
