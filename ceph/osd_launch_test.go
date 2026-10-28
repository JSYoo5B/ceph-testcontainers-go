package ceph

import (
	"context"
	"errors"
	"io"
	"maps"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type osdLaunchFileSnapshot struct {
	data string
	mode int64
}

func consumeOSDLaunchFiles(t *testing.T, req testcontainers.GenericContainerRequest) map[string]osdLaunchFileSnapshot {
	t.Helper()
	files := make(map[string]osdLaunchFileSnapshot)
	for _, file := range req.Files {
		data, err := io.ReadAll(file.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if _, duplicate := files[file.ContainerFilePath]; duplicate || len(data) == 0 {
			t.Fatal("duplicate or empty OSD launch file")
		}
		files[file.ContainerFilePath] = osdLaunchFileSnapshot{string(data), file.FileMode}
	}
	return files
}

func TestOSDLaunchRecipeRebuildsConsumedFilesAndRequestState(t *testing.T) {
	c := &Container{settings: options{essentialImage: "synthetic-osd-image", startupTimeout: time.Minute},
		bootstrap: bootstrapRecord{config: []byte("[global]\nfsid = synthetic-cluster\n"), keyring: []byte("synthetic-admin-keyring")}, startup: newStartupIdentity("synthetic-owner"),
		nets:   networkState{public: &testcontainers.DockerNetwork{Name: "synthetic-public"}, cluster: &testcontainers.DockerNetwork{Name: "synthetic-cluster"}},
		runDir: runDirState{volume: &runVolume{name: "synthetic-run"}}, stores: osdStorageState{memory: &osdMemoryStorage{name: "synthetic-memory"}}}
	osd := &OSDContainer{ID: 7, nativeUUID: "synthetic-osd-uuid", placement: OSDConfig{Host: "synthetic-host", Root: "default", Rack: "rack-a", DeviceClass: "fast"}}
	key := []byte("synthetic-osd-keyring")
	r := c.newOSDLaunchRecipe(osd, key)
	var first testcontainers.GenericContainerRequest
	if err := r.Customize(&first); err != nil {
		t.Fatal(err)
	}
	wantFiles, wantEnv := consumeOSDLaunchFiles(t, first), maps.Clone(first.Env)
	if len(wantFiles) != 4 || wantFiles["/etc/ceph/osd.keyring"].data != string(key) ||
		wantFiles["/etc/ceph/ceph.client.admin.keyring"].mode != 0o600 || wantFiles["/tc/osd.sh"].mode != 0o755 {
		t.Fatal("missing launch bytes or file permissions")
	}
	for _, file := range first.Files {
		if data, err := io.ReadAll(file.Reader); err != nil || len(data) != 0 {
			t.Fatal("the first request did not consume its Readers")
		}
	}
	// Change the original inputs and the fully consumed request. A subsequent
	// request must reflect the captured recipe, not either mutable object.
	c.bootstrap.config[0], c.bootstrap.keyring[0], key[0] = 'X', 'X', 'X'
	c.settings.essentialImage, c.settings.hostNetwork = "other-image", true
	c.nets.public.Name, c.nets.cluster.Name = "other-public", "other-cluster"
	c.runDir.volume.name, c.stores.memory.name, c.startup.owner = "other-run", "other-memory", "other-owner"
	first.Env["CEPH_OSD_UUID"], first.Labels[clusterOwnerLabel] = "foreign", "foreign"
	first.Networks[0] = "foreign"
	first.Mounts[0].Source.(testcontainers.DockerVolumeMountSource).VolumeOptions.NoCopy = false
	first.Mounts[0].Source.(testcontainers.DockerVolumeMountSource).VolumeOptions.Labels["foreign"] = "true"
	*first.WaitingFor.(*wait.MultiStrategy).Strategies[0].(*wait.ExecStrategy).Timeout() = time.Hour
	var second testcontainers.GenericContainerRequest
	if err := r.Customize(&second); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantFiles, consumeOSDLaunchFiles(t, second)) || !maps.Equal(wantEnv, second.Env) ||
		r.image != "synthetic-osd-image" || second.Labels[clusterOwnerLabel] != "synthetic-owner" ||
		!slices.Equal(second.Networks, []string{"synthetic-public", "synthetic-cluster"}) ||
		!slices.Equal(second.Entrypoint, []string{"/bin/sh", "/tc/osd.sh"}) {
		t.Fatal("recipe inherited consumed or mutated input/request state")
	}
	if len(second.Mounts) != 2 || second.Mounts[0].Source.Source() != "synthetic-memory" || second.Mounts[1].Source.Source() != "synthetic-run" {
		t.Fatal("recipe did not preserve its volume names")
	}
	memory := second.Mounts[0].Source.(testcontainers.DockerVolumeMountSource).VolumeOptions
	run := second.Mounts[1].Source.(testcontainers.DockerVolumeMountSource).VolumeOptions
	if !memory.NoCopy || run.NoCopy || memory.Labels["foreign"] != "" || *second.WaitingFor.(*wait.MultiStrategy).Strategies[0].(*wait.ExecStrategy).Timeout() != time.Minute {
		t.Fatal("mount or wait state leaked between requests")
	}
}

func TestOSDLaunchRecipeHostModeKeepsIndependentModifiers(t *testing.T) {
	c := &Container{settings: options{hostNetwork: true, publicAddress: "192.0.2.7", startupTimeout: time.Minute}}
	r := c.newOSDLaunchRecipe(&OSDContainer{ID: 7}, []byte("synthetic-keyring"))
	for range 2 {
		req := testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{Networks: []string{"foreign"}, ExposedPorts: []string{"1234/tcp"}}}
		if err := r.Customize(&req); err != nil {
			t.Fatal(err)
		}
		host := &container.HostConfig{PublishAllPorts: true}
		req.HostConfigModifier(host)
		if len(req.Networks) != 0 || len(req.ExposedPorts) != 0 || host.NetworkMode != "host" || host.PublishAllPorts {
			t.Fatal("OSD host-mode isolation changed")
		}
	}
}

func preparedOSDLaunch(t *testing.T) (*Container, osdLaunch) {
	t.Helper()
	c, _, _ := newInitialOSDCluster(options{essentialImage: "synthetic-osd-image"})
	c.bootstrap.config, c.bootstrap.keyring = []byte("[global]\nfsid = synthetic-cluster\n"), []byte("synthetic-admin-keyring")
	launch, err := c.registerOSD(t.Context(), OSDConfig{Root: "default", Host: "synthetic-host"}, c.settings.essentialImage, nil)
	if err != nil {
		t.Fatal(err)
	}
	return c, launch
}

func TestOSDLaunchNeverReplaysCreationOrLosesPartialHandle(t *testing.T) {
	for _, outcome := range []string{"success", "partial-error", "no-handle-error"} {
		t.Run(outcome, func(t *testing.T) {
			c, launch := preparedOSDLaunch(t)
			saved := launch.osd
			created := &initialOSDDaemon{id: saved.ID}
			creationErr := errors.New("synthetic uncertain create/readiness failure")
			calls := 0
			c.settings.essentialImage = "changed-after-preparation"
			c.stores.runner = func(_ context.Context, image string, opts ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
				calls++
				if image != "synthetic-osd-image" {
					t.Fatal("launch did not preserve its image reference")
				}
				var req testcontainers.GenericContainerRequest
				for _, opt := range opts {
					if err := opt.Customize(&req); err != nil {
						t.Fatal(err)
					}
				}
				consumeOSDLaunchFiles(t, req)
				if outcome == "no-handle-error" {
					return nil, creationErr
				}
				if outcome == "partial-error" {
					return created, creationErr
				}
				return created, nil
			}
			err := c.startOSD(t.Context(), launch)
			if (err == nil) != (outcome == "success") || (err != nil && !errors.Is(err, creationErr)) {
				t.Fatalf("creation outcome: %v", err)
			}
			copyOfLaunch := launch
			if err := c.startOSD(t.Context(), copyOfLaunch); err == nil || !strings.Contains(err.Error(), "already attempted") || calls != 1 {
				t.Fatal("a copied launch replayed container creation")
			}
			if c.osdHandles.get(saved.ID) != saved || (saved.Container == created) != (outcome != "no-handle-error") {
				t.Fatal("original descriptor/partial container ownership was lost")
			}
			if err := c.Terminate(t.Context()); err != nil {
				t.Fatal(err)
			}
			if c.osdHandles.count() != 0 || (outcome != "no-handle-error" && created.terminations != 1) {
				t.Fatal("partial handle was not cleaned up through the original owner")
			}
		})
	}
}

func TestOSDLaunchConcurrentCopiesCreateOnlyOneContainer(t *testing.T) {
	c, launch := preparedOSDLaunch(t)
	var calls atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	c.stores.runner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		calls.Add(1)
		close(entered)
		<-release
		return &initialOSDDaemon{id: launch.osd.ID}, nil
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func(copyOfLaunch osdLaunch) { defer wg.Done(); errs <- c.startOSD(t.Context(), copyOfLaunch) }(launch)
	}
	<-entered
	close(release)
	wg.Wait()
	close(errs)
	passed := 0
	for err := range errs {
		if err == nil {
			passed++
		}
	}
	if calls.Load() != 1 || passed != 1 {
		t.Fatalf("created %d containers, accepted %d starts", calls.Load(), passed)
	}
}

func TestOSDLaunchRefusesRebindingAndIncompletePreparation(t *testing.T) {
	c, launch := preparedOSDLaunch(t)
	previous := &initialOSDDaemon{id: launch.osd.ID}
	launch.osd.Container = previous
	c.stores.runner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		t.Fatal("refused launch reached container creation")
		return nil, nil
	}
	if err := c.startOSD(t.Context(), launch); err == nil || launch.osd.Container != previous {
		t.Fatal("existing container was rebound")
	}
	for _, incomplete := range []osdLaunch{{}, {osd: &OSDContainer{ID: 7}}} {
		if err := c.startOSD(t.Context(), incomplete); err == nil {
			t.Fatal("incomplete registration reached container creation")
		}
	}
}

func TestOSDLaunchRecipeConcurrentRequestsHaveIndependentReaders(t *testing.T) {
	_, launch := preparedOSDLaunch(t)
	var wg sync.WaitGroup
	snapshots := make(chan map[string]osdLaunchFileSnapshot, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var req testcontainers.GenericContainerRequest
			if err := launch.recipe.Customize(&req); err != nil {
				t.Error(err)
				return
			}
			snapshots <- consumeOSDLaunchFiles(t, req)
		}()
	}
	wg.Wait()
	close(snapshots)
	var previous map[string]osdLaunchFileSnapshot
	count := 0
	for files := range snapshots {
		if previous != nil && !reflect.DeepEqual(previous, files) {
			t.Fatal("concurrent requests shared consumed Readers")
		}
		previous = files
		count++
	}
	if count != 2 || launch.recipe.started.Load() {
		t.Fatal("building requests changed creation admission")
	}
}

func TestOSDLaunchCancelledBeforeCreationDoesNotConsumeAttempt(t *testing.T) {
	c, launch := preparedOSDLaunch(t)
	c.stores.runner = func(context.Context, string, ...testcontainers.ContainerCustomizer) (testcontainers.Container, error) {
		t.Fatal("cancelled launch reached container creation")
		return nil, nil
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := c.startOSD(ctx, launch); !errors.Is(err, context.Canceled) || launch.recipe.started.Load() || launch.osd.Container != nil {
		t.Fatalf("cancelled preflight changed creation state: %v", err)
	}
}
