//go:build all || integration

package integration_test

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusterutil"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/google/uuid"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// The consumer MGR module of TestMGRImageConsumerModule. tcprobe is one small
// module with one option and one read command, registered in the module's own
// command registry where the release has one (Tentacle and later) and in the
// global one on Squid; tcprobe_missing reports a
// dependency the image does not ship through can_run, as in-tree modules with
// optional dependencies do.
const (
	tcprobeModule = `import json

from mgr_module import HandleCommandResult, MgrModule, Option

try:
    from mgr_module import CLICommandBase
    ProbeCommand = CLICommandBase.make_registry_subtype('TCProbeCommand')
    read_command = ProbeCommand.Read
except ImportError:  # Squid has one global command registry
    from mgr_module import CLIReadCommand as read_command
    ProbeCommand = None


class Module(MgrModule):
    if ProbeCommand is not None:
        CLICommand = ProbeCommand
    MODULE_OPTIONS = [
        Option(name='level', type='int', default=1, min=1, max=9, desc='fixture probe level'),
    ]

    @read_command('tcprobe status')
    def status(self) -> HandleCommandResult:
        """Report the fixture probe level."""
        return HandleCommandResult(stdout=json.dumps({'level': self.get_module_option('level'), 'manager': self.get_mgr_id()}))
`
	tcprobeMissingModule = `from mgr_module import MgrModule

try:
    import tc_probe_absent_dependency  # noqa: F401
    missing = ''
except ImportError as e:
    missing = str(e)


class Module(MgrModule):
    @classmethod
    def can_run(cls):
        if missing:
            return False, 'tcprobe dependency unavailable: ' + missing
        return True, ''
`
	moduleInit = "from .module import Module  # noqa: F401\n"
)

var cephReleasePattern = regexp.MustCompile(`^ceph version (\d+\.\d+\.\d+)\S* `)

// mgrImageDocker is the Docker client of the MGR image tests.
func mgrImageDocker(t *testing.T, ctx context.Context) *mobycl.Client {
	t.Helper()
	client, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client.Client
}

// imageRelease reads the release that the ceph CLI of image reports, in a
// helper container of that image.
func imageRelease(t *testing.T, ctx context.Context, image string) string {
	t.Helper()
	helper, err := testcontainers.Run(ctx, image, ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, helper)
	if err != nil {
		t.Fatal(err)
	}
	output := execOutput(t, ctx, helper, "ceph", "--version")
	match := cephReleasePattern.FindStringSubmatch(strings.TrimSpace(output) + " ")
	if match == nil {
		t.Fatalf("unrecognized ceph --version output of %s: %q", image, output)
	}
	if err := helper.Terminate(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}
	return match[1]
}

// execOutput runs args in ctr and returns its output; a failure fails the test.
func execOutput(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) string {
	t.Helper()
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("%v exited %d: %s %v", args, code, output, err)
	}
	return string(output)
}

// officialMGRImage returns the official image of the essential image's release
// and records its immutable identity.
func officialMGRImage(t *testing.T, ctx context.Context, docker *mobycl.Client, essential string) string {
	t.Helper()
	release := imageRelease(t, ctx, essential)
	// The unmodified official image of the essential image's release; CI
	// pulls it before the batch from this reference prefix.
	official := "quay.io/ceph/ceph:v" + release
	if got := imageRelease(t, ctx, official); got != release {
		t.Fatalf("official image %s reports %s", official, got)
	}
	inspect, err := docker.ImageInspect(ctx, official)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MGR slot image %s: id %s, digests %v, platform %s/%s", official, inspect.ID, inspect.RepoDigests, inspect.Os, inspect.Architecture)
	return official
}

// buildMGRTestImage builds an image FROM base with dockerfile and files and
// removes it when the test ends, after the clusters that use it, which are
// registered later.
func buildMGRTestImage(t *testing.T, ctx context.Context, docker *mobycl.Client, base, dockerfile string, files map[string]string) string {
	t.Helper()
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	names := []string{"Dockerfile"}
	files["Dockerfile"] = "ARG BASE\nFROM ${BASE}\n" + dockerfile
	for name := range files {
		if name != "Dockerfile" {
			names = append(names, name)
		}
	}
	slices.Sort(names[1:])
	for _, name := range names {
		if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(files[name])), ModTime: time.Unix(0, 0)}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write([]byte(files[name])); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := provider.Close(); err != nil {
			t.Error(err)
		}
	}()
	var log bytes.Buffer
	tag, err := provider.BuildImage(ctx, &testcontainers.ContainerRequest{FromDockerfile: testcontainers.FromDockerfile{
		ContextArchive: bytes.NewReader(archive.Bytes()),
		Repo:           "ceph-testcontainers-mgr-image-test", Tag: uuid.NewString(),
		BuildArgs:      map[string]*string{"BASE": &base},
		BuildLogWriter: &log,
	}})
	if tag != "" {
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if _, err := docker.ImageRemove(ctx, tag, mobycl.ImageRemoveOptions{PruneChildren: true}); err != nil {
				t.Errorf("remove test image %s: %v", tag, err)
			}
		})
	}
	if err != nil {
		t.Fatalf("build test image from %s: %v\n%s", base, err, log.String())
	}
	return tag
}

// containerImageID is the image ID that Docker reports for a container.
func containerImageID(t *testing.T, ctx context.Context, docker *mobycl.Client, ctr testcontainers.Container) string {
	t.Helper()
	inspect, err := docker.ContainerInspect(ctx, ctr.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil || inspect.Container.Image == "" {
		t.Fatalf("inspect container %s: %v", ctr.GetContainerID(), err)
	}
	return inspect.Container.Image
}

func localImageID(t *testing.T, ctx context.Context, docker *mobycl.Client, image string) string {
	t.Helper()
	inspect, err := docker.ImageInspect(ctx, image)
	if err != nil || inspect.ID == "" {
		t.Fatalf("inspect image %s: %v", image, err)
	}
	return inspect.ID
}

// runMGRImageCluster starts a one-OSD cluster from images with managers
// initial MGRs, none when managers is 0, and registers its cleanup.
func runMGRImageCluster(t *testing.T, ctx context.Context, images ceph.ImageSet, managers int) (*ceph.Container, error) {
	t.Helper()
	config := ceph.ClusterConfig{}.WithOSDCount(1).WithManagerCount(managers)
	if managers == 0 {
		config = config.WithNoInitialManagers()
	}
	cluster, err := ceph.Run(ctx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), harness.FunctionalOSDConfig(t),
		ceph.WithClusterConfig(config), ceph.WithStartupTimeout(3*time.Minute))
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	return cluster, err
}

// waitManagers waits until the MGR map has an available active MGR among
// active and the other members as its standbys, and returns the active name.
func waitManagers(t *testing.T, ctx context.Context, cluster *ceph.Container, members []string, active ...string) string {
	t.Helper()
	members = slices.Sorted(slices.Values(members))
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last ceph.ManagerStatus
	var err error
	for {
		var status ceph.ManagerStatus
		if status, err = harness.ObserveCluster(cluster).ManagerStatus(ctx); err == nil {
			last = status
			names := []string{status.ActiveName}
			for _, standby := range status.Standbys {
				names = append(names, standby.Name)
			}
			slices.Sort(names)
			if status.Available && slices.Contains(active, status.ActiveName) && slices.Equal(names, members) {
				return status.ActiveName
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("MGR map did not reach members %v with an active one of %v: %+v %v", members, active, last, err)
		case <-time.After(time.Second):
		}
	}
}

// mgrModule returns the observed state of one module.
func mgrModule(t *testing.T, ctx context.Context, cluster *ceph.Container, name string) ceph.MGRModuleState {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	for {
		modules, err := harness.ObserveCluster(cluster).MGRModules(ctx)
		if err == nil {
			for _, module := range modules {
				if module.Name == name && module.Available {
					return module
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("MGR module %s is not advertised: %v %v", name, modules, err)
		case <-time.After(time.Second):
		}
	}
}

// tcprobeStatus is the reply of the consumer module's read command.
type tcprobeStatus struct {
	Level   *int   `json:"level"`
	Manager string `json:"manager"`
}

// waitTCProbe polls the consumer module's own command until it answers from
// manager with level.
func waitTCProbe(t *testing.T, ctx context.Context, cluster *ceph.Container, manager string, level int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	for {
		output, err := cluster.Ceph(ctx, "tcprobe", "status")
		var status tcprobeStatus
		if err == nil && json.Unmarshal(output, &status) == nil && status.Level != nil && *status.Level == level && status.Manager == manager {
			return
		}
		last = string(output)
		if err != nil {
			last += " " + err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("tcprobe status did not report level %d from mgr.%s: %s", level, manager, last)
		case <-time.After(2 * time.Second):
		}
	}
}

// assertClusterContainersGone requires that no container with the cluster's
// owner label remains after Terminate.
func assertClusterContainersGone(t *testing.T, ctx context.Context, docker *mobycl.Client, owner string) {
	t.Helper()
	listed, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", clusterutil.OwnerLabel+"="+owner)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 0 {
		var ids []string
		for _, item := range listed.Items {
			ids = append(ids, item.ID[:12]+" "+item.Image+" "+string(item.State))
		}
		t.Fatalf("containers of the terminated cluster remain: %v", ids)
	}
}

// clusterOwner reads the owner label of the cluster's first MON.
func clusterOwner(t *testing.T, ctx context.Context, docker *mobycl.Client, cluster *ceph.Container) string {
	t.Helper()
	inspect, err := docker.ContainerInspect(ctx, cluster.GetContainerID(), mobycl.ContainerInspectOptions{})
	if err != nil || inspect.Container.Config == nil || inspect.Container.Config.Labels[clusterutil.OwnerLabel] == "" {
		t.Fatalf("cluster owner label unavailable: %v", err)
	}
	return inspect.Container.Config.Labels[clusterutil.OwnerLabel]
}

// essentialBase is the essential image of the integration image set.
func essentialBase(images ceph.ImageSet) string {
	if images.All != "" {
		return images.All
	}
	return images.Essential
}

// testMGRImageOfficialSlot runs the initial active and standby MGRs and the
// later AddManager candidates from the official image in the MGR slot, with
// the role images for the other daemons, and checks the identity each layer
// reports: ClusterFacts, the handles, Docker and the running daemons.
func testMGRImageOfficialSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker := mgrImageDocker(t, ctx)
	images := harness.IntegrationImageSet(t)
	essential := essentialBase(images)
	official := officialMGRImage(t, ctx, docker, essential)
	images.MGR = official
	cluster, err := runMGRImageCluster(t, ctx, images, 2)
	if err != nil {
		t.Fatal(err)
	}
	facts := harness.StaticFacts(cluster)
	if facts.Images.MGR != official || facts.Images.Essential != essential {
		t.Fatalf("facts images %+v", facts.Images)
	}
	officialID, essentialID := localImageID(t, ctx, docker, official), localImageID(t, ctx, docker, essential)
	if officialID == essentialID {
		t.Fatal("the official and essential images are the same image; the test cannot tell them apart")
	}
	if got := containerImageID(t, ctx, docker, cluster); got != essentialID {
		t.Fatalf("MON a runs image %s, want the essential image %s", got, essentialID)
	}
	waitManagers(t, ctx, cluster, []string{"a", "b"}, "a")
	added, err := cluster.AddManager(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	overridden, err := cluster.AddManager(ctx, "d", ceph.DaemonOptions{}.WithImage(essential))
	if err != nil {
		t.Fatal(err)
	}
	waitManagers(t, ctx, cluster, []string{"a", "b", "c", "d"}, "a")
	want := map[string]string{"a": official, "b": official, "c": official, "d": essential}
	wantID := map[string]string{"a": officialID, "b": officialID, "c": officialID, "d": essentialID}
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		if mgr.Image() != want[mgr.DaemonName] {
			t.Fatalf("mgr.%s handle reports %q, want %q", mgr.DaemonName, mgr.Image(), want[mgr.DaemonName])
		}
		if got := containerImageID(t, ctx, docker, mgr); got != wantID[mgr.DaemonName] {
			t.Fatalf("mgr.%s container runs %s, want %s", mgr.DaemonName, got, wantID[mgr.DaemonName])
		}
		version, err := mgr.Version(ctx)
		if err != nil || version != facts.CephVersion {
			t.Fatalf("mgr.%s reports release %q (%v), cluster %s", mgr.DaemonName, version, err, facts.CephVersion)
		}
	}
	if added.Image() != official || overridden.Image() != essential {
		t.Fatalf("AddManager handles %q %q", added.Image(), overridden.Image())
	}
	// The official image ships the dashboard module, which the active MGR
	// advertises from its own image.
	dashboard := mgrModule(t, ctx, cluster, "dashboard")
	t.Logf("official MGR advertises dashboard: can_run=%t %q", dashboard.CanRun, dashboard.ErrorString)
	// A restarted standby keeps its container and image.
	standby := harness.ManagerHandle(t, ctx, cluster, "b")
	before := standby.GetContainerID()
	stopTimeout := 30 * time.Second
	if err := standby.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	waitManagers(t, ctx, cluster, []string{"a", "c", "d"}, "a")
	if err := standby.Start(ctx); err != nil {
		t.Fatal(err)
	}
	waitManagers(t, ctx, cluster, []string{"a", "b", "c", "d"}, "a")
	if standby.GetContainerID() != before || standby.Image() != official || containerImageID(t, ctx, docker, standby) != officialID {
		t.Fatal("restarting the standby changed its container or image")
	}
	// Removing the active MGR fails over to a standby; the remaining handles
	// keep their images.
	if err := harness.ManagerHandle(t, ctx, cluster, "a").Remove(ctx); err != nil {
		t.Fatal(err)
	}
	active := waitManagers(t, ctx, cluster, []string{"b", "c", "d"}, "b", "c", "d")
	t.Logf("official MGR slot: initial a (active) and b, added c from %s, d from the essential override; after removing a, mgr.%s is active", official, active)
}

// testMGRImageWithoutEssentialManager is the no-essential-dependency
// regression: an essential image without ceph-mgr cannot start a cluster on
// its own, and the partial cluster is removed completely, while the same
// essential image with the official image in the MGR slot starts managers
// that need nothing from the essential image.
func testMGRImageWithoutEssentialManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker := mgrImageDocker(t, ctx)
	images := harness.IntegrationImageSet(t)
	base := essentialBase(images)
	official := officialMGRImage(t, ctx, docker, base)
	withoutMGR := buildMGRTestImage(t, ctx, docker, base, "RUN rm /usr/bin/ceph-mgr && ! command -v ceph-mgr\n", map[string]string{})
	if images.All != "" {
		images = ceph.ImageSet{Client: images.All}
	}
	images.Essential = withoutMGR

	failCtx, failCancel := context.WithTimeout(ctx, 4*time.Minute)
	cluster, err := ceph.Run(failCtx, "", ceph.WithImageOptions(ceph.ImageOptions{}.WithImageSet(images)), harness.FunctionalOSDConfig(t),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)), ceph.WithStartupTimeout(2*time.Minute))
	failCancel()
	if err == nil || cluster == nil {
		t.Fatalf("an essential image without ceph-mgr started a cluster without a MGR slot: %v", err)
	}
	testcontainers.CleanupContainer(t, cluster)
	managers := clusteraccess.Managers(t, cluster)
	if len(managers) != 1 || managers[0].Image() != withoutMGR {
		t.Fatalf("failed start does not own its MGR from the essential image: %+v", managers)
	}
	owner := clusterOwner(t, ctx, docker, cluster)
	t.Logf("without a MGR slot Run failed as expected: %v", err)
	if err := cluster.Terminate(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}
	assertClusterContainersGone(t, ctx, docker, owner)

	images.MGR = official
	cluster, err = runMGRImageCluster(t, ctx, images, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitManagers(t, ctx, cluster, []string{"a"}, "a")
	if _, err := cluster.AddManager(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	waitManagers(t, ctx, cluster, []string{"a", "b"}, "a")
	officialID := localImageID(t, ctx, docker, official)
	for _, mgr := range clusteraccess.Managers(t, cluster) {
		if mgr.Image() != official || containerImageID(t, ctx, docker, mgr) != officialID {
			t.Fatalf("mgr.%s does not run the MGR slot image", mgr.DaemonName)
		}
	}
	if output := execOutput(t, ctx, cluster, "sh", "-c", "command -v ceph-mgr || echo absent"); strings.TrimSpace(output) != "absent" {
		t.Fatalf("the essential MON image ships ceph-mgr: %q", output)
	}
}

// testMGRImageConsumerModule qualifies a consumer MGR image built from the
// official image with one additional module, and a second module whose
// dependency the image lacks: the fixture reports both through MGRModules,
// enables the runnable one with override.SetMGRModule, refuses the other,
// configures the module with override.SetConfig (and refuses a value that Ceph
// rejects for the option's type), and keeps the image for an added MGR that
// takes over after a failover.
func testMGRImageConsumerModule(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker := mgrImageDocker(t, ctx)
	images := harness.IntegrationImageSet(t)
	official := officialMGRImage(t, ctx, docker, essentialBase(images))
	consumer := buildMGRTestImage(t, ctx, docker, official,
		"COPY tcprobe /usr/share/ceph/mgr/tcprobe\nCOPY tcprobe_missing /usr/share/ceph/mgr/tcprobe_missing\n",
		map[string]string{
			"tcprobe/__init__.py": moduleInit, "tcprobe/module.py": tcprobeModule,
			"tcprobe_missing/__init__.py": moduleInit, "tcprobe_missing/module.py": tcprobeMissingModule,
		})
	images.MGR = consumer
	cluster, err := runMGRImageCluster(t, ctx, images, 1)
	if err != nil {
		t.Fatal(err)
	}
	if harness.StaticFacts(cluster).Images.MGR != consumer {
		t.Fatalf("facts MGR image %q", harness.StaticFacts(cluster).Images.MGR)
	}
	probe := mgrModule(t, ctx, cluster, "tcprobe")
	missing := mgrModule(t, ctx, cluster, "tcprobe_missing")
	if probe.Enabled || !probe.CanRun || probe.ErrorString != "" {
		t.Fatalf("tcprobe state %+v", probe)
	}
	if missing.Enabled || missing.CanRun || !strings.Contains(missing.ErrorString, "tc_probe_absent_dependency") {
		t.Fatalf("tcprobe_missing state %+v", missing)
	}
	refused, err := override.SetMGRModule(ctx, cluster, "tcprobe_missing", true)
	if err == nil {
		t.Fatal("a module whose dependency is missing was enabled")
	}
	if err := refused.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if again := mgrModule(t, ctx, cluster, "tcprobe_missing"); again.Enabled {
		t.Fatal("the refused module is enabled")
	}
	enabled, err := override.SetMGRModule(ctx, cluster, "tcprobe", true)
	if err != nil {
		t.Fatal(err)
	}
	waitTCProbe(t, ctx, cluster, "a", 1)
	// Ceph validates a module option against the type the module declares:
	// a non-integer level is refused by the MON and the module keeps its
	// value.
	invalid, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: "mgr/tcprobe/level", Value: "seven"})
	var native *ceph.NativeFailure
	if err == nil || !errors.As(err, &native) || native.Errno != "EINVAL" {
		t.Fatalf("a non-integer value for an int module option was not refused by Ceph: %v", err)
	}
	t.Logf("non-integer tcprobe level refused by Ceph: %v", err)
	if err := invalid.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	waitTCProbe(t, ctx, cluster, "a", 1)
	level, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mgr", Name: "mgr/tcprobe/level", Value: "7"})
	if err != nil {
		t.Fatal(err)
	}
	waitTCProbe(t, ctx, cluster, "a", 7)
	// An added MGR uses the same consumer image and serves the module after
	// the active MGR is removed.
	added, err := cluster.AddManager(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if added.Image() != consumer || containerImageID(t, ctx, docker, added) != localImageID(t, ctx, docker, consumer) {
		t.Fatalf("added MGR image %q", added.Image())
	}
	waitManagers(t, ctx, cluster, []string{"a", "b"}, "a")
	if err := harness.ManagerHandle(t, ctx, cluster, "a").Remove(ctx); err != nil {
		t.Fatal(err)
	}
	waitManagers(t, ctx, cluster, []string{"b"}, "b")
	waitTCProbe(t, ctx, cluster, "b", 7)
	if err := level.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := enabled.Restore(ctx); err != nil {
		t.Fatal(err)
	}
}

// otherRelease is a release that differs from release, for an image that
// claims to be of another release than the cluster.
func otherRelease(release string) string {
	if release == "19.2.5" {
		return "20.2.4"
	}
	return "19.2.5"
}

// assertNoManagerEntered requires that no MGR identity exists, that the MGR
// map has neither an active MGR nor a standby, and that no container of the
// cluster was ever created from image: neither a daemon nor a release probe
// that was left behind.
func assertNoManagerEntered(t *testing.T, ctx context.Context, docker *mobycl.Client, cluster *ceph.Container, owner, image string) {
	t.Helper()
	var mgrMap struct {
		ActiveName string `json:"active_name"`
		ActiveGID  uint64 `json:"active_gid"`
		Standbys   []struct {
			Name string `json:"name"`
		} `json:"standbys"`
	}
	dump := execOutput(t, ctx, cluster, "ceph", "mgr", "dump", "--format", "json")
	if err := json.Unmarshal([]byte(dump), &mgrMap); err != nil {
		t.Fatalf("decode mgr dump: %v %s", err, dump)
	}
	if mgrMap.ActiveName != "" || mgrMap.ActiveGID != 0 || len(mgrMap.Standbys) != 0 {
		t.Fatalf("a MGR of the mismatched image registered: %+v", mgrMap)
	}
	var auth struct {
		AuthDump []struct {
			Entity string `json:"entity"`
		} `json:"auth_dump"`
	}
	listing := execOutput(t, ctx, cluster, "ceph", "auth", "ls", "--format", "json")
	if err := json.Unmarshal([]byte(listing), &auth); err != nil {
		t.Fatalf("decode auth ls: %v", err)
	}
	for _, identity := range auth.AuthDump {
		if strings.HasPrefix(identity.Entity, "mgr.") {
			t.Fatalf("the refused MGR identity %s exists", identity.Entity)
		}
	}
	listed, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", clusterutil.OwnerLabel+"="+owner).Add("ancestor", image)})
	if err != nil {
		t.Fatal(err)
	}
	if len(listed.Items) != 0 {
		t.Fatalf("%d containers of the cluster run the mismatched MGR image", len(listed.Items))
	}
	if managers := clusteraccess.Managers(t, cluster); len(managers) != 0 {
		t.Fatalf("the cluster owns %d MGRs of the mismatched image", len(managers))
	}
}

// testMGRImageOtherReleaseNeverStarts is the release gate of the MGR slot: an
// image whose ceph-mgr reports another release than the cluster is refused by
// the release probe before any MGR identity, key or daemon exists, for the
// initial manager of Run and for AddManager on a cluster without managers.
// The image is the official image with a ceph-mgr wrapper that claims another
// release for --version and runs the real daemon otherwise, so that a
// manager started before the gate would register.
func testMGRImageOtherReleaseNeverStarts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker := mgrImageDocker(t, ctx)
	images := harness.IntegrationImageSet(t)
	official := officialMGRImage(t, ctx, docker, essentialBase(images))
	claimed := otherRelease(imageRelease(t, ctx, official))
	wrapper := "#!/bin/sh\nif [ \"$1\" = --version ]; then\n\techo \"ceph version " + claimed + " (0000000000000000000000000000000000000000) other (stable)\"\n\texit 0\nfi\nexec /usr/bin/ceph-mgr.real \"$@\"\n"
	mismatched := buildMGRTestImage(t, ctx, docker, official,
		"RUN mv /usr/bin/ceph-mgr /usr/bin/ceph-mgr.real\nCOPY ceph-mgr /usr/bin/ceph-mgr\nRUN chmod 0755 /usr/bin/ceph-mgr\n",
		map[string]string{"ceph-mgr": wrapper})
	images.MGR = mismatched
	want := "MGR image Ceph release " + claimed + " differs from core "

	cluster, err := runMGRImageCluster(t, ctx, images, 1)
	if err == nil || cluster == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("Run with a MGR image of release %s: %v", claimed, err)
	}
	t.Logf("Run refused the initial MGR: %v", err)
	assertNoManagerEntered(t, ctx, docker, cluster, clusterOwner(t, ctx, docker, cluster), mismatched)
	if err := cluster.Terminate(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}

	cluster, err = runMGRImageCluster(t, ctx, images, 0)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := cluster.AddManager(ctx, "a")
	if err == nil || mgr != nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("AddManager with a MGR image of release %s: %+v %v", claimed, mgr, err)
	}
	t.Logf("AddManager refused the MGR: %v", err)
	owner := clusterOwner(t, ctx, docker, cluster)
	assertNoManagerEntered(t, ctx, docker, cluster, owner, mismatched)
	// The explicit per-daemon override keeps its unchecked contract: the same
	// image starts and registers.
	overridden, err := cluster.AddManager(ctx, "b", ceph.DaemonOptions{}.WithImage(mismatched))
	if err != nil || overridden.Image() != mismatched {
		t.Fatalf("an explicit per-daemon image was release-checked: %v", err)
	}
	waitManagers(t, ctx, cluster, []string{"b"}, "b")
	if err := cluster.Terminate(context.WithoutCancel(ctx)); err != nil {
		t.Fatal(err)
	}
	assertClusterContainersGone(t, ctx, docker, owner)
}
