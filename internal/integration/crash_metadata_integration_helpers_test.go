//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// testCrashMetadata makes the MGR and an OSD crash for real, with process
// memory dumps disabled, and collects their crash metadata from the stopped
// containers, next to a gracefully stopped OSD, a running MON and a
// caller-owned helper that is later removed.
func testCrashMetadata(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	baseline := dockerObjects(t, ctx, docker)

	cluster, client := newServiceCluster(t, append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2).WithPoolDefaults(1, 1))}, opts...)...)

	// A cluster whose bootstrap failed is read as far as it got. It starts
	// after the first cluster, so that no image pull counts against its
	// short startup timeout.
	image, imageOpts := integrationImages(t)
	partial, err := ceph.Run(ctx, image, append(imageOpts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)), ceph.WithStartupTimeout(3*time.Second))...)
	if partial == nil || err == nil {
		if partial != nil {
			_ = partial.Terminate(context.WithoutCancel(ctx))
		}
		t.Fatalf("a three second bootstrap returned %v, %v", partial, err)
	}
	partialReport, partialErr := partial.CollectCrashMetadata(ctx, ceph.CrashMetadataConfig{})
	if terminateErr := partial.Terminate(context.WithoutCancel(ctx)); terminateErr != nil {
		t.Fatal(terminateErr)
	}
	if partialErr != nil || len(partialReport.Containers) == 0 {
		t.Fatalf("partial cluster: %v; %+v", partialErr, partialReport)
	}
	for _, result := range partialReport.Containers {
		if result.Outcome != ceph.CrashDirectoryEmpty && result.Outcome != ceph.CrashContainerUnavailable {
			t.Fatalf("partial cluster %s %s: %+v", result.Role, result.Name, result)
		}
	}
	t.Logf("partial bootstrap (%v): %d containers read", err, len(partialReport.Containers))

	cc, err := cluster.ConnectionConfig(ctx)
	keyring := cc.Keyring
	if err != nil {
		t.Fatal(err)
	}
	adminKey := ""
	for _, line := range strings.Split(string(keyring), "\n") {
		if key, value, ok := strings.Cut(strings.TrimSpace(line), "="); ok && strings.TrimSpace(key) == "key" {
			adminKey = strings.TrimSpace(value)
		}
	}
	helper, err := testcontainers.Run(ctx, nativeClientImage(t, staticFacts(cluster).Images.Client), cluster.WithClient(), ceph.WithIdleEntrypoint())
	testcontainers.CleanupContainer(t, helper)
	if err != nil {
		t.Fatal(err)
	}
	// Initialize only this fresh, idle caller helper's directory, including
	// any package-owned crash/posted skeleton. Daemon crash files stay intact.
	// Use its writable rootfs so Docker's archive API observes the same path
	// as Exec, without renaming an overlay lower-layer directory.
	execCommand(t, ctx, helper, "python3", "-c", `import os, shutil
p = '/var/lib/ceph/crash'
if os.path.exists(p):
    shutil.rmtree(p)
os.makedirs(p)
`)
	config := ceph.CrashMetadataConfig{AdditionalContainers: []ceph.DiagnosticsContainer{{Role: "client", Name: "helper", Container: helper}}}
	osds := clusteraccess.OSDs(t, cluster)

	// Before any crash, every crash directory is empty.
	before := dockerObjects(t, ctx, docker)
	report, err := cluster.CollectCrashMetadata(ctx, config)
	if err != nil || !report.Complete {
		t.Fatalf("collection before any crash: %v; %+v", err, report)
	}
	for _, result := range report.Containers {
		if result.Outcome != ceph.CrashDirectoryEmpty || result.State != "running" {
			t.Fatalf("%s %s before any crash: %+v", result.Role, result.Name, result)
		}
	}
	if after := dockerObjects(t, ctx, docker); after.containers != before.containers || after.volumes != before.volumes {
		t.Fatalf("collection changed Docker objects: %+v -> %+v", before, after)
	}
	// A caller's absent directory is read whatever it is called, also when
	// described like the cluster's own control container. Only the empty
	// directory created above is removed, then recreated for crash collection.
	execCommand(t, ctx, helper, "python3", "-c", `import os; os.rmdir('/var/lib/ceph/crash')`)
	described := ceph.CrashMetadataConfig{AdditionalContainers: []ceph.DiagnosticsContainer{{Role: "control", Name: "cli", Container: helper}}}
	if report, err := cluster.CollectCrashMetadata(ctx, described); err != nil || !report.Complete ||
		!slices.ContainsFunc(report.Containers, func(result ceph.CrashMetadataContainer) bool {
			return result.ContainerID == helper.GetContainerID() && result.Outcome == ceph.CrashDirectoryAbsent
		}) {
		t.Fatalf("a caller container described as control/cli: %v; %+v", err, report)
	}

	execCommand(t, ctx, helper, "mkdir", "/var/lib/ceph/crash")

	crashDaemon(t, ctx, clusteraccess.InitialManager(t, cluster))
	crashDaemon(t, ctx, osds[0].Container)
	stop := 10 * time.Second
	if err := osds[1].Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	report, err = cluster.CollectCrashMetadata(ctx, config)
	if err != nil || !report.Complete {
		t.Fatalf("collection after the crashes: %v; %+v", err, report)
	}
	results := map[string]ceph.CrashMetadataContainer{}
	for _, result := range report.Containers {
		results[result.Role+"."+result.Name] = result
	}
	for name, ctr := range map[string]testcontainers.Container{"mgr.a": clusteraccess.InitialManager(t, cluster), "osd.0": osds[0].Container} {
		result := results[name]
		if result.Outcome != ceph.CrashRecordsFound || len(result.Records) != 1 || result.State != "exited" {
			t.Fatalf("%s after its crash: %+v", name, result)
		}
		record := result.Records[0]
		started, startErr := time.Parse(time.RFC3339Nano, result.StartedAt)
		finished, finishErr := time.Parse(time.RFC3339Nano, result.FinishedAt)
		crashed, crashErr := time.Parse(time.RFC3339Nano, record.Timestamp)
		if record.Entity != name || record.Process != "ceph-"+strings.Split(name, ".")[0] || !strings.HasPrefix(record.CephVersion, staticFacts(cluster).CephVersion) ||
			!record.Done || record.Posted || errors.Join(startErr, finishErr, crashErr) != nil || crashed.Before(started) || crashed.After(finished) {
			t.Fatalf("%s record %+v does not match the container from %s to %s", name, record, result.StartedAt, result.FinishedAt)
		}
		if backtrace, ok := record.Fields["backtrace"].([]string); !ok || len(backtrace) == 0 {
			t.Fatalf("%s record has no backtrace: %+v", name, record.Fields)
		}
		if logs := containerLogText(t, ctx, ctr); !strings.Contains(logs, "Caught signal (Segmentation fault)") {
			t.Fatalf("%s logs do not show the crash", name)
		}
		t.Logf("%s: crash %s at %s, Ceph %s, exit code %d, image %s; ignored entries %d", name, record.CrashID, record.Timestamp, record.CephVersion, result.ExitCode, result.ImageID, result.IgnoredEntries)
	}
	for name, state := range map[string]string{"osd.1": "exited", "mon.a": "running", "client.helper": "running"} {
		if result := results[name]; result.Outcome != ceph.CrashDirectoryEmpty || result.State != state {
			t.Fatalf("%s: %+v", name, result)
		}
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	for _, leaked := range []string{adminKey, "begin dump of recent events", "core."} {
		if leaked == "" || strings.Contains(string(data), leaked) {
			t.Fatalf("the report contains %q", leaked)
		}
	}

	// A removed caller container and an owned OSD container removed behind
	// the cluster's back are unavailable; the other records stay.
	if err := helper.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := docker.ContainerRemove(ctx, osds[1].GetContainerID(), mobycl.ContainerRemoveOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	removed := dockerObjects(t, ctx, docker)
	report, err = cluster.CollectCrashMetadata(ctx, config)
	if err != nil || report.Complete {
		t.Fatalf("collection after the removals: %v; complete %t", err, report.Complete)
	}
	for _, result := range report.Containers {
		switch result.Role + "." + result.Name {
		case "client.helper", "osd.1":
			if result.Outcome != ceph.CrashContainerUnavailable {
				t.Fatalf("removed %s: %+v", result.Name, result)
			}
		case "mgr.a", "osd.0":
			if result.Outcome != ceph.CrashRecordsFound {
				t.Fatalf("%s lost its records: %+v", result.Name, result)
			}
		}
	}

	// A canceled context still returns the report. A cleanup then collects
	// with a fresh context, as after a canceled test context.
	canceled, cancelNow := context.WithCancel(ctx)
	cancelNow()
	if report, err := cluster.CollectCrashMetadata(canceled, config); err == nil || report == nil || report.Complete {
		t.Fatalf("canceled collection: %v; %+v", err, report)
	}
	fresh, cancelFresh := context.WithTimeout(context.Background(), time.Minute)
	defer cancelFresh()
	if report, err := cluster.CollectCrashMetadata(fresh, config); err != nil || len(report.Containers) == 0 {
		t.Fatalf("fresh collection: %v", err)
	}
	if after := dockerObjects(t, ctx, docker); after.containers != removed.containers || after.volumes != removed.volumes {
		t.Fatalf("collection changed Docker objects: %+v -> %+v", removed, after)
	}

	// Docker can create a container although the create reply was lost, as
	// when a bootstrap's context ends. Such a container, here with the run
	// volume like a daemon, carries the cluster's label, and Terminate
	// removes it before the volume.
	lostCreate(t, ctx, docker, cluster)

	// The collections left nothing behind once the fixtures are gone.
	if err := errors.Join(client.Terminate(ctx), cluster.Terminate(ctx)); err != nil {
		t.Fatal(err)
	}
	if after := dockerObjects(t, ctx, docker); after.containers != baseline.containers || after.volumes != baseline.volumes {
		t.Fatalf("Docker objects after the teardown: %+v, before the clusters %+v", after, baseline)
	}
}

// lostCreate creates a container with the cluster's label and run volume, as
// Docker does for a create request whose reply was lost.
func lostCreate(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, cluster *ceph.Container) {
	t.Helper()
	inspection, err := clusteraccess.Monitors(t, cluster)[0].Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	owner := inspection.Config.Labels["org.testcontainers.ceph.cluster"]
	var mounts []mount.Mount
	for _, used := range inspection.Mounts {
		if used.Type == mount.TypeVolume && strings.HasPrefix(used.Name, "tc-ceph-run-") {
			mounts = append(mounts, mount.Mount{Type: mount.TypeVolume, Source: used.Name, Target: used.Destination})
		}
	}
	if owner == "" || len(mounts) != 1 {
		t.Fatalf("the MON container has the cluster label %q and run volume mounts %v", owner, mounts)
	}
	if _, err := docker.ContainerCreate(ctx, mobycl.ContainerCreateOptions{
		Config:     &container.Config{Image: staticFacts(cluster).Images.Essential, Cmd: []string{"true"}, Labels: map[string]string{"org.testcontainers.ceph.cluster": owner}},
		HostConfig: &container.HostConfig{Mounts: mounts},
	}); err != nil {
		t.Fatal(err)
	}
}

// crashDaemon makes the daemon that runs as PID 1 of ctr crash with SIGSEGV,
// with core dumps disabled, and waits until the container has stopped.
func crashDaemon(t *testing.T, ctx context.Context, ctr testcontainers.Container) {
	t.Helper()
	execOutput(t, ctx, ctr, "sh", "-c", "prlimit --pid 1 --core=0:0 && kill -SEGV 1")
	deadline := time.Now().Add(2 * time.Minute)
	for {
		state, err := ctr.State(ctx)
		if err == nil && !state.Running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the crashed daemon's container still runs: %+v %v", state, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func containerLogText(t *testing.T, ctx context.Context, ctr testcontainers.Container) string {
	t.Helper()
	logs, err := ctr.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logs.Close() }()
	data, err := io.ReadAll(io.LimitReader(logs, 64<<20))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

type dockerCounts struct {
	containers, volumes int
	// names lists the containers and volumes, for a failure message.
	names string
}

// dockerObjects counts the containers, except Testcontainers' reaper, and the
// volumes of the Docker engine.
func dockerObjects(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient) dockerCounts {
	t.Helper()
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	ctrs, names := 0, []string{}
	for _, ctr := range list.Items {
		if !strings.Contains(ctr.Image, "testcontainers/ryuk") {
			ctrs++
			names = append(names, fmt.Sprintf("container %.12s %s %s %v", ctr.ID, ctr.Image, ctr.State, ctr.Names))
		}
	}
	volumes, err := docker.VolumeList(ctx, mobycl.VolumeListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, volume := range volumes.Items {
		names = append(names, "volume "+volume.Name)
	}
	return dockerCounts{containers: ctrs, volumes: len(volumes.Items), names: strings.Join(names, "; ")}
}
