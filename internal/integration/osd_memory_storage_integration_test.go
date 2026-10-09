//go:build integration && topology

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/google/uuid"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

const osdMemoryLabel = "org.testcontainers.ceph.osd-memory-owner"
const osdMemoryBytes = int64(2 << 30)
const osdMemoryPayloadBytes = int64(128 << 20)

// One fresh bridge fixture verifies opt-in storage ownership, real replicated
// bytes and recovery. Sampling observes this run; it is not a disk comparison.
func TestOSDInMemoryStorageTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	engine, err := docker.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || engine.Info.ID == "" {
		t.Fatal("native engine identity unavailable", err)
	}
	beforeVolumes, beforeKeepers := osdMemoryLabelInventory(t, ctx, docker.Client)
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDInMemoryStorage(osdMemoryBytes))
	started := time.Now()
	cluster, err := ceph.Run(ctx, image, opts...)
	terminated := false
	owned := map[string]bool{}
	volumeName := ""
	keeperID := ""
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if t.Failed() {
				if keeperID != "" {
					osdMemoryCapacity(t, cleanup, docker.Client, keeperID, "failure")
				}
				for _, osd := range cluster.OSDs() {
					if osd.Container == nil {
						continue
					}
					osdMemoryFailureLogs(t, cleanup, osd.Container)
				}
			}
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("partial memory-storage cluster cleanup failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal("memory-storage control bootstrap failed", err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fsid := q.MonMap.FSID
	if id, err := uuid.Parse(fsid); err != nil || id == uuid.Nil || id.String() != fsid {
		t.Fatal("original native FSID unavailable")
	}
	afterVolumes, afterKeepers := osdMemoryLabelInventory(t, ctx, docker.Client)
	if !slices.Equal(beforeVolumes, afterVolumes) || !slices.Equal(beforeKeepers, afterKeepers) || len(cluster.OSDs()) != 0 || len(noInitialOSDNativeDump(t, ctx, cluster)) != 0 {
		t.Fatal("zero-OSD bootstrap allocated memory storage or registered an OSD")
	}
	t.Logf("OSD_MEMORY_LAZY fsid=%s new_volumes=0 new_keepers=0", fsid)
	for range 2 {
		osd, err := cluster.AddOSD(ctx)
		if osd != nil && osd.Container != nil {
			owned[osd.GetContainerID()] = true
		}
		if err != nil {
			t.Fatal("provision initial memory OSD", err)
		}
	}
	initial := cluster.OSDs()
	if len(initial) != 2 {
		t.Fatal("exact two-OSD ownership unavailable")
	}
	volumeName, keeper := osdMemoryBacking(t, ctx, docker.Client, cluster, initial, image)
	keeperID = keeper.ID
	owned[keeper.ID] = true
	for _, ctr := range append([]testcontainers.Container{cluster.Container}, osdContainers(cluster)...) {
		owned[ctr.GetContainerID()] = true
	}
	for _, manager := range cluster.Managers() {
		owned[manager.GetContainerID()] = true
	}
	network, err := docker.Client.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{})
	if err != nil || network.Network.ID == "" {
		t.Fatal("owned bridge identity unavailable", err)
	}
	stats := &osdMemoryStats{docker: docker.Client}
	defer stats.log(t)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "memory-retained", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8})
	if err != nil || pool == nil {
		t.Fatal("replicated memory pool setup failed", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	poolState, err := cluster.PoolStatus(ctx, pool.Name)
	if err != nil || poolState.ID < 0 || poolState.Size != 2 || poolState.MinSize != 1 {
		t.Fatal("original replicated pool identity unavailable", err)
	}
	identities := osdMemoryIdentities(t, ctx, cluster)
	for _, osd := range initial {
		osdMemoryDirectory(t, ctx, osd, fsid, identities[osd.ID])
	}
	osdMemoryPhase(t, "bootstrap", started)
	started = time.Now()
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "seed")
	osdMemoryCapacity(t, ctx, docker.Client, keeper.ID, "seed")
	stats.sample(ctx, owned)
	osdMemoryPhase(t, "seed-128MiB", started)
	started = time.Now()
	noout, err := cluster.TemporaryOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	grace := 3 * time.Second
	for _, osd := range initial {
		if err := osd.Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
	}
	osdMemoryWaitState(t, ctx, cluster, false)
	for _, osd := range initial {
		raw := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
		if raw.State.Running {
			t.Fatal("original OSD did not stop")
		}
	}
	stoppedKeeper := osdMemoryInspect(t, ctx, docker.Client, keeper.ID)
	if !stoppedKeeper.State.Running || stoppedKeeper.State.StartedAt != keeper.State.StartedAt || !maps.Equal(osdMemoryIdentities(t, ctx, cluster), identities) {
		t.Fatal("stopping every OSD changed its registration or original running keeper")
	}
	osdMemoryVolume(t, ctx, docker.Client, volumeName, keeper.Config.Labels[osdMemoryLabel])
	for _, osd := range initial {
		cid := osd.GetContainerID()
		if err := osd.Start(ctx); err != nil {
			t.Fatal(err)
		}
		if osd.GetContainerID() != cid {
			t.Fatal("restart replaced an OSD container")
		}
	}
	osdMemoryWaitState(t, ctx, cluster, true)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("original memory OSD recovery failed", err)
	}
	if !maps.Equal(osdMemoryIdentities(t, ctx, cluster), identities) {
		t.Fatal("restart replaced an original native OSD UUID")
	}
	for _, osd := range initial {
		osdMemoryDirectory(t, ctx, osd, fsid, identities[osd.ID])
	}
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "both-osds-restarted")
	stats.sample(ctx, owned)
	osdMemoryPhase(t, "stop-all-start-same", started)
	started = time.Now()
	added, err := cluster.AddOSD(ctx)
	if added != nil && added.Container != nil {
		owned[added.GetContainerID()] = true
	}
	if err != nil || added == nil {
		t.Fatal("third memory OSD setup failed", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("third OSD recovery failed", err)
	}
	osdMemoryPlacement(t, ctx, cluster, fsid, poolState.ID, pool.Name, added.ID, -1)
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "third-osd-backfill")
	stats.sample(ctx, owned)
	osdMemoryPhase(t, "add-third-backfill", started)
	started = time.Now()
	original := initial[0]
	oldCID, oldUUID := original.GetContainerID(), identities[original.ID]
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal("drain original memory OSD failed", err)
	}
	if _, err := docker.Client.ContainerInspect(ctx, oldCID, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("drained original container remains", err)
	}
	osdMemoryExec(t, ctx, added, "test", "!", "-e", "/var/lib/ceph/osd/ceph-"+strconv.Itoa(original.ID))
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("post-drain recovery failed", err)
	}
	osdMemoryPlacement(t, ctx, cluster, fsid, poolState.ID, pool.Name, added.ID, original.ID)
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "original-drained")
	replacement, err := cluster.AddOSD(ctx)
	if replacement != nil && replacement.Container != nil {
		owned[replacement.GetContainerID()] = true
	}
	if err != nil || replacement == nil {
		t.Fatal("fresh replacement memory OSD failed", err)
	}
	current := osdMemoryIdentities(t, ctx, cluster)
	if replacement.GetContainerID() == oldCID || current[replacement.ID] == "" || current[replacement.ID] == oldUUID {
		t.Fatal("replacement reused old container or native UUID")
	}
	osdMemoryDirectory(t, ctx, replacement, fsid, current[replacement.ID])
	osdMemoryBacking(t, ctx, docker.Client, cluster, cluster.OSDs(), image)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("replacement recovery failed", err)
	}
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "fresh-replacement")
	stats.sample(ctx, owned)
	t.Logf("OSD_MEMORY_REPLACEMENT old_id=%d new_id=%d reused_numeric_id=%t old_uuid=%s new_uuid=%s old_cid=%s new_cid=%s old_directory_absent_before_add=true", original.ID, replacement.ID, original.ID == replacement.ID, oldUUID, current[replacement.ID], oldCID, replacement.GetContainerID())
	osdMemoryPhase(t, "drain-and-fresh-replacement", started)
	started = time.Now()
	beforeRefusal := noInitialOSDNativeDump(t, ctx, cluster)
	stopSeconds := 1
	if _, err := docker.Client.ContainerStop(ctx, keeper.ID, mobycl.ContainerStopOptions{Timeout: &stopSeconds}); err != nil {
		t.Fatal("stop original keeper failed", err)
	}
	refusalCtx, refusalCancel := context.WithTimeout(ctx, 30*time.Second)
	refused, refusalErr := cluster.AddOSD(refusalCtx)
	contextErr := refusalCtx.Err()
	refusalCancel()
	if refused != nil || refusalErr == nil || contextErr != nil || errors.Is(refusalErr, context.DeadlineExceeded) || refusalErr.Error() != "OSD memory keeper is not running; stored data may be lost" {
		t.Fatal("lost keeper was not a completed pre-registration refusal", refusalErr, contextErr)
	}
	if !slices.Equal(beforeRefusal, noInitialOSDNativeDump(t, ctx, cluster)) || !maps.Equal(current, osdMemoryIdentities(t, ctx, cluster)) || len(cluster.OSDs()) != 3 {
		t.Fatal("lost-keeper refusal changed native OSD IDs/UUIDs or ownership")
	}
	afterRefusal := osdMemoryInspect(t, ctx, docker.Client, keeper.ID)
	if afterRefusal.State.Running || afterRefusal.State.StartedAt != keeper.State.StartedAt {
		t.Fatal("lost keeper was automatically restarted or replaced")
	}
	osdMemoryData(t, ctx, cluster, fsid, poolState.ID, pool.Name, "keeper-stopped-refusal")
	t.Logf("OSD_MEMORY_KEEPER_REFUSAL keeper_cid=%s registered_before=3 registered_after=3 automatic_restart=false", keeper.ID)
	osdMemoryPhase(t, "lost-keeper-refusal", started)
	started = time.Now()
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal("whole memory-storage cleanup failed", err)
	}
	terminated = true
	cleanupVerified := true
	for cid := range owned {
		if _, err := docker.Client.ContainerInspect(cleanup, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			cleanupVerified = false
			t.Error("owned memory fixture container remains", cid, err)
		}
	}
	if _, err := docker.Client.VolumeInspect(cleanup, volumeName, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		cleanupVerified = false
		t.Error("owned tmpfs named volume remains", err)
	}
	if _, err := docker.Client.NetworkInspect(cleanup, network.Network.ID, mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
		cleanupVerified = false
		t.Error("owned memory fixture network remains", err)
	}
	finalEngine, err := docker.Client.Info(cleanup, mobycl.InfoOptions{})
	if err != nil || finalEngine.Info.ID != engine.Info.ID {
		cleanupVerified = false
		t.Error("cleanup engine identity changed", err)
	}
	if cleanupVerified {
		t.Logf("OSD_MEMORY_CLEANUP engine=%s volume=%s owned_containers=%d owned_networks=1 remaining=0", engine.Info.ID, volumeName, len(owned))
	}
	osdMemoryPhase(t, "terminate", started)
}

func osdMemoryFailureLogs(t *testing.T, parent context.Context, ctr testcontainers.Container) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	state, err := ctr.State(ctx)
	t.Logf("OSD_MEMORY_FAILURE_STATE cid=%s state=%+v error=%v", ctr.GetContainerID(), state, err)
	reader, err := ctr.Logs(ctx)
	if err != nil {
		t.Logf("OSD_MEMORY_FAILURE_LOG cid=%s unavailable=%v", ctr.GetContainerID(), err)
		return
	}
	defer reader.Close()
	tail := &osdMemoryLogTail{}
	_, err = io.Copy(tail, reader)
	t.Logf("OSD_MEMORY_FAILURE_LOG cid=%s tail_bytes=%d error=%v\n%s", ctr.GetContainerID(), len(tail.data), err, tail.data)
}

func osdMemoryCapacity(t *testing.T, parent context.Context, docker *mobycl.Client, cid, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	created, err := docker.ExecCreate(ctx, cid, mobycl.ExecCreateOptions{
		AttachStdout: true, AttachStderr: true, TTY: true,
		Cmd: []string{"python3", "-c", `import json,os
s=os.statvfs('/tc/osd-memory')
print(json.dumps({'total_bytes':s.f_blocks*s.f_frsize,'available_bytes':s.f_bavail*s.f_frsize,'free_bytes':s.f_bfree*s.f_frsize,'available_inodes':s.f_favail}))`},
	})
	if err != nil {
		t.Logf("OSD_MEMORY_CAPACITY phase=%s unavailable=%v", phase, err)
		return
	}
	attached, err := docker.ExecAttach(ctx, created.ID, mobycl.ExecAttachOptions{TTY: true})
	if err != nil {
		t.Logf("OSD_MEMORY_CAPACITY phase=%s unavailable=%v", phase, err)
		return
	}
	defer attached.Close()
	data, readErr := io.ReadAll(io.LimitReader(attached.Reader, 4096))
	result, inspectErr := docker.ExecInspect(ctx, created.ID, mobycl.ExecInspectOptions{})
	t.Logf("OSD_MEMORY_CAPACITY phase=%s keeper=%s exit=%d read_error=%v inspect_error=%v data=%s", phase, cid, result.ExitCode, readErr, inspectErr, data)
}

// Keep the most recent bounded logs so an abort after noisy mkfs remains visible.
type osdMemoryLogTail struct{ data []byte }

func (w *osdMemoryLogTail) Write(p []byte) (int, error) {
	const limit = 4 << 20
	n := len(p)
	if n >= limit {
		w.data = append(w.data[:0], p[n-limit:]...)
	} else {
		if discard := len(w.data) + n - limit; discard > 0 {
			copy(w.data, w.data[discard:])
			w.data = w.data[:len(w.data)-discard]
		}
		w.data = append(w.data, p...)
	}
	return n, nil
}

func osdMemoryLabelInventory(t *testing.T, ctx context.Context, docker *mobycl.Client) ([]string, []string) {
	t.Helper()
	filter := make(mobycl.Filters).Add("label", osdMemoryLabel)
	volumes, err := docker.VolumeList(ctx, mobycl.VolumeListOptions{Filters: filter})
	if err != nil {
		t.Fatal(err)
	}
	keepers, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: filter})
	if err != nil {
		t.Fatal(err)
	}
	var names, ids []string
	for _, item := range volumes.Items {
		names = append(names, item.Name)
	}
	for _, item := range keepers.Items {
		ids = append(ids, item.ID)
	}
	slices.Sort(names)
	slices.Sort(ids)
	return names, ids
}

func osdMemoryInspect(t *testing.T, ctx context.Context, docker *mobycl.Client, cid string) container.InspectResponse {
	t.Helper()
	raw, err := docker.ContainerInspect(ctx, cid, mobycl.ContainerInspectOptions{})
	if err != nil || len(cid) != 64 || raw.Container.ID != cid || raw.Container.Config == nil || raw.Container.State == nil || raw.Container.HostConfig == nil {
		t.Fatal("exact original-CID inspection unavailable", err)
	}
	return raw.Container
}

func osdMemoryVolume(t *testing.T, ctx context.Context, docker *mobycl.Client, name, owner string) {
	t.Helper()
	result, err := docker.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{})
	if err != nil || result.Volume.Name != name || result.Volume.Driver != "local" || result.Volume.Labels[osdMemoryLabel] != owner || result.Volume.Options["type"] != "tmpfs" || result.Volume.Options["device"] != "tmpfs" || result.Volume.Options["o"] != fmt.Sprintf("size=%d,mode=0700", osdMemoryBytes) {
		t.Fatal("owned capped tmpfs volume inspection differs", err)
	}
}

func osdMemoryBacking(t *testing.T, ctx context.Context, docker *mobycl.Client, cluster *ceph.Container, osds []*ceph.OSDContainer, image string) (string, container.InspectResponse) {
	t.Helper()
	var name string
	for _, osd := range osds {
		raw := osdMemoryInspect(t, ctx, docker, osd.GetContainerID())
		found := 0
		for _, mounted := range raw.Mounts {
			if mounted.Destination != "/var/lib/ceph/osd" {
				continue
			}
			if mounted.Type != mount.TypeVolume || mounted.Name == "" || !mounted.RW || name != "" && name != mounted.Name {
				t.Fatal("OSDs do not share the same writable named volume")
			}
			name = mounted.Name
			found++
		}
		if found != 1 {
			t.Fatal("memory OSD mount is missing or duplicated")
		}
		osdMemoryExec(t, ctx, osd, "sh", "-c", `while IFS= read -r line; do case "$line" in *" /var/lib/ceph/osd "*" - tmpfs "*) exit 0;; esac; done < /proc/self/mountinfo; exit 1`)
	}
	owner := strings.TrimPrefix(name, "tc-ceph-osd-memory-")
	if id, err := uuid.Parse(owner); err != nil || id == uuid.Nil || name != "tc-ceph-osd-memory-"+id.String() {
		t.Fatal("generated volume ownership UUID unavailable")
	}
	osdMemoryVolume(t, ctx, docker, name, owner)
	listed, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", osdMemoryLabel+"="+owner)})
	if err != nil || len(listed.Items) != 1 {
		t.Fatal("exact owned keeper inventory differs", err)
	}
	keeper := osdMemoryInspect(t, ctx, docker, listed.Items[0].ID)
	control := osdMemoryInspect(t, ctx, docker, cluster.GetContainerID())
	if !keeper.State.Running || keeper.State.StartedAt == "" || keeper.Image != control.Image || !slices.Equal(keeper.Config.Entrypoint, []string{"sleep"}) || !slices.Equal(keeper.Config.Cmd, []string{"infinity"}) || string(keeper.HostConfig.NetworkMode) != "none" || keeper.Config.Labels[osdMemoryLabel] != owner {
		t.Fatal("sleep-only control-image keeper identity differs")
	}
	validMount := 0
	for _, mounted := range keeper.Mounts {
		if mounted.Destination == "/tc/osd-memory" && mounted.Type == mount.TypeVolume && mounted.Name == name && mounted.RW {
			validMount++
		}
	}
	if validMount != 1 {
		t.Fatal("keeper does not retain the exact OSD tmpfs volume")
	}
	t.Logf("OSD_MEMORY_BACKING image=%s volume=%s max_bytes=%d keeper_cid=%s osds=%d driver=local type=tmpfs", image, name, osdMemoryBytes, keeper.ID, len(osds))
	return name, keeper
}

func osdMemoryIdentities(t *testing.T, ctx context.Context, cluster *ceph.Container) map[int]string {
	t.Helper()
	result := map[int]string{}
	for _, item := range noInitialOSDNativeDump(t, ctx, cluster) {
		if _, exists := result[item.ID]; exists {
			t.Fatal("duplicate native OSD ID")
		}
		result[item.ID] = item.UUID
	}
	return result
}

func osdMemoryDirectory(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, fsid, osdUUID string) {
	t.Helper()
	data := osdMemoryExec(t, ctx, osd, "sh", "-c", `set -eu; dir=$1; test -f "$dir/ready"; cat "$dir/fsid" "$dir/ceph_fsid"`, "sh", "/var/lib/ceph/osd/ceph-"+strconv.Itoa(osd.ID))
	if fields := strings.Fields(string(data)); !slices.Equal(fields, []string{osdUUID, fsid}) {
		t.Fatal("ready memory directory differs from native OSD/cluster identity")
	}
}

func osdMemoryWaitState(t *testing.T, parent context.Context, cluster *ceph.Container, up bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	for {
		if up {
			for _, osd := range cluster.OSDs() {
				state, err := osd.State(ctx)
				if err != nil || state == nil || !state.Running {
					t.Fatalf("restarted OSD exited before becoming up: cid=%s state=%+v error=%v", osd.GetContainerID(), state, err)
				}
			}
		}
		states, err := cluster.OSDStates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ready := len(states) == 2
		for _, state := range states {
			ready = ready && state.Up == up && state.In
		}
		if ready {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("same owned OSD membership did not converge", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func osdMemoryPlacement(t *testing.T, parent context.Context, cluster *ceph.Container, fsid string, poolID int64, pool string, wanted, forbidden int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	for {
		snapshot, err := cluster.PoolPGs(ctx, pool)
		if err != nil || snapshot.FSID != fsid || snapshot.PoolBefore.ID != poolID || snapshot.PoolAfter.ID != poolID {
			t.Fatal("original pool placement identity changed", err)
		}
		ready, resident := snapshot.PGReady && len(snapshot.PGs) == 8, false
		for _, pg := range snapshot.PGs {
			ready = ready && pg.State == "active+clean" && !slices.Contains(pg.Up, forbidden) && !slices.Contains(pg.Acting, forbidden)
			resident = resident || slices.Contains(pg.Acting, wanted) && !pg.StatsInvalid && pg.Stats.Objects > 0 && pg.Stats.Bytes > 0
		}
		if ready && resident {
			t.Logf("OSD_MEMORY_PLACEMENT fsid=%s pool_id=%d wanted_osd=%d forbidden_osd=%d pgs=8 populated_wanted=true", fsid, poolID, wanted, forbidden)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("clean populated OSD placement did not converge", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func osdMemoryExec(t *testing.T, parent context.Context, ctr testcontainers.Container, args ...string) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal("memory fixture native exec failed", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("memory fixture native command failed: exit=%d error=%v output=%s", code, err, data)
	}
	return data
}

func osdMemoryData(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid string, poolID int64, pool, phase string) {
	t.Helper()
	status, err := cluster.PoolStatus(ctx, pool)
	if err != nil || status.ID != poolID || status.Size != 2 || status.MinSize != 1 {
		t.Fatal("retained pool identity/policy changed", err)
	}
	control, err := cluster.ControlContainerContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := osdMemoryExec(t, ctx, control, "python3", "-c", `import subprocess,sys
subprocess.run([sys.executable,'-c',sys.argv[1],*sys.argv[2:]],timeout=150,check=True)`, osdMemoryDataProbe, fsid, pool, phase)
	var result struct {
		FSID, SHA256 string
		Bytes        int64
		Objects      int
	}
	hash := sha256.New()
	for index := range 32 {
		block := make([]byte, 4096)
		for offset := range block {
			block[offset] = byte((index + offset) % 251)
		}
		for range 1024 {
			_, _ = hash.Write(block)
		}
	}
	if err := json.Unmarshal(data, &result); err != nil || result.FSID != fsid || result.Bytes != osdMemoryPayloadBytes || result.Objects != 32 || result.SHA256 != fmt.Sprintf("%x", hash.Sum(nil)) {
		t.Fatal("independent full-reader deterministic memory payload differs", err)
	}
	t.Logf("OSD_MEMORY_BYTES phase=%s fsid=%s pool_id=%d bytes=%d objects=%d sha256=%s", phase, fsid, poolID, result.Bytes, result.Objects, result.SHA256)
}

const osdMemoryDataProbe = `import hashlib,json,rados,sys
fsid,pool,phase=sys.argv[1:]
digest=hashlib.sha256()
total=0
with rados.Rados(conffile='/etc/ceph/ceph.conf', name='client.admin', conf={'client_mount_timeout':'10','rados_mon_op_timeout':'10','rados_osd_op_timeout':'30'}) as cluster:
    assert cluster.get_fsid()==fsid
    with cluster.open_ioctx(pool) as io:
        for index in range(32):
            payload=bytes((index+offset)%251 for offset in range(4096))*1024
            name='memory-%02d'%index
            if phase=='seed':
                io.write_full(name,payload)
            size,_=io.stat(name)
            actual=io.read(name,len(payload))
            assert size==len(payload) and actual==payload
            digest.update(actual)
            total+=len(actual)
print(json.dumps({'FSID':fsid,'SHA256':digest.hexdigest(),'Bytes':total,'Objects':32}))
`

func osdMemoryPhase(t *testing.T, phase string, started time.Time) {
	t.Helper()
	t.Logf("OSD_MEMORY_PHASE phase=%s seconds=%.3f", phase, time.Since(started).Seconds())
}

// Phase snapshots use raw cgroup usage including cache. Their maximum is an
// observation of this fixture, not an absolute peak or a disk benchmark.
type osdMemoryStats struct {
	docker       *mobycl.Client
	peak, errors uint64
	samples      int
}

func (s *osdMemoryStats) sample(parent context.Context, ids map[string]bool) {
	var usage uint64
	valid := 0
	for cid := range ids {
		ctx, done := context.WithTimeout(parent, 2*time.Second)
		result, err := s.docker.ContainerStats(ctx, cid, mobycl.ContainerStatsOptions{})
		var sample container.StatsResponse
		if err == nil {
			err = json.NewDecoder(result.Body).Decode(&sample)
			_ = result.Body.Close()
		}
		done()
		if err == nil && sample.ID == cid {
			usage += sample.MemoryStats.Usage
			valid++
		} else if !errdefs.IsNotFound(err) {
			s.errors++
		}
	}
	if valid != 0 {
		s.samples++
		s.peak = max(s.peak, usage)
	}
}

func (s *osdMemoryStats) log(t *testing.T) {
	t.Helper()
	t.Logf("OSD_MEMORY_DOCKER_STATS snapshot_peak_sum_usage_bytes=%d phase_snapshots=%d stats_errors=%d scope=control,mgr,osds,keeper benchmark=false", s.peak, s.samples, s.errors)
}
