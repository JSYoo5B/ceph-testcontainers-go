//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
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
