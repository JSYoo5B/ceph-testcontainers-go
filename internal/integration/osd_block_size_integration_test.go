//go:build integration && topology

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
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
)

// Small logical devices use an explicit preparation profile that skips mClock's
// startup I/O benchmark. The scheduler is unchanged. Separate 512 MiB controls
// retain the native benchmark; each case owns a fresh serial bridge cluster.
func TestSmallOSDBlockSizeTopology(t *testing.T) {
	for _, mib := range []int64{64, 128, 256, 512} {
		for _, ram := range []bool{false, true} {
			name := "disk"
			if ram {
				name = "ram"
			}
			t.Run(fmt.Sprintf("%s-%dMiB-skip-benchmark", name, mib), func(t *testing.T) {
				testSmallOSDBlockSize(t, mib<<20, ram, true)
			})
		}
	}
	for _, ram := range []bool{false, true} {
		name := "disk"
		if ram {
			name = "ram"
		}
		t.Run(name+"-512MiB-native-benchmark", func(t *testing.T) {
			testSmallOSDBlockSize(t, 512<<20, ram, false)
		})
	}
}

func testSmallOSDBlockSize(t *testing.T, size int64, ram, skipBenchmark bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	started := time.Now()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	engine, err := docker.Client.Info(ctx, mobycl.InfoOptions{})
	if err != nil || engine.Info.ID == "" {
		t.Fatal("small-device engine identity unavailable", err)
	}
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(size))
	ramCap := int64(0)
	if ram {
		// The cluster allocation ceiling is independent of per-OSD logical bytes
		// and leaves room for all three files; it reserves no physical memory.
		ramCap = 4 * size
		opts = append(opts, ceph.WithOSDInMemoryStorage(ramCap))
	}
	cluster, err := ceph.Run(ctx, image, opts...)
	terminated := false
	owned := map[string]bool{}
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if t.Failed() {
				for _, osd := range cluster.OSDs() {
					if osd.Container == nil {
						continue
					}
					osdMemoryFailureLogs(t, cleanup, osd.Container)
				}
			}
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("partial small-device cleanup failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal("small-device control setup failed", err)
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fsid := q.MonMap.FSID
	if id, err := uuid.Parse(fsid); err != nil || id == uuid.Nil || id.String() != fsid {
		t.Fatal("original cluster identity unavailable")
	}
	if skipBenchmark {
		if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
			t.Fatal("explicit lightweight preparation failed", err)
		}
	}
	for range 2 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal("small-device OSD setup failed", err)
		}
	}
	initial := cluster.OSDs()
	if len(initial) != 2 {
		t.Fatal("two original OSDs unavailable")
	}
	owned[cluster.GetContainerID()] = true
	for _, mgr := range cluster.Managers() {
		owned[mgr.GetContainerID()] = true
	}
	identities := osdMemoryIdentities(t, ctx, cluster)
	initialCIDs := map[int]string{}
	volume, keeper := "", container.InspectResponse{}
	for _, osd := range initial {
		owned[osd.GetContainerID()] = true
		initialCIDs[osd.ID] = osd.GetContainerID()
		smallOSDLogicalSize(t, ctx, docker.Client, cluster, osd, size, fsid, identities[osd.ID], skipBenchmark)
	}
	if ram {
		volume, keeper = smallOSDRAMBacking(t, ctx, docker.Client, cluster, initial, ramCap)
		owned[keeper.ID] = true
	}
	nativeNetwork, err := docker.Client.NetworkInspect(ctx, cluster.NetworkName(), mobycl.NetworkInspectOptions{})
	if err != nil || nativeNetwork.Network.ID == "" {
		t.Fatal("owned bridge identity unavailable", err)
	}
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "small-device-retained", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8})
	if err != nil || pool == nil {
		t.Fatal("replicated small-device pool setup failed", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	poolState, err := cluster.PoolStatus(ctx, pool.Name)
	if err != nil || poolState.ID < 0 {
		t.Fatal("original pool identity unavailable", err)
	}
	smallOSDData(t, ctx, cluster, fsid, pool.Name, poolState.ID, "seed")
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
		if osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID()).State.Running {
			t.Fatal("original OSD remains running")
		}
	}
	if ram {
		stopped := osdMemoryInspect(t, ctx, docker.Client, keeper.ID)
		if !stopped.State.Running || stopped.State.StartedAt != keeper.State.StartedAt {
			t.Fatal("stopping both OSDs lost the original RAM keeper")
		}
	}
	for _, osd := range initial {
		if err := osd.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	osdMemoryWaitState(t, ctx, cluster, true)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("small-device restart did not recover", err)
	}
	if !maps.Equal(identities, osdMemoryIdentities(t, ctx, cluster)) {
		t.Fatal("same-device restart changed native UUIDs")
	}
	for _, osd := range initial {
		if initialCIDs[osd.ID] != osd.GetContainerID() {
			t.Fatal("same-device restart replaced a container")
		}
		smallOSDLogicalSize(t, ctx, docker.Client, cluster, osd, size, fsid, identities[osd.ID], skipBenchmark)
	}
	smallOSDData(t, ctx, cluster, fsid, pool.Name, poolState.ID, "same-osds-restarted")
	added, err := cluster.AddOSD(ctx)
	if err != nil || added == nil {
		t.Fatal("small-device third OSD failed", err)
	}
	owned[added.GetContainerID()] = true
	smallOSDLogicalSize(t, ctx, docker.Client, cluster, added, size, fsid, osdMemoryIdentities(t, ctx, cluster)[added.ID], skipBenchmark)
	if ram {
		freshVolume, freshKeeper := smallOSDRAMBacking(t, ctx, docker.Client, cluster, cluster.OSDs(), ramCap)
		if freshVolume != volume || freshKeeper.ID != keeper.ID || freshKeeper.State.StartedAt != keeper.State.StartedAt {
			t.Fatal("third OSD replaced the shared RAM storage")
		}
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("small-device backfill did not recover", err)
	}
	osdMemoryPlacement(t, ctx, cluster, fsid, poolState.ID, pool.Name, added.ID, -1)
	smallOSDData(t, ctx, cluster, fsid, pool.Name, poolState.ID, "third-osd-backfill")
	original := initial[0]
	if err := cluster.RemoveOSD(ctx, original.ID); err != nil {
		t.Fatal("small-device drain failed", err)
	}
	if _, err := docker.Client.ContainerInspect(ctx, original.GetContainerID(), mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("original small-device container remains", err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("small-device post-drain recovery failed", err)
	}
	osdMemoryPlacement(t, ctx, cluster, fsid, poolState.ID, pool.Name, added.ID, original.ID)
	current := osdMemoryIdentities(t, ctx, cluster)
	if len(current) != 2 || current[initial[1].ID] != identities[initial[1].ID] {
		t.Fatal("drain changed original survivor identity")
	}
	smallOSDData(t, ctx, cluster, fsid, pool.Name, poolState.ID, "original-drained")
	t.Logf("SMALL_OSD_FUNCTIONAL fsid=%s logical_block_bytes=%d ram=%t ram_cap_bytes=%d skip_benchmark=%t payload_bytes=%d replicas=2 restart_same_cids=true restart_same_uuids=true original_drained=true seconds=%.3f", fsid, size, ram, ramCap, skipBenchmark, 8<<20, time.Since(started).Seconds())
	cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
	defer done()
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal("whole small-device cleanup failed", err)
	}
	terminated = true
	cleanupVerified := true
	for cid := range owned {
		if _, err := docker.Client.ContainerInspect(cleanup, cid, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			cleanupVerified = false
			t.Error("owned small-device container remains", cid, err)
		}
	}
	if volume != "" {
		if _, err := docker.Client.VolumeInspect(cleanup, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
			cleanupVerified = false
			t.Error("owned small-device RAM volume remains", err)
		}
	}
	if _, err := docker.Client.NetworkInspect(cleanup, nativeNetwork.Network.ID, mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
		cleanupVerified = false
		t.Error("owned small-device bridge remains", err)
	}
	finalEngine, err := docker.Client.Info(cleanup, mobycl.InfoOptions{})
	if err != nil || finalEngine.Info.ID != engine.Info.ID {
		cleanupVerified = false
		t.Error("small-device cleanup engine changed", err)
	}
	if cleanupVerified {
		t.Logf("SMALL_OSD_CLEANUP engine=%s ram=%t owned_containers=%d volume=%s remaining=0", engine.Info.ID, ram, len(owned), volume)
	}
}

func smallOSDLogicalSize(t *testing.T, ctx context.Context, docker *mobycl.Client, cluster *ceph.Container, osd *ceph.OSDContainer, expected int64, fsid, osdUUID string, skipBenchmark bool) {
	t.Helper()
	path := "/var/lib/ceph/osd/ceph-" + strconv.Itoa(osd.ID) + "/block"
	result, err := docker.ContainerStatPath(ctx, osd.GetContainerID(), mobycl.ContainerStatPathOptions{Path: path})
	if err != nil || !result.Stat.Mode.IsRegular() || result.Stat.Size != expected {
		t.Fatalf("actual BlueStore sparse-file size differs: expected=%d stat=%+v error=%v", expected, result.Stat, err)
	}
	osdMemoryDirectory(t, ctx, osd, fsid, osdUUID)
	metadataJSON, err := cluster.Ceph(ctx, "osd", "metadata", strconv.Itoa(osd.ID), "--format=json")
	var metadata struct {
		ID         int    `json:"id"`
		DeviceSize string `json:"bluestore_bdev_size"`
		Store      string `json:"osd_objectstore"`
	}
	if err != nil || json.Unmarshal(metadataJSON, &metadata) != nil || metadata.ID != osd.ID || metadata.DeviceSize != strconv.FormatInt(expected, 10) || metadata.Store != "bluestore" {
		t.Fatalf("native OSD capacity differs: expected=%d metadata=%s error=%v", expected, metadataJSON, err)
	}
	for key, want := range map[string]string{"osd_mclock_skip_benchmark": strconv.FormatBool(skipBenchmark), "osd_op_queue": "mclock_scheduler"} {
		data, err := cluster.Ceph(ctx, "tell", "osd."+strconv.Itoa(osd.ID), "config", "get", key)
		var settings map[string]string
		if err != nil || json.Unmarshal(data, &settings) != nil || settings[key] != want {
			t.Fatalf("actual inherited small-device daemon option differs: option=%s expected=%s output=%s error=%v", key, want, data, err)
		}
	}
	t.Logf("SMALL_OSD_DEVICE osd_id=%d cid=%s osd_uuid=%s logical_block_bytes=%d queue=mclock_scheduler skip_benchmark=%t", osd.ID, osd.GetContainerID(), osdUUID, result.Stat.Size, skipBenchmark)
}

func smallOSDRAMBacking(t *testing.T, ctx context.Context, docker *mobycl.Client, cluster *ceph.Container, osds []*ceph.OSDContainer, capBytes int64) (string, container.InspectResponse) {
	t.Helper()
	name := ""
	for _, osd := range osds {
		raw := osdMemoryInspect(t, ctx, docker, osd.GetContainerID())
		found := 0
		for _, item := range raw.Mounts {
			if item.Destination == "/var/lib/ceph/osd" {
				if item.Type != mount.TypeVolume || !item.RW || item.Name == "" || name != "" && item.Name != name {
					t.Fatal("small OSDs do not share the same RAM volume")
				}
				name = item.Name
				found++
			}
		}
		if found != 1 {
			t.Fatal("small RAM OSD mount is unavailable")
		}
	}
	owner := strings.TrimPrefix(name, "tc-ceph-osd-memory-")
	volume, err := docker.VolumeInspect(ctx, name, mobycl.VolumeInspectOptions{})
	if err != nil || owner == name || volume.Volume.Driver != "local" || volume.Volume.Labels[osdMemoryLabel] != owner || volume.Volume.Options["type"] != "tmpfs" || volume.Volume.Options["device"] != "tmpfs" || volume.Volume.Options["o"] != fmt.Sprintf("size=%d,mode=0700", capBytes) {
		t.Fatal("small-device RAM allocation ceiling differs", err)
	}
	list, err := docker.ContainerList(ctx, mobycl.ContainerListOptions{All: true, Filters: make(mobycl.Filters).Add("label", osdMemoryLabel+"="+owner)})
	if err != nil || len(list.Items) != 1 {
		t.Fatal("small-device keeper inventory differs", err)
	}
	keeper := osdMemoryInspect(t, ctx, docker, list.Items[0].ID)
	control := osdMemoryInspect(t, ctx, docker, cluster.GetContainerID())
	if !keeper.State.Running || keeper.State.StartedAt == "" || keeper.Image != control.Image || string(keeper.HostConfig.NetworkMode) != "none" || !slices.Equal(keeper.Config.Entrypoint, []string{"sleep"}) || !slices.Equal(keeper.Config.Cmd, []string{"infinity"}) {
		t.Fatal("small-device original keeper differs")
	}
	matched := 0
	for _, item := range keeper.Mounts {
		if item.Destination == "/tc/osd-memory" && item.Name == name && item.Type == mount.TypeVolume && item.RW {
			matched++
		}
	}
	if matched != 1 {
		t.Fatal("small-device keeper does not retain the exact writable RAM volume")
	}
	return name, keeper
}

func smallOSDData(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, pool string, poolID int64, phase string) {
	t.Helper()
	state, err := cluster.PoolStatus(ctx, pool)
	if err != nil || state.ID != poolID || state.Size != 2 || state.MinSize != 1 {
		t.Fatal("small-device original pool changed", err)
	}
	control, err := cluster.ControlContainerContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data := osdMemoryExec(t, ctx, control, "python3", "-c", `import subprocess,sys
subprocess.run([sys.executable,'-c',sys.argv[1],*sys.argv[2:]],timeout=90,check=True)`, smallOSDBytesProbe, fsid, pool, phase)
	var result struct {
		FSID, SHA256 string
		Bytes        int64
		Objects      int
	}
	hash := sha256.New()
	for index := range 16 {
		block := make([]byte, 4096)
		for offset := range block {
			block[offset] = byte((index + offset) % 251)
		}
		for range 128 {
			_, _ = hash.Write(block)
		}
	}
	if err := json.Unmarshal(data, &result); err != nil || result.FSID != fsid || result.Objects != 16 || result.Bytes != 8<<20 || result.SHA256 != fmt.Sprintf("%x", hash.Sum(nil)) {
		t.Fatal("small-device independent full-reader payload differs", err)
	}
	t.Logf("SMALL_OSD_BYTES phase=%s fsid=%s pool_id=%d bytes=%d objects=%d sha256=%s", phase, fsid, poolID, result.Bytes, result.Objects, result.SHA256)
}

const smallOSDBytesProbe = `import hashlib,json,rados,sys
fsid,pool,phase=sys.argv[1:]
digest=hashlib.sha256()
total=0
with rados.Rados(conffile='/etc/ceph/ceph.conf',name='client.admin',conf={'client_mount_timeout':'10','rados_mon_op_timeout':'10','rados_osd_op_timeout':'20'}) as cluster:
    assert cluster.get_fsid()==fsid
    with cluster.open_ioctx(pool) as io:
        for index in range(16):
            payload=bytes((index+offset)%251 for offset in range(4096))*128
            name='small-%02d'%index
            if phase=='seed':
                io.write_full(name,payload)
            size,_=io.stat(name)
            actual=io.read(name,len(payload))
            assert size==len(payload) and actual==payload
            digest.update(actual)
            total+=len(actual)
print(json.dumps({'FSID':fsid,'SHA256':digest.hexdigest(),'Bytes':total,'Objects':16}))
`
