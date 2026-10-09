//go:build all || (integration && topology)

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testCephFSMDSScaleTopology(t *testing.T, replay bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	config := ceph.CephFSConfig{Name: "scale"}
	if replay {
		config.StandbyMDS, config.StandbyReplay = 1, true
	}
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(1), ceph.WithCephFS(
		config, ceph.CephFSConfig{Name: "other"},
	))
	fs, other := cephFSOwnedFilesystem(t, cluster, "scale"), cephFSOwnedFilesystem(t, cluster, "other")
	initial, otherBefore := cephFSTopologyStatus(t, ctx, fs), cephFSTopologyStatus(t, ctx, other)
	cephFSMDSScaleCounts(t, ctx, fs, 1, config.StandbyMDS)
	if replay && len(initial.StandbyReplay) != 1 {
		t.Fatalf("initial replay follower did not join: %+v", initial)
	}
	identity := cephFSMDSScaleStorageIdentity(t, ctx, cluster, fs.FilesystemName)
	ioFixture := func(filesystem, token string, before, next int) {
		t.Helper()
		code, reader, err := client.Exec(ctx, []string{"python3", "-c", cephFSMDSScaleIOScript, filesystem, token, fmt.Sprint(before), fmt.Sprint(next)}, tcexec.Multiplexed())
		if err != nil {
			t.Fatal(err)
		}
		output, err := io.ReadAll(reader)
		if err != nil || code != 0 {
			t.Fatalf("fresh native CephFS scale session: exit=%d error=%v output=%s", code, err, output)
		}
		t.Logf("MDS scale native connection proof: %s", output)
	}
	ioFixture(fs.FilesystemName, "scale-data", -1, 0)
	ioFixture(other.FilesystemName, "other-data", -1, 0)
	for stage, counts := range [][2]int{{2, 1}, {1, 1}, {1, 0}} {
		before := fs.MDSs()
		if err := fs.ScaleMDS(ctx, counts[0], counts[1]); err != nil {
			t.Fatal(err)
		}
		cephFSMDSScaleCounts(t, ctx, fs, counts[0], counts[1])
		after := fs.MDSs()
		for _, previous := range before {
			if !slices.ContainsFunc(after, func(current *ceph.MDSContainer) bool { return current.GetContainerID() == previous.GetContainerID() }) {
				if _, err := previous.State(ctx); !errdefs.IsNotFound(err) {
					t.Fatalf("retired mds.%s container still exists or inspection is uncertain: %v", previous.ID, err)
				}
			}
		}
		if status := cephFSTopologyStatus(t, ctx, fs); status.FilesystemID != initial.FilesystemID {
			t.Fatal("scaling recreated the filesystem instead of changing its MDS topology")
		}
		if got := cephFSMDSScaleStorageIdentity(t, ctx, cluster, fs.FilesystemName); got != identity {
			t.Fatalf("scaling recreated backing pools: got=%v initial=%v", got, identity)
		}
		if status := cephFSTopologyStatus(t, ctx, other); len(status.Active) != 1 || status.Active[0].GID != otherBefore.Active[0].GID || len(other.MDSs()) != 1 {
			t.Fatalf("scaling touched another filesystem's MDS: %+v", status)
		}
		if len(cluster.ServiceContainers()) != counts[0]+counts[1]+1 {
			t.Fatal("cluster optional-service ownership retained removed MDS containers")
		}
		ioFixture(fs.FilesystemName, "scale-data", stage, stage+1)
		ioFixture(other.FilesystemName, "other-data", 0, 0)
		t.Logf("existing FSID=%d scaled to %d active + %d standby; actual owned MDS containers=%d; other filesystem unchanged", initial.FilesystemID, counts[0], counts[1], len(after))
	}
}

func cephFSMDSScaleCounts(t *testing.T, ctx context.Context, fs *ceph.CephFSContainer, active, standby int) {
	t.Helper()
	status := cephFSTopologyStatus(t, ctx, fs)
	if status.MaxMDS != active || len(status.Active) != active || len(status.Standby)+len(status.StandbyReplay) != standby || len(fs.MDSs()) != active+standby {
		t.Fatalf("native and owned MDS counts differ from desired %d/%d: native=%+v owned=%d", active, standby, status, len(fs.MDSs()))
	}
	for _, daemon := range fs.MDSs() {
		state, err := daemon.State(ctx)
		if err != nil || state == nil || !state.Running {
			t.Fatalf("owned mds.%s is not actually running: state=%+v error=%v", daemon.ID, state, err)
		}
	}
}

func cephFSMDSScaleStorageIdentity(t *testing.T, ctx context.Context, cluster *ceph.Container, filesystem string) [2]int64 {
	t.Helper()
	data, err := cluster.Ceph(ctx, "fs", "get", filesystem, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var info struct {
		MDSMap struct {
			MetadataPool int64   `json:"metadata_pool"`
			DataPools    []int64 `json:"data_pools"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &info); err != nil || info.MDSMap.MetadataPool <= 0 || len(info.MDSMap.DataPools) != 1 || info.MDSMap.DataPools[0] <= 0 {
		t.Fatalf("invalid filesystem storage identity: error=%v output=%s", err, data)
	}
	return [2]int64{info.MDSMap.MetadataPool, info.MDSMap.DataPools[0]}
}

const cephFSMDSScaleIOScript = `import cephfs, hashlib, json, os, sys, threading
filesystem, token, before, after = sys.argv[1:]
before, after = int(before), int(after)
deadline = threading.Timer(35, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '20')
fs.conf_set('rados_osd_op_timeout', '15')
fs.mount(filesystem_name=filesystem.encode())
def payload(stage):
    return (token + ':stage=' + str(stage) + '\n').encode() * 2048
try:
    if before >= 0:
        fd = fs.open('/scale-fixture', os.O_RDONLY)
        try:
            expected = payload(before)
            assert fs.read(fd, 0, len(expected) + 1) == expected, 'retained file bytes changed during MDS scaling'
        finally:
            fs.close(fd)
    data = payload(after)
    fd = fs.open('/scale-fixture', os.O_CREAT | os.O_RDWR | os.O_TRUNC, 0o600)
    try:
        assert fs.write(fd, data, 0) == len(data), 'short native write'
        fs.fsync(fd, False)
        assert fs.read(fd, 0, len(data) + 1) == data, 'new native bytes differ'
    finally:
        fs.close(fd)
    print(json.dumps({'filesystem': filesystem, 'stage': after, 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}))
finally:
    fs.shutdown()
    deadline.cancel()
`
