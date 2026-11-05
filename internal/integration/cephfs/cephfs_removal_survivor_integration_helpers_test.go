//go:build all || (integration && features)

package cephfs_test

import (
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

func testCephFSRemovalSurvivor(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	cluster, admin := harness.NewServiceCluster(t,
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(1<<30).WithStartupBenchmark(ceph.OSDBenchmarkSkip)),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)),
		cephfs.WithFilesystems(cephfs.Config{Name: harness.CephFSIsolationFSA},
			cephfs.Config{Name: harness.CephFSIsolationFSB, StandbyMDS: 1, StandbyReplay: true}))
	var fsA, fsB *cephfs.Filesystem
	for _, fs := range cephfs.Filesystems(cluster) {
		switch fs.FilesystemName {
		case harness.CephFSIsolationFSA:
			fsA = fs
		case harness.CephFSIsolationFSB:
			fsB = fs
		}
	}
	if fsA == nil || fsB == nil {
		t.Fatal("the cluster does not own both filesystems")
	}
	harness.CephFSIsolationJSON(t, ctx, admin, harness.CephFSIsolationSeed, harness.CephFSIsolationFSA, harness.CephFSIsolationFSB)

	testIsolationSurvivor(t, ctx, cluster, admin, fsA, fsB)
}

// testIsolationSurvivor removes iso-a and checks iso-b right after Remove
// returns: its FSMap ID, one active and one standby-replay owned daemon, the
// native join_fscid of both, and its bytes. Some monitors clear the
// join_fscid of every remaining daemon when a filesystem is removed, until
// each daemon's next beacon; if the standby-replay daemon's beacon comes
// first, the monitor fails the active over to it for its better affinity. The
// FSMap epochs written by the removal show whether this monitor cleared the
// affinity. Without the clear, the active and standby-replay GIDs must be the
// ones before the removal; with it, the failover is native, and iso-b must
// come back ready with the same topology.
func testIsolationSurvivor(t *testing.T, ctx context.Context, cluster *ceph.Container, admin testcontainers.Container, fsA, fsB *cephfs.Filesystem) {
	if err := fsB.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := fsB.MDSStatus(ctx)
	if err != nil || len(before.Active) != 1 || len(before.StandbyReplay) != 1 {
		t.Fatalf("%s before removal: %+v error=%v", harness.CephFSIsolationFSB, before, err)
	}
	beforeMap := isolationFSMap(t, ctx, cluster, "")
	if err := fsA.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fsB.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	removedMap := isolationFSMap(t, ctx, cluster, "")
	if len(removedMap.Filesystems) != 1 || removedMap.Filesystems[0].MDSMap.Name != harness.CephFSIsolationFSB || removedMap.Filesystems[0].ID != before.FilesystemID {
		t.Fatalf("FSMap after removal: %+v", removedMap)
	}
	for _, info := range removedMap.Filesystems[0].MDSMap.Info {
		if info.JoinFSCID != before.FilesystemID {
			t.Fatalf("Remove returned while %s daemon %s has join_fscid %d, want %d", harness.CephFSIsolationFSB, info.Name, info.JoinFSCID, before.FilesystemID)
		}
	}
	cleared := uint64(0)
	for epoch := beforeMap.Epoch + 1; epoch <= removedMap.Epoch && cleared == 0; epoch++ {
		for _, fs := range isolationFSMap(t, ctx, cluster, strconv.FormatUint(epoch, 10)).Filesystems {
			for _, info := range fs.MDSMap.Info {
				if fs.MDSMap.Name == harness.CephFSIsolationFSB && info.JoinFSCID != before.FilesystemID {
					cleared = epoch
				}
			}
		}
	}
	if cleared == 0 {
		if after.FilesystemID != before.FilesystemID || len(after.Active) != 1 || len(after.StandbyReplay) != 1 ||
			after.Active[0].GID != before.Active[0].GID || after.StandbyReplay[0].GID != before.StandbyReplay[0].GID {
			t.Fatalf("%s changed when %s was removed: before=%+v after=%+v", harness.CephFSIsolationFSB, harness.CephFSIsolationFSA, before, after)
		}
	} else {
		t.Logf("the monitor cleared the affinity of %s's daemons in FSMap epoch %d; status right after Remove: %+v", harness.CephFSIsolationFSB, cleared, after)
		if err := fsB.WaitReady(ctx); err != nil {
			t.Fatal(err)
		}
		settled, err := fsB.MDSStatus(ctx)
		if err != nil || settled.FilesystemID != before.FilesystemID || len(settled.Active) != 1 || len(settled.StandbyReplay) != 1 ||
			!settled.Active[0].Owned || !settled.StandbyReplay[0].Owned {
			t.Fatalf("%s did not settle after the native failover: %+v error=%v", harness.CephFSIsolationFSB, settled, err)
		}
	}
	if result := harness.CephFSIsolationJSON(t, ctx, admin, isolationSurvivorRead, harness.CephFSIsolationFSB); result["read"] != "seed-of-"+harness.CephFSIsolationFSB {
		t.Fatalf("fresh read of %s: %v", harness.CephFSIsolationFSB, result)
	}
}

// isolationFSMap reads the current FSMap, or the one of epoch.
func isolationFSMap(t *testing.T, ctx context.Context, cluster *ceph.Container, epoch string) isolationMap {
	t.Helper()
	args := []string{"fs", "dump", "--format", "json"}
	if epoch != "" {
		args = []string{"fs", "dump", epoch, "--format", "json"}
	}
	data, err := cluster.Ceph(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	var fsmap isolationMap
	if err := json.Unmarshal(data, &fsmap); err != nil {
		t.Fatalf("decode FSMap %s: %v", data, err)
	}
	return fsmap
}

type isolationMap struct {
	Epoch       uint64 `json:"epoch"`
	Filesystems []struct {
		ID     int64 `json:"id"`
		MDSMap struct {
			Name string `json:"fs_name"`
			Info map[string]struct {
				Name      string `json:"name"`
				JoinFSCID int64  `json:"join_fscid"`
			} `json:"info"`
		} `json:"mdsmap"`
	} `json:"filesystems"`
}

const isolationSurvivorRead = harness.CephFSIsolationPreamble + `
fs = handle()
fs.mount(filesystem_name=sys.argv[1].encode())
out = {"read": read(fs, '/shared/seed')}
fs.unmount()
fs.shutdown()
print(json.dumps(out))
`
