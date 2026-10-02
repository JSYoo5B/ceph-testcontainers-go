//go:build integration && topology

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSMultiActiveStandbyFailoverAndFilesystems(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithCephFS(
		ceph.CephFSConfig{Name: "alpha", ActiveMDS: 2, StandbyMDS: 1},
		ceph.CephFSConfig{Name: "beta"},
	))
	alpha, beta := cephFSOwnedFilesystem(t, cluster, "alpha"), cephFSOwnedFilesystem(t, cluster, "beta")
	alphaBefore := cephFSTopologyStatus(t, ctx, alpha)
	if len(alphaBefore.Active) != 2 || len(alphaBefore.Standby) != 1 || len(alpha.MDSs()) != 3 {
		t.Fatalf("multi-active topology was not built: %+v", alphaBefore)
	}
	betaBefore := cephFSTopologyStatus(t, ctx, beta)
	if alphaBefore.FilesystemID == betaBefore.FilesystemID || alpha.MetadataPool == beta.MetadataPool || alpha.DataPool == beta.DataPool {
		t.Fatal("named filesystems share an identity or backing pool")
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// Mount each named filesystem using a new userspace session. The same path
	// receives different bytes, proving filesystem selection and isolation.
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "seed", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "seed", 0, "")
	cephFSWaitForPinnedSubtree(t, ctx, cluster, alpha, "/pinned", 1)
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "verify", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "verify", 0, "")

	old := alphaBefore.Active[1]
	standby := alphaBefore.Standby[0]
	failed := cephFSOwnedMDS(t, alpha, old.Name)
	// SIGKILL models a daemon failure without flushing its MDS journal. Native
	// mds fail removes the old GID and triggers replacement by the standby.
	cephFSKillMDS(t, ctx, failed)
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForReplacement(t, ctx, alpha, old, standby)
	cephFSWaitForPinnedSubtree(t, ctx, cluster, alpha, "/pinned", 1)
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "after-failure", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "verify", 0, "")
	betaAfter := cephFSTopologyStatus(t, ctx, beta)
	if len(betaAfter.Active) != 1 || betaAfter.Active[0].GID != betaBefore.Active[0].GID {
		t.Fatal("alpha failure changed beta's active MDS")
	}
	if err := failed.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := alpha.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSTopologyIO(t, ctx, client, alpha.FilesystemName, "alpha-data", "final", 1, "")
	cephFSTopologyIO(t, ctx, client, beta.FilesystemName, "beta-data", "verify", 0, "")
	t.Logf("two isolated filesystems; alpha active ranks 0/1 plus standby; rank1 owns /pinned; killed mds.%s gid=%d replaced by mds.%s; fresh sessions preserved bytes and new writes; restarted daemon restored standby capacity; beta MDS unchanged", old.Name, old.GID, standby.Name)
}

func TestCephFSStandbyReplayFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t)
	fs, err := cluster.StartCephFSWithConfig(ctx, ceph.CephFSConfig{Name: "hot", StandbyMDS: 1, StandbyReplay: true})
	if err != nil {
		t.Fatal(err)
	}
	before := cephFSTopologyStatus(t, ctx, fs)
	if len(before.Active) != 1 || len(before.StandbyReplay) != 1 || before.StandbyReplay[0].Rank != 0 {
		t.Fatalf("journal-following standby not present: %+v", before)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "hot-data", "seed", 0, "")
	old, replay := before.Active[0], before.StandbyReplay[0]
	failed := cephFSOwnedMDS(t, fs, old.Name)
	cephFSKillMDS(t, ctx, failed)
	if _, err := cluster.Ceph(ctx, "mds", "fail", strconv.FormatUint(old.GID, 10)); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForReplacement(t, ctx, fs, old, replay)
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "hot-data", "after-failure", 0, "")
	if err := failed.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fs.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	after := cephFSTopologyStatus(t, ctx, fs)
	if len(after.StandbyReplay) != 1 || after.StandbyReplay[0].Name != old.Name || after.StandbyReplay[0].GID == old.GID {
		t.Fatalf("restarted daemon did not rejoin as a fresh journal follower: %+v", after)
	}
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "hot-data", "final", 0, "")
	t.Logf("standby-replay mds.%s took over rank0 after mds.%s gid=%d died; retained file and new write/read verified in fresh sessions; old daemon returned with fresh GID=%d as replay standby", replay.Name, old.Name, old.GID, after.StandbyReplay[0].GID)
}

func TestCephFSAdditionalErasureCodedDataPool(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(3))
	const ecPool = "ecfs-file-data"
	fs, err := cluster.StartCephFSWithConfig(ctx, ceph.CephFSConfig{Name: "ecfs", AdditionalDataPools: []ceph.PoolConfig{{
		Name: ecPool, PGNum: 8, MinSize: 3, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "ec-data", "seed", 0, ecPool)
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "ec-data", "after-failure", 0, ecPool)
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "ec-data", "final", 0, ecPool)
	// Layout selection alone is insufficient evidence: the selected pool must
	// contain actual CephFS objects and retain its native erasure configuration.
	output, err := cluster.Ceph(ctx, "osd", "pool", "get", ecPool, "all", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Size              int  `json:"size"`
		AllowECOverwrites bool `json:"allow_ec_overwrites"`
		MinSize           int  `json:"min_size"`
	}
	if err := json.Unmarshal(output, &settings); err != nil || settings.Size != 3 || !settings.AllowECOverwrites || settings.MinSize != 3 {
		t.Fatalf("CephFS data pool is not EC k2+m1 with overwrites: output=%s error=%v", output, err)
	}
	output, err = cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", "tc-"+ecPool+"-ec", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var profile map[string]string
	if err := json.Unmarshal(output, &profile); err != nil || profile["k"] != "2" || profile["m"] != "1" || profile["plugin"] != "jerasure" {
		t.Fatalf("CephFS file data uses an unexpected EC profile: output=%s error=%v", output, err)
	}
	code, reader, err := client.Exec(ctx, []string{"rados", "-p", ecPool, "ls"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	objects, err := io.ReadAll(reader)
	if err != nil || code != 0 || strings.TrimSpace(string(objects)) == "" {
		t.Fatalf("EC pool contains no CephFS file data: exit=%d objects=%s error=%v", code, objects, err)
	}
	t.Logf("replicated CephFS metadata/default data plus additional EC2+1 data pool; file layout=%s, actual objects=%s; partial overwrite/fsync and fresh-session byte proof", ecPool, strings.TrimSpace(string(objects)))
}

func cephFSTopologyStatus(t *testing.T, ctx context.Context, fs *ceph.CephFSContainer) *ceph.CephFSMDSStatus {
	t.Helper()
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func cephFSOwnedMDS(t *testing.T, fs *ceph.CephFSContainer, name string) *ceph.MDSContainer {
	t.Helper()
	for _, daemon := range fs.MDSs() {
		if daemon.ID == name {
			return daemon
		}
	}
	t.Fatalf("filesystem %s does not own mds.%s", fs.FilesystemName, name)
	return nil
}

func cephFSOwnedFilesystem(t *testing.T, cluster *ceph.Container, name string) *ceph.CephFSContainer {
	t.Helper()
	for _, fs := range cluster.Filesystems() {
		if fs.FilesystemName == name {
			return fs
		}
	}
	t.Fatalf("cluster initial composition does not own filesystem %s", name)
	return nil
}

func cephFSKillMDS(t *testing.T, parent context.Context, daemon *ceph.MDSContainer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer docker.Close()
	if _, err := docker.ContainerKill(ctx, daemon.GetContainerID(), mobycl.ContainerKillOptions{Signal: "KILL"}); err != nil {
		t.Fatal(err)
	}
	for {
		state, err := daemon.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if !state.Running {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("killed mds.%s still running: %v", daemon.ID, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func cephFSWaitForReplacement(t *testing.T, parent context.Context, fs *ceph.CephFSContainer, old, standby ceph.MDSStatus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last *ceph.CephFSMDSStatus
	var lastErr error
	for {
		last, lastErr = fs.MDSStatus(ctx)
		if lastErr == nil && len(last.Active) == last.MaxMDS {
			for _, current := range last.Active {
				if current.Rank == old.Rank && current.Name == standby.Name && current.GID == standby.GID && current.Owned && current.GID != old.GID {
					t.Logf("native MDS failover: rank=%d old=%s/%d new=%s/%d", old.Rank, old.Name, old.GID, current.Name, current.GID)
					return
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("owned standby never replaced failed rank: error=%v status=%+v", lastErr, last)
		case <-time.After(time.Second):
		}
	}
}

func cephFSWaitForPinnedSubtree(t *testing.T, parent context.Context, cluster *ceph.Container, fs *ceph.CephFSContainer, path string, rank int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var last string
	for {
		status, err := fs.MDSStatus(ctx)
		if err == nil {
			for _, active := range status.Active {
				if active.Rank != rank || !active.Owned {
					continue
				}
				data, err := cluster.Ceph(ctx, "tell", "mds."+active.Name, "get", "subtrees", "--format", "json")
				last = fmt.Sprintf("error=%v output=%s", err, data)
				if err != nil {
					continue
				}
				type subtree struct {
					IsAuth    bool `json:"is_auth"`
					AuthFirst int  `json:"auth_first"`
					ExportPin int  `json:"export_pin"`
					Dir       struct {
						Path string `json:"path"`
					} `json:"dir"`
				}
				var subtrees []subtree
				if err := json.Unmarshal(data, &subtrees); err != nil {
					var wrapped struct {
						Subtrees []subtree `json:"subtrees"`
					}
					if err := json.Unmarshal(data, &wrapped); err != nil {
						continue
					}
					subtrees = wrapped.Subtrees
				}
				for _, subtree := range subtrees {
					if subtree.Dir.Path == path && subtree.IsAuth && subtree.AuthFirst == rank && subtree.ExportPin == rank {
						t.Logf("native MDS subtree: %s is owned by rank%d mds.%s; export pin=%d", path, rank, active.Name, subtree.ExportPin)
						return
					}
				}
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			t.Fatalf("export pin never moved actual subtree authority to rank%d: %s", rank, last)
		case <-time.After(time.Second):
		}
	}
}

func cephFSTopologyIO(t *testing.T, parent context.Context, client testcontainers.Container, filesystem, token, phase string, pin int, pool string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	code, reader, err := client.Exec(ctx, []string{"python3", "-c", cephFSTopologyIOScript, filesystem, token, phase, strconv.Itoa(pin), pool}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		t.Fatalf("fresh libcephfs %s session on %s: exit=%d error=%v output=%s", phase, filesystem, code, err, output)
	}
	t.Logf("native CephFS session: %s", strings.TrimSpace(string(output)))
}

const cephFSTopologyIOScript = `import cephfs, hashlib, json, os, sys, threading
filesystem, token, phase, pin, pool = sys.argv[1:]
deadline = threading.Timer(45, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.conf_set('rados_osd_op_timeout', '15')
fs.mount(filesystem_name=filesystem.encode())
seed = token.encode() * 32768 + bytes(range(256))
new = b'after-mds-failure:' + token.encode() * 16384

def stored(data):
    return data[:123] + bytes(value ^ 0x5a for value in data[123:4219]) + data[4219:]

def read(path, expected):
    fd = fs.open(path, os.O_RDONLY)
    try:
        actual = fs.read(fd, 0, len(expected) + 1)
        assert actual == expected, 'retained bytes changed: ' + path
    finally:
        fs.close(fd)

def write(path, data):
    fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        assert fs.write(fd, data, 0) == len(data), 'short write'
        # Overwrite a range inside an existing object; EC data pools need
        # allow_ec_overwrites for this genuine CephFS userspace I/O.
        patch = stored(data)[123:4219]
        assert fs.write(fd, patch, 123) == len(patch), 'short ranged overwrite'
        fs.fsync(fd, False)
    finally:
        fs.close(fd)

try:
    if phase == 'seed':
        fs.mkdir('/pinned', 0o700)
        fs.setxattr('/pinned', 'ceph.dir.pin', pin.encode(), 0)
        if pool:
            fs.setxattr('/pinned', 'ceph.dir.layout.pool', pool.encode(), 0)
        write('/pinned/payload', seed)
    read('/pinned/payload', stored(seed))
    assert fs.getxattr('/pinned', 'ceph.dir.pin').decode() == pin
    if phase == 'after-failure':
        write('/pinned/new', new)
        read('/pinned/new', stored(new))
    elif phase == 'final':
        read('/pinned/new', stored(new))
    elif phase not in ('seed', 'verify'):
        raise ValueError('unknown fixture phase')
    layout_pool = fs.getxattr('/pinned/payload', 'ceph.file.layout.pool_name').decode()
    if pool:
        assert layout_pool == pool, 'CephFS file data used another pool'
    fs.sync_fs()
    print(json.dumps({'filesystem': filesystem, 'phase': phase, 'bytes': len(seed), 'sha256': hashlib.sha256(stored(seed)).hexdigest(), 'pin': int(pin), 'pool': layout_pool}))
finally:
    fs.shutdown()
    deadline.cancel()
`
