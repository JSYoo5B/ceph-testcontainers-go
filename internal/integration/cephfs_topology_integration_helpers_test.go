//go:build all || (integration && topology)

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
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func cephFSTopologyStatus(t *testing.T, ctx context.Context, fs *cephfs.Filesystem) *cephfs.FilesystemStatus {
	t.Helper()
	status, err := fs.MDSStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func cephFSOwnedMDS(t *testing.T, fs *cephfs.Filesystem, name string) *cephfs.MDS {
	t.Helper()
	for _, daemon := range fs.MDSs() {
		if daemon.ID == name {
			return daemon
		}
	}
	t.Fatalf("filesystem %s does not own mds.%s", fs.FilesystemName, name)
	return nil
}

func cephFSOwnedFilesystem(t *testing.T, cluster *ceph.Container, name string) *cephfs.Filesystem {
	t.Helper()
	for _, fs := range cephfs.Filesystems(cluster) {
		if fs.FilesystemName == name {
			return fs
		}
	}
	t.Fatalf("cluster initial composition does not own filesystem %s", name)
	return nil
}

func cephFSKillMDS(t *testing.T, parent context.Context, daemon *cephfs.MDS) {
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

func cephFSWaitForReplacement(t *testing.T, parent context.Context, fs *cephfs.Filesystem, old, standby cephfs.MDSStatus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	var last *cephfs.FilesystemStatus
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

func cephFSWaitForPinnedSubtree(t *testing.T, parent context.Context, cluster *ceph.Container, fs *cephfs.Filesystem, path string, rank int) {
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
