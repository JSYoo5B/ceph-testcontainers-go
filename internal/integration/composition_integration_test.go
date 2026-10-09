//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_topology))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestInitialClusterComposition(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithOSDCount(1),
		ceph.WithPools(ceph.PoolConfig{Name: "tc-initial", Application: "rados"}),
		ceph.WithCephFS(ceph.CephFSConfig{Name: "small"}))
	cluster, err := ceph.Run(ctx, image, opts...)
	if cluster != nil {
		testcontainers.CleanupContainer(t, cluster)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(cluster.OSDs()) != 1 || len(cluster.Filesystems()) != 1 {
		t.Fatal("Run did not build the requested initial topology")
	}
	fs := cluster.Filesystems()[0]
	if fs.FilesystemName != "small" || len(fs.MDSs()) != 1 {
		t.Fatal("Run did not expose the initialized filesystem")
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, image, cluster.WithClient(),
		testcontainers.WithEntrypoint("sleep"), testcontainers.WithCmd("infinity"))
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"seed", "verify"} {
		execCommand(t, ctx, client, "python3", "-c", `import cephfs, rados, sys
payload = bytes(range(256)) * 128
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout":"30", "rados_osd_op_timeout":"30"}) as cluster:
    with cluster.open_ioctx("tc-initial") as io:
        if sys.argv[1] == "seed":
            io.write_full("retained", payload)
        assert io.read("retained", len(payload)) == payload
fs = cephfs.LibCephFS(conffile="/etc/ceph/ceph.conf")
fs.conf_set("client_mount_timeout", "30")
fs.conf_set("client_fs", "small")
fs.mount()
try:
    if sys.argv[1] == "seed":
        fd = fs.open("/retained", "w", 0o600)
        assert fs.write(fd, payload, 0) == len(payload)
        fs.fsync(fd, False)
        fs.close(fd)
    fd = fs.open("/retained", "r")
    assert fs.read(fd, 0, len(payload)) == payload
    fs.close(fd)
finally:
    fs.shutdown()
print("initial one-OSD pool and filesystem native session passed")`, phase)
	}
	t.Log("Run composed one OSD, default single-copy pool, named filesystem and MDS; native RADOS/CephFS fresh sessions retained bytes")
}
