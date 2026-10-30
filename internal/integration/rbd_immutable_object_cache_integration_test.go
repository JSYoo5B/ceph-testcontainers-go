//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_immutable_object_cache))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client,rbd-aux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

// TestImmutableObjectCache reads a clone through the RBD immutable object
// cache in a client container with an identity limited to the pool: the
// first read promotes every parent object, a second read makes no RADOS
// read, and after Stop the client reads the same data from RADOS.
func TestImmutableObjectCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	cluster, _ := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "tc-ioc"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(ctx, cluster, pool); err != nil {
		t.Fatal(err)
	}
	const id = "ioc-client"
	identity, err := cluster.CreateClient(ctx, id, ceph.ClientCaps{Mon: "profile rbd", OSD: "profile rbd pool=" + pool})
	if err != nil {
		t.Fatal(err)
	}
	client, err := testcontainers.Run(ctx, harness.StaticFacts(cluster).Images.RBDAux, identity.WithIdentity(), ceph.WithIdleEntrypoint())
	if client != nil {
		testcontainers.CleanupContainer(t, client)
	}
	if err != nil {
		t.Fatal(err)
	}
	rbdCLI := func(args ...string) string {
		return harness.ExecOutput(t, ctx, client, append([]string{"rbd", "--id", id}, args...)...)
	}
	// 64 MiB in 4 MiB objects: 16 parent objects.
	rbdCLI("create", "--size", "64", "--image-feature", "layering", pool+"/parent")
	rbdCLI("bench", "--io-type", "write", "--io-size", "4M", "--io-total", "64M", "--io-pattern", "seq", pool+"/parent")
	rbdCLI("snap", "create", pool+"/parent@base")
	rbdCLI("snap", "protect", pool+"/parent@base")
	rbdCLI("clone", pool+"/parent@base", pool+"/child")
	checksum := func(image string, options map[string]string) string {
		args := []string{"sh", "-c", `rbd "$@" export --no-progress "$0" - | md5sum`, pool + "/" + image, "--id", id}
		for name, value := range options {
			args = append(args, "--"+name+"="+value)
		}
		return strings.Fields(harness.ExecOutput(t, ctx, client, args...))[0]
	}
	want := checksum("parent@base", nil)

	lease, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{ID: id})
	if lease != nil {
		defer func() { _ = lease.Restore(context.WithoutCancel(ctx)) }()
	}
	if err != nil {
		t.Fatal(err)
	}
	if second, err := rbd.StartImmutableObjectCache(ctx, client, rbd.ImmutableObjectCache{ID: id, Path: "/var/cache/other"}); !errors.Is(err, ceph.ErrInvalidRequest) || second != nil {
		t.Fatalf("a second daemon on the same socket: %v", err)
	}
	if status, err := lease.Status(ctx); err != nil || !status.Running || status.CachedObjects != 0 || status.RADOSReads != 0 {
		t.Fatalf("status before a read: %+v, %v", status, err)
	}
	if got := checksum("child", lease.ClientOptions()); got != want {
		t.Fatalf("the clone read through the cache has checksum %s, the parent %s", got, want)
	}
	// The daemon reads each parent object from RADOS once and writes it into
	// its cache directory after the client's read returned, so the last
	// files can appear a moment later. More than 16 RADOS reads fails at
	// once.
	cached, err := harness.Observe(ctx, 30*time.Second, 10*time.Second, 200*time.Millisecond, func(probeCtx context.Context) (rbd.ImmutableObjectCacheStatus, bool, error) {
		status, err := lease.Status(probeCtx)
		switch {
		case err != nil:
			return status, false, err
		case !status.Running || status.RADOSReads > 16 || status.CachedObjects > 16:
			return status, false, fmt.Errorf("unexpected cache state %+v", status)
		}
		return status, status.CachedObjects == 16 && status.CachedBytes == 64<<20 && status.RADOSReads == 16, nil
	})
	first := cached.Value
	if err != nil {
		t.Fatalf("status after the first read: %+v, %v", first, err)
	}
	if got := checksum("child", lease.ClientOptions()); got != want {
		t.Fatalf("the second read has checksum %s", got)
	}
	if again, err := lease.Status(ctx); err != nil || again.RADOSReads != first.RADOSReads || again.CachedObjects != 16 {
		t.Fatalf("status after the second read: %+v, %v", again, err)
	}

	if err := lease.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	stopped, err := lease.Status(ctx)
	if err != nil || stopped.Running || stopped.CachedObjects != 16 {
		t.Fatalf("status after Stop: %+v, %v", stopped, err)
	}
	if got := checksum("child", lease.ClientOptions()); got != want {
		t.Fatalf("the read without the daemon has checksum %s", got)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "sh", "-c", `test ! -e "$0" && test ! -e "$1" && test -z "$(ls /var/run/ceph | grep immutable-object-cache)"`,
		lease.Config.Path, lease.Config.Socket)
	t.Logf("pid %d cached %d objects (%d bytes) with %d RADOS reads, %d status reads after the first client read; a second read added none; after Stop the client read the same data from RADOS; Restore removed the socket and cache",
		first.PID, first.CachedObjects, first.CachedBytes, first.RADOSReads, cached.Attempts)
}
