//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_rbd_fixtures_basic))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client

package rbd_test

import (
	"context"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

func TestRBDAbsenceObservations(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
	const pool = "tc-absence"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 8, Replicas: 1, MinSize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(ctx, cluster, pool); err != nil {
		t.Fatal(err)
	}
	read := func(args ...string) []byte {
		t.Helper()
		out := harness.RBDOutput(t, ctx, client, args...)
		t.Logf("native %v: %s", args, out)
		return out
	}
	for _, enabled := range []bool{false, true} {
		if enabled {
			harness.RBDOutput(t, ctx, client, "mirror", "pool", "enable", pool, "image")
		}
		if err := harness.RBDRequireNoPeers(read("mirror", "pool", "info", pool, "--format", "json")); err != nil {
			t.Fatal(err)
		}
	}
	for _, image := range []struct{ name, features string }{{"plain", "layering"}, {"journal", "layering,exclusive-lock,journaling"}} {
		spec := pool + "/" + image.name
		harness.RBDOutput(t, ctx, client, "create", spec, "--size", "1M", "--image-feature", image.features)
		info := read("info", spec, "--format", "json")
		if err := harness.RBDRequireUnmirrored(info, spec); err != nil {
			t.Fatal(err)
		}
		if err := harness.RBDRequireUnmirrored(info, pool+"/wrong"); err == nil {
			t.Fatal("accepted another image's identity")
		}
		if err := harness.RBDRequireSnapshotAbsent(read("snap", "ls", spec, "--format", "json"), "missing"); err != nil {
			t.Fatal(err)
		}
		harness.RBDOutput(t, ctx, client, "snap", "create", spec+"@present")
		snaps := read("snap", "ls", spec, "--format", "json")
		if err := harness.RBDRequireSnapshotAbsent(snaps, "missing"); err != nil {
			t.Fatal(err)
		}
		if err := harness.RBDRequireSnapshotAbsent(snaps, "present"); err == nil {
			t.Fatal("accepted the existing snapshot as absent")
		}
	}
	spec := pool + "/journal"
	harness.RBDOutput(t, ctx, client, "mirror", "image", "enable", spec, "journal")
	if err := harness.RBDRequireUnmirrored(read("info", spec, "--format", "json"), spec); err == nil {
		t.Fatal("accepted an enrolled image as unmirrored")
	}
}
