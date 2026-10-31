//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

package integration_test

import (
	"context"
	"slices"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
)

func (s *radosPoolSuite) TestNativePoolReplacement() {
	t, cluster := s.T(), s.cluster
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	const pool = "tc-native-replacement"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rbd", PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	if err := rbd.InitPool(ctx, cluster, pool); err != nil {
		t.Fatal(err)
	}
	original, err := harness.PoolHandle(t, ctx, cluster, pool).Status(ctx)
	if err != nil || original.ID <= 0 {
		t.Fatal("original native pool ID unavailable", err)
	}
	stale, err := rbd.CreateNamespace(ctx, cluster, pool, "owned")
	if err != nil {
		t.Fatal(err)
	}
	permission, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete", Value: "true"})
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := permission.Restore(cleanup); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	// Only this test-created empty pool is replaced, deliberately outside
	// the public handle. A recreated same-name namespace must survive.
	harness.CephCommand(t, ctx, cluster, "osd", "pool", "delete", pool, pool, "--yes-i-really-really-mean-it")
	harness.CephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "1")
	harness.CephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", pool, "rbd")
	if err := rbd.InitPool(ctx, cluster, pool); err != nil {
		t.Fatal(err)
	}
	replacement, err := harness.PoolHandle(t, ctx, cluster, pool).Status(ctx)
	if err != nil || replacement.ID <= 0 || replacement.ID == original.ID {
		t.Fatal("native replacement was not distinguished", err)
	}
	fresh, err := rbd.CreateNamespace(ctx, cluster, pool, "owned")
	if err != nil {
		t.Fatal(err)
	}
	if err := rbd.RemoveNamespace(ctx, cluster, stale); err == nil {
		t.Fatal("stale pool incarnation removed replacement namespace")
	}
	names, err := rbd.ListNamespaces(ctx, cluster, pool)
	if err != nil || !slices.Equal(names, []string{"owned"}) {
		t.Fatal("replacement namespace was changed", err)
	}
	if err := rbd.RemoveNamespace(ctx, cluster, fresh); err != nil {
		t.Fatal(err)
	}
	if err := permission.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("actual pool ID %d → %d; stale handle refused, fresh namespace preserved and removed with its own handle", original.ID, replacement.ID)
}
