//go:build integration && features

package integration_test

import (
	"context"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestNativePoolReplacement(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, _ := newServiceCluster(t, opts...)
			const pool = "tc-native-replacement"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rbd", PGNum: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.InitRBDPool(ctx, pool); err != nil {
				t.Fatal(err)
			}
			original, err := cluster.PoolStatus(ctx, pool)
			if err != nil || original.ID <= 0 {
				t.Fatal("original native pool ID unavailable", err)
			}
			stale, err := cluster.CreateRBDNamespace(ctx, pool, "owned")
			if err != nil {
				t.Fatal(err)
			}
			permission, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "mon", Name: "mon_allow_pool_delete", Value: "true"})
			if permission != nil {
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					if err := permission.Restore(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			// Only this test-created empty pool is replaced, deliberately outside
			// the public handle. A recreated same-name namespace must survive.
			cephCommand(t, ctx, cluster, "osd", "pool", "delete", pool, pool, "--yes-i-really-really-mean-it")
			cephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "1")
			cephCommand(t, ctx, cluster, "osd", "pool", "application", "enable", pool, "rbd")
			if err := cluster.InitRBDPool(ctx, pool); err != nil {
				t.Fatal(err)
			}
			replacement, err := cluster.PoolStatus(ctx, pool)
			if err != nil || replacement.ID <= 0 || replacement.ID == original.ID {
				t.Fatal("native replacement was not distinguished", err)
			}
			fresh, err := cluster.CreateRBDNamespace(ctx, pool, "owned")
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.RemoveRBDNamespace(ctx, stale); err == nil {
				t.Fatal("stale pool incarnation removed replacement namespace")
			}
			names, err := cluster.ListRBDNamespaces(ctx, pool)
			if err != nil || !slices.Equal(names, []string{"owned"}) {
				t.Fatal("replacement namespace was changed", err)
			}
			if err := cluster.RemoveRBDNamespace(ctx, fresh); err != nil {
				t.Fatal(err)
			}
			if err := permission.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("actual pool ID %d → %d; stale handle refused, fresh namespace preserved and removed with its own handle", original.ID, replacement.ID)
		})
	}
}
