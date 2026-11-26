//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35 image-roles=essential,client,cephfs,rgw

package rbd_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

// TestRBDLifecycle verifies userspace RBD I/O via the image's CLI. It does not
// map a kernel block device or mount a filesystem on an RBD image.
func TestRBDLifecycle(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	runRBDLifecycle(t)
}

func TestRBDRunPoolSelection(t *testing.T) {
	for _, tc := range []struct {
		name, pool string
		option     ceph.Option
	}{
		{name: "default", pool: "rbd"},
		{name: "explicit empty", option: rbd.WithPools()},
		{name: "explicit named", pool: "volumes", option: rbd.WithPools(ceph.PoolConfig{Name: "volumes"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
			defer cancel()
			image, opts := harness.IntegrationImages(t)
			opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))
			if tc.option != nil {
				opts = append(opts, tc.option)
			}
			cluster, err := rbd.Run(ctx, image, opts...)
			if cluster != nil {
				testcontainers.CleanupContainer(t, cluster)
			}
			if err != nil {
				t.Fatal(err)
			}
			pools, err := harness.ObserveCluster(cluster.Container).Pools(ctx)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"rbd", "volumes"} {
				found := slices.ContainsFunc(pools, func(p *ceph.Pool) bool { return p.Name == name })
				if found != (name == tc.pool) {
					t.Fatalf("pool %s: present=%t, selected pool=%q", name, found, tc.pool)
				}
			}
			if tc.pool != "" {
				if _, err := rbd.ListNamespaces(ctx, cluster.Container, tc.pool); err != nil {
					t.Fatalf("selected pool lacks initialized RBD metadata: %v", err)
				}
			}
		})
	}
}
