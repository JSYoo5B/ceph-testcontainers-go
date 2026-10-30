//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

func TestMGRModules(t *testing.T) {
	harness.ParallelWhenEnabled(t)
	for _, mode := range harness.NetworkModes(t, "host", "bridge") {
		host, name := mode.Host, mode.Name
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := harness.NewServiceCluster(t, opts...)
			lookup := func(name string) ceph.MGRModuleState {
				t.Helper()
				deadline := time.Now().Add(35 * time.Second)
				for {
					modules, err := harness.ObserveCluster(cluster).MGRModules(ctx)
					if err == nil {
						for _, module := range modules {
							if module.Name == name {
								return module
							}
						}
						t.Fatalf("module %s is not advertised", name)
					}
					if time.Now().After(deadline) {
						t.Fatal("manager metadata did not converge", err)
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(500 * time.Millisecond):
					}
				}
			}
			apply := func(name string, enabled bool) *override.MGRModuleHandle {
				t.Helper()
				change, err := override.SetMGRModule(ctx, cluster, name, enabled)
				t.Cleanup(func() {
					cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
					defer cleanupCancel()
					if err := change.Restore(cleanupCtx); err != nil {
						t.Errorf("restore manager module: %v", err)
					}
				})
				if err != nil {
					t.Fatal(err)
				}
				return change
			}
			// Tentacle rbd_support and volumes are always-on. Their real command
			// readiness is independent of membership and is exercised below.
			rbdModule := lookup("rbd_support")
			if !rbdModule.AlwaysOn || !rbdModule.Enabled || !rbdModule.Available || !rbdModule.CanRun {
				t.Fatalf("native rbd_support prerequisites missing: %+v", rbdModule)
			}
			rbdLease := apply("rbd_support", true)
			if _, err := override.SetMGRModule(ctx, cluster, "rbd_support", false); err == nil {
				t.Fatal("always-on disable accepted")
			}
			if err := cluster.WaitMGRModuleReady(ctx, "rbd_support"); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitMGRModuleReady(ctx, "volumes"); err != nil {
				t.Fatal(err)
			}
			const pool = "tc-mgr-modules"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rbd"}); err != nil {
				t.Fatal(err)
			}
			if err := rbd.InitPool(ctx, cluster, pool); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			harness.ExecCommand(t, ctx, client, "python3", "-c", mgrModuleRBDProbe, pool)
			copy := *rbdLease
			if err := copy.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if err := rbdLease.Restore(ctx); err != nil || !lookup("rbd_support").Enabled {
				t.Fatal("original enabled rbd_support was not preserved", err)
			}
			// Mirroring is optional. The native MON FSMap policy protects the
			// module even when a separate multicluster fixture owns the daemon.
			if lookup("mirroring").Enabled {
				t.Fatal("fresh optional mirroring module must be disabled")
			}
			mirrorLease := apply("mirroring", true)
			if !lookup("mirroring").Enabled {
				t.Fatal("mirroring membership was not enabled")
			}
			if _, err := override.SetMGRModule(ctx, cluster, "mirroring", true); err == nil {
				t.Fatal("overlap accepted")
			}
			fs, err := cephfs.Start(ctx, cluster, cephfs.Config{Name: "tc-mgr-module-fs"})
			if err != nil {
				t.Fatal(err)
			}
			harness.CephCommand(t, ctx, cluster, "fs", "mirror", "enable", fs.FilesystemName)
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				if _, err := cluster.Ceph(cleanupCtx, "fs", "mirror", "disable", fs.FilesystemName); err != nil {
					t.Errorf("clear mirror policy: %v", err)
				}
			})
			if err := mirrorLease.Restore(ctx); err == nil || !lookup("mirroring").Enabled {
				t.Fatal("native mirroring policy was not protected")
			}
			if _, err := override.SetMGRModule(ctx, cluster, "volumes", false); err == nil {
				t.Fatal("CephFS volumes module disable accepted")
			}
			harness.CephCommand(t, ctx, cluster, "fs", "mirror", "disable", fs.FilesystemName)
			copy = *mirrorLease
			if err := copy.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			if err := mirrorLease.Restore(ctx); err != nil || lookup("mirroring").Enabled {
				t.Fatal("original disabled mirroring state was not restored", err)
			}
			harness.CephCommand(t, ctx, cluster, "mgr", "module", "enable", "mirroring")
			if !lookup("mirroring").Enabled {
				t.Fatal("native prior enabled state did not converge")
			}
			priorEnabled := apply("mirroring", false)
			if lookup("mirroring").Enabled {
				t.Fatal("temporary mirroring disable did not persist")
			}
			if err := priorEnabled.Restore(ctx); err != nil || !lookup("mirroring").Enabled {
				t.Fatal("original enabled optional module was not restored", err)
			}
			harness.CephCommand(t, ctx, cluster, "mgr", "module", "disable", "mirroring")
			t.Log("native MGR membership and dependencies, real rbd_support schedule metadata/task completion, optional-module enable/disable restoration, always-on and active mirroring-policy protection verified")
		})
	}
}
