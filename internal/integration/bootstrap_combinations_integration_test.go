//go:build all || (integration && (!ci || (ci_topology && (!ci_batch || ci_batch_bootstrap_combinations))))

//ci: timeout=30m job-timeout=40

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
)

// Option combinations that Run admits but no other test starts. Each case
// boots its cluster, checks the option took effect, and writes and reads an
// object through a client.
func TestBootstrapOptionCombinations(t *testing.T) {
	configFile := filepath.Join(t.TempDir(), "ceph.conf")
	if err := os.WriteFile(configFile, []byte("[osd]\nosd_max_pg_per_osd_hard_ratio = 7\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		run  clusterRun
		opts []testcontainers.ContainerCustomizer
		pool string
		// small follows the small OSD recipe: no initial OSDs, skip mClock
		// capacity calibration, then add two OSDs.
		small bool
		check func(t *testing.T, ctx context.Context, cluster *ceph.Container)
	}{
		{
			name:  "host-memory-storage",
			run:   ceph.Run,
			opts:  []testcontainers.ContainerCustomizer{ceph.WithHostNetwork(), ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(256 << 20), ceph.WithOSDInMemoryStorage(1 << 30)},
			pool:  "tc-combo",
			small: true,
		},
		{
			name:  "host-small-blocks",
			run:   ceph.Run,
			opts:  []testcontainers.ContainerCustomizer{ceph.WithHostNetwork(), ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(128 << 20)},
			pool:  "tc-combo",
			small: true,
		},
		{
			name: "secure-messenger-config-file",
			run:  ceph.Run,
			opts: []testcontainers.ContainerCustomizer{ceph.WithMessengerMode(ceph.MessengerV2Secure), ceph.WithConfigFile(configFile)},
			pool: "tc-combo",
			check: func(t *testing.T, ctx context.Context, cluster *ceph.Container) {
				if mode := cluster.MessengerMode(); mode != ceph.MessengerV2Secure {
					t.Fatalf("messenger mode = %v", mode)
				}
				if value := strings.TrimSpace(string(mustCeph(t, ctx, cluster, "config", "show", "osd.0", "osd_max_pg_per_osd_hard_ratio"))); value != "7.000000" && value != "7" {
					t.Fatalf("config file value on osd.0 = %q", value)
				}
			},
		},
		{
			// rbd.Run adds an RBD pool, which Run admits without a manager.
			name: "rbd-without-managers",
			run:  rbd.Run,
			opts: []testcontainers.ContainerCustomizer{ceph.WithNoInitialManagers()},
			pool: "rbd",
			check: func(t *testing.T, ctx context.Context, cluster *ceph.Container) {
				if managers := cluster.Managers(); len(managers) != 0 {
					t.Fatalf("managers = %d, want none", len(managers))
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			if !test.small {
				opts = append(opts, ceph.WithOSDCount(2))
			}
			opts = append(opts, test.opts...)
			cluster, client := newServiceClusterRun(t, test.run, image, opts...)
			if test.small {
				if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if _, err := cluster.AddOSD(ctx); err != nil {
						t.Fatal(err)
					}
				}
			}
			if test.pool != "rbd" {
				if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: test.pool, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
					t.Fatal(err)
				}
			}
			// Clean PG statistics come from a manager; without one the
			// object round trip below is the readiness check.
			if len(cluster.Managers()) != 0 {
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if test.check != nil {
				test.check(t, ctx, cluster)
			}
			execOutput(t, ctx, client, "sh", "-c", `printf combination > /tmp/in && rados --pool "$1" put probe /tmp/in && rados --pool "$1" get probe /tmp/out && cmp /tmp/in /tmp/out`, "sh", test.pool)
			if test.pool == "rbd" {
				execOutput(t, ctx, client, "rbd", "create", "rbd/disk", "--size", "16M")
				if info := execOutput(t, ctx, client, "rbd", "info", "rbd/disk"); !strings.Contains(info, "size 16 MiB") {
					t.Fatalf("rbd info = %s", info)
				}
			}
		})
	}
}
