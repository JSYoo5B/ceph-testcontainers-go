//go:build native_regression && (all || (integration && topology)) && (!ci || (ci_optional && (!ci_batch || ci_batch_osd_benchmark_policy)))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDBenchmarkStartupPolicy covers policy delivery, not a timing claim.
// Initial and later starts, same-container restart, and retained-store reopen
// must see the chosen policy even when local and central values disagree.
func TestOSDBenchmarkStartupPolicy(t *testing.T) {
	t.Run("functional-default", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
		defer cancel()
		image, opts := harness.IntegrationImages(t)
		opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)))
		cluster, err := ceph.Run(ctx, image, opts...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		osds := clusteraccess.OSDs(t, cluster)
		if len(osds) != 1 {
			t.Fatalf("initial OSDs=%d", len(osds))
		}
		policyOSDSettings(t, ctx, osds[0], true, false, "functional-initial")
		added, err := cluster.AddOSD(ctx)
		if err != nil {
			t.Fatal(err)
		}
		policyOSDSettings(t, ctx, added, true, false, "functional-added")
		grace := 3 * time.Second
		if err := osds[0].Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
		policyOSDWaitDown(t, ctx, cluster, osds[0].ID)
		if err := osds[0].Start(ctx); err != nil {
			t.Fatal(err)
		}
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
		policyOSDSettings(t, ctx, osds[0], true, false, "functional-restarted")
	})
	for _, policy := range []ceph.OSDBenchmarkPolicy{ceph.OSDBenchmarkSkip, ceph.OSDBenchmarkNative} {
		t.Run(string(policy), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			skip := policy == ceph.OSDBenchmarkSkip
			opposite := strconv.FormatBool(!skip)
			local := filepath.Join(t.TempDir(), "benchmark.conf")
			if err := os.WriteFile(local, []byte("[osd]\nosd_mclock_skip_benchmark = "+opposite+"\nosd_mclock_force_run_benchmark_on_init = false\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			image, opts := harness.IntegrationImages(t)
			opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1).WithPoolDefaults(1, 1)), ceph.WithOSDStorage(ceph.OSDStorage{}.WithVolume().WithStartupBenchmark(policy)), ceph.WithConfigFile(local))
			cluster, err := ceph.Run(ctx, image, opts...)
			if cluster != nil {
				testcontainers.CleanupContainer(t, cluster)
			}
			if err != nil {
				t.Fatal(err)
			}
			observer := harness.ObserveCluster(cluster)
			initial := clusteraccess.OSDs(t, cluster)
			if len(initial) != 1 {
				t.Fatalf("initial OSDs=%d", len(initial))
			}
			original := initial[0]
			originalCID := original.GetContainerID()
			identities := osdMemoryIdentities(t, ctx, cluster)
			initialBenchmarks := policyOSDSettings(t, ctx, original, skip, true, "initial")
			pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "policy-retained", Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := observer.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			policyOSDBytes(t, ctx, cluster, pool.Name, 3, true)
			noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
			if noout != nil {
				t.Cleanup(func() {
					cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
					defer stop()
					if err := noout.Restore(cleanup); err != nil {
						t.Error(err)
					}
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			grace := 3 * time.Second
			if err := original.Stop(ctx, &grace); err != nil {
				t.Fatal(err)
			}
			policyOSDWaitDown(t, ctx, cluster, original.ID)
			if err := original.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := observer.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			if original.GetContainerID() != originalCID || osdMemoryIdentities(t, ctx, cluster)[original.ID] != identities[original.ID] {
				t.Fatal("same-container restart changed identity")
			}
			restartedBenchmarks := policyOSDSettings(t, ctx, original, skip, true, "same-container-restart")
			t.Logf("OSD_BENCHMARK_RESTART policy=%s calibration_before=%d calibration_after=%d delta=%d", policy, initialBenchmarks, restartedBenchmarks, restartedBenchmarks-initialBenchmarks)
			policyOSDBytes(t, ctx, cluster, pool.Name, 3, false)
			detached, err := original.Detach(ctx)
			if err != nil {
				t.Fatal(err)
			}
			reopened, err := detached.Reopen(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if reopened.ID != original.ID || reopened.GetContainerID() == originalCID || detached.UUID() != identities[original.ID] {
				t.Fatal("retained reopen changed native identity or reused old container")
			}
			if err := observer.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			policyOSDSettings(t, ctx, reopened, skip, true, "retained-reopen")
			// This read precedes adding another OSD. The retained store is the only
			// replica that could supply the original bytes.
			policyOSDBytes(t, ctx, cluster, pool.Name, 3, false)
			change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: opposite})
			t.Cleanup(func() {
				cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := change.Restore(cleanup); err != nil {
					t.Error(err)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			added, err := cluster.AddOSD(ctx)
			if err != nil {
				t.Fatal(err)
			}
			policyOSDSettings(t, ctx, added, skip, true, "added-with-conflicting-central")
			replica, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "policy-recovery", Application: "rados", Replicas: 2, MinSize: 1, PGNum: 1})
			if err != nil {
				t.Fatal(err)
			}
			if err := observer.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			policyOSDBytes(t, ctx, cluster, replica.Name, 7, true)
			if err := reopened.Stop(ctx, &grace); err != nil {
				t.Fatal(err)
			}
			policyOSDWaitDown(t, ctx, cluster, reopened.ID)
			// Acknowledge different bytes through the surviving replica before rejoin.
			policyOSDBytes(t, ctx, cluster, replica.Name, 11, true)
			if err := reopened.Start(ctx); err != nil {
				t.Fatal(err)
			}
			if err := observer.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			policyOSDSettings(t, ctx, reopened, skip, true, "recovered-with-conflicting-central")
			policyOSDBytes(t, ctx, cluster, replica.Name, 11, false)
			if err := noout.Restore(ctx); err != nil {
				t.Fatal(err)
			}
			t.Logf("OSD_BENCHMARK_POLICY policy=%s initial=true added=true restart_same_cid=true reopen_same_uuid=true retained_single_copy=true acknowledged_outage_write=true recovery_bytes=true", policy)
		})
	}
	t.Run("unspecified-respects-central", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
		defer cancel()
		image, opts := harness.NativeIntegrationImages(t)
		opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()))
		cluster, err := ceph.Run(ctx, image, opts...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		change, err := override.SetConfig(ctx, cluster, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"})
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), time.Minute)
			defer stop()
			if err := change.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		first, err := cluster.AddOSD(ctx)
		if err != nil {
			t.Fatal(err)
		}
		policyOSDSettings(t, ctx, first, true, false, "unspecified-central-true")
		if err := change.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		second, err := cluster.AddOSD(ctx)
		if err != nil {
			t.Fatal(err)
		}
		policyOSDSettings(t, ctx, second, false, false, "unspecified-restored-default")
		t.Log("OSD_BENCHMARK_POLICY unspecified=true central_choice=true argv_unpinned=true")
	})
}

func policyOSDSettings(t *testing.T, ctx context.Context, osd *ceph.OSDContainer, skip, pinned bool, phase string) int {
	t.Helper()
	values := map[string]string{}
	for _, name := range []string{"osd_op_queue", "osd_mclock_skip_benchmark", "osd_mclock_force_run_benchmark_on_init", "osd_mclock_max_capacity_iops_hdd", "osd_mclock_max_capacity_iops_ssd", "osd_mclock_max_sequential_bandwidth_hdd", "osd_mclock_max_sequential_bandwidth_ssd"} {
		raw, err := osd.AdminSocket(ctx, "config", "get", name)
		if err != nil {
			t.Fatal(err)
		}
		var value map[string]string
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatal(err)
		}
		if value[name] == "" {
			t.Fatalf("missing effective %s: %s", name, raw)
		}
		values[name] = value[name]
	}
	if values["osd_op_queue"] != "mclock_scheduler" || values["osd_mclock_skip_benchmark"] != strconv.FormatBool(skip) || values["osd_mclock_force_run_benchmark_on_init"] != "false" {
		t.Fatalf("effective scheduler/policy/force differs: %+v", values)
	}
	args := strings.Split(strings.TrimSuffix(harness.ExecOutput(t, ctx, osd, "cat", "/proc/1/cmdline"), "\x00"), "\x00")
	found := 0
	for _, arg := range args {
		if strings.HasPrefix(arg, "--osd-mclock-skip-benchmark=") {
			found++
			if arg != "--osd-mclock-skip-benchmark="+strconv.FormatBool(skip) {
				t.Fatal("policy argv differs", arg)
			}
		}
	}
	if pinned && found != 1 || !pinned && found != 0 {
		t.Fatalf("benchmark argv count=%d pinned=%t", found, pinned)
	}
	encoded, _ := json.Marshal(values)
	t.Logf("OSD_BENCHMARK_EFFECTIVE phase=%s osd=%d pinned=%t values=%s", phase, osd.ID, pinned, encoded)
	logs, err := osd.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(logs)
	closeErr := logs.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("read owned OSD calibration logs: read=%v close=%v", err, closeErr)
	}
	text := string(data)
	benchmarks := strings.Count(text, "running osd bench with")
	if skip && benchmarks != 0 {
		t.Fatalf("skip policy ran calibration: phase=%s count=%d", phase, benchmarks)
	}
	if pinned && !skip && phase == "initial" {
		if benchmarks != 1 || !strings.Contains(text, "Prefill took") || !strings.Contains(text, "Clean-up done.") || !strings.Contains(text, "osd bench result -") {
			t.Fatalf("fresh explicit native OSD lacks a complete calibration receipt: count=%d", benchmarks)
		}
	}
	t.Logf("OSD_BENCHMARK_CALIBRATION phase=%s osd=%d skip=%t count=%d", phase, osd.ID, skip, benchmarks)
	return benchmarks
}

func policyOSDWaitDown(t *testing.T, parent context.Context, cluster *ceph.Container, id int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	want := ceph.OSDExpectation{Select: []ceph.OSDState{{ID: id}}, Up: false, In: true}
	for {
		states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, err := want.Evaluate(states)
		if err != nil {
			t.Fatal(err)
		}
		if result.Matched {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("owned OSD did not become down/in: %v: %v", ctx.Err(), result)
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func policyOSDBytes(t *testing.T, ctx context.Context, cluster *ceph.Container, pool string, salt int, write bool) {
	t.Helper()
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	mode := "read"
	if write {
		mode = "write"
	}
	out := osdMemoryExec(t, ctx, control, "python3", "-c", `import hashlib,json,rados,sys
pool,salt,mode=sys.argv[1],int(sys.argv[2]),sys.argv[3]
payload=bytes((i*13+salt)%251 for i in range(1048583))
with rados.Rados(conffile='/etc/ceph/ceph.conf',name='client.admin',conf={'client_mount_timeout':'10','rados_mon_op_timeout':'10','rados_osd_op_timeout':'30'}) as c:
    with c.open_ioctx(pool) as io:
        if mode=='write': io.write_full('policy-payload',payload)
        size,_=io.stat('policy-payload')
        actual=io.read('policy-payload',size)
        assert size==len(payload) and actual==payload
print(json.dumps({'Bytes':len(actual),'SHA256':hashlib.sha256(actual).hexdigest()}))`, pool, strconv.Itoa(salt), mode)
	var receipt struct {
		Bytes  int
		SHA256 string
	}
	if err := json.Unmarshal(out, &receipt); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 1048583)
	for index := range data {
		data[index] = byte((index*13 + salt) % 251)
	}
	if receipt.Bytes != len(data) || receipt.SHA256 != fmt.Sprintf("%x", sha256.Sum256(data)) {
		t.Fatalf("native policy byte receipt differs: %s", out)
	}
	t.Logf("OSD_BENCHMARK_BYTES pool=%s mode=%s salt=%d bytes=%d sha256=%s", pool, mode, salt, receipt.Bytes, receipt.SHA256)
}
