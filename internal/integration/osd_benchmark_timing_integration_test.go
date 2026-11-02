//go:build native_regression && (all || (integration && topology)) && (!ci || (ci_optional && (!ci_batch || ci_batch_osd_startup_benchmark)))

//ci: timeout=45m job-timeout=55 image-roles=essential,client

package integration_test

import (
	"context"
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
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDStartupBenchmarkQualification uses fresh clusters for initial startup
// and AddOSD comparisons, then measures restart separately. ABBA order gives
// native calibration and explicit skip equal average order positions. It never
// changes schedulers, capacities, benchmark duration or recovery timing.
func TestOSDStartupBenchmarkQualification(t *testing.T) {
	for _, backend := range []string{"disk", "tmpfs", "memstore"} {
		t.Run(backend, func(t *testing.T) {
			order := []bool{false, true, true, false}
			if backend == "memstore" {
				order = []bool{false, true}
			}
			for trial, skip := range order {
				t.Run(fmt.Sprintf("trial-%d-skip-%t", trial, skip), func(t *testing.T) { osdBenchmarkTrial(t, backend, skip) })
			}
		})
	}
}

func osdBenchmarkTrial(t *testing.T, backend string, skip bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	started := time.Now()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = docker.Close() }()
	config := filepath.Join(t.TempDir(), "osd.conf")
	if err := os.WriteFile(config, []byte(fmt.Sprintf("[osd]\nosd_mclock_skip_benchmark = %t\nosd_mclock_force_run_benchmark_on_init = false\n", skip)), 0600); err != nil {
		t.Fatal(err)
	}
	image, options := harness.IntegrationImages(t)
	storage := ceph.OSDStorage{}.WithBlockSize(1 << 30)
	switch backend {
	case "tmpfs":
		storage = storage.WithInMemory(4 << 30)
	case "memstore":
		storage = storage.WithObjectStore(ceph.OSDMemStore)
	}
	options = append(options, ceph.WithConfigFile(config), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(2)), ceph.WithOSDStorage(storage))
	run := time.Now()
	cluster, err := ceph.Run(ctx, image, options...)
	terminated := false
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
			defer stop()
			if t.Failed() {
				for _, o := range clusteraccess.OSDs(t, cluster) {
					osdMemoryFailureLogs(t, cleanup, o.Container)
				}
			}
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal("initial startup", err)
	}
	runElapsed := time.Since(run)
	observe := harness.ObserveCluster(cluster)
	if err := observe.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	initialElapsed := time.Since(run)
	facts, err := ceph.FactsOf(cluster)
	if err != nil {
		t.Fatal(err)
	}
	initial := clusteraccess.OSDs(t, cluster)
	if len(initial) != 2 {
		t.Fatal("initial OSD count", len(initial))
	}
	for _, o := range initial {
		osdBenchmarkReceipt(t, ctx, o, backend, skip, "initial", true)
	}
	add := time.Now()
	added, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal("AddOSD", err)
	}
	addElapsed := time.Since(add)
	if err := observe.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	addReady := time.Since(add)
	osdBenchmarkReceipt(t, ctx, added, backend, skip, "added", true)
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "benchmark-condition", Application: "rados", Replicas: 3, MinSize: 2, PGNum: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := observe.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	state, err := observe.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	control, err := cluster.ControlContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	osdBenchmarkBytes(t, ctx, control, state.FSID, pool.Name, "seed", 0)
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		if err := noout.Restore(restore); err != nil {
			t.Error(err)
		}
	}()
	original := initial[0]
	before := osdBenchmarkLogs(t, ctx, original)
	previousCount := strings.Count(before, "running osd bench with")
	grace := 3 * time.Second
	if err := original.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	// Mark the stopped owned OSD down explicitly. The fault is identical in both
	// cohorts; this does not measure or shorten heartbeat/failure detection.
	if _, err := cluster.Ceph(ctx, "osd", "down", strconv.Itoa(original.ID)); err != nil {
		t.Fatal(err)
	}
	osdBenchmarkBytes(t, ctx, control, state.FSID, pool.Name, "write", 7)
	restart := time.Now()
	if err := original.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restartProcess := time.Since(restart)
	if err := observe.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	restartReady := time.Since(restart)
	osdBenchmarkBytes(t, ctx, control, state.FSID, pool.Name, "read", 7)
	osdBenchmarkReceipt(t, ctx, original, backend, skip, "restart", false)
	afterCount := strings.Count(osdBenchmarkLogs(t, ctx, original), "running osd bench with")
	if afterCount < previousCount {
		t.Fatal("benchmark log history regressed")
	}
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_BENCHMARK_TRIAL ceph=%s backend=%s skip=%t initial_run=%s initial_ready=%s add=%s add_ready=%s restart_process=%s restart_ready=%s restart_calibrations=%d scenario=%s logical_bytes=%d tmpfs_cap_bytes=%d", facts.CephVersion, backend, skip, runElapsed, initialElapsed, addElapsed, addReady, restartProcess, restartReady, afterCount-previousCount, time.Since(started), 1<<30, map[bool]int64{true: 4 << 30}[backend == "tmpfs"])
	owned := []string{cluster.GetContainerID(), control.GetContainerID()}
	for _, o := range clusteraccess.OSDs(t, cluster) {
		owned = append(owned, o.GetContainerID())
	}
	for _, m := range clusteraccess.Managers(t, cluster) {
		owned = append(owned, m.GetContainerID())
	}
	volume := ""
	if backend == "tmpfs" {
		info, err := docker.Client.ContainerInspect(ctx, initial[0].GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range info.Container.Mounts {
			if m.Destination == "/var/lib/ceph/osd" {
				volume = m.Name
			}
		}
		if volume == "" {
			t.Fatal("owned tmpfs volume unavailable")
		}
	}

	cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	if err := cluster.Terminate(cleanup); err != nil {
		t.Fatal(err)
	}
	terminated = true
	for _, id := range owned {
		if _, err := docker.Client.ContainerInspect(cleanup, id, mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatalf("owned container survived termination: %s %v", id, err)
		}
	}
	if volume != "" {
		if _, err := docker.Client.VolumeInspect(cleanup, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
			t.Fatal("owned tmpfs volume survived", err)
		}
	}
	if _, err := docker.Client.NetworkInspect(cleanup, facts.NetworkName, mobycl.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Fatal("owned network survived", err)
	}
	t.Logf("OSD_BENCHMARK_CLEANUP backend=%s skip=%t owned_daemons=%d volume=%t remaining=0", backend, skip, len(owned), volume != "")

}

func osdBenchmarkReceipt(t *testing.T, ctx context.Context, o *ceph.OSDContainer, backend string, skip bool, phase string, fresh bool) {
	t.Helper()
	keys := []string{"osd_op_queue", "osd_mclock_skip_benchmark", "osd_mclock_force_run_benchmark_on_init", "osd_mclock_max_capacity_iops_hdd", "osd_mclock_max_capacity_iops_ssd", "osd_mclock_max_sequential_bandwidth_hdd", "osd_mclock_max_sequential_bandwidth_ssd"}
	settings := map[string]string{}
	for _, key := range keys {
		data, err := o.AdminSocket(ctx, "config", "get", key)
		if err != nil {
			t.Fatal(err)
		}
		var v map[string]any
		if err := json.Unmarshal(data, &v); err != nil {
			t.Fatal(err)
		}
		settings[key] = fmt.Sprint(v[key])
	}
	if settings["osd_op_queue"] != "mclock_scheduler" || settings["osd_mclock_skip_benchmark"] != strconv.FormatBool(skip) || settings["osd_mclock_force_run_benchmark_on_init"] != "false" {
		t.Fatalf("effective scheduling differs: %v", settings)
	}
	data, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	logs := osdBenchmarkLogs(t, ctx, o)
	count := strings.Count(logs, "running osd bench with")
	if fresh && backend != "memstore" && !skip && count != 1 {
		t.Fatalf("fresh native BlueStore did not calibrate once: osd=%d count=%d logs=%s", o.ID, count, logs)
	}
	if (skip || backend == "memstore") && count != 0 {
		t.Fatalf("excluded startup calibrated: backend=%s skip=%t count=%d", backend, skip, count)
	}
	var receipts []string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "osd bench") || strings.Contains(line, "Prefill took") || strings.Contains(line, "Clean-up done") || strings.Contains(line, "capacity_for_qos") {
			receipts = append(receipts, line)
		}
	}
	t.Logf("OSD_BENCHMARK_EFFECTIVE phase=%s backend=%s osd=%d settings=%s calibrations=%d receipts=%q", phase, backend, o.ID, data, count, receipts)
	resource := harness.ExecOutput(t, ctx, o.Container, "python3", "-c", osdBenchmarkResources, strconv.Itoa(o.ID))
	t.Logf("OSD_BENCHMARK_RESOURCES phase=%s backend=%s osd=%d data=%s", phase, backend, o.ID, resource)
}

func osdBenchmarkLogs(t *testing.T, ctx context.Context, o *ceph.OSDContainer) string {
	t.Helper()
	r, err := o.Logs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func osdBenchmarkBytes(t *testing.T, ctx context.Context, control testcontainers.Container, fsid, pool, phase string, generation int) {
	t.Helper()
	out := harness.ExecOutput(t, ctx, control, "python3", "-c", osdBenchmarkBytesProbe, fsid, pool, phase, strconv.Itoa(generation))
	t.Logf("OSD_BENCHMARK_BYTES phase=%s generation=%d receipt=%s", phase, generation, out)
}

const osdBenchmarkResources = `import json,os,sys
p='/var/lib/ceph/osd/ceph-'+sys.argv[1]
r={'logical_block_bytes':None,'allocated_block_bytes':None}
try:
 s=os.stat(p+'/block');r.update(logical_block_bytes=s.st_size,allocated_block_bytes=s.st_blocks*512)
except FileNotFoundError: pass
s=os.statvfs(p);r.update(filesystem_total_bytes=s.f_blocks*s.f_frsize,filesystem_available_bytes=s.f_bavail*s.f_frsize)
for line in open('/proc/1/status'):
 if line.startswith(('VmRSS:','VmSwap:')):
  key,value,*_=line.split();r[key.rstrip(':')+'_bytes']=int(value)*1024
print(json.dumps(r,sort_keys=True))`

const osdBenchmarkBytesProbe = `import hashlib,json,rados,sys
fsid,pool,phase,generation=sys.argv[1:];generation=int(generation)
hash=hashlib.sha256()
with rados.Rados(conffile='/etc/ceph/ceph.conf',name='client.admin',conf={'rados_mon_op_timeout':'10','rados_osd_op_timeout':'30'}) as c:
 assert c.get_fsid()==fsid
 with c.open_ioctx(pool) as io:
  for n in range(4):
   data=bytes((n+generation+x)%251 for x in range(4096))*128
   key='condition-'+str(n)
   if phase in ('seed','write'):io.write_full(key,data)
   size,_=io.stat(key);actual=io.read(key,len(data))
   assert size==len(data) and actual==data
   hash.update(actual)
print(json.dumps({'fsid':fsid,'bytes':2097152,'sha256':hash.hexdigest(),'generation':generation}))`
