//go:build (all || (integration && recovery)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_mclock_starvation)))

//ci: timeout=40m job-timeout=55 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestMClockScrubUnderHighPriorityLoad asks whether a deep scrub, which mClock
// schedules in its managed queue, finishes while sustained erasure-coded write
// traffic keeps the strict high-priority queue busy. Releases whose dequeue
// drains the high-priority queue first, without a bound, can postpone managed
// work for as long as that queue is refilled; releases with the starvation
// bound log "forcing a dequeue attempt" at debug_mclock 10 whenever the bound
// is reached with high-priority work pending.
//
// Each case starts one MON and three OSDs with osd_op_queue_cut_off set to
// its value. A replicated one-PG pool carries seeded objects and is the scrub
// target; a 2+1 EC pool takes the load. The target is deep-scrubbed once on a
// quiet cluster (control), then again after the load started. The test
// samples every OSD's dump_op_pq_state during the load, records whether the
// scrub stamp advanced before the load ended, and requires the seeded objects
// to read back exactly and the PG to be consistent afterwards.
func TestMClockScrubUnderHighPriorityLoad(t *testing.T) {
	for _, cutoff := range []string{"high", "low"} {
		t.Run("cutoff_"+cutoff, func(t *testing.T) { runMClockScrubUnderLoad(t, cutoff) })
	}
}

const (
	mclockLoadSeconds  = 120
	mclockLoadClients  = 4
	mclockSeedObjects  = 32
	mclockScrubPool    = "tc-mclock-scrub"
	mclockLoadPool     = "tc-mclock-load"
	mclockScrubAfter   = 15 * time.Second
	mclockSampleEvery  = 500 * time.Millisecond
	mclockForcedMarker = "forcing a dequeue attempt"
)

func runMClockScrubUnderLoad(t *testing.T, cutoff string) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	config := filepath.Join(t.TempDir(), "mclock.conf")
	if err := os.WriteFile(config, []byte("[osd]\nosd_op_queue = mclock_scheduler\nosd_op_queue_cut_off = "+cutoff+"\ndebug_mclock = 10\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	opts = append(opts, ceph.WithConfigFile(config),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip).WithBlockSize(8<<30)))
	cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)
	t.Logf("MCLOCK ceph=%s cutoff=%s", harness.StaticFacts(cluster).CephVersion, cutoff)
	osds := clusteraccess.OSDs(t, cluster)
	for _, o := range osds {
		for _, key := range []string{"osd_op_queue", "osd_op_queue_cut_off", "osd_mclock_profile", "osd_op_num_shards", "osd_op_num_threads_per_shard"} {
			out, err := o.AdminSocket(ctx, "config", "get", key)
			if err != nil {
				t.Fatalf("osd.%d config get %s: %v", o.ID, key, err)
			}
			t.Logf("MCLOCK osd.%d %s", o.ID, strings.Join(strings.Fields(string(out)), " "))
		}
		meta := harness.MustCeph(t, ctx, cluster, "osd", "metadata", fmt.Sprint(o.ID), "--format", "json")
		var m struct {
			Rotational string `json:"rotational"`
			ObjStore   string `json:"osd_objectstore"`
		}
		_ = json.Unmarshal(meta, &m)
		t.Logf("MCLOCK osd.%d rotational=%s objectstore=%s", o.ID, m.Rotational, m.ObjStore)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: mclockScrubPool, Application: "rados", PGNum: 1, Replicas: 3, MinSize: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: mclockLoadPool, Application: "rados", PGNum: 32, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf(`set -e
for i in $(seq 1 %d); do
  head -c 65536 /dev/urandom > /tmp/seed.$i
  rados -p %s put seed.$i /tmp/seed.$i
done
cd /tmp && sha256sum seed.* > /tmp/seed.sums`, mclockSeedObjects, mclockScrubPool))
	pgs, err := harness.PoolHandle(t, ctx, cluster, mclockScrubPool).PGs(ctx)
	if err != nil || len(pgs.PGs) != 1 {
		t.Fatal("scrub target PG unavailable", err)
	}
	target := pgs.PGs[0].PGID

	start := time.Now()
	if err := cluster.DeepScrubPG(ctx, target); err != nil {
		t.Fatal("quiet control deep scrub:", err)
	}
	t.Logf("MCLOCK control pg=%s quiet_deep_scrub=%s", target, time.Since(start).Round(time.Millisecond))

	logOffsets := map[int]int{}
	for _, o := range osds {
		logOffsets[o.ID] = len(harness.ContainerLogLines(t, ctx, o))
	}

	loadCtx, stopSampling := context.WithCancel(ctx)
	defer stopSampling()
	var samples sync.Map
	var sampler sync.WaitGroup
	for _, o := range osds {
		sampler.Add(1)
		go func() {
			defer sampler.Done()
			var both, highOnly, total int
			maxHigh, maxSched := 0, 0
			for loadCtx.Err() == nil {
				out, err := o.AdminSocket(loadCtx, "dump_op_pq_state")
				if err == nil {
					h, s, ok := mclockQueueSizes(out)
					if ok {
						total++
						if h > 0 && s > 0 {
							both++
						} else if h > 0 {
							highOnly++
						}
						maxHigh, maxSched = max(maxHigh, h), max(maxSched, s)
					}
				}
				select {
				case <-loadCtx.Done():
				case <-time.After(mclockSampleEvery):
				}
			}
			samples.Store(o.ID, fmt.Sprintf("samples=%d high_and_managed_pending=%d high_only=%d max_high_buckets=%d max_managed=%d", total, both, highOnly, maxHigh, maxSched))
		}()
	}

	loadDone := make(chan struct{})
	var loadOut []string
	var loadMu sync.Mutex
	var load sync.WaitGroup
	loadStart := time.Now()
	for i := range mclockLoadClients {
		load.Add(1)
		go func() {
			defer load.Done()
			code, out, err := harness.SessionExec(ctx, client, "timeout", fmt.Sprint(mclockLoadSeconds+60), "rados", "-p", mclockLoadPool,
				"bench", fmt.Sprint(mclockLoadSeconds), "write", "-b", "4096", "-t", "32", "--no-cleanup", "--run-name", fmt.Sprintf("load%d", i))
			loadMu.Lock()
			defer loadMu.Unlock()
			summary := []string{}
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, "Total writes made") || strings.HasPrefix(line, "Average IOPS") || strings.HasPrefix(line, "Average Latency(s)") || strings.HasPrefix(line, "Max latency(s)") {
					summary = append(summary, strings.Join(strings.Fields(line), " "))
				}
			}
			loadOut = append(loadOut, fmt.Sprintf("client=%d exit=%d err=%v summary=%q", i, code, err, summary))
		}()
	}
	go func() { load.Wait(); close(loadDone) }()

	select {
	case <-time.After(mclockScrubAfter):
	case <-loadDone:
		t.Fatal("the load ended before the scrub request")
	}
	scrubStart := time.Now()
	scrubCtx, cancelScrub := context.WithTimeout(ctx, time.Duration(mclockLoadSeconds)*time.Second+5*time.Minute)
	defer cancelScrub()
	scrubErr := make(chan error, 1)
	go func() { scrubErr <- cluster.DeepScrubPG(scrubCtx, target) }()

	var duringLoad bool
	var scrubElapsed time.Duration
	var err2 error
	select {
	case err2 = <-scrubErr:
		duringLoad = true
		scrubElapsed = time.Since(scrubStart)
		<-loadDone
	case <-loadDone:
		loadEnd := time.Since(scrubStart)
		err2 = <-scrubErr
		scrubElapsed = time.Since(scrubStart)
		t.Logf("MCLOCK load ended %s after the scrub request, before its stamp advanced", loadEnd.Round(time.Millisecond))
	}
	loadElapsed := time.Since(loadStart)
	stopSampling()
	sampler.Wait()
	t.Logf("MCLOCK loaded pg=%s scrub_err=%v scrub_elapsed=%s finished_during_load=%v load_elapsed=%s",
		target, err2, scrubElapsed.Round(time.Millisecond), duringLoad, loadElapsed.Round(time.Millisecond))
	for _, line := range loadOut {
		t.Logf("MCLOCK load %s", line)
	}
	for _, o := range osds {
		v, _ := samples.Load(o.ID)
		lines := harness.ContainerLogLines(t, ctx, o)
		forced, first := 0, ""
		for _, line := range lines[min(logOffsets[o.ID], len(lines)):] {
			if strings.Contains(line, mclockForcedMarker) {
				forced++
				if first == "" {
					first = line
				}
			}
		}
		t.Logf("MCLOCK osd.%d queues %v forced_dequeue_lines=%d first=%q", o.ID, v, forced, first)
	}
	if err2 != nil {
		t.Errorf("deep scrub under load did not finish within %s of the load end: %v", 5*time.Minute, err2)
	}

	sums := harness.ExecOutput(t, ctx, client, "sh", "-c", fmt.Sprintf(`set -e
cd /tmp && for i in $(seq 1 %d); do rados -p %s get seed.$i /tmp/read.$i; mv /tmp/read.$i /tmp/seed.$i; done
sha256sum -c /tmp/seed.sums --quiet && echo exact`, mclockSeedObjects, mclockScrubPool))
	if !strings.HasSuffix(sums, "exact") {
		t.Fatalf("seeded objects did not read back exactly: %s", sums)
	}
	objects, err := harness.ObserveCluster(cluster).PGInconsistencies(ctx, target)
	if err != nil || len(objects) != 0 {
		t.Fatalf("scrub target inconsistencies %+v: %v", objects, err)
	}
}

// mclockQueueSizes sums the per-shard high-priority bucket and managed request
// counts of one dump_op_pq_state reply, whose shards are objects named
// "OSD:ShardedOpWQ:<n>", wrapped in "pq" or not.
func mclockQueueSizes(data []byte) (high, managed int, ok bool) {
	var top map[string]json.RawMessage
	if json.Unmarshal(data, &top) != nil {
		return 0, 0, false
	}
	if inner, found := top["pq"]; found {
		top = nil
		if json.Unmarshal(inner, &top) != nil {
			return 0, 0, false
		}
	}
	for name, raw := range top {
		if !strings.HasPrefix(name, "OSD:ShardedOpWQ:") {
			continue
		}
		var shard struct {
			Sizes *struct {
				High      int `json:"high_priority_queue"`
				Scheduler int `json:"scheduler"`
			} `json:"queue_sizes"`
		}
		if json.Unmarshal(raw, &shard) != nil || shard.Sizes == nil {
			return 0, 0, false
		}
		high += shard.Sizes.High
		managed += shard.Sizes.Scheduler
		ok = true
	}
	return high, managed, ok
}
