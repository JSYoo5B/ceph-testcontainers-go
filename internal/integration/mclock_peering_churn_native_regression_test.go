//go:build (all || integration) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_mclock_peering_churn)))

//ci: timeout=40m job-timeout=55 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// TestMClockScrubUnderPeeringChurn asks whether a deep scrub, whose events
// mClock schedules in its managed queue, progresses while peering work on
// other PGs of the same OSDs keeps the strict queue refilled. Peering events
// and every PG message other than client ops (sub-ops, replies, notify, info,
// query) are immediate-class, so they bypass the managed queue whatever
// osd_op_queue_cut_off says, and a MON-driven stream of new intervals produces
// them independently of the OSDs' managed work.
//
// One MON and three OSDs start with debug_mclock 10; the MON proposes every
// 0.1 s so that the churn commits several OSD maps a second. A size-3 one-PG pool
// holds seeded objects and is the scrub target (PG seed 0, OSD shard 0). A
// separate size-3 pool of 232 PGs supplies the churn (with the target, the
// load pool and .mgr, just under the per-OSD PG limit), and two rados bench
// clients keep client ops waiting in the managed queue on a third pool: a client toggles its min_size,
// which starts a new interval for each of its PGs and none for the target.
// The target is deep-scrubbed once quiet and once during the churn; every
// OSD's queue sizes are sampled per shard, the target's last_became_active
// must not change, and the seeded objects must read back exactly.
func TestMClockScrubUnderPeeringChurn(t *testing.T) {
	for _, cutoff := range []string{"high", "low"} {
		t.Run("cutoff_"+cutoff, func(t *testing.T) { runMClockPeeringChurn(t, cutoff, "") })
	}
	// The smallest starvation bound the bounded builds accept; builds without
	// the option ignore it, which the config readback shows.
	t.Run("cutoff_high_bound_min", func(t *testing.T) { runMClockPeeringChurn(t, "high", "osd_mclock_max_starve_time_hdd = 0.1\n") })
}

const (
	churnSeconds    = 120
	churnTargetPool = "tc-churn-scrub"
	churnPool       = "tc-churn"
	churnPGs        = 232
	churnLoadPool   = "tc-churn-load"
	churnSeeds      = 600
	churnScrubAfter = 15 * time.Second
	churnForced     = "forcing a dequeue attempt"
)

// churnScript toggles min_size of a pool through the MON for a fixed time and
// prints the number of committed changes.
const churnScript = `import json, sys, time, rados
c = rados.Rados(conffile='/etc/ceph/ceph.conf'); c.connect()
end = time.time() + float(sys.argv[2]); n = 0; v = 1
while time.time() < end:
    r, _, err = c.mon_command(json.dumps({"prefix": "osd pool set", "pool": sys.argv[1], "var": "min_size", "val": str(v)}), b'')
    if r != 0:
        print("ERR", r, err); sys.exit(1)
    v = 3 - v; n += 1
print("changes", n)
`

const churnSeedScript = `import sys, os, hashlib, rados
c = rados.Rados(conffile='/etc/ceph/ceph.conf'); c.connect()
io = c.open_ioctx(sys.argv[1]); h = hashlib.sha256()
mode = sys.argv[3]
for i in range(int(sys.argv[2])):
    name = "seed.%d" % i
    if mode == "write":
        data = os.urandom(4096); io.write_full(name, data)
    else:
        data = io.read(name, 1 << 20)
    h.update(name.encode()); h.update(data)
print(h.hexdigest())
`

type churnShardSample struct{ total, both, highOnly, maxHigh, maxManaged int }

func runMClockPeeringChurn(t *testing.T, cutoff, extra string) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	config := filepath.Join(t.TempDir(), "mclock.conf")
	if err := os.WriteFile(config, []byte("[mon]\npaxos_propose_interval = 0.1\n[osd]\nosd_op_queue = mclock_scheduler\nosd_op_queue_cut_off = "+cutoff+"\ndebug_mclock = 10\n"+extra), 0o600); err != nil {
		t.Fatal(err)
	}
	opts = append(opts, ceph.WithConfigFile(config),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip).WithBlockSize(8<<30)))
	cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)
	t.Logf("CHURN ceph=%s cutoff=%s", harness.StaticFacts(cluster).CephVersion, cutoff)
	osds := clusteraccess.OSDs(t, cluster)
	for _, o := range osds {
		for _, key := range []string{"osd_op_queue_cut_off", "osd_mclock_profile", "debug_mclock", "osd_mclock_max_starve_time_hdd", "osd_op_num_shards_hdd", "osd_op_num_threads_per_shard_hdd", "osd_peering_op_priority"} {
			out, err := o.AdminSocket(ctx, "config", "get", key)
			if err != nil {
				t.Logf("CHURN osd.%d config get %s: %v", o.ID, key, err)
				continue
			}
			t.Logf("CHURN osd.%d %s", o.ID, strings.Join(strings.Fields(string(out)), " "))
		}
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: churnTargetPool, Application: "rados", PGNum: 1, Replicas: 3, MinSize: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: churnPool, Application: "rados", PGNum: churnPGs, Replicas: 3, MinSize: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: churnLoadPool, Application: "rados", PGNum: 8, Replicas: 3, MinSize: 2}); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	seeded := harness.LastLine(harness.ExecOutput(t, ctx, client, "python3", "-c", churnSeedScript, churnTargetPool, fmt.Sprint(churnSeeds), "write"))
	pgs, err := harness.PoolHandle(t, ctx, cluster, churnTargetPool).PGs(ctx)
	if err != nil || len(pgs.PGs) != 1 {
		t.Fatal("scrub target PG unavailable", err)
	}
	target := pgs.PGs[0].PGID
	var mapping struct {
		Acting  []int `json:"acting"`
		Primary int   `json:"acting_primary"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "pg", "map", string(target), "--format", "json"), &mapping); err != nil {
		t.Fatal(err)
	}
	becameActive := func() string {
		var dump struct {
			Stats []struct {
				PGID   string `json:"pgid"`
				Active string `json:"last_became_active"`
			} `json:"pg_stats"`
		}
		if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "pg", "ls-by-pool", churnTargetPool, "--format", "json"), &dump); err != nil || len(dump.Stats) != 1 {
			t.Fatal("target PG report", err)
		}
		return dump.Stats[0].Active
	}
	activeBefore := becameActive()
	t.Logf("CHURN target pg=%s acting=%v primary=osd.%d last_became_active=%s", target, mapping.Acting, mapping.Primary, activeBefore)

	start := time.Now()
	if err := cluster.DeepScrubPG(ctx, target); err != nil {
		t.Fatal("quiet control deep scrub:", err)
	}
	t.Logf("CHURN control pg=%s quiet_deep_scrub=%s", target, time.Since(start).Round(time.Millisecond))

	logOffsets := map[int]int{}
	for _, o := range osds {
		logOffsets[o.ID] = len(harness.ContainerLogLines(t, ctx, o))
	}
	sampleCtx, stopSampling := context.WithCancel(ctx)
	defer stopSampling()
	var samples sync.Map
	var sampler sync.WaitGroup
	for _, o := range osds {
		sampler.Add(1)
		go func() {
			defer sampler.Done()
			shards := map[string]*churnShardSample{}
			for sampleCtx.Err() == nil {
				if out, err := o.AdminSocket(sampleCtx, "dump_op_pq_state"); err == nil {
					for name, s := range churnShardSizes(out) {
						acc := shards[name]
						if acc == nil {
							acc = &churnShardSample{}
							shards[name] = acc
						}
						acc.total++
						if s[0] > 0 && s[1] > 0 {
							acc.both++
						} else if s[0] > 0 {
							acc.highOnly++
						}
						acc.maxHigh, acc.maxManaged = max(acc.maxHigh, s[0]), max(acc.maxManaged, s[1])
					}
				}
				select {
				case <-sampleCtx.Done():
				case <-time.After(250 * time.Millisecond):
				}
			}
			samples.Store(o.ID, shards)
		}()
	}

	loadOut := make(chan string, 2)
	for i := range 2 {
		go func() {
			code, out, err := harness.SessionExec(ctx, client, "timeout", fmt.Sprint(churnSeconds+60), "rados", "-p", churnLoadPool,
				"bench", fmt.Sprint(churnSeconds), "write", "-b", "4096", "-t", "16", "--no-cleanup", "--run-name", fmt.Sprintf("load%d", i))
			summary := []string{}
			for _, line := range strings.Split(out, "\n") {
				if strings.HasPrefix(line, "Total writes made") || strings.HasPrefix(line, "Average IOPS") || strings.HasPrefix(line, "Max latency(s)") {
					summary = append(summary, strings.Join(strings.Fields(line), " "))
				}
			}
			loadOut <- fmt.Sprintf("client=%d exit=%d err=%v summary=%q", i, code, err, summary)
		}()
	}
	churnDone := make(chan string, 1)
	go func() {
		code, out, err := harness.SessionExec(ctx, client, "timeout", fmt.Sprint(churnSeconds+60), "python3", "-c", churnScript, churnPool, fmt.Sprint(churnSeconds))
		churnDone <- fmt.Sprintf("exit=%d err=%v out=%q", code, err, harness.LastLine(strings.TrimSpace(out)))
	}()
	epochBefore := churnEpoch(t, ctx, cluster)
	select {
	case <-time.After(churnScrubAfter):
	case out := <-churnDone:
		t.Fatalf("the churn ended before the scrub request: %s", out)
	}
	scrubStart := time.Now()
	scrubErr := make(chan error, 1)
	go func() { scrubErr <- cluster.DeepScrubPG(ctx, target) }()
	var churnOut string
	var duringChurn bool
	var scrubElapsed, churnLeft time.Duration
	var errScrub error
	select {
	case errScrub = <-scrubErr:
		duringChurn = true
		scrubElapsed = time.Since(scrubStart)
		churnOut = <-churnDone
		churnLeft = time.Since(scrubStart) - scrubElapsed
	case churnOut = <-churnDone:
		churnLeft = 0
		t.Logf("CHURN churn ended %s after the scrub request, before its stamp advanced", time.Since(scrubStart).Round(time.Millisecond))
		errScrub = <-scrubErr
		scrubElapsed = time.Since(scrubStart)
	}
	stopSampling()
	sampler.Wait()
	for range 2 {
		t.Logf("CHURN load %s", <-loadOut)
	}
	epochAfter := churnEpoch(t, ctx, cluster)
	t.Logf("CHURN churn %s osdmap_epochs=%d..%d", churnOut, epochBefore, epochAfter)
	t.Logf("CHURN loaded pg=%s scrub_err=%v scrub_elapsed=%s finished_during_churn=%v churn_after_scrub=%s",
		target, errScrub, scrubElapsed.Round(time.Millisecond), duringChurn, churnLeft.Round(time.Millisecond))
	for _, o := range osds {
		v, _ := samples.Load(o.ID)
		shards, _ := v.(map[string]*churnShardSample)
		var parts []string
		for i := range len(shards) {
			if s := shards[fmt.Sprintf("OSD:ShardedOpWQ:%d", i)]; s != nil {
				parts = append(parts, fmt.Sprintf("shard%d{n=%d both=%d high_only=%d max_high_items=%d max_managed=%d}", i, s.total, s.both, s.highOnly, s.maxHigh, s.maxManaged))
			}
		}
		lines := harness.ContainerLogLines(t, ctx, o)
		forced, first := 0, ""
		for _, line := range lines[min(logOffsets[o.ID], len(lines)):] {
			if strings.Contains(line, churnForced) {
				forced++
				if first == "" {
					first = line
				}
			}
		}
		profile := 0
		for _, line := range lines {
			if strings.Contains(line, "mClockScheduler: Setting ") {
				profile++
			}
		}
		t.Logf("CHURN osd.%d %s forced_dequeue_lines=%d debug10_profile_lines=%d first=%q", o.ID, strings.Join(parts, " "), forced, profile, first)
	}
	if errScrub != nil {
		t.Errorf("deep scrub during churn did not finish: %v", errScrub)
	}
	if after := becameActive(); after != activeBefore {
		t.Errorf("the target PG became active again (%s -> %s): its interval changed, so the scrub was restarted, not starved", activeBefore, after)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		for _, args := range [][]string{{"-s"}, {"health", "detail"}, {"pg", "dump_stuck", "inactive", "unclean"}, {"osd", "pool", "ls", "detail"}} {
			out, cerr := cluster.Ceph(ctx, args...)
			t.Logf("CHURN diag ceph %v (%v):\n%s", args, cerr, out)
		}
		for _, o := range osds {
			out, aerr := o.AdminSocket(ctx, "dump_ops_in_flight")
			t.Logf("CHURN diag osd.%d dump_ops_in_flight (%v): %.4000s", o.ID, aerr, out)
			out, aerr = o.AdminSocket(ctx, "dump_op_pq_state")
			t.Logf("CHURN diag osd.%d dump_op_pq_state (%v): %.3000s", o.ID, aerr, out)
		}
		t.Fatal(err)
	}
	if read := harness.LastLine(harness.ExecOutput(t, ctx, client, "python3", "-c", churnSeedScript, churnTargetPool, fmt.Sprint(churnSeeds), "read")); read != seeded {
		t.Fatalf("seeded objects did not read back exactly: %s != %s", read, seeded)
	}
	objects, err := harness.ObserveCluster(cluster).PGInconsistencies(ctx, target)
	if err != nil || len(objects) != 0 {
		t.Fatalf("scrub target inconsistencies %+v: %v", objects, err)
	}
}

var churnQueueSize = regexp.MustCompile(`"queue_size":\s*(\d+)`)

func churnEpoch(t *testing.T, ctx context.Context, cluster *ceph.Container) int {
	t.Helper()
	var m struct {
		Epoch int `json:"epoch"`
	}
	if err := json.Unmarshal(harness.MustCeph(t, ctx, cluster, "osd", "dump", "--format", "json"), &m); err != nil {
		t.Fatal(err)
	}
	return m.Epoch
}

// churnShardSizes returns each shard's queued high-priority item count (the
// sum of the queue_size entries of its HighPriorityQueue section, which
// repeats that key once per priority) and managed request count from one
// dump_op_pq_state reply.
func churnShardSizes(data []byte) map[string][2]int {
	var top map[string]json.RawMessage
	if json.Unmarshal(data, &top) != nil {
		return nil
	}
	if inner, found := top["pq"]; found {
		top = nil
		if json.Unmarshal(inner, &top) != nil {
			return nil
		}
	}
	out := map[string][2]int{}
	for name, raw := range top {
		if !strings.HasPrefix(name, "OSD:ShardedOpWQ:") {
			continue
		}
		var shard struct {
			Sizes *struct {
				Scheduler int `json:"scheduler"`
			} `json:"queue_sizes"`
			High json.RawMessage `json:"HighPriorityQueue"`
		}
		if json.Unmarshal(raw, &shard) != nil || shard.Sizes == nil {
			continue
		}
		high := 0
		for _, m := range churnQueueSize.FindAllSubmatch(shard.High, -1) {
			n, _ := strconv.Atoi(string(m[1]))
			high += n
		}
		out[name] = [2]int{high, shard.Sizes.Scheduler}
	}
	return out
}
