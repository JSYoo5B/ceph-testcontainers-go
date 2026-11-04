//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_cpu_limit))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// cpuLimitSettings is the engine's record of one container's CPU settings
// and run, read independently of the lease.
type cpuLimitSettings struct {
	NanoCPUs, CPUQuota, CPUPeriod int64
	StartedAt                     string
	PID                           int
}

func readCPULimitSettings(t *testing.T, ctx context.Context, docker *testcontainers.DockerClient, id string) cpuLimitSettings {
	t.Helper()
	call, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	result, err := docker.ContainerInspect(call, id, mobycl.ContainerInspectOptions{})
	if err != nil || result.Container.State == nil || result.Container.HostConfig == nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	r := result.Container.HostConfig.Resources
	return cpuLimitSettings{NanoCPUs: r.NanoCPUs, CPUQuota: r.CPUQuota, CPUPeriod: r.CPUPeriod, StartedAt: result.Container.State.StartedAt, PID: result.Container.State.Pid}
}

func cpuLimitDocker(t *testing.T, ctx context.Context) *testcontainers.DockerClient {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CPU_LIMIT_ENGINE version=%s os=%s arch=%s cgroup=%s driver=%s ncpu=%d", info.Info.ServerVersion, info.Info.OperatingSystem, info.Info.Architecture, info.Info.CgroupVersion, info.Info.CgroupDriver, info.Info.NCPU)
	return docker
}

// TestCPULimitRestoresAdmittedBaselines limits and restores three baselines
// without load: an owned MON created with a NanoCPUs limit, and a WithClient
// container first unlimited and then with a CFS quota the test installed
// itself. Each step is checked against an independent inspection of the same
// container generation and the lease's kernel reading, and the MON keeps
// answering while it is limited.
func TestCPULimitRestoresAdmittedBaselines(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	docker := cpuLimitDocker(t, ctx)
	image, opts := harness.IntegrationImages(t)
	opts = append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialManagers().WithNoInitialOSDs()),
		testcontainers.WithHostConfigModifier(func(h *container.HostConfig) { h.NanoCPUs = 2_000_000_000 }))
	cluster, client := harness.NewServiceClusterWithOptions(t, image, opts...)
	quorum := func(label string) {
		t.Helper()
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"ceph", "--connect-timeout", "10", "quorum_status", "--format", "json"}, Timeout: time.Minute, MaxStdout: 64 << 10, MaxStderr: 4096, Label: label})
		if err != nil || !result.Complete() || result.ExitCode != 0 {
			t.Fatalf("%s: exit=%d complete=%t: %v", label, result.ExitCode, result.Complete(), err)
		}
	}

	roundTrip := func(name string, target testcontainers.Container, limit int64, baseline, limited, restored cpuLimitSettings, limitedQuota, baselineQuota time.Duration) {
		t.Helper()
		id := target.GetContainerID()
		before := readCPULimitSettings(t, ctx, docker, id)
		if got := (cpuLimitSettings{NanoCPUs: before.NanoCPUs, CPUQuota: before.CPUQuota, CPUPeriod: before.CPUPeriod}); got != baseline {
			t.Fatalf("%s: baseline settings %+v, want %+v", name, got, baseline)
		}
		lease, err := fault.ApplyCPULimit(ctx, cluster, target, fault.CPULimit{NanoCPUs: limit})
		if lease != nil {
			t.Cleanup(func() {
				cleanup, done := context.WithTimeout(context.Background(), time.Minute)
				defer done()
				if err := lease.Restore(cleanup); err != nil {
					t.Errorf("%s: restore during cleanup: %v", name, err)
				}
			})
		}
		if err != nil {
			t.Fatalf("%s: apply: %v", name, err)
		}
		if again, err := fault.ApplyCPULimit(ctx, cluster, target, fault.CPULimit{NanoCPUs: limit}); !errors.Is(err, ceph.ErrInvalidRequest) || again != lease {
			t.Fatalf("%s: a second limit was not refused with the held lease: %v", name, err)
		}
		now := readCPULimitSettings(t, ctx, docker, id)
		if got := (cpuLimitSettings{NanoCPUs: now.NanoCPUs, CPUQuota: now.CPUQuota, CPUPeriod: now.CPUPeriod}); got != limited {
			t.Fatalf("%s: limited settings %+v, want %+v", name, got, limited)
		}
		status, err := lease.Status(ctx)
		if err != nil || status.Unlimited || status.Quota != limitedQuota || status.Period != 100*time.Millisecond {
			t.Fatalf("%s: limited kernel reading %+v: %v", name, status, err)
		}
		t.Logf("CPU_LIMIT_APPLIED target=%s baseline=%+v limited=%+v kernel_quota=%s kernel_period=%s", name, baseline, limited, status.Quota, status.Period)
		quorum(name + " limited observer")
		if err := lease.Restore(ctx); err != nil {
			t.Fatalf("%s: restore: %v", name, err)
		}
		after := readCPULimitSettings(t, ctx, docker, id)
		if got := (cpuLimitSettings{NanoCPUs: after.NanoCPUs, CPUQuota: after.CPUQuota, CPUPeriod: after.CPUPeriod}); got != restored {
			t.Fatalf("%s: restored settings %+v, want %+v", name, got, restored)
		}
		if after.StartedAt != before.StartedAt || after.PID != before.PID {
			t.Fatalf("%s: the container was restarted: %+v -> %+v", name, before, after)
		}
		status, err = lease.Status(ctx)
		if err != nil || status.Unlimited != (baselineQuota == 0) || status.Quota != baselineQuota || status.Period != 100*time.Millisecond {
			t.Fatalf("%s: restored kernel reading %+v: %v", name, status, err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatalf("%s: repeated restore: %v", name, err)
		}
		t.Logf("CPU_LIMIT_RESTORED target=%s restored=%+v kernel_unlimited=%t kernel_quota=%s same_generation=true", name, restored, status.Unlimited, status.Quota)
		quorum(name + " restored observer")
	}

	roundTrip("mon nanocpus", cluster, 500_000_000,
		cpuLimitSettings{NanoCPUs: 2_000_000_000}, cpuLimitSettings{NanoCPUs: 500_000_000}, cpuLimitSettings{NanoCPUs: 2_000_000_000},
		50*time.Millisecond, 200*time.Millisecond)
	roundTrip("client unlimited", client, 250_000_000,
		cpuLimitSettings{}, cpuLimitSettings{CPUQuota: 25_000}, cpuLimitSettings{CPUQuota: -1},
		25*time.Millisecond, 0)
	// The test installs a finite quota itself; the lease restores it exactly.
	if _, err := docker.ContainerUpdate(ctx, client.GetContainerID(), mobycl.ContainerUpdateOptions{Resources: &container.Resources{CPUQuota: 150_000}}); err != nil {
		t.Fatal(err)
	}
	roundTrip("client quota", client, 500_000_000,
		cpuLimitSettings{CPUQuota: 150_000}, cpuLimitSettings{CPUQuota: 50_000}, cpuLimitSettings{CPUQuota: 150_000},
		50*time.Millisecond, 150*time.Millisecond)
}

// cpuLimitLoadScript is a bounded client workload, run with python3 -c in
// the client container, outside the limited OSD. argv: pool, seconds,
// threads, object bytes, tag. Each thread overwrites a fixed ring of 32
// names, so the space it occupies is bounded, and no operation is retried. It
// prints one JSON object after every thread returned.
const cpuLimitLoadScript = `import json,sys,threading,time,rados
pool,secs,nthreads,size,tag=sys.argv[1],int(sys.argv[2]),int(sys.argv[3]),int(sys.argv[4]),sys.argv[5]
c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'30','rados_mon_op_timeout':'30'})
c.connect()
lock=threading.Lock()
lat=[]
errs=[0]
sub=[0]
payload=bytes((i*31+7)%251 for i in range(size))
t0=time.monotonic_ns()
end=t0+secs*1000000000
def work(k):
    with c.open_ioctx(pool) as io:
        n=0
        while time.monotonic_ns()<end:
            name='cpuload-%s-%d-%d'%(tag,k,n%32)
            n+=1
            with lock: sub[0]+=1
            s=time.monotonic_ns()
            try:
                io.write_full(name,payload)
                d=time.monotonic_ns()-s
                with lock: lat.append(d)
            except Exception:
                with lock: errs[0]+=1
ths=[threading.Thread(target=work,args=(k,)) for k in range(nthreads)]
for x in ths: x.start()
for x in ths: x.join()
c.shutdown()
lat.sort()
print(json.dumps({"completed":True,"wall_ns":time.monotonic_ns()-t0,"submitted":sub[0],"acked":len(lat),"errors":errs[0],"p50_ns":lat[len(lat)//2] if lat else 0,"max_ns":lat[-1] if lat else 0}))
`

type cpuLimitLoadReport struct {
	Completed bool  `json:"completed"`
	WallNanos int64 `json:"wall_ns"`
	Submitted int64 `json:"submitted"`
	Acked     int64 `json:"acked"`
	Errors    int64 `json:"errors"`
	P50Nanos  int64 `json:"p50_ns"`
	MaxNanos  int64 `json:"max_ns"`
}

const cpuLimitPool = "tc-cpu"

// TestCPULimitThrottlesALiveOSD limits the acting primary of a single-PG
// pool to a quarter of a CPU while a bounded client workload writes to it.
// Under the limit the OSD's own cpu.stat throttling counters advance while it
// stays the same running process, up in the OSD map, and keeps acknowledging
// writes. After Restore the unlimited baseline is back, the same workload
// throttles nothing, and a live writer that ran through every phase reads
// back byte for byte from an independent client. Latency is logged, not
// asserted: it depends on the runner.
func TestCPULimitThrottlesALiveOSD(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	docker := cpuLimitDocker(t, ctx)
	info, err := docker.Info(ctx, mobycl.InfoOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if info.Info.NCPU < 2 {
		t.Skipf("eligibility: a quarter-CPU limit on one of three OSDs needs at least two engine CPUs, the engine has %d", info.Info.NCPU)
	}
	image, opts := harness.IntegrationImages(t)
	cluster, client := harness.NewServiceClusterWithOptions(t, image, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithNoInitialOSDs()), ceph.WithOSDStorage(ceph.OSDStorage{}.WithStartupBenchmark(ceph.OSDBenchmarkSkip)))...)
	for range 3 {
		if _, err := cluster.AddOSD(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: cpuLimitPool, PGNum: 1, Replicas: 3, MinSize: 2, Application: "rados"}); err != nil {
		t.Fatal(err)
	}
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	pool, err := cluster.Pool(ctx, cpuLimitPool)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := pool.PGs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if placed, err := (expect.PGs{Count: 1, State: "active+clean", ActingSize: 3}).Evaluate(snapshot); err != nil || !placed.Matched {
		t.Fatalf("placement: %v: %v", placed, err)
	}
	pg := snapshot.PGs[0]
	var osd *ceph.OSDContainer
	for _, candidate := range clusteraccess.OSDs(t, cluster) {
		if candidate.ID == pg.ActingPrimary {
			osd = candidate
		}
	}
	if osd == nil {
		t.Fatalf("acting primary osd.%d is not an owned OSD", pg.ActingPrimary)
	}
	mapEntry := func() osdMapEntry {
		t.Helper()
		raw, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		entry, err := parseOSDMapEntry(raw, osd.ID)
		if err != nil {
			t.Fatal(err)
		}
		return entry
	}
	initialMap := mapEntry()
	initial := readCPULimitSettings(t, ctx, docker, osd.GetContainerID())
	t.Logf("CPU_LIMIT_OSD_TARGET osd=%d pg=%s acting=%v up_from=%d settings=%+v", osd.ID, pg.PGID, pg.Acting, initialMap.UpFrom, initial)

	// The live writer keeps one connection and one ledger for the whole run.
	const ledger, stopFile = "/tmp/tc-cpu-limit.jsonl", "/tmp/tc-cpu-limit.stop"
	actor := startNative(ctx, cluster, client, secondFaultCommand("cpu limit actor", 20*time.Minute, "actor", cpuLimitPool, ledger, stopFile, "1", "900"))
	actorDone := false
	t.Cleanup(func() {
		if actorDone {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(outcome.err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.Background(), time.Minute)
				defer stop()
				if err := unstopped.Stop(stopCtx); err != nil {
					t.Errorf("stop the live writer: %v", err)
				}
			}
		case <-time.After(time.Minute):
			t.Error("the live writer did not stop")
		}
	})
	healthy := secondFaultAcks(t, ctx, cluster, client, ledger, secondFaultMark{}, 3, "while unlimited")

	// window runs the workload between two kernel readings of the OSD.
	window := func(lease *fault.CPULimitLease, label string) (fault.CPULimitStatus, fault.CPULimitStatus, cpuLimitLoadReport) {
		t.Helper()
		before, err := lease.Status(ctx)
		if err != nil {
			t.Fatalf("%s window: first reading: %v", label, err)
		}
		result, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", cpuLimitLoadScript, cpuLimitPool, "10", "8", "65536", label},
			Timeout: 3 * time.Minute, MaxStdout: 4 << 10, MaxStderr: 4 << 10, Label: "cpu limit load " + label,
		})
		if err != nil || result.ExitCode != 0 || !result.Complete() {
			t.Fatalf("%s window: workload exit %d: %v: %.1024s", label, result.ExitCode, err, result.Stderr)
		}
		var report cpuLimitLoadReport
		if err := json.Unmarshal([]byte(harness.LastJSONLine(string(result.Stdout))), &report); err != nil || !report.Completed || report.Acked+report.Errors != report.Submitted {
			t.Fatalf("%s window: workload report %.512s: %v", label, result.Stdout, err)
		}
		after, err := lease.Status(ctx)
		if err != nil {
			t.Fatalf("%s window: second reading: %v", label, err)
		}
		if entry := mapEntry(); !entry.Up || !entry.In || entry.UpFrom != initialMap.UpFrom || entry.DownAt != initialMap.DownAt {
			t.Fatalf("%s window: osd.%d left the map or flapped: %+v, initially %+v", label, osd.ID, entry, initialMap)
		}
		t.Logf("CPU_LIMIT_OSD_WINDOW label=%s quota=%s period=%s unlimited=%t d_periods=%d d_throttled=%d d_throttled_time=%s submitted=%d acked=%d errors=%d p50=%s max=%s wall=%s",
			label, after.Quota, after.Period, after.Unlimited, after.Periods-before.Periods, after.ThrottledPeriods-before.ThrottledPeriods, after.ThrottledTime-before.ThrottledTime,
			report.Submitted, report.Acked, report.Errors, time.Duration(report.P50Nanos), time.Duration(report.MaxNanos), time.Duration(report.WallNanos))
		return before, after, report
	}

	lease, err := fault.ApplyCPULimit(ctx, cluster, osd, fault.CPULimit{NanoCPUs: 250_000_000})
	if lease != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := lease.Restore(cleanup); err != nil {
				t.Errorf("restore during cleanup: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal("apply:", err)
	}
	limited, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
	before, after, load := window(lease, "limited")
	if after.Quota != 25*time.Millisecond || after.Period != 100*time.Millisecond || after.Unlimited {
		t.Fatalf("limited window ran under %+v", after)
	}
	if after.ThrottledPeriods <= before.ThrottledPeriods || after.ThrottledTime <= before.ThrottledTime || after.Periods <= before.Periods {
		t.Fatalf("the limited OSD was not throttled: %+v -> %+v", before, after)
	}
	if load.Acked == 0 {
		t.Fatal("no client write was acknowledged by the limited OSD")
	}
	underLimit := secondFaultAcks(t, ctx, cluster, client, ledger, limited, 1, "under the CPU limit")

	if err := lease.Restore(ctx); err != nil {
		t.Fatal("restore:", err)
	}
	restoredSettings := readCPULimitSettings(t, ctx, docker, osd.GetContainerID())
	if restoredSettings.NanoCPUs != 0 || restoredSettings.CPUQuota != -1 || restoredSettings.CPUPeriod != 0 || restoredSettings.StartedAt != initial.StartedAt || restoredSettings.PID != initial.PID {
		t.Fatalf("restored OSD settings %+v, initially %+v", restoredSettings, initial)
	}
	restoredMark, _ := secondFaultMarkNow(t, ctx, cluster, client, ledger)
	before, after, _ = window(lease, "restored")
	if !after.Unlimited || after.ThrottledPeriods != before.ThrottledPeriods {
		t.Fatalf("the restored OSD is still limited or throttled: %+v -> %+v", before, after)
	}
	afterRestore := secondFaultAcks(t, ctx, cluster, client, ledger, restoredMark, 3, "after the baseline was restored")
	if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}

	// Close the writer and read every acknowledged object back from an
	// independent client.
	harness.ExecCommand(t, ctx, client, "touch", stopFile)
	select {
	case outcome := <-actor.done:
		actorDone = true
		if outcome.err != nil || outcome.result.ExitCode != 0 || !outcome.result.Complete() {
			t.Fatalf("live writer shutdown: %v exit %d: %.1024s", outcome.err, outcome.result.ExitCode, outcome.result.Stderr)
		}
	case <-time.After(2 * time.Minute):
		t.Fatal("the live writer did not stop after its stop file appeared")
	}
	ops, err := secondFaultOps(secondFaultLedger(t, ctx, cluster, client, ledger), true)
	if err != nil {
		t.Fatal("writer ledger refused:", err)
	}
	final := secondFaultCount(ops)
	names := make([]string, len(ops))
	for i, op := range ops {
		names[i] = op.Name
	}
	encodedNames, err := json.Marshal(names)
	if err != nil {
		t.Fatal(err)
	}
	read, err := cluster.RunNativeCommand(ctx, harness.ToolkitContainer(t, cluster), secondFaultCommand("cpu limit independent read", 3*time.Minute, "read", cpuLimitPool, string(encodedNames)))
	if err != nil || read.ExitCode != 0 || !read.Complete() {
		t.Fatalf("independent read: exit %d: %v: %.2048s", read.ExitCode, err, read.Stderr)
	}
	var objects map[string]secondFaultObject
	if err := json.Unmarshal(read.Stdout, &objects); err != nil {
		t.Fatalf("independent read %.512s: %v", read.Stdout, err)
	}
	if err := secondFaultVerify(ops, objects); err != nil {
		t.Fatal("data oracle:", err)
	}
	if final.Acked < afterRestore.Acked || final.Refused != 0 {
		t.Fatalf("writer ledger %+v after %+v", final, afterRestore)
	}
	t.Logf("CPU_LIMIT_OSD_LEDGER healthy=%+v limited=%+v under_limit=%+v restored=%+v after_restore=%+v final=%+v verified=true", healthy, limited, underLimit, restoredMark, afterRestore, final)
}
