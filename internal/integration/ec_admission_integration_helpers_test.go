//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// A blocked write is submitted exactly once to an otherwise unused object.
// Timeout remains an unknown mutation outcome. Both connections shut down
// before OSD restoration; a later fresh reader inspects its effect, without
// resubmitting it or modifying the acknowledged head/snapshot control object.
const ecAdmissionProbe = `import hashlib,json,sys,time,rados
phase,pool=sys.argv[1:3]
data=b'EC admission uncertain;'+bytes((i*31+7)%251 for i in range(17331))
def connect():
    c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'5','rados_mon_op_timeout':'10'})
    c.connect(); return c
a,b=connect(),connect()
out=dict(phase=phase,writer=a.get_instance_id(),reader=b.get_instance_id())
assert out['writer']!=out['reader']
def attempt(fn):
    start=time.monotonic_ns()
    try:
        fn(); return dict(outcome='acknowledged',errno=0,elapsed_ns=time.monotonic_ns()-start)
    except rados.Error as e:
        return dict(outcome='unknown',errno=getattr(e,'errno',None),error=type(e).__name__,elapsed_ns=time.monotonic_ns()-start)
try:
    with a.open_ioctx(pool) as writer, b.open_ioctx(pool) as reader:
        if phase=='blocked':
            out['read']=attempt(lambda: reader.read('eligible',1<<20,0))
            out['write']=attempt(lambda: writer.write_full('admission-unknown',data))
        elif phase=='observe':
            try:
                size,mtime=reader.stat('admission-unknown')
                seen=reader.read('admission-unknown',size+1,0)
                assert seen==data and size==len(data),(size,len(seen))
                out['object']=dict(present=True,size=size,sha256=hashlib.sha256(seen).hexdigest(),version=reader.get_last_version())
            except rados.ObjectNotFound:
                out['object']=dict(present=False)
        else: raise ValueError(phase)
    print(json.dumps(out))
finally:
    a.shutdown(); b.shutdown()
`

func ecAdmissionClient(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool, phase string) map[string]any {
	t.Helper()
	r, err := cluster.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", ecAdmissionProbe, phase, pool}, Timeout: 45 * time.Second, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "EC admission " + phase})
	var receipt map[string]any
	if err != nil || !r.Complete() || r.ExitCode != 0 || json.Unmarshal([]byte(harness.LastJSONLine(string(r.Stdout))), &receipt) != nil || receipt["phase"] != phase {
		t.Fatalf("admission %s: %+v, %v", phase, r, err)
	}
	t.Logf("EC_ADMISSION_CLIENT %s", harness.LastJSONLine(string(r.Stdout)))
	return receipt
}

func ecAdmissionPool(t *testing.T, ctx context.Context, cluster *ceph.Container, pool *ceph.Pool) {
	t.Helper()
	raw, err := cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", pool.ErasureCodeProfile, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var profile map[string]string
	if err := json.Unmarshal(raw, &profile); err != nil {
		t.Fatal(err)
	}
	if profile["k"] != "2" || profile["m"] != "1" || profile["plugin"] != "jerasure" || profile["technique"] != "reed_sol_van" {
		t.Fatalf("unexpected native profile %s", raw)
	}
	raw, err = cluster.Ceph(ctx, "osd", "pool", "get", pool.Name, "all", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var facts map[string]any
	if err := json.Unmarshal(raw, &facts); err != nil {
		t.Fatal(err)
	}
	if facts["size"] != float64(3) || facts["min_size"] != float64(pool.MinSize) || facts["allow_ec_overwrites"] != true {
		t.Fatalf("native 2+1 overwrite pool policy differs: %s", raw)
	}
	t.Logf("EC_ADMISSION_POOL profile=%s rule=%s resolved_min_size=%d profile_facts=%s pool_facts=%s", pool.ErasureCodeProfile, pool.CRUSHRule, pool.MinSize, mustAdmissionJSON(t, profile), mustAdmissionJSON(t, facts))
}

func mustAdmissionJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func ecAdmissionInstanceOf(t *testing.T, ctx context.Context, osd *ceph.OSDContainer) ecAdmissionInstance {
	t.Helper()
	inspected, err := osd.Inspect(ctx)
	if err != nil || inspected == nil || inspected.State == nil {
		t.Fatalf("inspect selected osd.%d: %+v, %v", osd.ID, inspected, err)
	}
	if inspected.ID != osd.GetContainerID() {
		t.Fatalf("selected osd.%d inspected another container: %q, want %q", osd.ID, inspected.ID, osd.GetContainerID())
	}
	s := inspected.State
	return ecAdmissionInstance{ContainerID: inspected.ID, Running: s.Running, Paused: s.Paused, Restarting: s.Restarting,
		Dead: s.Dead, OOMKilled: s.OOMKilled, Pid: s.Pid, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt, ExitCode: s.ExitCode}
}

func testECAdmission(t *testing.T, minSize int, primaryLost bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	config := ceph.PoolConfig{Name: "tc-ec-admission", Application: "rados", PGNum: 1, MinSize: minSize, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}}
	pool, err := cluster.CreatePool(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	ecAdmissionPool(t, ctx, cluster, pool)
	seed := ecEligiblePhase(t, ctx, cluster, client, pool.Name, "write", nil)
	seed.SnapSHA256, seed.SnapVersion = seed.HeadSHA256, seed.HeadVersion
	ecEligiblePhase(t, ctx, cluster, client, pool.Name, "read", &seed)
	if initial := ecAdmissionClient(t, ctx, cluster, client, pool.Name, "observe"); initial["object"].(map[string]any)["present"] != false {
		t.Fatal("uncertain-write object existed before its single submission")
	}
	before, err := pool.PGs(ctx)
	if err != nil || len(before.PGs) != 1 {
		t.Fatalf("initial placement: %+v %v", before, err)
	}
	plan, err := planECAdmission(before.PGs[0].Acting, before.PGs[0].ActingPrimary, pool.MinSize, primaryLost)
	if err != nil {
		t.Fatal(err)
	}
	var victim *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID == plan.Victim {
			victim = osd
		}
	}
	if victim == nil {
		t.Fatal("planned fragment holder is not owned")
	}
	captured := ecAdmissionInstanceOf(t, ctx, victim)
	if err := checkECAdmissionHealthy(captured); err != nil {
		t.Fatal(err)
	}
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		if err := noout.Restore(cleanup); err != nil {
			t.Error(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	stopRequested := false
	t.Cleanup(func() {
		if !stopRequested {
			return
		}
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		state, err := victim.State(cleanup)
		if err != nil {
			t.Error(err)
			return
		}
		if !state.Running {
			if err := victim.Start(cleanup); err != nil {
				t.Error(err)
			}
		}
	})
	admitted := ecAdmissionInstanceOf(t, ctx, victim)
	if err := checkECAdmissionStop(captured, admitted); err != nil {
		t.Fatal(err)
	}
	t.Logf("EC_ADMISSION_STOP_REQUEST %s", mustAdmissionJSON(t, map[string]any{"plan": plan, "captured": captured, "admitted": admitted, "requested_at": time.Now().UTC()}))
	stop := 20 * time.Second
	stopRequested = true
	if err := victim.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	stopped := ecAdmissionInstanceOf(t, ctx, victim)
	t.Logf("EC_ADMISSION_STOP_RESULT %s", mustAdmissionJSON(t, map[string]any{"plan": plan, "result": stopped, "observed_at": time.Now().UTC()}))
	if err := checkECAdmissionStopped(captured, stopped); err != nil {
		t.Fatal(err)
	}
	var fault ceph.PoolPGSnapshot
	var lastErr error
	for deadline := time.Now().Add(3 * time.Minute); ; {
		fault, err = pool.PGs(ctx)
		if err == nil {
			lastErr = checkECAdmissionState(plan, before, fault)
		} else {
			lastErr = err
		}
		if lastErr == nil {
			break
		}
		if ctx.Err() != nil || time.Now().After(deadline) {
			t.Fatalf("planned admission state not reached: %v; snapshot %+v", lastErr, fault)
		}
		time.Sleep(500 * time.Millisecond)
	}
	faultInstance := ecAdmissionInstanceOf(t, ctx, victim)
	if err := checkECAdmissionStopped(captured, faultInstance); err != nil {
		t.Fatal(err)
	}
	states, err := harness.ObserveCluster(cluster).OSDStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("EC_ADMISSION_FAULT %s", mustAdmissionJSON(t, map[string]any{"plan": plan, "before": before, "fault": fault, "captured": captured, "admitted": admitted, "stop_result": stopped, "fault_instance": faultInstance, "osds": states}))
	ack := seed
	if pool.MinSize == 2 {
		ack = ecEligiblePhase(t, ctx, cluster, client, pool.Name, "overwrite", &seed)
		if ack.HeadSHA256 == seed.HeadSHA256 || ack.HeadVersion <= seed.HeadVersion || ack.SnapSHA256 != seed.SnapSHA256 || ack.SnapVersion != seed.SnapVersion {
			t.Fatalf("degraded acknowledgement does not preserve the independent snapshot: %+v seed %+v", ack, seed)
		}
	} else {
		r := ecAdmissionClient(t, ctx, cluster, client, pool.Name, "blocked")
		for _, name := range []string{"read", "write"} {
			out := r[name].(map[string]any)
			if out["outcome"] != "unknown" || out["errno"] != float64(110) {
				t.Fatalf("default min_size admitted %s or failed for another reason: %s", name, mustAdmissionJSON(t, r))
			}
		}
	}
	// Check the threshold and shard/primary identity again after I/O; a client
	// result cannot replace the policy/mapping observation around the interval.
	afterIO, err := pool.PGs(ctx)
	if err != nil || checkECAdmissionState(plan, before, afterIO) != nil {
		t.Fatalf("admission interval moved: %+v %v", afterIO, err)
	}
	if err := victim.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := pool.PGs(ctx)
	if err != nil || len(recovered.PGs) != 1 || !slices.Equal(recovered.PGs[0].Acting, plan.Original) || recovered.PoolAfter.MinSize != plan.MinSize || !strings.Contains(recovered.PGs[0].State, "clean") {
		t.Fatalf("restoration changed retained placement/policy: %+v %v", recovered, err)
	}
	readback := ecEligiblePhase(t, ctx, cluster, client, pool.Name, "verify", &ack)
	if readback.Writer == ack.Writer || readback.Reader == ack.Reader {
		t.Fatal("restoration reused an earlier reader connection")
	}
	effect := ecAdmissionClient(t, ctx, cluster, client, pool.Name, "observe")
	t.Logf("EC_ADMISSION_RECEIPT %s", mustAdmissionJSON(t, map[string]any{"case": t.Name(), "pool": pool.Name, "min_size": plan.MinSize, "k": 2, "m": 1, "plan": plan, "acknowledged": ack, "readback": readback, "unknown_write_effect": effect, "recovered": recovered, "scope": "retained stopped stores; no permanent-loss or reconstruction-interruption claim"}))
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.Remove(ctx); err != nil {
		t.Fatal(fmt.Errorf("remove admission pool: %w", err))
	}
}
