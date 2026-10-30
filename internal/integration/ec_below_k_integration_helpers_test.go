//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
)

// The initial two commands reuse the retained admission actor: command 1 is
// acknowledged at K; command 2 submits each below-K operation once, then exits.
// Its fixture object names are shared with the 4+2 scenario, in a fresh pool.
const ecBelowKRecoveryWrite = `import hashlib,json,os,sys,rados
pool,size,snap=sys.argv[1],int(sys.argv[2]),int(sys.argv[3])
def payload(v):return bytes((i*13+v*41)%251 for i in range(size))
def connect():
 c=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'rados_osd_op_timeout':'15','rados_mon_op_timeout':'10'});c.connect();return c
a,b=connect(),connect()
try:
 with a.open_ioctx(pool) as w,b.open_ioctx(pool) as r:
  w.set_self_managed_snap_write([snap])
  try:w.write_full('four-two',payload(3))
  finally:w.set_self_managed_snap_write([])
  head=r.read('four-two',size+1,0);hv=r.get_last_version()
  r.set_read(snap);clone=r.read('four-two',size+1,0);sv=r.get_last_version()
  assert head==payload(3) and clone==payload(0)
  out=dict(seq=3,phase='ack',pid=os.getpid(),writer=a.get_instance_id(),reader=b.get_instance_id(),value=3,size=size,snap=snap,head_sha256=hashlib.sha256(head).hexdigest(),head_version=hv,snap_sha256=hashlib.sha256(clone).hexdigest(),snap_version=sv)
finally:a.shutdown();b.shutdown()
print(json.dumps(out))
`

func ecBelowKWait(t *testing.T, ctx context.Context, c *ceph.Container, pool string, p ecBelowKPlan, base ceph.PoolPGSnapshot, lost []int, floor uint32) ceph.PoolPGSnapshot {
	t.Helper()
	wait, done := context.WithTimeout(ctx, 3*time.Minute)
	defer done()
	var last error
	for {
		s, err := harness.PoolHandle(t, wait, c, pool).PGs(wait)
		if err == nil {
			last = checkECBelowKPlacement(p, base, s, lost, floor)
			if last == nil {
				return s
			}
		} else {
			last = err
		}
		select {
		case <-wait.Done():
			t.Fatal("below-K placement gate:", last)
		case <-time.After(500 * time.Millisecond):
		}
	}
}
func ecBelowKRead(t *testing.T, ctx context.Context, c *ceph.Container, client testcontainers.Container, pool string, size int, seed, last ecFourTwoIO) ecFourTwoReadback {
	t.Helper()
	r, err := c.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", ecFourTwoVerifyScript, pool, strconv.Itoa(size), strconv.FormatUint(seed.Snap, 10), strconv.Itoa(last.Value)}, Timeout: time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "below-K fresh independent readback"})
	var read ecFourTwoReadback
	if err != nil || !r.Complete() || r.ExitCode != 0 || json.Unmarshal(r.Stdout, &read) != nil || checkECFourTwoReadback(seed, last, read, size, true) != nil {
		t.Fatalf("below-K readback: %s error %v stderr %.2048s", r.Stdout, err, r.Stderr)
	}
	return read
}
func testECBelowK(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 17*time.Minute)
	defer cancel()
	c, client := harness.NewServiceCluster(t, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3)))
	pool, err := c.CreatePool(ctx, ceph.PoolConfig{Name: "tc-below-k", Application: "rados", PGNum: 1, MinSize: 2, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true}})
	if err != nil {
		t.Fatal("2+1 topology admission:", err)
	}
	if err := c.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	ecAdmissionPool(t, ctx, c, pool)
	stripe := ecStripeGeometry(t, ctx, c, pool)
	if stripe == 0 || stripe%2 != 0 || stripe > 256<<10 {
		t.Fatalf("unsupported bounded K2 stripe %d", stripe)
	}
	size := int(2*stripe + 31)
	initial, err := harness.ObserveCluster(c).OSDStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	holders := map[int]*ceph.OSDContainer{}
	captured := map[int]ecAdmissionInstance{}
	for _, osd := range clusteraccess.OSDs(t, c) {
		holders[osd.ID] = osd
		captured[osd.ID] = ecAdmissionInstanceOf(t, ctx, osd)
		if err := checkECAdmissionHealthy(captured[osd.ID]); err != nil {
			t.Fatal(err)
		}
	}
	noout, err := override.SetOSDFlag(ctx, c, ceph.OSDFlagNoOut, true)
	var actor *ecFourTwoActor
	requested := map[int]bool{}
	stopped := map[int]bool{}
	var order []int
	policyRestored := false
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 2*time.Minute)
		defer done()
		if actor != nil {
			if err := actor.Finish(cleanup); err != nil {
				t.Errorf("actor shutdown unconfirmed; keep outcomes unknown and leave stores to Terminate: %v", err)
				return
			}
		}
		for i := len(order) - 1; i >= 0; i-- {
			id := order[i]
			if stopped[id] {
				if err := holders[id].Start(cleanup); err != nil {
					t.Errorf("restore osd.%d: %v", id, err)
				}
			}
		}
		if !policyRestored {
			if err := noout.Restore(cleanup); err != nil {
				t.Error(err)
			}
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	const path = "/tmp/tc-ec-below-k-actor"
	actor = startECFourTwoActor(ctx, c, client, pool.Name, path, size)
	seed := ecFourTwoReadStep(t, ctx, c, client, path, 0)
	if seed.Phase != "seed" || seed.PID <= 0 || seed.Writer == 0 || seed.Reader == 0 || seed.Writer == seed.Reader || seed.Snap == 0 || seed.Size != size || seed.HeadVersion == 0 || seed.HeadSHA256 != ecFourTwoPayloadHash(size, 0) || seed.SnapSHA256 != seed.HeadSHA256 || seed.SnapVersion != seed.HeadVersion {
		t.Fatalf("invalid below-K seed: %+v", seed)
	}
	base, err := pool.PGs(ctx)
	if err != nil || len(base.PGs) != 1 {
		t.Fatalf("initial placement %+v %v", base, err)
	}
	p, err := planECBelowK(base.PGs[0].Acting, base.PGs[0].ActingPrimary)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range p.Original {
		if holders[id] == nil {
			t.Fatal("unowned holder")
		}
	}
	stop := func(index int) {
		t.Helper()
		id := p.Original[index]
		osd := holders[id]
		now := ecAdmissionInstanceOf(t, ctx, osd)
		if err := checkECAdmissionStop(captured[id], now); err != nil {
			t.Fatal(err)
		}
		t.Logf("EC_BELOW_K_STOP_REQUEST %s", mustAdmissionJSON(t, map[string]any{"index": index, "captured": captured[id], "admitted": now}))
		requested[id] = true
		stopped[id] = true
		order = append(order, id)
		timeout := 20 * time.Second
		if err := osd.Stop(ctx, &timeout); err != nil {
			t.Fatal(err)
		}
		after := ecAdmissionInstanceOf(t, ctx, osd)
		t.Logf("EC_BELOW_K_STOP_RESULT %s", mustAdmissionJSON(t, after))
		if err := checkECAdmissionStopped(captured[id], after); err != nil {
			t.Fatal(err)
		}
	}
	instances := func() {
		t.Helper()
		for id, osd := range holders {
			now := ecAdmissionInstanceOf(t, ctx, osd)
			var err error
			if stopped[id] {
				err = checkECAdmissionStopped(captured[id], now)
			} else {
				err = checkECAdmissionStop(captured[id], now)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	stop(p.Losses[0])
	one := ecBelowKWait(t, ctx, c, pool.Name, p, base, p.Losses[:1], base.PGs[0].ReportedEpoch)
	instances()
	t.Logf("EC_BELOW_K_PHASE exact-K %s", mustAdmissionJSON(t, one))
	if err := actor.send(ctx, 1, 1, "write"); err != nil {
		t.Fatal("exact-K delivery unknown:", err)
	}
	ack := ecFourTwoReadStep(t, ctx, c, client, path, 1)
	if err := checkECFourTwoAck(seed, seed, ack, size, 1, 1); err != nil {
		t.Fatal(err)
	}
	oneAfter, err := pool.PGs(ctx)
	if err != nil || checkECBelowKPlacement(p, base, oneAfter, p.Losses[:1], base.PGs[0].ReportedEpoch) != nil {
		t.Fatalf("exact-K write interval changed %+v %v", oneAfter, err)
	}
	stop(p.Losses[1])
	below := ecBelowKWait(t, ctx, c, pool.Name, p, base, p.Losses, oneAfter.PGs[0].ReportedEpoch)
	instances()
	t.Logf("EC_BELOW_K_PHASE below-K %s", mustAdmissionJSON(t, below))
	if err := actor.send(ctx, 2, 2, "blocked"); err != nil {
		t.Fatal("below-K delivery unknown:", err)
	}
	unknown := ecFourTwoReadStep(t, ctx, c, client, path, 2)
	if err := checkECBelowKUnknown(seed, unknown); err != nil {
		t.Fatal(err)
	}
	belowAfter, err := pool.PGs(ctx)
	if err != nil || checkECBelowKPlacement(p, base, belowAfter, p.Losses, oneAfter.PGs[0].ReportedEpoch) != nil {
		t.Fatalf("below-K operation interval changed %+v %v", belowAfter, err)
	}
	if err := actor.Finish(ctx); err != nil {
		t.Fatal("shutdown before restoring stores:", err)
	}
	var shutdown ecFourTwoShutdown
	if json.Unmarshal(actor.finished.result.Stdout, &shutdown) != nil || checkECFourTwoShutdown(seed, shutdown) != nil {
		t.Fatal("original clients shutdown unconfirmed")
	}
	t.Logf("EC_BELOW_K_CLIENT_SHUTDOWN %s", actor.finished.result.Stdout)
	restoredID := p.Original[p.Losses[1]]
	if err := holders[restoredID].Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped[restoredID] = false
	resumed := ecBelowKWait(t, ctx, c, pool.Name, p, base, p.Losses[:1], belowAfter.PGs[0].ReportedEpoch)
	t.Logf("EC_BELOW_K_PHASE resumed-K %s", mustAdmissionJSON(t, resumed))
	for id, osd := range holders {
		now := ecAdmissionInstanceOf(t, ctx, osd)
		if id == restoredID {
			if err := checkECAdmissionHealthy(now); err != nil {
				t.Fatal(err)
			}
			if now.ContainerID != captured[id].ContainerID || now.StartedAt == captured[id].StartedAt {
				t.Fatal("K restoration did not retain the original store container")
			}
		} else if stopped[id] {
			if err := checkECAdmissionStopped(captured[id], now); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := checkECAdmissionStop(captured[id], now); err != nil {
				t.Fatal(err)
			}
		}
	}
	kRead := ecBelowKRead(t, ctx, c, client, pool.Name, size, seed, ack)
	write, err := c.RunNativeCommand(ctx, client, ceph.NativeCommand{Argv: []string{"python3", "-c", ecBelowKRecoveryWrite, pool.Name, strconv.Itoa(size), strconv.FormatUint(seed.Snap, 10)}, Timeout: time.Minute, MaxStdout: 1 << 16, MaxStderr: 1 << 16, Label: "below-K fresh exact-K recovery write"})
	var recoveredAck ecFourTwoIO
	if err != nil || !write.Complete() || write.ExitCode != 0 || json.Unmarshal(write.Stdout, &recoveredAck) != nil {
		t.Fatalf("recovery write completion unknown; no replay: %v %.2048s", err, write.Stderr)
	}
	freshBase := seed
	freshBase.PID, freshBase.Writer, freshBase.Reader = recoveredAck.PID, recoveredAck.Writer, recoveredAck.Reader
	if err := checkECFourTwoAck(freshBase, ack, recoveredAck, size, 3, 3); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{recoveredAck.Writer, recoveredAck.Reader} {
		if id == seed.Writer || id == seed.Reader || id == kRead.Reader {
			t.Fatal("recovery write reused a prior client")
		}
	}
	resumedAfter, err := pool.PGs(ctx)
	if err != nil || checkECBelowKPlacement(p, base, resumedAfter, p.Losses[:1], belowAfter.PGs[0].ReportedEpoch) != nil {
		t.Fatalf("resumed I/O interval changed %+v %v", resumedAfter, err)
	}
	remaining := p.Original[p.Losses[0]]
	if err := holders[remaining].Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped[remaining] = false
	if err := c.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	full := ecBelowKWait(t, ctx, c, pool.Name, p, base, nil, resumedAfter.PGs[0].ReportedEpoch)
	for id, osd := range holders {
		now := ecAdmissionInstanceOf(t, ctx, osd)
		if err := checkECAdmissionHealthy(now); err != nil {
			t.Fatal(err)
		}
		if now.ContainerID != captured[id].ContainerID || requested[id] && now.StartedAt == captured[id].StartedAt {
			t.Fatal("restored owned generation not observed")
		}
		if !requested[id] {
			if err := checkECAdmissionStop(captured[id], now); err != nil {
				t.Fatal(err)
			}
		}
	}
	final, err := harness.ObserveCluster(c).OSDStates(ctx)
	if err != nil || len(final) != len(initial) {
		t.Fatal("OSD registrations unavailable", err)
	}
	for i, s := range final {
		if s.ID != initial[i].ID || s.UUID != initial[i].UUID || !s.Up || !s.In {
			t.Fatal("OSD registration changed")
		}
	}
	read := ecBelowKRead(t, ctx, c, client, pool.Name, size, seed, recoveredAck)
	if slices.Contains([]uint64{kRead.Reader, recoveredAck.Writer, recoveredAck.Reader}, read.Reader) {
		t.Fatal("final read reused prior client")
	}
	t.Logf("EC_BELOW_K_RECEIPT %s", mustAdmissionJSON(t, map[string]any{"plan": p, "k": 2, "m": 1, "min_size": 2, "stripe_width": stripe, "seed": seed, "exact_k_before": one, "ack_before": ack, "below_k": below, "unknown_operations": unknown, "client_shutdown": shutdown, "resumed_k": resumed, "resumed_read": kRead, "resumed_write": recoveredAck, "full": full, "final_read": read, "initial_osds": initial, "final_osds": final, "scope": "temporarily unavailable retained shards; no permanent-loss inference"}))
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	policyRestored = true
	if err := pool.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}
