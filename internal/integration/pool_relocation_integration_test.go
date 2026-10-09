//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_pool_relocation))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Two single-replica OSD classes make every relocation observable: a PG is
// either on the fast OSD or on the slow one, never already in place.
func TestPoolPGRelocation(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(512<<20))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceClusterWithOptions(t, image, opts...)
			if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
				t.Fatal("prepare small OSDs without startup benchmark", err)
			}
			fast, err := cluster.AddOSDWithConfig(ctx, ceph.OSDConfig{DeviceClass: "fast"})
			if err != nil {
				t.Fatal("add fast OSD", err)
			}
			slow, err := cluster.AddOSDWithConfig(ctx, ceph.OSDConfig{DeviceClass: "slow"})
			if err != nil {
				t.Fatal("add slow OSD", err)
			}
			const poolName = "tc-relocation"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: poolName, Application: "rados", Replicas: 1, MinSize: 1, DeviceClass: "fast"}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			original, err := cluster.PoolStatus(ctx, poolName)
			if err != nil || original.ID <= 0 || original.PGNum != 8 {
				t.Fatal("native pool identity unavailable", original, err)
			}
			probe := func(t *testing.T, phase string) {
				t.Helper()
				t.Logf("POOL_RELOCATION_CLIENT %s", relocationExec(t, ctx, client, "python3", "-c", poolRelocationRADOSProbe, poolName, phase))
			}
			// WaitForClean can pass on PG reports made before a rule change, so
			// each phase polls the reported mapping and state it expects.
			waitPGs := func(t *testing.T, phase string, count, osd int, clean bool) {
				t.Helper()
				deadline, stop := context.WithTimeout(ctx, 3*time.Minute)
				defer stop()
				var last string
				for {
					snapshot, err := cluster.PoolPGs(deadline, poolName)
					if err == nil && snapshot.PoolAfter.ID != original.ID {
						t.Fatalf("%s: pool identity changed to %d", phase, snapshot.PoolAfter.ID)
					}
					// A held phase needs every PG remapped and some still waiting for
					// recovery; a PG without objects has nothing to recover.
					matched := err == nil && len(snapshot.PGs) == count
					waiting := false
					for _, pg := range snapshot.PGs {
						if !matched {
							break
						}
						settled := pg.State == "active+clean"
						waiting = waiting || strings.Contains(pg.State, "recovery_wait") || strings.Contains(pg.State, "backfill_wait")
						if !slices.Equal(pg.Up, []int{osd}) || !slices.Equal(pg.Acting, []int{osd}) || (clean && !settled) {
							matched, last = false, fmt.Sprintf("PG %s up=%v acting=%v state=%s", pg.PGID, pg.Up, pg.Acting, pg.State)
						}
					}
					if matched && !clean && !waiting {
						matched, last = false, "no PG is waiting for recovery"
					}
					if matched {
						states := map[string]int{}
						for _, pg := range snapshot.PGs {
							states[pg.State]++
						}
						t.Logf("POOL_RELOCATION phase=%s pool_id=%d pgs=%d osd=%d epoch=%d→%d states=%v", phase, original.ID, count, osd, snapshot.OSDMapEpochBefore, snapshot.OSDMapEpochAfter, states)
						return
					}
					if err != nil {
						last = err.Error()
					} else if len(snapshot.PGs) != count {
						last = fmt.Sprintf("%d reported PGs", len(snapshot.PGs))
					}
					select {
					case <-deadline.Done():
						t.Fatalf("%s: want %d PGs on OSD %d clean=%v; last %s", phase, count, osd, clean, last)
					case <-time.After(time.Second):
					}
				}
			}
			resize := func(t *testing.T, phase string, count int, osd int) {
				t.Helper()
				if err := cluster.SetPoolPGCount(ctx, poolName, count); err != nil {
					t.Fatal("set PG count", err)
				}
				requested, err := cluster.PoolStatus(ctx, poolName)
				if err != nil || requested.ID != original.ID || requested.PGNumTarget != count || requested.PGPlacementNumTarget != count {
					t.Fatalf("native PG targets not recorded: %+v %v", requested, err)
				}
				if err := cluster.WaitForPoolPGCount(ctx, poolName, count); err != nil {
					t.Fatal("wait for PG count", err)
				}
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
				waitPGs(t, phase, count, osd, true)
				probe(t, phase)
			}
			probe(t, "seed")
			waitPGs(t, "seed", 8, fast.ID, true)

			t.Run("split", func(t *testing.T) { resize(t, "split", 16, fast.ID) })

			t.Run("held_relocation", func(t *testing.T) {
				// Small PG logs make Ceph recover the moved objects through the
				// log instead of backfill, so norecover must be held as well.
				var holds []*ceph.OSDFlagOverride
				t.Cleanup(func() {
					restoreCtx, restoreCancel := context.WithTimeout(context.Background(), time.Minute)
					defer restoreCancel()
					for _, hold := range holds {
						if err := hold.Restore(restoreCtx); err != nil {
							t.Errorf("restore recovery flag: %v", err)
						}
					}
				})
				for _, flag := range []string{"nobackfill", "norecover"} {
					hold, err := cluster.TemporaryOSDFlag(ctx, flag, true)
					if hold != nil {
						holds = append(holds, hold)
					}
					if err != nil {
						t.Fatal("hold", flag, err)
					}
				}
				if err := cluster.SetPoolPlacement(ctx, poolName, ceph.PoolPlacement{DeviceClass: "slow"}); err != nil {
					t.Fatal("move pool to slow class", err)
				}
				moved, err := cluster.PoolStatus(ctx, poolName)
				if err != nil || moved.ID != original.ID || moved.CRUSHRule == original.CRUSHRule || moved.Size != 1 {
					t.Fatalf("pool rule was not replaced in place: %+v %v", moved, err)
				}
				// The slow OSD becomes primary while the objects remain on the
				// fast OSD; client reads recover each object on demand.
				waitPGs(t, "held", 16, slow.ID, false)
				probe(t, "held")
				for _, hold := range holds {
					if err := hold.Restore(ctx); err != nil {
						t.Fatal("release recovery", err)
					}
				}
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
				waitPGs(t, "relocated", 16, slow.ID, true)
				probe(t, "relocated")
			})

			t.Run("merge", func(t *testing.T) { resize(t, "merge", 8, slow.ID) })

			t.Run("return", func(t *testing.T) {
				if err := cluster.SetPoolPlacement(ctx, poolName, ceph.PoolPlacement{DeviceClass: "fast"}); err != nil {
					t.Fatal("return pool to fast class", err)
				}
				returned, err := cluster.PoolStatus(ctx, poolName)
				if err != nil || returned.ID != original.ID || returned.CRUSHRule != original.CRUSHRule {
					t.Fatalf("original CreatePool rule was not reused: %+v %v", returned, err)
				}
				if err := cluster.SetPoolPlacement(ctx, poolName, ceph.PoolPlacement{DeviceClass: "fast"}); err != nil {
					t.Fatal("repeat matching placement", err)
				}
				waitPGs(t, "returned", 8, fast.ID, true)
				probe(t, "returned")
				final, err := cluster.PoolStatus(ctx, poolName)
				if err != nil || final.ID != original.ID || final.Size != original.Size || final.MinSize != original.MinSize || final.PGNum != original.PGNum || final.Quota != original.Quota {
					t.Fatalf("relocation changed pool identity/policy: %+v %v", final, err)
				}
			})
		})
	}
}

func relocationExec(t *testing.T, ctx context.Context, ctr testcontainers.Container, args ...string) string {
	t.Helper()
	execCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	code, r, err := ctr.Exec(execCtx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%s exited %d: %s", args[0], code, out)
	}
	return strings.TrimSpace(string(out))
}

// Every phase rereads all earlier objects and adds its own, so any loss or
// corruption during split, held backfill, merge or return is detected.
const poolRelocationRADOSProbe = `import rados,sys,json,hashlib
pool,phase=sys.argv[1:]
phases=['seed','split','held','relocated','merge','returned']
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'20','rados_mon_op_timeout':'20'})
client.connect(); io=client.open_ioctx(pool)
def payload(name): return hashlib.sha256(name.encode()).digest()*2048
try:
    for index in range(24):
        name='%s-%02d'%(phase,index); io.write_full(name,payload(name))
    checked=0
    for earlier in phases[:phases.index(phase)+1]:
        for index in range(24):
            name='%s-%02d'%(earlier,index)
            assert io.read(name,len(payload(name))+1,0)==payload(name),name
            checked+=1
    print(json.dumps({'phase':phase,'written':24,'verified':checked}))
finally:
    io.close(); client.shutdown()
`
