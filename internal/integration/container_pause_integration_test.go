//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_container_pause))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

// A paused OSD keeps its process and sockets but answers nothing. With nodown
// it stays in the map, so clients wait for their own timeouts and the live
// primary reports slow operations. Without nodown its peer marks it down and
// degraded I/O resumes. Both phases end with the same OSD rejoining.
func TestPausedOSDFaults(t *testing.T) {
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
			fsid := strings.TrimSpace(string(mustCeph(t, ctx, cluster, "fsid")))
			// Two OSDs leave one peer to report the other down, and a short
			// complaint time makes the slow-op report observable within a phase.
			for _, setting := range []ceph.ConfigSetting{
				{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"},
				{Section: "mon", Name: "mon_osd_min_down_reporters", Value: "1"},
				{Section: "osd", Name: "osd_op_complaint_time", Value: "2"},
			} {
				if _, err := cluster.TemporaryConfig(ctx, setting); err != nil {
					t.Fatal("prepare", setting.Name, err)
				}
			}
			for range 2 {
				if _, err := cluster.AddOSD(ctx); err != nil {
					t.Fatal("add OSD", err)
				}
			}
			const poolName = "tc-pause"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: poolName, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			probe := func(t *testing.T, phase string) {
				t.Helper()
				t.Logf("CONTAINER_PAUSE_CLIENT %s", execOutput(t, ctx, client, "python3", "-c", pausedOSDRADOSProbe, poolName, phase))
			}
			probe(t, "seed")
			target := cluster.OSDs()[1]
			if target == nil || target.Container == nil {
				t.Fatal("second owned OSD unavailable")
			}
			if pause, err := cluster.PauseContainer(ctx, cluster.ControlContainer()); err == nil || pause != nil {
				t.Fatal("control CLI container was paused")
			}

			t.Run("frozen_up", func(t *testing.T) {
				hold, err := cluster.TemporaryOSDFlag(ctx, "nodown", true)
				if hold != nil {
					t.Cleanup(func() { restoreFlag(t, hold) })
				}
				if err != nil {
					t.Fatal("hold nodown", err)
				}
				pause, err := cluster.PauseContainer(ctx, target.Container)
				if pause != nil {
					t.Cleanup(func() { resumePause(t, pause) })
				}
				if err != nil {
					t.Fatal("pause OSD", err)
				}
				if again, err := cluster.PauseContainer(ctx, target.Container); err == nil || again != pause {
					t.Fatal("overlapping pause was admitted", err)
				}
				probe(t, "blocked")
				waitHealthCode(t, ctx, cluster, fsid, "SLOW_OPS", true)
				status, err := cluster.Status(ctx)
				if err != nil || status.OSDMap.NumUpOSDs != 2 {
					t.Fatalf("nodown did not keep the frozen OSD up: %+v %v", status.OSDMap, err)
				}
				if err := pause.Resume(ctx); err != nil {
					t.Fatal("resume OSD", err)
				}
				if err := pause.Resume(ctx); err != nil {
					t.Fatal("repeat resume", err)
				}
				if err := hold.Restore(ctx); err != nil {
					t.Fatal("restore nodown", err)
				}
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
				probe(t, "thawed")
				waitHealthCode(t, ctx, cluster, fsid, "SLOW_OPS", false)
			})

			t.Run("frozen_down", func(t *testing.T) {
				pause, err := cluster.PauseContainer(ctx, target.Container)
				if pause != nil {
					t.Cleanup(func() { resumePause(t, pause) })
				}
				if err != nil {
					t.Fatal("pause OSD", err)
				}
				// The heartbeat grace passes before the peer reports the OSD.
				waitOSDsUp(t, ctx, cluster, 1)
				probe(t, "degraded")
				if err := pause.Resume(ctx); err != nil {
					t.Fatal("resume OSD", err)
				}
				waitOSDsUp(t, ctx, cluster, 2)
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
				probe(t, "rejoined")
			})
		})
	}
}

func restoreFlag(t *testing.T, hold *ceph.OSDFlagOverride) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := hold.Restore(ctx); err != nil {
		t.Errorf("restore OSD flag: %v", err)
	}
}

func resumePause(t *testing.T, pause *ceph.ContainerPause) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := pause.Resume(ctx); err != nil {
		t.Errorf("resume paused container: %v", err)
	}
}

func waitOSDsUp(t *testing.T, ctx context.Context, cluster *ceph.Container, up int) {
	t.Helper()
	phaseCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	started := time.Now()
	for {
		status, err := cluster.Status(phaseCtx)
		if err == nil && status.OSDMap.NumUpOSDs == up {
			t.Logf("CONTAINER_PAUSE up_osds=%d after=%s", up, time.Since(started).Round(time.Second))
			return
		}
		select {
		case <-phaseCtx.Done():
			t.Fatalf("%d up OSDs not observed: %v %v", up, err, phaseCtx.Err())
		case <-time.After(time.Second):
		}
	}
}

// blocked: every PG includes the frozen OSD, so each write must end with the
// client's own ETIMEDOUT. Other phases must complete and preserve all data.
const pausedOSDRADOSProbe = `import rados,sys,json,hashlib,time
pool,phase=sys.argv[1:]
timeout='5' if phase=='blocked' else '120'
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'30'})
client.connect(); io=client.open_ioctx(pool)
def payload(name): return hashlib.sha256(name.encode()).digest()*1024
names=['retained-%02d'%i for i in range(8)]
try:
    if phase=='seed':
        for name in names: io.write_full(name,payload(name))
        print(json.dumps({'phase':phase,'written':len(names)})); sys.exit(0)
    if phase=='blocked':
        timed_out=0; started=time.time()
        for index in range(4):
            name='blocked-%d'%index
            try: io.write_full(name,payload(name))
            except rados.Error as error:
                assert error.errno==110,repr(error); timed_out+=1
            else: raise AssertionError('write to a frozen replica completed')
        print(json.dumps({'phase':phase,'timed_out':timed_out,'errno':110,'seconds':round(time.time()-started,1)})); sys.exit(0)
    name=phase+'-write'; started=time.time()
    io.write_full(name,payload(name)); assert io.read(name,len(payload(name))+1,0)==payload(name)
    for retained in names: assert io.read(retained,len(payload(retained))+1,0)==payload(retained),retained
    print(json.dumps({'phase':phase,'write_seconds':round(time.time()-started,1),'verified':len(names)+1}))
finally:
    io.close(); client.shutdown()
`
