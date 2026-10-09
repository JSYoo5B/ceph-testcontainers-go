//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_osd_full_ratios))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestOSDFullRatios(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
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
			for range 2 {
				if _, err := cluster.AddOSD(ctx); err != nil {
					t.Fatal("add small OSD", err)
				}
			}
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-full-ratios", Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			pool, err := cluster.PoolStatus(ctx, "tc-full-ratios")
			if err != nil || pool.ID <= 0 {
				t.Fatal("positive native pool identity unavailable", err)
			}
			original := integrationFullRatios(t, ctx, cluster)
			probe := func(t *testing.T, phase string) {
				t.Helper()
				execCommand(t, ctx, client, "python3", "-c", `import subprocess,sys
subprocess.run([sys.executable,"-c",sys.argv[1],sys.argv[2]],timeout=35,check=True)`, fullRatiosRADOSProbe, phase)
			}
			probe(t, "seed")
			for _, fault := range []struct {
				name, code string
				ratios     ceph.FullRatios
			}{
				{"nearfull", "OSD_NEARFULL", ceph.FullRatios{NearFull: .0001, BackfillFull: .90, Full: .95}},
				{"backfillfull", "OSD_BACKFILLFULL", ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .95}},
				{"full", "OSD_FULL", ceph.FullRatios{NearFull: .0001, BackfillFull: .0002, Full: .0003}},
			} {
				t.Run(fault.name, func(t *testing.T) {
					handle, err := cluster.TemporaryFullRatios(ctx, fault.ratios)
					// A failed multi-command application may still own a partial
					// override. Register its restoration before checking the error.
					if handle != nil {
						t.Cleanup(func() {
							restoreCtx, restoreCancel := context.WithTimeout(context.Background(), time.Minute)
							defer restoreCancel()
							if err := handle.Restore(restoreCtx); err != nil {
								t.Errorf("restore full ratios: %v", err)
							}
						})
					}
					if err != nil || handle == nil {
						t.Fatal("apply full ratios", err)
					}
					applied := integrationFullRatios(t, ctx, cluster)
					expected := ceph.FullRatios{NearFull: float64(float32(fault.ratios.NearFull)), BackfillFull: float64(float32(fault.ratios.BackfillFull)), Full: float64(float32(fault.ratios.Full))}
					if applied.FSID != original.FSID || applied.Epoch <= original.Epoch || applied.Ratios != expected {
						t.Fatalf("native OSDMap ratios differ: %+v expected=%+v", applied, expected)
					}
					if overlap, err := cluster.TemporaryFullRatios(ctx, fault.ratios); err == nil || overlap != nil {
						t.Fatal("overlapping full-ratio lease was admitted")
					}
					waitFullRatioHealth(t, ctx, cluster, original.FSID, fault.code, true)
					probe(t, fault.name)
					copy := *handle
					if err := copy.Restore(ctx); err != nil {
						t.Fatal("restore via copied handle", err)
					}
					if err := handle.Restore(ctx); err != nil {
						t.Fatal("repeat restoration", err)
					}
					restored := integrationFullRatios(t, ctx, cluster)
					if restored.FSID != original.FSID || restored.Ratios != original.Ratios || restored.Epoch < applied.Epoch {
						t.Fatalf("native full ratios not restored: before=%+v after=%+v", original, restored)
					}
					waitFullRatioHealth(t, ctx, cluster, original.FSID, fault.code, false)
					probe(t, "resumed-"+fault.name)
					state, err := cluster.PoolStatus(ctx, "tc-full-ratios")
					if err != nil || state.ID != pool.ID || state.Size != pool.Size || state.PGNum != pool.PGNum || state.Quota != pool.Quota {
						t.Fatalf("ratio override changed pool identity/policy: %+v %v", state, err)
					}
					t.Logf("FULL_RATIOS phase=%s fsid=%s pool_id=%d native_epoch=%d→%d→%d native_match=true original_ratios_restored=true", fault.name, original.FSID, pool.ID, original.Epoch, applied.Epoch, restored.Epoch)
				})
			}
		})
	}
}

// Compare to independent OSDMap JSON without assuming the monitor's map stops
// advancing between the two reads. Identity and ratios must match; the later
// native epoch must include the checked map.
func integrationFullRatios(t *testing.T, ctx context.Context, cluster *ceph.Container) ceph.FullRatioSnapshot {
	t.Helper()
	actual, err := cluster.FullRatios(ctx)
	if err != nil {
		t.Fatal("full-ratio check", err)
	}
	data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
	if err != nil {
		t.Fatal("native OSDMap oracle", err)
	}
	var native struct {
		FSID         string   `json:"fsid"`
		Epoch        uint32   `json:"epoch"`
		NearFull     *float64 `json:"nearfull_ratio"`
		BackfillFull *float64 `json:"backfillfull_ratio"`
		Full         *float64 `json:"full_ratio"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.NearFull == nil || native.BackfillFull == nil || native.Full == nil {
		t.Fatal("native full-ratio fields unavailable", err)
	}
	expected := ceph.FullRatios{NearFull: *native.NearFull, BackfillFull: *native.BackfillFull, Full: *native.Full}
	if native.FSID != actual.FSID || native.Epoch < actual.Epoch || expected != actual.Ratios {
		t.Fatalf("full-ratio check differs from native OSDMap: checked=%+v native_fsid=%s native_epoch=%d native_ratios=%+v", actual, native.FSID, native.Epoch, expected)
	}
	return actual
}

func waitFullRatioHealth(t *testing.T, ctx context.Context, cluster *ceph.Container, fsid, code string, present bool) {
	t.Helper()
	phaseCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	for {
		state := integrationHealthDetails(t, phaseCtx, cluster, fsid, code)
		_, found := state.Checks[code]
		if found == present {
			return
		}
		select {
		case <-phaseCtx.Done():
			t.Fatalf("native %s present=%v not observed: %v", code, present, phaseCtx.Err())
		case <-time.After(time.Second):
		}
	}
}

const fullRatiosRADOSProbe = `import rados,sys,json,hashlib
phase=sys.argv[1]
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'8','rados_mon_op_timeout':'8'})
client.connect(); io=client.open_ioctx('tc-full-ratios')
payload=b'full-ratio-fixture\0'*(450000)
try:
    if phase=='seed': io.write_full('retained',payload)
    else:
        assert io.read('retained',len(payload),0)==payload
        if phase=='full':
            try:
                with rados.WriteOpCtx() as op:
                    op.write_full(b'write must be rejected')
                    # librados.h enum: FULL_TRY=64 bypasses client full-map
                    # waiting, leaving the OSD to return its native ENOSPC.
                    io.operate_write_op(op,'denied',0,64)
            except rados.Error as error:
                assert error.errno==28,repr(error)
                print(json.dumps({'phase':phase,'errno':error.errno,'full_try':True}))
            else: raise AssertionError('full OSD accepted FULL_TRY write')
            try: io.stat('denied')
            except rados.ObjectNotFound: pass
            else: raise AssertionError('denied write created an object')
        else:
            io.write_full('written-'+phase,b'normal write works')
            assert io.read('written-'+phase,100,0)==b'normal write works'
    assert io.read('retained',len(payload),0)==payload
    print(json.dumps({'phase':phase,'retained_bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest(),'read_preserved':True}))
finally:
    io.close(); client.shutdown()
`
