//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_scrub_inconsistency))))

//ci: timeout=20m job-timeout=30

package integration_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

// One PG with two replicas makes every object share the same primary and
// replica. An injected replica read error is invisible to clients until a deep
// scrub reports it; repair rewrites the replica from the primary.
func TestScrubInconsistency(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
			defer cancel()
			image, opts := integrationImages(t)
			opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDBlockSize(512<<20))
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceClusterWithOptions(t, image, opts...)
			fsid := strings.TrimSpace(string(mustCeph(t, ctx, cluster, "fsid")))
			if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: "osd_mclock_skip_benchmark", Value: "true"}); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				if _, err := cluster.AddOSD(ctx); err != nil {
					t.Fatal("add OSD", err)
				}
			}
			const poolName = "tc-scrub"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: poolName, Application: "rados", PGNum: 1, Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			probe := func(t *testing.T, phase string) {
				t.Helper()
				t.Logf("SCRUB_CLIENT %s", execOutput(t, ctx, client, "python3", "-c", scrubRADOSProbe, poolName, phase))
			}
			probe(t, "seed")
			var mapping struct {
				PGID          string `json:"pgid"`
				Acting        []int  `json:"acting"`
				ActingPrimary int    `json:"acting_primary"`
			}
			if err := json.Unmarshal(mustCeph(t, ctx, cluster, "osd", "map", poolName, "victim", "--format", "json"), &mapping); err != nil || len(mapping.Acting) != 2 {
				t.Fatal("native object mapping unavailable", err)
			}
			replica := mapping.Acting[0]
			if replica == mapping.ActingPrimary {
				replica = mapping.Acting[1]
			}
			expectScrub := func(t *testing.T, phase string, inconsistent bool) {
				t.Helper()
				snapshot, err := cluster.PoolPGs(ctx, poolName)
				if err != nil || len(snapshot.PGs) != 1 || snapshot.PGs[0].PGID != mapping.PGID {
					t.Fatalf("%s: native PG report unavailable: %v", phase, err)
				}
				if strings.Contains(snapshot.PGs[0].State, "inconsistent") != inconsistent {
					t.Fatalf("%s: PG state %s, want inconsistent=%v", phase, snapshot.PGs[0].State, inconsistent)
				}
				objects, err := cluster.PGInconsistencies(ctx, mapping.PGID)
				if err != nil {
					t.Fatal("inconsistency check", err)
				}
				t.Logf("SCRUB phase=%s pg=%s state=%s inconsistent=%+v", phase, mapping.PGID, snapshot.PGs[0].State, objects)
				if !inconsistent {
					if len(objects) != 0 {
						t.Fatalf("%s: clean scrub reported objects %+v", phase, objects)
					}
					return
				}
				if len(objects) != 1 || objects[0].Name != "victim" || !slices.Equal(objects[0].UnionShardErrors, []string{"read_error"}) {
					t.Fatalf("%s: unexpected inconsistent objects %+v", phase, objects)
				}
				for _, shard := range objects[0].Shards {
					damaged := shard.OSD == replica
					if damaged != slices.Contains(shard.Errors, "read_error") || shard.Primary != (shard.OSD == mapping.ActingPrimary) {
						t.Fatalf("%s: unexpected shard report %+v", phase, shard)
					}
				}
			}

			if err := cluster.InjectObjectDataError(ctx, poolName, "victim", 99); err == nil {
				t.Fatal("unowned OSD accepted an injection")
			}
			if err := cluster.InjectObjectDataError(ctx, poolName, "victim", replica); err != nil {
				t.Fatal("inject replica data error", err)
			}
			probe(t, "injected")
			if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
				t.Fatal("deep scrub", err)
			}
			expectScrub(t, "scrubbed", true)
			waitHealthCode(t, ctx, cluster, fsid, "OSD_SCRUB_ERRORS", true)
			waitHealthCode(t, ctx, cluster, fsid, "PG_DAMAGED", true)
			probe(t, "damaged")

			if err := cluster.RepairPG(ctx, mapping.PGID); err != nil {
				t.Fatal("repair", err)
			}
			waitHealthCode(t, ctx, cluster, fsid, "PG_DAMAGED", false)
			waitHealthCode(t, ctx, cluster, fsid, "OSD_SCRUB_ERRORS", false)
			// Repair rewrote the replica, so the injected error is gone too.
			if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
				t.Fatal("deep scrub after repair", err)
			}
			expectScrub(t, "repaired", false)
			probe(t, "repaired")

			// A primary read error is repaired from the replica during the read.
			if err := cluster.InjectObjectDataError(ctx, poolName, "primary-victim", mapping.ActingPrimary); err != nil {
				t.Fatal("inject primary data error", err)
			}
			probe(t, "primary")
			if err := cluster.DeepScrubPG(ctx, mapping.PGID); err != nil {
				t.Fatal("deep scrub after primary read", err)
			}
			expectScrub(t, "primary", false)
		})
	}
}

const scrubRADOSProbe = `import rados,sys,json,hashlib
pool,phase=sys.argv[1:]
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':'30','rados_mon_op_timeout':'30'})
client.connect(); io=client.open_ioctx(pool)
def payload(name): return hashlib.sha256(name.encode()).digest()*512
names=['victim','primary-victim','healthy']
try:
    if phase=='seed':
        for name in names: io.write_full(name,payload(name))
    for name in names: assert io.read(name,len(payload(name))+1,0)==payload(name),name
    print(json.dumps({'phase':phase,'verified':len(names)}))
finally:
    io.close(); client.shutdown()
`
