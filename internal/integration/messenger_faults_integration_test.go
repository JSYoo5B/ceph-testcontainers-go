//go:build all || (integration && (!ci || (ci_recovery && (!ci_batch || ci_batch_container_pause))))

//ci: timeout=25m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// Ceph's own messenger fault options reproduce client-facing faults with the
// official image alone, which has no tc, ip or iptables, and work on host
// networks too. ms_blackhole_client on one OSD drops the client messages it
// receives while it stays up for its peers, so only objects whose primary is
// that OSD time out. ms_inject_delay_* delays the client messages OSDs receive.
func TestMessengerFaultInjection(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			var opts []testcontainers.ContainerCustomizer
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			const pool = "tc-messenger-faults"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
				t.Fatal(err)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			osds := cluster.OSDs()
			if len(osds) != 2 {
				t.Fatalf("want two owned OSDs, got %d", len(osds))
			}
			target := osds[1]
			// Pick objects until each OSD is the acting primary of two of them.
			primaries := map[string]int{}
			counts := map[int]int{}
			var names []string
			for index := 0; index < 64 && (counts[osds[0].ID] < 2 || counts[target.ID] < 2); index++ {
				object := fmt.Sprintf("object-%02d", index)
				var mapping struct {
					Primary *int `json:"acting_primary"`
				}
				if err := json.Unmarshal(mustCeph(t, ctx, cluster, "osd", "map", pool, object, "--format", "json"), &mapping); err != nil || mapping.Primary == nil {
					t.Fatal("native object mapping unavailable", err)
				}
				primaries[object], names = *mapping.Primary, append(names, object)
				counts[*mapping.Primary]++
			}
			if counts[osds[0].ID] < 2 || counts[target.ID] < 2 {
				t.Fatalf("objects do not cover both primaries: %v", counts)
			}
			probe := func(t *testing.T, generation, timeout string) []messengerFaultResult {
				t.Helper()
				encoded, err := json.Marshal(names)
				if err != nil {
					t.Fatal(err)
				}
				out := execOutput(t, ctx, client, "python3", "-c", messengerFaultProbe, pool, generation, timeout, string(encoded))
				var results []messengerFaultResult
				if err := json.Unmarshal([]byte(lastLine(out)), &results); err != nil || len(results) != len(names) {
					t.Fatalf("messenger fault probe output: %s %v", out, err)
				}
				t.Logf("MESSENGER_FAULT_CLIENT generation=%s results=%s", generation, lastLine(out))
				return results
			}
			allWritten := func(t *testing.T, generation string, limit float64) []float64 {
				t.Helper()
				var seconds []float64
				for _, result := range probe(t, generation, "60") {
					if result.Errno != 0 || !result.Verified || result.Seconds > limit {
						t.Fatalf("%s write of %s: %+v", generation, result.Name, result)
					}
					seconds = append(seconds, result.Seconds)
				}
				return seconds
			}
			allWritten(t, "seed", 10)

			t.Run("client-blackhole", func(t *testing.T) {
				hold := messengerFaultConfig(t, ctx, cluster, ceph.ConfigSetting{Section: fmt.Sprintf("osd.%d", target.ID), Name: "ms_blackhole_client", Value: "true"})
				for _, result := range probe(t, "blocked", "5") {
					blocked := primaries[result.Name] == target.ID
					if blocked && result.Errno != 110 || !blocked && (result.Errno != 0 || !result.Verified) {
						t.Fatalf("object %s with primary osd.%d: %+v", result.Name, primaries[result.Name], result)
					}
				}
				states, err := cluster.OSDStates(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, state := range states {
					if !state.Up || !state.In {
						t.Fatalf("osd.%d left the map while dropping client messages: %+v", state.ID, state)
					}
				}
				hold()
				allWritten(t, "after-blackhole", 10)
			})

			t.Run("client-delay", func(t *testing.T) {
				hold := messengerFaultConfig(t, ctx, cluster,
					ceph.ConfigSetting{Section: "osd", Name: "ms_inject_delay_type", Value: "client"},
					ceph.ConfigSetting{Section: "osd", Name: "ms_inject_delay_probability", Value: "1"},
					ceph.ConfigSetting{Section: "osd", Name: "ms_inject_delay_max", Value: "2"})
				// Each delay is uniform up to the maximum, so a write slower
				// than 0.3 s among four or more is certain in practice.
				if delayed := allWritten(t, "delayed", 5); slices.Max(delayed) < 0.3 {
					t.Fatalf("no client message was delayed: %v", delayed)
				}
				hold()
				if fast := allWritten(t, "after-delay", 1); slices.Max(fast) >= 1 {
					t.Fatalf("delay remained after restore: %v", fast)
				}
			})
		})
	}
}

type messengerFaultResult struct {
	Name     string  `json:"name"`
	Errno    int     `json:"errno"`
	Seconds  float64 `json:"seconds"`
	Verified bool    `json:"verified"`
}

// messengerFaultConfig applies settings and returns a function that restores
// them; cleanup also restores them if the test stops first.
func messengerFaultConfig(t *testing.T, ctx context.Context, cluster *ceph.Container, settings ...ceph.ConfigSetting) func() {
	t.Helper()
	var overrides []*ceph.ConfigOverride
	restore := func() {
		for _, override := range overrides {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
			err := override.Restore(cleanup)
			stop()
			if err != nil {
				t.Error("restore messenger fault option", err)
			}
		}
	}
	t.Cleanup(restore)
	for _, setting := range settings {
		override, err := cluster.TemporaryConfig(ctx, setting)
		if override != nil {
			overrides = append(overrides, override)
		}
		if err != nil {
			t.Fatal("apply", setting.Section, setting.Name, err)
		}
	}
	return restore
}

// A fresh client per phase opens new OSD sessions under the current options.
const messengerFaultProbe = `import rados,sys,json,time,hashlib
pool,generation,timeout,names=sys.argv[1],sys.argv[2],sys.argv[3],json.loads(sys.argv[4])
client=rados.Rados(conffile='/etc/ceph/ceph.conf',conf={'keyring':'/etc/ceph/ceph.client.admin.keyring','rados_osd_op_timeout':timeout,'rados_mon_op_timeout':'30'})
client.connect()
try:
    io=client.open_ioctx(pool); results=[]
    for name in names:
        payload=hashlib.sha256((name+':'+generation).encode()).digest()*256
        started=time.time(); result={'name':name,'errno':0,'verified':False}
        try:
            io.write_full(name,payload)
            result['verified']=io.read(name,len(payload)+1,0)==payload
        except rados.Error as error:
            result['errno']=error.errno
        result['seconds']=round(time.time()-started,2)
        results.append(result)
    io.close()
finally:
    client.shutdown()
print(json.dumps(results))
`
