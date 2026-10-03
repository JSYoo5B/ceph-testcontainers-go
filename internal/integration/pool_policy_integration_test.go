//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

func TestPoolPolicies(t *testing.T) {
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(3)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			for _, name := range []string{"tc-policy", "tc-unaffected"} {
				if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: name, Application: "rados", Replicas: 2, MinSize: 1}); err != nil {
					t.Fatal(err)
				}
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			probe := func(phase string) {
				t.Helper()
				execCommand(t, ctx, client, "python3", "-c", `import subprocess,sys
subprocess.run([sys.executable,"-c",sys.argv[1],sys.argv[2]],timeout=20,check=True)`, poolPolicyProbe, phase)
			}
			probe("seed")
			initial, err := cluster.PoolStatus(ctx, "tc-policy")
			if err != nil || initial.ID <= 0 {
				t.Fatalf("positive native policy pool ID unavailable: %+v error=%v", initial, err)
			}
			other, err := cluster.PoolStatus(ctx, "tc-unaffected")
			if err != nil || other.ID <= 0 || other.ID == initial.ID {
				t.Fatalf("native pool identities are not distinct: policy=%d other=%+v error=%v", initial.ID, other, err)
			}
			var osdMap struct {
				Pools []struct {
					ID   int64  `json:"pool"`
					Name string `json:"pool_name"`
				} `json:"pools"`
			}
			data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			if err != nil || json.Unmarshal(data, &osdMap) != nil {
				t.Fatal("independent native pool identity unavailable", err)
			}
			for _, state := range []ceph.PoolState{initial, other} {
				found := false
				for _, native := range osdMap.Pools {
					if native.Name == state.Name && native.ID == state.ID {
						found = true
					}
				}
				if !found {
					t.Fatalf("pool ID differs from authoritative OSD map: %+v", state)
				}
			}
			t.Logf("native policy pool ID=%d; unrelated pool ID=%d", initial.ID, other.ID)
			for _, size := range []struct{ size, min int }{{3, 2}, {2, 1}} {
				if err := cluster.SetPoolReplication(ctx, "tc-policy", size.size, size.min); err != nil {
					t.Fatal(err)
				}
				if err := cluster.WaitForClean(ctx); err != nil {
					t.Fatal(err)
				}
				state, err := cluster.PoolStatus(ctx, "tc-policy")
				if err != nil || state.ID != initial.ID || state.Size != size.size || state.MinSize != size.min || state.PGNum != initial.PGNum || state.CRUSHRule != initial.CRUSHRule {
					t.Fatalf("policy mutation changed identity/layout: %+v %v", state, err)
				}
				probe("verify")
			}
			// A completed native placement edit must override cached fixture
			// placement when deciding whether a new replica count is feasible.
			osdID := cluster.OSDs()[2].ID
			osdName := fmt.Sprintf("osd.%d", osdID)
			// CRUSH dump exposes the exact 16.16 item weight. Tree output's
			// rounded crush_weight is unsuitable for an exact restoration; a
			// 1 GiB fixture OSD has a much smaller weight than one TiB.
			nativeWeight := func() uint32 {
				t.Helper()
				data, err := cluster.Ceph(ctx, "osd", "crush", "dump", "--format", "json")
				if err != nil {
					t.Fatal("read native CRUSH weight", err)
				}
				var dump struct {
					Buckets []struct {
						Items []struct {
							ID     int     `json:"id"`
							Weight *uint32 `json:"weight"`
						} `json:"items"`
					} `json:"buckets"`
				}
				if err := json.Unmarshal(data, &dump); err != nil {
					t.Fatal("decode native CRUSH weight", err)
				}
				var weight *uint32
				for _, bucket := range dump.Buckets {
					for _, item := range bucket.Items {
						if item.ID != osdID {
							continue
						}
						if item.Weight == nil || (weight != nil && *weight != *item.Weight) {
							t.Fatalf("missing or inconsistent native CRUSH weight for %s", osdName)
						}
						weight = item.Weight
					}
				}
				if weight == nil {
					t.Fatalf("native CRUSH item %s is missing", osdName)
				}
				return *weight
			}
			originalWeight := nativeWeight()
			if originalWeight == 0 {
				t.Fatalf("native CRUSH item %s has no initial weight", osdName)
			}
			restoreWeight := strconv.FormatFloat(float64(originalWeight)/(1<<16), 'f', -1, 64)
			t.Logf("native %s CRUSH weight=%d/65536 (%s); restore exact original after zero-weight fault", osdName, originalWeight, restoreWeight)
			cephCommand(t, ctx, cluster, "osd", "crush", "reweight", osdName, "0")
			if weight := nativeWeight(); weight != 0 {
				t.Fatalf("zero-weight native fault was not applied: %s weight=%d", osdName, weight)
			}
			if err := cluster.SetPoolReplication(ctx, "tc-policy", 3, 2); err == nil {
				t.Fatal("zero-weight native placement accepted three replicas")
			}
			unchanged, err := cluster.PoolStatus(ctx, "tc-policy")
			if err != nil || unchanged.ID != initial.ID || unchanged.Size != 2 || unchanged.MinSize != 1 {
				t.Fatalf("rejected replication still changed policy: %+v %v", unchanged, err)
			}
			cephCommand(t, ctx, cluster, "osd", "crush", "reweight", osdName, restoreWeight)
			if weight := nativeWeight(); weight != originalWeight {
				t.Fatalf("native CRUSH restoration differs: %s before=%d after=%d", osdName, originalWeight, weight)
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			if err := cluster.SetPoolQuota(ctx, "tc-policy", ceph.PoolQuota{MaxBytes: 1, MaxObjects: 1}); err != nil {
				t.Fatal(err)
			}
			waitFull := func(full bool) {
				t.Helper()
				deadline := time.Now().Add(90 * time.Second)
				for {
					state, err := cluster.PoolStatus(ctx, "tc-policy")
					if err != nil || state.ID != initial.ID {
						t.Fatalf("pool identity changed while observing quota: %+v error=%v", state, err)
					}
					if slices.Contains(strings.Split(state.Flags, ","), "full") == full {
						return
					}
					if time.Now().After(deadline) {
						t.Fatalf("pool full=%v not observed: %+v", full, state)
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(time.Second):
					}
				}
			}
			waitFull(true)
			state, err := cluster.PoolStatus(ctx, "tc-policy")
			if err != nil || state.ID != initial.ID || state.Quota.MaxBytes != 1 || state.Quota.MaxObjects != 1 {
				t.Fatalf("quota not native: %+v %v", state, err)
			}
			probe("full")
			if err := cluster.SetPoolQuota(ctx, "tc-policy", ceph.PoolQuota{}); err != nil {
				t.Fatal(err)
			}
			waitFull(false)
			probe("resumed")
			final, err := cluster.PoolStatus(ctx, "tc-policy")
			if err != nil || final.ID != initial.ID || final.Quota != (ceph.PoolQuota{}) || final.Size != 2 || final.MinSize != 1 {
				t.Fatalf("restored quota changed native pool identity/policy: %+v error=%v", final, err)
			}
			state, err = cluster.PoolStatus(ctx, "tc-unaffected")
			if err != nil || state != other {
				t.Fatalf("unrelated policy changed: %+v %v", state, err)
			}
			pools, err := cluster.Pools(ctx)
			if err != nil || len(pools) < 2 {
				t.Fatal("pool listing unavailable", err)
			}
			t.Logf("native RADOS: replicas 2→3→2 and quota denial/recovery retain positive native pool ID %d→%d and unrelated pool ID=%d; bytes/CRUSH preserved; zero-weight placement refused; quota restored", initial.ID, final.ID, other.ID)
		})
	}
}

const poolPolicyProbe = `import rados,sys
phase=sys.argv[1]
payload=bytes(range(256))*256
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.conf_set("rados_osd_op_timeout","3")
c.connect(timeout=10)
try:
    with c.open_ioctx("tc-policy") as io:
        if phase=="seed": io.write_full("shared",payload)
        assert io.read("shared",len(payload))==payload
        if phase=="full":
            try: io.write_full("quota-denied",payload)
            except rados.Error: pass
            else: raise AssertionError("full pool accepted write")
        elif phase=="resumed":
            io.write_full("resumed",payload[::-1])
            assert io.read("resumed",len(payload))==payload[::-1]
    with c.open_ioctx("tc-unaffected") as io:
        io.write_full("unaffected",payload)
        assert io.read("unaffected",len(payload))==payload
    print("pool policy native phase passed:",phase)
finally: c.shutdown()
`
