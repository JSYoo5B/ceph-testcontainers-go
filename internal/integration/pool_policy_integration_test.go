//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"slices"
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
			if err != nil {
				t.Fatal(err)
			}
			other, err := cluster.PoolStatus(ctx, "tc-unaffected")
			if err != nil {
				t.Fatal(err)
			}
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
			osdName := fmt.Sprintf("osd.%d", cluster.OSDs()[2].ID)
			cephCommand(t, ctx, cluster, "osd", "crush", "reweight", osdName, "0")
			if err := cluster.SetPoolReplication(ctx, "tc-policy", 3, 2); err == nil {
				t.Fatal("zero-weight native placement accepted three replicas")
			}
			unchanged, err := cluster.PoolStatus(ctx, "tc-policy")
			if err != nil || unchanged.Size != 2 || unchanged.MinSize != 1 {
				t.Fatalf("rejected replication still changed policy: %+v %v", unchanged, err)
			}
			cephCommand(t, ctx, cluster, "osd", "crush", "reweight", osdName, "1")
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
					if err != nil {
						t.Fatal(err)
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
			if err != nil || state.Quota.MaxBytes != 1 || state.Quota.MaxObjects != 1 {
				t.Fatalf("quota not native: %+v %v", state, err)
			}
			probe("full")
			if err := cluster.SetPoolQuota(ctx, "tc-policy", ceph.PoolQuota{}); err != nil {
				t.Fatal(err)
			}
			waitFull(false)
			probe("resumed")
			state, err = cluster.PoolStatus(ctx, "tc-unaffected")
			if err != nil || state != other {
				t.Fatalf("unrelated policy changed: %+v %v", state, err)
			}
			pools, err := cluster.Pools(ctx)
			if err != nil || len(pools) < 2 {
				t.Fatal("pool listing unavailable", err)
			}
			t.Log("native RADOS: replicas 2→3→2 preserve bytes/IDs/CRUSH, native zero-weight placement rejects infeasible replicas, reported full quota blocks writes, clearing quota resumes writes; other pool unaffected")
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
