//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_rados_fixtures))))

//ci: timeout=120m job-timeout=130

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// These are client operations. The fixture supplies an ordinary authenticated
// replicated pool and native OSD classes; it does not duplicate librados CRUD.
func TestRADOSClientFixtures(t *testing.T) {
	parallelWhenEnabled(t)
	for _, host := range []bool{false, true} {
		name := "bridge"
		if host {
			name = "host"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
			defer cancel()
			opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
			if host {
				opts = append(opts, ceph.WithHostNetwork())
			}
			cluster, client := newServiceCluster(t, opts...)
			const pool = "tc-rados-fixtures"
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", PGNum: 1}); err != nil {
				t.Fatal(err)
			}
			state, err := cluster.PoolStatus(ctx, pool)
			if err != nil || state.ID <= 0 {
				t.Fatalf("native detail pool identity: %+v %v", state, err)
			}
			// osd dump uses pool, whereas pool ls detail uses pool_id. Compare
			// independent native forms so a silently zero ID cannot pass.
			var native struct {
				Pools []struct {
					ID   int64  `json:"pool"`
					Name string `json:"pool_name"`
				} `json:"pools"`
			}
			data, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			if err != nil || json.Unmarshal(data, &native) != nil {
				t.Fatal("native OSDMap comparison failed", err)
			}
			found := false
			for _, entry := range native.Pools {
				if entry.Name == pool {
					found = entry.ID == state.ID
				}
			}
			if !found {
				t.Fatal("public PoolStatus identity differs from native OSDMap")
			}
			if err := cluster.WaitForClean(ctx); err != nil {
				t.Fatal(err)
			}
			outer := `import subprocess,sys; subprocess.run(["python3","-c",sys.argv[1]],check=True,timeout=100)`
			proof := fencingExec(t, ctx, client, []string{"python3", "-c", outer, radosClientFixtureProbe})
			t.Log(string(proof))
		})
	}
}
