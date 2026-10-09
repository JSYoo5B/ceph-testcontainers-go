//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_default))))

//ci: timeout=20m job-timeout=35

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func TestErasureCodedPools(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(3))
	metadata, err := cluster.CreatePool(ctx, ceph.PoolConfig{
		Name: "tc-rbd-metadata", PGNum: 8, Replicas: 2, MinSize: 1, Application: "rbd",
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := cluster.CreatePool(ctx, ceph.PoolConfig{
		Name: "tc-ec-data", PGNum: 8, MinSize: 3, Application: "rbd",
		ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	for _, pool := range []*ceph.Pool{metadata, data} {
		output, err := cluster.Ceph(ctx, "osd", "pool", "get", pool.Name, "all", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var settings struct {
			Size              int    `json:"size"`
			MinSize           int    `json:"min_size"`
			PGNum             int    `json:"pg_num"`
			PGAutoscaleMode   string `json:"pg_autoscale_mode"`
			CRUSHRule         string `json:"crush_rule"`
			AllowECOverwrites bool   `json:"allow_ec_overwrites"`
		}
		if err := json.Unmarshal(output, &settings); err != nil {
			t.Fatal(err)
		}
		expectedSize := pool.Replicas
		if pool.ErasureCode != nil {
			expectedSize = pool.ErasureCode.K + pool.ErasureCode.M
		}
		if settings.Size != expectedSize || settings.MinSize != pool.MinSize || settings.PGNum != 8 || settings.PGAutoscaleMode != "off" || settings.CRUSHRule != pool.CRUSHRule {
			t.Fatalf("pool %s lost requested layout: %s", pool.Name, output)
		}
		if pool.ErasureCode != nil && !settings.AllowECOverwrites {
			t.Fatal("EC pool did not enable partial overwrites")
		}
		output, err = cluster.Ceph(ctx, "osd", "pool", "application", "get", pool.Name, "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var applications map[string]json.RawMessage
		if err := json.Unmarshal(output, &applications); err != nil || applications["rbd"] == nil {
			t.Fatalf("pool %s lacks the RBD application: %s error=%v", pool.Name, output, err)
		}
	}
	output, err := cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", data.ErasureCodeProfile, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var profile map[string]string
	if err := json.Unmarshal(output, &profile); err != nil {
		t.Fatal(err)
	}
	if profile["k"] != "2" || profile["m"] != "1" || profile["plugin"] != "jerasure" || profile["crush-failure-domain"] != "osd" {
		t.Fatalf("EC profile does not match the file-backed fixture: %s", output)
	}
	// Both processes use native librados/librbd. A fresh process reads the
	// retained bytes and snapshot through a new session, then deletes them.
	for _, phase := range []string{"seed", "verify"} {
		execCommand(t, ctx, client, "python3", "-c", erasurePoolClientScript, metadata.Name, data.Name, phase)
	}
	t.Log("native EC RADOS partial overwrites and RBD with replicated metadata/EC data: bytes and snapshot retained in a fresh session, then removed")
}
