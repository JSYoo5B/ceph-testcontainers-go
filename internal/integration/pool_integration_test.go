//go:build integration

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

const erasurePoolClientScript = `import rados, rbd, sys
metadata_pool, data_pool, phase = sys.argv[1:]
original = bytes(range(256)) * 512
patch = b"unaligned-erasure-code-overwrite" * 7
patch_offset = 4093
expected = original[:patch_offset] + patch + original[patch_offset + len(patch):]
image_offset = (4 << 20) - 8191
object_name, image_name, snapshot = "tc-ec-object", "tc-ec-image", "baseline"
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "30", "rados_osd_op_timeout": "30"}) as cluster:
    with cluster.open_ioctx(data_pool) as ioctx:
        if phase == "seed":
            ioctx.write_full(object_name, original)
            ioctx.write(object_name, patch, patch_offset)
        assert ioctx.read(object_name, len(expected) + 1) == expected, "EC RADOS byte mismatch"
        if phase == "verify":
            ioctx.remove_object(object_name)
            try:
                ioctx.stat(object_name)
            except rados.ObjectNotFound:
                pass
            else:
                raise AssertionError("deleted EC RADOS object remains")
    with cluster.open_ioctx(metadata_pool) as ioctx:
        api = rbd.RBD()
        if phase == "seed":
            api.create(ioctx, image_name, 8 << 20, old_format=False,
                       features=rbd.RBD_FEATURE_LAYERING, data_pool=data_pool)
        with rbd.Image(ioctx, image_name) as image:
            assert image.data_pool_id() == cluster.pool_lookup(data_pool), "RBD data did not select EC pool"
            if phase == "seed":
                assert image.write(original, image_offset) == len(original)
                image.flush()
                image.create_snap(snapshot)
                assert image.write(patch, image_offset + patch_offset) == len(patch)
                image.flush()
            assert image.read(image_offset, len(expected)) == expected, "EC RBD head byte mismatch"
            image.set_snap(snapshot)
            assert image.read(image_offset, len(original)) == original, "EC RBD snapshot byte mismatch"
            image.set_snap(None)
            if phase == "verify":
                image.remove_snap(snapshot)
        if phase == "verify":
            api.remove(ioctx, image_name)
            assert image_name not in api.list(ioctx), "deleted EC RBD image remains"
`
