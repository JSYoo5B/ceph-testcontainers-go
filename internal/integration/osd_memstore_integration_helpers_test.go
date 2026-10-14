//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
)

// legacyCRUSHMap is a straw-bucket map with legacy tunables and a rule that
// chooses OSDs directly, for clients that implement only the oldest CRUSH
// feature set.
const legacyCRUSHMap = `tunable choose_local_tries 2
tunable choose_local_fallback_tries 5
tunable choose_total_tries 19
tunable chooseleaf_descend_once 0
tunable chooseleaf_vary_r 0
tunable chooseleaf_stable 0
tunable straw_calc_version 0
device 0 osd.0
type 0 osd
type 10 root
root default {
    id -1
    alg straw
    hash 0
    item osd.0 weight 1.00000
}
rule replicated_rule {
    id 0
    type replicated
    step take default
    step choose firstn 0 type osd
    step emit
}
`

// installCRUSHMap compiles a text CRUSH map in the control container and
// installs it, after keeping OSDs from moving themselves or setting their
// device class when they start.
func installCRUSHMap(t *testing.T, ctx context.Context, cluster *ceph.Container, text string) {
	t.Helper()
	for _, name := range []string{"osd_crush_update_on_start", "osd_class_update_on_start"} {
		if _, err := cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "osd", Name: name, Value: "false"}); err != nil {
			t.Fatal(err)
		}
	}
	control := cluster.ControlContainer()
	if err := control.CopyToContainer(ctx, []byte(text), "/tmp/tc-crush.txt", 0o644); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, control, "crushtool", "-c", "/tmp/tc-crush.txt", "-o", "/tmp/tc-crush.bin")
	if _, err := cluster.Ceph(ctx, "osd", "setcrushmap", "-i", "/tmp/tc-crush.bin"); err != nil {
		t.Fatal(err)
	}
}

// testOSDMemStoreLegacyPlacement reproduces a single MemStore OSD with a
// legacy CRUSH map and a one-PG, size-one pool without hashpspool.
func testOSDMemStoreLegacyPlacement(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithOSDCount(1), ceph.WithPoolDefaults(1, 1),
		ceph.WithOSDObjectStore(ceph.OSDMemStore), ceph.WithOSDBlockSize(256 << 20)}, opts...)
	cluster, client := newServiceCluster(t, opts...)

	installCRUSHMap(t, ctx, cluster, legacyCRUSHMap)
	assertLegacyCRUSHMap(t, ctx, cluster)

	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: "tc-legacy", PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: true, Application: "rados"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	checkMemStoreLegacyPool(t, ctx, cluster, pool.Name)

	// The client writes and reads through the single PG on osd.0.
	execCommand(t, ctx, client, "sh", "-c", "head -c 5000 /dev/urandom > /tmp/object && rados -p tc-legacy put legacy /tmp/object && rados -p tc-legacy get legacy /tmp/read && cmp /tmp/object /tmp/read")
	out, err := cluster.Ceph(ctx, "osd", "map", "tc-legacy", "legacy", "--format", "json")
	var mapping struct {
		PGID   string `json:"pgid"`
		Acting []int  `json:"acting"`
	}
	if err != nil || json.Unmarshal(out, &mapping) != nil || !strings.HasSuffix(mapping.PGID, ".0") || len(mapping.Acting) != 1 || mapping.Acting[0] != 0 {
		t.Fatalf("object mapping %q, err=%v", out, err)
	}

	// A clean restart saves and loads the MemStore, and the OSD does not move
	// itself back into a host bucket.
	osd := cluster.OSDs()[0]
	stop := 20 * time.Second
	if err := osd.Stop(ctx, &stop); err != nil {
		t.Fatal(err)
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "sh", "-c", "rados -p tc-legacy get legacy /tmp/after && cmp /tmp/object /tmp/after")
	assertLegacyCRUSHMap(t, ctx, cluster)
	t.Logf("MemStore osd.0 with the legacy CRUSH map served pool %s (1 PG %s, size 1, no hashpspool) before and after a clean restart", pool.Name, mapping.PGID)
}

// checkMemStoreLegacyPool checks that osd.0 runs MemStore and that pool has
// one PG, size 1, min_size 1 and no hashpspool flag.
func checkMemStoreLegacyPool(t *testing.T, ctx context.Context, cluster *ceph.Container, pool string) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "metadata", "0", "--format", "json")
	var metadata struct {
		ObjectStore string `json:"osd_objectstore"`
	}
	if err != nil || json.Unmarshal(out, &metadata) != nil || metadata.ObjectStore != "memstore" {
		t.Fatalf("osd.0 metadata %q, err=%v", out, err)
	}
	pools, err := cluster.Pools(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, state := range pools {
		if state.Name != pool {
			continue
		}
		found = true
		if state.Size != 1 || state.MinSize != 1 || state.PGNum != 1 || strings.Contains(","+state.Flags+",", ",hashpspool,") {
			t.Fatalf("pool %+v keeps hashpspool or differs from size 1, PG 1", state)
		}
	}
	if !found {
		t.Fatalf("pool %s not found", pool)
	}
}

func assertLegacyCRUSHMap(t *testing.T, ctx context.Context, cluster *ceph.Container) {
	t.Helper()
	out, err := cluster.Ceph(ctx, "osd", "crush", "dump", "--format", "json")
	var dump struct {
		Tunables map[string]any `json:"tunables"`
		Buckets  []struct {
			Name string `json:"name"`
			Alg  string `json:"alg"`
		} `json:"buckets"`
	}
	if err != nil || json.Unmarshal(out, &dump) != nil {
		t.Fatalf("crush dump %q, err=%v", out, err)
	}
	for name, want := range map[string]float64{"choose_local_tries": 2, "choose_local_fallback_tries": 5, "choose_total_tries": 19,
		"chooseleaf_descend_once": 0, "chooseleaf_vary_r": 0, "chooseleaf_stable": 0, "straw_calc_version": 0} {
		if got, ok := dump.Tunables[name].(float64); !ok || got != want {
			t.Fatalf("tunable %s = %v, want %v", name, dump.Tunables[name], want)
		}
	}
	if len(dump.Buckets) != 1 || dump.Buckets[0].Name != "default" || dump.Buckets[0].Alg != "straw" {
		t.Fatalf("buckets %+v, want only the straw root default", dump.Buckets)
	}
}
