//go:build all || (integration && topology && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data_layout))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func TestCephFSAdditionalErasureCodedDataPool(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, ceph.WithOSDCount(3))
	const ecPool = "ecfs-file-data"
	fs, err := cephfs.Start(ctx, cluster, cephfs.Config{Name: "ecfs", AdditionalDataPools: []ceph.PoolConfig{{
		Name: ecPool, PGNum: 8, MinSize: 3, ErasureCode: &ceph.ErasureCodeConfig{K: 2, M: 1, AllowOverwrites: true},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "ec-data", "seed", 0, ecPool)
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "ec-data", "after-failure", 0, ecPool)
	cephFSTopologyIO(t, ctx, client, fs.FilesystemName, "ec-data", "final", 0, ecPool)
	// Layout selection alone is insufficient evidence: the selected pool must
	// contain actual CephFS objects and retain its native erasure configuration.
	output, err := cluster.Ceph(ctx, "osd", "pool", "get", ecPool, "all", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var settings struct {
		Size              int  `json:"size"`
		AllowECOverwrites bool `json:"allow_ec_overwrites"`
		MinSize           int  `json:"min_size"`
	}
	if err := json.Unmarshal(output, &settings); err != nil || settings.Size != 3 || !settings.AllowECOverwrites || settings.MinSize != 3 {
		t.Fatalf("CephFS data pool is not EC k2+m1 with overwrites: output=%s error=%v", output, err)
	}
	output, err = cluster.Ceph(ctx, "osd", "erasure-code-profile", "get", "tc-"+ecPool+"-ec", "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var profile map[string]string
	if err := json.Unmarshal(output, &profile); err != nil || profile["k"] != "2" || profile["m"] != "1" || profile["plugin"] != "jerasure" {
		t.Fatalf("CephFS file data uses an unexpected EC profile: output=%s error=%v", output, err)
	}
	code, reader, err := client.Exec(ctx, []string{"rados", "-p", ecPool, "ls"}, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	objects, err := io.ReadAll(reader)
	if err != nil || code != 0 || strings.TrimSpace(string(objects)) == "" {
		t.Fatalf("EC pool contains no CephFS file data: exit=%d objects=%s error=%v", code, objects, err)
	}
	t.Logf("replicated CephFS metadata/default data plus additional EC2+1 data pool; file layout=%s, actual objects=%s; partial overwrite/fsync and fresh-session byte proof", ecPool, strings.TrimSpace(string(objects)))
}
