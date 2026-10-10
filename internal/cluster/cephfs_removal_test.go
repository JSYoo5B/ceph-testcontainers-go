package cluster

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"

	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// fsRemovalFixture drops the filesystem from fs ls once fs rm succeeds.
type fsRemovalFixture struct{ *poolFixtureContainer }

func (ctr fsRemovalFixture) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	code, reader, err := ctr.poolFixtureContainer.Exec(ctx, args, opts...)
	if code == 0 && err == nil && slices.Contains(args, "fs") && slices.Contains(args, "rm") {
		ctr.output["fs ls --format json"] = "[]"
	}
	return code, reader, err
}

const removalFSPools = `[{"pool_id":1,"pool_name":".mgr","type":1,"size":2,"min_size":1,"pg_num":1,"crush_rule":0},` +
	`{"pool_id":3,"pool_name":"app-metadata","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":1},` +
	`{"pool_id":4,"pool_name":"app-data","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":2},` +
	`{"pool_id":5,"pool_name":"attached","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":3}]`

const removalFSRules = `[{"rule_id":0,"rule_name":"replicated_rule"},{"rule_id":1,"rule_name":"tc-app-metadata-replicated"},` +
	`{"rule_id":2,"rule_name":"tc-app-data-replicated"},{"rule_id":3,"rule_name":"tc-attached-replicated"}]`

func cephFSRemovalCluster(t *testing.T, fsList string) (*Container, *poolFixtureContainer) {
	t.Helper()
	ctr := removalFixture(map[string]string{
		"fs ls --format json":               fsList,
		"osd pool ls detail --format json":  removalFSPools,
		"osd crush rule dump --format json": removalFSRules,
	})
	cluster := poolFixtureCluster(fsRemovalFixture{ctr}, 3)
	config, err := normalizeCephFSConfig(CephFSConfig{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.filesystems = map[string]*CephFSContainer{"app": {FilesystemName: "app", MetadataPool: "app-metadata", DataPool: "app-data", cluster: cluster, config: config}}
	return cluster, ctr
}

func TestRemoveCephFSRemovesFilesystemAndConfiguredPools(t *testing.T) {
	cluster, ctr := cephFSRemovalCluster(t, `[{"name":"app","metadata_pool":"app-metadata","data_pools":["app-data","attached"]}]`)
	if err := RemoveCephFS(t.Context(), cluster, "app"); err != nil {
		t.Fatal(err)
	}
	var fsCommands, removed []string
	for _, call := range ctr.calls {
		command := strings.Join(call, " ")
		if strings.HasPrefix(command, "fs fail") || strings.HasPrefix(command, "fs rm") {
			fsCommands = append(fsCommands, command)
		}
		if strings.HasPrefix(command, "osd pool rm") {
			removed = append(removed, call[3])
		}
	}
	if want := []string{"fs fail app", "fs rm app --yes-i-really-mean-it"}; !slices.Equal(fsCommands, want) {
		t.Fatalf("filesystem commands = %q, want %q", fsCommands, want)
	}
	if want := []string{"app-metadata", "app-data"}; !slices.Equal(removed, want) {
		t.Fatalf("removed pools = %q, want only configured pools %q", removed, want)
	}
	if cluster.filesystems["app"] != nil {
		t.Fatal("removed filesystem is still owned")
	}
	if err := RemoveCephFS(t.Context(), cluster, "app"); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("second removal error = %v", err)
	}
}

func TestRemoveCephFSRetryContinuesAfterNativeRemoval(t *testing.T) {
	cluster, ctr := cephFSRemovalCluster(t, `[]`)
	ctr.output["osd pool ls detail --format json"] = `[{"pool_id":1,"pool_name":".mgr","type":1,"size":2,"min_size":1,"pg_num":1,"crush_rule":0},` +
		`{"pool_id":4,"pool_name":"app-data","type":1,"size":2,"min_size":1,"pg_num":8,"crush_rule":2}]`
	if err := RemoveCephFS(t.Context(), cluster, "app"); err != nil {
		t.Fatal(err)
	}
	for _, call := range ctr.calls {
		if call[0] == "fs" && (call[1] == "fail" || call[1] == "rm") {
			t.Fatalf("absent filesystem was removed again: %v", call)
		}
	}
	if !slices.ContainsFunc(ctr.calls, func(call []string) bool { return strings.Join(call, " ") == "osd pool rm app-data app-data --yes-i-really-really-mean-it" }) {
		t.Fatalf("remaining configured pool was not removed: %v", ctr.calls)
	}
	if cluster.filesystems["app"] != nil {
		t.Fatal("retried filesystem is still owned")
	}
}

func TestRemoveCephFSKeepsFilesystemWhenNativeRemovalFails(t *testing.T) {
	cluster, ctr := cephFSRemovalCluster(t, `[{"name":"app","metadata_pool":"app-metadata","data_pools":["app-data"]}]`)
	ctr.fail = "fs rm app --yes-i-really-mean-it"
	if err := RemoveCephFS(t.Context(), cluster, "app"); err == nil || !strings.Contains(err.Error(), `remove cephfs "app"`) {
		t.Fatalf("RemoveCephFS error = %v", err)
	}
	if cluster.filesystems["app"] == nil {
		t.Fatal("failed removal dropped ownership, so a retry is impossible")
	}
	if slices.ContainsFunc(ctr.calls, func(call []string) bool { return call[0] == "osd" && len(call) > 2 && call[2] == "rm" }) {
		t.Fatal("pools were removed although the filesystem still exists")
	}
	var missing *Container
	if err := RemoveCephFS(t.Context(), missing, "app"); err == nil {
		t.Fatal("nil cluster accepted")
	}
}
