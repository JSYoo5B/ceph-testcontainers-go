package cluster

import (
	"strings"
	"testing"
)

// cephFSRemovalCluster owns filesystem "app" whose configuration created
// app-metadata and app-data; "attached" stands for a pool added later.
func cephFSRemovalCluster(t *testing.T, present bool) (*Container, *cephStateFake) {
	t.Helper()
	f := newCephStateFake()
	for _, name := range []string{"app-metadata", "app-data", "attached"} {
		f.addFixturePool(name, false)
	}
	if present {
		f.filesystems = []fakeFilesystem{{Name: "app", Metadata: "app-metadata", Data: []string{"app-data", "attached"}}}
	}
	cluster := poolFixtureCluster(f, 3)
	config, err := normalizeCephFSConfig(CephFSConfig{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.filesystems = map[string]*CephFSContainer{"app": {FilesystemName: "app", MetadataPool: "app-metadata", DataPool: "app-data", cluster: cluster, config: config}}
	return cluster, f
}

func TestRemoveCephFSRemovesFilesystemAndConfiguredPools(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	if err := RemoveCephFS(t.Context(), cluster, "app"); err != nil {
		t.Fatal(err)
	}
	if len(f.filesystems) != 0 || f.hasPool("app-metadata") || f.hasPool("app-data") {
		t.Fatalf("filesystem or configured pools remain: %s", f)
	}
	if !f.hasPool("attached") {
		t.Fatal("a later attached pool was removed")
	}
	if cluster.filesystems["app"] != nil {
		t.Fatal("removed filesystem is still owned")
	}
	if err := RemoveCephFS(t.Context(), cluster, "app"); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("second removal error = %v", err)
	}
}

func TestRemoveCephFSRetryContinuesAfterNativeRemoval(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.failNext("osd pool rm app-data app-data --yes-i-really-really-mean-it", 1)
	if err := RemoveCephFS(t.Context(), cluster, "app"); err == nil {
		t.Fatal("injected pool removal failure was not reported")
	}
	if len(f.filesystems) != 0 || !f.hasPool("app-data") || cluster.filesystems["app"] == nil {
		t.Fatalf("unexpected state after partial removal: %s", f)
	}
	if err := RemoveCephFS(t.Context(), cluster, "app"); err != nil {
		t.Fatal("retry:", err)
	}
	if f.hasPool("app-metadata") || f.hasPool("app-data") || !f.hasPool("attached") || cluster.filesystems["app"] != nil {
		t.Fatalf("retry left %s", f)
	}
}

func TestRemoveCephFSKeepsFilesystemWhenNativeRemovalFails(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.failNext("fs rm app --yes-i-really-mean-it", 1)
	if err := RemoveCephFS(t.Context(), cluster, "app"); err == nil || !strings.Contains(err.Error(), `remove cephfs "app"`) {
		t.Fatalf("RemoveCephFS error = %v", err)
	}
	if cluster.filesystems["app"] == nil || len(f.filesystems) != 1 {
		t.Fatal("failed removal dropped the filesystem or its ownership")
	}
	if !f.hasPool("app-metadata") || !f.hasPool("app-data") {
		t.Fatal("pools were removed although the filesystem still exists")
	}
	var missing *Container
	if err := RemoveCephFS(t.Context(), missing, "app"); err == nil {
		t.Fatal("nil cluster accepted")
	}
}
