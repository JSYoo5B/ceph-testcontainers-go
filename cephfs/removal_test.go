package cephfs

import (
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// cephFSRemovalCluster owns filesystem "app" whose configuration created
// app-metadata and app-data; "attached" stands for a pool added later.
func cephFSRemovalCluster(t *testing.T, present bool) (*ceph.Container, *cephStateFake) {
	t.Helper()
	f := newCephStateFake()
	for _, name := range []string{"app-metadata", "app-data", "attached"} {
		f.AddFixturePool(name, false)
	}
	if present {
		f.Filesystems = []clustertest.FakeFilesystem{{Name: "app", Metadata: "app-metadata", Data: []string{"app-data", "attached"}}}
	}
	cluster := poolFixtureCluster(f, 3)
	config, err := normalizeCephFSConfig(Config{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	filesystems(cluster).byName = map[string]*Filesystem{"app": {FilesystemName: "app", MetadataPool: "app-metadata", DataPool: "app-data", cluster: cluster, config: config}}
	return cluster, f
}

func TestRemoveRemovesFilesystemAndConfiguredPools(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	if err := Remove(t.Context(), cluster, "app"); err != nil {
		t.Fatal(err)
	}
	if len(f.Filesystems) != 0 || f.HasPool("app-metadata") || f.HasPool("app-data") {
		t.Fatalf("filesystem or configured pools remain: %s", f)
	}
	if !f.HasPool("attached") {
		t.Fatal("a later attached pool was removed")
	}
	if filesystems(cluster).byName["app"] != nil {
		t.Fatal("removed filesystem is still owned")
	}
	if err := Remove(t.Context(), cluster, "app"); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("second removal error = %v", err)
	}
}

func TestRemoveRetryContinuesAfterNativeRemoval(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.FailNext("osd pool rm app-data app-data --yes-i-really-really-mean-it", 1)
	if err := Remove(t.Context(), cluster, "app"); err == nil {
		t.Fatal("injected pool removal failure was not reported")
	}
	if len(f.Filesystems) != 0 || !f.HasPool("app-data") || filesystems(cluster).byName["app"] == nil {
		t.Fatalf("unexpected state after partial removal: %s", f)
	}
	if err := Remove(t.Context(), cluster, "app"); err != nil {
		t.Fatal("retry:", err)
	}
	if f.HasPool("app-metadata") || f.HasPool("app-data") || !f.HasPool("attached") || filesystems(cluster).byName["app"] != nil {
		t.Fatalf("retry left %s", f)
	}
}

func TestRemoveKeepsFilesystemWhenNativeRemovalFails(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.FailNext("fs rm app --yes-i-really-mean-it", 1)
	if err := Remove(t.Context(), cluster, "app"); err == nil || !strings.Contains(err.Error(), `remove cephfs "app"`) {
		t.Fatalf("Remove error = %v", err)
	}
	if filesystems(cluster).byName["app"] == nil || len(f.Filesystems) != 1 {
		t.Fatal("failed removal dropped the filesystem or its ownership")
	}
	if !f.HasPool("app-metadata") || !f.HasPool("app-data") {
		t.Fatal("pools were removed although the filesystem still exists")
	}
	var missing *ceph.Container
	if err := Remove(t.Context(), missing, "app"); err == nil {
		t.Fatal("nil cluster accepted")
	}
}
