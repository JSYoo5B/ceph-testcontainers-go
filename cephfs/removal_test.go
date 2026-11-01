package cephfs

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// cephFSRemovalCluster owns filesystem "app" whose configuration created
// app-metadata and app-data; "attached" stands for a pool added later.
func cephFSRemovalCluster(t *testing.T, present bool) (*ceph.Container, *cephStateFake) {
	return cephFSRemovalClusterWithControl(t, present, nil)
}

func cephFSRemovalClusterWithControl(t *testing.T, present bool, wrap func(*cephStateFake) *shutdownSequenceCLI) (*ceph.Container, *cephStateFake) {
	t.Helper()
	f := newCephStateFake()
	for _, name := range []string{"app-metadata", "app-data", "attached"} {
		f.AddFixturePool(name, false)
	}
	if present {
		f.Filesystems = []clustertest.FakeFilesystem{{Name: "app", Metadata: "app-metadata", Data: []string{"app-data", "attached"}}}
	}
	f.Output["fs get app --format json"] = shutdownMap(41, 0, `{}`)
	var control testcontainers.Container = f
	if wrap != nil {
		control = wrap(f)
	}
	cluster := poolFixtureCluster(control, 3)
	config, err := normalizeCephFSConfig(Config{Name: "app"})
	if err != nil {
		t.Fatal(err)
	}
	filesystems(cluster).byName = map[string]*Filesystem{"app": newFilesystem(cluster, config, nil)}
	return cluster, f
}

// shutdownSequenceCLI returns scripted native maps while the real removal
// implementation controls ordering and the existing model owns pool cleanup.
type shutdownSequenceCLI struct {
	*cephStateFake
	maps   []string
	reads  int
	onRead func(int)
	onDown func()
}

func (f *shutdownSequenceCLI) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	command := strings.Join(clustertest.ModuleArgs(args), " ")
	if command == "fs get app --format json" && len(f.maps) != 0 {
		index := min(f.reads, len(f.maps)-1)
		f.Output[command] = f.maps[index]
		f.reads++
		if f.onRead != nil {
			f.onRead(f.reads)
		}
	}
	code, reader, err := f.cephStateFake.Exec(ctx, args, opts...)
	if command == "fs set app down true" && code == 0 && err == nil && f.onDown != nil {
		f.onDown()
	}
	return code, reader, err
}

// ownedFilesystem returns the descriptor that the cluster owns under name now.
func ownedFilesystem(t *testing.T, cluster *ceph.Container, name string) *Filesystem {
	t.Helper()
	fs := filesystems(cluster).byName[name]
	if fs == nil {
		t.Fatalf("the cluster owns no filesystem %q", name)
	}
	return fs
}

func TestRemoveRemovesFilesystemAndConfiguredPools(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(t.Context()); err != nil {
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
	if err := app.Remove(t.Context()); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("second removal error = %v", err)
	}
}

func TestRemoveRefusesAStaleDescriptorWithoutReachingASuccessor(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	old := ownedFilesystem(t, cluster, "app")
	if err := old.Remove(t.Context()); err != nil {
		t.Fatal(err)
	}
	// A later filesystem of the same name is another filesystem.
	successor := newFilesystem(cluster, old.config, nil)
	filesystems(cluster).byName["app"] = successor
	f.Filesystems = []clustertest.FakeFilesystem{{Name: "app", Metadata: "app-metadata", Data: []string{"app-data"}}}
	before := len(f.Filesystems)
	if err := old.Remove(t.Context()); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale removal error = %v", err)
	}
	if filesystems(cluster).byName["app"] != successor || len(f.Filesystems) != before {
		t.Fatal("a stale descriptor removed the filesystem that has its name now")
	}
}

func TestRemoveRetryContinuesAfterNativeRemoval(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.FailNext("osd pool rm app-data app-data --yes-i-really-really-mean-it", 1)
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(t.Context()); err == nil {
		t.Fatal("injected pool removal failure was not reported")
	}
	if len(f.Filesystems) != 0 || !f.HasPool("app-data") || filesystems(cluster).byName["app"] == nil {
		t.Fatalf("unexpected state after partial removal: %s", f)
	}
	if err := app.Remove(t.Context()); err != nil {
		t.Fatal("retry:", err)
	}
	if f.HasPool("app-metadata") || f.HasPool("app-data") || !f.HasPool("attached") || filesystems(cluster).byName["app"] != nil {
		t.Fatalf("retry left %s", f)
	}
}

func TestRemoveKeepsFilesystemWhenNativeRemovalFails(t *testing.T) {
	cluster, f := cephFSRemovalCluster(t, true)
	f.FailNext("fs rm app --yes-i-really-mean-it", 1)
	app := ownedFilesystem(t, cluster, "app")
	if err := app.Remove(t.Context()); err == nil || !strings.Contains(err.Error(), `remove cephfs "app"`) {
		t.Fatalf("Remove error = %v", err)
	}
	if filesystems(cluster).byName["app"] == nil || len(f.Filesystems) != 1 {
		t.Fatal("failed removal dropped the filesystem or its ownership")
	}
	if !f.HasPool("app-metadata") || !f.HasPool("app-data") {
		t.Fatal("pools were removed although the filesystem still exists")
	}
	if err := (*Filesystem)(nil).Remove(t.Context()); err == nil {
		t.Fatal("nil cluster accepted")
	}
}
