//go:build all || (integration && multicluster)

package cephfs_test

import (
	"context"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

const cephFSWatchScript = "/tmp/cephfs-intercluster.py"

func runCephFSMirrorDirectoryWatch(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t)
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expected := harness.CephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(harness.CephFSInterClusterScript), cephFSWatchScript, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
		t.Helper()
		output := harness.MultiClusterExecOutput(t, ctx, client, append([]string{"python3", cephFSWatchScript, filesystem}, args...)...)
		if summary := strings.TrimSpace(string(output)); summary != "" {
			t.Logf("CephFS %s: %s", args[0], summary)
		}
	}
	// checkpoint writes new bytes and takes a source snapshot, reads its
	// source ID independently and hands the archive of the snapshot to the
	// destination client for the byte check.
	checkpoint := func(label, name string) cephfsmirror.Snapshot {
		t.Helper()
		fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", label, name)
		snapshot := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, cephFSWatchScript, "/federation", name)
		archivePath := "/tmp/cephfs-" + name + ".json"
		fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/"+name, archivePath)
		if err := destinationClient.CopyToContainer(ctx, harness.MultiClusterReadFile(t, ctx, sourceClient, archivePath), archivePath, 0o600); err != nil {
			t.Fatal(err)
		}
		return snapshot
	}
	verifyMirrored := func(name string) {
		t.Helper()
		cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, name, true)
		fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/"+name, "/tmp/cephfs-"+name+".json")
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
	seeded := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, cephFSWatchScript, "/federation", "backup-1")

	mirrorImage := harness.StaticFacts(source).Images.CephFS
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", mirrorImage), cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{"/federation"},
	}))
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && mirror.Container != nil {
				harness.MultiClusterLogContainer(t, cleanupCtx, mirror.Container)
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate CephFS mirror: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("CephFS mirror peers: got %v, error %v; want one", peers, err)
	}
	expected.PeerID = peers[0]
	// Settle the seeded checkpoint first, so that the baseline is a known
	// state and every later change comes from this test's own steps.
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, cephFSWatchScript, expected, "/federation", seeded)

	w, err := mirror.WatchDirectory(ctx, "/federation", ceph.WatchConfig{Interval: 2 * time.Second, Buffer: 64})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the mirror's cleanup, so it runs first.
	t.Cleanup(func() {
		if err := w.Stop(); err != nil {
			t.Errorf("CephFS directory watch ended with: %v", err)
		}
	})
	baseline := <-w.Events()
	if baseline.Kind != ceph.WatchBaseline || baseline.Err != nil || !baseline.Status.Ready || baseline.Status.LastSyncedSnapshot == nil || *baseline.Status.LastSyncedSnapshot != seeded {
		t.Fatalf("CephFS directory watch baseline: %+v", baseline)
	}
	harness.CephFSAssertObservedIdentity(t, baseline.Status, expected, "/federation")

	// A new checkpoint: the watch reports exactly its source ID and name, and
	// only then are the destination bytes checked.
	first := checkpoint("watched", "watch-1")
	synced := cephFSAwaitDirectoryWatch(t, ctx, w, expected, "the first checkpoint reported as synced", func(s cephfsmirror.DirectoryStatus) bool {
		return s.Ready && s.LastSyncedSnapshot != nil && *s.LastSyncedSnapshot == first
	})
	verifyMirrored("watch-1")
	t.Logf("CephFS directory watch: daemon %s instance %s reported source snapshot %d %q as last synced; destination bytes verified independently", synced.DaemonName, synced.InstanceID, first.ID, first.Name)

	// Receiver loss: the next checkpoint is taken only after the watch has
	// seen the directory lose its live owner.
	stopTimeout := 5 * time.Second
	if err := mirror.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	lost := cephFSAwaitDirectoryWatch(t, ctx, w, expected, "the receiver loss", func(s cephfsmirror.DirectoryStatus) bool { return !s.Ready })
	if lost.MappingState == "mapped" && len(lost.DaemonProblems) == 0 {
		t.Fatalf("a mapped non-ready directory without the stopped receiver as evidence: %+v", lost)
	}
	second := checkpoint("receiver-lost", "watch-2")
	cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "watch-2", 10*time.Second)
	if err := mirror.Start(ctx); err != nil {
		t.Fatal(err)
	}
	resumed := cephFSAwaitDirectoryWatch(t, ctx, w, expected, "the restarted receiver catching up", func(s cephfsmirror.DirectoryStatus) bool {
		return s.Ready && s.LastSyncedSnapshot != nil && *s.LastSyncedSnapshot == second
	})
	verifyMirrored("watch-2")
	verifyMirrored("watch-1")
	t.Logf("CephFS directory watch: receiver loss reported as mapping=%s problems=%d; restarted instance %s reported source snapshot %d %q; destination bytes of both checkpoints verified independently", lost.MappingState, len(lost.DaemonProblems), resumed.InstanceID, second.ID, second.Name)
	if err := w.Stop(); err != nil {
		t.Fatalf("CephFS directory watch ended with: %v", err)
	}
}

// cephFSAwaitDirectoryWatch receives events until a successful read matches
// done, checking the fixed identities of each one. Gaps are logged and
// skipped; a closed channel fails with the cause.
func cephFSAwaitDirectoryWatch(t *testing.T, ctx context.Context, w *cephfsmirror.DirectoryWatch, expected cephfsmirror.DirectoryStatus, what string, done func(cephfsmirror.DirectoryStatus) bool) cephfsmirror.DirectoryStatus {
	t.Helper()
	timeout := time.NewTimer(4 * time.Minute)
	defer timeout.Stop()
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				t.Fatalf("CephFS directory watch ended before %s: %v", what, w.Stop())
			}
			if event.Kind == ceph.WatchGap {
				t.Logf("CephFS directory watch gap %d: %v", event.Sequence, event.Err)
				continue
			}
			s := event.Status
			t.Logf("CephFS directory watch %s %d: mapping=%s instance=%s daemon=%s state=%s ready=%t last=%v problems=%v", event.Kind, event.Sequence, s.MappingState, s.InstanceID, s.DaemonName, s.State, s.Ready, s.LastSyncedSnapshot, s.DaemonProblems)
			harness.CephFSAssertObservedIdentity(t, s, expected, "/federation")
			if done(s) {
				return s
			}
		case <-timeout.C:
			t.Fatalf("CephFS directory watch did not report %s", what)
		case <-ctx.Done():
			t.Fatalf("CephFS directory watch did not report %s: %v", what, ctx.Err())
		}
	}
}
