//go:build all || (integration && multicluster)

package cephfs_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func runMultiClusterCephFSSnapshotMirrorAndBackup(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
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
		if err := client.CopyToContainer(ctx, []byte(harness.CephFSInterClusterScript), "/tmp/cephfs-intercluster.py", 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
		t.Helper()
		command := append([]string{"python3", "/tmp/cephfs-intercluster.py", filesystem}, args...)
		output := harness.MultiClusterExecOutput(t, ctx, client, command...)
		if summary := strings.TrimSpace(string(output)); summary != "" {
			t.Logf("CephFS %s: %s", args[0], summary)
		}
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
	snapshot1 := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-1")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	archive := harness.MultiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-1.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-1.json", 0o600); err != nil {
		t.Fatal(err)
	}
	fsCommand(destinationClient, destinationFS.FilesystemName, "restore", "/restored", "/tmp/cephfs-backup-1.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify", "/restored", "/tmp/cephfs-backup-1.json")
	t.Log("CephFS snapshot backup: restored binary/empty/nested files, relative symlink, modes, owners and user xattrs into an independent filesystem")

	// Cluster/filesystem setup above stays separate from multicluster setup.
	// The source control runtime supplies the userspace mirror daemon.
	mirrorImage := harness.StaticFacts(source).Images.CephFS
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", mirrorImage), cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{"/federation"},
	}))
	if mirror != nil {
		// A partial setup may have an owned network attachment but no daemon.
		// Federation cleanup closes both before the pair's cluster cleanup.
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && mirror.Container != nil {
				harness.MultiClusterLogContainer(t, cleanupCtx, mirror.Container)
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate CephFS multicluster runtime: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("CephFS initial mirror peers: got %v, error %v; want one", peers, err)
	}
	expected.PeerID = peers[0]
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot1)
	initialStatus, err := mirror.DirectoryStatus(ctx, "/federation")
	if err != nil || !initialStatus.Ready || len(initialStatus.DaemonProblems) != 0 {
		t.Fatalf("single live CephFS mirror directory status: status=%+v error=%v", initialStatus, err)
	}
	harness.CephFSAssertObservedIdentity(t, initialStatus, expected, "/federation")
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-1", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	t.Log("native cephfs-mirror: first snapshot reached the destination; content, names, symlink, modes and owners checked separately from user xattrs")

	stopTimeout := 5 * time.Second
	if err := mirror.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	stoppedContainerID := mirror.GetContainerID()
	stoppedState, err := mirror.State(ctx)
	if err != nil || stoppedState == nil || stoppedState.Running {
		t.Fatalf("CephFS receiver did not stop: state=%+v error=%v", stoppedState, err)
	}
	stoppedStatus, stoppedErr := mirror.DirectoryStatus(ctx, "/federation")
	if stoppedStatus.Ready {
		t.Fatalf("stopped CephFS receiver reported a ready directory: status=%+v error=%v", stoppedStatus, stoppedErr)
	}
	harness.CephFSAssertObservedIdentity(t, stoppedStatus, expected, "/federation")
	pausedCtx, pausedCancel := context.WithTimeout(ctx, 2*time.Second)
	pausedStatus, pausedErr := mirror.WaitDirectoryReady(pausedCtx, "/federation")
	pausedCancel()
	if !errors.Is(pausedErr, context.DeadlineExceeded) || pausedStatus.Ready {
		t.Fatalf("paused CephFS directory wait: status=%+v error=%v; want non-ready deadline", pausedStatus, pausedErr)
	}
	stoppedState, err = mirror.State(ctx)
	if err != nil || stoppedState == nil || stoppedState.Running || mirror.GetContainerID() != stoppedContainerID {
		t.Fatalf("CephFS observation changed the stopped receiver: state=%+v error=%v", stoppedState, err)
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "mutate")
	snapshot2 := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-2")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	archive = harness.MultiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-2.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-2.json", 0o600); err != nil {
		t.Fatal(err)
	}
	cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-2", 10*time.Second)
	if err := mirror.Start(ctx); err != nil {
		t.Fatal(err)
	}
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot2)
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-2", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	t.Log("native cephfs-mirror restart: caught up changed bytes, deleted file, renamed file and new file; old snapshot retained its original data; user xattr differences reported explicitly")
	fsCommand(sourceClient, sourceFS.FilesystemName, "remove-snapshot", "backup-1")
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-1", false)
	t.Log("native CephFS snapshot deletion propagated to the destination")

	// Remove directory membership while the daemon stays running. Wait for the
	// daemon to consume the policy update before creating new source work.
	if err := mirror.RemoveDirectory(ctx, "/federation"); err != nil {
		t.Fatal(err)
	}
	harness.CephFSWaitForDaemonPolicy(t, ctx, source, mirror, 0, "")
	fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "directory-removed", "backup-3")
	snapshot3 := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-3")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-3", "/tmp/cephfs-backup-3.json")
	archive = harness.MultiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-3.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-3.json", 0o600); err != nil {
		t.Fatal(err)
	}
	cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-3", 10*time.Second)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	if err := mirror.AddDirectory(ctx, "/federation"); err != nil {
		t.Fatal(err)
	}
	harness.CephFSWaitForDaemonPolicy(t, ctx, source, mirror, 1, "")
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot3)
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-3", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-3", "/tmp/cephfs-backup-3.json")
	t.Log("native CephFS directory membership: unregister stopped new snapshot delivery for 10s, existing destination data survived, re-register caught up snapshot and new bytes")

	// Removing the peer is independent of directory membership. The old UUID
	// must disappear from both manager policy and the running daemon before a
	// fresh token is imported for the same source filesystem.
	peers, err = mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("CephFS mirror peers: got %v, error %v; want one", peers, err)
	}
	removedPeer := peers[0]
	if err := mirror.RemovePeer(ctx, removedPeer); err != nil {
		t.Fatal(err)
	}
	peers, err = mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 0 {
		t.Fatalf("CephFS mirror retained removed peer: got %v, error %v", peers, err)
	}
	harness.CephFSWaitForDaemonPolicy(t, ctx, source, mirror, 1, removedPeer)
	fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "peer-removed", "backup-4")
	snapshot4 := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-4")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-4", "/tmp/cephfs-backup-4.json")
	archive = harness.MultiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-4.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-4.json", 0o600); err != nil {
		t.Fatal(err)
	}
	cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-4", 10*time.Second)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-3", "/tmp/cephfs-backup-3.json")
	newPeer, err := mirror.RebootstrapPeer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peers, err = mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 || peers[0] != newPeer {
		t.Fatalf("CephFS rebootstrap peers: got %v, error %v; want %s", peers, err, newPeer)
	}
	if newPeer == removedPeer {
		t.Fatal("CephFS fresh bootstrap retained the removed peer UUID")
	}
	// A fresh public wait accepts the new peer for the same two filesystems
	// and owned directory; an in-progress wait pins its original peer UUID.
	expected.PeerID = newPeer
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot4)
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-4", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-4", "/tmp/cephfs-backup-4.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-3", "/tmp/cephfs-backup-3.json")
	t.Logf("native CephFS peer membership: removed %s, new snapshot remained absent for 10s, retained destination snapshots readable; fresh bootstrap %s caught up new snapshot and bytes", removedPeer, newPeer)
	harness.AdvanceServiceTopology(t, ctx, destination)

	if err := mirror.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	harness.StopMultiClusterSource(t, ctx, source)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-4", "/tmp/cephfs-backup-4.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify", "/restored", "/tmp/cephfs-backup-1.json")
	t.Log("source MON/MDS/OSDs stopped: fresh destination userspace sessions still read the mirrored snapshot and separately restored backup after destination OSD replacement")
}

// A single absent response would also pass before a pending asynchronous sync
// starts. Observe absence continuously across several daemon scan cycles and
// fail immediately on either an unexpected snapshot or a client/transport error.
func cephFSAssertSnapshotRemainsAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	checks := 0
	for {
		code, r, err := client.Exec(ctx, []string{"python3", "/tmp/cephfs-intercluster.py", filesystem, "snapshot-state", snapshot, "absent"}, tcexec.Multiplexed())
		if err != nil {
			t.Fatalf("check CephFS blocked snapshot %s: %v", snapshot, err)
		}
		out, err := io.ReadAll(r)
		if err != nil || code != 0 {
			t.Fatalf("CephFS snapshot %s did not remain absent: code=%d output=%s error=%v", snapshot, code, out, err)
		}
		checks++
		if time.Now().After(deadline) {
			t.Logf("CephFS blocked snapshot %s remained absent for %s across %d fresh client sessions", snapshot, duration, checks)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("check blocked CephFS snapshot: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func cephFSWaitForRemoteSnapshot(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string, present bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	expectation := "absent"
	if present {
		expectation = "present"
	}
	var last string
	for {
		code, r, err := client.Exec(waitCtx, []string{"python3", "/tmp/cephfs-intercluster.py", filesystem, "snapshot-state", snapshot, expectation}, tcexec.Multiplexed())
		if err != nil {
			last = err.Error()
		} else {
			out, err := io.ReadAll(r)
			last = string(out)
			if err == nil && code == 0 {
				return
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("CephFS remote snapshot %s did not become %s: %s", snapshot, expectation, last)
		case <-time.After(time.Second):
		}
	}
}
