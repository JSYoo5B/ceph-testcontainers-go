//go:build integration && multicluster

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// This exercises two separate mechanisms: an application snapshot archive and
// the native cephfs-mirror daemon. The archive is deliberately a small fixture
// format, not a general-purpose filesystem backup utility.
func TestMultiClusterCephFSSnapshotMirrorAndBackup(t *testing.T) {
	testMultiClusterCephFSSnapshotMirrorAndBackup(t)
}

func testMultiClusterCephFSSnapshotMirrorAndBackup(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
	sourceFS, err := source.StartCephFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := destination.StartCephFS(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expected := cephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSInterClusterScript), "/tmp/cephfs-intercluster.py", 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
		t.Helper()
		command := append([]string{"python3", "/tmp/cephfs-intercluster.py", filesystem}, args...)
		output := multiClusterExecOutput(t, ctx, client, command...)
		if summary := strings.TrimSpace(string(output)); summary != "" {
			t.Logf("CephFS %s: %s", args[0], summary)
		}
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
	snapshot1 := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-1")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-1", "/tmp/cephfs-backup-1.json")
	archive := multiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-1.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-1.json", 0o600); err != nil {
		t.Fatal(err)
	}
	fsCommand(destinationClient, destinationFS.FilesystemName, "restore", "/restored", "/tmp/cephfs-backup-1.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify", "/restored", "/tmp/cephfs-backup-1.json")
	t.Log("CephFS snapshot backup: restored binary/empty/nested files, relative symlink, modes, owners and user xattrs into an independent filesystem")

	// Cluster/filesystem setup above stays separate from multicluster setup.
	// The source control runtime supplies the userspace mirror daemon.
	mirrorImage := source.ControlImage()
	mirror, err := multicluster.RunCephFSMirror(ctx, mirrorImage, multicluster.CephFSMirrorConfig{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{"/federation"},
	})
	if mirror != nil {
		// A partial setup may have an owned network attachment but no daemon.
		// Federation cleanup closes both before the pair's cluster cleanup.
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && mirror.Container != nil {
				multiClusterLogContainer(t, cleanupCtx, mirror.Container)
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
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot1)
	initialStatus, err := mirror.DirectoryStatus(ctx, "/federation")
	if err != nil || !initialStatus.Ready || len(initialStatus.DaemonProblems) != 0 {
		t.Fatalf("single live CephFS mirror directory status: status=%+v error=%v", initialStatus, err)
	}
	cephFSAssertObservedIdentity(t, initialStatus, expected, "/federation")
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
	cephFSAssertObservedIdentity(t, stoppedStatus, expected, "/federation")
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
	snapshot2 := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-2")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	archive = multiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-2.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-2.json", 0o600); err != nil {
		t.Fatal(err)
	}
	cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-2", 10*time.Second)
	if err := mirror.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot2)
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
	cephFSWaitForDaemonPolicy(t, ctx, source, mirror, 0, "")
	fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "directory-removed", "backup-3")
	snapshot3 := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-3")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-3", "/tmp/cephfs-backup-3.json")
	archive = multiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-3.json")
	if err := destinationClient.CopyToContainer(ctx, archive, "/tmp/cephfs-backup-3.json", 0o600); err != nil {
		t.Fatal(err)
	}
	cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-3", 10*time.Second)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	if err := mirror.AddDirectory(ctx, "/federation"); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForDaemonPolicy(t, ctx, source, mirror, 1, "")
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot3)
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
	cephFSWaitForDaemonPolicy(t, ctx, source, mirror, 1, removedPeer)
	fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "peer-removed", "backup-4")
	snapshot4 := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-intercluster.py", "/federation", "backup-4")
	fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/backup-4", "/tmp/cephfs-backup-4.json")
	archive = multiClusterReadFile(t, ctx, sourceClient, "/tmp/cephfs-backup-4.json")
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
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-intercluster.py", expected, "/federation", snapshot4)
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-4", true)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-4", "/tmp/cephfs-backup-4.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-3", "/tmp/cephfs-backup-3.json")
	t.Logf("native CephFS peer membership: removed %s, new snapshot remained absent for 10s, retained destination snapshots readable; fresh bootstrap %s caught up new snapshot and bytes", removedPeer, newPeer)
	advanceServiceTopology(t, ctx, destination)

	if err := mirror.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	stopMultiClusterSource(t, ctx, source)
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-2", "/tmp/cephfs-backup-2.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/backup-4", "/tmp/cephfs-backup-4.json")
	fsCommand(destinationClient, destinationFS.FilesystemName, "verify", "/restored", "/tmp/cephfs-backup-1.json")
	t.Log("source MON/MDS/OSDs stopped: fresh destination userspace sessions still read the mirrored snapshot and separately restored backup after destination OSD replacement")
}

// Read the expected checkpoint from the source native client. Destination snap
// IDs and mirror-reported last_synced_snap cannot supply this independent value.
func cephFSReadSourceSnapshot(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, directory, name string) multicluster.CephFSMirrorSnapshot {
	t.Helper()
	raw := multiClusterExecOutput(t, ctx, client, "python3", script, filesystem, "snapshot-checkpoint", directory, name)
	var checkpoint multicluster.CephFSMirrorSnapshot
	if err := json.Unmarshal(raw, &checkpoint); err != nil || checkpoint.ID == 0 || checkpoint.ID > ^uint64(0)-2 || checkpoint.Name != name {
		t.Fatalf("invalid independent CephFS source snapshot %s/.snap/%s: data=%s error=%v", directory, name, raw, err)
	}
	return checkpoint
}

func cephFSObservedFilesystemIdentity(t *testing.T, ctx context.Context, source, destination *ceph.CephFSContainer) multicluster.CephFSMirrorDirectoryStatus {
	t.Helper()
	sourceStatus, err := source.MDSStatus(ctx)
	if err != nil || sourceStatus == nil || sourceStatus.FilesystemID <= 0 {
		t.Fatalf("read independent source filesystem identity: status=%+v error=%v", sourceStatus, err)
	}
	destinationStatus, err := destination.MDSStatus(ctx)
	if err != nil || destinationStatus == nil || destinationStatus.FilesystemID <= 0 {
		t.Fatalf("read independent destination filesystem identity: status=%+v error=%v", destinationStatus, err)
	}
	return multicluster.CephFSMirrorDirectoryStatus{
		SourceFilesystem: source.FilesystemName, DestinationFilesystem: destination.FilesystemName,
		SourceFilesystemID: int(sourceStatus.FilesystemID), DestinationFilesystemID: int(destinationStatus.FilesystemID),
	}
}

func cephFSAssertObservedIdentity(t *testing.T, status, expected multicluster.CephFSMirrorDirectoryStatus, directory string) {
	t.Helper()
	if status.SourceFilesystem != expected.SourceFilesystem || status.DestinationFilesystem != expected.DestinationFilesystem ||
		status.SourceFilesystemID != expected.SourceFilesystemID || status.DestinationFilesystemID != expected.DestinationFilesystemID ||
		status.Directory != directory || status.PeerID != expected.PeerID {
		t.Fatalf("CephFS directory observation changed independent filesystem/peer identity: status=%+v expected=%+v directory=%s", status, expected, directory)
	}
	if status.Ready {
		watcherID, err := strconv.ParseUint(status.InstanceID, 10, 64)
		if err != nil || watcherID == 0 || status.MappingState != "mapped" || status.DaemonName == "" {
			t.Fatalf("ready CephFS directory lacks a mapped native owner: status=%+v", status)
		}
	}
}

func cephFSWaitObservedSnapshot(t *testing.T, ctx context.Context, mirror *multicluster.CephFSMirror, sourceClient testcontainers.Container, script string, expected multicluster.CephFSMirrorDirectoryStatus, directory string, checkpoint multicluster.CephFSMirrorSnapshot) multicluster.CephFSMirrorDirectoryStatus {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	ready, err := mirror.WaitDirectoryReady(waitCtx, directory)
	if err != nil || !ready.Ready {
		t.Fatalf("wait ready CephFS directory %s: status=%+v error=%v", directory, ready, err)
	}
	cephFSAssertObservedIdentity(t, ready, expected, directory)
	status, err := mirror.WaitSnapshotSynced(waitCtx, directory, checkpoint)
	if err != nil || !status.Ready || status.LastSyncedSnapshot == nil || *status.LastSyncedSnapshot != checkpoint {
		t.Fatalf("wait exact source CephFS snapshot %s/.snap/%s id=%d: status=%+v error=%v", directory, checkpoint.Name, checkpoint.ID, status, err)
	}
	cephFSAssertObservedIdentity(t, status, expected, directory)
	current := cephFSReadSourceSnapshot(t, waitCtx, sourceClient, expected.SourceFilesystem, script, directory, checkpoint.Name)
	if current != checkpoint {
		t.Fatalf("source CephFS snapshot was recreated during observation: before=%+v after=%+v", checkpoint, current)
	}
	t.Logf("public CephFS directory checkpoint: directory=%s peer=%s owner=%s watcher=%s source_snap=%d/%s problems=%v", directory, status.PeerID, status.DaemonName, status.InstanceID, checkpoint.ID, checkpoint.Name, status.DaemonProblems)
	return status
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

func cephFSWaitForDaemonPolicy(t *testing.T, ctx context.Context, source *ceph.Container, mirror *multicluster.CephFSMirror, directoryCount int, removedPeer string) {
	t.Helper()
	data, err := source.Ceph(ctx, "fs", "get", mirror.SourceFilesystem, "--format", "json")
	if err != nil {
		t.Fatal(err)
	}
	var filesystem struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(data, &filesystem); err != nil || filesystem.ID <= 0 {
		t.Fatalf("decode CephFS filesystem ID: output=%s error=%v", data, err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last string
	for {
		code, r, err := mirror.Exec(waitCtx, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fmt.Sprintf("%s@%d", mirror.SourceFilesystem, filesystem.ID)}, tcexec.Multiplexed())
		if err != nil {
			last = err.Error()
		} else {
			output, err := io.ReadAll(r)
			last = string(output)
			var state struct {
				Peers    map[string]json.RawMessage `json:"peers"`
				SnapDirs *struct {
					Count int `json:"dir_count"`
				} `json:"snap_dirs"`
			}
			if err == nil && code == 0 && json.Unmarshal(output, &state) == nil {
				_, peerPresent := state.Peers[removedPeer]
				if state.SnapDirs != nil && state.Peers != nil && state.SnapDirs.Count == directoryCount && (removedPeer == "" || !peerPresent) {
					return
				}
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("CephFS daemon did not consume membership change: directories=%d removedPeer=%s last=%s", directoryCount, removedPeer, last)
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

const cephFSInterClusterScript = `import base64
import cephfs
import hashlib
import json
import os
import stat
import sys

filesystem, phase, *args = sys.argv[1:]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())

def write(path, data, mode=0o640):
    fd = fs.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, mode)
    try:
        # libcephfs rejects a zero-length write with EINVAL. O_CREAT/O_TRUNC
        # still creates the actual empty file; fsync covers it below.
        if data:
            assert fs.write(fd, data, 0) == len(data), 'short write: ' + path
        fs.fsync(fd, False)
    finally:
        fs.close(fd)

def read(path, size):
    fd = fs.open(path, os.O_RDONLY)
    try:
        data = fs.read(fd, 0, size + 1)
        assert len(data) == size, 'short read: ' + path
        return data
    finally:
        fs.close(fd)

def children(path):
    names = []
    with fs.opendir(path) as directory:
        while True:
            entry = fs.readdir(directory)
            if entry is None:
                break
            if entry.d_name not in (b'.', b'..', b'.snap'):
                names.append(entry.d_name.decode('utf-8'))
    return sorted(names)

def capture(root):
    entries = []
    def visit(relative):
        path = root + ('/' + relative if relative else '')
        st = fs.stat(path, follow_symlink=False)
        item = {'path': relative, 'mode': stat.S_IMODE(st.st_mode), 'uid': st.st_uid, 'gid': st.st_gid}
        if stat.S_ISDIR(st.st_mode):
            item['kind'] = 'directory'
        elif stat.S_ISREG(st.st_mode):
            item['kind'] = 'file'
            data = read(path, st.st_size)
            item['data'] = base64.b64encode(data).decode('ascii')
            item['sha256'] = hashlib.sha256(data).hexdigest()
        elif stat.S_ISLNK(st.st_mode):
            item['kind'] = 'symlink'
            item['target'] = fs.readlink(path, 4096).decode('utf-8')
        else:
            raise AssertionError('unsupported fixture type: ' + path)
        # Preserve user attributes only. Ceph virtual layout/quota attributes
        # belong to a filesystem's configuration and are outside this fixture.
        item['xattrs'] = {}
        if item['kind'] != 'symlink':
            _, keys = fs.listxattr(path)
            for key in keys.split(b'\x00'):
                if key.startswith(b'user.'):
                    item['xattrs'][key.decode()] = base64.b64encode(fs.getxattr(path, key, 65536)).decode('ascii')
        entries.append(item)
        if item['kind'] == 'directory':
            for child in children(path):
                visit(relative + '/' + child if relative else child)
    visit('')
    return entries

def load_archive(path):
    with open(path) as reader:
        archive = json.load(reader)
    assert archive['format'] == 'testcontainers-cephfs-fixture-v1'
    for item in archive['entries']:
        relative = item['path']
        assert not relative.startswith('/') and '..' not in relative.split('/'), 'invalid archive path'
        if item['kind'] == 'file':
            data = base64.b64decode(item['data'], validate=True)
            assert hashlib.sha256(data).hexdigest() == item['sha256'], 'archive digest mismatch'
    return archive['entries']

try:
    if phase == 'seed':
        fs.mkdir('/federation', 0o750)
        fs.mkdir('/federation/nested', 0o700)
        fs.mkdir('/federation/nested/deeper', 0o750)
        write('/federation/nested/deeper/자료.bin', bytes(range(256)) * 512, 0o600)
        write('/federation/empty', b'', 0o600)
        write('/federation/space name.txt', b'CephFS intercluster snapshot backup\n')
        write('/federation/remove-me', b'present only in the first snapshot')
        write('/federation/rename-me', b'content survives rename')
        fs.symlink('nested/deeper/자료.bin', '/federation/binary-link')
        fs.setxattr('/federation', 'user.testcontainers', b'root-metadata', 0)
        fs.setxattr('/federation/nested/deeper/자료.bin', 'user.testcontainers', b'binary-metadata-v1\x00\xff', 0)
        fs.chown('/federation/space name.txt', 1234, 2345)
        fs.chmod('/federation/space name.txt', 0o640)
        fs.sync_fs()
        fs.mkdir('/federation/.snap/backup-1', 0o755)
    elif phase == 'mutate':
        write('/federation/nested/deeper/자료.bin', b'updated snapshot payload\n' * 3000, 0o600)
        fs.setxattr('/federation/nested/deeper/자료.bin', 'user.testcontainers', b'binary-metadata-v2', 0)
        fs.unlink('/federation/remove-me')
        fs.rename('/federation/rename-me', '/federation/renamed')
        write('/federation/new-after-outage', b'written while mirror was stopped', 0o600)
        fs.sync_fs()
        fs.mkdir('/federation/.snap/backup-2', 0o755)
    elif phase == 'membership-checkpoint':
        label, snapshot = args
        write('/federation/membership-' + label, ('new bytes while ' + label + '\n').encode() * 2048, 0o600)
        fs.sync_fs()
        fs.mkdir('/federation/.snap/' + snapshot, 0o755)
    elif phase == 'archive':
        root, archive_path = args
        entries = capture(root)
        with open(archive_path, 'w') as writer:
            json.dump({'format': 'testcontainers-cephfs-fixture-v1', 'entries': entries}, writer, sort_keys=True)
        print(json.dumps({'entries': len(entries), 'files': sum(e['kind'] == 'file' for e in entries), 'action': 'archived'}))
    elif phase == 'restore':
        root, archive_path = args
        entries = load_archive(archive_path)
        for item in entries:
            path = root + ('/' + item['path'] if item['path'] else '')
            if item['kind'] == 'directory':
                fs.mkdir(path, 0o700)
            elif item['kind'] == 'file':
                write(path, base64.b64decode(item['data']), 0o600)
            elif item['kind'] == 'symlink':
                fs.symlink(item['target'], path)
            fs.chown(path, item['uid'], item['gid'], follow_symlink=False)
            if item['kind'] != 'symlink':
                fs.chmod(path, item['mode'])
                for key, value in item['xattrs'].items():
                    fs.setxattr(path, key, base64.b64decode(value), 0)
        fs.sync_fs()
    elif phase in ('verify', 'verify-mirror'):
        root, archive_path = args
        expected = load_archive(archive_path)
        actual = capture(root)
        xattr_differences = []
        if phase == 'verify-mirror':
            # Native snapshot mirroring and this application backup are
            # different mechanisms. Report native user-xattr preservation
            # independently instead of silently claiming archive semantics.
            assert [e['path'] for e in actual] == [e['path'] for e in expected], 'mirrored paths differ'
            for observed, wanted in zip(actual, expected):
                if observed['xattrs'] != wanted['xattrs']:
                    xattr_differences.append(wanted['path'] or '.')
                del observed['xattrs']
                del wanted['xattrs']
        if actual != expected:
            assert [e['path'] for e in actual] == [e['path'] for e in expected], 'snapshot/restore paths differ at ' + root
            for observed, wanted in zip(actual, expected):
                if observed != wanted:
                    changed = sorted(k for k in set(observed) | set(wanted) if observed.get(k) != wanted.get(k))
                    # Report the exact entry and fields, without dumping the
                    # binary fixture's base64 payload into integration logs.
                    raise AssertionError('snapshot/restore mismatch at ' + root + '/' + wanted['path'] + ': ' + ','.join(changed))
        print(json.dumps({'entries': len(actual), 'files': sum(e['kind'] == 'file' for e in actual), 'action': 'verified', 'user_xattr_difference_paths': xattr_differences}))
    elif phase == 'snapshot-state':
        name, expectation = args
        try:
            fs.stat('/federation/.snap/' + name)
            present = True
        except cephfs.ObjectNotFound:
            present = False
        assert present == (expectation == 'present'), 'snapshot state still pending'
    elif phase == 'snapshot-checkpoint':
        root, name = args
        assert root.startswith('/') and '\x00' not in root, 'invalid snapshot root'
        assert name and name not in ('.', '..') and '/' not in name and '\x00' not in name, 'invalid snapshot name'
        snapshot_path = root.rstrip('/') + '/.snap/' + name
        info = fs.snap_info(snapshot_path)
        snapshot_id = info['id']
        assert type(snapshot_id) is int and 1 <= snapshot_id <= 18446744073709551613, 'invalid source snapshot ID'
        print(json.dumps({'id': snapshot_id, 'name': name}, sort_keys=True))
    elif phase == 'remove-snapshot':
        fs.rmdir('/federation/.snap/' + args[0])
    else:
        raise AssertionError('unknown phase: ' + phase)
finally:
    fs.shutdown()
`
