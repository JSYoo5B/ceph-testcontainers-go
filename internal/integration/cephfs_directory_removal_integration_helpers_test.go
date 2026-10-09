//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testCephFSDirectoryRemovalRelease(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expected := cephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	const script = "/tmp/cephfs-mirror-daemons.py"
	const probe = "/tmp/cephfs-directory-removal-probe.py"
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSMirrorDaemonScript), script, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := client.CopyToContainer(ctx, []byte(cephFSDirectoryRemovalProbeScript), probe, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	directories := []string{"/daemon-a", "/daemon-b", "/daemon-c", "/daemon-d"}
	checkpoint := func(name string) map[string]cephfs.MirrorSnapshot {
		t.Helper()
		multiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", name)
		result := make(map[string]cephfs.MirrorSnapshot, len(directories))
		for _, directory := range directories {
			result[directory] = cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
		}
		return result
	}
	initial := checkpoint("before-directory-removal")
	const destinationSite = "directory-release-target"
	mirror, err := cephfs.RunMirror(ctx, source.ControlImage(), cephfs.MirrorConfig{Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName, DestinationSite: destinationSite, Directories: directories, DaemonCount: 2})
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() {
				for _, daemon := range mirror.Daemons() {
					multiClusterLogContainer(t, cleanupCtx, daemon)
				}
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate retained directory release fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("original owned peer: %v %v", peers, err)
	}
	expected.PeerID = peers[0]
	waitCheckpoints := func(selected []string, snapshots map[string]cephfs.MirrorSnapshot) map[string]string {
		t.Helper()
		owners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, selected, 2)
		for _, directory := range selected {
			status := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, snapshots[directory])
			if status.InstanceID != owners[directory] {
				t.Fatal("public checkpoint disagrees with native watcher assignment")
			}
		}
		return owners
	}
	owners := waitCheckpoints(directories, initial)
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "before-directory-removal")
	daemons := mirror.Daemons()
	if len(daemons) != 2 {
		t.Fatal("strict native release requires the complete two-daemon cohort")
	}
	originals := make(map[string]cephFSPeerRemovalNativeWitness, len(daemons))
	for _, daemon := range daemons {
		originals[daemon.DaemonName] = cephFSReadRemovalWitness(t, ctx, source, sourceFS, expected.SourceFilesystemID, daemon, expected.PeerID, true)
		cephFSAssertDirectoryRemovalPeerTuple(t, ctx, daemon, sourceFS.FilesystemName, expected.SourceFilesystemID, expected.PeerID, mirror.DestinationClientEntity, destinationSite, destinationFS.FilesystemName)
	}
	// Keep positive ownership on both original processes so the surviving paths
	// can independently prove ongoing replication across the same cohort.
	ownerCounts := make(map[string]int)
	for _, owner := range owners {
		ownerCounts[owner]++
	}
	selected := ""
	for _, directory := range directories {
		if ownerCounts[owners[directory]] > 1 {
			selected = directory
			break
		}
	}
	if selected == "" {
		t.Fatal("selected original daemon has no positively witnessed directory")
	}
	remaining := slices.DeleteFunc(slices.Clone(directories), func(path string) bool { return path == selected })
	for _, daemon := range daemons {
		stats := cephFSReadDirectoryRemovalStats(t, ctx, daemon, sourceFS.FilesystemName, expected.SourceFilesystemID, expected.PeerID)
		if _, present := stats[selected]; !present && owners[selected] == originals[daemon.DaemonName].gid {
			t.Fatal("independent positive directory pre-witness disagrees with mapped owner")
		}
	}
	originalControl := source.Container
	control, err := source.ControlContainerContext(ctx)
	if err != nil || control != originalControl {
		t.Fatalf("lost-response probe needs original single-MON CLI: %v", err)
	}
	fault := &cephFSDirectoryRemovalReplyFault{Container: originalControl, filesystem: sourceFS.FilesystemName, directory: selected}
	source.Container = fault
	t.Cleanup(func() { source.Container = originalControl })
	receipt, removeErr := mirror.BeginDirectoryRemoval(ctx, selected)
	if receipt == nil || !errors.Is(removeErr, errCephFSDirectoryRemoveReplyLost) || fault.removals.Load() != 1 || !fault.lost.Load() {
		t.Fatalf("real native lost reply did not retain receipt: calls=%d error=%v", fault.removals.Load(), removeErr)
	}
	beforeGate := fault.commands.Load()
	if err := mirror.RebalanceDirectories(ctx); err == nil || fault.commands.Load() != beforeGate {
		t.Fatal("pending directory release allowed cohort/policy overlap")
	}
	if _, err := mirror.BeginPeerRemoval(ctx, expected.PeerID); err == nil || fault.commands.Load() != beforeGate {
		t.Fatal("pending directory release allowed peer teardown")
	}
	retryCtx, retryCancel := context.WithTimeout(ctx, time.Minute)
	for {
		again, retryErr := mirror.BeginDirectoryRemoval(retryCtx, selected)
		if again != receipt || fault.removals.Load() != 1 {
			retryCancel()
			t.Fatal("original fresh-context retry replaced receipt or repeated native remove")
		}
		if retryErr == nil {
			break
		}
		select {
		case <-retryCtx.Done():
			retryCancel()
			t.Fatal("original directory policy convergence after lost reply", errors.Join(retryErr, retryCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	retryCancel()
	if err := mirror.RemoveDirectory(ctx, selected); err != nil || fault.removals.Load() != 1 {
		t.Fatal("legacy same-path retry discarded receipt or repeated absent policy removal", err)
	}
	source.Container = originalControl
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 3*time.Minute)
	released, err := receipt.WaitReleased(releaseCtx)
	releaseCancel()
	if err != nil || !released.Released || !released.PolicyRemoved || released.Directory != selected || released.PeerID != expected.PeerID || released.SourceFilesystem != expected.SourceFilesystem || released.DestinationFilesystem != expected.DestinationFilesystem || released.SourceFilesystemID != expected.SourceFilesystemID || released.DestinationFilesystemID != expected.DestinationFilesystemID || len(released.Daemons) != len(originals) {
		t.Fatalf("strict same-session public directory release: %+v %v", released, err)
	}
	for _, daemon := range daemons {
		before := originals[daemon.DaemonName]
		after := cephFSReadRemovalWitness(t, ctx, source, sourceFS, expected.SourceFilesystemID, daemon, expected.PeerID, true)
		cephFSAssertDirectoryRemovalPeerTuple(t, ctx, daemon, sourceFS.FilesystemName, expected.SourceFilesystemID, expected.PeerID, mirror.DestinationClientEntity, destinationSite, destinationFS.FilesystemName)
		report, present := released.Daemons[daemon.DaemonName]
		if !present || report.State != "released" || report.Problem != "" || report.ContainerID != before.cid || report.InstanceID != before.gid || after != before {
			t.Fatalf("directory release changed original process/watcher/peer identity: %+v before=%+v after=%+v", report, before, after)
		}
		if _, present := cephFSReadDirectoryRemovalStats(t, ctx, daemon, sourceFS.FilesystemName, expected.SourceFilesystemID, expected.PeerID)[selected]; present {
			t.Fatal("public release accepted a path still tracked by an original replayer")
		}
	}
	if current, err := mirror.PeerIDs(ctx); err != nil || !slices.Equal(current, peers) {
		t.Fatal("directory release changed original peer policy", err)
	}
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "before-directory-removal")
	pending := checkpoint("while-directory-removed")
	cephFSWaitForDaemonSnapshots(t, ctx, sourceClient, sourceFS.FilesystemName, "before-directory-removal")
	waitCheckpoints(remaining, pending)
	cephFSVerifyDirectoryRemovalSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, probe, "verify", "while-directory-removed", remaining)
	cephFSAssertRemovedDirectorySnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, probe, selected, "while-directory-removed", 10*time.Second)
	if err := mirror.AddDirectory(ctx, selected); err != nil {
		t.Fatal("released receipt did not release same-path registration gate", err)
	}
	if old, err := receipt.Status(ctx); err == nil || old.Released || old.Directory != selected {
		t.Fatalf("old receipt adopted the newly registered path: %+v %v", old, err)
	}
	waitCheckpoints(directories, pending)
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "while-directory-removed")
	fresh := checkpoint("after-directory-readd")
	waitCheckpoints(directories, fresh)
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "after-directory-readd")
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "before-directory-removal")
	cephFSWaitForDaemonSnapshots(t, ctx, sourceClient, sourceFS.FilesystemName, "before-directory-removal")
	t.Logf("public strict directory release: path=%s original peer=%s two original CID/StartedAt/watcher sessions preserved; real lost reply reconciled without another delete; other three paths copied bytes while removed path stayed absent; same-path re-add superseded old receipt and copied independent checkpoints", selected, expected.PeerID)
}

func cephFSReadDirectoryRemovalStats(t *testing.T, ctx context.Context, daemon *cephfs.MirrorDaemon, filesystem string, id int, peer string) map[string]json.RawMessage {
	t.Helper()
	data := multiClusterExecOutput(t, ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", filesystem, id), peer)
	var result map[string]json.RawMessage
	if json.Unmarshal(data, &result) != nil || result == nil {
		t.Fatal("independent peer directory stats missing")
	}
	for directory, raw := range result {
		var stats struct {
			State   *string `json:"state"`
			Synced  *uint64 `json:"snaps_synced"`
			Deleted *uint64 `json:"snaps_deleted"`
			Renamed *uint64 `json:"snaps_renamed"`
		}
		if directory == "/" || !path.IsAbs(directory) || path.Clean(directory) != directory || json.Unmarshal(raw, &stats) != nil || stats.State == nil || !slices.Contains([]string{"idle", "syncing", "failed"}, *stats.State) || stats.Synced == nil || stats.Deleted == nil || stats.Renamed == nil {
			t.Fatal("independent native peer directory stats malformed")
		}
	}
	return result
}

func cephFSAssertDirectoryRemovalPeerTuple(t *testing.T, ctx context.Context, daemon *cephfs.MirrorDaemon, filesystem string, id int, peer, client, site, destination string) {
	t.Helper()
	data := multiClusterExecOutput(t, ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fmt.Sprintf("%s@%d", filesystem, id))
	var session struct {
		Peers map[string]struct {
			Remote struct {
				Client     string `json:"client_name"`
				Site       string `json:"cluster_name"`
				Filesystem string `json:"fs_name"`
			} `json:"remote"`
		} `json:"peers"`
	}
	if json.Unmarshal(data, &session) != nil || len(session.Peers) != 1 {
		t.Fatal("independent directory release peer policy malformed")
	}
	remote, present := session.Peers[peer]
	if !present || remote.Remote.Client != client || remote.Remote.Site != site || remote.Remote.Filesystem != destination {
		t.Fatal("directory release replaced the original native peer tuple")
	}
}

func cephFSVerifyDirectoryRemovalSnapshots(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, phase, snapshot string, directories []string) {
	t.Helper()
	encoded, err := json.Marshal(directories)
	if err != nil {
		t.Fatal(err)
	}
	output := multiClusterExecOutput(t, ctx, client, "python3", script, filesystem, phase, snapshot, string(encoded))
	t.Logf("native directory release file proof: %s", output)
}

func cephFSAssertRemovedDirectorySnapshotAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, directory, snapshot string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for {
		cephFSVerifyDirectoryRemovalSnapshots(t, ctx, client, filesystem, script, "absent", snapshot, []string{directory})
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("observe removed directory snapshot absence", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

var errCephFSDirectoryRemoveReplyLost = errors.New("injected lost real native CephFS directory remove reply")

type cephFSDirectoryRemovalReplyFault struct {
	testcontainers.Container
	filesystem, directory string
	commands, removals    atomic.Int32
	lost                  atomic.Bool
}

func (f *cephFSDirectoryRemovalReplyFault) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.commands.Add(1)
	native := args
	if len(native) >= 3 && native[0] == "ceph" && native[1] == "--connect-timeout" {
		native = native[3:]
	}
	target := slices.Equal(native, []string{"fs", "snapshot", "mirror", "remove", f.filesystem, f.directory})
	if target {
		f.removals.Add(1)
	}
	code, reader, err := f.Container.Exec(ctx, args, opts...)
	if target && err == nil && code == 0 && f.lost.CompareAndSwap(false, true) {
		if reader != nil {
			if _, err := io.Copy(io.Discard, reader); err != nil {
				return 0, nil, err
			}
		}
		return 0, nil, errCephFSDirectoryRemoveReplyLost
	}
	return code, reader, err
}

const cephFSDirectoryRemovalProbeScript = `import cephfs
import hashlib
import json
import os
import sys

filesystem, phase, snapshot, encoded = sys.argv[1:]
directories = json.loads(encoded)
assert type(directories) is list and directories
assert snapshot and snapshot not in ('.', '..') and '/' not in snapshot and '\x00' not in snapshot
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())
proofs = []
try:
    for directory in directories:
        assert directory in ('/daemon-a', '/daemon-b', '/daemon-c', '/daemon-d')
        path = (directory + '/.snap/' + snapshot).encode()
        if phase == 'absent':
            try:
                fs.stat(path)
            except cephfs.ObjectNotFound:
                proofs.append({'directory': directory, 'snapshot': snapshot, 'absent': True})
                continue
            raise AssertionError('removed path received an unexpected snapshot: ' + directory)
        assert phase == 'verify'
        expected = (snapshot + ':' + directory + ':').encode() * 1024 + bytes(range(256))
        fd = fs.open(path + b'/payload', os.O_RDONLY)
        try:
            actual = fs.read(fd, 0, len(expected) + 1)
            assert actual == expected, 'snapshot byte mismatch: ' + directory
            proofs.append({'directory': directory, 'snapshot': snapshot, 'bytes': len(actual), 'sha256': hashlib.sha256(actual).hexdigest()})
        finally:
            fs.close(fd)
    print(json.dumps(proofs, sort_keys=True))
finally:
    fs.shutdown()
`
