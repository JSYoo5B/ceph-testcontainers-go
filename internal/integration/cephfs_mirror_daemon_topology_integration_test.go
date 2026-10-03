//go:build integration && multicluster

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Tentacle describes multiple mirror instances but still labels that deployment
// untested. This PoC checks native MGR assignment and userspace CephFS snapshot
// bytes instead of treating two running containers as proof of native HA.
func TestMultiClusterCephFSMirrorDaemonTopology(t *testing.T) {
	requireCephFSNativeShuffleDiagnostic(t)
	testCephFSMirrorDaemonTopology(t, false)
}

func requireCephFSNativeShuffleDiagnostic(t *testing.T) {
	t.Helper()
	if os.Getenv("CEPH_TEST_CEPHFS_NATIVE_SHUFFLE") != "1" {
		t.Skip("native live expansion fails on Ceph 20.2.4: MGR reports 'DirectoryState' object is not subscriptable and leaves the added daemon with zero directories; set CEPH_TEST_CEPHFS_NATIVE_SHUFFLE=1 to recheck native redistribution")
	}
}

// This variant keeps native dead-owner failover unchanged, but requests an
// explicit fixture rebalance after expanding the surviving set with AddDaemon.
func TestMultiClusterCephFSMirrorDaemonRebalanceTopology(t *testing.T) {
	testCephFSMirrorDaemonTopology(t, false, true)
}

func testCephFSMirrorDaemonTopology(t *testing.T, host bool, explicitRebalance ...bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
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
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSMirrorDaemonScript), "/tmp/cephfs-mirror-daemons.py", 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(name string) {
		t.Helper()
		multiClusterExecOutput(t, ctx, sourceClient, "python3", "/tmp/cephfs-mirror-daemons.py", sourceFS.FilesystemName, "checkpoint", name)
	}
	checkpoint("initial")
	image := os.Getenv("CEPH_TEST_MIRROR_IMAGE")
	if image == "" {
		image, _ = integrationImages(t)
	}
	directories := []string{"/daemon-a", "/daemon-b", "/daemon-c", "/daemon-d"}
	mirror, err := multicluster.RunCephFSMirror(ctx, image, multicluster.CephFSMirrorConfig{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: directories, DaemonCount: 2,
	})
	if mirror != nil {
		t.Cleanup(func() {
			if t.Failed() {
				logCtx, logCancel := context.WithTimeout(context.Background(), 20*time.Second)
				for _, daemon := range mirror.Daemons() {
					multiClusterLogContainer(t, logCtx, daemon.Container)
				}
				for _, manager := range source.Managers() {
					if manager != nil && manager.Container != nil {
						multiClusterLogContainer(t, logCtx, manager.Container)
					}
				}
				for _, args := range [][]string{
					{"fs", "snapshot", "mirror", "show", "distribution", sourceFS.FilesystemName},
					{"fs", "snapshot", "mirror", "daemon", "status"},
				} {
					output, err := source.Ceph(logCtx, args...)
					t.Logf("final native mirror topology: %s error=%v", output, err)
				}
				logCancel()
			}
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate CephFS mirror daemon topology: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	daemons := mirror.Daemons()
	if len(daemons) != 2 || daemons[0].DaemonName != "a" || daemons[1].DaemonName != "b" || mirror.Container != daemons[0] {
		t.Fatalf("initial two-daemon inventory: %+v", daemons)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("shared mirror peer policy: peers=%v error=%v", peers, err)
	}
	peerID := peers[0]
	owners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 2)
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "initial")

	// Cross-check admin peer status against the native directory map. The
	// process selected for outage must own real directories before it is stopped.
	fsDescription := multiClusterExecOutput(t, ctx, source.ControlContainer(), "ceph", "fs", "get", sourceFS.FilesystemName, "--format", "json")
	var filesystem struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(fsDescription, &filesystem); err != nil || filesystem.ID <= 0 {
		t.Fatalf("source filesystem ID: data=%s error=%v", fsDescription, err)
	}
	admin := multiClusterExecOutput(t, ctx, daemons[0], "ceph", "--admin-daemon", "/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", sourceFS.FilesystemName, filesystem.ID), peerID)
	var ownedDirectories map[string]json.RawMessage
	if err := json.Unmarshal(admin, &ownedDirectories); err != nil || len(ownedDirectories) == 0 {
		t.Fatalf("first daemon owns no native directories: data=%s error=%v", admin, err)
	}
	stoppedID := ""
	for directory := range ownedDirectories {
		owner := owners[directory]
		if owner == "" || (stoppedID != "" && owner != stoppedID) {
			t.Fatalf("admin directory assignments disagree with native MGR map: admin=%s owners=%v", admin, owners)
		}
		stoppedID = owner
	}
	stopTimeout := 5 * time.Second
	if err := daemons[0].Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	owners = cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
	for directory, owner := range owners {
		if owner == stoppedID {
			t.Fatalf("directory %s still belongs to stopped native daemon %s", directory, stoppedID)
		}
	}
	checkpoint("owner-stopped")
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "owner-stopped")
	t.Logf("native CephFS mirror owner %s stopped: four directories reassigned to the surviving instance; new snapshots and exact bytes arrived", stoppedID)

	if err := mirror.RemoveDaemon(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if mirror.Container != nil || len(mirror.Daemons()) != 1 {
		t.Fatal("removing initial daemon did not clear its compatibility handle")
	}
	replacement, err := mirror.AddDaemon(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if replacement.GetContainerID() == daemons[0].GetContainerID() || mirror.Container != nil {
		t.Fatal("replacement reused the removed process or reassigned the legacy handle")
	}
	if len(explicitRebalance) != 0 && explicitRebalance[0] {
		if err := mirror.RebalanceDirectories(ctx); err != nil {
			t.Fatal(err)
		}
		t.Log("fixture-controlled CephFS directory rebalance after adding replacement: unregister/release/re-register owned policies while retaining peer and snapshot data")
	}
	cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 2)
	checkpoint("replacement")
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "replacement")

	// Deliberately remove every process without removing its peer or directory
	// configuration. A later AddDaemon resumes the same link and catches up.
	if err := mirror.RemoveDaemon(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
	if err := mirror.RemoveDaemon(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if len(mirror.Daemons()) != 0 {
		t.Fatal("zero-daemon outage retained a process in the inventory")
	}
	checkpoint("all-removed")
	cephFSAssertDaemonSnapshotsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "all-removed", 10*time.Second)
	currentPeers, err := mirror.PeerIDs(ctx)
	if err != nil || !slices.Equal(currentPeers, peers) {
		t.Fatalf("daemon replacement changed owned peer: peers=%v error=%v", currentPeers, err)
	}
	if _, err := mirror.AddDaemon(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "all-removed")
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "initial")
	t.Log("CephFS mirror daemon topology 2→1→2→1→0→1: shared auth/peer retained, native assignments converged, zero-instance backlog caught up, initial snapshot bytes preserved")
	if len(explicitRebalance) != 0 && explicitRebalance[0] && !host {
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := docker.Close(); err != nil {
				t.Error(err)
			}
		})
		daemon := mirror.Daemons()[0]
		before, err := docker.ContainerInspect(ctx, daemon.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		local := before.Container.NetworkSettings.Networks[source.NetworkName()]
		peer := before.Container.NetworkSettings.Networks[destination.NetworkName()]
		if local == nil || peer == nil || !local.IPAddress.IsValid() || !peer.IPAddress.IsValid() || !before.Container.State.Running || before.Container.State.Pid <= 0 {
			t.Fatal("resumed mirror process and both source/destination endpoints are not live")
		}
		beforeOwners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
		link, err := mirror.InterruptPeerLink(ctx, "c")
		if link != nil {
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
				defer cleanupCancel()
				if err := link.Restore(cleanupCtx); err != nil {
					t.Errorf("restore CephFS destination endpoint: %v", err)
				}
			})
		}
		if err != nil {
			t.Fatal(err)
		}
		if link.NetworkName != destination.NetworkName() || link.OriginalAddress != peer.IPAddress {
			t.Fatal("interruption did not identify the original destination-only endpoint")
		}
		isolated, err := docker.ContainerInspect(ctx, daemon.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		kept := isolated.Container.NetworkSettings.Networks[source.NetworkName()]
		if isolated.Container.NetworkSettings.Networks[destination.NetworkName()] != nil || kept == nil || kept.IPAddress != local.IPAddress || !slices.Equal(kept.Aliases, local.Aliases) || !isolated.Container.State.Running || isolated.Container.State.Pid != before.Container.State.Pid {
			t.Fatal("peer cut did not preserve the original source endpoint and same running process")
		}
		checkpoint("peer-isolated")
		cephFSAssertDaemonSnapshotsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "peer-isolated", 10*time.Second)
		if current, err := mirror.PeerIDs(ctx); err != nil || !slices.Equal(current, peers) {
			t.Fatalf("peer-only isolation changed filesystem policy: peers=%v error=%v", current, err)
		}
		afterOwners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
		for directory, owner := range beforeOwners {
			if afterOwners[directory] != owner {
				t.Fatalf("peer-only cut changed native assignment for %s: before=%s after=%s", directory, owner, afterOwners[directory])
			}
		}
		if err := link.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if err := link.Restore(ctx); err != nil {
			t.Fatalf("CephFS endpoint restore was not idempotent: %v", err)
		}
		restored, err := docker.ContainerInspect(ctx, daemon.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		restoredPeer := restored.Container.NetworkSettings.Networks[destination.NetworkName()]
		restoredSource := restored.Container.NetworkSettings.Networks[source.NetworkName()]
		if restoredPeer == nil || restoredPeer.IPAddress != peer.IPAddress || !slices.Equal(restoredPeer.Aliases, peer.Aliases) || restoredPeer.GwPriority != peer.GwPriority || restoredSource == nil || restoredSource.IPAddress != local.IPAddress || !restored.Container.State.Running || restored.Container.State.Pid != before.Container.State.Pid {
			t.Fatal("peer restoration changed endpoint IP/aliases/priority, source endpoint, or mirror PID")
		}
		cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "peer-isolated")
		t.Log("CephFS peer bridge interruption: process, source assignment and shared peer remained; remote snapshots stayed absent during isolation and exact bytes caught up after restoring the same endpoint")
	}
}

func cephFSWaitForMirrorDaemonAssignments(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peerID string, directories []string, daemonCount int) map[string]string {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	last := ""
	for {
		owners, detail, err := cephFSObserveMirrorDaemonAssignments(waitCtx, source, filesystem, peerID, directories, daemonCount)
		last = detail
		if err == nil {
			t.Logf("native CephFS mirror assignments (%d instances): %s", daemonCount, detail)
			return owners
		}
		last += " error=" + err.Error()
		select {
		case <-waitCtx.Done():
			t.Fatalf("native CephFS mirror assignments did not converge: %s", last)
		case <-time.After(time.Second):
		}
	}
}

func cephFSObserveMirrorDaemonAssignments(ctx context.Context, source *ceph.Container, filesystem, peerID string, directories []string, daemonCount int) (map[string]string, string, error) {
	distributionData, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "show", "distribution", filesystem)
	if err != nil {
		return nil, "", err
	}
	var distribution struct {
		Mapping map[string]string `json:"mapping"`
	}
	if err := json.Unmarshal(distributionData, &distribution); err != nil {
		return nil, string(distributionData), err
	}
	if len(distribution.Mapping) != daemonCount {
		return nil, string(distributionData), fmt.Errorf("native instance count=%d want=%d", len(distribution.Mapping), daemonCount)
	}
	for id, summary := range distribution.Mapping {
		count, err := strconv.Atoi(strings.TrimSuffix(summary, " directories"))
		if id == "" || err != nil || count <= 0 {
			return nil, string(distributionData), fmt.Errorf("instance %q has no positive assignment: %q", id, summary)
		}
	}
	owners := make(map[string]string, len(directories))
	for _, directory := range directories {
		data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", filesystem, directory)
		if err != nil {
			return nil, string(distributionData), err
		}
		var mapping struct {
			State      string `json:"state"`
			InstanceID string `json:"instance_id"`
		}
		if err := json.Unmarshal(data, &mapping); err != nil {
			return nil, string(data), err
		}
		if _, exists := distribution.Mapping[mapping.InstanceID]; mapping.State != "mapped" || !exists {
			return nil, string(data), fmt.Errorf("directory %s is not mapped to a current native instance", directory)
		}
		owners[directory] = mapping.InstanceID
	}
	statusData, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "daemon", "status")
	if err != nil {
		return nil, string(distributionData), err
	}
	var statuses []struct {
		DaemonID    uint64 `json:"daemon_id"`
		Filesystems []struct {
			Name  string `json:"name"`
			Peers []struct {
				UUID string `json:"uuid"`
			} `json:"peers"`
		} `json:"filesystems"`
	}
	if err := json.Unmarshal(statusData, &statuses); err != nil {
		return nil, string(statusData), err
	}
	registered := make(map[string]bool)
	for _, status := range statuses {
		id := strconv.FormatUint(status.DaemonID, 10)
		for _, fs := range status.Filesystems {
			if fs.Name != filesystem {
				continue
			}
			for _, peer := range fs.Peers {
				if peer.UUID == peerID {
					registered[id] = true
				}
			}
		}
	}
	// Daemon service sessions and per-filesystem watchers use separate RADOS
	// GIDs. Directory mappings above refer to watchers; service status only
	// proves shared peer discovery and may retain a stopped service briefly.
	if len(registered) < daemonCount {
		return nil, string(statusData), fmt.Errorf("registered native services with shared peer=%d want at least=%d", len(registered), daemonCount)
	}
	return owners, string(distributionData), nil
}

func cephFSWaitForDaemonSnapshots(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	last := ""
	for {
		code, reader, err := client.Exec(waitCtx, []string{"python3", "/tmp/cephfs-mirror-daemons.py", filesystem, "verify", snapshot}, tcexec.Multiplexed())
		if err == nil {
			output, readErr := io.ReadAll(reader)
			last = string(output)
			if readErr == nil && code == 0 {
				return
			}
		} else {
			last = err.Error()
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("CephFS daemon snapshot %s did not deliver four directories' exact bytes: %s", snapshot, last)
		case <-time.After(time.Second):
		}
	}
}

func cephFSAssertDaemonSnapshotsAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for {
		multiClusterExecOutput(t, ctx, client, "python3", "/tmp/cephfs-mirror-daemons.py", filesystem, "absent", snapshot)
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("check CephFS zero-daemon outage: %v", ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

const cephFSMirrorDaemonScript = `import cephfs
import os
import sys

filesystem, phase, snapshot = sys.argv[1:]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())
directories = ['/daemon-a', '/daemon-b', '/daemon-c', '/daemon-d']
def payload(directory):
    return (snapshot + ':' + directory + ':').encode() * 1024 + bytes(range(256))
try:
    for directory in directories:
        root = directory.encode()
        path = root + b'/.snap/' + snapshot.encode()
        if phase == 'checkpoint':
            try:
                fs.mkdir(root, 0o755)
            except cephfs.ObjectExists:
                pass
            fd = fs.open(root + b'/payload', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
            try:
                data = payload(directory)
                assert fs.write(fd, data, 0) == len(data)
                fs.fsync(fd, False)
            finally:
                fs.close(fd)
            fs.sync_fs()
            fs.mkdir(path, 0o755)
        elif phase == 'verify':
            fd = fs.open(path + b'/payload', os.O_RDONLY)
            try:
                expected = payload(directory)
                actual = fs.read(fd, 0, len(expected) + 1)
                assert actual == expected, 'snapshot byte mismatch: ' + directory
            finally:
                fs.close(fd)
        elif phase == 'absent':
            try:
                fs.stat(path)
            except cephfs.ObjectNotFound:
                continue
            raise AssertionError('unexpected snapshot while every mirror process removed: ' + directory)
        else:
            raise AssertionError('unknown phase: ' + phase)
    print(phase + ' ' + snapshot + ': four directory snapshots checked')
finally:
    fs.shutdown()
`
