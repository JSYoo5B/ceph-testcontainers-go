//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

func testCephFSManagerTopology(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithOSDCount(1)}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		for index, cluster := range []*ceph.Container{source, destination} {
			statusCtx, statusCancel := context.WithTimeout(context.Background(), 5*time.Second)
			status, err := cluster.ManagerStatus(statusCtx)
			statusCancel()
			t.Logf("cluster %d final native MGR map: %+v error=%v", index, status, err)
			for _, manager := range cluster.Managers() {
				if manager == nil || manager.Container == nil {
					continue
				}
				logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Logf("cluster %d manager %s running=%t", index, manager.DaemonName, manager.IsRunning())
				multiClusterLogContainer(t, logCtx, manager.Container)
				logCancel()
			}
		}
	})
	if _, err := source.AddManager(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if err := source.RemoveManager(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if source.ManagerContainer() != nil {
		t.Fatal("initial manager a retained its legacy container handle after removal")
	}
	if _, err := source.AddManager(ctx, "c"); err != nil {
		t.Fatal(err)
	}
	status, err := source.ManagerStatus(ctx)
	if err != nil || !status.Available || status.ActiveName != "b" || len(status.Standbys) != 1 {
		t.Fatalf("source surviving manager topology: %+v error=%v", status, err)
	}
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
		if err := client.CopyToContainer(ctx, []byte(cephFSInterClusterScript), "/tmp/cephfs-intercluster.py", 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
		t.Helper()
		multiClusterExecOutput(t, ctx, client, append([]string{"python3", "/tmp/cephfs-intercluster.py", filesystem}, args...)...)
	}
	archiveSnapshot := func(snapshot string) {
		t.Helper()
		archivePath := "/tmp/" + snapshot + ".json"
		fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/"+snapshot, archivePath)
		if err := destinationClient.CopyToContainer(ctx, multiClusterReadFile(t, ctx, sourceClient, archivePath), archivePath, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	verifySnapshot := func(snapshot string) {
		t.Helper()
		cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, snapshot, true)
		fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/"+snapshot, "/tmp/"+snapshot+".json")
	}
	fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
	archiveSnapshot("backup-1")
	image := source.ControlImage()
	mirror, err := multicluster.RunCephFSMirror(ctx, image, multicluster.CephFSMirrorConfig{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{"/federation"},
	})
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() {
				multiClusterLogContainer(t, cleanupCtx, mirror.Container)
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate CephFS manager topology mirror: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cephFSAssertManagerPeerNetworks(t, ctx, source, destination.NetworkName(), true)
	verifySnapshot("backup-1")
	t.Log("CephFS mirror constructed after removing initial manager a; surviving b and standby c have native peer connectivity")

	if _, err := source.AddManager(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	if err := mirror.AttachManagers(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSAssertManagerPeerNetworks(t, ctx, source, destination.NetworkName(), true)
	if err := source.RemoveManager(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	status, err = source.ManagerStatus(ctx)
	if err != nil || !status.Available || (status.ActiveName != "c" && status.ActiveName != "d") {
		t.Fatalf("source manager replacement was not a prepared standby: %+v error=%v", status, err)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("source CephFS peers after manager replacement: %v error=%v", peers, err)
	}
	oldPeer := peers[0]
	if err := mirror.RemovePeer(ctx, oldPeer); err != nil {
		t.Fatal(err)
	}
	cephFSWaitForDaemonPolicy(t, ctx, source, mirror, 1, oldPeer)
	fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "manager-failover", "manager-failover")
	archiveSnapshot("manager-failover")
	newPeer, err := mirror.RebootstrapPeer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	peers, err = mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 || peers[0] != newPeer {
		t.Fatalf("reimported CephFS peer: %v want=%s error=%v", peers, newPeer, err)
	}
	verifySnapshot("manager-failover")
	verifySnapshot("backup-1")
	t.Logf("late manager d reconciled, active b removed, %s imported a fresh peer and mirrored new snapshot bytes while original destination snapshot survived", status.ActiveName)

	if err := mirror.Terminate(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSAssertManagerPeerNetworks(t, ctx, source, destination.NetworkName(), false)
	if _, err := source.Status(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.Status(ctx); err != nil {
		t.Fatal(err)
	}
	if host {
		t.Log("host mirror required no extra manager attachments before or after failover; cleanup preserved both cluster control planes")
	} else {
		t.Log("mirror cleanup removed its current manager attachments, tolerated removed b, and preserved both cluster control planes")
	}
}

func cephFSAssertManagerPeerNetworks(t *testing.T, ctx context.Context, source *ceph.Container, peerNetwork string, expected bool) {
	t.Helper()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer docker.Close()
	for _, manager := range source.Managers() {
		inspection, err := docker.ContainerInspect(ctx, manager.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if inspection.Container.NetworkSettings == nil {
			t.Fatalf("manager %s has no network settings", manager.DaemonName)
		}
		if source.UsesHostNetwork() {
			if inspection.Container.HostConfig == nil || inspection.Container.HostConfig.NetworkMode != "host" {
				t.Fatalf("manager %s is outside the shared host network namespace", manager.DaemonName)
			}
			for name := range inspection.Container.NetworkSettings.Networks {
				if name != "host" {
					t.Fatalf("host manager %s unexpectedly acquired network attachment %s", manager.DaemonName, name)
				}
			}
			continue
		}
		_, attached := inspection.Container.NetworkSettings.Networks[peerNetwork]
		if attached != expected {
			t.Fatalf("manager %s peer network attachment=%v want=%v", manager.DaemonName, attached, expected)
		}
	}
}
