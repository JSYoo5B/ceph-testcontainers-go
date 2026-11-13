//go:build (all || (integration && multicluster)) && native_regression && (!ci || (ci_optional && (!ci_batch || ci_batch_native_shuffle)))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_mirror_daemon_redistribution_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// image is the Ceph image the case runs on, for both clusters, their client
// containers and the mirror daemons: Tentacle 20.2.4, which reproduces the
// defect. A run selects another image with
//
//	go test -ldflags "-X github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/experiments/cephfs_mirror_daemon_redistribution_test.image=<image>"
//
// No fixed release is known: no upstream fix is recorded, and Tentacle
// 20.2.5 and Umbrella 21.2.0 reproduce it too.
var image = "ghcr.io/jsyoo5b/ceph-testcontainers-images@sha256:2e57651a5aa7a6791b2b6c67d63439a307e50a0f560f4c98323e2a17a7a9ab4b"

// Tentacle describes multiple mirror instances but still labels that deployment
// untested. This PoC checks native MGR assignment and userspace CephFS snapshot
// bytes instead of treating two running containers as proof of native HA.
func TestMultiClusterCephFSMirrorDaemonTopology(t *testing.T) {
	testDaemonRedistribution(t, "bridge", false)
}

// testDaemonRedistribution reports whether the MGR mirroring module fails to
// spread already assigned directories over the daemons when a daemon joins.
//
// Given two clusters with one filesystem each, a mirror link with two daemons
// and four directories, and the controls (the initial snapshots arrive with
// exact bytes and the admin socket agrees with the MGR map; when one daemon
// stops, its directories move to the survivor and new snapshots arrive),
// when the stopped daemon is removed and a replacement joins, which the MGR
// sees registered with the peer, then the defect is REPRODUCED if the MGR
// does not spread the directories over both daemons within three minutes,
// and NOT_REPRODUCED if it does and the next snapshots arrive. The case never
// calls RebalanceDirectories. Tentacle 20.2.4 reproduces it.
//
// A failure of the given or when steps fails the test, and so does a
// redistribution after which the snapshots do not arrive; the result does
// not.
func testDaemonRedistribution(t *testing.T, network string, host bool) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	t.Logf("CEPHFS_MIRROR_DAEMON_REDISTRIBUTION image=%s network=%s", image, network)
	opts := []testcontainers.ContainerCustomizer{harness.FunctionalOSDConfig(t), ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	clusters := make([]*ceph.Container, 2)
	clients := make([]testcontainers.Container, 2)
	fsids := make([]string, 2)
	for i := range clusters {
		cluster, err := ceph.Run(ctx, image, opts...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		status, err := harness.ObserveCluster(cluster).Status(ctx)
		if err != nil {
			t.Fatal(err)
		}
		client, err := testcontainers.Run(ctx, image, cluster.WithClient(), ceph.WithIdleEntrypoint(),
			testcontainers.WithWaitStrategy(wait.ForExec([]string{"ceph", "--connect-timeout", "5", "status"})))
		if client != nil {
			testcontainers.CleanupContainer(t, client)
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[i], clients[i], fsids[i] = cluster, client, status.FSID
	}
	if fsids[0] == "" || fsids[0] == fsids[1] {
		t.Fatalf("source and destination are not independent Ceph clusters: FSIDs %v", fsids)
	}
	source, destination, sourceClient, destinationClient := clusters[0], clusters[1], clients[0], clients[1]
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range clusters {
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expected := harness.CephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	for _, client := range clients {
		if err := client.CopyToContainer(ctx, []byte(harness.CephFSMirrorDaemonScript), "/tmp/cephfs-mirror-daemons.py", 0o600); err != nil {
			t.Fatal(err)
		}
	}
	directories := []string{"/daemon-a", "/daemon-b", "/daemon-c", "/daemon-d"}
	checkpoint := func(name string) map[string]cephfsmirror.Snapshot {
		t.Helper()
		harness.MultiClusterExecOutput(t, ctx, sourceClient, "python3", "/tmp/cephfs-mirror-daemons.py", sourceFS.FilesystemName, "checkpoint", name)
		checkpoints := make(map[string]cephfsmirror.Snapshot, len(directories))
		for _, directory := range directories {
			checkpoints[directory] = harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-mirror-daemons.py", directory, name)
		}
		return checkpoints
	}
	initial := checkpoint("initial")
	mirror, err := cephfsmirror.Run(ctx, image, cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: directories, DaemonCount: 2,
	}))
	if mirror != nil {
		t.Cleanup(func() {
			if t.Failed() {
				logCtx, logCancel := context.WithTimeout(context.Background(), 20*time.Second)
				for _, daemon := range mirror.Daemons() {
					harness.MultiClusterLogContainer(t, logCtx, daemon.Container)
				}
				for _, manager := range clusteraccess.Managers(t, source) {
					if manager != nil && manager.Container != nil {
						harness.MultiClusterLogContainer(t, logCtx, manager.Container)
					}
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
	expected.PeerID = peerID
	waitCheckpoints := func(checkpoints map[string]cephfsmirror.Snapshot, owners map[string]string, stoppedMember string) {
		t.Helper()
		for _, directory := range directories {
			status := harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-mirror-daemons.py", expected, directory, checkpoints[directory])
			if status.InstanceID != owners[directory] {
				t.Fatalf("public CephFS directory owner disagrees with independent native dirmap: directory=%s status=%+v owners=%v", directory, status, owners)
			}
			if stoppedMember != "" {
				if status.DaemonName == stoppedMember || status.DaemonProblems[stoppedMember] == "" {
					t.Fatalf("public CephFS checkpoint lost unrelated stopped member: directory=%s status=%+v", directory, status)
				}
				observed, observationErr := mirror.DirectoryStatus(ctx, directory)
				if observationErr == nil || !observed.Ready || observed.DaemonProblems[stoppedMember] == "" || observed.DaemonName == stoppedMember || observed.InstanceID != owners[directory] {
					t.Fatalf("CephFS partial member status did not retain live selected owner and stopped member problem: status=%+v error=%v", observed, observationErr)
				}
				harness.CephFSAssertObservedIdentity(t, observed, expected, directory)
			}
		}
	}

	// Control: the initial snapshots arrive, and the admin socket of the
	// first daemon agrees with the MGR map.
	owners := harness.CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 2)
	waitCheckpoints(initial, owners, "")
	harness.CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "initial")
	fsDescription := harness.MultiClusterExecOutput(t, ctx, clusteraccess.ControlContainer(t, source), "ceph", "fs", "get", sourceFS.FilesystemName, "--format", "json")
	var filesystem struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(fsDescription, &filesystem); err != nil || filesystem.ID <= 0 {
		t.Fatalf("source filesystem ID: data=%s error=%v", fsDescription, err)
	}
	admin := harness.MultiClusterExecOutput(t, ctx, daemons[0], "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", sourceFS.FilesystemName, filesystem.ID), peerID)
	ownedDirectories, err := harness.CephFSPeerStatusDirectories(admin, peerID)
	if err != nil || len(ownedDirectories) == 0 {
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

	// Failover control: the first daemon stops and the survivor takes its
	// directories.
	stopTimeout := 5 * time.Second
	if err := daemons[0].Stop(ctx, &stopTimeout); err != nil {
		t.Fatal(err)
	}
	owners = harness.CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
	for directory, owner := range owners {
		if owner == stoppedID {
			t.Fatalf("directory %s still belongs to stopped native daemon %s", directory, stoppedID)
		}
	}
	ownerStopped := checkpoint("owner-stopped")
	waitCheckpoints(ownerStopped, owners, "a")
	harness.CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "owner-stopped")
	t.Logf("native CephFS mirror owner %s stopped: four directories reassigned to the surviving instance; new snapshots and exact bytes arrived", stoppedID)

	// When: the stopped daemon is removed and a replacement joins.
	registeredBefore := registeredServices(t, ctx, source, sourceFS.FilesystemName, peerID)
	if err := daemons[0].Remove(ctx); err != nil {
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

	// Then: within the wait that the fixture gives an assignment, the MGR
	// spreads the four directories over both daemons, or it keeps them on
	// the survivor although the replacement registered with the peer.
	joined, spread, distribution := false, false, ""
	deadline := time.Now().Add(3 * time.Minute)
	for !spread && time.Now().Before(deadline) {
		if !joined {
			for id := range registeredServices(t, ctx, source, sourceFS.FilesystemName, peerID) {
				joined = joined || !registeredBefore[id]
			}
		}
		spread, distribution = spreadOverTwo(t, ctx, source, sourceFS.FilesystemName)
		if !spread {
			select {
			case <-ctx.Done():
				t.Fatalf("wait for the redistribution: %v", ctx.Err())
			case <-time.After(time.Second):
			}
		}
	}
	t.Logf("CEPHFS_MIRROR_DAEMON_REDISTRIBUTION_RECEIPT replacement registered: %t; directories spread over two instances: %t; distribution: %s", joined, spread, distribution)
	var symptoms []string
	switch {
	case spread:
		owners = harness.CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 2)
		replacementSnapshots := checkpoint("replacement")
		waitCheckpoints(replacementSnapshots, owners, "")
		harness.CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "replacement")
	case !joined:
		t.Fatalf("condition not established: the replacement daemon did not register with peer %s within three minutes (registered before: %v)", peerID, registeredBefore)
	default:
		symptoms = append(symptoms, "after the replacement joined, the MGR kept the directories as "+distribution)
		symptoms = append(symptoms, managerErrors(t, source)...)
	}
	if t.Failed() {
		return
	}
	result := "NOT_REPRODUCED"
	if len(symptoms) > 0 {
		result = "REPRODUCED"
	}
	t.Logf("EXPERIMENT_RESULT case=cephfs_mirror_daemon_redistribution/%s result=%s image=%s symptoms=%q", network, result, image, symptoms)
}

// registeredServices returns the daemon IDs of the mirror services that the
// MGR lists with peer peerID on filesystem.
func registeredServices(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peerID string) map[string]bool {
	t.Helper()
	data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "daemon", "status")
	if err != nil {
		t.Fatalf("mirror daemon status: %v", err)
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
	if err := json.Unmarshal(data, &statuses); err != nil {
		t.Fatalf("mirror daemon status %s: %v", data, err)
	}
	registered := make(map[string]bool)
	for _, status := range statuses {
		for _, fs := range status.Filesystems {
			for _, peer := range fs.Peers {
				if fs.Name == filesystem && peer.UUID == peerID {
					registered[strconv.FormatUint(status.DaemonID, 10)] = true
				}
			}
		}
	}
	return registered
}

// spreadOverTwo reports whether the MGR's directory distribution names two
// instances with at least one directory each, and returns the distribution.
func spreadOverTwo(t *testing.T, ctx context.Context, source *ceph.Container, filesystem string) (bool, string) {
	t.Helper()
	data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "show", "distribution", filesystem)
	if err != nil {
		t.Fatalf("mirror distribution: %v", err)
	}
	var distribution struct {
		Mapping map[string]string `json:"mapping"`
	}
	if err := json.Unmarshal(data, &distribution); err != nil {
		t.Fatalf("mirror distribution %s: %v", data, err)
	}
	if len(distribution.Mapping) != 2 {
		return false, string(data)
	}
	for _, summary := range distribution.Mapping {
		if count, err := strconv.Atoi(strings.TrimSuffix(summary, " directories")); err != nil || count <= 0 {
			return false, string(data)
		}
	}
	return true, string(data)
}

// managerErrors returns the lines of the source MGR logs that name the
// mirroring module's DirectoryState error.
func managerErrors(t *testing.T, source *ceph.Container) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var found []string
	for _, manager := range clusteraccess.Managers(t, source) {
		if manager == nil || manager.Container == nil {
			continue
		}
		logs, err := manager.Logs(ctx)
		if err != nil {
			t.Logf("MGR log unavailable: %v", err)
			continue
		}
		scanner := bufio.NewScanner(logs)
		scanner.Buffer(make([]byte, 64<<10), 1<<20)
		for scanner.Scan() {
			if line := scanner.Text(); strings.Contains(line, "'DirectoryState' object is not subscriptable") && len(found) < 3 {
				found = append(found, "MGR log: "+strings.TrimSpace(line))
			}
		}
		if err := logs.Close(); err != nil {
			t.Logf("close MGR log: %v", err)
		}
	}
	return found
}
