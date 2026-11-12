//go:build all || (integration && multicluster)

package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// RunCephFSMirrorDaemonTopology runs the mirror daemon topology 2→1→2→1→0→1
// and fails t at the first step that does not hold. With explicitRebalance
// the fixture's RebalanceDirectories follows the AddDaemon of the
// replacement, and the bridge variant also interrupts the peer link.
func RunCephFSMirrorDaemonTopology(t *testing.T, host bool, explicitRebalance ...bool) {
	t.Helper()
	runCephFSMirrorDaemonTopology(t, host, len(explicitRebalance) != 0 && explicitRebalance[0], nil)
}

// CephFSMirrorRedistributionJudge decides one native reading taken after a
// replacement daemon joined: the directories, the instance that held all of
// them after the failover, the MGR distribution (instance to "N
// directories"), each directory's mapped instance ("" when it is not
// mapped) and the number of services registered with the shared peer. It
// reports the symptom when the directories were not redistributed and its
// absence when they were; its error, whose text starts with the category,
// leaves no verdict. The scenario repeats the reading until the judge
// reports the absence or the wait ends, and then takes the last judgement.
type CephFSMirrorRedistributionJudge func(directories []string, survivor string, distribution, owners map[string]string, registered int) (symptom bool, detail string, err error)

// CephFSMirrorRedistribution is the reproduction of the scenario's
// redistribution step: its unit, its planned observation and its judge.
type CephFSMirrorRedistribution struct {
	Reproduction *Reproduction
	Observation  string
	Judge        CephFSMirrorRedistributionJudge
}

// RunCephFSMirrorDaemonRedistribution runs the same topology without the
// fixture's rebalance and reports the redistribution after the replacement
// joins as the reproduction's observation, which it concludes. Before that a
// failed step reports INCONCLUSIVE; after an observed symptom the scenario
// stops, and after an absent one it continues and fails t as usual.
func RunCephFSMirrorDaemonRedistribution(t *testing.T, host bool, redistribution CephFSMirrorRedistribution) {
	t.Helper()
	runCephFSMirrorDaemonTopology(t, host, false, &redistribution)
}

func runCephFSMirrorDaemonTopology(t *testing.T, host, explicitRebalance bool, redistribution *CephFSMirrorRedistribution) {
	t.Helper()
	// fail stops the scenario. Until the redistribution was observed it
	// reports the reproduction INCONCLUSIVE with the category; otherwise it
	// fails t with the message alone.
	fail := func(category, format string, args ...any) {
		t.Helper()
		if redistribution != nil {
			redistribution.Reproduction.Inconclusive(t, "%s: %s", category, fmt.Sprintf(format, args...))
		}
		t.Fatalf(format, args...)
	}
	const (
		prerequisite = "unavailable prerequisite"
		condition    = "condition not established"
		control      = "control failed"
	)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := NewMultiClusterPair(t, opts...)
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		fail(prerequisite, "%v", err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		fail(prerequisite, "%v", err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			fail(prerequisite, "%v", err)
		}
	}
	expected := CephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(CephFSMirrorDaemonScript), "/tmp/cephfs-mirror-daemons.py", 0o600); err != nil {
			fail(prerequisite, "%v", err)
		}
	}
	directories := []string{"/daemon-a", "/daemon-b", "/daemon-c", "/daemon-d"}
	checkpoint := func(name string) map[string]cephfsmirror.Snapshot {
		t.Helper()
		MultiClusterExecOutput(t, ctx, sourceClient, "python3", "/tmp/cephfs-mirror-daemons.py", sourceFS.FilesystemName, "checkpoint", name)
		checkpoints := make(map[string]cephfsmirror.Snapshot, len(directories))
		for _, directory := range directories {
			checkpoints[directory] = CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, "/tmp/cephfs-mirror-daemons.py", directory, name)
		}
		return checkpoints
	}
	initial := checkpoint("initial")
	image := StaticFacts(source).Images.CephFS
	mirror, err := cephfsmirror.Run(ctx, NativeMirrorImage(t, "cephfs", image), cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: directories, DaemonCount: 2,
	}))
	if mirror != nil {
		t.Cleanup(func() {
			if t.Failed() {
				logCtx, logCancel := context.WithTimeout(context.Background(), 20*time.Second)
				for _, daemon := range mirror.Daemons() {
					MultiClusterLogContainer(t, logCtx, daemon.Container)
				}
				for _, manager := range clusteraccess.Managers(t, source) {
					if manager != nil && manager.Container != nil {
						MultiClusterLogContainer(t, logCtx, manager.Container)
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
		fail(prerequisite, "%v", err)
	}
	daemons := mirror.Daemons()
	if len(daemons) != 2 || daemons[0].DaemonName != "a" || daemons[1].DaemonName != "b" || mirror.Container != daemons[0] {
		fail(condition, "initial two-daemon inventory: %+v", daemons)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		fail(condition, "shared mirror peer policy: peers=%v error=%v", peers, err)
	}
	peerID := peers[0]
	expected.PeerID = peerID
	waitCheckpoints := func(checkpoints map[string]cephfsmirror.Snapshot, owners map[string]string, stoppedMember string) {
		t.Helper()
		for _, directory := range directories {
			status := CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, "/tmp/cephfs-mirror-daemons.py", expected, directory, checkpoints[directory])
			if status.InstanceID != owners[directory] {
				fail(control, "public CephFS directory owner disagrees with independent native dirmap: directory=%s status=%+v owners=%v", directory, status, owners)
			}
			if stoppedMember != "" {
				if status.DaemonName == stoppedMember || status.DaemonProblems[stoppedMember] == "" {
					fail(control, "public CephFS checkpoint lost unrelated stopped member: directory=%s status=%+v", directory, status)
				}
				observed, observationErr := mirror.DirectoryStatus(ctx, directory)
				if observationErr == nil || !observed.Ready || observed.DaemonProblems[stoppedMember] == "" || observed.DaemonName == stoppedMember || observed.InstanceID != owners[directory] {
					fail(control, "CephFS partial member status did not retain live selected owner and stopped member problem: status=%+v error=%v", observed, observationErr)
				}
				CephFSAssertObservedIdentity(t, observed, expected, directory)
			}
		}
	}
	owners := CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 2)
	waitCheckpoints(initial, owners, "")
	CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "initial")

	// Cross-check admin peer status against the native directory map. The
	// process selected for outage must own real directories before it is stopped.
	fsDescription := MultiClusterExecOutput(t, ctx, clusteraccess.ControlContainer(t, source), "ceph", "fs", "get", sourceFS.FilesystemName, "--format", "json")
	var filesystem struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(fsDescription, &filesystem); err != nil || filesystem.ID <= 0 {
		fail(condition, "source filesystem ID: data=%s error=%v", fsDescription, err)
	}
	admin := MultiClusterExecOutput(t, ctx, daemons[0], "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", sourceFS.FilesystemName, filesystem.ID), peerID)
	ownedDirectories, err := CephFSPeerStatusDirectories(admin, peerID)
	if err != nil || len(ownedDirectories) == 0 {
		fail(control, "first daemon owns no native directories: data=%s error=%v", admin, err)
	}
	stoppedID := ""
	for directory := range ownedDirectories {
		owner := owners[directory]
		if owner == "" || (stoppedID != "" && owner != stoppedID) {
			fail(control, "admin directory assignments disagree with native MGR map: admin=%s owners=%v", admin, owners)
		}
		stoppedID = owner
	}
	stopTimeout := 5 * time.Second
	if err := daemons[0].Stop(ctx, &stopTimeout); err != nil {
		fail(condition, "%v", err)
	}
	owners = CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
	survivor := ""
	for directory, owner := range owners {
		if owner == stoppedID {
			fail(control, "directory %s still belongs to stopped native daemon %s", directory, stoppedID)
		}
		survivor = owner
	}
	ownerStopped := checkpoint("owner-stopped")
	waitCheckpoints(ownerStopped, owners, "a")
	CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "owner-stopped")
	t.Logf("native CephFS mirror owner %s stopped: four directories reassigned to the surviving instance; new snapshots and exact bytes arrived", stoppedID)

	if err := daemons[0].Remove(ctx); err != nil {
		fail(condition, "%v", err)
	}
	if mirror.Container != nil || len(mirror.Daemons()) != 1 {
		fail(condition, "removing initial daemon did not clear its compatibility handle")
	}
	replacement, err := mirror.AddDaemon(ctx, "a")
	if err != nil {
		fail(condition, "%v", err)
	}
	if replacement.GetContainerID() == daemons[0].GetContainerID() || mirror.Container != nil {
		fail(condition, "replacement reused the removed process or reassigned the legacy handle")
	}
	if explicitRebalance {
		if err := mirror.RebalanceDirectories(ctx); err != nil {
			t.Fatal(err)
		}
		t.Log("fixture-controlled CephFS directory rebalance after adding replacement: unregister/release/re-register owned policies while retaining peer and snapshot data")
	}
	if redistribution != nil {
		var redistributed bool
		owners, redistributed = cephFSObserveRedistribution(t, ctx, source, sourceFS.FilesystemName, peerID, directories, survivor, *redistribution)
		// The verdict is reported; a later step fails t on its own.
		redistribution = nil
		if !redistributed {
			return
		}
	} else {
		owners = CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 2)
	}
	replacementSnapshots := checkpoint("replacement")
	waitCheckpoints(replacementSnapshots, owners, "")
	CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "replacement")

	// Deliberately remove every process without removing its peer or directory
	// configuration. A later AddDaemon resumes the same link and catches up.
	if err := daemons[1].Remove(ctx); err != nil {
		t.Fatal(err)
	}
	CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
	if err := replacement.Remove(ctx); err != nil {
		t.Fatal(err)
	}
	if len(mirror.Daemons()) != 0 {
		t.Fatal("zero-daemon outage retained a process in the inventory")
	}
	allRemoved := checkpoint("all-removed")
	CephFSAssertDaemonSnapshotsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "all-removed", 10*time.Second)
	currentPeers, err := mirror.PeerIDs(ctx)
	if err != nil || !slices.Equal(currentPeers, peers) {
		t.Fatalf("daemon replacement changed owned peer: peers=%v error=%v", currentPeers, err)
	}
	resumed, err := mirror.AddDaemon(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	owners = CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1, slices.Collect(maps.Values(owners))...)
	waitCheckpoints(allRemoved, owners, "")
	CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "all-removed")
	CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "initial")
	t.Log("CephFS mirror daemon topology 2→1→2→1→0→1: shared auth/peer retained, native assignments converged, zero-instance backlog caught up, initial snapshot bytes preserved")
	if explicitRebalance && !host {
		docker, err := testcontainers.NewDockerClientWithOpts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := docker.Close(); err != nil {
				t.Error(err)
			}
		})
		before, err := docker.ContainerInspect(ctx, resumed.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		local := before.Container.NetworkSettings.Networks[StaticFacts(source).NetworkName]
		peer := before.Container.NetworkSettings.Networks[StaticFacts(destination).NetworkName]
		if local == nil || peer == nil || !local.IPAddress.IsValid() || !peer.IPAddress.IsValid() || !before.Container.State.Running || before.Container.State.Pid <= 0 {
			t.Fatal("resumed mirror process and both source/destination endpoints are not live")
		}
		beforeOwners := CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
		link, err := resumed.InterruptPeerLink(ctx)
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
		if link.NetworkName != StaticFacts(destination).NetworkName || link.OriginalAddress != peer.IPAddress {
			t.Fatal("interruption did not identify the original destination-only endpoint")
		}
		isolated, err := docker.ContainerInspect(ctx, resumed.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		kept := isolated.Container.NetworkSettings.Networks[StaticFacts(source).NetworkName]
		if isolated.Container.NetworkSettings.Networks[StaticFacts(destination).NetworkName] != nil || kept == nil || kept.IPAddress != local.IPAddress || !slices.Equal(kept.Aliases, local.Aliases) || !isolated.Container.State.Running || isolated.Container.State.Pid != before.Container.State.Pid {
			t.Fatal("peer cut did not preserve the original source endpoint and same running process")
		}
		peerIsolated := checkpoint("peer-isolated")
		CephFSAssertDaemonSnapshotsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "peer-isolated", 10*time.Second)
		if current, err := mirror.PeerIDs(ctx); err != nil || !slices.Equal(current, peers) {
			t.Fatalf("peer-only isolation changed filesystem policy: peers=%v error=%v", current, err)
		}
		afterOwners := CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, peerID, directories, 1)
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
		restored, err := docker.ContainerInspect(ctx, resumed.GetContainerID(), mobycl.ContainerInspectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		restoredPeer := restored.Container.NetworkSettings.Networks[StaticFacts(destination).NetworkName]
		restoredSource := restored.Container.NetworkSettings.Networks[StaticFacts(source).NetworkName]
		if restoredPeer == nil || restoredPeer.IPAddress != peer.IPAddress || !slices.Equal(restoredPeer.Aliases, peer.Aliases) || restoredPeer.GwPriority != peer.GwPriority || restoredSource == nil || restoredSource.IPAddress != local.IPAddress || !restored.Container.State.Running || restored.Container.State.Pid != before.Container.State.Pid {
			t.Fatal("peer restoration changed endpoint IP/aliases/priority, source endpoint, or mirror PID")
		}
		waitCheckpoints(peerIsolated, beforeOwners, "")
		CephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "peer-isolated")
		t.Log("CephFS peer bridge interruption: process, source assignment and shared peer remained; remote snapshots stayed absent during isolation and exact bytes caught up after restoring the same endpoint")
	}
}

// Retired lists instance IDs of processes that have already been removed or
// restarted. The manager can keep reporting such an instance until it notices
// the loss, and a replacement can leave the instance count unchanged; without
// this, a stale map with the expected count would satisfy the wait.
func CephFSWaitForMirrorDaemonAssignments(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peerID string, directories []string, daemonCount int, retired ...string) map[string]string {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	last := ""
	for {
		owners, detail, err := cephFSObserveMirrorDaemonAssignments(waitCtx, source, filesystem, peerID, directories, daemonCount)
		for _, owner := range owners {
			if err == nil && slices.Contains(retired, owner) {
				err = fmt.Errorf("native map still reports retired instance %s", owner)
			}
		}
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

// cephFSObserveRedistribution repeats the native reading after a replacement
// joined until the judge reports the absence of the symptom or three minutes
// pass, records the last judgement as the observation and concludes the
// reproduction. It reports whether the directories were redistributed, with
// their owners; an error of the judge reports INCONCLUSIVE and stops t.
func cephFSObserveRedistribution(t *testing.T, ctx context.Context, source *ceph.Container, filesystem, peerID string, directories []string, survivor string, r CephFSMirrorRedistribution) (map[string]string, bool) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last cephFSMirrorAssignments
	symptom, detail, err := false, "", error(fmt.Errorf("another fault: no native reading of the assignments completed"))
	for {
		reading, readErr := cephFSReadMirrorDaemonAssignments(waitCtx, source, filesystem, peerID, directories)
		switch {
		case readErr == nil:
			last = reading
			symptom, detail, err = r.Judge(directories, survivor, reading.distribution, reading.owners, len(reading.registered))
			if err == nil && !symptom {
				t.Logf("native CephFS mirror assignments after the replacement joined: %s", reading.detail)
				r.Reproduction.Absent(t, r.Observation, "%s", detail)
				r.Reproduction.Conclude()
				return reading.owners, true
			}
		case waitCtx.Err() == nil:
			// A reading that the end of the wait interrupted keeps the
			// judgement of the last complete one.
			err = fmt.Errorf("another fault: read the native assignments: %v (%s)", readErr, reading.detail)
		}
		select {
		case <-waitCtx.Done():
			t.Logf("native CephFS mirror assignments after the replacement joined: %s", last.detail)
			if err != nil {
				r.Reproduction.Inconclusive(t, "%v", err)
			}
			r.Reproduction.Symptom(t, r.Observation, "%s", detail)
			r.Reproduction.Conclude()
			return last.owners, false
		case <-time.After(time.Second):
		}
	}
}

// cephFSMirrorAssignments is one native reading of the MGR mirroring
// module: the distribution (instance to "N directories"), each directory's
// state and instance, and the daemon services registered with the shared
// peer. Detail holds the raw distribution.
type cephFSMirrorAssignments struct {
	distribution map[string]string
	states       map[string]string
	owners       map[string]string
	registered   map[string]bool
	detail       string
}

// cephFSReadMirrorDaemonAssignments reads the distribution, every
// directory's map and the daemon status. It fails only when a command or its
// JSON fails; the error's text is then the detail.
func cephFSReadMirrorDaemonAssignments(ctx context.Context, source *ceph.Container, filesystem, peerID string, directories []string) (cephFSMirrorAssignments, error) {
	reading := cephFSMirrorAssignments{states: map[string]string{}, owners: map[string]string{}, registered: map[string]bool{}}
	distributionData, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "show", "distribution", filesystem)
	if err != nil {
		return reading, err
	}
	reading.detail = string(distributionData)
	var distribution struct {
		Mapping map[string]string `json:"mapping"`
	}
	if err := json.Unmarshal(distributionData, &distribution); err != nil {
		return reading, err
	}
	reading.distribution = distribution.Mapping
	for _, directory := range directories {
		data, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", filesystem, directory)
		if err != nil {
			return reading, err
		}
		var mapping struct {
			State      string `json:"state"`
			InstanceID string `json:"instance_id"`
		}
		if err := json.Unmarshal(data, &mapping); err != nil {
			reading.detail = string(data)
			return reading, err
		}
		reading.states[directory] = mapping.State
		if _, exists := distribution.Mapping[mapping.InstanceID]; mapping.State == "mapped" && exists {
			reading.owners[directory] = mapping.InstanceID
		}
	}
	statusData, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "daemon", "status")
	if err != nil {
		return reading, err
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
		reading.detail = string(statusData)
		return reading, err
	}
	for _, status := range statuses {
		id := strconv.FormatUint(status.DaemonID, 10)
		for _, fs := range status.Filesystems {
			if fs.Name != filesystem {
				continue
			}
			for _, peer := range fs.Peers {
				if peer.UUID == peerID {
					reading.registered[id] = true
				}
			}
		}
	}
	return reading, nil
}

func cephFSObserveMirrorDaemonAssignments(ctx context.Context, source *ceph.Container, filesystem, peerID string, directories []string, daemonCount int) (map[string]string, string, error) {
	reading, err := cephFSReadMirrorDaemonAssignments(ctx, source, filesystem, peerID, directories)
	if err != nil {
		return nil, reading.detail, err
	}
	if len(reading.distribution) != daemonCount {
		return nil, reading.detail, fmt.Errorf("native instance count=%d want=%d", len(reading.distribution), daemonCount)
	}
	for id, summary := range reading.distribution {
		count, err := strconv.Atoi(strings.TrimSuffix(summary, " directories"))
		if id == "" || err != nil || count <= 0 {
			return nil, reading.detail, fmt.Errorf("instance %q has no positive assignment: %q", id, summary)
		}
	}
	for _, directory := range directories {
		if reading.owners[directory] == "" {
			return nil, reading.detail, fmt.Errorf("directory %s is not mapped to a current native instance", directory)
		}
	}
	// Daemon service sessions and per-filesystem watchers use separate RADOS
	// GIDs. Directory mappings above refer to watchers; service status only
	// proves shared peer discovery and may retain a stopped service briefly.
	if len(reading.registered) < daemonCount {
		return nil, reading.detail, fmt.Errorf("registered native services with shared peer=%d want at least=%d", len(reading.registered), daemonCount)
	}
	return reading.owners, reading.detail, nil
}

func CephFSWaitForDaemonSnapshots(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string) {
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

func CephFSAssertDaemonSnapshotsAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, snapshot string, duration time.Duration) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for {
		MultiClusterExecOutput(t, ctx, client, "python3", "/tmp/cephfs-mirror-daemons.py", filesystem, "absent", snapshot)
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

const CephFSMirrorDaemonScript = `import cephfs
import json
import os
import sys

filesystem, phase, *args = sys.argv[1:]
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())
directories = ['/daemon-a', '/daemon-b', '/daemon-c', '/daemon-d']
def payload(directory):
    return (snapshot + ':' + directory + ':').encode() * 1024 + bytes(range(256))
try:
    if phase == 'snapshot-checkpoint':
        root, name = args
        assert root.startswith('/') and '\x00' not in root, 'invalid snapshot root'
        assert name and name not in ('.', '..') and '/' not in name and '\x00' not in name, 'invalid snapshot name'
        snapshot_path = root.rstrip('/') + '/.snap/' + name
        info = fs.snap_info(snapshot_path)
        snapshot_id = info['id']
        assert type(snapshot_id) is int and 1 <= snapshot_id <= 18446744073709551613, 'invalid source snapshot ID'
        print(json.dumps({'id': snapshot_id, 'name': name}, sort_keys=True))
    else:
        snapshot, = args
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
