//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testCephFSPeerRemovalDrain(t *testing.T, host bool) {
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
	expected := cephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	const script = "/tmp/cephfs-mirror-daemons.py"
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSMirrorDaemonScript), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	directories := []string{"/daemon-a", "/daemon-b", "/daemon-c", "/daemon-d"}
	checkpoint := func(name string) map[string]multicluster.CephFSMirrorSnapshot {
		t.Helper()
		multiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", name)
		result := make(map[string]multicluster.CephFSMirrorSnapshot, len(directories))
		for _, directory := range directories {
			result[directory] = cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
		}
		return result
	}
	initial := checkpoint("before-peer-removal")
	mirror, err := multicluster.RunCephFSMirror(ctx, source.ControlImage(), multicluster.CephFSMirrorConfig{Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName, Directories: directories, DaemonCount: 2})
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
				t.Errorf("terminate retained peer drain fixture: %v", err)
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
	originalPeer := peers[0]
	expected.PeerID = originalPeer
	waitCheckpoints := func(name string, snapshots map[string]multicluster.CephFSMirrorSnapshot) {
		t.Helper()
		owners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, directories, 2)
		for _, directory := range directories {
			status := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, snapshots[directory])
			if status.InstanceID != owners[directory] {
				t.Fatal("public checkpoint disagrees with native watcher assignment")
			}
		}
		cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, name)
	}
	waitCheckpoints("before-peer-removal", initial)
	daemons := mirror.Daemons()
	if len(daemons) != 2 {
		t.Fatal("strict native drain requires the complete two-daemon cohort")
	}
	originals := make(map[string]cephFSPeerRemovalNativeWitness, len(daemons))
	for _, daemon := range daemons {
		originals[daemon.DaemonName] = cephFSReadRemovalWitness(t, ctx, source, sourceFS, expected.SourceFilesystemID, daemon, originalPeer, true)
	}

	// This default single-MON fixture's embedded handle is the source CLI. Keep a
	// transparent counter while retrying Begin, then restore the original handle
	// before observational Status/Wait. There is no Docker or native replacement.
	originalControl := source.Container
	control, err := source.ControlContainerContext(ctx)
	if err != nil || control != originalControl {
		t.Fatalf("lost-response probe needs original single-MON CLI: %v", err)
	}
	fault := &cephFSPeerRemovalReplyFault{Container: originalControl, filesystem: sourceFS.FilesystemName, peerID: originalPeer}
	source.Container = fault
	t.Cleanup(func() { source.Container = originalControl })
	receipt, removeErr := mirror.BeginPeerRemoval(ctx, originalPeer)
	if receipt == nil || !errors.Is(removeErr, errCephFSPeerRemoveReplyLost) || fault.removals.Load() != 1 || !fault.lost.Load() {
		t.Fatalf("real native lost reply did not retain receipt: receipt=%v calls=%d error=%v", receipt, fault.removals.Load(), removeErr)
	}
	beforeGate := fault.commands.Load()
	if _, err := mirror.RebootstrapPeer(ctx); err == nil || fault.commands.Load() != beforeGate {
		t.Fatal("pending explicit drain allowed token/policy/CLI overlap")
	}
	retryCtx, retryCancel := context.WithTimeout(ctx, time.Minute)
	for {
		again, retryErr := mirror.BeginPeerRemoval(retryCtx, originalPeer)
		if again != receipt || fault.removals.Load() != 1 {
			retryCancel()
			t.Fatal("fresh-context original retry replaced receipt or repeated native peer_remove")
		}
		if retryErr == nil {
			break
		}
		select {
		case <-retryCtx.Done():
			retryCancel()
			t.Fatal("original peer policy convergence after lost response", errors.Join(retryErr, retryCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
	retryCancel()
	source.Container = originalControl
	drainCtx, drainCancel := context.WithTimeout(ctx, 3*time.Minute)
	drained, err := receipt.WaitDrained(drainCtx)
	drainCancel()
	if err != nil || !drained.Drained || !drained.PolicyRemoved || drained.PeerID != originalPeer || drained.SourceFilesystem != expected.SourceFilesystem || drained.DestinationFilesystem != expected.DestinationFilesystem || drained.SourceFilesystemID != expected.SourceFilesystemID || drained.DestinationFilesystemID != expected.DestinationFilesystemID || len(drained.Daemons) != len(originals) {
		t.Fatalf("strict same-session public peer drain: %+v %v", drained, err)
	}
	for _, daemon := range daemons {
		before := originals[daemon.DaemonName]
		after := cephFSReadRemovalWitness(t, ctx, source, sourceFS, expected.SourceFilesystemID, daemon, originalPeer, false)
		report, present := drained.Daemons[daemon.DaemonName]
		if !present || report.State != "drained" || report.Problem != "" || report.ContainerID != before.cid || report.InstanceID != before.gid || after != before {
			t.Fatalf("public drain did not retain exact original CID/StartedAt/watcher/session: report=%+v before=%+v after=%+v", report, before, after)
		}
	}
	currentPeers, err := mirror.PeerIDs(ctx)
	if err != nil || len(currentPeers) != 0 {
		t.Fatalf("drained peer still in MGR policy: %v %v", currentPeers, err)
	}
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "before-peer-removal")
	pending := checkpoint("while-peer-removed")
	cephFSAssertDaemonSnapshotsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "while-peer-removed", 10*time.Second)
	newPeer, err := mirror.RebootstrapPeer(ctx)
	if err != nil || newPeer == "" || newPeer == originalPeer {
		t.Fatalf("drained receipt did not release explicit import gate: %s %v", newPeer, err)
	}
	if obsolete, err := receipt.Status(ctx); err == nil || obsolete.Drained || obsolete.PeerID != originalPeer {
		t.Fatalf("old receipt adopted a newly bootstrapped UUID: %+v %v", obsolete, err)
	}
	expected.PeerID = newPeer
	waitCheckpoints("while-peer-removed", pending)
	fresh := checkpoint("after-peer-rebootstrap")
	waitCheckpoints("after-peer-rebootstrap", fresh)
	cephFSWaitForDaemonSnapshots(t, ctx, destinationClient, destinationFS.FilesystemName, "before-peer-removal")
	t.Log("public strict two-daemon peer drain retained original process/watcher identities; a real lost removal reply was reconciled without another delete; old snapshot bytes survived and a fresh peer copied independent source checkpoints/new bytes")
}

type cephFSPeerRemovalNativeWitness struct{ cid, startedAt, address, gid string }

func cephFSReadRemovalWitness(t *testing.T, ctx context.Context, source *ceph.Container, filesystem *ceph.CephFSContainer, fsID int, daemon *multicluster.CephFSMirrorDaemon, peerID string, present bool) cephFSPeerRemovalNativeWitness {
	t.Helper()
	state, err := daemon.Inspect(ctx)
	if err != nil || state == nil || state.ID != daemon.GetContainerID() || state.State == nil || !state.State.Running || state.State.Paused || state.State.Restarting || state.State.Dead {
		t.Fatalf("independent original daemon process: %v", err)
	}
	started, err := time.Parse(time.RFC3339Nano, state.State.StartedAt)
	if err != nil || started.IsZero() {
		t.Fatal("independent original daemon start generation is missing")
	}
	fsCommand := fmt.Sprintf("%s@%d", filesystem.FilesystemName, fsID)
	output := multiClusterExecOutput(t, ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fsCommand)
	var session struct {
		Address  string                     `json:"rados_inst"`
		Peers    map[string]json.RawMessage `json:"peers"`
		SnapDirs *struct {
			Count *int `json:"dir_count"`
		} `json:"snap_dirs"`
	}
	if json.Unmarshal(output, &session) != nil || session.Address == "" || session.Peers == nil || session.SnapDirs == nil || session.SnapDirs.Count == nil {
		t.Fatal("independent normal filesystem session missing")
	}
	_, hasPeer := session.Peers[peerID]
	if hasPeer != present || len(session.Peers) != map[bool]int{true: 1, false: 0}[present] {
		t.Fatal("independent original peer session presence disagrees")
	}
	catalogData := multiClusterExecOutput(t, ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "help")
	var catalog map[string]string
	if json.Unmarshal(catalogData, &catalog) != nil || catalog["help"] == "" || catalog["get_command_descriptions"] == "" || catalog["fs mirror status "+fsCommand] == "" {
		t.Fatal("independent original filesystem help catalog malformed")
	}
	_, hasCommand := catalog["fs mirror peer status "+fsCommand+" "+peerID]
	if hasCommand != present {
		t.Fatal("independent EXACT old peer command registration disagrees")
	}
	control, err := source.ControlContainerContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	watcherData := multiClusterExecOutput(t, ctx, control, "rados", "--pool", filesystem.MetadataPool, "listwatchers", "cephfs_mirror")
	gid := ""
	for _, line := range strings.Split(strings.TrimSpace(string(watcherData)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.HasPrefix(fields[0], "watcher=") || !strings.HasPrefix(fields[1], "client.") || !strings.HasPrefix(fields[2], "cookie=") {
			t.Fatal("independent filesystem watcher schema malformed")
		}
		if strings.TrimPrefix(fields[0], "watcher=") == session.Address {
			if gid != "" {
				t.Fatal("duplicate original filesystem watcher")
			}
			gid = strings.TrimPrefix(fields[1], "client.")
		}
	}
	number, err := strconv.ParseUint(gid, 10, 64)
	if err != nil || number == 0 {
		t.Fatal("independent original filesystem watcher missing")
	}
	return cephFSPeerRemovalNativeWitness{cid: state.ID, startedAt: started.UTC().Format(time.RFC3339Nano), address: session.Address, gid: gid}
}

var errCephFSPeerRemoveReplyLost = errors.New("injected lost real native CephFS peer_remove reply")

type cephFSPeerRemovalReplyFault struct {
	testcontainers.Container
	filesystem, peerID string
	commands, removals atomic.Int32
	lost               atomic.Bool
}

func (f *cephFSPeerRemovalReplyFault) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	f.commands.Add(1)
	native := args
	if len(native) >= 3 && native[0] == "ceph" && native[1] == "--connect-timeout" {
		native = native[3:]
	}
	target := slices.Equal(native, []string{"fs", "snapshot", "mirror", "peer_remove", f.filesystem, f.peerID})
	if target {
		f.removals.Add(1)
	}
	code, reader, err := f.Container.Exec(ctx, args, opts...)
	if target && err == nil && code == 0 && f.lost.CompareAndSwap(false, true) {
		if reader != nil {
			if _, readErr := io.Copy(io.Discard, reader); readErr != nil {
				return 0, nil, readErr
			}
		}
		return 0, nil, errCephFSPeerRemoveReplyLost
	}
	return code, reader, err
}
