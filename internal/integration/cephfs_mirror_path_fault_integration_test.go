//go:build all || (integration && features && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_cephfs_mirror_path_fault))))

//ci: timeout=30m job-timeout=40 image-roles=essential,client,cephfs

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/netip"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// TestCephFSMirrorPeerPathCut cuts one path of a CephFS snapshot mirror: the
// source OSD's packets to the cephfs-mirror daemon. The daemon sees a new
// snapshot through the MDS but cannot copy it while the source client writes,
// snapshots and reads through the same OSD and MDS; after the cut is restored
// the snapshot arrives.
func TestCephFSMirrorPeerPathCut(t *testing.T) {
	for _, mode := range networkModes(t, "bridge") {
		t.Run(mode.Name, testCephFSMirrorPeerPathCut)
	}
}

func testCephFSMirrorPeerPathCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	netem := netemImage(t)
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, ceph.WithOSDCount(1))
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
	const script = "/tmp/cephfs-intercluster.py"
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSInterClusterScript), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	fsCommand := func(client testcontainers.Container, filesystem string, args ...string) {
		t.Helper()
		multiClusterExecOutput(t, ctx, client, append([]string{"python3", script, filesystem}, args...)...)
	}
	// archive captures a source snapshot with the source client and hands it
	// to the destination client, which compares it with the mirrored copy.
	archive := func(name string) {
		t.Helper()
		path := "/tmp/cephfs-path-" + name + ".json"
		fsCommand(sourceClient, sourceFS.FilesystemName, "archive", "/federation/.snap/"+name, path)
		if err := destinationClient.CopyToContainer(ctx, multiClusterReadFile(t, ctx, sourceClient, path), path, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	verify := func(name string) {
		t.Helper()
		fsCommand(destinationClient, destinationFS.FilesystemName, "verify-mirror", "/federation/.snap/"+name, "/tmp/cephfs-path-"+name+".json")
	}

	fsCommand(sourceClient, sourceFS.FilesystemName, "seed")
	first := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, "/federation", "backup-1")
	archive("backup-1")
	mirror, err := cephfs.RunMirror(ctx, nativeMirrorImage(t, "cephfs", source.ControlImage()), cephfs.MirrorConfig{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		Directories: []string{"/federation"},
	})
	if mirror != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && mirror.Container != nil {
				multiClusterLogContainer(t, cleanup, mirror.Container)
			}
			if err := mirror.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("mirror peers: %v, %v; want one", peers, err)
	}
	expected.PeerID = peers[0]
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, "/federation", first)
	cephFSWaitForRemoteSnapshot(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-1", true)
	verify("backup-1")
	daemon := mirror.Daemons()[0]
	synced, err := observe(ctx, time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (cephFSMirrorReplay, bool, error) {
		replay, err := cephFSMirrorPeerReplay(probeCtx, daemon, expected)
		return replay, err == nil && replay.State == "idle" && replay.Last == first, err
	})
	if err != nil {
		t.Fatalf("the daemon does not report %s synced: %+v, %v", first.Name, synced.Value, err)
	}
	baseline := synced.Value

	peer := cephFSMirrorDaemonAddress(t, ctx, daemon, source.NetworkName())
	osd := source.OSDs()[0]
	// osd.0 drops every packet it sends to the daemon's address on the
	// source network, and only those: the daemon's requests reach the OSD,
	// its replies do not. The source MON and MDS still reach the daemon.
	lease, err := fault.ApplyNetworkProfile(ctx, source, osd.Container, fault.NetworkProfile{HelperImage: netem, Plane: ceph.PublicNetworkPlane, Peer: peer, Loss: 100})
	if lease != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			if err := lease.Restore(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	cutAt := time.Now()
	// The source client writes a new file and snapshots it through the same
	// OSD and MDS, and reads the snapshot back.
	fsCommand(sourceClient, sourceFS.FilesystemName, "membership-checkpoint", "path-cut", "backup-2")
	second := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, "/federation", "backup-2")
	archive("backup-2")
	// For 30 s the daemon does not finish a sync: backup-1 stays its last
	// synced snapshot and the destination has no backup-2. The MGR's mapping
	// of the directory is recorded as it is.
	var samples int
	var during cephFSMirrorReplay
	mappings := map[string]time.Duration{}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Second) {
		during, err = cephFSMirrorPeerReplay(ctx, daemon, expected)
		if err != nil {
			t.Fatalf("peer status during the cut: %v", err)
		}
		if during.Last != first || during.Synced != baseline.Synced {
			t.Fatalf("the cut daemon advanced: %+v, before %+v", during, baseline)
		}
		mapping := cephFSMirrorDirmap(t, ctx, source, sourceFS.FilesystemName)
		if _, seen := mappings[mapping]; !seen {
			mappings[mapping] = time.Since(cutAt).Round(time.Second)
		}
		cephFSAssertSnapshotRemainsAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, "backup-2", 0)
		t.Logf("during the cut: daemon %s, syncing %q, last synced %s, %d synced; MGR dirmap %s", during.State, during.Current.Name, during.Last.Name, during.Synced, mapping)
		samples++
	}
	// The daemon saw backup-2 through the MDS and is stuck copying it. Squid's
	// peer status has no current_syncing_snap; where the daemon reports the
	// snapshot it copies, it is backup-2.
	if during.State != "syncing" || (during.Current != (cephfs.MirrorSnapshot{}) && during.Current != second) {
		t.Fatalf("the cut daemon is not syncing %s: %+v", second.Name, during)
	}
	verify("backup-1")
	cut, err := lease.Status(ctx)
	if err != nil || !cut.Applied || cut.Drops == 0 {
		t.Fatalf("netem status during the cut: %+v, %v", cut, err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}

	restored := time.Now()
	arrived, err := observe(ctx, 5*time.Minute, time.Minute, time.Second, func(probeCtx context.Context) (string, bool, error) {
		code, reader, err := destinationClient.Exec(probeCtx, []string{"python3", script, destinationFS.FilesystemName, "snapshot-state", "backup-2", "present"}, tcexec.Multiplexed())
		if err != nil {
			return "", false, err
		}
		out, _ := io.ReadAll(reader)
		return string(out), code == 0, nil
	})
	if err != nil {
		t.Fatalf("%s did not arrive on the destination: %v, last %s", second.Name, err, arrived.Value)
	}
	took := time.Since(restored).Round(time.Second)
	verify("backup-2")
	verify("backup-1")
	// The peer status reports the snapshot a little after it arrives.
	reported, err := observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (cephFSMirrorReplay, bool, error) {
		replay, err := cephFSMirrorPeerReplay(probeCtx, daemon, expected)
		return replay, err == nil && replay.State == "idle" && replay.Last == second, err
	})
	if err != nil {
		t.Fatalf("%s is not reported synced: %+v, %v", second.Name, reported.Value, err)
	}
	final := reported.Value
	if final.Synced != baseline.Synced+1 || final.LastBytes != cephFSPathCutBytes {
		t.Fatalf("after the restore: %+v, before %+v; want one more snapshot synced with %d bytes copied", final, baseline, cephFSPathCutBytes)
	}
	remapped, err := observe(ctx, 2*time.Minute, 30*time.Second, time.Second, func(probeCtx context.Context) (string, bool, error) {
		mapping, err := cephFSMirrorDirmapState(probeCtx, source, sourceFS.FilesystemName)
		return mapping, err == nil && mapping == "mapped", err
	})
	if err != nil {
		t.Fatalf("the MGR does not map /federation again: %s, %v", remapped.Value, err)
	}
	t.Logf("CEPHFS_MIRROR_PATH_RECEIPT osd.%d dropped %d packets to cephfs-mirror %s (%s) on the source network, the MDS path left intact; for %d samples over 30 s the daemon stayed %s on %s/%d with last synced %s/%d (%d synced) while the source client wrote, snapshotted and read %s through osd.%d and the MDS; MGR dirmap states after the cut %v; the destination had no %s; %s after the restore it arrived with the new bytes, the daemon reported it synced (%d synced, %d bytes copied) and the MGR mapped /federation again",
		osd.ID, cut.Drops, daemon.DaemonName, peer, samples, during.State, second.Name, second.ID, first.Name, first.ID, baseline.Synced, second.Name, osd.ID, mappings, second.Name, took, final.Synced, final.LastBytes)
}

// cephFSPathCutBytes is the size of the file that membership-checkpoint
// writes for the label path-cut: "new bytes while path-cut\n" 2048 times.
const cephFSPathCutBytes = 25 * 2048

// cephFSMirrorDaemonAddress returns daemon's address on network.
func cephFSMirrorDaemonAddress(t *testing.T, ctx context.Context, daemon *cephfs.MirrorDaemon, network string) netip.Addr {
	t.Helper()
	inspect, err := daemon.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	endpoint := inspect.NetworkSettings.Networks[network]
	if endpoint == nil || !endpoint.IPAddress.IsValid() {
		t.Fatalf("daemon %s has no endpoint on %s", daemon.DaemonName, network)
	}
	return endpoint.IPAddress
}

// cephFSMirrorReplay is what a daemon's own peer status reports for
// /federation, with the bytes that its last sync copied.
type cephFSMirrorReplay struct {
	State         string
	Current, Last cephfs.MirrorSnapshot
	LastBytes     uint64
	Synced        uint64
}

// cephFSMirrorPeerReplay reads the daemon's peer status for /federation
// through its admin socket.
func cephFSMirrorPeerReplay(ctx context.Context, daemon *cephfs.MirrorDaemon, identity cephfs.MirrorDirectoryStatus) (cephFSMirrorReplay, error) {
	code, reader, err := daemon.Exec(ctx, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "peer", "status", fmt.Sprintf("%s@%d", identity.SourceFilesystem, identity.SourceFilesystemID), identity.PeerID}, tcexec.Multiplexed())
	if err != nil {
		return cephFSMirrorReplay{}, err
	}
	out, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		return cephFSMirrorReplay{}, fmt.Errorf("peer status: exit %d: %s, %v", code, out, err)
	}
	type snapshot struct {
		ID    uint64 `json:"id"`
		Name  string `json:"name"`
		Bytes uint64 `json:"sync_bytes"`
	}
	var status map[string]struct {
		State   string    `json:"state"`
		Current *snapshot `json:"current_syncing_snap"`
		Last    *snapshot `json:"last_synced_snap"`
		Synced  *uint64   `json:"snaps_synced"`
	}
	if err := json.Unmarshal(out, &status); err != nil {
		return cephFSMirrorReplay{}, fmt.Errorf("decode peer status %s: %w", out, err)
	}
	entry, ok := status["/federation"]
	if !ok || entry.State == "" || entry.Last == nil || entry.Synced == nil {
		return cephFSMirrorReplay{}, fmt.Errorf("peer status has no synced /federation: %s", out)
	}
	replay := cephFSMirrorReplay{State: entry.State, Last: cephfs.MirrorSnapshot{ID: entry.Last.ID, Name: entry.Last.Name}, LastBytes: entry.Last.Bytes, Synced: *entry.Synced}
	if entry.Current != nil {
		replay.Current = cephfs.MirrorSnapshot{ID: entry.Current.ID, Name: entry.Current.Name}
	}
	return replay, nil
}

// cephFSMirrorDirmapState reads the MGR mirroring module's mapping state of
// /federation.
func cephFSMirrorDirmapState(ctx context.Context, cluster *ceph.Container, filesystem string) (string, error) {
	out, err := cluster.Ceph(ctx, "fs", "snapshot", "mirror", "dirmap", filesystem, "/federation")
	if err != nil {
		return "", err
	}
	var dirmap struct {
		State *string `json:"state"`
	}
	if err := json.Unmarshal(out, &dirmap); err != nil {
		return "", fmt.Errorf("decode dirmap %s: %w", out, err)
	}
	if dirmap.State == nil {
		// v20.2.4 reports null for an unassociated directory.
		return "unassociated", nil
	}
	return *dirmap.State, nil
}

func cephFSMirrorDirmap(t *testing.T, ctx context.Context, cluster *ceph.Container, filesystem string) string {
	t.Helper()
	state, err := cephFSMirrorDirmapState(ctx, cluster, filesystem)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
