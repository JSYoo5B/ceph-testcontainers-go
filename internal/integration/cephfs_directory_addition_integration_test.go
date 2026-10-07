//go:build integration && multicluster

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Two independent pairs cover explicit zero-daemon policy construction, actual
// applied response loss, separate unapplied dispatch loss, and real client I/O.
func TestMultiClusterCephFSDirectoryAdditionIntent(t *testing.T) {
	for _, host := range []bool{false, true} {
		mode := "bridge"
		if host {
			mode = "host"
		}
		t.Run(mode, func(t *testing.T) { testCephFSDirectoryAdditionIntent(t, host) })
	}
}

func testCephFSDirectoryAdditionIntent(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
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
	const seed, added, foreign, unapplied = "/daemon-a", "/addition-intent", "/foreign-control", "/unapplied-intent"
	const script = "/tmp/cephfs-directory-addition-probe.py"
	probe := strings.Replace(cephFSOriginalProcessProbeScript, "assert directory == '/daemon-a'", "assert directory in ('/daemon-a', '/addition-intent', '/foreign-control', '/unapplied-intent')", 1)
	probe = strings.Replace(probe, "if phase == 'checkpoint':", `if phase == 'prepare':
        try:
            fs.mkdir(root, 0o755)
        except cephfs.ObjectExists:
            pass
        fs.sync_fs()
    elif phase == 'checkpoint':`, 1)
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(probe), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(directory, name string) multicluster.CephFSMirrorSnapshot {
		t.Helper()
		multiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", directory, name)
		return cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
	}
	verify := func(directory string, snapshot multicluster.CephFSMirrorSnapshot, stage string) {
		t.Helper()
		for _, role := range []string{"source", "destination"} {
			client, filesystem := sourceClient, sourceFS.FilesystemName
			if role == "destination" {
				client, filesystem = destinationClient, destinationFS.FilesystemName
			}
			cephFSWaitOriginalProcessBytes(t, ctx, client, filesystem, script, directory, snapshot.Name, role, stage)
		}
		if got := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, snapshot.Name); got != snapshot {
			t.Fatalf("independent source checkpoint changed: before=%+v after=%+v", snapshot, got)
		}
	}
	seedCheckpoint := checkpoint(seed, "seed-before-zero-inventory")
	const site = "directory-addition-target"
	var customizers atomic.Int32
	mirror, err := multicluster.RunCephFSMirror(ctx, source.ControlImage(), multicluster.CephFSMirrorConfig{
		Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		DestinationSite: site, Directories: []string{seed}, DaemonCount: 1,
	}, testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
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
				t.Errorf("terminate directory addition fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(mirror.Daemons()) != 1 || customizers.Load() != 1 {
		t.Fatal("constructor did not retain ordinary single daemon/customizer contract")
	}
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("initial exact owned peer: %v %v", peers, err)
	}
	expected.PeerID = peers[0]
	policy := cephFSOriginalProcessPolicy{source: source, destination: destination, sourceFS: sourceFS, destinationFS: destinationFS, expected: expected, directory: seed, client: mirror.DestinationClientEntity, site: site}
	if err := policy.capture(ctx); err != nil {
		t.Fatal(err)
	}
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, seed, seedCheckpoint)
	verify(seed, seedCheckpoint, "initial-seed")
	for _, daemon := range mirror.Daemons() {
		if err := mirror.RemoveDaemon(ctx, daemon.DaemonName); err != nil {
			t.Fatal(err)
		}
	}
	if len(mirror.Daemons()) != 0 || mirror.Container != nil || customizers.Load() != 1 {
		t.Fatal("explicit removal did not leave zero owned inventory")
	}
	cephFSDirectoryAdditionWaitZeroWatchers(t, ctx, source, sourceFS.MetadataPool)
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed}); err != nil {
		t.Fatal(err)
	}
	control := source.Container
	if selected, err := source.ControlContainerContext(ctx); err != nil || selected != control {
		t.Fatalf("addition command oracle requires original single-MON control: %v", err)
	}
	counter := &cephFSDirectoryAdditionCommandCounter{Container: control, filesystem: sourceFS.FilesystemName, added: added, unapplied: unapplied}
	counter.unappliedFault.Store(true)
	source.Container = counter
	t.Cleanup(func() { source.Container = control })

	// A separate native-owned control is deliberately present BEFORE typed intent.
	foreignCheckpoint := checkpoint(foreign, "foreign-preexisting-policy")
	if _, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "add", sourceFS.FilesystemName, foreign); err != nil {
		t.Fatal(err)
	}
	cephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, foreign})
	beforeMutations := counter.allAdds.Load()
	if receipt, err := mirror.BeginDirectoryAddition(ctx, foreign); receipt != nil || err == nil || slices.Contains(mirror.Directories, foreign) || counter.allAdds.Load() != beforeMutations {
		t.Fatal("typed intent adopted preexisting foreign native path", err)
	}
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign}); err != nil {
		t.Fatal("preexisting foreign path changed after refusal", err)
	}

	// Registration requires an existing filesystem directory. Preparing only the
	// source inode creates no data, snapshot or mirror policy; the native array
	// must still prove this path absent before the typed intent is registered.
	multiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "prepare", added, "prepare-added-directory")
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign}); err != nil {
		t.Fatal("prepared source directory changed absent native registration", err)
	}
	applied, err := mirror.BeginDirectoryAddition(ctx, added)
	if applied == nil || !errors.Is(err, errCephFSDirectoryAdditionAppliedReplyLost) || counter.addedAttempts.Load() != 1 || counter.addedDispatches.Load() != 1 || counter.appliedLosses.Load() != 1 || slices.Contains(mirror.Directories, added) {
		t.Fatal("real successful add lost reply did not retain unpublished typed intent", err)
	}
	cephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, foreign, added})
	status, err := applied.Status(ctx)
	cephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, false)
	if err != nil || counter.addedDispatches.Load() != 1 || slices.Contains(mirror.Directories, added) || len(mirror.Daemons()) != 0 {
		t.Fatal("read-only visible Status published or resent addition", err)
	}
	cephFSDirectoryAdditionLog(t, "applied-visible-unpublished", status, counter, mirror, policy)
	cephFSDirectoryAdditionAssertPendingGates(t, ctx, mirror, counter, added, expected.PeerID)
	backlog := checkpoint(added, "backlog-with-no-owned-daemon")
	cephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, added, backlog.Name, "zero-inventory-add-backlog", 5*time.Second)
	for i := 0; i < 2; i++ {
		same, err := mirror.BeginDirectoryAddition(ctx, added)
		if same != applied || err != nil || counter.addedAttempts.Load() != 1 || counter.addedDispatches.Load() != 1 {
			t.Fatal("fresh applied reconciliation replaced intent or replayed native add", err)
		}
	}
	status, err = applied.Status(ctx)
	cephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, true)
	if err != nil || cephFSDirectoryAdditionPathCount(mirror.Directories, added) != 1 {
		t.Fatal("registered intent did not publish exactly one desired path", err)
	}
	cephFSDirectoryAdditionLog(t, "applied-explicit-reconcile", status, counter, mirror, policy)
	receiver, err := mirror.AddDaemon(ctx, "addition-receiver")
	if err != nil || receiver == nil || len(mirror.Daemons()) != 1 || customizers.Load() != 2 {
		t.Fatal("registered zero-inventory intent did not permit ordinary new receiver", err)
	}
	owners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, []string{seed, foreign, added}, 1)
	backlogStatus := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, backlog)
	if backlogStatus.DaemonName != receiver.DaemonName || backlogStatus.InstanceID != owners[added] {
		t.Fatal("backlog public replay attributed to another receiver")
	}
	verify(added, backlog, "recovered-added-backlog")
	fresh := checkpoint(added, "fresh-after-typed-addition")
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, fresh)
	verify(added, fresh, "fresh-added-checkpoint")
	verify(added, backlog, "frozen-added-backlog")
	verify(seed, seedCheckpoint, "retained-seed")
	verify(foreign, foreignCheckpoint, "foreign-policy-retained")
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, added}); err != nil {
		t.Fatal(err)
	}

	// A genuinely applied later removal loses its response. The prior addition
	// guards before native reads, even before a same-path removal retry reconciles.
	counter.removeLossArmed.Store(true)
	removed, err := mirror.BeginDirectoryRemoval(ctx, added)
	if removed == nil || !errors.Is(err, errCephFSDirectoryAdditionRemoveReplyLost) || counter.removeDispatches.Load() != 1 || counter.removeLosses.Load() != 1 {
		t.Fatal("new owned cycle did not retain real lost removal response", err)
	}
	beforeCommands := counter.commands.Load()
	if old, err := applied.Status(ctx); err == nil || old.Registered || old.PolicyPresent || counter.commands.Load() != beforeCommands {
		t.Fatal("attempted removal left prior addition authority usable")
	}
	cephFSDirectoryAdditionReconcileRemoval(t, ctx, mirror, added, removed, counter)
	releaseCtx, releaseCancel := context.WithTimeout(ctx, 3*time.Minute)
	released, err := removed.WaitReleased(releaseCtx)
	releaseCancel()
	if err != nil || !released.Released || !released.PolicyRemoved || released.PeerID != expected.PeerID || counter.removeDispatches.Load() != 1 {
		t.Fatalf("new typed owned cycle failed strict live release: %+v %v", released, err)
	}
	cephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, foreign})
	between := checkpoint(added, "backlog-after-cycle-release")
	cephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, added, between.Name, "strict-cycle-released", 5*time.Second)
	newAddition, err := mirror.BeginDirectoryAddition(ctx, added)
	if newAddition == nil || newAddition == applied || counter.addedDispatches.Load() != 2 {
		t.Fatal("new explicit add reused prior cycle or failed to create new ownership", err)
	}
	cephFSDirectoryAdditionReconcile(t, ctx, mirror, added, newAddition)
	if counter.addedDispatches.Load() != 2 {
		t.Fatal("acknowledged new cycle was resent during convergence")
	}
	status, err = newAddition.Status(ctx)
	cephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, true)
	if err != nil {
		t.Fatal(err)
	}
	beforeCommands = counter.commands.Load()
	if old, err := removed.Status(ctx); err == nil || old.Released || counter.commands.Load() != beforeCommands {
		t.Fatal("old removal acquired the new directory generation")
	}
	if old, err := applied.Status(ctx); err == nil || old.Registered || counter.commands.Load() != beforeCommands {
		t.Fatal("old addition acquired the new directory generation")
	}
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, between)
	verify(added, between, "new-cycle-backlog")
	newCheckpoint := checkpoint(added, "new-cycle-fresh-checkpoint")
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, newCheckpoint)
	verify(added, newCheckpoint, "new-cycle-fresh")
	verify(added, backlog, "frozen-first-cycle-backlog")
	verify(added, fresh, "frozen-first-cycle-fresh")
	verify(seed, seedCheckpoint, "seed-after-new-generation")
	cephFSDirectoryAdditionLog(t, "new-owned-cycle", status, counter, mirror, policy)

	// Separate unapplied fault: the first wrapper attempt never dispatches native
	// add. Fresh exact native absence authorizes a later attempt, not a cached ACK.
	unappliedCheckpoint := checkpoint(unapplied, "unapplied-request-backlog")
	uncertain, err := mirror.BeginDirectoryAddition(ctx, unapplied)
	if uncertain == nil || !errors.Is(err, errCephFSDirectoryAdditionUnappliedFault) || counter.unappliedAttempts.Load() != 1 || counter.unappliedDispatches.Load() != 0 || counter.unappliedFaults.Load() != 1 || slices.Contains(mirror.Directories, unapplied) {
		t.Fatal("unapplied attempt dispatched or published native ownership", err)
	}
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, added}); err != nil {
		t.Fatal("unapplied native path was not independently absent", err)
	}
	status, err = uncertain.Status(ctx)
	cephFSDirectoryAdditionAssertStatus(t, status, expected, unapplied, false, false)
	if err != nil || counter.unappliedDispatches.Load() != 0 {
		t.Fatal("unapplied Status resent or adopted intent", err)
	}
	cephFSDirectoryAdditionLog(t, "unapplied-absent-unpublished", status, counter, mirror, policy)
	cephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, unapplied, unappliedCheckpoint.Name, "unapplied-native-absence", 3*time.Second)
	if same, err := mirror.BeginDirectoryAddition(ctx, unapplied); same != uncertain || counter.unappliedAttempts.Load() != 2 || counter.unappliedDispatches.Load() != 1 {
		t.Fatal("fresh uncertain absent retry did not preserve exact intent", err)
	}
	cephFSDirectoryAdditionReconcile(t, ctx, mirror, unapplied, uncertain)
	if counter.unappliedAttempts.Load() != 2 || counter.unappliedDispatches.Load() != 1 {
		t.Fatal("acknowledged formerly-unapplied request was resent during convergence")
	}
	status, err = uncertain.Status(ctx)
	cephFSDirectoryAdditionAssertStatus(t, status, expected, unapplied, true, true)
	if err != nil || cephFSDirectoryAdditionPathCount(mirror.Directories, unapplied) != 1 {
		t.Fatal("unapplied retry failed one owned publication", err)
	}
	beforeCommands = counter.commands.Load()
	if old, err := newAddition.Status(ctx); err == nil || old.Registered || counter.commands.Load() != beforeCommands {
		t.Fatal("singleton different-path typed intent did not invalidate prior completed receipt")
	}
	cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, unapplied, unappliedCheckpoint)
	verify(unapplied, unappliedCheckpoint, "unapplied-retry-client-bytes")
	verify(seed, seedCheckpoint, "final-seed-unchanged")
	verify(foreign, foreignCheckpoint, "final-foreign-unchanged")
	beforeCommands = counter.commands.Load()
	if receipt, err := mirror.BeginDirectoryRemoval(ctx, foreign); receipt != nil || err == nil || counter.commands.Load() != beforeCommands {
		t.Fatal("native foreign policy gained privately owned removal authority", err)
	}
	if err := cephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, added, unapplied}); err != nil || slices.Contains(mirror.Directories, foreign) || customizers.Load() != 2 || counter.appliedLosses.Load() != 1 || counter.unappliedFaults.Load() != 1 || counter.removeLosses.Load() != 1 {
		t.Fatal("final original policy/client lifecycle/fault evidence changed", err)
	}
	cephFSDirectoryAdditionLog(t, "unapplied-fresh-retry", status, counter, mirror, policy)
}

func cephFSDirectoryAdditionAssertStatus(t *testing.T, status multicluster.CephFSMirrorDirectoryAdditionStatus, expected multicluster.CephFSMirrorDirectoryStatus, directory string, present, registered bool) {
	t.Helper()
	if status.Directory != directory || status.PeerID != expected.PeerID || status.SourceFilesystem != expected.SourceFilesystem || status.DestinationFilesystem != expected.DestinationFilesystem || status.SourceFilesystemID != expected.SourceFilesystemID || status.DestinationFilesystemID != expected.DestinationFilesystemID || status.PolicyPresent != present || status.Registered != registered {
		t.Fatalf("retained directory addition changed original identity or state: %+v", status)
	}
}

func cephFSDirectoryAdditionAssertPendingGates(t *testing.T, ctx context.Context, mirror *multicluster.CephFSMirror, counter *cephFSDirectoryAdditionCommandCounter, directory, peer string) {
	t.Helper()
	ops := []struct {
		name string
		run  func(context.Context) error
	}{
		{"legacy-same-add", func(ctx context.Context) error { return mirror.AddDirectory(ctx, directory) }},
		{"typed-other-add", func(ctx context.Context) error {
			_, err := mirror.BeginDirectoryAddition(ctx, "/blocked-intent")
			return err
		}},
		{"new-daemon", func(ctx context.Context) error { _, err := mirror.AddDaemon(ctx, "blocked-daemon"); return err }},
		{"rebalance", mirror.RebalanceDirectories},
		{"bootstrap", func(ctx context.Context) error { _, err := mirror.RebootstrapPeer(ctx); return err }},
		{"begin-peer-remove", func(ctx context.Context) error { _, err := mirror.BeginPeerRemoval(ctx, peer); return err }},
		{"legacy-peer-remove", func(ctx context.Context) error { return mirror.RemovePeer(ctx, peer) }},
		{"begin-directory-remove", func(ctx context.Context) error { _, err := mirror.BeginDirectoryRemoval(ctx, directory); return err }},
		{"legacy-directory-remove", func(ctx context.Context) error { return mirror.RemoveDirectory(ctx, directory) }},
	}
	for _, op := range ops {
		before := counter.commands.Load()
		gateCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := op.run(gateCtx)
		cancel()
		if err == nil || counter.commands.Load() != before {
			t.Fatalf("pending %s reached native CLI or was accepted: %v", op.name, err)
		}
	}
}

func cephFSDirectoryAdditionReconcileRemoval(t *testing.T, ctx context.Context, mirror *multicluster.CephFSMirror, directory string, receipt *multicluster.CephFSMirrorDirectoryRemoval, counter *cephFSDirectoryAdditionCommandCounter) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		same, err := mirror.BeginDirectoryRemoval(waitCtx, directory)
		if same != receipt {
			t.Fatal("removal response-loss retry replaced owned cycle")
		}
		last = err
		if err == nil {
			if counter.removeDispatches.Load() != 1 {
				t.Fatal("absent removal policy retried native remove")
			}
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("reconcile exact lost removal response", errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func cephFSDirectoryAdditionReconcile(t *testing.T, ctx context.Context, mirror *multicluster.CephFSMirror, directory string, receipt *multicluster.CephFSMirrorDirectoryAddition) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		same, err := mirror.BeginDirectoryAddition(waitCtx, directory)
		if same != receipt {
			t.Fatal("addition convergence replaced retained original intent", err)
		}
		last = err
		if err == nil {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("reconcile original directory addition convergence", errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func cephFSDirectoryAdditionWaitZeroWatchers(t *testing.T, ctx context.Context, source *ceph.Container, pool string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		watchers, err := cephFSOriginalProcessWatchers(waitCtx, source, pool)
		last = err
		if err == nil && len(watchers) == 0 {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("zero inventory retained a native receiving watcher: %v", errors.Join(last, waitCtx.Err()))
		case <-time.After(time.Second):
		}
	}
}

func cephFSDirectoryAdditionWaitScope(t *testing.T, ctx context.Context, p cephFSOriginalProcessPolicy, paths []string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var last error
	for {
		last = cephFSDirectoryAdditionCheckScope(waitCtx, p, paths)
		if last == nil {
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatal("original directory addition policy convergence", errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Independent native oracle preserves the original FSIDs, FS/metadata pool
// name+IDs, MON+MGR peer UUID/tuple, and strict full expected directory policy.
// It does not consult receipt state or production decoders.
func cephFSDirectoryAdditionCheckScope(ctx context.Context, p cephFSOriginalProcessPolicy, wantPaths []string) error {
	var sourcePeers map[string]json.RawMessage
	for i, cluster := range []*ceph.Container{p.source, p.destination} {
		data, err := cluster.Ceph(ctx, "fsid")
		if err != nil || strings.TrimSpace(string(data)) != p.fsids[i] {
			return errors.Join(err, errors.New("independent original addition cluster FSID changed"))
		}
		fs := []*ceph.CephFSContainer{p.sourceFS, p.destinationFS}[i]
		data, err = cluster.Ceph(ctx, "fs", "get", fs.FilesystemName, "--format", "json")
		if err != nil {
			return err
		}
		var value struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				Name *string `json:"fs_name"`
				Pool *int64  `json:"metadata_pool"`
			} `json:"mdsmap"`
			MirrorInfo *struct {
				Peers map[string]json.RawMessage `json:"peers"`
			} `json:"mirror_info"`
		}
		if json.Unmarshal(data, &value) != nil || value.ID == nil || *value.ID != []int{p.expected.SourceFilesystemID, p.expected.DestinationFilesystemID}[i] || value.MDSMap == nil || value.MDSMap.Name == nil || *value.MDSMap.Name != fs.FilesystemName || value.MDSMap.Pool == nil || *value.MDSMap.Pool != p.pools[i] {
			return errors.New("independent original addition filesystem/metadata pool changed")
		}
		data, err = cluster.Ceph(ctx, "osd", "lspools", "--format", "json")
		if err != nil {
			return err
		}
		var pools []struct {
			ID   int64  `json:"poolnum"`
			Name string `json:"poolname"`
		}
		if json.Unmarshal(data, &pools) != nil {
			return errors.New("independent addition pool catalog malformed")
		}
		matches := 0
		for _, pool := range pools {
			if pool.Name == fs.MetadataPool {
				if pool.ID != p.pools[i] {
					return errors.New("independent original addition metadata pool name rebound")
				}
				matches++
			}
		}
		if matches != 1 {
			return errors.New("independent original addition metadata pool unavailable")
		}
		if i == 0 {
			if value.MirrorInfo == nil || value.MirrorInfo.Peers == nil {
				return errors.New("independent addition MON peer policy unavailable")
			}
			sourcePeers = value.MirrorInfo.Peers
		}
	}
	if err := p.checkPeers(sourcePeers, true, true); err != nil {
		return err
	}
	data, err := p.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", p.sourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil {
		return errors.New("independent addition MGR peer policy malformed")
	}
	if err := p.checkPeers(peers, false, true); err != nil {
		return err
	}
	data, err = p.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", p.sourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var paths []string
	if json.Unmarshal(data, &paths) != nil || paths == nil {
		return errors.New("independent addition policy array malformed")
	}
	seen := make(map[string]bool)
	for _, directory := range paths {
		if !strings.HasPrefix(directory, "/") || path.Clean(directory) != directory || strings.ContainsAny(directory, "\x00\r\n") || seen[directory] {
			return errors.New("independent addition native path identity ambiguous")
		}
		seen[directory] = true
	}
	paths = slices.Clone(paths)
	wantPaths = slices.Clone(wantPaths)
	slices.Sort(paths)
	slices.Sort(wantPaths)
	if !slices.Equal(paths, wantPaths) {
		return fmt.Errorf("independent addition native paths=%v want=%v", paths, wantPaths)
	}
	return ctx.Err()
}

type cephFSDirectoryAdditionCommandCounter struct {
	testcontainers.Container
	filesystem, added, unapplied                            string
	commands, addedAttempts, addedDispatches, appliedLosses atomic.Int32
	allAdds                                                 atomic.Int32
	unappliedAttempts, unappliedDispatches, unappliedFaults atomic.Int32
	removeDispatches, removeLosses                          atomic.Int32
	addedLost, unappliedFault, removeLossArmed, removeLost  atomic.Bool
}

var errCephFSDirectoryAdditionAppliedReplyLost = errors.New("injected lost real successful native CephFS add reply")
var errCephFSDirectoryAdditionUnappliedFault = errors.New("injected unapplied CephFS add dispatch fault")
var errCephFSDirectoryAdditionRemoveReplyLost = errors.New("injected lost real later native CephFS removal reply")

func (c *cephFSDirectoryAdditionCommandCounter) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.commands.Add(1)
	native := args
	if len(native) >= 3 && native[0] == "ceph" && native[1] == "--connect-timeout" {
		native = native[3:]
	}
	if len(native) == 6 && slices.Equal(native[:4], []string{"fs", "snapshot", "mirror", "add"}) {
		c.allAdds.Add(1)
	}
	added := slices.Equal(native, []string{"fs", "snapshot", "mirror", "add", c.filesystem, c.added})
	unapplied := slices.Equal(native, []string{"fs", "snapshot", "mirror", "add", c.filesystem, c.unapplied})
	removed := slices.Equal(native, []string{"fs", "snapshot", "mirror", "remove", c.filesystem, c.added})
	if added {
		c.addedAttempts.Add(1)
		c.addedDispatches.Add(1)
	}
	if unapplied {
		c.unappliedAttempts.Add(1)
		if c.unappliedFault.CompareAndSwap(true, false) {
			c.unappliedFaults.Add(1)
			return 0, nil, errCephFSDirectoryAdditionUnappliedFault
		}
		c.unappliedDispatches.Add(1)
	}
	if removed {
		c.removeDispatches.Add(1)
	}
	code, reader, err := c.Container.Exec(ctx, args, opts...)
	if err == nil && code == 0 && ((added && c.addedLost.CompareAndSwap(false, true)) || (removed && c.removeLossArmed.Load() && c.removeLost.CompareAndSwap(false, true))) {
		if reader == nil {
			return 0, nil, errors.New("real successful directory mutation returned no response reader")
		}
		// Drain the successful native response fully; only that response is lost.
		if _, readErr := io.Copy(io.Discard, reader); readErr != nil {
			return 0, nil, readErr
		}
		if added {
			c.appliedLosses.Add(1)
			return 0, nil, errCephFSDirectoryAdditionAppliedReplyLost
		}
		c.removeLosses.Add(1)
		return 0, nil, errCephFSDirectoryAdditionRemoveReplyLost
	}
	return code, reader, err
}

func cephFSDirectoryAdditionPathCount(paths []string, selected string) int {
	count := 0
	for _, directory := range paths {
		if directory == selected {
			count++
		}
	}
	return count
}

func cephFSDirectoryAdditionLog(t *testing.T, stage string, status multicluster.CephFSMirrorDirectoryAdditionStatus, c *cephFSDirectoryAdditionCommandCounter, mirror *multicluster.CephFSMirror, policy cephFSOriginalProcessPolicy) {
	t.Helper()
	data, err := json.Marshal(struct {
		Case, Stage                                                                                                                            string
		Status                                                                                                                                 multicluster.CephFSMirrorDirectoryAdditionStatus
		OwnedDirectories                                                                                                                       []string
		OwnedDaemons                                                                                                                           int
		AddedAttempts, AddedDispatches, AppliedLosses, UnappliedAttempts, UnappliedDispatches, UnappliedFaults, RemoveDispatches, RemoveLosses int32
		OriginalClusterFSIDs                                                                                                                   [2]string
		OriginalMetadataPoolIDs                                                                                                                [2]int64
		OriginalMetadataPoolNames                                                                                                              [2]string
	}{t.Name(), stage, status, slices.Clone(mirror.Directories), len(mirror.Daemons()), c.addedAttempts.Load(), c.addedDispatches.Load(), c.appliedLosses.Load(), c.unappliedAttempts.Load(), c.unappliedDispatches.Load(), c.unappliedFaults.Load(), c.removeDispatches.Load(), c.removeLosses.Load(), policy.fsids, policy.pools, [2]string{policy.sourceFS.MetadataPool, policy.destinationFS.MetadataPool}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_DIRECTORY_ADDITION_NATIVE %s", data)
}
