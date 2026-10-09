//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testCephFSOriginalProcessQuiescenceRecovery(t *testing.T, host bool, kind string) {
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
	const directory = "/daemon-a"
	const script = "/tmp/cephfs-original-process-probe.py"
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(cephFSOriginalProcessProbeScript), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(name string) cephfs.MirrorSnapshot {
		t.Helper()
		multiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", directory, name)
		return cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
	}
	initial := checkpoint("before-original-process-removal")
	const site = "original-process-target"
	var factoryCalls atomic.Int32
	var customizerCalls atomic.Int32
	customizer := testcontainers.CustomizeRequestOption(func(_ *testcontainers.GenericContainerRequest) error {
		customizerCalls.Add(1)
		return nil
	})
	mirror, err := cephfs.RunMirror(ctx, source.ControlImage(), cephfs.MirrorConfig{
		Source: source, Destination: destination,
		SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		DestinationSite: site, Directories: []string{directory}, DaemonCount: 1,
		OriginalProcessClientFactory: func(ctx context.Context) (*mobycl.Client, error) {
			factoryCalls.Add(1)
			client, err := testcontainers.NewDockerClientWithOpts(ctx)
			if err != nil {
				return nil, err
			}
			return client.Client, nil // Fresh raw client ownership transfers to mirror.
		},
	}, customizer)
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
				t.Errorf("terminate original process fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	daemons := mirror.Daemons()
	if len(daemons) != 1 || factoryCalls.Load() != 1 || customizerCalls.Load() != 1 {
		t.Fatal("original process fixture must have exactly one bound daemon")
	}
	daemon := daemons[0]
	binding := daemon.ProcessObserverBindingStatus()
	if !binding.Available || binding.State != "bound" || binding.EngineID == "" || len(binding.ContainerID) != 64 || binding.ContainerID != daemon.GetContainerID() {
		t.Fatalf("public original engine/full CID binding unavailable: %+v", binding)
	}
	// This client is separate from both TC's cached provider and the production
	// factory client. Retain it across Stop, Start and membership removal.
	oracle, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := oracle.Close(); err != nil {
			t.Errorf("close independent raw Docker oracle: %v", err)
		}
	})
	peers, err := mirror.PeerIDs(ctx)
	if err != nil || len(peers) != 1 {
		t.Fatalf("original owned peer: %v %v", peers, err)
	}
	expected.PeerID = peers[0]
	owners := cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, []string{directory}, 1)
	ready := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, initial)
	if ready.InstanceID != owners[directory] {
		t.Fatal("public checkpoint disagrees with independent mapped owner")
	}
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		filesystem := sourceFS.FilesystemName
		role := "source"
		if client == destinationClient {
			filesystem = destinationFS.FilesystemName
			role = "destination"
		}
		cephFSWaitOriginalProcessBytes(t, ctx, client, filesystem, script, directory, initial.Name, role, "initial")
	}
	policy := cephFSOriginalProcessPolicy{source: source, destination: destination, sourceFS: sourceFS, destinationFS: destinationFS, expected: expected, directory: directory, client: mirror.DestinationClientEntity, site: site}
	if err := policy.capture(ctx); err != nil {
		t.Fatal("capture independent original native policy", err)
	}
	if err := policy.check(ctx, "initial"); err != nil {
		t.Fatal("positive original native policy", err)
	}
	original, err := cephFSOriginalProcessRawInspect(ctx, oracle.Client, binding)
	if err != nil || original.missing || !cephFSOriginalProcessLive(original.state) {
		t.Fatalf("positive original raw process witness: %+v %v", original, err)
	}
	watchers, err := cephFSOriginalProcessWatchers(ctx, source, sourceFS.MetadataPool)
	if err != nil {
		t.Fatal("positive original complete watcher set", err)
	}
	session, err := cephFSOriginalProcessSession(ctx, daemon, expected, true, 1, watchers)
	if err != nil || len(watchers) != 1 || watchers[session.gid] != session.address || ready.InstanceID != session.gid {
		t.Fatalf("complete original native watcher/assignment witness: %+v %+v %v", watchers, session, err)
	}
	// The binding stays attached to the returned Engine/CID, while Begin must
	// capture the currently running task. Restart before admission distinguishes
	// that receipt baseline from the positive creation binding's StartedAt.
	creationStartedAt, creationGID := original.state.StartedAt, session.gid
	stopTimeout := 10 * time.Second
	if err := daemon.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("pre-Begin normal stop", err)
	}
	if err := daemon.Start(ctx); err != nil {
		t.Fatal("pre-Begin start on original binding", err)
	}
	original, session = cephFSWaitOriginalProcessFreshSession(t, ctx, daemon, oracle.Client, binding, policy, creationStartedAt, creationGID)
	owners = cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, []string{directory}, 1)
	ready = cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, initial)
	if ready.InstanceID != session.gid || owners[directory] != session.gid || daemon.ProcessObserverBindingStatus() != binding || factoryCalls.Load() != 1 || customizerCalls.Load() != 1 {
		t.Fatal("pre-Begin restart changed creation binding/customizer or retained old assignment")
	}
	baseline := cephFSOriginalProcessBaseline{startedAt: original.state.StartedAt, instanceID: session.gid}
	originalPeer := expected.PeerID
	originalControl := source.Container
	control, err := source.ControlContainerContext(ctx)
	if err != nil || control != originalControl {
		t.Fatalf("acknowledgment command oracle needs original single-MON CLI: %v", err)
	}
	counter := &cephFSProcessRecoveryCommandCounter{Container: originalControl, filesystem: sourceFS.FilesystemName, peerID: originalPeer, directory: directory, kind: kind}
	source.Container = counter
	t.Cleanup(func() { source.Container = originalControl })
	var observe func(context.Context) (cephfs.MirrorProcessQuiescenceStatus, error)
	var acknowledge func(context.Context) (cephfs.MirrorProcessQuiescenceAcknowledgment, error)
	var legacyStatus func(context.Context) (bool, error)
	var retryBegin, legacyRemove func(context.Context) error
	if kind == "peer" {
		receipt, err := mirror.BeginPeerRemoval(ctx, originalPeer)
		if receipt == nil || !errors.Is(err, errCephFSProcessRecoveryRemoveReplyLost) {
			t.Fatal("public original recovery peer lost reply did not retain receipt/cause", err)
		}
		observe, acknowledge = receipt.ProcessQuiescence, receipt.AcknowledgeProcessQuiescence
		legacyStatus = func(ctx context.Context) (bool, error) {
			status, err := receipt.Status(ctx)
			return status.Drained, err
		}
		retryBegin = func(ctx context.Context) error {
			again, err := mirror.BeginPeerRemoval(ctx, originalPeer)
			if again != receipt {
				return errors.Join(err, errors.New("same-generation peer retry replaced retained receipt"))
			}
			return err
		}
		legacyRemove = func(ctx context.Context) error { return mirror.RemovePeer(ctx, originalPeer) }
	} else {
		receipt, err := mirror.BeginDirectoryRemoval(ctx, directory)
		if receipt == nil || !errors.Is(err, errCephFSProcessRecoveryRemoveReplyLost) {
			t.Fatal("public original recovery directory lost reply did not retain receipt/cause", err)
		}
		observe, acknowledge = receipt.ProcessQuiescence, receipt.AcknowledgeProcessQuiescence
		legacyStatus = func(ctx context.Context) (bool, error) {
			status, err := receipt.Status(ctx)
			return status.Released, err
		}
		retryBegin = func(ctx context.Context) error {
			again, err := mirror.BeginDirectoryRemoval(ctx, directory)
			if again != receipt {
				return errors.Join(err, errors.New("same-generation directory retry replaced retained receipt"))
			}
			return err
		}
		legacyRemove = func(ctx context.Context) error { return mirror.RemoveDirectory(ctx, directory) }
	}
	if counter.originalRemovals(kind) != 1 || counter.replyLosses.Load() != 1 || !counter.lost.Load() {
		t.Fatal("initial public Begin did not lose exactly one real successful original removal reply")
	}
	originalIntentLostReply := true // Both nonnil receipt and exact cause verified.
	if kind == "directory" && !slices.Contains(mirror.Directories, directory) {
		t.Fatal("uncertain native reply discarded original directory desire before explicit acknowledgment")
	}
	lostReplyLog, err := json.Marshal(struct {
		Case                    string `json:"case"`
		Kind                    string `json:"kind"`
		ReceiptReturned         bool   `json:"receipt_returned"`
		CausePreserved          bool   `json:"cause_preserved"`
		OriginalIntentLostReply bool   `json:"original_intent_lost_reply"`
		ReplyLosses             int32  `json:"reply_losses"`
		OriginalRemovals        int32  `json:"original_removals"`
	}{t.Name(), kind, true, true, originalIntentLostReply, counter.replyLosses.Load(), counter.originalRemovals(kind)})
	if err != nil {
		t.Fatal("encode retained original lost removal reply proof", err)
	}
	t.Logf("CEPHFS_PROCESS_QUIESCENCE_LOST_REPLY %s", lostReplyLog)
	// No live legacy completion observer is called before normal Stop; it could
	// legitimately complete the separate graceful contract and release the gate.
	// Do not retry Begin now: G observes the uncertain applied native policy and
	// explicit Ack must reconcile stale peerID/desired path only after removal.
	var backlog cephfs.MirrorSnapshot
	if err := daemon.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("normal stop before recovery acknowledgment", err)
	}
	for _, evidence := range []string{"exited", "later-run", "container-removed"} {
		if evidence == "later-run" {
			if err := daemon.Start(ctx); err != nil {
				t.Fatal("later original-CID task before recovery acknowledgment", err)
			}
		}
		if evidence == "container-removed" {
			if err := mirror.RemoveDaemon(ctx, daemon.DaemonName); err != nil || len(mirror.Daemons()) != 0 {
				t.Fatal("explicit original member removal before recovery acknowledgment", err)
			}
		}
		cephFSWaitOriginalProcessQuiescence(t, ctx, evidence, kind, daemon, oracle.Client, binding, baseline, policy, observe)
		if evidence == "exited" {
			backlog = checkpoint("after-original-process-quiescence")
		}
		if complete, _ := legacyStatus(ctx); complete {
			t.Fatal("original process report promoted Drained/Released")
		}
		if evidence != "container-removed" {
			before := counter.commands.Load()
			attempt, err := acknowledge(ctx)
			if err == nil || attempt.Acknowledged || attempt.Observation.OriginalQuiescent || counter.commands.Load() != before {
				t.Fatalf("owned %s task was acknowledged or reached CLI: %+v %v", evidence, attempt, err)
			}
			cephFSLogProcessRecoveryAcknowledgment(t, evidence, false, attempt.Observation, counter, counter.commands.Load()-before)
		}
		beforeGate := counter.commands.Load()
		var gateErr error
		if kind == "peer" {
			_, gateErr = mirror.RebootstrapPeer(ctx)
		} else {
			gateErr = mirror.AddDirectory(ctx, directory)
		}
		if gateErr == nil || counter.commands.Load() != beforeGate {
			t.Fatal("unacknowledged original process evidence released overlap gate or reached CLI")
		}
		if current := daemon.ProcessObserverBindingStatus(); current != binding || factoryCalls.Load() != 1 || customizerCalls.Load() != 1 {
			t.Fatal("original lifecycle rebound observer or replayed customizer", current)
		}
		for _, role := range []string{"source", "destination"} {
			client, filesystem := sourceClient, sourceFS.FilesystemName
			if role == "destination" {
				client, filesystem = destinationClient, destinationFS.FilesystemName
			}
			data := multiClusterExecOutput(t, ctx, client, "python3", script, filesystem, "verify", directory, initial.Name)
			cephFSLogOriginalProcessBytes(t, data, filesystem, directory, initial.Name, role, evidence)
		}
		if current := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, initial.Name); current != initial {
			t.Fatal("original source checkpoint changed before recovery", current, initial)
		}
		cephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, directory, backlog.Name, evidence, 10*time.Second)
	}
	// No old observation is supplied to Ack. Each invocation must query fresh G
	// evidence under the fixture gate, with every original full CID now removed.
	if kind == "directory" && !slices.Contains(mirror.Directories, directory) {
		t.Fatal("read-only G/legacy observations reconciled uncertain directory desire before explicit Ack")
	}
	for _, stage := range []string{"accepted", "accepted-repeat"} {
		before := counter.originalRemovals(kind)
		beforeCommands := counter.commands.Load()
		attempt, err := acknowledge(ctx)
		ackCommands := counter.commands.Load() - beforeCommands
		if err != nil || !attempt.Acknowledged || ackCommands <= 0 {
			t.Fatalf("fresh explicit removed-original acknowledgment %s: %+v %v", stage, attempt, err)
		}
		cephFSAssertProcessRecoveryAcknowledgment(t, attempt.Observation, kind, daemon, binding, baseline, expected, directory)
		if err := cephFSCheckOriginalProcessEvidence(ctx, "container-removed", kind, daemon, oracle.Client, binding, baseline, policy); err != nil || counter.originalRemovals(kind) != before || len(mirror.Daemons()) != 0 {
			t.Fatal("accepted acknowledgment changed original removal/evidence/inventory", err)
		}
		if complete, _ := legacyStatus(ctx); complete {
			t.Fatal("accepted acknowledgment promoted Drained/Released")
		}
		if kind == "directory" && slices.Contains(mirror.Directories, directory) {
			t.Fatal("accepted acknowledgment did not reconcile uncertain original directory desire")
		}
		cephFSLogProcessRecoveryAcknowledgment(t, stage, true, attempt.Observation, counter, ackCommands)
	}
	retryWithoutRemovalReplay := func(stage string) {
		t.Helper()
		before := counter.originalRemovals(kind)
		if err := retryBegin(ctx); err != nil {
			t.Fatal("same-generation acknowledged Begin retry", stage, err)
		}
		if err := legacyRemove(ctx); err != nil {
			t.Fatal("same-generation acknowledged legacy retry", stage, err)
		}
		if counter.originalRemovals(kind) != before || before != 1 {
			t.Fatal("acknowledged same-generation retry replayed native deletion", stage)
		}
		log, err := json.Marshal(struct {
			Case             string `json:"case"`
			Stage            string `json:"stage"`
			Kind             string `json:"kind"`
			OriginalRemovals int32  `json:"original_removals"`
		}{t.Name(), stage, kind, before})
		if err != nil {
			t.Fatal("encode same-generation no-remove-replay proof", err)
		}
		t.Logf("CEPHFS_PROCESS_QUIESCENCE_NO_REMOVE_REPLAY %s", log)
	}
	retryWithoutRemovalReplay("empty-inventory")
	newDaemon, err := mirror.AddDaemon(ctx, "recovery-b")
	if err != nil || newDaemon == nil || len(mirror.Daemons()) != 1 || newDaemon == daemon || factoryCalls.Load() != 2 || customizerCalls.Load() != 2 {
		t.Fatal("accepted acknowledgment did not permit exactly one fresh replacement", err)
	}
	newBinding := newDaemon.ProcessObserverBindingStatus()
	if !newBinding.Available || newBinding.State != "bound" || newBinding.EngineID != binding.EngineID || newBinding.ContainerID == binding.ContainerID || newBinding.ContainerID != newDaemon.GetContainerID() {
		t.Fatal("replacement raw binding did not establish a new full CID in original engine", newBinding)
	}
	// Reaffirmation now refuses the nonempty inventory, but cannot revoke the
	// accepted historical decision. Same-generation policy retries still work
	// without adopting the new daemon or consulting the retired raw task again.
	before := counter.commands.Load()
	rejected, err := acknowledge(ctx)
	if err == nil || rejected.Acknowledged || rejected.Observation.OriginalQuiescent || counter.commands.Load() != before {
		t.Fatalf("replacement membership was freshly acknowledged or reached CLI: %+v %v", rejected, err)
	}
	cephFSLogProcessRecoveryAcknowledgment(t, "replacement-refuses-reaffirmation", false, rejected.Observation, counter, counter.commands.Load()-before)
	retryWithoutRemovalReplay("replacement-inventory")
	if kind == "peer" {
		newPeer, err := mirror.RebootstrapPeer(ctx)
		if err != nil || newPeer == "" || newPeer == originalPeer {
			t.Fatal("accepted peer acknowledgment did not permit new peer generation", newPeer, err)
		}
		expected.PeerID = newPeer
	} else if err := mirror.AddDirectory(ctx, directory); err != nil {
		t.Fatal("accepted directory acknowledgment did not permit same-path registration", err)
	}
	// Both modes restore policy only after explicit new daemon admission. The old
	// receipt must guard before CLI and cannot become the new generation's state.
	before = counter.commands.Load()
	complete, legacyErr := legacyStatus(ctx)
	oldProcess, processErr := observe(ctx)
	oldAck, ackErr := acknowledge(ctx)
	if legacyErr == nil || complete || processErr == nil || oldProcess.OriginalQuiescent || oldProcess.PeerID != originalPeer || ackErr == nil || oldAck.Acknowledged || counter.commands.Load() != before {
		t.Fatalf("old receipt adopted recovery generation: legacy=%v process=%+v ack=%+v errors=%v/%v/%v", complete, oldProcess, oldAck, legacyErr, processErr, ackErr)
	}
	policy.expected = expected
	_, newSession := cephFSWaitOriginalProcessFreshSession(t, ctx, newDaemon, oracle.Client, newBinding, policy, baseline.startedAt, baseline.instanceID)
	owners = cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, []string{directory}, 1)
	if owners[directory] != newSession.gid || newSession.gid == baseline.instanceID {
		t.Fatal("recovery did not acquire a fresh native watcher/assignment")
	}
	backlogStatus := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, backlog)
	if backlogStatus.DaemonName != newDaemon.DaemonName || backlogStatus.InstanceID != newSession.gid {
		t.Fatal("recovery backlog public checkpoint did not belong to new daemon")
	}
	var fresh cephfs.MirrorSnapshot
	for _, stage := range []string{"recovered-backlog", "recovered-new", "retained-original"} {
		snapshot := backlog
		if stage == "recovered-new" {
			snapshot = checkpoint("after-explicit-process-recovery")
			fresh = snapshot
			status := cephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, snapshot)
			if status.DaemonName != newDaemon.DaemonName || status.InstanceID != newSession.gid {
				t.Fatal("fresh recovery checkpoint changed replacement owner")
			}
		}
		if stage == "retained-original" {
			snapshot = initial
		}
		for _, role := range []string{"source", "destination"} {
			client, filesystem := sourceClient, sourceFS.FilesystemName
			if role == "destination" {
				client, filesystem = destinationClient, destinationFS.FilesystemName
			}
			cephFSWaitOriginalProcessBytes(t, ctx, client, filesystem, script, directory, snapshot.Name, role, stage)
		}
		if current := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, snapshot.Name); current != snapshot {
			t.Fatal("recovery source checkpoint identity changed", current, snapshot)
		}
	}
	if err := policy.check(ctx, "initial"); err != nil || counter.originalRemovals(kind) != 1 || counter.replyLosses.Load() != 1 || daemon.ProcessObserverBindingStatus() != binding || factoryCalls.Load() != 2 || customizerCalls.Load() != 2 {
		t.Fatal("recovery changed retained binding/removal count/current authority", err)
	}
	if initial.ID == backlog.ID || initial.ID == fresh.ID || backlog.ID == fresh.ID {
		t.Fatal("original, backlog and fresh source checkpoints are not independent")
	}
	oldRaw, err := cephFSOriginalProcessRawInspect(ctx, oracle.Client, binding)
	watchers, watcherErr := cephFSOriginalProcessWatchers(ctx, source, sourceFS.MetadataPool)
	if err != nil || !oldRaw.missing || watcherErr != nil || len(watchers) != 1 || watchers[newSession.gid] != newSession.address {
		t.Fatal("recovery lost independent old-CID absence/new watcher proof", err, watcherErr)
	}
	log, err := json.Marshal(struct {
		Case                    string                `json:"case"`
		Kind                    string                `json:"kind"`
		OriginalPeer            string                `json:"original_peer"`
		RecoveredPeer           string                `json:"recovered_peer"`
		OriginalContainerID     string                `json:"original_cid"`
		ReplacementContainerID  string                `json:"replacement_cid"`
		OriginalInstanceID      string                `json:"original_gid"`
		ReplacementInstanceID   string                `json:"replacement_gid"`
		OriginalRemovals        int32                 `json:"original_removals"`
		ReplyLosses             int32                 `json:"reply_losses"`
		OriginalIntentLostReply bool                  `json:"original_intent_lost_reply"`
		Initial                 cephfs.MirrorSnapshot `json:"initial"`
		Backlog                 cephfs.MirrorSnapshot `json:"backlog"`
		Fresh                   cephfs.MirrorSnapshot `json:"fresh"`
	}{t.Name(), kind, originalPeer, expected.PeerID, binding.ContainerID, newBinding.ContainerID, baseline.instanceID, newSession.gid, counter.originalRemovals(kind), counter.replyLosses.Load(), originalIntentLostReply, initial, backlog, fresh})
	if err != nil {
		t.Fatal("encode explicit process recovery native proof", err)
	}
	t.Logf("CEPHFS_PROCESS_QUIESCENCE_RECOVERED %s", log)
}

func cephFSAssertProcessRecoveryAcknowledgment(t *testing.T, observation cephfs.MirrorProcessQuiescenceStatus, kind string, daemon *cephfs.MirrorDaemon, binding cephfs.MirrorProcessBindingStatus, baseline cephFSOriginalProcessBaseline, expected cephfs.MirrorDirectoryStatus, directory string) {
	t.Helper()
	wantDirectory := ""
	if kind == "directory" {
		wantDirectory = directory
	}
	entry, present := observation.Daemons[daemon.DaemonName]
	started, err := time.Parse(time.RFC3339Nano, entry.StartedAt)
	originalStarted, originalErr := time.Parse(time.RFC3339Nano, baseline.startedAt)
	if !observation.PolicyRemoved || !observation.OriginalQuiescent || observation.PeerID != expected.PeerID || observation.Directory != wantDirectory || observation.SourceFilesystem != expected.SourceFilesystem || observation.DestinationFilesystem != expected.DestinationFilesystem || observation.SourceFilesystemID != expected.SourceFilesystemID || observation.DestinationFilesystemID != expected.DestinationFilesystemID || len(observation.Daemons) != 1 || !present || !entry.OriginalTaskEnded || !entry.OriginalWatcherRetired || !entry.OriginalQuiescent || entry.Evidence != "container-removed" || entry.Problem != "" || entry.EngineID != binding.EngineID || entry.ContainerID != binding.ContainerID || entry.InstanceID != baseline.instanceID || err != nil || originalErr != nil || !started.Equal(originalStarted) {
		t.Fatalf("fresh accepted acknowledgment did not retain exact removed-original proof: %+v", observation)
	}
}

func cephFSLogProcessRecoveryAcknowledgment(t *testing.T, stage string, acknowledged bool, observation cephfs.MirrorProcessQuiescenceStatus, counter *cephFSProcessRecoveryCommandCounter, nativeCommands int32) {
	t.Helper()
	log, err := json.Marshal(struct {
		Case              string                               `json:"case"`
		Stage             string                               `json:"stage"`
		Acknowledged      bool                                 `json:"acknowledged"`
		NativeCommands    int32                                `json:"native_commands"`
		Observation       cephfs.MirrorProcessQuiescenceStatus `json:"observation"`
		PeerRemovals      int32                                `json:"peer_removals"`
		DirectoryRemovals int32                                `json:"directory_removals"`
		ReplyLosses       int32                                `json:"reply_losses"`
	}{t.Name(), stage, acknowledged, nativeCommands, observation, counter.peerRemovals.Load(), counter.directoryRemovals.Load(), counter.replyLosses.Load()})
	if err != nil {
		t.Fatal("encode explicit original process acknowledgment proof", err)
	}
	t.Logf("CEPHFS_PROCESS_QUIESCENCE_ACK %s", log)
}

type cephFSProcessRecoveryCommandCounter struct {
	testcontainers.Container
	filesystem, peerID, directory, kind       string
	commands, peerRemovals, directoryRemovals atomic.Int32
	replyLosses                               atomic.Int32
	lost                                      atomic.Bool
}

var errCephFSProcessRecoveryRemoveReplyLost = errors.New("injected lost real native original CephFS removal reply")

func (c *cephFSProcessRecoveryCommandCounter) originalRemovals(kind string) int32 {
	if kind == "peer" {
		return c.peerRemovals.Load()
	}
	return c.directoryRemovals.Load()
}

func (c *cephFSProcessRecoveryCommandCounter) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.commands.Add(1)
	native := args
	if len(native) >= 3 && native[0] == "ceph" && native[1] == "--connect-timeout" {
		native = native[3:]
	}
	peerTarget := slices.Equal(native, []string{"fs", "snapshot", "mirror", "peer_remove", c.filesystem, c.peerID})
	directoryTarget := slices.Equal(native, []string{"fs", "snapshot", "mirror", "remove", c.filesystem, c.directory})
	if peerTarget {
		c.peerRemovals.Add(1)
	}
	if directoryTarget {
		c.directoryRemovals.Add(1)
	}
	code, reader, err := c.Container.Exec(ctx, args, opts...)
	target := (c.kind == "peer" && peerTarget) || (c.kind == "directory" && directoryTarget)
	if target && err == nil && code == 0 && c.lost.CompareAndSwap(false, true) {
		if reader == nil {
			return 0, nil, errors.New("real successful original removal has no response reader")
		}
		// Execute the exact native command to successful completion and consume
		// its entire real response before reporting one lost reply to Begin.
		if _, readErr := io.Copy(io.Discard, reader); readErr != nil {
			return 0, nil, readErr
		}
		c.replyLosses.Add(1)
		return 0, nil, errCephFSProcessRecoveryRemoveReplyLost
	}
	return code, reader, err
}
