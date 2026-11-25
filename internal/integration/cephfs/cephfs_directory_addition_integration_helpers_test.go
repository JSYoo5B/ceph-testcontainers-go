//go:build all || (integration && multicluster)

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/supervised"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Report typed outcomes without printing native stdout, stderr or a nested
// Docker error.
func directoryAdditionObservationDiagnostics(ctx context.Context, err error, elapsed time.Duration) string {
	var timedOut *supervised.ProcessTimeoutError
	var native *ceph.NativeFailure
	var supervisor *supervised.ExitError
	var backend *supervised.BackendError
	outcome := "no command outcome"
	switch {
	case errors.As(err, &timedOut):
		outcome = fmt.Sprintf("native process timeout, exit=%d", timedOut.ExitCode)
	case errors.As(err, &native):
		outcome = fmt.Sprintf("native command exit=%d", native.ExitCode)
	case errors.As(err, &supervisor):
		outcome = fmt.Sprintf("supervisor exit=%d", supervisor.Code)
	case errors.As(err, &backend):
		switch backend.Op {
		case "exec monitor quorum supervisor":
			outcome = "Docker exec failed"
		case "read monitor quorum supervisor":
			outcome = "supervisor report read failed"
		default:
			outcome = "supervisor transport failed"
		}
	}
	_, stopUnconfirmed := supervised.Unconfirmed(err)
	return fmt.Sprintf("elapsed=%s observation_deadline=%t observation_canceled=%t caller_context=%v outcome=%s stop_unconfirmed=%t",
		elapsed.Round(time.Millisecond), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled), ctx.Err(), outcome, stopUnconfirmed)
}

// One monitor read inside Status has a five-second process limit, so a monitor
// that answers nothing for longer fails the observation. The library keeps the
// retained intent across such a query-only failure and expects the caller to
// repeat the observation: one unanswered read says nothing about policy or
// ownership. The limit matches the scenario's other policy waits.
const directoryAdditionObservationLimit = 2 * time.Minute

// observeDirectoryAdditionStatus repeats a read-only Status that is expected
// to succeed while its observation deadline expires under a live caller
// context. Guard failures and native refusals are returned at once.
func observeDirectoryAdditionStatus(t *testing.T, ctx context.Context, receipt *cephfsmirror.DirectoryAddition) (cephfsmirror.DirectoryAdditionStatus, error) {
	t.Helper()
	return retryDirectoryAdditionObservation(ctx, directoryAdditionObservationLimit, 500*time.Millisecond, receipt.Status,
		func(err error, elapsed time.Duration) {
			t.Logf("CEPHFS_DIRECTORY_ADDITION_OBSERVATION_RETRY %v (%s)", err, directoryAdditionObservationDiagnostics(ctx, err, elapsed))
		})
}

// retryDirectoryAdditionObservation returns the first observation that is not
// an expired observation deadline, or the last expired one once limit or the
// caller's context ends. retried receives every failure that is repeated.
func retryDirectoryAdditionObservation(ctx context.Context, limit, interval time.Duration,
	observe func(context.Context) (cephfsmirror.DirectoryAdditionStatus, error), retried func(error, time.Duration),
) (cephfsmirror.DirectoryAdditionStatus, error) {
	waitCtx, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	for {
		started := time.Now()
		status, err := observe(waitCtx)
		if err == nil || !errors.Is(err, context.DeadlineExceeded) || waitCtx.Err() != nil {
			return status, err
		}
		retried(err, time.Since(started))
		select {
		case <-waitCtx.Done():
			return status, err
		case <-time.After(interval):
		}
	}
}

func runCephFSDirectoryAdditionIntent(t *testing.T, host bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Minute)
	defer cancel()
	opts := []testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1))}
	if host {
		opts = append(opts, ceph.WithHostNetwork())
	}
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
	sourceFS, err := cephfs.Start(ctx, source, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	destinationFS, err := cephfs.Start(ctx, destination, cephfs.Config{})
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		if err := harness.ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	expected := harness.CephFSObservedFilesystemIdentity(t, ctx, sourceFS, destinationFS)
	const seed, added, foreign, unapplied = "/daemon-a", "/addition-intent", "/foreign-control", "/unapplied-intent"
	const script = "/tmp/cephfs-directory-addition-probe.py"
	probe := strings.Replace(harness.CephFSOriginalProcessProbeScript, "assert directory == '/daemon-a'", "assert directory in ('/daemon-a', '/addition-intent', '/foreign-control', '/unapplied-intent')", 1)
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
	checkpoint := func(directory, name string) cephfsmirror.Snapshot {
		t.Helper()
		harness.MultiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", directory, name)
		return harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
	}
	verify := func(directory string, snapshot cephfsmirror.Snapshot, stage string) {
		t.Helper()
		for _, role := range []string{"source", "destination"} {
			client, filesystem := sourceClient, sourceFS.FilesystemName
			if role == "destination" {
				client, filesystem = destinationClient, destinationFS.FilesystemName
			}
			harness.CephFSWaitOriginalProcessBytes(t, ctx, client, filesystem, script, directory, snapshot.Name, role, stage)
		}
		if got := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, snapshot.Name); got != snapshot {
			t.Fatalf("independent source checkpoint changed: before=%+v after=%+v", snapshot, got)
		}
	}
	seedCheckpoint := checkpoint(seed, "seed-before-zero-inventory")
	const site = "directory-addition-target"
	var customizers atomic.Int32
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", harness.StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{
		Source: source, Destination: destination, SourceFilesystem: sourceFS.FilesystemName, DestinationFilesystem: destinationFS.FilesystemName,
		DestinationSite: site, Directories: []string{seed}, DaemonCount: 1,
	}), testcontainers.CustomizeRequestOption(func(*testcontainers.GenericContainerRequest) error { customizers.Add(1); return nil }))
	if mirror != nil {
		t.Cleanup(func() {
			harness.ReleaseAfterDiagnostics(t, 2*time.Minute, func(ctx context.Context) {
				for _, daemon := range mirror.Daemons() {
					harness.MultiClusterLogContainer(t, ctx, daemon)
				}
			}, func(ctx context.Context) {
				if err := mirror.Terminate(ctx); err != nil {
					t.Errorf("terminate directory addition fixture: %v", err)
				}
			})
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
	policy := harness.CephFSOriginalProcessPolicy{Source: source, Destination: destination, SourceFS: sourceFS, DestinationFS: destinationFS, Expected: expected, Directory: seed, Client: mirror.DestinationClientEntity, Site: site}
	if err := policy.Capture(ctx); err != nil {
		t.Fatal(err)
	}
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, seed, seedCheckpoint)
	verify(seed, seedCheckpoint, "initial-seed")
	for _, daemon := range mirror.Daemons() {
		if err := daemon.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(mirror.Daemons()) != 0 || mirror.Container != nil || customizers.Load() != 1 {
		t.Fatal("explicit removal did not leave zero owned inventory")
	}
	harness.CephFSDirectoryAdditionWaitZeroWatchers(t, ctx, source, sourceFS.MetadataPool)
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed}); err != nil {
		t.Fatal(err)
	}
	control := source.Container
	if selected, err := source.ControlContainer(ctx); err != nil || selected != control {
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
	harness.CephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, foreign})
	beforeMutations := counter.allAdds.Load()
	if receipt, err := mirror.BeginDirectoryAddition(ctx, foreign); receipt != nil || err == nil || slices.Contains(mirror.Directories, foreign) || counter.allAdds.Load() != beforeMutations {
		t.Fatal("typed intent adopted preexisting foreign native path", err)
	}
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign}); err != nil {
		t.Fatal("preexisting foreign path changed after refusal", err)
	}

	// Prepare the source inode for the later checkpoint. Creating it leaves
	// the path absent from the native mirror policy.
	harness.MultiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "prepare", added, "prepare-added-directory")
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign}); err != nil {
		t.Fatal("prepared source directory changed absent native registration", err)
	}
	applied, err := mirror.BeginDirectoryAddition(ctx, added)
	if applied == nil || !errors.Is(err, errCephFSDirectoryAdditionAppliedReplyLost) || counter.addedAttempts.Load() != 1 || counter.addedDispatches.Load() != 1 || counter.appliedLosses.Load() != 1 || slices.Contains(mirror.Directories, added) {
		t.Fatal("real successful add lost reply did not retain unpublished typed intent", err)
	}
	harness.CephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, foreign, added})
	status, err := observeDirectoryAdditionStatus(t, ctx, applied)
	harness.CephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, false)
	if err != nil || counter.addedDispatches.Load() != 1 || slices.Contains(mirror.Directories, added) || len(mirror.Daemons()) != 0 {
		t.Fatal("read-only visible Status published or resent addition", err)
	}
	cephFSDirectoryAdditionLog(t, "applied-visible-unpublished", status, counter, mirror, policy)
	cephFSDirectoryAdditionAssertPendingGates(t, ctx, mirror, counter, added, expected.PeerID)
	backlog := checkpoint(added, "backlog-with-no-owned-daemon")
	harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, added, backlog.Name, "zero-inventory-add-backlog", 5*time.Second)
	for i := 0; i < 2; i++ {
		same, err := mirror.BeginDirectoryAddition(ctx, added)
		if same != applied || err != nil || counter.addedAttempts.Load() != 1 || counter.addedDispatches.Load() != 1 {
			t.Fatal("fresh applied reconciliation replaced intent or replayed native add", err)
		}
	}
	status, err = observeDirectoryAdditionStatus(t, ctx, applied)
	harness.CephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, true)
	if err != nil || cephFSDirectoryAdditionPathCount(mirror.Directories, added) != 1 {
		t.Fatal("registered intent did not publish exactly one desired path", err)
	}
	cephFSDirectoryAdditionLog(t, "applied-explicit-reconcile", status, counter, mirror, policy)
	// Restrict only this add to a read-only identity and interrupt its first
	// policy readback. A fresh retry must retire the known native refusal,
	// preserve the previous addition and admit an ordinary AddDaemon below.
	const readerKeyring = "/tmp/tc-directory-add-reader.keyring"
	if _, err := source.Ceph(ctx, "auth", "get-or-create", "client.directory-add-reader",
		"mon", "allow r", "mgr", "allow r", "-o", readerKeyring); err != nil {
		t.Fatal("prepare read-only directory-add identity", err)
	}
	const outside = "/outside-after-refusal"
	for _, path := range []string{"/refused-add", outside} {
		refusal := &cephFSRefusedDirectoryAdd{Container: counter, filesystem: sourceFS.FilesystemName, directory: path, keyring: readerKeyring}
		source.Container = refusal
		request, err := mirror.BeginDirectoryAddition(ctx, path)
		var nativeRefusal *ceph.NativeFailure
		if request == nil || !errors.As(err, &nativeRefusal) || !errors.Is(err, errCephFSRefusalReadback) || nativeRefusal.ExitCode != 13 {
			t.Fatalf("expected native permission refusal and interrupted readback: %v", err)
		}
		if path == outside {
			// The caller registers the path independently after the first Begin
			// has returned its known refusal.
			source.Container = counter
			if _, err := source.Ceph(ctx, "fs", "snapshot", "mirror", "add", sourceFS.FilesystemName, path); err != nil {
				t.Fatal(err)
			}
			source.Container = refusal
			if status, err := observeDirectoryAdditionStatus(t, ctx, request); err != nil || !status.PolicyPresent || status.Registered {
				t.Fatalf("outside registration before retry: %+v, %v", status, err)
			}
		}
		if retired, err := mirror.BeginDirectoryAddition(ctx, path); retired != nil || !errors.As(err, &nativeRefusal) || nativeRefusal.ExitCode != 13 || refusal.attempts.Load() != 1 || slices.Contains(mirror.Directories, path) {
			t.Fatalf("refused request for %q was replayed or claimed: attempts %d, err %v", path, refusal.attempts.Load(), err)
		}
	}
	source.Container = counter
	if _, err := source.Ceph(ctx, "auth", "del", "client.directory-add-reader"); err != nil {
		t.Fatal(err)
	}
	harness.MultiClusterExecOutput(t, ctx, control, "rm", "-f", readerKeyring)
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, outside, added}); err != nil {
		t.Fatal("native refusal changed owned or foreign policy", err)
	}
	started := time.Now()
	status, err = observeDirectoryAdditionStatus(t, ctx, applied)
	if err != nil {
		t.Fatalf("observe previous addition after settling refusals: %v (%s)", err, directoryAdditionObservationDiagnostics(ctx, err, time.Since(started)))
	}
	if !status.Registered || !status.PolicyPresent {
		t.Fatalf("settling refusal lost previous addition: %+v", status)
	}
	receiver, err := mirror.AddDaemon(ctx, "addition-receiver")
	if err != nil || receiver == nil || len(mirror.Daemons()) != 1 || customizers.Load() != 2 {
		t.Fatal("registered zero-inventory intent did not permit ordinary new receiver", err)
	}
	owners := harness.CephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, []string{seed, foreign, outside, added}, 1)
	backlogStatus := harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, backlog)
	if backlogStatus.DaemonName != receiver.DaemonName || backlogStatus.InstanceID != owners[added] {
		t.Fatal("backlog public replay attributed to another receiver")
	}
	verify(added, backlog, "recovered-added-backlog")
	fresh := checkpoint(added, "fresh-after-typed-addition")
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, fresh)
	verify(added, fresh, "fresh-added-checkpoint")
	verify(added, backlog, "frozen-added-backlog")
	verify(seed, seedCheckpoint, "retained-seed")
	verify(foreign, foreignCheckpoint, "foreign-policy-retained")
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, outside, added}); err != nil {
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
	harness.CephFSDirectoryAdditionWaitScope(t, ctx, policy, []string{seed, foreign, outside})
	between := checkpoint(added, "backlog-after-cycle-release")
	harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, added, between.Name, "strict-cycle-released", 5*time.Second)
	newAddition, err := mirror.BeginDirectoryAddition(ctx, added)
	if newAddition == nil || newAddition == applied || counter.addedDispatches.Load() != 2 {
		t.Fatal("new explicit add reused prior cycle or failed to create new ownership", err)
	}
	harness.CephFSDirectoryAdditionReconcile(t, ctx, mirror, added, newAddition)
	if counter.addedDispatches.Load() != 2 {
		t.Fatal("acknowledged new cycle was resent during convergence")
	}
	status, err = observeDirectoryAdditionStatus(t, ctx, newAddition)
	harness.CephFSDirectoryAdditionAssertStatus(t, status, expected, added, true, true)
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
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, between)
	verify(added, between, "new-cycle-backlog")
	newCheckpoint := checkpoint(added, "new-cycle-fresh-checkpoint")
	harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, added, newCheckpoint)
	verify(added, newCheckpoint, "new-cycle-fresh")
	verify(added, backlog, "frozen-first-cycle-backlog")
	verify(added, fresh, "frozen-first-cycle-fresh")
	verify(seed, seedCheckpoint, "seed-after-new-generation")
	cephFSDirectoryAdditionLog(t, "new-owned-cycle", status, counter, mirror, policy)

	// Refuse direct removal of any outside path while the fixture is idle.
	beforeCommands = counter.commands.Load()
	for _, path := range []string{foreign, outside} {
		if receipt, err := mirror.BeginDirectoryRemoval(ctx, path); receipt != nil || err == nil || counter.commands.Load() != beforeCommands || slices.Contains(mirror.Directories, path) {
			t.Fatalf("foreign policy %q gained privately owned removal authority: %v", path, err)
		}
	}

	// The wrapper knows that it did not dispatch this add. The fixture only
	// sees a transport failure, and an absent policy does not prove that
	// native dispatch cannot still take effect.
	unappliedCheckpoint := checkpoint(unapplied, "unapplied-request-backlog")
	uncertain, err := mirror.BeginDirectoryAddition(ctx, unapplied)
	if uncertain == nil || !errors.Is(err, errCephFSDirectoryAdditionUnappliedFault) || counter.unappliedAttempts.Load() != 1 || counter.unappliedDispatches.Load() != 0 || counter.unappliedFaults.Load() != 1 || slices.Contains(mirror.Directories, unapplied) {
		t.Fatal("unapplied attempt dispatched or published native ownership", err)
	}
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, outside, added}); err != nil {
		t.Fatal("unapplied native path was not independently absent", err)
	}
	status, err = observeDirectoryAdditionStatus(t, ctx, uncertain)
	harness.CephFSDirectoryAdditionAssertStatus(t, status, expected, unapplied, false, false)
	if err != nil || counter.unappliedDispatches.Load() != 0 {
		t.Fatal("unapplied Status resent or adopted intent", err)
	}
	cephFSDirectoryAdditionLog(t, "unapplied-absent-unpublished", status, counter, mirror, policy)
	harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, unapplied, unappliedCheckpoint.Name, "unapplied-native-absence", 3*time.Second)
	if same, err := mirror.BeginDirectoryAddition(ctx, unapplied); same != uncertain ||
		!errors.Is(err, errCephFSDirectoryAdditionUnappliedFault) || counter.unappliedAttempts.Load() != 1 || counter.unappliedDispatches.Load() != 0 {
		t.Fatal("absent policy caused the uncertain request to be replayed or retired", err)
	}
	status, err = observeDirectoryAdditionStatus(t, ctx, uncertain)
	harness.CephFSDirectoryAdditionAssertStatus(t, status, expected, unapplied, false, false)
	if err != nil || cephFSDirectoryAdditionPathCount(mirror.Directories, unapplied) != 0 {
		t.Fatal("unsettled intent gained ownership", err)
	}
	cephFSDirectoryAdditionAssertPendingGates(t, ctx, mirror, counter, unapplied, expected.PeerID)
	beforeCommands = counter.commands.Load()
	if old, err := newAddition.Status(ctx); err == nil || old.Registered || counter.commands.Load() != beforeCommands {
		t.Fatal("singleton different-path typed intent did not invalidate prior completed receipt")
	}
	harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, unapplied, unappliedCheckpoint.Name, "unapplied-after-retry", 3*time.Second)
	verify(seed, seedCheckpoint, "final-seed-unchanged")
	verify(foreign, foreignCheckpoint, "final-foreign-unchanged")
	if err := harness.CephFSDirectoryAdditionCheckScope(ctx, policy, []string{seed, foreign, outside, added}); err != nil || customizers.Load() != 2 || counter.appliedLosses.Load() != 1 || counter.unappliedFaults.Load() != 1 || counter.removeLosses.Load() != 1 {
		t.Fatal("final original policy/client lifecycle/fault evidence changed", err)
	}
	cephFSDirectoryAdditionLog(t, "unapplied-retry-retains-pending", status, counter, mirror, policy)
}

func cephFSDirectoryAdditionAssertPendingGates(t *testing.T, ctx context.Context, mirror *cephfsmirror.Container, counter *cephFSDirectoryAdditionCommandCounter, directory, peer string) {
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
		if err == nil || !strings.Contains(err.Error(), "explicit CephFS directory addition is incomplete") || counter.commands.Load() != before {
			t.Fatalf("pending %s reached native CLI or was accepted: %v", op.name, err)
		}
	}
}

func cephFSDirectoryAdditionReconcileRemoval(t *testing.T, ctx context.Context, mirror *cephfsmirror.Container, directory string, receipt *cephfsmirror.DirectoryRemoval, counter *cephFSDirectoryAdditionCommandCounter) {
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

var errCephFSRefusalReadback = errors.New("injected failed policy readback after native directory add refusal")

type cephFSRefusedDirectoryAdd struct {
	testcontainers.Container
	filesystem, directory, keyring string
	attempts                       atomic.Int32
	failRead                       atomic.Bool
}

func (f *cephFSRefusedDirectoryAdd) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	native := args
	if len(args) >= 3 && args[0] == "ceph" && args[1] == "--connect-timeout" {
		native = args[3:]
	}
	if slices.Equal(native, []string{"fs", "snapshot", "mirror", "add", f.filesystem, f.directory}) && f.attempts.Add(1) == 1 {
		f.failRead.Store(true)
		args = append(slices.Clone(args), "--name", "client.directory-add-reader", "--keyring", f.keyring)
	}
	if slices.Equal(native, []string{"fs", "snapshot", "mirror", "ls", f.filesystem}) && f.failRead.CompareAndSwap(true, false) {
		return 0, nil, errCephFSRefusalReadback
	}
	return f.Container.Exec(ctx, args, opts...)
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

var errCephFSDirectoryAdditionAppliedReplyLost = errors.New("injected lost real successful native CephFS add reply")

var errCephFSDirectoryAdditionUnappliedFault = errors.New("injected unapplied CephFS add dispatch fault")

var errCephFSDirectoryAdditionRemoveReplyLost = errors.New("injected lost real later native CephFS removal reply")

func cephFSDirectoryAdditionPathCount(paths []string, selected string) int {
	count := 0
	for _, directory := range paths {
		if directory == selected {
			count++
		}
	}
	return count
}

func cephFSDirectoryAdditionLog(t *testing.T, stage string, status cephfsmirror.DirectoryAdditionStatus, c *cephFSDirectoryAdditionCommandCounter, mirror *cephfsmirror.Container, policy harness.CephFSOriginalProcessPolicy) {
	t.Helper()
	data, err := json.Marshal(struct {
		Case, Stage                                                                                                                            string
		Status                                                                                                                                 cephfsmirror.DirectoryAdditionStatus
		OwnedDirectories                                                                                                                       []string
		OwnedDaemons                                                                                                                           int
		AddedAttempts, AddedDispatches, AppliedLosses, UnappliedAttempts, UnappliedDispatches, UnappliedFaults, RemoveDispatches, RemoveLosses int32
		OriginalClusterFSIDs                                                                                                                   [2]string
		OriginalMetadataPoolIDs                                                                                                                [2]int64
		OriginalMetadataPoolNames                                                                                                              [2]string
	}{t.Name(), stage, status, slices.Clone(mirror.Directories), len(mirror.Daemons()), c.addedAttempts.Load(), c.addedDispatches.Load(), c.appliedLosses.Load(), c.unappliedAttempts.Load(), c.unappliedDispatches.Load(), c.unappliedFaults.Load(), c.removeDispatches.Load(), c.removeLosses.Load(), policy.FSIDs, policy.Pools, [2]string{policy.SourceFS.MetadataPool, policy.DestinationFS.MetadataPool}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("CEPHFS_DIRECTORY_ADDITION_NATIVE %s", data)
}
