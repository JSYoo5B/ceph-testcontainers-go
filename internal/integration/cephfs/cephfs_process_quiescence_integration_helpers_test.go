//go:build all || (integration && multicluster)

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync/atomic"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	cephfsmirror "github.com/JSYoo5B/ceph-testcontainers-go/cephfs/mirror"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func runCephFSOriginalProcessQuiescence(t *testing.T, host bool, kind string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
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
	const directory = "/daemon-a"
	const script = "/tmp/cephfs-original-process-probe.py"
	for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
		if err := client.CopyToContainer(ctx, []byte(harness.CephFSOriginalProcessProbeScript), script, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	checkpoint := func(name string) cephfsmirror.Snapshot {
		t.Helper()
		harness.MultiClusterExecOutput(t, ctx, sourceClient, "python3", script, sourceFS.FilesystemName, "checkpoint", directory, name)
		return harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, name)
	}
	initial := checkpoint("before-original-process-removal")
	const site = "original-process-target"
	var factoryCalls atomic.Int32
	var customizerCalls atomic.Int32
	customizer := testcontainers.CustomizeRequestOption(func(_ *testcontainers.GenericContainerRequest) error {
		customizerCalls.Add(1)
		return nil
	})
	mirror, err := cephfsmirror.Run(ctx, harness.NativeMirrorImage(t, "cephfs", harness.StaticFacts(source).Images.CephFS), cephfsmirror.WithConfig(cephfsmirror.Config{
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
	}), customizer)
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() {
				for _, daemon := range mirror.Daemons() {
					harness.MultiClusterLogContainer(t, cleanupCtx, daemon)
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
	ready := harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, initial)
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
		harness.CephFSWaitOriginalProcessBytes(t, ctx, client, filesystem, script, directory, initial.Name, role, "initial")
	}
	policy := harness.CephFSOriginalProcessPolicy{Source: source, Destination: destination, SourceFS: sourceFS, DestinationFS: destinationFS, Expected: expected, Directory: directory, Client: mirror.DestinationClientEntity, Site: site}
	if err := policy.Capture(ctx); err != nil {
		t.Fatal("capture independent original native policy", err)
	}
	if err := policy.Check(ctx, "initial"); err != nil {
		t.Fatal("positive original native policy", err)
	}
	original, err := cephFSOriginalProcessRawInspect(ctx, oracle.Client, binding)
	if err != nil || original.Missing || !cephFSOriginalProcessLive(original.State) {
		t.Fatalf("positive original raw process witness: %+v %v", original, err)
	}
	watchers, err := harness.CephFSOriginalProcessWatchers(ctx, source, sourceFS.MetadataPool)
	if err != nil {
		t.Fatal("positive original complete watcher set", err)
	}
	session, err := cephFSOriginalProcessSession(ctx, daemon, expected, true, 1, watchers)
	if err != nil || len(watchers) != 1 || watchers[session.Gid] != session.Address || ready.InstanceID != session.Gid {
		t.Fatalf("complete original native watcher/assignment witness: %+v %+v %v", watchers, session, err)
	}
	// The binding stays attached to the returned Engine/CID, while Begin must
	// capture the currently running task. Restart before admission distinguishes
	// that receipt baseline from the positive creation binding's StartedAt.
	creationStartedAt, creationGID := original.State.StartedAt, session.Gid
	stopTimeout := 10 * time.Second
	if err := daemon.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("pre-Begin normal stop", err)
	}
	if err := daemon.Start(ctx); err != nil {
		t.Fatal("pre-Begin start on original binding", err)
	}
	original, session = cephFSWaitOriginalProcessFreshSession(t, ctx, daemon, oracle.Client, binding, policy, creationStartedAt, creationGID)
	owners = cephFSWaitForMirrorDaemonAssignments(t, ctx, source, sourceFS.FilesystemName, expected.PeerID, []string{directory}, 1)
	ready = harness.CephFSWaitObservedSnapshot(t, ctx, mirror, sourceClient, script, expected, directory, initial)
	if ready.InstanceID != session.Gid || owners[directory] != session.Gid || daemon.ProcessObserverBindingStatus() != binding || factoryCalls.Load() != 1 || customizerCalls.Load() != 1 {
		t.Fatal("pre-Begin restart changed creation binding/customizer or retained old assignment")
	}
	baseline := cephFSOriginalProcessBaseline{StartedAt: original.State.StartedAt, InstanceID: session.Gid}
	var observe func(context.Context) (cephfsmirror.ProcessQuiescenceStatus, error)
	var legacyComplete func(context.Context) bool
	if kind == "peer" {
		receipt, err := mirror.BeginPeerRemoval(ctx, expected.PeerID)
		if receipt == nil || err != nil {
			t.Fatal("public original peer receipt", err)
		}
		observe = receipt.ProcessQuiescence
		legacyComplete = func(ctx context.Context) bool { status, _ := receipt.Status(ctx); return status.Drained }
	} else {
		receipt, err := mirror.BeginDirectoryRemoval(ctx, directory)
		if receipt == nil || err != nil {
			t.Fatal("public original directory receipt", err)
		}
		observe = receipt.ProcessQuiescence
		legacyComplete = func(ctx context.Context) bool { status, _ := receipt.Status(ctx); return status.Released }
	}
	// Do not call live Status/WaitDrained/WaitReleased before stopping. Those are
	// separate completion contracts and could legitimately release overlap gates.
	var pending cephfsmirror.Snapshot
	originalControl := source.Container
	control, err := source.ControlContainer(ctx)
	if err != nil || control != originalControl {
		t.Fatalf("pending-gate oracle needs original single-MON CLI: %v", err)
	}
	counter := &cephFSOriginalProcessCommandCounter{Container: originalControl}
	source.Container = counter
	t.Cleanup(func() { source.Container = originalControl })
	if err := daemon.Stop(ctx, &stopTimeout); err != nil {
		t.Fatal("normal stop of original daemon", err)
	}
	for _, evidence := range []string{"exited", "later-run", "container-removed"} {
		if evidence == "later-run" {
			if err := daemon.Start(ctx); err != nil {
				t.Fatal("start later task in original retained container", err)
			}
		}
		if evidence == "container-removed" {
			if err := daemon.Remove(ctx); err != nil || len(mirror.Daemons()) != 0 {
				t.Fatal("remove original member while retaining its process observer", err)
			}
		}
		cephFSWaitOriginalProcessQuiescence(t, ctx, evidence, kind, daemon, oracle.Client, binding, baseline, policy, observe)
		if evidence == "exited" {
			// Create new work only after original task/watcher retirement is
			// positively observed. Begin's policy ACK alone permits an in-flight
			// original cycle to finish and is not a byte-freeze boundary.
			pending = checkpoint("after-original-process-quiescence")
		}
		if legacyComplete(ctx) {
			t.Fatal("original process evidence changed legacy Drained/Released completion")
		}
		before := counter.commands.Load()
		var gateErr error
		if kind == "peer" {
			_, gateErr = mirror.RebootstrapPeer(ctx)
		} else {
			gateErr = mirror.AddDirectory(ctx, directory)
		}
		if gateErr == nil || counter.commands.Load() != before {
			t.Fatal("ProcessQuiescence released pending overlap gate or reached native CLI")
		}
		if current := daemon.ProcessObserverBindingStatus(); current != binding || factoryCalls.Load() != 1 || customizerCalls.Load() != 1 {
			t.Fatalf("original observer was rebound/closed after %s: %+v", evidence, current)
		}
		for _, client := range []testcontainers.Container{sourceClient, destinationClient} {
			filesystem := sourceFS.FilesystemName
			role := "source"
			if client == destinationClient {
				filesystem = destinationFS.FilesystemName
				role = "destination"
			}
			data := harness.MultiClusterExecOutput(t, ctx, client, "python3", script, filesystem, "verify", directory, initial.Name)
			harness.CephFSLogOriginalProcessBytes(t, data, filesystem, directory, initial.Name, role, evidence)
		}
		if current := harness.CephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, initial.Name); current != initial {
			t.Fatal("original source snapshot identity changed", current, initial)
		}
		harness.CephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, directory, pending.Name, evidence, 10*time.Second)
		t.Logf("original process evidence=%s receipt=%s engine=%s CID=%s StartedAt=%s GID=%s; pending gate retained and snapshot bytes frozen", evidence, kind, binding.EngineID, binding.ContainerID, baseline.StartedAt, baseline.InstanceID)
	}
}

type cephFSOriginalProcessBaseline struct{ StartedAt, InstanceID string }

type cephFSOriginalProcessRawState struct {
	Missing bool
	State   *container.State
}

func cephFSOriginalProcessRawInspect(ctx context.Context, raw *mobycl.Client, binding cephfsmirror.ProcessBindingStatus) (cephFSOriginalProcessRawState, error) {
	var result cephFSOriginalProcessRawState
	before, err := raw.Info(ctx, mobycl.InfoOptions{}) // Uncached GET /info.
	if err != nil || before.Info.ID != binding.EngineID {
		return result, errors.Join(err, errors.New("independent original engine identity differs"))
	}
	inspect, inspectErr := raw.ContainerInspect(ctx, binding.ContainerID, mobycl.ContainerInspectOptions{})
	after, err := raw.Info(ctx, mobycl.InfoOptions{}) // Also required around typed404.
	if err != nil || after.Info.ID != binding.EngineID {
		return result, errors.Join(err, errors.New("independent original engine changed during exact CID inspect"))
	}
	if inspectErr != nil {
		if errdefs.IsNotFound(inspectErr) {
			result.Missing = true
			return result, ctx.Err()
		}
		return result, inspectErr
	}
	if inspect.Container.ID != binding.ContainerID || inspect.Container.State == nil {
		return result, errors.New("independent exact full CID response unavailable")
	}
	copy := *inspect.Container.State
	result.State = &copy
	return result, ctx.Err()
}

func cephFSOriginalProcessLive(state *container.State) bool {
	if state == nil || state.Status != container.StateRunning || !state.Running || state.Pid <= 0 || state.Paused || state.Restarting || state.Dead || state.Error != "" {
		return false
	}
	started, err := time.Parse(time.RFC3339Nano, state.StartedAt)
	return err == nil && !started.IsZero()
}

func cephFSOriginalProcessSameRawState(a, b cephFSOriginalProcessRawState) bool {
	if a.Missing || b.Missing {
		return a.Missing && b.Missing
	}
	return a.State != nil && b.State != nil && a.State.Status == b.State.Status && a.State.Running == b.State.Running && a.State.Paused == b.State.Paused && a.State.Restarting == b.State.Restarting && a.State.Dead == b.State.Dead && a.State.Pid == b.State.Pid && a.State.Error == b.State.Error && a.State.StartedAt == b.State.StartedAt && a.State.FinishedAt == b.State.FinishedAt
}

func cephFSWaitOriginalProcessQuiescence(t *testing.T, ctx context.Context, evidence, kind string, daemon *cephfsmirror.Daemon, raw *mobycl.Client, binding cephfsmirror.ProcessBindingStatus, baseline cephFSOriginalProcessBaseline, policy harness.CephFSOriginalProcessPolicy, observe func(context.Context) (cephfsmirror.ProcessQuiescenceStatus, error)) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last error
	for {
		last = cephFSCheckOriginalProcessEvidence(waitCtx, evidence, kind, daemon, raw, binding, baseline, policy)
		if last == nil {
			status, err := observe(waitCtx)
			expected := policy.Expected
			wantDirectory := ""
			if kind == "directory" {
				wantDirectory = policy.Directory
			}
			if err == nil && status.OriginalQuiescent {
				entry, present := status.Daemons[daemon.DaemonName]
				started, startErr := time.Parse(time.RFC3339Nano, entry.StartedAt)
				originalStarted, originalErr := time.Parse(time.RFC3339Nano, baseline.StartedAt)
				if !status.PolicyRemoved || status.PeerID != expected.PeerID || status.Directory != wantDirectory || status.SourceFilesystem != expected.SourceFilesystem || status.DestinationFilesystem != expected.DestinationFilesystem || status.SourceFilesystemID != expected.SourceFilesystemID || status.DestinationFilesystemID != expected.DestinationFilesystemID || len(status.Daemons) != 1 || !present || !entry.OriginalTaskEnded || !entry.OriginalWatcherRetired || !entry.OriginalQuiescent || entry.Evidence != evidence || entry.Problem != "" || entry.EngineID != binding.EngineID || entry.ContainerID != binding.ContainerID || entry.InstanceID != baseline.InstanceID || startErr != nil || originalErr != nil || !started.Equal(originalStarted) {
					t.Fatalf("public original receipt baseline/evidence differs: %+v", status)
				}
				if err := cephFSCheckOriginalProcessEvidence(waitCtx, evidence, kind, daemon, raw, binding, baseline, policy); err == nil {
					return
				} else {
					last = err
				}
			} else {
				last = errors.Join(err, fmt.Errorf("public original process still pending: %+v", status))
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("original process %s convergence: %v", evidence, errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func cephFSCheckOriginalProcessEvidence(ctx context.Context, evidence, kind string, daemon *cephfsmirror.Daemon, raw *mobycl.Client, binding cephfsmirror.ProcessBindingStatus, baseline cephFSOriginalProcessBaseline, policy harness.CephFSOriginalProcessPolicy) error {
	if err := policy.Check(ctx, kind); err != nil {
		return err
	}
	watchers, err := harness.CephFSOriginalProcessWatchers(ctx, policy.Source, policy.SourceFS.MetadataPool)
	if err != nil {
		return err
	}
	if _, present := watchers[baseline.InstanceID]; present {
		return errors.New("original numeric GID remains in complete native watcher set")
	}
	state, err := cephFSOriginalProcessRawInspect(ctx, raw, binding)
	if err != nil {
		return err
	}
	started, startErr := time.Parse(time.RFC3339Nano, baseline.StartedAt)
	if startErr != nil || started.IsZero() {
		return errors.New("original receipt start is invalid")
	}
	switch evidence {
	case "exited":
		if state.Missing || state.State == nil || state.State.Status != container.StateExited || state.State.Running || state.State.Pid != 0 || state.State.Paused || state.State.Restarting || state.State.Dead || state.State.Error != "" {
			return errors.New("independent original stopped task state is not coherent exited")
		}
		currentStarted, err := time.Parse(time.RFC3339Nano, state.State.StartedAt)
		finished, finishErr := time.Parse(time.RFC3339Nano, state.State.FinishedAt)
		if err != nil || !currentStarted.Equal(started) || finishErr != nil || !finished.After(started) {
			return errors.New("independent original task start/finish generation differs")
		}
	case "later-run":
		if state.Missing || !cephFSOriginalProcessLive(state.State) {
			return errors.New("independent later task is not normally running")
		}
		currentStarted, err := time.Parse(time.RFC3339Nano, state.State.StartedAt)
		if err != nil || !currentStarted.After(started) {
			return errors.New("independent later task did not advance original start generation")
		}
		count := 1
		if kind == "directory" {
			count = 0
		}
		current, err := cephFSOriginalProcessSession(ctx, daemon, policy.Expected, kind == "directory", count, watchers)
		if err != nil || current.Gid == baseline.InstanceID || watchers[current.Gid] != current.Address {
			return errors.Join(err, errors.New("independent later task's new session/watcher unavailable"))
		}
	case "container-removed":
		if !state.Missing {
			return errors.New("independent retained exact full CID did not return typed404")
		}
	default:
		return errors.New("unknown original process evidence stage")
	}
	after, err := cephFSOriginalProcessRawInspect(ctx, raw, binding)
	if err != nil || !cephFSOriginalProcessSameRawState(state, after) {
		return errors.Join(err, errors.New("independent raw task state changed during witness"))
	}
	afterWatchers, err := harness.CephFSOriginalProcessWatchers(ctx, policy.Source, policy.SourceFS.MetadataPool)
	if err != nil || !maps.Equal(watchers, afterWatchers) {
		return errors.Join(err, errors.New("complete native watcher set changed during witness"))
	}
	return policy.Check(ctx, kind)
}

type cephFSOriginalProcessSessionWitness struct{ Address, Gid string }

func cephFSWaitOriginalProcessFreshSession(t *testing.T, ctx context.Context, daemon *cephfsmirror.Daemon, raw *mobycl.Client, binding cephfsmirror.ProcessBindingStatus, policy harness.CephFSOriginalProcessPolicy, previousStartedAt, previousGID string) (cephFSOriginalProcessRawState, cephFSOriginalProcessSessionWitness) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	previous, err := time.Parse(time.RFC3339Nano, previousStartedAt)
	if err != nil || previous.IsZero() {
		t.Fatal("invalid creation start witness")
	}
	var result cephFSOriginalProcessRawState
	var session cephFSOriginalProcessSessionWitness
	var last error
	for {
		last = policy.Check(waitCtx, "initial")
		if last == nil {
			var watchers map[string]string
			watchers, last = harness.CephFSOriginalProcessWatchers(waitCtx, policy.Source, policy.SourceFS.MetadataPool)
			if last == nil {
				if _, oldPresent := watchers[previousGID]; oldPresent || len(watchers) != 1 {
					last = errors.New("pre-Begin original creation watcher not retired")
				} else {
					result, last = cephFSOriginalProcessRawInspect(waitCtx, raw, binding)
					if last == nil && !result.Missing && cephFSOriginalProcessLive(result.State) {
						current, err := time.Parse(time.RFC3339Nano, result.State.StartedAt)
						if err == nil && current.After(previous) {
							session, last = cephFSOriginalProcessSession(waitCtx, daemon, policy.Expected, true, 1, watchers)
							if last == nil && session.Gid != previousGID {
								after, inspectErr := cephFSOriginalProcessRawInspect(waitCtx, raw, binding)
								afterWatchers, watcherErr := harness.CephFSOriginalProcessWatchers(waitCtx, policy.Source, policy.SourceFS.MetadataPool)
								last = errors.Join(inspectErr, watcherErr, policy.Check(waitCtx, "initial"))
								if last == nil && cephFSOriginalProcessSameRawState(result, after) && maps.Equal(watchers, afterWatchers) {
									return result, session
								}
								last = errors.Join(last, errors.New("pre-Begin restarted native witness changed during observation"))
							}
						} else {
							last = errors.New("pre-Begin task has not advanced creation StartedAt")
						}
					} else {
						last = errors.Join(last, errors.New("pre-Begin restarted task is not normally running"))
					}
				}
			}
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("fresh native pre-Begin watcher/start convergence: %v", errors.Join(last, waitCtx.Err()))
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func cephFSOriginalProcessSession(ctx context.Context, daemon *cephfsmirror.Daemon, expected cephfsmirror.DirectoryStatus, hasPeer bool, directoryCount int, watchers map[string]string) (cephFSOriginalProcessSessionWitness, error) {
	var result cephFSOriginalProcessSessionWitness
	fsCommand := fmt.Sprintf("%s@%d", expected.SourceFilesystem, expected.SourceFilesystemID)
	data, err := harness.CephFSOriginalProcessExec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fsCommand)
	if err != nil {
		return result, err
	}
	var session struct {
		State    string                     `json:"state"`
		Address  string                     `json:"rados_inst"`
		Peers    map[string]json.RawMessage `json:"peers"`
		SnapDirs *struct {
			Count *int `json:"dir_count"`
		} `json:"snap_dirs"`
	}
	if json.Unmarshal(data, &session) != nil || session.State != "" || session.Address == "" || session.Peers == nil || session.SnapDirs == nil || session.SnapDirs.Count == nil || *session.SnapDirs.Count != directoryCount {
		return result, errors.New("independent normal native ASOK session unavailable")
	}
	_, present := session.Peers[expected.PeerID]
	if present != hasPeer || len(session.Peers) != map[bool]int{true: 1, false: 0}[hasPeer] {
		return result, errors.New("independent native ASOK original peer presence differs")
	}
	data, err = harness.CephFSOriginalProcessExec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "help")
	if err != nil {
		return result, err
	}
	var catalog map[string]string
	if json.Unmarshal(data, &catalog) != nil || catalog["help"] == "" || catalog["get_command_descriptions"] == "" || catalog["fs mirror status "+fsCommand] == "" {
		return result, errors.New("independent native ASOK catalog unavailable")
	}
	_, present = catalog["fs mirror peer status "+fsCommand+" "+expected.PeerID]
	if present != hasPeer {
		return result, errors.New("independent exact original peer ASOK command presence differs")
	}
	result.Address = session.Address // FSMirror dumps m_addrs, without client GID.
	for gid, address := range watchers {
		if address == session.Address {
			if result.Gid != "" {
				return result, errors.New("independent native session has duplicate watcher identities")
			}
			result.Gid = gid
		}
	}
	if result.Gid == "" {
		return result, errors.New("independent native session numeric GID unavailable")
	}
	return result, nil
}

type cephFSOriginalProcessCommandCounter struct {
	testcontainers.Container
	commands atomic.Int32
}

func (c *cephFSOriginalProcessCommandCounter) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.commands.Add(1)
	return c.Container.Exec(ctx, args, opts...)
}
