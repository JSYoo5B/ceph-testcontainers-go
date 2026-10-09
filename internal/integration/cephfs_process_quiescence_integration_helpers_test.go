//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/cephfs"
	"github.com/moby/moby/api/types/container"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testCephFSOriginalProcessQuiescence(t *testing.T, host bool, kind string) {
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
	var observe func(context.Context) (cephfs.MirrorProcessQuiescenceStatus, error)
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
	var pending cephfs.MirrorSnapshot
	originalControl := source.Container
	control, err := source.ControlContainerContext(ctx)
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
			if err := mirror.RemoveDaemon(ctx, daemon.DaemonName); err != nil || len(mirror.Daemons()) != 0 {
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
			data := multiClusterExecOutput(t, ctx, client, "python3", script, filesystem, "verify", directory, initial.Name)
			cephFSLogOriginalProcessBytes(t, data, filesystem, directory, initial.Name, role, evidence)
		}
		if current := cephFSReadSourceSnapshot(t, ctx, sourceClient, sourceFS.FilesystemName, script, directory, initial.Name); current != initial {
			t.Fatal("original source snapshot identity changed", current, initial)
		}
		cephFSAssertOriginalProcessSnapshotAbsent(t, ctx, destinationClient, destinationFS.FilesystemName, script, directory, pending.Name, evidence, 10*time.Second)
		t.Logf("original process evidence=%s receipt=%s engine=%s CID=%s StartedAt=%s GID=%s; pending gate retained and snapshot bytes frozen", evidence, kind, binding.EngineID, binding.ContainerID, baseline.startedAt, baseline.instanceID)
	}
}

type cephFSOriginalProcessBaseline struct{ startedAt, instanceID string }

type cephFSOriginalProcessRawState struct {
	missing bool
	state   *container.State
}

func cephFSOriginalProcessRawInspect(ctx context.Context, raw *mobycl.Client, binding cephfs.MirrorProcessBindingStatus) (cephFSOriginalProcessRawState, error) {
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
			result.missing = true
			return result, ctx.Err()
		}
		return result, inspectErr
	}
	if inspect.Container.ID != binding.ContainerID || inspect.Container.State == nil {
		return result, errors.New("independent exact full CID response unavailable")
	}
	copy := *inspect.Container.State
	result.state = &copy
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
	if a.missing || b.missing {
		return a.missing && b.missing
	}
	return a.state != nil && b.state != nil && a.state.Status == b.state.Status && a.state.Running == b.state.Running && a.state.Paused == b.state.Paused && a.state.Restarting == b.state.Restarting && a.state.Dead == b.state.Dead && a.state.Pid == b.state.Pid && a.state.Error == b.state.Error && a.state.StartedAt == b.state.StartedAt && a.state.FinishedAt == b.state.FinishedAt
}

func cephFSWaitOriginalProcessQuiescence(t *testing.T, ctx context.Context, evidence, kind string, daemon *cephfs.MirrorDaemon, raw *mobycl.Client, binding cephfs.MirrorProcessBindingStatus, baseline cephFSOriginalProcessBaseline, policy cephFSOriginalProcessPolicy, observe func(context.Context) (cephfs.MirrorProcessQuiescenceStatus, error)) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last error
	for {
		last = cephFSCheckOriginalProcessEvidence(waitCtx, evidence, kind, daemon, raw, binding, baseline, policy)
		if last == nil {
			status, err := observe(waitCtx)
			expected := policy.expected
			wantDirectory := ""
			if kind == "directory" {
				wantDirectory = policy.directory
			}
			if err == nil && status.OriginalQuiescent {
				entry, present := status.Daemons[daemon.DaemonName]
				started, startErr := time.Parse(time.RFC3339Nano, entry.StartedAt)
				originalStarted, originalErr := time.Parse(time.RFC3339Nano, baseline.startedAt)
				if !status.PolicyRemoved || status.PeerID != expected.PeerID || status.Directory != wantDirectory || status.SourceFilesystem != expected.SourceFilesystem || status.DestinationFilesystem != expected.DestinationFilesystem || status.SourceFilesystemID != expected.SourceFilesystemID || status.DestinationFilesystemID != expected.DestinationFilesystemID || len(status.Daemons) != 1 || !present || !entry.OriginalTaskEnded || !entry.OriginalWatcherRetired || !entry.OriginalQuiescent || entry.Evidence != evidence || entry.Problem != "" || entry.EngineID != binding.EngineID || entry.ContainerID != binding.ContainerID || entry.InstanceID != baseline.instanceID || startErr != nil || originalErr != nil || !started.Equal(originalStarted) {
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

func cephFSCheckOriginalProcessEvidence(ctx context.Context, evidence, kind string, daemon *cephfs.MirrorDaemon, raw *mobycl.Client, binding cephfs.MirrorProcessBindingStatus, baseline cephFSOriginalProcessBaseline, policy cephFSOriginalProcessPolicy) error {
	if err := policy.check(ctx, kind); err != nil {
		return err
	}
	watchers, err := cephFSOriginalProcessWatchers(ctx, policy.source, policy.sourceFS.MetadataPool)
	if err != nil {
		return err
	}
	if _, present := watchers[baseline.instanceID]; present {
		return errors.New("original numeric GID remains in complete native watcher set")
	}
	state, err := cephFSOriginalProcessRawInspect(ctx, raw, binding)
	if err != nil {
		return err
	}
	started, startErr := time.Parse(time.RFC3339Nano, baseline.startedAt)
	if startErr != nil || started.IsZero() {
		return errors.New("original receipt start is invalid")
	}
	switch evidence {
	case "exited":
		if state.missing || state.state == nil || state.state.Status != container.StateExited || state.state.Running || state.state.Pid != 0 || state.state.Paused || state.state.Restarting || state.state.Dead || state.state.Error != "" {
			return errors.New("independent original stopped task state is not coherent exited")
		}
		currentStarted, err := time.Parse(time.RFC3339Nano, state.state.StartedAt)
		finished, finishErr := time.Parse(time.RFC3339Nano, state.state.FinishedAt)
		if err != nil || !currentStarted.Equal(started) || finishErr != nil || !finished.After(started) {
			return errors.New("independent original task start/finish generation differs")
		}
	case "later-run":
		if state.missing || !cephFSOriginalProcessLive(state.state) {
			return errors.New("independent later task is not normally running")
		}
		currentStarted, err := time.Parse(time.RFC3339Nano, state.state.StartedAt)
		if err != nil || !currentStarted.After(started) {
			return errors.New("independent later task did not advance original start generation")
		}
		count := 1
		if kind == "directory" {
			count = 0
		}
		current, err := cephFSOriginalProcessSession(ctx, daemon, policy.expected, kind == "directory", count, watchers)
		if err != nil || current.gid == baseline.instanceID || watchers[current.gid] != current.address {
			return errors.Join(err, errors.New("independent later task's new session/watcher unavailable"))
		}
	case "container-removed":
		if !state.missing {
			return errors.New("independent retained exact full CID did not return typed404")
		}
	default:
		return errors.New("unknown original process evidence stage")
	}
	after, err := cephFSOriginalProcessRawInspect(ctx, raw, binding)
	if err != nil || !cephFSOriginalProcessSameRawState(state, after) {
		return errors.Join(err, errors.New("independent raw task state changed during witness"))
	}
	afterWatchers, err := cephFSOriginalProcessWatchers(ctx, policy.source, policy.sourceFS.MetadataPool)
	if err != nil || !maps.Equal(watchers, afterWatchers) {
		return errors.Join(err, errors.New("complete native watcher set changed during witness"))
	}
	return policy.check(ctx, kind)
}

type cephFSOriginalProcessSessionWitness struct{ address, gid string }

func cephFSWaitOriginalProcessFreshSession(t *testing.T, ctx context.Context, daemon *cephfs.MirrorDaemon, raw *mobycl.Client, binding cephfs.MirrorProcessBindingStatus, policy cephFSOriginalProcessPolicy, previousStartedAt, previousGID string) (cephFSOriginalProcessRawState, cephFSOriginalProcessSessionWitness) {
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
		last = policy.check(waitCtx, "initial")
		if last == nil {
			var watchers map[string]string
			watchers, last = cephFSOriginalProcessWatchers(waitCtx, policy.source, policy.sourceFS.MetadataPool)
			if last == nil {
				if _, oldPresent := watchers[previousGID]; oldPresent || len(watchers) != 1 {
					last = errors.New("pre-Begin original creation watcher not retired")
				} else {
					result, last = cephFSOriginalProcessRawInspect(waitCtx, raw, binding)
					if last == nil && !result.missing && cephFSOriginalProcessLive(result.state) {
						current, err := time.Parse(time.RFC3339Nano, result.state.StartedAt)
						if err == nil && current.After(previous) {
							session, last = cephFSOriginalProcessSession(waitCtx, daemon, policy.expected, true, 1, watchers)
							if last == nil && session.gid != previousGID {
								after, inspectErr := cephFSOriginalProcessRawInspect(waitCtx, raw, binding)
								afterWatchers, watcherErr := cephFSOriginalProcessWatchers(waitCtx, policy.source, policy.sourceFS.MetadataPool)
								last = errors.Join(inspectErr, watcherErr, policy.check(waitCtx, "initial"))
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

func cephFSOriginalProcessSession(ctx context.Context, daemon *cephfs.MirrorDaemon, expected cephfs.MirrorDirectoryStatus, hasPeer bool, directoryCount int, watchers map[string]string) (cephFSOriginalProcessSessionWitness, error) {
	var result cephFSOriginalProcessSessionWitness
	fsCommand := fmt.Sprintf("%s@%d", expected.SourceFilesystem, expected.SourceFilesystemID)
	data, err := cephFSOriginalProcessExec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", fsCommand)
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
	data, err = cephFSOriginalProcessExec(ctx, daemon, "ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "help")
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
	result.address = session.Address // FSMirror dumps m_addrs, without client GID.
	for gid, address := range watchers {
		if address == session.Address {
			if result.gid != "" {
				return result, errors.New("independent native session has duplicate watcher identities")
			}
			result.gid = gid
		}
	}
	if result.gid == "" {
		return result, errors.New("independent native session numeric GID unavailable")
	}
	return result, nil
}

// Parse the complete result, including unrelated/new identities. Never infer
// old GID retirement by searching only its old address or current inventory.
func cephFSOriginalProcessWatchers(ctx context.Context, source *ceph.Container, pool string) (map[string]string, error) {
	control, err := source.ControlContainerContext(ctx)
	if err != nil {
		return nil, err
	}
	data, err := cephFSOriginalProcessExec(ctx, control, "rados", "--pool", pool, "listwatchers", "cephfs_mirror")
	if err != nil {
		return nil, err
	}
	result := make(map[string]string)
	addresses := make(map[string]bool)
	if strings.TrimSpace(string(data)) == "" {
		return result, nil
	}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 3 || !strings.HasPrefix(fields[0], "watcher=") || !strings.HasPrefix(fields[1], "client.") || !strings.HasPrefix(fields[2], "cookie=") {
			return nil, errors.New("complete native watcher output malformed")
		}
		address, gid := strings.TrimPrefix(fields[0], "watcher="), strings.TrimPrefix(fields[1], "client.")
		number, err := strconv.ParseUint(gid, 10, 64)
		_, cookieErr := strconv.ParseUint(strings.TrimPrefix(fields[2], "cookie="), 10, 64)
		_, duplicate := result[gid]
		if address == "" || err != nil || number == 0 || strconv.FormatUint(number, 10) != gid || cookieErr != nil || duplicate || addresses[address] {
			return nil, errors.New("complete native watcher identity is malformed or duplicated")
		}
		result[gid], addresses[address] = address, true
	}
	return result, nil
}

type cephFSOriginalProcessPolicy struct {
	source, destination     *ceph.Container
	sourceFS, destinationFS *cephfs.Filesystem
	expected                cephfs.MirrorDirectoryStatus
	directory, client, site string
	fsids                   [2]string
	pools                   [2]int64
}

func (p *cephFSOriginalProcessPolicy) capture(ctx context.Context) error {
	for i, cluster := range []*ceph.Container{p.source, p.destination} {
		data, err := cluster.Ceph(ctx, "fsid")
		if err != nil {
			return err
		}
		p.fsids[i] = strings.TrimSpace(string(data))
		filesystem := []*cephfs.Filesystem{p.sourceFS, p.destinationFS}[i]
		data, err = cluster.Ceph(ctx, "fs", "get", filesystem.FilesystemName, "--format", "json")
		if err != nil {
			return err
		}
		var fs struct {
			MDSMap *struct {
				Pool *int64 `json:"metadata_pool"`
			} `json:"mdsmap"`
		}
		if json.Unmarshal(data, &fs) != nil || fs.MDSMap == nil || fs.MDSMap.Pool == nil || *fs.MDSMap.Pool <= 0 {
			return errors.New("independent native metadata pool identity unavailable")
		}
		p.pools[i] = *fs.MDSMap.Pool
	}
	if p.fsids[0] == "" || p.fsids[1] == "" || p.fsids[0] == p.fsids[1] {
		return errors.New("independent native cluster FSIDs unavailable or equal")
	}
	return nil
}

func (p cephFSOriginalProcessPolicy) check(ctx context.Context, kind string) error {
	var sourcePeers map[string]json.RawMessage
	for i, cluster := range []*ceph.Container{p.source, p.destination} {
		data, err := cluster.Ceph(ctx, "fsid")
		if err != nil || strings.TrimSpace(string(data)) != p.fsids[i] {
			return errors.Join(err, errors.New("independent native original cluster FSID changed"))
		}
		filesystem := []*cephfs.Filesystem{p.sourceFS, p.destinationFS}[i]
		id := []int{p.expected.SourceFilesystemID, p.expected.DestinationFilesystemID}[i]
		data, err = cluster.Ceph(ctx, "fs", "get", filesystem.FilesystemName, "--format", "json")
		if err != nil {
			return err
		}
		var fs struct {
			ID     *int `json:"id"`
			MDSMap *struct {
				Name *string `json:"fs_name"`
				Pool *int64  `json:"metadata_pool"`
			} `json:"mdsmap"`
			MirrorInfo *struct {
				Peers map[string]json.RawMessage `json:"peers"`
			} `json:"mirror_info"`
		}
		if json.Unmarshal(data, &fs) != nil || fs.ID == nil || *fs.ID != id || fs.MDSMap == nil || fs.MDSMap.Name == nil || *fs.MDSMap.Name != filesystem.FilesystemName || fs.MDSMap.Pool == nil || *fs.MDSMap.Pool != p.pools[i] {
			return errors.New("independent native original filesystem/metadata pool changed")
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
			return errors.New("independent native pool catalog malformed")
		}
		matches := 0
		for _, pool := range pools {
			if pool.Name == filesystem.MetadataPool {
				if pool.ID != p.pools[i] {
					return errors.New("independent native original metadata pool name was rebound")
				}
				matches++
			}
		}
		if matches != 1 {
			return errors.New("independent native original metadata pool name unavailable")
		}
		if i == 0 {
			if fs.MirrorInfo == nil || fs.MirrorInfo.Peers == nil {
				return errors.New("independent source MON mirroring peer policy unavailable")
			}
			sourcePeers = fs.MirrorInfo.Peers
		}
	}
	wantPeer := kind != "peer"
	if err := p.checkPeers(sourcePeers, true, wantPeer); err != nil {
		return err
	}
	data, err := p.source.Ceph(ctx, "fs", "snapshot", "mirror", "peer_list", p.sourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var peers map[string]json.RawMessage
	if json.Unmarshal(data, &peers) != nil || peers == nil {
		return errors.New("independent MGR peer policy malformed")
	}
	if err := p.checkPeers(peers, false, wantPeer); err != nil {
		return err
	}
	data, err = p.source.Ceph(ctx, "fs", "snapshot", "mirror", "ls", p.sourceFS.FilesystemName)
	if err != nil {
		return err
	}
	var directories []string
	if json.Unmarshal(data, &directories) != nil || directories == nil {
		return errors.New("independent MGR directory policy malformed")
	}
	wantDirectory := kind != "directory"
	if len(directories) != map[bool]int{true: 1, false: 0}[wantDirectory] || (wantDirectory && directories[0] != p.directory) {
		return errors.New("independent original directory policy differs")
	}
	return ctx.Err()
}

func (p cephFSOriginalProcessPolicy) checkPeers(peers map[string]json.RawMessage, remote, present bool) error {
	if peers == nil || len(peers) != map[bool]int{true: 1, false: 0}[present] {
		return errors.New("independent original peer policy presence differs")
	}
	if !present {
		return nil
	}
	data, found := peers[p.expected.PeerID]
	if !found {
		return errors.New("independent original peer UUID substituted")
	}
	if remote {
		var entry struct {
			Remote json.RawMessage `json:"remote"`
		}
		if json.Unmarshal(data, &entry) != nil || len(entry.Remote) == 0 {
			return errors.New("independent original remote peer tuple unavailable")
		}
		data = entry.Remote
	}
	var tuple map[string]string
	if json.Unmarshal(data, &tuple) != nil {
		return errors.New("independent original peer tuple malformed")
	}
	key := "site_name"
	if remote {
		key = "cluster_name"
	}
	if tuple["client_name"] != p.client || tuple[key] != p.site || tuple["fs_name"] != p.destinationFS.FilesystemName {
		return errors.New("independent original destination peer tuple substituted")
	}
	return nil
}

func cephFSOriginalProcessExec(ctx context.Context, client testcontainers.Container, args ...string) ([]byte, error) {
	code, reader, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, errors.New("independent native command returned no output reader")
	}
	data, err := io.ReadAll(reader)
	if err != nil || code != 0 {
		return nil, errors.Join(err, fmt.Errorf("independent native command exited %d", code))
	}
	return data, ctx.Err()
}

type cephFSOriginalProcessCommandCounter struct {
	testcontainers.Container
	commands atomic.Int32
}

func (c *cephFSOriginalProcessCommandCounter) Exec(ctx context.Context, args []string, opts ...tcexec.ProcessOption) (int, io.Reader, error) {
	c.commands.Add(1)
	return c.Container.Exec(ctx, args, opts...)
}

func cephFSWaitOriginalProcessBytes(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, directory, snapshot, role, stage string) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last error
	for {
		var data []byte
		if data, last = cephFSOriginalProcessExec(waitCtx, client, "python3", script, filesystem, "verify", directory, snapshot); last == nil {
			cephFSLogOriginalProcessBytes(t, data, filesystem, directory, snapshot, role, stage)
			return
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("independent mirrored checkpoint bytes: %v", errors.Join(last, waitCtx.Err()))
		case <-time.After(time.Second):
		}
	}
}

func cephFSLogOriginalProcessBytes(t *testing.T, data []byte, filesystem, directory, snapshot, role, stage string) {
	t.Helper()
	var proof struct {
		Snapshot string `json:"snapshot"`
		Bytes    int    `json:"bytes"`
		SHA256   string `json:"sha256"`
	}
	// Validate the successful native JSON instead of merely preserving opaque
	// output. The source/destination payload and hash must match the frozen
	// expected bytes independently of the Python verifier's exit status.
	expected := []byte(strings.Repeat(snapshot+":"+directory+":", 1024))
	for value := 0; value < 256; value++ {
		expected = append(expected, byte(value))
	}
	digest := sha256.Sum256(expected)
	if err := json.Unmarshal(data, &proof); err != nil || proof.Snapshot != snapshot || proof.Bytes <= 0 || proof.Bytes != len(expected) || proof.SHA256 != fmt.Sprintf("%x", digest) || (role != "source" && role != "destination") {
		t.Fatalf("invalid positive original snapshot byte/hash proof: role=%s stage=%s output=%s error=%v", role, stage, data, err)
	}
	log, err := json.Marshal(struct {
		Case       string `json:"case"`
		Role       string `json:"role"`
		Stage      string `json:"stage"`
		Filesystem string `json:"filesystem"`
		Directory  string `json:"directory"`
		Snapshot   string `json:"snapshot"`
		Bytes      int    `json:"bytes"`
		SHA256     string `json:"sha256"`
	}{t.Name(), role, stage, filesystem, directory, proof.Snapshot, proof.Bytes, proof.SHA256})
	if err != nil {
		t.Fatal("encode positive original snapshot byte/hash proof", err)
	}
	t.Logf("CEPHFS_ORIGINAL_PROCESS_BYTES %s", log)
}

func cephFSAssertOriginalProcessSnapshotAbsent(t *testing.T, ctx context.Context, client testcontainers.Container, filesystem, script, directory, snapshot, stage string, duration time.Duration) {
	t.Helper()
	started := time.Now()
	deadline := started.Add(duration)
	checks := 0
	for {
		data := multiClusterExecOutput(t, ctx, client, "python3", script, filesystem, "absent", directory, snapshot)
		var proof struct {
			Snapshot string `json:"snapshot"`
			Absent   bool   `json:"absent"`
		}
		if err := json.Unmarshal(data, &proof); err != nil || proof.Snapshot != snapshot || !proof.Absent {
			t.Fatalf("invalid independent removed-authority snapshot absence proof: stage=%s output=%s error=%v", stage, data, err)
		}
		checks++
		if time.Now().After(deadline) && checks >= 2 {
			log, err := json.Marshal(struct {
				Case       string `json:"case"`
				Role       string `json:"role"`
				Stage      string `json:"stage"`
				Filesystem string `json:"filesystem"`
				Directory  string `json:"directory"`
				Snapshot   string `json:"snapshot"`
				Absent     bool   `json:"absent"`
				Count      int    `json:"count"`
				DurationMS int64  `json:"duration_ms"`
			}{t.Name(), "destination", stage, filesystem, directory, snapshot, true, checks, time.Since(started).Milliseconds()})
			if err != nil {
				t.Fatal("encode aggregate independent snapshot absence proof", err)
			}
			t.Logf("CEPHFS_ORIGINAL_PROCESS_ABSENCE %s", log)
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

const cephFSOriginalProcessProbeScript = `import cephfs
import hashlib
import json
import os
import sys

filesystem, phase, directory, snapshot = sys.argv[1:]
assert directory == '/daemon-a'
assert snapshot and snapshot not in ('.', '..') and '/' not in snapshot and '\x00' not in snapshot
fs = cephfs.LibCephFS(conffile='/etc/ceph/ceph.conf', auth_id='admin')
fs.conf_set('client_mount_timeout', '30')
fs.mount(filesystem_name=filesystem.encode())
root = directory.encode()
path = root + b'/.snap/' + snapshot.encode()
expected = (snapshot + ':' + directory + ':').encode() * 1024 + bytes(range(256))
try:
    if phase == 'checkpoint':
        try:
            fs.mkdir(root, 0o755)
        except cephfs.ObjectExists:
            pass
        fd = fs.open(root + b'/payload', os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o640)
        try:
            assert fs.write(fd, expected, 0) == len(expected)
            fs.fsync(fd, False)
        finally:
            fs.close(fd)
        fs.sync_fs()
        fs.mkdir(path, 0o755)
    elif phase == 'snapshot-checkpoint':
        info = fs.snap_info(path)
        assert type(info['id']) is int and 1 <= info['id'] <= 18446744073709551613
        print(json.dumps({'id': info['id'], 'name': snapshot}, sort_keys=True))
    elif phase == 'verify':
        fd = fs.open(path + b'/payload', os.O_RDONLY)
        try:
            actual = fs.read(fd, 0, len(expected) + 1)
            assert actual == expected, 'original snapshot bytes changed'
            print(json.dumps({'snapshot': snapshot, 'bytes': len(actual), 'sha256': hashlib.sha256(actual).hexdigest()}, sort_keys=True))
        finally:
            fs.close(fd)
    elif phase == 'absent':
        try:
            fs.stat(path)
        except cephfs.ObjectNotFound:
            print(json.dumps({'snapshot': snapshot, 'absent': True}, sort_keys=True))
        else:
            raise AssertionError('removed authority copied a new snapshot')
    else:
        raise AssertionError('unknown phase')
finally:
    fs.shutdown()
`
