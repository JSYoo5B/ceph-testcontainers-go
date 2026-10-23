//go:build all || (integration && topology && (!ci || (ci_topology && (!ci_batch || ci_batch_osd_abrupt_write))))

//ci: timeout=20m job-timeout=30 image-roles=essential,client

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/containerd/errdefs"
	mobycl "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// TestOSDAbruptWriteRetainedStore checks acknowledged data and two caller-
// outstanding writes across actual SIGKILL of the only OSD. The ordinary local
// volume and Docker container survive; this is not a power-loss experiment.
func TestOSDAbruptWriteRetainedStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = docker.Close() })
	image, opts := integrationImages(t)
	opts = append(opts, ceph.WithNoInitialOSDs(), ceph.WithOSDVolumeStorage())
	cluster, err := ceph.Run(ctx, image, opts...)
	terminated := false
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			if t.Failed() {
				for _, osd := range cluster.OSDs() {
					osdMemoryFailureLogs(t, context.Background(), osd)
				}
			}
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Minute)
			defer done()
			if err := cluster.Terminate(cleanup); err != nil {
				t.Error("abrupt cleanup independently failed", err)
			}
		})
	}
	if err != nil {
		t.Fatal("partial fixture setup", err)
	}
	// A rejected, canceled addition must not register or allocate an OSD.
	canceled, stop := context.WithCancel(ctx)
	stop()
	if osd, err := cluster.AddOSD(canceled); !errors.Is(err, context.Canceled) || osd != nil || len(cluster.OSDs()) != 0 {
		t.Fatal("partial setup cancellation changed ownership", err)
	}
	osd, err := cluster.AddOSD(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const pool, ledger = "abrupt-data", "/tmp/tc-abrupt-write.jsonl"
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, Application: "rados", Replicas: 1, MinSize: 1, PGNum: 1}); err != nil {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	client, err := cluster.ClientContainerContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	volume, _ := osdVolumeBacking(t, ctx, docker.Client, cluster)
	registered := osdMemoryIdentities(t, ctx, cluster)
	quorum, err := cluster.QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	storePath := "/var/lib/ceph/osd/ceph-" + strconv.Itoa(osd.ID)
	store := osdMemoryExec(t, ctx, osd, "sh", "-c", `cat "$1/whoami" "$1/fsid" "$1/ceph_fsid"; stat -c '%i:%s' "$1/block"`, "identity", storePath)
	initial := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
	clientRaw := osdMemoryInspect(t, ctx, docker.Client, client.GetContainerID())
	platform := osdMemoryExec(t, ctx, client, "uname", "-sm")
	t.Logf("OSD_ABRUPT_RUNTIME ceph=%s platform=%s essential_image=%s client_image=%s cid=%s fsid=%s osd=%d uuid=%s volume=%s store=%q", cluster.CephVersion(), platform, initial.Image, clientRaw.Image, osd.GetContainerID(), quorum.MonMap.FSID, osd.ID, registered[osd.ID], volume, store)
	noout, err := cluster.OverrideOSDFlag(ctx, "noout", true)
	if err != nil {
		t.Fatal(err)
	}
	abruptPhase(t, ctx, cluster, client, "seed", pool, ledger, nil)
	var baseline map[string]abruptObjectState
	abruptPhase(t, ctx, cluster, client, "read", pool, ledger, &baseline)
	abruptPhase(t, ctx, cluster, client, "presubmit", pool, ledger, nil)
	var before map[string]abruptObjectState
	abruptPhase(t, ctx, cluster, client, "read", pool, ledger, &before)
	if !maps.Equal(baseline, before) {
		t.Fatal("before-submission rejection changed data")
	}

	// Clean shutdown is an explicit control, not the disruption implementation.
	grace := 30 * time.Second
	if err := osd.Stop(ctx, &grace); err != nil {
		t.Fatal("clean stop", err)
	}
	clean := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
	if clean.State.Running || clean.State.ExitCode != 0 || clean.State.OOMKilled {
		t.Fatalf("clean control did not exit cleanly: %+v", clean.State)
	}
	osdVolumeWaitState(t, ctx, cluster, registered, false)
	if err := osd.Start(ctx); err != nil {
		t.Fatal("clean control restart", err)
	}
	osdVolumeWaitState(t, ctx, cluster, registered, true)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	var cleanRead map[string]abruptObjectState
	abruptPhase(t, ctx, cluster, client, "read", pool, ledger, &cleanRead)
	if !maps.Equal(baseline, cleanRead) {
		t.Fatal("clean control changed data/version/metadata")
	}
	t.Log("OSD_ABRUPT_CONTROL presubmit=no_requests clean_stop=exit_0 independent_read=exact")

	poolStatus, err := cluster.PoolStatus(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	pg := fmt.Sprintf("%d.0", poolStatus.ID)
	restoreDelay := abruptDispatchDelay(t, ctx, cluster, fmt.Sprintf("osd.%d", osd.ID))
	actor := startNative(ctx, cluster, client, abruptCommand("actor", pool, ledger))
	actorFinished := false
	t.Cleanup(func() {
		if actorFinished {
			return
		}
		actor.cancel()
		select {
		case outcome := <-actor.done:
			t.Logf("OSD_ABRUPT_ORIGINAL cleanup_exit=%d error=%v", outcome.result.ExitCode, outcome.err)
		case <-time.After(30 * time.Second):
			t.Error("original actor cleanup did not confirm stop")
		}
	})
	var events []abruptWriteEvent
	var witnesses []abruptWitness
	daemon := fmt.Sprintf("osd.%d", osd.ID)
	for deadline := time.Now().Add(time.Minute); ; {
		events = abruptLedger(t, ctx, cluster, client, pool, ledger)
		var writer string
		returned := 0
		for _, event := range events {
			switch event.Event {
			case "intent":
				writer = event.Client
			case "api_return":
				returned++
			case "completed", "callback":
				t.Fatalf("original request finished before disruption: %+v", event)
			}
		}
		if returned == 2 {
			if witnesses = abruptWitnesses(t, ctx, cluster, docker.Client, osd, daemon, pg, writer); len(witnesses) == 2 {
				// A request that completed while the dump was read is no witness.
				if again := abruptLedger(t, ctx, cluster, client, pool, ledger); !slices.ContainsFunc(again, func(e abruptWriteEvent) bool { return e.Event == "completed" || e.Event == "callback" }) {
					events = again
					break
				}
				t.Fatal("original request finished while its witness was read")
			}
		}
		select {
		case outcome := <-actor.done:
			actorFinished = true
			t.Fatalf("actor ended before required phase: %+v", outcome)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("required OSD in-flight phase not observed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	encodedWitnesses, _ := json.Marshal(witnesses)
	t.Logf("OSD_ABRUPT_WITNESS pg=%s witnesses=%s", pg, encodedWitnesses)
	var beforeKill int64
	abruptPhase(t, ctx, cluster, client, "clock", pool, ledger, &beforeKill)
	running := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
	if !running.State.Running || running.State.Pid <= 0 {
		t.Fatal("captured daemon is not running")
	}
	_, killErr := docker.ContainerKill(ctx, osd.GetContainerID(), mobycl.ContainerKillOptions{Signal: "KILL"})
	t.Logf("OSD_ABRUPT_KILL cid=%s signal=KILL before_ns=%d error=%v", osd.GetContainerID(), beforeKill, killErr)
	if killErr != nil {
		t.Fatal("original SIGKILL error", killErr)
	}
	for deadline := time.Now().Add(30 * time.Second); ; {
		raw := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
		if !raw.State.Running {
			t.Logf("OSD_ABRUPT_EXIT state=%+v previous_pid=%d started_at=%s", raw.State, running.State.Pid, running.State.StartedAt)
			if raw.State.ExitCode != 137 || raw.State.OOMKilled {
				t.Fatal("SIGKILL exit not observed")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not exit after SIGKILL")
		}
		time.Sleep(50 * time.Millisecond)
	}
	var afterExit int64
	abruptPhase(t, ctx, cluster, client, "clock", pool, ledger, &afterExit)
	// Stop the original client while the OSD is down. It must not reconnect
	// and resubmit a librados pending request when the OSD comes back.
	actor.cancel()
	var outcome nativeOutcome
	select {
	case outcome = <-actor.done:
		actorFinished = true
	case <-time.After(30 * time.Second):
		t.Fatal("original writer stop unconfirmed; refuse restart")
	}
	t.Logf("OSD_ABRUPT_ORIGINAL exit=%d error=%v stdout=%s stderr=%s", outcome.result.ExitCode, outcome.err, outcome.result.Stdout, outcome.result.Stderr)
	var unstopped *ceph.NativeCommandUnstoppedError
	if !errors.Is(outcome.err, context.Canceled) || errors.As(outcome.err, &unstopped) {
		t.Fatal("original writer was not confirmed stopped", outcome.err)
	}
	events = abruptLedger(t, ctx, cluster, client, pool, ledger)
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_ABRUPT_LEDGER before_kill_ns=%d after_exit_ns=%d channel=%s events=%s", beforeKill, afterExit, abruptChannel(events), encoded)
	if err := abruptOverlap(events, beforeKill, afterExit); err != nil {
		t.Fatal("required abrupt window not established", err)
	}
	restoreDelay()
	osdVolumeWaitState(t, ctx, cluster, registered, false)
	if status, err := cluster.Status(ctx); err == nil {
		t.Logf("OSD_ABRUPT_RECOVERY phase=down status=%+v", status)
	} else {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled recovery wait", err)
	}
	if err := osd.Start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled restart", err)
	}
	if len(cluster.OSDs()) != 1 || cluster.OSDs()[0] != osd || osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID()).State.Running {
		t.Fatal("canceled restart lost ownership or started daemon")
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal("recovery restart independently failed", err)
	}
	osdVolumeWaitState(t, ctx, cluster, registered, true)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal("recovery independently failed", err)
	}
	restarted := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
	if restarted.ID != initial.ID || restarted.State.StartedAt == running.State.StartedAt || restarted.State.Pid == running.State.Pid {
		t.Fatal("same-container restart did not change process generation")
	}
	if name, _ := osdVolumeBacking(t, ctx, docker.Client, cluster); name != volume {
		t.Fatal("recovery mounted a different volume")
	}
	if !maps.Equal(registered, osdMemoryIdentities(t, ctx, cluster)) {
		t.Fatal("recovery changed MON registration")
	}
	if got := osdMemoryExec(t, ctx, osd, "sh", "-c", `cat "$1/whoami" "$1/fsid" "$1/ceph_fsid"; stat -c '%i:%s' "$1/block"`, "identity", storePath); string(got) != string(store) {
		t.Fatal("recovery changed store identity or block inode/size")
	}
	q, err := cluster.QuorumStatus(ctx)
	if err != nil || q.MonMap.FSID != quorum.MonMap.FSID {
		t.Fatal("recovery changed cluster FSID", err)
	}
	var recovered map[string]abruptObjectState
	readback := abruptPhase(t, ctx, cluster, client, "read", pool, ledger, &recovered)
	for _, name := range []string{"control", "acknowledged"} {
		if recovered[name] != baseline[name] {
			t.Fatalf("acknowledged object %s changed: %+v before %+v", name, recovered[name], baseline[name])
		}
	}
	for _, event := range events {
		if event.Event == "completed" && event.Ret == 0 {
			for _, submission := range events {
				if submission.Event == "intent" && submission.Object == event.Object && (recovered[event.Object].SHA256 != submission.SHA256 || recovered[event.Object].Size != submission.Bytes) {
					t.Fatal("eventual successful completion lost its submitted bytes")
				}
			}
		}
	}
	t.Logf("OSD_ABRUPT_READBACK unconfirmed=unknown no_replay=true objects=%s", readback)
	if err := noout.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Terminate(ctx); err != nil {
		t.Fatal("cleanup independently failed", err)
	}
	terminated = true
	if _, err := docker.ContainerInspect(ctx, osd.GetContainerID(), mobycl.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Error("owned OSD remains", err)
	}
	if _, err := docker.VolumeInspect(ctx, volume, mobycl.VolumeInspectOptions{}); !errdefs.IsNotFound(err) {
		t.Error("owned store remains", err)
	}
	t.Log("OSD_ABRUPT_CLEANUP remaining_osds=0 remaining_volumes=0 partial_add=canceled restart=canceled_owned wait=canceled")
}
