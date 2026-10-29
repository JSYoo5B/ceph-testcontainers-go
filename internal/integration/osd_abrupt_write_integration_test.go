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
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
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
	_, opts := integrationImages(t)
	opts = opts.WithCustomizers(ceph.WithNoInitialOSDs(), ceph.WithOSDVolumeStorage())
	cluster, err := ceph.Run(ctx, opts)
	terminated := false
	if cluster != nil {
		t.Cleanup(func() {
			if terminated {
				return
			}
			if t.Failed() {
				for _, osd := range clusteraccess.OSDs(t, cluster) {
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
	if osd, err := cluster.AddOSD(canceled); !errors.Is(err, context.Canceled) || osd != nil || len(clusteraccess.OSDs(t, cluster)) != 0 {
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
	client, err := cluster.ClientContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	volume, _ := osdVolumeBacking(t, ctx, docker.Client, cluster)
	registered := osdMemoryIdentities(t, ctx, cluster)
	quorum, err := observeCluster(cluster).QuorumStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	storePath := "/var/lib/ceph/osd/ceph-" + strconv.Itoa(osd.ID)
	store := osdMemoryExec(t, ctx, osd, "sh", "-c", `cat "$1/whoami" "$1/fsid" "$1/ceph_fsid"; stat -c '%i:%s' "$1/block"`, "identity", storePath)
	initial := osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID())
	clientRaw := osdMemoryInspect(t, ctx, docker.Client, client.GetContainerID())
	platform := osdMemoryExec(t, ctx, client, "uname", "-sm")
	t.Logf("OSD_ABRUPT_RUNTIME ceph=%s platform=%s essential_image=%s client_image=%s cid=%s fsid=%s osd=%d uuid=%s volume=%s store=%q", staticFacts(cluster).CephVersion, platform, initial.Image, clientRaw.Image, osd.GetContainerID(), quorum.MonMap.FSID, osd.ID, registered[osd.ID], volume, store)
	noout, err := override.SetOSDFlag(ctx, cluster, ceph.OSDFlagNoOut, true)
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

	poolStatus, err := poolHandle(t, ctx, cluster, pool).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pg := fmt.Sprintf("%d.0", poolStatus.ID)
	restoreDelay := abruptDispatchDelay(t, ctx, cluster, fmt.Sprintf("osd.%d", osd.ID))
	// Each writer is a Rados client of its own with exactly one request, so that
	// the OSD's request, which names its client, is bound to its object by the
	// ledger. The second writer starts after the first request is witnessed.
	writers := map[string]*nativeCall{}
	finished := map[string]bool{}
	t.Cleanup(func() {
		for object, writer := range writers {
			if finished[object] {
				continue
			}
			writer.cancel()
			select {
			case outcome := <-writer.done:
				t.Logf("OSD_ABRUPT_ORIGINAL object=%s cleanup_exit=%d error=%v", object, outcome.result.ExitCode, outcome.err)
			case <-time.After(30 * time.Second):
				t.Error("original writer cleanup did not confirm stop", object)
			}
		}
	})
	daemon := fmt.Sprintf("osd.%d", osd.ID)
	var witnesses []abruptWitness
	for _, object := range []string{"fresh", "replacement"} {
		writers[object] = startNative(ctx, cluster, client, abruptCommand("actor-"+object, pool, ledger))
		for deadline := time.Now().Add(time.Minute); ; {
			var writer string
			returned := false
			for _, event := range abruptLedger(t, ctx, cluster, client, pool, ledger) {
				switch {
				case event.Event == "intent" && event.Object == object:
					writer = event.Client
				case event.Event == "api_return" && event.Object == object:
					returned = true
				case event.Event == "completed" || event.Event == "callback":
					t.Fatalf("original request finished before disruption: %+v", event)
				}
			}
			if returned {
				witness, ok := abruptWitnessOf(t, ctx, cluster, docker.Client, osd, daemon, pg, object, writer)
				if ok {
					witnesses = append(witnesses, witness)
					break
				}
				// The first request is required to be visible. The second can
				// stay queued behind the first one in the same dispatch.
				if object == "replacement" {
					t.Log("OSD_ABRUPT_WITNESS second request not visible in the OSD, outstanding in its writer only")
					break
				}
			}
			select {
			case outcome := <-writers[object].done:
				finished[object] = true
				t.Fatalf("writer %s ended before the required phase: %+v", object, outcome)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatalf("required OSD in-flight phase of %s not observed; last dump %q", object, abruptLastDump)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// A request that completed while its witness was read is no witness.
	if slices.ContainsFunc(abruptLedger(t, ctx, cluster, client, pool, ledger), func(e abruptWriteEvent) bool { return e.Event == "completed" || e.Event == "callback" }) {
		t.Fatal("original request finished while its witness was read")
	}
	encodedWitnesses, _ := json.Marshal(witnesses)
	t.Logf("OSD_ABRUPT_WITNESS pg=%s witnesses=%s", pg, encodedWitnesses)
	bootedBefore := osdUpFrom(t, ctx, cluster, osd.ID)
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
	// Stop the original clients while the OSD is down. They must not reconnect
	// and resubmit a librados pending request when the OSD comes back.
	for object, writer := range writers {
		writer.cancel()
		select {
		case outcome := <-writer.done:
			finished[object] = true
			t.Logf("OSD_ABRUPT_ORIGINAL object=%s exit=%d error=%v stdout=%s stderr=%s", object, outcome.result.ExitCode, outcome.err, outcome.result.Stdout, outcome.result.Stderr)
			var unstopped *ceph.NativeCommandUnstoppedError
			if !errors.Is(outcome.err, context.Canceled) || errors.As(outcome.err, &unstopped) {
				t.Fatal("original writer was not confirmed stopped", object, outcome.err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("original writer stop unconfirmed; refuse restart", object)
		}
	}
	events := abruptLedger(t, ctx, cluster, client, pool, ledger)
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("OSD_ABRUPT_LEDGER before_kill_ns=%d after_exit_ns=%d channel=%s events=%s", beforeKill, afterExit, abruptChannel(events), encoded)
	if err := abruptOverlap(events, beforeKill, afterExit); err != nil {
		t.Fatal("required abrupt window not established", err)
	}
	restoreDelay()
	// With one OSD no peer reports it failed, so the MONs keep it "up" in the
	// map until their report timeout. The dead process is the observation here,
	// and the restart is told apart by a newer boot epoch below.
	if status, err := observeCluster(cluster).Status(ctx); err == nil {
		t.Logf("OSD_ABRUPT_RECOVERY phase=after-kill status=%+v", status)
	} else {
		t.Fatal(err)
	}
	if err := cluster.WaitForClean(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled recovery wait", err)
	}
	if err := osd.Start(canceled); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled restart", err)
	}
	if len(clusteraccess.OSDs(t, cluster)) != 1 || clusteraccess.OSDs(t, cluster)[0] != osd || osdMemoryInspect(t, ctx, docker.Client, osd.GetContainerID()).State.Running {
		t.Fatal("canceled restart lost ownership or started daemon")
	}
	if err := osd.Start(ctx); err != nil {
		t.Fatal("recovery restart independently failed", err)
	}
	for deadline := time.Now().Add(2 * time.Minute); osdUpFrom(t, ctx, cluster, osd.ID) <= bootedBefore; {
		if time.Now().After(deadline) {
			t.Fatalf("restarted OSD did not boot into a newer epoch than %d", bootedBefore)
		}
		time.Sleep(time.Second)
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
	q, err := observeCluster(cluster).QuorumStatus(ctx)
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
	if err := abruptVerifyOutcomes(events, recovered); err != nil {
		t.Fatal(err)
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
