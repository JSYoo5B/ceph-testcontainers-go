//go:build all || (integration && multicluster)

package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

func RunMultiClusterRBDJournalMirrorFailback(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := NewMultiClusterPair(t, opts...)
	const pool = "tc-rbd-journal"
	const name = "volume"
	const image = pool + "/" + name
	const size = 8 << 20
	RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	sourceStatus, err := ObserveCluster(source).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationStatus, err := ObserveCluster(destination).Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	forward := RBDJournalRunLink(t, ctx, source, destination, pool, "source", "destination")
	// Journal mode needs exclusive-lock. EnableImage then asks the native CLI
	// to add journaling explicitly to this selected image, leaving pool scope
	// "image" and unrelated volumes unchanged.
	ExecCommand(t, ctx, sourceClient, "rbd", "create", image, "--size", "8M", "--object-size", "1M", "--image-feature", "layering,exclusive-lock")
	before := RBDMultiClusterPayload(size, 37)
	RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 0, before)
	if err := forward.EnableImage(ctx, name); err != nil {
		t.Fatal(err)
	}
	initialReplay := RBDMirrorReplayReady(t, ctx, forward, name, rbdmirror.ModeJournal, "", "")
	RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, before)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, true)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, false)

	first := bytes.Clone(before)
	patch := RBDMultiClusterPayload(1<<20, 83)
	copy(first[3<<20:], patch)
	RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 3<<20, patch)
	RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, first)
	t.Log("journal replay propagated a native ranged write without a manual mirror snapshot")

	// Pause the receiver and prove its existing replica remains unchanged.
	// New source writes are replayed from the journal after the receiver starts.
	RBDScenarioStop(t, ctx, forward)
	paused, err := forward.ImageStatus(ctx, name)
	if err != nil || paused.ReplayReady || paused.GlobalID != initialReplay.GlobalID || paused.DestinationImageID != initialReplay.DestinationImageID {
		t.Fatalf("stopped receiver readiness/identity differs: %+v error=%v", paused, err)
	}
	pausedCtx, pausedCancel := context.WithTimeout(ctx, 3*time.Second)
	_, pausedErr := forward.WaitReplayReady(pausedCtx, name)
	pausedCancel()
	if !errors.Is(pausedErr, context.DeadlineExceeded) {
		t.Fatalf("stopped receiver wait ignored deadline: %v", pausedErr)
	}
	backlog := bytes.Clone(first)
	queued := RBDMultiClusterPayload(256<<10, 109)
	copy(backlog, queued)
	RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 0, queued)
	RBDJournalRead(t, ctx, destinationClient, destinationStatus.FSID, pool, name, first)
	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := RBDMirrorReplayReady(t, ctx, forward, name, rbdmirror.ModeJournal, "", "")
	if restarted.GlobalID != initialReplay.GlobalID || restarted.SourceImageID != initialReplay.SourceImageID || restarted.DestinationImageID != initialReplay.DestinationImageID || restarted.InstanceID == initialReplay.InstanceID {
		t.Fatal("receiver restart changed images or retained a stale process instance")
	}
	RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, backlog)

	reverse := RBDJournalRunLink(t, ctx, destination, source, pool, "destination", "source")
	RBDScenarioWaitTransmitPeer(t, ctx, forward.SourceRBD, pool, "destination")
	RBDScenarioWaitTransmitPeer(t, ctx, reverse.SourceRBD, pool, "source")
	// Writers are closed before demotion. Native promotion retries while the
	// journal receiver consumes the demotion marker, and never uses --force.
	RBDScenarioCommand(t, ctx, forward.SourceRBD, "mirror", "image", "demote", image)
	RBDMultiClusterPromote(t, ctx, destinationClient, image)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, false)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, true)
	RBDScenarioStop(t, ctx, forward)
	RBDMirrorReplayReady(t, ctx, reverse, name, rbdmirror.ModeJournal, "", "")
	onB := bytes.Clone(backlog)
	patchB := RBDMultiClusterPayload(1<<20, 137)
	copy(onB[5<<20:], patchB)
	RBDJournalWrite(t, ctx, destinationClient, destinationStatus.FSID, pool, name, 5<<20, patchB)
	RBDJournalWaitBytes(t, ctx, sourceClient, sourceStatus.FSID, pool, name, onB)

	RBDScenarioCommand(t, ctx, reverse.SourceRBD, "mirror", "image", "demote", image)
	RBDMultiClusterPromote(t, ctx, sourceClient, image)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, true)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, false)
	RBDJournalRead(t, ctx, sourceClient, sourceStatus.FSID, pool, name, onB)
	if err := forward.Start(ctx); err != nil {
		t.Fatal(err)
	}
	RBDMirrorReplayReady(t, ctx, forward, name, rbdmirror.ModeJournal, "", "")
	after := bytes.Clone(onB)
	patchA := RBDMultiClusterPayload(256<<10, 167)
	copy(after[2<<20:], patchA)
	RBDJournalWrite(t, ctx, sourceClient, sourceStatus.FSID, pool, name, 2<<20, patchA)
	RBDJournalWaitBytes(t, ctx, destinationClient, destinationStatus.FSID, pool, name, after)
	rbdJournalAssertPrimary(t, ctx, sourceClient, image, true)
	rbdJournalAssertPrimary(t, ctx, destinationClient, image, false)
	t.Logf("native RBD journal: full 8 MiB equality, receiver restart backlog, non-forced A -> B -> A, B-only writes retained, resumed A writes; final sha256=%x; no mirror snapshot commands", sha256.Sum256(after))
}

func RBDJournalRunLink(t *testing.T, ctx context.Context, source, destination *ceph.Container, pool, sourceSite, destinationSite string) *rbdmirror.Mirror {
	t.Helper()
	image := StaticFacts(source).Images.RBDAux
	link, err := rbdmirror.Run(ctx, NativeMirrorImage(t, "rbd-aux", image), rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: pool,
		SourceSite: sourceSite, DestinationSite: destinationSite, Mode: rbdmirror.ModeJournal,
	}))
	if link != nil {
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if t.Failed() && link.Container != nil {
				MultiClusterLogContainer(t, cleanupCtx, link.Container)
			}
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate journal RBD link: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return link
}

func rbdJournalAssertPrimary(t *testing.T, ctx context.Context, client testcontainers.Container, image string, primary bool) {
	t.Helper()
	var info struct {
		Features  []string `json:"features"`
		Mirroring struct {
			Mode, State string
			Primary     bool
			GlobalID    string `json:"global_id"`
		} `json:"mirroring"`
	}
	if err := json.Unmarshal(RBDOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if info.Mirroring.Mode != "journal" || info.Mirroring.State != "enabled" || info.Mirroring.GlobalID == "" || info.Mirroring.Primary != primary || !slices.Contains(info.Features, "journaling") || !slices.Contains(info.Features, "exclusive-lock") {
		t.Fatalf("unexpected journal image mode/features/primary=%t: %+v", primary, info)
	}
}

func RBDJournalWrite(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, image string, offset int, data []byte) {
	t.Helper()
	const path = "/tmp/rbd-journal-write"
	if err := client.CopyToContainer(ctx, data, path, 0o600); err != nil {
		t.Fatal(err)
	}
	code, output, err := RBDMultiClusterExec(ctx, client, "python3", "-c", RBDJournalIOScript, "write", fsid, pool, image, fmt.Sprint(offset), path)
	if err != nil || code != 0 {
		t.Fatalf("native journal image write failed: exit=%d error=%v output=%s", code, err, output)
	}
}

func RBDJournalRead(t *testing.T, ctx context.Context, client testcontainers.Container, fsid, pool, image string, expected []byte) {
	t.Helper()
	const path = "/tmp/rbd-journal-expected"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	code, output, err := RBDMultiClusterExec(ctx, client, "python3", "-c", RBDJournalIOScript, "read", fsid, pool, image, "0", path)
	if err != nil || code != 0 {
		t.Fatalf("fresh native journal image read differs: exit=%d error=%v output=%s", code, err, output)
	}
}

func RBDJournalWaitBytes(t *testing.T, parent context.Context, client testcontainers.Container, fsid, pool, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	const path = "/tmp/rbd-journal-expected"
	if err := client.CopyToContainer(ctx, expected, path, 0o600); err != nil {
		t.Fatal(err)
	}
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 35*time.Second)
		code, output, err := RBDMultiClusterExec(attempt, client, "python3", "-c", RBDJournalIOScript, "read", fsid, pool, image, "0", path)
		stop()
		if err == nil && code == 0 {
			t.Logf("fresh native journal replica verified full %d bytes: sha256=%x; evidence=%s", len(expected), sha256.Sum256(expected), output)
			return
		}
		last = fmt.Sprintf("exit=%d error=%v output=%s", code, err, output)
		select {
		case <-ctx.Done():
			statusCtx, statusCancel := context.WithTimeout(parent, 10*time.Second)
			_, status, _ := RBDMultiClusterExec(statusCtx, client, "rbd", "mirror", "image", "status", pool+"/"+image, "--format", "json")
			statusCancel()
			t.Fatalf("journal replica never reached expected bytes: %v; %s; status=%s", ctx.Err(), last, status)
		case <-time.After(2 * time.Second):
		}
	}
}

// Each invocation creates a new librados/librbd session and disables its image
// cache. The expected file is a fixture comparator; only mode=write modifies an
// RBD image. Replication is exclusively performed by the rbd-mirror daemon.
const RBDJournalIOScript = `import hashlib, json, os, rados, rbd, sys, threading
deadline = threading.Timer(30, lambda: os._exit(124))
deadline.daemon = True
deadline.start()
mode, fsid, pool, name, offset, path = sys.argv[1:]
with open(path, "rb") as fixture:
    expected = fixture.read()
try:
    with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={
        "keyring": "/etc/ceph/ceph.client.admin.keyring",
        "rados_mon_op_timeout": "15", "rados_osd_op_timeout": "15", "rbd_cache": "false",
    }) as cluster:
        assert cluster.get_fsid() == fsid, "native client reached another cluster"
        with cluster.open_ioctx(pool) as ioctx:
            with rbd.Image(ioctx, name, read_only=(mode == "read")) as image:
                if mode == "write":
                    written = image.write(expected, int(offset))
                    assert written == len(expected), "short native write"
                    image.flush()
                    actual = image.read(int(offset), len(expected))
                elif mode == "read":
                    assert image.size() == len(expected), "replica size mismatch"
                    actual = image.read(0, len(expected))
                else:
                    raise ValueError("unexpected fixture mode")
                assert actual == expected, "native image bytes differ: sha256=" + hashlib.sha256(actual).hexdigest()
                print(json.dumps({"fsid": cluster.get_fsid(), "bytes": len(actual), "sha256": hashlib.sha256(actual).hexdigest(), "mode": mode}))
finally:
    deadline.cancel()
`
