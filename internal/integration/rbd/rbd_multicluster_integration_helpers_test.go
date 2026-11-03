//go:build all || (integration && multicluster)

package rbd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/testcontainers/testcontainers-go"
)

func runMultiClusterRBDSnapshotMirror(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := harness.NewMultiClusterPair(t, opts...)
	const pool = "tc-rbd-mirror"
	const image = pool + "/replicated"
	const imageSize = 8 << 20
	harness.RBDMultiClusterPool(t, ctx, source, sourceClient, pool)
	harness.RBDMultiClusterPool(t, ctx, destination, destinationClient, pool)
	mirrorImage := harness.StaticFacts(source).Images.RBDAux
	t.Logf("native RBD multicluster runtime image=%s", mirrorImage)
	mirror, err := rbdmirror.Run(ctx, harness.NativeMirrorImage(t, "rbd-aux", mirrorImage), rbdmirror.WithConfig(rbdmirror.Config{
		Source: source, Destination: destination, Pool: pool,
	}))
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() && mirror.Container != nil {
				harness.MultiClusterLogContainer(t, cleanupCtx, mirror.Container)
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate RBD multicluster runtime: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	before := harness.RBDMultiClusterPayload(imageSize, 23)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-mirror-original", 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-mirror-original", image,
		"--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "enable", image, "snapshot")
	harness.RBDMirrorReplayReady(t, ctx, mirror, "replicated", rbdmirror.ModeSnapshot, "", "")
	harness.RBDMultiClusterWaitMirror(t, ctx, destinationClient, image, before)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	after := bytes.Clone(before)
	patch := harness.RBDMultiClusterPayload(1<<20, 83)
	copy(after[4<<20:], patch)
	harness.RBDMultiClusterWriteRange(t, ctx, sourceClient, image, imageSize, 4<<20, patch)
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
	harness.RBDMirrorReplayReady(t, ctx, mirror, "replicated", rbdmirror.ModeSnapshot, "", "")
	harness.RBDMultiClusterWaitMirror(t, ctx, destinationClient, image, after)
	t.Log("RBD native snapshot mirroring: initial and changed checkpoints reached an independent non-primary destination with exact 8 MiB bytes")

	// Orderly failover propagates demotion before promotion. Avoid --force,
	// which would exercise a separate split-brain recovery policy.
	harness.ExecCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "demote", image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, false)
	rbdMultiClusterPromote(t, ctx, destinationClient, image)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)
	stopGrace := 2 * time.Second
	if err := mirror.Stop(ctx, &stopGrace); err != nil {
		t.Fatal(err)
	}
	harness.StopMultiClusterSource(t, ctx, source)
	verifyRBDBytes(t, ctx, destinationClient, image, after)
	writable := bytes.Clone(after)
	copy(writable[:256<<10], harness.RBDMultiClusterPayload(256<<10, 109))
	harness.RBDMultiClusterWriteRange(t, ctx, destinationClient, image, imageSize, 0, writable[:256<<10])
	verifyRBDBytes(t, ctx, destinationClient, image, writable)
	if err := harness.ObserveCluster(destination).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("RBD native snapshot mirroring: clean demote/promote, source MON/OSD outage, and independent destination read/write verified")
}

func rbdMultiClusterAssertPrimary(t *testing.T, ctx context.Context, client testcontainers.Container, image string, expected bool) {
	t.Helper()
	var info map[string]json.RawMessage
	if err := json.Unmarshal(harness.RBDOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	var mirroring map[string]any
	if err := json.Unmarshal(info["mirroring"], &mirroring); err != nil {
		t.Fatal(err)
	}
	globalID, _ := mirroring["global_id"].(string)
	if mirroring["mode"] != "snapshot" || mirroring["state"] != "enabled" || globalID == "" || mirroring["primary"] != expected {
		t.Fatalf("%s has unexpected mirroring info: %+v; want snapshot mirroring primary=%v", image, mirroring, expected)
	}
}

func rbdMultiClusterPromote(t *testing.T, parent context.Context, client testcontainers.Container, image string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		code, out, err := harness.RBDMultiClusterExec(attempt, client, "rbd", "mirror", "image", "promote", image)
		stop()
		if err == nil && code == 0 {
			return
		}
		last = fmt.Sprintf("exit=%d error=%v output=%s", code, err, out)
		select {
		case <-ctx.Done():
			t.Fatalf("wait for propagated demotion and promote %s: %v; %s", image, ctx.Err(), last)
		case <-ticker.C:
		}
	}
}
