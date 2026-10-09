//go:build all || (integration && multicluster)

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/rbd"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

func testMultiClusterRBDSnapshotMirror(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t, opts...)
	const pool = "tc-rbd-mirror"
	const image = pool + "/replicated"
	const imageSize = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	mirrorImage := source.ControlImage()
	t.Logf("native RBD multicluster runtime image=%s", mirrorImage)
	mirror, err := rbd.RunMirror(ctx, mirrorImage, rbd.MirrorConfig{
		Source: source, Destination: destination, Pool: pool,
	})
	if mirror != nil {
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cleanupCancel()
			if t.Failed() && mirror.Container != nil {
				multiClusterLogContainer(t, cleanupCtx, mirror.Container)
			}
			if err := mirror.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate RBD multicluster runtime: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	before := rbdMultiClusterPayload(imageSize, 23)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-mirror-original", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-mirror-original", image,
		"--object-size", "1M", "--image-feature", "layering,exclusive-lock", "--no-progress")
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "enable", image, "snapshot")
	rbdMirrorReplayReady(t, ctx, mirror, "replicated", rbd.MirrorModeSnapshot, "", "")
	rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, before)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 83)
	copy(after[4<<20:], patch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, imageSize, 4<<20, patch)
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
	rbdMirrorReplayReady(t, ctx, mirror, "replicated", rbd.MirrorModeSnapshot, "", "")
	rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, after)
	t.Log("RBD native snapshot mirroring: initial and changed checkpoints reached an independent non-primary destination with exact 8 MiB bytes")

	// Orderly failover propagates demotion before promotion. Avoid --force,
	// which would exercise a separate split-brain recovery policy.
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "demote", image)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, false)
	rbdMultiClusterPromote(t, ctx, destinationClient, image)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, true)
	stopGrace := 2 * time.Second
	if err := mirror.Stop(ctx, &stopGrace); err != nil {
		t.Fatal(err)
	}
	stopMultiClusterSource(t, ctx, source)
	verifyRBDBytes(t, ctx, destinationClient, image, after)
	writable := bytes.Clone(after)
	copy(writable[:256<<10], rbdMultiClusterPayload(256<<10, 109))
	rbdMultiClusterWriteRange(t, ctx, destinationClient, image, imageSize, 0, writable[:256<<10])
	verifyRBDBytes(t, ctx, destinationClient, image, writable)
	if err := destination.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("RBD native snapshot mirroring: clean demote/promote, source MON/OSD outage, and independent destination read/write verified")
}

// Receiver readiness and native bytes/checkpoints are separate assertions.
func rbdMirrorReplayReady(t *testing.T, ctx context.Context, link *rbd.Mirror, name string, mode rbd.MirrorMode, sourceNamespace, destinationNamespace string) rbd.MirrorImageStatus {
	t.Helper()
	status, err := link.WaitReplayReady(ctx, name)
	if err != nil || !status.ReplayReady || status.State != "up+replaying" || status.Mode != mode || !status.SourcePrimary || status.DestinationPrimary || status.SourceNamespace != sourceNamespace || status.DestinationNamespace != destinationNamespace || status.SourceImageID == "" || status.DestinationImageID == "" || status.GlobalID == "" || status.DaemonName == "" || status.InstanceID == "" {
		t.Fatalf("public RBD replay readiness differs: %+v error=%v", status, err)
	}
	t.Logf("public RBD replay-ready mode=%s source=%q destination=%q image=%s daemon=%s instance=%s", status.Mode, sourceNamespace, destinationNamespace, name, status.DaemonName, status.InstanceID)
	return status
}

func rbdMultiClusterPool(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool string) {
	t.Helper()
	cephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "8")
	cephCommand(t, ctx, cluster, "osd", "pool", "set", pool, "pg_autoscale_mode", "off")
	execCommand(t, ctx, client, "rbd", "pool", "init", pool)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
}

func rbdMultiClusterPayload(size, seed int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*37+seed)%251 + 1)
	}
	return data
}

// This is a write fixture, not the backup encoder. Actual backup files above
// come from rbd export/export-diff. The public diff-v1 format permits small,
// deterministic ranged writes without host cgo or a mapped block device.
func rbdMultiClusterWriteRange(t *testing.T, ctx context.Context, client testcontainers.Container, image string, size, offset uint64, data []byte) {
	t.Helper()
	if offset > size || uint64(len(data)) > size-offset {
		t.Fatal("RBD fixture write exceeds image bounds")
	}
	diff := []byte("rbd diff v1\n")
	diff = append(diff, 's')
	diff = binary.LittleEndian.AppendUint64(diff, size)
	diff = append(diff, 'w')
	diff = binary.LittleEndian.AppendUint64(diff, offset)
	diff = binary.LittleEndian.AppendUint64(diff, uint64(len(data)))
	diff = append(diff, data...)
	diff = append(diff, 'e')
	if err := client.CopyToContainer(ctx, diff, "/tmp/rbd-fixture-write.diff", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, client, "rbd", "import-diff", "/tmp/rbd-fixture-write.diff", image, "--no-progress")
}

func rbdMultiClusterAssertPrimary(t *testing.T, ctx context.Context, client testcontainers.Container, image string, expected bool) {
	t.Helper()
	var info map[string]json.RawMessage
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
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

func rbdMultiClusterWaitMirror(t *testing.T, parent context.Context, client testcontainers.Container, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		_, _, _ = rbdMultiClusterExec(attempt, client, "rm", "-f", "/tmp/rbd-mirror-result")
		code, out, err := rbdMultiClusterExec(attempt, client, "rbd", "export", image, "/tmp/rbd-mirror-result", "--no-progress")
		if err == nil && code == 0 {
			reader, readErr := client.CopyFileFromContainer(attempt, "/tmp/rbd-mirror-result")
			if readErr == nil {
				actual, readErr := io.ReadAll(reader)
				reader.Close()
				if readErr == nil && bytes.Equal(actual, expected) {
					status := rbdOutput(t, attempt, client, "mirror", "image", "status", image, "--format", "json")
					t.Logf("destination mirror reached expected sha256=%x; status=%s", sha256.Sum256(expected), status)
					stop()
					return
				}
				last = fmt.Sprintf("destination data not yet replicated: read error=%v bytes=%d sha256=%x", readErr, len(actual), sha256.Sum256(actual))
			} else {
				last = readErr.Error()
			}
		} else {
			last = fmt.Sprintf("export exit=%d error=%v output=%s", code, err, out)
		}
		stop()
		select {
		case <-ctx.Done():
			statusCtx, statusCancel := context.WithTimeout(parent, 10*time.Second)
			_, status, _ := rbdMultiClusterExec(statusCtx, client, "rbd", "mirror", "image", "status", image, "--format", "json")
			statusCancel()
			t.Fatalf("wait for %s mirror: %v; %s; status=%s", image, ctx.Err(), last, status)
		case <-ticker.C:
		}
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
		code, out, err := rbdMultiClusterExec(attempt, client, "rbd", "mirror", "image", "promote", image)
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

func rbdMultiClusterExec(ctx context.Context, ctr testcontainers.Container, args ...string) (int, []byte, error) {
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		return code, nil, err
	}
	data, err := io.ReadAll(reader)
	return code, data, err
}
