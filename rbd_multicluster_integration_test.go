//go:build integration && multicluster

package ceph_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestMultiClusterRBDBackup restores CLI-generated full and incremental archives
// into an independent cluster after the source MON/OSDs are stopped.
func TestMultiClusterRBDBackup(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 14*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-backup"
	const original = pool + "/original"
	const restored = pool + "/restored"
	const imageSize = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)

	before := rbdMultiClusterPayload(imageSize, 19)
	if err := sourceClient.CopyToContainer(ctx, before, "/tmp/rbd-backup-original", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, sourceClient, "rbd", "import", "/tmp/rbd-backup-original", original,
		"--object-size", "1M", "--image-feature", "layering", "--no-progress")
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", original+"@baseline")
	execCommand(t, ctx, sourceClient, "rbd", "image-meta", "set", original, "backup-fixture", "format-2")
	execCommand(t, ctx, sourceClient, "rbd", "export", original, "/tmp/rbd-full.backup", "--export-format", "2", "--no-progress")
	full := multiClusterReadFile(t, ctx, sourceClient, "/tmp/rbd-full.backup")

	// Change exactly one 1 MiB object. This local write fixture uses the public
	// RBD diff stream; Ceph's export-diff command produces the actual backup.
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 61)
	copy(after[2<<20:], patch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, original, imageSize, 2<<20, patch)
	verifyRBDBytes(t, ctx, sourceClient, original, after)
	verifyRBDBytes(t, ctx, sourceClient, original+"@baseline", before)
	execCommand(t, ctx, sourceClient, "rbd", "snap", "create", original+"@next")
	execCommand(t, ctx, sourceClient, "rbd", "export-diff", "--from-snap", "baseline", original+"@next", "/tmp/rbd-next.diff", "--no-progress")
	delta := multiClusterReadFile(t, ctx, sourceClient, "/tmp/rbd-next.diff")
	if len(full) < imageSize || len(delta) == 0 || len(delta) >= len(full)/2 {
		t.Fatalf("unexpected archive sizes: full=%d incremental=%d", len(full), len(delta))
	}
	for path, data := range map[string][]byte{"/tmp/rbd-full.backup": full, "/tmp/rbd-next.diff": delta} {
		if err := destinationClient.CopyToContainer(ctx, data, path, 0o600); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, multiClusterReadFile(t, ctx, destinationClient, path)) {
			t.Fatalf("archive changed while transferring %s", path)
		}
	}
	stopMultiClusterSource(t, ctx, source)

	// Incremental restore must reject an unrelated image without its baseline.
	execCommand(t, ctx, destinationClient, "rbd", "create", pool+"/missing-baseline", "--size", "8M", "--image-feature", "layering")
	code, out, err := rbdMultiClusterExec(ctx, destinationClient, "rbd", "import-diff", "/tmp/rbd-next.diff", pool+"/missing-baseline", "--no-progress")
	if err != nil || code == 0 || !bytes.Contains(out, []byte("baseline")) {
		t.Fatalf("missing baseline should reject incremental restore: exit=%d error=%v output=%s", code, err, out)
	}
	execCommand(t, ctx, destinationClient, "rbd", "rm", pool+"/missing-baseline", "--no-progress")
	execCommand(t, ctx, destinationClient, "rbd", "import", "/tmp/rbd-full.backup", restored, "--export-format", "2", "--no-progress")
	verifyRBDInfo(t, ctx, destinationClient, restored, imageSize)
	verifyRBDBytes(t, ctx, destinationClient, restored, before)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@baseline", before)
	if got := strings.TrimSpace(string(rbdOutput(t, ctx, destinationClient, "image-meta", "get", restored, "backup-fixture"))); got != "format-2" {
		t.Fatalf("full backup lost image metadata: %q", got)
	}
	execCommand(t, ctx, destinationClient, "rbd", "import-diff", "/tmp/rbd-next.diff", restored, "--no-progress")
	verifyRBDBytes(t, ctx, destinationClient, restored, after)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@baseline", before)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@next", after)

	// Destination-only writes must preserve the restored historical snapshots.
	writable := bytes.Clone(after)
	copy(writable[:256<<10], rbdMultiClusterPayload(256<<10, 97))
	rbdMultiClusterWriteRange(t, ctx, destinationClient, restored, imageSize, 0, writable[:256<<10])
	verifyRBDBytes(t, ctx, destinationClient, restored, writable)
	verifyRBDBytes(t, ctx, destinationClient, restored+"@next", after)
	if err := destination.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("RBD intercluster backup: full format-2=%d bytes sha256=%x; incremental=%d bytes sha256=%x; snapshots, metadata, chain rejection and source-independent writable restore verified", len(full), sha256.Sum256(full), len(delta), sha256.Sum256(delta))
}

// TestMultiClusterRBDSnapshotMirror uses a real rbd-mirror daemon and rx-only
// peer. Snapshot mirroring is asynchronous and needs explicit checkpoints.
func TestMultiClusterRBDSnapshotMirror(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 16*time.Minute)
	defer cancel()
	source, destination, sourceClient, destinationClient := newMultiClusterPair(t)
	const pool = "tc-rbd-mirror"
	const image = pool + "/replicated"
	const imageSize = 8 << 20
	rbdMultiClusterPool(t, ctx, source, sourceClient, pool)
	rbdMultiClusterPool(t, ctx, destination, destinationClient, pool)
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "pool", "enable", "--site-name", "source", pool, "image")
	execCommand(t, ctx, destinationClient, "rbd", "mirror", "pool", "enable", "--site-name", "destination", pool, "image")
	token := rbdOutput(t, ctx, sourceClient, "mirror", "pool", "peer", "bootstrap", "create", "--site-name", "source", pool)
	if len(bytes.TrimSpace(token)) == 0 {
		t.Fatal("source mirroring bootstrap token is empty")
	}
	if err := destinationClient.CopyToContainer(ctx, token, "/tmp/rbd-peer-token", 0o600); err != nil {
		t.Fatal(err)
	}
	execCommand(t, ctx, destinationClient, "rbd", "mirror", "pool", "peer", "bootstrap", "import", "--site-name", "destination", "--direction", "rx-only", pool, "/tmp/rbd-peer-token")
	keyring, err := destination.Ceph(ctx, "auth", "get-or-create", "client.rbd-mirror.tc", "mon", "profile rbd-mirror", "osd", "profile rbd")
	if err != nil {
		t.Fatal(err)
	}
	mirrorImage := os.Getenv("CEPH_TEST_MIRROR_IMAGE")
	if mirrorImage == "" {
		mirrorImage = ceph.DefaultImage
	}
	t.Logf("native rbd-mirror daemon image=%s (the five slim roles do not include this optional daemon)", mirrorImage)
	mirror, err := testcontainers.Run(ctx, mirrorImage, destination.WithClient(),
		network.WithNetworkName(nil, source.NetworkName()),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader: bytes.NewReader(keyring), ContainerFilePath: "/etc/ceph/rbd-mirror.keyring", FileMode: 0o600,
		}),
		testcontainers.WithEntrypoint("rbd-mirror"),
		testcontainers.WithCmd("-f", "--name", "client.rbd-mirror.tc", "--keyring", "/etc/ceph/rbd-mirror.keyring",
			"--admin-socket", "/tmp/rbd-mirror.asok", "--log-to-stderr=true", "--log-to-file=false"),
		testcontainers.WithWaitStrategy(wait.ForExec([]string{"test", "-S", "/tmp/rbd-mirror.asok"}).WithStartupTimeout(time.Minute)),
	)
	if mirror != nil {
		cleanupMultiClusterContainer(t, mirror)
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
	rbdMultiClusterWaitMirror(t, ctx, destinationClient, image, before)
	rbdMultiClusterAssertPrimary(t, ctx, sourceClient, image, true)
	rbdMultiClusterAssertPrimary(t, ctx, destinationClient, image, false)
	after := bytes.Clone(before)
	patch := rbdMultiClusterPayload(1<<20, 83)
	copy(after[4<<20:], patch)
	rbdMultiClusterWriteRange(t, ctx, sourceClient, image, imageSize, 4<<20, patch)
	execCommand(t, ctx, sourceClient, "rbd", "mirror", "image", "snapshot", image)
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
