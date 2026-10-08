//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// TestRBDLifecycle verifies userspace RBD I/O via the image's CLI. It does not
// map a kernel block device or mount a filesystem on an RBD image.
func TestRBDLifecycle(t *testing.T) {
	testRBDLifecycle(t)
}

func testRBDLifecycle(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	cluster, client := newServiceCluster(t, opts...)
	const pool = "tc-rbd"
	cephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "8")
	cephCommand(t, ctx, cluster, "osd", "pool", "set", pool, "pg_autoscale_mode", "off")
	execCommand(t, ctx, client, "rbd", "pool", "init", pool)
	if err := cluster.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	nativeStatus, err := cluster.Status(ctx)
	if err != nil || nativeStatus.FSID == "" {
		t.Fatalf("read original pool usage cluster identity: status=%+v error=%v", nativeStatus, err)
	}

	// Explicit create/info/delete checks are separate from import, which creates
	// its own destination image.
	const imageSize = 8 << 20
	execCommand(t, ctx, client, "rbd", "create", pool+"/empty", "--size", "8M", "--image-feature", "layering")
	verifyRBDInfo(t, ctx, client, pool+"/empty", imageSize)
	execCommand(t, ctx, client, "rbd", "rm", pool+"/empty", "--no-progress")
	verifyRBDImages(t, ctx, client, pool, nil)

	// Cover multiple nonzero RADOS objects rather than relying on a sparse image
	// whose implicit zeroes would still read correctly without any stored data.
	before := make([]byte, imageSize)
	after := make([]byte, imageSize)
	for i := range before {
		before[i] = byte(i%251 + 1)
		after[i] = byte((i*37+17)%251 + 1)
	}
	for path, payload := range map[string][]byte{
		"/tmp/rbd-before": before,
		"/tmp/rbd-after":  after,
	} {
		if err := client.CopyToContainer(ctx, payload, path, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	execCommand(t, ctx, client, "rbd", "import", "/tmp/rbd-before", pool+"/original", "--object-size", "1M", "--image-feature", "layering", "--no-progress")
	verifyRBDInfo(t, ctx, client, pool+"/original", imageSize)
	verifyRBDBytes(t, ctx, client, pool+"/original", before)
	importUsage := verifyRBDPoolUsage(t, ctx, cluster, ceph.PoolUsageSnapshot{FSID: nativeStatus.FSID, Name: pool}, imageSize, "after_import")
	execCommand(t, ctx, client, "rbd", "snap", "create", pool+"/original@baseline")
	execCommand(t, ctx, client, "rbd", "snap", "protect", pool+"/original@baseline")
	execCommand(t, ctx, client, "rbd", "clone", pool+"/original@baseline", pool+"/clone", "--image-feature", "layering")
	verifyRBDInfo(t, ctx, client, pool+"/clone", imageSize)

	// Exporting a complete diff from an independently imported image provides a
	// deterministic overwrite without requiring /dev/rbd, NBD, or cgo on Go's
	// host. The snapshot and dependent clone must keep the original bytes.
	execCommand(t, ctx, client, "rbd", "import", "/tmp/rbd-after", pool+"/replacement", "--object-size", "1M", "--image-feature", "layering", "--no-progress")
	execCommand(t, ctx, client, "rbd", "export-diff", pool+"/replacement", "/tmp/rbd-replacement.diff", "--no-progress")
	execCommand(t, ctx, client, "rbd", "import-diff", "/tmp/rbd-replacement.diff", pool+"/original", "--no-progress")
	execCommand(t, ctx, client, "rbd", "rm", pool+"/replacement", "--no-progress")
	verifyRBDBytes(t, ctx, client, pool+"/original", after)
	verifyRBDBytes(t, ctx, client, pool+"/original@baseline", before)
	verifyRBDBytes(t, ctx, client, pool+"/clone", before)
	t.Log("RBD: image create/info/delete, 8 MiB import/export, snapshot/clone isolation verified")

	advanceServiceTopology(t, ctx, cluster)
	verifyRBDBytes(t, ctx, client, pool+"/original", after)
	verifyRBDBytes(t, ctx, client, pool+"/original@baseline", before)
	verifyRBDBytes(t, ctx, client, pool+"/clone", before)
	t.Log("RBD: parent, snapshot, and clone bytes survived OSD 2 -> 3 -> 2")
	verifyRBDPoolUsage(t, ctx, cluster, importUsage, imageSize, "after_topology")

	// Flatten severs the clone's parent dependency. Its bytes must remain intact
	// after the protected parent snapshot is removed.
	execCommand(t, ctx, client, "rbd", "flatten", pool+"/clone", "--no-progress")
	execCommand(t, ctx, client, "rbd", "snap", "unprotect", pool+"/original@baseline")
	execCommand(t, ctx, client, "rbd", "snap", "rm", pool+"/original@baseline", "--no-progress")
	verifyRBDBytes(t, ctx, client, pool+"/clone", before)
	execCommand(t, ctx, client, "rbd", "rm", pool+"/clone", "--no-progress")
	execCommand(t, ctx, client, "rbd", "rm", pool+"/original", "--no-progress")
	verifyRBDImages(t, ctx, client, pool, nil)
	t.Log("RBD: flatten, snapshot removal, and complete image cleanup verified")
}

func verifyRBDInfo(t *testing.T, ctx context.Context, client testcontainers.Container, image string, size uint64) {
	t.Helper()
	var info struct {
		Size     uint64   `json:"size"`
		Format   int      `json:"format"`
		Features []string `json:"features"`
	}
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
		t.Fatal(err)
	}
	if info.Size != size || info.Format != 2 {
		t.Fatalf("%s info: size=%d format=%d; want size=%d format=2", image, info.Size, info.Format, size)
	}
	if !strings.Contains(strings.Join(info.Features, ","), "layering") {
		t.Fatalf("%s lacks layering feature: %v", image, info.Features)
	}
}

func verifyRBDBytes(t *testing.T, ctx context.Context, client testcontainers.Container, image string, expected []byte) {
	t.Helper()
	const path = "/tmp/rbd-result"
	// The export command refuses to overwrite an existing local destination.
	execCommand(t, ctx, client, "rm", "-f", path)
	execCommand(t, ctx, client, "rbd", "export", image, path, "--no-progress")
	r, err := client.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(r)
	r.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("%s payload changed: got %d bytes, want %d bytes", image, len(actual), len(expected))
	}
}

func verifyRBDImages(t *testing.T, ctx context.Context, client testcontainers.Container, pool string, expected []string) {
	t.Helper()
	var images []string
	if err := json.Unmarshal(rbdOutput(t, ctx, client, "ls", pool, "--format", "json"), &images); err != nil {
		t.Fatal(err)
	}
	if len(images) != len(expected) {
		t.Fatalf("unexpected RBD images: got %v, want %v", images, expected)
	}
	for i := range images {
		if images[i] != expected[i] {
			t.Fatalf("unexpected RBD images: got %v, want %v", images, expected)
		}
	}
}

func rbdOutput(t *testing.T, ctx context.Context, client testcontainers.Container, args ...string) []byte {
	t.Helper()
	args = append([]string{"rbd"}, args...)
	code, r, err := client.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	if code != 0 {
		t.Fatalf("%s exited %d: %s", strings.Join(args, " "), code, out)
	}
	return out
}

// PG reports are asynchronous. Retry only a missing usage row or valid lagging
// counters; malformed/foreign observations and query errors remain failures.
func verifyRBDPoolUsage(t *testing.T, ctx context.Context, cluster *ceph.Container, expected ceph.PoolUsageSnapshot, minimumBytes uint64, phase string) ceph.PoolUsageSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last ceph.PoolUsageSnapshot
	var lastErr error
	for {
		if err := ctx.Err(); err != nil {
			t.Fatalf("%s pool usage did not converge: last=%+v observation_error=%v context=%v", phase, last, lastErr, err)
		}
		usage, err := cluster.PoolUsage(ctx, expected.Name)
		last, lastErr = usage, err
		if err != nil {
			// The API currently has no public absence sentinel. Match this single
			// exact condition; do not hide invalid schema, native identity drift,
			// transport failure or cancellation under a broad retry policy.
			if err.Error() != fmt.Sprintf("pool %q usage statistics are not reported", expected.Name) {
				t.Fatalf("%s pool usage observation failed: %v", phase, err)
			}
		} else {
			if usage.Name != expected.Name || usage.ID <= 0 || usage.FSID != expected.FSID || expected.ID > 0 && usage.ID != expected.ID {
				t.Fatalf("%s pool usage belongs to a different native identity: got=%+v expected=%+v", phase, usage, expected)
			}
			if usage.StoredBytes >= minimumBytes && usage.Objects >= 8 && usage.AllocatedBytes > 0 {
				if err := ctx.Err(); err != nil {
					t.Fatalf("%s pool usage context expired at observation: %v", phase, err)
				}
				t.Logf("POOL_USAGE_NATIVE phase=%s fsid=%s pool_id=%d pool=%s stored=%d objects=%d allocated=%d valid=true", phase, usage.FSID, usage.ID, usage.Name, usage.StoredBytes, usage.Objects, usage.AllocatedBytes)
				return usage
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s pool usage did not converge: last=%+v observation_error=%v context=%v", phase, last, lastErr, ctx.Err())
		case <-ticker.C:
		}
	}
}
