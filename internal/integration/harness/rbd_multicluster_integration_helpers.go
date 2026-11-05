//go:build all || (integration && multicluster)

package harness

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

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	rbdmirrorexpect "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror/expect"
	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

// Receiver readiness and native bytes/checkpoints are separate assertions.
func RBDMirrorReplayReady(t *testing.T, ctx context.Context, link *rbdmirror.Mirror, name string, mode rbdmirror.Mode, sourceNamespace, destinationNamespace string) rbdmirror.ImageStatus {
	t.Helper()
	status, err := link.WaitReplayReady(ctx, name)
	var result rbdmirrorexpect.ImageEvaluation
	if err == nil {
		want := rbdmirror.ImageStatus{Name: name, Mode: mode, SourceNamespace: sourceNamespace, DestinationNamespace: destinationNamespace}
		result, err = rbdmirrorexpect.Image{Select: want, Replaying: true}.Evaluate(status)
	}
	if err != nil || !result.Matched {
		logRBDMirrorImageState(t, ctx, link, status.Pool, sourceNamespace, destinationNamespace, name)
		t.Fatalf("public RBD replay readiness differs: %v error=%v status=%+v", result, err, status)
	}
	t.Logf("public RBD replay-ready mode=%s source=%q destination=%q image=%s daemon=%s instance=%s", status.Mode, sourceNamespace, destinationNamespace, name, status.DaemonName, status.InstanceID)
	return status
}

func RBDMultiClusterPool(t *testing.T, ctx context.Context, cluster *ceph.Container, client testcontainers.Container, pool string) {
	t.Helper()
	CephCommand(t, ctx, cluster, "osd", "pool", "create", pool, "8")
	CephCommand(t, ctx, cluster, "osd", "pool", "set", pool, "pg_autoscale_mode", "off")
	ExecCommand(t, ctx, client, "rbd", "pool", "init", pool)
	if err := ObserveCluster(cluster).WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
}

func RBDMultiClusterPayload(size, seed int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*37+seed)%251 + 1)
	}
	return data
}

// This is a write fixture, not the backup encoder. Actual backup files above
// come from rbd export/export-diff. The public diff-v1 format permits small,
// deterministic ranged writes without host cgo or a mapped block device.
func RBDMultiClusterWriteRange(t *testing.T, ctx context.Context, client testcontainers.Container, image string, size, offset uint64, data []byte) {
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
	ExecCommand(t, ctx, client, "rbd", "import-diff", "/tmp/rbd-fixture-write.diff", image, "--no-progress")
}

func RBDMultiClusterWaitMirror(t *testing.T, parent context.Context, client testcontainers.Container, image string, expected []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 20*time.Second)
		_, _, _ = RBDMultiClusterExec(attempt, client, "rm", "-f", "/tmp/rbd-mirror-result")
		code, out, err := RBDMultiClusterExec(attempt, client, "rbd", "export", image, "/tmp/rbd-mirror-result", "--no-progress")
		if err == nil && code == 0 {
			reader, readErr := client.CopyFileFromContainer(attempt, "/tmp/rbd-mirror-result")
			if readErr == nil {
				actual, readErr := io.ReadAll(reader)
				_ = reader.Close()
				if readErr == nil && bytes.Equal(actual, expected) {
					status := RBDOutput(t, attempt, client, "mirror", "image", "status", image, "--format", "json")
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
			_, status, _ := RBDMultiClusterExec(statusCtx, client, "rbd", "mirror", "image", "status", image, "--format", "json")
			statusCancel()
			t.Fatalf("wait for %s mirror: %v; %s; status=%s", image, ctx.Err(), last, status)
		case <-ticker.C:
		}
	}
}

func RBDMultiClusterExec(ctx context.Context, ctr testcontainers.Container, args ...string) (int, []byte, error) {
	code, reader, err := ctr.Exec(ctx, args, tcexec.Multiplexed())
	if err != nil {
		return code, nil, err
	}
	data, err := io.ReadAll(reader)
	return code, data, err
}

// logRBDMirrorImageState records both sites' native view of an image that
// did not become replay-ready.
func logRBDMirrorImageState(t *testing.T, ctx context.Context, link *rbdmirror.Mirror, pool, sourceNamespace, destinationNamespace, name string) {
	t.Helper()
	for _, site := range []struct {
		name, spec string
		run        func(context.Context, ...string) ([]byte, error)
	}{
		{"source", RBDScopeImage(pool, sourceNamespace, name), link.SourceRBD},
		{"destination", RBDScopeImage(pool, destinationNamespace, name), link.DestinationRBD},
	} {
		for _, args := range [][]string{{"info", site.spec, "--format", "json"}, {"mirror", "image", "status", site.spec, "--format", "json"}} {
			inspect, stop := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			out, err := site.run(inspect, args...)
			stop()
			t.Logf("RBD %s %v: %s error=%v", site.name, args, out, err)
		}
	}
}

func RBDMultiClusterAssertPrimary(t *testing.T, ctx context.Context, client testcontainers.Container, image string, expected bool) {
	t.Helper()
	var info map[string]json.RawMessage
	if err := json.Unmarshal(RBDOutput(t, ctx, client, "info", image, "--format", "json"), &info); err != nil {
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

func RBDMultiClusterPromote(t *testing.T, parent context.Context, client testcontainers.Container, image string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	var last string
	for {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		code, out, err := RBDMultiClusterExec(attempt, client, "rbd", "mirror", "image", "promote", image)
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
