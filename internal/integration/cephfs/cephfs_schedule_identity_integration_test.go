//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
	"github.com/testcontainers/testcontainers-go"
)

// The timestamps are read on an isolated root schedule with a future start,
// so no snapshot data is created or deleted by this identity qualification.
func TestCephFSScheduleCreationTimestampResolution(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	image, opts := harness.IntegrationImages(t)
	cluster, _ := harness.NewServiceClusterRun(t, harness.CEPHFSRun, image, append(opts, ceph.WithOSDStorage(ceph.OSDStorage{}.WithBlockSize(1<<30).WithStartupBenchmark(ceph.OSDBenchmarkSkip)))...)
	fs := cephfs.Filesystems(cluster)[0]
	module, err := override.SetMGRModule(ctx, cluster, "snap_schedule", true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := module.Restore(ctx); err != nil {
			t.Error(err)
		}
	}()
	command := func(args ...string) ([]byte, error) {
		return cluster.Ceph(ctx, append(args, "--fs", fs.FilesystemName)...)
	}
	for {
		if _, err := command("fs", "snap-schedule", "list", "/", "--format", "json"); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	start := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	stamp := func() string {
		data, err := command("fs", "snap-schedule", "status", "/", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var rows []struct {
			Created string `json:"created"`
		}
		if err := json.Unmarshal(data, &rows); err != nil || len(rows) != 1 || rows[0].Created == "" {
			t.Fatalf("status %s, %v", data, err)
		}
		return rows[0].Created
	}
	collided := false
	for attempt := 0; attempt < 8; attempt++ {
		lease, before, after, err := replaceCephFSSchedule(ctx, cluster, clusteraccess.ControlContainer(t, cluster), fs, start)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("attempt %d creation timestamp before=%s after=%s", attempt, before, after)
		if before == after {
			collided = true
			if _, err := lease.Status(ctx); err != nil {
				t.Fatalf("identical native identity differed: %v", err)
			}
			w, err := lease.Watch(ctx, ceph.WatchConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Stop(); err != nil {
				t.Fatal(err)
			}
			// This is a disposable future schedule with no snapshots. With
			// identical native identity fields the lease also accepts Restore.
			if err := lease.Restore(ctx); err != nil {
				t.Fatalf("restore of identical native identity: %v", err)
			}
			entries, err := command("fs", "snap-schedule", "status", "/", "--format", "json")
			if err != nil {
				t.Fatal(err)
			}
			var schedules []json.RawMessage
			if err := json.Unmarshal(entries, &schedules); err != nil || len(schedules) != 0 {
				t.Fatalf("the indistinguishable future schedule remained: %s (%v)", entries, err)
			}
			break
		}
		if _, err := command("fs", "snap-schedule", "remove", "/", "1h"); err != nil {
			t.Fatal(err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if !collided {
		t.Log("no same-second collision observed within eight attempts")
	}
	lease, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1h", Start: start})
	if err != nil {
		t.Fatal(err)
	}
	before := stamp()
	select {
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	case <-time.After(1100 * time.Millisecond):
	}
	if _, err := command("fs", "snap-schedule", "remove", "/", "1h"); err != nil {
		t.Fatal(err)
	}
	if _, err := command("fs", "snap-schedule", "add", "/", "1h", start.Format("2006-01-02T15:04:05")); err != nil {
		t.Fatal(err)
	}
	after := stamp()
	if before == after {
		t.Fatalf("distinct-time control collided: %s", before)
	}
	if _, err := lease.Status(ctx); err == nil {
		t.Fatal("different creation timestamp accepted")
	}
	if w, err := lease.Watch(ctx, ceph.WatchConfig{}); err == nil {
		_ = w.Stop()
		t.Fatal("watch accepted a different creation timestamp")
	}
	if err := lease.Restore(ctx); err == nil {
		t.Fatal("different creation timestamp restored")
	}
	if stamp() != after {
		t.Fatal("refused Restore changed the replacement")
	}
	if _, err := command("fs", "snap-schedule", "remove", "/", "1h"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("distinct timestamp control before=%s after=%s; same-second observed=%t", before, after, collided)
}

// Warm the MGR connection before Add, then replace the schedule without
// starting a CLI for every command. The future schedule never fires.
const replaceScheduleProbe = `
import json
import rados
import sys

fs, start = sys.argv[1:]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={
    "keyring": "/etc/ceph/ceph.client.admin.keyring",
    "rados_mon_op_timeout": "10",
}) as cluster:
    def command(action, **params):
        rc, data, err = cluster.mgr_command(json.dumps({
            "prefix": "fs snap-schedule " + action,
            "fs": fs, "path": "/", **params,
        }), b"")
        if rc:
            raise RuntimeError(f"{action}: {rc} {err}")
        return data

    def status():
        return json.loads(command("status", format="json"))

    status()
    if sys.stdin.readline() != "replace\n":
        raise RuntimeError("missing replacement signal")
    before, = status()
    command("remove", repeat="1h", start=start)
    command("add", snap_schedule="1h", start=start)
    after, = status()
    print(json.dumps([before["created"], after["created"]]))
`

func replaceCephFSSchedule(ctx context.Context, cluster *ceph.Container, control testcontainers.Container, fs *cephfs.Filesystem, start time.Time) (*snapschedule.Lease, string, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	const signal = "replace\n"
	input := ceph.NewNativeInput()
	type outcome struct {
		result ceph.NativeCommandResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := cluster.RunNativeCommand(ctx, control, ceph.NativeCommand{
			Argv:    []string{"python3", "-c", replaceScheduleProbe, fs.FilesystemName, start.Format("2006-01-02T15:04:05")},
			Timeout: 25 * time.Second, MaxStdout: 8192, MaxStderr: 8192, Label: "schedule replacement probe",
			Input: input, MaxInput: len(signal),
		})
		done <- outcome{result, err}
	}()
	lease, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1h", Start: start})
	if err == nil {
		err = input.Write(ctx, []byte(signal))
	}
	err = errors.Join(err, input.Close())
	out := <-done
	if err := errors.Join(err, out.err); err != nil {
		return nil, "", "", err
	}
	result := out.result
	if result.ExitCode != 0 || !result.Complete() || !result.InputEOF || result.InputDelivered != int64(len(signal)) {
		return nil, "", "", fmt.Errorf("schedule replacement probe failed: exit=%d stdout=%q stderr=%q", result.ExitCode, result.Stdout, result.Stderr)
	}
	var timestamps []string
	if err := json.Unmarshal(result.Stdout, &timestamps); err != nil || len(timestamps) != 2 || timestamps[0] == "" || timestamps[1] == "" {
		return nil, "", "", fmt.Errorf("replacement status missing creation identity: %s (%v)", result.Stdout, err)
	}
	return lease, timestamps[0], timestamps[1], nil
}
