//go:build all || (integration && (!ci || (ci_short && (!ci_batch || ci_batch_cephfs_fixtures_data))))

//ci: timeout=60m job-timeout=70 image-roles=essential,client,cephfs

package cephfs_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs/snapschedule"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/override"
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
		lease, err := snapschedule.Add(ctx, fs, snapschedule.Config{Path: "/", Repeat: "1h", Start: start})
		if err != nil {
			t.Fatal(err)
		}
		before := stamp()
		if _, err := command("fs", "snap-schedule", "remove", "/", "1h"); err != nil {
			t.Fatal(err)
		}
		if _, err := command("fs", "snap-schedule", "add", "/", "1h", start.Format("2006-01-02T15:04:05")); err != nil {
			t.Fatal(err)
		}
		after := stamp()
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
		}
		// Release the lease after raw removal, so this qualification never
		// asks Restore to delete an indistinguishable replacement.
		if _, err := command("fs", "snap-schedule", "remove", "/", "1h"); err != nil {
			t.Fatal(err)
		}
		if err := lease.Restore(ctx); err != nil {
			t.Fatal(err)
		}
		if collided {
			break
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
