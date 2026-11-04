//go:build all || (integration && features)

package integration_test

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

const healthWatchPool = "tc-health-watch"

// awaitHealthWatchEvent reads events until match accepts one, and fails on a
// closed watch, a sequence hole, another FSID or after limit. Gaps are
// counted, not fatal: a read can fail while an OSD stops. Only check codes
// are logged; messages can carry application data.
func awaitHealthWatchEvent(t *testing.T, w *ceph.HealthWatch, fsid string, last *uint64, gaps *int, limit time.Duration, what string, match func(ceph.HealthWatchEvent) bool) ceph.HealthWatchEvent {
	t.Helper()
	timeout := time.After(limit)
	for {
		select {
		case event, ok := <-w.Events():
			if !ok {
				t.Fatalf("%s: watch ended: %v", what, w.Stop())
			}
			if event.Sequence != *last+1 {
				t.Fatalf("%s: event %d follows %d", what, event.Sequence, *last)
			}
			*last = event.Sequence
			if event.Kind == ceph.WatchGap {
				*gaps++
				t.Logf("%s: gap at event %d after %v: %v", what, event.Sequence, event.Finished.Sub(event.Started), event.Err)
				continue
			}
			if event.Health.FSID != fsid {
				t.Fatalf("%s: event %d reports another FSID", what, event.Sequence)
			}
			if match(event) {
				return event
			}
			t.Logf("%s: event %d %s %s codes=%v mutes=%v", what, event.Sequence, event.Kind, event.Health.Status, healthWatchCodes(event), healthWatchMutes(event))
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

func healthWatchCodes(event ceph.HealthWatchEvent) []string {
	codes := make([]string, 0, len(event.Health.Checks))
	for code, check := range event.Health.Checks {
		if check.Muted {
			code += "(muted)"
		}
		codes = append(codes, code)
	}
	slices.Sort(codes)
	return codes
}

func healthWatchMutes(event ceph.HealthWatchEvent) []string {
	codes := make([]string, len(event.Health.Mutes))
	for i, mute := range event.Health.Mutes {
		codes[i] = mute.Code
	}
	return codes
}

func healthWatchMuted(event ceph.HealthWatchEvent, code string) bool {
	return slices.ContainsFunc(event.Health.Mutes, func(m ceph.HealthMute) bool { return m.Code == code })
}

// testHealthWatch observes OSD_DOWN appear when an owned OSD stops, become
// muted after the test mutes it, and clear after the OSD restarts, while the
// client keeps writing through a replicated pool. The events prove only what
// the monitor reported for the original FSID; the client's round trip, the
// OSD state and WaitForClean are the independent outcomes.
func testHealthWatch(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3))}, opts...)
	cluster, client := harness.NewServiceCluster(t, opts...)
	if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: healthWatchPool, Application: "rados", Replicas: 2, MinSize: 1, PGNum: 8}); err != nil {
		t.Fatal(err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	quorum, err := observer.QuorumStatus(ctx)
	if err != nil || quorum.MonMap.FSID == "" {
		t.Fatal("original cluster FSID unavailable", err)
	}
	fsid := quorum.MonMap.FSID
	// The cost of one read, the same three CLI calls the watch makes.
	const samples = 5
	began := time.Now()
	for range samples {
		if _, err := observer.HealthDetails(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d HealthDetails reads took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	const interval = 2 * time.Second
	watchStarted := time.Now()
	w, err := observer.WatchHealth(ctx, ceph.WatchConfig{Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	first := <-w.Events()
	last, gaps := first.Sequence, 0
	if first.Kind != ceph.WatchBaseline || first.Sequence != 1 || first.Health.FSID != fsid {
		t.Fatalf("baseline = %s %+v", first.Kind, healthWatchCodes(first))
	}
	if _, down := first.Health.Checks["OSD_DOWN"]; down || healthWatchMuted(first, "OSD_DOWN") {
		t.Fatalf("baseline already reports OSD_DOWN: %v %v", healthWatchCodes(first), healthWatchMutes(first))
	}
	t.Logf("baseline: %s codes=%v, read %v", first.Health.Status, healthWatchCodes(first), first.Finished.Sub(first.Started))

	target := clusteraccess.OSDs(t, cluster)[0]
	grace := 10 * time.Second
	if err := target.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	down := awaitHealthWatchEvent(t, w, fsid, &last, &gaps, 3*time.Minute, "down", func(e ceph.HealthWatchEvent) bool {
		check, ok := e.Health.Checks["OSD_DOWN"]
		return ok && !check.Muted && check.Severity == "HEALTH_WARN" && check.Count == 1
	})
	if down.Health.Status == "HEALTH_OK" {
		t.Fatalf("an unmuted warning with status %s", down.Health.Status)
	}
	t.Logf("down: event %d %s, observed %v after the stop returned", down.Sequence, down.Kind, down.Finished.Sub(stopped))

	// The OSD_DOWN report advances the test: the client must still write
	// and read back through the replicated pool.
	payload := bytes.Repeat([]byte("written while OSD_DOWN is reported\n"), 128)
	if err := client.CopyToContainer(ctx, payload, "/tmp/health-watch", 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", healthWatchPool, "put", "health-watch", "/tmp/health-watch")
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", healthWatchPool, "stat", "health-watch")

	// The test, not the watch, mutes the check; the cleanup removes the
	// mute if the monitor has not already dropped it.
	harness.CephCommand(t, ctx, cluster, "health", "mute", "OSD_DOWN", "10m")
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Minute)
		defer done()
		_, _ = cluster.Ceph(cleanup, "health", "unmute", "OSD_DOWN")
	})
	muted := awaitHealthWatchEvent(t, w, fsid, &last, &gaps, time.Minute, "muted", func(e ceph.HealthWatchEvent) bool {
		check, ok := e.Health.Checks["OSD_DOWN"]
		return ok && check.Muted && healthWatchMuted(e, "OSD_DOWN")
	})
	t.Logf("muted: event %d %s %s codes=%v", muted.Sequence, muted.Kind, muted.Health.Status, healthWatchCodes(muted))

	if err := target.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := time.Now()
	cleared := awaitHealthWatchEvent(t, w, fsid, &last, &gaps, 3*time.Minute, "cleared", func(e ceph.HealthWatchEvent) bool {
		_, ok := e.Health.Checks["OSD_DOWN"]
		return !ok
	})
	t.Logf("cleared: event %d %s, observed %v after the start returned, mutes=%v", cleared.Sequence, cleared.Kind, cleared.Finished.Sub(restarted), healthWatchMutes(cleared))
	// A nonsticky mute ends with its check, at the next monitor tick.
	if healthWatchMuted(cleared, "OSD_DOWN") {
		awaitHealthWatchEvent(t, w, fsid, &last, &gaps, time.Minute, "unmuted", func(e ceph.HealthWatchEvent) bool {
			return !healthWatchMuted(e, "OSD_DOWN")
		})
	}

	// Independent outcomes: the OSDMap reports the OSD up, the fixture's own
	// clean wait passes and the object written during the fault is readable.
	states, err := observer.OSDStates(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range states {
		if s.ID == target.ID && (!s.Up || !s.In) {
			t.Fatalf("osd.%d after restart: %+v", target.ID, s)
		}
	}
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", healthWatchPool, "get", "health-watch", "/tmp/health-watch.read")
	r, err := client.CopyFileFromContainer(ctx, "/tmp/health-watch.read")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("object read back %d bytes, want %d: %v", len(got), len(payload), err)
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("Stop = %v", err)
	}
	for event := range w.Events() {
		if event.Sequence != last+1 {
			t.Fatalf("event %d follows %d", event.Sequence, last)
		}
		last = event.Sequence
	}
	elapsed := time.Since(watchStarted)
	t.Logf("native health watch: baseline -> osd.%d stopped -> OSD_DOWN reported -> round trip -> muted -> restarted -> cleared; %d events, %d gaps, watch ran %v at a %v interval (about %d reads)",
		target.ID, last, gaps, elapsed.Round(time.Second), interval, int(elapsed/interval)+1)
}
