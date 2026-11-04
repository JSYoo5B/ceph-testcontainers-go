//go:build all || (integration && features)

package integration_test

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/ceph/expect"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clusteraccess"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/testcontainers/testcontainers-go"
)

const poolWatchPool = "tc-pg-watch"

// awaitPoolPGWatchEvent reads events until match accepts one, and fails on
// a closed watch, a sequence hole, an event of another pool or after limit.
// Gaps are counted, not fatal: a read can fail while an OSD stops.
func awaitPoolPGWatchEvent(t *testing.T, w *ceph.PoolPGWatch, poolID int64, last *uint64, gaps *int, limit time.Duration, what string, match func(ceph.PoolPGWatchEvent) bool) ceph.PoolPGWatchEvent {
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
			if event.Snapshot.PoolBefore.ID != poolID || event.Snapshot.PoolAfter.ID != poolID {
				t.Fatalf("%s: event %d reports pool %d/%d, want %d", what, event.Sequence, event.Snapshot.PoolBefore.ID, event.Snapshot.PoolAfter.ID, poolID)
			}
			if match(event) {
				return event
			}
			t.Logf("%s: event %d %s epochs %d..%d: %s", what, event.Sequence, event.Kind, event.Snapshot.OSDMapEpochBefore, event.Snapshot.OSDMapEpochAfter, poolPGWatchStates(event))
		case <-timeout:
			t.Fatalf("%s: no matching event within %v", what, limit)
		}
	}
}

func poolPGWatchStates(event ceph.PoolPGWatchEvent) string {
	states := make([]string, len(event.Snapshot.PGs))
	for i, pg := range event.Snapshot.PGs {
		states[i] = string(pg.PGID) + "=" + pg.State
	}
	return strings.Join(states, " ")
}

func poolPGWatchRow(event ceph.PoolPGWatchEvent, id ceph.PGID) (ceph.PGState, bool) {
	for _, pg := range event.Snapshot.PGs {
		if pg.PGID == id {
			return pg, true
		}
	}
	return ceph.PGState{}, false
}

func poolPGWatchAllClean(t *testing.T, event ceph.PoolPGWatchEvent, count int) bool {
	t.Helper()
	result, err := expect.PGs{Count: count, State: "active+clean"}.Evaluate(event.Snapshot)
	if err != nil {
		t.Fatalf("event %d: %v", event.Sequence, err)
	}
	return result.Matched
}

// poolPGWatchRoundTrip plays the SUT: it writes one object through the
// client container's rados CLI and reads it back, independently of the
// watch.
func poolPGWatchRoundTrip(t *testing.T, ctx context.Context, client testcontainers.Container, name string, payload []byte) {
	t.Helper()
	if err := client.CopyToContainer(ctx, payload, "/tmp/"+name, 0o600); err != nil {
		t.Fatal(err)
	}
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", poolWatchPool, "put", name, "/tmp/"+name)
	poolPGWatchReadBack(t, ctx, client, name, payload)
}

func poolPGWatchReadBack(t *testing.T, ctx context.Context, client testcontainers.Container, name string, payload []byte) {
	t.Helper()
	harness.ExecCommand(t, ctx, client, "timeout", "60", "rados", "-p", poolWatchPool, "get", name, "/tmp/"+name+".read")
	r, err := client.CopyFileFromContainer(ctx, "/tmp/"+name+".read")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	_ = r.Close()
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("object %s read back %d bytes, want %d: %v", name, len(got), len(payload), err)
	}
}

// testPoolPGWatch observes one pool's PGs go degraded when an OSD in their
// acting sets stops, and clean again after it restarts, while the client
// writes through the pool. The degraded event proves only that the manager
// reported those PGs of the same pool (same FSID and native ID) without the
// stopped OSD; the client's own round trips and the final WaitForClean are
// the independent outcomes.
func testPoolPGWatch(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
	defer cancel()
	opts = append([]testcontainers.ContainerCustomizer{ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(3))}, opts...)
	cluster, client := harness.NewServiceCluster(t, opts...)
	const pgCount = 8
	pool, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: poolWatchPool, Application: "rados", Replicas: 2, MinSize: 1, PGNum: pgCount})
	if err != nil {
		t.Fatal(err)
	}
	observer := harness.ObserveCluster(cluster)
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	// The cost of one read, the same five CLI calls the watch makes.
	const samples = 5
	began := time.Now()
	for range samples {
		if _, err := pool.PGs(ctx); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("query cost: %d Pool.PGs reads took %v (%v each)", samples, time.Since(began), time.Since(began)/samples)

	const interval = 2 * time.Second
	watchStarted := time.Now()
	w, err := pool.WatchPGs(ctx, ceph.WatchConfig{Interval: interval})
	if err != nil {
		t.Fatal(err)
	}
	// Registered after the cluster cleanup, so it runs before it.
	t.Cleanup(func() { _ = w.Stop() })
	var last uint64
	gaps := 0
	first := <-w.Events()
	last = first.Sequence
	if first.Kind != ceph.WatchBaseline || first.Sequence != 1 || !poolPGWatchAllClean(t, first, pgCount) {
		t.Fatalf("baseline = %+v", first)
	}
	poolID := first.Snapshot.PoolBefore.ID
	t.Logf("baseline: pool %d, epochs %d..%d, read %v", poolID, first.Snapshot.OSDMapEpochBefore, first.Snapshot.OSDMapEpochAfter, first.Finished.Sub(first.Started))

	// Stop the acting primary of the first PG, so at least that PG loses an
	// acting member.
	watched := first.Snapshot.PGs[0]
	var target *ceph.OSDContainer
	for _, osd := range clusteraccess.OSDs(t, cluster) {
		if osd.ID == watched.ActingPrimary {
			target = osd
		}
	}
	if target == nil {
		t.Fatalf("PG %s acting primary %d is not an owned OSD", watched.PGID, watched.ActingPrimary)
	}
	grace := 10 * time.Second
	if err := target.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	stopped := time.Now()
	degraded := awaitPoolPGWatchEvent(t, w, poolID, &last, &gaps, 3*time.Minute, "degraded", func(e ceph.PoolPGWatchEvent) bool {
		pg, ok := poolPGWatchRow(e, watched.PGID)
		tokens := strings.Split(pg.State, "+")
		return ok && slices.Contains(tokens, "active") && (slices.Contains(tokens, "degraded") || slices.Contains(tokens, "undersized")) && !slices.Contains(pg.Acting, target.ID)
	})
	if degraded.Snapshot.FSID != first.Snapshot.FSID || degraded.Snapshot.OSDMapEpochAfter <= first.Snapshot.OSDMapEpochAfter {
		t.Fatalf("degraded event = %+v", degraded)
	}
	t.Logf("degraded: event %d %s, observed %v after the stop returned: %s", degraded.Sequence, degraded.Kind, degraded.Finished.Sub(stopped), poolPGWatchStates(degraded))

	// The degraded report advances the test: with min_size 1 the client
	// must still write and read back through the pool.
	payload := bytes.Repeat([]byte("written while degraded\n"), 256)
	poolPGWatchRoundTrip(t, ctx, client, "while-degraded", payload)

	if err := target.Start(ctx); err != nil {
		t.Fatal(err)
	}
	restarted := time.Now()
	clean := awaitPoolPGWatchEvent(t, w, poolID, &last, &gaps, 5*time.Minute, "clean", func(e ceph.PoolPGWatchEvent) bool {
		pg, ok := poolPGWatchRow(e, watched.PGID)
		return poolPGWatchAllClean(t, e, pgCount) && ok && slices.Equal(pg.Up, watched.Up) && slices.Contains(pg.Acting, target.ID)
	})
	t.Logf("clean: event %d %s, observed %v after the start returned", clean.Sequence, clean.Kind, clean.Finished.Sub(restarted))

	// Independent outcomes: the fixture's own clean wait and the object
	// written while degraded.
	if err := observer.WaitForClean(ctx); err != nil {
		t.Fatal(err)
	}
	poolPGWatchReadBack(t, ctx, client, "while-degraded", payload)
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
	t.Logf("native pool PG watch: baseline -> osd.%d stopped -> pg %s reported degraded -> round trip -> restarted -> all %d PGs reported clean; %d events, %d gaps, watch ran %v at a %v interval (about %d reads)",
		target.ID, watched.PGID, pgCount, last, gaps, elapsed.Round(time.Second), interval, int(elapsed/interval)+1)
}
