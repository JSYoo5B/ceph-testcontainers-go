package multisite

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// syncWatchTestConfig polls fast enough for unit tests.
var syncWatchTestConfig = ceph.WatchConfig{Interval: 100 * time.Millisecond, QueryTimeout: 5 * time.Second}

// nextSyncEvent receives the next event or fails after a bound.
func nextSyncEvent[E any](t *testing.T, events <-chan E) E {
	t.Helper()
	select {
	case e, ok := <-events:
		if !ok {
			t.Fatal("the watch ended")
		}
		return e
	case <-time.After(10 * time.Second):
		t.Fatal("no event")
	}
	panic("unreachable")
}

// underTopology changes the fake while the fixture's topology lock is held.
// The watch reads only under that lock, so the change never races a read,
// and taking it proves the watch does not hold it between reads.
func underTopology(f *Fixture, change func()) {
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	change()
}

func nextPeriod(cli *rgwSyncCLI) {
	cli.current = syncJSONClone(cli.current)
	cli.current["id"], cli.current["realm_epoch"] = "period-next", json.Number("2")
	cli.current["period_map"].(map[string]any)["id"] = "period-next"
}

func TestRGWWatchSyncReportsLagGapAndRecovery(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	w, err := f.WatchSync(t.Context(), syncWatchTestConfig, "destination", "source")
	if err != nil {
		t.Fatal(err)
	}
	baseline := nextSyncEvent(t, w.Events())
	if baseline.Sequence != 1 || baseline.Kind != ceph.WatchBaseline || !baseline.Ready || baseline.Status.PeriodID != "period-original" || len(baseline.Status.DataSources) != 1 {
		t.Fatalf("baseline %+v", baseline)
	}

	underTopology(f, func() {
		cli.humanOutput = strings.Replace(rgwReadyText, "data is caught up with source", "data is behind on 3 shards", 1)
	})
	lag := nextSyncEvent(t, w.Events())
	if lag.Kind != ceph.WatchChanged || lag.Ready || lag.Status.DataSources[0].BehindShards != 3 || lag.Err != nil {
		t.Fatalf("lag %+v", lag)
	}
	// The event owns its slice: changing it changes nothing the watch keeps.
	lag.Status.DataSources[0].BehindShards = 99

	underTopology(f, func() { cli.humanOutput = "" })
	gap := nextSyncEvent(t, w.Events())
	if gap.Kind != ceph.WatchGap || gap.Ready || gap.Err == nil {
		t.Fatalf("gap %+v", gap)
	}
	underTopology(f, func() {
		cli.humanOutput = strings.Replace(rgwReadyText, "data is caught up with source", "data is behind on 3 shards", 1)
	})
	resumed := nextSyncEvent(t, w.Events())
	if resumed.Kind != ceph.WatchResumed || resumed.Ready || resumed.Status.DataSources[0].BehindShards != 3 {
		t.Fatalf("resumed %+v", resumed)
	}
	underTopology(f, func() { cli.humanOutput = rgwReadyText })
	ready := nextSyncEvent(t, w.Events())
	if ready.Kind != ceph.WatchChanged || !ready.Ready || ready.Sequence != 5 {
		t.Fatalf("ready %+v", ready)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-w.Events(); ok {
		t.Fatal("events stayed open after Stop")
	}
}

func TestRGWWatchSyncEndsOnPeriodChangeAndTerminate(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	w, err := f.WatchSync(t.Context(), syncWatchTestConfig, "destination", "source")
	if err != nil {
		t.Fatal(err)
	}
	nextSyncEvent(t, w.Events())
	underTopology(f, func() { nextPeriod(cli) })
	for range w.Events() {
	}
	if err := w.Stop(); !errors.Is(err, errSyncPeriodChanged) {
		t.Fatalf("period change ended with %v", err)
	}

	f, cli, _, _ = newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	w, err = f.WatchSync(t.Context(), syncWatchTestConfig, "destination")
	if err != nil {
		t.Fatal(err)
	}
	underTopology(f, func() { f.closed = true })
	for range w.Events() {
	}
	if err := w.Stop(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("terminated fixture ended with %v", err)
	}
}

func TestRGWWatchSyncRefusesInvalidRequests(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	if _, err := f.WatchSync(t.Context(), ceph.WatchConfig{Interval: time.Millisecond}, "destination"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("short interval: %v", err)
	}
	if _, err := f.WatchSync(t.Context(), syncWatchTestConfig, "destination", "destination"); err == nil {
		t.Fatal("the zone itself was accepted as a source")
	}
	if _, err := (*Fixture)(nil).WatchSync(t.Context(), syncWatchTestConfig, "destination"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil fixture: %v", err)
	}
	if err := (&SyncWatch{}).Stop(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("unstarted Stop: %v", err)
	}
	// A failed baseline starts nothing.
	cli.humanOutput = ""
	if w, err := f.WatchSync(t.Context(), syncWatchTestConfig, "destination", "source"); err == nil || w != nil {
		t.Fatalf("failed baseline started %v %v", w, err)
	}
}

func TestRGWWatchSyncComparesOnlySelectedSources(t *testing.T) {
	a := SyncStatus{Metadata: MetadataSyncStatus{State: "sync", CaughtUp: true}, DataSources: []DataSyncStatus{{SourceZone: "source", CaughtUp: true}, {SourceZone: "other", CaughtUp: true}}}
	b := a
	b.DataSources = []DataSyncStatus{{SourceZone: "other"}, {SourceZone: "source", CaughtUp: true}}
	if !syncWatchEqual(a, b, []string{"source"}) {
		t.Error("an unselected source or the order changed the selection")
	}
	if syncWatchEqual(a, b, []string{"source", "other"}) {
		t.Error("a selected source's change was ignored")
	}
	if syncWatchEqual(a, SyncStatus{Metadata: a.Metadata}, []string{"source"}) {
		t.Error("a selected source that disappeared was ignored")
	}
	if !syncWatchEqual(a, SyncStatus{Metadata: a.Metadata}, nil) {
		t.Error("data sources were compared for a metadata-only watch")
	}
}

func TestRGWWatchBucketSyncKeepsItsCheckpoint(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	w, err := g.WatchBucketSync(t.Context(), syncWatchTestConfig, "selected", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	baseline := nextSyncEvent(t, w.Events())
	if baseline.Kind != ceph.WatchBaseline || !baseline.Status.CaughtUp || baseline.Status.SourceBucket.ID != "bucket-original" {
		t.Fatalf("baseline %+v", baseline)
	}
	underTopology(f, func() {
		cli.bucketStatusOutput = strings.Replace(rgwReadyBucketStatus, `"behind_shards":[]`, `"behind_shards":[{"shard_id":0,"shard_marker":"secret-marker"}]`, 1)
	})
	lag := nextSyncEvent(t, w.Events())
	if lag.Kind != ceph.WatchChanged || lag.Status.CaughtUp || lag.Status.BehindShards != 1 {
		t.Fatalf("lag %+v", lag)
	}
	underTopology(f, func() { cli.bucketStatusOutput = rgwReadyBucketStatus })
	if caught := nextSyncEvent(t, w.Events()); caught.Kind != ceph.WatchChanged || !caught.Status.CaughtUp {
		t.Fatalf("caught up %+v", caught)
	}
	underTopology(f, func() { cli.bucketID = "bucket-recreated" })
	for range w.Events() {
	}
	if err := w.Stop(); !errors.Is(err, errSyncBucketRecreated) {
		t.Fatalf("recreated bucket ended with %v", err)
	}
}

func TestRGWWatchBucketSyncEndsOnGroupOrPeriodChange(t *testing.T) {
	_, _, g := newRGWSyncBucketStatusFixture(t)
	w, err := g.WatchBucketSync(t.Context(), syncWatchTestConfig, "selected", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	nextSyncEvent(t, w.Events())
	if err := g.SetStatus(t.Context(), SyncAllowed); err != nil {
		t.Fatal(err)
	}
	for range w.Events() {
	}
	if err := w.Stop(); err == nil || !strings.Contains(err.Error(), "changed since the baseline") {
		t.Fatalf("group change ended with %v", err)
	}

	f, cli, g := newRGWSyncBucketStatusFixture(t)
	w, err = g.WatchBucketSync(t.Context(), syncWatchTestConfig, "selected", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	nextSyncEvent(t, w.Events())
	underTopology(f, func() { nextPeriod(cli) })
	for e := range w.Events() {
		if e.Kind != ceph.WatchGap {
			t.Fatalf("event after the period change %+v", e)
		}
	}
	if err := w.Stop(); !errors.Is(err, errSyncPeriodChanged) {
		t.Fatalf("period change ended with %v", err)
	}
}
