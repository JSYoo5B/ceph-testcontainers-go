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

func TestRGWWatchSyncEndsOnAPeriodTheZoneHasNotImported(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	w, err := f.WatchSync(t.Context(), syncWatchTestConfig, "destination", "source")
	if err != nil {
		t.Fatal(err)
	}
	nextSyncEvent(t, w.Events())
	underTopology(f, func() {
		stale := cli.current
		nextPeriod(cli)
		cli.periodByZone = map[string]map[string]any{"dest-id": stale}
	})
	for e := range w.Events() {
		t.Fatalf("event after the period change %+v", e)
	}
	if err := w.Stop(); !errors.Is(err, errSyncPeriodChanged) {
		t.Fatalf("an unimported new period ended the watch with %v", err)
	}
}

func TestRGWWatchBucketSyncEndsOnAPeriodThatDropsThePipe(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.additionalBuckets = map[string]string{"input": "source-instance", "output": "destination-instance"}
	g, err := f.CreateSyncGroup(t.Context(), SyncPolicyScope{}, SyncGroupConfig{ID: "published", Status: SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.CreatePipe(t.Context(), SyncPipeConfig{ID: "selected", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &SyncBucketSelector{Name: "input"}, DestinationBucket: &SyncBucketSelector{Name: "output"}}); err != nil {
		t.Fatal(err)
	}
	if err := g.Apply(t.Context()); err != nil {
		t.Fatal(err)
	}
	cli.bucketStatusOutput = `{"realm":"realm","zonegroup":"group-id","zone":"dest-id","bucket":"output","bucket_instance_id":"destination-instance","sources":[{"source_zone":"source-id","source_name":"source","source_bucket":"input","source_bucket_id":"source-instance","total_shards":1,"behind_shards":[]}]}`
	w, err := g.WatchBucketSync(t.Context(), syncWatchTestConfig, "selected", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	if baseline := nextSyncEvent(t, w.Events()); !baseline.Status.CaughtUp {
		t.Fatalf("baseline %+v", baseline)
	}
	// A new committed period without the group: the read learns the period,
	// then fails the publication check.
	underTopology(f, func() {
		nextPeriod(cli)
		current := syncPeriodGroups(cli.current)[0]
		current["sync_policy"] = syncPolicyReplace(current["sync_policy"].(map[string]any), g.ID(), nil)
	})
	for e := range w.Events() {
		t.Fatalf("event after the period change %+v", e)
	}
	if err := w.Stop(); !errors.Is(err, errSyncPeriodChanged) {
		t.Fatalf("a period that dropped the pipe ended the watch with %v", err)
	}
}

func TestRGWWatchBucketPolicyReportsStagesUntilImported(t *testing.T) {
	f, cli, g := newRGWPolicyImportFixture(t)
	cli.localPeriod = syncJSONClone(cli.current)
	cli.localPeriod["epoch"] = json.Number("0")
	before := len(cli.calls)
	w, err := g.WatchBucketPolicy(t.Context(), syncWatchTestConfig, "destination")
	if err != nil {
		t.Fatal(err)
	}
	baseline := nextSyncEvent(t, w.Events())
	if baseline.Sequence != 1 || baseline.Kind != ceph.WatchBaseline || baseline.Status.PeriodImported || baseline.Status.Imported || baseline.Status.PeriodID != "period-original" || baseline.Status.ZoneID != "dest-id" || len(baseline.Status.Buckets) != 1 {
		t.Fatalf("baseline %+v", baseline)
	}
	// The event owns its slice: changing it changes nothing the watch keeps.
	baseline.Status.Buckets[0].ID = "caller-mutated"

	underTopology(f, func() { cli.localPeriod, cli.policyLagReads = nil, 1000 })
	period := nextSyncEvent(t, w.Events())
	if period.Kind != ceph.WatchChanged || !period.Status.PeriodImported || period.Status.PolicyImported || !period.Status.BucketsImported || period.Status.Imported {
		t.Fatalf("period stage %+v", period)
	}
	underTopology(f, func() { cli.missingBuckets = map[string]bool{"bucket": true} })
	gap := nextSyncEvent(t, w.Events())
	if gap.Kind != ceph.WatchGap || gap.Err == nil || !gap.Status.PeriodImported || gap.Status.BucketsImported || gap.Status.Imported || strings.Contains(gap.Err.Error(), "secret-marker") {
		t.Fatalf("gap %+v", gap)
	}
	underTopology(f, func() { cli.missingBuckets, cli.policyLagReads = nil, 0 })
	imported := nextSyncEvent(t, w.Events())
	if imported.Kind != ceph.WatchResumed || !imported.Status.Imported || !imported.Status.PolicyImported || imported.Sequence != 4 || imported.Status.Buckets[0].ID != "bucket-original" {
		t.Fatalf("imported %+v", imported)
	}
	if _, ok := <-w.Events(); ok {
		t.Fatal("the watch continued after the imported status")
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("an imported watch ended with %v", err)
	}
	requirePolicyImportReadOnly(t, cli.calls[before:])
}

func TestRGWWatchBucketPolicyEndsAtAnImportedBaseline(t *testing.T) {
	_, _, g := newRGWPolicyImportFixture(t)
	w, err := g.WatchBucketPolicy(t.Context(), syncWatchTestConfig, "destination")
	if err != nil {
		t.Fatal(err)
	}
	var events []BucketPolicyWatchEvent
	for e := range w.Events() {
		events = append(events, e)
	}
	if len(events) != 1 || events[0].Kind != ceph.WatchBaseline || !events[0].Status.Imported {
		t.Fatalf("events %+v", events)
	}
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestRGWWatchBucketPolicyEndsOnPinnedChanges(t *testing.T) {
	for _, change := range []string{"period", "recreated", "native-policy", "fixture-group", "detached", "terminated"} {
		t.Run(change, func(t *testing.T) {
			f, cli, g := newRGWPolicyImportFixture(t)
			cli.policyLagReads = 1000
			w, err := g.WatchBucketPolicy(t.Context(), syncWatchTestConfig, "destination")
			if err != nil {
				t.Fatal(err)
			}
			if baseline := nextSyncEvent(t, w.Events()); baseline.Status.Imported || baseline.Status.PolicyImported {
				t.Fatalf("baseline %+v", baseline)
			}
			if change == "fixture-group" {
				if err := g.SetStatus(t.Context(), SyncAllowed); err != nil {
					t.Fatal(err)
				}
			}
			underTopology(f, func() {
				switch change {
				case "period":
					nextPeriod(cli.rgwSyncCLI)
				case "recreated":
					cli.bucketID = "bucket-recreated"
				case "native-policy":
					syncNativeGroup(cli.bucketPolicy, g.ID())["status"] = "forbidden"
				case "detached":
					f.realm.destinationClient = nil
				case "terminated":
					f.closed = true
				}
			})
			for e := range w.Events() {
				t.Fatalf("event after the change %+v", e)
			}
			err = w.Stop()
			switch change {
			case "recreated":
				if !errors.Is(err, errSyncBucketRecreated) {
					t.Fatalf("recreated bucket ended with %v", err)
				}
			case "terminated":
				if !errors.Is(err, ceph.ErrInvalidRequest) {
					t.Fatalf("terminated fixture ended with %v", err)
				}
			default:
				if err == nil || !strings.HasPrefix(err.Error(), bucketPolicyWatchOperation) {
					t.Fatalf("%s ended with %v", change, err)
				}
			}
		})
	}
}

func TestRGWWatchBucketPolicyRefusesInvalidRequests(t *testing.T) {
	_, cli, g := newRGWPolicyImportFixture(t)
	if _, err := g.WatchBucketPolicy(t.Context(), ceph.WatchConfig{Interval: time.Millisecond}, "destination"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("short interval: %v", err)
	}
	if _, err := (*SyncGroup)(nil).WatchBucketPolicy(t.Context(), syncWatchTestConfig, "destination"); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("nil group: %v", err)
	}
	if err := (&BucketPolicyWatch{}).Stop(); !errors.Is(err, ceph.ErrInvalidRequest) {
		t.Fatalf("unstarted Stop: %v", err)
	}
	before := len(cli.calls)
	if w, err := g.WatchBucketPolicy(t.Context(), syncWatchTestConfig, "source"); err == nil || w != nil || len(cli.calls) != before {
		t.Fatalf("a zone outside the pipes' destinations started %v %v", w, err)
	}
	// A failed baseline starts nothing.
	cli.missingBuckets = map[string]bool{"bucket": true}
	if w, err := g.WatchBucketPolicy(t.Context(), syncWatchTestConfig, "destination"); err == nil || w != nil {
		t.Fatalf("failed baseline started %v %v", w, err)
	}
}
