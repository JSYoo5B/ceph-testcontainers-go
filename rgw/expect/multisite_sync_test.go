package expect

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

func caughtUpStream(source, id string) multisite.DataSyncStatus {
	return multisite.DataSyncStatus{SourceZone: source, SourceZoneID: id, Syncing: true, CaughtUp: true, Shards: 128, IncrementalShards: 128}
}

func behindStream(source, id string, behind int) multisite.DataSyncStatus {
	s := caughtUpStream(source, id)
	s.CaughtUp, s.BehindShards = false, behind
	return s
}

func zoneStatus(zone, id string, sources ...multisite.DataSyncStatus) multisite.SyncStatus {
	return multisite.SyncStatus{RealmID: "realm-1", PeriodID: "period-1", RealmEpoch: 2, Zone: zone, ZoneID: id, ZonegroupID: "zg-1",
		MetadataMaster: zone == "a", Metadata: multisite.MetadataSyncStatus{State: "sync", CaughtUp: true}, DataSources: sources}
}

// During a cut of b's link to a, b's stream from a is behind while a's
// stream from b is caught up: two observations, one per receiving zone.
func cutObservations() (b, a multisite.SyncStatus) {
	return zoneStatus("b", "zone-b", behindStream("a", "zone-a", 3)), zoneStatus("a", "zone-a", caughtUpStream("b", "zone-b"))
}

func TestMultisiteDataSyncMatchesAsymmetricLag(t *testing.T) {
	b, a := cutObservations()
	got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncBehind}.Evaluate(b)
	if err != nil || !got.Matched || got.Stream != b.DataSources[0] {
		t.Fatalf("b behind a: %+v %v", got, err)
	}
	if s := got.String(); s != "data sync of zone b from a want behind (observed 128 shards: 128 incremental, 0 full, 3 behind, 0 recovering): match" {
		t.Fatalf("summary: %q", s)
	}
	if got, err := (MultisiteDataSync{Zone: "a", Source: "b", State: MultisiteDataSyncCaughtUp}).Evaluate(a); err != nil || !got.Matched {
		t.Fatalf("a caught up with b: %+v %v", got, err)
	}
}

// The opposite stream does not pass for the selected one: an expectation
// evaluated against the other zone's observation names the zone, and the
// state of the other direction is a mismatch of its own.
func TestMultisiteDataSyncSwappedDirectionDoesNotMatch(t *testing.T) {
	b, a := cutObservations()
	got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncBehind}.Evaluate(a)
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, []MultisiteDataSyncMismatch{{Reason: MultisiteDataSyncMismatchZone, Want: "b", Observed: "a"}}) || got.Stream != (multisite.DataSyncStatus{}) {
		t.Fatalf("other zone: %+v %v", got, err)
	}
	if s := got.String(); s != "data sync of zone b from a want behind: zone: want b, observed a" {
		t.Fatalf("summary: %q", s)
	}
	got, err = MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncCaughtUp}.Evaluate(b)
	want := []MultisiteDataSyncMismatch{
		{Reason: MultisiteDataSyncMismatchCaughtUp, Want: "true", Observed: "false"},
		{Reason: MultisiteDataSyncMismatchBehind, Want: "0", Observed: "3"},
	}
	if err != nil || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("lagging stream as caught up: %+v %v", got, err)
	}
	got, err = MultisiteDataSync{Zone: "a", Source: "b", State: MultisiteDataSyncBehind}.Evaluate(a)
	want = []MultisiteDataSyncMismatch{
		{Reason: MultisiteDataSyncMismatchCaughtUp, Want: "false", Observed: "true"},
		{Reason: MultisiteDataSyncMismatchBehind, Want: ">0", Observed: "0"},
	}
	if err != nil || !slices.Equal(got.Mismatches, want) || !strings.HasSuffix(got.String(), ": caught-up: want false, observed true; behind-shards: want >0, observed 0") {
		t.Fatalf("caught-up stream as behind: %+v %v", got, err)
	}
}

// A stream that is only not caught up is not evidence of backlog.
func TestMultisiteDataSyncBehindNeedsPositiveBacklog(t *testing.T) {
	for name, stream := range map[string]multisite.DataSyncStatus{
		"full sync":  {SourceZone: "a", SourceZoneID: "zone-a", Syncing: true, Shards: 128, IncrementalShards: 100, FullShards: 28},
		"no shards":  {SourceZone: "a", SourceZoneID: "zone-a"},
		"recovering": {SourceZone: "a", SourceZoneID: "zone-a", Syncing: true, Shards: 128, IncrementalShards: 128, RecoveringShards: 2},
	} {
		got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncBehind}.Evaluate(zoneStatus("b", "zone-b", stream))
		if err != nil || !slices.Equal(got.Mismatches, []MultisiteDataSyncMismatch{{Reason: MultisiteDataSyncMismatchBehind, Want: ">0", Observed: "0"}}) {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
}

func TestMultisiteDataSyncMissingSourceIsTheOnlyMismatch(t *testing.T) {
	observed := zoneStatus("b", "zone-b", caughtUpStream("c", "zone-c"))
	got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncCaughtUp}.Evaluate(observed)
	if err != nil || got.Matched || !slices.Equal(got.Mismatches, []MultisiteDataSyncMismatch{{Reason: MultisiteDataSyncMismatchSource, Want: "a", Observed: "[c]"}}) {
		t.Fatalf("missing source: %+v %v", got, err)
	}
	got, err = MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncBehind}.Evaluate(zoneStatus("b", "zone-b"))
	if err != nil || !slices.Equal(got.Mismatches, []MultisiteDataSyncMismatch{{Reason: MultisiteDataSyncMismatchSource, Want: "a", Observed: "[]"}}) {
		t.Fatalf("no sources: %+v %v", got, err)
	}
	if s := got.String(); s != "data sync of zone b from a want behind: source: want a, observed []" {
		t.Fatalf("summary: %q", s)
	}
}

func TestMultisiteDataSyncRecovery(t *testing.T) {
	recovered := zoneStatus("b", "zone-b", caughtUpStream("a", "zone-a"))
	if got, err := (MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncCaughtUp}).Evaluate(recovered); err != nil || !got.Matched {
		t.Fatalf("recovered: %+v %v", got, err)
	}
	// Still recovering after the link returns: every unfinished condition is
	// named.
	recovering := zoneStatus("b", "zone-b", multisite.DataSyncStatus{SourceZone: "a", SourceZoneID: "zone-a", Shards: 128, IncrementalShards: 120, FullShards: 8, BehindShards: 5, RecoveringShards: 1})
	got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncCaughtUp}.Evaluate(recovering)
	want := []MultisiteDataSyncMismatch{
		{Reason: MultisiteDataSyncMismatchCaughtUp, Want: "true", Observed: "false"},
		{Reason: MultisiteDataSyncMismatchSyncing, Want: "true", Observed: "false"},
		{Reason: MultisiteDataSyncMismatchFull, Want: "0", Observed: "8"},
		{Reason: MultisiteDataSyncMismatchBehind, Want: "0", Observed: "5"},
		{Reason: MultisiteDataSyncMismatchRecovering, Want: "0", Observed: "1"},
	}
	if err != nil || !slices.Equal(got.Mismatches, want) {
		t.Fatalf("recovering:\n got %+v\nwant %+v (%v)", got.Mismatches, want, err)
	}
}

func TestMultisiteDataSyncResultSharesNoSlice(t *testing.T) {
	b, _ := cutObservations()
	got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncBehind}.Evaluate(b)
	if err != nil {
		t.Fatal(err)
	}
	b.DataSources[0] = caughtUpStream("a", "zone-a")
	if got.Observed.DataSources[0].CaughtUp || got.Observed.DataSources[0].BehindShards != 3 {
		t.Fatalf("result follows the caller's slice: %+v", got.Observed.DataSources)
	}
}

func TestMultisiteDataSyncSummaryBoundsMismatches(t *testing.T) {
	e := MultisiteDataSyncEvaluation{Expectation: MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncCaughtUp}}
	for range evaluationShown + 1 {
		e.Mismatches = append(e.Mismatches, MultisiteDataSyncMismatch{Reason: MultisiteDataSyncMismatchBehind, Want: "0", Observed: "1"})
	}
	if s := e.String(); strings.Count(s, "behind-shards:") != evaluationShown || !strings.HasSuffix(s, "; 1 more") {
		t.Fatalf("summary: %q", s)
	}
}

func TestMultisiteDataSyncRefusesInvalidExpectations(t *testing.T) {
	b, _ := cutObservations()
	for name, want := range map[string]MultisiteDataSync{
		"no zone":       {Source: "a", State: MultisiteDataSyncBehind},
		"no source":     {Zone: "b", State: MultisiteDataSyncBehind},
		"same zone":     {Zone: "b", Source: "b", State: MultisiteDataSyncBehind},
		"no state":      {Zone: "b", Source: "a"},
		"unknown state": {Zone: "b", Source: "a", State: "stale"},
	} {
		if _, err := want.Evaluate(b); !errors.Is(err, ceph.ErrInvalidRequest) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestMultisiteDataSyncMalformedObservationIsNotAMismatch(t *testing.T) {
	bad := func(edit func(*multisite.SyncStatus)) multisite.SyncStatus {
		s, _ := cutObservations()
		s.DataSources = slices.Clone(s.DataSources)
		edit(&s)
		return s
	}
	for name, observed := range map[string]multisite.SyncStatus{
		"no zone":          {},
		"no zone ID":       bad(func(s *multisite.SyncStatus) { s.ZoneID = "" }),
		"unidentified":     bad(func(s *multisite.SyncStatus) { s.DataSources[0].SourceZone = "" }),
		"unidentified ID":  bad(func(s *multisite.SyncStatus) { s.DataSources[0].SourceZoneID = "" }),
		"from itself":      bad(func(s *multisite.SyncStatus) { s.DataSources[0].SourceZoneID = "zone-b" }),
		"listed twice":     bad(func(s *multisite.SyncStatus) { s.DataSources = append(s.DataSources, s.DataSources[0]) }),
		"negative":         bad(func(s *multisite.SyncStatus) { s.DataSources[0].BehindShards = -1 }),
		"caught up behind": bad(func(s *multisite.SyncStatus) { s.DataSources[0].CaughtUp = true }),
		"caught up unsynced": bad(func(s *multisite.SyncStatus) {
			s.DataSources[0] = caughtUpStream("a", "zone-a")
			s.DataSources[0].Syncing = false
		}),
	} {
		got, err := MultisiteDataSync{Zone: "b", Source: "a", State: MultisiteDataSyncBehind}.Evaluate(observed)
		if err == nil || errors.Is(err, ceph.ErrInvalidRequest) || got.Matched || got.Mismatches != nil {
			t.Errorf("%s: %+v %v", name, got, err)
		}
	}
}
