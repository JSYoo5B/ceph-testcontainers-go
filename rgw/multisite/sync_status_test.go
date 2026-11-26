package multisite

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

const rgwReadyMetadata = `{"sync_status":{"info":{"status":"sync","num_shards":2,"period":"period-original","realm_epoch":1},"markers":[{"key":0,"val":{"state":1}},{"key":1,"val":{"state":1}}]},"full_sync":{"total":0,"complete":0}}`

func TestMetadataLogComparisonRequiresEveryShard(t *testing.T) {
	empty, current := "", "marker-2"
	local := map[int]*string{0: &empty, 1: &current}
	for _, tt := range []struct {
		name, remote string
		caughtUp     bool
		bad          bool
	}{
		{"current, including empty shard", `{"num_objects":2} {"marker":""} {"marker":"marker-2"}`, true, false},
		{"behind on populated shard", `{"num_objects":2} {"marker":""} {"marker":"marker-3"}`, false, false},
		{"behind on previously empty shard", `{"num_objects":2} {"marker":"marker-1"} {"marker":"marker-2"}`, false, false},
		{"unexpected inventory", `{"num_objects":1} {"marker":""}`, false, true},
		{"missing shard", `{"num_objects":2} {"marker":""}`, false, true},
		{"null marker", `{"num_objects":2} {"marker":null} {"marker":"marker-2"}`, false, true},
		{"extra shard", `{"num_objects":2} {"marker":""} {"marker":"marker-2"} {"marker":""}`, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compareMetadataLog([]byte(tt.remote), local)
			if got != tt.caughtUp || (err != nil) != tt.bad {
				t.Fatalf("caught up = %t, error = %v", got, err)
			}
		})
	}
}

const rgwReadyText = `          realm realm (fixture)
      zonegroup group-id (group)
           zone dest-id (destination)
   current time 2026-10-03T06:37:03Z
zonegroup features enabled: notification_v2,resharding
                   disabled: compress-encrypted
  metadata sync syncing
                full sync: 0/2 shards
                incremental sync: 2/2 shards
                metadata is caught up with master
      data sync source: source-id (source)
                        syncing
                        full sync: 0/128 shards
                        incremental sync: 128/128 shards
                        data is caught up with source
`

func TestRGWSyncStatusAndWaitRequireScopedRemoteComparison(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	status, err := f.WaitSyncReady(t.Context(), "destination", "source")
	if err != nil || status.MetadataMaster || !status.Metadata.CaughtUp || len(status.DataSources) != 1 || !status.DataSources[0].CaughtUp || status.DataSources[0].SourceZoneID != "source-id" {
		t.Fatalf("ready state rejected: %+v, %v", status, err)
	}
	cli.humanOutput = strings.Replace(rgwReadyText, "data is caught up with source", "data is behind on 1 shards", 1)
	status, err = f.SyncStatus(t.Context(), "destination")
	if err != nil || syncStatusReady(status, []string{"source"}) || status.DataSources[0].BehindShards != 1 {
		t.Fatal("remote log lag was treated as readiness")
	}
	cli.humanOutput = rgwReadyText
	cli.metadataOutput = strings.Replace(rgwReadyMetadata, `"period":"period-original"`, `"period":"old-period"`, 1)
	status, err = f.SyncStatus(t.Context(), "destination")
	if err != nil || status.Metadata.CaughtUp {
		t.Fatal("metadata on a different period was accepted")
	}
}

func TestRGWSyncStatusRequiresTheMasterPeriodEpoch(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	old := syncJSONClone(cli.current)
	cli.current["epoch"] = json.Number("2")
	cli.periodByZone = map[string]map[string]any{"dest-id": old}
	if _, err := f.SyncStatus(t.Context(), "destination"); err == nil || !strings.Contains(err.Error(), "has not imported") {
		t.Fatalf("same-ID older period epoch was accepted: %v", err)
	}
	delete(cli.periodByZone, "dest-id")
	if _, err := f.SyncStatus(t.Context(), "destination"); err != nil {
		t.Fatalf("matching current period was refused: %v", err)
	}
}

func TestRGWPolicyMasterReadsSelectedMastersOwnPeriod(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	current := syncJSONClone(cli.current)
	current["master_zone"] = "dest-id"
	current["epoch"] = json.Number("2")
	group := syncPeriodGroups(current)[0]
	group["master_zone"] = "dest-id"
	group["endpoints"] = []any{"http://destination:7480"}
	group["is_master"] = true
	stale := syncJSONClone(current)
	stale["epoch"] = json.Number("1")
	cli.current = current
	cli.periodByZone = map[string]map[string]any{"source-id": stale}
	master, period, err := f.syncPolicyMaster(t.Context())
	if err != nil || master.ID != "dest-id" || period["epoch"] != json.Number("2") {
		t.Fatalf("selected master's current period: %v %+v %v", master, period, err)
	}
}

func TestRGWSyncStatusDiscoversNewMasterPastFormerMastersOldPointer(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	old := syncJSONClone(cli.current)
	current := syncJSONClone(old)
	current["master_zone"] = "dest-id"
	current["epoch"] = json.Number("2")
	group := syncPeriodGroups(current)[0]
	group["master_zone"] = "dest-id"
	group["endpoints"] = []any{"http://destination:7480"}
	group["is_master"] = true
	cli.current = current
	cli.periodByZone = map[string]map[string]any{"source-id": old}
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	status, err := f.SyncStatus(t.Context(), "destination")
	if err != nil || !status.MetadataMaster || status.periodEpoch != 2 {
		t.Fatalf("new master's committed period was missed: %+v, %v", status, err)
	}
	stale, err := f.SyncStatus(t.Context(), "source")
	if err == nil || stale.MetadataMaster || stale.periodEpoch != 2 || !strings.Contains(err.Error(), "has not imported") {
		t.Fatalf("former master's old period was treated as current: %+v, %v", stale, err)
	}
}

func TestRGWSyncStatusRefusesIncompleteOrConflictingPeriodCandidates(t *testing.T) {
	for _, tc := range []struct {
		name, want string
		period     func(map[string]any) map[string]any
	}{
		{"incomplete", "committed", func(map[string]any) map[string]any { return map[string]any{"id": "period-original"} }},
		{"same version different master", "disagree", func(current map[string]any) map[string]any {
			candidate := syncJSONClone(current)
			candidate["master_zone"] = "dest-id"
			return candidate
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, cli, _, _ := newRGWSyncTestFixture()
			cli.periodByZone = map[string]map[string]any{"dest-id": tc.period(cli.current)}
			if _, err := f.SyncStatus(t.Context(), "source"); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("invalid attached period was accepted: %v", err)
			}
		})
	}
}

func TestRGWSyncStatusRejectsIncompleteAndMisleadingNativeReadiness(t *testing.T) {
	f, _, _, _ := newRGWSyncTestFixture()
	want := SyncStatus{RealmID: "realm", ZonegroupID: "group-id", ZoneID: "dest-id"}
	for _, change := range []struct{ old, new string }{
		{"metadata is caught up with master", "metadata is caught up with master\nmaster is on a different period"},
		{"data is caught up with source", "data is caught up with source\n2 shards are recovering"},
		{"incremental sync: 128/128 shards", "incremental sync: 127/128 shards"},
		{"syncing\n                        full sync", "not syncing\n                        full sync"},
		{"data is caught up with source", "data is caught up with source\nfailed to fetch source sync status"},
	} {
		t.Run(change.new, func(t *testing.T) {
			parsed, err := parseSyncStatusText([]byte(strings.Replace(rgwReadyText, change.old, change.new, 1)), want, f.zoneStates())
			if err != nil {
				t.Fatal(err)
			}
			if parsed.metadataCaughtUp && parsed.data[0].CaughtUp {
				t.Fatal("native warning or incomplete shard state was ignored")
			}
		})
	}
	for _, text := range []string{strings.Replace(rgwReadyText, "zone dest-id", "zone foreign-id", 1), strings.Replace(rgwReadyText, "source: source-id", "source: foreign-source", 1), "metadata sync metadata is caught up with master"} {
		if _, err := parseSyncStatusText([]byte(text), want, f.zoneStates()); err == nil {
			t.Fatal("foreign or missing identity was accepted")
		}
	}
	for _, metadata := range []string{strings.Replace(rgwReadyMetadata, `"key":1`, `"key":0`, 1), strings.Replace(rgwReadyMetadata, `"key":1`, `"key":2`, 1), "{}", "not json"} {
		if _, _, err := decodeSyncMetadata([]byte(metadata)); err == nil {
			t.Fatal("invalid native metadata shard state was accepted")
		}
	}
}

func TestRGWSyncWaitHonorsCancellationAndExplicitSources(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	for _, sources := range [][]string{{"destination"}, {"foreign"}, {"source", "source"}} {
		if _, err := f.WaitSyncReady(t.Context(), "destination", sources...); err == nil || len(cli.calls) != 0 {
			t.Fatal("invalid data source invoked CLI")
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.WaitSyncReady(ctx, "destination", "source"); !errors.Is(err, context.Canceled) || len(cli.calls) != 0 {
		t.Fatal("cancelled waiter invoked CLI")
	}
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, strings.Replace(rgwReadyText, "data is caught up with source", "data is behind on 1 shards", 1)
	// The deadline passes once the first observation of the destination
	// ended, rather than after a wall-clock interval that a slow
	// (race-instrumented) run can end before the observation read its sources.
	deadline := clustertest.NewSignalDeadline(t.Context())
	cli.onExec = expireAfterDestinationObservation(deadline)
	started := time.Now()
	last, err := f.WaitSyncReady(deadline, "destination", "source")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second || len(last.DataSources) != 1 || last.DataSources[0].CaughtUp {
		t.Fatalf("bounded wait lost last state: %+v %v", last, err)
	}
}

// TestRGWSyncReadyKeepsObservationWhenDeadlineCutsAnAttempt covers a wait
// whose deadline passes while a later observation fails: the wait returns the
// earlier complete observation and reports both it and the failed attempt,
// instead of the failed attempt's partial state and the deadline alone.
func TestRGWSyncReadyKeepsObservationWhenDeadlineCutsAnAttempt(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, strings.Replace(rgwReadyText, "data is caught up with source", "data is behind on 1 shards", 1)
	deadline := clustertest.NewSignalDeadline(t.Context())
	observations := 0
	cli.onExec = func(_ context.Context, args []string) {
		if syncTestFlag(args, "--zone-id") != "dest-id" || !syncTestCommand(args, "period", "get") {
			return
		}
		// Each sample reads the destination's period once while selecting
		// the current master and once while verifying its local import.
		if observations++; observations == 3 {
			cli.stderrOutput = "ERROR: could not find remote sync shard status for shard_id=17; secret-marker\n"
			deadline.Expire()
		}
	}
	last, err := f.WaitSyncReady(deadline, "destination", "source")
	if !errors.Is(err, context.DeadlineExceeded) || observations != 3 || len(last.DataSources) != 1 || last.DataSources[0].CaughtUp || last.DataSources[0].BehindShards != 1 {
		t.Fatalf("the cut attempt replaced the last observation: %+v %v (observations %d)", last, err, observations)
	}
	if msg := err.Error(); !strings.Contains(msg, "BehindShards:1") || strings.Contains(msg, "last_error=<nil>") || strings.Contains(msg, "secret-marker") {
		t.Fatalf("error does not report the observation and the failed attempt, or exposes native output: %v", err)
	}
}

func TestRGWSyncReadyStdoutDoesNotHideFailedRemoteComparisonOnStderr(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.metadataOutput, cli.humanOutput = rgwReadyMetadata, rgwReadyText
	cli.stderrOutput = "ERROR: could not find remote sync shard status for shard_id=17; secret-marker\n"
	status, err := f.SyncStatus(t.Context(), "destination")
	if err == nil || syncStatusReady(status, []string{"source"}) || strings.Contains(err.Error(), "secret-marker") {
		t.Fatalf("failed remote comparison was accepted or exposed: %+v %v", status, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := f.WaitSyncReady(ctx, "destination", "source"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("stderr comparison error satisfied readiness")
	}
}

func TestRGWCombinedStatusScopesDataComparisonFailure(t *testing.T) {
	for _, test := range []struct {
		name, metadata, text, stderr string
		sources                      []string
		ready                        bool
	}{
		{name: "healthy", metadata: rgwReadyMetadata, text: rgwReadyText, ready: true},
		{name: "unavailable data source", metadata: rgwReadyMetadata, text: strings.Replace(rgwReadyText, "data is caught up with source", "failed to fetch source sync status: Connection refused", 1), ready: true},
		{name: "selected unavailable source", metadata: rgwReadyMetadata, text: strings.Replace(rgwReadyText, "data is caught up with source", "failed to fetch source sync status: Connection refused", 1), sources: []string{"source"}},
		{name: "stale metadata period", metadata: strings.Replace(rgwReadyMetadata, "period-original", "old-period", 1), text: rgwReadyText},
		{name: "metadata behind master", metadata: rgwReadyMetadata, text: strings.Replace(rgwReadyText, "metadata is caught up with master", "metadata is behind on 1 shards", 1)},
		{name: "ambiguous comparison stderr", metadata: rgwReadyMetadata, text: rgwReadyText, stderr: "ERROR: could not find remote sync shard status for shard_id=17\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, cli, _, _ := newRGWSyncTestFixture()
			cli.metadataOutput, cli.humanOutput, cli.stderrOutput = test.metadata, test.text, test.stderr
			status, err := f.SyncStatus(t.Context(), "destination")
			if (err != nil) != (test.stderr != "") || syncStatusReady(status, test.sources) != test.ready {
				t.Fatalf("combined status gave the wrong readiness: %+v, %v", status, err)
			}
			if test.name == "unavailable data source" && (len(status.DataSources) != 1 || status.DataSources[0].CaughtUp) {
				t.Fatalf("data failure became readiness: %+v", status.DataSources)
			}
		})
	}
}
