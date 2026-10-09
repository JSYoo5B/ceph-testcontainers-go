package multicluster

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

const rgwReadyMetadata = `{"sync_status":{"info":{"status":"sync","num_shards":2,"period":"period-original","realm_epoch":1},"markers":[{"key":0,"val":{"state":1}},{"key":1,"val":{"state":1}}]},"full_sync":{"total":0,"complete":0}}`
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

func TestRGWSyncStatusRejectsIncompleteAndMisleadingNativeReadiness(t *testing.T) {
	f, _, _, _ := newRGWSyncTestFixture()
	want := RGWSyncStatus{RealmID: "realm", ZonegroupID: "group-id", ZoneID: "dest-id"}
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
		if _, err := decodeSyncMetadata([]byte(metadata)); err == nil {
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
	ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	last, err := f.WaitSyncReady(ctx, "destination", "source")
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second || len(last.DataSources) != 1 || last.DataSources[0].CaughtUp {
		t.Fatalf("bounded wait lost last state: %+v %v", last, err)
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
