package multicluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

const rgwReadyBucketStatus = `{"realm":"realm","zonegroup":"group-id","zone":"dest-id","bucket":"bucket","bucket_instance_id":"bucket-original","sources":[{"source_zone":"source-id","source_name":"source","source_bucket":"bucket","source_bucket_id":"bucket-original","total_shards":1,"behind_shards":[]}]}`

func newRGWSyncBucketStatusFixture(t *testing.T) (*RGWMultisite, *rgwSyncCLI, *RGWSyncGroup) {
	t.Helper()
	f, cli, _, _ := newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{Bucket: "bucket"}, RGWSyncGroupConfig{ID: "checkpoint", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "selected", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, Prefix: "selected/"}); err != nil {
		t.Fatal(err)
	}
	cli.bucketStatusOutput = rgwReadyBucketStatus
	return f, cli, g
}

func TestRGWBucketSyncStatusUsesOwnedPipeInstancesAndRemoteCheckpoint(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	status, err := f.WaitBucketSyncReady(t.Context(), g, "selected", "source", "destination")
	if err != nil || !status.CaughtUp || status.State != "incremental" || status.Shards != 1 || status.PeriodID != "period-original" || status.SourceBucket.ID != "bucket-original" || status.DestinationBucket.ID != "bucket-original" {
		t.Fatalf("exact bucket checkpoint rejected: %+v %v", status, err)
	}
	var native []string
	for _, args := range cli.calls {
		if syncTestCommand(args, "bucket", "sync", "status") {
			native = args
		}
	}
	if syncTestFlag(native, "--bucket-id") != "bucket-original" || syncTestFlag(native, "--source-bucket-id") != "bucket-original" || syncTestFlag(native, "--source-zone-id") != "source-id" || syncTestFlag(native, "--zone-id") != "dest-id" {
		t.Fatal("checkpoint used a guessed bucket or native zone")
	}
	cli.bucketStatusOutput = strings.Replace(rgwReadyBucketStatus, `"behind_shards":[]`, `"behind_shards":[{"shard_id":0,"shard_marker":"secret-marker"}]`, 1)
	status, err = f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination")
	if err != nil || status.CaughtUp || status.BehindShards != 1 || strings.Contains(status.State, "secret-marker") {
		t.Fatalf("native bucket lag was ignored or exposed: %+v %v", status, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	status, err = f.WaitBucketSyncReady(ctx, g, "selected", "source", "destination")
	if !errors.Is(err, context.DeadlineExceeded) || status.CaughtUp || status.BehindShards != 1 || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("bounded waiter accepted lag or exposed markers")
	}
}

func TestRGWBucketSyncDecoderRefusesDisabledFullAndExitZeroErrors(t *testing.T) {
	want := RGWBucketSyncStatus{RealmID: "realm", ZonegroupID: "group-id", SourceZone: "source", SourceZoneID: "source-id", ZoneID: "dest-id", SourceBucket: RGWSyncBucketIdentity{Name: "bucket", ID: "bucket-original"}, DestinationBucket: RGWSyncBucketIdentity{Name: "bucket", ID: "bucket-original"}}
	for _, test := range []struct{ replace, with, state string }{
		{`"total_shards":1,"behind_shards":[]`, `"error":"failed remote log secret-marker"`, "error"},
		{`"total_shards":1,"behind_shards":[]`, `"status":"init: bucket sync has not started"`, "init"},
		{`"total_shards":1,"behind_shards":[]`, `"status":"stopped: bucket sync is disabled"`, "stopped"},
		{`"total_shards":1,"behind_shards":[]`, `"status":"full sync: 9 objects completed"`, "full"},
		{`"sources":`, `"error":"Sync is disabled for bucket bucket or bucket has no sync sources","sources":`, "disabled_or_no_sources"},
	} {
		status, err := decodeBucketSyncStatus([]byte(strings.Replace(rgwReadyBucketStatus, test.replace, test.with, 1)), want)
		if err != nil || status.CaughtUp || status.State != test.state || strings.Contains(status.State, "secret-marker") {
			t.Fatalf("non-ready native result accepted/exposed: %+v %v", status, err)
		}
	}
	for _, change := range []struct{ before, after string }{
		{`"zone":"dest-id"`, `"zone":"foreign"`},
		{`"source_zone":"source-id"`, `"source_zone":"foreign"`},
		{`"source_bucket_id":"bucket-original"`, `"source_bucket_id":"new-instance"`},
		{`"bucket_instance_id":"bucket-original"`, `"bucket_instance_id":"new-instance"`},
		{`"total_shards":1,`, ""},
		{`"behind_shards":[]`, `"behind_shards":[{"shard_id":0},{"shard_id":0}]`},
	} {
		if _, err := decodeBucketSyncStatus([]byte(strings.Replace(rgwReadyBucketStatus, change.before, change.after, 1)), want); err == nil {
			t.Fatal("foreign identity or incomplete shard comparison accepted")
		}
	}
}

func TestRGWBucketCheckpointRejectsStaleUnownedAndUnpublishedInputs(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	for _, request := range []struct {
		group              *RGWSyncGroup
		pipe, source, dest string
	}{
		{&RGWSyncGroup{}, "selected", "source", "destination"},
		{g, "foreign", "source", "destination"},
		{g, "selected", "destination", "source"},
		{g, "selected", "source", "source"},
	} {
		if _, err := f.BucketSyncStatus(t.Context(), request.group, request.pipe, request.source, request.dest); err == nil {
			t.Fatal("unowned or wrong-direction checkpoint accepted")
		}
	}
	cli.bucketID = "recreated"
	before := len(cli.calls)
	if _, err := f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination"); err == nil {
		t.Fatal("recreated bucket name substituted into captured checkpoint")
	}
	for _, args := range cli.calls[before:] {
		if syncTestCommand(args, "bucket", "sync", "status") {
			t.Fatal("stale checkpoint reached native comparison")
		}
	}
	f, cli, _, _ = newRGWSyncTestFixture()
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "unpublished", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "selected", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination"); err == nil {
		t.Fatal("unpublished global policy accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before = len(cli.calls)
	if _, err := f.WaitBucketSyncReady(ctx, g, "selected", "source", "destination"); !errors.Is(err, context.Canceled) || len(cli.calls) != before {
		t.Fatal("cancelled checkpoint invoked native commands")
	}
}

func TestRGWBucketCheckpointRejectsRemoteFailureStderr(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	cli.stderrOutput = "ERROR: remote comparison failed secret-marker"
	status, err := f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination")
	if err == nil || status.CaughtUp || strings.Contains(err.Error(), "secret-marker") {
		t.Fatal("ready JSON hid stderr remote error or exposed native marker")
	}
}

func TestRGWBucketCheckpointRetainsTranslatedTenantInstanceSelectors(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.additionalBuckets = map[string]string{"tenant-a/input": "source-instance", "tenant-a/output": "destination-instance"}
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "translated", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	source, destination := &RGWSyncBucketSelector{Name: "input", Tenant: "tenant-a"}, &RGWSyncBucketSelector{Name: "output", Tenant: "tenant-a"}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "selected", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: source, DestinationBucket: destination}); err != nil {
		t.Fatal(err)
	}
	if err := f.ApplySyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	source.Name, destination.Tenant = "mutated", "different-tenant"
	cli.bucketStatusOutput = `{"realm":"realm","zonegroup":"group-id","zone":"dest-id","bucket":"output","bucket_instance_id":"destination-instance","sources":[{"source_zone":"source-id","source_name":"source","source_bucket":"input","source_bucket_id":"source-instance","total_shards":2,"behind_shards":[]}]}`
	status, err := f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination")
	if err != nil || !status.CaughtUp || status.SourceBucket != (RGWSyncBucketIdentity{Name: "input", Tenant: "tenant-a", ID: "source-instance"}) || status.DestinationBucket != (RGWSyncBucketIdentity{Name: "output", Tenant: "tenant-a", ID: "destination-instance"}) {
		t.Fatalf("private translated checkpoint changed: %+v %v", status, err)
	}
	var native []string
	for _, args := range cli.calls {
		if syncTestCommand(args, "bucket", "sync", "status") {
			native = args
		}
	}
	if syncTestFlag(native, "--bucket") != "tenant-a/output" || syncTestFlag(native, "--source-tenant") != "tenant-a" || syncTestFlag(native, "--source-bucket-id") != "source-instance" || syncTestFlag(native, "--tenant") != "" {
		t.Fatal("tenant checkpoint lost its native namespace/source instance")
	}
	cli.additionalBuckets["tenant-a/input"] = "replacement-source"
	before := len(cli.calls)
	if _, err := f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination"); err == nil {
		t.Fatal("translated source recreation accepted")
	}
	for _, args := range cli.calls[before:] {
		if syncTestCommand(args, "bucket", "sync", "status") {
			t.Fatal("stale translated source reached native comparison")
		}
	}
}

func TestRGWBucketCheckpointRequiresEveryMatchingPipeComparison(t *testing.T) {
	want := RGWBucketSyncStatus{RealmID: "realm", ZonegroupID: "group-id", SourceZone: "source", SourceZoneID: "source-id", ZoneID: "dest-id", SourceBucket: RGWSyncBucketIdentity{Name: "bucket", ID: "bucket-original"}, DestinationBucket: RGWSyncBucketIdentity{Name: "bucket", ID: "bucket-original"}}
	native, err := syncJSON([]byte(rgwReadyBucketStatus))
	if err != nil {
		t.Fatal(err)
	}
	source := native["sources"].([]any)[0].(map[string]any)
	native["sources"] = []any{source, syncJSONClone(source)}
	data, _ := json.Marshal(native)
	status, err := decodeBucketSyncStatus(data, want)
	if err != nil || !status.CaughtUp || status.Shards != 1 {
		t.Fatalf("native shared-endpoint multi-pipe checkpoint rejected: %+v %v", status, err)
	}
	other := native["sources"].([]any)[1].(map[string]any)
	other["behind_shards"] = []any{map[string]any{"shard_id": 0, "shard_marker": "redacted-marker"}}
	data, _ = json.Marshal(native)
	status, err = decodeBucketSyncStatus(data, want)
	if err != nil || status.CaughtUp || status.BehindShards != 1 {
		t.Fatal("one lagging pipe comparison was ignored")
	}
	other["error"] = "failed remote comparison secret-marker"
	data, _ = json.Marshal(native)
	status, err = decodeBucketSyncStatus(data, want)
	if err != nil || status.CaughtUp || status.State != "error" {
		t.Fatal("one remote error hid behind another ready pipe")
	}
	delete(other, "error")
	other["source_bucket_id"] = "foreign"
	data, _ = json.Marshal(native)
	if status, err = decodeBucketSyncStatus(data, want); err == nil || status.CaughtUp {
		t.Fatal("another ready comparison hid a foreign source instance")
	}
}

func TestRGWBucketCheckpointRefusesOldNonMasterSourcePeriod(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "reverse", SourceZones: []string{"destination"}, DestinationZones: []string{"source"}}); err != nil {
		t.Fatal(err)
	}
	stale := syncJSONClone(cli.current)
	stale["id"] = "old-source-period"
	cli.periodByZone = map[string]map[string]any{"dest-id": stale}
	before := len(cli.calls)
	status, err := f.BucketSyncStatus(t.Context(), g, "reverse", "destination", "source")
	if err == nil || status.CaughtUp {
		t.Fatal("old source period accepted under current destination policy")
	}
	for _, args := range cli.calls[before:] {
		if syncTestCommand(args, "bucket", "sync", "status") {
			t.Fatal("old source endpoint reached native bucket comparison")
		}
	}
}

func TestRGWBucketCheckpointCancellationDuringReadyNativeRead(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cli.cancelOnBucketStatus = cancel
	status, err := f.WaitBucketSyncReady(ctx, g, "selected", "source", "destination")
	if !errors.Is(err, context.Canceled) || status.CaughtUp {
		t.Fatal("ready native response completed after cancellation")
	}
}

func TestRGWSyncObserversBoundMutexQueueTime(t *testing.T) {
	f, cli, g := newRGWSyncBucketStatusFixture(t)
	f.topologyMu.Lock()
	defer f.topologyMu.Unlock()
	before := len(cli.calls)
	for _, observe := range []func(context.Context) error{
		func(ctx context.Context) error { _, err := f.SyncStatus(ctx, "destination"); return err },
		func(ctx context.Context) error { _, err := f.WaitSyncReady(ctx, "destination", "source"); return err },
		func(ctx context.Context) error {
			_, err := f.BucketSyncStatus(ctx, g, "selected", "source", "destination")
			return err
		},
		func(ctx context.Context) error {
			_, err := f.WaitBucketSyncReady(ctx, g, "selected", "source", "destination")
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		started := time.Now()
		err := observe(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second || len(cli.calls) != before {
			t.Fatal("observer deadline excluded time queued behind topology lock")
		}
	}
}

func TestRGWBucketCheckpointRechecksCommittedPolicyOnEveryPoll(t *testing.T) {
	f, cli, _, _ := newRGWSyncTestFixture()
	cli.additionalBuckets = map[string]string{"input": "source-instance", "output": "destination-instance"}
	g, err := f.CreateSyncGroup(t.Context(), RGWSyncPolicyScope{}, RGWSyncGroupConfig{ID: "published", Status: RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CreateSyncPipe(t.Context(), g, RGWSyncPipeConfig{ID: "selected", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, SourceBucket: &RGWSyncBucketSelector{Name: "input"}, DestinationBucket: &RGWSyncBucketSelector{Name: "output"}}); err != nil {
		t.Fatal(err)
	}
	if err := f.ApplySyncGroup(t.Context(), g); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := f.syncBucketCheckpoint(t.Context(), g, "selected", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	currentGroup := syncPeriodGroups(cli.current)[0]
	currentGroup["sync_policy"] = syncPolicyReplace(currentGroup["sync_policy"].(map[string]any), g.ID(), nil)
	before := len(cli.calls)
	status, err := f.syncBucketStatus(t.Context(), checkpoint)
	if err == nil || status.CaughtUp {
		t.Fatal("another imported committed policy substituted a different pipe's checkpoint")
	}
	for _, args := range cli.calls[before:] {
		if syncTestCommand(args, "bucket", "sync", "status") {
			t.Fatal("unpublished captured pipe reached native comparison on retry")
		}
	}
}

func TestRGWBucketCheckpointRequiresCommittedPeriodIdentity(t *testing.T) {
	for _, change := range []func(map[string]any){
		func(period map[string]any) { period["id"] = "" },
		func(period map[string]any) { period["id"] = "realm:staging" },
		func(period map[string]any) { period["realm_epoch"] = json.Number("0") },
	} {
		f, cli, g := newRGWSyncBucketStatusFixture(t)
		change(cli.current)
		before := len(cli.calls)
		status, err := f.BucketSyncStatus(t.Context(), g, "selected", "source", "destination")
		if err == nil || status.CaughtUp {
			t.Fatal("malformed or staging period accepted as a committed bucket checkpoint")
		}
		for _, args := range cli.calls[before:] {
			if syncTestCommand(args, "bucket", "sync", "status") {
				t.Fatal("malformed period reached native bucket comparison")
			}
		}
	}
}
