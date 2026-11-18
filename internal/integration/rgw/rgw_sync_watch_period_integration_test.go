//go:build all || (integration && multicluster && features && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_owned_bridge))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

// An unrelated policy commit advances a period's epoch with the same master.
// Both watches must end even though their bucket and selected policy survive.
func TestMultiClusterRGWSyncWatchSamePeriodCommit(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	link, source, destination := newRGWScenario(t, ctx, "tc-watch-period")
	global, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{}, multisite.SyncGroupConfig{ID: "permission", Status: multisite.SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := global.CreateFlow(ctx, multisite.SyncFlowConfig{ID: "pair", Zones: []string{"source", "destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := global.CreatePipe(ctx, multisite.SyncPipeConfig{ID: "all", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := global.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	refreshSyncWatchEndpoints(t, ctx, link, &source, &destination)
	const bucket = "tc-watch-period-bucket"
	createMultisiteBucket(t, ctx, link, source, "/"+bucket, nil)
	group, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: bucket}, multisite.SyncGroupConfig{ID: "watched", Status: multisite.SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := group.CreatePipe(ctx, multisite.SyncPipeConfig{ID: "all", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := group.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, group)
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	type periodVersion struct {
		ID         string `json:"id"`
		Epoch      uint64 `json:"epoch"`
		RealmEpoch uint64 `json:"realm_epoch"`
		Master     string `json:"master_zone"`
	}
	readPeriod := func() periodVersion {
		t.Helper()
		data, err := link.ZoneAdmin(ctx, "source", "period", "get", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var period periodVersion
		if err := json.Unmarshal(data, &period); err != nil || period.ID == "" || period.Epoch == 0 || period.Master == "" {
			t.Fatalf("period version %+v: %v", period, err)
		}
		return period
	}
	before := readPeriod()
	cfg := ceph.WatchConfig{Interval: time.Second, QueryTimeout: time.Minute}
	zoneWatch, err := link.WatchSync(ctx, cfg, "destination", "source")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = zoneWatch.Stop() }()
	bucketWatch, err := group.WatchBucketSync(ctx, cfg, "all", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bucketWatch.Stop() }()
	<-zoneWatch.Events()
	<-bucketWatch.Events()
	other, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{}, multisite.SyncGroupConfig{ID: "unrelated", Status: multisite.SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	after := readPeriod()
	if after.ID != before.ID || after.RealmEpoch != before.RealmEpoch || after.Master != before.Master || after.Epoch <= before.Epoch {
		t.Fatalf("commit did not retain its period and master while advancing epoch: before=%+v after=%+v", before, after)
	}
	zoneEvents, bucketEvents := zoneWatch.Events(), bucketWatch.Events()
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	for zoneEvents != nil || bucketEvents != nil {
		select {
		case _, ok := <-zoneEvents:
			if !ok {
				zoneEvents = nil
			}
		case _, ok := <-bucketEvents:
			if !ok {
				bucketEvents = nil
			}
		case <-timer.C:
			t.Fatal("watches kept running after the same-master commit")
		}
	}
	for name, stop := range map[string]func() error{"zone": zoneWatch.Stop, "bucket": bucketWatch.Stop} {
		if err := stop(); err == nil || !strings.Contains(err.Error(), "committed period changed") {
			t.Fatalf("%s watch ended with %v", name, err)
		}
	}
	refreshSyncWatchEndpoints(t, ctx, link, &source, &destination)
	source.Request(t, ctx, http.MethodHead, "/"+bucket, nil, http.StatusOK)
	t.Logf("same-master period %s epoch %d -> %d ended both watches", before.ID, before.Epoch, after.Epoch)
	checkNativeReloadRecovery(t, ctx, link, other)
}
