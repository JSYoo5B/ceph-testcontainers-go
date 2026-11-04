//go:build all || (integration && multicluster && features)

package rgw_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/integration/harness"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

const syncWatchBucket = "tc-sync-watch"

// refreshSyncWatchEndpoints reads both gateways' endpoints again: applying a
// zonegroup policy reloads the gateways, and a restarted gateway's mapped port
// can change.
func refreshSyncWatchEndpoints(t *testing.T, ctx context.Context, link *multisite.Fixture, source, destination *harness.S3HTTPClient) {
	t.Helper()
	var err error
	if source.Endpoint, err = link.Source.S3Endpoint(ctx); err != nil {
		t.Fatal(err)
	}
	if destination.Endpoint, err = link.Destination.S3Endpoint(ctx); err != nil {
		t.Fatal(err)
	}
}

// syncWatchObjects is the number of objects written while the destination
// gateway is stopped: enough to leave more than one data log shard behind.
const syncWatchObjects = 24

func syncWatchPayload(i int) []byte {
	return bytes.Repeat([]byte(fmt.Sprintf("sync watch object %02d\n", i)), 512)
}

// waitSyncEvent receives zone watch events until accept takes one, failing
// when the watch ends or the bound passes.
func waitSyncEvent(t *testing.T, w *multisite.SyncWatch, what string, bound time.Duration, accept func(multisite.SyncWatchEvent) bool) multisite.SyncWatchEvent {
	t.Helper()
	deadline := time.After(bound)
	for {
		select {
		case e, ok := <-w.Events():
			if !ok {
				t.Fatalf("zone watch ended before %s: %v", what, w.Stop())
			}
			t.Logf("zone watch event %d %s ready=%t metadata=%+v data=%+v err=%v", e.Sequence, e.Kind, e.Ready, e.Status.Metadata, e.Status.DataSources, e.Err)
			if accept(e) {
				return e
			}
		case <-deadline:
			t.Fatalf("zone watch: no %s within %s", what, bound)
		}
	}
}

// waitBucketEvent is waitSyncEvent for the bucket watch.
func waitBucketEvent(t *testing.T, w *multisite.BucketSyncWatch, what string, bound time.Duration, accept func(multisite.BucketSyncWatchEvent) bool) multisite.BucketSyncWatchEvent {
	t.Helper()
	deadline := time.After(bound)
	for {
		select {
		case e, ok := <-w.Events():
			if !ok {
				t.Fatalf("bucket watch ended before %s: %v", what, w.Stop())
			}
			t.Logf("bucket watch event %d %s state=%s caught_up=%t shards=%d behind=%d source=%s dest=%s err=%v", e.Sequence, e.Kind, e.Status.State, e.Status.CaughtUp, e.Status.Shards, e.Status.BehindShards, e.Status.SourceBucket.ID, e.Status.DestinationBucket.ID, e.Err)
			if accept(e) {
				return e
			}
		case <-deadline:
			t.Fatalf("bucket watch: no %s within %s", what, bound)
		}
	}
}

func testRGWSyncWatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Minute)
	defer cancel()
	link, sourceS3, destinationS3 := newRGWScenario(t, ctx, "tc-sync-watch")
	global, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{}, multisite.SyncGroupConfig{ID: "watch-allowed", Status: multisite.SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := global.CreateFlow(ctx, multisite.SyncFlowConfig{ID: "watch-pair", Zones: []string{"source", "destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := global.CreatePipe(ctx, multisite.SyncPipeConfig{ID: "permission", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := global.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	refreshSyncWatchEndpoints(t, ctx, link, &sourceS3, &destinationS3)
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	sourceS3.Request(t, ctx, http.MethodPut, "/"+syncWatchBucket, nil, http.StatusOK)
	bucket, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{Bucket: syncWatchBucket}, multisite.SyncGroupConfig{ID: "watched", Status: multisite.SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.CreatePipe(ctx, multisite.SyncPipeConfig{ID: "all", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := bucket.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	refreshSyncWatchEndpoints(t, ctx, link, &sourceS3, &destinationS3)
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	seed := syncWatchPayload(-1)
	sourceS3.Request(t, ctx, http.MethodPut, "/"+syncWatchBucket+"/seed", seed, http.StatusOK)
	waitOwnedSyncObject(t, ctx, destinationS3, "/"+syncWatchBucket+"/seed", http.StatusOK, seed)
	waitOwnedBucketCheckpoint(t, ctx, link, bucket, "all")
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}

	cfg := ceph.WatchConfig{Interval: 2 * time.Second, Buffer: 256}
	zone, err := link.WatchSync(ctx, cfg, "destination", "source")
	if err != nil {
		t.Fatal(err)
	}
	// Deferred, not a cleanup: the watch must stop before ctx is cancelled,
	// or Stop reports the cancellation as its cause.
	defer func() {
		if err := zone.Stop(); err != nil {
			t.Errorf("zone watch ended with %v", err)
		}
	}()
	checkpoint, err := bucket.WatchBucketSync(ctx, cfg, "all", "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	stopBucket := true
	defer func() {
		if stopBucket {
			if err := checkpoint.Stop(); err != nil {
				t.Errorf("bucket watch ended with %v", err)
			}
		}
	}()
	baseline := waitSyncEvent(t, zone, "baseline", time.Second, func(e multisite.SyncWatchEvent) bool { return true })
	if baseline.Kind != ceph.WatchBaseline || !baseline.Ready {
		t.Fatalf("zone baseline %+v", baseline)
	}
	bucketBaseline := waitBucketEvent(t, checkpoint, "baseline", time.Second, func(e multisite.BucketSyncWatchEvent) bool { return true })
	if bucketBaseline.Kind != ceph.WatchBaseline || !bucketBaseline.Status.CaughtUp {
		t.Fatalf("bucket baseline %+v", bucketBaseline)
	}

	// Another topology observation takes the lock while both watches run.
	for range 3 {
		started := time.Now()
		if _, err := link.SyncStatus(ctx, "source"); err != nil {
			t.Fatal(err)
		}
		t.Logf("SyncStatus beside both watches took %s", time.Since(started).Round(time.Millisecond))
	}

	fenceRGWScenarioGateway(t, ctx, link.Destination)
	for i := range syncWatchObjects {
		sourceS3.Request(t, ctx, http.MethodPut, fmt.Sprintf("/%s/held-%02d", syncWatchBucket, i), syncWatchPayload(i), http.StatusOK)
	}
	lag := waitSyncEvent(t, zone, "data lag", 4*time.Minute, func(e multisite.SyncWatchEvent) bool {
		return e.Err == nil && !e.Ready && len(e.Status.DataSources) == 1 && e.Status.DataSources[0].BehindShards > 0
	})
	bucketLag := waitBucketEvent(t, checkpoint, "bucket lag", 4*time.Minute, func(e multisite.BucketSyncWatchEvent) bool {
		return e.Err == nil && !e.Status.CaughtUp && e.Status.BehindShards > 0
	})
	t.Logf("SYNC_WATCH_LAG zone_event=%d behind_shards=%d bucket_event=%d bucket_behind_shards=%d", lag.Sequence, lag.Status.DataSources[0].BehindShards, bucketLag.Sequence, bucketLag.Status.BehindShards)

	if err := link.Destination.Start(ctx); err != nil {
		t.Fatal(err)
	}
	refreshSyncWatchEndpoints(t, ctx, link, &sourceS3, &destinationS3)
	ready := waitSyncEvent(t, zone, "catch-up", 8*time.Minute, func(e multisite.SyncWatchEvent) bool { return e.Ready })
	caught := waitBucketEvent(t, checkpoint, "bucket catch-up", 8*time.Minute, func(e multisite.BucketSyncWatchEvent) bool {
		return e.Err == nil && e.Status.CaughtUp
	})
	if caught.Status.SourceBucket != bucketBaseline.Status.SourceBucket || caught.Status.DestinationBucket != bucketBaseline.Status.DestinationBucket || ready.Status.PeriodID != baseline.Status.PeriodID {
		t.Fatalf("the watches changed identity: %+v %+v", caught.Status, ready.Status)
	}
	for i := range syncWatchObjects {
		waitOwnedSyncObject(t, ctx, destinationS3, fmt.Sprintf("/%s/held-%02d", syncWatchBucket, i), http.StatusOK, syncWatchPayload(i))
	}
	t.Logf("SYNC_WATCH_CAUGHT_UP zone_event=%d bucket_event=%d objects=%d exact", ready.Sequence, caught.Sequence, syncWatchObjects)

	// Recreating the bucket under the same name never reads as progress.
	for _, key := range append([]string{"seed"}, func() []string {
		var keys []string
		for i := range syncWatchObjects {
			keys = append(keys, fmt.Sprintf("held-%02d", i))
		}
		return keys
	}()...) {
		sourceS3.Request(t, ctx, http.MethodDelete, "/"+syncWatchBucket+"/"+key, nil, http.StatusNoContent)
	}
	sourceS3.Request(t, ctx, http.MethodDelete, "/"+syncWatchBucket, nil, http.StatusNoContent)
	waitRGWScenarioResponse(t, ctx, sourceS3, http.MethodPut, "/"+syncWatchBucket, http.StatusOK)
	deadline := time.After(4 * time.Minute)
	for ended := false; !ended; {
		select {
		case e, ok := <-checkpoint.Events():
			if !ok {
				ended = true
				break
			}
			if e.Err == nil && e.Kind != ceph.WatchChanged {
				t.Fatalf("the recreated bucket read as an observation: %+v", e)
			}
			t.Logf("bucket watch event %d %s err=%v", e.Sequence, e.Kind, e.Err)
		case <-deadline:
			t.Fatal("the bucket watch did not end after the bucket was recreated")
		}
	}
	stopBucket = false
	if err := checkpoint.Stop(); err == nil || !strings.Contains(err.Error(), "recreated") {
		t.Fatalf("the bucket watch ended with %v", err)
	}
	t.Logf("SYNC_WATCH_RECREATED bucket watch ended: %v", checkpoint.Stop())
}
