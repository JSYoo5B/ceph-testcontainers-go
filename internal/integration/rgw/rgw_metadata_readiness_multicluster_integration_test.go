//go:build all || (integration && multicluster && (!ci || (ci_topology && (!ci_batch || ci_batch_topology_extensions_rgw_peer_network))))

//ci: timeout=40m job-timeout=50 image-roles=essential,client,rgw

package rgw_test

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
)

// The target retains its metadata-master link while another data source loses
// its peer bridge. Metadata readiness and selected-source readiness must then
// disagree, and both must recover without changing the period.
func TestMultiClusterRGWMetadataReadinessDuringDataPeerCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	_, image, clusters := rgwTopologyClusters(t, ctx, 3, false)
	fixture, err := multisite.Run(ctx, image, multisite.WithTopology(multisite.TopologyConfig{
		Zonegroup: "us", MetadataMaster: "a",
		Zones: []multisite.ZoneConfig{
			{Name: "a", Cluster: clusters[0]},
			{Name: "b", Cluster: clusters[1]},
			{Name: "c", Cluster: clusters[2]},
		},
	}))
	rgwTopologyCleanup(t, fixture)
	if err != nil {
		t.Fatal(err)
	}
	zones := fixture.Zones()
	find := func(name string) multisite.Zone {
		t.Helper()
		i := slices.IndexFunc(zones, func(z multisite.Zone) bool { return z.Name == name })
		if i < 0 {
			t.Fatalf("zone %s is missing", name)
		}
		return zones[i]
	}
	master := rgwTopologyS3(t, ctx, find("a"))
	targetZone := find("b")
	target := rgwTopologyS3(t, ctx, targetZone)
	source := rgwTopologyS3(t, ctx, find("c"))
	const bucket = "/tc-rgw-metadata-scope"
	master.Request(t, ctx, http.MethodPut, bucket, nil, http.StatusOK)
	for _, zone := range zones {
		if _, err := fixture.WaitSyncReady(ctx, zone.Name); err != nil {
			t.Fatalf("initial metadata convergence in %s: %v", zone.Name, err)
		}
	}
	target.Request(t, ctx, http.MethodHead, bucket, nil, http.StatusOK)
	source.Request(t, ctx, http.MethodHead, bucket, nil, http.StatusOK)
	before, err := fixture.WaitSyncReady(ctx, "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("baseline ready; interrupting only zone c's peer link")
	cut, err := fixture.InterruptZoneLink(ctx, "c")
	if cut != nil {
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()
			if err := cut.Restore(cleanup); err != nil {
				t.Errorf("restore data peer: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("data written in the isolated third zone")
	source.Request(t, ctx, http.MethodPut, bucket+"/isolated", payload, http.StatusOK)
	target.Request(t, ctx, http.MethodGet, bucket+"/isolated", nil, http.StatusNotFound)

	t.Log("checking metadata-only readiness while zone c is isolated")
	metadataCtx, metadataCancel := context.WithTimeout(ctx, 75*time.Second)
	during, err := fixture.WaitSyncReady(metadataCtx, "b")
	metadataCancel()
	if err != nil || !during.Metadata.CaughtUp || during.PeriodID != before.PeriodID || during.RealmEpoch != before.RealmEpoch {
		t.Fatalf("metadata readiness with only the data peer cut: %+v, %v", during, err)
	}
	if len(during.DataSources) != 0 {
		t.Fatalf("metadata-only readiness returned data-source observations: %+v", during.DataSources)
	}
	t.Log("metadata-only readiness remained ready with the third zone isolated")
	t.Log("checking selected-source readiness while zone c is isolated")
	dataCtx, dataCancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = fixture.WaitSyncReady(dataCtx, "b", "c")
	dataCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("selected isolated source satisfied readiness: %v", err)
	}
	// Stop the consumer of fresh metadata without disconnecting its CLI client
	// from the master. A complete comparison now has to report real lag.
	grace := 5 * time.Second
	if err := targetZone.Gateway.Stop(ctx, &grace); err != nil {
		t.Fatal(err)
	}
	const fresh = "/tc-rgw-metadata-scope-fresh"
	master.Request(t, ctx, http.MethodPut, fresh, nil, http.StatusOK)
	staleCtx, staleCancel := context.WithTimeout(ctx, 8*time.Second)
	stale, err := fixture.WaitSyncReady(staleCtx, "b")
	staleCancel()
	if !errors.Is(err, context.DeadlineExceeded) || stale.PeriodID != before.PeriodID ||
		stale.Metadata.PeriodID != before.PeriodID || stale.Metadata.IncrementalShards != stale.Metadata.Shards ||
		stale.Metadata.Shards == 0 || stale.Metadata.CaughtUp {
		t.Fatalf("metadata-only readiness did not retain the complete behind observation: %+v, %v", stale, err)
	}
	t.Log("metadata-only readiness reported genuine lag after a new master metadata write")
	if err := targetZone.Gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Log("restoring zone c and waiting for selected-source convergence")
	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.WaitSyncReady(ctx, "b", "c")
	if err != nil || !after.Metadata.CaughtUp || after.PeriodID != before.PeriodID || after.RealmEpoch != before.RealmEpoch {
		t.Fatalf("restored source readiness: %+v, %v", after, err)
	}
	target = rgwTopologyS3(t, ctx, targetZone)
	target.Request(t, ctx, http.MethodHead, fresh, nil, http.StatusOK)
	waitMultisiteObject(t, ctx, target, bucket+"/isolated", http.StatusOK, payload)
	t.Log("metadata-only readiness passed during the third-zone peer cut; selected-source readiness failed, then recovered with byte-exact object replication")
}
