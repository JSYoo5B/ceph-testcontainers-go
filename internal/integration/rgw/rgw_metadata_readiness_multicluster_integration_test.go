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
		Timing: multisite.TimingProfile{SyncLeasePeriod: 5 * time.Minute},
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
	target := rgwTopologyS3(t, ctx, find("b"))
	source := rgwTopologyS3(t, ctx, find("c"))
	const bucket = "/tc-rgw-metadata-scope"
	createMultisiteBucket(t, ctx, fixture, master, bucket, nil)
	before, err := fixture.WaitSyncReady(ctx, "b", "c")
	if err != nil {
		t.Fatal(err)
	}
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

	metadataCtx, metadataCancel := context.WithTimeout(ctx, 75*time.Second)
	during, err := fixture.WaitSyncReady(metadataCtx, "b")
	metadataCancel()
	if err != nil || !during.Metadata.CaughtUp || during.PeriodID != before.PeriodID || during.RealmEpoch != before.RealmEpoch {
		t.Fatalf("metadata readiness with only the data peer cut: %+v, %v", during, err)
	}
	index := slices.IndexFunc(during.DataSources, func(s multisite.DataSyncStatus) bool { return s.SourceZone == "c" })
	if index < 0 || during.DataSources[index].CaughtUp {
		t.Fatalf("isolated data source comparison was accepted: %+v", during.DataSources)
	}
	t.Logf("metadata remained ready while source c comparison was unavailable: %+v", during.DataSources[index])
	dataCtx, dataCancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = fixture.WaitSyncReady(dataCtx, "b", "c")
	dataCancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("selected isolated source satisfied readiness: %v", err)
	}
	if err := cut.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := fixture.WaitSyncReady(ctx, "b", "c")
	if err != nil || !after.Metadata.CaughtUp || after.PeriodID != before.PeriodID || after.RealmEpoch != before.RealmEpoch {
		t.Fatalf("restored source readiness: %+v, %v", after, err)
	}
	waitMultisiteObject(t, ctx, target, bucket+"/isolated", http.StatusOK, payload)
	t.Log("metadata-only readiness passed during the third-zone peer cut; selected-source readiness failed, then recovered with byte-exact object replication")
}
