//go:build all || (integration && features && (!ci || (ci_short && (!ci_batch || ci_batch_cluster_fixtures))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func TestRGWPlacementStorageClasses(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWPlacementStorageClasses(t)
}

func TestHostNetworkRGWPlacementStorageClasses(t *testing.T) {
	parallelWhenEnabled(t)
	testRGWPlacementStorageClasses(t, ceph.WithHostNetwork())
}

func TestRGWPlacementRealmStorageClasses(t *testing.T) {
	parallelWhenEnabled(t)
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	image, options := integrationImages(t)
	options = append(options, ceph.WithOSDCount(1))
	clusters := make([]*ceph.Container, 2)
	for i := range clusters {
		cluster, err := ceph.Run(ctx, image, options...)
		if cluster != nil {
			testcontainers.CleanupContainer(t, cluster)
		}
		if err != nil {
			t.Fatal(err)
		}
		clusters[i] = cluster
		for _, name := range []string{"tc-realm-index", "tc-realm-extra", "tc-realm-standard", "tc-realm-ia"} {
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: name, PGNum: 1, Application: "rgw"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := cluster.WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = image
	}
	fixture, err := multicluster.RunRGWMultisite(ctx, rgwImage, multicluster.RGWMultisiteConfig{Source: clusters[0], Destination: clusters[1], ControlImage: image})
	if fixture != nil {
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			if err := fixture.Terminate(cleanup); err != nil {
				t.Error(err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		diagnosticCtx, diagnosticCancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer diagnosticCancel()
		for _, site := range []struct {
			name    string
			gateway *ceph.RGWContainer
		}{{"source", fixture.Source}, {"destination", fixture.Destination}} {
			for _, command := range [][]string{{"sync", "status"}, {"metadata", "sync", "status"}, {"data", "sync", "status", "--source-zone", fixture.SourceZoneID}, {"sync", "error", "list"}} {
				data, err := site.gateway.Admin(diagnosticCtx, command...)
				if err != nil {
					t.Logf("%s native %s diagnostic: %v", site.name, strings.Join(command[:min(3, len(command))], " "), err)
					continue
				}
				t.Logf("%s native %s diagnostic: %s", site.name, strings.Join(command[:min(3, len(command))], " "), rgwPlacementSafeDiagnostic(data, site.gateway))
			}
			logs, err := site.gateway.Logs(diagnosticCtx)
			if err == nil {
				data, readErr := io.ReadAll(io.LimitReader(logs, 2<<20))
				_ = logs.Close()
				if readErr == nil {
					t.Logf("%s recent RGW diagnostics: %s", site.name, rgwPlacementSafeLogDiagnostic(data, site.gateway))
				}
			}
		}
	})
	config := ceph.RGWPlacementConfig{Name: "tc-realm-tiered", IndexPool: "tc-realm-index", DataExtraPool: "tc-realm-extra", StorageClasses: []ceph.RGWStorageClassConfig{{Name: "STANDARD", DataPool: "tc-realm-standard"}, {Name: "STANDARD_IA", DataPool: "tc-realm-ia"}}}
	sourcePlacement, err := fixture.Source.CreatePlacement(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	destinationPlacement, err := fixture.Destination.CreatePlacement(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	if sourcePlacement.RealmID == "" || sourcePlacement.RealmID != destinationPlacement.RealmID || sourcePlacement.ZonegroupID != destinationPlacement.ZonegroupID || sourcePlacement.ZoneID == destinationPlacement.ZoneID {
		t.Fatal("multisite placement scopes are not exact native identities")
	}
	if err := fixture.Destination.ApplyPlacement(ctx, destinationPlacement); err == nil {
		t.Fatal("secondary zone published a realm placement")
	}
	// Native period update can stage a change without publishing it. Restore
	// the zonegroup object afterward so only the staging period differs; Apply
	// must inspect that pending policy before its own period update overwrites it.
	groupData, err := fixture.Source.Admin(ctx, "zonegroup", "get")
	var nativeGroup struct {
		APIName string `json:"api_name"`
	}
	if err != nil || json.Unmarshal(groupData, &nativeGroup) != nil || nativeGroup.APIName == "" {
		t.Fatal("cannot inspect native realm zonegroup API name")
	}
	if _, err := fixture.Source.Admin(ctx, "zonegroup", "modify", "--api-name", nativeGroup.APIName+"-pending"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Source.Admin(ctx, "period", "update"); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.Source.Admin(ctx, "zonegroup", "modify", "--api-name", nativeGroup.APIName); err != nil {
		t.Fatal(err)
	}
	stagingArgs := []string{"period", "get", "--period", sourcePlacement.RealmID + ":staging", "--epoch", "1"}
	stagedBefore, err := fixture.Source.Admin(ctx, stagingArgs...)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.Source.ApplyPlacement(ctx, sourcePlacement); err == nil || !strings.Contains(err.Error(), "existing staging period") {
		t.Fatalf("unrelated staged policy was not refused before overwrite: %v", err)
	}
	stagedAfter, err := fixture.Source.Admin(ctx, stagingArgs...)
	var before, after any
	if err != nil || json.Unmarshal(stagedBefore, &before) != nil || json.Unmarshal(stagedAfter, &after) != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("refused native staging period was changed")
	}
	// The test explicitly restages the restored zonegroup before permitting
	// publication; only the matching placement target now differs from current.
	if _, err := fixture.Source.Admin(ctx, "period", "update"); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Source.ApplyPlacement(ctx, sourcePlacement); err != nil {
		t.Fatal(err)
	}
	if err := fixture.PullDestinationPeriod(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fixture.Destination.ReloadPlacement(ctx, destinationPlacement); err != nil {
		t.Fatal(err)
	}
	for _, option := range []string{"rgw_sync_lease_period", "rgw_meta_sync_poll_interval", "rgw_data_sync_poll_interval"} {
		value, err := clusters[1].Ceph(ctx, "config", "get", "client.admin", option)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("native destination central config %s=%s (seconds)", option, strings.TrimSpace(string(value)))
	}
	endpoints := make([]string, 2)
	for i, gateway := range []*ceph.RGWContainer{fixture.Source, fixture.Destination} {
		endpoints[i], err = gateway.S3Endpoint(ctx)
		if err != nil {
			t.Fatal(err)
		}
	}
	clients := []s3HTTPClient{{endpoint: endpoints[0], accessKey: fixture.Source.AccessKey, secretKey: fixture.Source.SecretKey, region: fixture.Source.Region, http: &http.Client{Timeout: 30 * time.Second}}, {endpoint: endpoints[1], accessKey: fixture.Source.AccessKey, secretKey: fixture.Source.SecretKey, region: fixture.Destination.Region, http: &http.Client{Timeout: 30 * time.Second}}}
	const bucket = "/tc-realm-placement-bucket"
	body := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + sourcePlacement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	rgwPlacementWaitCreateBucket(t, ctx, clients[0], bucket, body)
	payload := bytes.Repeat([]byte("realm-selected-storage-class\n"), 4096)
	rgwPlacementRequest(t, ctx, clients[0], http.MethodPut, bucket+"/replicated", payload, "STANDARD_IA", http.StatusOK)
	rgwPlacementWaitReplica(t, ctx, clients[1], bucket+"/replicated", payload)
	var replicaListing struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(clients[1].request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &replicaListing); err != nil || len(replicaListing.Contents) != 1 || replicaListing.Contents[0].Key != "replicated" || replicaListing.Contents[0].StorageClass != "STANDARD_IA" {
		t.Fatalf("replica did not retain STANDARD_IA: %+v error=%v", replicaListing, err)
	}
	rgwPlacementPoolPayload(t, ctx, clusters[1], "tc-realm-ia", "replicated STANDARD_IA", payload)
	for _, gateway := range []*ceph.RGWContainer{fixture.Source, fixture.Destination} {
		state, err := gateway.PlacementStatus(ctx, map[*ceph.RGWContainer]*ceph.RGWPlacement{fixture.Source: sourcePlacement, fixture.Destination: destinationPlacement}[gateway])
		if err != nil || state.DefaultPlacement != "default-placement" {
			t.Fatal("realm publication changed native default or lost zone-local mapping")
		}
	}
	clients[0].request(t, ctx, http.MethodDelete, bucket+"/replicated", nil, http.StatusNoContent)
	clients[0].request(t, ctx, http.MethodDelete, bucket, nil, http.StatusNoContent)
	t.Log("matching zone-local placement mappings published through metadata-master guarded period commit; secondary publication refused, named bucket STANDARD_IA object replicated across native zones")
}
