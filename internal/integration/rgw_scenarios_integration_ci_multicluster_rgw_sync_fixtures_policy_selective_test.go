//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_rgw_sync_fixtures_policy_selective))))

//ci: timeout=40m job-timeout=50

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-testcontainers-go/rgw"
)

// Bucket metadata still replicates globally, but this policy enables object
// data only for one bucket/prefix and only from source to destination.
func TestMultiClusterRGWSelectivePolicy(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 14*time.Minute)
	defer cancel()
	link, sourceS3, destinationS3 := newRGWScenario(t, ctx, "tc-selective-policy")
	permission, err := link.CreateSyncGroup(ctx, rgw.SyncPolicyScope{}, rgw.SyncGroupConfig{ID: "selective", Status: rgw.SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncFlow(ctx, permission, rgw.SyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}); err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncPipe(ctx, permission, rgw.SyncPipeConfig{ID: "bucket-allowance", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	// Publish only this owned policy and reload both gateways before writes,
	// so the original all-bucket policy cannot race this scenario's data.
	if err := link.ApplySyncGroup(ctx, permission); err != nil {
		t.Fatal(err)
	}
	sourceS3.endpoint, err = link.Source.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationS3.endpoint, err = link.Destination.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioResponse(t, ctx, sourceS3, http.MethodGet, "/", http.StatusOK)
	waitRGWScenarioResponse(t, ctx, destinationS3, http.MethodGet, "/", http.StatusOK)
	const selected, localOnly = "/tc-policy-selected", "/tc-policy-local-only"
	sourceS3.request(t, ctx, http.MethodPut, selected, nil, http.StatusOK)
	sourceS3.request(t, ctx, http.MethodPut, localOnly, nil, http.StatusOK)
	bucket, err := link.CreateSyncGroup(ctx, rgw.SyncPolicyScope{Bucket: strings.TrimPrefix(selected, "/")}, rgw.SyncGroupConfig{ID: "selected-prefix", Status: rgw.SyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncPipe(ctx, bucket, rgw.SyncPipeConfig{ID: "prefix", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, Prefix: "published/"}); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	payload := bytes.Repeat([]byte("selected RGW policy object\n"), 1024)
	for _, path := range []string{selected + "/published/selected", selected + "/private/excluded", localOnly + "/published/excluded"} {
		sourceS3.request(t, ctx, http.MethodPut, path, payload, http.StatusOK)
		if got := sourceS3.request(t, ctx, http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(got, payload) {
			t.Fatalf("source fixture changed at %s", path)
		}
	}
	waitMultisiteObject(t, ctx, destinationS3, selected+"/published/selected", http.StatusOK, payload)
	waitRGWScenarioResponse(t, ctx, destinationS3, http.MethodHead, localOnly, http.StatusOK)
	// A secondary object write is valid, but the directional flow excludes
	// its return path. Test that independently from prefix/bucket selection.
	destinationS3.request(t, ctx, http.MethodPut, selected+"/published/reverse-excluded", payload, http.StatusOK)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second,
		rgwAbsentObject{destinationS3, selected + "/private/excluded"},
		rgwAbsentObject{destinationS3, localOnly + "/published/excluded"},
		rgwAbsentObject{sourceS3, selected + "/published/reverse-excluded"},
	)
	t.Log("selective policy: enabled bucket/prefix replicated; another prefix, another bucket and the reverse direction stayed absent during a 35-second observation window")

	// Bucket policy changes are dynamic and do not need a period commit.
	// Verify new writes after the change, without asserting historical
	// backfill or deletion of objects previously selected by the old prefix.
	if err := link.SetSyncPipePrefix(ctx, bucket, "prefix", "reports/"); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	sourceS3.request(t, ctx, http.MethodPut, selected+"/reports/after-policy-change", payload, http.StatusOK)
	sourceS3.request(t, ctx, http.MethodPut, selected+"/published/after-policy-change", payload, http.StatusOK)
	waitMultisiteObject(t, ctx, destinationS3, selected+"/reports/after-policy-change", http.StatusOK, payload)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destinationS3, selected + "/published/after-policy-change"})
	sourceS3.request(t, ctx, http.MethodDelete, selected+"/reports/after-policy-change", nil, http.StatusNoContent)
	waitMultisiteObject(t, ctx, destinationS3, selected+"/reports/after-policy-change", http.StatusNotFound, nil)
	if got := destinationS3.request(t, ctx, http.MethodGet, selected+"/published/selected", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("changing a policy unexpectedly changed the already replicated object")
	}
	t.Log("live prefix update selected the new reports/ write, excluded a new published/ write, propagated a selected deletion and retained the old copied object")
}
