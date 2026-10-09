//go:build all || (integration && multicluster && (!ci || (ci_multicluster && (!ci_batch || ci_batch_multicluster_topology_rgw_master_failover))))

//ci: timeout=60m job-timeout=70

package integration_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"
)

// This is a quiescent planned metadata-master change. It fences the current
// gateway, verifies metadata catch-up, and explicitly recovers the old site
// with a period pull before performing the same sequence for failback.
func TestMultiClusterRGWMetadataMasterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	link, sourceS3, destinationS3 := newRGWScenario(t, ctx, "tc-metadata-failover")
	const originalBucket = "/tc-before-master-change"
	payload := bytes.Repeat([]byte("RGW planned metadata master transition\n"), 512)
	sourceS3.request(t, ctx, http.MethodPut, originalBucket, nil, http.StatusOK)
	sourceS3.request(t, ctx, http.MethodPut, originalBucket+"/object", payload, http.StatusOK)
	waitMultisiteObject(t, ctx, destinationS3, originalBucket+"/object", http.StatusOK, payload)
	waitRGWMetadataCaughtUp(t, ctx, link.DestinationAdmin)
	fenceRGWScenarioGateway(t, ctx, link.Source)

	for _, command := range [][]string{
		{"zone", "modify", "--master", "--default"},
		{"zonegroup", "modify", "--master"},
		// Stage and commit separately. On Ceph 20.2.4 the combined command
		// initializes its driver from the already modified local zonegroup,
		// so its native promotion check can see an empty master sync status.
		// A separate commit reads sync state using the old current period.
		{"period", "update"},
		{"period", "commit"},
	} {
		rgwScenarioAdmin(t, ctx, link.DestinationAdmin, command...)
	}
	restartRGWScenarioGateway(t, ctx, link.Destination, &destinationS3)
	assertRGWMetadataMaster(t, ctx, link.DestinationAdmin, link.DestinationZoneID)
	promotedUser := rgwScenarioUser(t, ctx, link.DestinationAdmin, "tc-promoted-user", destinationS3)
	const promotedBucket = "/tc-created-after-promotion"
	promotedUser.request(t, ctx, http.MethodPut, promotedBucket, nil, http.StatusOK)
	promotedUser.request(t, ctx, http.MethodPut, promotedBucket+"/object", payload, http.StatusOK)
	if got := promotedUser.request(t, ctx, http.MethodGet, promotedBucket+"/object", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("promoted metadata master changed its fresh user's payload")
	}
	t.Log("with former master gateway fenced and metadata caught up: destination became metadata master and created a new user, bucket and private object")

	if err := link.PullSourcePeriod(ctx); err != nil {
		t.Fatal(err)
	}
	restartRGWScenarioGateway(t, ctx, link.Source, &sourceS3)
	assertRGWMetadataMaster(t, ctx, link.SourceAdmin, link.DestinationZoneID)
	recoveredSourceUser := promotedUser
	recoveredSourceUser.endpoint = sourceS3.endpoint
	waitMultisiteObject(t, ctx, recoveredSourceUser, promotedBucket+"/object", http.StatusOK, payload)
	waitRGWMetadataCaughtUp(t, ctx, link.SourceAdmin)
	if got := sourceS3.request(t, ctx, http.MethodGet, originalBucket+"/object", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("recovering the former master changed its pre-transition data")
	}
	t.Log("former master imported the new period and replicated the new master's user, bucket and object before failback")
	fenceRGWScenarioGateway(t, ctx, link.Destination)
	for _, command := range [][]string{
		{"zone", "modify", "--master", "--default"},
		{"zonegroup", "modify", "--master"},
		{"period", "update"},
		{"period", "commit"},
	} {
		rgwScenarioAdmin(t, ctx, link.SourceAdmin, command...)
	}
	restartRGWScenarioGateway(t, ctx, link.Source, &sourceS3)
	assertRGWMetadataMaster(t, ctx, link.SourceAdmin, link.SourceZoneID)
	failedBackUser := rgwScenarioUser(t, ctx, link.SourceAdmin, "tc-failed-back-user", sourceS3)
	const failedBackBucket = "/tc-created-after-failback"
	failedBackUser.request(t, ctx, http.MethodPut, failedBackBucket, nil, http.StatusOK)
	failedBackUser.request(t, ctx, http.MethodPut, failedBackBucket+"/object", payload, http.StatusOK)
	if err := link.PullDestinationPeriod(ctx); err != nil {
		t.Fatal(err)
	}
	restartRGWScenarioGateway(t, ctx, link.Destination, &destinationS3)
	assertRGWMetadataMaster(t, ctx, link.DestinationAdmin, link.SourceZoneID)
	recoveredDestinationUser := failedBackUser
	recoveredDestinationUser.endpoint = destinationS3.endpoint
	waitMultisiteObject(t, ctx, recoveredDestinationUser, failedBackBucket+"/object", http.StatusOK, payload)
	promotedUser.endpoint = destinationS3.endpoint
	if got := promotedUser.request(t, ctx, http.MethodGet, promotedBucket+"/object", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("failback changed the object created while destination was master")
	}
	t.Log("planned failback restored source metadata mastery; a second new user and bucket replicated to the recovered destination, preserving both earlier buckets")
}
