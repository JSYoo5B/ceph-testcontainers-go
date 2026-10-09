//go:build all || (integration && multicluster)

package integration_test

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
)

func waitRGWScenarioBucketPolicyImport(t *testing.T, ctx context.Context, link *multicluster.RGWMultisite, group *multicluster.RGWSyncGroup) {
	t.Helper()
	// Import is a prerequisite for future writes. It does not prove effective
	// discovery hints, data checkpoints, object bytes or permission effects.
	started := time.Now()
	status, err := link.WaitBucketSyncPolicyReady(ctx, group, "destination")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bucket policy metadata imported before future writes: elapsed=%s group=%s period=%s zone=%s/%s bucket=%s/%s:%s", time.Since(started).Round(time.Millisecond), status.GroupID, status.PeriodID, status.Zone, status.ZoneID, status.Bucket.Tenant, status.Bucket.Name, status.Bucket.ID)
}

func newRGWScenario(t *testing.T, ctx context.Context, realm string) (*multicluster.RGWMultisite, s3HTTPClient, s3HTTPClient) {
	t.Helper()
	source, destination, _, _ := newMultiClusterPair(t)
	controlImage, _ := integrationImages(t)
	image := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if image == "" {
		image = controlImage
	}
	link, err := multicluster.RunRGWMultisite(ctx, image, multicluster.RGWMultisiteConfig{Source: source, Destination: destination, ControlImage: controlImage, Realm: realm, SourceZone: "source", DestinationZone: "destination"})
	if link != nil {
		t.Cleanup(func() {
			if t.Failed() {
				for _, gateway := range []*ceph.RGWContainer{link.Source, link.Destination} {
					if gateway != nil {
						logCtx, logCancel := context.WithTimeout(context.Background(), 5*time.Second)
						multiClusterLogContainer(t, logCtx, gateway.Container)
						logCancel()
					}
				}
			}
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cleanupCancel()
			if err := link.Terminate(cleanupCtx); err != nil {
				t.Errorf("terminate RGW multicluster scenario: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	sourceEndpoint, err := link.Source.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destinationEndpoint, err := link.Destination.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sourceS3 := s3HTTPClient{endpoint: sourceEndpoint, accessKey: link.Source.AccessKey, secretKey: link.Source.SecretKey, region: link.Source.Region, http: &http.Client{Timeout: 15 * time.Second}}
	destinationS3 := sourceS3
	destinationS3.endpoint = destinationEndpoint
	return link, sourceS3, destinationS3
}

func rgwScenarioAdmin(t *testing.T, ctx context.Context, admin func(context.Context, ...string) ([]byte, error), args ...string) []byte {
	t.Helper()
	output, err := admin(ctx, args...)
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func fenceRGWScenarioGateway(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer) {
	t.Helper()
	if gateway.IsRunning() {
		grace := 3 * time.Second
		if err := gateway.Stop(ctx, &grace); err != nil {
			t.Fatal(err)
		}
	}
	if gateway.IsRunning() {
		t.Fatal("metadata master gateway was not fenced")
	}
}

func restartRGWScenarioGateway(t *testing.T, ctx context.Context, gateway *ceph.RGWContainer, client *s3HTTPClient) {
	t.Helper()
	fenceRGWScenarioGateway(t, ctx, gateway)
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}
	endpoint, err := gateway.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client.endpoint = endpoint
	// Starting the process and resolving its new host port does not prove
	// that the gateway can serve this realm's ordinary signed S3 requests.
	// Wait for both the listener and its user metadata before the next write.
	waitRGWScenarioResponse(t, ctx, *client, http.MethodGet, "/", http.StatusOK)
}

func assertRGWMetadataMaster(t *testing.T, ctx context.Context, admin func(context.Context, ...string) ([]byte, error), zoneID string) {
	t.Helper()
	var period struct {
		ID         string `json:"id"`
		RealmEpoch uint64 `json:"realm_epoch"`
		MasterZone string `json:"master_zone"`
	}
	if err := json.Unmarshal(rgwScenarioAdmin(t, ctx, admin, "period", "get", "--format", "json"), &period); err != nil || period.ID == "" || period.RealmEpoch == 0 || period.MasterZone != zoneID {
		t.Fatalf("unexpected current RGW period: id=%q realm_epoch=%d master=%q, want master=%q: %v", period.ID, period.RealmEpoch, period.MasterZone, zoneID, err)
	}
	var group struct {
		MasterZone string `json:"master_zone"`
	}
	if err := json.Unmarshal(rgwScenarioAdmin(t, ctx, admin, "zonegroup", "get"), &group); err != nil || group.MasterZone != zoneID {
		t.Fatalf("current period master %q was not reflected into the local zonegroup: master=%q: %v", zoneID, group.MasterZone, err)
	}
}

func waitRGWMetadataCaughtUp(t *testing.T, ctx context.Context, admin func(context.Context, ...string) ([]byte, error)) {
	t.Helper()
	// The human status can print "caught up" even alongside a warning that
	// metadata sync is on another period. Verify the native promotion guard's
	// realm epoch, period ID and complete incremental shard state as well as
	// the human comparison with the master. Application writes are quiescent.
	waitCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	var last string
	for {
		attemptCtx, attemptCancel := context.WithTimeout(waitCtx, 20*time.Second)
		periodJSON, periodErr := admin(attemptCtx, "period", "get", "--format", "json")
		var period struct {
			ID         string `json:"id"`
			RealmEpoch uint64 `json:"realm_epoch"`
		}
		if periodErr == nil {
			periodErr = json.Unmarshal(periodJSON, &period)
		}
		metadataJSON, metadataErr := admin(attemptCtx, "metadata", "sync", "status", "--format", "json")
		var metadata struct {
			SyncStatus struct {
				Info struct {
					Status     string `json:"status"`
					NumShards  int    `json:"num_shards"`
					Period     string `json:"period"`
					RealmEpoch uint64 `json:"realm_epoch"`
				} `json:"info"`
				Markers []struct {
					Key int `json:"key"`
					Val struct {
						State int `json:"state"`
					} `json:"val"`
				} `json:"markers"`
			} `json:"sync_status"`
		}
		if metadataErr == nil {
			metadataErr = json.Unmarshal(metadataJSON, &metadata)
		}
		info := metadata.SyncStatus.Info
		markersCurrent := info.NumShards > 0 && len(metadata.SyncStatus.Markers) == info.NumShards
		shards := make(map[int]bool, info.NumShards)
		for _, marker := range metadata.SyncStatus.Markers {
			if marker.Val.State != 1 || marker.Key < 0 || marker.Key >= info.NumShards || shards[marker.Key] {
				markersCurrent = false
			}
			shards[marker.Key] = true
		}
		output, err := admin(attemptCtx, "sync", "status")
		attemptCancel()
		if periodErr == nil && metadataErr == nil && err == nil && period.ID != "" && period.RealmEpoch > 0 && info.Status == "sync" && info.Period == period.ID && info.RealmEpoch == period.RealmEpoch && markersCurrent &&
			strings.Contains(string(output), "metadata is caught up with master") && !strings.Contains(string(output), "master is on a different period") {
			t.Logf("RGW metadata sync is caught up before fencing: period=%s realm_epoch=%d incremental_shards=%d", period.ID, period.RealmEpoch, info.NumShards)
			return
		}
		last = fmt.Sprintf("period_error=%v metadata_error=%v status_error=%v current_period=%s current_realm_epoch=%d sync_period=%s sync_realm_epoch=%d sync_state=%s incremental_shards_ready=%v status=%s metadata=%s", periodErr, metadataErr, err, period.ID, period.RealmEpoch, info.Period, info.RealmEpoch, info.Status, markersCurrent, output, metadataJSON)
		select {
		case <-waitCtx.Done():
			t.Fatalf("wait for RGW metadata catch-up: %v; %s", waitCtx.Err(), last)
		case <-time.After(time.Second):
		}
	}
}

func rgwScenarioUser(t *testing.T, ctx context.Context, admin func(context.Context, ...string) ([]byte, error), uid string, template s3HTTPClient) s3HTTPClient {
	t.Helper()
	var user struct {
		UserID string `json:"user_id"`
		Keys   []struct {
			Access string `json:"access_key"`
			Secret string `json:"secret_key"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(rgwScenarioAdmin(t, ctx, admin, "user", "create", "--uid", uid, "--display-name", "RGW metadata master scenario"), &user); err != nil || user.UserID != uid || len(user.Keys) != 1 || user.Keys[0].Access == "" || user.Keys[0].Secret == "" {
		t.Fatalf("new master did not return ordinary S3 user %s: %v", uid, err)
	}
	template.accessKey, template.secretKey = user.Keys[0].Access, user.Keys[0].Secret
	return template
}

func rgwScenarioRequest(ctx context.Context, client s3HTTPClient, method, path string) (int, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, method, client.endpoint+path, nil)
	if err != nil {
		return 0, nil, err
	}
	client.sign(request, nil, time.Now().UTC())
	response, err := client.http.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	return response.StatusCode, body, err
}

func waitRGWScenarioResponse(t *testing.T, ctx context.Context, client s3HTTPClient, method, path string, expected int) {
	t.Helper()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	var last string
	for {
		status, body, err := rgwScenarioRequest(waitCtx, client, method, path)
		if err == nil && status == expected {
			return
		}
		last = fmt.Sprintf("status=%d error=%v body=%s", status, err, body)
		select {
		case <-waitCtx.Done():
			t.Fatalf("wait for RGW %s %s status=%d: %s", method, path, expected, last)
		case <-time.After(time.Second):
		}
	}
}

type rgwAbsentObject struct {
	client s3HTTPClient
	path   string
}

func requireRGWObjectsAbsent(t *testing.T, ctx context.Context, duration time.Duration, objects ...rgwAbsentObject) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		for _, object := range objects {
			attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
			status, body, err := rgwScenarioRequest(attempt, object.client, http.MethodGet, object.path)
			cancel()
			var s3Error struct {
				Code string `xml:"Code"`
			}
			if err != nil || status != http.StatusNotFound || xml.Unmarshal(body, &s3Error) != nil || s3Error.Code != "NoSuchKey" {
				t.Fatalf("excluded RGW object %s must remain NoSuchKey: status=%d error=%v code=%s", object.path, status, err, s3Error.Code)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}
