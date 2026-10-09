//go:build all || (integration && multicluster && features)

package integration_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
	"github.com/jsyoo5b/ceph-testcontainers-go/multicluster"
	"github.com/testcontainers/testcontainers-go"
)

func testRGWOwnedSyncPolicy(t *testing.T, opts ...testcontainers.ContainerCustomizer) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Minute)
	defer cancel()
	// A single OSD per independent cluster is sufficient for protocol and
	// selective-sync fixtures; this scenario does not test storage redundancy.
	opts = append(opts, ceph.WithOSDCount(1))
	source, destination, _, _ := newMultiClusterPair(t, opts...)
	controlImage, _ := integrationImages(t)
	image := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if image == "" {
		image = controlImage
	}
	link, err := multicluster.RunRGWMultisite(ctx, image, multicluster.RGWMultisiteConfig{Source: source, Destination: destination, ControlImage: controlImage, Realm: "tc-owned-sync", SourceZone: "source", DestinationZone: "destination"})
	if link != nil {
		t.Cleanup(func() {
			if t.Failed() {
				for _, name := range []string{"source", "destination"} {
					inspect, stop := context.WithTimeout(context.Background(), 35*time.Second)
					status, statusErr := link.SyncStatus(inspect, name)
					stop()
					t.Logf("owned sync diagnostic zone=%s status=%+v error=%v", name, status, statusErr)
				}
			}
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if err := link.Terminate(cleanup); err != nil {
				t.Errorf("terminate owned RGW sync fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	global, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{}, multicluster.RGWSyncGroupConfig{ID: "selective", Status: multicluster.RGWSyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	symmetric := multicluster.RGWSyncFlowConfig{ID: "owned-pair", Zones: []string{"source", "destination"}}
	if err := link.CreateSyncFlow(ctx, global, symmetric); err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncPipe(ctx, global, multicluster.RGWSyncPipeConfig{ID: "permission", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, global); err != nil {
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
	started := time.Now()
	status, err := link.WaitSyncReady(ctx, "destination", "source")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("native readiness after policy reload: elapsed=%s period=%s realm_epoch=%d metadata_shards=%d data=%+v", time.Since(started).Round(time.Millisecond), status.PeriodID, status.RealmEpoch, status.Metadata.IncrementalShards, status.DataSources)
	const selected, localOnly = "/tc-owned-selected", "/tc-owned-local-only"
	sourceS3.request(t, ctx, http.MethodPut, selected, nil, http.StatusOK)
	sourceS3.request(t, ctx, http.MethodPut, localOnly, nil, http.StatusOK)
	bucket, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: "tc-owned-selected"}, multicluster.RGWSyncGroupConfig{ID: "selected-prefix", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	if err := link.CreateSyncPipe(ctx, bucket, multicluster.RGWSyncPipeConfig{ID: "prefix", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}, Prefix: "published/"}); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	payload := bytes.Repeat([]byte("owned RGW selective sync fixture\n"), 2048)
	for _, path := range []string{selected + "/published/selected", selected + "/private/excluded", localOnly + "/published/excluded"} {
		sourceS3.request(t, ctx, http.MethodPut, path, payload, http.StatusOK)
		if got := sourceS3.request(t, ctx, http.MethodGet, path, nil, http.StatusOK); !bytes.Equal(got, payload) {
			t.Fatal("source changed selective-sync payload")
		}
	}
	waitOwnedSyncObject(t, ctx, destinationS3, selected+"/published/selected", http.StatusOK, payload)
	waitOwnedBucketCheckpoint(t, ctx, link, bucket, "prefix")
	waitRGWScenarioResponse(t, ctx, destinationS3, http.MethodHead, localOnly, http.StatusOK)
	destinationS3.request(t, ctx, http.MethodPut, selected+"/published/reverse-excluded", payload, http.StatusOK)
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	requireRGWObjectsAbsent(t, ctx, 35*time.Second,
		rgwAbsentObject{destinationS3, selected + "/private/excluded"},
		rgwAbsentObject{destinationS3, localOnly + "/published/excluded"},
		rgwAbsentObject{sourceS3, selected + "/published/reverse-excluded"},
	)
	t.Log("owned policy replicated matching bucket/prefix bytes and excluded another prefix, another bucket and the reverse direction during 35 seconds")
	if err := link.SetSyncGroupStatus(ctx, bucket, multicluster.RGWSyncForbidden); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	waitOwnedBucketDisabled(t, ctx, link, bucket, "prefix")
	sourceS3.request(t, ctx, http.MethodPut, selected+"/published/while-forbidden", payload, http.StatusOK)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destinationS3, selected + "/published/while-forbidden"})
	if got := destinationS3.request(t, ctx, http.MethodGet, selected+"/published/selected", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("forbidden policy changed earlier replicated bytes")
	}
	if err := link.SetSyncGroupStatus(ctx, bucket, multicluster.RGWSyncEnabled); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
		t.Fatal(err)
	}
	sourceS3.request(t, ctx, http.MethodPut, selected+"/published/after-enabled", payload, http.StatusOK)
	waitOwnedSyncObject(t, ctx, destinationS3, selected+"/published/after-enabled", http.StatusOK, payload)
	reconcileOwnedBucketCheckpoint(t, ctx, link, bucket, "prefix")
	t.Log("dynamic forbidden status stopped selected future writes during 35 seconds; enabled restored exact future bytes/checkpoint and retained earlier copies")
	// A bucket update changes future selection without committing/restarting
	// the realm. Preserve copied objects while checking the new positive path.
	if err := link.SetSyncPipePrefix(ctx, bucket, "prefix", "reports/"); err != nil {
		t.Fatal(err)
	}
	waitRGWScenarioBucketPolicyImport(t, ctx, link, bucket)
	sourceS3.request(t, ctx, http.MethodPut, selected+"/reports/after-update", payload, http.StatusOK)
	sourceS3.request(t, ctx, http.MethodPut, selected+"/published/after-update", payload, http.StatusOK)
	waitOwnedSyncObject(t, ctx, destinationS3, selected+"/reports/after-update", http.StatusOK, payload)
	reconcileOwnedBucketCheckpoint(t, ctx, link, bucket, "prefix")
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destinationS3, selected + "/published/after-update"})
	sourceS3.request(t, ctx, http.MethodDelete, selected+"/reports/after-update", nil, http.StatusNoContent)
	waitOwnedSyncObject(t, ctx, destinationS3, selected+"/reports/after-update", http.StatusNotFound, nil)
	copy := *bucket
	if err := link.RemoveSyncGroup(ctx, bucket); err != nil {
		t.Fatal(err)
	}
	if err := link.RemoveSyncGroup(ctx, &copy); err != nil {
		t.Fatal(err)
	}
	sourceS3.request(t, ctx, http.MethodPut, selected+"/reports/after-removal", payload, http.StatusOK)
	requireRGWObjectsAbsent(t, ctx, 35*time.Second, rgwAbsentObject{destinationS3, selected + "/reports/after-removal"})
	if got := destinationS3.request(t, ctx, http.MethodGet, selected+"/published/selected", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("sync group removal changed earlier replicated object bytes")
	}
	// A recreated bucket name is a different server fixture. The old policy
	// handle must refuse removal and leave the replacement's data unchanged.
	const reused = "/tc-owned-recreated"
	sourceS3.request(t, ctx, http.MethodPut, reused, nil, http.StatusOK)
	old, err := link.CreateSyncGroup(ctx, multicluster.RGWSyncPolicyScope{Bucket: "tc-owned-recreated"}, multicluster.RGWSyncGroupConfig{ID: "old-instance", Status: multicluster.RGWSyncEnabled})
	if err != nil {
		t.Fatal(err)
	}
	sourceS3.request(t, ctx, http.MethodDelete, reused, nil, http.StatusNoContent)
	sourceS3.request(t, ctx, http.MethodPut, reused, nil, http.StatusOK)
	sourceS3.request(t, ctx, http.MethodPut, reused+"/replacement", payload, http.StatusOK)
	if err := link.RemoveSyncGroup(ctx, old); err == nil {
		t.Fatal("old owned handle removed policy on a recreated bucket name")
	}
	if got := sourceS3.request(t, ctx, http.MethodGet, reused+"/replacement", nil, http.StatusOK); !bytes.Equal(got, payload) {
		t.Fatal("recreated bucket removal guard changed replacement payload")
	}
	// Directional native identity is a pair; symmetrical removal uses its
	// owned name. Exercise both without broadening the directional pipe.
	if err := link.CreateSyncFlow(ctx, global, multicluster.RGWSyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}); err != nil {
		t.Fatal(err)
	}
	if err := link.RemoveSyncFlow(ctx, global, symmetric); err != nil {
		t.Fatal(err)
	}
	if err := link.RemoveSyncGroup(ctx, global); err != nil {
		t.Fatal(err)
	}
	if err := link.ApplySyncGroup(ctx, global); err != nil {
		t.Fatal(err)
	}
	t.Log("dynamic prefix update/deletion and owned group removal retained old replicas; recreated native bucket instance refused stale-handle removal")
}

func waitOwnedBucketCheckpoint(t *testing.T, ctx context.Context, link *multicluster.RGWMultisite, group *multicluster.RGWSyncGroup, pipe string) multicluster.RGWBucketSyncStatus {
	t.Helper()
	started := time.Now()
	status, err := link.WaitBucketSyncReady(ctx, group, pipe, "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	if !status.CaughtUp || status.Shards <= 0 || status.BehindShards != 0 {
		t.Fatalf("selected native bucket checkpoint is incomplete: %+v", status)
	}
	t.Logf("native bucket checkpoint after exact bytes: elapsed=%s period=%s source=%s/%s:%s destination=%s/%s:%s state=%s shards=%d behind=%d", time.Since(started).Round(time.Millisecond), status.PeriodID, status.SourceBucket.Tenant, status.SourceBucket.Name, status.SourceBucket.ID, status.DestinationBucket.Tenant, status.DestinationBucket.Name, status.DestinationBucket.ID, status.State, status.Shards, status.BehindShards)
	return status
}

func reconcileOwnedBucketCheckpoint(t *testing.T, ctx context.Context, link *multicluster.RGWMultisite, group *multicluster.RGWSyncGroup, pipe string) {
	t.Helper()
	before, err := link.BucketSyncStatus(ctx, group, pipe, "source", "destination")
	if err != nil || before.State != "incremental" || before.Shards <= 0 {
		t.Fatalf("explicit bucket reconciliation requires its exact enabled incremental checkpoint: state=%s error=%v", before.State, err)
	}
	destination := before.DestinationBucket.Name
	if before.DestinationBucket.Tenant != "" {
		destination = before.DestinationBucket.Tenant + "/" + destination
	}
	args := []string{"bucket", "sync", "run", "--bucket", destination, "--bucket-id", before.DestinationBucket.ID, "--source-zone", before.SourceZoneID, "--source-bucket", before.SourceBucket.Name, "--source-bucket-id", before.SourceBucket.ID}
	if before.SourceBucket.Tenant != "" {
		args = append(args, "--source-tenant", before.SourceBucket.Tenant)
	}
	// Policy reactivation need not revisit every previously disabled shard.
	// Explicitly resume existing native logs with the current policy. Do not
	// initialize/reset markers or override filters, permissions or lock leases.
	run, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	if _, err := link.ZoneAdmin(run, before.Zone, args...); err != nil {
		t.Fatalf("run exact owned bucket reconciliation: %v", err)
	}
	after := waitOwnedBucketCheckpoint(t, ctx, link, group, pipe)
	if after.RealmID != before.RealmID || after.PeriodID != before.PeriodID || after.SourceZoneID != before.SourceZoneID || after.ZoneID != before.ZoneID || after.SourceBucket != before.SourceBucket || after.DestinationBucket != before.DestinationBucket {
		t.Fatal("bucket reconciliation changed the captured zone/period/bucket identities")
	}
	t.Logf("explicit native bucket sync run preserved current policy/identities and caught up previous logs: prior_behind=%d", before.BehindShards)
}

func waitOwnedBucketDisabled(t *testing.T, ctx context.Context, link *multicluster.RGWMultisite, group *multicluster.RGWSyncGroup, pipe string) {
	t.Helper()
	if _, err := link.WaitSyncReady(ctx, "destination"); err != nil {
		t.Fatal(err)
	}
	wait, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()
	started := time.Now()
	var status multicluster.RGWBucketSyncStatus
	var lastErr error
	for {
		status, lastErr = link.BucketSyncStatus(wait, group, pipe, "source", "destination")
		if wait.Err() == nil && lastErr == nil && !status.CaughtUp && (status.State == "stopped" || status.State == "disabled_or_no_sources") {
			t.Logf("native forbidden bucket policy observed before excluded write: elapsed=%s state=%s", time.Since(started).Round(time.Millisecond), status.State)
			return
		}
		select {
		case <-wait.Done():
			t.Fatalf("wait forbidden RGW bucket policy: %v; state=%s; last_error=%v", wait.Err(), status.State, lastErr)
		case <-time.After(time.Second):
		}
	}
}

func waitOwnedSyncObject(t *testing.T, ctx context.Context, client s3HTTPClient, path string, status int, payload []byte) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	started := time.Now()
	var last string
	for {
		request, err := http.NewRequestWithContext(wait, http.MethodGet, client.endpoint+path, nil)
		if err == nil {
			client.sign(request, nil, time.Now())
			response, err := client.http.Do(request)
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				response.Body.Close()
				code := rgwBackendErrorCode(body)
				matches := (status == http.StatusNotFound && code == "NoSuchKey") || (status != http.StatusNotFound && bytes.Equal(body, payload))
				if wait.Err() == nil && readErr == nil && response.StatusCode == status && matches {
					t.Logf("strict replica path=%s status=%d bytes=%d elapsed=%s", path, status, len(payload), time.Since(started).Round(time.Millisecond))
					return
				}
				last = fmt.Sprintf("status=%d code=%s bytes=%d read_error=%v", response.StatusCode, code, len(body), readErr)
			} else {
				last = "HTTP request failed"
			}
		} else {
			last = "HTTP request could not be constructed"
		}
		select {
		case <-wait.Done():
			t.Fatalf("wait owned RGW replica %s: %v; %s", path, wait.Err(), last)
		case <-time.After(time.Second):
		}
	}
}
