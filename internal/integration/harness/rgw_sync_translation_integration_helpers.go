//go:build all || (integration && multicluster)

package harness

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	ceph "github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/multisite"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw/placement"
	"github.com/testcontainers/testcontainers-go"
)

func WaitRGWTranslationPolicyReady(t *testing.T, ctx context.Context, link *multisite.Fixture, group *multisite.SyncGroup) {
	t.Helper()
	status, err := group.WaitBucketPolicyReady(ctx, "destination")
	if err != nil || !status.Imported || !status.PeriodImported || !status.PolicyImported || !status.BucketsImported || status.Bucket.ID == "" || len(status.Buckets) == 0 {
		t.Fatalf("wait owned bucket policy import: group=%s period=%s zone=%s/%s bucket=%s/%s:%s period_imported=%t policy_imported=%t buckets_imported=%t error=%s", group.ID(), status.PeriodID, status.Zone, status.ZoneID, status.Bucket.Tenant, status.Bucket.Name, status.Bucket.ID, status.PeriodImported, status.PolicyImported, status.BucketsImported, RGWSyncDiagnosticError(err))
	}
	t.Logf("owned bucket policy imported before writes: group=%s period=%s zone=%s/%s scope=%s/%s:%s referenced_buckets=%+v", group.ID(), status.PeriodID, status.Zone, status.ZoneID, status.Bucket.Tenant, status.Bucket.Name, status.Bucket.ID, status.Buckets)
}

// Child cleanups run before the next sequential t.Run, including after Fatal.
// The public removal guard verifies the owned bucket instance and unchanged
// group, and successful explicit removals make this cleanup idempotent.
func CleanupRGWTranslationSyncGroup(t *testing.T, link *multisite.Fixture, group *multisite.SyncGroup, pipeIDs ...string) {
	t.Helper()
	pipeIDs = append([]string(nil), pipeIDs...)
	t.Cleanup(func() {
		if t.Failed() {
			inspect, stop := context.WithTimeout(context.Background(), time.Minute)
			for _, pipeID := range pipeIDs {
				attempt, cancel := context.WithTimeout(inspect, 20*time.Second)
				state, err := group.BucketSyncStatus(attempt, pipeID, "source", "destination")
				cancel()
				t.Logf("translation bucket diagnostic group=%s pipe=%s realm=%s period=%s zonegroup=%s realm_epoch=%d source_zone=%s/%s source_bucket=%s/%s:%s destination_zone=%s/%s destination_bucket=%s/%s:%s state=%s shards=%d behind_shards=%d caught_up=%t status_error=%s", group.ID(), pipeID, state.RealmID, state.PeriodID, state.ZonegroupID, state.RealmEpoch, state.SourceZone, state.SourceZoneID, state.SourceBucket.Tenant, state.SourceBucket.Name, state.SourceBucket.ID, state.Zone, state.ZoneID, state.DestinationBucket.Tenant, state.DestinationBucket.Name, state.DestinationBucket.ID, state.State, state.Shards, state.BehindShards, state.CaughtUp, RGWSyncDiagnosticError(err))
			}
			stop()
		}
		cleanup, stop := context.WithTimeout(context.Background(), 2*time.Minute)
		defer stop()
		if err := group.Remove(cleanup); err != nil {
			t.Errorf("cleanup isolated translation sync group %s: %v", group.ID(), err)
			return
		}
		t.Logf("removed isolated owned translation sync group %s before later subtests", group.ID())
	})
}

func AssertRGWSyncClass(t *testing.T, ctx context.Context, client S3HTTPClient, bucket, key, class string) {
	t.Helper()
	var listing struct {
		Contents []struct{ Key, StorageClass string } `xml:"Contents"`
	}
	if err := xml.Unmarshal(client.Request(t, ctx, http.MethodGet, bucket+"?list-type=2", nil, http.StatusOK), &listing); err != nil {
		t.Fatal(err)
	}
	for _, entry := range listing.Contents {
		if entry.Key == key {
			if entry.StorageClass != class {
				t.Fatalf("replica %s class=%s want=%s", key, entry.StorageClass, class)
			}
			return
		}
	}
	t.Fatalf("replica listing omitted %s", key)
}

// RGWSyncTranslationFixture is the two-zone realm, tiered placement, permission
// policy and two source owners that the RGW sync translation scenarios share.
type RGWSyncTranslationFixture struct {
	Ctx                                  context.Context
	Source, Destination                  *ceph.Container
	Link                                 *multisite.Fixture
	PrimaryPlacement, SecondaryPlacement *placement.Placement
	OwnerA, OwnerB                       *rgw.User
	Client                               func(t *testing.T, user *rgw.User, endpoint string) S3HTTPClient
	SourceEndpoint, DestEndpoint         string
	A, B, DestA, DestB                   S3HTTPClient
	Body                                 []byte
}

func NewRGWSyncTranslationFixture(t *testing.T, ctx context.Context, opts ...testcontainers.ContainerCustomizer) *RGWSyncTranslationFixture {
	// Independent scenarios share realm/storage bootstrap. Reserve bounded
	// fixture time for strict four-minute observations and child cleanup after
	// a failing scenario; per-object/checkpoint and exclusion windows stay fixed.
	source, destination, _, _ := NewMultiClusterPair(t, append(opts, ceph.WithClusterConfig(ceph.ClusterConfig{}.WithOSDCount(1)))...)
	image, _ := IntegrationImages(t)
	rgwImage := os.Getenv("CEPH_TEST_RGW_IMAGE")
	if rgwImage == "" {
		rgwImage = image
	}
	link, err := multisite.Run(ctx, rgwImage, multisite.WithConfig(multisite.Config{Source: source, Destination: destination, Realm: "tc-sync-translation", SourceZone: "source", DestinationZone: "destination"}))
	if link != nil {
		t.Cleanup(func() {
			if t.Failed() {
				for _, name := range []string{"source", "destination"} {
					inspect, stop := context.WithTimeout(context.Background(), 35*time.Second)
					state, statusErr := link.SyncStatus(inspect, name)
					stop()
					t.Logf("sync translation diagnostic zone=%s status=%+v error=%v", name, state, statusErr)
				}
			}
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			defer stop()
			if err := link.Terminate(cleanup); err != nil {
				t.Errorf("terminate translation fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []*ceph.Container{source, destination} {
		for _, pool := range []string{"tc-sync-index", "tc-sync-extra", "tc-sync-standard", "tc-sync-ia"} {
			if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: pool, PGNum: 1, Application: "rgw"}); err != nil {
				t.Fatal(err)
			}
		}
		if err := ObserveCluster(cluster).WaitForClean(ctx); err != nil {
			t.Fatal(err)
		}
	}
	inline := false
	placementConfig := placement.Config{Name: "tc-sync-tiered", IndexPool: "tc-sync-index", DataExtraPool: "tc-sync-extra", InlineData: &inline, StorageClasses: []placement.StorageClass{{Name: "STANDARD", DataPool: "tc-sync-standard"}, {Name: "STANDARD_IA", DataPool: "tc-sync-ia"}}}
	primaryPlacement, err := placement.Create(ctx, link.Source, placementConfig)
	if err != nil {
		t.Fatal(err)
	}
	secondaryPlacement, err := placement.Create(ctx, link.Destination, placementConfig)
	if err != nil {
		t.Fatal(err)
	}
	if err := primaryPlacement.Apply(ctx, link.Source); err != nil {
		t.Fatal(err)
	}
	if err := link.PullDestinationPeriod(ctx); err != nil {
		t.Fatal(err)
	}
	if err := secondaryPlacement.Reload(ctx, link.Destination); err != nil {
		t.Fatal(err)
	}
	global, err := link.CreateSyncGroup(ctx, multisite.SyncPolicyScope{}, multisite.SyncGroupConfig{ID: "permission", Status: multisite.SyncAllowed})
	if err != nil {
		t.Fatal(err)
	}
	if err := global.CreateFlow(ctx, multisite.SyncFlowConfig{SourceZone: "source", DestinationZone: "destination"}); err != nil {
		t.Fatal(err)
	}
	if err := global.CreatePipe(ctx, multisite.SyncPipeConfig{ID: "permit", SourceZones: []string{"source"}, DestinationZones: []string{"destination"}}); err != nil {
		t.Fatal(err)
	}
	if err := global.Apply(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := link.WaitSyncReady(ctx, "destination", "source"); err != nil {
		t.Fatal(err)
	}
	ownerA, err := link.Source.CreateUser(ctx, rgw.UserConfig{ID: "tc-sync-owner-a"})
	if err != nil {
		t.Fatal(err)
	}
	ownerB, err := link.Source.CreateUser(ctx, rgw.UserConfig{ID: "tc-sync-owner-b"})
	if err != nil {
		t.Fatal(err)
	}
	sourceEndpoint, err := link.Source.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	destEndpoint, err := link.Destination.S3Endpoint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := func(t *testing.T, user *rgw.User, endpoint string) S3HTTPClient {
		t.Helper()
		access, secret, err := user.CreationKey().Credentials()
		if err != nil {
			t.Fatal(err)
		}
		return S3HTTPClient{Endpoint: endpoint, AccessKey: access, SecretKey: secret, Region: link.Source.Region, HTTP: &http.Client{Timeout: 15 * time.Second}}
	}
	a, b := client(t, ownerA, sourceEndpoint), client(t, ownerB, sourceEndpoint)
	destA, destB := client(t, ownerA, destEndpoint), client(t, ownerB, destEndpoint)
	body := []byte(`<CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><LocationConstraint>` + primaryPlacement.LocationConstraint + `</LocationConstraint></CreateBucketConfiguration>`)
	return &RGWSyncTranslationFixture{Ctx: ctx, Source: source, Destination: destination, Link: link, PrimaryPlacement: primaryPlacement, SecondaryPlacement: secondaryPlacement, OwnerA: ownerA, OwnerB: ownerB, Client: client, SourceEndpoint: sourceEndpoint, DestEndpoint: destEndpoint, A: a, B: b, DestA: destA, DestB: destB, Body: body}
}

// Diagnostic failures never replace the test's original failure. Native output,
// credentials, sync markers and S3 error bodies are deliberately not logged.
func RGWSyncDiagnosticError(err error) string {
	switch {
	case err == nil:
		return "none"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "read_failed_redacted"
	}
}

type RGWAbsentObject struct {
	Client S3HTTPClient
	Path   string
}

func RequireRGWObjectsAbsent(t *testing.T, ctx context.Context, duration time.Duration, objects ...RGWAbsentObject) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		for _, object := range objects {
			attempt, cancel := context.WithTimeout(ctx, 10*time.Second)
			status, body, err := RGWScenarioRequest(attempt, object.Client, http.MethodGet, object.Path)
			cancel()
			var s3Error struct {
				Code string `xml:"Code"`
			}
			if err != nil || status != http.StatusNotFound || xml.Unmarshal(body, &s3Error) != nil || s3Error.Code != "NoSuchKey" {
				t.Fatalf("excluded RGW object %s must remain NoSuchKey: status=%d error=%v code=%s", object.Path, status, err, s3Error.Code)
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
}

func WaitOwnedBucketCheckpoint(t *testing.T, ctx context.Context, link *multisite.Fixture, group *multisite.SyncGroup, pipe string) multisite.BucketSyncStatus {
	t.Helper()
	started := time.Now()
	status, err := group.WaitBucketSyncReady(ctx, pipe, "source", "destination")
	if err != nil {
		t.Fatal(err)
	}
	if !status.CaughtUp || status.Shards <= 0 || status.BehindShards != 0 {
		t.Fatalf("selected native bucket checkpoint is incomplete: %+v", status)
	}
	t.Logf("native bucket checkpoint after exact bytes: elapsed=%s period=%s source=%s/%s:%s destination=%s/%s:%s state=%s shards=%d behind=%d", time.Since(started).Round(time.Millisecond), status.PeriodID, status.SourceBucket.Tenant, status.SourceBucket.Name, status.SourceBucket.ID, status.DestinationBucket.Tenant, status.DestinationBucket.Name, status.DestinationBucket.ID, status.State, status.Shards, status.BehindShards)
	return status
}

func WaitOwnedSyncObject(t *testing.T, ctx context.Context, client S3HTTPClient, path string, status int, payload []byte) {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 4*time.Minute)
	defer cancel()
	started := time.Now()
	var last string
	for {
		request, err := http.NewRequestWithContext(wait, http.MethodGet, client.Endpoint+path, nil)
		if err == nil {
			client.Sign(request, nil, time.Now())
			response, err := client.HTTP.Do(request)
			if err == nil {
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				code := RGWBackendErrorCode(body)
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
